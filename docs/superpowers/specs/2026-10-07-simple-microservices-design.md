# Simple Microservices: design

Demo minimalista de arquitetura desacoplada: gateways Python e Go gravam em outbox, um relay entrega a um webhook (router), e o router decide qual worker (Celery, TaskIQ, asyncio ou goroutine) processa cada job. Código enxuto; o objetivo é mostrar a arquitetura, não medir desempenho.

## 1. Objetivos e não-objetivos

**Objetivos**
- Desacoplamento: gateways não conhecem workers; só o router conhece.
- Mesma API nos dois gateways (Python e Go), mesmo comportamento.
- Garantia de entrega via transactional outbox (at-least-once) com workers idempotentes.
- Código mínimo, legível e documentado.

**Fora de escopo (cortado do projeto anterior)**
Benchmark (k6, bench/), OpenTelemetry/Grafana/Prometheus, fanout, DLQ, mock-server, JSON Schema por tipo de job, `docs/decisoes.md`, CHANGELOG, autenticação, CI/CD, Kubernetes.

## 2. Arquitetura

```
 cliente ──► gateway-py (FastAPI) ─┐
 cliente ──► gateway-go (Gin)     ─┤ 1 transação: jobs + outbox
                                   ▼
                          Postgres (jobs, outbox, job_results)
                                   │ poll (FOR UPDATE SKIP LOCKED)
                                   ▼
                              relay (Go)
                                   │ POST /dispatch (envelope)
                                   ▼
                          router (Go, webhook)
                          type → worker (tabela fixa)
                                   │ publish: exchange "jobs" (direct)
                                   ▼                routing key = worker
                              RabbitMQ
        ┌──────────────┬────────────┴─┬───────────────┐
   jobs.celery    jobs.taskiq    jobs.asyncio      jobs.go
        ▼              ▼              ▼               ▼
  worker-celery  worker-taskiq  worker-asyncio    worker-go
        └──────────────┴───────┬──────┴───────────────┘
                               ▼
              Postgres: job_results + jobs.status
```

### Por que outbox
Gravar `jobs` e publicar no broker são duas escritas sem transação comum. O gateway grava `jobs` + `outbox` numa só transação; apenas o relay entrega adiante. Broker ou router fora do ar não derrubam o `POST /jobs`: a outbox acumula.

### Por que um router (webhook) entre relay e broker
- Relay e gateways ficam ignorantes de filas e workers.
- A decisão de destino vive em um único lugar (tabela `type → worker`).
- Trocar/adicionar worker altera só o router e o `definitions.json`.

## 3. Serviços

| Pasta | Stack | Responsabilidade |
|---|---|---|
| `services/gateway-py` | FastAPI | `POST /jobs`, `GET /jobs/{id}`; grava jobs + outbox |
| `services/gateway-go` | Gin | idem, comportamento idêntico |
| `services/relay` | Go | poll da outbox; `POST` ao router; marca `sent` só após 2xx |
| `services/router` | Go | `POST /dispatch`; escolhe worker por `type`; publica no RabbitMQ |
| `services/worker-celery` | Python | bridge aio-pika → task Celery |
| `services/worker-taskiq` | Python | bridge aio-pika → task TaskIQ |
| `services/worker-asyncio` | Python | aio-pika puro |
| `services/worker-go` | Go | consumer + goroutines (um worker pool pequeno) |

Celery e TaskIQ têm formato de mensagem próprio; o envelope neutro não é nativo. Por isso o padrão **bridge**: consumer fino lê `jobs.<worker>` e chama a task do framework.

## 4. Contrato

`contracts/envelope.schema.json` é o único contrato. Todo serviço o fala.

```json
{
  "job_id": "7c9e6679-7425-40de-944b-e07fc1f90ae7",
  "type": "report.generate",
  "payload": {},
  "created_at": "2026-10-07T04:30:00Z",
  "origin": "gateway-go"
}
```

| Campo | Uso |
|---|---|
| `job_id` | idempotência e correlação |
| `type` | chave de roteamento no router |
| `payload` | entrada livre do job (sem schema por tipo nesta versão) |
| `created_at` | auditoria |
| `origin` | qual gateway recebeu |

Sem `traceparent` e `attempt` (observabilidade e retry cortados).

### Tabela de roteamento (router)

| type | worker |
|---|---|
| `report.generate` | celery |
| `email.send` | taskiq |
| `http.fetch` | asyncio |
| `image.resize` | go |

Os handlers são triviais (log + sleep curto + gravar resultado). A tabela é fixa no código do router; `type` desconhecido retorna `422`.

## 5. Dados (`db/init.sql`)

- `jobs(id uuid pk, type, payload jsonb, status, origin, created_at)`; status: `PENDING`, `DISPATCHED`, `DONE`, `FAILED`.
- `outbox(id bigserial pk, job_id, envelope jsonb, status, created_at)`; status: `pending`, `sent`, `failed`.
- `job_results(job_id, worker, result jsonb, finished_at, pk(job_id, worker))`.

## 6. Garantias e tratamento de erro

- **At-least-once:** relay pode reenviar se cair entre o 2xx e o `UPDATE`; workers aceitam o mesmo `job_id` mais de uma vez.
- **Idempotência:** resultado gravado por `(job_id, worker)` com `ON CONFLICT DO NOTHING`.
- **Ack explícito:** worker só confirma a mensagem depois de gravar o resultado.
- **Router indisponível / 5xx:** relay mantém a linha `pending` e tenta no próximo ciclo.
- **`type` desconhecido (422):** relay marca outbox `failed` e `jobs.status = FAILED`. Sem DLQ.
- **Mensagem inválida no worker:** `nack` sem requeue e log (sem DLQ nesta versão).

## 7. RabbitMQ

`infra/rabbitmq/definitions.json`: exchange `jobs` (direct), filas duráveis `jobs.celery`, `jobs.taskiq`, `jobs.asyncio`, `jobs.go`, binding com routing key igual ao nome do worker. Carregado no boot do broker; nenhum serviço declara filas.

## 8. Estrutura do repositório

```
simple-microservices/
├── services/
│   ├── gateway-py/  gateway-go/  relay/  router/
│   └── worker-celery/  worker-taskiq/  worker-asyncio/  worker-go/
├── contracts/envelope.schema.json
├── db/init.sql
├── infra/rabbitmq/definitions.json
├── docs/superpowers/specs/
├── docker-compose.yml
├── Makefile
├── AGENTS.md
└── README.md
```

Cada serviço tem `pyproject.toml` (uv) ou `go.mod` e Dockerfile próprios; nenhuma dependência cruzada além de `contracts/`.

## 9. Testes

Somente comportamento real:
1. Router: tabela de roteamento (type conhecido → worker; desconhecido → 422).
2. Relay: não marca `sent` se o router falha.
3. Worker (um deles): idempotência com o mesmo `job_id` duas vezes.

Verificação ponta a ponta: `docker compose up`, `POST /jobs` nos dois gateways, `GET /jobs/{id}` até `DONE` para cada um dos 4 types, via Swagger.

## 10. Documentação a ajustar

- **README.md:** reescrever curto (~150 linhas): propósito, diagrama, serviços, contrato, como rodar. Remover benchmark, otel, fanout, DLQ, fases.
- **AGENTS.md:** manter regras de tipagem, docstring, classes gerenciadoras, outbox, idempotência, ack, Swagger e testes. Remover: benchmark justo, observabilidade/traceparent, paridade de 4 workers por resultado idêntico (trocar por "mesmo contrato"), CHANGELOG, `docs/decisoes.md`, menções a DLQ e `make ruff` se o Makefile não o definir.

## 11. Decisões em aberto

Nenhuma. Suposições adotadas: relay e router em Go; CHANGELOG cortado; 4 types demo acima.
