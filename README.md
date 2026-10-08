# Simple Microservices

Demo minimalista de arquitetura desacoplada. Dois gateways (Python e Go) gravam o job numa **outbox**; um **relay** entrega a um **router** (webhook) que decide qual worker processa: Celery, TaskIQ, asyncio ou goroutine. Código enxuto, só para mostrar a arquitetura.

Guia de estudo (mecanismos e decisões): `docs/ESTUDO.md`. Logs, RabbitMQ e como acompanhar um job: `docs/OBSERVABILIDADE.md`. Comparação dos workers (`make bench`): `docs/WORKERS.md`. Comparação com o projeto anterior (`microservices`): `docs/COMPARACAO.md`. Spec: `docs/superpowers/specs/2026-10-07-simple-microservices-design.md`. Plano: `docs/superpowers/plans/2026-10-07-simple-microservices.md`.

---

## 1. Propósito

- Mostrar desacoplamento: gateways não conhecem workers; só o router conhece.
- Mesma API em Python (FastAPI) e Go (Gin), mesmo contrato, mesmo banco.
- Garantir entrega com transactional outbox (at-least-once) e workers idempotentes.
- Manter o código pequeno e legível.

**Fora de escopo:** benchmark, OpenTelemetry/Grafana/Prometheus, fanout, backoff exponencial, schema por tipo de job, autenticação, CI/CD, Kubernetes.

---

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

Gravar `jobs` e publicar no broker são duas escritas sem transação comum. Qualquer ordem falha: publicar antes de gravar cria job desconhecido; gravar antes de publicar deixa o job `PENDING` para sempre se o processo morrer no meio. O gateway grava `jobs` + `outbox` numa só transação e nunca fala com o RabbitMQ; só o relay encaminha. Broker ou router fora do ar não derrubam o `POST /jobs`: a outbox acumula.

### Por que um router entre relay e broker

- Relay e gateways ficam ignorantes de filas e workers.
- A decisão de destino vive em um lugar só (tabela `type → worker`).
- Trocar ou adicionar worker altera o router e o `definitions.json`, nada mais.

### Bridge (Celery e TaskIQ)

Os dois frameworks têm formato de mensagem próprio e o envelope neutro não é nativo. Cada um tem um consumer fino (**bridge**) que lê `jobs.<worker>` e entrega a task ao framework (`process_job.delay` / `process_job.kiq`). Por isso o Celery e o TaskIQ têm dois containers cada: o worker do framework e o bridge.

---

## 3. Serviços

| Pasta | Stack | Porta | Papel |
|---|---|---|---|
| `services/gateway-py` | FastAPI | 58001 | `POST /jobs`, `GET /jobs/{id}`; grava job + outbox |
| `services/gateway-go` | Gin | 58002 | idem, comportamento idêntico |
| `services/relay` | Go | — | poll da outbox; `POST` ao router; marca `sent` só após 2xx |
| `services/router` | Go | interna 8080 | `POST /dispatch`; escolhe worker por `type`; publica no RabbitMQ |
| `services/worker-celery` | Python | — | bridge aio-pika + task Celery |
| `services/worker-taskiq` | Python | — | bridge aio-pika + task TaskIQ |
| `services/worker-asyncio` | Python | — | aio-pika puro |
| `services/worker-go` | Go | — | consumer + uma goroutine por mensagem (limitada pelo prefetch) |
| `services/dlq-reaper` | Go | — | consome `jobs.dlq` e marca o job `FAILED` |

Swagger: gateway-py em `http://localhost:58001/docs`; gateway-go em `http://localhost:58002/docs/index.html`. RabbitMQ management: `http://localhost:55673` (guest/guest).

### API dos gateways

| Endpoint | Corpo | Resposta |
|---|---|---|
| `POST /jobs` | `{"type": "email.send", "payload": {}}` (`payload` opcional) | `202 {"job_id": "<uuid>"}`; `422` se `type` vazio/ausente |
| `GET /jobs/{id}` | — | `200 {id, type, status, origin, created_at, results[]}`; `404`; `422` se o id não é uuid |

O `payload` é livre; só os workers leem o campo opcional `workload` (`io-wait`, `io-block`, `cpu`), usado pelo `make bench`.

Erros usam sempre `{"detail": "<texto curto>"}`: `type is required`, `invalid request body`, `invalid job id`, `job not found`. O `origin` do envelope é `gateway-py` ou `gateway-go`.

---

## 4. Contrato

`contracts/envelope.schema.json` é o único contrato; todo serviço o fala.

```json
{
  "job_id": "7c9e6679-7425-40de-944b-e07fc1f90ae7",
  "type": "report.generate",
  "payload": {},
  "created_at": "2026-10-07T04:30:00Z",
  "origin": "gateway-go"
}
```

### Tabela de roteamento (`services/router/routes.go`)

| type | worker |
|---|---|
| `report.generate` | celery |
| `email.send` | taskiq |
| `http.fetch` | asyncio |
| `image.resize` | go |

Os handlers são triviais (espera curta + gravar resultado). O resultado é sempre `{"worker": "<nome>", "detail": "<texto>"}`.

---

## 5. Dados (`db/init.sql`)

| Tabela | Conteúdo | Status |
|---|---|---|
| `jobs` | job recebido | `PENDING → DISPATCHED → DONE` ou `FAILED` |
| `outbox` | envelope a entregar | `pending → sent` ou `failed` |
| `job_results` | resultado por worker, PK `(job_id, worker)` | — |

Relay: em sucesso (2xx) marca outbox `sent` e job `DISPATCHED` (só se ainda `PENDING`, para não regredir um `DONE`). Em 4xx (ex.: `type` desconhecido) marca `failed`/`FAILED`. Em erro de rede ou 5xx mantém `pending` e tenta no próximo ciclo.

## 6. RabbitMQ (`infra/rabbitmq/definitions.json`)

Exchange `jobs` (direct) e filas duráveis `jobs.celery`, `jobs.taskiq`, `jobs.asyncio`, `jobs.go`, com routing key igual ao nome do worker. Cada fila tem `x-dead-letter-exchange: jobs.dlx` (fanout) ligado à fila `jobs.dlq`. A exchange direct `jobs.retry` e as filas `jobs.retry.go` e `jobs.retry.asyncio` (TTL 5 s, devolvem à exchange `jobs`) implementam a espera do retry. Carregado no boot do broker; nenhum serviço declara topologia. O mesmo arquivo cria o usuário `guest`/`guest` (as definitions substituem o usuário padrão) e `rabbitmq.conf` libera `guest` fora do loopback; só para demo. Celery e TaskIQ criam as filas internas dos próprios frameworks (`celery` e `taskiq`).

## 7. Garantias e limites

- **At-least-once:** o relay pode reenviar se cair entre o 2xx e o `UPDATE`.
- **Idempotência:** resultado gravado por `(job_id, worker)` com `ON CONFLICT DO NOTHING`; o mesmo `job_id` duas vezes não duplica.
- **Ack:** worker só confirma a mensagem depois de gravar o resultado. Nos bridges, o ack vem depois de entregar a task ao broker do framework, que reconhece tarde (`acks_late` no Celery).
- **Mensagem inválida:** `reject`/`nack` sem requeue e log; o broker move a mensagem para `jobs.dlq` (motivo no header `x-death`), e o `dlq-reaper` a consome: marca o job `FAILED` (exceto se já `DONE`) e dá ack. Corpo sem `job_id` válido é logado e descartado. Não há reenvio da DLQ.
- **Retry (workers go e asyncio):** erro transitório (ex.: banco fora) não faz requeue imediato. O worker republica a mensagem (com confirm) na exchange `jobs.retry`, com o contador no header `x-attempt`; a fila `jobs.retry.<worker>` segura por 5 s (`x-message-ttl`) e devolve à fila de trabalho via `x-dead-letter-exchange: jobs`. São no máximo 3 execuções; esgotadas, `nack` sem requeue leva à DLQ e o `dlq-reaper` marca `FAILED`. Se a republicação falha, cai no `nack` com requeue. Atraso fixo, não exponencial. Os bridges Celery/TaskIQ não usam isso: o retry deles é do framework.
- **Healthcheck:** `GET /healthz` no router (:8080) e no relay (:8081, faz ping no Postgres); o compose os usa em `healthcheck`, e o relay só sobe com o router `healthy`.
- **Desligamento (Go):** em SIGTERM/SIGINT o router para de aceitar conexões e termina os publishes em voo (`http.Server.Shutdown`); relay, worker-go e dlq-reaper terminam o ciclo/mensagem em andamento com contexto sem cancelamento, para não abortar no meio um POST já aceito ou um `Save`. Workers Python não têm tratamento próprio.
- **Limites conscientes:** router e worker-go não reconectam sozinhos: ao perder o broker saem com erro e o compose os reinicia; a transação do relay fica aberta durante o POST.

---

## 8. Como rodar

```bash
make up        # sobe tudo (docker compose up -d --build)
make logs      # acompanha os logs de todos os serviços
make logs-jobs # só o caminho dos jobs (requer jq); JOB=<id> filtra um job
make down      # derruba e apaga volumes
```

Exemplo:

```bash
curl -s -X POST localhost:58001/jobs -H 'content-type: application/json' \
  -d '{"type":"email.send","payload":{"to":"a@b.c"}}'
curl -s localhost:58001/jobs/<job_id>      # status DONE e results[] em poucos segundos
```

Qualidade e testes:

```bash
make ruff      # lint + formato dos serviços Python
make ty        # checagem de tipos (ty) dos serviços Python
make e2e       # teste de ponta a ponta da stack no ar (zera o banco)
make bench     # compara os workers por carga de trabalho (zera o banco, ~5 min)
make vet       # go vet + gofmt dos serviços Go
make swagger   # regenera o OpenAPI do gateway-go
```

Testes rodam por serviço (`go test ./...` ou `uv run pytest -x --tb=short -q <arquivo>`). Os de integração exigem `TEST_DATABASE_URL=postgres://app:app@localhost:55432/app` e **fazem `TRUNCATE` das tabelas**: use só o Postgres de dev do compose. Sem a variável, são pulados.

---

## 9. Estrutura do repositório

```
simple-microservices/
├── services/
│   ├── gateway-py/  gateway-go/  relay/  router/
│   └── worker-celery/  worker-taskiq/  worker-asyncio/  worker-go/  dlq-reaper/
├── e2e/e2e.py                  # `make e2e`: cenários de ponta a ponta
├── e2e/bench.py                # `make bench`: compara os workers (docs/WORKERS.md)
├── contracts/envelope.schema.json
├── db/init.sql
├── infra/rabbitmq/{definitions.json,rabbitmq.conf}
├── docs/{ESTUDO.md,OBSERVABILIDADE.md,COMPARACAO.md,superpowers/{specs,plans}/}
├── docker-compose.yml
├── Makefile
├── CHANGELOG.md
├── AGENTS.md
└── README.md
```

Cada serviço tem `pyproject.toml` (uv) ou `go.mod` e `Dockerfile` próprios; não há imports entre serviços, só o contrato compartilhado.

---

## 10. Fases de implementação (por código)

Cada fase muda **uma linguagem** (ou só infra/docs) e termina com verificação e revisão antes da próxima. Detalhes passo a passo no plano.

| Fase | Entrega | Linguagem | Critério de pronto |
|---|---|---|---|
| 1 | `AGENTS.md` | docs | regras novas em vigor |
| 2 | Postgres, RabbitMQ, contrato, Makefile | infra | tabelas e filas criadas no boot |
| 3 | `router` | Go | testes de roteamento; mensagem chega na fila certa |
| 4 | `relay` | Go | outbox `sent` após 2xx; `pending` se router falha; `FAILED` em 4xx |
| 5 | `gateway-go` | Go | `POST /jobs` grava job + outbox; Swagger no ar |
| 6 | `worker-go` | Go | job `image.resize` chega a `DONE` |
| 7 | `gateway-py` | Python | mesma API do gateway-go |
| 8 | `worker-asyncio` | Python | job `http.fetch` chega a `DONE` |
| 9 | `worker-celery` | Python | job `report.generate` chega a `DONE` |
| 10 | `worker-taskiq` | Python | job `email.send` chega a `DONE` |
| 11 | Verificação ponta a ponta | Python (`e2e/e2e.py`) | `make e2e`: 8 jobs (2 gateways × 4 types) em `DONE`; `type` desconhecido em `FAILED`; router parado não derruba o `POST`; fila sem binding, DLQ e retry após falha transitória (go e asyncio) |
| 12 | Revisão final deste README contra o código | docs | portas, serviços e comandos conferem |

---

## 11. Stack

| Camada | Python | Go |
|---|---|---|
| Gateway | FastAPI + Pydantic + psycopg3 | Gin + pgx + swaggo |
| Relay e router | — | `net/http`, pgx, amqp091-go |
| Workers | Celery, TaskIQ, aio-pika | amqp091-go + goroutines |
| Mensageria | RabbitMQ | RabbitMQ |
| Banco | Postgres | Postgres |
| Orquestração | Docker Compose | Docker Compose |

---

## 12. Melhorias futuras

Ideias que ampliam o propósito de estudo, ordenadas por valor. Nenhuma está implementada; as fora de escopo da seção 1 continuam fora.

### Alto valor, custo baixo

1. **Backoff exponencial e reenvio da DLQ:** o retry atual (go e asyncio) usa atraso fixo de 5 s e máximo de 3 tentativas, e não cobre os bridges Celery/TaskIQ. Atraso crescente (uma fila de espera por degrau) e o reenvio de mensagens da `jobs.dlq` completam o ciclo de falha (hoje o `dlq-reaper` só marca `FAILED`).
2. **Comparar os workers com mais realismo:** `make bench` (feito) usa cargas sintéticas. Falta I/O real (HTTP para um mock com latência), memória/CPU por processo e percentis de latência.

### Valor médio

3. **Tabela de roteamento fora do código:** ler `type → worker` de arquivo, variável de ambiente ou tabela do Postgres, para trocar o worker de um tipo sem rebuild do router.
4. **`trace_id` no envelope:** propagado pelos serviços, além do `job_id`; primeiro passo para OpenTelemetry sem montar Grafana/Prometheus.
5. **Métricas simples:** `GET /metrics` no relay com jobs por status e idade da outbox mais antiga, para saber se ela está acumulando sem abrir o banco.

### Mais perto de produção (se o objetivo mudar)

6. **Pool de conexões nos workers** (`psycopg_pool`) e **graceful shutdown** nos workers Python (os serviços Go já têm). Hoje cada `save` abre uma conexão curta: suficiente para a demo, gargalo só com volume alto.
7. **Idempotency key no `POST /jobs`** (o cliente que reenvia hoje cria um job novo) e **autenticação** nos gateways.
8. **CI** rodando lint e testes.
