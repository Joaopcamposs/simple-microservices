# Comparação: `simple-microservices` × `microservices`

Este documento compara os dois projetos do mesmo autor:

- **`microservices`** (anterior): laboratório que mede Python (Celery, TaskIQ, asyncio) contra Go sob a mesma carga, com observabilidade e benchmark.
- **`simple-microservices`** (este): destilação didática do mesmo desenho, para entender a arquitetura desacoplada sem o peso do laboratório.

> Fonte da comparação: código e README dos dois repositórios. Números de linhas são aproximados (Python + Go, incluindo testes, sem `.venv` nem spec gerado do Swagger).

## 1. Propósito

| | `microservices` | `simple-microservices` |
|---|---|---|
| Pergunta que responde | "Qual stack é mais rápida, leve e confiável sob cada tipo de carga?" | "Como desacoplar gateway e workers sem perder job?" |
| Entrega | Resultados de benchmark (vazão, p50/p95/p99, CPU, memória, footprint) | Arquitetura legível e verificável de ponta a ponta |
| Público | Quem decide stack com dados | Quem estuda o padrão (outbox, roteamento, idempotência, ack) |
| Escopo | Amplo, com workloads reais | Mínimo: um tipo de job por worker, sem funcionalidade extra |

## 2. Arquitetura lado a lado

| Aspecto | `microservices` | `simple-microservices` |
|---|---|---|
| Entrada | `gateway-py` (FastAPI) e `gateway-go` (Gin) | Idem, mesma API nos dois |
| Persistência de intenção | Outbox transacional (`jobs` + `outbox`) | Idem |
| Publicação | `outbox-relay` publica direto no RabbitMQ | `relay` entrega por HTTP ao `router`; só o `router` publica no RabbitMQ |
| Quem conhece os workers | Exchange fanout `jobs` ou direct `jobs.direct` escolhido por `target` no pedido | Tabela `type → worker` isolada no `router` |
| Topologia RabbitMQ | Fanout (todos recebem) + modo direct (um worker) | Direct exchange `jobs`, routing key = worker |
| Workers | celery, taskiq, asyncio, go (bridge nos dois frameworks) | Idem |
| Resultado | `job_results` por `(job_id, worker)`, com `status` succeeded/failed | `job_results` por `(job_id, worker)`; status do job (`PENDING → DISPATCHED → DONE\|FAILED`) em `jobs` |
| Contrato | `envelope.schema.json` + schema por tipo de job em `contracts/jobs/*` | `envelope.schema.json` único |
| Orquestração | docker-compose com profiles `python`, `go`, `all` | docker-compose único |

Diferença estrutural mais importante: o novo introduz um **hop a mais** (relay → router por HTTP) para concentrar o roteamento num único serviço e dar ao relay um contrato de status claro. O anterior resolve o roteamento na exchange e no campo `target`.

## 3. Garantias e robustez

| Tema | `microservices` | `simple-microservices` |
|---|---|---|
| Entrega | At-least-once (outbox + confirm) | At-least-once (outbox + publisher confirms no router) |
| Concorrência do relay | `FOR UPDATE SKIP LOCKED`, lote de 100 | `FOR UPDATE SKIP LOCKED` |
| Idempotência | `ON CONFLICT` em `(job_id, worker)` | Idem |
| Publicação sem destino | `mandatory=true` + `NotifyReturn`: mensagem sem fila é detectada | Idem (`mandatory=true` + `NotifyReturn` no router): vira `ErrUnroutable` → 502 → outbox `pending`, tenta de novo |
| Mensagem inválida | Rejeitada para DLQ (`jobs.dlx → jobs.dlq`) | `reject`/`nack` sem requeue, com DLQ (`jobs.dlx → jobs.dlq`); nada consome a DLQ nem marca o job `FAILED` |
| Retry | Campo `attempt` no envelope | Não há; falha vira `FAILED` |
| Falha do broker | Relay reconecta com backoff | Router e worker-go saem com erro; o compose reinicia. Workers Python usam `connect_robust` |
| Falha de entrega ao router | Não se aplica | Contrato explícito: 202 → `sent`/`DISPATCHED`; 400/422 → `failed`/`FAILED`; 5xx/rede → `pending` (tenta de novo) |
| Tipo desconhecido | Validado por schema do job no gateway | Router não acha worker → 422 → `FAILED` visível em `GET /jobs/{id}` |
| Healthcheck do relay | `/healthz` e `/metrics` | Sem endpoint |
| Purge/limpeza da outbox | Tem | Não tem |

Resumo: o anterior é mais robusto na **borda do broker** (DLQ, `mandatory`, reconexão, métricas). O novo é mais claro na **borda do roteamento** (falha de tipo vira estado visível no job).

## 4. Observabilidade

| | `microservices` | `simple-microservices` |
|---|---|---|
| Traces | OpenTelemetry + Jaeger, `traceparent` no envelope atravessando Python e Go | Não há |
| Métricas | Prometheus + Grafana + cAdvisor | Não há |
| Logs | Estruturados nos serviços | JSON em todos os serviços (`slog` em Go; `JsonFormatter` em Python) com `job_id`; `make logs-jobs JOB=<id>` correlaciona o caminho do job |
| Guia de acompanhamento | Parcial no README | `docs/OBSERVABILIDADE.md` com passo a passo, diagnóstico por status e testes de robustez |

O novo troca instrumentação pesada por um **fluxo de depuração via logs** que cabe numa tela. Para benchmark, o anterior é indispensável; para estudo, o do novo basta.

## 5. Contratos e jobs

| | `microservices` | `simple-microservices` |
|---|---|---|
| Envelope | `job_id`, `type`, `payload`, `created_at`, `traceparent`, `attempt`, `origin` | `job_id`, `type`, `payload`, `created_at`, `origin` (sem `traceparent` nem `attempt`) |
| Tipos de job | `cpu.pbkdf2`, `io.fetch_urls`, `io.sleep`, `data.json_transform` (e `pipeline.fanout` previsto) | Um tipo por worker, só para exercitar o caminho: `report.generate` (celery), `email.send` (taskiq), `http.fetch` (asyncio), `image.resize` (go) |
| Validação de payload | Schema por tipo, lido pelos dois gateways | Validação do envelope pelo contrato único |
| Resultados idênticos entre stacks | Golden vectors em `contracts/jobs/examples.json`, testados nos 4 workers | Não se aplica |
| Mock de I/O | `mock-server` com latência e status controlados | Não há |

## 6. Workers

Os dois usam o padrão bridge para Celery e TaskIQ (consumidor fino lê a fila do contrato e entrega ao framework). Diferenças:

| | `microservices` | `simple-microservices` |
|---|---|---|
| Ack | Documentado por stack | Regra única em `AGENTS.md`: ack só depois de gravar (bridges: depois de entregar ao broker do framework, que reconhece tarde) |
| Celery | Pool de threads para I/O, processo filho para CPU | `acks_late`, prefetch 1, remote control off, `--without-mingle --without-gossip` (evita loop de reconexão no RabbitMQ) |
| TaskIQ | Idem ao asyncio com `to_thread` | `AioPikaBroker`, `kiq` assíncrono, fila padrão `taskiq` |
| Armazenamento | Store por worker | Store síncrono no Celery, assíncrono no taskiq e asyncio |
| Foco | Medir throughput | Mostrar o mesmo contrato sendo consumido por quatro modelos de execução |

## 7. Testes e verificação

| | `microservices` | `simple-microservices` |
|---|---|---|
| Unitários | Handlers, golden vectors, stores | Store idempotente, roteamento, validação |
| Integração/e2e | Benchmark com cenários e perdas contadas | Roteiro e2e manual: 8 jobs `DONE`, tipo `nope` → `FAILED`, router parado → `PENDING` → `DONE` |
| Benchmark | `bench/lab_bench` (driver próprio, 5 rodadas, mediana, desvio) | Não há |
| Qualidade estática | `ruff`, `go vet` | `make ruff` (com regras `D1`), `go vet`, `gofmt -l` |

## 8. Tamanho e esforço de leitura

| | `microservices` | `simple-microservices` |
|---|---|---|
| Código (Python + Go) | ~10 mil linhas | ~2,5 mil linhas |
| Infra extra | OTel Collector, Jaeger, Prometheus, Grafana, cAdvisor | Postgres + RabbitMQ |
| Documentação | README extenso, `docs/decisoes.md` | README, `docs/ESTUDO.md`, `docs/OBSERVABILIDADE.md`, `AGENTS.md`, `CHANGELOG.md` |
| Tempo para entender o fluxo | Horas | Uma sessão |

## 9. O que cada um faz melhor

**`microservices`**
- Mede de verdade: vazão, latência por percentil, CPU, memória, footprint e recuperação.
- Tem DLQ, `mandatory`, reconexão com backoff, `/healthz` e `/metrics` no relay.
- Workloads com resultado idêntico verificado entre quatro stacks.
- Trace distribuído atravessando Python e Go.

**`simple-microservices`**
- Roteamento centralizado: só o `router` conhece workers; adicionar worker é editar uma tabela.
- Contrato relay↔router explícito, com estados que aparecem para quem consulta o job.
- Regras de código e de fluxo (`AGENTS.md`) aplicadas: tipagem, docstrings, sem estado global, uma linguagem por etapa.
- Documentação de estudo e de operação sobre cada mecanismo e cada falha testada.
- Logs correlacionáveis por `job_id` com um comando.

## 10. Quando usar cada um

| Objetivo | Projeto |
|---|---|
| Escolher entre Celery, TaskIQ, asyncio e Go com números | `microservices` |
| Aprender outbox, roteamento, idempotência e ack | `simple-microservices` |
| Base para um serviço real | Novo como ponto de partida, trazendo DLQ, `mandatory` e métricas do anterior |
| Demonstrar o padrão a alguém em 30 min | `simple-microservices` |

## 11. O que trazer do anterior para o novo

Ordem sugerida, por retorno sobre custo (ver também a seção "Melhorias futuras" do README):

1. ~~**DLQ**~~ feito em 2026-10-08 (falta consumidor/reenvio).
2. ~~**`mandatory=true` + tratamento de `Return` no router**~~ feito em 2026-10-08.
3. **`/healthz` no relay e no router**, com `depends_on: condition: service_healthy` no compose.
4. **Teste e2e automatizado** reproduzindo o roteiro manual atual.
5. **Retry com contador (`attempt`)** quando houver falha transitória no worker.

## 12. O que não trazer

- **Stack de observabilidade completa** (OTel, Jaeger, Prometheus, Grafana, cAdvisor): descaracteriza o propósito mínimo; os logs correlacionados resolvem o estudo.
- **Benchmark e mock-server:** pertencem ao laboratório.
- **Modo fanout:** o novo escolheu uma exchange direct; mudar isso afeta o desenho do router.
- **Schemas por tipo de job e golden vectors:** só fazem sentido com workloads reais.

## 13. Análise geral

Os projetos não competem: o anterior é a **medição**, o novo é a **explicação**. O novo é mais fácil de ler, mais fácil de operar e documenta melhor as decisões; o anterior é mais forte onde o sistema encontra o mundo real (broker instável, mensagem sem destino, carga alta, necessidade de medir).

A maior fraqueza do novo é que nada consome a DLQ: mensagem inválida fica guardada, mas o job continua `DISPATCHED` e não há retry. A maior fraqueza do anterior para estudo é o volume: o mecanismo essencial (outbox → broker → worker idempotente) fica diluído entre benchmark, observabilidade e workloads.
