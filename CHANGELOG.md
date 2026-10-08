# Changelog

Mudanças notáveis do projeto, mais recentes primeiro.

## [Não lançado]

### 2026-10-08

- **router (fix):** publica com `mandatory=true` e trata `basic.return` (`ErrUnroutable`). Antes, routing key sem fila bound era confirmada e descartada, deixando o job `DISPATCHED` para sempre; agora responde 502 e o relay tenta de novo. Teste de integração opcional (`TEST_AMQP_URL`).
- **tipagem (Python):** `make ty` roda o `ty` (Astral) em cada serviço Python com o `.venv` dele. Ajustes para ficar limpo: callbacks do aio-pika tipados com `AbstractIncomingMessage`, `lifespan` do gateway-py retorna `AsyncGenerator` e o teste de repositório usa DSN `str`.

### 2026-10-07

- **docs:** `docs/COMPARACAO.md` compara este projeto ao `microservices` (propósito, arquitetura, robustez, observabilidade, contratos, workers, testes, tamanho) e lista o que trazer ou não do anterior.
- **docs:** README ganha a seção 12 com melhorias futuras (e2e versionado, retry/DLQ, comparação de workers, roteamento configurável, trace id, métricas, pool, idempotency key, CI).
- **gateway-py (logs):** log em JSON no mesmo formato dos demais serviços (`app/logs.py`), incluindo o access log do uvicorn; `POST /jobs` loga `job accepted` com `job_id`. `make logs-jobs` passa a mostrar os jobs que entram pelo gateway-py.
- **worker-taskiq (Python):** bridge aio-pika (`jobs.taskiq` → `process_job.kiq`) e worker TaskIQ assíncrono gravam resultado idempotente e marcam `DONE` para `email.send`. Dois serviços no compose.
- **docs/make:** `make logs-jobs` mostra uma linha por etapa do job (com `JOB=<id>` filtra um job); `docs/OBSERVABILIDADE.md` ensina a acompanhar logs, RabbitMQ e banco e a achar onde um job parou.
- **worker-celery (Python):** bridge aio-pika (`jobs.celery` → `process_job.delay`) e worker Celery (`acks_late`, prefetch 1, 4 processos) gravam resultado idempotente e marcam `DONE` para `report.generate`. Dois serviços no compose; remote control do Celery desligado por causa das filas transient recusadas pelo RabbitMQ atual.
- **router, worker-go (fix):** se o RabbitMQ derruba a conexão, o processo sai com erro e o compose o reinicia. Antes o router continuava vivo respondendo 502 indefinidamente.
- **worker-asyncio (Python):** consome `jobs.asyncio` (`http.fetch`) com aio-pika, grava resultado idempotente e marca `DONE`; ack só após gravar. Logs em JSON (`app/logs.py`) no mesmo formato dos serviços Go. Entra no compose.
- **logs (Go):** gateway-go, router, relay e worker-go passam a logar em JSON com `log/slog`, com `service` e `job_id` em cada etapa; gateway-go em modo release com access log no mesmo formato. Falhas esperadas (type desconhecido, rejeição, retry) agora aparecem nos logs.
- **worker-go (Go):** consome `jobs.go` com uma goroutine por mensagem (limitada pelo prefetch), grava `job_results` de forma idempotente e marca o job `DONE`; ack só após gravar. Entra no compose e no `make vet`.
- **relay (Go):** drena a outbox (`FOR UPDATE SKIP LOCKED`), faz POST ao router e marca `sent`/`failed` pelo status; 5xx ou router fora mantém `pending`. `DISPATCHED` nunca sobrescreve `DONE`. Entra no compose e no `make vet`.
- **router (Go):** webhook `POST /dispatch` que roteia por `type` (tabela fixa) e publica no exchange `jobs` com publisher confirms. Respostas 202/400/422/502. Entra no compose (porta interna 8080) e no `make vet`.
- **docs:** `docs/ESTUDO.md`, guia de estudo da arquitetura e da stack (mecanismos, decisões, armadilhas encontradas, roteiro de prática).
- **gateway-py:** erros 422 no formato `{"detail": "<texto curto>"}`, igual ao gateway-go. Corrigido 500 no primeiro request depois de restart do Postgres: o pool agora descarta conexões mortas (`check_connection`).
- **gateway-go:** API Gin com a mesma API do gateway-py (`POST /jobs`, `GET /jobs/{id}`), ids UUIDv7, `origin: gateway-go`, Swagger gerado por `make swagger` em `/docs/index.html`. Porta 58002. Erros 422 padronizados em `{"detail": "<texto curto>"}`.
- **gateway-py:** ids em UUIDv7 (`uuid-utils`). API FastAPI com `POST /jobs` (grava `jobs` + `outbox` na mesma transação, responde 202) e `GET /jobs/{id}` (status e resultados dos workers). Swagger em `/docs`. Porta 58001.
- **infra:** Postgres 17 (`db/init.sql`), RabbitMQ 4 com topologia e usuário `guest` em `definitions.json`, contrato `contracts/envelope.schema.json`, `Makefile`, `docker-compose.yml`. Portas do host alternativas (55432, 55672, 55673) para não colidir com o projeto anterior.
- **docs:** `README.md` e `AGENTS.md` reescritos; spec e plano em `docs/superpowers/`.
