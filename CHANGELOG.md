# Changelog

Mudanças notáveis do projeto, mais recentes primeiro.

## [Não lançado]

### 2026-10-07

- **relay (Go):** drena a outbox (`FOR UPDATE SKIP LOCKED`), faz POST ao router e marca `sent`/`failed` pelo status; 5xx ou router fora mantém `pending`. `DISPATCHED` nunca sobrescreve `DONE`. Entra no compose e no `make vet`.
- **router (Go):** webhook `POST /dispatch` que roteia por `type` (tabela fixa) e publica no exchange `jobs` com publisher confirms. Respostas 202/400/422/502. Entra no compose (porta interna 8080) e no `make vet`.
- **docs:** `docs/ESTUDO.md`, guia de estudo da arquitetura e da stack (mecanismos, decisões, armadilhas encontradas, roteiro de prática).
- **gateway-py:** erros 422 no formato `{"detail": "<texto curto>"}`, igual ao gateway-go. Corrigido 500 no primeiro request depois de restart do Postgres: o pool agora descarta conexões mortas (`check_connection`).
- **gateway-go:** API Gin com a mesma API do gateway-py (`POST /jobs`, `GET /jobs/{id}`), ids UUIDv7, `origin: gateway-go`, Swagger gerado por `make swagger` em `/docs/index.html`. Porta 58002. Erros 422 padronizados em `{"detail": "<texto curto>"}`.
- **gateway-py:** ids em UUIDv7 (`uuid-utils`). API FastAPI com `POST /jobs` (grava `jobs` + `outbox` na mesma transação, responde 202) e `GET /jobs/{id}` (status e resultados dos workers). Swagger em `/docs`. Porta 58001.
- **infra:** Postgres 17 (`db/init.sql`), RabbitMQ 4 com topologia e usuário `guest` em `definitions.json`, contrato `contracts/envelope.schema.json`, `Makefile`, `docker-compose.yml`. Portas do host alternativas (55432, 55672, 55673) para não colidir com o projeto anterior.
- **docs:** `README.md` e `AGENTS.md` reescritos; spec e plano em `docs/superpowers/`.
