# Changelog

Mudanças notáveis do projeto, mais recentes primeiro.

## [Não lançado]

### 2026-10-07

- **gateway-py:** erros 422 no formato `{"detail": "<texto curto>"}`, igual ao gateway-go. Corrigido 500 no primeiro request depois de restart do Postgres: o pool agora descarta conexões mortas (`check_connection`).
- **gateway-go:** API Gin com a mesma API do gateway-py (`POST /jobs`, `GET /jobs/{id}`), ids UUIDv7, `origin: gateway-go`, Swagger gerado por `make swagger` em `/docs/index.html`. Porta 58002. Erros 422 padronizados em `{"detail": "<texto curto>"}`.
- **gateway-py:** ids em UUIDv7 (`uuid-utils`). API FastAPI com `POST /jobs` (grava `jobs` + `outbox` na mesma transação, responde 202) e `GET /jobs/{id}` (status e resultados dos workers). Swagger em `/docs`. Porta 58001.
- **infra:** Postgres 17 (`db/init.sql`), RabbitMQ 4 com topologia e usuário `guest` em `definitions.json`, contrato `contracts/envelope.schema.json`, `Makefile`, `docker-compose.yml`. Portas do host alternativas (55432, 55672, 55673) para não colidir com o projeto anterior.
- **docs:** `README.md` e `AGENTS.md` reescritos; spec e plano em `docs/superpowers/`.
- **router (Go):** implementado e removido do repositório nesta etapa para isolar o Python. Será refeito na etapa Go.
