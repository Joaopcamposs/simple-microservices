# Changelog

Mudanças notáveis do projeto, mais recentes primeiro.

## [Não lançado]

### 2026-10-07

- **gateway-py:** ids em UUIDv7 (`uuid-utils`). API FastAPI com `POST /jobs` (grava `jobs` + `outbox` na mesma transação, responde 202) e `GET /jobs/{id}` (status e resultados dos workers). Swagger em `/docs`. Porta 58001.
- **infra:** Postgres 17 (`db/init.sql`), RabbitMQ 4 com topologia e usuário `guest` em `definitions.json`, contrato `contracts/envelope.schema.json`, `Makefile`, `docker-compose.yml`. Portas do host alternativas (55432, 55672, 55673) para não colidir com o projeto anterior.
- **docs:** `README.md` e `AGENTS.md` reescritos; spec e plano em `docs/superpowers/`.
- **router (Go):** implementado e removido do repositório nesta etapa para isolar o Python. Será refeito na etapa Go.
