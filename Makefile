# Atalhos do projeto. Serviços Go e Python ficam em services/.
GO_SERVICES := gateway-go router relay worker-go

.PHONY: up down logs logs-jobs ruff ty e2e vet swagger

# Sobe tudo (reconstrói imagens).
up:
	docker compose up -d --build

# Derruba tudo e apaga volumes (recria o banco do zero).
down:
	docker compose down -v

logs:
	docker compose logs -f --tail=50

# Só o caminho dos jobs: uma linha por etapa (hora, serviço, mensagem, job_id).
# Filtra o JSON com jq e descarta linhas sem job_id (banner do celery, postgres, rabbitmq).
# Uso: make logs-jobs  |  make logs-jobs JOB=<job_id> para seguir um job só.
logs-jobs:
	docker compose logs -f --since 5m --no-log-prefix \
		gateway-py gateway-go relay router worker-go worker-asyncio worker-celery-bridge worker-celery \
		| jq -rR --unbuffered 'fromjson? | select(.job_id != null and (.job_id | contains("$(JOB)"))) \
		| "\(.time[11:19]) \(.service) \(.msg) \(.job_id)"'

# Lint + formato dos serviços Python.
ruff:
	uvx ruff check services e2e
	uvx ruff format --check services e2e

# Checagem de tipos (ty) em cada serviço Python, usando o .venv do próprio serviço.
ty:
	@for s in gateway-py worker-asyncio worker-celery worker-taskiq; do \
		(cd services/$$s && uvx ty check) || exit 1; \
	done

# Teste e2e da stack no ar (exige `make up`); zera jobs/outbox/resultados.
e2e:
	python3 e2e/e2e.py

# go vet + gofmt em todos os serviços Go.
vet:
	@for s in $(GO_SERVICES); do \
		(cd services/$$s && go vet ./... && test -z "$$(gofmt -l .)") || exit 1; \
	done

# Regenera docs/ (OpenAPI) do gateway-go a partir das anotações swag.
swagger:
	cd services/gateway-go && go run github.com/swaggo/swag/cmd/swag@latest init
