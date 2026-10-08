# Atalhos do projeto. Serviços Go e Python ficam em services/.
GO_SERVICES := gateway-go router relay worker-go

.PHONY: up down logs ruff vet swagger

# Sobe tudo (reconstrói imagens).
up:
	docker compose up -d --build

# Derruba tudo e apaga volumes (recria o banco do zero).
down:
	docker compose down -v

logs:
	docker compose logs -f --tail=50

# Lint + formato dos serviços Python.
ruff:
	uvx ruff check services
	uvx ruff format --check services

# go vet + gofmt em todos os serviços Go.
vet:
	@for s in $(GO_SERVICES); do \
		(cd services/$$s && go vet ./... && test -z "$$(gofmt -l .)") || exit 1; \
	done

# Regenera docs/ (OpenAPI) do gateway-go a partir das anotações swag.
swagger:
	cd services/gateway-go && go run github.com/swaggo/swag/cmd/swag@latest init
