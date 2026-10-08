# AGENTS.md

Visão geral e arquitetura em `README.md`. Projeto de demonstração, minimalista: código enxuto, sem funcionalidade extra.

## Documentação

O `README.md` faz parte da entrega. Serviço criado/removido, endpoint novo, contrato (`contracts/envelope.schema.json`), schema (`db/init.sql`) ou tabela de roteamento alterados atualizam o README no mesmo passo. Doc desatualizada é bug. Mudança notável entra no `CHANGELOG.md` no mesmo passo.

## Regras de código

### Tipagem
- Python: tudo tipado (parâmetros, retornos, atributos). Sem `Any` implícito nem `dict` solto onde um modelo cabe.
- Sintaxe moderna (Python 3.13): `list[Job]`, `X | None`, `StrEnum`, `dataclass(frozen=True, slots=True)`.
- Dados são `dataclass` (domínio) ou `pydantic.BaseModel` (borda). Sem tuplas/dicts anônimos entre camadas.
- Go: structs com tags explícitas, erros retornados e tratados (nunca ignorados com `_`), `context.Context` como primeiro parâmetro em I/O.

### Funções e classes
- Uma responsabilidade por função; nome = verbo + objeto; sem flags booleanas que mudam o comportamento.
- Comportamento com estado ou dependências vive numa classe gerenciadora (Python) ou struct com `New...` (Go), com dependências injetadas. Sem estado global mutável; estado de processo é criado no `lifespan` (FastAPI) ou no `main` (Go).
- Funções livres só para lógica pura.

### Código limpo
- Nomes em inglês; comentários e docstrings em português.
- Todo arquivo tem docstring/comentário de módulo (o que é e por quê); toda classe, função e método tem docstring que explica propósito e limites, não repete o nome. SQL, Makefile, compose e Dockerfile têm comentários equivalentes. `ruff` (regras `D1`) barra docstring ausente em Python.
- Sem código morto, sem `print`. Log estruturado: `log/slog` em JSON (Go) ou `logging` (Python), sempre com `job_id` quando houver job.
- Mudanças mínimas e focadas; não refatore o que não foi pedido.

## Padrões do projeto

- **Contrato único:** todo serviço fala o envelope de `contracts/envelope.schema.json`. Mudança de contrato e de consumidores entra junto.
- **Mesma API nos gateways:** `gateway-py` e `gateway-go` expõem os mesmos endpoints e respostas.
- **Outbox:** gateways nunca publicam no RabbitMQ. Gravam `jobs` + `outbox` na mesma transação; só `relay` → `router` encaminha.
- **Router é o único que conhece workers** (tabela `type → worker`).
- **Idempotência:** workers aceitam o mesmo `job_id` mais de uma vez; resultado por `(job_id, worker)`.
- **Ack explícito:** ack só depois de gravar o resultado (nos bridges Celery/TaskIQ, depois de entregar ao broker do framework, que reconhece tarde). Mensagem inválida: `reject`/`nack` sem requeue.
- **Swagger:** gateways expõem `/docs` com `summary`, `description` e `tags`. No Gin o spec vem das anotações `swag`: mudou handler, rode `make swagger`. Serviços internos (`router`, `relay`, workers) são isentos.
- **Dependências por serviço:** sem imports entre serviços.

## Testes

- Rode só o teste relacionado: `pytest -x --tb=short -q <arquivo>` ou `go test ./...` no serviço. Nunca a suíte inteira por padrão.
- Máximo 2 tentativas no mesmo teste que falha; se continuar, pare e explique.
- Python: `pytest` + `pytest-asyncio` (`asyncio_mode = "auto"`).
- Antes de testes de integração: `docker ps`.
- Só testes que protegem comportamento real (outbox, roteamento, idempotência, validação). Sem testes que espelham a implementação.

## Fluxo de trabalho

- Uma etapa por vez, uma linguagem por etapa: nunca misture alterações Python e Go na mesma etapa.
- Antes de dar uma tarefa como pronta: `make ruff` e `make ty` limpos (Python), `go vet` e `gofmt -l` limpos (Go) e o fluxo afetado exercitado.
- Nunca faça commit ou push; essa responsabilidade é do humano.
