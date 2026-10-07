# Simple Microservices Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Demo minimalista: gateways Python/Go gravam outbox, relay entrega a um router (webhook) que escolhe o worker (Celery, TaskIQ, asyncio, goroutine) via RabbitMQ.

**Architecture:** Gateway grava `jobs` + `outbox` numa transação (Postgres). `relay` (Go) faz poll da outbox e dá `POST /dispatch` no `router` (Go). O `router` mapeia `type → worker` e publica no exchange direct `jobs` do RabbitMQ; cada worker consome sua fila e grava `job_results` (idempotente).

**Tech Stack:** Go 1.24 (Gin, pgx/v5, amqp091-go, swaggo), Python 3.13 (FastAPI, psycopg3, aio-pika, Celery, TaskIQ, uv, ruff, pytest-asyncio), Postgres 17, RabbitMQ 4, Docker Compose.

**Spec:** `docs/superpowers/specs/2026-10-07-simple-microservices-design.md`

## Global Constraints

- **Isolamento (pedido do usuário):** cada task toca **uma só linguagem** (Go, Python ou infra/docs). Nunca misturar alterações Python e Go na mesma task. Execute **uma task por vez** e pare ao final de cada uma para revisão.
- **Sem commit:** `AGENTS.md` proíbe o agente de commitar. O passo final de cada task é "Checkpoint": pare e avise o humano (que revisa e commita).
- Envelope único em `contracts/envelope.schema.json`: campos `job_id`, `type`, `payload`, `created_at`, `origin` (sem `traceparent`/`attempt`).
- Nenhuma dependência cruzada entre serviços além de `contracts/`; cada serviço tem `go.mod` ou `pyproject.toml` e `Dockerfile` próprios.
- Python 3.13, tipagem completa; Go com erros tratados e `context.Context` primeiro em I/O.
- Nomes em inglês; comentários e docstrings em português; todo arquivo tem docstring/comentário de módulo; toda função/classe tem docstring.
- Gateways nunca falam com RabbitMQ. Só o `router` publica.
- Resultado de worker: tabela `job_results` com PK `(job_id, worker)` e `ON CONFLICT DO NOTHING`. Formato do `result`: `{"worker": "<nome>", "detail": "<texto>"}`.
- Tabela de roteamento fixa: `report.generate→celery`, `email.send→taskiq`, `http.fetch→asyncio`, `image.resize→go`.
- Testes: `pytest -x --tb=short -q <arquivo>` (Python) e `go test ./...` dentro do serviço (Go). Só os testes da task. Máx. 2 tentativas no mesmo teste; se persistir, pare e explique.
- Testes de integração exigem `TEST_DATABASE_URL` (e fazem `TRUNCATE` das tabelas: **só usar no Postgres de dev do compose**); sem a variável, são pulados. Antes deles: `docker ps`.
- URL de dev do Postgres: `postgres://app:app@localhost:5432/app`. AMQP: `amqp://guest:guest@localhost:5672/`.

## Review Focus

Entradas/falhas que o spec implica e nenhum teste óbvio cobriria; cada linha tem teste na task dona:

1. `POST /jobs` com `type` vazio/ausente → 422; `payload` ausente → vira `{}` (Tasks 5 e 7).
2. `type` desconhecido no router → 422; relay marca outbox `failed` e job `FAILED`, nunca fica `PENDING` (Tasks 3 e 4).
3. Router fora do ar/5xx: relay mantém `pending` e não perde a linha (Task 4).
4. Job já `DONE` (worker rápido) não regride para `DISPATCHED` quando o relay marca a entrega (Task 4).
5. Mesmo `job_id` entregue duas vezes ao worker → uma linha em `job_results`, status `DONE` (Tasks 6, 8, 9, 10).

---

### Task 1: Reescrever AGENTS.md (docs)

**Files:**
- Modify: `AGENTS.md`

**Interfaces:**
- Produces: regras do projeto que as demais tasks seguem.

- [ ] **Step 1: Substituir o conteúdo de `AGENTS.md`**

````markdown
# AGENTS.md

Visão geral e arquitetura em `README.md`. Projeto de demonstração, minimalista: código enxuto, sem funcionalidade extra.

## Documentação

O `README.md` faz parte da entrega. Serviço criado/removido, endpoint novo, contrato (`contracts/envelope.schema.json`), schema (`db/init.sql`) ou tabela de roteamento alterados atualizam o README no mesmo passo. Doc desatualizada é bug. Não há CHANGELOG.

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
- Sem código morto, sem `print` (use `logging`/`log`).
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
- Antes de dar uma tarefa como pronta: `make ruff` limpo (Python), `go vet` e `gofmt -l` limpos (Go) e o fluxo afetado exercitado.
- Nunca faça commit ou push; essa responsabilidade é do humano.
````

- [ ] **Step 2: Checkpoint** — pare; humano revisa/commita.

---

### Task 2: Infra base (Postgres, RabbitMQ, contrato, Makefile)

**Files:**
- Create: `db/init.sql`, `contracts/envelope.schema.json`, `infra/rabbitmq/definitions.json`, `infra/rabbitmq/rabbitmq.conf`, `docker-compose.yml`, `Makefile`, `.gitignore`

**Interfaces:**
- Produces: tabelas `jobs`, `outbox`, `job_results`; exchange `jobs` (direct) e filas `jobs.{celery,taskiq,asyncio,go}` com routing key = nome do worker; DSN de compose `postgres://app:app@postgres:5432/app`, AMQP `amqp://guest:guest@rabbitmq:5672/`.

- [ ] **Step 1: `db/init.sql`**

```sql
-- Schema compartilhado. Carregado pelo Postgres no primeiro boot
-- (/docker-entrypoint-initdb.d). Sem migrations: demo.

-- Um job recebido por um gateway. status: PENDING -> DISPATCHED -> DONE | FAILED.
CREATE TABLE jobs (
    id         uuid        PRIMARY KEY,
    type       text        NOT NULL,
    payload    jsonb       NOT NULL,
    status     text        NOT NULL DEFAULT 'PENDING',
    origin     text        NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);

-- Outbox transacional: gravada na mesma transação que o job.
-- status: pending -> sent | failed. Só o relay a consome.
CREATE TABLE outbox (
    id         bigserial   PRIMARY KEY,
    job_id     uuid        NOT NULL REFERENCES jobs (id),
    envelope   jsonb       NOT NULL,
    status     text        NOT NULL DEFAULT 'pending',
    created_at timestamptz NOT NULL DEFAULT now()
);

-- Índice parcial: o relay só olha linhas pendentes, em ordem de id.
CREATE INDEX outbox_pending_idx ON outbox (id) WHERE status = 'pending';

-- Resultado por worker. A PK composta torna a gravação idempotente
-- (entrega at-least-once pode repetir o mesmo job_id).
CREATE TABLE job_results (
    job_id      uuid        NOT NULL REFERENCES jobs (id),
    worker      text        NOT NULL,
    result      jsonb       NOT NULL,
    finished_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (job_id, worker)
);
```

- [ ] **Step 2: `contracts/envelope.schema.json`**

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "title": "Envelope",
  "description": "Mensagem neutra trocada entre gateway, relay, router e workers.",
  "type": "object",
  "required": ["job_id", "type", "payload", "created_at", "origin"],
  "additionalProperties": false,
  "properties": {
    "job_id": { "type": "string", "format": "uuid" },
    "type": { "type": "string", "minLength": 1 },
    "payload": { "type": "object" },
    "created_at": { "type": "string", "format": "date-time" },
    "origin": { "type": "string", "enum": ["gateway-py", "gateway-go"] }
  }
}
```

- [ ] **Step 3: `infra/rabbitmq/definitions.json`** (JSON não aceita comentários; a topologia está descrita no README)

```json
{
  "vhosts": [{ "name": "/" }],
  "exchanges": [
    { "name": "jobs", "vhost": "/", "type": "direct", "durable": true, "auto_delete": false, "internal": false, "arguments": {} }
  ],
  "queues": [
    { "name": "jobs.celery", "vhost": "/", "durable": true, "auto_delete": false, "arguments": {} },
    { "name": "jobs.taskiq", "vhost": "/", "durable": true, "auto_delete": false, "arguments": {} },
    { "name": "jobs.asyncio", "vhost": "/", "durable": true, "auto_delete": false, "arguments": {} },
    { "name": "jobs.go", "vhost": "/", "durable": true, "auto_delete": false, "arguments": {} }
  ],
  "bindings": [
    { "source": "jobs", "vhost": "/", "destination": "jobs.celery", "destination_type": "queue", "routing_key": "celery", "arguments": {} },
    { "source": "jobs", "vhost": "/", "destination": "jobs.taskiq", "destination_type": "queue", "routing_key": "taskiq", "arguments": {} },
    { "source": "jobs", "vhost": "/", "destination": "jobs.asyncio", "destination_type": "queue", "routing_key": "asyncio", "arguments": {} },
    { "source": "jobs", "vhost": "/", "destination": "jobs.go", "destination_type": "queue", "routing_key": "go", "arguments": {} }
  ]
}
```

- [ ] **Step 4: `infra/rabbitmq/rabbitmq.conf`**

```
# Carrega exchanges, filas e bindings no boot; nenhum serviço declara topologia.
load_definitions = /etc/rabbitmq/definitions.json
```

- [ ] **Step 5: `docker-compose.yml` (só infra por enquanto)**

```yaml
# Demo: gateways -> outbox -> relay -> router -> RabbitMQ -> workers.
# Serviços de aplicação são adicionados nas tasks de cada serviço.
services:
  postgres:
    image: postgres:17-alpine
    environment:
      POSTGRES_USER: app
      POSTGRES_PASSWORD: app
      POSTGRES_DB: app
    ports: ["5432:5432"]
    volumes:
      # init.sql roda só no primeiro boot (volume vazio).
      - ./db/init.sql:/docker-entrypoint-initdb.d/init.sql:ro
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U app"]
      interval: 3s
      retries: 20

  rabbitmq:
    image: rabbitmq:4-management-alpine
    ports: ["5672:5672", "15672:15672"]
    volumes:
      - ./infra/rabbitmq/definitions.json:/etc/rabbitmq/definitions.json:ro
      - ./infra/rabbitmq/rabbitmq.conf:/etc/rabbitmq/conf.d/20-definitions.conf:ro
    healthcheck:
      test: ["CMD", "rabbitmq-diagnostics", "-q", "ping"]
      interval: 5s
      retries: 20
```

- [ ] **Step 6: `Makefile`** (indentar receitas com TAB)

```make
# Atalhos do projeto. Serviços Go e Python ficam em services/.
GO_SERVICES := router relay gateway-go worker-go

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
```

- [ ] **Step 7: `.gitignore`**

```
.venv/
__pycache__/
.pytest_cache/
.ruff_cache/
.idea/
*.test
```

- [ ] **Step 8: Verificar infra**

Run: `docker compose up -d postgres rabbitmq && sleep 15 && docker compose ps`
Expected: ambos `healthy`.
Run: `curl -s -u guest:guest localhost:15672/api/queues | python3 -c "import sys,json; print(sorted(q['name'] for q in json.load(sys.stdin)))"`
Expected: `['jobs.asyncio', 'jobs.celery', 'jobs.go', 'jobs.taskiq']`
Run: `docker compose exec postgres psql -U app -c '\dt'`
Expected: `jobs`, `outbox`, `job_results`.

- [ ] **Step 9: Checkpoint** — pare; humano revisa/commita.

---

### Task 3: router (Go)

**Files:**
- Create: `services/router/{go.mod,main.go,routes.go,handler.go,publisher.go,handler_test.go,Dockerfile}`
- Modify: `docker-compose.yml` (adicionar serviço `router`)

**Interfaces:**
- Produces: `POST /dispatch` (body = envelope JSON) → `202` publicado; `400` JSON inválido ou `type` vazio; `422` type desconhecido; `502` falha de publish. Erros: `{"detail": "..."}`. Tipos: `Worker`, `Routes`, `Publisher{Publish(ctx, Worker, []byte) error}`.

- [ ] **Step 1: Inicializar módulo**

Run: `cd services/router && go mod init router`

- [ ] **Step 2: Teste que falha — `handler_test.go`**

```go
package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// fakePublisher registra o que o handler tentou publicar.
type fakePublisher struct {
	calls  int
	worker Worker
	body   []byte
	err    error
}

func (f *fakePublisher) Publish(_ context.Context, w Worker, b []byte) error {
	f.calls++
	f.worker, f.body = w, b
	return f.err
}

func dispatch(pub Publisher, body string) *httptest.ResponseRecorder {
	h := NewDispatchHandler(DefaultRoutes(), pub)
	req := httptest.NewRequest(http.MethodPost, "/dispatch", strings.NewReader(body))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestDispatchRoutesByType(t *testing.T) {
	cases := map[string]Worker{
		"report.generate": "celery",
		"email.send":      "taskiq",
		"http.fetch":      "asyncio",
		"image.resize":    "go",
	}
	for typ, want := range cases {
		t.Run(typ, func(t *testing.T) {
			pub := &fakePublisher{}
			body := fmt.Sprintf(`{"job_id":"j1","type":%q}`, typ)
			rec := dispatch(pub, body)
			if rec.Code != http.StatusAccepted {
				t.Fatalf("status = %d, want 202", rec.Code)
			}
			if pub.worker != want {
				t.Fatalf("worker = %q, want %q", pub.worker, want)
			}
			if string(pub.body) != body {
				t.Fatalf("body alterado: %s", pub.body)
			}
		})
	}
}

func TestDispatchUnknownTypeIs422(t *testing.T) {
	pub := &fakePublisher{}
	rec := dispatch(pub, `{"job_id":"j1","type":"nope"}`)
	if rec.Code != http.StatusUnprocessableEntity || pub.calls != 0 {
		t.Fatalf("status = %d calls = %d", rec.Code, pub.calls)
	}
}

func TestDispatchInvalidBodyIs400(t *testing.T) {
	for _, body := range []string{`not json`, `{"job_id":"j1"}`, `{"job_id":"j1","type":""}`} {
		pub := &fakePublisher{}
		rec := dispatch(pub, body)
		if rec.Code != http.StatusBadRequest || pub.calls != 0 {
			t.Fatalf("body %q: status = %d calls = %d", body, rec.Code, pub.calls)
		}
	}
}

func TestDispatchPublishFailureIs502(t *testing.T) {
	pub := &fakePublisher{err: errors.New("broker down")}
	rec := dispatch(pub, `{"job_id":"j1","type":"email.send"}`)
	if rec.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502", rec.Code)
	}
}
```

- [ ] **Step 3: Rodar e ver falhar**

Run: `cd services/router && go test ./...`
Expected: FAIL (`undefined: Worker`, `NewDispatchHandler`...).

- [ ] **Step 4: `routes.go`**

```go
// Package main é o router: webhook que decide para qual worker cada job vai.
// routes.go guarda a tabela type -> worker, único lugar que conhece workers.
package main

// Worker é o consumidor de destino; vira routing key no exchange "jobs".
type Worker string

// Routes mapeia o type de um job para o worker que o processa.
type Routes map[string]Worker

// DefaultRoutes devolve a tabela de roteamento fixa da demo.
func DefaultRoutes() Routes {
	return Routes{
		"report.generate": "celery",
		"email.send":      "taskiq",
		"http.fetch":      "asyncio",
		"image.resize":    "go",
	}
}

// Lookup devolve o worker de um type; false se o type é desconhecido.
func (r Routes) Lookup(jobType string) (Worker, bool) {
	w, ok := r[jobType]
	return w, ok
}
```

- [ ] **Step 5: `handler.go`**

```go
package main

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
)

// maxBodyBytes limita o envelope aceito (proteção simples contra abuso).
const maxBodyBytes = 1 << 20

// Publisher entrega uma mensagem ao broker para o worker indicado.
type Publisher interface {
	Publish(ctx context.Context, worker Worker, body []byte) error
}

// DispatchHandler implementa POST /dispatch: valida, roteia e publica.
type DispatchHandler struct {
	routes    Routes
	publisher Publisher
}

// NewDispatchHandler cria o handler com a tabela e o publisher injetados.
func NewDispatchHandler(routes Routes, publisher Publisher) *DispatchHandler {
	return &DispatchHandler{routes: routes, publisher: publisher}
}

// routeKey são os únicos campos do envelope que o router precisa ler.
// O corpo original é republicado intacto.
type routeKey struct {
	JobID string `json:"job_id"`
	Type  string `json:"type"`
}

// ServeHTTP lê o envelope, escolhe o worker pelo type e publica o corpo original.
func (h *DispatchHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	if err != nil {
		writeError(w, http.StatusBadRequest, "unreadable body")
		return
	}
	var key routeKey
	if err := json.Unmarshal(body, &key); err != nil || key.Type == "" {
		writeError(w, http.StatusBadRequest, "invalid envelope")
		return
	}
	worker, ok := h.routes.Lookup(key.Type)
	if !ok {
		writeError(w, http.StatusUnprocessableEntity, "unknown job type: "+key.Type)
		return
	}
	if err := h.publisher.Publish(r.Context(), worker, body); err != nil {
		log.Printf("publish job %s to %s: %v", key.JobID, worker, err)
		writeError(w, http.StatusBadGateway, "broker unavailable")
		return
	}
	w.WriteHeader(http.StatusAccepted)
}

// writeError responde no formato {"detail": "..."} usado por toda a demo.
func writeError(w http.ResponseWriter, status int, detail string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(map[string]string{"detail": detail}); err != nil {
		log.Printf("write error response: %v", err)
	}
}
```

- [ ] **Step 6: `publisher.go`**

```go
package main

import (
	"context"
	"errors"
	"fmt"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
)

// exchangeName é o exchange direct declarado em infra/rabbitmq/definitions.json.
const exchangeName = "jobs"

// AMQPPublisher publica no RabbitMQ com publisher confirms.
// O canal AMQP não é seguro para uso concorrente, então um mutex serializa os publishes.
type AMQPPublisher struct {
	mu   sync.Mutex
	conn *amqp.Connection
	ch   *amqp.Channel
}

// NewAMQPPublisher conecta ao broker e liga o modo de confirmação.
// Não reconecta sozinho: o compose reinicia o processo (restart: unless-stopped).
func NewAMQPPublisher(url string) (*AMQPPublisher, error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, fmt.Errorf("dial amqp: %w", err)
	}
	ch, err := conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("open channel: %w", err)
	}
	if err := ch.Confirm(false); err != nil {
		return nil, fmt.Errorf("enable confirms: %w", err)
	}
	return &AMQPPublisher{conn: conn, ch: ch}, nil
}

// Publish envia o corpo ao exchange com routing key = worker e espera o confirm.
// mandatory=false: a topologia é fixa, então não tratamos mensagens devolvidas.
func (p *AMQPPublisher) Publish(ctx context.Context, worker Worker, body []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	conf, err := p.ch.PublishWithDeferredConfirmWithContext(ctx, exchangeName, string(worker), false, false, amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		Body:         body,
	})
	if err != nil {
		return fmt.Errorf("publish: %w", err)
	}
	acked, err := conf.WaitContext(ctx)
	if err != nil {
		return fmt.Errorf("wait confirm: %w", err)
	}
	if !acked {
		return errors.New("broker nack")
	}
	return nil
}

// Close encerra canal e conexão.
func (p *AMQPPublisher) Close() error {
	return p.conn.Close()
}
```

- [ ] **Step 7: `main.go`**

```go
package main

import (
	"log"
	"net/http"
	"os"
)

// env lê uma variável de ambiente com valor padrão.
func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// run monta as dependências e sobe o servidor HTTP.
func run() error {
	pub, err := NewAMQPPublisher(env("AMQP_URL", "amqp://guest:guest@localhost:5672/"))
	if err != nil {
		return err
	}
	defer func() {
		if err := pub.Close(); err != nil {
			log.Printf("close publisher: %v", err)
		}
	}()
	mux := http.NewServeMux()
	mux.Handle("POST /dispatch", NewDispatchHandler(DefaultRoutes(), pub))
	addr := env("ADDR", ":8080")
	log.Printf("router listening on %s", addr)
	return http.ListenAndServe(addr, mux)
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}
```

- [ ] **Step 8: Dependências e testes**

Run: `cd services/router && go get github.com/rabbitmq/amqp091-go && go mod tidy && go vet ./... && test -z "$(gofmt -l .)" && go test ./...`
Expected: PASS.

- [ ] **Step 9: `Dockerfile`**

```dockerfile
# Build multi-stage: binário estático em imagem mínima.
FROM golang:1.24-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/app .

FROM alpine:3.21
COPY --from=build /out/app /app
ENTRYPOINT ["/app"]
```

- [ ] **Step 10: Adicionar ao `docker-compose.yml`** (sob `services:`)

```yaml
  router:
    build: ./services/router
    restart: unless-stopped
    environment:
      AMQP_URL: amqp://guest:guest@rabbitmq:5672/
    depends_on:
      rabbitmq: { condition: service_healthy }
```

- [ ] **Step 11: Verificar de ponta a ponta o router**

Run: `docker compose up -d --build router` e, de dentro da rede: `docker compose exec router wget -qO- --post-data='{"job_id":"j1","type":"email.send","payload":{}}' --header='Content-Type: application/json' http://localhost:8080/dispatch; echo $?`
Expected: exit `0`; `curl -s -u guest:guest localhost:15672/api/queues/%2F/jobs.taskiq | grep -o '"messages":[0-9]*'` → `"messages":1`. Depois esvazie: `docker compose exec rabbitmq rabbitmqctl purge_queue jobs.taskiq`.

- [ ] **Step 12: Checkpoint** — pare; humano revisa/commita.

---

### Task 4: relay (Go)

**Files:**
- Create: `services/relay/{go.mod,main.go,dispatcher.go,relay.go,dispatcher_test.go,relay_test.go,Dockerfile}`
- Modify: `docker-compose.yml`

**Interfaces:**
- Consumes: `POST $ROUTER_URL` (Task 3): 2xx entregue, 4xx rejeitado, resto retry.
- Produces: `Relay.RunOnce(ctx) error` (um ciclo) e `Relay.Run(ctx)` (loop). Efeitos no banco: sucesso → `outbox.status='sent'` e `jobs.status='DISPATCHED'` (só se ainda `PENDING`); 4xx → `failed`/`FAILED`; retry → linha continua `pending`.

- [ ] **Step 1: Inicializar** — `cd services/relay && go mod init relay`

- [ ] **Step 2: Teste do dispatcher — `dispatcher_test.go`**

```go
package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestDispatcherMapsStatusToOutcome(t *testing.T) {
	cases := map[int]Outcome{202: Delivered, 400: Rejected, 422: Rejected, 500: Retry, 502: Retry}
	for status, want := range cases {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(status)
		}))
		got := NewDispatcher(srv.URL).Send(context.Background(), []byte(`{}`))
		srv.Close()
		if got != want {
			t.Fatalf("status %d: outcome = %v, want %v", status, got, want)
		}
	}
}

func TestDispatcherUnreachableRouterIsRetry(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	url := srv.URL
	srv.Close() // porta fechada: simula router fora do ar
	if got := NewDispatcher(url).Send(context.Background(), []byte(`{}`)); got != Retry {
		t.Fatalf("outcome = %v, want Retry", got)
	}
}
```

- [ ] **Step 3: Teste do relay (integração) — `relay_test.go`**

```go
package main

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// sendFunc adapta uma função ao Sender.
type sendFunc func(ctx context.Context, envelope []byte) Outcome

func (f sendFunc) Send(ctx context.Context, e []byte) Outcome { return f(ctx, e) }

const testJobID = "11111111-1111-1111-1111-111111111111"

// setup conecta ao banco de teste, LIMPA as tabelas e insere um job + outbox pendente.
// Exige TEST_DATABASE_URL (Postgres de dev do compose: o TRUNCATE apaga tudo).
func setup(t *testing.T, jobStatus string) *pgxpool.Pool {
	t.Helper()
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL não definido")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	stmts := []string{
		`TRUNCATE job_results, outbox, jobs CASCADE`,
		`INSERT INTO jobs (id, type, payload, origin, status) VALUES ('` + testJobID + `', 'email.send', '{}', 'gateway-go', '` + jobStatus + `')`,
		`INSERT INTO outbox (job_id, envelope) VALUES ('` + testJobID + `', '{"job_id":"` + testJobID + `","type":"email.send"}')`,
	}
	for _, s := range stmts {
		if _, err := pool.Exec(ctx, s); err != nil {
			t.Fatal(err)
		}
	}
	return pool
}

// statuses devolve (outbox.status, jobs.status) do job de teste.
func statuses(t *testing.T, pool *pgxpool.Pool) (string, string) {
	t.Helper()
	var outbox, job string
	err := pool.QueryRow(context.Background(),
		`SELECT o.status, j.status FROM outbox o JOIN jobs j ON j.id = o.job_id`).Scan(&outbox, &job)
	if err != nil {
		t.Fatal(err)
	}
	return outbox, job
}

func runOnce(t *testing.T, pool *pgxpool.Pool, outcome Outcome) {
	t.Helper()
	sender := sendFunc(func(context.Context, []byte) Outcome { return outcome })
	if err := NewRelay(pool, sender, 10, time.Second).RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestRelayDeliveredMarksSentAndDispatched(t *testing.T) {
	pool := setup(t, "PENDING")
	runOnce(t, pool, Delivered)
	if o, j := statuses(t, pool); o != "sent" || j != "DISPATCHED" {
		t.Fatalf("outbox=%s job=%s", o, j)
	}
}

func TestRelayRetryKeepsRowPending(t *testing.T) {
	pool := setup(t, "PENDING")
	runOnce(t, pool, Retry)
	if o, j := statuses(t, pool); o != "pending" || j != "PENDING" {
		t.Fatalf("outbox=%s job=%s", o, j)
	}
}

func TestRelayRejectedMarksFailed(t *testing.T) {
	pool := setup(t, "PENDING")
	runOnce(t, pool, Rejected)
	if o, j := statuses(t, pool); o != "failed" || j != "FAILED" {
		t.Fatalf("outbox=%s job=%s", o, j)
	}
}

func TestRelayDoesNotRegressDoneJob(t *testing.T) {
	pool := setup(t, "DONE") // worker terminou antes de o relay marcar a entrega
	runOnce(t, pool, Delivered)
	if o, j := statuses(t, pool); o != "sent" || j != "DONE" {
		t.Fatalf("outbox=%s job=%s", o, j)
	}
}
```

- [ ] **Step 4: Rodar e ver falhar**

Run: `cd services/relay && go test ./...`
Expected: FAIL (`undefined: Outcome`, `NewDispatcher`, `NewRelay`).

- [ ] **Step 5: `dispatcher.go`**

```go
// Package main é o relay: lê a outbox e entrega cada envelope ao router.
// dispatcher.go faz o POST HTTP e traduz a resposta em um Outcome.
package main

import (
	"bytes"
	"context"
	"log"
	"net/http"
	"time"
)

// Outcome é o resultado de uma tentativa de entrega ao router.
type Outcome int

const (
	// Delivered: router respondeu 2xx.
	Delivered Outcome = iota
	// Rejected: router respondeu 4xx; repetir não adianta (ex.: type desconhecido).
	Rejected
	// Retry: erro de rede ou 5xx; tentar de novo no próximo ciclo.
	Retry
)

// Dispatcher envia envelopes ao webhook do router.
type Dispatcher struct {
	url    string
	client *http.Client
}

// NewDispatcher cria o dispatcher para a URL do router (ex.: http://router:8080/dispatch).
func NewDispatcher(url string) *Dispatcher {
	return &Dispatcher{url: url, client: &http.Client{Timeout: 5 * time.Second}}
}

// Send faz POST do envelope e classifica a resposta.
func (d *Dispatcher) Send(ctx context.Context, envelope []byte) Outcome {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, d.url, bytes.NewReader(envelope))
	if err != nil {
		log.Printf("build request: %v", err)
		return Retry
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := d.client.Do(req)
	if err != nil {
		log.Printf("router unreachable: %v", err)
		return Retry
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			log.Printf("close body: %v", err)
		}
	}()
	switch {
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		return Delivered
	case resp.StatusCode >= 400 && resp.StatusCode < 500:
		return Rejected
	default:
		return Retry
	}
}
```

- [ ] **Step 6: `relay.go`**

```go
package main

import (
	"context"
	"errors"
	"log"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// claimQuery pega o lote de pendentes em ordem. FOR UPDATE SKIP LOCKED permite
// mais de um relay sem pegar a mesma linha.
const claimQuery = `
SELECT id, job_id::text, envelope
FROM outbox
WHERE status = 'pending'
ORDER BY id
LIMIT $1
FOR UPDATE SKIP LOCKED`

// Sender entrega um envelope ao router.
type Sender interface {
	Send(ctx context.Context, envelope []byte) Outcome
}

// entry é uma linha pendente da outbox.
type entry struct {
	ID       int64
	JobID    string
	Envelope []byte
}

// Relay move linhas da outbox para o router.
type Relay struct {
	pool     *pgxpool.Pool
	sender   Sender
	batch    int
	interval time.Duration
}

// NewRelay cria o relay com tamanho de lote e intervalo de polling.
func NewRelay(pool *pgxpool.Pool, sender Sender, batch int, interval time.Duration) *Relay {
	return &Relay{pool: pool, sender: sender, batch: batch, interval: interval}
}

// Run executa RunOnce a cada intervalo até o contexto ser cancelado.
func (r *Relay) Run(ctx context.Context) {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	for {
		if err := r.RunOnce(ctx); err != nil {
			log.Printf("relay cycle: %v", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// RunOnce processa um lote numa transação. A transação fica aberta durante o
// POST (aceitável na demo): se o relay morrer, o lock cai e a linha volta a
// ficar pendente, garantindo at-least-once. Em Retry o lote para para preservar a ordem.
func (r *Relay) RunOnce(ctx context.Context) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() {
		if err := tx.Rollback(ctx); err != nil && !errors.Is(err, pgx.ErrTxClosed) {
			log.Printf("rollback: %v", err)
		}
	}()
	entries, err := claim(ctx, tx, r.batch)
	if err != nil {
		return err
	}
	for _, e := range entries {
		switch r.sender.Send(ctx, e.Envelope) {
		case Delivered:
			err = mark(ctx, tx, e, "sent", "DISPATCHED")
		case Rejected:
			err = mark(ctx, tx, e, "failed", "FAILED")
		default:
			return tx.Commit(ctx) // Retry: confirma o que já foi marcado e para
		}
		if err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

// claim lê e trava as linhas pendentes do lote.
func claim(ctx context.Context, tx pgx.Tx, limit int) ([]entry, error) {
	rows, err := tx.Query(ctx, claimQuery, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var entries []entry
	for rows.Next() {
		var e entry
		if err := rows.Scan(&e.ID, &e.JobID, &e.Envelope); err != nil {
			return nil, err
		}
		entries = append(entries, e)
	}
	return entries, rows.Err()
}

// mark atualiza a outbox e o job. O job só muda se ainda estiver PENDING:
// um worker rápido pode já ter marcado DONE e isso não pode regredir.
func mark(ctx context.Context, tx pgx.Tx, e entry, outboxStatus, jobStatus string) error {
	if _, err := tx.Exec(ctx, `UPDATE outbox SET status = $1 WHERE id = $2`, outboxStatus, e.ID); err != nil {
		return err
	}
	_, err := tx.Exec(ctx, `UPDATE jobs SET status = $1 WHERE id = $2::uuid AND status = 'PENDING'`, jobStatus, e.JobID)
	return err
}
```

- [ ] **Step 7: `main.go`**

```go
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// env lê uma variável de ambiente com valor padrão.
func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// run monta as dependências e roda o loop até SIGINT/SIGTERM.
func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	pool, err := pgxpool.New(ctx, env("DATABASE_URL", "postgres://app:app@localhost:5432/app"))
	if err != nil {
		return err
	}
	defer pool.Close()
	interval, err := time.ParseDuration(env("POLL_INTERVAL", "1s"))
	if err != nil {
		return err
	}
	dispatcher := NewDispatcher(env("ROUTER_URL", "http://localhost:8080/dispatch"))
	log.Printf("relay polling every %s", interval)
	NewRelay(pool, dispatcher, 10, interval).Run(ctx)
	return nil
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}
```

- [ ] **Step 8: Rodar testes** (`docker ps` antes; Postgres do compose no ar)

Run: `cd services/relay && go get github.com/jackc/pgx/v5 && go mod tidy && go vet ./... && test -z "$(gofmt -l .)" && TEST_DATABASE_URL=postgres://app:app@localhost:5432/app go test ./...`
Expected: PASS (5 testes; nenhum SKIP).

- [ ] **Step 9: `Dockerfile`** — idêntico ao da Task 3 (copiar o mesmo conteúdo).

- [ ] **Step 10: Adicionar ao `docker-compose.yml`**

```yaml
  relay:
    build: ./services/relay
    restart: unless-stopped
    environment:
      DATABASE_URL: postgres://app:app@postgres:5432/app
      ROUTER_URL: http://router:8080/dispatch
      POLL_INTERVAL: 1s
    depends_on:
      postgres: { condition: service_healthy }
      router: { condition: service_started }
```

- [ ] **Step 11: Verificar** — `docker compose up -d --build relay`, inserir um job+outbox à mão e conferir `sent`:

```bash
docker compose exec postgres psql -U app -c "INSERT INTO jobs (id,type,payload,origin) VALUES ('22222222-2222-2222-2222-222222222222','image.resize','{}','gateway-go'); INSERT INTO outbox (job_id,envelope) VALUES ('22222222-2222-2222-2222-222222222222','{\"job_id\":\"22222222-2222-2222-2222-222222222222\",\"type\":\"image.resize\",\"payload\":{},\"created_at\":\"2026-10-07T00:00:00Z\",\"origin\":\"gateway-go\"}');"
sleep 3; docker compose exec postgres psql -U app -c "SELECT status FROM outbox; SELECT status FROM jobs;"
```
Expected: `sent` e `DISPATCHED`; mensagem na fila `jobs.go`. Limpe: `TRUNCATE job_results, outbox, jobs CASCADE;` e `rabbitmqctl purge_queue jobs.go`.

- [ ] **Step 12: Checkpoint** — pare; humano revisa/commita.

---

### Task 5: gateway-go (Go)

**Files:**
- Create: `services/gateway-go/{go.mod,main.go,models.go,repository.go,handler.go,handler_test.go,repository_test.go,Dockerfile}`, `services/gateway-go/docs/` (gerado)
- Modify: `docker-compose.yml`

**Interfaces:**
- Produces (idêntico ao gateway-py): `POST /jobs` body `{"type": str, "payload": obj?}` → `202 {"job_id": uuid}`; `422 {"detail": ...}` se `type` vazio/ausente. `GET /jobs/{id}` → `200 {"id","type","status","origin","created_at","results":[{"worker","result","finished_at"}]}` ou `404 {"detail":"job not found"}`. Swagger em `/docs/index.html`. `origin = "gateway-go"`.

- [ ] **Step 1: Inicializar** — `cd services/gateway-go && go mod init gateway-go`

- [ ] **Step 2: Testes que falham**

`handler_test.go`:

```go
package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
)

// newTestRouter monta o roteador sem repositório: os casos testados
// são rejeitados na validação, antes de tocar no banco.
func newTestRouter() *gin.Engine {
	gin.SetMode(gin.TestMode)
	return NewRouter(NewJobHandler(nil, "gateway-go"))
}

func TestCreateJobRejectsMissingType(t *testing.T) {
	for _, body := range []string{`{}`, `{"type":""}`, `{"payload":{"a":1}}`, `not json`} {
		req := httptest.NewRequest(http.MethodPost, "/jobs", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		newTestRouter().ServeHTTP(rec, req)
		if rec.Code != http.StatusUnprocessableEntity {
			t.Fatalf("body %q: status = %d, want 422", body, rec.Code)
		}
	}
}
```

`repository_test.go`:

```go
package main

import (
	"context"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

// testRepo conecta ao banco de teste e LIMPA as tabelas (só usar no Postgres de dev).
func testRepo(t *testing.T) (*JobRepository, *pgxpool.Pool) {
	t.Helper()
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL não definido")
	}
	pool, err := pgxpool.New(context.Background(), url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if _, err := pool.Exec(context.Background(), `TRUNCATE job_results, outbox, jobs CASCADE`); err != nil {
		t.Fatal(err)
	}
	return NewJobRepository(pool), pool
}

func TestCreateWritesJobAndOutboxTogether(t *testing.T) {
	repo, pool := testRepo(t)
	ctx := context.Background()
	id, err := repo.Create(ctx, CreateJobRequest{Type: "email.send"}, "gateway-go")
	if err != nil {
		t.Fatal(err)
	}
	var status, envType, payload string
	err = pool.QueryRow(ctx, `
		SELECT o.status, o.envelope->>'type', o.envelope->'payload'::text
		FROM outbox o WHERE o.job_id = $1::uuid`, id).Scan(&status, &envType, &payload)
	if err != nil {
		t.Fatal(err)
	}
	if status != "pending" || envType != "email.send" || payload != "{}" {
		t.Fatalf("outbox status=%s type=%s payload=%s", status, envType, payload)
	}
}

func TestGetUnknownJobReturnsNotFound(t *testing.T) {
	repo, _ := testRepo(t)
	if _, err := repo.Get(context.Background(), "33333333-3333-3333-3333-333333333333"); err != ErrNotFound {
		t.Fatalf("err = %v, want ErrNotFound", err)
	}
}
```

- [ ] **Step 3: Rodar e ver falhar** — `go test ./...` → FAIL (`undefined: NewRouter` etc.)

- [ ] **Step 4: `models.go`**

```go
// Package main é o gateway-go: API HTTP (Gin) que grava job + outbox.
// models.go define os modelos da borda HTTP e o envelope do contrato.
package main

import (
	"encoding/json"
	"time"
)

// CreateJobRequest é o corpo de POST /jobs.
type CreateJobRequest struct {
	Type    string         `json:"type" binding:"required" example:"email.send"`
	Payload map[string]any `json:"payload"`
}

// CreateJobResponse devolve o id do job aceito.
type CreateJobResponse struct {
	JobID string `json:"job_id" example:"7c9e6679-7425-40de-944b-e07fc1f90ae7"`
}

// JobResult é o resultado gravado por um worker.
type JobResult struct {
	Worker     string          `json:"worker"`
	Result     json.RawMessage `json:"result" swaggertype:"object"`
	FinishedAt time.Time       `json:"finished_at"`
}

// JobView é a visão de um job com os resultados dos workers.
type JobView struct {
	ID        string      `json:"id"`
	Type      string      `json:"type"`
	Status    string      `json:"status"`
	Origin    string      `json:"origin"`
	CreatedAt time.Time   `json:"created_at"`
	Results   []JobResult `json:"results"`
}

// ErrorResponse é o formato de erro comum da demo.
type ErrorResponse struct {
	Detail string `json:"detail"`
}

// Envelope é a mensagem do contrato (contracts/envelope.schema.json).
type Envelope struct {
	JobID     string         `json:"job_id"`
	Type      string         `json:"type"`
	Payload   map[string]any `json:"payload"`
	CreatedAt time.Time      `json:"created_at"`
	Origin    string         `json:"origin"`
}
```

- [ ] **Step 5: `repository.go`**

```go
package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ErrNotFound indica job inexistente.
var ErrNotFound = errors.New("job not found")

// JobRepository persiste jobs e a outbox.
type JobRepository struct {
	pool *pgxpool.Pool
}

// NewJobRepository cria o repositório sobre um pool de conexões.
func NewJobRepository(pool *pgxpool.Pool) *JobRepository {
	return &JobRepository{pool: pool}
}

// Create grava job e outbox na MESMA transação (outbox transacional) e devolve o id.
// O gateway nunca publica no broker.
func (r *JobRepository) Create(ctx context.Context, req CreateJobRequest, origin string) (string, error) {
	payload := req.Payload
	if payload == nil {
		payload = map[string]any{}
	}
	now := time.Now().UTC()
	id := uuid.NewString()
	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	envelopeJSON, err := json.Marshal(Envelope{JobID: id, Type: req.Type, Payload: payload, CreatedAt: now, Origin: origin})
	if err != nil {
		return "", err
	}
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return "", err
	}
	defer func() {
		if err := tx.Rollback(ctx); err != nil && !errors.Is(err, pgx.ErrTxClosed) {
			log.Printf("rollback: %v", err)
		}
	}()
	if _, err := tx.Exec(ctx,
		`INSERT INTO jobs (id, type, payload, origin, created_at) VALUES ($1::uuid, $2, $3, $4, $5)`,
		id, req.Type, payloadJSON, origin, now); err != nil {
		return "", err
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO outbox (job_id, envelope) VALUES ($1::uuid, $2)`, id, envelopeJSON); err != nil {
		return "", err
	}
	return id, tx.Commit(ctx)
}

// Get devolve o job e os resultados dos workers, ou ErrNotFound.
func (r *JobRepository) Get(ctx context.Context, id string) (JobView, error) {
	view := JobView{Results: []JobResult{}}
	err := r.pool.QueryRow(ctx,
		`SELECT id::text, type, status, origin, created_at FROM jobs WHERE id = $1::uuid`, id).
		Scan(&view.ID, &view.Type, &view.Status, &view.Origin, &view.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return JobView{}, ErrNotFound
	}
	if err != nil {
		return JobView{}, err
	}
	rows, err := r.pool.Query(ctx,
		`SELECT worker, result, finished_at FROM job_results WHERE job_id = $1::uuid ORDER BY worker`, id)
	if err != nil {
		return JobView{}, err
	}
	defer rows.Close()
	for rows.Next() {
		var res JobResult
		if err := rows.Scan(&res.Worker, &res.Result, &res.FinishedAt); err != nil {
			return JobView{}, err
		}
		view.Results = append(view.Results, res)
	}
	return view, rows.Err()
}
```

- [ ] **Step 6: `handler.go`**

```go
package main

import (
	"errors"
	"log"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

// JobHandler expõe os endpoints de jobs.
type JobHandler struct {
	repo   *JobRepository
	origin string
}

// NewJobHandler cria o handler; origin identifica o gateway no envelope.
func NewJobHandler(repo *JobRepository, origin string) *JobHandler {
	return &JobHandler{repo: repo, origin: origin}
}

// NewRouter registra as rotas do gateway (sem Swagger; ver main.go).
func NewRouter(h *JobHandler) *gin.Engine {
	r := gin.New()
	r.Use(gin.Recovery())
	r.POST("/jobs", h.CreateJob)
	r.GET("/jobs/:id", h.GetJob)
	return r
}

// CreateJob godoc
// @Summary     Cria um job
// @Description Grava o job e a outbox numa transação e responde 202. O processamento é assíncrono.
// @Tags        jobs
// @Accept      json
// @Produce     json
// @Param       body body     CreateJobRequest true "Job"
// @Success     202  {object} CreateJobResponse
// @Failure     422  {object} ErrorResponse
// @Router      /jobs [post]
func (h *JobHandler) CreateJob(c *gin.Context) {
	var req CreateJobRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusUnprocessableEntity, ErrorResponse{Detail: err.Error()})
		return
	}
	id, err := h.repo.Create(c.Request.Context(), req, h.origin)
	if err != nil {
		log.Printf("create job: %v", err)
		c.JSON(http.StatusInternalServerError, ErrorResponse{Detail: "internal error"})
		return
	}
	c.JSON(http.StatusAccepted, CreateJobResponse{JobID: id})
}

// GetJob godoc
// @Summary     Consulta um job
// @Description Devolve o status e os resultados gravados pelos workers.
// @Tags        jobs
// @Produce     json
// @Param       id  path     string true "ID do job (uuid)"
// @Success     200 {object} JobView
// @Failure     404 {object} ErrorResponse
// @Failure     422 {object} ErrorResponse
// @Router      /jobs/{id} [get]
func (h *JobHandler) GetJob(c *gin.Context) {
	id := c.Param("id")
	if _, err := uuid.Parse(id); err != nil { // paridade com o 422 do FastAPI
		c.JSON(http.StatusUnprocessableEntity, ErrorResponse{Detail: "invalid job id"})
		return
	}
	view, err := h.repo.Get(c.Request.Context(), id)
	if errors.Is(err, ErrNotFound) {
		c.JSON(http.StatusNotFound, ErrorResponse{Detail: "job not found"})
		return
	}
	if err != nil {
		log.Printf("get job: %v", err)
		c.JSON(http.StatusInternalServerError, ErrorResponse{Detail: "internal error"})
		return
	}
	c.JSON(http.StatusOK, view)
}
```

- [ ] **Step 7: `main.go`**

```go
package main

import (
	"context"
	"log"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"
	swaggerFiles "github.com/swaggo/files"
	ginSwagger "github.com/swaggo/gin-swagger"

	_ "gateway-go/docs" // OpenAPI gerado por `make swagger`
)

// @title       Gateway Go
// @version     1.0
// @description Recebe jobs, grava job + outbox e responde 202.
// @BasePath    /

// env lê uma variável de ambiente com valor padrão.
func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// run monta as dependências e sobe o servidor.
func run() error {
	pool, err := pgxpool.New(context.Background(), env("DATABASE_URL", "postgres://app:app@localhost:5432/app"))
	if err != nil {
		return err
	}
	defer pool.Close()
	router := NewRouter(NewJobHandler(NewJobRepository(pool), "gateway-go"))
	router.GET("/docs/*any", ginSwagger.WrapHandler(swaggerFiles.Handler))
	addr := env("ADDR", ":8000")
	log.Printf("gateway-go listening on %s (docs: /docs/index.html)", addr)
	return router.Run(addr)
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}
```

- [ ] **Step 8: Dependências, Swagger e testes**

Run:
```
cd services/gateway-go
go get github.com/gin-gonic/gin github.com/jackc/pgx/v5 github.com/google/uuid github.com/swaggo/gin-swagger github.com/swaggo/files
cd ../.. && make swagger   # cria services/gateway-go/docs
cd services/gateway-go && go mod tidy && go vet ./... && test -z "$(gofmt -l .)"
TEST_DATABASE_URL=postgres://app:app@localhost:5432/app go test ./...
```
Expected: PASS (3 testes; nenhum SKIP).

- [ ] **Step 9: `Dockerfile`** — igual ao da Task 3 (`COPY . .` já leva `docs/`).

- [ ] **Step 10: Adicionar ao `docker-compose.yml`**

```yaml
  gateway-go:
    build: ./services/gateway-go
    restart: unless-stopped
    ports: ["8002:8000"]
    environment:
      DATABASE_URL: postgres://app:app@postgres:5432/app
    depends_on:
      postgres: { condition: service_healthy }
```

- [ ] **Step 11: Verificar** — `docker compose up -d --build gateway-go`; abrir `http://localhost:8002/docs/index.html`; `POST /jobs {"type":"image.resize"}` → 202; com o `relay` e `router` no ar, `GET /jobs/{id}` mostra `DISPATCHED` e há 1 mensagem em `jobs.go`.

- [ ] **Step 12: Checkpoint** — pare; humano revisa/commita.

---

### Task 6: worker-go (Go)

**Files:**
- Create: `services/worker-go/{go.mod,main.go,handler.go,store.go,consumer.go,handler_test.go,store_test.go,Dockerfile}`
- Modify: `docker-compose.yml`

**Interfaces:**
- Consumes: fila `jobs.go` (mensagens = envelope JSON).
- Produces: linha em `job_results` `(job_id, "go", {"worker":"go","detail":...})` e `jobs.status='DONE'`; `Process(ctx, Envelope) (Result, error)`, `ErrUnsupportedType`, `ResultStore.Save(ctx, jobID, worker string, result Result) error`.

- [ ] **Step 1: Inicializar** — `cd services/worker-go && go mod init worker-go`

- [ ] **Step 2: Testes que falham**

`handler_test.go`:

```go
package main

import (
	"context"
	"errors"
	"testing"
)

func TestProcessImageResize(t *testing.T) {
	res, err := Process(context.Background(), Envelope{JobID: "j1", Type: "image.resize"})
	if err != nil || res.Worker != "go" {
		t.Fatalf("res=%+v err=%v", res, err)
	}
}

func TestProcessUnsupportedType(t *testing.T) {
	_, err := Process(context.Background(), Envelope{JobID: "j1", Type: "email.send"})
	if !errors.Is(err, ErrUnsupportedType) {
		t.Fatalf("err = %v, want ErrUnsupportedType", err)
	}
}
```

`store_test.go`:

```go
package main

import (
	"context"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

const testJobID = "44444444-4444-4444-4444-444444444444"

func TestSaveIsIdempotent(t *testing.T) {
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL não definido")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	// TRUNCATE: só usar no Postgres de dev.
	for _, s := range []string{
		`TRUNCATE job_results, outbox, jobs CASCADE`,
		`INSERT INTO jobs (id, type, payload, origin) VALUES ('` + testJobID + `', 'image.resize', '{}', 'gateway-go')`,
	} {
		if _, err := pool.Exec(ctx, s); err != nil {
			t.Fatal(err)
		}
	}
	store := NewResultStore(pool)
	for range 2 { // mesma mensagem entregue duas vezes
		if err := store.Save(ctx, testJobID, "go", Result{Worker: "go", Detail: "ok"}); err != nil {
			t.Fatal(err)
		}
	}
	var rows int
	var status string
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM job_results WHERE job_id = $1::uuid`, testJobID).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT status FROM jobs WHERE id = $1::uuid`, testJobID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if rows != 1 || status != "DONE" {
		t.Fatalf("rows=%d status=%s", rows, status)
	}
}
```

- [ ] **Step 3: Rodar e ver falhar** — `go test ./...` → FAIL (`undefined: Process`...).

- [ ] **Step 4: `handler.go`**

```go
// Package main é o worker-go: consome a fila jobs.go e processa com goroutines.
// handler.go contém o envelope e a lógica (simulada) do job.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"time"
)

// ErrUnsupportedType indica um type que este worker não sabe processar.
var ErrUnsupportedType = errors.New("unsupported job type")

// Envelope é a mensagem do contrato (contracts/envelope.schema.json).
type Envelope struct {
	JobID     string          `json:"job_id"`
	Type      string          `json:"type"`
	Payload   json.RawMessage `json:"payload"`
	CreatedAt time.Time       `json:"created_at"`
	Origin    string          `json:"origin"`
}

// Result é o resultado gravado em job_results.
type Result struct {
	Worker string `json:"worker"`
	Detail string `json:"detail"`
}

// Process executa o job. Aqui só simula trabalho com uma espera curta,
// respeitando o cancelamento do contexto.
func Process(ctx context.Context, env Envelope) (Result, error) {
	if env.Type != "image.resize" {
		return Result{}, ErrUnsupportedType
	}
	select {
	case <-time.After(200 * time.Millisecond):
		return Result{Worker: "go", Detail: "image resized"}, nil
	case <-ctx.Done():
		return Result{}, ctx.Err()
	}
}
```

- [ ] **Step 5: `store.go`**

```go
package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ResultStore grava o resultado do worker e conclui o job.
type ResultStore struct {
	pool *pgxpool.Pool
}

// NewResultStore cria o store sobre um pool de conexões.
func NewResultStore(pool *pgxpool.Pool) *ResultStore {
	return &ResultStore{pool: pool}
}

// Save é idempotente: ON CONFLICT DO NOTHING absorve a reentrega do mesmo
// (job_id, worker) e o UPDATE para DONE pode repetir sem efeito colateral.
func (s *ResultStore) Save(ctx context.Context, jobID, worker string, result Result) error {
	resultJSON, err := json.Marshal(result)
	if err != nil {
		return err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() {
		if err := tx.Rollback(ctx); err != nil && !errors.Is(err, pgx.ErrTxClosed) {
			log.Printf("rollback: %v", err)
		}
	}()
	if _, err := tx.Exec(ctx,
		`INSERT INTO job_results (job_id, worker, result) VALUES ($1::uuid, $2, $3) ON CONFLICT DO NOTHING`,
		jobID, worker, resultJSON); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `UPDATE jobs SET status = 'DONE' WHERE id = $1::uuid`, jobID); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
```

- [ ] **Step 6: `consumer.go`**

```go
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Consumer lê a fila e processa cada mensagem numa goroutine.
type Consumer struct {
	ch       *amqp.Channel
	queue    string
	prefetch int
	store    *ResultStore
}

// NewConsumer cria o consumer. prefetch limita as mensagens em voo e, portanto,
// o número de goroutines simultâneas (o broker não entrega mais que isso sem ack).
func NewConsumer(ch *amqp.Channel, queue string, prefetch int, store *ResultStore) *Consumer {
	return &Consumer{ch: ch, queue: queue, prefetch: prefetch, store: store}
}

// Run consome até o contexto ser cancelado e espera as goroutines em voo.
func (c *Consumer) Run(ctx context.Context) error {
	if err := c.ch.Qos(c.prefetch, 0, false); err != nil {
		return fmt.Errorf("qos: %w", err)
	}
	deliveries, err := c.ch.ConsumeWithContext(ctx, c.queue, "", false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("consume: %w", err)
	}
	var wg sync.WaitGroup
	for d := range deliveries {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c.handle(ctx, d)
		}()
	}
	wg.Wait()
	return nil
}

// handle processa uma mensagem. O ack só acontece depois de gravar o resultado.
// JSON inválido ou type não suportado: nack sem requeue. Erro transitório: nack com requeue.
func (c *Consumer) handle(ctx context.Context, d amqp.Delivery) {
	var env Envelope
	if err := json.Unmarshal(d.Body, &env); err != nil {
		log.Printf("invalid message: %v", err)
		c.finish(d.Nack(false, false))
		return
	}
	result, err := Process(ctx, env)
	if errors.Is(err, ErrUnsupportedType) {
		log.Printf("job %s: %v", env.JobID, err)
		c.finish(d.Nack(false, false))
		return
	}
	if err == nil {
		err = c.store.Save(ctx, env.JobID, result.Worker, result)
	}
	if err != nil {
		log.Printf("job %s failed: %v", env.JobID, err)
		c.finish(d.Nack(false, true))
		return
	}
	c.finish(d.Ack(false))
}

// finish registra falha ao confirmar/rejeitar a mensagem.
func (c *Consumer) finish(err error) {
	if err != nil {
		log.Printf("ack/nack: %v", err)
	}
}
```

- [ ] **Step 7: `main.go`**

```go
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/jackc/pgx/v5/pgxpool"
	amqp "github.com/rabbitmq/amqp091-go"
)

// env lê uma variável de ambiente com valor padrão.
func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// run monta as dependências e consome até SIGINT/SIGTERM.
func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	pool, err := pgxpool.New(ctx, env("DATABASE_URL", "postgres://app:app@localhost:5432/app"))
	if err != nil {
		return err
	}
	defer pool.Close()
	conn, err := amqp.Dial(env("AMQP_URL", "amqp://guest:guest@localhost:5672/"))
	if err != nil {
		return err
	}
	defer func() {
		if err := conn.Close(); err != nil {
			log.Printf("close amqp: %v", err)
		}
	}()
	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	log.Print("worker-go consuming jobs.go")
	return NewConsumer(ch, "jobs.go", 10, NewResultStore(pool)).Run(ctx)
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}
```

- [ ] **Step 8: Testes e lint**

Run: `cd services/worker-go && go get github.com/jackc/pgx/v5 github.com/rabbitmq/amqp091-go && go mod tidy && go vet ./... && test -z "$(gofmt -l .)" && TEST_DATABASE_URL=postgres://app:app@localhost:5432/app go test ./...`
Expected: PASS (3 testes).

- [ ] **Step 9: `Dockerfile`** — igual ao da Task 3.

- [ ] **Step 10: Adicionar ao `docker-compose.yml`**

```yaml
  worker-go:
    build: ./services/worker-go
    restart: unless-stopped
    environment:
      DATABASE_URL: postgres://app:app@postgres:5432/app
      AMQP_URL: amqp://guest:guest@rabbitmq:5672/
    depends_on:
      postgres: { condition: service_healthy }
      rabbitmq: { condition: service_healthy }
```

- [ ] **Step 11: Verificar** — `docker compose up -d --build` (stack Go completa); `POST http://localhost:8002/jobs {"type":"image.resize"}`; em ~3s `GET /jobs/{id}` → `status: DONE` e `results: [{"worker":"go",...}]`.

- [ ] **Step 12: Checkpoint** — pare; humano revisa/commita. (Fim do bloco Go.)

---

### Task 7: gateway-py (Python)

**Files:**
- Modify: `pyproject.toml` (raiz: só config do ruff), `docker-compose.yml`
- Create: `services/gateway-py/{pyproject.toml,Dockerfile}`, `services/gateway-py/app/{__init__,models,repository,main}.py`, `services/gateway-py/tests/{__init__,test_gateway,test_repository}.py`

**Interfaces:**
- Produces: mesma API da Task 5 (`POST /jobs`, `GET /jobs/{id}`, `/docs`), `origin = "gateway-py"`. `JobRepository.create(request, origin) -> UUID`, `JobRepository.get(job_id) -> JobView | None`.

- [ ] **Step 1: Config do ruff na raiz** — substituir `pyproject.toml`:

```toml
[project]
name = "simple-microservices"
version = "0.1.0"
requires-python = ">=3.13"
dependencies = []

# Lint compartilhado por todos os serviços Python (`make ruff`).
[tool.ruff]
target-version = "py313"
line-length = 100

[tool.ruff.lint]
# D1: exige docstring de módulo, classe, função e método (regra do AGENTS.md).
select = ["E", "F", "I", "UP", "D1"]

[tool.ruff.lint.per-file-ignores]
"**/tests/**" = ["D1"]
"**/__init__.py" = ["D104"]
```

- [ ] **Step 2: `services/gateway-py/pyproject.toml`**

```toml
[project]
name = "gateway-py"
version = "0.1.0"
requires-python = ">=3.13"
dependencies = ["fastapi", "uvicorn", "psycopg[binary,pool]", "pydantic"]

[dependency-groups]
dev = ["pytest", "pytest-asyncio", "httpx"]

[tool.pytest.ini_options]
asyncio_mode = "auto"
```

Run: `cd services/gateway-py && uv lock && uv sync` (gera `uv.lock`).

- [ ] **Step 3: Testes que falham**

`tests/test_gateway.py`:

```python
import httpx
import pytest

from app.main import create_app, get_repository
from app.models import JobView


class FakeRepository:
    async def create(self, request, origin):  # pragma: no cover - não deve ser chamado
        raise AssertionError("não deveria gravar")

    async def get(self, job_id) -> JobView | None:
        return None


@pytest.fixture
def client() -> httpx.AsyncClient:
    app = create_app()
    app.dependency_overrides[get_repository] = lambda: FakeRepository()
    # ASGITransport não roda o lifespan, então não precisa de banco.
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test")


@pytest.mark.parametrize("body", [{}, {"type": ""}, {"payload": {"a": 1}}])
async def test_create_job_rejects_missing_type(client, body) -> None:
    response = await client.post("/jobs", json=body)
    assert response.status_code == 422


async def test_get_unknown_job_is_404(client) -> None:
    response = await client.get("/jobs/33333333-3333-3333-3333-333333333333")
    assert response.status_code == 404
    assert response.json() == {"detail": "job not found"}
```

`tests/test_repository.py`:

```python
import os

import pytest
from psycopg_pool import AsyncConnectionPool

from app.models import CreateJobRequest
from app.repository import JobRepository

DSN = os.environ.get("TEST_DATABASE_URL")
pytestmark = pytest.mark.skipif(DSN is None, reason="TEST_DATABASE_URL não definido")


@pytest.fixture
async def pool():
    async with AsyncConnectionPool(DSN, open=False) as pool:
        await pool.open()
        # TRUNCATE: só usar no Postgres de dev.
        async with pool.connection() as conn:
            await conn.execute("TRUNCATE job_results, outbox, jobs CASCADE")
        yield pool


async def test_create_writes_job_and_outbox_together(pool) -> None:
    repo = JobRepository(pool)
    job_id = await repo.create(CreateJobRequest(type="email.send"), "gateway-py")
    async with pool.connection() as conn:
        cur = await conn.execute(
            "SELECT status, envelope->>'type', envelope->'payload' FROM outbox WHERE job_id = %s",
            (job_id,),
        )
        row = await cur.fetchone()
    assert row == ("pending", "email.send", {})
    view = await repo.get(job_id)
    assert view is not None and view.origin == "gateway-py" and view.status == "PENDING"
```


- [ ] **Step 4: Rodar e ver falhar** — `cd services/gateway-py && uv run pytest -x --tb=short -q tests/test_gateway.py` → FAIL (`ModuleNotFoundError: app.main`).

- [ ] **Step 5: `app/__init__.py`** — arquivo vazio (docstring de pacote opcional pelo ruff ignore D104).

- [ ] **Step 6: `app/models.py`**

```python
"""Modelos da borda HTTP do gateway-py e o envelope do contrato."""

from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, Field


class CreateJobRequest(BaseModel):
    """Corpo de POST /jobs. `type` é obrigatório; o router decide o destino."""

    type: str = Field(min_length=1, examples=["email.send"])
    payload: dict[str, object] = Field(default_factory=dict)


class CreateJobResponse(BaseModel):
    """Id do job aceito."""

    job_id: UUID


class JobResult(BaseModel):
    """Resultado gravado por um worker."""

    worker: str
    result: dict[str, object]
    finished_at: datetime


class JobView(BaseModel):
    """Job com status e resultados dos workers."""

    id: UUID
    type: str
    status: str
    origin: str
    created_at: datetime
    results: list[JobResult]


class Envelope(BaseModel):
    """Mensagem do contrato (contracts/envelope.schema.json)."""

    job_id: UUID
    type: str
    payload: dict[str, object]
    created_at: datetime
    origin: str
```

- [ ] **Step 7: `app/repository.py`**

```python
"""Acesso ao Postgres: grava job + outbox e consulta jobs."""

from datetime import UTC, datetime
from uuid import UUID, uuid4

from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from psycopg_pool import AsyncConnectionPool

from app.models import CreateJobRequest, Envelope, JobResult, JobView


class JobRepository:
    """Persistência de jobs e outbox sobre um pool assíncrono."""

    def __init__(self, pool: AsyncConnectionPool) -> None:
        """Recebe o pool de conexões (criado no lifespan da aplicação)."""
        self._pool = pool

    async def create(self, request: CreateJobRequest, origin: str) -> UUID:
        """Grava job e outbox na MESMA transação (outbox transacional).

        O gateway nunca publica no broker; só o relay encaminha a outbox.
        """
        job_id = uuid4()
        now = datetime.now(UTC)
        envelope = Envelope(
            job_id=job_id, type=request.type, payload=request.payload, created_at=now, origin=origin
        )
        async with self._pool.connection() as conn, conn.transaction():
            await conn.execute(
                "INSERT INTO jobs (id, type, payload, origin, created_at) VALUES (%s, %s, %s, %s, %s)",
                (job_id, request.type, Jsonb(request.payload), origin, now),
            )
            await conn.execute(
                "INSERT INTO outbox (job_id, envelope) VALUES (%s, %s)",
                (job_id, Jsonb(envelope.model_dump(mode="json"))),
            )
        return job_id

    async def get(self, job_id: UUID) -> JobView | None:
        """Devolve o job com os resultados dos workers, ou None se não existir."""
        async with self._pool.connection() as conn, conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                "SELECT id, type, status, origin, created_at FROM jobs WHERE id = %s", (job_id,)
            )
            row = await cur.fetchone()
            if row is None:
                return None
            await cur.execute(
                "SELECT worker, result, finished_at FROM job_results"
                " WHERE job_id = %s ORDER BY worker",
                (job_id,),
            )
            results = [JobResult(**r) for r in await cur.fetchall()]
        return JobView(**row, results=results)
```

- [ ] **Step 8: `app/main.py`**

```python
"""gateway-py: API FastAPI que grava job + outbox e responde 202."""

import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from uuid import UUID

from fastapi import Depends, FastAPI, HTTPException, Request
from psycopg_pool import AsyncConnectionPool

from app.models import CreateJobRequest, CreateJobResponse, JobView
from app.repository import JobRepository

ORIGIN = "gateway-py"


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Abre o pool no boot e o fecha no shutdown; o repositório fica em app.state."""
    dsn = os.environ.get("DATABASE_URL", "postgres://app:app@localhost:5432/app")
    async with AsyncConnectionPool(dsn, open=False) as pool:
        await pool.open()
        app.state.repository = JobRepository(pool)
        yield


def get_repository(request: Request) -> JobRepository:
    """Dependência: entrega o repositório criado no lifespan (substituível em testes)."""
    return request.app.state.repository


def create_app() -> FastAPI:
    """Monta a aplicação e registra as rotas."""
    app = FastAPI(title="Gateway Py", lifespan=lifespan)

    @app.post(
        "/jobs",
        status_code=202,
        response_model=CreateJobResponse,
        tags=["jobs"],
        summary="Cria um job",
        description="Grava o job e a outbox numa transação e responde 202. Processamento assíncrono.",
    )
    async def create_job(
        request: CreateJobRequest, repository: JobRepository = Depends(get_repository)
    ) -> CreateJobResponse:
        """Aceita o job; o relay/router/worker cuidam do resto."""
        return CreateJobResponse(job_id=await repository.create(request, ORIGIN))

    @app.get(
        "/jobs/{job_id}",
        response_model=JobView,
        tags=["jobs"],
        summary="Consulta um job",
        description="Devolve o status e os resultados gravados pelos workers.",
    )
    async def get_job(
        job_id: UUID, repository: JobRepository = Depends(get_repository)
    ) -> JobView:
        """Consulta o job; 404 se não existir."""
        view = await repository.get(job_id)
        if view is None:
            raise HTTPException(status_code=404, detail="job not found")
        return view

    return app


app = create_app()
```

- [ ] **Step 9: Testes e lint**

Run: `cd services/gateway-py && uv run pytest -x --tb=short -q tests/test_gateway.py` → PASS.
Run (Postgres no ar): `TEST_DATABASE_URL=postgres://app:app@localhost:5432/app uv run pytest -x --tb=short -q tests/test_repository.py` → PASS.
Run: `cd ../.. && make ruff` → limpo (ajuste `uvx ruff format services` se pedir).

- [ ] **Step 10: `Dockerfile`**

```dockerfile
# Imagem do gateway-py: uv instala dependências travadas (uv.lock).
FROM python:3.13-slim
COPY --from=ghcr.io/astral-sh/uv:0.8 /uv /usr/local/bin/uv
WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev
COPY app ./app
CMD ["uv", "run", "--no-dev", "uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
```

- [ ] **Step 11: Adicionar ao `docker-compose.yml`**

```yaml
  gateway-py:
    build: ./services/gateway-py
    restart: unless-stopped
    ports: ["8001:8000"]
    environment:
      DATABASE_URL: postgres://app:app@postgres:5432/app
    depends_on:
      postgres: { condition: service_healthy }
```

- [ ] **Step 12: Verificar** — `docker compose up -d --build gateway-py`; `http://localhost:8001/docs`; `POST /jobs {"type":"image.resize"}` → 202; com o bloco Go no ar, em ~3s `GET /jobs/{id}` → `DONE` com resultado `go`.

- [ ] **Step 13: Checkpoint** — pare; humano revisa/commita.

---

### Task 8: worker-asyncio (Python)

**Files:**
- Create: `services/worker-asyncio/{pyproject.toml,Dockerfile}`, `services/worker-asyncio/app/{__init__,models,store,consumer,main}.py`, `services/worker-asyncio/tests/{__init__,test_store}.py`
- Modify: `docker-compose.yml`

**Interfaces:**
- Consumes: fila `jobs.asyncio`.
- Produces: `ResultStore.save(job_id: UUID, worker: str, result: Result) -> None` (async, idempotente), `Envelope`, `Result(worker, detail)`; handler `http.fetch`.

- [ ] **Step 1: `pyproject.toml`**

```toml
[project]
name = "worker-asyncio"
version = "0.1.0"
requires-python = ">=3.13"
dependencies = ["aio-pika", "psycopg[binary]", "pydantic"]

[dependency-groups]
dev = ["pytest", "pytest-asyncio"]

[tool.pytest.ini_options]
asyncio_mode = "auto"
```

Run: `cd services/worker-asyncio && uv lock && uv sync`

- [ ] **Step 2: Teste que falha — `tests/test_store.py`**

```python
import os
from uuid import UUID

import psycopg
import pytest

from app.models import Result
from app.store import ResultStore

DSN = os.environ.get("TEST_DATABASE_URL")
JOB_ID = UUID("55555555-5555-5555-5555-555555555555")


@pytest.mark.skipif(DSN is None, reason="TEST_DATABASE_URL não definido")
async def test_save_is_idempotent() -> None:
    async with await psycopg.AsyncConnection.connect(DSN, autocommit=True) as conn:
        # TRUNCATE: só usar no Postgres de dev.
        await conn.execute("TRUNCATE job_results, outbox, jobs CASCADE")
        await conn.execute(
            "INSERT INTO jobs (id, type, payload, origin) VALUES (%s, 'http.fetch', '{}', 'gateway-py')",
            (JOB_ID,),
        )
        store = ResultStore(DSN)
        for _ in range(2):  # mesma mensagem entregue duas vezes
            await store.save(JOB_ID, "asyncio", Result(worker="asyncio", detail="ok"))
        cur = await conn.execute("SELECT count(*) FROM job_results WHERE job_id = %s", (JOB_ID,))
        assert (await cur.fetchone()) == (1,)
        cur = await conn.execute("SELECT status FROM jobs WHERE id = %s", (JOB_ID,))
        assert (await cur.fetchone()) == ("DONE",)
```

Run: `uv run pytest -x --tb=short -q tests/test_store.py` → FAIL (`ModuleNotFoundError`) (ou SKIP sem a variável: rode com `TEST_DATABASE_URL`).

- [ ] **Step 3: `app/__init__.py`** vazio; **`app/models.py`**

```python
"""Modelos do worker: envelope recebido e resultado gravado."""

from datetime import datetime
from uuid import UUID

from pydantic import BaseModel


class Envelope(BaseModel):
    """Mensagem do contrato (contracts/envelope.schema.json)."""

    job_id: UUID
    type: str
    payload: dict[str, object]
    created_at: datetime
    origin: str


class Result(BaseModel):
    """Resultado gravado em job_results; mesmo formato em todos os workers."""

    worker: str
    detail: str
```

- [ ] **Step 4: `app/store.py`**

```python
"""Gravação idempotente do resultado do worker no Postgres."""

from uuid import UUID

import psycopg
from psycopg.types.json import Jsonb

from app.models import Result


class ResultStore:
    """Grava resultados e conclui o job."""

    def __init__(self, dsn: str) -> None:
        """Guarda o DSN; abre uma conexão curta por gravação (suficiente na demo)."""
        self._dsn = dsn

    async def save(self, job_id: UUID, worker: str, result: Result) -> None:
        """Insere o resultado e marca o job DONE numa transação.

        ON CONFLICT DO NOTHING absorve a reentrega do mesmo (job_id, worker).
        """
        async with await psycopg.AsyncConnection.connect(self._dsn) as conn:
            await conn.execute(
                "INSERT INTO job_results (job_id, worker, result) VALUES (%s, %s, %s)"
                " ON CONFLICT DO NOTHING",
                (job_id, worker, Jsonb(result.model_dump())),
            )
            await conn.execute("UPDATE jobs SET status = 'DONE' WHERE id = %s", (job_id,))
```

(O `async with` na conexão commita ao sair sem erro.)

- [ ] **Step 5: `app/consumer.py`**

```python
"""Consumer aio-pika: lê jobs.asyncio e processa cada mensagem como task asyncio."""

import asyncio
import logging

from aio_pika import IncomingMessage
from aio_pika.abc import AbstractChannel
from pydantic import ValidationError

from app.models import Envelope, Result
from app.store import ResultStore

logger = logging.getLogger(__name__)

QUEUE = "jobs.asyncio"
JOB_TYPE = "http.fetch"
PREFETCH = 10  # limita mensagens em voo e, logo, a concorrência


async def process(envelope: Envelope) -> Result:
    """Executa o job `http.fetch` (simulado com espera curta, sem bloquear o loop)."""
    await asyncio.sleep(0.2)
    return Result(worker="asyncio", detail="url fetched")


def parse_envelope(body: bytes) -> Envelope:
    """Valida o corpo; levanta ValueError (inclui ValidationError) se inválido ou de outro type."""
    envelope = Envelope.model_validate_json(body)
    if envelope.type != JOB_TYPE:
        raise ValueError(f"unsupported job type: {envelope.type}")
    return envelope


class JobConsumer:
    """Consome a fila e grava o resultado antes de confirmar a mensagem."""

    def __init__(self, channel: AbstractChannel, store: ResultStore) -> None:
        """Recebe o canal AMQP e o store de resultados."""
        self._channel = channel
        self._store = store

    async def run(self) -> None:
        """Define o QoS e consome indefinidamente (callbacks rodam concorrentes)."""
        await self._channel.set_qos(prefetch_count=PREFETCH)
        queue = await self._channel.get_queue(QUEUE)
        await queue.consume(self._on_message)
        await asyncio.Event().wait()

    async def _on_message(self, message: IncomingMessage) -> None:
        """Valida, processa e grava. Inválida: reject sem requeue. Erro transitório: requeue."""
        try:
            envelope = parse_envelope(message.body)
        except (ValidationError, ValueError):
            logger.exception("mensagem inválida; descartando")
            await message.reject(requeue=False)
            return
        async with message.process(requeue=True):  # ack só se o bloco terminar sem erro
            result = await process(envelope)
            await self._store.save(envelope.job_id, result.worker, result)
```

- [ ] **Step 6: `app/main.py`**

```python
"""Entrada do worker-asyncio: conecta ao RabbitMQ e inicia o consumer."""

import asyncio
import logging
import os

import aio_pika

from app.consumer import JobConsumer
from app.store import ResultStore


async def main() -> None:
    """Conecta (com reconexão automática) e consome até o processo terminar."""
    logging.basicConfig(level=logging.INFO)
    connection = await aio_pika.connect_robust(
        os.environ.get("AMQP_URL", "amqp://guest:guest@localhost:5672/")
    )
    async with connection:
        channel = await connection.channel()
        store = ResultStore(os.environ.get("DATABASE_URL", "postgres://app:app@localhost:5432/app"))
        await JobConsumer(channel, store).run()


if __name__ == "__main__":
    asyncio.run(main())
```

- [ ] **Step 7: Testes e lint**

Run: `TEST_DATABASE_URL=postgres://app:app@localhost:5432/app uv run pytest -x --tb=short -q tests/test_store.py` → PASS. `make ruff` (na raiz) → limpo.

- [ ] **Step 8: `Dockerfile`** — igual ao da Task 7, com `CMD ["uv", "run", "--no-dev", "python", "-m", "app.main"]`.

- [ ] **Step 9: Adicionar ao `docker-compose.yml`**

```yaml
  worker-asyncio:
    build: ./services/worker-asyncio
    restart: unless-stopped
    environment:
      DATABASE_URL: postgres://app:app@postgres:5432/app
      AMQP_URL: amqp://guest:guest@rabbitmq:5672/
    depends_on:
      postgres: { condition: service_healthy }
      rabbitmq: { condition: service_healthy }
```

- [ ] **Step 10: Verificar** — `docker compose up -d --build worker-asyncio`; `POST http://localhost:8001/jobs {"type":"http.fetch"}` → em ~3s `DONE` com `results[0].worker == "asyncio"`.

- [ ] **Step 11: Checkpoint** — pare; humano revisa/commita.

---

### Task 9: worker-celery (Python)

**Files:**
- Create: `services/worker-celery/{pyproject.toml,Dockerfile}`, `services/worker-celery/app/{__init__,models,store,tasks,bridge}.py`, `services/worker-celery/tests/{__init__,test_store.py}`
- Modify: `docker-compose.yml` (dois serviços: `worker-celery` e `worker-celery-bridge`)

**Interfaces:**
- Consumes: fila `jobs.celery` (bridge). Celery usa o próprio broker no RabbitMQ (`CELERY_BROKER_URL`), fila interna `celery`.
- Produces: task `process_job(envelope: dict[str, object]) -> None`; `ResultStore.save(job_id, worker, result)` **síncrono** e idempotente.

**Nota de ack:** o bridge confirma a mensagem de `jobs.celery` depois de `process_job.delay(...)` (entregue à fila durável do Celery). A task usa `acks_late=True`, então o Celery só reconhece depois de gravar o resultado. Isso é a leitura de "ack explícito" para frameworks (registrada no AGENTS.md).

- [ ] **Step 1: `pyproject.toml`**

```toml
[project]
name = "worker-celery"
version = "0.1.0"
requires-python = ">=3.13"
dependencies = ["celery", "aio-pika", "psycopg[binary]", "pydantic"]

[dependency-groups]
dev = ["pytest"]
```

Run: `cd services/worker-celery && uv lock && uv sync`

- [ ] **Step 2: Teste que falha — `tests/test_store.py`**

```python
import os
from uuid import UUID

import psycopg
import pytest

from app.models import Result
from app.store import ResultStore

DSN = os.environ.get("TEST_DATABASE_URL")
JOB_ID = UUID("66666666-6666-6666-6666-666666666666")


@pytest.mark.skipif(DSN is None, reason="TEST_DATABASE_URL não definido")
def test_save_is_idempotent() -> None:
    with psycopg.connect(DSN, autocommit=True) as conn:
        # TRUNCATE: só usar no Postgres de dev.
        conn.execute("TRUNCATE job_results, outbox, jobs CASCADE")
        conn.execute(
            "INSERT INTO jobs (id, type, payload, origin) VALUES (%s, 'report.generate', '{}', 'gateway-py')",
            (JOB_ID,),
        )
        store = ResultStore(DSN)
        for _ in range(2):  # mesma mensagem entregue duas vezes
            store.save(JOB_ID, "celery", Result(worker="celery", detail="ok"))
        assert conn.execute("SELECT count(*) FROM job_results WHERE job_id = %s", (JOB_ID,)).fetchone() == (1,)
        assert conn.execute("SELECT status FROM jobs WHERE id = %s", (JOB_ID,)).fetchone() == ("DONE",)
```

Run: `TEST_DATABASE_URL=... uv run pytest -x --tb=short -q tests/test_store.py` → FAIL.

- [ ] **Step 3: `app/__init__.py` vazio; `app/models.py`** — copiar o conteúdo de `worker-asyncio/app/models.py` (Task 8, Step 3). Cópia deliberada: serviços não importam uns dos outros.

- [ ] **Step 4: `app/store.py`** (versão síncrona: Celery executa tasks em processos/threads sync)

```python
"""Gravação idempotente do resultado do worker no Postgres (versão síncrona)."""

from uuid import UUID

import psycopg
from psycopg.types.json import Jsonb

from app.models import Result


class ResultStore:
    """Grava resultados e conclui o job."""

    def __init__(self, dsn: str) -> None:
        """Guarda o DSN; abre uma conexão curta por gravação."""
        self._dsn = dsn

    def save(self, job_id: UUID, worker: str, result: Result) -> None:
        """Insere o resultado e marca o job DONE numa transação.

        ON CONFLICT DO NOTHING absorve a reentrega do mesmo (job_id, worker).
        """
        with psycopg.connect(self._dsn) as conn:
            conn.execute(
                "INSERT INTO job_results (job_id, worker, result) VALUES (%s, %s, %s)"
                " ON CONFLICT DO NOTHING",
                (job_id, worker, Jsonb(result.model_dump())),
            )
            conn.execute("UPDATE jobs SET status = 'DONE' WHERE id = %s", (job_id,))
```

- [ ] **Step 5: `app/tasks.py`**

```python
"""Task Celery que processa o job `report.generate`."""

import os
import time

from celery import Celery

from app.models import Envelope, Result
from app.store import ResultStore

app = Celery("worker_celery", broker=os.environ.get("CELERY_BROKER_URL", "amqp://guest:guest@localhost:5672//"))
# acks_late: o broker só recebe o ack depois que a task termina (grava o resultado).
app.conf.task_acks_late = True
app.conf.worker_prefetch_multiplier = 1


@app.task(name="process_job")
def process_job(envelope: dict[str, object]) -> None:
    """Processa o job (simulado com uma espera curta) e grava o resultado."""
    parsed = Envelope.model_validate(envelope)
    time.sleep(0.2)
    store = ResultStore(os.environ.get("DATABASE_URL", "postgres://app:app@localhost:5432/app"))
    store.save(parsed.job_id, "celery", Result(worker="celery", detail="report generated"))
```

- [ ] **Step 6: `app/bridge.py`**

```python
"""Bridge: lê o envelope de jobs.celery e o entrega ao Celery.

O envelope neutro não é o formato nativo do Celery, então este consumer fino
faz a ponte (padrão bridge): lê da fila do contrato e chama `process_job.delay`.
"""

import asyncio
import logging
import os

import aio_pika
from aio_pika import IncomingMessage
from pydantic import ValidationError

from app.models import Envelope
from app.tasks import process_job

logger = logging.getLogger(__name__)

QUEUE = "jobs.celery"


async def on_message(message: IncomingMessage) -> None:
    """Valida e enfileira no Celery. Inválida: reject sem requeue. Falha ao enfileirar: requeue."""
    try:
        envelope = Envelope.model_validate_json(message.body)
    except ValidationError:
        logger.exception("mensagem inválida; descartando")
        await message.reject(requeue=False)
        return
    async with message.process(requeue=True):
        # delay() é bloqueante (I/O de rede do kombu): roda em thread para não travar o loop.
        await asyncio.to_thread(process_job.delay, envelope.model_dump(mode="json"))


async def main() -> None:
    """Conecta ao RabbitMQ e consome jobs.celery."""
    logging.basicConfig(level=logging.INFO)
    connection = await aio_pika.connect_robust(
        os.environ.get("AMQP_URL", "amqp://guest:guest@localhost:5672/")
    )
    async with connection:
        channel = await connection.channel()
        await channel.set_qos(prefetch_count=32)
        queue = await channel.get_queue(QUEUE)
        await queue.consume(on_message)
        await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(main())
```

- [ ] **Step 7: Testes e lint**

Run: `TEST_DATABASE_URL=postgres://app:app@localhost:5432/app uv run pytest -x --tb=short -q tests/test_store.py` → PASS. `make ruff` → limpo.

- [ ] **Step 8: `Dockerfile`** — igual ao da Task 7 sem `CMD` fixo (`CMD` é definido por serviço no compose).

- [ ] **Step 9: Adicionar ao `docker-compose.yml`**

```yaml
  worker-celery:
    build: ./services/worker-celery
    restart: unless-stopped
    command: ["uv", "run", "--no-dev", "celery", "-A", "app.tasks", "worker", "--concurrency=4", "--loglevel=info"]
    environment:
      DATABASE_URL: postgres://app:app@postgres:5432/app
      CELERY_BROKER_URL: amqp://guest:guest@rabbitmq:5672//
    depends_on:
      postgres: { condition: service_healthy }
      rabbitmq: { condition: service_healthy }

  worker-celery-bridge:
    build: ./services/worker-celery
    restart: unless-stopped
    command: ["uv", "run", "--no-dev", "python", "-m", "app.bridge"]
    environment:
      AMQP_URL: amqp://guest:guest@rabbitmq:5672/
      CELERY_BROKER_URL: amqp://guest:guest@rabbitmq:5672//
    depends_on:
      rabbitmq: { condition: service_healthy }
```

- [ ] **Step 10: Verificar** — `docker compose up -d --build worker-celery worker-celery-bridge`; `POST http://localhost:8001/jobs {"type":"report.generate"}` → em ~3s `DONE` com `worker == "celery"`.

- [ ] **Step 11: Checkpoint** — pare; humano revisa/commita.

---

### Task 10: worker-taskiq (Python)

**Files:**
- Create: `services/worker-taskiq/{pyproject.toml,Dockerfile}`, `services/worker-taskiq/app/{__init__,models,store,tasks,bridge}.py`, `services/worker-taskiq/tests/{__init__,test_store.py}`
- Modify: `docker-compose.yml` (`worker-taskiq` e `worker-taskiq-bridge`)

**Interfaces:**
- Consumes: fila `jobs.taskiq` (bridge); TaskIQ usa fila própria `taskiq` no mesmo RabbitMQ.
- Produces: task assíncrona `process_job(envelope: dict[str, object]) -> None`; `ResultStore.save` **assíncrono** idempotente.

- [ ] **Step 1: `pyproject.toml`**

```toml
[project]
name = "worker-taskiq"
version = "0.1.0"
requires-python = ">=3.13"
dependencies = ["taskiq", "taskiq-aio-pika", "aio-pika", "psycopg[binary]", "pydantic"]

[dependency-groups]
dev = ["pytest", "pytest-asyncio"]

[tool.pytest.ini_options]
asyncio_mode = "auto"
```

Run: `cd services/worker-taskiq && uv lock && uv sync`

- [ ] **Step 2: Teste que falha — `tests/test_store.py`** — idêntico ao da Task 8 (store assíncrono), trocando: `JOB_ID = UUID("77777777-7777-7777-7777-777777777777")`, `type` do job `'email.send'`, worker `"taskiq"` no `save` e no `Result`.

Run com `TEST_DATABASE_URL` → FAIL (`ModuleNotFoundError`).

- [ ] **Step 3: `app/__init__.py` vazio; `app/models.py` e `app/store.py`** — copiar de `worker-asyncio/app/` (Task 8, Steps 3 e 4); serviços não importam uns dos outros.

- [ ] **Step 4: `app/tasks.py`**

```python
"""Task TaskIQ que processa o job `email.send`."""

import asyncio
import os

from taskiq_aio_pika import AioPikaBroker

from app.models import Envelope, Result
from app.store import ResultStore

broker = AioPikaBroker(
    os.environ.get("TASKIQ_BROKER_URL", "amqp://guest:guest@localhost:5672/"),
    queue_name="taskiq",
)


@broker.task(task_name="process_job")
async def process_job(envelope: dict[str, object]) -> None:
    """Processa o job (simulado com uma espera curta) e grava o resultado."""
    parsed = Envelope.model_validate(envelope)
    await asyncio.sleep(0.2)
    store = ResultStore(os.environ.get("DATABASE_URL", "postgres://app:app@localhost:5432/app"))
    await store.save(parsed.job_id, "taskiq", Result(worker="taskiq", detail="email sent"))
```

Antes de seguir, confira a assinatura instalada: `uv run python -c "import inspect, taskiq_aio_pika as t; print(inspect.signature(t.AioPikaBroker.__init__))"`. Ajuste `queue_name`/`url` se os nomes diferirem e registre o ajuste no checkpoint.

- [ ] **Step 5: `app/bridge.py`**

```python
"""Bridge: lê o envelope de jobs.taskiq e o entrega ao TaskIQ.

O envelope neutro não é o formato nativo do TaskIQ; o bridge lê da fila do
contrato e chama `process_job.kiq`, que enfileira no formato do framework.
"""

import asyncio
import logging
import os

import aio_pika
from aio_pika import IncomingMessage
from pydantic import ValidationError

from app.models import Envelope
from app.tasks import broker, process_job

logger = logging.getLogger(__name__)

QUEUE = "jobs.taskiq"


async def on_message(message: IncomingMessage) -> None:
    """Valida e enfileira no TaskIQ. Inválida: reject sem requeue. Falha ao enfileirar: requeue."""
    try:
        envelope = Envelope.model_validate_json(message.body)
    except ValidationError:
        logger.exception("mensagem inválida; descartando")
        await message.reject(requeue=False)
        return
    async with message.process(requeue=True):
        await process_job.kiq(envelope.model_dump(mode="json"))


async def main() -> None:
    """Inicia o broker TaskIQ (lado cliente) e consome jobs.taskiq."""
    logging.basicConfig(level=logging.INFO)
    await broker.startup()
    connection = await aio_pika.connect_robust(
        os.environ.get("AMQP_URL", "amqp://guest:guest@localhost:5672/")
    )
    async with connection:
        channel = await connection.channel()
        await channel.set_qos(prefetch_count=32)
        queue = await channel.get_queue(QUEUE)
        await queue.consume(on_message)
        await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(main())
```

- [ ] **Step 6: Testes e lint**

Run: `TEST_DATABASE_URL=postgres://app:app@localhost:5432/app uv run pytest -x --tb=short -q tests/test_store.py` → PASS. `make ruff` → limpo.

- [ ] **Step 7: `Dockerfile`** — igual ao da Task 9.

- [ ] **Step 8: Adicionar ao `docker-compose.yml`**

```yaml
  worker-taskiq:
    build: ./services/worker-taskiq
    restart: unless-stopped
    command: ["uv", "run", "--no-dev", "taskiq", "worker", "app.tasks:broker", "--workers", "1"]
    environment:
      DATABASE_URL: postgres://app:app@postgres:5432/app
      TASKIQ_BROKER_URL: amqp://guest:guest@rabbitmq:5672/
    depends_on:
      postgres: { condition: service_healthy }
      rabbitmq: { condition: service_healthy }

  worker-taskiq-bridge:
    build: ./services/worker-taskiq
    restart: unless-stopped
    command: ["uv", "run", "--no-dev", "python", "-m", "app.bridge"]
    environment:
      AMQP_URL: amqp://guest:guest@rabbitmq:5672/
      TASKIQ_BROKER_URL: amqp://guest:guest@rabbitmq:5672/
    depends_on:
      rabbitmq: { condition: service_healthy }
```

- [ ] **Step 9: Verificar** — `docker compose up -d --build worker-taskiq worker-taskiq-bridge`; `POST http://localhost:8001/jobs {"type":"email.send"}` → em ~3s `DONE` com `worker == "taskiq"`. (Fim do bloco Python.)

- [ ] **Step 10: Checkpoint** — pare; humano revisa/commita.

---

### Task 11: Verificação ponta a ponta (sem código novo)

**Files:** nenhum (se algo falhar, corrija na task da linguagem dona, isoladamente, e repita aqui).

- [ ] **Step 1: Subir do zero** — `make down && make up && sleep 20 && docker compose ps` → todos `running`/`healthy`.

- [ ] **Step 2: 8 jobs (2 gateways × 4 types)**

```bash
for gw in 8001 8002; do for t in report.generate email.send http.fetch image.resize; do
  curl -s -X POST localhost:$gw/jobs -H 'content-type: application/json' -d "{\"type\":\"$t\",\"payload\":{}}"; echo
done; done
```
Expected: 8 respostas `{"job_id": "..."}` com HTTP 202.

- [ ] **Step 3: Estado final** — `sleep 5; docker compose exec postgres psql -U app -c "SELECT j.type, j.origin, j.status, r.worker FROM jobs j LEFT JOIN job_results r ON r.job_id = j.id ORDER BY j.created_at;"`
Expected: 8 linhas `DONE`; workers `celery/taskiq/asyncio/go` conforme a tabela.

- [ ] **Step 4: type desconhecido** — `curl -s -X POST localhost:8001/jobs -H 'content-type: application/json' -d '{"type":"nope"}'`, depois `GET /jobs/{id}` → `status: FAILED`.

- [ ] **Step 5: Router fora do ar não derruba o gateway** — `docker compose stop router`; `POST /jobs` nos dois gateways → 202; `docker compose start router`; em ~3s os jobs viram `DONE`.

- [ ] **Step 6: Lint/vet final** — `make ruff && make vet` → limpos.

- [ ] **Step 7: Checkpoint** — pare; relate resultados ao humano.

---

### Task 12: README (docs)

**Files:**
- Modify: `README.md` (reescrever, ~150 linhas)

- [ ] **Step 1: Substituir o README** com as seções, nesta ordem (conteúdo extraído do spec e do código final):
  1. Título e propósito (demo de arquitetura desacoplada; minimalista).
  2. Arquitetura: diagrama ASCII do spec (§2) + "Por que outbox" + "Por que um router".
  3. Serviços: tabela (pasta, stack, papel, porta).
  4. Contrato: envelope (JSON de exemplo) + tabela de roteamento `type → worker`.
  5. Dados: resumo das 3 tabelas e dos status (`PENDING→DISPATCHED→DONE|FAILED`; outbox `pending→sent|failed`).
  6. RabbitMQ: exchange `jobs`, 4 filas, routing keys.
  7. Garantias: at-least-once, idempotência, ack (incluindo a nota do bridge), `type` desconhecido → `FAILED`, sem DLQ/retry/observabilidade (limitações conscientes).
  8. Como rodar: `make up`, URLs (`:8001/docs`, `:8002/docs/index.html`, `:15672`), exemplo de `POST /jobs`, `make down`, `make ruff`, `make vet`, `make swagger`, como rodar testes (`TEST_DATABASE_URL` + aviso do `TRUNCATE`).
  9. Estrutura do repositório (árvore do spec §8).

- [ ] **Step 2: Reler o README contra o código** — portas, nomes de serviços do compose, comandos do Makefile, tabela de roteamento (`services/router/routes.go`) e formato da API (Tasks 5/7). Corrija divergências.

- [ ] **Step 3: Checkpoint** — pare; humano revisa/commita.
