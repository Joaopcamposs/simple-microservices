# Guia de estudo: arquitetura e stack

Guia para quem já domina HTTP, SQL, Docker e o básico de mensageria e quer entender **por que** este projeto é montado assim e **como** as peças se integram. O `README.md` diz o que existe; este arquivo explica as decisões, os mecanismos e as armadilhas.

> **Estado atual da implementação:** infra (Postgres, RabbitMQ), `gateway-py`, `gateway-go`, `router`, `relay`, `worker-go`, `worker-asyncio` e `worker-celery` existem e foram verificados. O worker TaskIQ está especificado em `docs/superpowers/plans/2026-10-07-simple-microservices.md` e entra na próxima etapa. As seções marcam o que é **[implementado]** e o que é **[planejado]**.

---

## 1. O problema que a arquitetura resolve

Um cliente envia um job (`POST /jobs`). Queremos:

1. Responder rápido (202) sem esperar o processamento.
2. Nunca perder um job aceito, mesmo se o broker ou um serviço cair.
3. Poder escolher, por tipo de job, **qual tecnologia** processa (Celery, TaskIQ, asyncio puro ou goroutines), sem que quem recebe a requisição saiba disso.
4. Ter duas APIs (Python e Go) com contrato idêntico, para comparar ergonomia sem mudar o resto do sistema.

A parte difícil é o item 2, e é ela que molda o desenho.

### O problema do dual write

O gateway precisa fazer duas escritas em sistemas diferentes: gravar o job no Postgres e publicar no RabbitMQ. Não existe transação que cubra os dois.

| Ordem | Falha no meio | Resultado |
|---|---|---|
| publica, depois grava | crash após publicar | worker recebe job que o banco desconhece |
| grava, depois publica | crash após gravar | job `PENDING` para sempre, ninguém processa |

Retry no cliente não resolve: o cliente nem sabe se o job foi gravado.

### A solução: transactional outbox

O gateway grava `jobs` **e** `outbox` na mesma transação do Postgres e termina. Outro processo (o relay) lê a outbox e entrega adiante. A atomicidade que faltava vem do banco, que é o único sistema transacional envolvido. O custo é uma etapa a mais e latência de polling.

Consequência importante: **o gateway não depende do broker nem do router estarem de pé**. Eles podem ficar fora por minutos; a outbox acumula e drena depois.

---

## 2. Visão geral do fluxo

```
cliente → gateway (py|go) ──tx──► Postgres: jobs + outbox
                                      │ poll
                                      ▼
                                   relay (Go)
                                      │ POST /dispatch
                                      ▼
                                  router (Go)  ── tabela type → worker
                                      │ publish (exchange "jobs", routing key = worker)
                                      ▼
                                  RabbitMQ ── jobs.celery | jobs.taskiq | jobs.asyncio | jobs.go
                                      ▼
                                  worker ──► Postgres: job_results + jobs.status = DONE
```

Quem conhece o quê:

| Componente | Conhece | Ignora de propósito |
|---|---|---|
| gateways | Postgres | broker, router, workers |
| relay | Postgres (outbox), URL do router | filas, workers |
| router | tabela `type → worker`, RabbitMQ | banco, de onde veio o job |
| workers | sua fila, Postgres (resultado) | gateways, router, outros workers |

Esse isolamento é o objetivo do projeto. Para adicionar um worker novo mexe-se no router (tabela), no `definitions.json` (fila e binding) e no próprio worker. Nenhum gateway muda.

---

## 3. Contrato único

`contracts/envelope.schema.json` define a mensagem que trafega entre gateway, outbox, relay, router e workers:

```json
{ "job_id": "uuid", "type": "email.send", "payload": {}, "created_at": "...", "origin": "gateway-go" }
```

Pontos de estudo:

- **O envelope é gravado pronto na outbox** (coluna `envelope jsonb`). O relay não monta nada: lê e reenvia os bytes. O router lê só `type` e `job_id` e **republica o corpo original intacto**. Assim, um campo novo no contrato passa por componentes que não o conhecem.
- `additionalProperties: false` no schema deixa o contrato estrito para quem valida; mudança de contrato exige mudar produtor e consumidores no mesmo passo (regra do `AGENTS.md`).
- `origin` (`gateway-py` ou `gateway-go`) permite, numa demo, ver qual API originou o job sem tracing distribuído. `traceparent` e `attempt` foram cortados de propósito (sem observabilidade nem retry nesta versão).

---

## 4. Banco de dados (`db/init.sql`)

Três tabelas, cada uma com um papel diferente:

| Tabela | Papel | Escrita por |
|---|---|---|
| `jobs` | estado de negócio do job (`PENDING → DISPATCHED → DONE/FAILED`) | gateway cria; relay e worker atualizam |
| `outbox` | fila durável de entrega (`pending → sent/failed`) | gateway cria; relay atualiza |
| `job_results` | resultado por worker, PK `(job_id, worker)` | workers |

Detalhes que vale entender:

- **Dois status, duas perguntas.** `outbox.status` responde "o envelope já foi entregue ao router?"; `jobs.status` responde "em que ponto do ciclo de vida o job está?". Misturá-los esconderia onde o job travou.
- **Índice parcial** `outbox_pending_idx` (só linhas `pending`): o relay consulta sempre as pendentes; o índice fica pequeno mesmo com milhões de linhas `sent`.
- **PK composta em `job_results`** é a base da idempotência: o mesmo worker gravando o mesmo `job_id` duas vezes bate na PK e o `ON CONFLICT DO NOTHING` descarta. Veja a seção 8.
- `jobs.payload` é jsonb sem schema por tipo. É uma simplificação consciente da demo.

### UUIDv7 como id **[implementado]**

Os dois gateways geram `job_id` em UUIDv7 (Python: `uuid-utils`; Go: `uuid.NewV7()`). Diferente do v4, o v7 começa com um timestamp, então inserts caem no fim do índice da PK em vez de espalhar pela árvore B. É melhor para localidade de cache e fragmentação em tabelas grandes, e dá ordenação aproximada por criação. O Python 3.13 não tem `uuid.uuid7()` na stdlib (entra no 3.14), por isso a dependência; o Postgres 17 também não tem `uuidv7()` (é do 18), por isso o id é gerado na aplicação.

---

## 5. Gateways **[implementado]**

Dois serviços com a **mesma API**, para comparar stacks sem mudar o comportamento:

| | `POST /jobs` | `GET /jobs/{id}` |
|---|---|---|
| sucesso | `202 {"job_id"}` | `200 {id,type,status,origin,created_at,results[]}` |
| erros | `422 {"detail": "type is required"}` / `"invalid request body"` | `422 "invalid job id"`, `404 "job not found"` |

### gateway-py (FastAPI + psycopg3)

- **Camadas:** `models.py` (Pydantic: borda HTTP e envelope), `repository.py` (SQL), `main.py` (rotas e wiring).
- **`lifespan`:** o pool de conexões é aberto no startup e fechado no shutdown, e o `JobRepository` fica em `app.state`. Estado de processo nasce ali, não em variável global.
- **Injeção por `Depends(get_repository)`:** os testes trocam o repositório por um fake via `app.dependency_overrides`, sem banco. O `ASGITransport` do httpx não roda o `lifespan`, então o teste HTTP nem tenta conectar.
- **Transação:** `async with pool.connection() as conn, conn.transaction():` envolve os dois `INSERT`. Se o segundo falhar, o primeiro desfaz.
- **`Jsonb(...)`** do psycopg adapta dict Python para jsonb. O envelope é serializado com `model_dump(mode="json")`, que converte UUID e datetime em strings compatíveis com o contrato.
- **Erros 422:** o FastAPI devolve por padrão uma lista de erros do Pydantic. Um `@app.exception_handler(RequestValidationError)` olha o campo (`loc`) que falhou e responde o mesmo texto curto do gateway-go. É o que mantém "mesma API" verdadeiro de fato, não só nos caminhos felizes.
- **Pool com `check`:** `AsyncConnectionPool(..., check=AsyncConnectionPool.check_connection)`. Sem isso, depois de um restart do Postgres o pool entrega uma conexão morta e o primeiro request recebe 500 (`AdminShutdown`). Foi um bug real do projeto; existe um teste que derruba as conexões do pool via `pg_terminate_backend` e verifica a recuperação.

### gateway-go (Gin + pgx)

- **`handler.go`:** `NewRouter(handler)` monta o `gin.Engine`; `JobHandler` recebe o repositório e a `origin` por construtor. Sem estado global.
- **Binding:** `binding:"required"` no campo `Type` faz o `ShouldBindJSON` falhar com `validator.ValidationErrors`; qualquer outro erro (JSON quebrado) vira `invalid request body`. `describeBindError` faz essa tradução.
- **`pgxpool`** gerencia as conexões e reconecta sozinho; por isso o Go não teve o bug do pool morto.
- **Swagger com `swaggo`:** a documentação vem de comentários `@Summary`, `@Param`, `@Router` nos handlers. `make swagger` roda o CLI `swag init` e gera `docs/` (`docs.go`, `swagger.json`, `swagger.yaml`), que o `main.go` importa por efeito colateral (`_ "gateway-go/docs"`). Mudou handler, regenera. Dois cuidados reais: a versão do CLI (`swag@latest`) e a da biblioteca `swag` no `go.mod` precisam ser compatíveis, senão o `docs.go` gerado não compila; e o Gin redireciona `/docs` para `/docs/` (301) onde o handler do swagger não responde, então `RegisterDocs` registra `/docs` e `/docs/` explicitamente com redirect para `/docs/index.html`.
- **`json.RawMessage` + `swaggertype:"object"`** em `JobResult.Result`: o resultado do worker é jsonb arbitrário, devolvido sem re-serializar, e o Swagger o descreve como objeto.

### Por que dois gateways?

Para mostrar que o contrato (envelope + tabelas), e não a linguagem, é a fronteira entre serviços. Ambos escrevem as mesmas linhas; o resto do sistema não distingue um do outro, exceto pelo campo `origin`.

---

## 6. Relay **[implementado]**

### Relay e router: quem faz o quê

Os dois ficam no meio do caminho, mas resolvem problemas diferentes:

| | Relay | Router |
|---|---|---|
| Pergunta que responde | "esta linha da outbox já foi entregue?" | "para qual worker este `type` vai?" |
| Lado do banco | lê/escreve Postgres (`outbox`, `jobs`) | não conhece o banco |
| Lado do broker | não conhece o RabbitMQ | só ele publica no RabbitMQ |
| Estado | tem (a outbox é a fonte de verdade da entrega) | nenhum; cada request é independente |
| Garante | **não perder** job: só marca `sent` após 2xx, reenvia se falhar | **destino correto** e publicação **confirmada** pelo broker |
| Traduz | linha da tabela → chamada HTTP | chamada HTTP → mensagem AMQP com routing key |

Em resumo: o **relay é a ponte entre banco e mundo externo** (cuida de confiabilidade e retentativa); o **router é a ponte entre HTTP e broker** (cuida de roteamento). Sem o relay, nada tiraria o job da outbox. Sem o router, o relay teria que conhecer filas e workers, e cada novo worker mexeria nele. Separados, cada um muda por um único motivo: o relay muda se a forma de entregar mudar; o router, se a tabela de workers mudar.

O status HTTP é o contrato entre eles: 2xx "pode marcar entregue", 4xx "a mensagem é inválida, não repita", 5xx "tente de novo depois".

### Dava para ser um serviço só?

Dava, e em muitos sistemas reais é um só (o "outbox relay" publica direto no broker, sem router). Os dois desenhos funcionam; é troca de custos.

**Vantagens de separar (desenho atual):**

- **Um motivo de mudança por serviço.** Nova fila ou worker mexe só no router. Mudança na forma de ler a outbox (polling, `LISTEN/NOTIFY`, lease) mexe só no relay.
- **Relay sem dependência de broker.** Ele fala HTTP e Postgres. Trocar RabbitMQ por Kafka ou SQS altera só o router.
- **Roteamento reutilizável.** Qualquer outro produtor (um cron, uma ferramenta de admin, outro serviço) pode fazer `POST /dispatch` sem passar pela outbox.
- **Escala e falha independentes.** O router é stateless: dá para ter várias réplicas atrás de um balanceador. O relay tem estado (locks da outbox) e pede cuidado.
- **Teste simples.** O router testa-se com um fake de `Publisher` e sem banco. O relay testa-se com um fake de HTTP e sem broker.
- **Didático.** É a fronteira que o projeto quer mostrar: persistência confiável de um lado, roteamento do outro.

**Vantagens de unificar:**

- **Menos peças:** um container, um Dockerfile, um deploy, um conjunto de logs.
- **Sem salto de rede:** menos latência e um modo de falha a menos (relay não precisa tratar "router fora do ar").
- **Semântica mais forte e simples:** o relay marcaria `sent` direto após o publisher confirm do broker, sem traduzir status HTTP em decisão. Também some o contrato 2xx/4xx/5xx a manter.
- **Menos código:** some o servidor HTTP, o cliente HTTP e a serialização intermediária.
- **Operação mais barata** em times pequenos.

**Custo de unificar:** a tabela de workers e a lógica de outbox passam a viver no mesmo binário, então o relay volta a conhecer filas e workers. Um worker novo obriga a redeploy de quem lê o banco, e o roteamento deixa de ser reaproveitável por outros produtores.

**Quando eu unificaria:** um único produtor (a outbox), um único broker, time pequeno e sem previsão de trocar o broker. **Quando manteria separado:** vários produtores, roteamento que muda com frequência, ou necessidade de escalar a publicação sem multiplicar leitores da outbox. Neste projeto a separação é uma escolha didática, não uma necessidade técnica.

Processo Go que drena a outbox. Arquivos de `services/relay/`:

| Arquivo | Papel |
|---|---|
| `dispatcher.go` | `Dispatcher.Send`: POST ao router e tradução do status HTTP em `Outcome` (`Delivered`, `Rejected`, `Retry`) |
| `relay.go` | `Relay`: `Run` (loop com ticker), `RunOnce` (um ciclo/transação), `claim` (SELECT ... SKIP LOCKED) e `mark` (UPDATE outbox + jobs) |
| `main.go` | lê `DATABASE_URL`, `ROUTER_URL`, `POLL_INTERVAL`; encerra limpo em SIGINT/SIGTERM |

`Sender` é a interface que o `Relay` usa; o teste injeta um fake e exercita o banco real sem router. Verificado de ponta a ponta: com o router parado o job fica `PENDING`; ao voltar, vira `DISPATCHED`; `type` desconhecido vira `FAILED`.

Um ciclo (`RunOnce`):

1. Abre transação e seleciona linhas `pending` com `FOR UPDATE SKIP LOCKED`.
2. Para cada uma, `POST` do envelope ao router.
3. Pelo status HTTP decide o resultado:

| Resposta do router | Resultado | Efeito no banco |
|---|---|---|
| 2xx | entregue | `outbox → sent`; `jobs → DISPATCHED` (só se ainda `PENDING`) |
| 4xx (ex.: `type` desconhecido) | rejeitado | `outbox → failed`; `jobs → FAILED` |
| 5xx ou erro de rede | tentar depois | nada muda; a linha segue `pending` |

Mecanismos para estudar:

- **`FOR UPDATE SKIP LOCKED`:** permite várias instâncias do relay sem entregar a mesma linha em dobro. Quem chega depois pula as linhas já travadas em vez de esperar. É o padrão clássico de "fila em tabela Postgres".
- **`jobs → DISPATCHED` só se `PENDING`:** workers rápidos podem marcar `DONE` antes do relay atualizar. O `UPDATE ... WHERE status = 'PENDING'` impede que `DISPATCHED` sobrescreva `DONE`.
- **At-least-once:** se o relay cair entre o 2xx do router e o `COMMIT`, a linha continua `pending` e será reenviada. Por isso o resto do pipeline precisa ser idempotente. Não existe "exactly-once" aqui, e o projeto não finge que existe.
- **Limite conhecido:** a transação do relay fica aberta durante o `POST`. Simples e correto para a demo; em produção prenderia conexão e locks por mais tempo do que o ideal (alternativa: marcar `in_flight` com lease/timeout).

---

## 7. Router **[implementado]**

Webhook Go (`POST /dispatch`) que traduz `type` em destino e publica no RabbitMQ.

```
report.generate → celery      email.send → taskiq
http.fetch      → asyncio     image.resize → go
```

Status devolvidos (o relay depende deles): `202` publicado, `400` JSON inválido ou `type` vazio, `422` type desconhecido, `502` falha de publish.

Por que existe como serviço separado, em vez de o relay publicar direto:

- A **decisão de roteamento vive em um lugar só** e é trocável sem tocar no relay.
- O relay vira agnóstico de broker: fala HTTP. Dá para trocar RabbitMQ por outra coisa mexendo apenas no router.
- A diferença 4xx vs 5xx traduz "a mensagem é inválida, não adianta repetir" contra "o sistema está indisponível, repita". O relay não precisa entender por quê.

Arquivos de `services/router/` (cada um com uma responsabilidade):

| Arquivo | Papel |
|---|---|
| `routes.go` | tabela `type → worker` (`DefaultRoutes`) e `Lookup`. É o **único** lugar que conhece workers |
| `handler.go` | `DispatchHandler`: lê o corpo, extrai `type`, consulta a tabela, publica e traduz o resultado em status HTTP |
| `publisher.go` | `AMQPPublisher`: conexão, canal com confirms e `Publish`. Implementa a interface `Publisher` |
| `main.go` | monta publisher + handler e sobe `POST /dispatch` (estado de processo nasce aqui, sem global) |

Fluxo de um request: corpo bruto → `json.Unmarshal` só de `job_id` e `type` → `Lookup` → `Publish(worker, corpo bruto)` → 202. O corpo é republicado **byte a byte**; o router não reserializa nada.

Para ver na prática (a porta 8080 é interna ao compose):

```bash
docker exec simple-microservices-router-1 wget -qO- --post-data='{"job_id":"x","type":"email.send"}' http://localhost:8080/dispatch
# 202 e 1 mensagem em jobs.taskiq (RabbitMQ management em localhost:55673); type "nope" devolve 422
```

Detalhes de implementação:

- **Publisher confirms** (`ch.Confirm` + `PublishWithDeferredConfirmWithContext`): o router só responde 202 depois que o broker confirmou que persistiu a mensagem. Sem confirm, um 202 poderia mentir.
- **`DeliveryMode: Persistent`** + fila durável: a mensagem sobrevive a restart do broker.
- **Mutex no publish:** o `amqp.Channel` não é seguro para uso concorrente, e o handler HTTP roda uma goroutine por request.
- **Interface `Publisher`** injetada no handler: o teste usa um fake e valida roteamento e status sem broker.
- **Não reconecta sozinho, mas morre rápido.** `conn.NotifyClose` avisa quando o broker derruba a conexão; o `main` retorna erro, o processo sai e o compose o reinicia (`restart: unless-stopped`), já reconectado. Sem isso o router ficaria vivo respondendo 502 para sempre (bug real encontrado ao recriar o RabbitMQ). O worker-go faz o mesmo: o fim do canal de entregas vira erro. O worker-asyncio usa `connect_robust`, que reconecta sem reiniciar.

---

## 8. RabbitMQ **[infra implementada, consumidores planejados]**

### Topologia

Declarada **uma vez**, em `infra/rabbitmq/definitions.json`, carregada no boot do broker (`load_definitions` no `rabbitmq.conf`): exchange `jobs` (tipo **direct**), quatro filas duráveis e quatro bindings com routing key igual ao nome do worker. Nenhum serviço declara filas, então não há divergência de argumentos entre produtor e consumidor (o erro `PRECONDITION_FAILED` clássico).

Direct exchange basta porque o roteamento já foi decidido pelo router: a routing key *é* o destino. Topic ou fanout seriam mecanismo sem uso.

### Armadilha que apareceu no projeto

Carregar `definitions.json` **substitui** o usuário padrão do broker: sem uma seção `users`, não existe nenhum usuário e toda conexão leva `403 username or password not allowed`. Além disso, o `guest` só aceita conexões vindas de loopback, o que não vale entre containers. O projeto define `guest/guest` com permissões no JSON e liga `loopback_users = none`. Isso só é aceitável numa demo local.

### Ack, nack e garantias

- Consumidores usam **ack manual**: só confirmam depois de gravar o resultado. Se o worker morre no meio, a mensagem volta à fila.
- Mensagem inválida (JSON quebrado, campo faltando): `reject`/`nack` **sem requeue** e log. Reenfileirar uma mensagem que nunca vai parsear criaria loop infinito. Não há DLQ nesta versão, então ela é descartada.
- `prefetch` limita quantas mensagens um consumidor mantém sem ack; é também o limite de concorrência do worker Go (uma goroutine por mensagem).

---

## 9. Workers **[worker-go e worker-asyncio implementados; demais planejados]**

Os quatro executam a mesma tarefa trivial (espera curta, grava `{"worker","detail"}`), para que a **diferença esteja no modelo de execução**, não na lógica.

| Worker | Modelo | O que mostra |
|---|---|---|
| `worker-go` | consumer + uma goroutine por mensagem, limitada pelo prefetch | concorrência nativa e barata |
| `worker-asyncio` | `aio-pika` puro, event loop | I/O concorrente em uma thread, sem framework |
| `worker-celery` | bridge `aio-pika` + task Celery | framework de tarefas maduro (workers por processo/pool) |
| `worker-taskiq` | bridge `aio-pika` + task TaskIQ | framework async-first, API estilo FastAPI |

### worker-go **[implementado]**

Arquivos de `services/worker-go/`:

| Arquivo | Papel |
|---|---|
| `handler.go` | `Envelope`/`Result` e `Process`: a lógica simulada de `image.resize`. Devolve `ErrUnsupportedType` para outros types |
| `store.go` | `ResultStore.Save`: `INSERT ... ON CONFLICT DO NOTHING` em `job_results` + `jobs → DONE`, numa transação |
| `consumer.go` | `Consumer`: `Qos(prefetch)`, `Consume` sem auto-ack, uma goroutine por `Delivery`, `WaitGroup` no shutdown |
| `main.go` | monta pool, conexão AMQP e consumer; encerra em SIGINT/SIGTERM |

Pontos para estudar:

- **Prefetch é o limitador de concorrência.** `Qos(10)` faz o broker entregar no máximo 10 mensagens sem ack; como cada uma ganha uma goroutine, são no máximo 10 em paralelo. Não há worker pool explícito.
- **Ack só depois de gravar.** `Ack` vem depois do `Save`. Se o processo morre no meio, a mensagem volta para a fila e `ON CONFLICT` absorve a repetição.
- **Três destinos de falha.** JSON inválido ou type não suportado: `Nack` sem requeue (repetir não adianta). Erro transitório (banco): `Nack` com requeue. Sucesso: `Ack`.
- **Limite conhecido:** o requeue de erro transitório não tem backoff, então um banco fora do ar gera loop rápido de reentrega. DLQ e retry estão fora de escopo.
- Verificado de ponta a ponta: `POST /jobs` (`image.resize`) em cada gateway chega a `DONE` com `results[0].worker == "go"` em poucos segundos.

### worker-asyncio **[implementado]**

Arquivos de `services/worker-asyncio/app/`:

| Arquivo | Papel |
|---|---|
| `models.py` | `Envelope` (contrato) e `Result`, ambos `pydantic.BaseModel` |
| `store.py` | `ResultStore.save`: insert idempotente + `jobs → DONE` numa transação |
| `consumer.py` | `JobConsumer`: QoS, `queue.consume`, validação, processamento e ack |
| `logs.py` | `JsonFormatter`: mesmo formato JSON dos serviços Go (`service`, `job_id`) |
| `main.py` | configura log, conecta (`connect_robust`) e inicia o consumer |

Pontos para estudar (compare com o worker-go):

- **Concorrência cooperativa.** O `aio-pika` roda uma task por mensagem no mesmo event loop, uma thread só. O `asyncio.sleep` cede o controle; um `time.sleep` ou cálculo pesado travaria todas as mensagens. Em Go, a goroutine bloqueada não atrapalha as outras.
- **Ack com `message.process(requeue=True)`.** O ack acontece quando o bloco termina sem exceção, isto é, depois de `save`. Exceção no bloco devolve a mensagem à fila.
- **Mensagem inválida ou type alheio:** `reject(requeue=False)`, igual ao `Nack(false, false)` do Go.
- **Reconexão:** `connect_robust` reconecta sozinho ao RabbitMQ; o worker-go e o router não têm isso.
- **Conexão por gravação:** `ResultStore` abre uma conexão curta a cada `save`. Simples e suficiente aqui; com carga real usaria um pool, como o gateway-py.

### O padrão bridge (Celery e TaskIQ)

Celery e TaskIQ esperam mensagens **no formato próprio** (headers, serialização, nome da task). O envelope neutro do projeto não é isso. Em vez de fazer o router conhecer formatos de framework, cada um desses workers tem **dois containers**:

1. **bridge:** consumer fino em `aio-pika` que lê `jobs.<worker>` e chama `process_job.delay(...)` (Celery) ou `process_job.kiq(...)` (TaskIQ).
2. **worker do framework:** processa a task e grava o resultado.

Implicação de ack: o bridge confirma a mensagem do RabbitMQ **depois de entregar a task ao broker do framework**, não depois do processamento. Para a garantia não ficar mais fraca, o Celery roda com `acks_late` (só confirma a task quando termina). Esse é o tipo de detalhe que separa "funciona na demo" de "perde job quando o worker cai".

### worker-celery [implementado]

Dois containers, mesma imagem (`services/worker-celery`), processos diferentes escolhidos pelo `command` do compose:

| Arquivo | Papel |
|---|---|
| `app/bridge.py` | classe `Bridge`: consome `jobs.celery` com aio-pika, valida o envelope e chama `process_job.delay(...)` numa thread (`asyncio.to_thread`, porque o `delay` bloqueia). Ack só depois do `delay`; inválida: `reject` sem requeue |
| `app/tasks.py` | app Celery e a task `process_job`: espera 0,2 s, grava o resultado e loga `job done` |
| `app/store.py` | `ResultStore` **síncrono** (`psycopg.connect`): o Celery roda tasks em processos filhos, sem event loop |
| `app/logs.py`, `app/models.py` | mesmo log JSON e mesmos modelos do worker-asyncio |

Detalhes que valem estudar:

- **Dois ack, duas garantias.** O bridge confirma `jobs.celery` quando a task entrou na fila interna `celery`; dali em diante vale `task_acks_late=True`: o Celery só confirma depois de a task terminar. Worker morto no meio devolve a task à fila.
- **`worker_prefetch_multiplier=1`:** cada processo reserva uma task por vez; sem isso um job lento seguraria outros já reservados. `--concurrency=4` define os 4 processos.
- **Remote control desligado:** o Celery usa filas transient não-exclusivas (pidbox, mingle, gossip) para `inspect`/`revoke`. O RabbitMQ recente recusa esse tipo de fila (`transient_nonexcl_queues` deprecated), e o worker entrava em loop de "Connection to broker lost". Como a demo não usa esses comandos, `worker_enable_remote_control=False` e `--without-mingle --without-gossip`.
- **Log:** o sinal `setup_logging` troca o formato do Celery pelo JSON do projeto.
- **PATH:** o Dockerfile coloca `/app/.venv/bin` no `PATH`, assim o compose chama `celery` e `python` direto.

### Idempotência

Como a entrega é at-least-once, o mesmo `job_id` pode chegar duas vezes. Cada worker faz:

```sql
INSERT INTO job_results (job_id, worker, result) VALUES (...)
ON CONFLICT (job_id, worker) DO NOTHING
```

A segunda execução não duplica nem falha. O ack vem **depois** desse insert. Observe que a idempotência está no **efeito gravado**, não em tentar evitar a repetição.

---

## 10. Garantias e o que ficou de fora

| Propriedade | Como é obtida | Onde quebra (por escolha) |
|---|---|---|
| Job aceito não se perde | outbox na mesma transação | disco do Postgres |
| Entrega at-least-once | relay só marca `sent` após 2xx | duplicatas possíveis |
| Efeito único | `ON CONFLICT DO NOTHING` | só protege o resultado gravado, não efeitos externos |
| Isolamento de falha | gateway independe de broker/router | job fica `pending` até voltarem |

Cortes deliberados do projeto anterior: benchmark, OpenTelemetry/Grafana/Prometheus, fanout, DLQ, retry com backoff, schema por tipo de job, autenticação, CI/CD. Cada um é um bom exercício de extensão (veja a seção 12).

---

## 10.1 Logs e rastreio de um job **[serviços Go]**

> Passo a passo prático (comandos, RabbitMQ, banco, diagnóstico): `docs/OBSERVABILIDADE.md`.

Cada serviço Go loga em JSON, uma linha por evento, via `log/slog`. O `main` configura o logger uma vez (`slog.SetDefault(...).With("service", "<nome>")`) e o resto do código só chama `slog.Info/Warn/Error`. Sem biblioteca extra e sem logger passado por parâmetro.

O campo que une tudo é `job_id`: o mesmo id aparece em cada etapa, então `docker compose logs | grep <job_id>` conta a história do job.

| Serviço | Evento (`msg`) | Nível |
|---|---|---|
| gateway-go | `job accepted`, `request` (método, rota, status, `duration_ms`) | INFO |
| router | `job published` (com `worker`) / `unknown job type` | INFO / WARN |
| relay | `job delivered` / `job rejected, marked failed` / `job delivery failed, will retry` | INFO / WARN |
| worker-go | `job done` / `unsupported job type, rejected` / `job failed, requeued` | INFO / WARN / ERROR |

Convenção de nível: INFO é o caminho normal; WARN é falha esperada que o sistema trata (type desconhecido, router fora, mensagem rejeitada); ERROR é algo inesperado (banco, ack).

Exemplo de um job `bogus` que termina em `FAILED`: `job accepted` (gateway) → `unknown job type` (router) → `job rejected, marked failed` (relay). O motivo do `FAILED` agora está nos logs.

Os serviços Python ainda não seguem este formato; entram na etapa Python.

---

## 11. Como o código impõe a arquitetura

As regras do `AGENTS.md` não são estilo, são o que mantém as fronteiras:

- **Sem imports entre serviços.** O contrato compartilhado fica em `contracts/`; cada serviço tem `pyproject.toml`/`go.mod` e `Dockerfile` próprios.
- **Classes/structs com dependências injetadas, sem estado global.** É o que permite os testes sem infraestrutura (repositório fake no Python, `Publisher` fake no Go).
- **Gateways nunca publicam no broker.** Se alguém fizer, quebra a garantia da outbox.
- **Testes só de comportamento real** (outbox atômica, roteamento, idempotência, validação). Testes de integração usam `TEST_DATABASE_URL`, fazem `TRUNCATE` e são pulados sem a variável; por isso só rodam contra o Postgres de dev do compose.
- **Uma etapa por vez, uma linguagem por etapa.** Mantém cada mudança revisável.

---

## 12. Roteiro de estudo sugerido

1. **Suba o que existe:** `make up`; abra os dois Swaggers (`:58001/docs`, `:58002/docs`). Crie jobs pelos dois e veja `jobs` e `outbox` no Postgres (`docker compose exec postgres psql -U app`). Confirme que as linhas são idênticas exceto `origin`.
2. **Prove a atomicidade:** force um erro no segundo `INSERT` (ex.: quebre temporariamente o nome da coluna da outbox) e confirme que o job também não foi gravado.
3. **Mate o Postgres** com o gateway no ar (`docker compose restart postgres`) e observe o comportamento nos dois gateways. Remova o `check` do pool Python e veja o 500 voltar.
4. **Leia `definitions.json`** e tente consumir/publicar à mão pela UI de management (`:55673`).
5. **Quando relay e router existirem:** derrube o router e crie jobs; observe a outbox acumular; suba o router e veja drenar. Depois mande um `type` inexistente e acompanhe o caminho até `FAILED`.
6. **Quando os workers existirem:** envie o mesmo envelope duas vezes direto na fila e confirme que `job_results` tem uma linha só.

### Extensões para praticar

- Retry com backoff e DLQ (comece pelo `x-dead-letter-exchange` nas filas).
- Lease/`in_flight` na outbox para não segurar transação durante o POST do relay.
- Reconexão automática do router e dos workers.
- `traceparent` no envelope e OpenTelemetry ponta a ponta.
- Schema por tipo de job validado no gateway.
- Trocar polling por `LISTEN/NOTIFY` do Postgres para reduzir latência do relay.

---

## 13. Python ou Go: quando cada um faz sentido

O projeto usa os dois de propósito, mas a divisão não é sorteio. Cada escolha tem um critério.

| Componente | Linguagem | Por quê |
|---|---|---|
| gateways | Python **e** Go | é a comparação do estudo: mesma API, mesmo contrato, duas stacks. Dá para ver o que muda (validação, Swagger, pool de conexão) e o que não muda |
| relay, router | Go | processos pequenos, sem regra de negócio, que ficam rodando o tempo todo e fazem I/O concorrente. Binário estático, imagem de poucos MB, sem runtime para manter |
| worker-go | Go | mostra o modelo de concorrência nativo: uma goroutine por mensagem, limitada pelo prefetch |
| worker-asyncio | Python | mostra concorrência cooperativa (`async/await`) num único processo |
| worker-celery, worker-taskiq | Python | Celery e TaskIQ são frameworks Python. Existem para quem já tem código Python e quer filas sem escrever consumer |

### Onde Go costuma ganhar

- **Serviços de infraestrutura** (proxy, relay, router, consumers simples): binário único, baixo uso de memória, inicialização instantânea.
- **Concorrência alta e barata:** goroutines usam pouca memória e o runtime as distribui por todos os núcleos. Não há GIL.
- **Trabalho CPU-bound** dentro do mesmo processo (ex.: `image.resize`): em Python puro, threads não paralelizam CPU.
- **Erros explícitos e tipagem estática** por padrão, o que ajuda em código que quase não muda e precisa ser previsível.
- **Deploy simples:** `CGO_ENABLED=0` gera um arquivo só, copiado para uma imagem `alpine`.

### Onde Python costuma ganhar

- **Velocidade de desenvolvimento e ecossistema:** bibliotecas de dados, ML, PDF, integrações. Um worker `report.generate` real provavelmente usaria pandas ou similar, que não têm equivalente tão maduro em Go.
- **Frameworks prontos de fila e API:** Celery (retries, agendamento, canvas), FastAPI (validação e OpenAPI quase de graça via Pydantic).
- **Time que já domina Python:** o custo de manutenção pesa mais que o ganho de desempenho.
- **Trabalho I/O-bound:** com `asyncio` o Python aguenta muitas conexões simultâneas; o gargalo costuma ser rede ou banco, não a linguagem.

### Como decidir (regra prática)

1. O componente é **encanamento** (move dados, sem lógica de domínio)? Go.
2. O componente depende de **biblioteca que só existe em Python**, ou o time vive em Python? Python.
3. É **CPU-bound** e precisa de paralelismo real? Go (ou Python com processos/extensões nativas, com mais complexidade).
4. É I/O-bound e simples? Os dois servem; vence o que o time mantém melhor.

### Aviso honesto sobre este projeto

Aqui os handlers dos workers só dormem um instante e gravam resultado, e não há benchmark (está fora de escopo). **Nada neste repositório prova que um é mais rápido.** Os números de memória e throughput que se ouvem por aí dependem de carga real. O que o projeto mostra é a **forma** de cada stack: como cada uma estrutura concorrência, validação, ack e testes. Use-o para comparar o código, não para escolher por desempenho.

---

## 14. Glossário rápido de decisões

| Escolha | Alternativa | Por que esta |
|---|---|---|
| Outbox + relay | publicar direto; CDC (Debezium) | zero infra extra, atômico, fácil de ler |
| Router HTTP | relay publica direto no broker | roteamento num lugar só, relay agnóstico |
| Tabela fixa `type → worker` | regras dinâmicas, routing por header | demo: explícito e testável |
| RabbitMQ direct | Kafka, Redis Streams | roteamento por chave e ack por mensagem sem complexidade |
| Topologia em `definitions.json` | cada serviço declara | uma fonte da verdade, sem conflito de argumentos |
| Bridge para Celery/TaskIQ | router formata p/ cada framework | mantém o envelope neutro e o router ignorante |
| Idempotência por PK | dedupe em memória/cache | sobrevive a restart e a múltiplas instâncias |
| UUIDv7 gerado na app | v4; `uuidv7()` do PG 18 | ordenável por tempo, sem depender de versão do banco |
