# Logs e acompanhamento dos serviços

Guia prático para ver um job andar pelo sistema: logs, RabbitMQ e banco. Para o porquê dos mecanismos, veja `ESTUDO.md` (seção 10.1).

## 1. O caminho de um job nos logs

Todo serviço loga em JSON com `service` e, quando há job, `job_id`. Um job `report.generate` deixa estas linhas, nesta ordem:

| # | Serviço | Mensagem | Significa |
|---|---|---|---|
| 1 | gateway-py / gateway-go | `job accepted` | gravou `jobs` + `outbox` |
| 2 | relay | `job delivered` | router respondeu 202 |
| 3 | router | `job published` | publicou no exchange `jobs` |
| 4 | worker-celery-bridge | `job handed to celery` | entregou a task ao Celery |
| 5 | worker-celery | `job done` | gravou o resultado, job `DONE` |

O `worker-taskiq` segue o mesmo caminho, com `job handed to taskiq` e `job done` (a fila interna é `taskiq`). Nos outros workers o passo 4 não existe: `worker-go` e `worker-asyncio` logam `job done` direto.

Mensagens de falha: `unknown job type` e `publish failed` (router), `router replied with error` e `job delivery failed, will retry` (relay), `invalid message, rejected` (workers Python), `invalid envelope` (router).

## 2. Comandos

### Só o caminho dos jobs (recomendado)

```bash
make logs-jobs                    # todos os jobs, uma linha por etapa
make logs-jobs JOB=<job_id>       # um job só
```

Saída:

```
02:23:35 gateway-py job accepted 01a11952-bb94-...
02:23:35 relay job delivered 01a11952-bb94-...
02:23:35 router job published 01a11952-bb94-...
02:23:35 worker-celery-bridge job handed to celery 01a11952-bb94-...
02:23:36 worker-celery job done 01a11952-bb94-...
```

Requer `jq`. Mostra os últimos 5 minutos e continua acompanhando (Ctrl+C sai). O histórico inicial sai agrupado por serviço, não por hora; linhas novas chegam em ordem. Para ordenar o histórico de um job, passe a saída por `sort`: `make logs-jobs JOB=<id> | sort` (a hora é a primeira coluna).

### Tudo, sem filtro

```bash
make logs                                        # todos os serviços, últimas 50 linhas
docker compose logs -f --no-log-prefix relay router   # serviços escolhidos, sem prefixo
```

`make logs` inclui postgres, rabbitmq e o banner do Celery, por isso é ruidoso. Use quando `logs-jobs` não mostrar nada.

### Buscar um job ou erros

```bash
docker compose logs --no-log-prefix | grep <job_id>
docker compose logs --no-log-prefix | grep -E '"level": ?"(ERROR|WARNING)"'
```

## 3. RabbitMQ

Management em `http://localhost:55673` (guest/guest).

- **Queues → `jobs.celery`, `jobs.taskiq`, `jobs.asyncio`, `jobs.go`:** *Ready* é o que ninguém pegou ainda; *Unacked* é o que um consumer pegou e não confirmou. No fluxo normal cada mensagem passa por 1 e volta a 0 em milissegundos.
- **Queues → `celery`:** fila interna do framework. *Ready* > 0 com job `DISPATCHED` indica worker Celery parado.
- **Exchanges → `jobs`:** bindings das quatro filas (routing key = nome do worker).
- **Queues → `jobs.dlq`:** mensagens rejeitadas pelos workers (JSON inválido, `type` não suportado). O `dlq-reaper` as consome na hora, então o contador normalmente é 0; se subir, o `dlq-reaper` está parado. Para ver o corpo e o header `x-death`, pare o `dlq-reaper` e use *Get messages*. O job correspondente vira `FAILED` (log `dead message, job marked failed` com `job_id` e `source_queue`).
- **Get messages:** espia o conteúdo sem consumir, com *Ack mode* "Nack message requeue true".

Pelo terminal:

```bash
docker exec simple-microservices-rabbitmq-1 rabbitmqctl list_queues name messages consumers
```

`consumers` deve ser ≥ 1 em `jobs.celery`, `jobs.taskiq`, `jobs.asyncio`, `jobs.go`, `celery` e `taskiq`. Zero significa consumer caído.

## 4. Banco

```bash
docker exec simple-microservices-postgres-1 psql -U app -c \
  "select id, type, status from jobs order by created_at desc limit 5"
docker exec simple-microservices-postgres-1 psql -U app -c \
  "select status, count(*) from outbox group by status"
```

`outbox` com `pending` acumulando indica relay ou router fora.

## 5. Onde o job está parado

| Status do job | Onde olhar |
|---|---|
| `PENDING` | `outbox` pendente: logs do `relay` e do `router`; o router está de pé? |
| `DISPATCHED` | `jobs.<worker>` com *Ready* > 0: consumer fora. Fila `celery` com *Ready* > 0: worker Celery fora |
| `FAILED` | `type` desconhecido: log `unknown job type` no router |
| `DONE` | `GET /jobs/{id}` traz `results[]` |

## 6. Testes de robustez (celery e taskiq)

Valem para os dois; troque `celery` por `taskiq` (routing key, fila interna, serviço).

- **Idempotência:** no management, *Exchanges → jobs → Publish message*, routing key `celery`, com o envelope do job (`select envelope from outbox where job_id = '...'`). O job continua com uma linha em `results`; o bridge loga `job handed to celery` de novo, o que é esperado.
- **Mensagem inválida:** publique `{"x":1}` do mesmo jeito. O bridge loga `invalid message, rejected` e a fila volta a 0, sem requeue.
- **Worker parado:** `docker compose stop worker-celery` (ou `worker-taskiq`), crie um job: fica `DISPATCHED` e a fila `celery` guarda a task. Depois de `docker compose start worker-celery` ele vai a `DONE`.

## 7. Limpar o estado de teste

```bash
docker exec simple-microservices-postgres-1 psql -U app -c "TRUNCATE job_results, outbox, jobs CASCADE"
for q in jobs.celery jobs.taskiq jobs.asyncio jobs.go celery taskiq; do
  docker exec simple-microservices-rabbitmq-1 rabbitmqctl purge_queue $q
done
```
