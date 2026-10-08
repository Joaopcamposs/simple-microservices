# Comparação dos workers

`make bench` mede quanto tempo cada worker leva para zerar 100 jobs em três cargas de trabalho. A pergunta é "quando usar Celery, TaskIQ, asyncio ou Go", respondida com um experimento pequeno, não com um benchmark de laboratório.

## Como funciona

O campo opcional `workload` do `payload` escolhe a carga (padrão `io-wait`; valor desconhecido é rejeitado). O contrato não muda: o `payload` já era livre.

| Carga | O que faz |
|---|---|
| `io-wait` | espera 0,2 s cedendo a vez (`await asyncio.sleep`, `time.After`, `time.sleep` no processo do Celery) |
| `io-block` | espera 0,2 s **bloqueando** (`time.sleep` direto na corrotina dos workers asyncio e TaskIQ) |
| `cpu` | PBKDF2-SHA256 com 600 000 iterações (~100 ms) |

Parâmetros iguais nos quatro workers, concorrência igualada em 4: `CONCURRENCY=4` (worker-go, worker-asyncio), `--concurrency=4` (Celery, processos) e `--max-async-tasks 4` (TaskIQ).

O script pula gateway, outbox e relay (o relay entrega no máximo 10 jobs por segundo e esconderia o worker). Ele insere os jobs como `DISPATCHED` e publica os envelopes na exchange `jobs`. O tempo vai da primeira publicação até o último `DONE`. Cada célula é a mediana de 3 rodadas, depois de um aquecimento descartado.

## Resultado

Máquina local (10 núcleos), Docker, N = 100:

| worker  | io-wait | io-block | cpu |
|---------|---|---|---|
| go      | 5.3s (19/s) | 5.3s (19/s) | 3.0s (33/s) |
| asyncio | 5.9s (17/s) | 21.2s (5/s) | 12.0s (8/s) |
| celery  | 5.7s (17/s) | 5.6s (18/s) | 3.4s (30/s) |
| taskiq  | 5.8s (17/s) | 21.4s (5/s) | 12.0s (8/s) |

## O que o resultado mostra

- **I/O que cede (`io-wait`):** os quatro empatam. Com 0,2 s por job e concorrência 4, o teto é 20 jobs/s; o que sobra é overhead.
- **I/O que bloqueia (`io-block`):** asyncio e TaskIQ caem para 1/4. Uma chamada síncrona dentro da corrotina trava o event loop inteiro, e as 4 "tasks" concorrentes viram uma fila. Go e Celery não sentem: bloquear uma goroutine ou um processo filho não afeta os outros.
- **CPU (`cpu`):** asyncio e TaskIQ têm uma thread e um núcleo (8/s); Go e Celery usam vários núcleos (30 a 33/s). O mesmo vale para qualquer carga de CPU em event loop: precisa de `to_thread` ou de processos.

Ou seja: asyncio e TaskIQ só valem para I/O não bloqueante de verdade; para o resto, Go ou Celery.

## Limites

- Medição em máquina local dentro do Docker: vale a ordem de grandeza, não o número absoluto.
- A publicação das 100 mensagens (poucos segundos, via API de management) se sobrepõe ao processamento, igual para todos os workers.
- Sem CPU, memória e percentis de latência: isso é o benchmark do projeto anterior (`microservices`), fora do escopo aqui.
