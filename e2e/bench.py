"""Comparação dos workers (`make bench`): tempo para zerar N jobs por carga de trabalho.

Exige `make up`. Pula gateway, outbox e relay (o relay entrega no máximo 10 jobs por
segundo e esconderia o worker): insere os jobs no banco como DISPATCHED e publica os
envelopes direto na exchange `jobs`, como o router faria. O tempo vai da primeira
publicação até o último job chegar a DONE. Usa só a biblioteca padrão.
"""

import argparse
import json
import statistics
import sys
import time
from datetime import UTC, datetime

from e2e import ROUTES, Stack

WORKLOADS = ("io-wait", "io-block", "cpu")
TYPE_OF = {worker: job_type for job_type, worker in ROUTES.items()}
WORKERS = ("go", "asyncio", "celery", "taskiq")
TIMEOUT_S = 180.0


def run_batch(stack: Stack, worker: str, workload: str, count: int) -> float:
    """Roda `count` jobs no worker com a carga dada e devolve os segundos até todos DONE."""
    stack.reset()
    job_type = TYPE_OF[worker]
    payload = json.dumps({"workload": workload})
    ids = stack.psql(
        f"INSERT INTO jobs (id, type, payload, status, origin) "
        f"SELECT gen_random_uuid(), '{job_type}', '{payload}', 'DISPATCHED', 'bench' "
        f"FROM generate_series(1, {count}) RETURNING id"
    ).splitlines()
    now = datetime.now(UTC).isoformat()
    start = time.monotonic()
    for job_id in ids:
        envelope = {
            "job_id": job_id,
            "type": job_type,
            "payload": {"workload": workload},
            "created_at": now,
            "origin": "bench",
        }
        stack.publish(worker, json.dumps(envelope))
    while int(stack.psql("SELECT count(*) FROM jobs WHERE status = 'DONE'")) < count:
        if time.monotonic() - start > TIMEOUT_S:
            raise TimeoutError(f"{worker}/{workload}: não terminou em {TIMEOUT_S:.0f}s")
        time.sleep(0.1)
    return time.monotonic() - start


def format_cell(seconds: float, count: int) -> str:
    """Formata a mediana como `12.3s (8/s)`."""
    return f"{seconds:.1f}s ({count / seconds:.0f}/s)"


def main() -> int:
    """Roda a matriz worker × carga, descarta o aquecimento e imprime a tabela."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--n", type=int, default=100, help="jobs por rodada")
    parser.add_argument("--reps", type=int, default=3, help="rodadas por célula (mediana)")
    args = parser.parse_args()
    stack = Stack()
    medians: dict[tuple[str, str], float] = {}
    try:
        for worker in WORKERS:
            run_batch(stack, worker, "io-wait", 8)  # aquecimento: conexões e imports
            for workload in WORKLOADS:
                runs = [run_batch(stack, worker, workload, args.n) for _ in range(args.reps)]
                medians[(worker, workload)] = statistics.median(runs)
                print(f"{worker:8} {workload:8} {runs}", file=sys.stderr, flush=True)
    finally:
        stack.reset()
    print(f"\nN={args.n}, mediana de {args.reps} rodadas\n")
    print(f"| worker  | {' | '.join(WORKLOADS)} |")
    print(f"|---------|{'|'.join('---' for _ in WORKLOADS)}|")
    for worker in WORKERS:
        cells = " | ".join(format_cell(medians[(worker, w)], args.n) for w in WORKLOADS)
        print(f"| {worker:7} | {cells} |")
    return 0


if __name__ == "__main__":
    sys.exit(main())
