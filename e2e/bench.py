"""Tempo em carga de cada worker (`make bench`): tempo para zerar N jobs.

Cada worker executa o seu próprio job (redução de imagem, assinatura de relatório, envio
de e-mail, busca HTTP), então a tabela mostra como cada modelo se sai no que faz de melhor,
não uma disputa na mesma tarefa. Exige `make up`. Pula gateway, outbox e relay (o relay
entrega no máximo 10 jobs por segundo e esconderia o worker): insere os jobs no banco como
DISPATCHED e publica os envelopes direto na exchange `jobs`, como o router faria. O tempo
vai da primeira publicação até o último job chegar a DONE. Usa só a biblioteca padrão.
"""

import argparse
import json
import statistics
import sys
import time
from datetime import UTC, datetime

from e2e import ROUTES, Stack

TYPE_OF = {worker: job_type for job_type, worker in ROUTES.items()}
WORKERS = ("go", "asyncio", "celery", "taskiq")
TIMEOUT_S = 180.0


def run_batch(stack: Stack, worker: str, count: int) -> float:
    """Roda `count` jobs no worker e devolve os segundos até todos DONE."""
    stack.reset()
    job_type = TYPE_OF[worker]
    ids = stack.psql(
        f"INSERT INTO jobs (id, type, payload, status, origin) "
        f"SELECT gen_random_uuid(), '{job_type}', '{{}}', 'DISPATCHED', 'bench' "
        f"FROM generate_series(1, {count}) RETURNING id"
    ).splitlines()
    now = datetime.now(UTC).isoformat()
    start = time.monotonic()
    for job_id in ids:
        envelope = {
            "job_id": job_id,
            "type": job_type,
            "payload": {},
            "created_at": now,
            "origin": "bench",
        }
        stack.publish(worker, json.dumps(envelope))
    while int(stack.psql("SELECT count(*) FROM jobs WHERE status = 'DONE'")) < count:
        if time.monotonic() - start > TIMEOUT_S:
            raise TimeoutError(f"{worker}: não terminou em {TIMEOUT_S:.0f}s")
        time.sleep(0.1)
    return time.monotonic() - start


def format_cell(seconds: float, count: int) -> str:
    """Formata a mediana como `12.3s (8/s)`."""
    return f"{seconds:.1f}s ({count / seconds:.0f}/s)"


def main() -> int:
    """Roda cada worker (com aquecimento), toma a mediana das rodadas e imprime a tabela."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--n", type=int, default=100, help="jobs por rodada")
    parser.add_argument("--reps", type=int, default=3, help="rodadas por worker (mediana)")
    args = parser.parse_args()
    stack = Stack()
    medians: dict[str, float] = {}
    try:
        for worker in WORKERS:
            run_batch(stack, worker, 8)  # aquecimento: conexões e imports
            runs = [run_batch(stack, worker, args.n) for _ in range(args.reps)]
            medians[worker] = statistics.median(runs)
            print(f"{worker:8} {runs}", file=sys.stderr, flush=True)
    finally:
        stack.reset()
    print(f"\nN={args.n}, mediana de {args.reps} rodadas\n")
    print("| worker  | job | tempo |")
    print("|---------|-----|-------|")
    for worker in WORKERS:
        print(f"| {worker:7} | {TYPE_OF[worker]} | {format_cell(medians[worker], args.n)} |")
    return 0


if __name__ == "__main__":
    sys.exit(main())
