"""Cargas de trabalho que o payload pode pedir (campo `workload`).

Existem para a comparação entre workers (`make bench`): o mesmo job em três
perfis de execução, com os mesmos parâmetros nos quatro workers.
"""

import hashlib
import time

IO_WAIT = "io-wait"
IO_BLOCK = "io-block"
CPU = "cpu"
WORKLOADS = frozenset({IO_WAIT, IO_BLOCK, CPU})

IO_DELAY = 0.2  # segundos de I/O simulado
CPU_ITERATIONS = 600_000  # PBKDF2-SHA256: cerca de 100 ms


def workload_of(payload: dict[str, object]) -> str:
    """Lê `workload` do payload (padrão io-wait); levanta ValueError se desconhecido."""
    workload = payload.get("workload", IO_WAIT)
    if not isinstance(workload, str) or workload not in WORKLOADS:
        raise ValueError(f"unsupported workload: {workload!r}")
    return workload


def run_workload(workload: str) -> None:
    """Executa a carga na thread do processo filho do Celery.

    Em prefork, esperar e calcular bloqueiam só aquele processo: `io-wait` e
    `io-block` são equivalentes, e o `cpu` usa um núcleo por processo.
    """
    if workload in (IO_WAIT, IO_BLOCK):
        time.sleep(IO_DELAY)
    else:
        hashlib.pbkdf2_hmac("sha256", b"bench", b"salt", CPU_ITERATIONS)
