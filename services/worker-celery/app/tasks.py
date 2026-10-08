"""Task Celery que processa o job `report.generate`.

Este módulo é o "worker do framework": o processo `celery worker` o importa,
consome a fila interna `celery` e executa `process_job`. Quem alimenta essa
fila é o bridge (bridge.py).
"""

import logging
import os
import time
from typing import Any

from celery import Celery
from celery.signals import setup_logging

from app.logs import JsonFormatter
from app.models import Envelope, Result
from app.store import ResultStore

logger = logging.getLogger(__name__)

app = Celery(
    "worker_celery",
    broker=os.environ.get("CELERY_BROKER_URL", "amqp://guest:guest@localhost:55672//"),
)
# acks_late: o broker só recebe o ack depois que a task termina (grava o resultado).
# prefetch 1: cada processo pega uma task por vez, senão um job lento seguraria outros.
app.conf.task_acks_late = True
app.conf.worker_prefetch_multiplier = 1
# Remote control (pidbox) declara filas transient não-exclusivas, que o RabbitMQ novo recusa.
# A demo não usa inspect/revoke, então desliga.
app.conf.worker_enable_remote_control = False


@setup_logging.connect
def configure_logging(**_: Any) -> None:
    """Troca o formato de log do Celery pelo JSON do projeto (sinal chamado no boot)."""
    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter("worker-celery"))
    logging.basicConfig(level=logging.INFO, handlers=[handler], force=True)


@app.task(name="process_job")
def process_job(envelope: dict[str, object]) -> None:
    """Processa o job (simulado com uma espera curta) e grava o resultado."""
    parsed = Envelope.model_validate(envelope)
    time.sleep(0.2)
    store = ResultStore(os.environ.get("DATABASE_URL", "postgres://app:app@localhost:55432/app"))
    store.save(parsed.job_id, "celery", Result(worker="celery", detail="report generated"))
    logger.info("job done", extra={"job_id": parsed.job_id})
