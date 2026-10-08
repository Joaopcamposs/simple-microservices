"""Task TaskIQ que processa o job `email.send`.

Este módulo é o "worker do framework": o processo `taskiq worker` importa o
`broker`, consome a fila interna `taskiq` e executa `process_job`. Quem alimenta
essa fila é o bridge (bridge.py).
"""

import asyncio
import logging
import os

from taskiq import TaskiqEvents, TaskiqState
from taskiq_aio_pika import AioPikaBroker

from app.logs import JsonFormatter
from app.models import Envelope, Result
from app.store import ResultStore

logger = logging.getLogger(__name__)

# Sem task_queues, o broker usa a fila padrão `taskiq` (durável) no exchange `taskiq`.
broker = AioPikaBroker(os.environ.get("TASKIQ_BROKER_URL", "amqp://guest:guest@localhost:55672/"))


@broker.on_event(TaskiqEvents.WORKER_STARTUP)
async def configure_logging(state: TaskiqState) -> None:
    """Troca o formato de log do TaskIQ pelo JSON do projeto (roda só no processo worker)."""
    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter("worker-taskiq"))
    logging.basicConfig(level=logging.INFO, handlers=[handler], force=True)


@broker.task(task_name="process_job")
async def process_job(envelope: dict[str, object]) -> None:
    """Processa o job (simulado com uma espera curta) e grava o resultado."""
    parsed = Envelope.model_validate(envelope)
    await asyncio.sleep(0.2)
    store = ResultStore(os.environ.get("DATABASE_URL", "postgres://app:app@localhost:55432/app"))
    await store.save(parsed.job_id, "taskiq", Result(worker="taskiq", detail="email sent"))
    logger.info("job done", extra={"job_id": parsed.job_id})
