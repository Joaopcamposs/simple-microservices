"""Bridge: lê o envelope de jobs.celery e o entrega ao Celery.

O envelope neutro não é o formato nativo do Celery, então este consumer fino
faz a ponte (padrão bridge): lê da fila do contrato e chama `process_job.delay`,
que grava a task na fila interna do Celery. Assim o router continua sem saber
nada de frameworks.
"""

import asyncio
import logging
import os

import aio_pika
from aio_pika import IncomingMessage
from pydantic import ValidationError

from app.logs import JsonFormatter
from app.models import Envelope
from app.tasks import process_job

logger = logging.getLogger(__name__)

QUEUE = "jobs.celery"
PREFETCH = 32


class Bridge:
    """Consome jobs.celery e repassa cada envelope ao Celery."""

    async def on_message(self, message: IncomingMessage) -> None:
        """Valida e enfileira no Celery.

        Inválida: reject sem requeue. Falha ao enfileirar: requeue. O ack só vem
        depois de `delay()` ter entregue a task à fila durável do Celery; a partir
        daí a garantia é do `acks_late` da task.
        """
        try:
            envelope = Envelope.model_validate_json(message.body)
        except ValidationError as exc:
            logger.warning("invalid message, rejected: %s", exc)
            await message.reject(requeue=False)
            return
        async with message.process(requeue=True):
            # delay() é bloqueante (I/O de rede do kombu): roda em thread para não travar o loop.
            await asyncio.to_thread(process_job.delay, envelope.model_dump(mode="json"))
            logger.info("job handed to celery", extra={"job_id": envelope.job_id})

    async def run(self) -> None:
        """Conecta ao RabbitMQ (reconexão automática) e consome jobs.celery."""
        connection = await aio_pika.connect_robust(
            os.environ.get("AMQP_URL", "amqp://guest:guest@localhost:55672/")
        )
        async with connection:
            channel = await connection.channel()
            await channel.set_qos(prefetch_count=PREFETCH)
            queue = await channel.get_queue(QUEUE)
            await queue.consume(self.on_message)
            logger.info("consuming %s", QUEUE)
            await asyncio.Event().wait()


def main() -> None:
    """Configura o log JSON e roda o bridge."""
    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter("worker-celery-bridge"))
    logging.basicConfig(level=logging.INFO, handlers=[handler])
    asyncio.run(Bridge().run())


if __name__ == "__main__":
    main()
