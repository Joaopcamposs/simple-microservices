"""Bridge: lê o envelope de jobs.taskiq e o entrega ao TaskIQ.

O envelope neutro não é o formato nativo do TaskIQ, então este consumer fino
faz a ponte (padrão bridge): lê da fila do contrato e chama `process_job.kiq`,
que grava a task na fila interna do TaskIQ. Assim o router continua sem saber
nada de frameworks.
"""

import asyncio
import logging
import os

import aio_pika
from aio_pika.abc import AbstractIncomingMessage
from pydantic import ValidationError

from app.logs import JsonFormatter
from app.models import Envelope
from app.tasks import broker, process_job
from app.workload import workload_of

logger = logging.getLogger(__name__)

QUEUE = "jobs.taskiq"
PREFETCH = 32


class Bridge:
    """Consome jobs.taskiq e repassa cada envelope ao TaskIQ."""

    async def on_message(self, message: AbstractIncomingMessage) -> None:
        """Valida e enfileira no TaskIQ.

        Inválida: reject sem requeue. Falha ao enfileirar: requeue. O ack só vem
        depois de `kiq()` ter publicado a task na fila durável do TaskIQ; a partir
        daí o TaskIQ só confirma a task depois de executá-la (ack tardio).
        """
        try:
            envelope = Envelope.model_validate_json(message.body)
            workload_of(envelope.payload)
        except (ValidationError, ValueError) as exc:
            logger.warning("invalid message, rejected: %s", exc)
            await message.reject(requeue=False)
            return
        async with message.process(requeue=True):
            await process_job.kiq(envelope.model_dump(mode="json"))
            logger.info("job handed to taskiq", extra={"job_id": envelope.job_id})

    async def run(self) -> None:
        """Inicia o broker TaskIQ (lado cliente), conecta ao RabbitMQ e consome jobs.taskiq."""
        await broker.startup()
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
    handler.setFormatter(JsonFormatter("worker-taskiq-bridge"))
    logging.basicConfig(level=logging.INFO, handlers=[handler])
    asyncio.run(Bridge().run())


if __name__ == "__main__":
    main()
