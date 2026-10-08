"""Entrada do worker-asyncio: conecta ao RabbitMQ e inicia o consumer."""

import asyncio
import logging
import os

import aio_pika

from app.consumer import JobConsumer
from app.logs import JsonFormatter
from app.retry import RETRY_EXCHANGE, AmqpRetrier
from app.store import ResultStore


async def main() -> None:
    """Configura o log, conecta (com reconexão automática) e consome até o processo terminar."""
    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter("worker-asyncio"))
    logging.basicConfig(level=logging.INFO, handlers=[handler])
    connection = await aio_pika.connect_robust(
        os.environ.get("AMQP_URL", "amqp://guest:guest@localhost:55672/")
    )
    async with connection:
        channel = await connection.channel()
        store = ResultStore(
            os.environ.get("DATABASE_URL", "postgres://app:app@localhost:55432/app")
        )
        exchange = await channel.get_exchange(RETRY_EXCHANGE)
        await JobConsumer(channel, store, AmqpRetrier(exchange)).run()


if __name__ == "__main__":
    asyncio.run(main())
