"""Retry com atraso: reagenda a mensagem na fila de espera (TTL) do RabbitMQ.

A fila `jobs.retry.asyncio` segura a mensagem pelo TTL e o broker a devolve ao
exchange `jobs` com a mesma routing key; o contador de tentativas viaja no header.
"""

from typing import Protocol

import aio_pika
from aio_pika.abc import AbstractExchange

MAX_ATTEMPTS = 3  # total de execuções de um job antes de ir para a DLQ
ATTEMPT_HEADER = "x-attempt"
RETRY_EXCHANGE = "jobs.retry"
ROUTING_KEY = "asyncio"


class Retrier(Protocol):
    """Reagenda uma mensagem para nova tentativa depois de um atraso."""

    async def schedule(self, body: bytes, attempt: int) -> None:
        """Publica o corpo na fila de espera com o contador de tentativas."""


class AmqpRetrier:
    """Publica em `jobs.retry`; o canal confirma a publicação antes de retornar."""

    def __init__(self, exchange: AbstractExchange) -> None:
        """Recebe o exchange de retry (canal com publisher confirms)."""
        self._exchange = exchange

    async def schedule(self, body: bytes, attempt: int) -> None:
        """Publica persistente com `x-attempt`; levanta se o broker não confirmar."""
        message = aio_pika.Message(
            body,
            content_type="application/json",
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            headers={ATTEMPT_HEADER: attempt},
        )
        await self._exchange.publish(message, routing_key=ROUTING_KEY)
