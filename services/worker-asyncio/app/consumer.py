"""Consumer aio-pika: lê jobs.asyncio e processa cada mensagem como task asyncio.

Concorrência: o aio-pika chama o callback em uma task por mensagem, todas no
mesmo event loop (uma thread). O prefetch limita quantas ficam em voo.
"""

import asyncio
import logging

from aio_pika.abc import AbstractChannel, AbstractIncomingMessage
from pydantic import ValidationError

from app.models import Envelope, Result
from app.store import ResultStore

logger = logging.getLogger(__name__)

QUEUE = "jobs.asyncio"
JOB_TYPE = "http.fetch"
PREFETCH = 10  # limita mensagens em voo e, logo, a concorrência


class JobConsumer:
    """Consome a fila e grava o resultado antes de confirmar a mensagem."""

    def __init__(self, channel: AbstractChannel, store: ResultStore) -> None:
        """Recebe o canal AMQP e o store de resultados."""
        self._channel = channel
        self._store = store

    async def run(self) -> None:
        """Define o QoS e consome indefinidamente (callbacks rodam concorrentes)."""
        await self._channel.set_qos(prefetch_count=PREFETCH)
        queue = await self._channel.get_queue(QUEUE)
        await queue.consume(self._on_message)
        logger.info("consuming %s", QUEUE)
        await asyncio.Event().wait()

    def _parse(self, body: bytes) -> Envelope:
        """Valida o corpo; levanta ValueError (ou ValidationError) se inválido/outro type."""
        envelope = Envelope.model_validate_json(body)
        if envelope.type != JOB_TYPE:
            raise ValueError(f"unsupported job type: {envelope.type}")
        return envelope

    async def _process(self, envelope: Envelope) -> Result:
        """Executa o job `http.fetch` (simulado com espera curta, sem bloquear o loop)."""
        await asyncio.sleep(0.2)
        return Result(worker="asyncio", detail="url fetched")

    async def _on_message(self, message: AbstractIncomingMessage) -> None:
        """Valida, processa e grava.

        Inválida ou type não suportado: reject sem requeue (repetir não adianta).
        Erro transitório (ex.: banco): `message.process(requeue=True)` devolve a
        mensagem à fila; o ack só acontece se o bloco terminar sem erro, isto é,
        depois de gravar o resultado.
        """
        try:
            envelope = self._parse(message.body)
        except (ValidationError, ValueError) as exc:
            logger.warning("invalid or unsupported message, rejected: %s", exc)
            await message.reject(requeue=False)
            return
        extra = {"job_id": envelope.job_id}
        async with message.process(requeue=True):
            result = await self._process(envelope)
            await self._store.save(envelope.job_id, result.worker, result)
            logger.info("job done", extra=extra)
