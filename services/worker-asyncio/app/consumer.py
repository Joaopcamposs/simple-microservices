"""Consumer aio-pika: lê jobs.asyncio e processa cada mensagem como task asyncio.

Concorrência: o aio-pika chama o callback em uma task por mensagem, todas no
mesmo event loop (uma thread). O prefetch limita quantas ficam em voo.
"""

import asyncio
import logging
import os
from uuid import UUID

from aio_pika.abc import AbstractChannel, AbstractIncomingMessage
from pydantic import ValidationError

from app.models import Envelope, Result
from app.retry import ATTEMPT_HEADER, MAX_ATTEMPTS, Retrier
from app.store import ResultStore
from app.workload import run_workload, workload_of

logger = logging.getLogger(__name__)

QUEUE = "jobs.asyncio"
JOB_TYPE = "http.fetch"
PREFETCH = int(os.environ.get("CONCURRENCY", "10"))  # mensagens em voo = concorrência


class JobConsumer:
    """Consome a fila e grava o resultado antes de confirmar a mensagem."""

    def __init__(self, channel: AbstractChannel, store: ResultStore, retrier: Retrier) -> None:
        """Recebe o canal AMQP, o store de resultados e o agendador de retry."""
        self._channel = channel
        self._store = store
        self._retrier = retrier

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
        workload_of(envelope.payload)
        return envelope

    async def _process(self, envelope: Envelope) -> Result:
        """Executa o job `http.fetch` conforme o `workload` do payload (ver workload.py)."""
        await run_workload(workload_of(envelope.payload))
        return Result(worker="asyncio", detail="url fetched")

    async def _on_message(self, message: AbstractIncomingMessage) -> None:
        """Valida, processa e grava.

        Inválida ou type não suportado: reject sem requeue (repetir não adianta).
        Erro transitório (ex.: banco): reagenda na fila de espera e confirma a
        original; esgotadas as tentativas, nack sem requeue leva à DLQ. O ack só
        acontece depois de gravar o resultado (ou de reagendar).
        """
        try:
            envelope = self._parse(message.body)
        except (ValidationError, ValueError) as exc:
            logger.warning("invalid or unsupported message, rejected: %s", exc)
            await message.reject(requeue=False)
            return
        extra = {"job_id": envelope.job_id}
        try:
            result = await self._process(envelope)
            await self._store.save(envelope.job_id, result.worker, result)
        except Exception as exc:
            await self._retry_or_give_up(message, exc, extra)
            return
        await message.ack()
        logger.info("job done", extra=extra)

    async def _retry_or_give_up(
        self, message: AbstractIncomingMessage, cause: Exception, extra: dict[str, UUID]
    ) -> None:
        """Reagenda com attempt+1 ou manda à DLQ; sem broker para reagendar, devolve à fila."""
        next_attempt = int(str(message.headers.get(ATTEMPT_HEADER, 0))) + 1
        if next_attempt >= MAX_ATTEMPTS:
            logger.error("job failed, retries exhausted: %s", cause, extra=extra)
            await message.nack(requeue=False)
            return
        try:
            await self._retrier.schedule(message.body, next_attempt)
        except Exception:
            logger.exception("job failed, retry not scheduled, requeued", extra=extra)
            await message.nack(requeue=True)
            return
        logger.warning("job failed, retry %d scheduled: %s", next_attempt, cause, extra=extra)
        await message.ack()
