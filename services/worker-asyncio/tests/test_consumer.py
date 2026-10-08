"""Testes do ciclo de retry do JobConsumer, sem RabbitMQ nem Postgres (fakes)."""

from typing import Any, cast

from app.consumer import JobConsumer
from app.models import Result
from app.retry import ATTEMPT_HEADER, MAX_ATTEMPTS

VALID = (
    b'{"job_id":"11111111-1111-1111-1111-111111111111","type":"http.fetch","payload":{},'
    b'"created_at":"2026-10-08T00:00:00Z","origin":"gateway-py"}'
)


class FakeMessage:
    """Mensagem entregue; registra como foi encerrada."""

    def __init__(self, attempt: int = 0) -> None:
        """Cria a mensagem com o contador de tentativas no header."""
        self.body = VALID
        self.headers: dict[str, int] = {ATTEMPT_HEADER: attempt} if attempt else {}
        self.outcome = ""

    async def ack(self) -> None:
        """Registra o ack."""
        self.outcome = "ack"

    async def nack(self, requeue: bool = False) -> None:
        """Registra o nack e se pediu requeue."""
        self.outcome = "nack-requeue" if requeue else "nack"

    async def reject(self, requeue: bool = False) -> None:
        """Registra o reject."""
        self.outcome = "reject"


class FakeStore:
    """Store que falha ou não, simulando o banco."""

    def __init__(self, fail: bool) -> None:
        """Define se o save levanta erro."""
        self.fail = fail

    async def save(self, job_id: object, worker: str, result: Result) -> None:
        """Levanta OSError se configurado para falhar."""
        if self.fail:
            raise OSError("db down")


class FakeRetrier:
    """Registra reagendamentos; pode falhar para simular o broker."""

    def __init__(self, fail: bool = False) -> None:
        """Define se o agendamento levanta erro."""
        self.fail = fail
        self.attempts: list[int] = []

    async def schedule(self, body: bytes, attempt: int) -> None:
        """Guarda o attempt pedido ou levanta OSError."""
        if self.fail:
            raise OSError("publish")
        self.attempts.append(attempt)


async def deliver(message: FakeMessage, fail: bool, retrier: FakeRetrier) -> None:
    """Entrega a mensagem a um consumer montado com os fakes."""
    consumer = JobConsumer(cast(Any, None), cast(Any, FakeStore(fail)), retrier)
    await consumer._on_message(cast(Any, message))


async def test_transient_failure_schedules_retry() -> None:
    """Falha transitória reagenda com attempt+1 e confirma a original."""
    message, retrier = FakeMessage(), FakeRetrier()
    await deliver(message, True, retrier)
    assert retrier.attempts == [1] and message.outcome == "ack"


async def test_exhausted_retries_go_to_dead_letter() -> None:
    """Esgotadas as tentativas, nack sem requeue leva à DLQ."""
    message, retrier = FakeMessage(MAX_ATTEMPTS - 1), FakeRetrier()
    await deliver(message, True, retrier)
    assert retrier.attempts == [] and message.outcome == "nack"


async def test_retry_publish_failure_requeues() -> None:
    """Sem conseguir reagendar, devolve à fila: melhor repetir que perder."""
    message = FakeMessage()
    await deliver(message, True, FakeRetrier(fail=True))
    assert message.outcome == "nack-requeue"


async def test_success_acks_without_retry() -> None:
    """Sucesso confirma sem reagendar."""
    message, retrier = FakeMessage(), FakeRetrier()
    await deliver(message, False, retrier)
    assert retrier.attempts == [] and message.outcome == "ack"
