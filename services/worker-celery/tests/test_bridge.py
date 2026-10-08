"""Testes do Bridge Celery: mensagem inválida não chega ao framework; válida chega e é confirmada.

Sem RabbitMQ: a mensagem e a task do Celery são fakes.
"""

import asyncio
from typing import Any, cast

import pytest
from app import bridge
from app.bridge import Bridge

BODY = (
    b'{"job_id":"11111111-1111-1111-1111-111111111111","type":"report.generate","payload":{},'
    b'"created_at":"2026-10-08T00:00:00Z","origin":"gateway-py"}'
)


class FakeMessage:
    """Mensagem entregue; registra como foi encerrada."""

    def __init__(self, body: bytes) -> None:
        """Guarda o corpo; `outcome` fica vazio até o ack ou o reject."""
        self.body = body
        self.outcome = ""

    async def reject(self, requeue: bool = False) -> None:
        """Registra o reject."""
        self.outcome = "reject"

    def process(self, requeue: bool = False) -> "FakeMessage":
        """Imita `message.process()`: ack ao sair do bloco sem erro."""
        return self

    async def __aenter__(self) -> "FakeMessage":
        """Entra no bloco."""
        return self

    async def __aexit__(self, exc_type: object, *_: object) -> None:
        """Dá ack se o bloco terminou sem exceção."""
        if exc_type is None:
            self.outcome = "ack"


class FakeTask:
    """Task do framework que guarda o que foi enfileirado."""

    def __init__(self) -> None:
        """Começa sem nada enfileirado."""
        self.sent: list[dict[str, Any]] = []

    def delay(self, envelope: dict[str, Any]) -> None:
        """Guarda o envelope entregue ao Celery."""
        self.sent.append(envelope)


@pytest.fixture
def task(monkeypatch: pytest.MonkeyPatch) -> FakeTask:
    """Troca o `process_job` do bridge por uma task fake."""
    fake = FakeTask()
    monkeypatch.setattr(bridge, "process_job", fake)
    return fake


def deliver(body: bytes) -> FakeMessage:
    """Entrega o corpo ao bridge e devolve a mensagem para inspeção."""
    message = FakeMessage(body)
    asyncio.run(Bridge().on_message(cast(Any, message)))
    return message


def test_invalid_envelope_is_rejected_and_not_enqueued(task: FakeTask) -> None:
    """Envelope inválido não vai ao framework: o job ficaria preso se fosse confirmado."""
    message = deliver(b'{"type": 1}')
    assert message.outcome == "reject" and task.sent == []


def test_valid_message_is_enqueued_and_acked(task: FakeTask) -> None:
    """Mensagem válida chega ao Celery e só então é confirmada."""
    message = deliver(BODY)
    assert message.outcome == "ack" and len(task.sent) == 1
