"""Modelos do worker: envelope recebido (contrato) e resultado gravado."""

from datetime import datetime
from uuid import UUID

from pydantic import BaseModel


class Envelope(BaseModel):
    """Mensagem do contrato (contracts/envelope.schema.json)."""

    job_id: UUID
    type: str
    payload: dict[str, object]
    created_at: datetime
    origin: str


class Result(BaseModel):
    """Resultado gravado em job_results; mesmo formato em todos os workers."""

    worker: str
    detail: str
