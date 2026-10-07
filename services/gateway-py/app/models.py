"""Modelos da borda HTTP do gateway-py e o envelope do contrato."""

from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, Field


class CreateJobRequest(BaseModel):
    """Corpo de POST /jobs. `type` é obrigatório; o router decide o destino."""

    type: str = Field(min_length=1, examples=["email.send"])
    payload: dict[str, object] = Field(default_factory=dict)


class CreateJobResponse(BaseModel):
    """Id do job aceito."""

    job_id: UUID


class JobResult(BaseModel):
    """Resultado gravado por um worker."""

    worker: str
    result: dict[str, object]
    finished_at: datetime


class JobView(BaseModel):
    """Job com status e resultados dos workers."""

    id: UUID
    type: str
    status: str
    origin: str
    created_at: datetime
    results: list[JobResult]


class Envelope(BaseModel):
    """Mensagem do contrato (contracts/envelope.schema.json)."""

    job_id: UUID
    type: str
    payload: dict[str, object]
    created_at: datetime
    origin: str
