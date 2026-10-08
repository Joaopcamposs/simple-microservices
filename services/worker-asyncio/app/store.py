"""Gravação idempotente do resultado do worker no Postgres.

Idempotência: a entrega é at-least-once, então o mesmo job_id pode chegar duas
vezes. A PK (job_id, worker) + ON CONFLICT DO NOTHING faz a segunda gravação ser
um no-op, em vez de erro ou duplicata.
"""

from uuid import UUID

import psycopg
from psycopg.types.json import Jsonb

from app.models import Result


class ResultStore:
    """Grava resultados e conclui o job."""

    def __init__(self, dsn: str) -> None:
        """Guarda o DSN; abre uma conexão curta por gravação (suficiente na demo)."""
        self._dsn = dsn

    async def save(self, job_id: UUID, worker: str, result: Result) -> None:
        """Insere o resultado e marca o job DONE numa transação.

        O `async with` da conexão faz commit ao sair sem erro e rollback com erro.
        """
        async with await psycopg.AsyncConnection.connect(self._dsn) as conn:
            await conn.execute(
                "INSERT INTO job_results (job_id, worker, result) VALUES (%s, %s, %s)"
                " ON CONFLICT DO NOTHING",
                (job_id, worker, Jsonb(result.model_dump())),
            )
            await conn.execute("UPDATE jobs SET status = 'DONE' WHERE id = %s", (job_id,))
