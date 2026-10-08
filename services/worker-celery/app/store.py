"""Gravação idempotente do resultado do worker no Postgres (versão síncrona).

Síncrona porque o Celery executa as tasks em processos filhos sem event loop.
Idempotência: PK (job_id, worker) + ON CONFLICT DO NOTHING absorvem a reentrega.
"""

from uuid import UUID

import psycopg
from psycopg.types.json import Jsonb

from app.models import Result


class ResultStore:
    """Grava resultados e conclui o job."""

    def __init__(self, dsn: str) -> None:
        """Guarda o DSN; abre uma conexão curta por gravação."""
        self._dsn = dsn

    def save(self, job_id: UUID, worker: str, result: Result) -> None:
        """Insere o resultado e marca o job DONE numa transação.

        O `with` da conexão faz commit ao sair sem erro e rollback com erro.
        """
        with psycopg.connect(self._dsn) as conn:
            conn.execute(
                "INSERT INTO job_results (job_id, worker, result) VALUES (%s, %s, %s)"
                " ON CONFLICT DO NOTHING",
                (job_id, worker, Jsonb(result.model_dump())),
            )
            conn.execute("UPDATE jobs SET status = 'DONE' WHERE id = %s", (job_id,))
