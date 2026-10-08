"""Acesso ao Postgres: grava job + outbox e consulta jobs."""

from datetime import UTC, datetime
from uuid import UUID

from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from psycopg_pool import AsyncConnectionPool
from uuid_utils import uuid7

from app.models import CreateJobRequest, Envelope, JobResult, JobView


class JobRepository:
    """Persistência de jobs e outbox sobre um pool assíncrono."""

    @staticmethod
    def create_pool(dsn: str) -> AsyncConnectionPool:
        """Cria o pool (ainda fechado; use `await pool.open()`).

        `check` descarta conexões mortas, p.ex. depois de um restart do Postgres,
        em vez de entregá-las e estourar 500 no primeiro request.
        """
        return AsyncConnectionPool(dsn, open=False, check=AsyncConnectionPool.check_connection)

    def __init__(self, pool: AsyncConnectionPool) -> None:
        """Recebe o pool de conexões (criado no lifespan da aplicação)."""
        self._pool = pool

    async def create(self, request: CreateJobRequest, origin: str) -> UUID:
        """Grava job e outbox na MESMA transação (outbox transacional).

        O gateway nunca publica no broker; só o relay encaminha a outbox.
        """
        # UUIDv7: ordenável por tempo. Convertido ao UUID da stdlib, que psycopg e pydantic aceitam.
        job_id = UUID(int=uuid7().int)
        now = datetime.now(UTC)
        envelope = Envelope(
            job_id=job_id, type=request.type, payload=request.payload, created_at=now, origin=origin
        )
        async with self._pool.connection() as conn, conn.transaction():
            await conn.execute(
                "INSERT INTO jobs (id, type, payload, origin, created_at)"
                " VALUES (%s, %s, %s, %s, %s)",
                (job_id, request.type, Jsonb(request.payload), origin, now),
            )
            await conn.execute(
                "INSERT INTO outbox (job_id, envelope) VALUES (%s, %s)",
                (job_id, Jsonb(envelope.model_dump(mode="json"))),
            )
        return job_id

    async def get(self, job_id: UUID) -> JobView | None:
        """Devolve o job com os resultados dos workers, ou None se não existir."""
        async with self._pool.connection() as conn, conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                "SELECT id, type, status, origin, created_at FROM jobs WHERE id = %s", (job_id,)
            )
            row = await cur.fetchone()
            if row is None:
                return None
            await cur.execute(
                "SELECT worker, result, finished_at FROM job_results"
                " WHERE job_id = %s ORDER BY worker",
                (job_id,),
            )
            results = [JobResult(**r) for r in await cur.fetchall()]
        return JobView(**row, results=results)
