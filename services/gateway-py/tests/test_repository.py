import os

import psycopg
import pytest
from app.models import CreateJobRequest
from app.repository import JobRepository

DSN = os.environ.get("TEST_DATABASE_URL")
pytestmark = pytest.mark.skipif(DSN is None, reason="TEST_DATABASE_URL não definido")


@pytest.fixture
async def pool():
    async with JobRepository.create_pool(DSN) as pool:
        await pool.open()
        # TRUNCATE: só usar no Postgres de dev.
        async with pool.connection() as conn:
            await conn.execute("TRUNCATE job_results, outbox, jobs CASCADE")
        yield pool


async def test_create_writes_job_and_outbox_together(pool) -> None:
    repo = JobRepository(pool)
    job_id = await repo.create(CreateJobRequest(type="email.send"), "gateway-py")
    async with pool.connection() as conn:
        cur = await conn.execute(
            "SELECT status, envelope->>'type', envelope->'payload' FROM outbox WHERE job_id = %s",
            (job_id,),
        )
        row = await cur.fetchone()
    assert job_id.version == 7
    assert row == ("pending", "email.send", {})
    view = await repo.get(job_id)
    assert view is not None and view.origin == "gateway-py" and view.status == "PENDING"


async def test_create_survives_server_closing_pooled_connections(pool) -> None:
    repo = JobRepository(pool)
    await repo.create(CreateJobRequest(type="email.send"), "gateway-py")
    # Simula restart do Postgres: o servidor derruba as conexões que o pool guardou.
    async with await psycopg.AsyncConnection.connect(DSN, autocommit=True) as admin:
        await admin.execute(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity"
            " WHERE datname = current_database() AND pid <> pg_backend_pid()"
        )
    job_id = await repo.create(CreateJobRequest(type="email.send"), "gateway-py")
    assert await repo.get(job_id) is not None
