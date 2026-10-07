import os

import pytest
from app.models import CreateJobRequest
from app.repository import JobRepository
from psycopg_pool import AsyncConnectionPool

DSN = os.environ.get("TEST_DATABASE_URL")
pytestmark = pytest.mark.skipif(DSN is None, reason="TEST_DATABASE_URL não definido")


@pytest.fixture
async def pool():
    async with AsyncConnectionPool(DSN, open=False) as pool:
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
