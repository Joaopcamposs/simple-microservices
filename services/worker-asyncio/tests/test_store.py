"""Teste de integração do ResultStore: a gravação precisa ser idempotente."""

import os
from uuid import UUID

import psycopg
import pytest
from app.models import Result
from app.store import ResultStore

DSN = os.environ.get("TEST_DATABASE_URL")
JOB_ID = UUID("55555555-5555-5555-5555-555555555555")


@pytest.mark.skipif(DSN is None, reason="TEST_DATABASE_URL não definido")
async def test_save_is_idempotent() -> None:
    """Reentrega da mesma mensagem deixa uma linha em job_results e o job DONE."""
    assert DSN is not None
    async with await psycopg.AsyncConnection.connect(DSN, autocommit=True) as conn:
        # TRUNCATE: só usar no Postgres de dev.
        await conn.execute("TRUNCATE job_results, outbox, jobs CASCADE")
        await conn.execute(
            "INSERT INTO jobs (id, type, payload, origin)"
            " VALUES (%s, 'http.fetch', '{}', 'gateway-py')",
            (JOB_ID,),
        )
        store = ResultStore(DSN)
        for _ in range(2):
            await store.save(JOB_ID, "asyncio", Result(worker="asyncio", detail="ok"))
        cur = await conn.execute("SELECT count(*) FROM job_results WHERE job_id = %s", (JOB_ID,))
        assert (await cur.fetchone()) == (1,)
        cur = await conn.execute("SELECT status FROM jobs WHERE id = %s", (JOB_ID,))
        assert (await cur.fetchone()) == ("DONE",)
