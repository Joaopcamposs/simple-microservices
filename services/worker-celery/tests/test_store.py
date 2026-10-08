"""Teste de integração do ResultStore síncrono: a gravação precisa ser idempotente."""

import os
from uuid import UUID

import psycopg
import pytest
from app.models import Result
from app.store import ResultStore

DSN = os.environ.get("TEST_DATABASE_URL")
JOB_ID = UUID("66666666-6666-6666-6666-666666666666")


@pytest.mark.skipif(DSN is None, reason="TEST_DATABASE_URL não definido")
def test_save_is_idempotent() -> None:
    """Reentrega da mesma mensagem deixa uma linha em job_results e o job DONE."""
    assert DSN is not None
    with psycopg.connect(DSN, autocommit=True) as conn:
        # TRUNCATE: só usar no Postgres de dev.
        conn.execute("TRUNCATE job_results, outbox, jobs CASCADE")
        conn.execute(
            "INSERT INTO jobs (id, type, payload, origin)"
            " VALUES (%s, 'report.generate', '{}', 'gateway-py')",
            (JOB_ID,),
        )
        store = ResultStore(DSN)
        for _ in range(2):
            store.save(JOB_ID, "celery", Result(worker="celery", detail="ok"))
        rows = conn.execute("SELECT count(*) FROM job_results WHERE job_id = %s", (JOB_ID,))
        assert rows.fetchone() == (1,)
        status = conn.execute("SELECT status FROM jobs WHERE id = %s", (JOB_ID,))
        assert status.fetchone() == ("DONE",)
