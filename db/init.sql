-- Schema compartilhado. Carregado pelo Postgres no primeiro boot
-- (/docker-entrypoint-initdb.d). Sem migrations: demo.

-- Um job recebido por um gateway. status: PENDING -> DISPATCHED -> DONE | FAILED.
CREATE TABLE jobs (
    id         uuid        PRIMARY KEY,
    type       text        NOT NULL,
    payload    jsonb       NOT NULL,
    status     text        NOT NULL DEFAULT 'PENDING',
    origin     text        NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);

-- Outbox transacional: gravada na mesma transação que o job.
-- status: pending -> sent | failed. Só o relay a consome.
CREATE TABLE outbox (
    id         bigserial   PRIMARY KEY,
    job_id     uuid        NOT NULL REFERENCES jobs (id),
    envelope   jsonb       NOT NULL,
    status     text        NOT NULL DEFAULT 'pending',
    created_at timestamptz NOT NULL DEFAULT now()
);

-- Índice parcial: o relay só olha linhas pendentes, em ordem de id.
CREATE INDEX outbox_pending_idx ON outbox (id) WHERE status = 'pending';

-- Resultado por worker. A PK composta torna a gravação idempotente
-- (entrega at-least-once pode repetir o mesmo job_id).
CREATE TABLE job_results (
    job_id      uuid        NOT NULL REFERENCES jobs (id),
    worker      text        NOT NULL,
    result      jsonb       NOT NULL,
    finished_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (job_id, worker)
);
