package main

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

// JobStore atualiza o status dos jobs no Postgres.
type JobStore struct {
	pool *pgxpool.Pool
}

// NewJobStore cria o store sobre um pool de conexões.
func NewJobStore(pool *pgxpool.Pool) *JobStore {
	return &JobStore{pool: pool}
}

// MarkFailed marca o job FAILED, exceto se já estiver DONE (um resultado gravado
// vale mais que uma cópia morta da mesma mensagem). Idempotente: repetir não muda nada.
func (s *JobStore) MarkFailed(ctx context.Context, jobID string) error {
	_, err := s.pool.Exec(ctx,
		`UPDATE jobs SET status = 'FAILED' WHERE id = $1 AND status <> 'DONE'`, jobID)
	return err
}
