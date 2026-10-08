package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ResultStore grava o resultado do worker e conclui o job.
type ResultStore struct {
	pool *pgxpool.Pool
}

// NewResultStore cria o store sobre um pool de conexões.
func NewResultStore(pool *pgxpool.Pool) *ResultStore {
	return &ResultStore{pool: pool}
}

// Save grava resultado e marca o job DONE na mesma transação. É idempotente:
// ON CONFLICT DO NOTHING absorve a reentrega do mesmo (job_id, worker) e o
// UPDATE para DONE pode repetir sem efeito colateral.
func (s *ResultStore) Save(ctx context.Context, jobID, worker string, result Result) error {
	resultJSON, err := json.Marshal(result)
	if err != nil {
		return err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() {
		if err := tx.Rollback(ctx); err != nil && !errors.Is(err, pgx.ErrTxClosed) {
			log.Printf("rollback: %v", err)
		}
	}()
	if _, err := tx.Exec(ctx,
		`INSERT INTO job_results (job_id, worker, result) VALUES ($1::uuid, $2, $3) ON CONFLICT DO NOTHING`,
		jobID, worker, resultJSON); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `UPDATE jobs SET status = 'DONE' WHERE id = $1::uuid`, jobID); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
