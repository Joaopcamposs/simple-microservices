package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ErrNotFound indica job inexistente.
var ErrNotFound = errors.New("job not found")

// JobRepository persiste jobs e a outbox.
type JobRepository struct {
	pool *pgxpool.Pool
}

// NewJobRepository cria o repositório sobre um pool de conexões.
func NewJobRepository(pool *pgxpool.Pool) *JobRepository {
	return &JobRepository{pool: pool}
}

// Create grava job e outbox na MESMA transação (outbox transacional) e devolve o id.
// O gateway nunca publica no broker. O id é UUIDv7 (ordenável por tempo).
func (r *JobRepository) Create(ctx context.Context, req CreateJobRequest, origin string) (string, error) {
	payload := req.Payload
	if payload == nil {
		payload = map[string]any{}
	}
	now := time.Now().UTC()
	uid, err := uuid.NewV7()
	if err != nil {
		return "", err
	}
	id := uid.String()
	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}
	envelopeJSON, err := json.Marshal(Envelope{JobID: id, Type: req.Type, Payload: payload, CreatedAt: now, Origin: origin})
	if err != nil {
		return "", err
	}
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return "", err
	}
	defer func() {
		if err := tx.Rollback(ctx); err != nil && !errors.Is(err, pgx.ErrTxClosed) {
			slog.Error("rollback", "error", err)
		}
	}()
	if _, err := tx.Exec(ctx,
		`INSERT INTO jobs (id, type, payload, origin, created_at) VALUES ($1::uuid, $2, $3, $4, $5)`,
		id, req.Type, payloadJSON, origin, now); err != nil {
		return "", err
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO outbox (job_id, envelope) VALUES ($1::uuid, $2)`, id, envelopeJSON); err != nil {
		return "", err
	}
	return id, tx.Commit(ctx)
}

// Get devolve o job e os resultados dos workers, ou ErrNotFound.
func (r *JobRepository) Get(ctx context.Context, id string) (JobView, error) {
	view := JobView{Results: []JobResult{}}
	err := r.pool.QueryRow(ctx,
		`SELECT id::text, type, status, origin, created_at FROM jobs WHERE id = $1::uuid`, id).
		Scan(&view.ID, &view.Type, &view.Status, &view.Origin, &view.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return JobView{}, ErrNotFound
	}
	if err != nil {
		return JobView{}, err
	}
	rows, err := r.pool.Query(ctx,
		`SELECT worker, result, finished_at FROM job_results WHERE job_id = $1::uuid ORDER BY worker`, id)
	if err != nil {
		return JobView{}, err
	}
	defer rows.Close()
	for rows.Next() {
		var res JobResult
		if err := rows.Scan(&res.Worker, &res.Result, &res.FinishedAt); err != nil {
			return JobView{}, err
		}
		view.Results = append(view.Results, res)
	}
	return view, rows.Err()
}
