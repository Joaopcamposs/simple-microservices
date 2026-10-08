package main

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// claimQuery pega o lote de pendentes em ordem. FOR UPDATE SKIP LOCKED trava as
// linhas e faz outro relay pular as já travadas, então várias instâncias não
// entregam a mesma linha em dobro.
const claimQuery = `
SELECT id, job_id::text, envelope
FROM outbox
WHERE status = 'pending'
ORDER BY id
LIMIT $1
FOR UPDATE SKIP LOCKED`

// Sender entrega um envelope ao router. Interface para o teste do relay não
// precisar de HTTP.
type Sender interface {
	Send(ctx context.Context, envelope []byte) Outcome
}

// entry é uma linha pendente da outbox.
type entry struct {
	ID       int64
	JobID    string
	Envelope []byte
}

// Relay move linhas da outbox para o router e registra o resultado no banco.
type Relay struct {
	pool     *pgxpool.Pool
	sender   Sender
	batch    int
	interval time.Duration
}

// NewRelay cria o relay com tamanho de lote e intervalo de polling.
func NewRelay(pool *pgxpool.Pool, sender Sender, batch int, interval time.Duration) *Relay {
	return &Relay{pool: pool, sender: sender, batch: batch, interval: interval}
}

// Run executa RunOnce a cada intervalo até o contexto ser cancelado.
// Erro de um ciclo é logado e o loop segue: Postgres ou router podem voltar.
func (r *Relay) Run(ctx context.Context) {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	for {
		if err := r.RunOnce(ctx); err != nil {
			slog.Error("relay cycle", "error", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// RunOnce processa um lote numa transação. A transação fica aberta durante o
// POST (aceitável na demo): se o relay morrer, o lock cai e a linha volta a
// ficar pendente, garantindo at-least-once. Em Retry o lote para, preservando a
// ordem; o que já foi marcado antes é confirmado.
func (r *Relay) RunOnce(ctx context.Context) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() {
		if err := tx.Rollback(ctx); err != nil && !errors.Is(err, pgx.ErrTxClosed) {
			slog.Error("rollback", "error", err)
		}
	}()
	entries, err := r.claim(ctx, tx)
	if err != nil {
		return err
	}
	for _, e := range entries {
		switch r.sender.Send(ctx, e.Envelope) {
		case Delivered:
			slog.Info("job delivered", "job_id", e.JobID)
			err = r.mark(ctx, tx, e, "sent", "DISPATCHED")
		case Rejected:
			slog.Warn("job rejected, marked failed", "job_id", e.JobID)
			err = r.mark(ctx, tx, e, "failed", "FAILED")
		default:
			slog.Warn("job delivery failed, will retry", "job_id", e.JobID)
			return tx.Commit(ctx)
		}
		if err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

// claim lê e trava as linhas pendentes do lote dentro da transação.
func (r *Relay) claim(ctx context.Context, tx pgx.Tx) ([]entry, error) {
	rows, err := tx.Query(ctx, claimQuery, r.batch)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var entries []entry
	for rows.Next() {
		var e entry
		if err := rows.Scan(&e.ID, &e.JobID, &e.Envelope); err != nil {
			return nil, err
		}
		entries = append(entries, e)
	}
	return entries, rows.Err()
}

// mark atualiza a outbox e o job. O job só muda se ainda estiver PENDING:
// um worker rápido pode já ter marcado DONE e isso não pode regredir.
func (r *Relay) mark(ctx context.Context, tx pgx.Tx, e entry, outboxStatus, jobStatus string) error {
	if _, err := tx.Exec(ctx, `UPDATE outbox SET status = $1 WHERE id = $2`, outboxStatus, e.ID); err != nil {
		return err
	}
	_, err := tx.Exec(ctx, `UPDATE jobs SET status = $1 WHERE id = $2::uuid AND status = 'PENDING'`, jobStatus, e.JobID)
	return err
}
