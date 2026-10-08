// Consumidor da fila jobs.dlq: liga cada mensagem morta ao seu job e o marca FAILED.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5/pgtype"
	amqp "github.com/rabbitmq/amqp091-go"
)

// JobFailer marca um job como FAILED. Interface para testar sem banco.
type JobFailer interface {
	MarkFailed(ctx context.Context, jobID string) error
}

// Reaper lê a DLQ sequencialmente. Mensagem morta é rara, então não há paralelismo.
type Reaper struct {
	ch    *amqp.Channel
	queue string
	store JobFailer
}

// NewReaper cria o consumidor da fila indicada.
func NewReaper(ch *amqp.Channel, queue string, store JobFailer) *Reaper {
	return &Reaper{ch: ch, queue: queue, store: store}
}

// Run consome até o contexto ser cancelado; a mensagem em andamento termina com
// contexto sem cancelamento (não abortar o UPDATE no shutdown). Se a conexão cair devolve erro: o
// processo sai e o compose reinicia.
func (r *Reaper) Run(ctx context.Context) error {
	deliveries, err := r.ch.ConsumeWithContext(ctx, r.queue, "", false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("consume: %w", err)
	}
	work := context.WithoutCancel(ctx)
	for d := range deliveries {
		r.handle(work, d)
	}
	if ctx.Err() == nil {
		return errors.New("amqp deliveries closed")
	}
	return nil
}

// handle marca o job FAILED e só então dá ack (a mensagem sai da DLQ).
// Corpo sem job_id utilizável não liga a nenhum job: loga o corpo e dá ack,
// pois reentregar não ajuda. Erro do banco: nack com requeue.
func (r *Reaper) handle(ctx context.Context, d amqp.Delivery) {
	jobID, ok := extractJobID(d.Body)
	if !ok {
		slog.Warn("dead message without usable job_id, dropped", "body", string(d.Body))
		finish(d.Ack(false))
		return
	}
	if err := r.store.MarkFailed(ctx, jobID); err != nil {
		slog.Error("mark failed, requeued", "job_id", jobID, "error", err)
		finish(d.Nack(false, true))
		return
	}
	slog.Info("dead message, job marked failed", "job_id", jobID, "source_queue", sourceQueue(d))
	finish(d.Ack(false))
}

// extractJobID lê o job_id do envelope e o valida como UUID.
func extractJobID(body []byte) (string, bool) {
	var env struct {
		JobID string `json:"job_id"`
	}
	if err := json.Unmarshal(body, &env); err != nil {
		return "", false
	}
	var id pgtype.UUID
	if err := id.Scan(env.JobID); err != nil || !id.Valid {
		return "", false
	}
	return env.JobID, true
}

// sourceQueue devolve a fila de origem registrada pelo broker em x-death, para o log.
func sourceQueue(d amqp.Delivery) string {
	deaths, _ := d.Headers["x-death"].([]any)
	if len(deaths) == 0 {
		return ""
	}
	first, _ := deaths[0].(amqp.Table)
	queue, _ := first["queue"].(string)
	return queue
}

// finish registra falha ao confirmar/rejeitar a mensagem (ex.: canal fechado).
func finish(err error) {
	if err != nil {
		slog.Error("ack/nack", "error", err)
	}
}
