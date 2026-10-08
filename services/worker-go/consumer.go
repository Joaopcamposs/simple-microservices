package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Consumer lê a fila e processa cada mensagem numa goroutine.
type Consumer struct {
	ch       *amqp.Channel
	queue    string
	prefetch int
	store    *ResultStore
}

// NewConsumer cria o consumer. prefetch limita as mensagens em voo e, portanto,
// o número de goroutines simultâneas: o broker não entrega mais que isso sem ack.
func NewConsumer(ch *amqp.Channel, queue string, prefetch int, store *ResultStore) *Consumer {
	return &Consumer{ch: ch, queue: queue, prefetch: prefetch, store: store}
}

// Run consome até o contexto ser cancelado e espera as goroutines em voo.
func (c *Consumer) Run(ctx context.Context) error {
	if err := c.ch.Qos(c.prefetch, 0, false); err != nil {
		return fmt.Errorf("qos: %w", err)
	}
	deliveries, err := c.ch.ConsumeWithContext(ctx, c.queue, "", false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("consume: %w", err)
	}
	var wg sync.WaitGroup
	for d := range deliveries {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c.handle(ctx, d)
		}()
	}
	wg.Wait()
	return nil
}

// handle processa uma mensagem. O ack só acontece depois de gravar o resultado.
// JSON inválido ou type não suportado: nack sem requeue (repetir não adianta).
// Erro transitório (banco): nack com requeue, a mensagem volta para a fila.
func (c *Consumer) handle(ctx context.Context, d amqp.Delivery) {
	var env Envelope
	if err := json.Unmarshal(d.Body, &env); err != nil {
		slog.Warn("invalid message, rejected", "error", err)
		c.finish(d.Nack(false, false))
		return
	}
	result, err := Process(ctx, env)
	if errors.Is(err, ErrUnsupportedType) {
		slog.Warn("unsupported job type, rejected", "job_id", env.JobID, "type", env.Type)
		c.finish(d.Nack(false, false))
		return
	}
	if err == nil {
		err = c.store.Save(ctx, env.JobID, result.Worker, result)
	}
	if err != nil {
		slog.Error("job failed, requeued", "job_id", env.JobID, "type", env.Type, "error", err)
		c.finish(d.Nack(false, true))
		return
	}
	slog.Info("job done", "job_id", env.JobID, "type", env.Type, "worker", result.Worker)
	c.finish(d.Ack(false))
}

// finish registra falha ao confirmar/rejeitar a mensagem (ex.: canal fechado).
func (c *Consumer) finish(err error) {
	if err != nil {
		slog.Error("ack/nack", "error", err)
	}
}
