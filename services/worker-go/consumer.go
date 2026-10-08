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

// ResultSaver grava o resultado do job. Interface para testar sem banco.
type ResultSaver interface {
	Save(ctx context.Context, jobID, worker string, result Result) error
}

// Consumer lê a fila e processa cada mensagem numa goroutine.
type Consumer struct {
	ch       *amqp.Channel
	queue    string
	prefetch int
	store    ResultSaver
	retrier  Retrier
}

// NewConsumer cria o consumer. prefetch limita as mensagens em voo e, portanto,
// o número de goroutines simultâneas: o broker não entrega mais que isso sem ack.
func NewConsumer(ch *amqp.Channel, queue string, prefetch int, store ResultSaver, retrier Retrier) *Consumer {
	return &Consumer{ch: ch, queue: queue, prefetch: prefetch, store: store, retrier: retrier}
}

// Run consome até o contexto ser cancelado e espera as goroutines em voo.
// As mensagens em voo terminam com um contexto sem cancelamento: abortar o Save
// no shutdown viraria retry à toa. O limite é o prazo do orquestrador (SIGKILL).
func (c *Consumer) Run(ctx context.Context) error {
	if err := c.ch.Qos(c.prefetch, 0, false); err != nil {
		return fmt.Errorf("qos: %w", err)
	}
	deliveries, err := c.ch.ConsumeWithContext(ctx, c.queue, "", false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("consume: %w", err)
	}
	work := context.WithoutCancel(ctx)
	var wg sync.WaitGroup
	for d := range deliveries {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c.handle(work, d)
		}()
	}
	wg.Wait()
	// O canal de entregas fecha em shutdown (ctx cancelado) ou quando a conexão
	// cai. No segundo caso devolvemos erro: o processo sai e o compose reinicia.
	if ctx.Err() == nil {
		return errors.New("amqp deliveries closed")
	}
	return nil
}

// handle processa uma mensagem. O ack só acontece depois de gravar o resultado.
// JSON inválido ou type não suportado: nack sem requeue (repetir não adianta).
// Erro transitório (banco): reagenda na fila de espera (retry com atraso) e dá
// ack na original; esgotadas as tentativas, nack sem requeue leva à DLQ.
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
		c.retryOrGiveUp(ctx, d, env, err)
		return
	}
	slog.Info("job done", "job_id", env.JobID, "type", env.Type, "worker", result.Worker)
	c.finish(d.Ack(false))
}

// retryOrGiveUp trata a falha transitória: reagenda com attempt+1 e confirma a
// original, ou manda à DLQ se as tentativas acabaram. Se nem o reagendamento
// funciona (broker), devolve a mensagem à fila: melhor repetir que perder.
func (c *Consumer) retryOrGiveUp(ctx context.Context, d amqp.Delivery, env Envelope, cause error) {
	next := attemptOf(d) + 1
	if next >= maxAttempts {
		slog.Error("job failed, retries exhausted", "job_id", env.JobID, "type", env.Type, "attempts", next, "error", cause)
		c.finish(d.Nack(false, false))
		return
	}
	if err := c.retrier.Retry(ctx, d.Body, next); err != nil {
		slog.Error("job failed, retry not scheduled, requeued", "job_id", env.JobID, "type", env.Type, "error", err)
		c.finish(d.Nack(false, true))
		return
	}
	slog.Warn("job failed, retry scheduled", "job_id", env.JobID, "type", env.Type, "attempt", next, "error", cause)
	c.finish(d.Ack(false))
}

// finish registra falha ao confirmar/rejeitar a mensagem (ex.: canal fechado).
func (c *Consumer) finish(err error) {
	if err != nil {
		slog.Error("ack/nack", "error", err)
	}
}
