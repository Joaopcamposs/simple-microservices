package main

import (
	"context"
	"errors"
	"fmt"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	// maxAttempts é o total de execuções de um job antes de ir para a DLQ.
	maxAttempts = 3
	// attemptHeader guarda quantas vezes a mensagem já foi reagendada.
	attemptHeader = "x-attempt"
	// retryExchange é o exchange direct que leva à fila de espera (TTL) do worker;
	// ao expirar, o broker devolve a mensagem ao exchange jobs com a mesma routing key.
	retryExchange = "jobs.retry"
	// workerKey é a routing key deste worker.
	workerKey = "go"
)

// Retrier reagenda uma mensagem para uma nova tentativa depois de um atraso.
type Retrier interface {
	Retry(ctx context.Context, body []byte, attempt int) error
}

// AMQPRetrier publica na fila de espera com publisher confirm: só devolve sucesso
// depois que o broker aceitou a mensagem, pois o consumer vai dar ack na original.
// O mutex serializa os publishes (canal AMQP não é concorrente).
type AMQPRetrier struct {
	mu sync.Mutex
	ch *amqp.Channel
}

// NewAMQPRetrier liga o modo de confirmação no canal recebido.
func NewAMQPRetrier(ch *amqp.Channel) (*AMQPRetrier, error) {
	if err := ch.Confirm(false); err != nil {
		return nil, fmt.Errorf("enable confirms: %w", err)
	}
	return &AMQPRetrier{ch: ch}, nil
}

// Retry publica o corpo em jobs.retry com o contador de tentativas no header.
func (r *AMQPRetrier) Retry(ctx context.Context, body []byte, attempt int) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	conf, err := r.ch.PublishWithDeferredConfirmWithContext(ctx, retryExchange, workerKey, false, false, amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		Headers:      amqp.Table{attemptHeader: int32(attempt)},
		Body:         body,
	})
	if err != nil {
		return fmt.Errorf("publish retry: %w", err)
	}
	acked, err := conf.WaitContext(ctx)
	if err != nil {
		return fmt.Errorf("wait confirm: %w", err)
	}
	if !acked {
		return errors.New("broker nack")
	}
	return nil
}

// attemptOf lê o contador de tentativas do header (0 na primeira entrega).
func attemptOf(d amqp.Delivery) int {
	switch v := d.Headers[attemptHeader].(type) {
	case int32:
		return int(v)
	case int64:
		return int(v)
	default:
		return 0
	}
}
