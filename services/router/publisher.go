package main

import (
	"context"
	"errors"
	"fmt"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
)

// exchangeName é o exchange direct declarado em infra/rabbitmq/definitions.json.
// O router só publica nele; a topologia (filas e bindings) já existe no broker.
const exchangeName = "jobs"

// AMQPPublisher publica no RabbitMQ com publisher confirms: só devolve sucesso
// depois que o broker confirmou ter recebido a mensagem. Sem isso, um 202 do
// router poderia mentir e o job se perderia.
//
// O canal AMQP não é seguro para uso concorrente e cada request HTTP roda em
// sua própria goroutine, então um mutex serializa os publishes.
type AMQPPublisher struct {
	mu   sync.Mutex
	conn *amqp.Connection
	ch   *amqp.Channel
}

// NewAMQPPublisher conecta ao broker e liga o modo de confirmação.
// Não reconecta sozinho: se a conexão cair, o processo falha e o compose
// reinicia (restart: unless-stopped). Simples, e suficiente para a demo.
func NewAMQPPublisher(url string) (*AMQPPublisher, error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, fmt.Errorf("dial amqp: %w", err)
	}
	ch, err := conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("open channel: %w", err)
	}
	if err := ch.Confirm(false); err != nil {
		return nil, fmt.Errorf("enable confirms: %w", err)
	}
	return &AMQPPublisher{conn: conn, ch: ch}, nil
}

// Publish envia o corpo ao exchange com routing key = worker e espera o confirm.
// A mensagem é persistente (sobrevive a restart do broker, pois as filas são
// duráveis). mandatory=false: a topologia é fixa, então não tratamos devoluções.
func (p *AMQPPublisher) Publish(ctx context.Context, worker Worker, body []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	conf, err := p.ch.PublishWithDeferredConfirmWithContext(ctx, exchangeName, string(worker), false, false, amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		Body:         body,
	})
	if err != nil {
		return fmt.Errorf("publish: %w", err)
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

// Close encerra a conexão (o canal fecha junto).
func (p *AMQPPublisher) Close() error {
	return p.conn.Close()
}
