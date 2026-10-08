package main

import (
	"context"
	"errors"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
)

// fakeAck registra como a mensagem foi encerrada, sem precisar de RabbitMQ.
type fakeAck struct {
	acked, nacked, requeue bool
}

func (f *fakeAck) Ack(uint64, bool) error { f.acked = true; return nil }
func (f *fakeAck) Nack(_ uint64, _ bool, requeue bool) error {
	f.nacked, f.requeue = true, requeue
	return nil
}
func (f *fakeAck) Reject(uint64, bool) error { return nil }

// fakeSaver simula o banco: err != nil é uma falha transitória.
type fakeSaver struct{ err error }

func (f fakeSaver) Save(context.Context, string, string, Result) error { return f.err }

// fakeRetrier registra o reagendamento pedido.
type fakeRetrier struct {
	calls   int
	attempt int
	err     error
}

func (f *fakeRetrier) Retry(_ context.Context, _ []byte, attempt int) error {
	f.calls++
	f.attempt = attempt
	return f.err
}

const validJob = `{"job_id":"j1","type":"image.resize"}`

func deliver(saver ResultSaver, retrier Retrier, attempt int) *fakeAck {
	ack := &fakeAck{}
	headers := amqp.Table{}
	if attempt > 0 {
		headers[attemptHeader] = int32(attempt)
	}
	d := amqp.Delivery{Acknowledger: ack, Body: []byte(validJob), Headers: headers}
	NewConsumer(nil, "jobs.go", 1, saver, retrier).handle(context.Background(), d)
	return ack
}

// Falha transitória reagenda com attempt+1 e confirma a original (não há mais requeue imediato).
func TestTransientFailureSchedulesRetry(t *testing.T) {
	retrier := &fakeRetrier{}
	ack := deliver(fakeSaver{err: errors.New("db down")}, retrier, 0)
	if retrier.calls != 1 || retrier.attempt != 1 || !ack.acked || ack.nacked {
		t.Fatalf("calls=%d attempt=%d acked=%v nacked=%v", retrier.calls, retrier.attempt, ack.acked, ack.nacked)
	}
}

// Esgotadas as tentativas, a mensagem vai à DLQ (nack sem requeue) em vez de girar para sempre.
func TestExhaustedRetriesGoToDeadLetter(t *testing.T) {
	retrier := &fakeRetrier{}
	ack := deliver(fakeSaver{err: errors.New("db down")}, retrier, maxAttempts-1)
	if retrier.calls != 0 || !ack.nacked || ack.requeue || ack.acked {
		t.Fatalf("calls=%d nacked=%v requeue=%v acked=%v", retrier.calls, ack.nacked, ack.requeue, ack.acked)
	}
}

// Se nem o reagendamento funciona (broker), devolve à fila: melhor repetir que perder.
func TestRetryPublishFailureRequeues(t *testing.T) {
	ack := deliver(fakeSaver{err: errors.New("db down")}, &fakeRetrier{err: errors.New("publish")}, 0)
	if !ack.nacked || !ack.requeue {
		t.Fatalf("nacked=%v requeue=%v", ack.nacked, ack.requeue)
	}
}

// Sucesso confirma sem tocar no retry.
func TestSuccessAcksWithoutRetry(t *testing.T) {
	retrier := &fakeRetrier{}
	ack := deliver(fakeSaver{}, retrier, 0)
	if !ack.acked || retrier.calls != 0 {
		t.Fatalf("acked=%v calls=%d", ack.acked, retrier.calls)
	}
}
