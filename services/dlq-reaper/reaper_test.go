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

// fakeStore guarda os job_ids marcados e pode simular falha do banco.
type fakeStore struct {
	failed []string
	err    error
}

func (f *fakeStore) MarkFailed(_ context.Context, jobID string) error {
	f.failed = append(f.failed, jobID)
	return f.err
}

func handle(store JobFailer, body string) *fakeAck {
	ack := &fakeAck{}
	NewReaper(nil, "jobs.dlq", store).handle(context.Background(), amqp.Delivery{Acknowledger: ack, Body: []byte(body)})
	return ack
}

// Mensagem morta com job_id válido marca o job FAILED e sai da DLQ (ack).
func TestHandleMarksJobFailedAndAcks(t *testing.T) {
	store := &fakeStore{}
	ack := handle(store, `{"job_id":"01a11986-892d-7443-9123-5fa74aae1d45","type":"x"}`)
	if len(store.failed) != 1 || store.failed[0] != "01a11986-892d-7443-9123-5fa74aae1d45" || !ack.acked {
		t.Fatalf("failed = %v acked = %v", store.failed, ack.acked)
	}
}

// Corpo sem job_id utilizável não liga a nenhum job: nada é marcado e a mensagem é descartada com log.
func TestHandleUnlinkableBodyAcksWithoutTouchingDB(t *testing.T) {
	for _, body := range []string{`nao-e-json`, `{"type":"x"}`, `{"job_id":"abc"}`} {
		store := &fakeStore{}
		ack := handle(store, body)
		if len(store.failed) != 0 || !ack.acked {
			t.Fatalf("body %q: failed = %v acked = %v", body, store.failed, ack.acked)
		}
	}
}

// Erro do banco é transitório: a mensagem volta para a DLQ (nack com requeue).
func TestHandleStoreErrorRequeues(t *testing.T) {
	ack := handle(&fakeStore{err: errors.New("db down")}, `{"job_id":"01a11986-892d-7443-9123-5fa74aae1d45"}`)
	if !ack.nacked || !ack.requeue || ack.acked {
		t.Fatalf("nacked = %v requeue = %v acked = %v", ack.nacked, ack.requeue, ack.acked)
	}
}
