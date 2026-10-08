package main

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"
)

// Integração com RabbitMQ real (TEST_AMQP_URL). Publicar com routing key sem
// binding deve falhar com ErrUnroutable: antes o broker confirmava e descartava.
func TestPublishUnroutableReturnsError(t *testing.T) {
	url := os.Getenv("TEST_AMQP_URL")
	if url == "" {
		t.Skip("TEST_AMQP_URL não definido")
	}
	pub, err := NewAMQPPublisher(url)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = pub.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = pub.Publish(ctx, Worker("sem-binding"), []byte(`{}`))
	if !errors.Is(err, ErrUnroutable) {
		t.Fatalf("err = %v, want ErrUnroutable", err)
	}
	if err := pub.Publish(ctx, Worker("go"), []byte(`{"job_id":"t"}`)); err != nil {
		t.Fatalf("rota válida falhou depois de um return: %v", err)
	}
}
