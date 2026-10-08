// Package main é o worker-go: consome a fila jobs.go e processa cada mensagem
// numa goroutine. Mostra o modelo de concorrência nativo do Go (compare com o
// asyncio e com os bridges Celery/TaskIQ, em Python).
//
// handler.go contém o envelope do contrato e a lógica (simulada) do job.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"time"
)

// ErrUnsupportedType indica um type que este worker não sabe processar.
var ErrUnsupportedType = errors.New("unsupported job type")

// Envelope é a mensagem do contrato (contracts/envelope.schema.json).
type Envelope struct {
	JobID     string          `json:"job_id"`
	Type      string          `json:"type"`
	Payload   json.RawMessage `json:"payload"`
	CreatedAt time.Time       `json:"created_at"`
	Origin    string          `json:"origin"`
}

// Result é o resultado gravado em job_results.
type Result struct {
	Worker string `json:"worker"`
	Detail string `json:"detail"`
}

// Process executa o job. Só simula trabalho com uma espera curta, respeitando o
// cancelamento do contexto (shutdown não fica preso esperando o sleep).
func Process(ctx context.Context, env Envelope) (Result, error) {
	if env.Type != "image.resize" {
		return Result{}, ErrUnsupportedType
	}
	select {
	case <-time.After(200 * time.Millisecond):
		return Result{Worker: "go", Detail: "image resized"}, nil
	case <-ctx.Done():
		return Result{}, ctx.Err()
	}
}
