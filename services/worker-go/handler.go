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

// workloadOf lê o campo "workload" do payload. Sem o campo vale io-wait; payload
// que não é um objeto JSON também cai no padrão (o contrato deixa o payload livre).
func workloadOf(payload json.RawMessage) string {
	var p struct {
		Workload string `json:"workload"`
	}
	if err := json.Unmarshal(payload, &p); err != nil || p.Workload == "" {
		return workloadIOWait
	}
	return p.Workload
}

// Process executa o job conforme o workload do payload (ver workload.go).
// Workload desconhecido vira ErrUnsupportedType: repetir não adianta.
func Process(ctx context.Context, env Envelope) (Result, error) {
	if env.Type != "image.resize" {
		return Result{}, ErrUnsupportedType
	}
	if err := RunWorkload(ctx, workloadOf(env.Payload)); err != nil {
		return Result{}, err
	}
	return Result{Worker: "go", Detail: "image resized"}, nil
}
