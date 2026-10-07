// Package main é o gateway-go: API HTTP (Gin) que grava job + outbox.
// models.go define os modelos da borda HTTP e o envelope do contrato.
package main

import (
	"encoding/json"
	"time"
)

// CreateJobRequest é o corpo de POST /jobs.
type CreateJobRequest struct {
	Type    string         `json:"type" binding:"required" example:"email.send"`
	Payload map[string]any `json:"payload"`
}

// CreateJobResponse devolve o id do job aceito.
type CreateJobResponse struct {
	JobID string `json:"job_id" example:"7c9e6679-7425-40de-944b-e07fc1f90ae7"`
}

// JobResult é o resultado gravado por um worker.
type JobResult struct {
	Worker     string          `json:"worker"`
	Result     json.RawMessage `json:"result" swaggertype:"object"`
	FinishedAt time.Time       `json:"finished_at"`
}

// JobView é a visão de um job com os resultados dos workers.
type JobView struct {
	ID        string      `json:"id"`
	Type      string      `json:"type"`
	Status    string      `json:"status"`
	Origin    string      `json:"origin"`
	CreatedAt time.Time   `json:"created_at"`
	Results   []JobResult `json:"results"`
}

// ErrorResponse é o formato de erro comum da demo.
type ErrorResponse struct {
	Detail string `json:"detail"`
}

// Envelope é a mensagem do contrato (contracts/envelope.schema.json).
type Envelope struct {
	JobID     string         `json:"job_id"`
	Type      string         `json:"type"`
	Payload   map[string]any `json:"payload"`
	CreatedAt time.Time      `json:"created_at"`
	Origin    string         `json:"origin"`
}
