// Package main é o relay: lê a outbox no Postgres e entrega cada envelope ao router.
//
// dispatcher.go cuida só do lado HTTP: faz o POST e traduz a resposta em um
// Outcome. O relay não sabe nada de filas nem de workers; isso é do router.
package main

import (
	"bytes"
	"context"
	"log"
	"net/http"
	"time"
)

// Outcome é o resultado de uma tentativa de entrega ao router.
// É o único dado que o restante do relay usa para decidir o destino da linha.
type Outcome int

const (
	// Delivered: router respondeu 2xx.
	Delivered Outcome = iota
	// Rejected: router respondeu 4xx; repetir não adianta (ex.: type desconhecido).
	Rejected
	// Retry: erro de rede ou 5xx; tentar de novo no próximo ciclo.
	Retry
)

// Dispatcher envia envelopes ao webhook do router.
type Dispatcher struct {
	url    string
	client *http.Client
}

// NewDispatcher cria o dispatcher para a URL do router (ex.: http://router:8080/dispatch).
// O timeout evita que um router travado segure a transação do relay para sempre.
func NewDispatcher(url string) *Dispatcher {
	return &Dispatcher{url: url, client: &http.Client{Timeout: 5 * time.Second}}
}

// Send faz POST do envelope e classifica a resposta. Qualquer falha que não
// seja um 4xx explícito vira Retry: na dúvida, o job não pode ser descartado.
func (d *Dispatcher) Send(ctx context.Context, envelope []byte) Outcome {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, d.url, bytes.NewReader(envelope))
	if err != nil {
		log.Printf("build request: %v", err)
		return Retry
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := d.client.Do(req)
	if err != nil {
		log.Printf("router unreachable: %v", err)
		return Retry
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			log.Printf("close body: %v", err)
		}
	}()
	switch {
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		return Delivered
	case resp.StatusCode >= 400 && resp.StatusCode < 500:
		return Rejected
	default:
		return Retry
	}
}
