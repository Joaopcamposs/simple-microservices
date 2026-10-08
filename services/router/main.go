package main

import (
	"fmt"
	"log/slog"
	"net/http"
	"os"
)

// env lê uma variável de ambiente com valor padrão.
func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// run monta as dependências (publisher + handler) e sobe o servidor HTTP.
// Estado de processo nasce aqui, no main, e é injetado: não há variável global.
func run() error {
	pub, err := NewAMQPPublisher(env("AMQP_URL", "amqp://guest:guest@localhost:55672/"))
	if err != nil {
		return err
	}
	defer func() {
		if err := pub.Close(); err != nil {
			slog.Error("close publisher", "error", err)
		}
	}()
	mux := http.NewServeMux()
	mux.Handle("POST /dispatch", NewDispatchHandler(DefaultRoutes(), pub))
	addr := env("ADDR", ":8080")
	slog.Info("listening", "addr", addr)
	serverErr := make(chan error, 1)
	go func() { serverErr <- http.ListenAndServe(addr, mux) }()
	// Sem conexão com o broker o router não serve para nada (todo publish
	// daria 502): sair com erro faz o compose reiniciá-lo já reconectado.
	select {
	case err := <-serverErr:
		return err
	case amqpErr := <-pub.Closed():
		return fmt.Errorf("amqp connection lost: %w", amqpErr)
	}
}

// main configura o log estruturado (JSON, uma linha por evento, campo "service")
// e delega a run. Todo log do processo usa o slog padrão configurado aqui.
func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("service", "router"))
	if err := run(); err != nil {
		slog.Error("fatal", "error", err)
		os.Exit(1)
	}
}
