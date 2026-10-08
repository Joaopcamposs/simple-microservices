package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// batchSize é quantas linhas da outbox cada ciclo processa.
const batchSize = 10

// env lê uma variável de ambiente com valor padrão.
func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// run monta as dependências (pool, dispatcher, relay) e roda o loop até
// SIGINT/SIGTERM. Estado de processo nasce aqui, sem variável global.
func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	pool, err := pgxpool.New(ctx, env("DATABASE_URL", "postgres://app:app@localhost:55432/app"))
	if err != nil {
		return err
	}
	defer pool.Close()
	interval, err := time.ParseDuration(env("POLL_INTERVAL", "1s"))
	if err != nil {
		return err
	}
	dispatcher := NewDispatcher(env("ROUTER_URL", "http://localhost:8080/dispatch"))
	mux := http.NewServeMux()
	mux.Handle("GET /healthz", NewHealthHandler(pool))
	go func() {
		if err := http.ListenAndServe(env("ADDR", ":8081"), mux); err != nil {
			slog.Error("health server", "error", err)
		}
	}()
	slog.Info("polling outbox", "interval", interval.String())
	NewRelay(pool, dispatcher, batchSize, interval).Run(ctx)
	return nil
}

// main configura o log estruturado (JSON, uma linha por evento, campo "service")
// e delega a run. Todo log do processo usa o slog padrão configurado aqui.
func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("service", "relay"))
	if err := run(); err != nil {
		slog.Error("fatal", "error", err)
		os.Exit(1)
	}
}
