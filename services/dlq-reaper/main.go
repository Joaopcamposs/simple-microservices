package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/jackc/pgx/v5/pgxpool"
	amqp "github.com/rabbitmq/amqp091-go"
)

// env lê uma variável de ambiente com valor padrão.
func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// run monta as dependências (banco, broker) e consome a DLQ até SIGINT/SIGTERM.
// Não reconecta sozinho: se a conexão cair, o processo sai e o compose reinicia.
func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	pool, err := pgxpool.New(ctx, env("DATABASE_URL", "postgres://app:app@localhost:55432/app"))
	if err != nil {
		return err
	}
	defer pool.Close()
	conn, err := amqp.Dial(env("AMQP_URL", "amqp://guest:guest@localhost:55672/"))
	if err != nil {
		return err
	}
	defer func() {
		if err := conn.Close(); err != nil {
			slog.Error("close amqp", "error", err)
		}
	}()
	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	slog.Info("consuming", "queue", "jobs.dlq")
	return NewReaper(ch, "jobs.dlq", NewJobStore(pool)).Run(ctx)
}

// main configura o log estruturado (JSON, campo "service") e delega a run.
func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("service", "dlq-reaper"))
	if err := run(); err != nil {
		slog.Error("fatal", "error", err)
		os.Exit(1)
	}
}
