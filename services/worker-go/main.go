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

// prefetch é o máximo de mensagens (e goroutines) em processamento ao mesmo tempo.
const prefetch = 10

// env lê uma variável de ambiente com valor padrão.
func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// run monta as dependências (banco, broker, store, consumer) e consome até
// SIGINT/SIGTERM. Não reconecta sozinho: se a conexão cair, o processo sai e o
// compose reinicia.
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
	slog.Info("consuming", "queue", "jobs.go")
	return NewConsumer(ch, "jobs.go", prefetch, NewResultStore(pool)).Run(ctx)
}

// main configura o log estruturado (JSON, uma linha por evento, campo "service")
// e delega a run. Todo log do processo usa o slog padrão configurado aqui.
func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("service", "worker-go"))
	if err := run(); err != nil {
		slog.Error("fatal", "error", err)
		os.Exit(1)
	}
}
