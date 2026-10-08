package main

import (
	"context"
	"log"
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
			log.Printf("close amqp: %v", err)
		}
	}()
	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	log.Print("worker-go consuming jobs.go")
	return NewConsumer(ch, "jobs.go", prefetch, NewResultStore(pool)).Run(ctx)
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}
