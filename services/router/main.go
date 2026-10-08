package main

import (
	"log"
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
			log.Printf("close publisher: %v", err)
		}
	}()
	mux := http.NewServeMux()
	mux.Handle("POST /dispatch", NewDispatchHandler(DefaultRoutes(), pub))
	addr := env("ADDR", ":8080")
	log.Printf("router listening on %s", addr)
	return http.ListenAndServe(addr, mux)
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}
