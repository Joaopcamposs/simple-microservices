package main

import (
	"context"
	"net/http"
	"time"
)

// Pinger é o que o healthcheck precisa do banco; o pgxpool.Pool satisfaz.
type Pinger interface {
	Ping(ctx context.Context) error
}

// NewHealthHandler responde 200 se o banco responde ao ping e 503 se não.
// O relay só é útil com o Postgres de pé, então o ping é o sinal de saúde.
func NewHealthHandler(db Pinger) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if err := db.Ping(ctx); err != nil {
			http.Error(w, "database unavailable", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
}
