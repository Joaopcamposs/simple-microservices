package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

// fakePinger devolve o erro configurado, sem precisar de Postgres.
type fakePinger struct{ err error }

func (f fakePinger) Ping(context.Context) error { return f.err }

// Banco respondendo é 200; banco fora é 503 (o healthcheck do compose marca unhealthy).
func TestHealthReflectsDatabase(t *testing.T) {
	cases := map[string]struct {
		err  error
		want int
	}{
		"up":   {nil, http.StatusOK},
		"down": {errors.New("refused"), http.StatusServiceUnavailable},
	}
	for name, c := range cases {
		rec := httptest.NewRecorder()
		NewHealthHandler(fakePinger{c.err}).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
		if rec.Code != c.want {
			t.Errorf("%s: status %d, want %d", name, rec.Code, c.want)
		}
	}
}
