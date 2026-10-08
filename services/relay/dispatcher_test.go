package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

// O relay decide o destino da linha da outbox só pelo status HTTP do router.
// Este teste fixa essa tabela de tradução: status -> Outcome.
func TestDispatcherMapsStatusToOutcome(t *testing.T) {
	cases := map[int]Outcome{202: Delivered, 400: Rejected, 422: Rejected, 500: Retry, 502: Retry}
	for status, want := range cases {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(status)
		}))
		got := NewDispatcher(srv.URL).Send(context.Background(), []byte(`{}`))
		srv.Close()
		if got != want {
			t.Fatalf("status %d: outcome = %v, want %v", status, got, want)
		}
	}
}

// Router fora do ar não pode perder o job: o resultado tem de ser Retry.
func TestDispatcherUnreachableRouterIsRetry(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	url := srv.URL
	srv.Close() // porta fechada: simula router fora do ar
	if got := NewDispatcher(url).Send(context.Background(), []byte(`{}`)); got != Retry {
		t.Fatalf("outcome = %v, want Retry", got)
	}
}
