package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// fakePublisher registra o que o handler tentou publicar, sem precisar de RabbitMQ.
// É possível porque o handler depende da interface Publisher, não do AMQPPublisher.
type fakePublisher struct {
	calls  int
	worker Worker
	body   []byte
	err    error
}

func (f *fakePublisher) Publish(_ context.Context, w Worker, b []byte) error {
	f.calls++
	f.worker, f.body = w, b
	return f.err
}

// dispatch faz um POST /dispatch direto no handler e devolve a resposta gravada.
func dispatch(pub Publisher, body string) *httptest.ResponseRecorder {
	h := NewDispatchHandler(DefaultRoutes(), pub)
	req := httptest.NewRequest(http.MethodPost, "/dispatch", strings.NewReader(body))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

// Cada type conhecido vai para o worker da tabela, com o corpo original intacto.
func TestDispatchRoutesByType(t *testing.T) {
	cases := map[string]Worker{
		"report.generate": "celery",
		"email.send":      "taskiq",
		"http.fetch":      "asyncio",
		"image.resize":    "go",
	}
	for typ, want := range cases {
		t.Run(typ, func(t *testing.T) {
			pub := &fakePublisher{}
			body := fmt.Sprintf(`{"job_id":"j1","type":%q}`, typ)
			rec := dispatch(pub, body)
			if rec.Code != http.StatusAccepted {
				t.Fatalf("status = %d, want 202", rec.Code)
			}
			if pub.worker != want {
				t.Fatalf("worker = %q, want %q", pub.worker, want)
			}
			if string(pub.body) != body {
				t.Fatalf("body alterado: %s", pub.body)
			}
		})
	}
}

// Type desconhecido é erro permanente (422): o relay marca o job como FAILED.
func TestDispatchUnknownTypeIs422(t *testing.T) {
	pub := &fakePublisher{}
	rec := dispatch(pub, `{"job_id":"j1","type":"nope"}`)
	if rec.Code != http.StatusUnprocessableEntity || pub.calls != 0 {
		t.Fatalf("status = %d calls = %d", rec.Code, pub.calls)
	}
}

// Corpo que não é um envelope utilizável é erro permanente (400), sem publicar nada.
func TestDispatchInvalidBodyIs400(t *testing.T) {
	for _, body := range []string{`not json`, `{"job_id":"j1"}`, `{"job_id":"j1","type":""}`} {
		pub := &fakePublisher{}
		rec := dispatch(pub, body)
		if rec.Code != http.StatusBadRequest || pub.calls != 0 {
			t.Fatalf("body %q: status = %d calls = %d", body, rec.Code, pub.calls)
		}
	}
}

// Broker fora do ar é erro temporário (502): o relay tenta de novo depois.
func TestDispatchPublishFailureIs502(t *testing.T) {
	pub := &fakePublisher{err: errors.New("broker down")}
	rec := dispatch(pub, `{"job_id":"j1","type":"email.send"}`)
	if rec.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502", rec.Code)
	}
}

// O healthcheck do compose depende de GET /healthz responder 200.
func TestHealthOK(t *testing.T) {
	rec := httptest.NewRecorder()
	HandleHealth(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d, want 200", rec.Code)
	}
}
