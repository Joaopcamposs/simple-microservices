package main

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// sendFunc adapta uma função ao Sender, para o teste ditar o Outcome.
type sendFunc func(ctx context.Context, envelope []byte) Outcome

func (f sendFunc) Send(ctx context.Context, e []byte) Outcome { return f(ctx, e) }

const testJobID = "11111111-1111-1111-1111-111111111111"

// setup conecta ao banco de teste, LIMPA as tabelas e insere um job + outbox pendente.
// Exige TEST_DATABASE_URL (Postgres de dev do compose: o TRUNCATE apaga tudo).
func setup(t *testing.T, jobStatus string) *pgxpool.Pool {
	t.Helper()
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL não definido")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	stmts := []string{
		`TRUNCATE job_results, outbox, jobs CASCADE`,
		`INSERT INTO jobs (id, type, payload, origin, status) VALUES ('` + testJobID + `', 'email.send', '{}', 'gateway-go', '` + jobStatus + `')`,
		`INSERT INTO outbox (job_id, envelope) VALUES ('` + testJobID + `', '{"job_id":"` + testJobID + `","type":"email.send"}')`,
	}
	for _, s := range stmts {
		if _, err := pool.Exec(ctx, s); err != nil {
			t.Fatal(err)
		}
	}
	return pool
}

// statuses devolve (outbox.status, jobs.status) do job de teste.
func statuses(t *testing.T, pool *pgxpool.Pool) (string, string) {
	t.Helper()
	var outbox, job string
	err := pool.QueryRow(context.Background(),
		`SELECT o.status, j.status FROM outbox o JOIN jobs j ON j.id = o.job_id`).Scan(&outbox, &job)
	if err != nil {
		t.Fatal(err)
	}
	return outbox, job
}

// runOnce roda um ciclo do relay com um Sender que sempre devolve o outcome dado.
func runOnce(t *testing.T, pool *pgxpool.Pool, outcome Outcome) {
	t.Helper()
	sender := sendFunc(func(context.Context, []byte) Outcome { return outcome })
	if err := NewRelay(pool, sender, 10, time.Second).RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
}

// 2xx do router: outbox sent e job DISPATCHED.
func TestRelayDeliveredMarksSentAndDispatched(t *testing.T) {
	pool := setup(t, "PENDING")
	runOnce(t, pool, Delivered)
	if o, j := statuses(t, pool); o != "sent" || j != "DISPATCHED" {
		t.Fatalf("outbox=%s job=%s", o, j)
	}
}

// Router fora do ar / 5xx: nada muda, a linha será tentada de novo.
func TestRelayRetryKeepsRowPending(t *testing.T) {
	pool := setup(t, "PENDING")
	runOnce(t, pool, Retry)
	if o, j := statuses(t, pool); o != "pending" || j != "PENDING" {
		t.Fatalf("outbox=%s job=%s", o, j)
	}
}

// 4xx do router (ex.: type desconhecido): falha definitiva.
func TestRelayRejectedMarksFailed(t *testing.T) {
	pool := setup(t, "PENDING")
	runOnce(t, pool, Rejected)
	if o, j := statuses(t, pool); o != "failed" || j != "FAILED" {
		t.Fatalf("outbox=%s job=%s", o, j)
	}
}

// Worker rápido já marcou DONE: o relay não pode regredir para DISPATCHED.
func TestRelayDoesNotRegressDoneJob(t *testing.T) {
	pool := setup(t, "DONE")
	runOnce(t, pool, Delivered)
	if o, j := statuses(t, pool); o != "sent" || j != "DONE" {
		t.Fatalf("outbox=%s job=%s", o, j)
	}
}
