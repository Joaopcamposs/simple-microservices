package main

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// testRepo conecta ao banco de teste e LIMPA as tabelas (só usar no Postgres de dev).
func testRepo(t *testing.T) (*JobRepository, *pgxpool.Pool) {
	t.Helper()
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL não definido")
	}
	pool, err := pgxpool.New(context.Background(), url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if _, err := pool.Exec(context.Background(), `TRUNCATE job_results, outbox, jobs CASCADE`); err != nil {
		t.Fatal(err)
	}
	return NewJobRepository(pool), pool
}

func TestCreateWritesJobAndOutboxTogether(t *testing.T) {
	repo, pool := testRepo(t)
	ctx := context.Background()
	id, err := repo.Create(ctx, CreateJobRequest{Type: "email.send"}, "gateway-go")
	if err != nil {
		t.Fatal(err)
	}
	if parsed, err := uuid.Parse(id); err != nil || parsed.Version() != 7 {
		t.Fatalf("id %q não é UUIDv7 (err=%v)", id, err)
	}
	var status, envType, payload string
	err = pool.QueryRow(ctx, `
		SELECT o.status, o.envelope->>'type', (o.envelope->'payload')::text
		FROM outbox o WHERE o.job_id = $1::uuid`, id).Scan(&status, &envType, &payload)
	if err != nil {
		t.Fatal(err)
	}
	if status != "pending" || envType != "email.send" || payload != "{}" {
		t.Fatalf("outbox status=%s type=%s payload=%s", status, envType, payload)
	}
}

func TestGetUnknownJobReturnsNotFound(t *testing.T) {
	repo, _ := testRepo(t)
	if _, err := repo.Get(context.Background(), "33333333-3333-3333-3333-333333333333"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("err = %v, want ErrNotFound", err)
	}
}
