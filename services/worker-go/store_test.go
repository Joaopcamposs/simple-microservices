package main

import (
	"context"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

const testJobID = "44444444-4444-4444-4444-444444444444"

// Reentrega da mesma mensagem (at-least-once) não pode duplicar o resultado:
// uma linha em job_results e o job em DONE.
// Exige TEST_DATABASE_URL; o TRUNCATE apaga tudo, use só o Postgres de dev.
func TestSaveIsIdempotent(t *testing.T) {
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
	for _, s := range []string{
		`TRUNCATE job_results, outbox, jobs CASCADE`,
		`INSERT INTO jobs (id, type, payload, origin) VALUES ('` + testJobID + `', 'image.resize', '{}', 'gateway-go')`,
	} {
		if _, err := pool.Exec(ctx, s); err != nil {
			t.Fatal(err)
		}
	}
	store := NewResultStore(pool)
	for range 2 {
		if err := store.Save(ctx, testJobID, "go", Result{Worker: "go", Detail: "ok"}); err != nil {
			t.Fatal(err)
		}
	}
	var rows int
	var status string
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM job_results WHERE job_id = $1::uuid`, testJobID).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT status FROM jobs WHERE id = $1::uuid`, testJobID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if rows != 1 || status != "DONE" {
		t.Fatalf("rows=%d status=%s", rows, status)
	}
}
