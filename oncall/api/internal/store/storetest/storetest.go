// Package storetest gives tests a Store backed by an isolated schema in a real Postgres
// (ONCALL_TEST_PG_DSN, the compose Postgres locally or a service container in CI). Tests are skipped when it is unset.
package storetest

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/aditya388182/payments-cdc-pipeline/oncall/api/internal/store"
	"github.com/jackc/pgx/v5/pgxpool"
)

func New(t testing.TB) *store.Store {
	t.Helper()
	dsn := os.Getenv("ONCALL_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("ONCALL_TEST_PG_DSN not set (integration test needs Postgres)")
	}
	ctx := context.Background()
	schema := fmt.Sprintf("t_%d_%d", time.Now().UnixNano(), rand.Intn(1<<20))
	admin, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Exec(ctx, "create schema "+schema); err != nil {
		t.Fatal(err)
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	st, err := store.OpenConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		st.Close()
		_, _ = admin.Exec(ctx, "drop schema "+schema+" cascade")
		admin.Close()
	})
	return st
}
