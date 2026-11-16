package store_test

import (
	"context"
	"testing"
	"time"

	"github.com/aditya388182/payments-cdc-pipeline/oncall/api/internal/alerts"
	"github.com/aditya388182/payments-cdc-pipeline/oncall/api/internal/store"
	"github.com/aditya388182/payments-cdc-pipeline/oncall/api/internal/store/storetest"
)

func TestMigrationsApplyOnceAndAreIdempotent(t *testing.T) {
	st := storetest.New(t)
	ctx := context.Background()
	if err := st.Migrate(ctx); err != nil { // second run is a no-op
		t.Fatal(err)
	}
	got, err := st.MigrationsApplied(ctx)
	if err != nil || len(got) == 0 || got[0] != "001_core.sql" {
		t.Fatalf("applied = %v err=%v", got, err)
	}
}

func TestAuditLogWritesAndPagesNewestFirst(t *testing.T) {
	st := storetest.New(t)
	ctx := context.Background()
	for i := 0; i < 5; i++ {
		if err := st.AddAudit(ctx, store.AuditEntry{Actor: "admin-user", Role: "admin", Action: "dlq.replay", Target: "dlq:" + string(rune('0'+i)),
			RequestedAt: time.Now().UTC(), Result: "APPLIED", IdempotencyKey: "k" + string(rune('0'+i)), Details: map[string]any{"i": i}}); err != nil {
			t.Fatal(err)
		}
	}
	page1, next, err := st.ListAudit(ctx, 0, 2)
	if err != nil || len(page1) != 2 || next == 0 || page1[0].Target != "dlq:4" {
		t.Fatalf("page1=%+v next=%d err=%v", page1, next, err)
	}
	page2, next2, _ := st.ListAudit(ctx, next, 2)
	if len(page2) != 2 || page2[0].Target != "dlq:2" || next2 == 0 {
		t.Fatalf("page2 = %+v", page2)
	}
	page3, next3, _ := st.ListAudit(ctx, next2, 2)
	if len(page3) != 1 || next3 != 0 || page3[0].Details["i"].(float64) != 0 {
		t.Fatalf("page3 = %+v next=%d", page3, next3)
	}
}

func TestDeviceRegistrationUpsertsAndLists(t *testing.T) {
	st := storetest.New(t)
	ctx := context.Background()
	for _, u := range []string{"viewer-user", "admin-user"} { // same token re-registered by another user: still one row
		if err := st.UpsertDevice(ctx, "ExponentPushToken[abc]", u, "android"); err != nil {
			t.Fatal(err)
		}
	}
	_ = st.UpsertDevice(ctx, "ExponentPushToken[def]", "admin-user", "ios")
	toks, err := st.Tokens(ctx)
	if err != nil || len(toks) != 2 {
		t.Fatalf("tokens=%v err=%v", toks, err)
	}
	_ = st.RemoveDevice(ctx, "ExponentPushToken[abc]")
	toks, _ = st.Tokens(ctx)
	if len(toks) != 1 || toks[0] != "ExponentPushToken[def]" {
		t.Fatalf("after remove: %v", toks)
	}
}

func TestAlertStoreRoundTripAndOneLiveAlertPerRule(t *testing.T) {
	st := storetest.New(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Millisecond)
	a, err := st.Create(ctx, alerts.Alert{Rule: "lag", Severity: "critical", State: alerts.StateFiring, Summary: "lag high", FiredAt: now, UpdatedAt: now, Details: map[string]any{"lag_total": 5000}})
	if err != nil || a.ID == 0 {
		t.Fatalf("create: %+v %v", a, err)
	}
	if _, err := st.Create(ctx, alerts.Alert{Rule: "lag", Severity: "critical", State: alerts.StateFiring, Summary: "dup", FiredAt: now, UpdatedAt: now}); err == nil {
		t.Fatal("a second live alert for the same rule must be rejected by the unique index")
	}
	a.State = alerts.StateAcknowledged
	a.AckedBy = "admin-user"
	a.AckedAt = &now
	if err := st.Update(ctx, a); err != nil {
		t.Fatal(err)
	}
	got, _ := st.Get(ctx, a.ID)
	if got.State != alerts.StateAcknowledged || got.AckedBy != "admin-user" || got.Details["lag_total"].(float64) != 5000 {
		t.Fatalf("got = %+v", got)
	}
	act, _ := st.Active(ctx)
	if len(act) != 1 {
		t.Fatalf("active = %d", len(act))
	}
	got.State = alerts.StateResolved
	got.ResolvedAt = &now
	_ = st.Update(ctx, got)
	if _, err := st.Create(ctx, alerts.Alert{Rule: "lag", Severity: "critical", State: alerts.StateFiring, Summary: "again", FiredAt: now, UpdatedAt: now}); err != nil {
		t.Fatalf("after resolve a new alert for the rule must be allowed: %v", err)
	}
	list, _ := st.List(ctx, []string{alerts.StateResolved}, 10)
	if len(list) != 1 {
		t.Fatalf("resolved list = %d", len(list))
	}
	if _, err := st.Get(ctx, 99999); err != alerts.ErrNotFound {
		t.Fatalf("missing alert err = %v", err)
	}
}

func TestIdempotencyKeyLifecycle(t *testing.T) {
	st := storetest.New(t)
	ctx := context.Background()
	state, _, err := st.IdempotencyBegin(ctx, "admin-user", "k1", "POST /v1/dlq/1/replay", "h1")
	if err != nil || state != store.IdemFresh {
		t.Fatalf("first = %v %v", state, err)
	}
	if state, _, _ = st.IdempotencyBegin(ctx, "admin-user", "k1", "POST /v1/dlq/1/replay", "h1"); state != store.IdemInProgress {
		t.Fatalf("while running = %v", state)
	}
	_ = st.IdempotencyFinish(ctx, "admin-user", "k1", 200, []byte(`{"status":"APPLIED"}`))
	state, resp, _ := st.IdempotencyBegin(ctx, "admin-user", "k1", "POST /v1/dlq/1/replay", "h1")
	if state != store.IdemReplayed || resp.Status != 200 || string(resp.Body) != `{"status":"APPLIED"}` {
		t.Fatalf("replayed = %v %+v", state, resp)
	}
	if state, _, _ = st.IdempotencyBegin(ctx, "admin-user", "k1", "POST /v1/dlq/2/replay", "h2"); state != store.IdemConflict {
		t.Fatalf("different request with same key = %v", state)
	}
	if state, _, _ = st.IdempotencyBegin(ctx, "other-admin", "k1", "POST /v1/dlq/1/replay", "h1"); state != store.IdemFresh {
		t.Fatalf("keys are scoped per actor, got %v", state)
	}
	_ = st.IdempotencyAbort(ctx, "admin-user", "k1")
	if state, _, _ = st.IdempotencyBegin(ctx, "admin-user", "k1", "POST /v1/dlq/1/replay", "h1"); state != store.IdemFresh {
		t.Fatalf("after abort = %v", state)
	}
}
