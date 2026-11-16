package store_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aditya388182/payments-cdc-pipeline/oncall/api/internal/auth"
	"github.com/aditya388182/payments-cdc-pipeline/oncall/api/internal/store"
	"github.com/aditya388182/payments-cdc-pipeline/oncall/api/internal/store/storetest"
)

var t0 = time.Now().UTC().Truncate(time.Microsecond)

func TestAuthCodeCanBeConsumedExactlyOnceUnderConcurrency(t *testing.T) {
	st := store.AuthStore{Store: storetest.New(t)}
	ctx := context.Background()
	if err := st.SaveCode(ctx, "h1", auth.CodeData{Login: "admin-user", Role: "admin", CodeChallenge: "c", RedirectURI: "oncall://x", ExpiresAt: t0.Add(time.Minute)}); err != nil {
		t.Fatal(err)
	}
	var wins atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, ok, err := st.ConsumeCode(ctx, "h1", t0); err == nil && ok {
				wins.Add(1)
			}
		}()
	}
	wg.Wait()
	if wins.Load() != 1 {
		t.Fatalf("%d goroutines consumed the one-time code, want exactly 1", wins.Load())
	}
}

func TestExpiredCodeAndStateAreNotConsumable(t *testing.T) {
	st := store.AuthStore{Store: storetest.New(t)}
	ctx := context.Background()
	_ = st.SaveCode(ctx, "old", auth.CodeData{Login: "a", Role: "viewer", ExpiresAt: t0.Add(-time.Second)})
	if _, ok, _ := st.ConsumeCode(ctx, "old", t0); ok {
		t.Fatal("expired code consumed")
	}
	_ = st.SaveState(ctx, "s-old", auth.StateData{CodeChallenge: "c", RedirectURI: "oncall://x", ExpiresAt: t0.Add(-time.Second)})
	if _, ok, _ := st.ConsumeState(ctx, "s-old", t0); ok {
		t.Fatal("expired state consumed")
	}
	_ = st.SaveState(ctx, "s-new", auth.StateData{CodeChallenge: "c", RedirectURI: "oncall://x", ExpiresAt: t0.Add(time.Minute)})
	if d, ok, _ := st.ConsumeState(ctx, "s-new", t0); !ok || d.RedirectURI != "oncall://x" {
		t.Fatal("valid state not consumed")
	}
	if _, ok, _ := st.ConsumeState(ctx, "s-new", t0); ok {
		t.Fatal("state consumed twice")
	}
}

func TestRefreshRotationReuseAndRevocationInPostgres(t *testing.T) {
	st := store.AuthStore{Store: storetest.New(t)}
	ctx := context.Background()
	fam := "7c2f0e0e-3c1a-4b3a-9b1d-000000000001"
	if err := st.CreateRefresh(ctx, auth.RefreshRecord{ID: "7c2f0e0e-3c1a-4b3a-9b1d-0000000000a1", FamilyID: fam, Login: "admin-user", Role: "admin", TokenHash: "t1", ExpiresAt: t0.Add(time.Hour)}); err != nil {
		t.Fatal(err)
	}
	old, res, err := st.RotateRefresh(ctx, "t1", auth.RefreshRecord{ID: "7c2f0e0e-3c1a-4b3a-9b1d-0000000000a2", TokenHash: "t2", ExpiresAt: t0.Add(time.Hour)}, t0)
	if err != nil || res != auth.Rotated || old.Login != "admin-user" || old.FamilyID != fam {
		t.Fatalf("rotate: %v %v %+v", res, err, old)
	}
	// t1 again: reuse -> whole family revoked, including t2
	if _, res, _ := st.RotateRefresh(ctx, "t1", auth.RefreshRecord{ID: "7c2f0e0e-3c1a-4b3a-9b1d-0000000000a3", TokenHash: "t3", ExpiresAt: t0.Add(time.Hour)}, t0); res != auth.Reused {
		t.Fatalf("reuse result = %v", res)
	}
	if _, res, _ := st.RotateRefresh(ctx, "t2", auth.RefreshRecord{ID: "7c2f0e0e-3c1a-4b3a-9b1d-0000000000a4", TokenHash: "t4", ExpiresAt: t0.Add(time.Hour)}, t0); res != auth.Revoked {
		t.Fatalf("the newest token of a revoked family must be dead, got %v", res)
	}
	if _, res, _ := st.RotateRefresh(ctx, "nope", auth.RefreshRecord{}, t0); res != auth.NotFound {
		t.Fatalf("unknown = %v", res)
	}
}

func TestConcurrentRefreshOfTheSameTokenRotatesOnlyOnce(t *testing.T) {
	st := store.AuthStore{Store: storetest.New(t)}
	ctx := context.Background()
	_ = st.CreateRefresh(ctx, auth.RefreshRecord{ID: "7c2f0e0e-3c1a-4b3a-9b1d-0000000000b0", FamilyID: "7c2f0e0e-3c1a-4b3a-9b1d-000000000002", Login: "a", Role: "viewer", TokenHash: "tok", ExpiresAt: t0.Add(time.Hour)})
	var rotated, reused atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		id := "7c2f0e0e-3c1a-4b3a-9b1d-0000000001" + string(rune('0'+i)) + "0"
		go func(i int) {
			defer wg.Done()
			_, res, err := st.RotateRefresh(ctx, "tok", auth.RefreshRecord{ID: id, TokenHash: "next-" + id, ExpiresAt: t0.Add(time.Hour)}, t0)
			if err != nil {
				return
			}
			switch res {
			case auth.Rotated:
				rotated.Add(1)
			case auth.Reused, auth.Revoked:
				reused.Add(1)
			}
		}(i)
	}
	wg.Wait()
	if rotated.Load() != 1 || reused.Load() != 9 {
		t.Fatalf("rotated=%d reused/revoked=%d, want 1 and 9 (the row lock serialises the race)", rotated.Load(), reused.Load())
	}
}

func TestExpiredRefreshAndLogoutRevocation(t *testing.T) {
	st := store.AuthStore{Store: storetest.New(t)}
	ctx := context.Background()
	fam := "7c2f0e0e-3c1a-4b3a-9b1d-000000000003"
	_ = st.CreateRefresh(ctx, auth.RefreshRecord{ID: "7c2f0e0e-3c1a-4b3a-9b1d-0000000000c1", FamilyID: fam, Login: "a", Role: "viewer", TokenHash: "exp", ExpiresAt: t0.Add(-time.Minute)})
	if _, res, _ := st.RotateRefresh(ctx, "exp", auth.RefreshRecord{ID: "7c2f0e0e-3c1a-4b3a-9b1d-0000000000c2", TokenHash: "x", ExpiresAt: t0.Add(time.Hour)}, t0); res != auth.Expired {
		t.Fatalf("expired = %v", res)
	}
	_ = st.CreateRefresh(ctx, auth.RefreshRecord{ID: "7c2f0e0e-3c1a-4b3a-9b1d-0000000000c3", FamilyID: fam, Login: "a", Role: "viewer", TokenHash: "live", ExpiresAt: t0.Add(time.Hour)})
	if err := st.RevokeFamily(ctx, fam, t0); err != nil {
		t.Fatal(err)
	}
	if _, res, _ := st.RotateRefresh(ctx, "live", auth.RefreshRecord{ID: "7c2f0e0e-3c1a-4b3a-9b1d-0000000000c4", TokenHash: "y", ExpiresAt: t0.Add(time.Hour)}, t0); res != auth.Revoked {
		t.Fatalf("after logout = %v", res)
	}
}

func TestTokensAreStoredOnlyAsHashes(t *testing.T) {
	s := storetest.New(t)
	st := store.AuthStore{Store: s}
	ctx := context.Background()
	raw := auth.RandomToken(32)
	_ = st.CreateRefresh(ctx, auth.RefreshRecord{ID: "7c2f0e0e-3c1a-4b3a-9b1d-0000000000d1", FamilyID: "7c2f0e0e-3c1a-4b3a-9b1d-000000000004", Login: "a", Role: "viewer", TokenHash: auth.HashToken(raw), ExpiresAt: t0.Add(time.Hour)})
	var n int
	if err := s.Pool.QueryRow(ctx, `select count(*) from refresh_tokens where token_hash = $1`, raw).Scan(&n); err != nil || n != 0 {
		t.Fatalf("the raw token must never be stored (n=%d err=%v)", n, err)
	}
}

func TestAuthAuditWritesAnAuditRow(t *testing.T) {
	s := storetest.New(t)
	store.AuthAudit{Store: s}.Audit(context.Background(), auth.AuditEvent{Actor: "outsider", Action: "auth.login", Result: "not_allowed", Error: "not on the allowlist"})
	entries, _, err := s.ListAudit(context.Background(), 0, 10)
	if err != nil || len(entries) != 1 || entries[0].Action != "auth.login" || entries[0].Result != "not_allowed" || entries[0].Actor != "outsider" {
		t.Fatalf("%+v %v", entries, err)
	}
}
