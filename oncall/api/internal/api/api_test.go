package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	pb "github.com/aditya388182/payments-cdc-pipeline/oncall/api/gen/oncall/v1"
	"github.com/aditya388182/payments-cdc-pipeline/oncall/api/internal/alerts"
	"github.com/aditya388182/payments-cdc-pipeline/oncall/api/internal/auth"
	"github.com/aditya388182/payments-cdc-pipeline/oncall/api/internal/config"
	"github.com/aditya388182/payments-cdc-pipeline/oncall/api/internal/generator"
	"github.com/aditya388182/payments-cdc-pipeline/oncall/api/internal/hub"
	"github.com/aditya388182/payments-cdc-pipeline/oncall/api/internal/snapshot"
	"github.com/aditya388182/payments-cdc-pipeline/oncall/api/internal/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ---- fakes -----------------------------------------------------------------------------------

type fakeStore struct {
	*alerts.MemStore
	mu      sync.Mutex
	audit   []store.AuditEntry
	devices map[string]string
	idem    map[string]*idemRow
}

type idemRow struct {
	route, hash string
	status      int
	body        []byte
}

func newFakeStore() *fakeStore {
	return &fakeStore{MemStore: alerts.NewMemStore(), devices: map[string]string{}, idem: map[string]*idemRow{}}
}

func (f *fakeStore) Tokens(context.Context) ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []string
	for t := range f.devices {
		out = append(out, t)
	}
	return out, nil
}
func (f *fakeStore) UpsertDevice(_ context.Context, token, user, _ string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.devices[token] = user
	return nil
}
func (f *fakeStore) RemoveDeviceFor(_ context.Context, token, user string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	owner, ok := f.devices[token]
	if !ok || (user != "" && owner != user) {
		return false, nil
	}
	delete(f.devices, token)
	return true, nil
}
func (f *fakeStore) AddAudit(_ context.Context, e store.AuditEntry) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	e.ID = int64(len(f.audit) + 1)
	f.audit = append(f.audit, e)
	return nil
}
func (f *fakeStore) ListAudit(_ context.Context, _ int64, _ int) ([]store.AuditEntry, int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := append([]store.AuditEntry(nil), f.audit...)
	return out, 0, nil
}
func (f *fakeStore) IdempotencyBegin(_ context.Context, actor, key, route, hash string) (store.IdemState, store.StoredResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	k := actor + "|" + key
	row, ok := f.idem[k]
	if !ok {
		f.idem[k] = &idemRow{route: route, hash: hash}
		return store.IdemFresh, store.StoredResponse{}, nil
	}
	switch {
	case row.route != route || row.hash != hash:
		return store.IdemConflict, store.StoredResponse{}, nil
	case row.status == 0:
		return store.IdemInProgress, store.StoredResponse{}, nil
	}
	return store.IdemReplayed, store.StoredResponse{Status: row.status, Body: row.body}, nil
}
func (f *fakeStore) IdempotencyFinish(_ context.Context, actor, key string, st int, body []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	r := f.idem[actor+"|"+key]
	r.status, r.body = st, body
	return nil
}
func (f *fakeStore) IdempotencyAbort(_ context.Context, actor, key string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.idem, actor+"|"+key)
	return nil
}
func (f *fakeStore) Ping(context.Context) error { return nil }
func (f *fakeStore) auditCount(action string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, e := range f.audit {
		if e.Action == action {
			n++
		}
	}
	return n
}

type fakeReplay struct {
	mu      sync.Mutex
	calls   int
	results []*pb.ReplayResponse // consumed in order; the last one repeats
	err     error
}

func (f *fakeReplay) Replay(_ context.Context, id, by, key string) (*pb.ReplayResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.err != nil {
		return nil, f.err
	}
	i := f.calls - 1
	if i >= len(f.results) {
		i = len(f.results) - 1
	}
	r := f.results[i]
	return &pb.ReplayResponse{Status: r.Status, Detail: r.Detail, LakeLsn: r.LakeLsn}, nil
}
func (f *fakeReplay) ListDlq(_ context.Context, cursor string, limit int32, reason string) (*pb.ListDlqResponse, error) {
	return &pb.ListDlqResponse{Records: []*pb.DlqRecord{{Id: "1", TransactionId: "t-1", RejectionReason: "UNKNOWN_MERCHANT"}}, OpenTotal: 1, TopicMessages: 3}, nil
}
func (f *fakeReplay) GetDlq(_ context.Context, id string) (*pb.GetDlqResponse, error) {
	if id == "404" {
		return &pb.GetDlqResponse{}, nil
	}
	return &pb.GetDlqResponse{Found: true, Record: &pb.DlqRecord{Id: id, TransactionId: "t-1", PayloadJson: `{"a":1}`}}, nil
}
func (f *fakeReplay) LakeStats(context.Context) (*pb.LakeStatsResponse, error) {
	return &pb.LakeStatsResponse{}, nil
}
func (f *fakeReplay) DlqStats(context.Context) (*pb.DlqStatsResponse, error) {
	return &pb.DlqStatsResponse{}, nil
}

func applied(detail string) *pb.ReplayResponse {
	return &pb.ReplayResponse{Status: pb.ReplayStatus_REPLAY_STATUS_APPLIED, Detail: detail, LakeLsn: 77}
}

// ---- harness ---------------------------------------------------------------------------------

type env struct {
	srv    *httptest.Server
	st     *fakeStore
	rp     *fakeReplay
	gen    *generator.Fake
	signer *auth.Signer
	engine *alerts.Engine
	hub    *hub.Hub
}

func newEnv(t *testing.T) *env {
	t.Helper()
	cfg := config.Config{MaxRate: 200, CORSOrigins: []string{"http://127.0.0.1:8081"}}
	st := newFakeStore()
	h := hub.New()
	signer := auth.NewSigner(strings.Repeat("s", 40), 15*time.Minute)
	engine := alerts.NewEngine(nil, st, st, nil, h)
	rp := &fakeReplay{results: []*pb.ReplayResponse{applied("first")}}
	gen := &generator.Fake{}
	latest := &Latest{}
	latest.Set(snapshot.Snapshot{Seq: 7, LagTotal: 3})
	srv := httptest.NewServer(New(Deps{Cfg: cfg, Store: st, Hub: h, Engine: engine, Replay: rp, Gen: gen, Signer: signer, Latest: latest}))
	t.Cleanup(srv.Close)
	return &env{srv, st, rp, gen, signer, engine, h}
}

func (e *env) token(t *testing.T, login, role string) string {
	t.Helper()
	tok, _, err := e.signer.Issue(auth.Principal{Login: login, Role: role})
	if err != nil {
		t.Fatal(err)
	}
	return tok
}

func (e *env) do(t *testing.T, method, path, token string, body any, hdr map[string]string) (*http.Response, []byte) {
	t.Helper()
	var rd io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		rd = bytes.NewReader(b)
	}
	req, _ := http.NewRequest(method, e.srv.URL+path, rd)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp, b
}

func errCode(t *testing.T, b []byte) string {
	t.Helper()
	var e struct {
		Error struct{ Code, Message string } `json:"error"`
	}
	if err := json.Unmarshal(b, &e); err != nil {
		t.Fatalf("not an error body: %s", b)
	}
	return e.Error.Code
}

// ---- authentication / authorization ---------------------------------------------------------

var allRoutes = []struct{ method, path string }{
	{"GET", "/v1/live"}, {"GET", "/v1/snapshot"}, {"DELETE", "/v1/devices"}, {"GET", "/v1/alerts"}, {"POST", "/v1/alerts/1/ack"}, {"GET", "/v1/dlq"},
	{"GET", "/v1/dlq/1"}, {"POST", "/v1/devices"}, {"GET", "/v1/generator"}, {"GET", "/v1/me"},
	{"POST", "/v1/dlq/1/replay"}, {"POST", "/v1/generator"}, {"GET", "/v1/audit"},
}

var adminRoutes = map[string]bool{"POST /v1/dlq/1/replay": true, "POST /v1/generator": true, "GET /v1/audit": true}

func TestEveryProtectedRouteReturns401WithoutAToken(t *testing.T) {
	e := newEnv(t)
	for _, rt := range allRoutes {
		resp, body := e.do(t, rt.method, rt.path, "", nil, nil)
		if resp.StatusCode != 401 || errCode(t, body) != "unauthorized" {
			t.Errorf("%s %s: status %d body %s, want 401 unauthorized", rt.method, rt.path, resp.StatusCode, body)
		}
	}
}

func TestInvalidAndExpiredAndForgedTokensAre401(t *testing.T) {
	e := newEnv(t)
	past := auth.NewSigner(strings.Repeat("s", 40), time.Minute).WithClock(func() time.Time { return time.Now().Add(-time.Hour) })
	expired, _, _ := past.Issue(auth.Principal{Login: "x", Role: auth.RoleAdmin})
	forged, _, _ := auth.NewSigner(strings.Repeat("z", 40), time.Minute).Issue(auth.Principal{Login: "x", Role: auth.RoleAdmin})
	cases := map[string]string{"garbage": "not.a.jwt", "expired": expired, "wrong secret": forged}
	for name, tok := range cases {
		resp, body := e.do(t, "GET", "/v1/snapshot", tok, nil, nil)
		if resp.StatusCode != 401 {
			t.Errorf("%s: status %d", name, resp.StatusCode)
		}
		if name == "expired" && errCode(t, body) != "token_expired" {
			t.Errorf("expired token code = %s", errCode(t, body))
		}
	}
}

func TestViewerGets403OnAdminRoutesAndSucceedsOnViewerRoutes(t *testing.T) {
	e := newEnv(t)
	tok := e.token(t, "viewer-user", auth.RoleViewer)
	for _, rt := range allRoutes {
		if rt.path == "/v1/live" {
			continue // websocket upgrade is covered in the hub tests
		}
		resp, body := e.do(t, rt.method, rt.path, tok, map[string]any{}, map[string]string{"Idempotency-Key": "abcdefgh-1"})
		if adminRoutes[rt.method+" "+rt.path] {
			if resp.StatusCode != 403 || errCode(t, body) != "forbidden" {
				t.Errorf("%s %s: viewer got %d %s, want 403 forbidden", rt.method, rt.path, resp.StatusCode, body)
			}
		} else if resp.StatusCode == 401 || resp.StatusCode == 403 {
			t.Errorf("%s %s: viewer wrongly refused (%d)", rt.method, rt.path, resp.StatusCode)
		}
	}
	if len(e.rp.results) > 0 && e.rp.calls != 0 || len(e.gen.Starts) != 0 {
		t.Fatal("a refused viewer request must not reach replay-svc or the generator")
	}
}

func TestAdminReachesAdminRoutes(t *testing.T) {
	e := newEnv(t)
	tok := e.token(t, "admin-user", auth.RoleAdmin)
	if resp, _ := e.do(t, "GET", "/v1/audit", tok, nil, nil); resp.StatusCode != 200 {
		t.Fatalf("audit status %d", resp.StatusCode)
	}
}

// ---- replay ----------------------------------------------------------------------------------

func TestReplayWithoutIdempotencyKeyIs400AndNothingRuns(t *testing.T) {
	e := newEnv(t)
	resp, body := e.do(t, "POST", "/v1/dlq/12/replay", e.token(t, "admin-user", auth.RoleAdmin), nil, nil)
	if resp.StatusCode != 400 || errCode(t, body) != "idempotency_key_required" {
		t.Fatalf("%d %s", resp.StatusCode, body)
	}
	if e.rp.calls != 0 || e.st.auditCount("dlq.replay") != 0 {
		t.Fatal("work happened without an idempotency key")
	}
}

func TestInvalidIdempotencyKeyFormatIs400(t *testing.T) {
	e := newEnv(t)
	resp, body := e.do(t, "POST", "/v1/dlq/12/replay", e.token(t, "admin-user", auth.RoleAdmin), nil, map[string]string{"Idempotency-Key": "short"})
	if resp.StatusCode != 400 || errCode(t, body) != "idempotency_key_invalid" {
		t.Fatalf("%d %s", resp.StatusCode, body)
	}
}

func TestRepeatedKeyReturnsTheFirstResultWithoutCallingReplaySvcAgain(t *testing.T) {
	e := newEnv(t)
	e.rp.results = []*pb.ReplayResponse{applied("first"), {Status: pb.ReplayStatus_REPLAY_STATUS_ALREADY_APPLIED, Detail: "second"}}
	tok := e.token(t, "admin-user", auth.RoleAdmin)
	h := map[string]string{"Idempotency-Key": "key-aaaa-0001"}
	r1, b1 := e.do(t, "POST", "/v1/dlq/12/replay", tok, nil, h)
	r2, b2 := e.do(t, "POST", "/v1/dlq/12/replay", tok, nil, h)
	if r1.StatusCode != 200 || r2.StatusCode != 200 || string(b1) != string(b2) {
		t.Fatalf("first=%d %s second=%d %s", r1.StatusCode, b1, r2.StatusCode, b2)
	}
	if r1.Header.Get("Idempotent-Replayed") != "" || r2.Header.Get("Idempotent-Replayed") != "true" {
		t.Fatalf("replay headers: %q / %q", r1.Header.Get("Idempotent-Replayed"), r2.Header.Get("Idempotent-Replayed"))
	}
	if e.rp.calls != 1 {
		t.Fatalf("replay-svc called %d times for one key, want 1", e.rp.calls)
	}
	if e.st.auditCount("dlq.replay") != 1 {
		t.Fatalf("audit rows = %d, want 1 (the repeat is not a new action)", e.st.auditCount("dlq.replay"))
	}
}

func TestSameKeyForADifferentRecordIs422(t *testing.T) {
	e := newEnv(t)
	tok := e.token(t, "admin-user", auth.RoleAdmin)
	h := map[string]string{"Idempotency-Key": "key-aaaa-0002"}
	e.do(t, "POST", "/v1/dlq/12/replay", tok, nil, h)
	resp, body := e.do(t, "POST", "/v1/dlq/13/replay", tok, nil, h)
	if resp.StatusCode != 422 || errCode(t, body) != "idempotency_key_reused" {
		t.Fatalf("%d %s", resp.StatusCode, body)
	}
}

func TestReplayTwiceWithDifferentKeysGivesAppliedThenAlreadyAppliedAndTwoAuditRows(t *testing.T) {
	e := newEnv(t)
	e.rp.results = []*pb.ReplayResponse{applied("first"), {Status: pb.ReplayStatus_REPLAY_STATUS_ALREADY_APPLIED, Detail: "second"}}
	tok := e.token(t, "admin-user", auth.RoleAdmin)
	_, b1 := e.do(t, "POST", "/v1/dlq/12/replay", tok, nil, map[string]string{"Idempotency-Key": "key-aaaa-0003"})
	_, b2 := e.do(t, "POST", "/v1/dlq/12/replay", tok, nil, map[string]string{"Idempotency-Key": "key-aaaa-0004"})
	var r1, r2 struct{ Status string }
	_ = json.Unmarshal(b1, &r1)
	_ = json.Unmarshal(b2, &r2)
	if r1.Status != "APPLIED" || r2.Status != "ALREADY_APPLIED" {
		t.Fatalf("statuses %s / %s", r1.Status, r2.Status)
	}
	if e.st.auditCount("dlq.replay") != 2 {
		t.Fatalf("audit rows = %d, want 2", e.st.auditCount("dlq.replay"))
	}
	a := e.st.audit[0]
	if a.Actor != "admin-user" || a.Role != "admin" || a.Target != "dlq:12" || a.Result != "APPLIED" || a.IdempotencyKey != "key-aaaa-0003" || a.RequestedAt.IsZero() {
		t.Fatalf("audit row = %+v", a)
	}
}

func TestPendingReplayIsA202AndTheKeyCanBeRetriedToCompletion(t *testing.T) {
	e := newEnv(t)
	e.rp.results = []*pb.ReplayResponse{{Status: pb.ReplayStatus_REPLAY_STATUS_PENDING, Detail: "stale cache"}, applied("done")}
	tok := e.token(t, "admin-user", auth.RoleAdmin)
	h := map[string]string{"Idempotency-Key": "key-aaaa-0005"}
	r1, _ := e.do(t, "POST", "/v1/dlq/12/replay", tok, nil, h)
	r2, b2 := e.do(t, "POST", "/v1/dlq/12/replay", tok, nil, h)
	if r1.StatusCode != 202 || r2.StatusCode != 200 || !strings.Contains(string(b2), "APPLIED") {
		t.Fatalf("%d then %d %s", r1.StatusCode, r2.StatusCode, b2)
	}
	if e.rp.calls != 2 {
		t.Fatalf("calls = %d, want 2 (a pending result must not be cached)", e.rp.calls)
	}
}

func TestStillInvalidIs422WithTheValidatorReason(t *testing.T) {
	e := newEnv(t)
	e.rp.results = []*pb.ReplayResponse{{Status: pb.ReplayStatus_REPLAY_STATUS_STILL_INVALID, Detail: "validator still rejects the record: UNKNOWN_MERCHANT"}}
	resp, body := e.do(t, "POST", "/v1/dlq/12/replay", e.token(t, "admin-user", auth.RoleAdmin), nil, map[string]string{"Idempotency-Key": "key-aaaa-0006"})
	if resp.StatusCode != 422 || !strings.Contains(string(body), "UNKNOWN_MERCHANT") {
		t.Fatalf("%d %s", resp.StatusCode, body)
	}
}

func TestReplayServiceDownIs503AuditedAndKeyReleased(t *testing.T) {
	e := newEnv(t)
	e.rp.err = status.Error(codes.Unavailable, "connection refused")
	tok := e.token(t, "admin-user", auth.RoleAdmin)
	h := map[string]string{"Idempotency-Key": "key-aaaa-0007"}
	resp, body := e.do(t, "POST", "/v1/dlq/12/replay", tok, nil, h)
	if resp.StatusCode != 503 || errCode(t, body) != "replay_unavailable" || strings.Contains(string(body), "connection refused") {
		t.Fatalf("%d %s (must map to 503 without leaking the upstream message)", resp.StatusCode, body)
	}
	if e.st.audit[0].Result != "error" || e.st.audit[0].Error == "" {
		t.Fatalf("audit = %+v", e.st.audit[0])
	}
	e.rp.err = nil
	if resp, _ := e.do(t, "POST", "/v1/dlq/12/replay", tok, nil, h); resp.StatusCode != 200 {
		t.Fatalf("retry with the same key after a 503 got %d", resp.StatusCode)
	}
}

func TestBadDLQIdIs400(t *testing.T) {
	e := newEnv(t)
	tok := e.token(t, "admin-user", auth.RoleAdmin)
	if resp, _ := e.do(t, "POST", "/v1/dlq/abc/replay", tok, nil, map[string]string{"Idempotency-Key": "key-aaaa-0008"}); resp.StatusCode != 400 {
		t.Fatalf("status %d", resp.StatusCode)
	}
	if resp, _ := e.do(t, "GET", "/v1/dlq/1;drop", tok, nil, nil); resp.StatusCode != 400 {
		t.Fatalf("status %d", resp.StatusCode)
	}
}

func TestMapGRPCErrorAndReplayStatusTables(t *testing.T) {
	for code, want := range map[codes.Code]int{codes.Unavailable: 503, codes.DeadlineExceeded: 504, codes.InvalidArgument: 400, codes.NotFound: 404, codes.Internal: 502, codes.PermissionDenied: 502} {
		if got, _, _ := MapGRPCError(status.Error(code, "x")); got != want {
			t.Errorf("%v -> %d, want %d", code, got, want)
		}
	}
	for st, want := range map[pb.ReplayStatus]int{
		pb.ReplayStatus_REPLAY_STATUS_APPLIED: 200, pb.ReplayStatus_REPLAY_STATUS_ALREADY_APPLIED: 200, pb.ReplayStatus_REPLAY_STATUS_PENDING: 202,
		pb.ReplayStatus_REPLAY_STATUS_STILL_INVALID: 422, pb.ReplayStatus_REPLAY_STATUS_NOT_FOUND: 404, pb.ReplayStatus_REPLAY_STATUS_UNSPECIFIED: 502} {
		if got := ReplayHTTPStatus(st); got != want {
			t.Errorf("%v -> %d, want %d", st, got, want)
		}
	}
}

// ---- generator -------------------------------------------------------------------------------

func TestGeneratorRateAboveMaxIs422AndNothingStarts(t *testing.T) {
	e := newEnv(t)
	tok := e.token(t, "admin-user", auth.RoleAdmin)
	for _, rate := range []int{201, 100000, 0, -5} {
		resp, body := e.do(t, "POST", "/v1/generator", tok, map[string]any{"action": "start", "rate": rate}, map[string]string{"Idempotency-Key": fmt.Sprintf("key-gen-%05d", rate+10)})
		if resp.StatusCode != 422 || errCode(t, body) != "rate_out_of_range" {
			t.Errorf("rate %d: %d %s", rate, resp.StatusCode, body)
		}
	}
	if len(e.gen.Starts) != 0 {
		t.Fatalf("generator started with a refused rate: %v", e.gen.Starts)
	}
}

func TestGeneratorStartStopAuditedAndIdempotent(t *testing.T) {
	e := newEnv(t)
	tok := e.token(t, "admin-user", auth.RoleAdmin)
	h := map[string]string{"Idempotency-Key": "key-gen-start1"}
	r1, _ := e.do(t, "POST", "/v1/generator", tok, map[string]any{"action": "start", "rate": 50}, h)
	r2, _ := e.do(t, "POST", "/v1/generator", tok, map[string]any{"action": "start", "rate": 50}, h) // same key: first result, not a 409
	if r1.StatusCode != 200 || r2.StatusCode != 200 || len(e.gen.Starts) != 1 {
		t.Fatalf("%d %d starts=%v", r1.StatusCode, r2.StatusCode, e.gen.Starts)
	}
	if resp, body := e.do(t, "POST", "/v1/generator", tok, map[string]any{"action": "start", "rate": 20}, map[string]string{"Idempotency-Key": "key-gen-start2"}); resp.StatusCode != 409 {
		t.Fatalf("second start: %d %s", resp.StatusCode, body)
	}
	if resp, _ := e.do(t, "POST", "/v1/generator", tok, map[string]any{"action": "stop"}, map[string]string{"Idempotency-Key": "key-gen-stop01"}); resp.StatusCode != 200 {
		t.Fatalf("stop: %d", resp.StatusCode)
	}
	if e.st.auditCount("generator.start") != 2 || e.st.auditCount("generator.stop") != 1 {
		t.Fatalf("audit start=%d stop=%d (every admin action, including refused ones, is audited once)", e.st.auditCount("generator.start"), e.st.auditCount("generator.stop"))
	}
}

func TestGeneratorRequiresKeyAndValidAction(t *testing.T) {
	e := newEnv(t)
	tok := e.token(t, "admin-user", auth.RoleAdmin)
	if resp, _ := e.do(t, "POST", "/v1/generator", tok, map[string]any{"action": "start", "rate": 10}, nil); resp.StatusCode != 400 {
		t.Fatalf("no key: %d", resp.StatusCode)
	}
	if resp, _ := e.do(t, "POST", "/v1/generator", tok, map[string]any{"action": "explode"}, map[string]string{"Idempotency-Key": "key-gen-bad001"}); resp.StatusCode != 400 {
		t.Fatalf("bad action: %d", resp.StatusCode)
	}
	if resp, _ := e.do(t, "POST", "/v1/generator", tok, map[string]any{"action": "start", "rate": 10, "extra": 1}, map[string]string{"Idempotency-Key": "key-gen-bad002"}); resp.StatusCode != 400 {
		t.Fatalf("unknown field: %d", resp.StatusCode)
	}
}

// ---- alerts, DLQ, devices, audit -------------------------------------------------------------

func TestAckAlertIsAuditedAndStateIsEnforced(t *testing.T) {
	e := newEnv(t)
	now := time.Now().UTC()
	a, _ := e.st.Create(context.Background(), alerts.Alert{Rule: "lag", Severity: "critical", State: alerts.StateFiring, Summary: "x", FiredAt: now, UpdatedAt: now})
	tok := e.token(t, "viewer-user", auth.RoleViewer) // acknowledging is a viewer action
	resp, body := e.do(t, "POST", fmt.Sprintf("/v1/alerts/%d/ack", a.ID), tok, nil, nil)
	if resp.StatusCode != 200 || !strings.Contains(string(body), `"acknowledged"`) || !strings.Contains(string(body), "viewer-user") {
		t.Fatalf("%d %s", resp.StatusCode, body)
	}
	if e.st.auditCount("alert.ack") != 1 || e.st.audit[0].Actor != "viewer-user" {
		t.Fatalf("audit = %+v", e.st.audit)
	}
	if resp, _ := e.do(t, "POST", fmt.Sprintf("/v1/alerts/%d/ack", a.ID), tok, nil, nil); resp.StatusCode != 409 {
		t.Fatalf("second ack: %d", resp.StatusCode)
	}
	if resp, _ := e.do(t, "POST", "/v1/alerts/999/ack", tok, nil, nil); resp.StatusCode != 404 {
		t.Fatalf("unknown alert: %d", resp.StatusCode)
	}
	if resp, _ := e.do(t, "POST", "/v1/alerts/abc/ack", tok, nil, nil); resp.StatusCode != 400 {
		t.Fatalf("bad id: %d", resp.StatusCode)
	}
}

func TestAlertListFilterAndValidation(t *testing.T) {
	e := newEnv(t)
	tok := e.token(t, "viewer-user", auth.RoleViewer)
	now := time.Now().UTC()
	_, _ = e.st.Create(context.Background(), alerts.Alert{Rule: "a", State: alerts.StateFiring, FiredAt: now})
	_, _ = e.st.Create(context.Background(), alerts.Alert{Rule: "b", State: alerts.StateResolved, FiredAt: now})
	_, body := e.do(t, "GET", "/v1/alerts?state=firing", tok, nil, nil)
	var out struct{ Alerts []alerts.Alert }
	_ = json.Unmarshal(body, &out)
	if len(out.Alerts) != 1 || out.Alerts[0].Rule != "a" {
		t.Fatalf("%s", body)
	}
	if resp, _ := e.do(t, "GET", "/v1/alerts?state=bogus", tok, nil, nil); resp.StatusCode != 400 {
		t.Fatalf("bogus state: %d", resp.StatusCode)
	}
}

func TestDLQListDetailAndNotFound(t *testing.T) {
	e := newEnv(t)
	tok := e.token(t, "viewer-user", auth.RoleViewer)
	resp, body := e.do(t, "GET", "/v1/dlq?reason=unknown_merchant&limit=5", tok, nil, nil)
	if resp.StatusCode != 200 || !strings.Contains(string(body), "UNKNOWN_MERCHANT") || !strings.Contains(string(body), `"open_total":1`) {
		t.Fatalf("%d %s", resp.StatusCode, body)
	}
	if resp, body := e.do(t, "GET", "/v1/dlq/7", tok, nil, nil); resp.StatusCode != 200 || !strings.Contains(string(body), "payload_json") {
		t.Fatalf("detail %d %s", resp.StatusCode, body)
	}
	if resp, _ := e.do(t, "GET", "/v1/dlq/404", tok, nil, nil); resp.StatusCode != 404 {
		t.Fatalf("missing: %d", resp.StatusCode)
	}
	if resp, _ := e.do(t, "GET", "/v1/dlq?reason=drop%20table", tok, nil, nil); resp.StatusCode != 400 {
		t.Fatalf("bad reason: %d", resp.StatusCode)
	}
	if resp, _ := e.do(t, "GET", "/v1/dlq?cursor=abc", tok, nil, nil); resp.StatusCode != 400 {
		t.Fatalf("bad cursor: %d", resp.StatusCode)
	}
}

func TestRegisterDeviceValidatesAndStores(t *testing.T) {
	e := newEnv(t)
	tok := e.token(t, "viewer-user", auth.RoleViewer)
	if resp, _ := e.do(t, "POST", "/v1/devices", tok, map[string]any{"expo_push_token": "not-a-token"}, nil); resp.StatusCode != 422 {
		t.Fatalf("invalid token: %d", resp.StatusCode)
	}
	if resp, _ := e.do(t, "POST", "/v1/devices", tok, map[string]any{"expo_push_token": "ExponentPushToken[abcdefghij123]", "platform": "android"}, nil); resp.StatusCode != 200 {
		t.Fatalf("valid token: %d", resp.StatusCode)
	}
	if e.st.devices["ExponentPushToken[abcdefghij123]"] != "viewer-user" {
		t.Fatalf("devices = %v", e.st.devices)
	}
}

func TestUnregisterDeviceOwnerOnlyUnlessAdmin(t *testing.T) {
	e := newEnv(t)
	viewer := e.token(t, "viewer-user", auth.RoleViewer)
	other := e.token(t, "someone-else", auth.RoleViewer)
	admin := e.token(t, "admin-user", auth.RoleAdmin)
	tok := "ExponentPushToken[abcdefghij123]"
	e.do(t, "POST", "/v1/devices", viewer, map[string]any{"expo_push_token": tok}, nil)
	if resp, _ := e.do(t, "DELETE", "/v1/devices", other, map[string]any{"expo_push_token": tok}, nil); resp.StatusCode != 404 {
		t.Fatalf("another viewer removed someone's device: %d", resp.StatusCode)
	}
	if resp, _ := e.do(t, "DELETE", "/v1/devices", "", map[string]any{"expo_push_token": tok}, nil); resp.StatusCode != 401 {
		t.Fatalf("no token: %d", resp.StatusCode)
	}
	if resp, _ := e.do(t, "DELETE", "/v1/devices", viewer, map[string]any{"expo_push_token": tok}, nil); resp.StatusCode != 200 {
		t.Fatalf("owner: %d", resp.StatusCode)
	}
	e.do(t, "POST", "/v1/devices", viewer, map[string]any{"expo_push_token": tok}, nil)
	if resp, _ := e.do(t, "DELETE", "/v1/devices", admin, map[string]any{"expo_push_token": tok}, nil); resp.StatusCode != 200 {
		t.Fatalf("admin: %d", resp.StatusCode)
	}
}

func TestSnapshotEndpointAndMe(t *testing.T) {
	e := newEnv(t)
	tok := e.token(t, "admin-user", auth.RoleAdmin)
	resp, body := e.do(t, "GET", "/v1/snapshot", tok, nil, nil)
	var s snapshot.Snapshot
	if resp.StatusCode != 200 || json.Unmarshal(body, &s) != nil || s.Seq != 7 || s.LagTotal != 3 {
		t.Fatalf("%d %s", resp.StatusCode, body)
	}
	if _, body := e.do(t, "GET", "/v1/me", tok, nil, nil); !strings.Contains(string(body), `"admin"`) {
		t.Fatalf("me = %s", body)
	}
}

func TestErrorsAreJSONWithCodeAndMessage(t *testing.T) {
	e := newEnv(t)
	resp, body := e.do(t, "GET", "/v1/snapshot", "", nil, nil)
	var out map[string]map[string]string
	if err := json.Unmarshal(body, &out); err != nil || out["error"]["code"] == "" || out["error"]["message"] == "" || resp.Header.Get("Content-Type") != "application/json" {
		t.Fatalf("%s (%s)", body, resp.Header.Get("Content-Type"))
	}
}

func TestHealthAndReadyArePublic(t *testing.T) {
	e := newEnv(t)
	for _, p := range []string{"/healthz", "/readyz"} {
		if resp, _ := e.do(t, "GET", p, "", nil, nil); resp.StatusCode != 200 {
			t.Errorf("%s: %d", p, resp.StatusCode)
		}
	}
}

func TestMetricsAreNotServedOnThePublicRouter(t *testing.T) {
	e := newEnv(t)
	if resp, _ := e.do(t, "GET", "/metrics", "", nil, nil); resp.StatusCode != 404 {
		t.Fatalf("/metrics on the public API returned %d, want 404 (it is served on a separate loopback port)", resp.StatusCode)
	}
}

func TestAccessLogNeverContainsTheQueryStringToken(t *testing.T) {
	var buf bytes.Buffer
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&buf, nil)))
	defer slog.SetDefault(old)
	e := newEnv(t)
	tok := e.token(t, "viewer-user", auth.RoleViewer)
	// plain HTTP GET to the websocket route with a query token: the upgrade fails but the request is logged
	e.do(t, "GET", "/v1/live?access_token="+tok, "", nil, nil)
	e.do(t, "GET", "/v1/snapshot", tok, nil, nil)
	if strings.Contains(buf.String(), tok) || strings.Contains(buf.String(), "access_token") {
		t.Fatalf("a token leaked into the log: %s", buf.String())
	}
	if !strings.Contains(buf.String(), `"route":"/v1/snapshot"`) {
		t.Fatalf("expected a structured access log line, got %s", buf.String())
	}
}

func TestQueryTokenIsOnlyAcceptedOnTheWebSocketRoute(t *testing.T) {
	e := newEnv(t)
	tok := e.token(t, "viewer-user", auth.RoleViewer)
	if resp, _ := e.do(t, "GET", "/v1/snapshot?access_token="+tok, "", nil, nil); resp.StatusCode != 401 {
		t.Fatalf("REST route accepted a query token: %d", resp.StatusCode)
	}
	if resp, _ := e.do(t, "GET", "/v1/live?access_token="+tok, "", nil, nil); resp.StatusCode == 401 {
		t.Fatal("websocket route should accept the query token")
	}
}

func TestCORSPreflightForTheWebBuildOrigin(t *testing.T) {
	e := newEnv(t)
	req, _ := http.NewRequest("OPTIONS", e.srv.URL+"/v1/dlq/1/replay", nil)
	req.Header.Set("Origin", "http://127.0.0.1:8081")
	req.Header.Set("Access-Control-Request-Method", "POST")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 204 || resp.Header.Get("Access-Control-Allow-Origin") != "http://127.0.0.1:8081" ||
		!strings.Contains(resp.Header.Get("Access-Control-Allow-Headers"), "Idempotency-Key") {
		t.Fatalf("%d %v", resp.StatusCode, resp.Header)
	}
	req.Header.Set("Origin", "https://evil.example")
	resp, _ = http.DefaultClient.Do(req)
	if resp.Header.Get("Access-Control-Allow-Origin") != "" {
		t.Fatal("an unknown origin must not be allowed")
	}
}

var _ = errors.New
