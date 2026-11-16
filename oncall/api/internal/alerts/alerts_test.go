package alerts

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/aditya388182/payments-cdc-pipeline/oncall/api/internal/snapshot"
)

type fakeSender struct {
	mu   sync.Mutex
	sent []Notification
}

func (f *fakeSender) Send(_ context.Context, n Notification) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sent = append(f.sent, n)
	return nil
}

type fakeDevices struct{ tokens []string }

func (d fakeDevices) Tokens(context.Context) ([]string, error) { return d.tokens, nil }

type fakeBcast struct {
	mu     sync.Mutex
	events []string
}

func (b *fakeBcast) PublishAlert(event string, _ any) uint64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.events = append(b.events, event)
	return uint64(len(b.events))
}

type clock struct{ t time.Time }

func (c *clock) now() time.Time          { return c.t }
func (c *clock) advance(d time.Duration) { c.t = c.t.Add(d) }

func lagRule() Rule {
	return Rule{Name: "lag", Severity: "critical", For: 30 * time.Second, Eval: func(s snapshot.Snapshot) (bool, string) {
		return s.LagTotal > 100, "lag high"
	}}
}

type env struct {
	e   *Engine
	st  *MemStore
	snd *fakeSender
	b   *fakeBcast
	c   *clock
}

func newEnv(rules ...Rule) env {
	st, snd, b, c := NewMemStore(), &fakeSender{}, &fakeBcast{}, &clock{t: time.Date(2026, 11, 16, 12, 0, 0, 0, time.UTC)}
	return env{NewEngine(rules, st, fakeDevices{[]string{"ExponentPushToken[abc]"}}, snd, b, WithClock(c.now)), st, snd, b, c}
}

func lag(n int64) snapshot.Snapshot { return snapshot.Snapshot{LagTotal: n} }

func TestThresholdMustHoldForTheDurationBeforeFiring(t *testing.T) {
	v := newEnv(lagRule())
	ctx := context.Background()
	v.e.Evaluate(ctx, lag(500))
	v.c.advance(29 * time.Second)
	v.e.Evaluate(ctx, lag(500))
	if len(v.snd.sent) != 0 {
		t.Fatal("fired before the 30s duration elapsed")
	}
	v.c.advance(2 * time.Second)
	v.e.Evaluate(ctx, lag(500))
	if len(v.snd.sent) != 1 || v.b.events[0] != StateFiring {
		t.Fatalf("expected one firing push, got %d (%v)", len(v.snd.sent), v.b.events)
	}
}

func TestADipBelowTheThresholdResetsTheTimer(t *testing.T) {
	v := newEnv(lagRule())
	ctx := context.Background()
	v.e.Evaluate(ctx, lag(500))
	v.c.advance(20 * time.Second)
	v.e.Evaluate(ctx, lag(5)) // recovered briefly
	v.c.advance(20 * time.Second)
	v.e.Evaluate(ctx, lag(500)) // timer restarts here
	v.c.advance(20 * time.Second)
	v.e.Evaluate(ctx, lag(500))
	if len(v.snd.sent) != 0 {
		t.Fatal("fired although the condition never held for 30s in a row")
	}
}

func TestDedupWhileFiringNoSecondAlertNoSecondPush(t *testing.T) {
	v := newEnv(lagRule())
	ctx := context.Background()
	v.e.Evaluate(ctx, lag(500))
	v.c.advance(31 * time.Second)
	for i := 0; i < 10; i++ {
		v.e.Evaluate(ctx, lag(900))
		v.c.advance(2 * time.Second)
	}
	all, _ := v.st.List(ctx, nil, 0)
	if len(all) != 1 || len(v.snd.sent) != 1 {
		t.Fatalf("alerts=%d pushes=%d, want 1 and 1", len(all), len(v.snd.sent))
	}
}

func TestAckThenResolveEachPushOnceAndStateMachineIsOrdered(t *testing.T) {
	v := newEnv(lagRule())
	ctx := context.Background()
	v.e.Evaluate(ctx, lag(500))
	v.c.advance(31 * time.Second)
	v.e.Evaluate(ctx, lag(500))
	a, err := v.e.Ack(ctx, 1, "admin-user")
	if err != nil || a.State != StateAcknowledged || a.AckedBy != "admin-user" || a.AckedAt == nil {
		t.Fatalf("ack = %+v err=%v", a, err)
	}
	if _, err := v.e.Ack(ctx, 1, "admin-user"); err != ErrNotAckable {
		t.Fatalf("second ack err = %v, want ErrNotAckable", err)
	}
	v.e.Evaluate(ctx, lag(500)) // still bad while acknowledged: no new alert, no push
	v.c.advance(2 * time.Second)
	v.e.Evaluate(ctx, lag(0)) // recovered
	got, _ := v.st.Get(ctx, 1)
	if got.State != StateResolved || got.ResolvedAt == nil {
		t.Fatalf("final = %+v", got)
	}
	want := []string{StateFiring, StateAcknowledged, StateResolved}
	if len(v.b.events) != 3 || len(v.snd.sent) != 3 {
		t.Fatalf("events=%v pushes=%d, want exactly one per state change", v.b.events, len(v.snd.sent))
	}
	for i, w := range want {
		if v.b.events[i] != w || v.snd.sent[i].Data["state"] != w {
			t.Fatalf("event %d = %s / push %s, want %s", i, v.b.events[i], v.snd.sent[i].Data["state"], w)
		}
	}
}

func TestAfterResolveAProblemAgainCreatesANewAlert(t *testing.T) {
	v := newEnv(lagRule())
	ctx := context.Background()
	for round := 0; round < 2; round++ {
		v.e.Evaluate(ctx, lag(500))
		v.c.advance(31 * time.Second)
		v.e.Evaluate(ctx, lag(500))
		v.e.Evaluate(ctx, lag(0))
		v.c.advance(time.Second)
	}
	all, _ := v.st.List(ctx, nil, 0)
	if len(all) != 2 || all[0].State != StateResolved || all[1].State != StateResolved {
		t.Fatalf("alerts = %+v", all)
	}
}

func TestEngineRestartDoesNotDuplicateAnActiveAlert(t *testing.T) {
	v := newEnv(lagRule())
	ctx := context.Background()
	v.e.Evaluate(ctx, lag(500))
	v.c.advance(31 * time.Second)
	v.e.Evaluate(ctx, lag(500))
	// a new process with the same database
	e2 := NewEngine([]Rule{lagRule()}, v.st, fakeDevices{[]string{"t"}}, v.snd, v.b, WithClock(v.c.now))
	e2.Evaluate(ctx, lag(500))
	all, _ := v.st.List(ctx, nil, 0)
	if len(all) != 1 || len(v.snd.sent) != 1 {
		t.Fatalf("alerts=%d pushes=%d after restart", len(all), len(v.snd.sent))
	}
}

func TestAckOfUnknownAlert(t *testing.T) {
	v := newEnv(lagRule())
	if _, err := v.e.Ack(context.Background(), 99, "x"); err != ErrNotFound {
		t.Fatalf("err = %v", err)
	}
}

func TestDefaultRulesConnectorFreshnessDLQAndParity(t *testing.T) {
	cfg := configForTest()
	rules := map[string]Rule{}
	for _, r := range DefaultRules(cfg) {
		rules[r.Name] = r
	}
	if on, _ := rules["connector_not_running"].Eval(snapshot.Snapshot{Connector: snapshot.Connector{State: "FAILED"}}); !on {
		t.Error("connector FAILED should alert")
	}
	if on, _ := rules["connector_not_running"].Eval(snapshot.Snapshot{Connector: snapshot.Connector{State: "RUNNING"}}); on {
		t.Error("connector RUNNING should not alert")
	}
	if on, _ := rules["freshness_high"].Eval(snapshot.Snapshot{FreshnessSec: 999, CaughtUp: true}); on {
		t.Error("a caught-up (quiet) pipeline must not alert on freshness")
	}
	if on, _ := rules["freshness_high"].Eval(snapshot.Snapshot{FreshnessSec: 999}); !on {
		t.Error("stale and behind should alert")
	}
	if on, _ := rules["dlq_growth"].Eval(snapshot.Snapshot{DLQ: snapshot.DLQ{GrowthPerMin: 9}}); !on {
		t.Error("DLQ growth over the threshold should alert")
	}
	if on, _ := rules["parity_mismatch"].Eval(snapshot.Snapshot{Parity: snapshot.Parity{State: snapshot.CatchingUp}}); on {
		t.Error("catching up is not a mismatch")
	}
	if on, _ := rules["parity_mismatch"].Eval(snapshot.Snapshot{Parity: snapshot.Parity{State: snapshot.Mismatch}}); !on {
		t.Error("a drained mismatch should alert")
	}
}
