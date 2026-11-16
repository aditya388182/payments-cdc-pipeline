// Package alerts turns snapshots into alerts: thresholds with durations, dedup, acknowledge, resolve, and one
// push per state change.
package alerts

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/aditya388182/payments-cdc-pipeline/oncall/api/internal/config"
	"github.com/aditya388182/payments-cdc-pipeline/oncall/api/internal/snapshot"
)

const (
	StateFiring       = "firing"
	StateAcknowledged = "acknowledged"
	StateResolved     = "resolved"
)

var (
	ErrNotFound     = errors.New("alert not found")
	ErrNotAckable   = errors.New("alert is not firing")
	ErrInvalidState = errors.New("invalid alert state")
)

type Alert struct {
	ID         int64          `json:"id"`
	Rule       string         `json:"rule"`
	Severity   string         `json:"severity"`
	State      string         `json:"state"`
	Summary    string         `json:"summary"`
	Details    map[string]any `json:"details,omitempty"`
	FiredAt    time.Time      `json:"fired_at"`
	AckedAt    *time.Time     `json:"acked_at,omitempty"`
	AckedBy    string         `json:"acked_by,omitempty"`
	ResolvedAt *time.Time     `json:"resolved_at,omitempty"`
	UpdatedAt  time.Time      `json:"updated_at"`
}

// Store persists alerts (Postgres in production, in memory in tests).
type Store interface {
	Active(ctx context.Context) ([]Alert, error) // firing or acknowledged
	Create(ctx context.Context, a Alert) (Alert, error)
	Update(ctx context.Context, a Alert) error
	Get(ctx context.Context, id int64) (Alert, error)
	List(ctx context.Context, states []string, limit int) ([]Alert, error)
}

// Notification is what a push sender delivers.
type Notification struct {
	Tokens []string
	Title  string
	Body   string
	Data   map[string]string
}

type Sender interface {
	Send(ctx context.Context, n Notification) error
}

// Devices lists registered push tokens.
type Devices interface {
	Tokens(ctx context.Context) ([]string, error)
}

// Broadcaster publishes an alert state change on the WebSocket hub.
type Broadcaster interface {
	PublishAlert(event string, alert any) uint64
}

// Rule is a condition on a snapshot that must hold continuously for For before the alert fires.
type Rule struct {
	Name     string
	Severity string
	For      time.Duration
	Eval     func(s snapshot.Snapshot) (active bool, summary string)
}

// DefaultRules builds the rule set from configuration.
func DefaultRules(c config.Config) []Rule {
	return []Rule{
		{Name: "consumer_lag_high", Severity: "critical", For: c.LagFor, Eval: func(s snapshot.Snapshot) (bool, string) {
			return s.LagTotal > c.LagThreshold, fmt.Sprintf("Consumer lag %d is above %d", s.LagTotal, c.LagThreshold)
		}},
		{Name: "connector_not_running", Severity: "critical", For: c.ConnectorFor, Eval: func(s snapshot.Snapshot) (bool, string) {
			return s.Connector.State != "" && s.Connector.State != "RUNNING", fmt.Sprintf("Debezium connector is %s", s.Connector.State)
		}},
		{Name: "freshness_high", Severity: "warning", For: c.FreshnessFor, Eval: func(s snapshot.Snapshot) (bool, string) {
			// only meaningful while records are waiting: a quiet source is not a stale lake
			return !s.CaughtUp && s.FreshnessSec > c.FreshnessThreshold.Seconds(),
				fmt.Sprintf("Lake is %.0fs behind the source (threshold %.0fs)", s.FreshnessSec, c.FreshnessThreshold.Seconds())
		}},
		{Name: "dlq_growth", Severity: "warning", For: 0, Eval: func(s snapshot.Snapshot) (bool, string) {
			return s.DLQ.GrowthPerMin >= c.DLQGrowthThreshold && c.DLQGrowthThreshold > 0,
				fmt.Sprintf("%d new dead-lettered records in the last minute (threshold %d)", s.DLQ.GrowthPerMin, c.DLQGrowthThreshold)
		}},
		{Name: "parity_mismatch", Severity: "critical", For: 0, Eval: func(s snapshot.Snapshot) (bool, string) {
			return s.Parity.State == snapshot.Mismatch, fmt.Sprintf("Drained parity check: source and lake differ by %d rows", s.Parity.Diff)
		}},
	}
}

type Engine struct {
	rules   []Rule
	store   Store
	devices Devices
	sender  Sender
	bcast   Broadcaster
	onCount func(n int)
	onPush  func(result string)

	mu      sync.Mutex
	since   map[string]time.Time // rule -> when its condition first became true (not yet fired)
	active  map[string]Alert     // rule -> firing/acknowledged alert
	loaded  bool
	nowFunc func() time.Time
}

type Option func(*Engine)

func WithClock(f func() time.Time) Option { return func(e *Engine) { e.nowFunc = f } }
func WithCounters(active func(int), push func(string)) Option {
	return func(e *Engine) { e.onCount, e.onPush = active, push }
}

func NewEngine(rules []Rule, st Store, dev Devices, snd Sender, b Broadcaster, opts ...Option) *Engine {
	e := &Engine{rules: rules, store: st, devices: dev, sender: snd, bcast: b,
		since: map[string]time.Time{}, active: map[string]Alert{}, nowFunc: time.Now}
	for _, o := range opts {
		o(e)
	}
	return e
}

func (e *Engine) load(ctx context.Context) error {
	if e.loaded {
		return nil
	}
	act, err := e.store.Active(ctx)
	if err != nil {
		return err
	}
	for _, a := range act {
		e.active[a.Rule] = a
	}
	e.loaded = true
	return nil
}

// Evaluate applies every rule to the snapshot. Call it once per snapshot.
func (e *Engine) Evaluate(ctx context.Context, s snapshot.Snapshot) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if err := e.load(ctx); err != nil {
		slog.Error("alerts: loading active alerts failed", "err", err.Error())
		return
	}
	now := e.nowFunc()
	for _, r := range e.rules {
		on, summary := r.Eval(s)
		cur, isActive := e.active[r.Name]
		switch {
		case on && !isActive:
			first, seen := e.since[r.Name]
			if !seen {
				first = now
				e.since[r.Name] = now
			}
			if now.Sub(first) >= r.For {
				e.fire(ctx, r, summary, s, now)
			}
		case on && isActive:
			delete(e.since, r.Name) // already alerting: dedup, no new alert and no new push
			_ = cur
		case !on:
			delete(e.since, r.Name)
			if isActive {
				e.resolve(ctx, cur, now)
			}
		}
	}
	if e.onCount != nil {
		e.onCount(len(e.active))
	}
}

func (e *Engine) fire(ctx context.Context, r Rule, summary string, s snapshot.Snapshot, now time.Time) {
	a, err := e.store.Create(ctx, Alert{
		Rule: r.Name, Severity: r.Severity, State: StateFiring, Summary: summary, FiredAt: now, UpdatedAt: now,
		Details: map[string]any{"lag_total": s.LagTotal, "freshness_seconds": s.FreshnessSec, "connector": s.Connector.State,
			"parity": s.Parity.State, "dlq_growth_per_min": s.DLQ.GrowthPerMin},
	})
	if err != nil {
		slog.Error("alerts: creating alert failed", "rule", r.Name, "err", err.Error())
		return
	}
	delete(e.since, r.Name)
	e.active[r.Name] = a
	e.announce(ctx, StateFiring, a)
}

func (e *Engine) resolve(ctx context.Context, a Alert, now time.Time) {
	a.State = StateResolved
	a.ResolvedAt = &now
	a.UpdatedAt = now
	if err := e.store.Update(ctx, a); err != nil {
		slog.Error("alerts: resolving alert failed", "id", a.ID, "err", err.Error())
		return
	}
	delete(e.active, a.Rule)
	e.announce(ctx, StateResolved, a)
}

// Ack marks a firing alert acknowledged by `by`. Only a firing alert can be acknowledged.
func (e *Engine) Ack(ctx context.Context, id int64, by string) (Alert, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if err := e.load(ctx); err != nil {
		return Alert{}, err
	}
	a, err := e.store.Get(ctx, id)
	if err != nil {
		return Alert{}, err
	}
	if a.State != StateFiring {
		return a, ErrNotAckable
	}
	now := e.nowFunc()
	a.State = StateAcknowledged
	a.AckedAt = &now
	a.AckedBy = by
	a.UpdatedAt = now
	if err := e.store.Update(ctx, a); err != nil {
		return Alert{}, err
	}
	e.active[a.Rule] = a
	e.announce(ctx, StateAcknowledged, a)
	return a, nil
}

// announce is the single place a state change leaves the engine: WebSocket broadcast plus exactly one push.
func (e *Engine) announce(ctx context.Context, event string, a Alert) {
	if e.bcast != nil {
		e.bcast.PublishAlert(event, a)
	}
	if e.sender == nil || e.devices == nil {
		return
	}
	tokens, err := e.devices.Tokens(ctx)
	if err != nil || len(tokens) == 0 {
		if err != nil {
			slog.Warn("alerts: listing devices failed", "err", err.Error())
		}
		return
	}
	title := map[string]string{StateFiring: "ALERT: ", StateAcknowledged: "Acknowledged: ", StateResolved: "Resolved: "}[event] + a.Rule
	pctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	err = e.sender.Send(pctx, Notification{Tokens: tokens, Title: title, Body: a.Summary,
		Data: map[string]string{"alert_id": fmt.Sprint(a.ID), "rule": a.Rule, "state": event}})
	if e.onPush != nil {
		if err != nil {
			e.onPush("error")
		} else {
			e.onPush("ok")
		}
	}
	if err != nil {
		slog.Warn("alerts: push failed", "alert", a.ID, "err", err.Error())
	}
}
