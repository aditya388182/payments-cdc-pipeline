// Package snapshot defines the live payload pushed to clients every interval and the pure functions behind it.
package snapshot

import (
	"math"
	"sort"
	"time"
)

type PartitionLag struct {
	Partition int32 `json:"partition"`
	End       int64 `json:"end"`
	Processed int64 `json:"processed"`
	Lag       int64 `json:"lag"`
}

type Connector struct {
	State string   `json:"state"` // RUNNING | FAILED | PAUSED | UNASSIGNED | UNREACHABLE
	Tasks []string `json:"tasks,omitempty"`
}

type Parity struct {
	State   string `json:"state"` // in_sync | catching_up | mismatch
	Diff    int64  `json:"diff"`  // source - lake - open quarantined rows
	Allowed int64  `json:"allowed"`
	Drained bool   `json:"drained"`
	Detail  string `json:"detail"`
}

type DLQ struct {
	Messages     int64 `json:"messages"`
	Open         int64 `json:"open"`
	GrowthPerMin int64 `json:"growth_per_min"`
}

type Batch struct {
	BatchID        int64   `json:"batch_id"`
	InputRows      int64   `json:"input_rows"`
	MergedRows     int64   `json:"merged_rows"`
	DLQRows        int64   `json:"dlq_rows"`
	DurationSec    float64 `json:"duration_seconds"`
	MaxSourceTS    *string `json:"max_source_ts"`
	CommittedAt    string  `json:"committed_at"`
	ObservedAtUnix int64   `json:"-"`
}

type SourceHealth struct {
	OK     bool    `json:"ok"`
	Error  string  `json:"error,omitempty"`
	AgeSec float64 `json:"age_seconds"`
}

type Snapshot struct {
	Seq             uint64                  `json:"seq"`
	TS              time.Time               `json:"ts"`
	EventsInPerSec  float64                 `json:"events_in_per_sec"`
	EventsOutPerSec float64                 `json:"events_out_per_sec"`
	LagTotal        int64                   `json:"lag_total"`
	LagPartitions   []PartitionLag          `json:"lag_partitions"`
	CaughtUp        bool                    `json:"caught_up"`
	FreshnessSec    float64                 `json:"freshness_seconds"`
	FreshnessP50    float64                 `json:"freshness_p50_seconds"`
	FreshnessP95    float64                 `json:"freshness_p95_seconds"`
	Connector       Connector               `json:"connector"`
	SourceCount     int64                   `json:"source_count"`
	LakeCount       int64                   `json:"lake_count"`
	Parity          Parity                  `json:"parity"`
	DLQ             DLQ                     `json:"dlq"`
	LastBatch       *Batch                  `json:"last_batch,omitempty"`
	Sources         map[string]SourceHealth `json:"sources"`
}

// Parity states.
const (
	InSync     = "in_sync"
	CatchingUp = "catching_up"
	Mismatch   = "mismatch"
	Unknown    = "unknown" // a source needed for the comparison is unavailable
)

type ParityInput struct {
	SourceCount     int64
	LakeCount       int64
	QuarantinedOpen int64   // source rows the validator rejected and nobody replayed yet: they are NOT in the lake
	RatePerSec      float64 // events per second entering the pipeline
	P95LagSec       float64 // p95 end-to-end freshness over the last minute
	Drained         bool    // the stream has nothing left to process
}

// ParityBadge implements decision 4: "in sync" when |source - lake - quarantined| <= rate x p95 lag x 1.5
// (the rows that are legitimately in flight); "catching up" above that; "mismatch" only after the stream is
// drained, because only then should the counts agree exactly.
func ParityBadge(in ParityInput) Parity {
	diff := in.SourceCount - in.LakeCount - in.QuarantinedOpen
	abs := diff
	if abs < 0 {
		abs = -abs
	}
	allowed := int64(math.Ceil(math.Max(in.RatePerSec, 0) * math.Max(in.P95LagSec, 0) * 1.5))
	p := Parity{Diff: diff, Allowed: allowed, Drained: in.Drained}
	switch {
	case in.Drained && abs != 0:
		p.State = Mismatch
		p.Detail = "stream drained but source and lake row counts still differ"
	case abs <= allowed:
		p.State = InSync
		p.Detail = "within the rows expected in flight"
	default:
		p.State = CatchingUp
		p.Detail = "difference is larger than the in-flight allowance; stream is still catching up"
	}
	return p
}

// Percentile of v (copy sorted), q in [0,1], nearest-rank. Returns 0 for no samples.
func Percentile(v []float64, q float64) float64 {
	if len(v) == 0 {
		return 0
	}
	s := append([]float64(nil), v...)
	sort.Float64s(s)
	idx := int(math.Ceil(q*float64(len(s)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(s) {
		idx = len(s) - 1
	}
	return s[idx]
}

// Window keeps timestamped samples for the last d and answers percentile queries.
type Window struct {
	d       time.Duration
	samples []sample
}
type sample struct {
	t time.Time
	v float64
}

func NewWindow(d time.Duration) *Window { return &Window{d: d} }

func (w *Window) Add(t time.Time, v float64) {
	w.samples = append(w.samples, sample{t, v})
	cut := t.Add(-w.d)
	i := 0
	for i < len(w.samples) && w.samples[i].t.Before(cut) {
		i++
	}
	w.samples = w.samples[i:]
}

// Values returns the samples that are still inside the window at `now` (old ones expire even if nothing was added).
func (w *Window) Values(now time.Time) []float64 {
	cut := now.Add(-w.d)
	out := make([]float64, 0, len(w.samples))
	for _, s := range w.samples {
		if !s.t.Before(cut) {
			out = append(out, s.v)
		}
	}
	return out
}
