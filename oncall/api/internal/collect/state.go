// Package collect gathers pipeline signals (Kafka lag, connector status, Spark progress events, source and lake
// row counts, DLQ size) into a State that builds one Snapshot per tick without doing any I/O.
package collect

import (
	"strconv"
	"sync"
	"time"

	"github.com/aditya388182/payments-cdc-pipeline/oncall/api/internal/snapshot"
)

const (
	freshnessWindow = 60 * time.Second
	activeFor       = 60 * time.Second // a pipeline is "active" if a batch committed within this long
	drainQuiet      = 15 * time.Second // nothing moved for this long: the stream is drained
	rateInWindow    = 10 * time.Second
	rateOutWindow   = 30 * time.Second
	dlqWindow       = 60 * time.Second
)

// ProgressEvent is the JSON the Spark job publishes after each committed batch.
type ProgressEvent struct {
	AppID       string  `json:"app_id"`
	BatchID     int64   `json:"batch_id"`
	InputRows   int64   `json:"input_rows"`
	MergedRows  int64   `json:"merged_rows"`
	DLQRows     int64   `json:"dlq_rows"`
	DurationSec float64 `json:"duration_seconds"`
	MaxSourceTS *string `json:"max_source_ts"`
	CommittedAt string  `json:"committed_at"`
	// EndOffsets is the exclusive end offset per Kafka partition present in the batch (next offset to read).
	EndOffsets map[string]int64 `json:"end_offsets"`
}

type sourceStat struct {
	at  time.Time
	err string
	set bool
}

type timed struct {
	t time.Time
	v int64
}

type State struct {
	mu sync.Mutex

	lagParts   []snapshot.PartitionLag
	endHist    []timed
	endChanged time.Time

	connector snapshot.Connector

	sourceCount   int64
	sourceChanged time.Time

	lakeCount int64

	dlq     snapshot.DLQ
	dlqMiss int64
	dlqHist []timed

	batches     []batchObs
	lastBatch   *snapshot.Batch
	lastBatchAt time.Time
	maxSourceTS time.Time
	processed   map[int32]int64 // exclusive end offsets reported by progress events (max per partition)

	fresh *snapshot.Window
	src   map[string]*sourceStat
}

type batchObs struct {
	at     time.Time
	merged int64
}

func NewState() *State {
	return &State{fresh: snapshot.NewWindow(freshnessWindow), src: map[string]*sourceStat{}, processed: map[int32]int64{}}
}

func (s *State) mark(name string, now time.Time, err error) {
	st := s.src[name]
	if st == nil {
		st = &sourceStat{}
		s.src[name] = st
	}
	st.set = true
	if err != nil {
		st.err = err.Error()
		return
	}
	st.err = ""
	st.at = now
}

func (s *State) SetLag(now time.Time, parts []snapshot.PartitionLag, endTotal int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lagParts = parts
	if len(s.endHist) == 0 || s.endHist[len(s.endHist)-1].v != endTotal {
		s.endChanged = now
	}
	s.endHist = append(trim(s.endHist, now, 2*rateInWindow), timed{now, endTotal})
	s.mark("kafka", now, nil)
}

// Processed returns a copy of the per-partition exclusive end offsets the Spark job reported in progress events.
func (s *State) Processed() map[int32]int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[int32]int64, len(s.processed))
	for k, v := range s.processed {
		out[k] = v
	}
	return out
}

func (s *State) SetConnector(now time.Time, c snapshot.Connector) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.connector = c
	s.mark("connect", now, nil)
}

func (s *State) SetSourceCount(now time.Time, n int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.src["source_db"].isSet() || n != s.sourceCount {
		s.sourceChanged = now
	}
	s.sourceCount = n
	s.mark("source_db", now, nil)
}

func (st *sourceStat) isSet() bool { return st != nil && st.set && st.at != (time.Time{}) }

func (s *State) SetLake(now time.Time, rows int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lakeCount = rows
	s.mark("lake", now, nil)
}

func (s *State) SetDLQ(now time.Time, messages, open, openMissing int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dlq.Messages, s.dlq.Open, s.dlqMiss = messages, open, openMissing
	s.dlqHist = append(trim(s.dlqHist, now, 2*dlqWindow), timed{now, messages})
	s.mark("dlq", now, nil)
}

// Fail records that a source could not be read; its last good value stays but is flagged in the snapshot.
func (s *State) Fail(name string, now time.Time, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mark(name, now, err)
	if name == "connect" {
		s.connector = snapshot.Connector{State: "UNREACHABLE"}
	}
}

func (s *State) ObserveBatch(now time.Time, ev ProgressEvent) {
	s.mu.Lock()
	defer s.mu.Unlock()
	b := &snapshot.Batch{BatchID: ev.BatchID, InputRows: ev.InputRows, MergedRows: ev.MergedRows, DLQRows: ev.DLQRows,
		DurationSec: ev.DurationSec, MaxSourceTS: ev.MaxSourceTS, CommittedAt: ev.CommittedAt}
	if s.lastBatch == nil || ev.BatchID >= s.lastBatch.BatchID {
		s.lastBatch = b
	}
	s.lastBatchAt = now
	if ev.MaxSourceTS != nil {
		if t, err := time.Parse(time.RFC3339Nano, *ev.MaxSourceTS); err == nil && t.After(s.maxSourceTS) {
			s.maxSourceTS = t
		}
	}
	for k, v := range ev.EndOffsets {
		if p, err := strconv.ParseInt(k, 10, 32); err == nil && v > s.processed[int32(p)] {
			s.processed[int32(p)] = v
		}
	}
	s.batches = append(s.batches, batchObs{now, ev.MergedRows})
	cut := now.Add(-2 * rateOutWindow)
	i := 0
	for i < len(s.batches) && s.batches[i].at.Before(cut) {
		i++
	}
	s.batches = s.batches[i:]
	s.mark("progress", now, nil)
}

func trim(h []timed, now time.Time, keep time.Duration) []timed {
	cut := now.Add(-keep)
	i := 0
	for i < len(h) && h[i].t.Before(cut) {
		i++
	}
	return h[i:]
}

// Build assembles the snapshot for `now`. It must be called from a single goroutine with increasing `now`
// (it advances the freshness window). Seq is assigned by the hub.
func (s *State) Build(now time.Time) snapshot.Snapshot {
	s.mu.Lock()
	defer s.mu.Unlock()

	var lagTotal int64
	for _, p := range s.lagParts {
		lagTotal += p.Lag
	}
	kafkaOK := s.src["kafka"].isSet() && s.src["kafka"].err == ""
	caughtUp := kafkaOK && lagTotal == 0

	active := !s.lastBatchAt.IsZero() && now.Sub(s.lastBatchAt) <= activeFor
	var freshness float64
	if !s.maxSourceTS.IsZero() {
		freshness = now.Sub(s.maxSourceTS).Seconds()
		if freshness < 0 {
			freshness = 0
		}
		// staleness is only a latency sample while records are waiting: once caught up, the age of the newest
		// committed row just measures how long the source has been quiet
		if active && !caughtUp {
			s.fresh.Add(now, freshness)
		}
	}
	vals := s.fresh.Values(now)
	p50, p95 := snapshot.Percentile(vals, 0.5), snapshot.Percentile(vals, 0.95)

	rateIn := rate(s.endHist, now, rateInWindow)
	var merged int64
	for _, b := range s.batches {
		if now.Sub(b.at) <= rateOutWindow {
			merged += b.merged
		}
	}
	rateOut := float64(merged) / rateOutWindow.Seconds()

	growth := int64(0)
	if h := trim(append([]timed(nil), s.dlqHist...), now, dlqWindow); len(h) > 0 {
		growth = s.dlq.Messages - h[0].v
	}
	dlq := s.dlq
	dlq.GrowthPerMin = growth

	quiet := func(since time.Time) bool { return !since.IsZero() && now.Sub(since) >= drainQuiet }
	drained := caughtUp && (s.lastBatchAt.IsZero() || now.Sub(s.lastBatchAt) >= drainQuiet) && quiet(s.endChanged) && quiet(s.sourceChanged)

	srcOK := s.src["source_db"].isSet() && s.src["source_db"].err == ""
	lakeOK := s.src["lake"].isSet() && s.src["lake"].err == ""
	dlqOK := s.src["dlq"].isSet() && s.src["dlq"].err == ""
	var parity snapshot.Parity
	if srcOK && lakeOK && dlqOK && kafkaOK {
		parity = snapshot.ParityBadge(snapshot.ParityInput{SourceCount: s.sourceCount, LakeCount: s.lakeCount,
			QuarantinedOpen: s.dlqMiss, RatePerSec: rateIn, P95LagSec: p95, Drained: drained})
	} else {
		parity = snapshot.Parity{State: snapshot.Unknown, Detail: "a source needed for the comparison is unavailable"}
	}

	sources := map[string]snapshot.SourceHealth{}
	for name, st := range s.src {
		h := snapshot.SourceHealth{OK: st.err == "" && st.set, Error: st.err}
		if !st.at.IsZero() {
			h.AgeSec = now.Sub(st.at).Seconds()
		}
		sources[name] = h
	}

	parts := append([]snapshot.PartitionLag(nil), s.lagParts...)
	if parts == nil {
		parts = []snapshot.PartitionLag{}
	}
	return snapshot.Snapshot{
		TS: now.UTC(), EventsInPerSec: rateIn, EventsOutPerSec: rateOut, LagTotal: lagTotal, LagPartitions: parts,
		CaughtUp: caughtUp, FreshnessSec: freshness, FreshnessP50: p50, FreshnessP95: p95, Connector: s.connector,
		SourceCount: s.sourceCount, LakeCount: s.lakeCount, Parity: parity, DLQ: dlq, LastBatch: s.lastBatch, Sources: sources,
	}
}

func rate(h []timed, now time.Time, window time.Duration) float64 {
	if len(h) < 2 {
		return 0
	}
	last := h[len(h)-1]
	var base *timed
	for i := range h {
		if last.t.Sub(h[i].t) <= window {
			base = &h[i]
			break
		}
	}
	if base == nil || !last.t.After(base.t) {
		return 0
	}
	d := float64(last.v-base.v) / last.t.Sub(base.t).Seconds()
	if d < 0 {
		return 0
	}
	return d
}
