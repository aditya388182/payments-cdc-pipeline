package collect

import (
	"errors"
	"testing"
	"time"

	"github.com/aditya388182/payments-cdc-pipeline/oncall/api/internal/snapshot"
)

var t0 = time.Date(2026, 11, 16, 12, 0, 0, 0, time.UTC)

func at(sec int) time.Time { return t0.Add(time.Duration(sec) * time.Second) }

func parts(lag ...int64) []snapshot.PartitionLag {
	var out []snapshot.PartitionLag
	for i, l := range lag {
		out = append(out, snapshot.PartitionLag{Partition: int32(i), End: 1000 + l, Processed: 1000, Lag: l})
	}
	return out
}

func tsPtr(t time.Time) *string { s := t.UTC().Format(time.RFC3339Nano); return &s }

func healthy(s *State, now time.Time, lag []int64, end, src, lake int64) {
	s.SetLag(now, parts(lag...), end)
	s.SetConnector(now, snapshot.Connector{State: "RUNNING", Tasks: []string{"RUNNING"}})
	s.SetSourceCount(now, src)
	s.SetLake(now, lake)
	s.SetDLQ(now, 0, 0, 0)
}

func TestLagTotalAndPerPartition(t *testing.T) {
	s := NewState()
	healthy(s, at(0), []int64{5, 10, 0}, 100, 50, 50)
	snap := s.Build(at(0))
	if snap.LagTotal != 15 || len(snap.LagPartitions) != 3 || snap.CaughtUp {
		t.Fatalf("snap = %+v", snap)
	}
}

func TestEventsInPerSecFromEndOffsetGrowth(t *testing.T) {
	s := NewState()
	for i := 0; i <= 5; i++ {
		healthy(s, at(i*2), []int64{0}, int64(1000+i*40), 50, 50) // +40 events every 2 s = 20/s
	}
	if got := s.Build(at(10)).EventsInPerSec; got < 19.9 || got > 20.1 {
		t.Fatalf("events in/s = %v, want ~20", got)
	}
}

func TestEventsOutPerSecFromMergedRowsOverThirtySeconds(t *testing.T) {
	s := NewState()
	healthy(s, at(0), []int64{0}, 100, 5, 5)
	for i := 0; i < 3; i++ {
		s.ObserveBatch(at(10+i*10), ProgressEvent{BatchID: int64(i), MergedRows: 100, MaxSourceTS: tsPtr(at(8 + i*10))})
	}
	if got := s.Build(at(31)).EventsOutPerSec; got < 9.9 || got > 10.1 {
		t.Fatalf("events out/s = %v, want 10 (300 rows / 30 s)", got)
	}
}

func TestFreshnessFollowsMaxCommittedSourceTimeAndSamplesOnlyWhileBehind(t *testing.T) {
	s := NewState()
	healthy(s, at(0), []int64{3}, 100, 5, 5)
	s.ObserveBatch(at(10), ProgressEvent{BatchID: 1, MergedRows: 5, MaxSourceTS: tsPtr(at(4))})
	snap := s.Build(at(12))
	if snap.FreshnessSec != 8 {
		t.Fatalf("freshness = %v, want 8 (12 - 4)", snap.FreshnessSec)
	}
	// caught up and quiet: freshness keeps growing but must not feed the percentiles (they would inflate the
	// parity allowance at the start of the next burst)
	healthy(s, at(20), []int64{0}, 100, 5, 5)
	s.Build(at(20))
	healthy(s, at(40), []int64{0}, 100, 5, 5)
	quiet := s.Build(at(40))
	if quiet.FreshnessSec != 36 || quiet.FreshnessP95 != 8 {
		t.Fatalf("quiet: freshness=%v p95=%v (no new samples once caught up)", quiet.FreshnessSec, quiet.FreshnessP95)
	}
	// ten minutes later the window has emptied
	healthy(s, at(700), []int64{0}, 100, 5, 5)
	idle := s.Build(at(700))
	if idle.FreshnessSec != 696 || idle.FreshnessP95 != 0 {
		t.Fatalf("idle: freshness=%v p95=%v (window should have emptied)", idle.FreshnessSec, idle.FreshnessP95)
	}
}

func TestPercentilesOverTheLastMinute(t *testing.T) {
	s := NewState()
	for i := 0; i < 20; i++ {
		now := at(i * 2)
		healthy(s, now, []int64{4}, 100, 5, 5)
		s.ObserveBatch(now, ProgressEvent{BatchID: int64(i), MergedRows: 1, MaxSourceTS: tsPtr(now.Add(-time.Duration(i%10+1) * time.Second))})
		snap := s.Build(now)
		if i == 19 && (snap.FreshnessP50 <= 0 || snap.FreshnessP95 < snap.FreshnessP50) {
			t.Fatalf("p50=%v p95=%v", snap.FreshnessP50, snap.FreshnessP95)
		}
	}
}

func TestParityInSyncDuringStreamingLagNotACryWolf(t *testing.T) {
	s := NewState()
	// 20 events/s arriving, batches commit every 10 s about 8 s behind the source: ~160 rows legitimately in flight
	for i := 0; i <= 6; i++ {
		now := at(i * 2)
		healthy(s, now, []int64{30}, int64(1000+i*40), 1000+int64(i*40), 1000+int64(i*40)-150)
		if i%5 == 0 {
			s.ObserveBatch(now, ProgressEvent{BatchID: int64(i), MergedRows: 150, MaxSourceTS: tsPtr(now.Add(-8 * time.Second))})
		}
	}
	snap := s.Build(at(12))
	if snap.Parity.State != snapshot.InSync {
		t.Fatalf("parity = %+v (rate %.1f, p95 %.1f); normal streaming lag must not alarm", snap.Parity, snap.EventsInPerSec, snap.FreshnessP95)
	}
}

func TestParityCatchingUpWhenFarBehindAndNotYetDrained(t *testing.T) {
	s := NewState()
	for i := 0; i <= 6; i++ {
		healthy(s, at(i*2), []int64{5000}, int64(1000+i*40), 5000, 100)
	}
	snap := s.Build(at(12))
	if snap.Parity.State != snapshot.CatchingUp || snap.Parity.Drained {
		t.Fatalf("parity = %+v", snap.Parity)
	}
}

func TestParityMismatchOnlyAfterTheStreamIsDrained(t *testing.T) {
	s := NewState()
	// lag 0, source has 3 more rows than the lake, nothing moving
	for i := 0; i <= 3; i++ {
		healthy(s, at(i*2), []int64{0}, 500, 1003, 1000)
	}
	early := s.Build(at(6))
	if early.Parity.State == snapshot.Mismatch {
		t.Fatalf("flagged a mismatch %v s after the last change; not drained yet: %+v", 6, early.Parity)
	}
	healthy(s, at(30), []int64{0}, 500, 1003, 1000)
	late := s.Build(at(30))
	if late.Parity.State != snapshot.Mismatch || !late.Parity.Drained || late.Parity.Diff != 3 {
		t.Fatalf("parity = %+v, want a drained mismatch of 3", late.Parity)
	}
}

func TestQuarantinedRowsAreNotAMismatch(t *testing.T) {
	s := NewState()
	for _, sec := range []int{0, 2, 30} {
		healthy(s, at(sec), []int64{0}, 500, 1003, 1000)
		s.SetDLQ(at(sec), 3, 3, 3) // the 3 missing rows are open in the DLQ
	}
	snap := s.Build(at(30))
	if snap.Parity.State != snapshot.InSync || snap.Parity.Diff != 0 {
		t.Fatalf("parity = %+v", snap.Parity)
	}
}

func TestMissingSourcesGiveUnknownParityAndHealthFlags(t *testing.T) {
	s := NewState()
	healthy(s, at(0), []int64{0}, 500, 10, 10)
	s.Fail("lake", at(2), errors.New("replay-svc unavailable"))
	snap := s.Build(at(2))
	if snap.Parity.State != snapshot.Unknown {
		t.Fatalf("parity = %+v", snap.Parity)
	}
	if h := snap.Sources["lake"]; h.OK || h.Error == "" {
		t.Fatalf("lake health = %+v", h)
	}
	if !snap.Sources["kafka"].OK {
		t.Fatal("kafka should still be healthy")
	}
}

func TestConnectorUnreachableIsReported(t *testing.T) {
	s := NewState()
	s.Fail("connect", at(0), errors.New("connection refused"))
	if got := s.Build(at(0)).Connector.State; got != "UNREACHABLE" {
		t.Fatalf("connector = %q", got)
	}
}

func TestDLQGrowthPerMinute(t *testing.T) {
	s := NewState()
	s.SetDLQ(at(0), 100, 100, 0)
	s.SetDLQ(at(30), 104, 104, 0)
	s.SetDLQ(at(58), 109, 109, 0)
	if g := s.Build(at(58)).DLQ.GrowthPerMin; g != 9 {
		t.Fatalf("growth = %d, want 9", g)
	}
}
