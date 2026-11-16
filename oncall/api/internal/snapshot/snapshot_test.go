package snapshot

import (
	"testing"
	"time"
)

func TestParityInSyncWithinTheInFlightAllowance(t *testing.T) {
	// 20 events/s * 8 s p95 lag * 1.5 = 240 rows may legitimately be in flight
	p := ParityBadge(ParityInput{SourceCount: 10_000, LakeCount: 9_800, RatePerSec: 20, P95LagSec: 8})
	if p.State != InSync || p.Diff != 200 || p.Allowed != 240 {
		t.Fatalf("%+v", p)
	}
}

func TestParityCatchingUpAboveTheAllowance(t *testing.T) {
	p := ParityBadge(ParityInput{SourceCount: 10_000, LakeCount: 9_000, RatePerSec: 20, P95LagSec: 8})
	if p.State != CatchingUp || p.Diff != 1000 {
		t.Fatalf("%+v", p)
	}
}

func TestParityMismatchOnlyWhenDrained(t *testing.T) {
	notDrained := ParityBadge(ParityInput{SourceCount: 100, LakeCount: 97})
	if notDrained.State == Mismatch {
		t.Fatalf("not drained must never be a mismatch: %+v", notDrained)
	}
	drained := ParityBadge(ParityInput{SourceCount: 100, LakeCount: 97, Drained: true, RatePerSec: 50, P95LagSec: 30})
	if drained.State != Mismatch || drained.Diff != 3 {
		t.Fatalf("a drained difference is a mismatch whatever the allowance: %+v", drained)
	}
}

func TestParityDrainedAndEqualIsInSync(t *testing.T) {
	if p := ParityBadge(ParityInput{SourceCount: 100, LakeCount: 100, Drained: true}); p.State != InSync {
		t.Fatalf("%+v", p)
	}
}

func TestParitySubtractsQuarantinedRows(t *testing.T) {
	p := ParityBadge(ParityInput{SourceCount: 100, LakeCount: 90, QuarantinedOpen: 10, Drained: true})
	if p.State != InSync || p.Diff != 0 {
		t.Fatalf("%+v", p)
	}
	// after a replay the quarantined row is in the lake: counts agree without the subtraction
	p = ParityBadge(ParityInput{SourceCount: 100, LakeCount: 100, QuarantinedOpen: 0, Drained: true})
	if p.State != InSync {
		t.Fatalf("%+v", p)
	}
}

func TestParityLakeAheadOfSourceIsAlsoDetected(t *testing.T) {
	p := ParityBadge(ParityInput{SourceCount: 90, LakeCount: 100, Drained: true})
	if p.State != Mismatch || p.Diff != -10 {
		t.Fatalf("%+v", p)
	}
}

func TestPercentileNearestRank(t *testing.T) {
	v := []float64{5, 1, 4, 2, 3}
	if got := Percentile(v, 0.5); got != 3 {
		t.Fatalf("p50 = %v", got)
	}
	if got := Percentile(v, 0.95); got != 5 {
		t.Fatalf("p95 = %v", got)
	}
	if Percentile(nil, 0.5) != 0 {
		t.Fatal("empty should be 0")
	}
}

func TestWindowExpiresOldSamples(t *testing.T) {
	w := NewWindow(10 * time.Second)
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	w.Add(base, 1)
	w.Add(base.Add(5*time.Second), 2)
	if got := w.Values(base.Add(5 * time.Second)); len(got) != 2 {
		t.Fatalf("%v", got)
	}
	if got := w.Values(base.Add(12 * time.Second)); len(got) != 1 || got[0] != 2 {
		t.Fatalf("%v", got)
	}
	if got := w.Values(base.Add(30 * time.Second)); len(got) != 0 {
		t.Fatalf("%v", got)
	}
}
