package collect

import (
	"strings"
	"testing"
)

func TestParseProcessedOffsetsFromPushgatewayText(t *testing.T) {
	text := `# TYPE payments_cdc_processed_offset gauge
payments_cdc_processed_offset{instance="",job="payments_cdc",partition="0"} 1313
payments_cdc_processed_offset{instance="",job="payments_cdc",partition="2"} 1.5e3
payments_cdc_processed_offset_other{partition="1"} 9
payments_cdc_topic_end_offset{partition="0"} 5000
payments_cdc_batch_id{instance="",job="payments_cdc"} 14
`
	got := ParseProcessedOffsets(strings.NewReader(text))
	if len(got) != 2 || got[0] != 1313 || got[2] != 1500 {
		t.Fatalf("got %v", got)
	}
}

func TestConnectorFromStatusCollapsesTaskStates(t *testing.T) {
	cases := map[string]string{
		`{"connector":{"state":"RUNNING"},"tasks":[{"state":"RUNNING"}]}`:                        "RUNNING",
		`{"connector":{"state":"RUNNING"},"tasks":[{"state":"FAILED"}]}`:                         "FAILED",
		`{"connector":{"state":"PAUSED"},"tasks":[{"state":"RUNNING"}]}`:                         "PAUSED",
		`{"connector":{"state":"RUNNING"},"tasks":[{"state":"RUNNING"},{"state":"UNASSIGNED"}]}`: "UNASSIGNED",
	}
	for in, want := range cases {
		got, err := ConnectorFromStatus([]byte(in))
		if err != nil || got.State != want {
			t.Fatalf("%s -> %v err=%v, want %s", in, got.State, err, want)
		}
	}
	if _, err := ConnectorFromStatus([]byte("not json")); err == nil {
		t.Fatal("expected a decode error")
	}
}

func TestComputeLagPrefersProgressEventsAndFallsBackToPushgateway(t *testing.T) {
	ends := map[int32]int64{0: 2325, 1: 2305, 2: 2354, 3: 10}
	events := map[int32]int64{0: 2325, 2: 2354}          // exact, from the raw batch at commit time
	pushgw := map[int32]int64{0: 2300, 1: 2305, 2: 2353} // one batch stale: must NOT win where events exist
	got := ComputeLag(ends, events, pushgw)
	want := []int64{0, 0, 0, 10} // partition 1 falls back to the Pushgateway; partition 3 is unknown everywhere
	if len(got) != 4 {
		t.Fatalf("got %+v", got)
	}
	for i, w := range want {
		if got[i].Lag != w || got[i].Partition != int32(i) {
			t.Fatalf("partition %d lag = %d, want %d (%+v)", i, got[i].Lag, w, got)
		}
	}
}

func TestProcessedOffsetsOnlyMoveForward(t *testing.T) {
	s := NewState()
	s.ObserveBatch(at(0), ProgressEvent{BatchID: 1, EndOffsets: map[string]int64{"0": 100, "1": 50}})
	s.ObserveBatch(at(10), ProgressEvent{BatchID: 2, EndOffsets: map[string]int64{"0": 90, "2": 7}}) // 90 < 100: ignored
	p := s.Processed()
	if p[0] != 100 || p[1] != 50 || p[2] != 7 {
		t.Fatalf("processed = %v", p)
	}
}
