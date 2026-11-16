package metrics

import (
	"net/http/httptest"
	"strings"
	"testing"
)

func TestNilMetricsAreSafe(t *testing.T) {
	var m *M
	m.WSClients(1)
	m.WSDropped()
	m.WSConnect()
	m.SnapshotPublished(0.1)
	m.CollectorError("kafka")
	m.HTTP("/x", "GET", "2xx", 0.1)
	m.Replay("APPLIED")
	m.AlertsActive(2)
	m.Push("ok")
	m.GRPC("Replay", "OK")
	m.SnapshotValues(1, "in_sync")
}

func TestHandlerExposesTheCollectors(t *testing.T) {
	m := New()
	m.WSClients(2)
	m.WSDropped()
	m.Replay("APPLIED")
	m.SnapshotValues(42, "catching_up")
	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	body := rec.Body.String()
	for _, want := range []string{"oncall_ws_clients 2", "oncall_ws_clients_dropped_total 1", `oncall_replay_results_total{status="APPLIED"} 1`,
		"oncall_consumer_lag_total 42", `oncall_parity_state{state="catching_up"} 1`, `oncall_parity_state{state="in_sync"} 0`} {
		if !strings.Contains(body, want) {
			t.Errorf("missing %q in metrics output", want)
		}
	}
}
