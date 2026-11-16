// Package metrics holds the Prometheus collectors. Every method is safe on a nil *M so components can be
// used (and tested) without metrics.
package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type M struct {
	Reg *prometheus.Registry

	wsClients     prometheus.Gauge
	wsDropped     prometheus.Counter
	wsConnects    prometheus.Counter
	snapshots     prometheus.Counter
	snapshotBuild prometheus.Histogram
	collectorErrs *prometheus.CounterVec
	httpReqs      *prometheus.CounterVec
	httpDur       *prometheus.HistogramVec
	replays       *prometheus.CounterVec
	alertsFiring  prometheus.Gauge
	pushes        *prometheus.CounterVec
	grpcCalls     *prometheus.CounterVec
	lagTotal      prometheus.Gauge
	parityState   *prometheus.GaugeVec
}

func New() *M {
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	m := &M{
		Reg:           reg,
		wsClients:     prometheus.NewGauge(prometheus.GaugeOpts{Name: "oncall_ws_clients", Help: "Connected WebSocket clients."}),
		wsDropped:     prometheus.NewCounter(prometheus.CounterOpts{Name: "oncall_ws_clients_dropped_total", Help: "Clients disconnected because their buffer was full (slow consumer)."}),
		wsConnects:    prometheus.NewCounter(prometheus.CounterOpts{Name: "oncall_ws_connects_total", Help: "WebSocket connections accepted."}),
		snapshots:     prometheus.NewCounter(prometheus.CounterOpts{Name: "oncall_snapshots_published_total", Help: "Snapshots broadcast."}),
		snapshotBuild: prometheus.NewHistogram(prometheus.HistogramOpts{Name: "oncall_snapshot_build_seconds", Help: "Time to build and publish one snapshot.", Buckets: prometheus.ExponentialBuckets(0.0001, 4, 8)}),
		collectorErrs: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "oncall_collector_errors_total", Help: "Collector failures by source."}, []string{"source"}),
		httpReqs:      prometheus.NewCounterVec(prometheus.CounterOpts{Name: "oncall_http_requests_total", Help: "HTTP requests."}, []string{"route", "method", "code"}),
		httpDur:       prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "oncall_http_request_seconds", Help: "HTTP request duration.", Buckets: prometheus.DefBuckets}, []string{"route"}),
		replays:       prometheus.NewCounterVec(prometheus.CounterOpts{Name: "oncall_replay_results_total", Help: "Replay outcomes."}, []string{"status"}),
		alertsFiring:  prometheus.NewGauge(prometheus.GaugeOpts{Name: "oncall_alerts_active", Help: "Alerts currently firing or acknowledged."}),
		pushes:        prometheus.NewCounterVec(prometheus.CounterOpts{Name: "oncall_push_notifications_total", Help: "Push notifications attempted."}, []string{"result"}),
		grpcCalls:     prometheus.NewCounterVec(prometheus.CounterOpts{Name: "oncall_grpc_client_calls_total", Help: "gRPC calls to replay-svc."}, []string{"method", "code"}),
		lagTotal:      prometheus.NewGauge(prometheus.GaugeOpts{Name: "oncall_consumer_lag_total", Help: "Total consumer lag in the last snapshot."}),
		parityState:   prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "oncall_parity_state", Help: "1 for the current parity state."}, []string{"state"}),
	}
	reg.MustRegister(m.wsClients, m.wsDropped, m.wsConnects, m.snapshots, m.snapshotBuild, m.collectorErrs, m.httpReqs, m.httpDur,
		m.replays, m.alertsFiring, m.pushes, m.grpcCalls, m.lagTotal, m.parityState)
	return m
}

func (m *M) Handler() http.Handler {
	return promhttp.HandlerFor(m.Reg, promhttp.HandlerOpts{})
}

func (m *M) WSClients(delta float64) {
	if m != nil {
		m.wsClients.Add(delta)
	}
}
func (m *M) WSDropped() {
	if m != nil {
		m.wsDropped.Inc()
	}
}
func (m *M) WSConnect() {
	if m != nil {
		m.wsConnects.Inc()
	}
}
func (m *M) SnapshotPublished(seconds float64) {
	if m != nil {
		m.snapshots.Inc()
		m.snapshotBuild.Observe(seconds)
	}
}
func (m *M) CollectorError(source string) {
	if m != nil {
		m.collectorErrs.WithLabelValues(source).Inc()
	}
}
func (m *M) HTTP(route, method, code string, seconds float64) {
	if m != nil {
		m.httpReqs.WithLabelValues(route, method, code).Inc()
		m.httpDur.WithLabelValues(route).Observe(seconds)
	}
}
func (m *M) Replay(status string) {
	if m != nil {
		m.replays.WithLabelValues(status).Inc()
	}
}
func (m *M) AlertsActive(n int) {
	if m != nil {
		m.alertsFiring.Set(float64(n))
	}
}
func (m *M) Push(result string) {
	if m != nil {
		m.pushes.WithLabelValues(result).Inc()
	}
}
func (m *M) GRPC(method, code string) {
	if m != nil {
		m.grpcCalls.WithLabelValues(method, code).Inc()
	}
}
func (m *M) SnapshotValues(lagTotal int64, parity string) {
	if m == nil {
		return
	}
	m.lagTotal.Set(float64(lagTotal))
	for _, s := range []string{"in_sync", "catching_up", "mismatch"} {
		v := 0.0
		if s == parity {
			v = 1
		}
		m.parityState.WithLabelValues(s).Set(v)
	}
}
