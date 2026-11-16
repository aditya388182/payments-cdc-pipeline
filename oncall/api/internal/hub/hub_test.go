package hub

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aditya388182/payments-cdc-pipeline/oncall/api/internal/metrics"
	"github.com/coder/websocket"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }

type snap struct {
	Seq uint64 `json:"seq"`
	T   int64  `json:"t"` // publish time, unix nanos
}

func publish(h *Hub) uint64 {
	return h.PublishSnapshot(func(seq uint64) any { return snap{Seq: seq, T: time.Now().UnixNano()} })
}

type wire struct {
	Type  string          `json:"type"`
	Seq   uint64          `json:"seq"`
	Event string          `json:"event"`
	Data  json.RawMessage `json:"data"`
}

func decode(t testing.TB, f Frame) wire {
	t.Helper()
	var w wire
	if err := json.Unmarshal(f.Data, &w); err != nil {
		t.Fatalf("bad frame: %v", err)
	}
	return w
}

func TestFanOutToManyClientsInOrder(t *testing.T) {
	h := New(WithBuffer(32))
	const clients, frames = 200, 20
	var wg sync.WaitGroup
	results := make([][]uint64, clients)
	for i := 0; i < clients; i++ {
		c, _, _ := h.Subscribe(0, 0)
		wg.Add(1)
		go func(i int, c *Client) {
			defer wg.Done()
			for f := range c.Send {
				results[i] = append(results[i], f.Seq)
				if len(results[i]) == frames {
					h.Unsubscribe(c)
				}
			}
		}(i, c)
	}
	for i := 0; i < frames; i++ {
		publish(h)
	}
	wg.Wait()
	for i, got := range results {
		if len(got) != frames {
			t.Fatalf("client %d got %d frames, want %d", i, len(got), frames)
		}
		for j := 1; j < len(got); j++ {
			if got[j] != got[j-1]+1 {
				t.Fatalf("client %d: seq not consecutive: %v", i, got)
			}
		}
	}
	if n := h.ClientCount(); n != 0 {
		t.Fatalf("clients left: %d", n)
	}
}

func TestSlowClientIsDroppedWhileOthersKeepReceivingOnTime(t *testing.T) {
	h := New(WithBuffer(4))
	slow, _, _ := h.Subscribe(0, 0) // never reads

	const fast, frames = 20, 40
	var wg sync.WaitGroup
	var maxLatency atomic.Int64
	var received atomic.Int64
	for i := 0; i < fast; i++ {
		c, _, _ := h.Subscribe(0, 0)
		wg.Add(1)
		go func(c *Client) {
			defer wg.Done()
			n := 0
			for f := range c.Send {
				var s struct {
					Data snap `json:"data"`
				}
				_ = json.Unmarshal(f.Data, &s)
				lat := time.Now().UnixNano() - s.Data.T
				for {
					cur := maxLatency.Load()
					if lat <= cur || maxLatency.CompareAndSwap(cur, lat) {
						break
					}
				}
				received.Add(1)
				n++
				if n == frames {
					h.Unsubscribe(c)
				}
			}
		}(c)
	}

	start := time.Now()
	for i := 0; i < frames; i++ {
		publish(h)
		time.Sleep(2 * time.Millisecond)
	}
	publishTime := time.Since(start)
	wg.Wait()

	select {
	case <-slow.Dropped():
	default:
		t.Fatal("slow client was not dropped")
	}
	if got := received.Load(); got != fast*frames {
		t.Fatalf("fast clients received %d frames, want %d", got, fast*frames)
	}
	if lat := time.Duration(maxLatency.Load()); lat > 100*time.Millisecond {
		t.Fatalf("fast client latency %v: the slow client delayed the others", lat)
	}
	if publishTime > time.Duration(frames)*2*time.Millisecond+500*time.Millisecond {
		t.Fatalf("publishing took %v: the broadcast blocked", publishTime)
	}
	if h.ClientCount() != 0 {
		t.Fatalf("clients left: %d", h.ClientCount())
	}
}

func TestSeqResumeSendsMissedAlertsThenLatestSnapshotInOrder(t *testing.T) {
	h := New()
	publish(h)                                                       // 1
	publish(h)                                                       // 2
	h.PublishAlert("firing", map[string]any{"id": 1, "rule": "lag"}) // 3
	publish(h)                                                       // 4
	h.PublishAlert("resolved", map[string]any{"id": 1})              // 5
	publish(h)                                                       // 6 (latest snapshot)

	_, backlog, hello := h.Subscribe(2, h.Epoch())
	if !hello.Resumed || hello.Missed != 2 || hello.Seq != 6 {
		t.Fatalf("hello = %+v", hello)
	}
	var got []uint64
	for _, f := range backlog {
		got = append(got, f.Seq)
	}
	want := []uint64{3, 5, 6}
	if len(got) != len(want) {
		t.Fatalf("backlog seqs = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("backlog seqs = %v, want %v", got, want)
		}
	}
	if w := decode(t, backlog[0]); w.Type != "alert" || w.Event != "firing" {
		t.Fatalf("first backlog frame = %+v", w)
	}

	// a client that is already up to date gets nothing it has seen
	_, backlog, hello = h.Subscribe(6, h.Epoch())
	if len(backlog) != 0 || !hello.Resumed {
		t.Fatalf("up-to-date client got backlog=%d hello=%+v", len(backlog), hello)
	}
}

func TestResumeFromAnotherEpochOrFutureSeqIsTreatedAsFresh(t *testing.T) {
	h := New()
	publish(h)
	publish(h)
	_, backlog, hello := h.Subscribe(1, h.Epoch()+1)
	if hello.Resumed || len(backlog) != 1 || backlog[0].Seq != 2 {
		t.Fatalf("other epoch: hello=%+v backlog=%d", hello, len(backlog))
	}
	_, backlog, hello = h.Subscribe(99, h.Epoch())
	if hello.Resumed || len(backlog) != 1 {
		t.Fatalf("future seq: hello=%+v backlog=%d", hello, len(backlog))
	}
}

func TestSeqIsStrictlyIncreasingAcrossSnapshotsAndAlerts(t *testing.T) {
	h := New()
	var last uint64
	for i := 0; i < 50; i++ {
		var s uint64
		if i%7 == 0 {
			s = h.PublishAlert("firing", map[string]int{"i": i})
		} else {
			s = publish(h)
		}
		if s != last+1 {
			t.Fatalf("seq %d after %d", s, last)
		}
		last = s
	}
}

func wsServer(t *testing.T, h *Hub, opts WSOptions) (*httptest.Server, string) {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { h.ServeWS(w, r, opts) }))
	t.Cleanup(srv.Close)
	return srv, "ws" + strings.TrimPrefix(srv.URL, "http")
}

func readWire(t *testing.T, ctx context.Context, c *websocket.Conn) wire {
	t.Helper()
	_, b, err := c.Read(ctx)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	var w wire
	if err := json.Unmarshal(b, &w); err != nil {
		t.Fatalf("decode %q: %v", b, err)
	}
	return w
}

func TestWebSocketEndToEndAndResume(t *testing.T) {
	h := New()
	_, url := wsServer(t, h, WSOptions{})
	publish(h) // seq 1 exists before anyone connects: first paint
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	c1, _, err := websocket.Dial(ctx, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	hello := readWire(t, ctx, c1)
	first := readWire(t, ctx, c1)
	if hello.Type != "hello" || first.Type != "snapshot" || first.Seq != 1 {
		t.Fatalf("hello=%+v first=%+v", hello, first)
	}
	publish(h)
	if w := readWire(t, ctx, c1); w.Seq != 2 {
		t.Fatalf("live frame seq = %d", w.Seq)
	}
	c1.Close(websocket.StatusNormalClosure, "")

	// while disconnected: an alert and two snapshots happen
	h.PublishAlert("firing", map[string]string{"rule": "connector"})
	publish(h)
	publish(h)

	c2, _, err := websocket.Dial(ctx, url+"?last_seq=2&epoch="+itoa(h.Epoch()), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer c2.CloseNow()
	hello = readWire(t, ctx, c2)
	var hd Hello
	_ = json.Unmarshal(mustJSON(hello), &hd)
	a := readWire(t, ctx, c2)
	s := readWire(t, ctx, c2)
	if a.Type != "alert" || a.Seq != 3 || s.Type != "snapshot" || s.Seq != 5 {
		t.Fatalf("resume delivered alert=%+v snapshot=%+v", a, s)
	}
}

func TestPingTimeoutClosesADeadPeer(t *testing.T) {
	h := New()
	_, url := wsServer(t, h, WSOptions{PingInterval: 30 * time.Millisecond, PingTimeout: 80 * time.Millisecond})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	// A peer that connects but never reads cannot answer pings (the library answers pongs from its read loop).
	c, _, err := websocket.Dial(ctx, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer c.CloseNow()
	deadline := time.Now().Add(5 * time.Second)
	for h.ClientCount() != 1 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	for h.ClientCount() != 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if h.ClientCount() != 0 {
		t.Fatal("dead peer was not disconnected by the ping timeout")
	}
}

func TestAnAnsweringPeerStaysConnectedThroughPings(t *testing.T) {
	h := New()
	_, url := wsServer(t, h, WSOptions{PingInterval: 30 * time.Millisecond, PingTimeout: 200 * time.Millisecond})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	c, _, err := websocket.Dial(ctx, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer c.CloseNow()
	go func() { // reading in the background answers pings
		for {
			if _, _, err := c.Read(ctx); err != nil {
				return
			}
		}
	}()
	time.Sleep(400 * time.Millisecond)
	if h.ClientCount() != 1 {
		t.Fatalf("healthy peer was dropped (clients=%d)", h.ClientCount())
	}
}

func TestSlowWebSocketClientIsDisconnectedNotTheOthers(t *testing.T) {
	m := metrics.New()
	h := New(WithBuffer(64), WithMetrics(m))
	_, url := wsServer(t, h, WSOptions{WriteTimeout: 300 * time.Millisecond})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	slow, _, err := websocket.Dial(ctx, url, nil) // never reads
	if err != nil {
		t.Fatal(err)
	}
	defer slow.CloseNow()
	fast, _, err := websocket.Dial(ctx, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer fast.CloseNow()
	fast.SetReadLimit(-1) // the default 32 KiB read limit would reject the padded frames below
	var got atomic.Int64
	fastDone := make(chan struct{})
	go func() {
		defer close(fastDone)
		for {
			if _, _, err := fast.Read(ctx); err != nil {
				return
			}
			got.Add(1)
		}
	}()
	deadline := time.Now().Add(3 * time.Second)
	for h.ClientCount() != 2 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	big := strings.Repeat("x", 128*1024) // fill the slow peer's socket buffers so its writer blocks
	for i := 0; i < 600 && h.ClientCount() == 2; i++ {
		h.PublishSnapshot(func(seq uint64) any { return map[string]any{"seq": seq, "pad": big} })
		time.Sleep(10 * time.Millisecond)
	}
	if h.ClientCount() != 1 {
		t.Fatalf("expected exactly the slow client to be gone, clients=%d", h.ClientCount())
	}
	before := got.Load()
	publish(h)
	time.Sleep(150 * time.Millisecond)
	if got.Load() <= before {
		t.Fatal("the fast client stopped receiving after the slow one was dropped")
	}
	// the disconnect is counted: either the bounded buffer filled (dropped) or a frame write timed out
	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	body := rec.Body.String()
	if !strings.Contains(body, "oncall_ws_clients_dropped_total 1") && !strings.Contains(body, "oncall_ws_write_timeouts_total 1") {
		t.Fatalf("the slow client's disconnect was not counted in any metric:\n%s", grepLines(body, "oncall_ws_"))
	}
	// the slow connection really is closed by the server: draining it ends in an error
	drainCtx, dcancel := context.WithTimeout(ctx, 10*time.Second)
	defer dcancel()
	for {
		if _, _, err := slow.Read(drainCtx); err != nil {
			if drainCtx.Err() != nil {
				t.Fatal("slow connection was never closed by the server")
			}
			break
		}
	}
	fast.Close(websocket.StatusNormalClosure, "")
	<-fastDone
}

func grepLines(body, prefix string) string {
	var out []string
	for _, l := range strings.Split(body, "\n") {
		if strings.HasPrefix(l, prefix) {
			out = append(out, l)
		}
	}
	return strings.Join(out, "\n")
}

func itoa(n int64) string { b, _ := json.Marshal(n); return string(b) }
func mustJSON(v any) []byte {
	b, _ := json.Marshal(v)
	return b
}
