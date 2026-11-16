// Package hub fans snapshots and alert events out to many WebSocket clients.
//
// Design (the interview story): one producer publishes a frame; the hub copies a pointer to it into every
// client's own bounded channel with a NON-blocking send. A client whose buffer is full is a slow consumer and
// is disconnected (counted in metrics); it reconnects and resumes from its last seq. One slow phone therefore
// can never stall the broadcast, and memory per client is bounded by the channel size.
package hub

import (
	"encoding/json"
	"sync"
	"time"

	"github.com/aditya388182/payments-cdc-pipeline/oncall/api/internal/metrics"
)

// Frame is one message with its sequence number, already serialized once for all clients.
type Frame struct {
	Seq  uint64
	Kind string // "snapshot" | "alert"
	Data []byte // the full JSON envelope sent on the wire
}

type Hub struct {
	mu        sync.Mutex
	clients   map[*Client]struct{}
	seq       uint64
	latest    *Frame
	alerts    []Frame
	maxAlerts int
	bufSize   int
	epoch     int64
	nextID    uint64
	m         *metrics.M
}

type Option func(*Hub)

func WithBuffer(n int) Option         { return func(h *Hub) { h.bufSize = n } }
func WithMetrics(m *metrics.M) Option { return func(h *Hub) { h.m = m } }
func WithAlertHistory(n int) Option   { return func(h *Hub) { h.maxAlerts = n } }

func New(opts ...Option) *Hub {
	h := &Hub{clients: map[*Client]struct{}{}, maxAlerts: 256, bufSize: 8, epoch: time.Now().UnixMilli()}
	for _, o := range opts {
		o(h)
	}
	return h
}

// Epoch identifies this server run; a client's last_seq is only meaningful within one epoch.
func (h *Hub) Epoch() int64 { return h.epoch }

type Client struct {
	ID   uint64
	Send chan Frame
	hub  *Hub
	// Dropped is closed by the hub when it removed the client for being slow.
	dropped chan struct{}
	once    sync.Once
}

// Dropped is closed when the hub disconnected this client because its buffer was full.
func (c *Client) Dropped() <-chan struct{} { return c.dropped }

type Hello struct {
	Type    string `json:"type"`
	Seq     uint64 `json:"seq"`
	Epoch   int64  `json:"epoch"`
	Resumed bool   `json:"resumed"`
	Missed  int    `json:"missed_alerts"`
}

// Subscribe registers a client and returns the frames it must be sent first. lastSeq==0 or a different epoch
// is a fresh subscription (latest snapshot only); otherwise the client gets every alert it missed plus the
// latest snapshot, in seq order. The backlog is computed under the same lock that enqueues live frames, so a
// client never sees a gap or a duplicate between the backlog and the live stream.
func (h *Hub) Subscribe(lastSeq uint64, epoch int64) (*Client, []Frame, Hello) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.nextID++
	c := &Client{ID: h.nextID, Send: make(chan Frame, h.bufSize), hub: h, dropped: make(chan struct{})}
	h.clients[c] = struct{}{}
	h.m.WSClients(1)
	h.m.WSConnect()

	resumed := lastSeq > 0 && epoch == h.epoch && lastSeq <= h.seq
	var backlog []Frame
	missed := 0
	if resumed {
		for _, a := range h.alerts {
			if a.Seq > lastSeq {
				backlog = append(backlog, a)
				missed++
			}
		}
		if h.latest != nil && h.latest.Seq > lastSeq {
			backlog = append(backlog, *h.latest)
		}
		sortBySeq(backlog)
	} else if h.latest != nil {
		backlog = append(backlog, *h.latest)
	}
	return c, backlog, Hello{Type: "hello", Seq: h.seq, Epoch: h.epoch, Resumed: resumed, Missed: missed}
}

func sortBySeq(f []Frame) {
	for i := 1; i < len(f); i++ {
		for j := i; j > 0 && f[j].Seq < f[j-1].Seq; j-- {
			f[j], f[j-1] = f[j-1], f[j]
		}
	}
}

// Unsubscribe removes the client (idempotent). Safe to call from any goroutine.
func (h *Hub) Unsubscribe(c *Client) {
	h.mu.Lock()
	h.removeLocked(c, false)
	h.mu.Unlock()
}

func (h *Hub) removeLocked(c *Client, slow bool) {
	if _, ok := h.clients[c]; !ok {
		return
	}
	delete(h.clients, c)
	h.m.WSClients(-1)
	c.once.Do(func() {
		if slow {
			close(c.dropped)
			h.m.WSDropped()
		}
		close(c.Send) // only the hub sends, always under h.mu, so this cannot race a send
	})
}

// ClientCount is the number of connected clients.
func (h *Hub) ClientCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.clients)
}

type envelope struct {
	Type  string `json:"type"`
	Seq   uint64 `json:"seq"`
	Event string `json:"event,omitempty"`
	Data  any    `json:"data"`
}

// PublishSnapshot assigns the next seq, serializes once and fans the frame out. v is marshalled with the seq
// already set by the caller through the setSeq callback so the seq inside the payload matches the envelope.
func (h *Hub) PublishSnapshot(build func(seq uint64) any) uint64 {
	h.mu.Lock()
	h.seq++
	seq := h.seq
	data, err := json.Marshal(envelope{Type: "snapshot", Seq: seq, Data: build(seq)})
	if err != nil {
		h.mu.Unlock()
		return seq
	}
	f := Frame{Seq: seq, Kind: "snapshot", Data: data}
	h.latest = &f
	h.fanoutLocked(f)
	h.mu.Unlock()
	return seq
}

// PublishAlert broadcasts an alert state change (event: firing | acknowledged | resolved) and keeps it for resume.
func (h *Hub) PublishAlert(event string, alert any) uint64 {
	h.mu.Lock()
	h.seq++
	seq := h.seq
	data, err := json.Marshal(envelope{Type: "alert", Seq: seq, Event: event, Data: alert})
	if err != nil {
		h.mu.Unlock()
		return seq
	}
	f := Frame{Seq: seq, Kind: "alert", Data: data}
	h.alerts = append(h.alerts, f)
	if len(h.alerts) > h.maxAlerts {
		h.alerts = h.alerts[len(h.alerts)-h.maxAlerts:]
	}
	h.fanoutLocked(f)
	h.mu.Unlock()
	return seq
}

// fanoutLocked never blocks: a full buffer means a slow consumer, which is removed.
func (h *Hub) fanoutLocked(f Frame) {
	var slow []*Client
	for c := range h.clients {
		select {
		case c.Send <- f:
		default:
			slow = append(slow, c)
		}
	}
	for _, c := range slow {
		h.removeLocked(c, true)
	}
}

// Latest returns the most recent snapshot frame, if any.
func (h *Hub) Latest() (Frame, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.latest == nil {
		return Frame{}, false
	}
	return *h.latest, true
}

// Seq is the last assigned sequence number.
func (h *Hub) Seq() uint64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.seq
}
