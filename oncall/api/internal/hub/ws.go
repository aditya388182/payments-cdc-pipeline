package hub

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/coder/websocket"
)

// WSOptions tune the connection lifecycle. Zero values mean the defaults below.
type WSOptions struct {
	PingInterval time.Duration // default 15s
	PingTimeout  time.Duration // default 10s: no pong within this closes the connection
	WriteTimeout time.Duration // default 5s per frame
	// OriginPatterns lets the Expo web build on another loopback port connect (browser Origin check).
	OriginPatterns []string
}

func (o WSOptions) withDefaults() WSOptions {
	if o.PingInterval == 0 {
		o.PingInterval = 15 * time.Second
	}
	if o.PingTimeout == 0 {
		o.PingTimeout = 10 * time.Second
	}
	if o.WriteTimeout == 0 {
		o.WriteTimeout = 5 * time.Second
	}
	return o
}

// ServeWS upgrades the request and streams frames until the client leaves, is dropped as slow, or stops
// answering pings. Query parameters: last_seq and epoch (resume). Authentication is the caller's job.
func (h *Hub) ServeWS(w http.ResponseWriter, r *http.Request, opts WSOptions) {
	opts = opts.withDefaults()
	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{OriginPatterns: opts.OriginPatterns})
	if err != nil {
		return // Accept already wrote the HTTP error
	}
	defer conn.CloseNow()

	lastSeq, _ := strconv.ParseUint(r.URL.Query().Get("last_seq"), 10, 64)
	epoch, _ := strconv.ParseInt(r.URL.Query().Get("epoch"), 10, 64)
	client, backlog, hello := h.Subscribe(lastSeq, epoch)
	defer h.Unsubscribe(client)

	// CloseRead consumes control frames (pongs) and cancels ctx when the peer closes.
	ctx := conn.CloseRead(r.Context())
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	write := func(b []byte) error {
		wctx, wcancel := context.WithTimeout(ctx, opts.WriteTimeout)
		defer wcancel()
		return conn.Write(wctx, websocket.MessageText, b)
	}

	hb, _ := json.Marshal(hello)
	if err := write(hb); err != nil {
		return
	}
	for _, f := range backlog {
		if err := write(f.Data); err != nil {
			return
		}
	}

	go func() { // keep-alive: detects dead peers that never send a FIN
		t := time.NewTicker(opts.PingInterval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				pctx, pcancel := context.WithTimeout(ctx, opts.PingTimeout)
				err := conn.Ping(pctx)
				pcancel()
				if err != nil {
					slog.Info("ws ping failed, closing", "client", client.ID, "err", err.Error())
					cancel() // a dead peer never answers the close handshake: do not wait for it
					return
				}
			}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case <-client.Dropped():
			conn.Close(websocket.StatusPolicyViolation, "slow consumer: reconnect with last_seq")
			return
		case f, ok := <-client.Send:
			if !ok {
				select {
				case <-client.Dropped():
					conn.Close(websocket.StatusPolicyViolation, "slow consumer: reconnect with last_seq")
				default:
					conn.Close(websocket.StatusNormalClosure, "")
				}
				return
			}
			if err := write(f.Data); err != nil {
				return
			}
		}
	}
}
