// Command ws-slowclients opens N WebSocket connections to the live feed and then reads them far slower than the server
// publishes (one frame per -read-every), with a tiny kernel receive buffer, to reproduce "slow phones" for the load test
// (oncall/loadtest/run.sh). It reports how many connections the server had closed by the end.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
)

func main() {
	url := flag.String("url", "ws://127.0.0.1:8088/v1/live", "live endpoint")
	token := flag.String("token", os.Getenv("TOKEN"), "access token (or env TOKEN)")
	n := flag.Int("n", 100, "number of slow clients")
	dur := flag.Duration("duration", 6*time.Minute, "how long to keep them")
	rcvbuf := flag.Int("rcvbuf", 2048, "SO_RCVBUF in bytes for each connection (set before connecting)")
	every := flag.Duration("read-every", 6*time.Second, "read ONE frame this often (the server sends one every 2 s, so the backlog grows)")
	out := flag.String("out", "", "write a JSON summary here")
	flag.Parse()
	if *token == "" {
		fmt.Fprintln(os.Stderr, "token required")
		os.Exit(2)
	}

	ctx, cancel := context.WithTimeout(context.Background(), *dur)
	defer cancel()

	var connected, failed, closedByServer, framesRead atomic.Int64
	var mu sync.Mutex
	reasons := map[string]int{}
	var secondsUntilDropped []float64
	var wg sync.WaitGroup
	for i := 0; i < *n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			dialer := &net.Dialer{}
			hc := &http.Client{Transport: &http.Transport{DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
				c, err := dialer.DialContext(ctx, network, addr)
				if tc, ok := c.(*net.TCPConn); ok && err == nil {
					_ = tc.SetReadBuffer(*rcvbuf)
				}
				return c, err
			}}}
			conn, _, err := websocket.Dial(ctx, *url+"?access_token="+*token, &websocket.DialOptions{HTTPClient: hc})
			if err != nil {
				failed.Add(1)
				return
			}
			connected.Add(1)
			connectedAt := time.Now()
			defer conn.CloseNow()
			t := time.NewTicker(*every)
			defer t.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-t.C:
					rctx, rc := context.WithTimeout(ctx, 5*time.Second)
					_, _, err := conn.Read(rctx)
					rc()
					if err == nil {
						framesRead.Add(1)
						continue
					}
					if ctx.Err() != nil {
						return
					}
					// the server ended the connection (slow consumer, write timeout, ping timeout, ...)
					reason := err.Error()
					if len(reason) > 90 {
						reason = reason[:90]
					}
					if st := websocket.CloseStatus(err); st != -1 {
						reason = fmt.Sprintf("close %d: %s", st, reason)
					}
					closedByServer.Add(1)
					mu.Lock()
					reasons[reason]++
					secondsUntilDropped = append(secondsUntilDropped, time.Since(connectedAt).Seconds())
					mu.Unlock()
					return
				}
			}
		}(i)
	}
	wg.Wait()
	sort.Float64s(secondsUntilDropped)
	median := 0.0
	if len(secondsUntilDropped) > 0 {
		median = secondsUntilDropped[len(secondsUntilDropped)/2]
	}
	sum := map[string]any{
		"median_seconds_until_dropped": median,
		"slow_clients_requested":       *n, "connected": connected.Load(), "connect_failed": failed.Load(),
		"closed_by_server_before_end": closedByServer.Load(), "close_reasons": reasons, "frames_read": framesRead.Load(),
		"rcvbuf_bytes": *rcvbuf, "read_one_frame_every": every.String(), "duration": dur.String(),
	}
	b, _ := json.MarshalIndent(sum, "", "  ")
	fmt.Println(string(b))
	if *out != "" {
		_ = os.WriteFile(*out, b, 0o644)
	}
}
