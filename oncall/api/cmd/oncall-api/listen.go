package main

import (
	"context"
	"log/slog"
	"net"
	"syscall"

	"github.com/aditya388182/payments-cdc-pipeline/oncall/api/internal/config"
)

// listenAPI opens the public listener. ONCALL_WS_SNDBUF (bytes, default 0 = the OS default) sets SO_SNDBUF on the
// listening socket; Linux copies it to accepted connections. It exists for the load test: with the OS default a client
// that stops reading takes a very long time to fill its kernel buffers with 2-second snapshots, so a stalled reader would
// not be dropped inside a 5-minute run. Leave it at 0 in normal use.
func listenAPI(ctx context.Context, cfg config.Config) (net.Listener, error) {
	lc := net.ListenConfig{}
	if cfg.WSSendBuf > 0 {
		lc.Control = func(_, _ string, c syscall.RawConn) error {
			var serr error
			if err := c.Control(func(fd uintptr) {
				serr = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_SNDBUF, cfg.WSSendBuf)
			}); err != nil {
				return err
			}
			return serr
		}
		slog.Warn("ONCALL_WS_SNDBUF is set: load-test setting, not for normal use", "bytes", cfg.WSSendBuf)
	}
	return lc.Listen(ctx, "tcp", cfg.Addr)
}
