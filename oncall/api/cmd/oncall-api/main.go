// Command oncall-api is the on-call console backend: REST, WebSocket live feed, alerts, audit log, generator
// control and the gRPC client to replay-svc.
//
//	oncall-api            run the server (configuration from the environment, see .env.example)
//	oncall-api token      mint a short-lived dev access token: oncall-api token -sub admin-user -role admin
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/aditya388182/payments-cdc-pipeline/oncall/api/internal/alerts"
	"github.com/aditya388182/payments-cdc-pipeline/oncall/api/internal/api"
	"github.com/aditya388182/payments-cdc-pipeline/oncall/api/internal/auth"
	"github.com/aditya388182/payments-cdc-pipeline/oncall/api/internal/collect"
	"github.com/aditya388182/payments-cdc-pipeline/oncall/api/internal/config"
	"github.com/aditya388182/payments-cdc-pipeline/oncall/api/internal/generator"
	"github.com/aditya388182/payments-cdc-pipeline/oncall/api/internal/hub"
	"github.com/aditya388182/payments-cdc-pipeline/oncall/api/internal/metrics"
	"github.com/aditya388182/payments-cdc-pipeline/oncall/api/internal/push"
	"github.com/aditya388182/payments-cdc-pipeline/oncall/api/internal/replayclient"
	"github.com/aditya388182/payments-cdc-pipeline/oncall/api/internal/snapshot"
	"github.com/aditya388182/payments-cdc-pipeline/oncall/api/internal/store"
	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})))
	if len(os.Args) > 1 && os.Args[1] == "token" {
		mintToken(os.Args[2:])
		return
	}
	if err := run(); err != nil {
		slog.Error("fatal", "err", err.Error())
		os.Exit(1)
	}
}

func mintToken(args []string) {
	fs := flag.NewFlagSet("token", flag.ExitOnError)
	sub := fs.String("sub", "admin-user", "GitHub login to put in the token")
	role := fs.String("role", "admin", "viewer or admin")
	ttl := fs.Duration("ttl", 15*time.Minute, "lifetime")
	_ = fs.Parse(args)
	cfg, err := config.FromEnv()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	tok, _, err := auth.NewSigner(cfg.JWTSecret, *ttl).Issue(auth.Principal{Login: *sub, Role: *role})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println(tok)
}

func run() error {
	cfg, err := config.FromEnv()
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	st, err := store.Open(ctx, cfg.PGDSN)
	if err != nil {
		return fmt.Errorf("open oncall database: %w", err)
	}
	defer st.Close()
	if err := st.Migrate(ctx); err != nil {
		return fmt.Errorf("migrate: %w", err)
	}
	srcPool, err := pgxpool.New(ctx, cfg.SourcePGDSN)
	if err != nil {
		return fmt.Errorf("source database: %w", err)
	}
	defer srcPool.Close()

	m := metrics.New()
	rc, conn, err := replayclient.Dial(cfg.ReplayAddr, replayclient.Options{Metrics: m})
	if err != nil {
		return fmt.Errorf("replay client: %w", err)
	}
	defer conn.Close()

	h := hub.New(hub.WithBuffer(16), hub.WithMetrics(m))
	var sender alerts.Sender = push.NewFake(cfg.FakePushFile)
	if cfg.PushSender == "expo" {
		sender = &push.Expo{AccessToken: cfg.ExpoAccessToken}
	}
	engine := alerts.NewEngine(alerts.DefaultRules(cfg), st, st, sender, h, alerts.WithCounters(m.AlertsActive, m.Push))
	gen := generator.NewExec(cfg.RepoDir, cfg.GeneratorPython, cfg.GeneratorScript, "logs/generator.log")
	signer := auth.NewSigner(cfg.JWTSecret, cfg.AccessTTL)
	latest := &api.Latest{}

	deps := api.Deps{Cfg: cfg, Store: st, Hub: h, Engine: engine, Replay: rc, Gen: gen, Signer: signer, Metrics: m, Latest: latest}
	if authRoutes, err := buildAuth(cfg, st, signer); err != nil {
		return err
	} else if authRoutes != nil {
		deps.Auth = authRoutes
	}

	state := collect.NewState()
	collectors := &collect.Collectors{Cfg: cfg, State: state, Replay: rc, Source: srcPool, M: m}
	go collectors.Run(ctx)
	go snapshotLoop(ctx, cfg, state, h, latest, engine, m)

	srv := &http.Server{Addr: cfg.Addr, Handler: api.New(deps), ReadHeaderTimeout: 5 * time.Second, IdleTimeout: 120 * time.Second}
	msrv := &http.Server{Addr: cfg.MetricsAddr, Handler: metricsMux(m), ReadHeaderTimeout: 5 * time.Second}
	errc := make(chan error, 2)
	ln, err := listenAPI(ctx, cfg)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", cfg.Addr, err)
	}
	go func() { errc <- srv.Serve(ln) }()
	go func() { errc <- msrv.ListenAndServe() }()
	slog.Info("oncall-api listening", "addr", cfg.Addr, "metrics", cfg.MetricsAddr, "push_sender", cfg.PushSender, "max_rate", cfg.MaxRate)

	select {
	case <-ctx.Done():
	case err := <-errc:
		if !errors.Is(err, http.ErrServerClosed) {
			return err
		}
	}
	slog.Info("shutting down")
	sctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = srv.Shutdown(sctx)
	_ = msrv.Shutdown(sctx)
	_, _ = gen.Stop()
	return nil
}

func metricsMux(m *metrics.M) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/metrics", m.Handler())
	return mux
}

// snapshotLoop is the single producer: build one snapshot per tick, hand it to the hub (non-blocking fan-out),
// and evaluate alerts on a separate goroutine so a slow database or push call never delays the next tick.
func snapshotLoop(ctx context.Context, cfg config.Config, state *collect.State, h *hub.Hub, latest *api.Latest, engine *alerts.Engine, m *metrics.M) {
	evalCh := make(chan snapshot.Snapshot, 1)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case s := <-evalCh:
				engine.Evaluate(ctx, s)
			}
		}
	}()
	t := time.NewTicker(cfg.SnapshotInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-t.C:
			start := time.Now()
			snap := state.Build(now)
			seq := h.PublishSnapshot(func(seq uint64) any { snap.Seq = seq; return snap })
			snap.Seq = seq
			latest.Set(snap)
			m.SnapshotPublished(time.Since(start).Seconds())
			m.SnapshotValues(snap.LagTotal, snap.Parity.State)
			select { // latest wins: drop the stale pending evaluation, never block
			case evalCh <- snap:
			default:
				select {
				case <-evalCh:
				default:
				}
				select {
				case evalCh <- snap:
				default:
				}
			}
		}
	}
}
