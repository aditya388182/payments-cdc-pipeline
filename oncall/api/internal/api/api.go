// Package api is the HTTP surface of the console: chi router, JSON errors, auth middleware wiring, handlers.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/aditya388182/payments-cdc-pipeline/oncall/api/internal/alerts"
	"github.com/aditya388182/payments-cdc-pipeline/oncall/api/internal/auth"
	"github.com/aditya388182/payments-cdc-pipeline/oncall/api/internal/config"
	"github.com/aditya388182/payments-cdc-pipeline/oncall/api/internal/generator"
	"github.com/aditya388182/payments-cdc-pipeline/oncall/api/internal/hub"
	"github.com/aditya388182/payments-cdc-pipeline/oncall/api/internal/metrics"
	"github.com/aditya388182/payments-cdc-pipeline/oncall/api/internal/replayclient"
	"github.com/aditya388182/payments-cdc-pipeline/oncall/api/internal/snapshot"
	"github.com/aditya388182/payments-cdc-pipeline/oncall/api/internal/store"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
)

// Store is the persistence the handlers need (satisfied by *store.Store; tests use an in-memory fake).
type Store interface {
	alerts.Store
	alerts.Devices
	UpsertDevice(ctx context.Context, token, user, platform string) error
	RemoveDeviceFor(ctx context.Context, token, user string) (bool, error)
	AddAudit(ctx context.Context, e store.AuditEntry) error
	ListAudit(ctx context.Context, cursor int64, limit int) ([]store.AuditEntry, int64, error)
	IdempotencyBegin(ctx context.Context, actor, key, route, hash string) (store.IdemState, store.StoredResponse, error)
	IdempotencyFinish(ctx context.Context, actor, key string, status int, body []byte) error
	IdempotencyAbort(ctx context.Context, actor, key string) error
	Ping(ctx context.Context) error
}

// AuthRoutes mounts the public /v1/auth/* endpoints (OAuth + token exchange).
type AuthRoutes interface {
	Mount(r chi.Router)
}

// Latest holds the most recent snapshot for GET /v1/snapshot.
type Latest struct {
	p atomic.Pointer[snapshot.Snapshot]
}

func (l *Latest) Set(s snapshot.Snapshot) { l.p.Store(&s) }
func (l *Latest) Get() (snapshot.Snapshot, bool) {
	s := l.p.Load()
	if s == nil {
		return snapshot.Snapshot{}, false
	}
	return *s, true
}

type Deps struct {
	Cfg     config.Config
	Store   Store
	Hub     *hub.Hub
	Engine  *alerts.Engine
	Replay  replayclient.Service
	Gen     generator.Runner
	Signer  *auth.Signer
	Auth    AuthRoutes // optional (Phase 3)
	Metrics *metrics.M
	Latest  *Latest
	WS      hub.WSOptions
}

type server struct{ Deps }

func New(d Deps) http.Handler {
	s := &server{d}
	r := chi.NewRouter()
	r.Use(middleware.Recoverer, s.cors, s.accessLog) // no RealIP: it trusts spoofable X-Forwarded-For headers
	r.Use(middleware.RequestSize(1 << 20))

	r.Get("/healthz", func(w http.ResponseWriter, r *http.Request) { writeJSON(w, 200, map[string]string{"status": "ok"}) })
	r.Get("/readyz", s.readyz)

	r.Route("/v1", func(v chi.Router) {
		if s.Auth != nil {
			s.Auth.Mount(v)
		}
		// WebSocket: browsers cannot set headers, so this one route also accepts ?access_token=
		v.With(auth.Authenticate(s.Signer, true)).Get("/live", s.live)

		v.Group(func(g chi.Router) {
			g.Use(auth.Authenticate(s.Signer, false))
			g.Get("/snapshot", s.getSnapshot)
			g.Get("/alerts", s.listAlerts)
			g.Post("/alerts/{id}/ack", s.ackAlert)
			g.Get("/dlq", s.listDLQ)
			g.Get("/dlq/{id}", s.getDLQ)
			g.Post("/devices", s.registerDevice)
			g.Delete("/devices", s.unregisterDevice)
			g.Get("/generator", s.generatorStatus)
			g.Get("/me", s.me)

			g.Group(func(a chi.Router) {
				a.Use(auth.RequireAdmin)
				a.Post("/dlq/{id}/replay", s.replay)
				a.Post("/generator", s.generatorControl)
				a.Get("/audit", s.listAudit)
			})
		})
	})
	return r
}

// ---- JSON helpers ----------------------------------------------------------------------------

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, code, msg string) {
	writeJSON(w, status, map[string]any{"error": map[string]string{"code": code, "message": msg}})
}

func decodeJSON(r *http.Request, v any) error {
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return err
	}
	return nil
}

var errBadJSON = errors.New("invalid JSON body")

// ---- middleware ------------------------------------------------------------------------------

func (s *server) accessLog(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		ww := middleware.NewWrapResponseWriter(w, r.ProtoMajor)
		next.ServeHTTP(ww, r)
		route := chi.RouteContext(r.Context()).RoutePattern()
		if route == "" {
			route = "unmatched"
		}
		user := ""
		if p, ok := auth.PrincipalFrom(r.Context()); ok {
			user = p.Login
		}
		code := ww.Status()
		if code == 0 {
			code = 200
		}
		dur := time.Since(start)
		s.Metrics.HTTP(route, r.Method, statusClass(code), dur.Seconds())
		// path only: query strings can carry tokens (WebSocket access_token) and are never logged
		slog.Info("http", "method", r.Method, "route", route, "path", r.URL.Path, "status", code, "ms", dur.Milliseconds(), "user", user)
	})
}

func statusClass(code int) string {
	switch {
	case code >= 500:
		return "5xx"
	case code >= 400:
		return "4xx"
	case code >= 300:
		return "3xx"
	}
	return "2xx"
}

func (s *server) cors(next http.Handler) http.Handler {
	allowed := map[string]bool{}
	for _, o := range s.Cfg.CORSOrigins {
		allowed[o] = true
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if o := r.Header.Get("Origin"); o != "" && allowed[o] {
			h := w.Header()
			h.Set("Access-Control-Allow-Origin", o)
			h.Set("Vary", "Origin")
			h.Set("Access-Control-Allow-Headers", "Authorization, Content-Type, Idempotency-Key")
			h.Set("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS")
			h.Set("Access-Control-Expose-Headers", "Idempotent-Replayed")
			h.Set("Access-Control-Max-Age", "600")
		}
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *server) readyz(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if err := s.Store.Ping(ctx); err != nil {
		writeError(w, http.StatusServiceUnavailable, "not_ready", "database unavailable")
		return
	}
	writeJSON(w, 200, map[string]any{"status": "ready", "snapshot": s.Latest != nil && func() bool { _, ok := s.Latest.Get(); return ok }()})
}

// OriginPatterns derives the WebSocket origin allow-list from the CORS origins (host part only).
func OriginPatterns(origins []string) []string {
	var out []string
	for _, o := range origins {
		o = strings.TrimPrefix(strings.TrimPrefix(o, "http://"), "https://")
		out = append(out, o)
	}
	return out
}
