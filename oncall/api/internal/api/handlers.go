package api

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	pb "github.com/aditya388182/payments-cdc-pipeline/oncall/api/gen/oncall/v1"
	"github.com/aditya388182/payments-cdc-pipeline/oncall/api/internal/alerts"
	"github.com/aditya388182/payments-cdc-pipeline/oncall/api/internal/auth"
	"github.com/aditya388182/payments-cdc-pipeline/oncall/api/internal/generator"
	"github.com/aditya388182/payments-cdc-pipeline/oncall/api/internal/store"
	"github.com/go-chi/chi/v5"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *server) live(w http.ResponseWriter, r *http.Request) {
	opts := s.WS
	if len(opts.OriginPatterns) == 0 {
		opts.OriginPatterns = OriginPatterns(s.Cfg.CORSOrigins)
	}
	s.Hub.ServeWS(w, r, opts)
}

func (s *server) me(w http.ResponseWriter, r *http.Request) {
	p, _ := auth.PrincipalFrom(r.Context())
	writeJSON(w, 200, map[string]string{"login": p.Login, "role": p.Role})
}

func (s *server) getSnapshot(w http.ResponseWriter, r *http.Request) {
	if s.Latest != nil {
		if snap, ok := s.Latest.Get(); ok {
			writeJSON(w, 200, snap)
			return
		}
	}
	writeError(w, http.StatusServiceUnavailable, "no_snapshot", "no snapshot has been built yet")
}

// ---- alerts ----------------------------------------------------------------------------------

func (s *server) listAlerts(w http.ResponseWriter, r *http.Request) {
	var states []string
	if v := r.URL.Query().Get("state"); v != "" {
		for _, st := range strings.Split(v, ",") {
			switch st {
			case alerts.StateFiring, alerts.StateAcknowledged, alerts.StateResolved:
				states = append(states, st)
			default:
				writeError(w, http.StatusBadRequest, "bad_request", "state must be firing, acknowledged or resolved")
				return
			}
		}
	}
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	list, err := s.Store.List(r.Context(), states, limit)
	if err != nil {
		slog.Error("list alerts", "err", err.Error())
		writeError(w, http.StatusInternalServerError, "internal", "could not list alerts")
		return
	}
	if list == nil {
		list = []alerts.Alert{}
	}
	writeJSON(w, 200, map[string]any{"alerts": list})
}

func (s *server) ackAlert(w http.ResponseWriter, r *http.Request) {
	p, _ := auth.PrincipalFrom(r.Context())
	id, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil || id <= 0 {
		writeError(w, http.StatusBadRequest, "bad_request", "alert id must be a positive integer")
		return
	}
	started := time.Now().UTC()
	a, err := s.Engine.Ack(r.Context(), id, p.Login)
	entry := store.AuditEntry{Actor: p.Login, Role: p.Role, Action: "alert.ack", Target: fmt.Sprintf("alert:%d", id), RequestedAt: started, Result: "ok"}
	switch {
	case errors.Is(err, alerts.ErrNotFound):
		entry.Result, entry.Error = "not_found", err.Error()
		s.audit(r.Context(), entry)
		writeError(w, http.StatusNotFound, "not_found", "alert not found")
	case errors.Is(err, alerts.ErrNotAckable):
		entry.Result, entry.Error = "conflict", err.Error()
		s.audit(r.Context(), entry)
		writeError(w, http.StatusConflict, "not_ackable", "only a firing alert can be acknowledged")
	case err != nil:
		entry.Result, entry.Error = "error", err.Error()
		s.audit(r.Context(), entry)
		slog.Error("ack alert", "err", err.Error())
		writeError(w, http.StatusInternalServerError, "internal", "could not acknowledge the alert")
	default:
		s.audit(r.Context(), entry)
		writeJSON(w, 200, a)
	}
}

// ---- DLQ -------------------------------------------------------------------------------------

type dlqRecordJSON struct {
	ID              string `json:"id"`
	TransactionID   string `json:"transaction_id"`
	MerchantID      string `json:"merchant_id"`
	AmountMinor     int64  `json:"amount_minor"`
	Currency        string `json:"currency"`
	Status          string `json:"status"`
	EventType       string `json:"event_type"`
	Op              string `json:"op"`
	LSN             int64  `json:"lsn"`
	RejectionReason string `json:"rejection_reason"`
	RejectedAt      string `json:"rejected_at"`
	Applied         bool   `json:"applied"`
	Payload         string `json:"payload_json,omitempty"`
}

func toDLQ(r *pb.DlqRecord, withPayload bool) dlqRecordJSON {
	d := dlqRecordJSON{ID: r.Id, TransactionID: r.TransactionId, MerchantID: r.MerchantId, AmountMinor: r.AmountMinor, Currency: r.Currency,
		Status: r.Status, EventType: r.EventType, Op: r.Op, LSN: r.Lsn, RejectionReason: r.RejectionReason, RejectedAt: r.RejectedAt, Applied: r.Applied}
	if withPayload {
		d.Payload = r.PayloadJson
	}
	return d
}

var reasonRe = regexp.MustCompile(`^[A-Z_,]{0,64}$`)

func (s *server) listDLQ(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	reason := strings.ToUpper(q.Get("reason"))
	if !reasonRe.MatchString(reason) {
		writeError(w, http.StatusBadRequest, "bad_request", "reason must be upper-case letters and underscores")
		return
	}
	cursor := q.Get("cursor")
	if cursor != "" {
		if _, err := strconv.ParseUint(cursor, 10, 63); err != nil {
			writeError(w, http.StatusBadRequest, "bad_request", "cursor must be a number")
			return
		}
	}
	limit, _ := strconv.Atoi(q.Get("limit"))
	if limit <= 0 || limit > 100 {
		limit = 25
	}
	resp, err := s.Replay.ListDlq(r.Context(), cursor, int32(limit), reason)
	if err != nil {
		s.upstreamError(w, err)
		return
	}
	recs := make([]dlqRecordJSON, 0, len(resp.Records))
	for _, rec := range resp.Records {
		recs = append(recs, toDLQ(rec, false))
	}
	writeJSON(w, 200, map[string]any{"records": recs, "next_cursor": resp.NextCursor, "topic_messages": resp.TopicMessages, "open_total": resp.OpenTotal})
}

var idRe = regexp.MustCompile(`^[0-9]{1,18}$`)

func (s *server) getDLQ(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	if !idRe.MatchString(id) {
		writeError(w, http.StatusBadRequest, "bad_request", "dlq id must be a number")
		return
	}
	resp, err := s.Replay.GetDlq(r.Context(), id)
	if err != nil {
		s.upstreamError(w, err)
		return
	}
	if !resp.Found {
		writeError(w, http.StatusNotFound, "not_found", "no such DLQ record")
		return
	}
	writeJSON(w, 200, toDLQ(resp.Record, true))
}

// upstreamError maps a gRPC failure from replay-svc to an HTTP error without leaking internals.
func (s *server) upstreamError(w http.ResponseWriter, err error) {
	st, code, msg := MapGRPCError(err)
	slog.Warn("replay-svc call failed", "grpc_code", status.Code(err).String(), "err", err.Error())
	writeError(w, st, code, msg)
}

// MapGRPCError: UNAVAILABLE 503, DEADLINE_EXCEEDED 504, INVALID_ARGUMENT 400, NOT_FOUND 404, everything else 502.
func MapGRPCError(err error) (httpStatus int, code, message string) {
	switch status.Code(err) {
	case codes.Unavailable:
		return http.StatusServiceUnavailable, "replay_unavailable", "the replay service is unavailable; retry shortly"
	case codes.DeadlineExceeded:
		return http.StatusGatewayTimeout, "replay_timeout", "the replay service did not answer in time"
	case codes.InvalidArgument:
		return http.StatusBadRequest, "bad_request", "the replay service rejected the request"
	case codes.NotFound:
		return http.StatusNotFound, "not_found", "not found"
	case codes.Canceled:
		return 499, "canceled", "request canceled"
	default:
		return http.StatusBadGateway, "upstream_error", "unexpected error from the replay service"
	}
}

// ReplayHTTPStatus maps a replay outcome to an HTTP status.
func ReplayHTTPStatus(st pb.ReplayStatus) int {
	switch st {
	case pb.ReplayStatus_REPLAY_STATUS_APPLIED, pb.ReplayStatus_REPLAY_STATUS_ALREADY_APPLIED:
		return http.StatusOK
	case pb.ReplayStatus_REPLAY_STATUS_PENDING:
		return http.StatusAccepted
	case pb.ReplayStatus_REPLAY_STATUS_STILL_INVALID:
		return http.StatusUnprocessableEntity
	case pb.ReplayStatus_REPLAY_STATUS_NOT_FOUND:
		return http.StatusNotFound
	}
	return http.StatusBadGateway
}

// ---- idempotent admin actions ----------------------------------------------------------------

var idemKeyRe = regexp.MustCompile(`^[A-Za-z0-9._:-]{8,128}$`)

type idemResult struct {
	status int
	body   []byte
	keep   bool // store the result for replay of the same key (false: free the key so a retry can run again)
}

// idempotent runs fn at most once per (actor, Idempotency-Key). A repeat with the same request returns the first
// result (header Idempotent-Replayed: true); the same key for a different request is 422; a key whose request is
// still running is 409. The key is required (400 without it).
func (s *server) idempotent(w http.ResponseWriter, r *http.Request, p auth.Principal, routeKey string, fn func(ctx context.Context) idemResult) {
	key := r.Header.Get("Idempotency-Key")
	if key == "" {
		writeError(w, http.StatusBadRequest, "idempotency_key_required", "this action requires an Idempotency-Key header")
		return
	}
	if !idemKeyRe.MatchString(key) {
		writeError(w, http.StatusBadRequest, "idempotency_key_invalid", "Idempotency-Key must be 8-128 characters of letters, digits and ._:-")
		return
	}
	h := sha256.Sum256([]byte(routeKey))
	state, stored, err := s.Store.IdempotencyBegin(r.Context(), p.Login, key, routeKey, hex.EncodeToString(h[:]))
	if err != nil {
		slog.Error("idempotency begin", "err", err.Error())
		writeError(w, http.StatusInternalServerError, "internal", "could not process the request")
		return
	}
	switch state {
	case store.IdemReplayed:
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Idempotent-Replayed", "true")
		w.WriteHeader(stored.Status)
		_, _ = w.Write(stored.Body)
		return
	case store.IdemInProgress:
		writeError(w, http.StatusConflict, "request_in_progress", "a request with this Idempotency-Key is still being processed")
		return
	case store.IdemConflict:
		writeError(w, http.StatusUnprocessableEntity, "idempotency_key_reused", "this Idempotency-Key was already used for a different request")
		return
	}
	res := fn(r.Context())
	bg, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), 5*time.Second)
	defer cancel()
	if res.keep {
		if err := s.Store.IdempotencyFinish(bg, p.Login, key, res.status, res.body); err != nil {
			slog.Error("idempotency finish", "err", err.Error())
		}
	} else if err := s.Store.IdempotencyAbort(bg, p.Login, key); err != nil {
		slog.Error("idempotency abort", "err", err.Error())
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(res.status)
	_, _ = w.Write(res.body)
}

func jsonBody(v any) []byte {
	b, _ := marshal(v)
	return b
}

func (s *server) audit(ctx context.Context, e store.AuditEntry) {
	bg, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if err := s.Store.AddAudit(bg, e); err != nil {
		// the action already happened; do not hide that, but do not fail the response either
		slog.Error("AUDIT WRITE FAILED", "actor", e.Actor, "action", e.Action, "target", e.Target, "err", err.Error())
	}
}

func (s *server) replay(w http.ResponseWriter, r *http.Request) {
	p, _ := auth.PrincipalFrom(r.Context())
	id := chi.URLParam(r, "id")
	if !idRe.MatchString(id) {
		writeError(w, http.StatusBadRequest, "bad_request", "dlq id must be a number")
		return
	}
	key := r.Header.Get("Idempotency-Key")
	started := time.Now().UTC()
	s.idempotent(w, r, p, "POST /v1/dlq/"+id+"/replay", func(ctx context.Context) idemResult {
		resp, err := s.Replay.Replay(ctx, id, p.Login, key)
		entry := store.AuditEntry{Actor: p.Login, Role: p.Role, Action: "dlq.replay", Target: "dlq:" + id, RequestedAt: started, IdempotencyKey: key}
		if err != nil {
			st, code, msg := MapGRPCError(err)
			entry.Result, entry.Error = "error", err.Error()
			s.audit(ctx, entry)
			s.Metrics.Replay("error")
			return idemResult{status: st, body: jsonBody(map[string]any{"error": map[string]string{"code": code, "message": msg}}), keep: false}
		}
		name := strings.TrimPrefix(resp.Status.String(), "REPLAY_STATUS_")
		entry.Result = name
		entry.Details = map[string]any{"detail": resp.Detail, "lake_lsn": resp.LakeLsn}
		s.audit(ctx, entry)
		s.Metrics.Replay(name)
		body := jsonBody(map[string]any{"dlq_id": id, "status": name, "detail": resp.Detail, "lake_lsn": resp.LakeLsn})
		// PENDING means "published, not visible yet": free the key so the same request can be retried to completion
		return idemResult{status: ReplayHTTPStatus(resp.Status), body: body, keep: resp.Status != pb.ReplayStatus_REPLAY_STATUS_PENDING}
	})
}

// ---- generator -------------------------------------------------------------------------------

func (s *server) generatorStatus(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, 200, map[string]any{"generator": s.Gen.Status(), "max_rate": s.Cfg.MaxRate})
}

type generatorReq struct {
	Action string `json:"action"`
	Rate   int    `json:"rate"`
}

func (s *server) generatorControl(w http.ResponseWriter, r *http.Request) {
	p, _ := auth.PrincipalFrom(r.Context())
	var req generatorReq
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", errBadJSON.Error())
		return
	}
	switch req.Action {
	case "start":
		if req.Rate < 1 || req.Rate > s.Cfg.MaxRate {
			writeError(w, http.StatusUnprocessableEntity, "rate_out_of_range", fmt.Sprintf("rate must be between 1 and %d", s.Cfg.MaxRate))
			return
		}
	case "stop":
	default:
		writeError(w, http.StatusBadRequest, "bad_request", `action must be "start" or "stop"`)
		return
	}
	key := r.Header.Get("Idempotency-Key")
	started := time.Now().UTC()
	routeKey := fmt.Sprintf("POST /v1/generator %s %d", req.Action, req.Rate)
	s.idempotent(w, r, p, routeKey, func(ctx context.Context) idemResult {
		entry := store.AuditEntry{Actor: p.Login, Role: p.Role, Action: "generator." + req.Action, Target: "generator", RequestedAt: started,
			IdempotencyKey: key, Details: map[string]any{"rate": req.Rate}}
		var st generator.Status
		var err error
		if req.Action == "start" {
			st, err = s.Gen.Start(req.Rate)
		} else {
			st, err = s.Gen.Stop()
		}
		switch {
		case errors.Is(err, generator.ErrAlreadyRunning), errors.Is(err, generator.ErrNotRunning):
			entry.Result, entry.Error = "conflict", err.Error()
			s.audit(ctx, entry)
			return idemResult{status: http.StatusConflict, keep: true, body: jsonBody(map[string]any{"error": map[string]string{"code": "generator_state", "message": err.Error()}})}
		case err != nil:
			entry.Result, entry.Error = "error", err.Error()
			s.audit(ctx, entry)
			slog.Error("generator control", "err", err.Error())
			return idemResult{status: http.StatusInternalServerError, keep: false, body: jsonBody(map[string]any{"error": map[string]string{"code": "internal", "message": "could not control the generator"}})}
		}
		entry.Result = "ok"
		s.audit(ctx, entry)
		return idemResult{status: 200, keep: true, body: jsonBody(map[string]any{"generator": st})}
	})
}

// ---- devices & audit -------------------------------------------------------------------------

var expoTokenRe = regexp.MustCompile(`^(Expo|Exponent)PushToken\[[A-Za-z0-9_-]{8,}\]$`)

func (s *server) registerDevice(w http.ResponseWriter, r *http.Request) {
	p, _ := auth.PrincipalFrom(r.Context())
	var req struct {
		Token    string `json:"expo_push_token"`
		Platform string `json:"platform"`
	}
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "bad_request", errBadJSON.Error())
		return
	}
	if !expoTokenRe.MatchString(req.Token) {
		writeError(w, http.StatusUnprocessableEntity, "invalid_token", "expo_push_token must look like ExponentPushToken[...]")
		return
	}
	if len(req.Platform) > 16 {
		req.Platform = req.Platform[:16]
	}
	if err := s.Store.UpsertDevice(r.Context(), req.Token, p.Login, req.Platform); err != nil {
		slog.Error("register device", "err", err.Error())
		writeError(w, http.StatusInternalServerError, "internal", "could not register the device")
		return
	}
	writeJSON(w, 200, map[string]any{"registered": true})
}

func (s *server) unregisterDevice(w http.ResponseWriter, r *http.Request) {
	p, _ := auth.PrincipalFrom(r.Context())
	var req struct {
		Token string `json:"expo_push_token"`
	}
	if err := decodeJSON(r, &req); err != nil || req.Token == "" {
		writeError(w, http.StatusBadRequest, "bad_request", errBadJSON.Error())
		return
	}
	owner := p.Login
	if p.IsAdmin() {
		owner = "" // admins may remove any device
	}
	ok, err := s.Store.RemoveDeviceFor(r.Context(), req.Token, owner)
	if err != nil {
		slog.Error("unregister device", "err", err.Error())
		writeError(w, http.StatusInternalServerError, "internal", "could not remove the device")
		return
	}
	if !ok {
		writeError(w, http.StatusNotFound, "not_found", "no such device")
		return
	}
	writeJSON(w, 200, map[string]any{"registered": false})
}

func (s *server) listAudit(w http.ResponseWriter, r *http.Request) {
	cursor, _ := strconv.ParseInt(r.URL.Query().Get("cursor"), 10, 64)
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	entries, next, err := s.Store.ListAudit(r.Context(), cursor, limit)
	if err != nil {
		slog.Error("list audit", "err", err.Error())
		writeError(w, http.StatusInternalServerError, "internal", "could not read the audit log")
		return
	}
	if entries == nil {
		entries = []store.AuditEntry{}
	}
	nc := ""
	if next > 0 {
		nc = strconv.FormatInt(next, 10)
	}
	writeJSON(w, 200, map[string]any{"entries": entries, "next_cursor": nc})
}
