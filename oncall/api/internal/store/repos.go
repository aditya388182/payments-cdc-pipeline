package store

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/aditya388182/payments-cdc-pipeline/oncall/api/internal/alerts"
	"github.com/jackc/pgx/v5"
)

// ---- alerts (implements alerts.Store) ----------------------------------------------------------

const alertCols = `id, rule, severity, state, summary, details, fired_at, acked_at, acked_by, resolved_at, updated_at`

func scanAlert(row pgx.Row) (alerts.Alert, error) {
	var a alerts.Alert
	var details []byte
	if err := row.Scan(&a.ID, &a.Rule, &a.Severity, &a.State, &a.Summary, &details, &a.FiredAt, &a.AckedAt, &a.AckedBy, &a.ResolvedAt, &a.UpdatedAt); err != nil {
		return a, err
	}
	_ = json.Unmarshal(details, &a.Details)
	return a, nil
}

func (s *Store) Active(ctx context.Context) ([]alerts.Alert, error) {
	return s.queryAlerts(ctx, `select `+alertCols+` from alerts where state in ('firing','acknowledged') order by id`)
}

func (s *Store) List(ctx context.Context, states []string, limit int) ([]alerts.Alert, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	if len(states) == 0 {
		return s.queryAlerts(ctx, `select `+alertCols+` from alerts order by id desc limit $1`, limit)
	}
	return s.queryAlerts(ctx, `select `+alertCols+` from alerts where state = any($1) order by id desc limit $2`, states, limit)
}

func (s *Store) queryAlerts(ctx context.Context, q string, args ...any) ([]alerts.Alert, error) {
	rows, err := s.Pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []alerts.Alert
	for rows.Next() {
		a, err := scanAlert(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

func (s *Store) Create(ctx context.Context, a alerts.Alert) (alerts.Alert, error) {
	details, _ := json.Marshal(a.Details)
	row := s.Pool.QueryRow(ctx, `insert into alerts (rule, severity, state, summary, details, fired_at, updated_at)
		values ($1,$2,$3,$4,$5,$6,$7) returning `+alertCols, a.Rule, a.Severity, a.State, a.Summary, details, a.FiredAt, a.UpdatedAt)
	return scanAlert(row)
}

func (s *Store) Update(ctx context.Context, a alerts.Alert) error {
	tag, err := s.Pool.Exec(ctx, `update alerts set state=$2, summary=$3, acked_at=$4, acked_by=$5, resolved_at=$6, updated_at=$7 where id=$1`,
		a.ID, a.State, a.Summary, a.AckedAt, a.AckedBy, a.ResolvedAt, a.UpdatedAt)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return alerts.ErrNotFound
	}
	return nil
}

func (s *Store) Get(ctx context.Context, id int64) (alerts.Alert, error) {
	a, err := scanAlert(s.Pool.QueryRow(ctx, `select `+alertCols+` from alerts where id=$1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return a, alerts.ErrNotFound
	}
	return a, err
}

// ---- devices -----------------------------------------------------------------------------------

func (s *Store) UpsertDevice(ctx context.Context, token, user, platform string) error {
	_, err := s.Pool.Exec(ctx, `insert into devices (token, user_login, platform) values ($1,$2,$3)
		on conflict (token) do update set user_login=excluded.user_login, platform=excluded.platform, last_seen_at=now()`, token, user, platform)
	return err
}

func (s *Store) RemoveDevice(ctx context.Context, token string) error {
	_, err := s.Pool.Exec(ctx, `delete from devices where token=$1`, token)
	return err
}

// Tokens implements alerts.Devices.
func (s *Store) Tokens(ctx context.Context) ([]string, error) {
	rows, err := s.Pool.Query(ctx, `select token from devices order by created_at`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return pgx.CollectRows(rows, pgx.RowTo[string])
}

// ---- audit log ---------------------------------------------------------------------------------

type AuditEntry struct {
	ID             int64          `json:"id"`
	Actor          string         `json:"actor"`
	Role           string         `json:"role"`
	Action         string         `json:"action"`
	Target         string         `json:"target"`
	RequestedAt    time.Time      `json:"requested_at"`
	FinishedAt     time.Time      `json:"finished_at"`
	Result         string         `json:"result"`
	Error          string         `json:"error,omitempty"`
	IdempotencyKey string         `json:"idempotency_key,omitempty"`
	Details        map[string]any `json:"details,omitempty"`
}

func (s *Store) AddAudit(ctx context.Context, e AuditEntry) error {
	d, _ := json.Marshal(e.Details)
	if e.Details == nil {
		d = []byte("{}")
	}
	_, err := s.Pool.Exec(ctx, `insert into audit_log (actor, role, action, target, requested_at, result, error, idempotency_key, details)
		values ($1,$2,$3,$4,$5,$6,$7,$8,$9)`, e.Actor, e.Role, e.Action, e.Target, e.RequestedAt, e.Result, e.Error, e.IdempotencyKey, d)
	return err
}

// ListAudit returns entries newest first with id < cursor (cursor 0 = from the newest) and the next cursor
// (0 when there is nothing older).
func (s *Store) ListAudit(ctx context.Context, cursor int64, limit int) ([]AuditEntry, int64, error) {
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	if cursor <= 0 {
		cursor = 1 << 62
	}
	rows, err := s.Pool.Query(ctx, `select id, actor, role, action, target, requested_at, finished_at, result, error, idempotency_key, details
		from audit_log where id < $1 order by id desc limit $2`, cursor, limit+1)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	var out []AuditEntry
	for rows.Next() {
		var e AuditEntry
		var d []byte
		if err := rows.Scan(&e.ID, &e.Actor, &e.Role, &e.Action, &e.Target, &e.RequestedAt, &e.FinishedAt, &e.Result, &e.Error, &e.IdempotencyKey, &d); err != nil {
			return nil, 0, err
		}
		_ = json.Unmarshal(d, &e.Details)
		out = append(out, e)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, err
	}
	var next int64
	if len(out) > limit {
		out = out[:limit]
		next = out[len(out)-1].ID
	}
	return out, next, nil
}

// ---- idempotency keys --------------------------------------------------------------------------

type IdemState int

const (
	IdemFresh      IdemState = iota // first time: the caller must do the work and call IdempotencyFinish
	IdemReplayed                    // same key, same request: return the stored response
	IdemInProgress                  // same key still being processed by another request
	IdemConflict                    // same key reused for a different request
)

type StoredResponse struct {
	Status int
	Body   []byte
}

func (s *Store) IdempotencyBegin(ctx context.Context, actor, key, route, hash string) (IdemState, StoredResponse, error) {
	// a crashed request leaves status 0 behind; after two minutes it no longer blocks a retry
	if _, err := s.Pool.Exec(ctx, `delete from idempotency_keys where actor=$1 and key=$2 and status=0 and created_at < now() - interval '2 minutes'`, actor, key); err != nil {
		return 0, StoredResponse{}, err
	}
	tag, err := s.Pool.Exec(ctx, `insert into idempotency_keys (actor, key, route, request_hash) values ($1,$2,$3,$4) on conflict do nothing`, actor, key, route, hash)
	if err != nil {
		return 0, StoredResponse{}, err
	}
	if tag.RowsAffected() == 1 {
		return IdemFresh, StoredResponse{}, nil
	}
	var r, h string
	var st int
	var body []byte
	if err := s.Pool.QueryRow(ctx, `select route, request_hash, status, response from idempotency_keys where actor=$1 and key=$2`, actor, key).Scan(&r, &h, &st, &body); err != nil {
		return 0, StoredResponse{}, err
	}
	switch {
	case r != route || h != hash:
		return IdemConflict, StoredResponse{}, nil
	case st == 0:
		return IdemInProgress, StoredResponse{}, nil
	default:
		return IdemReplayed, StoredResponse{Status: st, Body: body}, nil
	}
}

func (s *Store) IdempotencyFinish(ctx context.Context, actor, key string, status int, body []byte) error {
	_, err := s.Pool.Exec(ctx, `update idempotency_keys set status=$3, response=$4 where actor=$1 and key=$2`, actor, key, status, body)
	return err
}

// IdempotencyAbort frees the key after a failure that should be retryable (5xx, still-pending replay).
func (s *Store) IdempotencyAbort(ctx context.Context, actor, key string) error {
	_, err := s.Pool.Exec(ctx, `delete from idempotency_keys where actor=$1 and key=$2`, actor, key)
	return err
}
