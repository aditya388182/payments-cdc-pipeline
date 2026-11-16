package store

import (
	"context"
	"errors"
	"time"

	"github.com/aditya388182/payments-cdc-pipeline/oncall/api/internal/auth"
	"github.com/jackc/pgx/v5"
)

// AuthStore adapts *Store to auth.Store (kept separate so the auth package does not import the store).
type AuthStore struct{ *Store }

var _ auth.Store = AuthStore{}

func (s AuthStore) SaveState(ctx context.Context, h string, d auth.StateData) error {
	_, err := s.Pool.Exec(ctx, `insert into oauth_states (state_hash, code_challenge, redirect_uri, expires_at) values ($1,$2,$3,$4)`, h, d.CodeChallenge, d.RedirectURI, d.ExpiresAt)
	return err
}

func (s AuthStore) ConsumeState(ctx context.Context, h string, now time.Time) (auth.StateData, bool, error) {
	var d auth.StateData
	err := s.Pool.QueryRow(ctx, `delete from oauth_states where state_hash=$1 returning code_challenge, redirect_uri, expires_at`, h).Scan(&d.CodeChallenge, &d.RedirectURI, &d.ExpiresAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return auth.StateData{}, false, nil
	}
	if err != nil {
		return auth.StateData{}, false, err
	}
	if !d.ExpiresAt.After(now) {
		return auth.StateData{}, false, nil
	}
	return d, true, nil
}

func (s AuthStore) SaveCode(ctx context.Context, h string, c auth.CodeData) error {
	_, err := s.Pool.Exec(ctx, `insert into auth_codes (code_hash, login, role, code_challenge, redirect_uri, expires_at) values ($1,$2,$3,$4,$5,$6)`,
		h, c.Login, c.Role, c.CodeChallenge, c.RedirectURI, c.ExpiresAt)
	return err
}

func (s AuthStore) ConsumeCode(ctx context.Context, h string, now time.Time) (auth.CodeData, bool, error) {
	var c auth.CodeData
	err := s.Pool.QueryRow(ctx, `update auth_codes set used_at=$2 where code_hash=$1 and used_at is null and expires_at > $2
		returning login, role, code_challenge, redirect_uri, expires_at`, h, now).Scan(&c.Login, &c.Role, &c.CodeChallenge, &c.RedirectURI, &c.ExpiresAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return auth.CodeData{}, false, nil
	}
	return c, err == nil, err
}

func (s AuthStore) CreateRefresh(ctx context.Context, r auth.RefreshRecord) error {
	_, err := s.Pool.Exec(ctx, `insert into refresh_tokens (id, family_id, login, role, token_hash, expires_at) values ($1,$2,$3,$4,$5,$6)`,
		r.ID, r.FamilyID, r.Login, r.Role, r.TokenHash, r.ExpiresAt)
	return err
}

func (s AuthStore) RotateRefresh(ctx context.Context, presented string, next auth.RefreshRecord, now time.Time) (auth.RefreshRecord, auth.RotateResult, error) {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return auth.RefreshRecord{}, 0, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	var old auth.RefreshRecord
	var usedAt, revokedAt *time.Time
	err = tx.QueryRow(ctx, `select id, family_id, login, role, token_hash, expires_at, used_at, revoked_at from refresh_tokens where token_hash=$1 for update`, presented).
		Scan(&old.ID, &old.FamilyID, &old.Login, &old.Role, &old.TokenHash, &old.ExpiresAt, &usedAt, &revokedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return auth.RefreshRecord{}, auth.NotFound, nil
	}
	if err != nil {
		return auth.RefreshRecord{}, 0, err
	}
	if usedAt == nil && revokedAt != nil {
		return old, auth.Revoked, nil
	}
	if usedAt != nil {
		// reuse: someone presented a token that was already rotated. Burn the whole family.
		if _, err := tx.Exec(ctx, `update refresh_tokens set revoked_at=$2 where family_id=$1 and revoked_at is null`, old.FamilyID, now); err != nil {
			return auth.RefreshRecord{}, 0, err
		}
		if err := tx.Commit(ctx); err != nil {
			return auth.RefreshRecord{}, 0, err
		}
		return old, auth.Reused, nil
	}
	if !old.ExpiresAt.After(now) {
		return old, auth.Expired, nil
	}
	if _, err := tx.Exec(ctx, `update refresh_tokens set used_at=$2 where id=$1`, old.ID, now); err != nil {
		return auth.RefreshRecord{}, 0, err
	}
	if _, err := tx.Exec(ctx, `insert into refresh_tokens (id, family_id, login, role, token_hash, expires_at) values ($1,$2,$3,$4,$5,$6)`,
		next.ID, old.FamilyID, old.Login, old.Role, next.TokenHash, next.ExpiresAt); err != nil {
		return auth.RefreshRecord{}, 0, err
	}
	if err := tx.Commit(ctx); err != nil {
		return auth.RefreshRecord{}, 0, err
	}
	return old, auth.Rotated, nil
}

func (s AuthStore) RevokeFamily(ctx context.Context, familyID string, now time.Time) error {
	_, err := s.Pool.Exec(ctx, `update refresh_tokens set revoked_at=$2 where family_id=$1 and revoked_at is null`, familyID, now)
	return err
}

// PurgeExpiredAuth removes expired states/codes and long-dead refresh tokens.
func (s *Store) PurgeExpiredAuth(ctx context.Context, now time.Time) error {
	for _, q := range []string{
		`delete from oauth_states where expires_at < $1`,
		`delete from auth_codes where expires_at < $1`,
		`delete from refresh_tokens where expires_at < $1 - interval '7 days'`,
	} {
		if _, err := s.Pool.Exec(ctx, q, now); err != nil {
			return err
		}
	}
	return nil
}

// AuthAudit adapts the audit table to auth.Auditor.
type AuthAudit struct{ *Store }

func (a AuthAudit) Audit(ctx context.Context, e auth.AuditEvent) {
	role := e.Role
	if role == "" {
		role = "-"
	}
	actor := e.Actor
	if actor == "" {
		actor = "-"
	}
	_ = a.AddAudit(context.WithoutCancel(ctx), AuditEntry{Actor: actor, Role: role, Action: e.Action, Target: "auth", RequestedAt: time.Now().UTC(), Result: e.Result, Error: e.Error, Details: e.Detail})
}
