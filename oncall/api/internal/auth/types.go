package auth

import (
	"context"
	"errors"
	"time"
)

// StateData is what the server remembers between /start and /callback (CSRF state bound to the app's PKCE challenge).
type StateData struct {
	CodeChallenge string
	RedirectURI   string
	ExpiresAt     time.Time
}

// CodeData is a one-time authorization code issued to the app, bound to its PKCE challenge.
type CodeData struct {
	Login         string
	Role          string
	CodeChallenge string
	RedirectURI   string
	ExpiresAt     time.Time
}

// RefreshRecord is one link in a rotating refresh-token family. Only the hash of the token is stored.
type RefreshRecord struct {
	ID        string
	FamilyID  string
	Login     string
	Role      string
	TokenHash string
	ExpiresAt time.Time
}

type RotateResult int

const (
	Rotated  RotateResult = iota
	NotFound              // unknown token
	Reused                // the token was already rotated (or revoked): the whole family has been revoked
	Expired
	Revoked // the token's family was already revoked (logout, or an earlier reuse): nothing more to revoke
)

// Store is the persistence of the flow. The Postgres implementation makes every consume/rotate atomic.
type Store interface {
	SaveState(ctx context.Context, stateHash string, s StateData) error
	// ConsumeState deletes the state and returns it if it existed and had not expired.
	ConsumeState(ctx context.Context, stateHash string, now time.Time) (StateData, bool, error)
	SaveCode(ctx context.Context, codeHash string, c CodeData) error
	// ConsumeCode marks the code used and returns it if it was unused and unexpired. A code can be consumed once.
	ConsumeCode(ctx context.Context, codeHash string, now time.Time) (CodeData, bool, error)
	CreateRefresh(ctx context.Context, r RefreshRecord) error
	// RotateRefresh atomically exchanges the presented token for next. If the presented token was already used or
	// rotated it revokes the entire family and returns Reused; a token whose family is already revoked is Revoked.
	RotateRefresh(ctx context.Context, presentedHash string, next RefreshRecord, now time.Time) (old RefreshRecord, res RotateResult, err error)
	RevokeFamily(ctx context.Context, familyID string, now time.Time) error
}

// AuditEvent is recorded for security-relevant events (login, denied login, refresh reuse, logout).
type AuditEvent struct {
	Actor  string
	Role   string
	Action string
	Result string
	Error  string
	Detail map[string]any
}

type Auditor interface {
	Audit(ctx context.Context, e AuditEvent)
}

var (
	ErrInvalidGrant = errors.New("invalid_grant")
)
