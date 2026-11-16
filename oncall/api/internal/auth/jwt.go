// Package auth: access-token (JWT) signing and verification, role checks, and the GitHub OAuth + PKCE flow.
package auth

import (
	"errors"
	"fmt"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

const (
	RoleViewer = "viewer"
	RoleAdmin  = "admin"
	issuer     = "oncall-api"
)

var (
	ErrInvalidToken = errors.New("invalid access token")
	ErrExpired      = errors.New("access token expired")
)

// Principal is the authenticated caller.
type Principal struct {
	Login string
	Role  string
	// Family is the refresh-token family this access token came from (empty for dev tokens).
	Family string
}

func (p Principal) IsAdmin() bool { return p.Role == RoleAdmin }

type claims struct {
	Role   string `json:"role"`
	Family string `json:"fam,omitempty"`
	jwt.RegisteredClaims
}

// Signer issues and verifies HS256 access tokens.
type Signer struct {
	secret []byte
	ttl    time.Duration
	now    func() time.Time
}

func NewSigner(secret string, ttl time.Duration) *Signer {
	return &Signer{secret: []byte(secret), ttl: ttl, now: time.Now}
}

// WithClock is for tests.
func (s *Signer) WithClock(f func() time.Time) *Signer { s.now = f; return s }

func (s *Signer) Issue(p Principal) (string, time.Time, error) {
	if p.Role != RoleViewer && p.Role != RoleAdmin {
		return "", time.Time{}, fmt.Errorf("unknown role %q", p.Role)
	}
	now := s.now()
	exp := now.Add(s.ttl)
	c := claims{Role: p.Role, Family: p.Family, RegisteredClaims: jwt.RegisteredClaims{
		Subject: p.Login, Issuer: issuer, IssuedAt: jwt.NewNumericDate(now), ExpiresAt: jwt.NewNumericDate(exp), ID: randomID(),
	}}
	tok, err := jwt.NewWithClaims(jwt.SigningMethodHS256, c).SignedString(s.secret)
	return tok, exp, err
}

// Verify checks signature (HS256 only), issuer and expiry.
func (s *Signer) Verify(token string) (Principal, error) {
	var c claims
	parsed, err := jwt.ParseWithClaims(token, &c, func(t *jwt.Token) (any, error) { return s.secret, nil },
		jwt.WithValidMethods([]string{"HS256"}), jwt.WithIssuer(issuer), jwt.WithExpirationRequired(),
		jwt.WithTimeFunc(s.now))
	if err != nil {
		if errors.Is(err, jwt.ErrTokenExpired) {
			return Principal{}, ErrExpired
		}
		return Principal{}, ErrInvalidToken
	}
	if !parsed.Valid || c.Subject == "" || (c.Role != RoleViewer && c.Role != RoleAdmin) {
		return Principal{}, ErrInvalidToken
	}
	return Principal{Login: c.Subject, Role: c.Role, Family: c.Family}, nil
}
