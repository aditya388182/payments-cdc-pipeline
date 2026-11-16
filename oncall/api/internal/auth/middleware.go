package auth

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
)

type ctxKey struct{}

// PrincipalFrom returns the authenticated caller stored by Authenticate.
func PrincipalFrom(ctx context.Context) (Principal, bool) {
	p, ok := ctx.Value(ctxKey{}).(Principal)
	return p, ok
}

// WithPrincipal is for tests and handlers that build a context by hand.
func WithPrincipal(ctx context.Context, p Principal) context.Context {
	return context.WithValue(ctx, ctxKey{}, p)
}

func writeErr(w http.ResponseWriter, status int, code, msg string) {
	w.Header().Set("Content-Type", "application/json")
	if status == http.StatusUnauthorized {
		w.Header().Set("WWW-Authenticate", `Bearer realm="oncall"`)
	}
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]string{"code": code, "message": msg}})
}

// Authenticate requires a valid access token: 401 without one. allowQueryToken lets a browser WebSocket pass
// the token as ?access_token= (browsers cannot set headers on a WebSocket); it is only enabled for that route.
func Authenticate(s *Signer, allowQueryToken bool) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			tok := ""
			if h := r.Header.Get("Authorization"); strings.HasPrefix(h, "Bearer ") {
				tok = strings.TrimSpace(strings.TrimPrefix(h, "Bearer "))
			} else if allowQueryToken {
				tok = r.URL.Query().Get("access_token")
			}
			if tok == "" {
				writeErr(w, http.StatusUnauthorized, "unauthorized", "missing access token")
				return
			}
			p, err := s.Verify(tok)
			switch err {
			case nil:
				next.ServeHTTP(w, r.WithContext(WithPrincipal(r.Context(), p)))
			case ErrExpired:
				writeErr(w, http.StatusUnauthorized, "token_expired", "access token expired")
			default:
				writeErr(w, http.StatusUnauthorized, "unauthorized", "invalid access token")
			}
		})
	}
}

// RequireAdmin returns 403 for an authenticated viewer.
func RequireAdmin(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p, ok := PrincipalFrom(r.Context())
		if !ok {
			writeErr(w, http.StatusUnauthorized, "unauthorized", "authentication required")
			return
		}
		if !p.IsAdmin() {
			writeErr(w, http.StatusForbidden, "forbidden", "admin role required")
			return
		}
		next.ServeHTTP(w, r)
	})
}
