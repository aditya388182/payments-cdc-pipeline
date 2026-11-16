package auth

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
)

// Settings configures the GitHub flow.
type Settings struct {
	PublicURL        string // this API's externally visible base URL (for the GitHub callback)
	GitHubBase       string // https://github.com or the mock
	GitHubAPIBase    string // https://api.github.com or the mock
	ClientID         string
	ClientSecret     string
	Allowlist        map[string]bool // lower-case GitHub logins allowed to sign in
	Admins           map[string]bool // subset of the allowlist that gets the admin role
	AllowedRedirects []string        // e.g. "oncall://", "exp://", "http://127.0.0.1"
	RefreshTTL       time.Duration
	StateTTL         time.Duration // default 10 min
	CodeTTL          time.Duration // default 2 min
}

type Handler struct {
	S      Settings
	Store  Store
	Signer *Signer
	Audit  Auditor
	HTTP   *http.Client
	Now    func() time.Time
}

func (h *Handler) now() time.Time {
	if h.Now != nil {
		return h.Now()
	}
	return time.Now()
}

func (h *Handler) httpc() *http.Client {
	if h.HTTP != nil {
		return h.HTTP
	}
	return &http.Client{Timeout: 5 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
}

func (h *Handler) audit(ctx context.Context, e AuditEvent) {
	if h.Audit != nil {
		h.Audit.Audit(ctx, e)
	}
}

// Mount registers the /auth routes on the /v1 router.
func (h *Handler) Mount(r chi.Router) {
	r.Get("/auth/github/start", h.start)
	r.Get("/auth/github/callback", h.callback)
	r.Post("/auth/token", h.token)
	r.With(Authenticate(h.Signer, false)).Post("/auth/logout", h.logout)
}

var challengeRe = regexp.MustCompile(`^[A-Za-z0-9_-]{43,128}$`)

// RedirectAllowed: custom schemes (oncall://, exp://) match on scheme; http(s) entries match scheme + exact host
// (any port), so "http://127.0.0.1" does not allow "http://127.0.0.1.evil.example". No userinfo or fragment.
func (s Settings) RedirectAllowed(raw string) bool {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme == "" || u.User != nil || u.Fragment != "" {
		return false
	}
	for _, a := range s.AllowedRedirects {
		au, err := url.Parse(a)
		if err != nil || au.Scheme != u.Scheme {
			continue
		}
		if au.Hostname() == "" { // custom-scheme entry like "oncall://"
			return true
		}
		if strings.EqualFold(au.Hostname(), u.Hostname()) {
			return true
		}
	}
	return false
}

func writeError(w http.ResponseWriter, status int, code, msg string) {
	writeErr(w, status, code, msg)
}

func (h *Handler) start(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	challenge, redirect := q.Get("code_challenge"), q.Get("redirect_uri")
	if m := q.Get("code_challenge_method"); m != "" && m != "S256" {
		writeError(w, http.StatusBadRequest, "invalid_request", "only code_challenge_method=S256 is supported")
		return
	}
	if !challengeRe.MatchString(challenge) {
		writeError(w, http.StatusBadRequest, "invalid_request", "code_challenge must be a base64url SHA-256 (43-128 characters)")
		return
	}
	if !h.S.RedirectAllowed(redirect) {
		writeError(w, http.StatusBadRequest, "invalid_request", "redirect_uri is not allowed")
		return
	}
	state := RandomToken(32)
	ttl := orDefault(h.S.StateTTL, 10*time.Minute)
	if err := h.Store.SaveState(r.Context(), HashToken(state), StateData{CodeChallenge: challenge, RedirectURI: redirect, ExpiresAt: h.now().Add(ttl)}); err != nil {
		slog.Error("save oauth state", "err", err.Error())
		writeError(w, http.StatusInternalServerError, "internal", "could not start sign-in")
		return
	}
	authz := url.Values{}
	authz.Set("client_id", h.S.ClientID)
	authz.Set("redirect_uri", strings.TrimRight(h.S.PublicURL, "/")+"/v1/auth/github/callback")
	authz.Set("state", state)
	authz.Set("scope", "read:user")
	http.Redirect(w, r, strings.TrimRight(h.S.GitHubBase, "/")+"/login/oauth/authorize?"+authz.Encode(), http.StatusFound)
}

func orDefault(d, def time.Duration) time.Duration {
	if d == 0 {
		return def
	}
	return d
}

// redirectToApp sends the browser back to the app's deep link with either a one-time code or an error.
func redirectToApp(w http.ResponseWriter, r *http.Request, base string, params map[string]string) {
	u, err := url.Parse(base)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "bad redirect")
		return
	}
	q := u.Query()
	for k, v := range params {
		q.Set(k, v)
	}
	u.RawQuery = q.Encode()
	http.Redirect(w, r, u.String(), http.StatusFound)
}

func (h *Handler) callback(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	state, code := q.Get("state"), q.Get("code")
	if state == "" {
		writeError(w, http.StatusBadRequest, "invalid_state", "missing state")
		return
	}
	sd, ok, err := h.Store.ConsumeState(r.Context(), HashToken(state), h.now())
	if err != nil {
		slog.Error("consume oauth state", "err", err.Error())
		writeError(w, http.StatusInternalServerError, "internal", "could not complete sign-in")
		return
	}
	if !ok { // unknown, already used, or expired: never redirect anywhere on a bad state (CSRF / replay)
		h.audit(r.Context(), AuditEvent{Actor: "", Action: "auth.login", Result: "invalid_state", Error: "state unknown, used or expired"})
		writeError(w, http.StatusBadRequest, "invalid_state", "sign-in state is invalid or expired; start again")
		return
	}
	if e := q.Get("error"); e != "" {
		redirectToApp(w, r, sd.RedirectURI, map[string]string{"error": "access_denied"})
		return
	}
	if code == "" {
		redirectToApp(w, r, sd.RedirectURI, map[string]string{"error": "invalid_request"})
		return
	}
	login, err := h.githubLogin(r.Context(), code)
	if err != nil {
		slog.Warn("github exchange failed", "err", err.Error())
		h.audit(r.Context(), AuditEvent{Action: "auth.login", Result: "exchange_failed", Error: err.Error()})
		redirectToApp(w, r, sd.RedirectURI, map[string]string{"error": "exchange_failed"})
		return
	}
	lc := strings.ToLower(login)
	if !h.S.Allowlist[lc] {
		h.audit(r.Context(), AuditEvent{Actor: login, Action: "auth.login", Result: "not_allowed", Error: "GitHub user is not on the allowlist"})
		redirectToApp(w, r, sd.RedirectURI, map[string]string{"error": "not_allowed"})
		return
	}
	role := RoleViewer
	if h.S.Admins[lc] {
		role = RoleAdmin
	}
	oneTime := RandomToken(32)
	if err := h.Store.SaveCode(r.Context(), HashToken(oneTime), CodeData{Login: lc, Role: role, CodeChallenge: sd.CodeChallenge, RedirectURI: sd.RedirectURI,
		ExpiresAt: h.now().Add(orDefault(h.S.CodeTTL, 2*time.Minute))}); err != nil {
		slog.Error("save auth code", "err", err.Error())
		writeError(w, http.StatusInternalServerError, "internal", "could not complete sign-in")
		return
	}
	redirectToApp(w, r, sd.RedirectURI, map[string]string{"code": oneTime})
}

// githubLogin exchanges the GitHub code (with the client secret, which never leaves the server) and reads the login.
// The GitHub access token is used once for GET /user and discarded.
func (h *Handler) githubLogin(ctx context.Context, code string) (string, error) {
	form := url.Values{"client_id": {h.S.ClientID}, "client_secret": {h.S.ClientSecret}, "code": {code},
		"redirect_uri": {strings.TrimRight(h.S.PublicURL, "/") + "/v1/auth/github/callback"}}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(h.S.GitHubBase, "/")+"/login/oauth/access_token", strings.NewReader(form.Encode()))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")
	resp, err := h.httpc().Do(req)
	if err != nil {
		return "", fmt.Errorf("token exchange: %w", err)
	}
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	resp.Body.Close()
	var tok struct {
		AccessToken string `json:"access_token"`
		Error       string `json:"error"`
	}
	if err := json.Unmarshal(body, &tok); err != nil || resp.StatusCode != http.StatusOK || tok.AccessToken == "" {
		return "", fmt.Errorf("token exchange rejected (HTTP %d %s)", resp.StatusCode, tok.Error)
	}
	ureq, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(h.S.GitHubAPIBase, "/")+"/user", nil)
	if err != nil {
		return "", err
	}
	ureq.Header.Set("Authorization", "Bearer "+tok.AccessToken)
	ureq.Header.Set("Accept", "application/vnd.github+json")
	uresp, err := h.httpc().Do(ureq)
	if err != nil {
		return "", fmt.Errorf("user lookup: %w", err)
	}
	defer uresp.Body.Close()
	ub, _ := io.ReadAll(io.LimitReader(uresp.Body, 1<<20))
	var user struct {
		Login string `json:"login"`
	}
	if uresp.StatusCode != http.StatusOK || json.Unmarshal(ub, &user) != nil || user.Login == "" {
		return "", fmt.Errorf("user lookup rejected (HTTP %d)", uresp.StatusCode)
	}
	return user.Login, nil
}

type tokenRequest struct {
	Code         string `json:"code"`
	CodeVerifier string `json:"code_verifier"`
	RefreshToken string `json:"refresh_token"`
}

type tokenResponse struct {
	AccessToken  string `json:"access_token"`
	TokenType    string `json:"token_type"`
	ExpiresIn    int    `json:"expires_in"`
	RefreshToken string `json:"refresh_token"`
	User         struct {
		Login string `json:"login"`
		Role  string `json:"role"`
	} `json:"user"`
}

func (h *Handler) token(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<16)
	var req tokenRequest
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "body must be JSON: {code, code_verifier} or {refresh_token}")
		return
	}
	switch {
	case req.Code != "" && req.RefreshToken == "":
		h.codeGrant(w, r, req)
	case req.RefreshToken != "" && req.Code == "":
		h.refreshGrant(w, r, req)
	default:
		writeError(w, http.StatusBadRequest, "invalid_request", "send either {code, code_verifier} or {refresh_token}")
	}
}

// S256ForTest exposes the PKCE transform to tests that play the app's role.
func S256ForTest(v string) string { return s256(v) }

func s256(verifier string) string {
	sum := sha256.Sum256([]byte(verifier))
	return base64.RawURLEncoding.EncodeToString(sum[:])
}

func (h *Handler) codeGrant(w http.ResponseWriter, r *http.Request, req tokenRequest) {
	if len(req.CodeVerifier) < 43 || len(req.CodeVerifier) > 128 {
		writeError(w, http.StatusBadRequest, "invalid_request", "code_verifier must be 43-128 characters")
		return
	}
	// consume FIRST: a code can be tried once, so a wrong verifier cannot be brute-forced against the same code
	cd, ok, err := h.Store.ConsumeCode(r.Context(), HashToken(req.Code), h.now())
	if err != nil {
		slog.Error("consume auth code", "err", err.Error())
		writeError(w, http.StatusInternalServerError, "internal", "could not issue tokens")
		return
	}
	if !ok {
		writeError(w, http.StatusBadRequest, ErrInvalidGrant.Error(), "the code is invalid, expired or already used")
		return
	}
	if subtle.ConstantTimeCompare([]byte(s256(req.CodeVerifier)), []byte(cd.CodeChallenge)) != 1 {
		h.audit(r.Context(), AuditEvent{Actor: cd.Login, Role: cd.Role, Action: "auth.login", Result: "pkce_mismatch", Error: "code_verifier does not match the code_challenge"})
		writeError(w, http.StatusBadRequest, ErrInvalidGrant.Error(), "code_verifier does not match")
		return
	}
	p := Principal{Login: cd.Login, Role: cd.Role, Family: newUUID()}
	resp, err := h.issue(r.Context(), p, "")
	if err != nil {
		slog.Error("issue tokens", "err", err.Error())
		writeError(w, http.StatusInternalServerError, "internal", "could not issue tokens")
		return
	}
	h.audit(r.Context(), AuditEvent{Actor: p.Login, Role: p.Role, Action: "auth.login", Result: "ok"})
	writeTokens(w, resp)
}

// issue mints an access token and a refresh token in the principal's family. When prev is empty it creates the
// first token of the family; otherwise the caller already rotated atomically in the store.
func (h *Handler) issue(ctx context.Context, p Principal, prevHash string) (tokenResponse, error) {
	access, exp, err := h.Signer.Issue(p)
	if err != nil {
		return tokenResponse{}, err
	}
	refresh := RandomToken(32)
	rec := RefreshRecord{ID: newUUID(), FamilyID: p.Family, Login: p.Login, Role: p.Role, TokenHash: HashToken(refresh), ExpiresAt: h.now().Add(h.S.RefreshTTL)}
	if prevHash == "" {
		if err := h.Store.CreateRefresh(ctx, rec); err != nil {
			return tokenResponse{}, err
		}
	}
	out := tokenResponse{AccessToken: access, TokenType: "Bearer", ExpiresIn: int(exp.Sub(h.now()).Seconds()), RefreshToken: refresh}
	out.User.Login, out.User.Role = p.Login, p.Role
	return out, nil
}

func (h *Handler) refreshGrant(w http.ResponseWriter, r *http.Request, req tokenRequest) {
	now := h.now()
	hash := HashToken(req.RefreshToken)
	nextToken := RandomToken(32)
	next := RefreshRecord{ID: newUUID(), TokenHash: HashToken(nextToken), ExpiresAt: now.Add(h.S.RefreshTTL)}
	old, res, err := h.Store.RotateRefresh(r.Context(), hash, next, now)
	if err != nil {
		slog.Error("rotate refresh", "err", err.Error())
		writeError(w, http.StatusInternalServerError, "internal", "could not refresh")
		return
	}
	switch res {
	case NotFound:
		writeError(w, http.StatusUnauthorized, ErrInvalidGrant.Error(), "refresh token is invalid")
		return
	case Expired:
		writeError(w, http.StatusUnauthorized, ErrInvalidGrant.Error(), "refresh token expired; sign in again")
		return
	case Revoked:
		writeError(w, http.StatusUnauthorized, ErrInvalidGrant.Error(), "this session was revoked; sign in again")
		return
	case Reused:
		h.audit(r.Context(), AuditEvent{Actor: old.Login, Role: old.Role, Action: "auth.refresh_reuse", Result: "family_revoked",
			Error: "an already-rotated refresh token was presented; every token in its family was revoked", Detail: map[string]any{"family": old.FamilyID}})
		writeError(w, http.StatusUnauthorized, ErrInvalidGrant.Error(), "refresh token was already used; the session has been revoked, sign in again")
		return
	}
	// re-evaluate access on every refresh: removing a user from the allowlist (or demoting them) takes effect here
	lc := strings.ToLower(old.Login)
	if !h.S.Allowlist[lc] {
		_ = h.Store.RevokeFamily(r.Context(), old.FamilyID, now)
		h.audit(r.Context(), AuditEvent{Actor: old.Login, Role: old.Role, Action: "auth.refresh", Result: "not_allowed", Error: "user is no longer on the allowlist; family revoked"})
		writeError(w, http.StatusUnauthorized, ErrInvalidGrant.Error(), "this account is no longer allowed")
		return
	}
	role := RoleViewer
	if h.S.Admins[lc] {
		role = RoleAdmin
	}
	p := Principal{Login: old.Login, Role: role, Family: old.FamilyID}
	access, exp, err := h.Signer.Issue(p)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal", "could not refresh")
		return
	}
	out := tokenResponse{AccessToken: access, TokenType: "Bearer", ExpiresIn: int(exp.Sub(now).Seconds()), RefreshToken: nextToken}
	out.User.Login, out.User.Role = p.Login, p.Role
	writeTokens(w, out)
}

func writeTokens(w http.ResponseWriter, t tokenResponse) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Pragma", "no-cache")
	_ = json.NewEncoder(w).Encode(t)
}

func (h *Handler) logout(w http.ResponseWriter, r *http.Request) {
	p, _ := PrincipalFrom(r.Context())
	if p.Family != "" {
		if err := h.Store.RevokeFamily(r.Context(), p.Family, h.now()); err != nil {
			slog.Error("logout", "err", err.Error())
			writeError(w, http.StatusInternalServerError, "internal", "could not sign out")
			return
		}
	}
	h.audit(r.Context(), AuditEvent{Actor: p.Login, Role: p.Role, Action: "auth.logout", Result: "ok"})
	w.WriteHeader(http.StatusNoContent)
}

func newUUID() string {
	b := randomBytes(16)
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}
