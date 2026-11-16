package api

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aditya388182/payments-cdc-pipeline/oncall/api/internal/alerts"
	"github.com/aditya388182/payments-cdc-pipeline/oncall/api/internal/auth"
	"github.com/aditya388182/payments-cdc-pipeline/oncall/api/internal/config"
	"github.com/aditya388182/payments-cdc-pipeline/oncall/api/internal/generator"
	"github.com/aditya388182/payments-cdc-pipeline/oncall/api/internal/hub"
	"github.com/aditya388182/payments-cdc-pipeline/oncall/api/internal/mockgithub"
)

const (
	clientSecret = "mock-client-secret-do-not-leak"
	appRedirect  = "oncall://auth/callback"
	jwtSecret    = "0123456789012345678901234567890123456789"
)

type flowEnv struct {
	api     *httptest.Server
	gh      *httptest.Server
	mock    *mockgithub.Server
	store   *auth.MemStore
	auditor *auth.MemAuditor
	now     *atomic.Int64 // unix nanos, the shared test clock
	handler *auth.Handler
}

func (f *flowEnv) advance(d time.Duration) { f.now.Add(int64(d)) }
func (f *flowEnv) clock() time.Time        { return time.Unix(0, f.now.Load()).UTC() }

func newFlow(t *testing.T, allow, admins []string) *flowEnv {
	t.Helper()
	f := &flowEnv{store: auth.NewMemStore(), auditor: &auth.MemAuditor{}, now: &atomic.Int64{}}
	f.now.Store(time.Date(2026, 11, 16, 12, 0, 0, 0, time.UTC).UnixNano())
	f.mock = mockgithub.New("mock-client-id", clientSecret, "admin-user", "viewer-user", "outsider")
	f.gh = httptest.NewServer(f.mock.Handler())
	t.Cleanup(f.gh.Close)

	set := func(l []string) map[string]bool {
		m := map[string]bool{}
		for _, s := range l {
			m[strings.ToLower(s)] = true
		}
		return m
	}
	signer := auth.NewSigner(jwtSecret, 15*time.Minute).WithClock(f.clock)
	// the API's own URL is only known after it starts; the handler reads it lazily through PublicURL below
	st := newFakeStore()
	h := hub.New()
	cfg := config.Config{MaxRate: 200, CORSOrigins: []string{"http://127.0.0.1:8081"}}
	mux := http.NewServeMux()
	f.api = httptest.NewServer(mux)
	t.Cleanup(f.api.Close)
	f.handler = &auth.Handler{
		S: auth.Settings{PublicURL: f.api.URL, GitHubBase: f.gh.URL, GitHubAPIBase: f.gh.URL, ClientID: "mock-client-id", ClientSecret: clientSecret,
			Allowlist: set(allow), Admins: set(admins), AllowedRedirects: []string{"oncall://", "exp://", "http://127.0.0.1", "http://localhost"},
			RefreshTTL: 30 * 24 * time.Hour},
		Store: f.store, Signer: signer, Audit: f.auditor, Now: f.clock,
		HTTP: &http.Client{Timeout: 5 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }},
	}
	mux.Handle("/", New(Deps{Cfg: cfg, Store: st, Hub: h, Engine: alerts.NewEngine(nil, st, st, nil, h), Replay: &fakeReplay{results: nil}, Gen: &generator.Fake{},
		Signer: signer, Auth: f.handler}))
	return f
}

var noRedirect = &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }, Timeout: 5 * time.Second}

func pkce() (verifier, challenge string) {
	verifier = auth.RandomToken(48) // 64 chars
	return verifier, base64URLSHA256(verifier)
}

func base64URLSHA256(v string) string {
	return auth.S256ForTest(v)
}

// signIn walks the browser flow and returns the one-time code the app receives (or the error redirect param).
func (f *flowEnv) signIn(t *testing.T, login, challenge, redirect string) (code, errParam string) {
	t.Helper()
	start := f.api.URL + "/v1/auth/github/start?code_challenge=" + challenge + "&code_challenge_method=S256&redirect_uri=" + url.QueryEscape(redirect)
	resp, err := noRedirect.Get(start)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 302 {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("start: %d %s", resp.StatusCode, b)
	}
	authorize := resp.Header.Get("Location")
	if strings.Contains(authorize, clientSecret) || strings.Contains(authorize, "client_secret") {
		t.Fatal("the client secret reached the browser redirect")
	}
	resp, err = noRedirect.Get(authorize + "&login=" + login) // the user clicks "sign in as <login>" on the mock page
	if err != nil {
		t.Fatal(err)
	}
	callback := resp.Header.Get("Location")
	resp, err = noRedirect.Get(callback) // browser follows GitHub's redirect to our callback
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 302 {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("callback: %d %s", resp.StatusCode, b)
	}
	back, _ := url.Parse(resp.Header.Get("Location"))
	return back.Query().Get("code"), back.Query().Get("error")
}

func (f *flowEnv) postToken(t *testing.T, body map[string]string) (int, map[string]any) {
	t.Helper()
	b, _ := json.Marshal(body)
	resp, err := http.Post(f.api.URL+"/v1/auth/token", "application/json", bytes.NewReader(b))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return resp.StatusCode, out
}

func (f *flowEnv) get(t *testing.T, path, token string) (int, string) {
	t.Helper()
	req, _ := http.NewRequest("GET", f.api.URL+path, nil)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

func (f *flowEnv) login(t *testing.T, user string) map[string]any {
	t.Helper()
	v, c := pkce()
	code, e := f.signIn(t, user, c, appRedirect)
	if code == "" {
		t.Fatalf("no code for %s (error %q)", user, e)
	}
	st, body := f.postToken(t, map[string]string{"code": code, "code_verifier": v})
	if st != 200 {
		t.Fatalf("token: %d %v", st, body)
	}
	return body
}

func str(m map[string]any, k string) string { s, _ := m[k].(string); return s }

// ---- the happy path and roles ------------------------------------------------------------------

func TestFullFlowViewerAndAdminRoles(t *testing.T) {
	f := newFlow(t, []string{"admin-user", "viewer-user"}, []string{"admin-user"})

	viewer := f.login(t, "viewer-user")
	if u := viewer["user"].(map[string]any); u["login"] != "viewer-user" || u["role"] != "viewer" {
		t.Fatalf("viewer = %v", u)
	}
	if st, _ := f.get(t, "/v1/snapshot", str(viewer, "access_token")); st != 503 && st != 200 {
		t.Fatalf("viewer on a viewer route: %d", st)
	}
	if st, body := f.get(t, "/v1/audit", str(viewer, "access_token")); st != 403 {
		t.Fatalf("viewer on an admin route: %d %s", st, body)
	}

	admin := f.login(t, "admin-user")
	if st, _ := f.get(t, "/v1/audit", str(admin, "access_token")); st != 200 {
		t.Fatalf("admin on an admin route: %d", st)
	}
	if str(admin, "token_type") != "Bearer" || admin["expires_in"].(float64) < 800 || admin["expires_in"].(float64) > 900 {
		t.Fatalf("token response = %v", admin)
	}
	if f.auditor.Count("auth.login", "ok") != 2 {
		t.Fatalf("login audits = %d", f.auditor.Count("auth.login", "ok"))
	}
}

func TestTheClientSecretStaysOnTheServer(t *testing.T) {
	f := newFlow(t, []string{"admin-user"}, []string{"admin-user"})
	tok := f.login(t, "admin-user")
	blob, _ := json.Marshal(tok)
	if strings.Contains(string(blob), clientSecret) {
		t.Fatal("client secret in the token response")
	}
	ex := f.mock.ReceivedExchanges()
	if len(ex) != 1 || ex[0].ClientSecret != clientSecret || ex[0].ClientID != "mock-client-id" {
		t.Fatalf("the API must send the secret server-to-server: %+v", ex)
	}
	if !strings.HasSuffix(ex[0].RedirectURI, "/v1/auth/github/callback") {
		t.Fatalf("redirect uri sent to GitHub = %q", ex[0].RedirectURI)
	}
}

// ---- PKCE ---------------------------------------------------------------------------------------

func TestPKCEMismatchIsRejectedAndBurnsTheCode(t *testing.T) {
	f := newFlow(t, []string{"admin-user"}, nil)
	verifier, challenge := pkce()
	code, _ := f.signIn(t, "admin-user", challenge, appRedirect)
	wrong, _ := pkce()
	st, body := f.postToken(t, map[string]string{"code": code, "code_verifier": wrong})
	if st != 400 || body["error"].(map[string]any)["code"] != "invalid_grant" {
		t.Fatalf("wrong verifier: %d %v", st, body)
	}
	// an attacker who guessed wrong cannot try again with another guess against the same code, and neither can the app
	if st, _ := f.postToken(t, map[string]string{"code": code, "code_verifier": verifier}); st != 400 {
		t.Fatalf("the code must be single-use even after a failed attempt, got %d", st)
	}
	if f.auditor.Count("auth.login", "pkce_mismatch") != 1 {
		t.Fatal("pkce mismatch not audited")
	}
}

func TestVerifierLengthIsEnforced(t *testing.T) {
	f := newFlow(t, []string{"admin-user"}, nil)
	_, challenge := pkce()
	code, _ := f.signIn(t, "admin-user", challenge, appRedirect)
	if st, _ := f.postToken(t, map[string]string{"code": code, "code_verifier": "short"}); st != 400 {
		t.Fatalf("short verifier accepted: %d", st)
	}
}

func TestStartValidatesChallengeMethodAndRedirect(t *testing.T) {
	f := newFlow(t, []string{"admin-user"}, nil)
	_, challenge := pkce()
	cases := map[string]string{
		"missing challenge": "/v1/auth/github/start?redirect_uri=" + url.QueryEscape(appRedirect),
		"short challenge":   "/v1/auth/github/start?code_challenge=abc&redirect_uri=" + url.QueryEscape(appRedirect),
		"plain method":      "/v1/auth/github/start?code_challenge=" + challenge + "&code_challenge_method=plain&redirect_uri=" + url.QueryEscape(appRedirect),
		"unknown scheme":    "/v1/auth/github/start?code_challenge=" + challenge + "&redirect_uri=" + url.QueryEscape("https://evil.example/cb"),
		"lookalike host":    "/v1/auth/github/start?code_challenge=" + challenge + "&redirect_uri=" + url.QueryEscape("http://127.0.0.1.evil.example/cb"),
		"userinfo trick":    "/v1/auth/github/start?code_challenge=" + challenge + "&redirect_uri=" + url.QueryEscape("http://127.0.0.1@evil.example/cb"),
		"fragment":          "/v1/auth/github/start?code_challenge=" + challenge + "&redirect_uri=" + url.QueryEscape("oncall://cb#x"),
		"no redirect":       "/v1/auth/github/start?code_challenge=" + challenge,
	}
	for name, path := range cases {
		resp, err := noRedirect.Get(f.api.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		if resp.StatusCode != 400 {
			t.Errorf("%s: status %d, want 400", name, resp.StatusCode)
		}
	}
	for _, ok := range []string{appRedirect, "exp://192.168.1.5:8081/--/auth", "http://127.0.0.1:8081/auth/callback", "http://localhost:19006/auth"} {
		resp, _ := noRedirect.Get(f.api.URL + "/v1/auth/github/start?code_challenge=" + challenge + "&redirect_uri=" + url.QueryEscape(ok))
		if resp.StatusCode != 302 {
			t.Errorf("%s should be allowed, got %d", ok, resp.StatusCode)
		}
	}
}

// ---- state (CSRF) ----------------------------------------------------------------------------------

func TestUnknownStateIsRejectedWithoutRedirecting(t *testing.T) {
	f := newFlow(t, []string{"admin-user"}, nil)
	resp, _ := noRedirect.Get(f.api.URL + "/v1/auth/github/callback?code=whatever&state=forged-state")
	if resp.StatusCode != 400 || resp.Header.Get("Location") != "" {
		t.Fatalf("status %d location %q: a bad state must not redirect anywhere", resp.StatusCode, resp.Header.Get("Location"))
	}
	resp, _ = noRedirect.Get(f.api.URL + "/v1/auth/github/callback?code=whatever")
	if resp.StatusCode != 400 {
		t.Fatalf("missing state: %d", resp.StatusCode)
	}
}

func TestStateCanOnlyBeUsedOnceAndExpires(t *testing.T) {
	f := newFlow(t, []string{"admin-user"}, nil)
	_, challenge := pkce()
	start := f.api.URL + "/v1/auth/github/start?code_challenge=" + challenge + "&redirect_uri=" + url.QueryEscape(appRedirect)
	resp, _ := noRedirect.Get(start)
	authz, _ := noRedirect.Get(resp.Header.Get("Location") + "&login=admin-user")
	callback := authz.Header.Get("Location")
	first, _ := noRedirect.Get(callback)
	if first.StatusCode != 302 || !strings.Contains(first.Header.Get("Location"), "code=") {
		t.Fatalf("first callback: %d %s", first.StatusCode, first.Header.Get("Location"))
	}
	second, _ := noRedirect.Get(callback) // replay of the same callback URL
	if second.StatusCode != 400 {
		t.Fatalf("replayed callback: %d, want 400", second.StatusCode)
	}

	resp, _ = noRedirect.Get(start)
	authz, _ = noRedirect.Get(resp.Header.Get("Location") + "&login=admin-user")
	f.advance(11 * time.Minute) // state TTL is 10 minutes
	late, _ := noRedirect.Get(authz.Header.Get("Location"))
	if late.StatusCode != 400 {
		t.Fatalf("expired state: %d", late.StatusCode)
	}
}

func TestUserDenyingAccessAtGitHubReturnsAnErrorToTheApp(t *testing.T) {
	f := newFlow(t, []string{"admin-user"}, nil)
	_, challenge := pkce()
	resp, _ := noRedirect.Get(f.api.URL + "/v1/auth/github/start?code_challenge=" + challenge + "&redirect_uri=" + url.QueryEscape(appRedirect))
	authz, _ := noRedirect.Get(resp.Header.Get("Location") + "&deny=1")
	cb, _ := noRedirect.Get(authz.Header.Get("Location"))
	loc, _ := url.Parse(cb.Header.Get("Location"))
	if loc.Scheme != "oncall" || loc.Query().Get("error") != "access_denied" || loc.Query().Get("code") != "" {
		t.Fatalf("redirect = %s", cb.Header.Get("Location"))
	}
}

// ---- allowlist ------------------------------------------------------------------------------------

func TestUserNotOnTheAllowlistIsRefusedAndNoCodeIsIssued(t *testing.T) {
	f := newFlow(t, []string{"admin-user"}, []string{"admin-user"})
	_, challenge := pkce()
	code, errParam := f.signIn(t, "outsider", challenge, appRedirect)
	if code != "" || errParam != "not_allowed" {
		t.Fatalf("code=%q error=%q, want a not_allowed refusal", code, errParam)
	}
	if f.auditor.Count("auth.login", "not_allowed") != 1 {
		t.Fatal("refused login not audited")
	}
}

func TestAllowlistIsCaseInsensitive(t *testing.T) {
	f := newFlow(t, []string{"Admin-User"}, []string{"ADMIN-USER"})
	tok := f.login(t, "admin-user")
	if tok["user"].(map[string]any)["role"] != "admin" {
		t.Fatalf("%v", tok["user"])
	}
}

// ---- refresh tokens: rotation and reuse detection ---------------------------------------------------

func TestRefreshRotatesAndTheOldTokenStopsWorking(t *testing.T) {
	f := newFlow(t, []string{"admin-user"}, []string{"admin-user"})
	first := f.login(t, "admin-user")
	st, second := f.postToken(t, map[string]string{"refresh_token": str(first, "refresh_token")})
	if st != 200 || str(second, "refresh_token") == "" || str(second, "refresh_token") == str(first, "refresh_token") || str(second, "access_token") == "" {
		t.Fatalf("rotation: %d %v", st, second)
	}
	if st, _ := f.get(t, "/v1/audit", str(second, "access_token")); st != 200 {
		t.Fatalf("new access token rejected: %d", st)
	}
	st, third := f.postToken(t, map[string]string{"refresh_token": str(second, "refresh_token")})
	if st != 200 || str(third, "refresh_token") == str(second, "refresh_token") {
		t.Fatalf("second rotation: %d %v", st, third)
	}
}

func TestReusingAnOldRefreshTokenRevokesTheWholeFamily(t *testing.T) {
	f := newFlow(t, []string{"admin-user"}, []string{"admin-user"})
	first := f.login(t, "admin-user")
	old := str(first, "refresh_token")
	_, second := f.postToken(t, map[string]string{"refresh_token": old}) // legitimate rotation
	current := str(second, "refresh_token")

	// an attacker (or a stolen backup) presents the OLD token
	st, body := f.postToken(t, map[string]string{"refresh_token": old})
	if st != 401 || body["error"].(map[string]any)["code"] != "invalid_grant" {
		t.Fatalf("reuse: %d %v", st, body)
	}
	// the legitimate holder's CURRENT token is dead too: the whole family was revoked
	if st, _ := f.postToken(t, map[string]string{"refresh_token": current}); st != 401 {
		t.Fatalf("the family must be revoked after reuse, current token got %d", st)
	}
	if f.auditor.Count("auth.refresh_reuse", "family_revoked") != 1 {
		t.Fatalf("exactly one reuse event expected (the later presentation of an already-revoked token is not a new reuse), got %d", f.auditor.Count("auth.refresh_reuse", "family_revoked"))
	}
}

func TestUnknownAndGarbageRefreshTokens(t *testing.T) {
	f := newFlow(t, []string{"admin-user"}, nil)
	if st, _ := f.postToken(t, map[string]string{"refresh_token": "nope"}); st != 401 {
		t.Fatalf("unknown: %d", st)
	}
	if st, _ := f.postToken(t, map[string]string{}); st != 400 {
		t.Fatalf("empty: %d", st)
	}
	if st, _ := f.postToken(t, map[string]string{"code": "x", "refresh_token": "y"}); st != 400 {
		t.Fatalf("both: %d", st)
	}
	resp, _ := http.Post(f.api.URL+"/v1/auth/token", "application/json", strings.NewReader("{not json"))
	if resp.StatusCode != 400 {
		t.Fatalf("bad json: %d", resp.StatusCode)
	}
}

func TestExpiredRefreshTokenAndExpiredCode(t *testing.T) {
	f := newFlow(t, []string{"admin-user"}, nil)
	first := f.login(t, "admin-user")
	f.advance(31 * 24 * time.Hour)
	if st, _ := f.postToken(t, map[string]string{"refresh_token": str(first, "refresh_token")}); st != 401 {
		t.Fatalf("expired refresh token: %d", st)
	}
	v, c := pkce()
	code, _ := f.signIn(t, "admin-user", c, appRedirect)
	f.advance(3 * time.Minute) // code TTL is 2 minutes
	if st, _ := f.postToken(t, map[string]string{"code": code, "code_verifier": v}); st != 400 {
		t.Fatalf("expired code: %d", st)
	}
}

func TestExpiredAccessTokenIs401(t *testing.T) {
	f := newFlow(t, []string{"admin-user"}, []string{"admin-user"})
	tok := f.login(t, "admin-user")
	if st, _ := f.get(t, "/v1/me", str(tok, "access_token")); st != 200 {
		t.Fatalf("fresh token: %d", st)
	}
	f.advance(16 * time.Minute)
	st, body := f.get(t, "/v1/me", str(tok, "access_token"))
	if st != 401 || !strings.Contains(body, "token_expired") {
		t.Fatalf("expired access token: %d %s", st, body)
	}
	// and the refresh token mints a new one
	st2, fresh := f.postToken(t, map[string]string{"refresh_token": str(tok, "refresh_token")})
	if st2 != 200 {
		t.Fatalf("refresh after expiry: %d", st2)
	}
	if st, _ := f.get(t, "/v1/me", str(fresh, "access_token")); st != 200 {
		t.Fatalf("new access token: %d", st)
	}
}

func TestRemovedFromAllowlistLosesAccessAtTheNextRefresh(t *testing.T) {
	f := newFlow(t, []string{"admin-user", "viewer-user"}, []string{"admin-user"})
	tok := f.login(t, "viewer-user")
	delete(f.handler.S.Allowlist, "viewer-user")
	st, body := f.postToken(t, map[string]string{"refresh_token": str(tok, "refresh_token")})
	if st != 401 {
		t.Fatalf("%d %v", st, body)
	}
}

func TestDemotedAdminGetsTheViewerRoleAtRefresh(t *testing.T) {
	f := newFlow(t, []string{"admin-user"}, []string{"admin-user"})
	tok := f.login(t, "admin-user")
	delete(f.handler.S.Admins, "admin-user")
	_, next := f.postToken(t, map[string]string{"refresh_token": str(tok, "refresh_token")})
	if next["user"].(map[string]any)["role"] != "viewer" {
		t.Fatalf("role after demotion = %v", next["user"])
	}
	if st, _ := f.get(t, "/v1/audit", str(next, "access_token")); st != 403 {
		t.Fatalf("demoted admin still reaches admin routes: %d", st)
	}
}

func TestLogoutRevokesTheRefreshFamily(t *testing.T) {
	f := newFlow(t, []string{"admin-user"}, nil)
	tok := f.login(t, "admin-user")
	req, _ := http.NewRequest("POST", f.api.URL+"/v1/auth/logout", nil)
	req.Header.Set("Authorization", "Bearer "+str(tok, "access_token"))
	resp, err := http.DefaultClient.Do(req)
	if err != nil || resp.StatusCode != 204 {
		t.Fatalf("logout: %v %v", resp, err)
	}
	if st, _ := f.postToken(t, map[string]string{"refresh_token": str(tok, "refresh_token")}); st != 401 {
		t.Fatalf("refresh after logout: %d", st)
	}
	resp, _ = http.Post(f.api.URL+"/v1/auth/logout", "application/json", nil)
	if resp.StatusCode != 401 {
		t.Fatalf("logout without a token: %d", resp.StatusCode)
	}
}

func TestOnlyHashesOfRefreshTokensAreKept(t *testing.T) {
	f := newFlow(t, []string{"admin-user"}, nil)
	tok := f.login(t, "admin-user")
	raw := str(tok, "refresh_token")
	if _, res, _ := f.store.RotateRefresh(context.Background(), raw, auth.RefreshRecord{ID: "x", TokenHash: "h", ExpiresAt: f.clock().Add(time.Hour)}, f.clock()); res != auth.NotFound {
		t.Fatalf("the raw token must not be a lookup key in the store, got %v", res)
	}
	if _, res, _ := f.store.RotateRefresh(context.Background(), auth.HashToken(raw), auth.RefreshRecord{ID: "y", TokenHash: "h2", ExpiresAt: f.clock().Add(time.Hour)}, f.clock()); res != auth.Rotated {
		t.Fatalf("the hash should find the token, got %v", res)
	}
}

func TestCodeCannotBeUsedTwice(t *testing.T) {
	f := newFlow(t, []string{"admin-user"}, nil)
	v, c := pkce()
	code, _ := f.signIn(t, "admin-user", c, appRedirect)
	if st, _ := f.postToken(t, map[string]string{"code": code, "code_verifier": v}); st != 200 {
		t.Fatalf("first use: %d", st)
	}
	if st, _ := f.postToken(t, map[string]string{"code": code, "code_verifier": v}); st != 400 {
		t.Fatalf("second use: %d", st)
	}
}

func TestRedirectKeepsExistingQueryParameters(t *testing.T) {
	f := newFlow(t, []string{"admin-user"}, nil)
	_, c := pkce()
	resp, _ := noRedirect.Get(f.api.URL + "/v1/auth/github/start?code_challenge=" + c + "&redirect_uri=" + url.QueryEscape("http://127.0.0.1:8081/auth/callback?next=%2Flive"))
	authz, _ := noRedirect.Get(resp.Header.Get("Location") + "&login=admin-user")
	cb, _ := noRedirect.Get(authz.Header.Get("Location"))
	loc, _ := url.Parse(cb.Header.Get("Location"))
	if loc.Path != "/auth/callback" || loc.Query().Get("next") != "/live" || loc.Query().Get("code") == "" {
		t.Fatalf("redirect = %s", loc)
	}
}
