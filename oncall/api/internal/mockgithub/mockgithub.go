// Package mockgithub is a tiny stand-in for GitHub's OAuth web flow (authorize page, token exchange, /user) so the
// whole sign-in works in tests, CI and the demo without a real OAuth app. It is NOT secure: loopback use only.
package mockgithub

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"html"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
)

type Server struct {
	ClientID     string
	ClientSecret string
	Users        []string // logins the authorize page offers

	mu       sync.Mutex
	codes    map[string]string // one-time code -> login
	tokens   map[string]string // access token -> login
	Received []TokenExchange   // what the API's server-to-server calls sent (for assertions)
}

type TokenExchange struct {
	ClientID, ClientSecret, Code, RedirectURI string
}

func New(clientID, clientSecret string, users ...string) *Server {
	return &Server{ClientID: clientID, ClientSecret: clientSecret, Users: users, codes: map[string]string{}, tokens: map[string]string{}}
}

func rnd(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /login/oauth/authorize", s.authorize)
	mux.HandleFunc("POST /login/oauth/access_token", s.accessToken)
	mux.HandleFunc("GET /user", s.user)
	return mux
}

func (s *Server) known(login string) bool {
	for _, u := range s.Users {
		if u == login {
			return true
		}
	}
	return false
}

func (s *Server) authorize(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	if q.Get("client_id") != s.ClientID {
		http.Error(w, "mock-github: unknown client_id", http.StatusBadRequest)
		return
	}
	redirect, state := q.Get("redirect_uri"), q.Get("state")
	ru, err := url.Parse(redirect)
	if err != nil || ru.Scheme == "" {
		http.Error(w, "mock-github: bad redirect_uri", http.StatusBadRequest)
		return
	}
	back := func(params url.Values) {
		rq := ru.Query()
		for k, v := range params {
			rq[k] = v
		}
		ru.RawQuery = rq.Encode()
		http.Redirect(w, r, ru.String(), http.StatusFound)
	}
	if q.Get("deny") != "" {
		back(url.Values{"error": {"access_denied"}, "state": {state}})
		return
	}
	if login := q.Get("login"); login != "" {
		if !s.known(login) {
			http.Error(w, "mock-github: unknown user", http.StatusBadRequest)
			return
		}
		code := rnd(16)
		s.mu.Lock()
		s.codes[code] = login
		s.mu.Unlock()
		back(url.Values{"code": {code}, "state": {state}})
		return
	}
	// the "sign in with GitHub" page: one link per user
	users := append([]string(nil), s.Users...)
	sort.Strings(users)
	var b strings.Builder
	b.WriteString(`<!doctype html><html lang="en"><head><meta charset="utf-8"><title>Mock GitHub sign-in</title></head><body style="font-family:sans-serif;max-width:32rem;margin:3rem auto"><h1>Mock GitHub</h1><p>Authorize <b>On-Call Console</b> as:</p><ul>`)
	for _, u := range users {
		l := *r.URL
		qq := l.Query()
		qq.Set("login", u)
		l.RawQuery = qq.Encode()
		fmt.Fprintf(&b, `<li><a id="user-%s" href="%s">Sign in as %s</a></li>`, html.EscapeString(u), html.EscapeString(l.RequestURI()), html.EscapeString(u))
	}
	dq := r.URL.Query()
	dq.Set("deny", "1")
	l := *r.URL
	l.RawQuery = dq.Encode()
	fmt.Fprintf(&b, `</ul><p><a id="deny" href="%s">Cancel</a></p></body></html>`, html.EscapeString(l.RequestURI()))
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write([]byte(b.String()))
}

func (s *Server) accessToken(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "bad form", http.StatusBadRequest)
		return
	}
	ex := TokenExchange{r.PostForm.Get("client_id"), r.PostForm.Get("client_secret"), r.PostForm.Get("code"), r.PostForm.Get("redirect_uri")}
	s.mu.Lock()
	s.Received = append(s.Received, ex)
	login, ok := s.codes[ex.Code]
	delete(s.codes, ex.Code) // single use, like GitHub
	s.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	if ex.ClientID != s.ClientID || ex.ClientSecret != s.ClientSecret || !ok {
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "bad_verification_code", "error_description": "The code passed is incorrect or expired."})
		return
	}
	tok := "gho_mock_" + rnd(16)
	s.mu.Lock()
	s.tokens[tok] = login
	s.mu.Unlock()
	_ = json.NewEncoder(w).Encode(map[string]string{"access_token": tok, "token_type": "bearer", "scope": "read:user"})
}

func (s *Server) user(w http.ResponseWriter, r *http.Request) {
	tok := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	s.mu.Lock()
	login, ok := s.tokens[tok]
	s.mu.Unlock()
	if !ok {
		w.WriteHeader(http.StatusUnauthorized)
		_ = json.NewEncoder(w).Encode(map[string]string{"message": "Bad credentials"})
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"login": login, "id": 1, "type": "User"})
}

// ReceivedExchanges returns a copy of the token exchanges seen so far.
func (s *Server) ReceivedExchanges() []TokenExchange {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]TokenExchange(nil), s.Received...)
}
