package main

import (
	"net/http"
	"time"

	"github.com/aditya388182/payments-cdc-pipeline/oncall/api/internal/api"
	"github.com/aditya388182/payments-cdc-pipeline/oncall/api/internal/auth"
	"github.com/aditya388182/payments-cdc-pipeline/oncall/api/internal/config"
	"github.com/aditya388182/payments-cdc-pipeline/oncall/api/internal/store"
)

// buildAuth wires the GitHub OAuth + PKCE routes. It is only enabled when a client id is configured; otherwise the
// API accepts only tokens minted with `oncall-api token`.
func buildAuth(cfg config.Config, st *store.Store, signer *auth.Signer) (api.AuthRoutes, error) {
	if cfg.GitHubClientID == "" {
		return nil, nil
	}
	return &auth.Handler{
		S: auth.Settings{PublicURL: cfg.PublicURL, GitHubBase: cfg.GitHubBase, GitHubAPIBase: cfg.GitHubAPIBase, ClientID: cfg.GitHubClientID,
			ClientSecret: cfg.GitHubClientSecret, Allowlist: cfg.Allowlist, Admins: cfg.Admins, AllowedRedirects: cfg.AllowedRedirects, RefreshTTL: cfg.RefreshTTL},
		Store: store.AuthStore{Store: st}, Signer: signer, Audit: store.AuthAudit{Store: st},
		HTTP: &http.Client{Timeout: 5 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }},
	}, nil
}
