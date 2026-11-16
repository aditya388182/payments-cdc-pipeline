package main

import (
	"github.com/aditya388182/payments-cdc-pipeline/oncall/api/internal/api"
	"github.com/aditya388182/payments-cdc-pipeline/oncall/api/internal/auth"
	"github.com/aditya388182/payments-cdc-pipeline/oncall/api/internal/config"
	"github.com/aditya388182/payments-cdc-pipeline/oncall/api/internal/store"
)

// buildAuth wires the OAuth routes (added in Phase 3). Until then the API only accepts tokens minted with
// `oncall-api token`.
func buildAuth(cfg config.Config, st *store.Store, signer *auth.Signer) (api.AuthRoutes, error) {
	return nil, nil
}
