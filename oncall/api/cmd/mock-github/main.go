// Command mock-github runs the mock GitHub OAuth server on loopback for local demos, Playwright and CI.
package main

import (
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/aditya388182/payments-cdc-pipeline/oncall/api/internal/mockgithub"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	addr := getenv("MOCK_GITHUB_ADDR", "127.0.0.1:8099")
	users := strings.Split(getenv("MOCK_GITHUB_USERS", "admin-user,viewer-user,outsider"), ",")
	s := mockgithub.New(getenv("ONCALL_GITHUB_CLIENT_ID", "mock-client-id"), os.Getenv("ONCALL_GITHUB_CLIENT_SECRET"), users...)
	slog.Info("mock GitHub listening (NOT for real credentials)", "addr", addr, "users", users)
	srv := &http.Server{Addr: addr, Handler: s.Handler(), ReadHeaderTimeout: 5 * time.Second}
	if err := srv.ListenAndServe(); err != nil {
		slog.Error("mock-github", "err", err.Error())
		os.Exit(1)
	}
}

func getenv(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
