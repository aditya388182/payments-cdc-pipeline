// Package config loads the service configuration from environment variables (see .env.example).
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	Addr        string // public API listener (loopback by default)
	MetricsAddr string // /metrics listener, never the public one
	PGDSN       string // oncall database
	SourcePGDSN string // the payments source database (row counts)
	JWTSecret   string

	ReplayAddr string // replay-svc gRPC

	KafkaBrokers   []string
	ConnectURL     string
	ConnectorName  string
	PushgatewayURL string
	CDCTopic       string
	ProgressTopic  string

	SnapshotInterval time.Duration
	MaxRate          int

	PushSender      string // fake | expo
	ExpoAccessToken string
	FakePushFile    string

	// Alert thresholds
	LagThreshold       int64
	LagFor             time.Duration
	FreshnessThreshold time.Duration
	FreshnessFor       time.Duration
	DLQGrowthThreshold int64
	ConnectorFor       time.Duration

	// Generator control
	RepoDir         string
	GeneratorPython string
	GeneratorScript string

	// OAuth (Phase 3)
	PublicURL          string
	GitHubBase         string // https://github.com in production, the mock server locally
	GitHubAPIBase      string
	GitHubClientID     string
	GitHubClientSecret string
	Allowlist          map[string]bool
	Admins             map[string]bool
	AccessTTL          time.Duration
	RefreshTTL         time.Duration
	AllowedRedirects   []string // exact redirect_uri prefixes the app may use
	CORSOrigins        []string // browser origins allowed to call the API (the Expo web build)
	WSSendBuf          int      // load test only: SO_SNDBUF for the API listener (0 = OS default)
}

func FromEnv() (Config, error) {
	c := Config{
		Addr:               get("ONCALL_ADDR", "127.0.0.1:8088"),
		MetricsAddr:        get("ONCALL_METRICS_ADDR", "127.0.0.1:8089"),
		PGDSN:              os.Getenv("ONCALL_PG_DSN"),
		SourcePGDSN:        get("ONCALL_SOURCE_PG_DSN", "postgres://payments:payments@127.0.0.1:5432/payments?sslmode=disable"),
		JWTSecret:          os.Getenv("ONCALL_JWT_SECRET"),
		ReplayAddr:         get("ONCALL_REPLAY_ADDR", "127.0.0.1:50051"),
		KafkaBrokers:       split(get("ONCALL_KAFKA_BROKERS", "127.0.0.1:29092")),
		ConnectURL:         get("ONCALL_CONNECT_URL", "http://127.0.0.1:8083"),
		ConnectorName:      get("ONCALL_CONNECTOR", "payments-postgres-cdc"),
		PushgatewayURL:     get("ONCALL_PUSHGATEWAY_URL", "http://127.0.0.1:9091/metrics"),
		CDCTopic:           get("ONCALL_CDC_TOPIC", "payments.public.transactions"),
		ProgressTopic:      get("ONCALL_PROGRESS_TOPIC", "pipeline.progress"),
		SnapshotInterval:   dur("ONCALL_SNAPSHOT_INTERVAL", 2*time.Second),
		MaxRate:            num("ONCALL_MAX_RATE", 200),
		PushSender:         get("ONCALL_PUSH_SENDER", "fake"),
		ExpoAccessToken:    os.Getenv("ONCALL_EXPO_ACCESS_TOKEN"),
		FakePushFile:       get("ONCALL_FAKE_PUSH_FILE", "logs/fake_push.jsonl"),
		LagThreshold:       int64(num("ONCALL_ALERT_LAG", 1000)),
		LagFor:             dur("ONCALL_ALERT_LAG_FOR", 30*time.Second),
		FreshnessThreshold: dur("ONCALL_ALERT_FRESHNESS", 120*time.Second),
		FreshnessFor:       dur("ONCALL_ALERT_FRESHNESS_FOR", 30*time.Second),
		DLQGrowthThreshold: int64(num("ONCALL_ALERT_DLQ_GROWTH", 5)),
		ConnectorFor:       dur("ONCALL_ALERT_CONNECTOR_FOR", 10*time.Second),
		RepoDir:            get("ONCALL_REPO_DIR", "."),
		GeneratorPython:    get("ONCALL_GENERATOR_PYTHON", "venv/bin/python"),
		GeneratorScript:    get("ONCALL_GENERATOR_SCRIPT", "scripts/data_generator.py"),
		PublicURL:          get("ONCALL_PUBLIC_URL", "http://127.0.0.1:8088"),
		GitHubBase:         get("ONCALL_GITHUB_BASE", "https://github.com"),
		GitHubAPIBase:      get("ONCALL_GITHUB_API_BASE", ""),
		GitHubClientID:     os.Getenv("ONCALL_GITHUB_CLIENT_ID"),
		GitHubClientSecret: os.Getenv("ONCALL_GITHUB_CLIENT_SECRET"),
		Allowlist:          set(os.Getenv("ONCALL_ALLOWLIST")),
		Admins:             set(os.Getenv("ONCALL_ADMINS")),
		AccessTTL:          dur("ONCALL_ACCESS_TTL", 15*time.Minute),
		RefreshTTL:         dur("ONCALL_REFRESH_TTL", 30*24*time.Hour),
		WSSendBuf:          num("ONCALL_WS_SNDBUF", 0),
		CORSOrigins:        split(get("ONCALL_CORS_ORIGINS", "http://127.0.0.1:8081,http://localhost:8081,http://127.0.0.1:19006,http://localhost:19006")),
		AllowedRedirects:   split(get("ONCALL_ALLOWED_REDIRECTS", "oncall://,exp://,http://127.0.0.1,http://localhost")),
	}
	if c.GitHubAPIBase == "" {
		if c.GitHubBase == "https://github.com" {
			c.GitHubAPIBase = "https://api.github.com"
		} else {
			c.GitHubAPIBase = c.GitHubBase // the mock serves both
		}
	}
	return c, c.Validate()
}

func (c Config) Validate() error {
	var missing []string
	if c.PGDSN == "" {
		missing = append(missing, "ONCALL_PG_DSN")
	}
	if len(c.JWTSecret) < 32 {
		missing = append(missing, "ONCALL_JWT_SECRET (>= 32 chars)")
	}
	if len(missing) > 0 {
		return fmt.Errorf("missing or invalid configuration: %s", strings.Join(missing, ", "))
	}
	return nil
}

func get(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

func num(k string, d int) int {
	if v := os.Getenv(k); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return d
}

func dur(k string, d time.Duration) time.Duration {
	if v := os.Getenv(k); v != "" {
		if x, err := time.ParseDuration(v); err == nil {
			return x
		}
	}
	return d
}

func split(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func set(s string) map[string]bool {
	m := map[string]bool{}
	for _, p := range split(s) {
		m[strings.ToLower(p)] = true
	}
	return m
}
