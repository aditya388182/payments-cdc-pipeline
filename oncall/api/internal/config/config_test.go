package config

import (
	"os"
	"strings"
	"testing"
	"time"
)

// isolate blanks every ONCALL_* variable so the developer's own environment cannot leak into a test.
func isolate(t *testing.T) {
	t.Helper()
	for _, kv := range os.Environ() {
		if k, _, ok := strings.Cut(kv, "="); ok && strings.HasPrefix(k, "ONCALL_") {
			t.Setenv(k, "")
		}
	}
}

func TestDefaultsAndRequiredValues(t *testing.T) {
	isolate(t)
	t.Setenv("ONCALL_PG_DSN", "")
	t.Setenv("ONCALL_JWT_SECRET", "")
	if _, err := FromEnv(); err == nil || !strings.Contains(err.Error(), "ONCALL_PG_DSN") || !strings.Contains(err.Error(), "ONCALL_JWT_SECRET") {
		t.Fatalf("err = %v, want both missing values named", err)
	}
	t.Setenv("ONCALL_PG_DSN", "postgres://x")
	t.Setenv("ONCALL_JWT_SECRET", "short")
	if _, err := FromEnv(); err == nil {
		t.Fatal("a short JWT secret must be rejected")
	}
	t.Setenv("ONCALL_JWT_SECRET", strings.Repeat("k", 32))
	c, err := FromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if c.Addr != "127.0.0.1:8088" || c.MetricsAddr != "127.0.0.1:8089" || c.MaxRate != 200 || c.SnapshotInterval != 2*time.Second || c.AccessTTL != 15*time.Minute {
		t.Fatalf("defaults wrong: %+v", c)
	}
	if !strings.HasPrefix(c.Addr, "127.0.0.1:") || !strings.HasPrefix(c.MetricsAddr, "127.0.0.1:") {
		t.Fatal("listeners must default to loopback")
	}
	if c.GitHubAPIBase != "https://api.github.com" {
		t.Fatalf("github api base = %q", c.GitHubAPIBase)
	}
}

func TestOverridesAndSets(t *testing.T) {
	isolate(t)
	t.Setenv("ONCALL_PG_DSN", "postgres://x")
	t.Setenv("ONCALL_JWT_SECRET", strings.Repeat("k", 32))
	t.Setenv("ONCALL_MAX_RATE", "50")
	t.Setenv("ONCALL_SNAPSHOT_INTERVAL", "500ms")
	t.Setenv("ONCALL_ALLOWLIST", "Alice, bob")
	t.Setenv("ONCALL_ADMINS", "ALICE")
	t.Setenv("ONCALL_GITHUB_BASE", "http://127.0.0.1:8099")
	c, err := FromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if c.MaxRate != 50 || c.SnapshotInterval != 500*time.Millisecond {
		t.Fatalf("%+v", c)
	}
	if !c.Allowlist["alice"] || !c.Allowlist["bob"] || !c.Admins["alice"] || c.Admins["bob"] {
		t.Fatalf("allowlist=%v admins=%v (logins are case-insensitive)", c.Allowlist, c.Admins)
	}
	if c.GitHubAPIBase != "http://127.0.0.1:8099" {
		t.Fatalf("mock api base = %q", c.GitHubAPIBase)
	}
}
