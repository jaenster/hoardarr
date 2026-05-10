package bootstrap_test

// E2E test for M0.5 — exercises the full bootstrap path: config load,
// SQLite open + migrate, outbox bus init, HTTP server with API-key
// middleware. Drives real HTTP requests against the wired-up app.
//
// No build tag: this is the M0.5 acceptance test and must run by default.
// Live integration tests (real NNTP provider, *arr Docker harness) get
// build tags when they land.

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jaenster/hoardarr/internal/adapter/sqlite"
	"github.com/jaenster/hoardarr/internal/bootstrap"
	"github.com/jaenster/hoardarr/internal/config"
)

func TestM05_FullBootstrap(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.toml")

	// Pick a free port; we'll bind to it via env override.
	port := mustFreePort(t)
	t.Setenv("HOARDARR_LISTEN", net.JoinHostPort("127.0.0.1", port))
	t.Setenv("HOARDARR_DATA_DIR", filepath.Join(dir, "data"))

	cfg, err := config.LoadOrCreate(cfgPath)
	if err != nil {
		t.Fatalf("LoadOrCreate: %v", err)
	}

	if cfg.Auth.APIKey == "" {
		t.Fatal("api key empty after LoadOrCreate")
	}
	apiKey := cfg.Auth.APIKey

	// Verify config.toml was written.
	if _, err := os.Stat(cfgPath); err != nil {
		t.Fatalf("config not written: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	app, err := bootstrap.Build(ctx, cfg, nil, nil)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	defer func() { _ = app.Shutdown() }()

	// Verify DB file exists and has the expected schema.
	if _, err := os.Stat(cfg.Storage.SQLite.Path); err != nil {
		t.Fatalf("db not created: %v", err)
	}
	verifyOutboxSchema(t, app.DB)

	// Verify directory tree was created.
	for _, d := range []string{cfg.Server.DataDir, cfg.Paths.IncompleteDir, cfg.Paths.CompleteDir} {
		if info, err := os.Stat(d); err != nil || !info.IsDir() {
			t.Errorf("expected dir %q to exist; err=%v", d, err)
		}
	}

	// Run the server in a goroutine and wait for it to listen.
	runDone := make(chan error, 1)
	go func() { runDone <- app.Run(ctx) }()
	waitListen(t, cfg.Server.Listen)

	base := "http://" + cfg.Server.Listen

	// Public health endpoint accessible without API key.
	healthBody := mustGetJSON(t, base+"/api/v1/health", "")
	if healthBody["status"] != "ok" {
		t.Errorf("health.status = %v; want ok", healthBody["status"])
	}

	// Protected endpoint without key → 401.
	{
		resp, err := http.Get(base + "/api/v1/whoami")
		if err != nil {
			t.Fatalf("GET whoami no key: %v", err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusUnauthorized {
			t.Errorf("whoami no key status = %d; want 401", resp.StatusCode)
		}
	}

	// Protected endpoint with header key → 200.
	whoamiBody := mustGetJSON(t, base+"/api/v1/whoami", apiKey)
	if whoamiBody["authenticated"] != true {
		t.Errorf("whoami.authenticated = %v; want true", whoamiBody["authenticated"])
	}

	// Same with query-param key.
	whoamiQuery := mustGetJSON(t, base+"/api/v1/whoami?apikey="+apiKey, "")
	if whoamiQuery["authenticated"] != true {
		t.Errorf("whoami via query.authenticated = %v; want true", whoamiQuery["authenticated"])
	}

	// Trigger graceful shutdown via context cancel.
	cancel()
	select {
	case err := <-runDone:
		if err != nil {
			t.Errorf("Run returned: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return within 5s after ctx cancel")
	}
}

func mustGetJSON(t *testing.T, url, apiKey string) map[string]any {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		t.Fatalf("NewRequest %q: %v", url, err)
	}
	if apiKey != "" {
		req.Header.Set("X-Api-Key", apiKey)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET %q: %v", url, err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET %q: status %d, body=%s", url, resp.StatusCode, body)
	}
	var out map[string]any
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatalf("decode %q: %v (body=%s)", url, err, body)
	}
	return out
}

func waitListen(t *testing.T, addr string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("server did not listen on %s within 5s", addr)
}

func mustFreePort(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("free port: %v", err)
	}
	defer l.Close()
	_, port, err := net.SplitHostPort(l.Addr().String())
	if err != nil {
		t.Fatalf("split addr: %v", err)
	}
	return port
}

func verifyOutboxSchema(t *testing.T, db *sqlite.DB) {
	t.Helper()
	expected := []string{"outbox", "outbox_subs", "schema_migrations"}
	for _, name := range expected {
		var got string
		err := db.QueryRowContext(context.Background(),
			`SELECT name FROM sqlite_master WHERE type='table' AND name = ?`, name,
		).Scan(&got)
		if err != nil {
			t.Errorf("table %q not present: %v", name, err)
			continue
		}
		if !strings.EqualFold(got, name) {
			t.Errorf("table name mismatch: got %q want %q", got, name)
		}
	}
}
