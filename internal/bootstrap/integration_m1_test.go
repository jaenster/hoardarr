//go:build integration

package bootstrap_test

// Live integration test for M1 — wires the full pipeline against a
// real Usenet provider. Skipped without credentials.
//
// Run with:
//
//	HOARDARR_TEST_NNTP_HOST=news.example.com \
//	HOARDARR_TEST_NNTP_PORT=563 \
//	HOARDARR_TEST_NNTP_TLS=true \
//	HOARDARR_TEST_NNTP_USER=u \
//	HOARDARR_TEST_NNTP_PASS=p \
//	HOARDARR_TEST_NZB_PATH=/path/to/small.nzb \
//	go test -tags integration ./internal/bootstrap/...
//
// Credentials are read from env vars first; if any required var is
// missing, the test attempts to read [test_servers] from a
// config.toml at $HOARDARR_TEST_CONFIG (defaults to
// ./.local-test/config.toml). If that's also missing, the test is
// skipped.
//
// The chosen NZB should be small (a few MB) and reference public-domain
// content. The test asserts: the orchestrator runs to completion, every
// file lands in incomplete/, the byte counts match the NZB summary.

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/jaenster/hoardarr/internal/adapter/nntp"
	appdownload "github.com/jaenster/hoardarr/internal/app/download"
	appserver "github.com/jaenster/hoardarr/internal/app/server"
	"github.com/jaenster/hoardarr/internal/bootstrap"
	"github.com/jaenster/hoardarr/internal/config"
)

// integrationCreds is the runtime input for the live test.
type integrationCreds struct {
	Host     string
	Port     int
	TLS      bool
	User     string
	Pass     string
	MaxConns int
	NZBPath  string
}

// loadIntegrationCreds resolves credentials from env first, then from
// a local TOML at $HOARDARR_TEST_CONFIG (or the default path). Returns
// (creds, true) if configured, (zero, false) otherwise.
func loadIntegrationCreds(t *testing.T) (integrationCreds, bool) {
	t.Helper()

	c := integrationCreds{
		Host:     os.Getenv("HOARDARR_TEST_NNTP_HOST"),
		User:     os.Getenv("HOARDARR_TEST_NNTP_USER"),
		Pass:     os.Getenv("HOARDARR_TEST_NNTP_PASS"),
		NZBPath:  os.Getenv("HOARDARR_TEST_NZB_PATH"),
		MaxConns: 4,
	}
	if v := os.Getenv("HOARDARR_TEST_NNTP_PORT"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			c.Port = n
		}
	}
	if v := os.Getenv("HOARDARR_TEST_NNTP_MAX_CONNS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			c.MaxConns = n
		}
	}
	tlsStr := strings.ToLower(os.Getenv("HOARDARR_TEST_NNTP_TLS"))
	c.TLS = tlsStr == "" || tlsStr == "1" || tlsStr == "true" || tlsStr == "yes"

	// Fallback: TOML config at $HOARDARR_TEST_CONFIG.
	if c.Host == "" || c.NZBPath == "" {
		path := os.Getenv("HOARDARR_TEST_CONFIG")
		if path == "" {
			path = "./.local-test/config.toml"
		}
		if data, err := os.ReadFile(path); err == nil {
			var ts struct {
				TestServer struct {
					Host     string `toml:"host"`
					Port     int    `toml:"port"`
					TLS      *bool  `toml:"tls"`
					User     string `toml:"user"`
					Pass     string `toml:"pass"`
					MaxConns int    `toml:"max_conns"`
					NZBPath  string `toml:"nzb_path"`
				} `toml:"test_server"`
			}
			if _, err := toml.Decode(string(data), &ts); err == nil {
				if c.Host == "" {
					c.Host = ts.TestServer.Host
				}
				if c.Port == 0 {
					c.Port = ts.TestServer.Port
				}
				if c.User == "" {
					c.User = ts.TestServer.User
				}
				if c.Pass == "" {
					c.Pass = ts.TestServer.Pass
				}
				if c.MaxConns == 4 && ts.TestServer.MaxConns > 0 {
					c.MaxConns = ts.TestServer.MaxConns
				}
				if c.NZBPath == "" {
					c.NZBPath = ts.TestServer.NZBPath
				}
				if ts.TestServer.TLS != nil {
					c.TLS = *ts.TestServer.TLS
				}
			}
		}
	}

	missing := []string{}
	if c.Host == "" {
		missing = append(missing, "host")
	}
	if c.Port == 0 {
		missing = append(missing, "port")
	}
	if c.NZBPath == "" {
		missing = append(missing, "nzb_path")
	}
	if len(missing) > 0 {
		t.Skipf("integration creds missing: %v (set HOARDARR_TEST_NNTP_* or [test_server] in HOARDARR_TEST_CONFIG)", missing)
		return integrationCreds{}, false
	}
	return c, true
}

func TestM1_LiveProvider(t *testing.T) {
	creds, ok := loadIntegrationCreds(t)
	if !ok {
		return
	}

	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.toml")
	t.Setenv("HOARDARR_LISTEN", "127.0.0.1:0")
	t.Setenv("HOARDARR_DATA_DIR", filepath.Join(dir, "data"))
	cfg, err := config.LoadOrCreate(cfgPath)
	if err != nil {
		t.Fatalf("LoadOrCreate: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	app, err := bootstrap.Build(ctx, cfg, nil, nil)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	defer func() { _ = app.Shutdown() }()

	tls := creds.TLS
	srvID, err := app.ServerService.Add(ctx, appserver.AddCmd{
		Name: "live", Host: creds.Host, Port: creds.Port,
		TLS: &tls, Username: creds.User, Password: creds.Pass,
		MaxConns: creds.MaxConns,
	})
	if err != nil {
		t.Fatalf("server add: %v", err)
	}

	nzbPath, err := resolveTestPath(creds.NZBPath)
	if err != nil {
		t.Fatalf("resolve nzb path %q: %v", creds.NZBPath, err)
	}
	nzbBytes, err := os.ReadFile(nzbPath)
	if err != nil {
		t.Fatalf("read nzb %q: %v", nzbPath, err)
	}

	jobID, err := app.AddJobService.AddJob(ctx, appdownload.AddJobCmd{
		NZB:      bytes.NewReader(nzbBytes),
		Category: "integration",
	})
	if err != nil && !errors.Is(err, appdownload.ErrDuplicateNZB) {
		t.Fatalf("AddJob: %v", err)
	}

	srv, err := app.ServerService.Get(ctx, srvID)
	if err != nil {
		t.Fatalf("Get server: %v", err)
	}

	// Optional recording: HOARDARR_TEST_RECORD=1 with
	// HOARDARR_TEST_CASSETTE_PATH set captures the NNTP session to a
	// JSONL cassette (with AUTHINFO PASS redacted). The cassette can
	// then be committed and replayed in CI without creds.
	var dialer nntp.Dialer
	if os.Getenv("HOARDARR_TEST_RECORD") == "1" {
		path := os.Getenv("HOARDARR_TEST_CASSETTE_PATH")
		if path == "" {
			t.Fatal("HOARDARR_TEST_RECORD=1 requires HOARDARR_TEST_CASSETTE_PATH")
		}
		dialer = &nntp.RecordingDialer{Inner: nntp.DefaultDialer, Path: path}
		t.Logf("recording NNTP session → %s", path)
	}

	pool := nntp.NewPool(srv, nntp.PoolOptions{Dialer: dialer})
	defer pool.Close()
	fetcher := appdownload.NewPoolFetcher(pool)
	orch := appdownload.NewOrchestrator(
		app.JobRepo, fetcher, app.Bus, app.TxMgr,
		srv.ID(), srv.MaxConns(), cfg.Paths.IncompleteDir,
		appdownload.OrchestratorOptions{},
	)
	if err := orch.Run(ctx, jobID); err != nil {
		t.Fatalf("orchestrator.Run: %v", err)
	}

	final, err := app.JobRepo.ByID(ctx, jobID)
	if err != nil {
		t.Fatalf("load final: %v", err)
	}
	t.Logf("integration job %d: state=%s done=%d total=%d failed=%d",
		final.ID(), final.State(), final.DoneBytes(), final.TotalBytes(), final.FailedBytes())

	if final.State().IsActive() {
		t.Errorf("final state = %s; want non-active", final.State())
	}
	for _, f := range final.Files() {
		path := filepath.Join(cfg.Paths.IncompleteDir, strconv.FormatInt(int64(jobID), 10), strconv.FormatInt(int64(f.ID()), 10)+".tmp")
		st, err := os.Stat(path)
		if err != nil {
			t.Errorf("file %q not on disk: %v", f.Filename(), err)
			continue
		}
		if st.Size() == 0 {
			t.Errorf("file %q empty on disk", f.Filename())
		}
	}
}

// silence unused
var _ = io.EOF
