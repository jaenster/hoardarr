package bootstrap_test

// Cassette-driven M1 e2e — runs the same flow as TestM1_LiveProvider
// but against a recorded JSONL cassette instead of a live provider.
//
// Skipped if no cassette is configured. To enable:
//
//	HOARDARR_TEST_CASSETTE=/abs/path/to/cassette.jsonl \
//	HOARDARR_TEST_NZB_PATH=/abs/path/to/recorded.nzb \
//	go test ./internal/bootstrap/...
//
// Or, if a default cassette is committed to the repo, the test reads
// it from `internal/bootstrap/testdata/cassettes/m1.jsonl` and the
// matching NZB from `internal/bootstrap/testdata/cassettes/m1.nzb`.
// Both are gitignored by default; commit them once recorded.

import (
	"bytes"
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/jaenster/hoardarr/internal/adapter/nntp"
	appdownload "github.com/jaenster/hoardarr/internal/app/download"
	appserver "github.com/jaenster/hoardarr/internal/app/server"
	"github.com/jaenster/hoardarr/internal/bootstrap"
	"github.com/jaenster/hoardarr/internal/config"
)

func TestM1_Cassette(t *testing.T) {
	cassettePath, nzbPath, ok := resolveCassetteFixture(t)
	if !ok {
		t.Skip("no cassette fixture: set HOARDARR_TEST_CASSETTE+HOARDARR_TEST_NZB_PATH " +
			"or commit testdata/cassettes/m1.{jsonl,nzb}")
	}

	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.toml")
	t.Setenv("HOARDARR_LISTEN", "127.0.0.1:0")
	t.Setenv("HOARDARR_DATA_DIR", filepath.Join(dir, "data"))
	cfg, err := config.LoadOrCreate(cfgPath)
	if err != nil {
		t.Fatalf("LoadOrCreate: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	app, err := bootstrap.Build(ctx, cfg, nil, nil)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	defer func() { _ = app.Shutdown() }()

	// Add a placeholder server. Host/port are irrelevant — the replay
	// dialer ignores them — but they have to satisfy validation.
	tls := false
	srvID, err := app.ServerService.Add(ctx, appserver.AddCmd{
		Name: "cassette", Host: "127.0.0.1", Port: 119,
		TLS:      &tls,
		Username: "u", Password: "p",
		MaxConns: 1,
	})
	if err != nil {
		t.Fatalf("server add: %v", err)
	}

	nzbBytes, err := os.ReadFile(nzbPath)
	if err != nil {
		t.Fatalf("read nzb %q: %v", nzbPath, err)
	}
	jobID, err := app.AddJobService.AddJob(ctx, appdownload.AddJobCmd{
		NZB:      bytes.NewReader(nzbBytes),
		Category: "cassette",
	})
	if err != nil && !errors.Is(err, appdownload.ErrDuplicateNZB) {
		t.Fatalf("AddJob: %v", err)
	}

	srv, err := app.ServerService.Get(ctx, srvID)
	if err != nil {
		t.Fatalf("Get server: %v", err)
	}

	// Replay dialer instead of network.
	pool := nntp.NewPool(srv, nntp.PoolOptions{
		Dialer: &nntp.ReplayDialer{Path: cassettePath},
	})
	defer pool.Close()
	fetcher := appdownload.NewPoolFetcher(pool)

	// Force MaxConns=1 because the cassette captures one session;
	// parallel workers would race over the single tape.
	orch := appdownload.NewOrchestrator(
		app.JobRepo, fetcher, app.Bus, app.TxMgr,
		srv.ID(), 1, cfg.Paths.IncompleteDir,
		appdownload.OrchestratorOptions{
			MaxAttempts: 1, // Cassette has exact bytes; retries can't help.
		},
	)
	if err := orch.Run(ctx, jobID); err != nil {
		t.Fatalf("orchestrator.Run: %v", err)
	}

	final, err := app.JobRepo.ByID(ctx, jobID)
	if err != nil {
		t.Fatalf("load final: %v", err)
	}
	t.Logf("cassette job %d: state=%s done=%d total=%d failed=%d",
		final.ID(), final.State(), final.DoneBytes(), final.TotalBytes(), final.FailedBytes())

	if final.State().IsActive() {
		t.Errorf("final state = %s; want non-active", final.State())
	}
	for _, f := range final.Files() {
		path := filepath.Join(cfg.Paths.IncompleteDir,
			strconv.FormatInt(int64(jobID), 10),
			strconv.FormatInt(int64(f.ID()), 10)+".tmp")
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

// resolveCassetteFixture returns paths to the cassette + NZB in this
// order:
//  1. env $HOARDARR_TEST_CASSETTE + $HOARDARR_TEST_NZB_PATH
//  2. <repo>/testdata/cassettes/m1.jsonl + m1.nzb (committed fixture)
//
// The second path uses resolveTestPath so it works regardless of
// the test process's cwd.
func resolveCassetteFixture(t *testing.T) (cassettePath, nzbPath string, ok bool) {
	t.Helper()

	if c := os.Getenv("HOARDARR_TEST_CASSETTE"); c != "" {
		cassetteFromEnv, err := resolveTestPath(c)
		if err != nil {
			t.Fatalf("resolve cassette env: %v", err)
		}
		nzb := os.Getenv("HOARDARR_TEST_NZB_PATH")
		if nzb == "" {
			t.Fatal("HOARDARR_TEST_CASSETTE set but HOARDARR_TEST_NZB_PATH missing")
		}
		nzbAbs, err := resolveTestPath(nzb)
		if err != nil {
			t.Fatalf("resolve nzb env: %v", err)
		}
		return cassetteFromEnv, nzbAbs, true
	}

	// Default: committed fixtures.
	cas, err := resolveTestPath("internal/bootstrap/testdata/cassettes/m1.jsonl")
	if err != nil {
		return "", "", false
	}
	nzb, err := resolveTestPath("internal/bootstrap/testdata/cassettes/m1.nzb")
	if err != nil {
		return "", "", false
	}
	if _, err := os.Stat(cas); err != nil {
		return "", "", false
	}
	if _, err := os.Stat(nzb); err != nil {
		return "", "", false
	}
	return cas, nzb, true
}

// silence unused
var _ = net.JoinHostPort
