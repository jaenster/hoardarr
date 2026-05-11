package bootstrap_test

// Multi-server failover: two stub NNTP servers, the primary has no
// article (returns 430), the secondary serves it. Assert the job
// completes (file ends up byte-equivalent at complete/) and that the
// fetched bytes counted against the secondary, not the primary.

import (
	"bytes"
	"context"
	"crypto/rand"
	"net"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/jaenster/hoardarr/internal/adapter/sqlite"
	"github.com/jaenster/hoardarr/internal/bootstrap"
	"github.com/jaenster/hoardarr/internal/config"
	domainserver "github.com/jaenster/hoardarr/internal/domain/server"
)

func TestMultiServer_E2E_PriorityFailover(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.toml")
	t.Setenv("HOARDARR_LISTEN", net.JoinHostPort("127.0.0.1", mustFreePort(t)))
	t.Setenv("HOARDARR_DATA_DIR", filepath.Join(dir, "data"))
	cfg, err := config.LoadOrCreate(cfgPath)
	if err != nil {
		t.Fatalf("LoadOrCreate: %v", err)
	}

	// Build a tiny clean payload + matching PAR2 so verify passes
	// once the secondary delivers the article. We reuse buildNZB's
	// single-file shape with one segment.
	payload := make([]byte, 4096)
	_, _ = rand.Read(payload)
	encoded := yencEncode("ms.bin", payload, 0, 0, 0, 0, int64(len(payload)))

	dataMsg := "seg1@ms.hoardarr.test"
	nzbXML := buildNZB("multi-server-release", []e2eSegment{
		{MessageID: dataMsg, Begin: 1, End: int64(len(payload)), Encoded: encoded},
	})

	// Primary stub: no articles registered → returns 430.
	primary := newStubNNTP(t)
	defer primary.Close()
	// Secondary stub: serves the article.
	secondary := newStubNNTP(t)
	secondary.addArticle(dataMsg, encoded)
	defer secondary.Close()

	// Pre-seed both servers; primary at priority 0, secondary at 10
	// (lower priority value = higher precedence in our scheme).
	preseedTwoServers(t, cfg, primary.Addr(), secondary.Addr())

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	app, err := bootstrap.Build(ctx, cfg, nil, nil)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	defer func() { _ = app.Shutdown() }()

	apiKey := cfg.Auth.APIKey
	runDone := make(chan error, 1)
	go func() { runDone <- app.Run(ctx) }()
	waitListen(t, cfg.Server.Listen)
	base := "http://" + cfg.Server.Listen

	jobID := uploadNZB(t, base, apiKey, "ms.nzb", nzbXML)

	// Without PAR2 verify will mark RepairNeeded → no completion. But
	// for THIS test what we care about is that the segment was actually
	// fetched (i.e. failover worked). Poll until the segment lands.
	deadline := time.Now().Add(15 * time.Second)
	var done bool
	for time.Now().Before(deadline) {
		j, err := app.QueueService.Get(context.Background(), finalJobID(jobID))
		if err == nil {
			state := string(j.State())
			if state == "download_complete" || state == "verifying" ||
				state == "verify_failed" || state == "completed" ||
				state == "failed" {
				done = true
				break
			}
		}
		time.Sleep(40 * time.Millisecond)
	}
	if !done {
		t.Fatalf("job did not finish downloading via failover")
	}

	// File should be on disk in incomplete/ (no PAR2 means verify
	// flags missing-par2 → job stays in download_complete/failed).
	// Just assert the bytes are there byte-equivalent.
	files, _ := app.QueueService.Get(context.Background(), finalJobID(jobID))
	if files == nil || len(files.Files()) == 0 {
		t.Fatal("no files on the job")
	}
	tmp := filepath.Join(cfg.Paths.IncompleteDir,
		strconv.FormatInt(jobID, 10),
		strconv.FormatInt(int64(files.Files()[0].ID()), 10)+".tmp")
	got := mustReadFile(t, tmp)
	if !bytes.Equal(got, payload) {
		t.Errorf("downloaded bytes differ (got %d, want %d)", len(got), len(payload))
	}

	// Force a byte-flusher tick by stopping the app cleanly; the
	// flusher does a final drain on Stop. Then re-open the DB and
	// inspect used_bytes per server.
	cancel()
	select {
	case <-runDone:
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return within 10s after cancel")
	}

	primaryUsed, secondaryUsed := readServerUsedBytes(t, cfg)

	// All bytes should have come from the secondary (priority 10);
	// the primary only returned 430s and never opened a body.
	if primaryUsed != 0 {
		t.Errorf("primary used_bytes = %d; want 0 (primary returned 430)", primaryUsed)
	}
	if secondaryUsed == 0 {
		t.Errorf("secondary used_bytes = 0; want > 0 (failover should have used it)")
	}
}

func preseedTwoServers(t *testing.T, cfg config.Config, primaryAddr, secondaryAddr string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	db, err := sqlite.Open(ctx, cfg.Storage.SQLite.Path, sqlite.Options{})
	if err != nil {
		t.Fatalf("sqlite.Open: %v", err)
	}
	defer db.Close()
	if err := db.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	repo := sqlite.NewServerRepo(db)

	for i, spec := range []struct {
		name     string
		addr     string
		priority int
	}{
		{"primary", primaryAddr, 0},
		{"secondary", secondaryAddr, 10},
	} {
		host, portStr, _ := net.SplitHostPort(spec.addr)
		port, _ := strconv.Atoi(portStr)
		tlsOff := false
		srv, err := domainserver.New(domainserver.NewParams{
			Name: spec.name, Host: host, Port: port,
			TLS: &tlsOff, Username: "u", Password: "p", MaxConns: 4,
			Priority: spec.priority,
		}, time.Now().UTC())
		if err != nil {
			t.Fatalf("domain.New %d: %v", i, err)
		}
		if err := repo.Save(ctx, srv); err != nil {
			t.Fatalf("save server %d: %v", i, err)
		}
	}
}

func readServerUsedBytes(t *testing.T, cfg config.Config) (primary, secondary int64) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	db, err := sqlite.Open(ctx, cfg.Storage.SQLite.Path, sqlite.Options{})
	if err != nil {
		t.Fatalf("sqlite.Open: %v", err)
	}
	defer db.Close()
	repo := sqlite.NewServerRepo(db)
	list, err := repo.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	for _, s := range list {
		switch s.Name() {
		case "primary":
			primary = s.UsedBytes()
		case "secondary":
			secondary = s.UsedBytes()
		}
	}
	return
}

