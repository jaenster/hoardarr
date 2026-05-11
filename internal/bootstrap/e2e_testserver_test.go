package bootstrap_test

// End-to-end against the new testserver/nntp library: spin up the
// fake NNTP server with realism knobs (bytes-per-second, latency),
// seed an NZB, drop it via HTTP, watch the job complete, assert the
// assembled bytes match the originals.
//
// This is the foundation Playwright leans on — if hoardarr can drive
// the fake to completion in-process, the harness shape is sound.

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"mime/multipart"
	"net/http"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/jaenster/hoardarr/internal/adapter/sqlite"
	"github.com/jaenster/hoardarr/internal/bootstrap"
	"github.com/jaenster/hoardarr/internal/config"
	domainserver "github.com/jaenster/hoardarr/internal/domain/server"
	testnntp "github.com/jaenster/hoardarr/internal/testserver/nntp"
)

func TestE2E_FakeNNTP_Pipeline(t *testing.T) {
	// --- fake NNTP -----------------------------------------------------
	fake, err := testnntp.Start(testnntp.Options{
		// Slow enough that progress bars would visibly tick in a
		// browser, fast enough that this test finishes in ~2s.
		BytesPerSec:    256 * 1024,
		ArticleLatency: 30 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("testnntp.Start: %v", err)
	}
	defer fake.Stop()

	// One 64 KiB file split into two equal yEnc multi-part segments.
	// SynthesizeFile sets =ypart so the receiver assembles them at
	// the right offsets.
	const totalSize = 64 * 1024
	want, spec := fake.SynthesizeFile("fake-seg", "fake.bin", totalSize, 2)
	nzb := testnntp.BuildNZB([]testnntp.FileSpec{spec})

	// --- hoardarr ------------------------------------------------------
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.toml")
	t.Setenv("HOARDARR_LISTEN", "127.0.0.1:"+mustFreePort(t))
	t.Setenv("HOARDARR_DATA_DIR", filepath.Join(dir, "data"))

	cfg, err := config.LoadOrCreate(cfgPath)
	if err != nil {
		t.Fatalf("LoadOrCreate: %v", err)
	}
	apiKey := cfg.Auth.APIKey

	// Pre-seed a server pointing at the fake. bootstrap.Build picks
	// servers up off the repo at start.
	preseedTestserver(t, cfg, fake.Host(), fake.Port())

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	app, err := bootstrap.Build(ctx, cfg, nil, nil)
	if err != nil {
		t.Fatalf("bootstrap.Build: %v", err)
	}
	defer func() { _ = app.Shutdown() }()
	runDone := make(chan error, 1)
	go func() { runDone <- app.Run(ctx) }()
	waitListen(t, cfg.Server.Listen)
	base := "http://" + cfg.Server.Listen

	// --- upload NZB ----------------------------------------------------
	jobID := uploadNZBMultipart(t, base, apiKey, "fake.nzb", nzb)

	// --- poll until complete -------------------------------------------
	deadline := time.Now().Add(20 * time.Second)
	var finalState string
	var doneBytes int64
	for time.Now().Before(deadline) {
		state, done := pollJobState(t, base, apiKey, jobID)
		finalState = state
		doneBytes = done
		if state == "download_complete" || state == "completed" || state == "failed" {
			break
		}
		time.Sleep(75 * time.Millisecond)
	}
	if finalState != "download_complete" && finalState != "completed" {
		t.Fatalf("job never reached completion; final state %q, done=%d", finalState, doneBytes)
	}

	// --- assert bytes --------------------------------------------------
	files := jobFiles(t, base, apiKey, jobID)
	if len(files) != 1 {
		t.Fatalf("file count = %d; want 1", len(files))
	}
	tmp := filepath.Join(cfg.Paths.IncompleteDir, strconv.FormatInt(jobID, 10),
		strconv.FormatInt(int64(files[0].ID), 10)+".tmp")
	got := mustReadFile(t, tmp)
	if !bytes.Equal(got, want) {
		t.Errorf("assembled bytes mismatch (got=%d want=%d)", len(got), len(want))
	}

	cancel()
	select {
	case <-runDone:
	case <-time.After(5 * time.Second):
		t.Errorf("Run did not return within 5s after cancel")
	}
}

// --- helpers --------------------------------------------------------

func preseedTestserver(t *testing.T, cfg config.Config, host string, port int) {
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
	tlsOff := false
	srv, err := domainserver.New(domainserver.NewParams{
		Name: "fake-nntp", Host: host, Port: port,
		TLS: &tlsOff, MaxConns: 4,
	}, time.Now().UTC())
	if err != nil {
		t.Fatalf("domain.New: %v", err)
	}
	repo := sqlite.NewServerRepo(db)
	if err := repo.Save(ctx, srv); err != nil {
		t.Fatalf("save server: %v", err)
	}
}

func uploadNZBMultipart(t *testing.T, base, apiKey, filename string, body []byte) int64 {
	t.Helper()
	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	fw, err := mw.CreateFormFile("nzb", filename)
	if err != nil {
		t.Fatalf("CreateFormFile: %v", err)
	}
	if _, err := fw.Write(body); err != nil {
		t.Fatalf("write nzb: %v", err)
	}
	_ = mw.Close()

	req, _ := http.NewRequest(http.MethodPost, base+"/api/v1/queue/nzb", &buf)
	req.Header.Set("Content-Type", mw.FormDataContentType())
	req.Header.Set("X-Api-Key", apiKey)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("upload: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("upload status=%d body=%s", resp.StatusCode, b)
	}
	var out struct {
		JobID int64 `json:"job_id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if out.JobID == 0 {
		t.Fatalf("upload returned job_id=0")
	}
	return out.JobID
}

func pollJobState(t *testing.T, base, apiKey string, jobID int64) (string, int64) {
	t.Helper()
	req, _ := http.NewRequest(http.MethodGet, base+"/api/v1/queue?include=all", nil)
	req.Header.Set("X-Api-Key", apiKey)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("queue: %v", err)
	}
	defer resp.Body.Close()
	var body struct {
		Jobs []struct {
			ID        int64  `json:"id"`
			State     string `json:"state"`
			DoneBytes int64  `json:"done_bytes"`
		} `json:"jobs"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&body)
	for _, j := range body.Jobs {
		if j.ID == jobID {
			return j.State, j.DoneBytes
		}
	}
	return "", 0
}
