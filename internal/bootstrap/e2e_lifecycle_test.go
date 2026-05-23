package bootstrap_test

// Lifecycle e2e tests for queue state transitions while a runner has
// segments in flight: pause / resume / remove.
//
// Reuses the gatedStubNNTP from e2e_crash_recovery_test.go to drive
// timing deterministically.

import (
	"bytes"
	"context"
	"crypto/rand"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/jaenster/hoardarr/internal/bootstrap"
	"github.com/jaenster/hoardarr/internal/config"
)

// TestM2_E2E_PauseResume drops a 2-segment NZB, releases the first
// segment, then pauses the job before the second segment is releasable.
// The orchestrator must:
//   - cancel the runner cleanly on JobPaused
//   - leave segment 2 in pending state (NOT failed)
//   - resume cleanly on JobResumed and finish the job
func TestM2_E2E_PauseResume(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.toml")

	httpPort := mustFreePort(t)
	t.Setenv("HOARDARR_LISTEN", net.JoinHostPort("127.0.0.1", httpPort))
	t.Setenv("HOARDARR_DATA_DIR", filepath.Join(dir, "data"))

	cfg, err := config.LoadOrCreate(cfgPath)
	if err != nil {
		t.Fatalf("LoadOrCreate: %v", err)
	}

	payload := make([]byte, 8192)
	_, _ = rand.Read(payload)
	const partSize = 4096
	totalSize := int64(len(payload))
	segs := []e2eSegment{
		{
			MessageID: "seg1@pause.hoardarr.test",
			Begin:     1, End: partSize,
			Encoded: yencEncode("p.bin", payload[:partSize], 1, 2, 1, partSize, totalSize),
		},
		{
			MessageID: "seg2@pause.hoardarr.test",
			Begin:     partSize + 1, End: 2 * partSize,
			Encoded: yencEncode("p.bin", payload[partSize:], 2, 2, partSize+1, 2*partSize, totalSize),
		},
	}
	nzbXML := buildNZB("pause-release", segs)

	stub := newGatedStubNNTP(t)
	for _, s := range segs {
		stub.add(s.MessageID, s.Encoded)
	}
	defer stub.Close()

	host, portStr, _ := net.SplitHostPort(stub.Addr())
	stubPort, _ := strconv.Atoi(portStr)
	preseedServer(t, cfg, host, stubPort)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	app, err := bootstrap.Build(ctx, cfg, nil, nil,
		// Production polls outbox every 5s; lifecycle tests exercise
		// pause/resume retry timing in real-time so they need fast
		// dispatcher reaction.
		bootstrap.WithOutboxPollInterval(50*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	defer func() { _ = app.Shutdown() }()

	apiKey := cfg.Auth.APIKey
	runDone := make(chan error, 1)
	go func() { runDone <- app.Run(ctx) }()
	waitListen(t, cfg.Server.Listen)
	base := "http://" + cfg.Server.Listen

	jobID := uploadNZB(t, base, apiKey, "pause.nzb", nzbXML)
	stub.release(segs[0].MessageID)
	waitSegmentState(t, app, jobID, segs[0].MessageID, "done", 5*time.Second)

	// Pause via the public REST API — exercises the same path the UI
	// uses (handler → QueueService.PauseJob → bus event → service
	// cancels the runner).
	postEmpty(t, base, apiKey, "/api/v1/queue/"+strconv.FormatInt(jobID, 10)+"/pause")

	// Wait for the job state to settle on "paused" in the DB. The
	// orchestrator might still be unwinding when the API returns.
	waitJobState(t, app, jobID, "paused", 3*time.Second)
	verifySegmentNotFailed(t, app, jobID, segs[1].MessageID)

	// Resume.
	postEmpty(t, base, apiKey, "/api/v1/queue/"+strconv.FormatInt(jobID, 10)+"/resume")

	// Release segment 2 and wait for completion.
	stub.release(segs[1].MessageID)
	waitJobComplete(t, app, jobID, 5*time.Second)

	// Verify assembled file.
	final, _ := app.QueueService.Get(ctx, finalJobID(jobID))
	files := final.Files()
	tmp := filepath.Join(cfg.Paths.IncompleteDir, strconv.FormatInt(jobID, 10),
		strconv.FormatInt(int64(files[0].ID()), 10)+".tmp")
	got, err := os.ReadFile(tmp)
	if err != nil {
		t.Fatalf("read assembled: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Errorf("assembled bytes differ from original")
	}

	cancel()
	select {
	case <-runDone:
	case <-time.After(5 * time.Second):
		t.Errorf("Run did not return within 5s")
	}
}

// TestM2_E2E_RemoveMidFlight verifies that DELETE /api/v1/queue/{id}
// while a segment is in flight: cancels the runner cleanly, removes
// the row, and purges the per-job incomplete directory from disk.
func TestM2_E2E_RemoveMidFlight(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.toml")

	httpPort := mustFreePort(t)
	t.Setenv("HOARDARR_LISTEN", net.JoinHostPort("127.0.0.1", httpPort))
	t.Setenv("HOARDARR_DATA_DIR", filepath.Join(dir, "data"))

	cfg, err := config.LoadOrCreate(cfgPath)
	if err != nil {
		t.Fatalf("LoadOrCreate: %v", err)
	}

	payload := make([]byte, 8192)
	_, _ = rand.Read(payload)
	const partSize = 4096
	totalSize := int64(len(payload))
	segs := []e2eSegment{
		{
			MessageID: "seg1@remove.hoardarr.test",
			Begin:     1, End: partSize,
			Encoded: yencEncode("r.bin", payload[:partSize], 1, 2, 1, partSize, totalSize),
		},
		{
			MessageID: "seg2@remove.hoardarr.test",
			Begin:     partSize + 1, End: 2 * partSize,
			Encoded: yencEncode("r.bin", payload[partSize:], 2, 2, partSize+1, 2*partSize, totalSize),
		},
	}
	nzbXML := buildNZB("remove-release", segs)

	stub := newGatedStubNNTP(t)
	for _, s := range segs {
		stub.add(s.MessageID, s.Encoded)
	}
	defer stub.Close()

	host, portStr, _ := net.SplitHostPort(stub.Addr())
	stubPort, _ := strconv.Atoi(portStr)
	preseedServer(t, cfg, host, stubPort)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	app, err := bootstrap.Build(ctx, cfg, nil, nil,
		// Production polls outbox every 5s; lifecycle tests exercise
		// pause/resume retry timing in real-time so they need fast
		// dispatcher reaction.
		bootstrap.WithOutboxPollInterval(50*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	defer func() { _ = app.Shutdown() }()

	apiKey := cfg.Auth.APIKey
	runDone := make(chan error, 1)
	go func() { runDone <- app.Run(ctx) }()
	waitListen(t, cfg.Server.Listen)
	base := "http://" + cfg.Server.Listen

	jobID := uploadNZB(t, base, apiKey, "remove.nzb", nzbXML)
	stub.release(segs[0].MessageID)
	waitSegmentState(t, app, jobID, segs[0].MessageID, "done", 5*time.Second)

	jobDir := filepath.Join(cfg.Paths.IncompleteDir, strconv.FormatInt(jobID, 10))
	if _, err := os.Stat(jobDir); err != nil {
		t.Fatalf("incomplete dir not created: %v", err)
	}

	// Remove via REST API.
	deleteResource(t, base, apiKey, "/api/v1/queue/"+strconv.FormatInt(jobID, 10))

	// Wait for the runner to fully exit and the dir to be purged.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(jobDir); os.IsNotExist(err) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if _, err := os.Stat(jobDir); !os.IsNotExist(err) {
		t.Errorf("incomplete dir still present after RemoveJob: %v", err)
	}

	// Job should be gone from the DB.
	if _, err := app.QueueService.Get(ctx, finalJobID(jobID)); err == nil {
		t.Errorf("job still in DB after RemoveJob")
	}

	cancel()
	select {
	case <-runDone:
	case <-time.After(5 * time.Second):
		t.Errorf("Run did not return within 5s")
	}
}

// --- helpers ---------------------------------------------------------

func waitJobState(t *testing.T, app *bootstrap.App, jobID int64, want string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		j, err := app.QueueService.Get(context.Background(), finalJobID(jobID))
		if err == nil && string(j.State()) == want {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("job %d never reached state %q", jobID, want)
}

func postEmpty(t *testing.T, base, apiKey, path string) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, base+path, nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	req.Header.Set("X-Api-Key", apiKey)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST %s: %v", path, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		t.Fatalf("POST %s: status %d", path, resp.StatusCode)
	}
}

func deleteResource(t *testing.T, base, apiKey, path string) {
	t.Helper()
	req, err := http.NewRequest(http.MethodDelete, base+path, nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	req.Header.Set("X-Api-Key", apiKey)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("DELETE %s: %v", path, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		t.Fatalf("DELETE %s: status %d", path, resp.StatusCode)
	}
}
