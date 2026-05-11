package bootstrap_test

// Boot hoardarr with NO servers configured, then add a server via
// HTTP and drop an NZB. The orchestrator should hot-wire a pool off
// the server.usenet.added event and complete the job without a
// restart. Regression for the "server in UI → must restart" wart.

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"mime/multipart"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/jaenster/hoardarr/internal/bootstrap"
	"github.com/jaenster/hoardarr/internal/config"
	testnntp "github.com/jaenster/hoardarr/internal/testserver/nntp"
)

func TestE2E_HotwirePoolAfterServerAdd(t *testing.T) {
	fake, err := testnntp.Start(testnntp.Options{
		BytesPerSec: 256 * 1024,
	})
	if err != nil {
		t.Fatalf("testnntp.Start: %v", err)
	}
	defer fake.Stop()

	const totalSize = 32 * 1024
	_, spec := fake.SynthesizeFile("hotwire-seg", "hotwire.bin", totalSize, 1)
	nzb := testnntp.BuildNZB([]testnntp.FileSpec{spec})

	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.toml")
	t.Setenv("HOARDARR_LISTEN", "127.0.0.1:"+mustFreePort(t))
	t.Setenv("HOARDARR_DATA_DIR", filepath.Join(dir, "data"))
	cfg, err := config.LoadOrCreate(cfgPath)
	if err != nil {
		t.Fatalf("LoadOrCreate: %v", err)
	}
	apiKey := cfg.Auth.APIKey

	// NB: no preseed — boot with zero servers so we can prove that
	// adding one after start wires up a pool live.
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
	defer func() {
		cancel()
		select {
		case <-runDone:
		case <-time.After(5 * time.Second):
			t.Errorf("Run did not return within 5s after cancel")
		}
	}()

	// POST a server via HTTP. The orchestrator subscribes to
	// server.usenet.added and should build a pool from this.
	addBody, _ := json.Marshal(map[string]any{
		"name":      "fake-hotwire",
		"host":      fake.Host(),
		"port":      fake.Port(),
		"tls":       false,
		"max_conns": 4,
	})
	addReq, _ := http.NewRequest(http.MethodPost, base+"/api/v1/servers", bytes.NewReader(addBody))
	addReq.Header.Set("Content-Type", "application/json")
	addReq.Header.Set("X-Api-Key", apiKey)
	addResp, err := http.DefaultClient.Do(addReq)
	if err != nil {
		t.Fatalf("POST /servers: %v", err)
	}
	addResp.Body.Close()
	if addResp.StatusCode != http.StatusCreated {
		t.Fatalf("POST /servers status=%d", addResp.StatusCode)
	}

	// Give the outbox dispatcher a moment to publish the
	// server.usenet.added event to the orchestrator.
	time.Sleep(150 * time.Millisecond)

	// Drop the NZB. With the pool hot-wired, the orchestrator should
	// drive it to completion without any restart.
	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	fw, _ := mw.CreateFormFile("nzb", "hotwire.nzb")
	_, _ = fw.Write(nzb)
	_ = mw.Close()
	upReq, _ := http.NewRequest(http.MethodPost, base+"/api/v1/queue/nzb", &buf)
	upReq.Header.Set("Content-Type", mw.FormDataContentType())
	upReq.Header.Set("X-Api-Key", apiKey)
	upResp, err := http.DefaultClient.Do(upReq)
	if err != nil {
		t.Fatalf("POST /nzb: %v", err)
	}
	defer upResp.Body.Close()
	if upResp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(upResp.Body)
		t.Fatalf("upload status=%d body=%s", upResp.StatusCode, body)
	}
	var addedJob struct {
		JobID int64 `json:"job_id"`
	}
	_ = json.NewDecoder(upResp.Body).Decode(&addedJob)

	// Poll until the job completes (or times out).
	deadline := time.Now().Add(15 * time.Second)
	var finalState string
	for time.Now().Before(deadline) {
		state, _ := pollJobState(t, base, apiKey, addedJob.JobID)
		finalState = state
		if state == "download_complete" || state == "completed" || state == "failed" {
			break
		}
		time.Sleep(75 * time.Millisecond)
	}
	if finalState != "download_complete" && finalState != "completed" {
		t.Fatalf("job did not complete; final state %q", finalState)
	}
}
