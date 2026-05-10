package bootstrap_test

// M2 end-to-end: drive the full pipeline through the public HTTP API.
//
// Flow under test:
//
//	pre-seed servers row pointing at stub NNTP
//	bootstrap.Build (pools picked up from DB)
//	POST /api/v1/queue/nzb (multipart)
//	long-running orchestrator (started by Run) sees JobCreated via bus,
//	  spins up a runner, dispatches segments, decodes, writes to disk
//	poll /api/v1/queue until job hits download_complete / completed
//	assert assembled file byte-equals original payload
//
// Mocks only the NNTP wire boundary; real SQLite, real outbox bus,
// real REST API, real long-running orchestrator service.

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"io"
	"mime/multipart"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jaenster/hoardarr/internal/adapter/sqlite"
	"github.com/jaenster/hoardarr/internal/bootstrap"
	"github.com/jaenster/hoardarr/internal/config"
	domainserver "github.com/jaenster/hoardarr/internal/domain/server"
)

func TestM2_E2E_HTTP_FullPipeline(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.toml")

	httpPort := mustFreePort(t)
	t.Setenv("HOARDARR_LISTEN", net.JoinHostPort("127.0.0.1", httpPort))
	t.Setenv("HOARDARR_DATA_DIR", filepath.Join(dir, "data"))

	cfg, err := config.LoadOrCreate(cfgPath)
	if err != nil {
		t.Fatalf("LoadOrCreate: %v", err)
	}

	// ---- payload + nzb + stub NNTP ------------------------------------
	payload := make([]byte, 8192)
	if _, err := rand.Read(payload); err != nil {
		t.Fatalf("rand: %v", err)
	}
	const partSize = 4096
	totalSize := int64(len(payload))
	segs := []e2eSegment{
		{
			MessageID: "seg1@m2.hoardarr.test",
			Begin:     1, End: partSize,
			Encoded: yencEncode("m2.bin", payload[:partSize], 1, 2, 1, partSize, totalSize),
		},
		{
			MessageID: "seg2@m2.hoardarr.test",
			Begin:     partSize + 1, End: 2 * partSize,
			Encoded: yencEncode("m2.bin", payload[partSize:], 2, 2, partSize+1, 2*partSize, totalSize),
		},
	}
	nzbXML := buildNZB("m2-release", segs)

	stub := newStubNNTP(t)
	for _, s := range segs {
		stub.addArticle(s.MessageID, s.Encoded)
	}
	defer stub.Close()

	// ---- pre-seed server registry so bootstrap builds a pool for it ----
	host, portStr, _ := net.SplitHostPort(stub.Addr())
	stubPort, _ := strconv.Atoi(portStr)
	preseedServer(t, cfg, host, stubPort)

	// ---- bootstrap App -------------------------------------------------
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	app, err := bootstrap.Build(ctx, cfg, nil, nil)
	if err != nil {
		t.Fatalf("bootstrap.Build: %v", err)
	}
	defer func() { _ = app.Shutdown() }()

	apiKey := cfg.Auth.APIKey

	// Run in goroutine; cancel terminates Run.
	runDone := make(chan error, 1)
	go func() { runDone <- app.Run(ctx) }()
	waitListen(t, cfg.Server.Listen)
	base := "http://" + cfg.Server.Listen

	// ---- POST /api/v1/queue/nzb ---------------------------------------
	jobID := uploadNZB(t, base, apiKey, "m2.nzb", nzbXML)

	// ---- poll /api/v1/queue until terminal -----------------------------
	deadline := time.Now().Add(20 * time.Second)
	var finalState string
	for time.Now().Before(deadline) {
		jobs := listQueue(t, base, apiKey, true)
		for _, j := range jobs {
			if int64(j.ID) != jobID {
				continue
			}
			finalState = j.State
		}
		if finalState == "download_complete" || finalState == "completed" || finalState == "failed" {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if finalState != "download_complete" && finalState != "completed" {
		t.Fatalf("job never reached completion; final state %q", finalState)
	}

	// ---- assert assembled file -----------------------------------------
	files := jobFiles(t, base, apiKey, jobID)
	if len(files) != 1 {
		t.Fatalf("file count = %d; want 1", len(files))
	}
	tmp := filepath.Join(cfg.Paths.IncompleteDir, strconv.FormatInt(jobID, 10),
		strconv.FormatInt(int64(files[0].ID), 10)+".tmp")
	got := mustReadFile(t, tmp)
	if !bytes.Equal(got, payload) {
		t.Errorf("assembled bytes differ from original (got=%d, want=%d)",
			len(got), len(payload))
	}

	// Trigger graceful shutdown via context cancel.
	cancel()
	select {
	case <-runDone:
	case <-time.After(5 * time.Second):
		t.Errorf("Run did not return within 5s after cancel")
	}
}

// --- helpers ---------------------------------------------------------

func preseedServer(t *testing.T, cfg config.Config, host string, port int) {
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
		Name: "stub-m2", Host: host, Port: port,
		TLS: &tlsOff, Username: "u", Password: "p", MaxConns: 4,
	}, time.Now().UTC())
	if err != nil {
		t.Fatalf("domain.New: %v", err)
	}
	repo := sqlite.NewServerRepo(db)
	if err := repo.Save(ctx, srv); err != nil {
		t.Fatalf("save server: %v", err)
	}
}

type apiJob struct {
	ID    int       `json:"id"`
	Name  string    `json:"name"`
	State string    `json:"state"`
	Files []apiFile `json:"files"`
}

type apiFile struct {
	ID       int    `json:"id"`
	Filename string `json:"filename"`
}

func uploadNZB(t *testing.T, base, apiKey, filename, xml string) int64 {
	t.Helper()
	body := &bytes.Buffer{}
	w := multipart.NewWriter(body)
	part, err := w.CreateFormFile("nzb", filename)
	if err != nil {
		t.Fatalf("CreateFormFile: %v", err)
	}
	if _, err := io.Copy(part, strings.NewReader(xml)); err != nil {
		t.Fatalf("copy: %v", err)
	}
	_ = w.Close()

	req, _ := http.NewRequest(http.MethodPost, base+"/api/v1/queue/nzb", body)
	req.Header.Set("Content-Type", w.FormDataContentType())
	req.Header.Set("X-Api-Key", apiKey)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST nzb: %v", err)
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		t.Fatalf("POST nzb status %d body=%s", resp.StatusCode, respBody)
	}
	var out struct {
		JobID int64 `json:"job_id"`
	}
	if err := json.Unmarshal(respBody, &out); err != nil {
		t.Fatalf("decode: %v body=%s", err, respBody)
	}
	if out.JobID == 0 {
		t.Fatalf("zero job_id; body=%s", respBody)
	}
	return out.JobID
}

func listQueue(t *testing.T, base, apiKey string, includeAll bool) []apiJob {
	t.Helper()
	url := base + "/api/v1/queue"
	if includeAll {
		url += "?include=all"
	}
	req, _ := http.NewRequest(http.MethodGet, url, nil)
	req.Header.Set("X-Api-Key", apiKey)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET queue: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("GET queue status %d body=%s", resp.StatusCode, body)
	}
	var out struct {
		Jobs []apiJob `json:"jobs"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	return out.Jobs
}

func jobFiles(t *testing.T, base, apiKey string, jobID int64) []apiFile {
	t.Helper()
	for _, j := range listQueue(t, base, apiKey, true) {
		if int64(j.ID) == jobID {
			return j.Files
		}
	}
	t.Fatalf("job %d not found", jobID)
	return nil
}

func mustReadFile(t *testing.T, path string) []byte {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return b
}
