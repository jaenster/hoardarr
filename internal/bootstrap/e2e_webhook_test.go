package bootstrap_test

// Webhook end-to-end: an httptest server captures POSTs; we register
// a subscription via REST, fire a bus event (using app.Bus.Publish
// from a tx), and assert the receiver got it.
//
// HMAC: we set a secret and verify the signature header matches.

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jaenster/hoardarr/internal/adapter/notify/webhook"
	"github.com/jaenster/hoardarr/internal/bootstrap"
	"github.com/jaenster/hoardarr/internal/config"
	"github.com/jaenster/hoardarr/internal/domain/download"
)

func TestWebhook_E2E_DispatchAndHMAC(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.toml")
	t.Setenv("HOARDARR_LISTEN", "127.0.0.1:"+mustFreePort(t))
	t.Setenv("HOARDARR_DATA_DIR", filepath.Join(dir, "data"))
	cfg, err := config.LoadOrCreate(cfgPath)
	if err != nil {
		t.Fatalf("LoadOrCreate: %v", err)
	}
	apiKey := cfg.Auth.APIKey

	// Capturing httptest server.
	var (
		mu       sync.Mutex
		received []capturedDelivery
	)
	receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		received = append(received, capturedDelivery{
			topic:     r.Header.Get(webhook.EventHeader),
			signature: r.Header.Get(webhook.SignatureHeader),
			body:      body,
		})
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer receiver.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	app, err := bootstrap.Build(ctx, cfg, nil, nil)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	defer func() { _ = app.Shutdown() }()

	runDone := make(chan error, 1)
	go func() { runDone <- app.Run(ctx) }()
	waitListen(t, cfg.Server.Listen)
	base := "http://" + cfg.Server.Listen

	// Register a subscription via the REST API.
	const secret = "my-shared-secret"
	body := mustJSON(t, map[string]any{
		"name":   "test-receiver",
		"url":    receiver.URL,
		"topics": []string{"download.job.completed"},
		"secret": secret,
	})
	req, _ := http.NewRequest(http.MethodPost, base+"/api/v1/subscriptions", body)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Api-Key", apiKey)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("create subscription: %v", err)
	}
	if resp.StatusCode != http.StatusCreated {
		buf, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		t.Fatalf("create status %d: %s", resp.StatusCode, buf)
	}
	_ = resp.Body.Close()

	// Drive an event through the bus by publishing JobCompleted
	// inside a tx — same path real services use.
	jobID := download.JobID(42)
	if err := app.TxMgr.InTx(ctx, func(ctx context.Context) error {
		return app.Bus.Publish(ctx, download.JobCompleted{
			JobID: jobID, At: time.Now().UTC(),
		})
	}); err != nil {
		t.Fatalf("publish: %v", err)
	}

	// Wait for at least one delivery.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		n := len(received)
		mu.Unlock()
		if n > 0 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Snapshot under the lock, then release — the upcoming /test call
	// posts back to the same receiver, which needs the mutex to
	// append the second delivery. Holding the lock across that call
	// deadlocks the receiver and looks like a sender timeout.
	mu.Lock()
	snapshot := append([]capturedDelivery(nil), received...)
	mu.Unlock()
	if len(snapshot) == 0 {
		t.Fatalf("no webhook deliveries received")
	}
	got := snapshot[0]

	if got.topic != "download.job.completed" {
		t.Errorf("X-Hoardarr-Event = %q; want download.job.completed", got.topic)
	}

	// HMAC must match: sha256=hex(HMAC-SHA256(body, secret)).
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(got.body)
	want := "sha256=" + hex.EncodeToString(mac.Sum(nil))
	if got.signature != want {
		t.Errorf("HMAC mismatch:\n  got  %s\n  want %s", got.signature, want)
	}

	// Body must JSON-decode into an envelope; payload contains the job id.
	var env map[string]any
	if err := json.Unmarshal(got.body, &env); err != nil {
		t.Fatalf("body not JSON: %v\n%s", err, got.body)
	}
	if env["Topic"] != "download.job.completed" {
		t.Errorf("envelope Topic = %v; want download.job.completed", env["Topic"])
	}

	// Test endpoint should also work.
	// list to find the id we just created.
	req, _ = http.NewRequest(http.MethodGet, base+"/api/v1/subscriptions", nil)
	req.Header.Set("X-Api-Key", apiKey)
	resp, _ = http.DefaultClient.Do(req)
	defer resp.Body.Close()
	var listBody struct {
		Subscriptions []struct {
			ID int64 `json:"id"`
		} `json:"subscriptions"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&listBody)
	if len(listBody.Subscriptions) == 0 {
		t.Fatal("subscription list empty")
	}
	id := listBody.Subscriptions[0].ID
	req, _ = http.NewRequest(http.MethodPost,
		base+"/api/v1/subscriptions/"+strconv.FormatInt(id, 10)+"/test", nil)
	req.Header.Set("X-Api-Key", apiKey)
	resp, _ = http.DefaultClient.Do(req)
	testBody, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Errorf("/test status %d; want 204; body=%s", resp.StatusCode, testBody)
	}

	// The test endpoint adds another delivery. Re-snapshot to compare.
	deadline = time.Now().Add(2 * time.Second)
	var finalSnapshot []capturedDelivery
	for time.Now().Before(deadline) {
		mu.Lock()
		finalSnapshot = append([]capturedDelivery(nil), received...)
		mu.Unlock()
		if len(finalSnapshot) >= 2 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if len(finalSnapshot) < 2 {
		t.Errorf("expected ≥2 deliveries (one bus event + one test); got %d", len(finalSnapshot))
	}
	// The test delivery should carry topic "notify.test".
	var sawTest bool
	for _, d := range finalSnapshot {
		if d.topic == "notify.test" {
			sawTest = true
			break
		}
	}
	if !sawTest {
		var topics []string
		for _, d := range finalSnapshot {
			topics = append(topics, d.topic)
		}
		t.Errorf("no notify.test delivery; got topics: %s", strings.Join(topics, ", "))
	}

	cancel()
	select {
	case <-runDone:
	case <-time.After(5 * time.Second):
		t.Errorf("Run did not return within 5s")
	}
}

type capturedDelivery struct {
	topic     string
	signature string
	body      []byte
}
