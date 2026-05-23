package bootstrap_test

// E2E coverage for the download-speed observability surface:
//   - GET /api/v1/system/throughput returns the new peak + cap fields.
//   - GET /api/v1/system/speed-history?range=... reflects in-memory ring
//     samples after bytes are injected through the live throughput tracker.
//   - PUT /api/v1/config/bandwidth updates the limiter and the cap field
//     surfaces back through subsequent throughput reads.
//
// Boots a full hoardarr without any NZB pipeline — we inject bytes into
// the throughput tracker directly, which mirrors what the orchestrator
// does in production via the byte-accounter observer.

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/jaenster/hoardarr/internal/bootstrap"
	"github.com/jaenster/hoardarr/internal/config"
)

func TestE2E_SpeedHistoryAndPeaks(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.toml")
	t.Setenv("HOARDARR_LISTEN", net.JoinHostPort("127.0.0.1", mustFreePort(t)))
	t.Setenv("HOARDARR_DATA_DIR", filepath.Join(dir, "data"))
	cfg, err := config.LoadOrCreate(cfgPath)
	if err != nil {
		t.Fatalf("LoadOrCreate: %v", err)
	}
	apiKey := cfg.Auth.APIKey

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

	// Inject bytes through the live tracker — same path the orchestrator
	// uses via the byte-accounter observer.
	tp := app.SystemService.Throughput()
	if tp == nil {
		t.Fatal("expected SystemService.Throughput non-nil")
	}
	tp.Add(5 * 1024 * 1024) // 5 MB into the current second's bucket

	// Throughput endpoint: shape + values.
	thr := mustGetJSON(t, base+"/api/v1/system/throughput", apiKey)
	for _, k := range []string{
		"window_seconds", "series", "total_bytes",
		"current_bytes_per_sec", "avg10s_bytes_per_sec", "avg60s_bytes_per_sec",
		"peak_window_bytes_per_sec", "peak_alltime_bytes_per_sec", "global_cap_bytes_per_sec",
	} {
		if _, ok := thr[k]; !ok {
			t.Errorf("throughput response missing %q (got keys: %v)", k, mapKeys(thr))
		}
	}
	if v := thr["peak_alltime_bytes_per_sec"]; toInt64(t, v) < 5*1024*1024 {
		t.Errorf("peak_alltime_bytes_per_sec = %v; want >= 5 MiB", v)
	}
	if v := thr["window_seconds"]; toInt64(t, v) != 300 {
		t.Errorf("window_seconds = %v; want 300 (the default 5-min view)", v)
	}

	// Speed history endpoint: range=5m → 1s resolution, in-memory.
	hist := mustGetJSON(t, base+"/api/v1/system/speed-history?range=5m", apiKey)
	if hist["range"] != "5m" {
		t.Errorf("range = %v; want 5m", hist["range"])
	}
	if v := toInt64(t, hist["resolution_seconds"]); v != 1 {
		t.Errorf("resolution_seconds = %d; want 1 for in-memory range", v)
	}
	samples, _ := hist["samples"].([]any)
	if len(samples) == 0 {
		t.Fatal("samples empty; expected at least the current-second bucket")
	}
	// At least one sample should be non-zero (the 5 MB we just injected).
	var sawNonZero bool
	for _, s := range samples {
		m, _ := s.(map[string]any)
		if toInt64(t, m["bytes_per_sec"]) > 0 {
			sawNonZero = true
			break
		}
	}
	if !sawNonZero {
		t.Errorf("no non-zero samples in 5m history despite injected bytes")
	}

	// Speed history endpoint: range=24h falls back to the DB store
	// (empty here since the flusher only ticks once per minute and
	// hasn't fired). Endpoint should still respond cleanly.
	day := mustGetJSON(t, base+"/api/v1/system/speed-history?range=24h", apiKey)
	if day["range"] != "24h" {
		t.Errorf("range = %v; want 24h", day["range"])
	}
	if v := toInt64(t, day["resolution_seconds"]); v != 60 {
		t.Errorf("resolution_seconds = %d; want 60 for DB-backed range", v)
	}
	if _, ok := day["samples"]; !ok {
		t.Errorf("samples missing from 24h response")
	}

	// Bad range value → 400.
	{
		req, _ := http.NewRequest(http.MethodGet, base+"/api/v1/system/speed-history?range=bogus", nil)
		req.Header.Set("X-Api-Key", apiKey)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("bad-range GET: %v", err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("bogus range status = %d; want 400", resp.StatusCode)
		}
	}

	// Set the bandwidth cap and confirm it surfaces back through
	// /system/throughput's global_cap_bytes_per_sec.
	capBody := []byte(`{"global_bytes_per_sec": 8388608}`) // 8 MiB/s
	req, _ := http.NewRequest(http.MethodPut, base+"/api/v1/config/bandwidth", bytes.NewReader(capBody))
	req.Header.Set("X-Api-Key", apiKey)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("PUT bandwidth: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("PUT bandwidth status = %d body=%s", resp.StatusCode, body)
	}

	thr2 := mustGetJSON(t, base+"/api/v1/system/throughput", apiKey)
	if v := toInt64(t, thr2["global_cap_bytes_per_sec"]); v != 8388608 {
		t.Errorf("global_cap_bytes_per_sec = %d; want 8388608", v)
	}

	cancel()
	select {
	case <-runDone:
	case <-time.After(5 * time.Second):
		t.Errorf("Run did not return within 5s after cancel")
	}
}

func toInt64(t *testing.T, v any) int64 {
	t.Helper()
	switch x := v.(type) {
	case float64:
		return int64(x)
	case json.Number:
		n, _ := x.Int64()
		return n
	case int64:
		return x
	default:
		t.Fatalf("expected number, got %T: %v", v, v)
		return 0
	}
}

func mapKeys(m map[string]any) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
