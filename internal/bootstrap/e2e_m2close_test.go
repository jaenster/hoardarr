package bootstrap_test

// M2-close end-to-end: exercise the M2 endpoints we wired late
// (history, system status, categories CRUD, paths) against the real
// HTTP stack. No NNTP, no jobs needed for most of this — but the
// history check seeds a completed Job directly through the repo so
// we can assert filtering + ordering without driving the orchestrator.

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/cookiejar"
	"path/filepath"
	"testing"
	"time"

	"github.com/jaenster/hoardarr/internal/adapter/sqlite"
	"github.com/jaenster/hoardarr/internal/api/rest"
	"github.com/jaenster/hoardarr/internal/bootstrap"
	"github.com/jaenster/hoardarr/internal/config"
	"github.com/jaenster/hoardarr/internal/domain/download"
)

func TestM2Close_E2E_HistorySystemPathsCategories(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.toml")

	t.Setenv("HOARDARR_LISTEN", "127.0.0.1:"+mustFreePort(t))
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

	client := apiKeyClient(t, apiKey)

	// --- system/status --------------------------------------------------
	t.Run("system_status", func(t *testing.T) {
		var st systemStatusResp
		mustGetJSON(t, base+"/api/v1/system/status", apiKey)
		// And via the typed client so we can assert fields:
		req, _ := http.NewRequest(http.MethodGet, base+"/api/v1/system/status", nil)
		resp := mustDo(t, client, req)
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status code %d", resp.StatusCode)
		}
		if err := json.NewDecoder(resp.Body).Decode(&st); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if st.Service != "hoardarr" {
			t.Errorf("service = %q; want hoardarr", st.Service)
		}
		if st.Version == "" {
			t.Errorf("version empty")
		}
		if st.UptimeMs < 0 {
			t.Errorf("uptime_ms negative: %d", st.UptimeMs)
		}
		if st.Queue.Active != 0 || st.Queue.Total != 0 {
			t.Errorf("expected empty queue; got active=%d total=%d", st.Queue.Active, st.Queue.Total)
		}
		// No servers preseeded → no pools.
		if len(st.Pools) != 0 {
			t.Errorf("expected 0 pools; got %d", len(st.Pools))
		}
	})

	// --- paths (read-only) ----------------------------------------------
	t.Run("paths_readonly", func(t *testing.T) {
		req, _ := http.NewRequest(http.MethodGet, base+"/api/v1/config/paths", nil)
		resp := mustDo(t, client, req)
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status code %d", resp.StatusCode)
		}
		var p pathsResp
		if err := json.NewDecoder(resp.Body).Decode(&p); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if p.IncompleteDir == "" || p.CompleteDir == "" || p.DataDir == "" {
			t.Errorf("expected non-empty paths; got %+v", p)
		}
		if p.RuntimeMutable {
			t.Errorf("expected runtime_mutable=false; got true")
		}
	})

	// --- categories CRUD ------------------------------------------------
	t.Run("categories_crud", func(t *testing.T) {
		// '*' default seeded by migration 002 should always be present.
		cats := listCategories(t, client, base)
		if !hasCategory(cats, "*") {
			t.Fatalf("default '*' category missing on fresh DB")
		}

		// Add.
		body := mustJSON(t, map[string]any{"name": "movies", "dir": "movies", "priority": 1})
		req, _ := http.NewRequest(http.MethodPost, base+"/api/v1/categories", body)
		req.Header.Set("Content-Type", "application/json")
		resp := mustDo(t, client, req)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("upsert status %d", resp.StatusCode)
		}
		cats = listCategories(t, client, base)
		if !hasCategory(cats, "movies") {
			t.Fatalf("'movies' missing after upsert")
		}

		// Update via upsert (same name, new dir).
		body = mustJSON(t, map[string]any{"name": "movies", "dir": "Films", "priority": 2})
		req, _ = http.NewRequest(http.MethodPost, base+"/api/v1/categories", body)
		req.Header.Set("Content-Type", "application/json")
		resp = mustDo(t, client, req)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("upsert update status %d", resp.StatusCode)
		}

		// Reject path traversal in dir.
		body = mustJSON(t, map[string]any{"name": "evil", "dir": "../escape"})
		req, _ = http.NewRequest(http.MethodPost, base+"/api/v1/categories", body)
		req.Header.Set("Content-Type", "application/json")
		resp = mustDo(t, client, req)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("expected 400 on traversal; got %d", resp.StatusCode)
		}

		// Reject path separator in name.
		body = mustJSON(t, map[string]any{"name": "a/b"})
		req, _ = http.NewRequest(http.MethodPost, base+"/api/v1/categories", body)
		req.Header.Set("Content-Type", "application/json")
		resp = mustDo(t, client, req)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Fatalf("expected 400 on slash in name; got %d", resp.StatusCode)
		}

		// Delete.
		req, _ = http.NewRequest(http.MethodDelete, base+"/api/v1/categories/movies", nil)
		resp = mustDo(t, client, req)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusNoContent {
			t.Fatalf("delete status %d", resp.StatusCode)
		}

		// Cannot delete reserved '*'.
		req, _ = http.NewRequest(http.MethodDelete, base+"/api/v1/categories/*", nil)
		resp = mustDo(t, client, req)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusForbidden {
			t.Fatalf("expected 403 on '*' delete; got %d", resp.StatusCode)
		}

		// Delete-missing → 404.
		req, _ = http.NewRequest(http.MethodDelete, base+"/api/v1/categories/ghost", nil)
		resp = mustDo(t, client, req)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusNotFound {
			t.Fatalf("expected 404 on missing category; got %d", resp.StatusCode)
		}
	})

	// --- history --------------------------------------------------------
	// Seed two terminal jobs directly via the repo so we don't need
	// to drive the full orchestrator — that path is covered by other
	// e2e tests. Here we're checking the History() query semantics.
	t.Run("history", func(t *testing.T) {
		repo := sqlite.NewJobRepo(app.DB)
		now := time.Now().UTC()

		// One completed in category "movies", one failed in "*".
		seedTerminalJob(t, ctx, repo, "movie-release", "movies",
			download.JobStateCompleted, now.Add(-2*time.Minute), "")
		seedTerminalJob(t, ctx, repo, "broken-release", "*",
			download.JobStateFailed, now.Add(-1*time.Minute), "boom")

		// All terminal: 2 jobs.
		all := listHistory(t, client, base, "")
		if len(all) != 2 {
			t.Fatalf("history count = %d; want 2", len(all))
		}
		// Most recent first by finished_at.
		if all[0].State != "failed" || all[1].State != "completed" {
			t.Errorf("ordering wrong: %v %v", all[0].State, all[1].State)
		}

		// state filter.
		onlyOK := listHistory(t, client, base, "?state=completed")
		if len(onlyOK) != 1 || onlyOK[0].State != "completed" {
			t.Errorf("state=completed → %+v", onlyOK)
		}

		// since filter excludes everything before the cut.
		future := now.Add(time.Hour).Format(time.RFC3339)
		none := listHistory(t, client, base, "?since="+future)
		if len(none) != 0 {
			t.Errorf("expected 0 with future since; got %d", len(none))
		}

		// limit=1 keeps the most recent.
		one := listHistory(t, client, base, "?limit=1")
		if len(one) != 1 || one[0].State != "failed" {
			t.Errorf("limit=1 should give the failed job; got %+v", one)
		}

		// Bad since → 400.
		req, _ := http.NewRequest(http.MethodGet, base+"/api/v1/history?since=nope", nil)
		resp := mustDo(t, client, req)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("bad since → %d; want 400", resp.StatusCode)
		}
	})
}

// --- helpers ---------------------------------------------------------

type systemStatusResp struct {
	Service   string `json:"service"`
	Version   string `json:"version"`
	StartedAt string `json:"started_at"`
	UptimeMs  int64  `json:"uptime_ms"`
	Queue     struct {
		Active int `json:"active"`
		Total  int `json:"total"`
	} `json:"queue"`
	Pools []struct {
		ServerID   int64  `json:"server_id"`
		ServerName string `json:"server_name"`
		MaxConns   int    `json:"max_conns"`
	} `json:"pools"`
}

type pathsResp struct {
	DataDir         string `json:"data_dir"`
	IncompleteDir   string `json:"incomplete_dir"`
	CompleteDir     string `json:"complete_dir"`
	RuntimeMutable  bool   `json:"runtime_mutable"`
	RequiresRestart bool   `json:"requires_restart"`
}

type historyJob struct {
	ID    int    `json:"id"`
	Name  string `json:"name"`
	State string `json:"state"`
	Error string `json:"error,omitempty"`
}

func apiKeyClient(t *testing.T, apiKey string) *http.Client {
	t.Helper()
	jar, err := cookiejar.New(nil)
	if err != nil {
		t.Fatalf("cookiejar: %v", err)
	}
	return &http.Client{
		Jar:     jar,
		Timeout: 10 * time.Second,
		Transport: &headerInjector{
			next:   http.DefaultTransport,
			header: "X-Api-Key",
			value:  apiKey,
		},
	}
}

type headerInjector struct {
	next   http.RoundTripper
	header string
	value  string
}

func (h *headerInjector) RoundTrip(r *http.Request) (*http.Response, error) {
	r.Header.Set(h.header, h.value)
	return h.next.RoundTrip(r)
}

func listCategories(t *testing.T, c *http.Client, base string) []rest.CategoryDTO {
	t.Helper()
	req, _ := http.NewRequest(http.MethodGet, base+"/api/v1/categories", nil)
	resp := mustDo(t, c, req)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("listCategories status %d", resp.StatusCode)
	}
	var body struct {
		Categories []rest.CategoryDTO `json:"categories"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode categories: %v", err)
	}
	return body.Categories
}

func hasCategory(cats []rest.CategoryDTO, name string) bool {
	for _, c := range cats {
		if c.Name == name {
			return true
		}
	}
	return false
}

func listHistory(t *testing.T, c *http.Client, base, query string) []historyJob {
	t.Helper()
	req, _ := http.NewRequest(http.MethodGet, base+"/api/v1/history"+query, nil)
	resp := mustDo(t, c, req)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("listHistory %s status %d: %s", query, resp.StatusCode, body)
	}
	var body struct {
		Jobs []historyJob `json:"jobs"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode history: %v", err)
	}
	return body.Jobs
}

func seedTerminalJob(t *testing.T, ctx context.Context, repo *sqlite.JobRepo,
	name, category string, state download.JobState, finishedAt time.Time, errMsg string,
) {
	t.Helper()
	j := download.HydrateJob(download.HydrateJobParams{
		ID:         0,
		NZBHash:    name + "-hash",
		Name:       name,
		Category:   category,
		Priority:   0,
		State:      state,
		AddedAt:    finishedAt.Add(-30 * time.Second),
		FinishedAt: finishedAt,
		ErrorMsg:   errMsg,
		NZBBlob:    []byte("<nzb/>"),
	})
	if err := repo.Save(ctx, j); err != nil {
		t.Fatalf("seed job %q: %v", name, err)
	}
}

// Local re-decl avoided: mustGetJSON / mustDo / mustJSON live in
// e2e_auth_test.go and bootstrap_test.go and are package-shared.
var _ = bytes.NewReader // keep bytes import alive if helpers move
