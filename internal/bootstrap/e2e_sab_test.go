package bootstrap_test

// SAB API e2e: exercises the /sabnzbd/api surface that Sonarr / Radarr
// etc. talk to. We don't yet have a Docker harness pointing real
// *arr containers at hoardarr — that's the cassette-style follow-up.
// This test pins the JSON shapes so consumers don't break silently.

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"mime/multipart"
	"net/http"
	"net/url"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jaenster/hoardarr/internal/bootstrap"
	"github.com/jaenster/hoardarr/internal/config"
)

func TestSAB_E2E_PhaseOneModes(t *testing.T) {
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
		t.Fatalf("Build: %v", err)
	}
	defer func() { _ = app.Shutdown() }()

	runDone := make(chan error, 1)
	go func() { runDone <- app.Run(ctx) }()
	waitListen(t, cfg.Server.Listen)
	base := "http://" + cfg.Server.Listen + "/sabnzbd/api"
	defer func() {
		cancel()
		select {
		case <-runDone:
		case <-time.After(5 * time.Second):
			t.Errorf("Run did not return within 5s after cancel")
		}
	}()

	// --- bad apikey → 401 -----------------------------------------------
	t.Run("apikey_required", func(t *testing.T) {
		resp := sabGet(t, base, url.Values{"mode": {"version"}, "apikey": {"nope"}})
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusUnauthorized {
			t.Errorf("bad apikey → %d; want 401", resp.StatusCode)
		}
	})

	// --- version --------------------------------------------------------
	t.Run("version", func(t *testing.T) {
		out := sabGetJSON(t, base, url.Values{"mode": {"version"}, "apikey": {apiKey}})
		v, _ := out["version"].(string)
		// We claim to be SAB v3.x so *arr accepts us; the exact bump
		// is fine to evolve, but stay >=3.0.0.
		if !strings.HasPrefix(v, "3.") {
			t.Errorf("version = %q; want 3.x", v)
		}
	})

	// --- get_cats -------------------------------------------------------
	t.Run("get_cats", func(t *testing.T) {
		out := sabGetJSON(t, base, url.Values{"mode": {"get_cats"}, "apikey": {apiKey}})
		cats, ok := out["categories"].([]any)
		if !ok {
			t.Fatalf("categories not a list: %T", out["categories"])
		}
		seenStar := false
		for _, c := range cats {
			if s, _ := c.(string); s == "*" {
				seenStar = true
			}
		}
		if !seenStar {
			t.Errorf("default '*' category missing from get_cats; got %v", cats)
		}
	})

	// --- get_config -----------------------------------------------------
	t.Run("get_config", func(t *testing.T) {
		out := sabGetJSON(t, base, url.Values{"mode": {"get_config"}, "apikey": {apiKey}})
		conf, ok := out["config"].(map[string]any)
		if !ok {
			t.Fatalf("config not an object: %T", out["config"])
		}
		misc, ok := conf["misc"].(map[string]any)
		if !ok {
			t.Fatalf("misc missing: %v", conf)
		}
		if cd, _ := misc["complete_dir"].(string); cd == "" {
			t.Errorf("misc.complete_dir empty")
		}
	})

	// --- queue list (empty) ---------------------------------------------
	t.Run("queue_empty", func(t *testing.T) {
		out := sabGetJSON(t, base, url.Values{"mode": {"queue"}, "apikey": {apiKey}})
		q, ok := out["queue"].(map[string]any)
		if !ok {
			t.Fatalf("queue not an object: %T", out["queue"])
		}
		if n, _ := q["noofslots"].(float64); n != 0 {
			t.Errorf("empty queue noofslots = %v; want 0", n)
		}
		if status, _ := q["status"].(string); status != "Idle" {
			t.Errorf("empty queue status = %q; want Idle", status)
		}
	})

	// --- addfile + queue + delete ---------------------------------------
	t.Run("addfile_then_queue_then_delete", func(t *testing.T) {
		// Use a small valid NZB. The existing buildNZB helper produces
		// a minimal valid structure; we don't actually fetch articles
		// (no NNTP pool configured) so the Job sits in queued state.
		nzbXML := buildNZB("sab-test", []e2eSegment{
			{MessageID: "sab-test-seg1@local", Begin: 1, End: 10,
				Encoded: []byte("=ybegin part=1 line=128 size=10 name=x.bin\r\n" +
					"abcdefghij\r\n=yend size=10 part=1 crc32=0\r\n")},
		})

		body, ct := buildSABAddFile(t, "sab.nzb", nzbXML)
		resp := sabPost(t, base+"?mode=addfile&apikey="+apiKey+"&cat=*", ct, body)
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			respBytes, _ := io.ReadAll(resp.Body)
			t.Fatalf("addfile status %d: %s", resp.StatusCode, respBytes)
		}
		var addOut struct {
			Status bool     `json:"status"`
			NZOIDs []string `json:"nzo_ids"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&addOut); err != nil {
			t.Fatalf("addfile decode: %v", err)
		}
		if !addOut.Status || len(addOut.NZOIDs) != 1 {
			t.Fatalf("addfile result = %+v", addOut)
		}
		nzoID := addOut.NZOIDs[0]
		if !strings.HasPrefix(nzoID, "SABnzbd_nzo_") {
			t.Errorf("nzo_id = %q; want SABnzbd_nzo_ prefix", nzoID)
		}

		// Queue should now have one slot.
		out := sabGetJSON(t, base, url.Values{"mode": {"queue"}, "apikey": {apiKey}})
		q := out["queue"].(map[string]any)
		slots, _ := q["slots"].([]any)
		if len(slots) != 1 {
			t.Fatalf("expected 1 slot; got %d", len(slots))
		}
		slot := slots[0].(map[string]any)
		if slot["nzo_id"] != nzoID {
			t.Errorf("slot nzo_id = %v; want %s", slot["nzo_id"], nzoID)
		}
		if slot["filename"] != "sab-test" {
			t.Errorf("slot filename = %v; want sab-test", slot["filename"])
		}

		// Pause via queue.name=pause.
		resp = sabPost(t, base, "application/x-www-form-urlencoded",
			strings.NewReader(url.Values{
				"mode": {"queue"}, "name": {"pause"},
				"value": {nzoID}, "apikey": {apiKey},
			}.Encode()))
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Errorf("pause status %d", resp.StatusCode)
		}

		// Delete via queue.name=delete.
		resp = sabPost(t, base, "application/x-www-form-urlencoded",
			strings.NewReader(url.Values{
				"mode": {"queue"}, "name": {"delete"},
				"value": {nzoID}, "apikey": {apiKey},
			}.Encode()))
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Errorf("delete status %d", resp.StatusCode)
		}

		// Queue should be empty again.
		out = sabGetJSON(t, base, url.Values{"mode": {"queue"}, "apikey": {apiKey}})
		q = out["queue"].(map[string]any)
		if n, _ := q["noofslots"].(float64); n != 0 {
			t.Errorf("after delete noofslots = %v; want 0", n)
		}
	})

	// --- history (empty) ------------------------------------------------
	t.Run("history_empty", func(t *testing.T) {
		out := sabGetJSON(t, base, url.Values{"mode": {"history"}, "apikey": {apiKey}})
		h, ok := out["history"].(map[string]any)
		if !ok {
			t.Fatalf("history not an object: %T", out["history"])
		}
		if n, _ := h["noofslots"].(float64); n != 0 {
			t.Errorf("empty history noofslots = %v; want 0", n)
		}
	})

	// --- unknown mode → 400 ---------------------------------------------
	t.Run("unknown_mode", func(t *testing.T) {
		resp := sabGet(t, base, url.Values{"mode": {"reorder"}, "apikey": {apiKey}})
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("unknown mode → %d; want 400", resp.StatusCode)
		}
	})
}

// --- helpers ---------------------------------------------------------

func sabGet(t *testing.T, base string, params url.Values) *http.Response {
	t.Helper()
	req, _ := http.NewRequest(http.MethodGet, base+"?"+params.Encode(), nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET sab: %v", err)
	}
	return resp
}

func sabGetJSON(t *testing.T, base string, params url.Values) map[string]any {
	t.Helper()
	resp := sabGet(t, base, params)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("GET sab %v: %d %s", params, resp.StatusCode, body)
	}
	var out map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode sab: %v", err)
	}
	return out
}

func sabPost(t *testing.T, url, contentType string, body io.Reader) *http.Response {
	t.Helper()
	req, _ := http.NewRequest(http.MethodPost, url, body)
	req.Header.Set("Content-Type", contentType)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST sab: %v", err)
	}
	return resp
}

// buildSABAddFile builds a multipart body suitable for addfile.
// Real SAB accepts the part named "name" or "nzbfile"; the *arr suite
// uses "name". We test the canonical path here.
func buildSABAddFile(t *testing.T, filename, xml string) (io.Reader, string) {
	t.Helper()
	body := &bytes.Buffer{}
	w := multipart.NewWriter(body)
	part, err := w.CreateFormFile("name", filename)
	if err != nil {
		t.Fatalf("CreateFormFile: %v", err)
	}
	if _, err := io.Copy(part, strings.NewReader(xml)); err != nil {
		t.Fatalf("copy: %v", err)
	}
	_ = w.Close()
	return body, w.FormDataContentType()
}
