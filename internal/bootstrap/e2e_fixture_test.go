package bootstrap_test

// End-to-end against a realistic Usenet release: multiple data files,
// a real PAR2 recovery set, and ~10% of articles intentionally
// "missing" on the fake NNTP. Hoardarr should download what's
// available, detect the gaps via PAR2 verify, repair, and deliver
// byte-equivalent files to the complete dir.
//
// This is the big test — it exercises orchestrator + multi-file
// pipelining + yEnc + PAR2 parser + GF(2^16) repair + deliver,
// against a real wire-format Usenet server. The numbers are tuned
// so the test runs in seconds: small files keep RS encoding fast,
// generous recovery slices guarantee repair succeeds.

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jaenster/hoardarr/internal/bootstrap"
	"github.com/jaenster/hoardarr/internal/config"
	"github.com/jaenster/hoardarr/internal/testserver/fixture"
	testnntp "github.com/jaenster/hoardarr/internal/testserver/nntp"
)

func TestE2E_RealisticReleaseWithRepair(t *testing.T) {
	// Generate the release: 3 data files × 128 KiB = 384 KiB,
	// sliced at 32 KiB → 12 slices, with 4 recovery slices (~33%
	// redundancy). That comfortably covers losing ~3 slices.
	fx, err := fixture.Generate(fixture.Options{
		Name:           "fixture-release",
		FileCount:      3,
		FileSize:       128 * 1024,
		ArticleSize:    32 * 1024,
		PAR2SliceSize:  32 * 1024,
		RecoverySlices: 4,
	})
	if err != nil {
		t.Fatalf("fixture.Generate: %v", err)
	}
	if len(fx.Articles) == 0 {
		t.Fatal("no articles generated")
	}
	t.Logf("fixture: %d files (%d data + %d recovery), %d articles, NZB %d bytes",
		fx.FileCount+fx.RecoveryFileCount+1, fx.FileCount, fx.RecoveryFileCount,
		len(fx.Articles), len(fx.NZB))

	// Fake NNTP with all articles registered + a deterministic
	// MissingFraction so we drop ~10% of them and exercise PAR2.
	// Seed is pinned so the test is reproducible.
	fake, err := testnntp.Start(testnntp.Options{
		MissingFraction: 0.10,
		Seed:            1,
	})
	if err != nil {
		t.Fatalf("testnntp.Start: %v", err)
	}
	defer fake.Stop()
	for id, body := range fx.Articles {
		fake.AddArticle(id, body)
	}

	// hoardarr — pre-seed a server pointing at the fake.
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.toml")
	t.Setenv("HOARDARR_LISTEN", "127.0.0.1:"+mustFreePort(t))
	t.Setenv("HOARDARR_DATA_DIR", filepath.Join(dir, "data"))
	cfg, err := config.LoadOrCreate(cfgPath)
	if err != nil {
		t.Fatalf("LoadOrCreate: %v", err)
	}
	apiKey := cfg.Auth.APIKey
	preseedTestserver(t, cfg, fake.Host(), fake.Port())

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
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

	// Upload NZB.
	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	fw, _ := mw.CreateFormFile("nzb", "fixture-release.nzb")
	_, _ = fw.Write(fx.NZB)
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
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("upload status=%d body=%s", resp.StatusCode, body)
	}
	var added struct {
		JobID int64 `json:"job_id"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&added)

	// Poll until completed (terminal) or failed.
	deadline := time.Now().Add(45 * time.Second)
	var finalState string
	for time.Now().Before(deadline) {
		state, _ := pollJobState(t, base, apiKey, added.JobID)
		finalState = state
		if state == "completed" || state == "failed" || state == "aborted" {
			break
		}
		time.Sleep(150 * time.Millisecond)
	}
	if finalState != "completed" {
		t.Fatalf("job did not reach completed; final state %q", finalState)
	}

	// Each data file should land in complete/<category>/<release>/
	// byte-equivalent to the original. PAR2 files aren't part of the
	// deliverable surface.
	for name, want := range fx.Files {
		if filepath.Ext(name) == ".par2" {
			continue
		}
		delivered := filepath.Join(cfg.Paths.CompleteDir, "*", "fixture-release", name)
		matches, _ := filepath.Glob(delivered)
		if len(matches) == 0 {
			// Fallback: scan for the file anywhere under complete.
			_ = filepath.Walk(cfg.Paths.CompleteDir, func(p string, info os.FileInfo, err error) error {
				if err == nil && !info.IsDir() && filepath.Base(p) == name {
					matches = append(matches, p)
				}
				return nil
			})
		}
		if len(matches) == 0 {
			t.Fatalf("file %q not delivered", name)
		}
		got, err := os.ReadFile(matches[0])
		if err != nil {
			t.Fatalf("read delivered %q: %v", matches[0], err)
		}
		if !bytes.Equal(got, want) {
			t.Errorf("file %q bytes differ from original (got=%d want=%d)",
				name, len(got), len(want))
		}
	}
}
