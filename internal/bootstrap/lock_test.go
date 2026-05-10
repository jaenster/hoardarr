package bootstrap_test

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jaenster/hoardarr/internal/bootstrap"
	"github.com/jaenster/hoardarr/internal/config"
)

// TestDataDirLock_RejectsConcurrentInstance ensures that two builds
// against the same data_dir don't both succeed. This protects users
// who accidentally start a second daemon (e.g. a stray docker-compose
// service) — they get a clear error instead of two processes
// scribbling on the same SQLite file.
func TestDataDirLock_RejectsConcurrentInstance(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.toml")

	t.Setenv("HOARDARR_LISTEN", "127.0.0.1:0")
	t.Setenv("HOARDARR_DATA_DIR", filepath.Join(dir, "data"))
	cfg, err := config.LoadOrCreate(cfgPath)
	if err != nil {
		t.Fatalf("LoadOrCreate: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	app1, err := bootstrap.Build(ctx, cfg, nil, nil)
	if err != nil {
		t.Fatalf("first Build: %v", err)
	}
	defer func() { _ = app1.Shutdown() }()

	// Second instance against the same data_dir must fail.
	_, err = bootstrap.Build(ctx, cfg, nil, nil)
	if err == nil {
		t.Fatal("second Build succeeded; expected lock contention error")
	}
	if !strings.Contains(err.Error(), "another hoardarr instance") {
		t.Errorf("err = %v; want clear 'another instance' message", err)
	}
}
