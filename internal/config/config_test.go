package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDefault(t *testing.T) {
	c := Default()
	if c.Server.Listen != ":8085" {
		t.Errorf("Listen = %q; want :8085", c.Server.Listen)
	}
	if c.Server.DataDir != "./data" {
		t.Errorf("DataDir = %q; want ./data", c.Server.DataDir)
	}
	if c.Storage.Backend != "sqlite" {
		t.Errorf("Backend = %q; want sqlite", c.Storage.Backend)
	}
	if c.Auth.APIKey != "" {
		t.Errorf("APIKey = %q; want empty (filled at LoadOrCreate)", c.Auth.APIKey)
	}
}

func TestLoadOrCreate_FreshFile(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.toml")

	cfg, err := LoadOrCreate(cfgPath)
	if err != nil {
		t.Fatalf("LoadOrCreate: %v", err)
	}

	// File should exist now.
	if _, err := os.Stat(cfgPath); err != nil {
		t.Fatalf("config file not created: %v", err)
	}

	// API key should be a 32-char hex string.
	if got := cfg.Auth.APIKey; len(got) != 32 || !isHex(got) {
		t.Errorf("APIKey = %q; want 32 hex chars", got)
	}

	// Paths should be normalized to absolute.
	if !filepath.IsAbs(cfg.Server.DataDir) {
		t.Errorf("DataDir = %q; want absolute", cfg.Server.DataDir)
	}
	if !filepath.IsAbs(cfg.Storage.SQLite.Path) {
		t.Errorf("SQLite.Path = %q; want absolute", cfg.Storage.SQLite.Path)
	}
	if !filepath.IsAbs(cfg.Paths.IncompleteDir) {
		t.Errorf("IncompleteDir = %q; want absolute", cfg.Paths.IncompleteDir)
	}
	if !filepath.IsAbs(cfg.Paths.CompleteDir) {
		t.Errorf("CompleteDir = %q; want absolute", cfg.Paths.CompleteDir)
	}

	// Re-load: should pick up the persisted key, not regenerate.
	cfg2, err := LoadOrCreate(cfgPath)
	if err != nil {
		t.Fatalf("LoadOrCreate (second): %v", err)
	}
	if cfg2.Auth.APIKey != cfg.Auth.APIKey {
		t.Errorf("APIKey changed across loads: %q vs %q", cfg.Auth.APIKey, cfg2.Auth.APIKey)
	}
}

func TestLoadOrCreate_ExistingFile(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.toml")

	const body = `
[server]
listen = ":1234"
data_dir = "./localdata"

[auth]
api_key = "deadbeefdeadbeefdeadbeefdeadbeef"

[storage]
backend = "sqlite"

[storage.sqlite]
path = "custom.db"

[paths]
incomplete_dir = "tmp_in"
complete_dir = "tmp_out"
`
	if err := os.WriteFile(cfgPath, []byte(body), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := LoadOrCreate(cfgPath)
	if err != nil {
		t.Fatalf("LoadOrCreate: %v", err)
	}

	if cfg.Server.Listen != ":1234" {
		t.Errorf("Listen = %q; want :1234", cfg.Server.Listen)
	}
	if cfg.Auth.APIKey != "deadbeefdeadbeefdeadbeefdeadbeef" {
		t.Errorf("APIKey = %q; want deadbeef...", cfg.Auth.APIKey)
	}
	// Relative paths should be resolved relative to the data_dir.
	wantDB := filepath.Join(cfg.Server.DataDir, "custom.db")
	if cfg.Storage.SQLite.Path != wantDB {
		t.Errorf("SQLite.Path = %q; want %q", cfg.Storage.SQLite.Path, wantDB)
	}
	wantIn := filepath.Join(cfg.Server.DataDir, "tmp_in")
	if cfg.Paths.IncompleteDir != wantIn {
		t.Errorf("IncompleteDir = %q; want %q", cfg.Paths.IncompleteDir, wantIn)
	}
	wantOut := filepath.Join(cfg.Server.DataDir, "tmp_out")
	if cfg.Paths.CompleteDir != wantOut {
		t.Errorf("CompleteDir = %q; want %q", cfg.Paths.CompleteDir, wantOut)
	}
}

func TestEnvOverrides(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.toml")

	// Pre-create a config file so LoadOrCreate just reads it.
	const body = `
[server]
listen = ":8085"
data_dir = "./data"

[auth]
api_key = "filebasedkey00000000000000000000"

[storage]
backend = "sqlite"
`
	if err := os.WriteFile(cfgPath, []byte(body), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	t.Setenv("HOARDARR_LISTEN", ":9999")
	t.Setenv("HOARDARR_API_KEY", "envoverridekey00000000000000000")
	t.Setenv("HOARDARR_DATA_DIR", filepath.Join(dir, "envdata"))

	cfg, err := LoadOrCreate(cfgPath)
	if err != nil {
		t.Fatalf("LoadOrCreate: %v", err)
	}

	if cfg.Server.Listen != ":9999" {
		t.Errorf("Listen = %q; want :9999 (env override)", cfg.Server.Listen)
	}
	if cfg.Auth.APIKey != "envoverridekey00000000000000000" {
		t.Errorf("APIKey = %q; want env override", cfg.Auth.APIKey)
	}
	if !strings.HasSuffix(cfg.Server.DataDir, "envdata") {
		t.Errorf("DataDir = %q; want suffix envdata", cfg.Server.DataDir)
	}
}

func TestValidate_RejectsBadBackend(t *testing.T) {
	c := Default()
	c.Auth.APIKey = "x"
	c.Storage.Backend = "mysql"
	if err := c.normalize(); err != nil {
		t.Fatalf("normalize: %v", err)
	}
	err := c.Validate()
	if err == nil {
		t.Fatal("expected validation error for unsupported backend")
	}
	if !strings.Contains(err.Error(), "mysql") {
		t.Errorf("err = %v; want mention of mysql", err)
	}
}

func TestValidate_RejectsMissingAPIKey(t *testing.T) {
	c := Default()
	if err := c.normalize(); err != nil {
		t.Fatalf("normalize: %v", err)
	}
	if err := c.Validate(); err == nil {
		t.Fatal("expected validation error for missing api_key")
	}
}

func TestNormalize_Idempotent(t *testing.T) {
	c := Default()
	c.Auth.APIKey = "x"
	if err := c.normalize(); err != nil {
		t.Fatalf("normalize 1: %v", err)
	}
	first := c
	if err := c.normalize(); err != nil {
		t.Fatalf("normalize 2: %v", err)
	}
	if c != first {
		t.Errorf("normalize not idempotent:\nfirst:  %+v\nsecond: %+v", first, c)
	}
}

func TestGenerateAPIKey(t *testing.T) {
	a, err := generateAPIKey()
	if err != nil {
		t.Fatalf("generateAPIKey: %v", err)
	}
	b, err := generateAPIKey()
	if err != nil {
		t.Fatalf("generateAPIKey 2: %v", err)
	}
	if a == b {
		t.Errorf("two generated keys are equal: %q", a)
	}
	if len(a) != 32 || !isHex(a) {
		t.Errorf("key %q is not 32 hex chars", a)
	}
}

func isHex(s string) bool {
	for _, r := range s {
		if !((r >= '0' && r <= '9') || (r >= 'a' && r <= 'f') || (r >= 'A' && r <= 'F')) {
			return false
		}
	}
	return true
}
