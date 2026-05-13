// Package config defines hoardarr's configuration schema, loading rules,
// and validation.
//
// Configuration is hierarchical TOML. The runtime layers values like so
// (highest precedence first):
//
//  1. Environment variables (HOARDARR_*)
//  2. The user's TOML file (default: ./config.toml)
//  3. Built-in defaults (this file's Default function)
//
// On first run with no config file, hoardarr writes a fresh config.toml
// with built-in defaults and a freshly-generated API key so the operator
// has something usable immediately.
package config

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/BurntSushi/toml"
)

// Config is the full configuration tree.
type Config struct {
	Server    Server    `toml:"server"`
	Auth      Auth      `toml:"auth"`
	Storage   Storage   `toml:"storage"`
	Paths     Paths     `toml:"paths"`
	Bandwidth Bandwidth `toml:"bandwidth"`
}

// Server holds HTTP listener configuration.
type Server struct {
	// Listen is the address (host:port) the HTTP server binds to.
	Listen string `toml:"listen"`

	// DataDir is the root directory for persistent state: SQLite DB,
	// incomplete/complete directories, logs. Resolved to an absolute
	// path during validation.
	DataDir string `toml:"data_dir"`

	// LogLevel selects the slog level: "debug" | "info" | "warn" | "error".
	// Empty resolves to "info" during validation.
	LogLevel string `toml:"log_level"`

	// URLBase is the path prefix this hoardarr instance is mounted at
	// behind a reverse proxy. Must start with "/" or be empty; must
	// not end with "/". Examples: "", "/hoardarr", "/apps/hoardarr".
	//
	// When set, the backend strips this prefix from incoming requests
	// before routing, and the frontend bootstrap injects a <base href>
	// + window.HOARDARR_BASE so client-side routes and assets resolve
	// correctly. One binary, picks up its mount path from env/config.
	URLBase string `toml:"url_base"`

	// MaxConcurrentJobs caps the number of jobs the orchestrator
	// drives in parallel. 0 means unlimited (default — every job
	// runs in its own goroutine as soon as it lands). Set to 1 for
	// strict serial behaviour (SABnzbd default); set to N>1 to
	// allow N jobs at once. Excess jobs sit in pending state until
	// a slot frees up. Live-editable from Settings → General.
	MaxConcurrentJobs int `toml:"max_concurrent_jobs"`

	// FailHopelessRatio aborts a download mid-flight when failed
	// bytes exceed this fraction of total bytes (SABnzbd's
	// fail_hopeless). 0 disables; 0.05 = 5% threshold. Live-
	// editable from Settings → General. Reasonable defaults sit
	// in the 5–10% range for typical PAR2-protected releases.
	FailHopelessRatio float64 `toml:"fail_hopeless_ratio"`

	// DeferRecoveryVols defers PAR2 per-slice recovery files
	// (`<base>.vol###+##.par2`) at job-add time. The data + the
	// small index .par2 download immediately; vol files come down
	// on demand when repair determines it needs them. Default
	// false (legacy behaviour: fetch every file eagerly). When
	// enabled, the SAB-style bandwidth savings apply only to
	// PAR2-protected releases that don't need repair.
	DeferRecoveryVols bool `toml:"defer_recovery_vols"`

	// DeleteSamples removes files matching the SAB sample/proof
	// regex from the target dir after a successful move. Default
	// true (matches SABnzbd's out-of-the-box behaviour). Disable
	// via Settings if you want raw releases preserved.
	DeleteSamples bool `toml:"delete_samples"`

	// CollapseSingleFolder lifts the contents of a single redundant
	// inner directory up one level when a release lands wrapped
	// (e.g. complete/cat/release/release-inner/file.mkv → complete/
	// cat/release/file.mkv). Default true.
	CollapseSingleFolder bool `toml:"collapse_single_folder"`
}

// Auth holds authentication configuration. Currently API-key only;
// web-session auth lands in M6.
type Auth struct {
	// APIKey is the shared secret required on every protected endpoint.
	// Generated automatically on first run if absent. Format: 32 hex
	// characters (16 bytes of crypto/rand entropy), matching SABnzbd /
	// Sonarr conventions.
	APIKey string `toml:"api_key"`
}

// Storage selects and configures the persistence backend. SQLite is the
// only backend implemented in v0.1; Postgres will land later as a peer
// adapter without schema changes here.
type Storage struct {
	// Backend selects the persistence adapter. Valid: "sqlite".
	// Future: "postgres", "boltdb", etc.
	Backend string `toml:"backend"`

	SQLite SQLite `toml:"sqlite"`
}

// SQLite is the configuration for the SQLite persistence adapter.
type SQLite struct {
	// Path is the on-disk DB file. If empty, resolves to
	// <data_dir>/hoardarr.db at validation time.
	Path string `toml:"path"`
}

// Bandwidth caps download throughput. Both knobs are bytes/sec; 0 means
// no cap. Per-server caps in servers.bandwidth_bytes_per_sec are applied
// in addition: effective rate = min(global, per-server) when both set.
type Bandwidth struct {
	GlobalBytesPerSec int64 `toml:"global_bytes_per_sec"`
}

// Paths configures where incomplete and completed downloads land.
type Paths struct {
	// IncompleteDir holds in-progress downloads. Files are pre-allocated
	// and segment-written here. Empty resolves to <data_dir>/incomplete.
	IncompleteDir string `toml:"incomplete_dir"`

	// CompleteDir is the parent of category-named subdirectories where
	// post-processed downloads are delivered. Empty resolves to
	// <data_dir>/complete.
	CompleteDir string `toml:"complete_dir"`
}

// Default returns a Config populated with built-in defaults.
//
// The API key is left empty; LoadOrCreate generates one when persisting
// a fresh config to disk. Tests that need a fixed key set it explicitly.
func Default() Config {
	return Config{
		Server: Server{
			Listen:   ":8085",
			DataDir:  "./data",
			LogLevel: "info",
			// 1 = strict serial (SABnzbd-style). The previous default
			// of 0 (unlimited) let every queued NZB run in parallel,
			// which contends for the shared NNTP pool and produces
			// jittery per-job speeds. Operators who actually want
			// parallel downloads can raise this from Settings.
			MaxConcurrentJobs:    1,
			DeleteSamples:        true,
			CollapseSingleFolder: true,
		},
		Auth: Auth{
			APIKey: "",
		},
		Storage: Storage{
			Backend: "sqlite",
			SQLite:  SQLite{Path: ""},
		},
		Paths: Paths{
			IncompleteDir: "",
			CompleteDir:   "",
		},
	}
}

// LoadOrCreate reads the config from path. If the file does not exist,
// it writes a fresh default config (with a generated API key) to path
// and returns that. Subsequent runs read the same file.
//
// After loading, environment-variable overrides are applied and the
// result is validated and path-resolved.
func LoadOrCreate(path string) (Config, error) {
	cfg := Default()

	switch _, err := os.Stat(path); {
	case err == nil:
		if _, err := toml.DecodeFile(path, &cfg); err != nil {
			return Config{}, fmt.Errorf("decode config %q: %w", path, err)
		}
	case errors.Is(err, os.ErrNotExist):
		key, err := generateAPIKey()
		if err != nil {
			return Config{}, fmt.Errorf("generate api key: %w", err)
		}
		cfg.Auth.APIKey = key
		if err := writeConfig(path, cfg); err != nil {
			return Config{}, fmt.Errorf("write fresh config %q: %w", path, err)
		}
	default:
		return Config{}, fmt.Errorf("stat config %q: %w", path, err)
	}

	applyEnvOverrides(&cfg)

	if err := cfg.normalize(); err != nil {
		return Config{}, fmt.Errorf("normalize config: %w", err)
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, fmt.Errorf("validate config: %w", err)
	}
	return cfg, nil
}

// Validate checks invariants and returns the first violation found.
//
// Validate runs after normalize, so paths are absolute and defaults are
// resolved before checking.
func (c *Config) Validate() error {
	if c.Server.Listen == "" {
		return errors.New("server.listen must not be empty")
	}
	if c.Server.DataDir == "" {
		return errors.New("server.data_dir must not be empty")
	}
	if c.Auth.APIKey == "" {
		return errors.New("auth.api_key must not be empty")
	}
	switch c.Storage.Backend {
	case "sqlite":
		if c.Storage.SQLite.Path == "" {
			return errors.New("storage.sqlite.path must not be empty after normalize")
		}
	default:
		return fmt.Errorf("storage.backend %q is not supported (valid: sqlite)", c.Storage.Backend)
	}
	if c.Paths.IncompleteDir == "" {
		return errors.New("paths.incomplete_dir must not be empty after normalize")
	}
	if c.Paths.CompleteDir == "" {
		return errors.New("paths.complete_dir must not be empty after normalize")
	}
	switch c.Server.LogLevel {
	case "debug", "info", "warn", "error":
		// valid
	default:
		return fmt.Errorf("server.log_level %q is not supported (valid: debug, info, warn, error)", c.Server.LogLevel)
	}
	if c.Server.URLBase != "" {
		if !strings.HasPrefix(c.Server.URLBase, "/") {
			return fmt.Errorf("server.url_base %q must start with /", c.Server.URLBase)
		}
		if strings.HasSuffix(c.Server.URLBase, "/") {
			return fmt.Errorf("server.url_base %q must not end with /", c.Server.URLBase)
		}
	}
	if c.Bandwidth.GlobalBytesPerSec < 0 {
		return fmt.Errorf("bandwidth.global_bytes_per_sec %d must be >= 0", c.Bandwidth.GlobalBytesPerSec)
	}
	return nil
}

// normalize resolves relative paths against the data dir, fills in
// derived defaults, and converts paths to absolute form.
//
// It is idempotent: calling it on an already-normalized Config is a
// no-op.
func (c *Config) normalize() error {
	dataDir, err := filepath.Abs(c.Server.DataDir)
	if err != nil {
		return fmt.Errorf("resolve data_dir: %w", err)
	}
	c.Server.DataDir = dataDir

	if c.Storage.SQLite.Path == "" {
		c.Storage.SQLite.Path = filepath.Join(dataDir, "hoardarr.db")
	} else if !filepath.IsAbs(c.Storage.SQLite.Path) {
		c.Storage.SQLite.Path = filepath.Join(dataDir, c.Storage.SQLite.Path)
	}

	if c.Paths.IncompleteDir == "" {
		c.Paths.IncompleteDir = filepath.Join(dataDir, "incomplete")
	} else if !filepath.IsAbs(c.Paths.IncompleteDir) {
		c.Paths.IncompleteDir = filepath.Join(dataDir, c.Paths.IncompleteDir)
	}

	if c.Paths.CompleteDir == "" {
		c.Paths.CompleteDir = filepath.Join(dataDir, "complete")
	} else if !filepath.IsAbs(c.Paths.CompleteDir) {
		c.Paths.CompleteDir = filepath.Join(dataDir, c.Paths.CompleteDir)
	}

	if c.Server.LogLevel == "" {
		c.Server.LogLevel = "info"
	}
	return nil
}

// applyEnvOverrides folds HOARDARR_* environment variables into cfg.
//
// Env vars take precedence over file values. The full list is documented
// in the operator-facing README; the canonical source is this function.
func applyEnvOverrides(cfg *Config) {
	if v, ok := os.LookupEnv("HOARDARR_LISTEN"); ok && v != "" {
		cfg.Server.Listen = v
	}
	if v, ok := os.LookupEnv("HOARDARR_DATA_DIR"); ok && v != "" {
		cfg.Server.DataDir = v
	}
	if v, ok := os.LookupEnv("HOARDARR_API_KEY"); ok && v != "" {
		cfg.Auth.APIKey = v
	}
	if v, ok := os.LookupEnv("HOARDARR_STORAGE_BACKEND"); ok && v != "" {
		cfg.Storage.Backend = strings.ToLower(v)
	}
	if v, ok := os.LookupEnv("HOARDARR_SQLITE_PATH"); ok && v != "" {
		cfg.Storage.SQLite.Path = v
	}
	if v, ok := os.LookupEnv("HOARDARR_INCOMPLETE_DIR"); ok && v != "" {
		cfg.Paths.IncompleteDir = v
	}
	if v, ok := os.LookupEnv("HOARDARR_COMPLETE_DIR"); ok && v != "" {
		cfg.Paths.CompleteDir = v
	}
	if v, ok := os.LookupEnv("HOARDARR_LOG_LEVEL"); ok && v != "" {
		cfg.Server.LogLevel = strings.ToLower(v)
	}
	if v, ok := os.LookupEnv("HOARDARR_URL_BASE"); ok {
		// Allow setting to empty via env to override a config-file
		// value, hence the unconditional assignment.
		cfg.Server.URLBase = strings.TrimRight(v, "/")
	}
	if v, ok := os.LookupEnv("HOARDARR_BANDWIDTH_GLOBAL"); ok && v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n >= 0 {
			cfg.Bandwidth.GlobalBytesPerSec = n
		}
	}
}

// generateAPIKey returns a 32-character hex-encoded random string,
// matching SABnzbd / Sonarr / Radarr conventions (16 bytes of entropy).
func generateAPIKey() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}

// Save serialises cfg to path. The directory is created if it does
// not exist. The file is written 0600 since it contains the API key.
//
// Used by runtime settings mutations (e.g. URL_BASE edits from the
// UI) to persist a fresh state to disk after updating in-memory.
func Save(path string, cfg Config) error {
	return writeConfig(path, cfg)
}

func writeConfig(path string, cfg Config) error {
	if dir := filepath.Dir(path); dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := f.WriteString(configHeader); err != nil {
		return err
	}
	enc := toml.NewEncoder(f)
	return enc.Encode(cfg)
}

const configHeader = `# hoardarr configuration.
#
# This file was created automatically on first run. Edit freely; values
# here override the built-in defaults. Environment variables (HOARDARR_*)
# override values in this file.
#
# The api_key was randomly generated. Treat it as a secret. Configure
# Sonarr / Radarr / Lidarr / Readarr / Prowlarr (or SABnzbd-compatible
# clients) with this key under the "API Key" field.

`
