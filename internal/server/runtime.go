package server

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log/slog"
	"strings"
	"sync"

	"github.com/jaenster/hoardarr/internal/config"
)

// SettingsStore is the slice of adapter/sqlite.SettingsRepo the Runtime
// uses for live persistence. Defined here as an interface so the server
// package doesn't import sqlite directly (which would couple the HTTP
// layer to the persistence adapter).
type SettingsStore interface {
	GetStringOr(ctx context.Context, key, dflt string) (string, error)
	GetIntOr(ctx context.Context, key string, dflt int) (int, error)
	GetFloatOr(ctx context.Context, key string, dflt float64) (float64, error)
	GetBoolOr(ctx context.Context, key string, dflt bool) (bool, error)
	Set(ctx context.Context, key, value string) error
	SetInt(ctx context.Context, key string, v int) error
	SetFloat(ctx context.Context, key string, v float64) error
	SetBool(ctx context.Context, key string, v bool) error
}

// Setting keys. These are the runtime-mutable fields that used to
// live in config.toml; they now persist in the SQLite settings table.
const (
	SettingURLBase              = "server.url_base"
	SettingMaxConcurrentJobs    = "server.max_concurrent_jobs"
	SettingFailHopelessRatio    = "server.fail_hopeless_ratio"
	SettingDeferRecoveryVols    = "server.defer_recovery_vols"
	SettingBandwidthGlobalBPS   = "bandwidth.global_bytes_per_sec"
	SettingDeleteSamples        = "deliver.delete_samples"
	SettingCollapseSingleFolder = "deliver.collapse_single_folder"
	SettingAPIKey               = "auth.api_key"
)

// Runtime holds runtime-mutable config that the UI can edit at any
// time and that the server consults on every request.
//
// Persistence lives in the SQLite `settings` table (one row per key),
// loaded once at construction and written through on every Set*. The
// in-memory mirror under rt.mu is the hot-path read; the DB write only
// happens on operator-driven changes (rare).
type Runtime struct {
	mu                   sync.RWMutex
	store                SettingsStore
	urlBase              string
	maxConcurrentJobs    int
	failHopelessRatio    float64
	deferRecoveryVols    bool
	bandwidthGlobalBPS   int64
	deleteSamples        bool
	collapseSingleFolder bool
	apiKey               string
	listeners            []func(maxConcurrent int)
	bandwidthListeners   []func(bytesPerSec int64)
}

// NewRuntime constructs a Runtime backed by store. Initial values are
// read from store; missing keys take their value from cfg (one-time
// migration from config.toml on first run) and are written back so
// the DB becomes authoritative. Returns the populated Runtime and a
// non-nil error only if the initial read fails.
//
// Pass a nil store to use an ephemeral in-memory store — useful in
// tests that don't open the DB. The cfg defaults still apply.
func NewRuntime(ctx context.Context, store SettingsStore, cfg config.Config, logger *slog.Logger) (*Runtime, error) {
	if logger == nil {
		logger = slog.Default()
	}
	if store == nil {
		store = NewMemoryStore()
	}
	rt := &Runtime{store: store}

	urlBase, err := store.GetStringOr(ctx, SettingURLBase, cfg.Server.URLBase)
	if err != nil {
		return nil, err
	}
	maxConc, err := store.GetIntOr(ctx, SettingMaxConcurrentJobs, cfg.Server.MaxConcurrentJobs)
	if err != nil {
		return nil, err
	}
	failHop, err := store.GetFloatOr(ctx, SettingFailHopelessRatio, cfg.Server.FailHopelessRatio)
	if err != nil {
		return nil, err
	}
	deferVols, err := store.GetBoolOr(ctx, SettingDeferRecoveryVols, cfg.Server.DeferRecoveryVols)
	if err != nil {
		return nil, err
	}
	bwGlobal, err := store.GetIntOr(ctx, SettingBandwidthGlobalBPS, int(cfg.Bandwidth.GlobalBytesPerSec))
	if err != nil {
		return nil, err
	}
	delSamples, err := store.GetBoolOr(ctx, SettingDeleteSamples, cfg.Server.DeleteSamples)
	if err != nil {
		return nil, err
	}
	collapse, err := store.GetBoolOr(ctx, SettingCollapseSingleFolder, cfg.Server.CollapseSingleFolder)
	if err != nil {
		return nil, err
	}
	apiKey, err := store.GetStringOr(ctx, SettingAPIKey, cfg.Auth.APIKey)
	if err != nil {
		return nil, err
	}

	// Idempotent backfill: writing what we just read is a no-op for
	// existing rows and seeds the row for missing keys. Cheap on every
	// startup; fully decouples future runtime from config.toml.
	if err := store.Set(ctx, SettingURLBase, urlBase); err != nil {
		logger.Warn("runtime: seed url_base", "err", err)
	}
	if err := store.SetInt(ctx, SettingMaxConcurrentJobs, maxConc); err != nil {
		logger.Warn("runtime: seed max_concurrent_jobs", "err", err)
	}
	if err := store.SetFloat(ctx, SettingFailHopelessRatio, failHop); err != nil {
		logger.Warn("runtime: seed fail_hopeless_ratio", "err", err)
	}
	if err := store.SetBool(ctx, SettingDeferRecoveryVols, deferVols); err != nil {
		logger.Warn("runtime: seed defer_recovery_vols", "err", err)
	}
	if err := store.SetInt(ctx, SettingBandwidthGlobalBPS, bwGlobal); err != nil {
		logger.Warn("runtime: seed bandwidth_global_bps", "err", err)
	}
	if err := store.SetBool(ctx, SettingDeleteSamples, delSamples); err != nil {
		logger.Warn("runtime: seed delete_samples", "err", err)
	}
	if err := store.SetBool(ctx, SettingCollapseSingleFolder, collapse); err != nil {
		logger.Warn("runtime: seed collapse_single_folder", "err", err)
	}
	if err := store.Set(ctx, SettingAPIKey, apiKey); err != nil {
		logger.Warn("runtime: seed api_key", "err", err)
	}

	rt.urlBase = urlBase
	rt.maxConcurrentJobs = maxConc
	rt.failHopelessRatio = failHop
	rt.deferRecoveryVols = deferVols
	rt.bandwidthGlobalBPS = int64(bwGlobal)
	rt.deleteSamples = delSamples
	rt.collapseSingleFolder = collapse
	rt.apiKey = apiKey
	return rt, nil
}

// BandwidthGlobalCap returns the persisted global download cap in
// bytes/second. 0 = uncapped.
func (rt *Runtime) BandwidthGlobalCap() int64 {
	rt.mu.RLock()
	defer rt.mu.RUnlock()
	return rt.bandwidthGlobalBPS
}

// SetBandwidthGlobalCap persists the new global cap and fans the value
// out to registered listeners (the download limiter). Negative values
// are clamped to 0 (uncapped).
func (rt *Runtime) SetBandwidthGlobalCap(v int64) (int64, error) {
	if v < 0 {
		v = 0
	}
	if err := rt.store.SetInt(context.Background(), SettingBandwidthGlobalBPS, int(v)); err != nil {
		return 0, fmt.Errorf("persist bandwidth_global_bps: %w", err)
	}
	rt.mu.Lock()
	rt.bandwidthGlobalBPS = v
	listeners := append([]func(int64){}, rt.bandwidthListeners...)
	rt.mu.Unlock()
	for _, l := range listeners {
		l(v)
	}
	return v, nil
}

// OnBandwidthGlobalChange registers fn for invocation whenever the
// global bandwidth cap changes. The download.Limiter subscribes so
// changes take effect immediately on the next dispatch.
func (rt *Runtime) OnBandwidthGlobalChange(fn func(bytesPerSec int64)) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	rt.bandwidthListeners = append(rt.bandwidthListeners, fn)
}

// FailHopelessRatio returns the SAB fail_hopeless threshold (0-1).
// 0 disables the check.
func (rt *Runtime) FailHopelessRatio() float64 {
	rt.mu.RLock()
	defer rt.mu.RUnlock()
	return rt.failHopelessRatio
}

// SetFailHopelessRatio validates v ∈ [0,1), persists it, returns the
// stored value. The orchestrator reads via the callback on every
// dispatch so changes take effect on the next job.
func (rt *Runtime) SetFailHopelessRatio(v float64) (float64, error) {
	if v < 0 || v >= 1 {
		return 0, fmt.Errorf("fail_hopeless_ratio %v must be in [0, 1)", v)
	}
	if err := rt.store.SetFloat(context.Background(), SettingFailHopelessRatio, v); err != nil {
		return 0, fmt.Errorf("persist fail_hopeless_ratio: %w", err)
	}
	rt.mu.Lock()
	rt.failHopelessRatio = v
	rt.mu.Unlock()
	return v, nil
}

// DeferRecoveryVols reports whether new jobs should hide PAR2
// per-slice recovery files from initial download (SAB-style).
func (rt *Runtime) DeferRecoveryVols() bool {
	rt.mu.RLock()
	defer rt.mu.RUnlock()
	return rt.deferRecoveryVols
}

// SetDeferRecoveryVols persists v and returns the stored value. Takes
// effect on newly-added jobs; in-flight jobs keep their existing
// fetch_recovery_vols flag.
func (rt *Runtime) SetDeferRecoveryVols(v bool) (bool, error) {
	if err := rt.store.SetBool(context.Background(), SettingDeferRecoveryVols, v); err != nil {
		return false, fmt.Errorf("persist defer_recovery_vols: %w", err)
	}
	rt.mu.Lock()
	rt.deferRecoveryVols = v
	rt.mu.Unlock()
	return v, nil
}

// DeleteSamples reports whether deliver should remove sample/proof
// files after a successful move.
func (rt *Runtime) DeleteSamples() bool {
	rt.mu.RLock()
	defer rt.mu.RUnlock()
	return rt.deleteSamples
}

// SetDeleteSamples persists v and returns the stored value. Takes
// effect on the next delivery; in-flight deliveries are not retroactively
// rescanned.
func (rt *Runtime) SetDeleteSamples(v bool) (bool, error) {
	if err := rt.store.SetBool(context.Background(), SettingDeleteSamples, v); err != nil {
		return false, fmt.Errorf("persist delete_samples: %w", err)
	}
	rt.mu.Lock()
	rt.deleteSamples = v
	rt.mu.Unlock()
	return v, nil
}

// APIKey returns the currently-active API key. Read by the auth
// middleware on every request so a rotation takes effect immediately
// for the next inbound call.
func (rt *Runtime) APIKey() string {
	rt.mu.RLock()
	defer rt.mu.RUnlock()
	return rt.apiKey
}

// RotateAPIKey generates a fresh 32-byte hex key, persists it, and
// returns it. Callers MUST display the returned key once and let the
// operator copy it into their *arr clients before navigating away —
// the old key stops working as soon as this returns.
func (rt *Runtime) RotateAPIKey() (string, error) {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("generate api key: %w", err)
	}
	key := hex.EncodeToString(buf)
	if err := rt.store.Set(context.Background(), SettingAPIKey, key); err != nil {
		return "", fmt.Errorf("persist api_key: %w", err)
	}
	rt.mu.Lock()
	rt.apiKey = key
	rt.mu.Unlock()
	return key, nil
}

// CollapseSingleFolder reports whether deliver should flatten a release
// that landed inside a single redundant inner directory.
func (rt *Runtime) CollapseSingleFolder() bool {
	rt.mu.RLock()
	defer rt.mu.RUnlock()
	return rt.collapseSingleFolder
}

// SetCollapseSingleFolder persists v and returns the stored value.
// Takes effect on the next delivery.
func (rt *Runtime) SetCollapseSingleFolder(v bool) (bool, error) {
	if err := rt.store.SetBool(context.Background(), SettingCollapseSingleFolder, v); err != nil {
		return false, fmt.Errorf("persist collapse_single_folder: %w", err)
	}
	rt.mu.Lock()
	rt.collapseSingleFolder = v
	rt.mu.Unlock()
	return v, nil
}

// URLBase returns the current runtime URL base. Always safe to call
// from any goroutine.
func (rt *Runtime) URLBase() string {
	rt.mu.RLock()
	defer rt.mu.RUnlock()
	return rt.urlBase
}

// MaxConcurrentJobs returns the current cap. 0 = unlimited.
func (rt *Runtime) MaxConcurrentJobs() int {
	rt.mu.RLock()
	defer rt.mu.RUnlock()
	return rt.maxConcurrentJobs
}

// SetMaxConcurrentJobs validates v, persists it, and notifies listeners
// (the orchestrator) so a higher cap immediately drains the pending
// backlog. Negative values are clamped to 0 (unlimited).
func (rt *Runtime) SetMaxConcurrentJobs(v int) (int, error) {
	if v < 0 {
		v = 0
	}
	if v > 1024 {
		return 0, fmt.Errorf("max_concurrent_jobs %d unreasonably high", v)
	}
	if err := rt.store.SetInt(context.Background(), SettingMaxConcurrentJobs, v); err != nil {
		return 0, fmt.Errorf("persist max_concurrent_jobs: %w", err)
	}
	rt.mu.Lock()
	rt.maxConcurrentJobs = v
	listeners := append([]func(int){}, rt.listeners...)
	rt.mu.Unlock()

	// Notify outside the lock to avoid deadlocks if a listener calls
	// back into Runtime.
	for _, l := range listeners {
		l(v)
	}
	return v, nil
}

// OnMaxConcurrentJobsChange registers fn for invocation whenever the
// cap changes. Used by the orchestrator to drain the pending backlog
// when the operator raises the limit.
func (rt *Runtime) OnMaxConcurrentJobsChange(fn func(maxConcurrent int)) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	rt.listeners = append(rt.listeners, fn)
}

// SetURLBase validates v, persists it to the settings store, and
// publishes the new value to readers. Returns the normalised value
// (trailing slash stripped) on success.
//
// The frontend asset cache and session-cookie Path read URLBase on
// every request, so the change takes effect on the next request
// without a restart.
func (rt *Runtime) SetURLBase(v string) (string, error) {
	v = strings.TrimRight(v, "/")
	if v != "" {
		if !strings.HasPrefix(v, "/") {
			return "", fmt.Errorf("url_base %q must start with /", v)
		}
		if strings.Contains(v, "//") {
			return "", fmt.Errorf("url_base %q must not contain consecutive slashes", v)
		}
	}
	if err := rt.store.Set(context.Background(), SettingURLBase, v); err != nil {
		return "", fmt.Errorf("persist url_base: %w", err)
	}
	rt.mu.Lock()
	rt.urlBase = v
	rt.mu.Unlock()
	return v, nil
}
