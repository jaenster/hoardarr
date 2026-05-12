package server

import (
	"fmt"
	"strings"
	"sync"

	"github.com/jaenster/hoardarr/internal/config"
)

// Runtime holds runtime-mutable config that the UI can edit at any
// time and that the server consults on every request.
//
// Currently this is just URLBase, but the shape is set up so future
// runtime-mutable settings (LogLevel, etc.) can land without
// repeating the persistence + invalidation dance.
//
// Mutations: SetURLBase rewrites both the in-memory value and the
// config file on disk in a single critical section. Callers either
// see the old value or the new one — never a half-applied state.
type Runtime struct {
	mu                sync.RWMutex
	configPath        string // empty disables persistence (used in tests)
	urlBase           string
	maxConcurrentJobs int
	failHopelessRatio float64
	// listeners are notified on max-concurrent changes so the
	// orchestrator can drain its pending-jobs backlog when the cap
	// goes up.
	listeners []func(maxConcurrent int)
}

// NewRuntime constructs a Runtime seeded from the given config and
// bound to the config file at configPath. Pass an empty path to
// disable persistence (writes become in-memory only).
func NewRuntime(cfg config.Config, configPath string) *Runtime {
	return &Runtime{
		configPath:        configPath,
		urlBase:           cfg.Server.URLBase,
		maxConcurrentJobs: cfg.Server.MaxConcurrentJobs,
		failHopelessRatio: cfg.Server.FailHopelessRatio,
	}
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
	rt.mu.Lock()
	if rt.configPath != "" {
		cfg, err := config.LoadOrCreate(rt.configPath)
		if err != nil {
			rt.mu.Unlock()
			return 0, fmt.Errorf("load config: %w", err)
		}
		cfg.Server.FailHopelessRatio = v
		if err := config.Save(rt.configPath, cfg); err != nil {
			rt.mu.Unlock()
			return 0, fmt.Errorf("save config: %w", err)
		}
	}
	rt.failHopelessRatio = v
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

	rt.mu.Lock()
	if rt.configPath != "" {
		cfg, err := config.LoadOrCreate(rt.configPath)
		if err != nil {
			rt.mu.Unlock()
			return 0, fmt.Errorf("load config: %w", err)
		}
		cfg.Server.MaxConcurrentJobs = v
		if err := config.Save(rt.configPath, cfg); err != nil {
			rt.mu.Unlock()
			return 0, fmt.Errorf("save config: %w", err)
		}
	}
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

// SetURLBase validates v, persists it to the config file, and
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
		// Reject obviously broken values; further validation
		// happens via config.Validate when reloading.
		if strings.Contains(v, "//") {
			return "", fmt.Errorf("url_base %q must not contain consecutive slashes", v)
		}
	}

	rt.mu.Lock()
	defer rt.mu.Unlock()

	if rt.configPath != "" {
		cfg, err := config.LoadOrCreate(rt.configPath)
		if err != nil {
			return "", fmt.Errorf("load config: %w", err)
		}
		cfg.Server.URLBase = v
		if err := config.Save(rt.configPath, cfg); err != nil {
			return "", fmt.Errorf("save config: %w", err)
		}
	}
	rt.urlBase = v
	return v, nil
}
