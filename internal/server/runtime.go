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
	mu         sync.RWMutex
	configPath string // empty disables persistence (used in tests)
	urlBase    string
}

// NewRuntime constructs a Runtime seeded from the given config and
// bound to the config file at configPath. Pass an empty path to
// disable persistence (writes become in-memory only).
func NewRuntime(cfg config.Config, configPath string) *Runtime {
	return &Runtime{
		configPath: configPath,
		urlBase:    cfg.Server.URLBase,
	}
}

// URLBase returns the current runtime URL base. Always safe to call
// from any goroutine.
func (rt *Runtime) URLBase() string {
	rt.mu.RLock()
	defer rt.mu.RUnlock()
	return rt.urlBase
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
