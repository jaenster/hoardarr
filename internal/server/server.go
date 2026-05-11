// Package server is hoardarr's HTTP composition layer.
//
// It owns the http.ServeMux, mounts public and API-key-protected routes,
// and serves the embedded React frontend (with SPA fallback to
// index.html for client-side routes).
//
// Domain logic does not live here. Handlers are thin: they decode HTTP
// inputs, call into application services (defined in internal/app/*),
// and encode responses. Application services are wired by internal/bootstrap
// and injected into Server via constructor options as they land.
package server

import (
	"encoding/json"
	"io/fs"
	"log/slog"
	"net/http"
	"strings"
	"sync"

	"github.com/jaenster/hoardarr/internal/api/rest"
	"github.com/jaenster/hoardarr/internal/api/sab"
	"github.com/jaenster/hoardarr/internal/api/sse"
	"github.com/jaenster/hoardarr/internal/config"
)

// Server wraps an http.ServeMux with hoardarr-specific routing,
// authentication middleware, and the frontend filesystem.
type Server struct {
	cfg     config.Config
	runtime *Runtime
	logger  *slog.Logger
	mux     *http.ServeMux
	web     fs.FS
	session SessionAuthenticator // optional; nil disables session auth (API key only)
	feCache *frontendCache       // lazy; rebuilt when URLBase changes
}

// New constructs a Server with the given configuration. web may be nil;
// when nil, requests for the frontend get a dev-placeholder page.
//
// runtime may be nil; when nil, URLBase is taken from cfg and is
// effectively read-only at runtime.
func New(cfg config.Config, runtime *Runtime, logger *slog.Logger, web fs.FS) *Server {
	if logger == nil {
		logger = slog.Default()
	}
	if runtime == nil {
		runtime = NewRuntime(cfg, "")
	}
	s := &Server{
		cfg:     cfg,
		runtime: runtime,
		logger:  logger,
		mux:     http.NewServeMux(),
		web:     web,
	}
	if web != nil {
		s.feCache = &frontendCache{web: web}
	}
	s.routes()
	return s
}

// Runtime returns the runtime-mutable config view. Useful for
// handlers that need to read or mutate URLBase post-construction.
func (s *Server) Runtime() *Runtime { return s.runtime }

// ServeHTTP makes Server an http.Handler. When URLBase is set, the
// prefix is stripped from incoming requests before routing — every
// internal route (REST, SSE, SAB, frontend) is registered without the
// prefix so the code is portable across deployments.
//
// Root convenience: if someone hits "/" with URLBase set, we redirect
// to "<base>/" so a bookmark of the host root lands on the SPA
// instead of 404. Same for "<base>" (no trailing slash) so the
// document base resolves correctly.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Wrap once with the access logger so every dispatched route gets
	// logged at debug level (no-op for higher log levels). Kept close
	// to ServeHTTP so we measure the full URLBase-strip + mux dispatch.
	accessLog(s.logger, http.HandlerFunc(s.serveInner)).ServeHTTP(w, r)
}

// serveInner is the original ServeHTTP body. Split out so the access
// log middleware sits at the boundary and sees the original request
// (with URLBase prefix intact) instead of the stripped form.
func (s *Server) serveInner(w http.ResponseWriter, r *http.Request) {
	base := s.runtime.URLBase()
	if base == "" {
		s.mux.ServeHTTP(w, r)
		return
	}
	if r.URL.Path == "/" || r.URL.Path == base {
		http.Redirect(w, r, base+"/", http.StatusMovedPermanently)
		return
	}
	if !strings.HasPrefix(r.URL.Path, base+"/") {
		http.NotFound(w, r)
		return
	}
	r2 := r.Clone(r.Context())
	r2.URL.Path = strings.TrimPrefix(r.URL.Path, base)
	if r2.URL.Path == "" {
		r2.URL.Path = "/"
	}
	if r.URL.RawPath != "" {
		r2.URL.RawPath = strings.TrimPrefix(r.URL.RawPath, base)
	}
	s.mux.ServeHTTP(w, r2)
}

// SetSessionAuthenticator installs the session validator used by the
// auth middleware. Pass nil to disable session auth entirely
// (API-key-only mode, for tests or stripped-down deployments).
//
// Must be called before MountREST / MountSSE so the middleware closure
// captures the right authenticator.
func (s *Server) SetSessionAuthenticator(sa SessionAuthenticator) {
	s.session = sa
}

// MountREST registers the /api/v1/* routes from rest.Handlers under
// the server's hybrid auth middleware (session cookie OR API key).
//
// /api/v1/health and the unauthenticated auth endpoints
// (/auth/setup, /auth/login, /auth/whoami) bypass the middleware.
func (s *Server) MountREST(h *rest.Handlers) {
	h.Mount(s.mux, authMiddleware(s.cfg.Auth.APIKey, s.session))
}

// MountSAB registers /sabnzbd/api with its own auth scheme: the SAB
// surface authenticates by ?apikey= (form or query), not by cookie or
// X-Api-Key header — that's what the *arr suite expects. The handler
// performs its own apikey check inside, so we don't wrap with the REST
// middleware here.
func (s *Server) MountSAB(h *sab.Handler) {
	s.mux.Handle("/sabnzbd/api", h)
	// Some clients path-prefix without /api; SAB itself accepts both.
	s.mux.Handle("/sabnzbd/", h)
}

// MountSSE registers /api/v1/queue/stream backed by the live event hub.
// Same hybrid auth as REST. EventSource clients without a session
// cookie pass the API key via ?apikey= query param (browsers can't
// set custom headers on EventSource).
func (s *Server) MountSSE(hub *sse.Hub) {
	protect := authMiddleware(s.cfg.Auth.APIKey, s.session)
	s.mux.Handle("GET /api/v1/queue/stream", protect(sse.Handler(hub)))
}

// routes mounts the request handlers.
//
// Routing convention:
//   - Public: /api/v1/health (liveness/readiness)
//   - Public auth bootstrap: /api/v1/auth/{whoami,setup,login} — needed
//     before the user has any credential
//   - Protected by hybrid auth (session cookie OR API key): every
//     other /api/v1/* route, including /api/v1/auth/logout
//   - SAB compatibility (M5+): /sabnzbd/api will be mounted similarly,
//     using the same middleware.
//   - Frontend (SPA): everything else.
//
// Each protected route is wrapped individually so the public endpoints
// can co-exist under /api/v1/ without subverting auth.
func (s *Server) routes() {
	protect := authMiddleware(s.cfg.Auth.APIKey, s.session)

	// Public.
	s.mux.HandleFunc("GET /api/v1/health", s.handleHealth)

	// Protected. Mounted per-route so the public health endpoint is not
	// accidentally guarded.
	s.mux.Handle("GET /api/v1/whoami", protect(http.HandlerFunc(s.handleWhoami)))

	// Frontend (SPA fallback).
	s.mux.Handle("/", s.handleFrontend())
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"status":  "ok",
		"service": "hoardarr",
	})
}

// handleWhoami is a small protected probe used by clients (and the
// in-tree e2e tests) to confirm their API key is accepted. It does not
// reveal the key itself.
func (s *Server) handleWhoami(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"service":       "hoardarr",
		"version":       "0.0.1-dev",
		"authenticated": true,
	})
}

// sentinelBase is what Vite bakes into every asset URL,
// import.meta.env.BASE_URL reference, and CSS url(). The frontend
// handler swaps it for the runtime base (URLBase + "/") on serve.
const sentinelBase = "/__HOARDARR_BASE__/"

// frontendCache memoises sentinel-replaced asset bytes per URLBase.
// On the first request and any time URLBase changes, the cache walks
// the embed FS once, rewrites every text file, and serves from the
// resulting map until URLBase changes again.
type frontendCache struct {
	web fs.FS

	mu     sync.Mutex
	base   string             // URLBase that "mapped" + "index" were built for
	built  bool               // false until the first build succeeds
	mapped map[string][]byte  // rewritten text files keyed by FS path
	index  []byte             // rewritten index.html for SPA fallback
}

// Get returns the rewritten frontend FS for the given URLBase,
// rebuilding the cache if the base changed since the last call.
func (c *frontendCache) Get(currentBase string) (map[string][]byte, []byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.built && c.base == currentBase {
		return c.mapped, c.index, nil
	}
	mapped, index, err := buildFrontendFSAt(c.web, currentBase)
	if err != nil {
		return nil, nil, err
	}
	c.base = currentBase
	c.mapped = mapped
	c.index = index
	c.built = true
	return mapped, index, nil
}

// handleFrontend serves the SPA: static files from web FS, with a
// fallback to index.html for client-side routes.
//
// All emitted asset URLs and code references to BASE_URL contain a
// fixed sentinel string. The cache walks the embed FS once per
// URLBase value, replaces the sentinel with the runtime base in
// every text file, and serves from the resulting in-memory map.
// When the operator changes URLBase from Settings, the next request
// rebuilds the cache transparently.
//
// If web is nil (no build present), serves a dev placeholder.
func (s *Server) handleFrontend() http.Handler {
	if s.web == nil {
		return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			_, _ = w.Write([]byte(devPlaceholderHTML))
		})
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mapped, indexHTML, err := s.feCache.Get(s.runtime.URLBase())
		if err != nil {
			s.logger.Error("frontend: cache build failed", "err", err)
			http.Error(w, "frontend assets missing", http.StatusInternalServerError)
			return
		}
		clean := strings.TrimPrefix(r.URL.Path, "/")
		if clean == "" || clean == "index.html" {
			serveIndexHTML(w, indexHTML)
			return
		}
		if body, ok := mapped[clean]; ok {
			serveAsset(w, clean, body)
			return
		}
		if _, statErr := fs.Stat(s.web, clean); statErr != nil {
			// SPA fallback for client-side routes.
			serveIndexHTML(w, indexHTML)
			return
		}
		http.FileServerFS(s.web).ServeHTTP(w, r)
	})
}

// buildFrontendFSAt walks web once and returns the sentinel-replaced
// bytes of every text file, plus the rewritten index.html for
// convenient SPA fallback. Binary files (fonts, images) are absent
// from the map; the request handler falls through to FileServerFS
// for those.
//
// "Text" here is a static allow-list of extensions Vite emits with
// the base path baked in. Adding to the list is cheap and safer than
// trying to sniff content type.
func buildFrontendFSAt(web fs.FS, base string) (map[string][]byte, []byte, error) {
	runtimeBase := base + "/"
	if base == "" {
		runtimeBase = "/"
	}
	rewrite := func(b []byte) []byte {
		return []byte(strings.ReplaceAll(string(b), sentinelBase, runtimeBase))
	}
	out := make(map[string][]byte)
	var indexHTML []byte
	err := fs.WalkDir(web, ".", func(p string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() {
			return nil
		}
		if !isRewriteable(p) {
			return nil
		}
		body, readErr := fs.ReadFile(web, p)
		if readErr != nil {
			return readErr
		}
		body = rewrite(body)
		out[p] = body
		if p == "index.html" {
			indexHTML = body
		}
		return nil
	})
	if err != nil {
		return nil, nil, err
	}
	if indexHTML == nil {
		raw, readErr := fs.ReadFile(web, "index.html")
		if readErr != nil {
			return nil, nil, readErr
		}
		indexHTML = rewrite(raw)
	}
	return out, indexHTML, nil
}

// isRewriteable lists the file extensions whose bytes can mention
// the sentinel. Everything else (woff, png, ico, …) is left alone
// and served via FileServerFS unmodified.
func isRewriteable(p string) bool {
	switch {
	case strings.HasSuffix(p, ".html"),
		strings.HasSuffix(p, ".js"),
		strings.HasSuffix(p, ".mjs"),
		strings.HasSuffix(p, ".css"),
		strings.HasSuffix(p, ".map"),
		strings.HasSuffix(p, ".svg"):
		return true
	}
	return false
}

func serveIndexHTML(w http.ResponseWriter, body []byte) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	// index.html depends on runtime base, so don't allow stale caches
	// after a URLBase change. Hashed asset filenames protect the rest.
	w.Header().Set("Cache-Control", "no-cache")
	_, _ = w.Write(body)
}

func serveAsset(w http.ResponseWriter, p string, body []byte) {
	switch {
	case strings.HasSuffix(p, ".js"), strings.HasSuffix(p, ".mjs"):
		w.Header().Set("Content-Type", "application/javascript; charset=utf-8")
	case strings.HasSuffix(p, ".css"):
		w.Header().Set("Content-Type", "text/css; charset=utf-8")
	case strings.HasSuffix(p, ".map"):
		w.Header().Set("Content-Type", "application/json")
	case strings.HasSuffix(p, ".svg"):
		w.Header().Set("Content-Type", "image/svg+xml")
	case strings.HasSuffix(p, ".html"):
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
	}
	_, _ = w.Write(body)
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

const devPlaceholderHTML = `<!doctype html>
<html><head><meta charset="utf-8"><title>hoardarr — dev</title>
<style>body{font-family:ui-sans-serif,system-ui,sans-serif;background:#1a1d24;color:#e4e6eb;padding:2rem;max-width:42rem;margin:auto;line-height:1.5}code{background:#2a2e36;padding:.15rem .35rem;border-radius:.2rem}</style>
</head><body>
<h1>hoardarr</h1>
<p>No frontend build found. Either:</p>
<ul>
  <li>Run <code>cd frontend &amp;&amp; npm install &amp;&amp; npm run dev</code> for live dev (Vite on :5173).</li>
  <li>Or build it once with <code>cd frontend &amp;&amp; npm install &amp;&amp; npm run build</code>; then this page becomes the React app.</li>
  <li>For a production single-binary, build with <code>go build -tags embed ./cmd/hoardarr</code> after the frontend is built.</li>
</ul>
<p>API health: <a href="/api/v1/health">/api/v1/health</a></p>
</body></html>`
