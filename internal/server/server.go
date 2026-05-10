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

	"github.com/jaenster/hoardarr/internal/api/rest"
	"github.com/jaenster/hoardarr/internal/api/sse"
	"github.com/jaenster/hoardarr/internal/config"
)

// Server wraps an http.ServeMux with hoardarr-specific routing,
// authentication middleware, and the frontend filesystem.
type Server struct {
	cfg     config.Config
	logger  *slog.Logger
	mux     *http.ServeMux
	web     fs.FS
	session SessionAuthenticator // optional; nil disables session auth (API key only)
}

// New constructs a Server with the given configuration. web may be nil;
// when nil, requests for the frontend get a dev-placeholder page.
func New(cfg config.Config, logger *slog.Logger, web fs.FS) *Server {
	if logger == nil {
		logger = slog.Default()
	}
	s := &Server{
		cfg:    cfg,
		logger: logger,
		mux:    http.NewServeMux(),
		web:    web,
	}
	s.routes()
	return s
}

// ServeHTTP makes Server an http.Handler.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
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

// handleFrontend serves the SPA: static files from web FS, with a fallback
// to index.html for client-side routes. If web is nil (no build present),
// serves a dev placeholder.
func (s *Server) handleFrontend() http.Handler {
	if s.web == nil {
		return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			_, _ = w.Write([]byte(devPlaceholderHTML))
		})
	}

	fileServer := http.FileServerFS(s.web)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		clean := strings.TrimPrefix(r.URL.Path, "/")
		if clean == "" {
			fileServer.ServeHTTP(w, r)
			return
		}
		if _, err := fs.Stat(s.web, clean); err != nil {
			r2 := r.Clone(r.Context())
			r2.URL.Path = "/"
			fileServer.ServeHTTP(w, r2)
			return
		}
		fileServer.ServeHTTP(w, r)
	})
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
