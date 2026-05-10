package server

import (
	"encoding/json"
	"io/fs"
	"log/slog"
	"net/http"
	"strings"

	"github.com/jaenster/hoardarr/internal/config"
)

type Server struct {
	cfg    config.Config
	logger *slog.Logger
	mux    *http.ServeMux
	web    fs.FS
}

func New(cfg config.Config, logger *slog.Logger, web fs.FS) *Server {
	s := &Server{
		cfg:    cfg,
		logger: logger,
		mux:    http.NewServeMux(),
		web:    web,
	}
	s.routes()
	return s
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}

func (s *Server) routes() {
	s.mux.HandleFunc("GET /api/v1/health", s.handleHealth)
	s.mux.Handle("/", s.handleFrontend())
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"status":  "ok",
		"service": "hoardarr",
	})
}

// handleFrontend serves the SPA: static files from web FS, with a fallback to
// index.html for client-side routes. If web is nil (no build present), serves
// a dev placeholder.
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
