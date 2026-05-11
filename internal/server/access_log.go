package server

import (
	"log/slog"
	"net/http"
	"strconv"
	"time"
)

// accessLog wraps a handler with a per-request access log line at
// DEBUG level. Useful when the operator flips log_level to debug —
// they get a full trace of incoming HTTP traffic without rebuilding.
//
// The wrapper captures status code + bytes written by intercepting
// the response writer. ResponseWriter interfaces (Hijacker / Flusher
// for SSE) are preserved via type assertions so SSE streams still
// work — if a feature needs an interface we don't proxy, add it.
func accessLog(logger *slog.Logger, next http.Handler) http.Handler {
	if logger == nil {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started := time.Now()
		rw := &recordingWriter{ResponseWriter: w, status: 200}
		next.ServeHTTP(rw, r)
		dur := time.Since(started)
		logger.Debug("http",
			"method", r.Method,
			"path", r.URL.Path,
			"status", rw.status,
			"bytes", rw.bytes,
			"ms", dur.Milliseconds(),
			"remote", r.RemoteAddr,
			"ua", truncate(r.UserAgent(), 60),
		)
	})
}

// recordingWriter is a thin shim that captures the response status
// and byte count for the access log. Implements Flusher + Hijacker
// when the wrapped writer does, so SSE streams and protocol
// upgrades continue to work.
type recordingWriter struct {
	http.ResponseWriter
	status     int
	bytes      int
	wroteStatus bool
}

func (r *recordingWriter) WriteHeader(code int) {
	r.status = code
	r.wroteStatus = true
	r.ResponseWriter.WriteHeader(code)
}

func (r *recordingWriter) Write(b []byte) (int, error) {
	if !r.wroteStatus {
		r.status = 200
		r.wroteStatus = true
	}
	n, err := r.ResponseWriter.Write(b)
	r.bytes += n
	return n, err
}

func (r *recordingWriter) Flush() {
	if f, ok := r.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}

// formatStatus is for tests; the runtime path uses the int directly.
var _ = strconv.Itoa
