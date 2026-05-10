package server

import (
	"crypto/subtle"
	"encoding/json"
	"net/http"
)

// apiKeyMiddleware returns middleware that rejects requests without a
// matching API key. Two locations are accepted:
//
//   - HTTP header "X-Api-Key: <key>"
//   - Query parameter "?apikey=<key>"
//
// The query-parameter form matches SABnzbd / Sonarr conventions, so the
// same middleware can guard both /api/v1/* and /sabnzbd/api endpoints.
//
// Comparison uses crypto/subtle.ConstantTimeCompare to avoid timing
// leakage of the configured key.
//
// Failures return JSON {"error": "..."} with HTTP 401 so clients
// (including SAB API consumers) get a structured response.
func apiKeyMiddleware(expected string) func(http.Handler) http.Handler {
	expectedBytes := []byte(expected)
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			provided := r.Header.Get("X-Api-Key")
			if provided == "" {
				provided = r.URL.Query().Get("apikey")
			}
			if !constantTimeStringEq(provided, expectedBytes) {
				writeAuthError(w, "missing or invalid API key")
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

func constantTimeStringEq(provided string, expected []byte) bool {
	if len(provided) == 0 || len(expected) == 0 {
		return false
	}
	pb := []byte(provided)
	if len(pb) != len(expected) {
		// Still run the compare to avoid a length-based timing tell.
		_ = subtle.ConstantTimeCompare(pb, pb)
		return false
	}
	return subtle.ConstantTimeCompare(pb, expected) == 1
}

func writeAuthError(w http.ResponseWriter, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusUnauthorized)
	_ = json.NewEncoder(w).Encode(map[string]any{"error": msg})
}
