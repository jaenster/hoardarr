package server

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"net/http"

	"github.com/jaenster/hoardarr/internal/api/rest"
	"github.com/jaenster/hoardarr/internal/domain/auth"
)

// SessionAuthenticator is the slice of the auth.Service that the
// middleware needs. Defining it as an interface keeps the server
// package decoupled from app/auth.
type SessionAuthenticator interface {
	AuthenticateRequest(ctx context.Context, token string) (*auth.User, error)
}

// authMiddleware returns middleware that accepts EITHER a valid
// session cookie OR a valid API key. Either is sufficient.
//
// The middleware exists for v0.1's mixed-auth model:
//
//   - Browser users login via /api/v1/auth/login → get a session
//     cookie → present it on every subsequent request.
//   - *arr clients (Sonarr, Radarr, Lidarr, Readarr, Prowlarr) and
//     SAB-API consumers can't easily do form-based auth, so they
//     present X-Api-Key (or ?apikey=) using the config'd shared key.
//
// A request without either credential is rejected 401.
func authMiddleware(expectedKey string, sa SessionAuthenticator) func(http.Handler) http.Handler {
	expectedBytes := []byte(expectedKey)
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Session cookie path (preferred for browser flows).
			if sa != nil {
				if cookie, err := r.Cookie(rest.SessionCookieName); err == nil && cookie.Value != "" {
					if _, err := sa.AuthenticateRequest(r.Context(), cookie.Value); err == nil {
						next.ServeHTTP(w, r)
						return
					}
				}
			}

			// API key path (header or query param).
			provided := r.Header.Get("X-Api-Key")
			if provided == "" {
				provided = r.URL.Query().Get("apikey")
			}
			if constantTimeStringEq(provided, expectedBytes) {
				next.ServeHTTP(w, r)
				return
			}

			writeAuthError(w, "authentication required")
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
