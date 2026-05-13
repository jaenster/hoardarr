package rest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/jaenster/hoardarr/internal/domain/auth"
)

// SessionCookieName is the HTTP cookie hoardarr issues on Login. The
// browser presents it on every subsequent request to the API.
const SessionCookieName = "hoardarr_session"

// Auther is the slice of the auth.Service that REST needs.
// Defined here as an interface so tests can pass a fake without
// pulling the full auth.Service in.
type Auther interface {
	NeedsSetup(ctx context.Context) (bool, error)
	SetupAdmin(ctx context.Context, username, password string) (auth.UserID, error)
	Login(ctx context.Context, username, password string) (auth.Session, error)
	Logout(ctx context.Context, token string) error
	AuthenticateRequest(ctx context.Context, token string) (*auth.User, error)
	ChangePassword(ctx context.Context, userID auth.UserID, oldPassword, newPassword string) error
}

// Mount registers /api/v1/auth/* routes. setup and login are public;
// whoami and logout require any of: session cookie OR API key
// (the protected wrapper handles both).
//
// Login + setup are wrapped with a per-IP sliding-window rate limit
// (10 attempts per minute by default). The bcrypt verification in
// handleLogin would otherwise let a single attacker pin a goroutine
// per attempt for ~250ms each; this caps the damage they can do
// without coordinating an IP rotation. Setup gets the same treatment
// because it's the only state-mutating public endpoint and the cost
// of an early hit is permanent (admin password set in concrete).
func (h *Handlers) mountAuth(mux *http.ServeMux, protect func(http.Handler) http.Handler) {
	loginLimiter := NewIPRateLimiter(10, time.Minute)
	mux.HandleFunc("GET /api/v1/auth/whoami", h.handleWhoami)
	mux.HandleFunc("POST /api/v1/auth/setup", rateLimitedHandler(loginLimiter, h.handleSetup))
	mux.HandleFunc("POST /api/v1/auth/login", rateLimitedHandler(loginLimiter, h.handleLogin))
	mux.Handle("POST /api/v1/auth/logout", protect(http.HandlerFunc(h.handleLogout)))
	mux.Handle("POST /api/v1/auth/change-password", protect(http.HandlerFunc(h.handleChangePassword)))
	mux.Handle("POST /api/v1/auth/rotate-api-key", protect(http.HandlerFunc(h.handleRotateAPIKey)))
}

// handleRotateAPIKey regenerates the server-wide API key. Returns the
// new key once in the response body — the caller MUST copy it into
// every *arr client before the next request, because the old key
// stops working immediately. Session-cookie auth is unaffected; the
// browser keeps its session.
func (h *Handlers) handleRotateAPIKey(w http.ResponseWriter, _ *http.Request) {
	if h.Runtime == nil {
		h.writeError(w, http.StatusServiceUnavailable, errors.New("runtime config unavailable"))
		return
	}
	writer, ok := h.Runtime.(URLBaseWriter)
	if !ok {
		h.writeError(w, http.StatusServiceUnavailable, errors.New("runtime config is read-only"))
		return
	}
	key, err := writer.RotateAPIKey()
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"api_key": key})
}

// handleWhoami returns the auth state. The frontend probes this on
// load to decide which screen to show:
//   - {"state":"needs_setup"} → render the first-run admin form
//   - {"state":"needs_login"} → render the login form
//   - {"state":"authenticated", "user": {...}} → render the app
//
// This endpoint is intentionally unauthenticated — it leaks no
// secrets and the frontend needs it BEFORE having any credentials.
func (h *Handlers) handleWhoami(w http.ResponseWriter, r *http.Request) {
	if h.Auth == nil {
		writeJSON(w, http.StatusOK, map[string]any{"state": "authenticated"})
		return
	}
	needsSetup, err := h.Auth.NeedsSetup(r.Context())
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, err)
		return
	}
	if needsSetup {
		writeJSON(w, http.StatusOK, map[string]any{"state": "needs_setup"})
		return
	}
	// Try to authenticate via session cookie.
	cookie, err := r.Cookie(SessionCookieName)
	if err == nil && cookie.Value != "" {
		user, err := h.Auth.AuthenticateRequest(r.Context(), cookie.Value)
		if err == nil {
			writeJSON(w, http.StatusOK, map[string]any{
				"state": "authenticated",
				"user": map[string]any{
					"id":       int64(user.ID()),
					"username": user.Username(),
					"role":     string(user.Role()),
				},
			})
			return
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"state": "needs_login"})
}

type setupReq struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

func (h *Handlers) handleSetup(w http.ResponseWriter, r *http.Request) {
	if h.Auth == nil {
		h.writeError(w, http.StatusServiceUnavailable, errors.New("auth disabled"))
		return
	}
	var req setupReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.writeError(w, http.StatusBadRequest, fmt.Errorf("decode: %w", err))
		return
	}
	if strings.TrimSpace(req.Username) == "" || req.Password == "" {
		h.writeError(w, http.StatusBadRequest, errors.New("username and password required"))
		return
	}
	if len(req.Password) < 8 {
		h.writeError(w, http.StatusBadRequest, errors.New("password must be at least 8 characters"))
		return
	}
	id, err := h.Auth.SetupAdmin(r.Context(), req.Username, req.Password)
	if err != nil {
		switch {
		case errors.Is(err, auth.ErrSetupAlreadyDone):
			h.writeError(w, http.StatusConflict, err)
		default:
			h.writeError(w, http.StatusBadRequest, err)
		}
		return
	}
	// Issue a session immediately so the operator doesn't have to log
	// in right after setup.
	sess, err := h.Auth.Login(r.Context(), req.Username, req.Password)
	if err != nil {
		// Setup succeeded but login failed — surface as success
		// without cookie; the frontend will handle the next step.
		writeJSON(w, http.StatusCreated, map[string]any{"id": int64(id)})
		return
	}
	h.setSessionCookie(w, sess)
	writeJSON(w, http.StatusCreated, map[string]any{"id": int64(id)})
}

type loginReq struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

func (h *Handlers) handleLogin(w http.ResponseWriter, r *http.Request) {
	if h.Auth == nil {
		h.writeError(w, http.StatusServiceUnavailable, errors.New("auth disabled"))
		return
	}
	var req loginReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.writeError(w, http.StatusBadRequest, fmt.Errorf("decode: %w", err))
		return
	}
	sess, err := h.Auth.Login(r.Context(), req.Username, req.Password)
	if err != nil {
		if errors.Is(err, auth.ErrInvalidCredentials) {
			h.writeError(w, http.StatusUnauthorized, err)
			return
		}
		h.writeError(w, http.StatusInternalServerError, err)
		return
	}
	h.setSessionCookie(w, sess)
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

type changePasswordReq struct {
	OldPassword string `json:"old_password"`
	NewPassword string `json:"new_password"`
}

// handleChangePassword requires an authenticated session (the protect
// wrapper enforces this). The session cookie identifies which user is
// asking; we reject if the user isn't found despite the valid session
// (shouldn't happen in practice but tightens the model).
func (h *Handlers) handleChangePassword(w http.ResponseWriter, r *http.Request) {
	if h.Auth == nil {
		h.writeError(w, http.StatusServiceUnavailable, errors.New("auth disabled"))
		return
	}
	cookie, err := r.Cookie(SessionCookieName)
	if err != nil || cookie.Value == "" {
		h.writeError(w, http.StatusUnauthorized, errors.New("session required"))
		return
	}
	user, err := h.Auth.AuthenticateRequest(r.Context(), cookie.Value)
	if err != nil {
		h.writeError(w, http.StatusUnauthorized, errors.New("session invalid"))
		return
	}
	var req changePasswordReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.writeError(w, http.StatusBadRequest, fmt.Errorf("decode: %w", err))
		return
	}
	if req.OldPassword == "" || req.NewPassword == "" {
		h.writeError(w, http.StatusBadRequest, errors.New("old_password and new_password required"))
		return
	}
	if err := h.Auth.ChangePassword(r.Context(), user.ID(), req.OldPassword, req.NewPassword); err != nil {
		if errors.Is(err, auth.ErrInvalidCredentials) {
			h.writeError(w, http.StatusUnauthorized, errors.New("current password is incorrect"))
			return
		}
		h.writeError(w, http.StatusBadRequest, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handlers) handleLogout(w http.ResponseWriter, r *http.Request) {
	if h.Auth == nil {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if cookie, err := r.Cookie(SessionCookieName); err == nil && cookie.Value != "" {
		_ = h.Auth.Logout(r.Context(), cookie.Value)
	}
	h.clearSessionCookie(w)
	w.WriteHeader(http.StatusNoContent)
}

// setSessionCookie writes the session token to the client as an
// HTTP-only cookie. Secure is left off for now because most
// self-hosted setups deploy behind a reverse-proxy that terminates
// TLS upstream — cookies set with Secure=true wouldn't make it
// across the proxy hop.
func (h *Handlers) setSessionCookie(w http.ResponseWriter, sess auth.Session) {
	maxAge := int(time.Until(sess.ExpiresAt).Seconds())
	if maxAge < 0 {
		maxAge = 0
	}
	http.SetCookie(w, &http.Cookie{
		Name:     SessionCookieName,
		Value:    sess.Token,
		Path:     h.sessionCookiePath(),
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
		MaxAge:   maxAge,
	})
}

func (h *Handlers) clearSessionCookie(w http.ResponseWriter) {
	http.SetCookie(w, &http.Cookie{
		Name:     SessionCookieName,
		Value:    "",
		Path:     h.sessionCookiePath(),
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
		MaxAge:   -1,
	})
}
