package bootstrap_test

// End-to-end auth: drive /api/v1/auth/* through the real HTTP stack
// against a freshly-built App. No NNTP, no jobs — just the auth flow.
//
// Flow:
//
//   empty DB → /auth/whoami says needs_setup
//   /api/v1/queue without creds → 401
//   /auth/setup creates the first admin and auto-issues a session cookie
//   /auth/setup again → 409 (setup-already-done)
//   /api/v1/queue with cookie → 200
//   /auth/login with wrong password → 401
//   /auth/login with right password → 200, cookie set
//   /auth/logout → cookie invalidated; reusing it → 401
//   API key still works for *arr clients (header path, no cookie)

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/cookiejar"
	"path/filepath"
	"testing"
	"time"

	"github.com/jaenster/hoardarr/internal/api/rest"
	"github.com/jaenster/hoardarr/internal/bootstrap"
	"github.com/jaenster/hoardarr/internal/config"
)

func TestAuth_E2E_FullFlow(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.toml")

	t.Setenv("HOARDARR_LISTEN", "127.0.0.1:"+mustFreePort(t))
	t.Setenv("HOARDARR_DATA_DIR", filepath.Join(dir, "data"))

	cfg, err := config.LoadOrCreate(cfgPath)
	if err != nil {
		t.Fatalf("LoadOrCreate: %v", err)
	}
	apiKey := cfg.Auth.APIKey

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	app, err := bootstrap.Build(ctx, cfg, nil, nil)
	if err != nil {
		t.Fatalf("bootstrap.Build: %v", err)
	}
	defer func() { _ = app.Shutdown() }()

	runDone := make(chan error, 1)
	go func() { runDone <- app.Run(ctx) }()
	waitListen(t, cfg.Server.Listen)
	base := "http://" + cfg.Server.Listen
	defer func() {
		cancel()
		select {
		case <-runDone:
		case <-time.After(5 * time.Second):
			t.Errorf("Run did not return within 5s after cancel")
		}
	}()

	// Each named step uses a fresh client unless it explicitly wants to
	// reuse cookies, so we don't accidentally carry session state across
	// scenarios that should be independent.

	// 1. whoami on empty DB.
	{
		c := newClient(t)
		var w whoamiResp
		authGetJSON(t, c, base+"/api/v1/auth/whoami", &w)
		if w.State != "needs_setup" {
			t.Fatalf("empty DB whoami state = %q; want needs_setup", w.State)
		}
	}

	// 2. queue without creds → 401.
	{
		c := newClient(t)
		resp := mustDo(t, c, must(http.NewRequest(http.MethodGet, base+"/api/v1/queue", nil)))
		if resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("/api/v1/queue (no auth) = %d; want 401", resp.StatusCode)
		}
		_ = resp.Body.Close()
	}

	// 3. setup creates admin + auto-issues cookie.
	cookieClient := newClient(t)
	{
		body := mustJSON(t, map[string]string{
			"username": "admin",
			"password": "supers3cret",
		})
		req := must(http.NewRequest(http.MethodPost, base+"/api/v1/auth/setup", body))
		req.Header.Set("Content-Type", "application/json")
		resp := mustDo(t, cookieClient, req)
		if resp.StatusCode != http.StatusCreated {
			t.Fatalf("/auth/setup = %d; want 201", resp.StatusCode)
		}
		_ = resp.Body.Close()
		if !hasSessionCookie(cookieClient, base) {
			t.Fatalf("setup did not issue session cookie")
		}
	}

	// 4. queue with cookie → 200.
	{
		resp := mustDo(t, cookieClient,
			must(http.NewRequest(http.MethodGet, base+"/api/v1/queue", nil)))
		if resp.StatusCode != http.StatusOK {
			b, _ := io.ReadAll(resp.Body)
			t.Fatalf("/api/v1/queue (cookie) = %d; want 200; body=%s", resp.StatusCode, b)
		}
		_ = resp.Body.Close()
	}

	// 5. whoami via cookie reports authenticated.
	{
		var w whoamiResp
		authGetJSON(t, cookieClient, base+"/api/v1/auth/whoami", &w)
		if w.State != "authenticated" {
			t.Fatalf("authed whoami state = %q; want authenticated", w.State)
		}
		if w.User == nil || w.User.Username != "admin" {
			t.Fatalf("authed whoami user = %+v; want username=admin", w.User)
		}
	}

	// 6. setup-again is rejected.
	{
		body := mustJSON(t, map[string]string{
			"username": "admin2",
			"password": "anotherone",
		})
		req := must(http.NewRequest(http.MethodPost, base+"/api/v1/auth/setup", body))
		req.Header.Set("Content-Type", "application/json")
		resp := mustDo(t, newClient(t), req)
		if resp.StatusCode != http.StatusConflict {
			t.Fatalf("second /auth/setup = %d; want 409", resp.StatusCode)
		}
		_ = resp.Body.Close()
	}

	// 7. whoami without cookie now reports needs_login (users exist).
	{
		c := newClient(t)
		var w whoamiResp
		authGetJSON(t, c, base+"/api/v1/auth/whoami", &w)
		if w.State != "needs_login" {
			t.Fatalf("post-setup anonymous whoami state = %q; want needs_login", w.State)
		}
	}

	// 8. wrong password → 401.
	{
		body := mustJSON(t, map[string]string{
			"username": "admin",
			"password": "wrong-password",
		})
		req := must(http.NewRequest(http.MethodPost, base+"/api/v1/auth/login", body))
		req.Header.Set("Content-Type", "application/json")
		resp := mustDo(t, newClient(t), req)
		if resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("/auth/login (bad pw) = %d; want 401", resp.StatusCode)
		}
		_ = resp.Body.Close()
	}

	// 9. unknown user → 401 (matches bad-pw to avoid user enumeration).
	{
		body := mustJSON(t, map[string]string{
			"username": "ghost",
			"password": "supers3cret",
		})
		req := must(http.NewRequest(http.MethodPost, base+"/api/v1/auth/login", body))
		req.Header.Set("Content-Type", "application/json")
		resp := mustDo(t, newClient(t), req)
		if resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("/auth/login (unknown user) = %d; want 401", resp.StatusCode)
		}
		_ = resp.Body.Close()
	}

	// 10. correct login issues a fresh cookie on a new client.
	freshClient := newClient(t)
	{
		body := mustJSON(t, map[string]string{
			"username": "admin",
			"password": "supers3cret",
		})
		req := must(http.NewRequest(http.MethodPost, base+"/api/v1/auth/login", body))
		req.Header.Set("Content-Type", "application/json")
		resp := mustDo(t, freshClient, req)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("/auth/login = %d; want 200", resp.StatusCode)
		}
		_ = resp.Body.Close()
		if !hasSessionCookie(freshClient, base) {
			t.Fatalf("login did not issue session cookie")
		}
	}

	// 11. logout clears the session — the same cookie should no longer
	//     authenticate. Capture the cookie value before logout so we
	//     can replay it manually.
	staleCookie := readSessionCookie(freshClient, base)
	if staleCookie == "" {
		t.Fatalf("expected session cookie before logout")
	}
	{
		req := must(http.NewRequest(http.MethodPost, base+"/api/v1/auth/logout", nil))
		resp := mustDo(t, freshClient, req)
		if resp.StatusCode != http.StatusNoContent {
			t.Fatalf("/auth/logout = %d; want 204", resp.StatusCode)
		}
		_ = resp.Body.Close()
	}

	// 12. replay the stale cookie on a bare client → 401 (server-side
	//     deletion is what we're verifying, not just cookie clearing).
	{
		req := must(http.NewRequest(http.MethodGet, base+"/api/v1/queue", nil))
		req.AddCookie(&http.Cookie{Name: rest.SessionCookieName, Value: staleCookie})
		resp := mustDo(t, newClient(t), req)
		if resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("/api/v1/queue with stale cookie = %d; want 401", resp.StatusCode)
		}
		_ = resp.Body.Close()
	}

	// 13. API key path (header) still works — for *arr clients that
	//     can't do cookie auth.
	{
		req := must(http.NewRequest(http.MethodGet, base+"/api/v1/queue", nil))
		req.Header.Set("X-Api-Key", apiKey)
		resp := mustDo(t, newClient(t), req)
		if resp.StatusCode != http.StatusOK {
			b, _ := io.ReadAll(resp.Body)
			t.Fatalf("/api/v1/queue (api key) = %d; want 200; body=%s", resp.StatusCode, b)
		}
		_ = resp.Body.Close()
	}

	// 14. short password is rejected at setup. Use a separate temp
	//     bootstrap so we don't fight the existing admin row. Skipped:
	//     covered by domain-level tests; an HTTP-level repro would
	//     require a second app instance and adds little signal.

	// 15. change-password flow. Login fresh, change the password,
	//     then assert old creds fail and new creds succeed.
	freshClient2 := newClient(t)
	{
		body := mustJSON(t, map[string]string{
			"username": "admin",
			"password": "supers3cret",
		})
		req := must(http.NewRequest(http.MethodPost, base+"/api/v1/auth/login", body))
		req.Header.Set("Content-Type", "application/json")
		resp := mustDo(t, freshClient2, req)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("login for change-password setup: %d", resp.StatusCode)
		}
	}
	// Wrong old → 401.
	{
		body := mustJSON(t, map[string]string{
			"old_password": "wrong",
			"new_password": "brandnew1234",
		})
		req := must(http.NewRequest(http.MethodPost, base+"/api/v1/auth/change-password", body))
		req.Header.Set("Content-Type", "application/json")
		resp := mustDo(t, freshClient2, req)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusUnauthorized {
			t.Errorf("wrong old: status %d; want 401", resp.StatusCode)
		}
	}
	// Too-short new → 400.
	{
		body := mustJSON(t, map[string]string{
			"old_password": "supers3cret",
			"new_password": "short",
		})
		req := must(http.NewRequest(http.MethodPost, base+"/api/v1/auth/change-password", body))
		req.Header.Set("Content-Type", "application/json")
		resp := mustDo(t, freshClient2, req)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("short new: status %d; want 400", resp.StatusCode)
		}
	}
	// Happy path → 204.
	{
		body := mustJSON(t, map[string]string{
			"old_password": "supers3cret",
			"new_password": "brandnew1234",
		})
		req := must(http.NewRequest(http.MethodPost, base+"/api/v1/auth/change-password", body))
		req.Header.Set("Content-Type", "application/json")
		resp := mustDo(t, freshClient2, req)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusNoContent {
			t.Fatalf("change-password: status %d; want 204", resp.StatusCode)
		}
	}
	// Old password no longer logs in.
	{
		body := mustJSON(t, map[string]string{
			"username": "admin",
			"password": "supers3cret",
		})
		req := must(http.NewRequest(http.MethodPost, base+"/api/v1/auth/login", body))
		req.Header.Set("Content-Type", "application/json")
		resp := mustDo(t, newClient(t), req)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusUnauthorized {
			t.Errorf("old password after change: status %d; want 401", resp.StatusCode)
		}
	}
	// New password does.
	{
		body := mustJSON(t, map[string]string{
			"username": "admin",
			"password": "brandnew1234",
		})
		req := must(http.NewRequest(http.MethodPost, base+"/api/v1/auth/login", body))
		req.Header.Set("Content-Type", "application/json")
		resp := mustDo(t, newClient(t), req)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Errorf("new password after change: status %d; want 200", resp.StatusCode)
		}
	}
}

// --- helpers ----------------------------------------------------------

type whoamiResp struct {
	State string         `json:"state"`
	User  *whoamiUserDTO `json:"user,omitempty"`
}
type whoamiUserDTO struct {
	ID       int64  `json:"id"`
	Username string `json:"username"`
	Role     string `json:"role"`
}

func newClient(t *testing.T) *http.Client {
	t.Helper()
	jar, err := cookiejar.New(nil)
	if err != nil {
		t.Fatalf("cookiejar.New: %v", err)
	}
	return &http.Client{
		Jar: jar,
		// 30s rather than the gut-feel 10s: bcrypt at default cost
		// (~70ms warm) climbs to several hundred ms under -race on
		// the GitHub-hosted runners, and the package's tests run
		// concurrently enough that a request can sit briefly behind
		// the scheduler. 30s is still well under the package-level
		// 10m go-test timeout in CI.
		Timeout: 30 * time.Second,
	}
}

func mustDo(t *testing.T, c *http.Client, req *http.Request) *http.Response {
	t.Helper()
	resp, err := c.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", req.Method, req.URL.Path, err)
	}
	return resp
}

func authGetJSON(t *testing.T, c *http.Client, url string, out any) {
	t.Helper()
	req := must(http.NewRequest(http.MethodGet, url, nil))
	resp := mustDo(t, c, req)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("GET %s = %d; body=%s", url, resp.StatusCode, body)
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		t.Fatalf("decode %s: %v", url, err)
	}
}

func mustJSON(t *testing.T, v any) *bytes.Reader {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}
	return bytes.NewReader(b)
}

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

func hasSessionCookie(c *http.Client, base string) bool {
	return readSessionCookie(c, base) != ""
}

func readSessionCookie(c *http.Client, base string) string {
	u, err := http.NewRequest(http.MethodGet, base, nil)
	if err != nil {
		return ""
	}
	for _, ck := range c.Jar.Cookies(u.URL) {
		if ck.Name == rest.SessionCookieName {
			return ck.Value
		}
	}
	return ""
}
