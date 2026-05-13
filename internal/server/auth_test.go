package server

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/jaenster/hoardarr/internal/config"
)

func newTestServer(t *testing.T, apiKey string) *Server {
	t.Helper()
	cfg := config.Default()
	cfg.Auth.APIKey = apiKey
	return New(cfg, nil, nil, nil)
}

func TestHealth_PublicAccessible(t *testing.T) {
	s := newTestServer(t, "secretkey0123456789abcdef0123456789")

	req := httptest.NewRequest(http.MethodGet, "/api/v1/health", nil)
	rr := httptest.NewRecorder()
	s.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("status = %d; want 200", rr.Code)
	}
}

func TestProtected_RejectsMissingKey(t *testing.T) {
	s := newTestServer(t, "secretkey0123456789abcdef0123456789")

	req := httptest.NewRequest(http.MethodGet, "/api/v1/whoami", nil)
	rr := httptest.NewRecorder()
	s.ServeHTTP(rr, req)

	if rr.Code != http.StatusUnauthorized {
		t.Errorf("status = %d; want 401", rr.Code)
	}
}

func TestProtected_RejectsWrongKey(t *testing.T) {
	s := newTestServer(t, "expected-key")

	req := httptest.NewRequest(http.MethodGet, "/api/v1/whoami", nil)
	req.Header.Set("X-Api-Key", "wrong-key")
	rr := httptest.NewRecorder()
	s.ServeHTTP(rr, req)

	if rr.Code != http.StatusUnauthorized {
		t.Errorf("status = %d; want 401", rr.Code)
	}
}

func TestProtected_AcceptsHeaderKey(t *testing.T) {
	const key = "good-key-12345"
	s := newTestServer(t, key)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/whoami", nil)
	req.Header.Set("X-Api-Key", key)
	rr := httptest.NewRecorder()
	s.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("status = %d; want 200", rr.Code)
	}
}

func TestProtected_AcceptsQueryKey(t *testing.T) {
	const key = "good-key-12345"
	s := newTestServer(t, key)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/whoami?apikey="+key, nil)
	rr := httptest.NewRecorder()
	s.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("status = %d; want 200", rr.Code)
	}
}

func TestConstantTimeStringEq(t *testing.T) {
	cases := []struct {
		provided string
		expected string
		want     bool
	}{
		{"", "", false},                    // empty rejected
		{"abc", "", false},                 // empty expected rejected
		{"abc", "abc", true},               // match
		{"abc", "abcd", false},             // length mismatch
		{"abc", "abd", false},              // value mismatch
		{"abcdef0123", "abcdef0123", true}, // long match
	}
	for _, c := range cases {
		got := constantTimeStringEq(c.provided, c.expected)
		if got != c.want {
			t.Errorf("constantTimeStringEq(%q, %q) = %v; want %v", c.provided, c.expected, got, c.want)
		}
	}
}
