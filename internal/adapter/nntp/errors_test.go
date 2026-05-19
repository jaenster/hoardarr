package nntp

import (
	"errors"
	"testing"
)

// TestClassifyResponse_TooManyConnections asserts that provider
// "too many connections" responses on any 4xx/5xx code map to
// ErrTooManyConnections, not ErrAuthFailed. Misclassification used to
// burn the segment retry budget on real-provider over-capacity
// (Eweka returns "502 too many connections" mid-session; others use
// 481/400 with similar text).
func TestClassifyResponse_TooManyConnections(t *testing.T) {
	cases := []struct {
		name string
		pe   *ProtocolError
	}{
		{"502 Eweka style", &ProtocolError{Code: 502, Message: "Too many connections"}},
		{"481 mid-session", &ProtocolError{Code: 481, Message: "too many connections from your IP"}},
		{"400 with limit text", &ProtocolError{Code: 400, Message: "connection limit reached"}},
		{"503 max-connections style", &ProtocolError{Code: 503, Message: "max connections exceeded"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := classifyResponse(tc.pe)
			if !errors.Is(got, ErrTooManyConnections) {
				t.Errorf("got %v, expected ErrTooManyConnections", got)
			}
			if errors.Is(got, ErrAuthFailed) {
				t.Errorf("got %v, was misclassified as ErrAuthFailed", got)
			}
		})
	}
}

// TestClassifyResponse_AuthFailedKept asserts that real auth-failure
// responses (no "too many" text) still classify as ErrAuthFailed.
func TestClassifyResponse_AuthFailedKept(t *testing.T) {
	pe := &ProtocolError{Code: 481, Message: "authentication rejected"}
	got := classifyResponse(pe)
	if !errors.Is(got, ErrAuthFailed) {
		t.Errorf("got %v, expected ErrAuthFailed", got)
	}
	if errors.Is(got, ErrTooManyConnections) {
		t.Errorf("got %v, was misclassified as ErrTooManyConnections", got)
	}
}

// TestClassifyResponse_ArticleMissing covers the existing 430 path.
func TestClassifyResponse_ArticleMissing(t *testing.T) {
	pe := &ProtocolError{Code: 430, Message: "no such article"}
	got := classifyResponse(pe)
	if !errors.Is(got, ErrArticleMissing) {
		t.Errorf("got %v, expected ErrArticleMissing", got)
	}
}
