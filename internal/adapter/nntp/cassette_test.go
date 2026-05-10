package nntp

import (
	"context"
	"encoding/base64"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestCassette_RecordReplayRoundTrip drives a full session against the
// scripted stub, records to a cassette, then verifies the cassette
// replays the same session against ReplayDialer without any backing
// network.
func TestCassette_RecordReplayRoundTrip(t *testing.T) {
	const body = "Hello cassette\r\nyEnc bytes here"
	scripted := func(s *stubSession) {
		s.Send("200 ready")
		s.ExpectLine("AUTHINFO USER user")
		s.Send("381 password required")
		s.ExpectLine("AUTHINFO PASS pass")
		s.Send("281 authenticated")
		s.ExpectLine("BODY <msg@host>")
		s.Send("222 0 <msg@host>")
		s.SendMulti(body)
		s.ExpectLine("QUIT")
		s.Send("205 closing")
	}

	addr, cleanup := startStubServer(t, scripted)
	defer cleanup()

	cassettePath := filepath.Join(t.TempDir(), "round-trip.jsonl")

	// --- record phase ---
	{
		dialer := &RecordingDialer{
			Inner: DefaultDialer,
			Path:  cassettePath,
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		c, err := Dial(ctx, stubUsenetServer(t, addr), WithDialer(dialer))
		if err != nil {
			t.Fatalf("record Dial: %v", err)
		}
		if err := c.Authenticate(ctx); err != nil {
			t.Fatalf("record Authenticate: %v", err)
		}
		rc, err := c.Body(ctx, "msg@host")
		if err != nil {
			t.Fatalf("record Body: %v", err)
		}
		got, _ := io.ReadAll(rc)
		_ = rc.Close()
		want := strings.ReplaceAll(body, "\r\n", "\n") + "\n"
		if string(got) != want {
			t.Errorf("record body = %q; want %q", got, want)
		}
		if err := c.Quit(ctx); err != nil {
			t.Errorf("record Quit: %v", err)
		}
	}

	// Cassette should exist and contain redacted AUTHINFO PASS.
	raw, err := os.ReadFile(cassettePath)
	if err != nil {
		t.Fatalf("read cassette: %v", err)
	}
	if !strings.Contains(string(raw), "C") {
		t.Errorf("cassette missing client entries")
	}
	// Decode entries to check redaction.
	entries, err := loadCassette(cassettePath)
	if err != nil {
		t.Fatalf("loadCassette: %v", err)
	}
	foundRedacted := false
	for _, e := range entries {
		decoded := mustDecodeBase64(t, e.Bytes)
		if strings.Contains(decoded, "AUTHINFO PASS pass") {
			t.Errorf("cassette leaked password: %q", decoded)
		}
		if strings.Contains(decoded, "AUTHINFO PASS <REDACTED>") {
			foundRedacted = true
		}
	}
	if !foundRedacted {
		t.Errorf("cassette did not redact AUTHINFO PASS")
	}

	// --- replay phase --- (no stub server backing this!)
	{
		replay := &ReplayDialer{Path: cassettePath}
		// Use a fresh server with the SAME password so the redaction
		// match works (cassette stores <REDACTED>, replay accepts any
		// password value at that position).
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		c, err := Dial(ctx, stubUsenetServer(t, "127.0.0.1:1"), WithDialer(replay))
		if err != nil {
			t.Fatalf("replay Dial: %v", err)
		}
		if err := c.Authenticate(ctx); err != nil {
			t.Fatalf("replay Authenticate: %v", err)
		}
		rc, err := c.Body(ctx, "msg@host")
		if err != nil {
			t.Fatalf("replay Body: %v", err)
		}
		got, _ := io.ReadAll(rc)
		_ = rc.Close()
		want := strings.ReplaceAll(body, "\r\n", "\n") + "\n"
		if string(got) != want {
			t.Errorf("replay body = %q; want %q", got, want)
		}
		if err := c.Quit(ctx); err != nil {
			t.Errorf("replay Quit: %v", err)
		}
	}
}

// TestCassette_ReplayDetectsClientDrift ensures that if the client
// sends something different on replay than what was recorded, we
// fail loudly (not silently use stale data).
func TestCassette_ReplayDetectsClientDrift(t *testing.T) {
	addr, cleanup := startStubServer(t, func(s *stubSession) {
		s.Send("200 ready")
		s.ExpectLine("AUTHINFO USER user")
		s.Send("381 password required")
		s.ExpectLine("AUTHINFO PASS pass")
		s.Send("281 authenticated")
		s.ExpectLine("QUIT")
		s.Send("205 closing")
	})
	defer cleanup()

	cassettePath := filepath.Join(t.TempDir(), "drift.jsonl")

	// Record an authenticate-then-quit session.
	{
		dialer := &RecordingDialer{Inner: DefaultDialer, Path: cassettePath}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		c, err := Dial(ctx, stubUsenetServer(t, addr), WithDialer(dialer))
		if err != nil {
			t.Fatalf("Dial: %v", err)
		}
		if err := c.Authenticate(ctx); err != nil {
			t.Fatalf("Authenticate: %v", err)
		}
		_ = c.Quit(ctx)
	}

	// Replay but call DATE instead of QUIT — should error.
	replay := &ReplayDialer{Path: cassettePath}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	c, err := Dial(ctx, stubUsenetServer(t, "127.0.0.1:1"), WithDialer(replay))
	if err != nil {
		t.Fatalf("replay Dial: %v", err)
	}
	if err := c.Authenticate(ctx); err != nil {
		t.Fatalf("replay Authenticate: %v", err)
	}
	if _, err := c.Date(ctx); err == nil {
		t.Error("replay accepted DATE that wasn't in the cassette")
	}
}

// TestRedactClientBytes covers the AUTHINFO PASS scrubber.
func TestRedactClientBytes(t *testing.T) {
	cases := []struct {
		in, want string
	}{
		{"AUTHINFO USER alice\r\n", "AUTHINFO USER alice\r\n"},
		{"AUTHINFO PASS hunter2\r\n", "AUTHINFO PASS <REDACTED>\r\n"},
		{"AUTHINFO PASS s3cr3t!\r\n", "AUTHINFO PASS <REDACTED>\r\n"},
		{"BODY <msg@h>\r\n", "BODY <msg@h>\r\n"},
		// No trailing CRLF — still redacted.
		{"AUTHINFO PASS x", "AUTHINFO PASS <REDACTED>"},
	}
	for _, c := range cases {
		got := string(redactClientBytes([]byte(c.in)))
		if got != c.want {
			t.Errorf("redactClientBytes(%q) = %q; want %q", c.in, got, c.want)
		}
	}
}

func mustDecodeBase64(t *testing.T, s string) string {
	t.Helper()
	b, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		t.Fatalf("decode base64: %v", err)
	}
	return string(b)
}
