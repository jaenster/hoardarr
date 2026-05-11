package nntp_test

// Driving the testserver against the real hoardarr nntp client +
// yEnc decoder. If these two layers can roundtrip a body via the fake
// server, downstream tests are safe to assume it.

import (
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/jaenster/hoardarr/internal/adapter/nntp"
	"github.com/jaenster/hoardarr/internal/adapter/yenc"
	"github.com/jaenster/hoardarr/internal/domain/server"
	testnntp "github.com/jaenster/hoardarr/internal/testserver/nntp"
)

func TestTestServer_HappyPath(t *testing.T) {
	s, err := testnntp.Start(testnntp.Options{})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer s.Stop()

	const msgID = "abc123@host"
	payload := []byte(strings.Repeat("hello hoardarr ", 64))
	s.AddArticle(msgID, testnntp.EncodeArticle("hello.bin", payload))

	got := fetchAndDecode(t, s, msgID, "", "")
	if string(got) != string(payload) {
		t.Fatalf("decoded payload mismatch:\n got: %q\n want: %q", got, payload)
	}
}

func TestTestServer_Auth(t *testing.T) {
	s, err := testnntp.Start(testnntp.Options{
		Username: "alice",
		Password: "letmein",
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer s.Stop()

	const msgID = "auth@host"
	payload := []byte("authed payload")
	s.AddArticle(msgID, testnntp.EncodeArticle("auth.bin", payload))

	// Wrong creds → auth fails before any article fetch.
	got := tryFetch(t, s, msgID, "alice", "wrong")
	if got != nil {
		t.Errorf("expected failure with bad password; got %q", got)
	}
	// Right creds → succeed.
	got = fetchAndDecode(t, s, msgID, "alice", "letmein")
	if string(got) != string(payload) {
		t.Errorf("auth happy path mismatch: %q vs %q", got, payload)
	}
}

func TestTestServer_MissingArticle(t *testing.T) {
	s, err := testnntp.Start(testnntp.Options{})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer s.Stop()
	// We never AddArticle, so any fetch returns 430.
	got := tryFetch(t, s, "nope@host", "", "")
	if got != nil {
		t.Errorf("expected 430 / nil body; got %q", got)
	}
}

func TestTestServer_BandwidthCap(t *testing.T) {
	// 64 KiB at ~32 KiB/s should take ~2s. Use a slightly larger
	// payload + tighter cap to make the timing assertion robust.
	const payloadSize = 64 * 1024
	const bps = 32 * 1024
	s, err := testnntp.Start(testnntp.Options{
		BytesPerSec: bps,
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer s.Stop()

	const msgID = "slow@host"
	payload := make([]byte, payloadSize)
	for i := range payload {
		payload[i] = byte(i)
	}
	s.AddArticle(msgID, testnntp.EncodeArticle("slow.bin", payload))

	t0 := time.Now()
	got := fetchAndDecode(t, s, msgID, "", "")
	elapsed := time.Since(t0)
	if len(got) != payloadSize {
		t.Errorf("got %d bytes; want %d", len(got), payloadSize)
	}
	// Lower bound: yEnc adds overhead and we have a ~64 KiB chunk
	// boundary inside the throttle loop, so expect at least ~1.5s.
	if elapsed < 1500*time.Millisecond {
		t.Errorf("throttle too fast: elapsed=%v want>=1.5s", elapsed)
	}
}

func fetchAndDecode(t *testing.T, s *testnntp.Server, msgID, user, pass string) []byte {
	t.Helper()
	srv := server.Hydrate(server.HydrateParams{
		ID: 0, Name: "probe", Host: s.Host(), Port: s.Port(), TLS: false,
		Username: user, Password: pass, MaxConns: 1, Enabled: true,
		AddedAt: time.Now().UTC(),
	})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := nntp.Dial(ctx, srv)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer conn.Close()
	if user != "" {
		if err := conn.Authenticate(ctx); err != nil {
			t.Fatalf("Authenticate: %v", err)
		}
	}
	if err := conn.ModeReader(ctx); err != nil {
		t.Fatalf("ModeReader: %v", err)
	}
	body, err := conn.Body(ctx, msgID)
	if err != nil {
		t.Fatalf("Body: %v", err)
	}
	defer body.Close()
	decoded, _, _, err := yenc.Decode(body)
	if err != nil {
		t.Fatalf("yenc.Decode: %v", err)
	}
	return decoded
}

// tryFetch is fetchAndDecode that returns nil on any failure instead
// of fatal-ing. Used to assert negative cases.
func tryFetch(t *testing.T, s *testnntp.Server, msgID, user, pass string) []byte {
	t.Helper()
	srv := server.Hydrate(server.HydrateParams{
		ID: 0, Name: "probe", Host: s.Host(), Port: s.Port(), TLS: false,
		Username: user, Password: pass, MaxConns: 1, Enabled: true,
		AddedAt: time.Now().UTC(),
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := nntp.Dial(ctx, srv)
	if err != nil {
		return nil
	}
	defer conn.Close()
	if user != "" {
		if err := conn.Authenticate(ctx); err != nil {
			return nil
		}
	}
	if err := conn.ModeReader(ctx); err != nil {
		return nil
	}
	body, err := conn.Body(ctx, msgID)
	if err != nil {
		return nil
	}
	defer body.Close()
	decoded, _, _, err := yenc.Decode(body)
	if err != nil {
		// Drain to keep the conn alive for the next call (if any).
		_, _ = io.Copy(io.Discard, body)
		return nil
	}
	return decoded
}
