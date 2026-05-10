package nntp

// NNTP cassettes — record and replay post-TLS NNTP traffic.
//
// Used by the integration test to:
//   - Record a real session against a live provider once
//     (HOARDARR_TEST_RECORD=1).
//   - Replay it deterministically forever after, with no creds
//     required (HOARDARR_TEST_CASSETTE=path).
//
// Wire fidelity: the cassette captures cleartext NNTP bytes after
// TLS termination. Each Read or Write on the underlying conn becomes
// one entry. On replay, client writes are matched against recorded
// writes (with AUTHINFO PASS argument treated as a wildcard); server
// responses are streamed back as the client reads.
//
// Sequence model: strict in-order. Replay assumes one logical session
// (single connection). Tests that need parallel connections should
// use the in-process scripted stub (stub_test.go) instead.

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/jaenster/hoardarr/internal/domain/server"
)

// CassetteEntry is one direction-tagged byte chunk. Direction is "C"
// for client-to-server (writes) or "S" for server-to-client (reads).
//
// Bytes is base64-encoded so JSON can carry arbitrary binary payloads
// (yEnc-encoded article bodies are mostly ASCII but can contain any
// byte 0x00..0xFF).
type CassetteEntry struct {
	Dir   string `json:"dir"`
	Bytes string `json:"b"`
}

// --- recording dialer -----------------------------------------------

// RecordingDialer wraps an inner Dialer (typically DefaultDialer) and
// tees every Read/Write on the resulting net.Conn into a JSONL file.
//
// AUTHINFO PASS lines have their argument replaced with "<REDACTED>"
// before being recorded so cassettes are safe to commit.
//
// Writes are buffered and flushed on Conn.Close.
type RecordingDialer struct {
	Inner Dialer
	Path  string
}

// Dial wraps Inner.Dial with a recording net.Conn.
func (r *RecordingDialer) Dial(ctx context.Context, s *server.UsenetServer) (net.Conn, error) {
	conn, err := r.Inner.Dial(ctx, s)
	if err != nil {
		return nil, err
	}
	rec := &recordingConn{Conn: conn, path: r.Path}
	return rec, nil
}

type recordingConn struct {
	net.Conn
	path    string
	mu      sync.Mutex
	entries []CassetteEntry
	closed  bool
}

// Write tees client→server bytes into the cassette and forwards.
func (r *recordingConn) Write(p []byte) (int, error) {
	r.mu.Lock()
	r.entries = append(r.entries, CassetteEntry{
		Dir:   "C",
		Bytes: base64.StdEncoding.EncodeToString(redactClientBytes(p)),
	})
	r.mu.Unlock()
	return r.Conn.Write(p)
}

// Read tees server→client bytes after they're received.
func (r *recordingConn) Read(p []byte) (int, error) {
	n, err := r.Conn.Read(p)
	if n > 0 {
		r.mu.Lock()
		r.entries = append(r.entries, CassetteEntry{
			Dir:   "S",
			Bytes: base64.StdEncoding.EncodeToString(append([]byte(nil), p[:n]...)),
		})
		r.mu.Unlock()
	}
	return n, err
}

// Close flushes the recorded entries to disk before closing the conn.
func (r *recordingConn) Close() error {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return r.Conn.Close()
	}
	r.closed = true
	entries := r.entries
	r.mu.Unlock()

	if writeErr := writeCassette(r.path, entries); writeErr != nil {
		_ = r.Conn.Close()
		return fmt.Errorf("write cassette %q: %w", r.path, writeErr)
	}
	return r.Conn.Close()
}

// redactClientBytes scans for "AUTHINFO PASS " at line starts and
// replaces the password with "<REDACTED>". Idempotent on bytes that
// don't contain the marker.
func redactClientBytes(p []byte) []byte {
	const marker = "AUTHINFO PASS "
	idx := bytes.Index(p, []byte(marker))
	if idx < 0 {
		return p
	}
	// Find end of line (CR or LF).
	tail := p[idx+len(marker):]
	end := bytes.IndexAny(tail, "\r\n")
	if end < 0 {
		end = len(tail)
	}
	out := make([]byte, 0, len(p)+12)
	out = append(out, p[:idx+len(marker)]...)
	out = append(out, []byte("<REDACTED>")...)
	out = append(out, tail[end:]...)
	return out
}

func writeCassette(path string, entries []CassetteEntry) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	w := bufio.NewWriter(f)
	enc := json.NewEncoder(w)
	for _, e := range entries {
		if err := enc.Encode(e); err != nil {
			return err
		}
	}
	return w.Flush()
}

// --- replay dialer --------------------------------------------------

// ReplayDialer hands out synthetic net.Conns whose Read/Write are
// driven by a recorded cassette. Useful for CI: deterministic protocol
// validation without provider creds.
//
// The same cassette is replayed for every Dial call. Tests that
// exercise multiple parallel connections need a different fixture.
type ReplayDialer struct {
	Path string
}

// Dial returns a synthetic net.Conn whose Read/Write methods consume
// from the cassette in strict order.
func (r *ReplayDialer) Dial(_ context.Context, _ *server.UsenetServer) (net.Conn, error) {
	entries, err := loadCassette(r.Path)
	if err != nil {
		return nil, fmt.Errorf("load cassette %q: %w", r.Path, err)
	}
	return newReplayConn(entries), nil
}

func loadCassette(path string) ([]CassetteEntry, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	dec := json.NewDecoder(f)
	var out []CassetteEntry
	for {
		var e CassetteEntry
		if err := dec.Decode(&e); err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, nil
}

// replayConn is an in-memory net.Conn that streams server data on
// Read and validates client writes against the cassette.
type replayConn struct {
	mu sync.Mutex

	entries []CassetteEntry
	cursor  int

	// pendingServerBytes accumulates bytes from "S" entries until they
	// are read by the caller. Read drains here; when empty, advances
	// the cursor across "S" entries until the next "C" or end.
	pendingServerBytes []byte

	closed bool
}

func newReplayConn(entries []CassetteEntry) *replayConn {
	c := &replayConn{entries: entries}
	c.fillServerBuffer()
	return c
}

// fillServerBuffer pulls all consecutive "S" entries from the current
// cursor into pendingServerBytes, leaving the cursor at the next "C"
// (or end).
func (c *replayConn) fillServerBuffer() {
	for c.cursor < len(c.entries) && c.entries[c.cursor].Dir == "S" {
		b, err := base64.StdEncoding.DecodeString(c.entries[c.cursor].Bytes)
		if err == nil {
			c.pendingServerBytes = append(c.pendingServerBytes, b...)
		}
		c.cursor++
	}
}

func (c *replayConn) Read(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return 0, io.ErrClosedPipe
	}
	if len(c.pendingServerBytes) == 0 {
		// Either at end or next entry is a client write — return EOF
		// so the client knows the cassette has nothing more.
		if c.cursor >= len(c.entries) {
			return 0, io.EOF
		}
		// Next entry is a client write; we have no more server bytes
		// until the client writes. Block? For replay simplicity,
		// return EOF — the test should be structured so reads always
		// follow corresponding writes.
		return 0, io.EOF
	}
	n := copy(p, c.pendingServerBytes)
	c.pendingServerBytes = c.pendingServerBytes[n:]
	return n, nil
}

func (c *replayConn) Write(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return 0, io.ErrClosedPipe
	}
	// Validate the client's bytes match the next "C" entry. Tolerate
	// the AUTHINFO PASS argument being substituted with the real
	// password (cassettes redact; client doesn't).
	if c.cursor >= len(c.entries) {
		return 0, fmt.Errorf("replay: client wrote past end of cassette: %q", string(p))
	}
	e := c.entries[c.cursor]
	if e.Dir != "C" {
		return 0, fmt.Errorf("replay: client wrote when cassette expects server (%q): cursor=%d", string(p), c.cursor)
	}
	expected, err := base64.StdEncoding.DecodeString(e.Bytes)
	if err != nil {
		return 0, fmt.Errorf("replay: bad base64 in cassette entry %d: %w", c.cursor, err)
	}
	if !replayBytesEqual(p, expected) {
		return 0, fmt.Errorf("replay: client wrote %q; cassette expected %q", string(p), string(expected))
	}
	c.cursor++
	c.fillServerBuffer()
	return len(p), nil
}

func (c *replayConn) Close() error {
	c.mu.Lock()
	c.closed = true
	c.mu.Unlock()
	return nil
}

// replayBytesEqual compares two byte slices, treating any
// "AUTHINFO PASS " line in expected with "<REDACTED>" as matching the
// same prefix in actual followed by any password.
func replayBytesEqual(actual, expected []byte) bool {
	const marker = "AUTHINFO PASS "
	idxE := bytes.Index(expected, []byte(marker))
	if idxE < 0 {
		return bytes.Equal(actual, expected)
	}
	// Try wildcard match. Find redacted token end.
	const redacted = "<REDACTED>"
	rIdx := bytes.Index(expected[idxE+len(marker):], []byte(redacted))
	if rIdx < 0 {
		// Cassette wasn't redacted (legacy?); fall back to literal.
		return bytes.Equal(actual, expected)
	}
	prefixEnd := idxE + len(marker)
	suffixStart := prefixEnd + rIdx + len(redacted)
	suffix := expected[suffixStart:]

	if !bytes.HasPrefix(actual, expected[:prefixEnd]) {
		return false
	}
	if !bytes.HasSuffix(actual, suffix) {
		return false
	}
	// Anything in between is the password — accepted.
	return true
}

// Stub net.Addr / deadline methods so replayConn satisfies net.Conn.

type fakeAddr struct{ name string }

func (a fakeAddr) Network() string { return "cassette" }
func (a fakeAddr) String() string  { return a.name }

func (c *replayConn) LocalAddr() net.Addr                { return fakeAddr{"replay-local"} }
func (c *replayConn) RemoteAddr() net.Addr               { return fakeAddr{"replay-remote"} }
func (c *replayConn) SetDeadline(_ time.Time) error      { return nil }
func (c *replayConn) SetReadDeadline(_ time.Time) error  { return nil }
func (c *replayConn) SetWriteDeadline(_ time.Time) error { return nil }

// silence: prevent strings package being unused for future helpers.
var _ = strings.HasPrefix
var _ = errors.New
