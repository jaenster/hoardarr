// Package nntp is hoardarr's from-scratch NNTP (RFC 3977) client.
//
// Scope: just enough to drive Usenet binary downloads. We do not
// support posting, the ALL groups commands, IHAVE/CHECK/TAKETHIS, or
// the streaming extensions used by peers. The supported command set:
//
//	AUTHINFO USER / AUTHINFO PASS — provider authentication
//	MODE READER                   — some servers require this
//	GROUP                         — informational; rarely needed when
//	                                fetching by Message-ID
//	BODY <message-id>             — the workhorse
//	ARTICLE <message-id>          — used when we want headers too
//	STAT <message-id>             — existence check
//	DATE                          — health check (cheap, no body)
//	QUIT                          — graceful close
//
// TLS: by default Dial wraps the connection in crypto/tls. The vast
// majority of public providers run TLS on 563 (or 443/8443/etc); we
// honour the UsenetServer.TLS flag.
//
// Concurrency: Conn is NOT safe for concurrent use. The pool gives
// each goroutine its own conn for the duration of an article fetch.
package nntp

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/textproto"
	"strconv"
	"strings"
	"time"

	"github.com/jaenster/hoardarr/internal/domain/server"
)

// Conn is a single NNTP connection. Construct via Dial, drive via the
// command methods, release with Close.
type Conn struct {
	server   *server.UsenetServer
	netConn  net.Conn
	tp       *textproto.Conn
	lastUsed time.Time
	authed   bool
	closed   bool
}

// Dial opens an NNTP connection to s and reads the greeting. TLS is
// negotiated when s.TLS() is true. On failure, the underlying socket
// is closed before returning.
//
// The dial respects ctx for both the TCP connect and the TLS handshake.
func Dial(ctx context.Context, s *server.UsenetServer) (*Conn, error) {
	addr := net.JoinHostPort(s.Host(), strconv.Itoa(s.Port()))

	d := &net.Dialer{}
	rawConn, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", addr, err)
	}

	netConn := rawConn
	if s.TLS() {
		tlsConn := tls.Client(rawConn, &tls.Config{
			ServerName: s.Host(),
			MinVersion: tls.VersionTLS12,
		})
		// Honour ctx for the handshake.
		if dl, ok := ctx.Deadline(); ok {
			_ = tlsConn.SetDeadline(dl)
		}
		if err := tlsConn.HandshakeContext(ctx); err != nil {
			_ = rawConn.Close()
			return nil, fmt.Errorf("tls handshake: %w", err)
		}
		_ = tlsConn.SetDeadline(time.Time{})
		netConn = tlsConn
	}

	tp := textproto.NewConn(netConn)
	c := &Conn{
		server:   s,
		netConn:  netConn,
		tp:       tp,
		lastUsed: time.Now(),
	}

	// Read greeting (200 = service available, 201 = no posting).
	code, msg, err := readCodeLineCtx(ctx, c, 0)
	if err != nil {
		_ = c.Close()
		return nil, fmt.Errorf("read greeting: %w", err)
	}
	if code != 200 && code != 201 {
		_ = c.Close()
		return nil, fmt.Errorf("%w: %d %s", ErrUnexpectedGreeting, code, msg)
	}
	return c, nil
}

// Server returns the UsenetServer this connection was opened against.
func (c *Conn) Server() *server.UsenetServer { return c.server }

// LastUsed reports the timestamp of the most recent successful command.
// Used by the pool to prune idle connections.
func (c *Conn) LastUsed() time.Time { return c.lastUsed }

// Close terminates the connection. Idempotent.
func (c *Conn) Close() error {
	if c.closed {
		return nil
	}
	c.closed = true
	return c.tp.Close()
}

// Authenticate runs AUTHINFO USER / AUTHINFO PASS. Skipped if the
// server.Username is empty (some private servers permit anonymous
// access for the groups they care about).
func (c *Conn) Authenticate(ctx context.Context) error {
	if c.authed {
		return nil
	}
	if c.server.Username() == "" {
		c.authed = true
		return nil
	}

	if err := c.send(ctx, "AUTHINFO USER "+c.server.Username()); err != nil {
		return err
	}
	code, msg, err := readCodeLineCtx(ctx, c, 0)
	if err != nil {
		return fmt.Errorf("authinfo user: %w", err)
	}
	if code == 281 {
		c.authed = true
		c.touch()
		return nil
	}
	if code != 381 {
		return classifyResponse(&ProtocolError{Code: code, Message: msg})
	}

	if err := c.send(ctx, "AUTHINFO PASS "+c.server.Password()); err != nil {
		return err
	}
	code, msg, err = readCodeLineCtx(ctx, c, 0)
	if err != nil {
		return fmt.Errorf("authinfo pass: %w", err)
	}
	if code != 281 {
		return classifyResponse(&ProtocolError{Code: code, Message: msg})
	}
	c.authed = true
	c.touch()
	return nil
}

// ModeReader sends "MODE READER". Some transit-mode NNTP servers need
// this before they will serve articles to readers. Many providers don't
// support the command at all and respond with various 5xx codes — in
// every case the right move is to proceed since article-fetch commands
// (BODY/STAT) work regardless. Recognised "ignore me" codes:
//
//	500 — command not recognised
//	501 — syntax error
//	502 — command unavailable / not allowed (some providers)
func (c *Conn) ModeReader(ctx context.Context) error {
	if err := c.send(ctx, "MODE READER"); err != nil {
		return err
	}
	code, msg, err := readCodeLineCtx(ctx, c, 0)
	if err != nil {
		return fmt.Errorf("mode reader: %w", err)
	}
	switch {
	case code == 200 || code == 201:
		c.touch()
		return nil
	case code == 500 || code == 501 || code == 502:
		// Provider doesn't implement / allow MODE READER; harmless.
		c.touch()
		return nil
	default:
		return classifyResponse(&ProtocolError{Code: code, Message: msg})
	}
}

// Date queries the server's current date — the cheapest health-check
// command (no body, immediate response). Used by the pool to validate
// stale conns before checkout.
func (c *Conn) Date(ctx context.Context) (time.Time, error) {
	if err := c.send(ctx, "DATE"); err != nil {
		return time.Time{}, err
	}
	code, msg, err := readCodeLineCtx(ctx, c, 0)
	if err != nil {
		return time.Time{}, fmt.Errorf("date: %w", err)
	}
	if code != 111 {
		return time.Time{}, classifyResponse(&ProtocolError{Code: code, Message: msg})
	}
	t, err := time.Parse("20060102150405", strings.TrimSpace(msg))
	if err != nil {
		return time.Time{}, fmt.Errorf("date parse: %w", err)
	}
	c.touch()
	return t, nil
}

// Body fetches the body of the article identified by messageID (without
// surrounding angle brackets — we add them).
//
// On success, the returned ReadCloser yields the de-dot-stuffed body
// bytes. The caller MUST drain to EOF (or call Close) before issuing
// another command on this Conn — the underlying stream is shared.
//
// Cancellation: while the body is being read, a watcher goroutine
// observes ctx.Done(). If the caller cancels mid-read, the underlying
// connection is closed, which makes the in-progress Read error out
// immediately. Without this, an idle peer can block reads indefinitely
// regardless of ctx.
//
// On 430, ErrArticleMissing is returned and the connection remains
// usable for further commands.
func (c *Conn) Body(ctx context.Context, messageID string) (io.ReadCloser, error) {
	if err := c.send(ctx, "BODY <"+messageID+">"); err != nil {
		return nil, err
	}
	code, msg, err := readCodeLineCtx(ctx, c, 0)
	if err != nil {
		return nil, fmt.Errorf("body status: %w", err)
	}
	if code != 222 {
		return nil, classifyResponse(&ProtocolError{Code: code, Message: msg})
	}
	c.touch()

	br := &bodyReader{r: c.tp.DotReader(), done: make(chan struct{})}
	go bodyWatcher(ctx, c, br)
	return br, nil
}

// bodyWatcher closes the underlying conn when ctx is cancelled,
// unblocking any in-flight Read on the body. Returns when the body
// reader is closed normally.
func bodyWatcher(ctx context.Context, c *Conn, br *bodyReader) {
	select {
	case <-ctx.Done():
		_ = c.netConn.Close()
	case <-br.done:
		return
	}
}

// Stat checks whether messageID exists on this server without
// transferring the body. Useful as a probe; in v0.1 the orchestrator
// just calls Body directly.
func (c *Conn) Stat(ctx context.Context, messageID string) error {
	if err := c.send(ctx, "STAT <"+messageID+">"); err != nil {
		return err
	}
	code, msg, err := readCodeLineCtx(ctx, c, 0)
	if err != nil {
		return fmt.Errorf("stat: %w", err)
	}
	if code != 223 {
		return classifyResponse(&ProtocolError{Code: code, Message: msg})
	}
	c.touch()
	return nil
}

// Quit is graceful close — sends QUIT, reads the 205 response, closes
// the socket. Errors are logged-and-swallowed by the pool; a failed
// Quit just means the conn drops a little less politely.
func (c *Conn) Quit(ctx context.Context) error {
	if c.closed {
		return nil
	}
	if err := c.send(ctx, "QUIT"); err == nil {
		_, _, _ = readCodeLineCtx(ctx, c, 0)
	}
	return c.Close()
}

// send writes one command line and flushes. Honours ctx via the
// underlying conn deadline.
func (c *Conn) send(ctx context.Context, line string) error {
	if dl, ok := ctx.Deadline(); ok {
		_ = c.netConn.SetWriteDeadline(dl)
	}
	defer func() { _ = c.netConn.SetWriteDeadline(time.Time{}) }()
	if _, err := c.tp.Cmd("%s", line); err != nil {
		return fmt.Errorf("send %q: %w", firstWord(line), err)
	}
	return nil
}

func (c *Conn) touch() { c.lastUsed = time.Now() }

// readCodeLineCtx applies ctx's deadline (if any) and watches for
// ctx cancellation, closing the underlying conn if cancelled mid-read.
// Without that, an idle peer can hold the read open indefinitely
// regardless of ctx.
func readCodeLineCtx(ctx context.Context, c *Conn, expectCode int) (int, string, error) {
	if dl, ok := ctx.Deadline(); ok {
		_ = c.netConn.SetReadDeadline(dl)
		defer func() { _ = c.netConn.SetReadDeadline(time.Time{}) }()
	}
	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-ctx.Done():
			_ = c.netConn.Close()
		case <-done:
		}
	}()
	return c.tp.ReadCodeLine(expectCode)
}

// bodyReader wraps the textproto DotReader; closing it drains any
// remaining bytes so the underlying conn stays in sync, and signals
// the cancellation watcher in Body() to exit.
type bodyReader struct {
	r      io.Reader
	closed bool
	// done is closed when Close runs. The watcher goroutine spawned
	// by Body() selects on this to know when to exit without forcing
	// the conn closed.
	done chan struct{}
}

func (b *bodyReader) Read(p []byte) (int, error) { return b.r.Read(p) }

// Close drains the body to EOF. Safe to call after a partial read.
func (b *bodyReader) Close() error {
	if b.closed {
		return nil
	}
	b.closed = true
	_, _ = io.Copy(io.Discard, b.r)
	if b.done != nil {
		close(b.done)
	}
	return nil
}

func firstWord(s string) string {
	if i := strings.IndexByte(s, ' '); i >= 0 {
		return s[:i]
	}
	return s
}
