// Package nntp implements a fake NNTP server suitable for end-to-end
// testing of hoardarr's download pipeline.
//
// The server speaks just enough of RFC 3977 to satisfy the hoardarr
// fetcher: 200/201 greeting, AUTHINFO USER/PASS, MODE READER, DATE,
// GROUP, BODY, ARTICLE, STAT, QUIT. Articles are pre-registered via
// AddArticle; BODY/ARTICLE responses send back the registered payload
// (already yEnc-encoded by EncodeArticle).
//
// The server is built for tests that want a real wire-level round trip
// without a real provider. It is NOT production-grade — no posting,
// no overview, no command pipelining, no XOVER, no streaming feeds.
// Trying to point a real NNTP client at it for browsing will not end
// well.
//
// Knobs for realism:
//   - BytesPerSec throttles BODY/ARTICLE writes so progress bars tick
//     visibly in a browser.
//   - ArticleLatency adds a fixed sleep before each BODY/ARTICLE.
//   - MissingFraction returns 430 for that fraction of articles at
//     random — useful for exercising the retry/missing path.
//   - RequireAuth + Username/Password gate every command (other than
//     AUTHINFO and QUIT) behind a 480 response.
package nntp

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"net"
	"strings"
	"sync"
	"time"
)

// Options configures a Server. Zero values are safe defaults: no auth,
// no throttling, no missing articles.
type Options struct {
	// Listen address. Defaults to "127.0.0.1:0" (random free port).
	Listen string

	// Username + Password for AUTHINFO. When both empty, any
	// credentials are accepted (or none at all). When set, the server
	// rejects connections that fail to authenticate.
	Username string
	Password string

	// BytesPerSec caps the write rate of BODY/ARTICLE responses. 0
	// means no cap (write at native speed).
	BytesPerSec int64

	// ArticleLatency is a fixed sleep before the 222/220 response.
	// Useful for simulating high-latency providers.
	ArticleLatency time.Duration

	// MissingFraction in [0, 1) returns 430 for that fraction of
	// articles, chosen uniformly at random. Deterministic across a
	// single Server instance for a given message-id — once we decide
	// "missing", subsequent requests for that id also return 430.
	MissingFraction float64

	// Logger receives wire-level traces. Defaults to a no-op handler.
	Logger *slog.Logger

	// Seed for the random missing-article decisions. 0 → time-based.
	// Tests that need determinism should pin this.
	Seed int64
}

// Server is a running fake NNTP server.
type Server struct {
	opts     Options
	listener net.Listener
	wg       sync.WaitGroup
	rng      *rand.Rand

	mu       sync.RWMutex
	articles map[string][]byte // msg-id → yEnc-encoded body (no dot-stuffing applied)
	missing  map[string]bool   // msg-id → decision cache

	ctx    context.Context
	cancel context.CancelFunc
}

// Start binds the listener and begins accepting connections. The
// caller must Stop the server when done (typically via t.Cleanup).
func Start(opts Options) (*Server, error) {
	if opts.Listen == "" {
		opts.Listen = "127.0.0.1:0"
	}
	if opts.Logger == nil {
		opts.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	if opts.MissingFraction < 0 {
		opts.MissingFraction = 0
	}
	if opts.MissingFraction >= 1 {
		// 100% missing is just a server that always fails — usable
		// but easy to set by accident; clamp slightly below 1 so the
		// random check still has work to do.
		opts.MissingFraction = 0.999
	}
	l, err := net.Listen("tcp", opts.Listen)
	if err != nil {
		return nil, fmt.Errorf("nntp testserver listen: %w", err)
	}
	seed := opts.Seed
	if seed == 0 {
		seed = time.Now().UnixNano()
	}
	ctx, cancel := context.WithCancel(context.Background())
	s := &Server{
		opts:     opts,
		listener: l,
		rng:      rand.New(rand.NewSource(seed)),
		articles: make(map[string][]byte),
		missing:  make(map[string]bool),
		ctx:      ctx,
		cancel:   cancel,
	}
	s.wg.Add(1)
	go s.acceptLoop()
	return s, nil
}

// Addr returns the listener address (host:port). Useful for pointing
// a hoardarr server at the fake.
func (s *Server) Addr() string {
	return s.listener.Addr().String()
}

// Host + Port split out Addr for convenience.
func (s *Server) Host() string {
	host, _, _ := net.SplitHostPort(s.Addr())
	return host
}

func (s *Server) Port() int {
	_, portStr, _ := net.SplitHostPort(s.Addr())
	var p int
	_, _ = fmt.Sscanf(portStr, "%d", &p)
	return p
}

// SetBytesPerSec adjusts the throttle live. New value takes effect on
// the next BODY/ARTICLE write; in-flight writes finish at the old
// rate. Pass 0 to remove the cap.
func (s *Server) SetBytesPerSec(bps int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.opts.BytesPerSec = bps
}

// SetArticleLatency adjusts the per-article delay.
func (s *Server) SetArticleLatency(d time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.opts.ArticleLatency = d
}

// SetMissingFraction adjusts the random-drop probability. Note: any
// cached decisions from previous requests survive (so retries see
// consistent behaviour). Use Reset to wipe the cache.
func (s *Server) SetMissingFraction(f float64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if f < 0 {
		f = 0
	}
	if f >= 1 {
		f = 0.999
	}
	s.opts.MissingFraction = f
}

// Reset clears registered articles + missing-article decisions.
// Connection state and options are preserved.
func (s *Server) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.articles = make(map[string][]byte)
	s.missing = make(map[string]bool)
}

// Stop closes the listener and drains any in-flight connections.
func (s *Server) Stop() {
	s.cancel()
	_ = s.listener.Close()
	s.wg.Wait()
}

// AddArticle registers a yEnc-encoded article body (the bytes between
// the "222 <ok>\r\n" line and the terminating ".\r\n"). The caller is
// responsible for encoding — use EncodeArticle to produce body bytes
// from a raw payload.
//
// msgID is the bare message-id, no angle brackets.
func (s *Server) AddArticle(msgID string, body []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.articles[msgID] = append([]byte(nil), body...)
}

// Articles returns the current set of registered message-ids. Useful
// for assertions in tests.
func (s *Server) Articles() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]string, 0, len(s.articles))
	for id := range s.articles {
		out = append(out, id)
	}
	return out
}

func (s *Server) acceptLoop() {
	defer s.wg.Done()
	for {
		c, err := s.listener.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) || s.ctx.Err() != nil {
				return
			}
			s.opts.Logger.Warn("testserver accept", "err", err)
			continue
		}
		s.wg.Add(1)
		go func(c net.Conn) {
			defer s.wg.Done()
			s.handle(c)
		}(c)
	}
}

// shouldDropArticle returns the cached or freshly-rolled decision for
// whether to fail this msg-id with 430. Once a decision is made we
// stick with it so retries see consistent behaviour.
func (s *Server) shouldDropArticle(msgID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.opts.MissingFraction <= 0 {
		return false
	}
	if v, ok := s.missing[msgID]; ok {
		return v
	}
	v := s.rng.Float64() < s.opts.MissingFraction
	s.missing[msgID] = v
	return v
}

func (s *Server) lookup(msgID string) ([]byte, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	body, ok := s.articles[msgID]
	return body, ok
}

// throttledWriter wraps a writer with a leaky-bucket-ish rate cap.
// We use sleep-on-write rather than a token bucket to keep the
// implementation trivial — testserver byte rates are at the MB/s
// range, not nanosecond-tight.
type throttledWriter struct {
	w    io.Writer
	bps  int64 // bytes/sec; 0 = no cap
	last time.Time
}

func (t *throttledWriter) Write(p []byte) (int, error) {
	if t.bps <= 0 {
		return t.w.Write(p)
	}
	// Write in ~64 KiB chunks so the throttle is smooth.
	const chunk = 64 * 1024
	total := 0
	for len(p) > 0 {
		n := len(p)
		if n > chunk {
			n = chunk
		}
		w, err := t.w.Write(p[:n])
		total += w
		if err != nil {
			return total, err
		}
		p = p[n:]
		// Sleep proportional to bytes written.
		d := time.Duration(int64(w) * int64(time.Second) / t.bps)
		time.Sleep(d)
	}
	return total, nil
}

type session struct {
	srv   *Server
	c     net.Conn
	br    *bufio.Reader
	w     io.Writer
	authed bool
	user   string
}

func (s *Server) handle(c net.Conn) {
	defer c.Close()
	sess := &session{
		srv: s,
		c:   c,
		br:  bufio.NewReader(c),
		w:   c,
	}
	// 200 = posting allowed (not that we offer it, but it's the more
	// common greeting and clients are happier with it).
	sess.writeLine("200 hoardarr fake nntp ready")
	for {
		if s.ctx.Err() != nil {
			return
		}
		line, err := sess.br.ReadString('\n')
		if err != nil {
			return
		}
		cmd := strings.TrimRight(line, "\r\n")
		if cmd == "" {
			continue
		}
		if !sess.dispatch(cmd) {
			return
		}
	}
}

func (s *session) dispatch(cmd string) bool {
	upper := strings.ToUpper(cmd)
	s.srv.opts.Logger.Debug("testserver cmd", "cmd", cmd)

	switch {
	case strings.HasPrefix(upper, "AUTHINFO USER"):
		s.user = strings.TrimSpace(cmd[len("AUTHINFO USER"):])
		s.writeLine("381 enter password")
	case strings.HasPrefix(upper, "AUTHINFO PASS"):
		pass := strings.TrimSpace(cmd[len("AUTHINFO PASS"):])
		if s.srv.opts.Username != "" || s.srv.opts.Password != "" {
			if s.user == s.srv.opts.Username && pass == s.srv.opts.Password {
				s.authed = true
				s.writeLine("281 authentication accepted")
			} else {
				s.writeLine("481 authentication failed")
			}
		} else {
			s.authed = true
			s.writeLine("281 authentication accepted")
		}
	case upper == "MODE READER":
		s.writeLine("200 reader mode")
	case upper == "DATE":
		now := time.Now().UTC()
		s.writeLine(fmt.Sprintf("111 %s", now.Format("20060102150405")))
	case strings.HasPrefix(upper, "GROUP"):
		// Pretend any group exists with one article; the fetcher
		// only cares that the command succeeds.
		s.writeLine("211 1 1 1 misc.test")
	case strings.HasPrefix(upper, "BODY") || strings.HasPrefix(upper, "ARTICLE"):
		s.serveArticle(cmd, upper)
	case strings.HasPrefix(upper, "STAT"):
		msgID := extractMessageID(cmd)
		if _, ok := s.srv.lookup(msgID); ok && !s.srv.shouldDropArticle(msgID) {
			s.writeLine(fmt.Sprintf("223 0 <%s>", msgID))
		} else {
			s.writeLine(fmt.Sprintf("430 no such article <%s>", msgID))
		}
	case upper == "QUIT":
		s.writeLine("205 bye")
		return false
	default:
		s.writeLine("500 unknown command")
	}
	return true
}

func (s *session) serveArticle(cmd, upper string) {
	if (s.srv.opts.Username != "" || s.srv.opts.Password != "") && !s.authed {
		s.writeLine("480 authentication required")
		return
	}
	msgID := extractMessageID(cmd)
	if msgID == "" {
		s.writeLine("501 bad command")
		return
	}
	s.srv.mu.RLock()
	latency := s.srv.opts.ArticleLatency
	s.srv.mu.RUnlock()
	if latency > 0 {
		select {
		case <-s.srv.ctx.Done():
			return
		case <-time.After(latency):
		}
	}
	body, ok := s.srv.lookup(msgID)
	if !ok || s.srv.shouldDropArticle(msgID) {
		s.writeLine(fmt.Sprintf("430 no such article <%s>", msgID))
		return
	}
	// 222 = BODY follows; 220 = ARTICLE follows. Both work for our
	// fetcher; differentiate cleanly.
	if strings.HasPrefix(upper, "ARTICLE") {
		s.writeLine(fmt.Sprintf("220 0 <%s>", msgID))
	} else {
		s.writeLine(fmt.Sprintf("222 0 <%s>", msgID))
	}
	s.srv.mu.RLock()
	bps := s.srv.opts.BytesPerSec
	s.srv.mu.RUnlock()
	tw := &throttledWriter{w: s.c, bps: bps}
	// Send the body with dot-stuffing applied. The textproto
	// terminator is ".\r\n" on its own line.
	if err := writeDotStuffed(tw, body); err != nil {
		return
	}
	if _, err := io.WriteString(s.c, ".\r\n"); err != nil {
		return
	}
}

func (s *session) writeLine(line string) {
	_, _ = io.WriteString(s.c, line+"\r\n")
}

// extractMessageID pulls the <msgid> out of a command line like
// "BODY <abc@host>" → "abc@host". Returns "" if no angle-bracket
// argument is found.
func extractMessageID(cmd string) string {
	lt := strings.IndexByte(cmd, '<')
	gt := strings.LastIndexByte(cmd, '>')
	if lt < 0 || gt < lt {
		return ""
	}
	return cmd[lt+1 : gt]
}

// writeDotStuffed copies body to w, escaping any line that starts with
// a '.' by prepending another '.'. body should already use CRLF line
// endings.
func writeDotStuffed(w io.Writer, body []byte) error {
	// Walk line by line.
	start := 0
	for i := 0; i < len(body); i++ {
		if body[i] != '\n' {
			continue
		}
		line := body[start : i+1]
		if len(line) > 0 && line[0] == '.' {
			if _, err := w.Write([]byte{'.'}); err != nil {
				return err
			}
		}
		if _, err := w.Write(line); err != nil {
			return err
		}
		start = i + 1
	}
	if start < len(body) {
		// Trailing chunk without newline. Pad with CRLF so the
		// terminator dot lands on its own line.
		tail := body[start:]
		if len(tail) > 0 && tail[0] == '.' {
			if _, err := w.Write([]byte{'.'}); err != nil {
				return err
			}
		}
		if _, err := w.Write(tail); err != nil {
			return err
		}
		if _, err := w.Write([]byte("\r\n")); err != nil {
			return err
		}
	}
	return nil
}
