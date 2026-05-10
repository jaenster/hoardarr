package nntp

// In-process scripted NNTP server for tests. The stub binds to a random
// localhost port (no TLS), accepts one connection, and runs a handler
// function over the resulting session. Handlers script the protocol
// turn-by-turn via Send / ExpectLine helpers.
//
// This file lives in the nntp package itself so tests can poke at
// internals if needed; production code never references it.

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jaenster/hoardarr/internal/domain/server"
)

// stubSession is the per-connection stub-server context. Handlers
// receive one and use it to script the conversation.
type stubSession struct {
	t   *testing.T
	c   net.Conn
	br  *bufio.Reader
	mu  sync.Mutex
	err error
}

func (s *stubSession) Send(format string, args ...any) {
	if s.err != nil {
		return
	}
	line := fmt.Sprintf(format, args...)
	if !strings.HasSuffix(line, "\r\n") {
		line += "\r\n"
	}
	if _, err := s.c.Write([]byte(line)); err != nil {
		s.err = fmt.Errorf("stub write: %w", err)
	}
}

// SendMulti sends a multi-line block followed by the NNTP terminator
// (".\r\n"). body should already include line terminators between lines.
func (s *stubSession) SendMulti(body string) {
	if s.err != nil {
		return
	}
	if body != "" && !strings.HasSuffix(body, "\r\n") {
		body += "\r\n"
	}
	body += ".\r\n"
	if _, err := s.c.Write([]byte(body)); err != nil {
		s.err = fmt.Errorf("stub write multi: %w", err)
	}
}

// ExpectLine reads the next CRLF-terminated line from the client and
// asserts it equals want (whitespace-trimmed). Failures call t.Errorf
// and continue so the handler can issue a corrective response.
func (s *stubSession) ExpectLine(want string) string {
	if s.err != nil {
		return ""
	}
	line, err := s.br.ReadString('\n')
	if err != nil {
		s.err = fmt.Errorf("stub read: %w", err)
		return ""
	}
	got := strings.TrimRight(line, "\r\n")
	if got != want {
		s.t.Helper()
		s.t.Errorf("stub: expected %q; got %q", want, got)
	}
	return got
}

// startStubServer binds a localhost listener and dispatches handler on
// the first incoming connection. Returns the address and a cleanup
// function. Subsequent connections are rejected.
func startStubServer(t *testing.T, handler func(*stubSession)) (addr string, cleanup func()) {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr = l.Addr().String()

	done := make(chan struct{})
	go func() {
		defer close(done)
		c, err := l.Accept()
		if err != nil {
			return // listener closed
		}
		defer c.Close()
		_ = c.SetDeadline(time.Now().Add(10 * time.Second))
		s := &stubSession{
			t:  t,
			c:  c,
			br: bufio.NewReader(c),
		}
		handler(s)
		if s.err != nil && s.err != io.EOF {
			t.Errorf("stub handler: %v", s.err)
		}
	}()

	cleanup = func() {
		_ = l.Close()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Errorf("stub did not finish within 2s")
		}
	}
	return addr, cleanup
}

func stubUsenetServer(t *testing.T, addr string) *server.UsenetServer {
	t.Helper()
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatalf("split addr: %v", err)
	}
	var port int
	if _, err := fmt.Sscan(portStr, &port); err != nil {
		t.Fatalf("port: %v", err)
	}
	tlsOff := false
	s, err := server.New(server.NewParams{
		Name:     "stub",
		Host:     host,
		Port:     port,
		TLS:      &tlsOff,
		Username: "user",
		Password: "pass",
		MaxConns: 1,
	}, time.Now())
	if err != nil {
		t.Fatalf("server.New: %v", err)
	}
	return s
}

func TestDial_Greeting200(t *testing.T) {
	addr, cleanup := startStubServer(t, func(s *stubSession) {
		s.Send("200 stub server ready")
		s.ExpectLine("QUIT")
		s.Send("205 closing")
	})
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := Dial(ctx, stubUsenetServer(t, addr))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	if err := c.Quit(ctx); err != nil {
		t.Errorf("Quit: %v", err)
	}
}

func TestDial_RejectsBadGreeting(t *testing.T) {
	addr, cleanup := startStubServer(t, func(s *stubSession) {
		s.Send("502 service unavailable")
	})
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := Dial(ctx, stubUsenetServer(t, addr)); err == nil {
		t.Fatal("expected error on 502 greeting")
	}
}

func TestAuthenticate_Success(t *testing.T) {
	addr, cleanup := startStubServer(t, func(s *stubSession) {
		s.Send("200 ready")
		s.ExpectLine("AUTHINFO USER user")
		s.Send("381 need password")
		s.ExpectLine("AUTHINFO PASS pass")
		s.Send("281 authenticated")
		s.ExpectLine("QUIT")
		s.Send("205 closing")
	})
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	c, err := Dial(ctx, stubUsenetServer(t, addr))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c.Quit(ctx)

	if err := c.Authenticate(ctx); err != nil {
		t.Fatalf("Authenticate: %v", err)
	}
}

func TestAuthenticate_Failure(t *testing.T) {
	addr, cleanup := startStubServer(t, func(s *stubSession) {
		s.Send("200 ready")
		s.ExpectLine("AUTHINFO USER user")
		s.Send("381 need password")
		s.ExpectLine("AUTHINFO PASS pass")
		s.Send("481 authentication rejected")
	})
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	c, err := Dial(ctx, stubUsenetServer(t, addr))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c.Close()

	err = c.Authenticate(ctx)
	if err == nil {
		t.Fatal("expected authentication error")
	}
	// Optional: confirm sentinel
	// (errors.Is over fmt.Errorf-wrapped errors works for our shape)
}

func TestBody_HappyPath(t *testing.T) {
	const body = "Hello yEnc body\r\nMore lines here"
	addr, cleanup := startStubServer(t, func(s *stubSession) {
		s.Send("200 ready")
		s.ExpectLine("BODY <msg1@host>")
		s.Send("222 0 <msg1@host>")
		s.SendMulti(body)
		s.ExpectLine("QUIT")
		s.Send("205 closing")
	})
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	c, err := Dial(ctx, stubUsenetServer(t, addr))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c.Quit(ctx)

	rc, err := c.Body(ctx, "msg1@host")
	if err != nil {
		t.Fatalf("Body: %v", err)
	}
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	_ = rc.Close()
	// textproto.DotReader normalises CRLF to LF (RFC 822 convention).
	// The yEnc decoder handles either form so this is a non-issue
	// downstream.
	want := strings.ReplaceAll(body, "\r\n", "\n") + "\n"
	if string(got) != want {
		t.Errorf("body = %q; want %q", got, want)
	}
}

func TestBody_DotStuffing(t *testing.T) {
	// The wire body has a line starting with ".." which should be
	// de-stuffed to ".".
	const wire = ".. dotted\r\nnormal line"
	const want = ". dotted\nnormal line\n"
	addr, cleanup := startStubServer(t, func(s *stubSession) {
		s.Send("200 ready")
		s.ExpectLine("BODY <msg@host>")
		s.Send("222 0 <msg@host>")
		s.SendMulti(wire)
	})
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	c, err := Dial(ctx, stubUsenetServer(t, addr))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c.Close()

	rc, err := c.Body(ctx, "msg@host")
	if err != nil {
		t.Fatalf("Body: %v", err)
	}
	got, _ := io.ReadAll(rc)
	if string(got) != want {
		t.Errorf("body = %q; want %q", got, want)
	}
}

func TestBody_ArticleMissing(t *testing.T) {
	addr, cleanup := startStubServer(t, func(s *stubSession) {
		s.Send("200 ready")
		s.ExpectLine("BODY <missing@host>")
		s.Send("430 No such article")
		s.ExpectLine("QUIT")
		s.Send("205 closing")
	})
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	c, err := Dial(ctx, stubUsenetServer(t, addr))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c.Quit(ctx)

	_, err = c.Body(ctx, "missing@host")
	if err == nil {
		t.Fatal("expected error for missing article")
	}
}

func TestDate(t *testing.T) {
	addr, cleanup := startStubServer(t, func(s *stubSession) {
		s.Send("200 ready")
		s.ExpectLine("DATE")
		s.Send("111 20260510120000")
		s.ExpectLine("QUIT")
		s.Send("205 closing")
	})
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	c, err := Dial(ctx, stubUsenetServer(t, addr))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c.Quit(ctx)

	got, err := c.Date(ctx)
	if err != nil {
		t.Fatalf("Date: %v", err)
	}
	if got.Year() != 2026 || got.Month() != 5 || got.Day() != 10 {
		t.Errorf("date = %v", got)
	}
}

// TestDial_ServerClosesAfterGreeting ensures the client surfaces a
// clean error if the server sends greeting and immediately closes
// (denied silently). Without ctx-cancel we'd risk hanging.
func TestDial_ServerClosesAfterGreeting(t *testing.T) {
	addr, cleanup := startStubServer(t, func(s *stubSession) {
		s.Send("200 hello")
		// Handler returns; conn closes.
	})
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	c, err := Dial(ctx, stubUsenetServer(t, addr))
	if err != nil {
		// Could fail at dial too, depending on race; either way is acceptable.
		return
	}
	defer c.Close()

	if err := c.Authenticate(ctx); err == nil {
		t.Error("expected error when server closed after greeting")
	}
}

// TestBody_AuthRequired480 ensures a 480 response on BODY surfaces
// as ErrAuthRequired rather than a generic protocol error. The
// orchestrator's retry policy will then dial a fresh authed conn.
func TestBody_AuthRequired480(t *testing.T) {
	addr, cleanup := startStubServer(t, func(s *stubSession) {
		s.Send("200 ready")
		s.ExpectLine("AUTHINFO USER user")
		s.Send("381 password required")
		s.ExpectLine("AUTHINFO PASS pass")
		s.Send("281 authenticated")
		s.ExpectLine("BODY <msg@host>")
		s.Send("480 authentication required")
		s.ExpectLine("QUIT")
		s.Send("205 closing")
	})
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	c, err := Dial(ctx, stubUsenetServer(t, addr))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c.Quit(ctx)

	if err := c.Authenticate(ctx); err != nil {
		t.Fatalf("Authenticate: %v", err)
	}

	_, err = c.Body(ctx, "msg@host")
	if !errors.Is(err, ErrAuthRequired) {
		t.Errorf("err = %v; want ErrAuthRequired", err)
	}
}

// TestAuthenticate_BadCredentials481 ensures a 481 on AUTHINFO PASS
// surfaces as ErrAuthFailed.
func TestAuthenticate_BadCredentials481(t *testing.T) {
	addr, cleanup := startStubServer(t, func(s *stubSession) {
		s.Send("200 ready")
		s.ExpectLine("AUTHINFO USER user")
		s.Send("381 password required")
		s.ExpectLine("AUTHINFO PASS pass")
		s.Send("481 authentication rejected")
	})
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	c, err := Dial(ctx, stubUsenetServer(t, addr))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c.Close()

	err = c.Authenticate(ctx)
	if !errors.Is(err, ErrAuthFailed) {
		t.Errorf("err = %v; want ErrAuthFailed", err)
	}
}

// Defence-in-depth: send() must reject control characters that would
// split the command, even if upstream validation slipped.
func TestSend_RejectsControlCharacters(t *testing.T) {
	cases := []string{
		"AUTHINFO USER good\r\nQUIT",
		"AUTHINFO PASS bad\npassword",
		"BODY <evil\r\n>",
		"BODY \x00 nullbyte",
	}
	for _, line := range cases {
		t.Run(line, func(t *testing.T) {
			if err := validateCommandLine(line); err == nil {
				t.Errorf("validateCommandLine(%q) = nil; want error", line)
			}
		})
	}
	if err := validateCommandLine("BODY <safe@host>"); err != nil {
		t.Errorf("validateCommandLine(safe) = %v; want nil", err)
	}
}

func TestModeReader_TolerantOf500(t *testing.T) {
	for _, code := range []string{"500 unknown command", "501 syntax", "502 not allowed"} {
		t.Run(code, func(t *testing.T) {
			addr, cleanup := startStubServer(t, func(s *stubSession) {
				s.Send("200 ready")
				s.ExpectLine("MODE READER")
				s.Send("%s", code)
				s.ExpectLine("QUIT")
				s.Send("205 closing")
			})
			defer cleanup()

			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			c, err := Dial(ctx, stubUsenetServer(t, addr))
			if err != nil {
				t.Fatalf("Dial: %v", err)
			}
			defer c.Quit(ctx)

			if err := c.ModeReader(ctx); err != nil {
				t.Errorf("ModeReader on %q should succeed; got %v", code, err)
			}
		})
	}
}
