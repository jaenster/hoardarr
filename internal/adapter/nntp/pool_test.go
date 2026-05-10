package nntp

import (
	"context"
	"io"
	"testing"
	"time"
)

// fullSession scripts a per-connection NNTP transcript: greeting +
// AUTHINFO + MODE READER + N body fetches + QUIT. Used by the pool
// tests to script a single-conn happy path.
func fullSession(s *stubSession, fetches int) {
	s.Send("200 ready")
	s.ExpectLine("AUTHINFO USER user")
	s.Send("381 need password")
	s.ExpectLine("AUTHINFO PASS pass")
	s.Send("281 authenticated")
	s.ExpectLine("MODE READER")
	s.Send("200 reader mode")
	for i := 0; i < fetches; i++ {
		// Echo the message-id back in the 222 line.
		line, err := s.br.ReadString('\n')
		if err != nil {
			s.err = err
			return
		}
		// "BODY <msg@host>\r\n" → "<msg@host>"
		mid := line[len("BODY ") : len(line)-2]
		s.Send("222 0 %s", mid)
		s.SendMulti("body bytes")
	}
	s.ExpectLine("QUIT")
	s.Send("205 closing")
}

func TestPool_AcquireSerial_ReusesConn(t *testing.T) {
	addr, cleanup := startStubServer(t, func(s *stubSession) {
		fullSession(s, 3)
	})
	defer cleanup()

	srv := stubUsenetServer(t, addr)
	pool := NewPool(srv, PoolOptions{})
	defer pool.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for i := 0; i < 3; i++ {
		c, release, err := pool.Acquire(ctx)
		if err != nil {
			t.Fatalf("Acquire %d: %v", i, err)
		}
		rc, err := c.Body(ctx, "msg@host")
		if err != nil {
			t.Fatalf("Body %d: %v", i, err)
		}
		if _, err := io.ReadAll(rc); err != nil {
			t.Fatalf("read %d: %v", i, err)
		}
		_ = rc.Close()
		release(nil)
		if got := pool.IdleCount(); got != 1 {
			t.Errorf("after release %d: IdleCount = %d; want 1", i, got)
		}
	}

	// Acquire one final time to drive the QUIT/205 the stub expects.
	c, release, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("final Acquire: %v", err)
	}
	_ = c.Quit(ctx)
	release(errFakeClose) // signal "dead conn"
}

// errFakeClose is just a non-nil error to pass to release().
var errFakeClose = &ProtocolError{Code: 999, Message: "test-close"}

func TestPool_ReleaseWithError_DoesNotPoolConn(t *testing.T) {
	addr, cleanup := startStubServer(t, func(s *stubSession) {
		s.Send("200 ready")
		s.ExpectLine("AUTHINFO USER user")
		s.Send("381 need password")
		s.ExpectLine("AUTHINFO PASS pass")
		s.Send("281 authenticated")
		s.ExpectLine("MODE READER")
		s.Send("200 reader")
		s.ExpectLine("QUIT")
		s.Send("205 closing")
	})
	defer cleanup()

	srv := stubUsenetServer(t, addr)
	pool := NewPool(srv, PoolOptions{})
	defer pool.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	c, release, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	_ = c.Quit(ctx)
	release(errFakeClose)

	if got := pool.IdleCount(); got != 0 {
		t.Errorf("IdleCount after errored release = %d; want 0", got)
	}
}

func TestPool_AcquireAfterClose_Errors(t *testing.T) {
	addr, cleanup := startStubServer(t, func(s *stubSession) {
		// Greet then accept QUIT immediately if Pool.Close routes one.
		s.Send("200 ready")
		// stub is allowed to exit after the listener closes.
	})
	defer cleanup()

	srv := stubUsenetServer(t, addr)
	pool := NewPool(srv, PoolOptions{})
	_ = pool.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	if _, _, err := pool.Acquire(ctx); err != ErrPoolClosed {
		t.Errorf("err = %v; want ErrPoolClosed", err)
	}
}

func TestPool_Reaper_ClosesIdlePastTTL(t *testing.T) {
	addr, cleanup := startStubServer(t, func(s *stubSession) {
		s.Send("200 ready")
		s.ExpectLine("AUTHINFO USER user")
		s.Send("381 need password")
		s.ExpectLine("AUTHINFO PASS pass")
		s.Send("281 authenticated")
		s.ExpectLine("MODE READER")
		s.Send("200 reader")
		// Idle... reaper will close us; the stub's deadline will then
		// drop the connection from its side.
	})
	defer cleanup()

	srv := stubUsenetServer(t, addr)
	pool := NewPool(srv, PoolOptions{
		IdleTTL:        50 * time.Millisecond,
		StaleThreshold: time.Second,
	})
	defer pool.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, release, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	release(nil)
	if got := pool.IdleCount(); got != 1 {
		t.Fatalf("after release: IdleCount = %d; want 1", got)
	}
	// Wait for the reaper to fire — IdleTTL 50ms, reaper interval is
	// max(IdleTTL/2, 10s); we need to wait longer than that. To make
	// the test fast, call reap() directly.
	time.Sleep(60 * time.Millisecond)
	pool.reap()
	if got := pool.IdleCount(); got != 0 {
		t.Errorf("after reap: IdleCount = %d; want 0", got)
	}
}
