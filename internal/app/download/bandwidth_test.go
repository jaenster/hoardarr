package download

import (
	"context"
	"testing"
	"time"

	"github.com/jaenster/hoardarr/internal/domain/server"
)

// TestLimiter_NoCapDoesNotBlock — caps unset, Wait returns immediately.
func TestLimiter_NoCapDoesNotBlock(t *testing.T) {
	l := NewLimiter(0)
	start := time.Now()
	if err := l.Wait(context.Background(), server.ServerID(1), 1024*1024); err != nil {
		t.Fatalf("Wait err: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 5*time.Millisecond {
		t.Errorf("Wait without cap took %v; expected near-instant", elapsed)
	}
}

// TestLimiter_GlobalCapThrottles — request more bytes than burst,
// observe the wait. We pick a small cap (32 KiB/s) and request 64 KiB
// so the bucket refills ~0.5s before all tokens are available.
func TestLimiter_GlobalCapThrottles(t *testing.T) {
	if testing.Short() {
		t.Skip("timing-sensitive")
	}
	const cap = 32 * 1024 // 32 KiB/s
	const need = 64 * 1024
	l := NewLimiter(cap)
	// Drain the initial burst so the next Wait actually has to refill.
	if err := l.Wait(context.Background(), server.ServerID(1), need); err != nil {
		t.Fatalf("initial wait: %v", err)
	}
	start := time.Now()
	if err := l.Wait(context.Background(), server.ServerID(1), need); err != nil {
		t.Fatalf("second wait: %v", err)
	}
	elapsed := time.Since(start)
	// 64 KiB / 32 KiB/s = 2s, but burst forgives some — we just need
	// "noticeably more than zero".
	if elapsed < 500*time.Millisecond {
		t.Errorf("expected refill wait, got %v", elapsed)
	}
	if elapsed > 5*time.Second {
		t.Errorf("excessive wait %v — limiter math wrong", elapsed)
	}
}

// TestLimiter_SetGlobalCapZeroDisables — flipping cap to 0 unblocks
// throttled calls; subsequent Waits should be near-instant.
func TestLimiter_SetGlobalCapZeroDisables(t *testing.T) {
	l := NewLimiter(1024) // 1 KiB/s — slow
	l.SetGlobalCap(0)
	start := time.Now()
	if err := l.Wait(context.Background(), server.ServerID(1), 1024*1024); err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 50*time.Millisecond {
		t.Errorf("post-disable Wait took %v; want near-instant", elapsed)
	}
}

// TestLimiter_PerServerCapIsolated — capping server A doesn't affect B.
func TestLimiter_PerServerCapIsolated(t *testing.T) {
	l := NewLimiter(0)
	l.SetServerCap(server.ServerID(1), 1024) // 1 KiB/s on srv 1 only
	start := time.Now()
	if err := l.Wait(context.Background(), server.ServerID(2), 1024*1024); err != nil {
		t.Fatalf("srv 2 Wait: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 50*time.Millisecond {
		t.Errorf("srv 2 (uncapped) Wait took %v; should be instant", elapsed)
	}
}

// TestLimiter_ContextCancelStops — a Wait blocked on the global bucket
// returns ctx.Err() when ctx is cancelled.
func TestLimiter_ContextCancelStops(t *testing.T) {
	l := NewLimiter(1) // 1 byte/sec — very slow
	// Burn the burst first.
	_ = l.Wait(context.Background(), server.ServerID(1), 64*1024)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- l.Wait(ctx, server.ServerID(1), 1024*1024) }()
	time.Sleep(50 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Error("expected ctx err; got nil")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Wait did not return after ctx cancel")
	}
}

// TestLimiter_GlobalCapAccessor — round-trip current cap.
func TestLimiter_GlobalCapAccessor(t *testing.T) {
	l := NewLimiter(123456)
	if got := l.GlobalCap(); got != 123456 {
		t.Errorf("GlobalCap = %d; want 123456", got)
	}
	l.SetGlobalCap(789)
	if got := l.GlobalCap(); got != 789 {
		t.Errorf("after SetGlobalCap: GlobalCap = %d; want 789", got)
	}
	l.SetGlobalCap(0)
	if got := l.GlobalCap(); got != 0 {
		t.Errorf("after disable: GlobalCap = %d; want 0", got)
	}
}
