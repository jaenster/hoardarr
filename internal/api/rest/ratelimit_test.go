package rest

import (
	"testing"
	"time"
)

func TestIPRateLimiter_AllowsUntilMax(t *testing.T) {
	l := NewIPRateLimiter(3, time.Minute)
	for i := 0; i < 3; i++ {
		if !l.Allow("1.2.3.4") {
			t.Fatalf("attempt %d rejected before max", i+1)
		}
	}
	if l.Allow("1.2.3.4") {
		t.Fatal("4th attempt should have been rejected")
	}
}

func TestIPRateLimiter_PerIPIsolation(t *testing.T) {
	l := NewIPRateLimiter(2, time.Minute)
	if !l.Allow("a") || !l.Allow("a") {
		t.Fatal("a should be allowed twice")
	}
	if l.Allow("a") {
		t.Fatal("a should be rate-limited on 3rd")
	}
	if !l.Allow("b") {
		t.Fatal("b should be allowed independently of a's quota")
	}
}

func TestIPRateLimiter_WindowExpiry(t *testing.T) {
	l := NewIPRateLimiter(2, 50*time.Millisecond)
	if !l.Allow("x") || !l.Allow("x") {
		t.Fatal("two attempts should succeed")
	}
	if l.Allow("x") {
		t.Fatal("3rd within window should fail")
	}
	time.Sleep(70 * time.Millisecond)
	if !l.Allow("x") {
		t.Fatal("after window, next attempt should succeed")
	}
}
