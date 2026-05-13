package rest

import (
	"net"
	"net/http"
	"sync"
	"time"
)

// IPRateLimiter is a sliding-window per-IP rate limiter. The intended
// caller is the /auth/login + /auth/setup pair — endpoints where a
// brute-forcer can grind us into bcrypt verification on every request
// otherwise.
//
// Implementation: each IP gets a circular buffer of attempt timestamps.
// On every call we expire timestamps older than the window, append the
// current, and reject if the live count exceeds `Max`. No background
// goroutine, no GC churn — entries are pruned lazily on the next call
// from the same IP.
//
// Memory cost: O(active_attackers * Max). With Max=10 and one byte per
// timestamp (16B time.Time), even 10k attackers fit in <2 MB.
type IPRateLimiter struct {
	Max    int           // attempts allowed in window
	Window time.Duration // sliding window length

	mu      sync.Mutex
	entries map[string][]time.Time

	// gc tracks when we last purged inactive IPs from the map. The
	// per-IP slice pruning is lazy; the map pruning runs at most every
	// 5 minutes when the limiter is consulted.
	lastGC time.Time
}

// NewIPRateLimiter constructs a limiter. Sensible default for auth
// endpoints: 10 attempts per minute.
func NewIPRateLimiter(max int, window time.Duration) *IPRateLimiter {
	return &IPRateLimiter{
		Max:     max,
		Window:  window,
		entries: make(map[string][]time.Time),
		lastGC:  time.Now(),
	}
}

// Allow records an attempt from ip and returns true if it's within the
// limit, false if the caller should be rejected with 429.
func (l *IPRateLimiter) Allow(ip string) bool {
	now := time.Now()
	cutoff := now.Add(-l.Window)

	l.mu.Lock()
	defer l.mu.Unlock()

	if now.Sub(l.lastGC) > 5*time.Minute {
		for k, ts := range l.entries {
			pruned := pruneBefore(ts, cutoff)
			if len(pruned) == 0 {
				delete(l.entries, k)
			} else {
				l.entries[k] = pruned
			}
		}
		l.lastGC = now
	}

	entries := pruneBefore(l.entries[ip], cutoff)
	if len(entries) >= l.Max {
		l.entries[ip] = entries
		return false
	}
	entries = append(entries, now)
	l.entries[ip] = entries
	return true
}

func pruneBefore(ts []time.Time, cutoff time.Time) []time.Time {
	i := 0
	for ; i < len(ts); i++ {
		if ts[i].After(cutoff) {
			break
		}
	}
	if i == 0 {
		return ts
	}
	out := make([]time.Time, len(ts)-i)
	copy(out, ts[i:])
	return out
}

// clientIP extracts the client address from r. Prefers
// X-Forwarded-For / X-Real-IP (so the limiter still works behind a
// reverse proxy that's been configured to set them) and falls back to
// the TCP-level remote addr. Always returns a non-empty string; falls
// back to RemoteAddr verbatim if no IP can be parsed.
func clientIP(r *http.Request) string {
	if v := r.Header.Get("X-Forwarded-For"); v != "" {
		// Take the first hop — the closest client-originated IP.
		for i := 0; i < len(v); i++ {
			if v[i] == ',' {
				v = v[:i]
				break
			}
		}
		if ip := net.ParseIP(trimSpaceASCII(v)); ip != nil {
			return ip.String()
		}
	}
	if v := r.Header.Get("X-Real-IP"); v != "" {
		if ip := net.ParseIP(trimSpaceASCII(v)); ip != nil {
			return ip.String()
		}
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err == nil {
		return host
	}
	return r.RemoteAddr
}

func trimSpaceASCII(s string) string {
	for len(s) > 0 && (s[0] == ' ' || s[0] == '\t') {
		s = s[1:]
	}
	for len(s) > 0 && (s[len(s)-1] == ' ' || s[len(s)-1] == '\t') {
		s = s[:len(s)-1]
	}
	return s
}

// rateLimitedHandler wraps next with the limiter; 429 on reject. The
// retry-after is the floor on when the oldest in-window attempt would
// fall out — small client-side hint, not a security guarantee.
func rateLimitedHandler(l *IPRateLimiter, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !l.Allow(clientIP(r)) {
			w.Header().Set("Retry-After", "60")
			http.Error(w, `{"error":"rate limit exceeded; try again later"}`, http.StatusTooManyRequests)
			return
		}
		next(w, r)
	}
}
