package system

// Throughput tracker: a rolling-window of per-second byte counts.
//
// Design:
//   - Caller calls Add(n) as bytes arrive (from the tiered fetcher
//     reader-wrapper). Each Add stamps the current second.
//   - Sample() returns the per-second history for the last WindowSize
//     seconds plus aggregate sum + current bytes/sec.
//   - Window is fixed at 5 minutes (300 samples). Memory cost: 2.4 KB.
//
// Thread-safe. The byte counts are summed up in a ring of int64 slots
// indexed by (unix_seconds % windowSize). When a slot's timestamp is
// older than now, we reset it before adding — that "lazy expiry" keeps
// the read path branch-free.

import (
	"sync"
	"time"
)

// WindowSize is how many one-second buckets we keep. 300 = 5 minutes.
const WindowSize = 300

// Throughput is the rolling-window byte-rate tracker.
type Throughput struct {
	mu      sync.Mutex
	buckets [WindowSize]int64
	stamps  [WindowSize]int64 // unix-seconds at which the bucket was last touched
	now     func() time.Time
}

// NewThroughput constructs an empty tracker.
func NewThroughput() *Throughput {
	return &Throughput{now: func() time.Time { return time.Now().UTC() }}
}

// Add records n bytes into the current second's bucket.
func (t *Throughput) Add(n int64) {
	if n <= 0 {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	sec := t.now().Unix()
	idx := int(sec % int64(WindowSize))
	if t.stamps[idx] != sec {
		t.buckets[idx] = 0
		t.stamps[idx] = sec
	}
	t.buckets[idx] += n
}

// Sample is the current snapshot.
type Sample struct {
	// Series is bytes-per-second for the last WindowSize seconds,
	// oldest first. Entries for seconds with no activity are 0.
	Series []int64

	// Total is the sum across the window.
	Total int64

	// CurrentBytesPerSec is the last second's bucket.
	CurrentBytesPerSec int64
}

// Sample returns the current window snapshot.
func (t *Throughput) Sample() Sample {
	t.mu.Lock()
	defer t.mu.Unlock()
	now := t.now().Unix()
	out := Sample{Series: make([]int64, WindowSize)}
	for offset := 0; offset < WindowSize; offset++ {
		sec := now - int64(WindowSize-1-offset)
		idx := int(sec % int64(WindowSize))
		// Negative modulo guard (idx may go negative for the early
		// boot path when sec < WindowSize). Rare; clamp.
		if idx < 0 {
			idx += WindowSize
		}
		if t.stamps[idx] == sec {
			out.Series[offset] = t.buckets[idx]
			out.Total += t.buckets[idx]
			if offset == WindowSize-1 {
				out.CurrentBytesPerSec = t.buckets[idx]
			}
		}
	}
	return out
}
