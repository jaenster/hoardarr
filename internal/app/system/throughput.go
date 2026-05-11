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

	// CurrentBytesPerSec is the last second's bucket. Jitters a lot
	// under concurrent fetches; use Avg10sBytesPerSec for UI display.
	CurrentBytesPerSec int64

	// Avg10sBytesPerSec is the average rate over the last 10 seconds.
	// Smoother than CurrentBytesPerSec — appropriate for human-facing
	// speed indicators and ETA calculations.
	Avg10sBytesPerSec int64

	// Avg60sBytesPerSec is the average rate over the last minute.
	// Slower-moving baseline for stable trends.
	Avg60sBytesPerSec int64
}

// AvgWindow is the short-window average bucket count used for the
// smoother human-facing rate. Tunable but 10 seconds is a good
// compromise between responsiveness and stability.
const AvgWindow = 10

// LongAvgWindow is the longer baseline window.
const LongAvgWindow = 60

// Sample returns the current window snapshot.
//
// Skips the most recent second when computing Avg10s/Avg60s because
// the current bucket is still accruing — including it biases the
// average downward at sample-time and makes the value lurch as
// seconds tick over.
func (t *Throughput) Sample() Sample {
	t.mu.Lock()
	defer t.mu.Unlock()
	now := t.now().Unix()
	out := Sample{Series: make([]int64, WindowSize)}

	var shortSum, longSum int64
	for offset := 0; offset < WindowSize; offset++ {
		sec := now - int64(WindowSize-1-offset)
		idx := int(sec % int64(WindowSize))
		// Negative modulo guard (idx may go negative for the early
		// boot path when sec < WindowSize). Rare; clamp.
		if idx < 0 {
			idx += WindowSize
		}
		if t.stamps[idx] != sec {
			continue
		}
		v := t.buckets[idx]
		out.Series[offset] = v
		out.Total += v
		if offset == WindowSize-1 {
			out.CurrentBytesPerSec = v
			continue // skip current bucket from averages — still accruing
		}
		// secsAgo = how many seconds back the bucket sits, 1 = previous.
		secsAgo := WindowSize - 1 - offset
		if secsAgo <= AvgWindow {
			shortSum += v
		}
		if secsAgo <= LongAvgWindow {
			longSum += v
		}
	}
	out.Avg10sBytesPerSec = shortSum / int64(AvgWindow)
	out.Avg60sBytesPerSec = longSum / int64(LongAvgWindow)
	return out
}
