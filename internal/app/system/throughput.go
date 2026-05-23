package system

// Throughput tracker: a rolling-window of per-second byte counts.
//
// The ring holds 1 hour of 1-second samples; Sample() defaults to the
// most recent DefaultSampleSeconds (5 min) so the canonical /throughput
// response shape stays unchanged. SampleRange(n) returns the last n
// seconds for callers that want a wider view (e.g. the speed-history
// endpoint).
//
// All-time peak is tracked in memory; persistence is the caller's job
// (load via SetAllTimePeak on startup, read via AllTimePeak on a
// periodic flush). The tracker doesn't touch storage itself so the
// adapter boundary stays clean.

import (
	"sync"
	"sync/atomic"
	"time"
)

// WindowSize is how many one-second buckets we keep in memory.
// 3600 = 1 hour. Cost: 3600 * 16 B (bucket+stamp) = ~57 KiB.
const WindowSize = 3600

// DefaultSampleSeconds is the default window returned by Sample().
// 300 = 5 minutes. Matches the historical response shape so existing
// clients keep working when the ring widens.
const DefaultSampleSeconds = 300

// Throughput is the rolling-window byte-rate tracker.
type Throughput struct {
	mu          sync.Mutex
	buckets     [WindowSize]int64
	stamps      [WindowSize]int64 // unix-seconds at which the bucket was last touched
	allTimePeak atomic.Int64
	now         func() time.Time
}

// NewThroughput constructs an empty tracker.
func NewThroughput() *Throughput {
	return &Throughput{now: func() time.Time { return time.Now().UTC() }}
}

// Add records n bytes into the current second's bucket. Bumps the
// all-time peak if the bucket's new total exceeds it.
func (t *Throughput) Add(n int64) {
	if n <= 0 {
		return
	}
	t.mu.Lock()
	sec := t.now().Unix()
	idx := int(sec % int64(WindowSize))
	if t.stamps[idx] != sec {
		t.buckets[idx] = 0
		t.stamps[idx] = sec
	}
	t.buckets[idx] += n
	bucket := t.buckets[idx]
	t.mu.Unlock()

	// Bump all-time peak outside the bucket lock. The atomic compare-
	// and-swap loop tolerates concurrent bumps from other goroutines
	// without serialising on the bucket mutex.
	for {
		prev := t.allTimePeak.Load()
		if bucket <= prev {
			break
		}
		if t.allTimePeak.CompareAndSwap(prev, bucket) {
			break
		}
	}
}

// AllTimePeak returns the highest single-second bucket seen since the
// process started or since SetAllTimePeak was last called with a value
// at least as large. Safe for concurrent use.
func (t *Throughput) AllTimePeak() int64 { return t.allTimePeak.Load() }

// SetAllTimePeak seeds the all-time peak. Used at startup to restore
// the persisted value; the in-memory peak only bumps if a future
// Add() bucket exceeds it.
func (t *Throughput) SetAllTimePeak(v int64) {
	if v < 0 {
		v = 0
	}
	t.allTimePeak.Store(v)
}

// Sample is the current snapshot.
type Sample struct {
	// Series is bytes-per-second for the last len(Series) seconds,
	// oldest first. Entries for seconds with no activity are 0.
	Series []int64

	// Total is the sum across the returned series.
	Total int64

	// CurrentBytesPerSec is the last second's bucket. Jitters a lot
	// under concurrent fetches; use Avg10sBytesPerSec for UI display.
	CurrentBytesPerSec int64

	// Avg10sBytesPerSec is the average rate over the last 10 seconds.
	Avg10sBytesPerSec int64

	// Avg60sBytesPerSec is the average rate over the last minute.
	Avg60sBytesPerSec int64

	// WindowPeakBytesPerSec is the highest bucket inside Series.
	WindowPeakBytesPerSec int64

	// WindowSeconds is len(Series); convenience for the REST DTO.
	WindowSeconds int
}

// AvgWindow is the short-window average bucket count used for the
// smoother human-facing rate.
const AvgWindow = 10

// LongAvgWindow is the longer baseline window.
const LongAvgWindow = 60

// Sample returns the current DefaultSampleSeconds-wide snapshot.
//
// Skips the most recent second when computing Avg10s/Avg60s because
// the current bucket is still accruing — including it biases the
// average downward at sample-time and makes the value lurch as
// seconds tick over.
func (t *Throughput) Sample() Sample { return t.SampleRange(DefaultSampleSeconds) }

// SampleRange returns the last n seconds (clamped to WindowSize).
func (t *Throughput) SampleRange(n int) Sample {
	if n <= 0 {
		n = DefaultSampleSeconds
	}
	if n > WindowSize {
		n = WindowSize
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	now := t.now().Unix()
	out := Sample{Series: make([]int64, n), WindowSeconds: n}

	var shortSum, longSum int64
	for offset := 0; offset < n; offset++ {
		sec := now - int64(n-1-offset)
		idx := int(sec % int64(WindowSize))
		if idx < 0 {
			idx += WindowSize
		}
		if t.stamps[idx] != sec {
			continue
		}
		v := t.buckets[idx]
		out.Series[offset] = v
		out.Total += v
		if v > out.WindowPeakBytesPerSec {
			out.WindowPeakBytesPerSec = v
		}
		if offset == n-1 {
			out.CurrentBytesPerSec = v
			continue // skip current bucket from averages — still accruing
		}
		secsAgo := n - 1 - offset
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
