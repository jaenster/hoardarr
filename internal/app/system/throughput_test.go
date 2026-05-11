package system

import (
	"testing"
	"time"
)

func TestThroughput_EmptyWindow(t *testing.T) {
	tp := NewThroughput()
	s := tp.Sample()
	if len(s.Series) != WindowSize {
		t.Errorf("len = %d; want %d", len(s.Series), WindowSize)
	}
	if s.Total != 0 || s.CurrentBytesPerSec != 0 {
		t.Errorf("empty window non-zero: total=%d cur=%d", s.Total, s.CurrentBytesPerSec)
	}
}

func TestThroughput_AddAccumulates(t *testing.T) {
	tp := NewThroughput()
	// Pin the clock for determinism.
	now := time.Unix(1_700_000_000, 0).UTC()
	tp.now = func() time.Time { return now }

	tp.Add(100)
	tp.Add(50)
	s := tp.Sample()
	if s.Total != 150 {
		t.Errorf("Total = %d; want 150", s.Total)
	}
	if s.CurrentBytesPerSec != 150 {
		t.Errorf("CurrentBytesPerSec = %d; want 150", s.CurrentBytesPerSec)
	}
}

func TestThroughput_SecondRollsOver(t *testing.T) {
	tp := NewThroughput()
	now := time.Unix(1_700_000_000, 0).UTC()
	tp.now = func() time.Time { return now }

	tp.Add(100) // second 0
	now = now.Add(time.Second)
	tp.Add(200) // second 1
	s := tp.Sample()

	// Both buckets should be visible in the window.
	if s.Total != 300 {
		t.Errorf("Total = %d; want 300", s.Total)
	}
	if s.CurrentBytesPerSec != 200 {
		t.Errorf("CurrentBytesPerSec = %d; want 200", s.CurrentBytesPerSec)
	}
	// The second-old bucket should be at index WindowSize-2.
	if got := s.Series[WindowSize-2]; got != 100 {
		t.Errorf("Series[-2] = %d; want 100", got)
	}
	if got := s.Series[WindowSize-1]; got != 200 {
		t.Errorf("Series[-1] = %d; want 200", got)
	}
}

func TestThroughput_OldBucketsExpire(t *testing.T) {
	tp := NewThroughput()
	now := time.Unix(1_700_000_000, 0).UTC()
	tp.now = func() time.Time { return now }

	tp.Add(999)
	// Jump well past the window.
	now = now.Add(time.Duration(WindowSize*2) * time.Second)
	s := tp.Sample()
	if s.Total != 0 {
		t.Errorf("expired bucket leaked into total = %d", s.Total)
	}
}

func TestThroughput_IgnoresNegativeOrZero(t *testing.T) {
	tp := NewThroughput()
	tp.Add(0)
	tp.Add(-5)
	if s := tp.Sample(); s.Total != 0 {
		t.Errorf("Total = %d; want 0", s.Total)
	}
}
