package system

import (
	"testing"
	"time"
)

func TestThroughput_EmptyWindow(t *testing.T) {
	tp := NewThroughput()
	s := tp.Sample()
	if len(s.Series) != DefaultSampleSeconds {
		t.Errorf("len = %d; want %d", len(s.Series), DefaultSampleSeconds)
	}
	if s.Total != 0 || s.CurrentBytesPerSec != 0 {
		t.Errorf("empty window non-zero: total=%d cur=%d", s.Total, s.CurrentBytesPerSec)
	}
	if s.WindowPeakBytesPerSec != 0 {
		t.Errorf("empty window peak non-zero: %d", s.WindowPeakBytesPerSec)
	}
}

func TestThroughput_AddAccumulates(t *testing.T) {
	tp := NewThroughput()
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
	if s.WindowPeakBytesPerSec != 150 {
		t.Errorf("WindowPeakBytesPerSec = %d; want 150", s.WindowPeakBytesPerSec)
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

	if s.Total != 300 {
		t.Errorf("Total = %d; want 300", s.Total)
	}
	if s.CurrentBytesPerSec != 200 {
		t.Errorf("CurrentBytesPerSec = %d; want 200", s.CurrentBytesPerSec)
	}
	n := len(s.Series)
	if got := s.Series[n-2]; got != 100 {
		t.Errorf("Series[-2] = %d; want 100", got)
	}
	if got := s.Series[n-1]; got != 200 {
		t.Errorf("Series[-1] = %d; want 200", got)
	}
	if s.WindowPeakBytesPerSec != 200 {
		t.Errorf("WindowPeakBytesPerSec = %d; want 200", s.WindowPeakBytesPerSec)
	}
}

func TestThroughput_OldBucketsExpire(t *testing.T) {
	tp := NewThroughput()
	now := time.Unix(1_700_000_000, 0).UTC()
	tp.now = func() time.Time { return now }

	tp.Add(999)
	// Jump well past the ring.
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

func TestThroughput_AllTimePeakBumps(t *testing.T) {
	tp := NewThroughput()
	now := time.Unix(1_700_000_000, 0).UTC()
	tp.now = func() time.Time { return now }

	tp.Add(500)
	if got := tp.AllTimePeak(); got != 500 {
		t.Errorf("AllTimePeak after 500 = %d; want 500", got)
	}
	now = now.Add(time.Second)
	tp.Add(200)
	if got := tp.AllTimePeak(); got != 500 {
		t.Errorf("AllTimePeak after smaller bucket = %d; want 500", got)
	}
	now = now.Add(time.Second)
	tp.Add(700)
	if got := tp.AllTimePeak(); got != 700 {
		t.Errorf("AllTimePeak after 700 = %d; want 700", got)
	}
}

func TestThroughput_SetAllTimePeakSeeds(t *testing.T) {
	tp := NewThroughput()
	tp.SetAllTimePeak(1234)
	if got := tp.AllTimePeak(); got != 1234 {
		t.Errorf("AllTimePeak after seed = %d; want 1234", got)
	}
	// A smaller Add doesn't lower the persisted peak.
	tp.Add(5)
	if got := tp.AllTimePeak(); got != 1234 {
		t.Errorf("AllTimePeak after small Add = %d; want 1234", got)
	}
}

func TestThroughput_SampleRangeRespectsN(t *testing.T) {
	tp := NewThroughput()
	now := time.Unix(1_700_000_000, 0).UTC()
	tp.now = func() time.Time { return now }
	tp.Add(42)

	for _, n := range []int{10, 60, 300, 1800, 3600} {
		s := tp.SampleRange(n)
		if len(s.Series) != n {
			t.Errorf("SampleRange(%d) len = %d; want %d", n, len(s.Series), n)
		}
		if s.WindowSeconds != n {
			t.Errorf("SampleRange(%d) WindowSeconds = %d; want %d", n, s.WindowSeconds, n)
		}
	}
	// Clamps above WindowSize.
	if s := tp.SampleRange(WindowSize * 4); len(s.Series) != WindowSize {
		t.Errorf("SampleRange overflow len = %d; want %d", len(s.Series), WindowSize)
	}
}
