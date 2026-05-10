package server

import (
	"strings"
	"testing"
	"time"
)

func ptrBool(b bool) *bool { return &b }
func ptrInt(i int) *int    { return &i }
func ptrStr(s string) *string { return &s }

func TestNew_DefaultsAndEvents(t *testing.T) {
	now := time.UnixMilli(1_700_000_000_000).UTC()
	s, err := New(NewParams{
		Name:     "main",
		Host:     "news.example.com",
		Port:     563,
		Username: "u", Password: "p",
	}, now)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if !s.TLS() {
		t.Errorf("TLS default = false; want true")
	}
	if s.MaxConns() != 8 {
		t.Errorf("MaxConns default = %d; want 8", s.MaxConns())
	}
	if !s.Enabled() {
		t.Errorf("Enabled default = false; want true")
	}
	evts := s.PullEvents()
	if len(evts) != 1 {
		t.Fatalf("event count = %d; want 1", len(evts))
	}
	if _, ok := evts[0].(ServerAdded); !ok {
		t.Errorf("first event is %T; want ServerAdded", evts[0])
	}
	if got := s.PullEvents(); len(got) != 0 {
		t.Errorf("events not drained: %d remain", len(got))
	}
}

func TestNew_RejectsInvalid(t *testing.T) {
	now := time.Now()
	cases := []struct {
		name string
		p    NewParams
		want string
	}{
		{"empty name", NewParams{Host: "h", Port: 563}, "name"},
		{"empty host", NewParams{Name: "n", Port: 563}, "host"},
		{"port low", NewParams{Name: "n", Host: "h", Port: 0}, "port"},
		{"port high", NewParams{Name: "n", Host: "h", Port: 99999}, "port"},
		{"max_conns 0 implicit OK (defaults to 8)", NewParams{Name: "n", Host: "h", Port: 1}, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := New(c.p, now)
			if c.want == "" {
				if err != nil {
					t.Fatalf("expected ok; got %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", c.want)
			}
			if !strings.Contains(err.Error(), c.want) {
				t.Errorf("err = %v; want contain %q", err, c.want)
			}
		})
	}
}

func TestSetID_PatchesPendingAddedEvent(t *testing.T) {
	s, _ := New(NewParams{Name: "x", Host: "h", Port: 1}, time.Now())
	s.SetID(42)
	evts := s.PullEvents()
	added, ok := evts[0].(ServerAdded)
	if !ok {
		t.Fatalf("first event %T; want ServerAdded", evts[0])
	}
	if added.ID != 42 {
		t.Errorf("ServerAdded.ID = %d; want 42 after SetID", added.ID)
	}
}

func TestSetEnabled_EmitsCorrectEvent(t *testing.T) {
	now := time.Now()
	s, _ := New(NewParams{Name: "x", Host: "h", Port: 1}, now)
	s.SetID(1)
	_ = s.PullEvents() // drain ServerAdded

	s.SetEnabled(true, now)
	if got := len(s.PullEvents()); got != 0 {
		t.Errorf("no-op enable emitted %d events", got)
	}

	s.SetEnabled(false, now)
	evts := s.PullEvents()
	if len(evts) != 1 {
		t.Fatalf("disable events = %d; want 1", len(evts))
	}
	if _, ok := evts[0].(ServerDisabled); !ok {
		t.Errorf("event is %T; want ServerDisabled", evts[0])
	}

	s.SetEnabled(true, now)
	evts = s.PullEvents()
	if _, ok := evts[0].(ServerEnabled); !ok {
		t.Errorf("event is %T; want ServerEnabled", evts[0])
	}
}

func TestUpdate_NoOp_NoEvent(t *testing.T) {
	now := time.Now()
	s, _ := New(NewParams{Name: "x", Host: "h", Port: 1}, now)
	s.SetID(1)
	_ = s.PullEvents()

	if err := s.Update(UpdateParams{Host: ptrStr("h")}, now); err != nil {
		t.Fatalf("Update: %v", err)
	}
	if got := len(s.PullEvents()); got != 0 {
		t.Errorf("no-op update emitted %d events", got)
	}
}

func TestUpdate_Mutations(t *testing.T) {
	now := time.UnixMilli(1).UTC()
	s, _ := New(NewParams{Name: "x", Host: "h", Port: 1}, now)
	s.SetID(7)
	_ = s.PullEvents()

	later := now.Add(time.Hour)
	if err := s.Update(UpdateParams{
		Host:     ptrStr("new"),
		Port:     ptrInt(563),
		TLS:      ptrBool(false),
		Username: ptrStr("user"),
		Password: ptrStr("pass"),
		MaxConns: ptrInt(20),
		Priority: ptrInt(2),
	}, later); err != nil {
		t.Fatalf("Update: %v", err)
	}
	if s.Host() != "new" || s.Port() != 563 || s.TLS() != false || s.Username() != "user" || s.Password() != "pass" || s.MaxConns() != 20 || s.Priority() != 2 {
		t.Errorf("mutations not applied: %+v", s)
	}
	if !s.UpdatedAt().Equal(later) {
		t.Errorf("UpdatedAt = %v; want %v", s.UpdatedAt(), later)
	}
	evts := s.PullEvents()
	if len(evts) != 1 {
		t.Fatalf("Update emitted %d events; want 1", len(evts))
	}
	if _, ok := evts[0].(ServerUpdated); !ok {
		t.Errorf("event is %T; want ServerUpdated", evts[0])
	}
}

func TestUpdate_RejectsInvalid(t *testing.T) {
	now := time.Now()
	s, _ := New(NewParams{Name: "x", Host: "h", Port: 1}, now)
	s.SetID(1)
	_ = s.PullEvents()

	if err := s.Update(UpdateParams{Port: ptrInt(0)}, now); err == nil {
		t.Error("expected error for invalid port")
	}
	if err := s.Update(UpdateParams{MaxConns: ptrInt(0)}, now); err == nil {
		t.Error("expected error for invalid max_conns")
	}
	if err := s.Update(UpdateParams{Host: ptrStr("   ")}, now); err == nil {
		t.Error("expected error for blank host")
	}
}
