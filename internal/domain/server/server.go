// Package server is the bounded context that owns Usenet provider
// configuration: hosts, credentials, connection caps, priority order.
//
// The download orchestrator consumes ServerRepository to know where to
// dispatch article fetches; it never sees host/port/credentials directly.
// The NNTP adapter receives a UsenetServer value when constructing a
// connection.
//
// Aggregate root: UsenetServer.
package server

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jaenster/hoardarr/internal/domain/event"
)

// ServerID identifies a UsenetServer. Allocated by the repository.
type ServerID int64

// UsenetServer is the aggregate root: one Usenet provider account.
//
// Fields are private so all mutation flows through methods that
// validate invariants and record domain events. Use New for
// construction (records ServerAdded) and Hydrate for repository
// rehydration (no events).
type UsenetServer struct {
	id        ServerID
	name      string
	host      string
	port      int
	tls       bool
	username  string
	password  string
	maxConns  int
	priority  int
	enabled   bool
	addedAt   time.Time
	updatedAt time.Time

	events []event.Event
}

// NewParams gathers required fields for New. Optional fields default
// to common-case values: TLS on, MaxConns 8, priority 0, enabled true.
type NewParams struct {
	Name     string
	Host     string
	Port     int
	TLS      *bool // pointer so "unset" is distinguishable from "false"
	Username string
	Password string
	MaxConns int
	Priority int
}

// New constructs a UsenetServer with validation. The returned aggregate
// has a pending ServerAdded event; call PullEvents after the repo Save
// succeeds.
//
// A zero ID is assigned at this stage; the repository sets the real ID
// on Save and updates the aggregate via SetID.
func New(p NewParams, now time.Time) (*UsenetServer, error) {
	name := strings.TrimSpace(p.Name)
	host := strings.TrimSpace(p.Host)
	if name == "" {
		return nil, errors.New("server: name required")
	}
	if host == "" {
		return nil, errors.New("server: host required")
	}
	if p.Port <= 0 || p.Port > 65535 {
		return nil, fmt.Errorf("server: port %d out of range", p.Port)
	}
	tls := true
	if p.TLS != nil {
		tls = *p.TLS
	}
	maxConns := p.MaxConns
	if maxConns == 0 {
		maxConns = 8
	}
	if maxConns < 1 {
		return nil, fmt.Errorf("server: max_conns %d must be > 0", maxConns)
	}
	if err := validateNNTPCredField("username", p.Username); err != nil {
		return nil, err
	}
	if err := validateNNTPCredField("password", p.Password); err != nil {
		return nil, err
	}

	s := &UsenetServer{
		name:      name,
		host:      host,
		port:      p.Port,
		tls:       tls,
		username:  p.Username,
		password:  p.Password,
		maxConns:  maxConns,
		priority:  p.Priority,
		enabled:   true,
		addedAt:   now,
		updatedAt: now,
	}
	s.events = append(s.events, ServerAdded{
		ID:   0, // repo fills in after Save
		Name: name,
		At:   now,
	})
	return s, nil
}

// HydrateParams is the snapshot the repository hands back when loading
// a row. Bypasses validation; the database is trusted.
type HydrateParams struct {
	ID        ServerID
	Name      string
	Host      string
	Port      int
	TLS       bool
	Username  string
	Password  string
	MaxConns  int
	Priority  int
	Enabled   bool
	AddedAt   time.Time
	UpdatedAt time.Time
}

// Hydrate reconstructs a UsenetServer from persistence. No events are
// emitted.
func Hydrate(p HydrateParams) *UsenetServer {
	return &UsenetServer{
		id:        p.ID,
		name:      p.Name,
		host:      p.Host,
		port:      p.Port,
		tls:       p.TLS,
		username:  p.Username,
		password:  p.Password,
		maxConns:  p.MaxConns,
		priority:  p.Priority,
		enabled:   p.Enabled,
		addedAt:   p.AddedAt,
		updatedAt: p.UpdatedAt,
	}
}

// Accessors.
func (s *UsenetServer) ID() ServerID       { return s.id }
func (s *UsenetServer) Name() string       { return s.name }
func (s *UsenetServer) Host() string       { return s.host }
func (s *UsenetServer) Port() int          { return s.port }
func (s *UsenetServer) TLS() bool          { return s.tls }
func (s *UsenetServer) Username() string   { return s.username }
func (s *UsenetServer) Password() string   { return s.password }
func (s *UsenetServer) MaxConns() int      { return s.maxConns }
func (s *UsenetServer) Priority() int      { return s.priority }
func (s *UsenetServer) Enabled() bool      { return s.enabled }
func (s *UsenetServer) AddedAt() time.Time { return s.addedAt }
func (s *UsenetServer) UpdatedAt() time.Time { return s.updatedAt }

// SetID is called by the repository after a successful Save when a
// fresh aggregate gets its database-assigned id. After SetID, any
// pending ServerAdded event is patched to carry the real id.
func (s *UsenetServer) SetID(id ServerID) {
	s.id = id
	for i := range s.events {
		if e, ok := s.events[i].(ServerAdded); ok && e.ID == 0 {
			e.ID = id
			s.events[i] = e
		}
	}
}

// SetEnabled toggles the active flag. Records a single event whose
// concrete type depends on the new state.
func (s *UsenetServer) SetEnabled(enabled bool, now time.Time) {
	if s.enabled == enabled {
		return
	}
	s.enabled = enabled
	s.updatedAt = now
	if enabled {
		s.events = append(s.events, ServerEnabled{ID: s.id, At: now})
	} else {
		s.events = append(s.events, ServerDisabled{ID: s.id, At: now})
	}
}

// UpdateParams selects which fields to mutate. Only non-nil fields are
// applied. Returns the same validation errors as New for the affected
// fields.
type UpdateParams struct {
	Host     *string
	Port     *int
	TLS      *bool
	Username *string
	Password *string
	MaxConns *int
	Priority *int
}

// Update applies a batch of field changes and records a single
// ServerUpdated event capturing the resulting state. No-op if every
// field is nil or matches the existing value.
func (s *UsenetServer) Update(p UpdateParams, now time.Time) error {
	changed := false
	if p.Host != nil {
		v := strings.TrimSpace(*p.Host)
		if v == "" {
			return errors.New("server: host must not be empty")
		}
		if v != s.host {
			s.host = v
			changed = true
		}
	}
	if p.Port != nil {
		if *p.Port <= 0 || *p.Port > 65535 {
			return fmt.Errorf("server: port %d out of range", *p.Port)
		}
		if *p.Port != s.port {
			s.port = *p.Port
			changed = true
		}
	}
	if p.TLS != nil && *p.TLS != s.tls {
		s.tls = *p.TLS
		changed = true
	}
	if p.Username != nil {
		if err := validateNNTPCredField("username", *p.Username); err != nil {
			return err
		}
		if *p.Username != s.username {
			s.username = *p.Username
			changed = true
		}
	}
	if p.Password != nil {
		if err := validateNNTPCredField("password", *p.Password); err != nil {
			return err
		}
		if *p.Password != s.password {
			s.password = *p.Password
			changed = true
		}
	}
	if p.MaxConns != nil {
		if *p.MaxConns < 1 {
			return fmt.Errorf("server: max_conns %d must be > 0", *p.MaxConns)
		}
		if *p.MaxConns != s.maxConns {
			s.maxConns = *p.MaxConns
			changed = true
		}
	}
	if p.Priority != nil && *p.Priority != s.priority {
		s.priority = *p.Priority
		changed = true
	}
	if changed {
		s.updatedAt = now
		s.events = append(s.events, ServerUpdated{ID: s.id, At: now})
	}
	return nil
}

// PullEvents returns and clears the pending event list. Application
// services call this after a successful repo Save and pass the result
// to bus.Publish.
func (s *UsenetServer) PullEvents() []event.Event {
	out := s.events
	s.events = nil
	return out
}

// validateNNTPCredField rejects credentials containing characters
// that could corrupt the NNTP wire protocol. NNTP commands are line-
// terminated by CRLF; a CR/LF in a username or password would let an
// operator (or attacker who can write to config) inject extra
// commands on the same connection.
//
// Allow empty (anonymous-access servers exist). Reject any byte that
// is a control character or NUL.
func validateNNTPCredField(name, v string) error {
	for i := 0; i < len(v); i++ {
		b := v[i]
		if b == '\r' || b == '\n' || b == 0 {
			return fmt.Errorf("server: %s contains control byte 0x%02x at offset %d", name, b, i)
		}
	}
	return nil
}
