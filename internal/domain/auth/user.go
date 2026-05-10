// Package auth is the bounded context for human-user accounts.
//
// Hoardarr supports two authentication modes side by side:
//
//   - API key (the existing config-driven secret): for *arr clients
//     and SAB-API consumers that can't do form-based auth. Header or
//     query param. Single-key, no users.
//   - User accounts (this package): for the web UI. Username +
//     password, sessions in HTTP-only cookies.
//
// Both modes terminate at the same /api/v1 endpoints — middleware
// accepts a valid API key OR a valid session.
//
// First-run flow: when the users table is empty, the API exposes a
// limited /api/v1/auth/setup endpoint that creates the first admin
// user. Once at least one user exists, /setup is gone and login is
// required.
package auth

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jaenster/hoardarr/internal/domain/event"
)

// UserID identifies a user. Allocated by the repository.
type UserID int64

// Role is the high-level permission tier. v0.1 has just admin and
// reserves the type for future tiers (read-only, etc).
type Role string

const (
	RoleAdmin Role = "admin"
)

// User is the aggregate root.
type User struct {
	id           UserID
	username     string
	passwordHash string
	role         Role
	createdAt    time.Time
	updatedAt    time.Time

	events []event.Event
}

// NewUserParams gathers inputs for User construction.
type NewUserParams struct {
	Username     string
	PasswordHash string // pre-hashed; the application service does the hashing
	Role         Role
}

// New constructs a fresh User. Records UserCreated. The repo back-fills
// the database id via SetID after Save.
func New(p NewUserParams, now time.Time) (*User, error) {
	if err := validateUsername(p.Username); err != nil {
		return nil, err
	}
	if p.PasswordHash == "" {
		return nil, errors.New("auth: password hash required")
	}
	if p.Role == "" {
		p.Role = RoleAdmin
	}
	u := &User{
		username:     strings.ToLower(strings.TrimSpace(p.Username)),
		passwordHash: p.PasswordHash,
		role:         p.Role,
		createdAt:    now,
		updatedAt:    now,
	}
	u.events = append(u.events, UserCreated{ID: 0, Username: u.username, Role: u.role, At: now})
	return u, nil
}

// HydrateParams is the snapshot the repository returns when loading.
type HydrateParams struct {
	ID           UserID
	Username     string
	PasswordHash string
	Role         Role
	CreatedAt    time.Time
	UpdatedAt    time.Time
}

// Hydrate reconstructs a User from persistence with no events.
func Hydrate(p HydrateParams) *User {
	return &User{
		id:           p.ID,
		username:     p.Username,
		passwordHash: p.PasswordHash,
		role:         p.Role,
		createdAt:    p.CreatedAt,
		updatedAt:    p.UpdatedAt,
	}
}

// Accessors.
func (u *User) ID() UserID            { return u.id }
func (u *User) Username() string      { return u.username }
func (u *User) PasswordHash() string  { return u.passwordHash }
func (u *User) Role() Role            { return u.role }
func (u *User) CreatedAt() time.Time  { return u.createdAt }
func (u *User) UpdatedAt() time.Time  { return u.updatedAt }

// SetID assigns a database id after insert. Patches the pending
// UserCreated event with the real id.
func (u *User) SetID(id UserID) {
	u.id = id
	for i := range u.events {
		if e, ok := u.events[i].(UserCreated); ok && e.ID == 0 {
			e.ID = id
			u.events[i] = e
		}
	}
}

// SetPasswordHash replaces the password hash. Records PasswordChanged.
func (u *User) SetPasswordHash(hash string, now time.Time) error {
	if hash == "" {
		return errors.New("auth: password hash must not be empty")
	}
	u.passwordHash = hash
	u.updatedAt = now
	u.events = append(u.events, PasswordChanged{ID: u.id, At: now})
	return nil
}

// PullEvents drains the pending event list.
func (u *User) PullEvents() []event.Event {
	out := u.events
	u.events = nil
	return out
}

// validateUsername enforces the few constraints worth having: non-empty
// after trim, no whitespace, no control characters, length cap.
// Lowercased on storage.
func validateUsername(raw string) error {
	v := strings.TrimSpace(raw)
	if v == "" {
		return errors.New("auth: username required")
	}
	if len(v) > 64 {
		return fmt.Errorf("auth: username too long (%d > 64)", len(v))
	}
	for i := 0; i < len(v); i++ {
		b := v[i]
		if b < 0x21 || b == 0x7f {
			return fmt.Errorf("auth: username contains control or whitespace byte 0x%02x at offset %d", b, i)
		}
	}
	return nil
}
