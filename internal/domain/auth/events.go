package auth

import (
	"strconv"
	"time"
)

// Topic prefix for the auth bounded context.
const TopicPrefix = "auth."

func aggID(id UserID) string { return strconv.FormatInt(int64(id), 10) }

// UserCreated — emitted when the first admin is created (first-run)
// or any subsequent user is added.
type UserCreated struct {
	ID       UserID    `json:"id"`
	Username string    `json:"username"`
	Role     Role      `json:"role"`
	At       time.Time `json:"at"`
}

func (e UserCreated) Topic() string         { return TopicPrefix + "user.created" }
func (e UserCreated) AggregateID() string   { return aggID(e.ID) }
func (e UserCreated) OccurredAt() time.Time { return e.At }

// PasswordChanged — user changed their password (or had it changed).
type PasswordChanged struct {
	ID UserID    `json:"id"`
	At time.Time `json:"at"`
}

func (e PasswordChanged) Topic() string         { return TopicPrefix + "user.password_changed" }
func (e PasswordChanged) AggregateID() string   { return aggID(e.ID) }
func (e PasswordChanged) OccurredAt() time.Time { return e.At }

// LoggedIn — successful authentication produced a fresh session.
type LoggedIn struct {
	UserID UserID    `json:"user_id"`
	At     time.Time `json:"at"`
}

func (e LoggedIn) Topic() string         { return TopicPrefix + "logged_in" }
func (e LoggedIn) AggregateID() string   { return aggID(e.UserID) }
func (e LoggedIn) OccurredAt() time.Time { return e.At }

// LoggedOut — session was explicitly invalidated (vs expired).
type LoggedOut struct {
	UserID UserID    `json:"user_id"`
	At     time.Time `json:"at"`
}

func (e LoggedOut) Topic() string         { return TopicPrefix + "logged_out" }
func (e LoggedOut) AggregateID() string   { return aggID(e.UserID) }
func (e LoggedOut) OccurredAt() time.Time { return e.At }
