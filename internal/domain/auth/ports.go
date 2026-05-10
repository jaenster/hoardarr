package auth

import (
	"context"
	"errors"
)

// UserRepository persists User aggregates.
type UserRepository interface {
	Save(ctx context.Context, u *User) error
	ByID(ctx context.Context, id UserID) (*User, error)
	ByUsername(ctx context.Context, username string) (*User, error)
	Count(ctx context.Context) (int, error)
}

// SessionStore persists Session values, keyed by Token.
type SessionStore interface {
	Put(ctx context.Context, s Session) error
	Get(ctx context.Context, token string) (Session, error)
	Touch(ctx context.Context, token string) error
	Delete(ctx context.Context, token string) error
	DeleteForUser(ctx context.Context, userID UserID) error
}

// PasswordHasher hashes plain-text passwords and verifies stored
// hashes. Implementations live in adapter packages
// (internal/adapter/bcrypt currently; argon2 may follow).
//
// Hash output is opaque text — implementations are expected to
// embed any algorithm parameters in the hash string itself
// (bcrypt's $2a$… format does this naturally).
type PasswordHasher interface {
	Hash(plain string) (string, error)
	Verify(hash, plain string) error
}

// Sentinel errors.
var (
	ErrUserNotFound        = errors.New("auth: user not found")
	ErrSessionNotFound     = errors.New("auth: session not found")
	ErrUsernameTaken       = errors.New("auth: username already taken")
	ErrInvalidCredentials  = errors.New("auth: invalid credentials")
	ErrSetupAlreadyDone    = errors.New("auth: setup already done")
	ErrSessionExpired      = errors.New("auth: session expired")
)
