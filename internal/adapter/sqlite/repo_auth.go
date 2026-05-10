package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jaenster/hoardarr/internal/domain/auth"
)

// UserRepo implements auth.UserRepository against SQLite.
type UserRepo struct {
	db *DB
}

// Compile-time check.
var _ auth.UserRepository = (*UserRepo)(nil)

// NewUserRepo wires the repo over db.
func NewUserRepo(db *DB) *UserRepo { return &UserRepo{db: db} }

// Save inserts a new row when ID == 0, updates otherwise.
//
// On insert, a UNIQUE-violation on username is mapped to
// ErrUsernameTaken so the application service can surface it cleanly
// (avoids leaking SQL error strings).
func (r *UserRepo) Save(ctx context.Context, u *auth.User) error {
	if u.ID() == 0 {
		return r.insert(ctx, u)
	}
	return r.update(ctx, u)
}

func (r *UserRepo) insert(ctx context.Context, u *auth.User) error {
	res, err := r.db.ExecCtx(ctx, `
		INSERT INTO users(username, password_hash, role, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?)
	`,
		strings.ToLower(u.Username()), u.PasswordHash(), string(u.Role()),
		u.CreatedAt().UnixMilli(), u.UpdatedAt().UnixMilli(),
	)
	if err != nil {
		if isUsernameUniqueViolation(err) {
			return auth.ErrUsernameTaken
		}
		return fmt.Errorf("insert user: %w", err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		return fmt.Errorf("last_id: %w", err)
	}
	u.SetID(auth.UserID(id))
	return nil
}

func (r *UserRepo) update(ctx context.Context, u *auth.User) error {
	_, err := r.db.ExecCtx(ctx, `
		UPDATE users SET
			password_hash = ?, role = ?, updated_at = ?
		WHERE id = ?
	`,
		u.PasswordHash(), string(u.Role()), u.UpdatedAt().UnixMilli(),
		int64(u.ID()),
	)
	if err != nil {
		return fmt.Errorf("update user: %w", err)
	}
	return nil
}

// ByID loads by primary key.
func (r *UserRepo) ByID(ctx context.Context, id auth.UserID) (*auth.User, error) {
	row := r.db.QueryRowCtx(ctx, selectUserByID, int64(id))
	return scanUser(row)
}

// ByUsername loads by username (case-insensitive — usernames are
// stored lowercased).
func (r *UserRepo) ByUsername(ctx context.Context, username string) (*auth.User, error) {
	row := r.db.QueryRowCtx(ctx, selectUserByUsername, strings.ToLower(strings.TrimSpace(username)))
	return scanUser(row)
}

// Count returns the total user count. Used by first-run detection.
func (r *UserRepo) Count(ctx context.Context) (int, error) {
	var n int
	if err := r.db.QueryRowCtx(ctx, `SELECT COUNT(*) FROM users`).Scan(&n); err != nil {
		return 0, err
	}
	return n, nil
}

const userColumns = `id, username, password_hash, role, created_at, updated_at`

const selectUserByID = `SELECT ` + userColumns + ` FROM users WHERE id = ?`
const selectUserByUsername = `SELECT ` + userColumns + ` FROM users WHERE username = ?`

func scanUser(row *sql.Row) (*auth.User, error) {
	var (
		id        int64
		username  string
		hash      string
		role      string
		createdAt int64
		updatedAt int64
	)
	if err := row.Scan(&id, &username, &hash, &role, &createdAt, &updatedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, auth.ErrUserNotFound
		}
		return nil, err
	}
	return auth.Hydrate(auth.HydrateParams{
		ID:           auth.UserID(id),
		Username:     username,
		PasswordHash: hash,
		Role:         auth.Role(role),
		CreatedAt:    time.UnixMilli(createdAt).UTC(),
		UpdatedAt:    time.UnixMilli(updatedAt).UTC(),
	}), nil
}

func isUsernameUniqueViolation(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "UNIQUE constraint failed: users.username")
}

// SessionRepo implements auth.SessionStore against SQLite.
type SessionRepo struct {
	db *DB
}

// Compile-time check.
var _ auth.SessionStore = (*SessionRepo)(nil)

// NewSessionRepo wires the repo over db.
func NewSessionRepo(db *DB) *SessionRepo { return &SessionRepo{db: db} }

// Put inserts a fresh session row.
func (r *SessionRepo) Put(ctx context.Context, s auth.Session) error {
	_, err := r.db.ExecCtx(ctx, `
		INSERT INTO sessions(token, user_id, created_at, expires_at, last_seen)
		VALUES (?, ?, ?, ?, ?)
	`,
		s.Token, int64(s.UserID),
		s.CreatedAt.UnixMilli(), s.ExpiresAt.UnixMilli(), s.LastSeen.UnixMilli(),
	)
	if err != nil {
		return fmt.Errorf("insert session: %w", err)
	}
	return nil
}

// Get loads a session by token. Returns ErrSessionNotFound if absent.
func (r *SessionRepo) Get(ctx context.Context, token string) (auth.Session, error) {
	var (
		userID    int64
		createdAt int64
		expiresAt int64
		lastSeen  int64
	)
	err := r.db.QueryRowCtx(ctx,
		`SELECT user_id, created_at, expires_at, last_seen FROM sessions WHERE token = ?`, token,
	).Scan(&userID, &createdAt, &expiresAt, &lastSeen)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return auth.Session{}, auth.ErrSessionNotFound
		}
		return auth.Session{}, err
	}
	return auth.Session{
		Token:     token,
		UserID:    auth.UserID(userID),
		CreatedAt: time.UnixMilli(createdAt).UTC(),
		ExpiresAt: time.UnixMilli(expiresAt).UTC(),
		LastSeen:  time.UnixMilli(lastSeen).UTC(),
	}, nil
}

// Touch updates last_seen to now. Best-effort — failures aren't fatal
// to the request, just signals to the cleanup job.
func (r *SessionRepo) Touch(ctx context.Context, token string) error {
	_, err := r.db.ExecCtx(ctx,
		`UPDATE sessions SET last_seen = ? WHERE token = ?`,
		time.Now().UnixMilli(), token)
	return err
}

// Delete removes a single session (logout).
func (r *SessionRepo) Delete(ctx context.Context, token string) error {
	_, err := r.db.ExecCtx(ctx, `DELETE FROM sessions WHERE token = ?`, token)
	return err
}

// DeleteForUser removes every session for a user (e.g. on password
// change, or "log me out everywhere").
func (r *SessionRepo) DeleteForUser(ctx context.Context, userID auth.UserID) error {
	_, err := r.db.ExecCtx(ctx, `DELETE FROM sessions WHERE user_id = ?`, int64(userID))
	return err
}
