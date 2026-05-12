package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"time"
)

// SettingsRepo is the key/value store for runtime-mutable settings.
// Callers go through the typed Get/Set helpers so the conversion +
// not-found handling is centralised.
type SettingsRepo struct {
	db *DB
}

// NewSettingsRepo wires the repo over db.
func NewSettingsRepo(db *DB) *SettingsRepo {
	return &SettingsRepo{db: db}
}

// ErrSettingNotFound — no row matches the key. The Get* helpers
// translate this to "use default" via the GetXxxOr variants.
var ErrSettingNotFound = errors.New("settings: not found")

// Get returns the raw stored value for key.
func (r *SettingsRepo) Get(ctx context.Context, key string) (string, error) {
	var v string
	err := r.db.QueryRowCtx(ctx, `SELECT value FROM settings WHERE key = ?`, key).Scan(&v)
	if errors.Is(err, sql.ErrNoRows) {
		return "", ErrSettingNotFound
	}
	if err != nil {
		return "", fmt.Errorf("get setting %s: %w", key, err)
	}
	return v, nil
}

// Set upserts a key/value pair.
func (r *SettingsRepo) Set(ctx context.Context, key, value string) error {
	_, err := r.db.ExecCtx(ctx, `
		INSERT INTO settings(key, value, updated_at) VALUES (?, ?, ?)
		ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at`,
		key, value, time.Now().UTC().UnixMilli())
	if err != nil {
		return fmt.Errorf("set setting %s: %w", key, err)
	}
	return nil
}

// GetStringOr returns the stored value or the default if missing.
func (r *SettingsRepo) GetStringOr(ctx context.Context, key, dflt string) (string, error) {
	v, err := r.Get(ctx, key)
	if errors.Is(err, ErrSettingNotFound) {
		return dflt, nil
	}
	return v, err
}

// GetIntOr returns the stored integer or the default if missing.
func (r *SettingsRepo) GetIntOr(ctx context.Context, key string, dflt int) (int, error) {
	v, err := r.Get(ctx, key)
	if errors.Is(err, ErrSettingNotFound) {
		return dflt, nil
	}
	if err != nil {
		return 0, err
	}
	n, perr := strconv.Atoi(v)
	if perr != nil {
		return 0, fmt.Errorf("parse setting %s as int: %w", key, perr)
	}
	return n, nil
}

// GetInt64Or returns the stored int64 or the default if missing.
func (r *SettingsRepo) GetInt64Or(ctx context.Context, key string, dflt int64) (int64, error) {
	v, err := r.Get(ctx, key)
	if errors.Is(err, ErrSettingNotFound) {
		return dflt, nil
	}
	if err != nil {
		return 0, err
	}
	n, perr := strconv.ParseInt(v, 10, 64)
	if perr != nil {
		return 0, fmt.Errorf("parse setting %s as int64: %w", key, perr)
	}
	return n, nil
}

// GetFloatOr returns the stored float64 or the default if missing.
func (r *SettingsRepo) GetFloatOr(ctx context.Context, key string, dflt float64) (float64, error) {
	v, err := r.Get(ctx, key)
	if errors.Is(err, ErrSettingNotFound) {
		return dflt, nil
	}
	if err != nil {
		return 0, err
	}
	f, perr := strconv.ParseFloat(v, 64)
	if perr != nil {
		return 0, fmt.Errorf("parse setting %s as float: %w", key, perr)
	}
	return f, nil
}

// GetBoolOr returns the stored bool or the default if missing.
// Accepts "1"/"0"/"true"/"false" (case-insensitive).
func (r *SettingsRepo) GetBoolOr(ctx context.Context, key string, dflt bool) (bool, error) {
	v, err := r.Get(ctx, key)
	if errors.Is(err, ErrSettingNotFound) {
		return dflt, nil
	}
	if err != nil {
		return false, err
	}
	switch v {
	case "1", "true", "TRUE", "True":
		return true, nil
	case "0", "false", "FALSE", "False":
		return false, nil
	}
	return false, fmt.Errorf("parse setting %s as bool: unexpected value %q", key, v)
}

// SetInt is the typed setter equivalent.
func (r *SettingsRepo) SetInt(ctx context.Context, key string, v int) error {
	return r.Set(ctx, key, strconv.Itoa(v))
}

// SetInt64 is the typed setter equivalent.
func (r *SettingsRepo) SetInt64(ctx context.Context, key string, v int64) error {
	return r.Set(ctx, key, strconv.FormatInt(v, 10))
}

// SetFloat is the typed setter equivalent.
func (r *SettingsRepo) SetFloat(ctx context.Context, key string, v float64) error {
	return r.Set(ctx, key, strconv.FormatFloat(v, 'g', -1, 64))
}

// SetBool is the typed setter equivalent. Stored as "0"/"1" so a
// human poking at the DB can read it without context.
func (r *SettingsRepo) SetBool(ctx context.Context, key string, v bool) error {
	if v {
		return r.Set(ctx, key, "1")
	}
	return r.Set(ctx, key, "0")
}
