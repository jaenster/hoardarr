package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jaenster/hoardarr/internal/domain/notify"
)

// SubscriptionRepo persists notify.Subscription aggregates against
// the `subscriptions` table.
type SubscriptionRepo struct {
	db *DB
}

// Compile-time port check.
var _ notify.Repository = (*SubscriptionRepo)(nil)

// NewSubscriptionRepo wires the repo over db.
func NewSubscriptionRepo(db *DB) *SubscriptionRepo {
	return &SubscriptionRepo{db: db}
}

func (r *SubscriptionRepo) Save(ctx context.Context, s *notify.Subscription) error {
	if s.ID() == 0 {
		return r.insert(ctx, s)
	}
	return r.update(ctx, s)
}

func (r *SubscriptionRepo) insert(ctx context.Context, s *notify.Subscription) error {
	topicsJSON, err := json.Marshal(s.Topics())
	if err != nil {
		return fmt.Errorf("encode topics: %w", err)
	}
	res, err := r.db.ExecCtx(ctx, `
		INSERT INTO subscriptions(
			name, kind, url, topics, secret, enabled,
			last_success_at, last_error_at, last_error,
			created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`,
		s.Name(), string(s.Kind()), s.URL(), string(topicsJSON),
		nullableString(s.Secret()), boolToInt(s.Enabled()),
		nullableMillis(s.LastSuccessAt()), nullableMillis(s.LastErrorAt()),
		nullableString(s.LastError()),
		s.CreatedAt().UnixMilli(), s.UpdatedAt().UnixMilli(),
	)
	if err != nil {
		return fmt.Errorf("insert subscription: %w", err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		return fmt.Errorf("last insert id: %w", err)
	}
	s.SetID(notify.SubscriptionID(id))
	return nil
}

func (r *SubscriptionRepo) update(ctx context.Context, s *notify.Subscription) error {
	topicsJSON, err := json.Marshal(s.Topics())
	if err != nil {
		return fmt.Errorf("encode topics: %w", err)
	}
	res, err := r.db.ExecCtx(ctx, `
		UPDATE subscriptions
		SET name = ?, kind = ?, url = ?, topics = ?, secret = ?, enabled = ?,
			last_success_at = ?, last_error_at = ?, last_error = ?,
			updated_at = ?
		WHERE id = ?
	`,
		s.Name(), string(s.Kind()), s.URL(), string(topicsJSON),
		nullableString(s.Secret()), boolToInt(s.Enabled()),
		nullableMillis(s.LastSuccessAt()), nullableMillis(s.LastErrorAt()),
		nullableString(s.LastError()),
		s.UpdatedAt().UnixMilli(),
		int64(s.ID()),
	)
	if err != nil {
		return fmt.Errorf("update subscription: %w", err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return notify.ErrNotFound
	}
	return nil
}

func (r *SubscriptionRepo) ByID(ctx context.Context, id notify.SubscriptionID) (*notify.Subscription, error) {
	row := r.db.QueryRowCtx(ctx, selectSubscriptionByID, int64(id))
	return scanSubscription(row)
}

func (r *SubscriptionRepo) List(ctx context.Context) ([]*notify.Subscription, error) {
	rows, err := r.db.QueryCtx(ctx, selectAllSubscriptions)
	if err != nil {
		return nil, fmt.Errorf("query subscriptions: %w", err)
	}
	defer rows.Close()
	var out []*notify.Subscription
	for rows.Next() {
		s, err := scanSubscriptionFromRows(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

func (r *SubscriptionRepo) Delete(ctx context.Context, id notify.SubscriptionID) error {
	res, err := r.db.ExecCtx(ctx, `DELETE FROM subscriptions WHERE id = ?`, int64(id))
	if err != nil {
		return fmt.Errorf("delete subscription: %w", err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return notify.ErrNotFound
	}
	return nil
}

type subscriptionScanner interface {
	Scan(dest ...any) error
}

func scanSubscription(row *sql.Row) (*notify.Subscription, error) {
	s, err := scanSubscriptionFromRows(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, notify.ErrNotFound
	}
	return s, err
}

func scanSubscriptionFromRows(s subscriptionScanner) (*notify.Subscription, error) {
	var (
		id            int64
		name          string
		kind          string
		urlStr        string
		topicsJSON    string
		secret        sql.NullString
		enabled       int
		lastSuccess   sql.NullInt64
		lastError     sql.NullInt64
		lastErrorMsg  sql.NullString
		createdAt     int64
		updatedAt     int64
	)
	if err := s.Scan(&id, &name, &kind, &urlStr, &topicsJSON,
		&secret, &enabled,
		&lastSuccess, &lastError, &lastErrorMsg,
		&createdAt, &updatedAt); err != nil {
		return nil, err
	}
	var topics []string
	if err := json.Unmarshal([]byte(topicsJSON), &topics); err != nil {
		return nil, fmt.Errorf("decode topics: %w", err)
	}
	return notify.Hydrate(notify.HydrateParams{
		ID:            notify.SubscriptionID(id),
		Name:          name,
		Kind:          notify.Kind(kind),
		URL:           urlStr,
		Topics:        topics,
		Secret:        secret.String,
		Enabled:       enabled != 0,
		LastSuccessAt: nullableTime(lastSuccess),
		LastErrorAt:   nullableTime(lastError),
		LastError:     lastErrorMsg.String,
		CreatedAt:     time.UnixMilli(createdAt).UTC(),
		UpdatedAt:     time.UnixMilli(updatedAt).UTC(),
	}), nil
}

const subscriptionColumns = `id, name, kind, url, topics, secret, enabled,
	last_success_at, last_error_at, last_error,
	created_at, updated_at`

const selectSubscriptionByID = `SELECT ` + subscriptionColumns + ` FROM subscriptions WHERE id = ?`
const selectAllSubscriptions = `SELECT ` + subscriptionColumns + ` FROM subscriptions ORDER BY name ASC`
