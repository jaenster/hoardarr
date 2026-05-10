package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/jaenster/hoardarr/internal/domain/server"
)

// ServerRepo implements domain/server.Repository against SQLite.
type ServerRepo struct {
	db *DB
}

// Compile-time check.
var _ server.Repository = (*ServerRepo)(nil)

// NewServerRepo wires the repository over db.
func NewServerRepo(db *DB) *ServerRepo {
	return &ServerRepo{db: db}
}

// Save inserts a new row when ID == 0 (and back-fills the id via
// SetID), or updates an existing row otherwise.
//
// The serialization is tx-aware: if ctx carries an active *sql.Tx it
// is used; otherwise a single-statement auto-commit runs.
func (r *ServerRepo) Save(ctx context.Context, s *server.UsenetServer) error {
	if s.ID() == 0 {
		return r.insert(ctx, s)
	}
	return r.update(ctx, s)
}

func (r *ServerRepo) insert(ctx context.Context, s *server.UsenetServer) error {
	res, err := r.db.ExecCtx(ctx, `
		INSERT INTO servers(
			name, host, port, tls, username, password,
			max_conns, priority, enabled, added_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`,
		s.Name(), s.Host(), s.Port(), boolToInt(s.TLS()), nullableString(s.Username()), nullableString(s.Password()),
		s.MaxConns(), s.Priority(), boolToInt(s.Enabled()),
		s.AddedAt().UnixMilli(), s.UpdatedAt().UnixMilli(),
	)
	if err != nil {
		return fmt.Errorf("insert server: %w", err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		return fmt.Errorf("last insert id: %w", err)
	}
	s.SetID(server.ServerID(id))
	return nil
}

func (r *ServerRepo) update(ctx context.Context, s *server.UsenetServer) error {
	res, err := r.db.ExecCtx(ctx, `
		UPDATE servers SET
			name = ?, host = ?, port = ?, tls = ?, username = ?, password = ?,
			max_conns = ?, priority = ?, enabled = ?, updated_at = ?
		WHERE id = ?
	`,
		s.Name(), s.Host(), s.Port(), boolToInt(s.TLS()), nullableString(s.Username()), nullableString(s.Password()),
		s.MaxConns(), s.Priority(), boolToInt(s.Enabled()),
		s.UpdatedAt().UnixMilli(),
		int64(s.ID()),
	)
	if err != nil {
		return fmt.Errorf("update server: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("rows affected: %w", err)
	}
	if n == 0 {
		return server.ErrNotFound
	}
	return nil
}

// ByID returns the server with the given id, or ErrNotFound.
func (r *ServerRepo) ByID(ctx context.Context, id server.ServerID) (*server.UsenetServer, error) {
	row := r.db.QueryRowCtx(ctx, selectServerByID, int64(id))
	return scanServer(row)
}

// ByName returns the server with the given (unique) display name.
func (r *ServerRepo) ByName(ctx context.Context, name string) (*server.UsenetServer, error) {
	row := r.db.QueryRowCtx(ctx, selectServerByName, name)
	return scanServer(row)
}

// List returns all servers ordered by priority ascending then id.
func (r *ServerRepo) List(ctx context.Context) ([]*server.UsenetServer, error) {
	return r.queryServers(ctx, selectAllServers)
}

// ListEnabled returns enabled servers ordered by priority ascending,
// id ascending. Used by the download orchestrator's dispatch.
func (r *ServerRepo) ListEnabled(ctx context.Context) ([]*server.UsenetServer, error) {
	return r.queryServers(ctx, selectEnabledServers)
}

// Delete removes the row by id. Returns ErrNotFound if id does not
// exist.
func (r *ServerRepo) Delete(ctx context.Context, id server.ServerID) error {
	res, err := r.db.ExecCtx(ctx, `DELETE FROM servers WHERE id = ?`, int64(id))
	if err != nil {
		return fmt.Errorf("delete server: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("rows affected: %w", err)
	}
	if n == 0 {
		return server.ErrNotFound
	}
	return nil
}

func (r *ServerRepo) queryServers(ctx context.Context, query string, args ...any) ([]*server.UsenetServer, error) {
	rows, err := r.db.QueryCtx(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query servers: %w", err)
	}
	defer rows.Close()
	var out []*server.UsenetServer
	for rows.Next() {
		s, err := scanServerFromRows(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

const serverColumns = `id, name, host, port, tls, username, password, max_conns, priority, enabled, added_at, updated_at`

const selectServerByID = `SELECT ` + serverColumns + ` FROM servers WHERE id = ?`
const selectServerByName = `SELECT ` + serverColumns + ` FROM servers WHERE name = ?`
const selectAllServers = `SELECT ` + serverColumns + ` FROM servers ORDER BY priority ASC, id ASC`
const selectEnabledServers = `SELECT ` + serverColumns + ` FROM servers WHERE enabled = 1 ORDER BY priority ASC, id ASC`

type serverScanner interface {
	Scan(dest ...any) error
}

func scanServer(row *sql.Row) (*server.UsenetServer, error) {
	s, err := scanServerFromRows(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, server.ErrNotFound
	}
	return s, err
}

func scanServerFromRows(s serverScanner) (*server.UsenetServer, error) {
	var (
		id        int64
		name      string
		host      string
		port      int
		tls       int
		username  sql.NullString
		password  sql.NullString
		maxConns  int
		priority  int
		enabled   int
		added     int64
		updated   int64
	)
	if err := s.Scan(&id, &name, &host, &port, &tls, &username, &password, &maxConns, &priority, &enabled, &added, &updated); err != nil {
		return nil, err
	}
	return server.Hydrate(server.HydrateParams{
		ID:        server.ServerID(id),
		Name:      name,
		Host:      host,
		Port:      port,
		TLS:       tls != 0,
		Username:  username.String,
		Password:  password.String,
		MaxConns:  maxConns,
		Priority:  priority,
		Enabled:   enabled != 0,
		AddedAt:   time.UnixMilli(added).UTC(),
		UpdatedAt: time.UnixMilli(updated).UTC(),
	}), nil
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

func nullableString(s string) any {
	if s == "" {
		return nil
	}
	return s
}
