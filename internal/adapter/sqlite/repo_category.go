package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
)

// ErrCategoryNotFound is returned by Get/Delete when no row matches.
var ErrCategoryNotFound = errors.New("category not found")

// ErrCategoryNameTaken is returned by Save when the PK is already used.
var ErrCategoryNameTaken = errors.New("category name already exists")

// ErrCategoryNameInvalid is returned by Save when the name fails the
// shape check (empty, whitespace, control chars, or path separators).
var ErrCategoryNameInvalid = errors.New("category name invalid")

// ErrCategoryReserved is returned by Delete when the caller targets
// the literal '*' default. SAB-compat consumers depend on it always
// being present, so removal is forbidden.
var ErrCategoryReserved = errors.New("category is reserved")

// Category is the read-side row from the `categories` table. CRUD is
// not yet implemented; the table is currently seeded with the default
// '*' (uncategorized) row by migration 002 and grows when categories
// are added via the SAB API or future settings UI.
type Category struct {
	Name     string
	Dir      string
	Priority int
}

// CategoryRepo provides read-only access to categories for the REST
// API and SAB-compat shim. Mutating methods land when category
// management lands in the UI.
type CategoryRepo struct {
	db *DB
}

// NewCategoryRepo wires the repo over db.
func NewCategoryRepo(db *DB) *CategoryRepo {
	return &CategoryRepo{db: db}
}

// List returns categories ordered by priority then name.
func (r *CategoryRepo) List(ctx context.Context) ([]Category, error) {
	rows, err := r.db.QueryCtx(ctx, `
		SELECT name, dir, priority FROM categories
		ORDER BY priority ASC, name ASC
	`)
	if err != nil {
		return nil, fmt.Errorf("query categories: %w", err)
	}
	defer rows.Close()
	var out []Category
	for rows.Next() {
		var c Category
		var dir sql.NullString
		if err := rows.Scan(&c.Name, &dir, &c.Priority); err != nil {
			return nil, err
		}
		c.Dir = dir.String
		out = append(out, c)
	}
	return out, rows.Err()
}

// Save inserts a new category or updates an existing one (UPSERT on
// name). Validates the name and dir for shape; the caller is expected
// to have already trimmed whitespace.
func (r *CategoryRepo) Save(ctx context.Context, c Category) error {
	if err := validateCategoryName(c.Name); err != nil {
		return err
	}
	if err := validateCategoryDir(c.Dir); err != nil {
		return err
	}
	now := time.Now().UTC().UnixMilli()
	_, err := r.db.ExecCtx(ctx, `
		INSERT INTO categories(name, dir, priority, added_at, updated_at)
		VALUES (?, ?, ?, ?, ?)
		ON CONFLICT(name) DO UPDATE SET
			dir = excluded.dir,
			priority = excluded.priority,
			updated_at = excluded.updated_at
	`, c.Name, c.Dir, c.Priority, now, now)
	if err != nil {
		return fmt.Errorf("save category: %w", err)
	}
	return nil
}

// Delete removes a category by name. The literal '*' default is
// reserved and cannot be deleted (SAB-compat depends on it).
func (r *CategoryRepo) Delete(ctx context.Context, name string) error {
	if name == "*" {
		return ErrCategoryReserved
	}
	res, err := r.db.ExecCtx(ctx, `DELETE FROM categories WHERE name = ?`, name)
	if err != nil {
		return fmt.Errorf("delete category: %w", err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrCategoryNotFound
	}
	return nil
}

// validateCategoryName rejects names that would confuse the SAB API,
// path-builders, or the on-disk layout. The literal '*' is allowed
// because it's the SAB-compat sentinel.
func validateCategoryName(name string) error {
	if name == "*" {
		return nil
	}
	if name == "" {
		return fmt.Errorf("%w: empty", ErrCategoryNameInvalid)
	}
	if len(name) > 64 {
		return fmt.Errorf("%w: too long", ErrCategoryNameInvalid)
	}
	if strings.TrimSpace(name) != name {
		return fmt.Errorf("%w: leading/trailing whitespace", ErrCategoryNameInvalid)
	}
	for _, r := range name {
		if r < 0x20 || r == 0x7f {
			return fmt.Errorf("%w: control char", ErrCategoryNameInvalid)
		}
		switch r {
		case '/', '\\', ':':
			return fmt.Errorf("%w: path separator %q", ErrCategoryNameInvalid, r)
		}
	}
	return nil
}

// validateCategoryDir keeps category dirs relative — never absolute,
// never escaping the complete-dir root via .. — so deliveries can't
// be redirected outside the configured tree.
func validateCategoryDir(dir string) error {
	if dir == "" {
		return nil
	}
	if strings.HasPrefix(dir, "/") || strings.HasPrefix(dir, `\`) {
		return fmt.Errorf("%w: dir must be relative", ErrCategoryNameInvalid)
	}
	for _, seg := range strings.FieldsFunc(dir, func(r rune) bool { return r == '/' || r == '\\' }) {
		if seg == ".." {
			return fmt.Errorf("%w: dir traversal not allowed", ErrCategoryNameInvalid)
		}
	}
	return nil
}
