package sqlite

import (
	"context"
	"database/sql"
	"fmt"
)

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
