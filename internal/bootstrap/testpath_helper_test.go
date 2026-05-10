package bootstrap_test

// Shared test helpers — must be available to all test files,
// regardless of build tags.

import (
	"errors"
	"os"
	"path/filepath"
)

// resolveTestPath returns path unchanged when absolute. Relative paths
// are resolved against the project root (the nearest ancestor of cwd
// containing go.mod) so values in .env or default fixture locations
// work regardless of which package directory `go test` was invoked from.
func resolveTestPath(path string) (string, error) {
	if path == "" {
		return "", errors.New("empty path")
	}
	if filepath.IsAbs(path) {
		return path, nil
	}
	root, err := projectRoot()
	if err != nil {
		return "", err
	}
	return filepath.Join(root, path), nil
}

// projectRoot walks up from cwd until it finds a directory containing
// go.mod. Returns an error if none is found.
func projectRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", errors.New("project root not found (no go.mod ancestor)")
		}
		dir = parent
	}
}
