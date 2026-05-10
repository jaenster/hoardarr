// Package bcrypt provides a PasswordHasher implementation backed by
// golang.org/x/crypto/bcrypt.
//
// Cost: defaults to bcrypt.DefaultCost (10), which on commodity
// hardware costs ~50–100ms per hash — slow enough to deter brute
// force, fast enough that login latency is acceptable. v0.1 uses the
// default; if profiling shows login is too slow on tiny ARM SBCs,
// we'll add a config knob.
//
// We use bcrypt rather than argon2 for v0.1 because bcrypt is in
// golang.org/x/crypto already (no new dep), the format is widely
// understood, and the security difference is academic for a
// self-hosted single-user tool. Migration path: detect hash prefix
// in the future hasher and rehash on next successful login.
package bcrypt

import (
	"errors"
	"fmt"

	"github.com/jaenster/hoardarr/internal/domain/auth"
	xbcrypt "golang.org/x/crypto/bcrypt"
)

// Hasher implements auth.PasswordHasher.
type Hasher struct {
	// Cost is the bcrypt cost factor (4..31). Zero means default.
	Cost int
}

// Compile-time check.
var _ auth.PasswordHasher = (*Hasher)(nil)

// Hash returns the bcrypt-encoded hash of plain. The output is the
// standard $2a$cost$… string and embeds all the parameters needed
// for verification.
func (h *Hasher) Hash(plain string) (string, error) {
	if plain == "" {
		return "", errors.New("bcrypt: empty password")
	}
	cost := h.Cost
	if cost == 0 {
		cost = xbcrypt.DefaultCost
	}
	out, err := xbcrypt.GenerateFromPassword([]byte(plain), cost)
	if err != nil {
		return "", fmt.Errorf("bcrypt: %w", err)
	}
	return string(out), nil
}

// Verify reports nil iff plain hashes to hash. Returns
// auth.ErrInvalidCredentials on mismatch (the application service maps
// other errors to a generic "invalid credentials" too — never leak
// whether the user exists vs the password is wrong).
func (h *Hasher) Verify(hash, plain string) error {
	if err := xbcrypt.CompareHashAndPassword([]byte(hash), []byte(plain)); err != nil {
		if errors.Is(err, xbcrypt.ErrMismatchedHashAndPassword) {
			return auth.ErrInvalidCredentials
		}
		return fmt.Errorf("bcrypt verify: %w", err)
	}
	return nil
}
