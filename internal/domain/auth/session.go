package auth

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"time"
)

// Session is one authenticated browser session. The Token is what the
// client presents (via cookie); it's a 256-bit cryptographic random
// hex-encoded so it's URL/cookie safe.
//
// Sessions are looked up by Token. Compromise of one session token
// doesn't help the attacker get others (no derivation chain).
type Session struct {
	Token     string
	UserID    UserID
	CreatedAt time.Time
	ExpiresAt time.Time
	LastSeen  time.Time
}

// NewSessionParams constructs a fresh Session.
type NewSessionParams struct {
	UserID UserID
	TTL    time.Duration
	Now    time.Time
}

// NewSession generates a fresh session with a freshly-random token.
func NewSession(p NewSessionParams) (Session, error) {
	if p.UserID == 0 {
		return Session{}, errors.New("auth: session needs UserID")
	}
	if p.TTL <= 0 {
		p.TTL = 7 * 24 * time.Hour
	}
	if p.Now.IsZero() {
		p.Now = time.Now().UTC()
	}
	tok, err := newSessionToken()
	if err != nil {
		return Session{}, err
	}
	return Session{
		Token:     tok,
		UserID:    p.UserID,
		CreatedAt: p.Now,
		ExpiresAt: p.Now.Add(p.TTL),
		LastSeen:  p.Now,
	}, nil
}

// IsValid reports whether the session is still active at now.
func (s Session) IsValid(now time.Time) bool {
	return !s.ExpiresAt.IsZero() && now.Before(s.ExpiresAt)
}

// newSessionToken returns 32 random bytes hex-encoded (64 chars).
func newSessionToken() (string, error) {
	var b [32]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}
