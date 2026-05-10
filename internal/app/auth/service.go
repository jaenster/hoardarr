// Package auth provides application services for the auth bounded
// context: SetupAdmin (first-run), Login, Logout, GetSession.
//
// Notes:
//
//   - Login deliberately returns the same ErrInvalidCredentials for
//     both "no such user" and "wrong password" — never leak which.
//   - Setup is gated by user count; once any user exists, Setup
//     returns ErrSetupAlreadyDone. The REST layer additionally hides
//     the /setup endpoint when count > 0 so curious browsers don't
//     even see a form.
package auth

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jaenster/hoardarr/internal/domain/auth"
	"github.com/jaenster/hoardarr/internal/domain/event"
	"github.com/jaenster/hoardarr/internal/domain/tx"
)

// Service composes the dependencies for auth use cases.
type Service struct {
	users    auth.UserRepository
	sessions auth.SessionStore
	hasher   auth.PasswordHasher
	bus      event.Bus
	txm      tx.TransactionManager
	now      func() time.Time

	sessionTTL time.Duration
}

// ServiceParams gathers Service dependencies.
type ServiceParams struct {
	Users      auth.UserRepository
	Sessions   auth.SessionStore
	Hasher     auth.PasswordHasher
	Bus        event.Bus
	TxManager  tx.TransactionManager
	Now        func() time.Time
	SessionTTL time.Duration // optional, default 7 days
}

// New constructs the Service.
func New(p ServiceParams) *Service {
	if p.Now == nil {
		p.Now = func() time.Time { return time.Now().UTC() }
	}
	if p.SessionTTL == 0 {
		p.SessionTTL = 7 * 24 * time.Hour
	}
	return &Service{
		users:      p.Users,
		sessions:   p.Sessions,
		hasher:     p.Hasher,
		bus:        p.Bus,
		txm:        p.TxManager,
		now:        p.Now,
		sessionTTL: p.SessionTTL,
	}
}

// NeedsSetup reports whether no users exist yet (first-run state).
// REST handlers use this to gate the /api/v1/auth/setup endpoint.
func (s *Service) NeedsSetup(ctx context.Context) (bool, error) {
	n, err := s.users.Count(ctx)
	if err != nil {
		return false, err
	}
	return n == 0, nil
}

// SetupAdmin creates the first admin user. Returns ErrSetupAlreadyDone
// if any user already exists. Concurrent SetupAdmin calls are
// serialised by the Save UNIQUE-username constraint — the second one
// will see ErrUsernameTaken and convert to a "setup race" error.
func (s *Service) SetupAdmin(ctx context.Context, username, password string) (auth.UserID, error) {
	var userID auth.UserID
	err := s.txm.InTx(ctx, func(ctx context.Context) error {
		n, err := s.users.Count(ctx)
		if err != nil {
			return err
		}
		if n > 0 {
			return auth.ErrSetupAlreadyDone
		}
		hash, err := s.hasher.Hash(password)
		if err != nil {
			return fmt.Errorf("hash: %w", err)
		}
		u, err := auth.New(auth.NewUserParams{
			Username:     username,
			PasswordHash: hash,
			Role:         auth.RoleAdmin,
		}, s.now())
		if err != nil {
			return err
		}
		if err := s.users.Save(ctx, u); err != nil {
			if errors.Is(err, auth.ErrUsernameTaken) {
				// Another setup request won; treat as already-done.
				return auth.ErrSetupAlreadyDone
			}
			return err
		}
		userID = u.ID()
		return s.bus.Publish(ctx, u.PullEvents()...)
	})
	if err != nil {
		return 0, err
	}
	return userID, nil
}

// Login verifies credentials and returns a fresh Session. On invalid
// credentials returns ErrInvalidCredentials (without distinguishing
// "no such user" from "wrong password").
func (s *Service) Login(ctx context.Context, username, password string) (auth.Session, error) {
	u, err := s.users.ByUsername(ctx, username)
	if err != nil {
		if errors.Is(err, auth.ErrUserNotFound) {
			return auth.Session{}, auth.ErrInvalidCredentials
		}
		return auth.Session{}, err
	}
	if err := s.hasher.Verify(u.PasswordHash(), password); err != nil {
		return auth.Session{}, auth.ErrInvalidCredentials
	}
	sess, err := auth.NewSession(auth.NewSessionParams{
		UserID: u.ID(),
		TTL:    s.sessionTTL,
		Now:    s.now(),
	})
	if err != nil {
		return auth.Session{}, fmt.Errorf("new session: %w", err)
	}
	if err := s.txm.InTx(ctx, func(ctx context.Context) error {
		if err := s.sessions.Put(ctx, sess); err != nil {
			return err
		}
		return s.bus.Publish(ctx, auth.LoggedIn{UserID: u.ID(), At: s.now()})
	}); err != nil {
		return auth.Session{}, err
	}
	return sess, nil
}

// Logout invalidates a session by token. Idempotent — no error if the
// token is unknown (rate-limit info leak; just delete and move on).
func (s *Service) Logout(ctx context.Context, token string) error {
	// Best-effort fetch so we can emit LoggedOut with the right user id.
	sess, err := s.sessions.Get(ctx, token)
	if err != nil && !errors.Is(err, auth.ErrSessionNotFound) {
		return err
	}
	return s.txm.InTx(ctx, func(ctx context.Context) error {
		if err := s.sessions.Delete(ctx, token); err != nil {
			return err
		}
		if sess.UserID != 0 {
			return s.bus.Publish(ctx, auth.LoggedOut{UserID: sess.UserID, At: s.now()})
		}
		return nil
	})
}

// AuthenticateRequest validates a session token and returns the
// associated user. Used by HTTP middleware. Returns ErrSessionExpired
// for stale sessions, ErrSessionNotFound for unknown tokens.
func (s *Service) AuthenticateRequest(ctx context.Context, token string) (*auth.User, error) {
	sess, err := s.sessions.Get(ctx, token)
	if err != nil {
		return nil, err
	}
	if !sess.IsValid(s.now()) {
		_ = s.sessions.Delete(ctx, token)
		return nil, auth.ErrSessionExpired
	}
	u, err := s.users.ByID(ctx, sess.UserID)
	if err != nil {
		return nil, err
	}
	// Best-effort touch — failures don't break the request.
	_ = s.sessions.Touch(ctx, token)
	return u, nil
}
