// Package download is the application layer for the download bounded
// context: AddJob use case and the in-process orchestrator that drives
// segment fetches end-to-end (NNTP → yEnc → disk).
package download

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/jaenster/hoardarr/internal/adapter/nntp"
	"github.com/jaenster/hoardarr/internal/domain/download"
	"github.com/jaenster/hoardarr/internal/domain/server"
)

// PoolFetcher adapts a single nntp.Pool to download.ArticleFetcher.
//
// M1 is single-server. M7 will introduce a multi-server fetcher that
// selects from a server registry by priority and falls through on 430;
// the same domain port covers both — only the impl changes.
type PoolFetcher struct {
	pool *nntp.Pool
}

// Compile-time check.
var _ download.ArticleFetcher = (*PoolFetcher)(nil)

// NewPoolFetcher wraps an nntp.Pool.
func NewPoolFetcher(pool *nntp.Pool) *PoolFetcher {
	return &PoolFetcher{pool: pool}
}

// Fetch acquires a conn from the pool, sends BODY <messageID>, and
// returns the body reader. The reader's Close releases the conn.
//
// On 430 the underlying *nntp.Conn is healthy; the conn is released
// to idle and the error wraps nntp.ErrArticleMissing.
func (f *PoolFetcher) Fetch(ctx context.Context, srv server.ServerID, messageID string) (io.ReadCloser, error) {
	if srv != f.pool.Server().ID() {
		return nil, fmt.Errorf("unknown server id %d (only %d available)", srv, f.pool.Server().ID())
	}

	conn, release, err := f.pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("acquire: %w", err)
	}

	body, err := conn.Body(ctx, messageID)
	if err != nil {
		// 430 → conn still healthy, return to pool. Other errors → conn
		// is potentially desynced, signal close.
		if errors.Is(err, nntp.ErrArticleMissing) {
			release(nil)
		} else {
			release(err)
		}
		return nil, err
	}
	return &releasingReader{ReadCloser: body, release: release}, nil
}

// releasingReader is an io.ReadCloser that forwards reads to body and
// releases the underlying conn on Close. Drain semantics: the wrapped
// body reader is responsible for draining itself; we just call its
// Close.
type releasingReader struct {
	io.ReadCloser
	release nntp.Release
	closed  bool
}

func (r *releasingReader) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true
	err := r.ReadCloser.Close()
	r.release(err)
	return err
}
