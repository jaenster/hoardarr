// Package nntptest probes an NNTP server's connectivity end-to-end:
// dial → greeting → AUTHINFO USER/PASS → MODE READER → DATE → QUIT.
// Each step is reported individually so the operator can see how far
// the handshake got — "auth succeeded but MODE READER failed" is
// useful diagnostic context that a single ok/err boolean would hide.
//
// This is the back-end of the "Test connection" button in Settings →
// Servers. It's deliberately a separate package from adapter/nntp so
// the production fetch path stays free of test-only ceremony.
package nntptest

import (
	"context"
	"time"

	"github.com/jaenster/hoardarr/internal/adapter/nntp"
	"github.com/jaenster/hoardarr/internal/domain/server"
)

// Params is what the operator filled in on the form (or what we
// already have stored for an existing server).
type Params struct {
	Host     string
	Port     int
	TLS      bool
	Username string
	Password string
}

// Result reports each handshake step independently. OK is true only
// when every step the operator's creds imply succeeded (Auth is
// skipped if Username is blank).
type Result struct {
	OK         bool
	Dial       bool
	Greeted    bool
	Auth       bool
	ModeReader bool
	Date       bool
	ServerDate string
	Err        string
	Elapsed    time.Duration
}

// Probe runs the handshake. It enforces an 8-second outer deadline on
// top of any deadline already on ctx — a misconfigured host shouldn't
// block the HTTP handler for the full TCP timeout.
func Probe(ctx context.Context, p Params) Result {
	start := time.Now()
	res := Result{}

	probeCtx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()

	srv := server.Hydrate(server.HydrateParams{
		ID:       0,
		Name:     "probe",
		Host:     p.Host,
		Port:     p.Port,
		TLS:      p.TLS,
		Username: p.Username,
		Password: p.Password,
		MaxConns: 1,
		Enabled:  true,
		AddedAt:  time.Now().UTC(),
	})

	conn, err := nntp.Dial(probeCtx, srv)
	if err != nil {
		res.Err = err.Error()
		res.Elapsed = time.Since(start)
		return res
	}
	res.Dial = true
	res.Greeted = true
	defer func() {
		_ = conn.Quit(probeCtx)
		_ = conn.Close()
	}()

	if p.Username != "" {
		if err := conn.Authenticate(probeCtx); err != nil {
			res.Err = "auth: " + err.Error()
			res.Elapsed = time.Since(start)
			return res
		}
	}
	res.Auth = true

	if err := conn.ModeReader(probeCtx); err != nil {
		res.Err = "mode reader: " + err.Error()
		res.Elapsed = time.Since(start)
		return res
	}
	res.ModeReader = true

	d, err := conn.Date(probeCtx)
	if err != nil {
		res.Err = "date: " + err.Error()
		res.Elapsed = time.Since(start)
		return res
	}
	res.Date = true
	res.ServerDate = d.UTC().Format(time.RFC3339)
	res.OK = true
	res.Elapsed = time.Since(start)
	return res
}
