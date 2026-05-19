package nntp

import (
	"errors"
	"fmt"
	"strings"
)

// ProtocolError is a structured error from an NNTP server response.
//
// The Code is the 3-digit status from RFC 3977; Message is the textual
// portion of the same line (without trailing CR/LF).
type ProtocolError struct {
	Code    int
	Message string
}

func (e *ProtocolError) Error() string {
	return fmt.Sprintf("nntp: %d %s", e.Code, e.Message)
}

// IsTransient returns true for 4xx codes ("transient negative") which
// indicate the command should be retried later or on a different server.
func (e *ProtocolError) IsTransient() bool {
	return e.Code/100 == 4
}

// IsPermanent returns true for 5xx codes ("permanent negative").
func (e *ProtocolError) IsPermanent() bool {
	return e.Code/100 == 5
}

// Sentinel errors for high-level cases. They wrap a *ProtocolError so
// callers can use errors.Is and still inspect the underlying code.
var (
	// ErrArticleMissing wraps 430 ("no such article") returned by
	// BODY/ARTICLE/STAT/HEAD when the message-id is not present on
	// this server. The orchestrator surfaces this as a missing
	// segment so PAR2 can pick it up later.
	ErrArticleMissing = errors.New("nntp: article missing")

	// ErrAuthFailed wraps 481/482/502 returned during AUTHINFO.
	ErrAuthFailed = errors.New("nntp: authentication failed")

	// ErrAuthRequired wraps 480 returned by commands that need
	// authentication before being honoured.
	ErrAuthRequired = errors.New("nntp: authentication required")

	// ErrUnexpectedGreeting is returned from Dial when the greeting
	// is not 200 or 201.
	ErrUnexpectedGreeting = errors.New("nntp: unexpected greeting")

	// ErrTooManyConnections is returned from Dial when the server's
	// greeting refuses the connection because the account is over its
	// per-account slot limit. Different providers signal this
	// differently — Eweka uses "502 too many connections", others use
	// 400. Detected by message text in classifyGreeting; the pool
	// uses it to back off briefly before dialling more.
	ErrTooManyConnections = errors.New("nntp: too many connections")
)

// classifyGreeting maps an unexpected greeting code/message to a
// sentinel where one fits. Returns ErrUnexpectedGreeting wrapping the
// details when nothing more specific matches. Called from Dial.
func classifyGreeting(code int, msg string) error {
	// Common provider phrasings; case-insensitive on the substring.
	low := strings.ToLower(msg)
	if strings.Contains(low, "too many connection") ||
		strings.Contains(low, "max connections") ||
		strings.Contains(low, "connection limit") {
		return fmt.Errorf("%w: %d %s", ErrTooManyConnections, code, msg)
	}
	return fmt.Errorf("%w: %d %s", ErrUnexpectedGreeting, code, msg)
}

// classifyResponse maps a *ProtocolError to a sentinel where one fits.
// Returns the original error if no sentinel applies.
//
// Connection-limit detection runs first because providers send "too
// many connections" on a variety of codes (Eweka 502, some on 400,
// occasional 481 on AUTHINFO PASS when the slot's already taken).
// Matching the message text catches all of them and routes the error
// to the pool's back-off / tier-fall-through path instead of the
// generic auth-failed / transient-retry path that burns the segment
// retry budget.
func classifyResponse(pe *ProtocolError) error {
	low := strings.ToLower(pe.Message)
	if strings.Contains(low, "too many connection") ||
		strings.Contains(low, "max connections") ||
		strings.Contains(low, "connection limit") {
		return fmt.Errorf("%w: %d %s", ErrTooManyConnections, pe.Code, pe.Message)
	}
	switch pe.Code {
	case 430:
		return fmt.Errorf("%w: %s", ErrArticleMissing, pe.Message)
	case 480:
		return fmt.Errorf("%w: %s", ErrAuthRequired, pe.Message)
	case 481, 482, 502:
		return fmt.Errorf("%w: %s", ErrAuthFailed, pe.Message)
	}
	return pe
}
