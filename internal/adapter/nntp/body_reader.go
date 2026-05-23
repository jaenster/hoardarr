package nntp

import (
	"bufio"
	"errors"
	"fmt"
	"io"
)

// fastBodyReader is an io.Reader that reads an NNTP article body off
// a textproto connection, doing dot-unstuffing per RFC 977 in line
// granularity instead of byte granularity.
//
// The stdlib's net/textproto.dotReader walks the body one byte at a
// time through a five-state machine (driven by bufio.ReadByte) so that
// "." escapes are picked off correctly. On the production V1500B that
// path was holding ~70% of CPU during a 7 MB/s download — dotReader
// and ReadByte together. yEnc article bodies are 5–10 thousand
// short (~130 char) lines, and the per-byte function-call overhead
// is the dominant cost; bytes.IndexByte-style line scanning amortises
// it cleanly.
//
// Wire-format reminder for the dot-stuffing rules we implement:
//
//   - Every line ends CRLF (or sometimes just LF — tolerated below).
//   - A line consisting solely of "." marks end-of-body.
//   - Any other line whose first byte is "." had an extra "." prepended
//     by the sender; we strip exactly one leading dot.
//
// CRLF terminators are rewritten to bare LF, matching
// textproto.dotReader's normalisation — the rest of the codebase
// (cassette record/replay, SAB handler, tests) was written against
// that contract.
type fastBodyReader struct {
	br *bufio.Reader
	// pending holds bytes from a prior line that didn't fit in the
	// caller's last Read buffer. Kept around so the next Read can
	// drain them before reading new lines.
	pending []byte
	// lineBuf is a reusable staging buffer for CRLF→LF rewriting.
	// Allocated once on first CRLF line, then reused for every
	// subsequent line so steady-state has no per-line allocation.
	lineBuf []byte
	eof     bool
}

func newFastBodyReader(br *bufio.Reader) *fastBodyReader {
	return &fastBodyReader{br: br}
}

// Read implements io.Reader. Returns 0, io.EOF after the end-of-body
// "." terminator has been consumed; the underlying bufio.Reader is
// then positioned just past it and the connection is ready for the
// next NNTP command.
func (r *fastBodyReader) Read(p []byte) (int, error) {
	n := 0
	// Flush any leftover bytes from the previous line first.
	if len(r.pending) > 0 {
		n = copy(p, r.pending)
		r.pending = r.pending[n:]
		if n == len(p) {
			return n, nil
		}
	}
	if r.eof {
		if n > 0 {
			return n, nil
		}
		return 0, io.EOF
	}
	for n < len(p) {
		line, err := r.br.ReadSlice('\n')
		if err == bufio.ErrBufferFull {
			// NNTP RFC 977 caps lines at 1000 chars; bufio's default
			// 4 KiB buffer is well above that. If we ever hit this
			// the wire stream is malformed (or someone bumped the
			// reader without telling us).
			return n, errors.New("nntp: body line exceeds reader buffer")
		}
		if err != nil && err != io.EOF {
			return n, fmt.Errorf("nntp body read: %w", err)
		}
		if len(line) == 0 {
			// Underlying EOF before terminator — propagate.
			return n, io.EOF
		}

		// End-of-body: ".\r\n" or, lenient, ".\n".
		if (len(line) == 3 && line[0] == '.' && line[1] == '\r' && line[2] == '\n') ||
			(len(line) == 2 && line[0] == '.' && line[1] == '\n') {
			r.eof = true
			if n > 0 {
				return n, nil
			}
			return 0, io.EOF
		}

		// Dot-unstuffing: a leading "." on any non-terminator line
		// was inserted by the sender; remove it. Net-positive on
		// yEnc payloads since dots are rare in encoded body data.
		if line[0] == '.' {
			line = line[1:]
		}

		// CRLF -> LF normalisation. ReadSlice returns a slice into
		// bufio's internal buffer; mutating it in place would corrupt
		// the next ReadSlice call (the buffer may still hold the
		// pre-line bytes the next read will reuse). Stage the
		// rewritten line in lineBuf, allocated once and reused.
		if m := len(line); m >= 2 && line[m-2] == '\r' && line[m-1] == '\n' {
			if cap(r.lineBuf) < m-1 {
				r.lineBuf = make([]byte, m-1)
			} else {
				r.lineBuf = r.lineBuf[:m-1]
			}
			copy(r.lineBuf, line[:m-2])
			r.lineBuf[m-2] = '\n'
			line = r.lineBuf
		}

		// Write the line into p. The common path (line fits) avoids
		// any further copies; the split path stashes the remainder
		// in pending so the next Read can drain it.
		if len(line) <= len(p)-n {
			copy(p[n:], line)
			n += len(line)
			continue
		}
		space := len(p) - n
		copy(p[n:], line[:space])
		n += space
		// Stash the remainder. Reuse pending's backing array when
		// possible so steady-state has no allocations.
		need := len(line) - space
		if cap(r.pending) < need {
			r.pending = make([]byte, need)
		} else {
			r.pending = r.pending[:need]
		}
		copy(r.pending, line[space:])
		return n, nil
	}
	return n, nil
}
