// Package yenc decodes yEnc-encoded Usenet articles.
//
// yEnc (https://www.yenc.org/yenc-draft.1.3.txt) is the binary-to-text
// encoding used for Usenet binary posts. Each article body looks like:
//
//	=ybegin part=1 total=8 line=128 size=750000 name=cool stuff.rar
//	=ypart begin=1 end=750000
//	[encoded bytes wrapped at line=128]
//	=yend size=750000 part=1 pcrc32=AABBCCDD
//
// The encoder shifts each input byte by +42 (mod 256) and escapes
// "critical" output bytes (NUL, LF, CR, =, sometimes leading TAB / dot
// / space) by prefixing with '=' and shifting an additional +64.
//
// This package decodes a single article body. Multi-part files are
// reassembled by the caller using Header.Begin and Header.End to write
// each segment at its known offset into the destination file.
//
// CRC32 verification: per-article via Trailer.PartCRC32 (multi-part) or
// Trailer.CRC32 (single-part). Decode returns ErrCRCMismatch when the
// computed value disagrees with the trailer.
package yenc

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"strconv"
	"strings"
)

// Header captures the =ybegin (and optional =ypart) header fields.
type Header struct {
	// Name is the filename declared by the poster. May contain spaces
	// or non-ASCII characters; verbatim from the article.
	Name string
	// Size is the total decoded file size declared by the poster
	// (before splitting into parts).
	Size int64
	// Line is the column wrap width (informational; decoders ignore it).
	Line int
	// Part is the 1-based part number for multi-part files. Zero for
	// single-part articles.
	Part int
	// Total is the total number of parts for multi-part files. Zero
	// for single-part.
	Total int
	// Begin is the 1-based byte offset (inclusive) of this part within
	// the assembled file. For single-part, callers should treat Begin=1.
	Begin int64
	// End is the 1-based byte offset (inclusive) of the last byte of
	// this part within the assembled file. For single-part, End=Size.
	End int64
}

// Trailer captures the =yend trailer.
type Trailer struct {
	// Size is the declared size of the decoded part.
	Size int64
	// Part is the 1-based part number (mirrors Header.Part).
	Part int
	// HasPartCRC indicates whether the trailer carried a pcrc32=
	// attribute (typical for multi-part).
	HasPartCRC bool
	PartCRC32  uint32
	// HasCRC indicates whether the trailer carried a crc32= attribute
	// (typical for single-part — total file CRC).
	HasCRC bool
	CRC32  uint32
}

// Decode reads a single yEnc-encoded article body from r and returns
// the decoded payload along with parsed Header / Trailer.
//
// CRC32 verification: if the trailer carries pcrc32 (multi-part) or
// crc32 (single-part), the value is compared against the CRC32 of the
// decoded payload. ErrCRCMismatch is returned on mismatch.
//
// Memory: the entire decoded payload is buffered. Articles are
// typically ~750 KiB; for the orchestrator's use case (write at
// offset, fsync per-file) buffering is fine. A future streaming API
// may be added if heap pressure becomes a concern.
func Decode(r io.Reader) ([]byte, Header, Trailer, error) {
	br := bufio.NewReaderSize(r, 64*1024)
	var (
		hdr     Header
		trl     Trailer
		gotBegin bool
		gotEnd   bool
		buf      bytes.Buffer
	)

	for {
		line, err := readLine(br)
		if err == io.EOF {
			if !gotEnd {
				return nil, hdr, trl, errors.New("yenc: unexpected EOF before =yend")
			}
			break
		}
		if err != nil {
			return nil, hdr, trl, err
		}
		if len(line) == 0 {
			continue
		}

		switch {
		case bytes.HasPrefix(line, []byte("=ybegin")):
			if gotBegin {
				return nil, hdr, trl, errors.New("yenc: duplicate =ybegin")
			}
			if err := parseBeginLine(line, &hdr); err != nil {
				return nil, hdr, trl, err
			}
			gotBegin = true

		case bytes.HasPrefix(line, []byte("=ypart")):
			if !gotBegin {
				return nil, hdr, trl, errors.New("yenc: =ypart before =ybegin")
			}
			if err := parsePartLine(line, &hdr); err != nil {
				return nil, hdr, trl, err
			}

		case bytes.HasPrefix(line, []byte("=yend")):
			if !gotBegin {
				return nil, hdr, trl, errors.New("yenc: =yend before =ybegin")
			}
			if err := parseEndLine(line, &trl); err != nil {
				return nil, hdr, trl, err
			}
			gotEnd = true

		default:
			if !gotBegin {
				// Pre-header noise (NNTP-stuffed dots, blank lines).
				continue
			}
			if err := decodeLine(line, &buf); err != nil {
				return nil, hdr, trl, err
			}
		}
	}

	if !gotBegin {
		return nil, hdr, trl, errors.New("yenc: no =ybegin")
	}

	// For single-part, default Begin/End to span the whole payload.
	if hdr.Begin == 0 {
		hdr.Begin = 1
	}
	if hdr.End == 0 {
		hdr.End = hdr.Size
	}

	out := buf.Bytes()
	switch {
	case trl.HasPartCRC:
		if got := crc32.ChecksumIEEE(out); got != trl.PartCRC32 {
			return out, hdr, trl, fmt.Errorf("%w: got %08x want %08x", ErrCRCMismatch, got, trl.PartCRC32)
		}
	case trl.HasCRC && hdr.Part == 0:
		// Whole-file CRC for single-part articles. Multi-part articles
		// also carry crc32= on the LAST part — that's the full-file
		// CRC and we don't validate it here (the orchestrator does
		// after assembly).
		if got := crc32.ChecksumIEEE(out); got != trl.CRC32 {
			return out, hdr, trl, fmt.Errorf("%w: got %08x want %08x", ErrCRCMismatch, got, trl.CRC32)
		}
	}

	return out, hdr, trl, nil
}

// ErrCRCMismatch is returned when the trailer's CRC does not match the
// decoded payload.
var ErrCRCMismatch = errors.New("yenc: crc32 mismatch")

// readLine returns the next line from br, with the trailing CR/LF
// stripped. EOF mid-line is treated as a terminating newline so the
// last line is still returned.
func readLine(br *bufio.Reader) ([]byte, error) {
	line, err := br.ReadBytes('\n')
	if len(line) == 0 && err != nil {
		return nil, err
	}
	// Strip CRLF / LF.
	line = bytes.TrimRight(line, "\r\n")
	if err != nil && err != io.EOF {
		return line, err
	}
	return line, nil
}

func parseBeginLine(line []byte, h *Header) error {
	// Format: =ybegin (part=N) (total=N) line=N size=N name=...
	// "name=" runs to end of line; everything else is space-delimited.
	rest := bytes.TrimPrefix(line, []byte("=ybegin"))
	rest = bytes.TrimSpace(rest)

	// "name=" is a special case: the value extends to end of line and
	// may contain spaces. Pull it off first.
	if idx := bytes.Index(rest, []byte("name=")); idx >= 0 {
		h.Name = strings.TrimSpace(string(rest[idx+len("name="):]))
		rest = bytes.TrimSpace(rest[:idx])
	}

	for _, kv := range bytes.Fields(rest) {
		k, v, ok := splitKV(kv)
		if !ok {
			continue
		}
		switch k {
		case "part":
			if n, err := strconv.Atoi(v); err == nil {
				h.Part = n
			}
		case "total":
			if n, err := strconv.Atoi(v); err == nil {
				h.Total = n
			}
		case "line":
			if n, err := strconv.Atoi(v); err == nil {
				h.Line = n
			}
		case "size":
			if n, err := strconv.ParseInt(v, 10, 64); err == nil {
				h.Size = n
			}
		}
	}
	if h.Size < 0 {
		return fmt.Errorf("yenc: =ybegin negative size: %q", line)
	}
	// size=0 is valid (zero-byte article); size attribute simply
	// being absent (not parsed as 0 — strconv would have failed)
	// would also leave h.Size at its zero-value default. Reject only
	// the genuinely impossible.
	return nil
}

func parsePartLine(line []byte, h *Header) error {
	rest := bytes.TrimPrefix(line, []byte("=ypart"))
	for _, kv := range bytes.Fields(rest) {
		k, v, ok := splitKV(kv)
		if !ok {
			continue
		}
		switch k {
		case "begin":
			if n, err := strconv.ParseInt(v, 10, 64); err == nil {
				h.Begin = n
			}
		case "end":
			if n, err := strconv.ParseInt(v, 10, 64); err == nil {
				h.End = n
			}
		}
	}
	if h.Begin <= 0 || h.End < h.Begin {
		return fmt.Errorf("yenc: =ypart bad begin/end (%d/%d)", h.Begin, h.End)
	}
	return nil
}

func parseEndLine(line []byte, t *Trailer) error {
	rest := bytes.TrimPrefix(line, []byte("=yend"))
	for _, kv := range bytes.Fields(rest) {
		k, v, ok := splitKV(kv)
		if !ok {
			continue
		}
		switch k {
		case "size":
			if n, err := strconv.ParseInt(v, 10, 64); err == nil {
				t.Size = n
			}
		case "part":
			if n, err := strconv.Atoi(v); err == nil {
				t.Part = n
			}
		case "pcrc32":
			if n, err := strconv.ParseUint(v, 16, 32); err == nil {
				t.PartCRC32 = uint32(n)
				t.HasPartCRC = true
			}
		case "crc32":
			if n, err := strconv.ParseUint(v, 16, 32); err == nil {
				t.CRC32 = uint32(n)
				t.HasCRC = true
			}
		}
	}
	return nil
}

func splitKV(kv []byte) (string, string, bool) {
	idx := bytes.IndexByte(kv, '=')
	if idx <= 0 {
		return "", "", false
	}
	return string(kv[:idx]), string(kv[idx+1:]), true
}

// decodeLine appends decoded bytes from line into buf. yEnc encodes
// each input byte as (raw + 42) mod 256; "critical" output bytes
// (NUL, LF, CR, '=', and a few context-sensitive ones) are escaped by
// prefixing '=' and adding 64. Decoder simply reverses both shifts.
func decodeLine(line []byte, buf *bytes.Buffer) error {
	for i := 0; i < len(line); i++ {
		b := line[i]
		if b == '=' {
			i++
			if i >= len(line) {
				return errors.New("yenc: dangling escape at line end")
			}
			buf.WriteByte(line[i] - 64 - 42)
			continue
		}
		buf.WriteByte(b - 42)
	}
	return nil
}
