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
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"strconv"
	"strings"
	"sync"
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

// ErrCRCMismatch is returned when the trailer's CRC does not match the
// decoded payload.
var ErrCRCMismatch = errors.New("yenc: crc32 mismatch")

var (
	prefixYBegin = []byte("=ybegin")
	prefixYPart  = []byte("=ypart")
	prefixYEnd   = []byte("=yend")
)

// inputPool recycles the read-all input buffer across decode calls.
// yEnc articles cluster around 750 KiB encoded; a 1 MiB starting cap
// fits the overwhelming majority without a grow. Pool-hit Decode calls
// allocate zero bytes for input scanning.
var inputPool = sync.Pool{
	New: func() any {
		b := make([]byte, 0, 1<<20)
		return &b
	},
}

// readAllSized slurps r into a pooled buffer. yEnc articles are bounded
// (~750 KiB typical, well under the per-article limits enforced by
// every real news server), so reading once and scanning in place is
// strictly cheaper than streaming via bufio — no per-line allocations,
// and we hand the body to bytes.IndexByte's SIMD asm fast path.
//
// The returned buffer is owned by the caller for the duration of decode
// and must be returned to the pool via releaseInput.
func readAllSized(r io.Reader) (*[]byte, error) {
	bufp := inputPool.Get().(*[]byte)
	buf := (*bufp)[:0]
	for {
		if len(buf) == cap(buf) {
			// Grow geometrically. append handles this cleanly.
			buf = append(buf, 0)[:len(buf)]
		}
		n, err := r.Read(buf[len(buf):cap(buf)])
		buf = buf[:len(buf)+n]
		if err != nil {
			*bufp = buf
			if err == io.EOF {
				return bufp, nil
			}
			return bufp, err
		}
	}
}

func releaseInput(bufp *[]byte) {
	// Cap the size we retain to avoid one giant article wedging a huge
	// buffer into the pool forever.
	if cap(*bufp) > 4<<20 {
		return
	}
	*bufp = (*bufp)[:0]
	inputPool.Put(bufp)
}

// Decode reads a single yEnc-encoded article body from r and returns
// the decoded payload along with parsed Header / Trailer.
//
// CRC32 verification: if the trailer carries pcrc32 (multi-part) or
// crc32 (single-part), the value is compared against the CRC32 of the
// decoded payload. ErrCRCMismatch is returned on mismatch.
//
// Performance: the body is read into a single buffer up front, then
// scanned in place. The output buffer is pre-allocated from the size
// declared on =ybegin / =ypart so the hot decode loop never reallocates.
// Per-line escape scanning uses bytes.IndexByte (SIMD asm), and runs
// between escapes are subtracted by 42 in a tight inner loop the
// compiler vectorises.
func Decode(r io.Reader) ([]byte, Header, Trailer, error) {
	var (
		hdr      Header
		trl      Trailer
		gotBegin bool
		gotEnd   bool
		out      []byte
	)

	bufp, err := readAllSized(r)
	if err != nil {
		releaseInput(bufp)
		return nil, hdr, trl, err
	}
	defer releaseInput(bufp)
	src := *bufp

	pos := 0
	for pos < len(src) {
		// Find end of current line.
		var line []byte
		if nl := bytes.IndexByte(src[pos:], '\n'); nl < 0 {
			line = src[pos:]
			pos = len(src)
		} else {
			line = src[pos : pos+nl]
			pos += nl + 1
		}
		// Strip a trailing \r so the same parser handles CRLF and bare LF.
		if n := len(line); n > 0 && line[n-1] == '\r' {
			line = line[:n-1]
		}
		if len(line) == 0 {
			continue
		}

		// Control lines start with "=y". Anything else with a leading
		// '=' is just a data line whose first byte is escaped.
		isControl := len(line) >= 2 && line[0] == '=' && line[1] == 'y'

		switch {
		case isControl && bytes.HasPrefix(line, prefixYBegin):
			if gotBegin {
				return nil, hdr, trl, errors.New("yenc: duplicate =ybegin")
			}
			if err := parseBeginLine(line, &hdr); err != nil {
				return nil, hdr, trl, err
			}
			gotBegin = true
			// Pre-allocate the output buffer. For multi-part we don't
			// yet know the per-part size (it comes on =ypart); fall back
			// to a small default and let append grow it.
			if hdr.Size > 0 && hdr.Total == 0 {
				out = make([]byte, 0, hdr.Size)
			}

		case isControl && bytes.HasPrefix(line, prefixYPart):
			if !gotBegin {
				return nil, hdr, trl, errors.New("yenc: =ypart before =ybegin")
			}
			if err := parsePartLine(line, &hdr); err != nil {
				return nil, hdr, trl, err
			}
			if out == nil {
				if sz := hdr.End - hdr.Begin + 1; sz > 0 {
					out = make([]byte, 0, sz)
				}
			}

		case isControl && bytes.HasPrefix(line, prefixYEnd):
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
			out, err = decodeLineAppend(out, line)
			if err != nil {
				return nil, hdr, trl, err
			}
		}
	}

	if !gotBegin {
		return nil, hdr, trl, errors.New("yenc: no =ybegin")
	}
	if !gotEnd {
		return nil, hdr, trl, errors.New("yenc: unexpected EOF before =yend")
	}

	// For single-part, default Begin/End to span the whole payload.
	if hdr.Begin == 0 {
		hdr.Begin = 1
	}
	if hdr.End == 0 {
		hdr.End = hdr.Size
	}

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

// decodeLineAppend decodes one body line and appends the bytes to out,
// returning the grown slice. The line has already had its trailing
// CR/LF stripped.
//
// Hot path: find the next '=' with bytes.IndexByte (SIMD asm — portable
// fast path on amd64 and arm64), then transform the unescaped run from
// src to out via subSpread, which subtracts 42 from 8 bytes at a time
// using portable uint64 SWAR. Read-once / write-once per byte: no
// intermediate buffer between the line and out.
func decodeLineAppend(out, line []byte) ([]byte, error) {
	for len(line) > 0 {
		eq := bytes.IndexByte(line, '=')
		if eq < 0 {
			// No more escapes. Transform the rest into out.
			return appendSub42(out, line), nil
		}
		if eq > 0 {
			out = appendSub42(out, line[:eq])
		}
		// Decode the escape itself: '=' + (raw + 42 + 64).
		if eq+1 >= len(line) {
			return out, errors.New("yenc: dangling escape at line end")
		}
		out = append(out, line[eq+1]-64-42)
		line = line[eq+2:]
	}
	return out, nil
}

// SWAR constants for byte-wise subtract-42 across 8 packed bytes.
//
// We want dst[i] = src[i] - 42, which is src[i] + 214 (mod 256). The
// trick to add a per-byte constant inside a 64-bit register without
// inter-byte carry is the classic:
//
//	((a & lo7) + (k & lo7)) ^ ((a ^ k) & hi1)
//
// For our constant k = 0xD6 per byte: (k & lo7) is 0x56 per byte, and
// ((a ^ k) & hi1) simplifies to (hi1 &^ a) since k's high bit is 1.
//
// Each masked-low add is at most 0x7F + 0x56 = 0xD5 — no carry into
// the next byte. The XOR restores the correct top bit per byte. The
// formula is portable Go and compiles to plain arithmetic on every
// architecture we care about (amd64, arm64).
const (
	subLowAdd = uint64(0x5656565656565656) // (0xD6 & 0x7F) per byte
	subLo7    = uint64(0x7F7F7F7F7F7F7F7F)
	subHi1    = uint64(0x8080808080808080)
)

// appendSub42 appends each byte of src to out with 42 subtracted,
// growing out via append when needed. The bulk loop processes 8 bytes
// per iteration via SWAR; a scalar tail handles the remainder.
func appendSub42(out, src []byte) []byte {
	n := len(out)
	// Ensure capacity. One grow per call at most; pre-allocation in
	// Decode means we usually skip this entirely.
	if cap(out)-n < len(src) {
		out = append(out, src...)[:n] // grow to fit, keep len
		out = out[:n+len(src)]        // restore extended view
	} else {
		out = out[:n+len(src)]
	}
	subCopy42(out[n:], src)
	return out
}

// subCopy42 writes dst[i] = src[i] - 42 for i in [0, len(src)).
// Requires len(dst) >= len(src). Uses uint64 SWAR for the bulk and a
// scalar tail.
func subCopy42(dst, src []byte) {
	i := 0
	n := len(src)
	// Bulk 8-byte loop. Hoisting the slice bounds checks via the
	// helper indexing pattern keeps the inner loop branch-free.
	for ; i+8 <= n; i += 8 {
		chunk := binary.LittleEndian.Uint64(src[i:])
		out := ((chunk & subLo7) + subLowAdd) ^ (subHi1 &^ chunk)
		binary.LittleEndian.PutUint64(dst[i:], out)
	}
	for ; i < n; i++ {
		dst[i] = src[i] - 42
	}
}

func parseBeginLine(line []byte, h *Header) error {
	// Format: =ybegin (part=N) (total=N) line=N size=N name=...
	// "name=" runs to end of line; everything else is space-delimited.
	rest := bytes.TrimPrefix(line, prefixYBegin)
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
	rest := bytes.TrimPrefix(line, prefixYPart)
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
	rest := bytes.TrimPrefix(line, prefixYEnd)
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

// decodeLine is kept for the direct unit tests in yenc_test.go. New
// code should use decodeLineAppend on the hot path.
func decodeLine(line []byte, buf *bytes.Buffer) error {
	out, err := decodeLineAppend(buf.Bytes(), line)
	if err != nil {
		return err
	}
	// Replace the buffer contents with the grown slice.
	buf.Reset()
	buf.Write(out)
	return nil
}
