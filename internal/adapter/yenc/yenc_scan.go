package yenc

// Alternative decoder: single-pass body scan-and-decode using SWAR
// byte-matching for the special bytes {'=', '\r', '\n'}. Kept as a
// separate implementation so we can benchmark it side-by-side against
// the line-by-line approach used by Decode.
//
// Status: as benchmarked on Apple M3 Max, this implementation is
// SLOWER than Decode. The reason is that the SWAR has-special check
// costs ~14 64-bit ops per 8-byte chunk, while the line-by-line
// version delegates the same scanning work to bytes.IndexByte which
// uses platform NEON/SSE assembly and processes 16+ bytes per
// instruction. Kept here for the benchmark contrast.

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
)

var (
	markerYEnd = []byte("\n=yend")
)

// Repeated-byte constants for the SWAR special-byte scan.
const (
	rep8Eq = uint64(0x3D3D3D3D3D3D3D3D) // '='
	rep8CR = uint64(0x0D0D0D0D0D0D0D0D) // '\r'
	rep8LF = uint64(0x0A0A0A0A0A0A0A0A) // '\n'
	rep8Lo = uint64(0x0101010101010101)
	rep8Hi = uint64(0x8080808080808080)
)

// hasZeroByte returns nonzero if any byte of x is zero. Classic SWAR.
func hasZeroByte(x uint64) uint64 {
	return (x - rep8Lo) &^ x & rep8Hi
}

// hasSpecial returns nonzero if any byte of chunk is '=', '\r', or '\n'.
func hasSpecial(chunk uint64) uint64 {
	return hasZeroByte(chunk^rep8Eq) |
		hasZeroByte(chunk^rep8CR) |
		hasZeroByte(chunk^rep8LF)
}

// DecodeScan is the alternative decoder entry point (test-only API).
func DecodeScan(r io.Reader) ([]byte, Header, Trailer, error) {
	bufp, err := readAllSized(r)
	if err != nil {
		releaseInput(bufp)
		return nil, Header{}, Trailer{}, err
	}
	defer releaseInput(bufp)
	return decodeBytesScan(*bufp)
}

func decodeBytesScan(src []byte) ([]byte, Header, Trailer, error) {
	var (
		hdr Header
		trl Trailer
	)

	ybeginStart := findLineStart(src, 0, prefixYBegin)
	if ybeginStart < 0 {
		return nil, hdr, trl, errors.New("yenc: no =ybegin")
	}
	ybeginLineEnd := indexLF(src, ybeginStart)
	if err := parseBeginLine(stripCR(src[ybeginStart:ybeginLineEnd]), &hdr); err != nil {
		return nil, hdr, trl, err
	}
	bodyStart := ybeginLineEnd
	if bodyStart < len(src) && src[bodyStart] == '\n' {
		bodyStart++
	}

	if bytes.HasPrefix(src[bodyStart:], prefixYPart) {
		ypartEnd := indexLF(src, bodyStart)
		if err := parsePartLine(stripCR(src[bodyStart:ypartEnd]), &hdr); err != nil {
			return nil, hdr, trl, err
		}
		bodyStart = ypartEnd
		if bodyStart < len(src) && src[bodyStart] == '\n' {
			bodyStart++
		}
	}

	var bodyEnd, yendLineStart int
	if bytes.HasPrefix(src[bodyStart:], prefixYEnd) {
		bodyEnd = bodyStart
		yendLineStart = bodyStart
	} else if rel := bytes.Index(src[bodyStart:], markerYEnd); rel >= 0 {
		bodyEnd = bodyStart + rel
		yendLineStart = bodyEnd + 1
	} else {
		return nil, hdr, trl, errors.New("yenc: unexpected EOF before =yend")
	}

	var outCap int
	switch {
	case hdr.Total == 0 && hdr.Size > 0:
		outCap = int(hdr.Size)
	case hdr.End > 0:
		outCap = int(hdr.End - hdr.Begin + 1)
	}
	if outCap < bodyEnd-bodyStart {
		outCap = bodyEnd - bodyStart
	}
	out := make([]byte, 0, outCap)

	out, err := decodeBodyScan(out, src[bodyStart:bodyEnd])
	if err != nil {
		return nil, hdr, trl, err
	}

	yendLineEnd := indexLF(src, yendLineStart)
	if err := parseEndLine(stripCR(src[yendLineStart:yendLineEnd]), &trl); err != nil {
		return nil, hdr, trl, err
	}

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
		if got := crc32.ChecksumIEEE(out); got != trl.CRC32 {
			return out, hdr, trl, fmt.Errorf("%w: got %08x want %08x", ErrCRCMismatch, got, trl.CRC32)
		}
	}

	return out, hdr, trl, nil
}

// decodeBodyScan walks the body in 8-byte chunks. Clean chunks (no
// '=', '\r', or '\n') get the SWAR subtract-42 in one branchless op;
// chunks with a special byte fall through to a scalar inner loop that
// handles CR/LF skipping and '=' escapes.
func decodeBodyScan(out, body []byte) ([]byte, error) {
	bodyLen := len(body)
	nOut := len(out)
	if cap(out) < nOut+bodyLen {
		grown := make([]byte, nOut+bodyLen)
		copy(grown, out)
		out = grown
	} else {
		out = out[:nOut+bodyLen]
	}
	j := nOut
	nIn := 0

	for nIn+8 <= bodyLen {
		chunk := binary.LittleEndian.Uint64(body[nIn:])
		if hasSpecial(chunk) == 0 {
			result := ((chunk & subLo7) + subLowAdd) ^ (subHi1 &^ chunk)
			binary.LittleEndian.PutUint64(out[j:], result)
			j += 8
			nIn += 8
			continue
		}
		end := nIn + 8
		for nIn < end {
			c := body[nIn]
			switch c {
			case '\r', '\n':
				nIn++
			case '=':
				nIn++
				if nIn >= bodyLen {
					return out[:j], errors.New("yenc: dangling escape at body end")
				}
				c2 := body[nIn]
				if c2 == '\r' || c2 == '\n' {
					return out[:j], errors.New("yenc: dangling escape at line end")
				}
				out[j] = c2 - 64 - 42
				j++
				nIn++
			default:
				out[j] = c - 42
				j++
				nIn++
			}
		}
	}

	for nIn < bodyLen {
		c := body[nIn]
		switch c {
		case '\r', '\n':
			nIn++
		case '=':
			nIn++
			if nIn >= bodyLen {
				return out[:j], errors.New("yenc: dangling escape at body end")
			}
			c2 := body[nIn]
			if c2 == '\r' || c2 == '\n' {
				return out[:j], errors.New("yenc: dangling escape at line end")
			}
			out[j] = c2 - 64 - 42
			j++
			nIn++
		default:
			out[j] = c - 42
			j++
			nIn++
		}
	}

	return out[:j], nil
}

// findLineStart returns the index of prefix anchored at column zero
// (either position from, or immediately after a '\n' at or after from).
// Returns -1 when the prefix never appears at a line start.
func findLineStart(src []byte, from int, prefix []byte) int {
	if from < len(src) && bytes.HasPrefix(src[from:], prefix) {
		return from
	}
	at := from
	for at < len(src) {
		off := bytes.IndexByte(src[at:], '\n')
		if off < 0 {
			return -1
		}
		next := at + off + 1
		if bytes.HasPrefix(src[next:], prefix) {
			return next
		}
		at = next
	}
	return -1
}

func indexLF(src []byte, from int) int {
	if i := bytes.IndexByte(src[from:], '\n'); i >= 0 {
		return from + i
	}
	return len(src)
}

func stripCR(line []byte) []byte {
	if n := len(line); n > 0 && line[n-1] == '\r' {
		return line[:n-1]
	}
	return line
}
