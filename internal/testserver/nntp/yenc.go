package nntp

import (
	"bytes"
	"fmt"
	"hash/crc32"
)

// EncodeArticle yEnc-encodes payload as a single-part article body
// suitable for AddArticle. The returned bytes include =ybegin /
// encoded lines / =yend, with CRLF line endings and no dot-stuffing
// (the server applies that on the wire).
//
// filename is the declared filename (=ybegin name=). Test callers
// typically use the same value as the NZB file's <file subject=>.
func EncodeArticle(filename string, payload []byte) []byte {
	const lineWidth = 128
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "=ybegin line=%d size=%d name=%s\r\n", lineWidth, len(payload), filename)
	writeYencBody(&buf, payload, lineWidth)
	crc := crc32.ChecksumIEEE(payload)
	fmt.Fprintf(&buf, "=yend size=%d crc32=%08x\r\n", len(payload), crc)
	return buf.Bytes()
}

// EncodeArticlePart yEnc-encodes a slice of a multi-part payload.
// begin/end are 1-based inclusive byte offsets within the full file;
// totalFileSize is the size of the assembled file (i.e. =ybegin size=);
// part/total are 1-based part numbers. =yend size carries the size of
// THIS part. This matches the yEnc spec — receivers Truncate to the
// =ybegin size, so per-segment size there would shrink the file.
func EncodeArticlePart(filename string, payload []byte, begin, end, totalFileSize int64, part, total int) []byte {
	const lineWidth = 128
	var buf bytes.Buffer
	fmt.Fprintf(&buf,
		"=ybegin part=%d total=%d line=%d size=%d name=%s\r\n",
		part, total, lineWidth, totalFileSize, filename)
	fmt.Fprintf(&buf, "=ypart begin=%d end=%d\r\n", begin, end)
	writeYencBody(&buf, payload, lineWidth)
	pcrc := crc32.ChecksumIEEE(payload)
	fmt.Fprintf(&buf,
		"=yend size=%d part=%d pcrc32=%08x\r\n",
		len(payload), part, pcrc)
	return buf.Bytes()
}

// writeYencBody encodes raw bytes per the yEnc rules:
//   - byte = (b + 42) mod 256
//   - escape NUL(00), LF(0A), CR(0D), '='(3D) by emitting '=' then
//     (byte + 64) mod 256
//   - additionally escape '.'(2E) at column 0 to keep clients happy
//     (some old yEnc decoders trip on a leading dot even though the
//     transport handles that separately)
func writeYencBody(buf *bytes.Buffer, payload []byte, lineWidth int) {
	col := 0
	for _, b := range payload {
		enc := byte((int(b) + 42) % 256)
		switch enc {
		case 0x00, 0x0A, 0x0D, 0x3D:
			buf.WriteByte('=')
			buf.WriteByte(byte((int(enc) + 64) % 256))
			col += 2
		case '.':
			if col == 0 {
				buf.WriteByte('=')
				buf.WriteByte(byte((int(enc) + 64) % 256))
				col += 2
				break
			}
			buf.WriteByte(enc)
			col++
		default:
			buf.WriteByte(enc)
			col++
		}
		if col >= lineWidth {
			buf.WriteString("\r\n")
			col = 0
		}
	}
	if col > 0 {
		buf.WriteString("\r\n")
	}
}
