package nntp

import (
	"fmt"
	"strconv"
	"strings"
)

// FileSpec describes one file in an NZB document. The subject mirrors
// real-world Usenet posts ([part/total] - "filename" yEnc (segment/total))
// so hoardarr's subject parser extracts a sensible filename.
type FileSpec struct {
	// Filename appears in the NZB's <file subject>. The parser uses
	// this to derive the on-disk name.
	Filename string

	// Poster appears as <file poster=>. Defaults to "test@hoardarr"
	// if empty.
	Poster string

	// Groups is the newsgroup list. Defaults to ["misc.test"] if empty.
	Groups []string

	// Segments lists the segments in order. Each segment's MessageID
	// must be registered on the test server via AddArticle.
	Segments []SegmentSpec
}

// SegmentSpec is one yEnc article belonging to a FileSpec.
type SegmentSpec struct {
	MessageID string
	Bytes     int64
}

// BuildNZB serialises files as an NZB XML document. The output is
// hand-written rather than via encoding/xml because we want control
// over the exact subject formatting and attribute ordering (some
// downstream parsers are picky).
func BuildNZB(files []FileSpec) []byte {
	var b strings.Builder
	b.WriteString(`<?xml version="1.0" encoding="UTF-8"?>` + "\n")
	b.WriteString(`<nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">` + "\n")
	for fi, f := range files {
		poster := f.Poster
		if poster == "" {
			poster = "test@hoardarr"
		}
		groups := f.Groups
		if len(groups) == 0 {
			groups = []string{"misc.test"}
		}
		total := len(f.Segments)
		// Subject contains a quoted filename; XML-escape the whole
		// thing once after assembly so the inner quotes become &quot;.
		subject := xmlEscape(fmt.Sprintf(
			`[%d/%d] - "%s" yEnc (1/%d)`,
			fi+1, len(files), f.Filename, total,
		))
		fmt.Fprintf(&b,
			`  <file poster="%s" date="0" subject="%s">`+"\n",
			xmlEscape(poster), subject,
		)
		b.WriteString(`    <groups>` + "\n")
		for _, g := range groups {
			fmt.Fprintf(&b, `      <group>%s</group>`+"\n", xmlEscape(g))
		}
		b.WriteString(`    </groups>` + "\n")
		b.WriteString(`    <segments>` + "\n")
		for i, seg := range f.Segments {
			fmt.Fprintf(&b,
				`      <segment bytes="%s" number="%d">%s</segment>`+"\n",
				strconv.FormatInt(seg.Bytes, 10), i+1, xmlEscape(seg.MessageID),
			)
		}
		b.WriteString(`    </segments>` + "\n")
		b.WriteString(`  </file>` + "\n")
	}
	b.WriteString(`</nzb>` + "\n")
	return []byte(b.String())
}

// xmlEscape escapes the minimal set of XML reserved chars. Avoids the
// encoding/xml dependency so the output matches the hand-written
// pattern above without extra ceremony.
func xmlEscape(s string) string {
	r := strings.NewReplacer(
		"&", "&amp;",
		"<", "&lt;",
		">", "&gt;",
		`"`, "&quot;",
		"'", "&apos;",
	)
	return r.Replace(s)
}

// SynthesizeAndRegister registers a single-part article. Convenient
// for one-segment files; for multi-segment files, use SynthesizeFile
// instead so the =ypart headers carry the right offsets.
func (s *Server) SynthesizeAndRegister(msgID, filename string, sizeBytes int) ([]byte, SegmentSpec) {
	payload := makePayload(msgID, sizeBytes)
	body := EncodeArticle(filename, payload)
	s.AddArticle(msgID, body)
	return payload, SegmentSpec{MessageID: msgID, Bytes: int64(sizeBytes)}
}

// SynthesizeFile splits totalSize bytes into segCount equally-sized
// parts (the last part absorbs any remainder), encodes each as a
// multi-part yEnc article with =ypart begin/end set so the receiver
// assembles them at the right offsets, and registers all parts on
// the server. Returns the concatenated payload (so callers can
// assert byte equivalence) and a FileSpec ready to drop into
// BuildNZB.
//
// msgIDPrefix is the base for message-ids — segments get
// "<prefix>-1@h", "<prefix>-2@h", etc.
func (s *Server) SynthesizeFile(msgIDPrefix, filename string, totalSize, segCount int) ([]byte, FileSpec) {
	if segCount <= 0 {
		segCount = 1
	}
	if segCount > totalSize {
		segCount = totalSize
	}
	payload := makePayload(msgIDPrefix, totalSize)
	base := totalSize / segCount
	segs := make([]SegmentSpec, 0, segCount)
	for i := 0; i < segCount; i++ {
		start := i * base
		end := start + base
		if i == segCount-1 {
			end = totalSize
		}
		// =ypart uses 1-based inclusive offsets.
		begin1 := int64(start + 1)
		end1 := int64(end)
		msgID := fmt.Sprintf("%s-%d@h", msgIDPrefix, i+1)
		body := EncodeArticlePart(filename, payload[start:end], begin1, end1, int64(totalSize), i+1, segCount)
		s.AddArticle(msgID, body)
		segs = append(segs, SegmentSpec{MessageID: msgID, Bytes: int64(end - start)})
	}
	return payload, FileSpec{Filename: filename, Segments: segs}
}

// makePayload returns sizeBytes of deterministic bytes derived from
// seed. A simple xorshift keeps it cheap and visually obvious.
func makePayload(seed string, size int) []byte {
	var x uint32 = 2166136261
	for i := 0; i < len(seed); i++ {
		x ^= uint32(seed[i])
		x *= 16777619
	}
	out := make([]byte, size)
	for i := 0; i < size; i++ {
		x ^= x << 13
		x ^= x >> 17
		x ^= x << 5
		out[i] = byte(x)
	}
	return out
}
