// Package nzb parses NZB documents (the XML manifest used to describe
// a Usenet release split across many articles).
//
// An NZB describes one or more files, each split into one or more
// "segments" (articles posted to Usenet). The parser is a faithful
// translation of the XML into a Document struct; the application
// layer converts a Document into a download.Job aggregate.
//
// Reference: https://sabnzbd.org/wiki/extra/nzb-spec — note that real
// NZBs in the wild deviate freely (custom xmlns, missing meta, non-UTF
// encodings, weird subject formats). The parser is intentionally
// lenient about everything it can be lenient about; it only fails on
// structural impossibility (e.g. a <segments> element with no children).
package nzb

import (
	"bufio"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"strings"
	"time"

	"golang.org/x/text/encoding/charmap"
)

// Document is the top-level parsed NZB.
type Document struct {
	Meta  []Meta
	Files []File
}

// Meta is one <meta type="...">value</meta> entry under <head>.
type Meta struct {
	Type  string
	Value string
}

// File is one <file> element: a single logical file split across
// segments.
type File struct {
	// Poster is the "poster" attribute of <file>. Optional.
	Poster string
	// Date is parsed from the unix-timestamp "date" attribute. Zero
	// time if absent or unparseable.
	Date time.Time
	// Subject is the raw "subject" attribute of <file>.
	Subject string
	// Filename is extracted from Subject (first quoted token). If no
	// quoted token is present, falls back to a best-effort heuristic.
	Filename string
	// Groups are the newsgroups the file's segments live in.
	Groups []string
	// Segments are the article references, in NZB order. The parser
	// does not sort by Number — callers that need ordering should
	// sort explicitly.
	Segments []Segment
}

// Segment is one <segment> element: an article reference.
type Segment struct {
	// Bytes is the declared article size on the wire (overhead included),
	// not the decoded data size.
	Bytes int64
	// Number is the 1-based segment number.
	Number int
	// MessageID is the article's Message-ID without surrounding angle
	// brackets. NNTP transport adds the brackets when issuing
	// ARTICLE/BODY commands.
	MessageID string
}

// Parse reads an NZB document from r.
func Parse(r io.Reader) (*Document, error) {
	dec := xml.NewDecoder(bufio.NewReader(r))
	dec.CharsetReader = charsetReader
	dec.Strict = false       // some NZBs have unescaped & in subjects
	dec.AutoClose = nil      // explicit closing tags only
	dec.Entity = xml.HTMLEntity

	var raw xmlNZB
	if err := dec.Decode(&raw); err != nil {
		return nil, fmt.Errorf("decode nzb: %w", err)
	}

	doc := &Document{
		Meta:  make([]Meta, 0, len(raw.Head.Meta)),
		Files: make([]File, 0, len(raw.Files)),
	}
	for _, m := range raw.Head.Meta {
		doc.Meta = append(doc.Meta, Meta{Type: strings.TrimSpace(m.Type), Value: strings.TrimSpace(m.Value)})
	}

	for i, f := range raw.Files {
		df, err := convertFile(f)
		if err != nil {
			return nil, fmt.Errorf("file[%d]: %w", i, err)
		}
		doc.Files = append(doc.Files, df)
	}

	if len(doc.Files) == 0 {
		return nil, errors.New("nzb: no files")
	}
	return doc, nil
}

// ParseBytes is a convenience over Parse.
func ParseBytes(b []byte) (*Document, error) {
	return Parse(strings.NewReader(string(b)))
}

func convertFile(f xmlFile) (File, error) {
	if len(f.Segments.Segments) == 0 {
		return File{}, errors.New("file has no segments")
	}
	out := File{
		Poster:  f.Poster,
		Subject: f.Subject,
		Groups:  make([]string, 0, len(f.Groups.Groups)),
		Segments: make([]Segment, 0, len(f.Segments.Segments)),
	}
	if f.Date != "" {
		if ts, err := strconv.ParseInt(f.Date, 10, 64); err == nil {
			out.Date = time.Unix(ts, 0).UTC()
		}
	}
	out.Filename = parseFilenameFromSubject(f.Subject)
	for _, g := range f.Groups.Groups {
		g = strings.TrimSpace(g)
		if g != "" {
			out.Groups = append(out.Groups, g)
		}
	}
	for i, s := range f.Segments.Segments {
		ds, err := convertSegment(s)
		if err != nil {
			return File{}, fmt.Errorf("segment[%d]: %w", i, err)
		}
		out.Segments = append(out.Segments, ds)
	}
	return out, nil
}

func convertSegment(s xmlSegment) (Segment, error) {
	mid := strings.TrimSpace(s.MessageID)
	if mid == "" {
		return Segment{}, errors.New("missing message-id")
	}
	mid = strings.TrimPrefix(mid, "<")
	mid = strings.TrimSuffix(mid, ">")
	if err := validateMessageID(mid); err != nil {
		return Segment{}, err
	}
	num, err := strconv.Atoi(s.Number)
	if err != nil || num < 1 {
		return Segment{}, fmt.Errorf("bad segment number %q", s.Number)
	}
	bytesN, _ := strconv.ParseInt(s.Bytes, 10, 64) // missing bytes is OK; pool sizes from yEnc
	return Segment{
		Bytes:     bytesN,
		Number:    num,
		MessageID: mid,
	}, nil
}

// validateMessageID rejects message-ids containing characters that
// would corrupt NNTP command lines. NNTP uses CRLF as command
// terminator; a CR/LF inside a message-id would let a malicious NZB
// inject a second BODY (or any other) command on the same connection.
//
// Spec-wise, RFC 5536 limits message-ids to printable US-ASCII
// excluding angle brackets, whitespace, and a few reserved chars.
// We use a permissive but injection-safe filter: reject control
// characters and whitespace.
func validateMessageID(mid string) error {
	if mid == "" {
		return errors.New("nzb: empty message-id")
	}
	for i := 0; i < len(mid); i++ {
		b := mid[i]
		if b < 0x21 || b == 0x7f {
			return fmt.Errorf("nzb: message-id contains control or whitespace byte 0x%02x at offset %d", b, i)
		}
	}
	return nil
}

// parseFilenameFromSubject tries to recover the filename from a Usenet
// subject line. Convention is to wrap it in double quotes:
//
//	[Group] [1/15] - "filename.rar" - [4194304/4194304] yEnc (1/8)
//
// We take the first quoted token. If no quotes, we fall back to the
// last whitespace-delimited token that looks like a filename
// (contains a "."), which is the convention some posters use.
func parseFilenameFromSubject(subject string) string {
	if m := firstQuotedRE.FindStringSubmatch(subject); m != nil {
		return strings.TrimSpace(m[1])
	}
	// Fallback heuristic.
	tokens := strings.Fields(subject)
	for i := len(tokens) - 1; i >= 0; i-- {
		t := tokens[i]
		// Trim trailing punctuation that often appears.
		t = strings.TrimRight(t, ",;)]}\"'")
		t = strings.TrimLeft(t, "([{\"'")
		if strings.Contains(t, ".") && !strings.ContainsAny(t, "/\\") {
			return t
		}
	}
	return ""
}

var firstQuotedRE = regexp.MustCompile(`"([^"]+)"`)

// charsetReader handles non-UTF-8 NZBs. The most common alternative
// in the wild is iso-8859-1 (the DTD's declared default). Anything
// else falls through to the default UTF-8 decoder.
func charsetReader(label string, input io.Reader) (io.Reader, error) {
	switch strings.ToLower(label) {
	case "iso-8859-1", "iso_8859-1", "latin1", "windows-1252", "":
		return charmap.ISO8859_1.NewDecoder().Reader(input), nil
	case "utf-8", "utf8":
		return input, nil
	default:
		return nil, fmt.Errorf("nzb: unsupported charset %q", label)
	}
}

// Internal XML structs — match the on-disk schema, then converted to
// the public Document/File/Segment shape so callers don't see
// XML-driven idioms.
type xmlNZB struct {
	XMLName xml.Name  `xml:"nzb"`
	Head    xmlHead   `xml:"head"`
	Files   []xmlFile `xml:"file"`
}

type xmlHead struct {
	Meta []xmlMeta `xml:"meta"`
}

type xmlMeta struct {
	Type  string `xml:"type,attr"`
	Value string `xml:",chardata"`
}

type xmlFile struct {
	Poster   string         `xml:"poster,attr"`
	Date     string         `xml:"date,attr"`
	Subject  string         `xml:"subject,attr"`
	Groups   xmlGroups      `xml:"groups"`
	Segments xmlSegmentList `xml:"segments"`
}

type xmlGroups struct {
	Groups []string `xml:"group"`
}

type xmlSegmentList struct {
	Segments []xmlSegment `xml:"segment"`
}

type xmlSegment struct {
	Bytes     string `xml:"bytes,attr"`
	Number    string `xml:"number,attr"`
	MessageID string `xml:",chardata"`
}
