package nzb

import (
	"strings"
	"testing"
)

const minimalNZB = `<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE nzb PUBLIC "-//newzBin//DTD NZB 1.1//EN" "http://www.newzbin.com/DTD/nzb/nzb-1.1.dtd">
<nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">
  <head>
    <meta type="title">Some.Release.Title</meta>
    <meta type="category">tv</meta>
  </head>
  <file poster="user@host.invalid" date="1700000000" subject='[1/2] - "release.r00" yEnc (1/2)'>
    <groups>
      <group>alt.binaries.test</group>
      <group>alt.binaries.misc</group>
    </groups>
    <segments>
      <segment bytes="715000" number="1">msg1@host</segment>
      <segment bytes="615000" number="2">msg2@host</segment>
    </segments>
  </file>
  <file poster="user@host.invalid" date="1700000001" subject='[2/2] - "release.par2" yEnc (1/1)'>
    <groups>
      <group>alt.binaries.test</group>
    </groups>
    <segments>
      <segment bytes="100000" number="1">par2-msg@host</segment>
    </segments>
  </file>
</nzb>
`

func TestParse_MinimalNZB(t *testing.T) {
	doc, err := ParseBytes([]byte(minimalNZB))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if len(doc.Meta) != 2 {
		t.Errorf("meta count = %d; want 2", len(doc.Meta))
	}
	if doc.Meta[0].Type != "title" || doc.Meta[0].Value != "Some.Release.Title" {
		t.Errorf("meta[0] = %+v", doc.Meta[0])
	}
	if len(doc.Files) != 2 {
		t.Fatalf("file count = %d; want 2", len(doc.Files))
	}

	f0 := doc.Files[0]
	if f0.Poster != "user@host.invalid" {
		t.Errorf("poster = %q", f0.Poster)
	}
	if f0.Filename != "release.r00" {
		t.Errorf("filename = %q; want release.r00", f0.Filename)
	}
	if len(f0.Groups) != 2 {
		t.Errorf("groups = %v", f0.Groups)
	}
	if len(f0.Segments) != 2 {
		t.Fatalf("segments = %d", len(f0.Segments))
	}
	if f0.Segments[0].MessageID != "msg1@host" {
		t.Errorf("seg[0] msgid = %q", f0.Segments[0].MessageID)
	}
	if f0.Segments[0].Bytes != 715000 {
		t.Errorf("seg[0] bytes = %d", f0.Segments[0].Bytes)
	}
	if f0.Date.Unix() != 1_700_000_000 {
		t.Errorf("date = %v", f0.Date)
	}
}

func TestParse_StripsAngleBracketsFromMessageID(t *testing.T) {
	body := strings.Replace(minimalNZB, `>msg1@host<`, `>&lt;msg1@host&gt;<`, 1)
	doc, err := ParseBytes([]byte(body))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if doc.Files[0].Segments[0].MessageID != "msg1@host" {
		t.Errorf("msgid = %q; want msg1@host", doc.Files[0].Segments[0].MessageID)
	}
}

func TestParse_ISO88591(t *testing.T) {
	// Latin-1 encoded NZB with a non-ASCII char in the subject. The
	// é (0xE9 in Latin-1) must round-trip to U+00E9 in the output.
	header := []byte(`<?xml version="1.0" encoding="iso-8859-1"?>` + "\n" + `<nzb><head/><file poster="x" date="0" subject='"caf`)
	suffix := []byte(`.rar"'>` + "\n" + `<groups><group>g</group></groups>` + "\n" + `<segments><segment bytes="1" number="1">m@h</segment></segments>` + "\n" + `</file></nzb>`)
	body := append(append(header, 0xE9), suffix...)
	doc, err := ParseBytes(body)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	got := doc.Files[0].Filename
	if !strings.Contains(got, "café") {
		t.Errorf("filename = %q; want contain café", got)
	}
}

func TestParse_RejectsEmpty(t *testing.T) {
	_, err := ParseBytes([]byte(`<nzb><head/></nzb>`))
	if err == nil {
		t.Fatal("expected error for empty <nzb>")
	}
}

func TestParse_RejectsFileWithNoSegments(t *testing.T) {
	body := `<nzb><head/><file poster="x" date="0" subject='"x.rar"'><groups><group>g</group></groups><segments></segments></file></nzb>`
	_, err := ParseBytes([]byte(body))
	if err == nil {
		t.Fatal("expected error for file with no segments")
	}
}

func TestParseFilenameFromSubject(t *testing.T) {
	cases := []struct {
		subject string
		want    string
	}{
		{`[1/2] - "release.r00" yEnc (1/2)`, "release.r00"},
		{`Some.Release [01/12] - "release.par2" yEnc (1/1)`, "release.par2"},
		{`"file.rar" yEnc (1/3)`, "file.rar"},
		{`Foo Bar - file.rar - more`, "file.rar"},
		{``, ""},
		// Obfuscated subject lines from private indexers — filename
		// is wrapped in []s and the quote slot is empty. The bracket
		// extractor catches these.
		{
			`[N3wZ] \jmAl6g259274\::[PRiVATE]-[WtFnZb]-[Monster.2022.S02E09.mkv]-[1/2] - "" yEnc  7337320579 (1/10237)`,
			"Monster.2022.S02E09.mkv",
		},
		{
			`[PRiVATE]-[WtFnZb]-[Release.Name.2160p.HDR.mkv]-[1/2] - "" yEnc 1234`,
			"Release.Name.2160p.HDR.mkv",
		},
		{
			`Foo - [release.par2]-[1/1] - "" yEnc (1/1)`,
			"release.par2",
		},
	}
	for _, c := range cases {
		t.Run(c.subject, func(t *testing.T) {
			got := parseFilenameFromSubject(c.subject)
			if got != c.want {
				t.Errorf("got %q; want %q", got, c.want)
			}
		})
	}
}

func FuzzParse(f *testing.F) {
	f.Add([]byte(minimalNZB))
	f.Fuzz(func(t *testing.T, body []byte) {
		// Must not panic. We accept any error.
		_, _ = ParseBytes(body)
	})
}
