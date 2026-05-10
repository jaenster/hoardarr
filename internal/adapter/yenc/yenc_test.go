package yenc

import (
	"bytes"
	"crypto/rand"
	"errors"
	"fmt"
	"hash/crc32"
	"strings"
	"testing"
)

// encodeForTest is a tiny yEnc encoder used to generate fixtures. We
// write the encoder here so tests are independent of the decoder
// implementation we're testing — the round-trip catches bugs on either
// side.
func encodeForTest(name string, payload []byte, part, total int, begin, end int64, lineWidth int) []byte {
	var buf bytes.Buffer
	if total > 0 {
		fmt.Fprintf(&buf, "=ybegin part=%d total=%d line=%d size=%d name=%s\r\n", part, total, lineWidth, end-begin+1, name)
		fmt.Fprintf(&buf, "=ypart begin=%d end=%d\r\n", begin, end)
	} else {
		fmt.Fprintf(&buf, "=ybegin line=%d size=%d name=%s\r\n", lineWidth, len(payload), name)
	}
	col := 0
	for _, b := range payload {
		out := byte(b + 42)
		critical := out == 0x00 || out == 0x0A || out == 0x0D || out == '='
		if critical || (col == 0 && (out == '\t' || out == ' ' || out == '.')) {
			buf.WriteByte('=')
			buf.WriteByte(out + 64)
			col += 2
		} else {
			buf.WriteByte(out)
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
	crc := crc32.ChecksumIEEE(payload)
	if total > 0 {
		fmt.Fprintf(&buf, "=yend size=%d part=%d pcrc32=%08x\r\n", end-begin+1, part, crc)
	} else {
		fmt.Fprintf(&buf, "=yend size=%d crc32=%08x\r\n", len(payload), crc)
	}
	return buf.Bytes()
}

func TestDecode_SinglePart_HelloWorld(t *testing.T) {
	payload := []byte("Hello, world! With special bytes: \x00\x0a\x0d\x3d here.")
	enc := encodeForTest("hello.txt", payload, 0, 0, 0, 0, 128)

	got, hdr, trl, err := Decode(bytes.NewReader(enc))
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Errorf("payload mismatch:\n  got = %q\n want = %q", got, payload)
	}
	if hdr.Name != "hello.txt" {
		t.Errorf("name = %q", hdr.Name)
	}
	if hdr.Size != int64(len(payload)) {
		t.Errorf("size = %d", hdr.Size)
	}
	if !trl.HasCRC {
		t.Errorf("HasCRC = false; want true for single-part")
	}
}

func TestDecode_MultiPart_SegmentByOffset(t *testing.T) {
	full := bytes.Repeat([]byte("ABCDEFGHIJKLMNOP"), 64) // 1024 bytes
	const partSize = 256
	for part := 1; part <= 4; part++ {
		begin := int64((part-1)*partSize + 1)
		end := int64(part * partSize)
		seg := full[begin-1 : end]
		enc := encodeForTest("file.bin", seg, part, 4, begin, end, 128)

		got, hdr, trl, err := Decode(bytes.NewReader(enc))
		if err != nil {
			t.Fatalf("part %d Decode: %v", part, err)
		}
		if !bytes.Equal(got, seg) {
			t.Errorf("part %d payload mismatch", part)
		}
		if hdr.Part != part || hdr.Total != 4 {
			t.Errorf("part %d hdr part/total = %d/%d", part, hdr.Part, hdr.Total)
		}
		if hdr.Begin != begin || hdr.End != end {
			t.Errorf("part %d hdr begin/end = %d/%d; want %d/%d", part, hdr.Begin, hdr.End, begin, end)
		}
		if !trl.HasPartCRC {
			t.Errorf("part %d HasPartCRC = false", part)
		}
	}
}

func TestDecode_RandomBinary(t *testing.T) {
	payload := make([]byte, 8192)
	if _, err := rand.Read(payload); err != nil {
		t.Fatalf("rand: %v", err)
	}
	enc := encodeForTest("blob.bin", payload, 0, 0, 0, 0, 128)
	got, _, _, err := Decode(bytes.NewReader(enc))
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Errorf("random binary mismatch (len got=%d want=%d)", len(got), len(payload))
	}
}

func TestDecode_CRCMismatch(t *testing.T) {
	payload := []byte("Hello")
	enc := encodeForTest("h.txt", payload, 0, 0, 0, 0, 128)
	// Replace the 8 hex chars of crc32= with an obviously wrong but
	// well-formed value.
	idx := bytes.Index(enc, []byte("crc32="))
	if idx < 0 {
		t.Fatal("test fixture missing crc32= marker")
	}
	copy(enc[idx+len("crc32="):idx+len("crc32=")+8], []byte("00000000"))
	_, _, _, err := Decode(bytes.NewReader(enc))
	if !errors.Is(err, ErrCRCMismatch) {
		t.Errorf("err = %v; want ErrCRCMismatch", err)
	}
}

func TestDecode_RejectsNoBegin(t *testing.T) {
	body := "no header here\r\n=yend size=0\r\n"
	_, _, _, err := Decode(strings.NewReader(body))
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestDecode_RejectsNoEnd(t *testing.T) {
	body := "=ybegin line=128 size=4 name=x\r\nXYZW\r\n"
	_, _, _, err := Decode(strings.NewReader(body))
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestDecode_DanglingEscape(t *testing.T) {
	// Line ending in '=' with nothing after it should error.
	body := "=ybegin line=128 size=1 name=x\r\nA=\r\n=yend size=1\r\n"
	_, _, _, err := Decode(strings.NewReader(body))
	if err == nil {
		t.Fatal("expected error for dangling escape")
	}
}

// Truncation: stream ends mid-body without =yend. Common when the
// NNTP conn drops mid-article. Must error cleanly, not panic, and
// not silently accept a partial decode.
func TestDecode_Truncated(t *testing.T) {
	full := encodeForTest("trunc.bin", []byte("here are some bytes"), 0, 0, 0, 0, 80)
	// Drop the trailing =yend line.
	idx := bytes.Index(full, []byte("=yend"))
	if idx < 0 {
		t.Fatal("test fixture missing =yend")
	}
	truncated := full[:idx]
	_, _, _, err := Decode(bytes.NewReader(truncated))
	if err == nil {
		t.Fatal("expected error for truncated body")
	}
	if !strings.Contains(err.Error(), "EOF") && !strings.Contains(err.Error(), "=yend") {
		t.Errorf("err = %v; want a message about missing =yend / EOF", err)
	}
}

// A zero-byte article (size=0) is valid yEnc — no body bytes between
// =ybegin and =yend. Decoder should return an empty slice, no error.
func TestDecode_EmptyBody(t *testing.T) {
	body := "=ybegin line=128 size=0 name=empty.bin\r\n=yend size=0 crc32=00000000\r\n"
	got, hdr, trl, err := Decode(strings.NewReader(body))
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("body length = %d; want 0", len(got))
	}
	if hdr.Size != 0 {
		t.Errorf("Size = %d; want 0", hdr.Size)
	}
	if !trl.HasCRC {
		t.Errorf("HasCRC = false; want true (single-part)")
	}
}

// Trailer size can drift from the actual decoded length (poster
// rounding, encoder bugs). We don't fail on this — CRC is the real
// integrity check — but the test documents the contract.
func TestDecode_TrailerSizeMismatch_NotFatal(t *testing.T) {
	payload := []byte("six bytes? no, 14 bytes here")
	enc := encodeForTest("x.bin", payload, 0, 0, 0, 0, 128)
	// Replace the =yend size= value with a wrong number, keep crc32 valid.
	idx := bytes.Index(enc, []byte("=yend size="))
	if idx < 0 {
		t.Fatal("test fixture missing =yend size=")
	}
	// Overwrite the digits after "size=" up to the next space with "999".
	pre := append([]byte(nil), enc[:idx+len("=yend size=")]...)
	rest := enc[idx+len("=yend size="):]
	spaceAt := bytes.IndexByte(rest, ' ')
	if spaceAt < 0 {
		t.Fatal("malformed fixture")
	}
	mangled := append(pre, []byte("999")...)
	mangled = append(mangled, rest[spaceAt:]...)

	got, _, trl, err := Decode(bytes.NewReader(mangled))
	if err != nil {
		t.Fatalf("Decode (size mismatch should not fail): %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Errorf("payload mismatch on size-mismatch trailer")
	}
	if trl.Size != 999 {
		t.Errorf("trailer Size = %d; want the bogus 999 we wrote", trl.Size)
	}
}

func TestDecodeLine_BasicShift(t *testing.T) {
	// Manually craft: "X" encoded is X+42 = 130 = 0x82. No critical
	// bytes here, so it's not escaped.
	var buf bytes.Buffer
	if err := decodeLine([]byte{0x82}, &buf); err != nil {
		t.Fatalf("decodeLine: %v", err)
	}
	if got := buf.Bytes(); len(got) != 1 || got[0] != 'X' {
		t.Errorf("got %v; want [X]", got)
	}
}

func TestDecodeLine_Escape(t *testing.T) {
	// '=' encoded raw is '=' (0x3D) + 42 = 0x67 = 'g'. Wait — '=' is
	// critical so it's escaped. Encoded form: '=' followed by ('=' + 42 + 64) = 0x3D + 42 + 64 = 0xA7.
	var buf bytes.Buffer
	if err := decodeLine([]byte{'=', 0xA7}, &buf); err != nil {
		t.Fatalf("decodeLine: %v", err)
	}
	if got := buf.Bytes(); len(got) != 1 || got[0] != '=' {
		t.Errorf("got %v; want [=]", got)
	}
}

func FuzzDecode(f *testing.F) {
	// Seed with a valid encoded payload.
	enc := encodeForTest("seed.bin", []byte("seed payload \x00\x0a\x0d="), 0, 0, 0, 0, 80)
	f.Add(enc)
	f.Fuzz(func(t *testing.T, body []byte) {
		// Must not panic. Any error is acceptable.
		_, _, _, _ = Decode(bytes.NewReader(body))
	})
}
