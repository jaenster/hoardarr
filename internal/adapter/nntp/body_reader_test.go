package nntp

import (
	"bufio"
	"bytes"
	"crypto/rand"
	"io"
	"net/textproto"
	"strings"
	"testing"
)

// nntpBody renders payload as an NNTP wire body: CRLF-terminated
// lines, any line starting with "." prefixed with another ".", and
// a final ".\r\n" terminator.
func nntpBody(payload []byte) []byte {
	var buf bytes.Buffer
	// Split on '\n' boundaries; strip trailing \r if present.
	lines := bytes.SplitAfter(payload, []byte("\n"))
	for _, ln := range lines {
		// Strip the terminator we'll re-add.
		stripped := bytes.TrimRight(ln, "\r\n")
		if len(stripped) == 0 && len(ln) == 0 {
			continue
		}
		if len(stripped) > 0 && stripped[0] == '.' {
			buf.WriteByte('.') // stuff
		}
		buf.Write(stripped)
		buf.WriteString("\r\n")
	}
	buf.WriteString(".\r\n")
	return buf.Bytes()
}

func TestFastBodyReader_ShortPayload(t *testing.T) {
	payload := []byte("hello\nworld\n")
	r := newFastBodyReader(bufio.NewReader(bytes.NewReader(nntpBody(payload))))
	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	// CRLF is normalised to LF (matches textproto.DotReader contract).
	want := "hello\nworld\n"
	if string(got) != want {
		t.Errorf("got %q; want %q", got, want)
	}
}

func TestFastBodyReader_DotStuffing(t *testing.T) {
	// Caller passes a body line that begins with '.'. The wire body
	// will have it stuffed; fastBodyReader must un-stuff.
	payload := []byte(".alpha\nnormal\n..double\n")
	r := newFastBodyReader(bufio.NewReader(bytes.NewReader(nntpBody(payload))))
	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	want := ".alpha\nnormal\n..double\n"
	if string(got) != want {
		t.Errorf("got %q; want %q", got, want)
	}
}

func TestFastBodyReader_EmptyBody(t *testing.T) {
	r := newFastBodyReader(bufio.NewReader(bytes.NewReader([]byte(".\r\n"))))
	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("got %d bytes; want empty", len(got))
	}
}

func TestFastBodyReader_BareLF(t *testing.T) {
	// Lenient handling of a terminator with bare LF (no \r).
	r := newFastBodyReader(bufio.NewReader(bytes.NewReader([]byte("hi\n.\n"))))
	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if string(got) != "hi\n" {
		t.Errorf("got %q; want %q", got, "hi\n")
	}
}

// Result against textproto.dotReader on the same random body — a
// byte-for-byte regression guard. Our reader normalises CRLF to LF
// the same way stdlib does, so the outputs should be identical.
func TestFastBodyReader_MatchesStdlib(t *testing.T) {
	payload := make([]byte, 64*1024)
	if _, err := rand.Read(payload); err != nil {
		t.Fatalf("rand: %v", err)
	}
	body := nntpBody(payload)

	fast := newFastBodyReader(bufio.NewReader(bytes.NewReader(body)))
	gotFast, err := io.ReadAll(fast)
	if err != nil {
		t.Fatalf("fast ReadAll: %v", err)
	}

	tr := textproto.NewReader(bufio.NewReader(bytes.NewReader(body)))
	gotStd, err := io.ReadAll(tr.DotReader())
	if err != nil {
		t.Fatalf("std ReadAll: %v", err)
	}

	if !bytes.Equal(gotFast, gotStd) {
		t.Errorf("mismatch: fast len=%d std len=%d", len(gotFast), len(gotStd))
	}
}

// Benchmark — same fixture for both readers so the only variable is
// the parser. Uses a realistic 750 KiB body with random bytes
// (matches yEnc-encoded entropy roughly; dot density is ~1/256).
func makeBenchBody(b *testing.B, size int) []byte {
	b.Helper()
	payload := make([]byte, size)
	if _, err := rand.Read(payload); err != nil {
		b.Fatalf("rand: %v", err)
	}
	// Inject newlines every ~128 bytes so the body has yEnc-shaped lines.
	for i := 128; i < len(payload); i += 128 {
		payload[i-1] = '\r'
		payload[i] = '\n'
	}
	return nntpBody(payload)
}

func BenchmarkBodyRead_Stdlib(b *testing.B) {
	body := makeBenchBody(b, 750*1024)
	b.SetBytes(int64(len(body)))
	b.ReportAllocs()
	dst := make([]byte, 32*1024)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tr := textproto.NewReader(bufio.NewReader(bytes.NewReader(body)))
		r := tr.DotReader()
		for {
			_, err := r.Read(dst)
			if err == io.EOF {
				break
			}
			if err != nil {
				b.Fatalf("Read: %v", err)
			}
		}
	}
}

func BenchmarkBodyRead_Fast(b *testing.B) {
	body := makeBenchBody(b, 750*1024)
	b.SetBytes(int64(len(body)))
	b.ReportAllocs()
	dst := make([]byte, 32*1024)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r := newFastBodyReader(bufio.NewReader(bytes.NewReader(body)))
		for {
			_, err := r.Read(dst)
			if err == io.EOF {
				break
			}
			if err != nil {
				b.Fatalf("Read: %v", err)
			}
		}
	}
}

// Smaller body — exercises per-call setup overhead, e.g. for short
// yEnc segments or SAB control responses.
func BenchmarkBodyRead_Stdlib_Small(b *testing.B) {
	body := makeBenchBody(b, 4*1024)
	b.SetBytes(int64(len(body)))
	b.ReportAllocs()
	dst := make([]byte, 4*1024)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tr := textproto.NewReader(bufio.NewReader(bytes.NewReader(body)))
		r := tr.DotReader()
		for {
			_, err := r.Read(dst)
			if err == io.EOF {
				break
			}
			if err != nil {
				b.Fatalf("Read: %v", err)
			}
		}
	}
}

func BenchmarkBodyRead_Fast_Small(b *testing.B) {
	body := makeBenchBody(b, 4*1024)
	b.SetBytes(int64(len(body)))
	b.ReportAllocs()
	dst := make([]byte, 4*1024)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r := newFastBodyReader(bufio.NewReader(bytes.NewReader(body)))
		for {
			_, err := r.Read(dst)
			if err == io.EOF {
				break
			}
			if err != nil {
				b.Fatalf("Read: %v", err)
			}
		}
	}
}

// Sanity guard: make sure ReadSlice path doesn't crash on weird input.
func TestFastBodyReader_NoTerminator(t *testing.T) {
	r := newFastBodyReader(bufio.NewReader(strings.NewReader("partial\n")))
	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if string(got) != "partial\n" {
		t.Errorf("got %q", got)
	}
}
