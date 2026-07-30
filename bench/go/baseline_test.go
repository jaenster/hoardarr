// Package gobaseline holds the Go-side numbers the Zig port is measured
// against. It exists only for the duration of the port: `bench/run.sh`
// runs these and `zig build bench` back to back so both columns come off
// the same machine in the same thermal state, and the whole directory is
// deleted along with the rest of the Go tree when the port lands.
//
// The benchmarks mirror bench/main.zig one for one: same payload sizes,
// same yEnc line width, same NZB shape, same TOML document. Payload bytes
// are random in both, so escape density (~1.6% of encoded bytes) matches
// even though the two PRNGs produce different bytes — for a throughput
// measurement that's what has to be equal, not the exact byte values.
package gobaseline

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"hash/crc32"
	"strings"
	"testing"

	"github.com/BurntSushi/toml"
	"github.com/jaenster/hoardarr/internal/adapter/nzb"
	"github.com/jaenster/hoardarr/internal/adapter/yenc"
)

// 750 KiB: the size most posters split binaries into, and the
// granularity the decoder actually runs at in production.
const articlePayload = 750 * 1024

func randBytes(tb testing.TB, n int) []byte {
	tb.Helper()
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		tb.Fatalf("rand: %v", err)
	}
	return b
}

func BenchmarkCRC32(b *testing.B) {
	buf := randBytes(b, articlePayload)
	b.SetBytes(int64(len(buf)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if crc32.ChecksumIEEE(buf) == 0xdeadbeef {
			b.Fatal("unreachable, keeps the call live")
		}
	}
}

// encodeArticle produces the same article shape bench/main.zig builds:
// single-part, line=128, random payload.
func encodeArticle(b *testing.B) []byte {
	b.Helper()
	payload := randBytes(b, articlePayload)

	// yenc.encodeForTest is unexported, so build the article here. The
	// encoder is trivial and this keeps the baseline honest — we measure
	// the real Decode against input it would see off the wire.
	var out bytes.Buffer
	fmt.Fprintf(&out, "=ybegin line=128 size=%d name=bench.bin\r\n", len(payload))
	col := 0
	for _, c := range payload {
		e := c + 42
		switch e {
		case 0x00, '\n', '\r', '=':
			out.WriteByte('=')
			out.WriteByte(e + 64)
			col += 2
		default:
			out.WriteByte(e)
			col++
		}
		if col >= 128 {
			out.WriteString("\r\n")
			col = 0
		}
	}
	if col > 0 {
		out.WriteString("\r\n")
	}
	fmt.Fprintf(&out, "=yend size=%d crc32=%08x\r\n", len(payload), crc32.ChecksumIEEE(payload))
	return out.Bytes()
}

func BenchmarkYencDecode(b *testing.B) {
	enc := encodeArticle(b)
	b.SetBytes(articlePayload)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, _, err := yenc.Decode(bytes.NewReader(enc)); err != nil {
			b.Fatalf("Decode: %v", err)
		}
	}
}

// makeNZB mirrors the Zig harness: 50 files x 200 segments, the shape of
// a full season pack, which is where parse time shows up in the UI.
func makeNZB() []byte {
	var sb strings.Builder
	sb.WriteString(`<?xml version="1.0" encoding="iso-8859-1"?>` + "\n")
	sb.WriteString(`<nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">` + "\n")
	sb.WriteString(`<head><meta type="name">Bench Release</meta></head>` + "\n")
	for f := 0; f < 50; f++ {
		fmt.Fprintf(&sb,
			`<file poster="bench &lt;bench@example.invalid&gt;" date="1700000000" subject="[%d/50] - &quot;bench.part%03d.rar&quot; yEnc (1/200)">`+"\n",
			f+1, f+1)
		sb.WriteString("<groups><group>alt.binaries.test</group><group>alt.binaries.misc</group></groups>\n<segments>\n")
		for s := 0; s < 200; s++ {
			fmt.Fprintf(&sb,
				"<segment bytes=\"768000\" number=\"%d\">part%dseg%d@news.example.invalid</segment>\n",
				s+1, f, s)
		}
		sb.WriteString("</segments>\n</file>\n")
	}
	sb.WriteString("</nzb>\n")
	return []byte(sb.String())
}

func BenchmarkNZBParse(b *testing.B) {
	src := makeNZB()
	b.SetBytes(int64(len(src)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := nzb.ParseBytes(src); err != nil {
			b.Fatalf("Parse: %v", err)
		}
	}
}

// The same document bench/main.zig parses.
const tomlDoc = `listen = ":8085"
data_dir = "/data"
api_key = "0123456789abcdef0123456789abcdef"
url_base = ""

[log]
level = "info"
format = "text"
max_size_mb = 32

[download]
incomplete_dir = "incomplete"
complete_dir = "complete"
bandwidth_global = 0
max_connections = 40

[server]
hosts = ["news.example.invalid", "news2.example.invalid"]
tls = true
port = 563
`

func BenchmarkTOMLParse(b *testing.B) {
	b.SetBytes(int64(len(tomlDoc)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var v map[string]any
		if _, err := toml.Decode(tomlDoc, &v); err != nil {
			b.Fatalf("Decode: %v", err)
		}
	}
}
