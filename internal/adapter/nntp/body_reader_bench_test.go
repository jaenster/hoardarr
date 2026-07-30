package nntp

import (
	"bufio"
	"bytes"
	"math/rand"
	"testing"
)

// Scaffolding for the Zig port's benchmark comparison. fastBodyReader is
// unexported, so this has to live in-package; it is deleted along with the
// rest of the Go tree when the port lands.
//
// Mirrors bench/main.zig's makeStuffedBody: 128-byte lines matching the
// yEnc wrap width articles actually arrive with, a dot-stuffed line every
// 64th line so the stuffing path runs at a realistic rate, and a total
// close to the 750 KiB a typical article carries.
func makeStuffedBody() []byte {
	const target = 750 * 1024
	rng := rand.New(rand.NewSource(0xD07))
	var out bytes.Buffer
	written := 0
	for written < target {
		line := make([]byte, 128)
		for i := range line {
			line[i] = byte(0x21 + rng.Intn(0x7E-0x21+1))
		}
		if written%(64*130) == 0 {
			line[0] = '.'
		}
		out.Write(line)
		out.WriteString("\r\n")
		written += len(line) + 2
	}
	out.WriteString(".\r\n")
	return out.Bytes()
}

func BenchmarkFastBodyReader(b *testing.B) {
	body := makeStuffedBody()
	dst := make([]byte, len(body))
	b.SetBytes(int64(len(body)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r := newFastBodyReader(bufio.NewReaderSize(bytes.NewReader(body), 64*1024))
		total := 0
		for {
			n, err := r.Read(dst[total:])
			total += n
			if err != nil {
				break
			}
		}
	}
}
