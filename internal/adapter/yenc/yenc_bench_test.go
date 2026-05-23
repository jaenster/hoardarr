package yenc

import (
	"bytes"
	"crypto/rand"
	"testing"
)

// makeBench encodes a random payload of the given size as a single-part
// yEnc article (representative of what arrives off the wire). We use
// rand.Read so the payload exercises both raw and escaped bytes —
// roughly 4/256 = 1.6% of encoded bytes will be critical and escaped,
// which matches real-world binaries.
func makeBench(b *testing.B, size int) []byte {
	b.Helper()
	payload := make([]byte, size)
	if _, err := rand.Read(payload); err != nil {
		b.Fatalf("rand: %v", err)
	}
	return encodeForTest("bench.bin", payload, 0, 0, 0, 0, 128)
}

func benchmarkDecode(b *testing.B, size int) {
	enc := makeBench(b, size)
	b.SetBytes(int64(size))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _, err := Decode(bytes.NewReader(enc))
		if err != nil {
			b.Fatalf("Decode: %v", err)
		}
	}
}

// Largest realistic article: news servers typically cap around 1 MiB
// per article (some allow more); this is the upper-bound hot path.
func BenchmarkDecode_1MiB(b *testing.B) { benchmarkDecode(b, 1024*1024) }

// Typical single-article size: most posters split binaries into ~750
// KiB segments. The dominant production workload.
func BenchmarkDecode_750KiB(b *testing.B) { benchmarkDecode(b, 750*1024) }

// Multi-part segment (typical par2/rar split): 256 KiB.
func BenchmarkDecode_256KiB(b *testing.B) { benchmarkDecode(b, 256*1024) }

// Tiny article: surfaces per-call overhead. With sync.Pool the input
// buffer alloc amortises to ~zero; this confirms small calls don't
// regress.
func BenchmarkDecode_4KiB(b *testing.B) { benchmarkDecode(b, 4*1024) }

// Multi-part decode: 4 segments of 256 KiB each, decoded back-to-back.
// Exercises the =ypart parsing path.
func BenchmarkDecode_MultiPart(b *testing.B) {
	const partSize = 256 * 1024
	const parts = 4
	full := make([]byte, partSize*parts)
	if _, err := rand.Read(full); err != nil {
		b.Fatalf("rand: %v", err)
	}
	segs := make([][]byte, parts)
	for p := 1; p <= parts; p++ {
		begin := int64((p-1)*partSize + 1)
		end := int64(p * partSize)
		segs[p-1] = encodeForTest("multi.bin", full[begin-1:end], p, parts, begin, end, 128)
	}
	b.SetBytes(int64(partSize * parts))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, enc := range segs {
			if _, _, _, err := Decode(bytes.NewReader(enc)); err != nil {
				b.Fatalf("Decode: %v", err)
			}
		}
	}
}

// Scan-and-decode variant — same fixtures, alternative implementation
// (DecodeScan). Kept side-by-side so we can attribute throughput deltas
// to the inner-loop strategy, not machine noise.
func benchmarkDecodeScan(b *testing.B, size int) {
	enc := makeBench(b, size)
	b.SetBytes(int64(size))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _, _, err := DecodeScan(bytes.NewReader(enc))
		if err != nil {
			b.Fatalf("DecodeScan: %v", err)
		}
	}
}

func BenchmarkDecodeScan_1MiB(b *testing.B)   { benchmarkDecodeScan(b, 1024*1024) }
func BenchmarkDecodeScan_750KiB(b *testing.B) { benchmarkDecodeScan(b, 750*1024) }
func BenchmarkDecodeScan_256KiB(b *testing.B) { benchmarkDecodeScan(b, 256*1024) }
func BenchmarkDecodeScan_4KiB(b *testing.B)   { benchmarkDecodeScan(b, 4*1024) }

// Inner-loop microbench: subCopy42 on its own. Isolates SWAR
// subtract throughput from everything else. The buffer is a 64 KiB
// scratch so the working set stays in L1.
func BenchmarkSubCopy42(b *testing.B) {
	const n = 64 * 1024
	src := make([]byte, n)
	dst := make([]byte, n)
	if _, err := rand.Read(src); err != nil {
		b.Fatalf("rand: %v", err)
	}
	b.SetBytes(int64(n))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		subCopy42(dst, src)
	}
}
