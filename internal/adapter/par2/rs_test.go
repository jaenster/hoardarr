package par2

import (
	"bytes"
	"crypto/rand"
	"testing"
)

func TestSliceToElementsRoundTrip(t *testing.T) {
	for _, n := range []int{0, 2, 4, 16, 1024} {
		buf := make([]byte, n)
		if n > 0 {
			_, _ = rand.Read(buf)
		}
		elems := SliceToElements(buf)
		back := ElementsToSlice(elems)
		if !bytes.Equal(buf, back) {
			t.Errorf("round-trip mismatch at n=%d", n)
		}
	}
}

func TestSplitIntoSlicesPads(t *testing.T) {
	// 10 bytes into 4-byte slices → 3 slices, last padded with two zero
	// bytes.
	data := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	slices := SplitIntoSlices(data, 4)
	if len(slices) != 3 {
		t.Fatalf("got %d slices; want 3", len(slices))
	}
	if !bytes.Equal(slices[0], []byte{1, 2, 3, 4}) {
		t.Errorf("slice 0 = %v", slices[0])
	}
	if !bytes.Equal(slices[2], []byte{9, 10, 0, 0}) {
		t.Errorf("slice 2 = %v; want last 2 bytes zero", slices[2])
	}
}

func TestEncodeReconstructHappyPath(t *testing.T) {
	// 4 data slices of 8 bytes each. Encode 2 recovery slices. Damage
	// 2 data slices. Recover. Assert byte-equivalence.
	const sliceSize = 8
	const dataSlices = 4
	original := make([][]byte, dataSlices)
	for i := range original {
		original[i] = make([]byte, sliceSize)
		_, _ = rand.Read(original[i])
	}

	// Exponents 1 and 2 — both non-zero, deterministic for testing.
	exps := []uint16{1, 2}
	recovery := map[uint16][]byte{}
	for _, e := range exps {
		recovery[e] = EncodeRecoverySlice(original, e)
	}

	// Damage slices 1 and 3.
	missing := []int{1, 3}
	present := make([][]byte, dataSlices)
	for i, s := range original {
		if isMissing(missing, i) {
			present[i] = nil
		} else {
			present[i] = append([]byte{}, s...)
		}
	}

	out, err := Reconstruct(ReconstructInput{
		N: dataSlices, SliceSize: sliceSize,
		Present: present, MissingIdx: missing,
		Recovery: recovery,
	})
	if err != nil {
		t.Fatalf("Reconstruct: %v", err)
	}
	if len(out) != 2 {
		t.Fatalf("got %d reconstructed slices; want 2", len(out))
	}
	if !bytes.Equal(out[0], original[1]) {
		t.Errorf("reconstructed slice 1 mismatch")
	}
	if !bytes.Equal(out[1], original[3]) {
		t.Errorf("reconstructed slice 3 mismatch")
	}
}

func TestReconstructTooFewRecoverySlices(t *testing.T) {
	// 3 data slices; 2 missing but only 1 recovery slice available.
	const sliceSize = 4
	data := [][]byte{
		{1, 2, 3, 4},
		{5, 6, 7, 8},
		{9, 10, 11, 12},
	}
	recovery := map[uint16][]byte{1: EncodeRecoverySlice(data, 1)}
	_, err := Reconstruct(ReconstructInput{
		N: 3, SliceSize: sliceSize,
		Present: [][]byte{data[0], nil, nil}, MissingIdx: []int{1, 2},
		Recovery: recovery,
	})
	if err != ErrUnrecoverable {
		t.Errorf("err = %v; want ErrUnrecoverable", err)
	}
}

func TestEncodeReconstructLargerRandom(t *testing.T) {
	// 16 data slices, 4096 bytes each. Damage 4. Provide 4 recovery
	// slices. Verify reconstruction byte-by-byte.
	const sliceSize = 4096
	const dataSlices = 16
	original := make([][]byte, dataSlices)
	for i := range original {
		original[i] = make([]byte, sliceSize)
		_, _ = rand.Read(original[i])
	}
	exps := []uint16{1, 2, 4, 8}
	recovery := map[uint16][]byte{}
	for _, e := range exps {
		recovery[e] = EncodeRecoverySlice(original, e)
	}

	missing := []int{2, 5, 11, 14}
	present := make([][]byte, dataSlices)
	for i, s := range original {
		if isMissing(missing, i) {
			present[i] = nil
		} else {
			present[i] = append([]byte{}, s...)
		}
	}

	out, err := Reconstruct(ReconstructInput{
		N: dataSlices, SliceSize: sliceSize,
		Present: present, MissingIdx: missing,
		Recovery: recovery,
	})
	if err != nil {
		t.Fatalf("Reconstruct: %v", err)
	}
	for j, idx := range missing {
		if !bytes.Equal(out[j], original[idx]) {
			t.Errorf("slice %d differs from original", idx)
		}
	}
}
