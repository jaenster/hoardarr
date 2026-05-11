package par2

// Reed-Solomon encode/decode helpers for PAR2.
//
// Layout PAR2 imposes on file data:
//
//   - The file's bytes are partitioned into fixed-size slices of
//     `slice_size` bytes. The last slice is zero-padded if the file
//     length doesn't divide evenly.
//   - Each slice is treated as a vector of GF(2^16) elements: two
//     consecutive bytes (little-endian) per element. slice_size must
//     therefore be even.
//
// Recovery generation: for each recovery slice with exponent `e`,
//
//     R_e[k] = Σ_{i=0..N-1} α^(i * e) * D_i[k]
//
// where D_i is the i-th data slice, k is the element index within the
// slice, and α=2 is the field generator. The sum runs over all data
// slices in the recovery set, in their canonical order.
//
// Repair (Reconstruct) inverts this: given the equations from
// available recovery slices for the missing data-slice indices, solve
// the linear system over GF(2^16) byte-wise.

import (
	"errors"
	"fmt"

	"github.com/jaenster/hoardarr/internal/adapter/par2/gf16"
)

// SliceToElements packs a slice of bytes into GF(2^16) elements
// little-endian. Length must be even. Caller owns the returned buffer.
func SliceToElements(buf []byte) []uint16 {
	if len(buf)%2 != 0 {
		panic("par2: SliceToElements requires even-length buffer")
	}
	out := make([]uint16, len(buf)/2)
	for i := range out {
		out[i] = uint16(buf[2*i]) | uint16(buf[2*i+1])<<8
	}
	return out
}

// ElementsToSlice unpacks back into a byte buffer little-endian.
func ElementsToSlice(elems []uint16) []byte {
	out := make([]byte, len(elems)*2)
	for i, v := range elems {
		out[2*i] = byte(v)
		out[2*i+1] = byte(v >> 8)
	}
	return out
}

// SplitIntoSlices partitions data into slices of size sliceSize bytes,
// zero-padding the last slice if needed. sliceSize must be even.
// Returns a slice of slices, each exactly sliceSize bytes long.
func SplitIntoSlices(data []byte, sliceSize int) [][]byte {
	if sliceSize <= 0 || sliceSize%2 != 0 {
		panic("par2: SplitIntoSlices requires even positive sliceSize")
	}
	n := (len(data) + sliceSize - 1) / sliceSize
	out := make([][]byte, n)
	for i := range out {
		buf := make([]byte, sliceSize)
		start := i * sliceSize
		end := start + sliceSize
		if end > len(data) {
			end = len(data)
		}
		copy(buf, data[start:end])
		out[i] = buf
	}
	return out
}

// EncodeRecoverySlice computes the recovery slice for one exponent.
//
//	R_e[k] = Σ_{i=0..N-1} α^(i*e) * D_i[k]
//
// dataSlices is the canonical list of data slices in order. Each slice
// must have the same length and be even-length. Returns a byte buffer
// of the same size as one input slice.
func EncodeRecoverySlice(dataSlices [][]byte, exponent uint16) []byte {
	if len(dataSlices) == 0 {
		return nil
	}
	sliceLen := len(dataSlices[0])
	for _, s := range dataSlices {
		if len(s) != sliceLen {
			panic("par2: EncodeRecoverySlice: data slices must be equal length")
		}
	}
	if sliceLen%2 != 0 {
		panic("par2: EncodeRecoverySlice: slice length must be even")
	}
	elemCount := sliceLen / 2
	acc := make([]uint16, elemCount)
	for i, s := range dataSlices {
		// Coefficient for this data slice with this recovery exponent.
		coef := gf16.ExpMod(uint32(i) * uint32(exponent))
		if coef == 0 {
			continue
		}
		for k := 0; k < elemCount; k++ {
			d := uint16(s[2*k]) | uint16(s[2*k+1])<<8
			acc[k] ^= gf16.Mul(coef, d)
		}
	}
	return ElementsToSlice(acc)
}

// ErrUnrecoverable is returned by Reconstruct when too few recovery
// slices are available to cover the missing data slices, or the
// resulting matrix is singular.
var ErrUnrecoverable = errors.New("par2: not enough recovery slices to repair")

// ReconstructInput aggregates everything Reconstruct needs.
//
//	N           — total number of data slices in the recovery set
//	SliceSize   — bytes per slice (even)
//	Present     — Present[i] is the i-th data slice if i is NOT missing,
//	              ignored otherwise. Length N. Missing indices may hold
//	              nil or any value.
//	MissingIdx  — indices into Present that are damaged/lost; in
//	              ascending order.
//	Recovery    — map from recovery-slice exponent to its bytes.
type ReconstructInput struct {
	N          int
	SliceSize  int
	Present    [][]byte
	MissingIdx []int
	Recovery   map[uint16][]byte
}

// Reconstruct returns the data slices for the missing indices, in the
// same order as MissingIdx. The function does not mutate the input.
//
// Algorithm:
//
//  1. Need len(MissingIdx) recovery slices. Pick that many exponents
//     from Recovery (deterministically smallest first).
//  2. For each chosen exponent e, compute the residual:
//     residual_e = recv_slice_e - Σ_{i present} α^(i*e) * D_i
//     so residual_e = Σ_{i missing} α^(i*e) * D_i
//  3. The coefficient matrix M[k,j] = α^(missingIdx[j] * exp_k) is
//     square and Vandermonde-like; invert it once and apply per element.
func Reconstruct(in ReconstructInput) ([][]byte, error) {
	missing := len(in.MissingIdx)
	if missing == 0 {
		return nil, nil
	}
	if in.SliceSize <= 0 || in.SliceSize%2 != 0 {
		return nil, fmt.Errorf("par2: bad SliceSize %d", in.SliceSize)
	}
	if len(in.Recovery) < missing {
		return nil, ErrUnrecoverable
	}

	// Deterministic choice of recovery exponents: smallest first.
	exps := sortedRecoveryExponents(in.Recovery)
	if len(exps) > missing {
		exps = exps[:missing]
	}

	// Build the coefficient matrix M[k,j] = α^(missingIdx[j] * exps[k]).
	M := gf16.NewMatrix(missing, missing)
	for k := 0; k < missing; k++ {
		for j := 0; j < missing; j++ {
			M.Set(k, j, gf16.ExpMod(uint32(in.MissingIdx[j])*uint32(exps[k])))
		}
	}
	Minv, err := M.Invert()
	if err != nil {
		return nil, ErrUnrecoverable
	}

	// For each recovery slice e:
	//   residual_e[k] = recv_e[k] - Σ_{i not missing} α^(i*e) * D_i[k]
	// Compute residuals once per chosen exponent, length elemCount each.
	elemCount := in.SliceSize / 2
	residuals := make([][]uint16, missing)
	for k, e := range exps {
		recvBytes := in.Recovery[e]
		if len(recvBytes) != in.SliceSize {
			return nil, fmt.Errorf("par2: recovery slice for exp=%d has length %d; want %d",
				e, len(recvBytes), in.SliceSize)
		}
		res := SliceToElements(recvBytes)
		for i := 0; i < in.N; i++ {
			if isMissing(in.MissingIdx, i) {
				continue
			}
			if len(in.Present[i]) != in.SliceSize {
				return nil, fmt.Errorf("par2: present slice %d has length %d; want %d",
					i, len(in.Present[i]), in.SliceSize)
			}
			coef := gf16.ExpMod(uint32(i) * uint32(e))
			if coef == 0 {
				continue
			}
			s := in.Present[i]
			for p := 0; p < elemCount; p++ {
				d := uint16(s[2*p]) | uint16(s[2*p+1])<<8
				res[p] ^= gf16.Mul(coef, d)
			}
		}
		residuals[k] = res
	}

	// Apply Minv element-by-element: missing[j][p] = Σ_k Minv[j,k] * residual[k][p]
	outBytes := make([][]byte, missing)
	outElems := make([][]uint16, missing)
	for j := range outElems {
		outElems[j] = make([]uint16, elemCount)
	}
	for j := 0; j < missing; j++ {
		for k := 0; k < missing; k++ {
			coef := Minv.At(j, k)
			if coef == 0 {
				continue
			}
			r := residuals[k]
			out := outElems[j]
			for p := 0; p < elemCount; p++ {
				out[p] ^= gf16.Mul(coef, r[p])
			}
		}
		outBytes[j] = ElementsToSlice(outElems[j])
	}
	return outBytes, nil
}

func isMissing(missingIdx []int, i int) bool {
	for _, m := range missingIdx {
		if m == i {
			return true
		}
		if m > i {
			return false
		}
	}
	return false
}

func sortedRecoveryExponents(m map[uint16][]byte) []uint16 {
	out := make([]uint16, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	// Insertion sort — len is at most a few hundred in practice and
	// avoids pulling in sort just for this.
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && out[j-1] > out[j]; j-- {
			out[j-1], out[j] = out[j], out[j-1]
		}
	}
	return out
}
