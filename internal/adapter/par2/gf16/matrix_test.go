package gf16

import (
	"errors"
	"math/rand"
	"testing"
)

func TestIdentityInverseIsItself(t *testing.T) {
	for n := 1; n <= 8; n++ {
		I := Identity(n)
		inv, err := I.Invert()
		if err != nil {
			t.Fatalf("n=%d Invert(I) err: %v", n, err)
		}
		for r := 0; r < n; r++ {
			for c := 0; c < n; c++ {
				want := uint16(0)
				if r == c {
					want = 1
				}
				if got := inv.At(r, c); got != want {
					t.Errorf("n=%d inv(I)[%d,%d] = %d; want %d",
						n, r, c, got, want)
				}
			}
		}
	}
}

func TestInvertTimesOriginalIsIdentity(t *testing.T) {
	// Random non-singular matrices: build A = identity + diag-shifts
	// (always invertible because the diagonal is non-zero), invert,
	// multiply, assert identity.
	rng := rand.New(rand.NewSource(0xC0FFEE))
	for _, n := range []int{1, 2, 3, 5, 8, 13} {
		// Construct A with non-zero diagonal so it's likely invertible.
		A := NewMatrix(n, n)
		for r := 0; r < n; r++ {
			for c := 0; c < n; c++ {
				v := uint16(rng.Uint32() & 0xFFFF)
				if r == c && v == 0 {
					v = 1
				}
				A.Set(r, c, v)
			}
		}
		Ainv, err := A.Invert()
		if err != nil {
			// Singular by chance — retry with a guaranteed-invertible matrix.
			A = Identity(n)
			for r := 0; r < n; r++ {
				A.Set(r, r, uint16(0x100+r))
			}
			Ainv, err = A.Invert()
			if err != nil {
				t.Fatalf("n=%d Invert err: %v", n, err)
			}
		}
		prod := A.MulMatrix(Ainv)
		for r := 0; r < n; r++ {
			for c := 0; c < n; c++ {
				want := uint16(0)
				if r == c {
					want = 1
				}
				if got := prod.At(r, c); got != want {
					t.Errorf("n=%d A*A^-1[%d,%d] = %d; want %d",
						n, r, c, got, want)
				}
			}
		}
	}
}

func TestSingularDetected(t *testing.T) {
	// A 2x2 with a zero row.
	A := FromRows(2, 2, []uint16{1, 2, 0, 0})
	if _, err := A.Invert(); !errors.Is(err, ErrSingular) {
		t.Errorf("expected ErrSingular; got %v", err)
	}

	// A 3x3 with linearly dependent rows: row2 = row0 ^ row1.
	row0 := []uint16{1, 2, 3}
	row1 := []uint16{4, 5, 6}
	row2 := []uint16{row0[0] ^ row1[0], row0[1] ^ row1[1], row0[2] ^ row1[2]}
	B := FromRows(3, 3, append(append(append([]uint16{}, row0...), row1...), row2...))
	if _, err := B.Invert(); !errors.Is(err, ErrSingular) {
		t.Errorf("expected ErrSingular for dependent rows; got %v", err)
	}
}

func TestVandermondeIsInvertible(t *testing.T) {
	// A Vandermonde matrix over distinct field elements is always
	// invertible. This is the property PAR2 relies on for recovery.
	// Build V[i,j] = α^(i*ExpJ) for distinct exponents.
	n := 5
	exps := []uint32{1, 2, 4, 8, 16}
	V := NewMatrix(n, n)
	for i := 0; i < n; i++ {
		for j := 0; j < n; j++ {
			V.Set(i, j, ExpMod(uint32(i)*exps[j]))
		}
	}
	inv, err := V.Invert()
	if err != nil {
		t.Fatalf("Vandermonde Invert err: %v", err)
	}
	prod := V.MulMatrix(inv)
	for r := 0; r < n; r++ {
		for c := 0; c < n; c++ {
			want := uint16(0)
			if r == c {
				want = 1
			}
			if got := prod.At(r, c); got != want {
				t.Errorf("Vandermonde V*V^-1[%d,%d] = %d; want %d",
					r, c, got, want)
			}
		}
	}
}
