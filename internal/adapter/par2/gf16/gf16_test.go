package gf16

import "testing"

func TestTablesAreInverses(t *testing.T) {
	// For every non-zero v, exp[log[v]] should equal v.
	for v := uint32(1); v < FieldSize; v++ {
		got := expTable[logTable[uint16(v)]]
		if uint32(got) != v {
			t.Fatalf("exp[log[%d]] = %d; want %d", v, got, v)
		}
	}
}

func TestExpCycle(t *testing.T) {
	// α^(MultGroupOrder) wraps to 1.
	if got := ExpMod(MultGroupOrder); got != 1 {
		t.Errorf("ExpMod(%d) = %d; want 1", MultGroupOrder, got)
	}
	if got := ExpMod(0); got != 1 {
		t.Errorf("ExpMod(0) = %d; want 1", got)
	}
	if got := ExpMod(1); got != Generator {
		t.Errorf("ExpMod(1) = %d; want %d", got, Generator)
	}
}

func TestMulIdentity(t *testing.T) {
	for _, v := range []uint16{0, 1, 2, 7, 0x1234, 0xFFFE, 0xFFFF} {
		if got := Mul(v, 1); got != v {
			t.Errorf("Mul(%d, 1) = %d", v, got)
		}
		if got := Mul(1, v); got != v {
			t.Errorf("Mul(1, %d) = %d", v, got)
		}
		if got := Mul(v, 0); got != 0 {
			t.Errorf("Mul(%d, 0) = %d", v, got)
		}
	}
}

func TestMulCommutative(t *testing.T) {
	pairs := [][2]uint16{
		{2, 3}, {0x100, 0x101}, {0xABCD, 0x1234}, {0xFFFF, 0xFFFE},
	}
	for _, p := range pairs {
		ab := Mul(p[0], p[1])
		ba := Mul(p[1], p[0])
		if ab != ba {
			t.Errorf("Mul(%d,%d)=%d != Mul(%d,%d)=%d",
				p[0], p[1], ab, p[1], p[0], ba)
		}
	}
}

func TestMulAssociative(t *testing.T) {
	// (a*b)*c == a*(b*c) on a sample.
	for _, a := range []uint16{1, 2, 7, 0x1234} {
		for _, b := range []uint16{3, 0x100, 0xBEEF} {
			for _, c := range []uint16{5, 0x10, 0xFFFF} {
				left := Mul(Mul(a, b), c)
				right := Mul(a, Mul(b, c))
				if left != right {
					t.Errorf("Mul non-assoc on (%d,%d,%d): %d vs %d",
						a, b, c, left, right)
				}
			}
		}
	}
}

func TestDistributivity(t *testing.T) {
	// a * (b + c) = a*b + a*c. Add is XOR.
	for _, a := range []uint16{1, 2, 0x100, 0xBEEF} {
		for _, b := range []uint16{3, 0x10, 0xFFFE} {
			for _, c := range []uint16{5, 0x1000, 0x1234} {
				left := Mul(a, b^c)
				right := Mul(a, b) ^ Mul(a, c)
				if left != right {
					t.Errorf("dist fail on (%d,%d,%d): %d vs %d",
						a, b, c, left, right)
				}
			}
		}
	}
}

func TestInvMulIsIdentity(t *testing.T) {
	for _, v := range []uint16{1, 2, 7, 0x100, 0x1234, 0xFFFE, 0xFFFF} {
		if got := Mul(v, Inv(v)); got != 1 {
			t.Errorf("v * inv(v) = %d for v=%d; want 1", got, v)
		}
	}
}

func TestDiv(t *testing.T) {
	for _, a := range []uint16{1, 2, 7, 0x100, 0xBEEF} {
		for _, b := range []uint16{1, 2, 7, 0x100, 0xBEEF} {
			q := Div(a, b)
			// q * b should yield a back.
			back := Mul(q, b)
			if back != a {
				t.Errorf("Div(%d,%d)=%d; %d*%d=%d != %d",
					a, b, q, q, b, back, a)
			}
		}
	}
}

func TestPow(t *testing.T) {
	// α^0 = 1, α^1 = 2, α^2 = 4 (no reduction at low exponents).
	tests := []struct {
		base uint16
		exp  uint32
		want uint16
	}{
		{2, 0, 1},
		{2, 1, 2},
		{2, 2, 4},
		{2, 3, 8},
		{2, 15, 0x8000},
		{2, MultGroupOrder, 1},     // full cycle
		{2, MultGroupOrder + 1, 2}, // wraps
		{0, 5, 0},
		{0, 0, 1}, // 0^0 = 1 by our convention (matches math/big)
		{1, 1234567, 1},
	}
	for _, tc := range tests {
		if got := Pow(tc.base, tc.exp); got != tc.want {
			t.Errorf("Pow(%d,%d) = %d; want %d",
				tc.base, tc.exp, got, tc.want)
		}
	}
}

func TestDivByZeroPanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic")
		}
	}()
	_ = Div(1, 0)
}

func TestInvOfZeroPanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic")
		}
	}()
	_ = Inv(0)
}
