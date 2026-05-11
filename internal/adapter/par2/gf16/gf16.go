// Package gf16 implements GF(2^16) arithmetic for PAR2 Reed-Solomon.
//
// Field parameters (fixed by the PAR2 specification):
//
//   - Order: 2^16 = 65536 elements
//   - Irreducible polynomial: 0x1100B = x^16 + x^12 + x^3 + x + 1
//   - Generator (primitive element): α = 2
//
// Operations use precomputed log/exp tables (each 64 KiB), so multiply
// is O(1) and reduces to a couple of table lookups + a 17-bit add.
// For the slice sizes PAR2 cares about (tens to hundreds of KiB) this
// is dominated by memory bandwidth rather than the table lookups.
//
// The tables are built once at package init. If startup-time matters
// later, they can be hoisted to //go:embed bytes baked at build time.
package gf16

const (
	// FieldSize is 2^16. The field has FieldSize elements; the
	// multiplicative group has FieldSize-1 elements (zero excluded).
	FieldSize = 1 << 16

	// MultGroupOrder is FieldSize - 1.
	MultGroupOrder = FieldSize - 1

	// IrreduciblePoly is PAR2's field-defining polynomial. The bit
	// pattern reads x^16 + x^12 + x^3 + x + 1. We only ever subtract
	// (xor) the low 17 bits; the x^16 term is implicit because the
	// reduction step takes effect when a multiply overflows bit 15.
	IrreduciblePoly uint32 = 0x1100B

	// Generator is the primitive element α used to build the cyclic
	// log table. PAR2 fixes this at α=2.
	Generator uint16 = 2
)

// expTable[i] = α^i for i in [0, MultGroupOrder). Indexing beyond
// MultGroupOrder is undefined here; callers reduce the exponent first
// using ExpMod.
//
// logTable[v] = i such that α^i = v, for v in [1, FieldSize). logTable[0]
// is left as 0 (a sentinel; callers must guard against the zero input
// before reading from logTable).
var (
	expTable [MultGroupOrder]uint16
	logTable [FieldSize]uint16
)

func init() {
	// Walk α^0, α^1, … by repeated multiply, reducing on bit-16 carry.
	// We open-code the reduction here because Mul itself reads
	// expTable — chicken-and-egg.
	var v uint32 = 1
	for i := 0; i < MultGroupOrder; i++ {
		expTable[i] = uint16(v)
		logTable[v] = uint16(i)
		v <<= 1
		if v&0x10000 != 0 {
			v ^= IrreduciblePoly
		}
	}
	// logTable[0] = 0 is a sentinel — never indexed in the hot path
	// because every operation that would do so checks for zero first.
}

// Add is XOR (characteristic-2 field). Provided for symmetry with the
// rest of the API; call sites can write either Add(a, b) or a^b.
func Add(a, b uint16) uint16 { return a ^ b }

// Sub equals Add in a characteristic-2 field. Wrapper for readability.
func Sub(a, b uint16) uint16 { return a ^ b }

// Mul returns a * b in GF(2^16).
func Mul(a, b uint16) uint16 {
	if a == 0 || b == 0 {
		return 0
	}
	return expTable[(uint32(logTable[a])+uint32(logTable[b]))%MultGroupOrder]
}

// Div returns a / b. Panics on division by zero (caller's bug; field
// division is total on the multiplicative group).
func Div(a, b uint16) uint16 {
	if b == 0 {
		panic("gf16: divide by zero")
	}
	if a == 0 {
		return 0
	}
	// log(a) - log(b) modulo MultGroupOrder. Done as +MultGroupOrder
	// then % to keep the intermediate positive.
	exp := (uint32(logTable[a]) + MultGroupOrder - uint32(logTable[b])) % MultGroupOrder
	return expTable[exp]
}

// Inv returns the multiplicative inverse of a. Panics on a==0.
//
// The mod-reduction at the end handles a==1 specifically: log[1]=0, so
// the bare subtract would index expTable at MultGroupOrder which is
// out of range. The cyclic-group identity exp[k] = exp[k mod (q-1)]
// makes this the natural fix.
func Inv(a uint16) uint16 {
	if a == 0 {
		panic("gf16: inverse of zero")
	}
	exp := (MultGroupOrder - uint32(logTable[a])) % MultGroupOrder
	return expTable[exp]
}

// Pow returns base^exp. exp is taken mod MultGroupOrder. Returns 0 if
// base==0 (and exp>0); for exp==0 returns 1 by convention.
func Pow(base uint16, exp uint32) uint16 {
	if exp == 0 {
		return 1
	}
	if base == 0 {
		return 0
	}
	return expTable[(uint32(logTable[base])*exp)%MultGroupOrder]
}

// ExpMod returns α^(e mod MultGroupOrder) — the building block PAR2's
// recovery-slice exponents use. Returns 1 for e==0.
func ExpMod(e uint32) uint16 {
	return expTable[e%MultGroupOrder]
}

// Log returns log_α(a). Caller must ensure a != 0 — the function
// returns 0 for the zero input but that's a sentinel, not a meaningful
// answer. Use only when a is known non-zero.
func Log(a uint16) uint16 { return logTable[a] }
