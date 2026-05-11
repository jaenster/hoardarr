package gf16

import "errors"

// ErrSingular is returned by Invert when the matrix is singular — for
// PAR2 this means the available recovery slices don't span enough
// independent equations to reconstruct the missing data slices.
var ErrSingular = errors.New("gf16: matrix is singular")

// Matrix is a row-major dense matrix of GF(2^16) elements. Rows have
// equal length; the zero value is unusable (use NewMatrix).
//
// Memory layout: one flat backing slice, indexed via At/Set. The dense
// representation is fine for PAR2 — matrices are at most
// MaxRecoverySlices × MaxRecoverySlices in size, typically <1024.
type Matrix struct {
	rows, cols int
	data       []uint16
}

// NewMatrix returns an r×c zero matrix.
func NewMatrix(r, c int) *Matrix {
	return &Matrix{rows: r, cols: c, data: make([]uint16, r*c)}
}

// FromRows takes an r×c value in row-major order and wraps it.
// The caller transfers ownership of data.
func FromRows(rows, cols int, data []uint16) *Matrix {
	if len(data) != rows*cols {
		panic("gf16: FromRows length mismatch")
	}
	return &Matrix{rows: rows, cols: cols, data: data}
}

// Dims returns rows, cols.
func (m *Matrix) Dims() (int, int) { return m.rows, m.cols }

// At returns the element at (r, c).
func (m *Matrix) At(r, c int) uint16 { return m.data[r*m.cols+c] }

// Set writes v to (r, c).
func (m *Matrix) Set(r, c int, v uint16) { m.data[r*m.cols+c] = v }

// Clone returns an independent copy.
func (m *Matrix) Clone() *Matrix {
	out := NewMatrix(m.rows, m.cols)
	copy(out.data, m.data)
	return out
}

// Identity returns the n×n identity matrix.
func Identity(n int) *Matrix {
	m := NewMatrix(n, n)
	for i := 0; i < n; i++ {
		m.Set(i, i, 1)
	}
	return m
}

// Invert returns the inverse of a square matrix via Gauss-Jordan
// elimination over GF(2^16). Returns ErrSingular if the matrix is
// not invertible.
//
// The algorithm augments [A | I] then reduces to [I | A^-1]. We allocate
// a single 2n-wide scratch and operate in place.
func (m *Matrix) Invert() (*Matrix, error) {
	if m.rows != m.cols {
		return nil, errors.New("gf16: Invert requires a square matrix")
	}
	n := m.rows

	// Work on a wide scratch of size n × 2n: left half = m, right half = I.
	wide := make([]uint16, n*2*n)
	for r := 0; r < n; r++ {
		for c := 0; c < n; c++ {
			wide[r*2*n+c] = m.At(r, c)
		}
		wide[r*2*n+n+r] = 1
	}

	// Forward + backward elimination in one pass: for each column,
	// pick a pivot in or below the current row, swap if needed,
	// normalise the pivot row to lead with 1, then zero the column
	// in every OTHER row (Gauss-Jordan rather than plain Gauss).
	for col := 0; col < n; col++ {
		// Find a non-zero pivot at or below row `col`.
		pivot := -1
		for r := col; r < n; r++ {
			if wide[r*2*n+col] != 0 {
				pivot = r
				break
			}
		}
		if pivot < 0 {
			return nil, ErrSingular
		}
		if pivot != col {
			swapRows(wide, n, col, pivot)
		}

		// Normalise the pivot row so the leading coefficient is 1.
		piv := wide[col*2*n+col]
		if piv != 1 {
			inv := Inv(piv)
			for c := 0; c < 2*n; c++ {
				wide[col*2*n+c] = Mul(wide[col*2*n+c], inv)
			}
		}

		// Eliminate the col-th column from every other row.
		for r := 0; r < n; r++ {
			if r == col {
				continue
			}
			factor := wide[r*2*n+col]
			if factor == 0 {
				continue
			}
			for c := 0; c < 2*n; c++ {
				wide[r*2*n+c] ^= Mul(factor, wide[col*2*n+c])
			}
		}
	}

	// Right half is the inverse.
	out := NewMatrix(n, n)
	for r := 0; r < n; r++ {
		for c := 0; c < n; c++ {
			out.Set(r, c, wide[r*2*n+n+c])
		}
	}
	return out, nil
}

func swapRows(buf []uint16, n, a, b int) {
	row := 2 * n
	for c := 0; c < row; c++ {
		buf[a*row+c], buf[b*row+c] = buf[b*row+c], buf[a*row+c]
	}
}

// Mul returns m * other. Dimensions must match (m.cols == other.rows);
// the result is m.rows × other.cols.
func (m *Matrix) MulMatrix(other *Matrix) *Matrix {
	if m.cols != other.rows {
		panic("gf16: matrix multiply dimension mismatch")
	}
	out := NewMatrix(m.rows, other.cols)
	for r := 0; r < m.rows; r++ {
		for c := 0; c < other.cols; c++ {
			var sum uint16
			for k := 0; k < m.cols; k++ {
				sum ^= Mul(m.At(r, k), other.At(k, c))
			}
			out.Set(r, c, sum)
		}
	}
	return out
}
