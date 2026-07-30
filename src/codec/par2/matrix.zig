//! Dense row-major matrices over GF(2^16), and Gauss-Jordan inversion.
//!
//! PAR2 needs exactly one thing from linear algebra: invert the
//! coefficient matrix that relates the available recovery slices to the
//! missing data slices. That matrix is at most
//! (recovery slices) × (recovery slices) — typically well under 1024 on
//! a side — so a dense flat buffer is the right representation and there
//! is no reason to be clever about it.

const std = @import("std");
const gf16 = @import("gf16.zig");

pub const Error = error{
    /// The available recovery slices do not span enough independent
    /// equations to reconstruct the missing data slices.
    Singular,
    NotSquare,
    DimensionMismatch,
    OutOfMemory,
};

/// An `rows` × `cols` matrix backed by one flat allocation. The zero
/// value is unusable; construct with `init`, `identity` or `fromRows`.
pub const Matrix = struct {
    rows: usize,
    cols: usize,
    data: []u16,

    /// An all-zero r×c matrix.
    pub fn init(alloc: std.mem.Allocator, r: usize, c: usize) error{OutOfMemory}!Matrix {
        const data = try alloc.alloc(u16, r * c);
        @memset(data, 0);
        return .{ .rows = r, .cols = c, .data = data };
    }

    pub fn deinit(m: *Matrix, alloc: std.mem.Allocator) void {
        alloc.free(m.data);
        m.* = undefined;
    }

    /// Copies `values` (row-major, length must be rows*cols).
    pub fn fromRows(
        alloc: std.mem.Allocator,
        rows: usize,
        cols: usize,
        values: []const u16,
    ) error{ OutOfMemory, DimensionMismatch }!Matrix {
        if (values.len != rows * cols) return error.DimensionMismatch;
        const data = try alloc.alloc(u16, values.len);
        @memcpy(data, values);
        return .{ .rows = rows, .cols = cols, .data = data };
    }

    /// The n×n identity.
    pub fn identity(alloc: std.mem.Allocator, n: usize) error{OutOfMemory}!Matrix {
        var m = try Matrix.init(alloc, n, n);
        for (0..n) |i| m.set(i, i, 1);
        return m;
    }

    pub inline fn at(m: *const Matrix, r: usize, c: usize) u16 {
        return m.data[r * m.cols + c];
    }

    pub inline fn set(m: *Matrix, r: usize, c: usize, v: u16) void {
        m.data[r * m.cols + c] = v;
    }

    /// Row `r` as a mutable slice.
    pub inline fn row(m: *Matrix, r: usize) []u16 {
        return m.data[r * m.cols ..][0..m.cols];
    }

    pub fn clone(m: *const Matrix, alloc: std.mem.Allocator) error{OutOfMemory}!Matrix {
        const data = try alloc.dupe(u16, m.data);
        return .{ .rows = m.rows, .cols = m.cols, .data = data };
    }

    /// Inverse via Gauss-Jordan elimination on the augmented [A | I].
    ///
    /// One n × 2n scratch buffer, reduced in place; when the left half
    /// is the identity the right half is the inverse. Row operations use
    /// the bulk `mulAddSlice`, so wide matrices get the vectorised
    /// multiply for free.
    pub fn invert(m: *const Matrix, alloc: std.mem.Allocator) Error!Matrix {
        if (m.rows != m.cols) return error.NotSquare;
        const n = m.rows;
        const w = 2 * n;

        const wide = try alloc.alloc(u16, n * w);
        defer alloc.free(wide);
        @memset(wide, 0);
        for (0..n) |r| {
            @memcpy(wide[r * w ..][0..n], m.data[r * n ..][0..n]);
            wide[r * w + n + r] = 1;
        }

        for (0..n) |col| {
            // Find a non-zero pivot at or below the diagonal.
            var pivot: ?usize = null;
            for (col..n) |r| {
                if (wide[r * w + col] != 0) {
                    pivot = r;
                    break;
                }
            }
            const p = pivot orelse return error.Singular;
            if (p != col) {
                for (0..w) |c| {
                    std.mem.swap(u16, &wide[col * w + c], &wide[p * w + c]);
                }
            }

            // Normalise the pivot row to lead with 1.
            const piv = wide[col * w + col];
            if (piv != 1) {
                // piv is non-zero by construction, so inv cannot fail.
                const scale = gf16.inv(piv) catch unreachable;
                const pivot_row = wide[col * w ..][0..w];
                for (pivot_row) |*v| v.* = gf16.mul(v.*, scale);
            }

            // Eliminate this column from every *other* row. Doing both
            // directions here (Gauss-Jordan rather than plain Gauss)
            // means no separate back-substitution pass.
            for (0..n) |r| {
                if (r == col) continue;
                const factor = wide[r * w + col];
                if (factor == 0) continue;
                // Aliasing: `col != r`, so source and destination rows
                // are disjoint.
                const src = wide[col * w ..][0..w];
                const dst = wide[r * w ..][0..w];
                gf16.mulAddSlice(dst, src, factor);
            }
        }

        var out = try Matrix.init(alloc, n, n);
        for (0..n) |r| {
            @memcpy(out.data[r * n ..][0..n], wide[r * w + n ..][0..n]);
        }
        return out;
    }

    /// m * other, an m.rows × other.cols matrix.
    pub fn mulMatrix(
        m: *const Matrix,
        alloc: std.mem.Allocator,
        other: *const Matrix,
    ) error{ OutOfMemory, DimensionMismatch }!Matrix {
        if (m.cols != other.rows) return error.DimensionMismatch;
        var out = try Matrix.init(alloc, m.rows, other.cols);
        for (0..m.rows) |r| {
            for (0..m.cols) |k| {
                const coef = m.at(r, k);
                if (coef == 0) continue;
                // Accumulate whole rows of `other` scaled by one element
                // of `m` — the same work as the textbook triple loop but
                // with the innermost dimension contiguous in both
                // operands, so the bulk multiply applies.
                gf16.mulAddSlice(
                    out.data[r * other.cols ..][0..other.cols],
                    other.data[k * other.cols ..][0..other.cols],
                    coef,
                );
            }
        }
        return out;
    }
};

// ---------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------

fn expectIdentity(m: *const Matrix) !void {
    for (0..m.rows) |r| {
        for (0..m.cols) |c| {
            const want: u16 = if (r == c) 1 else 0;
            try std.testing.expectEqual(want, m.at(r, c));
        }
    }
}

test "identity inverts to itself" {
    const t = std.testing;
    for (1..9) |n| {
        var id = try Matrix.identity(t.allocator, n);
        defer id.deinit(t.allocator);
        var got = try id.invert(t.allocator);
        defer got.deinit(t.allocator);
        try expectIdentity(&got);
    }
}

test "A * A^-1 is the identity" {
    const t = std.testing;
    var prng = std.Random.DefaultPrng.init(0xC0FFEE);
    const rand = prng.random();

    for ([_]usize{ 1, 2, 3, 5, 8, 13 }) |n| {
        var a = try Matrix.init(t.allocator, n, n);
        defer a.deinit(t.allocator);
        for (0..n) |r| {
            for (0..n) |c| {
                var v = rand.int(u16);
                // Keep the diagonal non-zero so the matrix is very
                // likely invertible.
                if (r == c and v == 0) v = 1;
                a.set(r, c, v);
            }
        }

        var a_inv = a.invert(t.allocator) catch |err| blk: {
            try t.expectEqual(Error.Singular, err);
            // Singular by chance — fall back to a guaranteed-invertible
            // diagonal matrix so the multiply half still gets exercised.
            a.deinit(t.allocator);
            a = try Matrix.identity(t.allocator, n);
            for (0..n) |r| a.set(r, r, @intCast(0x100 + r));
            break :blk try a.invert(t.allocator);
        };
        defer a_inv.deinit(t.allocator);

        var prod = try a.mulMatrix(t.allocator, &a_inv);
        defer prod.deinit(t.allocator);
        try expectIdentity(&prod);
    }
}

test "singular matrices are detected" {
    const t = std.testing;

    // A 2x2 with a zero row.
    var a = try Matrix.fromRows(t.allocator, 2, 2, &.{ 1, 2, 0, 0 });
    defer a.deinit(t.allocator);
    try t.expectError(error.Singular, a.invert(t.allocator));

    // A 3x3 whose third row is the XOR (= sum) of the first two.
    var b = try Matrix.fromRows(t.allocator, 3, 3, &.{
        1,     2,     3,
        4,     5,     6,
        1 ^ 4, 2 ^ 5, 3 ^ 6,
    });
    defer b.deinit(t.allocator);
    try t.expectError(error.Singular, b.invert(t.allocator));
}

test "non-square inversion is rejected" {
    const t = std.testing;
    var a = try Matrix.init(t.allocator, 2, 3);
    defer a.deinit(t.allocator);
    try t.expectError(error.NotSquare, a.invert(t.allocator));
}

test "mismatched multiply is rejected" {
    const t = std.testing;
    var a = try Matrix.init(t.allocator, 2, 3);
    defer a.deinit(t.allocator);
    var b = try Matrix.init(t.allocator, 2, 2);
    defer b.deinit(t.allocator);
    try t.expectError(error.DimensionMismatch, a.mulMatrix(t.allocator, &b));
    try t.expectError(error.DimensionMismatch, Matrix.fromRows(t.allocator, 2, 2, &.{ 1, 2, 3 }));
}

test "Vandermonde matrices are invertible" {
    // A Vandermonde matrix over distinct field elements is always
    // invertible — the property PAR2 recovery rests on.
    const t = std.testing;
    const n = 5;
    const exps = [n]u32{ 1, 2, 4, 8, 16 };

    var v = try Matrix.init(t.allocator, n, n);
    defer v.deinit(t.allocator);
    for (0..n) |i| {
        for (0..n) |j| {
            v.set(i, j, gf16.expMod(@as(u32, @intCast(i)) * exps[j]));
        }
    }

    var v_inv = try v.invert(t.allocator);
    defer v_inv.deinit(t.allocator);

    var prod = try v.mulMatrix(t.allocator, &v_inv);
    defer prod.deinit(t.allocator);
    try expectIdentity(&prod);

    // And the other way round — inversion is two-sided.
    var prod2 = try v_inv.mulMatrix(t.allocator, &v);
    defer prod2.deinit(t.allocator);
    try expectIdentity(&prod2);
}

test "clone is independent" {
    const t = std.testing;
    var a = try Matrix.fromRows(t.allocator, 2, 2, &.{ 1, 2, 3, 4 });
    defer a.deinit(t.allocator);
    var b = try a.clone(t.allocator);
    defer b.deinit(t.allocator);
    a.set(0, 0, 0xFFFF);
    try t.expectEqual(@as(u16, 1), b.at(0, 0));
}

test "larger random inversion round-trips" {
    // 64x64 — big enough that the row operations run through the
    // vectorised bulk multiply and any indexing slip would show up.
    const t = std.testing;
    const n = 64;
    var prng = std.Random.DefaultPrng.init(0xBEEF);
    const rand = prng.random();

    var a = try Matrix.init(t.allocator, n, n);
    defer a.deinit(t.allocator);
    for (a.data) |*v| v.* = rand.int(u16) | 1;

    var a_inv = try a.invert(t.allocator);
    defer a_inv.deinit(t.allocator);
    var prod = try a.mulMatrix(t.allocator, &a_inv);
    defer prod.deinit(t.allocator);
    try expectIdentity(&prod);
}
