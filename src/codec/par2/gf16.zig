//! GF(2^16) arithmetic for PAR2 Reed-Solomon.
//!
//! Field parameters, all fixed by the PAR2 specification:
//!
//!   * order 2^16 = 65536 elements
//!   * irreducible polynomial 0x1100B = x^16 + x^12 + x^3 + x + 1
//!   * generator (primitive element) α = 2
//!
//! Single-element arithmetic uses the classic log/exp table pair. The
//! two tables together are 256 KiB, so they land in .rodata and cost
//! nothing at startup — but they also do not fit in L1d, which matters
//! for the bulk path below.
//!
//! The repair hot loop is never "multiply two elements": it is always
//! `acc[k] ^= c * src[k]` over a whole slice with `c` constant for the
//! duration. Three implementations of that are provided:
//!
//!   * `mulAddSliceLogExp` — the direct translation of the scalar
//!     element-wise multiply. Correctness reference.
//!   * `mulAddSliceSplit` — two 256-entry tables (`c * x` and
//!     `c * (x << 8)`) built once per constant. 1 KiB of tables,
//!     permanently L1-resident, two independent loads per element.
//!   * `mulAddSliceSimd` — table-free. Multiplication by a constant is
//!     GF(2)-linear in the operand, so `c * v` is the XOR of `c * 2^i`
//!     over the set bits `i` of `v`. Sixteen masked XORs per vector,
//!     no memory traffic at all.
//!
//! `mulAddSlice` aliases the measured winner (see the comment there).
//! All three are asserted bit-identical over random input in the tests,
//! so switching is safe.

const std = @import("std");

/// The field has `field_size` elements; the multiplicative group has
/// `field_size - 1` (zero excluded).
pub const field_size = 1 << 16;

/// Order of the multiplicative group — exponents are taken mod this.
pub const mult_group_order = field_size - 1;

/// PAR2's field-defining polynomial. The bit pattern reads
/// x^16 + x^12 + x^3 + x + 1. Only the low 17 bits are ever XORed in;
/// the x^16 term is implicit in the carry test.
pub const irreducible_poly: u32 = 0x1100B;

/// The primitive element α used to build the cyclic log table. PAR2
/// fixes this at α = 2.
pub const generator: u16 = 2;

pub const Error = error{DivideByZero};

const Tables = struct {
    /// exp[i] = α^i for i in [0, mult_group_order).
    exp: [mult_group_order]u16,
    /// log[v] = i such that α^i = v, for v in [1, field_size).
    /// log[0] stays 0 — a sentinel, never a meaningful answer; every
    /// operation that would index it checks for zero first.
    log: [field_size]u16,
};

const tables: Tables = blk: {
    // One iteration per group element, each doing a handful of
    // branches; the default quota is nowhere near enough.
    @setEvalBranchQuota(2_000_000);
    var t: Tables = undefined;
    t.log[0] = 0;
    // Walk α^0, α^1, … by repeated doubling, reducing on the bit-16
    // carry. The reduction is open-coded because `mul` itself reads
    // these tables.
    var v: u32 = 1;
    for (0..mult_group_order) |i| {
        t.exp[i] = @intCast(v);
        t.log[v] = @intCast(i);
        v <<= 1;
        if (v & 0x10000 != 0) v ^= irreducible_poly;
    }
    break :blk t;
};

/// Addition is XOR — the field has characteristic 2. Provided for
/// symmetry with the rest of the API.
pub inline fn add(a: u16, b: u16) u16 {
    return a ^ b;
}

/// Subtraction equals addition in characteristic 2.
pub inline fn sub(a: u16, b: u16) u16 {
    return a ^ b;
}

/// a * b in GF(2^16).
pub fn mul(a: u16, b: u16) u16 {
    if (a == 0 or b == 0) return 0;
    const s = @as(u32, tables.log[a]) + @as(u32, tables.log[b]);
    return tables.exp[s % mult_group_order];
}

/// a / b. Field division is total on the multiplicative group, so a
/// zero divisor is a caller bug rather than a data condition.
pub fn div(a: u16, b: u16) Error!u16 {
    if (b == 0) return error.DivideByZero;
    if (a == 0) return 0;
    // log(a) - log(b) mod the group order, biased up to stay unsigned.
    const e = (@as(u32, tables.log[a]) + mult_group_order - @as(u32, tables.log[b])) % mult_group_order;
    return tables.exp[e];
}

/// Multiplicative inverse of a.
///
/// The final mod handles a == 1: log[1] = 0, so the bare subtract would
/// index `exp` at `mult_group_order`, one past the end. The cyclic-group
/// identity exp[k] = exp[k mod (q-1)] makes the reduction the natural fix.
pub fn inv(a: u16) Error!u16 {
    if (a == 0) return error.DivideByZero;
    const e = (mult_group_order - @as(u32, tables.log[a])) % mult_group_order;
    return tables.exp[e];
}

/// base^exp. The exponent is taken mod the group order. Returns 1 for
/// exp == 0 (including 0^0), and 0 for base == 0 with exp > 0.
pub fn pow(base: u16, e: u32) u16 {
    if (e == 0) return 1;
    if (base == 0) return 0;
    const l = @as(u64, tables.log[base]) * @as(u64, e);
    return tables.exp[@intCast(l % mult_group_order)];
}

/// α^(e mod mult_group_order) — the building block for PAR2's
/// recovery-slice coefficients. Returns 1 for e == 0.
pub fn expMod(e: u32) u16 {
    return tables.exp[e % mult_group_order];
}

/// log_α(a). The caller must ensure a != 0; the zero input returns the
/// sentinel 0, not a meaningful answer.
pub fn log(a: u16) u16 {
    return tables.log[a];
}

// ---------------------------------------------------------------------
// Bulk `acc ^= c * src`
// ---------------------------------------------------------------------

/// `basis(c)[i] = c * 2^i`. Because multiplication by a constant is a
/// GF(2)-linear map, these sixteen values determine the whole map:
/// `c * v = XOR over set bits i of v of basis[i]`. Both the split-table
/// builder and the SIMD path are derived from this.
fn constBasis(c: u16) [16]u16 {
    var basis: [16]u16 = undefined;
    var x: u32 = c;
    for (&basis) |*slot| {
        slot.* = @intCast(x);
        x <<= 1;
        if (x & 0x10000 != 0) x ^= irreducible_poly;
    }
    return basis;
}

/// Two 256-entry multiplication tables for one fixed constant:
/// `lo[x] = c * x` and `hi[x] = c * (x << 8)`, so
/// `c * v = lo[v & 0xFF] ^ hi[v >> 8]`.
///
/// 1 KiB total, versus 256 KiB for the log/exp pair — it stays in L1
/// for the entire slice, which is the whole point.
pub const SplitTable = struct {
    lo: [256]u16,
    hi: [256]u16,

    pub fn init(c: u16) SplitTable {
        var t: SplitTable = undefined;
        const basis = constBasis(c);
        // Doubling construction: each new power of two doubles the
        // populated prefix by XORing in one basis vector. 255 XORs per
        // table instead of 256 log/exp multiplies.
        t.lo[0] = 0;
        t.hi[0] = 0;
        var bit: usize = 0;
        while (bit < 8) : (bit += 1) {
            const span = @as(usize, 1) << @intCast(bit);
            for (0..span) |x| {
                t.lo[x + span] = t.lo[x] ^ basis[bit];
                t.hi[x + span] = t.hi[x] ^ basis[bit + 8];
            }
        }
        return t;
    }

    pub inline fn at(t: *const SplitTable, v: u16) u16 {
        return t.lo[v & 0xFF] ^ t.hi[v >> 8];
    }
};

/// Vector width for the SIMD path. u16 lanes, so this is 8 on NEON and
/// 16 on AVX2.
const vlen = std.simd.suggestVectorLength(u16) orelse 8;

/// Reference implementation: element-wise `mul` through the log/exp
/// tables. Every other bulk path is asserted equal to this one.
pub fn mulAddSliceLogExp(acc: []u16, src: []const u16, c: u16) void {
    std.debug.assert(acc.len == src.len);
    if (c == 0) return;
    const lc = @as(u32, tables.log[c]);
    for (acc, src) |*a, s| {
        if (s == 0) continue;
        a.* ^= tables.exp[(lc + @as(u32, tables.log[s])) % mult_group_order];
    }
}

/// Split-table bulk multiply-accumulate. Two independent L1 loads per
/// element, no modulo, no branches.
pub fn mulAddSliceSplit(acc: []u16, src: []const u16, c: u16) void {
    std.debug.assert(acc.len == src.len);
    if (c == 0) return;
    const t = SplitTable.init(c);
    for (acc, src) |*a, s| a.* ^= t.at(s);
}

/// Table-free bulk multiply-accumulate.
///
/// `c * v = XOR over set bits i of v of (c * 2^i)`, so with the sixteen
/// `c * 2^i` constants splatted into registers the whole slice can be
/// done with shifts, ANDs and XORs — no loads besides the data itself.
/// The bit mask is produced by an arithmetic right shift of the bit into
/// the sign position, which broadcasts it across the lane without a
/// compare.
///
/// This is exactly the same linear map as the scalar path, not an
/// approximation of it: same XOR of the same sixteen basis vectors.
pub fn mulAddSliceSimd(acc: []u16, src: []const u16, c: u16) void {
    std.debug.assert(acc.len == src.len);
    if (c == 0) return;
    const basis = constBasis(c);

    const V = @Vector(vlen, u16);
    const S = @Vector(vlen, i16);
    const Sh = @Vector(vlen, u4);
    const sign_shift: Sh = @splat(15);

    var i: usize = 0;
    while (i + vlen <= src.len) : (i += vlen) {
        const v: V = src[i..][0..vlen].*;
        var r: V = @splat(0);
        inline for (0..16) |bit| {
            const up: Sh = @splat(@intCast(15 - bit));
            const mask: V = @bitCast(@as(S, @bitCast(v << up)) >> sign_shift);
            const coef: V = @splat(basis[bit]);
            r ^= mask & coef;
        }
        const a: V = acc[i..][0..vlen].*;
        acc[i..][0..vlen].* = a ^ r;
    }
    // Scalar tail through the same basis so the result cannot diverge.
    while (i < src.len) : (i += 1) {
        var r: u16 = 0;
        var v = src[i];
        var bit: usize = 0;
        while (v != 0) : (bit += 1) {
            if (v & 1 != 0) r ^= basis[bit];
            v >>= 1;
        }
        acc[i] ^= r;
    }
}

/// Below this many elements the split tables' 512-operation build cost
/// is a visible fraction of the work; above it, it amortises away.
const split_table_threshold = 2048;

/// `acc[k] ^= c * src[k]` over the whole slice — the repair hot loop.
///
/// Measured on aarch64 (8×u16 lanes, ReleaseFast, GB/s, best of three):
///
/// | elements | log/exp | split | simd |
/// |-|-|-|-|
/// | 512 | 2.81 | 3.18 | 4.45 |
/// | 8192 | 2.04 | 4.68 | 4.51 |
/// | 32768 | 1.95 | 4.71 | 4.54 |
/// | 524288 | 1.95 | 4.67 | 4.51 |
///
/// The two fast paths are within a few percent of each other on long
/// slices — sixteen register-only masked XORs per vector costs about the
/// same as two L1 loads per element — but they cross over at short
/// lengths, where the split tables have not paid for themselves yet.
/// Widening the SIMD path past the native vector length made it slower
/// (4.49 → 4.25 → 4.01 GB/s at 8 / 16 / 32 lanes), so it stays native.
///
/// log/exp loses by 2.4x purely on cache footprint: its tables are
/// 256 KiB and the split ones are 1 KiB.
pub fn mulAddSlice(acc: []u16, src: []const u16, c: u16) void {
    if (src.len < split_table_threshold) {
        mulAddSliceSimd(acc, src, c);
    } else {
        mulAddSliceSplit(acc, src, c);
    }
}

// ---------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------

test "tables are inverses" {
    for (1..field_size) |v| {
        try std.testing.expectEqual(@as(u16, @intCast(v)), tables.exp[tables.log[v]]);
    }
}

test "exp cycle" {
    try std.testing.expectEqual(@as(u16, 1), expMod(mult_group_order));
    try std.testing.expectEqual(@as(u16, 1), expMod(0));
    try std.testing.expectEqual(generator, expMod(1));
}

test "mul identity and annihilator" {
    for ([_]u16{ 0, 1, 2, 7, 0x1234, 0xFFFE, 0xFFFF }) |v| {
        try std.testing.expectEqual(v, mul(v, 1));
        try std.testing.expectEqual(v, mul(1, v));
        try std.testing.expectEqual(@as(u16, 0), mul(v, 0));
        try std.testing.expectEqual(@as(u16, 0), mul(0, v));
    }
}

test "mul is commutative" {
    const pairs = [_][2]u16{
        .{ 2, 3 }, .{ 0x100, 0x101 }, .{ 0xABCD, 0x1234 }, .{ 0xFFFF, 0xFFFE },
    };
    for (pairs) |p| {
        try std.testing.expectEqual(mul(p[0], p[1]), mul(p[1], p[0]));
    }
}

test "mul is associative" {
    for ([_]u16{ 1, 2, 7, 0x1234 }) |a| {
        for ([_]u16{ 3, 0x100, 0xBEEF }) |b| {
            for ([_]u16{ 5, 0x10, 0xFFFF }) |c| {
                try std.testing.expectEqual(mul(mul(a, b), c), mul(a, mul(b, c)));
            }
        }
    }
}

test "mul distributes over add" {
    for ([_]u16{ 1, 2, 0x100, 0xBEEF }) |a| {
        for ([_]u16{ 3, 0x10, 0xFFFE }) |b| {
            for ([_]u16{ 5, 0x1000, 0x1234 }) |c| {
                try std.testing.expectEqual(mul(a, b ^ c), mul(a, b) ^ mul(a, c));
            }
        }
    }
}

test "v * inv(v) is one" {
    for ([_]u16{ 1, 2, 7, 0x100, 0x1234, 0xFFFE, 0xFFFF }) |v| {
        try std.testing.expectEqual(@as(u16, 1), mul(v, try inv(v)));
    }
}

test "div then mul round-trips" {
    const vals = [_]u16{ 1, 2, 7, 0x100, 0xBEEF };
    for (vals) |a| {
        for (vals) |b| {
            const q = try div(a, b);
            try std.testing.expectEqual(a, mul(q, b));
        }
    }
}

test "pow" {
    const cases = [_]struct { base: u16, e: u32, want: u16 }{
        .{ .base = 2, .e = 0, .want = 1 },
        .{ .base = 2, .e = 1, .want = 2 },
        .{ .base = 2, .e = 2, .want = 4 },
        .{ .base = 2, .e = 3, .want = 8 },
        .{ .base = 2, .e = 15, .want = 0x8000 },
        .{ .base = 2, .e = mult_group_order, .want = 1 }, // full cycle
        .{ .base = 2, .e = mult_group_order + 1, .want = 2 }, // wraps
        .{ .base = 0, .e = 5, .want = 0 },
        .{ .base = 0, .e = 0, .want = 1 }, // 0^0 = 1 by convention
        .{ .base = 1, .e = 1234567, .want = 1 },
    };
    for (cases) |tc| {
        try std.testing.expectEqual(tc.want, pow(tc.base, tc.e));
    }
}

test "div by zero and inverse of zero are errors" {
    try std.testing.expectError(error.DivideByZero, div(1, 0));
    try std.testing.expectError(error.DivideByZero, inv(0));
}

test "split table matches scalar mul over the whole field" {
    // One constant, every possible operand — this is the tightest
    // available check that the doubling construction is right.
    for ([_]u16{ 1, 2, 3, 0x100, 0xBEEF, 0xFFFF }) |c| {
        const t = SplitTable.init(c);
        for (0..field_size) |v| {
            const e: u16 = @intCast(v);
            try std.testing.expectEqual(mul(c, e), t.at(e));
        }
    }
}

test "bulk paths agree with the log/exp reference" {
    const t = std.testing;
    var prng = std.Random.DefaultPrng.init(0xC0FFEE);
    const rand = prng.random();

    // Lengths deliberately straddle the vector width so the SIMD tail
    // is exercised: 0, sub-vector, exact multiple, multiple plus tail.
    const lens = [_]usize{ 0, 1, 3, vlen - 1, vlen, vlen + 1, 2 * vlen + 5, 1000, 4096 };
    for (lens) |n| {
        const src = try t.allocator.alloc(u16, n);
        defer t.allocator.free(src);
        const base = try t.allocator.alloc(u16, n);
        defer t.allocator.free(base);
        for (src, base) |*s, *b| {
            s.* = rand.int(u16);
            b.* = rand.int(u16);
        }

        const want = try t.allocator.alloc(u16, n);
        defer t.allocator.free(want);
        const split = try t.allocator.alloc(u16, n);
        defer t.allocator.free(split);
        const simd = try t.allocator.alloc(u16, n);
        defer t.allocator.free(simd);

        for ([_]u16{ 0, 1, 2, 0x1234, 0xFFFF, rand.int(u16) }) |c| {
            @memcpy(want, base);
            @memcpy(split, base);
            @memcpy(simd, base);
            mulAddSliceLogExp(want, src, c);
            mulAddSliceSplit(split, src, c);
            mulAddSliceSimd(simd, src, c);
            try t.expectEqualSlices(u16, want, split);
            try t.expectEqualSlices(u16, want, simd);
        }
    }
}

test "bulk multiply-accumulate is linear" {
    // acc ^= c*x then acc ^= c*y equals acc ^= c*(x^y): the property
    // Reed-Solomon actually depends on.
    const t = std.testing;
    var prng = std.Random.DefaultPrng.init(7);
    const rand = prng.random();
    const n = 257;

    var x: [n]u16 = undefined;
    var y: [n]u16 = undefined;
    var xor: [n]u16 = undefined;
    for (0..n) |i| {
        x[i] = rand.int(u16);
        y[i] = rand.int(u16);
        xor[i] = x[i] ^ y[i];
    }

    const c: u16 = 0xABCD;
    var a: [n]u16 = @splat(0);
    mulAddSlice(&a, &x, c);
    mulAddSlice(&a, &y, c);

    var b: [n]u16 = @splat(0);
    mulAddSlice(&b, &xor, c);

    try t.expectEqualSlices(u16, &b, &a);
}
