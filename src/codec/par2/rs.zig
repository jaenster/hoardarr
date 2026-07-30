//! Reed-Solomon encode and reconstruct for PAR2.
//!
//! The layout PAR2 imposes:
//!
//!   * a file's bytes are partitioned into fixed-size slices of
//!     `slice_size` bytes, the last one zero-padded;
//!   * each slice is a vector of GF(2^16) elements, two consecutive
//!     bytes little-endian per element, so `slice_size` must be even.
//!
//! Recovery generation, for the recovery slice with exponent `e`:
//!
//!     R_e[k] = Σ over i of α^(i·e) · D_i[k]
//!
//! where `D_i` is the i-th data slice of the recovery set in canonical
//! order, `k` indexes elements within the slice, and α = 2.
//!
//! Reconstruction inverts that. With `m` slices missing, pick `m`
//! recovery slices, subtract off the contribution of everything still
//! present, and what remains is an m×m linear system in the missing
//! slices — the same system for every element position `k`, so the
//! matrix is inverted once and applied element-wise.
//!
//! Everything here works on `[]u16` element arrays rather than raw
//! bytes: the bulk GF multiply wants aligned, natively-typed lanes, and
//! converting once per slice is a memcpy on a little-endian host.

const std = @import("std");
const builtin = @import("builtin");
const gf16 = @import("gf16.zig");
const Matrix = @import("matrix.zig").Matrix;

const native_endian = builtin.cpu.arch.endian();

pub const Error = error{
    OutOfMemory,
    /// Slice size is zero or odd. PAR2 slices are arrays of 16-bit
    /// field elements, so an odd size is not representable.
    BadSliceSize,
    /// Data slices of differing lengths were handed to the encoder, or
    /// a recovery/present slice did not match the declared slice size.
    SliceLengthMismatch,
    /// Fewer recovery slices than missing data slices, or the resulting
    /// coefficient matrix is singular. Either way the repair cannot
    /// proceed with what is available.
    Unrecoverable,
    /// A missing index does not address a slice in the set.
    MissingIndexOutOfRange,
};

/// One recovery slice as it comes off a `.par2` volume file.
pub const RecoverySlice = struct {
    exponent: u16,
    body: []const u8,
};

// ---------------------------------------------------------------------
// Byte <-> element conversion
// ---------------------------------------------------------------------

/// Reads `src` as little-endian u16 elements into `dst`.
/// `src.len` must equal `dst.len * 2`.
pub fn loadElements(dst: []u16, src: []const u8) void {
    std.debug.assert(src.len == dst.len * 2);
    // A plain memcpy on a little-endian host; the byte swap below is
    // dead code there.
    @memcpy(std.mem.sliceAsBytes(dst), src);
    if (native_endian != .little) {
        for (dst) |*v| v.* = @byteSwap(v.*);
    }
}

/// Writes `src` out as little-endian bytes into `dst`.
/// `dst.len` must equal `src.len * 2`.
pub fn storeElements(dst: []u8, src: []const u16) void {
    std.debug.assert(dst.len == src.len * 2);
    if (native_endian == .little) {
        @memcpy(dst, std.mem.sliceAsBytes(src));
    } else {
        for (src, 0..) |v, i| std.mem.writeInt(u16, dst[i * 2 ..][0..2], v, .little);
    }
}

/// Allocating form of `loadElements`. `buf.len` must be even.
pub fn bytesToElements(alloc: std.mem.Allocator, buf: []const u8) Error![]u16 {
    if (buf.len % 2 != 0) return error.BadSliceSize;
    const out = try alloc.alloc(u16, buf.len / 2);
    loadElements(out, buf);
    return out;
}

/// Allocating form of `storeElements`.
pub fn elementsToBytes(alloc: std.mem.Allocator, elems: []const u16) Error![]u8 {
    const out = try alloc.alloc(u8, elems.len * 2);
    storeElements(out, elems);
    return out;
}

/// Partitions `data` into `ceil(len / slice_size)` slices of exactly
/// `slice_size` bytes, zero-padding the last one. Caller owns both the
/// outer array and each slice; `freeSlices` releases both.
pub fn splitIntoSlices(
    alloc: std.mem.Allocator,
    data: []const u8,
    slice_size: usize,
) Error![][]u8 {
    if (slice_size == 0 or slice_size % 2 != 0) return error.BadSliceSize;
    const n = (data.len + slice_size - 1) / slice_size;
    const out = try alloc.alloc([]u8, n);
    var made: usize = 0;
    errdefer {
        for (out[0..made]) |s| alloc.free(s);
        alloc.free(out);
    }
    for (out, 0..) |*dst, i| {
        const buf = try alloc.alloc(u8, slice_size);
        made += 1;
        dst.* = buf;
        const from = i * slice_size;
        const to = @min(from + slice_size, data.len);
        @memcpy(buf[0 .. to - from], data[from..to]);
        @memset(buf[to - from ..], 0);
    }
    return out;
}

/// Frees what `splitIntoSlices` or `reconstruct` returned.
pub fn freeSlices(alloc: std.mem.Allocator, slices: [][]u8) void {
    for (slices) |s| alloc.free(s);
    alloc.free(slices);
}

// ---------------------------------------------------------------------
// Encode
// ---------------------------------------------------------------------

/// Computes the recovery slice for one exponent:
/// `R_e[k] = Σ over i of α^(i·e) · D_i[k]`.
///
/// `data_slices` is the canonical ordering of the set's data slices; all
/// must be the same even length. The result is a fresh buffer of that
/// same length, owned by the caller.
pub fn encodeRecoverySlice(
    alloc: std.mem.Allocator,
    data_slices: []const []const u8,
    exponent: u16,
) Error![]u8 {
    if (data_slices.len == 0) return alloc.alloc(u8, 0);
    const slice_len = data_slices[0].len;
    if (slice_len == 0 or slice_len % 2 != 0) return error.BadSliceSize;
    for (data_slices) |s| {
        if (s.len != slice_len) return error.SliceLengthMismatch;
    }
    const elem_count = slice_len / 2;

    const acc = try alloc.alloc(u16, elem_count);
    defer alloc.free(acc);
    @memset(acc, 0);

    // One scratch buffer reused for every data slice, so the whole
    // encode is three allocations regardless of set size.
    const scratch = try alloc.alloc(u16, elem_count);
    defer alloc.free(scratch);

    for (data_slices, 0..) |s, i| {
        const coef = gf16.expMod(@as(u32, @intCast(i)) *% @as(u32, exponent));
        if (coef == 0) continue;
        loadElements(scratch, s);
        gf16.mulAddSlice(acc, scratch, coef);
    }
    return elementsToBytes(alloc, acc);
}

// ---------------------------------------------------------------------
// Reconstruct
// ---------------------------------------------------------------------

/// Everything `reconstruct` needs.
pub const ReconstructInput = struct {
    /// Bytes per slice. Even.
    slice_size: usize,
    /// One entry per data slice in the set, in canonical order. Missing
    /// slices may be null; entries at indices listed in `missing` are
    /// never read.
    present: []const ?[]const u8,
    /// Indices into `present` that are damaged or lost, ascending. Order
    /// is preserved in the result.
    missing: []const usize,
    /// Available recovery slices. Sorted by exponent internally, so the
    /// caller may pass them in any order.
    recovery: []const RecoverySlice,
};

/// Rebuilds the missing data slices, returned in the same order as
/// `in.missing`. The input is not mutated. Caller owns the result;
/// release it with `freeSlices`.
pub fn reconstruct(alloc: std.mem.Allocator, in: ReconstructInput) Error![][]u8 {
    const missing_count = in.missing.len;
    if (missing_count == 0) return alloc.alloc([]u8, 0);
    if (in.slice_size == 0 or in.slice_size % 2 != 0) return error.BadSliceSize;
    if (in.recovery.len < missing_count) return error.Unrecoverable;
    for (in.missing) |m| {
        if (m >= in.present.len) return error.MissingIndexOutOfRange;
    }
    const elem_count = in.slice_size / 2;

    // Deterministic choice of recovery exponents: smallest first, so two
    // runs of the same repair pick the same equations.
    const exps = try alloc.alloc(u16, in.recovery.len);
    defer alloc.free(exps);
    for (in.recovery, 0..) |r, i| exps[i] = r.exponent;
    std.mem.sort(u16, exps, {}, std.sort.asc(u16));
    const chosen = exps[0..missing_count];

    // Coefficient matrix M[k][j] = α^(missing[j] · chosen[k]).
    // Vandermonde-like in the exponents, hence invertible for distinct
    // rows — the property that makes PAR2 recovery work at all.
    var m = try Matrix.init(alloc, missing_count, missing_count);
    defer m.deinit(alloc);
    for (chosen, 0..) |e, k| {
        for (in.missing, 0..) |idx, j| {
            m.set(k, j, gf16.expMod(@as(u32, @intCast(idx)) *% @as(u32, e)));
        }
    }
    var m_inv = m.invert(alloc) catch |err| switch (err) {
        error.OutOfMemory => return error.OutOfMemory,
        // A singular system means the chosen recovery slices do not span
        // the missing ones. Not a bug — just not repairable from these.
        error.Singular, error.NotSquare, error.DimensionMismatch => return error.Unrecoverable,
    };
    defer m_inv.deinit(alloc);

    // residual[k] = recv_chosen[k] - Σ over present i of α^(i·e) · D_i,
    // which leaves exactly Σ over missing i of α^(i·e) · D_i.
    const residuals = try alloc.alloc(u16, missing_count * elem_count);
    defer alloc.free(residuals);

    const scratch = try alloc.alloc(u16, elem_count);
    defer alloc.free(scratch);

    for (chosen, 0..) |e, k| {
        const body = findRecovery(in.recovery, e).?;
        if (body.len != in.slice_size) return error.SliceLengthMismatch;
        const res = residuals[k * elem_count ..][0..elem_count];
        loadElements(res, body);

        for (in.present, 0..) |maybe, i| {
            if (isMissing(in.missing, i)) continue;
            const s = maybe orelse return error.SliceLengthMismatch;
            if (s.len != in.slice_size) return error.SliceLengthMismatch;
            const coef = gf16.expMod(@as(u32, @intCast(i)) *% @as(u32, e));
            if (coef == 0) continue;
            loadElements(scratch, s);
            gf16.mulAddSlice(res, scratch, coef);
        }
    }

    // out[j] = Σ over k of M⁻¹[j][k] · residual[k].
    const out_elems = try alloc.alloc(u16, missing_count * elem_count);
    defer alloc.free(out_elems);
    @memset(out_elems, 0);

    for (0..missing_count) |j| {
        const dst = out_elems[j * elem_count ..][0..elem_count];
        for (0..missing_count) |k| {
            const coef = m_inv.at(j, k);
            if (coef == 0) continue;
            gf16.mulAddSlice(dst, residuals[k * elem_count ..][0..elem_count], coef);
        }
    }

    const out = try alloc.alloc([]u8, missing_count);
    var made: usize = 0;
    errdefer {
        for (out[0..made]) |s| alloc.free(s);
        alloc.free(out);
    }
    for (out, 0..) |*dst, j| {
        const buf = try alloc.alloc(u8, in.slice_size);
        made += 1;
        dst.* = buf;
        storeElements(buf, out_elems[j * elem_count ..][0..elem_count]);
    }
    return out;
}

fn findRecovery(recovery: []const RecoverySlice, exponent: u16) ?[]const u8 {
    for (recovery) |r| {
        if (r.exponent == exponent) return r.body;
    }
    return null;
}

/// `missing` is ascending in every caller, so the scan can stop early.
pub fn isMissing(missing: []const usize, i: usize) bool {
    for (missing) |m| {
        if (m == i) return true;
        if (m > i) return false;
    }
    return false;
}

// ---------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------

test "elements round-trip" {
    const t = std.testing;
    var prng = std.Random.DefaultPrng.init(0x5EED);
    const rand = prng.random();

    for ([_]usize{ 0, 2, 4, 16, 1024 }) |n| {
        const buf = try t.allocator.alloc(u8, n);
        defer t.allocator.free(buf);
        rand.bytes(buf);

        const elems = try bytesToElements(t.allocator, buf);
        defer t.allocator.free(elems);
        const back = try elementsToBytes(t.allocator, elems);
        defer t.allocator.free(back);
        try t.expectEqualSlices(u8, buf, back);
    }
}

test "elements are little-endian pairs" {
    const t = std.testing;
    const elems = try bytesToElements(t.allocator, "\x34\x12\xFF\x00");
    defer t.allocator.free(elems);
    try t.expectEqualSlices(u16, &.{ 0x1234, 0x00FF }, elems);
}

test "odd sizes are rejected" {
    const t = std.testing;
    try t.expectError(error.BadSliceSize, bytesToElements(t.allocator, "abc"));
    try t.expectError(error.BadSliceSize, splitIntoSlices(t.allocator, "abcd", 3));
    try t.expectError(error.BadSliceSize, splitIntoSlices(t.allocator, "abcd", 0));
}

test "splitIntoSlices zero-pads the tail" {
    const t = std.testing;
    const slices = try splitIntoSlices(t.allocator, &.{ 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 }, 4);
    defer freeSlices(t.allocator, slices);

    try t.expectEqual(@as(usize, 3), slices.len);
    try t.expectEqualSlices(u8, &.{ 1, 2, 3, 4 }, slices[0]);
    try t.expectEqualSlices(u8, &.{ 5, 6, 7, 8 }, slices[1]);
    try t.expectEqualSlices(u8, &.{ 9, 10, 0, 0 }, slices[2]);
}

test "splitIntoSlices on an exact multiple makes no extra slice" {
    const t = std.testing;
    const slices = try splitIntoSlices(t.allocator, &.{ 1, 2, 3, 4 }, 4);
    defer freeSlices(t.allocator, slices);
    try t.expectEqual(@as(usize, 1), slices.len);
}

/// Encodes recovery slices for `exps` over `data`. Caller frees with
/// `freeRecovery`.
fn encodeAll(
    alloc: std.mem.Allocator,
    data: []const []const u8,
    exps: []const u16,
) ![]RecoverySlice {
    const out = try alloc.alloc(RecoverySlice, exps.len);
    errdefer alloc.free(out);
    for (exps, 0..) |e, i| {
        out[i] = .{ .exponent = e, .body = try encodeRecoverySlice(alloc, data, e) };
    }
    return out;
}

fn freeRecovery(alloc: std.mem.Allocator, r: []RecoverySlice) void {
    for (r) |s| alloc.free(@constCast(s.body));
    alloc.free(r);
}

/// Allocates `count` random slices of `slice_size` bytes.
fn randomSlices(
    alloc: std.mem.Allocator,
    count: usize,
    slice_size: usize,
    seed: u64,
) ![][]u8 {
    var prng = std.Random.DefaultPrng.init(seed);
    const out = try alloc.alloc([]u8, count);
    for (out) |*s| {
        s.* = try alloc.alloc(u8, slice_size);
        prng.random().bytes(s.*);
    }
    return out;
}

/// `present[i]` is `original[i]` unless i is in `missing`.
fn presentView(
    alloc: std.mem.Allocator,
    original: []const []u8,
    missing: []const usize,
) ![]?[]const u8 {
    const present = try alloc.alloc(?[]const u8, original.len);
    for (original, 0..) |s, i| present[i] = if (isMissing(missing, i)) null else s;
    return present;
}

test "encode then reconstruct recovers the damaged slices" {
    const t = std.testing;
    const alloc = t.allocator;
    const slice_size = 8;

    const original = try randomSlices(alloc, 4, slice_size, 0xD00D);
    defer freeSlices(alloc, original);

    const recovery = try encodeAll(alloc, @ptrCast(original), &.{ 1, 2 });
    defer freeRecovery(alloc, recovery);

    const missing = [_]usize{ 1, 3 };
    const present = try presentView(alloc, original, &missing);
    defer alloc.free(present);

    const got = try reconstruct(alloc, .{
        .slice_size = slice_size,
        .present = present,
        .missing = &missing,
        .recovery = recovery,
    });
    defer freeSlices(alloc, got);

    try t.expectEqual(@as(usize, 2), got.len);
    try t.expectEqualSlices(u8, original[1], got[0]);
    try t.expectEqualSlices(u8, original[3], got[1]);
}

test "too few recovery slices is unrecoverable" {
    const t = std.testing;
    const alloc = t.allocator;
    const data = [_][]const u8{
        &.{ 1, 2, 3, 4 },
        &.{ 5, 6, 7, 8 },
        &.{ 9, 10, 11, 12 },
    };
    const recovery = try encodeAll(alloc, &data, &.{1});
    defer freeRecovery(alloc, recovery);

    try t.expectError(error.Unrecoverable, reconstruct(alloc, .{
        .slice_size = 4,
        .present = &.{ data[0], null, null },
        .missing = &.{ 1, 2 },
        .recovery = recovery,
    }));
}

test "reconstruct with nothing missing returns nothing" {
    const t = std.testing;
    const got = try reconstruct(t.allocator, .{
        .slice_size = 4,
        .present = &.{},
        .missing = &.{},
        .recovery = &.{},
    });
    defer freeSlices(t.allocator, got);
    try t.expectEqual(@as(usize, 0), got.len);
}

test "reconstruct rejects an out-of-range missing index" {
    const t = std.testing;
    try t.expectError(error.MissingIndexOutOfRange, reconstruct(t.allocator, .{
        .slice_size = 4,
        .present = &.{ &.{ 1, 2, 3, 4 }, null },
        .missing = &.{ 1, 7 },
        .recovery = &.{
            .{ .exponent = 1, .body = &.{ 0, 0, 0, 0 } },
            .{ .exponent = 2, .body = &.{ 0, 0, 0, 0 } },
        },
    }));
}

test "reconstruct rejects a wrongly-sized recovery slice" {
    const t = std.testing;
    try t.expectError(error.SliceLengthMismatch, reconstruct(t.allocator, .{
        .slice_size = 4,
        .present = &.{ &.{ 1, 2, 3, 4 }, null },
        .missing = &.{1},
        .recovery = &.{.{ .exponent = 1, .body = &.{ 0, 0 } }},
    }));
}

test "encodeRecoverySlice rejects ragged input" {
    const t = std.testing;
    try t.expectError(error.SliceLengthMismatch, encodeRecoverySlice(
        t.allocator,
        &.{ &.{ 1, 2, 3, 4 }, &.{ 1, 2 } },
        1,
    ));
    try t.expectError(error.BadSliceSize, encodeRecoverySlice(
        t.allocator,
        &.{&.{ 1, 2, 3 }},
        1,
    ));
}

test "encode then reconstruct over a larger random set" {
    // 16 slices of 4 KiB, four damaged, four recovery slices. Large
    // enough that the bulk multiply takes its vectorised path, so an
    // element-indexing slip would show up as a mismatch.
    const t = std.testing;
    const alloc = t.allocator;
    const slice_size = 4096;

    const original = try randomSlices(alloc, 16, slice_size, 0xFEEDFACE);
    defer freeSlices(alloc, original);

    const recovery = try encodeAll(alloc, @ptrCast(original), &.{ 1, 2, 4, 8 });
    defer freeRecovery(alloc, recovery);

    const missing = [_]usize{ 2, 5, 11, 14 };
    const present = try presentView(alloc, original, &missing);
    defer alloc.free(present);

    const got = try reconstruct(alloc, .{
        .slice_size = slice_size,
        .present = present,
        .missing = &missing,
        .recovery = recovery,
    });
    defer freeSlices(alloc, got);

    for (missing, 0..) |idx, j| {
        try t.expectEqualSlices(u8, original[idx], got[j]);
    }
}

test "reconstruct picks the smallest exponents regardless of input order" {
    // Hand over more recovery slices than needed, shuffled, including one
    // whose body is deliberately junk at a high exponent. If the
    // "smallest first" choice holds, the junk is never consulted and the
    // repair still succeeds.
    const t = std.testing;
    const alloc = t.allocator;
    const slice_size = 32;

    const original = try randomSlices(alloc, 6, slice_size, 0xABCDEF);
    defer freeSlices(alloc, original);

    const good = try encodeAll(alloc, @ptrCast(original), &.{ 3, 1, 2 });
    defer freeRecovery(alloc, good);

    const junk = try alloc.alloc(u8, slice_size);
    defer alloc.free(junk);
    @memset(junk, 0xA5);

    const recovery = [_]RecoverySlice{
        good[0], // exponent 3
        .{ .exponent = 900, .body = junk },
        good[1], // exponent 1
        good[2], // exponent 2
    };

    const missing = [_]usize{ 0, 4 };
    const present = try presentView(alloc, original, &missing);
    defer alloc.free(present);

    const got = try reconstruct(alloc, .{
        .slice_size = slice_size,
        .present = present,
        .missing = &missing,
        .recovery = &recovery,
    });
    defer freeSlices(alloc, got);

    try t.expectEqualSlices(u8, original[0], got[0]);
    try t.expectEqualSlices(u8, original[4], got[1]);
}

test "a recovery slice with exponent zero is the XOR of every data slice" {
    // α^0 = 1 for every index, so this degenerate case has to come out as
    // a plain parity slice — a cheap check that the coefficient exponent
    // is indexed the way the spec says.
    const t = std.testing;
    const alloc = t.allocator;
    const data = [_][]const u8{
        &.{ 0x01, 0x02, 0x03, 0x04 },
        &.{ 0x10, 0x20, 0x30, 0x40 },
        &.{ 0xFF, 0x00, 0xFF, 0x00 },
    };
    const r = try encodeRecoverySlice(alloc, &data, 0);
    defer alloc.free(r);

    var want: [4]u8 = @splat(0);
    for (data) |s| {
        for (s, 0..) |b, i| want[i] ^= b;
    }
    try t.expectEqualSlices(u8, &want, r);
}

test "encodeRecoverySlice on an empty set yields an empty slice" {
    const t = std.testing;
    const r = try encodeRecoverySlice(t.allocator, &.{}, 5);
    defer t.allocator.free(r);
    try t.expectEqual(@as(usize, 0), r.len);
}

test "a single missing slice needs only one recovery slice" {
    const t = std.testing;
    const alloc = t.allocator;
    const slice_size = 64;
    const count = 5;

    const original = try randomSlices(alloc, count, slice_size, 11);
    defer freeSlices(alloc, original);

    const recovery = try encodeAll(alloc, @ptrCast(original), &.{7});
    defer freeRecovery(alloc, recovery);

    // Every index in turn, so the i=0 coefficient (α^0 = 1) and the
    // interior ones all get exercised.
    for (0..count) |lost| {
        const missing = [_]usize{lost};
        const present = try presentView(alloc, original, &missing);
        defer alloc.free(present);

        const got = try reconstruct(alloc, .{
            .slice_size = slice_size,
            .present = present,
            .missing = &missing,
            .recovery = recovery,
        });
        defer freeSlices(alloc, got);
        try t.expectEqualSlices(u8, original[lost], got[0]);
    }
}
