//! Bounded cursor over an in-memory RAR header, plus the RAR5
//! variable-length integer codec.
//!
//! Every RAR header field is read through this type. That is deliberate:
//! RAR is reverse-engineered and the headers come off Usenet, so a
//! length field is a *claim*, never a fact. The cursor makes the bound
//! check unforgettable — there is no way to read a field without going
//! through a function that can return `error.EndOfHeader`.
//!
//! # RAR5 vint
//!
//! RAR5 encodes integers little-endian, 7 bits per byte, with the high
//! bit meaning "another byte follows":
//!
//!     0x00              -> 0
//!     0x7f              -> 127
//!     0x80 0x01         -> 128        (0x00 | 0x01 << 7)
//!     0xff 0x7f         -> 16383
//!     0x80 0x80 0x01    -> 16384
//!
//! The encoding has no natural length limit, so a hostile archive can
//! hand us a megabyte of 0x80 bytes. `max_vint_len` caps it at the 10
//! bytes needed to express 64 bits, and the accumulator is a u128 so
//! the overflow check is a comparison rather than a wrapping subtlety.

const std = @import("std");

pub const Error = error{
    /// A field ran past the end of the header buffer. Either the archive
    /// is truncated or a length field lied.
    EndOfHeader,
    /// A vint whose value does not fit in 64 bits, or one still asking
    /// for continuation after `max_vint_len` bytes.
    VintOverflow,
};

/// 10 * 7 = 70 bits, the fewest bytes that can hold a full u64.
pub const max_vint_len = 10;

pub const Cursor = struct {
    buf: []const u8,
    pos: usize = 0,

    pub fn init(buf: []const u8) Cursor {
        return .{ .buf = buf };
    }

    pub fn remaining(c: *const Cursor) usize {
        return c.buf.len - c.pos;
    }

    /// The unconsumed tail. Borrowed from `buf`.
    pub fn rest(c: *const Cursor) []const u8 {
        return c.buf[c.pos..];
    }

    pub fn take(c: *Cursor, n: usize) Error![]const u8 {
        if (n > c.remaining()) return error.EndOfHeader;
        const out = c.buf[c.pos..][0..n];
        c.pos += n;
        return out;
    }

    pub fn skip(c: *Cursor, n: usize) Error!void {
        _ = try c.take(n);
    }

    pub fn byte(c: *Cursor) Error!u8 {
        return (try c.take(1))[0];
    }

    pub fn u16le(c: *Cursor) Error!u16 {
        return std.mem.readInt(u16, (try c.take(2))[0..2], .little);
    }

    pub fn u32le(c: *Cursor) Error!u32 {
        return std.mem.readInt(u32, (try c.take(4))[0..4], .little);
    }

    pub fn u64le(c: *Cursor) Error!u64 {
        return std.mem.readInt(u64, (try c.take(8))[0..8], .little);
    }

    /// Reads one RAR5 vint. Bounded by both the buffer and
    /// `max_vint_len`; cannot overflow silently.
    pub fn vint(c: *Cursor) Error!u64 {
        var acc: u128 = 0;
        var shift: u7 = 0;
        var i: usize = 0;
        while (i < max_vint_len) : (i += 1) {
            const b = try c.byte();
            acc |= @as(u128, b & 0x7f) << shift;
            if (b & 0x80 == 0) {
                if (acc > std.math.maxInt(u64)) return error.VintOverflow;
                return @intCast(acc);
            }
            shift += 7;
        }
        // Eleven bytes and still continuing: not a number we will ever
        // need, so it is corruption or an attack.
        return error.VintOverflow;
    }

    /// Byte length of the vint starting at `pos`, without consuming it.
    /// Used where the wire format defines a field as "everything after
    /// the size vint", so the size of the size matters.
    pub fn peekVintLen(c: *const Cursor) Error!usize {
        var i: usize = 0;
        while (i < max_vint_len) : (i += 1) {
            if (c.pos + i >= c.buf.len) return error.EndOfHeader;
            if (c.buf[c.pos + i] & 0x80 == 0) return i + 1;
        }
        return error.VintOverflow;
    }
};

/// Encodes `v` as a vint into `out`, returning the bytes written.
/// `out` must be at least `max_vint_len` long. Only the test fixture
/// builders need this, but it belongs next to the decoder so the two
/// can be round-tripped against each other.
pub fn encodeVint(v: u64, out: []u8) []u8 {
    std.debug.assert(out.len >= max_vint_len);
    var x = v;
    var i: usize = 0;
    while (true) {
        const low: u8 = @intCast(x & 0x7f);
        x >>= 7;
        if (x == 0) {
            out[i] = low;
            i += 1;
            break;
        }
        out[i] = low | 0x80;
        i += 1;
    }
    return out[0..i];
}

// ---------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------

const t = std.testing;

test "vint decodes the documented boundary cases" {
    const cases = [_]struct { bytes: []const u8, want: u64 }{
        .{ .bytes = &.{0x00}, .want = 0 },
        .{ .bytes = &.{0x01}, .want = 1 },
        .{ .bytes = &.{0x7f}, .want = 127 },
        .{ .bytes = &.{ 0x80, 0x01 }, .want = 128 },
        .{ .bytes = &.{ 0xff, 0x7f }, .want = 16383 },
        .{ .bytes = &.{ 0x80, 0x80, 0x01 }, .want = 16384 },
        .{ .bytes = &.{ 0xac, 0x02 }, .want = 300 },
        // 2^32: 5 groups of 7 bits.
        .{ .bytes = &.{ 0x80, 0x80, 0x80, 0x80, 0x10 }, .want = 1 << 32 },
        // maxInt(u64): nine 0xff groups then the top bit.
        .{
            .bytes = &.{ 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01 },
            .want = std.math.maxInt(u64),
        },
    };
    for (cases) |c| {
        var cur = Cursor.init(c.bytes);
        try t.expectEqual(c.want, try cur.vint());
        try t.expectEqual(@as(usize, 0), cur.remaining());
        try t.expectEqual(c.bytes.len, try Cursor.init(c.bytes).peekVintLen());
    }
}

test "vint refuses to read past the buffer" {
    // Continuation bit set on the last available byte.
    var cur = Cursor.init(&.{0x80});
    try t.expectError(error.EndOfHeader, cur.vint());

    var empty = Cursor.init(&.{});
    try t.expectError(error.EndOfHeader, empty.vint());
    try t.expectError(error.EndOfHeader, empty.peekVintLen());

    // Truncated mid-way through a three-byte vint.
    var mid = Cursor.init(&.{ 0x80, 0x80 });
    try t.expectError(error.EndOfHeader, mid.vint());
}

test "vint rejects overflow rather than wrapping" {
    // Ten full groups: 70 bits of payload, so bit 64 is set.
    var over = Cursor.init(&.{ 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x7f });
    try t.expectError(error.VintOverflow, over.vint());

    // Eleven bytes still asking for more.
    var endless = Cursor.init(&.{ 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x00 });
    try t.expectError(error.VintOverflow, endless.vint());
    try t.expectError(error.VintOverflow, Cursor.init(&.{
        0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x00,
    }).peekVintLen());
}

test "vint round-trips through the encoder" {
    var buf: [max_vint_len]u8 = undefined;
    const values = [_]u64{
        0,       1,       127,     128,                      129,
        16383,   16384,   1 << 20, 1 << 31,                  (1 << 32) - 1,
        1 << 32, 1 << 56, 1 << 63, std.math.maxInt(u64) - 1, std.math.maxInt(u64),
    };
    for (values) |v| {
        const enc = encodeVint(v, &buf);
        try t.expect(enc.len <= max_vint_len);
        var cur = Cursor.init(enc);
        try t.expectEqual(v, try cur.vint());
        try t.expectEqual(enc.len, try Cursor.init(enc).peekVintLen());
    }
}

test "fixed-width reads are bounds checked" {
    var cur = Cursor.init(&.{ 0x01, 0x02, 0x03, 0x04, 0x05, 0x06 });
    try t.expectEqual(@as(u8, 1), try cur.byte());
    try t.expectEqual(@as(u16, 0x0302), try cur.u16le());
    try t.expectError(error.EndOfHeader, cur.u32le());
    try t.expectEqual(@as(usize, 3), cur.remaining());
    try t.expectEqualSlices(u8, &.{ 0x04, 0x05, 0x06 }, cur.rest());
    try t.expectError(error.EndOfHeader, cur.take(4));
    try t.expectEqualSlices(u8, &.{ 0x04, 0x05 }, try cur.take(2));
    try t.expectError(error.EndOfHeader, cur.u16le());

    var wide = Cursor.init(&.{ 0, 0, 0, 0, 0, 0, 0, 0x80 });
    try t.expectEqual(@as(u64, 1) << 63, try wide.u64le());
}
