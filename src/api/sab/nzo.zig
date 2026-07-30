//! `nzo_id` — the opaque job handle the *arr suite uses as a primary key.
//!
//! Shape: `"SABnzbd_nzo_" ++ base32(payload)` where payload is
//! `"<jobID>"` or `"<jobID>:<first-8-of-nzbHash>"`.
//!
//! Base32 rather than base64 because nzo_ids are typed, pasted and
//! embedded in URLs freely; `+` and `/` from base64 would drag
//! percent-encoding into every place one of these appears. RFC 4648
//! alphabet, no padding — Go's `base32.StdEncoding.WithPadding(NoPadding)`.
//!
//! ## Why the hash suffix exists
//!
//! SQLite's `INTEGER PRIMARY KEY` without `AUTOINCREMENT` reuses the
//! rowids of deleted rows. With an id-only nzo_id, Sonarr's per-grab
//! history (keyed on `downloadId`) matches a *stale failed* entry against
//! a brand-new job that happened to land on the same rowid, and calls
//! `DownloadEventHub.RemoveItem` on it within seconds of the grab. The
//! hash suffix makes the handle unique per creation rather than per id.
//!
//! `jobIdFrom` accepts both shapes, so nzo_ids minted before the suffix
//! existed — in-flight grabs, bookmarked URLs — keep resolving.

const std = @import("std");

pub const prefix = "SABnzbd_nzo_";

/// Longest handle we can produce: prefix + base32 of
/// "-9223372036854775808:deadbeef" (29 bytes -> 47 chars).
pub const Buf = [prefix.len + 64]u8;

pub const DecodeError = error{
    /// Missing the `SABnzbd_nzo_` prefix — almost always a client that
    /// invented an id or truncated one.
    BadPrefix,
    /// Not RFC 4648 base32, or a length no encoder could have produced.
    BadBase32,
    /// Decoded fine but the leading field is not a decimal integer.
    BadJobId,
};

/// Encodes `id` with no hash suffix. Only the fallback paths use this —
/// see the module comment for why the suffix matters.
pub fn encode(buf: *Buf, id: i64) []const u8 {
    return encodeWithHash(buf, id, "");
}

/// The canonical encoder. `hash` may be empty; when present, its first 8
/// characters are lowercased and appended after a colon.
pub fn encodeWithHash(buf: *Buf, id: i64, hash: []const u8) []const u8 {
    var payload: [64]u8 = undefined;
    var n: usize = 0;
    const id_str = std.fmt.bufPrint(&payload, "{d}", .{id}) catch unreachable;
    n = id_str.len;

    const trimmed = std.mem.trim(u8, hash, " \t\r\n");
    if (trimmed.len > 0) {
        payload[n] = ':';
        n += 1;
        const take = @min(trimmed.len, 8);
        for (trimmed[0..take]) |c| {
            payload[n] = std.ascii.toLower(c);
            n += 1;
        }
    }

    @memcpy(buf[0..prefix.len], prefix);
    const enc_len = base32Encode(buf[prefix.len..], payload[0..n]);
    return buf[0 .. prefix.len + enc_len];
}

/// The inverse of `encodeWithHash`, tolerating both shapes.
pub fn jobIdFrom(s: []const u8) DecodeError!i64 {
    if (!std.mem.startsWith(u8, s, prefix)) return error.BadPrefix;
    var out: [64]u8 = undefined;
    const decoded = try base32Decode(&out, s[prefix.len..]);
    // Everything from the ':' on is the disambiguating hash.
    const colon = std.mem.indexOfScalar(u8, decoded, ':') orelse decoded.len;
    return std.fmt.parseInt(i64, decoded[0..colon], 10) catch error.BadJobId;
}

// ---------------------------------------------------------------------
// Base32 (RFC 4648, no padding)
// ---------------------------------------------------------------------

const alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";

/// Encodes `src` into `dst`, which must hold `(src.len * 8 + 4) / 5`
/// bytes. Returns the number written.
pub fn base32Encode(dst: []u8, src: []const u8) usize {
    var acc: u16 = 0;
    var bits: u4 = 0;
    var n: usize = 0;
    for (src) |b| {
        acc = (acc << 8) | b;
        bits += 8;
        while (bits >= 5) {
            bits -= 5;
            dst[n] = alphabet[(acc >> bits) & 0x1F];
            n += 1;
        }
    }
    if (bits > 0) {
        // Trailing bits are zero-extended to a full symbol, which is what
        // makes the no-padding form decodable.
        dst[n] = alphabet[(acc << (5 - bits)) & 0x1F];
        n += 1;
    }
    return n;
}

/// Decodes `src` into `dst`. Uppercase only, as `base32.StdEncoding` is:
/// accepting lowercase would give every handle two spellings, and an id
/// with two spellings is an id that cannot be compared.
pub fn base32Decode(dst: []u8, src: []const u8) DecodeError![]const u8 {
    // Lengths a 5-bit encoder can never produce. 1 leftover symbol is 5
    // bits (no byte), 3 is 15 (one byte plus 7 spare), 6 is 30 (three
    // plus 6) — all of them mean the input was mangled.
    switch (src.len % 8) {
        1, 3, 6 => return error.BadBase32,
        else => {},
    }
    var acc: u16 = 0;
    var bits: u4 = 0;
    var n: usize = 0;
    for (src) |c| {
        const v: u8 = switch (c) {
            'A'...'Z' => c - 'A',
            '2'...'7' => c - '2' + 26,
            else => return error.BadBase32,
        };
        acc = (acc << 5) | v;
        bits += 5;
        if (bits >= 8) {
            bits -= 8;
            if (n == dst.len) return error.BadBase32;
            dst[n] = @truncate(acc >> bits);
            n += 1;
        }
    }
    return dst[0..n];
}

// ---------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------

const testing = std.testing;

test "base32 round-trips and matches Go's StdEncoding output" {
    // Right-hand sides come from
    // base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString.
    const cases = [_]struct { []const u8, []const u8 }{
        .{ "1", "GE" },
        .{ "42", "GQZA" },
        .{ "0", "GA" },
        .{ "42:deadbeef", "GQZDUZDFMFSGEZLFMY" },
        .{ "1234567890123:abcdef12", "GEZDGNBVGY3TQOJQGEZDGOTBMJRWIZLGGEZA" },
    };
    var enc_buf: [128]u8 = undefined;
    var dec_buf: [128]u8 = undefined;
    for (cases) |c| {
        const n = base32Encode(&enc_buf, c[0]);
        try testing.expectEqualStrings(c[1], enc_buf[0..n]);
        try testing.expectEqualStrings(c[0], try base32Decode(&dec_buf, c[1]));
    }
}

test "base32Decode rejects impossible lengths, lowercase and stray symbols" {
    var buf: [64]u8 = undefined;
    try testing.expectError(error.BadBase32, base32Decode(&buf, "G"));
    try testing.expectError(error.BadBase32, base32Decode(&buf, "GQZ"));
    try testing.expectError(error.BadBase32, base32Decode(&buf, "GQZDUZ"));
    try testing.expectError(error.BadBase32, base32Decode(&buf, "gqza"));
    try testing.expectError(error.BadBase32, base32Decode(&buf, "GEZA1"));
    try testing.expectError(error.BadBase32, base32Decode(&buf, "GEZA="));
    try testing.expectEqualStrings("", try base32Decode(&buf, ""));
}

test "encodeWithHash produces the SAB handle shape" {
    var buf: Buf = undefined;
    try testing.expectEqualStrings("SABnzbd_nzo_GQZDUZDFMFSGEZLFMY", encodeWithHash(&buf, 42, "deadbeefcafe"));
    // Only the first eight hash characters survive, lowercased, and
    // surrounding whitespace is ignored.
    try testing.expectEqualStrings(
        encodeWithHash(&buf, 42, "deadbeef"),
        "SABnzbd_nzo_GQZDUZDFMFSGEZLFMY",
    );
    var buf2: Buf = undefined;
    try testing.expectEqualStrings(
        encodeWithHash(&buf2, 42, "  DEADBEEFCAFE  "),
        encodeWithHash(&buf, 42, "deadbeef"),
    );
    try testing.expectEqualStrings("SABnzbd_nzo_GQZA", encode(&buf, 42));
    try testing.expectEqualStrings("SABnzbd_nzo_GA", encode(&buf, 0));
}

test "jobIdFrom inverts both the hashed and the bare shape" {
    var buf: Buf = undefined;
    const ids = [_]i64{ 0, 1, 42, 999999, 9223372036854775807, -1 };
    for (ids) |id| {
        try testing.expectEqual(id, try jobIdFrom(encode(&buf, id)));
        try testing.expectEqual(id, try jobIdFrom(encodeWithHash(&buf, id, "deadbeefcafe")));
    }
}

test "jobIdFrom rejects malformed handles rather than guessing an id" {
    try testing.expectError(error.BadPrefix, jobIdFrom(""));
    try testing.expectError(error.BadPrefix, jobIdFrom("42"));
    try testing.expectError(error.BadPrefix, jobIdFrom("sabnzbd_nzo_GQZA"));
    try testing.expectError(error.BadPrefix, jobIdFrom("SABnzbd_nzo"));
    try testing.expectError(error.BadBase32, jobIdFrom("SABnzbd_nzo_!!!!"));
    try testing.expectError(error.BadBase32, jobIdFrom("SABnzbd_nzo_G"));
    // Decodes cleanly but is not a number: "AAAA" -> 0x00 0x00.
    try testing.expectError(error.BadJobId, jobIdFrom("SABnzbd_nzo_"));
    try testing.expectError(error.BadJobId, jobIdFrom("SABnzbd_nzo_MFRGG"));
}
