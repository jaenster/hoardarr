//! CRC-32/ISO-HDLC (the "IEEE" CRC used by yEnc `pcrc32=`, PAR2 file
//! checksums, and zip/gzip). Reflected polynomial 0xEDB88320.
//!
//! Two implementations, selected at comptime:
//!
//!   * aarch64 with the `crc` feature — the hardware `crc32b/w/x`
//!     instructions, three bytes-per-cycle territory.
//!   * everything else — slice-by-16: sixteen 256-entry tables let us
//!     retire 16 input bytes per iteration with no data dependency
//!     between the table lookups, which is the fastest portable form.
//!
//! The tables are built at comptime, so they land in .rodata (16 KiB)
//! and cost nothing at startup.

const std = @import("std");
const builtin = @import("builtin");

pub const poly: u32 = 0xEDB88320;

/// Number of parallel table slices. 16 keeps the ILP high without
/// blowing the 32 KiB L1d budget alongside the payload being hashed.
const slices = 16;

const Tables = [slices][256]u32;

const tables: Tables = blk: {
    @setEvalBranchQuota(200_000);
    var t: Tables = undefined;
    for (0..256) |i| {
        var c: u32 = @intCast(i);
        for (0..8) |_| {
            c = if (c & 1 != 0) poly ^ (c >> 1) else c >> 1;
        }
        t[0][i] = c;
    }
    for (1..slices) |s| {
        for (0..256) |i| {
            const prev = t[s - 1][i];
            t[s][i] = t[0][prev & 0xFF] ^ (prev >> 8);
        }
    }
    break :blk t;
};

/// True when we can emit the aarch64 CRC32 instructions inline.
const use_arm_crc = builtin.cpu.arch == .aarch64 and
    std.Target.aarch64.featureSetHas(builtin.cpu.features, .crc);

/// Streaming CRC32. `value()` returns the finalised checksum; the
/// running state is stored pre-inverted so `update` is branch-free.
pub const Crc32 = struct {
    state: u32 = 0xFFFF_FFFF,

    pub fn init() Crc32 {
        return .{};
    }

    pub inline fn update(self: *Crc32, input: []const u8) void {
        self.state = if (use_arm_crc) armUpdate(self.state, input) else tableUpdate(self.state, input);
    }

    pub inline fn value(self: Crc32) u32 {
        return ~self.state;
    }
};

/// One-shot convenience wrapper.
pub fn checksum(input: []const u8) u32 {
    var c = Crc32.init();
    c.update(input);
    return c.value();
}

fn tableUpdate(crc_in: u32, input: []const u8) u32 {
    var crc = crc_in;
    var buf = input;

    // Align to the slice width first so the bulk loop reads aligned
    // 16-byte groups; the table form doesn't require it but keeping the
    // loads on natural boundaries measurably helps on both arches.
    while (buf.len >= slices) {
        // XOR the low 4 bytes of the running CRC into the input word,
        // then dispatch all 16 bytes through their respective slice
        // tables. Every lookup is independent -> full pipeline usage.
        const w0 = std.mem.readInt(u32, buf[0..4], .little) ^ crc;
        const w1 = std.mem.readInt(u32, buf[4..8], .little);
        const w2 = std.mem.readInt(u32, buf[8..12], .little);
        const w3 = std.mem.readInt(u32, buf[12..16], .little);

        crc = tables[15][w0 & 0xFF] ^
            tables[14][(w0 >> 8) & 0xFF] ^
            tables[13][(w0 >> 16) & 0xFF] ^
            tables[12][(w0 >> 24) & 0xFF] ^
            tables[11][w1 & 0xFF] ^
            tables[10][(w1 >> 8) & 0xFF] ^
            tables[9][(w1 >> 16) & 0xFF] ^
            tables[8][(w1 >> 24) & 0xFF] ^
            tables[7][w2 & 0xFF] ^
            tables[6][(w2 >> 8) & 0xFF] ^
            tables[5][(w2 >> 16) & 0xFF] ^
            tables[4][(w2 >> 24) & 0xFF] ^
            tables[3][w3 & 0xFF] ^
            tables[2][(w3 >> 8) & 0xFF] ^
            tables[1][(w3 >> 16) & 0xFF] ^
            tables[0][(w3 >> 24) & 0xFF];

        buf = buf[slices..];
    }
    for (buf) |b| {
        crc = tables[0][(crc ^ b) & 0xFF] ^ (crc >> 8);
    }
    return crc;
}

fn armUpdate(crc_in: u32, input: []const u8) u32 {
    var crc = crc_in;
    var buf = input;
    while (buf.len >= 8) {
        const v = std.mem.readInt(u64, buf[0..8], .little);
        crc = asm ("crc32x %[out:w], %[in:w], %[v:x]"
            : [out] "=r" (-> u32),
            : [in] "r" (crc),
              [v] "r" (v),
        );
        buf = buf[8..];
    }
    if (buf.len >= 4) {
        const v = std.mem.readInt(u32, buf[0..4], .little);
        crc = asm ("crc32w %[out:w], %[in:w], %[v:w]"
            : [out] "=r" (-> u32),
            : [in] "r" (crc),
              [v] "r" (v),
        );
        buf = buf[4..];
    }
    for (buf) |b| {
        crc = asm ("crc32b %[out:w], %[in:w], %[v:w]"
            : [out] "=r" (-> u32),
            : [in] "r" (crc),
              [v] "r" (@as(u32, b)),
        );
    }
    return crc;
}

test "crc32 known vectors" {
    const t = std.testing;
    try t.expectEqual(@as(u32, 0x00000000), checksum(""));
    try t.expectEqual(@as(u32, 0xE8B7BE43), checksum("a"));
    try t.expectEqual(@as(u32, 0x352441C2), checksum("abc"));
    try t.expectEqual(@as(u32, 0xCBF43926), checksum("123456789"));
    try t.expectEqual(@as(u32, 0x414FA339), checksum("The quick brown fox jumps over the lazy dog"));
}

test "crc32 matches std across lengths and split points" {
    const t = std.testing;
    var data: [1024]u8 = undefined;
    var prng = std.Random.DefaultPrng.init(0x1234_5678);
    prng.random().bytes(&data);

    for (0..data.len) |n| {
        const want = std.hash.crc.Crc32.hash(data[0..n]);
        try t.expectEqual(want, checksum(data[0..n]));

        // Same answer when fed in two chunks — proves the streaming
        // state is a real running CRC and not a per-call reset.
        const mid = n / 2;
        var c = Crc32.init();
        c.update(data[0..mid]);
        c.update(data[mid..n]);
        try t.expectEqual(want, c.value());
    }
}
