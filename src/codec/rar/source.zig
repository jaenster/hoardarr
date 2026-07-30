//! Random-access byte source for one archive volume.
//!
//! A RAR volume is a file we read forwards, but the reader also needs to
//! know how long it is *before* trusting any length field in it — the
//! "reject a header claiming a 900 GB file inside a 40 MB volume" check
//! is a comparison against `Source.size`. So the abstraction is
//! positional read + known length, not a stream.
//!
//! Positional reads (`pread`) rather than seek+read: no shared cursor to
//! get out of sync, and the reader can hold several volumes open at once
//! without them interfering.
//!
//! Two implementations: `Memory` for hand-built test fixtures, `File`
//! for the real thing. Nothing here reads a volume into memory.

const std = @import("std");

pub const ReadError = error{
    /// The underlying read failed. `File.last_error` carries the
    /// original `std.Io` error for the log; the reader only needs to
    /// know that the volume is unusable.
    SourceReadFailed,
};

pub const Error = ReadError || error{
    /// A read that the recorded volume length said should succeed came
    /// up short, i.e. the file shrank under us or `size` lied.
    TruncatedVolume,
};

pub const Source = struct {
    /// Total length in bytes, sampled once when the volume was opened.
    /// Every bound check in the parser is against this.
    size: u64,
    context: *anyopaque,
    readAtFn: *const fn (context: *anyopaque, offset: u64, buf: []u8) ReadError!usize,

    /// Reads up to `buf.len` bytes at `offset`, clamped to the end of
    /// the volume. Returns the byte count; 0 means `offset` is at or
    /// past the end.
    pub fn readAt(s: Source, offset: u64, buf: []u8) ReadError!usize {
        if (offset >= s.size) return 0;
        const room = s.size - offset;
        const want = @min(@as(u64, buf.len), room);
        if (want == 0) return 0;
        return s.readAtFn(s.context, offset, buf[0..@intCast(want)]);
    }

    /// Fills `buf` completely or fails. Used for header bytes, where a
    /// short read means the volume ends mid-header.
    pub fn readAll(s: Source, offset: u64, buf: []u8) Error!void {
        var done: usize = 0;
        while (done < buf.len) {
            const n = try s.readAt(offset + done, buf[done..]);
            if (n == 0) return error.TruncatedVolume;
            done += n;
        }
    }
};

/// A volume that is already in memory. Used by the fixture-driven tests
/// and by any caller that has the bytes anyway.
pub const Memory = struct {
    bytes: []const u8,

    pub fn init(bytes: []const u8) Memory {
        return .{ .bytes = bytes };
    }

    pub fn source(self: *Memory) Source {
        return .{
            .size = self.bytes.len,
            .context = self,
            .readAtFn = readAt,
        };
    }

    fn readAt(context: *anyopaque, offset: u64, buf: []u8) ReadError!usize {
        const self: *Memory = @ptrCast(@alignCast(context));
        // `Source.readAt` already clamped to `size`, which is
        // `bytes.len`, so this cast cannot truncate.
        const start: usize = @intCast(offset);
        const n = @min(buf.len, self.bytes.len - start);
        @memcpy(buf[0..n], self.bytes[start..][0..n]);
        return n;
    }
};

/// A volume backed by a file on disk.
pub const File = struct {
    io: std.Io,
    file: std.Io.File,
    len: u64,
    /// The most recent underlying failure, kept for the caller's log
    /// because `ReadError` deliberately collapses to one tag.
    last_error: ?std.Io.File.ReadPositionalError = null,

    pub const OpenError = std.Io.File.OpenError || std.Io.File.StatError;

    pub fn open(io: std.Io, dir: std.Io.Dir, sub_path: []const u8) OpenError!File {
        const f = try dir.openFile(io, sub_path, .{ .mode = .read_only });
        errdefer f.close(io);
        return .{ .io = io, .file = f, .len = try f.length(io) };
    }

    pub fn close(self: *File) void {
        self.file.close(self.io);
        self.* = undefined;
    }

    pub fn source(self: *File) Source {
        return .{
            .size = self.len,
            .context = self,
            .readAtFn = readAt,
        };
    }

    fn readAt(context: *anyopaque, offset: u64, buf: []u8) ReadError!usize {
        const self: *File = @ptrCast(@alignCast(context));
        return self.file.readPositionalAll(self.io, buf, offset) catch |err| {
            self.last_error = err;
            return error.SourceReadFailed;
        };
    }
};

// ---------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------

const t = std.testing;

test "memory source clamps at the end of the volume" {
    var mem = Memory.init("0123456789");
    const src = mem.source();
    try t.expectEqual(@as(u64, 10), src.size);

    var buf: [4]u8 = undefined;
    try t.expectEqual(@as(usize, 4), try src.readAt(0, &buf));
    try t.expectEqualStrings("0123", &buf);
    try t.expectEqual(@as(usize, 4), try src.readAt(6, &buf));
    try t.expectEqualStrings("6789", &buf);

    // Straddling the end: clamped, not an error.
    try t.expectEqual(@as(usize, 2), try src.readAt(8, &buf));
    try t.expectEqualStrings("89", buf[0..2]);

    // Starting at or past the end: zero bytes, still not an error.
    try t.expectEqual(@as(usize, 0), try src.readAt(10, &buf));
    try t.expectEqual(@as(usize, 0), try src.readAt(1 << 40, &buf));
}

test "readAll turns a short read into TruncatedVolume" {
    var mem = Memory.init("abc");
    const src = mem.source();
    var buf: [3]u8 = undefined;
    try src.readAll(0, &buf);
    try t.expectEqualStrings("abc", &buf);
    try t.expectError(error.TruncatedVolume, src.readAll(1, &buf));
    try t.expectError(error.TruncatedVolume, src.readAll(3, buf[0..1]));
}

test "file source reads positionally" {
    var tmp = t.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(t.io, .{ .sub_path = "vol.rar", .data = "Rar!\x1a\x07\x01\x00payload" });

    var vol = try File.open(t.io, tmp.dir, "vol.rar");
    defer vol.close();
    const src = vol.source();
    try t.expectEqual(@as(u64, 15), src.size);

    var buf: [7]u8 = undefined;
    try src.readAll(8, &buf);
    try t.expectEqualStrings("payload", &buf);
    // Positional reads leave no cursor behind, so the same offset reads
    // the same bytes twice.
    try src.readAll(8, &buf);
    try t.expectEqualStrings("payload", &buf);
    try t.expectError(error.TruncatedVolume, src.readAll(9, &buf));
}
