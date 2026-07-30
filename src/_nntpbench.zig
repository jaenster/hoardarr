const std = @import("std");
const p = @import("nntp/protocol.zig");

fn run(comptime bulk: bool, wire: []const u8, dst: []u8) usize {
    var r: p.BodyReader = .{};
    var off: usize = 0;
    var total: usize = 0;
    while (off < wire.len and !r.terminated) {
        const s = if (bulk) r.push(wire[off..], dst) else r.pushByteAtATime(wire[off..], dst);
        off += s.consumed;
        total += s.written;
        if (s.consumed == 0 and s.written == 0) break;
    }
    return total;
}

pub fn main() !void {
    const gpa = std.heap.page_allocator;
    const size = 750 * 1024;
    const wire = try gpa.alloc(u8, size);
    var prng = std.Random.DefaultPrng.init(1);
    prng.random().bytes(wire);
    var k: usize = 128;
    while (k < wire.len - 3) : (k += 128) { wire[k-1] = '\r'; wire[k] = '\n'; }
    @memcpy(wire[wire.len-3..], ".\r\n");
    const dst = try gpa.alloc(u8, 32 * 1024);

    const iters = 300;
    inline for (.{ true, false }) |bulk| {
        var acc: usize = 0;
        var timer = try std.time.Timer.start();
        for (0..iters) |_| acc += run(bulk, wire, dst);
        const ns = timer.read();
        const mbps = @as(f64, @floatFromInt(size * iters)) / (@as(f64, @floatFromInt(ns)) / 1e9) / (1024*1024);
        std.debug.print("{s}: {d:.0} MB/s (acc={d})\n", .{ if (bulk) "vector/bulk " else "byte-at-time", mbps, acc });
    }
}
