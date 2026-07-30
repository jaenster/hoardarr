//! Port of `internal/bootstrap/e2e_speed_test.go` —
//! `TestE2E_SpeedHistoryAndPeaks`.
//!
//! The download-speed observability surface, driven the way Go drives
//! it: bytes are injected straight into the live throughput tracker —
//! the same object the byte accounter feeds in production — and then
//! read back out through HTTP. No NZB pipeline is involved, which is
//! what makes this portable today.

const std = @import("std");
const testing = std.testing;

const h = @import("harness.zig");
const infra = @import("../bootstrap/infra.zig");

test "speed: throughput, speed history and the bandwidth cap round-trip" {
    const gpa = testing.allocator;
    var fx = try h.Harness.init(gpa, "speed", .{});
    defer fx.deinit();

    // 5 MiB into the current second's bucket, through the same tracker
    // the orchestrator's accounter writes to.
    const now = infra.nowMillis();
    fx.app.throughput.add(now, 5 * 1024 * 1024);

    // Every field the UI's speed graph reads. A missing one renders as
    // a blank chart rather than an error, which is why the shape is
    // asserted field by field rather than by "it answered 200".
    {
        var r = try fx.get("/api/v1/system/throughput");
        defer r.deinit();
        for ([_][]const u8{
            "window_seconds",
            "series",
            "total_bytes",
            "current_bytes_per_sec",
            "avg10s_bytes_per_sec",
            "avg60s_bytes_per_sec",
            "peak_window_bytes_per_sec",
            "peak_alltime_bytes_per_sec",
            "global_cap_bytes_per_sec",
        }) |k| {
            if (r.field(k) == null) {
                std.debug.print("\nthroughput response missing '{s}': {s}\n", .{ k, r.body });
                return error.ThroughputFieldMissing;
            }
        }

        const peak = try std.fmt.parseInt(i64, r.field("peak_alltime_bytes_per_sec").?, 10);
        if (peak < 5 * 1024 * 1024) {
            std.debug.print("\npeak_alltime = {d}; want >= 5 MiB\n", .{peak});
            return error.PeakNotRecorded;
        }
        // The default 5-minute view. A change here changes what the
        // graph means, so it is pinned.
        try r.expectField("window_seconds", "300");
    }

    // range=5m is served from the in-memory ring at 1s resolution.
    {
        var r = try fx.get("/api/v1/system/speed-history?range=5m");
        defer r.deinit();
        try r.expectField("range", "5m");
        try r.expectField("resolution_seconds", "1");
        if (r.field("samples") == null) return error.NoSamples;
        // At least one bucket has to carry the bytes just injected;
        // an all-zero series would mean the read path is not looking at
        // the same tracker the write path fed.
        var saw_nonzero = false;
        var it = std.mem.splitSequence(u8, r.body, "\"bytes_per_sec\":");
        _ = it.first();
        while (it.next()) |chunk| {
            const end = std.mem.indexOfNone(u8, chunk, "0123456789-") orelse chunk.len;
            const v = std.fmt.parseInt(i64, chunk[0..end], 10) catch continue;
            if (v > 0) {
                saw_nonzero = true;
                break;
            }
        }
        if (!saw_nonzero) {
            std.debug.print("\nno non-zero sample in the 5m history: {s}\n", .{r.body});
            return error.NoNonZeroSample;
        }
    }

    // range=24h falls back to the persisted minute buckets, which are
    // empty here because the flusher has not ticked. It still has to
    // answer cleanly rather than 500 on an empty table.
    {
        var r = try fx.get("/api/v1/system/speed-history?range=24h");
        defer r.deinit();
        try r.expectField("range", "24h");
        try r.expectField("resolution_seconds", "60");
        if (r.field("samples") == null) return error.NoSamples;
    }

    // An unrecognised range is refused rather than silently defaulted:
    // a graph quietly showing the wrong window is worse than an error.
    {
        var r = try fx.request(.{ .path = "/api/v1/system/speed-history?range=bogus" });
        defer r.deinit();
        try h.expectStatus(&r, 400, "speed-history?range=bogus");
    }

    // Setting the cap must surface back through the throughput read —
    // that is the field the UI shows next to the current speed.
    {
        var r = try fx.request(.{
            .method = .put,
            .path = "/api/v1/config/bandwidth",
            .body = "{\"global_bytes_per_sec\": 8388608}",
        });
        defer r.deinit();
        try h.expectStatus(&r, 200, "PUT /config/bandwidth");
    }
    {
        var r = try fx.get("/api/v1/system/throughput");
        defer r.deinit();
        try r.expectField("global_cap_bytes_per_sec", "8388608");
    }

    // And it is a setting, not process state: it has to be there after
    // a restart, or an operator's cap silently lifts on every update.
    try fx.restart(.{});
    {
        var r = try fx.get("/api/v1/system/throughput");
        defer r.deinit();
        try r.expectField("global_cap_bytes_per_sec", "8388608");
    }
}
