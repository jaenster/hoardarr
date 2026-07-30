//! A rolling window of per-second byte counts.
//!
//! One hour of one-second buckets in a fixed ring — 3600 × 16 bytes,
//! ~57 KiB, allocated once and never resized. The ring index is
//! `unix_second % 3600` and each slot carries the second it belongs to,
//! so a slot whose stamp does not match the second being read is stale
//! and reads as zero. That is what makes expiry free: nothing sweeps,
//! nothing shifts, an idle hour simply leaves stale stamps behind.
//!
//! The clock is injected. Go's tracker called `time.Now()` internally and
//! its tests reached into the unexported `now` field to swap it, which
//! works only because the tests live in the same package. Here it is a
//! parameter.

const std = @import("std");
const app_ports = @import("../ports.zig");

pub const Timestamp = app_ports.Timestamp;

/// One-second buckets kept in memory. 3600 = one hour.
pub const window_size: usize = 3600;

/// Default width of `sample`, matching the historical response shape so
/// existing clients keep working as the ring widens.
pub const default_sample_seconds: usize = 300;

/// The short smoothing window for the human-facing rate.
pub const avg_window: usize = 10;

/// The longer baseline window, and the value the persisted speed history
/// stores per minute.
pub const long_avg_window: usize = 60;

/// A snapshot of the window.
pub const Sample = struct {
    /// Bytes per second, oldest first. Seconds with no activity are 0.
    /// Borrowed from the caller-supplied buffer.
    series: []const i64,
    /// Sum across `series`.
    total: i64 = 0,
    /// The most recent second's bucket. Jitters under concurrent fetches;
    /// `avg10s` is the one to put in front of a human.
    current_bytes_per_sec: i64 = 0,
    avg10s_bytes_per_sec: i64 = 0,
    avg60s_bytes_per_sec: i64 = 0,
    /// Highest bucket inside `series`.
    window_peak_bytes_per_sec: i64 = 0,
    window_seconds: usize = 0,
};

pub const Throughput = struct {
    buckets: [window_size]i64 = @splat(0),
    /// The unix second each slot currently holds. A mismatch means the
    /// slot is stale, which is how expiry costs nothing.
    stamps: [window_size]i64 = @splat(std.math.minInt(i64)),
    all_time_peak: i64 = 0,

    /// Records `n` bytes into the bucket for `now`. Non-positive counts
    /// are ignored, so a bogus read cannot appear as a spike.
    pub fn add(self: *Throughput, now: Timestamp, n: i64) void {
        if (n <= 0) return;
        const sec = @divFloor(now, std.time.ms_per_s);
        const idx = ringIndex(sec);
        if (self.stamps[idx] != sec) {
            self.buckets[idx] = 0;
            self.stamps[idx] = sec;
        }
        self.buckets[idx] += n;
        if (self.buckets[idx] > self.all_time_peak) self.all_time_peak = self.buckets[idx];
    }

    /// The highest single-second bucket seen since the process started,
    /// or since `setAllTimePeak` seeded a larger value.
    pub fn allTimePeak(self: *const Throughput) i64 {
        return self.all_time_peak;
    }

    /// Seeds the peak from persistence at startup. Negative seeds clamp
    /// to zero rather than making every later bucket look like a record.
    pub fn setAllTimePeak(self: *Throughput, v: i64) void {
        self.all_time_peak = @max(v, 0);
    }

    /// `default_sample_seconds` of history.
    pub fn sample(self: *const Throughput, buf: []i64, now: Timestamp) Sample {
        return self.sampleRange(buf, now, default_sample_seconds);
    }

    /// The last `n` seconds, clamped to `window_size` and to `buf.len`.
    ///
    /// The averages deliberately exclude the most recent second: it is
    /// still accruing, so including it biases the average downward and
    /// makes the displayed rate lurch every time the second ticks over.
    pub fn sampleRange(self: *const Throughput, buf: []i64, now: Timestamp, n_in: usize) Sample {
        var n = if (n_in == 0) default_sample_seconds else n_in;
        n = @min(n, window_size);
        n = @min(n, buf.len);
        const series = buf[0..n];
        @memset(series, 0);

        const now_sec = @divFloor(now, std.time.ms_per_s);
        var out: Sample = .{ .series = series, .window_seconds = n };
        var short_sum: i64 = 0;
        var long_sum: i64 = 0;

        for (0..n) |offset| {
            const sec = now_sec - @as(i64, @intCast(n - 1 - offset));
            const idx = ringIndex(sec);
            if (self.stamps[idx] != sec) continue;
            const v = self.buckets[idx];
            series[offset] = v;
            out.total += v;
            if (v > out.window_peak_bytes_per_sec) out.window_peak_bytes_per_sec = v;
            if (offset == n - 1) {
                out.current_bytes_per_sec = v;
                continue;
            }
            const secs_ago = n - 1 - offset;
            if (secs_ago <= avg_window) short_sum += v;
            if (secs_ago <= long_avg_window) long_sum += v;
        }
        out.avg10s_bytes_per_sec = @divTrunc(short_sum, @as(i64, avg_window));
        out.avg60s_bytes_per_sec = @divTrunc(long_sum, @as(i64, long_avg_window));
        return out;
    }

    fn ringIndex(sec: i64) usize {
        const m = @mod(sec, @as(i64, window_size));
        return @intCast(m);
    }
};

// ---------------------------------------------------------------------
// Tests — internal/app/system/throughput_test.go
// ---------------------------------------------------------------------

const testing = std.testing;

const t0: Timestamp = 1_700_000_000_000;

fn buf300() [default_sample_seconds]i64 {
    return @splat(0);
}

test "an empty window reads as zero everywhere" {
    var tp: Throughput = .{};
    var b = buf300();
    const s = tp.sample(&b, t0);
    try testing.expectEqual(default_sample_seconds, s.series.len);
    try testing.expectEqual(@as(i64, 0), s.total);
    try testing.expectEqual(@as(i64, 0), s.current_bytes_per_sec);
    try testing.expectEqual(@as(i64, 0), s.window_peak_bytes_per_sec);
    try testing.expectEqual(@as(i64, 0), s.avg10s_bytes_per_sec);
}

test "adds inside one second accumulate into one bucket" {
    var tp: Throughput = .{};
    tp.add(t0, 100);
    tp.add(t0 + 500, 50);
    var b = buf300();
    const s = tp.sample(&b, t0);
    try testing.expectEqual(@as(i64, 150), s.total);
    try testing.expectEqual(@as(i64, 150), s.current_bytes_per_sec);
    try testing.expectEqual(@as(i64, 150), s.window_peak_bytes_per_sec);
}

test "a new second opens a new bucket and both stay in the series" {
    var tp: Throughput = .{};
    tp.add(t0, 100);
    tp.add(t0 + 1000, 200);
    var b = buf300();
    const s = tp.sample(&b, t0 + 1000);

    try testing.expectEqual(@as(i64, 300), s.total);
    try testing.expectEqual(@as(i64, 200), s.current_bytes_per_sec);
    try testing.expectEqual(@as(i64, 100), s.series[s.series.len - 2]);
    try testing.expectEqual(@as(i64, 200), s.series[s.series.len - 1]);
    try testing.expectEqual(@as(i64, 200), s.window_peak_bytes_per_sec);
}

test "a bucket older than the ring cannot leak back in" {
    var tp: Throughput = .{};
    tp.add(t0, 999);
    // Jump two full rings forward: the slot is reused, and its stamp no
    // longer matches, so it reads as empty rather than as an hour-old
    // number.
    var b = buf300();
    const s = tp.sample(&b, t0 + @as(i64, window_size) * 2 * std.time.ms_per_s);
    try testing.expectEqual(@as(i64, 0), s.total);
}

test "zero and negative byte counts are ignored" {
    var tp: Throughput = .{};
    tp.add(t0, 0);
    tp.add(t0, -5);
    var b = buf300();
    try testing.expectEqual(@as(i64, 0), tp.sample(&b, t0).total);
    try testing.expectEqual(@as(i64, 0), tp.allTimePeak());
}

test "the all-time peak only ever goes up" {
    var tp: Throughput = .{};
    tp.add(t0, 500);
    try testing.expectEqual(@as(i64, 500), tp.allTimePeak());
    tp.add(t0 + 1000, 200);
    try testing.expectEqual(@as(i64, 500), tp.allTimePeak());
    tp.add(t0 + 2000, 700);
    try testing.expectEqual(@as(i64, 700), tp.allTimePeak());
}

test "a seeded peak survives smaller buckets and clamps negatives" {
    var tp: Throughput = .{};
    tp.setAllTimePeak(1234);
    try testing.expectEqual(@as(i64, 1234), tp.allTimePeak());
    tp.add(t0, 5);
    try testing.expectEqual(@as(i64, 1234), tp.allTimePeak());
    tp.setAllTimePeak(-1);
    try testing.expectEqual(@as(i64, 0), tp.allTimePeak());
}

test "sampleRange honours n and clamps to the ring" {
    var tp: Throughput = .{};
    tp.add(t0, 42);
    var big: [window_size]i64 = @splat(0);
    for ([_]usize{ 10, 60, 300, 1800, 3600 }) |n| {
        const s = tp.sampleRange(&big, t0, n);
        try testing.expectEqual(n, s.series.len);
        try testing.expectEqual(n, s.window_seconds);
    }
    const clamped = tp.sampleRange(&big, t0, window_size * 4);
    try testing.expectEqual(window_size, clamped.series.len);
    // Zero means "the default width".
    const zero = tp.sampleRange(&big, t0, 0);
    try testing.expectEqual(default_sample_seconds, zero.series.len);
}

test "the current second is excluded from the averages" {
    var tp: Throughput = .{};
    // Ten completed seconds of 100 bytes, then a partial second.
    for (0..10) |i| tp.add(t0 + @as(i64, @intCast(i)) * 1000, 100);
    const now = t0 + 10_000;
    tp.add(now, 5);

    var b = buf300();
    const s = tp.sample(&b, now);
    // 10 × 100 over the 10-second window; the partial 5 is not folded in,
    // which is what stops the displayed rate lurching each second.
    try testing.expectEqual(@as(i64, 100), s.avg10s_bytes_per_sec);
    try testing.expectEqual(@as(i64, 5), s.current_bytes_per_sec);
    try testing.expectEqual(@as(i64, 1005), s.total);
}

test "the averages divide by the window, not by the samples present" {
    // A single busy second inside a quiet minute must not read as a
    // full-rate minute.
    var tp: Throughput = .{};
    tp.add(t0, 600);
    var b = buf300();
    const s = tp.sample(&b, t0 + 5_000);
    try testing.expectEqual(@as(i64, 60), s.avg10s_bytes_per_sec);
    try testing.expectEqual(@as(i64, 10), s.avg60s_bytes_per_sec);
}

test "a caller buffer smaller than n bounds the series" {
    var tp: Throughput = .{};
    tp.add(t0, 7);
    var small: [4]i64 = @splat(0);
    const s = tp.sampleRange(&small, t0, 300);
    try testing.expectEqual(@as(usize, 4), s.series.len);
    try testing.expectEqual(@as(i64, 7), s.current_bytes_per_sec);
}
