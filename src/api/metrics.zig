//! Prometheus instrumentation, hand-rolled.
//!
//! The text exposition format is a dozen lines of printing, so a client
//! library would be a dependency bought for nothing. What *is* worth
//! being careful about is the contract: metric names, label names and
//! bucket boundaries are somebody's dashboard and somebody's alert rule.
//! Renaming `hoardarr_nntp_articles_fetched_total` does not break a test,
//! it breaks a pager at 3 a.m. Every name here is byte-identical to the
//! Go collectors in `internal/metrics`.
//!
//! ## Shape
//!
//! Nine families, fixed at compile time, each with a bounded series
//! table. Label *names* are compile-time constants; label *values* are
//! runtime strings, owned by the family. Cardinality is capped per
//! family — the label sets are deliberately low-cardinality (server
//! name, a small outcome enum, a subscription name) but a cap is what
//! makes that a guarantee rather than an intention. Nothing here is
//! keyed by job id or message id.
//!
//! ## Not ported
//!
//! The Go build registered `collectors.NewGoCollector()` and
//! `NewProcessCollector()`, which is where `go_goroutines`,
//! `go_memstats_*` and `process_resident_memory_bytes` came from. There
//! is no Go runtime to report on, and inventing a partial `process_*`
//! subset from `/proc` would be a new adapter pretending to be a port.
//! The gap is deliberate and documented rather than papered over.
//!
//! ## Threading
//!
//! Instrumentation points are on the reactor thread and, once the NNTP
//! pool has worker threads, off it. One mutex per registry guards every
//! family; contention is irrelevant because the operations are a hash
//! lookup and an add. `std.Thread.Mutex` does not exist in this tree, so
//! this is `core/log.zig`'s futex mutex.

const std = @import("std");
const log = @import("../core/log.zig");

const Allocator = std.mem.Allocator;

/// `Content-Type` for the text exposition format. The version parameter
/// is part of the contract: scrapers negotiate on it.
pub const content_type = "text/plain; version=0.0.4; charset=utf-8";

/// Widest label set in use is `{server, state}` / `{server, outcome}`
/// plus build info's three.
pub const max_labels = 3;

/// Histogram buckets, excluding the implicit `+Inf`.
pub const max_buckets = 16;

/// Distinct label-value combinations one family will hold. Servers and
/// subscriptions are operator-created and rarely reach double digits;
/// 64 leaves room for a busy install and still bounds the memory a
/// misuse could cost.
pub const max_series_per_family = 64;

pub const Kind = enum {
    counter,
    gauge,
    histogram,

    fn text(self: Kind) []const u8 {
        return @tagName(self);
    }
};

/// Segment-fetch duration buckets, in seconds. Same ten boundaries the
/// Go `HistogramOpts` declared — a changed boundary silently invalidates
/// every stored quantile.
pub const segment_fetch_buckets = [_]f64{ 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60 };

// ---------------------------------------------------------------------
// Families
// ---------------------------------------------------------------------

const Series = struct {
    /// Owned copies: a caller's label value may be a slice into a
    /// connection buffer that is gone by the next scrape.
    labels: [max_labels][]const u8 = @splat(""),
    n_labels: usize = 0,

    /// Counter or gauge value.
    value: f64 = 0,

    /// Histogram state. `buckets[i]` counts observations `<= bounds[i]`
    /// and is *not* cumulative in storage; the cumulative sum is done at
    /// render time, which keeps `observe` a single increment.
    buckets: [max_buckets]u64 = @splat(0),
    sum: f64 = 0,
    count: u64 = 0,

    fn matches(self: *const Series, labels: []const []const u8) bool {
        if (self.n_labels != labels.len) return false;
        for (labels, 0..) |v, i| {
            if (!std.mem.eql(u8, self.labels[i], v)) return false;
        }
        return true;
    }
};

pub const Family = struct {
    name: []const u8,
    help: []const u8,
    kind: Kind,
    label_names: []const []const u8,
    bounds: []const f64 = &.{},

    series: std.ArrayList(Series) = .empty,
    /// Label sets refused because the table was full. Not exported as a
    /// metric — that would be a cardinality problem reporting itself
    /// through the same mechanism — but visible to the log and to tests.
    dropped: u64 = 0,

    fn deinit(self: *Family, gpa: Allocator) void {
        for (self.series.items) |*s| {
            for (s.labels[0..s.n_labels]) |v| gpa.free(v);
        }
        self.series.deinit(gpa);
    }

    /// Find or create the series for `labels`. Null when the table is
    /// full or the label count is wrong — both of which are programming
    /// errors at the call site, and neither of which is worth failing a
    /// download over, so they are counted and dropped.
    fn seriesFor(self: *Family, gpa: Allocator, labels: []const []const u8) ?*Series {
        std.debug.assert(labels.len == self.label_names.len);
        for (self.series.items) |*s| {
            if (s.matches(labels)) return s;
        }
        if (self.series.items.len >= max_series_per_family) {
            self.dropped += 1;
            return null;
        }
        var fresh: Series = .{ .n_labels = labels.len };
        for (labels, 0..) |v, i| {
            fresh.labels[i] = gpa.dupe(u8, v) catch {
                // Unwind the copies made so far: a half-initialised
                // series would leak and would compare wrong.
                for (fresh.labels[0..i]) |done| gpa.free(done);
                self.dropped += 1;
                return null;
            };
        }
        self.series.append(gpa, fresh) catch {
            for (fresh.labels[0..labels.len]) |v| gpa.free(v);
            self.dropped += 1;
            return null;
        };
        return &self.series.items[self.series.items.len - 1];
    }

    /// Value of one series, or null when it has never been touched. For
    /// tests and for the health checks that read their own metrics back.
    pub fn get(self: *const Family, labels: []const []const u8) ?f64 {
        for (self.series.items) |*s| {
            if (s.matches(labels)) return s.value;
        }
        return null;
    }

    pub fn seriesCount(self: *const Family) usize {
        return self.series.items.len;
    }
};

// ---------------------------------------------------------------------
// Registry
// ---------------------------------------------------------------------

pub const Registry = struct {
    gpa: Allocator,
    mu: log.Mutex = .{},

    build_info: Family = .{
        .name = "hoardarr_build_info",
        .help = "Build metadata of the running binary (always 1). Labels carry version + commit + go_version.",
        .kind = .gauge,
        .label_names = &.{ "version", "commit", "go_version" },
    },
    jobs: Family = .{
        .name = "hoardarr_jobs",
        .help = "Number of jobs currently in each download lifecycle state.",
        .kind = .gauge,
        .label_names = &.{"state"},
    },
    articles_fetched: Family = .{
        .name = "hoardarr_nntp_articles_fetched_total",
        .help = "NNTP article fetch attempts, by server and outcome (ok / missing / failed).",
        .kind = .counter,
        .label_names = &.{ "server", "outcome" },
    },
    bytes_downloaded: Family = .{
        .name = "hoardarr_nntp_bytes_downloaded_total",
        .help = "Decoded yEnc payload bytes written, by server.",
        .kind = .counter,
        .label_names = &.{"server"},
    },
    segment_fetch_seconds: Family = .{
        .name = "hoardarr_nntp_segment_fetch_seconds",
        .help = "End-to-end segment fetch duration (NNTP dial + ARTICLE + yEnc decode + disk write).",
        .kind = .histogram,
        .label_names = &.{ "server", "outcome" },
        .bounds = &segment_fetch_buckets,
    },
    nntp_connections: Family = .{
        .name = "hoardarr_nntp_connections",
        .help = "Live NNTP connection count per server, split by state (in_use, idle).",
        .kind = .gauge,
        .label_names = &.{ "server", "state" },
    },
    outbox_pending: Family = .{
        .name = "hoardarr_outbox_pending",
        .help = "Pending outbox rows per subscription (events not yet delivered).",
        .kind = .gauge,
        .label_names = &.{"subscription"},
    },
    outbox_dispatched: Family = .{
        .name = "hoardarr_outbox_dispatched_total",
        .help = "Successfully-delivered outbox events per subscription.",
        .kind = .counter,
        .label_names = &.{"subscription"},
    },
    outbox_failed: Family = .{
        .name = "hoardarr_outbox_dispatch_failed_total",
        .help = "Failed outbox deliveries per subscription (includes retries; final-park is logged separately).",
        .kind = .counter,
        .label_names = &.{"subscription"},
    },

    pub fn init(gpa: Allocator) Registry {
        return .{ .gpa = gpa };
    }

    pub fn deinit(self: *Registry) void {
        for (self.families()) |f| f.deinit(self.gpa);
        self.* = undefined;
    }

    /// Every family, in exposition order. Order is stable so a scrape
    /// diff between two versions is readable.
    pub fn families(self: *Registry) [9]*Family {
        return .{
            &self.build_info,
            &self.jobs,
            &self.articles_fetched,
            &self.bytes_downloaded,
            &self.segment_fetch_seconds,
            &self.nntp_connections,
            &self.outbox_pending,
            &self.outbox_dispatched,
            &self.outbox_failed,
        };
    }

    // -- mutation -----------------------------------------------------

    /// Counters only ever go up; a negative delta is a bug at the call
    /// site and is dropped rather than allowed to corrupt a rate().
    pub fn inc(self: *Registry, f: *Family, labels: []const []const u8, delta: f64) void {
        std.debug.assert(f.kind == .counter);
        if (!(delta >= 0)) return; // also rejects NaN
        self.mu.lock();
        defer self.mu.unlock();
        const s = f.seriesFor(self.gpa, labels) orelse return;
        s.value += delta;
    }

    pub fn set(self: *Registry, f: *Family, labels: []const []const u8, v: f64) void {
        std.debug.assert(f.kind == .gauge);
        self.mu.lock();
        defer self.mu.unlock();
        const s = f.seriesFor(self.gpa, labels) orelse return;
        s.value = v;
    }

    pub fn add(self: *Registry, f: *Family, labels: []const []const u8, delta: f64) void {
        std.debug.assert(f.kind == .gauge);
        self.mu.lock();
        defer self.mu.unlock();
        const s = f.seriesFor(self.gpa, labels) orelse return;
        s.value += delta;
    }

    pub fn observe(self: *Registry, f: *Family, labels: []const []const u8, v: f64) void {
        std.debug.assert(f.kind == .histogram);
        if (std.math.isNan(v)) return;
        self.mu.lock();
        defer self.mu.unlock();
        const s = f.seriesFor(self.gpa, labels) orelse return;
        s.count += 1;
        s.sum += v;
        for (f.bounds, 0..) |bound, i| {
            if (v <= bound) {
                s.buckets[i] += 1;
                // Storage is per-bucket, not cumulative: the render pass
                // accumulates. One increment per observation.
                break;
            }
        }
    }

    /// Drop every series of a gauge family before republishing the whole
    /// set. Without this, a state that no longer has any jobs — or a
    /// server that was deleted — keeps reporting its last value forever.
    pub fn resetFamily(self: *Registry, f: *Family) void {
        self.mu.lock();
        defer self.mu.unlock();
        for (f.series.items) |*s| {
            for (s.labels[0..s.n_labels]) |v| self.gpa.free(v);
        }
        f.series.clearRetainingCapacity();
    }

    // -- exposition ---------------------------------------------------

    /// Append the whole registry in Prometheus text format.
    pub fn write(self: *Registry, out: *std.ArrayList(u8), gpa: Allocator) Allocator.Error!void {
        self.mu.lock();
        defer self.mu.unlock();
        for (self.families()) |f| try writeFamily(out, gpa, f);
    }

    /// The same thing as an owned slice, for the `/metrics` handler.
    pub fn render(self: *Registry, gpa: Allocator) Allocator.Error![]u8 {
        var out: std.ArrayList(u8) = .empty;
        errdefer out.deinit(gpa);
        try self.write(&out, gpa);
        return out.toOwnedSlice(gpa);
    }
};

fn writeFamily(out: *std.ArrayList(u8), gpa: Allocator, f: *const Family) Allocator.Error!void {
    // A family with no series still gets its metadata: a scraper that
    // sees HELP/TYPE and no samples knows the metric exists and is
    // simply idle, which is what the Go client did for a registered
    // collector with no children.
    try out.appendSlice(gpa, "# HELP ");
    try out.appendSlice(gpa, f.name);
    try out.append(gpa, ' ');
    try writeHelp(out, gpa, f.help);
    try out.append(gpa, '\n');
    try out.appendSlice(gpa, "# TYPE ");
    try out.appendSlice(gpa, f.name);
    try out.append(gpa, ' ');
    try out.appendSlice(gpa, f.kind.text());
    try out.append(gpa, '\n');

    for (f.series.items) |*s| {
        switch (f.kind) {
            .counter, .gauge => {
                try out.appendSlice(gpa, f.name);
                try writeLabels(out, gpa, f.label_names, s, null);
                try out.append(gpa, ' ');
                try writeValue(out, gpa, s.value);
                try out.append(gpa, '\n');
            },
            .histogram => {
                var cumulative: u64 = 0;
                for (f.bounds, 0..) |bound, i| {
                    cumulative += s.buckets[i];
                    try out.appendSlice(gpa, f.name);
                    try out.appendSlice(gpa, "_bucket");
                    var le: [32]u8 = undefined;
                    try writeLabels(out, gpa, f.label_names, s, std.fmt.bufPrint(&le, "{d}", .{bound}) catch "0");
                    try out.append(gpa, ' ');
                    try writeUint(out, gpa, cumulative);
                    try out.append(gpa, '\n');
                }
                // The `+Inf` bucket is mandatory and equals the total
                // count; without it a histogram is not a histogram.
                try out.appendSlice(gpa, f.name);
                try out.appendSlice(gpa, "_bucket");
                try writeLabels(out, gpa, f.label_names, s, "+Inf");
                try out.append(gpa, ' ');
                try writeUint(out, gpa, s.count);
                try out.append(gpa, '\n');

                try out.appendSlice(gpa, f.name);
                try out.appendSlice(gpa, "_sum");
                try writeLabels(out, gpa, f.label_names, s, null);
                try out.append(gpa, ' ');
                try writeValue(out, gpa, s.sum);
                try out.append(gpa, '\n');

                try out.appendSlice(gpa, f.name);
                try out.appendSlice(gpa, "_count");
                try writeLabels(out, gpa, f.label_names, s, null);
                try out.append(gpa, ' ');
                try writeUint(out, gpa, s.count);
                try out.append(gpa, '\n');
            },
        }
    }
}

fn writeLabels(
    out: *std.ArrayList(u8),
    gpa: Allocator,
    names: []const []const u8,
    s: *const Series,
    le: ?[]const u8,
) Allocator.Error!void {
    if (names.len == 0 and le == null) return;
    try out.append(gpa, '{');
    for (names, 0..) |name, i| {
        if (i > 0) try out.append(gpa, ',');
        try out.appendSlice(gpa, name);
        try out.appendSlice(gpa, "=\"");
        try writeLabelValue(out, gpa, s.labels[i]);
        try out.append(gpa, '"');
    }
    if (le) |v| {
        if (names.len > 0) try out.append(gpa, ',');
        try out.appendSlice(gpa, "le=\"");
        try out.appendSlice(gpa, v); // bucket bounds are our own numbers
        try out.append(gpa, '"');
    }
    try out.append(gpa, '}');
}

/// Label values are attacker-adjacent: a server "name" is whatever the
/// operator typed, and an outcome string could one day come from a
/// response line. The format's escaping rule is exactly three
/// characters, and getting it wrong lets a value close its own quote and
/// forge a label — or a whole extra sample.
fn writeLabelValue(out: *std.ArrayList(u8), gpa: Allocator, v: []const u8) Allocator.Error!void {
    for (v) |c| switch (c) {
        '\\' => try out.appendSlice(gpa, "\\\\"),
        '"' => try out.appendSlice(gpa, "\\\""),
        '\n' => try out.appendSlice(gpa, "\\n"),
        // A bare CR is not in the escape set and would end the sample
        // line, so it is dropped rather than emitted.
        '\r' => {},
        else => try out.append(gpa, c),
    };
}

/// HELP runs to the end of the line, so only a backslash and a newline
/// need escaping. Help strings are compile-time constants here; this is
/// belt-and-braces so that stays true if one ever becomes dynamic.
fn writeHelp(out: *std.ArrayList(u8), gpa: Allocator, v: []const u8) Allocator.Error!void {
    for (v) |c| switch (c) {
        '\\' => try out.appendSlice(gpa, "\\\\"),
        '\n' => try out.appendSlice(gpa, "\\n"),
        '\r' => {},
        else => try out.append(gpa, c),
    };
}

fn writeUint(out: *std.ArrayList(u8), gpa: Allocator, v: u64) Allocator.Error!void {
    var buf: [24]u8 = undefined;
    try out.appendSlice(gpa, std.fmt.bufPrint(&buf, "{d}", .{v}) catch unreachable);
}

/// Sample values. Prometheus accepts Go's float syntax plus `NaN`,
/// `+Inf` and `-Inf`, and an integral float must not print as `3.0e0` —
/// scrapers cope, but every dashboard and every test becomes harder to
/// read.
fn writeValue(out: *std.ArrayList(u8), gpa: Allocator, v: f64) Allocator.Error!void {
    if (std.math.isNan(v)) return out.appendSlice(gpa, "NaN");
    if (std.math.isPositiveInf(v)) return out.appendSlice(gpa, "+Inf");
    if (std.math.isNegativeInf(v)) return out.appendSlice(gpa, "-Inf");
    var buf: [64]u8 = undefined;
    const s = std.fmt.bufPrint(&buf, "{d}", .{v}) catch return out.appendSlice(gpa, "0");
    try out.appendSlice(gpa, s);
}

// ---------------------------------------------------------------------
// Process-global instrumentation
// ---------------------------------------------------------------------

/// The registry instrumentation points write to. Null until the
/// bootstrap installs one, and every helper below is a no-op while it
/// is: metrics must never be the reason a code path cannot run, and a
/// unit test of the NNTP pool should not have to stand one up.
///
/// This is the one piece of global mutable state in the API layer, and
/// it is deliberate for the same reason the Go build had it: threading a
/// `*Registry` through every adapter constructor is ceremony bought for
/// nothing, because there is exactly one per process and its lifetime is
/// the process's.
pub var installed: ?*Registry = null;

pub fn install(r: *Registry) void {
    installed = r;
}

pub fn uninstall() void {
    installed = null;
}

/// The singular `{version, commit, go_version} = 1` row.
///
/// `go_version` keeps its name even though what it now carries is the
/// Zig version: it is a label key in existing dashboards and recording
/// rules, and renaming it to be tidy would break them for no operator
/// benefit.
pub fn setBuildInfo(version: []const u8, commit: []const u8, toolchain: []const u8) void {
    const r = installed orelse return;
    r.set(&r.build_info, &.{ version, commit, toolchain }, 1);
}

pub fn setJobsInState(state: []const u8, n: u64) void {
    const r = installed orelse return;
    r.set(&r.jobs, &.{state}, @floatFromInt(n));
}

pub fn resetJobs() void {
    const r = installed orelse return;
    r.resetFamily(&r.jobs);
}

pub const FetchOutcome = enum {
    ok,
    missing,
    failed,

    pub fn text(self: FetchOutcome) []const u8 {
        return @tagName(self);
    }
};

pub fn incArticlesFetched(server: []const u8, outcome: FetchOutcome) void {
    const r = installed orelse return;
    r.inc(&r.articles_fetched, &.{ server, outcome.text() }, 1);
}

pub fn addBytesDownloaded(server: []const u8, bytes: u64) void {
    const r = installed orelse return;
    r.inc(&r.bytes_downloaded, &.{server}, @floatFromInt(bytes));
}

/// Duration is taken in nanoseconds (what `sys.monotonicNanos` deals in)
/// and observed in seconds, which is the unit the metric name promises.
pub fn observeSegmentFetch(server: []const u8, outcome: FetchOutcome, nanos: u64) void {
    const r = installed orelse return;
    const seconds = @as(f64, @floatFromInt(nanos)) / @as(f64, std.time.ns_per_s);
    r.observe(&r.segment_fetch_seconds, &.{ server, outcome.text() }, seconds);
}

pub fn setNntpConnections(server: []const u8, in_use: u64, idle: u64) void {
    const r = installed orelse return;
    r.set(&r.nntp_connections, &.{ server, "in_use" }, @floatFromInt(in_use));
    r.set(&r.nntp_connections, &.{ server, "idle" }, @floatFromInt(idle));
}

pub fn setOutboxPending(subscription: []const u8, n: u64) void {
    const r = installed orelse return;
    r.set(&r.outbox_pending, &.{subscription}, @floatFromInt(n));
}

pub fn incOutboxDispatched(subscription: []const u8) void {
    const r = installed orelse return;
    r.inc(&r.outbox_dispatched, &.{subscription}, 1);
}

pub fn incOutboxFailed(subscription: []const u8) void {
    const r = installed orelse return;
    r.inc(&r.outbox_failed, &.{subscription}, 1);
}

// ---------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------

const testing = std.testing;

fn render(r: *Registry) ![]u8 {
    return r.render(testing.allocator);
}

/// Does the output contain this exact sample line?
fn hasLine(doc: []const u8, line: []const u8) bool {
    if (std.mem.startsWith(u8, doc, line) and doc.len > line.len and doc[line.len] == '\n') return true;
    var it = std.mem.splitScalar(u8, doc, '\n');
    while (it.next()) |l| {
        if (std.mem.eql(u8, l, line)) return true;
    }
    return false;
}

test "the metric names, types and help strings are the Go ones" {
    var r = Registry.init(testing.allocator);
    defer r.deinit();

    const doc = try render(&r);
    defer testing.allocator.free(doc);

    // A renamed metric is a broken dashboard, so the names are asserted
    // literally rather than derived from the struct.
    const expected = [_]struct { name: []const u8, kind: []const u8 }{
        .{ .name = "hoardarr_build_info", .kind = "gauge" },
        .{ .name = "hoardarr_jobs", .kind = "gauge" },
        .{ .name = "hoardarr_nntp_articles_fetched_total", .kind = "counter" },
        .{ .name = "hoardarr_nntp_bytes_downloaded_total", .kind = "counter" },
        .{ .name = "hoardarr_nntp_segment_fetch_seconds", .kind = "histogram" },
        .{ .name = "hoardarr_nntp_connections", .kind = "gauge" },
        .{ .name = "hoardarr_outbox_pending", .kind = "gauge" },
        .{ .name = "hoardarr_outbox_dispatched_total", .kind = "counter" },
        .{ .name = "hoardarr_outbox_dispatch_failed_total", .kind = "counter" },
    };
    for (expected) |e| {
        var buf: [128]u8 = undefined;
        const type_line = try std.fmt.bufPrint(&buf, "# TYPE {s} {s}", .{ e.name, e.kind });
        try testing.expect(hasLine(doc, type_line));
        var buf2: [64]u8 = undefined;
        try testing.expect(std.mem.indexOf(
            u8,
            doc,
            try std.fmt.bufPrint(&buf2, "# HELP {s} ", .{e.name}),
        ) != null);
    }
    try testing.expectEqual(@as(usize, 9), std.mem.count(u8, doc, "# TYPE "));
    try testing.expectEqual(@as(usize, 9), std.mem.count(u8, doc, "# HELP "));

    // And the label sets.
    try testing.expectEqualSlices([]const u8, &.{ "version", "commit", "go_version" }, r.build_info.label_names);
    try testing.expectEqualSlices([]const u8, &.{"state"}, r.jobs.label_names);
    try testing.expectEqualSlices([]const u8, &.{ "server", "outcome" }, r.articles_fetched.label_names);
    try testing.expectEqualSlices([]const u8, &.{"server"}, r.bytes_downloaded.label_names);
    try testing.expectEqualSlices([]const u8, &.{ "server", "outcome" }, r.segment_fetch_seconds.label_names);
    try testing.expectEqualSlices([]const u8, &.{ "server", "state" }, r.nntp_connections.label_names);
    try testing.expectEqualSlices([]const u8, &.{"subscription"}, r.outbox_pending.label_names);
    try testing.expectEqualSlices([]const u8, &.{"subscription"}, r.outbox_dispatched.label_names);
    try testing.expectEqualSlices([]const u8, &.{"subscription"}, r.outbox_failed.label_names);
}

test "a counter accumulates per label set" {
    var r = Registry.init(testing.allocator);
    defer r.deinit();

    r.inc(&r.articles_fetched, &.{ "news.eweka.nl", "ok" }, 1);
    r.inc(&r.articles_fetched, &.{ "news.eweka.nl", "ok" }, 1);
    r.inc(&r.articles_fetched, &.{ "news.eweka.nl", "missing" }, 1);
    r.inc(&r.articles_fetched, &.{ "block.example", "ok" }, 5);

    const doc = try render(&r);
    defer testing.allocator.free(doc);

    try testing.expect(hasLine(doc, "hoardarr_nntp_articles_fetched_total{server=\"news.eweka.nl\",outcome=\"ok\"} 2"));
    try testing.expect(hasLine(doc, "hoardarr_nntp_articles_fetched_total{server=\"news.eweka.nl\",outcome=\"missing\"} 1"));
    try testing.expect(hasLine(doc, "hoardarr_nntp_articles_fetched_total{server=\"block.example\",outcome=\"ok\"} 5"));
    try testing.expectEqual(@as(usize, 3), r.articles_fetched.seriesCount());
}

test "a counter refuses to go backwards" {
    var r = Registry.init(testing.allocator);
    defer r.deinit();

    r.inc(&r.bytes_downloaded, &.{"a"}, 100);
    r.inc(&r.bytes_downloaded, &.{"a"}, -50);
    r.inc(&r.bytes_downloaded, &.{"a"}, std.math.nan(f64));
    // A decreasing counter makes rate() produce a spike the size of the
    // whole counter, so the bad delta is dropped, not applied.
    try testing.expectEqual(@as(f64, 100), r.bytes_downloaded.get(&.{"a"}).?);
}

test "a gauge is set, added to, and resettable" {
    var r = Registry.init(testing.allocator);
    defer r.deinit();

    r.set(&r.jobs, &.{"queued"}, 3);
    r.set(&r.jobs, &.{"downloading"}, 1);
    r.set(&r.jobs, &.{"queued"}, 2);
    r.add(&r.jobs, &.{"queued"}, -1);
    try testing.expectEqual(@as(f64, 1), r.jobs.get(&.{"queued"}).?);

    const doc = try render(&r);
    defer testing.allocator.free(doc);
    try testing.expect(hasLine(doc, "hoardarr_jobs{state=\"queued\"} 1"));
    try testing.expect(hasLine(doc, "hoardarr_jobs{state=\"downloading\"} 1"));

    // Republish: a state that emptied must stop reporting, not keep its
    // last value until restart.
    r.resetFamily(&r.jobs);
    try testing.expectEqual(@as(usize, 0), r.jobs.seriesCount());
    r.set(&r.jobs, &.{"completed"}, 7);
    const doc2 = try render(&r);
    defer testing.allocator.free(doc2);
    try testing.expect(std.mem.indexOf(u8, doc2, "state=\"queued\"") == null);
    try testing.expect(hasLine(doc2, "hoardarr_jobs{state=\"completed\"} 7"));
}

test "histogram buckets are cumulative and carry sum, count and +Inf" {
    var r = Registry.init(testing.allocator);
    defer r.deinit();

    // 0.02 → first bucket; 0.3 → the 0.5 bucket; 90 → beyond the last
    // finite bound, so it only shows up in +Inf.
    r.observe(&r.segment_fetch_seconds, &.{ "s1", "ok" }, 0.02);
    r.observe(&r.segment_fetch_seconds, &.{ "s1", "ok" }, 0.3);
    r.observe(&r.segment_fetch_seconds, &.{ "s1", "ok" }, 90);

    const doc = try render(&r);
    defer testing.allocator.free(doc);
    const n = "hoardarr_nntp_segment_fetch_seconds";
    const l = "{server=\"s1\",outcome=\"ok\",le=";

    try testing.expect(hasLine(doc, n ++ "_bucket" ++ l ++ "\"0.05\"} 1"));
    try testing.expect(hasLine(doc, n ++ "_bucket" ++ l ++ "\"0.1\"} 1"));
    try testing.expect(hasLine(doc, n ++ "_bucket" ++ l ++ "\"0.25\"} 1"));
    try testing.expect(hasLine(doc, n ++ "_bucket" ++ l ++ "\"0.5\"} 2"));
    try testing.expect(hasLine(doc, n ++ "_bucket" ++ l ++ "\"60\"} 2"));
    try testing.expect(hasLine(doc, n ++ "_bucket" ++ l ++ "\"+Inf\"} 3"));
    try testing.expect(hasLine(doc, n ++ "_sum{server=\"s1\",outcome=\"ok\"} 90.32"));
    try testing.expect(hasLine(doc, n ++ "_count{server=\"s1\",outcome=\"ok\"} 3"));

    // Eleven bucket lines per series: ten bounds plus +Inf.
    try testing.expectEqual(@as(usize, 11), std.mem.count(u8, doc, n ++ "_bucket"));
}

test "the histogram bounds are the Go ones" {
    try testing.expectEqualSlices(
        f64,
        &.{ 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60 },
        &segment_fetch_buckets,
    );
    try testing.expect(segment_fetch_buckets.len <= max_buckets);
}

test "an observation exactly on a boundary lands in that bucket" {
    var r = Registry.init(testing.allocator);
    defer r.deinit();
    r.observe(&r.segment_fetch_seconds, &.{ "s", "ok" }, 0.05);
    const doc = try render(&r);
    defer testing.allocator.free(doc);
    // le is "less than or equal", so 0.05 counts in le="0.05".
    try testing.expect(hasLine(doc, "hoardarr_nntp_segment_fetch_seconds_bucket{server=\"s\",outcome=\"ok\",le=\"0.05\"} 1"));
}

test "hostile label values cannot forge a label or a sample" {
    var r = Registry.init(testing.allocator);
    defer r.deinit();

    // A "server name" the operator can type, chosen to break out of the
    // quoted value and append a second label and a second sample.
    const hostile = "a\" ,injected=\"1\"} 999\nhoardarr_jobs{state=\"forged\"} 1 #";
    r.set(&r.nntp_connections, &.{ hostile, "idle" }, 1);
    r.inc(&r.bytes_downloaded, &.{"back\\slash\rand\ncr"}, 2);

    const doc = try render(&r);
    defer testing.allocator.free(doc);

    // The forged label is present only in its escaped form, i.e. as
    // text inside the value, never as a label of its own.
    try testing.expect(std.mem.indexOf(u8, doc, "injected=\\\"") != null);
    try testing.expect(std.mem.indexOf(u8, doc, "injected=\"") == null);
    try testing.expect(std.mem.indexOf(u8, doc, "state=\"forged\"") == null);
    try testing.expect(hasLine(
        doc,
        "hoardarr_nntp_connections{server=\"a\\\" ,injected=\\\"1\\\"} 999\\nhoardarr_jobs{state=\\\"forged\\\"} 1 #\",state=\"idle\"} 1",
    ));
    try testing.expect(hasLine(doc, "hoardarr_nntp_bytes_downloaded_total{server=\"back\\\\slashand\\ncr\"} 2"));

    // Every line is either a comment or exactly one sample: no value
    // ended a line early.
    var it = std.mem.splitScalar(u8, doc, '\n');
    var samples: usize = 0;
    while (it.next()) |line| {
        if (line.len == 0) continue;
        if (line[0] == '#') continue;
        samples += 1;
        try testing.expect(std.mem.startsWith(u8, line, "hoardarr_"));
    }
    try testing.expectEqual(@as(usize, 2), samples);
}

test "cardinality is capped per family" {
    var r = Registry.init(testing.allocator);
    defer r.deinit();

    var buf: [32]u8 = undefined;
    for (0..max_series_per_family + 50) |i| {
        const name = try std.fmt.bufPrint(&buf, "sub-{d}", .{i});
        r.set(&r.outbox_pending, &.{name}, 1);
    }
    try testing.expectEqual(@as(usize, max_series_per_family), r.outbox_pending.seriesCount());
    try testing.expectEqual(@as(u64, 50), r.outbox_pending.dropped);
    // An existing series still updates once the table is full.
    r.set(&r.outbox_pending, &.{"sub-0"}, 42);
    try testing.expectEqual(@as(f64, 42), r.outbox_pending.get(&.{"sub-0"}).?);
}

test "label values are copied, not borrowed" {
    var r = Registry.init(testing.allocator);
    defer r.deinit();

    var scratch: [16]u8 = undefined;
    const name = try std.fmt.bufPrint(&scratch, "news.example", .{});
    r.inc(&r.bytes_downloaded, &.{name}, 7);
    // The caller's buffer is reused — exactly what happens when the
    // label came out of a connection's read buffer.
    @memset(&scratch, 'X');

    const doc = try render(&r);
    defer testing.allocator.free(doc);
    try testing.expect(hasLine(doc, "hoardarr_nntp_bytes_downloaded_total{server=\"news.example\"} 7"));
}

test "integral values print without an exponent or a fractional part" {
    var r = Registry.init(testing.allocator);
    defer r.deinit();

    r.inc(&r.bytes_downloaded, &.{"a"}, 1_000_000_000);
    r.set(&r.jobs, &.{"queued"}, 0);
    const doc = try render(&r);
    defer testing.allocator.free(doc);
    try testing.expect(hasLine(doc, "hoardarr_nntp_bytes_downloaded_total{server=\"a\"} 1000000000"));
    try testing.expect(hasLine(doc, "hoardarr_jobs{state=\"queued\"} 0"));
}

test "an empty registry still exposes every family's metadata" {
    var r = Registry.init(testing.allocator);
    defer r.deinit();
    const doc = try render(&r);
    defer testing.allocator.free(doc);
    // 9 families * 2 metadata lines, no samples.
    var it = std.mem.splitScalar(u8, doc, '\n');
    var lines: usize = 0;
    while (it.next()) |line| {
        if (line.len == 0) continue;
        try testing.expect(line[0] == '#');
        lines += 1;
    }
    try testing.expectEqual(@as(usize, 18), lines);
}

test "the exposition ends with a newline and has no blank lines" {
    var r = Registry.init(testing.allocator);
    defer r.deinit();
    r.set(&r.jobs, &.{"queued"}, 1);
    r.observe(&r.segment_fetch_seconds, &.{ "s", "ok" }, 1);
    const doc = try render(&r);
    defer testing.allocator.free(doc);

    try testing.expect(std.mem.endsWith(u8, doc, "\n"));
    try testing.expect(std.mem.indexOf(u8, doc, "\n\n") == null);
}

// -- the global instrumentation surface -------------------------------

test "instrumentation helpers are no-ops with no registry installed" {
    uninstall();
    // Every one of these is called from a hot path; none may fault or
    // allocate when metrics are not wired up (which is the case in every
    // unit test of every other module).
    setBuildInfo("1", "abc", "0.16.0");
    setJobsInState("queued", 1);
    resetJobs();
    incArticlesFetched("s", .ok);
    addBytesDownloaded("s", 10);
    observeSegmentFetch("s", .failed, std.time.ns_per_s);
    setNntpConnections("s", 1, 2);
    setOutboxPending("sub", 3);
    incOutboxDispatched("sub");
    incOutboxFailed("sub");
}

test "instrumentation helpers write through to the installed registry" {
    var r = Registry.init(testing.allocator);
    defer r.deinit();
    install(&r);
    defer uninstall();

    setBuildInfo("0.2.0", "deadbeef", "zig 0.16.0");
    setJobsInState("downloading", 2);
    incArticlesFetched("news.example", .ok);
    incArticlesFetched("news.example", .missing);
    addBytesDownloaded("news.example", 4096);
    // 250 ms, which must land in the le="0.25" bucket.
    observeSegmentFetch("news.example", .ok, 250 * std.time.ns_per_ms);
    setNntpConnections("news.example", 3, 5);
    setOutboxPending("sse-hub", 11);
    incOutboxDispatched("sse-hub");
    incOutboxFailed("sse-hub");

    const doc = try render(&r);
    defer testing.allocator.free(doc);

    try testing.expect(hasLine(doc, "hoardarr_build_info{version=\"0.2.0\",commit=\"deadbeef\",go_version=\"zig 0.16.0\"} 1"));
    try testing.expect(hasLine(doc, "hoardarr_jobs{state=\"downloading\"} 2"));
    try testing.expect(hasLine(doc, "hoardarr_nntp_articles_fetched_total{server=\"news.example\",outcome=\"ok\"} 1"));
    try testing.expect(hasLine(doc, "hoardarr_nntp_articles_fetched_total{server=\"news.example\",outcome=\"missing\"} 1"));
    try testing.expect(hasLine(doc, "hoardarr_nntp_bytes_downloaded_total{server=\"news.example\"} 4096"));
    try testing.expect(hasLine(doc, "hoardarr_nntp_segment_fetch_seconds_bucket{server=\"news.example\",outcome=\"ok\",le=\"0.25\"} 1"));
    try testing.expect(hasLine(doc, "hoardarr_nntp_segment_fetch_seconds_bucket{server=\"news.example\",outcome=\"ok\",le=\"0.1\"} 0"));
    try testing.expect(hasLine(doc, "hoardarr_nntp_connections{server=\"news.example\",state=\"in_use\"} 3"));
    try testing.expect(hasLine(doc, "hoardarr_nntp_connections{server=\"news.example\",state=\"idle\"} 5"));
    try testing.expect(hasLine(doc, "hoardarr_outbox_pending{subscription=\"sse-hub\"} 11"));
    try testing.expect(hasLine(doc, "hoardarr_outbox_dispatched_total{subscription=\"sse-hub\"} 1"));
    try testing.expect(hasLine(doc, "hoardarr_outbox_dispatch_failed_total{subscription=\"sse-hub\"} 1"));
}

test "the fetch-outcome vocabulary is ok/missing/failed" {
    try testing.expectEqualStrings("ok", FetchOutcome.ok.text());
    try testing.expectEqualStrings("missing", FetchOutcome.missing.text());
    try testing.expectEqualStrings("failed", FetchOutcome.failed.text());
}
