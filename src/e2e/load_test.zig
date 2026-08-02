//! The daemon under sustained concurrent load.
//!
//! Every other e2e test drives one job at a time, which leaves the
//! interesting failures untested: a reactor that starves a source when
//! several are hot, a connection pool that hands the same connection to
//! two waiters, a fiber stack that is freed once per job but reserved
//! twice, a descriptor that is only closed on the happy path. None of
//! those show up on a single download — they show up on the tenth, or on
//! the thousandth, which is where an operator lives.
//!
//! # Distinct releases, not one release queued N times
//!
//! `add_job` keys a job on the SHA-256 of the NZB bytes and answers a
//! repeat add with the *existing* job's id. Queueing one release under N
//! names therefore yields one job and N handles to it, and a test built
//! that way would report perfect concurrency while running a single
//! download. So `Batch` generates N releases that genuinely differ —
//! different names, different seeds, therefore different bytes,
//! different message-ids and different NZB hashes — and the harness
//! grew `serveFixture`/`addFixture`/`expectDeliveredMatches` so a caller
//! can own releases the harness does not.
//!
//! # All of them, not each of them
//!
//! Waiting on job 1 and then on job 2 passes just as well on a daemon
//! that runs them strictly in sequence. `runAll` advances the shared
//! loop once and then re-reads *every* job, so the run ends when the
//! last one finishes, and it reports the peak number of occupied slots
//! along the way — the one number that says the jobs really overlapped.

const std = @import("std");
const testing = std.testing;
const Allocator = std.mem.Allocator;

const h = @import("harness.zig");
const sys = @import("../posix/sys.zig");
const fiber = @import("../posix/fiber.zig");
const infra = @import("../bootstrap/infra.zig");
const settings = @import("../bootstrap/settings.zig");
const dserver = @import("../domain/server.zig");
const repo_server = @import("../store/repo_server.zig");
const tsfixture = @import("../testserver/fixture.zig");

// ---------------------------------------------------------------------
// A batch of distinct releases
// ---------------------------------------------------------------------

const Batch = struct {
    gpa: Allocator,
    releases: std.ArrayList(tsfixture.Fixture) = .empty,
    /// Job names, one per release. Also the release's base filename, so
    /// a delivery landing under the wrong name cannot match by accident.
    names: std.ArrayList([]u8) = .empty,
    /// Filled by `queue`, in the same order.
    ids: std.ArrayList(i64) = .empty,

    /// Generates `count` releases from one shape.
    ///
    /// The per-release seed offset is a large odd constant rather than
    /// the index so that two batches in the same run cannot share a
    /// corpus by lining up on a boundary; the name alone already forces
    /// distinct message-ids, but distinct *bytes* are what make the NZB
    /// hashes differ and therefore what makes these N jobs and not one.
    fn generate(
        gpa: Allocator,
        prefix: []const u8,
        count: usize,
        shape: tsfixture.Options,
    ) !Batch {
        var self: Batch = .{ .gpa = gpa };
        errdefer self.deinit();

        var i: usize = 0;
        while (i < count) : (i += 1) {
            const name = try std.fmt.allocPrint(gpa, "{s}.{d:0>2}", .{ prefix, i + 1 });
            try self.names.append(gpa, name);

            var opts = shape;
            opts.name = name;
            opts.seed = shape.seed +% i *% 0xA24B_AED4_963E_E407;
            try self.releases.append(gpa, try tsfixture.generate(gpa, opts));
        }
        return self;
    }

    fn deinit(self: *Batch) void {
        for (self.releases.items) |*r| r.deinit();
        self.releases.deinit(self.gpa);
        for (self.names.items) |n| self.gpa.free(n);
        self.names.deinit(self.gpa);
        self.ids.deinit(self.gpa);
    }

    /// Registers every article of every release with the provider.
    fn serve(self: *Batch, fx: *h.Harness) !void {
        for (self.releases.items) |*r| try fx.serveFixture(r);
    }

    /// Queues release `i`, remembering its job id.
    fn queueOne(self: *Batch, fx: *h.Harness, i: usize) !i64 {
        const id = try fx.addFixture(&self.releases.items[i], self.names.items[i]);
        try self.ids.append(self.gpa, id);
        return id;
    }

    /// Queues all of them, back to back, before any of them runs.
    fn queueAll(self: *Batch, fx: *h.Harness) !void {
        for (0..self.releases.items.len) |i| _ = try self.queueOne(fx, i);
    }

    /// Every job id differs from every other.
    ///
    /// This is the assertion that catches the dedupe silently collapsing
    /// the batch: without it, a batch that generated identical bytes
    /// would still "complete N jobs" and still deliver exact files,
    /// because there would only ever have been one.
    fn expectDistinctIds(self: *const Batch) !void {
        for (self.ids.items, 0..) |id, i| {
            if (id <= 0) return error.JobNotQueued;
            for (self.ids.items[i + 1 ..]) |other| {
                if (id == other) {
                    std.debug.print("\njob id {d} was handed out twice; the adds deduped\n", .{id});
                    return error.ReleasesWereNotDistinct;
                }
            }
        }
    }

    /// Every release compared byte for byte against what landed.
    fn expectAllDelivered(self: *Batch, fx: *h.Harness) !void {
        for (self.releases.items, self.names.items) |*r, name| {
            fx.expectDeliveredMatches(r, name) catch |e| {
                std.debug.print("delivery of {s} is wrong\n", .{name});
                return e;
            };
        }
    }

    /// Segments across the whole batch, so a test can say how much was
    /// actually in flight rather than how much was intended to be.
    fn totalSegments(self: *const Batch, fx: *h.Harness) !i64 {
        var n: i64 = 0;
        for (self.ids.items) |id| n += try fx.segmentCount(id);
        return n;
    }
};

// ---------------------------------------------------------------------
// Driving several jobs at once
// ---------------------------------------------------------------------

/// Advances the shared loop until every job in `ids` is terminal, and
/// returns the highest number of slots seen occupied at once.
///
/// The peak is sampled per tick rather than derived at the end because
/// by the time the last job finishes the evidence is gone: the slots are
/// reaped, and a daemon that ran the batch one job at a time looks
/// exactly like one that ran it ten at a time.
fn runAll(fx: *h.Harness, ids: []const i64, max_ms: u64) !usize {
    const deadline = sys.monotonicNanos() + max_ms * std.time.ns_per_ms;
    var peak: usize = 0;
    while (sys.monotonicNanos() < deadline) {
        _ = try fx.app.loop.tick(5);
        peak = @max(peak, fx.app.engine.activeSlots());

        var finished: usize = 0;
        for (ids) |id| {
            if ((try fx.jobState(id)).isTerminal()) finished += 1;
        }
        if (finished == ids.len) return peak;
    }

    // Which ones wedged, and where. A bare timeout on a batch says only
    // that something stopped; the state plus the durable timeline names
    // the stage that never ran, per job, which is the difference between
    // "the loop starved one job" and "every job stalled at the same
    // handoff".
    std.debug.print("\nthe batch did not finish:\n", .{});
    for (ids) |id| {
        std.debug.print("  job {d}: {s}\n", .{ id, (try fx.jobState(id)).toString() });
        var timeline = try fx.app.bus.eventsByJob(fx.app.db, fx.gpa, id);
        defer timeline.deinit();
        for (timeline.items.items) |env| std.debug.print("      {s}\n", .{env.topic});
    }
    return error.BatchDidNotFinish;
}

/// Every job ended in `completed`, with the ones that did not named.
fn expectAllCompleted(fx: *h.Harness, ids: []const i64) !void {
    var bad: usize = 0;
    for (ids) |id| {
        const state = try fx.jobState(id);
        if (state == .completed) continue;
        bad += 1;
        std.debug.print("\njob {d} ended in {s}; want completed\n", .{ id, state.toString() });
    }
    if (bad != 0) return error.JobsDidNotComplete;
}

// ---------------------------------------------------------------------
// Setup shared by the load tests
// ---------------------------------------------------------------------

/// Writes the `servers` row naming the provider the test just started.
///
/// Not `Harness.withRelease`: that generates its own single release and
/// pins `max_conns` at 4, and a load test needs to own the releases and
/// to squeeze the pool.
fn addServer(fx: *h.Harness, gpa: Allocator, port: u16, max_conns: u16) !void {
    var row = try dserver.UsenetServer.init(gpa, .{
        .name = "stub",
        .host = "127.0.0.1",
        .port = port,
        .tls = false,
        .max_conns = max_conns,
    }, infra.nowMillis());
    defer row.deinit();
    try repo_server.ServerRepo.init(gpa, fx.app.db).save(&row);
}

/// Raises the across-job concurrency cap.
///
/// The shipped default is 1 — the right default for a box whose uplink
/// is the bottleneck, and fatal to this file: at 1 the scheduler admits
/// one runner and the "concurrent" batch is a queue.
fn setConcurrency(fx: *h.Harness, jobs: i64) !void {
    try fx.app.runtime.setInt(settings.keys.max_concurrent_jobs, jobs);
}

/// Descriptor slots probed by the leak check. Well above anything the
/// daemon opens, so the count is the whole truth rather than a prefix of
/// it, and cheap enough to run twice in a test.
const fd_ceiling: sys.Fd = 4096;

// ---------------------------------------------------------------------
// The tests
// ---------------------------------------------------------------------

test "load: many jobs in flight at once all complete, and every delivered byte is exact" {
    const gpa = testing.allocator;
    const stacks_before = fiber.liveStacks();
    const job_count = 10;

    var fx = try h.Harness.init(gpa, "load-many", .{});
    defer fx.deinit();

    // Multi-file and multi-segment, so the batch puts a couple of
    // hundred segments through the fetcher rather than ten fat ones —
    // segment bookkeeping is where a shared pool goes wrong.
    var batch = try Batch.generate(gpa, "Hoardarr.Load.Many", job_count, .{
        .file_count = 2,
        .file_size = 64 * 1024,
        .article_size = 8 * 1024,
        .par2_slice_size = 16 * 1024,
        .recovery_slices = 2,
    });
    defer batch.deinit();

    // Four connections for ten jobs, on both sides of the wire: the pool
    // is genuinely contended, and the provider would answer 502 if the
    // pool ever exceeded its own cap.
    const port = try fx.startNntp(.{ .max_connections = 4 });
    try batch.serve(fx);
    try addServer(fx, gpa, port, 4);
    try setConcurrency(fx, job_count);
    try fx.startEngine();

    try batch.queueAll(fx);
    try batch.expectDistinctIds();

    const peak = try runAll(fx, batch.ids.items, 120_000);
    try expectAllCompleted(fx, batch.ids.items);

    // The jobs overlapped. Without this the test is a slow sequential
    // run wearing a load test's name.
    if (peak < 2) {
        std.debug.print("\npeak concurrency was {d}; the batch ran sequentially\n", .{peak});
        return error.NoConcurrency;
    }

    // The provider was never asked for more connections than it allows,
    // and it really did serve everything from a shared few.
    try testing.expectEqual(@as(usize, 0), fx.provider().refused);
    try testing.expect(fx.provider().open <= 4);

    // Enough segments that the contention is real rather than nominal,
    // and every one of them actually crossed the wire — a pipeline that
    // completed without asking for most of them would be the interesting
    // failure here.
    const segments = try batch.totalSegments(fx);
    const served = fx.provider().served;
    if (segments < 200 or served < @as(usize, @intCast(segments))) {
        std.debug.print(
            "\n{d} segments across the batch, {d} served by the provider\n",
            .{ segments, served },
        );
        return error.NotEnoughSegmentsInFlight;
    }

    // The assertion contention exists to threaten: ten interleaved
    // downloads, and not one byte landed in the wrong file.
    try batch.expectAllDelivered(fx);

    // Every fiber stack a slot reserved came back. A pool that leaked
    // one per job would still deliver correct bytes.
    try testing.expectEqual(stacks_before, fiber.liveStacks());
    try testing.expectEqual(@as(usize, 0), fx.app.engine.activeSlots());
}

test "load: repeated cycles do not leak file descriptors or memory" {
    const gpa = testing.allocator;
    const stacks_before = fiber.liveStacks();
    const cycles = 5;

    var fx = try h.Harness.init(gpa, "load-cycles", .{});
    defer fx.deinit();

    var batch = try Batch.generate(gpa, "Hoardarr.Load.Cycle", cycles, .{
        .file_count = 1,
        .file_size = 64 * 1024,
        .article_size = 16 * 1024,
        .par2_slice_size = 16 * 1024,
        .recovery_slices = 2,
    });
    defer batch.deinit();

    const port = try fx.startNntp(.{});
    try batch.serve(fx);
    try addServer(fx, gpa, port, 2);
    try fx.startEngine();

    // The first cycle is the warm-up and is deliberately not measured:
    // it is where the pool dials its connections, SQLite opens its WAL
    // and the delivery tree gets made. Counting from before it would
    // report that legitimate one-off growth as a leak, and the number
    // that matters is the one *between* steady-state cycles anyway.
    {
        const id = try batch.queueOne(fx, 0);
        try testing.expectEqual(h.JobState.completed, try fx.runUntilTerminal(id, 60_000));
        try fx.expectDeliveredMatches(&batch.releases.items[0], batch.names.items[0]);
    }

    const fds_before = sys.openFdCount(fd_ceiling);

    var i: usize = 1;
    while (i < cycles) : (i += 1) {
        const id = try batch.queueOne(fx, i);
        const final = try fx.runUntilTerminal(id, 60_000);
        if (final != .completed) {
            std.debug.print("\ncycle {d} ended in {s}\n", .{ i, final.toString() });
            return error.CycleDidNotComplete;
        }
        try fx.expectDeliveredMatches(&batch.releases.items[i], batch.names.items[i]);
    }

    const fds_after = sys.openFdCount(fd_ceiling);

    // Slack rather than equality: the pool is allowed to have dialled a
    // second connection somewhere in there and to still be holding it,
    // which is the design and not a leak. What must not happen is growth
    // that tracks the cycle count — four cycles leaking one descriptor
    // each would clear a slack of 2 and fail here.
    if (fds_after > fds_before + 2) {
        std.debug.print(
            "\nopen descriptors grew from {d} to {d} over {d} cycles\n",
            .{ fds_before, fds_after, cycles - 1 },
        );
        return error.DescriptorLeak;
    }

    // Nothing is holding a slot or a stack open between cycles. The
    // allocator half of this test needs no assertion: `testing.allocator`
    // fails the test on any block the harness's `deinit` did not free.
    try testing.expectEqual(@as(usize, 0), fx.app.engine.activeSlots());
    try testing.expectEqual(stacks_before, fiber.liveStacks());
}

test "load: the pipeline still completes when the provider is slow, lossy and connection-capped" {
    const gpa = testing.allocator;
    const job_count = 4;

    var fx = try h.Harness.init(gpa, "load-adverse", .{});
    defer fx.deinit();

    // 16 data slices with 8 recovery slices: parity for half the set,
    // which comfortably covers a 20% article loss but is not the blanket
    // that would let a broken reconstruction pass.
    var batch = try Batch.generate(gpa, "Hoardarr.Load.Adverse", job_count, .{
        .file_count = 1,
        .file_size = 256 * 1024,
        .article_size = 32 * 1024,
        .par2_slice_size = 16 * 1024,
        .recovery_slices = 8,
    });
    defer batch.deinit();

    // All three at once, which is the point: each of these is covered on
    // its own elsewhere, and each on its own leaves the daemon a way to
    // cope that the combination takes away. Two connections for four
    // jobs, every article delayed, and a fifth of them simply not there.
    const port = try fx.startNntp(.{
        .missing_fraction = 0.2,
        .article_latency_ns = 2 * std.time.ns_per_ms,
        .max_connections = 2,
    });
    try batch.serve(fx);
    try addServer(fx, gpa, port, 2);
    try setConcurrency(fx, job_count);
    try fx.startEngine();

    try batch.queueAll(fx);
    try batch.expectDistinctIds();

    _ = try runAll(fx, batch.ids.items, 180_000);
    try expectAllCompleted(fx, batch.ids.items);

    // Articles really were missing, so repair really ran. A fixture and
    // a salt that happened to drop nothing would make this an expensive
    // duplicate of the first test.
    if (fx.provider().missed == 0) {
        std.debug.print("\nnothing was dropped; there was no adversity to survive\n", .{});
        return error.NothingWouldBeMissing;
    }

    // Refusals are not asserted away here. A dropped article costs the
    // pool a connection and it re-dials, and a re-dial that overlaps the
    // old socket's close is over the provider's cap for as long as the
    // FIN takes — so "502 too many connections" is part of the weather
    // this test is about surviving, not a defect to pin a bound on. That
    // the batch completed above is the statement that matters.

    try batch.expectAllDelivered(fx);
}

test "load: one job's segments are fetched concurrently, up to the provider's connection cap" {
    // The regression this exists for is invisible to every other test in
    // this file, because a fake provider on loopback answers in
    // microseconds: fetching 60 articles one after another and fetching
    // them eight at a time then differ by the decode time and nothing
    // else. A real provider is 20–50ms away, where serial fetching is
    // bounded at one article per round trip however fast the link is.
    //
    // So the provider is given a per-article latency and the download is
    // timed against it. `served × latency` is the floor for a daemon that
    // waits for each article before asking for the next; a daemon using
    // its eight connection slots comes in around an eighth of that. The
    // margin between them is what makes this a measurement rather than a
    // flake — a third of the serial floor is still more than twice what
    // the parallel path needs.
    const gpa = testing.allocator;
    const conns: u16 = 8;
    const latency_ns: u64 = 30 * std.time.ns_per_ms;

    var fx = try h.Harness.init(gpa, "load-parallel", .{});
    defer fx.deinit();

    // Small articles over a whole megabyte, so there are many times more
    // segments than connections and the run is dominated by round trips
    // rather than by bytes.
    var batch = try Batch.generate(gpa, "Hoardarr.Load.Parallel", 1, .{
        .file_count = 1,
        .file_size = 1024 * 1024,
        .article_size = 16 * 1024,
        .par2_slice_size = 64 * 1024,
    });
    defer batch.deinit();

    const port = try fx.startNntp(.{
        .article_latency_ns = latency_ns,
        .max_connections = conns,
    });
    try batch.serve(fx);
    try addServer(fx, gpa, port, conns);
    try fx.startEngine();

    const id = try batch.queueOne(fx, 0);

    const started = sys.monotonicNanos();
    var peak_open: usize = 0;
    const deadline = started + 180 * std.time.ns_per_s;
    var final: h.JobState = .queued;
    while (sys.monotonicNanos() < deadline) {
        _ = try fx.app.loop.tick(1);
        // Sampled per tick because the evidence is gone by the end: the
        // pool closes its connections when the job does.
        peak_open = @max(peak_open, fx.provider().open);
        final = try fx.jobState(id);
        if (final.isTerminal()) break;
    }
    const elapsed_ns = sys.monotonicNanos() -| started;

    if (final != .completed) {
        std.debug.print("\nparallel job ended in {s}\n", .{final.toString()});
        return error.ParallelJobDidNotComplete;
    }
    try batch.expectAllDelivered(fx);

    const served = fx.provider().served;
    const serial_ns = @as(u64, @intCast(served)) * latency_ns;
    std.debug.print(
        "\n[load] {d} articles at {d} ms each: {d} ms elapsed, {d} ms if serial, {d} connections\n",
        .{
            served,
            latency_ns / std.time.ns_per_ms,
            elapsed_ns / std.time.ns_per_ms,
            serial_ns / std.time.ns_per_ms,
            peak_open,
        },
    );

    // The number that says the connection slots were actually used. A
    // daemon walking its segments in order needs exactly one.
    if (fx.provider().accepted < conns or peak_open < conns) {
        std.debug.print(
            "\nthe provider accepted {d} connections, {d} at once; it sold {d}\n",
            .{ fx.provider().accepted, peak_open, conns },
        );
        return error.ConnectionCapNotReached;
    }
    // And never more than it sold, which is the other half of honouring
    // the cap.
    try testing.expectEqual(@as(usize, 0), fx.provider().refused);

    if (elapsed_ns * 3 >= serial_ns) {
        std.debug.print(
            "\nthe download took {d} ms against a serial floor of {d} ms; the segments went out one at a time\n",
            .{ elapsed_ns / std.time.ns_per_ms, serial_ns / std.time.ns_per_ms },
        );
        return error.SegmentsFetchedSerially;
    }
}

test "load: sustained throughput through the whole daemon" {
    const gpa = testing.allocator;
    const release_name = "Hoardarr.Load.Throughput";

    var fx = try h.Harness.init(gpa, "load-throughput", .{});
    defer fx.deinit();

    // Tens of MiB in realistic-sized articles, so the number below is
    // dominated by the pipeline — yEnc decode, CRC, writeback, PAR2
    // verify — rather than by per-segment bookkeeping.
    try fx.withRelease(.{
        .name = release_name,
        .file_count = 4,
        .file_size = 6 * 1024 * 1024,
        .article_size = 512 * 1024,
        .par2_slice_size = 512 * 1024,
        .recovery_slices = 2,
    }, .{});

    const started = sys.monotonicNanos();
    const id = try fx.addRelease(release_name);
    const final = try fx.runUntilTerminal(id, 180_000);
    const elapsed_ns = sys.monotonicNanos() -| started;

    if (final != .completed) {
        std.debug.print("\nthroughput job ended in {s}\n", .{final.toString()});
        return error.ThroughputJobDidNotComplete;
    }
    try fx.expectDeliveredMatchesRelease(release_name);

    const payload_bytes: u64 = 4 * 6 * 1024 * 1024;
    const mb_per_sec =
        @as(f64, @floatFromInt(payload_bytes)) /
        (1024.0 * 1024.0) /
        (@as(f64, @floatFromInt(@max(elapsed_ns, 1))) / @as(f64, std.time.ns_per_s));
    std.debug.print(
        "\n[load] {d} MiB end to end in {d} ms — {d:.1} MiB/s\n",
        .{ payload_bytes / (1024 * 1024), elapsed_ns / std.time.ns_per_ms, mb_per_sec },
    );

    // Deliberately far below what the pipeline actually manages. The
    // number above is the observation; this is only a tripwire for a
    // regression that changes the order of magnitude, and it has to
    // survive a CI box running the whole suite in parallel on a loaded
    // machine. A tight bound here would fail for reasons that have
    // nothing to do with hoardarr.
    if (mb_per_sec < 1.0) {
        std.debug.print("\nthroughput collapsed to {d:.2} MiB/s\n", .{mb_per_sec});
        return error.ThroughputFloorMissed;
    }
}
