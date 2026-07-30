//! One real download, end to end, on one reactor.
//!
//! This is the test the whole port exists to pass:
//!
//!     addfile → download.JobCreated → segments fetched over NNTP
//!       → JobDownloadComplete → verify → VerifyOK → deliver
//!       → DeliveryComplete → the job is `completed`
//!
//! and then — the assertion that actually matters — the delivered file is
//! compared **byte for byte** against the fixture's original. "The job
//! says complete" is exactly the assertion that passes while the output
//! is corrupt: a yEnc decoder that drops an escape, a segment written at
//! the wrong offset and a PAR2 verifier that never ran all leave a green
//! job behind.
//!
//! ## Everything on one loop, and no sleeping
//!
//! The daemon, the NNTP server (`src/testserver/nntp.zig`) and the
//! progress checks all run on the same `reactor.Loop`, advanced by
//! `tick`. There is no second thread and nothing sleeps, so the test is
//! as deterministic as the reactor is: if the pipeline stalls, the tick
//! budget runs out and the test fails rather than hanging the suite.
//!
//! That arrangement is also what makes the fiber bridge honest here. The
//! stub's accept, the connection's readability, the job fiber's resume
//! timer and the outbox dispatcher's waker are all sources on the one
//! loop, dispatched in whatever order the kernel reports them — which is
//! precisely the interleaving a re-entrancy bug needs.

const std = @import("std");

const sys = @import("../posix/sys.zig");
const reactor = @import("../posix/reactor.zig");
const fiber = @import("../posix/fiber.zig");
const config = @import("../core/config.zig");
const log = @import("../core/log.zig");
const sqlite = @import("../store/sqlite.zig");
const migrate = @import("../store/migrate.zig");
const repo_server = @import("../store/repo_server.zig");
const repo_download = @import("../store/repo_download.zig");
const dserver = @import("../domain/server.zig");
const dstate = @import("../domain/download/state.zig");

const bootstrap = @import("../bootstrap.zig");
const infra = @import("infra.zig");
const settings = @import("settings.zig");
const fixture = @import("../testserver/fixture.zig");
const testserver = @import("../testserver/nntp.zig");

const Allocator = std.mem.Allocator;
const testing = std.testing;

/// A booted daemon, a fixture release, and an NNTP server that serves it
/// — all on one loop.
const Fixture = struct {
    gpa: Allocator,
    app: *bootstrap.App,
    /// Owned; unique to this process.
    dir: []u8,
    release: fixture.Fixture,
    server: testserver.Server = undefined,
    server_started: bool = false,

    const Options = struct {
        /// Share of articles the provider answers 430 to. The PAR2 set is
        /// sized so the release is still verifiable at the default of 0;
        /// raising it is how the missing-segment path gets exercised.
        missing_fraction: f64 = 0,
        max_connections: usize = 0,
        file_size: usize = 256 * 1024,
        article_size: usize = 64 * 1024,
        file_count: usize = 1,
    };

    fn init(gpa: Allocator, name: []const u8, opts: Options) !*Fixture {
        const self = try gpa.create(Fixture);
        errdefer gpa.destroy(self);

        // Unique per process, not just per test. A fixed path makes two
        // concurrent runs of the suite — which is normal on a developer's
        // machine and on CI — delete each other's `complete/` mid-test,
        // and the failure that produces looks exactly like a pipeline bug.
        const dir = try std.fmt.allocPrint(gpa, "/tmp/hoardarr-{s}-{x}", .{
            name,
            @as(u64, @bitCast(@as(i64, @truncate(sys.realtimeNanos())))) ^ sys.monotonicNanos(),
        });
        errdefer gpa.free(dir);

        // A fresh directory per run: a leftover database or a leftover
        // `complete/` from a previous failure would let this pass for the
        // wrong reason.
        var fs_impl = infra.RealFs{ .gpa = gpa };
        try fs_impl.filesystem().removeAll(dir);
        try sys.mkdirPath(dir);

        const release = try fixture.generate(gpa, .{
            .name = "Hoardarr.Test.Release",
            .file_count = opts.file_count,
            .file_size = opts.file_size,
            .article_size = opts.article_size,
            .par2_slice_size = 16 * 1024,
            .recovery_slices = 4,
        });

        const app = try gpa.create(bootstrap.App);
        app.* = .{ .gpa = gpa };
        app.cfg = try config.loadFromBytes(gpa, null, dir, .{
            .api_key_fallback = "0123456789abcdef0123456789abcdef",
        });
        try app.loop.init(gpa);
        try sys.mkdirPath(app.cfg.config.server.data_dir);

        var db_buf: [sys.path_max]u8 = undefined;
        const db_path = try sys.joinZ(&db_buf, app.cfg.config.server.data_dir, "hoardarr.db");
        app.db = try sqlite.Conn.open(gpa, db_path, .{});
        try migrate.migrate(app.db);

        self.* = .{ .gpa = gpa, .app = app, .dir = dir, .release = release };

        try app.wire();
        app.started = true;
        try app.ensureDirs();

        // The stub lives on the daemon's own loop, which is the whole
        // point: one thread drives both ends and the interleaving is the
        // reactor's, not a scheduler's.
        const port = try self.server.start(gpa, &app.loop, .{
            .missing_fraction = opts.missing_fraction,
            .max_connections = opts.max_connections,
        });
        self.server_started = true;
        for (release.articles) |a| try self.server.addArticle(a.message_id, a.body);

        // The server row the engine builds its pool from. Written through
        // the repository rather than the REST port so the test is about
        // the download path, not about the servers API.
        var row = try dserver.UsenetServer.init(gpa, .{
            .name = "stub",
            .host = "127.0.0.1",
            .port = @intCast(port),
            .tls = false,
            .max_conns = 4,
        }, infra.nowMillis());
        defer row.deinit();
        try repo_server.ServerRepo.init(gpa, app.db).save(&row);

        try app.startEngine();
        return self;
    }

    fn deinit(self: *Fixture) void {
        // The stub goes first, because its listener and its sessions are
        // sources on the daemon's loop and `App.deinit` ends with
        // `loop.deinit()`. The client side does not need a live peer to
        // be torn down: `App.deinit` cancels each job fiber, which hands
        // its connection back to a pool that then closes the socket.
        if (self.server_started) self.server.deinit();
        self.app.deinit();
        self.gpa.destroy(self.app);
        self.release.deinit();

        var fs_impl = infra.RealFs{ .gpa = self.gpa };
        fs_impl.filesystem().removeAll(self.dir) catch {};
        self.gpa.free(self.dir);
        self.gpa.destroy(self);
    }

    fn addRelease(self: *Fixture) !i64 {
        const added = try self.app.p_queue.port().add(self.app.api.beginRequest(), .{
            .nzb = self.release.nzb,
            .name = "Hoardarr.Test.Release",
            // No category, so the release lands directly under
            // `complete/`, which keeps the expected path a constant.
            .category = "",
            .source = "download-test",
        });
        return added.id;
    }

    fn jobState(self: *Fixture, id: i64) !dstate.JobState {
        const repo = repo_download.JobRepo.init(self.gpa, self.app.db);
        var job = try repo.byId(self.gpa, id);
        defer job.deinit();
        return job.state;
    }

    /// Advance the loop until `id` reaches a terminal state, or the tick
    /// budget runs out.
    ///
    /// Bounded rather than open-ended: a pipeline that wedges must fail
    /// this test in seconds, not hang the suite until CI kills it.
    fn runUntilTerminal(self: *Fixture, id: i64, max_ms: u64) !dstate.JobState {
        const deadline = sys.monotonicNanos() + max_ms * std.time.ns_per_ms;
        while (sys.monotonicNanos() < deadline) {
            _ = try self.app.loop.tick(5);
            const state = try self.jobState(id);
            if (state.isTerminal()) return state;
        }
        return error.PipelineDidNotFinish;
    }

    /// Everything the delivery left in `complete/<release>/`.
    fn deliveredFiles(self: *Fixture, a: Allocator) ![]const []const u8 {
        const dir = try std.fmt.allocPrint(a, "{s}/Hoardarr.Test.Release", .{
            self.app.cfg.config.paths.complete_dir,
        });
        var fs_impl = infra.RealFs{ .gpa = self.gpa };
        const entries = try fs_impl.filesystem().list(a, dir);
        var out: std.ArrayList([]const u8) = .empty;
        for (entries) |e| {
            if (e.is_dir) continue;
            try out.append(a, try std.fmt.allocPrint(a, "{s}/{s}", .{ dir, e.name }));
        }
        return out.items;
    }

    fn readFile(self: *Fixture, a: Allocator, path: []const u8) ![]u8 {
        _ = self;
        var buf: [sys.path_max]u8 = undefined;
        const p = try sys.pathZ(&buf, path);
        const fd = try sys.open(p, .{});
        defer sys.close(fd);
        const size = try sys.fileSize(fd);
        const bytes = try a.alloc(u8, @intCast(size));
        var off: usize = 0;
        while (off < bytes.len) {
            const n = try sys.read(fd, bytes[off..]);
            if (n == 0) break;
            off += n;
        }
        return bytes[0..off];
    }
};

test "a release downloads, verifies, delivers, and the bytes match the original" {
    const gpa = testing.allocator;
    const stacks_before = fiber.liveStacks();

    var fx = try Fixture.init(gpa, "download", .{});
    defer fx.deinit();

    const id = try fx.addRelease();
    try testing.expect(id > 0);

    const final = try fx.runUntilTerminal(id, 30_000);
    if (final != .completed) {
        std.debug.print("\njob ended in {s}\n", .{final.toString()});
        return error.JobDidNotComplete;
    }

    // The provider was actually asked for every article — a pipeline that
    // "completed" without fetching anything would still reach the state
    // above if the delivery step were lenient enough.
    try testing.expect(fx.server.served >= fx.release.articles.len);
    try testing.expect(fx.server.accepted >= 1);

    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const a = arena.allocator();

    const delivered = try fx.deliveredFiles(a);
    // The parity files are scratch and must not be delivered; exactly the
    // data files should be there.
    var expected_data: usize = 0;
    for (fx.release.files) |f| {
        if (f.is_data) expected_data += 1;
    }
    try testing.expectEqual(expected_data, delivered.len);

    // The assertion this whole file exists for.
    for (delivered) |path| {
        const name = std.fs.path.basename(path);
        const original = fx.release.fileBytes(name) orelse {
            std.debug.print("\ndelivered an unexpected file: {s}\n", .{name});
            return error.UnexpectedDeliveredFile;
        };
        const landed = try fx.readFile(a, path);
        try testing.expectEqual(original.len, landed.len);
        if (!std.mem.eql(u8, original, landed)) {
            // Report where, not just that: an offset tells a reader
            // whether it is a decode bug or a placement bug.
            for (original, landed, 0..) |x, y, i| {
                if (x == y) continue;
                std.debug.print("\n{s} differs at byte {d}: want {x}, got {x}\n", .{ name, i, x, y });
                break;
            }
            return error.DeliveredBytesDiffer;
        }
    }

    // The scratch directory is gone, so a finished job leaves nothing
    // behind for the next one to trip over.
    var fs_impl = infra.RealFs{ .gpa = gpa };
    const job_dir = try std.fmt.allocPrint(a, "{s}/{d}", .{
        fx.app.cfg.config.paths.incomplete_dir,
        id,
    });
    try testing.expect(!fs_impl.filesystem().exists(job_dir));

    // Every fiber the job used is gone. A leaked stack is invisible to
    // the testing allocator — the mapping is `mmap`'d — so this counter
    // is the only thing that sees it.
    try testing.expectEqual(stacks_before, fiber.liveStacks());
}

test "the event flow really ran, stage by stage" {
    const gpa = testing.allocator;
    var fx = try Fixture.init(gpa, "download-flow", .{
        .file_size = 64 * 1024,
        .article_size = 32 * 1024,
    });
    defer fx.deinit();

    const id = try fx.addRelease();
    try testing.expectEqual(dstate.JobState.completed, try fx.runUntilTerminal(id, 30_000));

    // The per-job timeline is the outbox's own record of what happened,
    // so asserting on it proves the *bus* carried the pipeline rather
    // than the services happening to be called in order.
    var timeline = try fx.app.bus.eventsByJob(fx.app.db, gpa, id);
    defer timeline.deinit();

    const want = [_][]const u8{
        "download.job.created",
        "download.job.started",
        "download.segment.completed",
        "download.file.completed",
        "download.job.download_complete",
        "verify.started",
        "verify.ok",
        "deliver.started",
        "deliver.complete",
        "download.job.completed",
    };
    for (want) |topic| {
        var seen = false;
        for (timeline.items.items) |env| {
            if (std.mem.eql(u8, env.topic, topic)) {
                seen = true;
                break;
            }
        }
        if (!seen) {
            std.debug.print("\ntimeline is missing {s}; it has:\n", .{topic});
            for (timeline.items.items) |env| std.debug.print("  {s}\n", .{env.topic});
            return error.StageMissingFromTimeline;
        }
    }
}

test "a job whose provider drops articles fails without wedging the loop" {
    // A provider that vanishes mid-job must fail the job and free its
    // slot. Dropping every article is the extreme form: nothing arrives,
    // the segments exhaust their budget, and the job has to end.
    const gpa = testing.allocator;
    const stacks_before = fiber.liveStacks();

    var fx = try Fixture.init(gpa, "download-missing", .{
        .missing_fraction = 1.0,
        .file_size = 32 * 1024,
        .article_size = 32 * 1024,
    });
    defer fx.deinit();

    const id = try fx.addRelease();
    const final = try fx.runUntilTerminal(id, 30_000);

    // Every segment 430'd, so the download phase failed outright rather
    // than sitting in `download_complete` looking green.
    try testing.expectEqual(dstate.JobState.failed, final);

    // And the slot came back: no fiber left parked on a socket, no
    // concurrency slot held by a job that is over.
    try testing.expectEqual(@as(usize, 0), fx.app.engine.activeSlots());
    try testing.expectEqual(stacks_before, fiber.liveStacks());

    // The loop still works afterwards, which is the "not wedged" half.
    _ = try fx.app.loop.tick(1);
}

test "shutdown with a download in flight tears the fibers down in order" {
    // The dangerous case: a fiber parked mid-fetch on a socket the loop
    // is about to close. `App.deinit` has to cancel it so it unwinds
    // through its own defers — releasing the job aggregate and handing
    // the provider connection back — before anything it holds is freed.
    const gpa = testing.allocator;
    const stacks_before = fiber.liveStacks();

    var fx = try Fixture.init(gpa, "download-shutdown", .{
        .file_size = 2 << 20,
        .article_size = 32 * 1024,
    });

    const id = try fx.addRelease();
    _ = id;

    // Tick just enough for the fetch to be genuinely in flight — a
    // connection open, a fiber parked on a body — and no further.
    var ticks: usize = 0;
    while (ticks < 200 and fx.server.served == 0) : (ticks += 1) {
        _ = try fx.app.loop.tick(1);
    }
    try testing.expect(fx.app.engine.activeSlots() >= 1);

    // No leak report and no crash is the assertion; `testing.allocator`
    // makes the first half of that automatic and `liveStacks` the second.
    fx.deinit();
    try testing.expectEqual(stacks_before, fiber.liveStacks());
}
