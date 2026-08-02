//! Boots a real `bootstrap.App` and drives it from a test.
//!
//! # What this is for
//!
//! Every component in the tree has unit tests. What none of them prove
//! is that the composition root wires them into something that works:
//! a port left null answers 503 with a body that looks legitimate, a
//! migration that half-ran leaves a daemon that starts and then fails
//! on the first write. The only assertion that catches those is one
//! made against a booted daemon over its own HTTP surface.
//!
//! So this boots the same `App` `bootstrap.run` boots — same config
//! loader, same SQLite file, same migrations, same object graph, same
//! route table — against a temporary data directory and an ephemeral
//! port, and lets a test drive it.
//!
//! # There are no sleeps
//!
//! The daemon is single-threaded on a reactor, and so is the test. A
//! test that slept would be sleeping on the same thread the daemon
//! needs to make progress on, so it would not merely be flaky — it
//! would deadlock. `pumpUntil` advances the loop and re-checks a
//! predicate against a wall-clock deadline; the deadline exists so a
//! wiring bug that never answers fails the test instead of hanging the
//! suite.
//!
//! The test NNTP server runs on the *same* loop, which is what makes
//! this work: one `tick` advances the HTTP client, the HTTP server, the
//! NNTP client and the NNTP server together, with no race to lose.
//!
//! # Isolation
//!
//! Each harness gets its own directory under `/tmp` with random bytes
//! in the name and its own kernel-assigned port, so the suite can run
//! in any order, in parallel, and repeatedly, without two tests sharing
//! a database or fighting for a port. `deinit` removes the directory —
//! a leftover database from a previous failure is exactly the thing
//! that makes a test pass for the wrong reason.

const std = @import("std");

const sys = @import("../posix/sys.zig");
const reactor = @import("../posix/reactor.zig");
const fiber = @import("../posix/fiber.zig");
const config = @import("../core/config.zig");
const client = @import("../net/http/client.zig");
const sqlite = @import("../store/sqlite.zig");
const migrate = @import("../store/migrate.zig");
const repo_server = @import("../store/repo_server.zig");
const repo_download = @import("../store/repo_download.zig");
const dserver = @import("../domain/server.zig");
const dstate = @import("../domain/download/state.zig");
const bootstrap = @import("../bootstrap.zig");
const infra = @import("../bootstrap/infra.zig");
const tsnntp = @import("../testserver/nntp.zig");
const tsfixture = @import("../testserver/fixture.zig");

const Allocator = std.mem.Allocator;
const testing = std.testing;

pub const JobState = dstate.JobState;
pub const SegmentState = dstate.SegmentState;

pub const Error = error{
    /// The predicate never became true before the deadline.
    Timeout,
    /// The exchange completed with a transport-level failure.
    RequestFailed,
} || Allocator.Error;

/// How long a `pumpUntil` waits before giving up. Generous, because it
/// is a failure bound and not a synchronisation delay: nothing waits
/// for it in the happy path.
pub const default_deadline_ns: u64 = 20 * std.time.ns_per_s;

/// The API key every harness boots with. Fixed rather than generated so
/// a test can write it as a literal in a URL; the daemon's own
/// key-generation path is covered by `bootstrap.zig`'s unit tests.
pub const api_key = "0123456789abcdef0123456789abcdef";

/// Where each bounded context's ids start in a harness database.
///
/// A job, its verify set, its repair attempt and its delivery are four
/// different counters that all start at 1. In a test that runs one job
/// they therefore all *are* 1, and a stage that routes on the wrong one
/// still lands on the right job — which is precisely how the post-download
/// pipeline spent months keying on the publishing context's aggregate id
/// instead of the job id with a green suite. Numbers this far apart, and
/// none of them 0 or 1, turn that confusion into a wrong answer: an event
/// routed by a verify-set id names a job that does not exist, and the
/// pipeline stops instead of quietly working on a stranger.
///
/// Do not collapse these back to 1 "for readability" — the readability is
/// what the bug hid behind.
pub const id_offset = struct {
    pub const job: i64 = 1_400;
    pub const verify_set: i64 = 2_700;
    pub const repair: i64 = 5_100;
    pub const delivery: i64 = 8_300;
};

/// The job the offset sentinels are parked under. Negative, so it can
/// never be a real job's id and no query keyed on one can reach them.
const sentinel_job: i64 = -1;

/// A booted daemon, its data directory, and the loop everything shares.
///
/// Heap-allocated in `init` and never moved: the reactor stores
/// `&app.server.listener.source` and `&app.ticker`, so a by-value
/// return would leave the loop pointing at a dead stack frame.
pub const Harness = struct {
    gpa: Allocator,
    app: *bootstrap.App,
    /// Owned. The temp directory tree, removed by `deinit`.
    dir: []u8,
    port: u16 = 0,

    /// The fake provider, when a test asked for one. On the same loop.
    nntp: ?*tsnntp.Server = null,
    /// The generated release the provider serves, when a test asked for
    /// one. Owned; outlives a restart so the *original bytes* a delivery
    /// is compared against are the same on both sides of it.
    release: ?tsfixture.Fixture = null,
    /// Remembered so `restart` can boot the engine again if it was up.
    engine_wanted: bool = false,

    pub const Options = struct {
        /// Arms the heartbeat and housekeeping timers. Off by default:
        /// most tests want a quiet loop they fully control, and a
        /// one-second ticker firing mid-assertion only adds noise.
        timers: bool = false,
        /// The key `config` falls back to when the settings table holds
        /// none yet.
        ///
        /// A restart test has to be able to change this: with a fixed
        /// fallback, "the key survived the restart" would pass even if
        /// nothing had been persisted, because the fallback would
        /// produce the same value both times.
        api_key_fallback: []const u8 = api_key,
        /// Starts the download engine — the job fibers, the pools, and
        /// the inline outbox dispatcher that carries verify, repair,
        /// extract and deliver. Off for the tests that only drive the
        /// API, so a stray pool dial cannot colour their results.
        engine: bool = false,
    };

    /// Boots a daemon against a fresh temporary directory.
    ///
    /// `label` only ends up in the directory name, to make a leftover
    /// tree after a crash identifiable.
    pub fn init(gpa: Allocator, label: []const u8, options: Options) !*Harness {
        const dir = try tempDir(gpa, label);
        errdefer gpa.free(dir);
        errdefer removeTree(gpa, dir);

        try sys.mkdirPath(dir);

        const self = try gpa.create(Harness);
        errdefer gpa.destroy(self);
        self.* = .{ .gpa = gpa, .app = undefined, .dir = dir };

        self.app = try gpa.create(bootstrap.App);
        errdefer gpa.destroy(self.app);

        try self.boot(options);
        // After `boot`, because the engine loads its pools from the
        // servers table and that only exists once the database is open.
        if (options.engine) try self.startEngine();
        return self;
    }

    /// Opens the database and wires the graph. Split out of `init` so
    /// `restart` can run exactly the same sequence a second time.
    fn boot(self: *Harness, options: Options) !void {
        const gpa = self.gpa;
        const app = self.app;
        app.* = .{ .gpa = gpa };

        app.cfg = try config.loadFromBytes(gpa, null, self.dir, .{
            .api_key_fallback = options.api_key_fallback,
        });
        errdefer app.cfg.deinit();

        try app.loop.init(gpa);
        errdefer app.loop.deinit();

        // `config.normalize` resolves the default `./data` against the
        // base directory, so the resolved one is what has to exist —
        // the same distinction `bootstrap.run` makes, for the same
        // reason.
        try sys.mkdirPath(app.cfg.config.server.data_dir);

        var db_buf: [sys.path_max]u8 = undefined;
        const db_path = try sys.joinZ(&db_buf, app.cfg.config.server.data_dir, "hoardarr.db");
        app.db = try sqlite.Conn.open(gpa, db_path, .{});
        errdefer app.db.close();
        try migrate.migrate(app.db);
        try seedIdOffsets(app.db);

        try app.wire();
        app.started = true;
        errdefer app.deinit();

        // `App.ensureDirs` is private to the composition root, so the
        // same directories are made here. Made at boot rather than
        // lazily for the reason the daemon does it: a test that asserts
        // on `incomplete/` should not be the thing that creates it.
        for ([_][]const u8{
            app.cfg.config.paths.incomplete_dir,
            app.cfg.config.paths.complete_dir,
        }) |d| {
            if (d.len != 0) try sys.mkdirPath(d);
        }

        // Port 0 lets the kernel choose. Two runs of the suite in
        // parallel therefore cannot collide.
        try app.server.listen(try bootstrap.parseListen("127.0.0.1:0"));
        self.port = try app.server.listener.boundPort();

        if (options.timers) try app.startTimers();
    }

    /// Starts the download engine. Separate from `boot` because a test
    /// that needs a provider has to register the servers row first, and
    /// the engine reads that table when it starts.
    pub fn startEngine(self: *Harness) !void {
        try self.app.startEngine();
        self.engine_wanted = true;
    }

    pub fn deinit(self: *Harness) void {
        // The provider goes first: its listener and its sessions are
        // sources on the daemon's loop, and `App.deinit` ends with
        // `loop.deinit()`. The client side needs no live peer to be torn
        // down — `App.deinit` cancels each job fiber, which hands its
        // connection back to a pool that then closes the socket.
        if (self.nntp) |s| {
            s.deinit();
            self.gpa.destroy(s);
            self.nntp = null;
        }
        self.app.deinit();
        self.gpa.destroy(self.app);
        if (self.release) |*r| r.deinit();
        removeTree(self.gpa, self.dir);
        self.gpa.free(self.dir);
        self.gpa.destroy(self);
    }

    /// Tears the daemon down and boots a new one over the same data
    /// directory — the same database file, the same WAL, the same
    /// incomplete tree.
    ///
    /// This is the only honest way to test restart behaviour: nothing
    /// is carried over in memory, so anything the second daemon knows
    /// it read back off disk. The daemon's own HTTP port changes,
    /// because the kernel picks a new one.
    ///
    /// The fake provider is torn down and rebound to the *same* port,
    /// because a `servers` row read back from the database names a port
    /// and it has to still be there. Its corpus does not survive — the
    /// caller re-registers the articles, which is also what would
    /// happen if the provider had been restarted alongside us.
    pub fn restart(self: *Harness, options_in: Options) !void {
        const want_engine = options_in.engine or self.engine_wanted;
        var options = options_in;
        options.engine = false;

        var nntp_opts: ?tsnntp.Options = null;
        if (self.nntp) |s| {
            var o = s.opts;
            o.port = try s.port();
            nntp_opts = o;
            s.deinit();
            self.gpa.destroy(s);
            self.nntp = null;
        }

        self.app.deinit();
        try self.boot(options);

        if (nntp_opts) |o| {
            _ = try self.startNntp(o);
            // The corpus does not survive `stop`, so it is re-registered
            // from the release the harness still owns. The article
            // *counters* deliberately start from zero, which is what
            // lets a caller ask "how much did the second daemon have to
            // fetch" and get an answer that means something.
            try self.serveRelease();
        }
        if (want_engine) try self.startEngine();
    }

    // -- the fake provider ---------------------------------------------

    /// Starts the content-addressed NNTP server on the daemon's own
    /// loop. Returns its port.
    pub fn startNntp(self: *Harness, opts: tsnntp.Options) !u16 {
        std.debug.assert(self.nntp == null);
        const s = try self.gpa.create(tsnntp.Server);
        errdefer self.gpa.destroy(s);
        const p = try s.start(self.gpa, &self.app.loop, opts);
        self.nntp = s;
        return p;
    }

    pub fn provider(self: *Harness) *tsnntp.Server {
        return self.nntp.?;
    }

    /// Generates a release, starts a provider serving it, writes the
    /// `servers` row that names that provider, and starts the engine.
    ///
    /// One call because the order matters and getting it wrong fails in
    /// a way that reads like a pipeline bug: the engine loads its pools
    /// from the servers table when it starts, so a row written
    /// afterwards would leave the job parked in `waiting_for_server`.
    pub fn withRelease(
        self: *Harness,
        release: tsfixture.Options,
        net: tsnntp.Options,
    ) !void {
        std.debug.assert(self.release == null);
        self.release = try tsfixture.generate(self.gpa, release);

        const port = try self.startNntp(net);
        try self.serveRelease();

        // The row carries whatever the fake provider demands, so a test
        // that asks for a provider requiring credentials gets a daemon
        // configured to present them — and one that does not gets a
        // daemon that must not send AUTHINFO at all.
        var row = try dserver.UsenetServer.init(self.gpa, .{
            .name = "stub",
            .host = "127.0.0.1",
            .port = @intCast(port),
            .tls = false,
            .max_conns = 4,
            .username = net.username,
            .password = net.password,
        }, infra.nowMillis());
        defer row.deinit();
        try repo_server.ServerRepo.init(self.gpa, self.app.db).save(&row);

        try self.startEngine();
    }

    /// Registers every article of the release with the running provider.
    pub fn serveRelease(self: *Harness) !void {
        // Captured by pointer: a `Fixture` owns an arena, and a by-value
        // copy of one is a second owner of the same allocator state.
        if (self.release) |*r| try self.serveFixture(r);
    }

    /// The same for a release the *caller* owns.
    ///
    /// The harness holds one release because almost every test wants
    /// exactly one. A test that needs several jobs in flight needs
    /// several *distinct* releases — `add_job` dedupes on the SHA-256 of
    /// the NZB bytes and hands back the existing job's id, so N adds of
    /// one release are one job — and it owns them itself rather than the
    /// harness growing a collection for one caller.
    pub fn serveFixture(self: *Harness, r: *const tsfixture.Fixture) !void {
        const s = self.nntp orelse return;
        for (r.articles) |a| try s.addArticle(a.message_id, a.body);
    }

    /// Queues the generated release through the same REST port the API
    /// uses, and returns its job id.
    pub fn addRelease(self: *Harness, name: []const u8) !i64 {
        if (self.release) |*r| return self.addFixture(r, name);
        return error.NoRelease;
    }

    /// `addRelease` against a caller-owned release.
    pub fn addFixture(self: *Harness, r: *const tsfixture.Fixture, name: []const u8) !i64 {
        try self.nudgeJobIds();
        const added = try self.app.p_queue.port().add(self.app.api.beginRequest(), .{
            .nzb = r.nzb,
            .name = name,
            // No category, so the release lands directly under
            // `complete/` and the expected path stays a constant.
            .category = "",
            .source = "e2e",
        });
        try self.dropJobIdSentinel();
        return added.id;
    }

    /// Pushes the `jobs` rowid counter past `id_offset.job` before the
    /// first job is created.
    ///
    /// A parked row, unlike the other three contexts, cannot stay: every
    /// queue and history listing reads the table unfiltered, so a
    /// sentinel job would show up in the API a test is asserting on. It
    /// only has to outlive the insert that follows it — after that the
    /// real job is the maximum and the counter never falls back.
    fn nudgeJobIds(self: *Harness) !void {
        const max = try self.app.db.scalarIntOr("SELECT COALESCE(MAX(id), 0) FROM jobs", .{}, 0);
        if (max >= id_offset.job) return;
        try self.app.db.execute(
            \\INSERT INTO jobs(id, nzb_hash, name, queue_order, state, total_bytes, added_at, nzb_blob)
            \\VALUES (?, 'id-offset-sentinel', 'id offset sentinel', 0, 'failed', 0, 0, x'')
        , .{id_offset.job});
    }

    fn dropJobIdSentinel(self: *Harness) !void {
        try self.app.db.execute(
            "DELETE FROM jobs WHERE id = ? AND nzb_hash = 'id-offset-sentinel'",
            .{id_offset.job},
        );
    }

    /// The post-download aggregates a job produced. Zero where that
    /// context never ran for the job.
    pub const AggregateIds = struct {
        job: i64,
        verify_set: i64,
        repair: i64,
        delivery: i64,
    };

    pub fn aggregateIds(self: *Harness, job_id: i64) !AggregateIds {
        return .{
            .job = job_id,
            .verify_set = try self.app.db.scalarIntOr(
                "SELECT id FROM par2_sets WHERE job_id = ?",
                .{job_id},
                0,
            ),
            .repair = try self.app.db.scalarIntOr(
                "SELECT id FROM repairs WHERE job_id = ?",
                .{job_id},
                0,
            ),
            .delivery = try self.app.db.scalarIntOr(
                "SELECT id FROM deliveries WHERE job_id = ?",
                .{job_id},
                0,
            ),
        };
    }

    /// Asserts that the ids a test is about to route events by are
    /// genuinely telling apart — the precondition every "the right job
    /// got the event" assertion silently depends on. A context that did
    /// not run for this job (id 0) is skipped rather than failed.
    pub fn expectIdsDistinct(self: *Harness, job_id: i64) !void {
        const ids = try self.aggregateIds(job_id);
        const named = [_]struct { name: []const u8, id: i64 }{
            .{ .name = "job", .id = ids.job },
            .{ .name = "verify set", .id = ids.verify_set },
            .{ .name = "repair", .id = ids.repair },
            .{ .name = "delivery", .id = ids.delivery },
        };
        for (named, 0..) |a, i| {
            if (a.id == 0) continue;
            if (a.id == 1) {
                std.debug.print("\nthe {s} id is 1; the offsets did not take\n", .{a.name});
                return error.AggregateIdsCoincide;
            }
            for (named[i + 1 ..]) |b| {
                if (b.id == 0) continue;
                if (a.id != b.id) continue;
                std.debug.print(
                    "\nthe {s} id and the {s} id are both {d}; a stage routing on the wrong one would pass\n",
                    .{ a.name, b.name, a.id },
                );
                return error.AggregateIdsCoincide;
            }
        }
    }

    // -- job progress ----------------------------------------------------

    pub fn jobState(self: *Harness, id: i64) !JobState {
        const repo = repo_download.JobRepo.init(self.gpa, self.app.db);
        var job = try repo.byId(self.gpa, id);
        defer job.deinit();
        return job.state;
    }

    /// How many of the job's segments are in `state`, read straight from
    /// the table rather than from the in-memory aggregate — which is the
    /// only reading that means anything to a restart test.
    pub fn segmentsIn(self: *Harness, id: i64, state: SegmentState) !i64 {
        return self.app.db.scalarInt(
            \\SELECT count(*) FROM segments s
            \\JOIN files f ON f.id = s.file_id
            \\WHERE f.job_id = ? AND s.state = ?
        , .{ id, @tagName(state) });
    }

    pub fn segmentCount(self: *Harness, id: i64) !i64 {
        return self.app.db.scalarInt(
            \\SELECT count(*) FROM segments s
            \\JOIN files f ON f.id = s.file_id
            \\WHERE f.job_id = ?
        , .{id});
    }

    /// Advances the loop until the job reaches a terminal state.
    ///
    /// Bounded rather than open-ended: a pipeline that wedges must fail
    /// in seconds rather than hang the suite until CI kills it.
    pub fn runUntilTerminal(self: *Harness, id: i64, max_ms: u64) !JobState {
        const deadline = sys.monotonicNanos() + max_ms * std.time.ns_per_ms;
        while (sys.monotonicNanos() < deadline) {
            _ = try self.app.loop.tick(5);
            const state = try self.jobState(id);
            if (state.isTerminal()) return state;
        }
        // A wedged pipeline is diagnosed from *where* it stopped, and a
        // bare `PipelineDidNotFinish` says only that it did. The state
        // and the durable timeline are the two things that identify the
        // stage that never ran.
        std.debug.print("\nthe job is stuck in {s}; its timeline:\n", .{
            (try self.jobState(id)).toString(),
        });
        var timeline = try self.app.bus.eventsByJob(self.app.db, self.gpa, id);
        defer timeline.deinit();
        for (timeline.items.items) |env| std.debug.print("  {s}\n", .{env.topic});
        return error.PipelineDidNotFinish;
    }

    /// Advances the loop until the job reaches exactly `want`, which is
    /// how a non-terminal milestone (`download_complete`, `paused`) is
    /// waited on without sleeping.
    pub fn runUntilState(self: *Harness, id: i64, want: JobState, max_ms: u64) !void {
        const deadline = sys.monotonicNanos() + max_ms * std.time.ns_per_ms;
        while (sys.monotonicNanos() < deadline) {
            _ = try self.app.loop.tick(5);
            const state = try self.jobState(id);
            if (state == want) return;
            if (state.isTerminal() and state != want) {
                std.debug.print("\njob reached the terminal state {s}; wanted {s}\n", .{
                    state.toString(), want.toString(),
                });
                return error.WrongTerminalState;
            }
        }
        return error.StateNotReached;
    }

    /// Whether the job's durable timeline already carries `topic`.
    ///
    /// The outbox is the only record that survives a crash, so "has this
    /// stage started" is asked of it rather than of anything in memory —
    /// which is also what lets a test crash the daemon at an exact point
    /// in the pipeline rather than at an approximate time.
    pub fn hasEvent(self: *Harness, id: i64, topic: []const u8) !bool {
        var timeline = try self.app.bus.eventsByJob(self.app.db, self.gpa, id);
        defer timeline.deinit();
        for (timeline.items.items) |env| {
            if (std.mem.eql(u8, env.topic, topic)) return true;
        }
        return false;
    }

    /// Everything the delivery left in `complete/<name>/`.
    pub fn deliveredFiles(self: *Harness, a: Allocator, name: []const u8) ![]const []const u8 {
        const dir = try std.fmt.allocPrint(a, "{s}/{s}", .{ self.completeDir(), name });
        var fs_impl = infra.RealFs{ .gpa = self.gpa };
        const entries = try fs_impl.filesystem().list(a, dir);
        var out: std.ArrayList([]const u8) = .empty;
        for (entries) |e| {
            if (e.is_dir) continue;
            try out.append(a, try std.fmt.allocPrint(a, "{s}/{s}", .{ dir, e.name }));
        }
        return out.items;
    }

    /// Every delivered data file compared byte for byte against the
    /// fixture's original, and nothing delivered that should not be.
    ///
    /// "The job says complete" is exactly the assertion that passes
    /// while the output is corrupt, which is why this — not the state —
    /// is what a download test ends on.
    pub fn expectDeliveredMatchesRelease(self: *Harness, name: []const u8) !void {
        if (self.release) |*r| return self.expectDeliveredMatches(r, name);
        return error.NoRelease;
    }

    /// `expectDeliveredMatchesRelease` against a caller-owned release.
    pub fn expectDeliveredMatches(
        self: *Harness,
        r: *const tsfixture.Fixture,
        name: []const u8,
    ) !void {
        var arena = std.heap.ArenaAllocator.init(self.gpa);
        defer arena.deinit();
        const a = arena.allocator();

        const delivered = try self.deliveredFiles(a, name);

        // The parity volumes are scratch and must not be delivered.
        var expected: usize = 0;
        for (r.files) |f| {
            if (f.is_data) expected += 1;
        }
        if (delivered.len != expected) {
            std.debug.print("\ndelivered {d} files; want {d}:\n", .{ delivered.len, expected });
            for (delivered) |p| std.debug.print("  {s}\n", .{p});
            return error.WrongDeliveredFileCount;
        }

        for (delivered) |path| {
            const base = std.fs.path.basename(path);
            const original = r.fileBytes(base) orelse {
                std.debug.print("\ndelivered an unexpected file: {s}\n", .{base});
                return error.UnexpectedDeliveredFile;
            };
            const landed = try readFile(a, path);
            expectBytesEqual(original, landed) catch |e| {
                std.debug.print("delivered file {s} does not match the original\n", .{base});
                return e;
            };
        }
    }

    // -- driving the loop ----------------------------------------------

    /// Advances the reactor until `predicate` returns true or the
    /// deadline passes.
    ///
    /// The 10ms tick bound is a ceiling on how long one `tick` may
    /// block, not a poll interval: a ready socket wakes it immediately.
    /// It exists so a predicate that depends on a timer rather than on
    /// I/O still gets re-checked.
    pub fn pumpUntil(
        self: *Harness,
        ctx: anytype,
        comptime predicate: fn (@TypeOf(ctx)) bool,
        deadline_ns: u64,
    ) Error!void {
        const start = sys.monotonicNanos();
        while (!predicate(ctx)) {
            if (sys.monotonicNanos() -| start > deadline_ns) return error.Timeout;
            _ = self.app.loop.tick(10) catch return error.Timeout;
        }
    }

    /// Advances the loop `n` times regardless of any condition. For the
    /// rare case where a test needs the daemon to notice something it
    /// cannot observe from outside; prefer `pumpUntil`.
    pub fn pump(self: *Harness, n: usize) void {
        var i: usize = 0;
        while (i < n) : (i += 1) _ = self.app.loop.tick(1) catch return;
    }

    // -- HTTP ------------------------------------------------------------

    /// A completed response, owned by the caller.
    pub const Reply = struct {
        gpa: Allocator,
        status: u16 = 0,
        body: []u8 = &.{},
        /// First `Set-Cookie` header, copied. Empty when absent.
        set_cookie: []u8 = &.{},
        done: bool = false,
        failed: ?anyerror = null,

        pub fn deinit(self: *Reply) void {
            self.gpa.free(self.body);
            self.gpa.free(self.set_cookie);
            self.* = undefined;
        }

        /// The value of the session cookie in `Set-Cookie`, before the
        /// first `;`. Empty when the header did not name that cookie.
        pub fn sessionCookie(self: *const Reply) []const u8 {
            const prefix = "hoardarr_session=";
            if (!std.mem.startsWith(u8, self.set_cookie, prefix)) return "";
            const rest = self.set_cookie[prefix.len..];
            const end = std.mem.indexOfScalar(u8, rest, ';') orelse rest.len;
            return rest[0..end];
        }

        pub fn contains(self: *const Reply, needle: []const u8) bool {
            return std.mem.indexOf(u8, self.body, needle) != null;
        }

        /// The raw JSON value for a top-level-ish `"key":` in the body,
        /// with surrounding quotes stripped for a string.
        ///
        /// Deliberately a scanner rather than a parser: these tests
        /// assert on a handful of scalars, and a scanner cannot claim a
        /// field parsed correctly when the daemon emitted something
        /// structurally different — it just fails to find it.
        pub fn field(self: *const Reply, key: []const u8) ?[]const u8 {
            return jsonField(self.body, key);
        }

        pub fn expectField(self: *const Reply, key: []const u8, want: []const u8) !void {
            const got = self.field(key) orelse {
                std.debug.print("\nfield '{s}' absent from: {s}\n", .{ key, self.body });
                return error.FieldMissing;
            };
            if (!std.mem.eql(u8, got, want)) {
                std.debug.print("\nfield '{s}' = '{s}'; want '{s}'\n", .{ key, got, want });
                return error.FieldMismatch;
            }
        }

        fn onComplete(ctx: ?*anyopaque, result: client.Error!client.Response) void {
            const self: *Reply = @ptrCast(@alignCast(ctx.?));
            self.done = true;
            var res = result catch |e| {
                self.failed = e;
                return;
            };
            defer res.deinit();
            self.status = res.status;
            self.body = self.gpa.dupe(u8, res.body) catch &.{};
            if (res.get("set-cookie")) |c| {
                self.set_cookie = self.gpa.dupe(u8, c) catch &.{};
            }
        }
    };

    pub const Req = struct {
        method: client.Method = .get,
        path: []const u8 = "/",
        body: []const u8 = "",
        content_type: []const u8 = "application/json",
        /// Sent as `X-Api-Key`. Empty sends no key at all, which is how
        /// the unauthenticated cases are expressed.
        api_key: []const u8 = api_key,
        /// Sent as `Cookie: hoardarr_session=<value>`.
        session: []const u8 = "",
        extra: []const client.Header = &.{},
    };

    /// One request/response against the daemon, driven on the shared
    /// loop. The caller owns the reply.
    pub fn request(self: *Harness, req: Req) !Reply {
        var reply: Reply = .{ .gpa = self.gpa };
        errdefer reply.deinit();

        var cookie_buf: [256]u8 = undefined;
        var headers: [8]client.Header = undefined;
        var n: usize = 0;
        if (req.api_key.len > 0) {
            headers[n] = .{ .name = "X-Api-Key", .value = req.api_key };
            n += 1;
        }
        if (req.session.len > 0) {
            const v = try std.fmt.bufPrint(&cookie_buf, "hoardarr_session={s}", .{req.session});
            headers[n] = .{ .name = "Cookie", .value = v };
            n += 1;
        }
        for (req.extra) |h| {
            headers[n] = h;
            n += 1;
        }

        var ex: client.Exchange = undefined;
        try ex.start(
            self.gpa,
            &self.app.loop,
            try std.Io.net.IpAddress.parse("127.0.0.1", self.port),
            .{
                .method = req.method,
                .path = req.path,
                .host = "127.0.0.1",
                .headers = headers[0..n],
                .body = req.body,
                .content_type = req.content_type,
            },
            .{ .deadline_ns = default_deadline_ns },
            &Reply.onComplete,
            &reply,
        );
        defer ex.deinit();

        const Wait = struct {
            fn ready(r: *const Reply) bool {
                return r.done;
            }
        };
        try self.pumpUntil(&reply, Wait.ready, default_deadline_ns);
        if (reply.failed) |e| return e;
        return reply;
    }

    /// `request`, plus "and it answered 200".
    pub fn get(self: *Harness, path: []const u8) !Reply {
        var r = try self.request(.{ .path = path });
        errdefer r.deinit();
        try expectStatus(&r, 200, path);
        return r;
    }

    // -- filesystem ------------------------------------------------------

    /// A path inside this harness's data directory.
    pub fn dataPath(self: *Harness, gpa: Allocator, parts: []const []const u8) ![]u8 {
        var out: std.ArrayList(u8) = .empty;
        errdefer out.deinit(gpa);
        try out.appendSlice(gpa, self.app.cfg.config.server.data_dir);
        for (parts) |p| {
            try out.append(gpa, '/');
            try out.appendSlice(gpa, p);
        }
        return out.toOwnedSlice(gpa);
    }

    pub fn incompleteDir(self: *Harness) []const u8 {
        return self.app.cfg.config.paths.incomplete_dir;
    }

    pub fn completeDir(self: *Harness) []const u8 {
        return self.app.cfg.config.paths.complete_dir;
    }
};

// ---------------------------------------------------------------------
// Free helpers
// ---------------------------------------------------------------------

/// Parks one row in each post-download table so its rowid counter starts
/// past `id_offset`.
///
/// The rows have to stay there. SQLite hands out `max(rowid) + 1` over
/// the rows that currently exist, so deleting a sentinel would hand the
/// offset straight back to the next insert. They are attached to a job id
/// no job can have, with foreign keys off for the insert alone, and each
/// is in a terminal state — the recovery scans look for pending and
/// in-flight rows, and nothing else reads these tables without a job id.
///
/// `INSERT OR IGNORE` because `restart` boots over the same file and must
/// find the offsets already in place rather than fail on the primary key.
fn seedIdOffsets(db: *sqlite.Conn) !void {
    try db.exec("PRAGMA foreign_keys = OFF");
    defer db.exec("PRAGMA foreign_keys = ON") catch {};

    try db.execute(
        \\INSERT OR IGNORE INTO par2_sets(id, job_id, state, error_msg)
        \\VALUES (?, ?, 'failed', 'id offset sentinel')
    , .{ id_offset.verify_set, sentinel_job });
    try db.execute(
        \\INSERT OR IGNORE INTO repairs(id, job_id, state, err_msg, created_at)
        \\VALUES (?, ?, 'failed', 'id offset sentinel', 0)
    , .{ id_offset.repair, sentinel_job });
    try db.execute(
        \\INSERT OR IGNORE INTO deliveries(id, job_id, state, err_msg, created_at)
        \\VALUES (?, ?, 'skipped', 'id offset sentinel', 0)
    , .{ id_offset.delivery, sentinel_job });
}

/// Whole file contents. Caller owns the bytes.
pub fn readFile(gpa: Allocator, path: []const u8) ![]u8 {
    var buf: [sys.path_max]u8 = undefined;
    const p = try sys.pathZ(&buf, path);
    const fd = try sys.open(p, .{ .mode = .read_only });
    defer sys.close(fd);

    const size = try sys.fileSize(fd);
    const out = try gpa.alloc(u8, @intCast(size));
    errdefer gpa.free(out);

    var off: usize = 0;
    while (off < out.len) {
        const got = try sys.read(fd, out[off..]);
        if (got == 0) break;
        off += got;
    }
    return out[0..off];
}

pub fn fileExists(path: []const u8) bool {
    var buf: [sys.path_max]u8 = undefined;
    const p = sys.pathZ(&buf, path) catch return false;
    return sys.exists(p);
}

/// Byte-for-byte, with a failure message that says *where* they diverge
/// rather than only that they did. A length-only report sends you
/// looking at the assembler when the bug is one flipped segment.
pub fn expectBytesEqual(want: []const u8, got: []const u8) !void {
    compareBytes(want, got) catch |e| {
        switch (e) {
            error.LengthMismatch => std.debug.print(
                "\nlength differs: got {d}, want {d}\n",
                .{ got.len, want.len },
            ),
            error.BytesDiffer => {
                const at = firstDifference(want, got).?;
                std.debug.print(
                    "\nbytes differ at offset {d}: got 0x{x:0>2}, want 0x{x:0>2}\n",
                    .{ at, got[at], want[at] },
                );
            },
        }
        return e;
    };
}

/// The comparison without the diagnostics, so the harness's own test of
/// the failure path does not print a scary block on a green run.
pub fn compareBytes(want: []const u8, got: []const u8) error{ LengthMismatch, BytesDiffer }!void {
    if (want.len != got.len) return error.LengthMismatch;
    if (firstDifference(want, got) != null) return error.BytesDiffer;
}

fn firstDifference(want: []const u8, got: []const u8) ?usize {
    for (want[0..@min(want.len, got.len)], 0..) |w, i| {
        if (w != got[i]) return i;
    }
    return null;
}

pub fn expectStatus(r: *const Harness.Reply, want: u16, what: []const u8) !void {
    if (r.status == want) return;
    std.debug.print("\n{s} answered {d}; want {d}. body: {s}\n", .{ what, r.status, want, r.body });
    return error.UnexpectedStatus;
}

/// The raw JSON value following `"key":`, quotes stripped for strings.
/// Returns null when the key is absent.
pub fn jsonField(body: []const u8, key: []const u8) ?[]const u8 {
    var pat_buf: [128]u8 = undefined;
    const pat = std.fmt.bufPrint(&pat_buf, "\"{s}\":", .{key}) catch return null;
    const at = std.mem.indexOf(u8, body, pat) orelse return null;
    var i = at + pat.len;
    while (i < body.len and (body[i] == ' ' or body[i] == '\t')) i += 1;
    if (i >= body.len) return null;

    if (body[i] == '"') {
        i += 1;
        const start = i;
        while (i < body.len and body[i] != '"') : (i += 1) {
            if (body[i] == '\\') i += 1;
        }
        return body[start..@min(i, body.len)];
    }
    const start = i;
    while (i < body.len and body[i] != ',' and body[i] != '}' and body[i] != ']') i += 1;
    return std.mem.trim(u8, body[start..i], " \t\r\n");
}

/// `/tmp/hoardarr-e2e-<label>-<pid>-<16 hex>`.
///
/// Never a fixed path. A run that dies partway leaves its tree behind,
/// and the next run then fails on that leftover rather than on whatever
/// actually broke — which costs an afternoon every time. The pid makes a
/// leftover attributable; the random suffix makes two runs of the same
/// binary, or two tests in one binary, unable to collide.
fn tempDir(gpa: Allocator, label: []const u8) ![]u8 {
    var raw: [8]u8 = undefined;
    sys.randomBytes(&raw);
    // `posix/sys.zig` keeps `getpid` private, so the process is stamped
    // once at first use instead. It only has to make a leftover
    // attributable to one run; the random suffix is what makes a
    // collision impossible.
    if (process_stamp == 0) process_stamp = @truncate(sys.monotonicNanos() | 1);
    return std.fmt.allocPrint(gpa, "/tmp/hoardarr-e2e-{s}-{x}-{x}", .{
        label,
        process_stamp,
        &raw,
    });
}

/// Stamped once per process, so every directory this binary makes shares
/// a prefix a human can grep for after a crash.
var process_stamp: u32 = 0;

fn removeTree(gpa: Allocator, dir: []const u8) void {
    var fs_impl = infra.RealFs{ .gpa = gpa };
    fs_impl.filesystem().removeAll(dir) catch {};
}

// ---------------------------------------------------------------------
// tests for the harness itself
// ---------------------------------------------------------------------

test "the harness boots a real daemon that serves its own health check" {
    const gpa = testing.allocator;
    var h = try Harness.init(gpa, "boot", .{});
    defer h.deinit();

    var r = try h.get("/healthz");
    defer r.deinit();
    try testing.expect(r.contains("ok"));
}

test "two harnesses do not share a directory or a port" {
    const gpa = testing.allocator;
    var a = try Harness.init(gpa, "iso-a", .{});
    defer a.deinit();
    var b = try Harness.init(gpa, "iso-b", .{});
    defer b.deinit();

    try testing.expect(!std.mem.eql(u8, a.dir, b.dir));
    try testing.expect(a.port != b.port);
}

test "the json scanner reads the shapes the daemon actually emits" {
    const body =
        \\{"status":"ok","count":12,"nested":{"name":"x y"},"flag":true}
    ;
    try testing.expectEqualStrings("ok", jsonField(body, "status").?);
    try testing.expectEqualStrings("12", jsonField(body, "count").?);
    try testing.expectEqualStrings("x y", jsonField(body, "name").?);
    try testing.expectEqualStrings("true", jsonField(body, "flag").?);
    try testing.expect(jsonField(body, "absent") == null);
}

test "pumpUntil reports a timeout rather than hanging the suite" {
    const gpa = testing.allocator;
    var h = try Harness.init(gpa, "timeout", .{});
    defer h.deinit();

    const Never = struct {
        fn no(_: *const u8) bool {
            return false;
        }
    };
    const dummy: u8 = 0;
    try testing.expectError(error.Timeout, h.pumpUntil(&dummy, Never.no, 20 * std.time.ns_per_ms));
}

test "byte comparison fails on a single flipped byte, not just on length" {
    var a = [_]u8{ 1, 2, 3, 4 };
    const b = [_]u8{ 1, 2, 3, 4 };
    try expectBytesEqual(&a, &b);
    a[2] = 9;
    // `compareBytes` rather than `expectBytesEqual`: the latter prints a
    // diagnostic, and a green run should not print one.
    try testing.expectError(error.BytesDiffer, compareBytes(&a, &b));
    try testing.expectError(error.LengthMismatch, compareBytes(a[0..3], &b));
}
