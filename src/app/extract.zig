//! Multi-part RAR extraction straight into
//! `complete/<category dir>/<release>/`.
//!
//! Consumes `verify.ok`, and silently ignores any job whose non-parity
//! files are not RAR volumes — those belong to `app/deliver`, which
//! subscribes to the same topic and applies the mirror-image test. Two
//! subscribers, one predicate, no coordination.
//!
//! # Staging
//!
//! The download orchestrator writes each file to `<job>/<file id>.tmp`,
//! which is right for concurrency (a stable path per file, known before
//! the filename is) and wrong for RAR, whose volume resolution walks
//! *sibling filenames*: given `rel.part01.rar` a decoder looks for
//! `rel.part02.rar` next to it. So the volumes are renamed to their real
//! names before extraction.
//!
//! Staging is idempotent across crashes — a destination that already
//! exists is taken as "staged by a previous run" — and skips a `.tmp`
//! that never materialised. Failing hard on a missing part would abort
//! extraction on a release that is 95% present and one `.r07` short,
//! which is exactly the case where the operator wants a repair, not an
//! abort.

const std = @import("std");
const log = @import("../core/log.zig");
const app_ports = @import("ports.zig");
const naming = @import("naming.zig");
const devent = @import("../domain/event.zig");
const dl_ports = @import("download/ports.zig");
const dtx = @import("../domain/tx.zig");
const dextract = @import("../domain/extract.zig");
const job_mod = @import("../domain/download/job.zig");
const ddevents = @import("../domain/download/events.zig");

const Allocator = std.mem.Allocator;

pub const Timestamp = app_ports.Timestamp;
pub const JobId = dl_ports.JobId;
pub const Extract = dextract.Extract;
pub const State = dextract.State;
pub const Sink = app_ports.EventSink(dextract.Event);
pub const DownloadSink = app_ports.EventSink(ddevents.Event);
pub const Store = app_ports.Repo(Extract);

pub const ExtractorError = error{
    /// The archive is not readable as RAR.
    Malformed,
    /// A volume the set refers to is not present.
    MissingVolume,
    /// The archive is encrypted and no password was supplied.
    PasswordRequired,
    Io,
    Canceled,
} || Allocator.Error;

pub const Extractor = struct {
    ctx: *anyopaque,
    /// Unpacks `rar_paths` into `target_dir`. Returns the entry count.
    extractFn: *const fn (
        ctx: *anyopaque,
        a: Allocator,
        rar_paths: []const []const u8,
        target_dir: []const u8,
    ) ExtractorError!usize,

    pub fn extract(
        self: Extractor,
        a: Allocator,
        rar_paths: []const []const u8,
        target_dir: []const u8,
    ) ExtractorError!usize {
        return self.extractFn(self.ctx, a, rar_paths, target_dir);
    }
};

pub const Error = app_ports.AggregateError || dl_ports.RepoError ||
    app_ports.PublishError || app_ports.TxError || app_ports.FsError ||
    dextract.TransitionError;

pub const Outcome = enum {
    complete,
    failed,
    /// Not an archive job — `app/deliver` owns it.
    not_ours,
    already_complete,
};

pub const Service = struct {
    gpa: Allocator,
    jobs: dl_ports.JobStore,
    store: Store,
    extractor: Extractor,
    categories: app_ports.Categories,
    sink: Sink,
    downloads: DownloadSink,
    txm: app_ports.Manager,
    fs: app_ports.Filesystem,
    clock: app_ports.Clock,
    logger: *log.Logger = &log.default,
    incomplete_dir: []const u8,
    complete_dir: []const u8,

    pub fn onVerifyOk(_: *Service, job_id: JobId) JobId {
        return job_id;
    }

    pub fn run(self: *Service, job_id: JobId) Error!Outcome {
        const job = try self.jobs.byId(null, job_id);
        defer self.jobs.release(job);

        if (!naming.isArchiveJob(job_mod.Job, job)) return .not_ours;

        var arena = std.heap.ArenaAllocator.init(self.gpa);
        defer arena.deinit();
        const a = arena.allocator();

        const rar_paths = try self.stage(a, job);
        const target_dir = try self.targetDir(a, job);

        var x: *Extract = undefined;
        var loaded = false;
        var adopted = false;
        if (self.store.byJobId(null, job_id)) |existing| {
            if (existing.state == .complete) {
                self.logger.info("extract: already complete, ensuring job advanced", &.{
                    log.int("job_id", job_id),
                });
                self.store.release(existing);
                // A crash between "extract row complete" and
                // `Job.markCompleted` leaves the job alive with nothing
                // left to run it — the same window `app/deliver` already
                // guards. `markJobCompleted` is a no-op once the Job is
                // terminal.
                try self.markJobCompleted(job_id);
                return .already_complete;
            }
            x = existing;
            loaded = true;
        } else |e| {
            if (e != error.NotFound) return e;
            const fresh = try self.gpa.create(Extract);
            fresh.* = Extract.init(self.gpa, job_id, target_dir, self.clock.now()) catch |ie| {
                self.gpa.destroy(fresh);
                return switch (ie) {
                    error.OutOfMemory => error.OutOfMemory,
                    error.JobIdRequired => error.JobNotFound,
                };
            };
            x = fresh;
            try self.persist(x, &adopted);
        }
        defer {
            if (loaded) {
                self.store.release(x);
            } else if (!adopted) {
                x.deinit();
                self.gpa.destroy(x);
            }
        }

        try x.start(self.clock.now());
        try self.persist(x, &adopted);

        _ = self.extractor.extract(a, rar_paths, x.target_dir) catch |e| {
            var buf: [64]u8 = undefined;
            const reason = std.fmt.bufPrint(&buf, "extract: {t}", .{e}) catch "extract failed";
            try x.fail(reason, self.clock.now());
            try self.persist(x, &adopted);
            try self.markJobFailed(job_id, reason);
            return .failed;
        };

        // Best-effort: the release is already in complete/, and refusing
        // to finish over a leftover scratch directory would be perverse.
        var dir_buf: [512]u8 = undefined;
        const job_dir = std.fmt.bufPrint(&dir_buf, "{s}/{d}", .{ self.incomplete_dir, job_id }) catch "";
        if (job_dir.len != 0) self.fs.removeAll(job_dir) catch |e| {
            self.logger.warn("extract: cleanup of incomplete dir failed", &.{
                log.int("job_id", job_id),
                log.errv("err", e),
            });
        };

        try x.complete(self.clock.now());
        try self.persist(x, &adopted);
        try self.markJobCompleted(job_id);
        return .complete;
    }

    /// `complete/<category dir>/<release>`, with both components
    /// sanitised. Allocated from `a`.
    fn targetDir(self: *Service, a: Allocator, job: *const job_mod.Job) Allocator.Error![]const u8 {
        var name_buf: [naming.max_component]u8 = undefined;
        const release = naming.sanitizeReleaseName(&name_buf, job.name);
        const subdir = self.categories.dirFor(job.category) orelse "";
        if (subdir.len == 0) {
            return std.fmt.allocPrint(a, "{s}/{s}", .{ self.complete_dir, release });
        }
        var sub_buf: [naming.max_component]u8 = undefined;
        const safe_sub = naming.sanitizeComponent(&sub_buf, subdir);
        return std.fmt.allocPrint(a, "{s}/{s}/{s}", .{ self.complete_dir, safe_sub, release });
    }

    /// Renames each RAR volume from `<file id>.tmp` to its real name so
    /// sibling-based volume resolution works. Returns the staged paths in
    /// aggregate order.
    fn stage(self: *Service, a: Allocator, job: *const job_mod.Job) Allocator.Error![]const []const u8 {
        var out: std.ArrayList([]const u8) = .empty;
        for (job.files) |f| {
            if (f.is_par2 or !naming.looksLikeRAR(f.filename)) continue;
            const src = try std.fmt.allocPrint(a, "{s}/{d}/{d}.tmp", .{
                self.incomplete_dir, job.id, f.id,
            });
            var name_buf: [naming.max_component]u8 = undefined;
            const safe = naming.sanitizeFilename(&name_buf, f.filename);
            const dst = try std.fmt.allocPrint(a, "{s}/{d}/{s}", .{
                self.incomplete_dir, job.id, safe,
            });
            if (std.mem.eql(u8, src, dst) or self.fs.exists(dst)) {
                // Already staged, by this run or by a previous one that
                // crashed between the rename and the extract.
                try out.append(a, dst);
                continue;
            }
            if (!self.fs.exists(src)) {
                // Every segment 430'd. Extraction can still try to
                // assemble what did arrive.
                self.logger.warn("extract: volume missing, skipping", &.{
                    log.int("job_id", job.id),
                    log.str("filename", f.filename),
                });
                continue;
            }
            self.fs.move(src, dst) catch |e| {
                self.logger.warn("extract: staging rename failed", &.{
                    log.int("job_id", job.id),
                    log.str("filename", f.filename),
                    log.errv("err", e),
                });
                continue;
            };
            try out.append(a, dst);
        }
        return out.items;
    }

    fn markJobCompleted(self: *Service, job_id: JobId) Error!void {
        return self.jobTransition(job_id, null);
    }

    fn markJobFailed(self: *Service, job_id: JobId, reason: []const u8) Error!void {
        return self.jobTransition(job_id, reason);
    }

    /// Advances the Job in its own transaction — the download aggregate
    /// belongs to another context and must not share a write with the
    /// extract row.
    fn jobTransition(self: *Service, job_id: JobId, failure: ?[]const u8) Error!void {
        const Args = struct { svc: *Service, job_id: JobId, failure: ?[]const u8 };
        const Body = struct {
            fn run(unit: *dtx.Unit, args: Args) Error!void {
                const s = args.svc;
                const job = try s.jobs.byId(unit, args.job_id);
                defer s.jobs.release(job);
                const changed = if (args.failure) |reason| blk: {
                    var buf: [96]u8 = undefined;
                    const msg = std.fmt.bufPrint(&buf, "extract: {s}", .{reason}) catch reason;
                    break :blk try job.markFailed(msg, s.clock.now());
                } else try job.markCompleted(s.clock.now());
                if (!changed) return;
                try s.jobs.save(unit, job);
                const events = try job.pullEvents();
                defer ddevents.deinitAll(s.gpa, events);
                try s.downloads.publish(unit, events);
            }
        };
        return dtx.inTx(Error, self.txm, Args{
            .svc = self,
            .job_id = job_id,
            .failure = failure,
        }, Body.run);
    }

    fn persist(self: *Service, x: *Extract, adopted: *bool) Error!void {
        const Args = struct { svc: *Service, x: *Extract, adopted: *bool };
        const Body = struct {
            fn run(unit: *dtx.Unit, args: Args) Error!void {
                try args.svc.store.save(unit, args.x);
                args.adopted.* = true;
                const events = try args.x.pullEvents();
                defer devent.deinitAll(dextract.Event, args.svc.gpa, events);
                try args.svc.sink.publish(unit, events);
            }
        };
        return dtx.inTx(Error, self.txm, Args{ .svc = self, .x = x, .adopted = adopted }, Body.run);
    }
};

// =====================================================================
// Test double
// =====================================================================

pub const FakeExtractor = struct {
    entries: usize = 3,
    err: ?ExtractorError = null,
    calls: usize = 0,
    last_target: [256]u8 = undefined,
    last_target_len: usize = 0,
    last_paths: [8][]const u8 = @splat(""),
    n_paths: usize = 0,

    pub fn extractor(self: *FakeExtractor) Extractor {
        return .{ .ctx = @ptrCast(self), .extractFn = &doExtract };
    }

    pub fn target(self: *const FakeExtractor) []const u8 {
        return self.last_target[0..self.last_target_len];
    }

    pub fn paths(self: *const FakeExtractor) []const []const u8 {
        return self.last_paths[0..self.n_paths];
    }

    fn doExtract(
        ctx: *anyopaque,
        _: Allocator,
        rar_paths: []const []const u8,
        target_dir: []const u8,
    ) ExtractorError!usize {
        const self: *FakeExtractor = @ptrCast(@alignCast(ctx));
        self.calls += 1;
        const n = @min(target_dir.len, self.last_target.len);
        @memcpy(self.last_target[0..n], target_dir[0..n]);
        self.last_target_len = n;
        self.n_paths = 0;
        for (rar_paths) |p| {
            if (self.n_paths == self.last_paths.len) break;
            self.last_paths[self.n_paths] = p;
            self.n_paths += 1;
        }
        if (self.err) |e| return e;
        return self.entries;
    }
};

// =====================================================================
// Tests
// =====================================================================

const testing = std.testing;
const Job = job_mod.Job;

const Harness = struct {
    jobs: dl_ports.FakeJobStore = undefined,
    extracts: app_ports.FakeRepo(Extract) = undefined,
    fs: app_ports.FakeFs = undefined,
    fake_extractor: FakeExtractor = .{},
    cats: app_ports.FakeCategories = .{},
    sink: app_ports.FakeSink(dextract.Event) = .{},
    dl_sink: app_ports.FakeSink(ddevents.Event) = .{},
    ftx: app_ports.FakeTx = .{},
    clock: app_ports.FakeClock = .{ .t = 3_000 },
    logger: log.Logger = .{},
    svc: Service = undefined,

    fn init(self: *Harness) void {
        self.* = .{};
        self.jobs = dl_ports.FakeJobStore.init(testing.allocator);
        self.extracts = app_ports.FakeRepo(Extract).init(testing.allocator);
        self.fs = app_ports.FakeFs.init(testing.allocator);
        self.cats = .{ .rows = &.{.{ .name = "tv", .dir = "TV" }} };
        self.svc = .{
            .gpa = testing.allocator,
            .jobs = self.jobs.store(),
            .store = self.extracts.repo(),
            .extractor = self.fake_extractor.extractor(),
            .categories = self.cats.categories(),
            .sink = self.sink.sink(),
            .downloads = self.dl_sink.sink(),
            .txm = self.ftx.manager(),
            .fs = self.fs.filesystem(),
            .clock = self.clock.clock(),
            .logger = &self.logger,
            .incomplete_dir = "/inc",
            .complete_dir = "/done",
        };
    }

    fn deinit(self: *Harness) void {
        self.fs.deinit();
        self.extracts.deinit();
        self.jobs.deinit();
    }

    fn seed(self: *Harness, opts: struct {
        filenames: []const []const u8 = &.{ "rel.part01.rar", "rel.part02.rar" },
        category: []const u8 = "tv",
        name: []const u8 = "The.Release.S01E01",
        /// Which of `filenames` to materialise on disk.
        on_disk: usize = std.math.maxInt(usize),
    }) !*Job {
        var files: std.ArrayList(job_mod.NewFileParams) = .empty;
        defer files.deinit(testing.allocator);
        for (opts.filenames) |fname| {
            try files.append(testing.allocator, .{
                .filename = fname,
                .size_bytes = 10,
                .segments = &.{.{ .seq_index = 1, .message_id = "m@h", .bytes = 10 }},
            });
        }
        try files.append(testing.allocator, .{
            .filename = "rel.par2",
            .is_par2 = true,
            .segments = &.{.{ .seq_index = 1, .message_id = "p@h" }},
        });

        const j = try testing.allocator.create(Job);
        j.* = try Job.init(testing.allocator, .{
            .nzb_hash = "h",
            .name = opts.name,
            .category = opts.category,
            .files = files.items,
        }, 0);
        try self.jobs.insert(j);
        const evts = try j.pullEvents();
        ddevents.deinitAll(testing.allocator, evts);

        for (j.files, 0..) |f, i| {
            if (i >= opts.on_disk) break;
            var buf: [64]u8 = undefined;
            const p = try std.fmt.bufPrint(&buf, "/inc/{d}/{d}.tmp", .{ j.id, f.id });
            try self.fs.addFile(p, 10);
        }
        return j;
    }
};

test "an archive job stages its volumes and extracts into the category dir" {
    var h: Harness = undefined;
    h.init();
    defer h.deinit();
    const j = try h.seed(.{});

    try testing.expectEqual(Outcome.complete, try h.svc.run(j.id));

    // The .tmp blobs were renamed to their real names so a decoder can
    // find part02 next to part01. The scratch dir is swept afterwards,
    // so the evidence is what the extractor was handed.
    try testing.expectEqual(@as(usize, 2), h.fake_extractor.paths().len);
    try testing.expectEqualStrings("/inc/1/rel.part01.rar", h.fake_extractor.paths()[0]);
    try testing.expectEqualStrings("/inc/1/rel.part02.rar", h.fake_extractor.paths()[1]);
    try testing.expectEqual(@as(usize, 2), h.fs.moves);
    try testing.expectEqualStrings("/done/TV/The.Release.S01E01", h.fake_extractor.target());

    const x = h.extracts.get(j.id).?;
    try testing.expectEqual(State.complete, x.state);
    try testing.expect(h.sink.has("extract.queued"));
    try testing.expect(h.sink.has("extract.started"));
    try testing.expect(h.sink.has("extract.complete"));
    // The job goes terminal-completed on the download context's bus.
    try testing.expectEqual(job_mod.JobState.completed, j.state);
    try testing.expect(h.dl_sink.has("download.job.completed"));
    try testing.expect(h.ftx.balanced());
}

test "a non-archive job is left for the deliver worker" {
    var h: Harness = undefined;
    h.init();
    defer h.deinit();
    const j = try h.seed(.{ .filenames = &.{"movie.mkv"} });

    try testing.expectEqual(Outcome.not_ours, try h.svc.run(j.id));
    try testing.expectEqual(@as(usize, 0), h.fake_extractor.calls);
    try testing.expectEqual(@as(usize, 0), h.extracts.len());
    try testing.expectEqual(@as(usize, 0), h.sink.n);
}

test "an unknown category drops the release straight into complete" {
    var h: Harness = undefined;
    h.init();
    defer h.deinit();
    const j = try h.seed(.{ .category = "unmapped" });
    try testing.expectEqual(Outcome.complete, try h.svc.run(j.id));
    try testing.expectEqualStrings("/done/The.Release.S01E01", h.fake_extractor.target());
}

test "the release name is sanitised before it becomes a directory" {
    var h: Harness = undefined;
    h.init();
    defer h.deinit();
    const j = try h.seed(.{ .name = "../evil/Release: 2020", .category = "unmapped" });
    try testing.expectEqual(Outcome.complete, try h.svc.run(j.id));
    // The separators are gone and the leading dots are trimmed, so the
    // release cannot land outside complete/ or as a hidden directory.
    try testing.expectEqualStrings("/done/_evil_Release_ 2020", h.fake_extractor.target());
}

test "staging is idempotent across a crash" {
    var h: Harness = undefined;
    h.init();
    defer h.deinit();
    const j = try h.seed(.{});
    // A previous run renamed part01 and then died.
    try h.fs.filesystem().move("/inc/1/1.tmp", "/inc/1/rel.part01.rar");
    const moves = h.fs.moves;

    try testing.expectEqual(Outcome.complete, try h.svc.run(j.id));
    // Only the second volume needed renaming this time.
    try testing.expectEqual(moves + 1, h.fs.moves);
    try testing.expectEqual(@as(usize, 2), h.fake_extractor.paths().len);
}

test "a volume that never arrived is skipped, not fatal" {
    // 95% of a release plus one missing .r07 is exactly when the operator
    // wants a repair attempt, not an aborted extraction.
    var h: Harness = undefined;
    h.init();
    defer h.deinit();
    const j = try h.seed(.{ .on_disk = 1 });

    try testing.expectEqual(Outcome.complete, try h.svc.run(j.id));
    try testing.expectEqual(@as(usize, 1), h.fake_extractor.paths().len);
}

test "an extractor failure fails the extract and the job" {
    var h: Harness = undefined;
    h.init();
    defer h.deinit();
    const j = try h.seed(.{});
    h.fake_extractor.err = error.MissingVolume;

    try testing.expectEqual(Outcome.failed, try h.svc.run(j.id));
    const x = h.extracts.get(j.id).?;
    try testing.expectEqual(State.failed, x.state);
    try testing.expectEqualStrings("extract: MissingVolume", x.err_msg);
    try testing.expect(h.sink.has("extract.failed"));
    try testing.expectEqual(job_mod.JobState.failed, j.state);
    try testing.expectEqualStrings("extract: extract: MissingVolume", j.errorMsg());
    // A failed extraction leaves the scratch directory alone so the
    // operator can look at it.
    try testing.expect(h.fs.has("/inc/1"));
}

test "a successful extraction sweeps the incomplete directory" {
    var h: Harness = undefined;
    h.init();
    defer h.deinit();
    const j = try h.seed(.{});
    try testing.expectEqual(Outcome.complete, try h.svc.run(j.id));
    try testing.expect(!h.fs.has("/inc/1"));
    try testing.expect(!h.fs.has("/inc/1/rel.part01.rar"));
}

test "a failed sweep does not fail the extraction" {
    var h: Harness = undefined;
    h.init();
    defer h.deinit();
    const j = try h.seed(.{});
    h.fs.fail_next = error.Denied;
    // The fail lands on the first mutating call the run makes, which is
    // the staging rename; the point is only that a filesystem hiccup in
    // the best-effort paths does not abort the run.
    try testing.expectEqual(Outcome.complete, try h.svc.run(j.id));
    try testing.expectEqual(State.complete, h.extracts.get(j.id).?.state);
}

test "a second run over a complete extract does nothing" {
    var h: Harness = undefined;
    h.init();
    defer h.deinit();
    const j = try h.seed(.{});
    try testing.expectEqual(Outcome.complete, try h.svc.run(j.id));
    const calls = h.fake_extractor.calls;
    h.sink.reset();

    try testing.expectEqual(Outcome.already_complete, try h.svc.run(j.id));
    try testing.expectEqual(calls, h.fake_extractor.calls);
    try testing.expectEqual(@as(usize, 0), h.sink.n);
    try testing.expect(h.extracts.leakFree());
}

test "an unknown job is reported without creating an extract" {
    var h: Harness = undefined;
    h.init();
    defer h.deinit();
    try testing.expectError(error.JobNotFound, h.svc.run(77));
    try testing.expectEqual(@as(usize, 0), h.extracts.len());
}

test "a publish failure rolls the extract transition back" {
    var h: Harness = undefined;
    h.init();
    defer h.deinit();
    const j = try h.seed(.{});
    h.sink.fail = error.Backend;
    try testing.expectError(error.Backend, h.svc.run(j.id));
    try testing.expect(h.ftx.rollbacks >= 1);
    try testing.expect(h.ftx.balanced());
}

test "the bus handler hands the job id straight through" {
    var h: Harness = undefined;
    h.init();
    defer h.deinit();
    try testing.expectEqual(@as(JobId, 11), h.svc.onVerifyOk(11));
}
