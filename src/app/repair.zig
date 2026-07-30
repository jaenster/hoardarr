//! Reed-Solomon reconstruction of the files verification found damaged.
//!
//! Consumes `verify.repair_needed`, emits `repair.ok` or `repair.failed`.
//! On success the verify worker (which also subscribes to `repair.ok`)
//! re-runs verification, and the resulting `verify.ok` is what deliver
//! and extract are waiting for — no worker ever calls another directly.
//!
//! # On-demand recovery volumes
//!
//! The interesting path. When a job was added with recovery volumes
//! deferred, the PAR2 set on disk is the index alone: enough to *detect*
//! damage, not to repair it. If reconstruction comes up short of slices
//! and the job still has unfetched volumes, this service does not fail —
//! it flips `fetch_recovery_vols` on the Job, publishes
//! `RecoveryVolsRequested`, and parks. The orchestrator re-enters its
//! runner, the volumes land, a second `JobDownloadComplete` triggers
//! verification again, and repair gets another turn with the slices it
//! needed.
//!
//! Leaving the `Repair` in `running` while parked is deliberate: marking
//! it failed would publish `repair.failed` and take the job terminal
//! while the fix is already in flight.

const std = @import("std");
const log = @import("../core/log.zig");
const app_ports = @import("ports.zig");
const devent = @import("../domain/event.zig");
const dl_ports = @import("download/ports.zig");
const dtx = @import("../domain/tx.zig");
const drepair = @import("../domain/repair.zig");
const dverify = @import("../domain/verify.zig");
const job_mod = @import("../domain/download/job.zig");
const ddevents = @import("../domain/download/events.zig");
const verify_app = @import("verify.zig");

const Allocator = std.mem.Allocator;

pub const Timestamp = app_ports.Timestamp;
pub const JobId = dl_ports.JobId;
pub const Repair = drepair.Repair;
pub const State = drepair.State;
pub const DataPath = verify_app.DataPath;
pub const Sink = app_ports.EventSink(drepair.Event);
pub const DownloadSink = app_ports.EventSink(ddevents.Event);
pub const Store = app_ports.Repo(Repair);

/// Why a reconstruction pass could not complete.
pub const RepairerError = error{
    /// Not enough recovery slices to rebuild the damaged files. The one
    /// error this service reacts to rather than reports: it is what
    /// triggers on-demand volume fetching.
    UnrecoverableSet,
    /// The PAR2 set could not be parsed.
    Malformed,
    Io,
    Canceled,
} || Allocator.Error;

pub const FileFailure = struct {
    filename: []const u8,
    reason: []const u8,
};

/// What one reconstruction pass achieved.
pub const Report = struct {
    repaired: []const []const u8 = &.{},
    already_ok: []const []const u8 = &.{},
    /// Files that could not be rebuilt. Non-empty means the pass failed
    /// even when no error was returned.
    failed: []const FileFailure = &.{},
};

pub const Repairer = struct {
    ctx: *anyopaque,
    repairFn: *const fn (
        ctx: *anyopaque,
        a: Allocator,
        par2_paths: []const []const u8,
        data: []const DataPath,
    ) RepairerError!Report,

    pub fn repair(
        self: Repairer,
        a: Allocator,
        par2_paths: []const []const u8,
        data: []const DataPath,
    ) RepairerError!Report {
        return self.repairFn(self.ctx, a, par2_paths, data);
    }
};

pub const Error = app_ports.AggregateError || dl_ports.RepoError ||
    app_ports.PublishError || app_ports.TxError ||
    drepair.TransitionError || job_mod.RecoveryVolsError;

pub const Outcome = enum {
    ok,
    failed,
    /// Parked: deferred recovery volumes have been requested and the
    /// download context is fetching them.
    awaiting_recovery_vols,
    /// A previous pass already decided.
    already_terminal,
};

pub const Service = struct {
    gpa: Allocator,
    jobs: dl_ports.JobStore,
    store: Store,
    repairer: Repairer,
    sink: Sink,
    /// The download context's bus, for `JobFailed` and
    /// `RecoveryVolsRequested`. Two sinks rather than one erased sink,
    /// so neither context's events can be published to the other's topic
    /// by accident.
    downloads: DownloadSink,
    txm: app_ports.Manager,
    fs: app_ports.Filesystem,
    clock: app_ports.Clock,
    logger: *log.Logger = &log.default,
    incomplete_dir: []const u8,

    pub fn onRepairNeeded(_: *Service, job_id: JobId) JobId {
        return job_id;
    }

    pub fn run(self: *Service, job_id: JobId) Error!Outcome {
        const job = try self.jobs.byId(null, job_id);
        defer self.jobs.release(job);

        var arena = std.heap.ArenaAllocator.init(self.gpa);
        defer arena.deinit();
        const a = arena.allocator();

        var rep: *Repair = undefined;
        var loaded = false;
        var adopted = false;
        if (self.store.byJobId(null, job_id)) |existing| {
            if (existing.state.isTerminal()) {
                const state = existing.state;
                self.logger.info("repair: already terminal, skipping", &.{
                    log.int("job_id", job_id),
                    log.str("state", state.toString()),
                });
                // Copied out before the release: the message belongs to
                // the aggregate, and the store may hand it back to its
                // allocator.
                var reason_buf: [160]u8 = undefined;
                const n = @min(existing.err.len, reason_buf.len);
                @memcpy(reason_buf[0..n], existing.err[0..n]);
                self.store.release(existing);
                // A crash between "the repair was marked failed" and
                // "the Job was told" strands the job with no stage left
                // to move it; `markFailed` is a no-op once the Job is
                // terminal, so re-asserting it costs nothing and is what
                // lets `app/recovery.zig`'s sweep converge.
                if (state == .failed) try self.failJob(job_id, reason_buf[0..n]);
                return .already_terminal;
            }
            rep = existing;
            loaded = true;
        } else |e| {
            if (e != error.NotFound) return e;
            const fresh = try self.gpa.create(Repair);
            fresh.* = Repair.init(self.gpa, job_id, self.clock.now()) catch |ie| {
                self.gpa.destroy(fresh);
                return switch (ie) {
                    error.OutOfMemory => error.OutOfMemory,
                    error.JobIdRequired => error.JobNotFound,
                };
            };
            rep = fresh;
            try self.persist(rep, &adopted);
        }
        defer {
            if (loaded) {
                self.store.release(rep);
            } else if (!adopted) {
                rep.deinit();
                self.gpa.destroy(rep);
            }
        }

        try rep.start(self.clock.now());
        try self.persist(rep, &adopted);

        const paths = try verify_app.partitionJobPaths(a, .{
            .fs = self.fs,
            .logger = self.logger,
            .incomplete_dir = self.incomplete_dir,
            .who = "repair",
        }, job, job_id);

        if (paths.par2.len == 0) {
            try self.fail(rep, &adopted, job_id, "no PAR2 files available");
            return .failed;
        }

        const report = self.repairer.repair(a, paths.par2, paths.data) catch |e| {
            // Short of slices, but the job is holding recovery volumes it
            // never fetched: ask for them instead of giving up.
            if (e == error.UnrecoverableSet and job.hasDeferredRecoveryVols()) {
                self.requestRecoveryVols(job) catch |re| {
                    var buf: [96]u8 = undefined;
                    const reason = std.fmt.bufPrint(&buf, "request recovery vols: {t}", .{re}) catch
                        "request recovery vols failed";
                    try self.fail(rep, &adopted, job_id, reason);
                    return .failed;
                };
                self.logger.info("repair: insufficient slices, requesting deferred recovery vols", &.{
                    log.int("job_id", job_id),
                });
                // Stay `running`: the fix is in flight, and publishing
                // repair.failed now would take the job terminal.
                return .awaiting_recovery_vols;
            }
            var buf: [64]u8 = undefined;
            const reason = std.fmt.bufPrint(&buf, "repair: {t}", .{e}) catch "repair failed";
            try self.fail(rep, &adopted, job_id, reason);
            return .failed;
        };

        if (report.failed.len > 0) {
            // Surface the first reason; the rest are in the log.
            var buf: [256]u8 = undefined;
            const reason = std.fmt.bufPrint(&buf, "{s}: {s}", .{
                report.failed[0].filename,
                report.failed[0].reason,
            }) catch report.failed[0].reason;
            try self.fail(rep, &adopted, job_id, reason);
            return .failed;
        }

        try rep.markOk(self.clock.now());
        try self.persist(rep, &adopted);
        self.logger.info("repair ok", &.{
            log.int("job_id", job_id),
            log.uint("repaired", report.repaired.len),
            log.uint("already_ok", report.already_ok.len),
        });
        return .ok;
    }

    /// Marks the repair failed and takes the Job terminal so history
    /// shows it, in two transactions — the aggregates belong to different
    /// contexts and must not share a write.
    fn fail(
        self: *Service,
        rep: *Repair,
        adopted: *bool,
        job_id: JobId,
        reason: []const u8,
    ) Error!void {
        try rep.markFailed(reason, self.clock.now());
        try self.persist(rep, adopted);
        return self.failJob(job_id, reason);
    }

    /// Takes the Job terminal so history shows why the release stopped.
    /// Its own transaction: the aggregates belong to different contexts
    /// and must not share a write.
    fn failJob(self: *Service, job_id: JobId, reason: []const u8) Error!void {
        const Args = struct { svc: *Service, job_id: JobId, reason: []const u8 };
        const Body = struct {
            fn run(unit: *dtx.Unit, args: Args) Error!void {
                const s = args.svc;
                const job = try s.jobs.byId(unit, args.job_id);
                defer s.jobs.release(job);
                var buf: [288]u8 = undefined;
                const msg = std.fmt.bufPrint(&buf, "repair: {s}", .{args.reason}) catch args.reason;
                if (!try job.markFailed(msg, s.clock.now())) return;
                try s.jobs.save(unit, job);
                const events = try job.pullEvents();
                defer ddevents.deinitAll(s.gpa, events);
                try s.downloads.publish(unit, events);
            }
        };
        return dtx.inTx(Error, self.txm, Args{
            .svc = self,
            .job_id = job_id,
            .reason = reason,
        }, Body.run);
    }

    /// Flips `fetch_recovery_vols` on and publishes
    /// `RecoveryVolsRequested` so the orchestrator restarts the runner.
    fn requestRecoveryVols(self: *Service, job: *job_mod.Job) Error!void {
        const Args = struct { svc: *Service, job: *job_mod.Job };
        const Body = struct {
            fn run(unit: *dtx.Unit, args: Args) Error!void {
                const s = args.svc;
                try args.job.requestRecoveryVols(s.clock.now());
                try s.jobs.save(unit, args.job);
                const events = try args.job.pullEvents();
                defer ddevents.deinitAll(s.gpa, events);
                try s.downloads.publish(unit, events);
            }
        };
        return dtx.inTx(Error, self.txm, Args{ .svc = self, .job = job }, Body.run);
    }

    fn persist(self: *Service, rep: *Repair, adopted: *bool) Error!void {
        const Args = struct { svc: *Service, rep: *Repair, adopted: *bool };
        const Body = struct {
            fn run(unit: *dtx.Unit, args: Args) Error!void {
                try args.svc.store.save(unit, args.rep);
                args.adopted.* = true;
                const events = try args.rep.pullEvents();
                defer devent.deinitAll(drepair.Event, args.svc.gpa, events);
                try args.svc.sink.publish(unit, events);
            }
        };
        return dtx.inTx(Error, self.txm, Args{
            .svc = self,
            .rep = rep,
            .adopted = adopted,
        }, Body.run);
    }
};

// =====================================================================
// Test double
// =====================================================================

pub const FakeRepairer = struct {
    report: Report = .{},
    err: ?RepairerError = null,
    calls: usize = 0,
    last_par2_count: usize = 0,
    last_data_count: usize = 0,

    pub fn repairer(self: *FakeRepairer) Repairer {
        return .{ .ctx = @ptrCast(self), .repairFn = &doRepair };
    }

    fn doRepair(
        ctx: *anyopaque,
        _: Allocator,
        par2_paths: []const []const u8,
        data: []const DataPath,
    ) RepairerError!Report {
        const self: *FakeRepairer = @ptrCast(@alignCast(ctx));
        self.calls += 1;
        self.last_par2_count = par2_paths.len;
        self.last_data_count = data.len;
        if (self.err) |e| return e;
        return self.report;
    }
};

// =====================================================================
// Tests
// =====================================================================

const testing = std.testing;
const Job = job_mod.Job;

const Harness = struct {
    jobs: dl_ports.FakeJobStore = undefined,
    repairs: app_ports.FakeRepo(Repair) = undefined,
    fs: app_ports.FakeFs = undefined,
    fake_repairer: FakeRepairer = .{},
    sink: app_ports.FakeSink(drepair.Event) = .{},
    dl_sink: app_ports.FakeSink(ddevents.Event) = .{},
    ftx: app_ports.FakeTx = .{},
    clock: app_ports.FakeClock = .{ .t = 9_000 },
    logger: log.Logger = .{},
    svc: Service = undefined,

    fn init(self: *Harness) void {
        self.* = .{};
        self.jobs = dl_ports.FakeJobStore.init(testing.allocator);
        self.repairs = app_ports.FakeRepo(Repair).init(testing.allocator);
        self.fs = app_ports.FakeFs.init(testing.allocator);
        self.svc = .{
            .gpa = testing.allocator,
            .jobs = self.jobs.store(),
            .store = self.repairs.repo(),
            .repairer = self.fake_repairer.repairer(),
            .sink = self.sink.sink(),
            .downloads = self.dl_sink.sink(),
            .txm = self.ftx.manager(),
            .fs = self.fs.filesystem(),
            .clock = self.clock.clock(),
            .logger = &self.logger,
            .incomplete_dir = "/inc",
        };
    }

    fn deinit(self: *Harness) void {
        self.fs.deinit();
        self.repairs.deinit();
        self.jobs.deinit();
    }

    fn seed(self: *Harness, opts: struct {
        index_par2: bool = true,
        recovery_vol: bool = false,
        defer_vols: bool = false,
        vol_on_disk: bool = true,
    }) !*Job {
        var files: std.ArrayList(job_mod.NewFileParams) = .empty;
        defer files.deinit(testing.allocator);
        try files.append(testing.allocator, .{
            .filename = "movie.mkv",
            .size_bytes = 100,
            .segments = &.{.{ .seq_index = 1, .message_id = "d@h", .bytes = 100 }},
        });
        if (opts.index_par2) try files.append(testing.allocator, .{
            .filename = "rel.par2",
            .is_par2 = true,
            .segments = &.{.{ .seq_index = 1, .message_id = "p@h" }},
        });
        if (opts.recovery_vol) try files.append(testing.allocator, .{
            .filename = "rel.vol000+01.par2",
            .is_par2 = true,
            .is_recovery_vol = true,
            .segments = &.{.{ .seq_index = 1, .message_id = "v@h" }},
        });

        const j = try testing.allocator.create(Job);
        j.* = try Job.init(testing.allocator, .{
            .nzb_hash = "h",
            .name = "rel",
            .files = files.items,
            .defer_recovery_vols = opts.defer_vols,
        }, 0);
        try self.jobs.insert(j);
        const evts = try j.pullEvents();
        ddevents.deinitAll(testing.allocator, evts);
        for (j.files) |f| {
            if (f.is_recovery_vol and !opts.vol_on_disk) continue;
            var buf: [64]u8 = undefined;
            const p = try std.fmt.bufPrint(&buf, "/inc/{d}/{d}.tmp", .{ j.id, f.id });
            try self.fs.addFile(p, 10);
        }
        return j;
    }
};

test "a successful reconstruction records repair.ok" {
    var h: Harness = undefined;
    h.init();
    defer h.deinit();
    const j = try h.seed(.{});
    h.fake_repairer.report = .{ .repaired = &.{"movie.mkv"} };

    try testing.expectEqual(Outcome.ok, try h.svc.run(j.id));
    const rep = h.repairs.get(j.id).?;
    try testing.expectEqual(State.ok, rep.state);
    try testing.expect(h.sink.has("repair.queued"));
    try testing.expect(h.sink.has("repair.started"));
    try testing.expect(h.sink.has("repair.ok"));
    // The job is untouched — verify re-runs and decides from there.
    try testing.expectEqual(@as(usize, 0), h.dl_sink.n);
    try testing.expect(h.ftx.balanced());
}

test "a file that cannot be rebuilt fails the repair and the job" {
    var h: Harness = undefined;
    h.init();
    defer h.deinit();
    const j = try h.seed(.{});
    h.fake_repairer.report = .{ .failed = &.{.{ .filename = "movie.mkv", .reason = "short by 3 slices" }} };

    try testing.expectEqual(Outcome.failed, try h.svc.run(j.id));
    const rep = h.repairs.get(j.id).?;
    try testing.expectEqual(State.failed, rep.state);
    try testing.expectEqualStrings("movie.mkv: short by 3 slices", rep.err);
    try testing.expect(h.sink.has("repair.failed"));
    // And the job goes terminal so history shows it.
    try testing.expectEqual(job_mod.JobState.failed, j.state);
    try testing.expect(h.dl_sink.has("download.job.failed"));
    try testing.expectEqualStrings("repair: movie.mkv: short by 3 slices", j.errorMsg());
}

test "no parity at all fails with a plain reason" {
    var h: Harness = undefined;
    h.init();
    defer h.deinit();
    const j = try h.seed(.{ .index_par2 = false });

    try testing.expectEqual(Outcome.failed, try h.svc.run(j.id));
    try testing.expectEqualStrings("no PAR2 files available", h.repairs.get(j.id).?.err);
    try testing.expectEqual(@as(usize, 0), h.fake_repairer.calls);
}

test "an unrecoverable set with deferred volumes asks for them instead of failing" {
    var h: Harness = undefined;
    h.init();
    defer h.deinit();
    const j = try h.seed(.{ .recovery_vol = true, .defer_vols = true, .vol_on_disk = false });
    h.fake_repairer.err = error.UnrecoverableSet;

    try testing.expectEqual(Outcome.awaiting_recovery_vols, try h.svc.run(j.id));

    // The Job now wants its volumes and the orchestrator has been told.
    try testing.expect(j.fetchRecoveryVols());
    try testing.expect(h.dl_sink.has("download.job.recovery_vols_requested"));
    // Crucially the repair is still running, not failed: publishing
    // repair.failed here would take the job terminal while the fix is
    // already in flight.
    const rep = h.repairs.get(j.id).?;
    try testing.expectEqual(State.repairing, rep.state);
    try testing.expect(!h.sink.has("repair.failed"));
    try testing.expect(j.state != .failed);
}

test "an unrecoverable set with no deferred volumes is a plain failure" {
    var h: Harness = undefined;
    h.init();
    defer h.deinit();
    const j = try h.seed(.{ .recovery_vol = true, .defer_vols = false });
    h.fake_repairer.err = error.UnrecoverableSet;

    try testing.expectEqual(Outcome.failed, try h.svc.run(j.id));
    try testing.expectEqualStrings("repair: UnrecoverableSet", h.repairs.get(j.id).?.err);
    try testing.expect(h.sink.has("repair.failed"));
    try testing.expectEqual(job_mod.JobState.failed, j.state);
}

test "a repairer error other than short-slices fails immediately" {
    var h: Harness = undefined;
    h.init();
    defer h.deinit();
    const j = try h.seed(.{});
    h.fake_repairer.err = error.Malformed;

    try testing.expectEqual(Outcome.failed, try h.svc.run(j.id));
    try testing.expectEqualStrings("repair: Malformed", h.repairs.get(j.id).?.err);
}

test "a second run over a terminal repair does nothing" {
    var h: Harness = undefined;
    h.init();
    defer h.deinit();
    const j = try h.seed(.{});
    try testing.expectEqual(Outcome.ok, try h.svc.run(j.id));
    const calls = h.fake_repairer.calls;
    h.sink.reset();

    try testing.expectEqual(Outcome.already_terminal, try h.svc.run(j.id));
    try testing.expectEqual(calls, h.fake_repairer.calls);
    try testing.expectEqual(@as(usize, 0), h.sink.n);
    try testing.expect(h.repairs.leakFree());
}

test "deferred recovery volumes stay out of the parity set" {
    var h: Harness = undefined;
    h.init();
    defer h.deinit();
    const j = try h.seed(.{ .recovery_vol = true, .defer_vols = true, .vol_on_disk = false });
    try testing.expectEqual(Outcome.ok, try h.svc.run(j.id));
    // Only the index par2 — the mirror of the verify filter, and the
    // absence of it is what crashed repair in production.
    try testing.expectEqual(@as(usize, 1), h.fake_repairer.last_par2_count);
    try testing.expectEqual(@as(usize, 1), h.fake_repairer.last_data_count);
}

test "an unknown job is reported without creating a repair" {
    var h: Harness = undefined;
    h.init();
    defer h.deinit();
    try testing.expectError(error.JobNotFound, h.svc.run(404));
    try testing.expectEqual(@as(usize, 0), h.repairs.len());
}

test "a publish failure rolls the repair transition back" {
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
    try testing.expectEqual(@as(JobId, 7), h.svc.onRepairNeeded(7));
}

test "a failed repair found on a later run still pulls the job terminal" {
    // The crash window: the repair was marked failed and the process
    // died before the Job transition. `app/recovery.zig` re-offers the
    // job here precisely so this branch can converge, and without it the
    // job sits in `repairing` with every stage finished.
    var h: Harness = undefined;
    h.init();
    defer h.deinit();
    const j = try h.seed(.{});

    const rep = try testing.allocator.create(Repair);
    rep.* = try Repair.init(testing.allocator, j.id, 1);
    try rep.start(2);
    try rep.markFailed("short by 3 slices", 3);
    devent.deinitAll(drepair.Event, testing.allocator, try rep.pullEvents());
    try h.repairs.insert(rep);

    try testing.expectEqual(Outcome.already_terminal, try h.svc.run(j.id));
    try testing.expectEqual(job_mod.JobState.failed, j.state);
    try testing.expect(h.dl_sink.has("download.job.failed"));
    try testing.expectEqualStrings("repair: short by 3 slices", j.errorMsg());
    // No second reconstruction pass: the verdict stands.
    try testing.expectEqual(@as(usize, 0), h.fake_repairer.calls);
}

test "a repair resumed after a restart picks up its existing aggregate" {
    var h: Harness = undefined;
    h.init();
    defer h.deinit();
    const j = try h.seed(.{});

    // A previous process created and started the repair, then died.
    const rep = try testing.allocator.create(Repair);
    rep.* = try Repair.init(testing.allocator, j.id, 1);
    const queued = try rep.pullEvents();
    devent.deinitAll(drepair.Event, testing.allocator, queued);
    try h.repairs.insert(rep);

    try testing.expectEqual(Outcome.ok, try h.svc.run(j.id));
    try testing.expectEqual(@as(usize, 1), h.repairs.len());
    try testing.expectEqual(State.ok, rep.state);
    try testing.expect(h.repairs.leakFree());
}
