//! PAR2 verification: consume `download.job.download_complete`, hash the
//! files against the PAR2 set, emit `verify.ok` or `verify.repair_needed`.
//!
//! Go ran the hashing on its own goroutine so a slow verify could not
//! stall the bus dispatcher. The Zig runner does the same work inline and
//! the *caller* decides when to run it — `run` is a plain function, and
//! the bus handler is `onJobDownloadComplete`, which returns the job id
//! the caller should schedule. Nothing here spawns anything, so nothing
//! here needs a `WaitGroup` or a root context to cancel.
//!
//! # Which PAR2 files go in
//!
//! Two filters, both learned from a production failure:
//!
//!   * Recovery volumes are skipped entirely while the job has
//!     `fetch_recovery_vols = false`. They were deliberately not
//!     downloaded, so their `.tmp` files do not exist, and handing a
//!     missing path to the parser makes the whole verification fail with
//!     an open(2) error instead of a verification result.
//!   * Every remaining PAR2 path is checked for existence first. A
//!     recovery volume that *was* enabled but whose every segment 430'd
//!     is the same shape of problem arriving by a different route.
//!
//! Verification is best-effort against whatever bytes actually landed.
//! Missing files must surface as verification failures, never as parse
//! failures.

const std = @import("std");
const log = @import("../core/log.zig");
const app_ports = @import("ports.zig");
const devent = @import("../domain/event.zig");
const dl_ports = @import("download/ports.zig");
const dtx = @import("../domain/tx.zig");
const dverify = @import("../domain/verify.zig");
const job_mod = @import("../domain/download/job.zig");
const ddevents = @import("../domain/download/events.zig");

const Allocator = std.mem.Allocator;

pub const Timestamp = app_ports.Timestamp;
pub const JobId = dl_ports.JobId;
pub const VerifySet = dverify.VerifySet;
pub const VerifyState = dverify.VerifyState;
pub const Result = dverify.Result;
pub const FileResult = dverify.FileResult;
pub const Sink = app_ports.EventSink(dverify.Event);
pub const DownloadSink = app_ports.EventSink(ddevents.Event);
pub const Store = app_ports.Repo(VerifySet);

/// Why a verification pass could not produce a result at all — distinct
/// from "it ran and the files are damaged", which is a `Result`.
pub const VerifierError = error{
    /// The PAR2 set could not be parsed.
    Malformed,
    /// A path the caller supplied could not be read.
    Io,
    Canceled,
} || Allocator.Error;

/// One data file's name and where its bytes currently live.
pub const DataPath = struct {
    /// The name recorded inside the PAR2 set.
    filename: []const u8,
    /// The `<job dir>/<file id>.tmp` the bytes are in.
    path: []const u8,
};

/// The PAR2 verifier. Injected because the real one hashes gigabytes and
/// the service's own logic — path partitioning, idempotency, state
/// transitions — is what these tests are about.
pub const Verifier = struct {
    ctx: *anyopaque,
    verifyFn: *const fn (
        ctx: *anyopaque,
        a: Allocator,
        par2_paths: []const []const u8,
        data: []const DataPath,
    ) VerifierError!Result,

    pub fn verify(
        self: Verifier,
        a: Allocator,
        par2_paths: []const []const u8,
        data: []const DataPath,
    ) VerifierError!Result {
        return self.verifyFn(self.ctx, a, par2_paths, data);
    }
};

pub const Error = app_ports.AggregateError || dl_ports.RepoError ||
    app_ports.PublishError || app_ports.TxError || dverify.TransitionError;

/// What one `run` decided, so the caller can log or test it without
/// re-reading the aggregate.
pub const Outcome = enum {
    /// Every file verified.
    ok,
    /// At least one file is damaged; the repair worker takes over.
    repair_needed,
    /// The pass could not run — no PAR2, or the verifier errored.
    failed,
    /// A previous pass already decided; nothing to do.
    already_terminal,
};

pub const Paths = struct {
    par2: []const []const u8,
    data: []const DataPath,
};

pub const PartitionCtx = struct {
    fs: app_ports.Filesystem,
    logger: *log.Logger,
    incomplete_dir: []const u8,
    /// Log prefix, so the operator can tell a verify skip from a repair
    /// one.
    who: []const u8,
};

/// Splits a job's files into parity and data paths, dropping parity that
/// is not on disk.
///
/// Shared with `app/repair.zig`. Go had this loop written out twice, once
/// in each service, with a comment in the second copy pointing at the
/// first — which is precisely how two copies drift.
pub fn partitionJobPaths(
    a: Allocator,
    ctx: PartitionCtx,
    job: *const job_mod.Job,
    job_id: JobId,
) Allocator.Error!Paths {
    var par2: std.ArrayList([]const u8) = .empty;
    var data: std.ArrayList(DataPath) = .empty;
    for (job.files) |f| {
        const p = try std.fmt.allocPrint(a, "{s}/{d}/{d}.tmp", .{
            ctx.incomplete_dir, job_id, f.id,
        });
        if (!f.is_par2) {
            try data.append(a, .{ .filename = f.filename, .path = p });
            continue;
        }
        // Deliberately never fetched: the .tmp does not exist and never
        // will until repair asks for it.
        if (f.is_recovery_vol and !job.fetchRecoveryVols()) continue;
        // Fetching was enabled but the bytes never arrived. Same shape,
        // different cause; same answer.
        if (!ctx.fs.exists(p)) {
            ctx.logger.warn("par2 file missing, skipping", &.{
                log.str("who", ctx.who),
                log.int("job_id", job_id),
                log.int("file_id", f.id),
                log.boolean("recovery_vol", f.is_recovery_vol),
            });
            continue;
        }
        try par2.append(a, p);
    }
    return .{ .par2 = par2.items, .data = data.items };
}

pub const Service = struct {
    gpa: Allocator,
    jobs: dl_ports.JobStore,
    store: Store,
    verifier: Verifier,
    sink: Sink,
    /// The download context's bus, for `JobFailed`. Two sinks rather than
    /// one erased sink, so neither context's events can be published to
    /// the other's topic by accident.
    downloads: DownloadSink,
    txm: app_ports.Manager,
    fs: app_ports.Filesystem,
    clock: app_ports.Clock,
    logger: *log.Logger = &log.default,
    incomplete_dir: []const u8,

    /// Bus handler for `download.job.download_complete`. Returns the job
    /// to verify; the caller schedules `run` so a long hash does not sit
    /// inside the dispatcher's retry window.
    pub fn onJobDownloadComplete(_: *Service, job_id: JobId) JobId {
        return job_id;
    }

    /// Bus handler for `repair.ok`. The repair worker reconstructed the
    /// damaged files, so the previous `repair_needed` verdict is stale:
    /// reset and verify again. The follow-up `verify.ok` is what deliver
    /// and extract are listening for.
    pub fn onRepairOk(self: *Service, job_id: JobId) Error!Outcome {
        if (self.store.byJobId(null, job_id)) |vset| {
            defer self.store.release(vset);
            if (vset.state == .repair_needed) try self.resetSet(vset);
        } else |e| {
            if (e != error.NotFound) return e;
        }
        return self.run(job_id);
    }

    /// The whole verification flow for one job.
    pub fn run(self: *Service, job_id: JobId) Error!Outcome {
        const job = try self.jobs.byId(null, job_id);
        defer self.jobs.release(job);

        var arena = std.heap.ArenaAllocator.init(self.gpa);
        defer arena.deinit();
        const a = arena.allocator();

        const paths = try self.partitionPaths(a, job, job_id);

        // Load or create the set.
        //
        // Two very different ownerships hide behind one pointer: a loaded
        // aggregate belongs to the store and is handed back, a freshly
        // constructed one belongs to this scope until the first save
        // adopts it. Conflating them released an aggregate the store had
        // never lent out.
        var vset: *VerifySet = undefined;
        var loaded = false;
        var adopted = false;
        if (self.store.byJobId(null, job_id)) |existing| {
            vset = existing;
            loaded = true;
            if (existing.state == .repair_needed and job.fetchRecoveryVols()) {
                // A second-round download_complete after recovery volumes
                // landed. The flag flip is the signal that the previous
                // repair_needed is stale — only the repair worker sets it,
                // and only when it has actually asked for more volumes.
                try self.resetSet(existing);
            } else if (existing.state.isTerminal()) {
                const state = existing.state;
                self.logger.info("verify: already terminal, skipping", &.{
                    log.int("job_id", job_id),
                    log.str("state", state.toString()),
                });
                // Copied out before the release: the message belongs to
                // the aggregate, and the store may hand it straight back
                // to its allocator.
                var reason_buf: [160]u8 = undefined;
                const n = @min(existing.error_msg.len, reason_buf.len);
                @memcpy(reason_buf[0..n], existing.error_msg[0..n]);
                const reason = reason_buf[0..n];
                self.store.release(existing);
                // A crash between "the set was marked failed" and "the
                // Job was told" leaves a job with no evidence and no
                // stage left to run it. Re-asserting the transition here
                // is what makes the startup sweep able to converge on it;
                // `markFailed` is a no-op once the Job is terminal.
                if (state == .failed) try self.failJob(job_id, reason);
                return .already_terminal;
            }
        } else |e| {
            if (e != error.NotFound) return e;
            const fresh = try self.gpa.create(VerifySet);
            fresh.* = VerifySet.init(self.gpa, job_id, self.clock.now()) catch |ie| {
                self.gpa.destroy(fresh);
                return switch (ie) {
                    error.OutOfMemory => error.OutOfMemory,
                    // A zero job id here would mean the bus handed us an
                    // event with no aggregate; treat it as a missing job.
                    error.JobIdRequired => error.JobNotFound,
                };
            };
            vset = fresh;
        }
        defer {
            if (loaded) {
                self.store.release(vset);
            } else if (!adopted) {
                vset.deinit();
                self.gpa.destroy(vset);
            }
        }

        // No parity at all: we have no integrity evidence. Say so
        // plainly rather than pretending the release verified.
        if (paths.par2.len == 0) {
            _ = try vset.markStarted(self.clock.now());
            try vset.markFailed("no .par2 files in job", self.clock.now());
            try self.persist(vset, &adopted);
            try self.failJob(job_id, "no .par2 files in job");
            return .failed;
        }

        _ = try vset.markStarted(self.clock.now());
        try self.persist(vset, &adopted);

        const result = self.verifier.verify(a, paths.par2, paths.data) catch |e| {
            var buf: [64]u8 = undefined;
            const reason = std.fmt.bufPrint(&buf, "verify: {t}", .{e}) catch "verify failed";
            try vset.markFailed(reason, self.clock.now());
            try self.persist(vset, &adopted);
            try self.failJob(job_id, reason);
            return .failed;
        };

        if (result.allOk()) {
            try vset.markOk(self.clock.now());
            try self.persist(vset, &adopted);
            return .ok;
        }
        const failed = try result.failedNames(a);
        try vset.markRepairNeeded(failed, self.clock.now());
        try self.persist(vset, &adopted);
        return .repair_needed;
    }

    fn partitionPaths(
        self: *Service,
        a: Allocator,
        job: *const job_mod.Job,
        job_id: JobId,
    ) Allocator.Error!Paths {
        return partitionJobPaths(a, .{
            .fs = self.fs,
            .logger = self.logger,
            .incomplete_dir = self.incomplete_dir,
            .who = "verify",
        }, job, job_id);
    }

    fn resetSet(self: *Service, vset: *VerifySet) Error!void {
        const Args = struct { svc: *Service, vset: *VerifySet };
        const Body = struct {
            fn run(unit: *dtx.Unit, args: Args) Error!void {
                try args.vset.reset();
                try args.svc.store.save(unit, args.vset);
            }
        };
        return dtx.inTx(Error, self.txm, Args{ .svc = self, .vset = vset }, Body.run);
    }

    /// Takes the Job terminal because verification produced no evidence.
    ///
    /// `verify.failed` has no subscriber that can act on it — repair
    /// listens for `verify.repair_needed`, deliver and extract for
    /// `verify.ok` — so without this the job sat in `download_complete`
    /// with every stage finished and nothing left to move it. A release
    /// we cannot check is a failed release, and saying so is strictly
    /// better than a queue entry that never resolves: the alternative
    /// the operator eventually gets is a corrupt file and no warning.
    ///
    /// Its own transaction, because the Job belongs to the download
    /// context and must not share a write with the verify set.
    fn failJob(self: *Service, job_id: JobId, reason: []const u8) Error!void {
        const Args = struct { svc: *Service, job_id: JobId, reason: []const u8 };
        const Body = struct {
            fn run(unit: *dtx.Unit, args: Args) Error!void {
                const s = args.svc;
                const job = try s.jobs.byId(unit, args.job_id);
                defer s.jobs.release(job);
                var buf: [224]u8 = undefined;
                const msg = std.fmt.bufPrint(&buf, "verify: {s}", .{args.reason}) catch args.reason;
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

    /// Saves the set and publishes its queued events in one transaction.
    /// Sets `adopted` on the first successful save, because the store
    /// takes ownership of a freshly-inserted aggregate then.
    fn persist(self: *Service, vset: *VerifySet, adopted: *bool) Error!void {
        const Args = struct { svc: *Service, vset: *VerifySet, adopted: *bool };
        const Body = struct {
            fn run(unit: *dtx.Unit, args: Args) Error!void {
                try args.svc.store.save(unit, args.vset);
                // The store holds the pointer from here, whatever the
                // transaction does next: a rollback undoes the row, not
                // the object's lifetime. Setting the flag after `inTx`
                // instead would double-free on a failed publish.
                args.adopted.* = true;
                const events = try args.vset.pullEvents();
                defer devent.deinitAll(dverify.Event, args.svc.gpa, events);
                try args.svc.sink.publish(unit, events);
            }
        };
        return dtx.inTx(Error, self.txm, Args{
            .svc = self,
            .vset = vset,
            .adopted = adopted,
        }, Body.run);
    }
};

// =====================================================================
// Test double
// =====================================================================

/// A scripted `Verifier`. Records what it was asked to check so a test
/// can assert on the path partitioning without a PAR2 file in sight.
pub const FakeVerifier = struct {
    /// Per-file verdicts, keyed by filename. Anything not listed passes.
    failures: []const []const u8 = &.{},
    /// When set, the pass errors instead of producing a result.
    err: ?VerifierError = null,

    calls: usize = 0,
    last_par2: [][]const u8 = &.{},
    last_par2_count: usize = 0,
    last_data_count: usize = 0,
    /// Copy of the last par2 basenames seen, for assertions.
    seen_par2: [8][]const u8 = @splat(""),
    n_seen: usize = 0,

    pub fn verifier(self: *FakeVerifier) Verifier {
        return .{ .ctx = @ptrCast(self), .verifyFn = &doVerify };
    }

    pub fn seen(self: *const FakeVerifier) []const []const u8 {
        return self.seen_par2[0..self.n_seen];
    }

    fn doVerify(
        ctx: *anyopaque,
        a: Allocator,
        par2_paths: []const []const u8,
        data: []const DataPath,
    ) VerifierError!Result {
        const self: *FakeVerifier = @ptrCast(@alignCast(ctx));
        self.calls += 1;
        self.last_par2_count = par2_paths.len;
        self.last_data_count = data.len;
        self.n_seen = 0;
        for (par2_paths) |p| {
            if (self.n_seen == self.seen_par2.len) break;
            self.seen_par2[self.n_seen] = p;
            self.n_seen += 1;
        }
        if (self.err) |e| return e;

        const files = try a.alloc(FileResult, data.len);
        for (data, files) |d, *f| {
            var ok = true;
            for (self.failures) |bad| {
                if (std.mem.eql(u8, bad, d.filename)) ok = false;
            }
            f.* = .{ .filename = d.filename, .ok = ok, .reason = if (ok) "" else "md5 mismatch" };
        }
        return .{ .files = files };
    }
};

// =====================================================================
// Tests
// =====================================================================

const testing = std.testing;
const devents = ddevents;
const Job = job_mod.Job;

const Harness = struct {
    jobs: dl_ports.FakeJobStore = undefined,
    sets: app_ports.FakeRepo(VerifySet) = undefined,
    fs: app_ports.FakeFs = undefined,
    fake_verifier: FakeVerifier = .{},
    sink: app_ports.FakeSink(dverify.Event) = .{},
    dl_sink: app_ports.FakeSink(devents.Event) = .{},
    ftx: app_ports.FakeTx = .{},
    clock: app_ports.FakeClock = .{ .t = 5_000 },
    logger: log.Logger = .{},
    svc: Service = undefined,

    fn init(self: *Harness) void {
        self.* = .{};
        self.jobs = dl_ports.FakeJobStore.init(testing.allocator);
        self.sets = app_ports.FakeRepo(VerifySet).init(testing.allocator);
        self.fs = app_ports.FakeFs.init(testing.allocator);
        self.svc = .{
            .gpa = testing.allocator,
            .jobs = self.jobs.store(),
            .store = self.sets.repo(),
            .verifier = self.fake_verifier.verifier(),
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
        self.sets.deinit();
        self.jobs.deinit();
    }

    /// A job with one data file plus the parity the caller asks for.
    fn seed(self: *Harness, opts: struct {
        index_par2: bool = true,
        recovery_vol: bool = false,
        defer_vols: bool = false,
        /// Create the recovery volume's .tmp on disk.
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
        devents.deinitAll(testing.allocator, evts);

        // Materialise the .tmp files the real pipeline would have left.
        for (j.files) |f| {
            if (f.is_recovery_vol and !opts.vol_on_disk) continue;
            var buf: [64]u8 = undefined;
            const p = try std.fmt.bufPrint(&buf, "/inc/{d}/{d}.tmp", .{ j.id, f.id });
            try self.fs.addFile(p, 10);
        }
        return j;
    }
};

test "a clean set verifies, persists and publishes verify.ok" {
    var h: Harness = undefined;
    h.init();
    defer h.deinit();
    const j = try h.seed(.{});

    try testing.expectEqual(Outcome.ok, try h.svc.run(j.id));

    const vset = h.sets.get(j.id).?;
    try testing.expectEqual(VerifyState.ok, vset.state);
    try testing.expectEqual(@as(?Timestamp, h.clock.t), vset.finished_at);
    try testing.expect(h.sink.has("verify.started"));
    try testing.expect(h.sink.has("verify.ok"));
    // One data file in, one index par2.
    try testing.expectEqual(@as(usize, 1), h.fake_verifier.last_data_count);
    try testing.expectEqual(@as(usize, 1), h.fake_verifier.last_par2_count);
    try testing.expect(h.ftx.balanced());
}

test "a damaged file becomes repair_needed carrying the filename" {
    var h: Harness = undefined;
    h.init();
    defer h.deinit();
    const j = try h.seed(.{});
    h.fake_verifier.failures = &.{"movie.mkv"};

    try testing.expectEqual(Outcome.repair_needed, try h.svc.run(j.id));
    const vset = h.sets.get(j.id).?;
    try testing.expectEqual(VerifyState.repair_needed, vset.state);
    try testing.expectEqual(@as(usize, 1), vset.failed_files.len);
    try testing.expectEqualStrings("movie.mkv", vset.failed_files[0]);
    try testing.expect(h.sink.has("verify.repair_needed"));
}

test "a job with no parity fails with a reason rather than verifying" {
    var h: Harness = undefined;
    h.init();
    defer h.deinit();
    const j = try h.seed(.{ .index_par2 = false });

    try testing.expectEqual(Outcome.failed, try h.svc.run(j.id));
    const vset = h.sets.get(j.id).?;
    try testing.expectEqual(VerifyState.failed, vset.state);
    try testing.expectEqualStrings("no .par2 files in job", vset.error_msg);
    // The verifier is never called: there is nothing to check against.
    try testing.expectEqual(@as(usize, 0), h.fake_verifier.calls);
    try testing.expect(h.sink.has("verify.failed"));
    // And the Job goes with it. Nothing subscribes to `verify.failed`,
    // so a Job left in `download_complete` here is a Job nothing will
    // ever touch again.
    try testing.expectEqual(job_mod.JobState.failed, j.state);
    try testing.expect(h.dl_sink.has("download.job.failed"));
    try testing.expectEqualStrings("verify: no .par2 files in job", j.errorMsg());
}

test "deferred recovery volumes are excluded from the parity set" {
    // The production failure: a deferred volume's .tmp does not exist,
    // and handing the path to the parser turns the whole verification
    // into an open(2) error.
    var h: Harness = undefined;
    h.init();
    defer h.deinit();
    const j = try h.seed(.{ .recovery_vol = true, .defer_vols = true, .vol_on_disk = false });

    try testing.expectEqual(Outcome.ok, try h.svc.run(j.id));
    // Only the index par2 went in.
    try testing.expectEqual(@as(usize, 1), h.fake_verifier.last_par2_count);
    for (h.fake_verifier.seen()) |p| {
        try testing.expect(std.mem.indexOf(u8, p, "3.tmp") == null);
    }
}

test "an enabled recovery volume whose download failed is skipped too" {
    // Defence in depth: fetching was enabled, but every segment 430'd, so
    // the .tmp never materialised. Verification is best-effort against
    // whatever arrived.
    var h: Harness = undefined;
    h.init();
    defer h.deinit();
    const j = try h.seed(.{ .recovery_vol = true, .defer_vols = false, .vol_on_disk = false });

    try testing.expectEqual(Outcome.ok, try h.svc.run(j.id));
    try testing.expectEqual(@as(usize, 1), h.fake_verifier.last_par2_count);
}

test "an enabled recovery volume that did land is included" {
    var h: Harness = undefined;
    h.init();
    defer h.deinit();
    const j = try h.seed(.{ .recovery_vol = true, .defer_vols = false, .vol_on_disk = true });
    try testing.expectEqual(Outcome.ok, try h.svc.run(j.id));
    try testing.expectEqual(@as(usize, 2), h.fake_verifier.last_par2_count);
}

test "a verifier error fails the set with the reason, not a crash" {
    var h: Harness = undefined;
    h.init();
    defer h.deinit();
    const j = try h.seed(.{});
    h.fake_verifier.err = error.Malformed;

    try testing.expectEqual(Outcome.failed, try h.svc.run(j.id));
    const vset = h.sets.get(j.id).?;
    try testing.expectEqual(VerifyState.failed, vset.state);
    try testing.expectEqualStrings("verify: Malformed", vset.error_msg);
    try testing.expect(h.sink.has("verify.failed"));
    try testing.expectEqual(job_mod.JobState.failed, j.state);
    try testing.expect(h.dl_sink.has("download.job.failed"));
}

test "a failed set found on a later run still pulls the job terminal" {
    // The crash window: the set was marked failed and the process died
    // before the Job transition. `app/recovery.zig` re-offers the job to
    // this service precisely so this branch can converge.
    var h: Harness = undefined;
    h.init();
    defer h.deinit();
    const j = try h.seed(.{});

    const vset = try testing.allocator.create(VerifySet);
    vset.* = try VerifySet.init(testing.allocator, j.id, 1);
    devent.deinitAll(dverify.Event, testing.allocator, try vset.pullEvents());
    _ = try vset.markStarted(2);
    try vset.markFailed("verify: Malformed", 3);
    devent.deinitAll(dverify.Event, testing.allocator, try vset.pullEvents());
    try h.sets.insert(vset);

    try testing.expectEqual(Outcome.already_terminal, try h.svc.run(j.id));
    try testing.expectEqual(job_mod.JobState.failed, j.state);
    try testing.expect(h.dl_sink.has("download.job.failed"));
    // No second verification: the verdict stands.
    try testing.expectEqual(@as(usize, 0), h.fake_verifier.calls);
}

test "a second run over a terminal set does nothing" {
    var h: Harness = undefined;
    h.init();
    defer h.deinit();
    const j = try h.seed(.{});
    try testing.expectEqual(Outcome.ok, try h.svc.run(j.id));
    const calls = h.fake_verifier.calls;
    h.sink.reset();

    try testing.expectEqual(Outcome.already_terminal, try h.svc.run(j.id));
    try testing.expectEqual(calls, h.fake_verifier.calls);
    try testing.expectEqual(@as(usize, 0), h.sink.n);
    try testing.expect(h.sets.leakFree());
}

test "repair.ok resets a repair_needed set and verifies again" {
    var h: Harness = undefined;
    h.init();
    defer h.deinit();
    const j = try h.seed(.{});
    h.fake_verifier.failures = &.{"movie.mkv"};
    try testing.expectEqual(Outcome.repair_needed, try h.svc.run(j.id));

    // The repair worker fixed the file; the stale verdict must not block
    // the second pass.
    h.fake_verifier.failures = &.{};
    try testing.expectEqual(Outcome.ok, try h.svc.onRepairOk(j.id));
    const vset = h.sets.get(j.id).?;
    try testing.expectEqual(VerifyState.ok, vset.state);
    try testing.expectEqual(@as(usize, 0), vset.failed_files.len);
    try testing.expect(h.sets.leakFree());
}

test "repair.ok with no set at all just runs a first verification" {
    var h: Harness = undefined;
    h.init();
    defer h.deinit();
    const j = try h.seed(.{});
    try testing.expectEqual(Outcome.ok, try h.svc.onRepairOk(j.id));
    try testing.expectEqual(@as(usize, 1), h.sets.len());
}

test "recovery volumes landing after a repair_needed verdict reopen the set" {
    var h: Harness = undefined;
    h.init();
    defer h.deinit();
    const j = try h.seed(.{ .recovery_vol = true, .defer_vols = true, .vol_on_disk = false });
    h.fake_verifier.failures = &.{"movie.mkv"};
    try testing.expectEqual(Outcome.repair_needed, try h.svc.run(j.id));

    // The repair worker asked for the volumes and they arrived: the flag
    // flip is the signal that the previous verdict is stale.
    try j.requestRecoveryVols(h.clock.t);
    const evts = try j.pullEvents();
    devents.deinitAll(testing.allocator, evts);
    var buf: [64]u8 = undefined;
    const vol = try std.fmt.bufPrint(&buf, "/inc/{d}/{d}.tmp", .{ j.id, j.files[2].id });
    try h.fs.addFile(vol, 10);
    h.fake_verifier.failures = &.{};

    try testing.expectEqual(Outcome.ok, try h.svc.run(j.id));
    // And the volume is now part of the parity set.
    try testing.expectEqual(@as(usize, 2), h.fake_verifier.last_par2_count);
}

test "an unknown job is reported, not verified" {
    var h: Harness = undefined;
    h.init();
    defer h.deinit();
    try testing.expectError(error.JobNotFound, h.svc.run(999));
    try testing.expectEqual(@as(usize, 0), h.sets.len());
}

test "a publish failure rolls the set's transition back" {
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
    // Deliberately trivial: the point is that the handler does *not*
    // hash inline, because the bus dispatcher's retry window is not the
    // right place to spend minutes of CPU.
    try testing.expectEqual(@as(JobId, 42), h.svc.onJobDownloadComplete(42));
}
