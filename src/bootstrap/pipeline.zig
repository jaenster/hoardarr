//! The post-download half of the composition root: verify, repair,
//! extract, deliver, notify — and the codec adapters underneath them.
//!
//! ## What this file is
//!
//! `app/verify.zig`, `app/repair.zig`, `app/extract.zig` and
//! `app/deliver/service.zig` each take their heavy lifting as an injected
//! port (`Verifier`, `Repairer`, `Extractor`) precisely so their own
//! logic — path partitioning, idempotency, state transitions — can be
//! tested without hashing a gigabyte. This is where the real
//! implementations get attached: `codec/par2` behind `Verifier`,
//! `codec/rar` behind `Extractor`.
//!
//! ## The `std.Io` boundary
//!
//! `codec/par2` and `codec/rar` read files through `std.Io`, because
//! parsing a container is exactly the kind of code that should not know
//! about descriptors. The daemon does not otherwise adopt an `std.Io`
//! implementation — every socket in the process goes through
//! `posix/sys.zig` and the reactor — so the one used here is
//! `std.Io.Threaded.init_single_threaded`: a statically-initialised
//! instance that spawns nothing, whose `deinit` is unnecessary, and whose
//! file operations run synchronously on the calling thread. It is the
//! narrowest thing that satisfies the codecs' signature.
//!
//! ## What runs where
//!
//! These services used to run inline in their bus handler, on the reactor
//! thread — which meant that verification hashing every byte of a release
//! and extraction decompressing it stalled the event loop for as long as
//! that took: no HTTP, no SSE, no other download.
//!
//! They now run on a fiber, with their CPU handed to a worker pool. See
//! `bootstrap/offload.zig`, which owns both halves and states what it
//! costs. The split is visible here as the `Work` struct each adapter
//! carries: everything the codec needs in, everything it produced out,
//! and no pointer to anything single-threaded — because that struct is
//! the only thing a worker thread ever touches.
//!
//! A null `stages` on any adapter keeps it exactly as it was: called on
//! whichever thread called the port. That is what a test driving a
//! service directly gets, and it is why none of them needed changing.

const std = @import("std");

const log = @import("../core/log.zig");
const sqlite = @import("../store/sqlite.zig");

const par2_verifier = @import("../codec/par2/verifier.zig");
const par2_repair = @import("../codec/par2/repair.zig");
const rar_extract = @import("../codec/rar/extract.zig");

const app_ports = @import("../app/ports.zig");
const dl_ports = @import("../app/download/ports.zig");
const verify_app = @import("../app/verify.zig");
const repair_app = @import("../app/repair.zig");
const extract_app = @import("../app/extract.zig");
const deliver_app = @import("../app/deliver/service.zig");

const dverify = @import("../domain/verify.zig");
const drepair = @import("../domain/repair.zig");
const dextract = @import("../domain/extract.zig");
const ddeliver = @import("../domain/deliver.zig");

const offload_mod = @import("offload.zig");

const Allocator = std.mem.Allocator;
const Io = std.Io;

/// The process-wide `std.Io`.
///
/// `init_single_threaded` is a comptime-constructed value: no thread
/// pool, no allocator, `deinit` not required. Every call through it is a
/// direct syscall on whichever thread made it — which since the hashers
/// moved off the reactor is either the loop thread or the one worker
/// running the single in-flight stage, never both at once. That
/// serialisation is `offload.Stages`'s, not this value's: it holds no
/// state to race over, but nothing here would notice if it did.
pub var io_impl: Io.Threaded = .init_single_threaded;

pub fn io() Io {
    return io_impl.io();
}

/// Run `f(w)` on a worker thread when there is a stage fiber to park,
/// and inline when there is not.
///
/// Returns false only when the pool cancelled the task — shutdown, in
/// other words — in which case `w` holds nothing and the caller must not
/// pretend it does. `Unavailable` (no pool wired, or its queue is full)
/// falls back to running inline, which is what a build without threads
/// does for everything.
///
/// `w` lives on the stage fiber's stack and the worker writes through
/// it; see `offload.zig` on why that makes the shutdown ordering
/// load-bearing.
fn onWorker(
    stages: ?*offload_mod.Stages,
    comptime T: type,
    w: *T,
    comptime f: fn (*T) void,
) bool {
    const s = stages orelse {
        f(w);
        return true;
    };
    const run = s.current() orelse {
        f(w);
        return true;
    };
    offload_mod.offload(T, run, w, f) catch |e| switch (e) {
        error.Unavailable => {
            f(w);
            return true;
        },
        // Hashing a release during teardown would only delay it.
        error.Canceled => return false,
    };
    return true;
}

// =====================================================================
// PAR2 verification
// =====================================================================

/// `app/verify.zig`'s `Verifier` over `codec/par2`.
pub const Verifier = struct {
    gpa: Allocator,
    logger: *log.Logger = &log.default,
    /// Where the MD5 and CRC of every byte of the release actually run.
    /// Null keeps it on the calling thread, which is what a test that
    /// drives the service directly wants and what the daemon did before
    /// `offload.zig` existed.
    stages: ?*offload_mod.Stages = null,

    pub fn port(self: *Verifier) verify_app.Verifier {
        return .{ .ctx = @ptrCast(self), .verifyFn = &doVerify };
    }

    /// The CPU half, in the shape a worker can be handed: everything in,
    /// everything out, no pointer into anything single-threaded.
    ///
    /// `a` is the service's per-run arena. Only this task allocates from
    /// it while the task is in flight — the stage fiber that owns it is
    /// parked — so the arena is single-user throughout even though the
    /// user changed threads.
    const Work = struct {
        a: Allocator,
        par2_paths: []const []const u8,
        files: []par2_verifier.DataFile,
        result: ?par2_verifier.Result = null,
        err: ?anyerror = null,

        fn body(w: *Work) void {
            w.result = par2_verifier.verify(w.a, io(), Io.Dir.cwd(), w.par2_paths, w.files) catch |e| {
                w.err = e;
                return;
            };
        }
    };

    fn doVerify(
        ctx: *anyopaque,
        a: Allocator,
        par2_paths: []const []const u8,
        data: []const verify_app.DataPath,
    ) verify_app.VerifierError!dverify.Result {
        const self: *Verifier = @ptrCast(@alignCast(ctx));

        const files = try a.alloc(par2_verifier.DataFile, data.len);
        for (data, files) |d, *f| f.* = .{ .name = d.filename, .path = d.path };

        // `a` is the service's per-run arena, so the parsed set and the
        // result share its lifetime and nothing here has to be freed on
        // the error paths.
        var work: Work = .{ .a = a, .par2_paths = par2_paths, .files = files };
        if (!onWorker(self.stages, Work, &work, Work.body)) return error.Malformed;

        const result = work.result orelse {
            const e = work.err orelse error.Unexpected;
            self.logger.warn("par2: verification could not run", &.{log.errv("err", e)});
            return switch (e) {
                error.OutOfMemory => error.OutOfMemory,
                // Anything else is either an unreadable `.par2` or bytes
                // that are not a recovery set. Both are "we have no
                // integrity evidence", which the service reports as a
                // failed pass rather than a clean one.
                else => error.Malformed,
            };
        };

        const out = try a.alloc(dverify.FileResult, result.files.items.len);
        for (result.files.items, out) |src, *dst| {
            dst.* = .{
                .filename = src.filename,
                .ok = src.ok,
                .reason = src.reason.text(),
            };
        }
        return .{ .files = out };
    }
};

// =====================================================================
// PAR2 repair
// =====================================================================

/// `app/repair.zig`'s `Repairer` over `codec/par2`.
///
/// The one translation that carries weight is the shortfall: a set with
/// fewer recovery slices than damaged ones becomes `UnrecoverableSet`,
/// which is the single error the service reacts to rather than reports.
/// A job holding deferred recovery volumes uses it to go and fetch them
/// and try again, so collapsing it into a generic failure would take
/// jobs terminal that were one download away from repairing.
pub const Repairer = struct {
    logger: *log.Logger = &log.default,
    /// Reed-Solomon reconstruction is the heaviest arithmetic in the
    /// tree. Same treatment as verification: off the reactor thread when
    /// there is a stage fiber to park.
    stages: ?*offload_mod.Stages = null,

    pub fn port(self: *Repairer) repair_app.Repairer {
        return .{ .ctx = @ptrCast(self), .repairFn = &doRepair };
    }

    const Work = struct {
        a: Allocator,
        par2_paths: []const []const u8,
        files: []par2_repair.DataFile,
        result: ?par2_repair.Result = null,
        err: ?anyerror = null,

        fn body(w: *Work) void {
            w.result = par2_repair.repair(w.a, io(), Io.Dir.cwd(), w.par2_paths, w.files) catch |e| {
                w.err = e;
                return;
            };
        }
    };

    fn doRepair(
        ctx: *anyopaque,
        a: Allocator,
        par2_paths: []const []const u8,
        data: []const verify_app.DataPath,
    ) repair_app.RepairerError!repair_app.Report {
        const self: *Repairer = @ptrCast(@alignCast(ctx));

        const files = try a.alloc(par2_repair.DataFile, data.len);
        for (data, files) |d, *f| f.* = .{ .name = d.filename, .path = d.path };

        // `a` is the service's per-run arena, so nothing below has to be
        // freed on any path out of here.
        var work: Work = .{ .a = a, .par2_paths = par2_paths, .files = files };
        if (!onWorker(self.stages, Work, &work, Work.body)) return error.Malformed;

        const result = work.result orelse {
            const e = work.err orelse error.Unexpected;
            self.logger.warn("par2: reconstruction could not run", &.{
                log.errv("err", e),
                log.uint("par2_files", par2_paths.len),
            });
            return switch (e) {
                error.OutOfMemory => error.OutOfMemory,
                // The equations we had did not span the damage. Same
                // answer as a shortfall: go and find more parity.
                error.Singular => error.UnrecoverableSet,
                // Either an unreadable `.par2` or bytes that are not a
                // recovery set. Both mean we have no parity to work from.
                else => error.Malformed,
            };
        };

        if (result.shortfall) |s| {
            self.logger.warn("par2: not enough recovery slices to repair", &.{
                log.uint("damaged_slices", s.damaged_slices),
                log.uint("recovery_slices", s.recovery_slices),
            });
            return error.UnrecoverableSet;
        }

        const repaired = try a.alloc([]const u8, result.repaired.items.len);
        for (result.repaired.items, repaired) |src, *dst| dst.* = src.filename;
        const already_ok = try a.alloc([]const u8, result.already_ok.items.len);
        for (result.already_ok.items, already_ok) |src, *dst| dst.* = src;
        const failed = try a.alloc(repair_app.FileFailure, result.failed.items.len);
        for (result.failed.items, failed) |src, *dst| {
            dst.* = .{ .filename = src.filename, .reason = src.reason };
        }

        self.logger.info("par2: reconstruction pass complete", &.{
            log.uint("repaired", repaired.len),
            log.uint("already_ok", already_ok.len),
            log.uint("failed", failed.len),
            log.uint("matched_by_content", result.matched_by_content),
        });
        return .{ .repaired = repaired, .already_ok = already_ok, .failed = failed };
    }
};

// =====================================================================
// RAR extraction
// =====================================================================

/// `app/extract.zig`'s `Extractor` over `codec/rar`.
pub const Extractor = struct {
    gpa: Allocator,
    logger: *log.Logger = &log.default,
    /// Decompression is the other thing that used to stop the daemon for
    /// minutes. Same offload as verification.
    stages: ?*offload_mod.Stages = null,

    pub fn port(self: *Extractor) extract_app.Extractor {
        return .{ .ctx = @ptrCast(self), .extractFn = &doExtract };
    }

    /// Note the allocator: `rar_extract.extract` is handed the process
    /// `gpa`, not a per-run arena, and it runs on a worker. That is only
    /// safe because the daemon's allocator is thread-safe (a
    /// `DebugAllocator` in debug builds, the page allocator in release)
    /// and because exactly one stage is in flight at a time.
    const Work = struct {
        gpa: Allocator,
        rar_paths: []const []const u8,
        target_dir: []const u8,
        count: usize = 0,
        err: ?anyerror = null,
        ok: bool = false,

        fn body(w: *Work) void {
            var result = rar_extract.extract(w.gpa, io(), Io.Dir.cwd(), .{
                .archive_paths = w.rar_paths,
                .target_dir = w.target_dir,
            }) catch |e| {
                w.err = e;
                return;
            };
            defer result.deinit();
            w.count = result.files.len;
            w.ok = true;
        }
    };

    fn doExtract(
        ctx: *anyopaque,
        a: Allocator,
        rar_paths: []const []const u8,
        target_dir: []const u8,
    ) extract_app.ExtractorError!usize {
        const self: *Extractor = @ptrCast(@alignCast(ctx));
        _ = a;

        var work: Work = .{
            .gpa = self.gpa,
            .rar_paths = rar_paths,
            .target_dir = target_dir,
        };
        if (!onWorker(self.stages, Work, &work, Work.body)) return error.Io;

        if (!work.ok) {
            const e = work.err orelse error.Unexpected;
            self.logger.warn("rar: extraction failed", &.{log.errv("err", e)});
            return mapExtractError(e);
        }
        return work.count;
    }

    /// The four outcomes the service distinguishes. Everything else is
    /// `Io`, because from the pipeline's point of view an unreadable
    /// volume and a full disk are the same answer: this release did not
    /// unpack and the operator has to look.
    fn mapExtractError(e: anyerror) extract_app.ExtractorError {
        return switch (e) {
            error.OutOfMemory => error.OutOfMemory,
            error.NoArchivePaths, error.ArchiveNotFound => error.MissingVolume,
            error.MissingVolume, error.FileNotFound => error.MissingVolume,
            error.PasswordRequired, error.EncryptedHeaders => error.PasswordRequired,
            error.UnsupportedMethod,
            error.UnsupportedVersion,
            error.CorruptHeader,
            error.BadCrc,
            error.UnverifiableEntry,
            error.TooManyEntries,
            => error.Malformed,
            else => error.Io,
        };
    }
};

// =====================================================================
// The four aggregate stores
// =====================================================================

const stores = @import("stores.zig");
const repo_verify = @import("../store/repo_verify.zig");
const repo_repair = @import("../store/repo_repair.zig");
const repo_extract = @import("../store/repo_extract.zig");
const repo_deliver = @import("../store/repo_deliver.zig");

pub const VerifyStore = stores.AggregateStore(dverify.VerifySet, repo_verify.VerifyRepo);
pub const RepairStore = stores.AggregateStore(drepair.Repair, repo_repair.RepairRepo);
pub const ExtractStore = stores.AggregateStore(dextract.Extract, repo_extract.ExtractRepo);
pub const DeliverStore = stores.AggregateStore(ddeliver.Delivery, repo_deliver.DeliveryRepo);

// =====================================================================
// Notifications
// =====================================================================

/// The last stage of the pipeline: telling somebody it finished.
///
/// The implementation is `bootstrap/notify.zig` rather than this file,
/// for the reason the `wiring` namespace exists at all — one bounded
/// context's composition per file, and notify's is a fiber, a queue and
/// an HTTP/TLS client rather than the two-line adapter every other stage
/// here needs. Re-exported so the pipeline's wiring is reachable from
/// the pipeline's module.
pub const Notifier = @import("notify.zig").Notifier;

// =====================================================================
// Tests
// =====================================================================

const testing = std.testing;

test "the single-threaded Io spawns nothing and still reads a file" {
    // The property that makes it safe to use from the reactor thread: no
    // pool, no allocator, no deinit. If this ever starts needing one, the
    // module comment above is wrong.
    const dir = Io.Dir.cwd();
    const bytes = dir.readFileAlloc(io(), "build.zig", testing.allocator, .unlimited) catch |e| switch (e) {
        // The test binary's cwd is the project root under `zig build`,
        // but an IDE may run it elsewhere; that is not a failure of the
        // thing under test.
        error.FileNotFound => return,
        else => return e,
    };
    defer testing.allocator.free(bytes);
    try testing.expect(bytes.len > 0);
}

test "extraction failures map onto the four outcomes the service distinguishes" {
    // A password-protected archive is an operator action; a corrupt one
    // is a failed job. Collapsing them would put "add the password" in
    // front of somebody whose download is simply damaged.
    try testing.expectEqual(
        @as(extract_app.ExtractorError, error.PasswordRequired),
        Extractor.mapExtractError(error.PasswordRequired),
    );
    try testing.expectEqual(
        @as(extract_app.ExtractorError, error.MissingVolume),
        Extractor.mapExtractError(error.ArchiveNotFound),
    );
    try testing.expectEqual(
        @as(extract_app.ExtractorError, error.Malformed),
        Extractor.mapExtractError(error.CorruptHeader),
    );
    try testing.expectEqual(
        @as(extract_app.ExtractorError, error.OutOfMemory),
        Extractor.mapExtractError(error.OutOfMemory),
    );
    try testing.expectEqual(
        @as(extract_app.ExtractorError, error.Io),
        Extractor.mapExtractError(error.AccessDenied),
    );
}

test "a verifier over a real PAR2 set reports per-file verdicts" {
    const gpa = testing.allocator;
    const fixture = @import("../testserver/fixture.zig");

    var fx = try fixture.generate(gpa, .{
        .name = "pipeline",
        .file_count = 1,
        .file_size = 4096,
        .article_size = 4096,
        .par2_slice_size = 1024,
    });
    defer fx.deinit();

    // Lay the release out the way a finished download would have.
    const sys = @import("../posix/sys.zig");
    const dir = "/tmp/hoardarr-pipeline-verify";
    var fs_impl = @import("infra.zig").RealFs{ .gpa = gpa };
    const fs = fs_impl.filesystem();
    try fs.removeAll(dir);
    try sys.mkdirPath(dir);
    defer fs.removeAll(dir) catch {};

    var par2_paths: std.ArrayList([]const u8) = .empty;
    defer par2_paths.deinit(gpa);
    var data: std.ArrayList(verify_app.DataPath) = .empty;
    defer data.deinit(gpa);
    var owned: std.ArrayList([]u8) = .empty;
    defer {
        for (owned.items) |p| gpa.free(p);
        owned.deinit(gpa);
    }

    for (fx.files) |f| {
        const path = try std.fmt.allocPrint(gpa, "{s}/{s}", .{ dir, f.name });
        try owned.append(gpa, path);
        try fs.writeAt(path, 0, f.bytes, @intCast(f.bytes.len));
        if (f.is_data) {
            try data.append(gpa, .{ .filename = f.name, .path = path });
        } else {
            try par2_paths.append(gpa, path);
        }
    }

    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();

    var v: Verifier = .{ .gpa = gpa };
    var logger: log.Logger = .{};
    v.logger = &logger;

    const clean = try v.port().verify(arena.allocator(), par2_paths.items, data.items);
    try testing.expect(clean.allOk());
    try testing.expectEqual(@as(usize, 1), clean.files.len);

    // Corrupt a byte and the same set must now say so — this is the
    // verdict the whole repair branch hangs off, so "it ran" is not the
    // assertion that matters.
    const target = data.items[0].path;
    try fs.writeAt(target, 0, "\xff\xff\xff\xff", 0);
    const damaged = try v.port().verify(arena.allocator(), par2_paths.items, data.items);
    try testing.expect(!damaged.allOk());
    try testing.expectEqualStrings("md5 mismatch", damaged.files[0].reason);
}

test "a damaged release travels the repair service all the way to repair.ok" {
    // The whole point of the parity, end to end through the real
    // service: a job whose bytes are damaged on disk comes out of
    // `run` as `repair.ok` with the file byte-identical to what was
    // posted. Every part of this is real except the stores and the
    // clock — the PAR2 set, the damage, the reconstruction and the
    // files on disk are all genuine.
    const gpa = testing.allocator;
    const fixture = @import("../testserver/fixture.zig");
    const job_mod = @import("../domain/download/job.zig");
    const ddevents = @import("../domain/download/events.zig");
    const infra = @import("infra.zig");

    var fx = try fixture.generate(gpa, .{
        .name = "repairme",
        .file_count = 2,
        .file_size = 5000,
        .article_size = 4096,
        .par2_slice_size = 1024,
        .recovery_slices = 4,
    });
    defer fx.deinit();

    // A per-run directory under the cache, cleaned up on the way out. A
    // fixed path would leave a failing run's wreckage behind for the
    // next one to trip over.
    var tmp = testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();
    const incomplete = try std.fmt.allocPrint(gpa, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    defer gpa.free(incomplete);

    // One job file per generated file, PAR2 volumes included.
    var owned: std.ArrayList([]u8) = .empty;
    defer {
        for (owned.items) |p| gpa.free(p);
        owned.deinit(gpa);
    }
    var params: std.ArrayList(job_mod.NewFileParams) = .empty;
    defer params.deinit(gpa);
    var segs: std.ArrayList([1]job_mod.NewSegmentParams) = .empty;
    defer segs.deinit(gpa);
    try segs.ensureTotalCapacity(gpa, fx.files.len);
    for (fx.files, 0..) |f, i| {
        const mid = try std.fmt.allocPrint(gpa, "seg{d}@hoardarr", .{i});
        try owned.append(gpa, mid);
        segs.appendAssumeCapacity(.{.{
            .seq_index = 1,
            .message_id = mid,
            .bytes = @intCast(f.bytes.len),
        }});
        try params.append(gpa, .{
            .filename = f.name,
            .is_par2 = !f.is_data,
            .size_bytes = @intCast(f.bytes.len),
            .segments = &segs.items[i],
        });
    }

    var jobs = dl_ports.FakeJobStore.init(gpa);
    defer jobs.deinit();
    const job = try gpa.create(job_mod.Job);
    job.* = try job_mod.Job.init(gpa, .{
        .nzb_hash = "repairme",
        .name = "repairme",
        .files = params.items,
    }, 0);
    try jobs.insert(job);
    ddevents.deinitAll(gpa, try job.pullEvents());

    // Lay the release out where `partitionJobPaths` will look for it.
    var fs_impl = infra.RealFs{ .gpa = gpa };
    const fs = fs_impl.filesystem();
    const job_dir = try std.fmt.allocPrint(gpa, "{s}/{d}", .{ incomplete, job.id });
    defer gpa.free(job_dir);
    try fs.mkdirAll(job_dir);

    var target: []const u8 = "";
    for (job.files) |jf| {
        const bytes = fx.fileBytes(jf.filename).?;
        const p = try std.fmt.allocPrint(gpa, "{s}/{d}.tmp", .{ job_dir, jf.id });
        try owned.append(gpa, p);
        try fs.writeAt(p, 0, bytes, @intCast(bytes.len));
        if (!jf.is_par2 and target.len == 0) target = p;
    }

    // Damage one slice of the first data file.
    try fs.writeAt(target, 100, "\xff\xff\xff\xff\xff\xff\xff\xff", 0);

    var repairs = app_ports.FakeRepo(drepair.Repair).init(gpa);
    defer repairs.deinit();
    var sink: app_ports.FakeSink(drepair.Event) = .{};
    var dl_sink: app_ports.FakeSink(ddevents.Event) = .{};
    var ftx: app_ports.FakeTx = .{};
    var clock: app_ports.FakeClock = .{ .t = 9_000 };
    var logger: log.Logger = .{};
    var r: Repairer = .{ .logger = &logger };

    var svc: repair_app.Service = .{
        .gpa = gpa,
        .jobs = jobs.store(),
        .store = repairs.repo(),
        .repairer = r.port(),
        .sink = sink.sink(),
        .downloads = dl_sink.sink(),
        .txm = ftx.manager(),
        .fs = fs,
        .clock = clock.clock(),
        .logger = &logger,
        .incomplete_dir = incomplete,
    };

    try testing.expectEqual(repair_app.Outcome.ok, try svc.run(job.id));
    try testing.expectEqual(drepair.State.ok, repairs.get(job.id).?.state);
    try testing.expect(sink.has("repair.ok"));
    try testing.expect(!sink.has("repair.failed"));
    // The job stays untouched: verify re-runs off `repair.ok` and
    // decides from there.
    try testing.expectEqual(@as(usize, 0), dl_sink.n);
    try testing.expect(ftx.balanced());

    // And the bytes on disk are the ones that were posted.
    const repaired = try Io.Dir.cwd().readFileAlloc(io(), target, gpa, .limited(1 << 20));
    defer gpa.free(repaired);
    try testing.expectEqualSlices(u8, fx.files[0].bytes, repaired);
}
