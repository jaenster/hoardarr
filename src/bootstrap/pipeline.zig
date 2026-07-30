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
//! ## What runs on the loop thread, and what that costs
//!
//! These four services run inline in their bus handler, on the reactor
//! thread. Verification hashes every byte of the release and extraction
//! decompresses it, so a large job stalls the event loop for as long as
//! that takes: the HTTP surface stops answering and other downloads stop
//! progressing.
//!
//! That is a real limitation and it is stated here rather than hidden.
//! The alternative — a worker thread — would need its own `*sqlite.Conn`
//! and a hand-off for every state transition, and neither the services
//! nor the aggregates are written for it. The shape that fixes it
//! properly is a fiber whose hashing loop yields every few megabytes,
//! which is a change to `codec/par2`'s reader rather than to this file.

const std = @import("std");

const log = @import("../core/log.zig");
const sqlite = @import("../store/sqlite.zig");

const par2_verifier = @import("../codec/par2/verifier.zig");
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

const Allocator = std.mem.Allocator;
const Io = std.Io;

/// The process-wide `std.Io`.
///
/// `init_single_threaded` is a comptime-constructed value: no thread
/// pool, no allocator, `deinit` not required. Every call through it is a
/// direct syscall on whichever thread made it, which for this daemon is
/// always the loop thread.
pub var io_impl: Io.Threaded = .init_single_threaded;

pub fn io() Io {
    return io_impl.io();
}

// =====================================================================
// PAR2 verification
// =====================================================================

/// `app/verify.zig`'s `Verifier` over `codec/par2`.
pub const Verifier = struct {
    gpa: Allocator,
    logger: *log.Logger = &log.default,

    pub fn port(self: *Verifier) verify_app.Verifier {
        return .{ .ctx = @ptrCast(self), .verifyFn = &doVerify };
    }

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
        const result = par2_verifier.verify(a, io(), Io.Dir.cwd(), par2_paths, files) catch |e| {
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

/// `app/repair.zig`'s `Repairer`.
///
/// **Not implemented.** `codec/par2/rs.zig` has the Reed-Solomon
/// reconstruction and `codec/par2/par2.zig` has the recovery slices, but
/// nothing in the tree joins them into "rebuild these damaged files on
/// disk" — slice-level damage detection, loading the present slices,
/// writing the rebuilt ones back at their offsets.
///
/// Reporting `UnrecoverableSet` is the honest stand-in and it is not
/// inert: it is the one error `app/repair.zig` reacts to rather than
/// reports, so a job with deferred recovery volumes still asks the
/// download context for them, and only a job that already has all its
/// parity ends in `repair.failed`. What the operator sees is "hoardarr
/// could not repair this", which is true.
pub const Repairer = struct {
    logger: *log.Logger = &log.default,

    pub fn port(self: *Repairer) repair_app.Repairer {
        return .{ .ctx = @ptrCast(self), .repairFn = &doRepair };
    }

    fn doRepair(
        ctx: *anyopaque,
        a: Allocator,
        par2_paths: []const []const u8,
        data: []const verify_app.DataPath,
    ) repair_app.RepairerError!repair_app.Report {
        const self: *Repairer = @ptrCast(@alignCast(ctx));
        _ = a;
        _ = data;
        self.logger.warn("par2: reconstruction is not implemented in this build", &.{
            log.uint("par2_files", par2_paths.len),
        });
        return error.UnrecoverableSet;
    }
};

// =====================================================================
// RAR extraction
// =====================================================================

/// `app/extract.zig`'s `Extractor` over `codec/rar`.
pub const Extractor = struct {
    gpa: Allocator,
    logger: *log.Logger = &log.default,

    pub fn port(self: *Extractor) extract_app.Extractor {
        return .{ .ctx = @ptrCast(self), .extractFn = &doExtract };
    }

    fn doExtract(
        ctx: *anyopaque,
        a: Allocator,
        rar_paths: []const []const u8,
        target_dir: []const u8,
    ) extract_app.ExtractorError!usize {
        const self: *Extractor = @ptrCast(@alignCast(ctx));
        _ = a;

        var result = rar_extract.extract(self.gpa, io(), Io.Dir.cwd(), .{
            .archive_paths = rar_paths,
            .target_dir = target_dir,
        }) catch |e| {
            self.logger.warn("rar: extraction failed", &.{log.errv("err", e)});
            return mapExtractError(e);
        };
        defer result.deinit();
        return result.files.len;
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
