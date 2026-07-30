//! Reed-Solomon repair of a damaged release against its PAR2 set.
//!
//! `verifier.zig` answers "is this release intact"; this answers "make it
//! intact". The three jobs are locating the damage, solving for the lost
//! slices, and putting the result on disk without ever making things
//! worse than they already were.
//!
//! ## Locating damage
//!
//! A file's slices are checked against the IFSC table one at a time. Two
//! shortcuts matter in practice, because on Usenet the common failure is
//! not a flipped bit but a file that stops early:
//!
//!   * a slice that starts past the end of the file on disk is missing by
//!     definition and is never read or hashed;
//!   * a file with no usable IFSC table falls back to the whole-file MD5,
//!     which can only say all-present or all-missing — correct, just
//!     expensive in parity.
//!
//! ## Obfuscated names
//!
//! A file the PAR2 names but the NZB does not is looked up by the MD5 of
//! its first 16 KiB, which `FileDesc` records for exactly this purpose.
//! The repaired bytes are written back to the path the file was *found*
//! at, never to the PAR2-recorded name — the job's own file mapping is
//! the one the rest of the pipeline uses.
//!
//! That handle has one hole, inherited from the format: damage inside
//! the first 16 KiB changes the digest, so a renamed file damaged there
//! is unrecognisable and is reported as absent rather than silently
//! paired with whatever else is lying around.
//!
//! ## Bounded memory
//!
//! A release is routinely tens of gigabytes, so nothing here holds more
//! than a handful of slices. The reconstruction is written as a streaming
//! residual accumulation rather than a call to `rs.reconstruct`, which
//! wants every present slice in RAM at once:
//!
//!     residual[k] = R_{e_k} - Σ over present i of α^(i·e_k) · D_i
//!
//! is accumulated by reading each present slice once, in canonical order,
//! and folding it into all `m` accumulators before dropping it. What
//! remains is an m×m system in the missing slices, the same one
//! `rs.reconstruct` solves, and the same matrix inversion solves it.
//!
//! Peak working set is `(2m + 2) · slice_size` on top of the parsed PAR2
//! set, where `m` is the number of *missing* slices — never a function of
//! how large the release is. Two passes are made over the data files
//! (locate, then accumulate) plus one read/write of each damaged file.
//!
//! ## Never destroy data
//!
//! Every repaired file is assembled under a temporary name in the same
//! directory, read back and re-verified against the PAR2 descriptor, and
//! only then `rename`d into place. Rename within a filesystem is atomic,
//! so a reader sees either the old file or the complete new one. A repair
//! that fails leaves the damaged original exactly as it was: a damaged
//! file can be re-downloaded, whereas a half-written one looks fine and
//! is not.

const std = @import("std");
const builtin = @import("builtin");
const Io = std.Io;
const Md5 = std.crypto.hash.Md5;

const par2 = @import("par2.zig");
const rs = @import("rs.zig");
const gf16 = @import("gf16.zig");
const Matrix = @import("matrix.zig").Matrix;
const verifier = @import("verifier.zig");

/// The name a file is known by plus where it landed. Same shape the
/// verifier takes, so a caller can pass one list to both.
pub const DataFile = verifier.DataFile;

/// PAR2 indexes recovery slices by a 16-bit exponent, which caps a
/// recovery set at 65535 input slices. A set claiming more is either
/// corrupt or hostile; either way it is not a set we can solve, and the
/// cap is what keeps a forged `size` field from provoking a huge
/// allocation.
pub const max_data_slices = 65535;

/// Ceiling on the declared slice size. Real sets sit between 64 KiB and a
/// few MiB; this only exists so a forged Main packet cannot ask for a
/// gigabyte-per-accumulator working set.
pub const max_slice_size = 1 << 28;

pub const RepairError = error{
    OutOfMemory,
    /// Zero, odd, or absurdly large. PAR2 slices are arrays of 16-bit
    /// field elements, so an odd size is not even representable.
    BadSliceSize,
    /// The declared file sizes add up to more slices than PAR2 can index.
    TooManySlices,
    /// The Main packet lists a file id that no FileDesc describes, so the
    /// canonical slice ordering — which the Reed-Solomon coefficients are
    /// indexed by — cannot be built at all.
    IncompleteSet,
    /// The chosen recovery slices do not span the missing ones. Distinct
    /// exponents make the coefficient matrix Vandermonde-like and so
    /// invertible, which is why this should not happen; if it does, the
    /// answer is the same as a shortfall — find more parity.
    Singular,
};

pub const Error = par2.ReadError || RepairError;

/// A file that was rebuilt, and how much of it had to be.
pub const Repaired = struct {
    /// PAR2-declared name. Owned.
    filename: []u8,
    /// Where it was written. Not owned — it is the caller's `DataFile`.
    path: []const u8,
    slices_repaired: usize,
};

pub const Failure = struct {
    /// PAR2-declared name. Owned.
    filename: []u8,
    /// Owned. Carries the numbers, not just a category.
    reason: []u8,
};

/// How far short of repairable the set was. Set only when the pass did
/// not attempt reconstruction at all.
pub const Shortfall = struct {
    damaged_slices: usize,
    recovery_slices: usize,
};

pub const Result = struct {
    repaired: std.ArrayList(Repaired) = .empty,
    already_ok: std.ArrayList([]u8) = .empty,
    failed: std.ArrayList(Failure) = .empty,
    /// Non-null when there were fewer recovery slices than damaged ones.
    /// No file was touched in that case — a partial repair is worse than
    /// none, because it destroys the evidence of what was wrong.
    shortfall: ?Shortfall = null,
    /// How many PAR2 descriptors were matched to a file on disk by
    /// content rather than by name. Non-zero means an obfuscated release.
    matched_by_content: usize = 0,

    pub fn deinit(r: *Result, alloc: std.mem.Allocator) void {
        for (r.repaired.items) |x| alloc.free(x.filename);
        r.repaired.deinit(alloc);
        for (r.already_ok.items) |x| alloc.free(x);
        r.already_ok.deinit(alloc);
        for (r.failed.items) |x| {
            alloc.free(x.filename);
            alloc.free(x.reason);
        }
        r.failed.deinit(alloc);
        r.* = undefined;
    }

    pub fn ok(r: *const Result) bool {
        return r.shortfall == null and r.failed.items.len == 0;
    }
};

/// Parses `par2_paths` and repairs whatever they declare damaged.
/// All paths are relative to `dir`.
pub fn repair(
    alloc: std.mem.Allocator,
    io: Io,
    dir: Io.Dir,
    par2_paths: []const []const u8,
    data: []const DataFile,
) Error!Result {
    var set = try par2.parseFiles(alloc, io, dir, par2_paths);
    defer set.deinit(alloc);
    return repairSet(alloc, io, dir, &set, data);
}

// ---------------------------------------------------------------------
// Per-file workspace
// ---------------------------------------------------------------------

const FileState = struct {
    pf: *const par2.ParFile,
    /// Where it is on disk, or null when nothing matched it.
    path: ?[]const u8,
    /// Resolved through the content index rather than by name.
    by_content: bool,
    /// First global slice index belonging to this file.
    start: usize,
    count: usize,
    /// How many of this file's slices are damaged or absent.
    missing: usize,
    /// On disk but not the declared length. Almost always implies a
    /// damaged tail slice too, except when the declared size is an exact
    /// multiple of the slice size and the extra bytes sit past the last
    /// slice — then every slice checks out and the file is still wrong.
    /// Rewriting truncates it, and needs no parity to do so.
    oversize: bool,
};

/// `repair` against an already-parsed recovery set.
pub fn repairSet(
    alloc: std.mem.Allocator,
    io: Io,
    dir: Io.Dir,
    set: *const par2.RecoverySet,
    data: []const DataFile,
) Error!Result {
    if (set.slice_size == 0 or set.slice_size % 2 != 0) return error.BadSliceSize;
    if (set.slice_size > max_slice_size) return error.BadSliceSize;
    const slice_size: usize = @intCast(set.slice_size);

    var result: Result = .{};
    errdefer result.deinit(alloc);

    // --- resolve every declared file to a path ------------------------
    var states: std.ArrayList(FileState) = .empty;
    defer states.deinit(alloc);
    try states.ensureTotalCapacity(alloc, set.recovery_files.items.len);

    // Built lazily: a release whose names all line up never hashes a
    // prefix it did not otherwise need.
    var index: ?ContentIndex = null;
    defer if (index) |*i| i.deinit(alloc);

    var total_slices: usize = 0;
    for (set.recovery_files.items) |id| {
        // No descriptor means no size, and no size means every slice
        // index after this file is wrong. Refusing is the only safe
        // answer; guessing would silently repair the wrong bytes.
        const pf = fileById(set, id) orelse return error.IncompleteSet;
        if (pf.name.len == 0 and pf.size == 0 and pf.slices.items.len != 0) {
            return error.IncompleteSet;
        }

        const count = try sliceCount(pf.size, slice_size);
        if (count > max_data_slices - total_slices) return error.TooManySlices;

        var path = lookupByName(data, pf.name);
        var by_content = false;
        if (path == null) {
            if (index == null) index = try buildContentIndex(alloc, io, dir, data);
            path = index.?.map.get(pf.md5_16k);
            if (path != null) {
                by_content = true;
                result.matched_by_content += 1;
            }
        }

        states.appendAssumeCapacity(.{
            .pf = pf,
            .path = path,
            .by_content = by_content,
            .start = total_slices,
            .count = count,
            .missing = 0,
            .oversize = false,
        });
        total_slices += count;
    }

    // --- locate the damage --------------------------------------------
    var missing: std.ArrayList(usize) = .empty;
    defer missing.deinit(alloc);

    // One slice-sized staging buffer, reused for every read in every
    // pass. This is the whole of the per-slice memory cost.
    const buf = try alloc.alloc(u8, slice_size);
    defer alloc.free(buf);

    var any_oversize = false;
    for (states.items) |*st| {
        const before = missing.items.len;
        try locateDamage(alloc, io, dir, st, slice_size, buf, &missing);
        st.missing = missing.items.len - before;
        if (st.oversize) any_oversize = true;
    }

    if (missing.items.len == 0 and !any_oversize) {
        for (states.items) |st| {
            try result.already_ok.append(alloc, try alloc.dupe(u8, st.pf.name));
        }
        return result;
    }

    // --- is there enough parity? --------------------------------------
    var recovery: std.ArrayList(rs.RecoverySlice) = .empty;
    defer recovery.deinit(alloc);
    {
        var it = set.recovery_slices.iterator();
        while (it.next()) |e| {
            // A body that is not exactly one slice long is not a usable
            // equation; counting it would promise parity we do not have.
            if (e.value_ptr.len != slice_size) continue;
            try recovery.append(alloc, .{ .exponent = e.key_ptr.*, .body = e.value_ptr.* });
        }
    }
    std.mem.sort(rs.RecoverySlice, recovery.items, {}, byExponent);

    if (recovery.items.len < missing.items.len) {
        result.shortfall = .{
            .damaged_slices = missing.items.len,
            .recovery_slices = recovery.items.len,
        };
        for (states.items) |st| {
            if (st.missing == 0) {
                try result.already_ok.append(alloc, try alloc.dupe(u8, st.pf.name));
                continue;
            }
            try result.failed.append(alloc, .{
                .filename = try alloc.dupe(u8, st.pf.name),
                .reason = try std.fmt.allocPrint(
                    alloc,
                    "insufficient recovery: {d} damaged slices in the set, {d} recovery slices available",
                    .{ missing.items.len, recovery.items.len },
                ),
            });
        }
        return result;
    }

    // --- solve ---------------------------------------------------------
    // Nothing to solve when the only fault is a file that is too long;
    // the rewrite below truncates it from bytes that are already there.
    var rebuilt: []u16 = &.{};
    defer if (rebuilt.len != 0) alloc.free(rebuilt);
    if (missing.items.len != 0) {
        rebuilt = try solve(alloc, io, dir, .{
            .states = states.items,
            .missing = missing.items,
            .recovery = recovery.items[0..missing.items.len],
            .slice_size = slice_size,
            .buf = buf,
        });
    }

    // --- write, verify, rename ------------------------------------------
    for (states.items) |st| {
        if (st.missing == 0 and !st.oversize) {
            try result.already_ok.append(alloc, try alloc.dupe(u8, st.pf.name));
            continue;
        }
        try writeRepaired(alloc, io, dir, st, .{
            .missing = missing.items,
            .rebuilt = rebuilt,
            .slice_size = slice_size,
            .buf = buf,
        }, &result);
    }
    return result;
}

fn byExponent(_: void, a: rs.RecoverySlice, b: rs.RecoverySlice) bool {
    return a.exponent < b.exponent;
}

/// `RecoverySet.fileById` over a const set.
fn fileById(set: *const par2.RecoverySet, id: [16]u8) ?*const par2.ParFile {
    for (set.files.items) |*f| {
        if (std.mem.eql(u8, &f.id, &id)) return f;
    }
    return null;
}

fn lookupByName(data: []const DataFile, name: []const u8) ?[]const u8 {
    if (name.len == 0) return null;
    for (data) |d| {
        if (std.mem.eql(u8, d.name, name)) return d.path;
    }
    return null;
}

/// `ceil(size / slice_size)`, refusing anything PAR2 could not index.
/// Written as a division rather than a rounded-up addition because
/// `size` is attacker-controlled and the addition overflows.
fn sliceCount(size: u64, slice_size: usize) RepairError!usize {
    if (size == 0) return 0;
    const ss: u64 = slice_size;
    const n = size / ss + @intFromBool(size % ss != 0);
    if (n > max_data_slices) return error.TooManySlices;
    return @intCast(n);
}

// ---------------------------------------------------------------------
// Damage location
// ---------------------------------------------------------------------

/// Appends this file's damaged global slice indices to `missing`, in
/// ascending order.
fn locateDamage(
    alloc: std.mem.Allocator,
    io: Io,
    dir: Io.Dir,
    st: *FileState,
    slice_size: usize,
    buf: []u8,
    missing: *std.ArrayList(usize),
) error{OutOfMemory}!void {
    if (st.count == 0) return;

    const path = st.path orelse return appendAll(alloc, st, missing);

    var file = dir.openFile(io, path, .{}) catch return appendAll(alloc, st, missing);
    defer file.close(io);

    const on_disk: u64 = if (file.stat(io)) |s| s.size else |_| 0;
    st.oversize = on_disk != st.pf.size;

    // Without a per-slice table the only question we can answer is
    // whether the whole file is right. Expensive in parity when it is
    // not, but the alternative is guessing.
    if (st.pf.slices.items.len != st.count) {
        const reason = verifier.verifyFile(io, dir, path, st.pf.md5, st.pf.size);
        if (reason == .ok) return;
        return appendAll(alloc, st, missing);
    }

    try missing.ensureUnusedCapacity(alloc, st.count);
    for (st.pf.slices.items, 0..) |check, i| {
        const offset: u64 = @as(u64, i) * slice_size;
        // Past the end of what actually arrived: missing without reading
        // a byte. A truncated download is the common case, and hashing
        // megabytes of implied zeroes to learn that is pure waste.
        if (offset >= on_disk) {
            missing.appendAssumeCapacity(st.start + i);
            continue;
        }
        const n = file.readPositionalAll(io, buf, offset) catch {
            missing.appendAssumeCapacity(st.start + i);
            continue;
        };
        // IFSC was computed over the zero-padded slice.
        @memset(buf[n..], 0);
        var sum: [16]u8 = undefined;
        Md5.hash(buf, &sum, .{});
        if (!std.mem.eql(u8, &sum, &check.md5)) {
            missing.appendAssumeCapacity(st.start + i);
        }
    }
}

fn appendAll(
    alloc: std.mem.Allocator,
    st: *const FileState,
    missing: *std.ArrayList(usize),
) error{OutOfMemory}!void {
    try missing.ensureUnusedCapacity(alloc, st.count);
    for (0..st.count) |i| missing.appendAssumeCapacity(st.start + i);
}

// ---------------------------------------------------------------------
// The solve
// ---------------------------------------------------------------------

const SolveInput = struct {
    states: []const FileState,
    /// Ascending global indices of the damaged slices.
    missing: []const usize,
    /// Exactly `missing.len` recovery slices, smallest exponents first —
    /// the same deterministic choice `rs.reconstruct` makes, so a repair
    /// run twice picks the same equations.
    recovery: []const rs.RecoverySlice,
    slice_size: usize,
    buf: []u8,
};

/// Returns the rebuilt slices as one flat `m · elem_count` element array,
/// in the order of `in.missing`.
fn solve(
    alloc: std.mem.Allocator,
    io: Io,
    dir: Io.Dir,
    in: SolveInput,
) RepairError![]u16 {
    const m = in.missing.len;
    const elem_count = in.slice_size / 2;

    // residual[k] starts as the recovery slice and has the contribution
    // of every surviving data slice subtracted off. In GF(2^16),
    // subtraction is XOR, which is what `mulAddSlice` accumulates.
    const residuals = try alloc.alloc(u16, m * elem_count);
    defer alloc.free(residuals);
    for (in.recovery, 0..) |r, k| {
        rs.loadElements(residuals[k * elem_count ..][0..elem_count], r.body);
    }

    const scratch = try alloc.alloc(u16, elem_count);
    defer alloc.free(scratch);

    for (in.states) |st| {
        // Nothing survived here, so there is nothing to subtract and no
        // reason to open the file.
        if (st.missing == st.count) continue;
        const path = st.path orelse continue;
        var file = dir.openFile(io, path, .{}) catch continue;
        defer file.close(io);

        // `missing` is ascending and so is this walk, so one cursor
        // replaces a scan per slice.
        var cursor = lowerBound(in.missing, st.start);
        for (0..st.count) |i| {
            const g = st.start + i;
            if (cursor < in.missing.len and in.missing[cursor] == g) {
                cursor += 1;
                continue;
            }
            const offset: u64 = @as(u64, i) * in.slice_size;
            const n = file.readPositionalAll(io, in.buf, offset) catch 0;
            @memset(in.buf[n..], 0);
            rs.loadElements(scratch, in.buf);

            for (in.recovery, 0..) |r, k| {
                const coef = gf16.expMod(@as(u32, @intCast(g)) *% @as(u32, r.exponent));
                if (coef == 0) continue;
                gf16.mulAddSlice(residuals[k * elem_count ..][0..elem_count], scratch, coef);
            }
        }
    }

    // M[k][j] = α^(missing[j] · e_k). Vandermonde-like in the exponents,
    // hence invertible for distinct rows — the property PAR2 recovery
    // rests on.
    var mat = try Matrix.init(alloc, m, m);
    defer mat.deinit(alloc);
    for (in.recovery, 0..) |r, k| {
        for (in.missing, 0..) |idx, j| {
            mat.set(k, j, gf16.expMod(@as(u32, @intCast(idx)) *% @as(u32, r.exponent)));
        }
    }
    var inv = mat.invert(alloc) catch |err| switch (err) {
        error.OutOfMemory => return error.OutOfMemory,
        // Not solvable from these particular equations. The matrix is
        // square by construction, so this can only be singularity.
        else => return error.Singular,
    };
    defer inv.deinit(alloc);

    const out = try alloc.alloc(u16, m * elem_count);
    errdefer alloc.free(out);
    @memset(out, 0);
    for (0..m) |j| {
        const dst = out[j * elem_count ..][0..elem_count];
        for (0..m) |k| {
            const coef = inv.at(j, k);
            if (coef == 0) continue;
            gf16.mulAddSlice(dst, residuals[k * elem_count ..][0..elem_count], coef);
        }
    }
    return out;
}

/// First index into ascending `xs` whose value is >= `v`.
fn lowerBound(xs: []const usize, v: usize) usize {
    var lo: usize = 0;
    var hi: usize = xs.len;
    while (lo < hi) {
        const mid = lo + (hi - lo) / 2;
        if (xs[mid] < v) lo = mid + 1 else hi = mid;
    }
    return lo;
}

// ---------------------------------------------------------------------
// Writing
// ---------------------------------------------------------------------

const WriteInput = struct {
    missing: []const usize,
    rebuilt: []const u16,
    slice_size: usize,
    buf: []u8,
};

/// Suffix counter, so two repairs in one process cannot collide on a
/// temporary name even inside the same second.
var temp_counter: u32 = 0;

fn currentPid() u32 {
    return switch (builtin.os.tag) {
        .linux => @intCast(std.os.linux.getpid()),
        else => @intCast(std.c.getpid()),
    };
}

/// Assembles the repaired file beside the original, re-verifies it
/// against the PAR2 descriptor, and only then renames it into place.
fn writeRepaired(
    alloc: std.mem.Allocator,
    io: Io,
    dir: Io.Dir,
    st: FileState,
    in: WriteInput,
    result: *Result,
) error{OutOfMemory}!void {
    const path = st.path orelse {
        // Nothing on disk claimed this descriptor. A sidecar the poster
        // never uploaded is not a failed release — the verifier drops
        // those too — but anything else is.
        if (verifier.isQuickCheckIgnorable(st.pf.name)) return;
        return fail(alloc, result, st.pf.name, "no destination path for file", .{});
    };

    temp_counter += 1;
    const tmp = try std.fmt.allocPrint(alloc, "{s}.hoardarr-repair-{d}-{d}.tmp", .{
        path, currentPid(), temp_counter,
    });
    defer alloc.free(tmp);

    // The file may have been lost whole, taking its directory with it.
    if (std.fs.path.dirname(path)) |parent| {
        dir.createDirPath(io, parent) catch {};
    }

    var out = dir.createFile(io, tmp, .{ .truncate = true }) catch |e| {
        return fail(alloc, result, st.pf.name, "create temporary file: {t}", .{e});
    };
    var out_open = true;
    defer if (out_open) out.close(io);

    // Surviving slices are copied straight across; only the damaged ones
    // come from the solve.
    var src: ?Io.File = dir.openFile(io, path, .{}) catch null;
    defer if (src) |*f| f.close(io);

    const elem_count = in.slice_size / 2;
    var cursor = lowerBound(in.missing, st.start);
    var repaired_here: usize = 0;

    for (0..st.count) |i| {
        const g = st.start + i;
        const offset: u64 = @as(u64, i) * in.slice_size;
        if (cursor < in.missing.len and in.missing[cursor] == g) {
            rs.storeElements(in.buf, in.rebuilt[cursor * elem_count ..][0..elem_count]);
            cursor += 1;
            repaired_here += 1;
        } else if (src) |*f| {
            const n = f.readPositionalAll(io, in.buf, offset) catch 0;
            @memset(in.buf[n..], 0);
        } else {
            @memset(in.buf, 0);
        }
        // The final slice is zero-padded in the PAR2 maths but not on
        // disk, so the write stops at the declared size.
        const remaining = st.pf.size - offset;
        const n: usize = @intCast(@min(@as(u64, in.slice_size), remaining));
        out.writePositionalAll(io, in.buf[0..n], offset) catch |e| {
            out.close(io);
            out_open = false;
            dir.deleteFile(io, tmp) catch {};
            return fail(alloc, result, st.pf.name, "write: {t}", .{e});
        };
    }

    out.sync(io) catch {};
    out.close(io);
    out_open = false;

    // Read the bytes back off disk and hash them. Checking the buffer we
    // just built would only prove the arithmetic; this proves the file.
    const reason = verifier.verifyFile(io, dir, tmp, st.pf.md5, st.pf.size);
    if (reason != .ok) {
        dir.deleteFile(io, tmp) catch {};
        return fail(alloc, result, st.pf.name, "post-repair verification: {s}", .{reason.text()});
    }

    dir.rename(tmp, dir, path, io) catch |e| {
        dir.deleteFile(io, tmp) catch {};
        return fail(alloc, result, st.pf.name, "rename into place: {t}", .{e});
    };

    try result.repaired.append(alloc, .{
        .filename = try alloc.dupe(u8, st.pf.name),
        .path = path,
        .slices_repaired = repaired_here,
    });
}

fn fail(
    alloc: std.mem.Allocator,
    result: *Result,
    name: []const u8,
    comptime fmt: []const u8,
    args: anytype,
) error{OutOfMemory}!void {
    const filename = try alloc.dupe(u8, name);
    errdefer alloc.free(filename);
    const reason = try std.fmt.allocPrint(alloc, fmt, args);
    errdefer alloc.free(reason);
    try result.failed.append(alloc, .{ .filename = filename, .reason = reason });
}

// ---------------------------------------------------------------------
// Content-addressed matching
// ---------------------------------------------------------------------

const ContentIndex = struct {
    map: std.AutoHashMapUnmanaged([16]u8, []const u8),

    fn deinit(i: *ContentIndex, alloc: std.mem.Allocator) void {
        i.map.deinit(alloc);
    }
};

/// MD5 of the first 16 KiB of every data file, mapped to its path — the
/// same lookup the verifier uses, and for the same reason: obfuscated
/// releases carry NZB names that share nothing with the PAR2-recorded
/// ones, and `FileDesc.MD516k` exists precisely so a file can be
/// recognised by content instead.
fn buildContentIndex(
    alloc: std.mem.Allocator,
    io: Io,
    dir: Io.Dir,
    data: []const DataFile,
) error{OutOfMemory}!ContentIndex {
    var map: std.AutoHashMapUnmanaged([16]u8, []const u8) = .empty;
    errdefer map.deinit(alloc);
    try map.ensureTotalCapacity(alloc, @intCast(data.len));
    for (data) |d| {
        const digest = verifier.md5First16k(io, dir, d.path) catch continue;
        const gop = map.getOrPutAssumeCapacity(digest);
        if (!gop.found_existing) gop.value_ptr.* = d.path;
    }
    return .{ .map = map };
}

// =====================================================================
// Tests
// =====================================================================

const t = std.testing;
const crc32 = @import("../../core/crc32.zig");
const fixture = @import("../../testserver/fixture.zig");

/// Builds a complete single-file PAR2 set — Main, FileDesc, IFSC, N
/// recovery slices, Creator — over `payload`. `md5_override` exists so a
/// test can produce a set whose descriptor lies about the file, which is
/// the only honest way to exercise the post-repair verification.
fn buildPar2(
    alloc: std.mem.Allocator,
    payload: []const u8,
    name: []const u8,
    slice_size: usize,
    exponents: []const u16,
    md5_override: ?[16]u8,
) ![]u8 {
    const set_id: [16]u8 = @splat(0x5A);
    const file_id: [16]u8 = @splat(0x91);

    const slices = try rs.splitIntoSlices(alloc, payload, slice_size);
    defer rs.freeSlices(alloc, slices);

    const checks = try alloc.alloc(par2.SliceCheck, slices.len);
    defer alloc.free(checks);
    for (slices, checks) |s, *c| {
        var md5: [16]u8 = undefined;
        Md5.hash(s, &md5, .{});
        c.* = .{ .md5 = md5, .crc32 = crc32.checksum(s) };
    }

    var full: [16]u8 = undefined;
    Md5.hash(payload, &full, .{});
    var head: [16]u8 = undefined;
    Md5.hash(payload[0..@min(payload.len, verifier.md5_16k_len)], &head, .{});

    var stream: std.ArrayList(u8) = .empty;
    errdefer stream.deinit(alloc);

    const main = try par2.encodeMain(alloc, set_id, slice_size, &.{file_id});
    defer alloc.free(main);
    try stream.appendSlice(alloc, main);

    const fd = try par2.encodeFileDesc(
        alloc,
        set_id,
        file_id,
        md5_override orelse full,
        head,
        payload.len,
        name,
    );
    defer alloc.free(fd);
    try stream.appendSlice(alloc, fd);

    const ifsc = try par2.encodeIfsc(alloc, set_id, file_id, checks);
    defer alloc.free(ifsc);
    try stream.appendSlice(alloc, ifsc);

    for (exponents) |e| {
        const body = try rs.encodeRecoverySlice(alloc, @ptrCast(slices), e);
        defer alloc.free(body);
        const pkt = try par2.encodeRecvSlc(alloc, set_id, e, body);
        defer alloc.free(pkt);
        try stream.appendSlice(alloc, pkt);
    }

    const creator = try par2.encodeCreator(alloc, set_id, "hoardarr-repair-test");
    defer alloc.free(creator);
    try stream.appendSlice(alloc, creator);

    return stream.toOwnedSlice(alloc);
}

fn randomBytes(alloc: std.mem.Allocator, n: usize, seed: u64) ![]u8 {
    const buf = try alloc.alloc(u8, n);
    var prng = std.Random.DefaultPrng.init(seed);
    prng.random().bytes(buf);
    return buf;
}

fn readAll(alloc: std.mem.Allocator, dir: Io.Dir, path: []const u8) ![]u8 {
    return dir.readFileAlloc(std.testing.io, path, alloc, .limited(1 << 24));
}

/// Names of the entries in `dir`, for asserting that no temporary file
/// was left behind.
fn countEntries(dir: Io.Dir, needle: []const u8) !usize {
    var it = dir.iterate();
    var n: usize = 0;
    while (try it.next(std.testing.io)) |e| {
        if (std.mem.indexOf(u8, e.name, needle) != null) n += 1;
    }
    return n;
}

test "a single damaged slice is rebuilt byte for byte" {
    // Ported from repair_test.go's TestRepair_SingleSliceMissing. The
    // payload is deliberately not a multiple of the slice size so the
    // zero-padded tail is exercised, and there is one more recovery
    // slice than strictly needed.
    const alloc = t.allocator;
    const slice_size = 16;

    var tmp = std.testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();

    const original = try randomBytes(alloc, slice_size * 4 - 3, 0xC0FFEE);
    defer alloc.free(original);

    const par2_bytes = try buildPar2(alloc, original, "release.bin", slice_size, &.{ 1, 2 }, null);
    defer alloc.free(par2_bytes);
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "release.par2", .data = par2_bytes });

    // Corrupt slice 1, in the middle of the file.
    const damaged = try alloc.dupe(u8, original);
    defer alloc.free(damaged);
    @memset(damaged[slice_size .. 2 * slice_size], 0xFF);
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "release.bin", .data = damaged });

    var result = try repair(alloc, std.testing.io, tmp.dir, &.{"release.par2"}, &.{
        .{ .name = "release.bin", .path = "release.bin" },
    });
    defer result.deinit(alloc);

    try t.expect(result.ok());
    try t.expectEqual(@as(usize, 1), result.repaired.items.len);
    try t.expectEqualStrings("release.bin", result.repaired.items[0].filename);
    try t.expectEqual(@as(usize, 1), result.repaired.items[0].slices_repaired);
    try t.expectEqual(@as(usize, 0), result.already_ok.items.len);

    const got = try readAll(alloc, tmp.dir, "release.bin");
    defer alloc.free(got);
    try t.expectEqualSlices(u8, original, got);

    // And no temporary was left lying around.
    try t.expectEqual(@as(usize, 0), try countEntries(tmp.dir, ".tmp"));
}

test "an undamaged set repairs nothing" {
    // Ported from TestRepair_AlreadyOK.
    const alloc = t.allocator;
    var tmp = std.testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();

    const original = try randomBytes(alloc, 48, 0x1234);
    defer alloc.free(original);
    const par2_bytes = try buildPar2(alloc, original, "x.bin", 16, &.{1}, null);
    defer alloc.free(par2_bytes);

    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "x.par2", .data = par2_bytes });
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "x.bin", .data = original });

    var result = try repair(alloc, std.testing.io, tmp.dir, &.{"x.par2"}, &.{
        .{ .name = "x.bin", .path = "x.bin" },
    });
    defer result.deinit(alloc);

    try t.expect(result.ok());
    try t.expectEqual(@as(usize, 0), result.repaired.items.len);
    try t.expectEqual(@as(usize, 1), result.already_ok.items.len);
    try t.expectEqualStrings("x.bin", result.already_ok.items[0]);
}

test "too few recovery slices is reported with the numbers, and nothing is touched" {
    // Ported from TestRepair_TooFewRecoverySlices. Two damaged slices,
    // one recovery slice. The damaged file must come out of this exactly
    // as it went in: a partial repair would destroy the evidence.
    const alloc = t.allocator;
    const slice_size = 16;
    var tmp = std.testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();

    const original = try randomBytes(alloc, slice_size * 4, 0xBEEF);
    defer alloc.free(original);
    const par2_bytes = try buildPar2(alloc, original, "x.bin", slice_size, &.{1}, null);
    defer alloc.free(par2_bytes);
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "x.par2", .data = par2_bytes });

    const damaged = try alloc.dupe(u8, original);
    defer alloc.free(damaged);
    @memset(damaged[0..slice_size], 0xAA);
    @memset(damaged[slice_size .. 2 * slice_size], 0xBB);
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "x.bin", .data = damaged });

    var result = try repair(alloc, std.testing.io, tmp.dir, &.{"x.par2"}, &.{
        .{ .name = "x.bin", .path = "x.bin" },
    });
    defer result.deinit(alloc);

    try t.expect(!result.ok());
    try t.expect(result.shortfall != null);
    try t.expectEqual(@as(usize, 2), result.shortfall.?.damaged_slices);
    try t.expectEqual(@as(usize, 1), result.shortfall.?.recovery_slices);
    try t.expectEqual(@as(usize, 1), result.failed.items.len);
    try t.expectEqualStrings("x.bin", result.failed.items[0].filename);
    try t.expect(std.mem.indexOf(u8, result.failed.items[0].reason, "2 damaged") != null);
    try t.expect(std.mem.indexOf(u8, result.failed.items[0].reason, "1 recovery") != null);
    try t.expectEqual(@as(usize, 0), result.repaired.items.len);

    // Untouched, byte for byte.
    const after = try readAll(alloc, tmp.dir, "x.bin");
    defer alloc.free(after);
    try t.expectEqualSlices(u8, damaged, after);
    try t.expectEqual(@as(usize, 0), try countEntries(tmp.dir, ".tmp"));
}

test "a lying descriptor fails verification and leaves the original alone" {
    // The reconstruction here is arithmetically perfect but the FileDesc
    // MD5 does not describe the file, so the read-back check has to
    // reject it. Without that check this would report success over a
    // file nobody validated — the failure mode that quietly corrupts a
    // library.
    const alloc = t.allocator;
    const slice_size = 16;
    var tmp = std.testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();

    const original = try randomBytes(alloc, slice_size * 3, 0x7777);
    defer alloc.free(original);
    const lie: [16]u8 = @splat(0xDD);
    const par2_bytes = try buildPar2(alloc, original, "x.bin", slice_size, &.{ 1, 2 }, lie);
    defer alloc.free(par2_bytes);
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "x.par2", .data = par2_bytes });

    const damaged = try alloc.dupe(u8, original);
    defer alloc.free(damaged);
    @memset(damaged[0..slice_size], 0x00);
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "x.bin", .data = damaged });

    var result = try repair(alloc, std.testing.io, tmp.dir, &.{"x.par2"}, &.{
        .{ .name = "x.bin", .path = "x.bin" },
    });
    defer result.deinit(alloc);

    try t.expect(!result.ok());
    try t.expectEqual(@as(usize, 0), result.repaired.items.len);
    try t.expectEqual(@as(usize, 1), result.failed.items.len);
    try t.expect(std.mem.indexOf(u8, result.failed.items[0].reason, "post-repair") != null);

    // The damaged original survives, and the temporary is gone.
    const after = try readAll(alloc, tmp.dir, "x.bin");
    defer alloc.free(after);
    try t.expectEqualSlices(u8, damaged, after);
    try t.expectEqual(@as(usize, 0), try countEntries(tmp.dir, ".tmp"));
}

/// Lays a generated release out in `dir` and returns the PAR2 names and
/// the data-file list, both owned by `alloc`.
const Laid = struct {
    par2_names: [][]const u8,
    data: []DataFile,

    fn deinit(l: *Laid, alloc: std.mem.Allocator) void {
        alloc.free(l.par2_names);
        alloc.free(l.data);
    }
};

fn layOut(alloc: std.mem.Allocator, dir: Io.Dir, fx: *const fixture.Fixture) !Laid {
    var par2_names: std.ArrayList([]const u8) = .empty;
    errdefer par2_names.deinit(alloc);
    var data: std.ArrayList(DataFile) = .empty;
    errdefer data.deinit(alloc);

    for (fx.files) |f| {
        try dir.writeFile(std.testing.io, .{ .sub_path = f.name, .data = f.bytes });
        if (f.is_data) {
            try data.append(alloc, .{ .name = f.name, .path = f.name });
        } else {
            try par2_names.append(alloc, f.name);
        }
    }
    return .{
        .par2_names = try par2_names.toOwnedSlice(alloc),
        .data = try data.toOwnedSlice(alloc),
    };
}

const two_files: fixture.Options = .{
    .name = "rel",
    .file_count = 2,
    .file_size = 5000,
    .article_size = 4096,
    .par2_slice_size = 1024,
    .recovery_slices = 6,
};

test "damage spread across two files is repaired and re-verifies" {
    const alloc = t.allocator;
    var fx = try fixture.generate(alloc, two_files);
    defer fx.deinit();

    var tmp = std.testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();
    var laid = try layOut(alloc, tmp.dir, &fx);
    defer laid.deinit(alloc);
    try t.expectEqual(@as(usize, 2), laid.data.len);

    // One slice in the first file, two in the second.
    for ([_]struct { usize, usize }{ .{ 0, 100 }, .{ 1, 40 }, .{ 1, 3000 } }) |spec| {
        const f = fx.files[spec[0]];
        const bad = try alloc.dupe(u8, f.bytes);
        defer alloc.free(bad);
        bad[spec[1]] ^= 0xFF;
        try tmp.dir.writeFile(std.testing.io, .{ .sub_path = f.name, .data = bad });
    }

    var result = try repair(alloc, std.testing.io, tmp.dir, laid.par2_names, laid.data);
    defer result.deinit(alloc);

    try t.expect(result.ok());
    try t.expectEqual(@as(usize, 2), result.repaired.items.len);

    for (fx.files) |f| {
        if (!f.is_data) continue;
        const got = try readAll(alloc, tmp.dir, f.name);
        defer alloc.free(got);
        try t.expectEqualSlices(u8, f.bytes, got);
    }

    // And the verifier — the thing the pipeline actually asks — agrees.
    var vdata: std.ArrayList(verifier.DataFile) = .empty;
    defer vdata.deinit(alloc);
    for (laid.data) |d| try vdata.append(alloc, .{ .name = d.name, .path = d.path });
    var vr = try verifier.verify(alloc, std.testing.io, tmp.dir, laid.par2_names, vdata.items);
    defer vr.deinit(alloc);
    try t.expect(vr.allOk());
}

test "a whole missing file is rebuilt from parity alone" {
    const alloc = t.allocator;
    var fx = try fixture.generate(alloc, .{
        .name = "gone",
        .file_count = 2,
        .file_size = 3000,
        .article_size = 4096,
        .par2_slice_size = 1024,
        // File 0 is three slices; losing it whole needs three.
        .recovery_slices = 4,
    });
    defer fx.deinit();

    var tmp = std.testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();
    var laid = try layOut(alloc, tmp.dir, &fx);
    defer laid.deinit(alloc);

    try tmp.dir.deleteFile(std.testing.io, fx.files[0].name);

    var result = try repair(alloc, std.testing.io, tmp.dir, laid.par2_names, laid.data);
    defer result.deinit(alloc);

    try t.expect(result.ok());
    try t.expectEqual(@as(usize, 1), result.repaired.items.len);
    try t.expectEqual(@as(usize, 3), result.repaired.items[0].slices_repaired);

    const got = try readAll(alloc, tmp.dir, fx.files[0].name);
    defer alloc.free(got);
    try t.expectEqualSlices(u8, fx.files[0].bytes, got);
}

test "one slice more than there is parity fails cleanly with every original intact" {
    const alloc = t.allocator;
    var fx = try fixture.generate(alloc, .{
        .name = "short",
        .file_count = 2,
        .file_size = 4096,
        .article_size = 4096,
        .par2_slice_size = 1024,
        .recovery_slices = 2,
    });
    defer fx.deinit();

    var tmp = std.testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();
    var laid = try layOut(alloc, tmp.dir, &fx);
    defer laid.deinit(alloc);

    // Three damaged slices against two recovery slices.
    const bad0 = try alloc.dupe(u8, fx.files[0].bytes);
    defer alloc.free(bad0);
    bad0[10] ^= 0xFF;
    bad0[1030] ^= 0xFF;
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = fx.files[0].name, .data = bad0 });

    const bad1 = try alloc.dupe(u8, fx.files[1].bytes);
    defer alloc.free(bad1);
    bad1[2050] ^= 0xFF;
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = fx.files[1].name, .data = bad1 });

    var result = try repair(alloc, std.testing.io, tmp.dir, laid.par2_names, laid.data);
    defer result.deinit(alloc);

    try t.expect(!result.ok());
    try t.expectEqual(@as(usize, 3), result.shortfall.?.damaged_slices);
    try t.expectEqual(@as(usize, 2), result.shortfall.?.recovery_slices);
    try t.expectEqual(@as(usize, 2), result.failed.items.len);
    try t.expectEqual(@as(usize, 0), result.repaired.items.len);

    // Both files still hold exactly the damaged bytes we wrote.
    const a0 = try readAll(alloc, tmp.dir, fx.files[0].name);
    defer alloc.free(a0);
    try t.expectEqualSlices(u8, bad0, a0);
    const a1 = try readAll(alloc, tmp.dir, fx.files[1].name);
    defer alloc.free(a1);
    try t.expectEqualSlices(u8, bad1, a1);
    try t.expectEqual(@as(usize, 0), try countEntries(tmp.dir, ".tmp"));
}

/// Files large enough that a damaged byte can sit beyond the 16 KiB the
/// content-addressed match hashes.
const obfuscated_opts: fixture.Options = .{
    .name = "obf",
    .file_count = 2,
    .file_size = 20000,
    .article_size = 8192,
    .par2_slice_size = 1024,
    .recovery_slices = 4,
};

test "an obfuscated release is matched by content and repaired under its disk name" {
    // The release landed under names the PAR2 has never heard of, which
    // is the normal state of affairs for an obfuscated post. The repair
    // has to find the files by MD5 of their first 16 KiB and write the
    // result back under the name it found, not the one PAR2 records.
    const alloc = t.allocator;
    var fx = try fixture.generate(alloc, obfuscated_opts);
    defer fx.deinit();

    var tmp = std.testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();

    var par2_names: std.ArrayList([]const u8) = .empty;
    defer par2_names.deinit(alloc);
    var data: std.ArrayList(DataFile) = .empty;
    defer data.deinit(alloc);

    const obfuscated: [2][]const u8 = .{ "3f1c9ab0.1", "3f1c9ab0.2" };
    var di: usize = 0;
    for (fx.files) |f| {
        if (f.is_data) {
            const disk = obfuscated[di];
            di += 1;
            try tmp.dir.writeFile(std.testing.io, .{ .sub_path = disk, .data = f.bytes });
            // The NZB knows it only by the obfuscated name.
            try data.append(alloc, .{ .name = disk, .path = disk });
        } else {
            try tmp.dir.writeFile(std.testing.io, .{ .sub_path = f.name, .data = f.bytes });
            try par2_names.append(alloc, f.name);
        }
    }

    // Beyond the first 16 KiB, so the file is still recognisable by
    // content — see the test below for what happens when it is not.
    const bad = try alloc.dupe(u8, fx.files[1].bytes);
    defer alloc.free(bad);
    bad[18000] ^= 0xFF;
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = obfuscated[1], .data = bad });

    var result = try repair(alloc, std.testing.io, tmp.dir, par2_names.items, data.items);
    defer result.deinit(alloc);

    try t.expect(result.ok());
    try t.expectEqual(@as(usize, 2), result.matched_by_content);
    try t.expectEqual(@as(usize, 1), result.repaired.items.len);
    // Reported under the PAR2 name, written to the obfuscated path.
    try t.expectEqualStrings(fx.files[1].name, result.repaired.items[0].filename);
    try t.expectEqualStrings(obfuscated[1], result.repaired.items[0].path);

    const got = try readAll(alloc, tmp.dir, obfuscated[1]);
    defer alloc.free(got);
    try t.expectEqualSlices(u8, fx.files[1].bytes, got);
    // The PAR2-declared name was never created.
    try t.expectError(
        error.FileNotFound,
        tmp.dir.statFile(std.testing.io, fx.files[1].name, .{}),
    );
}

test "an obfuscated file damaged inside its first 16 KiB cannot be matched" {
    // The hole in content-addressed matching, pinned rather than hidden:
    // MD516k is the only handle PAR2 gives on a renamed file, so damage
    // inside the bytes it covers makes the file unrecognisable. The
    // descriptor then looks entirely absent — which is honest, and is
    // still repairable given enough parity, but there is nowhere on disk
    // to put the result, so it is reported rather than guessed at.
    const alloc = t.allocator;
    var fx = try fixture.generate(alloc, obfuscated_opts);
    defer fx.deinit();

    var tmp = std.testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();

    var par2_names: std.ArrayList([]const u8) = .empty;
    defer par2_names.deinit(alloc);
    var data: std.ArrayList(DataFile) = .empty;
    defer data.deinit(alloc);

    const obfuscated: [2][]const u8 = .{ "9e0d.1", "9e0d.2" };
    var di: usize = 0;
    for (fx.files) |f| {
        if (f.is_data) {
            const disk = obfuscated[di];
            di += 1;
            try tmp.dir.writeFile(std.testing.io, .{ .sub_path = disk, .data = f.bytes });
            try data.append(alloc, .{ .name = disk, .path = disk });
        } else {
            try tmp.dir.writeFile(std.testing.io, .{ .sub_path = f.name, .data = f.bytes });
            try par2_names.append(alloc, f.name);
        }
    }

    const bad = try alloc.dupe(u8, fx.files[1].bytes);
    defer alloc.free(bad);
    bad[100] ^= 0xFF;
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = obfuscated[1], .data = bad });

    var result = try repair(alloc, std.testing.io, tmp.dir, par2_names.items, data.items);
    defer result.deinit(alloc);

    // Only the intact file was recognised; the other descriptor now
    // looks like a file that never arrived, so all 20 of its slices are
    // unknowns against four recovery slices.
    try t.expectEqual(@as(usize, 1), result.matched_by_content);
    try t.expectEqual(@as(usize, 20), result.shortfall.?.damaged_slices);
    try t.expectEqual(@as(usize, 4), result.shortfall.?.recovery_slices);
    try t.expectEqual(@as(usize, 0), result.repaired.items.len);

    const after = try readAll(alloc, tmp.dir, obfuscated[1]);
    defer alloc.free(after);
    try t.expectEqualSlices(u8, bad, after);
}

test "the real ParPar index resolves its obfuscated parts by content" {
    // The checked-in fixture is a genuine 37-part obfuscated release with
    // seven of its parts saved under NZB-side names that share nothing
    // with the PAR2-recorded ones, and truncated to 16 KiB. Repair cannot
    // succeed — the index carries no recovery slices at all — but it must
    // still recognise those seven by content and account for the damage
    // honestly rather than blowing up on the size mismatch.
    const alloc = t.allocator;

    var dir = Io.Dir.cwd().openDir(std.testing.io, par2.fixture_dir, .{}) catch |err| switch (err) {
        error.FileNotFound => return error.SkipZigTest,
        else => return err,
    };
    defer dir.close(std.testing.io);

    const samples = [_][]const u8{
        "sample-1221.bin", "sample-1222.bin", "sample-1223.bin", "sample-1230.bin",
        "sample-1240.bin", "sample-1250.bin", "sample-1255.bin",
    };
    var data: [samples.len]DataFile = undefined;
    for (samples, 0..) |s, i| data[i] = .{ .name = s, .path = s };

    var result = try repair(alloc, std.testing.io, dir, &.{"main.par2"}, &data);
    defer result.deinit(alloc);

    try t.expectEqual(@as(usize, 7), result.matched_by_content);
    try t.expect(result.shortfall != null);
    // 36 full parts of 33 slices plus a 10-slice tail, every one of them
    // damaged or absent, against an index with no parity whatsoever.
    try t.expectEqual(@as(usize, 36 * 33 + 10), result.shortfall.?.damaged_slices);
    try t.expectEqual(@as(usize, 0), result.shortfall.?.recovery_slices);
    try t.expectEqual(@as(usize, 37), result.failed.items.len);
    try t.expectEqual(@as(usize, 0), result.repaired.items.len);

    // Nothing was written into the checked-in fixture directory.
    try t.expectEqual(@as(usize, 0), try countEntries(dir, ".tmp"));
}

test "a set whose Main packet names a file it never describes is refused" {
    // Without the descriptor there is no size, without the size there is
    // no slice count, and every global index after it is wrong. Guessing
    // would repair the wrong bytes into the wrong file.
    const alloc = t.allocator;
    const set_id: [16]u8 = @splat(1);
    const known: [16]u8 = @splat(2);
    const ghost: [16]u8 = @splat(3);

    var stream: std.ArrayList(u8) = .empty;
    defer stream.deinit(alloc);
    const main = try par2.encodeMain(alloc, set_id, 16, &.{ known, ghost });
    defer alloc.free(main);
    try stream.appendSlice(alloc, main);
    const fd = try par2.encodeFileDesc(alloc, set_id, known, @splat(0), @splat(0), 16, "a.bin");
    defer alloc.free(fd);
    try stream.appendSlice(alloc, fd);

    var set = try par2.parse(alloc, stream.items);
    defer set.deinit(alloc);

    try t.expectError(
        error.IncompleteSet,
        repairSet(alloc, std.testing.io, Io.Dir.cwd(), &set, &.{}),
    );
}

test "a forged slice size is refused before anything is allocated" {
    const alloc = t.allocator;
    for ([_]u64{ 0, 15, max_slice_size + 2 }) |bad| {
        var set: par2.RecoverySet = .{};
        defer set.deinit(alloc);
        set.slice_size = bad;
        try t.expectError(
            error.BadSliceSize,
            repairSet(alloc, std.testing.io, Io.Dir.cwd(), &set, &.{}),
        );
    }
}

test "a forged file size cannot provoke an unbounded slice count" {
    const alloc = t.allocator;
    const set_id: [16]u8 = @splat(1);
    const id: [16]u8 = @splat(2);

    var stream: std.ArrayList(u8) = .empty;
    defer stream.deinit(alloc);
    const main = try par2.encodeMain(alloc, set_id, 2, &.{id});
    defer alloc.free(main);
    try stream.appendSlice(alloc, main);
    // Two-byte slices over a file claiming to be eight exabytes: the
    // rounded-up count overflows u64 if computed naively, and would ask
    // for 2^63 slice records if it did not.
    const fd = try par2.encodeFileDesc(
        alloc,
        set_id,
        id,
        @splat(0),
        @splat(0),
        std.math.maxInt(u64),
        "huge.bin",
    );
    defer alloc.free(fd);
    try stream.appendSlice(alloc, fd);

    var set = try par2.parse(alloc, stream.items);
    defer set.deinit(alloc);
    try t.expectError(
        error.TooManySlices,
        repairSet(alloc, std.testing.io, Io.Dir.cwd(), &set, &.{}),
    );
}

test "recovery slices of the wrong length are not counted as parity" {
    // A truncated volume file yields a RecvSlic body that is not one
    // slice long. Counting it would promise an equation we cannot use
    // and turn a clean shortfall report into a failed solve.
    const alloc = t.allocator;
    const slice_size = 16;
    var tmp = std.testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();

    const original = try randomBytes(alloc, slice_size * 3, 0x42);
    defer alloc.free(original);
    const par2_bytes = try buildPar2(alloc, original, "x.bin", slice_size, &.{1}, null);
    defer alloc.free(par2_bytes);

    // Append a second recovery packet whose body is half a slice.
    const runt = try par2.encodeRecvSlc(alloc, @splat(0x5A), 2, &([_]u8{0} ** 8));
    defer alloc.free(runt);
    const both = try std.mem.concat(alloc, u8, &.{ par2_bytes, runt });
    defer alloc.free(both);
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "x.par2", .data = both });

    const damaged = try alloc.dupe(u8, original);
    defer alloc.free(damaged);
    @memset(damaged[0..slice_size], 0xAA);
    @memset(damaged[slice_size .. 2 * slice_size], 0xBB);
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "x.bin", .data = damaged });

    var result = try repair(alloc, std.testing.io, tmp.dir, &.{"x.par2"}, &.{
        .{ .name = "x.bin", .path = "x.bin" },
    });
    defer result.deinit(alloc);

    try t.expectEqual(@as(usize, 1), result.shortfall.?.recovery_slices);
}

test "repair is idempotent: a second pass finds nothing to do" {
    const alloc = t.allocator;
    var fx = try fixture.generate(alloc, two_files);
    defer fx.deinit();

    var tmp = std.testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();
    var laid = try layOut(alloc, tmp.dir, &fx);
    defer laid.deinit(alloc);

    const bad = try alloc.dupe(u8, fx.files[0].bytes);
    defer alloc.free(bad);
    bad[77] ^= 0xFF;
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = fx.files[0].name, .data = bad });

    var first = try repair(alloc, std.testing.io, tmp.dir, laid.par2_names, laid.data);
    defer first.deinit(alloc);
    try t.expectEqual(@as(usize, 1), first.repaired.items.len);

    var second = try repair(alloc, std.testing.io, tmp.dir, laid.par2_names, laid.data);
    defer second.deinit(alloc);
    try t.expect(second.ok());
    try t.expectEqual(@as(usize, 0), second.repaired.items.len);
    try t.expectEqual(@as(usize, 2), second.already_ok.items.len);
}

test "a file that is too long is truncated back, without touching the parity" {
    // The declared size is an exact multiple of the slice size, so the
    // appended bytes fall past the last slice and every IFSC entry still
    // checks out. Calling that "already ok" would be a lie the verifier
    // immediately contradicts — and the fix needs no recovery slices at
    // all, which is why this set ships without any.
    const alloc = t.allocator;
    var fx = try fixture.generate(alloc, .{
        .name = "long",
        .file_count = 1,
        .file_size = 4096,
        .article_size = 4096,
        .par2_slice_size = 1024,
        .recovery_slices = 0,
    });
    defer fx.deinit();

    var tmp = std.testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();
    var laid = try layOut(alloc, tmp.dir, &fx);
    defer laid.deinit(alloc);

    const grown = try std.mem.concat(alloc, u8, &.{ fx.files[0].bytes, &([_]u8{0xAB} ** 200) });
    defer alloc.free(grown);
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = fx.files[0].name, .data = grown });

    var result = try repair(alloc, std.testing.io, tmp.dir, laid.par2_names, laid.data);
    defer result.deinit(alloc);

    try t.expect(result.ok());
    try t.expectEqual(@as(usize, 1), result.repaired.items.len);
    try t.expectEqual(@as(usize, 0), result.repaired.items[0].slices_repaired);

    const got = try readAll(alloc, tmp.dir, fx.files[0].name);
    defer alloc.free(got);
    try t.expectEqualSlices(u8, fx.files[0].bytes, got);
}

test "a set with no PAR2 files at all" {
    var tmp = std.testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();
    try t.expectError(
        error.NoPar2Files,
        repair(t.allocator, std.testing.io, tmp.dir, &.{}, &.{}),
    );
}
