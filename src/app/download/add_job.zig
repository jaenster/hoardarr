//! The "add an NZB to the queue" use case, plus the pure functions that
//! turn a parsed NZB into a `Job`.
//!
//! Flow: hash the bytes, parse, name the release, build the file tree,
//! then in one transaction dedupe → insert → publish `JobCreated`.
//!
//! # Duplicates are a result, not an error
//!
//! Go returned a sentinel `ErrDuplicateNZB` alongside the existing job's
//! id, which every caller then had to `errors.Is` and treat as success.
//! Here `addJob` returns `Result{ id, duplicate }`: the id is always the
//! job the caller asked about, and `duplicate` says whether it already
//! existed. An error return is reserved for things that actually went
//! wrong.
//!
//! The dedupe has two layers because it has to. The hash pre-check
//! catches the common case cheaply; the `DuplicateNzbHash` recovery
//! catches the race where two uploads of identical bytes both pass the
//! pre-check and only one can win the INSERT. The second layer is not
//! belt-and-braces — it is the only correct answer, because SQLite is
//! the serialisation point, not the application.

const std = @import("std");
const log = @import("../../core/log.zig");
const nzb = @import("../../codec/nzb.zig");
const app_ports = @import("../ports.zig");
const ports = @import("ports.zig");
const dtx = @import("../../domain/tx.zig");
const job_mod = @import("../../domain/download/job.zig");
const dfile = @import("../../domain/download/file.zig");
const dsegment = @import("../../domain/download/segment.zig");
const devents = @import("../../domain/download/events.zig");

const Allocator = std.mem.Allocator;

pub const Timestamp = app_ports.Timestamp;
pub const Job = job_mod.Job;
pub const JobId = ports.JobId;
pub const NewFileParams = dfile.NewFileParams;
pub const NewSegmentParams = dsegment.NewSegmentParams;
pub const Sink = app_ports.EventSink(devents.Event);

// ---------------------------------------------------------------------
// Filename classification
// ---------------------------------------------------------------------

/// Case-insensitive `endsWith`. NZB filenames come from whoever posted
/// them, so every suffix test in this file has to be case-blind; Go
/// achieved that by lowering the whole string first, which allocated per
/// filename per file per NZB.
pub fn endsWithIgnoreCase(s: []const u8, suffix: []const u8) bool {
    if (suffix.len > s.len) return false;
    return std.ascii.eqlIgnoreCase(s[s.len - suffix.len ..], suffix);
}

/// Index of the last case-insensitive occurrence of `needle`.
pub fn lastIndexIgnoreCase(s: []const u8, needle: []const u8) ?usize {
    if (needle.len == 0 or needle.len > s.len) return null;
    var i = s.len - needle.len + 1;
    while (i > 0) {
        i -= 1;
        if (std.ascii.eqlIgnoreCase(s[i..][0..needle.len], needle)) return i;
    }
    return null;
}

pub fn containsIgnoreCase(s: []const u8, needle: []const u8) bool {
    return lastIndexIgnoreCase(s, needle) != null;
}

fn allDigits(s: []const u8) bool {
    if (s.len == 0) return false;
    for (s) |c| {
        if (c < '0' or c > '9') return false;
    }
    return true;
}

/// Whether the file is parity of any flavour — the small index `.par2`,
/// a recovery volume, or the ancient `.par`. PAR2 files are scratch: the
/// deliver step never moves them into `complete/`.
pub fn isPar2Filename(name: []const u8) bool {
    return endsWithIgnoreCase(name, ".par2") or
        containsIgnoreCase(name, ".vol") or
        endsWithIgnoreCase(name, ".par");
}

/// Whether the file is a per-slice recovery volume
/// (`<base>.vol###+##.par2`) as opposed to the small index `.par2`.
///
/// The distinction earns its keep: the index alone carries the MD5 of
/// every data file, so verification only needs that, and the bulky
/// volumes can stay unfetched until repair actually asks for them.
pub fn isRecoveryVolFilename(name: []const u8) bool {
    if (!endsWithIgnoreCase(name, ".par2")) return false;
    return containsIgnoreCase(name, ".vol");
}

/// Strips the "this is part N of M" suffix from a filename, or null when
/// nothing matched. Returns a subslice of `name`, so no allocation.
///
///     release.part001.rar → release
///     release.r00         → release
///     release.002         → release
///     release.vol00+01.par2 → release
pub fn stripReleaseSuffix(name: []const u8) ?[]const u8 {
    if (endsWithIgnoreCase(name, ".rar")) {
        if (lastIndexIgnoreCase(name, ".part")) |i| return name[0..i];
    }
    // `.r00` … `.r99`: four trailing characters, two of them digits.
    if (lastIndexIgnoreCase(name, ".r")) |i| {
        if (name.len - i == 4 and allDigits(name[i + 2 ..])) return name[0..i];
    }
    // `.001` numeric split.
    if (std.mem.lastIndexOfScalar(u8, name, '.')) |i| {
        if (name.len - i == 4 and allDigits(name[i + 1 ..])) return name[0..i];
    }
    if (endsWithIgnoreCase(name, ".par2")) {
        if (lastIndexIgnoreCase(name, ".vol")) |i| return name[0..i];
    }
    if (endsWithIgnoreCase(name, ".rar") or endsWithIgnoreCase(name, ".par2")) {
        if (std.mem.lastIndexOfScalar(u8, name, '.')) |i| return name[0..i];
    }
    return null;
}

/// Derives a release name from a set of filenames by stripping the
/// common multi-part suffix. Falls back to the first filename verbatim.
pub fn releaseFromFilenames(names: []const []const u8) []const u8 {
    if (names.len == 0) return "";
    for (names) |n| {
        if (stripReleaseSuffix(n)) |base| {
            if (base.len != 0 and base.len != n.len) return base;
        }
    }
    return names[0];
}

/// Picks the job label. `<meta type="title">` wins; otherwise the
/// release name inferred from the data filenames.
///
/// Data files are preferred over PAR2 ones because a label like
/// "release.vol00+01.par2" leaks the volume suffix into the UI and the
/// *arr history. Every returned slice borrows from `doc`.
pub fn chooseName(a: Allocator, doc: *const nzb.Document) Allocator.Error![]const u8 {
    for (doc.meta) |m| {
        if (std.ascii.eqlIgnoreCase(m.kind, "title") or std.ascii.eqlIgnoreCase(m.kind, "name")) {
            if (m.value.len != 0) return m.value;
        }
    }

    var data: std.ArrayList([]const u8) = .empty;
    defer data.deinit(a);
    var all: std.ArrayList([]const u8) = .empty;
    defer all.deinit(a);
    for (doc.files) |f| {
        try all.append(a, f.filename);
        if (!isPar2Filename(f.filename)) try data.append(a, f.filename);
    }
    const from_data = releaseFromFilenames(data.items);
    if (from_data.len != 0) return from_data;
    const from_all = releaseFromFilenames(all.items);
    if (from_all.len != 0) return from_all;
    return "unnamed";
}

// ---------------------------------------------------------------------
// buildFiles
// ---------------------------------------------------------------------

pub const BuiltFiles = struct {
    /// Allocated with the allocator passed to `buildFiles`; every string
    /// inside borrows from the `Document`, which must outlive the
    /// `Job.init` call that copies them.
    files: []NewFileParams,
    total_bytes: i64,

    pub fn deinit(self: BuiltFiles, a: Allocator) void {
        for (self.files) |f| a.free(f.segments);
        a.free(self.files);
    }
};

/// Turns a parsed NZB into the domain's constructor params.
///
/// Duplicate `<segment>` entries within one file are dropped, first
/// occurrence wins — by number *and* by message-id, because both
/// duplicate shapes occur in the wild. A real Sonarr grab in production
/// carried two `<segment number="1">` entries with different message-ids
/// (the poster re-uploaded a missed article and the indexer aggregated
/// both), which made the INSERT hit
/// `UNIQUE(segments.file_id, segments.seq_index)` and turned the whole
/// AddJob into a 400 for the client. SABnzbd dedupes silently; so do we.
pub fn buildFiles(a: Allocator, doc: *const nzb.Document) Allocator.Error!BuiltFiles {
    var out: std.ArrayList(NewFileParams) = .empty;
    errdefer {
        for (out.items) |f| a.free(f.segments);
        out.deinit(a);
    }
    var total: i64 = 0;

    var seen_num: std.AutoHashMapUnmanaged(u32, void) = .empty;
    defer seen_num.deinit(a);
    var seen_msg: std.StringHashMapUnmanaged(void) = .empty;
    defer seen_msg.deinit(a);

    for (doc.files) |f| {
        if (f.segments.len == 0) continue;
        seen_num.clearRetainingCapacity();
        seen_msg.clearRetainingCapacity();

        var segs: std.ArrayList(NewSegmentParams) = .empty;
        errdefer segs.deinit(a);
        var size: i64 = 0;
        for (f.segments) |s| {
            if (seen_num.contains(s.number)) continue;
            if (seen_msg.contains(s.message_id)) continue;
            try seen_num.put(a, s.number, {});
            try seen_msg.put(a, s.message_id, {});
            try segs.append(a, .{
                .seq_index = @intCast(s.number),
                .message_id = s.message_id,
                .bytes = s.bytes,
            });
            size += s.bytes;
        }
        if (segs.items.len == 0) {
            segs.deinit(a);
            continue;
        }
        try out.append(a, .{
            .filename = f.filename,
            .poster = f.poster,
            .groups = f.groups,
            .size_bytes = size,
            .is_par2 = isPar2Filename(f.filename),
            .is_recovery_vol = isRecoveryVolFilename(f.filename),
            .segments = try segs.toOwnedSlice(a),
        });
        total += size;
    }
    return .{ .files = try out.toOwnedSlice(a), .total_bytes = total };
}

// ---------------------------------------------------------------------
// The service
// ---------------------------------------------------------------------

pub const Command = struct {
    /// The raw NZB document.
    nzb: []const u8,
    /// Overrides `<meta type="title">` when non-empty.
    name: []const u8 = "",
    /// Empty for uncategorised.
    category: []const u8 = "",
    /// Lower is higher priority.
    priority: i32 = 0,
    /// The uploading client's user-agent ("Sonarr/4.0.5"), so the UI can
    /// attribute a job to its *arr origin. Empty for manual uploads.
    source: []const u8 = "",
};

pub const Result = struct {
    id: JobId,
    /// True when these exact bytes were already queued. The id is the
    /// existing job's.
    duplicate: bool = false,
};

pub const AddError = error{
    NzbBodyRequired,
    /// The bytes were not a parseable NZB.
    ParseFailed,
    /// Parsed, but every `<file>` was empty.
    NoUsableFiles,
    /// The NZB parsed but the aggregate rejected it.
    InvalidJob,
} || ports.RepoError || app_ports.PublishError || app_ports.TxError;

pub const Service = struct {
    gpa: Allocator,
    store: ports.JobStore,
    sink: Sink,
    txm: app_ports.Manager,
    clock: app_ports.Clock,
    logger: *log.Logger = &log.default,
    /// When on, new jobs are created with `fetch_recovery_vols = false`
    /// and the orchestrator leaves recovery volumes alone until repair
    /// asks for them. Null means off.
    defer_recovery_vols: ?app_ports.Toggle = null,

    pub fn addJob(self: *Service, cmd: Command) AddError!Result {
        if (cmd.nzb.len == 0) return error.NzbBodyRequired;

        var hash_buf: [64]u8 = undefined;
        const hash = sha256Hex(cmd.nzb, &hash_buf);

        var doc = nzb.parse(self.gpa, cmd.nzb) catch |e| switch (e) {
            error.OutOfMemory => return error.OutOfMemory,
            // Well-formed XML that declares nothing to fetch is not a
            // parse problem, and the operator's fix is different: the
            // indexer handed over an empty NZB.
            error.NoFiles, error.FileHasNoSegments => return error.NoUsableFiles,
            else => return error.ParseFailed,
        };
        defer doc.deinit();

        const trimmed = std.mem.trim(u8, cmd.name, " \t\r\n");
        const name = if (trimmed.len != 0) trimmed else try chooseName(self.gpa, &doc);

        const built = try buildFiles(self.gpa, &doc);
        defer built.deinit(self.gpa);
        if (built.files.len == 0) return error.NoUsableFiles;

        const Args = struct {
            svc: *Service,
            hash: []const u8,
            name: []const u8,
            cmd: Command,
            files: []const NewFileParams,
            result: *Result,
        };
        var result: Result = .{ .id = 0 };
        const Body = struct {
            fn run(unit: *dtx.Unit, args: Args) AddError!void {
                const s = args.svc;
                // Cheap pre-check. Catches every duplicate that is not a
                // genuine race.
                if (s.store.byNzbHash(unit, args.hash)) |existing| {
                    defer s.store.release(existing);
                    args.result.* = .{ .id = existing.id, .duplicate = true };
                    return;
                } else |e| {
                    if (e != error.JobNotFound) return e;
                }

                const now = s.clock.now();
                const defer_vols = if (s.defer_recovery_vols) |t| t.read() else false;
                const job = try s.gpa.create(Job);
                job.* = Job.init(s.gpa, .{
                    .nzb_hash = args.hash,
                    .name = args.name,
                    .category = args.cmd.category,
                    .priority = args.cmd.priority,
                    // Ties are broken by id downstream, so two adds
                    // inside one millisecond keep their arrival order.
                    .queue_order = now,
                    .source = args.cmd.source,
                    .nzb_blob = args.cmd.nzb,
                    .files = args.files,
                    .defer_recovery_vols = defer_vols,
                }, now) catch |e| {
                    s.gpa.destroy(job);
                    if (e == error.OutOfMemory) return error.OutOfMemory;
                    return error.InvalidJob;
                };
                // The store adopts the aggregate on a successful insert.
                // Until then this scope owns it — including on the
                // lost-the-race path, which returns normally and so is
                // not covered by an `errdefer`. That omission was a real
                // leak the first time round.
                var adopted = false;
                defer if (!adopted) {
                    job.deinit();
                    s.gpa.destroy(job);
                };

                s.store.save(unit, job) catch |e| {
                    if (e != error.DuplicateNzbHash) return e;
                    // Lost the race. Somebody committed identical bytes
                    // between our pre-check and our INSERT; find them.
                    if (s.store.byNzbHash(unit, args.hash)) |winner| {
                        defer s.store.release(winner);
                        args.result.* = .{ .id = winner.id, .duplicate = true };
                    } else |_| {
                        // The winner vanished again (removed between the
                        // two statements). Report the duplicate without
                        // an id rather than inventing one.
                        args.result.* = .{ .id = 0, .duplicate = true };
                    }
                    return;
                };
                adopted = true;

                args.result.* = .{ .id = job.id, .duplicate = false };
                const events = try job.pullEvents();
                defer devents.deinitAll(s.gpa, events);
                try s.sink.publish(unit, events);
            }
        };
        try dtx.inTx(AddError, self.txm, Args{
            .svc = self,
            .hash = hash,
            .name = name,
            .cmd = cmd,
            .files = built.files,
            .result = &result,
        }, Body.run);

        if (result.duplicate) {
            self.logger.info("addjob: duplicate nzb", &.{
                log.int("job_id", result.id),
                log.str("name", name),
            });
        } else {
            self.logger.info("addjob: queued", &.{
                log.int("job_id", result.id),
                log.str("name", name),
                log.int("total_bytes", built.total_bytes),
                log.uint("files", built.files.len),
            });
        }
        return result;
    }
};

/// Lower-case hex SHA-256 of `body`, written into `out`.
pub fn sha256Hex(body: []const u8, out: *[64]u8) []const u8 {
    var digest: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(body, &digest, .{});
    return std.fmt.bufPrint(out, "{x}", .{&digest}) catch unreachable;
}

// =====================================================================
// Tests — internal/app/download/add_job_buildfiles_test.go and
// concurrent_addjob_test.go, plus the naming heuristics Go left
// uncovered.
// =====================================================================

const testing = std.testing;

const nzb_one_file =
    \\<?xml version="1.0" encoding="UTF-8"?>
    \\<nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">
    \\  <head><meta type="title">Concurrent</meta></head>
    \\  <file poster="x" date="0" subject='[1/1] - "concurrent.bin" yEnc'>
    \\    <groups><group>g</group></groups>
    \\    <segments>
    \\      <segment bytes="100" number="1">m1@h</segment>
    \\    </segments>
    \\  </file>
    \\</nzb>
;

/// The duplicate-segment shape a real Sonarr grab produced.
const nzb_dupe_segments =
    \\<nzb>
    \\  <file subject='[1/1] - "release.part01.rar" yEnc'>
    \\    <segments>
    \\      <segment bytes="700000" number="1">first@host</segment>
    \\      <segment bytes="700000" number="2">second@host</segment>
    \\      <segment bytes="700000" number="1">first-reposted@host</segment>
    \\      <segment bytes="700000" number="3">third@host</segment>
    \\      <segment bytes="700000" number="4">third@host</segment>
    \\    </segments>
    \\  </file>
    \\</nzb>
;

const Harness = struct {
    store: ports.FakeJobStore = undefined,
    sink: app_ports.FakeSink(devents.Event) = .{},
    ftx: app_ports.FakeTx = .{},
    clock: app_ports.FakeClock = .{ .t = 1_700_000_000_000 },
    defer_vols: app_ports.FakeToggle = .{},
    logger: log.Logger = .{},
    svc: Service = undefined,

    fn init(self: *Harness) void {
        self.* = .{};
        self.store = ports.FakeJobStore.init(testing.allocator);
        self.svc = .{
            .gpa = testing.allocator,
            .store = self.store.store(),
            .sink = self.sink.sink(),
            .txm = self.ftx.manager(),
            .clock = self.clock.clock(),
            .logger = &self.logger,
            .defer_recovery_vols = self.defer_vols.toggle(),
        };
    }

    fn deinit(self: *Harness) void {
        self.store.deinit();
    }
};

// ---- filename classification ----------------------------------------

test "par2 and recovery-vol classification is case blind" {
    try testing.expect(isPar2Filename("set.par2"));
    try testing.expect(isPar2Filename("SET.PAR2"));
    try testing.expect(isPar2Filename("set.vol000+01.par2"));
    try testing.expect(isPar2Filename("old.par"));
    try testing.expect(!isPar2Filename("movie.mkv"));

    // The index par2 is not a recovery volume even though both are par2.
    try testing.expect(!isRecoveryVolFilename("set.par2"));
    try testing.expect(isRecoveryVolFilename("set.vol000+01.par2"));
    try testing.expect(isRecoveryVolFilename("SET.VOL00-01.PAR2"));
    try testing.expect(!isRecoveryVolFilename("set.vol000+01.rar"));
}

test "release suffixes are stripped and unknown shapes pass through" {
    const cases = [_]struct { []const u8, ?[]const u8 }{
        .{ "release.part001.rar", "release" },
        .{ "release.PART01.RAR", "release" },
        .{ "release.r00", "release" },
        .{ "release.r99", "release" },
        .{ "release.001", "release" },
        .{ "release.vol00+01.par2", "release" },
        .{ "release.rar", "release" },
        .{ "release.par2", "release" },
        // Not a part suffix: three digits are required, and .mkv is not
        // an archive extension.
        .{ "release.r0", null },
        .{ "movie.mkv", null },
        .{ "noextension", null },
    };
    for (cases) |c| {
        const got = stripReleaseSuffix(c[0]);
        if (c[1]) |want| {
            try testing.expectEqualStrings(want, got.?);
        } else {
            try testing.expectEqual(@as(?[]const u8, null), got);
        }
    }
}

test "releaseFromFilenames prefers the first strippable name" {
    try testing.expectEqualStrings("release", releaseFromFilenames(&.{
        "sidecar.nfo", "release.part01.rar", "release.part02.rar",
    }));
    // Nothing strippable: the first name verbatim.
    try testing.expectEqualStrings("only.nfo", releaseFromFilenames(&.{"only.nfo"}));
    try testing.expectEqualStrings("", releaseFromFilenames(&.{}));
}

test "lastIndexIgnoreCase finds the last match and rejects overlong needles" {
    try testing.expectEqual(@as(?usize, 7), lastIndexIgnoreCase("a.rar.b.RAR", ".rar"));
    try testing.expectEqual(@as(?usize, 0), lastIndexIgnoreCase("abc", "ABC"));
    try testing.expectEqual(@as(?usize, null), lastIndexIgnoreCase("ab", "abc"));
    try testing.expectEqual(@as(?usize, null), lastIndexIgnoreCase("abc", ""));
}

// ---- buildFiles -----------------------------------------------------

test "buildFiles dedupes duplicate segments, first occurrence wins" {
    var doc = try nzb.parse(testing.allocator, nzb_dupe_segments);
    defer doc.deinit();
    const built = try buildFiles(testing.allocator, &doc);
    defer built.deinit(testing.allocator);

    try testing.expectEqual(@as(usize, 1), built.files.len);
    const segs = built.files[0].segments;
    // Number 1 repeated with a different message-id, and number 4
    // reusing number 3's message-id: both dropped.
    try testing.expectEqual(@as(usize, 3), segs.len);
    try testing.expectEqual(@as(i32, 1), segs[0].seq_index);
    try testing.expectEqual(@as(i32, 2), segs[1].seq_index);
    try testing.expectEqual(@as(i32, 3), segs[2].seq_index);
    try testing.expectEqualStrings("first@host", segs[0].message_id);
    // The size totals reflect the three surviving segments only.
    try testing.expectEqual(@as(i64, 3 * 700_000), built.files[0].size_bytes);
    try testing.expectEqual(@as(i64, 3 * 700_000), built.total_bytes);
}

test "buildFiles drops files with no segments" {
    // The parser rejects a segment-less `<file>` outright, so this shape
    // can only arrive from a future caller that assembles a Document by
    // hand (the SAB API's URL-fetch path is the candidate). The guard
    // stays because "skip, do not fail" is the right answer and a
    // regression here would abort a whole grab over one empty element.
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const doc: nzb.Document = .{
        .arena = arena,
        .meta = &.{},
        .files = &.{
            .{
                .poster = "",
                .date_unix = null,
                .subject = "",
                .filename = "empty.rar",
                .groups = &.{},
                .segments = &.{},
            },
            .{
                .poster = "",
                .date_unix = null,
                .subject = "",
                .filename = "good.rar",
                .groups = &.{},
                .segments = &.{.{ .bytes = 100, .number = 1, .message_id = "a@h" }},
            },
        },
    };
    const built = try buildFiles(testing.allocator, &doc);
    defer built.deinit(testing.allocator);
    try testing.expectEqual(@as(usize, 1), built.files.len);
    try testing.expectEqualStrings("good.rar", built.files[0].filename);
    try testing.expectEqual(@as(i64, 100), built.total_bytes);
}

test "buildFiles marks parity and recovery volumes" {
    const src =
        \\<nzb>
        \\  <file subject='[1/1] - "rel.part01.rar" yEnc'>
        \\    <segments><segment bytes="10" number="1">a@h</segment></segments>
        \\  </file>
        \\  <file subject='[1/1] - "rel.par2" yEnc'>
        \\    <segments><segment bytes="20" number="1">b@h</segment></segments>
        \\  </file>
        \\  <file subject='[1/1] - "rel.vol000+01.par2" yEnc'>
        \\    <segments><segment bytes="30" number="1">c@h</segment></segments>
        \\  </file>
        \\</nzb>
    ;
    var doc = try nzb.parse(testing.allocator, src);
    defer doc.deinit();
    const built = try buildFiles(testing.allocator, &doc);
    defer built.deinit(testing.allocator);

    try testing.expectEqual(@as(usize, 3), built.files.len);
    try testing.expect(!built.files[0].is_par2);
    try testing.expect(built.files[1].is_par2 and !built.files[1].is_recovery_vol);
    try testing.expect(built.files[2].is_par2 and built.files[2].is_recovery_vol);
    try testing.expectEqual(@as(i64, 60), built.total_bytes);
}

// ---- naming ---------------------------------------------------------

test "chooseName prefers the meta title" {
    var doc = try nzb.parse(testing.allocator, nzb_one_file);
    defer doc.deinit();
    try testing.expectEqualStrings("Concurrent", try chooseName(testing.allocator, &doc));
}

test "chooseName falls back to a data filename, never a par2 one" {
    const src =
        \\<nzb>
        \\  <file subject='[1/1] - "Great.Movie.2020.vol00+01.par2" yEnc'>
        \\    <segments><segment bytes="10" number="1">a@h</segment></segments>
        \\  </file>
        \\  <file subject='[1/1] - "Great.Movie.2020.part01.rar" yEnc'>
        \\    <segments><segment bytes="10" number="1">b@h</segment></segments>
        \\  </file>
        \\</nzb>
    ;
    var doc = try nzb.parse(testing.allocator, src);
    defer doc.deinit();
    // The par2 volume comes first in the document, but its name would
    // leak the volume suffix into the UI.
    try testing.expectEqualStrings("Great.Movie.2020", try chooseName(testing.allocator, &doc));
}

test "chooseName uses par2 names only when there is no data file" {
    const src =
        \\<nzb>
        \\  <file subject='[1/1] - "Only.Parity.vol00+01.par2" yEnc'>
        \\    <segments><segment bytes="10" number="1">a@h</segment></segments>
        \\  </file>
        \\</nzb>
    ;
    var doc = try nzb.parse(testing.allocator, src);
    defer doc.deinit();
    try testing.expectEqualStrings("Only.Parity", try chooseName(testing.allocator, &doc));
}

test "chooseName says unnamed when there is nothing to go on" {
    // Unreachable through the parser (it rejects a file-less NZB), so
    // the Document is hand-built — the same reason `buildFiles` keeps its
    // empty-file guard.
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const doc: nzb.Document = .{ .arena = arena, .meta = &.{}, .files = &.{} };
    try testing.expectEqualStrings("unnamed", try chooseName(testing.allocator, &doc));
}

test "sha256Hex is the lower-case hex digest" {
    var buf: [64]u8 = undefined;
    try testing.expectEqualStrings(
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
        sha256Hex("", &buf),
    );
}

// ---- the use case ---------------------------------------------------

test "addJob persists the aggregate and publishes JobCreated" {
    var h: Harness = undefined;
    h.init();
    defer h.deinit();

    const r = try h.svc.addJob(.{ .nzb = nzb_one_file, .category = "tv", .source = "Sonarr/4.0" });
    try testing.expect(!r.duplicate);
    try testing.expectEqual(@as(JobId, 1), r.id);
    try testing.expectEqual(@as(usize, 1), h.store.len());
    try testing.expect(h.sink.has("download.job.created"));

    const j = h.store.get(r.id).?;
    try testing.expectEqualStrings("Concurrent", j.name);
    try testing.expectEqualStrings("tv", j.category);
    try testing.expectEqualStrings("Sonarr/4.0", j.source);
    try testing.expectEqual(@as(i64, 100), j.total_bytes);
    // queue_order comes from the injected clock, so the ordering is
    // reproducible instead of dependent on when the test ran.
    try testing.expectEqual(h.clock.t, j.queue_order);
    // The blob is kept so a job can be re-queued from history.
    try testing.expectEqualStrings(nzb_one_file, j.nzb_blob);
    try testing.expect(h.ftx.balanced());
}

test "addJob rejects an empty body and unparseable bytes" {
    var h: Harness = undefined;
    h.init();
    defer h.deinit();
    try testing.expectError(error.NzbBodyRequired, h.svc.addJob(.{ .nzb = "" }));
    try testing.expectError(error.ParseFailed, h.svc.addJob(.{ .nzb = "not xml <<<" }));
    // Well-formed, but there is nothing to download — a distinct
    // operator problem from malformed bytes, so a distinct error.
    try testing.expectError(error.NoUsableFiles, h.svc.addJob(.{ .nzb = "<nzb></nzb>" }));
    try testing.expectEqual(@as(usize, 0), h.store.len());
    // None of the rejections opened a transaction.
    try testing.expectEqual(@as(u32, 0), h.ftx.begins);
}

test "an explicit name overrides the meta title" {
    var h: Harness = undefined;
    h.init();
    defer h.deinit();
    const r = try h.svc.addJob(.{ .nzb = nzb_one_file, .name = "  Chosen.By.Hand  " });
    try testing.expectEqualStrings("Chosen.By.Hand", h.store.get(r.id).?.name);
}

test "the defer-recovery-vols toggle is read per add" {
    var h: Harness = undefined;
    h.init();
    defer h.deinit();

    const off = try h.svc.addJob(.{ .nzb = nzb_one_file });
    try testing.expect(h.store.get(off.id).?.fetchRecoveryVols());

    h.defer_vols.on = true;
    const on = try h.svc.addJob(.{ .nzb = nzb_dupe_segments });
    try testing.expect(!h.store.get(on.id).?.fetchRecoveryVols());
    try testing.expectEqual(@as(usize, 2), h.defer_vols.reads);
}

test "the hash pre-check reports a duplicate without a second row" {
    var h: Harness = undefined;
    h.init();
    defer h.deinit();

    const first = try h.svc.addJob(.{ .nzb = nzb_one_file });
    h.sink.reset();
    const second = try h.svc.addJob(.{ .nzb = nzb_one_file });

    try testing.expect(second.duplicate);
    try testing.expectEqual(first.id, second.id);
    try testing.expectEqual(@as(usize, 1), h.store.len());
    // A duplicate publishes nothing: subscribers already saw the create.
    try testing.expectEqual(@as(usize, 0), h.sink.n);
    try testing.expect(h.store.leakFree());
}

test "two interleaved adds of identical bytes converge on one job" {
    // The Zig rendition of TestAddJob_ConcurrentSameNZB. Go started
    // twelve goroutines and relied on SQLite to serialise them, which
    // makes the interesting interleaving a matter of luck — the test
    // passes even when the losing branch never runs.
    //
    // Here the interleaving is stated outright. `race_winner` makes the
    // store answer "no such hash" to caller A's pre-check and then lose
    // A's INSERT to caller B, who became visible in between. That is the
    // one ordering the recovery branch exists for, and it now runs on
    // every test invocation instead of occasionally.
    var h: Harness = undefined;
    h.init();
    defer h.deinit();

    var hash_buf: [64]u8 = undefined;
    const hash = sha256Hex(nzb_one_file, &hash_buf);
    const winner = try ports.testJob(testing.allocator, hash, "m1@h", 100);
    h.store.race_winner = winner;

    const r = try h.svc.addJob(.{ .nzb = nzb_one_file });

    try testing.expect(r.duplicate);
    try testing.expectEqual(winner.id, r.id);
    // Exactly one row: the loser's half-built aggregate was discarded,
    // not committed.
    try testing.expectEqual(@as(usize, 1), h.store.len());
    try testing.expectEqual(@as(usize, 0), h.sink.n);
    try testing.expect(h.store.leakFree());
    try testing.expect(h.ftx.balanced());
}

test "twelve adds of the same bytes yield one success and eleven duplicates" {
    var h: Harness = undefined;
    h.init();
    defer h.deinit();

    var successes: usize = 0;
    var duplicates: usize = 0;
    var ids: [12]JobId = @splat(0);
    for (&ids) |*id| {
        const r = try h.svc.addJob(.{ .nzb = nzb_one_file });
        id.* = r.id;
        if (r.duplicate) duplicates += 1 else successes += 1;
    }

    try testing.expectEqual(@as(usize, 1), successes);
    try testing.expectEqual(@as(usize, 11), duplicates);
    // Every caller observed the same id, and the DB holds one row.
    for (ids) |id| try testing.expectEqual(ids[0], id);
    try testing.expectEqual(@as(usize, 1), h.store.len());
    try testing.expect(h.store.leakFree());
}

test "a losing insert whose winner then vanishes reports the duplicate anyway" {
    // The pathological tail of the race: we lost the INSERT and the
    // winner was removed before we could look it up. Reporting id 0 with
    // duplicate set beats inventing an id the caller would then poll.
    var h: Harness = undefined;
    h.init();
    defer h.deinit();
    h.store.fail_save_duplicate = true;

    const r = try h.svc.addJob(.{ .nzb = nzb_one_file });
    try testing.expect(r.duplicate);
    try testing.expectEqual(@as(JobId, 0), r.id);
    try testing.expectEqual(@as(usize, 0), h.store.len());
}

test "a backend failure during save rolls back and publishes nothing" {
    var h: Harness = undefined;
    h.init();
    defer h.deinit();
    h.store.fail_save = error.Backend;

    try testing.expectError(error.Backend, h.svc.addJob(.{ .nzb = nzb_one_file }));
    try testing.expectEqual(@as(usize, 0), h.store.len());
    try testing.expectEqual(@as(usize, 0), h.sink.n);
    try testing.expectEqual(@as(u32, 1), h.ftx.rollbacks);
    try testing.expect(h.ftx.balanced());
}
