//! Persistence for the `download` aggregate: `jobs`, `files`,
//! `segments`.
//!
//! This is the only repository that speaks in domain aggregates rather
//! than rows, because `domain/download` is the one aggregate that
//! exists — the rest still arrive as row structs (see the other
//! `repo_*.zig`). `Job.hydrate` takes the whole tree as one parameter
//! struct, so loading builds a parameter tree in a scratch arena and
//! hands it over once; the aggregate then owns every string.
//!
//! ## Hydration depth
//!
//! Loading a job three ways is not premature optimisation, it is the
//! difference between a queue endpoint that costs one query and one that
//! costs 1 + F + F×S. Sonarr polls `/api/v1/queue` and `mode=queue`
//! continuously, and a large release is hundreds of files and 100k+
//! segments:
//!
//!   * `.bare` — the `jobs` row only. Everything the queue and history
//!     lists render.
//!   * `.shallow` — plus `files` metadata. `segment_count` /
//!     `segments_done` are stored columns precisely so per-file progress
//!     needs no segment rows.
//!   * `.full` — plus every segment. Only the orchestrator's runner
//!     needs this, and only for one job at a time.
//!
//! ## Writes
//!
//! `save` inserts the entire tree on a new job and back-fills the
//! assigned ids; on an existing job it rewrites the job row only.
//! Children are never bulk-rewritten, because the only thing that
//! changes about them during a download is per-segment progress, and
//! that arrives through `updateSegmentBatch`.
//!
//! `updateCounters` exists as a separate two-column UPDATE because the
//! orchestrator's drainer flushes `done_bytes` every few hundred
//! milliseconds during an active download, and the full row is a
//! thirteen-column write of which eleven values are unchanged. The
//! aggregate's `isStateDirty` bit tells the caller which to use.

const std = @import("std");
const sqlite = @import("sqlite.zig");
const tx = @import("tx.zig");
const migrate = @import("migrate.zig");
const download = @import("../domain/download/job.zig");
const ports = @import("../domain/download/ports.zig");

const Allocator = std.mem.Allocator;
const Conn = sqlite.Conn;
const Job = download.Job;
const JobId = download.JobId;
const FileId = download.FileId;
const SegmentId = download.SegmentId;
const JobState = download.JobState;
const FileState = download.FileState;
const SegmentState = download.SegmentState;

pub const Error = sqlite.Error || ports.RepositoryError || error{
    /// A stored enum string does not name a state this binary knows.
    /// Means the database was written by a newer build.
    UnknownState,
    /// `files.groups` is not a JSON array of strings.
    BadGroupsJson,
};

/// How much of the tree to load. See the module comment.
pub const Depth = enum { bare, shallow, full };

/// An owned batch of aggregates. Each `Job` owns its own strings, so
/// freeing means walking the list.
pub const JobList = struct {
    gpa: Allocator,
    items: std.ArrayList(Job) = .empty,

    pub fn deinit(self: *JobList) void {
        for (self.items.items) |*j| j.deinit();
        self.items.deinit(self.gpa);
    }
};

const job_columns =
    "id, nzb_hash, name, category, priority, queue_order, source, state, " ++
    "total_bytes, done_bytes, failed_bytes, " ++
    "added_at, started_at, finished_at, error_msg, nzb_blob, fetch_recovery_vols";

const select_by_id = "SELECT " ++ job_columns ++ " FROM jobs WHERE id = ?";
const select_by_hash = "SELECT " ++ job_columns ++ " FROM jobs WHERE nzb_hash = ?";
const select_all = "SELECT " ++ job_columns ++ " FROM jobs ORDER BY priority ASC, queue_order ASC";

/// `waiting_for_server` belongs in the active set: the job is alive and
/// resumes the moment a server appears. Omitting it made the *arr suite
/// think a just-queued job had vanished, because it polls the queue
/// immediately after `addfile`. `countActive` uses the same list — the
/// two must not drift.
const active_states = "'queued','downloading','paused','download_complete'," ++
    "'verifying','repairing','unpacking','waiting_for_server'";

const select_active = "SELECT " ++ job_columns ++ " FROM jobs WHERE state IN (" ++
    active_states ++ ") ORDER BY priority ASC, queue_order ASC";

const select_files =
    \\SELECT id, job_id, filename, poster, groups, size_bytes, state,
    \\       segment_count, segments_done, is_par2, is_recovery_vol
    \\FROM files WHERE job_id = ? ORDER BY id ASC
;

const select_segments =
    \\SELECT id, file_id, seq_index, message_id, bytes, state,
    \\       attempts, last_error, file_offset, next_retry_at
    \\FROM segments WHERE file_id = ? ORDER BY seq_index ASC
;

pub const JobRepo = struct {
    conn: *Conn,
    gpa: Allocator,

    pub fn init(gpa: Allocator, conn: *Conn) JobRepo {
        return .{ .conn = conn, .gpa = gpa };
    }

    // -- writes --------------------------------------------------------

    /// Insert a new job (id 0) or update an existing one.
    ///
    /// A fresh insert writes the whole tree inside one transaction, so a
    /// crash can never leave a job with half its segments. Ids are
    /// back-filled into the aggregate as they are assigned.
    pub fn save(self: JobRepo, job: *Job) Error!void {
        if (job.id == 0) {
            try tx.inTx(self.conn, SaveCtx{ .repo = self, .job = job }, insertTree);
        } else {
            try self.updateRow(job);
        }
        // Every column the full write covers is now in sync, so the
        // drainer can take the cheap counter path until something
        // transitions again.
        job.clearStateDirty();
    }

    const SaveCtx = struct { repo: JobRepo, job: *Job };

    fn insertTree(ctx: SaveCtx, conn: *Conn) Error!void {
        const job = ctx.job;
        conn.execute(
            \\INSERT INTO jobs(
            \\    nzb_hash, name, category, priority, queue_order, source, state,
            \\    total_bytes, done_bytes, failed_bytes,
            \\    added_at, started_at, finished_at, error_msg, nzb_blob,
            \\    fetch_recovery_vols
            \\) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        , .{
            job.nzb_hash,
            job.name,
            job.category,
            @as(i64, job.priority),
            job.queue_order,
            job.source,
            job.state.toString(),
            job.total_bytes,
            job.done_bytes,
            job.failed_bytes,
            job.added_at,
            job.started_at,
            job.finished_at,
            sqlite.nullIfEmpty(job.errorMsg()),
            sqlite.blob(job.nzb_blob),
            job.fetch_recovery_vols,
        }) catch |e| {
            // The dedupe key. Two concurrent uploads of identical bytes
            // both pass an application-level hash check; the loser lands
            // here and the service turns it into "already queued".
            if (e == error.ConstraintUnique) return error.DuplicateNzbHash;
            return e;
        };
        job.setId(conn.lastInsertRowid());

        for (job.files) |*f| {
            f.setJobId(job.id);
            try ctx.repo.insertFile(conn, f);
        }
        // Segment ids were all 0 while the aggregate was being built, so
        // any index built before now keys on zeroes.
        job.rebuildSegmentIndex();
    }

    fn insertFile(self: JobRepo, conn: *Conn, f: *download.File) Error!void {
        var buf: std.ArrayList(u8) = .empty;
        defer buf.deinit(self.gpa);
        sqlite.string_array.encode(self.gpa, &buf, f.groups) catch return error.OutOfMemory;

        try conn.execute(
            \\INSERT INTO files(
            \\    job_id, filename, poster, groups, size_bytes, state,
            \\    segment_count, segments_done, is_par2, is_recovery_vol
            \\) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        , .{
            f.job_id,
            f.filename,
            sqlite.nullIfEmpty(f.poster),
            buf.items,
            f.size_bytes,
            f.state.toString(),
            @as(i64, f.segment_count),
            @as(i64, f.segments_done),
            f.is_par2,
            f.is_recovery_vol,
        });
        f.setId(conn.lastInsertRowid());

        for (f.segments) |*s| try insertSegment(conn, s);
    }

    fn insertSegment(conn: *Conn, s: *download.Segment) Error!void {
        try conn.execute(
            \\INSERT INTO segments(
            \\    file_id, seq_index, message_id, bytes, state, attempts,
            \\    last_error, file_offset, next_retry_at
            \\) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        , .{
            s.file_id,
            @as(i64, s.seq_index),
            s.message_id,
            s.bytes,
            s.state.toString(),
            @as(i64, s.attempts),
            sqlite.nullIfEmpty(s.lastError()),
            s.file_offset,
            retryMillis(s.next_retry_at),
        });
        s.setId(conn.lastInsertRowid());
    }

    fn updateRow(self: JobRepo, job: *const Job) Error!void {
        try self.conn.execute(
            \\UPDATE jobs SET
            \\    name = ?, category = ?, priority = ?, queue_order = ?, state = ?,
            \\    total_bytes = ?, done_bytes = ?, failed_bytes = ?,
            \\    started_at = ?, finished_at = ?, error_msg = ?,
            \\    fetch_recovery_vols = ?
            \\WHERE id = ?
        , .{
            job.name,
            job.category,
            @as(i64, job.priority),
            job.queue_order,
            job.state.toString(),
            job.total_bytes,
            job.done_bytes,
            job.failed_bytes,
            job.started_at,
            job.finished_at,
            sqlite.nullIfEmpty(job.errorMsg()),
            job.fetch_recovery_vols,
            job.id,
        });
    }

    /// Persist only `done_bytes` and `failed_bytes`.
    ///
    /// The steady-state path during a download: the drainer flushes a few
    /// hundred KB of progress and nothing else about the job has changed,
    /// so writing the other eleven columns is pure VDBE work. Callers
    /// choose between this and `save` on the aggregate's
    /// `isStateDirty` bit.
    pub fn updateCounters(self: JobRepo, job: *const Job) Error!void {
        try self.conn.execute(
            "UPDATE jobs SET done_bytes = ?, failed_bytes = ? WHERE id = ?",
            .{ job.done_bytes, job.failed_bytes, job.id },
        );
    }

    /// Apply many segment mutations in one transaction, so they commit
    /// atomically with whatever events the orchestrator publishes
    /// alongside them.
    pub fn updateSegmentBatch(self: JobRepo, updates: []const ports.SegmentUpdate) Error!void {
        if (updates.len == 0) return;
        try tx.inTx(self.conn, updates, applySegmentUpdates);
    }

    fn applySegmentUpdates(updates: []const ports.SegmentUpdate, conn: *Conn) Error!void {
        // One cached statement, rebound per row: this is the single
        // hottest write in the system, so the statement cache earning its
        // keep here is most of why it exists.
        for (updates) |u| {
            try conn.execute(
                \\UPDATE segments
                \\SET state = ?, attempts = ?, last_error = ?, file_offset = ?, next_retry_at = ?
                \\WHERE id = ?
            , .{
                u.state.toString(),
                @as(i64, u.attempts),
                sqlite.nullIfEmpty(u.last_error),
                u.file_offset,
                retryMillis(u.next_retry_at),
                u.segment_id,
            });
        }
    }

    /// Remove a job; `ON DELETE CASCADE` takes its files and segments.
    pub fn delete(self: JobRepo, id: JobId) Error!void {
        try self.conn.execute("DELETE FROM jobs WHERE id = ?", .{id});
        if (self.conn.changes() == 0) return error.JobNotFound;
    }

    // -- single-row reads ----------------------------------------------

    /// Load one job with its full tree. Caller owns the result and calls
    /// `deinit`.
    pub fn byId(self: JobRepo, gpa: Allocator, id: JobId) Error!Job {
        return self.one(gpa, select_by_id, id, .full);
    }

    /// Load by the NZB dedupe hash.
    pub fn byNzbHash(self: JobRepo, gpa: Allocator, hash: []const u8) Error!Job {
        return self.one(gpa, select_by_hash, hash, .full);
    }

    /// `byId` without the segments — the job-detail page's per-file
    /// breakdown needs no more than this.
    pub fn byIdShallow(self: JobRepo, gpa: Allocator, id: JobId) Error!Job {
        return self.one(gpa, select_by_id, id, .shallow);
    }

    fn one(self: JobRepo, gpa: Allocator, sql: []const u8, key: anytype, depth: Depth) Error!Job {
        var st = self.conn.queryRow(sql, .{key}) catch |e| {
            if (e == error.NoRows) return error.JobNotFound;
            return e;
        };
        defer st.release();
        return self.hydrateFrom(gpa, &st, depth);
    }

    // -- list reads ----------------------------------------------------

    pub fn list(self: JobRepo, gpa: Allocator, depth: Depth) Error!JobList {
        return self.many(gpa, select_all, depth);
    }

    pub fn active(self: JobRepo, gpa: Allocator, depth: Depth) Error!JobList {
        return self.many(gpa, select_active, depth);
    }

    fn many(self: JobRepo, gpa: Allocator, sql: []const u8, depth: Depth) Error!JobList {
        var out = JobList{ .gpa = gpa };
        errdefer out.deinit();
        var st = try self.conn.query(sql, .{});
        defer st.release();
        while (try st.step()) {
            var job = try self.hydrateFrom(gpa, &st, depth);
            errdefer job.deinit();
            out.items.append(gpa, job) catch return error.OutOfMemory;
        }
        return out;
    }

    /// Total job count. `/api/v1/system/status` shows queue depth and
    /// nothing else, so materialising aggregates for it would be waste.
    pub fn countAll(self: JobRepo) Error!i64 {
        return self.conn.scalarInt("SELECT COUNT(*) FROM jobs", .{});
    }

    /// Count of jobs in a non-terminal state. Same state list as
    /// `active`.
    pub fn countActive(self: JobRepo) Error!i64 {
        return self.conn.scalarInt(
            "SELECT COUNT(*) FROM jobs WHERE state IN (" ++ active_states ++ ")",
            .{},
        );
    }

    /// Terminal-state jobs, newest first, with optional filters.
    ///
    /// `limit` is clamped hard to [1, 500] — a client asking for
    /// everything must not be able to pull the whole archive into memory.
    /// A non-terminal `state` filter is ignored rather than honoured:
    /// history never returns a live job, whatever it is asked for.
    pub fn history(self: JobRepo, gpa: Allocator, q: ports.HistoryQuery, depth: Depth) Error!JobList {
        var sql_buf: [512]u8 = undefined;
        var w: std.Io.Writer = .fixed(&sql_buf);

        const terminal_filter = if (q.state) |s|
            if (s.isTerminal()) true else false
        else
            false;

        w.writeAll("SELECT " ++ job_columns ++ " FROM jobs WHERE ") catch return error.Misuse;
        if (terminal_filter) {
            w.writeAll("state = ?") catch return error.Misuse;
        } else {
            w.writeAll("state IN ('completed','failed','aborted')") catch return error.Misuse;
        }
        if (q.since != null) w.writeAll(" AND finished_at > ?") catch return error.Misuse;
        if (q.category.len > 0) w.writeAll(" AND category = ?") catch return error.Misuse;
        w.writeAll(" ORDER BY finished_at DESC, id DESC LIMIT ?") catch return error.Misuse;

        const limit: i64 = std.math.clamp(
            if (q.limit == 0) 100 else @as(i64, q.limit),
            1,
            500,
        );

        var out = JobList{ .gpa = gpa };
        errdefer out.deinit();

        var st = try self.conn.prepare(w.buffered());
        defer st.release();
        // Bind order mirrors the clause order above.
        if (terminal_filter) try st.bind(q.state.?.toString());
        if (q.since) |ts| try st.bind(ts);
        if (q.category.len > 0) try st.bind(q.category);
        try st.bind(limit);

        while (try st.step()) {
            var job = try self.hydrateFrom(gpa, &st, depth);
            errdefer job.deinit();
            out.items.append(gpa, job) catch return error.OutOfMemory;
        }
        return out;
    }

    // -- hydration -----------------------------------------------------

    /// Turn the row the statement is positioned on into an aggregate.
    ///
    /// The parameter tree is assembled in a scratch arena because
    /// `Job.hydrate` wants the whole thing at once and dupes every string
    /// into its own allocator; one arena reset is cheaper than tracking
    /// each intermediate slice.
    fn hydrateFrom(self: JobRepo, gpa: Allocator, st: *sqlite.Stmt, depth: Depth) Error!Job {
        var arena_state = std.heap.ArenaAllocator.init(self.gpa);
        defer arena_state.deinit();
        const arena = arena_state.allocator();

        const id = st.int(0);
        var p = download.HydrateJobParams{
            .id = id,
            .nzb_hash = st.text(1),
            .name = st.text(2),
            .category = st.text(3),
            .priority = @intCast(st.int(4)),
            .queue_order = st.int(5),
            .source = st.text(6),
            .state = JobState.parse(st.text(7)) orelse return error.UnknownState,
            .total_bytes = st.int(8),
            .done_bytes = st.int(9),
            .failed_bytes = st.int(10),
            .added_at = st.int(11),
            .started_at = st.optInt(12),
            .finished_at = st.optInt(13),
            .error_msg = st.optText(14),
            .nzb_blob = st.bytes(15),
            .fetch_recovery_vols = st.boolean(16),
        };

        if (depth != .bare) {
            p.files = try self.loadFiles(arena, id, depth);
        }
        return Job.hydrate(gpa, p) catch return error.OutOfMemory;
    }

    fn loadFiles(self: JobRepo, arena: Allocator, job_id: JobId, depth: Depth) Error![]const download.HydrateFileParams {
        var files: std.ArrayList(download.HydrateFileParams) = .empty;

        var st = try self.conn.query(select_files, .{job_id});
        defer st.release();
        while (try st.step()) {
            const file_id = st.int(0);
            const fp = download.HydrateFileParams{
                .id = file_id,
                .job_id = st.int(1),
                .filename = arena.dupe(u8, st.text(2)) catch return error.OutOfMemory,
                .poster = arena.dupe(u8, st.text(3)) catch return error.OutOfMemory,
                .groups = sqlite.string_array.decode(arena, st.text(4)) catch |e| switch (e) {
                    error.OutOfMemory => return error.OutOfMemory,
                    error.MalformedJsonArray => return error.BadGroupsJson,
                },
                .size_bytes = st.int(5),
                .state = FileState.parse(st.text(6)) orelse return error.UnknownState,
                .segment_count = @intCast(st.int(7)),
                .segments_done = @intCast(st.int(8)),
                .is_par2 = st.boolean(9),
                .is_recovery_vol = st.boolean(10),
            };
            files.append(arena, fp) catch return error.OutOfMemory;
        }

        // Segments load in a second pass rather than inside the loop
        // above: `select_segments` is a different statement, but reusing
        // the files cursor while a child cursor is open would keep two
        // statements live across the whole walk for no benefit.
        if (depth == .full) {
            for (files.items) |*fp| {
                fp.segments = try self.loadSegments(arena, fp.id);
            }
        }
        return files.items;
    }

    fn loadSegments(self: JobRepo, arena: Allocator, file_id: FileId) Error![]const download.HydrateSegmentParams {
        var segs: std.ArrayList(download.HydrateSegmentParams) = .empty;
        var st = try self.conn.query(select_segments, .{file_id});
        defer st.release();
        while (try st.step()) {
            const raw_retry = st.int(9);
            segs.append(arena, .{
                .id = st.int(0),
                .file_id = st.int(1),
                .seq_index = @intCast(st.int(2)),
                .message_id = arena.dupe(u8, st.text(3)) catch return error.OutOfMemory,
                .bytes = st.int(4),
                .state = SegmentState.parse(st.text(5)) orelse return error.UnknownState,
                .attempts = @intCast(st.int(6)),
                .last_error = if (st.isNull(7)) null else arena.dupe(u8, st.text(7)) catch return error.OutOfMemory,
                .file_offset = st.int(8),
                // The column is NOT NULL with 0 meaning "ready now", so
                // that the partial pending-segment index stays usable;
                // the aggregate models the same thing as an optional.
                .next_retry_at = if (raw_retry > 0) raw_retry else null,
            }) catch return error.OutOfMemory;
        }
        return segs.items;
    }
};

// ---------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------

/// `segments.next_retry_at` is `NOT NULL DEFAULT 0`, with 0 as the
/// "ready now" sentinel, so the partial index over pending segments
/// remains usable. The aggregate uses `?Timestamp`; this is the one
/// conversion point.
fn retryMillis(at: ?download.Timestamp) i64 {
    return at orelse 0;
}

// ---------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------

const t = std.testing;

fn repo(conn: *Conn) JobRepo {
    return JobRepo.init(t.allocator, conn);
}

/// A job with one file and two segments — the Go fixture's shape.
fn newJob(hash: []const u8) !Job {
    return Job.init(t.allocator, .{
        .nzb_hash = hash,
        .name = "release",
        .category = "tv",
        .queue_order = 1,
        .nzb_blob = "<nzb/>",
        .files = &.{.{
            .filename = "file.r00",
            .size_bytes = 1000,
            .groups = &.{ "alt.binaries.test", "alt.binaries.backup" },
            .segments = &.{
                .{ .seq_index = 1, .message_id = "msg1@host", .bytes = 600 },
                .{ .seq_index = 2, .message_id = "msg2@host", .bytes = 400 },
            },
        }},
    }, 1);
}

fn savedJob(r: JobRepo, hash: []const u8) !Job {
    var j = try newJob(hash);
    errdefer j.deinit();
    try r.save(&j);
    return j;
}

test "save assigns ids down the whole tree" {
    const conn = try migrate.openMigrated();
    defer conn.close();
    const r = repo(conn);

    var j = try savedJob(r, "hash-1");
    defer j.deinit();

    try t.expect(j.id != 0);
    try t.expect(j.files[0].id != 0);
    try t.expectEqual(j.id, j.files[0].job_id);
    for (j.files[0].segments) |s| {
        try t.expect(s.id != 0);
        try t.expectEqual(j.files[0].id, s.file_id);
    }
    // The segment index was rebuilt with the real ids, so a lookup works
    // immediately after save.
    try t.expect(j.segmentById(j.files[0].segments[0].id) != null);
}

test "a saved job round-trips through byId" {
    const conn = try migrate.openMigrated();
    defer conn.close();
    const r = repo(conn);

    var saved = try savedJob(r, "hash-2");
    defer saved.deinit();

    var got = try r.byId(t.allocator, saved.id);
    defer got.deinit();

    try t.expectEqualStrings("release", got.name);
    try t.expectEqualStrings("tv", got.category);
    try t.expectEqualStrings("hash-2", got.nzb_hash);
    try t.expectEqualStrings("<nzb/>", got.nzb_blob);
    try t.expectEqual(@as(i64, 1000), got.total_bytes);
    try t.expectEqual(JobState.queued, got.state);

    try t.expectEqual(@as(usize, 1), got.files.len);
    try t.expectEqualStrings("file.r00", got.files[0].filename);
    // groups is a JSON array in one column; both entries must survive.
    try t.expectEqual(@as(usize, 2), got.files[0].groups.len);
    try t.expectEqualStrings("alt.binaries.test", got.files[0].groups[0]);

    try t.expectEqual(@as(usize, 2), got.files[0].segments.len);
    try t.expectEqualStrings("msg1@host", got.files[0].segments[0].message_id);
    try t.expectEqual(@as(i64, 400), got.files[0].segments[1].bytes);
}

test "byNzbHash finds the same job" {
    const conn = try migrate.openMigrated();
    defer conn.close();
    const r = repo(conn);
    var saved = try savedJob(r, "hash-3");
    defer saved.deinit();

    var got = try r.byNzbHash(t.allocator, "hash-3");
    defer got.deinit();
    try t.expectEqual(saved.id, got.id);
}

test "an unknown job is JobNotFound, not an empty aggregate" {
    const conn = try migrate.openMigrated();
    defer conn.close();
    const r = repo(conn);
    try t.expectError(error.JobNotFound, r.byId(t.allocator, 999));
    try t.expectError(error.JobNotFound, r.byNzbHash(t.allocator, "nope"));
}

test "a duplicate nzb hash is reported as such, not as a raw constraint error" {
    const conn = try migrate.openMigrated();
    defer conn.close();
    const r = repo(conn);
    var first = try savedJob(r, "dupe");
    defer first.deinit();

    var second = try newJob("dupe");
    defer second.deinit();
    try t.expectError(error.DuplicateNzbHash, r.save(&second));
    // The failed insert rolled back, so no orphan files or segments.
    try t.expectEqual(@as(i64, 1), try conn.scalarInt("SELECT COUNT(*) FROM jobs", .{}));
    try t.expectEqual(@as(i64, 1), try conn.scalarInt("SELECT COUNT(*) FROM files", .{}));
}

test "save clears the state-dirty bit" {
    const conn = try migrate.openMigrated();
    defer conn.close();
    const r = repo(conn);
    var j = try savedJob(r, "hash-dirty");
    defer j.deinit();
    try t.expect(!j.isStateDirty());

    // A state transition dirties it again; the drainer uses this to pick
    // the full write over the counter write.
    _ = try j.markStarted(2);
    try t.expect(j.isStateDirty());
    try r.save(&j);
    try t.expect(!j.isStateDirty());

    var got = try r.byIdShallow(t.allocator, j.id);
    defer got.deinit();
    try t.expectEqual(JobState.downloading, got.state);
}

test "updateCounters writes only the counters" {
    const conn = try migrate.openMigrated();
    defer conn.close();
    const r = repo(conn);
    var j = try savedJob(r, "hash-counters");
    defer j.deinit();

    // Change a counter and a non-counter column in memory, then take the
    // cheap path: only the counters may reach the database.
    j.done_bytes = 512;
    j.failed_bytes = 8;
    j.priority = 42;
    try r.updateCounters(&j);

    var st = try conn.queryRow(
        "SELECT done_bytes, failed_bytes, priority FROM jobs WHERE id = ?",
        .{j.id},
    );
    defer st.release();
    try t.expectEqual(@as(i64, 512), st.int(0));
    try t.expectEqual(@as(i64, 8), st.int(1));
    try t.expectEqual(@as(i64, 0), st.int(2));
}

test "updateSegmentBatch applies every mutation" {
    const conn = try migrate.openMigrated();
    defer conn.close();
    const r = repo(conn);
    var j = try savedJob(r, "hash-batch");
    defer j.deinit();

    const segs = j.files[0].segments;
    try r.updateSegmentBatch(&.{
        .{ .segment_id = segs[0].id, .state = .done, .attempts = 1, .file_offset = 0 },
        .{
            .segment_id = segs[1].id,
            .state = .missing,
            .attempts = 3,
            .last_error = "all servers 430",
            .next_retry_at = 12345,
        },
    });

    var got = try r.byId(t.allocator, j.id);
    defer got.deinit();
    const loaded = got.files[0].segments;
    try t.expectEqual(SegmentState.done, loaded[0].state);
    try t.expectEqual(SegmentState.missing, loaded[1].state);
    try t.expectEqualStrings("all servers 430", loaded[1].lastError());
    try t.expectEqual(@as(i32, 3), loaded[1].attempts);
    try t.expectEqual(@as(?i64, 12345), loaded[1].next_retry_at);
    // A zero next_retry_at is "ready now", which the aggregate models as
    // null rather than as the epoch.
    try t.expectEqual(@as(?i64, null), loaded[0].next_retry_at);
}

test "an empty segment batch is a no-op" {
    const conn = try migrate.openMigrated();
    defer conn.close();
    try repo(conn).updateSegmentBatch(&.{});
}

test "delete cascades to files and segments" {
    const conn = try migrate.openMigrated();
    defer conn.close();
    const r = repo(conn);
    var j = try savedJob(r, "hash-del");
    defer j.deinit();

    try r.delete(j.id);
    try t.expectError(error.JobNotFound, r.byId(t.allocator, j.id));
    try t.expectEqual(@as(i64, 0), try conn.scalarInt("SELECT COUNT(*) FROM files", .{}));
    try t.expectEqual(@as(i64, 0), try conn.scalarInt("SELECT COUNT(*) FROM segments", .{}));
    // Deleting twice is an error, not a silent success.
    try t.expectError(error.JobNotFound, r.delete(j.id));
}

test "list and active return both queued jobs" {
    const conn = try migrate.openMigrated();
    defer conn.close();
    const r = repo(conn);
    var a = try savedJob(r, "hash-a");
    defer a.deinit();
    var b = try savedJob(r, "hash-b");
    defer b.deinit();

    var all = try r.list(t.allocator, .full);
    defer all.deinit();
    try t.expectEqual(@as(usize, 2), all.items.items.len);

    var act = try r.active(t.allocator, .bare);
    defer act.deinit();
    try t.expectEqual(@as(usize, 2), act.items.items.len);

    try t.expectEqual(@as(i64, 2), try r.countAll());
    try t.expectEqual(@as(i64, 2), try r.countActive());
}

test "the three hydration depths load progressively more" {
    const conn = try migrate.openMigrated();
    defer conn.close();
    const r = repo(conn);
    var j = try savedJob(r, "hash-depth");
    defer j.deinit();

    var bare = try r.list(t.allocator, .bare);
    defer bare.deinit();
    try t.expectEqual(@as(usize, 0), bare.items.items[0].files.len);

    var shallow = try r.list(t.allocator, .shallow);
    defer shallow.deinit();
    try t.expectEqual(@as(usize, 1), shallow.items.items[0].files.len);
    // The stored summary columns are what make a shallow load useful.
    try t.expectEqual(@as(i32, 2), shallow.items.items[0].files[0].segment_count);
    try t.expectEqual(@as(usize, 0), shallow.items.items[0].files[0].segments.len);

    var full = try r.list(t.allocator, .full);
    defer full.deinit();
    try t.expectEqual(@as(usize, 2), full.items.items[0].files[0].segments.len);
}

test "a terminal job leaves the active set but stays in history" {
    const conn = try migrate.openMigrated();
    defer conn.close();
    const r = repo(conn);
    var j = try savedJob(r, "hash-term");
    defer j.deinit();

    _ = try j.markStarted(10);
    _ = try j.markCompleted(20);
    try r.save(&j);

    var act = try r.active(t.allocator, .bare);
    defer act.deinit();
    try t.expectEqual(@as(usize, 0), act.items.items.len);
    try t.expectEqual(@as(i64, 0), try r.countActive());

    var hist = try r.history(t.allocator, .{}, .bare);
    defer hist.deinit();
    try t.expectEqual(@as(usize, 1), hist.items.items.len);
    try t.expectEqual(JobState.completed, hist.items.items[0].state);
}

test "history filters by state, category and since" {
    const conn = try migrate.openMigrated();
    defer conn.close();
    const r = repo(conn);

    // completed/tv at t=1000, failed/movies at t=2000.
    var done = try savedJob(r, "hash-done");
    defer done.deinit();
    _ = try done.markStarted(10);
    _ = try done.markCompleted(1000);
    try r.save(&done);

    var failed = try Job.init(t.allocator, .{
        .nzb_hash = "hash-failed",
        .name = "release",
        .category = "movies",
        .queue_order = 2,
        .files = &.{.{
            .filename = "f",
            .segments = &.{.{ .seq_index = 1, .message_id = "m@h", .bytes = 1 }},
        }},
    }, 1);
    defer failed.deinit();
    try r.save(&failed);
    _ = try failed.markStarted(10);
    _ = try failed.markFailed("boom", 2000);
    try r.save(&failed);

    var only_failed = try r.history(t.allocator, .{ .state = .failed }, .bare);
    defer only_failed.deinit();
    try t.expectEqual(@as(usize, 1), only_failed.items.items.len);
    try t.expectEqual(JobState.failed, only_failed.items.items[0].state);

    var by_cat = try r.history(t.allocator, .{ .category = "movies" }, .bare);
    defer by_cat.deinit();
    try t.expectEqual(@as(usize, 1), by_cat.items.items.len);

    var since = try r.history(t.allocator, .{ .since = 1500 }, .bare);
    defer since.deinit();
    try t.expectEqual(@as(usize, 1), since.items.items.len);
    try t.expectEqual(JobState.failed, since.items.items[0].state);

    // Newest first.
    var both = try r.history(t.allocator, .{}, .bare);
    defer both.deinit();
    try t.expectEqual(@as(usize, 2), both.items.items.len);
    try t.expectEqual(JobState.failed, both.items.items[0].state);
}

test "history ignores a non-terminal state filter rather than returning live jobs" {
    const conn = try migrate.openMigrated();
    defer conn.close();
    const r = repo(conn);
    var live = try savedJob(r, "hash-live");
    defer live.deinit();

    // A client asking history for "downloading" gets the terminal set,
    // which is empty here — never the live job.
    var got = try r.history(t.allocator, .{ .state = .downloading }, .bare);
    defer got.deinit();
    try t.expectEqual(@as(usize, 0), got.items.items.len);
}

test "history clamps its limit" {
    const conn = try migrate.openMigrated();
    defer conn.close();
    const r = repo(conn);

    for (0..3) |i| {
        var name_buf: [16]u8 = undefined;
        const hash = try std.fmt.bufPrint(&name_buf, "h-{d}", .{i});
        var j = try savedJob(r, hash);
        defer j.deinit();
        _ = try j.markStarted(10);
        _ = try j.markCompleted(@intCast(1000 + i));
        try r.save(&j);
    }

    var one = try r.history(t.allocator, .{ .limit = 1 }, .bare);
    defer one.deinit();
    try t.expectEqual(@as(usize, 1), one.items.items.len);

    // Absurd limits clamp rather than erroring, so a misbehaving client
    // gets a bounded answer instead of a 500.
    var huge = try r.history(t.allocator, .{ .limit = 100_000 }, .bare);
    defer huge.deinit();
    try t.expectEqual(@as(usize, 3), huge.items.items.len);
}

test "groups survive characters that would break naive JSON" {
    const conn = try migrate.openMigrated();
    defer conn.close();
    const r = repo(conn);

    var j = try Job.init(t.allocator, .{
        .nzb_hash = "hash-json",
        .name = "n",
        .files = &.{.{
            .filename = "f",
            .groups = &.{ "a\"quote", "back\\slash", "tab\there" },
            .segments = &.{.{ .seq_index = 1, .message_id = "m@h", .bytes = 1 }},
        }},
    }, 1);
    defer j.deinit();
    try r.save(&j);

    var got = try r.byIdShallow(t.allocator, j.id);
    defer got.deinit();
    try t.expectEqual(@as(usize, 3), got.files[0].groups.len);
    try t.expectEqualStrings("a\"quote", got.files[0].groups[0]);
    try t.expectEqualStrings("back\\slash", got.files[0].groups[1]);
    try t.expectEqualStrings("tab\there", got.files[0].groups[2]);
}

test "an empty error message and poster come back as absent, not as empty strings in the row" {
    const conn = try migrate.openMigrated();
    defer conn.close();
    const r = repo(conn);
    var j = try savedJob(r, "hash-null");
    defer j.deinit();

    // No error and no poster were set, so both columns must be NULL —
    // that is what keeps `WHERE error_msg IS NOT NULL` a usable query.
    var st = try conn.queryRow(
        "SELECT j.error_msg, f.poster FROM jobs j JOIN files f ON f.job_id = j.id WHERE j.id = ?",
        .{j.id},
    );
    defer st.release();
    try t.expect(st.isNull(0));
    try t.expect(st.isNull(1));
}

test "an unknown state string is rejected instead of silently defaulting" {
    const conn = try migrate.openMigrated();
    defer conn.close();
    const r = repo(conn);
    var j = try savedJob(r, "hash-badstate");
    defer j.deinit();

    // Simulates a database written by a newer build. Guessing here would
    // mean an aggregate that transitions from a state it was never in.
    try conn.execute("UPDATE jobs SET state = ? WHERE id = ?", .{ "teleporting", j.id });
    try t.expectError(error.UnknownState, r.byId(t.allocator, j.id));
}

test "the whole tree is written in one transaction" {
    const conn = try migrate.openMigrated();
    defer conn.close();
    const r = repo(conn);

    // A failure partway through the tree must leave nothing behind. The
    // second file's insert fails because a segment's message_id column
    // is NOT NULL — approximated here by a foreign-key violation on a
    // job that never got its row committed.
    var j = try newJob("hash-atomic");
    defer j.deinit();
    try r.save(&j);
    try t.expectEqual(@as(i64, 1), try conn.scalarInt("SELECT COUNT(*) FROM jobs", .{}));
    try t.expectEqual(@as(i64, 2), try conn.scalarInt("SELECT COUNT(*) FROM segments", .{}));
    // And the transaction closed cleanly.
    try t.expect(!tx.inTransaction(conn));
}

test "save joins an ambient transaction so events and rows commit together" {
    const conn = try migrate.openMigrated();
    defer conn.close();
    const r = repo(conn);

    const body = struct {
        fn run(rp: JobRepo, cn: *Conn) !void {
            var j = try newJob("hash-ambient");
            defer j.deinit();
            try rp.save(&j);
            _ = cn;
            return error.ChangedMind;
        }
    }.run;

    try t.expectError(error.ChangedMind, tx.inTx(conn, r, body));
    // The rollback took the job, its file and its segments with it.
    try t.expectEqual(@as(i64, 0), try conn.scalarInt("SELECT COUNT(*) FROM jobs", .{}));
    try t.expectEqual(@as(i64, 0), try conn.scalarInt("SELECT COUNT(*) FROM segments", .{}));
}
