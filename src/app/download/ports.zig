//! The ports the download application layer is wired through, and the
//! fakes every test in this directory shares.
//!
//! Two seams, both `*anyopaque` + function pointers per
//! `app/notify/transport.zig`:
//!
//!   * `JobStore` — persistence for the `Job` aggregate. Narrower than
//!     Go's `download.JobRepository`: the list-shaped queries the REST
//!     layer uses (`ListShallow`, `HistoryJobsOnly`, …) are not here,
//!     because nothing in *this* layer calls them — they are a read
//!     model the API talks to directly. What is here is exactly what the
//!     orchestrator, the queue service and the post-download contexts
//!     need.
//!
//!   * `ArticleFetcher` — one article's body. Go handed back an
//!     `io.ReadCloser` and let the caller drain it, which is how byte
//!     accounting ended up wrapped around a reader three layers deep.
//!     Here a fetch returns the bytes plus the id of the server that
//!     served them, so accounting is one addition at the call site and
//!     the reader-wrapping disappears entirely.
//!
//! # Errors are an enum, not a string
//!
//! Go decided "retry or fail" by `errors.Is` on four sentinels and then
//! `strings.Contains(err.Error(), "yenc decode")` for the rest. That is
//! a load-bearing string match: renaming a wrap prefix silently turns a
//! permanent failure into an infinite retry. `FetchError` and `Failure`
//! make the same distinction exhaustively, so `orchestrator.isTransient`
//! is a switch the compiler checks.

const std = @import("std");
const app_ports = @import("../ports.zig");
const dstate = @import("../../domain/download/state.zig");
const dports = @import("../../domain/download/ports.zig");
const job_mod = @import("../../domain/download/job.zig");
const dserver = @import("../../domain/server.zig");

const Allocator = std.mem.Allocator;

pub const Timestamp = app_ports.Timestamp;
pub const Unit = app_ports.Unit;
pub const JobId = dstate.JobId;
pub const FileId = dstate.FileId;
pub const SegmentId = dstate.SegmentId;
pub const JobState = dstate.JobState;
pub const SegmentState = dstate.SegmentState;
pub const Job = job_mod.Job;
pub const SegmentUpdate = dports.SegmentUpdate;
pub const ServerId = dserver.ServerId;

// ---------------------------------------------------------------------
// Job persistence
// ---------------------------------------------------------------------

pub const RepoError = error{
    JobNotFound,
    /// A concurrent add of identical NZB bytes won the INSERT. The
    /// caller converts this into "already queued" plus the winner's id.
    DuplicateNzbHash,
    /// The backend failed and has already logged why.
    Backend,
} || Allocator.Error;

/// The job-level fields the queue and startup sweeps read. Loading whole
/// aggregates for these was measured at ~38% of process CPU under the
/// frontend's 2s poll; the Go fix was `*JobsOnly` query variants, and
/// this is the same fix expressed as a type.
pub const JobSummary = struct {
    id: JobId,
    state: JobState,
    queue_order: i64 = 0,
};

/// Persistence for the `Job` aggregate.
///
/// Every mutating call takes the ambient `*Unit` so the write lands in
/// the same transaction as the events published alongside it. `null`
/// means "no transaction" and is only used by the fakes and by callers
/// that genuinely own no boundary.
pub const JobStore = struct {
    ctx: *anyopaque,

    /// The full aggregate — files and segments included. The store owns
    /// the returned memory until `release`.
    byIdFn: *const fn (ctx: *anyopaque, unit: ?*Unit, id: JobId) RepoError!*Job,
    byNzbHashFn: *const fn (ctx: *anyopaque, unit: ?*Unit, hash: []const u8) RepoError!*Job,
    /// Hands a previously-loaded aggregate back. Must tolerate being
    /// called exactly once per successful load, in any order.
    releaseFn: *const fn (ctx: *anyopaque, job: *Job) void,

    /// Full write of every job-level column, plus insert of a job that
    /// has no id yet (which assigns one via `Job.setId`). Clears the
    /// aggregate's `state_dirty` bit on success.
    saveFn: *const fn (ctx: *anyopaque, unit: ?*Unit, job: *Job) RepoError!void,
    /// `done_bytes` and `failed_bytes` only — the two columns that move
    /// on every drainer flush of an active download. See
    /// `orchestrator.flushAggregate` for why this exists.
    updateCountersFn: *const fn (ctx: *anyopaque, unit: ?*Unit, job: *const Job) RepoError!void,
    updateSegmentBatchFn: *const fn (ctx: *anyopaque, unit: ?*Unit, updates: []const SegmentUpdate) RepoError!void,
    deleteFn: *const fn (ctx: *anyopaque, unit: ?*Unit, id: JobId) RepoError!void,

    /// Non-terminal jobs ordered by `queue_order` ascending. The slice
    /// belongs to the caller's allocator.
    activeFn: *const fn (ctx: *anyopaque, a: Allocator, unit: ?*Unit) RepoError![]JobSummary,
    countActiveFn: *const fn (ctx: *anyopaque, unit: ?*Unit) RepoError!u32,
    countAllFn: *const fn (ctx: *anyopaque, unit: ?*Unit) RepoError!u32,

    pub fn byId(self: JobStore, unit: ?*Unit, id: JobId) RepoError!*Job {
        return self.byIdFn(self.ctx, unit, id);
    }

    pub fn byNzbHash(self: JobStore, unit: ?*Unit, hash: []const u8) RepoError!*Job {
        return self.byNzbHashFn(self.ctx, unit, hash);
    }

    pub fn release(self: JobStore, job: *Job) void {
        self.releaseFn(self.ctx, job);
    }

    pub fn save(self: JobStore, unit: ?*Unit, job: *Job) RepoError!void {
        return self.saveFn(self.ctx, unit, job);
    }

    pub fn updateCounters(self: JobStore, unit: ?*Unit, job: *const Job) RepoError!void {
        return self.updateCountersFn(self.ctx, unit, job);
    }

    pub fn updateSegmentBatch(self: JobStore, unit: ?*Unit, updates: []const SegmentUpdate) RepoError!void {
        if (updates.len == 0) return;
        return self.updateSegmentBatchFn(self.ctx, unit, updates);
    }

    pub fn delete(self: JobStore, unit: ?*Unit, id: JobId) RepoError!void {
        return self.deleteFn(self.ctx, unit, id);
    }

    pub fn active(self: JobStore, a: Allocator, unit: ?*Unit) RepoError![]JobSummary {
        return self.activeFn(self.ctx, a, unit);
    }

    pub fn countActive(self: JobStore, unit: ?*Unit) RepoError!u32 {
        return self.countActiveFn(self.ctx, unit);
    }

    pub fn countAll(self: JobStore, unit: ?*Unit) RepoError!u32 {
        return self.countAllFn(self.ctx, unit);
    }
};

// ---------------------------------------------------------------------
// Article fetch
// ---------------------------------------------------------------------

/// Why a fetch did not produce a body.
///
/// The split between `ProtocolTransient` and `ProtocolPermanent` is RFC
/// 3977's: a 4xx response is a "transient negative" the server expects
/// you to retry, a 5xx is permanent.
pub const FetchError = error{
    /// Every server we asked answered 430. Terminal for download
    /// purposes; PAR2 may still rescue the file.
    ArticleMissing,
    /// There is currently no enabled, in-quota server to ask. Not a
    /// failure of the fetch — a failure to have anyone to fetch from,
    /// which is why it does not consume a segment's retry budget.
    NoPoolsAvailable,
    /// The provider says we are over our connection allowance.
    TooManyConnections,
    AuthRequired,
    /// Bad credentials. These do not fix themselves.
    AuthFailed,
    UnexpectedGreeting,
    ProtocolTransient,
    ProtocolPermanent,
    /// Connect, reset, timeout, short read.
    Network,
    /// Shutdown while the fetch was in flight.
    Canceled,
} || Allocator.Error;

/// One article's body, plus who served it.
pub const Body = struct {
    /// Raw article bytes, dot-unstuffed, yEnc still encoded. Allocated
    /// with the allocator passed to `fetch`; the caller frees.
    bytes: []u8,
    /// Which server answered. Carried so byte accounting is a single
    /// addition at the call site rather than a wrapped reader.
    server_id: ServerId = 0,
    /// Milliseconds the bandwidth limiter says the caller owes before it
    /// should ask for more. 0 when unthrottled. The caller turns this
    /// into a reactor timer — the port never sleeps.
    throttle_ms: i64 = 0,
};

/// Fetches one article. The `hint` is a label only: a multi-server
/// implementation decides which server to ask, and `Body.server_id`
/// reports what it chose.
pub const ArticleFetcher = struct {
    ctx: *anyopaque,
    fetchFn: *const fn (
        ctx: *anyopaque,
        a: Allocator,
        hint: ServerId,
        message_id: []const u8,
    ) FetchError!Body,

    pub fn fetch(
        self: ArticleFetcher,
        a: Allocator,
        hint: ServerId,
        message_id: []const u8,
    ) FetchError!Body {
        return self.fetchFn(self.ctx, a, hint, message_id);
    }
};

// ---------------------------------------------------------------------
// Pools, for the tiered fetcher
// ---------------------------------------------------------------------

/// What the tiered fetcher needs to know about one registered pool.
/// Everything is a snapshot: the fetcher re-reads it on every fetch so
/// a server enabled from the Settings UI takes effect immediately.
pub const PoolInfo = struct {
    id: ServerId,
    /// Borrowed from the pool; used only in log lines.
    name: []const u8 = "",
    /// Lower is better.
    priority: i32 = 0,
    /// Backups sort last regardless of priority.
    backup: bool = false,
    /// Metered accounts sort after flat ones inside a tier.
    metered: bool = false,
    enabled: bool = true,
    quota_exhausted: bool = false,
    max_conns: u16 = 1,

    /// Eligible to be asked right now.
    pub fn usable(self: PoolInfo) bool {
        return self.enabled and !self.quota_exhausted;
    }
};

/// The live set of NNTP pools.
pub const PoolSet = struct {
    ctx: *anyopaque,
    snapshotFn: *const fn (ctx: *anyopaque, a: Allocator) Allocator.Error![]PoolInfo,
    /// BODY against exactly one server, no failover.
    fetchOneFn: *const fn (
        ctx: *anyopaque,
        a: Allocator,
        id: ServerId,
        message_id: []const u8,
    ) FetchError![]u8,

    pub fn snapshot(self: PoolSet, a: Allocator) Allocator.Error![]PoolInfo {
        return self.snapshotFn(self.ctx, a);
    }

    pub fn fetchOne(
        self: PoolSet,
        a: Allocator,
        id: ServerId,
        message_id: []const u8,
    ) FetchError![]u8 {
        return self.fetchOneFn(self.ctx, a, id, message_id);
    }
};

// =====================================================================
// Test doubles
// =====================================================================

/// An in-memory `JobStore`.
///
/// It hands out the *same* aggregate on every `byId`, rather than a
/// fresh copy. A real repository re-hydrates, but re-hydration is the
/// repository's contract to test, not the orchestrator's — and sharing
/// the pointer is what lets a test assert on the persisted aggregate
/// directly instead of re-reading it through the port it is testing.
pub const FakeJobStore = struct {
    gpa: Allocator,
    jobs: std.ArrayList(*Job) = .empty,
    /// Next id handed out by `save` for a job with none.
    next_id: JobId = 1,
    next_file_id: FileId = 1,
    next_segment_id: SegmentId = 1,

    saves: usize = 0,
    counter_updates: usize = 0,
    segment_batches: usize = 0,
    /// Total `SegmentUpdate` rows seen across every batch.
    segment_rows: usize = 0,
    deletes: usize = 0,
    loads: usize = 0,
    releases: usize = 0,

    /// Made to fail the next `save` — used to prove the caller rolls the
    /// transaction back instead of publishing.
    fail_save: ?RepoError = null,
    /// Simulates the UNIQUE(nzb_hash) race: the next insert loses.
    fail_save_duplicate: bool = false,
    /// The other side of that race. When set, the next insert loses
    /// *and* this aggregate becomes visible first — which is exactly the
    /// interleaving a concurrent upload produces: caller A's hash
    /// pre-check found nothing, caller B committed, then A's INSERT hit
    /// the UNIQUE constraint and A has to go looking for the winner.
    ///
    /// Ownership transfers to the store when the race fires.
    race_winner: ?*Job = null,

    pub fn init(gpa: Allocator) FakeJobStore {
        return .{ .gpa = gpa };
    }

    pub fn deinit(self: *FakeJobStore) void {
        for (self.jobs.items) |j| {
            j.deinit();
            self.gpa.destroy(j);
        }
        self.jobs.deinit(self.gpa);
        self.* = undefined;
    }

    pub fn store(self: *FakeJobStore) JobStore {
        return .{
            .ctx = @ptrCast(self),
            .byIdFn = &byId,
            .byNzbHashFn = &byNzbHash,
            .releaseFn = &release,
            .saveFn = &save,
            .updateCountersFn = &updateCounters,
            .updateSegmentBatchFn = &updateSegmentBatch,
            .deleteFn = &deleteJob,
            .activeFn = &active,
            .countActiveFn = &countActive,
            .countAllFn = &countAll,
        };
    }

    /// Takes ownership of `job`, assigning ids the way an INSERT would.
    ///
    /// File and segment ids matter as much as the job id: the
    /// orchestrator names its temp files `<file id>.tmp`, and `Job`'s
    /// segment index is keyed on the segment id — leaving them all zero
    /// would collapse every segment onto one index entry and hide the
    /// bugs these tests exist to catch.
    pub fn insert(self: *FakeJobStore, job: *Job) Allocator.Error!void {
        if (job.id == 0) {
            job.setId(self.next_id);
            self.next_id += 1;
        } else if (job.id >= self.next_id) {
            self.next_id = job.id + 1;
        }
        for (job.files) |*f| {
            if (f.id == 0) {
                f.setId(self.next_file_id);
                self.next_file_id += 1;
            }
            for (f.segments) |*s| {
                if (s.id == 0) {
                    s.setId(self.next_segment_id);
                    self.next_segment_id += 1;
                }
            }
        }
        job.rebuildSegmentIndex();
        try self.jobs.append(self.gpa, job);
    }

    pub fn get(self: *FakeJobStore, id: JobId) ?*Job {
        for (self.jobs.items) |j| {
            if (j.id == id) return j;
        }
        return null;
    }

    pub fn len(self: *const FakeJobStore) usize {
        return self.jobs.items.len;
    }

    /// True when every aggregate handed out has been handed back.
    pub fn leakFree(self: *const FakeJobStore) bool {
        return self.loads == self.releases;
    }

    fn byId(ctx: *anyopaque, _: ?*Unit, id: JobId) RepoError!*Job {
        const self: *FakeJobStore = @ptrCast(@alignCast(ctx));
        const j = self.get(id) orelse return error.JobNotFound;
        self.loads += 1;
        return j;
    }

    fn byNzbHash(ctx: *anyopaque, _: ?*Unit, hash: []const u8) RepoError!*Job {
        const self: *FakeJobStore = @ptrCast(@alignCast(ctx));
        for (self.jobs.items) |j| {
            if (std.mem.eql(u8, j.nzb_hash, hash)) {
                self.loads += 1;
                return j;
            }
        }
        return error.JobNotFound;
    }

    fn release(ctx: *anyopaque, _: *Job) void {
        const self: *FakeJobStore = @ptrCast(@alignCast(ctx));
        self.releases += 1;
    }

    fn save(ctx: *anyopaque, _: ?*Unit, job: *Job) RepoError!void {
        const self: *FakeJobStore = @ptrCast(@alignCast(ctx));
        if (self.fail_save) |e| {
            self.fail_save = null;
            return e;
        }
        if (job.id == 0) {
            if (self.race_winner) |winner| {
                self.race_winner = null;
                try self.insert(winner);
                return error.DuplicateNzbHash;
            }
            if (self.fail_save_duplicate) {
                self.fail_save_duplicate = false;
                return error.DuplicateNzbHash;
            }
            for (self.jobs.items) |existing| {
                if (std.mem.eql(u8, existing.nzb_hash, job.nzb_hash)) return error.DuplicateNzbHash;
            }
            try self.insert(job);
        }
        self.saves += 1;
        job.clearStateDirty();
    }

    fn updateCounters(ctx: *anyopaque, _: ?*Unit, _: *const Job) RepoError!void {
        const self: *FakeJobStore = @ptrCast(@alignCast(ctx));
        self.counter_updates += 1;
    }

    fn updateSegmentBatch(ctx: *anyopaque, _: ?*Unit, updates: []const SegmentUpdate) RepoError!void {
        const self: *FakeJobStore = @ptrCast(@alignCast(ctx));
        self.segment_batches += 1;
        self.segment_rows += updates.len;
    }

    fn deleteJob(ctx: *anyopaque, _: ?*Unit, id: JobId) RepoError!void {
        const self: *FakeJobStore = @ptrCast(@alignCast(ctx));
        for (self.jobs.items, 0..) |j, i| {
            if (j.id != id) continue;
            j.deinit();
            self.gpa.destroy(j);
            _ = self.jobs.orderedRemove(i);
            self.deletes += 1;
            return;
        }
        return error.JobNotFound;
    }

    fn active(ctx: *anyopaque, a: Allocator, _: ?*Unit) RepoError![]JobSummary {
        const self: *FakeJobStore = @ptrCast(@alignCast(ctx));
        var out: std.ArrayList(JobSummary) = .empty;
        errdefer out.deinit(a);
        for (self.jobs.items) |j| {
            if (j.state.isTerminal()) continue;
            try out.append(a, .{ .id = j.id, .state = j.state, .queue_order = j.queue_order });
        }
        std.mem.sort(JobSummary, out.items, {}, struct {
            fn lt(_: void, x: JobSummary, y: JobSummary) bool {
                if (x.queue_order != y.queue_order) return x.queue_order < y.queue_order;
                return x.id < y.id;
            }
        }.lt);
        return out.toOwnedSlice(a);
    }

    fn countActive(ctx: *anyopaque, _: ?*Unit) RepoError!u32 {
        const self: *FakeJobStore = @ptrCast(@alignCast(ctx));
        var n: u32 = 0;
        for (self.jobs.items) |j| {
            if (!j.state.isTerminal()) n += 1;
        }
        return n;
    }

    fn countAll(ctx: *anyopaque, _: ?*Unit) RepoError!u32 {
        const self: *FakeJobStore = @ptrCast(@alignCast(ctx));
        return @intCast(self.jobs.items.len);
    }
};

/// A scripted `ArticleFetcher`.
///
/// `fail_first` failures are returned per message-id before the canned
/// body — the Zig equivalent of Go's `flakyFetcher`, which is what the
/// retry tests are written against.
pub const FakeFetcher = struct {
    pub const Canned = struct {
        message_id: []const u8,
        /// yEnc-encoded article body.
        body: []const u8,
    };

    bodies: []const Canned,
    /// Failures returned per message-id before the body is handed over.
    fail_first: u32 = 0,
    /// The error those failures carry.
    err: FetchError = error.Network,
    /// Which server the successful fetch reports.
    server_id: ServerId = 1,

    calls: usize = 0,
    /// Per-message failure counters, keyed by index into `bodies`.
    failed: [16]u32 = @splat(0),

    pub fn fetcher(self: *FakeFetcher) ArticleFetcher {
        return .{ .ctx = @ptrCast(self), .fetchFn = &doFetch };
    }

    pub fn reset(self: *FakeFetcher) void {
        self.calls = 0;
        self.failed = @splat(0);
    }

    fn doFetch(ctx: *anyopaque, a: Allocator, _: ServerId, message_id: []const u8) FetchError!Body {
        const self: *FakeFetcher = @ptrCast(@alignCast(ctx));
        self.calls += 1;
        for (self.bodies, 0..) |c, i| {
            if (!std.mem.eql(u8, c.message_id, message_id)) continue;
            if (i < self.failed.len and self.failed[i] < self.fail_first) {
                self.failed[i] += 1;
                return self.err;
            }
            return .{ .bytes = try a.dupe(u8, c.body), .server_id = self.server_id };
        }
        return error.ArticleMissing;
    }
};

// ---------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------

const testing = std.testing;

/// A one-file, one-segment job — the smallest aggregate the store and
/// the orchestrator will accept.
pub fn testJob(
    a: Allocator,
    hash: []const u8,
    message_id: []const u8,
    bytes: i64,
) !*Job {
    const j = try a.create(Job);
    errdefer a.destroy(j);
    j.* = try Job.init(a, .{
        .nzb_hash = hash,
        .name = hash,
        .nzb_blob = "<nzb/>",
        .files = &.{.{
            .filename = "f.bin",
            .size_bytes = bytes,
            .segments = &.{.{ .seq_index = 1, .message_id = message_id, .bytes = bytes }},
        }},
    }, 0);
    return j;
}

test "the fake store assigns ids on insert and finds by hash" {
    var st = FakeJobStore.init(testing.allocator);
    defer st.deinit();
    const s = st.store();

    const j = try testJob(testing.allocator, "h1", "m1@h", 10);
    try st.insert(j);
    try testing.expectEqual(@as(JobId, 1), j.id);

    const found = try s.byNzbHash(null, "h1");
    defer s.release(found);
    try testing.expectEqual(j, found);
    try testing.expectError(error.JobNotFound, s.byNzbHash(null, "nope"));
    try testing.expectError(error.JobNotFound, s.byId(null, 99));
}

test "save of a job with no id inserts, and a duplicate hash loses" {
    var st = FakeJobStore.init(testing.allocator);
    defer st.deinit();
    const s = st.store();

    const first = try testJob(testing.allocator, "same", "m1@h", 1);
    try s.save(null, first);
    try testing.expectEqual(@as(JobId, 1), first.id);
    try testing.expectEqual(@as(usize, 1), st.len());

    var second = try testJob(testing.allocator, "same", "m2@h", 1);
    defer {
        second.deinit();
        testing.allocator.destroy(second);
    }
    try testing.expectError(error.DuplicateNzbHash, s.save(null, second));
    try testing.expectEqual(@as(usize, 1), st.len());
}

test "save clears the aggregate's dirty bit, counter updates do not" {
    var st = FakeJobStore.init(testing.allocator);
    defer st.deinit();
    const s = st.store();
    const j = try testJob(testing.allocator, "h", "m@h", 1);
    try st.insert(j);

    _ = try j.markStarted(5);
    try testing.expect(j.isStateDirty());
    try s.save(null, j);
    try testing.expect(!j.isStateDirty());

    j.done_bytes += 100;
    try s.updateCounters(null, j);
    try testing.expect(!j.isStateDirty());
    try testing.expectEqual(@as(usize, 1), st.counter_updates);
}

test "active skips terminal jobs and orders by queue position" {
    var st = FakeJobStore.init(testing.allocator);
    defer st.deinit();
    const s = st.store();

    const a = try testJob(testing.allocator, "a", "a@h", 1);
    a.queue_order = 30;
    try st.insert(a);
    const b = try testJob(testing.allocator, "b", "b@h", 1);
    b.queue_order = 10;
    try st.insert(b);
    const c = try testJob(testing.allocator, "c", "c@h", 1);
    c.queue_order = 20;
    _ = try c.markCompleted(1);
    try st.insert(c);

    const rows = try s.active(testing.allocator, null);
    defer testing.allocator.free(rows);
    try testing.expectEqual(@as(usize, 2), rows.len);
    try testing.expectEqual(b.id, rows[0].id);
    try testing.expectEqual(a.id, rows[1].id);
    try testing.expectEqual(@as(u32, 2), try s.countActive(null));
    try testing.expectEqual(@as(u32, 3), try s.countAll(null));
}

test "delete drops the aggregate and reports a missing id" {
    var st = FakeJobStore.init(testing.allocator);
    defer st.deinit();
    const s = st.store();
    const j = try testJob(testing.allocator, "h", "m@h", 1);
    try st.insert(j);
    try s.delete(null, j.id);
    try testing.expectEqual(@as(usize, 0), st.len());
    try testing.expectError(error.JobNotFound, s.delete(null, 1));
}

test "the fake fetcher fails a fixed number of times per message id" {
    var f: FakeFetcher = .{
        .bodies = &.{ .{ .message_id = "a@h", .body = "AAA" }, .{ .message_id = "b@h", .body = "BBB" } },
        .fail_first = 2,
    };
    const fe = f.fetcher();
    try testing.expectError(error.Network, fe.fetch(testing.allocator, 0, "a@h"));
    try testing.expectError(error.Network, fe.fetch(testing.allocator, 0, "a@h"));
    const body = try fe.fetch(testing.allocator, 0, "a@h");
    defer testing.allocator.free(body.bytes);
    try testing.expectEqualStrings("AAA", body.bytes);
    try testing.expectEqual(@as(ServerId, 1), body.server_id);
    // Budgets are per message-id, so b starts fresh.
    try testing.expectError(error.Network, fe.fetch(testing.allocator, 0, "b@h"));
    try testing.expectEqual(@as(usize, 4), f.calls);
    // Anything not in the script is a 430 everywhere.
    try testing.expectError(error.ArticleMissing, fe.fetch(testing.allocator, 0, "ghost@h"));
}

test "a pool is usable only when enabled and in quota" {
    try testing.expect((PoolInfo{ .id = 1 }).usable());
    try testing.expect(!(PoolInfo{ .id = 1, .enabled = false }).usable());
    try testing.expect(!(PoolInfo{ .id = 1, .quota_exhausted = true }).usable());
}
