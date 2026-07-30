//! The seam between the SAB API and everything below it.
//!
//! This package is an anti-corruption layer, and it is only worth having
//! one if it can be tested without the machinery it corrupts. So the
//! handler talks to four vtables — queue reads and commands, category
//! reads, job submission, NZB fetching — plus three trivial providers
//! (API key, throughput, clock). Every one of them has a `Fake*` double
//! here, and the whole handler test suite runs on those: no SQLite, no
//! orchestrator, no sockets.
//!
//! The pattern is `app/notify/transport.zig`'s: a struct of function
//! pointers with a `*anyopaque` context, rather than `anytype`, so
//! `Handler` stays a plain non-generic struct that a process can hold in
//! a field for its lifetime.
//!
//! ## Narrowed views, not aggregates
//!
//! `JobView` is deliberately not `domain.download.Job`. The DTO layer
//! reads eleven scalars off a job and nothing else; handing it the
//! aggregate would let a projection reach for an accessor that has to
//! load segments, and would make every DTO test build a real aggregate.
//! `fromJob` is the one place the two are connected, so the wiring layer
//! pays that cost once.
//!
//! Everything a port returns is allocated from the **request arena** the
//! handler passes in. No port result is ever freed individually; the
//! arena dies when the response is written.

const std = @import("std");
const download = @import("../../domain/download/job.zig");

const Allocator = std.mem.Allocator;

pub const JobId = download.JobId;
pub const JobState = download.JobState;
pub const FileState = download.FileState;

/// Why a port could not answer. The *status code* is chosen by the call
/// site, not by the error — the Go handler mapped the same repository
/// error to 500 on a list, 404 on a lookup and 400 on a command, and the
/// *arr clients depend on that (a 500 from `mode=queue` makes Sonarr back
/// off; a 400 makes it drop the item).
pub const PortError = error{
    /// The store or service is unreachable / failed internally.
    Unavailable,
    /// No such job.
    NotFound,
    /// Well-formed but refused — wrong state, constraint violation.
    Rejected,
    OutOfMemory,
};

// ---------------------------------------------------------------------
// Views
// ---------------------------------------------------------------------

/// Everything the SAB projections read off a job. Timestamps are the
/// domain's unix **milliseconds**; `null` is Go's zero `time.Time`.
pub const JobView = struct {
    id: JobId = 0,
    nzb_hash: []const u8 = "",
    name: []const u8 = "",
    category: []const u8 = "",
    priority: i32 = 0,
    state: JobState = .queued,
    total_bytes: i64 = 0,
    done_bytes: i64 = 0,
    added_at_ms: ?i64 = null,
    finished_at_ms: ?i64 = null,
    error_msg: []const u8 = "",

    /// Borrows every string from `j`, so the view must not outlive the
    /// aggregate.
    pub fn fromJob(j: *const download.Job) JobView {
        return .{
            .id = j.id,
            .nzb_hash = j.nzb_hash,
            .name = j.name,
            .category = j.category,
            .priority = j.priority,
            .state = j.state,
            .total_bytes = j.total_bytes,
            .done_bytes = j.done_bytes,
            .added_at_ms = if (j.added_at == 0) null else j.added_at,
            .finished_at_ms = j.finished_at,
            .error_msg = j.error_msg orelse "",
        };
    }
};

/// What `mode=get_files` needs per file.
pub const FileView = struct {
    id: i64 = 0,
    filename: []const u8 = "",
    size_bytes: i64 = 0,
    segment_count: i32 = 0,
    segments_done: i32 = 0,
    state: FileState = .pending,

    pub fn fromFile(f: *const download.File) FileView {
        return .{
            .id = f.id,
            .filename = f.filename,
            .size_bytes = f.size_bytes,
            .segment_count = f.segment_count,
            .segments_done = f.segments_done,
            .state = f.state,
        };
    }
};

pub const JobDetail = struct {
    job: JobView,
    files: []const FileView = &.{},

    /// Projects an aggregate, allocating the file slice from `gpa` (the
    /// request arena).
    pub fn fromJob(gpa: Allocator, j: *const download.Job) Allocator.Error!JobDetail {
        const files = try gpa.alloc(FileView, j.files.len);
        for (j.files, 0..) |*f, i| files[i] = FileView.fromFile(f);
        return .{ .job = JobView.fromJob(j), .files = files };
    }
};

/// A download category, as `mode=get_cats` / `mode=get_config` report it.
pub const Category = struct {
    name: []const u8,
    dir: []const u8 = "",
    priority: i32 = 0,
};

// ---------------------------------------------------------------------
// Ports
// ---------------------------------------------------------------------

pub const QueuePort = struct {
    ctx: *anyopaque,

    /// Active (non-terminal) jobs, in queue order.
    activeJobsFn: *const fn (ctx: *anyopaque, gpa: Allocator) PortError![]const JobView,
    /// Terminal jobs, newest first, at most `limit`.
    historyJobsFn: *const fn (ctx: *anyopaque, gpa: Allocator, limit: usize) PortError![]const JobView,
    /// One job with its files hydrated.
    getFn: *const fn (ctx: *anyopaque, gpa: Allocator, id: JobId) PortError!JobDetail,

    pauseFn: *const fn (ctx: *anyopaque, id: JobId) PortError!void,
    resumeFn: *const fn (ctx: *anyopaque, id: JobId) PortError!void,
    removeFn: *const fn (ctx: *anyopaque, id: JobId) PortError!void,
    /// Flips a failed job to completed without touching the files on
    /// disk. SAB's web UI exposes this and *arr calls it after a manual
    /// re-import.
    markCompletedFn: *const fn (ctx: *anyopaque, id: JobId) PortError!void,

    pub fn activeJobs(self: QueuePort, gpa: Allocator) PortError![]const JobView {
        return self.activeJobsFn(self.ctx, gpa);
    }
    pub fn historyJobs(self: QueuePort, gpa: Allocator, limit: usize) PortError![]const JobView {
        return self.historyJobsFn(self.ctx, gpa, limit);
    }
    pub fn get(self: QueuePort, gpa: Allocator, id: JobId) PortError!JobDetail {
        return self.getFn(self.ctx, gpa, id);
    }
    pub fn pause(self: QueuePort, id: JobId) PortError!void {
        return self.pauseFn(self.ctx, id);
    }
    pub fn @"resume"(self: QueuePort, id: JobId) PortError!void {
        return self.resumeFn(self.ctx, id);
    }
    pub fn remove(self: QueuePort, id: JobId) PortError!void {
        return self.removeFn(self.ctx, id);
    }
    pub fn markCompleted(self: QueuePort, id: JobId) PortError!void {
        return self.markCompletedFn(self.ctx, id);
    }
};

pub const CategoryPort = struct {
    ctx: *anyopaque,
    listFn: *const fn (ctx: *anyopaque, gpa: Allocator) PortError![]const Category,

    pub fn list(self: CategoryPort, gpa: Allocator) PortError![]const Category {
        return self.listFn(self.ctx, gpa);
    }
};

pub const AddJobCmd = struct {
    /// Raw NZB bytes. Borrowed from the request body.
    nzb: []const u8,
    name: []const u8,
    category: []const u8 = "",
    /// Free-text provenance; the Go handler passed the User-Agent.
    source: []const u8 = "",
};

pub const AddOutcome = struct {
    id: JobId,
    /// The NZB was already known. Not an error: the Go handler tolerated
    /// `ErrDuplicateNZB` and answered 200 with the existing job's handle,
    /// because Sonarr retries a grab it thinks failed and a 400 there
    /// makes it blacklist the release.
    duplicate: bool = false,
};

pub const AddJobPort = struct {
    ctx: *anyopaque,
    addFn: *const fn (ctx: *anyopaque, gpa: Allocator, cmd: AddJobCmd) PortError!AddOutcome,

    pub fn add(self: AddJobPort, gpa: Allocator, cmd: AddJobCmd) PortError!AddOutcome {
        return self.addFn(self.ctx, gpa, cmd);
    }
};

pub const FetchError = error{
    /// DNS, connect, TLS, timeout, or a malformed response — anything
    /// that leaves us without a body. Answered as 502.
    Fetch,
    OutOfMemory,
};

pub const FetchResponse = struct {
    status: u16,
    /// Raw `Content-Disposition`, used to recover a display name.
    content_disposition: []const u8 = "",
    body: []const u8 = "",
};

/// `mode=addurl` fetches an NZB over HTTP. A port rather than a client so
/// the tests exercise redirect-free, socket-free paths.
pub const FetchPort = struct {
    ctx: *anyopaque,
    getFn: *const fn (ctx: *anyopaque, gpa: Allocator, url: []const u8) FetchError!FetchResponse,

    pub fn get(self: FetchPort, gpa: Allocator, url: []const u8) FetchError!FetchResponse {
        return self.getFn(self.ctx, gpa, url);
    }
};

/// The active API key, read per request so rotating it from Settings takes
/// effect on the very next request instead of at the next restart.
pub const ApiKeyPort = struct {
    ctx: ?*anyopaque = null,
    keyFn: *const fn (ctx: ?*anyopaque) []const u8,

    pub fn key(self: ApiKeyPort) []const u8 {
        return self.keyFn(self.ctx);
    }

    /// Provider for a key held in a `[]const u8` the caller owns.
    /// Assigning to that variable rotates the key.
    pub fn fromPointer(ctx: ?*anyopaque) []const u8 {
        const p: *const []const u8 = @ptrCast(@alignCast(ctx.?));
        return p.*;
    }
};

/// Overall download rate in bytes/sec, for `queue.speed` / `kbpersec` and
/// the per-slot ETAs. Optional: without it the queue reports 0 and
/// "unknown", which is what the Go build did before it was wired.
pub const ThroughputPort = struct {
    ctx: ?*anyopaque = null,
    rateFn: *const fn (ctx: ?*anyopaque) i64,

    pub fn rate(self: ThroughputPort) i64 {
        return self.rateFn(self.ctx);
    }
};

/// Wall clock in unix seconds. Injectable because `eta` is a formatted
/// timestamp, and a test that cannot pin the clock cannot assert the
/// bytes of a queue response.
pub const ClockPort = struct {
    ctx: ?*anyopaque = null,
    nowFn: *const fn (ctx: ?*anyopaque) i64,

    pub fn now(self: ClockPort) i64 {
        return self.nowFn(self.ctx);
    }
};

// ---------------------------------------------------------------------
// Test doubles
// ---------------------------------------------------------------------

/// In-memory queue. Serves `active` and `history` as given, resolves
/// `get` against both, and records every command so a test can assert
/// *which* jobs an action touched rather than only that it returned 200.
pub const FakeQueue = struct {
    active: []const JobView = &.{},
    history: []const JobView = &.{},
    /// Files returned by `get`, for `mode=get_files`.
    files: []const FileView = &.{},

    /// When set, the matching call fails with this instead of answering.
    fail_active: ?PortError = null,
    fail_history: ?PortError = null,
    fail_get: ?PortError = null,
    fail_command: ?PortError = null,

    /// Recorded calls.
    history_limit: usize = 0,
    paused: [16]JobId = @splat(0),
    n_paused: usize = 0,
    resumed: [16]JobId = @splat(0),
    n_resumed: usize = 0,
    removed: [16]JobId = @splat(0),
    n_removed: usize = 0,
    completed: [16]JobId = @splat(0),
    n_completed: usize = 0,

    pub fn port(self: *FakeQueue) QueuePort {
        return .{
            .ctx = @ptrCast(self),
            .activeJobsFn = &fakeActive,
            .historyJobsFn = &fakeHistory,
            .getFn = &fakeGet,
            .pauseFn = &fakePause,
            .resumeFn = &fakeResume,
            .removeFn = &fakeRemove,
            .markCompletedFn = &fakeMarkCompleted,
        };
    }

    fn self_(ctx: *anyopaque) *FakeQueue {
        return @ptrCast(@alignCast(ctx));
    }

    fn fakeActive(ctx: *anyopaque, gpa: Allocator) PortError![]const JobView {
        _ = gpa;
        const self = self_(ctx);
        if (self.fail_active) |e| return e;
        return self.active;
    }

    fn fakeHistory(ctx: *anyopaque, gpa: Allocator, limit: usize) PortError![]const JobView {
        _ = gpa;
        const self = self_(ctx);
        self.history_limit = limit;
        if (self.fail_history) |e| return e;
        return self.history[0..@min(limit, self.history.len)];
    }

    fn fakeGet(ctx: *anyopaque, gpa: Allocator, id: JobId) PortError!JobDetail {
        _ = gpa;
        const self = self_(ctx);
        if (self.fail_get) |e| return e;
        for (self.active) |j| {
            if (j.id == id) return .{ .job = j, .files = self.files };
        }
        for (self.history) |j| {
            if (j.id == id) return .{ .job = j, .files = self.files };
        }
        return error.NotFound;
    }

    fn record(list: *[16]JobId, n: *usize, id: JobId) void {
        if (n.* < list.len) {
            list[n.*] = id;
            n.* += 1;
        }
    }

    fn fakePause(ctx: *anyopaque, id: JobId) PortError!void {
        const self = self_(ctx);
        if (self.fail_command) |e| return e;
        record(&self.paused, &self.n_paused, id);
    }
    fn fakeResume(ctx: *anyopaque, id: JobId) PortError!void {
        const self = self_(ctx);
        if (self.fail_command) |e| return e;
        record(&self.resumed, &self.n_resumed, id);
    }
    fn fakeRemove(ctx: *anyopaque, id: JobId) PortError!void {
        const self = self_(ctx);
        if (self.fail_command) |e| return e;
        record(&self.removed, &self.n_removed, id);
    }
    fn fakeMarkCompleted(ctx: *anyopaque, id: JobId) PortError!void {
        const self = self_(ctx);
        if (self.fail_command) |e| return e;
        record(&self.completed, &self.n_completed, id);
    }

    pub fn pauses(self: *const FakeQueue) []const JobId {
        return self.paused[0..self.n_paused];
    }
    pub fn resumes(self: *const FakeQueue) []const JobId {
        return self.resumed[0..self.n_resumed];
    }
    pub fn removes(self: *const FakeQueue) []const JobId {
        return self.removed[0..self.n_removed];
    }
    pub fn completions(self: *const FakeQueue) []const JobId {
        return self.completed[0..self.n_completed];
    }
};

pub const FakeCategories = struct {
    rows: []const Category = &.{},
    fail: ?PortError = null,

    pub fn port(self: *FakeCategories) CategoryPort {
        return .{ .ctx = @ptrCast(self), .listFn = &list };
    }

    fn list(ctx: *anyopaque, gpa: Allocator) PortError![]const Category {
        _ = gpa;
        const self: *FakeCategories = @ptrCast(@alignCast(ctx));
        if (self.fail) |e| return e;
        return self.rows;
    }
};

pub const FakeAddJob = struct {
    /// Handed back on success.
    id: JobId = 1,
    duplicate: bool = false,
    fail: ?PortError = null,

    /// Last command seen, for assertions on name / category / source and
    /// on the NZB bytes actually forwarded.
    last: ?AddJobCmd = null,
    calls: usize = 0,

    pub fn port(self: *FakeAddJob) AddJobPort {
        return .{ .ctx = @ptrCast(self), .addFn = &add };
    }

    fn add(ctx: *anyopaque, gpa: Allocator, cmd: AddJobCmd) PortError!AddOutcome {
        _ = gpa;
        const self: *FakeAddJob = @ptrCast(@alignCast(ctx));
        self.last = cmd;
        self.calls += 1;
        if (self.fail) |e| return e;
        return .{ .id = self.id, .duplicate = self.duplicate };
    }
};

pub const FakeFetch = struct {
    status: u16 = 200,
    content_disposition: []const u8 = "",
    body: []const u8 = "",
    fail: ?FetchError = null,

    last_url: []const u8 = "",
    calls: usize = 0,

    pub fn port(self: *FakeFetch) FetchPort {
        return .{ .ctx = @ptrCast(self), .getFn = &get };
    }

    fn get(ctx: *anyopaque, gpa: Allocator, url: []const u8) FetchError!FetchResponse {
        _ = gpa;
        const self: *FakeFetch = @ptrCast(@alignCast(ctx));
        self.last_url = url;
        self.calls += 1;
        if (self.fail) |e| return e;
        return .{
            .status = self.status,
            .content_disposition = self.content_disposition,
            .body = self.body,
        };
    }
};

/// A fixed key and a fixed clock — the two things every handler test
/// needs to pin before it can assert bytes.
pub const FakeKey = struct {
    value: []const u8 = "",

    pub fn port(self: *FakeKey) ApiKeyPort {
        return .{ .ctx = @ptrCast(self), .keyFn = &key };
    }

    fn key(ctx: ?*anyopaque) []const u8 {
        const self: *FakeKey = @ptrCast(@alignCast(ctx.?));
        return self.value;
    }
};

pub const FakeClock = struct {
    unix_secs: i64 = 0,

    pub fn port(self: *FakeClock) ClockPort {
        return .{ .ctx = @ptrCast(self), .nowFn = &now };
    }

    fn now(ctx: ?*anyopaque) i64 {
        const self: *FakeClock = @ptrCast(@alignCast(ctx.?));
        return self.unix_secs;
    }
};

pub const FakeThroughput = struct {
    bytes_per_sec: i64 = 0,

    pub fn port(self: *FakeThroughput) ThroughputPort {
        return .{ .ctx = @ptrCast(self), .rateFn = &rate };
    }

    fn rate(ctx: ?*anyopaque) i64 {
        const self: *FakeThroughput = @ptrCast(@alignCast(ctx.?));
        return self.bytes_per_sec;
    }
};

// ---------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------

const testing = std.testing;

test "FakeQueue serves lists and resolves get from both of them" {
    var q: FakeQueue = .{
        .active = &.{.{ .id = 1, .name = "a" }},
        .history = &.{ .{ .id = 2, .name = "b" }, .{ .id = 3, .name = "c" } },
        .files = &.{.{ .id = 9, .filename = "f.rar" }},
    };
    const p = q.port();
    const active = try p.activeJobs(testing.allocator);
    try testing.expectEqual(@as(usize, 1), active.len);

    const hist = try p.historyJobs(testing.allocator, 1);
    try testing.expectEqual(@as(usize, 1), hist.len);
    try testing.expectEqual(@as(usize, 1), q.history_limit);

    const d = try p.get(testing.allocator, 3);
    try testing.expectEqualStrings("c", d.job.name);
    try testing.expectEqual(@as(usize, 1), d.files.len);
    try testing.expectError(error.NotFound, p.get(testing.allocator, 99));
}

test "FakeQueue records which jobs each command touched" {
    var q: FakeQueue = .{ .active = &.{.{ .id = 7 }} };
    const p = q.port();
    try p.pause(7);
    try p.@"resume"(7);
    try p.remove(8);
    try p.markCompleted(9);
    try testing.expectEqualSlices(JobId, &.{7}, q.pauses());
    try testing.expectEqualSlices(JobId, &.{7}, q.resumes());
    try testing.expectEqualSlices(JobId, &.{8}, q.removes());
    try testing.expectEqualSlices(JobId, &.{9}, q.completions());
}

test "FakeQueue injects failures per call site" {
    var q: FakeQueue = .{ .fail_active = error.Unavailable, .fail_command = error.Rejected };
    const p = q.port();
    try testing.expectError(error.Unavailable, p.activeJobs(testing.allocator));
    try testing.expectError(error.Rejected, p.pause(1));
    // A port that was not told to fail still answers.
    _ = try p.historyJobs(testing.allocator, 10);
}

test "ApiKeyPort.fromPointer follows a rotating key" {
    var key: []const u8 = "first";
    const p: ApiKeyPort = .{ .ctx = @ptrCast(&key), .keyFn = &ApiKeyPort.fromPointer };
    try testing.expectEqualStrings("first", p.key());
    key = "second";
    try testing.expectEqualStrings("second", p.key());
}

test "JobView.fromJob narrows the aggregate and normalises the zero instant" {
    var job: download.Job = .{ .allocator = testing.allocator };
    job.id = 5;
    job.name = "Rel";
    job.nzb_hash = "abcdef1234";
    job.category = "tv";
    job.total_bytes = 100;
    job.done_bytes = 40;
    job.state = .downloading;
    // added_at 0 is Go's zero time.Time, which every SAB field renders as 0.
    const v = JobView.fromJob(&job);
    try testing.expectEqual(@as(JobId, 5), v.id);
    try testing.expectEqualStrings("Rel", v.name);
    try testing.expectEqual(@as(?i64, null), v.added_at_ms);
    try testing.expectEqual(@as(?i64, null), v.finished_at_ms);
    try testing.expectEqualStrings("", v.error_msg);

    job.added_at = 1700000000_000;
    job.error_msg = "boom";
    const v2 = JobView.fromJob(&job);
    try testing.expectEqual(@as(?i64, 1700000000_000), v2.added_at_ms);
    try testing.expectEqualStrings("boom", v2.error_msg);
}

test "FakeAddJob records the command and can report a duplicate" {
    var add: FakeAddJob = .{ .id = 42, .duplicate = true };
    const p = add.port();
    const out = try p.add(testing.allocator, .{ .nzb = "<nzb/>", .name = "Rel", .category = "tv" });
    try testing.expectEqual(@as(JobId, 42), out.id);
    try testing.expect(out.duplicate);
    try testing.expectEqualStrings("<nzb/>", add.last.?.nzb);
    try testing.expectEqualStrings("Rel", add.last.?.name);
}
