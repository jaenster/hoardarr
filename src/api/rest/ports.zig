//! The ports the REST layer needs, and a fake for each.
//!
//! Every one is a struct of function pointers with an opaque context,
//! the same shape as `app/notify/transport.zig`. Handlers know these and
//! nothing else: no store type, no app service, no SQL. `bootstrap/` is
//! the only place that decides which adapter satisfies which port —
//! `docs/architecture.md` is explicit about that, and it is what lets
//! this whole layer be tested without a database, a socket or a
//! filesystem.
//!
//! The Go equivalents were interfaces declared next to the handler
//! (`Auther`, `SystemStatuser`, `BackupAdmin`, …) for exactly the same
//! reason. This file collects them because in Zig the vtable is data
//! rather than a type, and one file of data is easier to keep honest
//! than nine.
//!
//! ## Errors
//!
//! One error set, mapped once to HTTP in `handlers.zig`. An adapter
//! translates its own failures into these — `error.NotFound` for a
//! missing row, `error.Conflict` for a unique-constraint violation, and
//! so on. That mapping is the adapter's job because only it knows what
//! its failures mean; the handler must never be in the business of
//! guessing a status code from a driver error.
//!
//! ## Memory
//!
//! Anything a port returns by slice is allocated from the `arena` it is
//! handed, which the handler resets after the response goes out. No port
//! returns memory the caller has to free individually, and no port
//! returns a slice that outlives the request.

const std = @import("std");
const download = @import("../../domain/download/job.zig");
const server_domain = @import("../../domain/server.zig");
const notify_domain = @import("../../domain/notify.zig");
const schedule_domain = @import("../../domain/schedule.zig");
const command_domain = @import("../../domain/command.zig");
const auth_domain = @import("../../domain/auth.zig");
const event = @import("../../domain/event.zig");

const Allocator = std.mem.Allocator;

pub const Job = download.Job;
pub const JobId = download.JobId;
pub const JobState = download.JobState;
pub const UsenetServer = server_domain.UsenetServer;
pub const ServerId = server_domain.ServerId;
pub const Subscription = notify_domain.Subscription;
pub const SubscriptionId = notify_domain.SubscriptionId;
pub const Task = schedule_domain.Task;
pub const TaskId = schedule_domain.TaskId;
pub const Command = command_domain.Command;
pub const CommandId = command_domain.CommandId;
pub const Timestamp = event.Timestamp;
pub const Envelope = event.Envelope;

/// What a port may fail with. Deliberately about *meaning*, not about
/// mechanism: the handler turns each of these into one status code and
/// one message, and an adapter that cannot decide picks `Internal`.
pub const Error = error{
    /// No such row. 404.
    NotFound,
    /// Unique constraint, or an operation the current state forbids —
    /// a duplicate server name, setup run twice. 409.
    Conflict,
    /// The request was understood and refused. 400.
    Invalid,
    /// Credentials missing or wrong. 401.
    Unauthorized,
    /// Understood, authenticated, and still not allowed — deleting a
    /// reserved category. 403.
    Forbidden,
    /// A dependency is not wired up or is momentarily unusable. 503.
    Unavailable,
    /// An upstream we are a proxy for failed: a webhook test that could
    /// not reach the subscriber. 502.
    Upstream,
    /// The client went away mid-query. Logged at debug, never as an
    /// error — see `isClientDisconnect` in the Go original.
    Canceled,
    /// Anything else. 500.
    Internal,
} || Allocator.Error;

/// One human-readable sentence per error, for the `{"error": …}` body.
/// Deliberately free of internals: a driver message can name a table, a
/// path, or a column, and none of that belongs in an unauthenticated
/// 500. Handlers that have something more specific to say pass their own
/// message instead.
pub fn message(err: Error) []const u8 {
    return switch (err) {
        error.NotFound => "not found",
        error.Conflict => "conflict",
        error.Invalid => "invalid request",
        error.Unauthorized => "authentication required",
        error.Forbidden => "forbidden",
        error.Unavailable => "service unavailable",
        error.Upstream => "upstream request failed",
        error.Canceled => "request cancelled",
        error.Internal => "internal error",
        error.OutOfMemory => "out of memory",
    };
}

// ---------------------------------------------------------------------
// Queue
// ---------------------------------------------------------------------

/// Filter for `/api/v1/history`. Mirrors `download.HistoryQuery`.
pub const HistoryQuery = struct {
    /// Exact match; empty means every category.
    category: []const u8 = "",
    state: ?JobState = null,
    /// Unix ms; only jobs finished after this instant.
    since_ms: ?i64 = null,
    /// 0 means "the adapter's default". The handler has already clamped
    /// anything the client sent.
    limit: i32 = 0,
};

pub const AddJobRequest = struct {
    /// Raw NZB bytes. Borrowed for the duration of the call.
    nzb: []const u8,
    /// Display name, already stripped of its `.nzb` suffix.
    name: []const u8 = "",
    category: []const u8 = "",
    /// The client's `User-Agent`, so the queue can show what added a job.
    source: []const u8 = "",
};

/// A duplicate NZB is not an error on this API: the Go handler answered
/// 200 with `duplicate: true` and the existing job's state, because
/// Sonarr re-posting the same release is routine and a 4xx makes it
/// retry forever.
pub const AddJobResult = struct {
    id: JobId,
    duplicate: bool = false,
    /// Populated on a duplicate, when the existing job could be read.
    state: []const u8 = "",
    name: []const u8 = "",
};

pub const Queue = struct {
    ctx: ?*anyopaque = null,

    /// Jobs in a non-terminal state, without files or segments. The list
    /// endpoint does not render per-file rows, and skipping the N file
    /// queries per poll is the dominant win under *arr polling pressure.
    listActiveFn: *const fn (ctx: ?*anyopaque, arena: Allocator) Error![]const *Job,
    /// Every job, still without files.
    listAllFn: *const fn (ctx: ?*anyopaque, arena: Allocator) Error![]const *Job,
    historyFn: *const fn (ctx: ?*anyopaque, arena: Allocator, q: HistoryQuery) Error![]const *Job,
    /// One job with files *and* segments hydrated — the detail page.
    getFn: *const fn (ctx: ?*anyopaque, arena: Allocator, id: JobId) Error!*Job,
    pauseFn: *const fn (ctx: ?*anyopaque, id: JobId) Error!void,
    resumeFn: *const fn (ctx: ?*anyopaque, id: JobId) Error!void,
    removeFn: *const fn (ctx: ?*anyopaque, id: JobId) Error!void,
    reorderFn: *const fn (ctx: ?*anyopaque, ids: []const JobId) Error!void,
    addFn: *const fn (ctx: ?*anyopaque, arena: Allocator, req: AddJobRequest) Error!AddJobResult,

    pub fn listActive(self: Queue, arena: Allocator) Error![]const *Job {
        return self.listActiveFn(self.ctx, arena);
    }
    pub fn listAll(self: Queue, arena: Allocator) Error![]const *Job {
        return self.listAllFn(self.ctx, arena);
    }
    pub fn history(self: Queue, arena: Allocator, q: HistoryQuery) Error![]const *Job {
        return self.historyFn(self.ctx, arena, q);
    }
    pub fn get(self: Queue, arena: Allocator, id: JobId) Error!*Job {
        return self.getFn(self.ctx, arena, id);
    }
    pub fn pause(self: Queue, id: JobId) Error!void {
        return self.pauseFn(self.ctx, id);
    }
    pub fn unpause(self: Queue, id: JobId) Error!void {
        return self.resumeFn(self.ctx, id);
    }
    pub fn remove(self: Queue, id: JobId) Error!void {
        return self.removeFn(self.ctx, id);
    }
    pub fn reorder(self: Queue, ids: []const JobId) Error!void {
        return self.reorderFn(self.ctx, ids);
    }
    pub fn add(self: Queue, arena: Allocator, req: AddJobRequest) Error!AddJobResult {
        return self.addFn(self.ctx, arena, req);
    }
};

/// The per-job timeline: every bus envelope that touched one job.
pub const Events = struct {
    ctx: ?*anyopaque = null,
    byJobFn: *const fn (ctx: ?*anyopaque, arena: Allocator, id: JobId) Error![]const Envelope,

    pub fn byJob(self: Events, arena: Allocator, id: JobId) Error![]const Envelope {
        return self.byJobFn(self.ctx, arena, id);
    }
};

// ---------------------------------------------------------------------
// Servers
// ---------------------------------------------------------------------

pub const AddServerCmd = struct {
    name: []const u8,
    host: []const u8,
    port: u16,
    /// Absent means the adapter's default, which is TLS on.
    tls: ?bool = null,
    username: []const u8 = "",
    password: []const u8 = "",
    max_conns: i32 = 0,
    priority: i32 = 0,
    backup: bool = false,
    /// Empty means flat.
    billing_mode: []const u8 = "",
    quota_bytes: i64 = 0,
    bandwidth_bytes_per_sec: i64 = 0,
};

/// Every field optional, so "absent" and "set to the zero value" stay
/// distinguishable — the same reason the Go request struct was all
/// pointers.
pub const UpdateServerCmd = struct {
    id: ServerId,
    host: ?[]const u8 = null,
    port: ?u16 = null,
    tls: ?bool = null,
    username: ?[]const u8 = null,
    password: ?[]const u8 = null,
    max_conns: ?i32 = null,
    priority: ?i32 = null,
    backup: ?bool = null,
    billing_mode: ?[]const u8 = null,
    quota_bytes: ?i64 = null,
    bandwidth_bytes_per_sec: ?i64 = null,
};

pub const Servers = struct {
    ctx: ?*anyopaque = null,

    listFn: *const fn (ctx: ?*anyopaque, arena: Allocator) Error![]const *UsenetServer,
    getFn: *const fn (ctx: ?*anyopaque, arena: Allocator, id: ServerId) Error!*UsenetServer,
    addFn: *const fn (ctx: ?*anyopaque, cmd: AddServerCmd) Error!ServerId,
    updateFn: *const fn (ctx: ?*anyopaque, cmd: UpdateServerCmd) Error!void,
    removeFn: *const fn (ctx: ?*anyopaque, id: ServerId) Error!void,
    setEnabledFn: *const fn (ctx: ?*anyopaque, id: ServerId, enabled: bool) Error!void,

    pub fn list(self: Servers, arena: Allocator) Error![]const *UsenetServer {
        return self.listFn(self.ctx, arena);
    }
    pub fn get(self: Servers, arena: Allocator, id: ServerId) Error!*UsenetServer {
        return self.getFn(self.ctx, arena, id);
    }
    pub fn add(self: Servers, cmd: AddServerCmd) Error!ServerId {
        return self.addFn(self.ctx, cmd);
    }
    pub fn update(self: Servers, cmd: UpdateServerCmd) Error!void {
        return self.updateFn(self.ctx, cmd);
    }
    pub fn remove(self: Servers, id: ServerId) Error!void {
        return self.removeFn(self.ctx, id);
    }
    pub fn setEnabled(self: Servers, id: ServerId, enabled: bool) Error!void {
        return self.setEnabledFn(self.ctx, id, enabled);
    }
};

/// Result of a connection probe. Field for field the Go
/// `nntptest.Result`, because the Settings UI renders each step as its
/// own tick or cross.
pub const ProbeResult = struct {
    ok: bool = false,
    dial: bool = false,
    greeted: bool = false,
    auth: bool = false,
    mode_reader: bool = false,
    date: bool = false,
    /// The server's `DATE` reply, when it gave one.
    server_date: []const u8 = "",
    /// Failure text. Never the password — the probe builds this itself.
    err: []const u8 = "",
    elapsed_ms: i64 = 0,
};

pub const ProbeParams = struct {
    host: []const u8,
    port: u16,
    tls: bool = true,
    username: []const u8 = "",
    password: []const u8 = "",
};

/// Dialling a Usenet server to see whether the credentials work.
///
/// A port rather than a direct call because the NNTP client is not
/// ported yet — and because a handler that opens a socket cannot be
/// tested. `probe` never fails: a refused connection is a `ProbeResult`
/// with `ok = false`, which is what the UI renders.
pub const ServerProbe = struct {
    ctx: ?*anyopaque = null,
    probeFn: *const fn (ctx: ?*anyopaque, arena: Allocator, p: ProbeParams) Allocator.Error!ProbeResult,

    pub fn probe(self: ServerProbe, arena: Allocator, p: ProbeParams) Allocator.Error!ProbeResult {
        return self.probeFn(self.ctx, arena, p);
    }
};

// ---------------------------------------------------------------------
// Categories
// ---------------------------------------------------------------------

/// The wire and storage shape of a category. Owned by this layer rather
/// than imported from the store, because `sqlite.Category` is one
/// adapter's row type and this is the contract.
pub const Category = struct {
    name: []const u8,
    dir: []const u8 = "",
    priority: i32 = 0,
};

pub const Categories = struct {
    ctx: ?*anyopaque = null,

    listFn: *const fn (ctx: ?*anyopaque, arena: Allocator) Error![]const Category,
    /// Insert or update by name.
    saveFn: *const fn (ctx: ?*anyopaque, c: Category) Error!void,
    /// `error.Forbidden` for a reserved name, `error.NotFound` for an
    /// unknown one — both distinguishable in the Go handler's switch.
    deleteFn: *const fn (ctx: ?*anyopaque, name: []const u8) Error!void,

    pub fn list(self: Categories, arena: Allocator) Error![]const Category {
        return self.listFn(self.ctx, arena);
    }
    pub fn save(self: Categories, c: Category) Error!void {
        return self.saveFn(self.ctx, c);
    }
    pub fn delete(self: Categories, name: []const u8) Error!void {
        return self.deleteFn(self.ctx, name);
    }
};

// ---------------------------------------------------------------------
// System
// ---------------------------------------------------------------------

pub const PoolStatus = struct {
    server_id: i64 = 0,
    server_name: []const u8 = "",
    host: []const u8 = "",
    port: u16 = 0,
    max_conns: i32 = 0,
    in_use: i32 = 0,
    idle: i32 = 0,
    enabled: bool = false,
    backup: bool = false,
    billing_mode: []const u8 = "",
    quota_bytes: i64 = 0,
    used_bytes: i64 = 0,
};

pub const SystemStatus = struct {
    service: []const u8 = "hoardarr",
    version: []const u8 = "",
    commit: []const u8 = "",
    build_date: []const u8 = "",
    /// `runtime_version` on the wire. Was Go's `runtime.Version()`; now
    /// the Zig version the binary was built with. The field name stays
    /// because the System page renders it by name.
    runtime_version: []const u8 = "",
    os: []const u8 = "",
    arch: []const u8 = "",
    is_docker: bool = false,
    database_type: []const u8 = "sqlite",
    migration_version: i32 = 0,
    started_at_ms: i64 = 0,
    uptime_ms: i64 = 0,
    queue_active: i32 = 0,
    queue_total: i32 = 0,
    pools: []const PoolStatus = &.{},
};

/// The rolling throughput window. `series` is one entry per second,
/// oldest first.
pub const ThroughputSample = struct {
    window_seconds: i32 = 0,
    series: []const i64 = &.{},
    total_bytes: i64 = 0,
    current_bytes_per_sec: i64 = 0,
    avg10s_bytes_per_sec: i64 = 0,
    avg60s_bytes_per_sec: i64 = 0,
    window_peak_bytes_per_sec: i64 = 0,
    all_time_peak_bytes_per_sec: i64 = 0,
};

pub const SpeedSample = struct {
    at_ms: i64,
    bytes_per_sec: i64,
};

pub const SpeedHistory = struct {
    resolution_seconds: i32 = 1,
    samples: []const SpeedSample = &.{},
    window_peak_bytes_per_sec: i64 = 0,
    all_time_peak_bytes_per_sec: i64 = 0,
};

/// Default window `Sample()` returns, and the in-memory ring's width.
/// Both are part of the response contract: `window_seconds` is echoed to
/// the client, and the handler picks the DB-backed path for a range
/// wider than the ring.
pub const default_sample_seconds: i32 = 300;
pub const throughput_window_size: i32 = 3600;

pub const System = struct {
    ctx: ?*anyopaque = null,

    statusFn: *const fn (ctx: ?*anyopaque, arena: Allocator) Error!SystemStatus,
    /// Null when throughput tracking is not running, which the Go
    /// handler rendered as an all-zero sample rather than an error.
    throughputFn: *const fn (ctx: ?*anyopaque, arena: Allocator, seconds: i32) Error!?ThroughputSample,
    /// Samples covering `span_seconds` back from now. The adapter
    /// chooses the in-memory ring or the persistent store and reports
    /// which resolution it used.
    historyFn: *const fn (ctx: ?*anyopaque, arena: Allocator, span_seconds: i32) Error!SpeedHistory,

    pub fn status(self: System, arena: Allocator) Error!SystemStatus {
        return self.statusFn(self.ctx, arena);
    }
    pub fn throughput(self: System, arena: Allocator, seconds: i32) Error!?ThroughputSample {
        return self.throughputFn(self.ctx, arena, seconds);
    }
    pub fn speedHistory(self: System, arena: Allocator, span_seconds: i32) Error!SpeedHistory {
        return self.historyFn(self.ctx, arena, span_seconds);
    }
};

// ---------------------------------------------------------------------
// Health
// ---------------------------------------------------------------------

pub const Severity = enum {
    warning,
    err,

    pub fn text(self: Severity) []const u8 {
        // "error" on the wire; `err` only because `error` is a keyword.
        return switch (self) {
            .warning => "warning",
            .err => "error",
        };
    }
};

/// One health finding. `source` is stable across runs so the UI can
/// dedupe and animate transitions.
pub const Issue = struct {
    source: []const u8,
    severity: Severity,
    message: []const u8,
    docs_url: []const u8 = "",
};

pub const HealthSnapshot = struct {
    issues: []const Issue = &.{},
    last_run_ms: i64 = 0,
};

pub const Health = struct {
    ctx: ?*anyopaque = null,

    snapshotFn: *const fn (ctx: ?*anyopaque, arena: Allocator) Error!HealthSnapshot,
    /// Ask for a re-run. Returns once the run is *requested*, not once
    /// it is finished — the Go handler slept 200 ms and re-read, which
    /// this layer does not do because it would block the reactor.
    refreshFn: *const fn (ctx: ?*anyopaque) void,

    pub fn snapshot(self: Health, arena: Allocator) Error!HealthSnapshot {
        return self.snapshotFn(self.ctx, arena);
    }
    pub fn refresh(self: Health) void {
        self.refreshFn(self.ctx);
    }
};

// ---------------------------------------------------------------------
// Scheduled tasks
// ---------------------------------------------------------------------

pub const Schedule = struct {
    ctx: ?*anyopaque = null,

    listFn: *const fn (ctx: ?*anyopaque, arena: Allocator) Error![]const *Task,
    byIdFn: *const fn (ctx: ?*anyopaque, arena: Allocator, id: TaskId) Error!*Task,
    /// Pull the task's next run forward to now. Not "run it here": that
    /// would block the request on a possibly-long task and bypass the
    /// scheduler's claim model.
    runNowFn: *const fn (ctx: ?*anyopaque, id: TaskId) Error!void,

    pub fn list(self: Schedule, arena: Allocator) Error![]const *Task {
        return self.listFn(self.ctx, arena);
    }
    pub fn byId(self: Schedule, arena: Allocator, id: TaskId) Error!*Task {
        return self.byIdFn(self.ctx, arena, id);
    }
    pub fn runNow(self: Schedule, id: TaskId) Error!void {
        return self.runNowFn(self.ctx, id);
    }
};

// ---------------------------------------------------------------------
// Commands
// ---------------------------------------------------------------------

pub const Commands = struct {
    ctx: ?*anyopaque = null,

    submitFn: *const fn (ctx: ?*anyopaque, name: []const u8, body: []const u8) Error!CommandId,
    listFn: *const fn (ctx: ?*anyopaque, arena: Allocator, limit: i32) Error![]const *Command,
    byIdFn: *const fn (ctx: ?*anyopaque, arena: Allocator, id: CommandId) Error!*Command,
    /// Registered handler names, for the UI's dropdown.
    namesFn: *const fn (ctx: ?*anyopaque, arena: Allocator) Error![]const []const u8,

    pub fn submit(self: Commands, name: []const u8, body: []const u8) Error!CommandId {
        return self.submitFn(self.ctx, name, body);
    }
    pub fn list(self: Commands, arena: Allocator, limit: i32) Error![]const *Command {
        return self.listFn(self.ctx, arena, limit);
    }
    pub fn byId(self: Commands, arena: Allocator, id: CommandId) Error!*Command {
        return self.byIdFn(self.ctx, arena, id);
    }
    pub fn names(self: Commands, arena: Allocator) Error![]const []const u8 {
        return self.namesFn(self.ctx, arena);
    }
};

// ---------------------------------------------------------------------
// Files: backups, log files, disk space
// ---------------------------------------------------------------------

pub const FileInfo = struct {
    name: []const u8,
    size_bytes: i64 = 0,
    /// Unix ms. `created_at` for a backup, `updated_at` for a log file.
    at_ms: i64 = 0,
    /// Log files only: the one currently being written.
    active: bool = false,
};

pub const Backups = struct {
    ctx: ?*anyopaque = null,

    listFn: *const fn (ctx: ?*anyopaque, arena: Allocator) Error![]const FileInfo,
    /// Runs the backup and returns when it is done. `VACUUM INTO` on a
    /// hoardarr-sized database is sub-second.
    runFn: *const fn (ctx: ?*anyopaque) Error!void,
    /// Whole file, into `arena`. The port — not the handler — owns the
    /// "is this name safe" decision, because it owns the directory.
    /// `error.Invalid` for a name that fails that check.
    readFn: *const fn (ctx: ?*anyopaque, arena: Allocator, name: []const u8) Error![]const u8,

    pub fn list(self: Backups, arena: Allocator) Error![]const FileInfo {
        return self.listFn(self.ctx, arena);
    }
    pub fn run(self: Backups) Error!void {
        return self.runFn(self.ctx);
    }
    pub fn read(self: Backups, arena: Allocator, name: []const u8) Error![]const u8 {
        return self.readFn(self.ctx, arena, name);
    }
};

pub const LogFiles = struct {
    ctx: ?*anyopaque = null,

    listFn: *const fn (ctx: ?*anyopaque, arena: Allocator) Error![]const FileInfo,
    readFn: *const fn (ctx: ?*anyopaque, arena: Allocator, name: []const u8) Error![]const u8,

    pub fn list(self: LogFiles, arena: Allocator) Error![]const FileInfo {
        return self.listFn(self.ctx, arena);
    }
    pub fn read(self: LogFiles, arena: Allocator, name: []const u8) Error![]const u8 {
        return self.readFn(self.ctx, arena, name);
    }
};

pub const DiskEntry = struct {
    label: []const u8,
    path: []const u8,
    free_bytes: i64 = 0,
    total_bytes: i64 = 0,
    used_bytes: i64 = 0,
    /// False when `statfs` failed; the UI renders that differently from
    /// "healthy but full".
    reachable: bool = true,
    err: []const u8 = "",
};

pub const DiskSpace = struct {
    ctx: ?*anyopaque = null,
    snapshotFn: *const fn (ctx: ?*anyopaque, arena: Allocator) Error![]const DiskEntry,

    pub fn snapshot(self: DiskSpace, arena: Allocator) Error![]const DiskEntry {
        return self.snapshotFn(self.ctx, arena);
    }
};

// ---------------------------------------------------------------------
// Subscriptions (webhooks)
// ---------------------------------------------------------------------

pub const AddSubscriptionCmd = struct {
    name: []const u8,
    /// Empty means `webhook`.
    kind: []const u8 = "",
    url: []const u8,
    topics: []const []const u8 = &.{},
    secret: []const u8 = "",
};

pub const UpdateSubscriptionCmd = struct {
    url: ?[]const u8 = null,
    topics: ?[]const []const u8 = null,
    secret: ?[]const u8 = null,
    enabled: ?bool = null,
};

pub const Subscriptions = struct {
    ctx: ?*anyopaque = null,

    listFn: *const fn (ctx: ?*anyopaque, arena: Allocator) Error![]const *Subscription,
    addFn: *const fn (ctx: ?*anyopaque, cmd: AddSubscriptionCmd) Error!SubscriptionId,
    updateFn: *const fn (ctx: ?*anyopaque, id: SubscriptionId, cmd: UpdateSubscriptionCmd) Error!void,
    removeFn: *const fn (ctx: ?*anyopaque, id: SubscriptionId) Error!void,
    setEnabledFn: *const fn (ctx: ?*anyopaque, id: SubscriptionId, enabled: bool) Error!void,
    /// Send a test delivery. `error.Upstream` when the subscriber
    /// answered badly — a 502, because we are the proxy here.
    testFn: *const fn (ctx: ?*anyopaque, id: SubscriptionId) Error!void,

    pub fn list(self: Subscriptions, arena: Allocator) Error![]const *Subscription {
        return self.listFn(self.ctx, arena);
    }
    pub fn add(self: Subscriptions, cmd: AddSubscriptionCmd) Error!SubscriptionId {
        return self.addFn(self.ctx, cmd);
    }
    pub fn update(self: Subscriptions, id: SubscriptionId, cmd: UpdateSubscriptionCmd) Error!void {
        return self.updateFn(self.ctx, id, cmd);
    }
    pub fn remove(self: Subscriptions, id: SubscriptionId) Error!void {
        return self.removeFn(self.ctx, id);
    }
    pub fn setEnabled(self: Subscriptions, id: SubscriptionId, enabled: bool) Error!void {
        return self.setEnabledFn(self.ctx, id, enabled);
    }
    pub fn sendTest(self: Subscriptions, id: SubscriptionId) Error!void {
        return self.testFn(self.ctx, id);
    }
};

// ---------------------------------------------------------------------
// Runtime configuration
// ---------------------------------------------------------------------

/// The runtime-mutable settings, read fresh on every request.
///
/// Reading through a port rather than from a snapshot is what keeps the
/// session cookie's `Path` and the General panel correct after the
/// operator changes the URL base from the UI — the Go `URLBaseReader`
/// existed for exactly that.
pub const RuntimeConfig = struct {
    ctx: ?*anyopaque = null,

    urlBaseFn: *const fn (ctx: ?*anyopaque) []const u8,
    apiKeyFn: *const fn (ctx: ?*anyopaque) []const u8,
    maxConcurrentJobsFn: *const fn (ctx: ?*anyopaque) i32,
    failHopelessRatioFn: *const fn (ctx: ?*anyopaque) f64,
    deferRecoveryVolsFn: *const fn (ctx: ?*anyopaque) bool,
    deleteSamplesFn: *const fn (ctx: ?*anyopaque) bool,
    collapseSingleFolderFn: *const fn (ctx: ?*anyopaque) bool,

    /// The mutation half. Null means the config is read-only, which the
    /// Go handler expressed as a failed type assertion to
    /// `URLBaseWriter` and answered with 503.
    writer: ?Writer = null,

    /// Static values that are only editable in `config.toml`, so they
    /// need no accessor.
    listen: []const u8 = "",
    log_level: []const u8 = "",
    /// e.g. "http://hoardarr:8085/sabnzbd/api"
    sab_base: []const u8 = "",
    data_dir: []const u8 = "",
    incomplete_dir: []const u8 = "",
    complete_dir: []const u8 = "",

    pub const Writer = struct {
        ctx: ?*anyopaque = null,
        setUrlBaseFn: *const fn (ctx: ?*anyopaque, v: []const u8) Error!void,
        setMaxConcurrentJobsFn: *const fn (ctx: ?*anyopaque, v: i32) Error!void,
        setFailHopelessRatioFn: *const fn (ctx: ?*anyopaque, v: f64) Error!void,
        setDeferRecoveryVolsFn: *const fn (ctx: ?*anyopaque, v: bool) Error!void,
        setDeleteSamplesFn: *const fn (ctx: ?*anyopaque, v: bool) Error!void,
        setCollapseSingleFolderFn: *const fn (ctx: ?*anyopaque, v: bool) Error!void,
        /// Mints a new API key and returns it. The caller shows it once;
        /// the old key stops working immediately.
        rotateApiKeyFn: *const fn (ctx: ?*anyopaque, arena: Allocator) Error![]const u8,
    };

    pub fn urlBase(self: RuntimeConfig) []const u8 {
        return self.urlBaseFn(self.ctx);
    }
    pub fn apiKey(self: RuntimeConfig) []const u8 {
        return self.apiKeyFn(self.ctx);
    }
    pub fn maxConcurrentJobs(self: RuntimeConfig) i32 {
        return self.maxConcurrentJobsFn(self.ctx);
    }
    pub fn failHopelessRatio(self: RuntimeConfig) f64 {
        return self.failHopelessRatioFn(self.ctx);
    }
    pub fn deferRecoveryVols(self: RuntimeConfig) bool {
        return self.deferRecoveryVolsFn(self.ctx);
    }
    pub fn deleteSamples(self: RuntimeConfig) bool {
        return self.deleteSamplesFn(self.ctx);
    }
    pub fn collapseSingleFolder(self: RuntimeConfig) bool {
        return self.collapseSingleFolderFn(self.ctx);
    }

    /// Path for the session cookie: always trailing-slash terminated so
    /// the browser sends it for every URL under the mount point.
    pub fn sessionCookiePath(self: RuntimeConfig, out: []u8) []const u8 {
        const base = self.urlBase();
        if (base.len == 0 or base.len + 1 > out.len) return "/";
        @memcpy(out[0..base.len], base);
        out[base.len] = '/';
        return out[0 .. base.len + 1];
    }
};

/// Runtime control of the global download cap. The token bucket
/// reconfigures in place, so this is safe mid-download.
pub const Bandwidth = struct {
    ctx: ?*anyopaque = null,
    globalCapFn: *const fn (ctx: ?*anyopaque) i64,
    setGlobalCapFn: *const fn (ctx: ?*anyopaque, bytes_per_sec: i64) void,

    pub fn globalCap(self: Bandwidth) i64 {
        return self.globalCapFn(self.ctx);
    }
    pub fn setGlobalCap(self: Bandwidth, bytes_per_sec: i64) void {
        self.setGlobalCapFn(self.ctx, bytes_per_sec);
    }
};

// ---------------------------------------------------------------------
// Auth
// ---------------------------------------------------------------------

/// The identity a credential resolved to.
pub const Identity = struct {
    user_id: auth_domain.UserId,
    username: []const u8,
    role: []const u8,
};

/// The slice of the auth service REST needs. Password hashing and
/// session minting live in `domain/auth.zig`; this port is the
/// transaction boundary around them.
pub const Auth = struct {
    ctx: ?*anyopaque = null,

    /// True when no admin exists yet, i.e. first run.
    needsSetupFn: *const fn (ctx: ?*anyopaque) Error!bool,
    /// Creates the first admin. `error.Conflict` once setup has been
    /// done — this endpoint is public, so it must be a one-shot.
    setupAdminFn: *const fn (ctx: ?*anyopaque, username: []const u8, password: []const u8) Error!auth_domain.UserId,
    /// `error.Unauthorized` for bad credentials. Returns the session
    /// token and its absolute expiry in unix ms.
    loginFn: *const fn (ctx: ?*anyopaque, arena: Allocator, username: []const u8, password: []const u8) Error!SessionInfo,
    logoutFn: *const fn (ctx: ?*anyopaque, token: []const u8) void,
    /// Resolve a session token. `error.Unauthorized` for unknown,
    /// expired or revoked.
    authenticateFn: *const fn (ctx: ?*anyopaque, arena: Allocator, token: []const u8) Error!Identity,
    changePasswordFn: *const fn (ctx: ?*anyopaque, user_id: auth_domain.UserId, old: []const u8, new: []const u8) Error!void,

    pub const SessionInfo = struct {
        token: []const u8,
        expires_at_ms: i64,
    };

    pub fn needsSetup(self: Auth) Error!bool {
        return self.needsSetupFn(self.ctx);
    }
    pub fn setupAdmin(self: Auth, username: []const u8, password: []const u8) Error!auth_domain.UserId {
        return self.setupAdminFn(self.ctx, username, password);
    }
    pub fn login(self: Auth, arena: Allocator, username: []const u8, password: []const u8) Error!SessionInfo {
        return self.loginFn(self.ctx, arena, username, password);
    }
    pub fn logout(self: Auth, token: []const u8) void {
        self.logoutFn(self.ctx, token);
    }
    pub fn authenticate(self: Auth, arena: Allocator, token: []const u8) Error!Identity {
        return self.authenticateFn(self.ctx, arena, token);
    }
    pub fn changePassword(self: Auth, user_id: auth_domain.UserId, old: []const u8, new: []const u8) Error!void {
        return self.changePasswordFn(self.ctx, user_id, old, new);
    }
};

/// Keeps a copy of what a fake was handed.
///
/// Almost everything a handler passes to a port is a slice into the
/// request — the connection's read buffer or its body — and both are
/// reused the moment the response goes out. A fake that stored the slice
/// and let a test read it afterwards would be reading whatever the next
/// request wrote there, which is the kind of test that passes locally
/// and fails in CI.
///
/// So a fake with a recorder copies; a fake without one (the unit tests
/// in this file, where the caller owns the memory) borrows. `keep` is
/// the one call site of that decision.
pub const Recorder = struct {
    arena: std.heap.ArenaAllocator,

    pub fn init(gpa: Allocator) Recorder {
        return .{ .arena = std.heap.ArenaAllocator.init(gpa) };
    }

    pub fn deinit(self: *Recorder) void {
        self.arena.deinit();
    }

    pub fn keep(self: *Recorder, s: []const u8) []const u8 {
        return self.arena.allocator().dupe(u8, s) catch "";
    }

    pub fn keepList(self: *Recorder, list: []const []const u8) []const []const u8 {
        const out = self.arena.allocator().alloc([]const u8, list.len) catch return &.{};
        for (list, 0..) |v, i| out[i] = self.keep(v);
        return out;
    }
};

/// `rec.keep(s)` when there is a recorder, `s` otherwise.
fn keep(rec: ?*Recorder, s: []const u8) []const u8 {
    const r = rec orelse return s;
    return r.keep(s);
}

fn keepOpt(rec: ?*Recorder, s: ?[]const u8) ?[]const u8 {
    return keep(rec, s orelse return null);
}

fn keepList(rec: ?*Recorder, list: []const []const u8) []const []const u8 {
    const r = rec orelse return list;
    return r.keepList(list);
}

// =====================================================================
// Fakes
//
// One double per port, each a plain struct the test fills in. They are
// deliberately dumb: scripted returns, recorded calls, an injectable
// failure. Anything cleverer would be a second implementation of the
// thing under test.
// =====================================================================

/// Scripted queue. Jobs are borrowed — the test owns them and their
/// lifetime, which keeps the fake free of aggregate construction.
pub const FakeQueue = struct {
    active: []const *Job = &.{},
    all: []const *Job = &.{},
    history_result: []const *Job = &.{},
    by_id: []const *Job = &.{},

    /// Returned by every method that can fail, when set.
    fail: ?Error = null,
    add_result: AddJobResult = .{ .id = 1 },

    // Recorded calls.
    paused: std.ArrayList(JobId) = .empty,
    resumed: std.ArrayList(JobId) = .empty,
    removed: std.ArrayList(JobId) = .empty,
    reordered: std.ArrayList(JobId) = .empty,
    last_history: ?HistoryQuery = null,
    last_add: ?AddJobRequest = null,
    gpa: Allocator,
    rec: ?*Recorder = null,

    pub fn deinit(self: *FakeQueue) void {
        self.paused.deinit(self.gpa);
        self.resumed.deinit(self.gpa);
        self.removed.deinit(self.gpa);
        self.reordered.deinit(self.gpa);
    }

    pub fn port(self: *FakeQueue) Queue {
        return .{
            .ctx = self,
            .listActiveFn = listActive,
            .listAllFn = listAll,
            .historyFn = history,
            .getFn = get,
            .pauseFn = pause,
            .resumeFn = unpause,
            .removeFn = remove,
            .reorderFn = reorder,
            .addFn = add,
        };
    }

    fn self_(ctx: ?*anyopaque) *FakeQueue {
        return @ptrCast(@alignCast(ctx.?));
    }

    fn listActive(ctx: ?*anyopaque, _: Allocator) Error![]const *Job {
        const s = self_(ctx);
        if (s.fail) |e| return e;
        return s.active;
    }
    fn listAll(ctx: ?*anyopaque, _: Allocator) Error![]const *Job {
        const s = self_(ctx);
        if (s.fail) |e| return e;
        return s.all;
    }
    fn history(ctx: ?*anyopaque, _: Allocator, q: HistoryQuery) Error![]const *Job {
        const s = self_(ctx);
        var copy = q;
        copy.category = keep(s.rec, q.category);
        s.last_history = copy;
        if (s.fail) |e| return e;
        return s.history_result;
    }
    fn get(ctx: ?*anyopaque, _: Allocator, id: JobId) Error!*Job {
        const s = self_(ctx);
        if (s.fail) |e| return e;
        for (s.by_id) |j| {
            if (j.id == id) return j;
        }
        return error.NotFound;
    }
    fn pause(ctx: ?*anyopaque, id: JobId) Error!void {
        const s = self_(ctx);
        if (s.fail) |e| return e;
        try s.paused.append(s.gpa, id);
    }
    fn unpause(ctx: ?*anyopaque, id: JobId) Error!void {
        const s = self_(ctx);
        if (s.fail) |e| return e;
        try s.resumed.append(s.gpa, id);
    }
    fn remove(ctx: ?*anyopaque, id: JobId) Error!void {
        const s = self_(ctx);
        if (s.fail) |e| return e;
        try s.removed.append(s.gpa, id);
    }
    fn reorder(ctx: ?*anyopaque, ids: []const JobId) Error!void {
        const s = self_(ctx);
        if (s.fail) |e| return e;
        try s.reordered.appendSlice(s.gpa, ids);
    }
    fn add(ctx: ?*anyopaque, _: Allocator, req: AddJobRequest) Error!AddJobResult {
        const s = self_(ctx);
        s.last_add = .{
            .nzb = keep(s.rec, req.nzb),
            .name = keep(s.rec, req.name),
            .category = keep(s.rec, req.category),
            .source = keep(s.rec, req.source),
        };
        if (s.fail) |e| return e;
        return s.add_result;
    }
};

pub const FakeEvents = struct {
    envelopes: []const Envelope = &.{},
    fail: ?Error = null,
    last_job: JobId = 0,

    pub fn port(self: *FakeEvents) Events {
        return .{ .ctx = self, .byJobFn = byJob };
    }

    fn byJob(ctx: ?*anyopaque, _: Allocator, id: JobId) Error![]const Envelope {
        const s: *FakeEvents = @ptrCast(@alignCast(ctx.?));
        s.last_job = id;
        if (s.fail) |e| return e;
        return s.envelopes;
    }
};

pub const FakeServers = struct {
    servers: []const *UsenetServer = &.{},
    fail: ?Error = null,
    next_id: ServerId = 1,
    rec: ?*Recorder = null,

    last_add: ?AddServerCmd = null,
    last_update: ?UpdateServerCmd = null,
    removed: ?ServerId = null,
    enabled_calls: [8]struct { id: ServerId, enabled: bool } = undefined,
    enabled_len: usize = 0,

    pub fn port(self: *FakeServers) Servers {
        return .{
            .ctx = self,
            .listFn = list,
            .getFn = get,
            .addFn = add,
            .updateFn = update,
            .removeFn = remove,
            .setEnabledFn = setEnabled,
        };
    }

    fn self_(ctx: ?*anyopaque) *FakeServers {
        return @ptrCast(@alignCast(ctx.?));
    }

    fn list(ctx: ?*anyopaque, _: Allocator) Error![]const *UsenetServer {
        const s = self_(ctx);
        if (s.fail) |e| return e;
        return s.servers;
    }
    fn get(ctx: ?*anyopaque, _: Allocator, id: ServerId) Error!*UsenetServer {
        const s = self_(ctx);
        if (s.fail) |e| return e;
        for (s.servers) |srv| {
            if (srv.id == id) return srv;
        }
        return error.NotFound;
    }
    fn add(ctx: ?*anyopaque, cmd: AddServerCmd) Error!ServerId {
        const s = self_(ctx);
        var copy = cmd;
        copy.name = keep(s.rec, cmd.name);
        copy.host = keep(s.rec, cmd.host);
        copy.username = keep(s.rec, cmd.username);
        copy.password = keep(s.rec, cmd.password);
        copy.billing_mode = keep(s.rec, cmd.billing_mode);
        s.last_add = copy;
        if (s.fail) |e| return e;
        return s.next_id;
    }
    fn update(ctx: ?*anyopaque, cmd: UpdateServerCmd) Error!void {
        const s = self_(ctx);
        var copy = cmd;
        copy.host = keepOpt(s.rec, cmd.host);
        copy.username = keepOpt(s.rec, cmd.username);
        copy.password = keepOpt(s.rec, cmd.password);
        copy.billing_mode = keepOpt(s.rec, cmd.billing_mode);
        s.last_update = copy;
        if (s.fail) |e| return e;
    }
    fn remove(ctx: ?*anyopaque, id: ServerId) Error!void {
        const s = self_(ctx);
        s.removed = id;
        if (s.fail) |e| return e;
    }
    fn setEnabled(ctx: ?*anyopaque, id: ServerId, enabled: bool) Error!void {
        const s = self_(ctx);
        if (s.enabled_len < s.enabled_calls.len) {
            s.enabled_calls[s.enabled_len] = .{ .id = id, .enabled = enabled };
            s.enabled_len += 1;
        }
        if (s.fail) |e| return e;
    }
};

pub const FakeProbe = struct {
    result: ProbeResult = .{ .ok = true, .dial = true, .greeted = true, .elapsed_ms = 12 },
    last: ?ProbeParams = null,
    rec: ?*Recorder = null,

    pub fn port(self: *FakeProbe) ServerProbe {
        return .{ .ctx = self, .probeFn = probe };
    }

    fn probe(ctx: ?*anyopaque, _: Allocator, p: ProbeParams) Allocator.Error!ProbeResult {
        const s: *FakeProbe = @ptrCast(@alignCast(ctx.?));
        s.last = .{
            .host = keep(s.rec, p.host),
            .port = p.port,
            .tls = p.tls,
            .username = keep(s.rec, p.username),
            .password = keep(s.rec, p.password),
        };
        return s.result;
    }
};

pub const FakeCategories = struct {
    categories: []const Category = &.{},
    fail: ?Error = null,
    saved: ?Category = null,
    deleted: ?[]const u8 = null,
    rec: ?*Recorder = null,

    pub fn port(self: *FakeCategories) Categories {
        return .{ .ctx = self, .listFn = list, .saveFn = save, .deleteFn = delete };
    }

    fn self_(ctx: ?*anyopaque) *FakeCategories {
        return @ptrCast(@alignCast(ctx.?));
    }
    fn list(ctx: ?*anyopaque, _: Allocator) Error![]const Category {
        const s = self_(ctx);
        if (s.fail) |e| return e;
        return s.categories;
    }
    fn save(ctx: ?*anyopaque, c: Category) Error!void {
        const s = self_(ctx);
        s.saved = .{ .name = keep(s.rec, c.name), .dir = keep(s.rec, c.dir), .priority = c.priority };
        if (s.fail) |e| return e;
    }
    fn delete(ctx: ?*anyopaque, name: []const u8) Error!void {
        const s = self_(ctx);
        s.deleted = keep(s.rec, name);
        if (s.fail) |e| return e;
    }
};

pub const FakeSystem = struct {
    status_result: SystemStatus = .{},
    throughput_result: ?ThroughputSample = null,
    history_result: SpeedHistory = .{},
    fail: ?Error = null,
    last_span_seconds: i32 = 0,

    pub fn port(self: *FakeSystem) System {
        return .{ .ctx = self, .statusFn = status, .throughputFn = throughput, .historyFn = history };
    }

    fn self_(ctx: ?*anyopaque) *FakeSystem {
        return @ptrCast(@alignCast(ctx.?));
    }
    fn status(ctx: ?*anyopaque, _: Allocator) Error!SystemStatus {
        const s = self_(ctx);
        if (s.fail) |e| return e;
        return s.status_result;
    }
    fn throughput(ctx: ?*anyopaque, _: Allocator, _: i32) Error!?ThroughputSample {
        const s = self_(ctx);
        if (s.fail) |e| return e;
        return s.throughput_result;
    }
    fn history(ctx: ?*anyopaque, _: Allocator, span: i32) Error!SpeedHistory {
        const s = self_(ctx);
        s.last_span_seconds = span;
        if (s.fail) |e| return e;
        return s.history_result;
    }
};

pub const FakeHealth = struct {
    snapshot_result: HealthSnapshot = .{},
    fail: ?Error = null,
    refreshes: usize = 0,

    pub fn port(self: *FakeHealth) Health {
        return .{ .ctx = self, .snapshotFn = snapshot, .refreshFn = refresh };
    }

    fn snapshot(ctx: ?*anyopaque, _: Allocator) Error!HealthSnapshot {
        const s: *FakeHealth = @ptrCast(@alignCast(ctx.?));
        if (s.fail) |e| return e;
        return s.snapshot_result;
    }
    fn refresh(ctx: ?*anyopaque) void {
        const s: *FakeHealth = @ptrCast(@alignCast(ctx.?));
        s.refreshes += 1;
    }
};

pub const FakeSchedule = struct {
    tasks: []const *Task = &.{},
    fail: ?Error = null,
    ran: ?TaskId = null,

    pub fn port(self: *FakeSchedule) Schedule {
        return .{ .ctx = self, .listFn = list, .byIdFn = byId, .runNowFn = runNow };
    }

    fn self_(ctx: ?*anyopaque) *FakeSchedule {
        return @ptrCast(@alignCast(ctx.?));
    }
    fn list(ctx: ?*anyopaque, _: Allocator) Error![]const *Task {
        const s = self_(ctx);
        if (s.fail) |e| return e;
        return s.tasks;
    }
    fn byId(ctx: ?*anyopaque, _: Allocator, id: TaskId) Error!*Task {
        const s = self_(ctx);
        if (s.fail) |e| return e;
        for (s.tasks) |t| {
            if (t.id == id) return t;
        }
        return error.NotFound;
    }
    fn runNow(ctx: ?*anyopaque, id: TaskId) Error!void {
        const s = self_(ctx);
        s.ran = id;
        if (s.fail) |e| return e;
        for (s.tasks) |t| {
            if (t.id == id) return;
        }
        return error.NotFound;
    }
};

pub const FakeCommands = struct {
    commands: []const *Command = &.{},
    name_list: []const []const u8 = &.{},
    fail: ?Error = null,
    submit_id: CommandId = 1,
    last_submit_name: []const u8 = "",
    last_submit_body: []const u8 = "",
    last_limit: i32 = 0,
    rec: ?*Recorder = null,

    pub fn port(self: *FakeCommands) Commands {
        return .{ .ctx = self, .submitFn = submit, .listFn = list, .byIdFn = byId, .namesFn = names };
    }

    fn self_(ctx: ?*anyopaque) *FakeCommands {
        return @ptrCast(@alignCast(ctx.?));
    }
    fn submit(ctx: ?*anyopaque, name: []const u8, body: []const u8) Error!CommandId {
        const s = self_(ctx);
        s.last_submit_name = keep(s.rec, name);
        s.last_submit_body = keep(s.rec, body);
        if (s.fail) |e| return e;
        return s.submit_id;
    }
    fn list(ctx: ?*anyopaque, _: Allocator, limit: i32) Error![]const *Command {
        const s = self_(ctx);
        s.last_limit = limit;
        if (s.fail) |e| return e;
        return s.commands;
    }
    fn byId(ctx: ?*anyopaque, _: Allocator, id: CommandId) Error!*Command {
        const s = self_(ctx);
        if (s.fail) |e| return e;
        for (s.commands) |c| {
            if (c.id == id) return c;
        }
        return error.NotFound;
    }
    fn names(ctx: ?*anyopaque, _: Allocator) Error![]const []const u8 {
        const s = self_(ctx);
        if (s.fail) |e| return e;
        return s.name_list;
    }
};

pub const FakeBackups = struct {
    files: []const FileInfo = &.{},
    contents: []const u8 = "sqlite-bytes",
    fail: ?Error = null,
    read_fail: ?Error = null,
    runs: usize = 0,
    last_read: []const u8 = "",
    rec: ?*Recorder = null,

    pub fn port(self: *FakeBackups) Backups {
        return .{ .ctx = self, .listFn = list, .runFn = run, .readFn = read };
    }

    fn self_(ctx: ?*anyopaque) *FakeBackups {
        return @ptrCast(@alignCast(ctx.?));
    }
    fn list(ctx: ?*anyopaque, _: Allocator) Error![]const FileInfo {
        const s = self_(ctx);
        if (s.fail) |e| return e;
        return s.files;
    }
    fn run(ctx: ?*anyopaque) Error!void {
        const s = self_(ctx);
        s.runs += 1;
        if (s.fail) |e| return e;
    }
    fn read(ctx: ?*anyopaque, _: Allocator, name: []const u8) Error![]const u8 {
        const s = self_(ctx);
        s.last_read = keep(s.rec, name);
        if (s.read_fail) |e| return e;
        return s.contents;
    }
};

pub const FakeLogFiles = struct {
    files: []const FileInfo = &.{},
    contents: []const u8 = "log text\n",
    fail: ?Error = null,
    read_fail: ?Error = null,
    last_read: []const u8 = "",
    rec: ?*Recorder = null,

    pub fn port(self: *FakeLogFiles) LogFiles {
        return .{ .ctx = self, .listFn = list, .readFn = read };
    }

    fn self_(ctx: ?*anyopaque) *FakeLogFiles {
        return @ptrCast(@alignCast(ctx.?));
    }
    fn list(ctx: ?*anyopaque, _: Allocator) Error![]const FileInfo {
        const s = self_(ctx);
        if (s.fail) |e| return e;
        return s.files;
    }
    fn read(ctx: ?*anyopaque, _: Allocator, name: []const u8) Error![]const u8 {
        const s = self_(ctx);
        s.last_read = keep(s.rec, name);
        if (s.read_fail) |e| return e;
        return s.contents;
    }
};

pub const FakeDiskSpace = struct {
    entries: []const DiskEntry = &.{},
    fail: ?Error = null,

    pub fn port(self: *FakeDiskSpace) DiskSpace {
        return .{ .ctx = self, .snapshotFn = snapshot };
    }

    fn snapshot(ctx: ?*anyopaque, _: Allocator) Error![]const DiskEntry {
        const s: *FakeDiskSpace = @ptrCast(@alignCast(ctx.?));
        if (s.fail) |e| return e;
        return s.entries;
    }
};

pub const FakeSubscriptions = struct {
    subscriptions: []const *Subscription = &.{},
    fail: ?Error = null,
    test_fail: ?Error = null,
    next_id: SubscriptionId = 1,
    rec: ?*Recorder = null,

    last_add: ?AddSubscriptionCmd = null,
    last_update: ?UpdateSubscriptionCmd = null,
    last_update_id: SubscriptionId = 0,
    removed: ?SubscriptionId = null,
    tested: ?SubscriptionId = null,
    last_enabled: ?bool = null,

    pub fn port(self: *FakeSubscriptions) Subscriptions {
        return .{
            .ctx = self,
            .listFn = list,
            .addFn = add,
            .updateFn = update,
            .removeFn = remove,
            .setEnabledFn = setEnabled,
            .testFn = sendTest,
        };
    }

    fn self_(ctx: ?*anyopaque) *FakeSubscriptions {
        return @ptrCast(@alignCast(ctx.?));
    }
    fn list(ctx: ?*anyopaque, _: Allocator) Error![]const *Subscription {
        const s = self_(ctx);
        if (s.fail) |e| return e;
        return s.subscriptions;
    }
    fn add(ctx: ?*anyopaque, cmd: AddSubscriptionCmd) Error!SubscriptionId {
        const s = self_(ctx);
        s.last_add = .{
            .name = keep(s.rec, cmd.name),
            .kind = keep(s.rec, cmd.kind),
            .url = keep(s.rec, cmd.url),
            .topics = keepList(s.rec, cmd.topics),
            .secret = keep(s.rec, cmd.secret),
        };
        if (s.fail) |e| return e;
        return s.next_id;
    }
    fn update(ctx: ?*anyopaque, id: SubscriptionId, cmd: UpdateSubscriptionCmd) Error!void {
        const s = self_(ctx);
        s.last_update_id = id;
        var copy = cmd;
        copy.url = keepOpt(s.rec, cmd.url);
        copy.secret = keepOpt(s.rec, cmd.secret);
        if (cmd.topics) |t| copy.topics = keepList(s.rec, t);
        s.last_update = copy;
        if (s.fail) |e| return e;
    }
    fn remove(ctx: ?*anyopaque, id: SubscriptionId) Error!void {
        const s = self_(ctx);
        s.removed = id;
        if (s.fail) |e| return e;
    }
    fn setEnabled(ctx: ?*anyopaque, id: SubscriptionId, enabled: bool) Error!void {
        const s = self_(ctx);
        s.last_update_id = id;
        s.last_enabled = enabled;
        if (s.fail) |e| return e;
    }
    fn sendTest(ctx: ?*anyopaque, id: SubscriptionId) Error!void {
        const s = self_(ctx);
        s.tested = id;
        if (s.test_fail) |e| return e;
    }
};

/// Mutable runtime config, with the setters recording and validating the
/// way the real one does (so a handler test can assert the 400 path).
pub const FakeRuntimeConfig = struct {
    url_base: []const u8 = "",
    api_key: []const u8 = "testkey0123456789abcdef0123456789ab",
    max_concurrent_jobs: i32 = 2,
    fail_hopeless_ratio: f64 = 0.05,
    defer_recovery_vols: bool = true,
    delete_samples: bool = false,
    collapse_single_folder: bool = true,

    /// When false, `port()` returns a config with no writer — the
    /// read-only case the Go handler answered 503 for.
    writable: bool = true,
    /// Returned by every setter when set.
    set_fail: ?Error = null,
    rotated_key: []const u8 = "rotatedkey0123456789abcdef012345678",
    rotations: usize = 0,

    listen: []const u8 = "0.0.0.0:8085",
    log_level: []const u8 = "info",
    sab_base: []const u8 = "http://hoardarr:8085/sabnzbd/api",
    data_dir: []const u8 = "/data",
    incomplete_dir: []const u8 = "/data/incomplete",
    complete_dir: []const u8 = "/data/complete",

    pub fn port(self: *FakeRuntimeConfig) RuntimeConfig {
        return .{
            .ctx = self,
            .urlBaseFn = urlBase,
            .apiKeyFn = apiKey,
            .maxConcurrentJobsFn = maxConcurrentJobs,
            .failHopelessRatioFn = failHopelessRatio,
            .deferRecoveryVolsFn = deferRecoveryVols,
            .deleteSamplesFn = deleteSamples,
            .collapseSingleFolderFn = collapseSingleFolder,
            .listen = self.listen,
            .log_level = self.log_level,
            .sab_base = self.sab_base,
            .data_dir = self.data_dir,
            .incomplete_dir = self.incomplete_dir,
            .complete_dir = self.complete_dir,
            .writer = if (self.writable) .{
                .ctx = self,
                .setUrlBaseFn = setUrlBase,
                .setMaxConcurrentJobsFn = setMaxConcurrentJobs,
                .setFailHopelessRatioFn = setFailHopelessRatio,
                .setDeferRecoveryVolsFn = setDeferRecoveryVols,
                .setDeleteSamplesFn = setDeleteSamples,
                .setCollapseSingleFolderFn = setCollapseSingleFolder,
                .rotateApiKeyFn = rotateApiKey,
            } else null,
        };
    }

    fn self_(ctx: ?*anyopaque) *FakeRuntimeConfig {
        return @ptrCast(@alignCast(ctx.?));
    }

    fn urlBase(ctx: ?*anyopaque) []const u8 {
        return self_(ctx).url_base;
    }
    fn apiKey(ctx: ?*anyopaque) []const u8 {
        return self_(ctx).api_key;
    }
    fn maxConcurrentJobs(ctx: ?*anyopaque) i32 {
        return self_(ctx).max_concurrent_jobs;
    }
    fn failHopelessRatio(ctx: ?*anyopaque) f64 {
        return self_(ctx).fail_hopeless_ratio;
    }
    fn deferRecoveryVols(ctx: ?*anyopaque) bool {
        return self_(ctx).defer_recovery_vols;
    }
    fn deleteSamples(ctx: ?*anyopaque) bool {
        return self_(ctx).delete_samples;
    }
    fn collapseSingleFolder(ctx: ?*anyopaque) bool {
        return self_(ctx).collapse_single_folder;
    }

    fn setUrlBase(ctx: ?*anyopaque, v: []const u8) Error!void {
        const s = self_(ctx);
        if (s.set_fail) |e| return e;
        // The real one enforces "empty, or a single leading slash and no
        // trailing slash"; a handler test needs that rejection to exist.
        if (v.len > 0 and (v[0] != '/' or v[v.len - 1] == '/')) return error.Invalid;
        s.url_base = v;
    }
    fn setMaxConcurrentJobs(ctx: ?*anyopaque, v: i32) Error!void {
        const s = self_(ctx);
        if (s.set_fail) |e| return e;
        if (v < 1) return error.Invalid;
        s.max_concurrent_jobs = v;
    }
    fn setFailHopelessRatio(ctx: ?*anyopaque, v: f64) Error!void {
        const s = self_(ctx);
        if (s.set_fail) |e| return e;
        if (!(v >= 0 and v <= 1)) return error.Invalid;
        s.fail_hopeless_ratio = v;
    }
    fn setDeferRecoveryVols(ctx: ?*anyopaque, v: bool) Error!void {
        const s = self_(ctx);
        if (s.set_fail) |e| return e;
        s.defer_recovery_vols = v;
    }
    fn setDeleteSamples(ctx: ?*anyopaque, v: bool) Error!void {
        const s = self_(ctx);
        if (s.set_fail) |e| return e;
        s.delete_samples = v;
    }
    fn setCollapseSingleFolder(ctx: ?*anyopaque, v: bool) Error!void {
        const s = self_(ctx);
        if (s.set_fail) |e| return e;
        s.collapse_single_folder = v;
    }
    fn rotateApiKey(ctx: ?*anyopaque, _: Allocator) Error![]const u8 {
        const s = self_(ctx);
        if (s.set_fail) |e| return e;
        s.rotations += 1;
        s.api_key = s.rotated_key;
        return s.rotated_key;
    }
};

pub const FakeBandwidth = struct {
    cap: i64 = 0,
    sets: usize = 0,

    pub fn port(self: *FakeBandwidth) Bandwidth {
        return .{ .ctx = self, .globalCapFn = globalCap, .setGlobalCapFn = setGlobalCap };
    }

    fn globalCap(ctx: ?*anyopaque) i64 {
        const s: *FakeBandwidth = @ptrCast(@alignCast(ctx.?));
        return s.cap;
    }
    fn setGlobalCap(ctx: ?*anyopaque, v: i64) void {
        const s: *FakeBandwidth = @ptrCast(@alignCast(ctx.?));
        s.cap = v;
        s.sets += 1;
    }
};

/// In-memory auth. Real bcrypt is deliberately *not* used here — at cost
/// 10 a verification is ~100 ms and a handler test that logs in a dozen
/// times would take a second and a half. `domain/auth.zig` owns the
/// hashing and has its own tests for it; what these tests need is the
/// credential *decision*, so the fake compares plaintext.
pub const FakeAuth = struct {
    /// No admin yet: `/auth/whoami` answers `needs_setup`.
    needs_setup: bool = false,
    username: []const u8 = "admin",
    password: []const u8 = "correct horse battery",
    user_id: auth_domain.UserId = 1,
    role: []const u8 = "admin",

    /// The one live session. Empty means nobody is logged in.
    session_token: []const u8 = "",
    session_expires_ms: i64 = 4_000_000_000_000,
    /// Set to make `authenticate` reject the otherwise-valid token, for
    /// the expired-session case.
    session_expired: bool = false,

    minted_token: []const u8 = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
    fail: ?Error = null,

    logins: usize = 0,
    logouts: usize = 0,
    setups: usize = 0,
    password_changes: usize = 0,
    last_new_password: []const u8 = "",
    rec: ?*Recorder = null,

    pub fn port(self: *FakeAuth) Auth {
        return .{
            .ctx = self,
            .needsSetupFn = needsSetup,
            .setupAdminFn = setupAdmin,
            .loginFn = login,
            .logoutFn = logout,
            .authenticateFn = authenticate,
            .changePasswordFn = changePassword,
        };
    }

    fn self_(ctx: ?*anyopaque) *FakeAuth {
        return @ptrCast(@alignCast(ctx.?));
    }

    fn needsSetup(ctx: ?*anyopaque) Error!bool {
        const s = self_(ctx);
        if (s.fail) |e| return e;
        return s.needs_setup;
    }

    fn setupAdmin(ctx: ?*anyopaque, username: []const u8, password: []const u8) Error!auth_domain.UserId {
        const s = self_(ctx);
        if (s.fail) |e| return e;
        if (!s.needs_setup) return error.Conflict;
        s.setups += 1;
        s.needs_setup = false;
        s.username = keep(s.rec, username);
        s.password = keep(s.rec, password);
        return s.user_id;
    }

    fn login(ctx: ?*anyopaque, _: Allocator, username: []const u8, password: []const u8) Error!Auth.SessionInfo {
        const s = self_(ctx);
        if (s.fail) |e| return e;
        s.logins += 1;
        if (!std.mem.eql(u8, username, s.username) or !std.mem.eql(u8, password, s.password)) {
            return error.Unauthorized;
        }
        s.session_token = s.minted_token;
        s.session_expired = false;
        return .{ .token = s.minted_token, .expires_at_ms = s.session_expires_ms };
    }

    fn logout(ctx: ?*anyopaque, token: []const u8) void {
        const s = self_(ctx);
        s.logouts += 1;
        if (std.mem.eql(u8, token, s.session_token)) s.session_token = "";
    }

    fn authenticate(ctx: ?*anyopaque, _: Allocator, token: []const u8) Error!Identity {
        const s = self_(ctx);
        if (s.fail) |e| return e;
        if (s.session_token.len == 0) return error.Unauthorized;
        if (s.session_expired) return error.Unauthorized;
        // Constant-time is the real implementation's job; equality is
        // what this fake is asserting about.
        if (!std.mem.eql(u8, token, s.session_token)) return error.Unauthorized;
        return .{ .user_id = s.user_id, .username = s.username, .role = s.role };
    }

    fn changePassword(ctx: ?*anyopaque, user_id: auth_domain.UserId, old: []const u8, new: []const u8) Error!void {
        const s = self_(ctx);
        if (s.fail) |e| return e;
        if (user_id != s.user_id) return error.NotFound;
        if (!std.mem.eql(u8, old, s.password)) return error.Unauthorized;
        if (new.len < 8) return error.Invalid;
        s.password = keep(s.rec, new);
        s.last_new_password = s.password;
        s.password_changes += 1;
    }
};

// ---------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------

const testing = std.testing;

test "every error has a message that leaks nothing" {
    const all = [_]Error{
        error.NotFound,    error.Conflict,    error.Invalid,  error.Unauthorized,
        error.Forbidden,   error.Upstream,    error.Canceled, error.Internal,
        error.Unavailable, error.OutOfMemory,
    };
    for (all) |e| {
        const m = message(e);
        try testing.expect(m.len > 0);
        // No path, no SQL, no quotes to escape.
        try testing.expect(std.mem.indexOfAny(u8, m, "/\"\\\n") == null);
    }
}

test "the session cookie path is always slash-terminated" {
    var cfg: FakeRuntimeConfig = .{};
    var buf: [128]u8 = undefined;

    try testing.expectEqualStrings("/", cfg.port().sessionCookiePath(&buf));
    cfg.url_base = "/hoardarr";
    try testing.expectEqualStrings("/hoardarr/", cfg.port().sessionCookiePath(&buf));
    cfg.url_base = "/deeply/nested/mount";
    try testing.expectEqualStrings("/deeply/nested/mount/", cfg.port().sessionCookiePath(&buf));

    // A base longer than the buffer degrades to "/" rather than
    // truncating into a path that would scope the cookie somewhere else.
    var tiny: [4]u8 = undefined;
    try testing.expectEqualStrings("/", cfg.port().sessionCookiePath(&tiny));
}

test "the runtime config reads through, so a rotation is visible immediately" {
    var cfg: FakeRuntimeConfig = .{ .api_key = "first" };
    const p = cfg.port();
    try testing.expectEqualStrings("first", p.apiKey());
    _ = try p.writer.?.rotateApiKeyFn(p.writer.?.ctx, testing.allocator);
    // Same port value, new key: the handler holds the port, not a copy
    // of the key.
    try testing.expectEqualStrings(cfg.rotated_key, p.apiKey());
}

test "a read-only runtime config has no writer" {
    var cfg: FakeRuntimeConfig = .{ .writable = false };
    try testing.expect(cfg.port().writer == null);
}

test "the fake auth enforces the credential decisions the handlers rely on" {
    var auth: FakeAuth = .{};
    const p = auth.port();
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    try testing.expectError(error.Unauthorized, p.authenticate(a, "anything"));
    try testing.expectError(error.Unauthorized, p.login(a, "admin", "wrong"));

    const s = try p.login(a, "admin", "correct horse battery");
    const who = try p.authenticate(a, s.token);
    try testing.expectEqualStrings("admin", who.username);
    try testing.expectError(error.Unauthorized, p.authenticate(a, "some other token"));

    auth.session_expired = true;
    try testing.expectError(error.Unauthorized, p.authenticate(a, s.token));
    auth.session_expired = false;

    p.logout(s.token);
    try testing.expectError(error.Unauthorized, p.authenticate(a, s.token));
}

test "the fake queue records commands and reports NotFound" {
    var q: FakeQueue = .{ .gpa = testing.allocator };
    defer q.deinit();
    const p = q.port();
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    try p.pause(7);
    try p.unpause(7);
    try p.remove(9);
    try p.reorder(&.{ 3, 1, 2 });
    try testing.expectEqualSlices(JobId, &.{7}, q.paused.items);
    try testing.expectEqualSlices(JobId, &.{7}, q.resumed.items);
    try testing.expectEqualSlices(JobId, &.{9}, q.removed.items);
    try testing.expectEqualSlices(JobId, &.{ 3, 1, 2 }, q.reordered.items);
    try testing.expectError(error.NotFound, p.get(arena.allocator(), 1));

    q.fail = error.Canceled;
    try testing.expectError(error.Canceled, p.pause(7));
}
