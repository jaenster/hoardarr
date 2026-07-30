//! The REST layer's ports, satisfied by the real services and
//! repositories.
//!
//! `api/rest/ports.zig` declares sixteen seams and a `Fake*` for each;
//! this file is the other implementation of each one. The handlers see
//! nothing but the vtables, which is what lets the whole REST test suite
//! run without a database — and what lets this file be the only place
//! that knows a `Job` comes from SQLite.
//!
//! ## Read paths allocate from the request arena
//!
//! Every list-shaped port is handed the arena the handler resets after
//! the response goes out, and every repository read is given that same
//! arena. The aggregates it returns therefore own strings that die with
//! the arena, so nothing here has a `deinit` to forget. That is also why
//! the ports are allowed to return borrowed pointers into aggregates the
//! caller never frees.
//!
//! ## Errors
//!
//! `ports.Error` is about meaning, not mechanism: `NotFound` for a
//! missing row, `Conflict` for a name that is taken, `Internal` for
//! anything the operator has to look at. The mapping lives here because
//! only this layer knows what a `error.Constraint` from SQLite meant.

const std = @import("std");

const build_info = @import("build_info");
const log = @import("../core/log.zig");
const config_mod = @import("../core/config.zig");
const reactor = @import("../posix/reactor.zig");

const ports = @import("../api/rest/ports.zig");

const app_ports = @import("../app/ports.zig");
const dl_ports = @import("../app/download/ports.zig");
const queue_svc = @import("../app/download/queue.zig");
const add_job_svc = @import("../app/download/add_job.zig");
const bandwidth = @import("../app/download/bandwidth.zig");
const system_svc = @import("../app/system/service.zig");
const command_svc = @import("../app/command/service.zig");
const auth_svc = @import("../app/auth.zig");

const dserver = @import("../domain/server.zig");
const dnotify = @import("../domain/notify.zig");
const dschedule = @import("../domain/schedule.zig");
const dcommand = @import("../domain/command.zig");
const dauth = @import("../domain/auth.zig");
const djob = @import("../domain/download/job.zig");
const dl_domain_ports = @import("../domain/download/ports.zig");

const sqlite = @import("../store/sqlite.zig");
const migrate = @import("../store/migrate.zig");
const outbox = @import("../store/outbox.zig");
const repo_download = @import("../store/repo_download.zig");
const repo_server = @import("../store/repo_server.zig");
const repo_category = @import("../store/repo_category.zig");
const repo_schedule = @import("../store/repo_schedule.zig");
const repo_command = @import("../store/repo_command.zig");
const repo_subscription = @import("../store/repo_subscription.zig");
const repo_settings = @import("../store/repo_settings.zig");
const repo_speed_history = @import("../store/repo_speed_history.zig");

const infra = @import("infra.zig");
const settings = @import("settings.zig");

const Allocator = std.mem.Allocator;
const Conn = sqlite.Conn;

fn logBackend(op: []const u8, err: anyerror) void {
    log.default.err("rest port failed", &.{
        log.str("op", op),
        log.str("error", @errorName(err)),
    });
}

/// The one mapping from a backend failure to an HTTP meaning.
fn mapErr(op: []const u8, e: anyerror) ports.Error {
    return switch (e) {
        error.OutOfMemory => error.OutOfMemory,
        error.JobNotFound,
        error.ServerNotFound,
        error.TaskNotFound,
        error.CommandNotFound,
        error.SubscriptionNotFound,
        error.UserNotFound,
        error.SessionNotFound,
        error.NotFound,
        error.NoRows,
        => error.NotFound,
        error.DuplicateName,
        error.DuplicateNzbHash,
        error.UsernameTaken,
        error.SetupAlreadyDone,
        error.Constraint,
        => error.Conflict,
        error.Reserved => error.Forbidden,
        error.InvalidCredentials, error.SessionExpired => error.Unauthorized,
        error.NameEmpty,
        error.NameTooLong,
        error.NameNotTrimmed,
        error.NameInvalidChar,
        error.DirNotRelative,
        error.DirTraversal,
        error.NameRequired,
        error.HostRequired,
        error.PortOutOfRange,
        error.MaxConnsOutOfRange,
        error.NegativeQuota,
        error.NegativeBandwidth,
        error.UrlRequired,
        error.PasswordTooShort,
        error.InvalidUser,
        error.NzbBodyRequired,
        error.ParseFailed,
        error.NoUsableFiles,
        error.InvalidJob,
        => error.Invalid,
        else => {
            logBackend(op, e);
            return error.Internal;
        },
    };
}

// =====================================================================
// Queue
// =====================================================================

/// Reads go straight to `JobRepo`; commands go through the queue service
/// so they publish their events in the same transaction.
///
/// The split is not an accident of wiring. A list endpoint under *arr
/// polling pressure must not hydrate aggregates, and the app layer
/// deliberately does not carry the list-shaped queries — see
/// `app/download/ports.zig`'s note on the read model.
pub const Queue = struct {
    gpa: Allocator,
    conn: *Conn,
    commands: *queue_svc.Service,
    adder: *add_job_svc.Service,

    pub fn port(self: *Queue) ports.Queue {
        return .{
            .ctx = @ptrCast(self),
            .listActiveFn = &listActive,
            .listAllFn = &listAll,
            .historyFn = &history,
            .getFn = &get,
            .pauseFn = &pause,
            .resumeFn = &unpause,
            .removeFn = &remove,
            .reorderFn = &reorder,
            .addFn = &add,
        };
    }

    fn self_(ctx: ?*anyopaque) *Queue {
        return @ptrCast(@alignCast(ctx.?));
    }

    fn repo(self: *Queue) repo_download.JobRepo {
        return repo_download.JobRepo.init(self.gpa, self.conn);
    }

    /// Turns an owned `JobList` into the slice of pointers the port
    /// returns. The list's aggregates were built from `arena`, so the
    /// pointers stay valid until the handler resets it and nothing here
    /// owns a free.
    fn toPointers(arena: Allocator, list: repo_download.JobList) ports.Error![]const *ports.Job {
        const owned = list;
        // Only the backing array is `arena`'s to reclaim wholesale; the
        // elements are moved, not copied, so the aggregates stay put.
        const out = try arena.alloc(*ports.Job, owned.items.items.len);
        for (owned.items.items, 0..) |*j, i| out[i] = j;
        return out;
    }

    fn listActive(ctx: ?*anyopaque, arena: Allocator) ports.Error![]const *ports.Job {
        const self = self_(ctx);
        const list = self.repo().active(arena, .bare) catch |e| return mapErr("queue.active", e);
        return toPointers(arena, list);
    }

    fn listAll(ctx: ?*anyopaque, arena: Allocator) ports.Error![]const *ports.Job {
        const self = self_(ctx);
        const list = self.repo().list(arena, .bare) catch |e| return mapErr("queue.list", e);
        return toPointers(arena, list);
    }

    fn history(ctx: ?*anyopaque, arena: Allocator, q: ports.HistoryQuery) ports.Error![]const *ports.Job {
        const self = self_(ctx);
        const list = self.repo().history(arena, .{
            .category = q.category,
            .state = q.state,
            .since = q.since_ms,
            .limit = @intCast(@max(q.limit, 0)),
        }, .bare) catch |e| return mapErr("queue.history", e);
        return toPointers(arena, list);
    }

    fn get(ctx: ?*anyopaque, arena: Allocator, id: ports.JobId) ports.Error!*ports.Job {
        const self = self_(ctx);
        const job = self.repo().byId(arena, id) catch |e| return mapErr("queue.get", e);
        const p = try arena.create(ports.Job);
        p.* = job;
        return p;
    }

    fn pause(ctx: ?*anyopaque, id: ports.JobId) ports.Error!void {
        self_(ctx).commands.pauseJob(id) catch |e| return mapErr("queue.pause", e);
    }

    fn unpause(ctx: ?*anyopaque, id: ports.JobId) ports.Error!void {
        self_(ctx).commands.resumeJob(id) catch |e| return mapErr("queue.resume", e);
    }

    fn remove(ctx: ?*anyopaque, id: ports.JobId) ports.Error!void {
        self_(ctx).commands.removeJob(id) catch |e| return mapErr("queue.remove", e);
    }

    fn reorder(ctx: ?*anyopaque, ids: []const ports.JobId) ports.Error!void {
        self_(ctx).commands.reorder(ids) catch |e| return mapErr("queue.reorder", e);
    }

    /// A duplicate is answered 200 with the winner's handle, not 4xx: an
    /// *arr re-grabbing a release it thinks failed is routine, and a 4xx
    /// there makes it blacklist the release.
    fn add(ctx: ?*anyopaque, arena: Allocator, req: ports.AddJobRequest) ports.Error!ports.AddJobResult {
        const self = self_(ctx);
        const r = self.adder.addJob(.{
            .nzb = req.nzb,
            .name = req.name,
            .category = req.category,
            .source = req.source,
        }) catch |e| return mapErr("queue.add", e);

        var out: ports.AddJobResult = .{ .id = r.id, .duplicate = r.duplicate };
        if (r.duplicate) {
            // Best-effort: the client gets the handle either way, and a
            // read failure here must not turn a successful dedupe into a
            // 500.
            if (self.repo().byIdShallow(arena, r.id)) |job| {
                out.state = job.state.toString();
                out.name = job.name;
            } else |_| {}
        }
        return out;
    }
};

/// The per-job timeline, straight off the outbox.
pub const Events = struct {
    conn: *Conn,
    bus: *outbox.Bus,

    pub fn port(self: *Events) ports.Events {
        return .{ .ctx = @ptrCast(self), .byJobFn = &byJob };
    }

    fn byJob(ctx: ?*anyopaque, arena: Allocator, id: ports.JobId) ports.Error![]const ports.Envelope {
        const self: *Events = @ptrCast(@alignCast(ctx.?));
        const timeline = self.bus.eventsByJob(self.conn, arena, id) catch |e|
            return mapErr("events.byJob", e);
        // Allocated from the arena, so the timeline's own `deinit` would
        // be a no-op at best and a double-free at worst.
        const out = try arena.alloc(ports.Envelope, timeline.items.items.len);
        for (timeline.items.items, 0..) |env, i| {
            out[i] = .{
                .id = .{ .bytes = env.id },
                .topic = env.topic,
                .aggregate_id = env.aggregate_id,
                .occurred_at = env.occurred_at_ms,
                .payload = env.payload,
                .attempts = env.attempts,
            };
        }
        return out;
    }
};

// =====================================================================
// Servers
// =====================================================================

pub const Servers = struct {
    gpa: Allocator,
    conn: *Conn,
    /// Called after a change that the download side has to hear about —
    /// a new server can unpark jobs, a removed one has to lose its pool.
    on_change: ?ChangeHook = null,

    pub const ChangeHook = struct {
        ctx: *anyopaque,
        changedFn: *const fn (ctx: *anyopaque) void,
    };

    pub fn port(self: *Servers) ports.Servers {
        return .{
            .ctx = @ptrCast(self),
            .listFn = &list,
            .getFn = &get,
            .addFn = &add,
            .updateFn = &update,
            .removeFn = &remove,
            .setEnabledFn = &setEnabled,
        };
    }

    fn self_(ctx: ?*anyopaque) *Servers {
        return @ptrCast(@alignCast(ctx.?));
    }

    fn repo(self: *Servers, a: Allocator) repo_server.ServerRepo {
        return repo_server.ServerRepo.init(a, self.conn);
    }

    fn notifyChanged(self: *Servers) void {
        const hook = self.on_change orelse return;
        hook.changedFn(hook.ctx);
    }

    fn list(ctx: ?*anyopaque, arena: Allocator) ports.Error![]const *ports.UsenetServer {
        const self = self_(ctx);
        const l = self.repo(arena).list(arena) catch |e| return mapErr("servers.list", e);
        const out = try arena.alloc(*ports.UsenetServer, l.items.items.len);
        for (l.items.items, 0..) |*s, i| out[i] = s;
        return out;
    }

    fn get(ctx: ?*anyopaque, arena: Allocator, id: ports.ServerId) ports.Error!*ports.UsenetServer {
        const self = self_(ctx);
        const s = self.repo(arena).byId(arena, id) catch |e| return mapErr("servers.get", e);
        const p = try arena.create(ports.UsenetServer);
        p.* = s;
        return p;
    }

    fn add(ctx: ?*anyopaque, cmd: ports.AddServerCmd) ports.Error!ports.ServerId {
        const self = self_(ctx);
        var s = dserver.UsenetServer.init(self.gpa, .{
            .name = cmd.name,
            .host = cmd.host,
            .port = @intCast(cmd.port),
            .tls = cmd.tls,
            .username = cmd.username,
            .password = cmd.password,
            .max_conns = cmd.max_conns,
            .priority = cmd.priority,
            .backup = cmd.backup,
            .billing_mode = dserver.BillingMode.parse(cmd.billing_mode) orelse .flat,
            .quota_bytes = cmd.quota_bytes,
            .bandwidth_bytes_per_sec = cmd.bandwidth_bytes_per_sec,
        }, infra.nowMillis()) catch |e| return mapErr("servers.add", e);
        defer s.deinit();

        self.repo(self.gpa).save(&s) catch |e| return mapErr("servers.add", e);
        self.notifyChanged();
        return s.id;
    }

    fn update(ctx: ?*anyopaque, cmd: ports.UpdateServerCmd) ports.Error!void {
        const self = self_(ctx);
        var s = self.repo(self.gpa).byId(self.gpa, cmd.id) catch |e|
            return mapErr("servers.update", e);
        defer s.deinit();

        s.update(.{
            .host = cmd.host,
            .port = if (cmd.port) |p| @as(i32, @intCast(p)) else null,
            .tls = cmd.tls,
            .username = cmd.username,
            .password = cmd.password,
            .max_conns = cmd.max_conns,
            .priority = cmd.priority,
            .backup = cmd.backup,
            .billing_mode = if (cmd.billing_mode) |m| dserver.BillingMode.parse(m) else null,
            .quota_bytes = cmd.quota_bytes,
            .bandwidth_bytes_per_sec = cmd.bandwidth_bytes_per_sec,
        }, infra.nowMillis()) catch |e| return mapErr("servers.update", e);

        self.repo(self.gpa).save(&s) catch |e| return mapErr("servers.update", e);
        self.notifyChanged();
    }

    fn remove(ctx: ?*anyopaque, id: ports.ServerId) ports.Error!void {
        const self = self_(ctx);
        self.repo(self.gpa).remove(id) catch |e| return mapErr("servers.remove", e);
        self.notifyChanged();
    }

    fn setEnabled(ctx: ?*anyopaque, id: ports.ServerId, enabled: bool) ports.Error!void {
        const self = self_(ctx);
        var s = self.repo(self.gpa).byId(self.gpa, id) catch |e|
            return mapErr("servers.setEnabled", e);
        defer s.deinit();
        try s.setEnabled(enabled, infra.nowMillis());
        self.repo(self.gpa).save(&s) catch |e| return mapErr("servers.setEnabled", e);
        self.notifyChanged();
    }
};

// =====================================================================
// Categories
// =====================================================================

pub const Categories = struct {
    gpa: Allocator,
    conn: *Conn,

    pub fn port(self: *Categories) ports.Categories {
        return .{
            .ctx = @ptrCast(self),
            .listFn = &list,
            .saveFn = &save,
            .deleteFn = &remove,
        };
    }

    fn self_(ctx: ?*anyopaque) *Categories {
        return @ptrCast(@alignCast(ctx.?));
    }

    fn list(ctx: ?*anyopaque, arena: Allocator) ports.Error![]const ports.Category {
        const self = self_(ctx);
        const repo = repo_category.CategoryRepo.init(self.conn);
        const l = repo.list(arena) catch |e| return mapErr("categories.list", e);
        const out = try arena.alloc(ports.Category, l.items.items.len);
        for (l.items.items, 0..) |c, i| {
            out[i] = .{ .name = c.name, .dir = c.dir, .priority = c.priority };
        }
        return out;
    }

    fn save(ctx: ?*anyopaque, c: ports.Category) ports.Error!void {
        const self = self_(ctx);
        const repo = repo_category.CategoryRepo.init(self.conn);
        repo.save(.{ .name = c.name, .dir = c.dir, .priority = c.priority }) catch |e|
            return mapErr("categories.save", e);
    }

    fn remove(ctx: ?*anyopaque, name: []const u8) ports.Error!void {
        const self = self_(ctx);
        const repo = repo_category.CategoryRepo.init(self.conn);
        // `Reserved` becomes 403 and `NotFound` becomes 404; the handler
        // renders those differently and the difference is the point.
        repo.remove(name) catch |e| return mapErr("categories.delete", e);
    }
};

// =====================================================================
// System
// =====================================================================

pub const System = struct {
    gpa: Allocator,
    conn: *Conn,
    svc: *system_svc.Service,
    /// Widest window the in-memory ring can answer. A wider request goes
    /// to the persisted minute buckets instead.
    ring_seconds: i32 = ports.throughput_window_size,

    pub fn port(self: *System) ports.System {
        return .{
            .ctx = @ptrCast(self),
            .statusFn = &status,
            .throughputFn = &throughput,
            .historyFn = &speedHistory,
        };
    }

    fn self_(ctx: ?*anyopaque) *System {
        return @ptrCast(@alignCast(ctx.?));
    }

    fn status(ctx: ?*anyopaque, arena: Allocator) ports.Error!ports.SystemStatus {
        const self = self_(ctx);
        const s = self.svc.status(arena) catch |e| return mapErr("system.status", e);

        const pools = try arena.alloc(ports.PoolStatus, s.pools.len);
        for (s.pools, 0..) |p, i| {
            pools[i] = .{
                .server_id = p.server_id,
                .server_name = p.server_name,
                .max_conns = p.max_conns,
                .in_use = p.in_use,
                .idle = p.idle,
                .enabled = p.enabled,
                .backup = p.backup,
                .billing_mode = if (p.metered) "metered" else "flat",
                .quota_bytes = p.quota_bytes,
                .used_bytes = p.used_bytes,
            };
        }

        return .{
            .service = s.service,
            .version = s.version,
            .commit = s.commit,
            .build_date = s.build_date,
            .runtime_version = @import("builtin").zig_version_string,
            .os = s.os,
            .arch = s.arch,
            .is_docker = isDocker(),
            .database_type = s.database_type,
            .migration_version = @intCast(s.migration_version),
            .started_at_ms = s.started_at,
            .uptime_ms = s.uptime_ms,
            .queue_active = @intCast(s.queue_active),
            .queue_total = @intCast(s.queue_total),
            .pools = pools,
        };
    }

    fn throughput(ctx: ?*anyopaque, arena: Allocator, seconds: i32) ports.Error!?ports.ThroughputSample {
        const self = self_(ctx);
        const want: usize = @intCast(@max(seconds, 1));
        const buf = try arena.alloc(i64, want);
        const s = self.svc.sample(buf);
        return .{
            .window_seconds = @intCast(s.series.len),
            .series = s.series,
            .total_bytes = s.total,
            .current_bytes_per_sec = s.current_bytes_per_sec,
            .avg10s_bytes_per_sec = s.avg10s_bytes_per_sec,
            .avg60s_bytes_per_sec = s.avg60s_bytes_per_sec,
            .window_peak_bytes_per_sec = s.window_peak_bytes_per_sec,
            .all_time_peak_bytes_per_sec = self.svc.throughput.allTimePeak(),
        };
    }

    /// The ring for anything it can cover, the minute buckets beyond it.
    /// Which one answered is reported, because a graph drawn at the wrong
    /// resolution is a graph that lies about its peaks.
    fn speedHistory(ctx: ?*anyopaque, arena: Allocator, span_seconds: i32) ports.Error!ports.SpeedHistory {
        const self = self_(ctx);
        const span = @max(span_seconds, 1);
        const now_s = infra.nowSeconds();

        if (span <= self.ring_seconds) {
            const buf = try arena.alloc(i64, @intCast(span));
            const s = self.svc.sample(buf);
            const samples = try arena.alloc(ports.SpeedSample, s.series.len);
            const first_at = now_s - @as(i64, @intCast(s.series.len));
            for (s.series, 0..) |v, i| {
                samples[i] = .{
                    .at_ms = (first_at + @as(i64, @intCast(i)) + 1) * 1000,
                    .bytes_per_sec = v,
                };
            }
            return .{
                .resolution_seconds = 1,
                .samples = samples,
                .window_peak_bytes_per_sec = s.window_peak_bytes_per_sec,
                .all_time_peak_bytes_per_sec = self.svc.throughput.allTimePeak(),
            };
        }

        const repo = repo_speed_history.SpeedHistoryRepo.init(self.conn);
        const rows = repo.range(arena, now_s - span, now_s) catch |e|
            return mapErr("system.speedHistory", e);
        const samples = try arena.alloc(ports.SpeedSample, rows.items.len);
        var peak: i64 = 0;
        for (rows.items, 0..) |r, i| {
            samples[i] = .{ .at_ms = r.at_seconds * 1000, .bytes_per_sec = r.bytes_per_sec };
            peak = @max(peak, r.bytes_per_sec);
        }
        return .{
            .resolution_seconds = 60,
            .samples = samples,
            .window_peak_bytes_per_sec = peak,
            .all_time_peak_bytes_per_sec = self.svc.throughput.allTimePeak(),
        };
    }
};

/// Whether we are inside a container. The System page shows it, and the
/// answer changes what an operator should check first.
fn isDocker() bool {
    const sys = @import("../posix/sys.zig");
    var buf: [sys.path_max]u8 = undefined;
    const p = sys.pathZ(&buf, "/.dockerenv") catch return false;
    return sys.exists(p);
}

// =====================================================================
// Scheduled tasks
// =====================================================================

pub const Schedule = struct {
    gpa: Allocator,
    conn: *Conn,

    pub fn port(self: *Schedule) ports.Schedule {
        return .{
            .ctx = @ptrCast(self),
            .listFn = &list,
            .byIdFn = &byId,
            .runNowFn = &runNow,
        };
    }

    fn self_(ctx: ?*anyopaque) *Schedule {
        return @ptrCast(@alignCast(ctx.?));
    }

    fn list(ctx: ?*anyopaque, arena: Allocator) ports.Error![]const *ports.Task {
        const self = self_(ctx);
        const repo = repo_schedule.ScheduleRepo.init(arena, self.conn);
        const l = repo.list(arena) catch |e| return mapErr("schedule.list", e);
        const out = try arena.alloc(*ports.Task, l.items.items.len);
        for (l.items.items, 0..) |*t, i| out[i] = t;
        return out;
    }

    fn byId(ctx: ?*anyopaque, arena: Allocator, id: ports.TaskId) ports.Error!*ports.Task {
        const self = self_(ctx);
        const repo = repo_schedule.ScheduleRepo.init(arena, self.conn);
        const t = repo.byId(arena, id) catch |e| return mapErr("schedule.byId", e);
        const p = try arena.create(ports.Task);
        p.* = t;
        return p;
    }

    /// Pulls the next run forward rather than running the task here: a
    /// request must not block on a task, and running it outside the
    /// scheduler would bypass the claim that stops two processes doing it
    /// twice.
    fn runNow(ctx: ?*anyopaque, id: ports.TaskId) ports.Error!void {
        const self = self_(ctx);
        const repo = repo_schedule.ScheduleRepo.init(self.gpa, self.conn);
        var t = repo.byId(self.gpa, id) catch |e| return mapErr("schedule.runNow", e);
        defer t.deinit();
        t.next_run_at = infra.nowMillis();
        t.updated_at = t.next_run_at;
        repo.save(&t) catch |e| return mapErr("schedule.runNow", e);
    }
};

// =====================================================================
// Commands
// =====================================================================

pub const Commands = struct {
    gpa: Allocator,
    conn: *Conn,
    svc: *command_svc.Service,

    pub fn port(self: *Commands) ports.Commands {
        return .{
            .ctx = @ptrCast(self),
            .submitFn = &submit,
            .listFn = &list,
            .byIdFn = &byId,
            .namesFn = &names,
        };
    }

    fn self_(ctx: ?*anyopaque) *Commands {
        return @ptrCast(@alignCast(ctx.?));
    }

    fn submit(ctx: ?*anyopaque, name: []const u8, body: []const u8) ports.Error!ports.CommandId {
        const self = self_(ctx);
        // `NoHandler` is a 400, not a 404: the route exists, the command
        // name the client chose does not.
        return self.svc.submit(name, body, .api) catch |e| switch (e) {
            error.NoHandler, error.InvalidCommand => error.Invalid,
            else => mapErr("commands.submit", e),
        };
    }

    fn list(ctx: ?*anyopaque, arena: Allocator, limit: i32) ports.Error![]const *ports.Command {
        const self = self_(ctx);
        const repo = repo_command.CommandRepo.init(arena, self.conn);
        const l = repo.list(arena, @intCast(@max(limit, 1))) catch |e|
            return mapErr("commands.list", e);
        const out = try arena.alloc(*ports.Command, l.items.items.len);
        for (l.items.items, 0..) |*c, i| out[i] = c;
        return out;
    }

    fn byId(ctx: ?*anyopaque, arena: Allocator, id: ports.CommandId) ports.Error!*ports.Command {
        const self = self_(ctx);
        const repo = repo_command.CommandRepo.init(arena, self.conn);
        const c = repo.byId(arena, id) catch |e| return mapErr("commands.byId", e);
        const p = try arena.create(ports.Command);
        p.* = c;
        return p;
    }

    fn names(ctx: ?*anyopaque, arena: Allocator) ports.Error![]const []const u8 {
        const self = self_(ctx);
        return self.svc.names(arena) catch |e| return mapErr("commands.names", e);
    }
};

// =====================================================================
// Subscriptions
// =====================================================================

pub const Subscriptions = struct {
    gpa: Allocator,
    conn: *Conn,
    /// Sends the test delivery. Absent, `POST /subscriptions/{id}/test`
    /// answers 503 rather than pretending it sent something.
    tester: ?Tester = null,

    pub const Tester = struct {
        ctx: *anyopaque,
        sendFn: *const fn (ctx: *anyopaque, sub: *const dnotify.Subscription) bool,
    };

    pub fn port(self: *Subscriptions) ports.Subscriptions {
        return .{
            .ctx = @ptrCast(self),
            .listFn = &list,
            .addFn = &add,
            .updateFn = &update,
            .removeFn = &remove,
            .setEnabledFn = &setEnabled,
            .testFn = &sendTest,
        };
    }

    fn self_(ctx: ?*anyopaque) *Subscriptions {
        return @ptrCast(@alignCast(ctx.?));
    }

    fn repo(self: *Subscriptions, a: Allocator) repo_subscription.SubscriptionRepo {
        return repo_subscription.SubscriptionRepo.init(a, self.conn);
    }

    fn list(ctx: ?*anyopaque, arena: Allocator) ports.Error![]const *ports.Subscription {
        const self = self_(ctx);
        const l = self.repo(arena).list(arena) catch |e| return mapErr("subs.list", e);
        const out = try arena.alloc(*ports.Subscription, l.items.items.len);
        for (l.items.items, 0..) |*s, i| out[i] = s;
        return out;
    }

    fn add(ctx: ?*anyopaque, cmd: ports.AddSubscriptionCmd) ports.Error!ports.SubscriptionId {
        const self = self_(ctx);
        var s = dnotify.Subscription.init(self.gpa, .{
            .name = cmd.name,
            .kind = dnotify.Kind.parse(cmd.kind) orelse .webhook,
            .url = cmd.url,
            .topics = cmd.topics,
            .secret = cmd.secret,
        }, infra.nowMillis()) catch |e| return mapErr("subs.add", e);
        defer s.deinit();
        self.repo(self.gpa).save(&s) catch |e| return mapErr("subs.add", e);
        return s.id;
    }

    fn update(
        ctx: ?*anyopaque,
        id: ports.SubscriptionId,
        cmd: ports.UpdateSubscriptionCmd,
    ) ports.Error!void {
        const self = self_(ctx);
        var s = self.repo(self.gpa).byId(self.gpa, id) catch |e| return mapErr("subs.update", e);
        defer s.deinit();
        s.update(.{
            .url = cmd.url,
            .topics = cmd.topics,
            .secret = cmd.secret,
            .enabled = cmd.enabled,
        }, infra.nowMillis()) catch |e| return mapErr("subs.update", e);
        self.repo(self.gpa).save(&s) catch |e| return mapErr("subs.update", e);
    }

    fn remove(ctx: ?*anyopaque, id: ports.SubscriptionId) ports.Error!void {
        const self = self_(ctx);
        self.repo(self.gpa).remove(id) catch |e| return mapErr("subs.remove", e);
    }

    fn setEnabled(ctx: ?*anyopaque, id: ports.SubscriptionId, enabled: bool) ports.Error!void {
        const self = self_(ctx);
        var s = self.repo(self.gpa).byId(self.gpa, id) catch |e| return mapErr("subs.setEnabled", e);
        defer s.deinit();
        try s.setEnabled(enabled, infra.nowMillis());
        self.repo(self.gpa).save(&s) catch |e| return mapErr("subs.setEnabled", e);
    }

    fn sendTest(ctx: ?*anyopaque, id: ports.SubscriptionId) ports.Error!void {
        const self = self_(ctx);
        const tester = self.tester orelse return error.Unavailable;
        var s = self.repo(self.gpa).byId(self.gpa, id) catch |e| return mapErr("subs.test", e);
        defer s.deinit();
        // A subscriber that answered badly is a 502: we are the proxy
        // here, and the operator's webhook is the thing that is broken.
        if (!tester.sendFn(tester.ctx, &s)) return error.Upstream;
    }
};

// =====================================================================
// Runtime configuration
// =====================================================================

/// The runtime-mutable settings, read fresh on every request.
///
/// Reads and writes both go through `settings.Runtime`, which is the
/// single owner of the "database value, falling back to config.toml"
/// rule. Reading a snapshot here instead would make the session cookie's
/// `Path` stale the moment the operator changed the URL base.
pub const RuntimeConfig = struct {
    rt: *settings.Runtime,

    pub fn port(self: *RuntimeConfig) ports.RuntimeConfig {
        const cfg = self.rt.file;
        return .{
            .ctx = @ptrCast(self),
            .urlBaseFn = &urlBase,
            .apiKeyFn = &apiKey,
            .maxConcurrentJobsFn = &maxConcurrentJobs,
            .failHopelessRatioFn = &failHopelessRatio,
            .deferRecoveryVolsFn = &deferRecoveryVols,
            .deleteSamplesFn = &deleteSamples,
            .collapseSingleFolderFn = &collapseSingleFolder,
            .writer = .{
                .ctx = @ptrCast(self),
                .setUrlBaseFn = &setUrlBase,
                .setMaxConcurrentJobsFn = &setMaxConcurrentJobs,
                .setFailHopelessRatioFn = &setFailHopelessRatio,
                .setDeferRecoveryVolsFn = &setDeferRecoveryVols,
                .setDeleteSamplesFn = &setDeleteSamples,
                .setCollapseSingleFolderFn = &setCollapseSingleFolder,
                .rotateApiKeyFn = &rotateApiKey,
            },
            .listen = cfg.server.listen,
            .log_level = cfg.server.log_level,
            .sab_base = self.rt.sab_base,
            .data_dir = cfg.server.data_dir,
            .incomplete_dir = cfg.paths.incomplete_dir,
            .complete_dir = cfg.paths.complete_dir,
        };
    }

    fn self_(ctx: ?*anyopaque) *RuntimeConfig {
        return @ptrCast(@alignCast(ctx.?));
    }

    fn urlBase(ctx: ?*anyopaque) []const u8 {
        return self_(ctx).rt.urlBase();
    }
    fn apiKey(ctx: ?*anyopaque) []const u8 {
        return self_(ctx).rt.apiKey();
    }
    fn maxConcurrentJobs(ctx: ?*anyopaque) i32 {
        return @intCast(self_(ctx).rt.maxConcurrentJobs());
    }
    fn failHopelessRatio(ctx: ?*anyopaque) f64 {
        return self_(ctx).rt.failHopelessRatio();
    }
    fn deferRecoveryVols(ctx: ?*anyopaque) bool {
        return self_(ctx).rt.deferRecoveryVols();
    }
    fn deleteSamples(ctx: ?*anyopaque) bool {
        return self_(ctx).rt.deleteSamples();
    }
    fn collapseSingleFolder(ctx: ?*anyopaque) bool {
        return self_(ctx).rt.collapseSingleFolder();
    }

    fn setUrlBase(ctx: ?*anyopaque, v: []const u8) ports.Error!void {
        self_(ctx).rt.setUrlBase(v) catch |e| return mapErr("config.setUrlBase", e);
    }
    fn setMaxConcurrentJobs(ctx: ?*anyopaque, v: i32) ports.Error!void {
        self_(ctx).rt.setInt(settings.keys.max_concurrent_jobs, v) catch |e|
            return mapErr("config.setMaxConcurrentJobs", e);
    }
    fn setFailHopelessRatio(ctx: ?*anyopaque, v: f64) ports.Error!void {
        self_(ctx).rt.setFloat(settings.keys.fail_hopeless_ratio, v) catch |e|
            return mapErr("config.setFailHopelessRatio", e);
    }
    fn setDeferRecoveryVols(ctx: ?*anyopaque, v: bool) ports.Error!void {
        self_(ctx).rt.setBool(settings.keys.defer_recovery_vols, v) catch |e|
            return mapErr("config.setDeferRecoveryVols", e);
    }
    fn setDeleteSamples(ctx: ?*anyopaque, v: bool) ports.Error!void {
        self_(ctx).rt.setBool(settings.keys.delete_samples, v) catch |e|
            return mapErr("config.setDeleteSamples", e);
    }
    fn setCollapseSingleFolder(ctx: ?*anyopaque, v: bool) ports.Error!void {
        self_(ctx).rt.setBool(settings.keys.collapse_single_folder, v) catch |e|
            return mapErr("config.setCollapseSingleFolder", e);
    }
    fn rotateApiKey(ctx: ?*anyopaque, arena: Allocator) ports.Error![]const u8 {
        return self_(ctx).rt.rotateApiKey(arena) catch |e|
            return mapErr("config.rotateApiKey", e);
    }
};

/// The global download cap. The token bucket reconfigures in place, so
/// this is safe to change with downloads in flight.
pub const Bandwidth = struct {
    limiter: *bandwidth.Limiter,
    rt: *settings.Runtime,

    pub fn port(self: *Bandwidth) ports.Bandwidth {
        return .{
            .ctx = @ptrCast(self),
            .globalCapFn = &globalCap,
            .setGlobalCapFn = &setGlobalCap,
        };
    }

    fn self_(ctx: ?*anyopaque) *Bandwidth {
        return @ptrCast(@alignCast(ctx.?));
    }

    fn globalCap(ctx: ?*anyopaque) i64 {
        return self_(ctx).limiter.globalCap();
    }

    fn setGlobalCap(ctx: ?*anyopaque, bytes_per_sec: i64) void {
        const self = self_(ctx);
        self.limiter.setGlobalCap(bytes_per_sec, infra.nowMillis());
        // Persisted as well as applied, or the cap silently reverts at
        // the next restart and the operator blames the limiter.
        self.rt.setInt(settings.keys.bandwidth_global, bytes_per_sec) catch |e| {
            logBackend("bandwidth.persist", e);
        };
    }
};

// =====================================================================
// Auth
// =====================================================================

pub const Auth = struct {
    svc: *auth_svc.Service,

    pub fn port(self: *Auth) ports.Auth {
        return .{
            .ctx = @ptrCast(self),
            .needsSetupFn = &needsSetup,
            .setupAdminFn = &setupAdmin,
            .loginFn = &login,
            .logoutFn = &logout,
            .authenticateFn = &authenticate,
            .changePasswordFn = &changePassword,
        };
    }

    fn self_(ctx: ?*anyopaque) *Auth {
        return @ptrCast(@alignCast(ctx.?));
    }

    fn needsSetup(ctx: ?*anyopaque) ports.Error!bool {
        return self_(ctx).svc.needsSetup() catch |e| return mapErr("auth.needsSetup", e);
    }

    fn setupAdmin(ctx: ?*anyopaque, username: []const u8, password: []const u8) ports.Error!dauth.UserId {
        return self_(ctx).svc.setupAdmin(username, password) catch |e| switch (e) {
            // Setup is a public one-shot; a second attempt is a conflict,
            // never a second admin.
            error.SetupAlreadyDone, error.UsernameTaken => error.Conflict,
            error.PasswordTooShort, error.InvalidUser => error.Invalid,
            else => mapErr("auth.setupAdmin", e),
        };
    }

    fn login(
        ctx: ?*anyopaque,
        arena: Allocator,
        username: []const u8,
        password: []const u8,
    ) ports.Error!ports.Auth.SessionInfo {
        const s = self_(ctx).svc.login(username, password) catch |e| switch (e) {
            error.InvalidCredentials => return error.Unauthorized,
            else => return mapErr("auth.login", e),
        };
        return .{
            .token = try arena.dupe(u8, &s.token),
            .expires_at_ms = s.expires_at,
        };
    }

    fn logout(ctx: ?*anyopaque, token: []const u8) void {
        self_(ctx).svc.logout(token) catch |e| {
            // A logout that could not delete the row is worth a log line
            // and nothing else: the client is throwing the cookie away.
            logBackend("auth.logout", e);
        };
    }

    fn authenticate(ctx: ?*anyopaque, arena: Allocator, token: []const u8) ports.Error!ports.Identity {
        const u = self_(ctx).svc.authenticate(token) catch |e| switch (e) {
            error.SessionNotFound, error.SessionExpired, error.UserNotFound => return error.Unauthorized,
            else => return mapErr("auth.authenticate", e),
        };
        // The service hands back a borrowed aggregate; the identity has to
        // outlive it, so the strings are copied into the request arena.
        return .{
            .user_id = u.id,
            .username = try arena.dupe(u8, u.username),
            .role = try arena.dupe(u8, u.role.toString()),
        };
    }

    fn changePassword(
        ctx: ?*anyopaque,
        user_id: dauth.UserId,
        old: []const u8,
        new: []const u8,
    ) ports.Error!void {
        self_(ctx).svc.changePassword(user_id, old, new) catch |e| switch (e) {
            error.InvalidCredentials => return error.Unauthorized,
            error.PasswordTooShort => return error.Invalid,
            else => return mapErr("auth.changePassword", e),
        };
    }
};

// =====================================================================
// Tests
// =====================================================================

const testing = std.testing;

test "backend failures map onto HTTP meanings, not onto 500 for everything" {
    // The distinctions the UI renders differently.
    try testing.expectEqual(ports.Error.NotFound, mapErr("t", error.JobNotFound));
    try testing.expectEqual(ports.Error.NotFound, mapErr("t", error.ServerNotFound));
    try testing.expectEqual(ports.Error.Conflict, mapErr("t", error.DuplicateName));
    try testing.expectEqual(ports.Error.Forbidden, mapErr("t", error.Reserved));
    try testing.expectEqual(ports.Error.Unauthorized, mapErr("t", error.InvalidCredentials));
    try testing.expectEqual(ports.Error.Invalid, mapErr("t", error.NameTooLong));
    try testing.expectEqual(ports.Error.OutOfMemory, mapErr("t", error.OutOfMemory));
    // Anything unrecognised is a 500 *and* a log line, never a silent 200.
    try testing.expectEqual(ports.Error.Internal, mapErr("t", error.Misuse));
}
