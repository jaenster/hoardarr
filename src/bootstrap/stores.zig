//! Store adapters: the SQLite repositories dressed up as the ports the
//! application layer declares.
//!
//! Every repository in `src/store` returns its aggregate **by value** and
//! speaks its own error set. Every application port wants a **pointer**
//! it can hand back with `release`, and one narrow error set. This file
//! is that translation, and it is the only place it happens.
//!
//! ## Why pointers and `release`
//!
//! An aggregate owns its strings, so the app layer needs a stable address
//! to mutate and a defined moment to free. `byId` therefore heap-copies
//! the repository's by-value result and `release` destroys it. The extra
//! allocation is one per aggregate load, against a query that already
//! allocated every string in it.
//!
//! ## Transactions
//!
//! The `?*Unit` parameter every port carries is ignored here on purpose.
//! A SQLite connection *is* the transaction — `store/tx.zig` opens one on
//! the connection and the repositories write through the same handle — so
//! honouring the unit as well would let the two disagree about which
//! transaction a write joined. The unit still does its job: it is what
//! opened the transaction on this connection in the first place.

const std = @import("std");

const log = @import("../core/log.zig");
const app_ports = @import("../app/ports.zig");
const dl_ports = @import("../app/download/ports.zig");
const app_auth = @import("../app/auth.zig");
const app_schedule = @import("../app/schedule.zig");
const app_command = @import("../app/command/service.zig");

const dauth = @import("../domain/auth.zig");
const dschedule = @import("../domain/schedule.zig");
const dcommand = @import("../domain/command.zig");
const djob = @import("../domain/download/job.zig");

const sqlite = @import("../store/sqlite.zig");
const repo_download = @import("../store/repo_download.zig");
const repo_auth = @import("../store/repo_auth.zig");
const repo_schedule = @import("../store/repo_schedule.zig");
const repo_command = @import("../store/repo_command.zig");

const Allocator = std.mem.Allocator;
const Conn = sqlite.Conn;
const Unit = app_ports.Unit;

/// One line per backend failure, so a 500 in the API has something behind
/// it in the log. The port errors are deliberately vague — see
/// `api/rest/ports.zig` on why a driver message never reaches a client.
fn logBackend(op: []const u8, err: anyerror) void {
    log.default.err("store operation failed", &.{
        log.str("op", op),
        log.str("error", @errorName(err)),
    });
}

// ---------------------------------------------------------------------
// Jobs
// ---------------------------------------------------------------------

/// `app/download/ports.zig`'s `JobStore` over `JobRepo`.
pub const JobStore = struct {
    gpa: Allocator,
    conn: *Conn,

    /// The aggregate adopted by the last successful insert.
    ///
    /// `JobStore.save` adopts a job whose id was 0 — the in-memory fake
    /// keeps it in its list and frees it at `deinit`, and the caller's
    /// `defer if (!adopted)` in `add_job.zig` stops freeing it. A SQLite
    /// adapter has nowhere to keep it, but it cannot free it on the spot
    /// either: the caller reads `job.id` and drains `job.pullEvents()`
    /// immediately afterwards.
    ///
    /// So it is freed one insert late. The caller's use of an adopted
    /// aggregate never outlives the call that adopted it — add-job is the
    /// only inserter and it publishes and returns — so holding exactly
    /// one is enough, and it is bounded at one rather than a list that
    /// grows for the process lifetime.
    adopted: ?*djob.Job = null,

    /// Frees whatever the last insert adopted. Called by the composition
    /// root before the connection closes.
    pub fn deinit(self: *JobStore) void {
        self.releaseAdopted();
    }

    fn releaseAdopted(self: *JobStore) void {
        const prev = self.adopted orelse return;
        self.adopted = null;
        prev.deinit();
        self.gpa.destroy(prev);
    }

    pub fn port(self: *JobStore) dl_ports.JobStore {
        return .{
            .ctx = @ptrCast(self),
            .byIdFn = &byId,
            .byNzbHashFn = &byNzbHash,
            .releaseFn = &release,
            .saveFn = &save,
            .updateCountersFn = &updateCounters,
            .updateSegmentBatchFn = &updateSegmentBatch,
            .deleteFn = &delete,
            .activeFn = &active,
            .countActiveFn = &countActive,
            .countAllFn = &countAll,
        };
    }

    fn self_(ctx: *anyopaque) *JobStore {
        return @ptrCast(@alignCast(ctx));
    }

    fn repo(self: *JobStore) repo_download.JobRepo {
        return repo_download.JobRepo.init(self.gpa, self.conn);
    }

    fn mapErr(op: []const u8, e: anyerror) dl_ports.RepoError {
        return switch (e) {
            error.OutOfMemory => error.OutOfMemory,
            error.JobNotFound => error.JobNotFound,
            // A UNIQUE(nzb_hash) violation is the concurrent-add race the
            // caller turns into "already queued".
            error.Constraint, error.DuplicateNzbHash => error.DuplicateNzbHash,
            else => {
                logBackend(op, e);
                return error.Backend;
            },
        };
    }

    /// Heap-copies the repository's by-value aggregate. `release` is the
    /// matching free, exactly once per successful load.
    fn own(gpa: Allocator, job: djob.Job) dl_ports.RepoError!*djob.Job {
        const p = gpa.create(djob.Job) catch {
            var j = job;
            j.deinit();
            return error.OutOfMemory;
        };
        p.* = job;
        return p;
    }

    fn byId(ctx: *anyopaque, _: ?*Unit, id: dl_ports.JobId) dl_ports.RepoError!*djob.Job {
        const self = self_(ctx);
        const job = self.repo().byId(self.gpa, id) catch |e| return mapErr("job.byId", e);
        return own(self.gpa, job);
    }

    fn byNzbHash(ctx: *anyopaque, _: ?*Unit, hash: []const u8) dl_ports.RepoError!*djob.Job {
        const self = self_(ctx);
        const job = self.repo().byNzbHash(self.gpa, hash) catch |e| return mapErr("job.byNzbHash", e);
        return own(self.gpa, job);
    }

    fn release(ctx: *anyopaque, job: *djob.Job) void {
        const self = self_(ctx);
        job.deinit();
        self.gpa.destroy(job);
    }

    fn save(ctx: *anyopaque, _: ?*Unit, job: *djob.Job) dl_ports.RepoError!void {
        const self = self_(ctx);
        const inserting = job.id == 0;
        self.repo().save(job) catch |e| return mapErr("job.save", e);
        // Ownership transfers only on a *successful* insert; on failure
        // the caller still owns it and frees it itself.
        if (inserting) {
            self.releaseAdopted();
            self.adopted = job;
        }
    }

    fn updateCounters(ctx: *anyopaque, _: ?*Unit, job: *const djob.Job) dl_ports.RepoError!void {
        const self = self_(ctx);
        self.repo().updateCounters(job) catch |e| return mapErr("job.updateCounters", e);
    }

    fn updateSegmentBatch(
        ctx: *anyopaque,
        _: ?*Unit,
        updates: []const dl_ports.SegmentUpdate,
    ) dl_ports.RepoError!void {
        const self = self_(ctx);
        self.repo().updateSegmentBatch(updates) catch |e| return mapErr("job.updateSegmentBatch", e);
    }

    fn delete(ctx: *anyopaque, _: ?*Unit, id: dl_ports.JobId) dl_ports.RepoError!void {
        const self = self_(ctx);
        self.repo().delete(id) catch |e| return mapErr("job.delete", e);
    }

    /// The `bare` depth is the whole point: the orchestrator's sweep wants
    /// ids and states, and hydrating files and segments for it was the
    /// measured CPU cost the `JobSummary` type exists to avoid.
    fn active(ctx: *anyopaque, a: Allocator, _: ?*Unit) dl_ports.RepoError![]dl_ports.JobSummary {
        const self = self_(ctx);
        var list = self.repo().active(self.gpa, .bare) catch |e| return mapErr("job.active", e);
        defer list.deinit();

        const out = try a.alloc(dl_ports.JobSummary, list.items.items.len);
        for (list.items.items, 0..) |*j, i| {
            out[i] = .{ .id = j.id, .state = j.state, .queue_order = j.queue_order };
        }
        return out;
    }

    fn countActive(ctx: *anyopaque, _: ?*Unit) dl_ports.RepoError!u32 {
        const self = self_(ctx);
        const n = self.repo().countActive() catch |e| return mapErr("job.countActive", e);
        return @intCast(@max(n, 0));
    }

    fn countAll(ctx: *anyopaque, _: ?*Unit) dl_ports.RepoError!u32 {
        const self = self_(ctx);
        const n = self.repo().countAll() catch |e| return mapErr("job.countAll", e);
        return @intCast(@max(n, 0));
    }
};

// ---------------------------------------------------------------------
// The four post-download aggregates
// ---------------------------------------------------------------------

/// `app_ports.Repo(A)` over any of the four repositories that share the
/// one-row-per-job shape: verify, repair, extract, deliver.
///
/// Generic because the four are identical apart from their types — the
/// same argument `app_ports.Repo` itself makes. `RepoT` must expose
/// `init(gpa, conn)`, `byJobId(gpa, job_id)` and `save(*A)`, which all
/// four already do.
pub fn AggregateStore(comptime A: type, comptime RepoT: type) type {
    return struct {
        const Self = @This();

        gpa: Allocator,
        conn: *Conn,
        /// Same one-insert-late adoption as `JobStore.adopted`; see the
        /// comment there for why it cannot be freed on the spot.
        adopted: ?*A = null,

        pub fn deinit(self: *Self) void {
            self.releaseAdopted();
        }

        fn releaseAdopted(self: *Self) void {
            const prev = self.adopted orelse return;
            self.adopted = null;
            prev.deinit();
            self.gpa.destroy(prev);
        }

        pub fn port(self: *Self) app_ports.Repo(A) {
            return .{
                .ctx = @ptrCast(self),
                .byJobIdFn = &byJobId,
                .releaseFn = &release,
                .saveFn = &save,
            };
        }

        fn self_(ctx: *anyopaque) *Self {
            return @ptrCast(@alignCast(ctx));
        }

        fn mapErr(e: anyerror) app_ports.AggregateError {
            return switch (e) {
                error.OutOfMemory => error.OutOfMemory,
                error.VerifySetNotFound,
                error.RepairNotFound,
                error.ExtractNotFound,
                error.DeliveryNotFound,
                error.NoRows,
                => error.NotFound,
                else => {
                    logBackend(@typeName(A), e);
                    return error.Backend;
                },
            };
        }

        fn byJobId(ctx: *anyopaque, _: ?*Unit, job_id: i64) app_ports.AggregateError!*A {
            const self = self_(ctx);
            const repo = RepoT.init(self.gpa, self.conn);
            const value = repo.byJobId(self.gpa, job_id) catch |e| return mapErr(e);
            const p = self.gpa.create(A) catch {
                var v = value;
                v.deinit();
                return error.OutOfMemory;
            };
            p.* = value;
            return p;
        }

        fn release(ctx: *anyopaque, a: *A) void {
            const self = self_(ctx);
            a.deinit();
            self.gpa.destroy(a);
        }

        fn save(ctx: *anyopaque, _: ?*Unit, a: *A) app_ports.AggregateError!void {
            const self = self_(ctx);
            const inserting = a.id == 0;
            const repo = RepoT.init(self.gpa, self.conn);
            repo.save(a) catch |e| return mapErr(e);
            if (inserting) {
                self.releaseAdopted();
                self.adopted = a;
            }
        }
    };
}

// ---------------------------------------------------------------------
// Auth
// ---------------------------------------------------------------------

pub const UserStore = struct {
    gpa: Allocator,
    conn: *Conn,

    pub fn port(self: *UserStore) app_auth.UserStore {
        return .{
            .ctx = @ptrCast(self),
            .countFn = &count,
            .byIdFn = &byId,
            .byUsernameFn = &byUsername,
            .releaseFn = &release,
            .saveFn = &save,
        };
    }

    fn self_(ctx: *anyopaque) *UserStore {
        return @ptrCast(@alignCast(ctx));
    }

    fn repo(self: *UserStore) repo_auth.UserRepo {
        return repo_auth.UserRepo.init(self.gpa, self.conn);
    }

    fn mapErr(e: anyerror) app_auth.StoreError {
        return switch (e) {
            error.OutOfMemory => error.OutOfMemory,
            error.UserNotFound, error.NoRows => error.UserNotFound,
            else => {
                logBackend("user", e);
                return error.Backend;
            },
        };
    }

    fn count(ctx: *anyopaque, _: ?*Unit) app_auth.StoreError!u32 {
        const self = self_(ctx);
        const n = self.repo().count() catch |e| return mapErr(e);
        return @intCast(@max(n, 0));
    }

    fn own(self: *UserStore, u: dauth.User) app_auth.StoreError!*dauth.User {
        const p = self.gpa.create(dauth.User) catch {
            var v = u;
            v.deinit();
            return error.OutOfMemory;
        };
        p.* = u;
        return p;
    }

    fn byId(ctx: *anyopaque, _: ?*Unit, id: dauth.UserId) app_auth.StoreError!*dauth.User {
        const self = self_(ctx);
        const u = self.repo().byId(self.gpa, id) catch |e| return mapErr(e);
        return self.own(u);
    }

    fn byUsername(ctx: *anyopaque, _: ?*Unit, username: []const u8) app_auth.StoreError!*dauth.User {
        const self = self_(ctx);
        const u = self.repo().byUsername(self.gpa, username) catch |e| return mapErr(e);
        return self.own(u);
    }

    fn release(ctx: *anyopaque, u: *dauth.User) void {
        const self = self_(ctx);
        u.deinit();
        self.gpa.destroy(u);
    }

    fn save(ctx: *anyopaque, _: ?*Unit, u: *dauth.User) app_auth.StoreError!void {
        const self = self_(ctx);
        self.repo().save(u) catch |e| return mapErr(e);
    }
};

pub const SessionStore = struct {
    conn: *Conn,

    pub fn port(self: *SessionStore) app_auth.SessionStore {
        return .{
            .ctx = @ptrCast(self),
            .getFn = &get,
            .putFn = &put,
            .deleteFn = &remove,
            .touchFn = &touch,
        };
    }

    fn self_(ctx: *anyopaque) *SessionStore {
        return @ptrCast(@alignCast(ctx));
    }

    fn repo(self: *SessionStore) repo_auth.SessionRepo {
        return repo_auth.SessionRepo.init(self.conn);
    }

    fn mapErr(e: anyerror) app_auth.StoreError {
        return switch (e) {
            error.OutOfMemory => error.OutOfMemory,
            error.SessionNotFound, error.NoRows => error.SessionNotFound,
            else => {
                logBackend("session", e);
                return error.Backend;
            },
        };
    }

    fn get(ctx: *anyopaque, token: []const u8) app_auth.StoreError!dauth.Session {
        const self = self_(ctx);
        return self.repo().get(token) catch |e| return mapErr(e);
    }

    fn put(ctx: *anyopaque, _: ?*Unit, s: dauth.Session) app_auth.StoreError!void {
        const self = self_(ctx);
        self.repo().put(s) catch |e| return mapErr(e);
    }

    fn remove(ctx: *anyopaque, _: ?*Unit, token: []const u8) app_auth.StoreError!void {
        const self = self_(ctx);
        self.repo().remove(token) catch |e| return mapErr(e);
    }

    fn touch(ctx: *anyopaque, token: []const u8, now: i64) app_auth.StoreError!void {
        const self = self_(ctx);
        self.repo().touch(token, now) catch |e| return mapErr(e);
    }
};

// ---------------------------------------------------------------------
// Scheduled tasks
// ---------------------------------------------------------------------

pub const ScheduleStore = struct {
    gpa: Allocator,
    conn: *Conn,

    pub fn port(self: *ScheduleStore) app_schedule.Store {
        return .{
            .ctx = @ptrCast(self),
            .byNameFn = &byName,
            .releaseFn = &release,
            .saveFn = &save,
            .claimDueFn = &claimDue,
            .resetStaleClaimsFn = &resetStaleClaims,
        };
    }

    fn self_(ctx: *anyopaque) *ScheduleStore {
        return @ptrCast(@alignCast(ctx));
    }

    fn repo(self: *ScheduleStore) repo_schedule.ScheduleRepo {
        return repo_schedule.ScheduleRepo.init(self.gpa, self.conn);
    }

    fn mapErr(e: anyerror) app_schedule.StoreError {
        return switch (e) {
            error.OutOfMemory => error.OutOfMemory,
            error.TaskNotFound, error.NoRows => error.TaskNotFound,
            error.ClaimLost => error.ClaimLost,
            error.DuplicateName, error.Constraint => error.DuplicateName,
            else => {
                logBackend("schedule", e);
                return error.Backend;
            },
        };
    }

    fn own(self: *ScheduleStore, t: dschedule.Task) app_schedule.StoreError!*dschedule.Task {
        const p = self.gpa.create(dschedule.Task) catch {
            var v = t;
            v.deinit();
            return error.OutOfMemory;
        };
        p.* = t;
        return p;
    }

    fn byName(ctx: *anyopaque, _: ?*Unit, name: []const u8) app_schedule.StoreError!*dschedule.Task {
        const self = self_(ctx);
        const t = self.repo().byName(self.gpa, name) catch |e| return mapErr(e);
        return self.own(t);
    }

    fn release(ctx: *anyopaque, t: *dschedule.Task) void {
        const self = self_(ctx);
        t.deinit();
        self.gpa.destroy(t);
    }

    fn save(ctx: *anyopaque, _: ?*Unit, t: *dschedule.Task) app_schedule.StoreError!void {
        const self = self_(ctx);
        self.repo().save(t) catch |e| return mapErr(e);
    }

    fn claimDue(
        ctx: *anyopaque,
        a: Allocator,
        _: ?*Unit,
        now: i64,
        limit: usize,
    ) app_schedule.StoreError![]*dschedule.Task {
        const self = self_(ctx);
        var list = self.repo().claimDue(self.gpa, now, @intCast(limit)) catch |e| return mapErr(e);
        // The list's backing array is freed here; the tasks themselves are
        // moved out into individual allocations the caller releases.
        defer list.items.deinit(self.gpa);

        const out = a.alloc(*dschedule.Task, list.items.items.len) catch {
            for (list.items.items) |*t| t.deinit();
            return error.OutOfMemory;
        };
        var n: usize = 0;
        errdefer {
            for (out[0..n]) |p| {
                p.deinit();
                self.gpa.destroy(p);
            }
            for (list.items.items[n..]) |*t| t.deinit();
        }
        for (list.items.items) |t| {
            out[n] = try self.own(t);
            n += 1;
        }
        return out;
    }

    fn resetStaleClaims(ctx: *anyopaque, _: ?*Unit, now: i64) app_schedule.StoreError!usize {
        const self = self_(ctx);
        const n = self.repo().resetStaleClaims(now) catch |e| return mapErr(e);
        return @intCast(@max(n, 0));
    }
};

// ---------------------------------------------------------------------
// Commands
// ---------------------------------------------------------------------

pub const CommandStore = struct {
    gpa: Allocator,
    conn: *Conn,

    pub fn port(self: *CommandStore) app_command.Store {
        return .{
            .ctx = @ptrCast(self),
            .saveFn = &save,
            .claimNextFn = &claimNext,
            .releaseFn = &release,
            .resetStaleClaimsFn = &resetStaleClaims,
        };
    }

    fn self_(ctx: *anyopaque) *CommandStore {
        return @ptrCast(@alignCast(ctx));
    }

    fn repo(self: *CommandStore) repo_command.CommandRepo {
        return repo_command.CommandRepo.init(self.gpa, self.conn);
    }

    fn mapErr(e: anyerror) app_command.StoreError {
        return switch (e) {
            error.OutOfMemory => error.OutOfMemory,
            error.CommandNotFound, error.NoRows => error.CommandNotFound,
            else => {
                logBackend("command", e);
                return error.Backend;
            },
        };
    }

    fn save(ctx: *anyopaque, c: *dcommand.Command) app_command.StoreError!void {
        const self = self_(ctx);
        self.repo().save(c) catch |e| return mapErr(e);
    }

    fn claimNext(ctx: *anyopaque, now: i64) app_command.StoreError!?*dcommand.Command {
        const self = self_(ctx);
        const claimed = self.repo().claimNext(self.gpa, now) catch |e| return mapErr(e);
        const value = claimed orelse return null;
        const p = self.gpa.create(dcommand.Command) catch {
            var v = value;
            v.deinit();
            return error.OutOfMemory;
        };
        p.* = value;
        return p;
    }

    fn release(ctx: *anyopaque, c: *dcommand.Command) void {
        const self = self_(ctx);
        c.deinit();
        self.gpa.destroy(c);
    }

    fn resetStaleClaims(ctx: *anyopaque, cutoff: i64) app_command.StoreError!usize {
        const self = self_(ctx);
        const n = self.repo().resetStaleClaims(cutoff) catch |e| return mapErr(e);
        return @intCast(@max(n, 0));
    }
};

// ---------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------

const testing = std.testing;
const migrate = @import("../store/migrate.zig");

fn openMemory() !*Conn {
    const conn = try Conn.open(testing.allocator, ":memory:", .{});
    try migrate.migrate(conn);
    return conn;
}

test "the job store round-trips an aggregate through the port" {
    const conn = try openMemory();
    defer conn.close();

    var adapter: JobStore = .{ .gpa = testing.allocator, .conn = conn };
    defer adapter.deinit();
    const store = adapter.port();

    var job = try djob.Job.init(testing.allocator, .{
        .nzb_hash = "hash-1",
        .name = "Release.Name",
        .category = "tv",
        .source = "test",
        .files = &.{.{
            .filename = "Release.Name.mkv",
            .size_bytes = 1024,
            .segments = &.{.{ .seq_index = 1, .message_id = "<a@b>", .bytes = 1024 }},
        }},
    }, 1_700_000_000_000);
    errdefer job.deinit();
    const owned = try testing.allocator.create(djob.Job);
    owned.* = job;

    try store.save(null, owned);
    try testing.expect(owned.id != 0);
    const id = owned.id;
    // An insert adopts the aggregate: the caller must not free it, and
    // `adapter.deinit` is what eventually does.

    const loaded = try store.byId(null, id);
    defer store.release(loaded);
    try testing.expectEqualStrings("Release.Name", loaded.name);

    try testing.expectEqual(@as(u32, 1), try store.countAll(null));
    try testing.expectEqual(@as(u32, 1), try store.countActive(null));

    const summaries = try store.active(testing.allocator, null);
    defer testing.allocator.free(summaries);
    try testing.expectEqual(@as(usize, 1), summaries.len);
    try testing.expectEqual(id, summaries[0].id);

    try store.delete(null, id);
    try testing.expectError(error.JobNotFound, store.byId(null, id));
}

test "a missing job is JobNotFound, not a backend failure" {
    const conn = try openMemory();
    defer conn.close();

    var adapter: JobStore = .{ .gpa = testing.allocator, .conn = conn };
    const store = adapter.port();
    // The distinction matters: the queue service treats NotFound as "gone"
    // and Backend as "stop and shout".
    try testing.expectError(error.JobNotFound, store.byId(null, 999));
    try testing.expectError(error.JobNotFound, store.byNzbHash(null, "nope"));
}

test "the user store counts, saves and reloads through the port" {
    const conn = try openMemory();
    defer conn.close();

    var adapter: UserStore = .{ .gpa = testing.allocator, .conn = conn };
    const store = adapter.port();

    try testing.expectEqual(@as(u32, 0), try store.count(null));

    const u = try testing.allocator.create(dauth.User);
    u.* = try dauth.User.init(testing.allocator, .{
        .username = "admin",
        .password_hash = "$2b$10$notarealhashbutlongenoughxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
    }, 1_700_000_000_000);
    try store.save(null, u);
    store.release(u);

    try testing.expectEqual(@as(u32, 1), try store.count(null));
    const found = try store.byUsername(null, "admin");
    defer store.release(found);
    try testing.expectEqualStrings("admin", found.username);

    try testing.expectError(error.UserNotFound, store.byUsername(null, "ghost"));
}

test "the session store puts, gets and deletes" {
    const conn = try openMemory();
    defer conn.close();

    var users: UserStore = .{ .gpa = testing.allocator, .conn = conn };
    const u = try testing.allocator.create(dauth.User);
    u.* = try dauth.User.init(testing.allocator, .{
        .username = "admin",
        .password_hash = "$2b$10$notarealhashbutlongenoughxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
    }, 1_700_000_000_000);
    try users.port().save(null, u);
    const user_id = u.id;
    users.port().release(u);

    var adapter: SessionStore = .{ .conn = conn };
    const store = adapter.port();

    const s = dauth.Session{
        .token = @splat('a'),
        .user_id = user_id,
        .created_at = 1_700_000_000_000,
        .expires_at = 1_700_000_600_000,
        .last_seen = 1_700_000_000_000,
    };
    try store.put(null, s);

    const got = try store.get(&s.token);
    try testing.expectEqual(user_id, got.user_id);

    try store.delete(null, &s.token);
    try testing.expectError(error.SessionNotFound, store.get(&s.token));
}
