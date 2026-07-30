//! The SAB compatibility layer's ports.
//!
//! `api/sab/ports.zig` narrows a job down to eleven scalars precisely so
//! the DTO layer never touches an aggregate. `JobView.fromJob` is the one
//! place the two are connected, and this file is where that cost is paid
//! — once, in the wiring, exactly as the port's doc comment intends.

const std = @import("std");

const log = @import("../core/log.zig");
const ports = @import("../api/sab/ports.zig");
const queue_svc = @import("../app/download/queue.zig");
const add_job_svc = @import("../app/download/add_job.zig");
const system_svc = @import("../app/system/service.zig");
const sqlite = @import("../store/sqlite.zig");
const repo_download = @import("../store/repo_download.zig");
const repo_category = @import("../store/repo_category.zig");

const infra = @import("infra.zig");
const settings = @import("settings.zig");

const Allocator = std.mem.Allocator;
const Conn = sqlite.Conn;

/// The SAB error vocabulary is three-valued, and which one a failure maps
/// to changes what Sonarr does next: `Unavailable` makes it back off,
/// `Rejected` makes it drop the item. Anything we cannot classify is
/// `Unavailable`, because backing off is the recoverable mistake.
fn mapErr(op: []const u8, e: anyerror) ports.PortError {
    return switch (e) {
        error.OutOfMemory => error.OutOfMemory,
        error.JobNotFound, error.NoRows => error.NotFound,
        error.DuplicateNzbHash,
        error.NzbBodyRequired,
        error.ParseFailed,
        error.NoUsableFiles,
        error.InvalidJob,
        => error.Rejected,
        else => {
            log.default.err("sab port failed", &.{
                log.str("op", op),
                log.str("error", @errorName(e)),
            });
            return error.Unavailable;
        },
    };
}

// ---------------------------------------------------------------------
// Queue
// ---------------------------------------------------------------------

pub const Queue = struct {
    gpa: Allocator,
    conn: *Conn,
    commands: *queue_svc.Service,

    pub fn port(self: *Queue) ports.QueuePort {
        return .{
            .ctx = @ptrCast(self),
            .activeJobsFn = &activeJobs,
            .historyJobsFn = &historyJobs,
            .getFn = &get,
            .pauseFn = &pause,
            .resumeFn = &unpause,
            .removeFn = &remove,
            .markCompletedFn = &markCompleted,
        };
    }

    fn self_(ctx: *anyopaque) *Queue {
        return @ptrCast(@alignCast(ctx));
    }

    fn repo(self: *Queue) repo_download.JobRepo {
        return repo_download.JobRepo.init(self.gpa, self.conn);
    }

    /// `gpa` here is always the per-request arena the handler built, so
    /// the aggregates the views borrow from die with the response.
    fn activeJobs(ctx: *anyopaque, gpa: Allocator) ports.PortError![]const ports.JobView {
        const self = self_(ctx);
        const list = self.repo().active(gpa, .bare) catch |e| return mapErr("sab.active", e);
        const out = try gpa.alloc(ports.JobView, list.items.items.len);
        for (list.items.items, 0..) |*j, i| out[i] = ports.JobView.fromJob(j);
        return out;
    }

    fn historyJobs(ctx: *anyopaque, gpa: Allocator, limit: usize) ports.PortError![]const ports.JobView {
        const self = self_(ctx);
        const list = self.repo().history(gpa, .{
            .limit = @intCast(@min(limit, std.math.maxInt(i32))),
        }, .bare) catch |e| return mapErr("sab.history", e);
        const out = try gpa.alloc(ports.JobView, list.items.items.len);
        for (list.items.items, 0..) |*j, i| out[i] = ports.JobView.fromJob(j);
        return out;
    }

    /// `mode=get_files` needs the per-file breakdown but never the
    /// segments, which is exactly what `shallow` loads.
    fn get(ctx: *anyopaque, gpa: Allocator, id: ports.JobId) ports.PortError!ports.JobDetail {
        const self = self_(ctx);
        const job = self.repo().byIdShallow(gpa, id) catch |e| return mapErr("sab.get", e);
        const p = try gpa.create(@TypeOf(job));
        p.* = job;
        return ports.JobDetail.fromJob(gpa, p);
    }

    fn pause(ctx: *anyopaque, id: ports.JobId) ports.PortError!void {
        self_(ctx).commands.pauseJob(id) catch |e| return mapErr("sab.pause", e);
    }

    fn unpause(ctx: *anyopaque, id: ports.JobId) ports.PortError!void {
        self_(ctx).commands.resumeJob(id) catch |e| return mapErr("sab.resume", e);
    }

    fn remove(ctx: *anyopaque, id: ports.JobId) ports.PortError!void {
        self_(ctx).commands.removeJob(id) catch |e| return mapErr("sab.remove", e);
    }

    fn markCompleted(ctx: *anyopaque, id: ports.JobId) ports.PortError!void {
        self_(ctx).commands.markCompleted(id) catch |e| return mapErr("sab.markCompleted", e);
    }
};

// ---------------------------------------------------------------------
// Categories
// ---------------------------------------------------------------------

pub const Categories = struct {
    conn: *Conn,

    pub fn port(self: *Categories) ports.CategoryPort {
        return .{ .ctx = @ptrCast(self), .listFn = &list };
    }

    fn list(ctx: *anyopaque, gpa: Allocator) ports.PortError![]const ports.Category {
        const self: *Categories = @ptrCast(@alignCast(ctx));
        const repo = repo_category.CategoryRepo.init(self.conn);
        const l = repo.list(gpa) catch |e| return mapErr("sab.categories", e);
        const out = try gpa.alloc(ports.Category, l.items.items.len);
        for (l.items.items, 0..) |c, i| {
            out[i] = .{ .name = c.name, .dir = c.dir, .priority = c.priority };
        }
        return out;
    }
};

// ---------------------------------------------------------------------
// Job submission
// ---------------------------------------------------------------------

pub const AddJob = struct {
    svc: *add_job_svc.Service,

    pub fn port(self: *AddJob) ports.AddJobPort {
        return .{ .ctx = @ptrCast(self), .addFn = &add };
    }

    fn add(ctx: *anyopaque, _: Allocator, cmd: ports.AddJobCmd) ports.PortError!ports.AddOutcome {
        const self: *AddJob = @ptrCast(@alignCast(ctx));
        const r = self.svc.addJob(.{
            .nzb = cmd.nzb,
            .name = cmd.name,
            .category = cmd.category,
            .source = cmd.source,
        }) catch |e| switch (e) {
            // A re-grab of a release the *arr thinks failed is routine.
            // The handler answers 200 with the winner's handle, which is
            // why this is not an error path at all.
            error.OutOfMemory => return error.OutOfMemory,
            else => return mapErr("sab.add", e),
        };
        return .{ .id = r.id, .duplicate = r.duplicate };
    }
};

// ---------------------------------------------------------------------
// Trivial providers
// ---------------------------------------------------------------------

/// The live API key, read per request so a rotation from Settings takes
/// effect on the very next call rather than at the next restart.
pub const ApiKey = struct {
    rt: *settings.Runtime,

    pub fn port(self: *ApiKey) ports.ApiKeyPort {
        return .{ .ctx = @ptrCast(self), .keyFn = &read };
    }

    fn read(ctx: ?*anyopaque) []const u8 {
        const self: *ApiKey = @ptrCast(@alignCast(ctx.?));
        return self.rt.apiKey();
    }
};

/// Overall download rate, for `queue.speed` and the per-slot ETAs.
pub const Throughput = struct {
    svc: *system_svc.Service,

    pub fn port(self: *Throughput) ports.ThroughputPort {
        return .{ .ctx = @ptrCast(self), .rateFn = &rate };
    }

    fn rate(ctx: ?*anyopaque) i64 {
        const self: *Throughput = @ptrCast(@alignCast(ctx.?));
        var buf: [16]i64 = undefined;
        return self.svc.sample(&buf).current_bytes_per_sec;
    }
};

// ---------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------

const testing = std.testing;

test "a failure maps to the verdict the *arr clients act on" {
    // Back off, do not blacklist.
    try testing.expectEqual(ports.PortError.Unavailable, mapErr("t", error.Misuse));
    try testing.expectEqual(ports.PortError.NotFound, mapErr("t", error.JobNotFound));
    // The NZB itself was the problem, so retrying it forever is pointless.
    try testing.expectEqual(ports.PortError.Rejected, mapErr("t", error.ParseFailed));
    try testing.expectEqual(ports.PortError.OutOfMemory, mapErr("t", error.OutOfMemory));
}
