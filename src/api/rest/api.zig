//! `Api` — everything the REST handlers were wired with.
//!
//! One instance per process, held by `bootstrap` and handed to the HTTP
//! server as its `app_ctx`. Handlers reach it with `ctx.app(Api)`.
//!
//! Every port is optional. A build without a scheduler, or a test that
//! only cares about the queue, leaves the rest null and the endpoints
//! answer 503 rather than faulting. That is the same "nil disables it"
//! shape the Go `Handlers` struct had, with the difference that the
//! route still exists — see `respond.unavailable` for why.

const std = @import("std");
const http = @import("../../net/http/server.zig");
const logring = @import("../../core/logring.zig");
const sse = @import("../sse.zig");
const metrics = @import("../metrics.zig");
const ports = @import("ports.zig");
const ratelimit = @import("ratelimit.zig");
const ui = @import("../ui.zig");

const Allocator = std.mem.Allocator;

pub const Api = struct {
    gpa: Allocator,

    /// Per-request scratch. Reset at the top of every handler rather
    /// than freed, so a steady stream of `/queue` polls costs one
    /// allocation in total once the arena has grown to fit the largest
    /// response. Nothing may outlive the handler that took it — which is
    /// also true of the request slices it is usually built from.
    scratch: std.heap.ArenaAllocator,

    // -- ports --------------------------------------------------------

    queue: ?ports.Queue = null,
    events: ?ports.Events = null,
    servers: ?ports.Servers = null,
    probe: ?ports.ServerProbe = null,
    categories: ?ports.Categories = null,
    system: ?ports.System = null,
    health: ?ports.Health = null,
    schedule: ?ports.Schedule = null,
    commands: ?ports.Commands = null,
    backups: ?ports.Backups = null,
    log_files: ?ports.LogFiles = null,
    disk: ?ports.DiskSpace = null,
    subscriptions: ?ports.Subscriptions = null,
    config: ?ports.RuntimeConfig = null,

    /// Assets rewritten for the mount path, owned by the composition
    /// root. Null in a build with no frontend and in tests that never
    /// serve one, in which case the embedded bytes are served as-is.
    ui_assets: ?*ui.Rewriter = null,
    bandwidth: ?ports.Bandwidth = null,
    auth: ?ports.Auth = null,

    // -- in-process collaborators -------------------------------------

    /// The log ring the System page reads and tails. Core, not a port:
    /// it is already an in-memory data structure with no I/O to fake.
    log_ring: ?*logring.Ring = null,
    /// Fan-out for `/api/v1/events` and `/api/v1/queue/stream`.
    events_hub: ?*sse.Hub = null,
    /// Fan-out for `/api/v1/system/logs/tail`.
    logs_hub: ?*sse.Hub = null,
    metrics: ?*metrics.Registry = null,

    /// Guards `/auth/login` and `/auth/setup`. The Go build wrapped
    /// those two routes in middleware; the Zig router has no middleware
    /// layer, so the two handlers consult this directly — which also
    /// keeps it obvious at the call site that they are the rate-limited
    /// ones.
    login_limiter: ratelimit.Limiter,

    /// Monotonic nanoseconds. Injectable so a test can drive the rate
    /// limiter's window without sleeping.
    nowFn: *const fn () u64 = ratelimit.nowNanos,

    /// Scratch for the session check in the HTTP layer's auth, which
    /// runs before any handler and therefore before `beginRequest`.
    /// Separate from `scratch` so validating a cookie cannot invalidate
    /// what a handler is holding.
    session_scratch: std.heap.ArenaAllocator,

    pub fn init(gpa: Allocator) Api {
        return .{
            .gpa = gpa,
            .scratch = std.heap.ArenaAllocator.init(gpa),
            .session_scratch = std.heap.ArenaAllocator.init(gpa),
            .login_limiter = ratelimit.Limiter.initAuth(gpa, 0),
        };
    }

    pub fn deinit(self: *Api) void {
        self.login_limiter.deinit();
        self.session_scratch.deinit();
        self.scratch.deinit();
        self.* = undefined;
    }

    // -- the HTTP layer's view of authentication ----------------------

    /// What the server is wired with. Both credentials are read through
    /// the ports on every request, so rotating the key or revoking a
    /// session takes effect on the very next one.
    ///
    /// With no auth port there is no session half — API-key-only mode.
    /// With no runtime config there is no key either, and
    /// `constantTimeStringEq` refuses an empty expected key, so an
    /// unconfigured server is a closed one rather than an open one.
    pub fn authConfig(self: *Api) http.Auth {
        return .{
            .key_ctx = self,
            .key_fn = expectedKey,
            .session = if (self.auth != null)
                .{ .ctx = self, .authenticate = authenticateSession }
            else
                null,
        };
    }

    fn expectedKey(ctx: ?*anyopaque) []const u8 {
        const self: *Api = @ptrCast(@alignCast(ctx.?));
        const cfg = self.config orelse return "";
        return cfg.apiKey();
    }

    fn authenticateSession(ctx: ?*anyopaque, token: []const u8) bool {
        const self: *Api = @ptrCast(@alignCast(ctx.?));
        const auth = self.auth orelse return false;
        _ = self.session_scratch.reset(.retain_capacity);
        _ = auth.authenticate(self.session_scratch.allocator(), token) catch return false;
        return true;
    }

    /// Reset the scratch arena and hand out its allocator. Called once
    /// at the top of a handler; calling it twice inside one handler
    /// invalidates everything the first call produced, so don't.
    pub fn beginRequest(self: *Api) Allocator {
        _ = self.scratch.reset(.retain_capacity);
        return self.scratch.allocator();
    }

    pub fn now(self: *const Api) u64 {
        return self.nowFn();
    }

    /// The copy of `path` with the frontend's base sentinel resolved to
    /// `base`, or null when this asset carries no sentinel and the
    /// embedded bytes are already correct.
    pub fn uiAsset(self: *Api, base: []const u8, path: []const u8) ?*const ui.Rewriter.Entry {
        const rw = self.ui_assets orelse return null;
        return rw.find(base, path);
    }
};
