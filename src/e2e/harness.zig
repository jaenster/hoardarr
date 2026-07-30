//! Boots a real `bootstrap.App` and drives it from a test.
//!
//! # What this is for
//!
//! Every component in the tree has unit tests. What none of them prove
//! is that the composition root wires them into something that works:
//! a port left null answers 503 with a body that looks legitimate, a
//! migration that half-ran leaves a daemon that starts and then fails
//! on the first write. The only assertion that catches those is one
//! made against a booted daemon over its own HTTP surface.
//!
//! So this boots the same `App` `bootstrap.run` boots — same config
//! loader, same SQLite file, same migrations, same object graph, same
//! route table — against a temporary data directory and an ephemeral
//! port, and lets a test drive it.
//!
//! # There are no sleeps
//!
//! The daemon is single-threaded on a reactor, and so is the test. A
//! test that slept would be sleeping on the same thread the daemon
//! needs to make progress on, so it would not merely be flaky — it
//! would deadlock. `pumpUntil` advances the loop and re-checks a
//! predicate against a wall-clock deadline; the deadline exists so a
//! wiring bug that never answers fails the test instead of hanging the
//! suite.
//!
//! The test NNTP server runs on the *same* loop, which is what makes
//! this work: one `tick` advances the HTTP client, the HTTP server, the
//! NNTP client and the NNTP server together, with no race to lose.
//!
//! # Isolation
//!
//! Each harness gets its own directory under `/tmp` with random bytes
//! in the name and its own kernel-assigned port, so the suite can run
//! in any order, in parallel, and repeatedly, without two tests sharing
//! a database or fighting for a port. `deinit` removes the directory —
//! a leftover database from a previous failure is exactly the thing
//! that makes a test pass for the wrong reason.

const std = @import("std");

const sys = @import("../posix/sys.zig");
const reactor = @import("../posix/reactor.zig");
const config = @import("../core/config.zig");
const client = @import("../net/http/client.zig");
const sqlite = @import("../store/sqlite.zig");
const migrate = @import("../store/migrate.zig");
const bootstrap = @import("../bootstrap.zig");
const infra = @import("../bootstrap/infra.zig");
const tsnntp = @import("../testserver/nntp.zig");

const Allocator = std.mem.Allocator;
const testing = std.testing;

pub const Error = error{
    /// The predicate never became true before the deadline.
    Timeout,
    /// The exchange completed with a transport-level failure.
    RequestFailed,
} || Allocator.Error;

/// How long a `pumpUntil` waits before giving up. Generous, because it
/// is a failure bound and not a synchronisation delay: nothing waits
/// for it in the happy path.
pub const default_deadline_ns: u64 = 20 * std.time.ns_per_s;

/// The API key every harness boots with. Fixed rather than generated so
/// a test can write it as a literal in a URL; the daemon's own
/// key-generation path is covered by `bootstrap.zig`'s unit tests.
pub const api_key = "0123456789abcdef0123456789abcdef";

/// A booted daemon, its data directory, and the loop everything shares.
///
/// Heap-allocated in `init` and never moved: the reactor stores
/// `&app.server.listener.source` and `&app.ticker`, so a by-value
/// return would leave the loop pointing at a dead stack frame.
pub const Harness = struct {
    gpa: Allocator,
    app: *bootstrap.App,
    /// Owned. The temp directory tree, removed by `deinit`.
    dir: []u8,
    port: u16 = 0,

    /// The fake provider, when a test asked for one. On the same loop.
    nntp: ?*tsnntp.Server = null,

    pub const Options = struct {
        /// Arms the heartbeat and housekeeping timers. Off by default:
        /// most tests want a quiet loop they fully control, and a
        /// one-second ticker firing mid-assertion only adds noise.
        timers: bool = false,
        /// The key `config` falls back to when the settings table holds
        /// none yet.
        ///
        /// A restart test has to be able to change this: with a fixed
        /// fallback, "the key survived the restart" would pass even if
        /// nothing had been persisted, because the fallback would
        /// produce the same value both times.
        api_key_fallback: []const u8 = api_key,
    };

    /// Boots a daemon against a fresh temporary directory.
    ///
    /// `label` only ends up in the directory name, to make a leftover
    /// tree after a crash identifiable.
    pub fn init(gpa: Allocator, label: []const u8, options: Options) !*Harness {
        const dir = try tempDir(gpa, label);
        errdefer gpa.free(dir);
        errdefer removeTree(gpa, dir);

        try sys.mkdirPath(dir);

        const self = try gpa.create(Harness);
        errdefer gpa.destroy(self);
        self.* = .{ .gpa = gpa, .app = undefined, .dir = dir };

        self.app = try gpa.create(bootstrap.App);
        errdefer gpa.destroy(self.app);

        try self.boot(options);
        return self;
    }

    /// Opens the database and wires the graph. Split out of `init` so
    /// `restart` can run exactly the same sequence a second time.
    fn boot(self: *Harness, options: Options) !void {
        const gpa = self.gpa;
        const app = self.app;
        app.* = .{ .gpa = gpa };

        app.cfg = try config.loadFromBytes(gpa, null, self.dir, .{
            .api_key_fallback = options.api_key_fallback,
        });
        errdefer app.cfg.deinit();

        try app.loop.init(gpa);
        errdefer app.loop.deinit();

        // `config.normalize` resolves the default `./data` against the
        // base directory, so the resolved one is what has to exist —
        // the same distinction `bootstrap.run` makes, for the same
        // reason.
        try sys.mkdirPath(app.cfg.config.server.data_dir);

        var db_buf: [sys.path_max]u8 = undefined;
        const db_path = try sys.joinZ(&db_buf, app.cfg.config.server.data_dir, "hoardarr.db");
        app.db = try sqlite.Conn.open(gpa, db_path, .{});
        errdefer app.db.close();
        try migrate.migrate(app.db);

        try app.wire();
        app.started = true;
        errdefer app.deinit();

        // `App.ensureDirs` is private to the composition root, so the
        // same directories are made here. Made at boot rather than
        // lazily for the reason the daemon does it: a test that asserts
        // on `incomplete/` should not be the thing that creates it.
        for ([_][]const u8{
            app.cfg.config.paths.incomplete_dir,
            app.cfg.config.paths.complete_dir,
        }) |d| {
            if (d.len != 0) try sys.mkdirPath(d);
        }

        // Port 0 lets the kernel choose. Two runs of the suite in
        // parallel therefore cannot collide.
        try app.server.listen(try bootstrap.parseListen("127.0.0.1:0"));
        self.port = try app.server.listener.boundPort();

        if (options.timers) try app.startTimers();
    }

    pub fn deinit(self: *Harness) void {
        if (self.nntp) |s| {
            s.deinit();
            self.gpa.destroy(s);
            self.nntp = null;
        }
        self.app.deinit();
        self.gpa.destroy(self.app);
        removeTree(self.gpa, self.dir);
        self.gpa.free(self.dir);
        self.gpa.destroy(self);
    }

    /// Tears the daemon down and boots a new one over the same data
    /// directory — the same database file, the same WAL, the same
    /// incomplete tree.
    ///
    /// This is the only honest way to test restart behaviour: nothing
    /// is carried over in memory, so anything the second daemon knows
    /// it read back off disk. The daemon's own HTTP port changes,
    /// because the kernel picks a new one.
    ///
    /// The fake provider is torn down and rebound to the *same* port,
    /// because a `servers` row read back from the database names a port
    /// and it has to still be there. Its corpus does not survive — the
    /// caller re-registers the articles, which is also what would
    /// happen if the provider had been restarted alongside us.
    pub fn restart(self: *Harness, options: Options) !void {
        var nntp_opts: ?tsnntp.Options = null;
        if (self.nntp) |s| {
            var o = s.opts;
            o.port = try s.port();
            nntp_opts = o;
            s.deinit();
            self.gpa.destroy(s);
            self.nntp = null;
        }

        self.app.deinit();
        try self.boot(options);

        if (nntp_opts) |o| _ = try self.startNntp(o);
    }

    // -- the fake provider ---------------------------------------------

    /// Starts the content-addressed NNTP server on the daemon's own
    /// loop. Returns its port.
    pub fn startNntp(self: *Harness, opts: tsnntp.Options) !u16 {
        std.debug.assert(self.nntp == null);
        const s = try self.gpa.create(tsnntp.Server);
        errdefer self.gpa.destroy(s);
        const p = try s.start(self.gpa, &self.app.loop, opts);
        self.nntp = s;
        return p;
    }

    // -- driving the loop ----------------------------------------------

    /// Advances the reactor until `predicate` returns true or the
    /// deadline passes.
    ///
    /// The 10ms tick bound is a ceiling on how long one `tick` may
    /// block, not a poll interval: a ready socket wakes it immediately.
    /// It exists so a predicate that depends on a timer rather than on
    /// I/O still gets re-checked.
    pub fn pumpUntil(
        self: *Harness,
        ctx: anytype,
        comptime predicate: fn (@TypeOf(ctx)) bool,
        deadline_ns: u64,
    ) Error!void {
        const start = sys.monotonicNanos();
        while (!predicate(ctx)) {
            if (sys.monotonicNanos() -| start > deadline_ns) return error.Timeout;
            _ = self.app.loop.tick(10) catch return error.Timeout;
        }
    }

    /// Advances the loop `n` times regardless of any condition. For the
    /// rare case where a test needs the daemon to notice something it
    /// cannot observe from outside; prefer `pumpUntil`.
    pub fn pump(self: *Harness, n: usize) void {
        var i: usize = 0;
        while (i < n) : (i += 1) _ = self.app.loop.tick(1) catch return;
    }

    // -- HTTP ------------------------------------------------------------

    /// A completed response, owned by the caller.
    pub const Reply = struct {
        gpa: Allocator,
        status: u16 = 0,
        body: []u8 = &.{},
        /// First `Set-Cookie` header, copied. Empty when absent.
        set_cookie: []u8 = &.{},
        done: bool = false,
        failed: ?anyerror = null,

        pub fn deinit(self: *Reply) void {
            self.gpa.free(self.body);
            self.gpa.free(self.set_cookie);
            self.* = undefined;
        }

        /// The value of the session cookie in `Set-Cookie`, before the
        /// first `;`. Empty when the header did not name that cookie.
        pub fn sessionCookie(self: *const Reply) []const u8 {
            const prefix = "hoardarr_session=";
            if (!std.mem.startsWith(u8, self.set_cookie, prefix)) return "";
            const rest = self.set_cookie[prefix.len..];
            const end = std.mem.indexOfScalar(u8, rest, ';') orelse rest.len;
            return rest[0..end];
        }

        pub fn contains(self: *const Reply, needle: []const u8) bool {
            return std.mem.indexOf(u8, self.body, needle) != null;
        }

        /// The raw JSON value for a top-level-ish `"key":` in the body,
        /// with surrounding quotes stripped for a string.
        ///
        /// Deliberately a scanner rather than a parser: these tests
        /// assert on a handful of scalars, and a scanner cannot claim a
        /// field parsed correctly when the daemon emitted something
        /// structurally different — it just fails to find it.
        pub fn field(self: *const Reply, key: []const u8) ?[]const u8 {
            return jsonField(self.body, key);
        }

        pub fn expectField(self: *const Reply, key: []const u8, want: []const u8) !void {
            const got = self.field(key) orelse {
                std.debug.print("\nfield '{s}' absent from: {s}\n", .{ key, self.body });
                return error.FieldMissing;
            };
            if (!std.mem.eql(u8, got, want)) {
                std.debug.print("\nfield '{s}' = '{s}'; want '{s}'\n", .{ key, got, want });
                return error.FieldMismatch;
            }
        }

        fn onComplete(ctx: ?*anyopaque, result: client.Error!client.Response) void {
            const self: *Reply = @ptrCast(@alignCast(ctx.?));
            self.done = true;
            var res = result catch |e| {
                self.failed = e;
                return;
            };
            defer res.deinit();
            self.status = res.status;
            self.body = self.gpa.dupe(u8, res.body) catch &.{};
            if (res.get("set-cookie")) |c| {
                self.set_cookie = self.gpa.dupe(u8, c) catch &.{};
            }
        }
    };

    pub const Req = struct {
        method: client.Method = .get,
        path: []const u8 = "/",
        body: []const u8 = "",
        content_type: []const u8 = "application/json",
        /// Sent as `X-Api-Key`. Empty sends no key at all, which is how
        /// the unauthenticated cases are expressed.
        api_key: []const u8 = api_key,
        /// Sent as `Cookie: hoardarr_session=<value>`.
        session: []const u8 = "",
        extra: []const client.Header = &.{},
    };

    /// One request/response against the daemon, driven on the shared
    /// loop. The caller owns the reply.
    pub fn request(self: *Harness, req: Req) !Reply {
        var reply: Reply = .{ .gpa = self.gpa };
        errdefer reply.deinit();

        var cookie_buf: [256]u8 = undefined;
        var headers: [8]client.Header = undefined;
        var n: usize = 0;
        if (req.api_key.len > 0) {
            headers[n] = .{ .name = "X-Api-Key", .value = req.api_key };
            n += 1;
        }
        if (req.session.len > 0) {
            const v = try std.fmt.bufPrint(&cookie_buf, "hoardarr_session={s}", .{req.session});
            headers[n] = .{ .name = "Cookie", .value = v };
            n += 1;
        }
        for (req.extra) |h| {
            headers[n] = h;
            n += 1;
        }

        var ex: client.Exchange = undefined;
        try ex.start(
            self.gpa,
            &self.app.loop,
            try std.Io.net.IpAddress.parse("127.0.0.1", self.port),
            .{
                .method = req.method,
                .path = req.path,
                .host = "127.0.0.1",
                .headers = headers[0..n],
                .body = req.body,
                .content_type = req.content_type,
            },
            .{ .deadline_ns = default_deadline_ns },
            &Reply.onComplete,
            &reply,
        );
        defer ex.deinit();

        const Wait = struct {
            fn ready(r: *const Reply) bool {
                return r.done;
            }
        };
        try self.pumpUntil(&reply, Wait.ready, default_deadline_ns);
        if (reply.failed) |e| return e;
        return reply;
    }

    /// `request`, plus "and it answered 200".
    pub fn get(self: *Harness, path: []const u8) !Reply {
        var r = try self.request(.{ .path = path });
        errdefer r.deinit();
        try expectStatus(&r, 200, path);
        return r;
    }

    // -- filesystem ------------------------------------------------------

    /// A path inside this harness's data directory.
    pub fn dataPath(self: *Harness, gpa: Allocator, parts: []const []const u8) ![]u8 {
        var out: std.ArrayList(u8) = .empty;
        errdefer out.deinit(gpa);
        try out.appendSlice(gpa, self.app.cfg.config.server.data_dir);
        for (parts) |p| {
            try out.append(gpa, '/');
            try out.appendSlice(gpa, p);
        }
        return out.toOwnedSlice(gpa);
    }

    pub fn incompleteDir(self: *Harness) []const u8 {
        return self.app.cfg.config.paths.incomplete_dir;
    }

    pub fn completeDir(self: *Harness) []const u8 {
        return self.app.cfg.config.paths.complete_dir;
    }
};

// ---------------------------------------------------------------------
// Free helpers
// ---------------------------------------------------------------------

/// Whole file contents. Caller owns the bytes.
pub fn readFile(gpa: Allocator, path: []const u8) ![]u8 {
    var buf: [sys.path_max]u8 = undefined;
    const p = try sys.pathZ(&buf, path);
    const fd = try sys.open(p, .{ .mode = .read_only });
    defer sys.close(fd);

    const size = try sys.fileSize(fd);
    const out = try gpa.alloc(u8, @intCast(size));
    errdefer gpa.free(out);

    var off: usize = 0;
    while (off < out.len) {
        const got = try sys.read(fd, out[off..]);
        if (got == 0) break;
        off += got;
    }
    return out[0..off];
}

pub fn fileExists(path: []const u8) bool {
    var buf: [sys.path_max]u8 = undefined;
    const p = sys.pathZ(&buf, path) catch return false;
    return sys.exists(p);
}

/// Byte-for-byte, with a failure message that says *where* they diverge
/// rather than only that they did. A length-only report sends you
/// looking at the assembler when the bug is one flipped segment.
pub fn expectBytesEqual(want: []const u8, got: []const u8) !void {
    compareBytes(want, got) catch |e| {
        switch (e) {
            error.LengthMismatch => std.debug.print(
                "\nlength differs: got {d}, want {d}\n",
                .{ got.len, want.len },
            ),
            error.BytesDiffer => {
                const at = firstDifference(want, got).?;
                std.debug.print(
                    "\nbytes differ at offset {d}: got 0x{x:0>2}, want 0x{x:0>2}\n",
                    .{ at, got[at], want[at] },
                );
            },
        }
        return e;
    };
}

/// The comparison without the diagnostics, so the harness's own test of
/// the failure path does not print a scary block on a green run.
pub fn compareBytes(want: []const u8, got: []const u8) error{ LengthMismatch, BytesDiffer }!void {
    if (want.len != got.len) return error.LengthMismatch;
    if (firstDifference(want, got) != null) return error.BytesDiffer;
}

fn firstDifference(want: []const u8, got: []const u8) ?usize {
    for (want[0..@min(want.len, got.len)], 0..) |w, i| {
        if (w != got[i]) return i;
    }
    return null;
}

pub fn expectStatus(r: *const Harness.Reply, want: u16, what: []const u8) !void {
    if (r.status == want) return;
    std.debug.print("\n{s} answered {d}; want {d}. body: {s}\n", .{ what, r.status, want, r.body });
    return error.UnexpectedStatus;
}

/// The raw JSON value following `"key":`, quotes stripped for strings.
/// Returns null when the key is absent.
pub fn jsonField(body: []const u8, key: []const u8) ?[]const u8 {
    var pat_buf: [128]u8 = undefined;
    const pat = std.fmt.bufPrint(&pat_buf, "\"{s}\":", .{key}) catch return null;
    const at = std.mem.indexOf(u8, body, pat) orelse return null;
    var i = at + pat.len;
    while (i < body.len and (body[i] == ' ' or body[i] == '\t')) i += 1;
    if (i >= body.len) return null;

    if (body[i] == '"') {
        i += 1;
        const start = i;
        while (i < body.len and body[i] != '"') : (i += 1) {
            if (body[i] == '\\') i += 1;
        }
        return body[start..@min(i, body.len)];
    }
    const start = i;
    while (i < body.len and body[i] != ',' and body[i] != '}' and body[i] != ']') i += 1;
    return std.mem.trim(u8, body[start..i], " \t\r\n");
}

/// `/tmp/hoardarr-e2e-<label>-<16 hex>`. Random rather than a counter so
/// two concurrent `zig test` processes cannot pick the same name.
fn tempDir(gpa: Allocator, label: []const u8) ![]u8 {
    var raw: [8]u8 = undefined;
    sys.randomBytes(&raw);
    return std.fmt.allocPrint(gpa, "/tmp/hoardarr-e2e-{s}-{x}", .{ label, &raw });
}

fn removeTree(gpa: Allocator, dir: []const u8) void {
    var fs_impl = infra.RealFs{ .gpa = gpa };
    fs_impl.filesystem().removeAll(dir) catch {};
}

// ---------------------------------------------------------------------
// tests for the harness itself
// ---------------------------------------------------------------------

test "the harness boots a real daemon that serves its own health check" {
    const gpa = testing.allocator;
    var h = try Harness.init(gpa, "boot", .{});
    defer h.deinit();

    var r = try h.get("/healthz");
    defer r.deinit();
    try testing.expect(r.contains("ok"));
}

test "two harnesses do not share a directory or a port" {
    const gpa = testing.allocator;
    var a = try Harness.init(gpa, "iso-a", .{});
    defer a.deinit();
    var b = try Harness.init(gpa, "iso-b", .{});
    defer b.deinit();

    try testing.expect(!std.mem.eql(u8, a.dir, b.dir));
    try testing.expect(a.port != b.port);
}

test "the json scanner reads the shapes the daemon actually emits" {
    const body =
        \\{"status":"ok","count":12,"nested":{"name":"x y"},"flag":true}
    ;
    try testing.expectEqualStrings("ok", jsonField(body, "status").?);
    try testing.expectEqualStrings("12", jsonField(body, "count").?);
    try testing.expectEqualStrings("x y", jsonField(body, "name").?);
    try testing.expectEqualStrings("true", jsonField(body, "flag").?);
    try testing.expect(jsonField(body, "absent") == null);
}

test "pumpUntil reports a timeout rather than hanging the suite" {
    const gpa = testing.allocator;
    var h = try Harness.init(gpa, "timeout", .{});
    defer h.deinit();

    const Never = struct {
        fn no(_: *const u8) bool {
            return false;
        }
    };
    const dummy: u8 = 0;
    try testing.expectError(error.Timeout, h.pumpUntil(&dummy, Never.no, 20 * std.time.ns_per_ms));
}

test "byte comparison fails on a single flipped byte, not just on length" {
    var a = [_]u8{ 1, 2, 3, 4 };
    const b = [_]u8{ 1, 2, 3, 4 };
    try expectBytesEqual(&a, &b);
    a[2] = 9;
    // `compareBytes` rather than `expectBytesEqual`: the latter prints a
    // diagnostic, and a green run should not print one.
    try testing.expectError(error.BytesDiffer, compareBytes(&a, &b));
    try testing.expectError(error.LengthMismatch, compareBytes(a[0..3], &b));
}
