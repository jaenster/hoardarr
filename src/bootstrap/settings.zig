//! Runtime-mutable configuration: the settings table in front of
//! `config.toml`.
//!
//! Two sources, one answer. `config.toml` is what the operator wrote and
//! what a fresh install starts from; the `settings` table is what the UI
//! has changed since. The table wins where it has a row, because
//! otherwise a value edited in Settings would silently revert at the next
//! restart — which was the single most confusing behaviour of the Go
//! build before it grew the same table.
//!
//! ## Why the strings are cached here
//!
//! `RuntimeConfig.urlBase()` and `apiKey()` return a borrowed `[]const u8`
//! with no allocator in sight: the HTTP layer calls them once per request
//! from inside the auth check, before any arena exists. So the two
//! string-valued settings are read from the table into buffers this
//! struct owns, refreshed whenever they are written. Everything else is a
//! scalar and is read straight through on every call, which is what keeps
//! a Settings edit effective on the *next* job rather than the next
//! restart.

const std = @import("std");

const log = @import("../core/log.zig");
const sys = @import("../posix/sys.zig");
const config_mod = @import("../core/config.zig");
const app_ports = @import("../app/ports.zig");
const sqlite = @import("../store/sqlite.zig");
const repo_settings = @import("../store/repo_settings.zig");

const Allocator = std.mem.Allocator;
const Conn = sqlite.Conn;

/// The `settings` keys this layer owns. Named constants because a typo in
/// a string literal is a setting that reads its default forever and never
/// reports a problem.
pub const keys = struct {
    pub const url_base = "server.url_base";
    pub const api_key = "auth.api_key";
    pub const max_concurrent_jobs = "server.max_concurrent_jobs";
    pub const fail_hopeless_ratio = "server.fail_hopeless_ratio";
    pub const defer_recovery_vols = "server.defer_recovery_vols";
    pub const delete_samples = "server.delete_samples";
    pub const collapse_single_folder = "server.collapse_single_folder";
    pub const bandwidth_global = "bandwidth.global_bytes_per_sec";
};

pub const Error = repo_settings.Error;

/// Longest a URL base or an API key may be. Both are operator-chosen and
/// both live in fixed buffers, so the cap is enforced rather than
/// truncated — a silently shortened API key is a locked-out operator.
pub const max_cached_len = 256;

pub const Runtime = struct {
    gpa: Allocator,
    conn: *Conn,
    /// What `config.toml` said. The fallback for every key with no row,
    /// and the source of the values that are not editable at runtime.
    file: config_mod.Config,
    /// Advertised to SAB clients as the base of the compatibility API.
    sab_base: []const u8 = "",

    url_base_buf: [max_cached_len]u8 = undefined,
    url_base_len: usize = 0,
    api_key_buf: [max_cached_len]u8 = undefined,
    api_key_len: usize = 0,

    fn repo(self: *Runtime) repo_settings.SettingsRepo {
        return repo_settings.SettingsRepo.init(self.conn);
    }

    /// Loads the cached strings. Called once at start-up, after migration
    /// and before the listener, so the very first request already sees
    /// whatever the operator last set.
    pub fn refresh(self: *Runtime) void {
        self.url_base_len = self.cache(&self.url_base_buf, keys.url_base, self.file.server.url_base);
        self.api_key_len = self.cache(&self.api_key_buf, keys.api_key, self.file.auth.api_key);
    }

    fn cache(self: *Runtime, buf: *[max_cached_len]u8, key: []const u8, fallback: []const u8) usize {
        const value = self.repo().get(self.gpa, key) catch fallback_blk: {
            break :fallback_blk null;
        };
        const text = if (value) |v| v else fallback;
        defer if (value) |v| self.gpa.free(v);

        if (text.len > buf.len) {
            log.default.warn("setting too long, using the configured value", &.{
                log.str("key", key),
                log.uint("len", text.len),
            });
            const n = @min(fallback.len, buf.len);
            @memcpy(buf[0..n], fallback[0..n]);
            return n;
        }
        @memcpy(buf[0..text.len], text);
        return text.len;
    }

    // -- reads ---------------------------------------------------------

    pub fn urlBase(self: *const Runtime) []const u8 {
        return self.url_base_buf[0..self.url_base_len];
    }

    pub fn apiKey(self: *const Runtime) []const u8 {
        return self.api_key_buf[0..self.api_key_len];
    }

    pub fn maxConcurrentJobs(self: *Runtime) i64 {
        return self.repo().getIntOr(keys.max_concurrent_jobs, self.file.server.max_concurrent_jobs) catch
            self.file.server.max_concurrent_jobs;
    }

    pub fn failHopelessRatio(self: *Runtime) f64 {
        return self.repo().getFloatOr(keys.fail_hopeless_ratio, self.file.server.fail_hopeless_ratio) catch
            self.file.server.fail_hopeless_ratio;
    }

    pub fn deferRecoveryVols(self: *Runtime) bool {
        return self.repo().getBoolOr(keys.defer_recovery_vols, self.file.server.defer_recovery_vols) catch
            self.file.server.defer_recovery_vols;
    }

    pub fn deleteSamples(self: *Runtime) bool {
        return self.repo().getBoolOr(keys.delete_samples, self.file.server.delete_samples) catch
            self.file.server.delete_samples;
    }

    pub fn collapseSingleFolder(self: *Runtime) bool {
        return self.repo().getBoolOr(keys.collapse_single_folder, self.file.server.collapse_single_folder) catch
            self.file.server.collapse_single_folder;
    }

    pub fn bandwidthGlobal(self: *Runtime) i64 {
        return self.repo().getIntOr(keys.bandwidth_global, self.file.bandwidth.global_bytes_per_sec) catch
            self.file.bandwidth.global_bytes_per_sec;
    }

    // -- writes --------------------------------------------------------

    pub fn setInt(self: *Runtime, key: []const u8, v: i64) Error!void {
        try self.repo().setInt(key, v);
    }

    pub fn setFloat(self: *Runtime, key: []const u8, v: f64) Error!void {
        try self.repo().setFloat(key, v);
    }

    pub fn setBool(self: *Runtime, key: []const u8, v: bool) Error!void {
        try self.repo().setBool(key, v);
    }

    pub fn setUrlBase(self: *Runtime, v: []const u8) Error!void {
        if (v.len > max_cached_len) return error.Misuse;
        try self.repo().set(keys.url_base, v);
        @memcpy(self.url_base_buf[0..v.len], v);
        self.url_base_len = v.len;
    }

    /// Mints a new key, persists it and starts honouring it immediately.
    /// The old key stops working on this call, not at the next restart —
    /// which is the entire point of rotating one.
    pub fn rotateApiKey(self: *Runtime, arena: Allocator) Error![]const u8 {
        var raw: [16]u8 = undefined;
        sys.randomBytes(&raw);
        var hex: [32]u8 = undefined;
        _ = std.fmt.bufPrint(&hex, "{x}", .{&raw}) catch unreachable;

        try self.repo().set(keys.api_key, &hex);
        @memcpy(self.api_key_buf[0..hex.len], &hex);
        self.api_key_len = hex.len;
        return arena.dupe(u8, &hex) catch return error.OutOfMemory;
    }

    // -- app-layer views -----------------------------------------------

    /// `Toggle` over one boolean setting. `key` must outlive the toggle,
    /// which for the `keys` constants is the program.
    pub fn toggle(self: *Runtime, comptime key: []const u8) app_ports.Toggle {
        const Impl = struct {
            fn read(ctx: *anyopaque) bool {
                const rt: *Runtime = @ptrCast(@alignCast(ctx));
                return rt.repo().getBoolOr(key, defaultBool(rt, key)) catch defaultBool(rt, key);
            }
        };
        return .{ .ctx = @ptrCast(self), .readFn = &Impl.read };
    }

    /// `Knob` over one integer setting.
    pub fn knob(self: *Runtime, comptime key: []const u8) app_ports.Knob {
        const Impl = struct {
            fn read(ctx: *anyopaque) i64 {
                const rt: *Runtime = @ptrCast(@alignCast(ctx));
                return rt.repo().getIntOr(key, defaultInt(rt, key)) catch defaultInt(rt, key);
            }
        };
        return .{ .ctx = @ptrCast(self), .readFn = &Impl.read };
    }

    fn defaultBool(self: *const Runtime, comptime key: []const u8) bool {
        return comptime_blk: {
            break :comptime_blk if (comptime std.mem.eql(u8, key, keys.defer_recovery_vols))
                self.file.server.defer_recovery_vols
            else if (comptime std.mem.eql(u8, key, keys.delete_samples))
                self.file.server.delete_samples
            else if (comptime std.mem.eql(u8, key, keys.collapse_single_folder))
                self.file.server.collapse_single_folder
            else
                false;
        };
    }

    fn defaultInt(self: *const Runtime, comptime key: []const u8) i64 {
        return comptime_blk: {
            break :comptime_blk if (comptime std.mem.eql(u8, key, keys.max_concurrent_jobs))
                self.file.server.max_concurrent_jobs
            else if (comptime std.mem.eql(u8, key, keys.bandwidth_global))
                self.file.bandwidth.global_bytes_per_sec
            else
                0;
        };
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

test "a setting falls back to config.toml until the table has a row" {
    const conn = try openMemory();
    defer conn.close();

    var rt: Runtime = .{
        .gpa = testing.allocator,
        .conn = conn,
        .file = .{ .server = .{ .url_base = "/hoardarr", .max_concurrent_jobs = 3 } },
    };
    rt.refresh();

    try testing.expectEqualStrings("/hoardarr", rt.urlBase());
    try testing.expectEqual(@as(i64, 3), rt.maxConcurrentJobs());

    // Once written, the table wins — otherwise a Settings edit reverts on
    // the next restart, which is the bug this table exists to fix.
    try rt.setUrlBase("/media/hoardarr");
    try rt.setInt(keys.max_concurrent_jobs, 8);
    try testing.expectEqualStrings("/media/hoardarr", rt.urlBase());
    try testing.expectEqual(@as(i64, 8), rt.maxConcurrentJobs());

    // And it survives a reload of the cache.
    rt.refresh();
    try testing.expectEqualStrings("/media/hoardarr", rt.urlBase());
}

test "rotating the api key takes effect on the same call" {
    const conn = try openMemory();
    defer conn.close();

    var rt: Runtime = .{
        .gpa = testing.allocator,
        .conn = conn,
        .file = .{ .auth = .{ .api_key = "original-key" } },
    };
    rt.refresh();
    try testing.expectEqualStrings("original-key", rt.apiKey());

    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const minted = try rt.rotateApiKey(arena.allocator());

    try testing.expectEqual(@as(usize, 32), minted.len);
    for (minted) |c| try testing.expect(std.ascii.isHex(c));
    // The old key must stop working immediately, not at the next restart.
    try testing.expectEqualStrings(minted, rt.apiKey());
    try testing.expect(!std.mem.eql(u8, "original-key", rt.apiKey()));
}

test "a toggle and a knob read the table fresh on every call" {
    const conn = try openMemory();
    defer conn.close();

    var rt: Runtime = .{
        .gpa = testing.allocator,
        .conn = conn,
        .file = .{ .server = .{ .delete_samples = true, .max_concurrent_jobs = 2 } },
    };
    rt.refresh();

    const t = rt.toggle(keys.delete_samples);
    const k = rt.knob(keys.max_concurrent_jobs);
    try testing.expect(t.read());
    try testing.expectEqual(@as(i64, 2), k.read());

    try rt.setBool(keys.delete_samples, false);
    try rt.setInt(keys.max_concurrent_jobs, 5);
    // No restart, no cache invalidation: the next job sees the new value.
    try testing.expect(!t.read());
    try testing.expectEqual(@as(i64, 5), k.read());
}
