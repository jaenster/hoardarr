//! hoardarr's configuration schema, loading rules and validation.
//!
//! Configuration is hierarchical TOML. Values are layered, highest
//! precedence first:
//!
//!   1. Environment variables (`HOARDARR_*`)
//!   2. The operator's TOML file (default: `./config.toml`)
//!   3. The built-in defaults in this file
//!
//! On first run with no config file, `loadOrCreate` writes a fresh
//! `config.toml` containing the defaults and a freshly generated API key,
//! so the operator has something usable immediately.
//!
//! Decoding is driven by reflection over the schema structs: a field's
//! Zig name *is* its TOML key unless the struct carries a `toml_names`
//! declaration that overrides it. Adding a setting is therefore one line
//! in a struct — no decode switch to keep in sync.
//!
//! Every string in a loaded `Config` lives in the `Loaded.arena`, or is a
//! static default. `Loaded.deinit` frees the lot.

const std = @import("std");
const Allocator = std.mem.Allocator;
const toml = @import("toml.zig");

/// Environment source. Injected rather than read from the process so that
/// tests never have to mutate the real environment.
pub const Env = std.process.Environ.Map;

pub const Config = struct {
    server: Server = .{},
    auth: Auth = .{},
    storage: Storage = .{},
    paths: Paths = .{},
    bandwidth: Bandwidth = .{},
};

pub const Server = struct {
    /// Address (host:port) the HTTP server binds to.
    listen: []const u8 = ":8085",

    /// Root directory for persistent state: SQLite DB, incomplete and
    /// complete directories, logs. Resolved to an absolute path by
    /// `normalize`.
    data_dir: []const u8 = "./data",

    /// Log level: "debug" | "info" | "warn" | "error". Empty resolves to
    /// "info" in `normalize`.
    log_level: []const u8 = "info",

    /// Path prefix this instance is mounted at behind a reverse proxy.
    /// Must start with "/" or be empty, and must not end with "/".
    url_base: []const u8 = "",

    /// Cap on jobs the orchestrator drives in parallel. 0 means
    /// unlimited; 1 is strict serial, which is the default because
    /// parallel jobs contend for the shared NNTP pool and produce
    /// jittery per-job speeds.
    max_concurrent_jobs: i64 = 1,

    /// Abort a download once failed bytes exceed this fraction of total
    /// bytes. 0 disables the check.
    fail_hopeless_ratio: f64 = 0,

    /// Defer PAR2 per-slice recovery volumes at job-add time and fetch
    /// them only if repair needs them.
    defer_recovery_vols: bool = false,

    /// Remove sample/proof files from the target directory after a
    /// successful move.
    delete_samples: bool = true,

    /// Lift the contents of a single redundant inner directory up one
    /// level when a release lands wrapped.
    collapse_single_folder: bool = true,
};

pub const Auth = struct {
    /// Shared secret required on every protected endpoint. Generated on
    /// first run: 32 hex characters, 16 bytes of entropy, matching the
    /// SABnzbd / Sonarr convention.
    api_key: []const u8 = "",
};

pub const Storage = struct {
    /// Persistence adapter. Only "sqlite" is implemented.
    backend: []const u8 = "sqlite",
    sqlite: SQLite = .{},
};

pub const SQLite = struct {
    /// On-disk DB file. Empty resolves to `<data_dir>/hoardarr.db`.
    path: []const u8 = "",
};

pub const Paths = struct {
    /// In-progress downloads. Empty resolves to `<data_dir>/incomplete`.
    incomplete_dir: []const u8 = "",
    /// Parent of the category directories completed downloads land in.
    /// Empty resolves to `<data_dir>/complete`.
    complete_dir: []const u8 = "",
};

pub const Bandwidth = struct {
    /// Global download cap in bytes/sec; 0 means no cap. Per-server caps
    /// apply on top: effective rate = min(global, per-server).
    global_bytes_per_sec: i64 = 0,
};

/// A `Config` populated with the built-in defaults.
///
/// The API key is left empty; `loadOrCreate` generates one when it
/// persists a fresh config.
pub fn default() Config {
    return .{};
}

/// A loaded config plus the arena owning its strings.
pub const Loaded = struct {
    arena: *std.heap.ArenaAllocator,
    config: Config,

    pub fn deinit(l: Loaded) void {
        const gpa = l.arena.child_allocator;
        l.arena.deinit();
        gpa.destroy(l.arena);
    }
};

/// Operator-facing failure detail. `msg` points into `buf`, so a
/// `Diagnostic` must be used in place, never copied.
pub const Diagnostic = struct {
    buf: [512]u8 = undefined,
    msg: []const u8 = "",
    /// Position of a TOML syntax error, when that is what failed.
    toml: toml.Diagnostic = .{},

    fn set(d: *Diagnostic, comptime fmt: []const u8, args: anytype) void {
        d.msg = std.fmt.bufPrint(&d.buf, fmt, args) catch &d.buf;
    }
};

pub const ValidateError = error{
    EmptyListen,
    EmptyDataDir,
    EmptyAPIKey,
    EmptySQLitePath,
    UnsupportedBackend,
    EmptyIncompleteDir,
    EmptyCompleteDir,
    UnsupportedLogLevel,
    URLBaseNotRooted,
    URLBaseTrailingSlash,
    NegativeBandwidth,
};

pub const DecodeError = error{ TypeMismatch, ValueOutOfRange } || Allocator.Error;

pub const LoadError = error{
    ReadFailed,
    WriteFailed,
    ParseFailed,
    EntropyUnavailable,
    WorkingDirectoryUnavailable,
} || DecodeError || ValidateError;

/// Largest config file we will read. A config that big is a mistake.
const max_config_bytes = 1 << 20;

/// Read the config from `dir`/`sub_path`, or write a fresh one with a
/// generated API key when the file does not exist. Environment overrides
/// are folded in afterwards, then the result is normalized and validated.
pub fn loadOrCreate(
    gpa: Allocator,
    io: std.Io,
    dir: std.Io.Dir,
    sub_path: []const u8,
    opts: Options,
) LoadError!Loaded {
    const arena_ptr = try gpa.create(std.heap.ArenaAllocator);
    errdefer gpa.destroy(arena_ptr);
    arena_ptr.* = .init(gpa);
    errdefer arena_ptr.deinit();
    const arena = arena_ptr.allocator();

    var cfg = default();

    if (dir.readFileAlloc(io, sub_path, arena, .limited(max_config_bytes))) |src| {
        var tdiag: toml.Diagnostic = .{};
        var parsed = toml.parse(gpa, src, &tdiag) catch |err| {
            if (opts.diag) |d| {
                d.toml = tdiag;
                d.set("{s}: {t} at line {d} column {d}", .{ sub_path, err, tdiag.line, tdiag.column });
            }
            return error.ParseFailed;
        };
        defer parsed.deinit();
        try decodeTable(Config, "", arena, parsed.root, &cfg, opts.diag);
    } else |err| switch (err) {
        error.FileNotFound => {
            cfg.auth.api_key = try generateApiKey(arena, io);
            // Written before the environment is folded in, exactly like
            // the Go implementation: the file records the generated key
            // even when HOARDARR_API_KEY is also set.
            save(gpa, io, dir, sub_path, cfg) catch |werr| {
                if (opts.diag) |d| d.set("write fresh config {s}: {t}", .{ sub_path, werr });
                return error.WriteFailed;
            };
        },
        error.OutOfMemory => return error.OutOfMemory,
        else => {
            if (opts.diag) |d| d.set("read config {s}: {t}", .{ sub_path, err });
            return error.ReadFailed;
        },
    }

    if (opts.env) |env| try applyEnvOverrides(arena, &cfg, env);

    const base = opts.base_dir orelse std.process.currentPathAlloc(io, arena) catch |err| {
        if (opts.diag) |d| d.set("resolve working directory: {t}", .{err});
        return error.WorkingDirectoryUnavailable;
    };
    try normalize(arena, &cfg, base);
    try validate(&cfg, opts.diag);

    return .{ .arena = arena_ptr, .config = cfg };
}

pub const Options = struct {
    /// Environment to take `HOARDARR_*` overrides from. Null applies no
    /// overrides at all, which is what most tests want.
    env: ?*const Env = null,
    /// Directory relative paths resolve against. Null means the process
    /// working directory.
    base_dir: ?[]const u8 = null,
    diag: ?*Diagnostic = null,
};

/// Check invariants, returning the first violation. Runs after
/// `normalize`, so paths are absolute and derived defaults are filled in.
pub fn validate(c: *const Config, diag: ?*Diagnostic) ValidateError!void {
    if (c.server.listen.len == 0) return fail(diag, error.EmptyListen, "server.listen must not be empty", .{});
    if (c.server.data_dir.len == 0) return fail(diag, error.EmptyDataDir, "server.data_dir must not be empty", .{});
    if (c.auth.api_key.len == 0) return fail(diag, error.EmptyAPIKey, "auth.api_key must not be empty", .{});

    if (std.mem.eql(u8, c.storage.backend, "sqlite")) {
        if (c.storage.sqlite.path.len == 0) {
            return fail(diag, error.EmptySQLitePath, "storage.sqlite.path must not be empty after normalize", .{});
        }
    } else {
        return fail(diag, error.UnsupportedBackend, "storage.backend \"{s}\" is not supported (valid: sqlite)", .{c.storage.backend});
    }

    if (c.paths.incomplete_dir.len == 0) {
        return fail(diag, error.EmptyIncompleteDir, "paths.incomplete_dir must not be empty after normalize", .{});
    }
    if (c.paths.complete_dir.len == 0) {
        return fail(diag, error.EmptyCompleteDir, "paths.complete_dir must not be empty after normalize", .{});
    }

    const lvl = c.server.log_level;
    const level_ok = std.mem.eql(u8, lvl, "debug") or std.mem.eql(u8, lvl, "info") or
        std.mem.eql(u8, lvl, "warn") or std.mem.eql(u8, lvl, "error");
    if (!level_ok) {
        return fail(diag, error.UnsupportedLogLevel, "server.log_level \"{s}\" is not supported (valid: debug, info, warn, error)", .{lvl});
    }

    if (c.server.url_base.len != 0) {
        if (c.server.url_base[0] != '/') {
            return fail(diag, error.URLBaseNotRooted, "server.url_base \"{s}\" must start with /", .{c.server.url_base});
        }
        if (c.server.url_base[c.server.url_base.len - 1] == '/') {
            return fail(diag, error.URLBaseTrailingSlash, "server.url_base \"{s}\" must not end with /", .{c.server.url_base});
        }
    }

    if (c.bandwidth.global_bytes_per_sec < 0) {
        return fail(diag, error.NegativeBandwidth, "bandwidth.global_bytes_per_sec {d} must be >= 0", .{c.bandwidth.global_bytes_per_sec});
    }
}

fn fail(diag: ?*Diagnostic, err: ValidateError, comptime fmt: []const u8, args: anytype) ValidateError {
    if (diag) |d| d.set(fmt, args);
    return err;
}

/// Resolve relative paths against the data dir, fill in derived defaults,
/// and make every path absolute. Idempotent.
pub fn normalize(arena: Allocator, c: *Config, base_dir: []const u8) Allocator.Error!void {
    c.server.data_dir = try std.fs.path.resolve(arena, &.{ base_dir, c.server.data_dir });
    const data_dir = c.server.data_dir;

    c.storage.sqlite.path = try under(arena, data_dir, c.storage.sqlite.path, "hoardarr.db");
    c.paths.incomplete_dir = try under(arena, data_dir, c.paths.incomplete_dir, "incomplete");
    c.paths.complete_dir = try under(arena, data_dir, c.paths.complete_dir, "complete");

    if (c.server.log_level.len == 0) c.server.log_level = "info";
}

fn under(arena: Allocator, data_dir: []const u8, value: []const u8, fallback: []const u8) Allocator.Error![]const u8 {
    if (value.len == 0) return std.fs.path.resolve(arena, &.{ data_dir, fallback });
    if (std.fs.path.isAbsolute(value)) return value;
    return std.fs.path.resolve(arena, &.{ data_dir, value });
}

/// Fold `HOARDARR_*` variables into `c`. Env beats file; this function is
/// the canonical list of supported variables.
fn applyEnvOverrides(arena: Allocator, c: *Config, env: *const Env) Allocator.Error!void {
    if (nonEmpty(env, "HOARDARR_LISTEN")) |v| c.server.listen = try arena.dupe(u8, v);
    if (nonEmpty(env, "HOARDARR_DATA_DIR")) |v| c.server.data_dir = try arena.dupe(u8, v);
    if (nonEmpty(env, "HOARDARR_API_KEY")) |v| c.auth.api_key = try arena.dupe(u8, v);
    if (nonEmpty(env, "HOARDARR_STORAGE_BACKEND")) |v| c.storage.backend = try lower(arena, v);
    if (nonEmpty(env, "HOARDARR_SQLITE_PATH")) |v| c.storage.sqlite.path = try arena.dupe(u8, v);
    if (nonEmpty(env, "HOARDARR_INCOMPLETE_DIR")) |v| c.paths.incomplete_dir = try arena.dupe(u8, v);
    if (nonEmpty(env, "HOARDARR_COMPLETE_DIR")) |v| c.paths.complete_dir = try arena.dupe(u8, v);
    if (nonEmpty(env, "HOARDARR_LOG_LEVEL")) |v| c.server.log_level = try lower(arena, v);

    // Set unconditionally, empty included, so an operator can clear a
    // url_base that the config file sets.
    if (env.get("HOARDARR_URL_BASE")) |v| {
        c.server.url_base = try arena.dupe(u8, std.mem.trimEnd(u8, v, "/"));
    }

    // A malformed or negative value is ignored rather than fatal: the
    // rest of the config is still perfectly usable.
    if (nonEmpty(env, "HOARDARR_BANDWIDTH_GLOBAL")) |v| {
        if (std.fmt.parseInt(i64, v, 10)) |n| {
            if (n >= 0) c.bandwidth.global_bytes_per_sec = n;
        } else |_| {}
    }
}

fn nonEmpty(env: *const Env, key: []const u8) ?[]const u8 {
    const v = env.get(key) orelse return null;
    return if (v.len == 0) null else v;
}

fn lower(arena: Allocator, v: []const u8) Allocator.Error![]const u8 {
    const out = try arena.dupe(u8, v);
    return std.ascii.lowerString(out, out);
}

/// 32 hex characters from 16 bytes of fresh entropy, matching the
/// SABnzbd / Sonarr / Radarr key format.
fn generateApiKey(arena: Allocator, io: std.Io) error{ EntropyUnavailable, OutOfMemory }![]const u8 {
    var raw: [16]u8 = undefined;
    io.randomSecure(&raw) catch return error.EntropyUnavailable;
    return arena.dupe(u8, &std.fmt.bytesToHex(raw, .lower));
}

// ------------------------------------------------------------- decoding

/// TOML key for a field: the field name, unless the struct declares a
/// `toml_names` mapping that renames it.
fn tomlName(comptime T: type, comptime field: []const u8) []const u8 {
    if (@hasDecl(T, "toml_names") and @hasField(@TypeOf(T.toml_names), field)) {
        return @field(T.toml_names, field);
    }
    return field;
}

/// Copy `tbl` into `out` by walking `T`'s fields.
///
/// Keys with no matching field are ignored, which is what the Go
/// implementation did: an operator upgrading across a setting rename must
/// not be locked out of their own instance by a stale key.
fn decodeTable(
    comptime T: type,
    comptime prefix: []const u8,
    arena: Allocator,
    tbl: *const toml.Table,
    out: *T,
    diag: ?*Diagnostic,
) DecodeError!void {
    inline for (std.meta.fields(T)) |f| {
        const key = comptime tomlName(T, f.name);
        const full = prefix ++ key;
        if (tbl.get(key)) |v| {
            const dst = &@field(out.*, f.name);
            switch (@typeInfo(f.type)) {
                .@"struct" => switch (v) {
                    .table => |sub| try decodeTable(f.type, full ++ ".", arena, sub, dst, diag),
                    else => return mismatch(diag, full, "table", v),
                },
                .bool => switch (v) {
                    .boolean => |b| dst.* = b,
                    else => return mismatch(diag, full, "boolean", v),
                },
                .int => switch (v) {
                    .integer => |n| dst.* = std.math.cast(f.type, n) orelse {
                        if (diag) |d| d.set("config key {s}: {d} does not fit {s}", .{ full, n, @typeName(f.type) });
                        return error.ValueOutOfRange;
                    },
                    else => return mismatch(diag, full, "integer", v),
                },
                .float => switch (v) {
                    .float => |x| dst.* = @floatCast(x),
                    // An operator writing `0` where a float is expected
                    // means 0.0, not a type error.
                    .integer => |n| dst.* = @floatFromInt(n),
                    else => return mismatch(diag, full, "float", v),
                },
                .pointer => |ptr| {
                    comptime std.debug.assert(ptr.size == .slice and ptr.child == u8);
                    switch (v) {
                        .string => |s| dst.* = try arena.dupe(u8, s),
                        else => return mismatch(diag, full, "string", v),
                    }
                },
                else => @compileError("config field " ++ full ++ ": unsupported type " ++ @typeName(f.type)),
            }
        }
    }
}

fn mismatch(diag: ?*Diagnostic, key: []const u8, want: []const u8, got: toml.Value) DecodeError {
    if (diag) |d| d.set("config key {s}: expected {s}, found {t}", .{ key, want, got });
    return error.TypeMismatch;
}

// ------------------------------------------------------------- encoding

pub const header =
    \\# hoardarr configuration.
    \\#
    \\# This file was created automatically on first run. Edit freely; values
    \\# here override the built-in defaults. Environment variables (HOARDARR_*)
    \\# override values in this file.
    \\#
    \\# The api_key was randomly generated. Treat it as a secret. Configure
    \\# Sonarr / Radarr / Lidarr / Readarr / Prowlarr (or SABnzbd-compatible
    \\# clients) with this key under the "API Key" field.
    \\
    \\
;

/// Render `cfg` as a complete config file, header included. Caller owns
/// the result.
pub fn render(gpa: Allocator, cfg: Config) Allocator.Error![]u8 {
    var out: std.Io.Writer.Allocating = .init(gpa);
    defer out.deinit();
    out.writer.writeAll(header) catch return error.OutOfMemory;
    encodeTable(&out.writer, Config, cfg, "", 0) catch return error.OutOfMemory;
    return out.toOwnedSlice();
}

/// Persist `cfg`. The parent directory is created if missing; the file is
/// written 0600 because it holds the API key.
pub fn save(
    gpa: Allocator,
    io: std.Io,
    dir: std.Io.Dir,
    sub_path: []const u8,
    cfg: Config,
) (Allocator.Error || std.Io.Dir.WriteFileError || std.Io.Dir.CreateDirPathError)!void {
    const text = try render(gpa, cfg);
    defer gpa.free(text);

    if (std.fs.path.dirname(sub_path)) |parent| try dir.createDirPath(io, parent);
    try dir.writeFile(io, .{
        .sub_path = sub_path,
        .data = text,
        .flags = .{ .permissions = .fromMode(0o600) },
    });
}

fn encodeTable(
    w: *std.Io.Writer,
    comptime T: type,
    v: T,
    comptime prefix: []const u8,
    comptime depth: usize,
) std.Io.Writer.Error!void {
    // Scalars first: a key written after a `[sub.table]` header would
    // belong to that table, not to this one.
    inline for (std.meta.fields(T)) |f| {
        if (@typeInfo(f.type) != .@"struct") {
            try w.splatByteAll(' ', depth * 2);
            try w.print("{s} = ", .{comptime tomlName(T, f.name)});
            try encodeScalar(w, f.type, @field(v, f.name));
            try w.writeByte('\n');
        }
    }
    comptime var tables: usize = 0;
    inline for (std.meta.fields(T)) |f| {
        if (@typeInfo(f.type) == .@"struct") {
            const path = comptime if (prefix.len == 0) tomlName(T, f.name) else prefix ++ "." ++ tomlName(T, f.name);
            // Blank line between top-level sections only, which is what
            // the file hoardarr has been writing since day one looks like.
            if (depth == 0 and tables != 0) try w.writeByte('\n');
            tables += 1;
            try w.splatByteAll(' ', depth * 2);
            try w.print("[{s}]\n", .{path});
            try encodeTable(w, f.type, @field(v, f.name), path, depth + 1);
        }
    }
}

fn encodeScalar(w: *std.Io.Writer, comptime T: type, v: T) std.Io.Writer.Error!void {
    switch (@typeInfo(T)) {
        .bool => try w.writeAll(if (v) "true" else "false"),
        .int => try w.print("{d}", .{v}),
        .float => {
            // TOML floats need a fraction or an exponent, or they read
            // back as integers.
            var buf: [64]u8 = undefined;
            const s = std.fmt.bufPrint(&buf, "{d}", .{v}) catch unreachable;
            try w.writeAll(s);
            if (std.mem.indexOfAny(u8, s, ".eE") == null) try w.writeAll(".0");
        },
        .pointer => try encodeString(w, v),
        else => @compileError("cannot encode " ++ @typeName(T)),
    }
}

fn encodeString(w: *std.Io.Writer, s: []const u8) std.Io.Writer.Error!void {
    try w.writeByte('"');
    for (s) |c| switch (c) {
        '"' => try w.writeAll("\\\""),
        '\\' => try w.writeAll("\\\\"),
        '\n' => try w.writeAll("\\n"),
        '\r' => try w.writeAll("\\r"),
        '\t' => try w.writeAll("\\t"),
        0x08 => try w.writeAll("\\b"),
        0x0C => try w.writeAll("\\f"),
        0x00...0x07, 0x0B, 0x0E...0x1F, 0x7F => try w.print("\\u{X:0>4}", .{c}),
        else => try w.writeByte(c),
    };
    try w.writeByte('"');
}

// ---------------------------------------------------------------- tests

const t = std.testing;

fn isHex(s: []const u8) bool {
    for (s) |c| if (!std.ascii.isHex(c)) return false;
    return true;
}

fn write(dir: std.Io.Dir, body: []const u8) !void {
    try dir.writeFile(t.io, .{ .sub_path = "config.toml", .data = body });
}

test "defaults" {
    const c = default();
    try t.expectEqualStrings(":8085", c.server.listen);
    try t.expectEqualStrings("./data", c.server.data_dir);
    try t.expectEqualStrings("info", c.server.log_level);
    try t.expectEqualStrings("", c.server.url_base);
    try t.expectEqual(@as(i64, 1), c.server.max_concurrent_jobs);
    try t.expectEqual(@as(f64, 0), c.server.fail_hopeless_ratio);
    try t.expectEqual(false, c.server.defer_recovery_vols);
    try t.expectEqual(true, c.server.delete_samples);
    try t.expectEqual(true, c.server.collapse_single_folder);
    try t.expectEqualStrings("sqlite", c.storage.backend);
    try t.expectEqualStrings("", c.storage.sqlite.path);
    try t.expectEqualStrings("", c.paths.incomplete_dir);
    try t.expectEqualStrings("", c.paths.complete_dir);
    try t.expectEqual(@as(i64, 0), c.bandwidth.global_bytes_per_sec);
    // Filled in by loadOrCreate, not by the defaults.
    try t.expectEqualStrings("", c.auth.api_key);
}

test "loadOrCreate writes a fresh config and reuses it" {
    var tmp = t.tmpDir(.{});
    defer tmp.cleanup();

    var loaded = try loadOrCreate(t.allocator, t.io, tmp.dir, "config.toml", .{});
    defer loaded.deinit();
    const cfg = loaded.config;

    _ = try tmp.dir.statFile(t.io, "config.toml", .{});

    try t.expectEqual(@as(usize, 32), cfg.auth.api_key.len);
    try t.expect(isHex(cfg.auth.api_key));

    try t.expect(std.fs.path.isAbsolute(cfg.server.data_dir));
    try t.expect(std.fs.path.isAbsolute(cfg.storage.sqlite.path));
    try t.expect(std.fs.path.isAbsolute(cfg.paths.incomplete_dir));
    try t.expect(std.fs.path.isAbsolute(cfg.paths.complete_dir));

    // Reload: the persisted key must come back, not a new one.
    var again = try loadOrCreate(t.allocator, t.io, tmp.dir, "config.toml", .{});
    defer again.deinit();
    try t.expectEqualStrings(cfg.auth.api_key, again.config.auth.api_key);
}

test "loadOrCreate creates missing parent directories" {
    var tmp = t.tmpDir(.{});
    defer tmp.cleanup();

    var loaded = try loadOrCreate(t.allocator, t.io, tmp.dir, "nested/deeper/config.toml", .{});
    defer loaded.deinit();
    _ = try tmp.dir.statFile(t.io, "nested/deeper/config.toml", .{});
}

test "loadOrCreate reads an existing file and resolves relative paths" {
    var tmp = t.tmpDir(.{});
    defer tmp.cleanup();
    try write(tmp.dir,
        \\[server]
        \\listen = ":1234"
        \\data_dir = "./localdata"
        \\
        \\[auth]
        \\api_key = "deadbeefdeadbeefdeadbeefdeadbeef"
        \\
        \\[storage]
        \\backend = "sqlite"
        \\
        \\[storage.sqlite]
        \\path = "custom.db"
        \\
        \\[paths]
        \\incomplete_dir = "tmp_in"
        \\complete_dir = "tmp_out"
        \\
    );

    var loaded = try loadOrCreate(t.allocator, t.io, tmp.dir, "config.toml", .{});
    defer loaded.deinit();
    const cfg = loaded.config;

    try t.expectEqualStrings(":1234", cfg.server.listen);
    try t.expectEqualStrings("deadbeefdeadbeefdeadbeefdeadbeef", cfg.auth.api_key);
    try t.expect(std.mem.endsWith(u8, cfg.server.data_dir, "localdata"));

    for ([_][2][]const u8{
        .{ cfg.storage.sqlite.path, "custom.db" },
        .{ cfg.paths.incomplete_dir, "tmp_in" },
        .{ cfg.paths.complete_dir, "tmp_out" },
    }) |pair| {
        const want = try std.fs.path.join(t.allocator, &.{ cfg.server.data_dir, pair[1] });
        defer t.allocator.free(want);
        try t.expectEqualStrings(want, pair[0]);
    }
}

test "loadOrCreate keeps every setting the file specifies" {
    var tmp = t.tmpDir(.{});
    defer tmp.cleanup();
    try write(tmp.dir,
        \\[server]
        \\listen = "0.0.0.0:9000"
        \\data_dir = "/srv/hoardarr"
        \\log_level = "debug"
        \\url_base = "/apps/hoardarr"
        \\max_concurrent_jobs = 4
        \\fail_hopeless_ratio = 0.05
        \\defer_recovery_vols = true
        \\delete_samples = false
        \\collapse_single_folder = false
        \\
        \\[auth]
        \\api_key = "k"
        \\
        \\[bandwidth]
        \\global_bytes_per_sec = 1048576
        \\
    );

    var loaded = try loadOrCreate(t.allocator, t.io, tmp.dir, "config.toml", .{});
    defer loaded.deinit();
    const s = loaded.config.server;

    try t.expectEqualStrings("0.0.0.0:9000", s.listen);
    try t.expectEqualStrings("/srv/hoardarr", s.data_dir);
    try t.expectEqualStrings("debug", s.log_level);
    try t.expectEqualStrings("/apps/hoardarr", s.url_base);
    try t.expectEqual(@as(i64, 4), s.max_concurrent_jobs);
    try t.expectEqual(@as(f64, 0.05), s.fail_hopeless_ratio);
    try t.expectEqual(true, s.defer_recovery_vols);
    try t.expectEqual(false, s.delete_samples);
    try t.expectEqual(false, s.collapse_single_folder);
    try t.expectEqual(@as(i64, 1048576), loaded.config.bandwidth.global_bytes_per_sec);
    try t.expectEqualStrings("/srv/hoardarr/hoardarr.db", loaded.config.storage.sqlite.path);
}

test "environment overrides the file" {
    var tmp = t.tmpDir(.{});
    defer tmp.cleanup();
    try write(tmp.dir,
        \\[server]
        \\listen = ":8085"
        \\data_dir = "./data"
        \\url_base = "/fromfile"
        \\
        \\[auth]
        \\api_key = "filebasedkey00000000000000000000"
        \\
        \\[storage]
        \\backend = "sqlite"
        \\
    );

    var env = Env.init(t.allocator);
    defer env.deinit();
    try env.put("HOARDARR_LISTEN", ":9999");
    try env.put("HOARDARR_API_KEY", "envoverridekey00000000000000000");
    try env.put("HOARDARR_DATA_DIR", "/srv/envdata");
    try env.put("HOARDARR_STORAGE_BACKEND", "SQLite");
    try env.put("HOARDARR_SQLITE_PATH", "env.db");
    try env.put("HOARDARR_INCOMPLETE_DIR", "env_in");
    try env.put("HOARDARR_COMPLETE_DIR", "/abs/env_out");
    try env.put("HOARDARR_LOG_LEVEL", "WARN");
    try env.put("HOARDARR_URL_BASE", "/envbase/");
    try env.put("HOARDARR_BANDWIDTH_GLOBAL", "2048");

    var loaded = try loadOrCreate(t.allocator, t.io, tmp.dir, "config.toml", .{ .env = &env });
    defer loaded.deinit();
    const cfg = loaded.config;

    try t.expectEqualStrings(":9999", cfg.server.listen);
    try t.expectEqualStrings("envoverridekey00000000000000000", cfg.auth.api_key);
    try t.expectEqualStrings("/srv/envdata", cfg.server.data_dir);
    try t.expectEqualStrings("sqlite", cfg.storage.backend);
    try t.expectEqualStrings("/srv/envdata/env.db", cfg.storage.sqlite.path);
    try t.expectEqualStrings("/srv/envdata/env_in", cfg.paths.incomplete_dir);
    try t.expectEqualStrings("/abs/env_out", cfg.paths.complete_dir);
    try t.expectEqualStrings("warn", cfg.server.log_level);
    try t.expectEqualStrings("/envbase", cfg.server.url_base);
    try t.expectEqual(@as(i64, 2048), cfg.bandwidth.global_bytes_per_sec);
}

test "empty environment values do not override, except url_base" {
    var tmp = t.tmpDir(.{});
    defer tmp.cleanup();
    try write(tmp.dir,
        \\[server]
        \\listen = ":1234"
        \\url_base = "/fromfile"
        \\
        \\[auth]
        \\api_key = "k"
        \\
    );

    var env = Env.init(t.allocator);
    defer env.deinit();
    try env.put("HOARDARR_LISTEN", "");
    try env.put("HOARDARR_API_KEY", "");
    try env.put("HOARDARR_BANDWIDTH_GLOBAL", "not-a-number");
    try env.put("HOARDARR_URL_BASE", "");

    var loaded = try loadOrCreate(t.allocator, t.io, tmp.dir, "config.toml", .{ .env = &env });
    defer loaded.deinit();

    try t.expectEqualStrings(":1234", loaded.config.server.listen);
    try t.expectEqualStrings("k", loaded.config.auth.api_key);
    try t.expectEqual(@as(i64, 0), loaded.config.bandwidth.global_bytes_per_sec);
    // Env clears a file-provided url_base; that is the one override that
    // applies when empty.
    try t.expectEqualStrings("", loaded.config.server.url_base);
}

test "negative bandwidth in the environment is ignored" {
    var tmp = t.tmpDir(.{});
    defer tmp.cleanup();
    try write(tmp.dir, "[auth]\napi_key = \"k\"\n\n[bandwidth]\nglobal_bytes_per_sec = 512\n");

    var env = Env.init(t.allocator);
    defer env.deinit();
    try env.put("HOARDARR_BANDWIDTH_GLOBAL", "-1");

    var loaded = try loadOrCreate(t.allocator, t.io, tmp.dir, "config.toml", .{ .env = &env });
    defer loaded.deinit();
    try t.expectEqual(@as(i64, 512), loaded.config.bandwidth.global_bytes_per_sec);
}

test "validate rejects an unsupported backend and names it" {
    var arena: std.heap.ArenaAllocator = .init(t.allocator);
    defer arena.deinit();

    var c = default();
    c.auth.api_key = "x";
    c.storage.backend = "mysql";
    try normalize(arena.allocator(), &c, "/base");

    var diag: Diagnostic = .{};
    try t.expectError(error.UnsupportedBackend, validate(&c, &diag));
    try t.expect(std.mem.indexOf(u8, diag.msg, "mysql") != null);
}

test "validate rejects a missing api key" {
    var arena: std.heap.ArenaAllocator = .init(t.allocator);
    defer arena.deinit();

    var c = default();
    try normalize(arena.allocator(), &c, "/base");
    try t.expectError(error.EmptyAPIKey, validate(&c, null));
}

test "validate rules for listen, log level, url_base and bandwidth" {
    var arena: std.heap.ArenaAllocator = .init(t.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    var base = default();
    base.auth.api_key = "x";
    try normalize(alloc, &base, "/base");
    try validate(&base, null);

    {
        var c = base;
        c.server.listen = "";
        try t.expectError(error.EmptyListen, validate(&c, null));
    }
    {
        var c = base;
        c.server.log_level = "verbose";
        try t.expectError(error.UnsupportedLogLevel, validate(&c, null));
    }
    for ([_][]const u8{ "debug", "info", "warn", "error" }) |lvl| {
        var c = base;
        c.server.log_level = lvl;
        try validate(&c, null);
    }
    {
        var c = base;
        c.server.url_base = "hoardarr";
        try t.expectError(error.URLBaseNotRooted, validate(&c, null));
    }
    {
        var c = base;
        c.server.url_base = "/hoardarr/";
        try t.expectError(error.URLBaseTrailingSlash, validate(&c, null));
    }
    for ([_][]const u8{ "", "/hoardarr", "/apps/hoardarr" }) |ub| {
        var c = base;
        c.server.url_base = ub;
        try validate(&c, null);
    }
    {
        var c = base;
        c.bandwidth.global_bytes_per_sec = -1;
        try t.expectError(error.NegativeBandwidth, validate(&c, null));
    }
}

test "normalize is idempotent" {
    var arena: std.heap.ArenaAllocator = .init(t.allocator);
    defer arena.deinit();
    const alloc = arena.allocator();

    var c = default();
    c.auth.api_key = "x";
    try normalize(alloc, &c, "/base");
    const first = c;
    try normalize(alloc, &c, "/base");

    try t.expectEqualStrings(first.server.data_dir, c.server.data_dir);
    try t.expectEqualStrings(first.storage.sqlite.path, c.storage.sqlite.path);
    try t.expectEqualStrings(first.paths.incomplete_dir, c.paths.incomplete_dir);
    try t.expectEqualStrings(first.paths.complete_dir, c.paths.complete_dir);
    try t.expectEqualStrings(first.server.log_level, c.server.log_level);
}

test "normalize derives the default paths from the data dir" {
    var arena: std.heap.ArenaAllocator = .init(t.allocator);
    defer arena.deinit();

    var c = default();
    c.server.log_level = "";
    try normalize(arena.allocator(), &c, "/base");

    try t.expectEqualStrings("/base/data", c.server.data_dir);
    try t.expectEqualStrings("/base/data/hoardarr.db", c.storage.sqlite.path);
    try t.expectEqualStrings("/base/data/incomplete", c.paths.incomplete_dir);
    try t.expectEqualStrings("/base/data/complete", c.paths.complete_dir);
    try t.expectEqualStrings("info", c.server.log_level);
}

test "generated api keys are 32 hex chars and never repeat" {
    var arena: std.heap.ArenaAllocator = .init(t.allocator);
    defer arena.deinit();

    const a = try generateApiKey(arena.allocator(), t.io);
    const b = try generateApiKey(arena.allocator(), t.io);
    try t.expectEqual(@as(usize, 32), a.len);
    try t.expect(isHex(a));
    try t.expect(!std.mem.eql(u8, a, b));
}

test "unknown keys are ignored, wrong types are not" {
    var tmp = t.tmpDir(.{});
    defer tmp.cleanup();
    try write(tmp.dir, "[auth]\napi_key = \"k\"\n\n[server]\nlisten = \":1\"\nfrom_a_future_release = 42\n");

    var loaded = try loadOrCreate(t.allocator, t.io, tmp.dir, "config.toml", .{});
    defer loaded.deinit();
    try t.expectEqualStrings(":1", loaded.config.server.listen);

    try write(tmp.dir, "[auth]\napi_key = \"k\"\n\n[server]\nlisten = 1234\n");
    var diag: Diagnostic = .{};
    try t.expectError(error.TypeMismatch, loadOrCreate(t.allocator, t.io, tmp.dir, "config.toml", .{ .diag = &diag }));
    try t.expect(std.mem.indexOf(u8, diag.msg, "server.listen") != null);
}

test "a broken config file reports where it broke" {
    var tmp = t.tmpDir(.{});
    defer tmp.cleanup();
    try write(tmp.dir, "[server]\nlisten = \":1\"\noops\n");

    var diag: Diagnostic = .{};
    try t.expectError(error.ParseFailed, loadOrCreate(t.allocator, t.io, tmp.dir, "config.toml", .{ .diag = &diag }));
    try t.expectEqual(@as(usize, 3), diag.toml.line);
}

test "render round-trips through the parser" {
    var cfg = default();
    cfg.auth.api_key = "564f540f59f92fd5dfd7714d6182a1b7";
    cfg.server.data_dir = "/srv/hoardarr/data";
    cfg.server.url_base = "/hoardarr";
    cfg.server.fail_hopeless_ratio = 0.05;
    cfg.server.max_concurrent_jobs = 3;
    cfg.server.defer_recovery_vols = true;
    cfg.storage.sqlite.path = "/srv/hoardarr/data/hoardarr.db";
    cfg.paths.incomplete_dir = "/srv/hoardarr/data/incomplete";
    cfg.paths.complete_dir = "/srv/hoardarr/data/complete";
    cfg.bandwidth.global_bytes_per_sec = 1024;

    const text = try render(t.allocator, cfg);
    defer t.allocator.free(text);
    try t.expect(std.mem.startsWith(u8, text, "# hoardarr configuration."));
    try t.expect(std.mem.indexOf(u8, text, "\n[server]\n") != null);
    try t.expect(std.mem.indexOf(u8, text, "\n  [storage.sqlite]\n") != null);

    var parsed = try toml.parse(t.allocator, text, null);
    defer parsed.deinit();

    var round = default();
    var arena: std.heap.ArenaAllocator = .init(t.allocator);
    defer arena.deinit();
    try decodeTable(Config, "", arena.allocator(), parsed.root, &round, null);

    try t.expectEqualDeep(cfg, round);
}

test "render escapes strings that would break the file" {
    var cfg = default();
    cfg.auth.api_key = "quote:\" back:\\ tab:\t";

    const text = try render(t.allocator, cfg);
    defer t.allocator.free(text);

    var parsed = try toml.parse(t.allocator, text, null);
    defer parsed.deinit();
    try t.expectEqualStrings(cfg.auth.api_key, parsed.root.getTable("auth").?.getString("api_key").?);
}

test "toml_names remaps a key whose TOML spelling differs" {
    const Renamed = struct {
        zig_side: []const u8 = "",
        plain: i64 = 0,

        pub const toml_names = .{ .zig_side = "toml-side" };
    };

    var parsed = try toml.parse(t.allocator, "\"toml-side\" = \"v\"\nplain = 7\nzig_side = \"ignored\"\n", null);
    defer parsed.deinit();

    var arena: std.heap.ArenaAllocator = .init(t.allocator);
    defer arena.deinit();

    var out: Renamed = .{};
    try decodeTable(Renamed, "", arena.allocator(), parsed.root, &out, null);
    try t.expectEqualStrings("v", out.zig_side);
    try t.expectEqual(@as(i64, 7), out.plain);
}

test "save then load preserves every value" {
    var tmp = t.tmpDir(.{});
    defer tmp.cleanup();

    var cfg = default();
    cfg.auth.api_key = "abc123";
    cfg.server.data_dir = "/srv/hoardarr";
    cfg.server.log_level = "warn";
    cfg.server.max_concurrent_jobs = 0;
    cfg.server.fail_hopeless_ratio = 0.1;
    cfg.server.delete_samples = false;
    try save(t.allocator, t.io, tmp.dir, "config.toml", cfg);

    var loaded = try loadOrCreate(t.allocator, t.io, tmp.dir, "config.toml", .{});
    defer loaded.deinit();

    try t.expectEqualStrings("abc123", loaded.config.auth.api_key);
    try t.expectEqualStrings("/srv/hoardarr", loaded.config.server.data_dir);
    try t.expectEqualStrings("warn", loaded.config.server.log_level);
    try t.expectEqual(@as(i64, 0), loaded.config.server.max_concurrent_jobs);
    try t.expectEqual(@as(f64, 0.1), loaded.config.server.fail_hopeless_ratio);
    try t.expectEqual(false, loaded.config.server.delete_samples);
}
