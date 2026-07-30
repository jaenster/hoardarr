//! `hoardarr server add|list|rm` — Usenet providers from a shell.
//!
//! Same trade as `download.zig`: Go opened the database, this talks to
//! the running daemon over `/api/v1/servers`. See `api.zig`'s module
//! comment for why. The visible gain beyond correctness is that a server
//! added here is live immediately — the daemon reloads its registry from
//! the same call — where the Go version's edit only took effect on the
//! next restart.
//!
//! Passwords go in and never come back out: `--pass` is sent in the JSON
//! body, and `GET /api/v1/servers` has no field that could return one, so
//! `list` cannot print one even by accident.

const std = @import("std");
const api = @import("api.zig");
const config = @import("../core/config.zig");
const json = @import("../api/rest/json.zig");

const Allocator = std.mem.Allocator;

pub const usage =
    \\Usage:
    \\  hoardarr server add --name X --host Y [--port 563] [--no-tls]
    \\                      [--user U] [--pass P] [--conns N] [--priority N]
    \\  hoardarr server list
    \\  hoardarr server rm <id>
    \\
;

pub fn run(gpa: Allocator, env: std.process.Environ, args: []const []const u8) u8 {
    if (args.len == 0) return api.fail("server: subcommand required (add|list|rm)", .{});
    const sub = args[0];
    const rest = args[1..];

    if (std.mem.eql(u8, sub, "add")) return add(gpa, env, rest);
    if (std.mem.eql(u8, sub, "list") or std.mem.eql(u8, sub, "ls")) return list(gpa, env, rest);
    if (std.mem.eql(u8, sub, "rm") or
        std.mem.eql(u8, sub, "remove") or
        std.mem.eql(u8, sub, "delete")) return remove(gpa, env, rest);

    return api.fail("server: unknown subcommand \"{s}\"", .{sub});
}

// ---------------------------------------------------------------------
// add
// ---------------------------------------------------------------------

/// Go's flag defaults, unchanged: 563 is TLS-NNTP and TLS is on unless
/// explicitly disabled, because a plaintext provider connection leaks
/// every article id to the path.
pub const AddOptions = struct {
    name: []const u8 = "",
    host: []const u8 = "",
    port: u16 = 563,
    tls: bool = true,
    user: []const u8 = "",
    pass: []const u8 = "",
    conns: i64 = 8,
    priority: i64 = 0,
};

pub const ParseError = error{
    MissingValue,
    UnknownFlag,
    BadNumber,
    MissingRequired,
    UnexpectedArg,
};

/// Pure, so the whole flag grammar is tested without a daemon.
pub fn parseAdd(args: []const []const u8) ParseError!AddOptions {
    var o: AddOptions = .{};
    var i: usize = 0;
    while (i < args.len) : (i += 1) {
        const f = try Flag.split(args[i]);
        if (f.eq("no-tls")) {
            o.tls = false;
            continue;
        }
        // Every remaining flag takes a value, from `--f=v` or the next
        // argument. Doing the lookup once here rather than per flag is
        // what keeps the arm list readable.
        const v = f.value orelse blk: {
            i += 1;
            if (i >= args.len) return error.MissingValue;
            break :blk args[i];
        };
        if (f.eq("name")) {
            o.name = v;
        } else if (f.eq("host")) {
            o.host = v;
        } else if (f.eq("port")) {
            o.port = std.fmt.parseInt(u16, v, 10) catch return error.BadNumber;
            if (o.port == 0) return error.BadNumber;
        } else if (f.eq("user")) {
            o.user = v;
        } else if (f.eq("pass")) {
            o.pass = v;
        } else if (f.eq("conns")) {
            o.conns = std.fmt.parseInt(i64, v, 10) catch return error.BadNumber;
        } else if (f.eq("priority")) {
            o.priority = std.fmt.parseInt(i64, v, 10) catch return error.BadNumber;
        } else return error.UnknownFlag;
    }
    if (o.name.len == 0 or o.host.len == 0) return error.MissingRequired;
    return o;
}

/// One `--flag[=value]` argument, split once so the parser above does not
/// repeat the `=` handling per flag. Single-dash long flags are accepted
/// because Go's `flag` package accepted them and scripts use them.
const Flag = struct {
    name: []const u8,
    value: ?[]const u8,

    fn split(arg: []const u8) ParseError!Flag {
        var rest = arg;
        if (std.mem.startsWith(u8, rest, "--")) {
            rest = rest[2..];
        } else if (rest.len > 1 and rest[0] == '-') {
            rest = rest[1..];
        } else return error.UnexpectedArg;
        if (rest.len == 0) return error.UnknownFlag;

        if (std.mem.indexOfScalar(u8, rest, '=')) |at| {
            return .{ .name = rest[0..at], .value = rest[at + 1 ..] };
        }
        return .{ .name = rest, .value = null };
    }

    fn eq(self: Flag, want: []const u8) bool {
        return std.mem.eql(u8, self.name, want);
    }
};

/// The `POST /api/v1/servers` body. Separated from the request so a test
/// can assert the exact JSON without a socket.
pub fn addBody(gpa: Allocator, o: AddOptions) Allocator.Error![]u8 {
    // `json.Writer` is the same encoder the REST layer uses, so a string
    // that needs escaping is escaped by the code that already had to get
    // that right — a provider password is arbitrary bytes.
    var w = json.Writer.init(gpa);
    errdefer w.deinit();
    try w.beginObject();
    try w.strField("name", o.name);
    try w.strField("host", o.host);
    try w.intField("port", o.port);
    try w.boolField("tls", o.tls);
    try w.strField("username", o.user);
    try w.strField("password", o.pass);
    try w.intField("max_conns", o.conns);
    try w.intField("priority", o.priority);
    try w.endObject();
    const out = try gpa.dupe(u8, w.items());
    w.deinit();
    return out;
}

fn add(gpa: Allocator, env: std.process.Environ, args: []const []const u8) u8 {
    const o = parseAdd(args) catch |err| switch (err) {
        // Go's message, verbatim.
        error.MissingRequired => return api.fail("server add: --name and --host are required", .{}),
        error.MissingValue => return api.fail("server add: a flag is missing its value", .{}),
        error.BadNumber => return api.fail("server add: --port, --conns and --priority take numbers", .{}),
        error.UnknownFlag => return api.fail("server add: unknown flag", .{}),
        error.UnexpectedArg => return api.fail("server add: unexpected argument", .{}),
    };

    var ep = Connection.open(gpa, env, "server add") orelse return 1;
    defer ep.deinit();

    const body = addBody(gpa, o) catch return api.fail("server add: out of memory", .{});
    defer gpa.free(body);

    var resp = api.call(gpa, ep.endpoint, .{
        .method = .post,
        .path = "/api/v1/servers",
        .body = body,
    }) catch |err| return api.fail("server add: {t}", .{err});
    defer resp.deinit();

    if (resp.status < 200 or resp.status >= 300) {
        var buf: [512]u8 = undefined;
        return api.fail("server add: {s}", .{api.errorText(&resp, &buf)});
    }

    const id = intField(gpa, resp.body, "id") orelse 0;
    api.report("server added id={d} name={s} host={s} port={d} tls={}", .{
        id, o.name, o.host, o.port, o.tls,
    });
    return 0;
}

// ---------------------------------------------------------------------
// list
// ---------------------------------------------------------------------

fn list(gpa: Allocator, env: std.process.Environ, args: []const []const u8) u8 {
    if (args.len != 0) return api.fail("server list: takes no arguments", .{});

    var ep = Connection.open(gpa, env, "server list") orelse return 1;
    defer ep.deinit();

    var resp = api.call(gpa, ep.endpoint, .{ .path = "/api/v1/servers" }) catch |err|
        return api.fail("server list: {t}", .{err});
    defer resp.deinit();

    if (resp.status < 200 or resp.status >= 300) {
        var buf: [512]u8 = undefined;
        return api.fail("server list: {s}", .{api.errorText(&resp, &buf)});
    }

    const parsed = std.json.parseFromSlice(std.json.Value, gpa, resp.body, .{}) catch
        return api.fail("server list: unreadable response from the daemon", .{});
    defer parsed.deinit();
    if (parsed.value != .object) return api.fail("server list: unreadable response from the daemon", .{});
    const rows = switch (parsed.value.object.get("servers") orelse .null) {
        .array => |a| a.items,
        else => return api.fail("server list: unreadable response from the daemon", .{}),
    };

    if (rows.len == 0) {
        api.report("no servers configured", .{});
        return 0;
    }

    const table = renderTable(gpa, rows) catch
        return api.fail("server list: out of memory", .{});
    defer gpa.free(table);
    api.writeStdout(table);
    return 0;
}

/// Go used `text/tabwriter`; this pads to the widest cell per column,
/// which is the same output for the same input.
///
/// Column set and order are Go's, so a script parsing this by field index
/// keeps working: ID, NAME, HOST:PORT, TLS, CONNS, PRIORITY, ENABLED.
pub fn renderTable(gpa: Allocator, rows: []const std.json.Value) Allocator.Error![]u8 {
    const n_cols = 7;
    const headers = [n_cols][]const u8{ "ID", "NAME", "HOST:PORT", "TLS", "CONNS", "PRIORITY", "ENABLED" };

    var arena_state = std.heap.ArenaAllocator.init(gpa);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    var cells: std.ArrayList([n_cols][]const u8) = .empty;
    try cells.append(arena, headers);

    for (rows) |row| {
        if (row != .object) continue;
        const o = row.object;
        try cells.append(arena, .{
            try std.fmt.allocPrint(arena, "{d}", .{intOf(o, "id")}),
            strOf(o, "name"),
            try std.fmt.allocPrint(arena, "{s}:{d}", .{ strOf(o, "host"), intOf(o, "port") }),
            yesNo(boolOf(o, "tls")),
            try std.fmt.allocPrint(arena, "{d}", .{intOf(o, "max_conns")}),
            try std.fmt.allocPrint(arena, "{d}", .{intOf(o, "priority")}),
            yesNo(boolOf(o, "enabled")),
        });
    }

    var width: [n_cols]usize = @splat(0);
    for (cells.items) |r| {
        for (r, 0..) |c, i| width[i] = @max(width[i], c.len);
    }

    var out: std.Io.Writer.Allocating = .init(gpa);
    errdefer out.deinit();
    for (cells.items) |r| {
        for (r, 0..) |c, i| {
            out.writer.writeAll(c) catch return error.OutOfMemory;
            // Two spaces after every column but the last, matching the
            // tabwriter padding Go was configured with. No trailing
            // whitespace, which `diff` and `grep -x` both care about.
            if (i + 1 < n_cols) {
                out.writer.splatByteAll(' ', width[i] - c.len + 2) catch return error.OutOfMemory;
            }
        }
        out.writer.writeByte('\n') catch return error.OutOfMemory;
    }
    return out.toOwnedSlice();
}

fn yesNo(b: bool) []const u8 {
    return if (b) "yes" else "no";
}

// ---------------------------------------------------------------------
// rm
// ---------------------------------------------------------------------

fn remove(gpa: Allocator, env: std.process.Environ, args: []const []const u8) u8 {
    if (args.len == 0) return api.fail("server rm: <id> required", .{});
    if (args.len != 1) return api.fail("server rm: one <id> only", .{});
    const id = std.fmt.parseInt(i64, args[0], 10) catch
        return api.fail("server rm: bad id \"{s}\"", .{args[0]});

    var ep = Connection.open(gpa, env, "server rm") orelse return 1;
    defer ep.deinit();

    var path_buf: [64]u8 = undefined;
    const path = std.fmt.bufPrint(&path_buf, "/api/v1/servers/{d}", .{id}) catch unreachable;

    var resp = api.call(gpa, ep.endpoint, .{ .method = .delete, .path = path }) catch |err|
        return api.fail("server rm: {t}", .{err});
    defer resp.deinit();

    if (resp.status < 200 or resp.status >= 300) {
        var buf: [512]u8 = undefined;
        return api.fail("server rm: {s}", .{api.errorText(&resp, &buf)});
    }
    api.report("server removed id={d}", .{id});
    return 0;
}

// ---------------------------------------------------------------------
// Shared
// ---------------------------------------------------------------------

/// Config resolution plus its one failure message, so each subcommand is
/// three lines rather than fifteen. Returns null after reporting, which
/// is why every caller's next statement is `return 1`.
const Connection = struct {
    resolved: api.Resolved,
    endpoint: api.Endpoint,

    fn open(gpa: Allocator, env: std.process.Environ, who: []const u8) ?Connection {
        var diag: config.Diagnostic = .{};
        const resolved = api.resolve(gpa, env, &diag) catch |err| {
            if (err == error.EmptyAPIKey) {
                _ = api.fail(
                    "{s}: no API key; set HOARDARR_API_KEY or auth.api_key in config.toml",
                    .{who},
                );
            } else {
                _ = api.fail("{s}: {s}", .{
                    who,
                    if (diag.msg.len != 0) diag.msg else @errorName(err),
                });
            }
            return null;
        };
        return .{ .resolved = resolved, .endpoint = resolved.endpoint };
    }

    fn deinit(self: *Connection) void {
        self.resolved.deinit();
    }
};

fn intField(gpa: Allocator, body: []const u8, key: []const u8) ?i64 {
    const parsed = std.json.parseFromSlice(std.json.Value, gpa, body, .{}) catch return null;
    defer parsed.deinit();
    if (parsed.value != .object) return null;
    return switch (parsed.value.object.get(key) orelse .null) {
        .integer => |v| v,
        else => null,
    };
}

fn intOf(o: std.json.ObjectMap, key: []const u8) i64 {
    return switch (o.get(key) orelse .null) {
        .integer => |v| v,
        .float => |v| @intFromFloat(v),
        else => 0,
    };
}

fn strOf(o: std.json.ObjectMap, key: []const u8) []const u8 {
    return switch (o.get(key) orelse .null) {
        .string => |v| v,
        else => "",
    };
}

fn boolOf(o: std.json.ObjectMap, key: []const u8) bool {
    return switch (o.get(key) orelse .null) {
        .bool => |v| v,
        else => false,
    };
}

// ---------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------

const testing = std.testing;

test "add defaults are Go's: TLS on, port 563, 8 connections" {
    const o = try parseAdd(&.{ "--name", "n", "--host", "h" });
    try testing.expectEqualStrings("n", o.name);
    try testing.expectEqualStrings("h", o.host);
    try testing.expectEqual(@as(u16, 563), o.port);
    try testing.expect(o.tls);
    try testing.expectEqual(@as(i64, 8), o.conns);
    try testing.expectEqual(@as(i64, 0), o.priority);
    try testing.expectEqualStrings("", o.user);
}

test "add accepts every flag, in both forms" {
    const o = try parseAdd(&.{
        "--name=eweka",  "--host=news.eweka.nl", "--port=119",
        "--no-tls",      "--user",               "u",
        "--pass",        "p",                    "--conns=20",
        "--priority=-1",
    });
    try testing.expectEqualStrings("eweka", o.name);
    try testing.expectEqualStrings("news.eweka.nl", o.host);
    try testing.expectEqual(@as(u16, 119), o.port);
    try testing.expect(!o.tls);
    try testing.expectEqualStrings("u", o.user);
    try testing.expectEqualStrings("p", o.pass);
    try testing.expectEqual(@as(i64, 20), o.conns);
    try testing.expectEqual(@as(i64, -1), o.priority);

    // Single-dash long flags, as Go's `flag` package took them.
    const g = try parseAdd(&.{ "-name", "n", "-host=h", "-port", "563" });
    try testing.expectEqualStrings("n", g.name);
    try testing.expectEqualStrings("h", g.host);
}

test "add rejects what Go rejected" {
    try testing.expectError(error.MissingRequired, parseAdd(&.{}));
    try testing.expectError(error.MissingRequired, parseAdd(&.{ "--name", "n" }));
    try testing.expectError(error.MissingRequired, parseAdd(&.{ "--host", "h" }));
    try testing.expectError(error.MissingValue, parseAdd(&.{ "--name", "n", "--host" }));
    try testing.expectError(error.UnknownFlag, parseAdd(&.{ "--name", "n", "--host", "h", "--nope", "1" }));
    try testing.expectError(error.BadNumber, parseAdd(&.{ "--name", "n", "--host", "h", "--port", "x" }));
    try testing.expectError(error.BadNumber, parseAdd(&.{ "--name", "n", "--host", "h", "--port", "0" }));
    // 70000 does not fit a port and must not wrap to 4464.
    try testing.expectError(error.BadNumber, parseAdd(&.{ "--name", "n", "--host", "h", "--port", "70000" }));
    try testing.expectError(error.UnexpectedArg, parseAdd(&.{"positional"}));
}

test "the add body is the JSON the REST handler reads" {
    const gpa = testing.allocator;
    const body = try addBody(gpa, .{
        .name = "eweka",
        .host = "news.eweka.nl",
        .port = 563,
        .tls = true,
        .user = "u",
        .pass = "p",
        .conns = 20,
        .priority = 1,
    });
    defer gpa.free(body);

    const p = try std.json.parseFromSlice(std.json.Value, gpa, body, .{});
    defer p.deinit();
    const o = p.value.object;
    try testing.expectEqualStrings("eweka", o.get("name").?.string);
    try testing.expectEqualStrings("news.eweka.nl", o.get("host").?.string);
    try testing.expectEqual(@as(i64, 563), o.get("port").?.integer);
    try testing.expect(o.get("tls").?.bool);
    try testing.expectEqualStrings("u", o.get("username").?.string);
    try testing.expectEqualStrings("p", o.get("password").?.string);
    try testing.expectEqual(@as(i64, 20), o.get("max_conns").?.integer);
    try testing.expectEqual(@as(i64, 1), o.get("priority").?.integer);
}

test "a password full of JSON metacharacters is escaped, not injected" {
    const gpa = testing.allocator;
    const nasty = "p\"\\,\"priority\":999,\"x\":\"\n\t";
    const body = try addBody(gpa, .{ .name = "n", .host = "h", .pass = nasty });
    defer gpa.free(body);

    const p = try std.json.parseFromSlice(std.json.Value, gpa, body, .{});
    defer p.deinit();
    const o = p.value.object;
    try testing.expectEqualStrings(nasty, o.get("password").?.string);
    // The injected key did not become a real one.
    try testing.expectEqual(@as(i64, 0), o.get("priority").?.integer);
    try testing.expectEqual(@as(usize, 8), o.count());
}

fn parseRows(gpa: Allocator, text: []const u8) !std.json.Parsed(std.json.Value) {
    return std.json.parseFromSlice(std.json.Value, gpa, text, .{});
}

test "list renders Go's columns, padded to the widest cell" {
    const gpa = testing.allocator;
    var p = try parseRows(gpa,
        \\[{"id":1,"name":"eweka","host":"news.eweka.nl","port":563,"tls":true,
        \\  "max_conns":20,"priority":0,"enabled":true},
        \\ {"id":12,"name":"b","host":"h","port":119,"tls":false,
        \\  "max_conns":4,"priority":-1,"enabled":false}]
    );
    defer p.deinit();

    const table = try renderTable(gpa, p.value.array.items);
    defer gpa.free(table);

    var lines = std.mem.splitScalar(u8, std.mem.trimEnd(u8, table, "\n"), '\n');
    const header = lines.next().?;
    try testing.expect(std.mem.startsWith(u8, header, "ID  NAME"));
    for ([_][]const u8{ "HOST:PORT", "TLS", "CONNS", "PRIORITY", "ENABLED" }) |h| {
        try testing.expect(std.mem.indexOf(u8, header, h) != null);
    }

    const first = lines.next().?;
    try testing.expect(std.mem.indexOf(u8, first, "news.eweka.nl:563") != null);
    try testing.expect(std.mem.indexOf(u8, first, "yes") != null);
    const second = lines.next().?;
    try testing.expect(std.mem.indexOf(u8, second, "h:119") != null);
    try testing.expect(std.mem.indexOf(u8, second, "no") != null);
    try testing.expect(std.mem.indexOf(u8, second, "-1") != null);
    try testing.expect(lines.next() == null);

    // Columns line up: every row's second column starts at the same
    // offset, which is the whole point of the padding.
    const name_col = std.mem.indexOf(u8, header, "NAME").?;
    try testing.expectEqualStrings("eweka", first[name_col..][0..5]);
    try testing.expectEqualStrings("b", second[name_col..][0..1]);

    // No trailing whitespace on any line.
    var check = std.mem.splitScalar(u8, std.mem.trimEnd(u8, table, "\n"), '\n');
    while (check.next()) |l| try testing.expect(l[l.len - 1] != ' ');
}

test "list never prints a password, whatever the daemon sends" {
    const gpa = testing.allocator;
    // The API has no password field, but assert the renderer would not
    // surface one if a future field appeared.
    var p = try parseRows(gpa,
        \\[{"id":1,"name":"n","host":"h","port":563,"tls":true,"max_conns":1,
        \\  "priority":0,"enabled":true,"password":"hunter2","username":"u"}]
    );
    defer p.deinit();

    const table = try renderTable(gpa, p.value.array.items);
    defer gpa.free(table);
    try testing.expect(std.mem.indexOf(u8, table, "hunter2") == null);
}

test "a malformed row is skipped rather than crashing the table" {
    const gpa = testing.allocator;
    var p = try parseRows(gpa,
        \\["not an object", {"id":3,"name":"ok","host":"h","port":1,"tls":true,
        \\  "max_conns":1,"priority":0,"enabled":true}, 42]
    );
    defer p.deinit();

    const table = try renderTable(gpa, p.value.array.items);
    defer gpa.free(table);
    var lines = std.mem.splitScalar(u8, std.mem.trimEnd(u8, table, "\n"), '\n');
    _ = lines.next(); // header
    try testing.expect(std.mem.indexOf(u8, lines.next().?, "ok") != null);
    try testing.expect(lines.next() == null);
}
