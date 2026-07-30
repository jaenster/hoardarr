//! `hoardarr download <nzb-path> [--cat NAME]` — queue an NZB from a
//! shell.
//!
//! Go's version did more than this: it built the whole application in
//! process, picked the highest-priority server, ran the orchestrator to
//! completion and printed a byte summary. That shape only worked because
//! the CLI *was* the downloader. It no longer is — the daemon owns the
//! queue, the connection pool and the post-processing pipeline — so this
//! hands the NZB to the running daemon and reports the job id. Watching
//! it finish is the queue's job, and `GET /api/v1/queue` is how.
//!
//! `--priority` is gone with it: `POST /api/v1/queue/nzb` has no priority
//! field, and accepting a flag that silently does nothing is worse than
//! not accepting it. Ordering is `POST /api/v1/queue/reorder`.
//!
//! A duplicate NZB is not a failure. The API answers 200 with
//! `duplicate: true` rather than a 4xx, because an *arr client re-posting
//! a release it already sent is routine; the CLI says so and exits 0, as
//! Go's did when `AddJob` returned `ErrDuplicateNZB`.

const std = @import("std");
const api = @import("api.zig");
const sys = @import("../posix/sys.zig");
const config = @import("../core/config.zig");

const Allocator = std.mem.Allocator;

pub const usage =
    \\Usage: hoardarr download <nzb-path> [--cat CATEGORY]
    \\
;

/// The largest NZB we will post. A 40 GiB release's NZB is a few MiB;
/// anything past this is not an NZB and would only be rejected by the
/// server's own body cap after we had read it all into memory.
pub const max_nzb_bytes = 32 << 20;

pub const Options = struct {
    nzb_path: []const u8 = "",
    category: []const u8 = "",
};

pub const ParseError = error{
    MissingPath,
    MissingValue,
    UnknownFlag,
    TooManyArgs,
};

/// Pure, so the flag grammar is tested without a filesystem or a daemon.
///
/// `--cat X` and `--cat=X` both work: Go's `flag` package accepted both
/// and a script written against it may use either.
pub fn parseArgs(args: []const []const u8) ParseError!Options {
    var o: Options = .{};
    var i: usize = 0;
    while (i < args.len) : (i += 1) {
        const a = args[i];
        if (std.mem.eql(u8, a, "--cat") or std.mem.eql(u8, a, "-cat")) {
            i += 1;
            if (i >= args.len) return error.MissingValue;
            o.category = args[i];
            continue;
        }
        if (std.mem.startsWith(u8, a, "--cat=")) {
            o.category = a["--cat=".len..];
            continue;
        }
        if (std.mem.startsWith(u8, a, "-cat=")) {
            o.category = a["-cat=".len..];
            continue;
        }
        // A bare "--" ends flag parsing, so an NZB whose name starts with
        // a dash is still reachable.
        if (std.mem.eql(u8, a, "--")) {
            i += 1;
            if (i >= args.len) return error.MissingPath;
            if (o.nzb_path.len != 0) return error.TooManyArgs;
            o.nzb_path = args[i];
            continue;
        }
        if (a.len > 1 and a[0] == '-') return error.UnknownFlag;
        if (o.nzb_path.len != 0) return error.TooManyArgs;
        o.nzb_path = a;
    }
    if (o.nzb_path.len == 0) return error.MissingPath;
    return o;
}

pub fn run(gpa: Allocator, env: std.process.Environ, args: []const []const u8) u8 {
    const o = parseArgs(args) catch |err| switch (err) {
        // Go's message, verbatim: scripts grep for it.
        error.MissingPath => return api.fail("download: <nzb-path> required", .{}),
        error.MissingValue => return api.fail("download: --cat needs a value", .{}),
        error.UnknownFlag => return api.fail("download: unknown flag", .{}),
        error.TooManyArgs => return api.fail("download: only one <nzb-path>", .{}),
    };

    const nzb = readFile(gpa, o.nzb_path) catch |err|
        return api.fail("open nzb: {t}", .{err});
    defer gpa.free(nzb);
    if (nzb.len == 0) return api.fail("download: {s} is empty", .{o.nzb_path});

    var diag: config.Diagnostic = .{};
    const resolved = api.resolve(gpa, env, &diag) catch |err| switch (err) {
        error.EmptyAPIKey => return api.fail(
            "download: no API key; set HOARDARR_API_KEY or auth.api_key in config.toml",
            .{},
        ),
        else => return api.fail("download: {s}", .{if (diag.msg.len != 0) diag.msg else @errorName(err)}),
    };
    defer resolved.deinit();

    const body = buildForm(gpa, baseName(o.nzb_path), o.category, nzb) catch
        return api.fail("download: out of memory", .{});
    defer body.deinit(gpa);

    var resp = api.call(gpa, resolved.endpoint, .{
        .method = .post,
        .path = "/api/v1/queue/nzb",
        .body = body.bytes,
        .content_type = body.content_type,
        // Generous: the daemon parses the NZB and writes every file and
        // segment row inside the request.
        .deadline_ns = 60 * std.time.ns_per_s,
    }) catch |err| return api.fail("download: {t}", .{err});
    defer resp.deinit();

    if (resp.status < 200 or resp.status >= 300) {
        var buf: [512]u8 = undefined;
        return api.fail("download: {s}", .{api.errorText(&resp, &buf)});
    }

    const parsed = std.json.parseFromSlice(std.json.Value, gpa, resp.body, .{}) catch
        return api.fail("download: unreadable response from the daemon", .{});
    defer parsed.deinit();
    if (parsed.value != .object) return api.fail("download: unreadable response from the daemon", .{});
    const obj = parsed.value.object;

    const job_id: i64 = switch (obj.get("job_id") orelse .null) {
        .integer => |v| v,
        else => 0,
    };
    const duplicate = switch (obj.get("duplicate") orelse .null) {
        .bool => |v| v,
        else => false,
    };

    if (duplicate) {
        api.report("nzb already queued; resuming job_id={d}", .{job_id});
    } else {
        api.report("nzb queued job_id={d}", .{job_id});
    }
    return 0;
}

// ---------------------------------------------------------------------
// multipart/form-data
// ---------------------------------------------------------------------

/// A built form: the body and the `Content-Type` that describes it.
/// Both are separate allocations the caller owns.
pub const Form = struct {
    bytes: []u8,
    /// `multipart/form-data; boundary=…`, so the caller does not have to
    /// rebuild the boundary it never saw.
    content_type: []u8,

    pub fn deinit(self: Form, gpa: Allocator) void {
        gpa.free(self.bytes);
        gpa.free(self.content_type);
    }
};

/// Hex of 16 random bytes. Long enough that it cannot collide with the
/// NZB's contents by accident, and random rather than fixed so a
/// hand-crafted NZB containing a known boundary cannot split the part.
const boundary_len = 32;

/// Builds the body `POST /api/v1/queue/nzb` expects: an `nzb` file part
/// whose filename becomes the queue's display name, plus a `category`
/// field when one was asked for.
pub fn buildForm(
    gpa: Allocator,
    filename: []const u8,
    category: []const u8,
    nzb: []const u8,
) Allocator.Error!Form {
    var raw: [16]u8 = undefined;
    sys.randomBytes(&raw);
    var boundary: [boundary_len]u8 = undefined;
    const hex = "0123456789abcdef";
    for (raw, 0..) |b, i| {
        boundary[2 * i] = hex[b >> 4];
        boundary[2 * i + 1] = hex[b & 0x0F];
    }

    var out: std.Io.Writer.Allocating = .init(gpa);
    errdefer out.deinit();
    const w = &out.writer;

    // Arena-free but allocating writer: the only failure mode is OOM.
    writeForm(w, &boundary, filename, category, nzb) catch return error.OutOfMemory;
    const bytes = try out.toOwnedSlice();
    errdefer gpa.free(bytes);

    const ct = try std.fmt.allocPrint(gpa, "multipart/form-data; boundary={s}", .{boundary});
    return .{ .bytes = bytes, .content_type = ct };
}

fn writeForm(
    w: *std.Io.Writer,
    boundary: []const u8,
    filename: []const u8,
    category: []const u8,
    nzb: []const u8,
) std.Io.Writer.Error!void {
    if (category.len != 0) {
        try w.print("--{s}\r\n", .{boundary});
        try w.writeAll("Content-Disposition: form-data; name=\"category\"\r\n\r\n");
        try w.writeAll(category);
        try w.writeAll("\r\n");
    }
    try w.print("--{s}\r\n", .{boundary});
    try w.writeAll("Content-Disposition: form-data; name=\"nzb\"; filename=\"");
    // A quote or a CR in the filename would end the parameter early and
    // could forge a second header line. The name is operator-supplied,
    // but it comes from a path that an *arr wrote, so it is sanitised
    // rather than trusted.
    for (filename) |c| {
        if (c == '"' or c == '\\' or c == '\r' or c == '\n') continue;
        try w.writeByte(c);
    }
    try w.writeAll("\"\r\nContent-Type: application/x-nzb\r\n\r\n");
    try w.writeAll(nzb);
    try w.print("\r\n--{s}--\r\n", .{boundary});
}

/// Final path component, so `/downloads/Foo.nzb` is displayed as
/// `Foo.nzb`. The server strips the `.nzb` suffix itself.
pub fn baseName(p: []const u8) []const u8 {
    const slash = std.mem.lastIndexOfScalar(u8, p, '/') orelse return p;
    return p[slash + 1 ..];
}

fn readFile(gpa: Allocator, p: []const u8) ![]u8 {
    var path_buf: [sys.path_max]u8 = undefined;
    const z = try sys.pathZ(&path_buf, p);
    return api.readFileAlloc(gpa, z, max_nzb_bytes);
}

// ---------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------

const testing = std.testing;
const multipart = @import("../api/rest/multipart.zig");

test "flag grammar: both --cat forms, and the path is positional" {
    try testing.expectEqualStrings("a.nzb", (try parseArgs(&.{"a.nzb"})).nzb_path);

    const a = try parseArgs(&.{ "--cat", "tv", "a.nzb" });
    try testing.expectEqualStrings("tv", a.category);
    try testing.expectEqualStrings("a.nzb", a.nzb_path);

    const b = try parseArgs(&.{ "a.nzb", "--cat=movies" });
    try testing.expectEqualStrings("movies", b.category);
    try testing.expectEqualStrings("a.nzb", b.nzb_path);

    // Go's `flag` accepted single-dash long flags too.
    try testing.expectEqualStrings("tv", (try parseArgs(&.{ "-cat", "tv", "a.nzb" })).category);
    try testing.expectEqualStrings("tv", (try parseArgs(&.{ "-cat=tv", "a.nzb" })).category);

    // `--` reaches a file whose name starts with a dash.
    try testing.expectEqualStrings("-weird.nzb", (try parseArgs(&.{ "--", "-weird.nzb" })).nzb_path);
}

test "flag grammar rejects what Go rejected" {
    try testing.expectError(error.MissingPath, parseArgs(&.{}));
    try testing.expectError(error.MissingPath, parseArgs(&.{ "--cat", "tv" }));
    try testing.expectError(error.MissingValue, parseArgs(&.{"--cat"}));
    try testing.expectError(error.UnknownFlag, parseArgs(&.{ "--priority", "1", "a.nzb" }));
    try testing.expectError(error.TooManyArgs, parseArgs(&.{ "a.nzb", "b.nzb" }));
}

test "baseName is the display name the queue will show" {
    try testing.expectEqualStrings("Foo.nzb", baseName("/downloads/Foo.nzb"));
    try testing.expectEqualStrings("Foo.nzb", baseName("Foo.nzb"));
    try testing.expectEqualStrings("", baseName("/downloads/"));
}

// The form is parsed back with the *server's* scanner rather than by
// hand: if the two ever disagree about framing, that is exactly the bug
// this catches.
test "the form round-trips through the server's own multipart scanner" {
    const gpa = testing.allocator;
    const nzb =
        \\<?xml version="1.0"?><nzb><file subject="x"/></nzb>
    ;
    const form = try buildForm(gpa, "Release.Name.nzb", "tv", nzb);
    defer form.deinit(gpa);

    const b = multipart.boundary(form.content_type) orelse return error.NoBoundary;
    const file = multipart.field(form.bytes, b, "nzb") orelse return error.NoNzbPart;
    try testing.expectEqualStrings(nzb, file.body);
    try testing.expectEqualStrings("Release.Name.nzb", file.filename);
    try testing.expect(file.isFile());
    try testing.expectEqualStrings("tv", multipart.value(form.bytes, b, "category"));
}

test "no category means no category part at all" {
    const gpa = testing.allocator;
    const form = try buildForm(gpa, "A.nzb", "", "<nzb/>");
    defer form.deinit(gpa);

    const b = multipart.boundary(form.content_type) orelse return error.NoBoundary;
    try testing.expectEqualStrings("", multipart.value(form.bytes, b, "category"));
    const file = multipart.field(form.bytes, b, "nzb") orelse return error.NoNzbPart;
    try testing.expectEqualStrings("<nzb/>", file.body);
}

test "a quote in the filename cannot forge a part header" {
    const gpa = testing.allocator;
    const hostile = "a\";name=\"nzb\";filename=\"b.nzb";
    const form = try buildForm(gpa, hostile, "", "<nzb/>");
    defer form.deinit(gpa);

    const b = multipart.boundary(form.content_type) orelse return error.NoBoundary;
    const file = multipart.field(form.bytes, b, "nzb") orelse return error.NoNzbPart;
    // The quotes are dropped, so the parameter still ends where we put it
    // and the body is the NZB rather than a slice of the injected header.
    try testing.expectEqualStrings("<nzb/>", file.body);
    try testing.expect(std.mem.indexOfScalar(u8, file.filename, '"') == null);
}

test "binary NZB bytes survive the form intact" {
    const gpa = testing.allocator;
    // Not valid XML, deliberately: the form must not care, and a CRLF
    // run inside the body must not be mistaken for a boundary.
    var payload: [512]u8 = undefined;
    for (&payload, 0..) |*c, i| c.* = @truncate(i);
    @memcpy(payload[100..106], "\r\n--ab");

    const form = try buildForm(gpa, "x.nzb", "", &payload);
    defer form.deinit(gpa);

    const b = multipart.boundary(form.content_type) orelse return error.NoBoundary;
    const file = multipart.field(form.bytes, b, "nzb") orelse return error.NoNzbPart;
    try testing.expectEqualSlices(u8, &payload, file.body);
}
