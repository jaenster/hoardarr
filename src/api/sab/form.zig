//! Request-parameter extraction for the SAB endpoint.
//!
//! SAB does not have a request *body format* so much as three of them, and
//! the *arr suite uses all three: `GET /sabnzbd/api?mode=queue&apikey=…`,
//! `POST` with `application/x-www-form-urlencoded`, and `POST` with
//! `multipart/form-data` for `mode=addfile`. The Go handler papered over
//! this with `r.ParseMultipartForm` / `r.ParseForm` and a `formGet` helper;
//! this file is that helper, made explicit.
//!
//! ## Precedence
//!
//! `Form.get` reproduces Go's `formGet` exactly, including its quirk: a
//! parameter present in the *body* wins even when its value is empty, and
//! only a parameter absent from the body falls through to the query
//! string. (`r.PostFormValue` returns "", then `r.FormValue` consults
//! `r.Form`, whose first entry for that key is the same empty body value.)
//! Within one source the *first* occurrence wins, as `url.Values.Get`
//! does — not the last.
//!
//! ## One deliberate divergence
//!
//! `%00` in a parameter is rejected here (`net/http/request.zig` refuses
//! a decoded NUL) where Go accepted it. A NUL in an nzo_id, a category or
//! a URL has no legitimate use and is a classic way to smuggle a
//! different string past a downstream consumer that stops at the NUL.
//!
//! ## Filenames from multipart
//!
//! `filename` is passed through `base` before use, as Go's
//! `multipart.Part.FileName` does per RFC 7578 §4.2: a client must not be
//! able to steer a display name — or anything derived from it — with
//! directory components.

const std = @import("std");
const http = @import("../../net/http/request.zig");

const Allocator = std.mem.Allocator;

pub const Pair = struct { key: []const u8, value: []const u8 };

pub const FilePart = struct {
    /// The `name=` of the multipart part.
    field: []const u8,
    /// `filename=`, already reduced to its last path element.
    filename: []const u8,
    content: []const u8,
};

pub const ParseError = error{
    /// Malformed percent-encoding, or a multipart body we cannot walk.
    /// Answered as 400, which is what Go's `parse form:` wrapper produced.
    BadForm,
    OutOfMemory,
};

/// Everything the parser needs, without a `Request`. Handler tests build
/// this directly; production goes through `fromRequest`.
pub const Input = struct {
    /// True for POST / PUT / PATCH — the methods whose body Go's
    /// `ParseForm` reads. A GET body is ignored even if present.
    body_bearing: bool = false,
    /// Raw query string, without the '?'.
    query: []const u8 = "",
    content_type: []const u8 = "",
    body: []const u8 = "",
    user_agent: []const u8 = "",
};

pub const Form = struct {
    query: []const Pair = &.{},
    post: []const Pair = &.{},
    files: []const FilePart = &.{},
    user_agent: []const u8 = "",

    /// Go's `formGet`. See the module comment for the precedence rules.
    pub fn get(self: Form, key: []const u8) []const u8 {
        for (self.post) |p| {
            if (std.mem.eql(u8, p.key, key)) return p.value;
        }
        for (self.query) |p| {
            if (std.mem.eql(u8, p.key, key)) return p.value;
        }
        return "";
    }

    pub fn has(self: Form, key: []const u8) bool {
        for (self.post) |p| {
            if (std.mem.eql(u8, p.key, key)) return true;
        }
        for (self.query) |p| {
            if (std.mem.eql(u8, p.key, key)) return true;
        }
        return false;
    }

    /// First file part matching any of `names`, in the order given. SAB
    /// itself uses `name`; several forks use `nzbfile`, and *arr builds
    /// vary, so the handler tries both.
    pub fn firstFile(self: Form, names: []const []const u8) ?FilePart {
        for (names) |n| {
            for (self.files) |file| {
                if (std.mem.eql(u8, file.field, n)) return file;
            }
        }
        return null;
    }
};

/// Parses `in` into a `Form`. Everything is allocated from `gpa`, which is
/// expected to be the per-request arena: nothing here is freed
/// individually.
pub fn parse(gpa: Allocator, in: Input) ParseError!Form {
    var query: std.ArrayList(Pair) = .empty;
    var post: std.ArrayList(Pair) = .empty;
    var files: std.ArrayList(FilePart) = .empty;

    try parseUrlEncoded(gpa, &query, in.query);

    if (in.body_bearing and in.body.len > 0) {
        if (mediaTypeIs(in.content_type, "multipart/form-data")) {
            const boundary = paramValue(in.content_type, "boundary") orelse return error.BadForm;
            try parseMultipart(gpa, &post, &files, boundary, in.body);
        } else if (mediaTypeIs(in.content_type, "application/x-www-form-urlencoded")) {
            try parseUrlEncoded(gpa, &post, in.body);
        }
        // Any other content type carries no form parameters, which is
        // also what Go's `parsePostForm` concluded.
    }

    return .{
        .query = try query.toOwnedSlice(gpa),
        .post = try post.toOwnedSlice(gpa),
        .files = try files.toOwnedSlice(gpa),
        .user_agent = in.user_agent,
    };
}

/// Adapter for the real server. Kept trivial so that everything worth
/// testing is reachable through `parse`.
pub fn fromRequest(gpa: Allocator, req: *const http.Request) ParseError!Form {
    return parse(gpa, .{
        .body_bearing = switch (req.method) {
            .post, .put, .patch => true,
            else => false,
        },
        .query = req.query,
        .content_type = req.header("content-type") orelse "",
        .body = req.body,
        .user_agent = req.header("user-agent") orelse "",
    });
}

fn parseUrlEncoded(gpa: Allocator, out: *std.ArrayList(Pair), s: []const u8) ParseError!void {
    var it = http.QueryIter{ .rest = s };
    while (it.next()) |kv| {
        try out.append(gpa, .{
            .key = try decode(gpa, kv.key),
            .value = try decode(gpa, kv.value),
        });
    }
}

fn decode(gpa: Allocator, raw: []const u8) ParseError![]const u8 {
    if (raw.len == 0) return "";
    // Decoding never grows.
    const buf = try gpa.alloc(u8, raw.len);
    return http.percentDecode(buf, raw, .query) catch error.BadForm;
}

// ---------------------------------------------------------------------
// Media types and parameters
// ---------------------------------------------------------------------

/// True when `header`'s media type (the part before the first ';') equals
/// `want`, ASCII-case-insensitively.
pub fn mediaTypeIs(header: []const u8, want: []const u8) bool {
    const semi = std.mem.indexOfScalar(u8, header, ';') orelse header.len;
    const mt = std.mem.trim(u8, header[0..semi], " \t");
    return std.ascii.eqlIgnoreCase(mt, want);
}

/// Value of a `; name=value` parameter, quoted or bare. Returns a slice
/// into `header` for the bare form; for a quoted value containing no
/// escapes it is also a borrowed slice, and a value *with* a backslash
/// escape is rejected rather than unescaped — no client sends one, and a
/// half-implemented unquoting routine is worse than none.
pub fn paramValue(header: []const u8, name: []const u8) ?[]const u8 {
    var rest = header;
    // Skip the media type itself.
    const semi = std.mem.indexOfScalar(u8, rest, ';') orelse return null;
    rest = rest[semi + 1 ..];
    while (rest.len > 0) {
        const end = std.mem.indexOfScalar(u8, rest, ';') orelse rest.len;
        var param = std.mem.trim(u8, rest[0..end], " \t");
        rest = if (end == rest.len) "" else rest[end + 1 ..];
        const eq = std.mem.indexOfScalar(u8, param, '=') orelse continue;
        const pname = std.mem.trim(u8, param[0..eq], " \t");
        if (!std.ascii.eqlIgnoreCase(pname, name)) continue;
        const v = std.mem.trim(u8, param[eq + 1 ..], " \t");
        if (v.len >= 2 and v[0] == '"') {
            // A quoted value may itself contain ';', so re-scan from the
            // opening quote in the original header rather than the
            // semicolon-split fragment.
            var q = (@intFromPtr(param.ptr) - @intFromPtr(header.ptr)) + eq + 1;
            while (q < header.len and (header[q] == ' ' or header[q] == '\t')) q += 1;
            if (q >= header.len or header[q] != '"') return null;
            const tail = header[q + 1 ..];
            const close = std.mem.indexOfScalar(u8, tail, '"') orelse return null;
            const quoted = tail[0..close];
            if (std.mem.indexOfScalar(u8, quoted, '\\') != null) return null;
            return quoted;
        }
        // Bare token.
        param = v;
        return if (param.len == 0) null else param;
    }
    return null;
}

/// Go's `filepath.Base` for the POSIX separator. Applied to every
/// client-supplied filename.
pub fn base(path: []const u8) []const u8 {
    if (path.len == 0) return ".";
    var p = path;
    while (p.len > 0 and p[p.len - 1] == '/') p = p[0 .. p.len - 1];
    if (p.len == 0) return "/";
    if (std.mem.lastIndexOfScalar(u8, p, '/')) |i| p = p[i + 1 ..];
    if (p.len == 0) return "/";
    return p;
}

// ---------------------------------------------------------------------
// Multipart
// ---------------------------------------------------------------------

fn parseMultipart(
    gpa: Allocator,
    post: *std.ArrayList(Pair),
    files: *std.ArrayList(FilePart),
    boundary: []const u8,
    body: []const u8,
) ParseError!void {
    // RFC 2046 caps a boundary at 70 characters; a longer one is a client
    // we do not need to accommodate.
    if (boundary.len == 0 or boundary.len > 70) return error.BadForm;
    var dash_buf: [72]u8 = undefined;
    dash_buf[0] = '-';
    dash_buf[1] = '-';
    @memcpy(dash_buf[2..][0..boundary.len], boundary);
    const delim = dash_buf[0 .. 2 + boundary.len];

    // Skip the preamble.
    var i = std.mem.indexOf(u8, body, delim) orelse return error.BadForm;
    i += delim.len;

    while (i <= body.len) {
        // `--` after the delimiter closes the body.
        if (std.mem.startsWith(u8, body[i..], "--")) return;
        // Transport padding, then CRLF, then the part's header block.
        const nl = std.mem.indexOf(u8, body[i..], "\r\n") orelse return;
        i += nl + 2;

        const hdr_len = std.mem.indexOf(u8, body[i..], "\r\n\r\n") orelse return error.BadForm;
        const headers = body[i .. i + hdr_len];
        i += hdr_len + 4;

        const rest = body[i..];
        var content_len = rest.len;
        var next = rest.len;
        if (findDelimiter(rest, delim)) |at| {
            content_len = at;
            next = at + 2 + delim.len;
        }
        const content = rest[0..content_len];
        i += next;

        try addPart(gpa, post, files, headers, content);
        if (next == rest.len) return;
    }
}

/// Offset of the `\r\n--boundary` that ends a part's content.
fn findDelimiter(hay: []const u8, delim: []const u8) ?usize {
    var from: usize = 0;
    while (from + 2 <= hay.len) {
        const at = std.mem.indexOfPos(u8, hay, from, "\r\n") orelse return null;
        if (std.mem.startsWith(u8, hay[at + 2 ..], delim)) return at;
        from = at + 1;
    }
    return null;
}

fn addPart(
    gpa: Allocator,
    post: *std.ArrayList(Pair),
    files: *std.ArrayList(FilePart),
    headers: []const u8,
    content: []const u8,
) ParseError!void {
    const disp = headerValue(headers, "content-disposition") orelse return;
    const field = paramValue(disp, "name") orelse return;
    if (paramValue(disp, "filename")) |raw_name| {
        try files.append(gpa, .{
            .field = field,
            .filename = base(raw_name),
            .content = content,
        });
        return;
    }
    // A value part. Multipart values are literal bytes — no
    // percent-decoding, unlike a urlencoded body.
    try post.append(gpa, .{ .key = field, .value = content });
}

fn headerValue(block: []const u8, name: []const u8) ?[]const u8 {
    var rest = block;
    while (rest.len > 0) {
        const end = std.mem.indexOf(u8, rest, "\r\n") orelse rest.len;
        const line = rest[0..end];
        rest = if (end == rest.len) "" else rest[end + 2 ..];
        const colon = std.mem.indexOfScalar(u8, line, ':') orelse continue;
        if (std.ascii.eqlIgnoreCase(std.mem.trim(u8, line[0..colon], " \t"), name)) {
            return std.mem.trim(u8, line[colon + 1 ..], " \t");
        }
    }
    return null;
}

// ---------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------

const testing = std.testing;

/// Every test parses into an arena, which is how the handler uses this.
const Arena = struct {
    inner: std.heap.ArenaAllocator,

    fn init() Arena {
        return .{ .inner = .init(testing.allocator) };
    }
    fn deinit(self: *Arena) void {
        self.inner.deinit();
    }
    fn a(self: *Arena) Allocator {
        return self.inner.allocator();
    }
};

test "a GET carries everything in the query" {
    var arena = Arena.init();
    defer arena.deinit();
    const form = try parse(arena.a(), .{ .query = "mode=queue&apikey=abc&limit=25" });
    try testing.expectEqualStrings("queue", form.get("mode"));
    try testing.expectEqualStrings("abc", form.get("apikey"));
    try testing.expectEqualStrings("25", form.get("limit"));
    try testing.expectEqualStrings("", form.get("missing"));
    try testing.expect(!form.has("missing"));
}

test "query values are percent-decoded and + is a space" {
    var arena = Arena.init();
    defer arena.deinit();
    const form = try parse(arena.a(), .{
        .query = "name=Some%20Release+Name&cat=tv%2Fhd&t=%2B",
    });
    try testing.expectEqualStrings("Some Release Name", form.get("name"));
    try testing.expectEqualStrings("tv/hd", form.get("cat"));
    try testing.expectEqualStrings("+", form.get("t"));
}

test "malformed percent-encoding is a parse failure, not a silent pass" {
    var arena = Arena.init();
    defer arena.deinit();
    try testing.expectError(error.BadForm, parse(arena.a(), .{ .query = "mode=%zz" }));
    try testing.expectError(error.BadForm, parse(arena.a(), .{ .query = "mode=%" }));
    // A decoded NUL is refused where Go accepted it. See the module note.
    try testing.expectError(error.BadForm, parse(arena.a(), .{ .query = "mode=a%00b" }));
}

test "a urlencoded body is read only for body-bearing methods" {
    var arena = Arena.init();
    defer arena.deinit();
    const in: Input = .{
        .query = "mode=queue",
        .content_type = "application/x-www-form-urlencoded",
        .body = "mode=history&name=delete",
    };
    // GET: the body is ignored, exactly as Go's ParseForm ignored it.
    const as_get = try parse(arena.a(), in);
    try testing.expectEqualStrings("queue", as_get.get("mode"));
    try testing.expectEqualStrings("", as_get.get("name"));

    var post_in = in;
    post_in.body_bearing = true;
    const as_post = try parse(arena.a(), post_in);
    try testing.expectEqualStrings("history", as_post.get("mode"));
    try testing.expectEqualStrings("delete", as_post.get("name"));
}

test "the body wins over the query even when its value is empty" {
    var arena = Arena.init();
    defer arena.deinit();
    // Go's formGet: PostFormValue returns "", then FormValue reads
    // Form[key][0], which is that same empty body value.
    const form = try parse(arena.a(), .{
        .body_bearing = true,
        .query = "cat=tv",
        .content_type = "application/x-www-form-urlencoded",
        .body = "cat=",
    });
    try testing.expectEqualStrings("", form.get("cat"));
    try testing.expect(form.has("cat"));
}

test "the first occurrence within one source wins" {
    var arena = Arena.init();
    defer arena.deinit();
    const form = try parse(arena.a(), .{ .query = "mode=queue&mode=history" });
    try testing.expectEqualStrings("queue", form.get("mode"));
}

test "a content type with no form payload contributes nothing" {
    var arena = Arena.init();
    defer arena.deinit();
    const form = try parse(arena.a(), .{
        .body_bearing = true,
        .query = "mode=addfile",
        .content_type = "application/json",
        .body = "{\"mode\":\"queue\"}",
    });
    try testing.expectEqualStrings("addfile", form.get("mode"));
}

const test_boundary = "----WebKitFormBoundaryABC123";

fn multipartBody(comptime parts: []const u8) []const u8 {
    return "--" ++ test_boundary ++ "\r\n" ++ parts ++ "--" ++ test_boundary ++ "--\r\n";
}

test "a multipart upload yields the file part and the value parts" {
    var arena = Arena.init();
    defer arena.deinit();
    const body = multipartBody(
        "Content-Disposition: form-data; name=\"name\"; filename=\"Release.Name.S01E01.nzb\"\r\n" ++
            "Content-Type: application/x-nzb\r\n\r\n" ++
            "<nzb><file/></nzb>\r\n" ++
            "--" ++ test_boundary ++ "\r\n" ++
            "Content-Disposition: form-data; name=\"cat\"\r\n\r\n" ++
            "tv\r\n" ++
            "--" ++ test_boundary ++ "\r\n" ++
            "Content-Disposition: form-data; name=\"apikey\"\r\n\r\n" ++
            "secret\r\n",
    );
    const form = try parse(arena.a(), .{
        .body_bearing = true,
        .content_type = "multipart/form-data; boundary=" ++ test_boundary,
        .body = body,
    });
    try testing.expectEqualStrings("tv", form.get("cat"));
    try testing.expectEqualStrings("secret", form.get("apikey"));
    const file = form.firstFile(&.{ "name", "nzbfile" }).?;
    try testing.expectEqualStrings("name", file.field);
    try testing.expectEqualStrings("Release.Name.S01E01.nzb", file.filename);
    try testing.expectEqualStrings("<nzb><file/></nzb>", file.content);
}

test "the nzbfile part name is accepted too, and order of preference holds" {
    var arena = Arena.init();
    defer arena.deinit();
    const body = multipartBody(
        "Content-Disposition: form-data; name=\"nzbfile\"; filename=\"a.nzb\"\r\n\r\n" ++
            "AAA\r\n",
    );
    const form = try parse(arena.a(), .{
        .body_bearing = true,
        .content_type = "multipart/form-data; boundary=\"" ++ test_boundary ++ "\"",
        .body = body,
    });
    const file = form.firstFile(&.{ "name", "nzbfile" }).?;
    try testing.expectEqualStrings("nzbfile", file.field);
    try testing.expectEqualStrings("AAA", file.content);
}

test "a multipart body with no file part reports none" {
    var arena = Arena.init();
    defer arena.deinit();
    const body = multipartBody("Content-Disposition: form-data; name=\"cat\"\r\n\r\ntv\r\n");
    const form = try parse(arena.a(), .{
        .body_bearing = true,
        .content_type = "multipart/form-data; boundary=" ++ test_boundary,
        .body = body,
    });
    try testing.expectEqual(@as(?FilePart, null), form.firstFile(&.{ "name", "nzbfile" }));
    try testing.expectEqualStrings("tv", form.get("cat"));
}

test "an NZB containing the boundary text is not truncated at it" {
    var arena = Arena.init();
    defer arena.deinit();
    // Only a CRLF-prefixed delimiter ends a part, so the same characters
    // mid-line are ordinary content.
    const payload = "<nzb>--" ++ test_boundary ++ " inline</nzb>";
    const body = multipartBody(
        "Content-Disposition: form-data; name=\"name\"; filename=\"x.nzb\"\r\n\r\n" ++
            payload ++ "\r\n",
    );
    const form = try parse(arena.a(), .{
        .body_bearing = true,
        .content_type = "multipart/form-data; boundary=" ++ test_boundary,
        .body = body,
    });
    try testing.expectEqualStrings(payload, form.firstFile(&.{"name"}).?.content);
}

test "binary content with embedded CRLF survives intact" {
    var arena = Arena.init();
    defer arena.deinit();
    const payload = "line1\r\nline2\r\n--not-the-boundary\r\nline3";
    const body = multipartBody(
        "Content-Disposition: form-data; name=\"name\"; filename=\"x.nzb\"\r\n\r\n" ++
            payload ++ "\r\n",
    );
    const form = try parse(arena.a(), .{
        .body_bearing = true,
        .content_type = "multipart/form-data; boundary=" ++ test_boundary,
        .body = body,
    });
    try testing.expectEqualStrings(payload, form.firstFile(&.{"name"}).?.content);
}

test "a multipart body missing its boundary parameter or delimiter is rejected" {
    var arena = Arena.init();
    defer arena.deinit();
    try testing.expectError(error.BadForm, parse(arena.a(), .{
        .body_bearing = true,
        .content_type = "multipart/form-data",
        .body = "whatever",
    }));
    try testing.expectError(error.BadForm, parse(arena.a(), .{
        .body_bearing = true,
        .content_type = "multipart/form-data; boundary=xyz",
        .body = "no delimiter here",
    }));
}

test "a filename with directory components is reduced to its last element" {
    var arena = Arena.init();
    defer arena.deinit();
    const body = multipartBody(
        "Content-Disposition: form-data; name=\"name\"; " ++
            "filename=\"../../etc/passwd/Release.nzb\"\r\n\r\nX\r\n",
    );
    const form = try parse(arena.a(), .{
        .body_bearing = true,
        .content_type = "multipart/form-data; boundary=" ++ test_boundary,
        .body = body,
    });
    try testing.expectEqualStrings("Release.nzb", form.firstFile(&.{"name"}).?.filename);
}

test "base reproduces filepath.Base" {
    try testing.expectEqualStrings(".", base(""));
    try testing.expectEqualStrings("/", base("/"));
    try testing.expectEqualStrings("/", base("///"));
    try testing.expectEqualStrings("a", base("a"));
    try testing.expectEqualStrings("c", base("a/b/c"));
    try testing.expectEqualStrings("b", base("a/b/"));
    try testing.expectEqualStrings("..", base("../.."));
    try testing.expectEqualStrings("passwd", base("/etc/passwd"));
}

test "mediaTypeIs ignores parameters and case" {
    try testing.expect(mediaTypeIs("multipart/form-data; boundary=x", "multipart/form-data"));
    try testing.expect(mediaTypeIs("MULTIPART/Form-Data", "multipart/form-data"));
    try testing.expect(mediaTypeIs(" application/x-www-form-urlencoded ; charset=utf-8", "application/x-www-form-urlencoded"));
    try testing.expect(!mediaTypeIs("application/json", "multipart/form-data"));
    try testing.expect(!mediaTypeIs("", "multipart/form-data"));
}

test "paramValue handles quoted, bare and absent parameters" {
    try testing.expectEqualStrings("x", paramValue("multipart/form-data; boundary=x", "boundary").?);
    try testing.expectEqualStrings("a b", paramValue("multipart/form-data; boundary=\"a b\"", "boundary").?);
    // A quoted value containing the parameter separator.
    try testing.expectEqualStrings("a;b", paramValue("x/y; boundary=\"a;b\"; z=1", "boundary").?);
    try testing.expectEqualStrings("R.nzb", paramValue("attachment; filename=\"R.nzb\"", "filename").?);
    try testing.expectEqualStrings("R.nzb", paramValue("attachment; filename=R.nzb", "filename").?);
    try testing.expectEqualStrings("R.nzb", paramValue("attachment; FileName = \"R.nzb\"", "filename").?);
    try testing.expectEqual(@as(?[]const u8, null), paramValue("attachment", "filename"));
    try testing.expectEqual(@as(?[]const u8, null), paramValue("attachment; filename=", "filename"));
    // Backslash escapes are refused rather than half-unescaped.
    try testing.expectEqual(@as(?[]const u8, null), paramValue("attachment; filename=\"a\\\"b\"", "filename"));
}

test "the user agent rides along for the job's source field" {
    var arena = Arena.init();
    defer arena.deinit();
    const form = try parse(arena.a(), .{ .query = "mode=version", .user_agent = "Sonarr/4.0" });
    try testing.expectEqualStrings("Sonarr/4.0", form.user_agent);
}
