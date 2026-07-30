//! The SABnzbd API surface that Sonarr / Radarr / Lidarr / Readarr /
//! Prowlarr expect from a download client.
//!
//! This is an anti-corruption layer: requests in SAB shapes are translated
//! into hoardarr operations through the ports in `ports.zig`, and responses
//! are encoded in the JSON shapes the *arr suite reads (`dto.zig`).
//!
//! Design choices that are not obvious:
//!
//!   * **`mode=` dispatch.** Real SAB multiplexes its entire API through
//!     one URL with a `mode` parameter, so `/sabnzbd/api` accepts every
//!     method and routes on `mode`. An unrecognised mode is a **400 with
//!     `status:false`**, never a 404 — a 404 makes *arr conclude the whole
//!     download client is misconfigured and stop talking to it, whereas a
//!     400 on one call is a call that failed.
//!
//!   * **`apikey` in a parameter, not a header.** SAB takes the key as a
//!     query or form field. That is why the routes are registered
//!     `.public` and this handler authenticates itself: the server's own
//!     middleware would also accept a session cookie (which no *arr has)
//!     and would *not* see a key that arrived in a multipart body — which
//!     is exactly where `mode=addfile` puts it. **No mode is exempt**,
//!     `version` included: an unauthenticated caller learns nothing here,
//!     not even that hoardarr is running.
//!
//!   * **`nzo_id` is opaque but reversible.** See `nzo.zig`. The *arr
//!     suite uses these as primary keys for queue identity, so they must
//!     stay stable for the life of a job.
//!
//!   * **The version we report lies.** See `dto.reported_version`.
//!
//! ## Shape of the port
//!
//! `dispatch` is a pure function of `(ports, form.Input) -> (status,
//! body)`. Everything else in this file is either an error-response
//! helper or eight lines of glue onto `net/http`. That split is what lets
//! the whole surface be tested without a socket, a store, or the app
//! layer — and it is why the tests below can assert *exact response
//! bytes* for every mode.

const std = @import("std");
const log = @import("../../core/log.zig");
const server = @import("../../net/http/server.zig");
const httpreq = @import("../../net/http/request.zig");
const sys = @import("../../posix/sys.zig");

const dto = @import("dto.zig");
const form = @import("form.zig");
const fmt = @import("fmt.zig");
const nzo = @import("nzo.zig");
const ports = @import("ports.zig");
const sort_eval = @import("sort_eval.zig");

const Allocator = std.mem.Allocator;

/// Path the *arr clients are configured with.
pub const mount_path = "/sabnzbd/api";
/// Some clients path-prefix without `/api`; SAB itself accepts both.
pub const mount_prefix = "/sabnzbd/";

/// One rendered response. `body` is allocated from the request arena.
pub const Result = struct {
    status: u16,
    body: []const u8,
};

/// An `Allocating` writer can only fail by failing to allocate; std
/// surfaces that as the generic `WriteFailed`. Translate it back so the
/// handler's error set stays honest — allocation is genuinely its only
/// failure mode.
fn oom(e: std.Io.Writer.Error) Allocator.Error {
    return switch (e) {
        error.WriteFailed => error.OutOfMemory,
    };
}

fn realNow(_: ?*anyopaque) i64 {
    return @intCast(@divFloor(sys.realtimeNanos(), std.time.ns_per_s));
}

pub const Handler = struct {
    api_key: ports.ApiKeyPort,
    queue: ports.QueuePort,
    categories: ports.CategoryPort,
    add_job: ports.AddJobPort,
    /// `mode=addurl` needs one. Absent, that mode answers 502 rather than
    /// pretending to have fetched something.
    fetch: ?ports.FetchPort = null,
    /// Absent, the queue reports 0 B/s and "unknown" ETAs, which is what
    /// the Go build did before throughput was wired.
    throughput: ?ports.ThroughputPort = null,
    clock: ports.ClockPort = .{ .nowFn = &realNow },
    /// Reported as `misc.complete_dir` and used to guess history
    /// `storage` paths.
    complete_dir: []const u8 = "",
    /// 5xx responses are logged; 4xx are the client's problem and would
    /// only give an unauthenticated caller a way to fill the log.
    logger: ?*log.Logger = null,

    // -- dispatch ------------------------------------------------------

    /// Authenticates, then routes on `mode=`. The only failure mode is
    /// allocation: every other error becomes a SAB error document, which
    /// is what consumers know how to read.
    pub fn dispatch(self: *Handler, gpa: Allocator, in: form.Input) Allocator.Error!Result {
        const f = form.parse(gpa, in) catch |e| switch (e) {
            error.OutOfMemory => return error.OutOfMemory,
            // Go wrapped `ParseForm`'s error as "parse form: …". The
            // wording differs; the status and the shape do not.
            error.BadForm => return self.fail(gpa, 400, "parse form: malformed parameters"),
        };

        if (!server.constantTimeStringEq(f.get("apikey"), self.api_key.key())) {
            return self.fail(gpa, 401, "invalid apikey");
        }

        const mode = f.get("mode");
        if (mode.len == 0) return self.fail(gpa, 400, "mode= required");

        if (std.mem.eql(u8, mode, "version")) return self.modeVersion(gpa);
        if (std.mem.eql(u8, mode, "get_config")) return self.modeGetConfig(gpa);
        if (std.mem.eql(u8, mode, "get_cats")) return self.modeGetCats(gpa);
        if (std.mem.eql(u8, mode, "addfile")) return self.modeAddFile(gpa, f);
        if (std.mem.eql(u8, mode, "addurl")) return self.modeAddUrl(gpa, f);
        if (std.mem.eql(u8, mode, "queue")) return self.modeQueue(gpa, f);
        if (std.mem.eql(u8, mode, "history")) return self.modeHistory(gpa, f);
        if (std.mem.eql(u8, mode, "get_files")) return self.modeGetFiles(gpa, f);
        if (std.mem.eql(u8, mode, "eval_sort")) return self.modeEvalSort(gpa, f);

        return self.failQuoted(gpa, 400, "mode=", mode, " not implemented");
    }

    // -- modes ---------------------------------------------------------

    fn modeVersion(self: *Handler, gpa: Allocator) Allocator.Error!Result {
        _ = self;
        var out = Out.init(gpa);
        dto.writeVersion(out.w()) catch |e| return oom(e);
        return out.ok();
    }

    /// Real SAB returns a vast nested structure. We return only the parts
    /// consumers read — `misc.complete_dir` and the categories — because
    /// synthesising the rest would be inventing values clients might act
    /// on.
    fn modeGetConfig(self: *Handler, gpa: Allocator) Allocator.Error!Result {
        const cats = self.categories.list(gpa) catch |e| return self.failErr(gpa, 500, e);
        var out = Out.init(gpa);
        dto.writeConfig(out.w(), self.complete_dir, cats) catch |e| return oom(e);
        return out.ok();
    }

    fn modeGetCats(self: *Handler, gpa: Allocator) Allocator.Error!Result {
        const cats = self.categories.list(gpa) catch |e| return self.failErr(gpa, 500, e);
        var out = Out.init(gpa);
        dto.writeCats(out.w(), cats) catch |e| return oom(e);
        return out.ok();
    }

    /// A multipart NZB upload. SAB names the file part `name`; several
    /// forks use `nzbfile`, and we accept either.
    fn modeAddFile(self: *Handler, gpa: Allocator, f: form.Form) Allocator.Error!Result {
        const part = f.firstFile(&.{ "name", "nzbfile" }) orelse
            return self.fail(gpa, 400, "no nzb file part (expected name= or nzbfile=)");

        // Display name from the upload's filename, so a client posting
        // "Release.Name.S01E01.nzb" shows up under that label in the
        // queue. Both spellings of the extension are stripped, in Go's
        // order.
        const display = trimNzbSuffix(part.filename);

        const outcome = self.add_job.add(gpa, .{
            .nzb = part.content,
            .name = display,
            .category = f.get("cat"),
            .source = f.user_agent,
        }) catch |e| return self.failErr(gpa, 400, e);

        return self.respondWithHandle(gpa, outcome.id);
    }

    /// Fetches an NZB by URL and routes the bytes through the same
    /// submission path as `addfile`. Sonarr's "Send NZB by URL" hits this;
    /// without it that button silently fails and the job never appears.
    fn modeAddUrl(self: *Handler, gpa: Allocator, f: form.Form) Allocator.Error!Result {
        const url = std.mem.trim(u8, f.get("name"), " \t\r\n");
        if (url.len == 0) return self.fail(gpa, 400, "name= (url) required");
        if (!std.mem.startsWith(u8, url, "http://") and !std.mem.startsWith(u8, url, "https://")) {
            return self.fail(gpa, 400, "url must be http:// or https://");
        }
        const fetch = self.fetch orelse
            return self.fail(gpa, 502, "fetch nzb: no fetch transport configured");

        const resp = fetch.get(gpa, url) catch |e| switch (e) {
            error.OutOfMemory => return error.OutOfMemory,
            error.Fetch => return self.fail(gpa, 502, "fetch nzb: transport failure"),
        };
        if (resp.status / 100 != 2) {
            var buf: [64]u8 = undefined;
            const msg = std.fmt.bufPrint(&buf, "fetch nzb: upstream {d}", .{resp.status}) catch
                "fetch nzb: upstream error";
            return self.fail(gpa, 502, msg);
        }

        // Display-name precedence: explicit nzbname=, then
        // Content-Disposition (indexers send `attachment;
        // filename="Release.Name.nzb"`), then the URL path, then a
        // placeholder.
        var display = std.mem.trim(u8, f.get("nzbname"), " \t\r\n");
        if (display.len == 0) {
            if (form.paramValue(resp.content_disposition, "filename")) |fn_raw| {
                // Reduced to its last path element. Go did not do this
                // here (it did for the multipart upload); a display name
                // becomes a directory name downstream, and letting a
                // remote indexer put "../" in it is not a capability
                // worth preserving for fidelity's sake.
                display = trimNzbSuffix(form.base(fn_raw));
            }
        }
        if (display.len == 0) display = filenameFromUrl(url);
        if (display.len == 0) display = "addurl-job";

        const source = try std.mem.concat(gpa, u8, &.{ f.user_agent, " (addurl)" });
        const outcome = self.add_job.add(gpa, .{
            .nzb = resp.body,
            .name = display,
            .category = f.get("cat"),
            .source = source,
        }) catch |e| return self.failErr(gpa, 400, e);

        return self.respondWithHandle(gpa, outcome.id);
    }

    /// Overloaded: a bare `mode=queue` lists, `name=pause|resume|delete`
    /// mutates. Real SAB also has reorder / setpriority, which nothing in
    /// the *arr suite calls.
    fn modeQueue(self: *Handler, gpa: Allocator, f: form.Form) Allocator.Error!Result {
        const name = f.get("name");
        if (name.len == 0) return self.modeQueueList(gpa);
        if (std.mem.eql(u8, name, "pause")) return self.queueAction(gpa, f, .pause);
        if (std.mem.eql(u8, name, "resume")) return self.queueAction(gpa, f, .@"resume");
        if (std.mem.eql(u8, name, "delete")) return self.queueAction(gpa, f, .delete);
        return self.failQuoted(gpa, 400, "queue.name=", name, " not implemented");
    }

    fn modeQueueList(self: *Handler, gpa: Allocator) Allocator.Error!Result {
        const jobs = self.queue.activeJobs(gpa) catch |e| return self.failErr(gpa, 500, e);
        const rate: i64 = if (self.throughput) |t| t.rate() else 0;
        var out = Out.init(gpa);
        dto.writeQueue(out.w(), jobs, rate, self.clock.now()) catch |e| return oom(e);
        return out.ok();
    }

    const Action = enum { pause, @"resume", delete, mark_completed };

    /// `value=` is a comma-separated list of nzo_ids. The Go loop aborts
    /// on the first failure — including partway through the list, leaving
    /// the earlier ids already actioned. Preserved: *arr retries the whole
    /// call, and every action here is idempotent.
    fn queueAction(
        self: *Handler,
        gpa: Allocator,
        f: form.Form,
        action: Action,
    ) Allocator.Error!Result {
        var ids: std.ArrayList([]const u8) = .empty;
        var it = std.mem.splitScalar(u8, f.get("value"), ',');
        while (it.next()) |part| {
            const raw = std.mem.trim(u8, part, " \t\r\n");
            if (raw.len == 0) continue;
            const id = nzo.jobIdFrom(raw) catch |e|
                return self.failNzo(gpa, raw, @errorName(e));
            const r = switch (action) {
                .pause => self.queue.pause(id),
                .@"resume" => self.queue.@"resume"(id),
                .delete => self.queue.remove(id),
                .mark_completed => self.queue.markCompleted(id),
            };
            r catch |e| return self.failErr(gpa, 400, e);
            try ids.append(gpa, raw);
        }
        var out = Out.init(gpa);
        dto.writeStatusIds(out.w(), ids.items) catch |e| return oom(e);
        return out.ok();
    }

    fn modeHistory(self: *Handler, gpa: Allocator, f: form.Form) Allocator.Error!Result {
        const name = f.get("name");
        if (name.len == 0) return self.modeHistoryList(gpa, f);
        // History delete is the same plumbing as queue delete: the store
        // does not distinguish, and removing a terminal row is just an
        // update that clears it from the history view.
        if (std.mem.eql(u8, name, "delete")) return self.queueAction(gpa, f, .delete);
        if (std.mem.eql(u8, name, "mark_as_completed")) {
            return self.queueAction(gpa, f, .mark_completed);
        }
        return self.failQuoted(gpa, 400, "history.name=", name, " not implemented");
    }

    fn modeHistoryList(self: *Handler, gpa: Allocator, f: form.Form) Allocator.Error!Result {
        var limit: usize = 100;
        const raw = f.get("limit");
        if (raw.len > 0) {
            // A malformed or non-positive limit falls back to the default
            // rather than 400-ing, as Go's `if n, err := Atoi; err == nil
            // && n > 0` did.
            if (std.fmt.parseInt(usize, raw, 10)) |n| {
                if (n > 0) limit = n;
            } else |_| {}
        }
        const jobs = self.queue.historyJobs(gpa, limit) catch |e|
            return self.failErr(gpa, 500, e);
        var out = Out.init(gpa);
        dto.writeHistory(out.w(), jobs, self.complete_dir) catch |e| return oom(e);
        return out.ok();
    }

    /// Per-file listing for one job. Some Sonarr versions enumerate these
    /// in the queue detail view.
    fn modeGetFiles(self: *Handler, gpa: Allocator, f: form.Form) Allocator.Error!Result {
        const raw = std.mem.trim(u8, f.get("value"), " \t\r\n");
        if (raw.len == 0) return self.fail(gpa, 400, "value= (nzo_id) required");
        const id = nzo.jobIdFrom(raw) catch |e| return self.failNzo(gpa, raw, @errorName(e));
        const detail = self.queue.get(gpa, id) catch |e| return self.failErr(gpa, 404, e);
        var out = Out.init(gpa);
        dto.writeFiles(out.w(), detail.files) catch |e| return oom(e);
        return out.ok();
    }

    /// Renders a SAB sort template. *arr calls this to preview where an
    /// import will land; a 4xx makes it refuse the download client
    /// outright, which is why `evalSort` never fails on content.
    fn modeEvalSort(self: *Handler, gpa: Allocator, f: form.Form) Allocator.Error!Result {
        const template = f.get("name");
        if (template.len == 0) return self.fail(gpa, 400, "name= (template) required");
        const ctx = sort_eval.buildSortContext(.{
            .ctx = @ptrCast(&f),
            .getFn = &formGetter,
        });
        const result = try sort_eval.evalSort(gpa, template, ctx);
        var out = Out.init(gpa);
        dto.writeEvalSort(out.w(), result) catch |e| return oom(e);
        return out.ok();
    }

    // -- responses -----------------------------------------------------

    /// The `addfile` / `addurl` answer. Looks the job back up so the
    /// handle carries its NZB hash, which is what makes an nzo_id unique
    /// per *creation* rather than per row id — see `nzo.zig`. A failed
    /// lookup degrades to the bare encoding rather than failing the
    /// submission, which has already succeeded.
    fn respondWithHandle(self: *Handler, gpa: Allocator, id: ports.JobId) Allocator.Error!Result {
        var buf: nzo.Buf = undefined;
        var handle: []const u8 = nzo.encode(&buf, id);
        if (id != 0) {
            if (self.queue.get(gpa, id)) |detail| {
                handle = nzo.encodeWithHash(&buf, id, detail.job.nzb_hash);
            } else |_| {}
        }
        var out = Out.init(gpa);
        dto.writeStatusIds(out.w(), &.{handle}) catch |e| return oom(e);
        return out.ok();
    }

    fn fail(self: *Handler, gpa: Allocator, status: u16, message: []const u8) Allocator.Error!Result {
        if (status >= 500) {
            if (self.logger) |l| {
                l.err("sab handler", &.{ log.int("status", status), log.str("err", message) });
            }
        }
        var out = Out.init(gpa);
        dto.writeError(out.w(), message) catch |e| return oom(e);
        return .{ .status = status, .body = out.bytes() };
    }

    /// A port error, rendered by name. The Go handler echoed
    /// `err.Error()`; the text differs, the shape and the status do not.
    fn failErr(self: *Handler, gpa: Allocator, status: u16, e: anyerror) Allocator.Error!Result {
        if (e == error.OutOfMemory) return error.OutOfMemory;
        return self.fail(gpa, status, @errorName(e));
    }

    /// `nzo_id "<raw>": <reason>`, mirroring Go's `fmt.Errorf("nzo_id %q:
    /// %w", …)`.
    fn failNzo(
        self: *Handler,
        gpa: Allocator,
        raw: []const u8,
        reason: []const u8,
    ) Allocator.Error!Result {
        const msg = try std.mem.concat(gpa, u8, &.{ "nzo_id \"", raw, "\": ", reason });
        return self.fail(gpa, 400, msg);
    }

    /// `<prefix>"<value>"<suffix>` — the `%q`-quoted "not implemented"
    /// messages. The value is client-supplied and is escaped by the JSON
    /// writer, so a hostile mode name cannot break the document.
    fn failQuoted(
        self: *Handler,
        gpa: Allocator,
        status: u16,
        comptime prefix: []const u8,
        value: []const u8,
        comptime suffix: []const u8,
    ) Allocator.Error!Result {
        const msg = try std.mem.concat(gpa, u8, &.{ prefix, "\"", value, "\"", suffix });
        return self.fail(gpa, status, msg);
    }
};

/// Growable body buffer over the request arena. The arena owns the bytes,
/// so there is no `deinit`: they live exactly as long as the response.
const Out = struct {
    alloc: std.Io.Writer.Allocating,

    fn init(gpa: Allocator) Out {
        return .{ .alloc = .init(gpa) };
    }

    fn w(self: *Out) *std.Io.Writer {
        return &self.alloc.writer;
    }

    fn bytes(self: *Out) []const u8 {
        return self.alloc.writer.buffered();
    }

    fn ok(self: *Out) Result {
        return .{ .status = 200, .body = self.bytes() };
    }
};

fn formGetter(ctx: *const anyopaque, name: []const u8) []const u8 {
    const f: *const form.Form = @ptrCast(@alignCast(ctx));
    return f.get(name);
}

/// Both spellings of the extension, stripped in Go's order: `.nzb` first,
/// then `.NZB`. Sequential, not repeated — `Release.NZB` loses its suffix
/// and `Release.nzb.NZB` keeps the inner one, which is what
/// `strings.TrimSuffix` twice in a row did.
fn trimNzbSuffix(name: []const u8) []const u8 {
    var s = name;
    if (std.mem.endsWith(u8, s, ".nzb")) s = s[0 .. s.len - 4];
    if (std.mem.endsWith(u8, s, ".NZB")) s = s[0 .. s.len - 4];
    return s;
}

/// A display name out of a URL like
/// `https://indexer.example/getnzb?id=abc.nzb&apikey=…`: drop the query,
/// take the last path element, drop a `.nzb` suffix.
pub fn filenameFromUrl(u_in: []const u8) []const u8 {
    var u = u_in;
    if (std.mem.indexOfScalar(u8, u, '?')) |i| u = u[0..i];
    if (std.mem.lastIndexOfScalar(u8, u, '/')) |i| u = u[i + 1 ..];
    return trimNzbSuffix(u);
}

// ---------------------------------------------------------------------
// HTTP glue
// ---------------------------------------------------------------------

/// Builds the `form.Input` for a live request. Header slices point into
/// the connection buffer and are valid for the duration of the handler,
/// which is exactly how long the `Form` lives.
pub fn inputFromRequest(req: *const httpreq.Request) form.Input {
    return .{
        .body_bearing = switch (req.method) {
            .post, .put, .patch => true,
            else => false,
        },
        .query = req.query,
        .content_type = req.header("content-type") orelse "",
        .body = req.body,
        .user_agent = req.header("user-agent") orelse "",
    };
}

/// The two routes to register, given the application struct the server was
/// wired with and the name of the field holding this `Handler`.
///
/// Both are `.public` and match every method — see the module comment for
/// why the server's own auth must not gate this surface, and why POST as
/// well as GET has to reach it.
pub fn routes(comptime App: type, comptime field_name: []const u8) [2]server.Route {
    const Glue = struct {
        fn serve(ctx: *server.Ctx) server.HandlerError!void {
            const h: *Handler = &@field(ctx.app(App), field_name);
            var arena: std.heap.ArenaAllocator = .init(ctx.server.gpa);
            defer arena.deinit();
            const result = try h.dispatch(arena.allocator(), inputFromRequest(ctx.req));
            try ctx.res.send(result.status, "application/json", result.body);
        }
    };
    return .{
        .{ .method = null, .path = mount_path, .handler = &Glue.serve, .access = .public },
        .{
            .method = null,
            .path = mount_prefix,
            .kind = .prefix,
            .handler = &Glue.serve,
            .access = .public,
        },
    };
}

// ---------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------

const testing = std.testing;

const key = "0123456789abcdef0123456789abcdef";

/// One handler wired entirely to fakes, plus the arena every dispatch
/// writes into.
const Fixture = struct {
    arena: std.heap.ArenaAllocator,
    queue: ports.FakeQueue = .{},
    cats: ports.FakeCategories = .{},
    add: ports.FakeAddJob = .{},
    fetch: ports.FakeFetch = .{},
    api_key: ports.FakeKey = .{ .value = key },
    clock: ports.FakeClock = .{ .unix_secs = 1700000000 },
    throughput: ports.FakeThroughput = .{},
    handler: Handler = undefined,

    fn init(self: *Fixture) void {
        self.arena = .init(testing.allocator);
        self.handler = .{
            .api_key = self.api_key.port(),
            .queue = self.queue.port(),
            .categories = self.cats.port(),
            .add_job = self.add.port(),
            .fetch = self.fetch.port(),
            .throughput = self.throughput.port(),
            .clock = self.clock.port(),
            .complete_dir = "/data/complete",
        };
    }

    fn deinit(self: *Fixture) void {
        self.arena.deinit();
    }

    /// A GET with `apikey` already appended.
    fn get(self: *Fixture, query: []const u8) !Result {
        const q = try std.mem.concat(self.arena.allocator(), u8, &.{ query, "&apikey=" ++ key });
        return self.handler.dispatch(self.arena.allocator(), .{ .query = q });
    }

    /// A GET with no key at all.
    fn raw(self: *Fixture, in: form.Input) !Result {
        return self.handler.dispatch(self.arena.allocator(), in);
    }
};

fn expectJsonField(body: []const u8, path: []const []const u8, want: []const u8) !void {
    const parsed = try std.json.parseFromSlice(std.json.Value, testing.allocator, body, .{});
    defer parsed.deinit();
    var cur = parsed.value;
    for (path) |seg| cur = cur.object.get(seg) orelse return error.MissingField;
    try testing.expectEqualStrings(want, cur.string);
}

// -- auth ------------------------------------------------------------

test "every mode requires the apikey, version included" {
    // The Go handler checked the key before dispatching, with no
    // exemptions. An unauthenticated caller cannot even learn the version.
    const modes = [_][]const u8{
        "version", "get_config", "get_cats",  "addfile",   "addurl",
        "queue",   "history",    "get_files", "eval_sort", "not_a_mode",
        "",
    };
    for (modes) |m| {
        var fx: Fixture = .{ .arena = undefined };
        fx.init();
        defer fx.deinit();
        const q = try std.mem.concat(fx.arena.allocator(), u8, &.{ "mode=", m });
        const r = try fx.raw(.{ .query = q });
        try testing.expectEqual(@as(u16, 401), r.status);
        try testing.expectEqualStrings("{\"error\":\"invalid apikey\",\"status\":false}\n", r.body);
    }
}

test "a wrong, truncated or extended key is rejected" {
    const bad = [_][]const u8{
        "",
        "0",
        key ++ "0",
        key[0 .. key.len - 1],
        "0123456789abcdef0123456789abcdee",
        "0123456789ABCDEF0123456789ABCDEF",
    };
    for (bad) |b| {
        var fx: Fixture = .{ .arena = undefined };
        fx.init();
        defer fx.deinit();
        const q = try std.mem.concat(fx.arena.allocator(), u8, &.{ "mode=version&apikey=", b });
        const r = try fx.raw(.{ .query = q });
        try testing.expectEqual(@as(u16, 401), r.status);
    }
}

test "an unconfigured server is not an open one" {
    var fx: Fixture = .{ .arena = undefined };
    fx.init();
    defer fx.deinit();
    fx.api_key.value = "";
    // An empty expected key must match nothing, including an empty
    // presented key.
    const r = try fx.raw(.{ .query = "mode=version&apikey=" });
    try testing.expectEqual(@as(u16, 401), r.status);
    const r2 = try fx.raw(.{ .query = "mode=version" });
    try testing.expectEqual(@as(u16, 401), r2.status);
}

test "the key is accepted from a multipart body, which is where addfile puts it" {
    var fx: Fixture = .{ .arena = undefined };
    fx.init();
    defer fx.deinit();
    const b = "--BB\r\n" ++
        "Content-Disposition: form-data; name=\"apikey\"\r\n\r\n" ++ key ++ "\r\n" ++
        "--BB\r\n" ++
        "Content-Disposition: form-data; name=\"mode\"\r\n\r\nversion\r\n" ++
        "--BB--\r\n";
    const r = try fx.raw(.{
        .body_bearing = true,
        .content_type = "multipart/form-data; boundary=BB",
        .body = b,
    });
    try testing.expectEqual(@as(u16, 200), r.status);
    try testing.expectEqualStrings("{\"version\":\"3.7.2\"}\n", r.body);
}

test "the key is read per request so a rotation takes effect immediately" {
    var fx: Fixture = .{ .arena = undefined };
    fx.init();
    defer fx.deinit();
    try testing.expectEqual(@as(u16, 200), (try fx.get("mode=version")).status);
    fx.api_key.value = "a-different-key-entirely-32-chars";
    try testing.expectEqual(@as(u16, 401), (try fx.get("mode=version")).status);
}

// -- dispatch --------------------------------------------------------

test "a missing mode is a 400, not a 404" {
    var fx: Fixture = .{ .arena = undefined };
    fx.init();
    defer fx.deinit();
    const r = try fx.get("mode=");
    try testing.expectEqual(@as(u16, 400), r.status);
    try testing.expectEqualStrings("{\"error\":\"mode= required\",\"status\":false}\n", r.body);

    // No mode parameter at all is the same case.
    const r2 = try fx.get("apikey=" ++ key);
    try testing.expectEqual(@as(u16, 400), r2.status);
}

test "an unknown mode is a 400 SAB error document, never a 404" {
    // A 404 makes *arr conclude the download client is misconfigured and
    // stop talking to it entirely.
    var fx: Fixture = .{ .arena = undefined };
    fx.init();
    defer fx.deinit();
    const r = try fx.get("mode=bogus");
    try testing.expectEqual(@as(u16, 400), r.status);
    try testing.expectEqualStrings(
        "{\"error\":\"mode=\\\"bogus\\\" not implemented\",\"status\":false}\n",
        r.body,
    );
}

test "a hostile mode name cannot break the error document" {
    var fx: Fixture = .{ .arena = undefined };
    fx.init();
    defer fx.deinit();
    const r = try fx.get("mode=%22%7D%2C%7B%22x%22%3A%22y");
    try testing.expectEqual(@as(u16, 400), r.status);
    try expectJsonField(r.body, &.{"error"}, "mode=\"\"},{\"x\":\"y\" not implemented");
}

test "mode=version answers the exact Go bytes" {
    var fx: Fixture = .{ .arena = undefined };
    fx.init();
    defer fx.deinit();
    const r = try fx.get("mode=version");
    try testing.expectEqual(@as(u16, 200), r.status);
    try testing.expectEqualStrings("{\"version\":\"3.7.2\"}\n", r.body);
}

test "a POST reaches the same dispatch as a GET" {
    var fx: Fixture = .{ .arena = undefined };
    fx.init();
    defer fx.deinit();
    const r = try fx.raw(.{
        .body_bearing = true,
        .content_type = "application/x-www-form-urlencoded",
        .body = "mode=version&apikey=" ++ key,
    });
    try testing.expectEqual(@as(u16, 200), r.status);
    try testing.expectEqualStrings("{\"version\":\"3.7.2\"}\n", r.body);
}

test "malformed parameters are a 400 rather than a crash" {
    var fx: Fixture = .{ .arena = undefined };
    fx.init();
    defer fx.deinit();
    const r = try fx.raw(.{ .query = "mode=version&apikey=%zz" });
    try testing.expectEqual(@as(u16, 400), r.status);
    try expectJsonField(r.body, &.{"error"}, "parse form: malformed parameters");
}

// -- config / cats ---------------------------------------------------

test "mode=get_config answers the exact Go bytes" {
    var fx: Fixture = .{ .arena = undefined };
    fx.init();
    defer fx.deinit();
    fx.cats.rows = &.{
        .{ .name = "*", .dir = "", .priority = 0 },
        .{ .name = "tv", .dir = "tv", .priority = 1 },
    };
    const r = try fx.get("mode=get_config");
    try testing.expectEqual(@as(u16, 200), r.status);
    try testing.expectEqualStrings(
        "{\"config\":{\"categories\":[{\"dir\":\"\",\"name\":\"*\",\"newzbin\":\"\"," ++
            "\"order\":0,\"pp\":\"\",\"priority\":0,\"script\":\"None\"},{\"dir\":\"tv\"," ++
            "\"name\":\"tv\",\"newzbin\":\"\",\"order\":1,\"pp\":\"\",\"priority\":1," ++
            "\"script\":\"None\"}],\"misc\":{\"complete_dir\":\"/data/complete\"," ++
            "\"version\":\"3.7.2\"}}}\n",
        r.body,
    );
}

test "mode=get_cats answers the exact Go bytes" {
    var fx: Fixture = .{ .arena = undefined };
    fx.init();
    defer fx.deinit();
    fx.cats.rows = &.{ .{ .name = "*" }, .{ .name = "tv" }, .{ .name = "movies" } };
    const r = try fx.get("mode=get_cats");
    try testing.expectEqual(@as(u16, 200), r.status);
    try testing.expectEqualStrings("{\"categories\":[\"*\",\"tv\",\"movies\"]}\n", r.body);
}

test "a category store failure is a 500 for both config modes" {
    for ([_][]const u8{ "mode=get_config", "mode=get_cats" }) |q| {
        var fx: Fixture = .{ .arena = undefined };
        fx.init();
        defer fx.deinit();
        fx.cats.fail = error.Unavailable;
        const r = try fx.get(q);
        try testing.expectEqual(@as(u16, 500), r.status);
        try expectJsonField(r.body, &.{"error"}, "Unavailable");
    }
}

// -- queue -----------------------------------------------------------

const queue_job: ports.JobView = .{
    .id = 42,
    .nzb_hash = "deadbeefcafe",
    .name = "Some.Release.S01E02.1080p-GRP",
    .category = "tv",
    .state = .downloading,
    .total_bytes = 1234567890,
    .done_bytes = 456789012,
    .added_at_ms = 1700000000_000,
};

test "mode=queue answers the exact Go bytes" {
    var fx: Fixture = .{ .arena = undefined };
    fx.init();
    defer fx.deinit();
    fx.queue.active = &.{queue_job};
    fx.throughput.bytes_per_sec = 1572864;
    const r = try fx.get("mode=queue");
    try testing.expectEqual(@as(u16, 200), r.status);
    try testing.expectEqualStrings(
        "{\"queue\":{\"diskspace1\":\"0\",\"diskspace2\":\"0\",\"diskspacetotal1\":\"0\"," ++
            "\"diskspacetotal2\":\"0\",\"finish\":1,\"kbpersec\":\"1536.00\",\"limit\":1," ++
            "\"mb\":\"1177.38\",\"mbleft\":\"741.75\",\"noofslots\":1,\"noofslots_total\":1," ++
            "\"paused\":false,\"size\":\"1.15 GB\",\"sizeleft\":\"741.75 MB\",\"slots\":[" ++
            "{\"_doneMB\":\"435.63\",\"avg_age\":\"0d\",\"cat\":\"tv\",\"direct_unpack\":null," ++
            "\"eta\":\"22:21 Tue 14 Nov\",\"filename\":\"Some.Release.S01E02.1080p-GRP\"," ++
            "\"index\":0,\"labels\":[],\"mb\":\"1177.38\",\"mbleft\":\"741.75\"," ++
            "\"mbmissing\":\"0.00\",\"missing\":0," ++
            "\"nzo_id\":\"SABnzbd_nzo_GQZDUZDFMFSGEZLFMY\",\"password\":\"\"," ++
            "\"percentage\":\"36\",\"priority\":\"Normal\",\"script\":\"None\"," ++
            "\"size\":\"1.15 GB\",\"sizeleft\":\"741.75 MB\",\"status\":\"Downloading\"," ++
            "\"time_added\":1700000000,\"timeleft\":\"0:08:14\",\"unpackopts\":\"3\"}]," ++
            "\"speed\":\"1.5 MB/s\",\"speedlimit\":\"0\",\"speedlimit_abs\":\"\",\"start\":0," ++
            "\"status\":\"Downloading\",\"timeleft\":\"0:08:14\",\"version\":\"3.7.2\"}}\n",
        r.body,
    );
}

test "without a throughput port the queue reports zero and unknown" {
    var fx: Fixture = .{ .arena = undefined };
    fx.init();
    defer fx.deinit();
    fx.handler.throughput = null;
    fx.queue.active = &.{queue_job};
    const r = try fx.get("mode=queue");
    try testing.expect(std.mem.indexOf(u8, r.body, "\"speed\":\"0 B/s\"") != null);
    try testing.expect(std.mem.indexOf(u8, r.body, "\"kbpersec\":\"0.00\"") != null);
    try testing.expect(std.mem.indexOf(u8, r.body, "\"eta\":\"unknown\"") != null);
}

test "a queue store failure is a 500" {
    var fx: Fixture = .{ .arena = undefined };
    fx.init();
    defer fx.deinit();
    fx.queue.fail_active = error.Unavailable;
    const r = try fx.get("mode=queue");
    try testing.expectEqual(@as(u16, 500), r.status);
}

test "queue pause, resume and delete act on the decoded ids" {
    var fx: Fixture = .{ .arena = undefined };
    fx.init();
    defer fx.deinit();
    const handle = "SABnzbd_nzo_GQZDUZDFMFSGEZLFMY"; // 42:deadbeef

    const p = try fx.get("mode=queue&name=pause&value=" ++ handle);
    try testing.expectEqual(@as(u16, 200), p.status);
    try testing.expectEqualStrings("{\"nzo_ids\":[\"" ++ handle ++ "\"],\"status\":true}\n", p.body);
    try testing.expectEqualSlices(ports.JobId, &.{42}, fx.queue.pauses());

    _ = try fx.get("mode=queue&name=resume&value=" ++ handle);
    try testing.expectEqualSlices(ports.JobId, &.{42}, fx.queue.resumes());

    _ = try fx.get("mode=queue&name=delete&value=" ++ handle);
    try testing.expectEqualSlices(ports.JobId, &.{42}, fx.queue.removes());
}

test "a comma-separated value list is actioned in order, blanks skipped" {
    var fx: Fixture = .{ .arena = undefined };
    fx.init();
    defer fx.deinit();
    // 42:deadbeef, 7:cafebabe, and the bare-id form for 9.
    const r = try fx.get(
        "mode=queue&name=delete&value=SABnzbd_nzo_GQZDUZDFMFSGEZLFMY,," ++
            "%20SABnzbd_nzo_G45GGYLGMVRGCYTF%20,SABnzbd_nzo_HE,",
    );
    try testing.expectEqual(@as(u16, 200), r.status);
    try testing.expectEqualSlices(ports.JobId, &.{ 42, 7, 9 }, fx.queue.removes());
    // The echoed ids are the trimmed originals, not re-encoded.
    try testing.expectEqualStrings(
        "{\"nzo_ids\":[\"SABnzbd_nzo_GQZDUZDFMFSGEZLFMY\",\"SABnzbd_nzo_G45GGYLGMVRGCYTF\"," ++
            "\"SABnzbd_nzo_HE\"],\"status\":true}\n",
        r.body,
    );
}

test "an empty value list is a 200 with no ids" {
    var fx: Fixture = .{ .arena = undefined };
    fx.init();
    defer fx.deinit();
    const r = try fx.get("mode=queue&name=pause&value=");
    try testing.expectEqual(@as(u16, 200), r.status);
    try testing.expectEqualStrings("{\"nzo_ids\":[],\"status\":true}\n", r.body);
    try testing.expectEqual(@as(usize, 0), fx.queue.n_paused);
}

test "a malformed nzo_id is a 400 naming the offending handle" {
    var fx: Fixture = .{ .arena = undefined };
    fx.init();
    defer fx.deinit();
    const r = try fx.get("mode=queue&name=delete&value=not-an-nzo-id");
    try testing.expectEqual(@as(u16, 400), r.status);
    try expectJsonField(r.body, &.{"error"}, "nzo_id \"not-an-nzo-id\": BadPrefix");
    try testing.expectEqual(@as(usize, 0), fx.queue.n_removed);

    const r2 = try fx.get("mode=queue&name=delete&value=SABnzbd_nzo_!!!!");
    try expectJsonField(r2.body, &.{"error"}, "nzo_id \"SABnzbd_nzo_!!!!\": BadBase32");
}

test "a refused command is a 400" {
    var fx: Fixture = .{ .arena = undefined };
    fx.init();
    defer fx.deinit();
    fx.queue.fail_command = error.Rejected;
    const r = try fx.get("mode=queue&name=pause&value=SABnzbd_nzo_GQZDUZDFMFSGEZLFMY");
    try testing.expectEqual(@as(u16, 400), r.status);
    try expectJsonField(r.body, &.{"error"}, "Rejected");
}

test "an unimplemented queue action is a 400 that names it" {
    var fx: Fixture = .{ .arena = undefined };
    fx.init();
    defer fx.deinit();
    const r = try fx.get("mode=queue&name=reorder");
    try testing.expectEqual(@as(u16, 400), r.status);
    try expectJsonField(r.body, &.{"error"}, "queue.name=\"reorder\" not implemented");
}

// -- history ---------------------------------------------------------

test "mode=history answers the exact Go bytes" {
    var fx: Fixture = .{ .arena = undefined };
    fx.init();
    defer fx.deinit();
    fx.queue.history = &.{.{
        .id = 42,
        .nzb_hash = "deadbeefcafe",
        .name = "Some.Release.S01E02.1080p-GRP",
        .category = "tv",
        .state = .completed,
        .total_bytes = 1234567890,
        .done_bytes = 1234567890,
        .added_at_ms = 1700000000_000,
        .finished_at_ms = 1700003600_000,
    }};
    const r = try fx.get("mode=history");
    try testing.expectEqual(@as(u16, 200), r.status);
    try testing.expectEqualStrings(
        "{\"history\":{\"day_size\":\"0 B\",\"month_size\":\"0 B\",\"noofslots\":1,\"slots\":[" ++
            "{\"action_line\":\"\",\"archive\":false,\"bytes\":1234567890,\"category\":\"tv\"," ++
            "\"completed\":1700003600,\"completeness\":null,\"download_time\":0," ++
            "\"downloaded\":1234567890,\"duplicate_key\":\"\",\"fail_message\":\"\",\"id\":42," ++
            "\"loaded\":false,\"md5sum\":\"\",\"meta\":null," ++
            "\"name\":\"Some.Release.S01E02.1080p-GRP\"," ++
            "\"nzb_name\":\"Some.Release.S01E02.1080p-GRP.nzb\"," ++
            "\"nzo_id\":\"SABnzbd_nzo_GQZDUZDFMFSGEZLFMY\",\"password\":\"\"," ++
            "\"path\":\"/data/complete/Some.Release.S01E02.1080p-GRP\",\"postproc_time\":0," ++
            "\"pp\":\"X\",\"report\":\"\",\"retry\":false,\"script\":\"None\"," ++
            "\"script_line\":\"\",\"series\":\"\",\"size\":\"1.15 GB\",\"stage_log\":[]," ++
            "\"status\":\"Completed\"," ++
            "\"storage\":\"/data/complete/Some.Release.S01E02.1080p-GRP\"," ++
            "\"time_added\":1700000000,\"url\":\"\",\"url_info\":\"\"}]," ++
            "\"total_size\":\"1.15 GB\",\"version\":\"3.7.2\",\"week_size\":\"0 B\"}}\n",
        r.body,
    );
}

test "the history limit defaults to 100 and only a positive integer overrides it" {
    const cases = [_]struct { []const u8, usize }{
        .{ "mode=history", 100 },
        .{ "mode=history&limit=", 100 },
        .{ "mode=history&limit=25", 25 },
        .{ "mode=history&limit=0", 100 },
        .{ "mode=history&limit=-5", 100 },
        .{ "mode=history&limit=abc", 100 },
        .{ "mode=history&limit=1", 1 },
    };
    for (cases) |c| {
        var fx: Fixture = .{ .arena = undefined };
        fx.init();
        defer fx.deinit();
        _ = try fx.get(c[0]);
        try testing.expectEqual(c[1], fx.queue.history_limit);
    }
}

test "a history store failure is a 500" {
    var fx: Fixture = .{ .arena = undefined };
    fx.init();
    defer fx.deinit();
    fx.queue.fail_history = error.Unavailable;
    const r = try fx.get("mode=history");
    try testing.expectEqual(@as(u16, 500), r.status);
}

test "history delete reuses the queue removal path" {
    var fx: Fixture = .{ .arena = undefined };
    fx.init();
    defer fx.deinit();
    const r = try fx.get("mode=history&name=delete&value=SABnzbd_nzo_HE");
    try testing.expectEqual(@as(u16, 200), r.status);
    try testing.expectEqualSlices(ports.JobId, &.{9}, fx.queue.removes());
}

test "history mark_as_completed flips the job" {
    var fx: Fixture = .{ .arena = undefined };
    fx.init();
    defer fx.deinit();
    const r = try fx.get("mode=history&name=mark_as_completed&value=SABnzbd_nzo_HE");
    try testing.expectEqual(@as(u16, 200), r.status);
    try testing.expectEqualSlices(ports.JobId, &.{9}, fx.queue.completions());
    try testing.expectEqualStrings(
        "{\"nzo_ids\":[\"SABnzbd_nzo_HE\"],\"status\":true}\n",
        r.body,
    );
}

test "an unimplemented history action is a 400 that names it" {
    var fx: Fixture = .{ .arena = undefined };
    fx.init();
    defer fx.deinit();
    const r = try fx.get("mode=history&name=retry");
    try testing.expectEqual(@as(u16, 400), r.status);
    try expectJsonField(r.body, &.{"error"}, "history.name=\"retry\" not implemented");
}

// -- get_files -------------------------------------------------------

test "mode=get_files answers the exact Go bytes" {
    var fx: Fixture = .{ .arena = undefined };
    fx.init();
    defer fx.deinit();
    fx.queue.active = &.{queue_job};
    fx.queue.files = &.{
        .{
            .id = 7,
            .filename = "release.part01.rar",
            .size_bytes = 52428800,
            .segment_count = 100,
            .segments_done = 50,
            .state = .downloading,
        },
    };
    const r = try fx.get("mode=get_files&value=SABnzbd_nzo_GQZDUZDFMFSGEZLFMY");
    try testing.expectEqual(@as(u16, 200), r.status);
    try testing.expectEqualStrings(
        "{\"files\":[{\"bytes\":52428800,\"easy_id\":7," ++
            "\"filename\":\"release.part01.rar\",\"mb\":\"50.00\",\"mbleft\":\"25.00\"," ++
            "\"nzf_id\":\"nzf_7\",\"set\":\"\",\"status\":\"Active\"}]}\n",
        r.body,
    );
}

test "get_files needs a value and rejects a bad handle" {
    var fx: Fixture = .{ .arena = undefined };
    fx.init();
    defer fx.deinit();
    const r = try fx.get("mode=get_files");
    try testing.expectEqual(@as(u16, 400), r.status);
    try expectJsonField(r.body, &.{"error"}, "value= (nzo_id) required");

    const r2 = try fx.get("mode=get_files&value=nope");
    try testing.expectEqual(@as(u16, 400), r2.status);
    try expectJsonField(r2.body, &.{"error"}, "nzo_id \"nope\": BadPrefix");
}

test "get_files for an unknown job is a 404" {
    var fx: Fixture = .{ .arena = undefined };
    fx.init();
    defer fx.deinit();
    const r = try fx.get("mode=get_files&value=SABnzbd_nzo_HE");
    try testing.expectEqual(@as(u16, 404), r.status);
    try expectJsonField(r.body, &.{"error"}, "NotFound");
}

// -- eval_sort -------------------------------------------------------

test "mode=eval_sort answers the exact Go bytes" {
    var fx: Fixture = .{ .arena = undefined };
    fx.init();
    defer fx.deinit();
    const r = try fx.get(
        "mode=eval_sort&name=%25title%20(%25year)%2FSeason%20%250s%2FS%250sE%250e%25ext" ++
            "&title=Great%20Show&year=2020&season_num=3&episode_num=7&resolution=1080p",
    );
    try testing.expectEqual(@as(u16, 200), r.status);
    try testing.expectEqualStrings(
        "{\"result\":\"Great Show (2020)/Season 03/S03E07.mkv\",\"status\":true}\n",
        r.body,
    );
}

test "eval_sort needs a template" {
    var fx: Fixture = .{ .arena = undefined };
    fx.init();
    defer fx.deinit();
    const r = try fx.get("mode=eval_sort");
    try testing.expectEqual(@as(u16, 400), r.status);
    try expectJsonField(r.body, &.{"error"}, "name= (template) required");
}

test "eval_sort never 4xxes on template content" {
    // A 4xx here makes *arr refuse the download client outright, so even
    // nonsense has to render.
    var fx: Fixture = .{ .arena = undefined };
    fx.init();
    defer fx.deinit();
    for ([_][]const u8{ "%25unknown", "%7Bnope%7D", "%25", "..%2F..%2Fetc", "%22quote%22" }) |t| {
        const q = try std.mem.concat(fx.arena.allocator(), u8, &.{ "mode=eval_sort&name=", t });
        const r = try fx.get(q);
        try testing.expectEqual(@as(u16, 200), r.status);
        try testing.expect(std.json.validate(testing.allocator, r.body) catch false);
    }
}

// -- addfile ---------------------------------------------------------

fn addfileBody(comptime filename: []const u8, comptime content: []const u8) []const u8 {
    return "--BB\r\n" ++
        "Content-Disposition: form-data; name=\"apikey\"\r\n\r\n" ++ key ++ "\r\n" ++
        "--BB\r\n" ++
        "Content-Disposition: form-data; name=\"mode\"\r\n\r\naddfile\r\n" ++
        "--BB\r\n" ++
        "Content-Disposition: form-data; name=\"cat\"\r\n\r\ntv\r\n" ++
        "--BB\r\n" ++
        "Content-Disposition: form-data; name=\"name\"; filename=\"" ++ filename ++ "\"\r\n" ++
        "Content-Type: application/x-nzb\r\n\r\n" ++ content ++ "\r\n" ++
        "--BB--\r\n";
}

fn postMultipart(fx: *Fixture, body: []const u8) !Result {
    return fx.raw(.{
        .body_bearing = true,
        .content_type = "multipart/form-data; boundary=BB",
        .body = body,
        .user_agent = "Sonarr/4.0.0",
    });
}

test "mode=addfile submits the NZB and answers with the job handle" {
    var fx: Fixture = .{ .arena = undefined };
    fx.init();
    defer fx.deinit();
    fx.add.id = 42;
    // The lookup that decorates the handle with the NZB hash.
    fx.queue.active = &.{queue_job};

    const r = try postMultipart(&fx, addfileBody("Release.Name.S01E01.nzb", "<nzb><file/></nzb>"));
    try testing.expectEqual(@as(u16, 200), r.status);
    try testing.expectEqualStrings(
        "{\"nzo_ids\":[\"SABnzbd_nzo_GQZDUZDFMFSGEZLFMY\"],\"status\":true}\n",
        r.body,
    );
    const cmd = fx.add.last.?;
    try testing.expectEqualStrings("<nzb><file/></nzb>", cmd.nzb);
    // The .nzb suffix is stripped for the display name.
    try testing.expectEqualStrings("Release.Name.S01E01", cmd.name);
    try testing.expectEqualStrings("tv", cmd.category);
    try testing.expectEqualStrings("Sonarr/4.0.0", cmd.source);
}

test "addfile accepts either extension spelling and strips path components" {
    const cases = [_]struct { []const u8, []const u8 }{
        .{ "Release.nzb", "Release" },
        .{ "Release.NZB", "Release" },
        // Go trimmed ".nzb" first and ".NZB" second, so only the
        // outer suffix goes: "Release.nzb.NZB" -> "Release.nzb".
        .{ "Release.nzb.NZB", "Release.nzb" },
        .{ "Release", "Release" },
        .{ "../../etc/Release.nzb", "Release" },
    };
    inline for (cases) |c| {
        var fx: Fixture = .{ .arena = undefined };
        fx.init();
        defer fx.deinit();
        _ = try postMultipart(&fx, addfileBody(c[0], "X"));
        try testing.expectEqualStrings(c[1], fx.add.last.?.name);
    }
}

test "addfile without a file part is a 400" {
    var fx: Fixture = .{ .arena = undefined };
    fx.init();
    defer fx.deinit();
    const r = try fx.get("mode=addfile");
    try testing.expectEqual(@as(u16, 400), r.status);
    try expectJsonField(r.body, &.{"error"}, "no nzb file part (expected name= or nzbfile=)");
    try testing.expectEqual(@as(usize, 0), fx.add.calls);
}

test "addfile reports a duplicate as success, because a 400 blacklists the release" {
    var fx: Fixture = .{ .arena = undefined };
    fx.init();
    defer fx.deinit();
    fx.add.id = 42;
    fx.add.duplicate = true;
    fx.queue.active = &.{queue_job};
    const r = try postMultipart(&fx, addfileBody("Release.nzb", "X"));
    try testing.expectEqual(@as(u16, 200), r.status);
    try testing.expectEqualStrings(
        "{\"nzo_ids\":[\"SABnzbd_nzo_GQZDUZDFMFSGEZLFMY\"],\"status\":true}\n",
        r.body,
    );
}

test "a rejected submission is a 400" {
    var fx: Fixture = .{ .arena = undefined };
    fx.init();
    defer fx.deinit();
    fx.add.fail = error.Rejected;
    const r = try postMultipart(&fx, addfileBody("Release.nzb", "X"));
    try testing.expectEqual(@as(u16, 400), r.status);
    try expectJsonField(r.body, &.{"error"}, "Rejected");
}

test "an unresolvable job still yields a valid bare handle" {
    var fx: Fixture = .{ .arena = undefined };
    fx.init();
    defer fx.deinit();
    // The submission succeeded but the follow-up lookup fails: the client
    // still gets a usable nzo_id, just without the hash disambiguator.
    fx.add.id = 42;
    fx.queue.fail_get = error.Unavailable;
    const r = try postMultipart(&fx, addfileBody("Release.nzb", "X"));
    try testing.expectEqual(@as(u16, 200), r.status);
    try testing.expectEqualStrings("{\"nzo_ids\":[\"SABnzbd_nzo_GQZA\"],\"status\":true}\n", r.body);
    try testing.expectEqual(@as(ports.JobId, 42), try nzo.jobIdFrom("SABnzbd_nzo_GQZA"));
}

test "a zero job id skips the lookup entirely" {
    var fx: Fixture = .{ .arena = undefined };
    fx.init();
    defer fx.deinit();
    fx.add.id = 0;
    const r = try postMultipart(&fx, addfileBody("Release.nzb", "X"));
    try testing.expectEqualStrings("{\"nzo_ids\":[\"SABnzbd_nzo_GA\"],\"status\":true}\n", r.body);
}

// -- addurl ----------------------------------------------------------

test "mode=addurl fetches, submits and answers with the handle" {
    var fx: Fixture = .{ .arena = undefined };
    fx.init();
    defer fx.deinit();
    fx.add.id = 42;
    fx.queue.active = &.{queue_job};
    fx.fetch.body = "<nzb/>";
    fx.fetch.content_disposition = "attachment; filename=\"The.Release.S02E03.nzb\"";

    const r = try fx.raw(.{
        .query = "mode=addurl&apikey=" ++ key ++
            "&name=https%3A%2F%2Findexer.example%2Fgetnzb%3Fid%3Dabc&cat=tv",
        .user_agent = "Radarr/5.0",
    });
    try testing.expectEqual(@as(u16, 200), r.status);
    try testing.expectEqualStrings(
        "{\"nzo_ids\":[\"SABnzbd_nzo_GQZDUZDFMFSGEZLFMY\"],\"status\":true}\n",
        r.body,
    );
    try testing.expectEqualStrings("https://indexer.example/getnzb?id=abc", fx.fetch.last_url);
    const cmd = fx.add.last.?;
    try testing.expectEqualStrings("<nzb/>", cmd.nzb);
    try testing.expectEqualStrings("The.Release.S02E03", cmd.name);
    try testing.expectEqualStrings("tv", cmd.category);
    try testing.expectEqualStrings("Radarr/5.0 (addurl)", cmd.source);
}

test "the addurl display name follows Go's precedence" {
    const cases = [_]struct {
        query: []const u8,
        disposition: []const u8,
        want: []const u8,
    }{
        // nzbname= wins over everything.
        .{
            .query = "&name=https%3A%2F%2Fx.test%2Fa.nzb&nzbname=Explicit",
            .disposition = "attachment; filename=\"FromHeader.nzb\"",
            .want = "Explicit",
        },
        // Then Content-Disposition.
        .{
            .query = "&name=https%3A%2F%2Fx.test%2Fa.nzb",
            .disposition = "attachment; filename=\"FromHeader.nzb\"",
            .want = "FromHeader",
        },
        // Then the URL path.
        .{ .query = "&name=https%3A%2F%2Fx.test%2Fa.nzb", .disposition = "", .want = "a" },
        .{
            .query = "&name=https%3A%2F%2Fx.test%2Fgetnzb%3Fid%3Dabc.nzb",
            .disposition = "",
            .want = "getnzb",
        },
        // Then a placeholder, when the URL yields nothing usable.
        .{ .query = "&name=https%3A%2F%2Fx.test%2F", .disposition = "", .want = "addurl-job" },
        // A directory component in the header cannot steer the name.
        .{
            .query = "&name=https%3A%2F%2Fx.test%2Fa.nzb",
            .disposition = "attachment; filename=\"../../evil.nzb\"",
            .want = "evil",
        },
    };
    for (cases) |c| {
        var fx: Fixture = .{ .arena = undefined };
        fx.init();
        defer fx.deinit();
        fx.fetch.content_disposition = c.disposition;
        fx.fetch.body = "<nzb/>";
        const q = try std.mem.concat(fx.arena.allocator(), u8, &.{
            "mode=addurl&apikey=" ++ key, c.query,
        });
        const r = try fx.raw(.{ .query = q });
        try testing.expectEqual(@as(u16, 200), r.status);
        try testing.expectEqualStrings(c.want, fx.add.last.?.name);
    }
}

test "addurl validates the URL before touching the network" {
    const bad = [_]struct { []const u8, []const u8 }{
        .{ "", "name= (url) required" },
        .{ "%20%20", "name= (url) required" },
        .{ "ftp%3A%2F%2Fx.test%2Fa.nzb", "url must be http:// or https://" },
        .{ "file%3A%2F%2F%2Fetc%2Fpasswd", "url must be http:// or https://" },
        .{ "%2Flocal%2Fpath", "url must be http:// or https://" },
        .{ "x.test%2Fa.nzb", "url must be http:// or https://" },
    };
    for (bad) |c| {
        var fx: Fixture = .{ .arena = undefined };
        fx.init();
        defer fx.deinit();
        const q = try std.mem.concat(fx.arena.allocator(), u8, &.{
            "mode=addurl&apikey=" ++ key ++ "&name=", c[0],
        });
        const r = try fx.raw(.{ .query = q });
        try testing.expectEqual(@as(u16, 400), r.status);
        try expectJsonField(r.body, &.{"error"}, c[1]);
        try testing.expectEqual(@as(usize, 0), fx.fetch.calls);
    }
}

test "a fetch failure or a non-2xx upstream is a 502" {
    var fx: Fixture = .{ .arena = undefined };
    fx.init();
    defer fx.deinit();
    fx.fetch.fail = error.Fetch;
    const r = try fx.get("mode=addurl&name=https%3A%2F%2Fx.test%2Fa.nzb");
    try testing.expectEqual(@as(u16, 502), r.status);
    try expectJsonField(r.body, &.{"error"}, "fetch nzb: transport failure");

    var fx2: Fixture = .{ .arena = undefined };
    fx2.init();
    defer fx2.deinit();
    fx2.fetch.status = 404;
    const r2 = try fx2.get("mode=addurl&name=https%3A%2F%2Fx.test%2Fa.nzb");
    try testing.expectEqual(@as(u16, 502), r2.status);
    try expectJsonField(r2.body, &.{"error"}, "fetch nzb: upstream 404");
    try testing.expectEqual(@as(usize, 0), fx2.add.calls);

    // A 3xx is not a success either: the port does not follow redirects.
    var fx3: Fixture = .{ .arena = undefined };
    fx3.init();
    defer fx3.deinit();
    fx3.fetch.status = 302;
    const r3 = try fx3.get("mode=addurl&name=https%3A%2F%2Fx.test%2Fa.nzb");
    try testing.expectEqual(@as(u16, 502), r3.status);
}

test "addurl without a fetch port says so instead of pretending" {
    var fx: Fixture = .{ .arena = undefined };
    fx.init();
    defer fx.deinit();
    fx.handler.fetch = null;
    const r = try fx.get("mode=addurl&name=https%3A%2F%2Fx.test%2Fa.nzb");
    try testing.expectEqual(@as(u16, 502), r.status);
    try testing.expectEqual(@as(usize, 0), fx.add.calls);
}

test "filenameFromUrl reproduces the Go helper" {
    const cases = [_]struct { []const u8, []const u8 }{
        .{ "https://indexer.example/getnzb?id=abc.nzb&apikey=k", "getnzb" },
        .{ "https://nzb.example/foo/bar/release.name.nzb", "release.name" },
        .{ "https://example.com/r.nzb", "r" },
        .{ "", "" },
    };
    for (cases) |c| try testing.expectEqualStrings(c[1], filenameFromUrl(c[0]));
}

// -- hostile input end to end ----------------------------------------

test "every response is valid JSON for a hostile release name" {
    const names = [_][]const u8{
        "He said %22hi%22 %5C",
        "ctrl%01%1f",
        "amp %26 lt %3C gt %3E",
        "%7B%22injected%22%3A1%7D",
    };
    var fx: Fixture = .{ .arena = undefined };
    fx.init();
    defer fx.deinit();

    // A name carrying a quote, a brace, a control byte and invalid UTF-8
    // — everything an NZB off the internet can put in front of us.
    var job = queue_job;
    job.name = "\"},{\"x\":\x01 \xff";
    job.error_msg = job.name;
    job.category = job.name;
    fx.queue.active = &.{job};
    fx.queue.history = &.{job};
    fx.cats.rows = &.{.{ .name = job.name, .dir = job.name }};

    for ([_][]const u8{ "mode=queue", "mode=history", "mode=get_cats", "mode=get_config" }) |q| {
        const r = try fx.get(q);
        try testing.expectEqual(@as(u16, 200), r.status);
        try testing.expect(std.json.validate(testing.allocator, r.body) catch false);
    }

    // And the same bytes reflected back through an error message.
    for (names) |n| {
        const q = try std.mem.concat(fx.arena.allocator(), u8, &.{ "mode=", n });
        const r = try fx.get(q);
        try testing.expectEqual(@as(u16, 400), r.status);
        try testing.expect(std.json.validate(testing.allocator, r.body) catch false);
    }
}

test "an invalid-UTF-8 apikey is rejected without corrupting the response" {
    var fx: Fixture = .{ .arena = undefined };
    fx.init();
    defer fx.deinit();
    const r = try fx.raw(.{ .query = "mode=version&apikey=%ff%fe%fd" });
    try testing.expectEqual(@as(u16, 401), r.status);
    try testing.expect(std.json.validate(testing.allocator, r.body) catch false);
}

// -- routing ---------------------------------------------------------

test "the route table mounts both SAB paths, publicly and for any method" {
    const App = struct { sab: Handler };
    const table = routes(App, "sab");
    try testing.expectEqual(@as(usize, 2), table.len);
    try testing.expectEqualStrings("/sabnzbd/api", table[0].path);
    try testing.expectEqual(server.Route.Kind.exact, table[0].kind);
    try testing.expectEqualStrings("/sabnzbd/", table[1].path);
    try testing.expectEqual(server.Route.Kind.prefix, table[1].kind);
    for (table) |r| {
        // Any method: *arr uses POST for addfile and GET for the rest.
        try testing.expectEqual(@as(?httpreq.Method, null), r.method);
        // Public, because this handler authenticates itself. See the
        // module comment.
        try testing.expectEqual(server.Route.Access.public, r.access);
    }
}

test "inputFromRequest reads the body only for body-bearing methods" {
    var req: httpreq.Request = .{ .method = .get, .query = "mode=queue", .body = "mode=history" };
    try testing.expect(!inputFromRequest(&req).body_bearing);
    req.method = .post;
    try testing.expect(inputFromRequest(&req).body_bearing);
    req.method = .put;
    try testing.expect(inputFromRequest(&req).body_bearing);
    req.method = .delete;
    try testing.expect(!inputFromRequest(&req).body_bearing);
    try testing.expectEqualStrings("mode=queue", inputFromRequest(&req).query);
    try testing.expectEqualStrings("", inputFromRequest(&req).content_type);
}
