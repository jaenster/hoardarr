//! Application layer for outbound notifications.
//!
//! The bus delivers events for a curated set of user-meaningful topics
//! and the service fans each one out to every enabled `Subscription`
//! whose topic filter matches. The curation is deliberate: the
//! fine-grained internal events (`segment.completed` fires once per
//! segment, `queue.*` on every pause) are noise to a subscriber and
//! would amplify one busy job into thousands of HTTP requests.
//!
//! # Structure
//!
//! Three seams, all injected, so the whole dispatch path tests with no
//! socket, no database and no clock:
//!
//!   * `transport.Transport` — the network. See `transport.zig`.
//!   * `JobLookup` — payload enrichment. Go passed a
//!     `download.JobRepository`; here it is a single function that
//!     answers "snapshot of job N", because that is all enrichment ever
//!     asked of the repository and it keeps this module independent of
//!     the download context's persistence.
//!   * `OutcomeSink` — where per-delivery telemetry goes. Go opened a
//!     transaction, re-read the aggregate and called
//!     `MarkDeliverySuccess` / `MarkDeliveryFailure` inline. That
//!     couples the dispatch path to the transaction manager for
//!     bookkeeping that no downstream subscriber cares about, so here
//!     the service reports the outcome and the composition root decides
//!     how to persist it.
//!
//! # Concurrency
//!
//! Go spawned a goroutine per (subscription, event) pair so a slow
//! subscriber could not block its siblings. The Zig port dispatches
//! sequentially: `Transport` will be reactor-backed, so a send that
//! would have blocked a thread instead yields, and one dispatch loop is
//! enough. That makes `Service` free of locks and of the
//! `sync.WaitGroup` shutdown dance.
//!
//! # Secrets
//!
//! A subscription URL is a bearer credential. Nothing in this file
//! passes a full URL to the logger — `Target.safeUrl` strips the path,
//! and failure reasons come from `Delivery.describe`, which is built
//! from status codes only. There is a test that asserts it.

const std = @import("std");
const log = @import("../../core/log.zig");
const event = @import("../../domain/event.zig");
const dnotify = @import("../../domain/notify.zig");
const render = @import("render.zig");
const transport = @import("transport.zig");
const discord = @import("discord.zig");
const slack = @import("slack.zig");
const webhook = @import("webhook.zig");

const Allocator = std.mem.Allocator;
const Writer = std.Io.Writer;
const Target = transport.Target;
const Timestamp = event.Timestamp;

pub const Subscription = dnotify.Subscription;
pub const SubscriptionId = dnotify.SubscriptionId;
pub const Kind = dnotify.Kind;

/// Bus topics the service forwards to subscribers. High-volume internal
/// segment and file events are intentionally absent.
pub const subscribable_topics = [_][]const u8{
    "download.job.created",
    "download.job.download_complete",
    "download.job.download_failed",
    "download.job.completed",
    "download.job.failed",
    "verify.ok",
    "verify.repair_needed",
    "verify.failed",
    "repair.ok",
    "repair.failed",
    "deliver.complete",
    "deliver.failed",
    "extract.complete",
    "extract.failed",
};

/// Topic the synthetic `sendTest` event carries. Subscribers use it to
/// recognise a "does this hook work" probe.
pub const test_topic = "notify.test";

/// Payload of the synthetic test event, documented here because it is
/// part of the contract with subscribers.
pub const test_payload = "{\"note\":\"hoardarr test event\"}";

pub fn isSubscribable(topic: []const u8) bool {
    for (subscribable_topics) |t| {
        if (std.mem.eql(u8, t, topic)) return true;
    }
    return false;
}

// ---------------------------------------------------------------------
// Injected ports
// ---------------------------------------------------------------------

/// The slice of a Job a subscriber needs to react without a follow-up
/// GET: `deliver.complete` arrives with everything a Discord embed
/// wants already in it.
pub const JobSnapshot = struct {
    id: i64,
    name: []const u8,
    category: []const u8,
    state: []const u8,
    source: []const u8,
    total_bytes: i64,
    done_bytes: i64,
    failed_bytes: i64,
    file_count: u32,
    added_at: Timestamp,
};

/// Job lookup for enrichment. Null on the `Service` disables enrichment
/// entirely, which is what the Go version did when `Jobs` was nil.
pub const JobLookup = struct {
    ctx: *anyopaque,
    /// Null when the job is gone — a job removed between the event and
    /// the dispatch is normal, not an error.
    byIdFn: *const fn (ctx: *anyopaque, id: i64) ?JobSnapshot,

    pub fn byId(self: JobLookup, id: i64) ?JobSnapshot {
        return self.byIdFn(self.ctx, id);
    }
};

/// Where per-delivery telemetry goes. `reason` is empty on success and
/// is guaranteed free of URLs and secrets (it comes from
/// `Delivery.describe`), so it is safe to persist and to show in the
/// Settings UI.
pub const OutcomeSink = struct {
    ctx: *anyopaque,
    recordFn: *const fn (
        ctx: *anyopaque,
        id: SubscriptionId,
        ok: bool,
        reason: []const u8,
        now: Timestamp,
    ) void,

    pub fn record(
        self: OutcomeSink,
        id: SubscriptionId,
        ok: bool,
        reason: []const u8,
        now: Timestamp,
    ) void {
        self.recordFn(self.ctx, id, ok, reason, now);
    }
};

pub const DispatchError = error{
    /// `sub.kind` has no adapter compiled in. Every kind the domain
    /// defines is wired in `deliverTo`, so this is a pure safety net: it
    /// exists so that adding a `Kind` without an adapter fails loudly at
    /// dispatch instead of being swallowed by an `else` prong.
    NoSenderForKind,
} || Allocator.Error;

// ---------------------------------------------------------------------
// Service
// ---------------------------------------------------------------------

pub const Service = struct {
    /// Backs the per-dispatch scratch arena. One payload's worth of
    /// strings is allocated and released per delivery, so nothing here
    /// grows with uptime.
    gpa: Allocator,
    transport: transport.Transport,
    now: *const fn () Timestamp,
    logger: *log.Logger = &log.default,
    jobs: ?JobLookup = null,
    outcome: ?OutcomeSink = null,

    /// Snapshot of the enabled subscriptions, owned by the caller.
    ///
    /// Go kept this behind an RWMutex and reloaded it from the
    /// repository on every `notify.subscription.*` admin event. The
    /// reload belongs to whoever owns the repository, so here the
    /// composition root installs a fresh slice with `setActive` and the
    /// dispatch path only reads. Same staleness bound as Go's — at most
    /// one event tick — with no lock.
    active: []const *const Subscription = &.{},

    pub fn setActive(self: *Service, subs: []const *const Subscription) void {
        self.active = subs;
    }

    /// Fans one envelope out to every matching enabled subscription.
    ///
    /// A delivery failure is recorded and logged but never propagated:
    /// returning an error would make the bus redeliver the event, which
    /// duplicates the deliveries that *did* succeed. Retry inside one
    /// dispatch is `transport.deliver`'s job; retry across restarts is
    /// the outbox's.
    pub fn onEvent(self: *Service, env: event.Envelope) Allocator.Error!void {
        var arena = std.heap.ArenaAllocator.init(self.gpa);
        defer arena.deinit();
        const a = arena.allocator();

        const enriched = try self.enrich(a, env);

        for (self.active) |sub| {
            if (!sub.wants(enriched.topic)) continue;
            self.dispatch(a, sub, enriched) catch |e| switch (e) {
                error.OutOfMemory => return error.OutOfMemory,
                error.NoSenderForKind => {
                    self.logger.err("notify: no sender for subscription kind", &.{
                        log.int("sub_id", sub.id),
                        log.str("kind", @tagName(sub.kind)),
                    });
                },
            };
        }
    }

    /// Builds and delivers one (subscription, event) pair, then records
    /// the outcome.
    pub fn dispatch(
        self: *Service,
        arena: Allocator,
        sub: *const Subscription,
        env: event.Envelope,
    ) DispatchError!void {
        const target = targetOf(sub);
        const d = try self.deliverTo(arena, sub.kind, target, env);
        self.recordOutcome(sub, d, env.topic, target);
    }

    /// Sends a synthetic event straight to one subscription, bypassing
    /// the bus. This is what the Settings UI's "Test" button calls.
    /// `arena` backs the built payload, which the `Transport` borrows
    /// for the duration of the call — so it is the caller's (the REST
    /// handler's request arena), not a private one that would free the
    /// request body out from under a test's assertions.
    pub fn sendTest(
        self: *Service,
        arena: Allocator,
        sub: *const Subscription,
    ) DispatchError!transport.Delivery {
        const target = targetOf(sub);
        const env: event.Envelope = .{
            .id = .nil,
            .topic = test_topic,
            .aggregate_id = "test",
            .occurred_at = self.now(),
            .payload = test_payload,
        };
        const d = try self.deliverTo(arena, sub.kind, target, env);
        self.recordOutcome(sub, d, test_topic, target);
        return d;
    }

    /// Routes to the adapter for `kind`. This is the whole of what Go's
    /// `adapter/notify/router` did; a two-arm switch does not need a map
    /// and a vtable.
    fn deliverTo(
        self: *Service,
        arena: Allocator,
        kind: Kind,
        target: Target,
        env: event.Envelope,
    ) DispatchError!transport.Delivery {
        return switch (kind) {
            .discord => try discord.send(arena, self.transport, target, env),
            .slack => try slack.send(arena, self.transport, target, env),
            .webhook => try webhook.send(arena, self.transport, target, env),
        };
    }

    /// Logs and reports one delivery result.
    ///
    /// The log line carries the subscription id, the topic and
    /// `Target.safeUrl` — never the URL itself, because for Discord and
    /// Slack the URL path *is* the credential.
    fn recordOutcome(
        self: *Service,
        sub: *const Subscription,
        d: transport.Delivery,
        topic: []const u8,
        target: Target,
    ) void {
        var buf: transport.Delivery.DescribeBuf = undefined;
        if (d.ok()) {
            self.logger.debug("notify: delivered", &.{
                log.int("sub_id", sub.id),
                log.str("topic", topic),
                log.uint("status", d.status),
            });
            if (self.outcome) |o| o.record(sub.id, true, "", self.now());
            return;
        }
        const reason = d.describe(&buf);
        self.logger.warn("notify: delivery failed", &.{
            log.int("sub_id", sub.id),
            log.str("topic", topic),
            log.str("host", target.safe()),
            log.str("reason", reason),
        });
        if (self.outcome) |o| o.record(sub.id, false, reason, self.now());
    }

    /// Rewrites the payload to `{"event":<original>,"job":{...}}` when
    /// the original carried a `job_id` that resolves.
    ///
    /// The original bytes are embedded verbatim rather than round-tripped
    /// through a JSON decoder. Go re-marshalled a `map[string]any`, which
    /// silently reordered keys and turned every integer into a float;
    /// splicing keeps the subscriber's HMAC-able bytes byte-identical to
    /// what the producer wrote.
    ///
    /// Falls back to the unmodified envelope when there is no job repo,
    /// the payload is not a JSON object, there is no usable `job_id`, or
    /// the lookup misses — all four are normal, none is an error.
    pub fn enrich(
        self: *Service,
        arena: Allocator,
        env: event.Envelope,
    ) Allocator.Error!event.Envelope {
        const jobs = self.jobs orelse return env;
        if (env.payload.len == 0) return env;

        const root = std.json.parseFromSliceLeaky(
            std.json.Value,
            arena,
            env.payload,
            .{},
        ) catch |e| switch (e) {
            error.OutOfMemory => return error.OutOfMemory,
            else => return env,
        };
        const obj = switch (root) {
            .object => |o| o,
            else => return env,
        };
        const id = jobIdOf(obj) orelse return env;
        if (id == 0) return env;
        const j = jobs.byId(id) orelse return env;

        var out: Writer.Allocating = .init(arena);
        errdefer out.deinit();
        writeEnriched(&out.writer, env.payload, j) catch |e| switch (e) {
            error.WriteFailed => return error.OutOfMemory,
        };

        var enriched = env;
        enriched.payload = try out.toOwnedSlice();
        return enriched;
    }
};

/// Narrows the aggregate to what an adapter is allowed to see. The
/// adapters never receive the `Subscription` itself, which is what keeps
/// payload construction a pure function and keeps the adapter tests free
/// of the domain module.
pub fn targetOf(sub: *const Subscription) Target {
    return .{
        .name = sub.name,
        .url = sub.url,
        .secret = sub.secret,
    };
}

/// `job_id` as an i64. JSON hands numbers over as `.integer` normally
/// and `.float` when the producer wrote a decimal point, so both are
/// accepted; Go had the same problem with `float64` from
/// `map[string]any`.
fn jobIdOf(obj: std.json.ObjectMap) ?i64 {
    const v = obj.get("job_id") orelse return null;
    return switch (v) {
        .integer => |n| n,
        .float => |f| if (std.math.isFinite(f)) @intFromFloat(f) else null,
        .number_string => |s| std.fmt.parseInt(i64, s, 10) catch null,
        else => null,
    };
}

fn writeEnriched(w: *Writer, original: []const u8, j: JobSnapshot) Writer.Error!void {
    var ts: render.Rfc3339Buf = undefined;
    try w.writeAll("{");
    try render.writeJsonKey(w, "event");
    // Verbatim splice: `original` is an already-validated JSON object,
    // braces included.
    try w.writeAll(original);
    try w.writeAll(",");
    try render.writeJsonKey(w, "job");
    try w.writeAll("{");
    try render.writeJsonKey(w, "id");
    try w.print("{d}", .{j.id});
    try w.writeAll(",");
    try render.writeJsonField(w, "name", j.name);
    try w.writeAll(",");
    try render.writeJsonField(w, "category", j.category);
    try w.writeAll(",");
    try render.writeJsonField(w, "state", j.state);
    try w.writeAll(",");
    try render.writeJsonField(w, "source", j.source);
    try w.writeAll(",");
    try render.writeJsonKey(w, "total_bytes");
    try w.print("{d},", .{j.total_bytes});
    try render.writeJsonKey(w, "done_bytes");
    try w.print("{d},", .{j.done_bytes});
    try render.writeJsonKey(w, "failed_bytes");
    try w.print("{d},", .{j.failed_bytes});
    try render.writeJsonKey(w, "file_count");
    try w.print("{d},", .{j.file_count});
    try render.writeJsonField(w, "added_at", render.rfc3339(&ts, j.added_at));
    try w.writeAll("}}");
}

// ---------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------

const testing = std.testing;

fn fixedNow() Timestamp {
    return 1_700_000_000_000;
}

const snapshot: JobSnapshot = .{
    .id = 42,
    .name = "Foo.Bar.S01E02.1080p.WEB-DL.H264-RLSGRP",
    .category = "tv",
    .state = "completed",
    .source = "Sonarr/4.0.0",
    .total_bytes = 5_368_709_120,
    .done_bytes = 5_368_709_120,
    .failed_bytes = 0,
    .file_count = 4,
    .added_at = 1_700_000_000_000,
};

const StubJobs = struct {
    snap: JobSnapshot = snapshot,
    /// Ids the stub pretends not to know.
    missing: bool = false,
    calls: usize = 0,

    fn lookup(self: *StubJobs) JobLookup {
        return .{ .ctx = @ptrCast(self), .byIdFn = &byId };
    }

    fn byId(ctx: *anyopaque, id: i64) ?JobSnapshot {
        const self: *StubJobs = @ptrCast(@alignCast(ctx));
        self.calls += 1;
        if (self.missing) return null;
        var s = self.snap;
        s.id = id;
        return s;
    }
};

const RecordedOutcome = struct {
    id: SubscriptionId,
    ok: bool,
    reason: [128]u8,
    reason_len: usize,

    fn reasonSlice(self: *const RecordedOutcome) []const u8 {
        return self.reason[0..self.reason_len];
    }
};

const StubOutcomes = struct {
    items: [8]RecordedOutcome = undefined,
    n: usize = 0,

    fn sink(self: *StubOutcomes) OutcomeSink {
        return .{ .ctx = @ptrCast(self), .recordFn = &record };
    }

    fn record(
        ctx: *anyopaque,
        id: SubscriptionId,
        ok: bool,
        reason: []const u8,
        _: Timestamp,
    ) void {
        const self: *StubOutcomes = @ptrCast(@alignCast(ctx));
        if (self.n == self.items.len) return;
        var it: RecordedOutcome = .{ .id = id, .ok = ok, .reason = undefined, .reason_len = 0 };
        const n = @min(reason.len, it.reason.len);
        @memcpy(it.reason[0..n], reason[0..n]);
        it.reason_len = n;
        self.items[self.n] = it;
        self.n += 1;
    }
};

/// Captures the attributes of every record a logger emits, so a test can
/// assert on what did — and did not — reach the log.
const LogCapture = struct {
    /// Flat store of "msg" then every key and value the logger saw.
    text: [4096]u8 = undefined,
    len: usize = 0,
    records: usize = 0,
    levels: [8]log.Level = undefined,

    fn mirror(self: *LogCapture) log.Mirror {
        return .{ .ctx = @ptrCast(self), .publish = &publish };
    }

    fn publish(
        ctx: *anyopaque,
        _: i128,
        level: log.Level,
        msg: []const u8,
        attrs: []const log.Attr,
    ) void {
        const self: *LogCapture = @ptrCast(@alignCast(ctx));
        if (self.records < self.levels.len) self.levels[self.records] = level;
        self.records += 1;
        self.put(msg);
        for (attrs) |a| {
            self.put(a.key);
            switch (a.value) {
                .str => |s| self.put(s),
                .err => |e| self.put(@errorName(e)),
                else => {},
            }
        }
    }

    fn put(self: *LogCapture, s: []const u8) void {
        const n = @min(s.len, self.text.len - self.len);
        @memcpy(self.text[self.len..][0..n], s[0..n]);
        self.len += n;
    }

    fn seen(self: *const LogCapture) []const u8 {
        return self.text[0..self.len];
    }
};

/// A `Service` wired to stubs, with a logger that has no sinks (so the
/// test suite stays quiet) and a mirror that records everything.
const Harness = struct {
    fake: transport.FakeTransport,
    jobs: StubJobs = .{},
    outcomes: StubOutcomes = .{},
    capture: LogCapture = .{},
    logger: log.Logger = .{},
    svc: Service = undefined,

    fn init(self: *Harness, script: []const transport.Error!transport.Response) void {
        // Assign the whole struct so every field's default applies —
        // `var h: Harness = undefined` leaves the stubs as garbage
        // otherwise, and the vtable pointers they hand out are taken
        // from `self`, so the harness has to be initialised in place.
        self.* = .{ .fake = .{ .script = script } };
        self.logger.setLevel(.debug);
        self.logger.setMirror(self.capture.mirror());
        self.svc = .{
            .gpa = testing.allocator,
            .transport = self.fake.transport(),
            .now = &fixedNow,
            .logger = &self.logger,
            .jobs = self.jobs.lookup(),
            .outcome = self.outcomes.sink(),
        };
    }
};

/// A persisted subscription, built through the domain's own trusted
/// rehydration path rather than a struct literal, so a change to the
/// aggregate's invariants shows up here.
fn discordSub(
    id: SubscriptionId,
    url: []const u8,
    topics: []const []const u8,
) Allocator.Error!Subscription {
    return Subscription.hydrate(testing.allocator, .{
        .id = id,
        .name = "d",
        .kind = .discord,
        .url = url,
        .topics = topics,
        .enabled = true,
        .created_at = 0,
        .updated_at = 0,
    });
}

test "the curated topic set is exactly the one the bus is asked for" {
    try testing.expectEqual(@as(usize, 14), subscribable_topics.len);
    try testing.expect(isSubscribable("deliver.complete"));
    try testing.expect(isSubscribable("download.job.created"));
    try testing.expect(isSubscribable("extract.failed"));
    // Deliberately excluded: per-segment and queue churn.
    try testing.expect(!isSubscribable("download.segment.completed"));
    try testing.expect(!isSubscribable("download.job.paused"));
    try testing.expect(!isSubscribable("queue.reordered"));
    // The synthetic probe never comes off the bus.
    try testing.expect(!isSubscribable(test_topic));
}

test "enrich splices the job snapshot beside the original event" {
    var h: Harness = undefined;
    h.init(&.{.{ .status = 204 }});
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    const env: event.Envelope = .{
        .id = .nil,
        .topic = "deliver.complete",
        .aggregate_id = "42",
        .occurred_at = fixedNow(),
        .payload = "{\"job_id\":42,\"path\":\"/media/tv\"}",
    };
    const out = try h.svc.enrich(arena.allocator(), env);
    try testing.expect(out.payload.ptr != env.payload.ptr);

    const parsed = try std.json.parseFromSlice(
        std.json.Value,
        testing.allocator,
        out.payload,
        .{},
    );
    defer parsed.deinit();
    const inner = parsed.value.object.get("event").?.object;
    // The producer's own fields survive verbatim, integers included.
    try testing.expectEqual(@as(i64, 42), inner.get("job_id").?.integer);
    try testing.expectEqualStrings("/media/tv", inner.get("path").?.string);

    const job = parsed.value.object.get("job").?.object;
    try testing.expectEqualStrings(snapshot.name, job.get("name").?.string);
    try testing.expectEqualStrings("tv", job.get("category").?.string);
    try testing.expectEqual(@as(i64, 5_368_709_120), job.get("total_bytes").?.integer);
    try testing.expectEqual(@as(i64, 4), job.get("file_count").?.integer);
    try testing.expectEqualStrings("2023-11-14T22:13:20Z", job.get("added_at").?.string);

    // And the enriched envelope renders the way the adapters expect.
    const v = try render.View.from(arena.allocator(), out);
    try testing.expectEqualStrings("Sonarr", v.source);
    try testing.expectEqualStrings("5.00 GB", v.size_human);
}

test "enrich leaves the envelope alone when it cannot help" {
    var h: Harness = undefined;
    h.init(&.{.{ .status = 204 }});
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    const cases = [_][]const u8{
        // No job_id.
        "{\"note\":\"hi\"}",
        // Not an object.
        "[1,2]",
        // Not JSON.
        "nonsense",
        // Empty.
        "",
        // job_id present but zero, which is not a real job.
        "{\"job_id\":0}",
        // job_id of the wrong type.
        "{\"job_id\":\"forty-two\"}",
    };
    for (cases) |payload| {
        const env: event.Envelope = .{
            .id = .nil,
            .topic = "deliver.complete",
            .aggregate_id = "0",
            .occurred_at = fixedNow(),
            .payload = payload,
        };
        const out = try h.svc.enrich(arena.allocator(), env);
        try testing.expectEqualStrings(payload, out.payload);
    }

    // A job_id that does not resolve is also a pass-through.
    h.jobs.missing = true;
    h.svc.jobs = h.jobs.lookup();
    const out = try h.svc.enrich(arena.allocator(), .{
        .id = .nil,
        .topic = "deliver.complete",
        .aggregate_id = "9",
        .occurred_at = fixedNow(),
        .payload = "{\"job_id\":9}",
    });
    try testing.expectEqualStrings("{\"job_id\":9}", out.payload);
}

test "enrichment is skipped entirely without a job lookup" {
    var h: Harness = undefined;
    h.init(&.{.{ .status = 204 }});
    h.svc.jobs = null;
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const out = try h.svc.enrich(arena.allocator(), .{
        .id = .nil,
        .topic = "deliver.complete",
        .aggregate_id = "42",
        .occurred_at = fixedNow(),
        .payload = "{\"job_id\":42}",
    });
    try testing.expectEqualStrings("{\"job_id\":42}", out.payload);
    try testing.expectEqual(@as(usize, 0), h.jobs.calls);
}

test "onEvent fans out only to enabled subscriptions that match" {
    var h: Harness = undefined;
    h.init(&.{.{ .status = 204 }});

    var match = try discordSub(1, "https://example.test/a", &.{"deliver.complete"});
    defer match.deinit();
    var wild = try discordSub(2, "https://example.test/b", &.{"deliver.*"});
    defer wild.deinit();
    var other = try discordSub(3, "https://example.test/c", &.{"verify.ok"});
    defer other.deinit();
    var disabled = try discordSub(4, "https://example.test/d", &.{"deliver.complete"});
    defer disabled.deinit();
    disabled.enabled = false;

    const subs = [_]*const Subscription{ &match, &wild, &other, &disabled };
    h.svc.setActive(&subs);

    try h.svc.onEvent(.{
        .id = .nil,
        .topic = "deliver.complete",
        .aggregate_id = "42",
        .occurred_at = fixedNow(),
        .payload = "{\"job_id\":42}",
    });

    // Two matches: the exact topic and the wildcard. Not the other
    // topic, not the disabled one.
    try testing.expectEqual(@as(usize, 2), h.fake.calls);
    try testing.expectEqual(@as(usize, 2), h.outcomes.n);
    for (h.outcomes.items[0..h.outcomes.n]) |o| try testing.expect(o.ok);
}

test "a delivery failure is recorded and logged but not propagated" {
    var h: Harness = undefined;
    h.init(&.{.{ .status = 500 }});

    var sub = try discordSub(7, "https://discord.com/api/webhooks/1/tok", &.{"deliver.complete"});
    defer sub.deinit();
    const subs = [_]*const Subscription{&sub};
    h.svc.setActive(&subs);

    // onEvent must not return an error — the bus would redeliver and
    // duplicate the sends that succeeded.
    try h.svc.onEvent(.{
        .id = .nil,
        .topic = "deliver.complete",
        .aggregate_id = "42",
        .occurred_at = fixedNow(),
        .payload = "{\"job_id\":42}",
    });

    try testing.expectEqual(@as(usize, 1), h.outcomes.n);
    const rec = &h.outcomes.items[0];
    try testing.expectEqual(@as(SubscriptionId, 7), rec.id);
    try testing.expect(!rec.ok);
    try testing.expectEqualStrings("1 attempt(s) failed: status 500", rec.reasonSlice());
    // Logged once, at warn — a failed notification is operator-visible
    // but is not an error of the downloader's.
    try testing.expectEqual(@as(usize, 1), h.capture.records);
    try testing.expectEqual(log.Level.warn, h.capture.levels[0]);
    try testing.expect(std.mem.indexOf(u8, h.capture.seen(), "delivery failed") != null);
}

test "sendTest delivers the synthetic probe and reports the result" {
    var h: Harness = undefined;
    h.init(&.{.{ .status = 204 }});

    var sub = try discordSub(3, "https://example.test/hook", &.{"deliver.complete"});
    defer sub.deinit();
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    const d = try h.svc.sendTest(arena.allocator(), &sub);
    try testing.expect(d.ok());
    try testing.expectEqual(@as(usize, 1), h.fake.calls);

    // The probe never touches the job repo and renders as a test event.
    try testing.expectEqual(@as(usize, 0), h.jobs.calls);
    const parsed = try std.json.parseFromSlice(
        std.json.Value,
        testing.allocator,
        h.fake.last.?.body,
        .{},
    );
    defer parsed.deinit();
    const embed = parsed.value.object.get("embeds").?.array.items[0].object;
    // The probe has no release name, so the title is the verb.
    try testing.expectEqualStrings("Test notification", embed.get("title").?.string);
    try testing.expectEqualStrings("**Test notification**", embed.get("description").?.string);

    try testing.expectEqual(@as(usize, 1), h.outcomes.n);
    try testing.expect(h.outcomes.items[0].ok);
}

test "a failing test send reports the failure" {
    var h: Harness = undefined;
    h.init(&.{error.Timeout});
    var sub = try discordSub(3, "https://example.test/hook", &.{"deliver.complete"});
    defer sub.deinit();
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    const d = try h.svc.sendTest(arena.allocator(), &sub);
    try testing.expectError(error.Timeout, d.toError());
    try testing.expectEqualStrings(
        "1 attempt(s) failed: Timeout",
        h.outcomes.items[0].reasonSlice(),
    );
}

test "a webhook subscription dispatches to the generic sender, signed" {
    var h: Harness = undefined;
    h.init(&.{.{ .status = 204 }});
    var sub = try Subscription.hydrate(testing.allocator, .{
        .id = 5,
        .name = "w",
        .kind = .webhook,
        .url = "https://example.test/hook",
        .secret = "shared-key",
        .topics = &.{"deliver.complete"},
        .enabled = true,
        .created_at = 0,
        .updated_at = 0,
    });
    defer sub.deinit();
    // `dispatch` rather than `onEvent`: the request the fake records
    // borrows from the arena, and `onEvent`'s is private and already
    // released by the time the assertions run.
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    try h.svc.dispatch(arena.allocator(), &sub, .{
        .id = .nil,
        .topic = "deliver.complete",
        .aggregate_id = "42",
        .occurred_at = fixedNow(),
        .payload = "{\"job_id\":42}",
    });
    try testing.expectEqual(@as(usize, 1), h.fake.calls);
    try testing.expectEqual(@as(usize, 1), h.outcomes.n);

    // The body is the envelope, not a rendered chat message, and it is
    // signed with the subscription's secret.
    const req = h.fake.last.?;
    var sig: ?[]const u8 = null;
    for (req.headers) |hdr| {
        if (std.mem.eql(u8, hdr.name, webhook.signature_header)) sig = hdr.value;
    }
    var expect: webhook.SignatureBuf = undefined;
    try testing.expectEqualStrings(
        webhook.sign(&expect, "shared-key", req.body),
        sig orelse return error.NoSignatureHeader,
    );
    try testing.expect(std.mem.indexOf(u8, req.body, "\"Topic\":\"deliver.complete\"") != null);
}

test "no log record and no recorded reason contains the webhook token" {
    const token = "aVerySecretWebhookToken";
    const url = "https://discord.com/api/webhooks/1234567890/" ++ token;

    // Success and failure log different records; only the failure
    // mentions the host at all, so each case says what it expects.
    const cases = [_]struct { transport.Error!transport.Response, bool }{
        .{ .{ .status = 204 }, false },
        .{ .{ .status = 500 }, true },
        .{ error.Connect, true },
    };
    for (cases) |case| {
        const outcome = case[0];
        const expect_host = case[1];
        var h: Harness = undefined;
        h.init(&.{outcome});
        var sub = try discordSub(11, url, &.{"deliver.complete"});
        defer sub.deinit();
        // Overwrite the owned copy in place so `deinit` still frees the
        // right allocation.
        testing.allocator.free(sub.secret);
        sub.secret = try testing.allocator.dupe(u8, "hmac-shared-secret");
        const subs = [_]*const Subscription{&sub};
        h.svc.setActive(&subs);

        try h.svc.onEvent(.{
            .id = .nil,
            .topic = "deliver.complete",
            .aggregate_id = "42",
            .occurred_at = fixedNow(),
            .payload = "{\"job_id\":42}",
        });

        try testing.expect(h.capture.records >= 1);
        const logged = h.capture.seen();
        // Neither the token, the full URL, nor the HMAC secret may
        // appear anywhere in a log record.
        try testing.expect(std.mem.indexOf(u8, logged, token) == null);
        try testing.expect(std.mem.indexOf(u8, logged, url) == null);
        try testing.expect(std.mem.indexOf(u8, logged, "hmac-shared-secret") == null);
        // The bare host is fine, and is what makes a failure log useful.
        // A success line does not need it and does not carry it.
        try testing.expectEqual(
            expect_host,
            std.mem.indexOf(u8, logged, "discord.com") != null,
        );

        // The persisted reason is likewise credential-free.
        for (h.outcomes.items[0..h.outcomes.n]) |o| {
            try testing.expect(std.mem.indexOf(u8, o.reasonSlice(), token) == null);
            try testing.expect(std.mem.indexOf(u8, o.reasonSlice(), "discord.com") == null);
        }
    }
}
