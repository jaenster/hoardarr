//! Server-Sent Events fan-out: the live queue and live log views.
//!
//! One `Hub` owns a fixed table of subscribers. Something upstream — the
//! outbox bus for events, the log ring for records — hands the hub a
//! message; the hub renders it once and writes that one buffer to every
//! subscriber. The connection itself is the send buffer: hoardarr's HTTP
//! layer already queues what the kernel will not take and reports how
//! much is outstanding, so a second layer of per-subscriber queueing
//! would only duplicate it (and, being unbounded, would be the thing
//! that ran the box out of memory).
//!
//! ## Slow consumers are dropped, not buffered
//!
//! A browser tab that is throttled, suspended, or behind a proxy that
//! stopped reading will not drain its socket. The Go hub handled that by
//! dropping *events* once a client's channel filled — best-effort
//! delivery, bounded memory, but a client that never recovers keeps its
//! slot and keeps being written to forever.
//!
//! Here the rule is stricter and, for this data, more honest: before
//! every write the hub asks the sink how many bytes are still queued,
//! and a subscriber over `max_pending_bytes` is disconnected. The UI's
//! `EventSource` reconnects on its own and re-reads the queue over REST,
//! which resynchronises it completely — whereas a client that silently
//! missed a `job.completed` stays wrong until the user reloads. Dropping
//! is therefore both the cheaper and the more correct failure mode.
//!
//! The cap is per subscriber and the subscriber count is fixed, so the
//! hub's worst-case memory is `max_subscribers * max_pending_bytes` and
//! neither factor is client-controlled.
//!
//! ## Ownership
//!
//! `Sink` is a three-function port. Production fills it from an
//! `http.Conn` whose response has been put into event-stream framing and
//! detached; tests fill it with a double that can pretend to stall. The
//! hub never touches a socket, never allocates per message beyond one
//! reusable render buffer, and never blocks.

const std = @import("std");
const log = @import("../core/log.zig");
const logring = @import("../core/logring.zig");
const json = @import("rest/json.zig");

const Allocator = std.mem.Allocator;

/// Topics forwarded to the UI's queue stream. Verbatim from the Go
/// `sse.DefaultTopics` — the bootstrap subscribes the hub to exactly
/// these on the outbox bus, and a topic missing here is a live view that
/// silently stops updating.
pub const default_topics = [_][]const u8{
    "download.job.created",
    "download.job.started",
    "download.job.paused",
    "download.job.resumed",
    "download.job.removed",
    "download.job.download_complete",
    "download.job.download_failed",
    "download.job.completed",
    "download.job.failed",
    "download.segment.dispatched",
    "download.segment.completed",
    "download.segment.missing",
    "download.segment.failed",
    "download.file.completed",
    "verify.started",
    "verify.ok",
    "verify.repair_needed",
    "verify.failed",
    "repair.queued",
    "repair.started",
    "repair.ok",
    "repair.failed",
    "deliver.queued",
    "deliver.started",
    "deliver.complete",
    "deliver.skipped",
    "deliver.failed",
    "extract.queued",
    "extract.started",
    "extract.complete",
    "extract.failed",
    "server.usenet.added",
    "server.usenet.updated",
    "server.usenet.enabled",
    "server.usenet.disabled",
    "server.usenet.removed",
    "notify.subscription.added",
    "notify.subscription.enabled",
    "notify.subscription.disabled",
    "notify.subscription.removed",
    "system.throughput",
    "system.pools",
};

pub fn isDefaultTopic(topic: []const u8) bool {
    for (default_topics) |t| {
        if (std.mem.eql(u8, t, topic)) return true;
    }
    return false;
}

/// Open streams per hub. Two hubs exist (queue events, log tail), so a
/// browser tab costs one slot in each. 64 is far past any real
/// deployment and keeps the table a fixed 2 KiB.
pub const max_subscribers = 64;

/// Queued-but-unsent bytes a subscriber may accumulate before it is
/// dropped. One SSE message is a few hundred bytes, so this is thousands
/// of messages of slack — a client hitting it is not slow, it is gone.
pub const default_max_pending_bytes: usize = 256 << 10;

/// Heartbeat cadence. Matches the Go handler: proxies commonly close an
/// idle upstream at 60 s, so a comment every 15 s keeps the stream up
/// without waking the reactor often enough to matter.
pub const heartbeat_interval_ns: u64 = 15 * std.time.ns_per_s;

// ---------------------------------------------------------------------
// The sink port
// ---------------------------------------------------------------------

pub const WriteError = error{
    /// The peer is gone.
    StreamClosed,
    /// The outbound queue is full: the client stopped reading.
    QueueFull,
    OutOfMemory,
    IoFailed,
};

/// One open stream, as the hub sees it.
pub const Sink = struct {
    ctx: ?*anyopaque = null,

    /// Write one complete SSE message. Must take all of it or fail —
    /// there is no partial-write bookkeeping here on purpose.
    writeFn: *const fn (ctx: ?*anyopaque, bytes: []const u8) WriteError!void,
    /// Bytes queued but not yet accepted by the kernel.
    pendingFn: *const fn (ctx: ?*anyopaque) usize,
    /// Tear the stream down. Called at most once per subscriber, and the
    /// implementation may re-enter `Hub.unsubscribe` from inside it.
    closeFn: *const fn (ctx: ?*anyopaque) void,

    pub fn write(self: Sink, bytes: []const u8) WriteError!void {
        return self.writeFn(self.ctx, bytes);
    }

    pub fn pending(self: Sink) usize {
        return self.pendingFn(self.ctx);
    }

    pub fn close(self: Sink) void {
        self.closeFn(self.ctx);
    }
};

pub const DropReason = enum {
    /// Over `max_pending_bytes`, or the sink reported a full queue.
    slow,
    /// The peer went away or the write failed.
    closed,
    /// `Hub.close` — process shutdown.
    shutdown,
};

// ---------------------------------------------------------------------
// Hub
// ---------------------------------------------------------------------

pub const Hub = struct {
    /// Handle returned by `subscribe`. The generation counter makes a
    /// double unsubscribe and a stale handle both harmless, which is
    /// what the Go hub got from closing the channel under a UUID key.
    pub const Token = struct {
        slot: usize,
        gen: u32,
    };

    pub const SubscribeError = error{TooManySubscribers};

    gpa: Allocator,
    max_pending_bytes: usize = default_max_pending_bytes,

    /// Guards the subscriber table. Publishers are the reactor thread in
    /// production, but the log ring can be published to from any thread,
    /// so the table is locked. `std.Thread.Mutex` does not exist in this
    /// tree; `core/log.zig` owns the futex one.
    mu: log.Mutex = .{},
    subs: [max_subscribers]?Subscriber = @splat(null),
    gens: [max_subscribers]u32 = @splat(0),
    /// Reusable render buffer: one message is built here and written to
    /// every subscriber. Grows to the largest message seen and then
    /// stops allocating.
    scratch: std.ArrayList(u8) = .empty,

    stats: Stats = .{},

    pub const Stats = struct {
        /// Messages handed to `publish`, before fan-out.
        published: u64 = 0,
        /// Successful per-subscriber writes.
        delivered: u64 = 0,
        subscribed: u64 = 0,
        dropped_slow: u64 = 0,
        dropped_closed: u64 = 0,
        /// `publish` calls that could not render, i.e. OOM.
        render_failed: u64 = 0,
    };

    const Subscriber = struct {
        sink: Sink,
        delivered: u64 = 0,
    };

    /// Sinks condemned during a fan-out, to be closed once the table
    /// lock is released.
    ///
    /// The lock cannot be held across `closeFn`: in production that hook
    /// tears down the HTTP connection, whose close callback calls
    /// `unsubscribe`, which takes this same lock. `core/log.zig`'s mutex
    /// is not recursive — and making it recursive would be the wrong fix,
    /// because a callback into application code is exactly where a lock
    /// should not still be held.
    const DropList = struct {
        items: [max_subscribers]Sink = undefined,
        len: usize = 0,

        fn add(self: *DropList, sink: Sink) void {
            if (self.len == self.items.len) return;
            self.items[self.len] = sink;
            self.len += 1;
        }

        fn closeAll(self: *const DropList) void {
            for (self.items[0..self.len]) |sink| sink.close();
        }
    };

    pub fn init(gpa: Allocator) Hub {
        return .{ .gpa = gpa };
    }

    /// Disconnects every subscriber and releases the render buffer.
    pub fn deinit(self: *Hub) void {
        self.closeAll(.shutdown);
        self.scratch.deinit(self.gpa);
        self.* = undefined;
    }

    pub fn count(self: *Hub) usize {
        self.mu.lock();
        defer self.mu.unlock();
        var n: usize = 0;
        for (self.subs) |s| {
            if (s != null) n += 1;
        }
        return n;
    }

    /// Register an open stream. The caller has already sent the
    /// event-stream head and detached the connection.
    pub fn subscribe(self: *Hub, sink: Sink) SubscribeError!Token {
        self.mu.lock();
        defer self.mu.unlock();
        for (&self.subs, 0..) |*slot, i| {
            if (slot.* != null) continue;
            slot.* = .{ .sink = sink };
            self.stats.subscribed += 1;
            return .{ .slot = i, .gen = self.gens[i] };
        }
        return error.TooManySubscribers;
    }

    /// Release a slot. Idempotent, safe with a stale token, and safe to
    /// call from inside the sink's own `closeFn` — which is exactly what
    /// happens in production, where the HTTP layer's close hook is what
    /// tells the hub the client went away.
    ///
    /// Does **not** call `closeFn`: this is the "the stream is already
    /// gone" direction.
    pub fn unsubscribe(self: *Hub, token: Token) void {
        self.mu.lock();
        defer self.mu.unlock();
        _ = self.slotPtr(token) orelse return;
        self.release(token.slot);
    }

    /// Deliveries this subscriber has accepted. Test and diagnostics use.
    pub fn deliveredTo(self: *Hub, token: Token) ?u64 {
        self.mu.lock();
        defer self.mu.unlock();
        const s = self.slotPtr(token) orelse return null;
        return s.delivered;
    }

    /// True while the token names an open stream.
    pub fn isLive(self: *Hub, token: Token) bool {
        self.mu.lock();
        defer self.mu.unlock();
        return self.slotPtr(token) != null;
    }

    // -- publishing ---------------------------------------------------

    /// `event: <topic>\ndata: <payload>\n\n` to every subscriber.
    ///
    /// `payload` is spliced in as-is and is expected to be JSON — an
    /// outbox envelope, a rendered log entry. It may contain newlines
    /// (a pretty-printed payload out of the database does): each line
    /// becomes its own `data:` field, which is how SSE carries a
    /// multi-line body and what the browser reassembles.
    ///
    /// Never fails: a subscriber that cannot take the message is
    /// dropped, and a render that cannot allocate is counted and
    /// discarded. A live view is not worth failing a caller over.
    pub fn publish(self: *Hub, topic: []const u8, payload: []const u8) void {
        var drops: DropList = .{};
        {
            self.mu.lock();
            defer self.mu.unlock();
            self.stats.published += 1;
            self.scratch.clearRetainingCapacity();
            self.render(topic, payload) catch {
                self.stats.render_failed += 1;
                return;
            };
            self.fanOutLocked(self.scratch.items, &drops);
        }
        drops.closeAll();
    }

    /// A pre-rendered message, for a caller that has its own buffer.
    /// `msg` must already be a complete SSE block ending in a blank
    /// line.
    pub fn publishRaw(self: *Hub, msg: []const u8) void {
        var drops: DropList = .{};
        {
            self.mu.lock();
            defer self.mu.unlock();
            self.stats.published += 1;
            self.fanOutLocked(msg, &drops);
        }
        drops.closeAll();
    }

    /// The keep-alive comment. A comment rather than an event so a
    /// client's `onmessage` never sees it.
    pub fn heartbeat(self: *Hub) void {
        var drops: DropList = .{};
        {
            self.mu.lock();
            defer self.mu.unlock();
            self.fanOutLocked(": ping\n\n", &drops);
        }
        drops.closeAll();
    }

    /// Greeting for one freshly-subscribed stream, so the client knows
    /// the connection is established before any event arrives. The Go
    /// handler sent `: connected <uuid>`; the slot number serves the
    /// same purpose without minting a UUID nobody correlates.
    pub fn sendHello(self: *Hub, token: Token) void {
        var buf: [48]u8 = undefined;
        const msg = std.fmt.bufPrint(&buf, ": connected {d}\n\n", .{token.slot}) catch return;
        var drops: DropList = .{};
        {
            self.mu.lock();
            defer self.mu.unlock();
            const s = self.slotPtr(token) orelse return;
            self.deliverLocked(token.slot, s, msg, &drops);
        }
        drops.closeAll();
    }

    /// One event to one subscriber. Used by the log tail's initial
    /// `ready` event.
    pub fn sendTo(self: *Hub, token: Token, topic: []const u8, payload: []const u8) void {
        var drops: DropList = .{};
        {
            self.mu.lock();
            defer self.mu.unlock();
            const s = self.slotPtr(token) orelse return;
            self.scratch.clearRetainingCapacity();
            self.render(topic, payload) catch {
                self.stats.render_failed += 1;
                return;
            };
            self.deliverLocked(token.slot, s, self.scratch.items, &drops);
        }
        drops.closeAll();
    }

    /// Disconnect everyone — server shutdown, or a config change that
    /// invalidates what the streams are showing.
    pub fn closeAll(self: *Hub, reason: DropReason) void {
        var drops: DropList = .{};
        {
            self.mu.lock();
            defer self.mu.unlock();
            for (0..max_subscribers) |i| {
                const s = self.subs[i] orelse continue;
                self.dropLocked(i, s.sink, reason, &drops);
            }
        }
        drops.closeAll();
    }

    // -- internals ----------------------------------------------------

    fn slotPtr(self: *Hub, token: Token) ?*Subscriber {
        if (token.slot >= max_subscribers) return null;
        if (self.gens[token.slot] != token.gen) return null;
        if (self.subs[token.slot] == null) return null;
        return &self.subs[token.slot].?;
    }

    fn render(self: *Hub, topic: []const u8, payload: []const u8) Allocator.Error!void {
        const gpa = self.gpa;
        try self.scratch.appendSlice(gpa, "event: ");
        try appendFieldValue(&self.scratch, gpa, topic);
        try self.scratch.append(gpa, '\n');

        // Split on LF; a bare CR is dropped rather than emitted, because
        // SSE treats CR, LF and CRLF alike as line ends and a stray one
        // would split the field in the client's parser.
        var rest = payload;
        while (true) {
            const nl = std.mem.indexOfScalar(u8, rest, '\n');
            const line = if (nl) |i| rest[0..i] else rest;
            try self.scratch.appendSlice(gpa, "data: ");
            try appendFieldValue(&self.scratch, gpa, line);
            try self.scratch.append(gpa, '\n');
            if (nl) |i| rest = rest[i + 1 ..] else break;
            if (rest.len == 0) break;
        }
        try self.scratch.append(gpa, '\n');
    }

    fn fanOutLocked(self: *Hub, msg: []const u8, drops: *DropList) void {
        for (0..max_subscribers) |i| {
            const s = &(self.subs[i] orelse continue);
            self.deliverLocked(i, s, msg, drops);
        }
    }

    fn deliverLocked(self: *Hub, slot: usize, s: *Subscriber, msg: []const u8, drops: *DropList) void {
        // Backpressure check first: a client that has stopped reading
        // must not be handed more bytes to queue, and the queue is the
        // only place they could go.
        const sink = s.sink;
        if (sink.pending() > self.max_pending_bytes) {
            self.dropLocked(slot, sink, .slow, drops);
            return;
        }
        sink.write(msg) catch |err| {
            self.dropLocked(slot, sink, switch (err) {
                error.QueueFull => .slow,
                else => .closed,
            }, drops);
            return;
        };
        s.delivered += 1;
        self.stats.delivered += 1;
    }

    /// Free the slot now, close the stream later. Freeing first means the
    /// re-entrant `unsubscribe` from the close hook sees a generation
    /// mismatch and does nothing, and means a slot is never held by a
    /// subscriber that is on its way out.
    fn dropLocked(self: *Hub, slot: usize, sink: Sink, reason: DropReason, drops: *DropList) void {
        switch (reason) {
            .slow => self.stats.dropped_slow += 1,
            .closed, .shutdown => self.stats.dropped_closed += 1,
        }
        self.release(slot);
        drops.add(sink);
    }

    fn release(self: *Hub, slot: usize) void {
        self.subs[slot] = null;
        self.gens[slot] +%= 1;
    }
};

/// Append one SSE field value, with the bytes that would break framing
/// removed. Field values cannot contain CR or LF, and a NUL in a
/// `text/event-stream` is illegal, so all three are dropped.
///
/// The common case — a topic, or a line of JSON out of `rest/json.zig`,
/// neither of which can contain any of them — takes the fast path and is
/// a single `appendSlice`.
fn appendFieldValue(buf: *std.ArrayList(u8), gpa: Allocator, s: []const u8) Allocator.Error!void {
    if (std.mem.indexOfAny(u8, s, "\r\n\x00") == null) {
        return buf.appendSlice(gpa, s);
    }
    for (s) |c| {
        if (c == '\r' or c == '\n' or c == 0) continue;
        try buf.append(gpa, c);
    }
}

// ---------------------------------------------------------------------
// Rendering a log entry
// ---------------------------------------------------------------------

/// One log-ring entry as the JSON the System page's live tail consumes:
/// `{"time":..,"level":"WARN","message":..,"attrs":{"k":"v"}}`.
///
/// The shape is `loghub.Entry`'s, field names and all, because
/// `frontend/src/api/types.ts` declares it — including `attrs` being
/// `Record<string, string>`, which is why a typed attribute is rendered
/// as its text form rather than as a JSON number or bool. Go stringified
/// at capture time; here the ring keeps the type and this is where it is
/// flattened, so the log file still gets the typed value.
///
/// Everything goes through `rest/json.zig`, so an attribute carrying
/// control bytes — a server greeting, a filename off the wire — cannot
/// break the event.
pub fn renderLogEntry(w: *json.Writer, e: *const logring.Entry) json.Error!void {
    try w.beginObject();
    try w.timeField("time", @intCast(@divFloor(e.time_ns, std.time.ns_per_ms)));
    try w.strField("level", e.level.label());
    try w.strField("message", e.message());
    // Go's `attrs,omitempty`: absent rather than `{}` when there are none.
    const n = e.attrCount();
    if (n > 0) {
        try w.key("attrs");
        try w.beginObject();
        for (0..n) |i| {
            try w.key(e.attrKey(i));
            var buf: [64]u8 = undefined;
            try w.string(switch (e.attrValue(i)) {
                .str => |v| v,
                .int => |v| std.fmt.bufPrint(&buf, "{d}", .{v}) catch "?",
                .uint => |v| std.fmt.bufPrint(&buf, "{d}", .{v}) catch "?",
                .float => |v| std.fmt.bufPrint(&buf, "{d}", .{v}) catch "?",
                .boolean => |v| if (v) "true" else "false",
                .err => |v| @errorName(v),
                .none => "",
            });
        }
        try w.endObject();
    }
    // Not in the Go shape: the ring can clip an over-long record, and a
    // clipped line should not be mistaken for the whole story. Additive,
    // so an older frontend ignores it.
    if (e.truncated) try w.boolField("truncated", true);
    try w.endObject();
}

// ---------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------

const testing = std.testing;

/// A subscriber that records what it was given and can be told to
/// pretend it has stopped reading.
const FakeSink = struct {
    gpa: Allocator,
    got: std.ArrayList(u8) = .empty,
    messages: usize = 0,
    /// What `pending` reports. Raise it above the hub's cap to simulate
    /// a client that is not draining.
    pending_bytes: usize = 0,
    /// Fail the next write with this.
    fail_with: ?WriteError = null,
    closed: usize = 0,
    /// When set, `closeFn` calls `Hub.unsubscribe` — what the real HTTP
    /// close hook does.
    hub: ?*Hub = null,
    token: Hub.Token = .{ .slot = 0, .gen = 0 },

    fn deinit(self: *FakeSink) void {
        self.got.deinit(self.gpa);
    }

    fn sink(self: *FakeSink) Sink {
        return .{ .ctx = self, .writeFn = write, .pendingFn = pending, .closeFn = close };
    }

    fn write(ctx: ?*anyopaque, bytes: []const u8) WriteError!void {
        const self: *FakeSink = @ptrCast(@alignCast(ctx.?));
        if (self.fail_with) |e| return e;
        try self.got.appendSlice(self.gpa, bytes);
        self.messages += 1;
    }

    fn pending(ctx: ?*anyopaque) usize {
        const self: *FakeSink = @ptrCast(@alignCast(ctx.?));
        return self.pending_bytes;
    }

    fn close(ctx: ?*anyopaque) void {
        const self: *FakeSink = @ptrCast(@alignCast(ctx.?));
        self.closed += 1;
        if (self.hub) |h| h.unsubscribe(self.token);
    }
};

test "an event is rendered once and fanned out to every subscriber" {
    var hub = Hub.init(testing.allocator);
    defer hub.deinit();

    var a: FakeSink = .{ .gpa = testing.allocator };
    defer a.deinit();
    var b: FakeSink = .{ .gpa = testing.allocator };
    defer b.deinit();
    _ = try hub.subscribe(a.sink());
    _ = try hub.subscribe(b.sink());
    try testing.expectEqual(@as(usize, 2), hub.count());

    hub.publish("download.job.started", "{\"job_id\":7}");

    const want = "event: download.job.started\ndata: {\"job_id\":7}\n\n";
    try testing.expectEqualStrings(want, a.got.items);
    try testing.expectEqualStrings(want, b.got.items);
    try testing.expectEqual(@as(u64, 1), hub.stats.published);
    try testing.expectEqual(@as(u64, 2), hub.stats.delivered);
}

test "unsubscribe stops delivery and is idempotent" {
    var hub = Hub.init(testing.allocator);
    defer hub.deinit();

    var a: FakeSink = .{ .gpa = testing.allocator };
    defer a.deinit();
    const tok = try hub.subscribe(a.sink());

    hub.publish("t", "{}");
    hub.unsubscribe(tok);
    hub.unsubscribe(tok);
    hub.unsubscribe(.{ .slot = 999, .gen = 0 });
    hub.publish("t", "{}");

    try testing.expectEqual(@as(usize, 1), a.messages);
    try testing.expectEqual(@as(usize, 0), hub.count());
    // A stale token must not report as live even after the slot is
    // handed to someone else.
    var b: FakeSink = .{ .gpa = testing.allocator };
    defer b.deinit();
    _ = try hub.subscribe(b.sink());
    try testing.expect(!hub.isLive(tok));
}

test "a slow consumer is dropped rather than allowed to grow memory" {
    var hub = Hub.init(testing.allocator);
    hub.max_pending_bytes = 1024;
    defer hub.deinit();

    var slow: FakeSink = .{ .gpa = testing.allocator };
    defer slow.deinit();
    var fast: FakeSink = .{ .gpa = testing.allocator };
    defer fast.deinit();
    const slow_tok = try hub.subscribe(slow.sink());
    _ = try hub.subscribe(fast.sink());

    // Both healthy to start with.
    hub.publish("download.segment.completed", "{\"n\":1}");
    try testing.expectEqual(@as(usize, 1), slow.messages);

    // The client stops reading: its socket queue is over the cap.
    slow.pending_bytes = 1025;

    for (0..1000) |i| {
        var buf: [64]u8 = undefined;
        const payload = try std.fmt.bufPrint(&buf, "{{\"n\":{d}}}", .{i});
        hub.publish("download.segment.completed", payload);
    }

    // Dropped on the very first attempt after it stalled, and never
    // written to again — so nothing accumulated anywhere on its behalf.
    try testing.expectEqual(@as(usize, 1), slow.messages);
    try testing.expectEqual(@as(usize, 1), slow.closed);
    try testing.expectEqual(@as(u64, 1), hub.stats.dropped_slow);
    try testing.expect(!hub.isLive(slow_tok));
    try testing.expectEqual(@as(usize, 1), hub.count());

    // And the healthy subscriber kept every message.
    try testing.expectEqual(@as(usize, 1001), fast.messages);
}

test "a sink reporting a full queue is dropped as slow" {
    var hub = Hub.init(testing.allocator);
    defer hub.deinit();

    var s: FakeSink = .{ .gpa = testing.allocator, .fail_with = error.QueueFull };
    defer s.deinit();
    const tok = try hub.subscribe(s.sink());

    hub.publish("t", "{}");
    try testing.expectEqual(@as(u64, 1), hub.stats.dropped_slow);
    try testing.expectEqual(@as(u64, 0), hub.stats.dropped_closed);
    try testing.expect(!hub.isLive(tok));
}

test "a closed peer is dropped as closed" {
    var hub = Hub.init(testing.allocator);
    defer hub.deinit();

    var s: FakeSink = .{ .gpa = testing.allocator, .fail_with = error.StreamClosed };
    defer s.deinit();
    _ = try hub.subscribe(s.sink());

    hub.publish("t", "{}");
    try testing.expectEqual(@as(u64, 1), hub.stats.dropped_closed);
    try testing.expectEqual(@as(usize, 1), s.closed);
    try testing.expectEqual(@as(usize, 0), hub.count());
}

test "a close hook that re-enters unsubscribe during fan-out is safe" {
    var hub = Hub.init(testing.allocator);
    defer hub.deinit();

    // Three subscribers; the middle one dies mid-broadcast and its
    // close hook calls back into the hub, exactly as the HTTP layer's
    // on_close does.
    var a: FakeSink = .{ .gpa = testing.allocator };
    defer a.deinit();
    var dying: FakeSink = .{ .gpa = testing.allocator, .fail_with = error.StreamClosed };
    defer dying.deinit();
    var c: FakeSink = .{ .gpa = testing.allocator };
    defer c.deinit();

    _ = try hub.subscribe(a.sink());
    dying.hub = &hub;
    dying.token = try hub.subscribe(dying.sink());
    _ = try hub.subscribe(c.sink());

    hub.publish("t", "{\"x\":1}");

    // The subscriber after the casualty still got the message: the slot
    // table was not reshuffled underneath the loop.
    try testing.expectEqual(@as(usize, 1), a.messages);
    try testing.expectEqual(@as(usize, 1), c.messages);
    try testing.expectEqual(@as(usize, 1), dying.closed);
    try testing.expectEqual(@as(usize, 2), hub.count());

    hub.publish("t", "{\"x\":2}");
    try testing.expectEqual(@as(usize, 2), a.messages);
    try testing.expectEqual(@as(usize, 2), c.messages);
    // Never written to again, and never closed twice.
    try testing.expectEqual(@as(usize, 0), dying.messages);
    try testing.expectEqual(@as(usize, 1), dying.closed);
}

test "the subscriber table is bounded" {
    var hub = Hub.init(testing.allocator);
    defer hub.deinit();

    var sinks: [max_subscribers]FakeSink = undefined;
    for (&sinks) |*s| s.* = .{ .gpa = testing.allocator };
    defer for (&sinks) |*s| s.deinit();

    for (&sinks) |*s| _ = try hub.subscribe(s.sink());
    var extra: FakeSink = .{ .gpa = testing.allocator };
    defer extra.deinit();
    try testing.expectError(error.TooManySubscribers, hub.subscribe(extra.sink()));

    // A slot freed by a departing client is reusable.
    hub.unsubscribe(.{ .slot = 3, .gen = 0 });
    _ = try hub.subscribe(extra.sink());
    try testing.expectEqual(@as(usize, max_subscribers), hub.count());
}

test "a multi-line payload becomes one data field per line" {
    var hub = Hub.init(testing.allocator);
    defer hub.deinit();

    var s: FakeSink = .{ .gpa = testing.allocator };
    defer s.deinit();
    _ = try hub.subscribe(s.sink());

    hub.publish("verify.failed", "{\n  \"a\": 1\n}");
    try testing.expectEqualStrings(
        "event: verify.failed\ndata: {\ndata:   \"a\": 1\ndata: }\n\n",
        s.got.items,
    );
}

test "framing bytes in a topic or payload cannot forge an event" {
    var hub = Hub.init(testing.allocator);
    defer hub.deinit();

    var s: FakeSink = .{ .gpa = testing.allocator };
    defer s.deinit();
    _ = try hub.subscribe(s.sink());

    // A topic that tries to close its own event and start another, and a
    // payload with a CR and a NUL in it.
    hub.publish("evil\n\ndata: injected\n\nevent: spoofed", "{\"a\":\"b\r\x00\"}");

    const got = s.got.items;
    // Exactly one event: one terminating blank line, at the very end, so
    // the forged "event:"/"data:" text is stuck inside a field value
    // rather than starting a message of its own.
    try testing.expectEqual(@as(usize, 1), std.mem.count(u8, got, "\n\n"));
    try testing.expect(std.mem.endsWith(u8, got, "\n\n"));
    try testing.expectEqual(@as(usize, 3), std.mem.count(u8, got, "\n"));
    try testing.expectEqualStrings(
        "event: evildata: injectedevent: spoofed\ndata: {\"a\":\"b\"}\n\n",
        got,
    );
}

test "heartbeat is a comment, hello identifies the stream" {
    var hub = Hub.init(testing.allocator);
    defer hub.deinit();

    var s: FakeSink = .{ .gpa = testing.allocator };
    defer s.deinit();
    const tok = try hub.subscribe(s.sink());

    hub.sendHello(tok);
    hub.heartbeat();
    try testing.expectEqualStrings(": connected 0\n\n: ping\n\n", s.got.items);
}

test "a stalled subscriber is dropped by the heartbeat too" {
    var hub = Hub.init(testing.allocator);
    hub.max_pending_bytes = 16;
    defer hub.deinit();

    var s: FakeSink = .{ .gpa = testing.allocator, .pending_bytes = 17 };
    defer s.deinit();
    _ = try hub.subscribe(s.sink());

    // No events at all, just the keep-alive: a client that stopped
    // reading is still found and dropped within one heartbeat.
    hub.heartbeat();
    try testing.expectEqual(@as(usize, 0), hub.count());
    try testing.expectEqual(@as(u64, 1), hub.stats.dropped_slow);
}

test "sendTo targets one subscriber" {
    var hub = Hub.init(testing.allocator);
    defer hub.deinit();

    var a: FakeSink = .{ .gpa = testing.allocator };
    defer a.deinit();
    var b: FakeSink = .{ .gpa = testing.allocator };
    defer b.deinit();
    const ta = try hub.subscribe(a.sink());
    _ = try hub.subscribe(b.sink());

    hub.sendTo(ta, "ready", "{}");
    try testing.expectEqualStrings("event: ready\ndata: {}\n\n", a.got.items);
    try testing.expectEqual(@as(usize, 0), b.messages);
}

test "closeAll disconnects everyone" {
    var hub = Hub.init(testing.allocator);
    defer hub.deinit();

    var a: FakeSink = .{ .gpa = testing.allocator };
    defer a.deinit();
    var b: FakeSink = .{ .gpa = testing.allocator };
    defer b.deinit();
    a.hub = &hub;
    a.token = try hub.subscribe(a.sink());
    b.hub = &hub;
    b.token = try hub.subscribe(b.sink());

    hub.closeAll(.shutdown);
    try testing.expectEqual(@as(usize, 1), a.closed);
    try testing.expectEqual(@as(usize, 1), b.closed);
    try testing.expectEqual(@as(usize, 0), hub.count());

    // And deinit does not double-close.
    hub.deinit();
    hub = Hub.init(testing.allocator);
    try testing.expectEqual(@as(usize, 1), a.closed);
}

test "publish with no subscribers is free and does not fail" {
    var hub = Hub.init(testing.allocator);
    defer hub.deinit();
    for (0..100) |_| hub.publish("download.job.created", "{}");
    try testing.expectEqual(@as(u64, 100), hub.stats.published);
    try testing.expectEqual(@as(u64, 0), hub.stats.delivered);
}

test "default topics match the Go list" {
    try testing.expectEqual(@as(usize, 42), default_topics.len);
    try testing.expect(isDefaultTopic("download.job.created"));
    try testing.expect(isDefaultTopic("system.pools"));
    try testing.expect(isDefaultTopic("notify.subscription.removed"));
    try testing.expect(!isDefaultTopic("auth.user.created"));
    try testing.expect(!isDefaultTopic(""));
}

test "a log entry renders as the shape the live tail expects" {
    var ring = try logring.Ring.init(testing.allocator, 8);
    defer ring.deinit();

    var logger: log.Logger = .{};
    logger.setMirror(ring.mirror());
    logger.log(.warn, "nntp: article missing", &.{
        log.str("server", "news.example\n\"quoted\""),
        log.int("attempts", 3),
        log.boolean("backup", true),
    });

    var buf: [4]logring.Entry = undefined;
    const n = ring.snapshot(&buf);
    try testing.expectEqual(@as(usize, 1), n);

    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    var w = json.Writer.init(arena.allocator());
    try renderLogEntry(&w, &buf[0]);

    const v = try std.json.parseFromSliceLeaky(std.json.Value, arena.allocator(), w.items(), .{});
    try testing.expectEqualStrings("WARN", v.object.get("level").?.string);
    try testing.expectEqualStrings("nntp: article missing", v.object.get("message").?.string);
    try testing.expect(v.object.get("time").?.string.len == log.ts_len);
    // `attrs` is Record<string, string> on the wire, so typed values
    // arrive as text.
    const attrs = v.object.get("attrs").?.object;
    try testing.expectEqualStrings("news.example\n\"quoted\"", attrs.get("server").?.string);
    try testing.expectEqualStrings("3", attrs.get("attempts").?.string);
    try testing.expectEqualStrings("true", attrs.get("backup").?.string);
}

test "a log entry with hostile attribute bytes still publishes one event" {
    var hub = Hub.init(testing.allocator);
    defer hub.deinit();
    var s: FakeSink = .{ .gpa = testing.allocator };
    defer s.deinit();
    _ = try hub.subscribe(s.sink());

    var ring = try logring.Ring.init(testing.allocator, 4);
    defer ring.deinit();
    var logger: log.Logger = .{};
    logger.setMirror(ring.mirror());
    logger.log(.err, "decode failed", &.{
        log.str("name", "a\nb\r\ndata: forged\n\n\xffc"),
    });

    var entries: [2]logring.Entry = undefined;
    _ = ring.snapshot(&entries);

    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    var w = json.Writer.init(arena.allocator());
    try renderLogEntry(&w, &entries[0]);
    hub.publish("log", w.items());

    const got = s.got.items;
    try testing.expectEqual(@as(usize, 1), std.mem.count(u8, got, "event: "));
    try testing.expectEqual(@as(usize, 1), std.mem.count(u8, got, "\n\n"));
    // The data line is still valid JSON once the SSE framing is peeled.
    const data_start = std.mem.indexOf(u8, got, "data: ").? + 6;
    const data_end = std.mem.indexOfPos(u8, got, data_start, "\n").?;
    _ = try std.json.parseFromSliceLeaky(
        std.json.Value,
        arena.allocator(),
        got[data_start..data_end],
        .{},
    );
}
