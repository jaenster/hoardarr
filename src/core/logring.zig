//! In-memory ring buffer of recent log records, plus a fanout to live
//! subscribers. This is what the System page's "Logs" tab reads: the
//! last N lines on load, then a tail of new ones over SSE.
//!
//! Ported from Go's `internal/loghub`. Same reasoning: reading the tail
//! back out of the log file would mean seek logic and a race with
//! rotation, whereas a few hundred KB of RAM and a memcpy per record
//! answers the question directly. Older entries fall off when the ring
//! wraps — this is a tail view, not an audit log.
//!
//! It registers with a `log.Logger` as a `log.Mirror`, so the file and
//! stdout sinks keep working unchanged. A tee, not a replacement.
//!
//! Two differences from the Go version, both forced by not having a GC:
//!
//!   * An `Entry` owns its text. The logger hands out slices that point
//!     at the caller's stack, so the ring has to copy the message and
//!     every attribute before returning. Storage is a fixed byte array
//!     per slot, addressed by offset rather than by slice, which keeps
//!     `Entry` copyable — `snapshot` hands out values, and a slice into
//!     a moved struct would dangle.
//!   * Subscribers are callbacks instead of channels. The SSE hub is
//!     reactor-driven, so "deliver an entry" is "append to a
//!     connection's out-buffer" — a function pointer, not a rendezvous.
//!     Delivery is synchronous under the subscriber lock, and a
//!     subscriber that cannot accept an entry drops it, exactly like
//!     Go's `select` with a `default` arm.

const std = @import("std");
const log = @import("log.zig");

/// Attributes kept per entry. Records with more are truncated; the UI
/// shows a tail, and a record with a dozen attributes is already
/// unusual.
pub const max_attrs = 12;

/// Text bytes kept per entry — message plus all attribute keys and
/// string values. At the default capacity of 1024 entries this is the
/// bulk of the ring's footprint (~640 KiB), which matches the Go
/// version's "a few hundred KB".
pub const entry_text_bytes = 512;

/// Offset/length into an `Entry`'s own storage. Offsets rather than
/// slices so an `Entry` can be copied by value.
const Span = struct {
    off: u16 = 0,
    len: u16 = 0,
};

/// An attribute with its string parts relocated into entry storage.
const StoredAttr = struct {
    key: Span,
    /// Mirrors `log.Value` but with `str` replaced by a span. Non-string
    /// values are already self-contained.
    value: union(enum) {
        str: Span,
        int: i64,
        uint: u64,
        float: f64,
        boolean: bool,
        err: anyerror,
        none: void,
    },
};

/// One captured record. Self-contained and copyable.
pub const Entry = struct {
    /// Wall-clock nanoseconds since the Unix epoch, as taken by the
    /// logger — the same instant the file sink stamped.
    time_ns: i128 = 0,
    level: log.Level = .info,
    msg_span: Span = .{},
    attrs: [max_attrs]StoredAttr = undefined,
    n_attrs: u8 = 0,
    /// Set when the message, an attribute, or the attribute count did
    /// not fit. Surfaced to the UI so a clipped line is not mistaken
    /// for the whole story.
    truncated: bool = false,
    text_len: u16 = 0,
    text: [entry_text_bytes]u8 = undefined,

    pub fn message(self: *const Entry) []const u8 {
        return self.text[self.msg_span.off..][0..self.msg_span.len];
    }

    pub fn attrCount(self: *const Entry) usize {
        return self.n_attrs;
    }

    pub fn attrKey(self: *const Entry, i: usize) []const u8 {
        const s = self.attrs[i].key;
        return self.text[s.off..][0..s.len];
    }

    /// Rehydrate an attribute value as a `log.Value`, with string data
    /// borrowed from this entry. Valid as long as `self` is.
    pub fn attrValue(self: *const Entry, i: usize) log.Value {
        return switch (self.attrs[i].value) {
            .str => |s| .{ .str = self.text[s.off..][0..s.len] },
            .int => |x| .{ .int = x },
            .uint => |x| .{ .uint = x },
            .float => |x| .{ .float = x },
            .boolean => |x| .{ .boolean = x },
            .err => |e| .{ .err = e },
            .none => .none,
        };
    }

    pub fn attr(self: *const Entry, i: usize) log.Attr {
        return .{ .key = self.attrKey(i), .value = self.attrValue(i) };
    }

    /// Look up an attribute by key. Linear, but `max_attrs` is 12 and
    /// this is only used by the API layer and by tests.
    pub fn find(self: *const Entry, key: []const u8) ?log.Value {
        for (0..self.n_attrs) |i| {
            if (std.mem.eql(u8, self.attrKey(i), key)) return self.attrValue(i);
        }
        return null;
    }

    /// Re-encode this entry as a one-line record. Used by the SSE
    /// endpoint, which ships the same shape the file gets, and by the
    /// JSON list endpoint.
    pub fn render(self: *const Entry, out: []u8, format: log.Format) []const u8 {
        var attrs: [max_attrs]log.Attr = undefined;
        for (0..self.n_attrs) |i| attrs[i] = self.attr(i);
        return log.encode(out, format, self.time_ns, self.level, self.message(), attrs[0..self.n_attrs]);
    }

    fn store(self: *Entry, s: []const u8) Span {
        const room = entry_text_bytes - self.text_len;
        const n = @min(s.len, room);
        if (n < s.len) self.truncated = true;
        @memcpy(self.text[self.text_len..][0..n], s[0..n]);
        const span = Span{ .off = self.text_len, .len = @intCast(n) };
        self.text_len += @intCast(n);
        return span;
    }
};

/// Callback invoked for every published entry. `entry` is borrowed for
/// the duration of the call only — a subscriber that needs to keep it
/// must copy.
pub const OnEntry = *const fn (ctx: *anyopaque, entry: *const Entry) void;

pub const Subscriber = struct {
    ctx: *anyopaque,
    on_entry: OnEntry,
};

/// Concurrent subscribers. The SSE endpoint holds one per open
/// connection; the System page rarely has more than a couple of tabs
/// watching.
pub const max_subscribers = 32;

pub const Ring = struct {
    /// Handle returned by `subscribe`, passed back to `unsubscribe`.
    /// The generation counter makes double-unsubscribe and stale-handle
    /// unsubscribe both harmless, which is what Go got from `sync.Once`
    /// plus closing the channel.
    pub const Token = struct {
        slot: usize,
        gen: u32,
    };

    pub const InitError = std.mem.Allocator.Error;

    allocator: std.mem.Allocator,
    /// Guards the ring. Separate from `subs_mu` so a slow subscriber
    /// callback cannot block a writer from recording.
    mu: log.Mutex = .{},
    slots: []Entry,
    head: usize = 0,
    wrapped: bool = false,

    subs_mu: log.Mutex = .{},
    subs: [max_subscribers]?Subscriber = @splat(null),
    gens: [max_subscribers]u32 = @splat(0),

    /// Go defaulted to 1024 entries; a capacity of 0 is treated the
    /// same way rather than rejected, because the caller is a config
    /// value and 0 means "unset".
    pub const default_capacity = 1024;

    pub fn init(allocator: std.mem.Allocator, cap_hint: usize) InitError!Ring {
        const cap = if (cap_hint == 0) default_capacity else cap_hint;
        const slots = try allocator.alloc(Entry, cap);
        // Only the header fields need initialising; `text` is written
        // before it is ever read, bounded by `text_len`.
        for (slots) |*s| s.* = .{};
        return .{ .allocator = allocator, .slots = slots };
    }

    pub fn deinit(self: *Ring) void {
        self.allocator.free(self.slots);
        self.* = undefined;
    }

    pub fn capacity(self: *const Ring) usize {
        return self.slots.len;
    }

    /// Append a record and fan it out. Copies every borrowed byte, so
    /// the caller's stack buffers are free the moment this returns.
    pub fn publish(
        self: *Ring,
        ts_ns: i128,
        level: log.Level,
        msg: []const u8,
        attrs: []const log.Attr,
    ) void {
        self.mu.lock();
        const slot = &self.slots[self.head];
        slot.* = .{ .time_ns = ts_ns, .level = level };
        slot.msg_span = slot.store(msg);
        const n = @min(attrs.len, max_attrs);
        if (attrs.len > max_attrs) slot.truncated = true;
        for (attrs[0..n], 0..) |a, i| {
            slot.attrs[i] = .{
                .key = slot.store(a.key),
                .value = switch (a.value) {
                    .str => |s| .{ .str = slot.store(s) },
                    .int => |x| .{ .int = x },
                    .uint => |x| .{ .uint = x },
                    .float => |x| .{ .float = x },
                    .boolean => |x| .{ .boolean = x },
                    .err => |e| .{ .err = e },
                    .none => .none,
                },
            };
        }
        slot.n_attrs = @intCast(n);
        self.head = (self.head + 1) % self.slots.len;
        if (self.head == 0) self.wrapped = true;
        // Copy out before releasing so the fanout sees a stable entry
        // even if the ring wraps onto this slot immediately after.
        const published = slot.*;
        self.mu.unlock();

        self.subs_mu.lock();
        defer self.subs_mu.unlock();
        for (self.subs) |maybe| {
            if (maybe) |s| s.on_entry(s.ctx, &published);
        }
    }

    /// `log.Mirror` adapter. Hand the result to
    /// `Logger.setMirror(ring.mirror())`.
    pub fn mirror(self: *Ring) log.Mirror {
        return .{ .ctx = self, .publish = mirrorPublish };
    }

    fn mirrorPublish(
        ctx: *anyopaque,
        ts_ns: i128,
        level: log.Level,
        msg: []const u8,
        attrs: []const log.Attr,
    ) void {
        const self: *Ring = @ptrCast(@alignCast(ctx));
        self.publish(ts_ns, level, msg, attrs);
    }

    /// Number of entries currently held.
    pub fn count(self: *Ring) usize {
        self.mu.lock();
        defer self.mu.unlock();
        return if (self.wrapped) self.slots.len else self.head;
    }

    /// Copy the ring contents oldest → newest into `out`, returning how
    /// many entries were written. Caller-provided storage keeps the
    /// snapshot path allocation-free; the REST handler reuses one
    /// buffer across requests.
    pub fn snapshot(self: *Ring, out: []Entry) usize {
        self.mu.lock();
        defer self.mu.unlock();
        const total = if (self.wrapped) self.slots.len else self.head;
        const n = @min(total, out.len);
        if (n == 0) return 0;
        // `head` is the next write slot, so it is also the oldest entry
        // once the ring has wrapped. Before the first wrap the ring is a
        // plain prefix and the oldest is at 0.
        const oldest = if (self.wrapped) self.head else 0;
        // Take the newest `n` when the caller's buffer is smaller than
        // the ring — a truncated tail is what the UI wants, not a
        // truncated head.
        const start = (oldest + (total - n)) % self.slots.len;
        for (0..n) |i| out[i] = self.slots[(start + i) % self.slots.len];
        return n;
    }

    pub const SubscribeError = error{TooManySubscribers};

    pub fn subscribe(self: *Ring, s: Subscriber) SubscribeError!Token {
        self.subs_mu.lock();
        defer self.subs_mu.unlock();
        for (&self.subs, 0..) |*slot, i| {
            if (slot.* != null) continue;
            slot.* = s;
            return .{ .slot = i, .gen = self.gens[i] };
        }
        return error.TooManySubscribers;
    }

    /// Idempotent: a stale or already-released token is ignored. Safe to
    /// call any number of times, which is what the Go `cancel` closure
    /// guaranteed via `sync.Once`.
    pub fn unsubscribe(self: *Ring, token: Token) void {
        self.subs_mu.lock();
        defer self.subs_mu.unlock();
        if (token.slot >= max_subscribers) return;
        if (self.gens[token.slot] != token.gen) return;
        if (self.subs[token.slot] == null) return;
        self.subs[token.slot] = null;
        self.gens[token.slot] +%= 1;
    }
};

// ---------------------------------------------------------------------
// tests
//
// The first block mirrors internal/loghub/loghub_test.go one-for-one.
// ---------------------------------------------------------------------

const t = std.testing;

fn entryAt(ring: *Ring, i: usize, buf: []Entry) Entry {
    const n = ring.snapshot(buf);
    std.debug.assert(i < n);
    return buf[i];
}

test "snapshot of an empty ring is empty" {
    var ring = try Ring.init(t.allocator, 8);
    defer ring.deinit();
    var buf: [8]Entry = undefined;
    try t.expectEqual(@as(usize, 0), ring.snapshot(&buf));
    try t.expectEqual(@as(usize, 0), ring.count());
}

test "capacity zero falls back to the default" {
    var ring = try Ring.init(t.allocator, 0);
    defer ring.deinit();
    try t.expectEqual(Ring.default_capacity, ring.capacity());
}

test "fill under capacity keeps insertion order" {
    var ring = try Ring.init(t.allocator, 8);
    defer ring.deinit();
    for (0..3) |i| {
        ring.publish(@as(i128, @intCast(i)) * std.time.ns_per_s, .info, "msg", &.{});
    }
    var buf: [8]Entry = undefined;
    try t.expectEqual(@as(usize, 3), ring.snapshot(&buf));
    try t.expectEqual(@as(i128, 0), buf[0].time_ns);
    try t.expectEqual(@as(i128, 2 * std.time.ns_per_s), buf[2].time_ns);
    try t.expectEqualStrings("msg", buf[0].message());
}

test "wrapping drops the oldest entries" {
    const cap = 4;
    var ring = try Ring.init(t.allocator, cap);
    defer ring.deinit();
    // Six entries into a ring of four: only the last four survive.
    for (0..6) |i| {
        ring.publish(@as(i128, @intCast(i)) * std.time.ns_per_s, .info, "m", &.{});
    }
    var buf: [cap]Entry = undefined;
    try t.expectEqual(@as(usize, cap), ring.snapshot(&buf));
    try t.expectEqual(@as(i128, 2 * std.time.ns_per_s), buf[0].time_ns);
    try t.expectEqual(@as(i128, 5 * std.time.ns_per_s), buf[3].time_ns);
    try t.expectEqual(@as(usize, cap), ring.count());
}

const Recorder = struct {
    calls: usize = 0,
    last_msg: [64]u8 = undefined,
    last_len: usize = 0,

    fn onEntry(ctx: *anyopaque, entry: *const Entry) void {
        const self: *Recorder = @ptrCast(@alignCast(ctx));
        self.calls += 1;
        const m = entry.message();
        const n = @min(m.len, self.last_msg.len);
        @memcpy(self.last_msg[0..n], m[0..n]);
        self.last_len = n;
    }

    fn subscriber(self: *Recorder) Subscriber {
        return .{ .ctx = self, .on_entry = onEntry };
    }

    fn last(self: *const Recorder) []const u8 {
        return self.last_msg[0..self.last_len];
    }
};

test "subscriber receives new entries" {
    var ring = try Ring.init(t.allocator, 4);
    defer ring.deinit();
    var rec: Recorder = .{};
    const token = try ring.subscribe(rec.subscriber());
    defer ring.unsubscribe(token);

    ring.publish(0, .info, "hello", &.{});
    try t.expectEqual(@as(usize, 1), rec.calls);
    try t.expectEqualStrings("hello", rec.last());
}

test "unsubscribe stops delivery and is idempotent" {
    var ring = try Ring.init(t.allocator, 4);
    defer ring.deinit();
    var rec: Recorder = .{};
    const token = try ring.subscribe(rec.subscriber());
    ring.unsubscribe(token);
    // Repeated and stale unsubscribes must be harmless — the Go
    // version leaned on sync.Once for this.
    ring.unsubscribe(token);
    ring.unsubscribe(.{ .slot = 0, .gen = 999 });
    ring.unsubscribe(.{ .slot = max_subscribers + 5, .gen = 0 });

    ring.publish(0, .info, "after cancel", &.{});
    try t.expectEqual(@as(usize, 0), rec.calls);
    // The ring itself still recorded it.
    try t.expectEqual(@as(usize, 1), ring.count());
}

test "subscriber slots are reusable" {
    var ring = try Ring.init(t.allocator, 4);
    defer ring.deinit();
    var rec: Recorder = .{};
    for (0..max_subscribers * 3) |_| {
        const token = try ring.subscribe(rec.subscriber());
        ring.unsubscribe(token);
    }
    // All slots free again, so a full house still fits.
    var tokens: [max_subscribers]Ring.Token = undefined;
    for (&tokens) |*tok| tok.* = try ring.subscribe(rec.subscriber());
    try t.expectError(error.TooManySubscribers, ring.subscribe(rec.subscriber()));
    for (tokens) |tok| ring.unsubscribe(tok);
}

test "mirror tees the logger into the ring without replacing its sinks" {
    var ring = try Ring.init(t.allocator, 8);
    defer ring.deinit();

    var logger: log.Logger = .{ .clock = zeroClock };
    logger.setLevel(.info);
    logger.setMirror(ring.mirror());
    // No fd/file sink attached: the mirror is additive, and the
    // logger works with the mirror as its only consumer.
    logger.info("test message", &.{log.str("key", "value")});

    var buf: [8]Entry = undefined;
    try t.expectEqual(@as(usize, 1), ring.snapshot(&buf));
    try t.expectEqualStrings("test message", buf[0].message());
    try t.expectEqualStrings("value", buf[0].find("key").?.str);
    try t.expectEqual(log.Level.info, buf[0].level);
}

test "mirror respects the logger's level" {
    var ring = try Ring.init(t.allocator, 8);
    defer ring.deinit();

    var logger: log.Logger = .{ .clock = zeroClock };
    logger.setLevel(.warn);
    logger.setMirror(ring.mirror());

    logger.debug("noisy", &.{});
    logger.info("also-ignored", &.{});
    logger.warn("important", &.{});

    var buf: [8]Entry = undefined;
    try t.expectEqual(@as(usize, 1), ring.snapshot(&buf));
    try t.expectEqualStrings("important", buf[0].message());
    try t.expectEqual(log.Level.warn, buf[0].level);
}

fn zeroClock() i128 {
    return 0;
}

// --- beyond the Go suite ---------------------------------------------

test "typed attribute values survive the round trip" {
    var ring = try Ring.init(t.allocator, 4);
    defer ring.deinit();
    ring.publish(0, .err, "typed", &.{
        log.str("s", "text"),
        log.int("i", -9),
        log.uint("u", 42),
        log.float("f", 2.5),
        log.boolean("b", true),
        log.errv("e", error.Timeout),
        log.none("n"),
    });
    var buf: [4]Entry = undefined;
    _ = ring.snapshot(&buf);
    const e = &buf[0];
    try t.expectEqual(@as(usize, 7), e.attrCount());
    try t.expectEqualStrings("text", e.find("s").?.str);
    try t.expectEqual(@as(i64, -9), e.find("i").?.int);
    try t.expectEqual(@as(u64, 42), e.find("u").?.uint);
    try t.expectEqual(@as(f64, 2.5), e.find("f").?.float);
    try t.expectEqual(true, e.find("b").?.boolean);
    try t.expectEqual(anyerror.Timeout, e.find("e").?.err);
    try t.expect(e.find("n").? == .none);
    try t.expect(e.find("missing") == null);
}

test "entries are self-contained after the caller's buffers die" {
    var ring = try Ring.init(t.allocator, 4);
    defer ring.deinit();
    {
        // Deliberately scoped: the ring must have copied these bytes,
        // not kept pointers into this frame.
        var msg: [16]u8 = undefined;
        @memcpy(&msg, "scoped message  ");
        var val: [8]u8 = undefined;
        @memcpy(&val, "scopedvl");
        ring.publish(0, .info, msg[0..14], &.{log.str("k", &val)});
        @memset(&msg, 'Z');
        @memset(&val, 'Z');
    }
    var buf: [4]Entry = undefined;
    _ = ring.snapshot(&buf);
    try t.expectEqualStrings("scoped message", buf[0].message());
    try t.expectEqualStrings("scopedvl", buf[0].find("k").?.str);
}

test "snapshot entries stay valid when copied by value" {
    var ring = try Ring.init(t.allocator, 2);
    defer ring.deinit();
    ring.publish(0, .info, "first", &.{log.str("k", "v")});

    var buf: [2]Entry = undefined;
    _ = ring.snapshot(&buf);
    var copy = buf[0];
    // Overwrite both the source slot and the snapshot buffer; the copy
    // addresses its own storage by offset, so it is unaffected.
    ring.publish(0, .info, "second", &.{});
    ring.publish(0, .info, "third", &.{});
    @memset(std.mem.asBytes(&buf[0]), 0xAA);
    try t.expectEqualStrings("first", copy.message());
    try t.expectEqualStrings("v", copy.find("k").?.str);
}

test "oversized text is truncated and flagged" {
    var ring = try Ring.init(t.allocator, 2);
    defer ring.deinit();
    var big: [entry_text_bytes * 2]u8 = undefined;
    @memset(&big, 'q');
    ring.publish(0, .info, &big, &.{log.str("k", "v")});
    var buf: [2]Entry = undefined;
    _ = ring.snapshot(&buf);
    try t.expect(buf[0].truncated);
    try t.expectEqual(@as(usize, entry_text_bytes), buf[0].message().len);
    // Storage is a hard bound, never exceeded.
    try t.expect(buf[0].text_len <= entry_text_bytes);
}

test "attribute count beyond max_attrs is flagged" {
    var ring = try Ring.init(t.allocator, 2);
    defer ring.deinit();
    var attrs: [max_attrs + 5]log.Attr = undefined;
    for (&attrs) |*a| a.* = log.uint("n", 1);
    ring.publish(0, .info, "many", &attrs);
    var buf: [2]Entry = undefined;
    _ = ring.snapshot(&buf);
    try t.expectEqual(@as(usize, max_attrs), buf[0].attrCount());
    try t.expect(buf[0].truncated);
}

test "snapshot into a short buffer returns the newest entries" {
    var ring = try Ring.init(t.allocator, 8);
    defer ring.deinit();
    for (0..8) |i| ring.publish(@intCast(i), .info, "m", &.{});
    var buf: [3]Entry = undefined;
    try t.expectEqual(@as(usize, 3), ring.snapshot(&buf));
    try t.expectEqual(@as(i128, 5), buf[0].time_ns);
    try t.expectEqual(@as(i128, 7), buf[2].time_ns);
}

test "render re-encodes an entry as a valid record" {
    var ring = try Ring.init(t.allocator, 2);
    defer ring.deinit();
    // Hostile value: the round trip through the ring must not lose the
    // escaping guarantee.
    ring.publish(0, .warn, "rendered", &.{
        log.str("nzb", "bad\"name\n\x00\xC3.nzb"),
        log.uint("bytes", 7),
    });
    var entries: [2]Entry = undefined;
    _ = ring.snapshot(&entries);

    var out: [log.line_buf_size]u8 = undefined;
    const json = entries[0].render(&out, .json);
    try t.expect(std.mem.endsWith(u8, json, "\n"));
    const body = json[0 .. json.len - 1];
    try t.expect(try std.json.validate(t.allocator, body));
    const parsed = try std.json.parseFromSlice(std.json.Value, t.allocator, body, .{});
    defer parsed.deinit();
    try t.expectEqualStrings("rendered", parsed.value.object.get("msg").?.string);
    try t.expectEqual(@as(i64, 7), parsed.value.object.get("bytes").?.integer);
    try t.expectEqualStrings("WARN", parsed.value.object.get("level").?.string);

    const text = entries[0].render(&out, .text);
    try t.expect(std.mem.startsWith(u8, text, "time=1970-01-01T00:00:00.000Z level=WARN msg=rendered"));
}

test "concurrent publishers keep every entry well-formed" {
    var ring = try Ring.init(t.allocator, 512);
    defer ring.deinit();
    var rec: Recorder = .{};
    const token = try ring.subscribe(rec.subscriber());
    defer ring.unsubscribe(token);

    const threads = 8;
    const per_thread = 250;
    const Worker = struct {
        fn run(r: *Ring, id: usize) void {
            for (0..per_thread) |i| {
                r.publish(@intCast(i), .info, "concurrent", &.{
                    log.uint("thread", id),
                    log.uint("seq", i),
                });
            }
        }
    };
    var pool: [threads]std.Thread = undefined;
    for (&pool, 0..) |*th, id| th.* = try std.Thread.spawn(.{}, Worker.run, .{ &ring, id });
    for (&pool) |th| th.join();

    // The ring is full and every surviving slot is a complete record —
    // a torn publish would show up as a short message or a bad attr
    // count, since the whole slot is written under `mu`.
    try t.expectEqual(@as(usize, 512), ring.count());
    const buf = try t.allocator.alloc(Entry, 512);
    defer t.allocator.free(buf);
    try t.expectEqual(@as(usize, 512), ring.snapshot(buf));
    for (buf) |*e| {
        try t.expectEqualStrings("concurrent", e.message());
        try t.expectEqual(@as(usize, 2), e.attrCount());
        try t.expect(e.find("thread").?.uint < threads);
        try t.expect(e.find("seq").?.uint < per_thread);
        try t.expect(!e.truncated);
    }
    // Every publish reached the subscriber; the fanout drops nothing
    // when the callback keeps up.
    try t.expectEqual(@as(usize, threads * per_thread), rec.calls);
}
