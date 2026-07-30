//! Discord webhook adapter.
//!
//! Spec: https://discord.com/developers/docs/resources/webhook
//!
//! One embed per event:
//!
//!   * `title`       — the cleaned release name, or the verb when there
//!                     is no release to name.
//!   * `description` — bold verb, then the *raw* release name in a code
//!                     block so the operator can copy it verbatim
//!                     (the title's dot-stripping is for readability,
//!                     not for pasting into a search box).
//!   * `color`       — outcome-derived: green / red / amber / blue.
//!   * `fields`      — a two-column inline grid of Source / Category /
//!                     Size / Files / Quality / State, plus a
//!                     full-width Error row for failures.
//!   * `footer`      — "hoardarr • <subscription name>".
//!
//! **No HMAC.** Discord webhooks verify nothing: possession of the URL
//! is the authorisation. `Target.secret` is therefore ignored here
//! rather than smuggled into a query parameter — the Go version
//! appended it as `wait=<secret>`, which Discord discards and which put
//! the operator's shared secret into Discord's request logs for no
//! benefit at all.
//!
//! `buildPayload` is pure: envelope in, bytes out, no clock and no
//! socket. `send` is the only part that touches the network, and it
//! does so through the injected `Transport`.

const std = @import("std");
const event = @import("../../domain/event.zig");
const render = @import("render.zig");
const transport = @import("transport.zig");

const Allocator = std.mem.Allocator;
const Writer = std.Io.Writer;
const Target = transport.Target;

/// Discord's per-field value limit is 1024 characters; cap below it so a
/// runaway error message cannot get the whole payload rejected.
const field_value_max = 1000;

pub const user_agent = "hoardarr-discord/1";

pub const headers: []const transport.Header = &.{
    .{ .name = "Content-Type", .value = "application/json" },
    .{ .name = "User-Agent", .value = user_agent },
};

/// One attempt only. Discord's rate limiter reacts badly to a retry
/// burst; see `transport.RetryPolicy`.
pub const policy: transport.RetryPolicy = .chat;

/// Serialises `env` as a Discord webhook body. Allocated from `arena`.
///
/// Pure by construction, which is what makes the payload tests network-
/// free: the only inputs are the target's name and the envelope.
pub fn buildPayload(
    arena: Allocator,
    target: Target,
    env: event.Envelope,
) Allocator.Error![]u8 {
    var out: Writer.Allocating = .init(arena);
    errdefer out.deinit();
    writeBody(&out.writer, arena, target, env) catch |e| switch (e) {
        // The writer is arena-backed, so the only way it fails is OOM.
        error.WriteFailed => return error.OutOfMemory,
        error.OutOfMemory => return error.OutOfMemory,
    };
    return out.toOwnedSlice();
}

/// The full request, ready to hand to a `Transport`.
pub fn buildRequest(
    arena: Allocator,
    target: Target,
    env: event.Envelope,
) Allocator.Error!transport.Request {
    return .{
        .url = target.url,
        .headers = headers,
        .body = try buildPayload(arena, target, env),
    };
}

/// Builds and delivers. Returns what happened rather than an error so
/// the caller can record the status and attempt count; call
/// `Delivery.toError` if you would rather have an error union.
pub fn send(
    arena: Allocator,
    t: transport.Transport,
    target: Target,
    env: event.Envelope,
) Allocator.Error!transport.Delivery {
    const req = try buildRequest(arena, target, env);
    return transport.deliver(t, policy, req);
}

fn writeBody(
    w: *Writer,
    arena: Allocator,
    target: Target,
    env: event.Envelope,
) (Allocator.Error || Writer.Error)!void {
    const v = try render.View.from(arena, env);

    const title = if (v.clean_title.len != 0) v.clean_title else v.verb;

    try w.writeAll("{");
    try render.writeJsonField(w, "username", "hoardarr");
    try w.writeAll(",");
    try render.writeJsonKey(w, "embeds");
    try w.writeAll("[{");

    try render.writeJsonField(w, "title", title);

    // Description: bold verb plus the raw release in a fenced block.
    // Built directly into the output string so there is no intermediate
    // allocation for what is usually two short lines.
    try w.writeAll(",");
    try render.writeJsonKey(w, "description");
    try writeDescription(w, v, title);

    try w.writeAll(",");
    try render.writeJsonKey(w, "color");
    try w.print("{d}", .{v.outcome.color()});

    var ts: render.Rfc3339Buf = undefined;
    try w.writeAll(",");
    try render.writeJsonField(w, "timestamp", render.rfc3339(&ts, v.occurred_at));

    try w.writeAll(",");
    try render.writeJsonKey(w, "fields");
    try writeFields(w, v);

    try w.writeAll(",");
    try render.writeJsonKey(w, "footer");
    try w.writeAll("{");
    try render.writeJsonKey(w, "text");
    try w.writeAll("\"hoardarr • ");
    // The name is operator-supplied, so it still goes through the
    // escaper — the surrounding quotes are just written by hand here.
    try render.writeJsonInner(w, target.name);
    try w.writeAll("\"}");

    try w.writeAll("}]}");
}

/// The `description` field: bold verb, then the raw release in a fenced
/// code block. Assembled as one JSON string, so the markdown newlines
/// are the two-byte escape `\n` and the release name goes through the
/// escaper without its own quotes.
fn writeDescription(
    w: *Writer,
    v: render.View,
    title: []const u8,
) Writer.Error!void {
    try w.writeByte('"');
    try w.writeAll("**");
    try render.writeJsonInner(w, v.verb);
    try w.writeAll("**");
    if (v.release.len != 0 and !std.mem.eql(u8, v.release, title)) {
        try w.writeAll("\\n```\\n");
        try render.writeJsonInner(w, v.release);
        try w.writeAll("\\n```");
    }
    try w.writeByte('"');
}

/// Two-column inline grid. Empty values are dropped so the grid stays
/// visually balanced; the Error row goes full-width because a stack
/// trace in a 200-pixel column is unreadable.
fn writeFields(w: *Writer, v: render.View) Writer.Error!void {
    var count_buf: [24]u8 = undefined;
    const files: []const u8 = if (v.file_count > 0)
        std.fmt.bufPrint(&count_buf, "{d}", .{v.file_count}) catch unreachable
    else
        "";

    const pairs = [_]struct { []const u8, []const u8 }{
        .{ "Source", v.source },
        .{ "Category", v.category },
        .{ "Size", v.size_human },
        .{ "Files", files },
        .{ "Quality", v.quality },
        .{ "State", v.state },
    };

    try w.writeAll("[");
    var first = true;
    for (pairs) |p| {
        if (p[1].len == 0) continue;
        if (!first) try w.writeAll(",");
        first = false;
        try w.writeAll("{");
        try render.writeJsonField(w, "name", p[0]);
        try w.writeAll(",");
        try render.writeJsonField(w, "value", p[1]);
        try w.writeAll(",");
        try render.writeJsonKey(w, "inline");
        try w.writeAll("true}");
    }
    if (v.error_msg.len != 0) {
        if (!first) try w.writeAll(",");
        first = false;
        try w.writeAll("{");
        try render.writeJsonField(w, "name", "Error");
        try w.writeAll(",");
        try render.writeJsonField(w, "value", render.truncate(v.error_msg, field_value_max));
        try w.writeAll(",");
        try render.writeJsonKey(w, "inline");
        try w.writeAll("false}");
    }
    // A payload we could not project still produces a notification —
    // one that says so, rather than a message with silently missing
    // fields that the operator reads as "nothing was downloaded".
    if (v.diag) |d| {
        if (!first) try w.writeAll(",");
        try w.writeAll("{");
        try render.writeJsonField(w, "name", "Payload");
        try w.writeAll(",");
        try render.writeJsonField(w, "value", d.message());
        try w.writeAll(",");
        try render.writeJsonKey(w, "inline");
        try w.writeAll("false}");
    }
    try w.writeAll("]");
}

// ---------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------

const testing = std.testing;

/// Enriched envelope, exactly the shape `notify.Service.enrich`
/// produces after looking the job up.
const enriched_payload =
    \\{
    \\  "event": {"job_id": 42},
    \\  "job": {
    \\    "id": 42,
    \\    "name": "Foo.Bar.S01E02.1080p.WEB-DL.H264-RLSGRP",
    \\    "category": "tv",
    \\    "state": "completed",
    \\    "source": "Sonarr/4.0.0",
    \\    "total_bytes": 5368709120,
    \\    "file_count": 4
    \\  }
    \\}
;

fn testEnv(topic: []const u8, payload: []const u8) event.Envelope {
    return .{
        .id = .nil,
        .topic = topic,
        .aggregate_id = "42",
        .occurred_at = 1_700_000_000_000,
        .payload = payload,
    };
}

/// Parses a built payload and hands back the single embed object. Every
/// assertion goes through a real JSON parse, so a payload that only
/// *looks* right fails the test.
const Embed = struct {
    parsed: std.json.Parsed(std.json.Value),
    obj: std.json.ObjectMap,

    fn init(body: []const u8) !Embed {
        const parsed = try std.json.parseFromSlice(
            std.json.Value,
            testing.allocator,
            body,
            .{},
        );
        errdefer parsed.deinit();
        const embeds = parsed.value.object.get("embeds").?.array;
        try testing.expectEqual(@as(usize, 1), embeds.items.len);
        return .{ .parsed = parsed, .obj = embeds.items[0].object };
    }

    fn deinit(self: Embed) void {
        self.parsed.deinit();
    }

    fn str(self: Embed, key: []const u8) []const u8 {
        return self.obj.get(key).?.string;
    }

    /// name → value over the field array.
    fn field(self: Embed, name: []const u8) ?std.json.ObjectMap {
        for (self.obj.get("fields").?.array.items) |f| {
            if (std.mem.eql(u8, f.object.get("name").?.string, name)) return f.object;
        }
        return null;
    }
};

test "rich embed carries the cleaned title, bold verb and raw release" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    const target: Target = .{ .name = "discord-test", .url = "https://example.test/hook" };
    const body = try buildPayload(
        arena.allocator(),
        target,
        testEnv("deliver.complete", enriched_payload),
    );

    const e = try Embed.init(body);
    defer e.deinit();

    try testing.expect(std.mem.indexOf(u8, e.str("title"), "Foo Bar S01E02") != null);

    const desc = e.str("description");
    try testing.expect(std.mem.indexOf(u8, desc, "**Delivered**") != null);
    try testing.expect(std.mem.indexOf(
        u8,
        desc,
        "Foo.Bar.S01E02.1080p.WEB-DL.H264-RLSGRP",
    ) != null);
    // The code fence survives as real newlines after JSON decoding.
    try testing.expect(std.mem.indexOf(u8, desc, "\n```\n") != null);

    try testing.expectEqual(
        @as(i64, render.Outcome.ok.color()),
        e.obj.get("color").?.integer,
    );
    try testing.expectEqualStrings("2023-11-14T22:13:20Z", e.str("timestamp"));

    for ([_][]const u8{ "Source", "Category", "Size", "Files", "Quality", "State" }) |name| {
        try testing.expect(e.field(name) != null);
    }
    // The Go version once emitted debug fields here; they must stay gone.
    for ([_][]const u8{ "Topic", "Aggregate" }) |name| {
        try testing.expect(e.field(name) == null);
    }
    try testing.expectEqualStrings("Sonarr", e.field("Source").?.get("value").?.string);
    try testing.expectEqualStrings("5.00 GB", e.field("Size").?.get("value").?.string);
    try testing.expectEqualStrings("4", e.field("Files").?.get("value").?.string);
    try testing.expect(e.field("Source").?.get("inline").?.bool);

    const footer = e.obj.get("footer").?.object;
    try testing.expectEqualStrings("hoardarr • discord-test", footer.get("text").?.string);
    try testing.expectEqualStrings("hoardarr", e.parsed.value.object.get("username").?.string);
}

test "an event with no release falls back to the verb as title" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const body = try buildPayload(
        arena.allocator(),
        .{ .name = "d", .url = "https://example.test/hook" },
        testEnv("verify.started", "{}"),
    );
    const e = try Embed.init(body);
    defer e.deinit();
    try testing.expectEqualStrings("Verifying", e.str("title"));
    // No release means no code block, just the bold verb.
    try testing.expectEqualStrings("**Verifying**", e.str("description"));
    try testing.expectEqual(
        @as(i64, render.Outcome.info.color()),
        e.obj.get("color").?.integer,
    );
}

test "a failing status is reported, not swallowed" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    var fake: transport.FakeTransport = .{ .script = &.{.{ .status = 500 }} };

    const d = try send(
        arena.allocator(),
        fake.transport(),
        .{ .name = "d", .url = "https://example.test/hook" },
        testEnv("deliver.complete", enriched_payload),
    );
    try testing.expect(!d.ok());
    try testing.expectError(error.Unavailable, d.toError());
    // Chat policy: exactly one attempt against a 500.
    try testing.expectEqual(@as(usize, 1), fake.calls);
}

test "send posts to the target URL with the JSON content type" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    var fake: transport.FakeTransport = .{ .script = &.{.{ .status = 204 }} };

    const url = "https://discord.com/api/webhooks/123/tok";
    const d = try send(
        arena.allocator(),
        fake.transport(),
        .{ .name = "d", .url = url },
        testEnv("deliver.complete", enriched_payload),
    );
    try testing.expect(d.ok());

    const req = fake.last.?;
    try testing.expectEqualStrings("POST", req.method);
    try testing.expectEqualStrings(url, req.url);
    var seen_ct = false;
    var seen_ua = false;
    for (req.headers) |h| {
        if (std.mem.eql(u8, h.name, "Content-Type")) {
            try testing.expectEqualStrings("application/json", h.value);
            seen_ct = true;
        }
        if (std.mem.eql(u8, h.name, "User-Agent")) {
            try testing.expectEqualStrings("hoardarr-discord/1", h.value);
            seen_ua = true;
        }
    }
    try testing.expect(seen_ct and seen_ua);
}

test "a failure event gets a full-width Error field and the red colour" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const body = try buildPayload(
        arena.allocator(),
        .{ .name = "d", .url = "https://example.test/hook" },
        testEnv(
            "deliver.failed",
            \\{"event":{"err":"target filesystem is read-only"},"job":{"name":"X-G","state":"failed"}}
            ,
        ),
    );
    const e = try Embed.init(body);
    defer e.deinit();

    try testing.expectEqual(
        @as(i64, render.Outcome.fail.color()),
        e.obj.get("color").?.integer,
    );
    const f = e.field("Error").?;
    try testing.expect(std.mem.indexOf(u8, f.get("value").?.string, "read-only") != null);
    try testing.expect(!f.get("inline").?.bool);
}

test "an oversized error message is capped below Discord's field limit" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const long = "e" ** 4000;
    const payload = try std.fmt.allocPrint(
        arena.allocator(),
        "{{\"event\":{{\"err\":\"{s}\"}},\"job\":{{\"name\":\"X-G\"}}}}",
        .{long},
    );
    const body = try buildPayload(
        arena.allocator(),
        .{ .name = "d", .url = "https://example.test/hook" },
        testEnv("deliver.failed", payload),
    );
    const e = try Embed.init(body);
    defer e.deinit();
    try testing.expectEqual(
        @as(usize, field_value_max - 1),
        e.field("Error").?.get("value").?.string.len,
    );
}

test "a malformed payload produces a diagnostic, not a broken message" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const body = try buildPayload(
        arena.allocator(),
        .{ .name = "d", .url = "https://example.test/hook" },
        testEnv("deliver.complete", "{not json"),
    );
    const e = try Embed.init(body);
    defer e.deinit();
    // Still a complete, well-formed embed with the topic-derived parts.
    try testing.expectEqualStrings("Delivered", e.str("title"));
    const f = e.field("Payload").?;
    try testing.expectEqualStrings(
        render.Diagnostic.invalid_json.message(),
        f.get("value").?.string,
    );
    try testing.expect(!f.get("inline").?.bool);
}

test "hostile release names still produce parseable JSON" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    // Quotes, backslashes, a brace, a NUL, a bare CR, an unpaired UTF-16
    // surrogate encoded as CESU-8, a lone continuation byte, and a
    // truncated 4-byte sequence — all of which turn up in real NZBs.
    const names = [_][]const u8{
        "a\\\"b\\\\c",
        "}{\\\"embeds\\\":[]",
        "x\\u0000y",
        "\\u001fctl",
        "\\ud800lone",
    };
    for (names) |n| {
        const payload = try std.fmt.allocPrint(
            arena.allocator(),
            "{{\"job\":{{\"name\":\"{s}\",\"category\":\"tv\"}}}}",
            .{n},
        );
        const body = try buildPayload(
            arena.allocator(),
            .{ .name = "he\"re\\", .url = "https://example.test/hook" },
            testEnv("deliver.complete", payload),
        );
        const parsed = std.json.parseFromSlice(
            std.json.Value,
            testing.allocator,
            body,
            .{},
        ) catch |err| {
            std.debug.print("unparseable payload for name {s}:\n{s}\n", .{ n, body });
            return err;
        };
        defer parsed.deinit();
        try testing.expect(std.unicode.utf8ValidateSlice(body));
    }
}

test "raw invalid UTF-8 in a release name cannot break the payload" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    // Bypass JSON decoding of the payload: construct the View's input so
    // the release name reaches the escaper as raw bytes. `notify.test`
    // takes the release from the top-level "name", so a hand-rolled
    // payload with escaped high bytes lands there.
    var raw: [300]u8 = undefined;
    for (&raw, 0..) |*b, i| b.* = @intCast(0x80 + (i % 0x80));

    var payload: Writer.Allocating = .init(arena.allocator());
    try payload.writer.writeAll("{\"name\":\"");
    for (raw) |b| try payload.writer.print("\\u{x:0>4}", .{b});
    try payload.writer.writeAll("\"}");

    const body = try buildPayload(
        arena.allocator(),
        .{ .name = "d", .url = "https://example.test/hook" },
        testEnv("notify.test", payload.written()),
    );
    const parsed = try std.json.parseFromSlice(
        std.json.Value,
        testing.allocator,
        body,
        .{},
    );
    defer parsed.deinit();
    try testing.expect(std.unicode.utf8ValidateSlice(body));
}

test "the subscription secret never reaches the payload or the URL" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const secret = "s3cr3t-hmac-key";
    var fake: transport.FakeTransport = .{ .script = &.{.{ .status = 204 }} };
    const target: Target = .{
        .name = "d",
        .url = "https://discord.com/api/webhooks/123/tok",
        .secret = secret,
    };
    _ = try send(
        arena.allocator(),
        fake.transport(),
        target,
        testEnv("deliver.complete", enriched_payload),
    );
    const req = fake.last.?;
    // Discord verifies nothing, so the secret must not be smuggled into
    // the body, the URL or a header where Discord would log it.
    try testing.expect(std.mem.indexOf(u8, req.body, secret) == null);
    try testing.expect(std.mem.indexOf(u8, req.url, secret) == null);
    for (req.headers) |h| {
        try testing.expect(std.mem.indexOf(u8, h.value, secret) == null);
    }
}
