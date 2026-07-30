//! The generic HTTP-POST subscriber — the third notify adapter, and the
//! only one whose consumer is not a product we can read the docs for.
//!
//! Discord and Slack get a rendered *message*; a webhook subscriber gets
//! the bus envelope itself, verbatim, so an operator can wire hoardarr
//! into whatever they already run. That makes the wire format a public
//! contract in a way the chat payloads are not: a subscriber has already
//! written code against these field names and against the signature
//! scheme, and changing either silently breaks them. Both are therefore
//! Go's, byte for byte.
//!
//! # Body
//!
//! ```json
//! {"ID":"…","Topic":"…","AggregateID":"…","OccurredAt":"…",
//!  "Payload":{…},"Attempts":1}
//! ```
//!
//! Capitalised keys because Go's `encoding/json` had no struct tags on
//! `event.Envelope` and defaulted to the exported field names. Ugly, but
//! it is what every existing subscriber parses.
//!
//! `Payload` is embedded raw — it is already JSON, and re-encoding it as
//! a string would force every subscriber to parse twice.
//!
//! # Signature
//!
//! When the subscription carries a secret:
//!
//!     X-Hoardarr-Signature: sha256=<hex(HMAC-SHA256(secret, body))>
//!
//! The MAC covers the **exact body bytes** and nothing else — no headers,
//! no timestamp prefix, no canonicalisation. A subscriber recomputes it
//! over the raw request body it received. That is why `buildPayload`
//! returns bytes and `sign` takes them: signing anything the transport
//! might re-serialise differently is how these schemes break.
//!
//! # Secrets
//!
//! Two of them here, not one. The URL is a credential (it is why
//! `Target.safeUrl` exists) and so is `Target.secret`. Neither ever
//! reaches the body, and the secret leaves this module only as a
//! digest — see the tests at the bottom, which assert exactly that.

const std = @import("std");
const event = @import("../../domain/event.zig");
const render = @import("render.zig");
const transport = @import("transport.zig");

const Allocator = std.mem.Allocator;
const Writer = std.Io.Writer;
const Target = transport.Target;
const HmacSha256 = std.crypto.auth.hmac.sha2.HmacSha256;

/// Where a subscriber finds the digest. Go's constant, unchanged.
pub const signature_header = "X-Hoardarr-Signature";

/// The bus topic, so a consumer can route without parsing the body.
pub const event_header = "X-Hoardarr-Event";

/// The envelope id, so a consumer can dedupe across the at-least-once
/// redeliveries the outbox is allowed to make.
pub const delivery_header = "X-Hoardarr-Delivery";

pub const user_agent = "hoardarr-webhook/1";

/// 3 attempts, 200 ms then 400 ms. Unlike the chat adapters this one
/// does retry: a generic subscriber is usually something the operator
/// runs themselves, and a restart mid-delivery is the common failure.
pub const policy: transport.RetryPolicy = .webhook;

// ---------------------------------------------------------------------
// Signing
// ---------------------------------------------------------------------

/// `sha256=` plus 64 hex characters.
pub const SignatureBuf = [7 + 2 * HmacSha256.mac_length]u8;

/// Writes `sha256=<lowercase hex>` into caller storage and returns a
/// slice of it. Nothing is allocated, so this can run on a path that has
/// no arena.
///
/// Lowercase hex rather than base64 because that is what Go's
/// `hex.EncodeToString` produced, and a subscriber comparing strings
/// rather than bytes would see every signature fail if we changed it.
pub fn sign(buf: *SignatureBuf, secret: []const u8, body: []const u8) []const u8 {
    var mac: [HmacSha256.mac_length]u8 = undefined;
    HmacSha256.create(&mac, body, secret);

    @memcpy(buf[0..7], "sha256=");
    const hex = "0123456789abcdef";
    for (mac, 0..) |b, i| {
        buf[7 + 2 * i] = hex[b >> 4];
        buf[7 + 2 * i + 1] = hex[b & 0x0F];
    }
    return buf[0..];
}

// ---------------------------------------------------------------------
// Payload
// ---------------------------------------------------------------------

/// Serialises `env` as the webhook body. Allocated from `arena`.
///
/// Pure: same envelope in, same bytes out, no clock and no network. The
/// signature is computed over the result, so a test can assert the exact
/// bytes a subscriber will verify.
pub fn buildPayload(
    arena: Allocator,
    env: event.Envelope,
) Allocator.Error![]u8 {
    var out: Writer.Allocating = .init(arena);
    errdefer out.deinit();
    writeBody(&out.writer, arena, env) catch |e| switch (e) {
        // Arena-backed writer: the only failure mode is OOM.
        error.WriteFailed => return error.OutOfMemory,
        error.OutOfMemory => return error.OutOfMemory,
    };
    return out.toOwnedSlice();
}

/// Body, URL and headers for one delivery. Every slice borrows from
/// `arena`, including the two per-event header values and the signature,
/// which is why they are materialised here rather than in `send`.
pub fn buildRequest(
    arena: Allocator,
    target: Target,
    env: event.Envelope,
) Allocator.Error!transport.Request {
    const body = try buildPayload(arena, env);

    var id_buf: event.Uuid.TextBuf = undefined;
    const delivery = try arena.dupe(u8, env.id.writeText(&id_buf));

    // A topic with a CRLF in it would be a header-injection vector. The
    // bus only ever produces literals, but the client rejects such a
    // header outright rather than trusting that, so nothing here has to.
    var headers: std.ArrayList(transport.Header) = .empty;
    try headers.ensureTotalCapacityPrecise(arena, 5);
    headers.appendSliceAssumeCapacity(&.{
        .{ .name = "Content-Type", .value = "application/json" },
        .{ .name = "User-Agent", .value = user_agent },
        .{ .name = event_header, .value = env.topic },
        .{ .name = delivery_header, .value = delivery },
    });

    // No secret means no header at all, rather than an empty one: an
    // empty signature that a subscriber compares against its own
    // computed digest would fail confusingly instead of being absent.
    if (target.secret.len != 0) {
        const sig = try arena.create(SignatureBuf);
        headers.appendAssumeCapacity(.{
            .name = signature_header,
            .value = sign(sig, target.secret, body),
        });
    }

    return .{
        .url = target.url,
        .headers = try headers.toOwnedSlice(arena),
        .body = body,
    };
}

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
    env: event.Envelope,
) (Allocator.Error || Writer.Error)!void {
    var id_buf: event.Uuid.TextBuf = undefined;

    try w.writeAll("{");
    try render.writeJsonField(w, "ID", env.id.writeText(&id_buf));
    try w.writeAll(",");
    // Topic and aggregate id are escaped even though the bus only emits
    // literals for them: `render.writeJsonString` is total, so routing
    // them through it costs nothing and removes the question.
    try render.writeJsonField(w, "Topic", env.topic);
    try w.writeAll(",");
    try render.writeJsonField(w, "AggregateID", env.aggregate_id);
    try w.writeAll(",");
    try render.writeJsonKey(w, "OccurredAt");
    try writeTimestamp(w, env.occurred_at);
    try w.writeAll(",");
    try render.writeJsonKey(w, "Payload");
    try writePayload(w, arena, env.payload);
    try w.writeAll(",");
    try render.writeJsonKey(w, "Attempts");
    try w.print("{d}", .{env.attempts});
    try w.writeAll("}");
}

/// Go's `time.Time` marshalled as RFC3339 with the fractional part
/// trimmed of trailing zeros — `…:20Z` when the millisecond field is
/// zero, `…:20.5Z` for 500 ms.
///
/// Second-precision formatting is `render.rfc3339`'s; only the fraction
/// is added here, because the chat adapters deliberately do *not* show
/// one and a webhook subscriber comparing against Go's output would
/// notice its absence.
fn writeTimestamp(w: *Writer, ms: event.Timestamp) Writer.Error!void {
    var buf: render.Rfc3339Buf = undefined;
    const text = render.rfc3339(&buf, ms);
    try w.writeByte('"');
    try w.writeAll(text[0 .. text.len - 1]); // everything but the 'Z'

    const frac: u64 = @intCast(@mod(ms, 1000));
    if (frac != 0) {
        var fb: [4]u8 = undefined;
        const d = std.fmt.bufPrint(&fb, ".{d:0>3}", .{frac}) catch unreachable;
        try w.writeAll(std.mem.trimEnd(u8, d, "0"));
    }
    try w.writeAll("Z\"");
}

/// The payload is already JSON, so it goes in raw.
///
/// Go marshalled a `json.RawMessage`, which validates and errors the
/// whole delivery out if the bytes are not JSON. We do not fail: a
/// malformed payload is emitted as a JSON *string* instead, so the body
/// a subscriber receives is always parseable and always says what we
/// actually had. Dropping the delivery — Go's behaviour — loses the one
/// notification that would have told the operator something is wrong.
fn writePayload(w: *Writer, arena: Allocator, payload: []const u8) (Allocator.Error || Writer.Error)!void {
    // An absent payload is `null`, matching a nil `json.RawMessage`.
    if (payload.len == 0) {
        try w.writeAll("null");
        return;
    }
    if (try std.json.validate(arena, payload)) {
        try w.writeAll(payload);
        return;
    }
    try render.writeJsonString(w, payload);
}

// ---------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------

const testing = std.testing;

const enriched_payload =
    \\{"event":{"job_id":42},"job":{"name":"Foo.Bar.S01E02-RLSGRP","category":"tv"}}
;

fn testEnv(topic: []const u8, payload: []const u8) event.Envelope {
    return .{
        .id = event.Uuid.parse("018b1f2c-3d4e-7f80-9a1b-2c3d4e5f6071") catch unreachable,
        .topic = topic,
        .aggregate_id = "42",
        .occurred_at = 1_700_000_000_000,
        .payload = payload,
    };
}

test "the body is the envelope, with Go's field names" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    const body = try buildPayload(arena.allocator(), testEnv("deliver.complete", enriched_payload));
    const p = try std.json.parseFromSlice(std.json.Value, testing.allocator, body, .{});
    defer p.deinit();
    const o = p.value.object;

    try testing.expectEqualStrings("018b1f2c-3d4e-7f80-9a1b-2c3d4e5f6071", o.get("ID").?.string);
    try testing.expectEqualStrings("deliver.complete", o.get("Topic").?.string);
    try testing.expectEqualStrings("42", o.get("AggregateID").?.string);
    try testing.expectEqualStrings("2023-11-14T22:13:20Z", o.get("OccurredAt").?.string);
    try testing.expectEqual(@as(i64, 1), o.get("Attempts").?.integer);

    // The payload is nested JSON, not a string: a subscriber reads
    // `Payload.job.name` without a second parse.
    const job = o.get("Payload").?.object.get("job").?.object;
    try testing.expectEqualStrings("Foo.Bar.S01E02-RLSGRP", job.get("name").?.string);
}

test "a sub-second timestamp keeps its milliseconds, trailing zeros trimmed" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const cases = [_]struct { i64, []const u8 }{
        .{ 1_700_000_000_000, "2023-11-14T22:13:20Z" },
        .{ 1_700_000_000_123, "2023-11-14T22:13:20.123Z" },
        .{ 1_700_000_000_500, "2023-11-14T22:13:20.5Z" },
        .{ 1_700_000_000_050, "2023-11-14T22:13:20.05Z" },
    };
    for (cases) |c| {
        var env = testEnv("x.y", "{}");
        env.occurred_at = c[0];
        const body = try buildPayload(a, env);
        const p = try std.json.parseFromSlice(std.json.Value, testing.allocator, body, .{});
        defer p.deinit();
        try testing.expectEqualStrings(c[1], p.value.object.get("OccurredAt").?.string);
    }
}

test "an empty payload is null, a malformed one is a string" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    {
        const body = try buildPayload(a, testEnv("x.y", ""));
        const p = try std.json.parseFromSlice(std.json.Value, testing.allocator, body, .{});
        defer p.deinit();
        try testing.expect(p.value.object.get("Payload").? == .null);
    }
    {
        // Go would have failed the whole delivery here. We still send,
        // and the body still parses.
        const body = try buildPayload(a, testEnv("x.y", "{not json"));
        const p = try std.json.parseFromSlice(std.json.Value, testing.allocator, body, .{});
        defer p.deinit();
        try testing.expectEqualStrings("{not json", p.value.object.get("Payload").?.string);
    }
}

test "HMAC-SHA256 over the exact body, hex, sha256= prefixed" {
    // Known answer: RFC 4231 test case 2, key "Jefe", data
    // "what do ya want for nothing?".
    var buf: SignatureBuf = undefined;
    try testing.expectEqualStrings(
        "sha256=5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843",
        sign(&buf, "Jefe", "what do ya want for nothing?"),
    );
}

test "signing an empty secret still produces a digest, but no header" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    const req = try buildRequest(
        arena.allocator(),
        .{ .name = "w", .url = "https://example.test/hook" },
        testEnv("deliver.complete", enriched_payload),
    );
    for (req.headers) |h| {
        try testing.expect(!std.mem.eql(u8, h.name, signature_header));
    }
    try testing.expectEqual(@as(usize, 4), req.headers.len);
}

test "the signature verifies against the body a subscriber receives" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    const secret = "topsecret-shared-key";
    const req = try buildRequest(
        arena.allocator(),
        .{ .name = "w", .url = "https://example.test/hook", .secret = secret },
        testEnv("deliver.complete", enriched_payload),
    );

    var seen: ?[]const u8 = null;
    for (req.headers) |h| {
        if (std.mem.eql(u8, h.name, signature_header)) seen = h.value;
    }
    const sig = seen orelse return error.NoSignatureHeader;

    // Recompute the way a subscriber would: over the raw request body.
    var expect: SignatureBuf = undefined;
    try testing.expectEqualStrings(sign(&expect, secret, req.body), sig);
    try testing.expect(std.mem.startsWith(u8, sig, "sha256="));
    try testing.expectEqual(@as(usize, 7 + 64), sig.len);
    // Any other body must not verify — the MAC is over the payload, not
    // over some canonical form of it.
    var other: SignatureBuf = undefined;
    try testing.expect(!std.mem.eql(u8, sign(&other, secret, "{}"), sig));
}

test "routing headers carry the topic and the envelope id" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    var fake: transport.FakeTransport = .{ .script = &.{.{ .status = 204 }} };
    const url = "https://example.test/hooks/abc";
    const d = try send(
        arena.allocator(),
        fake.transport(),
        .{ .name = "w", .url = url, .secret = "k" },
        testEnv("verify.failed", enriched_payload),
    );
    try testing.expect(d.ok());

    const req = fake.last.?;
    try testing.expectEqualStrings("POST", req.method);
    try testing.expectEqualStrings(url, req.url);
    try testing.expectEqualStrings("application/json", req.headers[0].value);
    try testing.expectEqualStrings(user_agent, req.headers[1].value);
    try testing.expectEqualStrings(event_header, req.headers[2].name);
    try testing.expectEqualStrings("verify.failed", req.headers[2].value);
    try testing.expectEqualStrings(delivery_header, req.headers[3].name);
    try testing.expectEqualStrings("018b1f2c-3d4e-7f80-9a1b-2c3d4e5f6071", req.headers[3].value);
}

test "the webhook retry policy is the one in transport, not a local copy" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    var fake: transport.FakeTransport = .{ .script = &.{.{ .status = 503 }} };
    const d = try send(
        arena.allocator(),
        fake.transport(),
        .{ .name = "w", .url = "https://example.test/hook" },
        testEnv("deliver.complete", enriched_payload),
    );
    try testing.expectError(error.Unavailable, d.toError());
    try testing.expectEqual(@as(usize, 3), fake.calls);
    try testing.expectEqualSlices(u64, &.{
        200 * std.time.ns_per_ms,
        400 * std.time.ns_per_ms,
    }, fake.sleeps());
}

test "a 4xx stops immediately — a misconfigured subscriber will not improve" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    var fake: transport.FakeTransport = .{ .script = &.{.{ .status = 404 }} };
    const d = try send(
        arena.allocator(),
        fake.transport(),
        .{ .name = "w", .url = "https://example.test/hook" },
        testEnv("deliver.complete", enriched_payload),
    );
    try testing.expectError(error.Rejected, d.toError());
    try testing.expectEqual(@as(usize, 1), fake.calls);
}

test "hostile topics and ids still produce parseable JSON" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    // Release names reach the envelope from an NZB off the internet, so
    // every string field gets the escaper, and every result gets a real
    // parse rather than a substring check.
    const hostile = [_][]const u8{
        "a\"b\\c",
        "\",\"Payload\":{\"pwned\":true},\"x\":\"",
        "nul\x00and\x1fcontrol",
        "\xffinvalid utf8\xc3",
        "line\nbreak\ttab",
    };
    for (hostile) |h| {
        var env = testEnv(h, enriched_payload);
        env.aggregate_id = h;
        const body = try buildPayload(a, env);
        const p = std.json.parseFromSlice(std.json.Value, testing.allocator, body, .{}) catch |err| {
            std.debug.print("unparseable body for {s}:\n{s}\n", .{ h, body });
            return err;
        };
        defer p.deinit();
        try testing.expect(std.unicode.utf8ValidateSlice(body));
        // The injection attempt did not add or replace a key.
        try testing.expectEqual(@as(usize, 6), p.value.object.count());
        try testing.expect(p.value.object.get("Payload").? == .object);
    }
}

test "an adversarial release name inside the payload survives untouched" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    // Already-escaped JSON: it is embedded raw, so the escaping the
    // producer did must be exactly what the subscriber sees.
    const payload =
        \\{"job":{"name":"x\",\"admin\":true,\"y\":\"z"}}
    ;
    const body = try buildPayload(a, testEnv("deliver.complete", payload));
    const p = try std.json.parseFromSlice(std.json.Value, testing.allocator, body, .{});
    defer p.deinit();
    const job = p.value.object.get("Payload").?.object.get("job").?.object;
    try testing.expectEqual(@as(usize, 1), job.count());
    try testing.expectEqualStrings("x\",\"admin\":true,\"y\":\"z", job.get("name").?.string);
}

test "neither the secret nor the URL's token reaches the body" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    const secret = "s3cr3t-hmac-key";
    const token = "aVerySecretUrlToken";
    var fake: transport.FakeTransport = .{ .script = &.{.{ .status = 200 }} };
    _ = try send(
        arena.allocator(),
        fake.transport(),
        .{
            .name = "w",
            .url = "https://example.test/hooks/" ++ token,
            .secret = secret,
        },
        testEnv("deliver.complete", enriched_payload),
    );

    const req = fake.last.?;
    try testing.expect(std.mem.indexOf(u8, req.body, secret) == null);
    try testing.expect(std.mem.indexOf(u8, req.body, token) == null);
    // The secret leaves only as a digest, and the URL only in the URL.
    for (req.headers) |h| {
        try testing.expect(std.mem.indexOf(u8, h.value, secret) == null);
        try testing.expect(std.mem.indexOf(u8, h.value, token) == null);
    }
    // And the one thing a log line may carry drops both.
    const safe = Target.safeUrl(req.url);
    try testing.expectEqualStrings("https://example.test", safe);
    try testing.expect(std.mem.indexOf(u8, safe, token) == null);
}
