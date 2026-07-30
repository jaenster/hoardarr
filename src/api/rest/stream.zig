//! Wiring an open HTTP connection into an SSE hub.
//!
//! `net/http` already owns the hard part: `beginEventStream` sends the
//! head with the right framing and the no-buffering headers,
//! `ctx.detach` hands the connection to the application with no timeouts
//! armed and inbound bytes discarded, and the close hook fires exactly
//! once however the connection dies. What is left is a three-function
//! adapter from that to `sse.Sink`, plus one small heap object per
//! stream to hold the pair together.
//!
//! ## Lifetime
//!
//! One `Attachment` per open stream, allocated at subscribe and freed in
//! the close hook — the single point where "this connection is gone" is
//! known, whatever caused it:
//!
//!   * client closed the tab      → socket readable-zero → `Conn.close`
//!   * hub dropped a slow reader  → `closeFn` → `finishStream` → close
//!   * server shutdown            → `Conn.close`
//!
//! All three end in `on_close`, which unsubscribes (idempotent, and a
//! no-op when the hub already released the slot) and destroys the
//! attachment. There is no path that frees it twice and none that leaks
//! it.

const std = @import("std");
const http = @import("../../net/http/server.zig");
const response = @import("../../net/http/response.zig");
const sse = @import("../sse.zig");
const Api = @import("api.zig").Api;

const Allocator = std.mem.Allocator;

pub const Error = error{ TooManySubscribers, OutOfMemory } || response.Error;

pub const Attachment = struct {
    gpa: Allocator,
    hub: *sse.Hub,
    conn: *http.Conn,
    token: sse.Hub.Token = .{ .slot = 0, .gen = 0 },

    fn sink(self: *Attachment) sse.Sink {
        return .{ .ctx = self, .writeFn = write, .pendingFn = pending, .closeFn = close };
    }

    fn write(ctx: ?*anyopaque, bytes: []const u8) sse.WriteError!void {
        const self: *Attachment = @ptrCast(@alignCast(ctx.?));
        self.conn.resp().write(bytes) catch |err| return switch (err) {
            error.QueueFull => error.QueueFull,
            error.StreamClosed, error.ResponseFinished => error.StreamClosed,
            error.OutOfMemory => error.OutOfMemory,
            else => error.IoFailed,
        };
    }

    fn pending(ctx: ?*anyopaque) usize {
        const self: *Attachment = @ptrCast(@alignCast(ctx.?));
        return self.conn.resp().pending();
    }

    fn close(ctx: ?*anyopaque) void {
        const self: *Attachment = @ptrCast(@alignCast(ctx.?));
        // Ends the chunked body and closes the connection, which comes
        // back round to `onClose` below.
        self.conn.finishStream();
    }

    fn onClose(ctx: ?*anyopaque) void {
        const self: *Attachment = @ptrCast(@alignCast(ctx.?));
        self.hub.unsubscribe(self.token);
        self.gpa.destroy(self);
    }
};

/// Put the response into event-stream framing, register with `hub`, and
/// hand the connection over. After this returns the handler is done: the
/// hub owns the stream. The token comes back so the caller can send the
/// greeting this one stream expects.
pub fn attach(api: *Api, ctx: *http.Ctx, hub: *sse.Hub) Error!sse.Hub.Token {
    const at = try api.gpa.create(Attachment);
    errdefer api.gpa.destroy(at);
    at.* = .{ .gpa = api.gpa, .hub = hub, .conn = ctx.conn };

    // Subscribe before the head goes out: a hub that is full should
    // answer with an error status, which is no longer possible once the
    // 200 has been sent.
    at.token = try hub.subscribe(at.sink());
    errdefer hub.unsubscribe(at.token);

    try ctx.res.beginEventStream();
    ctx.detach(.{ .ctx = at, .on_close = Attachment.onClose });
    return at.token;
}
