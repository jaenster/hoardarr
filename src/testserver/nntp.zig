//! A fake NNTP server that speaks enough of RFC 3977 to satisfy
//! hoardarr's fetcher, running on the caller's reactor loop.
//!
//! Unlike `nntp/conn.zig`'s `StubServer` — which replays a fixed script
//! of command/reply pairs and fails the test on anything unexpected —
//! this one is content-addressed: articles are registered by message-id
//! and served to whoever asks, in any order, over as many connections as
//! the client opens. That is what an end-to-end run needs; a script is
//! what a protocol unit test needs.
//!
//! Supported: the 200 greeting, AUTHINFO USER/PASS, MODE READER, DATE,
//! GROUP, BODY, ARTICLE, HEAD, STAT, QUIT. Not supported, deliberately:
//! posting, overview, XOVER, streaming feeds. Pointing a real newsreader
//! at this will disappoint it.
//!
//! # It is strict, and it remembers
//!
//! A fake that answers whatever it likes cannot prove the client asked
//! correctly — which is how a TLS client that never flushed a single
//! command past the greeting was once "verified by hand" against a
//! scripted peer. So: every verb is matched whole, every argument is
//! parsed, and anything malformed or out of sequence is refused with the
//! code a real server would send instead of being guessed at. A command
//! is never answered as if it were a different one.
//!
//! Every line taken off the wire is recorded with the connection that
//! sent it (`received`, `commandSequence`, `requestCount`, `sawVerb`), so
//! a test asserts on what the client *sent* and not only on the bytes
//! that came back. The two are independent failures: a fetch can decode
//! perfectly while the handshake skipped AUTHINFO.
//!
//! The knobs exist so the e2e suite can reproduce what real providers
//! do to you:
//!
//!   * `setMissingFraction` — a share of articles answer 430, so the
//!     PAR2 repair path gets exercised. The decision is a hash of the
//!     message-id, not a coin flip: the same id drops on every run and
//!     on every retry, because a flaky e2e suite proves nothing.
//!   * `setBytesPerSec` — paced body writes, so speed accounting and
//!     stall timeouts have something to measure.
//!   * `setArticleLatency` — a fixed delay before the response line,
//!     for high-latency providers.
//!   * `max_connections` — past the cap, new connections get
//!     "502 too many connections" and a hangup, which is exactly what
//!     the pool's cap and the failover path need to see.
//!
//! No threads: pacing and latency are reactor timers, so a test drives
//! one loop and everything — client and server — advances on it.

const std = @import("std");
const Allocator = std.mem.Allocator;

const sys = @import("../posix/sys.zig");
const reactor = @import("../posix/reactor.zig");
const socket = @import("../net/socket.zig");

const IpAddress = std.Io.net.IpAddress;

pub const Error = Allocator.Error || sys.Error;
pub const StartError = Error || error{InvalidAddress};

pub const Options = struct {
    /// Listen address. The default lets the kernel pick the port; ask
    /// `Server.port()` for it afterwards.
    host: []const u8 = "127.0.0.1",
    port: u16 = 0,

    /// When either is non-empty, AUTHINFO must succeed with exactly
    /// these before BODY/ARTICLE/HEAD will answer. Both empty means the
    /// server accepts any credentials, or none.
    username: []const u8 = "",
    password: []const u8 = "",

    /// Cap on the write rate of article bodies. 0 is no cap.
    bytes_per_sec: u64 = 0,

    /// Delay inserted before the response line of BODY/ARTICLE/HEAD.
    article_latency_ns: u64 = 0,

    /// Share of articles answered with 430, in [0, 1]. Which ones is a
    /// function of the message-id and `missing_salt`, so it is stable
    /// across runs and across retries of the same article.
    missing_fraction: f64 = 0,
    /// Changes *which* articles drop without changing how many.
    missing_salt: u64 = 0,

    /// Simultaneous connections allowed. 0 is unlimited. Connection
    /// number cap+1 gets `busy_greeting` and is hung up on.
    max_connections: usize = 0,

    greeting: []const u8 = "200 hoardarr fake nntp ready\r\n",
    /// Eweka's phrasing; `nntp/protocol.zig` classifies it as a
    /// too-many-connections condition on any 4xx/5xx code.
    busy_greeting: []const u8 = "502 too many connections from your IP\r\n",
};

/// One command line as the server received it.
pub const Received = struct {
    /// Accept order of the connection that sent it, starting at 1. What
    /// makes "the pool opened four connections and each did its own
    /// handshake" an assertion rather than a hope.
    conn: usize,
    /// The line, terminator stripped. Owned by the server.
    line: []const u8,
    /// The message-id argument of a retrieval command, as a slice of
    /// `line`. Empty for every other verb.
    id: []const u8 = "",
    /// False when the server refused the line instead of answering it.
    accepted: bool = true,
};

pub const Server = struct {
    gpa: Allocator,
    loop: *reactor.Loop,
    listener: socket.Listener,
    opts: Options,

    /// message-id (bare, no angle brackets) → yEnc body. Keys and
    /// values both owned.
    corpus: std.StringHashMapUnmanaged([]u8) = .empty,

    /// Live sessions, including ones waiting to be reaped.
    sessions: std.ArrayList(*Session) = .empty,
    /// Dead sessions are torn down from a timer rather than from inside
    /// a socket callback: the reactor dispatches a batch of ready
    /// sources per tick, and unregistering one mid-batch would disturb
    /// the backend's arrays. Timers fire after the batch, which is the
    /// safe moment.
    reap_timer: reactor.Timer = .{ .callback = onReap },

    // Counters, for assertions in tests.
    accepted: usize = 0,
    /// Connections turned away by `max_connections`.
    refused: usize = 0,
    /// Currently-open sessions that were not refused.
    open: usize = 0,
    /// Articles served in full.
    served: usize = 0,
    /// Bytes handed to the transport, response lines included. Counted
    /// where they are offered rather than where the kernel accepts them,
    /// because that is the load the server is generating; what a slow
    /// client has actually drained is its own connection's business.
    bytes_written: u64 = 0,
    /// Requests answered 430, whether unknown or deliberately dropped.
    missed: usize = 0,
    /// Command lines dispatched, across all connections.
    commands: usize = 0,
    /// Command lines refused as malformed, unknown or out of sequence.
    /// Non-zero in a test that did not intend it means the client is
    /// speaking a protocol this server does not recognise.
    rejected: usize = 0,

    /// Every command line taken off the wire, in arrival order. Owned.
    received: std.ArrayList(Received) = .empty,

    /// Binds and starts accepting. Returns the bound port. Initialises
    /// in place: the reactor keeps a pointer to `&self.listener.source`,
    /// so a by-value return would leave it dangling.
    pub fn start(self: *Server, gpa: Allocator, loop: *reactor.Loop, opts: Options) StartError!u16 {
        self.* = .{
            .gpa = gpa,
            .loop = loop,
            .listener = undefined,
            .opts = opts,
        };
        try self.listener.listen(try parseAddr(opts.host, opts.port), onAccept, 64);
        errdefer self.listener.close();
        self.listener.context = self;
        try loop.add(&self.listener.source);
        return self.listener.boundPort();
    }

    /// Closes the listener and tears down every connection. Idempotent
    /// enough to be both `stop` and `deinit`; `deinit` is the alias.
    pub fn stop(self: *Server) void {
        if (self.listener.source.fd != sys.invalid_fd) {
            if (self.listener.source.isRegistered()) self.loop.remove(&self.listener.source);
            self.listener.close();
        }
        if (self.reap_timer.isArmed()) self.loop.cancelTimer(&self.reap_timer);

        for (self.sessions.items) |s| self.destroySession(s);
        self.sessions.deinit(self.gpa);
        self.sessions = .empty;
        self.open = 0;

        self.clearCorpus();
        self.corpus.deinit(self.gpa);
        self.corpus = .empty;

        self.clearReceived();
        self.received.deinit(self.gpa);
        self.received = .empty;
    }

    pub fn deinit(self: *Server) void {
        self.stop();
    }

    pub fn port(self: *const Server) sys.Error!u16 {
        return self.listener.boundPort();
    }

    // -- corpus -------------------------------------------------------

    /// Registers a yEnc body under `msg_id` (bare, no angle brackets).
    /// Both are copied. Re-registering an id replaces it.
    pub fn addArticle(self: *Server, msg_id: []const u8, body: []const u8) Allocator.Error!void {
        const body_copy = try self.gpa.dupe(u8, body);
        errdefer self.gpa.free(body_copy);

        const gop = try self.corpus.getOrPut(self.gpa, msg_id);
        if (gop.found_existing) {
            self.gpa.free(gop.value_ptr.*);
        } else {
            gop.key_ptr.* = self.gpa.dupe(u8, msg_id) catch |err| {
                // Leave no entry with a borrowed key behind.
                _ = self.corpus.remove(msg_id);
                return err;
            };
        }
        gop.value_ptr.* = body_copy;
    }

    /// Every registered message-id. Caller owns the outer slice; the ids
    /// themselves belong to the server and die with `reset`/`stop`.
    pub fn articles(self: *const Server, gpa: Allocator) Allocator.Error![][]const u8 {
        const out = try gpa.alloc([]const u8, self.corpus.count());
        var i: usize = 0;
        var it = self.corpus.keyIterator();
        while (it.next()) |k| : (i += 1) out[i] = k.*;
        return out;
    }

    pub fn articleCount(self: *const Server) usize {
        return self.corpus.count();
    }

    pub fn hasArticle(self: *const Server, msg_id: []const u8) bool {
        return self.corpus.contains(msg_id);
    }

    /// Drops every registered article. Connections and options survive,
    /// which is what a test wants between phases.
    pub fn reset(self: *Server) void {
        self.clearCorpus();
        self.clearReceived();
        self.served = 0;
        self.missed = 0;
        self.commands = 0;
        self.rejected = 0;
        self.bytes_written = 0;
    }

    fn clearCorpus(self: *Server) void {
        var it = self.corpus.iterator();
        while (it.next()) |e| {
            self.gpa.free(e.key_ptr.*);
            self.gpa.free(e.value_ptr.*);
        }
        self.corpus.clearRetainingCapacity();
    }

    fn clearReceived(self: *Server) void {
        for (self.received.items) |r| self.gpa.free(@constCast(r.line));
        self.received.clearRetainingCapacity();
    }

    // -- what the client actually sent ---------------------------------

    /// The command lines one connection sent, in order. Caller owns the
    /// outer slice; the lines belong to the server.
    ///
    /// Per connection rather than in aggregate because the handshake is a
    /// per-connection sequence: a pool that authenticated once and then
    /// opened three unauthenticated connections would look perfectly
    /// healthy in a flat list.
    pub fn commandSequence(self: *const Server, gpa: Allocator, conn: usize) Allocator.Error![][]const u8 {
        var out: std.ArrayList([]const u8) = .empty;
        errdefer out.deinit(gpa);
        for (self.received.items) |r| {
            if (r.conn == conn) try out.append(gpa, r.line);
        }
        return out.toOwnedSlice(gpa);
    }

    /// How many lines began with `verb` — a whole verb, so `MODE` does
    /// not match `MODEM` and `AUTHINFO USER` does not match a bare
    /// `AUTHINFO`.
    pub fn countVerb(self: *const Server, verb: []const u8) usize {
        var n: usize = 0;
        for (self.received.items) |r| {
            if (lineHasVerb(r.line, verb)) n += 1;
        }
        return n;
    }

    pub fn sawVerb(self: *const Server, verb: []const u8) bool {
        return self.countVerb(verb) != 0;
    }

    /// How many retrieval commands named `msg_id` (bare, no angle
    /// brackets), refusals included — a retry asks twice.
    pub fn requestCount(self: *const Server, msg_id: []const u8) usize {
        var n: usize = 0;
        for (self.received.items) |r| {
            if (r.id.len != 0 and std.mem.eql(u8, r.id, msg_id)) n += 1;
        }
        return n;
    }

    pub fn wasRequested(self: *const Server, msg_id: []const u8) bool {
        return self.requestCount(msg_id) != 0;
    }

    /// Distinct message-ids asked for, in first-request order. Caller
    /// owns the outer slice; the ids belong to the server.
    pub fn requestedIds(self: *const Server, gpa: Allocator) Allocator.Error![][]const u8 {
        var out: std.ArrayList([]const u8) = .empty;
        errdefer out.deinit(gpa);
        for (self.received.items) |r| {
            if (r.id.len == 0) continue;
            var seen = false;
            for (out.items) |prev| {
                if (std.mem.eql(u8, prev, r.id)) {
                    seen = true;
                    break;
                }
            }
            if (!seen) try out.append(gpa, r.id);
        }
        return out.toOwnedSlice(gpa);
    }

    // -- knobs --------------------------------------------------------

    /// Takes effect on the next body write; an in-flight one finishes at
    /// the rate it started with.
    pub fn setBytesPerSec(self: *Server, bps: u64) void {
        self.opts.bytes_per_sec = bps;
    }

    pub fn setArticleLatency(self: *Server, ns: u64) void {
        self.opts.article_latency_ns = ns;
    }

    /// Clamped to [0, 1]. Because the decision is a pure function of the
    /// message-id, changing this changes past answers too — which is the
    /// point when a test wants to "restore" a provider mid-run.
    pub fn setMissingFraction(self: *Server, f: f64) void {
        self.opts.missing_fraction = std.math.clamp(f, 0, 1);
    }

    pub fn setMissingSalt(self: *Server, salt: u64) void {
        self.opts.missing_salt = salt;
    }

    pub fn setCredentials(self: *Server, username: []const u8, password: []const u8) void {
        self.opts.username = username;
        self.opts.password = password;
    }

    pub fn setMaxConnections(self: *Server, n: usize) void {
        self.opts.max_connections = n;
    }

    /// Whether `msg_id` is in the deliberately-missing share. Pure, so a
    /// test can compute the expected loss set up front instead of
    /// inferring it from what the run happened to fetch.
    pub fn wouldDrop(self: *const Server, msg_id: []const u8) bool {
        const f = self.opts.missing_fraction;
        if (f <= 0) return false;
        if (f >= 1) return true;
        // The top 53 bits of a Wyhash digest, scaled into [0, 1) — the
        // standard way to get a uniform double out of a 64-bit hash
        // without the rounding artefacts of a plain division.
        const h = std.hash.Wyhash.hash(self.opts.missing_salt, msg_id);
        const u: f64 = @as(f64, @floatFromInt(h >> 11)) * 0x1.0p-53;
        return u < f;
    }

    fn requiresAuth(self: *const Server) bool {
        return self.opts.username.len != 0 or self.opts.password.len != 0;
    }

    fn lookup(self: *const Server, msg_id: []const u8) ?[]const u8 {
        return self.corpus.get(msg_id);
    }

    // -- connection lifecycle -----------------------------------------

    fn onAccept(l: *socket.Listener, fd: sys.Fd) void {
        const self: *Server = @ptrCast(@alignCast(l.context.?));
        self.accepted += 1;

        const over_cap = self.opts.max_connections != 0 and self.open >= self.opts.max_connections;

        const s = self.gpa.create(Session) catch {
            sys.close(fd);
            return;
        };
        s.* = .{ .stream = undefined, .server = self, .refused = over_cap, .conn = self.accepted };
        s.stream.initAccepted(self.gpa, self.loop, fd, &Session.handler) catch {
            sys.close(fd);
            self.gpa.destroy(s);
            return;
        };
        self.sessions.append(self.gpa, s) catch {
            s.stream.deinit();
            self.gpa.destroy(s);
            return;
        };

        if (over_cap) {
            self.refused += 1;
            // Greet with the refusal and hang up. Real providers do
            // exactly this, and it is the shape the pool's failover
            // logic is written against.
            s.stream.write(self.opts.busy_greeting) catch {};
            s.markDead();
            return;
        }
        self.open += 1;
        // NNTP is server-speaks-first.
        s.stream.write(self.opts.greeting) catch s.markDead();
    }

    /// Queue `s` for teardown at the next safe point.
    fn scheduleReap(self: *Server) void {
        if (self.reap_timer.isArmed()) return;
        // Zero delay: fires at the end of the current tick, after the
        // whole ready batch has been dispatched.
        self.loop.addTimer(&self.reap_timer, 0) catch {};
    }

    fn onReap(timer: *reactor.Timer) void {
        const self: *Server = @fieldParentPtr("reap_timer", timer);
        var i: usize = 0;
        while (i < self.sessions.items.len) {
            const s = self.sessions.items[i];
            if (!s.dead) {
                i += 1;
                continue;
            }
            _ = self.sessions.swapRemove(i);
            self.destroySession(s);
        }
    }

    fn destroySession(self: *Server, s: *Session) void {
        if (s.timer.isArmed()) self.loop.cancelTimer(&s.timer);
        s.out.deinit(self.gpa);
        s.stream.deinit();
        self.gpa.destroy(s);
    }
};

// ---------------------------------------------------------------------
// Session
// ---------------------------------------------------------------------

/// Longest command line accepted. Real NNTP commands are far shorter;
/// this only has to hold a message-id plus the verb.
const max_line = 1024;

const Session = struct {
    stream: socket.Stream,
    server: *Server,
    /// Accept order, starting at 1. Stamped onto every recorded line so
    /// a test can read one connection's conversation on its own.
    conn: usize = 0,
    /// Index into `server.received` of the line being dispatched, when it
    /// was recorded — null when the recording allocation failed, which a
    /// test allocator turns into a leak-free failure elsewhere.
    record_at: ?usize = null,
    /// Serves both the latency delay and the throttle pacing — they are
    /// never armed at the same time.
    timer: reactor.Timer = .{ .callback = onTimer },

    in: [max_line * 2]u8 = undefined,
    in_len: usize = 0,
    /// The command currently being dispatched, copied out of `in` so the
    /// input buffer can be compacted first.
    line: [max_line]u8 = undefined,
    line_len: usize = 0,

    user: [128]u8 = undefined,
    user_len: usize = 0,
    /// An `AUTHINFO USER` is outstanding, so a `PASS` is in sequence.
    saw_user: bool = false,
    authed: bool = false,

    /// True for a connection turned away by the cap; it never gets a
    /// greeting or a command.
    refused: bool = false,
    dead: bool = false,
    /// Set after an over-long line: everything up to the next newline is
    /// the tail of that garbage and must not be parsed as a command.
    discarding: bool = false,

    /// Dot-stuffed body plus terminator, waiting to go out.
    out: std.ArrayList(u8) = .empty,
    out_pos: usize = 0,

    /// The article a delayed response is for.
    pending_id: [max_line]u8 = undefined,
    pending_id_len: usize = 0,
    pending_kind: Kind = .body,

    state: State = .idle,

    const State = enum {
        /// Ready to take the next command.
        idle,
        /// Waiting out `article_latency_ns` before answering.
        latency,
        /// Draining `out` at whatever the throttle allows.
        sending,
    };

    const Kind = enum { body, article, head };

    const handler: socket.Handler = .{
        .on_readable = onReadable,
        .on_close = onClose,
    };

    fn onReadable(st: *socket.Stream) void {
        const self: *Session = @fieldParentPtr("stream", st);
        if (self.dead) return;

        while (true) {
            if (self.in_len == self.in.len) break;
            const n = st.read(self.in[self.in_len..]) catch |err| switch (err) {
                error.WouldBlock => break,
                else => {
                    self.markDead();
                    return;
                },
            };
            if (n == 0) {
                // Peer half-closed. Nothing more will arrive, and a
                // level-triggered loop would report readable forever.
                self.markDead();
                return;
            }
            self.in_len += n;
        }
        self.pump();
    }

    fn onClose(st: *socket.Stream, _: ?socket.Error) void {
        const self: *Session = @fieldParentPtr("stream", st);
        self.markDead();
    }

    fn onTimer(timer: *reactor.Timer) void {
        const self: *Session = @fieldParentPtr("timer", timer);
        if (self.dead) return;
        if (self.state == .latency) {
            self.state = .idle;
            self.beginSend(self.pending_id[0..self.pending_id_len], self.pending_kind);
        }
        self.pump();
    }

    /// The one place progress is made. Flat rather than recursive: a
    /// client that pipelines a hundred commands must not cost a hundred
    /// stack frames.
    fn pump(self: *Session) void {
        while (!self.dead) {
            switch (self.state) {
                // The timer owns the next step in both non-idle states.
                .latency => return,
                .sending => {
                    self.pumpSend();
                    if (self.state == .sending) return;
                },
                .idle => {
                    if (!self.takeLine()) return;
                    self.dispatch();
                },
            }
        }
    }

    /// Moves the next complete command line into `self.line`, stripped
    /// of its terminator, and compacts `in`. False when no full line is
    /// buffered yet.
    fn takeLine(self: *Session) bool {
        if (self.discarding) {
            const nl = std.mem.indexOfScalar(u8, self.in[0..self.in_len], '\n') orelse {
                self.in_len = 0;
                return false;
            };
            const rest = self.in_len - (nl + 1);
            if (rest > 0) std.mem.copyForwards(u8, self.in[0..rest], self.in[nl + 1 .. self.in_len]);
            self.in_len = rest;
            self.discarding = false;
        }
        const nl = std.mem.indexOfScalar(u8, self.in[0..self.in_len], '\n') orelse {
            // A client that fills the buffer without a newline is not
            // speaking NNTP. Say so, then swallow the rest of its line
            // so the next real command is not parsed as its tail.
            if (self.in_len == self.in.len) {
                self.server.rejected += 1;
                self.writeLine("501 command line too long");
                self.in_len = 0;
                self.discarding = true;
            }
            return false;
        };
        const raw = std.mem.trimEnd(u8, self.in[0..nl], "\r");
        self.line_len = @min(raw.len, self.line.len);
        @memcpy(self.line[0..self.line_len], raw[0..self.line_len]);

        const rest = self.in_len - (nl + 1);
        if (rest > 0) std.mem.copyForwards(u8, self.in[0..rest], self.in[nl + 1 .. self.in_len]);
        self.in_len = rest;
        return true;
    }

    /// Answers one command line, or refuses it.
    ///
    /// Every branch matches a *whole* verb and parses its argument. The
    /// looser "does the line start with BODY" test this replaced could
    /// not tell a command apart from a truncated or corrupted one, so a
    /// client that sent `BODYX <a@h>` — or that sent nothing at all and
    /// had its buffer answered by a scripted peer — still saw a 222.
    fn dispatch(self: *Session) void {
        const cmd = self.line[0..self.line_len];
        // A refused connection has already been told why; anything it
        // says now is recorded but never answered.
        if (self.refused) {
            self.record(cmd);
            return;
        }
        self.server.commands += 1;
        self.record(cmd);

        // An empty line is not a command. Answering it would mean the
        // server invented a request the client never made.
        if (cmd.len == 0) return self.reject("500 empty command");

        const verb_end = std.mem.indexOfAny(u8, cmd, " \t") orelse cmd.len;
        const verb = cmd[0..verb_end];
        const args = std.mem.trim(u8, cmd[verb_end..], " \t");

        if (eqlIgnoreCase(verb, "AUTHINFO")) {
            self.authinfo(args);
        } else if (eqlIgnoreCase(verb, "MODE")) {
            // A `MODE` this server does not implement gets the code a
            // real one sends, never the reader-mode answer.
            if (!eqlIgnoreCase(args, "READER")) return self.reject("501 unsupported MODE");
            self.writeLine("200 reader mode");
        } else if (eqlIgnoreCase(verb, "DATE")) {
            if (args.len != 0) return self.reject("501 DATE takes no argument");
            var buf: [32]u8 = undefined;
            self.writeLine(std.fmt.bufPrint(&buf, "111 {s}", .{utcStamp()}) catch "111 19700101000000");
        } else if (eqlIgnoreCase(verb, "GROUP")) {
            // Any group exists and has one article. The fetcher only
            // cares that the command succeeded — but it has to have
            // named a group.
            if (args.len == 0) return self.reject("501 GROUP needs a group name");
            self.writeLine("211 1 1 1 misc.test");
        } else if (eqlIgnoreCase(verb, "BODY")) {
            self.serveArticle(args, .body);
        } else if (eqlIgnoreCase(verb, "ARTICLE")) {
            self.serveArticle(args, .article);
        } else if (eqlIgnoreCase(verb, "HEAD")) {
            self.serveArticle(args, .head);
        } else if (eqlIgnoreCase(verb, "STAT")) {
            const id = parseMessageId(args) orelse return self.reject("501 bad command");
            self.noteId(id);
            if (self.server.lookup(id) != null and !self.server.wouldDrop(id)) {
                self.writeFmt("223 0 <{s}>", .{id});
            } else {
                self.server.missed += 1;
                self.writeFmt("430 no such article <{s}>", .{id});
            }
        } else if (eqlIgnoreCase(verb, "QUIT")) {
            if (args.len != 0) return self.reject("501 QUIT takes no argument");
            self.writeLine("205 bye");
            self.stream.shutdownWrite();
            self.markDead();
        } else if (eqlIgnoreCase(verb, "CAPABILITIES")) {
            self.write("101 capability list follows\r\nVERSION 2\r\nREADER\r\n.\r\n");
        } else {
            self.reject("500 unknown command");
        }
    }

    /// `AUTHINFO USER x` / `AUTHINFO PASS y`, in that order.
    ///
    /// The ordering is enforced rather than assumed: a client that sent
    /// PASS first would otherwise authenticate against whatever username
    /// happened to be left in the session buffer — which is exactly the
    /// class of "the stub answered a command the client never sent
    /// properly" this file exists to rule out.
    fn authinfo(self: *Session, args: []const u8) void {
        const sub_end = std.mem.indexOfAny(u8, args, " \t") orelse args.len;
        const sub = args[0..sub_end];
        const rest = std.mem.trim(u8, args[sub_end..], " \t");

        if (eqlIgnoreCase(sub, "USER")) {
            if (rest.len == 0) return self.reject("501 AUTHINFO USER needs a username");
            self.user_len = @min(rest.len, self.user.len);
            @memcpy(self.user[0..self.user_len], rest[0..self.user_len]);
            self.saw_user = true;
            self.writeLine("381 enter password");
            return;
        }
        if (eqlIgnoreCase(sub, "PASS")) {
            if (rest.len == 0) return self.reject("501 AUTHINFO PASS needs a password");
            if (!self.saw_user) return self.reject("482 authentication commands out of sequence");
            // One PASS answers one USER; a second must name its user
            // again, as RFC 4643 requires.
            self.saw_user = false;
            if (self.server.requiresAuth()) {
                if (std.mem.eql(u8, self.user[0..self.user_len], self.server.opts.username) and
                    std.mem.eql(u8, rest, self.server.opts.password))
                {
                    self.authed = true;
                    self.writeLine("281 authentication accepted");
                } else {
                    self.writeLine("481 authentication failed");
                }
            } else {
                self.authed = true;
                self.writeLine("281 authentication accepted");
            }
            return;
        }
        self.reject("501 unsupported AUTHINFO variant");
    }

    fn serveArticle(self: *Session, args: []const u8, kind: Kind) void {
        if (self.server.requiresAuth() and !self.authed) {
            self.writeLine("480 authentication required");
            return;
        }
        // By article number is legal NNTP and deliberately unimplemented:
        // hoardarr only ever retrieves by message-id, and a stub that
        // guessed at a numeric argument would be inventing a corpus
        // position the test never registered.
        const id = parseMessageId(args) orelse {
            self.reject("501 bad command");
            return;
        };
        self.noteId(id);
        const latency = self.server.opts.article_latency_ns;
        if (latency == 0) {
            self.beginSend(id, kind);
            return;
        }
        self.pending_id_len = @min(id.len, self.pending_id.len);
        @memcpy(self.pending_id[0..self.pending_id_len], id[0..self.pending_id_len]);
        self.pending_kind = kind;
        self.state = .latency;
        self.server.loop.addTimer(&self.timer, latency) catch {
            // Can't delay; answering immediately beats stalling forever.
            self.state = .idle;
            self.beginSend(id, kind);
        };
    }

    /// Writes the status line and queues the body. Leaves the session in
    /// `.sending` when there is a body to pace out, `.idle` otherwise.
    fn beginSend(self: *Session, id: []const u8, kind: Kind) void {
        const body = self.server.lookup(id);
        if (body == null or self.server.wouldDrop(id)) {
            self.server.missed += 1;
            self.writeFmt("430 no such article <{s}>", .{id});
            return;
        }

        switch (kind) {
            .body => self.writeFmt("222 0 <{s}>", .{id}),
            .article => self.writeFmt("220 0 <{s}>", .{id}),
            // HEAD gets a synthetic header block rather than the body;
            // the fetcher only uses it to probe for existence.
            .head => self.writeFmt("221 0 <{s}>", .{id}),
        }
        if (self.dead) return;

        self.out.clearRetainingCapacity();
        self.out_pos = 0;
        const payload: []const u8 = if (kind == .head) "" else body.?;
        appendDotStuffed(&self.out, self.server.gpa, payload) catch {
            self.markDead();
            return;
        };
        if (kind == .head) {
            self.out.print(self.server.gpa, "Message-ID: <{s}>\r\nNewsgroups: misc.test\r\n", .{id}) catch {
                self.markDead();
                return;
            };
        }
        self.out.appendSlice(self.server.gpa, ".\r\n") catch {
            self.markDead();
            return;
        };
        self.state = .sending;
    }

    /// Pushes as much of `out` as the throttle permits. When capped,
    /// arms the timer for the time those bytes should have taken and
    /// leaves the state at `.sending`.
    fn pumpSend(self: *Session) void {
        const bps = self.server.opts.bytes_per_sec;
        const remaining = self.out.items.len - self.out_pos;

        if (bps == 0) {
            self.write(self.out.items[self.out_pos..]);
            self.out_pos = self.out.items.len;
            self.finishSend();
            return;
        }

        // ~20 chunks a second: fine enough that a progress reading looks
        // continuous, coarse enough that the timer is not the bottleneck.
        const chunk = @max(@as(usize, 64), @min(remaining, @as(usize, @intCast(bps / 20 + 1))));
        self.write(self.out.items[self.out_pos..][0..@min(chunk, remaining)]);
        if (self.dead) return;
        const sent = @min(chunk, remaining);
        self.out_pos += sent;

        if (self.out_pos >= self.out.items.len) {
            self.finishSend();
            return;
        }
        const delay = @as(u64, @intCast(sent)) *% std.time.ns_per_s / bps;
        self.server.loop.addTimer(&self.timer, delay) catch {
            // Without a timer there is no pacing left to do; finish at
            // full speed rather than wedging the connection.
            self.write(self.out.items[self.out_pos..]);
            self.out_pos = self.out.items.len;
            self.finishSend();
        };
    }

    fn finishSend(self: *Session) void {
        self.out.clearRetainingCapacity();
        self.out_pos = 0;
        self.state = .idle;
        self.server.served += 1;
    }

    /// Files the line under the connection that sent it. Recorded before
    /// it is answered, so a command that is refused — or that kills the
    /// session — is still in the transcript a test reads.
    fn record(self: *Session, cmd: []const u8) void {
        self.record_at = null;
        const gpa = self.server.gpa;
        const copy = gpa.dupe(u8, cmd) catch return;
        self.server.received.append(gpa, .{ .conn = self.conn, .line = copy }) catch {
            gpa.free(copy);
            return;
        };
        self.record_at = self.server.received.items.len - 1;
    }

    /// Attaches the parsed message-id to the line being dispatched. A
    /// slice of the recorded copy, not of `self.line`, which is reused by
    /// the next command.
    fn noteId(self: *Session, id: []const u8) void {
        const at = self.record_at orelse return;
        const r = &self.server.received.items[at];
        const off = @intFromPtr(id.ptr) - @intFromPtr(&self.line);
        r.id = r.line[off..][0..id.len];
    }

    /// Refuses the line with `reply` and marks it refused in the
    /// transcript, so "the client sent nothing this server understood"
    /// is a countable outcome rather than a silent one.
    fn reject(self: *Session, reply: []const u8) void {
        self.server.rejected += 1;
        if (self.record_at) |at| self.server.received.items[at].accepted = false;
        self.writeLine(reply);
    }

    fn write(self: *Session, bytes: []const u8) void {
        if (self.dead) return;
        self.server.bytes_written += bytes.len;
        self.stream.write(bytes) catch self.markDead();
    }

    fn writeLine(self: *Session, line: []const u8) void {
        self.write(line);
        self.write("\r\n");
    }

    fn writeFmt(self: *Session, comptime fmt: []const u8, args: anytype) void {
        var buf: [max_line + 64]u8 = undefined;
        const s = std.fmt.bufPrint(&buf, fmt ++ "\r\n", args) catch {
            self.writeLine("500 internal error");
            return;
        };
        self.write(s);
    }

    fn markDead(self: *Session) void {
        if (self.dead) return;
        self.dead = true;
        if (self.timer.isArmed()) self.server.loop.cancelTimer(&self.timer);
        if (!self.refused) self.server.open -= 1;
        self.server.scheduleReap();
    }
};

// ---------------------------------------------------------------------
// Wire helpers
// ---------------------------------------------------------------------

/// Copies `body` with dot-stuffing applied: a line that begins with '.'
/// gets a second one, so it cannot be mistaken for the terminator. The
/// body is expected to already use CRLF endings; a tail without one gets
/// a CRLF appended so the terminating dot lands on its own line.
fn appendDotStuffed(out: *std.ArrayList(u8), gpa: Allocator, body: []const u8) Allocator.Error!void {
    var start: usize = 0;
    while (std.mem.indexOfScalarPos(u8, body, start, '\n')) |nl| {
        const line = body[start .. nl + 1];
        if (line[0] == '.') try out.append(gpa, '.');
        try out.appendSlice(gpa, line);
        start = nl + 1;
    }
    if (start < body.len) {
        const tail = body[start..];
        if (tail[0] == '.') try out.append(gpa, '.');
        try out.appendSlice(gpa, tail);
        try out.appendSlice(gpa, "\r\n");
    }
}

/// The argument of a retrieval command: `<abc@host>` → `abc@host`.
///
/// Null for anything else, and that strictness is the point. A scan for
/// the first '<' and the last '>' would accept `BODY junk <a@h> junk`,
/// and worse, would accept `BODY <a@h> BODY <b@h>` as a request for one
/// id spanning both — turning a client that failed to terminate its
/// commands into a passing test.
fn parseMessageId(args: []const u8) ?[]const u8 {
    if (args.len < 3) return null;
    if (args[0] != '<' or args[args.len - 1] != '>') return null;
    const inner = args[1 .. args.len - 1];
    if (inner.len == 0) return null;
    // Whitespace or a nested bracket means this is not one message-id.
    if (std.mem.indexOfAny(u8, inner, "<> \t") != null) return null;
    return inner;
}

/// Whether `line` opens with the whole of `verb`. `MODE` does not match
/// `MODEREADER`, and `AUTHINFO USER` does not match a bare `AUTHINFO`.
fn lineHasVerb(line: []const u8, verb: []const u8) bool {
    if (!std.ascii.startsWithIgnoreCase(line, verb)) return false;
    return line.len == verb.len or line[verb.len] == ' ' or line[verb.len] == '\t';
}

fn eqlIgnoreCase(a: []const u8, b: []const u8) bool {
    return std.ascii.eqlIgnoreCase(a, b);
}

fn parseAddr(host: []const u8, port: u16) error{InvalidAddress}!IpAddress {
    return IpAddress.parse(host, port) catch error.InvalidAddress;
}

/// `YYYYMMDDHHMMSS` in UTC, the format the DATE response wants.
fn utcStamp() [14]u8 {
    const now = sys.realtimeNanos();
    const secs: u64 = @intCast(@divFloor(now, std.time.ns_per_s));
    const es: std.time.epoch.EpochSeconds = .{ .secs = secs };
    const day = es.getEpochDay();
    const ymd = day.calculateYearDay();
    const md = ymd.calculateMonthDay();
    const ds = es.getDaySeconds();

    var out: [14]u8 = undefined;
    _ = std.fmt.bufPrint(&out, "{d:0>4}{d:0>2}{d:0>2}{d:0>2}{d:0>2}{d:0>2}", .{
        ymd.year,
        md.month.numeric(),
        md.day_index + 1,
        ds.getHoursIntoDay(),
        ds.getMinutesIntoHour(),
        ds.getSecondsIntoMinute(),
    }) catch unreachable;
    return out;
}

// ---------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------

const t = std.testing;
const fixture = @import("fixture.zig");
const yenc = @import("../codec/yenc.zig");

/// A minimal line-oriented NNTP client on the same loop. Enough to drive
/// the server: it collects everything the server sends and lets a test
/// assert on the transcript, with the dot-terminated block decoded back
/// out of the stream.
const Client = struct {
    stream: socket.Stream = undefined,
    gpa: Allocator,
    rx: std.ArrayList(u8) = .empty,
    /// The stream owns an fd only between `connect` and `close`.
    live: bool = false,
    closed: bool = false,
    err: ?socket.Error = null,

    const handler: socket.Handler = .{
        .on_readable = onReadable,
        .on_close = onClose,
        .on_connected = onConnected,
    };

    fn connect(self: *Client, loop: *reactor.Loop, p: u16) !void {
        try self.stream.connect(self.gpa, loop, try IpAddress.parse("127.0.0.1", p), &handler);
        self.live = true;
    }

    /// Hangs up. Idempotent, so a test can close early and still let the
    /// deferred `deinit` run.
    fn close(self: *Client) void {
        if (!self.live) return;
        self.live = false;
        self.closed = true;
        self.stream.deinit();
    }

    fn deinit(self: *Client) void {
        self.close();
        self.rx.deinit(self.gpa);
    }

    fn onConnected(st: *socket.Stream, err: ?socket.Error) void {
        const self: *Client = @fieldParentPtr("stream", st);
        self.err = err;
    }

    fn onReadable(st: *socket.Stream) void {
        const self: *Client = @fieldParentPtr("stream", st);
        var buf: [8192]u8 = undefined;
        while (true) {
            const n = st.read(&buf) catch break;
            if (n == 0) {
                self.closed = true;
                st.state = .closed;
                break;
            }
            self.rx.appendSlice(self.gpa, buf[0..n]) catch break;
        }
    }

    fn onClose(st: *socket.Stream, err: ?socket.Error) void {
        const self: *Client = @fieldParentPtr("stream", st);
        self.closed = true;
        self.err = err;
        st.state = .closed;
    }

    fn send(self: *Client, line: []const u8) !void {
        try self.stream.write(line);
        try self.stream.write("\r\n");
    }

    fn text(self: *const Client) []const u8 {
        return self.rx.items;
    }

    /// Forgets the transcript so the next exchange can be parsed from
    /// offset zero.
    fn clear(self: *Client) void {
        self.rx.clearRetainingCapacity();
    }
};

fn pumpUntil(loop: *reactor.Loop, deadline_ms: u64, ctx: anytype, done: fn (@TypeOf(ctx)) bool) !void {
    const start = sys.monotonicNanos();
    while (!done(ctx)) {
        if (sys.monotonicNanos() - start > deadline_ms * std.time.ns_per_ms) return error.TestTimeout;
        _ = try loop.tick(2);
    }
}

fn pumpFor(loop: *reactor.Loop, ms: u64) !void {
    const start = sys.monotonicNanos();
    while (sys.monotonicNanos() - start < ms * std.time.ns_per_ms) {
        _ = try loop.tick(2);
    }
}

fn containsStr(haystack: []const u8, needle: []const u8) bool {
    return std.mem.indexOf(u8, haystack, needle) != null;
}

/// Boots a loop, a server and a connected client, and returns once the
/// greeting has landed.
const Harness = struct {
    loop: reactor.Loop = undefined,
    server: Server = undefined,
    client: Client,
    port: u16 = 0,

    fn init(self: *Harness, opts: Options) !void {
        self.client = .{ .gpa = t.allocator };
        try self.loop.init(t.allocator);
        self.port = try self.server.start(t.allocator, &self.loop, opts);
        try self.client.connect(&self.loop, self.port);
        try pumpUntil(&self.loop, 2000, &self.client, struct {
            fn f(c: *Client) bool {
                return c.rx.items.len > 0 or c.closed or c.err != null;
            }
        }.f);
    }

    fn deinit(self: *Harness) void {
        self.client.deinit();
        self.server.deinit();
        self.loop.deinit();
    }

    /// Runs the loop until the client's transcript contains `needle`.
    fn awaitText(self: *Harness, needle: []const u8) !void {
        const Ctx = struct { c: *Client, n: []const u8 };
        var ctx: Ctx = .{ .c = &self.client, .n = needle };
        try pumpUntil(&self.loop, 5000, &ctx, struct {
            fn f(x: *Ctx) bool {
                return containsStr(x.c.rx.items, x.n);
            }
        }.f);
    }
};

test "greets, serves a known article, 430s an unknown one" {
    var h: Harness = undefined;
    try h.init(.{});
    defer h.deinit();

    try h.server.addArticle("a@h", "=ybegin line=128 size=5 name=x\r\nhello\r\n=yend size=5\r\n");
    try t.expect(containsStr(h.client.text(), "200 hoardarr"));

    try h.client.send("BODY <a@h>");
    try h.awaitText("\r\n.\r\n");
    try t.expect(containsStr(h.client.text(), "222 0 <a@h>"));
    try t.expect(containsStr(h.client.text(), "=ybegin line=128 size=5"));

    try h.client.send("BODY <nope@h>");
    try h.awaitText("430 no such article <nope@h>");

    try t.expectEqual(@as(usize, 1), h.server.served);
    try t.expectEqual(@as(usize, 1), h.server.missed);
}

test "ARTICLE, STAT, GROUP, MODE READER, DATE and QUIT" {
    var h: Harness = undefined;
    try h.init(.{});
    defer h.deinit();

    try h.server.addArticle("x@h", "payload\r\n");

    try h.client.send("MODE READER");
    try h.awaitText("200 reader mode");
    try h.client.send("GROUP alt.binaries.test");
    try h.awaitText("211 1 1 1 misc.test");
    try h.client.send("DATE");
    try h.awaitText("111 20");
    try h.client.send("STAT <x@h>");
    try h.awaitText("223 0 <x@h>");
    try h.client.send("STAT <gone@h>");
    try h.awaitText("430 no such article <gone@h>");
    try h.client.send("ARTICLE <x@h>");
    try h.awaitText("220 0 <x@h>");
    try h.client.send("FROBNICATE");
    try h.awaitText("500 unknown command");
    try h.client.send("QUIT");
    try h.awaitText("205 bye");
}

test "a body whose lines start with a dot is stuffed on the wire" {
    var h: Harness = undefined;
    try h.init(.{});
    defer h.deinit();

    try h.server.addArticle("dot@h", ".leading\r\n..two\r\nplain\r\n");
    try h.client.send("BODY <dot@h>");
    try h.awaitText("\r\n.\r\n");

    // Each leading dot is doubled; the terminator is the only bare one.
    try t.expect(containsStr(h.client.text(), "\r\n..leading\r\n...two\r\nplain\r\n.\r\n"));
}

test "a body without a trailing CRLF still terminates cleanly" {
    var h: Harness = undefined;
    try h.init(.{});
    defer h.deinit();

    try h.server.addArticle("raw@h", "no newline here");
    try h.client.send("BODY <raw@h>");
    try h.awaitText("\r\n.\r\n");
    try t.expect(containsStr(h.client.text(), "no newline here\r\n.\r\n"));
}

test "auth rejects bad credentials and gates article retrieval" {
    var h: Harness = undefined;
    try h.init(.{ .username = "alice", .password = "s3cret" });
    defer h.deinit();

    try h.server.addArticle("g@h", "body\r\n");

    // Unauthenticated retrieval is refused.
    try h.client.send("BODY <g@h>");
    try h.awaitText("480 authentication required");

    // Wrong password.
    try h.client.send("AUTHINFO USER alice");
    try h.awaitText("381 enter password");
    try h.client.send("AUTHINFO PASS wrong");
    try h.awaitText("481 authentication failed");

    try h.client.send("BODY <g@h>");
    try pumpFor(&h.loop, 20);
    try t.expectEqual(@as(usize, 0), h.server.served);

    // Wrong user, right password.
    try h.client.send("AUTHINFO USER mallory");
    try h.awaitText("381 enter password");
    try h.client.send("AUTHINFO PASS s3cret");
    try pumpFor(&h.loop, 20);
    try t.expectEqual(@as(usize, 2), std.mem.count(u8, h.client.text(), "481 "));

    // Right both.
    try h.client.send("AUTHINFO USER alice");
    try h.awaitText("381 enter password");
    try h.client.send("AUTHINFO PASS s3cret");
    try h.awaitText("281 authentication accepted");
    try h.client.send("BODY <g@h>");
    try h.awaitText("222 0 <g@h>");
    try t.expectEqual(@as(usize, 1), h.server.served);
}

test "with no credentials configured any AUTHINFO is accepted" {
    var h: Harness = undefined;
    try h.init(.{});
    defer h.deinit();

    try h.client.send("AUTHINFO USER whoever");
    try h.awaitText("381 enter password");
    try h.client.send("AUTHINFO PASS whatever");
    try h.awaitText("281 authentication accepted");
}

test "missing fraction is deterministic across servers and across retries" {
    // Two independently-started servers with the same salt must drop
    // exactly the same ids, and asking twice must give the same answer.
    // Without that, a repair test is a coin flip.
    var loop: reactor.Loop = undefined;
    try loop.init(t.allocator);
    defer loop.deinit();

    var a: Server = undefined;
    _ = try a.start(t.allocator, &loop, .{ .missing_fraction = 0.4, .missing_salt = 7 });
    defer a.deinit();
    var b: Server = undefined;
    _ = try b.start(t.allocator, &loop, .{ .missing_fraction = 0.4, .missing_salt = 7 });
    defer b.deinit();

    var buf: [64]u8 = undefined;
    var dropped: usize = 0;
    for (0..1000) |i| {
        const id = try std.fmt.bufPrint(&buf, "seg{d:0>4}@hoardarr", .{i});
        const first = a.wouldDrop(id);
        try t.expectEqual(first, a.wouldDrop(id));
        try t.expectEqual(first, b.wouldDrop(id));
        if (first) dropped += 1;
    }
    // Uniform enough to plan a repair around: 40% ± 5 points.
    try t.expect(dropped > 350 and dropped < 450);

    // A different salt keeps the rate but moves the set.
    var c: Server = undefined;
    _ = try c.start(t.allocator, &loop, .{ .missing_fraction = 0.4, .missing_salt = 8 });
    defer c.deinit();
    var differing: usize = 0;
    for (0..1000) |i| {
        const id = try std.fmt.bufPrint(&buf, "seg{d:0>4}@hoardarr", .{i});
        if (a.wouldDrop(id) != c.wouldDrop(id)) differing += 1;
    }
    try t.expect(differing > 100);
}

test "missing fraction of 0 and 1 are the degenerate cases" {
    var loop: reactor.Loop = undefined;
    try loop.init(t.allocator);
    defer loop.deinit();

    var s: Server = undefined;
    _ = try s.start(t.allocator, &loop, .{});
    defer s.deinit();

    try t.expect(!s.wouldDrop("anything@h"));
    s.setMissingFraction(1);
    try t.expect(s.wouldDrop("anything@h"));
    s.setMissingFraction(-3);
    try t.expect(!s.wouldDrop("anything@h"));
    s.setMissingFraction(9);
    try t.expect(s.wouldDrop("anything@h"));
}

test "a dropped article answers 430 over the wire, consistently" {
    var h: Harness = undefined;
    try h.init(.{ .missing_fraction = 1 });
    defer h.deinit();

    try h.server.addArticle("present@h", "body\r\n");
    try h.client.send("BODY <present@h>");
    try h.awaitText("430 no such article <present@h>");
    try t.expect(h.server.hasArticle("present@h"));

    // Retrying gets the same answer, not a fresh roll.
    try h.client.send("BODY <present@h>");
    try pumpFor(&h.loop, 30);
    try t.expectEqual(@as(usize, 2), std.mem.count(u8, h.client.text(), "430 "));

    // Turning the knob down restores it.
    h.server.setMissingFraction(0);
    try h.client.send("BODY <present@h>");
    try h.awaitText("222 0 <present@h>");
}

test "throttling actually slows delivery" {
    var h: Harness = undefined;
    try h.init(.{});
    defer h.deinit();

    const body = try t.allocator.alloc(u8, 40_000);
    defer t.allocator.free(body);
    @memset(body, 'x');
    try h.server.addArticle("big@h", body);

    // Unthrottled first, as the control.
    var start = sys.monotonicNanos();
    try h.client.send("BODY <big@h>");
    try h.awaitText("\r\n.\r\n");
    const fast_ns = sys.monotonicNanos() - start;

    // 40 KB at 100 KB/s cannot complete in under ~400 ms.
    h.server.setBytesPerSec(100_000);
    h.client.clear();
    start = sys.monotonicNanos();
    try h.client.send("BODY <big@h>");
    try h.awaitText("\r\n.\r\n");
    const slow_ns = sys.monotonicNanos() - start;

    try t.expect(slow_ns > 250 * std.time.ns_per_ms);
    try t.expect(slow_ns > fast_ns * 4);
    try t.expectEqual(@as(usize, 2), h.server.served);
}

test "throttling still delivers the body byte-exactly" {
    var h: Harness = undefined;
    try h.init(.{ .bytes_per_sec = 200_000 });
    defer h.deinit();

    var prng = std.Random.DefaultPrng.init(0xBEEF);
    const payload = try t.allocator.alloc(u8, 6000);
    defer t.allocator.free(payload);
    prng.random().bytes(payload);

    const article = try fixture.encodeArticle(t.allocator, "chunk.bin", payload, 128);
    defer t.allocator.free(article);
    try h.server.addArticle("chunked@h", article);

    h.client.clear();
    try h.client.send("BODY <chunked@h>");
    try h.awaitText("\r\n.\r\n");

    const rx = h.client.text();
    const nl = std.mem.indexOf(u8, rx, "\r\n").? + 2;
    const end = std.mem.indexOf(u8, rx[nl..], "\r\n.\r\n").? + nl + 2;
    var decoded = try yenc.decode(t.allocator, rx[nl..end]);
    defer decoded.deinit(t.allocator);
    try t.expectEqualSlices(u8, payload, decoded.payload);
}

test "article latency delays the response line" {
    var h: Harness = undefined;
    try h.init(.{ .article_latency_ns = 120 * std.time.ns_per_ms });
    defer h.deinit();

    try h.server.addArticle("slow@h", "body\r\n");
    const start = sys.monotonicNanos();
    try h.client.send("BODY <slow@h>");
    try h.awaitText("222 0 <slow@h>");
    try t.expect(sys.monotonicNanos() - start >= 100 * std.time.ns_per_ms);

    h.server.setArticleLatency(0);
    const start2 = sys.monotonicNanos();
    try h.client.send("BODY <slow@h>");
    try h.awaitText("\r\n.\r\n");
    try t.expect(sys.monotonicNanos() - start2 < 100 * std.time.ns_per_ms);
}

test "latency applies to a missing article too" {
    var h: Harness = undefined;
    try h.init(.{ .article_latency_ns = 60 * std.time.ns_per_ms, .missing_fraction = 1 });
    defer h.deinit();

    try h.server.addArticle("m@h", "body\r\n");
    const start = sys.monotonicNanos();
    try h.client.send("BODY <m@h>");
    try h.awaitText("430 ");
    try t.expect(sys.monotonicNanos() - start >= 50 * std.time.ns_per_ms);
}

test "the connection cap refuses the extra connection and recovers after a close" {
    var loop: reactor.Loop = undefined;
    try loop.init(t.allocator);
    defer loop.deinit();

    var srv: Server = undefined;
    const p = try srv.start(t.allocator, &loop, .{ .max_connections = 2 });
    defer srv.deinit();

    var c1: Client = .{ .gpa = t.allocator };
    defer c1.deinit();
    var c2: Client = .{ .gpa = t.allocator };
    defer c2.deinit();
    var c3: Client = .{ .gpa = t.allocator };
    defer c3.deinit();

    try c1.connect(&loop, p);
    try c2.connect(&loop, p);
    try pumpUntil(&loop, 2000, &c2, struct {
        fn f(c: *Client) bool {
            return c.rx.items.len > 0;
        }
    }.f);
    try t.expect(containsStr(c1.text(), "200 hoardarr"));
    try t.expect(containsStr(c2.text(), "200 hoardarr"));
    try t.expectEqual(@as(usize, 2), srv.open);

    try c3.connect(&loop, p);
    try pumpUntil(&loop, 2000, &c3, struct {
        fn f(c: *Client) bool {
            return c.rx.items.len > 0;
        }
    }.f);
    try t.expect(containsStr(c3.text(), "502 too many connections"));
    try t.expectEqual(@as(usize, 1), srv.refused);

    // The refused connection is hung up on and never counted as open.
    try pumpFor(&loop, 30);
    try t.expectEqual(@as(usize, 2), srv.open);

    // Freeing a slot lets the next client in.
    c1.close();
    try pumpUntil(&loop, 2000, &srv, struct {
        fn f(s: *Server) bool {
            return s.open < 2;
        }
    }.f);

    var c4: Client = .{ .gpa = t.allocator };
    defer c4.deinit();
    try c4.connect(&loop, p);
    try pumpUntil(&loop, 2000, &c4, struct {
        fn f(c: *Client) bool {
            return c.rx.items.len > 0;
        }
    }.f);
    try t.expect(containsStr(c4.text(), "200 hoardarr"));
    try t.expectEqual(@as(usize, 1), srv.refused);
}

test "addArticle copies, replaces, and reset clears" {
    var loop: reactor.Loop = undefined;
    try loop.init(t.allocator);
    defer loop.deinit();

    var srv: Server = undefined;
    _ = try srv.start(t.allocator, &loop, .{});
    defer srv.deinit();

    var scratch = [_]u8{ 'a', 'b', 'c' };
    try srv.addArticle("k@h", &scratch);
    scratch[0] = 'z';
    try t.expectEqualStrings("abc", srv.lookup("k@h").?);

    try srv.addArticle("k@h", "replaced");
    try t.expectEqualStrings("replaced", srv.lookup("k@h").?);
    try t.expectEqual(@as(usize, 1), srv.articleCount());

    try srv.addArticle("j@h", "other");
    const ids = try srv.articles(t.allocator);
    defer t.allocator.free(ids);
    try t.expectEqual(@as(usize, 2), ids.len);

    srv.reset();
    try t.expectEqual(@as(usize, 0), srv.articleCount());
    try t.expect(!srv.hasArticle("k@h"));
}

test "the whole fixture corpus round-trips through the server" {
    // The real integration point: everything the fixture generated is
    // registered, fetched over the wire, and decodes back to the exact
    // file bytes. If the dot-stuffing, the framing or the encoder were
    // wrong, this is where it shows.
    var fx = try fixture.generate(t.allocator, .{
        .name = "wire.test",
        .file_count = 2,
        .file_size = 5000,
        .article_size = 2048,
        .par2_slice_size = 1024,
        .recovery_slices = 1,
    });
    defer fx.deinit();

    var h: Harness = undefined;
    try h.init(.{});
    defer h.deinit();

    for (fx.articles) |art| try h.server.addArticle(art.message_id, art.body);
    try t.expectEqual(fx.articles.len, h.server.articleCount());

    for (fx.files) |f| {
        const rebuilt = try t.allocator.alloc(u8, f.bytes.len);
        defer t.allocator.free(rebuilt);
        @memset(rebuilt, 0xA5);

        for (fx.articles) |art| {
            if (!std.mem.eql(u8, art.filename, f.name)) continue;

            h.client.clear();
            var buf: [256]u8 = undefined;
            try h.client.send(try std.fmt.bufPrint(&buf, "BODY <{s}>", .{art.message_id}));
            try h.awaitText("\r\n.\r\n");

            const rx = h.client.text();
            try t.expect(std.mem.startsWith(u8, rx, "222 "));
            const body_start = std.mem.indexOf(u8, rx, "\r\n").? + 2;
            const body_end = std.mem.indexOf(u8, rx[body_start..], "\r\n.\r\n").? + body_start + 2;

            var decoded = try yenc.decode(t.allocator, rx[body_start..body_end]);
            defer decoded.deinit(t.allocator);
            const off: usize = @intCast(decoded.header.begin - 1);
            @memcpy(rebuilt[off..][0..decoded.payload.len], decoded.payload);
        }
        try t.expectEqualSlices(u8, f.bytes, rebuilt);
    }
    try t.expectEqual(fx.articles.len, h.server.served);
}

test "a lossy server drops the same fixture articles every run" {
    // The combination the repair tests depend on: a fixed fraction of a
    // known corpus, identical on every run, and small enough that the
    // fixture's recovery slices cover it.
    var fx = try fixture.generate(t.allocator, .{
        .name = "lossy.test",
        .file_count = 1,
        .file_size = 8000,
        .article_size = 1000,
        .par2_slice_size = 1000,
        .recovery_slices = 4,
    });
    defer fx.deinit();

    var h: Harness = undefined;
    try h.init(.{ .missing_fraction = 0.25, .missing_salt = 3 });
    defer h.deinit();
    for (fx.articles) |art| try h.server.addArticle(art.message_id, art.body);

    var expected: std.ArrayList([]const u8) = .empty;
    defer expected.deinit(t.allocator);
    for (fx.articles) |art| {
        if (h.server.wouldDrop(art.message_id)) try expected.append(t.allocator, art.message_id);
    }
    try t.expect(expected.items.len > 0);
    try t.expect(expected.items.len < fx.articles.len);

    // And the wire agrees with the prediction.
    for (fx.articles) |art| {
        h.client.clear();
        var buf: [256]u8 = undefined;
        try h.client.send(try std.fmt.bufPrint(&buf, "BODY <{s}>", .{art.message_id}));

        const predicted = h.server.wouldDrop(art.message_id);
        try h.awaitText(if (predicted) "430 " else "\r\n.\r\n");
        try t.expectEqual(predicted, containsStr(h.client.text(), "430 "));
    }
}

test "pipelined commands are answered in order" {
    var h: Harness = undefined;
    try h.init(.{});
    defer h.deinit();

    try h.server.addArticle("p1@h", "one\r\n");
    try h.server.addArticle("p2@h", "two\r\n");

    // All in one write, so the server sees them in a single read.
    try h.client.stream.write("BODY <p1@h>\r\nBODY <p2@h>\r\nSTAT <p1@h>\r\n");
    try h.awaitText("223 0 <p1@h>");

    const rx = h.client.text();
    const first = std.mem.indexOf(u8, rx, "222 0 <p1@h>").?;
    const second = std.mem.indexOf(u8, rx, "222 0 <p2@h>").?;
    const third = std.mem.indexOf(u8, rx, "223 0 <p1@h>").?;
    try t.expect(first < second and second < third);
}

test "an over-long command line is rejected without wedging the session" {
    var h: Harness = undefined;
    try h.init(.{});
    defer h.deinit();

    // Terminated, so the discard that follows the 501 stops at this
    // line's CRLF instead of swallowing the next real command.
    const junk = try t.allocator.alloc(u8, max_line * 2 + 16);
    defer t.allocator.free(junk);
    @memset(junk, 'A');
    junk[junk.len - 2] = '\r';
    junk[junk.len - 1] = '\n';
    try h.client.stream.write(junk);
    try h.awaitText("501 command line too long");

    try h.server.addArticle("after@h", "still here\r\n");
    try h.client.send("BODY <after@h>");
    try h.awaitText("222 0 <after@h>");
}

test "a command with no message-id is a 501" {
    var h: Harness = undefined;
    try h.init(.{});
    defer h.deinit();

    try h.client.send("BODY");
    try h.awaitText("501 bad command");
    try h.client.send("BODY <>");
    try pumpFor(&h.loop, 20);
    try t.expectEqual(@as(usize, 2), std.mem.count(u8, h.client.text(), "501 "));
}

test "stop tears down live connections without leaking" {
    var loop: reactor.Loop = undefined;
    try loop.init(t.allocator);
    defer loop.deinit();

    var srv: Server = undefined;
    const p = try srv.start(t.allocator, &loop, .{});

    var c: Client = .{ .gpa = t.allocator };
    defer c.deinit();
    try c.connect(&loop, p);
    try pumpUntil(&loop, 2000, &c, struct {
        fn f(x: *Client) bool {
            return x.rx.items.len > 0;
        }
    }.f);
    try srv.addArticle("live@h", "body\r\n");

    // Stop mid-session, with a registered corpus and an open connection.
    srv.stop();
    try t.expectEqual(@as(usize, 0), srv.articleCount());
    // A second stop must be harmless — deinit is the same call.
    srv.deinit();

    try pumpFor(&loop, 20);
}

test "parseMessageId takes the shape hoardarr sends and refuses the rest" {
    try t.expectEqualStrings("a@b", parseMessageId("<a@b>").?);
    try t.expect(parseMessageId("") == null);
    try t.expect(parseMessageId("<>") == null);
    try t.expect(parseMessageId(">a@b<") == null);
    try t.expect(parseMessageId("123") == null);
    // The shapes a lenient scan would have accepted: junk around the id,
    // and two commands run together by a client that forgot its CRLF.
    try t.expect(parseMessageId("junk <a@b>") == null);
    try t.expect(parseMessageId("<a@b> junk") == null);
    try t.expect(parseMessageId("<a@b> BODY <c@d>") == null);
    try t.expect(parseMessageId("<a>b@c>") == null);
}

test "lineHasVerb matches whole verbs only" {
    try t.expect(lineHasVerb("BODY <a@h>", "BODY"));
    try t.expect(lineHasVerb("body <a@h>", "BODY"));
    try t.expect(lineHasVerb("QUIT", "QUIT"));
    try t.expect(lineHasVerb("AUTHINFO USER x", "AUTHINFO USER"));
    try t.expect(!lineHasVerb("AUTHINFO PASS x", "AUTHINFO USER"));
    try t.expect(!lineHasVerb("BODYGUARD <a@h>", "BODY"));
    try t.expect(!lineHasVerb("MODEREADER", "MODE"));
}

test "a verb that only looks like a command is refused, not served" {
    var h: Harness = undefined;
    try h.init(.{});
    defer h.deinit();

    try h.server.addArticle("a@h", "body\r\n");

    // The leniency this closes: a prefix match answered `BODYGUARD` with
    // the article, so a client whose command was truncated or corrupted
    // on the way out still saw a 222 and a body.
    try h.client.send("BODYGUARD <a@h>");
    try h.awaitText("500 unknown command");
    try t.expectEqual(@as(usize, 0), h.server.served);

    // A retrieval with junk around the id is a 501, not a fetch.
    try h.client.send("BODY <a@h> BODY <a@h>");
    try h.awaitText("501 bad command");
    try t.expectEqual(@as(usize, 0), h.server.served);

    // An unimplemented MODE gets 501, never the reader-mode answer.
    try h.client.send("MODE STREAM");
    try h.awaitText("501 unsupported MODE");

    try t.expectEqual(@as(usize, 3), h.server.rejected);
    // And the whole exchange is on the record, with none of it accepted.
    try t.expectEqual(@as(usize, 3), h.server.received.items.len);
    for (h.server.received.items) |r| try t.expect(!r.accepted);

    // The session is still usable: strictness refuses a command, it does
    // not poison the connection.
    try h.client.send("BODY <a@h>");
    try h.awaitText("222 0 <a@h>");
}

test "AUTHINFO PASS before AUTHINFO USER is out of sequence" {
    var h: Harness = undefined;
    try h.init(.{ .username = "alice", .password = "s3cret" });
    defer h.deinit();

    try h.server.addArticle("g@h", "body\r\n");

    // Out of order: the old server matched the empty username buffer
    // against the configured one and answered 481, which reads like a
    // credential problem rather than a client that skipped a step.
    try h.client.send("AUTHINFO PASS s3cret");
    try h.awaitText("482 authentication commands out of sequence");
    try h.client.send("BODY <g@h>");
    try h.awaitText("480 authentication required");

    // In order, it works — and PASS is spent, so a second one without a
    // fresh USER is out of sequence again.
    try h.client.send("AUTHINFO USER alice");
    try h.awaitText("381 enter password");
    try h.client.send("AUTHINFO PASS s3cret");
    try h.awaitText("281 authentication accepted");
    try h.client.send("AUTHINFO PASS s3cret");
    try h.awaitText("482 ");

    try h.client.send("AUTHINFO SASL PLAIN");
    try h.awaitText("501 unsupported AUTHINFO variant");
}

test "the transcript records what the client sent, per connection" {
    var loop: reactor.Loop = undefined;
    try loop.init(t.allocator);
    defer loop.deinit();

    var srv: Server = undefined;
    const p = try srv.start(t.allocator, &loop, .{});
    defer srv.deinit();
    try srv.addArticle("one@h", "1\r\n");
    try srv.addArticle("two@h", "2\r\n");

    var c1: Client = .{ .gpa = t.allocator };
    defer c1.deinit();
    var c2: Client = .{ .gpa = t.allocator };
    defer c2.deinit();
    try c1.connect(&loop, p);
    try c2.connect(&loop, p);
    try pumpUntil(&loop, 2000, &c2, struct {
        fn f(c: *Client) bool {
            return c.rx.items.len > 0;
        }
    }.f);

    try c1.send("MODE READER");
    try c1.send("BODY <one@h>");
    try c2.send("BODY <two@h>");
    try c2.send("BODY <two@h>");
    try pumpUntil(&loop, 2000, &srv, struct {
        fn f(s: *Server) bool {
            return s.served == 3;
        }
    }.f);

    // Each connection's own conversation, in order.
    const first = try srv.commandSequence(t.allocator, 1);
    defer t.allocator.free(first);
    try t.expectEqual(@as(usize, 2), first.len);
    try t.expectEqualStrings("MODE READER", first[0]);
    try t.expectEqualStrings("BODY <one@h>", first[1]);

    const second = try srv.commandSequence(t.allocator, 2);
    defer t.allocator.free(second);
    try t.expectEqual(@as(usize, 2), second.len);

    try t.expectEqual(@as(usize, 1), srv.countVerb("MODE READER"));
    try t.expectEqual(@as(usize, 3), srv.countVerb("BODY"));
    try t.expect(!srv.sawVerb("AUTHINFO"));

    try t.expectEqual(@as(usize, 1), srv.requestCount("one@h"));
    try t.expectEqual(@as(usize, 2), srv.requestCount("two@h"));
    try t.expect(!srv.wasRequested("three@h"));

    const ids = try srv.requestedIds(t.allocator);
    defer t.allocator.free(ids);
    try t.expectEqual(@as(usize, 2), ids.len);
    try t.expectEqualStrings("one@h", ids[0]);
    try t.expectEqualStrings("two@h", ids[1]);
}

test "dot stuffing matches the transport rules" {
    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(t.allocator);

    try appendDotStuffed(&buf, t.allocator, ".x\r\ny\r\n");
    try t.expectEqualStrings("..x\r\ny\r\n", buf.items);

    buf.clearRetainingCapacity();
    try appendDotStuffed(&buf, t.allocator, "");
    try t.expectEqualStrings("", buf.items);

    buf.clearRetainingCapacity();
    try appendDotStuffed(&buf, t.allocator, ".");
    try t.expectEqualStrings("..\r\n", buf.items);
}

test "the DATE stamp is a plausible UTC timestamp" {
    const s = utcStamp();
    try t.expectEqual(@as(usize, 14), s.len);
    for (s) |c| try t.expect(std.ascii.isDigit(c));
    const year = try std.fmt.parseInt(u16, s[0..4], 10);
    try t.expect(year >= 2024 and year < 2200);
    const month = try std.fmt.parseInt(u8, s[4..6], 10);
    try t.expect(month >= 1 and month <= 12);
}
