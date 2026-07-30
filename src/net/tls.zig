//! TLS client on top of the reactor.
//!
//! ## The problem this solves
//!
//! `std.crypto.tls.Client.init` performs a whole TLS handshake — multiple
//! network round trips — inside one synchronous call, pulling from a
//! `std.Io.Reader` and pushing to a `std.Io.Writer`. Our sockets are
//! non-blocking and driven by a single-threaded reactor, so a reader that
//! reached the end of what the kernel had would have to answer "would
//! block", and `init` cannot be resumed from there.
//!
//! So we give it a stack. `Transport` below is a `Reader`/`Writer` pair
//! over a raw fd whose fill and drain functions, on `error.WouldBlock`,
//! call `Fiber.park` — registering reactor interest and context-switching
//! back to the event loop until the fd is ready. From inside the fiber the
//! code looks blocking; from the loop's point of view nothing blocks. See
//! `posix/fiber.zig`.
//!
//! ## Consequence: the whole session lives in the fiber
//!
//! This is the part that shapes the API. It is not only the handshake that
//! parks — the plaintext `Reader` and `Writer` that `Client` hands back
//! after `init` sit on top of the same transport, so *every* TLS read and
//! write can park too. Calling one from outside the fiber would park a
//! stack that has nowhere to switch to.
//!
//! Therefore a `Conn` takes a session function and runs it on the fiber:
//!
//!     try conn.init(gpa, loop, fd, session, my_ctx, fiber.default_stack_size);
//!     conn.on_finished = onFinished;
//!     conn.start();
//!
//!     fn session(conn: *tls.Conn) void {
//!         conn.handshake(.{
//!             .host = "news.example.com",
//!             .trust = .{ .ca_bundle = bundle },
//!             .read_buffer = &read_buf,
//!             .write_buffer = &write_buf,
//!         }) catch |err| {
//!             log.warn("tls handshake failed: {t} ({?})", .{ err, conn.detail });
//!             return;
//!         };
//!         const w = conn.writer();
//!         w.writeAll("AUTHINFO USER bob\r\n") catch return;
//!         w.flush() catch return;
//!         const line = conn.reader().takeDelimiterExclusive('\n') catch return;
//!         ...
//!     }
//!
//! Everything in there blocks; none of it blocks the loop.
//!
//! ## Why this does not embed `socket.Stream`
//!
//! `Stream` is the right transport for callback-shaped protocols and it is
//! where the non-blocking write queue and the writable-storm avoidance
//! live. It cannot be reused here, for a concrete reason: a `Stream` owns
//! a `reactor.Source` for its fd, and so does `Fiber`. Registering the
//! same fd twice is not merely redundant — `epoll_ctl(ADD)` rejects it
//! with `EEXIST`, and the reactor's epoll backend is what ships. Exactly
//! one thing may own readiness for an fd, and for a fibered connection
//! that has to be the fiber, because `park` is the only thing that can
//! wake it.
//!
//! What `Stream` does *for* us is not needed either: its outbound queue
//! exists so a short write can return to the loop without blocking, and a
//! fiber solves the same problem by parking instead. So this module uses
//! the same `sys` primitives `Stream` uses, minus the queue.
//!
//! ## Certificate trust
//!
//! `Options.trust` has no default. Every call site has to say, in words,
//! which of the three trust models it wants, and the one that skips
//! verification is called `insecure_skip_verification_dangerous` so that
//! it cannot appear in a diff without being noticed. There is deliberately
//! no way to end up unverified by omission.

const std = @import("std");
const sys = @import("../posix/sys.zig");
const reactor = @import("../posix/reactor.zig");
const fiber_mod = @import("../posix/fiber.zig");
const socket = @import("socket.zig");

const tls = std.crypto.tls;
const Reader = std.Io.Reader;
const Writer = std.Io.Writer;
const Limit = std.Io.Limit;
const Fiber = fiber_mod.Fiber;
const Allocator = std.mem.Allocator;
const Fd = sys.Fd;
const assert = std.debug.assert;

pub const Error = socket.Error;

/// Minimum size of `Options.read_buffer`. `Client` asserts it, because a
/// TLS record has to be processed contiguously and this is the largest one
/// the protocol allows.
pub const min_read_buffer: usize = tls.Client.min_buffer_len;

/// A comfortable write buffer. TLS fragments anything larger, so this is a
/// throughput knob rather than a correctness one.
pub const default_write_buffer: usize = 16 * 1024;

pub const entropy_len = tls.Client.Options.entropy_len;

// ---------------------------------------------------------------------
// Transport: a parking Reader/Writer pair over one fd
// ---------------------------------------------------------------------

/// The bridge. Useful on its own for any blocking-shaped protocol that
/// wants to run on the reactor, not just TLS.
///
/// Detailed errors are recorded in `read_error` / `write_error` rather than
/// returned, because `std.Io.Reader` and `std.Io.Writer` are only allowed
/// to report `ReadFailed` / `WriteFailed`. That is the std convention; the
/// fields are how a caller finds out what actually happened.
pub const Transport = struct {
    fiber: *Fiber,
    fd: Fd,
    reader: Reader,
    writer: Writer,

    read_error: ?Error = null,
    write_error: ?Error = null,
    /// The peer closed its side cleanly. Distinguishes a graceful EOF from
    /// a reset, which matters because a TLS stream that ends without
    /// `close_notify` is a possible truncation attack.
    at_end: bool = false,
    /// A park was resumed with `error.Canceled` rather than by readiness.
    /// Kept separate from `read_error` so a shutdown does not get reported
    /// as a network fault: `std.Io` collapses both into `ReadFailed`, and
    /// "we asked it to stop" is not a provider problem.
    canceled: bool = false,

    /// Bytes actually moved through the socket, before encryption. Cheap
    /// to maintain and the only place a caller can see it: everything
    /// above this is ciphertext.
    bytes_in: u64 = 0,
    bytes_out: u64 = 0,
    /// How many times each direction had to park. A connection that parks
    /// once per byte is a symptom worth being able to see.
    read_parks: u64 = 0,
    write_parks: u64 = 0,

    pub fn init(fiber: *Fiber, fd: Fd, read_buffer: []u8, write_buffer: []u8) Transport {
        return .{
            .fiber = fiber,
            .fd = fd,
            .reader = .{
                .vtable = &.{
                    .stream = readerStream,
                    .readVec = readerReadVec,
                },
                .buffer = read_buffer,
                .seek = 0,
                .end = 0,
            },
            .writer = .{
                .vtable = &.{ .drain = writerDrain },
                .buffer = write_buffer,
                .end = 0,
            },
        };
    }

    // -- reading ------------------------------------------------------

    fn readerReadVec(r: *Reader, data: [][]u8) Reader.Error!usize {
        const self: *Transport = @alignCast(@fieldParentPtr("reader", r));

        // Prefer the reader's own buffer. `Reader.fill` calls us with an
        // empty `data[0]` after guaranteeing free capacity, and TLS needs
        // whole records contiguous, so this is both the common and the
        // desirable case. Returning 0 having advanced `end` is explicitly
        // allowed by the vtable contract.
        if (r.end < r.buffer.len) {
            r.end += try self.fill(r.buffer[r.end..]);
            return 0;
        }
        for (data) |slice| {
            if (slice.len != 0) return try self.fill(slice);
        }
        // Nowhere to put anything: the buffer is full and every
        // destination was empty. The caller has to consume or rebase.
        unreachable;
    }

    fn readerStream(r: *Reader, w: *Writer, limit: Limit) Reader.StreamError!usize {
        const self: *Transport = @alignCast(@fieldParentPtr("reader", r));
        const dest = limit.slice(try w.writableSliceGreedy(1));
        const n = try self.fill(dest);
        w.advance(n);
        return n;
    }

    /// One logical read, parking as many times as it takes.
    fn fill(self: *Transport, dest: []u8) Reader.Error!usize {
        assert(dest.len != 0);
        while (true) {
            const n = sys.read(self.fd, dest) catch |err| switch (err) {
                error.WouldBlock => {
                    if (!self.parkFor(.readable, &self.read_error, &self.read_parks)) {
                        return error.ReadFailed;
                    }
                    if (self.at_end) return error.EndOfStream;
                    continue;
                },
                else => |e| {
                    self.read_error = e;
                    return error.ReadFailed;
                },
            };
            if (n == 0) {
                self.at_end = true;
                return error.EndOfStream;
            }
            self.bytes_in += n;
            return n;
        }
    }

    // -- writing ------------------------------------------------------

    fn writerDrain(w: *Writer, data: []const []const u8, splat: usize) Writer.Error!usize {
        const self: *Transport = @alignCast(@fieldParentPtr("writer", w));

        // Buffered bytes were logically written when they were buffered,
        // so they go out first and are excluded from the return value.
        try self.push(w.buffer[0..w.end]);
        w.end = 0;

        const head = data[0 .. data.len - 1];
        const pattern = data[head.len];
        var written: usize = 0;
        for (head) |bytes| {
            try self.push(bytes);
            written += bytes.len;
        }
        if (pattern.len != 0) {
            for (0..splat) |_| try self.push(pattern);
        }
        // The pattern counts once per repetition even though it is one
        // slice: that is what the vtable contract asks for, and what
        // `Writer.Discarding` does.
        return written + pattern.len * splat;
    }

    /// Write every byte or fail, parking as many times as it takes.
    ///
    /// Looping here is exactly what `socket.Stream` must not do — but that
    /// is because `Stream` runs on the loop's stack, and this runs on the
    /// fiber's. The loop is free the entire time we are parked.
    fn push(self: *Transport, bytes: []const u8) Writer.Error!void {
        var rest = bytes;
        while (rest.len != 0) {
            const n = sys.write(self.fd, rest) catch |err| switch (err) {
                error.WouldBlock => {
                    if (!self.parkFor(.writable, &self.write_error, &self.write_parks)) {
                        return error.WriteFailed;
                    }
                    if (self.at_end) {
                        self.write_error = error.BrokenPipe;
                        return error.WriteFailed;
                    }
                    continue;
                },
                else => |e| {
                    self.write_error = e;
                    return error.WriteFailed;
                },
            };
            if (n == 0) {
                self.write_error = error.BrokenPipe;
                return error.WriteFailed;
            }
            self.bytes_out += n;
            rest = rest[n..];
        }
    }

    // -- parking ------------------------------------------------------

    /// Park until the fd is ready in `interest`.
    ///
    /// Returns false on failure, with the reason left in `err_slot`; the
    /// caller turns that into `ReadFailed` or `WriteFailed` because those
    /// are the only things `std.Io` lets it say. A bool rather than an
    /// error union so one implementation serves both directions.
    ///
    /// The terminal handling is what stops a dead socket from becoming an
    /// infinite park/read loop: a hung-up or errored fd is reported ready
    /// on every single tick, so treating `hup`/`err` as "try the syscall
    /// again" would spin the loop at 100% forever.
    fn parkFor(
        self: *Transport,
        interest: reactor.Interest,
        err_slot: *?Error,
        park_count: *u64,
    ) bool {
        const ready = self.fiber.park(self.fd, interest) catch |err| {
            switch (err) {
                error.Canceled => {
                    self.canceled = true;
                    err_slot.* = error.Interrupted;
                },
                else => |e| err_slot.* = e,
            }
            return false;
        };
        park_count.* += 1;

        if (ready.err) {
            // SO_ERROR is the only thing that says *why*.
            sys.socketError(self.fd) catch |e| {
                err_slot.* = e;
                return false;
            };
            err_slot.* = error.ConnectionReset;
            return false;
        }
        // A half-close with nothing left to read is an ordinary end of
        // stream; the readable flag would be set if bytes remained.
        if (ready.hup and !ready.read) self.at_end = true;
        return true;
    }
};

// ---------------------------------------------------------------------
// Trust configuration
// ---------------------------------------------------------------------

/// How to decide whether the peer is who it claims to be.
///
/// There is no default. Picking one is a security decision and the type
/// system is the cheapest place to force it to be made explicitly.
pub const Trust = union(enum) {
    /// Verify the chain against a CA bundle supplied by the caller.
    ///
    /// The bundle is injected rather than loaded here on purpose: the
    /// shipping container has no `/etc/ssl/certs` to read, so the roots
    /// have to be compiled into the binary. The build step that embeds
    /// them is not wired up yet, so today the only caller that can use
    /// this is one that has a bundle from somewhere else.
    ca_bundle: CaBundle,

    /// Accept a self-signed certificate, provided it was issued for the
    /// host we asked for. Authenticates nothing — anyone can self-sign —
    /// but it does pin the connection to one key for its lifetime, which
    /// is why it exists separately from skipping verification entirely.
    self_signed_only,

    /// Skip certificate *and* hostname verification.
    ///
    /// This makes the connection trivially interceptable by anyone on the
    /// path. It exists for talking to a local test endpoint and for
    /// nothing else. The name is long and unpleasant so that it cannot
    /// appear in a code review without being noticed, and so that
    /// `grep -rn insecure src/` finds every use.
    ///
    /// Note one non-obvious consequence: `std.crypto.tls.Client` ties SNI
    /// and hostname verification to the same option, and omits the SNI
    /// extension entirely when verification is off. So this mode does not
    /// send a server name, and cannot reach a provider that shares an
    /// address with other hosts. Another reason it is only good for
    /// pointing at something local. Use `self_signed_only` if you need SNI
    /// without a CA bundle.
    insecure_skip_verification_dangerous,
};

/// The pieces `std.crypto.tls.Client` needs in order to use a CA bundle.
///
/// `io` and `lock` are passed straight through from std's own option
/// struct rather than being hidden: std guards the bundle with an
/// `std.Io.RwLock`, which needs an `std.Io` to block on. This project has
/// no `std.Io` implementation — that is the entire reason `posix/reactor.zig`
/// exists — so whoever wires up the embedded bundle has to decide what to
/// supply, and pretending otherwise here would just move the surprise.
pub const CaBundle = struct {
    gpa: Allocator,
    bundle: *std.crypto.Certificate.Bundle,
    lock: *std.Io.RwLock,
    io: std.Io,
};

// ---------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------

/// Handshake outcomes, split so a user debugging a provider connection
/// learns something from the message.
///
/// `std.crypto.tls.Client.InitError` has around sixty members; collapsing
/// them all into one `TlsFailed` would be useless, and passing them
/// through would leak std's internals into every log line. These are the
/// distinctions that change what someone does next. The exact underlying
/// error is kept in `Conn.detail`.
pub const HandshakeError = error{
    /// The chain did not validate against the configured trust anchors.
    /// Either the wrong roots are embedded, or something is intercepting.
    CertificateNotTrusted,
    /// The chain validates but was not issued for the host we asked for.
    /// Almost always a misconfigured hostname, not an attack.
    CertificateHostMismatch,
    /// Expired — or the container's clock is wrong, which is at least as
    /// likely and looks identical from here.
    CertificateExpired,
    /// Not valid yet. In practice: the clock is wrong.
    CertificateNotYetValid,
    /// The certificate could not be parsed. A broken or non-TLS peer.
    CertificateMalformed,
    /// The peer refused the handshake and said why. See `Conn.alert`.
    TlsAlert,
    /// Protocol-level failure: a malformed record, an unexpected message,
    /// no cipher suite in common.
    TlsProtocolError,
    /// The socket failed, or the peer went away mid-handshake. See
    /// `Conn.transport.read_error` / `.write_error`.
    TransportFailed,
    /// Our own configuration is wrong — a buffer too small, entropy that
    /// isn't. A bug on this side, not the peer's.
    Misconfigured,
    /// The fiber was cancelled: shutdown, or a deadline the owner armed.
    Canceled,
    OutOfMemory,
};

/// Collapse std's `InitError` into `HandshakeError`.
fn mapInitError(err: tls.Client.InitError) HandshakeError {
    return switch (err) {
        error.CertificateHostMismatch => error.CertificateHostMismatch,
        error.CertificateExpired => error.CertificateExpired,
        error.CertificateNotYetValid => error.CertificateNotYetValid,
        error.TlsAlert => error.TlsAlert,

        // Everything that means "the chain exists but does not establish
        // trust". Signature checks, issuer mismatches, and unusable public
        // keys all land here: from the operator's point of view they are
        // one problem.
        error.TlsCertificateNotVerified,
        error.CertificateIssuerMismatch,
        error.CertificateSignatureInvalid,
        error.CertificateSignatureInvalidLength,
        error.CertificateSignatureAlgorithmMismatch,
        error.CertificateSignatureAlgorithmUnsupported,
        error.CertificateSignatureNamedCurveUnsupported,
        error.CertificateSignatureUnsupportedBitCount,
        error.CertificatePublicKeyInvalid,
        error.SignatureVerificationFailed,
        error.InvalidSignature,
        error.WeakPublicKey,
        error.IdentityElement,
        error.NonCanonical,
        error.NotSquare,
        error.TlsBadSignatureScheme,
        error.TlsBadRsaSignatureBitCount,
        => error.CertificateNotTrusted,

        // The bytes are not a certificate we can make sense of.
        error.CertificateFieldHasInvalidLength,
        error.CertificateFieldHasWrongDataType,
        error.CertificateHasUnrecognizedObjectId,
        error.CertificateHasInvalidBitString,
        error.CertificateTimeInvalid,
        error.UnsupportedCertificateVersion,
        error.InvalidEncoding,
        error.NegativeIntoUnsigned,
        => error.CertificateMalformed,

        // Our side is set up wrong.
        error.InsufficientEntropy,
        error.BufferTooSmall,
        error.TargetTooSmall,
        error.MessageTooLong,
        => error.Misconfigured,

        // The transport gave up. The reason is on the `Transport`.
        error.ReadFailed,
        error.WriteFailed,
        error.NotOpenForWriting,
        error.DiskQuota,
        error.LockViolation,
        => error.TransportFailed,

        error.Canceled => error.Canceled,

        // Record-layer and state-machine failures. Left as a catch-all
        // deliberately: new members of std's error set are far more likely
        // to be protocol details than new categories, and a compile error
        // on every std upgrade buys nothing.
        else => error.TlsProtocolError,
    };
}

// ---------------------------------------------------------------------
// Conn
// ---------------------------------------------------------------------

pub const Options = struct {
    /// Hostname, used for SNI and (unless verification is skipped) for
    /// certificate matching. Required even in the insecure mode, because
    /// SNI is how a provider on a shared address routes the connection at
    /// all. Borrowed only for the duration of `handshake`.
    host: []const u8,
    /// No default. See `Trust`.
    trust: Trust,
    /// At least `min_read_buffer` bytes. Not owned.
    read_buffer: []u8,
    /// Not owned.
    write_buffer: []u8,
    /// 240 bytes of CSPRNG output. Null means "take it from the OS", which
    /// is what production wants; tests pass a fixed buffer when they need
    /// a reproducible ClientHello.
    entropy: ?*const [entropy_len]u8 = null,
    /// Wall clock, for certificate validity. Null means "ask the OS".
    /// Injectable so a test can pin a moment inside a fixture's validity
    /// window instead of depending on today's date.
    realtime_now: ?std.Io.Timestamp = null,
};

/// A TLS connection whose entire lifetime runs on a fiber.
///
/// Initialise in place: the fiber's forged stack frame stores a `*Fiber`
/// into this struct, the reactor stores `&conn.fiber.source`, and
/// `Transport`'s reader and writer are recovered by `@fieldParentPtr`.
/// None of that survives a move.
pub const Conn = struct {
    fiber: Fiber = undefined,
    transport: Transport = undefined,
    client: tls.Client = undefined,

    gpa: Allocator,
    loop: *reactor.Loop,
    fd: Fd = sys.invalid_fd,

    state: State = .idle,
    /// Populated when `handshake` returns `error.TlsAlert`. This is the
    /// peer's own explanation and the single most useful thing to log.
    alert: ?tls.Alert = null,
    /// The unmapped `std` error behind whatever `HandshakeError` came out.
    /// For logs only — branch on the `HandshakeError`, not on this.
    detail: ?anyerror = null,

    /// Runs on the fiber. Must call `handshake` before anything else.
    session: *const fn (conn: *Conn) void,
    /// Caller's data, for the session function.
    context: ?*anyopaque = null,
    /// Fires on the loop's stack once the session has returned. Safe to
    /// `deinit` and free the `Conn` from here.
    on_finished: ?*const fn (conn: *Conn) void = null,

    pub const State = enum {
        idle,
        handshaking,
        /// Handshake complete; plaintext streams usable.
        open,
        /// Handshake failed, or the session returned.
        closed,
    };

    /// Take ownership of an already-connected, non-blocking fd and set up
    /// the fiber. Nothing runs yet.
    ///
    /// Split from `start` so the caller can set `on_finished` first: the
    /// session begins executing the moment the fiber is entered, and may
    /// well finish before `start` returns.
    ///
    /// `deinit` closes `fd`.
    pub fn init(
        self: *Conn,
        gpa: Allocator,
        loop: *reactor.Loop,
        fd: Fd,
        session: *const fn (conn: *Conn) void,
        context: ?*anyopaque,
        stack_size: usize,
    ) Error!void {
        self.* = .{
            .gpa = gpa,
            .loop = loop,
            .fd = fd,
            .session = session,
            .context = context,
        };
        try self.fiber.init(gpa, loop, stack_size, run, self);
        self.fiber.on_finished = fiberFinished;
    }

    /// Enter the fiber. Returns once the session first parks — typically
    /// immediately, in the middle of the handshake — or finishes. From then
    /// on the reactor drives it.
    pub fn start(self: *Conn) void {
        assert(self.state == .idle);
        self.fiber.enter();
    }

    /// Release the fiber stack and close the socket.
    ///
    /// If the session is still parked this abandons it without unwinding —
    /// see `posix/fiber.zig`. To stop a live connection cleanly, `cancel`
    /// it first and let the session return.
    pub fn deinit(self: *Conn) void {
        self.fiber.deinit();
        if (self.fd != sys.invalid_fd) {
            sys.close(self.fd);
            self.fd = sys.invalid_fd;
        }
        self.state = .closed;
    }

    /// Resume the session with `error.Canceled` out of whatever it is
    /// parked on, so it unwinds through its own `defer`s.
    pub fn cancel(self: *Conn) void {
        self.fiber.cancel();
    }

    pub fn isDone(self: *const Conn) bool {
        return self.fiber.isDone();
    }

    /// Run the TLS handshake. Call this first, from inside the session.
    pub fn handshake(self: *Conn, options: Options) HandshakeError!void {
        assert(self.state == .idle);
        if (options.read_buffer.len < min_read_buffer) return error.Misconfigured;

        var gathered: [entropy_len]u8 = undefined;
        const entropy = options.entropy orelse blk: {
            osRandom(&gathered) catch return error.Misconfigured;
            break :blk &gathered;
        };

        const now = options.realtime_now orelse std.Io.Timestamp.fromNanoseconds(
            @intCast(sys.realtimeNanos()),
        );

        self.transport = .init(&self.fiber, self.fd, options.read_buffer, options.write_buffer);
        self.state = .handshaking;

        var alert: tls.Alert = undefined;
        self.client = tls.Client.init(&self.transport.reader, &self.transport.writer, .{
            .host = switch (options.trust) {
                // Hostname checking is only meaningful alongside chain
                // checking; std ties the two together and so do we.
                .insecure_skip_verification_dangerous => .no_verification,
                else => .{ .explicit = options.host },
            },
            .ca = switch (options.trust) {
                .ca_bundle => |ca| .{ .bundle = .{
                    .gpa = ca.gpa,
                    .io = ca.io,
                    .lock = ca.lock,
                    .bundle = ca.bundle,
                } },
                .self_signed_only => .self_signed,
                .insecure_skip_verification_dangerous => .no_verification,
            },
            .read_buffer = options.read_buffer,
            .write_buffer = options.write_buffer,
            .entropy = entropy,
            .realtime_now = now,
            .alert = &alert,
        }) catch |err| {
            self.state = .closed;
            self.detail = err;
            // Cancellation reaches us disguised as a transport failure,
            // because that is the only shape `std.Io.Reader` can report.
            // Undisguise it: "we shut this down" and "the provider dropped
            // us" want different log lines and different retry policy.
            if (self.transport.canceled) return error.Canceled;
            const mapped = mapInitError(err);
            if (mapped == error.TlsAlert) self.alert = alert;
            return mapped;
        };

        self.state = .open;
    }

    /// The plaintext stream from the server. Only valid after a successful
    /// `handshake`, and only from inside the session.
    pub fn reader(self: *Conn) *Reader {
        assert(self.state == .open);
        return &self.client.reader;
    }

    /// The plaintext stream to the server. Buffered — `flush` is what puts
    /// a record on the wire.
    pub fn writer(self: *Conn) *Writer {
        assert(self.state == .open);
        return &self.client.writer;
    }

    /// Send `close_notify` and flush. Without this the peer cannot tell an
    /// orderly shutdown from a truncation attack.
    pub fn close(self: *Conn) void {
        if (self.state != .open) return;
        self.client.end() catch {};
        self.state = .closed;
    }

    /// Whether the peer sent `close_notify`.
    pub fn peerClosed(self: *const Conn) bool {
        return self.state == .open and self.client.eof();
    }

    fn run(f: *Fiber, ctx: ?*anyopaque) void {
        _ = f;
        const self: *Conn = @ptrCast(@alignCast(ctx.?));
        self.session(self);
        if (self.state == .open) self.state = .closed;
    }

    fn fiberFinished(f: *Fiber) void {
        const self: *Conn = @fieldParentPtr("fiber", f);
        if (self.on_finished) |cb| cb(self);
    }
};

// ---------------------------------------------------------------------
// Connecting from inside a fiber
// ---------------------------------------------------------------------

/// Open a TCP connection, blocking-style, from inside a fiber.
///
/// This is the counterpart to `socket.Stream.connect` for fibered code: a
/// non-blocking connect plus a park on writability plus the `SO_ERROR`
/// check that distinguishes "connected" from "refused" — readiness alone
/// proves nothing, because a rejected connect reports the socket writable
/// too.
///
/// The returned fd is owned by the caller and is ready to hand to
/// `Conn.start`.
pub const DialError = Error || error{Canceled};

pub fn dial(f: *Fiber, addr: std.Io.net.IpAddress) DialError!Fd {
    var sa = sys.Sockaddr.fromIp(addr);
    const fd = try sys.socket(sa.family(), sys.SOCK_STREAM, 0);
    errdefer sys.close(fd);

    var pending = true;
    sys.connect(fd, &sa) catch |err| switch (err) {
        error.InProgress, error.WouldBlock => {},
        error.AlreadyConnected => pending = false,
        else => |e| return e,
    };

    if (pending) {
        const ready = try f.park(fd, .writable);
        if (ready.err) {
            try sys.socketError(fd);
            return error.ConnectionReset;
        }
        try sys.socketError(fd);
    }
    return fd;
}

// ---------------------------------------------------------------------
// Entropy
// ---------------------------------------------------------------------

/// Fill `buf` from the operating system's CSPRNG.
///
/// Not `std.Random`: the TLS handshake's key share comes out of this, so a
/// userspace PRNG seeded once is not acceptable. On Linux that is
/// `getrandom(2)` with no libc in the way; on Darwin `arc4random_buf`,
/// which is the documented interface and cannot fail.
pub fn osRandom(buf: []u8) sys.Error!void {
    if (sys.is_linux) {
        var off: usize = 0;
        while (off < buf.len) {
            const rc = std.os.linux.getrandom(buf.ptr + off, buf.len - off, 0);
            switch (std.os.linux.errno(rc)) {
                .SUCCESS => {
                    // A short read is legal for large requests; a zero-length
                    // one would be an infinite loop, so treat it as failure.
                    if (rc == 0) return error.Unexpected;
                    off += rc;
                },
                .INTR => continue,
                else => |e| return sys.mapError(e),
            }
        }
        return;
    }
    std.c.arc4random_buf(buf.ptr, buf.len);
}

// ---------------------------------------------------------------------
// tests
// ---------------------------------------------------------------------
//
// What is and is not covered here, stated plainly so nobody reads more
// into it than is there:
//
//   * The parking transport is exercised against real loopback TCP
//     sockets, in both directions, including short reads, short writes,
//     multi-megabyte transfers, clean EOF and cancellation.
//   * `std.crypto.tls.Client.init` is driven for real over that transport.
//     The ClientHello it produces is inspected on the wire, and the
//     server-side responses fed back are real TLS records: a fatal alert,
//     garbage, and a truncated stream. So the record layer, the error
//     mapping and the fiber plumbing are all genuinely exercised.
//   * A *complete* handshake is not. `std.crypto.tls` ships a client and
//     no server, so there is nothing in-tree to shake hands with, and no
//     test here reaches ServerHello, key schedule, or certificate chain
//     verification. Those paths are std's, but our stack-size choice and
//     the plaintext `reader()`/`writer()` are untested against a real
//     peer. That has to happen against an actual provider or an
//     out-of-tree endpoint before this is trusted in production.

const testing = std.testing;
const IpAddress = std.Io.net.IpAddress;

fn loopbackAny() IpAddress {
    return IpAddress.parse("127.0.0.1", 0) catch unreachable;
}

/// Drive the loop until `done`, with a wall-clock ceiling. A fiber that is
/// never resumed hangs rather than fails, so nothing here loops unbounded.
fn pumpUntil(loop: *reactor.Loop, deadline_ms: u64, ctx: anytype, done: fn (@TypeOf(ctx)) bool) !void {
    const start = sys.monotonicNanos();
    while (!done(ctx)) {
        if (sys.monotonicNanos() - start > deadline_ms * std.time.ns_per_ms) {
            return error.TestTimeout;
        }
        _ = try loop.tick(5);
    }
}

/// A scripted peer: a listener that accepts one connection and then runs a
/// step list against it, driven by the same reactor as the code under
/// test.
///
/// This is the only mock in the file and it sits exactly at the wire: it
/// speaks bytes over a real socket. Everything above it — the transport,
/// the fiber, `std.crypto.tls.Client` — is the real implementation.
const Peer = struct {
    listener: socket.Listener = undefined,
    source: reactor.Source = undefined,
    loop: *reactor.Loop,
    gpa: Allocator,
    fd: Fd = sys.invalid_fd,

    script: []const Step,
    step: usize = 0,
    /// Remainder of the current `send`/`trickle` step.
    pending: []const u8 = "",
    /// One byte per wakeup, for `trickle`.
    drip: bool = false,

    /// Everything received, in order, never consumed — tests assert on it.
    got: std.ArrayList(u8) = .empty,
    /// Receipts not yet accounted for by an `expect_len` step.
    unclaimed: usize = 0,
    sent: usize = 0,
    /// The script ran to completion.
    done: bool = false,
    write_failed: bool = false,

    const Step = union(enum) {
        /// Wait until this many further bytes have arrived.
        expect_len: usize,
        /// Send all of it, however many wakeups that takes.
        send: []const u8,
        /// Send one byte per wakeup. Forces the reader under test to park
        /// between every single byte.
        trickle: []const u8,
        /// Close the connection.
        close,
    };

    fn start(self: *Peer, gpa: Allocator, loop: *reactor.Loop, script: []const Step) !u16 {
        self.* = .{ .loop = loop, .gpa = gpa, .script = script };
        try self.listener.listen(loopbackAny(), onAccept, socket.default_backlog);
        self.listener.context = self;
        try loop.add(&self.listener.source);
        return self.listener.boundPort();
    }

    fn deinit(self: *Peer) void {
        self.closeConn();
        if (self.listener.source.isRegistered()) self.loop.remove(&self.listener.source);
        self.listener.close();
        self.got.deinit(self.gpa);
    }

    fn closeConn(self: *Peer) void {
        if (self.fd == sys.invalid_fd) return;
        if (self.source.isRegistered()) self.loop.remove(&self.source);
        sys.close(self.fd);
        self.fd = sys.invalid_fd;
    }

    fn onAccept(l: *socket.Listener, fd: Fd) void {
        const self: *Peer = @ptrCast(@alignCast(l.context.?));
        if (self.fd != sys.invalid_fd) {
            sys.close(fd);
            return;
        }
        self.fd = fd;
        // Both directions, because the script interleaves them and this is
        // a test peer: the writable-storm concern that shapes
        // `socket.Stream` does not apply to something with a deadline.
        self.source = .{ .fd = fd, .interest = .both, .callback = onReady };
        self.loop.add(&self.source) catch {
            sys.close(fd);
            self.fd = sys.invalid_fd;
            return;
        };
        self.advance();
    }

    fn onReady(src: *reactor.Source, ready: reactor.Ready) void {
        const self: *Peer = @fieldParentPtr("source", src);
        if (ready.read) {
            var buf: [16384]u8 = undefined;
            while (true) {
                const n = sys.read(src.fd, &buf) catch break;
                if (n == 0) break;
                self.got.appendSlice(self.gpa, buf[0..n]) catch break;
                self.unclaimed += n;
            }
        }
        self.advance();
    }

    fn advance(self: *Peer) void {
        while (self.fd != sys.invalid_fd) {
            if (self.pending.len != 0) {
                if (!self.pushPending()) return;
                continue;
            }
            if (self.step >= self.script.len) {
                self.done = true;
                return;
            }
            switch (self.script[self.step]) {
                .expect_len => |n| {
                    if (self.unclaimed < n) return;
                    self.unclaimed -= n;
                    self.step += 1;
                },
                .send => |bytes| {
                    self.step += 1;
                    self.pending = bytes;
                    self.drip = false;
                },
                .trickle => |bytes| {
                    self.step += 1;
                    self.pending = bytes;
                    self.drip = true;
                },
                .close => {
                    self.step += 1;
                    self.closeConn();
                    self.done = true;
                    return;
                },
            }
        }
    }

    /// True once `pending` is empty. False means "come back on the next
    /// wakeup" — either the socket is full, or this is a drip step.
    fn pushPending(self: *Peer) bool {
        while (self.pending.len != 0) {
            const chunk = if (self.drip) self.pending[0..1] else self.pending;
            const n = sys.write(self.fd, chunk) catch |err| switch (err) {
                error.WouldBlock => return false,
                else => {
                    self.write_failed = true;
                    return false;
                },
            };
            if (n == 0) return false;
            self.pending = self.pending[n..];
            self.sent += n;
            if (self.drip) return self.pending.len == 0;
        }
        return true;
    }
};

/// A fiber that connects to `port` and then runs `body` with a `Transport`
/// over the connected socket. Used by the transport-level tests, which have
/// no interest in TLS.
const RawSession = struct {
    fiber: Fiber = undefined,
    transport: Transport = undefined,
    gpa: Allocator,
    port: u16,
    read_buf: []u8,
    write_buf: []u8,
    body: *const fn (self: *RawSession) void,
    context: ?*anyopaque = null,
    err: ?anyerror = null,
    fd: Fd = sys.invalid_fd,

    fn start(self: *RawSession, loop: *reactor.Loop, stack: usize) !void {
        try self.fiber.init(self.gpa, loop, stack, run, self);
        self.fiber.enter();
    }

    fn deinit(self: *RawSession) void {
        self.fiber.deinit();
        if (self.fd != sys.invalid_fd) sys.close(self.fd);
    }

    fn run(f: *Fiber, ctx: ?*anyopaque) void {
        const self: *RawSession = @ptrCast(@alignCast(ctx.?));
        const fd = dial(f, IpAddress.parse("127.0.0.1", self.port) catch unreachable) catch |err| {
            self.err = err;
            return;
        };
        self.fd = fd;
        self.transport = .init(f, fd, self.read_buf, self.write_buf);
        self.body(self);
    }
};

test "the transport reader reassembles a payload delivered one byte at a time" {
    const gpa = testing.allocator;
    var loop: reactor.Loop = undefined;
    try loop.init(gpa);
    defer loop.deinit();

    // 400 separate writes on the peer side means the reader under test has
    // to park and be resumed hundreds of times to see one logical payload.
    const payload = "0123456789abcdef" ** 25;
    var peer: Peer = undefined;
    const port = try peer.start(gpa, &loop, &.{
        .{ .trickle = payload },
        .close,
    });
    defer peer.deinit();

    const Body = struct {
        got: [payload.len]u8 = undefined,
        n: usize = 0,

        fn run(s: *RawSession) void {
            const self: *@This() = @ptrCast(@alignCast(s.context.?));
            // Blocking-shaped: ask for the whole thing and let the
            // transport park however many times it takes.
            s.transport.reader.readSliceAll(&self.got) catch |err| {
                s.err = err;
                return;
            };
            self.n = self.got.len;
        }
    };

    var body: Body = .{};
    var read_buf: [4096]u8 = undefined;
    var write_buf: [1024]u8 = undefined;
    var session: RawSession = .{
        .gpa = gpa,
        .port = port,
        .read_buf = &read_buf,
        .write_buf = &write_buf,
        .body = Body.run,
        .context = &body,
    };
    try session.start(&loop, fiber_mod.min_stack_size);
    defer session.deinit();

    try pumpUntil(&loop, 10_000, &session, struct {
        fn done(s: *RawSession) bool {
            return s.fiber.isDone();
        }
    }.done);

    try testing.expectEqual(@as(?anyerror, null), session.err);
    try testing.expectEqual(payload.len, body.n);
    try testing.expectEqualStrings(payload, &body.got);
    // The whole point: it got there by parking, not by blocking the loop.
    //
    // Only ">= 1" is asserted, deliberately. The park count is a function of
    // how the kernel interleaves the two ends: under a lightly loaded run
    // it measured 108 for these 400 bytes, but when the rest of the suite is
    // competing for the CPU the peer's writes accumulate in the socket
    // buffer before the reader is scheduled, several trickled bytes are
    // coalesced into one read, and the count collapses. An earlier "> 50"
    // here was flaky in the full suite for exactly that reason. One park
    // proves the mechanism; the count would only be measuring the scheduler.
    try testing.expect(session.transport.read_parks >= 1);
    try testing.expectEqual(@as(u64, payload.len), session.transport.bytes_in);
}

test "the transport writer delivers more than the socket buffer, parking on backpressure" {
    const gpa = testing.allocator;
    var loop: reactor.Loop = undefined;
    try loop.init(gpa);
    defer loop.deinit();

    // 4 MiB is far past any socket send buffer, so every short write is
    // exercised repeatedly rather than incidentally.
    const size = 4 << 20;
    const payload = try gpa.alloc(u8, size);
    defer gpa.free(payload);
    var prng = std.Random.DefaultPrng.init(0xB0A7);
    prng.random().bytes(payload);

    var peer: Peer = undefined;
    const port = try peer.start(gpa, &loop, &.{.{ .expect_len = size }});
    defer peer.deinit();

    const Body = struct {
        payload: []const u8,

        fn run(s: *RawSession) void {
            const self: *@This() = @ptrCast(@alignCast(s.context.?));
            s.transport.writer.writeAll(self.payload) catch |err| {
                s.err = err;
                return;
            };
            s.transport.writer.flush() catch |err| {
                s.err = err;
            };
        }
    };

    var body: Body = .{ .payload = payload };
    var read_buf: [4096]u8 = undefined;
    var write_buf: [8192]u8 = undefined;
    var session: RawSession = .{
        .gpa = gpa,
        .port = port,
        .read_buf = &read_buf,
        .write_buf = &write_buf,
        .body = Body.run,
        .context = &body,
    };
    try session.start(&loop, fiber_mod.min_stack_size);
    defer session.deinit();

    // Wait for the *peer* to have it all: the fiber finishing only means
    // the kernel accepted the last byte, not that it arrived.
    const Ctx = struct { s: *RawSession, p: *Peer, want: usize };
    var ctx = Ctx{ .s = &session, .p = &peer, .want = size };
    try pumpUntil(&loop, 60_000, &ctx, struct {
        fn done(c: *Ctx) bool {
            return c.p.got.items.len >= c.want or (c.s.fiber.isDone() and c.s.err != null);
        }
    }.done);

    try testing.expectEqual(@as(?anyerror, null), session.err);
    try testing.expectEqual(@as(u64, size), session.transport.bytes_out);
    try testing.expect(session.transport.write_parks > 0);
    try testing.expectEqual(@as(usize, size), peer.got.items.len);
    // Byte-exact: a queue that reordered or dropped on a short write would
    // still produce the right length.
    try testing.expect(std.mem.eql(u8, payload, peer.got.items));
}

test "a clean peer close surfaces as EndOfStream, and stays that way" {
    const gpa = testing.allocator;
    var loop: reactor.Loop = undefined;
    try loop.init(gpa);
    defer loop.deinit();

    var peer: Peer = undefined;
    const port = try peer.start(gpa, &loop, &.{
        .{ .send = "half a message" },
        .close,
    });
    defer peer.deinit();

    const Body = struct {
        first: ?anyerror = null,
        second: ?anyerror = null,
        prefix: usize = 0,

        fn run(s: *RawSession) void {
            const self: *@This() = @ptrCast(@alignCast(s.context.?));
            var buf: [64]u8 = undefined;
            // Ask for more than the peer will ever send.
            self.first = if (s.transport.reader.readSliceAll(buf[0..40])) |_| null else |e| e;
            self.prefix = s.transport.reader.bufferedLen();
            // A second attempt must report the same thing rather than
            // spinning: a hung-up fd is reported ready on every tick.
            self.second = if (s.transport.reader.readSliceAll(buf[0..8])) |_| null else |e| e;
        }
    };

    var body: Body = .{};
    var read_buf: [4096]u8 = undefined;
    var write_buf: [1024]u8 = undefined;
    var session: RawSession = .{
        .gpa = gpa,
        .port = port,
        .read_buf = &read_buf,
        .write_buf = &write_buf,
        .body = Body.run,
        .context = &body,
    };
    try session.start(&loop, fiber_mod.min_stack_size);
    defer session.deinit();

    try pumpUntil(&loop, 10_000, &session, struct {
        fn done(s: *RawSession) bool {
            return s.fiber.isDone();
        }
    }.done);

    try testing.expectEqual(@as(?anyerror, error.EndOfStream), body.first);
    try testing.expectEqual(@as(?anyerror, error.EndOfStream), body.second);
    try testing.expect(session.transport.at_end);
    try testing.expectEqual(@as(?Error, null), session.transport.read_error);
}

test "a fiber parked in the transport can be cancelled" {
    const gpa = testing.allocator;
    var loop: reactor.Loop = undefined;
    try loop.init(gpa);
    defer loop.deinit();

    // A peer that accepts and then says nothing at all: the reader parks
    // and would stay parked forever.
    var peer: Peer = undefined;
    const port = try peer.start(gpa, &loop, &.{.{ .expect_len = std.math.maxInt(usize) }});
    defer peer.deinit();

    const Body = struct {
        saw: ?anyerror = null,
        cleaned_up: bool = false,
        /// Set once we are about to block on the read, so the test cancels
        /// that park rather than the one inside `dial`.
        reading: bool = false,

        fn run(s: *RawSession) void {
            const self: *@This() = @ptrCast(@alignCast(s.context.?));
            // The reason cancellation exists rather than just freeing the
            // fiber: this defer has to run.
            defer self.cleaned_up = true;
            var buf: [16]u8 = undefined;
            self.reading = true;
            self.saw = if (s.transport.reader.readSliceAll(&buf)) |_| null else |e| e;
        }
    };

    var body: Body = .{};
    var read_buf: [4096]u8 = undefined;
    var write_buf: [1024]u8 = undefined;
    var session: RawSession = .{
        .gpa = gpa,
        .port = port,
        .read_buf = &read_buf,
        .write_buf = &write_buf,
        .body = Body.run,
        .context = &body,
    };
    try session.start(&loop, fiber_mod.min_stack_size);
    defer session.deinit();

    const Ready = struct { s: *RawSession, b: *Body };
    var ready = Ready{ .s = &session, .b = &body };
    try pumpUntil(&loop, 10_000, &ready, struct {
        fn done(r: *Ready) bool {
            return r.b.reading and r.s.fiber.state == .parked;
        }
    }.done);

    session.fiber.cancel();

    try testing.expect(session.fiber.isDone());
    try testing.expect(body.cleaned_up);
    try testing.expectEqual(@as(?anyerror, error.ReadFailed), body.saw);
    // `std.Io` can only say `ReadFailed`; the transport is where the
    // distinction between shutdown and network fault lives.
    try testing.expect(session.transport.canceled);
}

// -- TLS ------------------------------------------------------------------

/// Stack the TLS tests give a handshake. Kept separate from
/// `fiber.default_stack_size` only so the measurement test can be read
/// against the value the rest of the tests use.
const handshake_stack: usize = fiber_mod.default_stack_size;

/// Fixed entropy, so the ClientHello a test inspects is reproducible.
const test_entropy: [entropy_len]u8 = blk: {
    var e: [entropy_len]u8 = undefined;
    for (&e, 0..) |*b, i| b.* = @truncate(i *% 7 +% 3);
    break :blk e;
};

/// A `Conn` plus the buffers and bookkeeping a test needs around it.
const TlsHarness = struct {
    conn: Conn = undefined,
    gpa: Allocator,
    port: u16,
    host: []const u8 = "test.invalid",
    trust: Trust = .insecure_skip_verification_dangerous,
    read_buf: []u8,
    write_buf: []u8,

    dial_err: ?anyerror = null,
    handshake_err: ?anyerror = null,
    /// Set once the session function has returned.
    session_done: bool = false,
    /// Filled by `after` on a successful handshake.
    after: ?*const fn (h: *TlsHarness) void = null,

    fn deinit(self: *TlsHarness) void {
        self.conn.deinit();
    }

    fn session(conn: *Conn) void {
        const self: *TlsHarness = @ptrCast(@alignCast(conn.context.?));
        conn.handshake(.{
            .host = self.host,
            .trust = self.trust,
            .read_buffer = self.read_buf,
            .write_buffer = self.write_buf,
            .entropy = &test_entropy,
        }) catch |err| {
            self.handshake_err = err;
            self.session_done = true;
            return;
        };
        if (self.after) |f| f(self);
        self.session_done = true;
    }
};

/// Connect to loopback and build a `Conn` around the fd, without entering
/// the fiber yet. Loopback cannot meaningfully block on connect, so the
/// test does not need `dial` here — and `dial` needs a fiber, which is what
/// we are about to build.
fn startHarness(h: *TlsHarness, loop: *reactor.Loop, stack: usize) !void {
    const addr = try IpAddress.parse("127.0.0.1", h.port);
    var sa = sys.Sockaddr.fromIp(addr);
    const fd = try sys.socket(sa.family(), sys.SOCK_STREAM, 0);
    errdefer sys.close(fd);
    sys.connect(fd, &sa) catch |err| switch (err) {
        error.InProgress, error.WouldBlock, error.AlreadyConnected => {},
        else => |e| return e,
    };
    try h.conn.init(h.gpa, loop, fd, TlsHarness.session, h, stack);
}

test "the handshake puts a well-formed ClientHello on the wire" {
    const gpa = testing.allocator;
    var loop: reactor.Loop = undefined;
    try loop.init(gpa);
    defer loop.deinit();

    // Say nothing back, then hang up. The handshake will fail; the point is
    // what it emitted on the way there, which proves `Client.init` really
    // ran through our parking `Writer` and put bytes on a real socket.
    var peer: Peer = undefined;
    const port = try peer.start(gpa, &loop, &.{
        .{ .expect_len = 1 },
        .close,
    });
    defer peer.deinit();

    const read_buf = try gpa.alloc(u8, min_read_buffer);
    defer gpa.free(read_buf);
    const write_buf = try gpa.alloc(u8, default_write_buffer);
    defer gpa.free(write_buf);

    var h: TlsHarness = .{
        .gpa = gpa,
        .port = port,
        .host = "news.example.com",
        // Not the insecure mode: std omits SNI entirely when host
        // verification is off, so this has to be a mode that keeps it.
        .trust = .self_signed_only,
        .read_buf = read_buf,
        .write_buf = write_buf,
    };
    try startHarness(&h, &loop, handshake_stack);
    defer h.deinit();
    h.conn.start();

    try pumpUntil(&loop, 10_000, &h, struct {
        fn done(x: *TlsHarness) bool {
            return x.session_done;
        }
    }.done);

    const hello = peer.got.items;
    try testing.expect(hello.len > 64);
    // TLS record header: handshake content type, then the legacy record
    // version that every TLS 1.3 ClientHello still uses.
    try testing.expectEqual(@as(u8, 0x16), hello[0]);
    try testing.expectEqual(@as(u8, 0x03), hello[1]);
    try testing.expectEqual(@as(u8, 0x01), hello[2]);
    const record_len = std.mem.readInt(u16, hello[3..5], .big);
    try testing.expectEqual(hello.len - 5, record_len);
    // Handshake header: client_hello, then a 24-bit length.
    try testing.expectEqual(@as(u8, 0x01), hello[5]);
    const hs_len = (@as(u32, hello[6]) << 16) | (@as(u32, hello[7]) << 8) | hello[8];
    try testing.expectEqual(@as(u32, @intCast(hello.len - 9)), hs_len);
    // The hostname we asked for has to be in there, or SNI is broken and a
    // provider on a shared address would hand us the wrong certificate.
    try testing.expect(std.mem.indexOf(u8, hello, "news.example.com") != null);

    // And the failure was reported, not swallowed.
    try testing.expect(h.handshake_err != null);
}

test "a fatal alert from the peer surfaces as TlsAlert with the peer's reason" {
    const gpa = testing.allocator;
    var loop: reactor.Loop = undefined;
    try loop.init(gpa);
    defer loop.deinit();

    // A real, minimal TLS alert record: content type 21, version 3.3,
    // length 2, level fatal (2), description handshake_failure (40).
    const alert_record = [_]u8{ 0x15, 0x03, 0x03, 0x00, 0x02, 0x02, 0x28 };

    var peer: Peer = undefined;
    const port = try peer.start(gpa, &loop, &.{
        .{ .expect_len = 1 },
        .{ .send = &alert_record },
        .close,
    });
    defer peer.deinit();

    const read_buf = try gpa.alloc(u8, min_read_buffer);
    defer gpa.free(read_buf);
    const write_buf = try gpa.alloc(u8, default_write_buffer);
    defer gpa.free(write_buf);

    var h: TlsHarness = .{
        .gpa = gpa,
        .port = port,
        .read_buf = read_buf,
        .write_buf = write_buf,
    };
    try startHarness(&h, &loop, handshake_stack);
    defer h.deinit();
    h.conn.start();

    try pumpUntil(&loop, 10_000, &h, struct {
        fn done(x: *TlsHarness) bool {
            return x.session_done;
        }
    }.done);

    try testing.expectEqual(@as(?anyerror, error.TlsAlert), h.handshake_err);
    // The whole reason `TlsAlert` is its own error: the peer told us why,
    // and that is the single most useful thing to put in a log.
    try testing.expect(h.conn.alert != null);
    try testing.expectEqual(tls.Alert.Level.fatal, h.conn.alert.?.level);
    try testing.expectEqual(tls.Alert.Description.handshake_failure, h.conn.alert.?.description);
}

test "a peer that answers with garbage surfaces a protocol error, not a hang" {
    const gpa = testing.allocator;
    var loop: reactor.Loop = undefined;
    try loop.init(gpa);
    defer loop.deinit();

    // The classic misconfiguration: a plaintext service on the TLS port.
    var peer: Peer = undefined;
    const port = try peer.start(gpa, &loop, &.{
        .{ .expect_len = 1 },
        .{ .send = "HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\n\r\n" },
        .close,
    });
    defer peer.deinit();

    const read_buf = try gpa.alloc(u8, min_read_buffer);
    defer gpa.free(read_buf);
    const write_buf = try gpa.alloc(u8, default_write_buffer);
    defer gpa.free(write_buf);

    var h: TlsHarness = .{
        .gpa = gpa,
        .port = port,
        .read_buf = read_buf,
        .write_buf = write_buf,
    };
    try startHarness(&h, &loop, handshake_stack);
    defer h.deinit();
    h.conn.start();

    try pumpUntil(&loop, 10_000, &h, struct {
        fn done(x: *TlsHarness) bool {
            return x.session_done;
        }
    }.done);

    // Which protocol error it is does not matter; that it is not a
    // certificate error and not a hang does.
    try testing.expect(h.handshake_err != null);
    const err = h.handshake_err.?;
    try testing.expect(err != error.CertificateNotTrusted);
    try testing.expect(err != error.CertificateHostMismatch);
    try testing.expect(h.conn.state == .closed);
    try testing.expect(h.conn.detail != null);
}

test "a peer that hangs up mid-handshake surfaces TransportFailed" {
    const gpa = testing.allocator;
    var loop: reactor.Loop = undefined;
    try loop.init(gpa);
    defer loop.deinit();

    // Half a record, then gone: the record layer is left waiting for bytes
    // that never come.
    var peer: Peer = undefined;
    const port = try peer.start(gpa, &loop, &.{
        .{ .expect_len = 1 },
        .{ .send = &[_]u8{ 0x16, 0x03, 0x03, 0x10, 0x00 } },
        .close,
    });
    defer peer.deinit();

    const read_buf = try gpa.alloc(u8, min_read_buffer);
    defer gpa.free(read_buf);
    const write_buf = try gpa.alloc(u8, default_write_buffer);
    defer gpa.free(write_buf);

    var h: TlsHarness = .{
        .gpa = gpa,
        .port = port,
        .read_buf = read_buf,
        .write_buf = write_buf,
    };
    try startHarness(&h, &loop, handshake_stack);
    defer h.deinit();
    h.conn.start();

    try pumpUntil(&loop, 10_000, &h, struct {
        fn done(x: *TlsHarness) bool {
            return x.session_done;
        }
    }.done);

    try testing.expect(h.handshake_err != null);
    try testing.expect(h.conn.transport.at_end);
}

test "cancelling a handshake reports Canceled, not a transport fault" {
    const gpa = testing.allocator;
    var loop: reactor.Loop = undefined;
    try loop.init(gpa);
    defer loop.deinit();

    // Accept the ClientHello and then say nothing, forever.
    var peer: Peer = undefined;
    const port = try peer.start(gpa, &loop, &.{.{ .expect_len = std.math.maxInt(usize) }});
    defer peer.deinit();

    const read_buf = try gpa.alloc(u8, min_read_buffer);
    defer gpa.free(read_buf);
    const write_buf = try gpa.alloc(u8, default_write_buffer);
    defer gpa.free(write_buf);

    var h: TlsHarness = .{
        .gpa = gpa,
        .port = port,
        .read_buf = read_buf,
        .write_buf = write_buf,
    };
    try startHarness(&h, &loop, handshake_stack);
    defer h.deinit();
    h.conn.start();

    try pumpUntil(&loop, 10_000, &h, struct {
        fn done(x: *TlsHarness) bool {
            return x.conn.fiber.state == .parked or x.session_done;
        }
    }.done);
    try testing.expect(!h.session_done);

    // This is the shutdown path, and the deadline path: the owner's timer
    // fires and has to get the news into code that only knows how to block.
    h.conn.cancel();

    try testing.expect(h.session_done);
    try testing.expectEqual(@as(?anyerror, error.Canceled), h.handshake_err);
}

test "an undersized read buffer is refused before the handshake starts" {
    const gpa = testing.allocator;
    var loop: reactor.Loop = undefined;
    try loop.init(gpa);
    defer loop.deinit();

    var peer: Peer = undefined;
    const port = try peer.start(gpa, &loop, &.{.{ .expect_len = std.math.maxInt(usize) }});
    defer peer.deinit();

    // `Client.init` asserts this, which in a release build is undefined
    // behaviour rather than a message. Catching it here turns a
    // configuration mistake into a named error.
    var small: [256]u8 = undefined;
    const write_buf = try gpa.alloc(u8, default_write_buffer);
    defer gpa.free(write_buf);

    var h: TlsHarness = .{
        .gpa = gpa,
        .port = port,
        .read_buf = &small,
        .write_buf = write_buf,
    };
    try startHarness(&h, &loop, fiber_mod.min_stack_size);
    defer h.deinit();
    h.conn.start();

    try testing.expect(h.session_done);
    try testing.expectEqual(@as(?anyerror, error.Misconfigured), h.handshake_err);
    // Nothing was sent, so the peer saw nothing.
    try testing.expectEqual(@as(usize, 0), peer.got.items.len);
}

test "dial from inside a fiber reports a refused connection" {
    const gpa = testing.allocator;
    var loop: reactor.Loop = undefined;
    try loop.init(gpa);
    defer loop.deinit();

    // Bind and release a port so nothing is listening on it. A refused
    // connect reports the socket *writable*, so readiness alone proves
    // nothing and `SO_ERROR` is the only answer.
    var probe: socket.Listener = undefined;
    try probe.listen(loopbackAny(), struct {
        fn f(_: *socket.Listener, fd: Fd) void {
            sys.close(fd);
        }
    }.f, 1);
    const dead_port = try probe.boundPort();
    probe.close();

    const Body = struct {
        var result: ?anyerror = null;
        var fd: Fd = sys.invalid_fd;

        fn run(f: *Fiber, ctx: ?*anyopaque) void {
            const port: *const u16 = @ptrCast(@alignCast(ctx.?));
            const addr = IpAddress.parse("127.0.0.1", port.*) catch unreachable;
            if (dial(f, addr)) |got| {
                fd = got;
                result = null;
            } else |err| {
                result = err;
            }
        }
    };
    Body.result = null;
    Body.fd = sys.invalid_fd;

    var f: Fiber = undefined;
    try f.init(gpa, &loop, fiber_mod.min_stack_size, Body.run, @constCast(&dead_port));
    defer f.deinit();
    defer if (Body.fd != sys.invalid_fd) sys.close(Body.fd);

    f.enter();
    try pumpUntil(&loop, 10_000, &f, struct {
        fn done(x: *Fiber) bool {
            return x.isDone();
        }
    }.done);

    try testing.expectEqual(@as(?anyerror, error.ConnectionRefused), Body.result);
}

test "dial from inside a fiber connects to a live listener" {
    const gpa = testing.allocator;
    var loop: reactor.Loop = undefined;
    try loop.init(gpa);
    defer loop.deinit();

    var peer: Peer = undefined;
    const port = try peer.start(gpa, &loop, &.{.{ .expect_len = 5 }});
    defer peer.deinit();

    const Body = struct {
        var result: ?anyerror = null;
        var fd: Fd = sys.invalid_fd;
        var wrote: bool = false;

        fn run(f: *Fiber, ctx: ?*anyopaque) void {
            const port_p: *const u16 = @ptrCast(@alignCast(ctx.?));
            const addr = IpAddress.parse("127.0.0.1", port_p.*) catch unreachable;
            fd = dial(f, addr) catch |err| {
                result = err;
                return;
            };
            sys.writeAll(fd, "hello") catch |err| {
                result = err;
                return;
            };
            wrote = true;
        }
    };
    Body.result = null;
    Body.fd = sys.invalid_fd;
    Body.wrote = false;

    var f: Fiber = undefined;
    try f.init(gpa, &loop, fiber_mod.min_stack_size, Body.run, @constCast(&port));
    defer f.deinit();
    defer if (Body.fd != sys.invalid_fd) sys.close(Body.fd);

    f.enter();
    try pumpUntil(&loop, 10_000, &peer, struct {
        fn done(p: *Peer) bool {
            return p.got.items.len >= 5;
        }
    }.done);

    try testing.expectEqual(@as(?anyerror, null), Body.result);
    try testing.expect(Body.wrote);
    try testing.expectEqualStrings("hello", peer.got.items);
}

test "handshake stack high-water leaves the default stack size room to spare" {
    const gpa = testing.allocator;
    var loop: reactor.Loop = undefined;
    try loop.init(gpa);
    defer loop.deinit();

    // This is the measurement behind `fiber.default_stack_size`. It covers
    // `Client.init`'s own frame — which Zig commits in the prologue, so it
    // is charged in full even though this handshake fails early — plus the
    // ClientHello construction and one record parse. It does *not* cover
    // certificate chain verification, because nothing in-tree can produce a
    // certificate to verify. Read the number as a floor, not a ceiling.
    const alert_record = [_]u8{ 0x15, 0x03, 0x03, 0x00, 0x02, 0x02, 0x28 };
    var peer: Peer = undefined;
    const port = try peer.start(gpa, &loop, &.{
        .{ .expect_len = 1 },
        .{ .send = &alert_record },
        .close,
    });
    defer peer.deinit();

    const read_buf = try gpa.alloc(u8, min_read_buffer);
    defer gpa.free(read_buf);
    const write_buf = try gpa.alloc(u8, default_write_buffer);
    defer gpa.free(write_buf);

    var h: TlsHarness = .{
        .gpa = gpa,
        .port = port,
        .read_buf = read_buf,
        .write_buf = write_buf,
    };
    try startHarness(&h, &loop, handshake_stack);
    defer h.deinit();
    h.conn.fiber.fillCanary();
    h.conn.start();

    try pumpUntil(&loop, 10_000, &h, struct {
        fn done(x: *TlsHarness) bool {
            return x.session_done;
        }
    }.done);

    const high_water = h.conn.fiber.stackHighWater();
    try testing.expect(high_water > 0);
    // Three quarters is the line at which the default stops being generous
    // and starts being a guess. If this trips, measure again and raise
    // `default_stack_size` — do not raise the fraction.
    try testing.expect(high_water < handshake_stack * 3 / 4);

    std.log.debug("TLS handshake stack high-water: {d} of {d} reserved", .{
        high_water, handshake_stack,
    });
}
