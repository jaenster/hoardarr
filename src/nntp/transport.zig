//! What an NNTP connection speaks over: a plain socket, or TLS.
//!
//! Essentially every commercial Usenet provider listens on 563 with TLS,
//! so "plaintext only" is not a limitation, it is a build that cannot
//! reach a single server an operator would actually configure. This file
//! is the seam that lets `conn.zig`'s state machine run over either.
//!
//! ## Two shapes that do not compose on their own
//!
//! `conn.zig` is callback-driven: the reactor says "readable", it calls
//! `read`, it advances a cursor, it returns. `std.crypto.tls.Client` is
//! the opposite — `init` runs a multi-round-trip handshake inside one
//! synchronous call, and the plaintext `Reader`/`Writer` it hands back
//! block too. `posix/fiber.zig` bridges that by giving the blocking code
//! its own stack, which is what `net/tls.zig` is built on.
//!
//! So the two paths are inverted with respect to each other:
//!
//!   * **plaintext** — the loop pulls. `Conn.onReadable` reads into its
//!     own buffer and drives the state machine.
//!   * **TLS** — the fiber pushes. The session loop below asks the owner
//!     for buffer space, decrypts into it, and tells the owner how much
//!     arrived. The owner's state machine runs on the fiber's stack.
//!
//! `Driver` is the whole of what the TLS session needs from its owner, so
//! this file does not import `conn.zig` and the two can be reasoned about
//! separately.
//!
//! ## Why writes are queued rather than written
//!
//! A TLS write can park, and only the session fiber may park on this fd —
//! it owns the reactor registration, and registering an fd twice is
//! `EEXIST` on epoll. `Conn.send` can be called from the fiber (a status
//! line drove the machine forward) *or* from outside it (a job fiber
//! resumed by a timer calls `fetchBody`). One rule covers both: `write`
//! appends to a buffer, and the session flushes it at the top of its
//! loop. When the call came from outside, a zero-delay timer wakes the
//! session; when it came from inside, the loop reaches the flush by
//! itself.
//!
//! ## Nothing enters a fiber from a callback
//!
//! `bootstrap/runtime.zig` states the rule and the reason: `Pool.release`
//! calls a queued waiter's callback before returning, and `Conn` calls
//! `on_body` from the middle of its read loop. Everything here obeys it.
//! The two places that would violate it are both routed through a
//! zero-delay reactor timer:
//!
//!   * waking the session for a queued command (`wake`), and
//!   * reporting a failure that was raised *on the fiber's own stack* —
//!     because the owner's error handler is entitled to destroy the
//!     connection, and freeing a fiber's stack while running on it is a
//!     `munmap` of the frame doing the freeing.
//!
//! ## Certificate trust
//!
//! `Trust` has no default and the unverified mode is spelled
//! `insecure_skip_verification_dangerous`, exactly as `net/tls.zig` has
//! it: there is no way to end up unverified by omission, and
//! `grep -rn insecure src/` finds every use. `CaStore` below is how the
//! roots get in, and it takes PEM bytes rather than a path because the
//! shipping container is `scratch` and has no `/etc/ssl/certs`.

const std = @import("std");
const sys = @import("../posix/sys.zig");
const reactor = @import("../posix/reactor.zig");
const socket = @import("../net/socket.zig");
const tlsmod = @import("../net/tls.zig");
const fiber_mod = @import("../posix/fiber.zig");

const stdtls = std.crypto.tls;
const Allocator = std.mem.Allocator;
const Fiber = fiber_mod.Fiber;
const Fd = sys.Fd;
const IpAddress = std.Io.net.IpAddress;
const assert = std.debug.assert;

/// Failures that only a TLS connection can produce.
///
/// Split rather than collapsed into one `TlsFailed` because an operator
/// debugging a provider needs to know which: the wrong roots embedded, a
/// hostname that does not match the certificate, a clock that is wrong,
/// and a server that refused the handshake are four different fixes.
pub const TlsError = error{
    /// The chain did not validate against the configured trust anchors.
    /// Either the embedded roots are wrong or something is intercepting.
    TlsCertificateNotTrusted,
    /// The chain validates but was not issued for the host we asked for.
    /// Almost always a misconfigured hostname, not an attack.
    TlsCertificateHostMismatch,
    /// Expired — or the container's clock is wrong, which is at least as
    /// likely and looks identical from here.
    TlsCertificateExpired,
    /// Not valid yet. In practice: the clock is wrong.
    TlsCertificateNotYetValid,
    /// The certificate could not be parsed. A broken or non-TLS peer.
    TlsCertificateMalformed,
    /// The peer refused the handshake and said why; see `Conn.tlsAlert`.
    TlsAlert,
    /// A malformed record, an unexpected message, no cipher in common.
    /// Also what a plaintext service on port 563 looks like.
    TlsProtocolError,
    /// Our own configuration is wrong — a buffer too small, entropy that
    /// isn't. A bug on this side, not the peer's.
    TlsMisconfigured,
};

pub const Error = socket.Error || TlsError;

// ---------------------------------------------------------------------
// Trust
// ---------------------------------------------------------------------

/// How to decide the peer is who it claims to be.
///
/// No default, deliberately. Picking one is a security decision and the
/// type system is the cheapest place to force it to be made out loud.
pub const Trust = union(enum) {
    /// Verify the chain against roots the caller supplies.
    ca_bundle: *CaStore,

    /// Accept a self-signed certificate provided it was issued for the
    /// host we asked for. Authenticates nothing — anyone can self-sign —
    /// but it does pin the connection to one key for its lifetime.
    self_signed_only,

    /// Skip certificate *and* hostname verification, which makes the
    /// connection trivially interceptable by anyone on the path.
    ///
    /// The name is long and unpleasant so it cannot appear in a review
    /// without being noticed. Note that `std.crypto.tls.Client` ties SNI
    /// to hostname verification and omits the server name entirely in
    /// this mode, so it cannot reach a provider that shares an address.
    insecure_skip_verification_dangerous,
};

/// Trust anchors, held in memory.
///
/// PEM bytes rather than a path: the shipping image is `scratch`, so
/// there is no `/etc/ssl/certs` to rescan and the roots have to arrive
/// either compiled in (`@embedFile`) or from the config. `std`'s own
/// loaders all want an `std.Io`, which this project does not have.
///
/// Initialise in place — `Trust.ca_bundle` holds a `*CaStore` and
/// `std.crypto.tls.Client` is handed `&self.lock`.
pub const CaStore = struct {
    gpa: Allocator,
    bundle: std.crypto.Certificate.Bundle = .empty,
    /// Guards `bundle` for `std.crypto.tls.Client`, which insists on one.
    ///
    /// It is never contended: every connection runs on the one reactor
    /// thread, and `lockShared`/`unlockShared` take the uncontended path
    /// without ever looking at the `std.Io` they are handed. That is why
    /// `dead_io` below can be what it is.
    lock: std.Io.RwLock = .init,

    pub const AddPemError = Allocator.Error || std.base64.Error ||
        std.crypto.Certificate.ParseError ||
        error{MissingEndCertificateMarker};

    pub fn init(gpa: Allocator) CaStore {
        return .{ .gpa = gpa };
    }

    pub fn deinit(self: *CaStore) void {
        self.bundle.deinit(self.gpa);
    }

    /// Number of distinct trust anchors indexed. Certificates that were
    /// already expired, duplicated, or used an object id `std` does not
    /// recognise are dropped by `parseCert` and are not counted.
    pub fn count(self: *const CaStore) usize {
        return self.bundle.map.count();
    }

    /// Add every `BEGIN CERTIFICATE` block in `pem`.
    ///
    /// This is `Bundle.addCertsFromFile` with the file taken out: same
    /// base64 decode into the bundle's own byte arena, same `parseCert`,
    /// no `std.Io.File.Reader` in the middle.
    pub fn addPem(self: *CaStore, pem: []const u8, now_sec: i64) AddPemError!usize {
        const gpa = self.gpa;
        const cb = &self.bundle;
        const begin_marker = "-----BEGIN CERTIFICATE-----";
        const end_marker = "-----END CERTIFICATE-----";

        var added: usize = 0;
        var start_index: usize = 0;
        while (std.mem.findPos(u8, pem, start_index, begin_marker)) |begin| {
            const cert_start = begin + begin_marker.len;
            const cert_end = std.mem.findPos(u8, pem, cert_start, end_marker) orelse
                return error.MissingEndCertificateMarker;
            start_index = cert_end + end_marker.len;

            const encoded = std.mem.trim(u8, pem[cert_start..cert_end], " \t\r\n");
            // Base64 never expands, so the encoded length is a safe upper
            // bound on the decoded one. Reserving before taking the
            // destination slice is what keeps the slice valid: the map's
            // keys are offsets, so a reallocation of `bytes` is fine, but
            // a live pointer into it is not.
            try cb.bytes.ensureUnusedCapacity(gpa, encoded.len);
            const decoded_start: u32 = @intCast(cb.bytes.items.len);
            const dest = cb.bytes.allocatedSlice()[decoded_start..];
            cb.bytes.items.len += try pem_base64.decode(dest, encoded);
            try cb.parseCert(gpa, decoded_start, now_sec);
            // `parseCert` rewinds `bytes` for anything it rejected.
            if (cb.bytes.items.len != decoded_start) added += 1;
        }
        return added;
    }
};

const pem_base64 = std.base64.standard.decoderWithIgnore(" \t\r\n");

/// The `std.Io` handed to `std.Io.RwLock` alongside a `CaStore`.
///
/// The lock is only ever taken uncontended — one thread, one reactor —
/// and both `lockShared` and `unlockShared` return on a pure atomic path
/// without dereferencing `io` when that is true. Should that assumption
/// ever stop holding, this faults on the first call rather than doing
/// something plausible with a garbage vtable. Adopting a real `std.Io`
/// implementation just to own a lock nobody contends is the thing this
/// project is built to avoid.
const dead_io: std.Io = .{
    .userdata = null,
    .vtable = @ptrFromInt(@alignOf(std.Io.VTable)),
};

// ---------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------

pub const TlsConfig = struct {
    /// SNI, and (unless verification is skipped) the name the certificate
    /// has to have been issued for. Borrowed for the connection's whole
    /// lifetime, so it must outlive the `Conn`.
    host: []const u8,
    /// No default. See `Trust`.
    trust: Trust,
    /// Fiber stack. The default is `posix/fiber.zig`'s measured value;
    /// certificate chain verification sits above what that measurement
    /// covered, which is why it is 7x the shipping high-water mark.
    stack_size: usize = fiber_mod.default_stack_size,
};

pub const Security = union(enum) {
    plaintext,
    tls: TlsConfig,
};

// ---------------------------------------------------------------------
// Driver: what the TLS session needs from its owner
// ---------------------------------------------------------------------

/// The owner's half of the TLS session, as five function pointers rather
/// than an import, so this file does not depend on `conn.zig`.
///
/// `on_open`, `space` and `filled` run **on the fiber's stack**.
/// `on_closed` never does — see the module comment.
pub const Driver = struct {
    ctx: *anyopaque,
    /// The handshake succeeded. The owner's protocol may begin.
    on_open: *const fn (ctx: *anyopaque) void,
    /// Where to decrypt into. An empty slice means the owner's buffer is
    /// full, which the session treats as a fatal desync — the same answer
    /// `Conn.compact` gives on the plaintext path.
    space: *const fn (ctx: *anyopaque) []u8,
    /// `n` bytes were written into what `space` returned. Returning false
    /// ends the session without an error.
    filled: *const fn (ctx: *anyopaque, n: usize) bool,
    /// True while the owner expects bytes. False means the session may
    /// idle instead of reading — which is the difference between a
    /// connection that costs nothing while parked and one that spins.
    awaiting: *const fn (ctx: *anyopaque) bool,
    /// The session ended. Always delivered on the loop's stack.
    on_closed: *const fn (ctx: *anyopaque, err: Error) void,
};

// ---------------------------------------------------------------------
// Transport
// ---------------------------------------------------------------------

/// One connection's byte pipe. `plain` is live when `tls` is null.
///
/// A struct with an unused field rather than a tagged union, because
/// `conn.zig` recovers itself from the socket callbacks with
/// `@fieldParentPtr("plain", ...)` and a stable field offset is what
/// makes that legible.
pub const Transport = struct {
    plain: socket.Stream = undefined,
    /// Heap-allocated so a plaintext connection pays nothing for it: the
    /// TLS state is ~64 KiB of record buffers plus a fiber.
    tls: ?*Tls = null,

    pub fn isTls(self: *const Transport) bool {
        return self.tls != null;
    }

    /// Queue bytes for the peer. On the plaintext path this is
    /// `socket.Stream.write` unchanged; on the TLS path it buffers and
    /// the session fiber encrypts and flushes.
    pub fn write(self: *Transport, bytes: []const u8) Error!void {
        if (self.tls) |t| return t.queue(bytes);
        return self.plain.write(bytes);
    }

    pub fn deinit(self: *Transport) void {
        if (self.tls) |t| {
            t.deinit();
            self.tls = null;
            return;
        }
        self.plain.deinit();
    }
};

// ---------------------------------------------------------------------
// The TLS session
// ---------------------------------------------------------------------

/// Every TLS connection needs *four* buffers, and all four have to be at
/// least one whole record.
///
/// Four, because `std.crypto.tls.Client` sits between two streams in each
/// direction: it reads ciphertext from the `Reader` it is handed and
/// writes plaintext into `Options.read_buffer`, and mirror-image for the
/// writers. Pointing one slice at both ends of a direction decrypts a
/// record on top of the record being decrypted.
///
/// A whole record, because both `Client.flush` and `Client.fill` ask
/// their underlying stream for `min_buffer_len` contiguous bytes and
/// assert on the answer — in a release build that is a corrupted write,
/// not a message. 16 KiB is *not* enough: a record is the cleartext limit
/// plus the header and the AEAD tag.
const record_len = stdtls.Client.min_buffer_len;
const cipher_in_len = record_len;
const cipher_out_len = record_len;
const plain_in_len = record_len;
const plain_out_len = record_len;

pub const Tls = struct {
    gpa: Allocator,
    loop: *reactor.Loop,
    driver: Driver,
    addr: IpAddress,
    cfg: TlsConfig,

    fiber: Fiber = undefined,
    transport: tlsmod.Transport = undefined,
    client: stdtls.Client = undefined,
    fd: Fd = sys.invalid_fd,

    /// One allocation, sliced four ways.
    slab: []u8,

    /// Plaintext the owner has handed us, not yet encrypted. `sending` is
    /// the buffer currently being written: swapping rather than indexing
    /// is what makes it safe for the owner to queue more while a flush is
    /// parked inside the socket.
    out: std.ArrayList(u8) = .empty,
    sending: std.ArrayList(u8) = .empty,

    /// Resumes the session for a command queued from outside it.
    wake: reactor.Timer,
    /// Delivers `on_closed`. See `onFinished`.
    report: reactor.Timer,

    fiber_live: bool = false,
    open: bool = false,
    /// The owner already knows the connection is finished, so the session
    /// ending must not report anything.
    stop: bool = false,
    reported: bool = false,
    /// True only while the session is parked in its idle wait, which is
    /// the one park that may be interrupted for a queued command.
    idle_parked: bool = false,

    err: ?Error = null,
    /// The peer's own explanation, when it sent one. The single most
    /// useful thing to put in a log.
    alert: ?stdtls.Alert = null,
    /// The unmapped `std` error behind whatever `TlsError` came out. For
    /// logs; branch on the error, not on this.
    detail: ?anyerror = null,

    /// Injectable clock and entropy, so a test can pin a moment inside a
    /// fixture's validity window and get a reproducible ClientHello.
    entropy: ?*const [tlsmod.entropy_len]u8 = null,
    realtime_now: ?std.Io.Timestamp = null,

    /// Allocate and set up the fiber. Nothing runs yet.
    ///
    /// Split from `begin` so the owner can store the pointer first: the
    /// session can fail and report before its first park — a refused
    /// connect does exactly that — and an owner that has not yet recorded
    /// the transport would handle that failure against a half-built
    /// connection.
    pub fn create(
        gpa: Allocator,
        loop: *reactor.Loop,
        addr: IpAddress,
        cfg: TlsConfig,
        driver: Driver,
    ) Error!*Tls {
        const self = try gpa.create(Tls);
        errdefer gpa.destroy(self);

        const slab = try gpa.alloc(u8, cipher_in_len + cipher_out_len + plain_in_len + plain_out_len);
        errdefer gpa.free(slab);

        self.* = .{
            .gpa = gpa,
            .loop = loop,
            .driver = driver,
            .addr = addr,
            .cfg = cfg,
            .slab = slab,
            .wake = .{ .callback = onWake },
            .report = .{ .callback = onReport },
        };

        try self.fiber.init(gpa, loop, cfg.stack_size, run, self);
        self.fiber_live = true;
        self.fiber.on_finished = onFinished;
        return self;
    }

    /// Enter the session. Returns once it first parks — typically
    /// immediately, inside the TCP connect.
    pub fn begin(self: *Tls) void {
        self.fiber.enter();
    }

    pub fn deinit(self: *Tls) void {
        if (self.wake.isArmed()) self.loop.cancelTimer(&self.wake);
        if (self.report.isArmed()) self.loop.cancelTimer(&self.report);
        // Abandons a still-parked session without unwinding it. That is
        // safe here and only here: everything the session's stack points
        // at is owned by this struct or by the `Conn` above it, and both
        // outlive the call.
        if (self.fiber_live) self.fiber.deinit();
        if (self.fd != sys.invalid_fd) sys.close(self.fd);
        self.out.deinit(self.gpa);
        self.sending.deinit(self.gpa);
        self.gpa.free(self.slab);
        self.gpa.destroy(self);
    }

    pub fn isDone(self: *const Tls) bool {
        return !self.fiber_live or self.fiber.isDone();
    }

    /// Unwind a parked session so its `defer`s run. No-op once finished.
    pub fn halt(self: *Tls) void {
        self.stop = true;
        if (!self.fiber_live) return;
        if (self.fiber.state == .parked) self.fiber.cancel();
    }

    fn cipherIn(self: *Tls) []u8 {
        return self.slab[0..cipher_in_len];
    }
    fn cipherOut(self: *Tls) []u8 {
        return self.slab[cipher_in_len..][0..cipher_out_len];
    }
    fn plainIn(self: *Tls) []u8 {
        return self.slab[cipher_in_len + cipher_out_len ..][0..plain_in_len];
    }
    fn plainOut(self: *Tls) []u8 {
        return self.slab[cipher_in_len + cipher_out_len + plain_in_len ..][0..plain_out_len];
    }

    // -- the owner's side ---------------------------------------------

    /// Buffer plaintext for the peer, and wake the session if it is
    /// idling. Never writes: only the fiber may touch the socket.
    fn queue(self: *Tls, bytes: []const u8) Error!void {
        if (self.stop or self.isDone()) return error.NotConnected;
        try self.out.appendSlice(self.gpa, bytes);
        if (!self.idle_parked) return;
        if (self.wake.isArmed()) return;
        // Never enter the fiber from here: `queue` runs on whatever stack
        // called `Conn.fetchBody`, which may be another fiber.
        self.loop.addTimer(&self.wake, 0) catch {
            // No timer to be had. The session stays parked until the peer
            // says something; the owner's per-command deadline is what
            // stops that from being forever.
        };
    }

    fn onWake(t: *reactor.Timer) void {
        const self: *Tls = @alignCast(@fieldParentPtr("wake", t));
        if (!self.idle_parked) return;
        if (!self.fiber_live or self.fiber.state != .parked) return;
        // Breaks the idle park only. Every other park in the session is
        // inside a TLS read, and `idle_parked` is false for those.
        self.fiber.failWith(error.Interrupted);
    }

    // -- the session ---------------------------------------------------

    fn run(f: *Fiber, ctx: ?*anyopaque) void {
        const self: *Tls = @ptrCast(@alignCast(ctx.?));
        self.session(f);
        // Whatever happened, the owner hears about it from `onFinished`,
        // on the loop's stack.
    }

    fn session(self: *Tls, f: *Fiber) void {
        const fd = tlsmod.dial(f, self.addr) catch |err| {
            self.err = switch (err) {
                error.Canceled => return,
                else => |e| e,
            };
            return;
        };
        self.fd = fd;

        self.transport = .init(f, fd, self.cipherIn(), self.cipherOut());
        self.handshake() catch |err| {
            self.err = err;
            return;
        };
        self.open = true;

        // The owner's protocol starts here. It runs on this stack and may
        // queue commands, which the loop below flushes.
        self.driver.on_open(self.driver.ctx);

        while (!self.stop) {
            // Re-drive whatever is already buffered, *then* flush. A
            // status line can arrive in the same record as the body
            // terminator before it, and the state that can consume it
            // only exists once the next command has been queued — so the
            // drive has to come first or that command sits unsent while
            // the session blocks on a read.
            if (!self.driver.filled(self.driver.ctx, 0)) {
                self.stop = true;
                return;
            }

            self.flush() catch |err| {
                self.err = err;
                return;
            };
            if (self.stop) return;

            if (!self.driver.awaiting(self.driver.ctx)) {
                // Nothing outstanding. Park on readability rather than
                // reading: a TLS read would block here forever, and the
                // park costs nothing while it lasts. It ends either
                // because the peer said something (including hanging up)
                // or because `queue` woke us for a command.
                self.idle_parked = true;
                const woke = f.park(fd, .readable);
                self.idle_parked = false;
                _ = woke catch |err| switch (err) {
                    error.Interrupted => continue,
                    error.Canceled => return,
                    else => |e| {
                        self.err = e;
                        return;
                    },
                };
            }

            const dst = self.driver.space(self.driver.ctx);
            if (dst.len == 0) {
                // The owner cannot take any more, which on this protocol
                // means one unterminated line longer than its whole
                // buffer. No real server does that.
                self.err = error.TlsProtocolError;
                return;
            }

            const available = self.client.reader.peekGreedy(1) catch |err| {
                self.err = self.readError(err);
                return;
            };
            const n = @min(available.len, dst.len);
            @memcpy(dst[0..n], available[0..n]);
            self.client.reader.toss(n);

            if (!self.driver.filled(self.driver.ctx, n)) {
                // The owner is finished with us and has already told
                // whoever needed to know.
                self.stop = true;
                return;
            }
        }
    }

    fn handshake(self: *Tls) TlsError!void {
        var gathered: [tlsmod.entropy_len]u8 = undefined;
        const entropy = self.entropy orelse blk: {
            tlsmod.osRandom(&gathered) catch return error.TlsMisconfigured;
            break :blk &gathered;
        };
        const now = self.realtime_now orelse std.Io.Timestamp.fromNanoseconds(
            @intCast(sys.realtimeNanos()),
        );

        var alert: stdtls.Alert = undefined;
        self.client = stdtls.Client.init(&self.transport.reader, &self.transport.writer, .{
            .host = switch (self.cfg.trust) {
                // std ties SNI to hostname verification and omits the
                // server name entirely when verification is off; keeping
                // the two together is the only honest option.
                .insecure_skip_verification_dangerous => .no_verification,
                else => .{ .explicit = self.cfg.host },
            },
            .ca = switch (self.cfg.trust) {
                .ca_bundle => |store| .{ .bundle = .{
                    .gpa = store.gpa,
                    .io = dead_io,
                    .lock = &store.lock,
                    .bundle = &store.bundle,
                } },
                .self_signed_only => .self_signed,
                .insecure_skip_verification_dangerous => .no_verification,
            },
            // Four distinct buffers. `read_buffer`/`write_buffer` are the
            // *plaintext* sides; the ciphertext sides are the reader and
            // writer above, which own `cipherIn`/`cipherOut`.
            .read_buffer = self.plainIn(),
            .write_buffer = self.plainOut(),
            .entropy = entropy,
            .realtime_now = now,
            .alert = &alert,
        }) catch |err| {
            self.detail = err;
            const mapped = mapInitError(err);
            if (mapped == error.TlsAlert) self.alert = alert;
            return mapped;
        };
    }

    /// Encrypt and send everything the owner queued.
    ///
    /// Nothing queued means nothing to do — not even a `flush` on the TLS
    /// writer, which would be a syscall per turn of the session loop.
    fn flush(self: *Tls) Error!void {
        if (self.out.items.len == 0) return;
        while (self.out.items.len != 0) {
            // Hand the filled buffer to `sending` and give `out` the
            // empty one. A write below can park, and the owner is allowed
            // to queue another command while it is parked; without the
            // swap that append could reallocate the slice being written.
            std.mem.swap(std.ArrayList(u8), &self.out, &self.sending);
            self.out.clearRetainingCapacity();
            self.client.writer.writeAll(self.sending.items) catch |err| {
                self.detail = err;
                return self.writeError();
            };
            self.sending.clearRetainingCapacity();
        }
        self.client.writer.flush() catch |err| {
            self.detail = err;
            return self.writeError();
        };
    }

    fn writeError(self: *Tls) Error {
        if (self.transport.canceled) return error.Interrupted;
        if (self.transport.write_error) |e| return e;
        return error.TlsProtocolError;
    }

    /// Turn a plaintext-read failure into something an operator can act
    /// on. `std.Io` can only say `ReadFailed`; the interesting reason is
    /// on the TLS client or on the transport below it.
    fn readError(self: *Tls, err: anyerror) Error {
        self.detail = err;
        if (err == error.EndOfStream) return error.ConnectionReset;
        if (self.transport.canceled) return error.Interrupted;
        if (self.client.read_err) |tls_err| {
            self.detail = tls_err;
            return switch (tls_err) {
                error.TlsAlert => blk: {
                    self.alert = self.client.alert;
                    break :blk error.TlsAlert;
                },
                // A stream that ends without `close_notify` is
                // indistinguishable from a truncation attack, so it is a
                // reset rather than a clean end.
                error.TlsConnectionTruncated => error.ConnectionReset,
                else => error.TlsProtocolError,
            };
        }
        if (self.transport.read_error) |e| return e;
        if (self.transport.at_end) return error.ConnectionReset;
        return error.TlsProtocolError;
    }

    /// The session's body has returned. Its stack is idle by now, but the
    /// resumer's is not: this runs inside `Fiber.enter`, which may itself
    /// have been called from `begin` before the owner finished wiring
    /// itself up. So the news goes out on a zero-delay timer, from the
    /// top of a later dispatch, with no frame of ours underneath it.
    fn onFinished(f: *Fiber) void {
        const self: *Tls = @alignCast(@fieldParentPtr("fiber", f));
        if (self.reported) return;
        self.reported = true;
        if (self.stop) return;
        self.loop.addTimer(&self.report, 0) catch {
            // Out of memory for a timer. Reporting late is better than
            // never; the owner's deadline is the only other backstop.
            self.deliver();
        };
    }

    fn onReport(t: *reactor.Timer) void {
        const self: *Tls = @alignCast(@fieldParentPtr("report", t));
        self.deliver();
    }

    fn deliver(self: *Tls) void {
        if (self.stop) return;
        self.stop = true;
        self.driver.on_closed(self.driver.ctx, self.err orelse error.ConnectionReset);
    }
};

/// Collapse `std.crypto.tls.Client.InitError` into the four categories an
/// operator can actually do something about.
///
/// Kept as an explicit list rather than an `else` catch-all for the
/// certificate cases: a new member of std's error set is far more likely
/// to be a protocol detail than a new category, and silently
/// reclassifying a certificate failure as a protocol error would hide the
/// one thing worth knowing.
pub fn mapInitError(err: stdtls.Client.InitError) TlsError {
    return switch (err) {
        error.CertificateHostMismatch => error.TlsCertificateHostMismatch,
        error.CertificateExpired => error.TlsCertificateExpired,
        error.CertificateNotYetValid => error.TlsCertificateNotYetValid,
        error.TlsAlert => error.TlsAlert,

        // The chain exists but does not establish trust. Signature
        // checks, issuer mismatches and unusable public keys are one
        // problem from the operator's seat.
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
        => error.TlsCertificateNotTrusted,

        // The bytes are not a certificate we can make sense of.
        error.CertificateFieldHasInvalidLength,
        error.CertificateFieldHasWrongDataType,
        error.CertificateHasUnrecognizedObjectId,
        error.CertificateHasInvalidBitString,
        error.CertificateTimeInvalid,
        error.UnsupportedCertificateVersion,
        error.InvalidEncoding,
        error.NegativeIntoUnsigned,
        => error.TlsCertificateMalformed,

        // Our side is set up wrong.
        error.InsufficientEntropy,
        error.BufferTooSmall,
        error.TargetTooSmall,
        error.MessageTooLong,
        => error.TlsMisconfigured,

        else => error.TlsProtocolError,
    };
}

// ---------------------------------------------------------------------
// tests
// ---------------------------------------------------------------------
//
// The TLS end-to-end tests live in `conn.zig`, where the state machine
// they drive is. What is here is the trust plumbing: PEM parsing against
// a real certificate, and the error taxonomy.

const testing = std.testing;

/// A real, self-signed X.509 certificate for `nntp.test.invalid`, valid
/// until 2125. Generated once with `openssl req -x509`; embedded rather
/// than generated per run so the test does not depend on `openssl` being
/// installed, and long-lived so it does not rot.
pub const test_ca_pem =
    \\-----BEGIN CERTIFICATE-----
    \\MIIBkDCCATWgAwIBAgIUYRrQShPGNkH94PcPyFlfXbKKvjcwCgYIKoZIzj0EAwIw
    \\HDEaMBgGA1UEAwwRbm50cC50ZXN0LmludmFsaWQwIBcNMjYwNzMwMTU0ODUzWhgP
    \\MjEyNjA3MDYxNTQ4NTNaMBwxGjAYBgNVBAMMEW5udHAudGVzdC5pbnZhbGlkMFkw
    \\EwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAE/LU5IFaZC/uwwXa4XboByGKRr4g/PQ1G
    \\zou++R4xNbBonDwN0NUZxvzOIEzirUWgBxUcPWDYPfbwnQt2flMb6aNTMFEwHQYD
    \\VR0OBBYEFA7PT8n5uY6yjh/ums0r4yOFnRAoMB8GA1UdIwQYMBaAFA7PT8n5uY6y
    \\jh/ums0r4yOFnRAoMA8GA1UdEwEB/wQFMAMBAf8wCgYIKoZIzj0EAwIDSQAwRgIh
    \\AP6wPlxaEr2DfcyL5AswD5y5T0WIxQW0a0mJl2iF33wqAiEAhfXYykeI2T+CqlWe
    \\/3K8Yui1wGs4F21SOYjAlCjQlmM=
    \\-----END CERTIFICATE-----
    \\
;

test "a real certificate is decoded, parsed and indexed by subject" {
    var store: CaStore = .init(testing.allocator);
    defer store.deinit();

    // The point of the exercise: this is what an embedded CA bundle goes
    // through, minus the `std.Io` that `Bundle`'s own loaders need and
    // this project does not have.
    const now_sec: i64 = @intCast(@divFloor(sys.realtimeNanos(), std.time.ns_per_s));
    try testing.expectEqual(@as(usize, 1), try store.addPem(test_ca_pem, now_sec));
    try testing.expectEqual(@as(usize, 1), store.count());
    // The DER really landed in the arena, and the subject index points at
    // it — which is what `Bundle.verify` looks up an issuer with.
    try testing.expect(store.bundle.bytes.items.len > 200);

    // Twice is idempotent: `parseCert` rewinds a duplicate subject.
    _ = try store.addPem(test_ca_pem, now_sec);
    try testing.expectEqual(@as(usize, 1), store.count());
}

test "an expired root is dropped rather than trusted" {
    var store: CaStore = .init(testing.allocator);
    defer store.deinit();

    // Same certificate, evaluated well after its notAfter. A bundle that
    // kept expired anchors would let a long-dead root keep validating.
    const far_future: i64 = 6_000_000_000; // year 2160
    try testing.expectEqual(@as(usize, 0), try store.addPem(test_ca_pem, far_future));
    try testing.expectEqual(@as(usize, 0), store.count());
}

test "an empty PEM adds nothing and is not an error" {
    var store: CaStore = .init(testing.allocator);
    defer store.deinit();

    try testing.expectEqual(@as(usize, 0), try store.addPem("", 0));
    try testing.expectEqual(@as(usize, 0), try store.addPem("# just a comment\n", 0));
    try testing.expectEqual(@as(usize, 0), store.count());
}

test "a PEM block with no end marker is refused rather than half-parsed" {
    var store: CaStore = .init(testing.allocator);
    defer store.deinit();

    try testing.expectError(
        error.MissingEndCertificateMarker,
        store.addPem("-----BEGIN CERTIFICATE-----\nAAAA\n", 0),
    );
}

test "every certificate error stays distinguishable from a protocol error" {
    // The whole reason `TlsError` has seven members: an operator staring
    // at a failed provider needs to know whether to fix the roots, the
    // hostname, the clock, or the port.
    try testing.expectEqual(TlsError.TlsCertificateHostMismatch, mapInitError(error.CertificateHostMismatch));
    try testing.expectEqual(TlsError.TlsCertificateExpired, mapInitError(error.CertificateExpired));
    try testing.expectEqual(TlsError.TlsCertificateNotYetValid, mapInitError(error.CertificateNotYetValid));
    try testing.expectEqual(TlsError.TlsCertificateNotTrusted, mapInitError(error.TlsCertificateNotVerified));
    try testing.expectEqual(TlsError.TlsCertificateNotTrusted, mapInitError(error.CertificateIssuerMismatch));
    try testing.expectEqual(TlsError.TlsCertificateMalformed, mapInitError(error.InvalidEncoding));
    try testing.expectEqual(TlsError.TlsAlert, mapInitError(error.TlsAlert));
    try testing.expectEqual(TlsError.TlsMisconfigured, mapInitError(error.InsufficientEntropy));
    // The catch-all is for record-layer and state-machine failures only.
    try testing.expectEqual(TlsError.TlsProtocolError, mapInitError(error.TlsUnexpectedMessage));
    try testing.expectEqual(TlsError.TlsProtocolError, mapInitError(error.TlsRecordOverflow));
}

// ---------------------------------------------------------------------
// Interop, verified by hand
// ---------------------------------------------------------------------
//
// `std.crypto.tls` ships a client and no server, so a completed handshake
// cannot be tested in tree. It was instead driven by hand against
// OpenSSL 3.4.1 speaking TLS 1.3, in both Debug and ReleaseFast, and this
// is how to repeat it. Nothing in CI does.
//
//   openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 \
//       -keyout key.pem -out cert.pem -days 36500 -nodes \
//       -subj /CN=nntp.test.invalid
//   printf '200 ready\r\n200 reader\r\n222 0 <a@b>\r\nline one\r\n.\r\n' \
//       | openssl s_server -quiet -naccept 1 -accept 14563 \
//           -cert cert.pem -key key.pem
//
// then a `Conn` with `security = .tls`, `host = "nntp.test.invalid"` and
// `trust = .{ .ca_bundle = &store }`, where `store` holds `test_ca_pem`.
//
// What that established:
//
//   * A full TLS 1.3 handshake, chain verification against an in-memory
//     PEM bundle included, and a complete NNTP conversation over it:
//     greeting, AUTHINFO USER/PASS, MODE READER, and two `BODY` fetches
//     — the second of them served from the idle park, which is the wake
//     path a pooled connection uses for every segment after its first.
//   * A 128 KiB body spanning many TLS records reassembled byte-exact,
//     dot-unstuffing and all.
//   * Each trust mode behaving as named: the right root connects, an
//     empty bundle gives `TlsCertificateNotTrusted`, the wrong hostname
//     gives `TlsCertificateHostMismatch`, and `self_signed_only` and the
//     insecure mode both connect.
//
// What it did not: a real provider, a chain deeper than one certificate,
// session resumption, or TLS 1.2.

test "the trust modes are three, and the unsafe one has to be named" {
    // A compile-time assertion in test form: if someone adds a default to
    // `Trust`, or renames the dangerous variant to something that reads
    // as harmless, this is what notices.
    try testing.expectEqual(@as(usize, 3), @typeInfo(Trust).@"union".fields.len);
    var found = false;
    inline for (@typeInfo(Trust).@"union".fields) |f| {
        if (std.mem.eql(u8, f.name, "insecure_skip_verification_dangerous")) found = true;
    }
    try testing.expect(found);
}
