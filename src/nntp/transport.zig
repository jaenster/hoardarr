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
    ///
    /// The two flushes at the end are both load-bearing.
    /// `std.crypto.tls.Client.flush` only *stages* a record: it encrypts
    /// into the ciphertext writer's buffer and advances it, leaving the
    /// syscall to whoever owns that writer. Stopping after the first one
    /// left every command after the greeting sitting in the ciphertext
    /// buffer, and the session then blocked in a read waiting for the
    /// answer to something the provider had never received — a hang that
    /// only ended on the owner's deadline. `std.http.Client.Connection`
    /// pairs the two the same way.
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
        self.transport.writer.flush() catch |err| {
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
// Interop against a live peer
// ---------------------------------------------------------------------
//
// The replay tests below shake hands for real on every run, but against a
// recording. This is the live counterpart — a second, independent
// implementation on the other side, which a recording cannot be. Nothing
// in CI does it.
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
// Read the earlier claim that this had established a complete NNTP
// conversation with scepticism: `s_server -quiet` answers from a script
// regardless of what the client sends, so it could not tell a command
// that reached the wire from one still sitting in the ciphertext buffer —
// which is exactly what every command after the greeting was doing. A
// peer that only speaks when spoken to is what catches that, and the
// replay peer counts the client's bytes before releasing each answer.

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

// ---------------------------------------------------------------------
// A recorded provider handshake
// ---------------------------------------------------------------------
//
// `std.crypto.tls` ships a client and no server, so nothing in tree can
// shake hands with `Client` — which is exactly why a handshake that had
// never completed could ship. A recording closes that: in TLS 1.3 every
// client secret comes out of `Options.entropy`, so pinning those 240
// bytes makes the ClientHello, the ECDHE share and the transcript hash
// byte-identical on every run, and server bytes captured against them
// decrypt again forever. `Options.realtime_now` pins the clock inside the
// certificate's validity window so the fixture does not rot.
//
// Captured from news.eweka.nl:563 against deliberately wrong credentials,
// so the conversation it drives is greeting, AUTHINFO USER, AUTHINFO PASS,
// rejection — the whole shape a provider connection has before it fetches
// anything, and every one of those commands is a write that has to reach
// the wire.
//
// To re-record: run the same exchange against a real provider with
// `replay_entropy` and `replay_realtime_ns` pinned, dump the socket bytes
// in order, and rewrite the flights below. The client byte counts are the
// lengths of its records and change if the credentials do.

const replay_host = "news.eweka.nl";
const replay_user = "probe-not-real";
const replay_pass = "probe-not-real";

/// The moment of capture. The leaf certificate was valid then and the
/// anchor below is valid until 2035, so verification is reproducible.
const replay_realtime_ns: i128 = 1785488807081942000;

/// Arbitrary but fixed. Any 240 bytes would do; what matters is that they
/// are the same ones the flights were captured against.
const replay_entropy: [tlsmod.entropy_len]u8 = blk: {
    var e: [tlsmod.entropy_len]u8 = undefined;
    for (&e, 0..) |*b, i| b.* = @truncate(i *% 7 +% 3);
    break :blk e;
};

/// ISRG Root X1, the anchor the captured chain terminates at. Embedded
/// rather than read from the host so the test does not depend on the
/// developer's certificate store.
const replay_ca_pem =
    \\-----BEGIN CERTIFICATE-----
    \\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
    \\-----END CERTIFICATE-----
    \\
;

const unb64 = tlsmod.unb64;

const replay_flight_1 = unb64(
    "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" ++
        "nQXuMiBS76BabUQF",
);

const replay_flight_2 = unb64(
    "FwMDAQq8VDesecUl16dYiLur7kbmjXS4ndddnqC5WeqgdcytngRJnZFwoZyB5by+" ++
        "QS68mTZ4Nacvj5/SPyelejXtkHAp9dBey2Et3N/KwPwPu5ai3k+woomHHzwYqC44" ++
        "3vz3aBbPB0DZEeUMkZCsn4UtnQJkR42J3+l4oBdl76TpBlAwq5d+4LIX4A1fDdlj" ++
        "WVMDWpldi/4CZJrWem+C+pSJXHpMtrlv8Ecs78TAMZA8phtWUEbAAOYsKILmEfmP" ++
        "w92irtu/KFgZpdUfWhUPtzNdz3REFdTWxibAwdLnY9ZxTxJYpYj31w+iJAwQrflI" ++
        "vWiy1zIhfNUuD5pSUsvjg1J3qnQ/Uj0OIF4zEAtyQxcDAwEKU5Gtw1MHX0rZEGFf" ++
        "fxGFuYk7oVu+gZ7XJFlunKzfy9Xpho3HbFYzT9DsaG+DUS4oL4n72Ma/CcBkVwZ9" ++
        "PQXyLEpPG4vXkPCc1cvYe+Y89WmPWrtMk7Rc53uNbLrtqOVnPNqknfQ8BaHRS68t" ++
        "DrOdD9KCnHlrkpc0NRa3nGquPJZ6gypF6AaKoj4HGk0scC66P7Kd4Yby0BVN/J7V" ++
        "hDo0aa45igQscJWFoqGJOkdLaZCZxNvG9LysVOXi44aC6rqWGuWxfaY4hkTO2v8u" ++
        "gNj65NweTymmPp+wu52U7EWNEJQuzshNmjXLPPqb8M0pHNxAJW81HbLVeZzWiPKd" ++
        "2CUiou4KCLIVZafURbUXAwMAJ3n5AVOIY/Uh65vAT4vTIoDSVBMN4TBP6gz/GiEq" ++
        "0bJDlpAjgteYcQ==",
);

const replay_flight_3 = unb64(
    "FwMDACRxUVqbaoIAyk50QUKeQoh+Qxpt6Mow0a5nBm3saeNVqvrURGA=",
);

const replay_flight_4 = unb64(
    "FwMDACx3MAEvnU7CMyQSyesWCnhpRiHPAAkVgdlCpb3uWsue7wXziWQtJF7h19EA" ++
        "eQ==",
);

/// The peer's side, keyed on how many bytes the client has to have sent
/// before each flight is released. Releasing a flight early would let a
/// client that never sent its command still receive the answer, which is
/// precisely the bug this replays.
const replay_script = [_]tlsmod.ScriptedPeer.Step{
    .{ .expect_len = 1613 },
    .{ .send = replay_flight_1 },
    .{ .expect_len = 80 },
    .{ .send = replay_flight_2 },
    .{ .expect_len = 52 },
    .{ .send = replay_flight_3 },
    .{ .expect_len = 52 },
    .{ .send = replay_flight_4 },
};

/// Drive `loop` until `done`, with a wall-clock ceiling. A session that
/// never gets its answer hangs rather than fails, so nothing here loops
/// unbounded — and a hang is exactly the failure being guarded against.
fn pumpUntil(loop: *reactor.Loop, deadline_ms: u64, ctx: anytype, done: fn (@TypeOf(ctx)) bool) !void {
    const start = sys.monotonicNanos();
    while (!done(ctx)) {
        if (sys.monotonicNanos() - start > deadline_ms * std.time.ns_per_ms) {
            return error.TestTimeout;
        }
        _ = try loop.tick(5);
    }
}

/// The transcript the replay is expected to produce, in full.
const replay_transcript = "200 Welcome to Eweka\r\n381 PASS required\r\n502 Authentication Failed\r\n";

/// A minimal NNTP client: answer the greeting with `AUTHINFO USER`, answer
/// 381 with `AUTHINFO PASS`, and stop on anything else. Enough to make the
/// session flush twice after the handshake, which is where the bug was.
const ReplayDriver = struct {
    tls: *Tls = undefined,
    seen: [1024]u8 = undefined,
    len: usize = 0,
    scanned: usize = 0,
    opened: bool = false,
    sent_user: bool = false,
    sent_pass: bool = false,
    done: bool = false,
    closed_err: ?Error = null,

    fn driver(self: *ReplayDriver) Driver {
        return .{
            .ctx = self,
            .on_open = onOpen,
            .space = space,
            .filled = filled,
            .awaiting = awaiting,
            .on_closed = onClosed,
        };
    }

    fn onOpen(ctx: *anyopaque) void {
        const self: *ReplayDriver = @ptrCast(@alignCast(ctx));
        self.opened = true;
    }

    fn space(ctx: *anyopaque) []u8 {
        const self: *ReplayDriver = @ptrCast(@alignCast(ctx));
        return self.seen[self.len..];
    }

    fn filled(ctx: *anyopaque, n: usize) bool {
        const self: *ReplayDriver = @ptrCast(@alignCast(ctx));
        self.len += n;
        while (std.mem.indexOfScalarPos(u8, self.seen[0..self.len], self.scanned, '\n')) |nl| {
            const line = self.seen[self.scanned..nl];
            self.scanned = nl + 1;
            if (std.mem.startsWith(u8, line, "200")) {
                self.tls.queue("AUTHINFO USER " ++ replay_user ++ "\r\n") catch return false;
                self.sent_user = true;
            } else if (std.mem.startsWith(u8, line, "381")) {
                self.tls.queue("AUTHINFO PASS " ++ replay_pass ++ "\r\n") catch return false;
                self.sent_pass = true;
            } else {
                self.done = true;
                return false;
            }
        }
        return true;
    }

    fn awaiting(ctx: *anyopaque) bool {
        const self: *ReplayDriver = @ptrCast(@alignCast(ctx));
        return !self.done;
    }

    fn onClosed(ctx: *anyopaque, err: Error) void {
        const self: *ReplayDriver = @ptrCast(@alignCast(ctx));
        self.closed_err = err;
        self.done = true;
    }
};

test "a recorded provider handshake completes and every command after it reaches the wire" {
    const gpa = testing.allocator;
    var loop: reactor.Loop = undefined;
    try loop.init(gpa);
    defer loop.deinit();

    var store: CaStore = .init(gpa);
    defer store.deinit();
    const now_sec: i64 = @intCast(@divFloor(replay_realtime_ns, std.time.ns_per_s));
    try testing.expectEqual(@as(usize, 1), try store.addPem(replay_ca_pem, now_sec));

    var peer: tlsmod.ScriptedPeer = undefined;
    const port = try peer.start(gpa, &loop, &replay_script);
    defer peer.deinit();

    var drv: ReplayDriver = .{};
    const t = try Tls.create(gpa, &loop, try IpAddress.parse("127.0.0.1", port), .{
        .host = replay_host,
        .trust = .{ .ca_bundle = &store },
    }, drv.driver());
    defer t.deinit();
    drv.tls = t;
    t.entropy = &replay_entropy;
    t.realtime_now = std.Io.Timestamp.fromNanoseconds(@intCast(replay_realtime_ns));
    t.begin();

    try pumpUntil(&loop, 20_000, &drv, struct {
        fn f(d: *ReplayDriver) bool {
            return d.done;
        }
    }.f);

    // A real chain, verified against a real root, over the real record
    // layer: this is the coverage nothing in tree had.
    try testing.expect(drv.opened);
    try testing.expect(drv.sent_user);
    try testing.expect(drv.sent_pass);
    try testing.expectEqualStrings(replay_transcript, drv.seen[0..drv.len]);
    // The peer only released each answer after counting the command's
    // bytes, so getting all three proves both commands left the process.
    // Flushing only the plaintext side leaves them in the ciphertext
    // buffer and this test times out instead.
    try testing.expectEqual(@as(?Error, null), drv.closed_err);
}

/// The same recording, driven through `net/tls.zig`'s `Conn` — the shape
/// `bootstrap/notify.zig` uses for `https://`, which has its own two-stage
/// `flush` to get wrong.
const ConnReplay = struct {
    gpa: Allocator,
    store: *CaStore,
    conn: tlsmod.Conn = undefined,
    cipher_read: []u8,
    cipher_write: []u8,
    seen: [1024]u8 = undefined,
    len: usize = 0,
    err: ?anyerror = null,
    done: bool = false,

    fn session(c: *tlsmod.Conn) void {
        const self: *ConnReplay = @ptrCast(@alignCast(c.context.?));
        defer self.done = true;

        c.handshake(.{
            .host = replay_host,
            .trust = .{ .ca_bundle = .{
                .gpa = self.store.gpa,
                .io = dead_io,
                .lock = &self.store.lock,
                .bundle = &self.store.bundle,
            } },
            .read_buffer = self.cipher_read,
            .write_buffer = self.cipher_write,
            .gpa = self.gpa,
            .entropy = &replay_entropy,
            .realtime_now = std.Io.Timestamp.fromNanoseconds(@intCast(replay_realtime_ns)),
        }) catch |e| {
            self.err = e;
            return;
        };

        self.take(c) catch return;
        self.say(c, "AUTHINFO USER " ++ replay_user ++ "\r\n") catch return;
        self.take(c) catch return;
        self.say(c, "AUTHINFO PASS " ++ replay_pass ++ "\r\n") catch return;
        self.take(c) catch return;
        c.close();
    }

    fn take(self: *ConnReplay, c: *tlsmod.Conn) !void {
        const line = c.reader().takeDelimiterInclusive('\n') catch |e| {
            self.err = e;
            return e;
        };
        @memcpy(self.seen[self.len..][0..line.len], line);
        self.len += line.len;
    }

    fn say(self: *ConnReplay, c: *tlsmod.Conn, line: []const u8) !void {
        c.writer().writeAll(line) catch |e| {
            self.err = e;
            return e;
        };
        c.flush() catch |e| {
            self.err = e;
            return e;
        };
    }
};

test "the same recording drives a tls.Conn session end to end" {
    const gpa = testing.allocator;
    var loop: reactor.Loop = undefined;
    try loop.init(gpa);
    defer loop.deinit();

    var store: CaStore = .init(gpa);
    defer store.deinit();
    const now_sec: i64 = @intCast(@divFloor(replay_realtime_ns, std.time.ns_per_s));
    try testing.expectEqual(@as(usize, 1), try store.addPem(replay_ca_pem, now_sec));

    var peer: tlsmod.ScriptedPeer = undefined;
    const port = try peer.start(gpa, &loop, &replay_script);
    defer peer.deinit();

    const cipher_read = try gpa.alloc(u8, tlsmod.min_read_buffer);
    defer gpa.free(cipher_read);
    const cipher_write = try gpa.alloc(u8, tlsmod.default_write_buffer);
    defer gpa.free(cipher_write);

    var replay: ConnReplay = .{
        .gpa = gpa,
        .store = &store,
        .cipher_read = cipher_read,
        .cipher_write = cipher_write,
    };

    // Loopback cannot meaningfully block on connect, and `dial` would need
    // the fiber we are about to build around this fd.
    const addr = try IpAddress.parse("127.0.0.1", port);
    var sa = sys.Sockaddr.fromIp(addr);
    const fd = try sys.socket(sa.family(), sys.SOCK_STREAM, 0);
    sys.connect(fd, &sa) catch |err| switch (err) {
        error.InProgress, error.WouldBlock, error.AlreadyConnected => {},
        else => |e| return e,
    };
    try replay.conn.init(gpa, &loop, fd, ConnReplay.session, &replay, fiber_mod.default_stack_size);
    defer replay.conn.deinit();
    replay.conn.start();

    try pumpUntil(&loop, 20_000, &replay, struct {
        fn f(r: *ConnReplay) bool {
            return r.done;
        }
    }.f);

    try testing.expectEqual(@as(?anyerror, null), replay.err);
    try testing.expectEqualStrings(replay_transcript, replay.seen[0..replay.len]);
}
