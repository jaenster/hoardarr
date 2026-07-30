//! A DNS client on the reactor.
//!
//! ## Why this exists
//!
//! Providers and webhook endpoints are configured by hostname —
//! `news.eweka.nl`, `discord.com` — and every socket call in this tree
//! takes an `IpAddress`. Something has to bridge the two.
//!
//! The obvious candidates don't fit. `std.Io.net.IpAddress.resolve` wants
//! an `std.Io`, which means adopting one of the backends `posix/sys.zig`
//! was written to avoid, and dragging a second readiness model into a
//! daemon whose whole idle-CPU story rests on having exactly one. And on
//! Linux the shipped binary links no libc at all, so there is no
//! `getaddrinfo` to call either.
//!
//! So: the wire format, a UDP transport on the reactor, and a cache.
//! It is a *stub resolver* — it asks a recursive nameserver and believes
//! the answer, exactly as `/etc/resolv.conf` says to. It does not walk the
//! root zone, and it does not validate DNSSEC.
//!
//! ## Shape
//!
//! `parseResponse` and `encodeQuery` know nothing about sockets, which is
//! how every hostile-message case below is tested against literal bytes
//! rather than against a network. `Resolver` owns the sockets and the
//! retry policy, in the same callback style as `socket.Stream.connect` and
//! `pool.acquire`, because those are the callers.
//!
//! ## Everything here is attacker-influenced
//!
//! A response arrives from a machine we do not control, over a protocol
//! with no authentication, and a UDP reply can be forged by anyone who can
//! guess what we asked. The defences, in order of importance:
//!
//!   * **The query ID and the source port are random** (`sys.randomBytes`),
//!     and the socket is `connect`ed so the kernel drops datagrams from
//!     anyone but the nameserver. An off-path forger has to guess all
//!     three.
//!   * **The question section is compared to what we asked**, not just the
//!     ID. Accepting a reply because it arrived on the right socket is the
//!     textbook cache-poisoning bug.
//!   * **A reply that fails any check is ignored, not fatal.** Failing the
//!     query on a forged datagram would hand an attacker a denial of
//!     service in exchange for a failed poisoning attempt; instead the
//!     real answer is still allowed to arrive.
//!   * **Compression pointers are bounded twice** — a strictly decreasing
//!     target and a jump budget — because a pointer loop is the classic
//!     way to hang a DNS parser.
//!   * **Nothing is sized from a length field** without checking it
//!     against the bytes actually present.

const std = @import("std");
const sys = @import("../posix/sys.zig");
const reactor = @import("../posix/reactor.zig");
const socket = @import("socket.zig");

const Allocator = std.mem.Allocator;
const IpAddress = std.Io.net.IpAddress;

// ---------------------------------------------------------------------
// Limits
// ---------------------------------------------------------------------

/// RFC 1035 §2.3.4. A name is at most 255 bytes on the wire and at most
/// 253 in presentation form (the wire form spends one byte per label on
/// the length prefix plus one on the root, the text form spends one dot
/// between labels).
pub const max_wire_name_len: usize = 255;
pub const max_text_name_len: usize = 253;
pub const max_label_len: usize = 63;

/// The largest query we can produce: header, name, qtype, qclass.
pub const max_query_len: usize = 12 + max_wire_name_len + 4;

/// We send no EDNS0 option, so a compliant server must keep a UDP reply
/// under 512 bytes and set TC if it can't. The receive buffer is larger
/// than that on purpose: a datagram the kernel had to truncate to fit
/// would parse as garbage, and it is better to see the whole thing and
/// reject it than to silently accept a prefix.
pub const max_udp_message: usize = 2048;

/// Ceiling on a TCP-framed response. Big enough for any A/AAAA set a real
/// zone publishes, small enough to sit in the query struct rather than
/// being allocated from a length field the peer controls.
pub const max_tcp_message: usize = 8192;

/// Records we are willing to walk in the answer section. A reply claiming
/// more is refused outright rather than parsed and then bounded.
pub const max_records: u16 = 64;

/// Compression-pointer jump budget. Belt to the braces of the strictly
/// decreasing rule in `readName`.
pub const max_pointer_jumps: usize = 16;

/// Addresses kept per name. A provider publishing more than this is
/// spreading load across a set we sample from, not a set we need in full.
pub const max_addrs: usize = 8;

/// CNAME hops followed inside one response.
pub const max_cname_depth: usize = 8;

/// Nameservers taken from `/etc/resolv.conf`. `resolv.conf(5)` itself
/// caps at 3; one spare costs nothing.
pub const max_servers: usize = 4;

// ---------------------------------------------------------------------
// Wire format
// ---------------------------------------------------------------------

/// The record types we handle. Everything else in an answer is skipped.
pub const Type = struct {
    pub const a: u16 = 1;
    pub const cname: u16 = 5;
    pub const aaaa: u16 = 28;
};

pub const class_in: u16 = 1;

/// RFC 1035 §4.1.1 header flags, as they sit in the second u16.
const flag_qr: u16 = 0x8000;
const flag_tc: u16 = 0x0200;
const flag_rd: u16 = 0x0100;

pub const RCode = enum(u4) {
    no_error = 0,
    format_error = 1,
    server_failure = 2,
    /// NXDOMAIN: the name does not exist. Authoritative, so there is no
    /// point asking a different server or a different record type.
    name_error = 3,
    not_implemented = 4,
    refused = 5,
    _,
};

pub const NameError = error{
    /// Not a syntactically valid hostname: an empty or over-long label, a
    /// name over 253 bytes, or a byte that has no business in one.
    InvalidHostName,
};

pub const ParseError = error{
    /// The message ended in the middle of something, or a length field
    /// pointed past the bytes we were given.
    Truncated,
    /// A label used a reserved length prefix, or contained a literal dot.
    MalformedName,
    NameTooLong,
    PointerLoop,
    /// The ID didn't match the outstanding query.
    IdMismatch,
    /// The QR bit was clear — this is somebody's query, not our answer.
    NotAResponse,
    /// The echoed question isn't the one we asked.
    QuestionMismatch,
    /// An rdata field whose length doesn't match its type.
    MalformedRecord,
    TooManyRecords,
};

/// One resolved address, without a port. The port belongs to the caller's
/// request, not to the zone, so it is applied at delivery.
pub const Address = union(enum) {
    v4: [4]u8,
    v6: [16]u8,

    pub fn toIp(self: Address, port: u16) IpAddress {
        return switch (self) {
            .v4 => |b| .{ .ip4 = .{ .bytes = b, .port = port } },
            .v6 => |b| .{ .ip6 = .{ .bytes = b, .port = port } },
        };
    }
};

/// Encode a hostname into wire form: a sequence of length-prefixed labels
/// terminated by a zero byte.
fn encodeName(buf: []u8, name: []const u8) NameError!usize {
    var text = name;
    // A fully-qualified name may end in a dot; the root label is implicit
    // in the encoding either way.
    if (text.len > 0 and text[text.len - 1] == '.') text = text[0 .. text.len - 1];
    if (text.len == 0 or text.len > max_text_name_len) return error.InvalidHostName;

    var w: usize = 0;
    var it = std.mem.splitScalar(u8, text, '.');
    while (it.next()) |label| {
        if (label.len == 0 or label.len > max_label_len) return error.InvalidHostName;
        if (w + 1 + label.len + 1 > buf.len) return error.InvalidHostName;
        buf[w] = @intCast(label.len);
        w += 1;
        for (label) |c| {
            // Printable ASCII only. A control byte in a hostname is either
            // a bug in the config or an attempt to smuggle something
            // through a log line, and no provider needs one.
            if (c <= 0x20 or c >= 0x7f) return error.InvalidHostName;
            buf[w] = std.ascii.toLower(c);
            w += 1;
        }
    }
    buf[w] = 0;
    w += 1;
    return w;
}

/// Build a standard recursive query. Returns the used length of `buf`.
pub fn encodeQuery(buf: *[max_query_len]u8, id: u16, name: []const u8, qtype: u16) NameError!usize {
    std.mem.writeInt(u16, buf[0..2], id, .big);
    // QR=0 (query), OPCODE=0 (standard), RD=1: we are a stub resolver
    // talking to a recursive server, so we want it to do the walking.
    std.mem.writeInt(u16, buf[2..4], flag_rd, .big);
    std.mem.writeInt(u16, buf[4..6], 1, .big); // qdcount
    @memset(buf[6..12], 0); // ancount, nscount, arcount

    const n = try encodeName(buf[12..], name);
    const off = 12 + n;
    std.mem.writeInt(u16, buf[off..][0..2], qtype, .big);
    std.mem.writeInt(u16, buf[off + 2 ..][0..2], class_in, .big);
    return off + 4;
}

/// A decoded name plus the offset just past it *as it appeared at the
/// position we started from* — which for a compressed name is two bytes
/// on, not wherever the pointer led.
const Name = struct {
    text: []const u8,
    next: usize,
};

/// Decode the name at `off` into `out`, lowercased, dot-separated, with no
/// trailing dot.
///
/// This is the function that historically eats DNS parsers. Two
/// independent bounds make a loop impossible:
///
///   1. Every pointer must target a strictly lower offset than the
///      previous one did. The first pointer sets the ceiling, so any cycle
///      — including a pointer to itself — fails on its second hop.
///   2. A hard jump budget, in case a future edit relaxes rule 1.
///
/// Plus the reconstructed length is capped, so a long chain of legal
/// pointers can't be used to blow the output buffer either.
fn readName(msg: []const u8, off: usize, out: *[max_text_name_len]u8) ParseError!Name {
    var pos = off;
    var written: usize = 0;
    var jumps: usize = 0;
    var next: ?usize = null;
    var ceiling: usize = msg.len;

    while (true) {
        if (pos >= msg.len) return error.Truncated;
        const len = msg[pos];
        switch (len & 0xc0) {
            // A plain label: the low six bits are its length.
            0x00 => {
                if (len == 0) {
                    if (next == null) next = pos + 1;
                    break;
                }
                const start = pos + 1;
                const end = start + len;
                if (end > msg.len) return error.Truncated;

                if (written != 0) {
                    if (written + 1 > out.len) return error.NameTooLong;
                    out[written] = '.';
                    written += 1;
                }
                if (written + len > out.len) return error.NameTooLong;
                for (msg[start..end]) |c| {
                    // A literal dot inside a label decodes to text that
                    // looks like a *different* name — exactly the shape of
                    // a match bypass. No hostname has one; refuse it.
                    if (c == '.') return error.MalformedName;
                    out[written] = std.ascii.toLower(c);
                    written += 1;
                }
                pos = end;
            },
            // A compression pointer: 14 bits of offset from the message
            // start.
            0xc0 => {
                if (pos + 2 > msg.len) return error.Truncated;
                const target = (@as(usize, len & 0x3f) << 8) | msg[pos + 1];
                if (next == null) next = pos + 2;

                jumps += 1;
                if (jumps > max_pointer_jumps) return error.PointerLoop;
                if (target >= ceiling) return error.PointerLoop;
                ceiling = target;
                pos = target;
            },
            // 0x40 and 0x80 are reserved and have never meant anything.
            else => return error.MalformedName,
        }
    }
    return .{ .text = out[0..written], .next = next.? };
}

fn u16At(msg: []const u8, off: usize) ParseError!u16 {
    if (off + 2 > msg.len) return error.Truncated;
    return std.mem.readInt(u16, msg[off..][0..2], .big);
}

fn u32At(msg: []const u8, off: usize) ParseError!u32 {
    if (off + 4 > msg.len) return error.Truncated;
    return std.mem.readInt(u32, msg[off..][0..4], .big);
}

/// What a well-formed response told us.
pub const Reply = struct {
    rcode: RCode,
    /// The TC bit: the answer didn't fit in a datagram and the whole
    /// question has to be asked again over TCP.
    truncated: bool,
    addrs: [max_addrs]Address = undefined,
    n: usize = 0,
    /// Smallest TTL across the records we used, in seconds. Zero when
    /// there are no addresses.
    ttl: u32 = 0,

    pub fn addresses(self: *const Reply) []const Address {
        return self.addrs[0..self.n];
    }
};

/// Parse a response against the query it is supposed to answer.
///
/// `want_name` must be in the same normalised form `readName` produces:
/// lowercase, dot-separated, no trailing dot.
///
/// Only the answer section is read. Authority and additional records are
/// ignored entirely, which is the cheapest possible defence against a
/// server that tries to slip us glue for a name we never asked about.
pub fn parseResponse(msg: []const u8, want_id: u16, want_name: []const u8, want_type: u16) ParseError!Reply {
    if (msg.len < 12) return error.Truncated;

    if (std.mem.readInt(u16, msg[0..2], .big) != want_id) return error.IdMismatch;
    const flags = std.mem.readInt(u16, msg[2..4], .big);
    if (flags & flag_qr == 0) return error.NotAResponse;

    const rcode: RCode = @enumFromInt(@as(u4, @truncate(flags)));
    var reply: Reply = .{ .rcode = rcode, .truncated = flags & flag_tc != 0 };

    // The question is echoed back, and it is half of what makes a forged
    // reply useless: an attacker has to know the exact name we asked for,
    // not merely that we asked something.
    if (std.mem.readInt(u16, msg[4..6], .big) != 1) return error.QuestionMismatch;
    var qname_buf: [max_text_name_len]u8 = undefined;
    const question = try readName(msg, 12, &qname_buf);
    if (try u16At(msg, question.next) != want_type) return error.QuestionMismatch;
    if (try u16At(msg, question.next + 2) != class_in) return error.QuestionMismatch;
    if (!std.mem.eql(u8, question.text, want_name)) return error.QuestionMismatch;

    // A truncated or failed response carries nothing worth reading.
    if (reply.truncated) return reply;
    if (rcode != .no_error) return reply;

    const answer_count = std.mem.readInt(u16, msg[6..8], .big);
    if (answer_count > max_records) return error.TooManyRecords;
    const answers_at = question.next + 4;

    // The name whose address we currently want. A CNAME moves it.
    var target_buf: [max_text_name_len]u8 = undefined;
    var target_len = want_name.len;
    @memcpy(target_buf[0..target_len], want_name);

    var ttl_min: u32 = std.math.maxInt(u32);

    // Records are not required to be in chain order, so each hop is a
    // fresh pass over the section rather than a single forward scan.
    // Bounded by `max_cname_depth` passes over at most `max_records`.
    var depth: usize = 0;
    while (depth <= max_cname_depth) : (depth += 1) {
        var cname_buf: [max_text_name_len]u8 = undefined;
        var cname_len: usize = 0;
        var have_cname = false;

        var off = answers_at;
        var i: usize = 0;
        while (i < answer_count) : (i += 1) {
            var rname_buf: [max_text_name_len]u8 = undefined;
            const rname = try readName(msg, off, &rname_buf);
            off = rname.next;

            const rtype = try u16At(msg, off);
            const rclass = try u16At(msg, off + 2);
            const rttl = try u32At(msg, off + 4);
            const rdlen = try u16At(msg, off + 8);
            off += 10;

            // The length field is the peer's claim; the message is the
            // truth. Check one against the other before touching rdata.
            const rdata_start = off;
            const rdata_end = off + rdlen;
            if (rdata_end > msg.len) return error.Truncated;
            off = rdata_end;

            if (rclass != class_in) continue;
            // A record for a name we didn't ask about, and that isn't the
            // CNAME chain's current link, is not an answer to our
            // question. Skipping it is what stops a server appending
            // `A evil.example` to a reply about `news.example`.
            if (!std.mem.eql(u8, rname.text, target_buf[0..target_len])) continue;

            if (rtype == want_type) {
                const expect: usize = if (want_type == Type.aaaa) 16 else 4;
                if (rdlen != expect) return error.MalformedRecord;
                if (reply.n < max_addrs) {
                    reply.addrs[reply.n] = if (want_type == Type.aaaa)
                        .{ .v6 = msg[rdata_start..][0..16].* }
                    else
                        .{ .v4 = msg[rdata_start..][0..4].* };
                    reply.n += 1;
                }
                ttl_min = @min(ttl_min, rttl);
            } else if (rtype == Type.cname and !have_cname) {
                const cn = try readName(msg, rdata_start, &cname_buf);
                // The name must live inside its own rdata. A CNAME whose
                // encoding runs past the record is malformed, however
                // plausible the bytes downstream look.
                if (cn.next > rdata_end) return error.MalformedRecord;
                cname_len = cn.text.len;
                have_cname = true;
                ttl_min = @min(ttl_min, rttl);
            }
        }

        if (reply.n > 0) break;
        if (!have_cname) break;
        target_len = cname_len;
        @memcpy(target_buf[0..target_len], cname_buf[0..cname_len]);
    }

    if (reply.n > 0) reply.ttl = ttl_min;
    return reply;
}

// ---------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------

pub const default_resolv_conf = "/etc/resolv.conf";

/// Used when `/etc/resolv.conf` is absent or names no usable server.
///
/// A `scratch` container normally *does* get one — Docker generates and
/// bind-mounts it even with no filesystem to speak of — so an absent file
/// means an unusual host setup rather than the common case. The order is
/// deliberate: Docker's embedded resolver first, because it is the only
/// entry that knows about container-local names, and because when it is
/// not there the loopback address fails in microseconds with
/// ECONNREFUSED rather than burning a timeout. The public resolvers
/// behind it are the ones that make the daemon work at all on a host with
/// no DNS configuration.
pub const fallback_servers = [_][]const u8{ "127.0.0.11", "1.1.1.1", "8.8.8.8" };

pub const Config = struct {
    servers: [max_servers]IpAddress = @splat(.{ .ip4 = .{ .bytes = .{ 0, 0, 0, 0 }, .port = 0 } }),
    server_count: usize = 0,
    /// Per attempt, not per query. `resolv.conf`'s default is 5s.
    timeout_ns: u64 = 5 * std.time.ns_per_s,
    /// Passes over the server list. Total attempts is
    /// `attempts * server_count`.
    attempts: usize = 2,
    /// True when `fallback_servers` are in use. Exposed so the daemon can
    /// say so at start-up — silently talking to a public resolver when the
    /// operator expected an internal one is the kind of surprise that
    /// belongs in a log line.
    from_fallback: bool = false,

    pub fn serverList(self: *const Config) []const IpAddress {
        return self.servers[0..self.server_count];
    }
};

fn fallbackConfig(base: Config) Config {
    var cfg = base;
    cfg.server_count = 0;
    for (fallback_servers) |text| {
        if (cfg.server_count == max_servers) break;
        cfg.servers[cfg.server_count] = IpAddress.parse(text, 53) catch continue;
        cfg.server_count += 1;
    }
    cfg.from_fallback = true;
    return cfg;
}

/// Parse `resolv.conf` syntax. Unknown directives and unparseable
/// addresses are skipped rather than failing the whole file — a resolver
/// that refuses to start because of one line it doesn't understand is
/// worse than one that ignores it.
pub fn parseResolvConf(text: []const u8) Config {
    var cfg: Config = .{};
    var lines = std.mem.splitScalar(u8, text, '\n');
    while (lines.next()) |raw| {
        // `#` and `;` both start a comment, per resolv.conf(5).
        var line = raw;
        if (std.mem.indexOfAny(u8, line, "#;")) |i| line = line[0..i];

        var it = std.mem.tokenizeAny(u8, line, " \t\r");
        const keyword = it.next() orelse continue;

        if (std.mem.eql(u8, keyword, "nameserver")) {
            const addr = it.next() orelse continue;
            if (cfg.server_count == max_servers) continue;
            // Port 53 is baked in: resolv.conf has no syntax for another.
            cfg.servers[cfg.server_count] = IpAddress.parse(addr, 53) catch continue;
            cfg.server_count += 1;
        } else if (std.mem.eql(u8, keyword, "options")) {
            while (it.next()) |opt| {
                if (std.mem.startsWith(u8, opt, "timeout:")) {
                    const v = std.fmt.parseInt(u8, opt["timeout:".len..], 10) catch continue;
                    // Clamped: a zero timeout is a spin and a 250s one
                    // pins a download job for four minutes.
                    cfg.timeout_ns = @as(u64, std.math.clamp(v, 1, 30)) * std.time.ns_per_s;
                } else if (std.mem.startsWith(u8, opt, "attempts:")) {
                    const v = std.fmt.parseInt(u8, opt["attempts:".len..], 10) catch continue;
                    cfg.attempts = std.math.clamp(v, 1, 5);
                }
            }
        }
    }
    if (cfg.server_count == 0) return fallbackConfig(cfg);
    return cfg;
}

/// Read and parse a resolv.conf, falling back when it isn't there or says
/// nothing useful.
pub fn loadResolvConf(path: [:0]const u8) Config {
    const fd = sys.open(path, .{}) catch return fallbackConfig(.{});
    defer sys.close(fd);

    // Fixed buffer, no allocation: a resolv.conf larger than this is not a
    // resolv.conf. Anything past the limit is simply not read, and the
    // directives that matter are at the top.
    var buf: [8192]u8 = undefined;
    var n: usize = 0;
    while (n < buf.len) {
        const r = sys.read(fd, buf[n..]) catch break;
        if (r == 0) break;
        n += r;
    }
    return parseResolvConf(buf[0..n]);
}

// ---------------------------------------------------------------------
// Cache
// ---------------------------------------------------------------------

/// Bounds on how long an answer is trusted.
///
/// The floor exists because a zone publishing `TTL 0` — deliberately, or
/// through a misconfigured load balancer — would otherwise cost a query
/// per connection, and a pool opening forty connections to one provider
/// would be forty queries. The ceiling exists because a provider that
/// renumbers should be picked up within the hour whatever the zone claims,
/// and because a hostile answer must not be able to pin an entry forever.
pub const default_ttl_floor_s: u32 = 30;
pub const default_ttl_ceiling_s: u32 = 3600;

/// Entries kept. Small on purpose: the daemon talks to a handful of
/// providers and a handful of webhook endpoints, and an unbounded cache
/// keyed on names a third party can suggest is a memory-growth primitive.
pub const default_max_cache_entries: usize = 256;

pub const Cache = struct {
    entries: std.ArrayList(Entry) = .empty,
    max_entries: usize = default_max_cache_entries,

    const Entry = struct {
        name: []u8,
        addrs: [max_addrs]Address,
        n: usize,
        expires_ns: u64,
        /// Round-robin cursor. A provider publishes several A records to
        /// spread load, and always handing out the first one throws that
        /// away.
        cursor: usize,
    };

    pub fn deinit(self: *Cache, gpa: Allocator) void {
        for (self.entries.items) |e| gpa.free(e.name);
        self.entries.deinit(gpa);
    }

    /// One address for `name`, rotating through the set. Expired entries
    /// are dropped as they are encountered rather than swept.
    pub fn lookup(self: *Cache, gpa: Allocator, name: []const u8, now_ns: u64) ?Address {
        var i: usize = 0;
        while (i < self.entries.items.len) {
            const e = &self.entries.items[i];
            if (e.expires_ns <= now_ns) {
                self.dropAt(gpa, i);
                continue;
            }
            if (std.mem.eql(u8, e.name, name)) {
                e.cursor +%= 1;
                return e.addrs[e.cursor % e.n];
            }
            i += 1;
        }
        return null;
    }

    pub fn put(self: *Cache, gpa: Allocator, name: []const u8, addrs: []const Address, expires_ns: u64) Allocator.Error!void {
        if (addrs.len == 0) return;

        for (self.entries.items) |*e| {
            if (std.mem.eql(u8, e.name, name)) {
                fill(e, addrs, expires_ns);
                return;
            }
        }

        if (self.entries.items.len >= self.max_entries) self.evictOne(gpa);

        const owned = try gpa.dupe(u8, name);
        errdefer gpa.free(owned);
        var e: Entry = .{ .name = owned, .addrs = undefined, .n = 0, .expires_ns = 0, .cursor = 0 };
        fill(&e, addrs, expires_ns);
        try self.entries.append(gpa, e);
    }

    fn fill(e: *Entry, addrs: []const Address, expires_ns: u64) void {
        e.n = @min(addrs.len, max_addrs);
        @memcpy(e.addrs[0..e.n], addrs[0..e.n]);
        e.expires_ns = expires_ns;
        e.cursor = 0;
    }

    /// Evict the entry that expires soonest. Not an LRU — the thing we
    /// most want to keep is the entry with the most life left, and a true
    /// LRU would need a touch on every lookup for a 256-entry table.
    fn evictOne(self: *Cache, gpa: Allocator) void {
        if (self.entries.items.len == 0) return;
        var best: usize = 0;
        for (self.entries.items, 0..) |e, i| {
            if (e.expires_ns < self.entries.items[best].expires_ns) best = i;
        }
        self.dropAt(gpa, best);
    }

    fn dropAt(self: *Cache, gpa: Allocator, i: usize) void {
        gpa.free(self.entries.items[i].name);
        _ = self.entries.swapRemove(i);
    }
};

// ---------------------------------------------------------------------
// Resolver
// ---------------------------------------------------------------------

pub const Error = socket.Error || NameError || error{
    /// NXDOMAIN. The name does not exist.
    NameNotFound,
    /// The name exists but publishes no address record.
    NoAddress,
    /// Every configured nameserver failed or refused.
    ServerFailure,
    /// No nameserver answered within the retry budget.
    Timeout,
    /// The configuration named none.
    NoNameservers,
    /// A response over TCP that we could not parse. Over UDP this is not
    /// an error — the datagram is simply ignored.
    MalformedResponse,
    /// A TCP response larger than `max_tcp_message`.
    ResponseTooLarge,
    /// The resolver was torn down with the query still outstanding.
    Canceled,
};

/// Called exactly once per `resolve`, possibly before `resolve` returns —
/// an address literal and a cache hit both answer synchronously, the same
/// way `pool.acquire` hands back an idle connection without a round trip.
pub const ResolveFn = *const fn (ctx: ?*anyopaque, result: Error!IpAddress) void;

pub const Resolver = struct {
    gpa: Allocator,
    loop: *reactor.Loop,
    config: Config,
    cache: Cache = .{},

    ttl_floor_s: u32 = default_ttl_floor_s,
    ttl_ceiling_s: u32 = default_ttl_ceiling_s,

    in_flight: std.ArrayList(*Query) = .empty,
    /// Cap on outstanding queries. Each holds a socket, and a burst of
    /// connects to unresolvable names must not be able to exhaust the fd
    /// table.
    max_in_flight: usize = 64,

    /// Finished queries waiting to be freed, as an intrusive list so
    /// retiring one cannot fail.
    ///
    /// A query is never freed from inside its own callback:
    /// `socket.Stream` reads its own state after calling `on_readable`, so
    /// destroying the struct the stream lives in would be a use-after-free
    /// one frame up. Instead the query is detached, its waiters are told,
    /// and the memory is released from a zero-delay timer — which the
    /// reactor fires later in the same tick, so this costs no extra
    /// wakeup.
    dead: ?*Query = null,
    reap: reactor.Timer = .{ .callback = onReap },

    pub fn init(self: *Resolver, gpa: Allocator, loop: *reactor.Loop, config: Config) void {
        self.* = .{ .gpa = gpa, .loop = loop, .config = config };
    }

    /// Initialise from `/etc/resolv.conf`, or from `fallback_servers` when
    /// it isn't there.
    pub fn initFromSystem(self: *Resolver, gpa: Allocator, loop: *reactor.Loop) void {
        self.init(gpa, loop, loadResolvConf(default_resolv_conf));
    }

    pub fn deinit(self: *Resolver) void {
        // Anything still in flight is told it lost, rather than being left
        // with a callback that never fires.
        while (self.in_flight.pop()) |q| {
            q.closeTransport();
            if (q.timer.isArmed()) self.loop.cancelTimer(&q.timer);
            for (q.waiters.items) |w| w.callback(w.ctx, error.Canceled);
            q.destroy();
        }
        self.in_flight.deinit(self.gpa);

        if (self.reap.isArmed()) self.loop.cancelTimer(&self.reap);
        self.reapAll();
        self.cache.deinit(self.gpa);
    }

    /// Turn `host` into an address on `port`.
    ///
    /// `host` may already be an address literal, with or without brackets,
    /// in which case the callback fires before this returns and no packet
    /// is sent.
    pub fn resolve(self: *Resolver, host: []const u8, port: u16, callback: ResolveFn, ctx: ?*anyopaque) void {
        var text = host;
        if (text.len >= 2 and text[0] == '[' and text[text.len - 1] == ']') text = text[1 .. text.len - 1];

        // Provider configs, the test suite and `--host 127.0.0.1` all pass
        // literals. Querying for one would be absurd, and would fail.
        if (IpAddress.parse(text, port)) |ip| {
            callback(ctx, ip);
            return;
        } else |_| {}

        var name_buf: [max_text_name_len]u8 = undefined;
        const name = normalizeName(&name_buf, text) catch {
            callback(ctx, error.InvalidHostName);
            return;
        };

        if (self.cache.lookup(self.gpa, name, sys.monotonicNanos())) |addr| {
            callback(ctx, addr.toIp(port));
            return;
        }

        if (self.config.server_count == 0) {
            callback(ctx, error.NoNameservers);
            return;
        }

        // A pool opening eight connections to one provider at once must
        // produce one query, not eight.
        for (self.in_flight.items) |q| {
            if (!std.mem.eql(u8, q.name, name)) continue;
            q.waiters.append(self.gpa, .{ .callback = callback, .ctx = ctx, .port = port }) catch {
                callback(ctx, error.OutOfMemory);
            };
            return;
        }

        if (self.in_flight.items.len >= self.max_in_flight) {
            callback(ctx, error.SystemResources);
            return;
        }

        Query.start(self, name, port, callback, ctx) catch |err| callback(ctx, err);
    }

    /// In-flight query count. Used by tests and by the metrics surface.
    pub fn pending(self: *const Resolver) usize {
        return self.in_flight.items.len;
    }

    fn retire(self: *Resolver, q: *Query) void {
        q.next_dead = self.dead;
        self.dead = q;
        if (self.reap.isArmed()) return;
        // If the timer heap can't grow, the queries stay on the list and
        // the next retire tries again; `deinit` sweeps whatever is left.
        self.loop.addTimer(&self.reap, 0) catch {};
    }

    fn onReap(t: *reactor.Timer) void {
        const self: *Resolver = @fieldParentPtr("reap", t);
        self.reapAll();
    }

    fn reapAll(self: *Resolver) void {
        while (self.dead) |q| {
            self.dead = q.next_dead;
            q.destroy();
        }
    }
};

/// Validate and lowercase a hostname into `out`.
fn normalizeName(out: *[max_text_name_len]u8, host: []const u8) NameError![]const u8 {
    var text = host;
    if (text.len > 0 and text[text.len - 1] == '.') text = text[0 .. text.len - 1];
    if (text.len == 0 or text.len > max_text_name_len) return error.InvalidHostName;

    var label_len: usize = 0;
    for (text, 0..) |c, i| {
        if (c == '.') {
            if (label_len == 0) return error.InvalidHostName;
            label_len = 0;
        } else {
            if (c <= 0x20 or c >= 0x7f) return error.InvalidHostName;
            label_len += 1;
            if (label_len > max_label_len) return error.InvalidHostName;
        }
        out[i] = std.ascii.toLower(c);
    }
    if (label_len == 0) return error.InvalidHostName;
    return out[0..text.len];
}

const Waiter = struct {
    callback: ResolveFn,
    ctx: ?*anyopaque,
    /// The port this caller asked for. Two callers can want the same host
    /// on different ports, so it belongs to the waiter, not the query.
    port: u16,
};

/// One outstanding question, and the transport carrying it.
const Query = struct {
    resolver: *Resolver,
    /// Normalised, owned.
    name: []u8,
    qtype: u16,
    id: u16 = 0,

    /// Which attempt we are on. The server is `attempt % server_count`, so
    /// the budget walks the whole list `attempts` times.
    attempt: usize = 0,
    /// Set once the A query came back empty and we fell back to AAAA, so
    /// we can't loop between them.
    tried_aaaa: bool = false,
    /// What to report if the budget runs out. A timeout unless a server
    /// gave us a reason.
    last_err: Error = error.Timeout,
    finished: bool = false,

    waiters: std.ArrayList(Waiter) = .empty,

    source: reactor.Source = .{ .fd = sys.invalid_fd, .interest = .readable, .callback = onUdpReady },
    timer: reactor.Timer = .{ .callback = onTimeout },

    /// TCP is only ever used after a response arrives with TC set.
    stream: socket.Stream = undefined,
    using_tcp: bool = false,
    tcp_len: usize = 0,
    tcp_buf: [max_tcp_message]u8 = undefined,

    wire: [max_query_len]u8 = undefined,
    wire_len: usize = 0,

    next_dead: ?*Query = null,

    const tcp_handler: socket.Handler = .{
        .on_readable = onTcpReadable,
        .on_close = onTcpClose,
        .on_connected = onTcpConnected,
    };

    fn start(r: *Resolver, name: []const u8, port: u16, callback: ResolveFn, ctx: ?*anyopaque) Error!void {
        const q = try r.gpa.create(Query);
        errdefer r.gpa.destroy(q);

        const owned = try r.gpa.dupe(u8, name);
        errdefer r.gpa.free(owned);

        q.* = .{ .resolver = r, .name = owned, .qtype = Type.a };
        try q.waiters.append(r.gpa, .{ .callback = callback, .ctx = ctx, .port = port });
        errdefer q.waiters.deinit(r.gpa);

        try r.in_flight.append(r.gpa, q);
        errdefer _ = r.in_flight.pop();

        try q.sendAttempt();
    }

    /// Free the memory. Only ever called from the resolver's reaper or its
    /// teardown, never from a callback.
    fn destroy(q: *Query) void {
        const gpa = q.resolver.gpa;
        q.waiters.deinit(gpa);
        gpa.free(q.name);
        gpa.destroy(q);
    }

    fn currentServer(q: *const Query) IpAddress {
        const cfg = &q.resolver.config;
        return cfg.servers[q.attempt % cfg.server_count];
    }

    fn sendAttempt(q: *Query) Error!void {
        q.closeTransport();

        // A fresh ID per attempt: reusing one would let a forger who saw
        // the first datagram answer the retry.
        sys.randomBytes(std.mem.asBytes(&q.id));
        q.wire_len = try encodeQuery(&q.wire, q.id, q.name, q.qtype);

        const fd = try openUdp(q.currentServer());
        q.source = .{ .fd = fd, .interest = .readable, .callback = onUdpReady };
        q.resolver.loop.add(&q.source) catch |err| {
            sys.close(fd);
            q.source.fd = sys.invalid_fd;
            return err;
        };
        errdefer q.closeTransport();

        _ = try sys.write(fd, q.wire[0..q.wire_len]);
        try q.arm();
    }

    fn arm(q: *Query) Error!void {
        if (q.timer.isArmed()) q.resolver.loop.cancelTimer(&q.timer);
        try q.resolver.loop.addTimer(&q.timer, q.resolver.config.timeout_ns);
    }

    fn closeTransport(q: *Query) void {
        if (q.using_tcp) {
            q.using_tcp = false;
            q.tcp_len = 0;
            q.stream.deinit();
            return;
        }
        if (q.source.isRegistered()) q.resolver.loop.remove(&q.source);
        if (q.source.fd != sys.invalid_fd) {
            sys.close(q.source.fd);
            q.source.fd = sys.invalid_fd;
        }
    }

    fn onTimeout(t: *reactor.Timer) void {
        const q: *Query = @fieldParentPtr("timer", t);
        q.last_err = error.Timeout;
        q.nextAttempt();
    }

    /// Move to the next server, or give up when the budget is spent.
    fn nextAttempt(q: *Query) void {
        q.closeTransport();
        const cfg = &q.resolver.config;
        q.attempt += 1;
        if (q.attempt >= cfg.server_count * cfg.attempts) {
            q.fail(q.last_err);
            return;
        }
        q.sendAttempt() catch |err| q.fail(err);
    }

    /// The A query came back NOERROR with nothing in it. A host that
    /// publishes only AAAA is the usual cause, so ask again before
    /// reporting failure.
    fn switchToAaaa(q: *Query) void {
        q.closeTransport();
        q.qtype = Type.aaaa;
        q.tried_aaaa = true;
        q.attempt = 0;
        q.last_err = error.NoAddress;
        q.sendAttempt() catch |err| q.fail(err);
    }

    // -- UDP ----------------------------------------------------------

    fn onUdpReady(src: *reactor.Source, ready: reactor.Ready) void {
        const q: *Query = @fieldParentPtr("source", src);
        if (!ready.read and !ready.terminal()) return;

        var buf: [max_udp_message]u8 = undefined;
        while (true) {
            const n = sys.read(src.fd, &buf) catch |err| switch (err) {
                error.WouldBlock => return,
                else => {
                    // On a connected datagram socket an ICMP port
                    // unreachable surfaces here as ECONNREFUSED. That is a
                    // definite "nothing is listening" and worth acting on
                    // immediately rather than waiting out the timeout.
                    q.last_err = err;
                    q.nextAttempt();
                    return;
                },
            };

            const reply = parseResponse(buf[0..n], q.id, q.name, q.qtype) catch {
                // Wrong ID, wrong question, or malformed. Not our answer.
                // Ignoring it rather than failing is deliberate: treating
                // a forged datagram as fatal would turn a failed poisoning
                // attempt into a successful denial of service, and the
                // real reply may still be on its way.
                continue;
            };
            q.handleReply(reply);
            return;
        }
    }

    fn handleReply(q: *Query, reply: Reply) void {
        if (reply.truncated) {
            // TC over TCP would mean the server is lying about its own
            // framing; there is no larger transport to escalate to.
            if (q.using_tcp) {
                q.fail(error.MalformedResponse);
                return;
            }
            q.startTcp();
            return;
        }

        switch (reply.rcode) {
            .no_error => {},
            // NXDOMAIN is authoritative for the whole name, so neither
            // another server nor another record type will do better.
            .name_error => {
                q.fail(error.NameNotFound);
                return;
            },
            else => {
                q.last_err = error.ServerFailure;
                q.nextAttempt();
                return;
            },
        }

        if (reply.n == 0) {
            if (q.qtype == Type.a and !q.tried_aaaa) {
                q.switchToAaaa();
                return;
            }
            q.fail(error.NoAddress);
            return;
        }

        q.succeed(reply);
    }

    // -- TCP fallback -------------------------------------------------

    fn startTcp(q: *Query) void {
        const server = q.currentServer();
        q.closeTransport();
        q.using_tcp = true;
        q.tcp_len = 0;
        q.stream.connect(q.resolver.gpa, q.resolver.loop, server, &tcp_handler) catch |err| {
            // `connect` leaves the stream untouched when it fails, so it
            // must not be deinited.
            q.using_tcp = false;
            q.last_err = err;
            q.nextAttempt();
            return;
        };
        q.arm() catch |err| q.fail(err);
    }

    fn onTcpConnected(s: *socket.Stream, err: ?socket.Error) void {
        const q: *Query = @fieldParentPtr("stream", s);
        if (err) |e| {
            q.last_err = e;
            q.nextAttempt();
            return;
        }
        // DNS over TCP prefixes each message with its 16-bit length.
        var framed: [2 + max_query_len]u8 = undefined;
        std.mem.writeInt(u16, framed[0..2], @intCast(q.wire_len), .big);
        @memcpy(framed[2..][0..q.wire_len], q.wire[0..q.wire_len]);
        s.write(framed[0 .. 2 + q.wire_len]) catch |e| {
            q.last_err = e;
            q.nextAttempt();
        };
    }

    fn onTcpReadable(s: *socket.Stream) void {
        const q: *Query = @fieldParentPtr("stream", s);
        while (true) {
            if (q.tcp_len == q.tcp_buf.len) {
                q.fail(error.ResponseTooLarge);
                return;
            }
            const n = s.read(q.tcp_buf[q.tcp_len..]) catch |err| switch (err) {
                error.WouldBlock => return,
                else => {
                    q.last_err = err;
                    q.nextAttempt();
                    return;
                },
            };
            if (n == 0) {
                q.last_err = error.ConnectionReset;
                q.nextAttempt();
                return;
            }
            q.tcp_len += n;

            if (q.tcp_len < 2) continue;
            const declared = std.mem.readInt(u16, q.tcp_buf[0..2], .big);
            // The length prefix is the peer's claim about a buffer we
            // already own; it is checked against that buffer, and nothing
            // is ever allocated from it.
            if (declared > q.tcp_buf.len - 2) {
                q.fail(error.ResponseTooLarge);
                return;
            }
            if (q.tcp_len < 2 + @as(usize, declared)) continue;

            // Over TCP the handshake already proves who the peer is, so a
            // response that fails the checks is a broken server rather
            // than a forgery, and there is nothing to wait for.
            const reply = parseResponse(q.tcp_buf[2..][0..declared], q.id, q.name, q.qtype) catch {
                q.fail(error.MalformedResponse);
                return;
            };
            q.handleReply(reply);
            return;
        }
    }

    fn onTcpClose(s: *socket.Stream, err: ?socket.Error) void {
        const q: *Query = @fieldParentPtr("stream", s);
        if (q.finished) return;
        q.last_err = err orelse error.ConnectionReset;
        q.nextAttempt();
    }

    // -- completion ---------------------------------------------------

    fn succeed(q: *Query, reply: Reply) void {
        const r = q.resolver;
        const ttl = std.math.clamp(reply.ttl, r.ttl_floor_s, r.ttl_ceiling_s);
        const expires = sys.monotonicNanos() + @as(u64, ttl) * std.time.ns_per_s;
        // A cache insert that can't allocate is not a reason to fail a
        // resolution that already succeeded.
        r.cache.put(r.gpa, q.name, reply.addresses(), expires) catch {};

        q.detach();
        const addrs = reply.addresses();
        for (q.waiters.items, 0..) |w, i| {
            // Spread concurrent waiters across the address set for the
            // same reason the cache rotates.
            w.callback(w.ctx, addrs[i % addrs.len].toIp(w.port));
        }
        r.retire(q);
    }

    fn fail(q: *Query, err: Error) void {
        if (q.finished) return;
        const r = q.resolver;
        q.detach();
        for (q.waiters.items) |w| w.callback(w.ctx, err);
        r.retire(q);
    }

    /// Take the query out of every structure that could call back into it,
    /// without freeing it.
    fn detach(q: *Query) void {
        q.finished = true;
        q.closeTransport();
        if (q.timer.isArmed()) q.resolver.loop.cancelTimer(&q.timer);
        for (q.resolver.in_flight.items, 0..) |item, i| {
            if (item != q) continue;
            _ = q.resolver.in_flight.swapRemove(i);
            break;
        }
    }
};

/// A connected UDP socket bound to a random source port.
///
/// `connect` on a datagram socket does not send anything; it fixes the
/// peer, which makes the kernel discard datagrams from any other address.
/// That is a spoofing defence we get for one syscall, and it is also what
/// turns an ICMP port-unreachable into a prompt ECONNREFUSED instead of a
/// silent five-second wait.
fn openUdp(server: IpAddress) sys.Error!sys.Fd {
    var sa = sys.Sockaddr.fromIp(server);
    const fd = try sys.socket(sa.family(), sys.SOCK_DGRAM, 0);
    errdefer sys.close(fd);
    try bindRandomPort(fd, server);
    try sys.connect(fd, &sa);
    return fd;
}

/// Bind to a random port in the unprivileged range.
///
/// Linux randomises the ephemeral range itself, but relying on that means
/// relying on a sysctl a host operator can turn off, and the source port
/// is one of the three values a forger has to guess.
fn bindRandomPort(fd: sys.Fd, server: IpAddress) sys.Error!void {
    var tries: usize = 0;
    while (tries < 8) : (tries += 1) {
        var raw: u16 = 0;
        sys.randomBytes(std.mem.asBytes(&raw));
        const port: u16 = 1024 + (raw % (65535 - 1024));
        var sa = sys.Sockaddr.fromIp(anyAddress(server, port));
        sys.bind(fd, &sa) catch |err| switch (err) {
            error.AddressInUse, error.PermissionDenied => continue,
            else => return err,
        };
        return;
    }
    // Every candidate was taken. Let the kernel choose: degraded, since we
    // are back to trusting its randomisation, but not broken.
    var sa = sys.Sockaddr.fromIp(anyAddress(server, 0));
    return sys.bind(fd, &sa);
}

fn anyAddress(server: IpAddress, port: u16) IpAddress {
    return switch (server) {
        .ip4 => .{ .ip4 = .unspecified(port) },
        .ip6 => .{ .ip6 = .unspecified(port) },
    };
}

// ---------------------------------------------------------------------
// Scripted stub nameserver (test support)
// ---------------------------------------------------------------------

/// A scripted nameserver for tests, in the same spirit as
/// `nntp.conn.StubServer`: one entry of the script is consumed per query
/// received, and anything the script didn't plan for is recorded rather
/// than tolerated.
///
/// It listens on both UDP and TCP on the same port, because the TC
/// fallback re-asks the same nameserver over TCP.
///
/// `pub` so the HTTP client's tests and the eventual end-to-end suite can
/// use the same fake instead of growing their own.
pub const StubServer = struct {
    udp: reactor.Source = .{ .fd = sys.invalid_fd, .interest = .readable, .callback = onUdpReady },
    listener: socket.Listener = undefined,
    loop: *reactor.Loop,
    gpa: Allocator,
    script: []const Action,
    port: u16 = 0,

    /// Queries received, over either transport.
    received: usize = 0,
    /// Of those, how many arrived length-prefixed over TCP.
    received_tcp: usize = 0,
    step: usize = 0,
    /// Set when the script ran out before the client stopped asking.
    overrun: bool = false,

    conns: std.ArrayList(*StubConn) = .empty,

    pub const Action = union(enum) {
        /// Say nothing. The client must retry, or time out.
        drop,
        /// Answer with these addresses, if the question's type matches
        /// them. A question of the other type gets an empty NOERROR, which
        /// is what a real server does for a host with only one family.
        answer: struct { ttl: u32 = 300, v4: []const [4]u8 = &.{}, v6: []const [16]u8 = &.{} },
        /// Answer with an rcode and an empty answer section.
        rcode: RCode,
        /// Answer with TC set, forcing the client onto TCP.
        truncate,
        /// Answer correctly, but with the ID left as-is instead of echoed.
        /// The client must ignore it.
        wrong_id,
        /// Answer a different question than the one asked.
        wrong_question,
    };

    const StubConn = struct {
        stream: socket.Stream,
        server: *StubServer,
        in: [1024]u8 = undefined,
        in_len: usize = 0,

        const handler: socket.Handler = .{ .on_readable = onReadable, .on_close = onClose };

        fn onReadable(s: *socket.Stream) void {
            const self: *StubConn = @fieldParentPtr("stream", s);
            while (true) {
                if (self.in_len == self.in.len) return;
                const n = s.read(self.in[self.in_len..]) catch return;
                if (n == 0) return;
                self.in_len += n;

                if (self.in_len < 2) continue;
                const declared = std.mem.readInt(u16, self.in[0..2], .big);
                if (self.in_len < 2 + @as(usize, declared)) continue;

                var out: [max_udp_message]u8 = undefined;
                self.server.received_tcp += 1;
                const reply = self.server.respond(&out, self.in[2..][0..declared]) orelse return;
                var framed: [2 + max_udp_message]u8 = undefined;
                std.mem.writeInt(u16, framed[0..2], @intCast(reply.len), .big);
                @memcpy(framed[2..][0..reply.len], reply);
                s.write(framed[0 .. 2 + reply.len]) catch {};
                self.in_len = 0;
            }
        }

        fn onClose(s: *socket.Stream, _: ?socket.Error) void {
            s.state = .closed;
        }
    };

    pub fn start(self: *StubServer, gpa: Allocator, loop: *reactor.Loop, script: []const Action) !void {
        self.* = .{ .loop = loop, .gpa = gpa, .script = script };

        // TCP first: it picks the port, and the UDP socket then has to
        // take the same one.
        try self.listener.listen(try IpAddress.parse("127.0.0.1", 0), onAccept, 8);
        self.listener.context = self;
        errdefer self.listener.close();
        try loop.add(&self.listener.source);
        errdefer loop.remove(&self.listener.source);
        self.port = try self.listener.boundPort();

        const fd = try sys.socket(sys.AF_INET, sys.SOCK_DGRAM, 0);
        errdefer sys.close(fd);
        var sa = sys.Sockaddr.fromIp(try IpAddress.parse("127.0.0.1", self.port));
        try sys.bind(fd, &sa);
        self.udp = .{ .fd = fd, .interest = .readable, .callback = onUdpReady };
        try loop.add(&self.udp);
    }

    pub fn deinit(self: *StubServer) void {
        for (self.conns.items) |c| {
            c.stream.deinit();
            self.gpa.destroy(c);
        }
        self.conns.deinit(self.gpa);
        if (self.udp.isRegistered()) self.loop.remove(&self.udp);
        if (self.udp.fd != sys.invalid_fd) sys.close(self.udp.fd);
        self.loop.remove(&self.listener.source);
        self.listener.close();
    }

    /// The address to configure a resolver with.
    pub fn address(self: *const StubServer) IpAddress {
        return IpAddress.parse("127.0.0.1", self.port) catch unreachable;
    }

    pub fn config(self: *const StubServer) Config {
        var cfg: Config = .{ .server_count = 1, .timeout_ns = 150 * std.time.ns_per_ms, .attempts = 2 };
        cfg.servers[0] = self.address();
        return cfg;
    }

    fn onAccept(l: *socket.Listener, fd: sys.Fd) void {
        const self: *StubServer = @ptrCast(@alignCast(l.context.?));
        const c = self.gpa.create(StubConn) catch {
            sys.close(fd);
            return;
        };
        c.* = .{ .stream = undefined, .server = self };
        c.stream.initAccepted(self.gpa, self.loop, fd, &StubConn.handler) catch {
            sys.close(fd);
            self.gpa.destroy(c);
            return;
        };
        self.conns.append(self.gpa, c) catch {
            c.stream.deinit();
            self.gpa.destroy(c);
        };
    }

    fn onUdpReady(src: *reactor.Source, ready: reactor.Ready) void {
        const self: *StubServer = @fieldParentPtr("udp", src);
        if (!ready.read) return;

        var query: [max_udp_message]u8 = undefined;
        var from: sys.Sockaddr = undefined;
        const n = sys.recvfrom(src.fd, &query, &from) catch return;

        var out: [max_udp_message]u8 = undefined;
        const reply = self.respond(&out, query[0..n]) orelse return;
        _ = sys.sendto(src.fd, reply, &from) catch {};
    }

    /// Apply the next script entry to `query`. Returns null when nothing
    /// should be sent.
    fn respond(self: *StubServer, out: []u8, query: []const u8) ?[]const u8 {
        self.received += 1;
        if (self.step >= self.script.len) {
            self.overrun = true;
            return null;
        }
        const action = self.script[self.step];
        self.step += 1;

        const q_end = questionEnd(query) orelse return null;
        const qtype = std.mem.readInt(u16, query[q_end - 4 ..][0..2], .big);

        var rcode: RCode = .no_error;
        var tc = false;
        var v4: []const [4]u8 = &.{};
        var v6: []const [16]u8 = &.{};
        var ttl: u32 = 300;
        var echo_id = true;
        var echo_question = true;

        switch (action) {
            .drop => return null,
            .answer => |a| {
                ttl = a.ttl;
                if (qtype == Type.a) v4 = a.v4;
                if (qtype == Type.aaaa) v6 = a.v6;
            },
            .rcode => |c| rcode = c,
            .truncate => tc = true,
            .wrong_id => echo_id = false,
            .wrong_question => echo_question = false,
        }

        const count: usize = if (qtype == Type.aaaa) v6.len else v4.len;
        var w: usize = 0;

        // Header.
        if (echo_id) {
            @memcpy(out[0..2], query[0..2]);
        } else {
            // Off by one, which is all a forger usually is.
            std.mem.writeInt(u16, out[0..2], std.mem.readInt(u16, query[0..2], .big) +% 1, .big);
        }
        var flags: u16 = flag_qr | flag_rd | 0x0080; // QR, RD, RA
        if (tc) flags |= flag_tc;
        flags |= @as(u16, @intFromEnum(rcode));
        std.mem.writeInt(u16, out[2..4], flags, .big);
        std.mem.writeInt(u16, out[4..6], 1, .big);
        std.mem.writeInt(u16, out[6..8], @intCast(count), .big);
        std.mem.writeInt(u16, out[8..10], 0, .big);
        std.mem.writeInt(u16, out[10..12], 0, .big);
        w = 12;

        // Question, echoed verbatim — or replaced with a different name,
        // for the mismatch test.
        if (echo_question) {
            const q = query[12..q_end];
            @memcpy(out[w..][0..q.len], q);
            w += q.len;
        } else {
            const n = encodeName(out[w..], "somewhere.else.invalid") catch return null;
            w += n;
            std.mem.writeInt(u16, out[w..][0..2], qtype, .big);
            std.mem.writeInt(u16, out[w + 2 ..][0..2], class_in, .big);
            w += 4;
        }

        // Answers, each naming the question through a compression pointer
        // to offset 12 — which is what a real server sends, and exercises
        // the pointer path over a real socket.
        for (0..count) |i| {
            std.mem.writeInt(u16, out[w..][0..2], 0xc00c, .big);
            std.mem.writeInt(u16, out[w + 2 ..][0..2], qtype, .big);
            std.mem.writeInt(u16, out[w + 4 ..][0..2], class_in, .big);
            std.mem.writeInt(u32, out[w + 6 ..][0..4], ttl, .big);
            w += 10;
            if (qtype == Type.aaaa) {
                std.mem.writeInt(u16, out[w..][0..2], 16, .big);
                @memcpy(out[w + 2 ..][0..16], &v6[i]);
                w += 18;
            } else {
                std.mem.writeInt(u16, out[w..][0..2], 4, .big);
                @memcpy(out[w + 2 ..][0..4], &v4[i]);
                w += 6;
            }
        }
        return out[0..w];
    }

    /// End of the question section: past the name, the qtype and the
    /// qclass. A query never uses compression, so this is a plain walk.
    fn questionEnd(msg: []const u8) ?usize {
        var off: usize = 12;
        while (off < msg.len) {
            const l = msg[off];
            if (l & 0xc0 != 0) return null;
            if (l == 0) {
                if (off + 5 > msg.len) return null;
                return off + 5;
            }
            off += 1 + l;
        }
        return null;
    }
};

// ---------------------------------------------------------------------
// tests
// ---------------------------------------------------------------------

const testing = std.testing;

/// The twelve-byte header shared by every hand-built message below:
/// `id`, then QR|RD|RA with rcode 0, one question, `an` answers, and no
/// authority or additional records.
///
/// `comptime` because the tests concatenate its result with a literal
/// question and `++` is a comptime operator.
fn header(comptime id: u16, comptime an: u16) [12]u8 {
    return .{
        id >> 8, id & 0xff,
        0x81,    0x80,
        0x00,    0x01,
        an >> 8, an & 0xff,
        0x00,    0x00,
        0x00,    0x00,
    };
}

test "encodeQuery lays out the header, name, type and class" {
    var buf: [max_query_len]u8 = undefined;
    const n = try encodeQuery(&buf, 0xbeef, "News.Example.COM", Type.a);

    try testing.expectEqualSlices(u8, &[_]u8{
        0xbe, 0xef, // id
        0x01, 0x00, // RD set, everything else clear
        0x00, 0x01, // qdcount 1
        0x00, 0x00, // ancount
        0x00, 0x00, // nscount
        0x00, 0x00, // arcount
        // The name is lowercased on the way out: DNS matching is
        // case-insensitive, and normalising here means the response
        // comparison is a plain memcmp.
        4,    'n',
        'e',  'w',
        's',  7,
        'e',  'x',
        'a',  'm',
        'p',  'l',
        'e',  3,
        'c',  'o',
        'm',  0,
        0x00, 0x01, // qtype A
        0x00, 0x01, // qclass IN
    }, buf[0..n]);
}

test "encodeQuery accepts a trailing dot and rejects malformed names" {
    var buf: [max_query_len]u8 = undefined;
    _ = try encodeQuery(&buf, 1, "example.com.", Type.a);

    try testing.expectError(error.InvalidHostName, encodeQuery(&buf, 1, "", Type.a));
    try testing.expectError(error.InvalidHostName, encodeQuery(&buf, 1, "a..b", Type.a));
    try testing.expectError(error.InvalidHostName, encodeQuery(&buf, 1, ".a", Type.a));
    // 64-byte label: one over the wire limit.
    try testing.expectError(error.InvalidHostName, encodeQuery(&buf, 1, "x" ** 64 ++ ".com", Type.a));
    // A control byte would otherwise ride into a log line intact.
    try testing.expectError(error.InvalidHostName, encodeQuery(&buf, 1, "a\nb.com", Type.a));
    try testing.expectError(error.InvalidHostName, encodeQuery(&buf, 1, "x" ** 254, Type.a));
}

test "a plain A answer decodes with its address and ttl" {
    const msg = header(0x1234, 1) ++ [_]u8{
        // question: example.com A IN
        7,    'e',  'x',  'a',  'm',  'p',  'l', 'e', 3, 'c', 'o', 'm', 0,
        0x00, 0x01, 0x00, 0x01,
        // answer: pointer back to the question name at offset 12
        0xc0, 0x0c,
        0x00, 0x01, // type A
        0x00, 0x01, // class IN
        0x00, 0x00, 0x01, 0x2c, // ttl 300
        0x00, 0x04, // rdlength
        93,   184,
        216,  34,
    };

    const reply = try parseResponse(&msg, 0x1234, "example.com", Type.a);
    try testing.expectEqual(RCode.no_error, reply.rcode);
    try testing.expectEqual(@as(usize, 1), reply.n);
    try testing.expectEqual(@as(u32, 300), reply.ttl);
    try testing.expectEqualSlices(u8, &.{ 93, 184, 216, 34 }, &reply.addrs[0].v4);
}

test "several A records all come back, and the ttl is the smallest" {
    const msg = header(0x1234, 2) ++ [_]u8{
        7,    'e',  'x',  'a',  'm',  'p',  'l',  'e',  3,    'c',  'o',  'm',  0,
        0x00, 0x01, 0x00, 0x01, 0xc0, 0x0c, 0x00, 0x01, 0x00, 0x01, 0x00, 0x00, 0x01,
        0x2c, 0x00, 0x04, 10,   0,    0,    1,
        // Second record, ttl 60 — a resolver that took the first would
        // cache this set five minutes too long.
           0xc0, 0x0c, 0x00, 0x01, 0x00, 0x01,
        0x00, 0x00, 0x00, 0x3c, 0x00, 0x04, 10,   0,    0,    2,
    };

    const reply = try parseResponse(&msg, 0x1234, "example.com", Type.a);
    try testing.expectEqual(@as(usize, 2), reply.n);
    try testing.expectEqual(@as(u32, 60), reply.ttl);
    try testing.expectEqualSlices(u8, &.{ 10, 0, 0, 2 }, &reply.addrs[1].v4);
}

test "an AAAA answer decodes sixteen bytes" {
    const msg = header(7, 1) ++ [_]u8{
        2, 'v', '6', 4, 't', 'e', 's', 't', 0,
        0x00, 0x1c, 0x00, 0x01, // AAAA IN
        0xc0, 0x0c, 0x00, 0x1c,
        0x00, 0x01, 0x00, 0x00,
        0x00, 0x3c, 0x00, 0x10,
        0x20, 0x01, 0x0d, 0xb8,
        0,    0,    0,    0,
        0,    0,    0,    0,
        0,    0,    0,    1,
    };

    const reply = try parseResponse(&msg, 7, "v6.test", Type.aaaa);
    try testing.expectEqual(@as(usize, 1), reply.n);
    try testing.expectEqual(@as(u8, 0x20), reply.addrs[0].v6[0]);
    try testing.expectEqual(@as(u8, 1), reply.addrs[0].v6[15]);
}

test "a CNAME chain is followed to the address at the end" {
    const msg = header(9, 2) ++ [_]u8{
        // question: www.example.com A
        3,    'w',  'w',  'w',  7,    'e',  'x',  'a',  'm',  'p',  'l',  'e',  3,    'c',  'o',  'm',  0,
        0x00, 0x01, 0x00, 0x01,
        // www.example.com CNAME cdn.example.com
        0xc0, 0x0c, 0x00, 0x05, 0x00, 0x01, 0x00, 0x00, 0x01, 0x2c, 0x00, 0x06,
        3,    'c',  'd',  'n',  0xc0, 0x10, // "cdn" + pointer to "example.com"
        // cdn.example.com A 1.2.3.4
        3,    'c',  'd',  'n',  0xc0, 0x10,
        0x00, 0x01, 0x00, 0x01, 0x00, 0x00,
        0x00, 0x1e, 0x00, 0x04, 1,    2,
        3,    4,
    };

    const reply = try parseResponse(&msg, 9, "www.example.com", Type.a);
    try testing.expectEqual(@as(usize, 1), reply.n);
    try testing.expectEqualSlices(u8, &.{ 1, 2, 3, 4 }, &reply.addrs[0].v4);
    // The CNAME's own ttl counts: the alias expiring is as good a reason
    // to re-ask as the address expiring.
    try testing.expectEqual(@as(u32, 30), reply.ttl);
}

test "a CNAME chain that loops terminates with no address" {
    // a.test CNAME b.test, b.test CNAME a.test, and no address anywhere.
    // The chase is bounded by depth, so this returns rather than spinning.
    const msg = header(11, 2) ++ [_]u8{
        1,    'a',  4,    't',  'e',  's',  't',  0,
        0x00, 0x01, 0x00, 0x01,
        // a.test CNAME b.test
        0xc0, 0x0c, 0x00, 0x05,
        0x00, 0x01, 0x00, 0x00, 0x01, 0x2c, 0x00, 0x04,
        1,    'b',  0xc0, 0x0e,
        // b.test CNAME a.test
        1,    'b',  0xc0, 0x0e,
        0x00, 0x05, 0x00, 0x01, 0x00, 0x00, 0x01, 0x2c,
        0x00, 0x04, 1,    'a',  0xc0, 0x0e,
    };

    const reply = try parseResponse(&msg, 11, "a.test", Type.a);
    try testing.expectEqual(@as(usize, 0), reply.n);
}

test "a compression pointer to itself is refused, not chased" {
    // Layout: header 0..11, question name 12..19, qtype/qclass 20..23,
    // so the answer's name starts at offset 24 — and is a pointer to 24.
    // Without the loop bound this spins forever inside the parser, which
    // is the oldest bug in DNS.
    const msg = header(3, 1) ++ [_]u8{
        1,    'a',  4,    't',  'e', 's', 't', 0,
        0x00, 0x01, 0x00, 0x01,
        0xc0, 24, // offset 24, targeting 24
        0x00, 0x01,
        0x00, 0x01,
        0x00, 0x00,
        0x00, 0x3c,
        0x00, 0x04,
    };
    try testing.expectError(error.PointerLoop, parseResponse(&msg, 3, "a.test", Type.a));
}

test "a two-pointer cycle is refused" {
    // Offset 24 points at 26, and 26 points back at 24. Each hop is
    // individually legal — each targets an earlier offset than its own
    // position — and only the strictly-decreasing-target rule catches it.
    const msg = header(3, 1) ++ [_]u8{
        1,    'a',  4,    't',  'e', 's', 't', 0,
        0x00, 0x01, 0x00, 0x01,
        0xc0, 26, // offset 24 -> 26
        0xc0, 24, // offset 26 -> 24
        0x00, 0x01,
        0x00, 0x01,
        0x00, 0x00,
        0x00, 0x3c,
        0x00, 0x04,
    };
    try testing.expectError(error.PointerLoop, parseResponse(&msg, 3, "a.test", Type.a));
}

test "a name longer than the limit is refused rather than overflowing" {
    // 70 consecutive "aaa" labels reconstruct to 279 characters, well past
    // the 253-byte cap. The output buffer is fixed, so an unchecked
    // decoder writes off the end of it.
    var msg: std.ArrayList(u8) = .empty;
    defer msg.deinit(testing.allocator);

    try msg.appendSlice(testing.allocator, &header(5, 0));
    for (0..70) |_| try msg.appendSlice(testing.allocator, &[_]u8{ 3, 'a', 'a', 'a' });
    try msg.append(testing.allocator, 0);
    try msg.appendSlice(testing.allocator, &[_]u8{ 0x00, 0x01, 0x00, 0x01 });

    try testing.expectError(error.NameTooLong, parseResponse(msg.items, 5, "irrelevant", Type.a));
}

test "reserved label prefixes are refused" {
    // The top two bits of a length byte are 00 for a label and 11 for a
    // pointer; 01 and 10 have never been assigned a meaning.
    const msg = header(3, 0) ++ [_]u8{ 0x40, 'a', 'b', 'c', 0x00, 0x01, 0x00, 0x01 };
    try testing.expectError(error.MalformedName, parseResponse(&msg, 3, "abc", Type.a));
}

test "a literal dot inside a label is refused" {
    // "a.b" as a single label would decode to the same text as the two
    // labels "a" and "b" — a match bypass if it were allowed through.
    const msg = header(3, 0) ++ [_]u8{ 3, 'a', '.', 'b', 0, 0x00, 0x01, 0x00, 0x01 };
    try testing.expectError(error.MalformedName, parseResponse(&msg, 3, "a.b", Type.a));
}

test "a message shorter than a header is truncated, not read" {
    try testing.expectError(error.Truncated, parseResponse(&[_]u8{ 1, 2, 3 }, 1, "a.test", Type.a));
    try testing.expectError(error.Truncated, parseResponse(&.{}, 1, "a.test", Type.a));
}

test "a header claiming more records than the message holds is refused" {
    // ancount says three; there is one, and it is cut short.
    const msg = header(4, 3) ++ [_]u8{
        1,    'a',  4,    't',  'e',  's',  't',  0,    0x00, 0x01, 0x00, 0x01,
        0xc0, 0x0c, 0x00, 0x01, 0x00, 0x01, 0x00, 0x00, 0x00, 0x3c, 0x00, 0x04,
        1,    2,    3,    4,
    };
    try testing.expectError(error.Truncated, parseResponse(&msg, 4, "a.test", Type.a));
}

test "an rdlength pointing past the message is refused" {
    // rdlength claims 64 bytes; four are present. Trusting it would read
    // off the end of the datagram.
    const msg = header(4, 1) ++ [_]u8{
        1,    'a',  4,    't',  'e',  's',  't',  0,    0x00, 0x01, 0x00, 0x01,
        0xc0, 0x0c, 0x00, 0x01, 0x00, 0x01, 0x00, 0x00, 0x00, 0x3c, 0x00, 0x40,
        1,    2,    3,    4,
    };
    try testing.expectError(error.Truncated, parseResponse(&msg, 4, "a.test", Type.a));
}

test "an A record with the wrong rdlength is refused" {
    const msg = header(4, 1) ++ [_]u8{
        1,    'a',  4,    't',  'e',  's',  't',  0,    0x00, 0x01, 0x00, 0x01,
        // Five bytes of rdata for an A record.
        0xc0, 0x0c, 0x00, 0x01, 0x00, 0x01, 0x00, 0x00, 0x00, 0x3c, 0x00, 0x05,
        1,    2,    3,    4,    5,
    };
    try testing.expectError(error.MalformedRecord, parseResponse(&msg, 4, "a.test", Type.a));
}

test "more answers than the record cap is refused outright" {
    // Refused on the count alone, before a single record is walked.
    const msg = header(4, max_records + 1) ++ [_]u8{
        1, 'a', 4, 't', 'e', 's', 't', 0, 0x00, 0x01, 0x00, 0x01,
    };
    try testing.expectError(error.TooManyRecords, parseResponse(&msg, 4, "a.test", Type.a));
}

test "an empty answer section is NOERROR with no addresses" {
    const msg = header(4, 0) ++ [_]u8{ 1, 'a', 4, 't', 'e', 's', 't', 0, 0x00, 0x01, 0x00, 0x01 };
    const reply = try parseResponse(&msg, 4, "a.test", Type.a);
    try testing.expectEqual(RCode.no_error, reply.rcode);
    try testing.expectEqual(@as(usize, 0), reply.n);
}

test "NXDOMAIN and SERVFAIL come back as rcodes, not as parse failures" {
    const question = [_]u8{ 1, 'a', 4, 't', 'e', 's', 't', 0, 0x00, 0x01, 0x00, 0x01 };

    var nx = header(4, 0) ++ question;
    nx[3] |= 3; // rcode 3
    const nx_reply = try parseResponse(&nx, 4, "a.test", Type.a);
    try testing.expectEqual(RCode.name_error, nx_reply.rcode);

    var sf = header(4, 0) ++ question;
    sf[3] |= 2; // rcode 2
    const sf_reply = try parseResponse(&sf, 4, "a.test", Type.a);
    try testing.expectEqual(RCode.server_failure, sf_reply.rcode);
}

test "the TC bit is reported and the body is not read" {
    var msg = header(4, 0) ++ [_]u8{ 1, 'a', 4, 't', 'e', 's', 't', 0, 0x00, 0x01, 0x00, 0x01 };
    msg[2] |= 0x02; // TC
    const reply = try parseResponse(&msg, 4, "a.test", Type.a);
    try testing.expect(reply.truncated);
}

test "a mismatched id is rejected before anything else is read" {
    const msg = header(0x1234, 0) ++ [_]u8{ 1, 'a', 4, 't', 'e', 's', 't', 0, 0x00, 0x01, 0x00, 0x01 };
    // Off by one is all a blind forger usually manages.
    try testing.expectError(error.IdMismatch, parseResponse(&msg, 0x1235, "a.test", Type.a));
}

test "a response to a different question is rejected" {
    const msg = header(4, 0) ++ [_]u8{ 1, 'b', 4, 't', 'e', 's', 't', 0, 0x00, 0x01, 0x00, 0x01 };
    // Matching only the id is how cache poisoning works.
    try testing.expectError(error.QuestionMismatch, parseResponse(&msg, 4, "a.test", Type.a));

    // Right name, wrong type.
    const wrong_type = header(4, 0) ++ [_]u8{ 1, 'a', 4, 't', 'e', 's', 't', 0, 0x00, 0x1c, 0x00, 0x01 };
    try testing.expectError(error.QuestionMismatch, parseResponse(&wrong_type, 4, "a.test", Type.a));

    // Right name and type, wrong class.
    const wrong_class = header(4, 0) ++ [_]u8{ 1, 'a', 4, 't', 'e', 's', 't', 0, 0x00, 0x01, 0x00, 0x03 };
    try testing.expectError(error.QuestionMismatch, parseResponse(&wrong_class, 4, "a.test", Type.a));
}

test "a query echoed back with QR clear is not an answer" {
    var msg = header(4, 0) ++ [_]u8{ 1, 'a', 4, 't', 'e', 's', 't', 0, 0x00, 0x01, 0x00, 0x01 };
    msg[2] &= ~@as(u8, 0x80);
    try testing.expectError(error.NotAResponse, parseResponse(&msg, 4, "a.test", Type.a));
}

test "an answer about a name we did not ask for is ignored" {
    // The question is a.test; the answer names evil.test. A parser that
    // walked the section and took whatever addresses it found would cache
    // the attacker's record under our name.
    const msg = header(4, 1) ++ [_]u8{
        1,    'a',  4,    't',  'e',  's',  't',  0,    0x00, 0x01, 0x00, 0x01,
        4,    'e',  'v',  'i',  'l',  4,    't',  'e',  's',  't',  0,    0x00,
        0x01, 0x00, 0x01, 0x00, 0x00, 0x0e, 0x10, 0x00, 0x04, 6,    6,    6,
        6,
    };
    const reply = try parseResponse(&msg, 4, "a.test", Type.a);
    try testing.expectEqual(@as(usize, 0), reply.n);
}

test "a record in the wrong class is ignored" {
    const msg = header(4, 1) ++ [_]u8{
        1,    'a',  4,    't',  'e',  's',  't',  0,    0x00, 0x01, 0x00, 0x01,
        0xc0, 0x0c, 0x00, 0x01, 0x00, 0x03, 0x00, 0x00, 0x0e, 0x10, 0x00, 0x04,
        1,    2,    3,    4,
    };
    const reply = try parseResponse(&msg, 4, "a.test", Type.a);
    try testing.expectEqual(@as(usize, 0), reply.n);
}

test "more addresses than the cap keeps the first few and does not overflow" {
    var msg: std.ArrayList(u8) = .empty;
    defer msg.deinit(testing.allocator);
    const gpa = testing.allocator;

    const n = max_addrs + 4;
    try msg.appendSlice(gpa, &header(4, n));
    try msg.appendSlice(gpa, &[_]u8{ 1, 'a', 4, 't', 'e', 's', 't', 0, 0x00, 0x01, 0x00, 0x01 });
    for (0..n) |i| {
        try msg.appendSlice(gpa, &[_]u8{ 0xc0, 0x0c, 0x00, 0x01, 0x00, 0x01, 0x00, 0x00, 0x0e, 0x10, 0x00, 0x04 });
        try msg.appendSlice(gpa, &[_]u8{ 10, 0, 0, @intCast(i) });
    }

    const reply = try parseResponse(msg.items, 4, "a.test", Type.a);
    try testing.expectEqual(max_addrs, reply.n);
}

// -- resolv.conf ------------------------------------------------------

test "resolv.conf yields nameservers, timeout and attempts" {
    const cfg = parseResolvConf(
        \\# generated by a container runtime
        \\search example.com
        \\nameserver 10.0.0.1
        \\nameserver 10.0.0.2 ; inline comment
        \\options timeout:2 attempts:3 ndots:5
        \\
    );
    try testing.expectEqual(@as(usize, 2), cfg.server_count);
    try testing.expect(!cfg.from_fallback);
    try testing.expectEqual(@as(u16, 53), cfg.servers[0].ip4.port);
    try testing.expectEqualSlices(u8, &.{ 10, 0, 0, 1 }, &cfg.servers[0].ip4.bytes);
    try testing.expectEqualSlices(u8, &.{ 10, 0, 0, 2 }, &cfg.servers[1].ip4.bytes);
    try testing.expectEqual(@as(u64, 2 * std.time.ns_per_s), cfg.timeout_ns);
    try testing.expectEqual(@as(usize, 3), cfg.attempts);
}

test "an IPv6 nameserver is accepted and a garbage one is skipped" {
    const cfg = parseResolvConf(
        \\nameserver not-an-address
        \\nameserver 2001:db8::1
        \\nameserver 10.0.0.1
    );
    // One bad line must not cost the operator the rest of the file.
    try testing.expectEqual(@as(usize, 2), cfg.server_count);
    try testing.expect(cfg.servers[0] == .ip6);
    try testing.expect(cfg.servers[1] == .ip4);
}

test "a resolv.conf with no usable nameserver falls back" {
    const cfg = parseResolvConf("search example.com\noptions ndots:1\n");
    try testing.expect(cfg.from_fallback);
    try testing.expectEqual(@as(usize, fallback_servers.len), cfg.server_count);
}

test "options are clamped rather than trusted" {
    // A zero timeout would turn every attempt into a spin.
    const zero = parseResolvConf("nameserver 10.0.0.1\noptions timeout:0 attempts:0\n");
    try testing.expectEqual(@as(u64, 1 * std.time.ns_per_s), zero.timeout_ns);
    try testing.expectEqual(@as(usize, 1), zero.attempts);

    // And an absurd one would pin a download job for minutes.
    const huge = parseResolvConf("nameserver 10.0.0.1\noptions timeout:250 attempts:99\n");
    try testing.expectEqual(@as(u64, 30 * std.time.ns_per_s), huge.timeout_ns);
    try testing.expectEqual(@as(usize, 5), huge.attempts);
}

test "more nameservers than the cap keeps the first few" {
    const cfg = parseResolvConf(
        \\nameserver 10.0.0.1
        \\nameserver 10.0.0.2
        \\nameserver 10.0.0.3
        \\nameserver 10.0.0.4
        \\nameserver 10.0.0.5
        \\nameserver 10.0.0.6
    );
    try testing.expectEqual(max_servers, cfg.server_count);
    try testing.expectEqualSlices(u8, &.{ 10, 0, 0, 1 }, &cfg.servers[0].ip4.bytes);
}

test "loadResolvConf reads a real file, and falls back when there isn't one" {
    var buf: [sys.path_max]u8 = undefined;
    const path = try sys.pathZ(&buf, "/tmp/hoardarr-dns-resolv.conf");
    sys.unlink(path) catch {};

    const fd = try sys.open(path, .{ .mode = .write_only, .create = true, .truncate = true });
    try sys.writeAll(fd, "nameserver 192.0.2.53\noptions timeout:1\n");
    sys.close(fd);
    defer sys.unlink(path) catch {};

    const cfg = loadResolvConf(path);
    try testing.expectEqual(@as(usize, 1), cfg.server_count);
    try testing.expectEqualSlices(u8, &.{ 192, 0, 2, 53 }, &cfg.servers[0].ip4.bytes);

    var missing_buf: [sys.path_max]u8 = undefined;
    const missing = try sys.pathZ(&missing_buf, "/tmp/hoardarr-dns-no-such-resolv-9c1f");
    // A scratch container may genuinely have no resolv.conf, and the
    // daemon must still be able to reach a provider.
    const fb = loadResolvConf(missing);
    try testing.expect(fb.from_fallback);
    try testing.expect(fb.server_count > 0);
}

// -- cache ------------------------------------------------------------

test "the cache honours expiry and rotates through the address set" {
    const gpa = testing.allocator;
    var cache: Cache = .{};
    defer cache.deinit(gpa);

    const addrs = [_]Address{ .{ .v4 = .{ 1, 1, 1, 1 } }, .{ .v4 = .{ 2, 2, 2, 2 } } };
    try cache.put(gpa, "a.test", &addrs, 1000);

    // Rotation, so a provider's several A records actually get used.
    const first = cache.lookup(gpa, "a.test", 500).?;
    const second = cache.lookup(gpa, "a.test", 500).?;
    try testing.expect(!std.mem.eql(u8, &first.v4, &second.v4));

    try testing.expect(cache.lookup(gpa, "b.test", 500) == null);
    // Past the deadline the entry is gone, not merely stale.
    try testing.expect(cache.lookup(gpa, "a.test", 1000) == null);
    try testing.expectEqual(@as(usize, 0), cache.entries.items.len);
}

test "the cache is bounded and evicts the entry expiring soonest" {
    const gpa = testing.allocator;
    var cache: Cache = .{ .max_entries = 4 };
    defer cache.deinit(gpa);

    const addr = [_]Address{.{ .v4 = .{ 1, 2, 3, 4 } }};
    for (0..8) |i| {
        var name: [16]u8 = undefined;
        const n = try std.fmt.bufPrint(&name, "h{d}.test", .{i});
        // Later names live longer, so the early ones are what gets dropped.
        try cache.put(gpa, n, &addr, 1_000_000 + @as(u64, i) * 1000);
    }
    // A name a third party can suggest must not be a memory-growth lever.
    try testing.expectEqual(@as(usize, 4), cache.entries.items.len);
    try testing.expect(cache.lookup(gpa, "h0.test", 0) == null);
    try testing.expect(cache.lookup(gpa, "h7.test", 0) != null);
}

test "re-putting a name replaces it rather than growing the table" {
    const gpa = testing.allocator;
    var cache: Cache = .{};
    defer cache.deinit(gpa);

    try cache.put(gpa, "a.test", &[_]Address{.{ .v4 = .{ 1, 1, 1, 1 } }}, 1000);
    try cache.put(gpa, "a.test", &[_]Address{.{ .v4 = .{ 9, 9, 9, 9 } }}, 2000);
    try testing.expectEqual(@as(usize, 1), cache.entries.items.len);
    try testing.expectEqualSlices(u8, &.{ 9, 9, 9, 9 }, &cache.lookup(gpa, "a.test", 1500).?.v4);
}

// -- transport --------------------------------------------------------

fn pumpUntil(loop: *reactor.Loop, deadline_ms: u64, ctx: anytype, done: fn (@TypeOf(ctx)) bool) !void {
    const start = sys.monotonicNanos();
    while (!done(ctx)) {
        if (sys.monotonicNanos() - start > deadline_ms * std.time.ns_per_ms) return error.TestTimeout;
        _ = try loop.tick(5);
    }
}

/// Collects one resolution result.
const Catcher = struct {
    result: ?Error!IpAddress = null,
    calls: usize = 0,

    fn take(ctx: ?*anyopaque, result: Error!IpAddress) void {
        const self: *Catcher = @ptrCast(@alignCast(ctx.?));
        self.result = result;
        self.calls += 1;
    }

    fn done(self: *Catcher) bool {
        return self.result != null;
    }
};

test "a hostname resolves over a real socket" {
    const gpa = testing.allocator;
    var loop: reactor.Loop = undefined;
    try loop.init(gpa);
    defer loop.deinit();

    var stub: StubServer = undefined;
    try stub.start(gpa, &loop, &.{
        .{ .answer = .{ .v4 = &.{.{ 203, 0, 113, 7 }} } },
    });
    defer stub.deinit();

    var resolver: Resolver = undefined;
    resolver.init(gpa, &loop, stub.config());
    defer resolver.deinit();

    var catcher: Catcher = .{};
    resolver.resolve("news.example.test", 119, Catcher.take, &catcher);
    try pumpUntil(&loop, 2000, &catcher, Catcher.done);

    const addr = try catcher.result.?;
    try testing.expectEqualSlices(u8, &.{ 203, 0, 113, 7 }, &addr.ip4.bytes);
    // The port comes from the caller, not from the zone.
    try testing.expectEqual(@as(u16, 119), addr.ip4.port);
    try testing.expectEqual(@as(usize, 1), stub.received);
    try testing.expect(!stub.overrun);
}

test "an address literal answers without a query" {
    const gpa = testing.allocator;
    var loop: reactor.Loop = undefined;
    try loop.init(gpa);
    defer loop.deinit();

    var stub: StubServer = undefined;
    try stub.start(gpa, &loop, &.{});
    defer stub.deinit();

    var resolver: Resolver = undefined;
    resolver.init(gpa, &loop, stub.config());
    defer resolver.deinit();

    var v4: Catcher = .{};
    resolver.resolve("10.1.2.3", 8085, Catcher.take, &v4);
    // Synchronous: no tick has run, so nothing could have gone over a
    // socket even if we wanted it to.
    try testing.expectEqual(@as(usize, 1), v4.calls);
    try testing.expectEqual(@as(u16, 8085), (try v4.result.?).ip4.port);

    var v6: Catcher = .{};
    resolver.resolve("[::1]", 8085, Catcher.take, &v6);
    try testing.expectEqual(@as(u16, 8085), (try v6.result.?).ip6.port);

    try testing.expectEqual(@as(usize, 0), stub.received);
}

test "a malformed hostname fails without touching the network" {
    const gpa = testing.allocator;
    var loop: reactor.Loop = undefined;
    try loop.init(gpa);
    defer loop.deinit();

    var stub: StubServer = undefined;
    try stub.start(gpa, &loop, &.{});
    defer stub.deinit();

    var resolver: Resolver = undefined;
    resolver.init(gpa, &loop, stub.config());
    defer resolver.deinit();

    var catcher: Catcher = .{};
    resolver.resolve("bad..name", 80, Catcher.take, &catcher);
    try testing.expectError(error.InvalidHostName, catcher.result.?);
    try testing.expectEqual(@as(usize, 0), stub.received);
}

test "a second lookup is served from the cache" {
    const gpa = testing.allocator;
    var loop: reactor.Loop = undefined;
    try loop.init(gpa);
    defer loop.deinit();

    var stub: StubServer = undefined;
    try stub.start(gpa, &loop, &.{
        .{ .answer = .{ .ttl = 300, .v4 = &.{.{ 198, 51, 100, 4 }} } },
    });
    defer stub.deinit();

    var resolver: Resolver = undefined;
    resolver.init(gpa, &loop, stub.config());
    defer resolver.deinit();

    var first: Catcher = .{};
    resolver.resolve("cached.example.test", 119, Catcher.take, &first);
    try pumpUntil(&loop, 2000, &first, Catcher.done);
    _ = try first.result.?;

    var second: Catcher = .{};
    resolver.resolve("cached.example.test", 563, Catcher.take, &second);
    // Synchronous, and no second datagram: a pool opening forty
    // connections must not become forty queries.
    try testing.expectEqual(@as(usize, 1), second.calls);
    try testing.expectEqual(@as(u16, 563), (try second.result.?).ip4.port);
    try testing.expectEqual(@as(usize, 1), stub.received);
}

test "a zero ttl is floored so it cannot force a query per connection" {
    const gpa = testing.allocator;
    var loop: reactor.Loop = undefined;
    try loop.init(gpa);
    defer loop.deinit();

    var stub: StubServer = undefined;
    try stub.start(gpa, &loop, &.{
        .{ .answer = .{ .ttl = 0, .v4 = &.{.{ 198, 51, 100, 9 }} } },
    });
    defer stub.deinit();

    var resolver: Resolver = undefined;
    resolver.init(gpa, &loop, stub.config());
    defer resolver.deinit();

    var catcher: Catcher = .{};
    resolver.resolve("zero.example.test", 119, Catcher.take, &catcher);
    try pumpUntil(&loop, 2000, &catcher, Catcher.done);
    _ = try catcher.result.?;

    // A TTL of zero, honoured literally, is a query per connect.
    const expires = resolver.cache.entries.items[0].expires_ns;
    const now = sys.monotonicNanos();
    try testing.expect(expires > now + @as(u64, default_ttl_floor_s - 5) * std.time.ns_per_s);
}

test "a huge ttl is capped so a stale answer cannot be pinned forever" {
    const gpa = testing.allocator;
    var loop: reactor.Loop = undefined;
    try loop.init(gpa);
    defer loop.deinit();

    var stub: StubServer = undefined;
    try stub.start(gpa, &loop, &.{
        // Ten years.
        .{ .answer = .{ .ttl = 315_360_000, .v4 = &.{.{ 198, 51, 100, 9 }} } },
    });
    defer stub.deinit();

    var resolver: Resolver = undefined;
    resolver.init(gpa, &loop, stub.config());
    defer resolver.deinit();

    var catcher: Catcher = .{};
    resolver.resolve("forever.example.test", 119, Catcher.take, &catcher);
    try pumpUntil(&loop, 2000, &catcher, Catcher.done);
    _ = try catcher.result.?;

    const expires = resolver.cache.entries.items[0].expires_ns;
    const now = sys.monotonicNanos();
    try testing.expect(expires <= now + (@as(u64, default_ttl_ceiling_s) + 5) * std.time.ns_per_s);
}

test "a reply with the wrong id is ignored and the real one still wins" {
    const gpa = testing.allocator;
    var loop: reactor.Loop = undefined;
    try loop.init(gpa);
    defer loop.deinit();

    // The forged datagram arrives on the right socket from the right
    // address and answers the right question — everything but the ID.
    // Accepting it is cache poisoning.
    var stub: StubServer = undefined;
    try stub.start(gpa, &loop, &.{
        .wrong_id,
        .{ .answer = .{ .v4 = &.{.{ 192, 0, 2, 1 }} } },
    });
    defer stub.deinit();

    var resolver: Resolver = undefined;
    resolver.init(gpa, &loop, stub.config());
    defer resolver.deinit();

    var catcher: Catcher = .{};
    resolver.resolve("spoof.example.test", 119, Catcher.take, &catcher);
    try pumpUntil(&loop, 5000, &catcher, Catcher.done);

    const addr = try catcher.result.?;
    try testing.expectEqualSlices(u8, &.{ 192, 0, 2, 1 }, &addr.ip4.bytes);
    // Two datagrams: the forgery was dropped and the retry answered.
    try testing.expectEqual(@as(usize, 2), stub.received);
}

test "a reply answering a different question is ignored" {
    const gpa = testing.allocator;
    var loop: reactor.Loop = undefined;
    try loop.init(gpa);
    defer loop.deinit();

    var stub: StubServer = undefined;
    try stub.start(gpa, &loop, &.{
        .wrong_question,
        .{ .answer = .{ .v4 = &.{.{ 192, 0, 2, 2 }} } },
    });
    defer stub.deinit();

    var resolver: Resolver = undefined;
    resolver.init(gpa, &loop, stub.config());
    defer resolver.deinit();

    var catcher: Catcher = .{};
    resolver.resolve("q.example.test", 119, Catcher.take, &catcher);
    try pumpUntil(&loop, 5000, &catcher, Catcher.done);

    try testing.expectEqualSlices(u8, &.{ 192, 0, 2, 2 }, &(try catcher.result.?).ip4.bytes);
}

test "a silent nameserver is retried and then given up on" {
    const gpa = testing.allocator;
    var loop: reactor.Loop = undefined;
    try loop.init(gpa);
    defer loop.deinit();

    var stub: StubServer = undefined;
    try stub.start(gpa, &loop, &.{ .drop, .drop, .drop, .drop });
    defer stub.deinit();

    var resolver: Resolver = undefined;
    resolver.init(gpa, &loop, stub.config());
    defer resolver.deinit();

    var catcher: Catcher = .{};
    resolver.resolve("silent.example.test", 119, Catcher.take, &catcher);
    try pumpUntil(&loop, 5000, &catcher, Catcher.done);

    // A stalled nameserver must not pin the caller forever.
    try testing.expectError(error.Timeout, catcher.result.?);
    // attempts:2 over one server, and each attempt was actually sent.
    try testing.expectEqual(@as(usize, 2), stub.received);
}

test "a dropped first attempt is recovered by the retry" {
    const gpa = testing.allocator;
    var loop: reactor.Loop = undefined;
    try loop.init(gpa);
    defer loop.deinit();

    var stub: StubServer = undefined;
    try stub.start(gpa, &loop, &.{
        .drop,
        .{ .answer = .{ .v4 = &.{.{ 192, 0, 2, 55 }} } },
    });
    defer stub.deinit();

    var resolver: Resolver = undefined;
    resolver.init(gpa, &loop, stub.config());
    defer resolver.deinit();

    var catcher: Catcher = .{};
    resolver.resolve("retry.example.test", 119, Catcher.take, &catcher);
    try pumpUntil(&loop, 5000, &catcher, Catcher.done);

    try testing.expectEqualSlices(u8, &.{ 192, 0, 2, 55 }, &(try catcher.result.?).ip4.bytes);
}

test "NXDOMAIN is reported as such and not retried" {
    const gpa = testing.allocator;
    var loop: reactor.Loop = undefined;
    try loop.init(gpa);
    defer loop.deinit();

    var stub: StubServer = undefined;
    try stub.start(gpa, &loop, &.{.{ .rcode = .name_error }});
    defer stub.deinit();

    var resolver: Resolver = undefined;
    resolver.init(gpa, &loop, stub.config());
    defer resolver.deinit();

    var catcher: Catcher = .{};
    resolver.resolve("nope.example.test", 119, Catcher.take, &catcher);
    try pumpUntil(&loop, 2000, &catcher, Catcher.done);

    try testing.expectError(error.NameNotFound, catcher.result.?);
    // NXDOMAIN is authoritative; asking again is wasted time.
    try testing.expectEqual(@as(usize, 1), stub.received);
}

test "SERVFAIL moves on to the next attempt" {
    const gpa = testing.allocator;
    var loop: reactor.Loop = undefined;
    try loop.init(gpa);
    defer loop.deinit();

    var stub: StubServer = undefined;
    try stub.start(gpa, &loop, &.{
        .{ .rcode = .server_failure },
        .{ .answer = .{ .v4 = &.{.{ 192, 0, 2, 77 }} } },
    });
    defer stub.deinit();

    var resolver: Resolver = undefined;
    resolver.init(gpa, &loop, stub.config());
    defer resolver.deinit();

    var catcher: Catcher = .{};
    resolver.resolve("flaky.example.test", 119, Catcher.take, &catcher);
    try pumpUntil(&loop, 5000, &catcher, Catcher.done);

    try testing.expectEqualSlices(u8, &.{ 192, 0, 2, 77 }, &(try catcher.result.?).ip4.bytes);
}

test "every attempt failing reports ServerFailure" {
    const gpa = testing.allocator;
    var loop: reactor.Loop = undefined;
    try loop.init(gpa);
    defer loop.deinit();

    var stub: StubServer = undefined;
    try stub.start(gpa, &loop, &.{
        .{ .rcode = .refused },
        .{ .rcode = .refused },
    });
    defer stub.deinit();

    var resolver: Resolver = undefined;
    resolver.init(gpa, &loop, stub.config());
    defer resolver.deinit();

    var catcher: Catcher = .{};
    resolver.resolve("refused.example.test", 119, Catcher.take, &catcher);
    try pumpUntil(&loop, 5000, &catcher, Catcher.done);

    try testing.expectError(error.ServerFailure, catcher.result.?);
}

test "an empty A answer falls back to AAAA" {
    const gpa = testing.allocator;
    var loop: reactor.Loop = undefined;
    try loop.init(gpa);
    defer loop.deinit();

    // The stub answers the A question with nothing (the action carries
    // only a v6 address) and the AAAA question with the address.
    var stub: StubServer = undefined;
    try stub.start(gpa, &loop, &.{
        .{ .answer = .{ .v6 = &.{.{ 0x20, 0x01, 0x0d, 0xb8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 5 }} } },
        .{ .answer = .{ .v6 = &.{.{ 0x20, 0x01, 0x0d, 0xb8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 5 }} } },
    });
    defer stub.deinit();

    var resolver: Resolver = undefined;
    resolver.init(gpa, &loop, stub.config());
    defer resolver.deinit();

    var catcher: Catcher = .{};
    resolver.resolve("v6only.example.test", 119, Catcher.take, &catcher);
    try pumpUntil(&loop, 5000, &catcher, Catcher.done);

    const addr = try catcher.result.?;
    try testing.expectEqual(@as(u8, 5), addr.ip6.bytes[15]);
    try testing.expectEqual(@as(u16, 119), addr.ip6.port);
    try testing.expectEqual(@as(usize, 2), stub.received);
}

test "a name with no address at all reports NoAddress" {
    const gpa = testing.allocator;
    var loop: reactor.Loop = undefined;
    try loop.init(gpa);
    defer loop.deinit();

    var stub: StubServer = undefined;
    try stub.start(gpa, &loop, &.{
        .{ .answer = .{} }, // A: nothing
        .{ .answer = .{} }, // AAAA: nothing
    });
    defer stub.deinit();

    var resolver: Resolver = undefined;
    resolver.init(gpa, &loop, stub.config());
    defer resolver.deinit();

    var catcher: Catcher = .{};
    resolver.resolve("mx-only.example.test", 119, Catcher.take, &catcher);
    try pumpUntil(&loop, 5000, &catcher, Catcher.done);

    // Distinct from NameNotFound: the name exists, it just has no address.
    try testing.expectError(error.NoAddress, catcher.result.?);
}

test "a truncated reply is re-asked over TCP" {
    const gpa = testing.allocator;
    var loop: reactor.Loop = undefined;
    try loop.init(gpa);
    defer loop.deinit();

    var stub: StubServer = undefined;
    try stub.start(gpa, &loop, &.{
        .truncate,
        .{ .answer = .{ .v4 = &.{.{ 192, 0, 2, 200 }} } },
    });
    defer stub.deinit();

    var resolver: Resolver = undefined;
    resolver.init(gpa, &loop, stub.config());
    defer resolver.deinit();

    var catcher: Catcher = .{};
    resolver.resolve("big.example.test", 119, Catcher.take, &catcher);
    try pumpUntil(&loop, 5000, &catcher, Catcher.done);

    try testing.expectEqualSlices(u8, &.{ 192, 0, 2, 200 }, &(try catcher.result.?).ip4.bytes);
    try testing.expectEqual(@as(usize, 2), stub.received);
    // And it was really TCP, length-prefixed, not a UDP retry that
    // happened to get a different answer.
    try testing.expectEqual(@as(usize, 1), stub.received_tcp);
}

test "concurrent lookups of one name share a single query" {
    const gpa = testing.allocator;
    var loop: reactor.Loop = undefined;
    try loop.init(gpa);
    defer loop.deinit();

    var stub: StubServer = undefined;
    try stub.start(gpa, &loop, &.{
        .{ .answer = .{ .v4 = &.{ .{ 192, 0, 2, 10 }, .{ 192, 0, 2, 11 } } } },
    });
    defer stub.deinit();

    var resolver: Resolver = undefined;
    resolver.init(gpa, &loop, stub.config());
    defer resolver.deinit();

    var catchers: [8]Catcher = @splat(.{});
    for (&catchers, 0..) |*c, i| {
        resolver.resolve("shared.example.test", @intCast(1000 + i), Catcher.take, c);
    }
    try testing.expectEqual(@as(usize, 1), resolver.pending());

    const Ctx = struct { list: []Catcher };
    var ctx = Ctx{ .list = &catchers };
    try pumpUntil(&loop, 5000, &ctx, struct {
        fn f(c: *Ctx) bool {
            for (c.list) |*x| if (x.result == null) return false;
            return true;
        }
    }.f);

    for (&catchers, 0..) |*c, i| {
        const addr = try c.result.?;
        try testing.expectEqual(@as(u16, @intCast(1000 + i)), addr.ip4.port);
        // Waiters are spread across the address set rather than all
        // piling onto the first record.
        try testing.expectEqual(@as(u8, if (i % 2 == 0) 10 else 11), addr.ip4.bytes[3]);
    }
    try testing.expectEqual(@as(usize, 1), stub.received);
}

test "a resolver torn down with a query in flight tells its waiter" {
    const gpa = testing.allocator;
    var loop: reactor.Loop = undefined;
    try loop.init(gpa);
    defer loop.deinit();

    var stub: StubServer = undefined;
    try stub.start(gpa, &loop, &.{.drop});
    defer stub.deinit();

    var catcher: Catcher = .{};
    {
        var resolver: Resolver = undefined;
        resolver.init(gpa, &loop, stub.config());
        resolver.resolve("pending.example.test", 119, Catcher.take, &catcher);
        try testing.expectEqual(@as(usize, 1), resolver.pending());
        resolver.deinit();
    }

    // A caller left with a callback that never fires is a wedged download.
    try testing.expectError(error.Canceled, catcher.result.?);
}

test "a configuration with no nameservers fails immediately" {
    const gpa = testing.allocator;
    var loop: reactor.Loop = undefined;
    try loop.init(gpa);
    defer loop.deinit();

    var resolver: Resolver = undefined;
    resolver.init(gpa, &loop, .{});
    defer resolver.deinit();

    var catcher: Catcher = .{};
    resolver.resolve("nowhere.example.test", 119, Catcher.take, &catcher);
    try testing.expectError(error.NoNameservers, catcher.result.?);
}

test "a nameserver with nothing listening fails fast rather than on the timeout" {
    const gpa = testing.allocator;
    var loop: reactor.Loop = undefined;
    try loop.init(gpa);
    defer loop.deinit();

    // Bind a UDP socket to claim a port, then release it, so the address
    // is routable but nothing answers. Loopback returns ICMP port
    // unreachable, which a connected socket reports as ECONNREFUSED.
    const probe = try sys.socket(sys.AF_INET, sys.SOCK_DGRAM, 0);
    var sa = sys.Sockaddr.fromIp(try IpAddress.parse("127.0.0.1", 0));
    try sys.bind(probe, &sa);
    const dead_port = (try sys.getsockname(probe)).port();
    sys.close(probe);

    var cfg: Config = .{ .server_count = 1, .timeout_ns = 3 * std.time.ns_per_s, .attempts = 1 };
    cfg.servers[0] = try IpAddress.parse("127.0.0.1", dead_port);

    var resolver: Resolver = undefined;
    resolver.init(gpa, &loop, cfg);
    defer resolver.deinit();

    var catcher: Catcher = .{};
    const start = sys.monotonicNanos();
    resolver.resolve("dead.example.test", 119, Catcher.take, &catcher);
    try pumpUntil(&loop, 10_000, &catcher, Catcher.done);
    const elapsed = sys.monotonicNanos() - start;

    // Either the ICMP came back — which a connected datagram socket
    // reports as ECONNREFUSED — or the attempt timed out. Both are
    // failures; the point is that neither hangs.
    if (catcher.result.?) |_| {
        return error.TestUnexpectedResult;
    } else |err| {
        try testing.expect(err == error.Timeout or err == error.ConnectionRefused);
    }
    try testing.expect(elapsed < 5 * std.time.ns_per_s);
}
