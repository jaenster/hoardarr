//! TOML reader, scoped to hoardarr's configuration file.
//!
//! This is deliberately not a complete TOML 1.0 implementation: it covers
//! the constructs a config file needs and *rejects* everything else with a
//! concrete error rather than skipping it, so a typo in `config.toml`
//! never turns into a silently-ignored setting.
//!
//! Supported: comments, `[table]` and `[a.b]` headers, bare and quoted
//! keys, dotted keys, basic strings (with the full escape set including
//! `\uXXXX` / `\UXXXXXXXX`), literal strings, integers (decimal plus
//! `0x`/`0o`/`0b`, `_` separators), floats (fraction, exponent, `inf`,
//! `nan`), booleans, arrays (heterogeneous, multi-line, trailing comma),
//! and inline tables.
//!
//! Rejected on purpose: array-of-tables (`[[x]]`), multi-line strings
//! (`"""` / `'''`), and every date/time type. Nothing in hoardarr's schema
//! is one of those, and a config that contains one is far more likely to
//! be a mistake than an intent.
//!
//! The whole document lands in one arena. Every string is copied out of
//! the input, so the caller may free the source right after parsing;
//! `Parsed.deinit` releases the entire tree in one shot.

const std = @import("std");
const Allocator = std.mem.Allocator;

pub const Value = union(enum) {
    string: []const u8,
    integer: i64,
    float: f64,
    boolean: bool,
    array: []const Value,
    table: *Table,
};

pub const Table = struct {
    map: Map = .empty,

    /// Set when a `[header]` line, an inline table, or a dotted key made
    /// this table explicitly. A second `[header]` naming the same path is
    /// a redefinition and rejected; a table that only exists because it
    /// was a prefix of `[a.b]` stays implicit and may still be declared.
    defined: bool = false,

    /// Inline tables are sealed once closed: no later header and no
    /// dotted key may add keys to them.
    sealed: bool = false,

    pub const Map = std.StringArrayHashMapUnmanaged(Value);

    pub fn get(tbl: *const Table, key: []const u8) ?Value {
        return tbl.map.get(key);
    }

    pub fn getTable(tbl: *const Table, key: []const u8) ?*const Table {
        return switch (tbl.get(key) orelse return null) {
            .table => |sub| sub,
            else => null,
        };
    }

    pub fn getString(tbl: *const Table, key: []const u8) ?[]const u8 {
        return switch (tbl.get(key) orelse return null) {
            .string => |s| s,
            else => null,
        };
    }

    pub fn getInteger(tbl: *const Table, key: []const u8) ?i64 {
        return switch (tbl.get(key) orelse return null) {
            .integer => |n| n,
            else => null,
        };
    }

    pub fn getFloat(tbl: *const Table, key: []const u8) ?f64 {
        return switch (tbl.get(key) orelse return null) {
            .float => |f| f,
            .integer => |n| @floatFromInt(n),
            else => null,
        };
    }

    pub fn getBool(tbl: *const Table, key: []const u8) ?bool {
        return switch (tbl.get(key) orelse return null) {
            .boolean => |b| b,
            else => null,
        };
    }

    pub fn getArray(tbl: *const Table, key: []const u8) ?[]const Value {
        return switch (tbl.get(key) orelse return null) {
            .array => |a| a,
            else => null,
        };
    }

    pub fn count(tbl: *const Table) usize {
        return tbl.map.count();
    }
};

/// A parsed document plus the arena that owns it.
pub const Parsed = struct {
    arena: *std.heap.ArenaAllocator,
    root: *Table,

    pub fn deinit(p: Parsed) void {
        const gpa = p.arena.child_allocator;
        p.arena.deinit();
        gpa.destroy(p.arena);
    }
};

/// Where parsing stopped, 1-based, for operator-facing error messages.
pub const Diagnostic = struct {
    line: usize = 0,
    column: usize = 0,
};

pub const Error = error{
    UnexpectedCharacter,
    UnexpectedEndOfInput,
    ExpectedKey,
    ExpectedEquals,
    ExpectedNewline,
    ExpectedCloseBracket,
    ExpectedCommaOrCloseBracket,
    ExpectedCommaOrCloseBrace,
    DuplicateKey,
    DuplicateTable,
    KeyIsNotATable,
    ExtendsSealedTable,
    UnterminatedString,
    UnterminatedArray,
    UnterminatedInlineTable,
    InvalidEscape,
    InvalidUnicodeEscape,
    InvalidControlCharacter,
    NewlineInInlineTable,
    InvalidNumber,
    NumberOutOfRange,
    UnsupportedDatetime,
    UnsupportedMultilineString,
    UnsupportedArrayOfTables,
} || Allocator.Error;

/// Parse `src` into a table tree. On failure, `diag` (when given) carries
/// the 1-based position where the parser gave up.
pub fn parse(gpa: Allocator, src: []const u8, diag: ?*Diagnostic) Error!Parsed {
    const arena = try gpa.create(std.heap.ArenaAllocator);
    errdefer gpa.destroy(arena);
    arena.* = .init(gpa);
    errdefer arena.deinit();

    const alloc = arena.allocator();
    const root = try alloc.create(Table);
    root.* = .{ .defined = true };

    var p: Parser = .{ .src = src, .arena = alloc, .root = root, .cur = root };
    p.run() catch |err| {
        if (diag) |d| d.* = .{ .line = p.line, .column = p.i - p.line_start + 1 };
        return err;
    };
    return .{ .arena = arena, .root = root };
}

const Parser = struct {
    src: []const u8,
    i: usize = 0,
    line: usize = 1,
    line_start: usize = 0,
    arena: Allocator,
    root: *Table,
    /// Table that unprefixed keys land in — the most recent `[header]`.
    cur: *Table,

    fn run(p: *Parser) Error!void {
        // A leading UTF-8 BOM is not part of the grammar but editors on
        // other platforms add one; dropping it beats failing on byte 0.
        if (std.mem.startsWith(u8, p.src, "\xEF\xBB\xBF")) p.i = 3;

        while (true) {
            p.skipBlank();
            const c = p.peek() orelse return;
            if (c == '[') try p.tableHeader() else try p.keyval(p.cur);
            try p.endOfLine();
        }
    }

    // -------------------------------------------------------- scanning

    fn peek(p: *const Parser) ?u8 {
        return if (p.i < p.src.len) p.src[p.i] else null;
    }

    fn peekAt(p: *const Parser, n: usize) ?u8 {
        return if (p.i + n < p.src.len) p.src[p.i + n] else null;
    }

    /// Consume one byte, keeping the line counter honest.
    fn bump(p: *Parser) void {
        if (p.src[p.i] == '\n') {
            p.line += 1;
            p.line_start = p.i + 1;
        }
        p.i += 1;
    }

    fn skipSpaces(p: *Parser) void {
        while (p.i < p.src.len and (p.src[p.i] == ' ' or p.src[p.i] == '\t')) p.i += 1;
    }

    fn skipCommentBody(p: *Parser) void {
        while (p.i < p.src.len and p.src[p.i] != '\n') p.i += 1;
    }

    /// Skip everything that carries no meaning between statements, and
    /// inside arrays: spaces, newlines and comments.
    fn skipBlank(p: *Parser) void {
        while (p.i < p.src.len) {
            switch (p.src[p.i]) {
                ' ', '\t', '\r', '\n' => p.bump(),
                '#' => p.skipCommentBody(),
                else => return,
            }
        }
    }

    /// A statement must be the last thing on its line.
    fn endOfLine(p: *Parser) Error!void {
        p.skipSpaces();
        if (p.peek() == '#') p.skipCommentBody();
        const c = p.peek() orelse return;
        if (c == '\r' and p.peekAt(1) == '\n') p.i += 1;
        if (p.peek() != '\n') return error.ExpectedNewline;
        p.bump();
    }

    // ----------------------------------------------------------- items

    fn tableHeader(p: *Parser) Error!void {
        p.bump(); // '['
        if (p.peek() == '[') return error.UnsupportedArrayOfTables;

        const path = try p.keyPath();
        p.skipSpaces();
        if (p.peek() != ']') return error.ExpectedCloseBracket;
        p.bump();

        var tbl = p.root;
        for (path, 0..) |seg, idx| {
            const last = idx + 1 == path.len;
            const gop = try tbl.map.getOrPut(p.arena, seg);
            if (!gop.found_existing) {
                const child = try p.arena.create(Table);
                child.* = .{};
                gop.value_ptr.* = .{ .table = child };
            }
            switch (gop.value_ptr.*) {
                .table => |child| {
                    if (child.sealed) return error.ExtendsSealedTable;
                    if (last) {
                        if (child.defined) return error.DuplicateTable;
                        child.defined = true;
                    }
                    tbl = child;
                },
                else => return error.KeyIsNotATable,
            }
        }
        p.cur = tbl;
    }

    fn keyval(p: *Parser, dest: *Table) Error!void {
        const path = try p.keyPath();
        p.skipSpaces();
        if (p.peek() != '=') return error.ExpectedEquals;
        p.bump();
        p.skipSpaces();
        const v = try p.value();

        var tbl = dest;
        for (path[0 .. path.len - 1]) |seg| {
            const gop = try tbl.map.getOrPut(p.arena, seg);
            if (!gop.found_existing) {
                const child = try p.arena.create(Table);
                // Tables opened by a dotted key count as defined: a later
                // `[header]` for the same path is a redefinition.
                child.* = .{ .defined = true };
                gop.value_ptr.* = .{ .table = child };
            }
            switch (gop.value_ptr.*) {
                .table => |child| {
                    if (child.sealed) return error.ExtendsSealedTable;
                    tbl = child;
                },
                else => return error.KeyIsNotATable,
            }
        }

        const gop = try tbl.map.getOrPut(p.arena, path[path.len - 1]);
        if (gop.found_existing) return error.DuplicateKey;
        gop.value_ptr.* = v;
    }

    /// A dotted key: one or more parts, `.`-separated, spaces allowed
    /// around the dots. Every part is copied into the arena.
    fn keyPath(p: *Parser) Error![]const []const u8 {
        var parts: std.ArrayList([]const u8) = .empty;
        while (true) {
            p.skipSpaces();
            try parts.append(p.arena, try p.keyPart());
            p.skipSpaces();
            if (p.peek() != '.') break;
            p.bump();
        }
        return parts.toOwnedSlice(p.arena);
    }

    fn keyPart(p: *Parser) Error![]const u8 {
        switch (p.peek() orelse return error.ExpectedKey) {
            '"' => {
                if (std.mem.startsWith(u8, p.src[p.i..], "\"\"\"")) return error.UnsupportedMultilineString;
                return p.basicString();
            },
            '\'' => {
                if (std.mem.startsWith(u8, p.src[p.i..], "'''")) return error.UnsupportedMultilineString;
                return p.literalString();
            },
            else => {
                const start = p.i;
                while (p.i < p.src.len and isBareKeyChar(p.src[p.i])) p.i += 1;
                if (p.i == start) return error.ExpectedKey;
                return p.arena.dupe(u8, p.src[start..p.i]);
            },
        }
    }

    fn value(p: *Parser) Error!Value {
        switch (p.peek() orelse return error.UnexpectedEndOfInput) {
            '"' => {
                if (std.mem.startsWith(u8, p.src[p.i..], "\"\"\"")) return error.UnsupportedMultilineString;
                return .{ .string = try p.basicString() };
            },
            '\'' => {
                if (std.mem.startsWith(u8, p.src[p.i..], "'''")) return error.UnsupportedMultilineString;
                return .{ .string = try p.literalString() };
            },
            '[' => return p.array(),
            '{' => return p.inlineTable(),
            '\n', '\r' => return error.UnexpectedEndOfInput,
            else => return p.scalar(),
        }
    }

    fn array(p: *Parser) Error!Value {
        p.bump(); // '['
        var items: std.ArrayList(Value) = .empty;
        while (true) {
            p.skipBlank();
            const c = p.peek() orelse return error.UnterminatedArray;
            if (c == ']') {
                p.bump();
                break;
            }
            try items.append(p.arena, try p.value());
            p.skipBlank();
            switch (p.peek() orelse return error.UnterminatedArray) {
                ',' => p.bump(),
                ']' => {
                    p.bump();
                    break;
                },
                else => return error.ExpectedCommaOrCloseBracket,
            }
        }
        return .{ .array = try items.toOwnedSlice(p.arena) };
    }

    fn inlineTable(p: *Parser) Error!Value {
        p.bump(); // '{'
        const tbl = try p.arena.create(Table);
        tbl.* = .{ .defined = true };

        p.skipSpaces();
        if (p.peek() == '}') {
            p.bump();
        } else while (true) {
            p.skipSpaces();
            switch (p.peek() orelse return error.UnterminatedInlineTable) {
                '\n', '\r' => return error.NewlineInInlineTable,
                else => {},
            }
            try p.keyval(tbl);
            p.skipSpaces();
            switch (p.peek() orelse return error.UnterminatedInlineTable) {
                ',' => p.bump(),
                '}' => {
                    p.bump();
                    break;
                },
                '\n', '\r' => return error.NewlineInInlineTable,
                else => return error.ExpectedCommaOrCloseBrace,
            }
        }
        // Sealed only now, so the keyvals above could write into it.
        tbl.sealed = true;
        return .{ .table = tbl };
    }

    // --------------------------------------------------------- strings

    fn basicString(p: *Parser) Error![]const u8 {
        p.bump(); // opening quote
        var out: std.ArrayList(u8) = .empty;
        while (true) {
            const c = p.peek() orelse return error.UnterminatedString;
            switch (c) {
                '"' => {
                    p.bump();
                    break;
                },
                '\n' => return error.UnterminatedString,
                '\\' => {
                    p.bump();
                    const e = p.peek() orelse return error.UnterminatedString;
                    p.bump();
                    switch (e) {
                        'b' => try out.append(p.arena, 0x08),
                        't' => try out.append(p.arena, '\t'),
                        'n' => try out.append(p.arena, '\n'),
                        'f' => try out.append(p.arena, 0x0C),
                        'r' => try out.append(p.arena, '\r'),
                        '"' => try out.append(p.arena, '"'),
                        '\\' => try out.append(p.arena, '\\'),
                        'u' => try p.unicodeEscape(&out, 4),
                        'U' => try p.unicodeEscape(&out, 8),
                        else => return error.InvalidEscape,
                    }
                },
                // Tab is the one control character a basic string may
                // carry raw; everything else must be escaped.
                0x00...0x08, 0x0B...0x1F, 0x7F => return error.InvalidControlCharacter,
                else => {
                    try out.append(p.arena, c);
                    p.bump();
                },
            }
        }
        return out.toOwnedSlice(p.arena);
    }

    fn unicodeEscape(p: *Parser, out: *std.ArrayList(u8), digits: usize) Error!void {
        if (p.i + digits > p.src.len) return error.InvalidUnicodeEscape;
        const hex = p.src[p.i .. p.i + digits];
        for (hex) |c| if (!std.ascii.isHex(c)) return error.InvalidUnicodeEscape;
        p.i += digits;

        const cp = std.fmt.parseUnsigned(u21, hex, 16) catch return error.InvalidUnicodeEscape;
        var buf: [4]u8 = undefined;
        const n = std.unicode.utf8Encode(cp, &buf) catch return error.InvalidUnicodeEscape;
        try out.appendSlice(p.arena, buf[0..n]);
    }

    fn literalString(p: *Parser) Error![]const u8 {
        p.bump(); // opening quote
        const start = p.i;
        while (true) {
            const c = p.peek() orelse return error.UnterminatedString;
            switch (c) {
                '\'' => {
                    const s = p.src[start..p.i];
                    p.bump();
                    return p.arena.dupe(u8, s);
                },
                '\n' => return error.UnterminatedString,
                0x00...0x08, 0x0B...0x1F, 0x7F => return error.InvalidControlCharacter,
                else => p.bump(),
            }
        }
    }

    // --------------------------------------------------------- scalars

    /// Booleans, integers and floats all share one token scan; the shape
    /// of the token decides which it is. Date/time tokens are recognised
    /// here purely so they can be rejected with a precise error.
    fn scalar(p: *Parser) Error!Value {
        const start = p.i;
        scan: while (p.i < p.src.len) {
            switch (p.src[p.i]) {
                ' ', '\t', '\r', '\n', ',', ']', '}', '#' => break :scan,
                else => p.i += 1,
            }
        }
        const tok = p.src[start..p.i];
        if (tok.len == 0) return error.UnexpectedCharacter;

        if (std.mem.eql(u8, tok, "true")) return .{ .boolean = true };
        if (std.mem.eql(u8, tok, "false")) return .{ .boolean = false };
        if (looksLikeDatetime(tok)) return error.UnsupportedDatetime;
        if (!isNumberStart(tok[0])) return error.UnexpectedCharacter;
        if (isFloatToken(tok)) return .{ .float = try parseFloatToken(tok) };
        return .{ .integer = try parseIntToken(tok) };
    }
};

fn isBareKeyChar(c: u8) bool {
    return std.ascii.isAlphanumeric(c) or c == '_' or c == '-';
}

fn isNumberStart(c: u8) bool {
    return std.ascii.isDigit(c) or c == '+' or c == '-' or c == 'i' or c == 'n';
}

/// TOML dates start `YYYY-MM-DD`; times contain a colon. Both are cheap to
/// spot and neither is representable in hoardarr's schema.
fn looksLikeDatetime(tok: []const u8) bool {
    if (std.mem.indexOfScalar(u8, tok, ':') != null) return true;
    if (tok.len < 10) return false;
    if (!std.ascii.isDigit(tok[0])) return false;
    for (tok[0..4]) |c| if (!std.ascii.isDigit(c)) return false;
    return tok[4] == '-' and std.ascii.isDigit(tok[5]) and std.ascii.isDigit(tok[6]) and tok[7] == '-';
}

fn isFloatToken(tok: []const u8) bool {
    var s = tok;
    if (s[0] == '+' or s[0] == '-') s = s[1..];
    if (std.mem.eql(u8, s, "inf") or std.mem.eql(u8, s, "nan")) return true;
    // Radix-prefixed integers may contain 'e'/'E' as hex digits.
    if (s.len >= 2 and s[0] == '0' and (s[1] == 'x' or s[1] == 'o' or s[1] == 'b')) return false;
    return std.mem.indexOfAny(u8, s, ".eE") != null;
}

/// Copy `s` without its `_` separators, rejecting any underscore that is
/// not wedged between two digits.
fn stripUnderscores(buf: []u8, s: []const u8, allow_hex: bool) Error![]const u8 {
    if (s.len > buf.len) return error.NumberOutOfRange;
    var n: usize = 0;
    for (s, 0..) |c, idx| {
        if (c == '_') {
            if (idx == 0 or idx + 1 == s.len) return error.InvalidNumber;
            const before = s[idx - 1];
            const after = s[idx + 1];
            const ok = if (allow_hex)
                std.ascii.isHex(before) and std.ascii.isHex(after)
            else
                std.ascii.isDigit(before) and std.ascii.isDigit(after);
            if (!ok) return error.InvalidNumber;
            continue;
        }
        buf[n] = c;
        n += 1;
    }
    return buf[0..n];
}

fn parseIntToken(tok: []const u8) Error!i64 {
    var s = tok;
    var negative = false;
    var signed = false;
    if (s[0] == '+' or s[0] == '-') {
        negative = s[0] == '-';
        signed = true;
        s = s[1..];
    }
    if (s.len == 0) return error.InvalidNumber;

    var base: u8 = 10;
    if (s.len >= 2 and s[0] == '0' and (s[1] == 'x' or s[1] == 'o' or s[1] == 'b')) {
        // TOML forbids a sign on radix-prefixed integers.
        if (signed) return error.InvalidNumber;
        base = switch (s[1]) {
            'x' => 16,
            'o' => 8,
            else => 2,
        };
        s = s[2..];
        if (s.len == 0) return error.InvalidNumber;
    } else if (s.len > 1 and s[0] == '0') {
        return error.InvalidNumber; // leading zeros are not decimal syntax
    }

    var buf: [72]u8 = undefined;
    const clean = try stripUnderscores(&buf, s, base == 16);
    if (clean.len == 0) return error.InvalidNumber;

    // Parsed wide so that -9223372036854775808 — whose magnitude does not
    // fit i64 — still round-trips.
    const magnitude = std.fmt.parseUnsigned(i128, clean, base) catch |err| return switch (err) {
        error.Overflow => error.NumberOutOfRange,
        error.InvalidCharacter => error.InvalidNumber,
    };
    const v: i128 = if (negative) -magnitude else magnitude;
    if (v < std.math.minInt(i64) or v > std.math.maxInt(i64)) return error.NumberOutOfRange;
    return @intCast(v);
}

fn parseFloatToken(tok: []const u8) Error!f64 {
    var buf: [340]u8 = undefined;
    const clean = try stripUnderscores(&buf, tok, false);

    var body = clean;
    if (body.len != 0 and (body[0] == '+' or body[0] == '-')) body = body[1..];
    if (!std.mem.eql(u8, body, "inf") and !std.mem.eql(u8, body, "nan")) try validateFloatBody(body);

    return std.fmt.parseFloat(f64, clean) catch error.InvalidNumber;
}

/// `digits [ "." digits ] [ ("e"|"E") [sign] digits ]`, no leading zeros.
/// std's parseFloat is far more permissive than TOML, so the shape is
/// checked here rather than trusted to it.
fn validateFloatBody(s: []const u8) Error!void {
    var i: usize = 0;
    const int_start = i;
    while (i < s.len and std.ascii.isDigit(s[i])) i += 1;
    if (i == int_start) return error.InvalidNumber;
    if (i - int_start > 1 and s[int_start] == '0') return error.InvalidNumber;

    if (i < s.len and s[i] == '.') {
        i += 1;
        const frac_start = i;
        while (i < s.len and std.ascii.isDigit(s[i])) i += 1;
        if (i == frac_start) return error.InvalidNumber;
    }
    if (i < s.len and (s[i] == 'e' or s[i] == 'E')) {
        i += 1;
        if (i < s.len and (s[i] == '+' or s[i] == '-')) i += 1;
        const exp_start = i;
        while (i < s.len and std.ascii.isDigit(s[i])) i += 1;
        if (i == exp_start) return error.InvalidNumber;
    }
    if (i != s.len) return error.InvalidNumber;
}

// ---------------------------------------------------------------- tests

const t = std.testing;

/// Parse and hand back the tree; the caller deinits.
fn parseOk(src: []const u8) !Parsed {
    var diag: Diagnostic = .{};
    return parse(t.allocator, src, &diag) catch |err| {
        std.debug.print("unexpected parse failure {t} at {d}:{d}\n", .{ err, diag.line, diag.column });
        return err;
    };
}

fn expectFails(comptime want: Error, src: []const u8) !void {
    var diag: Diagnostic = .{};
    const res = parse(t.allocator, src, &diag);
    if (res) |p| {
        var owned = p;
        owned.deinit();
        std.debug.print("expected {t}, but input parsed fine:\n{s}\n", .{ want, src });
        return error.TestExpectedError;
    } else |err| {
        try t.expectEqual(want, err);
        try t.expect(diag.line >= 1);
    }
}

test "empty and comment-only documents" {
    for ([_][]const u8{ "", "\n", "  \n\t\n", "# just a comment", "# a\n# b\n" }) |src| {
        var p = try parseOk(src);
        defer p.deinit();
        try t.expectEqual(@as(usize, 0), p.root.count());
    }
}

test "scalars at the document root" {
    var p = try parseOk(
        \\name = "hoardarr"
        \\empty = ""
        \\port = 8085
        \\ratio = 0.05
        \\enabled = true
        \\disabled = false
    );
    defer p.deinit();

    try t.expectEqualStrings("hoardarr", p.root.getString("name").?);
    try t.expectEqualStrings("", p.root.getString("empty").?);
    try t.expectEqual(@as(i64, 8085), p.root.getInteger("port").?);
    try t.expectEqual(@as(f64, 0.05), p.root.getFloat("ratio").?);
    try t.expectEqual(true, p.root.getBool("enabled").?);
    try t.expectEqual(false, p.root.getBool("disabled").?);
}

test "tables, nested table headers and re-entry" {
    var p = try parseOk(
        \\[server]
        \\listen = ":8085"
        \\
        \\[storage]
        \\backend = "sqlite"
        \\
        \\  [storage.sqlite]
        \\  path = "/data/hoardarr.db"
        \\
        \\[paths]
        \\complete_dir = "/data/complete"
    );
    defer p.deinit();

    try t.expectEqual(@as(usize, 3), p.root.count());
    try t.expectEqualStrings(":8085", p.root.getTable("server").?.getString("listen").?);
    const storage = p.root.getTable("storage").?;
    try t.expectEqualStrings("sqlite", storage.getString("backend").?);
    try t.expectEqualStrings("/data/hoardarr.db", storage.getTable("sqlite").?.getString("path").?);
    try t.expectEqualStrings("/data/complete", p.root.getTable("paths").?.getString("complete_dir").?);
}

test "implicit parent table is declarable afterwards" {
    var p = try parseOk(
        \\[a.b]
        \\x = 1
        \\[a]
        \\y = 2
    );
    defer p.deinit();
    const a = p.root.getTable("a").?;
    try t.expectEqual(@as(i64, 1), a.getTable("b").?.getInteger("x").?);
    try t.expectEqual(@as(i64, 2), a.getInteger("y").?);
}

test "dotted keys" {
    var p = try parseOk(
        \\a.b.c = 1
        \\a.d = 2
        \\ spaced . key = "v"
    );
    defer p.deinit();
    const a = p.root.getTable("a").?;
    try t.expectEqual(@as(i64, 1), a.getTable("b").?.getInteger("c").?);
    try t.expectEqual(@as(i64, 2), a.getInteger("d").?);
    try t.expectEqualStrings("v", p.root.getTable("spaced").?.getString("key").?);
}

test "quoted keys" {
    var p = try parseOk(
        \\"with space" = 1
        \\'literal.key' = 2
        \\"" = 3
        \\[ "quoted table" ]
        \\k = 4
    );
    defer p.deinit();
    try t.expectEqual(@as(i64, 1), p.root.getInteger("with space").?);
    try t.expectEqual(@as(i64, 2), p.root.getInteger("literal.key").?);
    try t.expectEqual(@as(i64, 3), p.root.getInteger("").?);
    try t.expectEqual(@as(i64, 4), p.root.getTable("quoted table").?.getInteger("k").?);
}

test "bare key character set" {
    var p = try parseOk(
        \\api_key = 1
        \\dash-key = 2
        \\Mixed_CASE-9 = 3
    );
    defer p.deinit();
    try t.expectEqual(@as(i64, 1), p.root.getInteger("api_key").?);
    try t.expectEqual(@as(i64, 2), p.root.getInteger("dash-key").?);
    try t.expectEqual(@as(i64, 3), p.root.getInteger("Mixed_CASE-9").?);
}

test "basic string escapes" {
    var p = try parseOk(
        \\s = "tab:\t nl:\n cr:\r quote:\" back:\\ bs:\b ff:\f"
        \\u = "\u00e9\u20ac"
        \\U = "\U0001F600"
        \\win = "C:\\data\\incomplete"
    );
    defer p.deinit();
    try t.expectEqualStrings("tab:\t nl:\n cr:\r quote:\" back:\\ bs:\x08 ff:\x0C", p.root.getString("s").?);
    try t.expectEqualStrings("é€", p.root.getString("u").?);
    try t.expectEqualStrings("😀", p.root.getString("U").?);
    try t.expectEqualStrings("C:\\data\\incomplete", p.root.getString("win").?);
}

test "literal strings keep backslashes" {
    var p = try parseOk(
        \\p = 'C:\data\no\escapes'
        \\q = 'he said "hi"'
    );
    defer p.deinit();
    try t.expectEqualStrings("C:\\data\\no\\escapes", p.root.getString("p").?);
    try t.expectEqualStrings("he said \"hi\"", p.root.getString("q").?);
}

test "integers in every accepted form" {
    var p = try parseOk(
        \\zero = 0
        \\pos = +99
        \\neg = -17
        \\big = 1_000_000
        \\hex = 0xDEAD_beef
        \\oct = 0o755
        \\bin = 0b1010_0001
        \\max = 9223372036854775807
        \\min = -9223372036854775808
    );
    defer p.deinit();
    try t.expectEqual(@as(i64, 0), p.root.getInteger("zero").?);
    try t.expectEqual(@as(i64, 99), p.root.getInteger("pos").?);
    try t.expectEqual(@as(i64, -17), p.root.getInteger("neg").?);
    try t.expectEqual(@as(i64, 1_000_000), p.root.getInteger("big").?);
    try t.expectEqual(@as(i64, 0xDEADBEEF), p.root.getInteger("hex").?);
    try t.expectEqual(@as(i64, 0o755), p.root.getInteger("oct").?);
    try t.expectEqual(@as(i64, 0b10100001), p.root.getInteger("bin").?);
    try t.expectEqual(std.math.maxInt(i64), p.root.getInteger("max").?);
    try t.expectEqual(std.math.minInt(i64), p.root.getInteger("min").?);
}

test "floats in every accepted form" {
    var p = try parseOk(
        \\a = 0.0
        \\b = 3.1415
        \\c = -0.01
        \\d = 5e+22
        \\e = 1e06
        \\f = -2E-2
        \\g = 6.626e-34
        \\h = 9_224_617.445_991
        \\pinf = inf
        \\ninf = -inf
        \\notnum = nan
    );
    defer p.deinit();
    try t.expectEqual(@as(f64, 0.0), p.root.getFloat("a").?);
    try t.expectEqual(@as(f64, 3.1415), p.root.getFloat("b").?);
    try t.expectEqual(@as(f64, -0.01), p.root.getFloat("c").?);
    try t.expectEqual(@as(f64, 5e22), p.root.getFloat("d").?);
    try t.expectEqual(@as(f64, 1e6), p.root.getFloat("e").?);
    try t.expectEqual(@as(f64, -0.02), p.root.getFloat("f").?);
    try t.expectEqual(@as(f64, 6.626e-34), p.root.getFloat("g").?);
    try t.expectEqual(@as(f64, 9224617.445991), p.root.getFloat("h").?);
    try t.expect(std.math.isPositiveInf(p.root.getFloat("pinf").?));
    try t.expect(std.math.isNegativeInf(p.root.getFloat("ninf").?));
    try t.expect(std.math.isNan(p.root.getFloat("notnum").?));
}

test "arrays" {
    var p = try parseOk(
        \\empty = []
        \\ints = [1, 2, 3]
        \\strs = ["a", 'b']
        \\mixed = [1, "two", 3.0, true]
        \\nested = [[1, 2], [3]]
        \\multi = [
        \\  1, # first
        \\  2,
        \\  # a lone comment
        \\  3,
        \\]
    );
    defer p.deinit();
    try t.expectEqual(@as(usize, 0), p.root.getArray("empty").?.len);

    const ints = p.root.getArray("ints").?;
    try t.expectEqual(@as(usize, 3), ints.len);
    try t.expectEqual(@as(i64, 2), ints[1].integer);

    const strs = p.root.getArray("strs").?;
    try t.expectEqualStrings("a", strs[0].string);
    try t.expectEqualStrings("b", strs[1].string);

    const mixed = p.root.getArray("mixed").?;
    try t.expectEqual(@as(usize, 4), mixed.len);
    try t.expectEqual(true, mixed[3].boolean);

    const nested = p.root.getArray("nested").?;
    try t.expectEqual(@as(usize, 2), nested[0].array.len);
    try t.expectEqual(@as(i64, 3), nested[1].array[0].integer);

    try t.expectEqual(@as(usize, 3), p.root.getArray("multi").?.len);
}

test "inline tables" {
    var p = try parseOk(
        \\empty = {}
        \\point = { x = 1, y = 2 }
        \\deep = { a = { b = "c" }, list = [1, 2] }
        \\dotted = { a.b = 1 }
    );
    defer p.deinit();
    try t.expectEqual(@as(usize, 0), p.root.getTable("empty").?.count());
    const point = p.root.getTable("point").?;
    try t.expectEqual(@as(i64, 1), point.getInteger("x").?);
    try t.expectEqual(@as(i64, 2), point.getInteger("y").?);
    const deep = p.root.getTable("deep").?;
    try t.expectEqualStrings("c", deep.getTable("a").?.getString("b").?);
    try t.expectEqual(@as(usize, 2), deep.getArray("list").?.len);
    try t.expectEqual(@as(i64, 1), p.root.getTable("dotted").?.getTable("a").?.getInteger("b").?);
}

test "comments, trailing whitespace and CRLF line endings" {
    var p = try parseOk("# lead\r\n[server]\t # after header\r\nlisten = \":1\"  # after value\r\n\r\n");
    defer p.deinit();
    try t.expectEqualStrings(":1", p.root.getTable("server").?.getString("listen").?);
}

test "utf-8 byte order mark is tolerated" {
    var p = try parseOk("\xEF\xBB\xBFa = 1\n");
    defer p.deinit();
    try t.expectEqual(@as(i64, 1), p.root.getInteger("a").?);
}

test "hoardarr's own config.toml shape" {
    var p = try parseOk(
        \\# hoardarr configuration.
        \\#
        \\# The api_key was randomly generated.
        \\
        \\[server]
        \\  listen = ":8085"
        \\  data_dir = "/srv/hoardarr/data"
        \\  log_level = "info"
        \\  url_base = "/hoardarr"
        \\  max_concurrent_jobs = 1
        \\  fail_hopeless_ratio = 0.05
        \\  defer_recovery_vols = false
        \\  delete_samples = true
        \\  collapse_single_folder = true
        \\
        \\[auth]
        \\  api_key = "564f540f59f92fd5dfd7714d6182a1b7"
        \\
        \\[storage]
        \\  backend = "sqlite"
        \\  [storage.sqlite]
        \\    path = "/srv/hoardarr/data/hoardarr.db"
        \\
        \\[paths]
        \\  incomplete_dir = "/srv/hoardarr/data/incomplete"
        \\  complete_dir = "/srv/hoardarr/data/complete"
        \\
        \\[bandwidth]
        \\  global_bytes_per_sec = 0
    );
    defer p.deinit();

    const server = p.root.getTable("server").?;
    try t.expectEqualStrings(":8085", server.getString("listen").?);
    try t.expectEqualStrings("/hoardarr", server.getString("url_base").?);
    try t.expectEqual(@as(i64, 1), server.getInteger("max_concurrent_jobs").?);
    try t.expectEqual(@as(f64, 0.05), server.getFloat("fail_hopeless_ratio").?);
    try t.expectEqual(false, server.getBool("defer_recovery_vols").?);
    try t.expectEqualStrings("564f540f59f92fd5dfd7714d6182a1b7", p.root.getTable("auth").?.getString("api_key").?);
    try t.expectEqual(@as(i64, 0), p.root.getTable("bandwidth").?.getInteger("global_bytes_per_sec").?);
}

test "diagnostic points at the offending line" {
    var diag: Diagnostic = .{};
    try t.expectError(error.ExpectedEquals, parse(t.allocator, "a = 1\nb = 2\noops\n", &diag));
    try t.expectEqual(@as(usize, 3), diag.line);
}

test "rejects structurally broken documents" {
    try expectFails(error.ExpectedEquals, "key\n");
    try expectFails(error.ExpectedEquals, "key value\n");
    try expectFails(error.ExpectedKey, "= 1\n");
    try expectFails(error.UnexpectedEndOfInput, "key =\n");
    try expectFails(error.ExpectedNewline, "a = 1 b = 2\n");
    try expectFails(error.ExpectedNewline, "a = 1 2\n");
    try expectFails(error.ExpectedKey, "[]\n");
    try expectFails(error.ExpectedCloseBracket, "[server\nlisten = 1\n");
    try expectFails(error.ExpectedNewline, "[server] junk\n");
    try expectFails(error.ExpectedKey, "[a.]\nx = 1\n");
    try expectFails(error.UnexpectedCharacter, "a = ?\n");
}

test "rejects duplicate definitions" {
    try expectFails(error.DuplicateKey, "a = 1\na = 2\n");
    try expectFails(error.DuplicateKey, "a = 1\na = 1\n");
    try expectFails(error.DuplicateTable, "[a]\n[a]\n");
    try expectFails(error.DuplicateTable, "[a.b]\n[a.b]\n");
    try expectFails(error.KeyIsNotATable, "a = 1\n[a]\n");
    try expectFails(error.KeyIsNotATable, "a = 1\na.b = 2\n");
    try expectFails(error.KeyIsNotATable, "[a]\nb = 1\n[a.b]\n");
    try expectFails(error.DuplicateTable, "a.b = 1\n[a]\n");
    try expectFails(error.ExtendsSealedTable, "x = { a = 1 }\nx.b = 2\n");
    try expectFails(error.ExtendsSealedTable, "x = { a = 1 }\n[x.y]\n");
}

test "rejects malformed strings" {
    try expectFails(error.UnterminatedString, "a = \"open\n");
    try expectFails(error.UnterminatedString, "a = \"open");
    try expectFails(error.UnterminatedString, "a = 'open\n");
    try expectFails(error.InvalidEscape, "a = \"\\q\"\n");
    try expectFails(error.InvalidUnicodeEscape, "a = \"\\u00\"\n");
    try expectFails(error.InvalidUnicodeEscape, "a = \"\\uD800\"\n");
    try expectFails(error.InvalidControlCharacter, "a = \"bell\x07\"\n");
    try expectFails(error.UnsupportedMultilineString, "a = \"\"\"\nmulti\n\"\"\"\n");
    try expectFails(error.UnsupportedMultilineString, "a = '''\nmulti\n'''\n");
}

test "rejects malformed numbers" {
    try expectFails(error.InvalidNumber, "a = 01\n");
    try expectFails(error.InvalidNumber, "a = 1__0\n");
    try expectFails(error.UnexpectedCharacter, "a = _1\n");
    try expectFails(error.InvalidNumber, "a = 1_\n");
    try expectFails(error.InvalidNumber, "a = 0x\n");
    try expectFails(error.InvalidNumber, "a = -0x1\n");
    try expectFails(error.InvalidNumber, "a = 0b12\n");
    try expectFails(error.InvalidNumber, "a = 1.\n");
    try expectFails(error.UnexpectedCharacter, "a = .5\n");
    try expectFails(error.InvalidNumber, "a = 1e\n");
    try expectFails(error.InvalidNumber, "a = 1.2.3\n");
    try expectFails(error.UnexpectedCharacter, "a = truex\n");
    try expectFails(error.NumberOutOfRange, "a = 9223372036854775808\n");
    try expectFails(error.NumberOutOfRange, "a = -9223372036854775809\n");
}

test "rejects malformed arrays and inline tables" {
    try expectFails(error.UnterminatedArray, "a = [1, 2\n");
    try expectFails(error.ExpectedCommaOrCloseBracket, "a = [1 2]\n");
    try expectFails(error.NewlineInInlineTable, "a = { x = 1\n");
    try expectFails(error.NewlineInInlineTable, "a = {\nx = 1 }\n");
    try expectFails(error.NewlineInInlineTable, "a = { x = 1,\ny = 2 }\n");
    try expectFails(error.ExpectedKey, "a = { x = 1, }\n");
    try expectFails(error.DuplicateKey, "a = { x = 1, x = 2 }\n");
}

test "rejects the constructs hoardarr's schema does not use" {
    try expectFails(error.UnsupportedArrayOfTables, "[[servers]]\nhost = \"a\"\n");
    try expectFails(error.UnsupportedDatetime, "a = 1979-05-27T07:32:00Z\n");
    try expectFails(error.UnsupportedDatetime, "a = 1979-05-27\n");
    try expectFails(error.UnsupportedDatetime, "a = 07:32:00\n");
    try expectFails(error.UnsupportedDatetime, "a = 1979-05-27 07:32:00\n");
}
