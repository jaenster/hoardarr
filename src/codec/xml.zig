//! A pull scanner for the subset of XML that NZB documents actually
//! use: elements, attributes, character data, CDATA sections, the five
//! predefined entities, and numeric character references. Comments,
//! processing instructions and the document type declaration are
//! recognised and skipped.
//!
//! Deliberately *not* supported, because no NZB needs it and every one
//! of them is an attack surface:
//!
//!   * User-declared entities. The internal DTD subset is skipped
//!     wholesale, so `<!ENTITY lol "lolol...">` never enters a
//!     substitution table and the billion-laughs family of expansion
//!     bombs cannot get off the ground. A reference to an undeclared
//!     entity is emitted as literal text (see below).
//!   * External references of any kind — no SYSTEM/PUBLIC id is ever
//!     dereferenced, so there is no XXE vector.
//!   * Namespace resolution. NZBs carry `xmlns=".../nzb"` but nothing
//!     ever needs the URI; callers match on the local name via
//!     `localName`.
//!
//! Leniency is asymmetric on purpose. NZBs in the wild routinely carry
//! a bare `&` in a subject line, so a `&` that does not introduce a
//! recognised entity or a well-formed character reference is passed
//! through as data instead of failing the document. Everything
//! *structural* is strict: an unterminated tag, comment, CDATA section
//! or attribute value, a mismatched end tag, or content after the root
//! element is an error, never a guess.
//!
//! Input is a single in-memory slice. That is what an NZB is (it
//! arrives as an upload or a file), and it lets names and most text
//! nodes be returned as slices of the input with no copying at all.

const std = @import("std");

/// Bounds applied to untrusted input. Everything the scanner allocates
/// or recurses on is capped here, so a hostile document can cost at
/// most `max_attrs * max_value_len` bytes of scratch and `max_depth`
/// stack entries regardless of its size.
pub const Limits = struct {
    /// Maximum element nesting. Also what stops an unclosed-element
    /// bomb (`<a><a><a>...`) from growing the name stack without end.
    max_depth: u16 = 100,
    /// Maximum length of an element or attribute name.
    max_name_len: u32 = 4096,
    /// Maximum attributes on one element.
    max_attrs: u16 = 128,
    /// Maximum decoded length of one text node or one attribute value.
    max_value_len: usize = 1 << 20,
};

pub const Error = error{
    DepthExceeded,
    DuplicateAttribute,
    InvalidCharacterReference,
    MalformedName,
    MalformedTag,
    MismatchedEndTag,
    MultipleRootElements,
    NameTooLong,
    NoRootElement,
    TextOutsideRoot,
    TooManyAttributes,
    UnclosedElement,
    UnquotedAttributeValue,
    UnterminatedAttribute,
    UnterminatedCdata,
    UnterminatedComment,
    UnterminatedDoctype,
    UnterminatedPi,
    UnterminatedTag,
    ValueTooLong,
} || std.mem.Allocator.Error;

pub const Attr = struct {
    name: []const u8,
    value: []const u8,
};

pub const Element = struct {
    /// Name exactly as written, prefix included.
    name: []const u8,
    attrs: []const Attr,
    /// True for `<foo/>`. A matching `.close` event is emitted next
    /// regardless, so consumers never special-case this.
    self_closing: bool,

    pub fn local(self: Element) []const u8 {
        return localName(self.name);
    }

    /// Attribute lookup by local name, so `date` matches both `date`
    /// and `ns:date`. Linear — attribute counts are single digits.
    pub fn attr(self: Element, name: []const u8) ?[]const u8 {
        for (self.attrs) |a| {
            if (std.mem.eql(u8, localName(a.name), name)) return a.value;
        }
        return null;
    }
};

pub const Event = union(enum) {
    open: Element,
    /// Name of the element being closed, as written in the source (for
    /// a self-closing element, the start tag's spelling).
    close: []const u8,
    /// Character data with entities and character references resolved.
    /// Adjacent runs of text and CDATA are coalesced into one event, so
    /// a `<segment>` body arrives whole.
    text: []const u8,
    eof,
};

/// Strips an optional `prefix:` from an XML name.
pub fn localName(name: []const u8) []const u8 {
    if (std.mem.indexOfScalar(u8, name, ':')) |i| return name[i + 1 ..];
    return name;
}

/// An attribute value recorded before `buf` has stopped moving. `off`
/// indexes either the source (when the value needed no decoding and can
/// be aliased) or the scratch buffer.
const RawAttr = struct {
    name: []const u8,
    off: usize,
    len: usize,
    direct: bool,
};

pub const Scanner = struct {
    gpa: std.mem.Allocator,
    src: []const u8,
    pos: usize = 0,
    limits: Limits = .{},

    /// Scratch for decoded text and attribute values. Cleared at the
    /// top of every `next`, which is why returned slices only live
    /// until the following call.
    buf: std.ArrayList(u8) = .empty,
    raw_attrs: std.ArrayList(RawAttr) = .empty,
    attrs: std.ArrayList(Attr) = .empty,
    /// Open element names, as slices of `src`. Doubles as the depth
    /// counter and as the end-tag matcher.
    stack: std.ArrayList([]const u8) = .empty,

    /// Set by a self-closing tag so the synthetic close comes out on
    /// the next call.
    pending_close: ?[]const u8 = null,
    seen_root: bool = false,

    pub fn init(gpa: std.mem.Allocator, src: []const u8, limits: Limits) Scanner {
        // A UTF-8 BOM is legal and carries no information once we are
        // already committed to UTF-8 input.
        const body = if (std.mem.startsWith(u8, src, "\xEF\xBB\xBF")) src[3..] else src;
        return .{ .gpa = gpa, .src = body, .limits = limits };
    }

    pub fn deinit(self: *Scanner) void {
        self.buf.deinit(self.gpa);
        self.raw_attrs.deinit(self.gpa);
        self.attrs.deinit(self.gpa);
        self.stack.deinit(self.gpa);
        self.* = undefined;
    }

    /// Returns the next event. Slices inside the event borrow from the
    /// scanner and are invalidated by the following call; copy anything
    /// that must outlive it.
    pub fn next(self: *Scanner) Error!Event {
        if (self.pending_close) |name| {
            self.pending_close = null;
            return .{ .close = name };
        }
        self.buf.clearRetainingCapacity();

        while (true) {
            if (self.pos >= self.src.len) {
                if (self.stack.items.len != 0) return error.UnclosedElement;
                if (!self.seen_root) return error.NoRootElement;
                return .eof;
            }
            if (self.src[self.pos] != '<') {
                if (self.stack.items.len == 0) {
                    // Only whitespace may sit beside the root element.
                    try self.skipMiscText();
                    continue;
                }
                return try self.scanText();
            }
            // Markup. Every branch below either returns an event or
            // advances past something skippable.
            const rest = self.src[self.pos..];
            if (std.mem.startsWith(u8, rest, "</")) return try self.scanEndTag();
            if (std.mem.startsWith(u8, rest, "<?")) {
                try self.skipUntil("?>", error.UnterminatedPi);
                continue;
            }
            if (std.mem.startsWith(u8, rest, "<!--")) {
                self.pos += 4;
                try self.skipUntil("-->", error.UnterminatedComment);
                continue;
            }
            if (std.mem.startsWith(u8, rest, cdata_open)) {
                if (self.stack.items.len == 0) return error.TextOutsideRoot;
                return try self.scanText();
            }
            if (std.mem.startsWith(u8, rest, "<!")) {
                try self.skipDecl();
                continue;
            }
            return try self.scanStartTag();
        }
    }

    /// Consumes events until the element opened by the most recent
    /// `open` is closed, discarding everything inside it.
    pub fn skipElement(self: *Scanner) Error!void {
        var depth: usize = 1;
        while (depth > 0) {
            switch (try self.next()) {
                .open => depth += 1,
                .close => depth -= 1,
                .text => {},
                .eof => return error.UnclosedElement,
            }
        }
    }

    const cdata_open = "<![CDATA[";

    fn skipMiscText(self: *Scanner) Error!void {
        while (self.pos < self.src.len and self.src[self.pos] != '<') {
            if (!isSpace(self.src[self.pos])) return error.TextOutsideRoot;
            self.pos += 1;
        }
    }

    fn skipUntil(self: *Scanner, needle: []const u8, err: Error) Error!void {
        const at = std.mem.indexOfPos(u8, self.src, self.pos, needle) orelse return err;
        self.pos = at + needle.len;
    }

    /// Skips `<!DOCTYPE ...>` and any other `<!...>` declaration,
    /// including an internal subset. Quoted strings are honoured so a
    /// `>` inside a SYSTEM id does not end the declaration early. The
    /// subset's contents are discarded, never interpreted.
    fn skipDecl(self: *Scanner) Error!void {
        self.pos += 2;
        var in_subset = false;
        while (self.pos < self.src.len) {
            const c = self.src[self.pos];
            switch (c) {
                '"', '\'' => {
                    const end = std.mem.indexOfScalarPos(u8, self.src, self.pos + 1, c) orelse
                        return error.UnterminatedDoctype;
                    self.pos = end + 1;
                },
                '[' => {
                    in_subset = true;
                    self.pos += 1;
                },
                ']' => {
                    in_subset = false;
                    self.pos += 1;
                },
                '>' => {
                    self.pos += 1;
                    if (!in_subset) return;
                },
                else => self.pos += 1,
            }
        }
        return error.UnterminatedDoctype;
    }

    fn scanStartTag(self: *Scanner) Error!Event {
        self.pos += 1;
        const name = try self.scanName();
        self.raw_attrs.clearRetainingCapacity();

        var self_closing = false;
        while (true) {
            self.skipSpace();
            if (self.pos >= self.src.len) return error.UnterminatedTag;
            switch (self.src[self.pos]) {
                '>' => {
                    self.pos += 1;
                    break;
                },
                '/' => {
                    self.pos += 1;
                    if (self.pos >= self.src.len or self.src[self.pos] != '>') return error.MalformedTag;
                    self.pos += 1;
                    self_closing = true;
                    break;
                },
                else => try self.scanAttr(),
            }
        }

        // Values are only stable now that `buf` has stopped growing.
        self.attrs.clearRetainingCapacity();
        try self.attrs.ensureTotalCapacity(self.gpa, self.raw_attrs.items.len);
        for (self.raw_attrs.items) |ra| {
            const src = if (ra.direct) self.src else self.buf.items;
            self.attrs.appendAssumeCapacity(.{ .name = ra.name, .value = src[ra.off .. ra.off + ra.len] });
        }

        if (self.stack.items.len == 0) {
            if (self.seen_root) return error.MultipleRootElements;
            self.seen_root = true;
        }
        if (self_closing) {
            self.pending_close = name;
        } else {
            if (self.stack.items.len >= self.limits.max_depth) return error.DepthExceeded;
            try self.stack.append(self.gpa, name);
        }
        return .{ .open = .{ .name = name, .attrs = self.attrs.items, .self_closing = self_closing } };
    }

    fn scanAttr(self: *Scanner) Error!void {
        if (self.raw_attrs.items.len >= self.limits.max_attrs) return error.TooManyAttributes;
        const name = try self.scanName();
        for (self.raw_attrs.items) |ra| {
            if (std.mem.eql(u8, ra.name, name)) return error.DuplicateAttribute;
        }
        self.skipSpace();
        if (self.pos >= self.src.len or self.src[self.pos] != '=') return error.MalformedTag;
        self.pos += 1;
        self.skipSpace();
        if (self.pos >= self.src.len) return error.UnterminatedTag;
        const quote = self.src[self.pos];
        if (quote != '"' and quote != '\'') return error.UnquotedAttributeValue;
        self.pos += 1;

        const start = self.pos;
        const end = std.mem.indexOfScalarPos(u8, self.src, start, quote) orelse
            return error.UnterminatedAttribute;
        const raw = self.src[start..end];

        var rec: RawAttr = undefined;
        if (std.mem.indexOfScalar(u8, raw, '&') == null) {
            // No references: alias the source directly.
            if (raw.len > self.limits.max_value_len) return error.ValueTooLong;
            rec = .{ .name = name, .off = start, .len = raw.len, .direct = true };
        } else {
            const off = self.buf.items.len;
            const saved = self.pos;
            self.pos = start;
            // `.attr`: a `<` inside a value is data here. The quote is
            // the only terminator, and `end` already found it.
            while (self.pos < end) try self.appendRun(end, .attr);
            self.pos = saved;
            const len = self.buf.items.len - off;
            if (len > self.limits.max_value_len) return error.ValueTooLong;
            rec = .{ .name = name, .off = off, .len = len, .direct = false };
        }
        try self.raw_attrs.append(self.gpa, rec);
        self.pos = end + 1;
    }

    fn scanEndTag(self: *Scanner) Error!Event {
        self.pos += 2;
        const name = try self.scanName();
        self.skipSpace();
        if (self.pos >= self.src.len or self.src[self.pos] != '>') return error.MalformedTag;
        self.pos += 1;

        const open = self.stack.pop() orelse return error.MismatchedEndTag;
        if (!std.mem.eql(u8, open, name)) return error.MismatchedEndTag;
        return .{ .close = open };
    }

    /// Character data up to the next tag, comment or PI. CDATA
    /// sections are folded in so `a<![CDATA[b]]>c` is one `"abc"`.
    fn scanText(self: *Scanner) Error!Event {
        const start = self.pos;

        // Fast path: one plain run with nothing to decode, returned as
        // a slice of the input. Covers essentially every real segment
        // body and meta value.
        var i = start;
        var plain = true;
        while (i < self.src.len and self.src[i] != '<') : (i += 1) {
            if (self.src[i] == '&') {
                plain = false;
                break;
            }
        }
        if (plain and !std.mem.startsWith(u8, self.src[i..], cdata_open)) {
            if (i - start > self.limits.max_value_len) return error.ValueTooLong;
            self.pos = i;
            return .{ .text = self.src[start..i] };
        }

        while (self.pos < self.src.len) {
            if (self.src[self.pos] == '<') {
                if (!std.mem.startsWith(u8, self.src[self.pos..], cdata_open)) break;
                self.pos += cdata_open.len;
                const end = std.mem.indexOfPos(u8, self.src, self.pos, "]]>") orelse
                    return error.UnterminatedCdata;
                try self.push(self.src[self.pos..end]);
                self.pos = end + 3;
                continue;
            }
            try self.appendRun(self.src.len, .content);
        }
        return .{ .text = self.buf.items };
    }

    /// Where a run is being decoded. In element content a `<` ends the
    /// run; inside an attribute value it is just data, and treating it
    /// as a terminator would stall the caller's loop.
    const RunKind = enum { content, attr };

    /// Appends one stretch of `src[pos..limit]` to `buf`: either a
    /// reference, or the plain bytes up to the next stop byte. Always
    /// advances `pos` by at least one byte.
    fn appendRun(self: *Scanner, limit: usize, kind: RunKind) Error!void {
        if (self.src[self.pos] == '&') return self.appendReference(limit);
        var end = self.pos;
        while (end < limit and self.src[end] != '&') : (end += 1) {
            if (kind == .content and self.src[end] == '<') break;
        }
        try self.push(self.src[self.pos..end]);
        self.pos = end;
    }

    /// Resolves one `&...;`. The five predefined entities and numeric
    /// character references are substituted. Anything else — including
    /// a bare `&` and named entities we do not know — is emitted
    /// literally, because real NZB subjects contain unescaped `&` and
    /// rejecting the whole document over it is worse than passing the
    /// byte through.
    fn appendReference(self: *Scanner, limit: usize) Error!void {
        const rest = self.src[self.pos..limit];
        // Longest thing we ever accept is "&#x10FFFF;"; a `;` further
        // out than that is not part of a reference.
        const window = rest[0..@min(rest.len, 16)];
        const semi = std.mem.indexOfScalar(u8, window, ';') orelse {
            try self.push("&");
            self.pos += 1;
            return;
        };
        const body = window[1..semi];

        if (body.len != 0 and body[0] == '#') {
            // A malformed *numeric* reference is unambiguous garbage,
            // so unlike a named one it is an error rather than data.
            const cp = try parseCharRef(body[1..]);
            var utf8: [4]u8 = undefined;
            const n = std.unicode.utf8Encode(cp, &utf8) catch return error.InvalidCharacterReference;
            try self.push(utf8[0..n]);
            self.pos += semi + 1;
            return;
        }
        const named: ?[]const u8 = if (std.mem.eql(u8, body, "amp"))
            "&"
        else if (std.mem.eql(u8, body, "lt"))
            "<"
        else if (std.mem.eql(u8, body, "gt"))
            ">"
        else if (std.mem.eql(u8, body, "quot"))
            "\""
        else if (std.mem.eql(u8, body, "apos"))
            "'"
        else
            null;
        if (named) |s| {
            try self.push(s);
            self.pos += semi + 1;
            return;
        }
        try self.push("&");
        self.pos += 1;
    }

    fn push(self: *Scanner, bytes: []const u8) Error!void {
        if (self.buf.items.len + bytes.len > self.limits.max_value_len) return error.ValueTooLong;
        try self.buf.appendSlice(self.gpa, bytes);
    }

    fn scanName(self: *Scanner) Error!([]const u8) {
        const start = self.pos;
        if (self.pos >= self.src.len or !isNameStart(self.src[self.pos])) return error.MalformedName;
        self.pos += 1;
        while (self.pos < self.src.len and isNameChar(self.src[self.pos])) self.pos += 1;
        const name = self.src[start..self.pos];
        if (name.len > self.limits.max_name_len) return error.NameTooLong;
        return name;
    }

    fn skipSpace(self: *Scanner) void {
        while (self.pos < self.src.len and isSpace(self.src[self.pos])) self.pos += 1;
    }
};

fn isSpace(c: u8) bool {
    return c == ' ' or c == '\t' or c == '\r' or c == '\n';
}

fn isNameStart(c: u8) bool {
    return std.ascii.isAlphabetic(c) or c == '_' or c == ':' or c >= 0x80;
}

fn isNameChar(c: u8) bool {
    return isNameStart(c) or std.ascii.isDigit(c) or c == '-' or c == '.';
}

/// `body` is what follows `&#`. Rejects codepoints XML forbids (NUL and
/// most other C0 controls, the surrogate range, U+FFFE/U+FFFF, anything
/// above U+10FFFF) and digit strings long enough to overflow.
fn parseCharRef(body: []const u8) Error!u21 {
    const hex = body.len != 0 and (body[0] == 'x' or body[0] == 'X');
    const digits = if (hex) body[1..] else body;
    if (digits.len == 0 or digits.len > 8) return error.InvalidCharacterReference;
    const v = std.fmt.parseUnsigned(u32, digits, if (hex) 16 else 10) catch
        return error.InvalidCharacterReference;
    if (v > 0x10FFFF) return error.InvalidCharacterReference;
    const cp: u21 = @intCast(v);
    if (!isXmlChar(cp)) return error.InvalidCharacterReference;
    return cp;
}

fn isXmlChar(cp: u21) bool {
    return cp == 0x9 or cp == 0xA or cp == 0xD or
        (cp >= 0x20 and cp <= 0xD7FF) or
        (cp >= 0xE000 and cp <= 0xFFFD) or
        (cp >= 0x10000 and cp <= 0x10FFFF);
}

/// Returns the `encoding="..."` label from the XML declaration, or null
/// when there is no declaration or it carries no encoding. Only the
/// declaration is inspected — this is a byte scan, not a parse, because
/// it has to run before we know how to decode the document.
pub fn declaredEncoding(src_in: []const u8) ?[]const u8 {
    const src = if (std.mem.startsWith(u8, src_in, "\xEF\xBB\xBF")) src_in[3..] else src_in;
    if (!std.mem.startsWith(u8, src, "<?xml")) return null;
    const end = std.mem.indexOf(u8, src, "?>") orelse return null;
    const decl = src[5..end];
    const at = std.mem.indexOf(u8, decl, "encoding") orelse return null;
    var i = at + "encoding".len;
    while (i < decl.len and isSpace(decl[i])) i += 1;
    if (i >= decl.len or decl[i] != '=') return null;
    i += 1;
    while (i < decl.len and isSpace(decl[i])) i += 1;
    if (i >= decl.len) return null;
    const quote = decl[i];
    if (quote != '"' and quote != '\'') return null;
    i += 1;
    const close = std.mem.indexOfScalarPos(u8, decl, i, quote) orelse return null;
    return decl[i..close];
}

/// Transcodes ISO-8859-1 to UTF-8. Every input byte maps to the
/// codepoint of the same value, so the output is at most twice as long.
pub fn latin1ToUtf8(gpa: std.mem.Allocator, src: []const u8) std.mem.Allocator.Error![]u8 {
    var high: usize = 0;
    for (src) |c| high += @intFromBool(c >= 0x80);
    const out = try gpa.alloc(u8, src.len + high);
    var i: usize = 0;
    for (src) |c| {
        if (c < 0x80) {
            out[i] = c;
            i += 1;
        } else {
            out[i] = 0xC0 | (c >> 6);
            out[i + 1] = 0x80 | (c & 0x3F);
            i += 2;
        }
    }
    return out;
}

// ---------------------------------------------------------------- tests

const t = std.testing;

/// Collects every event as a flat, printable trace so a test can assert
/// on the whole shape of a document in one string compare.
fn trace(gpa: std.mem.Allocator, src: []const u8, limits: Limits) Error![]u8 {
    var sc = Scanner.init(gpa, src, limits);
    defer sc.deinit();
    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(gpa);
    while (true) {
        switch (try sc.next()) {
            .open => |e| {
                try out.appendSlice(gpa, "open ");
                try out.appendSlice(gpa, e.name);
                for (e.attrs) |a| {
                    try out.appendSlice(gpa, " ");
                    try out.appendSlice(gpa, a.name);
                    try out.appendSlice(gpa, "=");
                    try out.appendSlice(gpa, a.value);
                }
                try out.appendSlice(gpa, "\n");
            },
            .close => |n| {
                try out.appendSlice(gpa, "close ");
                try out.appendSlice(gpa, n);
                try out.appendSlice(gpa, "\n");
            },
            .text => |s| {
                try out.appendSlice(gpa, "text[");
                try out.appendSlice(gpa, s);
                try out.appendSlice(gpa, "]\n");
            },
            .eof => break,
        }
    }
    return out.toOwnedSlice(gpa);
}

fn expectTrace(src: []const u8, want: []const u8) !void {
    const got = try trace(t.allocator, src, .{});
    defer t.allocator.free(got);
    try t.expectEqualStrings(want, got);
}

fn expectFail(src: []const u8, want: anyerror) !void {
    try t.expectError(want, trace(t.allocator, src, .{}));
}

test "elements, attributes and text" {
    try expectTrace("<r a=\"1\" b='two'>hi<c/></r>",
        \\open r a=1 b=two
        \\text[hi]
        \\open c
        \\close c
        \\close r
        \\
    );
}

test "self-closing root still emits a close" {
    try expectTrace("<r/>",
        \\open r
        \\close r
        \\
    );
}

test "predefined entities in text and attributes" {
    try expectTrace("<r s=\"a&amp;b&lt;c&gt;d&quot;e&apos;f\">&lt;msg&amp;id&gt;</r>",
        \\open r s=a&b<c>d"e'f
        \\text[<msg&id>]
        \\close r
        \\
    );
}

test "numeric character references, decimal and hex" {
    try expectTrace("<r a=\"&#65;&#x42;\">&#x20AC;&#233;</r>",
        \\open r a=AB
        \\text[€é]
        \\close r
        \\
    );
}

test "cdata is verbatim and coalesces with surrounding text" {
    try expectTrace("<r>a<![CDATA[<b>&amp;]]>c</r>",
        \\open r
        \\text[a<b>&amp;c]
        \\close r
        \\
    );
    // A `]]` that is not the terminator stays in the payload.
    try expectTrace("<r><![CDATA[x]]y]]></r>",
        \\open r
        \\text[x]]y]
        \\close r
        \\
    );
}

test "comments, processing instructions and doctype are skipped" {
    try expectTrace(
        \\<?xml version="1.0"?>
        \\<!-- a > b -->
        \\<!DOCTYPE r SYSTEM "weird>id">
        \\<r><?pi data?>x<!--c--></r>
    ,
        \\open r
        \\text[x]
        \\close r
        \\
    );
}

test "bare ampersand and unknown entity pass through as data" {
    // Real NZB subjects contain unescaped `&`; failing the document
    // over it would reject NZBs that every other client accepts.
    try expectTrace("<r a=\"A &amp B &nbsp; C\">S &, T &amp; U</r>",
        \\open r a=A &amp B &nbsp; C
        \\text[S &, T & U]
        \\close r
        \\
    );
}

test "whitespace beside the root is fine, other content is not" {
    try expectTrace("  <r/>\n\n",
        \\open r
        \\close r
        \\
    );
    try expectFail("<r/>junk", error.TextOutsideRoot);
    try expectFail("<r/><s/>", error.MultipleRootElements);
    try expectFail("<!-- only a comment -->", error.NoRootElement);
    try expectFail("<r/><![CDATA[x]]>", error.TextOutsideRoot);
}

test "structural errors are rejected, never guessed" {
    try expectFail("<r>", error.UnclosedElement);
    try expectFail("<r></s>", error.MismatchedEndTag);
    try expectFail("</r>", error.MismatchedEndTag);
    try expectFail("<r", error.UnterminatedTag);
    try expectFail("<r a=", error.UnterminatedTag);
    try expectFail("<r a=\"x>", error.UnterminatedAttribute);
    try expectFail("<r a=x>", error.UnquotedAttributeValue);
    try expectFail("<r a>", error.MalformedTag);
    try expectFail("<r a=\"1\" a=\"2\"/>", error.DuplicateAttribute);
    try expectFail("<r/ >", error.MalformedTag);
    try expectFail("<1r/>", error.MalformedName);
    try expectFail("<r><![CDATA[x</r>", error.UnterminatedCdata);
    try expectFail("<r><!-- x </r>", error.UnterminatedComment);
    try expectFail("<?xml <r/>", error.UnterminatedPi);
    try expectFail("<!DOCTYPE r [ <r/>", error.UnterminatedDoctype);
    try expectFail("<!DOCTYPE r SYSTEM \"unclosed>", error.UnterminatedDoctype);
}

test "invalid character references are rejected" {
    try expectFail("<r>&#0;</r>", error.InvalidCharacterReference);
    try expectFail("<r>&#xD800;</r>", error.InvalidCharacterReference);
    try expectFail("<r>&#x110000;</r>", error.InvalidCharacterReference);
    try expectFail("<r>&#xFFFFFFFFFF;</r>", error.InvalidCharacterReference);
    try expectFail("<r>&#;</r>", error.InvalidCharacterReference);
    try expectFail("<r>&#xZZ;</r>", error.InvalidCharacterReference);
    try expectFail("<r a=\"&#8;\"/>", error.InvalidCharacterReference);
}

test "depth is bounded" {
    var deep: std.ArrayList(u8) = .empty;
    defer deep.deinit(t.allocator);
    for (0..1000) |_| try deep.appendSlice(t.allocator, "<a>");
    try t.expectError(error.DepthExceeded, trace(t.allocator, deep.items, .{}));

    // The limit is on nesting, not on how many elements a document has.
    var wide: std.ArrayList(u8) = .empty;
    defer wide.deinit(t.allocator);
    try wide.appendSlice(t.allocator, "<r>");
    for (0..10_000) |_| try wide.appendSlice(t.allocator, "<a/>");
    try wide.appendSlice(t.allocator, "</r>");
    const got = try trace(t.allocator, wide.items, .{});
    defer t.allocator.free(got);
    try t.expect(got.len > 10_000);
}

test "oversized attribute values and text nodes are bounded" {
    var big: std.ArrayList(u8) = .empty;
    defer big.deinit(t.allocator);
    try big.appendSlice(t.allocator, "<r a=\"");
    for (0..5000) |_| try big.appendSlice(t.allocator, "x");
    try big.appendSlice(t.allocator, "\">");
    for (0..5000) |_| try big.appendSlice(t.allocator, "y");
    try big.appendSlice(t.allocator, "</r>");

    try t.expectError(error.ValueTooLong, trace(t.allocator, big.items, .{ .max_value_len = 1000 }));

    // Same for a value that has to go through the decoder.
    var esc: std.ArrayList(u8) = .empty;
    defer esc.deinit(t.allocator);
    try esc.appendSlice(t.allocator, "<r a=\"");
    for (0..500) |_| try esc.appendSlice(t.allocator, "&amp;");
    try esc.appendSlice(t.allocator, "\"/>");
    try t.expectError(error.ValueTooLong, trace(t.allocator, esc.items, .{ .max_value_len = 100 }));
}

test "too many attributes and overlong names are bounded" {
    var many: std.ArrayList(u8) = .empty;
    defer many.deinit(t.allocator);
    try many.appendSlice(t.allocator, "<r");
    var nb: [32]u8 = undefined;
    for (0..200) |i| try many.appendSlice(t.allocator, try std.fmt.bufPrint(&nb, " a{d}=\"1\"", .{i}));
    try many.appendSlice(t.allocator, "/>");
    try t.expectError(error.TooManyAttributes, trace(t.allocator, many.items, .{}));

    var long: std.ArrayList(u8) = .empty;
    defer long.deinit(t.allocator);
    try long.appendSlice(t.allocator, "<");
    for (0..100) |_| try long.appendSlice(t.allocator, "n");
    try long.appendSlice(t.allocator, "/>");
    try t.expectError(error.NameTooLong, trace(t.allocator, long.items, .{ .max_name_len = 10 }));
}

test "entity expansion bombs cannot expand" {
    // The internal subset is skipped, so `lol` is never declared and
    // `&lol;` is literal text. No recursion, no growth.
    const bomb =
        \\<!DOCTYPE r [
        \\<!ENTITY a "aaaaaaaaaa">
        \\<!ENTITY b "&a;&a;&a;&a;&a;&a;&a;&a;&a;&a;">
        \\<!ENTITY c "&b;&b;&b;&b;&b;&b;&b;&b;&b;&b;">
        \\]>
        \\<r>&c;</r>
    ;
    try expectTrace(bomb,
        \\open r
        \\text[&c;]
        \\close r
        \\
    );
}

test "skipElement discards a subtree" {
    var sc = Scanner.init(t.allocator, "<r><skip><a x=\"1\">t</a><b/></skip><keep>k</keep></r>", .{});
    defer sc.deinit();
    _ = try sc.next(); // open r
    _ = try sc.next(); // open skip
    try sc.skipElement();
    const ev = try sc.next();
    try t.expectEqualStrings("keep", ev.open.name);
}

test "localName and attribute lookup ignore prefixes" {
    try t.expectEqualStrings("file", localName("ns:file"));
    try t.expectEqualStrings("file", localName("file"));

    var sc = Scanner.init(t.allocator, "<n:r n:date=\"7\" other=\"x\"/>", .{});
    defer sc.deinit();
    const ev = try sc.next();
    try t.expectEqualStrings("r", ev.open.local());
    try t.expectEqualStrings("7", ev.open.attr("date").?);
    try t.expect(ev.open.attr("missing") == null);
}

test "declaredEncoding reads only the declaration" {
    try t.expectEqualStrings("iso-8859-1", declaredEncoding("<?xml version=\"1.0\" encoding=\"iso-8859-1\"?><r/>").?);
    try t.expectEqualStrings("UTF-8", declaredEncoding("<?xml encoding = 'UTF-8' ?><r/>").?);
    try t.expect(declaredEncoding("<r encoding=\"x\"/>") == null);
    try t.expect(declaredEncoding("<?xml version=\"1.0\"?><r/>") == null);
    try t.expect(declaredEncoding("<?xml encoding=") == null);
    try t.expectEqualStrings("utf-8", declaredEncoding("\xEF\xBB\xBF<?xml encoding=\"utf-8\"?><r/>").?);
}

test "latin1ToUtf8" {
    const got = try latin1ToUtf8(t.allocator, "caf\xE9 \x00\x7F");
    defer t.allocator.free(got);
    try t.expectEqualStrings("café \x00\x7F", got);
}

test "utf-8 bom is consumed" {
    try expectTrace("\xEF\xBB\xBF<r/>",
        \\open r
        \\close r
        \\
    );
}

test "hostile input never crashes or leaks" {
    // Stand-in for the Go fuzz target: mutate a valid document at every
    // offset and truncate it at every length. Any error is acceptable;
    // a crash, an out-of-bounds read or a leak is not.
    const seed = "<?xml version=\"1.0\"?><!DOCTYPE r [<!ENTITY e \"x\">]>" ++
        "<r a=\"1\" b='&amp;'><c>t&#65;<![CDATA[z]]></c><d/><!--k--></r>";
    const pokes = "<>/&;\"'![]-#x\x00\xFF= \n";

    try t.expectError(error.NoRootElement, trace(t.allocator, "", .{}));

    // Every truncation.
    for (0..seed.len) |i| {
        if (trace(t.allocator, seed[0..i], .{})) |ok| {
            t.allocator.free(ok);
        } else |e| switch (e) {
            error.OutOfMemory => return e,
            else => {},
        }
    }
    // Every single-byte substitution with a byte that means something
    // to the scanner.
    var buf: [seed.len]u8 = undefined;
    for (0..seed.len) |i| {
        for (pokes) |p| {
            @memcpy(&buf, seed);
            buf[i] = p;
            if (trace(t.allocator, &buf, .{})) |ok| {
                t.allocator.free(ok);
            } else |e| switch (e) {
                error.OutOfMemory => return e,
                else => {},
            }
        }
    }
}
