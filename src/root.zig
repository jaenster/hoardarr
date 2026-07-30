//! hoardarr — root module. Every subsystem is re-exported here so
//! `zig build test` walks the whole tree, and so `@import("hoardarr")`
//! from main.zig / bench sees one namespace.
//!
//! Zig 0.16 only discovers tests in files that are semantically
//! reachable from the root, and `refAllDeclsRecursive` is gone, so the
//! `test` block at the bottom names every module explicitly. Adding a
//! new file means adding a line there — deliberate, so a file can never
//! silently drop out of the test run.

pub const core = struct {
    pub const crc32 = @import("core/crc32.zig");
    pub const toml = @import("core/toml.zig");
    pub const config = @import("core/config.zig");
};

pub const codec = struct {
    pub const yenc = @import("codec/yenc.zig");
    pub const xml = @import("codec/xml.zig");
    pub const nzb = @import("codec/nzb.zig");
    pub const par2 = struct {
        pub const gf16 = @import("codec/par2/gf16.zig");
        pub const matrix = @import("codec/par2/matrix.zig");
        pub const par2 = @import("codec/par2/par2.zig");
        pub const rs = @import("codec/par2/rs.zig");
        pub const verifier = @import("codec/par2/verifier.zig");
    };
};

pub const nntp = struct {
    pub const protocol = @import("nntp/protocol.zig");
};

pub const store = struct {
    pub const sqlite_c = @import("store/sqlite_c.zig");
};

pub const posix = struct {
    pub const sys = @import("posix/sys.zig");
    pub const reactor = @import("posix/reactor.zig");
};

test {
    _ = core.crc32;
    _ = core.toml;
    _ = core.config;
    _ = codec.yenc;
    _ = codec.xml;
    _ = codec.nzb;
    _ = codec.par2.gf16;
    _ = codec.par2.matrix;
    _ = codec.par2.par2;
    _ = codec.par2.rs;
    _ = codec.par2.verifier;
    _ = nntp.protocol;
    _ = store.sqlite_c;
    _ = posix.sys;
    _ = posix.reactor;
}
