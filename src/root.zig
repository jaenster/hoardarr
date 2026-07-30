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
    pub const log = @import("core/log.zig");
    pub const logring = @import("core/logring.zig");
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

pub const domain = struct {
    pub const download = struct {
        pub const state = @import("domain/download/state.zig");
        pub const segment = @import("domain/download/segment.zig");
        pub const file = @import("domain/download/file.zig");
        pub const events = @import("domain/download/events.zig");
        pub const job = @import("domain/download/job.zig");
        pub const ports = @import("domain/download/ports.zig");
    };

    pub const event = @import("domain/event.zig");
    pub const tx = @import("domain/tx.zig");
    pub const server = @import("domain/server.zig");
    pub const auth = @import("domain/auth.zig");
    pub const command = @import("domain/command.zig");
    pub const schedule = @import("domain/schedule.zig");
    pub const notify = @import("domain/notify.zig");
    pub const verify = @import("domain/verify.zig");
    pub const repair = @import("domain/repair.zig");
    pub const extract = @import("domain/extract.zig");
    pub const deliver = @import("domain/deliver.zig");
};

pub const nntp = struct {
    pub const protocol = @import("nntp/protocol.zig");
};

pub const store = struct {
    pub const sqlite_c = @import("store/sqlite_c.zig");
};

pub const app = struct {
    pub const notify = struct {
        pub const render = @import("app/notify/render.zig");
        pub const transport = @import("app/notify/transport.zig");
        pub const discord = @import("app/notify/discord.zig");
        pub const slack = @import("app/notify/slack.zig");
        pub const service = @import("app/notify/service.zig");
    };
};

pub const net = struct {
    pub const socket = @import("net/socket.zig");
};

pub const posix = struct {
    pub const sys = @import("posix/sys.zig");
    pub const reactor = @import("posix/reactor.zig");
};

test {
    _ = core.crc32;
    _ = core.toml;
    _ = core.config;
    _ = core.log;
    _ = core.logring;
    _ = codec.yenc;
    _ = codec.xml;
    _ = codec.nzb;
    _ = codec.par2.gf16;
    _ = codec.par2.matrix;
    _ = codec.par2.par2;
    _ = codec.par2.rs;
    _ = codec.par2.verifier;
    _ = domain.download.state;
    _ = domain.download.segment;
    _ = domain.download.file;
    _ = domain.download.events;
    _ = domain.download.job;
    _ = domain.download.ports;
    _ = domain.event;
    _ = domain.tx;
    _ = domain.server;
    _ = domain.auth;
    _ = domain.command;
    _ = domain.schedule;
    _ = domain.notify;
    _ = domain.verify;
    _ = domain.repair;
    _ = domain.extract;
    _ = domain.deliver;
    _ = nntp.protocol;
    _ = store.sqlite_c;
    _ = app.notify.render;
    _ = app.notify.transport;
    _ = app.notify.discord;
    _ = app.notify.slack;
    _ = app.notify.service;
    _ = net.socket;
    _ = posix.sys;
    _ = posix.reactor;
}
