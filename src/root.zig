//! hoardarr — root module. Every subsystem is re-exported here so
//! `zig build test` walks the whole tree, and so `@import("hoardarr")`
//! from main.zig / bench sees one namespace.
//!
//! Zig 0.16 only discovers tests in files that are semantically
//! reachable from the root, and `refAllDeclsRecursive` is gone, so the
//! `test` block at the bottom names every module explicitly. Adding a
//! new file means adding a line there — deliberate, so a file can never
//! silently drop out of the test run.

/// Re-exported so `main.zig` reads version metadata through this module
/// rather than importing the generated options file a second time.
pub const build_info = @import("build_info");

pub const bootstrap = @import("bootstrap.zig");

/// The composition root's adapters, split out of `bootstrap.zig` so each
/// bounded context's wiring is one file.
pub const wiring = struct {
    pub const infra = @import("bootstrap/infra.zig");
};

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
    pub const rar = struct {
        pub const cursor = @import("codec/rar/cursor.zig");
        pub const source = @import("codec/rar/source.zig");
        pub const path = @import("codec/rar/path.zig");
        pub const volume = @import("codec/rar/volume.zig");
        pub const rar3 = @import("codec/rar/rar3.zig");
        pub const rar5 = @import("codec/rar/rar5.zig");
        pub const rar = @import("codec/rar/rar.zig");
        pub const extract = @import("codec/rar/extract.zig");
    };
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
    pub const conn = @import("nntp/conn.zig");
    pub const pool = @import("nntp/pool.zig");
};

pub const store = struct {
    pub const sqlite_c = @import("store/sqlite_c.zig");
    pub const sqlite = @import("store/sqlite.zig");
    pub const tx = @import("store/tx.zig");
    pub const migrate = @import("store/migrate.zig");
    pub const outbox = @import("store/outbox.zig");
    pub const repo_download = @import("store/repo_download.zig");
    pub const repo_verify = @import("store/repo_verify.zig");
    pub const repo_repair = @import("store/repo_repair.zig");
    pub const repo_extract = @import("store/repo_extract.zig");
    pub const repo_deliver = @import("store/repo_deliver.zig");
    pub const repo_server = @import("store/repo_server.zig");
    pub const repo_auth = @import("store/repo_auth.zig");
    pub const repo_schedule = @import("store/repo_schedule.zig");
    pub const repo_command = @import("store/repo_command.zig");
    pub const repo_subscription = @import("store/repo_subscription.zig");
    pub const repo_category = @import("store/repo_category.zig");
    pub const repo_settings = @import("store/repo_settings.zig");
    pub const repo_speed_history = @import("store/repo_speed_history.zig");
};

pub const app = struct {
    pub const ports = @import("app/ports.zig");
    pub const naming = @import("app/naming.zig");
    pub const verify = @import("app/verify.zig");
    pub const repair = @import("app/repair.zig");
    pub const extract = @import("app/extract.zig");
    pub const schedule = @import("app/schedule.zig");
    pub const auth = @import("app/auth.zig");
    pub const download = struct {
        pub const ports = @import("app/download/ports.zig");
        pub const orchestrator = @import("app/download/orchestrator.zig");
        pub const add_job = @import("app/download/add_job.zig");
        pub const queue = @import("app/download/queue.zig");
        pub const service = @import("app/download/service.zig");
        pub const bandwidth = @import("app/download/bandwidth.zig");
        pub const byte_accounter = @import("app/download/byte_accounter.zig");
        pub const tiered_fetcher = @import("app/download/tiered_fetcher.zig");
    };
    pub const deliver = struct {
        pub const obfuscation = @import("app/deliver/obfuscation.zig");
        pub const postprocess = @import("app/deliver/postprocess.zig");
        pub const service = @import("app/deliver/service.zig");
    };
    pub const system = struct {
        pub const throughput = @import("app/system/throughput.zig");
        pub const service = @import("app/system/service.zig");
    };
    pub const command = struct {
        pub const service = @import("app/command/service.zig");
        pub const handlers = @import("app/command/handlers.zig");
    };
    pub const notify = struct {
        pub const render = @import("app/notify/render.zig");
        pub const transport = @import("app/notify/transport.zig");
        pub const discord = @import("app/notify/discord.zig");
        pub const slack = @import("app/notify/slack.zig");
        pub const service = @import("app/notify/service.zig");
    };
};

pub const api = struct {
    pub const sse = @import("api/sse.zig");
    pub const metrics = @import("api/metrics.zig");
    pub const rest = struct {
        pub const json = @import("api/rest/json.zig");
        pub const ratelimit = @import("api/rest/ratelimit.zig");
        pub const multipart = @import("api/rest/multipart.zig");
        pub const ports = @import("api/rest/ports.zig");
        pub const dto = @import("api/rest/dto.zig");
        pub const respond = @import("api/rest/respond.zig");
        pub const api = @import("api/rest/api.zig");
        pub const stream = @import("api/rest/stream.zig");
        pub const auth = @import("api/rest/auth.zig");
        pub const queue = @import("api/rest/queue.zig");
        pub const servers = @import("api/rest/servers.zig");
        pub const categories = @import("api/rest/categories.zig");
        pub const system = @import("api/rest/system.zig");
        pub const config = @import("api/rest/config.zig");
        pub const subscriptions = @import("api/rest/subscriptions.zig");
        pub const handlers = @import("api/rest/handlers.zig");
    };
    pub const sab = struct {
        pub const fmt = @import("api/sab/fmt.zig");
        pub const nzo = @import("api/sab/nzo.zig");
        pub const sort_eval = @import("api/sab/sort_eval.zig");
        pub const ports = @import("api/sab/ports.zig");
        pub const dto = @import("api/sab/dto.zig");
        pub const form = @import("api/sab/form.zig");
        pub const handler = @import("api/sab/handler.zig");
    };
};

pub const net = struct {
    pub const socket = @import("net/socket.zig");
    pub const tls = @import("net/tls.zig");
    pub const http = struct {
        pub const request = @import("net/http/request.zig");
        pub const response = @import("net/http/response.zig");
        pub const server = @import("net/http/server.zig");
        pub const client = @import("net/http/client.zig");
    };
};

pub const testserver = struct {
    pub const fixture = @import("testserver/fixture.zig");
    pub const nntp = @import("testserver/nntp.zig");
};

pub const posix = struct {
    pub const sys = @import("posix/sys.zig");
    pub const reactor = @import("posix/reactor.zig");
    pub const signals = @import("posix/signals.zig");
    pub const fiber = @import("posix/fiber.zig");
};

test {
    _ = bootstrap;
    _ = wiring.infra;
    _ = core.crc32;
    _ = core.toml;
    _ = core.config;
    _ = core.log;
    _ = core.logring;
    _ = codec.yenc;
    _ = codec.xml;
    _ = codec.nzb;
    _ = codec.rar.cursor;
    _ = codec.rar.source;
    _ = codec.rar.path;
    _ = codec.rar.volume;
    _ = codec.rar.rar3;
    _ = codec.rar.rar5;
    _ = codec.rar.rar;
    _ = codec.rar.extract;
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
    _ = nntp.conn;
    _ = nntp.pool;
    _ = store.sqlite_c;
    _ = store.sqlite;
    _ = store.tx;
    _ = store.migrate;
    _ = store.outbox;
    _ = store.repo_download;
    _ = store.repo_verify;
    _ = store.repo_repair;
    _ = store.repo_extract;
    _ = store.repo_deliver;
    _ = store.repo_server;
    _ = store.repo_auth;
    _ = store.repo_schedule;
    _ = store.repo_command;
    _ = store.repo_subscription;
    _ = store.repo_category;
    _ = store.repo_settings;
    _ = store.repo_speed_history;
    _ = app.ports;
    _ = app.naming;
    _ = app.verify;
    _ = app.repair;
    _ = app.extract;
    _ = app.schedule;
    _ = app.auth;
    _ = app.download.ports;
    _ = app.download.orchestrator;
    _ = app.download.add_job;
    _ = app.download.queue;
    _ = app.download.service;
    _ = app.download.bandwidth;
    _ = app.download.byte_accounter;
    _ = app.download.tiered_fetcher;
    _ = app.deliver.obfuscation;
    _ = app.deliver.postprocess;
    _ = app.deliver.service;
    _ = app.system.throughput;
    _ = app.system.service;
    _ = app.command.service;
    _ = app.command.handlers;
    _ = app.notify.render;
    _ = app.notify.transport;
    _ = app.notify.discord;
    _ = app.notify.slack;
    _ = app.notify.service;
    _ = api.sse;
    _ = api.metrics;
    _ = api.rest.json;
    _ = api.rest.ratelimit;
    _ = api.rest.multipart;
    _ = api.rest.ports;
    _ = api.rest.dto;
    _ = api.rest.respond;
    _ = api.rest.api;
    _ = api.rest.stream;
    _ = api.rest.auth;
    _ = api.rest.queue;
    _ = api.rest.servers;
    _ = api.rest.categories;
    _ = api.rest.system;
    _ = api.rest.config;
    _ = api.rest.subscriptions;
    _ = api.rest.handlers;
    _ = api.sab.fmt;
    _ = api.sab.nzo;
    _ = api.sab.sort_eval;
    _ = api.sab.ports;
    _ = api.sab.dto;
    _ = api.sab.form;
    _ = api.sab.handler;
    _ = net.socket;
    _ = net.tls;
    _ = net.http.request;
    _ = net.http.response;
    _ = net.http.server;
    _ = net.http.client;
    _ = testserver.fixture;
    _ = testserver.nntp;
    _ = posix.sys;
    _ = posix.reactor;
    _ = posix.signals;
    _ = posix.fiber;
}
