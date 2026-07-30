test {
    _ = @import("app/ports.zig");
    _ = @import("app/naming.zig");
    _ = @import("app/verify.zig");
    _ = @import("app/repair.zig");
    _ = @import("app/extract.zig");
    _ = @import("app/schedule.zig");
    _ = @import("app/auth.zig");
    _ = @import("app/download/ports.zig");
    _ = @import("app/download/orchestrator.zig");
    _ = @import("app/download/add_job.zig");
    _ = @import("app/download/queue.zig");
    _ = @import("app/download/service.zig");
    _ = @import("app/download/bandwidth.zig");
    _ = @import("app/download/byte_accounter.zig");
    _ = @import("app/download/tiered_fetcher.zig");
    _ = @import("app/deliver/obfuscation.zig");
    _ = @import("app/deliver/postprocess.zig");
    _ = @import("app/deliver/service.zig");
    _ = @import("app/system/throughput.zig");
    _ = @import("app/system/service.zig");
    _ = @import("app/command/service.zig");
    _ = @import("app/command/handlers.zig");
}
