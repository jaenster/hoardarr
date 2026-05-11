-- Per-server bandwidth cap. 0 means "no per-server cap" (the default).
-- The global cap (Config.Bandwidth.GlobalBytesPerSec) is a separate
-- knob applied in addition; the effective rate is min(global,
-- per-server) when both are non-zero.

ALTER TABLE servers ADD COLUMN bandwidth_bytes_per_sec INTEGER NOT NULL DEFAULT 0
    CHECK (bandwidth_bytes_per_sec >= 0);
