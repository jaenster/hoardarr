-- speed_history: long-term storage for downsampled download throughput.
--
-- One row per minute. The background flusher writes the most recent
-- minute's average bytes/sec from the in-memory ring. /api/v1/system/
-- speed-history reads from this table for ranges longer than the
-- in-memory window (1 hour).
--
-- Retention is handled by a scheduled purge (entries older than ~30
-- days are deleted); ~43k rows at steady state, tiny.

CREATE TABLE speed_history (
  bucket_at      INTEGER PRIMARY KEY,  -- unix-seconds, aligned to the minute
  bytes_per_sec  INTEGER NOT NULL
);
