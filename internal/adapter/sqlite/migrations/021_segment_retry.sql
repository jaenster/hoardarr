-- Durable per-segment retry: store the earliest time at which a
-- segment is eligible for re-dispatch. Backoff state lived in-memory
-- on each goroutine before this; a process restart lost it, which
-- led to a flaky article churning through the full retry budget on
-- every boot instead of respecting the back-off window.
--
-- Migration is forward-only: 0 = "ready now", populated on transient
-- failure via Segment.MarkPendingRetry(at).

ALTER TABLE segments
    ADD COLUMN next_retry_at INTEGER NOT NULL DEFAULT 0;

-- The pending-dispatch query becomes
--     WHERE state = 'pending' AND next_retry_at <= ?now
-- so a partial index sorted by next_retry_at lets the orchestrator's
-- "what's due next" lookup stay sub-millisecond even on a 100k-row
-- segments table.
DROP INDEX IF EXISTS seg_pending;
CREATE INDEX seg_pending
    ON segments(next_retry_at, id)
    WHERE state IN ('pending', 'inflight');
