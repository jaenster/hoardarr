-- Deliver bounded context — one row per Job that has reached the
-- post-verify stage. The row tells us whether a delivery is pending,
-- in flight, complete, failed, or skipped (because the job is an
-- archive and Extract owns the move instead).
--
-- A FK to jobs(id) with ON DELETE CASCADE means RemoveJob also removes
-- delivery history; we treat the delivery row as bookkeeping for the
-- Job's lifecycle, not as a separate audit log.

CREATE TABLE deliveries (
    id          INTEGER PRIMARY KEY,

    job_id      INTEGER NOT NULL UNIQUE
                REFERENCES jobs(id) ON DELETE CASCADE,

    state       TEXT NOT NULL
                CHECK (state IN ('pending','moving','complete','failed','skipped')),

    -- Resolved at insertion: complete_dir/<category-dir>/<release-name>/.
    -- May be empty for a brand-new pending row that hasn't gone through
    -- target-resolution yet.
    target_dir  TEXT NOT NULL DEFAULT '',

    err_msg     TEXT,

    -- Timestamps in unix-ms (UTC). created_at is non-null; the others
    -- are populated as the aggregate transitions.
    created_at  INTEGER NOT NULL,
    started_at  INTEGER,
    finished_at INTEGER
);

CREATE INDEX deliveries_job ON deliveries(job_id);
CREATE INDEX deliveries_state ON deliveries(state) WHERE state IN ('pending','moving');
