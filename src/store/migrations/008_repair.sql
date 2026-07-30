-- Repair bounded context — one row per Job that the verify worker
-- flagged as damaged. State machine mirrors deliver/extract for
-- uniform query patterns.

CREATE TABLE repairs (
    id          INTEGER PRIMARY KEY,
    job_id      INTEGER NOT NULL UNIQUE
                REFERENCES jobs(id) ON DELETE CASCADE,
    state       TEXT NOT NULL
                CHECK (state IN ('pending','repairing','ok','failed')),
    err_msg     TEXT,
    created_at  INTEGER NOT NULL,
    started_at  INTEGER,
    finished_at INTEGER
);

CREATE INDEX repairs_job ON repairs(job_id);
CREATE INDEX repairs_state ON repairs(state) WHERE state IN ('pending','repairing');
