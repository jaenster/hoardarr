-- Extract bounded context — one row per archive Job, populated when
-- verify.ok fires for a Job containing RAR files. Mirrors the deliver
-- table shape so query patterns stay uniform across post-processing
-- contexts.
--
-- UNIQUE(job_id) makes idempotent retry trivial: we look up by job,
-- get StatePending or nothing, never two competing rows.

CREATE TABLE extracts (
    id          INTEGER PRIMARY KEY,
    job_id      INTEGER NOT NULL UNIQUE
                REFERENCES jobs(id) ON DELETE CASCADE,

    state       TEXT NOT NULL
                CHECK (state IN ('pending','extracting','complete','failed')),

    target_dir  TEXT NOT NULL DEFAULT '',
    err_msg     TEXT,

    created_at  INTEGER NOT NULL,
    started_at  INTEGER,
    finished_at INTEGER
);

CREATE INDEX extracts_job ON extracts(job_id);
CREATE INDEX extracts_state ON extracts(state) WHERE state IN ('pending','extracting');
