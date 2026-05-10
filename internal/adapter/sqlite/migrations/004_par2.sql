-- PAR2 verify state — one row per Job, tracking the post-download
-- integrity-check lifecycle.
--
-- Created when the verify worker picks up a JobDownloadComplete event;
-- updated as the verification proceeds. Cascade DELETEs with the job.

CREATE TABLE par2_sets (
    id           INTEGER PRIMARY KEY,
    job_id       INTEGER NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,

    -- pending | verifying | ok | repair_needed | failed
    state        TEXT NOT NULL,

    started_at   INTEGER,
    finished_at  INTEGER,

    -- Reason populated when state = failed.
    error_msg    TEXT,

    -- JSON array of filenames that didn't match the PAR2 checksum
    -- (populated when state = repair_needed). Empty when ok.
    failed_files TEXT NOT NULL DEFAULT '[]'
);

-- One verify-set per job — re-verification reuses the same row.
CREATE UNIQUE INDEX par2_sets_job ON par2_sets(job_id);
