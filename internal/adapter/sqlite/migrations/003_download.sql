-- Download domain: jobs, files, segments.
--
-- Each Job is one NZB. A Job has many Files; each File has many
-- Segments (one per Usenet article). Segments are written to disk at
-- their known offset (computed from yEnc =ypart begin) before their
-- row flips to 'done', so a crash mid-fetch can restart safely:
-- on boot the orchestrator resets any 'inflight' rows to 'pending'
-- and re-fetches; the WriteAt at the same offset is idempotent.
--
-- Hot-query indexes (see plan doc): jobs_active for "what's running
-- right now", files_job for "is this job done downloading", and
-- seg_pending for "give me work to do".

CREATE TABLE jobs (
    id           INTEGER PRIMARY KEY,

    -- sha256 (hex) of the NZB body. Dedupe key — re-adding the same NZB
    -- finds the existing row instead of creating a duplicate.
    nzb_hash     TEXT NOT NULL UNIQUE,

    name         TEXT NOT NULL,
    category     TEXT NOT NULL DEFAULT '',
    priority     INTEGER NOT NULL DEFAULT 0,

    -- queue_order is the explicit user ordering within priority. New
    -- jobs default to a monotonic timestamp so insertion order is
    -- the natural ordering until the user reorders.
    queue_order  INTEGER NOT NULL,

    state        TEXT NOT NULL,

    total_bytes  INTEGER NOT NULL,
    done_bytes   INTEGER NOT NULL DEFAULT 0,
    failed_bytes INTEGER NOT NULL DEFAULT 0,

    added_at     INTEGER NOT NULL,
    started_at   INTEGER,
    finished_at  INTEGER,
    error_msg    TEXT,

    -- The original NZB bytes. Tens of KB typically; useful for re-queue
    -- from history without holding onto the original file path.
    nzb_blob     BLOB NOT NULL
);

-- Active queue: drives the orchestrator's "next jobs" query every tick.
CREATE INDEX jobs_active ON jobs(priority ASC, queue_order ASC, id ASC)
    WHERE state IN ('queued', 'downloading', 'paused');

CREATE TABLE files (
    id            INTEGER PRIMARY KEY,
    job_id        INTEGER NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,

    filename      TEXT NOT NULL,        -- subject-derived; may be renamed at delivery
    poster        TEXT,
    -- groups serialized as JSON array (one or two newsgroups in practice).
    groups        TEXT NOT NULL,

    size_bytes    INTEGER NOT NULL,     -- estimate from yEnc =ybegin size or NZB sum
    state         TEXT NOT NULL,        -- pending | downloading | complete | failed

    segment_count INTEGER NOT NULL,
    segments_done INTEGER NOT NULL DEFAULT 0,

    is_par2       INTEGER NOT NULL DEFAULT 0 CHECK (is_par2 IN (0, 1))
);
CREATE INDEX files_job ON files(job_id, state);

CREATE TABLE segments (
    id          INTEGER PRIMARY KEY,
    file_id     INTEGER NOT NULL REFERENCES files(id) ON DELETE CASCADE,

    seq_index   INTEGER NOT NULL,        -- 1-based within file
    message_id  TEXT NOT NULL,           -- without surrounding angle brackets
    bytes       INTEGER NOT NULL,        -- declared article size from NZB

    state       TEXT NOT NULL,           -- pending | inflight | done | missing | failed

    attempts    INTEGER NOT NULL DEFAULT 0,
    last_error  TEXT,

    -- 0-based byte offset within the assembled file. Set after the
    -- first successful yEnc decode (=ypart begin - 1, or 0 for
    -- single-part articles).
    file_offset INTEGER NOT NULL DEFAULT 0,

    UNIQUE (file_id, seq_index)
);
CREATE INDEX seg_file ON segments(file_id, seq_index);
CREATE INDEX seg_pending ON segments(state)
    WHERE state IN ('pending', 'inflight');
