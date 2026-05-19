-- commands: durable queue for async one-off operations the operator
-- (or another component) triggers. Distinct from scheduled_tasks
-- (which are cron-like recurring/oneshot) — commands are single-fire
-- and surfaced individually in the UI with their status/result.
--
-- ClaimNext is the worker's interaction: UPDATE the oldest queued
-- row to status='running' atomically with started_at set. Trigger
-- captures provenance (manual UI button / api caller / scheduled task
-- subscriber) so the operator can filter "what *I* started".

CREATE TABLE commands (
  id          INTEGER PRIMARY KEY,
  name        TEXT NOT NULL,
  body        BLOB,
  trigger     TEXT NOT NULL,                        -- 'manual'|'api'|'scheduled'
  status      TEXT NOT NULL DEFAULT 'queued',       -- 'queued'|'running'|'completed'
  result      TEXT NOT NULL DEFAULT '',             -- 'successful'|'failed' on completion
  error       TEXT NOT NULL DEFAULT '',
  queued_at   INTEGER NOT NULL,                     -- unix-ms
  started_at  INTEGER,                              -- unix-ms when worker claimed
  ended_at    INTEGER                               -- unix-ms when completion recorded
);

-- Worker query: oldest queued first. The WHERE clause filters by
-- status so the index stays focused on queued rows.
CREATE INDEX commands_queued ON commands(queued_at) WHERE status = 'queued';

-- History query: list most recent N regardless of state.
CREATE INDEX commands_recent ON commands(id DESC);
