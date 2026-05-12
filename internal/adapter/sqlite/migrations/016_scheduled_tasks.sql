-- scheduled_tasks: durable scheduler for recurring and one-shot work
-- that must survive process restart. Replaces ad-hoc time.Ticker
-- goroutines for things like outbox prune, WAL checkpoint, SQLite
-- ANALYZE, future RSS polls, etc.
--
-- Cadence is a Go duration string ("5m", "1h30m") OR null for one-shot.
-- A claim model (status='running' + claimed_at) lets a single worker
-- own a task without a separate lock — crash recovery just resets
-- stale claims at startup.

CREATE TABLE scheduled_tasks (
  id                   INTEGER PRIMARY KEY,
  name                 TEXT NOT NULL UNIQUE,
  kind                 TEXT NOT NULL,                -- 'recurring' | 'oneshot'
  cadence              TEXT,                         -- Go duration, e.g. "5m"; null for oneshot
  payload              BLOB,                         -- handler-specific JSON
  next_run_at          INTEGER NOT NULL,             -- unix-ms
  last_run_at          INTEGER,                      -- unix-ms; null until first run
  last_error           TEXT,
  consecutive_failures INTEGER NOT NULL DEFAULT 0,
  enabled              INTEGER NOT NULL DEFAULT 1,   -- 0/1 bool
  status               TEXT NOT NULL DEFAULT 'idle', -- 'idle' | 'running'
  claimed_at           INTEGER,                      -- unix-ms when worker took it
  created_at           INTEGER NOT NULL,
  updated_at           INTEGER NOT NULL
);

CREATE INDEX scheduled_tasks_due
  ON scheduled_tasks(next_run_at)
  WHERE enabled = 1 AND status = 'idle';
