-- 014: index for the history query path.
--
-- /api/v1/history and SAB ?mode=history both filter by terminal state
-- and order by finished_at DESC. The existing jobs_active index is
-- (priority, queue_order, id) — useless for that query. Sonarr polls
-- history every ~minute and history grows over time, so a full table
-- scan + sort hurts more every week.
--
-- Partial index keeps the index small (only terminal rows) and includes
-- exactly the ORDER BY columns so SQLite can return rows without sort.
CREATE INDEX IF NOT EXISTS jobs_history
  ON jobs(finished_at DESC, id DESC)
  WHERE state IN ('completed', 'failed', 'aborted');
