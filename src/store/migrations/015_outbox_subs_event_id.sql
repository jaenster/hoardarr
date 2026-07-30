-- 015: speed up outbox cascade + orphan checks.
--
-- outbox_subs's primary key is (subscription, event_id). That covers
-- lookups by subscription but not by event_id alone — so any "find all
-- outbox_subs rows for this event" query (FK cascade on outbox DELETE,
-- orphan sweep in the pruner) falls back to a table scan. With 16M
-- subs rows that took the writer lock long enough to wedge other
-- transactions with SQLITE_BUSY.
CREATE INDEX IF NOT EXISTS outbox_subs_event_id
  ON outbox_subs(event_id);
