-- Capture which client put this job on the queue. Populated from
-- the HTTP User-Agent header on /api/v1/queue/nzb and
-- /sabnzbd/api?mode=addfile. Typical values: "Sonarr/4.0.5.1710",
-- "Radarr/5.4.6.8723", "" (manual uploads via curl / browser drop).
--
-- Nullable + default empty so existing rows don't need backfill.
ALTER TABLE jobs ADD COLUMN source TEXT NOT NULL DEFAULT '';
