-- On-demand PAR2 recovery-volume fetching (SABnzbd's "don't download
-- vol files unless needed"). Two columns are added:
--
--   files.is_recovery_vol — true for files matching <name>.vol###+##.par2
--   (the per-slice recovery files). The small index .par2 stays
--   is_recovery_vol=false because it carries the FileDesc / IFSC
--   packets that the verifier needs even when no repair is wanted.
--
--   jobs.fetch_recovery_vols — gate on the orchestrator's
--   PendingSegments query. Defaults to 1 (legacy behaviour: always
--   download every file). When the Settings → General knob
--   "defer recovery vols" is enabled, AddJob seeds this as 0 and the
--   repair worker flips it to 1 only when par2.Repair returns
--   ErrUnrecoverableSet AND there are deferred files to fetch.

ALTER TABLE files ADD COLUMN is_recovery_vol INTEGER NOT NULL DEFAULT 0;
ALTER TABLE jobs  ADD COLUMN fetch_recovery_vols INTEGER NOT NULL DEFAULT 1;
