-- Seed the remaining two *arr-default categories so Lidarr (music)
-- and Readarr (books) also work out-of-the-box. Migration 012 already
-- seeded tv (Sonarr) and movies (Radarr); this fills the set.
--
-- INSERT OR IGNORE — safe against operators who already created these
-- categories manually before pulling this build.
INSERT OR IGNORE INTO categories(name, dir, priority, added_at, updated_at)
VALUES
  ('music', 'music', 0, unixepoch('now') * 1000, unixepoch('now') * 1000),
  ('books', 'books', 0, unixepoch('now') * 1000, unixepoch('now') * 1000);
