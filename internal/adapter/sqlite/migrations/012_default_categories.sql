-- Seed default tv / movies categories so a fresh install lines up
-- with the *arr suite out of the box. Sonarr defaults to category
-- "tv" and Radarr to "movies"; both expect the category subdir
-- under complete/ to exist when they call addfile.
--
-- INSERT OR IGNORE so this is a no-op for installs where the
-- operator already created (or removed) these categories. The
-- migration runs exactly once per database thanks to
-- schema_migrations, but the OR IGNORE keeps semantics stable if
-- an operator wipes the migration row and re-applies.
INSERT OR IGNORE INTO categories(name, dir, priority, added_at, updated_at)
VALUES
  ('tv',     'tv',     0, unixepoch('now') * 1000, unixepoch('now') * 1000),
  ('movies', 'movies', 0, unixepoch('now') * 1000, unixepoch('now') * 1000);
