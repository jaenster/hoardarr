-- settings: live key/value store for runtime-mutable configuration.
--
-- Replaces the runtime-editable subset of config.toml so the
-- Settings → General/Bandwidth/Auth/Paths UI is the single source of
-- truth and survives container rebuilds (no .toml volume needed).
--
-- Schema is intentionally simple — TEXT values, callers parse to the
-- expected shape. Keeps the table generic so future settings don't
-- need migrations.
--
-- Bootstrap-only fields stay in config.toml or env vars: server.listen,
-- server.data_dir, storage.sqlite.path, server.log_level. Those must
-- be known before the DB is even open.

CREATE TABLE settings (
  key        TEXT PRIMARY KEY,
  value      TEXT NOT NULL,
  updated_at INTEGER NOT NULL
);
