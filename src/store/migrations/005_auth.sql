-- User accounts + browser sessions.
--
-- Auth context: hoardarr previously had a single API key as its only
-- credential. Web UI now uses username/password sessions. The API
-- key remains alongside (for *arr clients and SAB-API consumers that
-- can't do form auth).
--
-- First-run flow: when users count = 0, the API exposes a
-- /api/v1/auth/setup endpoint that creates the first admin. Once a
-- user exists, /setup is gone and login is required.

CREATE TABLE users (
    id            INTEGER PRIMARY KEY,

    -- Username, lowercased on insert. UNIQUE so login can be
    -- case-insensitive without ambiguity.
    username      TEXT NOT NULL UNIQUE,

    -- bcrypt hash (or other adapter-chosen algorithm). The format is
    -- self-describing in the hash string, so we can migrate algorithms
    -- without a schema change.
    password_hash TEXT NOT NULL,

    -- Permission tier. v0.1 has only "admin" but the column is here so
    -- adding read-only / per-category roles later is a data migration
    -- not a schema migration.
    role          TEXT NOT NULL DEFAULT 'admin',

    created_at    INTEGER NOT NULL,
    updated_at    INTEGER NOT NULL
);

CREATE TABLE sessions (
    -- Cryptographic random hex token, presented by the client via
    -- HTTP-only cookie. Sessions are looked up by primary key only.
    token       TEXT PRIMARY KEY,

    user_id     INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,

    created_at  INTEGER NOT NULL,
    expires_at  INTEGER NOT NULL,
    last_seen   INTEGER NOT NULL
);

-- For "log this user out everywhere".
CREATE INDEX sessions_user ON sessions(user_id);
