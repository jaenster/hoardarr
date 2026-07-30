-- Usenet server registry.
--
-- Each row is one provider account (host:port + credentials + connection
-- caps + priority). The download orchestrator iterates servers in
-- priority-ascending order when an article is missing; lower number
-- means higher priority (matches SABnzbd convention).
--
-- Passwords are stored in plaintext. The threat model is the same as
-- SABnzbd / Sonarr / Radarr: anyone with read access to the data
-- directory can already read everything, and the running binary needs
-- the cleartext password to authenticate against NNTP. Encrypting at
-- rest with a key that lives next to the data buys nothing real.

CREATE TABLE servers (
    id         INTEGER PRIMARY KEY,

    -- Display name. Free-form; unique to make CLI selection unambiguous.
    name       TEXT NOT NULL UNIQUE,

    -- Network endpoint.
    host       TEXT NOT NULL,
    port       INTEGER NOT NULL CHECK (port > 0 AND port <= 65535),

    -- 1 = TLS; 0 = plaintext (rare on modern NNTP).
    tls        INTEGER NOT NULL DEFAULT 1 CHECK (tls IN (0, 1)),

    -- Optional credentials. Some servers permit anonymous access for
    -- public groups, so both can be NULL.
    username   TEXT,
    password   TEXT,

    -- Maximum concurrent connections this provider permits.
    max_conns  INTEGER NOT NULL CHECK (max_conns > 0),

    -- Lower number = higher priority. 0 is "main", 1+ are fallbacks.
    priority   INTEGER NOT NULL DEFAULT 0,

    -- Soft toggle: 0 disables the server without deleting it. The pool
    -- skips disabled servers when assembling its dispatch list.
    enabled    INTEGER NOT NULL DEFAULT 1 CHECK (enabled IN (0, 1)),

    added_at   INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

-- Active servers ordered by priority — the hot read for the orchestrator.
CREATE INDEX servers_active ON servers(priority, id) WHERE enabled = 1;

-- Categories: per-category output dir + optional post-processing script.
-- Used at delivery time (M4); rows here are also surfaced to SAB API
-- consumers (M5) in get_cats responses.
CREATE TABLE categories (
    name        TEXT PRIMARY KEY,
    -- Relative path under complete_dir; empty means complete_dir itself.
    dir         TEXT NOT NULL DEFAULT '',
    -- Optional path to a post-processing script run after delivery.
    post_script TEXT,
    -- Default download priority for jobs assigned this category.
    priority    INTEGER NOT NULL DEFAULT 0,
    added_at    INTEGER NOT NULL,
    updated_at  INTEGER NOT NULL
);

-- Seed the default category so the *arr tools find it on first connect.
-- The literal '*' is SAB's "uncategorized" sentinel.
INSERT INTO categories(name, dir, priority, added_at, updated_at)
VALUES ('*', '', 0, unixepoch('now') * 1000, unixepoch('now') * 1000);
