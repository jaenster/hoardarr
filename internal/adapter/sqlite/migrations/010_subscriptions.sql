-- Subscriptions: outbound webhook (and later: Discord / Slack /
-- Pushover) destinations. Each row is one consumer of a topic set.
--
-- topics is JSON-encoded so this table needs no second relation
-- table; matching is done in code (exact or trailing-wildcard).

CREATE TABLE subscriptions (
    id              INTEGER PRIMARY KEY,

    name            TEXT NOT NULL UNIQUE,

    -- 'webhook' today; future kinds are added without schema change.
    kind            TEXT NOT NULL DEFAULT 'webhook',

    url             TEXT NOT NULL,

    -- JSON array of topic strings. Wildcards: trailing "*" matches
    -- by prefix (e.g. "deliver.*").
    topics          TEXT NOT NULL,

    -- Optional HMAC-SHA256 key. When set, the sender signs the body
    -- and consumers can authenticate the payload.
    secret          TEXT,

    enabled         INTEGER NOT NULL DEFAULT 1 CHECK (enabled IN (0, 1)),

    -- Operational telemetry surfaced in the UI. unix-ms; null means
    -- "never".
    last_success_at INTEGER,
    last_error_at   INTEGER,
    last_error      TEXT,

    created_at      INTEGER NOT NULL,
    updated_at      INTEGER NOT NULL
);

CREATE INDEX subscriptions_enabled ON subscriptions(enabled) WHERE enabled = 1;
