-- Outbox: durable queue of domain events awaiting fan-out to subscribers.
--
-- Events are inserted into this table inside the same transaction that
-- mutates aggregate state, so state changes and event emission are atomic.
-- A background dispatcher (per subscription) reads pending rows and calls
-- subscriber handlers; per-subscription delivery is tracked in outbox_subs.
--
-- Rows are not deleted automatically; a separate retention job (post-v0.1)
-- prunes events older than a configured horizon once they've been delivered
-- to all known subscriptions. For now they accumulate, which is fine for
-- audit/replay and the volume is small.

CREATE TABLE outbox (
    -- Time-ordered UUID (v7); also used as monotonic delivery cursor.
    -- Stored as a 16-byte BLOB so we can index lexicographically.
    id           BLOB PRIMARY KEY,

    -- Topic: "context.aggregate.verb", e.g. "download.job.created".
    topic        TEXT NOT NULL,

    -- Aggregate the event is about. Format owned by the producing context;
    -- typically the aggregate's stable identifier as a string.
    aggregate_id TEXT NOT NULL,

    -- Wall-clock time the event occurred, observed by the producer.
    -- Unix milliseconds.
    occurred_at  INTEGER NOT NULL,

    -- JSON-encoded event body. The Topic determines the schema.
    payload      BLOB NOT NULL
);

-- For per-subscription tailing — each dispatcher fetches the next
-- undelivered event in (id) order. A composite index on
-- (topic, id) helps when subscriptions filter by topic.
CREATE INDEX outbox_topic_id ON outbox(topic, id);

-- outbox_subs: per-subscription delivery tracking.
--
-- Allows late-joining subscriptions to choose between "from-now-forward"
-- (prime cursor at the latest event_id) and "replay all history"
-- (prime cursor at zero / no row yet). Per-event attempt count and
-- last_error support exponential backoff and poison-message parking.
CREATE TABLE outbox_subs (
    -- Stable subscription name. The application service that registers
    -- the subscriber owns the name and it must be unique across the
    -- entire system.
    subscription TEXT NOT NULL,

    -- Event ID this row tracks delivery state for.
    event_id     BLOB NOT NULL,

    -- Time of successful delivery; null until delivered.
    delivered_at INTEGER,

    -- Delivery attempts (1-based). Incremented per try.
    attempts     INTEGER NOT NULL DEFAULT 0,

    -- Last error message from the handler. Cleared on success.
    last_error   TEXT,

    -- Earliest time the dispatcher should retry a failed delivery.
    -- Unix milliseconds. NULL = ready immediately.
    next_retry_at INTEGER,

    PRIMARY KEY (subscription, event_id),
    FOREIGN KEY (event_id) REFERENCES outbox(id) ON DELETE CASCADE
);

-- For the dispatcher's "give me the next pending event for this
-- subscription" query.
CREATE INDEX outbox_subs_pending
    ON outbox_subs(subscription, next_retry_at, event_id)
    WHERE delivered_at IS NULL;
