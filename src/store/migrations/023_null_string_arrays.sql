-- Normalise list columns that hold the literal text `null`.
--
-- These TEXT columns store a JSON array of strings; par2_sets.failed_files
-- even declares DEFAULT '[]'. A default only applies when a writer omits
-- the column, and the predecessor implementation never did: it encoded
-- the list with Go's encoding/json, which marshals a nil slice as `null`
-- rather than `[]`. The result is durable rows whose stored value
-- contradicts the schema's own statement of what an empty list looks
-- like -- on live databases, the majority of par2_sets rows.
--
-- `null` and `[]` denote the same absence, so this is a pure
-- normalisation: no row changes meaning, and a reader no longer has to
-- know which writer produced a row. Re-running is a no-op, because the
-- WHERE clause stops matching once a row has been rewritten.

UPDATE par2_sets     SET failed_files = '[]' WHERE failed_files = 'null';
UPDATE files         SET groups       = '[]' WHERE groups       = 'null';
UPDATE subscriptions SET topics       = '[]' WHERE topics       = 'null';
