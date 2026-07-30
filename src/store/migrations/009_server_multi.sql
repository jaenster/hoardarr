-- Multi-server upgrade: backup flag, billing mode, and byte counters.
--
-- backup: when 1 the server is consulted only after every non-backup
-- tier has reported the article missing. Distinct from priority because
-- you can have several priority tiers AND a final "backup" tier on top.
--
-- billing_mode: 'flat' (unlimited monthly) or 'metered' (block account
-- billed per byte). The orchestrator prefers flat within a tier.
--
-- quota_bytes: total purchased bytes for metered providers; 0 means
-- "unknown / unlimited", in which case we don't auto-disable.
--
-- used_bytes: monotonic counter, incremented after each successful
-- BODY download. Surfaced on the System page so the operator can
-- see how much of a block account has been consumed.

ALTER TABLE servers ADD COLUMN backup INTEGER NOT NULL DEFAULT 0
    CHECK (backup IN (0, 1));

ALTER TABLE servers ADD COLUMN billing_mode TEXT NOT NULL DEFAULT 'flat'
    CHECK (billing_mode IN ('flat', 'metered'));

ALTER TABLE servers ADD COLUMN quota_bytes INTEGER NOT NULL DEFAULT 0
    CHECK (quota_bytes >= 0);

ALTER TABLE servers ADD COLUMN used_bytes INTEGER NOT NULL DEFAULT 0
    CHECK (used_bytes >= 0);
