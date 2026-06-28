-- Issue #582: Add content fingerprint for cross-retry event deduplication.
-- The fingerprint is a SHA-256 hex digest of (tx_hash, contract_id, event_type, event_data).
-- It enables detecting content-identical events that bypass the (tx_hash, contract_id, event_type)
-- unique constraint (e.g. same payload re-submitted with a different tx hash during retries).

ALTER TABLE events ADD COLUMN IF NOT EXISTS fingerprint TEXT;

CREATE INDEX IF NOT EXISTS idx_events_fingerprint ON events (fingerprint)
    WHERE fingerprint IS NOT NULL;
