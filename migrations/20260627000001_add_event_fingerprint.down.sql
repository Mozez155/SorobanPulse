DROP INDEX IF EXISTS idx_events_fingerprint;
ALTER TABLE events DROP COLUMN IF EXISTS fingerprint;
