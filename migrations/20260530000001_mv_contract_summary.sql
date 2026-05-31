-- #460: Materialized view for per-contract summary data
-- Used by GET /v1/contracts/:contract_id/summary for fast lookups.
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_contract_summary AS
SELECT
    contract_id,
    COUNT(*)                                                        AS total_events,
    MIN(timestamp)                                                  AS first_event_at,
    MAX(timestamp)                                                  AS last_event_at,
    MIN(ledger)                                                     AS min_ledger,
    MAX(ledger)                                                     AS max_ledger,
    COUNT(DISTINCT tx_hash)                                         AS unique_tx_count,
    COUNT(*) FILTER (WHERE event_type = 'contract')                 AS contract_events,
    COUNT(*) FILTER (WHERE event_type = 'diagnostic')               AS diagnostic_events,
    COUNT(*) FILTER (WHERE event_type = 'system')                   AS system_events
FROM events
GROUP BY contract_id;

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_contract_summary_contract_id
    ON mv_contract_summary (contract_id);

-- #461: Ensure the composite index (contract_id, ledger DESC) exists for the
-- combined contract_id + ledger range filter query pattern.
-- This index is already created by 20260325000001_composite_indices.sql but we
-- add a guard here so the CI EXPLAIN ANALYZE check can rely on it.
CREATE INDEX IF NOT EXISTS idx_events_contract_ledger ON events(contract_id, ledger DESC);
