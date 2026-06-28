# Event Deduplication

SorobanPulse uses a layered deduplication strategy so that an event is stored **at most once**, even when the indexer retries the same ledger range or when the same event is re-submitted with a different transaction hash.

## Deduplication Layers

Checks run in priority order — first match wins:

### 1. Bloom Filter (Issue #266)

An in-memory probabilistic set keyed on `(tx_hash, contract_id, event_type)`. Events that are **very likely already stored** are skipped before any database I/O.

- False positives → a new event is incorrectly skipped (rare, bounded by the configured FP rate).
- False negatives → impossible by design; the DB constraint catches anything the bloom filter misses.
- **Metric:** `soroban_pulse_bloom_filter_hits_total`

### 2. Content Fingerprint (Issue #582)

A SHA-256 hex digest of `(tx_hash, contract_id, event_type, event_data)` stored in the `fingerprint` column. When `ENABLE_CONTENT_DEDUP=true`, this fingerprint is checked against recent rows before inserting, catching content-identical events even if they arrived with a different `tx_hash` during a retry.

- The check is bounded by `DEDUP_WINDOW_SECS` (default: 3 600 seconds / 1 hour) to limit scan cost.
- Non-fatal: if the fingerprint query fails the insert proceeds and the DB constraint acts as a backstop.
- **Metric:** `soroban_pulse_content_dedup_hits_total`

### 3. Database Unique Constraint

The `events` table has a unique constraint on `(tx_hash, contract_id, event_type)`. Inserts that violate this constraint are silently ignored via `ON CONFLICT DO NOTHING`.

- **Metric:** rows affected == 0 → `events_skipped_duplicate` in the indexer cycle log.

## Fingerprint Computation

```
fingerprint = sha256(tx_hash + NUL + contract_id + NUL + event_type + NUL + json(event_data))
```

The JSON serialisation of `event_data` uses `serde_json::Value::to_string()` (canonical key order within an object is not guaranteed by JSON — two events with the same logical data but different field ordering may produce different fingerprints). This is intentional: the fingerprint supplements the DB constraint, which is the authoritative deduplication guard.

## Configuration

| Environment Variable | Default | Description |
|----------------------|---------|-------------|
| `ENABLE_CONTENT_DEDUP` | `false` | Enable fingerprint-based content deduplication. |
| `DEDUP_WINDOW_SECS` | `3600` | Lookback window for fingerprint checks (seconds). |
| `BLOOM_FILTER_CAPACITY` | `1000000` | Bloom filter item capacity. |
| `BLOOM_FILTER_FP_RATE` | `0.001` | Target false-positive rate for the bloom filter. |

## Guarantees

| Scenario | Outcome |
|----------|---------|
| Same event re-indexed (same tx_hash, contract, type) | Deduplicated by bloom filter or DB constraint |
| Same event with a different tx_hash (retry scenario, `ENABLE_CONTENT_DEDUP=true`) | Deduplicated by fingerprint check within the window |
| Same event with a different tx_hash (`ENABLE_CONTENT_DEDUP=false`) | **Not** deduplicated — stored as a new row |
| Different event, same tx_hash prefix collision | Not deduplicated (different `(tx_hash, contract_id, event_type)` tuple) |

## Metrics

| Metric | Description |
|--------|-------------|
| `soroban_pulse_bloom_filter_hits_total` | Events skipped by bloom filter |
| `soroban_pulse_content_dedup_hits_total` | Events skipped by fingerprint check |
| `soroban_pulse_fingerprints_stored_total` | Fingerprints written on successful insert |
| `soroban_pulse_events_duplicate_total` | General duplicate counter (DB constraint) |
