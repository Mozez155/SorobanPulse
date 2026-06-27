# Feature Flag Rollback Automation

Soroban Pulse automatically rolls back enabled feature flags when the HTTP error rate spikes above a configurable threshold.

## How It Works

1. A background task (`src/feature_flags.rs`) polls `request_logs` every 60 seconds.
2. It computes the 5xx error rate over a sliding 5-minute window.
3. If the rate exceeds the threshold (default 5%), all flags with `auto_rollback = TRUE` are disabled.
4. Every rollback is written to `feature_flag_audit` with the triggering error rate and reason.

## Database Schema

```sql
-- Feature flags table
feature_flags (id, name, enabled, auto_rollback, rollback_threshold, description, created_at, updated_at)

-- Audit trail
feature_flag_audit (id, flag_id, action, reason, triggered_by, created_at)
```

See `migrations/20260627000001_feature_flags.sql`.

## Prometheus Metric

| Metric | Description |
|--------|-------------|
| `soroban_pulse_feature_flag_error_rate` | Current 5-minute error rate (0.0–1.0) |

## Audit Trail

Every flag state change is appended to `feature_flag_audit`. Query examples:

```sql
-- Recent rollbacks
SELECT f.name, a.reason, a.triggered_by, a.created_at
FROM feature_flag_audit a
JOIN feature_flags f ON f.id = a.flag_id
WHERE a.action = 'rollback'
ORDER BY a.created_at DESC
LIMIT 20;

-- Full history for a specific flag
SELECT action, reason, triggered_by, created_at
FROM feature_flag_audit
WHERE flag_id = '<flag-uuid>'
ORDER BY created_at DESC;
```

## Creating a Feature Flag

```sql
INSERT INTO feature_flags (name, enabled, auto_rollback, description)
VALUES ('new-indexer-path', TRUE, TRUE, 'Experimental indexer code path');
```

## Manual Rollback

```sql
UPDATE feature_flags SET enabled = FALSE, updated_at = NOW() WHERE name = 'my-flag';

INSERT INTO feature_flag_audit (flag_id, action, reason, triggered_by)
SELECT id, 'rollback', 'Manual rollback by on-call', 'operator'
FROM feature_flags WHERE name = 'my-flag';
```

## Tuning

| Parameter | Default | Description |
|-----------|---------|-------------|
| `rollback_threshold` (per flag) | 0.05 | Error rate fraction that triggers rollback |
| `auto_rollback` (per flag) | TRUE | Whether this flag participates in auto-rollback |
| Monitor poll interval | 60 s | How often the watcher checks error rates |
| Error rate window | 300 s | Sliding window for error rate calculation |
