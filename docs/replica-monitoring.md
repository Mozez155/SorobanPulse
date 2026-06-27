# Replica Sync Monitoring

Soroban Pulse monitors PostgreSQL streaming replication lag via a background task (`src/replica_monitor.rs`) and exposes the data through Prometheus metrics and an admin API endpoint.

## Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `soroban_pulse_replica_count` | Gauge | Number of connected streaming replicas |
| `soroban_pulse_replica_lag_bytes` | Gauge | WAL bytes not yet replayed on replica (label: `client_addr`) |
| `soroban_pulse_replica_write_lag_seconds` | Gauge | Lag to replica write acknowledgement |
| `soroban_pulse_replica_flush_lag_seconds` | Gauge | Lag to replica flush acknowledgement |
| `soroban_pulse_replica_replay_lag_seconds` | Gauge | Lag to replay on replica (most meaningful for data currency) |

## API Endpoint

```
GET /v1/admin/replication/status
Authorization: <ADMIN_API_KEY>
```

Example response:

```json
{
  "replicas": [
    {
      "client_addr": "10.0.0.2",
      "state": "streaming",
      "sent_lag_bytes": 8192,
      "write_lag_seconds": 0.01,
      "flush_lag_seconds": 0.02,
      "replay_lag_seconds": 0.03
    }
  ],
  "replica_count": 1
}
```

Returns `[]` replicas when queried from a replica node (pg_stat_replication is only populated on the primary).

## Alert Thresholds

Warnings are logged when:
- `sent_lag_bytes` > 10 MiB
- `replay_lag_seconds` > 30 s

Prometheus alert rules are defined in `docs/alerts.yml` under the `ReplicaLagHigh` and `ReplicaLagCritical` groups.

## Configuration

The monitor polls every 60 seconds by default. It is started automatically on application startup alongside the index monitor.

## Failover Dashboard

The `docs/grafana-dashboard.json` includes panels for:
- Replica count over time
- Replay lag per replica (time series)
- Byte lag waterfall

## Troubleshooting

| Symptom | Action |
|---------|--------|
| `replica_count` = 0 | Check `pg_stat_replication` on primary; verify replica connection |
| High replay lag | Check replica I/O; consider promoting replica or tuning `wal_receiver_timeout` |
| Endpoint returns empty | Endpoint was called on a replica node; query the primary |
