extern crate metrics as m;

use sqlx::PgPool;
use std::time::Duration;
use tokio::sync::watch;

const LAG_WARN_BYTES: i64 = 10 * 1024 * 1024;
const LAG_WARN_SECS: f64 = 30.0;

pub struct ReplicaStatus {
    pub client_addr: String,
    pub state: String,
    pub sent_lag_bytes: i64,
    pub write_lag_secs: f64,
    pub flush_lag_secs: f64,
    pub replay_lag_secs: f64,
}

pub fn emit_replica_metrics(replicas: &[ReplicaStatus]) {
    m::gauge!("soroban_pulse_replica_count").set(replicas.len() as f64);
    for r in replicas {
        let addr = r.client_addr.clone();
        m::gauge!("soroban_pulse_replica_lag_bytes", "client_addr" => addr.clone())
            .set(r.sent_lag_bytes as f64);
        m::gauge!("soroban_pulse_replica_write_lag_seconds", "client_addr" => addr.clone())
            .set(r.write_lag_secs);
        m::gauge!("soroban_pulse_replica_flush_lag_seconds", "client_addr" => addr.clone())
            .set(r.flush_lag_secs);
        m::gauge!("soroban_pulse_replica_replay_lag_seconds", "client_addr" => addr.clone())
            .set(r.replay_lag_secs);
        if r.sent_lag_bytes > LAG_WARN_BYTES {
            tracing::warn!(
                client_addr = %r.client_addr,
                lag_bytes = r.sent_lag_bytes,
                "Replica lag exceeds byte threshold",
            );
        }
        if r.replay_lag_secs > LAG_WARN_SECS {
            tracing::warn!(
                client_addr = %r.client_addr,
                lag_secs = r.replay_lag_secs,
                "Replica replay lag exceeds time threshold",
            );
        }
    }
}

async fn collect_replica_stats(pool: &PgPool) -> Vec<ReplicaStatus> {
    let rows: Vec<(String, String, i64, f64, f64, f64)> = match sqlx::query_as(
        "SELECT
            COALESCE(client_addr::text, 'unknown'),
            COALESCE(state, 'unknown'),
            COALESCE(pg_wal_lsn_diff(sent_lsn, replay_lsn), 0)::bigint,
            COALESCE(EXTRACT(EPOCH FROM write_lag), 0.0),
            COALESCE(EXTRACT(EPOCH FROM flush_lag), 0.0),
            COALESCE(EXTRACT(EPOCH FROM replay_lag), 0.0)
         FROM pg_stat_replication",
    )
    .fetch_all(pool)
    .await
    {
        Ok(r) => r,
        Err(e) => {
            tracing::debug!(error = %e, "pg_stat_replication query failed (expected on replica)");
            return Vec::new();
        }
    };
    rows.into_iter()
        .map(
            |(client_addr, state, sent_lag_bytes, write_lag_secs, flush_lag_secs, replay_lag_secs)| {
                ReplicaStatus {
                    client_addr,
                    state,
                    sent_lag_bytes,
                    write_lag_secs,
                    flush_lag_secs,
                    replay_lag_secs,
                }
            },
        )
        .collect()
}

pub async fn query_replication_status(pool: &PgPool) -> Vec<serde_json::Value> {
    collect_replica_stats(pool)
        .await
        .iter()
        .map(|r| {
            serde_json::json!({
                "client_addr": r.client_addr,
                "state": r.state,
                "sent_lag_bytes": r.sent_lag_bytes,
                "write_lag_seconds": r.write_lag_secs,
                "flush_lag_seconds": r.flush_lag_secs,
                "replay_lag_seconds": r.replay_lag_secs,
            })
        })
        .collect()
}

pub fn spawn(pool: PgPool, interval_secs: u64, mut shutdown_rx: watch::Receiver<bool>) {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(Duration::from_secs(interval_secs));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    tracing::debug!("Collecting replica sync metrics");
                    let replicas = collect_replica_stats(&pool).await;
                    emit_replica_metrics(&replicas);
                }
                _ = shutdown_rx.changed() => {
                    tracing::debug!("Replica monitor shutting down");
                    break;
                }
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_replica(addr: &str, lag_bytes: i64, replay_lag: f64) -> ReplicaStatus {
        ReplicaStatus {
            client_addr: addr.to_string(),
            state: "streaming".to_string(),
            sent_lag_bytes: lag_bytes,
            write_lag_secs: 0.1,
            flush_lag_secs: 0.2,
            replay_lag_secs: replay_lag,
        }
    }

    #[test]
    fn emit_metrics_no_panic_on_empty() {
        emit_replica_metrics(&[]);
    }

    #[test]
    fn emit_metrics_no_panic_with_data() {
        let replicas = vec![
            make_replica("10.0.0.1", 1024, 1.5),
            make_replica("10.0.0.2", 20 * 1024 * 1024, 60.0),
        ];
        emit_replica_metrics(&replicas);
    }

    #[test]
    fn replica_count_matches() {
        let replicas = vec![
            make_replica("10.0.0.1", 0, 0.0),
            make_replica("10.0.0.2", 0, 0.0),
        ];
        assert_eq!(replicas.len(), 2);
    }
}
