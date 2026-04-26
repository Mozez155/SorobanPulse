use sqlx::PgPool;
use tokio::time::{interval, Duration};
use tracing::{info, warn};

use crate::metrics;

/// Batch size for DELETE operations — keeps transactions short.
const PRUNE_BATCH_SIZE: i64 = 1000;

/// Runs the event retention pruning job as a background Tokio task.
///
/// When `retention_days` is 0 the job is disabled and returns immediately.
/// Otherwise it deletes events older than `retention_days` days in batches
/// of [`PRUNE_BATCH_SIZE`] every `interval_hours` hours.
pub async fn run_pruner(
    pool: PgPool,
    retention_days: u64,
    interval_hours: u64,
    mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
) {
    if retention_days == 0 {
        info!("Event pruning disabled (EVENT_RETENTION_DAYS=0)");
        return;
    }

    info!(
        retention_days = retention_days,
        interval_hours = interval_hours,
        "Event pruner started"
    );

    let mut tick = interval(Duration::from_secs(interval_hours * 3600));

    loop {
        tokio::select! {
            _ = tick.tick() => {
                prune_events(&pool, retention_days).await;
            }
            _ = shutdown_rx.changed() => {
                if *shutdown_rx.borrow() {
                    info!("Event pruner shutting down");
                    break;
                }
            }
        }
    }
}

/// Deletes events older than `retention_days` days in batches.
/// Returns the total number of rows deleted.
pub async fn prune_events(pool: &PgPool, retention_days: u64) -> u64 {
    let mut total_deleted: u64 = 0;

    loop {
        let result = sqlx::query(
            r#"
            DELETE FROM events
            WHERE id IN (
                SELECT id FROM events
                WHERE timestamp < NOW() - ($1 || ' days')::INTERVAL
                LIMIT $2
            )
            "#,
        )
        .bind(retention_days as i64)
        .bind(PRUNE_BATCH_SIZE)
        .execute(pool)
        .await;

        match result {
            Ok(r) => {
                let deleted = r.rows_affected();
                total_deleted += deleted;
                if deleted > 0 {
                    metrics::record_events_pruned(deleted);
                }
                if deleted < PRUNE_BATCH_SIZE as u64 {
                    // No more rows to delete in this run
                    break;
                }
            }
            Err(e) => {
                warn!(error = %e, "Pruning batch failed");
                break;
            }
        }
    }

    if total_deleted > 0 {
        info!(
            deleted = total_deleted,
            retention_days = retention_days,
            "Pruned old events"
        );
    }

    total_deleted
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use serde_json::json;

    #[sqlx::test(migrations = "./migrations")]
    async fn prune_deletes_old_events(pool: PgPool) {
        // Insert an event with a timestamp 100 days ago
        sqlx::query(
            "INSERT INTO events (contract_id, event_type, tx_hash, ledger, timestamp, event_data)
             VALUES ($1, $2, $3, $4, $5, $6)",
        )
        .bind("C_OLD_CONTRACT_PRUNE_TEST_0000000000000000000000000000")
        .bind("contract")
        .bind("a".repeat(64))
        .bind(1_i64)
        .bind(Utc::now() - chrono::Duration::days(100))
        .bind(json!({}))
        .execute(&pool)
        .await
        .unwrap();

        // Insert a recent event (should NOT be pruned)
        sqlx::query(
            "INSERT INTO events (contract_id, event_type, tx_hash, ledger, timestamp, event_data)
             VALUES ($1, $2, $3, $4, $5, $6)",
        )
        .bind("C_NEW_CONTRACT_PRUNE_TEST_0000000000000000000000000000")
        .bind("contract")
        .bind("b".repeat(64))
        .bind(2_i64)
        .bind(Utc::now())
        .bind(json!({}))
        .execute(&pool)
        .await
        .unwrap();

        let deleted = prune_events(&pool, 90).await;
        assert_eq!(deleted, 1, "should have deleted the old event");

        let remaining: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM events")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(remaining, 1, "recent event should remain");
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn prune_keeps_events_within_retention(pool: PgPool) {
        // Insert an event 30 days ago — within 90-day retention window
        sqlx::query(
            "INSERT INTO events (contract_id, event_type, tx_hash, ledger, timestamp, event_data)
             VALUES ($1, $2, $3, $4, $5, $6)",
        )
        .bind("C_RECENT_CONTRACT_PRUNE_TEST_000000000000000000000000000")
        .bind("contract")
        .bind("c".repeat(64))
        .bind(3_i64)
        .bind(Utc::now() - chrono::Duration::days(30))
        .bind(json!({}))
        .execute(&pool)
        .await
        .unwrap();

        let deleted = prune_events(&pool, 90).await;
        assert_eq!(deleted, 0, "event within retention window should not be pruned");
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn prune_disabled_when_retention_zero(pool: PgPool) {
        // Insert an old event
        sqlx::query(
            "INSERT INTO events (contract_id, event_type, tx_hash, ledger, timestamp, event_data)
             VALUES ($1, $2, $3, $4, $5, $6)",
        )
        .bind("C_ZERO_RETENTION_TEST_00000000000000000000000000000000")
        .bind("contract")
        .bind("d".repeat(64))
        .bind(4_i64)
        .bind(Utc::now() - chrono::Duration::days(200))
        .bind(json!({}))
        .execute(&pool)
        .await
        .unwrap();

        // retention_days=0 means disabled — prune_events should not be called,
        // but if called directly it would still delete. The run_pruner function
        // is what checks for 0 and returns early. Test that directly:
        let (tx, rx) = tokio::sync::watch::channel(false);
        let pool_clone = pool.clone();
        let handle = tokio::spawn(async move {
            run_pruner(pool_clone, 0, 24, rx).await;
        });
        // Give it a moment to start and return
        tokio::time::sleep(Duration::from_millis(50)).await;
        let _ = tx.send(true);
        handle.await.unwrap();

        // Event should still be there since pruner was disabled
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM events")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(count, 1, "pruner disabled — event should not be deleted");
    }
}
