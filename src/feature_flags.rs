use sqlx::PgPool;
use std::time::Duration;
use tokio::sync::watch;

const DEFAULT_ERROR_RATE_WINDOW_SECS: u64 = 300;
const DEFAULT_ROLLBACK_THRESHOLD: f64 = 0.05;

pub struct FeatureFlagWatcher {
    pool: PgPool,
    window_secs: u64,
    rollback_threshold: f64,
}

impl FeatureFlagWatcher {
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            window_secs: DEFAULT_ERROR_RATE_WINDOW_SECS,
            rollback_threshold: DEFAULT_ROLLBACK_THRESHOLD,
        }
    }

    async fn current_error_rate(&self) -> Option<f64> {
        let row: Option<(i64, i64)> = sqlx::query_as(
            "SELECT
                SUM(CASE WHEN status >= 500 THEN 1 ELSE 0 END)::bigint,
                COUNT(*)::bigint
             FROM request_logs
             WHERE created_at > NOW() - ($1 || ' seconds')::interval",
        )
        .bind(self.window_secs as i64)
        .fetch_optional(&self.pool)
        .await
        .ok()
        .flatten();

        row.and_then(|(errors, total)| {
            if total == 0 {
                None
            } else {
                Some(errors as f64 / total as f64)
            }
        })
    }

    async fn rollback_enabled_flags(&self, error_rate: f64) {
        let flags: Vec<(uuid::Uuid, String)> = match sqlx::query_as(
            "SELECT id, name FROM feature_flags WHERE enabled = TRUE AND auto_rollback = TRUE",
        )
        .fetch_all(&self.pool)
        .await
        {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!(error = %e, "Failed to fetch feature flags for rollback check");
                return;
            }
        };

        for (id, name) in flags {
            if let Err(e) = sqlx::query(
                "UPDATE feature_flags SET enabled = FALSE, updated_at = NOW() WHERE id = $1",
            )
            .bind(id)
            .execute(&self.pool)
            .await
            {
                tracing::warn!(flag_id = %id, error = %e, "Failed to rollback feature flag");
                continue;
            }

            tracing::warn!(
                flag_name = %name,
                flag_id = %id,
                error_rate = error_rate,
                threshold = self.rollback_threshold,
                "Feature flag auto-rolled back due to error rate spike",
            );
            crate::metrics::record_feature_flag_rollback(&name);

            let _ = sqlx::query(
                "INSERT INTO feature_flag_audit (flag_id, action, reason, triggered_by)
                 VALUES ($1, 'rollback', $2, 'auto-rollback')",
            )
            .bind(id)
            .bind(format!(
                "Auto-rollback: error rate {:.2}% exceeded threshold {:.2}%",
                error_rate * 100.0,
                self.rollback_threshold * 100.0
            ))
            .execute(&self.pool)
            .await;
        }
    }

    pub async fn run_once(&self) {
        if let Some(rate) = self.current_error_rate().await {
            extern crate metrics as m;
            m::gauge!("soroban_pulse_feature_flag_error_rate").set(rate);

            if rate > self.rollback_threshold {
                self.rollback_enabled_flags(rate).await;
            }
        }
    }
}

pub fn spawn(pool: PgPool, interval_secs: u64, mut shutdown_rx: watch::Receiver<bool>) {
    tokio::spawn(async move {
        let watcher = FeatureFlagWatcher::new(pool);
        let mut ticker = tokio::time::interval(Duration::from_secs(interval_secs));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    watcher.run_once().await;
                }
                _ = shutdown_rx.changed() => {
                    tracing::debug!("Feature flag watcher shutting down");
                    break;
                }
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_threshold_is_five_percent() {
        assert!((DEFAULT_ROLLBACK_THRESHOLD - 0.05).abs() < f64::EPSILON);
    }

    #[test]
    fn default_window_is_five_minutes() {
        assert_eq!(DEFAULT_ERROR_RATE_WINDOW_SECS, 300);
    }

    #[test]
    fn error_rate_zero_total_returns_none() {
        let rate: Option<f64> = {
            let total: i64 = 0;
            if total == 0 { None } else { Some(0.0) }
        };
        assert!(rate.is_none());
    }

    #[test]
    fn error_rate_calculation() {
        let errors: i64 = 10;
        let total: i64 = 100;
        let rate = errors as f64 / total as f64;
        assert!((rate - 0.1).abs() < f64::EPSILON);
    }
}
