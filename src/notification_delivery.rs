//! Notification delivery receipts (Issue #475).
//!
//! The notification system delivers events to configured channels (webhook,
//! email, …) but, without delivery receipts, operators cannot determine whether
//! a notification was actually delivered. Compliance requirements often mandate
//! proof of delivery for critical alerts.
//!
//! Every delivery attempt is recorded in the `notification_deliveries` table
//! and exposed through the `GET /v1/admin/notifications/deliveries` endpoint.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::metrics;

/// Outcome of a single notification delivery attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeliveryStatus {
    Success,
    Failure,
}

impl DeliveryStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            DeliveryStatus::Success => "success",
            DeliveryStatus::Failure => "failure",
        }
    }
}

/// A persisted delivery receipt as returned by the admin query endpoint.
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct DeliveryReceipt {
    pub id: Uuid,
    pub channel_type: String,
    pub channel_config_id: Option<Uuid>,
    pub event_id: Option<Uuid>,
    pub status: String,
    pub delivered_at: DateTime<Utc>,
    pub error: Option<String>,
}

/// Record a single delivery attempt in `notification_deliveries` and increment
/// the matching success/failure counter.
///
/// Recording failures must never mask the original delivery error, so a DB
/// error here is logged and swallowed rather than propagated.
pub async fn record_delivery(
    pool: &sqlx::PgPool,
    channel_type: &str,
    channel_config_id: Option<Uuid>,
    event_id: Option<Uuid>,
    status: DeliveryStatus,
    error: Option<&str>,
) {
    match status {
        DeliveryStatus::Success => metrics::record_notification_delivery_success(),
        DeliveryStatus::Failure => metrics::record_notification_delivery_failure(),
    }

    if let Err(e) = sqlx::query(
        "INSERT INTO notification_deliveries \
         (channel_type, channel_config_id, event_id, status, error) \
         VALUES ($1, $2, $3, $4, $5)",
    )
    .bind(channel_type)
    .bind(channel_config_id)
    .bind(event_id)
    .bind(status.as_str())
    .bind(error)
    .execute(pool)
    .await
    {
        tracing::error!(error = %e, "Failed to record notification delivery receipt");
    }
}

/// Best-effort resolution of the `events.id` for a delivered event so the
/// receipt can be linked back to the originating event. Returns `None` if the
/// event cannot be found or the lookup fails.
pub async fn resolve_event_id(
    pool: &sqlx::PgPool,
    event: &crate::models::SorobanEvent,
) -> Option<Uuid> {
    sqlx::query_scalar::<_, Uuid>(
        "SELECT id FROM events \
         WHERE tx_hash = $1 AND contract_id = $2 AND event_type = $3 \
         LIMIT 1",
    )
    .bind(&event.tx_hash)
    .bind(&event.contract_id)
    .bind(&event.event_type)
    .fetch_optional(pool)
    .await
    .ok()
    .flatten()
}

/// Query delivery history, most recent first. Supports optional filtering by
/// channel type and status, and a bounded limit.
pub async fn query_deliveries(
    pool: &sqlx::PgPool,
    channel_type: Option<&str>,
    status: Option<&str>,
    limit: i64,
) -> Result<Vec<DeliveryReceipt>, sqlx::Error> {
    sqlx::query_as::<_, DeliveryReceipt>(
        "SELECT id, channel_type, channel_config_id, event_id, status, delivered_at, error \
         FROM notification_deliveries \
         WHERE ($1::text IS NULL OR channel_type = $1) \
           AND ($2::text IS NULL OR status = $2) \
         ORDER BY delivered_at DESC \
         LIMIT $3",
    )
    .bind(channel_type)
    .bind(status)
    .bind(limit)
    .fetch_all(pool)
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::PgPool;

    #[test]
    fn delivery_status_serializes_to_expected_strings() {
        assert_eq!(DeliveryStatus::Success.as_str(), "success");
        assert_eq!(DeliveryStatus::Failure.as_str(), "failure");
    }

    #[sqlx::test]
    async fn record_delivery_persists_a_success_receipt(pool: PgPool) {
        let event_id = Uuid::new_v4();
        record_delivery(
            &pool,
            "webhook",
            None,
            Some(event_id),
            DeliveryStatus::Success,
            None,
        )
        .await;

        let rows = query_deliveries(&pool, None, None, 100).await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].channel_type, "webhook");
        assert_eq!(rows[0].status, "success");
        assert_eq!(rows[0].event_id, Some(event_id));
        assert!(rows[0].error.is_none());
    }

    #[sqlx::test]
    async fn record_delivery_persists_a_failure_with_error(pool: PgPool) {
        record_delivery(
            &pool,
            "webhook",
            None,
            None,
            DeliveryStatus::Failure,
            Some("HTTP 500: boom"),
        )
        .await;

        let rows = query_deliveries(&pool, None, None, 100).await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].status, "failure");
        assert_eq!(rows[0].error.as_deref(), Some("HTTP 500: boom"));
    }

    #[sqlx::test]
    async fn query_deliveries_filters_by_channel_and_status(pool: PgPool) {
        record_delivery(&pool, "webhook", None, None, DeliveryStatus::Success, None).await;
        record_delivery(&pool, "email", None, None, DeliveryStatus::Failure, Some("smtp")).await;

        let only_email = query_deliveries(&pool, Some("email"), None, 100).await.unwrap();
        assert_eq!(only_email.len(), 1);
        assert_eq!(only_email[0].channel_type, "email");

        let only_failures = query_deliveries(&pool, None, Some("failure"), 100).await.unwrap();
        assert_eq!(only_failures.len(), 1);
        assert_eq!(only_failures[0].status, "failure");

        let all = query_deliveries(&pool, None, None, 100).await.unwrap();
        assert_eq!(all.len(), 2);
    }
}
