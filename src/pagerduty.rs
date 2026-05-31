use reqwest::Client;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::{metrics, models::SorobanEvent};

/// PagerDuty Events API v2 client
pub struct PagerDutyClient {
    client: Client,
    routing_key: String,
    service_name: String,
    severity_mapping: HashMap<String, String>,
    auto_resolve: bool,
}

impl PagerDutyClient {
    pub fn new(
        routing_key: String,
        service_name: String,
        severity_mapping: HashMap<String, String>,
        auto_resolve: bool,
    ) -> Self {
        let client = Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .expect("Failed to build PagerDuty HTTP client");

        Self {
            client,
            routing_key,
            service_name,
            severity_mapping,
            auto_resolve,
        }
    }

    /// Trigger a PagerDuty incident for an event
    pub async fn trigger_incident(
        &self,
        event: &SorobanEvent,
        pool: Option<&sqlx::PgPool>,
    ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        let dedup_key = format!("soroban-pulse-{}-{}", event.contract_id, event.event_type);
        let severity = self.severity_mapping
            .get(&event.event_type.to_string())
            .unwrap_or(&"error".to_string())
            .clone();

        let payload = json!({
            "routing_key": self.routing_key,
            "event_action": "trigger",
            "dedup_key": dedup_key,
            "payload": {
                "summary": format!("Soroban contract event: {} on {}", event.event_type, event.contract_id),
                "source": self.service_name,
                "severity": severity,
                "component": "soroban-contract",
                "group": event.contract_id,
                "class": event.event_type,
                "custom_details": {
                    "contract_id": event.contract_id,
                    "event_type": event.event_type,
                    "tx_hash": event.tx_hash,
                    "ledger": event.ledger,
                    "timestamp": event.timestamp,
                    "event_data": event.event_data
                }
            }
        });

        let response = self.send_event(payload).await?;
        
        // Store incident in database for auto-resolve tracking
        if let Some(pool) = pool {
            let incident_key = response.get("incident_key")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());

            if let Err(e) = sqlx::query(
                "INSERT INTO pagerduty_incidents (dedup_key, contract_id, event_type, incident_key, status) 
                 VALUES ($1, $2, $3, $4, 'triggered')
                 ON CONFLICT (dedup_key) DO UPDATE SET 
                 incident_key = EXCLUDED.incident_key, 
                 status = 'triggered',
                 resolved_at = NULL"
            )
            .bind(&dedup_key)
            .bind(&event.contract_id)
            .bind(&event.event_type.to_string())
            .bind(&incident_key)
            .execute(pool)
            .await
            {
                error!(error = %e, "Failed to store PagerDuty incident in database");
            }
        }

        info!(
            contract_id = %event.contract_id,
            event_type = %event.event_type,
            dedup_key = %dedup_key,
            "PagerDuty incident triggered"
        );

        Ok(dedup_key)
    }

    /// Resolve a PagerDuty incident
    pub async fn resolve_incident(
        &self,
        dedup_key: &str,
        pool: Option<&sqlx::PgPool>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let payload = json!({
            "routing_key": self.routing_key,
            "event_action": "resolve",
            "dedup_key": dedup_key
        });

        self.send_event(payload).await?;

        // Update incident status in database
        if let Some(pool) = pool {
            if let Err(e) = sqlx::query(
                "UPDATE pagerduty_incidents SET status = 'resolved', resolved_at = NOW() 
                 WHERE dedup_key = $1"
            )
            .bind(dedup_key)
            .execute(pool)
            .await
            {
                error!(error = %e, "Failed to update PagerDuty incident status in database");
            }
        }

        info!(dedup_key = %dedup_key, "PagerDuty incident resolved");
        Ok(())
    }

    /// Send event to PagerDuty Events API v2
    async fn send_event(&self, payload: Value) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        let mut backoff_ms = 1000u64;
        
        for attempt in 1..=3u32 {
            let response = self.client
                .post("https://events.pagerduty.com/v2/enqueue")
                .header("Content-Type", "application/json")
                .json(&payload)
                .send()
                .await;

            match response {
                Ok(resp) if resp.status().is_success() => {
                    let body: Value = resp.json().await?;
                    return Ok(body);
                }
                Ok(resp) => {
                    let status = resp.status();
                    let body = resp.text().await.unwrap_or_default();
                    warn!(
                        status = %status,
                        body = %body,
                        attempt = attempt,
                        "PagerDuty API request failed"
                    );
                }
                Err(e) => {
                    warn!(
                        error = %e,
                        attempt = attempt,
                        "PagerDuty API request error"
                    );
                }
            }

            if attempt < 3 {
                sleep(Duration::from_millis(backoff_ms)).await;
                backoff_ms *= 2;
            }
        }

        metrics::record_pagerduty_failure();
        Err("PagerDuty delivery failed after 3 attempts".into())
    }

    /// Check for incidents that should be auto-resolved
    pub async fn auto_resolve_stale_incidents(
        &self,
        pool: &sqlx::PgPool,
        stale_threshold_minutes: i64,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if !self.auto_resolve {
            return Ok(());
        }

        // Find incidents that haven't had new events for the threshold period
        let stale_incidents: Vec<(String, String)> = sqlx::query_as(
            "SELECT DISTINCT pi.dedup_key, pi.contract_id
             FROM pagerduty_incidents pi
             WHERE pi.status = 'triggered'
             AND NOT EXISTS (
                 SELECT 1 FROM events e 
                 WHERE e.contract_id = pi.contract_id 
                 AND e.event_type = pi.event_type::text
                 AND e.created_at > NOW() - INTERVAL '1 minute' * $1
             )"
        )
        .bind(stale_threshold_minutes)
        .fetch_all(pool)
        .await?;

        for (dedup_key, contract_id) in stale_incidents {
            if let Err(e) = self.resolve_incident(&dedup_key, Some(pool)).await {
                error!(
                    error = %e,
                    dedup_key = %dedup_key,
                    contract_id = %contract_id,
                    "Failed to auto-resolve PagerDuty incident"
                );
            }
        }

        Ok(())
    }
}

/// Deliver an event to PagerDuty with retry logic
pub async fn deliver_pagerduty(
    client: &PagerDutyClient,
    event: SorobanEvent,
    pool: Option<&sqlx::PgPool>,
) {
    if let Err(e) = client.trigger_incident(&event, pool).await {
        error!(
            error = %e,
            contract_id = %event.contract_id,
            event_type = %event.event_type,
            "Failed to deliver PagerDuty notification"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_pagerduty_client_creation() {
        let mut severity_mapping = HashMap::new();
        severity_mapping.insert("contract".to_string(), "error".to_string());
        severity_mapping.insert("diagnostic".to_string(), "warning".to_string());
        severity_mapping.insert("system".to_string(), "info".to_string());

        let client = PagerDutyClient::new(
            "test-routing-key".to_string(),
            "Test Service".to_string(),
            severity_mapping,
            true,
        );

        assert_eq!(client.routing_key, "test-routing-key");
        assert_eq!(client.service_name, "Test Service");
        assert!(client.auto_resolve);
    }
}