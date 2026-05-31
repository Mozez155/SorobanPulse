use reqwest::Client;
use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::time::Duration;
use tokio::sync::broadcast;
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::{metrics, models::SorobanEvent, retry_policy::RetryPolicy};

#[derive(Debug, Serialize, Deserialize)]
pub struct TwilioConfig {
    pub account_sid: String,
    pub auth_token: SecretString,
    pub from_number: String,
    pub to_numbers: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct TwilioResponse {
    sid: String,
    status: String,
    error_message: Option<String>,
}

pub struct SmsNotifier {
    client: Client,
    config: TwilioConfig,
    contract_filter: Vec<String>,
    retry_policy: RetryPolicy,
    pool: sqlx::PgPool,
}

impl SmsNotifier {
    pub fn new(
        config: TwilioConfig,
        contract_filter: Vec<String>,
        retry_policy: RetryPolicy,
        pool: sqlx::PgPool,
    ) -> Self {
        let client = Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .expect("Failed to build SMS HTTP client");

        Self {
            client,
            config,
            contract_filter,
            retry_policy,
            pool,
        }
    }

    pub fn spawn(
        self,
        mut event_rx: broadcast::Receiver<SorobanEvent>,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            loop {
                match event_rx.recv().await {
                    Ok(event) => {
                        // Apply contract filter if configured
                        if !self.contract_filter.is_empty()
                            && !self.contract_filter.contains(&event.contract_id)
                        {
                            continue;
                        }

                        // Check if this is a critical event (customize logic as needed)
                        if self.is_critical_event(&event) {
                            for phone_number in &self.config.to_numbers {
                                let message = self.format_sms_message(&event);
                                self.send_sms(phone_number.clone(), message, &event).await;
                            }
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        warn!(
                            skipped = n,
                            "SMS notifier lagged, some events skipped"
                        );
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        })
    }

    fn is_critical_event(&self, event: &SorobanEvent) -> bool {
        // Customize this logic based on your needs
        // For now, consider all contract events as potentially critical
        matches!(event.event_type, crate::models::EventType::Contract)
    }

    fn format_sms_message(&self, event: &SorobanEvent) -> String {
        let base_message = format!(
            "Soroban Alert: {} event on contract {} at ledger {}",
            event.event_type,
            &event.contract_id[..8], // Truncate contract ID
            event.ledger
        );

        // Truncate to 140 chars to leave room for link
        let truncated = if base_message.len() > 140 {
            format!("{}...", &base_message[..137])
        } else {
            base_message
        };

        // Add link to full event (customize URL as needed)
        format!("{} View: https://soroban-pulse.com/events/{}", truncated, event.id)
    }

    async fn send_sms(&self, phone_number: String, message: String, event: &SorobanEvent) {
        let notification_id = Uuid::new_v4();

        // Insert pending notification
        if let Err(e) = sqlx::query(
            "INSERT INTO sms_notifications (id, phone_number, message, status) VALUES ($1, $2, $3, 'pending')"
        )
        .bind(notification_id)
        .bind(&phone_number)
        .bind(&message)
        .execute(&self.pool)
        .await
        {
            error!(error = %e, "Failed to insert SMS notification record");
            return;
        }

        let result = self.retry_policy.execute_with_retry(|attempt| {
            let client = self.client.clone();
            let config = &self.config;
            let phone_number = phone_number.clone();
            let message = message.clone();
            
            async move {
                self.send_twilio_sms(&client, config, &phone_number, &message, attempt).await
            }
        }).await;

        match result {
            Ok(twilio_sid) => {
                info!(
                    phone_number = %phone_number,
                    twilio_sid = %twilio_sid,
                    contract_id = %event.contract_id,
                    "SMS sent successfully"
                );

                // Update notification record
                if let Err(e) = sqlx::query(
                    "UPDATE sms_notifications SET twilio_sid = $1, status = 'sent', sent_at = NOW() WHERE id = $2"
                )
                .bind(&twilio_sid)
                .bind(notification_id)
                .execute(&self.pool)
                .await
                {
                    error!(error = %e, "Failed to update SMS notification status");
                }

                metrics::increment_counter("soroban_pulse_sms_notifications_total", &[("status", "success")]);
            }
            Err(error_msg) => {
                error!(
                    phone_number = %phone_number,
                    error = %error_msg,
                    contract_id = %event.contract_id,
                    "SMS delivery failed after all retries"
                );

                // Update notification record with error
                if let Err(e) = sqlx::query(
                    "UPDATE sms_notifications SET status = 'failed', error_message = $1 WHERE id = $2"
                )
                .bind(&error_msg)
                .bind(notification_id)
                .execute(&self.pool)
                .await
                {
                    error!(error = %e, "Failed to update SMS notification error status");
                }

                metrics::increment_counter("soroban_pulse_sms_notifications_total", &[("status", "failure")]);
            }
        }
    }

    async fn send_twilio_sms(
        &self,
        client: &Client,
        config: &TwilioConfig,
        to: &str,
        message: &str,
        attempt: u32,
    ) -> Result<String, String> {
        let url = format!(
            "https://api.twilio.com/2010-04-01/Accounts/{}/Messages.json",
            config.account_sid
        );

        let form_data = [
            ("From", config.from_number.as_str()),
            ("To", to),
            ("Body", message),
        ];

        let response = client
            .post(&url)
            .basic_auth(&config.account_sid, Some(config.auth_token.expose_secret()))
            .form(&form_data)
            .send()
            .await
            .map_err(|e| format!("HTTP request failed: {}", e))?;

        if response.status().is_success() {
            let twilio_response: TwilioResponse = response
                .json()
                .await
                .map_err(|e| format!("Failed to parse Twilio response: {}", e))?;

            if let Some(error_msg) = twilio_response.error_message {
                return Err(format!("Twilio API error: {}", error_msg));
            }

            Ok(twilio_response.sid)
        } else {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            Err(format!("Twilio API returned {}: {}", status, body))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{EventType, SorobanEvent};
    use chrono::Utc;
    use serde_json::json;

    #[test]
    fn test_format_sms_message() {
        let config = TwilioConfig {
            account_sid: "test".to_string(),
            auth_token: SecretString::new("test".to_string()),
            from_number: "+1234567890".to_string(),
            to_numbers: vec!["+0987654321".to_string()],
        };

        let pool = sqlx::PgPool::connect("postgresql://test").await.unwrap();
        let notifier = SmsNotifier::new(
            config,
            vec![],
            RetryPolicy::sms_default(),
            pool,
        );

        let event = SorobanEvent {
            id: Uuid::new_v4(),
            contract_id: "CABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890".to_string(),
            event_type: EventType::Contract,
            tx_hash: "test_hash".to_string(),
            ledger: 12345,
            timestamp: Utc::now(),
            event_data: json!({"test": "data"}),
        };

        let message = notifier.format_sms_message(&event);
        assert!(message.len() <= 160);
        assert!(message.contains("CABCDEFG")); // Truncated contract ID
        assert!(message.contains("12345")); // Ledger number
        assert!(message.contains("https://soroban-pulse.com/events/"));
    }

    #[test]
    fn test_is_critical_event() {
        let config = TwilioConfig {
            account_sid: "test".to_string(),
            auth_token: SecretString::new("test".to_string()),
            from_number: "+1234567890".to_string(),
            to_numbers: vec!["+0987654321".to_string()],
        };

        let pool = sqlx::PgPool::connect("postgresql://test").await.unwrap();
        let notifier = SmsNotifier::new(
            config,
            vec![],
            RetryPolicy::sms_default(),
            pool,
        );

        let contract_event = SorobanEvent {
            id: Uuid::new_v4(),
            contract_id: "test".to_string(),
            event_type: EventType::Contract,
            tx_hash: "test".to_string(),
            ledger: 1,
            timestamp: Utc::now(),
            event_data: json!({}),
        };

        let system_event = SorobanEvent {
            id: Uuid::new_v4(),
            contract_id: "test".to_string(),
            event_type: EventType::System,
            tx_hash: "test".to_string(),
            ledger: 1,
            timestamp: Utc::now(),
            event_data: json!({}),
        };

        assert!(notifier.is_critical_event(&contract_event));
        assert!(!notifier.is_critical_event(&system_event));
    }
}