use lettre::message::{header, MultiPart, SinglePart};
use lettre::transport::smtp::authentication::Credentials;
use lettre::{Message, SmtpTransport, Transport};
use secrecy::{ExposeSecret, SecretString};
use std::collections::HashMap;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::{interval, sleep};
use tracing::{error, info, warn};

use crate::{metrics, models::SorobanEvent};

/// Batched email notification sender.
/// Collects events for up to 1 minute, then sends a single summary email.
pub struct EmailNotifier {
    smtp_host: String,
    smtp_port: u16,
    smtp_user: Option<String>,
    smtp_password: Option<SecretString>,
    from: String,
    to: Vec<String>,
    contract_filter: Vec<String>,
}

impl EmailNotifier {
    pub fn new(
        smtp_host: String,
        smtp_port: u16,
        smtp_user: Option<String>,
        smtp_password: Option<SecretString>,
        from: String,
        to: Vec<String>,
        contract_filter: Vec<String>,
    ) -> Self {
        Self {
            smtp_host,
            smtp_port,
            smtp_user,
            smtp_password,
            from,
            to,
            contract_filter,
        }
    }

    /// Spawn a background task that batches events and sends emails every minute.
    pub fn spawn(
        self,
        mut event_rx: tokio::sync::broadcast::Receiver<SorobanEvent>,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut batch_interval = interval(Duration::from_secs(60));
            batch_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            let mut events_buffer: Vec<SorobanEvent> = Vec::new();

            loop {
                tokio::select! {
                    _ = batch_interval.tick() => {
                        if !events_buffer.is_empty() {
                            self.send_batch_email(&events_buffer).await;
                            events_buffer.clear();
                        }
                    }
                    result = event_rx.recv() => {
                        match result {
                            Ok(event) => {
                                // Apply contract filter if configured
                                if !self.contract_filter.is_empty()
                                    && !self.contract_filter.contains(&event.contract_id)
                                {
                                    continue;
                                }
                                events_buffer.push(event);
                            }
                            Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                                warn!(
                                    skipped = n,
                                    "Email notifier lagged, some events skipped"
                                );
                            }
                            Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                                // Channel closed, send any remaining events and exit
                                if !events_buffer.is_empty() {
                                    self.send_batch_email(&events_buffer).await;
                                }
                                break;
                            }
                        }
                    }
                }
            }
        })
    }

    /// Send a summary email for a batch of events.
    async fn send_batch_email(&self, events: &[SorobanEvent]) {
        if events.is_empty() {
            return;
        }

        // Group events by contract ID for better readability
        let mut by_contract: HashMap<String, Vec<&SorobanEvent>> = HashMap::new();
        for event in events {
            by_contract
                .entry(event.contract_id.clone())
                .or_default()
                .push(event);
        }

        let subject = format!(
            "Soroban Pulse: {} new event{} indexed",
            events.len(),
            if events.len() == 1 { "" } else { "s" }
        );

        let mut body = String::new();
        body.push_str(&format!(
            "Soroban Pulse indexed {} new event{} in the last minute.\n\n",
            events.len(),
            if events.len() == 1 { "" } else { "s" }
        ));

        for (contract_id, contract_events) in by_contract.iter() {
            body.push_str(&format!(
                "Contract: {}\n  Events: {}\n",
                contract_id,
                contract_events.len()
            ));

            for event in contract_events.iter().take(10) {
                body.push_str(&format!(
                    "  - Type: {}, Ledger: {}, TxHash: {}\n",
                    event.event_type, event.ledger, event.tx_hash
                ));
            }

            if contract_events.len() > 10 {
                body.push_str(&format!(
                    "  ... and {} more event{}\n",
                    contract_events.len() - 10,
                    if contract_events.len() - 10 == 1 {
                        ""
                    } else {
                        "s"
                    }
                ));
            }
            body.push('\n');
        }

        // Build and send email
        if let Err(e) = self.send_email(&subject, &body).await {
            error!(error = %e, "Failed to send email notification");
            metrics::record_email_failure();
        } else {
            info!(
                recipients = self.to.len(),
                event_count = events.len(),
                "Email notification sent successfully"
            );
        }
    }

    /// Send an email using SMTP.
    async fn send_email(&self, subject: &str, body: &str) -> Result<(), Box<dyn std::error::Error>> {
        // Build message with all recipients
        let mut message_builder = Message::builder()
            .from(self.from.parse()?)
            .subject(subject);

        for recipient in &self.to {
            message_builder = message_builder.to(recipient.parse()?);
        }

        let message = message_builder
            .header(header::ContentType::TEXT_PLAIN)
            .body(body.to_string())?;

        // Build SMTP transport
        let mut transport_builder = SmtpTransport::relay(&self.smtp_host)?
            .port(self.smtp_port);

        if let (Some(user), Some(password)) = (&self.smtp_user, &self.smtp_password) {
            transport_builder = transport_builder.credentials(Credentials::new(
                user.clone(),
                password.expose_secret().clone(),
            ));
        }

        let mailer = transport_builder.build();

        // Send email (blocking operation, run in spawn_blocking)
        let result = tokio::task::spawn_blocking(move || mailer.send(&message)).await?;

        match result {
            Ok(_) => Ok(()),
            Err(e) => Err(Box::new(e)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn mock_event(contract_id: &str, ledger: u64) -> SorobanEvent {
        SorobanEvent {
            contract_id: contract_id.to_string(),
            event_type: "contract".to_string(),
            tx_hash: "abc123".to_string(),
            ledger,
            ledger_closed_at: "2026-04-28T00:00:00Z".to_string(),
            ledger_hash: None,
            in_successful_call: true,
            value: json!({"test": "data"}),
            topic: None,
        }
    }

    #[test]
    fn test_email_notifier_creation() {
        let notifier = EmailNotifier::new(
            "smtp.example.com".to_string(),
            587,
            Some("user".to_string()),
            Some(SecretString::new("pass".to_string())),
            "from@example.com".to_string(),
            vec!["to@example.com".to_string()],
            vec![],
        );

        assert_eq!(notifier.smtp_host, "smtp.example.com");
        assert_eq!(notifier.smtp_port, 587);
        assert_eq!(notifier.from, "from@example.com");
        assert_eq!(notifier.to.len(), 1);
    }

    #[test]
    fn test_secret_string_redacted_in_debug() {
        let secret = SecretString::new("my_password".to_string());
        let debug_str = format!("{:?}", secret);
        assert!(!debug_str.contains("my_password"));
        assert!(debug_str.contains("[REDACTED]"));
    }

    #[test]
    fn test_contract_filter_logic() {
        let filter = vec!["CONTRACT_A".to_string(), "CONTRACT_B".to_string()];
        
        let event_a = mock_event("CONTRACT_A", 100);
        let event_b = mock_event("CONTRACT_B", 101);
        let event_c = mock_event("CONTRACT_C", 102);

        assert!(filter.contains(&event_a.contract_id));
        assert!(filter.contains(&event_b.contract_id));
        assert!(!filter.contains(&event_c.contract_id));
    }

    #[test]
    fn test_empty_contract_filter_allows_all() {
        let filter: Vec<String> = vec![];
        let event = mock_event("ANY_CONTRACT", 100);

        // Empty filter means all events pass
        assert!(filter.is_empty() || filter.contains(&event.contract_id));
    }
}
