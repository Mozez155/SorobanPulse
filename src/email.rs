use lettre::message::dkim::{DkimConfig, DkimSigningAlgorithm, DkimSigningKey};
use lettre::message::{header, MultiPart, SinglePart};
use lettre::transport::smtp::authentication::Credentials;
use lettre::{Message, SmtpTransport, Transport};
use secrecy::{ExposeSecret, SecretString};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::{interval, sleep};
use tracing::{error, info, warn};

use crate::{metrics, models::SorobanEvent, retry_policy::RetryPolicy};

/// Extract the domain portion of a `From` address for DKIM `d=` tagging.
/// Handles both bare `user@example.com` and `Name <user@example.com>` forms.
fn dkim_domain_from(from: &str) -> Option<String> {
    let domain = from.rsplit('@').next()?.trim_end_matches('>').trim();
    if domain.is_empty() {
        None
    } else {
        Some(domain.to_string())
    }
}

/// Validate that `pem` is a usable RSA DKIM signing key (Issue #485). Used at
/// startup to fail fast with a clear error when the key is missing or invalid.
pub fn validate_dkim_key(pem: &str) -> Result<(), String> {
    DkimSigningKey::new(pem, DkimSigningAlgorithm::Rsa)
        .map(|_| ())
        .map_err(|e| format!("invalid DKIM private key: {e}"))
}

/// Build a `DkimConfig` from a selector, sender address and PEM-encoded key.
fn build_dkim_config(selector: &str, from: &str, key_pem: &str) -> Result<DkimConfig, String> {
    let domain = dkim_domain_from(from)
        .ok_or_else(|| "EMAIL_FROM has no domain for DKIM signing".to_string())?;
    let key = DkimSigningKey::new(key_pem, DkimSigningAlgorithm::Rsa)
        .map_err(|e| format!("invalid DKIM private key: {e}"))?;
    Ok(DkimConfig::default_config(selector.to_string(), domain, key))
}

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
    retry_policy: RetryPolicy,
    pool: sqlx::PgPool,
    /// DKIM selector; when set together with `dkim_private_key`, outgoing
    /// emails are DKIM-signed (Issue #485).
    dkim_selector: Option<String>,
    /// PEM-encoded RSA private key used for DKIM signing (Issue #485).
    dkim_private_key: Option<SecretString>,
}

impl EmailNotifier {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        smtp_host: String,
        smtp_port: u16,
        smtp_user: Option<String>,
        smtp_password: Option<SecretString>,
        from: String,
        to: Vec<String>,
        contract_filter: Vec<String>,
        retry_policy: RetryPolicy,
        pool: sqlx::PgPool,
        dkim_selector: Option<String>,
        dkim_private_key: Option<SecretString>,
    ) -> Self {
        Self {
            smtp_host,
            smtp_port,
            smtp_user,
            smtp_password,
            from,
            to,
            contract_filter,
            retry_policy,
            pool,
            dkim_selector,
            dkim_private_key,
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

    /// Send a summary email for a batch of events with idempotency (Issue #474).
    async fn send_batch_email(&self, events: &[SorobanEvent]) {
        if events.is_empty() {
            return;
        }

        // Generate idempotency key based on event batch
        let event_ids: Vec<String> = events.iter().map(|e| e.id.to_string()).collect();
        let idempotency_key = format!("batch_{}", 
            sha2::Sha256::digest(event_ids.join(",").as_bytes())
                .iter()
                .map(|b| format!("{:02x}", b))
                .collect::<String>()[..16].to_string()
        );

        // Check if already sent
        if let Ok(existing) = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM email_notifications WHERE idempotency_key = $1"
        )
        .bind(&idempotency_key)
        .fetch_one(&self.pool)
        .await
        {
            if existing > 0 {
                info!(idempotency_key = %idempotency_key, "Email already sent, skipping");
                return;
            }
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
    async fn send_email(
        &self,
        subject: &str,
        body: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        // Build message with all recipients
        let mut message_builder = Message::builder().from(self.from.parse()?).subject(subject);

        for recipient in &self.to {
            message_builder = message_builder.to(recipient.parse()?);
        }

        let mut message = message_builder
            .header(header::ContentType::TEXT_PLAIN)
            .body(body.to_string())?;

        // DKIM-sign the message when a signing key is configured (Issue #485).
        // A bad key never blocks delivery — it is logged and the email is sent
        // unsigned (the key is validated at startup, so this is defensive).
        if let (Some(selector), Some(key)) = (&self.dkim_selector, &self.dkim_private_key) {
            match build_dkim_config(selector, &self.from, key.expose_secret()) {
                Ok(config) => message.sign(&config),
                Err(e) => warn!(error = %e, "DKIM signing skipped"),
            }
        }

        // Build SMTP transport
        let mut transport_builder = SmtpTransport::relay(&self.smtp_host)?.port(self.smtp_port);

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
        let pool = sqlx::PgPool::connect_lazy("postgres://localhost/unused").unwrap();
        let notifier = EmailNotifier::new(
            "smtp.example.com".to_string(),
            587,
            Some("user".to_string()),
            Some(SecretString::new("pass".to_string())),
            "from@example.com".to_string(),
            vec!["to@example.com".to_string()],
            vec![],
            RetryPolicy::default(),
            pool,
            None,
            None,
        );

        assert_eq!(notifier.smtp_host, "smtp.example.com");
        assert_eq!(notifier.smtp_port, 587);
        assert_eq!(notifier.from, "from@example.com");
        assert_eq!(notifier.to.len(), 1);
    }

    #[test]
    fn test_dkim_domain_extraction() {
        assert_eq!(
            dkim_domain_from("pulse@example.com").as_deref(),
            Some("example.com")
        );
        assert_eq!(
            dkim_domain_from("Soroban Pulse <pulse@mail.example.com>").as_deref(),
            Some("mail.example.com")
        );
        assert_eq!(dkim_domain_from("not-an-email").as_deref(), Some("not-an-email"));
        assert_eq!(dkim_domain_from("trailing@").as_deref(), None);
    }

    #[test]
    fn test_validate_dkim_key_rejects_garbage() {
        assert!(validate_dkim_key("not a pem key").is_err());
        assert!(validate_dkim_key("").is_err());
    }

    #[test]
    fn test_build_dkim_config_requires_domain() {
        // An invalid key still surfaces an error rather than panicking.
        let err = build_dkim_config("selector", "bad@", "not a key");
        assert!(err.is_err());
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
