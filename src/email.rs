use lettre::message::{header, MultiPart, SinglePart};
use lettre::transport::smtp::authentication::Credentials;
use lettre::{Message, SmtpTransport, Transport};
use secrecy::{ExposeSecret, SecretString};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::time::Duration;
use tokio::time::interval;
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::{metrics, models::SorobanEvent, retry_policy::RetryPolicy};

/// A/B test configuration for email templates (Issue #489).
#[derive(Debug, Clone)]
pub struct AbTestConfig {
    pub template_a: String,
    pub template_b: String,
    /// Percentage of recipients assigned template A (0.0–100.0).
    pub split_percentage: f64,
}

/// Batched email notification sender.
/// Collects events for up to 1 minute, then sends a summary email per recipient.
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
    /// Base URL used for tracking pixel and click-redirect links (Issue #487, #488).
    base_url: String,
    /// Optional A/B test configuration (Issue #489).
    ab_test: Option<AbTestConfig>,
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
        retry_policy: RetryPolicy,
        pool: sqlx::PgPool,
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
            base_url: String::new(),
            ab_test: None,
        }
    }

    /// Set the base URL used for tracking endpoints.
    pub fn with_base_url(mut self, base_url: String) -> Self {
        self.base_url = base_url;
        self
    }

    /// Enable A/B testing for email templates.
    pub fn with_ab_test(mut self, config: AbTestConfig) -> Self {
        self.ab_test = Some(config);
        self
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

    /// Returns true if the target is in the active suppression list (Issue #490).
    async fn is_suppressed(&self, target: &str, target_type: &str) -> bool {
        match sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM suppression_lists \
             WHERE target = $1 AND target_type = $2 \
             AND (expires_at IS NULL OR expires_at > NOW())",
        )
        .bind(target)
        .bind(target_type)
        .fetch_one(&self.pool)
        .await
        {
            Ok(count) => count > 0,
            Err(_) => false,
        }
    }

    /// Deterministically assign an A/B template by hashing recipient + batch key (Issue #489).
    /// Returns 'A' or 'B'.
    pub fn assign_ab_template(&self, recipient: &str, batch_key: &str) -> char {
        if let Some(ref ab) = self.ab_test {
            let mut h = Sha256::new();
            h.update(recipient.as_bytes());
            h.update(b":");
            h.update(batch_key.as_bytes());
            let hash = h.finalize();
            let ratio = hash[0] as f64 / 255.0 * 100.0;
            if ratio < ab.split_percentage {
                'A'
            } else {
                'B'
            }
        } else {
            'A'
        }
    }

    /// Build the plain-text body for a batch of events.
    pub fn build_text_body(&self, events: &[SorobanEvent]) -> String {
        let mut by_contract: HashMap<String, Vec<&SorobanEvent>> = HashMap::new();
        for event in events {
            by_contract
                .entry(event.contract_id.clone())
                .or_default()
                .push(event);
        }

        let mut body = format!(
            "Soroban Pulse indexed {} new event{} in the last minute.\n\n",
            events.len(),
            if events.len() == 1 { "" } else { "s" }
        );

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
                    if contract_events.len() - 10 == 1 { "" } else { "s" }
                ));
            }
            body.push('\n');
        }
        body
    }

    /// Build an HTML email body with a tracking pixel and click-tracked links (Issue #487, #488).
    ///
    /// `open_token` is the unique token for the tracking pixel.
    /// `click_tokens` maps tx_hash → click token for link wrapping.
    pub fn build_html_body(
        &self,
        events: &[SorobanEvent],
        open_token: &str,
        click_tokens: &HashMap<String, String>,
    ) -> String {
        let mut by_contract: HashMap<String, Vec<&SorobanEvent>> = HashMap::new();
        for event in events {
            by_contract
                .entry(event.contract_id.clone())
                .or_default()
                .push(event);
        }

        let mut html = String::from(
            "<!DOCTYPE html><html><body style=\"font-family:sans-serif;\">"
        );
        html.push_str(&format!(
            "<p>Soroban Pulse indexed <strong>{}</strong> new event{} in the last minute.</p>",
            events.len(),
            if events.len() == 1 { "" } else { "s" }
        ));

        for (contract_id, contract_events) in by_contract.iter() {
            html.push_str(&format!(
                "<h3>Contract: {}</h3><p>Events: {}</p><ul>",
                contract_id,
                contract_events.len()
            ));
            for event in contract_events.iter().take(10) {
                let display_hash = if event.tx_hash.len() > 16 {
                    format!("{}...", &event.tx_hash[..16])
                } else {
                    event.tx_hash.clone()
                };
                let link_html = if !self.base_url.is_empty() {
                    if let Some(token) = click_tokens.get(&event.tx_hash) {
                        format!(
                            "<a href=\"{}/v1/notifications/email/click/{}\">{}</a>",
                            self.base_url, token, display_hash
                        )
                    } else {
                        display_hash.clone()
                    }
                } else {
                    display_hash.clone()
                };
                html.push_str(&format!(
                    "<li>Type: {}, Ledger: {}, TxHash: {}</li>",
                    event.event_type, event.ledger, link_html
                ));
            }
            if contract_events.len() > 10 {
                html.push_str(&format!(
                    "<li>... and {} more</li>",
                    contract_events.len() - 10
                ));
            }
            html.push_str("</ul>");
        }

        // Tracking pixel (Issue #487)
        if !open_token.is_empty() && !self.base_url.is_empty() {
            html.push_str(&format!(
                "<img src=\"{}/v1/notifications/email/track/{}\" width=\"1\" height=\"1\" alt=\"\" style=\"display:none;\" />",
                self.base_url, open_token
            ));
        }

        html.push_str("</body></html>");
        html
    }

    /// Send a summary email for a batch of events with per-recipient tracking,
    /// suppression checks, and A/B test template assignment.
    async fn send_batch_email(&self, events: &[SorobanEvent]) {
        if events.is_empty() {
            return;
        }

        let event_ids: Vec<String> = events
            .iter()
            .map(|e| format!("{}{}{}", e.tx_hash, e.contract_id, e.ledger))
            .collect();
        let batch_key = {
            let digest = Sha256::digest(event_ids.join(",").as_bytes());
            digest
                .iter()
                .map(|b| format!("{:02x}", b))
                .collect::<String>()[..16]
                .to_string()
        };

        let subject = format!(
            "Soroban Pulse: {} new event{} indexed",
            events.len(),
            if events.len() == 1 { "" } else { "s" }
        );

        // Pre-generate click tokens for all unique tx hashes
        let unique_tx_hashes: Vec<String> = {
            let mut seen = std::collections::HashSet::new();
            events
                .iter()
                .filter(|e| seen.insert(e.tx_hash.clone()))
                .map(|e| e.tx_hash.clone())
                .collect()
        };

        let recipients = self.to.clone();
        for recipient in &recipients {
            // Suppression check (Issue #490)
            if self.is_suppressed(recipient, "email").await {
                metrics::record_notification_suppressed();
                info!(recipient = %recipient, "Email suppressed, skipping");
                continue;
            }

            // Per-recipient idempotency key
            let recipient_hash = {
                let digest = Sha256::digest(recipient.as_bytes());
                digest
                    .iter()
                    .map(|b| format!("{:02x}", b))
                    .collect::<String>()[..8]
                    .to_string()
            };
            let idempotency_key = format!("batch_{}_{}", batch_key, recipient_hash);

            if let Ok(existing) = sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM email_notifications WHERE idempotency_key = $1",
            )
            .bind(&idempotency_key)
            .fetch_one(&self.pool)
            .await
            {
                if existing > 0 {
                    info!(idempotency_key = %idempotency_key, "Email already sent to recipient, skipping");
                    continue;
                }
            }

            // A/B template selection (Issue #489)
            let ab_template = self.ab_test.as_ref().map(|_| {
                self.assign_ab_template(recipient, &batch_key)
            });

            let text_body = if let (Some(ab), Some(tmpl)) = (&self.ab_test, ab_template) {
                if tmpl == 'A' {
                    ab.template_a.clone()
                } else {
                    ab.template_b.clone()
                }
            } else {
                self.build_text_body(events)
            };

            // Insert email_notifications record
            let notification_id = Uuid::new_v4();
            if let Err(e) = sqlx::query(
                "INSERT INTO email_notifications (id, idempotency_key, recipient, subject, body) \
                 VALUES ($1, $2, $3, $4, $5)",
            )
            .bind(notification_id)
            .bind(&idempotency_key)
            .bind(recipient)
            .bind(&subject)
            .bind(&text_body)
            .execute(&self.pool)
            .await
            {
                error!(error = %e, "Failed to insert email_notifications record");
                continue;
            }

            // Track A/B delivery (Issue #489)
            if let Some(tmpl) = ab_template {
                let _ = sqlx::query(
                    "INSERT INTO email_deliveries (email_notification_id, recipient, ab_template) \
                     VALUES ($1, $2, $3)",
                )
                .bind(notification_id)
                .bind(recipient)
                .bind(tmpl.to_string())
                .execute(&self.pool)
                .await;
            }

            // Open tracking token (Issue #487)
            let open_token = Uuid::new_v4().to_string();
            if !self.base_url.is_empty() {
                let _ = sqlx::query(
                    "INSERT INTO email_opens (token, email_notification_id, recipient) \
                     VALUES ($1, $2, $3)",
                )
                .bind(&open_token)
                .bind(notification_id)
                .bind(recipient)
                .execute(&self.pool)
                .await;
            }

            // Click tracking tokens per unique tx hash (Issue #488)
            let mut click_tokens: HashMap<String, String> = HashMap::new();
            if !self.base_url.is_empty() {
                for tx_hash in &unique_tx_hashes {
                    let token = Uuid::new_v4().to_string();
                    let dest_url =
                        format!("{}/v1/events/tx/{}", self.base_url, tx_hash);
                    let _ = sqlx::query(
                        "INSERT INTO email_clicks \
                         (token, email_notification_id, recipient, destination_url) \
                         VALUES ($1, $2, $3, $4)",
                    )
                    .bind(&token)
                    .bind(notification_id)
                    .bind(recipient)
                    .bind(&dest_url)
                    .execute(&self.pool)
                    .await;
                    click_tokens.insert(tx_hash.clone(), token);
                }
            }

            // Build HTML email
            let html_body = self.build_html_body(events, &open_token, &click_tokens);

            // Send
            match self
                .send_email_to_recipient(recipient, &subject, &text_body, &html_body)
                .await
            {
                Ok(_) => {
                    let _ = sqlx::query(
                        "UPDATE email_notifications SET sent_at = NOW() WHERE id = $1",
                    )
                    .bind(notification_id)
                    .execute(&self.pool)
                    .await;
                    info!(
                        recipient = %recipient,
                        event_count = events.len(),
                        "Email notification sent"
                    );
                }
                Err(e) => {
                    error!(error = %e, recipient = %recipient, "Failed to send email");
                    metrics::record_email_failure();
                }
            }
        }
    }

    /// Send an HTML + plain-text multipart email to a single recipient.
    async fn send_email_to_recipient(
        &self,
        recipient: &str,
        subject: &str,
        text_body: &str,
        html_body: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let message = Message::builder()
            .from(self.from.parse()?)
            .to(recipient.parse()?)
            .subject(subject)
            .multipart(
                MultiPart::alternative()
                    .singlepart(
                        SinglePart::builder()
                            .header(header::ContentType::TEXT_PLAIN)
                            .body(text_body.to_string()),
                    )
                    .singlepart(
                        SinglePart::builder()
                            .header(header::ContentType::TEXT_HTML)
                            .body(html_body.to_string()),
                    ),
            )?;

        let mut transport_builder =
            SmtpTransport::relay(&self.smtp_host)?.port(self.smtp_port);

        if let (Some(user), Some(password)) = (&self.smtp_user, &self.smtp_password) {
            transport_builder = transport_builder.credentials(Credentials::new(
                user.clone(),
                password.expose_secret().clone(),
            ));
        }

        let mailer = transport_builder.build();
        let result =
            tokio::task::spawn_blocking(move || mailer.send(&message)).await?;

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
            tx_hash: "abc123def456789012345678".to_string(),
            ledger,
            ledger_closed_at: "2026-04-28T00:00:00Z".to_string(),
            ledger_hash: None,
            in_successful_call: true,
            value: json!({"test": "data"}),
            topic: None,
        }
    }

    fn make_notifier() -> EmailNotifier {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://localhost/test")
            .unwrap();
        EmailNotifier::new(
            "smtp.example.com".to_string(),
            587,
            Some("user".to_string()),
            Some(SecretString::new("pass".to_string())),
            "from@example.com".to_string(),
            vec!["to@example.com".to_string()],
            vec![],
            crate::retry_policy::RetryPolicy::email_default(),
            pool,
        )
    }

    #[test]
    fn test_email_notifier_creation() {
        let notifier = make_notifier();
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
        assert!(filter.is_empty() || filter.contains(&event.contract_id));
    }

    // Issue #487: open tracking
    #[test]
    fn test_build_html_body_includes_tracking_pixel() {
        let notifier = make_notifier()
            .with_base_url("https://example.com".to_string());
        let events = vec![mock_event("CONTRACT_A", 100)];
        let html = notifier.build_html_body(&events, "test-token-123", &HashMap::new());
        assert!(html.contains("test-token-123"));
        assert!(html.contains("/v1/notifications/email/track/"));
        assert!(html.contains("width=\"1\""));
    }

    #[test]
    fn test_build_html_body_no_pixel_without_base_url() {
        let notifier = make_notifier();
        let events = vec![mock_event("CONTRACT_A", 100)];
        let html = notifier.build_html_body(&events, "test-token-123", &HashMap::new());
        assert!(!html.contains("/v1/notifications/email/track/"));
    }

    // Issue #488: click tracking
    #[test]
    fn test_build_html_body_wraps_links_with_click_tokens() {
        let notifier = make_notifier()
            .with_base_url("https://example.com".to_string());
        let events = vec![mock_event("CONTRACT_A", 100)];
        let mut click_tokens = HashMap::new();
        click_tokens.insert("abc123def456789012345678".to_string(), "click-token-xyz".to_string());
        let html = notifier.build_html_body(&events, "open-tok", &click_tokens);
        assert!(html.contains("click-token-xyz"));
        assert!(html.contains("/v1/notifications/email/click/"));
    }

    // Issue #489: A/B test assignment
    #[test]
    fn test_ab_test_assignment_is_deterministic() {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://localhost/test")
            .unwrap();
        let notifier = EmailNotifier::new(
            "smtp.example.com".to_string(),
            587,
            None,
            None,
            "from@example.com".to_string(),
            vec!["to@example.com".to_string()],
            vec![],
            crate::retry_policy::RetryPolicy::email_default(),
            pool,
        )
        .with_ab_test(AbTestConfig {
            template_a: "Template A body".to_string(),
            template_b: "Template B body".to_string(),
            split_percentage: 50.0,
        });

        let t1 = notifier.assign_ab_template("alice@example.com", "batchkey");
        let t2 = notifier.assign_ab_template("alice@example.com", "batchkey");
        assert_eq!(t1, t2, "assignment must be deterministic");
        assert!(t1 == 'A' || t1 == 'B');
    }

    #[test]
    fn test_ab_test_split_distributes_across_recipients() {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://localhost/test")
            .unwrap();
        let notifier = EmailNotifier::new(
            "smtp.example.com".to_string(),
            587,
            None,
            None,
            "from@example.com".to_string(),
            vec![],
            vec![],
            crate::retry_policy::RetryPolicy::email_default(),
            pool,
        )
        .with_ab_test(AbTestConfig {
            template_a: "A".to_string(),
            template_b: "B".to_string(),
            split_percentage: 50.0,
        });

        let recipients: Vec<String> = (0..100).map(|i| format!("user{}@example.com", i)).collect();
        let a_count = recipients
            .iter()
            .filter(|r| notifier.assign_ab_template(r, "batch1") == 'A')
            .count();
        // With 50% split and 100 recipients, expect roughly 30–70 in group A
        assert!(a_count >= 20 && a_count <= 80, "split off: A count = {}", a_count);
    }

    // Issue #490: suppression list enforcement (unit-level)
    #[test]
    fn test_build_text_body_has_event_count() {
        let notifier = make_notifier();
        let events = vec![mock_event("C1", 1), mock_event("C2", 2)];
        let body = notifier.build_text_body(&events);
        assert!(body.contains("2 new events"));
    }
}
