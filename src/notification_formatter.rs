use serde_json::{json, Value};
use std::collections::HashMap;

use crate::models::{NotificationFormat, SorobanEvent};

/// Format an event for different notification platforms
pub fn format_notification(
    event: &SorobanEvent,
    format: NotificationFormat,
    template: Option<&str>,
) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
    match format {
        NotificationFormat::Raw => Ok(serde_json::to_value(event)?),
        NotificationFormat::Slack => format_slack_message(event, template),
        NotificationFormat::Discord => format_discord_message(event, template),
        NotificationFormat::Teams => format_teams_message(event, template),
        NotificationFormat::Pagerduty => format_pagerduty_message(event, template),
    }
}

/// Format event as Slack Block Kit message
fn format_slack_message(
    event: &SorobanEvent,
    template: Option<&str>,
) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
    if let Some(tmpl) = template {
        return format_with_template(event, tmpl);
    }

    let color = match event.event_type.as_str() {
        "contract" => "#36a64f", // Green
        "diagnostic" => "#ff9500", // Orange
        "system" => "#2196F3", // Blue
        _ => "#808080", // Gray
    };

    let event_data_preview = if let Some(obj) = event.event_data.as_object() {
        obj.keys()
            .take(3)
            .map(|k| format!("`{}`", k))
            .collect::<Vec<_>>()
            .join(", ")
    } else {
        "N/A".to_string()
    };

    Ok(json!({
        "attachments": [{
            "color": color,
            "blocks": [
                {
                    "type": "header",
                    "text": {
                        "type": "plain_text",
                        "text": format!("Soroban Event: {}", event.event_type)
                    }
                },
                {
                    "type": "section",
                    "fields": [
                        {
                            "type": "mrkdwn",
                            "text": format!("*Contract:*\n`{}`", event.contract_id)
                        },
                        {
                            "type": "mrkdwn",
                            "text": format!("*Type:*\n{}", event.event_type)
                        },
                        {
                            "type": "mrkdwn",
                            "text": format!("*Ledger:*\n{}", event.ledger)
                        },
                        {
                            "type": "mrkdwn",
                            "text": format!("*Timestamp:*\n{}", event.timestamp.format("%Y-%m-%d %H:%M:%S UTC"))
                        }
                    ]
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": format!("*Transaction:* `{}`\n*Event Data:* {}", event.tx_hash, event_data_preview)
                    }
                }
            ]
        }]
    }))
}

/// Format event as Discord embed message
fn format_discord_message(
    event: &SorobanEvent,
    template: Option<&str>,
) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
    if let Some(tmpl) = template {
        return format_with_template(event, tmpl);
    }

    let color = match event.event_type.as_str() {
        "contract" => 0x36a64f, // Green
        "diagnostic" => 0xff9500, // Orange
        "system" => 0x2196F3, // Blue
        _ => 0x808080, // Gray
    };

    let event_data_preview = if let Some(obj) = event.event_data.as_object() {
        obj.keys()
            .take(5)
            .map(|k| format!("`{}`", k))
            .collect::<Vec<_>>()
            .join(", ")
    } else {
        "N/A".to_string()
    };

    Ok(json!({
        "embeds": [{
            "title": format!("Soroban Event: {}", event.event_type),
            "color": color,
            "timestamp": event.timestamp.to_rfc3339(),
            "fields": [
                {
                    "name": "Contract ID",
                    "value": format!("`{}`", event.contract_id),
                    "inline": true
                },
                {
                    "name": "Event Type",
                    "value": event.event_type,
                    "inline": true
                },
                {
                    "name": "Ledger",
                    "value": event.ledger.to_string(),
                    "inline": true
                },
                {
                    "name": "Transaction Hash",
                    "value": format!("`{}`", event.tx_hash),
                    "inline": false
                },
                {
                    "name": "Event Data Keys",
                    "value": event_data_preview,
                    "inline": false
                }
            ],
            "footer": {
                "text": "Soroban Pulse"
            }
        }]
    }))
}

/// Format event as Microsoft Teams message
fn format_teams_message(
    event: &SorobanEvent,
    template: Option<&str>,
) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
    if let Some(tmpl) = template {
        return format_with_template(event, tmpl);
    }

    let theme_color = match event.event_type.as_str() {
        "contract" => "36a64f", // Green
        "diagnostic" => "ff9500", // Orange
        "system" => "2196F3", // Blue
        _ => "808080", // Gray
    };

    Ok(json!({
        "@type": "MessageCard",
        "@context": "http://schema.org/extensions",
        "themeColor": theme_color,
        "summary": format!("Soroban Event: {}", event.event_type),
        "sections": [{
            "activityTitle": format!("Soroban Event: {}", event.event_type),
            "activitySubtitle": format!("Contract: {}", event.contract_id),
            "facts": [
                {
                    "name": "Event Type",
                    "value": event.event_type
                },
                {
                    "name": "Ledger",
                    "value": event.ledger.to_string()
                },
                {
                    "name": "Transaction Hash",
                    "value": event.tx_hash
                },
                {
                    "name": "Timestamp",
                    "value": event.timestamp.format("%Y-%m-%d %H:%M:%S UTC").to_string()
                }
            ],
            "markdown": true
        }]
    }))
}

/// Format event as PagerDuty event (used when webhook format is pagerduty)
fn format_pagerduty_message(
    event: &SorobanEvent,
    template: Option<&str>,
) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
    if let Some(tmpl) = template {
        return format_with_template(event, tmpl);
    }

    let severity = match event.event_type.as_str() {
        "contract" => "error",
        "diagnostic" => "warning",
        "system" => "info",
        _ => "error",
    };

    Ok(json!({
        "routing_key": "PLACEHOLDER_ROUTING_KEY", // Will be replaced by actual routing key
        "event_action": "trigger",
        "dedup_key": format!("soroban-pulse-{}-{}", event.contract_id, event.event_type),
        "payload": {
            "summary": format!("Soroban contract event: {} on {}", event.event_type, event.contract_id),
            "source": "Soroban Pulse",
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
    }))
}

/// Format event using a custom Handlebars template
fn format_with_template(
    event: &SorobanEvent,
    template: &str,
) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
    // Simple template replacement for now - could be enhanced with Handlebars crate
    let mut result = template.to_string();
    
    // Create template context
    let mut context = HashMap::new();
    context.insert("contract_id", event.contract_id.clone());
    context.insert("event_type", event.event_type.to_string());
    context.insert("tx_hash", event.tx_hash.clone());
    context.insert("ledger", event.ledger.to_string());
    context.insert("timestamp", event.timestamp.to_rfc3339());
    
    // Simple string replacement
    for (key, value) in context {
        result = result.replace(&format!("{{{{{}}}}}", key), &value);
    }
    
    // Try to parse as JSON, fallback to plain text
    match serde_json::from_str::<Value>(&result) {
        Ok(json_val) => Ok(json_val),
        Err(_) => Ok(json!({ "text": result })),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use crate::models::EventType;

    fn create_test_event() -> SorobanEvent {
        SorobanEvent {
            id: uuid::Uuid::new_v4(),
            contract_id: "CABC123456789".to_string(),
            event_type: EventType::Contract,
            tx_hash: "abcdef123456789".to_string(),
            ledger: 12345,
            timestamp: Utc::now(),
            event_data: json!({"action": "transfer", "amount": 1000}),
            tenant_id: None,
        }
    }

    #[test]
    fn test_slack_formatting() {
        let event = create_test_event();
        let result = format_slack_message(&event, None).unwrap();
        
        assert!(result.get("attachments").is_some());
        let attachment = &result["attachments"][0];
        assert!(attachment.get("blocks").is_some());
        assert_eq!(attachment["color"], "#36a64f");
    }

    #[test]
    fn test_discord_formatting() {
        let event = create_test_event();
        let result = format_discord_message(&event, None).unwrap();
        
        assert!(result.get("embeds").is_some());
        let embed = &result["embeds"][0];
        assert_eq!(embed["color"], 0x36a64f);
        assert!(embed["title"].as_str().unwrap().contains("contract"));
    }

    #[test]
    fn test_teams_formatting() {
        let event = create_test_event();
        let result = format_teams_message(&event, None).unwrap();
        
        assert_eq!(result["@type"], "MessageCard");
        assert_eq!(result["themeColor"], "36a64f");
        assert!(result.get("sections").is_some());
    }

    #[test]
    fn test_template_formatting() {
        let event = create_test_event();
        let template = r#"{"message": "Event {{event_type}} on contract {{contract_id}}"}"#;
        let result = format_with_template(&event, template).unwrap();
        
        assert!(result["message"].as_str().unwrap().contains("contract"));
        assert!(result["message"].as_str().unwrap().contains("CABC123456789"));
    }
}