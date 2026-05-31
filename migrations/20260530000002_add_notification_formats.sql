-- Add notification format support to webhook configurations
-- This will be used to store webhook format preferences

CREATE TABLE IF NOT EXISTS webhook_configs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    url TEXT NOT NULL,
    secret TEXT,
    notification_format TEXT NOT NULL DEFAULT 'raw', -- raw, slack, discord, teams, pagerduty
    message_template TEXT, -- Optional Handlebars template
    contract_filter TEXT[], -- Array of contract IDs to filter
    event_type_filter TEXT[], -- Array of event types to filter
    active BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_webhook_configs_active ON webhook_configs(active);
CREATE INDEX IF NOT EXISTS idx_webhook_configs_format ON webhook_configs(notification_format);

-- Add PagerDuty specific configuration table
CREATE TABLE IF NOT EXISTS pagerduty_configs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    routing_key TEXT NOT NULL,
    service_name TEXT NOT NULL DEFAULT 'Soroban Pulse',
    contract_filter TEXT[], -- Array of contract IDs to monitor
    event_type_filter TEXT[], -- Array of event types to monitor
    severity_mapping JSONB NOT NULL DEFAULT '{"contract": "error", "diagnostic": "warning", "system": "info"}',
    auto_resolve BOOLEAN NOT NULL DEFAULT true,
    active BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_pagerduty_configs_active ON pagerduty_configs(active);

-- Add incident tracking for PagerDuty auto-resolve
CREATE TABLE IF NOT EXISTS pagerduty_incidents (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    dedup_key TEXT NOT NULL UNIQUE,
    contract_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    incident_key TEXT, -- PagerDuty incident key
    status TEXT NOT NULL DEFAULT 'triggered', -- triggered, acknowledged, resolved
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    resolved_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_pagerduty_incidents_status ON pagerduty_incidents(status);
CREATE INDEX IF NOT EXISTS idx_pagerduty_incidents_contract ON pagerduty_incidents(contract_id);
CREATE INDEX IF NOT EXISTS idx_pagerduty_incidents_dedup_key ON pagerduty_incidents(dedup_key);