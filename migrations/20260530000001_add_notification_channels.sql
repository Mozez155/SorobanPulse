-- Migration for Issue #474: Per-channel retry policies
CREATE TABLE IF NOT EXISTS notification_channels (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name TEXT NOT NULL UNIQUE,
    channel_type TEXT NOT NULL CHECK (channel_type IN ('webhook', 'email', 'sms')),
    config JSONB NOT NULL,
    retry_policy JSONB NOT NULL DEFAULT '{
        "max_attempts": 3,
        "initial_backoff_ms": 1000,
        "backoff_multiplier": 2.0,
        "max_backoff_ms": 60000
    }',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Add idempotency keys for email notifications
CREATE TABLE IF NOT EXISTS email_notifications (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    idempotency_key TEXT NOT NULL UNIQUE,
    recipient TEXT NOT NULL,
    subject TEXT NOT NULL,
    body TEXT NOT NULL,
    sent_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_email_notifications_idempotency ON email_notifications(idempotency_key);
CREATE INDEX IF NOT EXISTS idx_email_notifications_sent_at ON email_notifications(sent_at);