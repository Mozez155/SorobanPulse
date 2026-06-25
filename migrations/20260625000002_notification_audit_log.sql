CREATE TABLE IF NOT EXISTS notification_audit_log (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    channel_type TEXT NOT NULL,
    recipient TEXT NOT NULL,
    event_id TEXT,
    triggered_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    delivered_at TIMESTAMPTZ,
    status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'delivered', 'failed')),
    error TEXT
);

CREATE INDEX IF NOT EXISTS idx_notification_audit_log_triggered_at ON notification_audit_log (triggered_at);
CREATE INDEX IF NOT EXISTS idx_notification_audit_log_channel_type ON notification_audit_log (channel_type);
CREATE INDEX IF NOT EXISTS idx_notification_audit_log_status ON notification_audit_log (status);
