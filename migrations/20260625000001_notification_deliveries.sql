-- Issue #475: track notification delivery receipts.
--
-- Records every notification delivery attempt so operators can audit whether a
-- notification was delivered, and demonstrate proof of delivery for compliance.
CREATE TABLE IF NOT EXISTS notification_deliveries (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    -- Channel that the notification was delivered through (webhook, email, sms, ...).
    channel_type TEXT NOT NULL,
    -- Optional reference to a configured channel in notification_channels.
    channel_config_id UUID,
    -- The event that triggered the notification. May be NULL when the event is
    -- not persisted (e.g. batched email summaries).
    event_id UUID,
    -- Delivery outcome: 'success' or 'failure'.
    status TEXT NOT NULL CHECK (status IN ('success', 'failure')),
    -- When the delivery attempt completed.
    delivered_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    -- Error detail when status = 'failure'.
    error TEXT
);

CREATE INDEX IF NOT EXISTS idx_notification_deliveries_event_id
    ON notification_deliveries (event_id);
CREATE INDEX IF NOT EXISTS idx_notification_deliveries_delivered_at
    ON notification_deliveries (delivered_at DESC);
CREATE INDEX IF NOT EXISTS idx_notification_deliveries_channel_type
    ON notification_deliveries (channel_type);
