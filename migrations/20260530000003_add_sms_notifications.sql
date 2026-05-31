-- Migration for Issue #473: SMS notifications via Twilio
CREATE TABLE IF NOT EXISTS sms_notifications (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    phone_number TEXT NOT NULL,
    message TEXT NOT NULL,
    twilio_sid TEXT,
    status TEXT NOT NULL DEFAULT 'pending',
    error_message TEXT,
    sent_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_sms_notifications_phone ON sms_notifications(phone_number);
CREATE INDEX IF NOT EXISTS idx_sms_notifications_status ON sms_notifications(status);
CREATE INDEX IF NOT EXISTS idx_sms_notifications_created_at ON sms_notifications(created_at DESC);