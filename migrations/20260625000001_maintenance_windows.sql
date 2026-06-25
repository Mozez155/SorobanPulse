CREATE TABLE IF NOT EXISTS maintenance_windows (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    start_time TIMESTAMPTZ NOT NULL,
    end_time TIMESTAMPTZ NOT NULL,
    contract_ids TEXT[] NOT NULL DEFAULT '{}',
    description TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_window_order CHECK (end_time > start_time)
);

CREATE INDEX IF NOT EXISTS idx_maintenance_windows_active
    ON maintenance_windows (start_time, end_time)
    WHERE end_time > NOW();
