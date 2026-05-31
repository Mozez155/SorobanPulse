-- Migration for Issue #467: Custom event data aggregation
CREATE TABLE IF NOT EXISTS aggregation_rules (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name TEXT NOT NULL UNIQUE,
    description TEXT,
    group_by_path TEXT NOT NULL, -- JSONB path expression like '$.token'
    aggregation_function TEXT NOT NULL CHECK (aggregation_function IN ('count', 'sum', 'avg', 'min', 'max')),
    value_path TEXT, -- For sum/avg/min/max, the path to the numeric value
    filters JSONB, -- Additional filters to apply
    created_by TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_aggregation_rules_name ON aggregation_rules(name);
CREATE INDEX IF NOT EXISTS idx_aggregation_rules_created_at ON aggregation_rules(created_at DESC);