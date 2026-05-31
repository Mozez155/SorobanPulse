-- Migration for Issue #468: Saved queries functionality
CREATE TABLE IF NOT EXISTS saved_queries (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name TEXT NOT NULL,
    description TEXT,
    query_params JSONB NOT NULL,
    created_by TEXT, -- API key or user identifier
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_executed_at TIMESTAMPTZ,
    execution_count BIGINT NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_saved_queries_name ON saved_queries(name);
CREATE INDEX IF NOT EXISTS idx_saved_queries_created_by ON saved_queries(created_by);
CREATE INDEX IF NOT EXISTS idx_saved_queries_created_at ON saved_queries(created_at DESC);