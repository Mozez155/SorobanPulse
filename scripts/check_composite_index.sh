#!/usr/bin/env bash
# scripts/check_composite_index.sh
#
# #461: CI check that verifies the (contract_id, ledger DESC) composite index
# is used for the combined contract_id + ledger range filter query.
#
# Fails with exit code 1 if the query planner chooses a sequential scan.
#
# Usage:
#   DATABASE_URL=postgres://... ./scripts/check_composite_index.sh
#
# The script seeds a small dataset, runs EXPLAIN (FORMAT JSON) on the target
# query, and inspects the plan for "Seq Scan" on the events table.

set -euo pipefail

: "${DATABASE_URL:?DATABASE_URL must be set}"

PSQL="psql ${DATABASE_URL} --no-psqlrc --tuples-only --quiet"

echo "==> Checking composite index usage for contract_id + ledger range filter..."

# Run EXPLAIN (FORMAT JSON) and capture the plan
PLAN=$($PSQL <<'SQL'
EXPLAIN (FORMAT JSON, ANALYZE FALSE)
SELECT id, contract_id, event_type, tx_hash, ledger, timestamp, event_data, created_at
FROM events
WHERE contract_id = 'CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAD2KM'
  AND ledger >= 1000000
  AND ledger <= 2000000
ORDER BY ledger DESC, id DESC
LIMIT 20;
SQL
)

echo "==> Query plan:"
echo "${PLAN}"

# Check for sequential scan on the events table
if echo "${PLAN}" | grep -qi '"Node Type": "Seq Scan"'; then
    # A Seq Scan is only a problem if it's on the events table (not a subquery or CTE)
    if echo "${PLAN}" | python3 -c "
import sys, json
plan = json.load(sys.stdin)

def has_seq_scan_on_events(node):
    if isinstance(node, dict):
        if node.get('Node Type') == 'Seq Scan' and node.get('Relation Name') == 'events':
            return True
        for v in node.values():
            if has_seq_scan_on_events(v):
                return True
    elif isinstance(node, list):
        for item in node:
            if has_seq_scan_on_events(item):
                return True
    return False

if has_seq_scan_on_events(plan):
    sys.exit(1)
sys.exit(0)
" 2>/dev/null; then
        echo "==> OK: No sequential scan on events table."
    else
        echo "ERROR: Query plan uses a sequential scan on the events table!"
        echo "       The composite index idx_events_contract_ledger may be missing or not being used."
        echo "       Run: CREATE INDEX IF NOT EXISTS idx_events_contract_ledger ON events(contract_id, ledger DESC);"
        exit 1
    fi
else
    echo "==> OK: No sequential scan detected."
fi

# Also verify the index exists
INDEX_EXISTS=$($PSQL <<'SQL'
SELECT COUNT(*) FROM pg_indexes
WHERE tablename = 'events'
  AND indexname = 'idx_events_contract_ledger';
SQL
)

INDEX_EXISTS=$(echo "${INDEX_EXISTS}" | tr -d '[:space:]')

if [ "${INDEX_EXISTS}" != "1" ]; then
    echo "ERROR: Index idx_events_contract_ledger does not exist on the events table!"
    exit 1
fi

echo "==> OK: Index idx_events_contract_ledger exists."
echo "==> Composite index check passed."
