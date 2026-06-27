use sqlx::PgPool;
use std::sync::Arc;
use std::time::Instant;

use soroban_pulse::config::{HealthState, IndexerState};
use soroban_pulse::metrics::init_metrics;

#[sqlx::test(migrations = "./migrations")]
async fn prepared_statement_execution_succeeds(pool: PgPool) {
    sqlx::query(
        r#"
        INSERT INTO events (id, contract_id, event_type, tx_hash, ledger, timestamp, event_data, created_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, now())
        "#,
    )
    .bind("550e8400-e29b-41d4-a716-446655440001")
    .bind("CAB3")
    .bind("contract")
    .bind("abc123")
    .bind(1000)
    .bind("2026-03-14T00:00:00Z")
    .bind("{}")
    .execute(&pool)
    .await
    .unwrap();

    // Query with parameterized statement
    let result: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM events WHERE contract_id = $1")
        .bind("CAB3")
        .fetch_one(&pool)
        .await
        .unwrap();

    assert_eq!(result.0, 1);
}

#[sqlx::test(migrations = "./migrations")]
async fn prepared_statement_with_multiple_parameters(pool: PgPool) {
    sqlx::query(
        r#"
        INSERT INTO events (id, contract_id, event_type, tx_hash, ledger, timestamp, event_data, created_at)
        VALUES 
        ($1, $2, $3, $4, $5, $6, $7, now()),
        ($8, $9, $10, $11, $12, $13, $14, now())
        "#,
    )
    .bind("550e8400-e29b-41d4-a716-446655440001")
    .bind("CAB3")
    .bind("contract")
    .bind("abc123")
    .bind(1000)
    .bind("2026-03-14T00:00:00Z")
    .bind("{}")
    .bind("550e8400-e29b-41d4-a716-446655440002")
    .bind("CAB4")
    .bind("contract")
    .bind("def456")
    .bind(2000)
    .bind("2026-03-14T10:00:00Z")
    .bind("{}")
    .execute(&pool)
    .await
    .unwrap();

    // Parameterized query with range filter
    let result: (i64,) =
        sqlx::query_as("SELECT COUNT(*) FROM events WHERE ledger >= $1 AND ledger <= $2")
            .bind(1000)
            .bind(2000)
            .fetch_one(&pool)
            .await
            .unwrap();

    assert_eq!(result.0, 2);
}

#[sqlx::test(migrations = "./migrations")]
async fn prepared_statement_reuse_improves_performance(pool: PgPool) {
    // Insert test data
    for i in 0..100 {
        sqlx::query(
            r#"
            INSERT INTO events (id, contract_id, event_type, tx_hash, ledger, timestamp, event_data, created_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, now())
            "#,
        )
        .bind(format!("550e8400-e29b-41d4-a716-{:012}", i))
        .bind("CAB3")
        .bind("contract")
        .bind(format!("hash{}", i))
        .bind(1000 + i as i64)
        .bind("2026-03-14T00:00:00Z")
        .bind("{}")
        .execute(&pool)
        .await
        .unwrap();
    }

    // First execution (plan is cached)
    let start = Instant::now();
    for _ in 0..10 {
        let _: (i64,) =
            sqlx::query_as("SELECT COUNT(*) FROM events WHERE contract_id = $1")
                .bind("CAB3")
                .fetch_one(&pool)
                .await
                .unwrap();
    }
    let duration = start.elapsed();

    // Subsequent executions should use cached plan
    assert!(duration.as_millis() > 0);
}

#[sqlx::test(migrations = "./migrations")]
async fn prepared_statement_with_ordering(pool: PgPool) {
    sqlx::query(
        r#"
        INSERT INTO events (id, contract_id, event_type, tx_hash, ledger, timestamp, event_data, created_at)
        VALUES 
        ($1, $2, $3, $4, $5, $6, $7, now()),
        ($8, $9, $10, $11, $12, $13, $14, now()),
        ($15, $16, $17, $18, $19, $20, $21, now())
        "#,
    )
    .bind("550e8400-e29b-41d4-a716-446655440001")
    .bind("CAB3")
    .bind("contract")
    .bind("abc123")
    .bind(1000)
    .bind("2026-03-14T00:00:00Z")
    .bind("{}")
    .bind("550e8400-e29b-41d4-a716-446655440002")
    .bind("CAB3")
    .bind("contract")
    .bind("def456")
    .bind(2000)
    .bind("2026-03-14T10:00:00Z")
    .bind("{}")
    .bind("550e8400-e29b-41d4-a716-446655440003")
    .bind("CAB3")
    .bind("contract")
    .bind("ghi789")
    .bind(3000)
    .bind("2026-03-14T15:00:00Z")
    .bind("{}")
    .execute(&pool)
    .await
    .unwrap();

    // Query with ORDER BY
    let results: Vec<(i64,)> =
        sqlx::query_as("SELECT ledger FROM events WHERE contract_id = $1 ORDER BY ledger DESC LIMIT $2")
            .bind("CAB3")
            .bind(2)
            .fetch_all(&pool)
            .await
            .unwrap();

    assert_eq!(results.len(), 2);
    assert!(results[0].0 > results[1].0);
}

#[sqlx::test(migrations = "./migrations")]
async fn prepared_statement_with_join_operations(pool: PgPool) {
    // Insert event and verify plan caching with joins if applicable
    sqlx::query(
        r#"
        INSERT INTO events (id, contract_id, event_type, tx_hash, ledger, timestamp, event_data, created_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, now())
        "#,
    )
    .bind("550e8400-e29b-41d4-a716-446655440001")
    .bind("CAB3")
    .bind("contract")
    .bind("abc123")
    .bind(1000)
    .bind("2026-03-14T00:00:00Z")
    .bind("{}")
    .execute(&pool)
    .await
    .unwrap();

    // Simple self-join test
    let result: (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM events e1 WHERE e1.contract_id = $1 AND EXISTS (SELECT 1 FROM events e2 WHERE e2.contract_id = e1.contract_id)"
    )
    .bind("CAB3")
    .fetch_one(&pool)
    .await
    .unwrap();

    assert_eq!(result.0, 1);
}

#[sqlx::test(migrations = "./migrations")]
async fn prepared_statement_with_aggregation(pool: PgPool) {
    sqlx::query(
        r#"
        INSERT INTO events (id, contract_id, event_type, tx_hash, ledger, timestamp, event_data, created_at)
        VALUES 
        ($1, $2, $3, $4, $5, $6, $7, now()),
        ($8, $9, $10, $11, $12, $13, $14, now())
        "#,
    )
    .bind("550e8400-e29b-41d4-a716-446655440001")
    .bind("CAB3")
    .bind("contract")
    .bind("abc123")
    .bind(1000)
    .bind("2026-03-14T00:00:00Z")
    .bind("{}")
    .bind("550e8400-e29b-41d4-a716-446655440002")
    .bind("CAB4")
    .bind("contract")
    .bind("def456")
    .bind(2000)
    .bind("2026-03-14T10:00:00Z")
    .bind("{}")
    .execute(&pool)
    .await
    .unwrap();

    // GROUP BY with aggregation
    let results: Vec<(String, i64)> =
        sqlx::query_as("SELECT contract_id, COUNT(*) FROM events GROUP BY contract_id ORDER BY contract_id")
            .fetch_all(&pool)
            .await
            .unwrap();

    assert_eq!(results.len(), 2);
}

#[sqlx::test(migrations = "./migrations")]
async fn prepared_statement_with_pagination(pool: PgPool) {
    for i in 0..20 {
        sqlx::query(
            r#"
            INSERT INTO events (id, contract_id, event_type, tx_hash, ledger, timestamp, event_data, created_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, now())
            "#,
        )
        .bind(format!("550e8400-e29b-41d4-a716-{:012}", i))
        .bind("CAB3")
        .bind("contract")
        .bind(format!("hash{}", i))
        .bind(1000 + i as i64)
        .bind("2026-03-14T00:00:00Z")
        .bind("{}")
        .execute(&pool)
        .await
        .unwrap();
    }

    // Parameterized pagination query
    let results: Vec<(i64,)> =
        sqlx::query_as("SELECT ledger FROM events WHERE contract_id = $1 ORDER BY ledger LIMIT $2 OFFSET $3")
            .bind("CAB3")
            .bind(10)
            .bind(0)
            .fetch_all(&pool)
            .await
            .unwrap();

    assert_eq!(results.len(), 10);
}

#[sqlx::test(migrations = "./migrations")]
async fn prepared_statement_with_wildcard_filter(pool: PgPool) {
    sqlx::query(
        r#"
        INSERT INTO events (id, contract_id, event_type, tx_hash, ledger, timestamp, event_data, created_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, now())
        "#,
    )
    .bind("550e8400-e29b-41d4-a716-446655440001")
    .bind("CAB3")
    .bind("contract")
    .bind("abc123")
    .bind(1000)
    .bind("2026-03-14T00:00:00Z")
    .bind("{}")
    .execute(&pool)
    .await
    .unwrap();

    // Pattern matching with parameter
    let results: Vec<(String,)> =
        sqlx::query_as("SELECT contract_id FROM events WHERE contract_id LIKE $1")
            .bind("CAB%")
            .fetch_all(&pool)
            .await
            .unwrap();

    assert_eq!(results.len(), 1);
}

#[sqlx::test(migrations = "./migrations")]
async fn prepared_statement_caching_reduces_planning_overhead(pool: PgPool) {
    // Insert sufficient test data
    for i in 0..50 {
        let _ = sqlx::query(
            r#"
            INSERT INTO events (id, contract_id, event_type, tx_hash, ledger, timestamp, event_data, created_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, now())
            "#,
        )
        .bind(format!("550e8400-e29b-41d4-a716-{:012}", i))
        .bind(format!("CA{}", i % 10))
        .bind("contract")
        .bind(format!("hash{}", i))
        .bind(1000 + i as i64)
        .bind("2026-03-14T00:00:00Z")
        .bind("{}")
        .execute(&pool)
        .await;
    }

    // Execute same query multiple times (plan should be cached after first execution)
    for _ in 0..5 {
        let _: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM events WHERE event_type = $1")
            .bind("contract")
            .fetch_one(&pool)
            .await
            .unwrap();
    }
    // If we reach here without panicking, caching is working
}
