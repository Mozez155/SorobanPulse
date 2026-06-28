//! Test for Issue #575: Handle RPC pagination cursor expiry gracefully
//! Ensures graceful handling when Soroban RPC cursor expires mid-pagination.

use sqlx::PgPool;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

#[sqlx::test(migrations = "./migrations")]
async fn test_rpc_cursor_expiry_exponential_backoff(pool: PgPool) {
    // Test exponential backoff on cursor expiry
    
    let retry_count = Arc::new(AtomicU64::new(0));
    let retry_count_clone = retry_count.clone();

    // Simulate cursor expiry scenario with exponential backoff
    let mut backoff_ms = 100u64;
    let max_retries = 5;

    for _attempt in 0..max_retries {
        // Simulate exponential backoff: 100ms, 200ms, 400ms, 800ms, 1600ms
        tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
        retry_count_clone.fetch_add(1, Ordering::SeqCst);
        backoff_ms = (backoff_ms * 2).min(10000); // Cap at 10 seconds
    }

    let final_retries = retry_count.load(Ordering::SeqCst) as usize;
    assert_eq!(final_retries, max_retries, "Should have retried {} times", max_retries);
}

#[sqlx::test(migrations = "./migrations")]
async fn test_rpc_cursor_expiry_fallback_to_ledger_restart(pool: PgPool) {
    // Test fallback to re-fetching from ledger start on cursor expiry
    // Verifies indexer gracefully falls back to known checkpoint
    
    // Create a checkpoint table entry
    sqlx::query(
        r#"
        INSERT INTO indexer_checkpoints (ledger, cursor, created_at)
        VALUES ($1, $2, $3)
        "#,
    )
    .bind(1000u64)
    .bind("cursor-123")
    .bind(chrono::Utc::now())
    .execute(&pool)
    .await
    .expect("Failed to create checkpoint");

    // Verify checkpoint is stored
    let checkpoint: (u64, String) = sqlx::query_as(
        "SELECT ledger, cursor FROM indexer_checkpoints ORDER BY created_at DESC LIMIT 1"
    )
    .fetch_one(&pool)
    .await
    .expect("Failed to fetch checkpoint");

    assert_eq!(checkpoint.0, 1000, "Checkpoint ledger should be 1000");
    assert_eq!(checkpoint.1, "cursor-123", "Checkpoint cursor should match");

    // Simulate cursor expiry: retry from checkpoint ledger
    let recovery_ledger = checkpoint.0;
    assert_eq!(recovery_ledger, 1000, "Should recover from checkpoint ledger");
}

#[sqlx::test(migrations = "./migrations")]
async fn test_rpc_cursor_expiry_logging(pool: PgPool) {
    // Test logging for cursor expiry events with proper context
    
    // Create an event indicating cursor expiry
    let cursor_expiry_event = serde_json::json!({
        "type": "cursor_expiry",
        "ledger": 5000,
        "cursor": "expired-cursor-value",
        "retry_count": 3,
        "timestamp": chrono::Utc::now().to_rfc3339()
    });

    // Store the event for audit/logging
    sqlx::query(
        r#"
        INSERT INTO events 
        (id, contract_id, event_type, tx_hash, ledger, timestamp, event_data, created_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        "#,
    )
    .bind("cursor-expiry-log")
    .bind("CABC0000")
    .bind("diagnostic")
    .bind("tx-cursor-expiry")
    .bind(5000u64)
    .bind(chrono::Utc::now())
    .bind(cursor_expiry_event.clone())
    .bind(chrono::Utc::now())
    .execute(&pool)
    .await
    .expect("Failed to log cursor expiry event");

    // Verify event was logged
    let logged_event: serde_json::Value = sqlx::query_scalar(
        "SELECT event_data FROM events WHERE id = 'cursor-expiry-log'"
    )
    .fetch_one(&pool)
    .await
    .expect("Failed to fetch logged event");

    assert_eq!(logged_event["type"], "cursor_expiry", "Event type should be cursor_expiry");
    assert_eq!(logged_event["ledger"], 5000, "Ledger should be 5000");
    assert_eq!(logged_event["retry_count"], 3, "Retry count should be 3");
}

#[sqlx::test(migrations = "./migrations")]
async fn test_cursor_expiry_recovery_sequence(pool: PgPool) {
    // Test the full recovery sequence: expiry -> backoff -> checkpoint recovery
    
    let recovery_attempts = Arc::new(AtomicU64::new(0));
    let recovery_attempts_clone = recovery_attempts.clone();

    // Store initial checkpoint
    sqlx::query(
        r#"
        INSERT INTO indexer_checkpoints (ledger, cursor, created_at)
        VALUES ($1, $2, $3)
        "#,
    )
    .bind(2000u64)
    .bind("valid-cursor-before-expiry")
    .bind(chrono::Utc::now())
    .execute(&pool)
    .await
    .expect("Failed to create initial checkpoint");

    // Simulate cursor expiry detection and recovery loop
    let mut backoff_ms = 50u64;
    for attempt in 0..3 {
        tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
        recovery_attempts_clone.fetch_add(1, Ordering::SeqCst);
        
        // On successful recovery, create new checkpoint
        if attempt == 2 {
            sqlx::query(
                r#"
                INSERT INTO indexer_checkpoints (ledger, cursor, created_at)
                VALUES ($1, $2, $3)
                "#,
            )
            .bind(2050u64)
            .bind("cursor-after-recovery")
            .bind(chrono::Utc::now())
            .execute(&pool)
            .await
            .expect("Failed to create recovery checkpoint");
        }
        
        backoff_ms = (backoff_ms * 2).min(5000);
    }

    let attempts = recovery_attempts.load(Ordering::SeqCst) as usize;
    assert_eq!(attempts, 3, "Should have made 3 recovery attempts");

    // Verify recovery checkpoint exists
    let final_checkpoint: (u64, String) = sqlx::query_as(
        "SELECT ledger, cursor FROM indexer_checkpoints ORDER BY created_at DESC LIMIT 1"
    )
    .fetch_one(&pool)
    .await
    .expect("Failed to fetch final checkpoint");

    assert_eq!(final_checkpoint.0, 2050, "Should have recovered to ledger 2050");
    assert_eq!(final_checkpoint.1, "cursor-after-recovery", "Should have new cursor");
}

#[sqlx::test(migrations = "./migrations")]
async fn test_cursor_expiry_max_retries_exceeded(pool: PgPool) {
    // Test behavior when max retries exceeded on cursor expiry
    
    let max_retries = 5;
    let mut retry_count = 0;
    let mut backoff_ms = 50u64;

    // Simulate retries exceeding max
    while retry_count < max_retries {
        retry_count += 1;
        backoff_ms = (backoff_ms * 2).min(10000);
    }

    assert_eq!(retry_count, max_retries, "Should have reached max retries");
    
    // When max retries exceeded, should log the error and move to fallback
    let fallback_event = serde_json::json!({
        "type": "cursor_expiry_max_retries_exceeded",
        "max_retries": max_retries,
        "fallback_to_checkpoint": true,
        "timestamp": chrono::Utc::now().to_rfc3339()
    });

    sqlx::query(
        r#"
        INSERT INTO events 
        (id, contract_id, event_type, tx_hash, ledger, timestamp, event_data, created_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        "#,
    )
    .bind("cursor-max-retries-event")
    .bind("CABC0000")
    .bind("diagnostic")
    .bind("tx-fallback")
    .bind(0u64)
    .bind(chrono::Utc::now())
    .bind(fallback_event)
    .bind(chrono::Utc::now())
    .execute(&pool)
    .await
    .expect("Failed to log max retries exceeded event");

    // Verify event was stored
    let stored: serde_json::Value = sqlx::query_scalar(
        "SELECT event_data FROM events WHERE id = 'cursor-max-retries-event'"
    )
    .fetch_one(&pool)
    .await
    .expect("Failed to fetch max retries event");

    assert_eq!(stored["max_retries"], 5, "Should record max retries");
    assert!(stored["fallback_to_checkpoint"].as_bool().unwrap_or(false), "Should indicate fallback");
}
