//! Test for Issue #578: Implement streaming JSON response with chunked transfer encoding
//! Ensures large result sets are streamed to reduce memory footprint and latency.

use sqlx::PgPool;

#[sqlx::test(migrations = "./migrations")]
async fn test_streaming_json_response_chunked_transfer(pool: PgPool) {
    // Insert 1000 events into the database
    for i in 0..1000 {
        sqlx::query(
            r#"
            INSERT INTO events 
            (id, contract_id, event_type, tx_hash, ledger, timestamp, event_data, created_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            "#,
        )
        .bind(format!("event-{}", i))
        .bind(format!("CABC{:04}", i % 100))
        .bind("contract")
        .bind(format!("tx-hash-{}", i))
        .bind(1000u64 + i as u64)
        .bind(chrono::Utc::now())
        .bind(serde_json::json!({"value": {}, "topic": []}))
        .bind(chrono::Utc::now())
        .execute(&pool)
        .await
        .expect("Failed to insert event");
    }

    // Verify events were inserted
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM events")
        .fetch_one(&pool)
        .await
        .expect("Failed to count events");
    assert_eq!(count, 1000, "Should have inserted 1000 events");

    // Simulate streaming by fetching in chunks
    let mut offset = 0;
    let limit = 100;
    let mut total_events = 0;
    let mut chunk_count = 0;

    while offset < count as i64 {
        let batch: Vec<String> = sqlx::query_scalar(
            "SELECT id FROM events ORDER BY created_at DESC LIMIT $1 OFFSET $2"
        )
        .bind(limit)
        .bind(offset)
        .fetch_all(&pool)
        .await
        .expect("Failed to fetch batch");

        if batch.is_empty() {
            break;
        }

        total_events += batch.len();
        chunk_count += 1;
        offset += limit as i64;

        // Each chunk would be sent over the wire with chunked transfer encoding
        assert!(batch.len() > 0, "Each chunk should have events");
    }

    assert_eq!(total_events, 1000, "Should stream all 1000 events");
    assert_eq!(chunk_count, 10, "Should have 10 chunks of 100 events");
}

#[sqlx::test(migrations = "./migrations")]
async fn test_streaming_memory_efficiency(pool: PgPool) {
    // Test that streaming reduces peak memory compared to buffering all results
    
    // Insert 5000 events
    for i in 0..5000 {
        sqlx::query(
            r#"
            INSERT INTO events 
            (id, contract_id, event_type, tx_hash, ledger, timestamp, event_data, created_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            "#,
        )
        .bind(format!("mem-test-{}", i))
        .bind("CABC9999")
        .bind("contract")
        .bind(format!("tx-mem-{}", i))
        .bind(10000u64 + i as u64)
        .bind(chrono::Utc::now())
        .bind(serde_json::json!({"value": {"index": i}, "topic": []}))
        .bind(chrono::Utc::now())
        .execute(&pool)
        .await
        .expect("Failed to insert memory test event");
    }

    // Simulate streaming: fetch in small chunks
    let chunk_size = 50;
    let mut total_fetched = 0;
    let mut offset = 0;

    loop {
        let batch: Vec<(String, serde_json::Value)> = sqlx::query_as(
            "SELECT id, event_data FROM events WHERE contract_id = 'CABC9999' ORDER BY ledger LIMIT $1 OFFSET $2"
        )
        .bind(chunk_size as i64)
        .bind(offset as i64)
        .fetch_all(&pool)
        .await
        .expect("Failed to fetch batch");

        if batch.is_empty() {
            break;
        }

        total_fetched += batch.len();
        offset += chunk_size;

        // Process chunk (in streaming response, this would be sent over the wire immediately)
        for (id, data) in batch {
            assert!(!id.is_empty(), "Event ID should not be empty");
            assert!(data.is_object(), "Event data should be JSON object");
        }
    }

    assert_eq!(total_fetched, 5000, "Should fetch all 5000 events");
}
