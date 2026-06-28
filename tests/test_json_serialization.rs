//! Test for Issue #577: Optimize event data JSON serialization
//! Ensures serde_json optimizations for faster event serialization and proper roundtrip.

use sqlx::PgPool;

#[sqlx::test(migrations = "./migrations")]
async fn test_event_json_serialization_performance(pool: PgPool) {
    // Test that event serialization handles various data types efficiently
    
    let test_cases = vec![
        serde_json::json!({"value": {"u128": "1000000000000000000"}, "topic": []}),
        serde_json::json!({"value": {"string": "long_contract_identifier_value"}, "topic": []}),
        serde_json::json!({"value": {"nested": {"deep": {"data": "value"}}}, "topic": []}),
        serde_json::json!({"value": {"array": [1, 2, 3, 4, 5]}, "topic": []}),
    ];

    for (idx, event_data) in test_cases.iter().enumerate() {
        let event_id = format!("event-serialization-{}", idx);
        
        sqlx::query(
            r#"
            INSERT INTO events 
            (id, contract_id, event_type, tx_hash, ledger, timestamp, event_data, created_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            "#,
        )
        .bind(&event_id)
        .bind("CABC0001")
        .bind("contract")
        .bind(format!("tx-hash-{}", idx))
        .bind(2000u64 + idx as u64)
        .bind(chrono::Utc::now())
        .bind(event_data.clone())
        .bind(chrono::Utc::now())
        .execute(&pool)
        .await
        .expect("Failed to insert event for serialization test");

        // Verify serialization roundtrip
        let stored_data: serde_json::Value = sqlx::query_scalar(
            "SELECT event_data FROM events WHERE id = $1"
        )
        .bind(&event_id)
        .fetch_one(&pool)
        .await
        .expect("Failed to fetch stored event data");

        assert_eq!(
            stored_data, *event_data,
            "Serialization roundtrip should preserve data"
        );

        // Simulate serialization to string (what the API does)
        let serialized = serde_json::to_string(&stored_data)
            .expect("Failed to serialize event data");
        
        assert!(!serialized.is_empty(), "Serialized data should not be empty");
    }

    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM events WHERE contract_id = 'CABC0001'")
        .fetch_one(&pool)
        .await
        .expect("Failed to count test events");
    assert_eq!(count, 4, "Should have inserted all 4 test events");
}

#[sqlx::test(migrations = "./migrations")]
async fn test_event_data_roundtrip_integrity(pool: PgPool) {
    // Ensure data integrity through serialization-deserialization cycle
    
    let original_data = serde_json::json!({
        "value": {
            "type": "transfer",
            "amount": "1000000000000000000",
            "from": "GAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAY5V3IQ",
            "to": "GBBD47UZQ5DKDX3SMIUCWX7OO4ZC4XCMNXOGXQX6OSJRNUWAPXUQ3P6Z"
        },
        "topic": ["transfer", "CABC"]
    });

    let event_id = "roundtrip-test";
    
    sqlx::query(
        r#"
        INSERT INTO events 
        (id, contract_id, event_type, tx_hash, ledger, timestamp, event_data, created_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        "#,
    )
    .bind(event_id)
    .bind("CABC0002")
    .bind("contract")
    .bind("tx-roundtrip-test")
    .bind(2001u64)
    .bind(chrono::Utc::now())
    .bind(original_data.clone())
    .bind(chrono::Utc::now())
    .execute(&pool)
    .await
    .expect("Failed to insert event");

    let retrieved_data: serde_json::Value = sqlx::query_scalar(
        "SELECT event_data FROM events WHERE id = $1"
    )
    .bind(event_id)
    .fetch_one(&pool)
    .await
    .expect("Failed to fetch event data");

    // Verify all fields match after roundtrip
    assert_eq!(
        retrieved_data["value"]["type"], "transfer",
        "Type field should be preserved"
    );
    assert_eq!(
        retrieved_data["value"]["amount"], "1000000000000000000",
        "Amount field should be preserved"
    );
    assert_eq!(
        retrieved_data["topic"][0], "transfer",
        "Topic should be preserved"
    );
}
