use axum::body::to_bytes;
use axum::http::{Request, StatusCode};
use sqlx::PgPool;
use std::sync::Arc;
use tower::ServiceExt;

use soroban_pulse::config::{HealthState, IndexerState};
use soroban_pulse::metrics::init_metrics;
use soroban_pulse::routes::create_router;

fn make_router(pool: PgPool, api_key: Option<String>) -> axum::Router {
    let health_state = Arc::new(HealthState::new(60));
    health_state.update_last_poll();
    let indexer_state = Arc::new(IndexerState::new());
    let prometheus_handle = init_metrics();
    let api_keys = api_key.into_iter().collect();
    let config = soroban_pulse::config::Config::default();
    create_router(
        pool,
        api_keys,
        &[],
        60,
        health_state,
        indexer_state,
        prometheus_handle,
        15000,
        config,
    )
}

#[sqlx::test(migrations = "./migrations")]
async fn replay_with_transform_returns_200(pool: PgPool) {
    sqlx::query(
        r#"
        INSERT INTO events (id, contract_id, event_type, tx_hash, ledger, timestamp, event_data, created_at)
        VALUES ('550e8400-e29b-41d4-a716-446655440001', 'CAB3', 'contract', 'abc123', 1000, '2026-03-14T00:00:00Z', '{"value": {}, "topic": []}', now())
        "#,
    )
    .execute(&pool)
    .await
    .unwrap();

    let app = make_router(pool, None);

    let body = r#"{
        "from_ledger": 1000,
        "transformation_script": "return event"
    }"#;

    let resp = app
        .oneshot(
            Request::builder()
                .uri("/v1/replay/with-transform")
                .method("POST")
                .header("content-type", "application/json")
                .body(axum::body::Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
}

#[sqlx::test(migrations = "./migrations")]
async fn replay_with_transform_validates_script(pool: PgPool) {
    let app = make_router(pool, None);

    let body = r#"{
        "from_ledger": 1000,
        "transformation_script": "invalid lua syntax @#$%"
    }"#;

    let resp = app
        .oneshot(
            Request::builder()
                .uri("/v1/replay/with-transform")
                .method("POST")
                .header("content-type", "application/json")
                .body(axum::body::Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[sqlx::test(migrations = "./migrations")]
async fn replay_transform_dry_run_does_not_deliver(pool: PgPool) {
    sqlx::query(
        r#"
        INSERT INTO events (id, contract_id, event_type, tx_hash, ledger, timestamp, event_data, created_at)
        VALUES ('550e8400-e29b-41d4-a716-446655440002', 'CAB4', 'contract', 'def456', 2000, '2026-03-14T10:00:00Z', '{"value": {}, "topic": []}', now())
        "#,
    )
    .execute(&pool)
    .await
    .unwrap();

    let app = make_router(pool, None);

    let body = r#"{
        "from_ledger": 2000,
        "transformation_script": "return event",
        "dry_run": true
    }"#;

    let resp = app
        .oneshot(
            Request::builder()
                .uri("/v1/replay/with-transform")
                .method("POST")
                .header("content-type", "application/json")
                .body(axum::body::Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body: serde_json::Value =
        serde_json::from_slice(&to_bytes(resp.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(body.get("dry_run").and_then(|v| v.as_bool()), Some(true));
}

#[sqlx::test(migrations = "./migrations")]
async fn replay_transform_returns_preview_on_dry_run(pool: PgPool) {
    sqlx::query(
        r#"
        INSERT INTO events (id, contract_id, event_type, tx_hash, ledger, timestamp, event_data, created_at)
        VALUES ('550e8400-e29b-41d4-a716-446655440003', 'CAB5', 'contract', 'ghi789', 3000, '2026-03-14T15:00:00Z', '{"value": {}, "topic": []}', now())
        "#,
    )
    .execute(&pool)
    .await
    .unwrap();

    let app = make_router(pool, None);

    let body = r#"{
        "from_ledger": 3000,
        "transformation_script": "return event",
        "dry_run": true
    }"#;

    let resp = app
        .oneshot(
            Request::builder()
                .uri("/v1/replay/with-transform")
                .method("POST")
                .header("content-type", "application/json")
                .body(axum::body::Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body: serde_json::Value =
        serde_json::from_slice(&to_bytes(resp.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert!(body.get("preview").is_some() || body.get("sample_results").is_some());
}

#[sqlx::test(migrations = "./migrations")]
async fn replay_transform_with_timestamp_filter(pool: PgPool) {
    sqlx::query(
        r#"
        INSERT INTO events (id, contract_id, event_type, tx_hash, ledger, timestamp, event_data, created_at)
        VALUES ('550e8400-e29b-41d4-a716-446655440004', 'CAB6', 'contract', 'jkl012', 4000, '2026-03-14T20:00:00Z', '{"value": {}, "topic": []}', now())
        "#,
    )
    .execute(&pool)
    .await
    .unwrap();

    let app = make_router(pool, None);

    let body = r#"{
        "from_timestamp": "2026-03-14T20:00:00Z",
        "to_timestamp": "2026-03-15T00:00:00Z",
        "transformation_script": "return event"
    }"#;

    let resp = app
        .oneshot(
            Request::builder()
                .uri("/v1/replay/with-transform")
                .method("POST")
                .header("content-type", "application/json")
                .body(axum::body::Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
}

#[sqlx::test(migrations = "./migrations")]
async fn replay_transform_returns_transformation_status(pool: PgPool) {
    let app = make_router(pool, None);

    let body = r#"{
        "from_ledger": 1000,
        "transformation_script": "return event"
    }"#;

    let resp = app
        .oneshot(
            Request::builder()
                .uri("/v1/replay/with-transform")
                .method("POST")
                .header("content-type", "application/json")
                .body(axum::body::Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body: serde_json::Value =
        serde_json::from_slice(&to_bytes(resp.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert!(body.get("replay_id").is_some());
    assert!(body.get("status").is_some() || body.get("transformation_id").is_some());
}

#[sqlx::test(migrations = "./migrations")]
async fn replay_transform_missing_required_fields_returns_400(pool: PgPool) {
    let app = make_router(pool, None);

    let body = r#"{
        "transformation_script": "return event"
    }"#;

    let resp = app
        .oneshot(
            Request::builder()
                .uri("/v1/replay/with-transform")
                .method("POST")
                .header("content-type", "application/json")
                .body(axum::body::Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[sqlx::test(migrations = "./migrations")]
async fn replay_transform_filter_by_contract(pool: PgPool) {
    let app = make_router(pool, None);

    let body = r#"{
        "from_ledger": 5000,
        "contract_id": "CABC...",
        "transformation_script": "return event"
    }"#;

    let resp = app
        .oneshot(
            Request::builder()
                .uri("/v1/replay/with-transform")
                .method("POST")
                .header("content-type", "application/json")
                .body(axum::body::Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
}
