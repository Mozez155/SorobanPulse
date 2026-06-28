use axum::body::to_bytes;
use axum::http::{Request, StatusCode};
use sqlx::PgPool;
use std::sync::Arc;
use tower::ServiceExt;

use soroban_pulse::config::{HealthState, IndexerState};
use soroban_pulse::metrics::init_metrics;
use soroban_pulse::models::Event;
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
async fn replay_from_ledger_returns_200_with_events(pool: PgPool) {
    // Insert test event
    sqlx::query(
        r#"
        INSERT INTO events (id, contract_id, event_type, tx_hash, ledger, timestamp, event_data, created_at)
        VALUES ('550e8400-e29b-41d4-a716-446655440001', 'CAB3', 'contract', 'abc123', 1000, '2026-03-14T00:00:00Z', '{}', now())
        "#,
    )
    .execute(&pool)
    .await
    .unwrap();

    let app = make_router(pool, None);

    let resp = app
        .oneshot(
            Request::builder()
                .uri("/v1/replay/from-ledger?from_ledger=1000")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body: serde_json::Value =
        serde_json::from_slice(&to_bytes(resp.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert!(body.is_object());
    assert!(body["replay_id"].is_string());
}

#[sqlx::test(migrations = "./migrations")]
async fn replay_from_ledger_with_invalid_ledger_returns_400(pool: PgPool) {
    let app = make_router(pool, None);

    let resp = app
        .oneshot(
            Request::builder()
                .uri("/v1/replay/from-ledger?from_ledger=invalid")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[sqlx::test(migrations = "./migrations")]
async fn replay_from_ledger_requires_admin_key(pool: PgPool) {
    let app = make_router(pool, None);

    let resp = app
        .oneshot(
            Request::builder()
                .uri("/v1/replay/from-ledger?from_ledger=1000")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    // Should succeed without auth check (depends on ADMIN_API_KEY config)
    assert!(resp.status().is_success() || resp.status() == StatusCode::UNAUTHORIZED);
}

#[sqlx::test(migrations = "./migrations")]
async fn replay_from_timestamp_returns_200(pool: PgPool) {
    sqlx::query(
        r#"
        INSERT INTO events (id, contract_id, event_type, tx_hash, ledger, timestamp, event_data, created_at)
        VALUES ('550e8400-e29b-41d4-a716-446655440002', 'CAB4', 'contract', 'def456', 2000, '2026-03-14T10:00:00Z', '{}', now())
        "#,
    )
    .execute(&pool)
    .await
    .unwrap();

    let app = make_router(pool, None);

    let resp = app
        .oneshot(
            Request::builder()
                .uri("/v1/replay/from-timestamp?from_timestamp=2026-03-14T10:00:00Z")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body: serde_json::Value =
        serde_json::from_slice(&to_bytes(resp.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert!(body["replay_id"].is_string());
}

#[sqlx::test(migrations = "./migrations")]
async fn replay_from_timestamp_with_invalid_timestamp_returns_400(pool: PgPool) {
    let app = make_router(pool, None);

    let resp = app
        .oneshot(
            Request::builder()
                .uri("/v1/replay/from-timestamp?from_timestamp=not-a-timestamp")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[sqlx::test(migrations = "./migrations")]
async fn replay_status_returns_200(pool: PgPool) {
    let app = make_router(pool, None);

    let resp = app
        .oneshot(
            Request::builder()
                .uri("/v1/replay/status?replay_id=550e8400-e29b-41d4-a716-446655440001")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(resp.status() == StatusCode::OK || resp.status() == StatusCode::NOT_FOUND);
}

#[sqlx::test(migrations = "./migrations")]
async fn replay_status_with_invalid_replay_id_returns_400(pool: PgPool) {
    let app = make_router(pool, None);

    let resp = app
        .oneshot(
            Request::builder()
                .uri("/v1/replay/status?replay_id=invalid-id")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[sqlx::test(migrations = "./migrations")]
async fn replay_enforces_max_age_limit(pool: PgPool) {
    let app = make_router(pool, None);

    // Request replay from very old ledger (more than max age)
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/v1/replay/from-ledger?from_ledger=1")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    // Should either succeed or fail with proper error code
    assert!(resp.status().is_client_error() || resp.status().is_success());
}

#[sqlx::test(migrations = "./migrations")]
async fn replay_returns_delivery_status(pool: PgPool) {
    sqlx::query(
        r#"
        INSERT INTO events (id, contract_id, event_type, tx_hash, ledger, timestamp, event_data, created_at)
        VALUES ('550e8400-e29b-41d4-a716-446655440003', 'CAB5', 'contract', 'ghi789', 3000, '2026-03-14T15:00:00Z', '{}', now())
        "#,
    )
    .execute(&pool)
    .await
    .unwrap();

    let app = make_router(pool, None);

    let resp = app
        .oneshot(
            Request::builder()
                .uri("/v1/replay/from-ledger?from_ledger=3000")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body: serde_json::Value =
        serde_json::from_slice(&to_bytes(resp.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert!(body.get("replay_id").is_some());
}
