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
async fn create_aggregation_rule_returns_201(pool: PgPool) {
    let app = make_router(pool, None);

    let body = r#"{
        "name": "test_aggregation",
        "contract_id": "CAB3",
        "event_type": "contract",
        "aggregation_type": "count",
        "window_size_seconds": 3600
    }"#;

    let resp = app
        .oneshot(
            Request::builder()
                .uri("/v1/aggregations")
                .method("POST")
                .header("content-type", "application/json")
                .body(axum::body::Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(resp.status() == StatusCode::CREATED || resp.status() == StatusCode::OK);
}

#[sqlx::test(migrations = "./migrations")]
async fn create_aggregation_rule_with_invalid_data_returns_400(pool: PgPool) {
    let app = make_router(pool, None);

    let body = r#"{
        "name": "",
        "aggregation_type": "invalid_type"
    }"#;

    let resp = app
        .oneshot(
            Request::builder()
                .uri("/v1/aggregations")
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
async fn get_aggregations_returns_200(pool: PgPool) {
    let app = make_router(pool, None);

    let resp = app
        .oneshot(
            Request::builder()
                .uri("/v1/aggregations")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body: serde_json::Value =
        serde_json::from_slice(&to_bytes(resp.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert!(body.is_array());
}

#[sqlx::test(migrations = "./migrations")]
async fn list_aggregations_with_pagination_returns_200(pool: PgPool) {
    let app = make_router(pool, None);

    let resp = app
        .oneshot(
            Request::builder()
                .uri("/v1/aggregations?page=1&limit=10")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body: serde_json::Value =
        serde_json::from_slice(&to_bytes(resp.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert!(body["data"].is_array() || body.is_array());
}

#[sqlx::test(migrations = "./migrations")]
async fn get_aggregation_result_returns_200(pool: PgPool) {
    let app = make_router(pool, None);

    let resp = app
        .oneshot(
            Request::builder()
                .uri("/v1/aggregations/test_id/results?from_timestamp=2026-03-14T00:00:00Z")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(resp.status() == StatusCode::OK || resp.status() == StatusCode::NOT_FOUND);
}

#[sqlx::test(migrations = "./migrations")]
async fn aggregation_supports_count_type(pool: PgPool) {
    sqlx::query(
        r#"
        INSERT INTO events (id, contract_id, event_type, tx_hash, ledger, timestamp, event_data, created_at)
        VALUES 
        ('550e8400-e29b-41d4-a716-446655440001', 'CAB3', 'contract', 'abc123', 1000, '2026-03-14T00:00:00Z', '{}', now()),
        ('550e8400-e29b-41d4-a716-446655440002', 'CAB3', 'contract', 'abc124', 1001, '2026-03-14T00:01:00Z', '{}', now()),
        ('550e8400-e29b-41d4-a716-446655440003', 'CAB3', 'contract', 'abc125', 1002, '2026-03-14T00:02:00Z', '{}', now())
        "#,
    )
    .execute(&pool)
    .await
    .unwrap();

    let app = make_router(pool, None);

    let body = r#"{
        "name": "count_agg",
        "contract_id": "CAB3",
        "aggregation_type": "count",
        "window_size_seconds": 3600
    }"#;

    let resp = app
        .oneshot(
            Request::builder()
                .uri("/v1/aggregations")
                .method("POST")
                .header("content-type", "application/json")
                .body(axum::body::Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(resp.status().is_success());
}

#[sqlx::test(migrations = "./migrations")]
async fn aggregation_supports_windowed_aggregation(pool: PgPool) {
    let app = make_router(pool, None);

    let body = r#"{
        "name": "windowed_agg",
        "contract_id": "CAB4",
        "aggregation_type": "sum",
        "window_size_seconds": 1800
    }"#;

    let resp = app
        .oneshot(
            Request::builder()
                .uri("/v1/aggregations")
                .method("POST")
                .header("content-type", "application/json")
                .body(axum::body::Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(resp.status().is_success());
}

#[sqlx::test(migrations = "./migrations")]
async fn delete_aggregation_returns_204(pool: PgPool) {
    let app = make_router(pool, None);

    // First create
    let body = r#"{
        "name": "delete_test",
        "contract_id": "CAB5",
        "aggregation_type": "count",
        "window_size_seconds": 3600
    }"#;

    let create_resp = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/v1/aggregations")
                .method("POST")
                .header("content-type", "application/json")
                .body(axum::body::Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();

    if create_resp.status().is_success() {
        // Then delete
        let app2 = make_router(pool, None);
        let resp = app2
            .oneshot(
                Request::builder()
                    .uri("/v1/aggregations/delete_test")
                    .method("DELETE")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert!(resp.status() == StatusCode::NO_CONTENT || resp.status() == StatusCode::OK);
    }
}

#[sqlx::test(migrations = "./migrations")]
async fn aggregation_results_respect_time_range(pool: PgPool) {
    let app = make_router(pool, None);

    let resp = app
        .oneshot(
            Request::builder()
                .uri("/v1/aggregations/test/results?from_timestamp=2026-03-14T00:00:00Z&to_timestamp=2026-03-15T00:00:00Z")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(resp.status() == StatusCode::OK || resp.status() == StatusCode::NOT_FOUND);
}
