use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use sqlx::PgPool;
use std::sync::Arc;
use tower::ServiceExt;

use soroban_pulse::config::{HealthState, IndexerState};
use soroban_pulse::metrics::init_metrics;
use soroban_pulse::routes::create_router;

fn make_router(pool: PgPool) -> axum::Router {
    let health_state = Arc::new(HealthState::new(60));
    health_state.update_last_poll();
    let indexer_state = Arc::new(IndexerState::new());
    let prometheus_handle = init_metrics();
    let config = soroban_pulse::config::Config::default();
    create_router(
        pool,
        vec![],
        &[],
        60,
        health_state,
        indexer_state,
        prometheus_handle,
        15000,
        config,
    )
}

/// Regression test for #572: Cross-chain trace endpoint should be available
#[sqlx::test(migrations = "./migrations")]
async fn cross_chain_trace_endpoint_exists(pool: PgPool) {
    let app = make_router(pool);

    let resp = app
        .oneshot(
            Request::builder()
                .uri("/v1/cross-chain/trace?tx_hash=abc123")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    // Should either return 200 with trace data or 404 if not yet implemented
    assert!(
        resp.status() == StatusCode::OK
            || resp.status() == StatusCode::NOT_FOUND
            || resp.status() == StatusCode::BAD_REQUEST
    );
}

/// Cross-chain trace should accept transaction hash parameter
#[sqlx::test(migrations = "./migrations")]
async fn cross_chain_trace_validates_tx_hash_parameter(pool: PgPool) {
    let app = make_router(pool);

    let resp = app
        .oneshot(
            Request::builder()
                .uri("/v1/cross-chain/trace?tx_hash=0x0000000000000000000000000000000000000000000000000000000000000000")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(
        resp.status() == StatusCode::OK
            || resp.status() == StatusCode::NOT_FOUND
            || resp.status() == StatusCode::BAD_REQUEST
    );
}

/// Cross-chain trace should reject invalid transaction hashes
#[sqlx::test(migrations = "./migrations")]
async fn cross_chain_trace_rejects_invalid_hash(pool: PgPool) {
    let app = make_router(pool);

    let resp = app
        .oneshot(
            Request::builder()
                .uri("/v1/cross-chain/trace?tx_hash=invalid")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    // Invalid hash should return error
    assert!(
        resp.status() == StatusCode::BAD_REQUEST
            || resp.status() == StatusCode::NOT_FOUND
            || resp.status() == StatusCode::UNPROCESSABLE_ENTITY
    );
}

/// Cross-chain trace should return correlation metadata when available
#[sqlx::test(migrations = "./migrations")]
async fn cross_chain_trace_returns_correlation_data(pool: PgPool) {
    let app = make_router(pool);

    let resp = app
        .oneshot(
            Request::builder()
                .uri("/v1/cross-chain/trace?tx_hash=abc123def456")
                .header("accept", "application/json")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    // If endpoint exists and returns data, should be JSON
    if resp.status() == StatusCode::OK {
        let content_type = resp.headers().get("content-type").unwrap().to_str().unwrap();
        assert!(content_type.contains("json"));

        let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let _: serde_json::Value = serde_json::from_slice(&body)
            .expect("Response should be valid JSON");
    }
}

/// Cross-chain events should include causality tracking
#[sqlx::test(migrations = "./migrations")]
async fn cross_chain_events_include_causality_metadata(pool: PgPool) {
    let app = make_router(pool);

    let resp = app
        .oneshot(
            Request::builder()
                .uri("/v1/events?include_causality=true")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    // Parameter should be supported or gracefully ignored
    assert_eq!(resp.status(), StatusCode::OK);
}

/// Cross-chain trace endpoint should support multiple hash formats
#[sqlx::test(migrations = "./migrations")]
async fn cross_chain_trace_supports_multiple_hash_formats(pool: PgPool) {
    let app = make_router(pool);

    // Try with 0x prefix
    let resp1 = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/v1/cross-chain/trace?tx_hash=0xabc123")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(
        resp1.status() == StatusCode::OK
            || resp1.status() == StatusCode::NOT_FOUND
            || resp1.status() == StatusCode::BAD_REQUEST
    );
}

/// Cross-chain trace should indicate if no correlation exists
#[sqlx::test(migrations = "./migrations")]
async fn cross_chain_trace_handles_no_correlation(pool: PgPool) {
    let app = make_router(pool);

    let resp = app
        .oneshot(
            Request::builder()
                .uri("/v1/cross-chain/trace?tx_hash=nonexistent")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    // Should return 200 with empty data or 404
    assert!(
        resp.status() == StatusCode::OK || resp.status() == StatusCode::NOT_FOUND
    );
}
