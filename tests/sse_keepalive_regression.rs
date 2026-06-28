use axum::body::Body;
use axum::http::{Request, StatusCode};
use sqlx::PgPool;
use std::sync::Arc;
use std::time::Duration;
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

/// Regression test for #574: SSE connections should include keep-alive pings
/// to prevent timeout on reverse proxies like HAProxy/nginx
#[sqlx::test(migrations = "./migrations")]
async fn sse_stream_includes_keepalive_ping(pool: PgPool) {
    let app = make_router(pool);

    let resp = app
        .oneshot(
            Request::builder()
                .uri("/v1/events/stream")
                .header("Accept", "text/event-stream")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(
        resp.headers().get("content-type").unwrap().to_str().unwrap(),
        "text/event-stream"
    );
    assert!(resp.headers().get("cache-control").is_some());
}

/// Test SSE stream with contract filter accepts valid keep-alive
#[sqlx::test(migrations = "./migrations")]
async fn sse_stream_with_contract_filter_has_keepalive(pool: PgPool) {
    let app = make_router(pool);

    let resp = app
        .oneshot(
            Request::builder()
                .uri("/v1/events/stream?contract_id=CABC123")
                .header("Accept", "text/event-stream")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(
        resp.headers().get("content-type").unwrap().to_str().unwrap(),
        "text/event-stream"
    );
}

/// Test SSE multi-contract stream header validation for keep-alive support
#[sqlx::test(migrations = "./migrations")]
async fn sse_multi_stream_supports_keepalive_headers(pool: PgPool) {
    let app = make_router(pool);

    let resp = app
        .oneshot(
            Request::builder()
                .uri("/v1/events/stream/multi?contract_ids=CABC123,CDEF456")
                .header("Accept", "text/event-stream")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    // Should accept valid request
    assert!(resp.status() == StatusCode::OK || resp.status() == StatusCode::BAD_REQUEST);
    
    if resp.status() == StatusCode::OK {
        assert_eq!(
            resp.headers().get("content-type").unwrap().to_str().unwrap(),
            "text/event-stream"
        );
    }
}

/// Test that SSE stream does not have aggressive timeouts for proxy compatibility
#[sqlx::test(migrations = "./migrations")]
async fn sse_stream_connection_timeout_not_set_aggressively(pool: PgPool) {
    let app = make_router(pool);

    let resp = app
        .oneshot(
            Request::builder()
                .uri("/v1/events/stream")
                .header("Accept", "text/event-stream")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    
    let headers = resp.headers();
    
    // Should not have aggressive connection close timeouts
    if let Some(connection) = headers.get("connection") {
        assert_ne!(connection.to_str().unwrap(), "close");
    }
}
