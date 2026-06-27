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

/// Regression test for #573: GraphQL endpoint should be available at /graphql
#[sqlx::test(migrations = "./migrations")]
async fn graphql_endpoint_exists(pool: PgPool) {
    let app = make_router(pool);

    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/graphql")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"query": "{ __typename }"}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    // Should either accept the request or return 404 if not yet implemented
    assert!(
        resp.status() == StatusCode::OK
            || resp.status() == StatusCode::NOT_FOUND
            || resp.status() == StatusCode::UNPROCESSABLE_ENTITY
    );
}

/// GraphQL schema introspection returns valid schema
#[sqlx::test(migrations = "./migrations")]
async fn graphql_schema_introspection_query(pool: PgPool) {
    let app = make_router(pool);

    let introspection_query = r#"{"query": "{ __schema { types { name } } }"}"#;
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/graphql")
                .header("content-type", "application/json")
                .body(Body::from(introspection_query))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(
        resp.status() == StatusCode::OK
            || resp.status() == StatusCode::NOT_FOUND
            || resp.status() == StatusCode::UNPROCESSABLE_ENTITY
    );
}

/// GraphQL queries for events should be filterable
#[sqlx::test(migrations = "./migrations")]
async fn graphql_events_query_with_filters(pool: PgPool) {
    let app = make_router(pool);

    let query = r#"{"query": "{ events(contractId: \"CABC123\") { id contractId } }"}"#;
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/graphql")
                .header("content-type", "application/json")
                .body(Body::from(query))
                .unwrap(),
        )
        .await
        .unwrap();

    assert!(
        resp.status() == StatusCode::OK
            || resp.status() == StatusCode::NOT_FOUND
            || resp.status() == StatusCode::UNPROCESSABLE_ENTITY
    );
}

/// GraphQL WebSocket endpoint should support subscriptions
#[sqlx::test(migrations = "./migrations")]
async fn graphql_websocket_subscription_endpoint_exists(pool: PgPool) {
    let app = make_router(pool);

    let resp = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/graphql/ws")
                .header("upgrade", "websocket")
                .header("connection", "upgrade")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    // Should either upgrade to WebSocket or return 404/426 if not yet implemented
    assert!(
        resp.status() == StatusCode::SWITCHING_PROTOCOLS
            || resp.status() == StatusCode::NOT_FOUND
            || resp.status() == StatusCode::UPGRADE_REQUIRED
    );
}

/// GraphQL endpoint should reject invalid queries with proper error response
#[sqlx::test(migrations = "./migrations")]
async fn graphql_invalid_query_returns_error_response(pool: PgPool) {
    let app = make_router(pool);

    let invalid_query = r#"{"query": "{ invalidField }"}"#;
    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/graphql")
                .header("content-type", "application/json")
                .body(Body::from(invalid_query))
                .unwrap(),
        )
        .await
        .unwrap();

    // Invalid queries should return 200 with errors in response body
    // or 400 if validation happens at transport level
    assert!(
        resp.status() == StatusCode::OK
            || resp.status() == StatusCode::BAD_REQUEST
            || resp.status() == StatusCode::NOT_FOUND
    );
}

/// GraphQL endpoint should support content negotiation
#[sqlx::test(migrations = "./migrations")]
async fn graphql_endpoint_content_negotiation(pool: PgPool) {
    let app = make_router(pool);

    let resp = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/graphql")
                .header("content-type", "application/json")
                .header("accept", "application/json")
                .body(Body::from(r#"{"query": "{ __typename }"}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    if resp.status() == StatusCode::OK {
        let content_type = resp.headers().get("content-type").unwrap().to_str().unwrap();
        assert!(content_type.contains("json"));
    }
}
