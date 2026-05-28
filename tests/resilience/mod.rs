//! Network partition resilience tests.
//!
//! These tests verify that the service degrades gracefully and recovers correctly
//! when the RPC endpoint or database becomes unreachable.
//!
//! They use the `MockRpcClient` from the indexer module to simulate network failures
//! without requiring Docker or toxiproxy, making them runnable in standard CI.

mod test_helpers;
use test_helpers::wait_for;

use soroban_pulse::config::{Config, HealthState, IndexerState};
use soroban_pulse::indexer::{Indexer, RpcClient};
use soroban_pulse::models::{GetEventsResult, SorobanEvent};
use sqlx::PgPool;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::watch;

// ---------------------------------------------------------------------------
// Minimal mock RPC client (mirrors the one in indexer.rs tests)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct MockRpcClient {
    latest_ledger_responses: Arc<Mutex<VecDeque<Result<u64, String>>>>,
    get_events_responses: Arc<Mutex<VecDeque<Result<GetEventsResult, String>>>>,
}

impl MockRpcClient {
    fn new() -> Self {
        Self {
            latest_ledger_responses: Arc::new(Mutex::new(VecDeque::new())),
            get_events_responses: Arc::new(Mutex::new(VecDeque::new())),
        }
    }

    fn push_latest_ledger(&self, r: Result<u64, String>) {
        self.latest_ledger_responses.lock().unwrap().push_back(r);
    }

    fn push_get_events(&self, r: Result<GetEventsResult, String>) {
        self.get_events_responses.lock().unwrap().push_back(r);
    }
}

#[async_trait::async_trait]
impl RpcClient for MockRpcClient {
    async fn get_latest_ledger(&self, _url: &str) -> Result<u64, String> {
        self.latest_ledger_responses
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or(Ok(100))
    }

    async fn get_events(
        &self,
        _url: &str,
        _start: u64,
        _cursor: Option<String>,
    ) -> Result<GetEventsResult, String> {
        self.get_events_responses
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or(Ok(GetEventsResult {
                events: vec![],
                latest_ledger: 100,
                rpc_cursor: None,
                protocol_version: None,
            }))
    }
}

fn make_event(ledger: u64) -> SorobanEvent {
    SorobanEvent {
        contract_id: "C1".into(),
        event_type: "contract".into(),
        tx_hash: format!("{:0>64}", ledger),
        ledger,
        ledger_closed_at: "2026-03-24T00:00:00Z".into(),
        value: serde_json::Value::Null,
        topic: None,
    }
}

fn make_indexer(pool: PgPool, rpc: MockRpcClient) -> Indexer<MockRpcClient> {
    let (_, shutdown_rx) = watch::channel(false);
    Indexer::new(
        pool,
        Config {
            database_url: String::new(),
            stellar_rpc_url: String::new(),
            start_ledger: 100,
            port: 3000,
            behind_proxy: false,
            start_ledger_fallback: true,
            indexer_lag_warn_threshold: 1000,
            rpc_connect_timeout_secs: 30,
            rpc_request_timeout_secs: 60,
            api_keys: Vec::new(),
            db_max_connections: 10,
            db_min_connections: 2,
            allowed_origins: vec!["*".to_string()],
            rate_limit_per_minute: 60,
            indexer_stall_timeout_secs: 60,
            db_statement_timeout_ms: 5000,
            indexer_poll_interval_ms: 5000,
            indexer_error_backoff_ms: 10000,
            sse_keepalive_interval_ms: 15000,
            sse_max_connections: 1000,
            environment: soroban_pulse::config::Environment::Development,
            max_body_size_bytes: 1024 * 1024,
            log_sample_rate: 1,
            event_data_encryption_key: None,
            event_data_encryption_key_old: None,
        },
        shutdown_rx,
        rpc,
    )
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// When the RPC returns an error, `fetch_and_store_events` propagates it as
/// `IndexerFetchError::Rpc` so the run loop can apply the error backoff.
#[sqlx::test(migrations = "./migrations")]
async fn rpc_unreachable_returns_error(pool: PgPool) {
    let rpc = MockRpcClient::new();
    rpc.push_get_events(Err("connection refused".to_string()));

    let indexer = make_indexer(pool, rpc);
    let result = indexer.fetch_and_store_events_pub(100).await;
    assert!(
        result.is_err(),
        "expected Rpc error when RPC is unreachable"
    );
}

/// After an RPC failure the indexer recovers on the next call when the RPC
/// becomes available again.
#[sqlx::test(migrations = "./migrations")]
async fn rpc_recovers_after_failure(pool: PgPool) {
    let rpc = MockRpcClient::new();
    // First call fails.
    rpc.push_get_events(Err("network partition".to_string()));
    // Second call succeeds with one event.
    rpc.push_get_events(Ok(GetEventsResult {
        events: vec![make_event(101)],
        latest_ledger: 101,
        rpc_cursor: None,
        protocol_version: Some(1),
    }));

    let indexer = make_indexer(pool.clone(), rpc);

    // First attempt fails.
    assert!(indexer.fetch_and_store_events_pub(100).await.is_err());

    // Second attempt succeeds and stores the event.
    let latest = indexer.fetch_and_store_events_pub(100).await.unwrap();
    assert!(latest > 100, "latest ledger should advance after recovery");

    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM events")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 1, "event should be stored after recovery");
}

/// Multiple consecutive RPC failures do not corrupt state — the indexer can
/// still process events once the RPC is available again.
#[sqlx::test(migrations = "./migrations")]
async fn rpc_multiple_failures_then_recovery(pool: PgPool) {
    let rpc = MockRpcClient::new();
    for _ in 0..3 {
        rpc.push_get_events(Err("timeout".to_string()));
    }
    rpc.push_get_events(Ok(GetEventsResult {
        events: vec![make_event(105), make_event(106)],
        latest_ledger: 106,
        rpc_cursor: None,
        protocol_version: Some(1),
    }));

    let indexer = make_indexer(pool.clone(), rpc);

    for _ in 0..3 {
        assert!(indexer.fetch_and_store_events_pub(100).await.is_err());
    }

    let latest = indexer.fetch_and_store_events_pub(100).await.unwrap();
    assert!(latest > 100);

    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM events")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 2);
}

/// When the RPC is unreachable the HTTP server continues to serve requests.
/// This is verified by checking that the DB-backed handler still responds
/// (the indexer stall does not crash the process or block the HTTP layer).
#[sqlx::test(migrations = "./migrations")]
async fn http_server_available_during_indexer_stall(pool: PgPool) {
    use axum::body::{to_bytes, Body};
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let health_state = Arc::new(HealthState::new(3600)); // very long timeout — not stalled
    let indexer_state = Arc::new(IndexerState::new());
    let prometheus_handle = soroban_pulse::metrics::init_metrics();
    let config = Config::default();

    let app = soroban_pulse::routes::create_router(
        pool,
        vec![],
        &[],
        60,
        health_state,
        indexer_state,
        prometheus_handle,
        2000,
        config,
    );

    // Even with no indexer running, the HTTP server should respond to /v1/events.
    let response = app
        .oneshot(
            Request::builder()
                .uri("/v1/events")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(v["data"], serde_json::json!([]));
}

/// Health endpoint reports degraded status when the indexer is stalled.
#[sqlx::test(migrations = "./migrations")]
async fn health_reports_degraded_when_indexer_stalled(pool: PgPool) {
    use axum::body::{to_bytes, Body};
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    // stall_timeout_secs = 1 and last_poll never updated → stalled immediately
    let health_state = Arc::new(HealthState::new(1));
    let indexer_state = Arc::new(IndexerState::new());
    let prometheus_handle = soroban_pulse::metrics::init_metrics();
    let config = Config::default();

    let app = soroban_pulse::routes::create_router(
        pool,
        vec![],
        &[],
        60,
        health_state,
        indexer_state,
        prometheus_handle,
        2000,
        config,
    );

    // Wait for the stall threshold to be exceeded using polling instead of fixed sleep
    wait_for(
        || async {
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .uri("/healthz/ready")
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            response.status() == StatusCode::SERVICE_UNAVAILABLE
        },
        Duration::from_secs(30),
        Duration::from_millis(100),
    )
    .await
    .expect("Health endpoint should report degraded status");

    let response = app
        .oneshot(
            Request::builder()
                .uri("/healthz/ready")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(v["status"], "degraded");
    assert_eq!(v["indexer"], "stalled");
}

/// Health endpoint reports ok after the indexer resumes polling.
#[sqlx::test(migrations = "./migrations")]
async fn health_reports_ok_after_indexer_resumes(pool: PgPool) {
    use axum::body::{to_bytes, Body};
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    let health_state = Arc::new(HealthState::new(60));
    // Simulate a fresh poll.
    health_state.update_last_poll();

    let indexer_state = Arc::new(IndexerState::new());
    let prometheus_handle = soroban_pulse::metrics::init_metrics();
    let config = Config::default();

    let app = soroban_pulse::routes::create_router(
        pool,
        vec![],
        &[],
        60,
        health_state,
        indexer_state,
        prometheus_handle,
        2000,
        config,
    );

    let response = app
        .oneshot(
            Request::builder()
                .uri("/healthz/ready")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(v["status"], "ok");
    assert_eq!(v["indexer"], "ok");
}
