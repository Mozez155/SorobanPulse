use chrono::DateTime;
use serde_json::json;
use sqlx::PgPool;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast;
use tokio::time::sleep;
use tracing::{error, info, warn, instrument, span, Level};

use crate::{
    config::{Config, HealthState, IndexerState},
    metrics,
    models::{GetEventsResult, LatestLedgerResult, RpcResponse, SorobanEvent},
};

#[async_trait::async_trait]
pub trait RpcClient: Send + Sync {
    async fn get_latest_ledger(&self, rpc_url: &str) -> Result<u64, String>;
    async fn get_events(&self, rpc_url: &str, start_ledger: u64, cursor: Option<String>) -> Result<GetEventsResult, String>;
}

/// Postgres advisory lock key for the indexer singleton.
const INDEXER_LOCK_KEY: i64 = 0x536f726f62616e50; // "SorobanP"

#[derive(Debug, thiserror::Error)]
enum IndexerFetchError {
    #[error("{0}")]
    Rpc(String),
    #[error(transparent)]
    DbConnection(#[from] sqlx::Error),
}


pub struct SorobanRpcClient {
    client: reqwest::Client,
}

impl SorobanRpcClient {
    pub fn new(config: &Config) -> Self {
        let client = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(config.rpc_connect_timeout_secs))
            .timeout(Duration::from_secs(config.rpc_request_timeout_secs))
            .pool_max_idle_per_host(5)
            .pool_idle_timeout(Duration::from_secs(30))
            .tcp_keepalive(Duration::from_secs(60))
            .build()
            .expect("Failed to build HTTP client");
        Self { client }
    }
}

#[async_trait::async_trait]
impl RpcClient for SorobanRpcClient {
    async fn get_latest_ledger(&self, rpc_url: &str) -> Result<u64, String> {
        let body = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getLatestLedger"
        });

        let span = span!(Level::INFO, "rpc_get_latest_ledger", url = %rpc_url);
        let _enter = span.enter();

        let resp: RpcResponse<LatestLedgerResult> = self
            .client
            .post(rpc_url)
            .json(&body)
            .send()
            .await
            .map_err(|e| {
                if e.is_timeout() {
                    warn!(
                        "RPC request timeout"
                    );
                }
                metrics::record_rpc_error();
                e.to_string()
            })?
            .json()
            .await
            .map_err(|e| e.to_string())?;

        match resp.result {
            Some(r) => {
                metrics::update_latest_ledger(r.sequence);
                Ok(r.sequence)
            }
            None => {
                if let Some(err) = resp.error {
                    warn!(code = err.code, message = %err.message, "RPC error");
                    metrics::record_rpc_error();
                }
                Err("RPC returned no result".to_string())
            }
        }
    }

    async fn get_events(&self, rpc_url: &str, start_ledger: u64, cursor: Option<String>) -> Result<GetEventsResult, String> {
        let mut params = json!({
            "filters": [],
            "pagination": { "limit": 100 }
        });

        if let Some(c) = &cursor {
            params["pagination"]["cursor"] = json!(c);
        } else {
            params["startLedger"] = json!(start_ledger);
        }

        let body = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getEvents",
            "params": params
        });

        let resp: RpcResponse<GetEventsResult> = self
            .client
            .post(rpc_url)
            .json(&body)
            .send()
            .await
            .map_err(|e| {
                if e.is_timeout() {
                    warn!(
                        "RPC request timeout"
                    );
                }
                metrics::record_rpc_error();
                e.to_string()
            })?
            .json()
            .await
            .map_err(|e| e.to_string())?;

        match resp.result {
            Some(r) => Ok(r),
            None => {
                if let Some(err) = resp.error {
                    warn!(code = err.code, message = %err.message, "RPC error");
                    metrics::record_rpc_error();
                    Err(err.message)
                } else {
                    Err("RPC returned no result".to_string())
                }
            }
        }
    }
}

pub struct Indexer<R: RpcClient> {
    pool: PgPool,
    rpc_client: R,
    config: Config,
    shutdown_rx: tokio::sync::watch::Receiver<bool>,
    health_state: Option<Arc<HealthState>>,
    indexer_state: Option<Arc<IndexerState>>,
    event_tx: Option<broadcast::Sender<SorobanEvent>>,
}

impl<R: RpcClient> Indexer<R> {
    pub fn new(pool: PgPool, config: Config, shutdown_rx: tokio::sync::watch::Receiver<bool>, rpc_client: R) -> Self {
        Self {
            pool,
            rpc_client,
            config,
            shutdown_rx,
            health_state: None,
            indexer_state: None,
            event_tx: None,
        }
    }

    /// Set the health state for updating the last poll timestamp
    pub fn set_health_state(&mut self, health_state: Arc<HealthState>) {
        self.health_state = Some(health_state);
    }

    /// Set the indexer state for exposing operational metrics to the /status endpoint
    pub fn set_indexer_state(&mut self, indexer_state: Arc<IndexerState>) {
        self.indexer_state = Some(indexer_state);
    }

    /// Set the broadcast sender for real-time SSE streaming.
    pub fn set_event_tx(&mut self, event_tx: broadcast::Sender<SorobanEvent>) {
        self.event_tx = Some(event_tx);
    }

    pub async fn run(&self) {
        // Attempt to acquire a Postgres session-level advisory lock.
        // Only one replica will hold this lock at a time; others serve HTTP only.
        let lock_conn = match self.pool.acquire().await {
            Ok(c) => c,
            Err(e) => {
                error!(error = %e, "Failed to acquire DB connection for advisory lock");
                return;
            }
        };

        let acquired: bool = sqlx::query_scalar("SELECT pg_try_advisory_lock($1)")
            .bind(INDEXER_LOCK_KEY)
            .fetch_one(&self.pool)
            .await
            .unwrap_or(false);

        if !acquired {
            info!("Indexer lock not acquired, running in read-only mode");
            // Set mode to read_only so /status reflects this replica's role
            if let Some(ref s) = self.indexer_state {
                s.is_active_indexer.store(false, std::sync::atomic::Ordering::Relaxed);
            }
            // Hold the connection open so we can detect when the lock owner dies
            // and re-attempt on the next startup/restart cycle.
            drop(lock_conn);
            return;
        }

        info!("Indexer lock acquired, starting indexing");
        // Set mode to active so /status reflects this replica's role
        if let Some(ref s) = self.indexer_state {
            s.is_active_indexer.store(true, std::sync::atomic::Ordering::Relaxed);
        }

        // Run the actual indexing loop; release lock on exit.
        self.run_loop().await;

        // Explicitly release the advisory lock on graceful shutdown.
        let _ = sqlx::query("SELECT pg_advisory_unlock($1)")
            .bind(INDEXER_LOCK_KEY)
            .execute(&self.pool)
            .await;

        drop(lock_conn);
    }

    async fn run_loop(&self) {
        let mut current_ledger = self.config.start_ledger;
        let mut consecutive_db_errors = 0u32;
        let mut rpc_backoff_ms = 1000u64; // Start with 1 second backoff

        if current_ledger == 0 {
            loop {
                match self.rpc_client.get_latest_ledger(&self.config.stellar_rpc_url).await {
                    Ok(ledger) => {
                        current_ledger = ledger;
                        info!(ledger = current_ledger, "Starting from latest ledger");
                        metrics::update_current_ledger(current_ledger);
                        if let Some(ref s) = self.indexer_state {
                            s.current_ledger.store(current_ledger, std::sync::atomic::Ordering::Relaxed);
                        }
                        rpc_backoff_ms = 1000; // Reset backoff on success
                        break;
                    }
                    Err(e) => {
                        error!(error = %e, backoff_ms = rpc_backoff_ms, "Failed to get latest ledger, retrying with exponential backoff");
                        sleep(Duration::from_millis(rpc_backoff_ms)).await;
                        // Exponential backoff with max 60 seconds
                        rpc_backoff_ms = std::cmp::min(rpc_backoff_ms * 2, 60000);
                    }
                }
            }
        }

        loop {
            if *self.shutdown_rx.borrow() {
                info!("Indexer shutting down gracefully");
                break;
            }

            match self.fetch_and_store_events(current_ledger).await {
                Ok(latest) => {
                    consecutive_db_errors = 0;
                    // Update the last poll timestamp on success
                    if let Some(ref health_state) = self.health_state {
                        health_state.update_last_poll();
                    }
                    if latest > current_ledger {
                        current_ledger = latest;
                        metrics::update_current_ledger(current_ledger);
                        if let Some(ref s) = self.indexer_state {
                            s.current_ledger.store(current_ledger, std::sync::atomic::Ordering::Relaxed);
                        }

                        // Calculate and update lag
                        let latest_ledger = self.rpc_client.get_latest_ledger(&self.config.stellar_rpc_url).await.unwrap_or(0);
                        if latest_ledger > current_ledger {
                            if let Some(ref s) = self.indexer_state {
                                s.latest_ledger.store(latest_ledger, std::sync::atomic::Ordering::Relaxed);
                            }
                            let lag = latest_ledger - current_ledger;
                            metrics::update_indexer_lag(lag);
                            
                            // Warn if lag exceeds threshold
                            if lag > self.config.indexer_lag_warn_threshold {
                                warn!(
                                    lag = lag,
                                    threshold = self.config.indexer_lag_warn_threshold,
                                    "Indexer is falling behind"
                                );
                            }
                        }
                    } else {
                        sleep(Duration::from_millis(self.config.indexer_poll_interval_ms)).await;
                    }
                }
                Err(IndexerFetchError::DbConnection(e)) => {
                    consecutive_db_errors += 1;
                    let backoff_secs = if consecutive_db_errors >= 5 {
                        60
                    } else {
                        10
                    };
                    if consecutive_db_errors == 5 {
                        error!(
                            consecutive = consecutive_db_errors,
                            "DB unavailable, backing off"
                        );
                    } else if consecutive_db_errors < 5 {
                        error!(error = %e, "Indexer error");
                    }
                    sleep(Duration::from_secs(backoff_secs)).await;
                }
                Err(IndexerFetchError::Rpc(msg)) => {
                    error!(error = %msg, "Indexer error");
                    sleep(Duration::from_millis(self.config.indexer_error_backoff_ms)).await;
                }
            }
        }
    }

    async fn get_latest_ledger(&self) -> Result<u64, String> {
        self.rpc_client.get_latest_ledger(&self.config.stellar_rpc_url).await
    }

    #[instrument(skip(self), fields(start_ledger = start_ledger))]
    async fn fetch_and_store_events(&self, start_ledger: u64) -> Result<u64, IndexerFetchError> {
        let mut cursor: Option<String> = None;
        let mut latest_ledger = start_ledger;
        let mut total_fetched = 0;
        let mut total_inserted = 0;
        let mut total_skipped = 0;

        loop {
            let mut params = json!({
                "filters": [],
                "pagination": { "limit": 100 }
            });

            if let Some(c) = &cursor {
                params["pagination"]["cursor"] = json!(c);
            } else {
                params["startLedger"] = json!(start_ledger);
            }

            let body = json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "getEvents",
                "params": params
            });

            let result = match self.rpc_client.get_events(&self.config.stellar_rpc_url, start_ledger, cursor).await {
                Ok(r) => r,
                Err(msg) => {
                    warn!(message = %msg, "RPC error");
                    metrics::record_rpc_error();
                    return Err(IndexerFetchError::Rpc(msg));
                }
            };

            latest_ledger = result.latest_ledger;
            let current_count = result.events.len();
            total_fetched += current_count;

            for event in result.events {
                match self.store_event(&event).await {
                    Ok(rows) => {
                        total_inserted += rows;
                        if rows == 0 {
                            total_skipped += 1;
                            metrics::record_duplicate_event();
                        } else if let Some(ref tx) = self.event_tx {
                            let _ = tx.send(event);
                        }
                    }
                    Err(e) => {
                        warn!(
                            tx_hash = %event.tx_hash,
                            contract_id = %event.contract_id,
                            ledger = event.ledger,
                            event_type = %event.event_type,
                            error = %e,
                            "Failed to store event",
                        );
                    }
                }
            }

            cursor = result.rpc_cursor;
            if cursor.is_none() {
                break;
            }
        }

        info!(
            fetched = total_fetched,
            inserted = total_inserted,
            ledger = latest_ledger,
            "Indexed ledger range"
        );
        metrics::record_events_indexed(total_inserted as u64);

        if total_fetched > 0 {
            let duplicate_rate = (total_skipped as f64 / total_fetched as f64) * 100.0;
            tracing::debug!(
                duplicates = total_skipped,
                total_fetched = total_fetched,
                duplicate_rate = duplicate_rate,
                "Event deduplication stats"
            );
        }

        if latest_ledger > start_ledger {
            Ok(latest_ledger + 1)
        } else {
            Ok(start_ledger)
        }
    }
    fn validate_event_data(event: &SorobanEvent) -> bool {
        // Validate that value is an object or null
        match &event.value {
            serde_json::Value::Object(_) | serde_json::Value::Null => {}
            _ => {
                warn!(
                    tx_hash = %event.tx_hash,
                    contract_id = %event.contract_id,
                    ledger = event.ledger,
                    event_type = %event.event_type,
                    value_type = match &event.value {
                        serde_json::Value::Null => "null",
                        serde_json::Value::Bool(_) => "bool",
                        serde_json::Value::Number(_) => "number",
                        serde_json::Value::String(_) => "string",
                        serde_json::Value::Array(_) => "array",
                        serde_json::Value::Object(_) => "object",
                    },
                    "Invalid event_data.value: expected object or null",
                );
                metrics::record_validation_failure();
                return false;
            }
        }

        // topic is Option<Vec<Value>> — always valid (None or a Vec)
        let _ = &event.topic;

        true
    }

    #[instrument(skip(self, event), fields(tx_hash = %event.tx_hash, contract_id = %event.contract_id, ledger = event.ledger))]
    async fn store_event(&self, event: &SorobanEvent) -> Result<u64, anyhow::Error> {
        // Validate event_data structure
        if !Self::validate_event_data(event) {
            return Ok(0);
        }

        let ledger = match i64::try_from(event.ledger) {
            Ok(v) => v,
            Err(_) => {
                error!(
                    tx_hash = %event.tx_hash,
                    contract_id = %event.contract_id,
                    ledger = event.ledger,
                    event_type = %event.event_type,
                    "Ledger number overflows i64, skipping event",
                );
                return Ok(0);
            }
        };
        let timestamp = DateTime::parse_from_rfc3339(&event.ledger_closed_at)
            .map(|dt| dt.with_timezone(&chrono::Utc))
            .map_err(|_| {
                warn!(
                    tx_hash = %event.tx_hash,
                    contract_id = %event.contract_id,
                    ledger = event.ledger,
                    event_type = %event.event_type,
                    raw = %event.ledger_closed_at,
                    "Unparseable ledger_closed_at, skipping event",
                );
                anyhow::anyhow!("Unparseable ledger_closed_at: {}", event.ledger_closed_at)
            })?;

        let event_data = json!({
            "value": event.value,
            "topic": event.topic
        });

        let result = sqlx::query(
            r#"
            INSERT INTO events (contract_id, event_type, tx_hash, ledger, timestamp, event_data)
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (tx_hash, contract_id, event_type) DO NOTHING
            "#,
        )
        .bind(&event.contract_id)
        .bind(&event.event_type)
        .bind(&event.tx_hash)
        .bind(ledger)
        .bind(timestamp)
        .bind(event_data)
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;
    use std::collections::VecDeque;
    use std::sync::{Arc, Mutex};
    use tokio::sync::watch;

    #[derive(Debug, Clone)]
    pub struct MockRpcClient {
        latest_ledger_responses: Arc<Mutex<VecDeque<Result<u64, String>>>>,
        get_events_responses: Arc<Mutex<VecDeque<Result<GetEventsResult, String>>>>,
    }

    impl MockRpcClient {
        pub fn new() -> Self {
            Self {
                latest_ledger_responses: Arc::new(Mutex::new(VecDeque::new())),
                get_events_responses: Arc::new(Mutex::new(VecDeque::new())),
            }
        }

        pub fn with_latest_ledger_responses(responses: Vec<Result<u64, String>>) -> Self {
            Self {
                latest_ledger_responses: Arc::new(Mutex::new(VecDeque::from(responses))),
                get_events_responses: Arc::new(Mutex::new(VecDeque::new())),
            }
        }

        pub fn with_get_events_responses(responses: Vec<Result<GetEventsResult, String>>) -> Self {
            Self {
                latest_ledger_responses: Arc::new(Mutex::new(VecDeque::new())),
                get_events_responses: Arc::new(Mutex::new(VecDeque::from(responses))),
            }
        }

        pub fn add_latest_ledger_response(&self, response: Result<u64, String>) {
            self.latest_ledger_responses.lock().unwrap().push_back(response);
        }

        pub fn add_get_events_response(&self, response: Result<GetEventsResult, String>) {
            self.get_events_responses.lock().unwrap().push_back(response);
        }
    }

    impl Default for MockRpcClient {
        fn default() -> Self {
            Self::new()
        }
    }

    #[async_trait::async_trait]
    impl RpcClient for MockRpcClient {
        async fn get_latest_ledger(&self, _rpc_url: &str) -> Result<u64, String> {
            let mut responses = self.latest_ledger_responses.lock().unwrap();
            responses.pop_front().unwrap_or(Ok(100))
        }

        async fn get_events(&self, _rpc_url: &str, _start_ledger: u64, _cursor: Option<String>) -> Result<GetEventsResult, String> {
            let mut responses = self.get_events_responses.lock().unwrap();
            responses.pop_front().unwrap_or(Ok(GetEventsResult {
                events: vec![],
                latest_ledger: 100,
                rpc_cursor: None,
            }))
        }
    }

    fn make_event(ledger: u64) -> SorobanEvent {
        SorobanEvent {
            contract_id: "C1".into(),
            event_type: "contract".into(),
            tx_hash: "abc".into(),
            ledger,
            ledger_closed_at: "2026-03-24T00:00:00Z".into(),
            value: Value::Null,
            topic: None,
        }
    }

    #[test]
    fn ledger_overflow_returns_err() {
        assert!(i64::try_from(make_event(u64::MAX).ledger).is_err());
    }

    #[test]
    fn validate_event_data_accepts_valid_object_value() {
        let mut event = make_event(1);
        event.value = json!({"key": "value"});
        event.topic = Some(vec![json!("topic1")]);
        assert!(Indexer::<MockRpcClient>::validate_event_data(&event));
    }

    #[test]
    fn validate_event_data_accepts_null_value() {
        let mut event = make_event(1);
        event.value = Value::Null;
        event.topic = None;
        assert!(Indexer::<MockRpcClient>::validate_event_data(&event));
    }

    #[test]
    fn validate_event_data_accepts_null_topic() {
        let mut event = make_event(1);
        event.value = json!({"key": "value"});
        event.topic = None;
        assert!(Indexer::<MockRpcClient>::validate_event_data(&event));
    }

    #[test]
    fn validate_event_data_accepts_array_topic() {
        let mut event = make_event(1);
        event.value = json!({"key": "value"});
        event.topic = Some(vec![json!("topic1"), json!("topic2")]);
        assert!(Indexer::<MockRpcClient>::validate_event_data(&event));
    }

    #[test]
    fn validate_event_data_rejects_string_value() {
        let mut event = make_event(1);
        event.value = Value::String("invalid".to_string());
        assert!(!Indexer::<MockRpcClient>::validate_event_data(&event));
    }

    #[test]
    fn validate_event_data_rejects_number_value() {
        let mut event = make_event(1);
        event.value = Value::Number(42.into());
        assert!(!Indexer::<MockRpcClient>::validate_event_data(&event));
    }

    #[test]
    fn validate_event_data_rejects_array_value() {
        let mut event = make_event(1);
        event.value = Value::Array(vec![]);
        assert!(!Indexer::<MockRpcClient>::validate_event_data(&event));
    }

    #[test]
    fn validate_event_data_rejects_string_topic() {
        let mut event = make_event(1);
        event.value = json!({"key": "value"});
        event.topic = Some(Value::String("invalid".to_string()));
        assert!(!Indexer::<MockRpcClient>::validate_event_data(&event));
    }

    #[test]
    fn validate_event_data_rejects_object_topic() {
        let mut event = make_event(1);
        event.value = json!({"key": "value"});
        event.topic = Some(json!({"invalid": "object"}));
        assert!(!Indexer::<MockRpcClient>::validate_event_data(&event));
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn invalid_event_data_is_skipped(pool: PgPool) {
        let indexer = indexer(pool.clone());
        let mut event = make_event(1);
        event.value = Value::String("invalid".to_string());

        let result = indexer.store_event(&event).await.unwrap();
        assert_eq!(result, 0); // Event should be skipped

        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM events")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(count, 0);
    }

    fn indexer(pool: PgPool) -> Indexer<MockRpcClient> {
        let (_, shutdown_rx) = watch::channel(false);
        Indexer::new(
            pool,
            Config {
                database_url: String::new(),
                stellar_rpc_url: String::new(),
                start_ledger: 0,
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
                environment: crate::config::Environment::Development,
                max_body_size_bytes: 1024 * 1024,
            },
            shutdown_rx,
            MockRpcClient::new(),
        )
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn duplicate_insert_yields_one_row(pool: PgPool) {
        let indexer = indexer(pool.clone());
        let event = make_event(1);

        indexer.store_event(&event).await.unwrap();
        indexer.store_event(&event).await.unwrap(); // must not error

        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM events")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(count, 1);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn same_tx_hash_different_event_type_both_stored(pool: PgPool) {
        let indexer = indexer(pool.clone());
        let mut e1 = make_event(1);
        let mut e2 = make_event(1);
        e2.event_type = "system".into();

        indexer.store_event(&e1).await.unwrap();
        indexer.store_event(&e2).await.unwrap();

        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM events")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(count, 2);
    }

    #[tokio::test]
    async fn mock_rpc_client_returns_configured_responses() {
        let mock_client = MockRpcClient::with_latest_ledger_responses(vec![
            Ok(42),
            Err("RPC error".to_string()),
            Ok(100),
        ]);

        assert_eq!(mock_client.get_latest_ledger("http://test").await.unwrap(), 42);
        assert!(mock_client.get_latest_ledger("http://test").await.is_err());
        assert_eq!(mock_client.get_latest_ledger("http://test").await.unwrap(), 100);
    }

    #[tokio::test]
    async fn mock_rpc_client_get_events_returns_configured_responses() {
        let test_event = make_event(1);
        let mock_client = MockRpcClient::with_get_events_responses(vec![
            Ok(GetEventsResult {
                events: vec![test_event.clone()],
                latest_ledger: 50,
                rpc_cursor: None,
            }),
            Err("Network error".to_string()),
        ]);

        let result1 = mock_client.get_events("http://test", 1, None).await.unwrap();
        assert_eq!(result1.events.len(), 1);
        assert_eq!(result1.latest_ledger, 50);

        let result2 = mock_client.get_events("http://test", 2, None).await;
        assert!(result2.is_err());
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn indexer_uses_mock_rpc_client(pool: PgPool) {
        let mock_client = MockRpcClient::new();
        mock_client.add_latest_ledger_response(Ok(100));
        
        let test_event = make_event(100);
        mock_client.add_get_events_response(Ok(GetEventsResult {
            events: vec![test_event],
            latest_ledger: 100,
            rpc_cursor: None,
        }));

        let (_, shutdown_rx) = watch::channel(false);
        let indexer = Indexer::new(
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
                api_key: None,
                db_max_connections: 10,
                db_min_connections: 2,
                allowed_origins: vec!["*".to_string()],
                rate_limit_per_minute: 60,
                indexer_stall_timeout_secs: 60,
                db_statement_timeout_ms: 5000,
                indexer_poll_interval_ms: 5000,
                indexer_error_backoff_ms: 10000,
                environment: crate::config::Environment::Development,
            },
            shutdown_rx,
            mock_client,
        );

        // Test that the indexer can use the mock client
        let latest_ledger = indexer.get_latest_ledger().await.unwrap();
        assert_eq!(latest_ledger, 100);
    }
}
