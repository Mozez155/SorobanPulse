use std::env;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use url::Url;

/// Shared operational state updated by the indexer and read by the /status handler.
pub struct IndexerState {
    pub current_ledger: AtomicU64,
    pub latest_ledger: AtomicU64,
    /// True when this replica holds the advisory lock and is actively indexing.
    pub is_active_indexer: AtomicBool,
    started_at: u64,
}

impl IndexerState {
    pub fn new() -> Self {
        let started_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        Self {
            current_ledger: AtomicU64::new(0),
            latest_ledger: AtomicU64::new(0),
            is_active_indexer: AtomicBool::new(false),
            started_at,
        }
    }

    pub fn uptime_secs(&self) -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
            .saturating_sub(self.started_at)
    }
}

/// Deployment environment — controls strictness of defaults.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Environment {
    Development,
    Staging,
    Production,
}

impl Environment {
    fn from_str(s: &str) -> Self {
        match s.to_ascii_lowercase().as_str() {
            "production" | "prod" => Self::Production,
            "staging" | "stage" => Self::Staging,
            _ => Self::Development,
        }
    }

    /// Returns `true` for staging and production.
    pub fn is_production_like(&self) -> bool {
        matches!(self, Self::Staging | Self::Production)
    }
}

/// Shared state for health checks, accessible between the indexer and HTTP handlers
#[derive(Clone)]
pub struct HealthState {
    /// Unix timestamp of the last successful indexer poll
    pub last_indexer_poll: Arc<AtomicU64>,
    /// Timeout in seconds after which the indexer is considered stalled
    pub indexer_stall_timeout_secs: u64,
}

impl HealthState {
    pub fn new(indexer_stall_timeout_secs: u64) -> Self {
        Self {
            last_indexer_poll: Arc::new(AtomicU64::new(0)),
            indexer_stall_timeout_secs,
        }
    }

    /// Update the last poll timestamp to the current time
    pub fn update_last_poll(&self) {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        self.last_indexer_poll.store(now, Ordering::SeqCst);
    }

    /// Check if the indexer is stalled (no successful poll within the timeout)
    /// Returns Some(seconds_ago) if stalled, None if OK
    pub fn is_indexer_stalled(&self) -> Option<u64> {
        let last_poll = self.last_indexer_poll.load(Ordering::SeqCst);
        if last_poll == 0 {
            // No poll ever completed
            return Some(0);
        }

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let elapsed = now.saturating_sub(last_poll);
        if elapsed > self.indexer_stall_timeout_secs {
            Some(elapsed)
        } else {
            None
        }
    }
}

#[derive(Clone, Debug)]
pub struct Config {
    pub database_url: String,
    pub stellar_rpc_url: String,
    pub start_ledger: u64,
    pub start_ledger_fallback: bool,
    pub port: u16,
    pub api_keys: Vec<String>,
    pub db_max_connections: u32,
    pub db_min_connections: u32,
    pub behind_proxy: bool,
    pub rpc_connect_timeout_secs: u64,
    pub rpc_request_timeout_secs: u64,
    pub allowed_origins: Vec<String>,
    pub rate_limit_per_minute: u32,
    pub indexer_lag_warn_threshold: u64,
    pub indexer_stall_timeout_secs: u64,
    pub db_statement_timeout_ms: u64,
    pub indexer_poll_interval_ms: u64,
    pub indexer_error_backoff_ms: u64,
    pub sse_keepalive_interval_ms: u64,
    pub sse_max_connections: usize,
    pub environment: Environment,
    pub max_body_size_bytes: usize,
    pub event_retention_days: u64,
    pub pruning_interval_hours: u64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            database_url: "postgres://localhost/soroban_pulse".to_string(),
            stellar_rpc_url: "https://soroban-testnet.stellar.org".to_string(),
            start_ledger: 0,
            start_ledger_fallback: false,
            port: 3000,
            api_keys: Vec::new(),
            db_max_connections: 10,
            db_min_connections: 2,
            behind_proxy: false,
            rpc_connect_timeout_secs: 5,
            rpc_request_timeout_secs: 30,
            allowed_origins: vec!["*".to_string()],
            rate_limit_per_minute: 60,
            indexer_lag_warn_threshold: 100,
            indexer_stall_timeout_secs: 60,
            db_statement_timeout_ms: 5000,
            indexer_poll_interval_ms: 5000,
            indexer_error_backoff_ms: 10000,
            sse_keepalive_interval_ms: 15000,
            sse_max_connections: 1000,
            environment: Environment::Development,
            max_body_size_bytes: 1024 * 1024, // 1 MB default
            event_retention_days: 90,
            pruning_interval_hours: 24,
        }
    }
}

fn validate_rpc_url(raw: &str) -> String {
    let url = Url::parse(raw)
        .unwrap_or_else(|e| panic!("STELLAR_RPC_URL is not a valid URL: {e}"));

    let allow_insecure = env::var("ALLOW_INSECURE_RPC")
        .map(|v| matches!(v.to_ascii_lowercase().as_str(), "true" | "1" | "yes" | "y"))
        .unwrap_or(false);

    match url.scheme() {
        "https" => {}
        "http" if allow_insecure => {}
        "http" => panic!(
            "STELLAR_RPC_URL uses http — set ALLOW_INSECURE_RPC=true to permit insecure connections"
        ),
        scheme => panic!("STELLAR_RPC_URL has disallowed scheme '{scheme}' — only https is permitted"),
    }

    if !allow_insecure {
        let host = url.host_str().unwrap_or("");
        let is_loopback = host == "localhost"
            || host == "127.0.0.1"
            || host == "::1"
            || host.ends_with(".local");
        let is_private = host.starts_with("10.")
            || host.starts_with("192.168.")
            || host.starts_with("169.254.")
            || (host.starts_with("172.") && {
                host.split('.')
                    .nth(1)
                    .and_then(|o| o.parse::<u8>().ok())
                    .map(|o| (16..=31).contains(&o))
                    .unwrap_or(false)
            });
        if is_loopback || is_private {
            panic!(
                "STELLAR_RPC_URL points to a non-routable host '{host}' — \
                 set ALLOW_INSECURE_RPC=true to allow this in development"
            );
        }
    }

    // Return URL without credentials
    let mut safe = url.clone();
    let _ = safe.set_username("");
    let _ = safe.set_password(None);
    safe.to_string()
}

/// Read DATABASE_URL from DATABASE_URL_FILE if set, otherwise fall back to DATABASE_URL.
fn resolve_database_url() -> String {
    if let Ok(file_path) = env::var("DATABASE_URL_FILE") {
        std::fs::read_to_string(&file_path)
            .unwrap_or_else(|e| panic!("Failed to read DATABASE_URL_FILE at '{file_path}': {e}"))
            .trim()
            .to_string()
    } else {
        env::var("DATABASE_URL").expect("DATABASE_URL must be set (or DATABASE_URL_FILE)")
    }
}

impl Config {
    /// Returns the DATABASE_URL with credentials stripped — safe to log.
    pub fn safe_db_url(&self) -> String {
        Url::parse(&self.database_url)
            .map(|mut u| {
                let _ = u.set_username("");
                let _ = u.set_password(None);
                u.to_string()
            })
            .unwrap_or_else(|_| "<unparseable>".to_string())
    }

    pub fn from_env() -> Self {
        let environment = Environment::from_str(
            &env::var("ENVIRONMENT").unwrap_or_else(|_| "development".to_string()),
        );

        let behind_proxy = env::var("BEHIND_PROXY")
            .ok()
            .map(|v| matches!(v.to_ascii_lowercase().as_str(), "true" | "1" | "yes" | "y"))
            .unwrap_or(false);

        let start_ledger = env::var("START_LEDGER")
            .unwrap_or_else(|_| "0".to_string())
            .parse()
            .expect("START_LEDGER must be a number");

        let start_ledger_fallback = env::var("START_LEDGER_FALLBACK")
            .ok()
            .map(|v| matches!(v.to_ascii_lowercase().as_str(), "true" | "1" | "yes" | "y"))
            .unwrap_or(false);

        let port = env::var("PORT")
            .unwrap_or_else(|_| "3000".to_string())
            .parse()
            .expect("PORT must be a number");

        // In production-like environments, CORS wildcard is not allowed.
        let allowed_origins: Vec<String> = env::var("ALLOWED_ORIGINS")
            .unwrap_or_else(|_| "*".to_string())
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();

        if environment.is_production_like()
            && allowed_origins.iter().any(|o| o == "*")
        {
            panic!(
                "ALLOWED_ORIGINS=* is not permitted in {environment:?} — \
                 set explicit origins or use ENVIRONMENT=development"
            );
        }

        Self {
            database_url: resolve_database_url(),
            stellar_rpc_url: validate_rpc_url(
                &env::var("STELLAR_RPC_URL")
                    .unwrap_or_else(|_| "https://soroban-testnet.stellar.org".to_string()),
            ),
            start_ledger,
            start_ledger_fallback,
            port,
            api_keys: {
                let mut keys = Vec::new();
                if let Ok(key) = env::var("API_KEY") {
                    if !key.is_empty() {
                        keys.push(key);
                    }
                }
                if let Ok(key) = env::var("API_KEY_SECONDARY") {
                    if !key.is_empty() {
                        keys.push(key);
                    }
                }
                keys
            },
            db_max_connections: env::var("DB_MAX_CONNECTIONS")
                .unwrap_or_else(|_| "10".to_string())
                .parse()
                .expect("DB_MAX_CONNECTIONS must be a number"),
            db_min_connections: env::var("DB_MIN_CONNECTIONS")
                .unwrap_or_else(|_| "2".to_string())
                .parse()
                .expect("DB_MIN_CONNECTIONS must be a number"),
            behind_proxy,
            rpc_connect_timeout_secs: env::var("RPC_CONNECT_TIMEOUT_SECS")
                .unwrap_or_else(|_| "5".to_string())
                .parse()
                .expect("RPC_CONNECT_TIMEOUT_SECS must be a number"),
            rpc_request_timeout_secs: env::var("RPC_REQUEST_TIMEOUT_SECS")
                .unwrap_or_else(|_| "30".to_string())
                .parse()
                .expect("RPC_REQUEST_TIMEOUT_SECS must be a number"),
            allowed_origins,
            rate_limit_per_minute: env::var("RATE_LIMIT_PER_MINUTE")
                .unwrap_or_else(|_| "60".to_string())
                .parse()
                .expect("RATE_LIMIT_PER_MINUTE must be a positive integer"),
            indexer_lag_warn_threshold: env::var("INDEXER_LAG_WARN_THRESHOLD")
                .unwrap_or_else(|_| "100".to_string())
                .parse()
                .expect("INDEXER_LAG_WARN_THRESHOLD must be a number"),
            indexer_stall_timeout_secs: env::var("INDEXER_STALL_TIMEOUT_SECS")
                .unwrap_or_else(|_| "60".to_string())
                .parse()
                .expect("INDEXER_STALL_TIMEOUT_SECS must be a number"),
            db_statement_timeout_ms: env::var("DB_STATEMENT_TIMEOUT_MS")
                .unwrap_or_else(|_| "5000".to_string())
                .parse()
                .expect("DB_STATEMENT_TIMEOUT_MS must be a number"),
            indexer_poll_interval_ms: {
                let v: u64 = env::var("INDEXER_POLL_INTERVAL_MS")
                    .unwrap_or_else(|_| "5000".to_string())
                    .parse()
                    .expect("INDEXER_POLL_INTERVAL_MS must be a number");
                assert!((100..=60000).contains(&v),
                    "INDEXER_POLL_INTERVAL_MS must be between 100 and 60000 ms, got {v}");
                v
            },
            indexer_error_backoff_ms: {
                let v: u64 = env::var("INDEXER_ERROR_BACKOFF_MS")
                    .unwrap_or_else(|_| "10000".to_string())
                    .parse()
                    .expect("INDEXER_ERROR_BACKOFF_MS must be a number");
                assert!((100..=60000).contains(&v),
                    "INDEXER_ERROR_BACKOFF_MS must be between 100 and 60000 ms, got {v}");
                v
            },
            sse_keepalive_interval_ms: {
                let v: u64 = env::var("SSE_KEEPALIVE_INTERVAL_MS")
                    .unwrap_or_else(|_| "15000".to_string())
                    .parse()
                    .expect("SSE_KEEPALIVE_INTERVAL_MS must be a number");
                assert!((1000..=60000).contains(&v),
                    "SSE_KEEPALIVE_INTERVAL_MS must be between 1000 and 60000 ms, got {v}");
                v
            },
            sse_max_connections: {
                let v: usize = env::var("SSE_MAX_CONNECTIONS")
                    .unwrap_or_else(|_| "1000".to_string())
                    .parse()
                    .expect("SSE_MAX_CONNECTIONS must be a number");
                assert!(v > 0, "SSE_MAX_CONNECTIONS must be greater than 0, got {v}");
                v
            },
            environment,
            max_body_size_bytes: env::var("MAX_BODY_SIZE_BYTES")
                .unwrap_or_else(|_| (1024 * 1024).to_string())
                .parse()
                .expect("MAX_BODY_SIZE_BYTES must be a number"),
            event_retention_days: env::var("EVENT_RETENTION_DAYS")
                .unwrap_or_else(|_| "90".to_string())
                .parse()
                .expect("EVENT_RETENTION_DAYS must be a non-negative integer"),
            pruning_interval_hours: env::var("PRUNING_INTERVAL_HOURS")
                .unwrap_or_else(|_| "24".to_string())
                .parse()
                .expect("PRUNING_INTERVAL_HOURS must be a positive integer"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;

    #[test]
    fn test_environment_from_str() {
        assert_eq!(Environment::from_str("production"), Environment::Production);
        assert_eq!(Environment::from_str("prod"), Environment::Production);
        assert_eq!(Environment::from_str("staging"), Environment::Staging);
        assert_eq!(Environment::from_str("stage"), Environment::Staging);
        assert_eq!(Environment::from_str("development"), Environment::Development);
        assert_eq!(Environment::from_str("dev"), Environment::Development);
        assert_eq!(Environment::from_str("unknown"), Environment::Development);
    }

    #[test]
    fn test_environment_is_production_like() {
        assert!(!Environment::Development.is_production_like());
        assert!(Environment::Staging.is_production_like());
        assert!(Environment::Production.is_production_like());
    }

    #[test]
    fn test_indexer_state_new() {
        let state = IndexerState::new();
        assert_eq!(state.current_ledger.load(std::sync::atomic::Ordering::SeqCst), 0);
        assert_eq!(state.latest_ledger.load(std::sync::atomic::Ordering::SeqCst), 0);
        assert!(state.started_at > 0);
    }

    #[test]
    fn test_indexer_state_uptime() {
        let state = IndexerState::new();
        let uptime1 = state.uptime_secs();
        std::thread::sleep(std::time::Duration::from_millis(10));
        let uptime2 = state.uptime_secs();
        assert!(uptime2 >= uptime1);
    }

    #[test]
    fn test_health_state_new() {
        let health_state = HealthState::new(60);
        assert_eq!(health_state.indexer_stall_timeout_secs, 60);
        assert_eq!(health_state.last_indexer_poll.load(std::sync::atomic::Ordering::SeqCst), 0);
    }

    #[test]
    fn test_health_state_update_and_check() {
        let health_state = HealthState::new(60);
        
        // Initially stalled (no poll ever)
        assert_eq!(health_state.is_indexer_stalled(), Some(0));
        
        // Update poll
        health_state.update_last_poll();
        assert_eq!(health_state.is_indexer_stalled(), None);
        
        // Simulate time passing (can't easily test actual time passage in unit tests)
        // But we can test the logic with a very short timeout
        let health_state_short = HealthState::new(0);
        health_state_short.update_last_poll();
        std::thread::sleep(std::time::Duration::from_millis(10));
        assert!(health_state_short.is_indexer_stalled().is_some());
    }

    #[test]
    fn test_config_default() {
        let config = Config::default();
        assert_eq!(config.database_url, "postgres://localhost/soroban_pulse");
        assert_eq!(config.stellar_rpc_url, "https://soroban-testnet.stellar.org");
        assert_eq!(config.port, 3000);
        assert_eq!(config.start_ledger, 0);
        assert!(!config.start_ledger_fallback);
        assert_eq!(config.environment, Environment::Development);
    }

    #[test]
    fn test_config_safe_db_url() {
        let mut config = Config::default();
        config.database_url = "postgres://user:password@localhost/db".to_string();
        let safe_url = config.safe_db_url();
        assert!(!safe_url.contains("password"));
        assert!(safe_url.contains("localhost"));
    }

    #[test]
    fn test_config_safe_db_url_unparseable() {
        let mut config = Config::default();
        config.database_url = "not-a-url".to_string();
        assert_eq!(config.safe_db_url(), "<unparseable>");
    }
}
