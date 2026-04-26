use sqlx::{postgres::PgPoolOptions, Executor, PgPool};
use tracing::info;

/// Per-endpoint query timeout configuration (in milliseconds)
pub struct QueryTimeouts {
    pub fast_lookup: u64,      // Simple lookups by ID/hash (e.g., 1000ms)
    pub standard_query: u64,   // Standard paginated queries (e.g., 5000ms)
    pub expensive_query: u64,  // Expensive queries like COUNT(*) (e.g., 10000ms)
}

impl Default for QueryTimeouts {
    fn default() -> Self {
        Self {
            fast_lookup: 1000,
            standard_query: 5000,
            expensive_query: 10000,
        }
    }
}

pub async fn create_pool(
    database_url: &str,
    db_max_connections: u32,
    db_min_connections: u32,
    db_statement_timeout_ms: u64,
) -> Result<PgPool, sqlx::Error> {
    info!(
        min_connections = db_min_connections,
        max_connections = db_max_connections,
        statement_timeout_ms = db_statement_timeout_ms,
        "Configuring Postgres connection pool"
    );

    PgPoolOptions::new()
        .max_connections(db_max_connections)
        .min_connections(db_min_connections)
        .after_connect(move |conn, _| {
            Box::pin(async move {
                conn.execute(
                    format!("SET statement_timeout = '{db_statement_timeout_ms}ms'").as_str(),
                )
                .await
                .map(|_| ())
            })
        })
        .connect(database_url)
        .await
}

/// Helper to set per-query timeout using SET LOCAL statement_timeout
/// This should be called at the beginning of a transaction
pub async fn set_query_timeout(
    conn: &mut sqlx::PgConnection,
    timeout_ms: u64,
) -> Result<(), sqlx::Error> {
    sqlx::query(&format!("SET LOCAL statement_timeout = '{timeout_ms}ms'"))
        .execute(&mut *conn)
        .await
        .map(|_| ())
}

/// Runs migrations under a Postgres session-level advisory lock so that
/// concurrent replicas starting simultaneously do not race each other.
/// The lock is always released — even if migration fails.
pub async fn run_migrations(pool: &PgPool) -> Result<(), sqlx::migrate::MigrateError> {
    const MIGRATION_LOCK_ID: i64 = 0xD0C0_1234_i64; // arbitrary stable key

    let mut conn = pool.acquire().await.map_err(sqlx::migrate::MigrateError::from)?;

    sqlx::query("SELECT pg_advisory_lock($1)")
        .bind(MIGRATION_LOCK_ID)
        .execute(&mut *conn)
        .await
        .map_err(sqlx::migrate::MigrateError::from)?;

    let result = sqlx::migrate!("./migrations").run(&mut *conn).await;

    // Always release — ignore unlock errors so the migration result is returned.
    let _ = sqlx::query("SELECT pg_advisory_unlock($1)")
        .bind(MIGRATION_LOCK_ID)
        .execute(&mut *conn)
        .await;

    result
}
