use axum::{extract::{Path, Query, State}, Json, response::IntoResponse, http::StatusCode};
use axum::response::sse::{Event, KeepAlive, Sse};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use futures::stream::{self, Stream, StreamExt};
use serde_json::{json, Value};
use sqlx::Row;
use std::convert::Infallible;
use std::time::Duration;
use std::sync::OnceLock;
use tokio::sync::Mutex;
use uuid::Uuid;
use chrono::{DateTime, Utc};

use std::sync::atomic::Ordering;
use crate::{error::AppError, models::{self, ContractSummary, PaginationParams, StreamParams}, routes::AppState};

/// Simple in-process cache entry for the contracts list.
struct CacheEntry {
    data: Value,
    expires_at: std::time::Instant,
}

static CONTRACTS_CACHE: OnceLock<Mutex<Option<CacheEntry>>> = OnceLock::new();

fn contracts_cache() -> &'static Mutex<Option<CacheEntry>> {
    CONTRACTS_CACHE.get_or_init(|| Mutex::new(None))
}

/// Encode a (ledger, id) pair as an opaque URL-safe base64 cursor.
fn encode_cursor(ledger: i64, id: Uuid) -> String {
    URL_SAFE_NO_PAD.encode(format!("{ledger}:{id}"))
}

/// Decode a cursor back to (ledger, id). Returns a validation error on malformed input.
fn decode_cursor(cursor: &str) -> Result<(i64, Uuid), AppError> {
    let bytes = URL_SAFE_NO_PAD
        .decode(cursor)
        .map_err(|_| AppError::Validation("invalid cursor".to_string()))?;
    let s = std::str::from_utf8(&bytes)
        .map_err(|_| AppError::Validation("invalid cursor".to_string()))?;
    let (ledger_str, id_str) = s
        .split_once(':')
        .ok_or_else(|| AppError::Validation("invalid cursor".to_string()))?;
    let ledger = ledger_str
        .parse::<i64>()
        .map_err(|_| AppError::Validation("invalid cursor".to_string()))?;
    let id = Uuid::parse_str(id_str)
        .map_err(|_| AppError::Validation("invalid cursor".to_string()))?;
    Ok((ledger, id))
}

/// Map sqlx rows to a JSON array, projecting only the requested columns.
fn rows_to_json(rows: &[sqlx::postgres::PgRow], columns: &[&str]) -> Result<Vec<Value>, AppError> {
    let mut events = Vec::with_capacity(rows.len());
    for row in rows {
        let mut event = serde_json::Map::new();
        for &col in columns {
            match col {
                "id" => { event.insert(col.to_string(), json!(row.try_get::<Uuid, _>(col)?)); }
                "contract_id" => { event.insert(col.to_string(), json!(row.try_get::<String, _>(col)?)); }
                "event_type" => { event.insert(col.to_string(), json!(row.try_get::<String, _>(col)?)); }
                "tx_hash" => { event.insert(col.to_string(), json!(row.try_get::<String, _>(col)?)); }
                "ledger" => { event.insert(col.to_string(), json!(row.try_get::<i64, _>(col)?)); }
                "timestamp" => { event.insert(col.to_string(), json!(row.try_get::<DateTime<Utc>, _>(col)?)); }
                "event_data" => { event.insert(col.to_string(), row.try_get::<Value, _>(col)?); }
                "created_at" => { event.insert(col.to_string(), json!(row.try_get::<DateTime<Utc>, _>(col)?)); }
                _ => {}
            }
        }
        events.push(Value::Object(event));
    }
    Ok(events)
}

/// Resolve requested columns or return a 400 with unknown field names.
fn resolve_columns<'a>(params: &'a PaginationParams) -> Result<Vec<&'a str>, AppError> {
    params.columns().map_err(|(unknown, allowed)| {
        AppError::Validation(format!(
            "unknown fields: [{}]; valid fields are: [{}]",
            unknown.join(", "),
            allowed.join(", ")
        ))
    })
}

fn validate_contract_id(contract_id: &str) -> Result<(), AppError> {
    if contract_id.len() != 56 {
        return Err(AppError::Validation("invalid contract_id format".to_string()));
    }
    if !contract_id.starts_with('C') {
        return Err(AppError::Validation("invalid contract_id format".to_string()));
    }
    if !contract_id.chars().all(|c| c.is_ascii_alphanumeric()) {
        return Err(AppError::Validation("invalid contract_id format".to_string()));
    }
    Ok(())
}

fn validate_tx_hash(tx_hash: &str) -> Result<(), AppError> {
    if tx_hash.len() != 64 {
        return Err(AppError::Validation("invalid tx_hash format".to_string()));
    }
    // Accept both uppercase and lowercase hex — callers should normalize to lowercase first.
    if !tx_hash.chars().all(|c| c.is_ascii_hexdigit()) {
        return Err(AppError::Validation("invalid tx_hash format".to_string()));
    }
    Ok(())
}

async fn build_health_response(state: &AppState) -> (StatusCode, Value) {
    let mut db_ok = true;
    let db_status: &str;

    let timeout = Duration::from_millis(state.health_check_timeout_ms);

    let db_check = tokio::time::timeout(
        timeout,
        sqlx::query("SELECT 1").fetch_one(&state.pool),
    )
    .await;

    match db_check {
        Ok(Ok(_)) => {
            db_status = "ok";
        }
        Ok(Err(sqlx::Error::PoolTimedOut)) => {
            db_ok = false;
            db_status = "pool_exhausted";
        }
        Ok(Err(_)) => {
            db_ok = false;
            db_status = "unreachable";
        }
        Err(_) => {
            // tokio timeout elapsed
            db_ok = false;
            db_status = "unreachable";
        }
    }

    // Check indexer status
    let indexer_status = if let Some(secs_ago) = state.health_state.is_indexer_stalled() {
        json!({
            "indexer": "stalled",
            "last_poll_secs_ago": secs_ago
        })
    } else {
        json!({"indexer": "ok"})
    };

    // Determine overall status
    let is_degraded = !db_ok || indexer_status.get("indexer").and_then(|v| v.as_str()) == Some("stalled");

    if is_degraded {
        let response = json!({
            "status": "degraded",
            "db": db_status,
        });
        // Merge indexer status
        let mut obj = serde_json::to_value(response).unwrap();
        if let Value::Object(ref mut map) = obj {
            if let Value::Object(indexer_map) = indexer_status {
                map.extend(indexer_map);
            }
        }
        (StatusCode::SERVICE_UNAVAILABLE, obj)
    } else {
        let response = json!({
            "status": "ok",
            "db": "ok",
            "indexer": "ok"
        });
        (StatusCode::OK, response)
    }
}

#[utoipa::path(
    get,
    path = "/health",
    tag = "system",
    responses(
        (status = 200, description = "Service is healthy"),
        (status = 503, description = "Service is degraded"),
    )
)]
pub async fn health(State(state): State<AppState>) -> (StatusCode, Json<Value>) {
    let (status, body) = build_health_response(&state).await;
    (status, Json(body))
}

#[utoipa::path(
    get,
    path = "/healthz/live",
    tag = "system",
    responses(
        (status = 200, description = "Process is alive"),
    )
)]
pub async fn health_live() -> (StatusCode, Json<Value>) {
    (StatusCode::OK, Json(json!({ "status": "alive" })))
}

#[utoipa::path(
    get,
    path = "/healthz/ready",
    tag = "system",
    responses(
        (status = 200, description = "Service is ready"),
        (status = 503, description = "Service is not ready"),
    )
)]
pub async fn health_ready(State(state): State<AppState>) -> (StatusCode, Json<Value>) {
    let (status, body) = build_health_response(&state).await;
    (status, Json(body))
}

#[utoipa::path(
    get,
    path = "/status",
    tag = "system",
    responses(
        (status = 200, description = "Indexer operational status"),
    )
)]
pub async fn status(State(state): State<AppState>) -> Json<Value> {
    let current_ledger = state.indexer_state.current_ledger.load(Ordering::Relaxed);
    let latest_ledger = state.indexer_state.latest_ledger.load(Ordering::Relaxed);
    let lag_ledgers = latest_ledger.saturating_sub(current_ledger);
    let uptime_secs = state.indexer_state.uptime_secs();

    let indexer_status = if state.health_state.is_indexer_stalled().is_some() {
        "stalled"
    } else {
        "running"
    };

    let indexer_mode = if state.indexer_state.is_active_indexer.load(Ordering::Relaxed) {
        "active"
    } else {
        "read_only"
    };

    let total_events: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM events")
        .fetch_one(&state.pool)
        .await
        .unwrap_or(0);

    // Query event counts by type
    let events_by_type_rows: Vec<(String, i64)> = sqlx::query_as(
        "SELECT event_type, COUNT(*) as count FROM events GROUP BY event_type"
    )
        .fetch_all(&state.pool)
        .await
        .unwrap_or_default();

    // Build events_by_type object with all event types (defaulting to 0 if not present)
    let mut events_by_type = serde_json::json!({
        "contract": 0i64,
        "diagnostic": 0i64,
        "system": 0i64,
    });

    for (event_type, count) in events_by_type_rows {
        if let Some(obj) = events_by_type.as_object_mut() {
            obj.insert(event_type, serde_json::json!(count));
        }
    }

    Json(json!({
        "version": env!("CARGO_PKG_VERSION"),
        "uptime_secs": uptime_secs,
        "current_ledger": current_ledger,
        "latest_ledger": latest_ledger,
        "lag_ledgers": lag_ledgers,
        "total_events": total_events,
        "events_by_type": events_by_type,
        "indexer_status": indexer_status,
        "indexer_mode": indexer_mode,
    }))
}

pub async fn metrics(State(state): State<AppState>) -> impl IntoResponse {
    crate::metrics::update_db_pool_metrics(&state.pool);
    state.prometheus_handle.render()
}

/// Serve the raw OpenAPI JSON spec.
pub async fn openapi_json() -> impl IntoResponse {
    use crate::routes::ApiDoc;
    use utoipa::OpenApi;
    Json(ApiDoc::openapi())
}

/// Serve a minimal Swagger UI HTML page.
pub async fn swagger_ui() -> impl IntoResponse {
    axum::response::Html(
        "<!DOCTYPE html><html><head><title>Soroban Pulse API</title>\
        <meta charset=\"utf-8\"/>\
        <link rel=\"stylesheet\" href=\"https://unpkg.com/swagger-ui-dist@5/swagger-ui.css\"></head>\
        <body><div id=\"swagger-ui\"></div>\
        <script src=\"https://unpkg.com/swagger-ui-dist@5/swagger-ui-bundle.js\"></script>\
        <script>SwaggerUIBundle({url:\"/openapi.json\",dom_id:\"#swagger-ui\"})</script>\
        </body></html>"
    )
}

/// Stream new events in real time via Server-Sent Events.
#[utoipa::path(
    get,
    path = "/v1/events/stream",
    tag = "events",
    params(
        ("contract_id" = Option<String>, Query, description = "Filter by contract ID"),
    ),
    responses(
        (status = 200, description = "SSE stream of new events (text/event-stream)"),
        (status = 400, description = "Invalid contract_id format"),
    )
)]
pub async fn stream_events(
    State(state): State<AppState>,
    Query(params): Query<StreamParams>,
    headers: axum::http::HeaderMap,
) -> Result<Sse<impl Stream<Item = Result<Event, Infallible>>>, (StatusCode, Json<Value>)> {
    // Check if we've reached the max SSE connections limit
    let current_connections = state.sse_connections.load(std::sync::atomic::Ordering::Relaxed);
    if current_connections >= state.sse_max_connections {
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({
                "error": "too many SSE connections",
                "code": "SSE_LIMIT_EXCEEDED"
            }))
        ));
    }

    // Increment connection counter
    state.sse_connections.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let new_count = state.sse_connections.load(std::sync::atomic::Ordering::Relaxed);
    crate::metrics::update_sse_connections(new_count);
    
    let keepalive_ms = state.sse_keepalive_interval_ms;
    let contract_filter = params.contract_id;
    let sse_connections = state.sse_connections.clone();

    // Validate contract_id if provided
    if let Some(ref cid) = contract_filter {
        validate_contract_id(cid).map_err(|e| {
            let body = json!({ "error": e.to_string(), "code": "VALIDATION_ERROR" });
            (StatusCode::BAD_REQUEST, Json(body))
        })?;
    }

    // Replay missed events if the client sends Last-Event-ID (a UUID).
    let last_event_id = headers
        .get("last-event-id")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| Uuid::parse_str(s).ok());

    let replay: Vec<crate::models::Event> = if let Some(last_id) = last_event_id {
        let q = if let Some(ref cid) = contract_filter {
            sqlx::query_as::<_, crate::models::Event>(
                "SELECT id, contract_id, event_type, tx_hash, ledger, timestamp, event_data, created_at, 0::bigint AS total_count \
                 FROM events WHERE created_at > (SELECT created_at FROM events WHERE id = $1) \
                 AND contract_id = $2 ORDER BY created_at ASC",
            )
            .bind(last_id)
            .bind(cid)
            .fetch_all(&state.pool)
            .await
        } else {
            sqlx::query_as::<_, crate::models::Event>(
                "SELECT id, contract_id, event_type, tx_hash, ledger, timestamp, event_data, created_at, 0::bigint AS total_count \
                 FROM events WHERE created_at > (SELECT created_at FROM events WHERE id = $1) \
                 ORDER BY created_at ASC",
            )
            .bind(last_id)
            .fetch_all(&state.pool)
            .await
        };
        q.unwrap_or_default()
    } else {
        vec![]
    };

    let rx = state.event_tx.subscribe();

    let replay_stream = stream::iter(replay.into_iter().map(move |ev| {
        let data = serde_json::to_string(&ev).unwrap_or_default();
        Ok(Event::default()
            .id(ev.id.to_string())
            .retry(Duration::from_millis(keepalive_ms))
            .data(data))
    }));

    let live_stream = stream::unfold(
        (rx, contract_filter, keepalive_ms),
        move |(mut rx, filter, ka)| async move {
            loop {
                match rx.recv().await {
                    Ok(event) => {
                        if let Some(ref cid) = filter {
                            if &event.contract_id != cid {
                                continue;
                            }
                        }
                        let data = serde_json::to_string(&event).unwrap_or_default();
                        let sse = Event::default()
                            .id(format!("{}-{}", event.tx_hash, event.ledger))
                            .retry(Duration::from_millis(ka))
                            .data(data);
                        return Some((Ok(sse), (rx, filter, ka)));
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => return None,
                }
            }
        },
    );

    let combined = replay_stream.chain(live_stream);

    // Wrap the stream to decrement the connection counter when the stream ends
    let stream_with_cleanup = stream::unfold(
        (Box::pin(combined), sse_connections.clone()),
        move |(mut stream, counter)| async move {
            match stream.next().await {
                Some(item) => Some((item, (stream, counter))),
                None => {
                    let new_count = counter.fetch_sub(1, std::sync::atomic::Ordering::Relaxed) - 1;
                    crate::metrics::update_sse_connections(new_count);
                    None
                }
            }
        }
    );

    Ok(Sse::new(stream_with_cleanup).keep_alive(
        KeepAlive::new()
            .interval(Duration::from_millis(keepalive_ms))
            .text("ping"),
    ))
}

/// Converts an `Event` to a JSON object containing only the requested fields.
fn filter_fields(event: &models::Event, columns: &[&str]) -> Value {
    let mut map = serde_json::Map::new();
    for &col in columns {
        match col {
            "id"          => { map.insert(col.to_string(), json!(event.id)); }
            "contract_id" => { map.insert(col.to_string(), json!(event.contract_id)); }
            "event_type"  => { map.insert(col.to_string(), json!(event.event_type)); }
            "tx_hash"     => { map.insert(col.to_string(), json!(event.tx_hash)); }
            "ledger"      => { map.insert(col.to_string(), json!(event.ledger)); }
            "timestamp"   => { map.insert(col.to_string(), json!(event.timestamp)); }
            "event_data"  => { map.insert(col.to_string(), event.event_data.clone()); }
            "created_at"  => { map.insert(col.to_string(), json!(event.created_at)); }
            _ => {}
        }
    }
    Value::Object(map)
}

#[utoipa::path(
    get,
    path = "/v1/events",
    tag = "events",
    params(
        ("page" = Option<i64>, Query, description = "Page number (default: 1)"),
        ("limit" = Option<i64>, Query, description = "Results per page, 1–100 (default: 20)"),
        ("exact_count" = Option<bool>, Query, description = "Use exact COUNT(*) instead of approximate"),
        ("event_type" = Option<EventType>, Query, description = "Filter by event type: contract, diagnostic, system"),
        ("from_ledger" = Option<i64>, Query, description = "Return events at or after this ledger"),
        ("to_ledger" = Option<i64>, Query, description = "Return events at or before this ledger"),
    ),
    responses(
        (status = 200, description = "Paginated list of events"),
        (status = 400, description = "Invalid query parameters"),
    )
)]

pub async fn get_events(
    State(state): State<AppState>,
    Query(params): Query<PaginationParams>,
) -> Result<Json<Value>, AppError> {
    // Validate ledger range
    if let (Some(from), Some(to)) = (params.from_ledger, params.to_ledger) {
        if from > to {
            return Err(AppError::Validation(
                "from_ledger must be <= to_ledger".to_string(),
            ));
        }
    }

    let limit = params.limit();
    let columns = resolve_columns(&params)?;

    // Cursor-based path
    if let Some(ref cursor_str) = params.cursor {
        let (cursor_ledger, cursor_id) = decode_cursor(cursor_str)?;

        let mut conditions: Vec<String> = vec![
            format!("(ledger, id) < ($1, $2)")
        ];
        let mut bind_idx: i32 = 3;

        if params.event_type.is_some() {
            conditions.push(format!("event_type = ${bind_idx}"));
            bind_idx += 1;
        }
        if params.from_ledger.is_some() {
            conditions.push(format!("ledger >= ${bind_idx}"));
            bind_idx += 1;
        }
        if params.to_ledger.is_some() {
            conditions.push(format!("ledger <= ${bind_idx}"));
            bind_idx += 1;
        }

        let where_clause = format!("WHERE {}", conditions.join(" AND "));

        // Always fetch ledger + id so we can build next_cursor; merge with requested columns.
        let mut select_cols = columns.to_vec();
        if !select_cols.contains(&"ledger") { select_cols.push("ledger"); }
        if !select_cols.contains(&"id") { select_cols.push("id"); }

        let query_str = format!(
            "SELECT {} FROM events {} ORDER BY ledger DESC, id DESC LIMIT ${}",
            select_cols.join(", "),
            where_clause,
            bind_idx,
        );

        let mut q = sqlx::query(&query_str)
            .bind(cursor_ledger)
            .bind(cursor_id);
        if let Some(ref et) = params.event_type { q = q.bind(et); }
        if let Some(fl) = params.from_ledger { q = q.bind(fl); }
        if let Some(tl) = params.to_ledger { q = q.bind(tl); }
        q = q.bind(limit);

        let rows = q.fetch_all(&state.pool).await?;

        let has_more = rows.len() as i64 == limit;
        let next_cursor = if has_more {
            let last = rows.last().unwrap();
            let last_ledger: i64 = last.try_get("ledger")?;
            let last_id: Uuid = last.try_get("id")?;
            Some(encode_cursor(last_ledger, last_id))
        } else {
            None
        };

        let events = rows_to_json(&rows, &columns)?;

        return Ok(Json(json!({
            "data": events,
            "next_cursor": next_cursor,
            "limit": limit,
        })));
    }

    // Offset-based path (deprecated fallback)
    let offset = params.offset();
    let exact = params.exact_count.unwrap_or(false);

    let mut conditions: Vec<String> = Vec::new();
    let mut bind_idx: i32 = 1;

    if params.event_type.is_some() {
        conditions.push(format!("event_type = ${bind_idx}"));
        bind_idx += 1;
    }
    if params.from_ledger.is_some() {
        conditions.push(format!("ledger >= ${bind_idx}"));
        bind_idx += 1;
    }
    if params.to_ledger.is_some() {
        conditions.push(format!("ledger <= ${bind_idx}"));
        bind_idx += 1;
    }

    let where_clause = if conditions.is_empty() {
        String::new()
    } else {
        format!("WHERE {}", conditions.join(" AND "))
    };

    // Always include ledger + id so we can emit next_cursor even in offset mode.
    let mut select_cols = columns.to_vec();
    if !select_cols.contains(&"ledger") { select_cols.push("ledger"); }
    if !select_cols.contains(&"id") { select_cols.push("id"); }

    let query_str = format!(
        "SELECT {} FROM events {} ORDER BY ledger DESC, id DESC LIMIT ${} OFFSET ${}",
        select_cols.join(", "),
        where_clause,
        bind_idx,
        bind_idx + 1,
    );

    let mut q = sqlx::query(&query_str);
    if let Some(ref et) = params.event_type { q = q.bind(et); }
    if let Some(fl) = params.from_ledger { q = q.bind(fl); }
    if let Some(tl) = params.to_ledger { q = q.bind(tl); }
    q = q.bind(limit).bind(offset);

    let rows = q.fetch_all(&state.pool).await?;

    let has_more = rows.len() as i64 == limit;
    let next_cursor = if has_more {
        let last = rows.last().unwrap();
        let last_ledger: i64 = last.try_get("ledger")?;
        let last_id: Uuid = last.try_get("id")?;
        Some(encode_cursor(last_ledger, last_id))
    } else {
        None
    };

    let events = rows_to_json(&rows, &columns)?;

    let (total, approximate): (i64, bool) = if exact {
        let count_str = format!("SELECT COUNT(*) FROM events {}", where_clause);
        let mut cq = sqlx::query_scalar::<_, i64>(&count_str);
        if let Some(ref et) = params.event_type { cq = cq.bind(et); }
        if let Some(fl) = params.from_ledger { cq = cq.bind(fl); }
        if let Some(tl) = params.to_ledger { cq = cq.bind(tl); }
        let count = cq.fetch_one(&state.pool).await?;
        (count, false)
    } else {
        let count: i64 = sqlx::query_scalar(
            "SELECT reltuples::bigint FROM pg_class WHERE relname = 'events'",
        )
        .fetch_one(&state.pool)
        .await?;
        (count, true)
    };

    Ok(Json(json!({
        "data": events,
        "next_cursor": next_cursor,
        "total": total,
        "page": params.page.unwrap_or(1),
        "limit": limit,
        "approximate": approximate,
        "pagination": "offset — migrate to cursor parameter for better performance",
    })))
}

#[utoipa::path(
    get,
    path = "/v1/events/contract/{contract_id}",
    tag = "events",
    params(
        ("contract_id" = String, Path, description = "Stellar contract ID (56-char, starts with C)"),
        ("page" = Option<i64>, Query, description = "Page number (default: 1)"),
        ("limit" = Option<i64>, Query, description = "Results per page, 1–100 (default: 20)"),
        ("from_ledger" = Option<i64>, Query, description = "Return events at or after this ledger"),
        ("to_ledger" = Option<i64>, Query, description = "Return events at or before this ledger"),
    ),
    responses(
        (status = 200, description = "Events for the given contract"),
        (status = 400, description = "Invalid contract_id format or ledger range"),
        (status = 404, description = "No events found for contract"),
    )
)]
pub async fn get_events_by_contract(
    State(state): State<AppState>,
    Path(contract_id): Path<String>,
    Query(params): Query<PaginationParams>,
) -> Result<Json<Value>, AppError> {
    validate_contract_id(&contract_id)?;

    // Validate ledger range
    if let (Some(from), Some(to)) = (params.from_ledger, params.to_ledger) {
        if from > to {
            return Err(AppError::Validation(
                "from_ledger must be <= to_ledger".to_string(),
            ));
        }
    }

    let limit = params.limit();
    let offset = params.offset();
    let columns = resolve_columns(&params)?;

    // Build query dynamically based on optional ledger filters
    let mut conditions: Vec<String> = vec!["contract_id = $1".to_string()];
    let mut bind_idx: i32 = 2;

    if params.from_ledger.is_some() {
        conditions.push(format!("ledger >= ${bind_idx}"));
        bind_idx += 1;
    }
    if params.to_ledger.is_some() {
        conditions.push(format!("ledger <= ${bind_idx}"));
        bind_idx += 1;
    }

    let where_clause = format!("WHERE {}", conditions.join(" AND "));
    let query_str = format!(
        "SELECT id, contract_id, event_type, tx_hash, ledger, timestamp, event_data, created_at, 0::bigint AS total_count \
         FROM events {} ORDER BY ledger DESC LIMIT ${} OFFSET ${}",
        where_clause, bind_idx, bind_idx + 1,
    );

    let mut q = sqlx::query_as::<_, models::Event>(&query_str).bind(&contract_id);
    if let Some(fl) = params.from_ledger { q = q.bind(fl); }
    if let Some(tl) = params.to_ledger { q = q.bind(tl); }
    q = q.bind(limit).bind(offset);

    let rows = q.fetch_all(&state.pool).await?;

    if rows.is_empty() {
        return Err(AppError::NotFound);
    }

    let events: Vec<Value> = rows.iter().map(|e| filter_fields(e, &columns)).collect();

    let mut response = json!({
        "data": events,
        "contract_id": contract_id,
        "page": params.page.unwrap_or(1),
        "limit": limit,
    });

    // Echo back applied filters for client confirmation
    if let Some(fl) = params.from_ledger {
        response["from_ledger"] = json!(fl);
    }
    if let Some(tl) = params.to_ledger {
        response["to_ledger"] = json!(tl);
    }

    Ok(Json(response))
}

#[utoipa::path(
    get,
    path = "/v1/events/tx/{tx_hash}",
    tag = "events",
    params(
        ("tx_hash" = String, Path, description = "Transaction hash (64 hex chars, case-insensitive — normalized to lowercase)"),
    ),
    responses(
        (status = 200, description = "Events for the given transaction (empty array if none)"),
        (status = 400, description = "Invalid tx_hash format"),
    )
)]
pub async fn get_events_by_tx(
    State(state): State<AppState>,
    Path(tx_hash): Path<String>,
    Query(params): Query<PaginationParams>,
) -> Result<Json<Value>, AppError> {
    // Normalize to lowercase so uppercase/mixed-case hashes from blockchain explorers work.
    let tx_hash = tx_hash.to_lowercase();
    validate_tx_hash(&tx_hash)?;

    let columns = resolve_columns(&params)?;

    let mut select_cols = columns.to_vec();
    if !select_cols.contains(&"ledger") { select_cols.push("ledger"); }
    if !select_cols.contains(&"id") { select_cols.push("id"); }

    let query_str = format!(
        "SELECT {} FROM events WHERE tx_hash = $1 ORDER BY ledger DESC, id DESC",
        select_cols.join(", "),
    );

    let rows = sqlx::query(&query_str)
        .bind(&tx_hash)
        .fetch_all(&state.pool)
        .await?;

    let events = rows_to_json(&rows, &columns)?;

    Ok(Json(json!({ "data": events, "tx_hash": tx_hash })))
}

#[utoipa::path(
    get,
    path = "/v1/contracts",
    tag = "events",
    params(
        ("page" = Option<i64>, Query, description = "Page number (default 1)"),
        ("limit" = Option<i64>, Query, description = "Items per page (1-100, default 20)"),
    ),
    responses(
        (status = 200, description = "Paginated list of indexed contract IDs"),
    )
)]
pub async fn get_contracts(
    State(state): State<AppState>,
    Query(params): Query<PaginationParams>,
) -> Result<Json<Value>, AppError> {
    let limit = params.limit();
    let offset = params.offset();

    // Check cache
    {
        let cache = contracts_cache().lock().await;
        if let Some(ref entry) = *cache {
            if entry.expires_at > std::time::Instant::now() {
                return Ok(Json(entry.data.clone()));
            }
        }
    }

    let rows = sqlx::query_as::<_, ContractSummary>(
        "SELECT contract_id, COUNT(*) AS event_count, MAX(ledger) AS latest_ledger \
         FROM events GROUP BY contract_id ORDER BY latest_ledger DESC \
         LIMIT $1 OFFSET $2",
    )
    .bind(limit)
    .bind(offset)
    .fetch_all(&state.pool)
    .await?;

    let total: i64 = sqlx::query_scalar("SELECT COUNT(DISTINCT contract_id) FROM events")
        .fetch_one(&state.pool)
        .await?;

    let result = json!({
        "data": rows,
        "total": total,
        "page": params.page.unwrap_or(1),
        "limit": limit,
    });

    // Store in cache with 30-second TTL
    {
        let mut cache = contracts_cache().lock().await;
        *cache = Some(CacheEntry {
            data: result.clone(),
            expires_at: std::time::Instant::now() + Duration::from_secs(30),
        });
    }

    Ok(Json(result))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::{to_bytes, Body};
    use axum::http::{Request, StatusCode};
    use chrono::Utc;
    use sqlx::PgPool;
    use std::sync::Arc;
    use tower::ServiceExt;
    use crate::config::{HealthState, IndexerState};

    fn create_test_router(pool: PgPool) -> axum::Router {
        let health_state = Arc::new(HealthState::new(60));
        let indexer_state = Arc::new(IndexerState::new());
        let prometheus_handle = crate::metrics::init_metrics();
        crate::routes::create_router(pool, vec![], &[], 60, health_state, indexer_state, prometheus_handle, 2000)
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn get_events_by_tx_no_events_returns_200_empty_data(pool: PgPool) {
        let app = create_test_router(pool);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/events/tx/unknown_tx_hash_no_events_deadbeef")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["data"], json!([]));
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn get_events_by_contract_no_events_returns_200_empty_data(pool: PgPool) {
        let app = create_test_router(pool);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/events/contract/unknown_contract_no_events_deadbeef")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["data"], json!([]));
        assert_eq!(v["contract_id"], json!("unknown_contract_no_events_deadbeef"));
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn get_events_by_tx_with_row_returns_200_with_data(pool: PgPool) {
        let tx_hash = "a1b2c3d4e5f6";
        sqlx::query(
            r#"
            INSERT INTO events (contract_id, event_type, tx_hash, ledger, timestamp, event_data)
            VALUES ($1, $2, $3, $4, $5, $6)
            "#,
        )
        .bind("C_TEST")
        .bind("contract")
        .bind(tx_hash)
        .bind(1_i64)
        .bind(Utc::now())
        .bind(json!({ "value": null, "topic": null }))
        .execute(&pool)
        .await
        .unwrap();

        let app = create_test_router(pool);

        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!("/v1/events/tx/{tx_hash}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert!(v["data"].is_array());
        assert_eq!(v["data"].as_array().unwrap().len(), 1);
        assert_eq!(v["tx_hash"], json!(tx_hash));
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn database_error_response_does_not_leak_internals(pool: PgPool) {
        let app = create_test_router(pool);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/events?limit=invalid")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body_str = String::from_utf8(body.to_vec()).unwrap();
        
        // Verify response contains generic error message
        assert!(body_str.contains("internal server error"));
        
        // Verify no SQLx internals are leaked
        assert!(!body_str.to_lowercase().contains("sqlx"));
        assert!(!body_str.contains("events"));
        assert!(!body_str.contains("table"));
        assert!(!body_str.contains("column"));
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn contract_id_too_long_returns_400(pool: PgPool) {
        let app = create_test_router(pool);
        let long_id = "C".repeat(100);

        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!("/v1/events/{}", long_id))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["error"], "invalid contract_id format");
        assert_eq!(v["code"], "VALIDATION_ERROR");
        assert!(v["correlation_id"].as_str().is_some());
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn contract_id_invalid_format_returns_400(pool: PgPool) {
        let app = create_test_router(pool);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/events/GABC123456789012345678901234567890123456789012345678")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["error"], "invalid contract_id format");
        assert_eq!(v["code"], "VALIDATION_ERROR");
        assert!(v["correlation_id"].as_str().is_some());
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn tx_hash_invalid_length_returns_400(pool: PgPool) {
        let app = create_test_router(pool);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/events/tx/abc123")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["error"], "invalid tx_hash format");
        assert_eq!(v["code"], "VALIDATION_ERROR");
        assert!(v["correlation_id"].as_str().is_some());
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn tx_hash_non_hex_returns_400(pool: PgPool) {
        let app = create_test_router(pool);
        let invalid_hex = "z".repeat(64);

        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!("/v1/events/tx/{}", invalid_hex))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["error"], "invalid tx_hash format");
        assert_eq!(v["code"], "VALIDATION_ERROR");
        assert!(v["correlation_id"].as_str().is_some());
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn tx_hash_uppercase_hex_is_normalized_to_lowercase(pool: PgPool) {
        // Insert an event with a lowercase tx_hash
        let lowercase_hash = "a".repeat(64);
        sqlx::query(
            "INSERT INTO events (contract_id, event_type, tx_hash, ledger, timestamp, event_data)
             VALUES ($1, $2, $3, $4, $5, $6)",
        )
        .bind("C_TEST_UPPER")
        .bind("contract")
        .bind(&lowercase_hash)
        .bind(1_i64)
        .bind(Utc::now())
        .bind(json!({ "value": null, "topic": null }))
        .execute(&pool)
        .await
        .unwrap();

        let app = create_test_router(pool);

        // Request with uppercase hash — should be normalized and return the same event
        let uppercase_hash = "A".repeat(64);
        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!("/v1/events/tx/{}", uppercase_hash))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert!(v["data"].is_array());
        assert_eq!(v["data"].as_array().unwrap().len(), 1);
        // Response tx_hash should be the normalized lowercase form
        assert_eq!(v["tx_hash"], json!(lowercase_hash));
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn tx_hash_mixed_case_is_normalized_to_lowercase(pool: PgPool) {
        let lowercase_hash = "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2";
        sqlx::query(
            "INSERT INTO events (contract_id, event_type, tx_hash, ledger, timestamp, event_data)
             VALUES ($1, $2, $3, $4, $5, $6)",
        )
        .bind("C_TEST_MIXED")
        .bind("contract")
        .bind(lowercase_hash)
        .bind(2_i64)
        .bind(Utc::now())
        .bind(json!({ "value": null, "topic": null }))
        .execute(&pool)
        .await
        .unwrap();

        let app = create_test_router(pool);

        // Mixed-case version of the same hash
        let mixed_hash = "A1B2C3D4E5F6A1B2C3D4E5F6A1B2C3D4E5F6A1B2C3D4E5F6A1B2C3D4E5F6A1B2";
        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!("/v1/events/tx/{}", mixed_hash))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["data"].as_array().unwrap().len(), 1);
        assert_eq!(v["tx_hash"], json!(lowercase_hash));
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn get_events_paginated_returns_approximate_count_by_default(pool: PgPool) {
        let app = create_test_router(pool);

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
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["approximate"], true);
        assert!(v.get("total").is_some());
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn get_events_paginated_returns_exact_count_when_requested(pool: PgPool) {
        let app = create_test_router(pool);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/events?exact_count=true")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["approximate"], false);
        assert_eq!(v["total"], 0); // Empty table
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn get_events_with_fields_filter_returns_only_requested_fields(pool: PgPool) {
        let app = create_test_router(pool.clone());
        
        // Insert a test row
        sqlx::query(
            "INSERT INTO events (id, contract_id, event_type, tx_hash, ledger, timestamp, event_data)
             VALUES ($1, $2, $3, $4, $5, $6, $7)"
        )
        .bind(Uuid::new_v4())
        .bind("C1234567890123456789012345678901234567890123456789012345")
        .bind("test")
        .bind("a".repeat(64))
        .bind(100_i64)
        .bind(Utc::now())
        .bind(json!({"foo": "bar"}))
        .execute(&pool)
        .await
        .unwrap();

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/events?fields=id,ledger")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        
        let event = &v["data"][0];
        assert!(event.get("id").is_some());
        assert!(event.get("ledger").is_some());
        assert!(event.get("contract_id").is_none());
        assert!(event.get("event_data").is_none());
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn get_events_total_count_scenarios(pool: PgPool) {
        let app = create_test_router(pool.clone());

        // 1. Empty set
        let response = app.clone()
            .oneshot(Request::builder().uri("/v1/events").body(Body::empty()).unwrap())
            .await.unwrap();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["total"], 0);
        assert_eq!(v["data"].as_array().unwrap().len(), 0);

        // 2. Single page (3 events, limit 20)
        for i in 0..3 {
            sqlx::query("INSERT INTO events (contract_id, event_type, tx_hash, ledger, timestamp, event_data) VALUES ($1, $2, $3, $4, $5, $6)")
                .bind(format!("C{:0>55}", i))
                .bind("contract")
                .bind(format!("{:0>64}", i))
                .bind(i as i64)
                .bind(Utc::now())
                .bind(json!({}))
                .execute(&pool).await.unwrap();
        }

        let response = app.clone()
            .oneshot(Request::builder().uri("/v1/events?limit=20").body(Body::empty()).unwrap())
            .await.unwrap();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert!(v["total"].as_u64().is_some()); // Can be approximate or exact
        assert!(v["total"].as_u64().is_some());
        assert_eq!(v["data"].as_array().unwrap().len(), 3);

        // 3. Multi-page (limit 2, total 3)
        let response = app.clone()
            .oneshot(Request::builder().uri("/v1/events?limit=2&page=1").body(Body::empty()).unwrap())
            .await.unwrap();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert!(v["total"].as_u64().is_some());
        assert_eq!(v["data"].as_array().unwrap().len(), 2);

        let response = app
            .oneshot(Request::builder().uri("/v1/events?limit=2&page=2").body(Body::empty()).unwrap())
            .await.unwrap();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert!(v["total"].as_u64().is_some());
        assert_eq!(v["data"].as_array().unwrap().len(), 1);
    }

    /// Test that health endpoint returns 503 when DB is unreachable
    #[tokio::test]
    async fn health_db_unreachable_returns_503() {
        let pool = PgPool::connect_lazy("postgres://invalid-host:5432/invalid_db").unwrap();
        let health_state = Arc::new(HealthState::new(60));
        let prometheus_handle = crate::metrics::init_metrics();
        let indexer_state = Arc::new(IndexerState::new());
        let app = crate::routes::create_router(pool, None, &[], 60, health_state, indexer_state, prometheus_handle, 2000);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        // The DB is unreachable so should return 503
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["status"], "degraded");
        assert!(matches!(v["db"].as_str(), Some("unreachable") | Some("pool_exhausted")));
    }

    // Health endpoint tests
    #[sqlx::test(migrations = "./migrations")]
    async fn health_happy_path_returns_200(pool: PgPool) {
        let app = create_test_router(pool);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["status"], "ok");
        assert_eq!(v["db"], "ok");
        assert_eq!(v["indexer"], "ok");
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn healthz_live_returns_200(pool: PgPool) {
        let app = create_test_router(pool);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/healthz/live")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["status"], "alive");
    }

    #[tokio::test]
    async fn healthz_ready_unreachable_db_returns_503() {
        let pool = PgPool::connect_lazy("postgres://invalid-host:5432/invalid_db").unwrap();
        let health_state = Arc::new(HealthState::new(60));
        let prometheus_handle = crate::metrics::init_metrics();
        let indexer_state = Arc::new(IndexerState::new());
        let app = crate::routes::create_router(pool, None, &[], 60, health_state, indexer_state, prometheus_handle, 2000);

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
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["status"], "degraded");
        assert!(matches!(v["db"].as_str(), Some("unreachable") | Some("pool_exhausted")));
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn healthz_ready_indexer_stalled_returns_503(pool: PgPool) {
        let health_state = Arc::new(HealthState::new(1));
        // never updated, treated as stalled
        let prometheus_handle = crate::metrics::init_metrics();
        let indexer_state = Arc::new(IndexerState::new());
        let app = crate::routes::create_router(pool, None, &[], 60, health_state, indexer_state, prometheus_handle, 2000);

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
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["status"], "degraded");
        assert_eq!(v["indexer"], "stalled");
    }

    // Status endpoint tests
    #[sqlx::test(migrations = "./migrations")]
    async fn status_returns_operational_info(pool: PgPool) {
        let app = create_test_router(pool);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/status")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        
        // Verify required fields are present
        assert!(v.get("version").is_some());
        assert!(v.get("uptime_secs").is_some());
        assert!(v.get("current_ledger").is_some());
        assert!(v.get("latest_ledger").is_some());
        assert!(v.get("lag_ledgers").is_some());
        assert!(v.get("total_events").is_some());
        assert!(v.get("indexer_status").is_some());
        
        // Verify total_events is 0 for empty DB
        assert_eq!(v["total_events"], 0);
    }

    // OpenAPI endpoint tests
    #[sqlx::test(migrations = "./migrations")]
    async fn openapi_json_returns_valid_spec(pool: PgPool) {
        let app = create_test_router(pool);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/openapi.json")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        
        // Verify it's a valid OpenAPI spec
        assert_eq!(v["openapi"], "3.0.0");
        assert!(v.get("info").is_some());
        assert!(v.get("paths").is_some());
    }

    // Main events endpoint tests - Happy path
    #[sqlx::test(migrations = "./migrations")]
    async fn get_events_empty_db_returns_200_with_empty_data(pool: PgPool) {
        let app = create_test_router(pool);

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
        let v: Value = serde_json::from_slice(&body).unwrap();
        
        assert_eq!(v["data"], json!([]));
        assert_eq!(v["total"], 0);
        assert_eq!(v["page"], 1);
        assert_eq!(v["limit"], 20);
        assert_eq!(v["approximate"], true);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn get_events_with_data_returns_paginated_results(pool: PgPool) {
        let app = create_test_router(pool.clone());
        
        // Insert test data
        for i in 0..5 {
            sqlx::query(
                "INSERT INTO events (contract_id, event_type, tx_hash, ledger, timestamp, event_data) 
                 VALUES ($1, $2, $3, $4, $5, $6)"
            )
            .bind(format!("C{:0>55}", i))
            .bind("contract")
            .bind(format!("{:0>64}", i))
            .bind(i as i64)
            .bind(Utc::now())
            .bind(json!({"test": i}))
            .execute(&pool)
            .await
            .unwrap();
        }

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/events?limit=3")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        
        assert_eq!(v["data"].as_array().unwrap().len(), 3);
        assert_eq!(v["total"], 5);
        assert_eq!(v["page"], 1);
        assert_eq!(v["limit"], 3);
    }

    // Main events endpoint tests - Error cases
    #[sqlx::test(migrations = "./migrations")]
    async fn get_events_invalid_event_type_returns_400(pool: PgPool) {
        let app = create_test_router(pool);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/events?event_type=invalid")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert!(v["error"].as_str().unwrap().contains("event_type must be one of"));
        assert_eq!(v["code"], "VALIDATION_ERROR");
        assert!(v["correlation_id"].as_str().is_some());
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn get_events_invalid_ledger_range_returns_400(pool: PgPool) {
        let app = create_test_router(pool);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/events?from_ledger=100&to_ledger=50")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert!(v["error"].as_str().unwrap().contains("from_ledger must be <= to_ledger"));
        assert_eq!(v["code"], "VALIDATION_ERROR");
        assert!(v["correlation_id"].as_str().is_some());
    }

    // Events by contract tests - Happy path
    #[sqlx::test(migrations = "./migrations")]
    async fn get_events_by_contract_with_data_returns_200(pool: PgPool) {
        let app = create_test_router(pool.clone());
        let contract_id = "C1234567890123456789012345678901234567890123456789012345";
        
        // Insert test data
        sqlx::query(
            "INSERT INTO events (contract_id, event_type, tx_hash, ledger, timestamp, event_data) 
             VALUES ($1, $2, $3, $4, $5, $6)"
        )
        .bind(contract_id)
        .bind("contract")
        .bind("a".repeat(64))
        .bind(100_i64)
        .bind(Utc::now())
        .bind(json!({"test": "data"}))
        .execute(&pool)
        .await
        .unwrap();

        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!("/v1/events/contract/{}", contract_id))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        
        assert_eq!(v["data"].as_array().unwrap().len(), 1);
        assert_eq!(v["contract_id"], contract_id);
    }

    // Events by contract tests - Error cases
    #[sqlx::test(migrations = "./migrations")]
    async fn get_events_by_contract_not_found_returns_404(pool: PgPool) {
        let app = create_test_router(pool);
        let contract_id = "C1234567890123456789012345678901234567890123456789012345";

        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!("/v1/events/contract/{}", contract_id))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    // Events by transaction tests - Happy path
    #[sqlx::test(migrations = "./migrations")]
    async fn get_events_by_tx_multiple_events_returns_all(pool: PgPool) {
        let app = create_test_router(pool.clone());
        let tx_hash = "a1b2c3d4e5f6789012345678901234567890123456789012345678901234567890";
        
        // Insert multiple events for same transaction
        for i in 0..3 {
            sqlx::query(
                "INSERT INTO events (contract_id, event_type, tx_hash, ledger, timestamp, event_data) 
                 VALUES ($1, $2, $3, $4, $5, $6)"
            )
            .bind(format!("C{:0>55}", i))
            .bind("contract")
            .bind(&tx_hash)
            .bind(i as i64)
            .bind(Utc::now())
            .bind(json!({"event_num": i}))
            .execute(&pool)
            .await
            .unwrap();
        }

        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!("/v1/events/tx/{}", tx_hash))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        
        assert_eq!(v["data"].as_array().unwrap().len(), 3);
        assert_eq!(v["tx_hash"], tx_hash);
    }

    // Stream events endpoint tests
    #[sqlx::test(migrations = "./migrations")]
    async fn stream_events_returns_sse_stream(pool: PgPool) {
        let app = create_test_router(pool);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/events/stream")
                    .header("Accept", "text/event-stream")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get("content-type").unwrap(),
            "text/event-stream"
        );
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn stream_events_with_contract_filter(pool: PgPool) {
        let app = create_test_router(pool);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/events/stream?contract_id=C1234567890123456789012345678901234567890123456789012345")
                    .header("Accept", "text/event-stream")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get("content-type").unwrap(),
            "text/event-stream"
        );
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn stream_events_invalid_contract_id_returns_400(pool: PgPool) {
        let app = create_test_router(pool);

        // Invalid: too short
        let response = app.clone()
            .oneshot(
                Request::builder()
                    .uri("/v1/events/stream?contract_id=CABC")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["error"], "invalid contract_id format");

        // Invalid: doesn't start with C
        let response = app.clone()
            .oneshot(
                Request::builder()
                    .uri("/v1/events/stream?contract_id=A1234567890123456789012345678901234567890123456789012345")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        // Invalid: contains non-alphanumeric
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/v1/events/stream?contract_id=C123456789012345678901234567890123456789012345678901234!")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    // Metrics endpoint tests
    #[sqlx::test(migrations = "./migrations")]
    async fn metrics_returns_prometheus_format(pool: PgPool) {
        let app = create_test_router(pool);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/metrics")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        // Metrics endpoint should return text/plain content type
        assert_eq!(
            response.headers().get("content-type").unwrap(),
            "text/plain; version=0.0.4"
        );
    }

    // Pagination boundary condition tests
    #[sqlx::test(migrations = "./migrations")]
    async fn pagination_boundary_conditions(pool: PgPool) {
        let app = create_test_router(pool.clone());
        
        // Insert exactly 25 test events
        for i in 0..25 {
            sqlx::query(
                "INSERT INTO events (contract_id, event_type, tx_hash, ledger, timestamp, event_data) 
                 VALUES ($1, $2, $3, $4, $5, $6)"
            )
            .bind(format!("C{:0>55}", i))
            .bind("contract")
            .bind(format!("{:0>64}", i))
            .bind(i as i64)
            .bind(Utc::now())
            .bind(json!({"test": i}))
            .execute(&pool)
            .await
            .unwrap();
        }

        // Test limit boundary: limit=1 (minimum)
        let response = app.clone()
            .oneshot(Request::builder().uri("/v1/events?limit=1").body(Body::empty()).unwrap())
            .await.unwrap();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["limit"], 1);
        assert_eq!(v["data"].as_array().unwrap().len(), 1);
        assert_eq!(v["total"], 25);

        // Test limit boundary: limit=100 (maximum)
        let response = app.clone()
            .oneshot(Request::builder().uri("/v1/events?limit=100").body(Body::empty()).unwrap())
            .await.unwrap();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["limit"], 100);
        assert_eq!(v["data"].as_array().unwrap().len(), 25); // All events
        assert_eq!(v["total"], 25);

        // Test page boundary: page=1, limit=10
        let response = app.clone()
            .oneshot(Request::builder().uri("/v1/events?page=1&limit=10").body(Body::empty()).unwrap())
            .await.unwrap();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["page"], 1);
        assert_eq!(v["limit"], 10);
        assert_eq!(v["data"].as_array().unwrap().len(), 10);

        // Test page boundary: page=3, limit=10 (last page with 5 items)
        let response = app.clone()
            .oneshot(Request::builder().uri("/v1/events?page=3&limit=10").body(Body::empty()).unwrap())
            .await.unwrap();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["page"], 3);
        assert_eq!(v["limit"], 10);
        assert_eq!(v["data"].as_array().unwrap().len(), 5);

        // Test page boundary: page=4, limit=10 (beyond last page, empty)
        let response = app.clone()
            .oneshot(Request::builder().uri("/v1/events?page=4&limit=10").body(Body::empty()).unwrap())
            .await.unwrap();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["page"], 4);
        assert_eq!(v["limit"], 10);
        assert_eq!(v["data"].as_array().unwrap().len(), 0);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn pagination_invalid_parameters_are_clamped(pool: PgPool) {
        let app = create_test_router(pool.clone());
        
        // Insert test data
        for i in 0..5 {
            sqlx::query(
                "INSERT INTO events (contract_id, event_type, tx_hash, ledger, timestamp, event_data) 
                 VALUES ($1, $2, $3, $4, $5, $6)"
            )
            .bind(format!("C{:0>55}", i))
            .bind("contract")
            .bind(format!("{:0>64}", i))
            .bind(i as i64)
            .bind(Utc::now())
            .bind(json!({"test": i}))
            .execute(&pool)
            .await
            .unwrap();
        }

        // Test limit=0 gets clamped to 1
        let response = app.clone()
            .oneshot(Request::builder().uri("/v1/events?limit=0").body(Body::empty()).unwrap())
            .await.unwrap();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["limit"], 1);

        // Test limit=200 gets clamped to 100
        let response = app.clone()
            .oneshot(Request::builder().uri("/v1/events?limit=200").body(Body::empty()).unwrap())
            .await.unwrap();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["limit"], 100);

        // Test page=0 gets treated as page=1
        let response = app
            .oneshot(Request::builder().uri("/v1/events?page=0").body(Body::empty()).unwrap())
            .await.unwrap();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["page"], 1);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn pagination_exact_count_vs_approximate_count(pool: PgPool) {
        let app = create_test_router(pool.clone());
        
        // Insert test data
        for i in 0..15 {
            sqlx::query(
                "INSERT INTO events (contract_id, event_type, tx_hash, ledger, timestamp, event_data) 
                 VALUES ($1, $2, $3, $4, $5, $6)"
            )
            .bind(format!("C{:0>55}", i))
            .bind("contract")
            .bind(format!("{:0>64}", i))
            .bind(i as i64)
            .bind(Utc::now())
            .bind(json!({"test": i}))
            .execute(&pool)
            .await
            .unwrap();
        }

        // Test approximate count (default)
        let response = app.clone()
            .oneshot(Request::builder().uri("/v1/events").body(Body::empty()).unwrap())
            .await.unwrap();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["approximate"], true);
        // Approximate count may not be exact but should be reasonable
        let approx_count = v["total"].as_i64().unwrap();
        assert!(approx_count >= 0); // Should be non-negative

        // Test exact count
        let response = app.clone()
            .oneshot(Request::builder().uri("/v1/events?exact_count=true").body(Body::empty()).unwrap())
            .await.unwrap();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["approximate"], false);
        assert_eq!(v["total"], 15); // Exact count should match

        // Test filtered queries always use exact count
        let response = app
            .oneshot(Request::builder().uri("/v1/events?event_type=contract").body(Body::empty()).unwrap())
            .await.unwrap();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["approximate"], false);
        assert_eq!(v["total"], 15); // All events are contract type
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn pagination_with_filters(pool: PgPool) {
        let app = create_test_router(pool.clone());
        
        // Insert mixed event types
        for i in 0..10 {
            sqlx::query(
                "INSERT INTO events (contract_id, event_type, tx_hash, ledger, timestamp, event_data) 
                 VALUES ($1, $2, $3, $4, $5, $6)"
            )
            .bind(format!("C{:0>55}", i))
            .bind(if i % 2 == 0 { "contract" } else { "diagnostic" })
            .bind(format!("{:0>64}", i))
            .bind(i as i64)
            .bind(Utc::now())
            .bind(json!({"test": i}))
            .execute(&pool)
            .await
            .unwrap();
        }

        // Test pagination with event_type filter
        let response = app.clone()
            .oneshot(Request::builder().uri("/v1/events?event_type=contract&limit=3").body(Body::empty()).unwrap())
            .await.unwrap();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["data"].as_array().unwrap().len(), 3);
        assert_eq!(v["total"], 5); // 5 contract events
        assert_eq!(v["approximate"], false); // Filtered queries use exact count

        // Test pagination with ledger range filter
        let response = app.clone()
            .oneshot(Request::builder().uri("/v1/events?from_ledger=2&to_ledger=8&limit=5").body(Body::empty()).unwrap())
            .await.unwrap();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["data"].as_array().unwrap().len(), 5);
        assert_eq!(v["total"], 7); // Events with ledger 2-8
        assert_eq!(v["approximate"], false); // Filtered queries use exact count

        // Test pagination with both filters
        let response = app
            .oneshot(Request::builder().uri("/v1/events?event_type=contract&from_ledger=0&to_ledger=6&limit=10").body(Body::empty()).unwrap())
            .await.unwrap();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["data"].as_array().unwrap().len(), 4); // Contract events with ledger 0-6
        assert_eq!(v["total"], 4);
        assert_eq!(v["approximate"], false);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn pagination_fields_filtering(pool: PgPool) {
        let app = create_test_router(pool.clone());
        
        // Insert test data
        sqlx::query(
            "INSERT INTO events (contract_id, event_type, tx_hash, ledger, timestamp, event_data) 
             VALUES ($1, $2, $3, $4, $5, $6)"
        )
        .bind("C1234567890123456789012345678901234567890123456789012345")
        .bind("contract")
        .bind("a".repeat(64))
        .bind(100_i64)
        .bind(Utc::now())
        .bind(json!({"test": "data"}))
        .execute(&pool)
        .await
        .unwrap();

        // Test fields filter with single field
        let response = app.clone()
            .oneshot(Request::builder().uri("/v1/events?fields=ledger").body(Body::empty()).unwrap())
            .await.unwrap();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        let event = &v["data"][0];
        assert!(event.get("ledger").is_some());
        assert!(event.get("contract_id").is_none());
        assert!(event.get("event_type").is_none());
        assert!(event.get("tx_hash").is_none());
        assert!(event.get("timestamp").is_none());
        assert!(event.get("event_data").is_none());
        assert!(event.get("created_at").is_none());

        // Test fields filter with multiple fields
        let response = app.clone()
            .oneshot(Request::builder().uri("/v1/events?fields=ledger,contract_id,event_type").body(Body::empty()).unwrap())
            .await.unwrap();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        let event = &v["data"][0];
        assert!(event.get("ledger").is_some());
        assert!(event.get("contract_id").is_some());
        assert!(event.get("event_type").is_some());
        assert!(event.get("tx_hash").is_none());
        assert!(event.get("timestamp").is_none());
        assert!(event.get("event_data").is_none());
        assert!(event.get("created_at").is_none());

        // Test fields filter with invalid fields (should be ignored)
        let response = app.clone()
            .oneshot(Request::builder().uri("/v1/events?fields=ledger,invalid_field,contract_id").body(Body::empty()).unwrap())
            .await.unwrap();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        let event = &v["data"][0];
        assert!(event.get("ledger").is_some());
        assert!(event.get("contract_id").is_some());
        assert!(event.get("invalid_field").is_none());

        // Test empty fields filter (should return all fields)
        let response = app
            .oneshot(Request::builder().uri("/v1/events?fields=").body(Body::empty()).unwrap())
            .await.unwrap();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        let event = &v["data"][0];
        assert!(event.get("ledger").is_some());
        assert!(event.get("contract_id").is_some());
        assert!(event.get("event_type").is_some());
        assert!(event.get("tx_hash").is_some());
        assert!(event.get("timestamp").is_some());
        assert!(event.get("event_data").is_some());
        assert!(event.get("created_at").is_some());
    }

    // --- Cursor pagination tests ---

    async fn insert_events(pool: &PgPool, count: usize) {
        for i in 0..count {
            sqlx::query(
                "INSERT INTO events (contract_id, event_type, tx_hash, ledger, timestamp, event_data)
                 VALUES ($1, $2, $3, $4, $5, $6)"
            )
            .bind(format!("C{:0>55}", i))
            .bind("contract")
            .bind(format!("{:0>64}", i))
            .bind(i as i64)
            .bind(Utc::now())
            .bind(json!({}))
            .execute(pool)
            .await
            .unwrap();
        }
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn cursor_pagination_traverses_all_pages(pool: PgPool) {
        insert_events(&pool, 5).await;
        let app = create_test_router(pool);

        // Page 1: limit=2, no cursor
        let resp = app.clone()
            .oneshot(Request::builder().uri("/v1/events?limit=2").body(Body::empty()).unwrap())
            .await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["data"].as_array().unwrap().len(), 2);
        let cursor1 = v["next_cursor"].as_str().expect("next_cursor must be present on page 1").to_string();

        // Page 2: use cursor from page 1
        let resp = app.clone()
            .oneshot(Request::builder().uri(format!("/v1/events?limit=2&cursor={cursor1}")).body(Body::empty()).unwrap())
            .await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["data"].as_array().unwrap().len(), 2);
        let cursor2 = v["next_cursor"].as_str().expect("next_cursor must be present on page 2").to_string();

        // Page 3: last page — 1 row, next_cursor must be null
        let resp = app.clone()
            .oneshot(Request::builder().uri(format!("/v1/events?limit=2&cursor={cursor2}")).body(Body::empty()).unwrap())
            .await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["data"].as_array().unwrap().len(), 1);
        assert!(v["next_cursor"].is_null(), "next_cursor must be null on last page");
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn cursor_pagination_no_duplicate_or_missing_rows(pool: PgPool) {
        insert_events(&pool, 6).await;
        let app = create_test_router(pool);

        let mut seen_ledgers: Vec<i64> = Vec::new();
        let mut cursor: Option<String> = None;

        loop {
            let uri = match &cursor {
                Some(c) => format!("/v1/events?limit=2&cursor={c}"),
                None => "/v1/events?limit=2".to_string(),
            };
            let resp = app.clone()
                .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
                .await.unwrap();
            let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
            let v: Value = serde_json::from_slice(&body).unwrap();
            let page = v["data"].as_array().unwrap();
            for ev in page {
                seen_ledgers.push(ev["ledger"].as_i64().unwrap());
            }
            cursor = v["next_cursor"].as_str().map(|s| s.to_string());
            if cursor.is_none() { break; }
        }

        // All 6 ledgers seen exactly once, in descending order
        assert_eq!(seen_ledgers.len(), 6);
        let mut sorted = seen_ledgers.clone();
        sorted.sort_by(|a, b| b.cmp(a));
        assert_eq!(seen_ledgers, sorted);
        let unique: std::collections::HashSet<_> = seen_ledgers.iter().collect();
        assert_eq!(unique.len(), 6);
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn cursor_invalid_returns_400(pool: PgPool) {
        let app = create_test_router(pool);
        let resp = app
            .oneshot(Request::builder().uri("/v1/events?cursor=notvalidbase64!!!").body(Body::empty()).unwrap())
            .await.unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["error"], "invalid cursor");
    }

    #[sqlx::test(migrations = "./migrations")]
    async fn offset_response_includes_next_cursor(pool: PgPool) {
        insert_events(&pool, 3).await;
        let app = create_test_router(pool);

        let resp = app
            .oneshot(Request::builder().uri("/v1/events?limit=2").body(Body::empty()).unwrap())
            .await.unwrap();
        let body = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let v: Value = serde_json::from_slice(&body).unwrap();
        // Offset path still returns total/page/approximate AND next_cursor
        assert!(v.get("total").is_some());
        assert!(v.get("page").is_some());
        assert!(v["next_cursor"].is_string(), "offset path must also return next_cursor");
    }
}
