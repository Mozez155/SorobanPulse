use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::PgPool;
use uuid::Uuid;

use crate::{
    error::AppError,
    models::{Event, PaginationParams},
    routes::{AppState, PaginatedResponse},
};

#[derive(Debug, Serialize, sqlx::FromRow)]
pub struct SavedQuery {
    pub id: Uuid,
    pub name: String,
    pub description: Option<String>,
    pub query_params: Value,
    pub created_by: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub last_executed_at: Option<DateTime<Utc>>,
    pub execution_count: i64,
}

#[derive(Debug, Deserialize)]
pub struct CreateSavedQueryRequest {
    pub name: String,
    pub description: Option<String>,
    pub query_params: PaginationParams,
}

#[derive(Debug, Deserialize)]
pub struct UpdateSavedQueryRequest {
    pub name: Option<String>,
    pub description: Option<String>,
    pub query_params: Option<PaginationParams>,
}

#[derive(Debug, Deserialize)]
pub struct ListSavedQueriesParams {
    pub page: Option<i64>,
    pub limit: Option<i64>,
}

pub async fn create_saved_query(
    State(state): State<AppState>,
    Json(request): Json<CreateSavedQueryRequest>,
) -> Result<Json<SavedQuery>, AppError> {
    let query_params_json = serde_json::to_value(&request.query_params)
        .map_err(|e| AppError::Validation(format!("Invalid query parameters: {}", e)))?;

    let saved_query = sqlx::query_as::<_, SavedQuery>(
        r#"
        INSERT INTO saved_queries (name, description, query_params, created_by)
        VALUES ($1, $2, $3, $4)
        RETURNING *
        "#,
    )
    .bind(&request.name)
    .bind(&request.description)
    .bind(&query_params_json)
    .bind("api_user") // TODO: Extract from auth context
    .fetch_one(&state.pool)
    .await
    .map_err(|e| match e {
        sqlx::Error::Database(db_err) if db_err.constraint().is_some() => {
            AppError::Validation("Query name already exists".to_string())
        }
        _ => AppError::Internal(format!("Failed to create saved query: {}", e)),
    })?;

    Ok(Json(saved_query))
}
pub async fn list_saved_queries(
    State(state): State<AppState>,
    Query(params): Query<ListSavedQueriesParams>,
) -> Result<Json<PaginatedResponse<SavedQuery>>, AppError> {
    let page = params.page.unwrap_or(1).max(1);
    let limit = params.limit.unwrap_or(20).clamp(1, 100);
    let offset = (page - 1) * limit;

    let queries = sqlx::query_as::<_, SavedQuery>(
        r#"
        SELECT * FROM saved_queries
        ORDER BY created_at DESC
        LIMIT $1 OFFSET $2
        "#,
    )
    .bind(limit)
    .bind(offset)
    .fetch_all(&state.pool)
    .await
    .map_err(|e| AppError::Internal(format!("Failed to fetch saved queries: {}", e)))?;

    let total_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM saved_queries")
        .fetch_one(&state.pool)
        .await
        .map_err(|e| AppError::Internal(format!("Failed to count saved queries: {}", e)))?;

    Ok(Json(PaginatedResponse {
        data: queries,
        page,
        limit,
        total: total_count,
        has_more: offset + limit < total_count,
    }))
}

pub async fn get_saved_query(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<SavedQuery>, AppError> {
    let query = sqlx::query_as::<_, SavedQuery>(
        "SELECT * FROM saved_queries WHERE id = $1"
    )
    .bind(id)
    .fetch_optional(&state.pool)
    .await
    .map_err(|e| AppError::Internal(format!("Failed to fetch saved query: {}", e)))?
    .ok_or(AppError::NotFound("Saved query not found".to_string()))?;

    Ok(Json(query))
}

pub async fn update_saved_query(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    Json(request): Json<UpdateSavedQueryRequest>,
) -> Result<Json<SavedQuery>, AppError> {
    let query_params_json = if let Some(params) = request.query_params {
        Some(serde_json::to_value(&params)
            .map_err(|e| AppError::Validation(format!("Invalid query parameters: {}", e)))?)
    } else {
        None
    };

    let updated_query = sqlx::query_as::<_, SavedQuery>(
        r#"
        UPDATE saved_queries 
        SET name = COALESCE($1, name),
            description = COALESCE($2, description),
            query_params = COALESCE($3, query_params),
            updated_at = NOW()
        WHERE id = $4
        RETURNING *
        "#,
    )
    .bind(&request.name)
    .bind(&request.description)
    .bind(&query_params_json)
    .bind(id)
    .fetch_optional(&state.pool)
    .await
    .map_err(|e| AppError::Internal(format!("Failed to update saved query: {}", e)))?
    .ok_or(AppError::NotFound("Saved query not found".to_string()))?;

    Ok(Json(updated_query))
}

pub async fn delete_saved_query(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<StatusCode, AppError> {
    let rows_affected = sqlx::query("DELETE FROM saved_queries WHERE id = $1")
        .bind(id)
        .execute(&state.pool)
        .await
        .map_err(|e| AppError::Internal(format!("Failed to delete saved query: {}", e)))?
        .rows_affected();

    if rows_affected == 0 {
        return Err(AppError::NotFound("Saved query not found".to_string()));
    }

    Ok(StatusCode::NO_CONTENT)
}

pub async fn execute_saved_query(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<PaginatedResponse<Event>>, AppError> {
    // First, fetch the saved query and update execution stats
    let saved_query = sqlx::query_as::<_, SavedQuery>(
        r#"
        UPDATE saved_queries 
        SET last_executed_at = NOW(), execution_count = execution_count + 1
        WHERE id = $1
        RETURNING *
        "#,
    )
    .bind(id)
    .fetch_optional(&state.pool)
    .await
    .map_err(|e| AppError::Internal(format!("Failed to fetch saved query: {}", e)))?
    .ok_or(AppError::NotFound("Saved query not found".to_string()))?;

    // Deserialize the stored query parameters
    let query_params: PaginationParams = serde_json::from_value(saved_query.query_params)
        .map_err(|e| AppError::Internal(format!("Invalid stored query parameters: {}", e)))?;

    // Execute the query using existing event query logic
    crate::routes::get_events_with_params(State(state), Query(query_params)).await
}