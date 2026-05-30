use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde::Serialize;
use std::cell::RefCell;
use thiserror::Error;
use uuid::Uuid;

thread_local! {
    static REQUEST_ID: RefCell<Option<String>> = RefCell::new(None);
}

pub fn set_request_id(id: String) {
    REQUEST_ID.with(|rid| {
        *rid.borrow_mut() = Some(id);
    });
}

pub fn get_request_id() -> String {
    REQUEST_ID.with(|rid| {
        rid.borrow()
            .clone()
            .unwrap_or_else(|| Uuid::new_v4().to_string())
    })
}

/// Machine-readable error response body.
#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct ErrorResponse {
    pub error: String,
    pub code: &'static str,
    pub correlation_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub validation_errors: Option<Vec<ValidationErrorDetail>>,
}

/// Details of a single validation error.
#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct ValidationErrorDetail {
    pub instance_path: String,
    pub schema_path: String,
    pub message: String,
}

#[derive(Debug, Error)]
pub enum AppError {
    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),

    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),

    #[error("Not found")]
    NotFound,

    #[error("Validation error: {0}")]
    Validation(String),

    #[error("Forbidden: {0}")]
    Forbidden(String),

    #[error("Validation error with details")]
    ValidationWithDetails(String, Vec<ValidationErrorDetail>),

    #[error("Internal error: {0}")]
    #[allow(dead_code)]
    Internal(String),
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let correlation_id = get_request_id();

        let (status, message, code, validation_errors) = match &self {
            AppError::NotFound => (StatusCode::NOT_FOUND, "not found".to_string(), "NOT_FOUND", None),
            AppError::Validation(msg) => (StatusCode::BAD_REQUEST, msg.clone(), "VALIDATION_ERROR", None),
            AppError::ValidationWithDetails(msg, errors) => (
                StatusCode::BAD_REQUEST,
                msg.clone(),
                "VALIDATION_ERROR",
                Some(errors.clone()),
            ),
            AppError::Forbidden(msg) => (
                StatusCode::FORBIDDEN,
                msg.clone(),
                "FORBIDDEN",
                None,
            ),
            AppError::Database(e) => {
                if is_query_timeout(e) {
                    let body = ErrorResponse {
                        error: "query timeout".to_string(),
                        code: "DATABASE_ERROR",
                        correlation_id,
                        validation_errors: None,
                    };
                    return (StatusCode::SERVICE_UNAVAILABLE, Json(body)).into_response();
                }
                tracing::error!(
                    error = %e,
                    "Database error"
                );
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal server error".to_string(),
                    "DATABASE_ERROR",
                    None,
                )
            }
            AppError::Http(e) => {
                tracing::error!(
                    error = %e,
                    "HTTP error"
                );
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal server error".to_string(),
                    "INTERNAL_ERROR",
                    None,
                )
            }
            AppError::Internal(msg) => {
                tracing::error!(
                    error = %msg,
                    "Internal error"
                );
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal server error".to_string(),
                    "INTERNAL_ERROR",
                    None,
                )
            }
        };

        let body = ErrorResponse {
            error: message,
            code,
            correlation_id,
            validation_errors,
        };
        (status, Json(body)).into_response()
    }
}

impl AppError {
    pub fn into_response_parts(self) -> (StatusCode, Json<serde_json::Value>) {
        let correlation_id = get_request_id();

        let (status, message, code, validation_errors) = match &self {
            AppError::NotFound => (StatusCode::NOT_FOUND, "not found".to_string(), "NOT_FOUND", None),
            AppError::Validation(msg) => (StatusCode::BAD_REQUEST, msg.clone(), "VALIDATION_ERROR", None),
            AppError::ValidationWithDetails(msg, errors) => (
                StatusCode::BAD_REQUEST,
                msg.clone(),
                "VALIDATION_ERROR",
                Some(errors.clone()),
            ),
            AppError::Forbidden(msg) => (
                StatusCode::FORBIDDEN,
                msg.clone(),
                "FORBIDDEN",
                None,
            ),
            AppError::Database(e) => {
                if is_query_timeout(e) {
                    let body = serde_json::json!({
                        "error": "query timeout",
                        "code": "DATABASE_ERROR",
                        "correlation_id": correlation_id,
                    });
                    return (StatusCode::SERVICE_UNAVAILABLE, Json(body));
                }
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal server error".to_string(),
                    "DATABASE_ERROR",
                    None,
                )
            }
            AppError::Forbidden(msg) => (
                StatusCode::FORBIDDEN,
                msg.clone(),
                "FORBIDDEN".to_string(),
                None,
            ),
            AppError::Http(_) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal server error".to_string(),
                "INTERNAL_ERROR",
                None,
            ),
            AppError::Internal(_) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal server error".to_string(),
                "INTERNAL_ERROR",
                None,
            ),
        };

        let mut body = serde_json::json!({
            "error": message,
            "code": code,
            "correlation_id": correlation_id,
        });

        if let Some(errors) = validation_errors {
            body["validation_errors"] = serde_json::to_value(errors).unwrap_or(serde_json::json!([]));
        }

        (status, Json(body))
    }
}

fn is_query_timeout(e: &sqlx::Error) -> bool {
    // Postgres error code 57014 = query_canceled (covers statement_timeout)
    if let sqlx::Error::Database(db_err) = e {
        return db_err.code().as_deref() == Some("57014");
    }
    false
}
