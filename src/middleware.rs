use axum::{
    extract::{Request, State},
    middleware::Next,
    response::Response,
    http::StatusCode,
    Json,
};
use std::sync::Arc;
use sha2::{Sha256, Digest};

/// Middleware to extract request_id from headers and store in thread-local
pub async fn request_id_middleware(
    mut req: Request,
    next: Next,
) -> Response {
    let request_id = req
        .headers()
        .get("x-request-id")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("unknown")
        .to_string();
    
    crate::error::set_request_id(request_id);
    next.run(req).await
}

#[derive(Clone)]
pub struct AuthState {
    pub api_keys: Vec<String>,
}

pub async fn auth_middleware(
    State(state): State<Arc<AuthState>>,
    req: Request,
    next: Next,
) -> Result<Response, (StatusCode, Json<serde_json::Value>)> {
    let path = req.uri().path();
    
    // Exclude /health and /healthz/*
    if path == "/health" || path.starts_with("/healthz/") {
        return Ok(next.run(req).await);
    }
    
    if !state.api_keys.is_empty() {
        let auth_header = req.headers().get("Authorization")
            .and_then(|h| h.to_str().ok())
            .and_then(|s| s.strip_prefix("Bearer "));
            
        let api_key_header = req.headers().get("X-Api-Key")
            .and_then(|h| h.to_str().ok());
            
        let provided_key = auth_header.or(api_key_header);
        
        if !provided_key.map_or(false, |key| state.api_keys.contains(&key.to_string())) {
            return Err((
                StatusCode::UNAUTHORIZED,
                Json(serde_json::json!({ "error": "unauthorized" }))
            ));
        }
    }
    
    Ok(next.run(req).await)
}

pub async fn cache_middleware(
    req: Request,
    next: Next,
) -> Response {
    let path = req.uri().path().to_owned();
    let query = req.uri().query().unwrap_or("").to_owned();
    
    let mut response = next.run(req).await;
    
    // Add cache headers based on endpoint
    let cache_control = if path.ends_with("/tx/") || (path.contains("/tx/") && !path.contains("?")) {
        // GET /v1/events/tx/:hash - immutable, cache for 1 hour
        "public, max-age=3600, immutable"
    } else if path == "/v1/events" || path == "/events" {
        // GET /v1/events - check for filters
        if query.contains("to_ledger") {
            // With to_ledger filter - cache for 60 seconds
            "public, max-age=60"
        } else {
            // No filters - cache for 5 seconds with stale-while-revalidate
            "public, max-age=5, stale-while-revalidate=10"
        }
    } else if path.contains("/contract/") {
        // GET /v1/events/contract/:id - cache for 5 seconds with stale-while-revalidate
        "public, max-age=5, stale-while-revalidate=10"
    } else {
        // Default - no caching
        return response;
    };
    
    response.headers_mut().insert(
        "Cache-Control",
        cache_control.parse().unwrap_or_else(|_| "no-cache".parse().unwrap()),
    );
    
    // Add ETag header based on response body hash
    let (mut parts, body) = response.into_parts();
    parts.headers.insert(
        "Cache-Control",
        cache_control.parse().unwrap_or_else(|_| "no-cache".parse().unwrap()),
    );
    if let Ok(body_bytes) = axum::body::to_bytes(body, usize::MAX).await {
        let mut hasher = Sha256::new();
        hasher.update(&body_bytes);
        let hash = format!("{:x}", hasher.finalize());
        let etag = format!("\"{}\"", &hash[..16]);
        parts.headers.insert(
            "ETag",
            etag.parse().unwrap_or_else(|_| "\"unknown\"".parse().unwrap()),
        );
        Response::from_parts(parts, axum::body::Body::from(body_bytes))
    } else {
        Response::from_parts(parts, axum::body::Body::empty())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{routing::get, Router};
    use tower::ServiceExt;
    use axum::http::{Request, StatusCode};
    use axum::body::Body;
    use axum::response::Response;

    async fn setup_app(api_keys: Vec<String>) -> Router {
        let auth_state = Arc::new(AuthState { api_keys });
        Router::new()
            .route("/test", get(|| async { "OK" }))
            .route("/health", get(|| async { "OK" }))
            .route("/healthz/live", get(|| async { "OK" }))
            .route_layer(axum::middleware::from_fn_with_state(auth_state, auth_middleware))
    }

    #[tokio::test]
    async fn test_auth_bypassed_when_no_key_configured() {
        let app = setup_app(vec![]).await;
        
        let response: Response = app
            .oneshot(Request::builder().uri("/test").body(Body::empty()).unwrap())
            .await
            .unwrap();
            
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_auth_success_with_bearer_token() {
        let app = setup_app(vec!["secret123".to_string()]).await;
        
        let response: Response = app
            .oneshot(
                Request::builder()
                    .uri("/test")
                    .header("Authorization", "Bearer secret123")
                    .body(Body::empty())
                    .unwrap()
            )
            .await
            .unwrap();
            
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_auth_success_with_x_api_key() {
        let app = setup_app(vec!["secret123".to_string()]).await;
        
        let response: Response = app
            .oneshot(
                Request::builder()
                    .uri("/test")
                    .header("X-Api-Key", "secret123")
                    .body(Body::empty())
                    .unwrap()
            )
            .await
            .unwrap();
            
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_auth_failure_with_invalid_key() {
        let app = setup_app(vec!["secret123".to_string()]).await;
        
        let response: Response = app
            .oneshot(
                Request::builder()
                    .uri("/test")
                    .header("Authorization", "Bearer wrongkey")
                    .body(Body::empty())
                    .unwrap()
            )
            .await
            .unwrap();
            
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn test_auth_failure_with_missing_key() {
        let app = setup_app(vec!["secret123".to_string()]).await;
        
        let response: Response = app
            .oneshot(
                Request::builder()
                    .uri("/test")
                    .body(Body::empty())
                    .unwrap()
            )
            .await
            .unwrap();
            
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn test_auth_success_with_secondary_key() {
        let app = setup_app(vec!["primary".to_string(), "secondary".to_string()]).await;
        
        let response: Response = app
            .oneshot(
                Request::builder()
                    .uri("/test")
                    .header("Authorization", "Bearer secondary")
                    .body(Body::empty())
                    .unwrap()
            )
            .await
            .unwrap();
            
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_health_endpoints_bypass_auth() {
        let app = setup_app(vec!["secret123".to_string()]).await;
        
        let response: Response = app.clone()
            .oneshot(Request::builder().uri("/health").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        
        let response: Response = app
            .oneshot(Request::builder().uri("/healthz/live").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }
}
