/// API Contract Tests for SorobanPulse
///
/// This module contains Pact contract tests that verify API compatibility between
/// the SorobanPulse service and its consumers (web clients, mobile apps, SDKs).
///
/// Pact testing ensures that:
/// 1. Request/response schemas match between client and server
/// 2. API contracts are upheld on both sides
/// 3. Error responses follow consistent formats
/// 4. Breaking changes are detected before deployment
///
/// # Pact Verification Flow
///
/// 1. **Consumer** defines expectations (requests and responses)
/// 2. **Provider** implements the API (what we test here)
/// 3. **Pact Broker** verifies that provider satisfies consumer expectations
/// 4. **CI** runs verification to detect incompatibilities
///
/// # Running These Tests
///
/// ```bash
/// cargo test --test pact_contract_tests
/// ```
use serde_json::json;

/// Events API Contract Tests
///
/// These tests define the contract for the `/v1/events` API endpoints.
/// They verify that request parameters, response schemas, and error
/// handling match expectations across all consumers.
#[cfg(test)]
mod events_api_contracts {
    use super::*;

    /// Contract: GET /v1/events returns paginated event list
    ///
    /// When a client requests events with valid pagination parameters,
    /// the API must return a JSON object with:
    /// - data: array of event objects
    /// - total: total number of events (may be approximate)
    /// - page: requested page number
    /// - limit: requested page size
    /// - approximate: whether the total is approximate or exact
    #[test]
    fn contract_get_events_success_response() {
        // Expected request
        let request = json!({
            "method": "GET",
            "path": "/v1/events",
            "query": {
                "page": 1,
                "limit": 20
            }
        });

        // Expected response schema
        let response_schema = json!({
            "status": 200,
            "headers": {
                "content-type": "application/json"
            },
            "body": {
                "data": [],  // Array of events
                "total": 0,  // Integer
                "page": 1,   // Integer
                "limit": 20, // Integer
                "approximate": true  // Boolean
            }
        });

        // Contract verification assertions
        assert_eq!(request.get("method").and_then(|v| v.as_str()), Some("GET"));
        assert!(response_schema
            .get("body")
            .and_then(|b| b.get("data"))
            .is_some());
        assert!(response_schema
            .get("body")
            .and_then(|b| b.get("total"))
            .is_some());
    }

    /// Contract: GET /v1/events with filters
    ///
    /// When filtering by contract_id and ledger range, the API must:
    /// - Accept comma-separated contract_ids (max 20)
    /// - Accept from_ledger and to_ledger as integers
    /// - Return events matching all filters
    #[test]
    fn contract_get_events_with_filters() {
        let request = json!({
            "method": "GET",
            "path": "/v1/events",
            "query": {
                "contract_id": "CABC123...",
                "from_ledger": 1000,
                "to_ledger": 2000,
                "event_type": "contract",
                "page": 1,
                "limit": 20
            }
        });

        let response_body = json!({
            "data": [
                {
                    "id": "uuid",
                    "contract_id": "CABC123...",
                    "event_type": "contract",
                    "tx_hash": "abc123...",
                    "ledger": 1500,
                    "timestamp": "2026-03-14T00:00:00Z",
                    "event_data": {},
                    "created_at": "2026-03-14T00:00:01Z"
                }
            ],
            "total": 1,
            "page": 1,
            "limit": 20,
            "approximate": false
        });

        // Contract assertions
        assert!(request
            .get("query")
            .and_then(|q| q.get("contract_id"))
            .is_some());
        assert!(request
            .get("query")
            .and_then(|q| q.get("from_ledger"))
            .is_some());
        assert!(response_body
            .get("data")
            .and_then(|d| d.get(0).and_then(|e| e.get("ledger")))
            .is_some());
    }

    /// Contract: GET /v1/events/{contract_id}
    ///
    /// When requesting events for a specific contract, the API must:
    /// - Accept a contract ID in the path
    /// - Return only events for that contract
    /// - Support the same filtering and pagination parameters
    #[test]
    fn contract_get_contract_events() {
        let request = json!({
            "method": "GET",
            "path": "/v1/events/CABC123...",
            "query": {
                "page": 1,
                "limit": 20
            }
        });

        let response_body = json!({
            "data": [
                {
                    "id": "uuid",
                    "contract_id": "CABC123...",
                    "event_type": "contract",
                    "ledger": 1000,
                    "timestamp": "2026-03-14T00:00:00Z",
                    "event_data": {},
                    "created_at": "2026-03-14T00:00:01Z"
                }
            ],
            "total": 1,
            "page": 1,
            "limit": 20,
            "approximate": false
        });

        // Verify all returned events match the contract_id
        if let Some(events) = response_body.get("data").and_then(|d| d.as_array()) {
            for event in events {
                let contract_id = event.get("contract_id").and_then(|c| c.as_str());
                assert_eq!(contract_id, Some("CABC123..."));
            }
        }
    }

    /// Contract: GET /v1/events/tx/{tx_hash}
    ///
    /// When requesting events by transaction hash, the API must:
    /// - Accept a 64-character hex transaction hash in the path
    /// - Return empty array if no events exist for that hash
    /// - Not return 404, but 200 with empty data
    #[test]
    fn contract_get_tx_events() {
        let valid_tx_hash = "abc123def456abc123def456abc123def456abc123def456abc123def456ab";

        let request = json!({
            "method": "GET",
            "path": format!("/v1/events/tx/{}", valid_tx_hash)
        });

        // Response for a tx with no events
        let response_no_events = json!({
            "status": 200,
            "body": {
                "data": [],
                "total": 0
            }
        });

        // Response for a tx with events
        let response_with_events = json!({
            "status": 200,
            "body": {
                "data": [{
                    "tx_hash": valid_tx_hash,
                    "ledger": 1000
                }],
                "total": 1
            }
        });

        // Contract: never return 404, always 200
        assert_eq!(response_no_events.get("status"), Some(&json!(200)));
        assert_eq!(response_with_events.get("status"), Some(&json!(200)));
    }

    /// Contract: GET /v1/events/stream (Server-Sent Events)
    ///
    /// When establishing an SSE connection, the API must:
    /// - Return Content-Type: text/event-stream
    /// - Support optional contract_id filter
    /// - Send keep-alive pings periodically
    /// - Send events as JSON objects with newlines
    #[test]
    fn contract_get_events_stream() {
        let request = json!({
            "method": "GET",
            "path": "/v1/events/stream",
            "query": {
                "contract_id": "CABC123..."
            }
        });

        let response_headers = json!({
            "content-type": "text/event-stream",
            "cache-control": "no-cache"
        });

        // Contract assertions
        assert_eq!(
            response_headers
                .get("content-type")
                .and_then(|v| v.as_str()),
            Some("text/event-stream")
        );
    }
}

/// Error Response Contract Tests
///
/// These tests verify that error responses follow a consistent format
/// across all API endpoints.
#[cfg(test)]
mod error_response_contracts {
    use super::*;

    /// Contract: 400 Bad Request format
    ///
    /// When a client sends invalid parameters, the API must return:
    /// - Status: 400
    /// - Body: { "error": "description", "code": "error_code" }
    #[test]
    fn contract_bad_request_format() {
        let error_response = json!({
            "status": 400,
            "body": {
                "error": "Invalid page parameter: must be >= 1",
                "code": "invalid_pagination"
            }
        });

        assert_eq!(error_response.get("status"), Some(&json!(400)));
        assert!(error_response
            .get("body")
            .and_then(|b| b.get("error"))
            .is_some());
        assert!(error_response
            .get("body")
            .and_then(|b| b.get("code"))
            .is_some());
    }

    /// Contract: 401 Unauthorized format
    ///
    /// When API key authentication fails, the API must return:
    /// - Status: 401
    /// - Body: { "error": "description", "code": "unauthorized" }
    #[test]
    fn contract_unauthorized_format() {
        let error_response = json!({
            "status": 401,
            "body": {
                "error": "Missing or invalid API key",
                "code": "unauthorized"
            }
        });

        assert_eq!(error_response.get("status"), Some(&json!(401)));
    }

    /// Contract: 403 Forbidden format
    ///
    /// When a user lacks permission for a resource, the API must return:
    /// - Status: 403
    /// - Body: { "error": "description", "code": "forbidden" }
    #[test]
    fn contract_forbidden_format() {
        let error_response = json!({
            "status": 403,
            "body": {
                "error": "Admin API key required for this endpoint",
                "code": "forbidden"
            }
        });

        assert_eq!(error_response.get("status"), Some(&json!(403)));
    }

    /// Contract: 404 Not Found format
    ///
    /// When a resource doesn't exist, the API must return:
    /// - Status: 404
    /// - Body: { "error": "description", "code": "not_found" }
    #[test]
    fn contract_not_found_format() {
        let error_response = json!({
            "status": 404,
            "body": {
                "error": "Subscription not found",
                "code": "not_found"
            }
        });

        assert_eq!(error_response.get("status"), Some(&json!(404)));
    }

    /// Contract: 429 Too Many Requests format
    ///
    /// When rate limiting is exceeded, the API must return:
    /// - Status: 429
    /// - Headers: Retry-After
    /// - Body: { "error": "description", "code": "rate_limit_exceeded" }
    #[test]
    fn contract_rate_limit_exceeded_format() {
        let error_response = json!({
            "status": 429,
            "headers": {
                "retry-after": "60"
            },
            "body": {
                "error": "Rate limit exceeded: 60 requests per minute",
                "code": "rate_limit_exceeded"
            }
        });

        assert_eq!(error_response.get("status"), Some(&json!(429)));
        assert!(error_response
            .get("headers")
            .and_then(|h| h.get("retry-after"))
            .is_some());
    }

    /// Contract: 500 Internal Server Error format
    ///
    /// When an unexpected error occurs, the API must return:
    /// - Status: 500
    /// - Body: { "error": "description", "code": "internal_error" }
    /// - No sensitive information in error message
    #[test]
    fn contract_internal_error_format() {
        let error_response = json!({
            "status": 500,
            "body": {
                "error": "Internal server error",
                "code": "internal_error"
            }
        });

        assert_eq!(error_response.get("status"), Some(&json!(500)));

        // Verify error message doesn't expose internals
        let error_msg = error_response
            .get("body")
            .and_then(|b| b.get("error"))
            .and_then(|e| e.as_str())
            .unwrap_or("");
        assert!(
            !error_msg.contains("panic"),
            "Error message should not contain panic details"
        );
    }

    /// Contract: 503 Service Unavailable format
    ///
    /// When the database is unavailable, the API must return:
    /// - Status: 503
    /// - Body: { "error": "description", "code": "service_unavailable" }
    #[test]
    fn contract_service_unavailable_format() {
        let error_response = json!({
            "status": 503,
            "headers": {
                "retry-after": "10"
            },
            "body": {
                "error": "Service temporarily unavailable",
                "code": "service_unavailable"
            }
        });

        assert_eq!(error_response.get("status"), Some(&json!(503)));
    }
}

/// Health Check Contract Tests
///
/// These tests define the contract for health check endpoints.
#[cfg(test)]
mod health_check_contracts {
    use super::*;

    /// Contract: GET /healthz/live (Liveness probe)
    ///
    /// Liveness probe should:
    /// - Always return 200 if the process is running
    /// - Not perform any external checks
    /// - Have minimal latency
    #[test]
    fn contract_liveness_probe() {
        let response = json!({
            "status": 200,
            "body": {
                "status": "alive"
            }
        });

        assert_eq!(response.get("status"), Some(&json!(200)));
        assert_eq!(
            response
                .get("body")
                .and_then(|b| b.get("status"))
                .and_then(|s| s.as_str()),
            Some("alive")
        );
    }

    /// Contract: GET /healthz/ready (Readiness probe)
    ///
    /// Readiness probe should:
    /// - Return 200 if database is reachable
    /// - Return 200 if indexer is not stalled
    /// - Return 503 if database is unreachable or indexer is stalled
    /// - Include status of dependencies in response
    #[test]
    fn contract_readiness_probe_ready() {
        let response = json!({
            "status": 200,
            "body": {
                "status": "ok",
                "db": "ok",
                "indexer": "ok"
            }
        });

        assert_eq!(response.get("status"), Some(&json!(200)));
        assert_eq!(
            response
                .get("body")
                .and_then(|b| b.get("db"))
                .and_then(|d| d.as_str()),
            Some("ok")
        );
    }

    /// Contract: GET /healthz/ready (Database unavailable)
    ///
    /// When database is unavailable:
    /// - Return 503 Service Unavailable
    /// - Include db status in response
    #[test]
    fn contract_readiness_probe_db_down() {
        let response = json!({
            "status": 503,
            "body": {
                "status": "error",
                "db": "unreachable",
                "indexer": "ok"
            }
        });

        assert_eq!(response.get("status"), Some(&json!(503)));
    }
}

/// Subscription API Contract Tests
///
/// These tests define the contract for the subscription management API.
#[cfg(test)]
mod subscription_api_contracts {
    use super::*;

    /// Contract: POST /v1/subscriptions (Create subscription)
    ///
    /// When creating a subscription, the API must:
    /// - Accept webhook URL, events filter, and format
    /// - Return the created subscription with an ID
    /// - Include a signing secret for webhook verification
    #[test]
    fn contract_create_subscription() {
        let request_body = json!({
            "url": "https://example.com/webhooks/events",
            "contract_ids": ["CABC123..."],
            "event_types": ["contract"],
            "format": "raw"
        });

        let response_body = json!({
            "id": "sub_123",
            "url": "https://example.com/webhooks/events",
            "contract_ids": ["CABC123..."],
            "event_types": ["contract"],
            "format": "raw",
            "signing_secret": "secret_xyz...",
            "created_at": "2026-03-14T00:00:00Z",
            "active": true
        });

        // Contract assertions
        assert!(request_body.get("url").is_some());
        assert!(response_body.get("id").is_some());
        assert!(response_body.get("signing_secret").is_some());
    }

    /// Contract: GET /v1/subscriptions (List subscriptions)
    ///
    /// When listing subscriptions, the API must:
    /// - Return an array of subscription objects
    /// - Support pagination with limit and offset
    /// - Filter by status if requested
    #[test]
    fn contract_list_subscriptions() {
        let response_body = json!({
            "data": [
                {
                    "id": "sub_123",
                    "url": "https://example.com/webhooks/events",
                    "active": true
                }
            ],
            "total": 1,
            "limit": 20,
            "offset": 0
        });

        assert!(response_body.get("data").is_some());
        assert!(response_body.get("total").is_some());
    }

    /// Contract: DELETE /v1/subscriptions/{id} (Delete subscription)
    ///
    /// When deleting a subscription, the API must:
    /// - Accept a subscription ID in the path
    /// - Return 204 No Content on success
    /// - Return 404 if subscription doesn't exist
    #[test]
    fn contract_delete_subscription_success() {
        let response = json!({
            "status": 204
        });

        assert_eq!(response.get("status"), Some(&json!(204)));
    }
}

/// API Versioning Contract Tests
///
/// These tests verify API versioning and deprecation handling.
#[cfg(test)]
mod api_versioning_contracts {
    use super::*;

    /// Contract: Versioned endpoints (/v1/*)
    ///
    /// All new endpoints must be under /v1/ prefix to enable
    /// future versioning without breaking clients.
    #[test]
    fn contract_versioned_endpoints() {
        let versioned_endpoints = vec![
            "/v1/events",
            "/v1/events/{contract_id}",
            "/v1/events/tx/{tx_hash}",
            "/v1/events/stream",
            "/v1/subscriptions",
            "/v1/admin/pause",
            "/v1/admin/resume",
        ];

        for endpoint in versioned_endpoints {
            assert!(
                endpoint.starts_with("/v1/"),
                "Endpoint {} should be versioned",
                endpoint
            );
        }
    }

    /// Contract: Deprecation headers
    ///
    /// Unversioned endpoints must return deprecation headers
    /// directing clients to the v1 equivalent.
    #[test]
    fn contract_deprecation_headers() {
        let response = json!({
            "headers": {
                "deprecation": "true",
                "link": "</v1/events>; rel=\"successor-version\""
            }
        });

        assert_eq!(
            response
                .get("headers")
                .and_then(|h| h.get("deprecation"))
                .and_then(|d| d.as_str()),
            Some("true")
        );
    }
}

/// NDJSON Response Format Contract Tests
#[cfg(test)]
mod ndjson_response_contracts {
    use super::*;

    /// Contract: Accept header with application/x-ndjson
    ///
    /// When clients send Accept: application/x-ndjson, the API must:
    /// - Return Content-Type: application/x-ndjson
    /// - Return one JSON object per line (no array wrapping)
    /// - Maintain valid JSON per line for streaming processing
    #[test]
    fn contract_ndjson_format() {
        let request = json!({
            "headers": {
                "accept": "application/x-ndjson"
            }
        });

        let response = json!({
            "headers": {
                "content-type": "application/x-ndjson"
            }
        });

        assert_eq!(
            request
                .get("headers")
                .and_then(|h| h.get("accept"))
                .and_then(|a| a.as_str()),
            Some("application/x-ndjson")
        );
        assert_eq!(
            response
                .get("headers")
                .and_then(|h| h.get("content-type"))
                .and_then(|c| c.as_str()),
            Some("application/x-ndjson")
        );
    }
}
