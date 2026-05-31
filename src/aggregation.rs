use axum::{
    extract::{Query, State},
    Json,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::PgPool;

use crate::{error::AppError, routes::AppState};

#[derive(Debug, Deserialize)]
pub struct AggregationParams {
    pub group_by: String,
    pub agg_fn: AggregationFunction,
    pub value_path: Option<String>,
    pub contract_id: Option<String>,
    pub contract_ids: Option<String>,
    pub from_ledger: Option<i64>,
    pub to_ledger: Option<i64>,
    pub limit: Option<i64>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum AggregationFunction {
    Count,
    Sum,
    Avg,
    Min,
    Max,
}

#[derive(Debug, Serialize)]
pub struct AggregationResult {
    pub group_value: Value,
    pub result: f64,
}

#[derive(Debug, Serialize)]
pub struct AggregationResponse {
    pub results: Vec<AggregationResult>,
    pub total_groups: usize,
    pub aggregation_function: AggregationFunction,
    pub group_by_path: String,
}

const MAX_AGGREGATION_GROUPS: i64 = 1000;
const ALLOWED_JSONB_PATHS: &[&str] = &[
    "$.token",
    "$.amount", 
    "$.from",
    "$.to",
    "$.topic",
    "$.type",
    "$.status",
];

pub async fn aggregate_events(
    State(state): State<AppState>,
    Query(params): Query<AggregationParams>,
) -> Result<Json<AggregationResponse>, AppError> {
    // Validate JSONB path to prevent injection
    validate_jsonb_path(&params.group_by)?;
    
    if let Some(ref value_path) = params.value_path {
        validate_jsonb_path(value_path)?;
    }

    // Build the aggregation query
    let (query, bind_values) = build_aggregation_query(&params)?;
    
    // Execute the query
    let rows = sqlx::query(&query);
    let mut bound_query = rows;
    
    // Bind parameters in order
    for value in bind_values {
        bound_query = match value {
            BindValue::String(s) => bound_query.bind(s),
            BindValue::I64(i) => bound_query.bind(i),
        };
    }
    
    let results = bound_query
        .fetch_all(&state.pool)
        .await
        .map_err(|e| AppError::Internal(format!("Aggregation query failed: {}", e)))?;

    // Parse results
    let mut aggregation_results = Vec::new();
    for row in results {
        let group_value: Value = row.try_get("group_value")
            .map_err(|e| AppError::Internal(format!("Failed to get group_value: {}", e)))?;
        let result: f64 = row.try_get("agg_result")
            .map_err(|e| AppError::Internal(format!("Failed to get agg_result: {}", e)))?;
        
        aggregation_results.push(AggregationResult {
            group_value,
            result,
        });
    }

    Ok(Json(AggregationResponse {
        total_groups: aggregation_results.len(),
        results: aggregation_results,
        aggregation_function: params.agg_fn,
        group_by_path: params.group_by,
    }))
}

fn validate_jsonb_path(path: &str) -> Result<(), AppError> {
    // Basic validation - must start with $. and contain only allowed characters
    if !path.starts_with("$.") {
        return Err(AppError::Validation(
            "JSONB path must start with '$.'".to_string()
        ));
    }

    // Check against allowlist for security
    if !ALLOWED_JSONB_PATHS.contains(&path) {
        return Err(AppError::Validation(
            format!("JSONB path '{}' is not allowed. Allowed paths: {:?}", path, ALLOWED_JSONB_PATHS)
        ));
    }

    // Additional validation - no SQL injection patterns
    let forbidden_patterns = &["--", "/*", "*/", ";", "DROP", "DELETE", "UPDATE", "INSERT"];
    let upper_path = path.to_uppercase();
    for pattern in forbidden_patterns {
        if upper_path.contains(pattern) {
            return Err(AppError::Validation(
                format!("JSONB path contains forbidden pattern: {}", pattern)
            ));
        }
    }

    Ok(())
}

#[derive(Debug)]
enum BindValue {
    String(String),
    I64(i64),
}

fn build_aggregation_query(params: &AggregationParams) -> Result<(String, Vec<BindValue>), AppError> {
    let mut query = String::from("SELECT ");
    let mut bind_values = Vec::new();
    let mut bind_index = 1;

    // Add the grouping column
    query.push_str(&format!("event_data->'{}' as group_value, ", 
        params.group_by.trim_start_matches("$.")));

    // Add the aggregation function
    match params.agg_fn {
        AggregationFunction::Count => {
            query.push_str("COUNT(*) as agg_result");
        }
        AggregationFunction::Sum => {
            let value_path = params.value_path.as_ref()
                .ok_or_else(|| AppError::Validation("value_path required for sum aggregation".to_string()))?;
            query.push_str(&format!("SUM((event_data->>'{}')::numeric) as agg_result", 
                value_path.trim_start_matches("$.")));
        }
        AggregationFunction::Avg => {
            let value_path = params.value_path.as_ref()
                .ok_or_else(|| AppError::Validation("value_path required for avg aggregation".to_string()))?;
            query.push_str(&format!("AVG((event_data->>'{}')::numeric) as agg_result", 
                value_path.trim_start_matches("$.")));
        }
        AggregationFunction::Min => {
            let value_path = params.value_path.as_ref()
                .ok_or_else(|| AppError::Validation("value_path required for min aggregation".to_string()))?;
            query.push_str(&format!("MIN((event_data->>'{}')::numeric) as agg_result", 
                value_path.trim_start_matches("$.")));
        }
        AggregationFunction::Max => {
            let value_path = params.value_path.as_ref()
                .ok_or_else(|| AppError::Validation("value_path required for max aggregation".to_string()))?;
            query.push_str(&format!("MAX((event_data->>'{}')::numeric) as agg_result", 
                value_path.trim_start_matches("$.")));
        }
    }

    query.push_str(" FROM events WHERE 1=1");

    // Add filters
    if let Some(ref contract_id) = params.contract_id {
        query.push_str(&format!(" AND contract_id = ${}", bind_index));
        bind_values.push(BindValue::String(contract_id.clone()));
        bind_index += 1;
    }

    if let Some(ref contract_ids) = params.contract_ids {
        let ids: Vec<&str> = contract_ids.split(',').map(|s| s.trim()).collect();
        if ids.len() > 20 {
            return Err(AppError::Validation("Maximum 20 contract IDs allowed".to_string()));
        }
        
        let placeholders: Vec<String> = (0..ids.len())
            .map(|i| format!("${}", bind_index + i))
            .collect();
        query.push_str(&format!(" AND contract_id IN ({})", placeholders.join(", ")));
        
        for id in ids {
            bind_values.push(BindValue::String(id.to_string()));
            bind_index += 1;
        }
    }

    if let Some(from_ledger) = params.from_ledger {
        query.push_str(&format!(" AND ledger >= ${}", bind_index));
        bind_values.push(BindValue::I64(from_ledger));
        bind_index += 1;
    }

    if let Some(to_ledger) = params.to_ledger {
        query.push_str(&format!(" AND ledger <= ${}", bind_index));
        bind_values.push(BindValue::I64(to_ledger));
        bind_index += 1;
    }

    // Add GROUP BY and ORDER BY
    query.push_str(&format!(" GROUP BY event_data->'{}' ORDER BY agg_result DESC", 
        params.group_by.trim_start_matches("$.")));

    // Add LIMIT for complexity control
    let limit = params.limit.unwrap_or(100).min(MAX_AGGREGATION_GROUPS);
    query.push_str(&format!(" LIMIT {}", limit));

    Ok((query, bind_values))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_jsonb_path() {
        // Valid paths
        assert!(validate_jsonb_path("$.token").is_ok());
        assert!(validate_jsonb_path("$.amount").is_ok());
        
        // Invalid paths
        assert!(validate_jsonb_path("token").is_err()); // Missing $.
        assert!(validate_jsonb_path("$.invalid").is_err()); // Not in allowlist
        assert!(validate_jsonb_path("$.token; DROP TABLE").is_err()); // SQL injection
    }

    #[test]
    fn test_build_aggregation_query() {
        let params = AggregationParams {
            group_by: "$.token".to_string(),
            agg_fn: AggregationFunction::Count,
            value_path: None,
            contract_id: Some("CABC123".to_string()),
            contract_ids: None,
            from_ledger: Some(1000),
            to_ledger: Some(2000),
            limit: Some(50),
        };

        let (query, bind_values) = build_aggregation_query(&params).unwrap();
        
        assert!(query.contains("COUNT(*) as agg_result"));
        assert!(query.contains("GROUP BY event_data->'token'"));
        assert!(query.contains("contract_id = $1"));
        assert!(query.contains("ledger >= $2"));
        assert!(query.contains("ledger <= $3"));
        assert!(query.contains("LIMIT 50"));
        
        assert_eq!(bind_values.len(), 3);
    }
}