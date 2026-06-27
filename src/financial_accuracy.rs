//! Financial Accuracy Testing Module (Issue #593)
//!
//! Provides data validation framework, event count reconciliation,
//! sum verification, and accuracy reporting for Soroban event data.

use anyhow::{anyhow, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use tracing::{error, info, warn};

// ---------------------------------------------------------------------------
// Data Validation Framework
// ---------------------------------------------------------------------------

/// Severity of a validation finding.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ValidationSeverity {
    Error,
    Warning,
    Info,
}

/// A single validation finding produced by any validator.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationFinding {
    pub severity: ValidationSeverity,
    pub field: String,
    pub message: String,
    pub value: Option<Value>,
}

/// Aggregated result returned by a validator.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationResult {
    pub valid: bool,
    pub findings: Vec<ValidationFinding>,
    pub validated_at: DateTime<Utc>,
}

impl ValidationResult {
    pub fn ok() -> Self {
        Self {
            valid: true,
            findings: vec![],
            validated_at: Utc::now(),
        }
    }

    pub fn with_findings(findings: Vec<ValidationFinding>) -> Self {
        let valid = !findings
            .iter()
            .any(|f| f.severity == ValidationSeverity::Error);
        Self {
            valid,
            findings,
            validated_at: Utc::now(),
        }
    }
}

/// Trait every validator must implement.
pub trait EventValidator: Send + Sync {
    fn name(&self) -> &'static str;
    fn validate(&self, event: &Value) -> ValidationResult;
}

/// Runs all registered validators against a batch of events.
pub struct DataValidationFramework {
    validators: Vec<Box<dyn EventValidator>>,
}

impl DataValidationFramework {
    pub fn new() -> Self {
        Self {
            validators: vec![
                Box::new(RequiredFieldsValidator),
                Box::new(AmountRangeValidator),
                Box::new(LedgerSequenceValidator),
                Box::new(TimestampValidator),
            ],
        }
    }

    /// Validate a single event through all registered validators.
    pub fn validate_event(&self, event: &Value) -> Vec<ValidationResult> {
        self.validators
            .iter()
            .map(|v| {
                let result = v.validate(event);
                if !result.valid {
                    warn!(validator = v.name(), "validation failed for event");
                }
                result
            })
            .collect()
    }

    /// Validate a batch; returns one entry per event.
    pub fn validate_batch(&self, events: &[Value]) -> Vec<Vec<ValidationResult>> {
        events.iter().map(|e| self.validate_event(e)).collect()
    }

    /// Returns true only if every event passes every validator.
    pub fn batch_is_valid(&self, events: &[Value]) -> bool {
        self.validate_batch(events)
            .iter()
            .all(|results| results.iter().all(|r| r.valid))
    }
}

impl Default for DataValidationFramework {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Built-in validators
// ---------------------------------------------------------------------------

struct RequiredFieldsValidator;
impl EventValidator for RequiredFieldsValidator {
    fn name(&self) -> &'static str {
        "required_fields"
    }

    fn validate(&self, event: &Value) -> ValidationResult {
        let required = ["id", "contract_id", "ledger", "timestamp"];
        let mut findings = vec![];

        for field in &required {
            if event.get(field).is_none() {
                findings.push(ValidationFinding {
                    severity: ValidationSeverity::Error,
                    field: (*field).to_string(),
                    message: format!("required field `{field}` is missing"),
                    value: None,
                });
            }
        }

        ValidationResult::with_findings(findings)
    }
}

struct AmountRangeValidator;
impl EventValidator for AmountRangeValidator {
    fn name(&self) -> &'static str {
        "amount_range"
    }

    fn validate(&self, event: &Value) -> ValidationResult {
        let mut findings = vec![];

        // Look for amount fields nested inside event_data
        if let Some(amount_val) = event
            .get("event_data")
            .and_then(|d| d.get("amount"))
        {
            match amount_val.as_f64() {
                None => {
                    findings.push(ValidationFinding {
                        severity: ValidationSeverity::Error,
                        field: "event_data.amount".to_string(),
                        message: "amount is not a valid number".to_string(),
                        value: Some(amount_val.clone()),
                    });
                }
                Some(v) if v < 0.0 => {
                    findings.push(ValidationFinding {
                        severity: ValidationSeverity::Error,
                        field: "event_data.amount".to_string(),
                        message: format!("amount {v} is negative"),
                        value: Some(amount_val.clone()),
                    });
                }
                Some(v) if v > 1_000_000_000_000.0 => {
                    findings.push(ValidationFinding {
                        severity: ValidationSeverity::Warning,
                        field: "event_data.amount".to_string(),
                        message: format!("amount {v} exceeds expected maximum — verify correctness"),
                        value: Some(amount_val.clone()),
                    });
                }
                _ => {}
            }
        }

        ValidationRest::with_findings(findings)
    }
}

struct LedgerSequenceValidator;
impl EventValidator for LedgerSequenceValidator {
    fn name(&self) -> &'static str {
        "ledger_sequence"
    }

    fn validate(&self, event: &Value) -> ValidationResult {
        let mut findings = vec![];

        if let Some(ledger) = event.get("ledger") {
            match ledger.as_i64() {
                None => {
                    findings.push(ValidationFinding {
                        severity: ValidationSeverity::Error,
                        field: "ledger".to_string(),
                        message: "ledger is not a valid integer".to_string(),
                        value: Some(ledger.clone()),
                    });
                }
                Some(v) if v <= 0 => {
                    findings.push(ValidationFinding {
                        severity: ValidationSeverity::Error,
                        field: "ledger".to_string(),
                        message: format!("ledger {v} must be positive"),
                        value: Some(ledger.clone()),
                    });
                }
                _ => {}
            }
        }

        ValidationResult::with_findings(findings)
    }
}

struct TimestampValidator;
impl EventValidator for TimestampValidator {
    fn name(&self) -> &'static str {
        "timestamp"
    }

    fn validate(&self, event: &Value) -> ValidationResult {
        let mut findings = vec![];

        if let Some(ts) = event.get("timestamp").and_then(Value::as_str) {
            if ts.parse::<DateTime<Utc>>().is_err() {
                findings.push(ValidationFinding {
                    severity: ValidationSeverity::Error,
                    field: "timestamp".to_string(),
                    message: format!("timestamp `{ts}` is not valid RFC 3339"),
                    value: Some(Value::String(ts.to_string())),
                });
            }
        }

        ValidationResult::with_findings(findings)
    }
}

// ---------------------------------------------------------------------------
// Event Count Reconciliation (Task 2)
// ---------------------------------------------------------------------------

/// Counts from two sources to reconcile.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventCountSnapshot {
    pub source: String,
    pub contract_id: Option<String>,
    pub from_ledger: i64,
    pub to_ledger: i64,
    pub count: u64,
    pub captured_at: DateTime<Utc>,
}

/// Result of comparing two snapshots.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReconciliationResult {
    pub matches: bool,
    pub primary: EventCountSnapshot,
    pub secondary: EventCountSnapshot,
    pub delta: i64,
    pub delta_pct: f64,
    pub reconciled_at: DateTime<Utc>,
}

/// Reconciles event counts between a primary source (DB) and a secondary
/// source (e.g. RPC node or external feed).
pub struct EventCountReconciler {
    /// Tolerance in absolute count before flagging a mismatch.
    pub tolerance: u64,
}

impl EventCountReconciler {
    pub fn new(tolerance: u64) -> Self {
        Self { tolerance }
    }

    pub fn reconcile(
        &self,
        primary: EventCountSnapshot,
        secondary: EventCountSnapshot,
    ) -> ReconciliationResult {
        let delta = primary.count as i64 - secondary.count as i64;
        let delta_pct = if secondary.count == 0 {
            if primary.count == 0 { 0.0 } else { 100.0 }
        } else {
            (delta.abs() as f64 / secondary.count as f64) * 100.0
        };

        let matches = (delta.unsigned_abs()) <= self.tolerance;

        if !matches {
            warn!(
                primary_count = primary.count,
                secondary_count = secondary.count,
                delta,
                delta_pct,
                "event count reconciliation mismatch"
            );
        } else {
            info!(
                primary_count = primary.count,
                secondary_count = secondary.count,
                "event count reconciliation passed"
            );
        }

        ReconciliationResult {
            matches,
            primary,
            secondary,
            delta,
            delta_pct,
            reconciled_at: Utc::now(),
        }
    }
}

// ---------------------------------------------------------------------------
// Sum Verification (Task 3)
// ---------------------------------------------------------------------------

/// Configuration for a sum-verification run.
#[derive(Debug, Clone)]
pub struct SumVerificationConfig {
    /// JSON pointer path to the numeric field, e.g. "/event_data/amount"
    pub field_path: String,
    /// Maximum allowed deviation between expected and actual sums.
    pub tolerance: f64,
}

/// Result of verifying a sum across a batch of events.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SumVerificationResult {
    pub field_path: String,
    pub expected_sum: f64,
    pub actual_sum: f64,
    pub delta: f64,
    pub verified: bool,
    pub event_count: usize,
    pub null_count: usize,
    pub verified_at: DateTime<Utc>,
}

/// Extracts a numeric field from each event and verifies the total.
pub struct SumVerifier;

impl SumVerifier {
    /// Verify that the sum of `config.field_path` across `events` equals
    /// `expected_sum` within `config.tolerance`.
    pub fn verify(
        config: &SumVerificationConfig,
        events: &[Value],
        expected_sum: f64,
    ) -> SumVerificationResult {
        let mut actual_sum = 0.0_f64;
        let mut null_count = 0usize;

        for event in events {
            match event.pointer(&config.field_path).and_then(Value::as_f64) {
                Some(v) => actual_sum += v,
                None => null_count += 1,
            }
        }

        let delta = (actual_sum - expected_sum).abs();
        let verified = delta <= config.tolerance;

        if !verified {
            error!(
                field_path = %config.field_path,
                expected_sum,
                actual_sum,
                delta,
                "sum verification FAILED"
            );
        }

        SumVerificationResult {
            field_path: config.field_path.clone(),
            expected_sum,
            actual_sum,
            delta,
            verified,
            event_count: events.len(),
            null_count,
            verified_at: Utc::now(),
        }
    }

    /// Verify sums for multiple field paths at once.
    pub fn verify_many(
        configs: &[(SumVerificationConfig, f64)],
        events: &[Value],
    ) -> Vec<SumVerificationResult> {
        configs
            .iter()
            .map(|(cfg, expected)| Self::verify(cfg, events, *expected))
            .collect()
    }
}

// ---------------------------------------------------------------------------
// Accuracy Reports (Task 4)
// ---------------------------------------------------------------------------

/// Full accuracy report for a batch of events.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccuracyReport {
    pub report_id: String,
    pub generated_at: DateTime<Utc>,
    pub event_count: usize,
    pub valid_count: usize,
    pub invalid_count: usize,
    pub accuracy_pct: f64,
    pub reconciliation: Option<ReconciliationResult>,
    pub sum_verifications: Vec<SumVerificationResult>,
    pub findings_by_severity: HashMap<String, usize>,
    pub passed: bool,
}

impl AccuracyReport {
    /// Build an `AccuracyReport` from pre-computed components.
    pub fn build(
        validation_results: &[Vec<ValidationResult>],
        reconciliation: Option<ReconciliationResult>,
        sum_verifications: Vec<SumVerificationResult>,
    ) -> Self {
        let event_count = validation_results.len();
        let mut invalid_count = 0usize;
        let mut findings_by_severity: HashMap<String, usize> = HashMap::new();

        for results in validation_results {
            let event_valid = results.iter().all(|r| r.valid);
            if !event_valid {
                invalid_count += 1;
            }
            for result in results {
                for finding in &result.findings {
                    let key = format!("{:?}", finding.severity).to_lowercase();
                    *findings_by_severity.entry(key).or_insert(0) += 1;
                }
            }
        }

        let valid_count = event_count - invalid_count;
        let accuracy_pct = if event_count == 0 {
            100.0
        } else {
            (valid_count as f64 / event_count as f64) * 100.0
        };

        let reconciliation_ok = reconciliation.as_ref().map_or(true, |r| r.matches);
        let sums_ok = sum_verifications.iter().all(|s| s.verified);
        let passed = invalid_count == 0 && reconciliation_ok && sums_ok;

        AccuracyReport {
            report_id: uuid::Uuid::new_v4().to_string(),
            generated_at: Utc::now(),
            event_count,
            valid_count,
            invalid_count,
            accuracy_pct,
            reconciliation,
            sum_verifications,
            findings_by_severity,
            passed,
        }
    }

    /// Human-readable summary for logs / CLI output.
    pub fn summary(&self) -> String {
        format!(
            "AccuracyReport [{}] | events={} valid={} invalid={} accuracy={:.2}% passed={}",
            self.report_id,
            self.event_count,
            self.valid_count,
            self.invalid_count,
            self.accuracy_pct,
            self.passed
        )
    }
}

// ---------------------------------------------------------------------------
// High-level runner that wires everything together
// ---------------------------------------------------------------------------

/// Runs the full financial accuracy pipeline over a batch of raw events.
pub fn run_accuracy_check(
    events: &[Value],
    secondary_count: Option<EventCountSnapshot>,
    sum_checks: Vec<(SumVerificationConfig, f64)>,
) -> Result<AccuracyReport> {
    if events.is_empty() {
        return Err(anyhow!("cannot run accuracy check on empty event batch"));
    }

    let framework = DataValidationFramework::new();
    let validation_results = framework.validate_batch(events);

    let reconciliation = secondary_count.map(|secondary| {
        let primary = EventCountSnapshot {
            source: "database".to_string(),
            contract_id: secondary.contract_id.clone(),
            from_ledger: secondary.from_ledger,
            to_ledger: secondary.to_ledger,
            count: events.len() as u64,
            captured_at: Utc::now(),
        };
        EventCountReconciler::new(0).reconcile(primary, secondary)
    });

    let sum_verifications = SumVerifier::verify_many(&sum_checks, events);

    let report = AccuracyReport::build(&validation_results, reconciliation, sum_verifications);
    info!(summary = %report.summary(), "accuracy check complete");

    Ok(report)
}
