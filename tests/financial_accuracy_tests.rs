//! Financial Accuracy Tests (Issue #593)
//!
//! Covers: data validation framework, event count reconciliation,
//! sum verification, accuracy reports, and the high-level runner.

use chrono::Utc;
use serde_json::json;
use soroban_pulse::financial_accuracy::{
    AccuracyReport, DataValidationFramework, EventCountReconciler, EventCountSnapshot,
    EventValidator, ReconciliationResult, SumVerificationConfig, SumVerifier,
    ValidationSeverity, run_accuracy_check,
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn valid_event(id: &str, ledger: i64, amount: f64) -> serde_json::Value {
    json!({
        "id": id,
        "contract_id": "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAB5NQ",
        "ledger": ledger,
        "timestamp": "2024-01-01T12:00:00Z",
        "event_data": {
            "amount": amount,
            "from": "GABC",
            "to": "GXYZ"
        }
    })
}

fn count_snapshot(source: &str, count: u64) -> EventCountSnapshot {
    EventCountSnapshot {
        source: source.to_string(),
        contract_id: None,
        from_ledger: 1000,
        to_ledger: 2000,
        count,
        captured_at: Utc::now(),
    }
}

// ---------------------------------------------------------------------------
// Task 1 — Data Validation Framework
// ---------------------------------------------------------------------------

#[test]
fn test_valid_event_passes_all_validators() {
    let fw = DataValidationFramework::new();
    let event = valid_event("evt-1", 1500, 100.0);
    let results = fw.validate_event(&event);
    assert!(results.iter().all(|r| r.valid), "all validators should pass");
}

#[test]
fn test_missing_required_fields_fails() {
    let fw = DataValidationFramework::new();
    let event = json!({ "ledger": 1500 }); // missing id, contract_id, timestamp
    let results = fw.validate_event(&event)    let has_error = results
        .iter()
        .any(|r| r.findings.iter().any(|f| f.severity == ValidationSeverity::Error));
    assert!(has_error, "missing fields should produce errors");
}

#[test]
fn test_negative_amount_is_error() {
    let fw = DataValidationFramework::new();
    let event = valid_event("evt-2", 1500, -50.0);
    let results = fw.validate_event(&event);
    let errors: Vec<_> = results
        .iter()
        .flat_map(|r| r.findings.iter())
        .filter(|f| f.severity == ValidationSeverity::Error && f.field.contains("amount"))
        .collect();
    assert!(!errors.is_empty(), "negative amount should be an error");
}

#[test]
fn test_oversized_amount_is_warning_not_error() {
    let fw = DataValidationFramework::new();
    let event = valid_event("evt-3", 1500, 2_000_000_000_000.0);
    let results = fw.validate_event(&event);
    let warnings: Vec<_> = results
        .iter()
        .flat_map(|r| r.findings.iter())
        .filter(|f| f.severity == ValidationSeverity::Warning)
        .collect();
    let errors: Vec<_> = results
        .iter()
        .flat_map(|r| r.findings.iter())
        .filter(|f| f.severity == ValidationSeverity::Error && f.field.contains("amount"))
        .collect();
    assert!(!warnings.is_empty(), "oversized amount should warn");
    assert!(errors.is_empty(), "oversized amount should not be an error");
}

#[test]
fn test_invalid_ledger_fails() {
    let fw = DataValidationFramework::new();
    let event = json!({
        "id": "evt-4",
        "contract_id": "CABC",
        "ledger": -1,
        "timestamp": "2024-01-01T12:00:00Z"
    });
    let results = fw.validate_event(&event);
    let errors: Vec<_> = results
        .iter()
        .flat_map(|r| r.findings.iter())
        .filter(|f| f.severity == ValidationSeverity::Error && f.field == "ledger")
        .collect();
    assert!(!errors.is_empty(), "negative ledger should be an error");
}

#[test]
fn test_invalid_timestamp_fails() {
    let fw = DataValidationFramework::new();
    let event = json!({
        "id": "evt-5",
        "contract_id": "CABC",
        "ledger": 1000,
        "timestamp": "not-a-date"
    });
    let results = fw.validate_event(&event);
    let errors: Vec<_> = results
        .iter()
        .flat_map(|r| r.findings.iter())
        .filter(|f| f.severity == ValidationSeverity::Error && f.field == "timestamp")
        .collect();
    assert!(!errors.is_empty(), "invalid timestamp should be an error");
}

#[test]
fn test_batch_valid_returns_true() {
    let fw = DataValidationFramework::new();
    let events = vec![
        valid_event("e1", 1000, 10.0),
        valid_event("e2", 1001, 20.0),
        valid_event("e3", 1002, 30.0),
    ];
    assert!(fw.batch_is_valid(&events));
}

#[test]
fn test_batch_with_invalid_event_returns_false() {
    let fw = DataValidationFramework::new();
    let events = vec![
        valid_event("e1", 1000, 10.0),
        json!({ "ledger": -5 }), // bad
    ];
    assert!(!fw.batch_is_valid(&events));
}

// ---------------------------------------------------------------------------
// Task 2 — Event Count Reconciliation
// ---------------------------------------------------------------------------

#[test]
fn test_reconciliation_exact_match() {
    let reconciler = EventCountReconciler::new(0);
    let primary = count_snapshot("db", 500);
    let secondary = count_snapshot("rpc", 500);
    let result = reconciler.reconcile(primary, secondary);
    assert!(result.matches);
    assert_eq!(result.delta, 0);
}

#[test]
fn test_reconciliation_within_tolerance() {
    let reconciler = EventCountReconciler::new(5);
    let primary = count_snapshot("db", 503);
    let secondary = count_snapshot("rpc", 500);
    let result = reconciler.reconcile(primary, secondary);
    assert!(result.matches, "delta of 3 should be within tolerance of 5");
}

#[test]
fn test_reconciliation_exceeds_tolerance() {
    let reconciler = EventCountReconciler::new(2);
    let primary = count_snapshot("db", 510);
    let secondary = count_snapshot("rpc", 500);
    let relt = reconciler.reconcile(primary, secondary);
    assert!(!result.matches, "delta of 10 should exceed tolerance of 2");
    assert_eq!(result.delta, 10);
}

#[test]
fn test_reconciliation_delta_pct_zero_secondary() {
    let reconciler = EventCountReconciler::new(0);
    let primary = count_snapshot("db", 0);
    let secondary = count_snapshot("rpc", 0);
    let result = reconciler.reconcile(primary, secondary);
    assert_eq!(result.delta_pct, 0.0);
}

#[test]
fn test_reconciliation_delta_pct_nonzero() {
    let reconciler = EventCountReconciler::new(100);
    let primary = count_snapshot("db", 110);
    let secondary = count_snapshot("rpc", 100);
    let result = reconciler.reconcile(primary, secondary);
    assert!((result.delta_pct - 10.0).abs() < 0.001);
}

// ---------------------------------------------------------------------------
// Task 3 — Sum Verification
// ---------------------------------------------------------------------------

#[test]
fn test_sum_verification_exact() {
    let events vec![
        valid_event("e1", 1000, 100.0),
        valid_event("e2", 1001, 200.0),
        valid_event("e3", 1002, 300.0),
    ];
    let config = SumVerificationConfig {
        field_path: "/event_data/amount".to_string(),
        tolerance: 0.0,
    };
    let result = SumVerifier::verify(&config, &events, 600.0);
    assert!(result.verified);
    assert!((result.actual_sum - 600.0).abs() < f64::EPSILON);
}

#[test]
fn test_sum_verification_within_tolerance() {
    let events = vec![
        valid_event("e1", 1000, 100.01),
        valid_event("e2", 1001, 199.99),
    ];
    let config = SumVerificationConfig {
        field_path: "/event_data/amount".to_string(),
        tolerance: 0.05,
    };
    let result = SumVerifier::verify(&config, &events, 300.0);
    assert!(result.verified, "delta within 0.05 tolerance should pass");
}

#[test]
fn test_sum_verification_fails_on_mismatch() {
    let events = vec![
        valid_event("e1", 1000, 100.0),
        valid_event("e2", 1001, 100.0),
    ];
    let config = SumVerificationConfig {
        field_path: "/event_data/amount".to_string(),
        tolerance: 0.0,
    };
    let result = SumVerifier::verify(&config, &events, 999.0);
    assert!(!result.verified);
}

#[test]
fn test_sum_verification_null_fields_counted() {
    let events = vec![
        valid_event("e1", 1000, 50.0),
        json!({   // no amount field
            "id": "e2", "contract_id": "C", "ledger": 1001,
            "timestamp": "2024-01-01T12:00:00Z",
            "event_data": {}
        }),
    ];
    let config = SumVerificationConfig {
        field_path: "/event_data/amount".to_string(),
        tolerance: 0.0,
    };
    let result = SumVerifier::verify(&config, &events, 50.0);
    assert_eq!(result.null_count, 1);
    assert!(result.verified);
}

#[test]
fn test_verify_many_all_pass() {
    let events = vec![valid_event("e1", 1000, 100.0)];
    let configs = vec![
        (
            SumVerificationConfig {
                field_path: "/event_data/amount".to_string(),
                tolerance: 0.0,
            },
            100.0,
        ),
    ];
    let results = SumVerifier::verify_many(&configs, &events);
    assert!(results.iter().all(|r| r.verified));
}

// ---------------------------------------------------------------------------
// Task 4 — Accuracy Reports
// ---------------------------------------------------------------------------

#[test]
fn test_accuracy_report_all_valid() {
    let events = vec![
        valid_event("e1", 1000, 10.0),
        valid_event("e2", 1001, 20.0),
    ];
    let fw = DataValidationFramework::new();
    let validation_results = fw.validate_batch(&events);
    let report = AccuracyReport::build(&validation_results, None, vec![]);
    assert!(report.passed);
    assert_eq!(report.event_count, 2);
    assert_eq!(report.invalid_count, 0);
    assert!((report.accuracy_pct - 100.0).abs() < f64::EPSILON);
}

#[test]
fn test_accuracy_report_with_invalid_events() {
    let events = vec![
        valid_event("e1", 1000, 10.0),
        json!("ledger": -1 }), // invalid
    ];
    let fw = DataValidationFramework::new();
    let validation_results = fw.validate_batch(&events);
    let report = AccuracyReport::build(&validation_results, None, vec![]);
    assert!(!report.passed);
    assert_eq!(report.invalid_count, 1);
    assert!(report.accuracy_pct < 100.0);
}

#[test]
fn test_accuracy_report_failed_reconciliation_fails_report() {
    let events = vec![valid_event("e1", 1000, 10.0)];
    let fw = DataValidationFramework::new();
    let validation_results = fw.validate_batch(&events);

    let reconciler = EventCountReconciler::new(0);
    let primary = count_snapshot("db", 1);
    let secondary = count_snapshot("rpc", 999); // massive mismatch
    let reconciliation = Some(reconciler.reconcile(primary, secondary));

    let report = AccuracyReport::build(&validation_results, reconciliation, vec![]);
    assert!(!report.passed, "failed reconciliation should fail the report");
}

#[test]
fn test_accuracy_report_summary_format() {
    let events = vec![valid_event("e1", 1000, 5.0)];
    let fw = DataValidationFramework::new();
    let validation_results = fw.validate_batch(&events);
    let report = AccuracyReport::build(&validation_results, None, vec![]);
    let summary = report.summary();
    assert!(summary.contains("AccuracyReport"));
    assert!(summary.contains("accuracy="));
    assert!(summary.contains("passed="));
}

// ---------------------------------------------------------------------------
// High-level runner
// ---------------------------------------------------------------------------

#[test]
fn test_run_accuracy_check_happy_path() {
    let events = vec![
        valid_event("e1", 1000, 50.0),
        valid_event("e2", 1001, 50.0),
    ];
    let secondary = count_snapshot("rpc", 2);
    let sum_checks = vec![(
        SumVerificationConfig {
            field_path: "/event_data/amount".to_string(),
            tolerance: 0.0,
        },
        100.0,
    )];
    let report = run_accuracy_check(&events, Some(secondary), sum_checks).unwrap();
    assert!(report.passed);
}

#[test]
fn test_run_accuracy_check_empty_events_errors() {
    let result = run_accuracy_check(&[], None, vec![]);
    assert!(result.is_err());
}
