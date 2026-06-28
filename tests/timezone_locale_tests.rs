//! Timezone and Locale Tests (Issue #594)
//!
//! Covers: multiple timezone configurations, locale-specific formatting,
//! timezone conversions, daylight saving transitions, and documentation.

use chrono::{DateTime, TimeZone, Utc};
use soroban_pulse::timezone_locale::{
    convert_to_all_timezones, detect_dst_transition, format_datetime_for_locale,
    format_number_for_locale, parse_timestamp_lenient, scan_for_dst_transitions,
    validate_timestamp_range, DstTransitionKind, Locale, TimezoneConfig, TimezoneHandlingDoc,
    SumVerificationConfig,
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn utc(year: i32, month: u32, day: u32, hour: u32) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(year, month, day, hour, 0, 0).unwrap()
}

// ---------------------------------------------------------------------------
// Task 1 — Multiple tezone configurations
// ---------------------------------------------------------------------------

#[test]
fn test_utc_config_zero_offset() {
    let tz = TimezoneConfig::utc();
    assert_eq!(tz.offset_seconds, 0);
    assert_eq!(tz.dst_offset_seconds, 0);
    assert!(!tz.is_dst_active(6));
}

#[test]
fn test_us_eastern_standard_time_offset() {
    let tz = TimezoneConfig::us_eastern();
    // January — no DST
    assert_eq!(tz.effective_offset(1), -5 * 3600);
}

#[test]
fn test_us_eastern_dst_offset() {
    let tz = TimezoneConfig::us_eastern();
    // July — DST active
    assert_eq!(tz.effective_offset(7), -4 * 3600);
}

#[test]
fn test_us_pacific_standard_time_offset() {
    let tz = TimezoneConfig::us_pacific();
    assert_eq!(tz.effective_offset(1), -8 * 3600);
}

#[test]
fn test_us_pacific_dst_offset() {
    let tz = TimezoneConfig::us_pacific();
    assert_eq!(tz.effective_offset(7), -7 * 3600);
}

#[test]
fn test_central_european_standard_offset() {
    let tz = TimezoneConfig::central_europ);
    assert_eq!(tz.effective_offset(1), 3600); // CET = UTC+1
}

#[test]
fn test_central_european_dst_offset() {
    let tz = TimezoneConfig::central_european();
    assert_eq!(tz.effective_offset(7), 2 * 3600); // CEST = UTC+2
}

#[test]
fn test_japan_no_dst() {
    let tz = TimezoneConfig::japan();
    assert_eq!(tz.effective_offset(1), 9 * 3600);
    assert_eq!(tz.effective_offset(7), 9 * 3600);
    assert!(!tz.is_dst_active(7));
}

#[test]
fn test_west_africa_no_dst() {
    let tz = TimezoneConfig::west_africa();
    assert_eq!(tz.effective_offset(1), 3600);
    assert_eq!(tz.effective_offset(7), 3600);
    assert!(!tz.is_dst_active(6));
}

#[test]
fn test_dst_boundary_start_month() {
    let tz = TimezoneConfig::us_eastern();
    assert!(!tz.is_dst_active(2));  // Feb — off
    assert!(tz.is_dst_active(3));   // Mar — on
}

#[test]
fn test_dst_boundary_end_month() {
    let tz = TimezoneConfig::us_eastern();
    assert!(tz.is_dst_active(10));  // Oct — still on
    assert!(!tz.is_dst_active(11)Nov — off
}

// ---------------------------------------------------------------------------
// Task 2 — Locale-specific formatting
// ---------------------------------------------------------------------------

#[test]
fn test_locale_from_str_en_us() {
    assert_eq!(Locale::from_str("en-US"), Some(Locale::EnUs));
    assert_eq!(Locale::from_str("en_us"), Some(Locale::EnUs));
}

#[test]
fn test_locale_from_str_de() {
    assert_eq!(Locale::from_str("de"), Some(Locale::De));
    assert_eq!(Locale::from_str("de-DE"), Some(Locale::De));
}

#[test]
fn test_locale_from_str_unknown_returns_none() {
    assert_eq!(Locale::from_str("xx-ZZ"), None);
}

#[test]
fn test_format_datetime_en_us() {
    let dt = utc(2024, 1, 15, 9);
    let formatted = format_datetime_for_locale(&dt, &Locale::EnUs);
    assert_eq!(formatted, "01/15/2024 09:00:00");
}

#[test]
fn test_format_datetime_en_gb() {
    let dt = utc(2024, 1, 15, 9);
    let formatted = format_datetime_for_locale(&dt, &Locale::EnGb);
    assert_eq!(formatted,/01/2024 09:00:00");
}

#[test]
fn test_format_datetime_de() {
    let dt = utc(2024, 1, 15, 9);
    let formatted = format_datetime_for_locale(&dt, &Locale::De);
    assert_eq!(formatted, "15.01.2024 09:00:00");
}

#[test]
fn test_format_datetime_ja() {
    let dt = utc(2024, 1, 15, 9);
    let formatted = format_datetime_for_locale(&dt, &Locale::Ja);
    assert!(formatted.contains("2024年"));
    assert!(formatted.contains("01月"));
    assert!(formatted.contains("15日"));
}

#[test]
fn test_format_datetime_ng_same_as_gb() {
    let dt = utc(2024, 3, 20, 14);
    assert_eq!(
        format_datetime_for_locale(&dt, &Locale::Ng),
        format_datetime_for_locale(&dt, &Locale::EnGb)
    );
}

#[test]
fn test_format_number_en_us_thousands() {
    let formatted = format_number_for_locale(1_234_567.89, 2, &Locale::EnUs);
    assert_eq!(formatted, "1,234,567.89");
}

#[test]
fn test_format_number_de_thousands() {
    let formatted = format_number_for_locale(1_234_567.89, 2, &Locale::De);
    assert_eq!(form "1.234.567,89");
}

#[test]
fn test_format_number_fr_thousands() {
    let formatted = format_number_for_locale(1_234.5, 1, &Locale::Fr);
    assert_eq!(formatted, "1 234,5");
}

#[test]
fn test_format_number_no_decimals() {
    let formatted = format_number_for_locale(42.0, 0, &Locale::EnUs);
    assert_eq!(formatted, "42");
}

#[test]
fn test_format_number_negative() {
    let formatted = format_number_for_locale(-500.0, 2, &Locale::EnUs);
    assert_eq!(formatted, "-500.00");
}

// ---------------------------------------------------------------------------
// Task 3 — Timezone conversions
// ---------------------------------------------------------------------------

#[test]
fn test_convert_utc_to_eastern_winter() {
    let tz = TimezoneConfig::us_eastern();
    let utc_dt = utc(2024, 1, 15, 12); // noon UTC, January
    let local = tz.convert_from_utc(&utc_dt);
    assert_eq!(local.hour(), 7); // 12 - 5 = 07:00 EST
}

#[test]
fn test_convert_utc_to_eastern_summer() {
    let tz = TimezoneConfig::us_etern();
    let utc_dt = utc(2024, 7, 15, 12); // noon UTC, July
    let local = tz.convert_from_utc(&utc_dt);
    assert_eq!(local.hour(), 8); // 12 - 4 = 08:00 EDT
}

#[test]
fn test_convert_utc_to_japan() {
    let tz = TimezoneConfig::japan();
    let utc_dt = utc(2024, 6, 1, 0); // midnight UTC
    let local = tz.convert_from_utc(&utc_dt);
    assert_eq!(local.hour(), 9); // UTC+9
}

#[test]
fn test_convert_to_all_timezones_returns_all() {
    let utc_dt = utc(2024, 6, 15, 12);
    let zones = vec![
        TimezoneConfig::utc(),
        TimezoneConfig::us_eastern(),
        TimezoneConfig::japan(),
    ];
    let results = convert_to_all_timezones(&utc_dt, &zones);
    assert_eq!(results.len(), 3);
    assert!(results.contains_key("UTC"));
    assert!(results.contains_key("America/New_York"));
    assert!(results.contains_key("Asia/Tokyo"));
}

#[test]
fn test_parse_timestamp_utc_rfc3339() {
    let result = parse_timestamp_lenient("2024-06-15T12:00:00Z");
    assert!(result.is_ok());
    assert_eq!(result.unwrap().hour(), 12);
}

#[test]
fn test_parse_timestamp_with_offset() {
    let result = parse_timestamp_lenient("2024-06-15T07:00:00-05:00");
    assert!(result.is_ok());
    assert_eq!(result.unwrap().hour(), 12); // converted to UTC
}

#[test]
fn test_parse_timestamp_invalid_fails() {
    let result = parse_timestamp_lenient("not-a-date");
    assert!(result.is_err());
}

#[test]
fn test_validate_timestamp_range_valid() {
    let from = utc(2024, 1, 1, 0);
    let to = utc(2024, 12, 31, 23);
    assert!(validate_timestamp_range(&from, &to).is_ok());
}

#[test]
fn test_validate_timestamp_range_equal_ok() {
    let ts = utc(2024, 6, 1, 0);
    assert!(validate_timestamp_range(&ts, &ts).is_ok());
}

#[test]
fn test_validate_timestamp_range_inverted_fails() {
    let from = utc(2024, 12, 31, 23);
    let to = utc(2024, 1, 1, 0);
    assert!(validate_timestamp_range(&from, &to).is_err());
}

// ---------------------------------------------------------------------------
// Task 4 — Daylight saving traitions
// ---------------------------------------------------------------------------

#[test]
fn test_detect_dst_spring_forward_eastern() {
    let tz = TimezoneConfig::us_eastern();
    let before = utc(2024, 2, 28, 12); // February — standard
    let after = utc(2024, 3, 15, 12);  // March — DST active
    let transition = detect_dst_transition(&tz, &before, &after);
    assert!(transition.is_some());
    let t = transition.unwrap();
    assert_eq!(t.kind, DstTransitionKind::SpringForward);
    assert_eq!(t.offset_before_seconds, -5 * 3600);
    assert_eq!(t.offset_after_seconds, -4 * 3600);
}

#[test]
fn test_detect_dst_fall_back_eastern() {
    let tz = TimezoneConfig::us_eastern();
    let before = utc(2024, 10, 31, 12); // October — DST
    let after = utc(2024, 11, 15, 12);  // November — standard
    let transition = detect_dst_transition(&tz, &before, &after);
    assert!(transition.is_some());
    let t = transition.unwrap();
    assert_eq!(t.kind, DstTransitionKind::FallBack);
}

#[test]_no_dst_transition_within_same_period() {
    let tz = TimezoneConfig::us_eastern();
    let before = utc(2024, 6, 1, 0);
    let after = utc(2024, 7, 1, 0);
    assert!(detect_dst_transition(&tz, &before, &after).is_none());
}

#[test]
fn test_no_dst_transition_for_japan() {
    let tz = TimezoneConfig::japan();
    let before = utc(2024, 2, 1, 0);
    let after = utc(2024, 4, 1, 0);
    assert!(detect_dst_transition(&tz, &before, &after).is_none());
}

#[test]
fn test_scan_finds_both_transitions_in_a_year() {
    let tz = TimezoneConfig::us_eastern();
    // One timestamp per month covering the full year
    let timestamps: Vec<DateTime<Utc>> = (1..=12)
        .map(|m| utc(2024, m, 15, 12))
        .collect();
    let transitions = scan_for_dst_transitions(&tz, &timestamps);
    assert_eq!(transitions.len(), 2, "expect spring-forward and fall-back");
}

#[test]
fn test_scan_no_transitions_for_no_dst_zone() {
    let tz = TimezoneConfig::west_africa();
    let timestamps: Vec<DateTime<Utc>> = (1..=12)
        .map(|m| utc(2024, m, 15, 12))
        .collect();
    let transitions = scan_for_dst_transitions(&tz, &timestamps);
    assert!(transitions.is_empty());
}

#[test]
fn test_scan_empty_input() {
    let tz = TimezoneConfig::us_eastern();
    let transitions = scan_for_dst_transitions(&tz, &[]);
    assert!(transitions.is_empty());
}

// ---------------------------------------------------------------------------
// Task 5 — Documentation
// ---------------------------------------------------------------------------

#[test]
fn test_doc_storage_format_is_utc() {
    let doc = TimezoneHandlingDoc::generate();
    assert!(doc.storage_format.contains("UTC"));
}

#[test]
fn test_doc_lists_all_supported_timezones() {
    let doc = TimezoneHandlingDoc::generate();
    assert!(doc.supported_timezones.iter().any(|s| s.contains("UTC")));
    assert!(doc.supported_timezones.iter().any(|s| s.contains("New_York")));
    assert!(doc.supported_timezones.iter().any(|s| s.contains("Tokyo")));
    assert!(doc.supported_mezones.iter().any(|s| s.contains("Lagos")));
}

#[test]
fn test_doc_mentions_iso8601() {
    let doc = TimezoneHandlingDoc::generate();
    assert!(doc.api_input_format.contains("ISO 8601"));
}

#[test]
fn test_doc_dst_strategy_mentions_transitions() {
    let doc = TimezoneHandlingDoc::generate();
    assert!(doc.dst_strategy.to_lowercase().contains("dst"));
}
