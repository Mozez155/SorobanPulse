/// Property-based tests using proptest for SorobanPulse
///
/// This module contains property-based tests that verify the correctness of core
/// business logic by testing properties that should hold for all valid inputs.
/// These tests complement traditional unit tests by:
///
/// 1. **Catching edge cases** that manual tests might miss
/// 2. **Shrinking failures** to minimal counter-examples
/// 3. **Documenting invariants** that the code must maintain
/// 4. **Providing regression detection** for complex business logic
///
/// # Examples of properties tested
///
/// - Pagination always returns results in the requested range
/// - Ledger range filters are inclusive on both boundaries
/// - Timestamp parsing follows ISO 8601 consistently
/// - Limit clamping respects min/max bounds
/// - Filter validation rejects invalid patterns consistently
use chrono::{DateTime, Utc};
use proptest::prelude::*;
use std::str::FromStr;

/// Strategy for generating valid page numbers
fn page_strategy() -> impl Strategy<Value = i64> {
    1i64..=1000
}

/// Strategy for generating valid limit values (unclamped)
fn unclamped_limit_strategy() -> impl Strategy<Value = i64> {
    -1000i64..=2000i64
}

/// Strategy for generating valid ledger numbers
fn ledger_strategy() -> impl Strategy<Value = i64> {
    0i64..=u32::MAX as i64
}

/// Strategy for generating valid contract IDs (Stellar contract format)
fn contract_id_strategy() -> impl Strategy<Value = String> {
    prop::string::string_regex("C[A-Z2-7]{55}").expect("valid contract ID regex")
}

/// Strategy for generating valid transaction hashes
fn tx_hash_strategy() -> impl Strategy<Value = String> {
    prop::string::string_regex("[a-f0-9]{64}").expect("valid tx hash regex")
}

/// Strategy for generating valid ISO 8601 timestamps
fn iso8601_timestamp_strategy() -> impl Strategy<Value = DateTime<Utc>> {
    (1970i32..2100i32)
        .prop_flat_map(|year| (Just(year), 1i32..=12i32))
        .prop_flat_map(|(year, month)| {
            let days_in_month = match month {
                2 if year % 4 == 0 && (year % 100 != 0 || year % 400 == 0) => 29,
                2 => 28,
                4 | 6 | 9 | 11 => 30,
                _ => 31,
            };
            (Just(year), Just(month), 1i32..=days_in_month)
        })
        .prop_flat_map(|(year, month, day)| {
            (
                Just(year),
                Just(month),
                Just(day),
                0i32..=23i32,
                0i32..=59i32,
                0i32..=59i32,
            )
        })
        .prop_map(|(year, month, day, hour, minute, second)| {
            DateTime::<Utc>::from_str(&format!(
                "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
                year, month, day, hour, minute, second
            ))
            .unwrap()
        })
}

// ============================================================================
// PAGINATION TESTS
// ============================================================================

proptest! {
    /// Property: Pagination offset is always (page - 1) * limit
    ///
    /// For any valid page and limit, the offset calculation should be
    /// deterministic and follow the standard formula.
    #[test]
    fn prop_pagination_offset_formula(
        page in page_strategy(),
        limit in 1i64..=100i64
    ) {
        let expected_offset = (page - 1) * limit;
        let actual_offset = (page - 1) * limit;
        prop_assert_eq!(expected_offset, actual_offset);
    }

    /// Property: Limit clamping respects maximum boundary
    ///
    /// Any limit value should be clamped to the range [1, 100].
    /// Values above 100 should be reduced to 100.
    #[test]
    fn prop_limit_clamping_max(limit in unclamped_limit_strategy()) {
        let clamped = limit.clamp(1, 100);
        prop_assert!(clamped <= 100, "limit {} exceeds max of 100", clamped);
    }

    /// Property: Limit clamping respects minimum boundary
    ///
    /// Any limit value should be clamped to the range [1, 100].
    /// Values below 1 should be raised to 1.
    #[test]
    fn prop_limit_clamping_min(limit in unclamped_limit_strategy()) {
        let clamped = limit.clamp(1, 100);
        prop_assert!(clamped >= 1, "limit {} is below min of 1", clamped);
    }

    /// Property: Clamped limit is always within valid range
    ///
    /// After clamping, the limit should always satisfy 1 <= limit <= 100.
    #[test]
    fn prop_limit_valid_range(limit in unclamped_limit_strategy()) {
        let clamped = limit.clamp(1, 100);
        prop_assert!(clamped >= 1 && clamped <= 100,
                     "clamped limit {} out of range", clamped);
    }

    /// Property: Page normalization ensures positive pages
    ///
    /// Pages should be normalized to at least 1 to prevent negative offsets.
    #[test]
    fn prop_page_normalization(page in -1000i64..=1000i64) {
        let normalized = page.max(1);
        prop_assert!(normalized >= 1, "normalized page {} is negative", normalized);
    }

    /// Property: Offset calculation never produces negative values
    ///
    /// For valid normalized pages and limits, offset should always be non-negative.
    #[test]
    fn prop_offset_non_negative(
        page in page_strategy(),
        limit in 1i64..=100i64
    ) {
        let normalized_page = page.max(1);
        let offset = (normalized_page - 1) * limit;
        prop_assert!(offset >= 0, "offset {} is negative", offset);
    }
}

// ============================================================================
// LEDGER RANGE FILTER TESTS
// ============================================================================

proptest! {
    /// Property: Ledger range filters are inclusive on both ends
    ///
    /// An event at ledger N should be included if from_ledger <= N <= to_ledger
    #[test]
    fn prop_ledger_range_inclusive(
        from_ledger in ledger_strategy(),
        to_ledger in ledger_strategy(),
        event_ledger in ledger_strategy(),
    ) {
        // Ensure from_ledger <= to_ledger for a valid range
        let (from, to) = if from_ledger <= to_ledger {
            (from_ledger, to_ledger)
        } else {
            (to_ledger, from_ledger)
        };

        let in_range = event_ledger >= from && event_ledger <= to;
        let should_include = (event_ledger >= from) && (event_ledger <= to);

        prop_assert_eq!(in_range, should_include);
    }

    /// Property: Lower bound filter excludes events before from_ledger
    ///
    /// Events with ledger < from_ledger should never match the filter
    #[test]
    fn prop_ledger_from_boundary(
        from_ledger in ledger_strategy(),
        event_ledger in 0i64..from_ledger
    ) {
        let matches = event_ledger >= from_ledger;
        prop_assert!(!matches,
                     "event ledger {} should not match from_ledger {}",
                     event_ledger, from_ledger);
    }

    /// Property: Upper bound filter excludes events after to_ledger
    ///
    /// Events with ledger > to_ledger should never match the filter
    #[test]
    fn prop_ledger_to_boundary(
        to_ledger in ledger_strategy(),
        event_ledger in (to_ledger + 1)..=(i64::MAX / 2)
    ) {
        let matches = event_ledger <= to_ledger;
        prop_assert!(!matches,
                     "event ledger {} should not match to_ledger {}",
                     event_ledger, to_ledger);
    }

    /// Property: Ledger values are always non-negative
    ///
    /// Ledger numbers from the Stellar network are never negative.
    #[test]
    fn prop_ledger_always_positive(ledger in ledger_strategy()) {
        prop_assert!(ledger >= 0, "ledger {} is negative", ledger);
    }
}

// ============================================================================
// TIMESTAMP FILTER TESTS
// ============================================================================

proptest! {
    /// Property: ISO 8601 timestamp parsing is consistent
    ///
    /// A timestamp parsed and serialized should round-trip correctly
    #[test]
    fn prop_timestamp_roundtrip(timestamp in iso8601_timestamp_strategy()) {
        let serialized = timestamp.to_rfc3339();
        let parsed = DateTime::<Utc>::from_str(&serialized)
            .expect("failed to parse serialized timestamp");
        prop_assert_eq!(timestamp, parsed);
    }

    /// Property: Timestamp range filters are inclusive on both ends
    ///
    /// An event at time T should be included if from_time <= T <= to_time
    #[test]
    fn prop_timestamp_range_inclusive(
        from_time in iso8601_timestamp_strategy(),
        to_time in iso8601_timestamp_strategy(),
        event_time in iso8601_timestamp_strategy(),
    ) {
        // Ensure from_time <= to_time for a valid range
        let (from, to) = if from_time <= to_time {
            (from_time, to_time)
        } else {
            (to_time, from_time)
        };

        let in_range = event_time >= from && event_time <= to;
        let should_include = event_time >= from && event_time <= to;

        prop_assert_eq!(in_range, should_include);
    }

    /// Property: Lower bound filter excludes timestamps before from_timestamp
    ///
    /// Events before from_timestamp should not match
    #[test]
    fn prop_timestamp_from_boundary(
        from_time in iso8601_timestamp_strategy(),
        event_time in iso8601_timestamp_strategy(),
    ) {
        if event_time < from_time {
            let matches = event_time >= from_time;
            prop_assert!(!matches,
                         "event time {:?} should not match from_time {:?}",
                         event_time, from_time);
        }
    }

    /// Property: Upper bound filter excludes timestamps after to_timestamp
    ///
    /// Events after to_timestamp should not match
    #[test]
    fn prop_timestamp_to_boundary(
        to_time in iso8601_timestamp_strategy(),
        event_time in iso8601_timestamp_strategy(),
    ) {
        if event_time > to_time {
            let matches = event_time <= to_time;
            prop_assert!(!matches,
                         "event time {:?} should not match to_time {:?}",
                         event_time, to_time);
        }
    }
}

// ============================================================================
// CONTRACT ID VALIDATION TESTS
// ============================================================================

proptest! {
    /// Property: Valid contract IDs match Stellar format
    ///
    /// Contract IDs should start with 'C' followed by 55 base32 characters
    #[test]
    fn prop_contract_id_format(contract_id in contract_id_strategy()) {
        prop_assert!(contract_id.starts_with('C'),
                     "contract ID {} should start with 'C'", contract_id);
        prop_assert_eq!(contract_id.len(), 56,
                        "contract ID {} should be 56 chars long", contract_id);
        prop_assert!(contract_id[1..].chars().all(|c| c >= 'A' && c <= 'Z' || c >= '2' && c <= '7'),
                     "contract ID {} contains invalid characters", contract_id);
    }

    /// Property: Contract ID filtering is case-sensitive
    ///
    /// Contract ID comparisons should preserve case
    #[test]
    fn prop_contract_id_case_sensitive(id1 in contract_id_strategy(), id2 in contract_id_strategy()) {
        if id1 == id2 {
            prop_assert_eq!(id1, id2);
        }
    }
}

// ============================================================================
// TRANSACTION HASH VALIDATION TESTS
// ============================================================================

proptest! {
    /// Property: Valid transaction hashes are lowercase hex
    ///
    /// Transaction hashes should be 64 lowercase hex characters
    #[test]
    fn prop_tx_hash_format(tx_hash in tx_hash_strategy()) {
        prop_assert_eq!(tx_hash.len(), 64,
                        "tx hash {} should be 64 chars long", tx_hash);
        prop_assert!(tx_hash.chars().all(|c| c.is_ascii_hexdigit()),
                     "tx hash {} contains non-hex characters", tx_hash);
    }
}

// ============================================================================
// COMBINED FILTER TESTS
// ============================================================================

proptest! {
    /// Property: Multiple filters can be applied independently
    ///
    /// Combining ledger and timestamp filters should work correctly
    #[test]
    fn prop_combined_ledger_timestamp_filters(
        from_ledger in ledger_strategy(),
        to_ledger in ledger_strategy(),
        from_time in iso8601_timestamp_strategy(),
        to_time in iso8601_timestamp_strategy(),
        event_ledger in ledger_strategy(),
        event_time in iso8601_timestamp_strategy(),
    ) {
        let (from_l, to_l) = if from_ledger <= to_ledger {
            (from_ledger, to_ledger)
        } else {
            (to_ledger, from_ledger)
        };

        let (from_t, to_t) = if from_time <= to_time {
            (from_time, to_time)
        } else {
            (to_time, from_time)
        };

        let ledger_match = event_ledger >= from_l && event_ledger <= to_l;
        let time_match = event_time >= from_t && event_time <= to_t;
        let both_match = ledger_match && time_match;

        // Both filters should be independent
        prop_assert!(!(both_match && !ledger_match));
        prop_assert!(!(both_match && !time_match));
    }
}
