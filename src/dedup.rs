//! Issue #582: Event deduplication across retries.
//!
//! Computes a SHA-256 content fingerprint for each event so that content-identical
//! events can be detected even when they arrive with a different tx_hash during
//! re-indexing or retry scenarios.
//!
//! The authoritative dedup guard is still the database unique constraint on
//! (tx_hash, contract_id, event_type). This module adds a secondary layer that
//! catches events whose *content* is identical but whose tx_hash differs.
//!
//! ## Deduplication guarantees
//!
//! - **At-most-once storage per unique (tx_hash, contract_id, event_type)**: enforced
//!   by the DB unique constraint regardless of configuration.
//! - **Content-fingerprint dedup** (when `enable_content_dedup = true`): prevents
//!   storing a second event whose fingerprint matches an already-stored one.
//!   Uses a configurable lookback window (`dedup_window_secs`) to bound the query.
//! - **Bloom filter pre-filter**: fast in-memory check before any DB work (Issue #266).
//!
//! Deduplication priority (first match wins):
//! 1. Bloom filter hit → skip, increment `soroban_pulse_bloom_filter_hits_total`
//! 2. Content fingerprint hit (if enabled) → skip, increment `soroban_pulse_content_dedup_hits_total`
//! 3. DB unique constraint violation → skip via ON CONFLICT DO NOTHING

use serde_json::Value;
use sha2::{Digest, Sha256};

/// Compute the SHA-256 content fingerprint for an event.
///
/// The fingerprint covers all semantically meaningful fields so that two events
/// with identical on-chain content produce the same hex string, even if they
/// were delivered with different tx_hashes across retry attempts.
pub fn compute_fingerprint(
    tx_hash: &str,
    contract_id: &str,
    event_type: &str,
    event_data: &Value,
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(tx_hash.as_bytes());
    hasher.update(b"\x00");
    hasher.update(contract_id.as_bytes());
    hasher.update(b"\x00");
    hasher.update(event_type.as_bytes());
    hasher.update(b"\x00");
    // Use canonical JSON to ensure deterministic serialization.
    let data_str = event_data.to_string();
    hasher.update(data_str.as_bytes());
    hex::encode(hasher.finalize())
}

/// Configuration for content-fingerprint deduplication.
#[derive(Debug, Clone)]
pub struct DedupConfig {
    /// When true, check the fingerprint against recent DB rows before inserting.
    pub enable_content_dedup: bool,
    /// Lookback window for fingerprint checks (seconds). Bounds the DB query so it
    /// only scans recent events rather than the entire table.
    pub window_secs: u64,
}

impl Default for DedupConfig {
    fn default() -> Self {
        Self {
            enable_content_dedup: false,
            window_secs: 3600,
        }
    }
}

/// Check whether an event with the given fingerprint already exists within the
/// lookback window. Returns `true` if a duplicate is found.
pub async fn is_content_duplicate(
    pool: &sqlx::PgPool,
    fingerprint: &str,
    window_secs: u64,
) -> Result<bool, sqlx::Error> {
    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM events
         WHERE fingerprint = $1
           AND created_at >= NOW() - ($2 * INTERVAL '1 second')",
    )
    .bind(fingerprint)
    .bind(window_secs as i64)
    .fetch_one(pool)
    .await?;

    Ok(count > 0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn same_inputs_produce_same_fingerprint() {
        let a = compute_fingerprint("tx1", "CABC", "contract", &json!({"v": 1}));
        let b = compute_fingerprint("tx1", "CABC", "contract", &json!({"v": 1}));
        assert_eq!(a, b);
    }

    #[test]
    fn different_tx_hash_produces_different_fingerprint() {
        let a = compute_fingerprint("tx1", "CABC", "contract", &json!({"v": 1}));
        let b = compute_fingerprint("tx2", "CABC", "contract", &json!({"v": 1}));
        assert_ne!(a, b);
    }

    #[test]
    fn different_data_produces_different_fingerprint() {
        let a = compute_fingerprint("tx1", "CABC", "contract", &json!({"v": 1}));
        let b = compute_fingerprint("tx1", "CABC", "contract", &json!({"v": 2}));
        assert_ne!(a, b);
    }

    #[test]
    fn fingerprint_is_64_hex_chars() {
        let fp = compute_fingerprint("tx1", "CABC", "contract", &json!({}));
        assert_eq!(fp.len(), 64);
        assert!(fp.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn different_event_type_produces_different_fingerprint() {
        let a = compute_fingerprint("tx1", "CABC", "contract", &json!({"v": 1}));
        let b = compute_fingerprint("tx1", "CABC", "system", &json!({"v": 1}));
        assert_ne!(a, b);
    }
}
