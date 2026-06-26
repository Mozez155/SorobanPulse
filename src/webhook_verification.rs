/// Webhook Signature Verification Module (Issue #565)
///
/// This module provides utilities for webhook subscribers to verify the authenticity
/// of webhook requests signed by Soroban Pulse.
///
/// # Algorithm: HMAC-SHA256
///
/// Soroban Pulse signs all webhook requests (when a webhook secret is configured) using HMAC-SHA256.
///
/// ## Signature Format
///
/// The signature is sent in the `X-Signature-256` header with the format:
/// ```
/// X-Signature-256: sha256=<hex_digest>
/// ```
///
/// ## Verification Steps
///
/// 1. Extract the signature from the `X-Signature-256` header
/// 2. Extract the `sha256=` prefix (expect exactly "sha256=")
/// 3. Compute HMAC-SHA256 of the raw request body using your webhook secret as the key
/// 4. Compare the computed digest (in hex) with the provided signature using constant-time comparison
/// 5. If they match, the request is authentic
///
/// # Example Verification in Rust
///
/// ```rust
/// use hmac::{Hmac, Mac};
/// use sha2::Sha256;
/// use subtle::ConstantTimeEq;
///
/// type HmacSha256 = Hmac<Sha256>;
///
/// fn verify_webhook_signature(
///     header_value: &str,
///     secret: &str,
///     body: &[u8],
/// ) -> Result<(), &'static str> {
///     // Extract and validate prefix
///     let (algo, provided_sig) = header_value
///         .split_once('=')
///         .ok_or("Invalid signature header format")?;
///
///     if algo != "sha256" {
///         return Err("Unsupported signature algorithm");
///     }
///
///     // Compute expected signature
///     let mut mac = HmacSha256::new_from_slice(secret.as_bytes())
///         .map_err(|_| "Invalid secret length")?;
///     mac.update(body);
///     let computed = hex::encode(mac.finalize().into_bytes());
///
///     // Constant-time comparison
///     if provided_sig.as_bytes().ct_eq(computed.as_bytes()).into() {
///         Ok(())
///     } else {
///         Err("Signature verification failed")
///     }
/// }
/// ```
///
/// # Example Verification in Python
///
/// ```python
/// import hmac
/// import hashlib
///
/// def verify_webhook_signature(header_value: str, secret: str, body: bytes) -> bool:
///     \"\"\"Verify webhook signature using HMAC-SHA256.\"\"\"
///     # Extract signature from header (format: "sha256=<hex_digest>")
///     if not header_value.startswith("sha256="):
///         return False
///
///     provided_sig = header_value[7:]  # Skip "sha256=" prefix
///
///     # Compute expected signature
///     computed_sig = hmac.new(
///         secret.encode(),
///         body,
///         hashlib.sha256
///     ).hexdigest()
///
///     # Constant-time comparison
///     return hmac.compare_digest(provided_sig, computed_sig)
/// ```
///
/// # Example Verification in JavaScript
///
/// ```javascript
/// const crypto = require('crypto');
///
/// async function verifyWebhookSignature(headerValue, secret, body) {
///     // Extract signature from header
///     const [algo, providedSig] = headerValue.split('=');
///
///     if (algo !== 'sha256') {
///         return false;
///     }
///
///     // Compute expected signature
///     const computed = crypto
///         .createHmac('sha256', secret)
///         .update(body, 'utf-8')
///         .digest('hex');
///
///     // Constant-time comparison using timingSafeEqual
///     try {
///         crypto.timingSafeEqual(
///             Buffer.from(providedSig),
///             Buffer.from(computed)
///         );
///         return true;
///     } catch {
///         return false;
///     }
/// }
/// ```
///
/// # Security Considerations
///
/// - **Always use constant-time comparison**: Never use simple string equality (==) to compare signatures.
///   This prevents timing attacks that could leak information about the signature.
///
/// - **Secure secret storage**: Store your webhook secret securely (e.g., in environment variables or
///   secure vaults). Never commit secrets to version control.
///
/// - **Request body matching**: Always verify the signature against the exact request body you received.
///   If you modify the body before verification, the signature will be invalid.
///
/// - **Replay protection**: Consider implementing replay protection by checking request timestamps
///   or using a nonce-based system if available.
///
/// - **HTTPS only**: Always use HTTPS for webhook endpoints to prevent man-in-the-middle attacks.

use hmac::{Hmac, Mac};
use sha2::Sha256;
use subtle::ConstantTimeEq;

type HmacSha256 = Hmac<Sha256>;

/// Verify a webhook signature against a request body.
///
/// # Arguments
///
/// * `header_value` - The value of the `X-Signature-256` header (e.g., "sha256=<hex_digest>")
/// * `secret` - The webhook secret configured in Soroban Pulse
/// * `body` - The raw request body bytes
///
/// # Returns
///
/// `Ok(())` if the signature is valid, `Err(&str)` with a description if verification fails.
pub fn verify_signature(
    header_value: &str,
    secret: &str,
    body: &[u8],
) -> Result<(), &'static str> {
    // Parse the header format: "sha256=<hex_digest>"
    let (algo, provided_sig) = header_value
        .split_once('=')
        .ok_or("Invalid signature header format")?;

    if algo != "sha256" {
        return Err("Unsupported signature algorithm");
    }

    // Compute the expected signature
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes())
        .map_err(|_| "Invalid secret length")?;
    mac.update(body);
    let computed = hex::encode(mac.finalize().into_bytes());

    // Use constant-time comparison to prevent timing attacks
    if provided_sig.as_bytes().ct_eq(computed.as_bytes()).into() {
        Ok(())
    } else {
        Err("Signature verification failed: digest mismatch")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_verify_valid_signature() {
        let secret = "test_secret";
        let body = br#"{"event":"test","data":"hello"}"#;
        let signature = crate::webhook::sign_payload(secret, body);
        let header_value = format!("sha256={}", signature);

        assert!(verify_signature(&header_value, secret, body).is_ok());
    }

    #[test]
    fn test_verify_invalid_signature() {
        let secret = "test_secret";
        let body = br#"{"event":"test","data":"hello"}"#;
        let wrong_sig = "0000000000000000000000000000000000000000000000000000000000000000";
        let header_value = format!("sha256={}", wrong_sig);

        assert!(verify_signature(&header_value, secret, body).is_err());
    }

    #[test]
    fn test_verify_wrong_secret() {
        let secret = "test_secret";
        let body = br#"{"event":"test","data":"hello"}"#;
        let signature = crate::webhook::sign_payload(secret, body);
        let header_value = format!("sha256={}", signature);

        assert!(verify_signature(&header_value, "wrong_secret", body).is_err());
    }

    #[test]
    fn test_verify_tampered_body() {
        let secret = "test_secret";
        let body = br#"{"event":"test","data":"hello"}"#;
        let signature = crate::webhook::sign_payload(secret, body);
        let header_value = format!("sha256={}", signature);

        let tampered_body = br#"{"event":"test","data":"world"}"#;
        assert!(verify_signature(&header_value, secret, tampered_body).is_err());
    }

    #[test]
    fn test_verify_invalid_header_format() {
        let secret = "test_secret";
        let body = br#"{"event":"test","data":"hello"}"#;

        // Missing '=' separator
        assert!(verify_signature("sha256_invalid", secret, body).is_err());

        // Unsupported algorithm
        assert!(verify_signature("md5=somehash", secret, body).is_err());
    }
}
