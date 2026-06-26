# Webhook Request Signing and Verification (Issue #565)

## Overview

Soroban Pulse supports HMAC-SHA256 request signing for webhook delivery. When you configure a webhook secret, all webhook requests to your endpoint will be signed, allowing you to verify their authenticity and ensure they come from Soroban Pulse.

## Signature Header

Every webhook request includes an `X-Signature-256` header with the signature in the following format:

```
X-Signature-256: sha256=<hex_digest>
```

- **Algorithm**: HMAC-SHA256
- **Format**: `sha256=<hexadecimal_digest>`
- **Key**: Your configured webhook secret

## Verification Algorithm

To verify a webhook signature:

1. **Extract the signature** from the `X-Signature-256` header
2. **Validate the format** - ensure it starts with `sha256=`
3. **Compute the expected signature** using HMAC-SHA256:
   - Key: Your webhook secret
   - Message: The raw request body
4. **Compare** the computed digest (in hexadecimal) with the provided signature using **constant-time comparison**
5. **Accept the request** only if the signatures match

## Implementation Examples

### Rust

Using the `hmac`, `sha2`, and `subtle` crates:

```rust
use hmac::{Hmac, Mac};
use sha2::Sha256;
use subtle::ConstantTimeEq;

type HmacSha256 = Hmac<Sha256>;

fn verify_webhook_signature(
    header_value: &str,
    secret: &str,
    body: &[u8],
) -> Result<(), String> {
    // Parse header format: "sha256=<hex_digest>"
    let (algo, provided_sig) = header_value
        .split_once('=')
        .ok_or("Invalid signature header format")?;

    if algo != "sha256" {
        return Err("Unsupported algorithm".to_string());
    }

    // Compute expected signature
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes())
        .map_err(|_| "Invalid secret")?;
    mac.update(body);
    let computed = hex::encode(mac.finalize().into_bytes());

    // Constant-time comparison
    if provided_sig.as_bytes().ct_eq(computed.as_bytes()).into() {
        Ok(())
    } else {
        Err("Signature mismatch".to_string())
    }
}
```

You can also use the built-in verification in Soroban Pulse:

```rust
use soroban_pulse::webhook_verification;

let result = webhook_verification::verify_signature(
    &header_value,
    &secret,
    &request_body,
);
```

### Python

Using the standard library:

```python
import hmac
import hashlib

def verify_webhook_signature(header_value: str, secret: str, body: bytes) -> bool:
    """Verify webhook signature using HMAC-SHA256."""
    # Parse header format
    if not header_value.startswith("sha256="):
        return False
    
    provided_sig = header_value[7:]  # Skip "sha256=" prefix
    
    # Compute expected signature
    computed_sig = hmac.new(
        secret.encode('utf-8'),
        body,
        hashlib.sha256
    ).hexdigest()
    
    # Constant-time comparison
    return hmac.compare_digest(provided_sig, computed_sig)
```

### JavaScript / Node.js

Using the crypto module:

```javascript
const crypto = require('crypto');

function verifyWebhookSignature(headerValue, secret, body) {
    // Parse header format
    const [algo, providedSig] = headerValue.split('=');
    
    if (algo !== 'sha256') {
        return false;
    }
    
    // Compute expected signature
    const computed = crypto
        .createHmac('sha256', secret)
        .update(body, 'utf-8')
        .digest('hex');
    
    // Constant-time comparison
    try {
        crypto.timingSafeEqual(
            Buffer.from(providedSig),
            Buffer.from(computed)
        );
        return true;
    } catch {
        return false;
    }
}
```

### Go

Using the crypto/hmac package:

```go
package main

import (
    "crypto/hmac"
    "crypto/sha256"
    "encoding/hex"
    "strings"
)

func verifyWebhookSignature(headerValue string, secret string, body []byte) bool {
    // Parse header format
    parts := strings.Split(headerValue, "=")
    if len(parts) != 2 || parts[0] != "sha256" {
        return false
    }
    
    providedSig := parts[1]
    
    // Compute expected signature
    h := hmac.New(sha256.New, []byte(secret))
    h.Write(body)
    computed := hex.EncodeToString(h.Sum(nil))
    
    // Constant-time comparison
    return hmac.Equal([]byte(providedSig), []byte(computed))
}
```

## Important Security Considerations

### ⚠️ Use Constant-Time Comparison

**Never** use simple string equality (==, ===) to compare signatures. This is vulnerable to timing attacks:

```python
# ❌ WRONG - vulnerable to timing attacks
if provided_sig == computed_sig:
    accept_request()

# ✅ CORRECT - constant-time comparison
if hmac.compare_digest(provided_sig, computed_sig):
    accept_request()
```

### Protect Your Secret

- Store your webhook secret in environment variables or secure vaults
- Never commit secrets to version control
- Rotate secrets periodically
- Use different secrets for different webhook endpoints if possible

### Request Body Integrity

- Always verify the signature against the **exact raw request body** you received
- If you parse or modify the body before verification, the signature will be invalid
- Keep the original bytes of the request body for verification

### Use HTTPS

- Always use HTTPS endpoints for webhooks
- This prevents man-in-the-middle attacks
- Verify SSL certificates properly

### Consider Additional Protection

- Implement request timestamp validation to prevent replay attacks
- Use request rate limiting on your endpoint
- Log all webhook attempts for security auditing

## Configuration

To enable webhook signing in Soroban Pulse:

1. Configure your webhook URL:
   ```
   WEBHOOK_URL=https://your-domain.com/webhook
   ```

2. Set your webhook secret:
   ```
   WEBHOOK_SECRET=your_secure_secret_here
   ```

When both are configured, all webhook deliveries will include the `X-Signature-256` header with HMAC-SHA256 signatures.

## Troubleshooting

### Signature Verification Failing

**Common causes:**
- Secret mismatch: Ensure you're using the exact secret configured in Soroban Pulse
- Body parsing: Make sure you're verifying against the raw request body, not parsed/modified JSON
- Header case: Header names are case-insensitive in HTTP, but the signature value is case-sensitive
- Character encoding: Ensure consistent UTF-8 encoding for both secret and body

**Debugging steps:**
1. Log both the received signature and computed signature (in development only)
2. Verify the header is being received correctly
3. Check that you're using the raw request body bytes
4. Ensure the secret matches what's configured in Soroban Pulse

## Examples of Common Mistakes

```python
# ❌ Mistake 1: Parsing JSON body before verification
data = json.loads(request.body)  # This changes the bytes!
signature = verify_signature(header, secret, json.dumps(data))  # Wrong - differs from original

# ✅ Correct: Verify first, then parse
raw_body = request.body
if verify_signature(header, secret, raw_body):
    data = json.loads(raw_body)  # Parse after verification

# ❌ Mistake 2: Missing the algorithm prefix
computed = "abc123def456..."  # Missing "sha256="
# Header is "sha256=abc123def456..."

# ✅ Correct: Include algorithm in comparison
if verify_signature("sha256=abc123def456...", secret, raw_body):
    pass

# ❌ Mistake 3: Wrong secret
# Using development secret in production, or vice versa
verify_signature(header, "wrong_secret", body)

# ✅ Correct: Use environment variable
secret = os.getenv("WEBHOOK_SECRET")
verify_signature(header, secret, body)
```

## Key Rotation

When you need to update your webhook secret:

1. Generate a new secret
2. Configure Soroban Pulse to use the new secret
3. Update your webhook handler to accept both old and new secrets temporarily
4. After verifying all webhooks are using the new secret, remove the old one

```python
def verify_webhook_signature(header_value, body, old_secret=None):
    secret = os.getenv("WEBHOOK_SECRET")
    
    # Try current secret
    if verify_signature(header_value, secret, body):
        return True
    
    # Try old secret during rotation period
    if old_secret and verify_signature(header_value, old_secret, body):
        return True
    
    return False
```

## Support

If you have issues with webhook signing verification:

1. Check the logs for errors
2. Verify your secret is configured correctly
3. Ensure you're using the raw request body
4. Use constant-time comparison
5. Check that the algorithm is "sha256"

For more information, refer to the [webhook documentation](./webhooks.md).
