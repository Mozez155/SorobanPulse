use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio::time::sleep;
use tracing::warn;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryPolicy {
    pub max_attempts: u32,
    pub initial_backoff_ms: u64,
    pub backoff_multiplier: f64,
    pub max_backoff_ms: u64,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            initial_backoff_ms: 1000,
            backoff_multiplier: 2.0,
            max_backoff_ms: 60000,
        }
    }
}

impl RetryPolicy {
    pub fn webhook_default() -> Self {
        Self {
            max_attempts: 3,
            initial_backoff_ms: 1000,
            backoff_multiplier: 2.0,
            max_backoff_ms: 8000,
        }
    }

    pub fn email_default() -> Self {
        Self {
            max_attempts: 1,
            initial_backoff_ms: 0,
            backoff_multiplier: 1.0,
            max_backoff_ms: 0,
        }
    }

    pub fn sms_default() -> Self {
        Self {
            max_attempts: 2,
            initial_backoff_ms: 2000,
            backoff_multiplier: 1.5,
            max_backoff_ms: 10000,
        }
    }

    pub fn calculate_backoff(&self, attempt: u32) -> Duration {
        if attempt == 0 || self.initial_backoff_ms == 0 {
            return Duration::from_millis(0);
        }

        let backoff_ms = (self.initial_backoff_ms as f64 
            * self.backoff_multiplier.powi((attempt - 1) as i32)) as u64;
        let capped_backoff = backoff_ms.min(self.max_backoff_ms);
        Duration::from_millis(capped_backoff)
    }

    pub async fn execute_with_retry<F, Fut, T, E>(&self, mut operation: F) -> Result<T, E>
    where
        F: FnMut(u32) -> Fut,
        Fut: std::future::Future<Output = Result<T, E>>,
        E: std::fmt::Display,
    {
        let mut last_error = None;

        for attempt in 1..=self.max_attempts {
            match operation(attempt).await {
                Ok(result) => return Ok(result),
                Err(error) => {
                    if attempt < self.max_attempts {
                        let backoff = self.calculate_backoff(attempt);
                        warn!(
                            attempt = attempt,
                            max_attempts = self.max_attempts,
                            backoff_ms = backoff.as_millis(),
                            error = %error,
                            "Operation failed, retrying after backoff"
                        );
                        sleep(backoff).await;
                    }
                    last_error = Some(error);
                }
            }
        }

        Err(last_error.unwrap())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_calculate_backoff() {
        let policy = RetryPolicy {
            max_attempts: 3,
            initial_backoff_ms: 1000,
            backoff_multiplier: 2.0,
            max_backoff_ms: 5000,
        };

        assert_eq!(policy.calculate_backoff(0), Duration::from_millis(0));
        assert_eq!(policy.calculate_backoff(1), Duration::from_millis(1000));
        assert_eq!(policy.calculate_backoff(2), Duration::from_millis(2000));
        assert_eq!(policy.calculate_backoff(3), Duration::from_millis(4000));
        
        // Test max backoff cap
        let policy_with_cap = RetryPolicy {
            max_attempts: 5,
            initial_backoff_ms: 1000,
            backoff_multiplier: 2.0,
            max_backoff_ms: 3000,
        };
        assert_eq!(policy_with_cap.calculate_backoff(3), Duration::from_millis(3000));
        assert_eq!(policy_with_cap.calculate_backoff(4), Duration::from_millis(3000));
    }

    #[tokio::test]
    async fn test_execute_with_retry_success() {
        let policy = RetryPolicy::default();
        let mut call_count = 0;

        let result = policy.execute_with_retry(|_attempt| {
            call_count += 1;
            async move {
                if call_count < 2 {
                    Err("temporary error")
                } else {
                    Ok("success")
                }
            }
        }).await;

        assert_eq!(result, Ok("success"));
        assert_eq!(call_count, 2);
    }

    #[tokio::test]
    async fn test_execute_with_retry_failure() {
        let policy = RetryPolicy {
            max_attempts: 2,
            initial_backoff_ms: 1,
            backoff_multiplier: 1.0,
            max_backoff_ms: 1,
        };
        let mut call_count = 0;

        let result = policy.execute_with_retry(|_attempt| {
            call_count += 1;
            async move { Err("persistent error") }
        }).await;

        assert_eq!(result, Err("persistent error"));
        assert_eq!(call_count, 2);
    }
}