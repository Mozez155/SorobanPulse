//! Test helper utilities for resilience and integration tests.

use std::time::Duration;
use tokio::time::timeout;

/// Poll a condition with configurable timeout and interval.
///
/// Repeatedly checks the condition function until it returns true or the timeout is exceeded.
/// This is more reliable than fixed sleep-based waits on slow CI runners.
///
/// # Arguments
/// * `condition` - A function that returns true when the desired state is reached
/// * `timeout_duration` - Maximum time to wait for the condition
/// * `poll_interval` - How often to check the condition
///
/// # Returns
/// * `Ok(())` if the condition became true within the timeout
/// * `Err(String)` if the timeout was exceeded
///
/// # Example
/// ```ignore
/// wait_for(
///     || async { db.count_events().await.unwrap() > 0 },
///     Duration::from_secs(30),
///     Duration::from_millis(100),
/// ).await?;
/// ```
pub async fn wait_for<F, Fut>(
    mut condition: F,
    timeout_duration: Duration,
    poll_interval: Duration,
) -> Result<(), String>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    timeout(timeout_duration, async {
        loop {
            if condition().await {
                return Ok(());
            }
            tokio::time::sleep(poll_interval).await;
        }
    })
    .await
    .map_err(|_| format!("Condition not met within {:?}", timeout_duration))?
}
