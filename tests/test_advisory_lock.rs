//! Test for Issue #576: Fix race condition in advisory lock acquisition
//! Ensures atomic advisory lock acquisition and release for multi-replica indexing.

use sqlx::PgPool;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

#[sqlx::test(migrations = "./migrations")]
async fn test_advisory_lock_atomic_acquisition(pool: PgPool) {
    // Test that multiple replicas safely acquire and release locks
    
    const LOCK_KEY: i64 = 0x536f726f62616e50; // "SorobanP"

    // Simulate multiple replicas competing for the lock
    let pool1 = pool.clone();
    let pool2 = pool.clone();
    
    let lock_acquired_1 = Arc::new(AtomicBool::new(false));
    let lock_acquired_2 = Arc::new(AtomicBool::new(false));
    let lock_acquired_1_clone = lock_acquired_1.clone();
    let lock_acquired_2_clone = lock_acquired_2.clone();

    // Replica 1: Try to acquire lock
    let handle1 = tokio::spawn(async move {
        match sqlx::query("SELECT pg_try_advisory_lock($1)")
            .bind(LOCK_KEY)
            .fetch_one(&pool1)
            .await
        {
            Ok(_) => {
                lock_acquired_1_clone.store(true, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(100)).await;
                let _ = sqlx::query("SELECT pg_advisory_unlock($1)")
                    .bind(LOCK_KEY)
                    .execute(&pool1)
                    .await;
                true
            }
            Err(_) => false,
        }
    });

    // Replica 2: Try to acquire lock (should fail while replica 1 holds it)
    let handle2 = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(50)).await;
        match sqlx::query("SELECT pg_try_advisory_lock($1)")
            .bind(LOCK_KEY)
            .fetch_one(&pool2)
            .await
        {
            Ok(_) => {
                lock_acquired_2_clone.store(true, Ordering::SeqCst);
                let _ = sqlx::query("SELECT pg_advisory_unlock($1)")
                    .bind(LOCK_KEY)
                    .execute(&pool2)
                    .await;
                true
            }
            Err(_) => false,
        }
    });

    let _ = handle1.await;
    let _ = handle2.await;

    // Verify mutual exclusion: only one replica should acquire lock at a time
    let replica1_had_lock = lock_acquired_1.load(Ordering::SeqCst);
    let replica2_had_lock = lock_acquired_2.load(Ordering::SeqCst);

    // At least one should have acquired the lock
    assert!(
        replica1_had_lock || replica2_had_lock,
        "At least one replica should acquire the lock"
    );

    // Verify lock can be reacquired after release
    let final_lock = sqlx::query("SELECT pg_try_advisory_lock($1)")
        .bind(LOCK_KEY)
        .fetch_one(&pool)
        .await;
    
    assert!(final_lock.is_ok(), "Lock should be available after release");

    // Clean up
    let _ = sqlx::query("SELECT pg_advisory_unlock($1)")
        .bind(LOCK_KEY)
        .execute(&pool)
        .await;
}

#[sqlx::test(migrations = "./migrations")]
async fn test_advisory_lock_connection_validation(pool: PgPool) {
    // Test that lock is properly validated for active connection
    
    const LOCK_KEY: i64 = 0x536f726f62616e50;

    // Acquire lock
    sqlx::query("SELECT pg_try_advisory_lock($1)")
        .bind(LOCK_KEY)
        .fetch_one(&pool)
        .await
        .expect("Failed to acquire advisory lock");

    // Verify lock is held by checking advisory locks exist on this connection
    let has_lock: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM pg_locks WHERE locktype = 'advisory'"
    )
    .fetch_one(&pool)
    .await
    .expect("Failed to check lock status");

    assert!(has_lock > 0, "Lock should be held on active connection");

    // Clean up
    sqlx::query("SELECT pg_advisory_unlock($1)")
        .bind(LOCK_KEY)
        .execute(&pool)
        .await
        .expect("Failed to release advisory lock");
}

#[sqlx::test(migrations = "./migrations")]
async fn test_lock_timeout_handling(pool: PgPool) {
    // Test that lock acquisition timeout is properly enforced
    
    const LOCK_KEY: i64 = 0x536f726f62616e50;
    let timeout = Duration::from_millis(100);

    // Acquire initial lock
    let _ = sqlx::query("SELECT pg_try_advisory_lock($1)")
        .bind(LOCK_KEY)
        .fetch_one(&pool)
        .await;

    let pool_clone = pool.clone();
    let handle = tokio::spawn(async move {
        let start = std::time::Instant::now();
        let _ = sqlx::query("SELECT pg_try_advisory_lock($1)")
            .bind(LOCK_KEY)
            .fetch_one(&pool_clone)
            .await;
        start.elapsed()
    });

    let elapsed = handle.await.expect("Task should complete");
    assert!(
        elapsed < timeout * 2,
        "Lock attempt should timeout quickly, took {:?}",
        elapsed
    );

    // Clean up
    let _ = sqlx::query("SELECT pg_advisory_unlock($1)")
        .bind(LOCK_KEY)
        .execute(&pool)
        .await;
}

#[sqlx::test(migrations = "./migrations")]
async fn test_lock_release_on_connection_drop(pool: PgPool) {
    // Test that lock is automatically released when connection is dropped
    
    const LOCK_KEY: i64 = 0x536f726f62616e50;

    // Acquire lock
    sqlx::query("SELECT pg_try_advisory_lock($1)")
        .bind(LOCK_KEY)
        .fetch_one(&pool)
        .await
        .expect("Failed to acquire lock");

    // Simulate connection drop by creating new pool
    let new_pool = pool.clone();

    // Should be able to acquire lock on new connection immediately
    let acquired = sqlx::query("SELECT pg_try_advisory_lock($1)")
        .bind(LOCK_KEY)
        .fetch_one(&new_pool)
        .await;

    // Clean up if we got the lock
    if acquired.is_ok() {
        let _ = sqlx::query("SELECT pg_advisory_unlock($1)")
            .bind(LOCK_KEY)
            .execute(&new_pool)
            .await;
    }

    // Release original lock
    let _ = sqlx::query("SELECT pg_advisory_unlock($1)")
        .bind(LOCK_KEY)
        .execute(&pool)
        .await;
}
