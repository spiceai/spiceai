/*
Copyright 2026 The Spice.ai OSS Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

//! Integration tests for `object_store_occ` using AWS S3.
//!
//! These tests simulate distributed access by using multiple `ObjectState` instances
//! pointing to the same S3 bucket/prefix.
//!
//! Required environment variables:
//! - `AWS_S3_BUCKET`: S3 bucket name
//! - `AWS_S3_PREFIX`: Prefix within the bucket for test data
//!
//! AWS credentials are picked up from environment, profile, or instance metadata.

#![allow(clippy::expect_used)]

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use object_store::ObjectStore;
use object_store::aws::AmazonS3Builder;
use object_store_occ::{InsertResult, ObjectState, UpdateResult, WriteResult};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct TestRecord {
    id: String,
    version: u64,
    data: String,
}

fn create_store() -> Arc<dyn ObjectStore> {
    let bucket = std::env::var("AWS_S3_BUCKET")
        .expect("AWS_S3_BUCKET environment variable must be set to run integration tests");

    let store = AmazonS3Builder::from_env()
        .with_bucket_name(&bucket)
        .build()
        .expect("failed to build S3 store from environment");

    Arc::new(store)
}

fn get_test_prefix() -> String {
    let base_prefix = std::env::var("AWS_S3_PREFIX").unwrap_or_default();
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0);

    if base_prefix.is_empty() {
        format!("object_store_occ_test/{timestamp}/")
    } else {
        let base = base_prefix.trim_end_matches('/');
        format!("{base}/object_store_occ_test/{timestamp}/")
    }
}

#[tokio::test]
async fn test_concurrent_insert_conflict() {
    let store = create_store();
    let prefix = get_test_prefix();

    // Two separate ObjectState instances simulating distributed writers
    let state1: ObjectState<TestRecord> =
        ObjectState::new(Arc::clone(&store)).with_prefix(format!("{prefix}records/"));
    let state2: ObjectState<TestRecord> =
        ObjectState::new(Arc::clone(&store)).with_prefix(format!("{prefix}records/"));

    let record = TestRecord {
        id: "item1".to_string(),
        version: 1,
        data: "initial".to_string(),
    };

    // First writer inserts successfully
    let result1 = state1
        .insert("item1", &record)
        .await
        .expect("insert failed");
    assert_eq!(result1, InsertResult::Ok);

    // Second writer tries to insert the same key - should get AlreadyExists
    let result2 = state2
        .insert("item1", &record)
        .await
        .expect("insert failed");
    assert_eq!(result2, InsertResult::AlreadyExists);
}

#[tokio::test]
async fn test_concurrent_update_conflict() {
    let store = create_store();
    let prefix = get_test_prefix();

    let state1: ObjectState<TestRecord> =
        ObjectState::new(Arc::clone(&store)).with_prefix(format!("{prefix}records/"));
    let state2: ObjectState<TestRecord> =
        ObjectState::new(Arc::clone(&store)).with_prefix(format!("{prefix}records/"));

    let initial = TestRecord {
        id: "item1".to_string(),
        version: 1,
        data: "initial".to_string(),
    };

    // Insert initial record
    state1
        .insert("item1", &initial)
        .await
        .expect("insert failed");

    // Both readers fetch the current state (populating their caches with same ETag)
    let _ = state1.get("item1").await.expect("get failed");
    let _ = state2.get("item1").await.expect("get failed");

    // First writer updates successfully
    let update1 = TestRecord {
        id: "item1".to_string(),
        version: 2,
        data: "updated by writer 1".to_string(),
    };
    let result1 = state1
        .update("item1", &update1)
        .await
        .expect("update failed");
    assert_eq!(result1, UpdateResult::Ok);

    // Second writer tries to update with stale ETag - should get Conflict
    let update2 = TestRecord {
        id: "item1".to_string(),
        version: 2,
        data: "updated by writer 2".to_string(),
    };
    let result2 = state2
        .update("item1", &update2)
        .await
        .expect("update failed");

    match result2 {
        UpdateResult::Conflict { current } => {
            // Should see writer 1's update
            assert_eq!(current.version, 2);
            assert_eq!(current.data, "updated by writer 1");
        }
        other => panic!("expected Conflict, got {other:?}"),
    }
}

#[tokio::test]
async fn test_insert_or_update_distributed() {
    let store = create_store();
    let prefix = get_test_prefix();

    let state1: ObjectState<TestRecord> =
        ObjectState::new(Arc::clone(&store)).with_prefix(format!("{prefix}records/"));
    let state2: ObjectState<TestRecord> =
        ObjectState::new(Arc::clone(&store)).with_prefix(format!("{prefix}records/"));

    let record1 = TestRecord {
        id: "item1".to_string(),
        version: 1,
        data: "from writer 1".to_string(),
    };

    let record2 = TestRecord {
        id: "item1".to_string(),
        version: 1,
        data: "from writer 2".to_string(),
    };

    // First writer creates the record
    let result1 = state1
        .insert_or_update("item1", &record1)
        .await
        .expect("insert_or_update failed");
    assert_eq!(result1, WriteResult::Inserted);

    // Second writer updates (since record exists)
    let result2 = state2
        .insert_or_update("item1", &record2)
        .await
        .expect("insert_or_update failed");
    assert_eq!(result2, WriteResult::Updated);

    // Verify final state
    let final_record = state1.get("item1").await.expect("get failed");
    assert_eq!(
        final_record.map(|r| r.data),
        Some("from writer 2".to_string())
    );
}

#[tokio::test]
async fn test_refresh_sees_external_changes() {
    let store = create_store();
    let prefix = get_test_prefix();

    let state1: ObjectState<TestRecord> =
        ObjectState::new(Arc::clone(&store)).with_prefix(format!("{prefix}records/"));
    let state2: ObjectState<TestRecord> =
        ObjectState::new(Arc::clone(&store)).with_prefix(format!("{prefix}records/"));

    // Writer 1 creates some records
    for i in 0..3 {
        let record = TestRecord {
            id: format!("item{i}"),
            version: 1,
            data: format!("data {i}"),
        };
        state1
            .insert(&format!("item{i}"), &record)
            .await
            .expect("insert failed");
    }

    // Writer 2 has empty cache
    assert!(state2.get_cached("item0").is_none());
    assert!(state2.get_cached("item1").is_none());
    assert!(state2.get_cached("item2").is_none());

    // After refresh, writer 2 sees all records
    state2.refresh().await.expect("refresh failed");

    assert!(state2.get_cached("item0").is_some());
    assert!(state2.get_cached("item1").is_some());
    assert!(state2.get_cached("item2").is_some());
}

#[tokio::test]
async fn test_list_keys_distributed() {
    let store = create_store();
    let prefix = get_test_prefix();

    let state1: ObjectState<TestRecord> =
        ObjectState::new(Arc::clone(&store)).with_prefix(format!("{prefix}records/"));
    let state2: ObjectState<TestRecord> =
        ObjectState::new(Arc::clone(&store)).with_prefix(format!("{prefix}records/"));

    // Writer 1 creates some records
    for i in 0..3 {
        let record = TestRecord {
            id: format!("item{i}"),
            version: 1,
            data: format!("data {i}"),
        };
        state1
            .insert(&format!("key{i}"), &record)
            .await
            .expect("insert failed");
    }

    // Writer 2 can list all keys even without refresh
    let mut keys = state2.list_keys().await.expect("list_keys failed");
    keys.sort();
    assert_eq!(keys, vec!["key0", "key1", "key2"]);
}

#[tokio::test]
async fn test_sequential_updates_same_writer() {
    let store = create_store();
    let prefix = get_test_prefix();

    let state: ObjectState<TestRecord> =
        ObjectState::new(Arc::clone(&store)).with_prefix(format!("{prefix}records/"));

    let mut record = TestRecord {
        id: "counter".to_string(),
        version: 0,
        data: "initial".to_string(),
    };

    // Insert initial
    state
        .insert("counter", &record)
        .await
        .expect("insert failed");

    // Sequential updates should all succeed
    for i in 1..=5 {
        record.version = i;
        record.data = format!("update {i}");

        let result = state
            .update("counter", &record)
            .await
            .expect("update failed");
        assert_eq!(result, UpdateResult::Ok, "update {i} failed");
    }

    // Verify final state
    let final_record = state.get("counter").await.expect("get failed");
    assert_eq!(final_record.map(|r| r.version), Some(5));
}

#[tokio::test]
async fn test_update_after_external_modification() {
    let store = create_store();
    let prefix = get_test_prefix();

    let state1: ObjectState<TestRecord> =
        ObjectState::new(Arc::clone(&store)).with_prefix(format!("{prefix}records/"));
    let state2: ObjectState<TestRecord> =
        ObjectState::new(Arc::clone(&store)).with_prefix(format!("{prefix}records/"));

    let initial = TestRecord {
        id: "item1".to_string(),
        version: 1,
        data: "initial".to_string(),
    };

    // Writer 1 creates and caches
    state1
        .insert("item1", &initial)
        .await
        .expect("insert failed");

    // Writer 2 modifies externally
    let external_update = TestRecord {
        id: "item1".to_string(),
        version: 2,
        data: "external update".to_string(),
    };
    let _ = state2.get("item1").await; // Populate cache
    state2
        .update("item1", &external_update)
        .await
        .expect("update failed");

    // Writer 1 tries to update with stale cache
    let stale_update = TestRecord {
        id: "item1".to_string(),
        version: 2,
        data: "stale update".to_string(),
    };
    let result = state1
        .update("item1", &stale_update)
        .await
        .expect("update failed");

    // Should get conflict with the external update
    match result {
        UpdateResult::Conflict { current } => {
            assert_eq!(current.data, "external update");
        }
        other => panic!("expected Conflict, got {other:?}"),
    }

    // After conflict, cache should be updated - next update should succeed
    let retry_update = TestRecord {
        id: "item1".to_string(),
        version: 3,
        data: "retry after conflict".to_string(),
    };
    let retry_result = state1
        .update("item1", &retry_update)
        .await
        .expect("retry update failed");
    assert_eq!(retry_result, UpdateResult::Ok);
}

#[tokio::test]
async fn test_concurrent_async_writers() {
    let store = create_store();
    let prefix = get_test_prefix();

    // Create initial record
    let state: ObjectState<TestRecord> =
        ObjectState::new(Arc::clone(&store)).with_prefix(format!("{prefix}concurrent/"));

    let initial = TestRecord {
        id: "counter".to_string(),
        version: 0,
        data: "initial".to_string(),
    };
    state
        .insert("counter", &initial)
        .await
        .expect("insert failed");

    // Spawn two async writers that will race to update the same key
    let store1 = Arc::clone(&store);
    let store2 = Arc::clone(&store);
    let prefix1 = prefix.clone();
    let prefix2 = prefix.clone();

    let writer1 = tokio::spawn(async move {
        let state: ObjectState<TestRecord> =
            ObjectState::new(store1).with_prefix(format!("{prefix1}concurrent/"));

        let mut successes = 0;
        let mut conflicts = 0;

        for i in 1..=10 {
            // Fetch current state
            let current = state.get("counter").await.expect("get failed");
            let current_version = current.map_or(0, |r| r.version);

            let update = TestRecord {
                id: "counter".to_string(),
                version: current_version + 1,
                data: format!("writer1 update {i}"),
            };

            match state
                .update("counter", &update)
                .await
                .expect("update call failed")
            {
                UpdateResult::Ok => successes += 1,
                UpdateResult::Conflict { .. } => conflicts += 1,
                UpdateResult::NotFound => panic!("unexpected NotFound"),
            }

            // Small delay to interleave with other writer
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        }

        (successes, conflicts)
    });

    let writer2 = tokio::spawn(async move {
        let state: ObjectState<TestRecord> =
            ObjectState::new(store2).with_prefix(format!("{prefix2}concurrent/"));

        let mut successes = 0;
        let mut conflicts = 0;

        for i in 1..=10 {
            // Fetch current state
            let current = state.get("counter").await.expect("get failed");
            let current_version = current.map_or(0, |r| r.version);

            let update = TestRecord {
                id: "counter".to_string(),
                version: current_version + 1,
                data: format!("writer2 update {i}"),
            };

            match state
                .update("counter", &update)
                .await
                .expect("update call failed")
            {
                UpdateResult::Ok => successes += 1,
                UpdateResult::Conflict { .. } => conflicts += 1,
                UpdateResult::NotFound => panic!("unexpected NotFound"),
            }

            // Small delay to interleave with other writer
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        }

        (successes, conflicts)
    });

    let (result1, result2) = tokio::join!(writer1, writer2);
    let (successes1, conflicts1) = result1.expect("writer1 panicked");
    let (successes2, conflicts2) = result2.expect("writer2 panicked");

    // Both writers attempted 10 updates each
    assert_eq!(successes1 + conflicts1, 10);
    assert_eq!(successes2 + conflicts2, 10);

    // At least some updates should have succeeded from each writer
    // (unless extremely unlucky timing)
    println!("Writer1: {successes1} successes, {conflicts1} conflicts");
    println!("Writer2: {successes2} successes, {conflicts2} conflicts");

    // Total successful updates should equal final version
    let final_state = state.get("counter").await.expect("final get failed");
    let final_version = final_state.map_or(0, |r| r.version);

    // The final version should equal total successful updates
    assert_eq!(
        final_version,
        u64::try_from(successes1 + successes2).expect("overflow")
    );

    // We should have seen at least some conflicts (concurrent access)
    // This isn't guaranteed but is highly likely with the timing
    println!("Final version: {final_version}");
    println!("Total conflicts: {}", conflicts1 + conflicts2);
}

async fn write_with_retry(
    store: Arc<dyn ObjectStore>,
    prefix: String,
    writer_name: &str,
    target_increments: u32,
) -> u32 {
    let state: ObjectState<TestRecord> =
        ObjectState::new(store).with_prefix(format!("{prefix}retry/"));

    let mut successful_increments = 0;
    let mut total_attempts = 0;

    while successful_increments < target_increments {
        total_attempts += 1;

        // Fetch current state
        let current = state.get("counter").await.expect("get failed");
        let current_version = current.as_ref().map_or(0, |r| r.version);

        let update = TestRecord {
            id: "counter".to_string(),
            version: current_version + 1,
            data: format!("{writer_name} increment {}", successful_increments + 1),
        };

        match state
            .update("counter", &update)
            .await
            .expect("update failed")
        {
            UpdateResult::Ok => {
                successful_increments += 1;
            }
            UpdateResult::Conflict { .. } => {
                // Retry - the loop will fetch fresh state
            }
            UpdateResult::NotFound => panic!("unexpected NotFound"),
        }

        // Small delay
        tokio::time::sleep(tokio::time::Duration::from_millis(5)).await;
    }

    println!("{writer_name}: {successful_increments} increments in {total_attempts} attempts");
    total_attempts
}

#[tokio::test]
async fn test_concurrent_async_writers_with_retry() {
    let store = create_store();
    let prefix = get_test_prefix();

    // Create initial record
    let state: ObjectState<TestRecord> =
        ObjectState::new(Arc::clone(&store)).with_prefix(format!("{prefix}retry/"));

    let initial = TestRecord {
        id: "counter".to_string(),
        version: 0,
        data: "initial".to_string(),
    };
    state
        .insert("counter", &initial)
        .await
        .expect("insert failed");

    let store1 = Arc::clone(&store);
    let store2 = Arc::clone(&store);
    let prefix1 = prefix.clone();
    let prefix2 = prefix.clone();

    let writer1 =
        tokio::spawn(async move { write_with_retry(store1, prefix1, "Writer1", 5).await });

    let writer2 =
        tokio::spawn(async move { write_with_retry(store2, prefix2, "Writer2", 5).await });

    let (attempts1, attempts2) = tokio::join!(writer1, writer2);
    let attempts1 = attempts1.expect("writer1 panicked");
    let attempts2 = attempts2.expect("writer2 panicked");

    // Each writer successfully incremented 5 times
    // Final version should be 10 (5 + 5)
    let final_state = state.get("counter").await.expect("final get failed");
    let final_version = final_state.map_or(0, |r| r.version);

    assert_eq!(final_version, 10, "Expected 10 total increments");

    // Total attempts should be >= 10 (at least 10 successes needed)
    // and likely > 10 due to conflicts
    println!("Total attempts: {}", attempts1 + attempts2);
    assert!(
        attempts1 + attempts2 >= 10,
        "Should have at least 10 total attempts"
    );
}

/// Tests that an update fails with `Conflict` when an external process modifies the object
/// between our `get()` (which caches the `ETag`) and `update()` calls.
/// This verifies S3's conditional write mechanism (If-Match headers) at the protocol level.
#[tokio::test]
async fn test_update_races_with_external_write() {
    let store = create_store();
    let prefix = get_test_prefix();

    let state: ObjectState<TestRecord> =
        ObjectState::new(Arc::clone(&store)).with_prefix(format!("{prefix}race/"));

    let initial = TestRecord {
        id: "item".to_string(),
        version: 1,
        data: "initial".to_string(),
    };

    // Insert and cache ETag
    state.insert("item", &initial).await.expect("insert failed");
    let _ = state.get("item").await.expect("get failed"); // Cache ETag

    // External write directly to S3 (simulates another process bypassing ObjectState)
    let path = object_store::path::Path::from(format!("{prefix}race/item.json"));
    let external_record = TestRecord {
        id: "item".to_string(),
        version: 99,
        data: "external modification".to_string(),
    };
    let payload = serde_json::to_vec(&external_record).expect("serialize failed");
    store
        .put(&path, payload.into())
        .await
        .expect("external put failed");

    // Our update should fail with Conflict (ETag mismatch from external write)
    let our_update = TestRecord {
        id: "item".to_string(),
        version: 2,
        data: "our update".to_string(),
    };
    let result = state
        .update("item", &our_update)
        .await
        .expect("update call failed");

    match result {
        UpdateResult::Conflict { current } => {
            assert_eq!(current.version, 99, "Should see external modification");
            assert_eq!(current.data, "external modification");
        }
        other => panic!("Expected Conflict due to external write, got {other:?}"),
    }
}
