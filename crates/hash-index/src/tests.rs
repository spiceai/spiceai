/*
Copyright 2025 The Spice.ai OSS Authors

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

//! Comprehensive tests for hash-index crate.
//!
//! This module contains extensive tests covering:
//! - Edge cases (empty, single row, large tables, boundaries)
//! - Null value handling
//! - Concurrent operations (read/write/delete)
//! - Data correctness validation
//! - Hash collision handling
//! - All supported data types

// Test-specific allows for cleaner test code
#![allow(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::cast_lossless,
    clippy::cast_possible_wrap,
    clippy::similar_names,
    clippy::uninlined_format_args,
    clippy::doc_markdown
)]

use std::collections::HashSet;
use std::sync::Arc;
use std::thread;

use arrow::array::{
    BinaryArray, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array, StringArray,
    UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use crate::{HashIndex, HashIndexBuilder, InsertResult, RowLocation, hash_key};

// =============================================================================
// Helper Functions
// =============================================================================

fn create_int64_batch(ids: Vec<i64>) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let id_array = Int64Array::from(ids);
    RecordBatch::try_new(schema, vec![Arc::new(id_array)]).expect("failed to create batch")
}

fn create_int64_batch_nullable(ids: Vec<Option<i64>>) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
    let id_array = Int64Array::from(ids);
    RecordBatch::try_new(schema, vec![Arc::new(id_array)]).expect("failed to create batch")
}

fn create_string_batch(ids: Vec<&str>) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, false)]));
    let id_array = StringArray::from(ids);
    RecordBatch::try_new(schema, vec![Arc::new(id_array)]).expect("failed to create batch")
}

fn create_string_batch_nullable(ids: Vec<Option<&str>>) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, true)]));
    let id_array = StringArray::from(ids);
    RecordBatch::try_new(schema, vec![Arc::new(id_array)]).expect("failed to create batch")
}

fn create_composite_batch(ids: Vec<i64>, names: Vec<&str>) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    let id_array = Int64Array::from(ids);
    let name_array = StringArray::from(names);
    RecordBatch::try_new(schema, vec![Arc::new(id_array), Arc::new(name_array)])
        .expect("failed to create batch")
}

// =============================================================================
// Edge Case Tests
// =============================================================================

#[test]
fn test_empty_index() {
    let index = HashIndex::new(vec!["id".to_string()]);
    assert!(index.is_empty());
    assert_eq!(index.len(), 0);
    assert!(index.get(&42_i64).is_none());
    assert!(index.get_by_hash(hash_key(&42_i64)).is_none());
}

#[test]
fn test_empty_partitions() {
    let partitions: Vec<Vec<RecordBatch>> = vec![];
    let index = HashIndexBuilder::new(vec!["id".to_string()])
        .build(&partitions)
        .expect("failed to build index");
    assert!(index.is_empty());
}

#[test]
fn test_empty_batches() {
    let batch = create_int64_batch(vec![]);
    let partitions = vec![vec![batch]];
    let index = HashIndexBuilder::new(vec!["id".to_string()])
        .build(&partitions)
        .expect("failed to build index");
    assert!(index.is_empty());
}

#[test]
fn test_single_row() {
    let batch = create_int64_batch(vec![42]);
    let partitions = vec![vec![batch]];
    let index = HashIndexBuilder::new(vec!["id".to_string()])
        .build(&partitions)
        .expect("failed to build index");

    assert_eq!(index.len(), 1);
    assert_eq!(index.get(&42_i64), Some(RowLocation::new(0, 0, 0)));
    assert!(index.get(&99_i64).is_none());
}

#[test]
fn test_large_table_10k() {
    let ids: Vec<i64> = (0..10_000).collect();
    let batch = create_int64_batch(ids.clone());
    let partitions = vec![vec![batch]];

    let index = HashIndexBuilder::new(vec!["id".to_string()])
        .with_expected_rows(10_000)
        .build(&partitions)
        .expect("failed to build index");

    assert_eq!(index.len(), 10_000);

    // Verify all keys are findable
    for (row, id) in ids.iter().enumerate() {
        let loc = index.get(id);
        assert_eq!(
            loc,
            Some(RowLocation::new(0, 0, row as u32)),
            "Failed to find id {id}"
        );
    }

    // Verify non-existent keys return None
    assert!(index.get(&10_001_i64).is_none());
    assert!(index.get(&(-1_i64)).is_none());
}

#[test]
fn test_large_table_100k() {
    let ids: Vec<i64> = (0..100_000).collect();
    let batch = create_int64_batch(ids);
    let partitions = vec![vec![batch]];

    let index = HashIndexBuilder::new(vec!["id".to_string()])
        .with_expected_rows(100_000)
        .build(&partitions)
        .expect("failed to build index");

    assert_eq!(index.len(), 100_000);

    // Spot check some keys
    assert!(index.get(&0_i64).is_some());
    assert!(index.get(&50_000_i64).is_some());
    assert!(index.get(&99_999_i64).is_some());
    assert!(index.get(&100_000_i64).is_none());
}

#[test]
fn test_boundary_values_int64() {
    let ids = vec![i64::MIN, i64::MIN + 1, -1, 0, 1, i64::MAX - 1, i64::MAX];
    let batch = create_int64_batch(ids.clone());
    let partitions = vec![vec![batch]];

    let index = HashIndexBuilder::new(vec!["id".to_string()])
        .build(&partitions)
        .expect("failed to build index");

    for (row, id) in ids.iter().enumerate() {
        let loc = index.get(id);
        assert_eq!(loc, Some(RowLocation::new(0, 0, row as u32)));
    }
}

#[test]
fn test_negative_values() {
    let ids: Vec<i64> = (-1000..0).collect();
    let batch = create_int64_batch(ids.clone());
    let partitions = vec![vec![batch]];

    let index = HashIndexBuilder::new(vec!["id".to_string()])
        .build(&partitions)
        .expect("failed to build index");

    for (row, id) in ids.iter().enumerate() {
        let loc = index.get(id);
        assert_eq!(loc, Some(RowLocation::new(0, 0, row as u32)));
    }
}

// =============================================================================
// Null Value Handling Tests
// =============================================================================

#[test]
fn test_null_values_excluded() {
    let ids = vec![Some(1), None, Some(3), None, Some(5)];
    let batch = create_int64_batch_nullable(ids);
    let partitions = vec![vec![batch]];

    let index = HashIndexBuilder::new(vec!["id".to_string()])
        .build(&partitions)
        .expect("failed to build index");

    // Only non-null values should be indexed
    assert_eq!(index.len(), 3);
    assert!(index.get(&1_i64).is_some());
    assert!(index.get(&3_i64).is_some());
    assert!(index.get(&5_i64).is_some());
}

#[test]
fn test_all_null_values() {
    let ids: Vec<Option<i64>> = vec![None, None, None];
    let batch = create_int64_batch_nullable(ids);
    let partitions = vec![vec![batch]];

    let index = HashIndexBuilder::new(vec!["id".to_string()])
        .build(&partitions)
        .expect("failed to build index");

    assert!(index.is_empty());
}

#[test]
fn test_null_strings() {
    let ids = vec![Some("alice"), None, Some("bob"), None];
    let batch = create_string_batch_nullable(ids);
    let partitions = vec![vec![batch]];

    let index = HashIndexBuilder::new(vec!["id".to_string()])
        .build(&partitions)
        .expect("failed to build index");

    assert_eq!(index.len(), 2);
    assert!(index.get(&"alice").is_some());
    assert!(index.get(&"bob").is_some());
}

// =============================================================================
// Multiple Partition and Batch Tests
// =============================================================================

#[test]
fn test_multiple_partitions() {
    let batch1 = create_int64_batch(vec![1, 2, 3]);
    let batch2 = create_int64_batch(vec![4, 5, 6]);
    let batch3 = create_int64_batch(vec![7, 8, 9]);

    let partitions = vec![vec![batch1], vec![batch2], vec![batch3]];

    let index = HashIndexBuilder::new(vec!["id".to_string()])
        .build(&partitions)
        .expect("failed to build index");

    assert_eq!(index.len(), 9);

    // Verify partition tracking
    assert_eq!(index.get(&1_i64), Some(RowLocation::new(0, 0, 0)));
    assert_eq!(index.get(&4_i64), Some(RowLocation::new(1, 0, 0)));
    assert_eq!(index.get(&7_i64), Some(RowLocation::new(2, 0, 0)));
}

#[test]
fn test_multiple_batches_per_partition() {
    let batch1 = create_int64_batch(vec![1, 2]);
    let batch2 = create_int64_batch(vec![3, 4]);
    let batch3 = create_int64_batch(vec![5, 6]);

    let partitions = vec![vec![batch1, batch2, batch3]];

    let index = HashIndexBuilder::new(vec!["id".to_string()])
        .build(&partitions)
        .expect("failed to build index");

    assert_eq!(index.len(), 6);

    // Verify batch tracking
    assert_eq!(index.get(&1_i64), Some(RowLocation::new(0, 0, 0)));
    assert_eq!(index.get(&3_i64), Some(RowLocation::new(0, 1, 0)));
    assert_eq!(index.get(&5_i64), Some(RowLocation::new(0, 2, 0)));
}

#[test]
fn test_mixed_partition_sizes() {
    let batch1 = create_int64_batch(vec![1]);
    let batch2 = create_int64_batch(vec![2, 3, 4, 5, 6, 7, 8, 9, 10]);
    let batch3 = create_int64_batch(vec![11, 12]);

    let partitions = vec![vec![batch1], vec![batch2], vec![batch3]];

    let index = HashIndexBuilder::new(vec!["id".to_string()])
        .build(&partitions)
        .expect("failed to build index");

    assert_eq!(index.len(), 12);

    // Verify row tracking within larger batch
    assert_eq!(index.get(&5_i64), Some(RowLocation::new(1, 0, 3)));
    assert_eq!(index.get(&10_i64), Some(RowLocation::new(1, 0, 8)));
}

// =============================================================================
// Duplicate Key Handling Tests
// =============================================================================

#[test]
fn test_duplicate_keys_rejected() {
    let batch = create_int64_batch(vec![1, 2, 1, 3]); // 1 is duplicated
    let partitions = vec![vec![batch]];

    let result = HashIndexBuilder::new(vec!["id".to_string()])
        .allow_duplicates(false)
        .build(&partitions);

    let _err = result.expect_err("expected duplicate key error");
}

#[test]
fn test_duplicate_keys_allowed() {
    let batch = create_int64_batch(vec![1, 2, 1, 3]); // 1 is duplicated
    let partitions = vec![vec![batch]];

    let index = HashIndexBuilder::new(vec!["id".to_string()])
        .allow_duplicates(true)
        .build(&partitions)
        .expect("failed to build index");

    // Should have one of the two locations for key 1
    assert!(index.get(&1_i64).is_some());
    assert_eq!(index.len(), 3); // 3 unique keys: 1, 2, 3
}

#[test]
fn test_insert_duplicate_returns_false() {
    let index = HashIndex::new(vec!["id".to_string()]);

    let hash = hash_key(&42_i64);
    let loc1 = RowLocation::simple(0, 0);
    let loc2 = RowLocation::simple(0, 1);

    assert!(matches!(index.insert(hash, loc1), InsertResult::Inserted));
    assert!(matches!(
        index.insert(hash, loc2),
        InsertResult::HashCollision(_)
    )); // Should return collision
    assert_eq!(index.len(), 1);
    assert_eq!(index.get_by_hash(hash), Some(loc1)); // Original preserved
}

// =============================================================================
// Insert, Delete, and Update Workflow Tests
// =============================================================================

#[test]
fn test_insert_workflow() {
    let index = HashIndex::new(vec!["id".to_string()]);

    // Insert sequence
    for i in 0..100_i64 {
        let hash = hash_key(&i);
        let loc = RowLocation::simple(0, i as u32);
        assert!(matches!(index.insert(hash, loc), InsertResult::Inserted));
    }

    assert_eq!(index.len(), 100);

    // Verify all insertions
    for i in 0..100_i64 {
        assert!(index.get(&i).is_some());
    }
}

#[test]
fn test_delete_workflow() {
    let index = HashIndex::new(vec!["id".to_string()]);

    // Insert first
    for i in 0..50_i64 {
        let hash = hash_key(&i);
        let loc = RowLocation::simple(0, i as u32);
        index.insert(hash, loc);
    }

    assert_eq!(index.len(), 50);

    // Delete half
    for i in 0..25_i64 {
        let hash = hash_key(&i);
        let removed = index.remove(hash);
        assert!(removed.is_some());
    }

    assert_eq!(index.len(), 25);

    // Verify deleted are gone
    for i in 0..25_i64 {
        assert!(index.get(&i).is_none());
    }

    // Verify remaining are still there
    for i in 25..50_i64 {
        assert!(index.get(&i).is_some());
    }
}

#[test]
fn test_delete_nonexistent() {
    let index = HashIndex::new(vec!["id".to_string()]);

    let hash = hash_key(&42_i64);
    let removed = index.remove(hash);
    assert!(removed.is_none());
    assert!(index.is_empty());
}

#[test]
fn test_insert_after_delete() {
    let index = HashIndex::new(vec!["id".to_string()]);

    let hash = hash_key(&42_i64);
    let loc1 = RowLocation::simple(0, 0);
    let loc2 = RowLocation::simple(0, 1);

    // Insert, delete, insert again
    index.insert(hash, loc1);
    index.remove(hash);
    assert!(matches!(index.insert(hash, loc2), InsertResult::Inserted)); // Should succeed

    assert_eq!(index.get_by_hash(hash), Some(loc2));
    assert_eq!(index.len(), 1);
}

#[test]
fn test_update_workflow() {
    let index = HashIndex::new(vec!["id".to_string()]);

    // Initial insert
    for i in 0..100_i64 {
        let hash = hash_key(&i);
        let loc = RowLocation::simple(0, i as u32);
        index.insert(hash, loc);
    }

    // Update (replace) some entries
    for i in 50..75_i64 {
        let hash = hash_key(&i);
        let new_loc = RowLocation::simple(1, i as u32);
        index.insert_or_replace(hash, new_loc);
    }

    assert_eq!(index.len(), 100); // Count unchanged

    // Verify updates
    for i in 50..75_i64 {
        let loc = index.get(&i);
        assert_eq!(loc, Some(RowLocation::simple(1, i as u32)));
    }

    // Verify non-updated entries
    for i in 0..50_i64 {
        let loc = index.get(&i);
        assert_eq!(loc, Some(RowLocation::simple(0, i as u32)));
    }
}

#[test]
fn test_clear_and_rebuild() {
    let index = HashIndex::new(vec!["id".to_string()]);

    // Initial insert
    for i in 0..100_i64 {
        let hash = hash_key(&i);
        let loc = RowLocation::simple(0, i as u32);
        index.insert(hash, loc);
    }

    assert_eq!(index.len(), 100);

    // Clear
    index.clear();
    assert!(index.is_empty());

    // Rebuild with new data
    for i in 100..200_i64 {
        let hash = hash_key(&i);
        let loc = RowLocation::simple(0, (i - 100) as u32);
        index.insert(hash, loc);
    }

    assert_eq!(index.len(), 100);

    // Verify old data gone
    for i in 0..100_i64 {
        assert!(index.get(&i).is_none());
    }

    // Verify new data present
    for i in 100..200_i64 {
        assert!(index.get(&i).is_some());
    }
}

// =============================================================================
// Concurrent Access Tests
// =============================================================================

#[test]
fn test_concurrent_reads() {
    let ids: Vec<i64> = (0..1000).collect();
    let batch = create_int64_batch(ids);
    let partitions = vec![vec![batch]];

    let index = Arc::new(
        HashIndexBuilder::new(vec!["id".to_string()])
            .build(&partitions)
            .expect("failed to build index"),
    );

    // Spawn multiple reader threads
    let handles: Vec<_> = (0..8)
        .map(|thread_id| {
            let index = Arc::clone(&index);
            thread::spawn(move || {
                for i in 0..1000_i64 {
                    let loc = index.get(&i);
                    assert!(loc.is_some(), "Thread {thread_id} failed to find {i}");
                }
            })
        })
        .collect();

    for handle in handles {
        handle.join().expect("Thread panicked");
    }
}

#[test]
fn test_concurrent_reads_and_writes() {
    let index = Arc::new(HashIndex::new(vec!["id".to_string()]));

    // Writer thread
    let writer_index = Arc::clone(&index);
    let writer = thread::spawn(move || {
        for i in 0..1000_i64 {
            let hash = hash_key(&i);
            let loc = RowLocation::simple(0, i as u32);
            writer_index.insert(hash, loc);
        }
    });

    // Reader threads (start slightly after)
    let handles: Vec<_> = (0..4)
        .map(|_| {
            let index = Arc::clone(&index);
            thread::spawn(move || {
                for _ in 0..100 {
                    // Keep reading, some may be found, some may not yet
                    for i in 0..100_i64 {
                        let _ = index.get(&i);
                    }
                }
            })
        })
        .collect();

    writer.join().expect("Writer panicked");
    for handle in handles {
        handle.join().expect("Reader panicked");
    }

    // After writer completes, all should be findable
    for i in 0..1000_i64 {
        assert!(index.get(&i).is_some());
    }
}

#[test]
fn test_concurrent_writes() {
    // Verify hash_key is thread-safe across different values
    for test_val in [0_i64, 1, 50, 99] {
        let main_hash = hash_key(&test_val);

        let handles: Vec<_> = (0..2)
            .map(|id| {
                thread::spawn(move || {
                    let h = hash_key(&test_val);
                    (id, h)
                })
            })
            .collect();

        for h in handles {
            let (id, thread_hash) = h.join().expect("thread panicked");
            assert_eq!(
                thread_hash, main_hash,
                "Thread {id} hash mismatch for {test_val}: main={main_hash}, thread={thread_hash}"
            );
        }
    }

    // Test with single thread first
    let index_single = Arc::new(HashIndex::new(vec!["id".to_string()]));
    for i in 0..100_i64 {
        let hash = hash_key(&i);
        let loc = RowLocation::simple(0, i as u32);
        index_single.insert(hash, loc);
    }

    for i in 0..100_i64 {
        assert!(
            index_single.get(&i).is_some(),
            "Single thread: missing key {i}"
        );
    }

    // Now concurrent case - compute hashes in threads
    let index = Arc::new(HashIndex::new(vec!["id".to_string()]));

    // Insert from 2 threads, 50 each = 100 total
    let handles: Vec<_> = (0..2)
        .map(|thread_id| {
            let index = Arc::clone(&index);
            thread::spawn(move || {
                let start = thread_id * 50;
                let end = start + 50;
                for i in start..end {
                    let key = i as i64;
                    let hash = hash_key(&key);
                    let loc = RowLocation::simple(thread_id as u32, (i - start) as u32);
                    index.insert(hash, loc);
                }
            })
        })
        .collect();

    for handle in handles {
        handle.join().expect("Writer panicked");
    }

    assert_eq!(
        index.len(),
        100,
        "Expected 100 entries after concurrent insert"
    );

    // Verify all keys are present
    for i in 0..100_i64 {
        assert!(
            index.get(&i).is_some(),
            "Missing key {i} after concurrent insert"
        );
    }
}

#[test]
fn test_concurrent_read_write_delete() {
    let index = Arc::new(HashIndex::new(vec!["id".to_string()]));

    // Pre-populate
    for i in 0..500_i64 {
        let hash = hash_key(&i);
        let loc = RowLocation::simple(0, i as u32);
        index.insert(hash, loc);
    }

    // Writer (adds new entries)
    let writer_index = Arc::clone(&index);
    let writer = thread::spawn(move || {
        for i in 500..1000_i64 {
            let hash = hash_key(&i);
            let loc = RowLocation::simple(1, (i - 500) as u32);
            writer_index.insert(hash, loc);
        }
    });

    // Deleter (removes some existing entries)
    let deleter_index = Arc::clone(&index);
    let deleter = thread::spawn(move || {
        for i in (0..250_i64).step_by(2) {
            let hash = hash_key(&i);
            deleter_index.remove(hash);
        }
    });

    // Readers
    let handles: Vec<_> = (0..2)
        .map(|_| {
            let index = Arc::clone(&index);
            thread::spawn(move || {
                for _ in 0..50 {
                    for i in 0..500_i64 {
                        let _ = index.get(&i);
                    }
                }
            })
        })
        .collect();

    writer.join().expect("Writer panicked");
    deleter.join().expect("Deleter panicked");
    for handle in handles {
        handle.join().expect("Reader panicked");
    }

    // Final verification
    assert!(index.len() > 600); // 500 new + some remaining from deletion
}

// =============================================================================
// All Data Type Tests
// =============================================================================

#[test]
fn test_int8_keys() {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int8, false)]));
    let array = Int8Array::from(vec![1_i8, 2, 3, -1, -128, 127]);
    let batch =
        RecordBatch::try_new(schema, vec![Arc::new(array)]).expect("failed to create batch");

    let partitions = vec![vec![batch]];
    let index = HashIndexBuilder::new(vec!["id".to_string()])
        .build(&partitions)
        .expect("failed to build index");

    assert_eq!(index.len(), 6);
}

#[test]
fn test_int16_keys() {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int16, false)]));
    let array = Int16Array::from(vec![1_i16, 2, -32768, 32767]);
    let batch =
        RecordBatch::try_new(schema, vec![Arc::new(array)]).expect("failed to create batch");

    let partitions = vec![vec![batch]];
    let index = HashIndexBuilder::new(vec!["id".to_string()])
        .build(&partitions)
        .expect("failed to build index");

    assert_eq!(index.len(), 4);
}

#[test]
fn test_int32_keys() {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
    let array = Int32Array::from(vec![1_i32, 2, i32::MIN, i32::MAX]);
    let batch =
        RecordBatch::try_new(schema, vec![Arc::new(array)]).expect("failed to create batch");

    let partitions = vec![vec![batch]];
    let index = HashIndexBuilder::new(vec!["id".to_string()])
        .build(&partitions)
        .expect("failed to build index");

    assert_eq!(index.len(), 4);
}

#[test]
fn test_uint8_keys() {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::UInt8, false)]));
    let array = UInt8Array::from(vec![0_u8, 1, 127, 255]);
    let batch =
        RecordBatch::try_new(schema, vec![Arc::new(array)]).expect("failed to create batch");

    let partitions = vec![vec![batch]];
    let index = HashIndexBuilder::new(vec!["id".to_string()])
        .build(&partitions)
        .expect("failed to build index");

    assert_eq!(index.len(), 4);
}

#[test]
fn test_uint16_keys() {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::UInt16, false)]));
    let array = UInt16Array::from(vec![0_u16, 1, 32767, 65535]);
    let batch =
        RecordBatch::try_new(schema, vec![Arc::new(array)]).expect("failed to create batch");

    let partitions = vec![vec![batch]];
    let index = HashIndexBuilder::new(vec!["id".to_string()])
        .build(&partitions)
        .expect("failed to build index");

    assert_eq!(index.len(), 4);
}

#[test]
fn test_uint32_keys() {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::UInt32, false)]));
    let array = UInt32Array::from(vec![0_u32, 1, u32::MAX / 2, u32::MAX]);
    let batch =
        RecordBatch::try_new(schema, vec![Arc::new(array)]).expect("failed to create batch");

    let partitions = vec![vec![batch]];
    let index = HashIndexBuilder::new(vec!["id".to_string()])
        .build(&partitions)
        .expect("failed to build index");

    assert_eq!(index.len(), 4);
}

#[test]
fn test_uint64_keys() {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::UInt64, false)]));
    let array = UInt64Array::from(vec![0_u64, 1, u64::MAX / 2, u64::MAX]);
    let batch =
        RecordBatch::try_new(schema, vec![Arc::new(array)]).expect("failed to create batch");

    let partitions = vec![vec![batch]];
    let index = HashIndexBuilder::new(vec!["id".to_string()])
        .build(&partitions)
        .expect("failed to build index");

    assert_eq!(index.len(), 4);
}

#[test]
fn test_binary_keys() {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Binary, false)]));
    let array = BinaryArray::from(vec![
        b"hello".as_slice(),
        b"world".as_slice(),
        b"".as_slice(),
        b"\x00\x01\x02".as_slice(),
    ]);
    let batch =
        RecordBatch::try_new(schema, vec![Arc::new(array)]).expect("failed to create batch");

    let partitions = vec![vec![batch]];
    let index = HashIndexBuilder::new(vec!["id".to_string()])
        .build(&partitions)
        .expect("failed to build index");

    assert_eq!(index.len(), 4);
}

// =============================================================================
// String Edge Case Tests
// =============================================================================

#[test]
fn test_empty_string_key() {
    let batch = create_string_batch(vec![""]);
    let partitions = vec![vec![batch]];

    let index = HashIndexBuilder::new(vec!["id".to_string()])
        .build(&partitions)
        .expect("failed to build index");

    assert_eq!(index.len(), 1);
    assert!(index.get(&"").is_some());
}

#[test]
fn test_unicode_string_keys() {
    let batch = create_string_batch(vec!["hello", "世界", "🎉", "αβγ", "مرحبا"]);
    let partitions = vec![vec![batch]];

    let index = HashIndexBuilder::new(vec!["id".to_string()])
        .build(&partitions)
        .expect("failed to build index");

    assert_eq!(index.len(), 5);
    assert!(index.get(&"世界").is_some());
    assert!(index.get(&"🎉").is_some());
    assert!(index.get(&"αβγ").is_some());
}

#[test]
fn test_long_string_keys() {
    let long_string = "a".repeat(10_000);
    let batch = create_string_batch(vec![&long_string, "short"]);
    let partitions = vec![vec![batch]];

    let index = HashIndexBuilder::new(vec!["id".to_string()])
        .build(&partitions)
        .expect("failed to build index");

    assert_eq!(index.len(), 2);
    assert!(index.get(&long_string.as_str()).is_some());
}

#[test]
fn test_whitespace_string_keys() {
    let batch = create_string_batch(vec![" ", "  ", "\t", "\n", "   spaces   "]);
    let partitions = vec![vec![batch]];

    let index = HashIndexBuilder::new(vec!["id".to_string()])
        .build(&partitions)
        .expect("failed to build index");

    assert_eq!(index.len(), 5);
    assert!(index.get(&" ").is_some());
    assert!(index.get(&"\t").is_some());
}

// =============================================================================
// Composite Key Tests
// =============================================================================

#[test]
fn test_composite_key_two_columns() {
    let batch = create_composite_batch(vec![1, 1, 2, 2], vec!["alice", "bob", "alice", "bob"]);
    let partitions = vec![vec![batch]];

    let index = HashIndexBuilder::new(vec!["id".to_string(), "name".to_string()])
        .build(&partitions)
        .expect("failed to build index");

    assert_eq!(index.len(), 4); // All combinations are unique
}

#[test]
fn test_composite_key_duplicate_detection() {
    let batch = create_composite_batch(
        vec![1, 2, 1], // First and third rows have same composite key
        vec!["alice", "bob", "alice"],
    );
    let partitions = vec![vec![batch]];

    let result = HashIndexBuilder::new(vec!["id".to_string(), "name".to_string()])
        .allow_duplicates(false)
        .build(&partitions);

    let _err = result.expect_err("expected duplicate key error");
}

#[test]
fn test_composite_key_three_columns() {
    // Test three-column compound primary key
    let schema = Arc::new(Schema::new(vec![
        Field::new("region", DataType::Utf8, false),
        Field::new("year", DataType::Int32, false),
        Field::new("product_id", DataType::Int64, false),
    ]));

    let region_array = StringArray::from(vec!["US", "US", "EU", "EU", "US"]);
    let year_array = Int32Array::from(vec![2024, 2024, 2024, 2024, 2025]);
    let product_id_array = Int64Array::from(vec![100, 101, 100, 101, 100]);

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(region_array),
            Arc::new(year_array),
            Arc::new(product_id_array),
        ],
    )
    .expect("failed to create batch");

    let partitions = vec![vec![batch]];

    let index = HashIndexBuilder::new(vec![
        "region".to_string(),
        "year".to_string(),
        "product_id".to_string(),
    ])
    .build(&partitions)
    .expect("failed to build index");

    // All 5 combinations should be unique
    assert_eq!(index.len(), 5);
}

#[test]
fn test_composite_key_three_columns_with_duplicates() {
    // Test duplicate detection with three-column compound key
    let schema = Arc::new(Schema::new(vec![
        Field::new("region", DataType::Utf8, false),
        Field::new("year", DataType::Int32, false),
        Field::new("product_id", DataType::Int64, false),
    ]));

    // Row 0 and Row 3 have the same compound key: ("US", 2024, 100)
    let region_array = StringArray::from(vec!["US", "US", "EU", "US"]);
    let year_array = Int32Array::from(vec![2024, 2024, 2024, 2024]);
    let product_id_array = Int64Array::from(vec![100, 101, 100, 100]); // Row 0 and 3 are same

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(region_array),
            Arc::new(year_array),
            Arc::new(product_id_array),
        ],
    )
    .expect("failed to create batch");

    let partitions = vec![vec![batch]];

    let result = HashIndexBuilder::new(vec![
        "region".to_string(),
        "year".to_string(),
        "product_id".to_string(),
    ])
    .allow_duplicates(false)
    .build(&partitions);

    result.expect_err("expected duplicate key error for compound key");
}

#[test]
fn test_composite_key_column_order_matters() {
    // Different column order should produce different hashes
    // Even with same values, (id=1, name="alice") should differ from (name="alice", id=1)
    // if column order in the key definition is different
    let batch = create_composite_batch(vec![1, 2, 3], vec!["alice", "bob", "carol"]);
    let partitions = vec![vec![batch]];

    // Build index with order: id, name
    let index1 = HashIndexBuilder::new(vec!["id".to_string(), "name".to_string()])
        .build(&partitions)
        .expect("failed to build index");

    // Build index with order: name, id
    let index2 = HashIndexBuilder::new(vec!["name".to_string(), "id".to_string()])
        .build(&partitions)
        .expect("failed to build index");

    // Both should have 3 entries
    assert_eq!(index1.len(), 3);
    assert_eq!(index2.len(), 3);

    // The key_columns should reflect the order
    assert_eq!(index1.key_columns(), &["id", "name"]);
    assert_eq!(index2.key_columns(), &["name", "id"]);
}

#[test]
fn test_composite_key_large_table() {
    // Test compound key with 10k rows to ensure proper scaling
    let n = 10_000;
    let ids: Vec<i64> = (0..n).collect();
    let names: Vec<String> = (0..n).map(|i| format!("user_{}", i % 100)).collect();
    let name_refs: Vec<&str> = names.iter().map(String::as_str).collect();

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    let id_array = Int64Array::from(ids);
    let name_array = StringArray::from(name_refs);
    let batch = RecordBatch::try_new(schema, vec![Arc::new(id_array), Arc::new(name_array)])
        .expect("failed to create batch");

    let partitions = vec![vec![batch]];

    let index = HashIndexBuilder::new(vec!["id".to_string(), "name".to_string()])
        .with_expected_rows(n as usize)
        .build(&partitions)
        .expect("failed to build index");

    // All combinations are unique (id is unique even though name repeats)
    assert_eq!(index.len(), n as usize);
}

#[test]
fn test_composite_key_partial_match_not_found() {
    // Ensure that matching only some columns doesn't find a row
    let batch = create_composite_batch(vec![1, 2, 3], vec!["alice", "bob", "carol"]);
    let partitions = vec![vec![batch]];

    let index = HashIndexBuilder::new(vec!["id".to_string(), "name".to_string()])
        .build(&partitions)
        .expect("failed to build index");

    // We can't directly look up partial keys with the current API,
    // but we can verify the index has distinct entries for each full compound key
    assert_eq!(index.len(), 3);
}

#[test]
fn test_composite_key_across_partitions() {
    // Test that compound keys work correctly across multiple partitions
    let batch1 = create_composite_batch(vec![1, 1], vec!["alice", "bob"]);
    let batch2 = create_composite_batch(vec![2, 2], vec!["alice", "bob"]);
    let batch3 = create_composite_batch(vec![1, 2], vec!["carol", "carol"]);

    let partitions = vec![vec![batch1], vec![batch2], vec![batch3]];

    let index = HashIndexBuilder::new(vec!["id".to_string(), "name".to_string()])
        .build(&partitions)
        .expect("failed to build index");

    // 2 + 2 + 2 = 6 unique compound keys
    assert_eq!(index.len(), 6);
}

#[test]
fn test_composite_key_rebuild_consistency() {
    // Verify that rebuilding the index produces consistent results
    let batch = create_composite_batch(vec![1, 2, 3, 4, 5], vec!["a", "b", "c", "d", "e"]);
    let partitions = vec![vec![batch]];

    let index = HashIndexBuilder::new(vec!["id".to_string(), "name".to_string()])
        .build(&partitions)
        .expect("failed to build index");

    assert_eq!(index.len(), 5);

    // Rebuild should maintain same count
    index.rebuild(&partitions).expect("failed to rebuild index");
    assert_eq!(index.len(), 5);
}

// =============================================================================
// Hash Collision Stress Test
// =============================================================================

#[test]
fn test_many_entries_same_h2() {
    // Create keys that might produce similar H2 values
    let index = HashIndex::new(vec!["id".to_string()]);

    // Insert many entries
    for i in 0..10_000_i64 {
        let hash = hash_key(&i);
        let loc = RowLocation::simple(0, i as u32);
        assert!(
            matches!(index.insert(hash, loc), InsertResult::Inserted),
            "Failed to insert {i}"
        );
    }

    // All should be findable
    for i in 0..10_000_i64 {
        assert!(index.get(&i).is_some(), "Failed to find {i}");
    }
}

// =============================================================================
// Batch Lookup Tests
// =============================================================================

#[test]
fn test_batch_lookup_empty() {
    let index = HashIndex::new(vec!["id".to_string()]);
    let results = index.get_batch(&[]);
    assert!(results.is_empty());
}

#[test]
fn test_batch_lookup_all_hits() {
    let ids: Vec<i64> = (0..100).collect();
    let batch = create_int64_batch(ids.clone());
    let partitions = vec![vec![batch]];

    let index = HashIndexBuilder::new(vec!["id".to_string()])
        .build(&partitions)
        .expect("failed to build index");

    #[expect(clippy::redundant_closure)]
    let lookup_hashes: Vec<u64> = ids.iter().map(|k| hash_key(k)).collect();
    let results = index.get_batch(&lookup_hashes);

    assert_eq!(results.len(), 100);
    for (i, result) in results.iter().enumerate() {
        assert_eq!(*result, Some(RowLocation::new(0, 0, i as u32)));
    }
}

#[test]
fn test_batch_lookup_all_misses() {
    let ids: Vec<i64> = (0..100).collect();
    let batch = create_int64_batch(ids);
    let partitions = vec![vec![batch]];

    let index = HashIndexBuilder::new(vec!["id".to_string()])
        .build(&partitions)
        .expect("failed to build index");

    let miss_hashes: Vec<u64> = (100..200_i64).map(|k| hash_key(&k)).collect();
    let results = index.get_batch(&miss_hashes);

    assert_eq!(results.len(), 100);
    for result in results {
        assert!(result.is_none());
    }
}

#[test]
fn test_batch_lookup_mixed() {
    let ids: Vec<i64> = (0..100).collect();
    let batch = create_int64_batch(ids);
    let partitions = vec![vec![batch]];

    let index = HashIndexBuilder::new(vec!["id".to_string()])
        .build(&partitions)
        .expect("failed to build index");

    // Mix of hits and misses
    let lookup_hashes: Vec<u64> = vec![
        hash_key(&0_i64),
        hash_key(&999_i64), // miss
        hash_key(&50_i64),
        hash_key(&(-1_i64)), // miss
        hash_key(&99_i64),
    ];
    let results = index.get_batch(&lookup_hashes);

    assert!(results[0].is_some());
    assert!(results[1].is_none());
    assert!(results[2].is_some());
    assert!(results[3].is_none());
    assert!(results[4].is_some());
}

// =============================================================================
// Rebuild Tests
// =============================================================================

#[test]
fn test_rebuild_index() {
    let batch1 = create_int64_batch(vec![1, 2, 3]);
    let partitions1 = vec![vec![batch1]];

    let index = HashIndexBuilder::new(vec!["id".to_string()])
        .build(&partitions1)
        .expect("failed to build index");

    assert_eq!(index.len(), 3);

    // Rebuild with new data
    let batch2 = create_int64_batch(vec![10, 20, 30, 40]);
    let partitions2 = vec![vec![batch2]];

    index.rebuild(&partitions2).expect("failed to rebuild");

    assert_eq!(index.len(), 4);

    // Old data should be gone
    assert!(index.get(&1_i64).is_none());
    assert!(index.get(&2_i64).is_none());

    // New data should be present
    assert!(index.get(&10_i64).is_some());
    assert!(index.get(&40_i64).is_some());
}

// =============================================================================
// Data Correctness Verification
// =============================================================================

#[test]
fn test_location_correctness() {
    // Create a complex multi-partition, multi-batch structure
    let batch_p0_b0 = create_int64_batch(vec![1, 2]);
    let batch_p0_b1 = create_int64_batch(vec![3, 4, 5]);
    let batch_p1_b0 = create_int64_batch(vec![6]);
    let batch_p1_b1 = create_int64_batch(vec![7, 8]);
    let batch_p2_b0 = create_int64_batch(vec![9, 10, 11, 12]);

    let partitions = vec![
        vec![batch_p0_b0, batch_p0_b1],
        vec![batch_p1_b0, batch_p1_b1],
        vec![batch_p2_b0],
    ];

    let index = HashIndexBuilder::new(vec!["id".to_string()])
        .build(&partitions)
        .expect("failed to build index");

    // Verify exact locations
    assert_eq!(index.get(&1_i64), Some(RowLocation::new(0, 0, 0)));
    assert_eq!(index.get(&2_i64), Some(RowLocation::new(0, 0, 1)));
    assert_eq!(index.get(&3_i64), Some(RowLocation::new(0, 1, 0)));
    assert_eq!(index.get(&4_i64), Some(RowLocation::new(0, 1, 1)));
    assert_eq!(index.get(&5_i64), Some(RowLocation::new(0, 1, 2)));
    assert_eq!(index.get(&6_i64), Some(RowLocation::new(1, 0, 0)));
    assert_eq!(index.get(&7_i64), Some(RowLocation::new(1, 1, 0)));
    assert_eq!(index.get(&8_i64), Some(RowLocation::new(1, 1, 1)));
    assert_eq!(index.get(&9_i64), Some(RowLocation::new(2, 0, 0)));
    assert_eq!(index.get(&10_i64), Some(RowLocation::new(2, 0, 1)));
    assert_eq!(index.get(&11_i64), Some(RowLocation::new(2, 0, 2)));
    assert_eq!(index.get(&12_i64), Some(RowLocation::new(2, 0, 3)));
}

#[test]
fn test_all_keys_unique_in_index() {
    let ids: Vec<i64> = (0..5000).collect();
    let batch = create_int64_batch(ids.clone());
    let partitions = vec![vec![batch]];

    let index = HashIndexBuilder::new(vec!["id".to_string()])
        .build(&partitions)
        .expect("failed to build index");

    // Collect all locations
    let mut locations = HashSet::new();
    for id in &ids {
        if let Some(loc) = index.get(id) {
            let key = (loc.partition, loc.batch, loc.row);
            assert!(
                locations.insert(key),
                "Duplicate location for key {id}: {loc:?}"
            );
        }
    }

    assert_eq!(locations.len(), 5000);
}

// =============================================================================
// Growth and Capacity Tests
// =============================================================================

#[test]
fn test_grow_multiple_times() {
    let index = HashIndex::new(vec!["id".to_string()]);

    // Insert enough to trigger multiple grows
    // Starting capacity is GROUP_SIZE (16), grows at 87.5% load
    for i in 0..10_000_i64 {
        let hash = hash_key(&i);
        let loc = RowLocation::simple(0, i as u32);
        index.insert(hash, loc);
    }

    assert_eq!(index.len(), 10_000);

    // Verify all entries survived growth
    for i in 0..10_000_i64 {
        assert!(index.get(&i).is_some(), "Missing key {i} after growth");
    }
}

#[test]
fn test_high_load_factor() {
    let index = HashIndex::new(vec!["id".to_string()]);

    // Insert many entries
    for i in 0..1000_i64 {
        let hash = hash_key(&i);
        let loc = RowLocation::simple(0, i as u32);
        index.insert(hash, loc);
    }

    // Delete half to create tombstones
    for i in (0..1000_i64).step_by(2) {
        let hash = hash_key(&i);
        index.remove(hash);
    }

    // Insert new entries that might land on tombstones
    for i in 1000..1500_i64 {
        let hash = hash_key(&i);
        let loc = RowLocation::simple(1, (i - 1000) as u32);
        index.insert(hash, loc);
    }

    // Verify all expected entries
    for i in (1..1000_i64).step_by(2) {
        assert!(index.get(&i).is_some(), "Missing odd key {i}");
    }
    for i in 1000..1500_i64 {
        assert!(index.get(&i).is_some(), "Missing new key {i}");
    }
}

// =============================================================================
// Float Key Tests (via RowConverter)
// =============================================================================

#[test]
fn test_float_keys_via_rowconverter() {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "id",
        DataType::Float64,
        false,
    )]));
    let array = Float64Array::from(vec![1.0, 2.5, 3.75, 0.0, -1.0]);
    let batch =
        RecordBatch::try_new(schema, vec![Arc::new(array)]).expect("failed to create batch");

    let partitions = vec![vec![batch]];

    // Float keys use RowConverter under the hood
    let index = HashIndexBuilder::new(vec!["id".to_string()])
        .build(&partitions)
        .expect("failed to build index");

    assert_eq!(index.len(), 5);
}

// =============================================================================
// Data Integrity and Correctness Tests
// =============================================================================

/// Test that all inserted keys can be retrieved with correct locations.
#[test]
fn test_data_integrity_all_keys_retrievable() {
    let ids: Vec<i64> = (0..10_000).collect();
    let batch = create_int64_batch(ids.clone());
    let partitions = vec![vec![batch]];

    let index = HashIndexBuilder::new(vec!["id".to_string()])
        .build(&partitions)
        .expect("failed to build");

    // Every key must be retrievable
    for (row_idx, id) in ids.iter().enumerate() {
        let loc = index
            .get(id)
            .unwrap_or_else(|| panic!("Key {id} not found in index"));
        assert_eq!(loc.partition, 0, "Wrong partition for key {id}");
        assert_eq!(loc.batch, 0, "Wrong batch for key {id}");
        assert_eq!(
            loc.row, row_idx as u32,
            "Wrong row for key {id}: expected {row_idx}, got {}",
            loc.row
        );
    }
}

/// Test that non-existent keys return None.
#[test]
fn test_data_integrity_non_existent_keys() {
    let ids: Vec<i64> = (0..1000).collect();
    let batch = create_int64_batch(ids);
    let partitions = vec![vec![batch]];

    let index = HashIndexBuilder::new(vec!["id".to_string()])
        .build(&partitions)
        .expect("failed to build");

    // Keys outside the range should not be found
    for id in 1000..2000_i64 {
        assert!(index.get(&id).is_none(), "Non-existent key {id} was found");
    }

    // Negative keys should not be found
    for id in -100..0_i64 {
        assert!(
            index.get(&id).is_none(),
            "Non-existent negative key {id} was found"
        );
    }
}

/// Test that batch lookup returns same results as individual lookups.
#[test]
fn test_data_integrity_batch_vs_individual_lookup() {
    let ids: Vec<i64> = (0..5000).collect();
    let batch = create_int64_batch(ids);
    let partitions = vec![vec![batch]];

    let index = HashIndexBuilder::new(vec!["id".to_string()])
        .build(&partitions)
        .expect("failed to build");

    // Mix of existing and non-existing keys
    let lookup_keys: Vec<i64> = (0..6000).step_by(2).collect();
    let hashes: Vec<u64> = lookup_keys.iter().map(hash_key).collect();

    let batch_results = index.get_batch(&hashes);

    for (i, key) in lookup_keys.iter().enumerate() {
        let individual = index.get(key);
        let batched = batch_results[i];
        assert_eq!(
            individual, batched,
            "Mismatch for key {key}: individual={individual:?}, batch={batched:?}"
        );
    }
}

/// Test that `insert_or_replace` correctly updates locations.
#[test]
fn test_data_integrity_insert_or_replace() {
    let index = HashIndex::new(vec!["id".to_string()]);

    // Insert initial entries
    for i in 0..100_i64 {
        let hash = hash_key(&i);
        let loc = RowLocation::new(0, 0, i as u32);
        index.insert(hash, loc);
    }

    // Replace with new locations
    for i in 0..100_i64 {
        let hash = hash_key(&i);
        let new_loc = RowLocation::new(1, 1, (i + 1000) as u32);
        index.insert_or_replace(hash, new_loc);
    }

    // Verify updated locations
    for i in 0..100_i64 {
        let loc = index.get(&i).expect("Key should exist");
        assert_eq!(loc.partition, 1, "Partition not updated for key {i}");
        assert_eq!(loc.batch, 1, "Batch not updated for key {i}");
        assert_eq!(loc.row, (i + 1000) as u32, "Row not updated for key {i}");
    }

    // Length should stay the same (no new entries)
    assert_eq!(index.len(), 100);
}

/// Test that remove actually removes entries.
#[test]
fn test_data_integrity_remove_correctness() {
    let index = HashIndex::new(vec!["id".to_string()]);

    // Insert entries
    for i in 0..500_i64 {
        let hash = hash_key(&i);
        let loc = RowLocation::simple(0, i as u32);
        index.insert(hash, loc);
    }

    assert_eq!(index.len(), 500);

    // Remove even entries
    for i in (0..500_i64).step_by(2) {
        let hash = hash_key(&i);
        let removed = index.remove(hash);
        assert!(
            removed.is_some(),
            "Remove should return the old location for key {i}"
        );
    }

    assert_eq!(index.len(), 250);

    // Even entries should be gone
    for i in (0..500_i64).step_by(2) {
        assert!(index.get(&i).is_none(), "Even key {i} should be removed");
    }

    // Odd entries should remain
    for i in (1..500_i64).step_by(2) {
        assert!(index.get(&i).is_some(), "Odd key {i} should still exist");
    }
}

/// Test remove followed by re-insert of the same key.
#[test]
fn test_data_integrity_remove_then_reinsert() {
    let index = HashIndex::new(vec!["id".to_string()]);

    let key = 42_i64;
    let hash = hash_key(&key);

    // Insert
    let loc1 = RowLocation::new(0, 0, 0);
    assert!(matches!(index.insert(hash, loc1), InsertResult::Inserted));
    assert_eq!(index.get(&key), Some(loc1));

    // Remove
    assert_eq!(index.remove(hash), Some(loc1));
    assert!(index.get(&key).is_none());
    assert_eq!(index.len(), 0);

    // Re-insert with different location
    let loc2 = RowLocation::new(1, 1, 100);
    assert!(matches!(index.insert(hash, loc2), InsertResult::Inserted));
    assert_eq!(index.get(&key), Some(loc2));
    assert_eq!(index.len(), 1);
}

/// Test that clear removes all entries.
#[test]
fn test_data_integrity_clear() {
    let ids: Vec<i64> = (0..1000).collect();
    let batch = create_int64_batch(ids.clone());
    let partitions = vec![vec![batch]];

    let index = HashIndexBuilder::new(vec!["id".to_string()])
        .build(&partitions)
        .expect("failed to build");

    assert_eq!(index.len(), 1000);

    index.clear();

    assert_eq!(index.len(), 0);
    assert!(index.is_empty());

    // All keys should be gone
    for id in ids {
        assert!(index.get(&id).is_none(), "Key {id} should be cleared");
    }
}

/// Test rebuild replaces all data correctly.
#[test]
fn test_data_integrity_rebuild() {
    // Build initial index
    let ids1: Vec<i64> = (0..500).collect();
    let batch1 = create_int64_batch(ids1.clone());
    let partitions1 = vec![vec![batch1]];

    let index = HashIndexBuilder::new(vec!["id".to_string()])
        .build(&partitions1)
        .expect("failed to build");

    assert_eq!(index.len(), 500);

    // Rebuild with different data
    let ids2: Vec<i64> = (500..1500).collect();
    let batch2 = create_int64_batch(ids2.clone());
    let partitions2 = vec![vec![batch2]];

    index.rebuild(&partitions2).expect("rebuild failed");

    assert_eq!(index.len(), 1000);

    // Old keys should be gone
    for id in ids1 {
        assert!(index.get(&id).is_none(), "Old key {id} should be replaced");
    }

    // New keys should exist
    for (row_idx, id) in ids2.iter().enumerate() {
        let loc = index.get(id).expect("New key should exist");
        assert_eq!(loc.row, row_idx as u32, "Wrong row for rebuilt key {id}");
    }
}

/// Test `add_batches` appends correctly without affecting existing data.
#[test]
fn test_data_integrity_add_batches() {
    let index = HashIndex::new(vec!["id".to_string()]);

    // Add initial batch
    let ids1: Vec<i64> = (0..100).collect();
    let batch1 = create_int64_batch(ids1.clone());
    index
        .add_batches(0, 0, &[batch1])
        .expect("add_batches failed");

    assert_eq!(index.len(), 100);

    // Add more batches
    let ids2: Vec<i64> = (100..200).collect();
    let batch2 = create_int64_batch(ids2.clone());
    index
        .add_batches(0, 1, &[batch2])
        .expect("add_batches failed");

    assert_eq!(index.len(), 200);

    // Verify first batch locations
    for (row_idx, id) in ids1.iter().enumerate() {
        let loc = index.get(id).expect("Key from first batch should exist");
        assert_eq!(loc.batch, 0, "First batch should have batch=0");
        assert_eq!(loc.row, row_idx as u32);
    }

    // Verify second batch locations
    for (row_idx, id) in ids2.iter().enumerate() {
        let loc = index.get(id).expect("Key from second batch should exist");
        assert_eq!(loc.batch, 1, "Second batch should have batch=1");
        assert_eq!(loc.row, row_idx as u32);
    }
}

/// Test bloom filter correctly identifies definite negatives.
#[test]
fn test_data_integrity_bloom_filter() {
    let ids: Vec<i64> = (0..10_000).collect();
    let batch = create_int64_batch(ids.clone());
    let partitions = vec![vec![batch]];

    let index = HashIndexBuilder::new(vec!["id".to_string()])
        .with_bloom_filter(true)
        .build(&partitions)
        .expect("failed to build");

    assert!(index.has_bloom_filter());

    // All existing keys should pass bloom filter
    for id in &ids {
        let hash = hash_key(id);
        assert!(
            index.might_contain(hash),
            "Bloom filter false negative for {id}"
        );
    }

    // For non-existent keys, bloom filter might say true (false positive) but get() must return None
    let mut false_positives = 0;
    for id in 10_000..20_000_i64 {
        let hash = hash_key(&id);
        if index.might_contain(hash) {
            false_positives += 1;
        }
        // But actual lookup must return None
        assert!(index.get(&id).is_none(), "Non-existent key {id} was found");
    }

    // False positive rate should be reasonable (<5%)
    let fp_rate = false_positives as f64 / 10_000.0;
    assert!(
        fp_rate < 0.05,
        "Bloom filter false positive rate too high: {fp_rate:.2}%"
    );
}

/// Test that multi-partition index correctly tracks partition indices.
#[test]
fn test_data_integrity_multi_partition() {
    let ids1: Vec<i64> = (0..100).collect();
    let ids2: Vec<i64> = (100..200).collect();
    let ids3: Vec<i64> = (200..300).collect();

    let batch1 = create_int64_batch(ids1.clone());
    let batch2 = create_int64_batch(ids2.clone());
    let batch3 = create_int64_batch(ids3.clone());

    let partitions = vec![vec![batch1], vec![batch2], vec![batch3]];

    let index = HashIndexBuilder::new(vec!["id".to_string()])
        .build(&partitions)
        .expect("failed to build");

    // Verify partition indices
    for id in &ids1 {
        let loc = index.get(id).expect("Key should exist");
        assert_eq!(loc.partition, 0, "Wrong partition for key {id}");
    }

    for id in &ids2 {
        let loc = index.get(id).expect("Key should exist");
        assert_eq!(loc.partition, 1, "Wrong partition for key {id}");
    }

    for id in &ids3 {
        let loc = index.get(id).expect("Key should exist");
        assert_eq!(loc.partition, 2, "Wrong partition for key {id}");
    }
}

/// Test string key data integrity.
#[test]
fn test_data_integrity_string_keys() {
    let ids: Vec<&str> = (0..1000)
        .map(|i| {
            // Use static strings to avoid lifetime issues
            Box::leak(format!("key_{i:06}").into_boxed_str()) as &str
        })
        .collect();
    let batch = create_string_batch(ids.clone());
    let partitions = vec![vec![batch]];

    let index = HashIndexBuilder::new(vec!["id".to_string()])
        .build(&partitions)
        .expect("failed to build");

    // Verify all string keys
    for (row_idx, id) in ids.iter().enumerate() {
        let loc = index
            .get(&(*id).to_string())
            .unwrap_or_else(|| panic!("String key '{id}' not found"));
        assert_eq!(loc.row, row_idx as u32, "Wrong row for string key '{id}'");
    }
}

/// Test hash collision handling (synthetic).
#[test]
fn test_data_integrity_hash_collision_simulation() {
    // We can't force real hash collisions, but we can test that
    // different keys with similar hash patterns work correctly
    let index = HashIndex::new(vec!["id".to_string()]);

    // Insert many keys that will land in similar shards
    for i in 0..10_000_i64 {
        let hash = hash_key(&i);
        let loc = RowLocation::simple(0, i as u32);
        assert!(
            matches!(index.insert(hash, loc), InsertResult::Inserted),
            "Failed to insert key {i}"
        );
    }

    // All should be retrievable
    for i in 0..10_000_i64 {
        let loc = index
            .get(&i)
            .unwrap_or_else(|| panic!("Key {i} not found after bulk insert"));
        assert_eq!(loc.row, i as u32);
    }
}

/// Test concurrent insert and lookup consistency.
#[test]
fn test_data_integrity_concurrent_consistency() {
    let index = Arc::new(HashIndex::new(vec!["id".to_string()]));
    let num_threads = 8;
    let keys_per_thread = 1000;

    // Spawn writer threads
    let writers: Vec<_> = (0..num_threads)
        .map(|t| {
            let idx = Arc::clone(&index);
            thread::spawn(move || {
                let start = t * keys_per_thread;
                for i in start..(start + keys_per_thread) {
                    let hash = hash_key(&(i as i64));
                    let loc = RowLocation::simple(t as u32, (i - start) as u32);
                    idx.insert(hash, loc);
                }
            })
        })
        .collect();

    for w in writers {
        w.join().expect("writer panicked");
    }

    // Verify all keys are present with correct thread assignment
    for t in 0..num_threads {
        let start = t * keys_per_thread;
        for i in start..(start + keys_per_thread) {
            let loc = index
                .get(&(i as i64))
                .unwrap_or_else(|| panic!("Key {i} not found"));
            assert_eq!(
                loc.batch, t as u32,
                "Key {i} has wrong batch (thread assignment)"
            );
        }
    }

    assert_eq!(index.len(), num_threads * keys_per_thread);
}

/// Test that duplicate key detection works.
#[test]
fn test_data_integrity_duplicate_detection() {
    let ids: Vec<i64> = vec![1, 2, 3, 2, 5]; // Note: 2 is duplicated
    let batch = create_int64_batch(ids);
    let partitions = vec![vec![batch]];

    let result = HashIndexBuilder::new(vec!["id".to_string()])
        .allow_duplicates(false)
        .build(&partitions);

    let _err = result.expect_err("Should detect duplicate key");
}

/// Test that `allow_duplicates` mode uses last-write-wins.
#[test]
fn test_data_integrity_allow_duplicates_last_wins() {
    let ids: Vec<i64> = vec![1, 2, 1, 3, 2]; // 1 appears at rows 0,2; 2 appears at rows 1,4
    let batch = create_int64_batch(ids);
    let partitions = vec![vec![batch]];

    let index = HashIndexBuilder::new(vec!["id".to_string()])
        .allow_duplicates(true)
        .build(&partitions)
        .expect("should build with duplicates allowed");

    // Last occurrence wins
    let loc1 = index.get(&1_i64).expect("Key 1 should exist");
    assert_eq!(loc1.row, 2, "Key 1 should point to last occurrence (row 2)");

    let loc2 = index.get(&2_i64).expect("Key 2 should exist");
    assert_eq!(loc2.row, 4, "Key 2 should point to last occurrence (row 4)");

    let loc3 = index.get(&3_i64).expect("Key 3 should exist");
    assert_eq!(loc3.row, 3, "Key 3 should be at row 3");

    // Total unique keys
    assert_eq!(index.len(), 3);
}

/// Test that hash index correctly handles hash value 0.
/// Hash 0 should be stored and retrieved correctly without any collision issues.
#[test]
fn test_zero_hash_works_correctly() {
    let index = HashIndex::new(vec!["id".to_string()]);

    // Insert using hash = 0
    let location = RowLocation::new(0, 0, 42);
    let inserted = index.insert(0, location);
    assert_eq!(
        inserted,
        InsertResult::Inserted,
        "Insert with hash=0 should succeed"
    );

    // get_by_hash(0) should return the location
    let result = index.get_by_hash(0);
    assert!(result.is_some(), "Keys with hash=0 should be retrievable");
    assert_eq!(result, Some(location));

    // Verify the index length is correct
    assert_eq!(index.len(), 1);
}

/// Test that hash 0 and hash 1 do NOT collide (they are distinct keys).
#[test]
fn test_zero_and_one_hash_are_distinct() {
    let index = HashIndex::new(vec!["id".to_string()]);

    // Insert with hash = 0
    let loc1 = RowLocation::new(0, 0, 1);
    assert_eq!(
        index.insert(0, loc1),
        InsertResult::Inserted,
        "Insert with hash=0 should succeed"
    );

    // Insert with hash = 1 - this should succeed as a distinct key
    let loc2 = RowLocation::new(0, 0, 2);
    let inserted = index.insert(1, loc2);

    // Hash 0 and hash 1 are distinct keys, so both should be stored
    assert_eq!(
        inserted,
        InsertResult::Inserted,
        "Insert with hash=1 should succeed because 0 and 1 are distinct keys"
    );

    // Verify both entries can be retrieved independently
    let result_0 = index.get_by_hash(0);
    let result_1 = index.get_by_hash(1);
    assert_eq!(result_0, Some(loc1), "Hash 0 should return loc1");
    assert_eq!(result_1, Some(loc2), "Hash 1 should return loc2");
    assert_ne!(
        result_0, result_1,
        "Hash 0 and 1 should return different results"
    );

    // Verify index contains both entries
    assert_eq!(index.len(), 2);
}

/// Test that hash index correctly handles the sentinel value (u64::MAX).
/// Keys that hash to u64::MAX are normalized to u64::MAX - 1 to avoid collision
/// with the empty slot marker.
#[test]
fn test_max_hash_normalized() {
    let index = HashIndex::new(vec!["id".to_string()]);

    // Insert using hash = u64::MAX (the sentinel value)
    let location = RowLocation::new(0, 0, 42);
    let inserted = index.insert(u64::MAX, location);
    assert_eq!(
        inserted,
        InsertResult::Inserted,
        "Insert with hash=u64::MAX should succeed"
    );

    // get_by_hash(u64::MAX) should return the location
    let result = index.get_by_hash(u64::MAX);
    assert!(
        result.is_some(),
        "Keys with hash=u64::MAX should be retrievable after normalization"
    );
    assert_eq!(result, Some(location));

    // Verify the index length is correct
    assert_eq!(index.len(), 1);

    // u64::MAX and u64::MAX - 1 will collide since u64::MAX is normalized to u64::MAX - 1
    let loc2 = RowLocation::new(0, 0, 99);
    let inserted2 = index.insert(u64::MAX - 1, loc2);
    assert!(
        matches!(inserted2, InsertResult::HashCollision(_)),
        "Insert with hash=u64::MAX-1 should fail because u64::MAX normalizes to u64::MAX-1"
    );
}

// =============================================================================
// Error Handling Tests
// =============================================================================

#[test]
fn test_error_key_column_not_found() {
    let batch = create_int64_batch(vec![1, 2, 3]);
    let partitions = vec![vec![batch]];

    let result = HashIndexBuilder::new(vec!["nonexistent_column".to_string()]).build(&partitions);

    let err = result.expect_err("should fail with missing column");
    let err_msg = format!("{err}");
    assert!(
        err_msg.contains("not found"),
        "Error should mention column not found: {err_msg}"
    );
}

#[test]
fn test_error_empty_key_columns() {
    let batch = create_int64_batch(vec![1, 2, 3]);
    let partitions = vec![vec![batch]];

    // Empty key columns list should error
    let result = HashIndexBuilder::new(vec![]).build(&partitions);

    let err = result.expect_err("should fail with empty key columns");
    let err_msg = format!("{err}");
    assert!(
        err_msg.contains("not found") || err_msg.contains("no columns"),
        "Error should mention missing columns: {err_msg}"
    );
}

#[test]
fn test_error_unsupported_key_type_timestamp() {
    use arrow::array::TimestampNanosecondArray;

    // Timestamp types fall back to RowConverter which should work
    let schema = Arc::new(Schema::new(vec![Field::new(
        "ts",
        DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None),
        false,
    )]));
    let array = TimestampNanosecondArray::from(vec![1_000_000_000, 2_000_000_000]);
    let batch =
        RecordBatch::try_new(schema, vec![Arc::new(array)]).expect("failed to create batch");

    let partitions = vec![vec![batch]];

    // Timestamp should work via RowConverter fallback
    let result = HashIndexBuilder::new(vec!["ts".to_string()]).build(&partitions);
    assert!(
        result.is_ok(),
        "Timestamp should work via RowConverter: {:?}",
        result.err()
    );
}

// =============================================================================
// Builder Configuration Tests
// =============================================================================

#[test]
fn test_try_build_below_threshold_returns_none() {
    let batch = create_int64_batch(vec![1, 2, 3, 4, 5]);
    let partitions = vec![vec![batch]];

    // Set threshold higher than row count
    let result = HashIndexBuilder::new(vec!["id".to_string()])
        .with_min_rows_threshold(100)
        .try_build(&partitions)
        .expect("try_build should succeed");

    assert!(
        result.is_none(),
        "Should return None when below threshold (5 rows < 100 threshold)"
    );
}

#[test]
fn test_try_build_above_threshold_returns_some() {
    let ids: Vec<i64> = (0..1000).collect();
    let batch = create_int64_batch(ids);
    let partitions = vec![vec![batch]];

    let result = HashIndexBuilder::new(vec!["id".to_string()])
        .with_min_rows_threshold(100)
        .try_build(&partitions)
        .expect("try_build should succeed");

    assert!(
        result.is_some(),
        "Should return Some when above threshold (1000 rows > 100 threshold)"
    );
    assert_eq!(result.as_ref().map(HashIndex::len), Some(1000));
}

#[test]
fn test_builder_with_bloom_filter_disabled() {
    let batch = create_int64_batch(vec![1, 2, 3]);
    let partitions = vec![vec![batch]];

    let index = HashIndexBuilder::new(vec!["id".to_string()])
        .with_bloom_filter(false)
        .build(&partitions)
        .expect("failed to build index");

    assert!(!index.has_bloom_filter());
    // Lookups should still work
    assert!(index.get(&1_i64).is_some());
}

#[test]
fn test_builder_with_expected_rows() {
    let ids: Vec<i64> = (0..100).collect();
    let batch = create_int64_batch(ids);
    let partitions = vec![vec![batch]];

    // Pre-size for 1000 rows
    let index = HashIndexBuilder::new(vec!["id".to_string()])
        .with_expected_rows(1000)
        .build(&partitions)
        .expect("failed to build index");

    assert_eq!(index.len(), 100);
    // Verify lookups work
    assert!(index.get(&50_i64).is_some());
}

// =============================================================================
// Clone and Contains Tests
// =============================================================================

#[test]
fn test_hash_index_clone() {
    let ids: Vec<i64> = (0..100).collect();
    let batch = create_int64_batch(ids.clone());
    let partitions = vec![vec![batch]];

    let original = HashIndexBuilder::new(vec!["id".to_string()])
        .build(&partitions)
        .expect("failed to build index");

    let cloned = original.clone();

    // Both should have same content
    assert_eq!(original.len(), cloned.len());
    for id in &ids {
        assert_eq!(original.get(id), cloned.get(id));
    }

    // Modifying clone should not affect original
    let new_hash = hash_key(&999_i64);
    cloned.insert(new_hash, RowLocation::simple(0, 999));

    assert_eq!(cloned.len(), 101);
    assert_eq!(original.len(), 100);
}

#[test]
fn test_contains_method() {
    let batch = create_int64_batch(vec![1, 2, 3]);
    let partitions = vec![vec![batch]];

    let index = HashIndexBuilder::new(vec!["id".to_string()])
        .build(&partitions)
        .expect("failed to build index");

    assert!(index.contains(hash_key(&1_i64)));
    assert!(index.contains(hash_key(&2_i64)));
    assert!(index.contains(hash_key(&3_i64)));
    assert!(!index.contains(hash_key(&999_i64)));
}

// =============================================================================
// Binary Key Tests
// =============================================================================

#[test]
fn test_binary_key_extraction() {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Binary, false)]));
    let array = BinaryArray::from(vec![
        b"key1".as_slice(),
        b"key2".as_slice(),
        b"\x00\x01\x02".as_slice(),
    ]);
    let batch =
        RecordBatch::try_new(schema, vec![Arc::new(array)]).expect("failed to create batch");

    let partitions = vec![vec![batch]];
    let index = HashIndexBuilder::new(vec!["id".to_string()])
        .build(&partitions)
        .expect("failed to build index");

    assert_eq!(index.len(), 3);
    // Verify all keys are indexed
    assert!(index.get(&b"key1".as_slice()).is_some());
    assert!(index.get(&b"key2".as_slice()).is_some());
    assert!(index.get(&b"\x00\x01\x02".as_slice()).is_some());
}

#[test]
fn test_binary_key_with_nulls() {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Binary, true)]));
    let array = BinaryArray::from(vec![
        Some(b"key1".as_slice()),
        None,
        Some(b"key3".as_slice()),
    ]);
    let batch =
        RecordBatch::try_new(schema, vec![Arc::new(array)]).expect("failed to create batch");

    let partitions = vec![vec![batch]];
    let index = HashIndexBuilder::new(vec!["id".to_string()])
        .build(&partitions)
        .expect("failed to build index");

    // Null row should be excluded
    assert_eq!(index.len(), 2);
}

#[test]
fn test_large_binary_array() {
    use arrow::array::LargeBinaryArray;

    let schema = Arc::new(Schema::new(vec![Field::new(
        "id",
        DataType::LargeBinary,
        false,
    )]));
    let array = LargeBinaryArray::from(vec![b"largekey1".as_slice(), b"largekey2".as_slice()]);
    let batch =
        RecordBatch::try_new(schema, vec![Arc::new(array)]).expect("failed to create batch");

    let partitions = vec![vec![batch]];
    let index = HashIndexBuilder::new(vec!["id".to_string()])
        .build(&partitions)
        .expect("failed to build index");

    assert_eq!(index.len(), 2);
}

#[test]
fn test_large_string_array() {
    use arrow::array::LargeStringArray;

    let schema = Arc::new(Schema::new(vec![Field::new(
        "id",
        DataType::LargeUtf8,
        false,
    )]));
    let array = LargeStringArray::from(vec!["large_str_1", "large_str_2", "large_str_3"]);
    let batch =
        RecordBatch::try_new(schema, vec![Arc::new(array)]).expect("failed to create batch");

    let partitions = vec![vec![batch]];
    let index = HashIndexBuilder::new(vec!["id".to_string()])
        .build(&partitions)
        .expect("failed to build index");

    assert_eq!(index.len(), 3);
}

// =============================================================================
// Composite Key Advanced Tests
// =============================================================================

#[test]
fn test_composite_key_with_all_nulls_row_skipped() {
    // Test that a row with null values in ANY composite key column is skipped.
    // This is the correct behavior, matching single-column null handling.
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("name", DataType::Utf8, true),
    ]));

    let id_array = Int64Array::from(vec![Some(1), None, Some(3)]);
    let name_array = StringArray::from(vec![Some("alice"), None, Some("carol")]);

    let batch = RecordBatch::try_new(schema, vec![Arc::new(id_array), Arc::new(name_array)])
        .expect("failed to create batch");

    let partitions = vec![vec![batch]];

    let index = HashIndexBuilder::new(vec!["id".to_string(), "name".to_string()])
        .build(&partitions)
        .expect("failed to build index");

    // Row 1 (index 1) has null in both columns, so it should be skipped.
    // Only rows 0 and 2 should be indexed.
    assert_eq!(index.len(), 2);
}

#[test]
fn test_composite_key_four_columns() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("region", DataType::Utf8, false),
        Field::new("year", DataType::Int32, false),
        Field::new("month", DataType::Int32, false),
        Field::new("product_id", DataType::Int64, false),
    ]));

    let region_array = StringArray::from(vec!["US", "US", "EU"]);
    let year_array = Int32Array::from(vec![2024, 2024, 2024]);
    let month_array = Int32Array::from(vec![1, 2, 1]);
    let product_id_array = Int64Array::from(vec![100, 100, 100]);

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(region_array),
            Arc::new(year_array),
            Arc::new(month_array),
            Arc::new(product_id_array),
        ],
    )
    .expect("failed to create batch");

    let partitions = vec![vec![batch]];

    let index = HashIndexBuilder::new(vec![
        "region".to_string(),
        "year".to_string(),
        "month".to_string(),
        "product_id".to_string(),
    ])
    .build(&partitions)
    .expect("failed to build index");

    assert_eq!(index.len(), 3);
}

// =============================================================================
// Bloom Filter Edge Cases
// =============================================================================

#[test]
fn test_bloom_filter_memory_usage_bytes() {
    use crate::BloomFilter;

    let bloom = BloomFilter::new(1000);
    // memory_usage and memory_usage_bytes should return the same value
    assert_eq!(bloom.memory_usage(), bloom.memory_usage_bytes());
}

#[test]
fn test_bloom_filter_with_zero_items() {
    use crate::BloomFilter;

    // A bloom filter sized for 0 items should have minimum capacity
    let bloom = BloomFilter::new(0);
    assert!(!bloom.is_empty(), "Should have minimum capacity, not empty");
    assert!(bloom.num_bits() >= 64, "Should have at least 64 bits");
}

#[test]
fn test_empty_bloom_filter_always_positive() {
    use crate::BloomFilter;

    let bloom = BloomFilter::empty();
    assert!(bloom.is_empty());
    // Empty bloom filter always returns true (must fall through to hash table)
    assert!(bloom.might_contain(0));
    assert!(bloom.might_contain(12345));
    assert!(bloom.might_contain(u64::MAX));
}

// =============================================================================
// HashIndex Constructor Variants
// =============================================================================

#[test]
fn test_hash_index_with_capacity() {
    let index = HashIndex::with_capacity(vec!["id".to_string()], 10000);

    assert!(index.is_empty());
    assert_eq!(index.len(), 0);

    // Should be able to insert efficiently
    for i in 0..1000_i64 {
        let hash = hash_key(&i);
        index.insert(hash, RowLocation::simple(0, i as u32));
    }

    assert_eq!(index.len(), 1000);
}

#[test]
fn test_hash_index_with_bloom_filter() {
    let index = HashIndex::with_bloom_filter(vec!["id".to_string()], 1000);

    assert!(index.has_bloom_filter());
    assert!(index.is_empty());

    // Insert some entries
    for i in 0..100_i64 {
        let hash = hash_key(&i);
        index.insert(hash, RowLocation::simple(0, i as u32));
    }

    // Bloom filter should work
    assert!(index.might_contain(hash_key(&50_i64)));
}

#[test]
fn test_hash_index_builder_method() {
    let builder = HashIndex::builder(vec!["id".to_string()]);
    let batch = create_int64_batch(vec![1, 2, 3]);
    let partitions = vec![vec![batch]];

    let index = builder.build(&partitions).expect("failed to build");
    assert_eq!(index.len(), 3);
}

// =============================================================================
// Debug and Display Tests
// =============================================================================

#[test]
fn test_hash_index_debug() {
    let batch = create_int64_batch(vec![1, 2, 3]);
    let partitions = vec![vec![batch]];

    let index = HashIndexBuilder::new(vec!["id".to_string()])
        .build(&partitions)
        .expect("failed to build index");

    let debug_str = format!("{index:?}");
    assert!(debug_str.contains("HashIndex"));
    assert!(debug_str.contains("len"));
    assert!(debug_str.contains("key_columns"));
}

#[test]
fn test_row_location_default() {
    let loc = RowLocation::default();
    assert_eq!(loc.partition, 0);
    assert_eq!(loc.batch, 0);
    assert_eq!(loc.row, 0);
}

// =============================================================================
// Memory Usage Tests
// =============================================================================

#[test]
fn test_memory_usage_bytes() {
    let ids: Vec<i64> = (0..10000).collect();
    let batch = create_int64_batch(ids);
    let partitions = vec![vec![batch]];

    let index = HashIndexBuilder::new(vec!["id".to_string()])
        .with_bloom_filter(true)
        .build(&partitions)
        .expect("failed to build index");

    let memory = index.memory_usage_bytes();
    // Should be reasonable for 10k entries
    // Each slot is ~16 bytes, with 256 shards
    assert!(
        memory > 0,
        "Memory usage should be positive: {memory} bytes"
    );
    assert!(
        memory < 10_000_000,
        "Memory usage should be reasonable: {memory} bytes"
    );
}

// =============================================================================
// Stress Tests for Linear Probing
// =============================================================================

#[test]
fn test_delete_chain_maintenance() {
    // Test that backward-shift deletion maintains probe chains correctly
    let index = HashIndex::new(vec!["id".to_string()]);

    // Insert 1000 entries
    for i in 0..1000_i64 {
        let hash = hash_key(&i);
        index.insert(hash, RowLocation::simple(0, i as u32));
    }

    // Delete every third entry
    for i in (0..1000_i64).step_by(3) {
        let hash = hash_key(&i);
        index.remove(hash);
    }

    // Verify remaining entries are still findable
    for i in 0..1000_i64 {
        if i % 3 == 0 {
            assert!(
                index.get(&i).is_none(),
                "Deleted key {i} should not be found"
            );
        } else {
            assert!(
                index.get(&i).is_some(),
                "Key {i} should still be findable after nearby deletions"
            );
        }
    }
}

#[test]
fn test_insert_delete_insert_cycle() {
    let index = HashIndex::new(vec!["id".to_string()]);

    // Perform multiple cycles of insert-delete-insert
    for cycle in 0..5 {
        // Insert 100 entries
        for i in 0..100_i64 {
            let key = cycle * 1000 + i;
            let hash = hash_key(&key);
            // Both outcomes are acceptable: successful insert or hash collision
            // with a key from a previous cycle that wasn't deleted
            let _ = index.insert(hash, RowLocation::simple(cycle as u32, i as u32));
        }

        // Delete half
        for i in 0..50_i64 {
            let key = cycle * 1000 + i;
            let hash = hash_key(&key);
            index.remove(hash);
        }
    }

    // Verify index is in consistent state
    assert!(!index.is_empty(), "Index should have some entries");
}

// =============================================================================
// index_threshold Function Tests
// =============================================================================

#[test]
fn test_index_threshold_calculation() {
    use crate::index_threshold;

    // With parallelism = 1
    assert_eq!(index_threshold(1), 256);

    // With parallelism = 8 (common default)
    assert_eq!(index_threshold(8), 2048);

    // With parallelism = 64
    assert_eq!(index_threshold(64), 16384);
}

// =============================================================================
// Batch Bloom Filter Tests
// =============================================================================

#[test]
fn test_batch_bloom_filter_from() {
    use crate::{BatchBloomFilter, BloomFilter};

    let bloom = BloomFilter::new(100);
    let mut batch_bloom: BatchBloomFilter = bloom.into();

    // Should work as expected
    batch_bloom.inner_mut().insert(12345);
    assert!(batch_bloom.inner().might_contain(12345));
}

// =============================================================================
// Bug Regression Tests
// =============================================================================

/// Bug #1: RowConverterKeyExtractor::hash_key should return None for rows with null keys
/// in composite key columns, but it currently always returns Some.
///
/// For single-column keys (primitive, string, binary), null values correctly return None
/// and are excluded from the index. Composite keys should behave the same way.
///
/// Verify that composite keys with null values in any column are correctly excluded.
#[test]
fn test_composite_key_null_values_excluded() {
    // Create a batch with composite key where one row has null in the key column
    let schema = Arc::new(Schema::new(vec![
        Field::new("region", DataType::Utf8, true), // nullable
        Field::new("id", DataType::Int64, false),
    ]));

    let region_array = StringArray::from(vec![Some("US"), None, Some("EU")]);
    let id_array = Int64Array::from(vec![1, 2, 3]);

    let batch = RecordBatch::try_new(schema, vec![Arc::new(region_array), Arc::new(id_array)])
        .expect("failed to create batch");

    let partitions = vec![vec![batch]];

    let index = HashIndexBuilder::new(vec!["region".to_string(), "id".to_string()])
        .build(&partitions)
        .expect("failed to build index");

    // Row 1 has null in region column, so it should be excluded.
    // Only rows 0 (US, 1) and 2 (EU, 3) should be indexed.
    assert_eq!(
        index.len(),
        2,
        "Composite key with null value should NOT be indexed"
    );
}

/// Test that null in the second column of a composite key also excludes the row.
#[test]
fn test_composite_key_null_in_second_column_excluded() {
    // Create a batch where the null is in the second key column
    let schema = Arc::new(Schema::new(vec![
        Field::new("region", DataType::Utf8, false),
        Field::new("id", DataType::Int64, true), // nullable in second column
    ]));

    let region_array = StringArray::from(vec!["US", "EU", "APAC"]);
    let id_array = Int64Array::from(vec![Some(1), None, Some(3)]);

    let batch = RecordBatch::try_new(schema, vec![Arc::new(region_array), Arc::new(id_array)])
        .expect("failed to create batch");

    let partitions = vec![vec![batch]];

    let index = HashIndexBuilder::new(vec!["region".to_string(), "id".to_string()])
        .build(&partitions)
        .expect("failed to build index");

    // Row 1 (EU, null) should be excluded because id is null
    // Only rows 0 (US, 1) and 2 (APAC, 3) should be indexed
    assert_eq!(
        index.len(),
        2,
        "Composite key with null in any column should NOT be indexed"
    );
}

/// Additional test: Verify single-column nullable key correctly excludes nulls
/// (This should pass - it's the baseline correct behavior)
#[test]
fn test_single_column_null_key_excluded_baseline() {
    // Single column with nulls - should correctly exclude null rows
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
    let id_array = Int64Array::from(vec![Some(1), None, Some(3)]);
    let batch =
        RecordBatch::try_new(schema, vec![Arc::new(id_array)]).expect("failed to create batch");

    let partitions = vec![vec![batch]];

    let index = HashIndexBuilder::new(vec!["id".to_string()])
        .build(&partitions)
        .expect("failed to build index");

    // This correctly excludes null - only 2 rows indexed
    assert_eq!(
        index.len(),
        2,
        "Single column null key should be excluded (this is correct behavior)"
    );
}

/// Bug #2: Race condition in insert_or_replace length tracking.
///
/// The insert_or_replace method reads shard.len() before and after the operation
/// using separate lock acquisitions, creating a TOCTOU race condition.
///
/// This test attempts to trigger the race by having multiple threads do
/// concurrent insert_or_replace operations on overlapping keys.
#[test]
fn test_concurrent_insert_or_replace_length_correctness() {
    let index = Arc::new(HashIndex::new(vec!["id".to_string()]));
    let num_threads = 8;
    let ops_per_thread = 1000;

    // All threads will insert/replace the same set of 100 keys
    // This maximizes contention and likelihood of hitting the race
    let keys: Vec<i64> = (0..100).collect();

    let handles: Vec<_> = (0..num_threads)
        .map(|thread_id| {
            let index = Arc::clone(&index);
            let keys = keys.clone();

            thread::spawn(move || {
                for op in 0..ops_per_thread {
                    let key = keys[op % keys.len()];
                    let hash = hash_key(&key);
                    let loc = RowLocation::simple(thread_id as u32, op as u32);
                    index.insert_or_replace(hash, loc);
                }
            })
        })
        .collect();

    for handle in handles {
        handle.join().expect("thread panicked");
    }

    let final_len = index.len();

    // With the race condition, the final_len may not equal the number of unique keys (100)
    // because the len counter can get out of sync.
    //
    // A correct implementation should always have exactly 100 entries.
    // Note: The race may not manifest every run, but repeated runs should catch it.
    assert_eq!(
        final_len, 100,
        "After concurrent insert_or_replace of 100 unique keys, index should have exactly 100 entries. \
         Got {final_len}. This may indicate a race condition in length tracking."
    );

    // Additional sanity check: verify all 100 keys are actually in the index
    for key in &keys {
        assert!(
            index.get(key).is_some(),
            "Key {key} should be in index after insert_or_replace"
        );
    }
}

/// Stress test for insert_or_replace to detect length counter drift over time.
#[test]
fn test_insert_or_replace_length_consistency() {
    let index = HashIndex::new(vec!["id".to_string()]);

    // Insert 1000 unique keys
    for i in 0..1000_i64 {
        let hash = hash_key(&i);
        index.insert_or_replace(hash, RowLocation::simple(0, i as u32));
    }

    assert_eq!(
        index.len(),
        1000,
        "Should have 1000 entries after initial insert"
    );

    // Replace all 1000 keys with new locations - len should stay the same
    for i in 0..1000_i64 {
        let hash = hash_key(&i);
        index.insert_or_replace(hash, RowLocation::simple(1, i as u32));
    }

    assert_eq!(
        index.len(),
        1000,
        "Length should remain 1000 after replacing all keys (no new entries)"
    );

    // Verify all keys point to the new locations
    for i in 0..1000_i64 {
        let loc = index.get(&i).expect("key should exist");
        assert_eq!(loc.batch, 1, "Key {i} should have been replaced to batch 1");
    }
}
