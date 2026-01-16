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

use crate::{HashIndex, HashIndexBuilder, RowLocation, hash_key};

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

    assert!(index.insert(hash, loc1));
    assert!(!index.insert(hash, loc2)); // Should return false
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
        assert!(index.insert(hash, loc));
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
    assert!(index.insert(hash, loc2)); // Should succeed

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
        assert!(index.insert(hash, loc));
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
    assert!(index.insert(hash, loc1));
    assert_eq!(index.get(&key), Some(loc1));

    // Remove
    assert_eq!(index.remove(hash), Some(loc1));
    assert!(index.get(&key).is_none());
    assert_eq!(index.len(), 0);

    // Re-insert with different location
    let loc2 = RowLocation::new(1, 1, 100);
    assert!(index.insert(hash, loc2));
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
        assert!(index.insert(hash, loc), "Failed to insert key {i}");
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
