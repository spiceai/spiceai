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

//! Write-time executor assignment for new partition values.
//!
//! Decides which executor should receive rows for a previously-unseen partition
//! value during a write-through operation.
//!
//! ## Strategies
//!
//! - **Bucket-deterministic**: For tables partitioned with `bucket(N, col)`,
//!   the executor is chosen as `sorted_executors[k % len(sorted_executors)]`
//!   where `k` is the bucket scalar value and executors are alphabetically
//!   ordered. This distributes buckets evenly across available executors and
//!   guarantees co-location of the same bucket index across different tables,
//!   enabling local joins without data shuffling.
//!
//! - **Least-loaded**: Fallback for non-bucket partitions. Picks the executor
//!   with the fewest existing partition assignments, incrementally accounting
//!   for each pick within the same batch.

use std::collections::HashMap;

use arrow::array::RecordBatch;
use datafusion::scalar::ScalarValue;
use datafusion_expr::Expr;
use regex::Regex;
use tokio::sync::mpsc::Sender;

use super::metadata::PartitionValue;

type ExecutorId = String;

/// Selects the executor for each new partition value in `entries`.
///
/// For bucket-partitioned entries (single key matching `bucket(N, col)` with
/// scalar value `k`), the executor is chosen deterministically as the
/// `(k % len(executors))`'th executor in alphabetical order. This ensures that
/// bucket assignments are stable and evenly spread across executors regardless
/// of arrival order.
///
/// All other entries fall back to least-loaded assignment, incrementally
/// accounting for each prior pick.
pub(crate) fn select_least_loaded_executors(
    partitions_by_executor: &HashMap<String, Vec<Expr>>,
    senders: &HashMap<ExecutorId, Sender<RecordBatch>>,
    entries: &[(Vec<ScalarValue>, PartitionValue, RecordBatch)],
) -> Result<Vec<ExecutorId>, super::write_through::Error> {
    use super::write_through::Error;

    let count = entries.len();
    if senders.is_empty() {
        return Err(Error::NoExecutorsAvailable);
    }

    // Alphabetically sorted executor list for deterministic bucket assignment.
    let mut sorted_executors: Vec<&str> = senders.keys().map(String::as_str).collect();
    sorted_executors.sort_unstable();

    // Track load counts so each successive pick accounts for prior assignments.
    let mut load: HashMap<&str, usize> = senders
        .keys()
        .map(|id| {
            (
                id.as_str(),
                partitions_by_executor.get(id.as_str()).map_or(0, Vec::len),
            )
        })
        .collect();

    let mut result = Vec::with_capacity(count);
    for (scalar_values, partition_value, _) in entries {
        // Try deterministic bucket assignment: single-key partition whose key
        // is `bucket(N, col)` and whose scalar value can be read as an integer.
        if let Some(executor_id) =
            try_bucket_assignment(partition_value, scalar_values, &sorted_executors)
        {
            *load
                .get_mut(executor_id.as_str())
                .ok_or(Error::NoExecutorsAvailable)? += 1;
            result.push(executor_id);
            continue;
        }

        // Fallback: least-loaded executor.
        let executor_id = load
            .iter()
            .min_by_key(|&(_, &count)| count)
            .map(|(&id, _)| id.to_string())
            .ok_or(Error::NoExecutorsAvailable)?;
        *load
            .get_mut(executor_id.as_str())
            .ok_or(Error::NoExecutorsAvailable)? += 1;
        result.push(executor_id);
    }
    Ok(result)
}

/// Attempts deterministic bucket-based executor assignment.
///
/// Returns `Some(executor_id)` when:
///  - `partition_value` has exactly one key matching `bucket(N, …)`
///  - `scalar_values` has exactly one element convertible to an integer `k`
///
/// The assigned executor is `sorted_executors[k % len(sorted_executors)]`,
/// distributing buckets evenly across however many executors are available.
fn try_bucket_assignment(
    partition_value: &PartitionValue,
    scalar_values: &[ScalarValue],
    sorted_executors: &[&str],
) -> Option<String> {
    if partition_value.len() != 1 || scalar_values.len() != 1 || sorted_executors.is_empty() {
        return None;
    }

    let key = partition_value.keys().next()?;
    // Verify this is a bucket partition; we don't use N for indexing.
    let _n = parse_bucket_n(key)?;

    let k = scalar_to_u16(&scalar_values[0])?;
    let idx = usize::from(k) % sorted_executors.len();
    Some(sorted_executors[idx].to_string())
}

/// Parses the `N` from a partition key of the form `bucket(N, …)`.
/// Returns `None` if the key does not match this pattern.
fn parse_bucket_n(key: &str) -> Option<u16> {
    // Match optional surrounding parentheses, whitespace, then bucket(N, ...)
    // Captures the number N in the first group
    Regex::new(r"^\s*\(?\s*bucket\s*\(\s*(\d+)\s*,.*\)\s*\)?\s*$")
        .ok()?
        .captures(key)?
        .get(1)?
        .as_str()
        .parse::<u16>()
        .ok()
}

/// Converts a [`ScalarValue`] to `u16`, supporting common integer and unsigned types.
fn scalar_to_u16(scalar: &ScalarValue) -> Option<u16> {
    match scalar {
        ScalarValue::Int8(Some(v)) => u16::try_from(*v).ok(),
        ScalarValue::Int16(Some(v)) => u16::try_from(*v).ok(),
        ScalarValue::Int32(Some(v)) => u16::try_from(*v).ok(),
        ScalarValue::Int64(Some(v)) => u16::try_from(*v).ok(),
        ScalarValue::UInt8(Some(v)) => Some(u16::from(*v)),
        ScalarValue::UInt16(Some(v)) => Some(*v),
        ScalarValue::UInt32(Some(v)) => u16::try_from(*v).ok(),
        ScalarValue::UInt64(Some(v)) => u16::try_from(*v).ok(),
        _ => None,
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use super::*;
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn empty_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![0]))]).unwrap()
    }

    fn make_entry(
        key: &str,
        value: &str,
        scalar: ScalarValue,
    ) -> (Vec<ScalarValue>, PartitionValue, RecordBatch) {
        let mut pv = HashMap::new();
        pv.insert(key.to_string(), value.to_string());
        (vec![scalar], pv, empty_batch())
    }

    #[test]
    fn test_parse_bucket_n_valid() {
        assert_eq!(parse_bucket_n("bucket(3, c_nationkey)"), Some(3));
        assert_eq!(parse_bucket_n("bucket(16, col)"), Some(16));
        assert_eq!(parse_bucket_n("bucket( 5 , col )"), Some(5));
        assert_eq!(parse_bucket_n("(bucket(10, p_partkey))"), Some(10));
        assert_eq!(parse_bucket_n("( bucket(10 , p_partkey ))"), Some(10));
    }

    #[test]
    fn test_parse_bucket_n_invalid() {
        assert_eq!(parse_bucket_n("not_bucket(3, col)"), None);
        assert_eq!(parse_bucket_n("bucket(col)"), None);
        assert_eq!(parse_bucket_n("bucket(, col)"), None);
        assert_eq!(parse_bucket_n("bucket(-1, col)"), None);
        assert_eq!(parse_bucket_n(""), None);
    }

    #[test]
    fn test_parse_bucket_n_zero() {
        assert_eq!(parse_bucket_n("bucket(0, col)"), Some(0));
    }

    #[test]
    fn test_scalar_to_u16() {
        assert_eq!(scalar_to_u16(&ScalarValue::Int32(Some(7))), Some(7));
        assert_eq!(scalar_to_u16(&ScalarValue::UInt8(Some(255))), Some(255));
        assert_eq!(scalar_to_u16(&ScalarValue::Int64(Some(42))), Some(42));
        assert_eq!(scalar_to_u16(&ScalarValue::Int32(Some(-1))), None);
        assert_eq!(scalar_to_u16(&ScalarValue::Int32(None)), None);
        assert_eq!(
            scalar_to_u16(&ScalarValue::Utf8(Some("hello".to_string()))),
            None
        );
    }

    #[test]
    fn test_try_bucket_assignment_deterministic() {
        let executors = vec!["exec_a", "exec_b", "exec_c"];

        // bucket(3, col) with 3 executors: k % 3
        // k=0 → exec_a, k=1 → exec_b, k=2 → exec_c, k=3 → exec_a
        for (k, expected) in [(0, "exec_a"), (1, "exec_b"), (2, "exec_c"), (3, "exec_a")] {
            let mut pv = HashMap::new();
            pv.insert("bucket(3, col)".to_string(), k.to_string());
            let result = try_bucket_assignment(&pv, &[ScalarValue::Int32(Some(k))], &executors);
            assert_eq!(result.as_deref(), Some(expected), "k={k}");
        }
    }

    #[test]
    fn test_try_bucket_assignment_more_buckets_than_executors() {
        // 6 buckets but only 2 executors: k % 2
        let executors = vec!["exec_a", "exec_b"];

        for (k, expected) in [(0, "exec_a"), (1, "exec_b"), (2, "exec_a"), (5, "exec_b")] {
            let mut pv = HashMap::new();
            pv.insert("bucket(6, col)".to_string(), k.to_string());
            let result = try_bucket_assignment(&pv, &[ScalarValue::Int32(Some(k))], &executors);
            assert_eq!(result.as_deref(), Some(expected), "k={k}");
        }
    }

    #[test]
    fn test_try_bucket_assignment_skips_non_bucket() {
        let executors = vec!["exec_a", "exec_b"];
        let mut pv = HashMap::new();
        pv.insert("region".to_string(), "us-east".to_string());
        assert_eq!(
            try_bucket_assignment(
                &pv,
                &[ScalarValue::Utf8(Some("us-east".to_string()))],
                &executors
            ),
            None
        );
    }

    #[test]
    fn test_try_bucket_assignment_skips_multi_key() {
        let executors = vec!["exec_a", "exec_b"];
        let mut pv = HashMap::new();
        pv.insert("bucket(2, col1)".to_string(), "0".to_string());
        pv.insert("bucket(2, col2)".to_string(), "1".to_string());
        assert_eq!(
            try_bucket_assignment(
                &pv,
                &[ScalarValue::Int32(Some(0)), ScalarValue::Int32(Some(1))],
                &executors
            ),
            None
        );
    }

    #[test]
    fn test_select_bucket_over_least_loaded() {
        // 3 executors, exec_a already has 10 partitions, exec_b has 0.
        // bucket(3, col) with k=0 should still go to exec_a (alphabetically first).
        let mut partitions_by_executor: HashMap<String, Vec<Expr>> = HashMap::new();
        partitions_by_executor.insert("exec_a".to_string(), vec![datafusion_expr::lit(true); 10]);
        partitions_by_executor.insert("exec_b".to_string(), vec![]);
        partitions_by_executor.insert("exec_c".to_string(), vec![]);

        let senders: HashMap<String, Sender<RecordBatch>> = {
            let mut m = HashMap::new();
            for id in ["exec_a", "exec_b", "exec_c"] {
                let (tx, _rx) = tokio::sync::mpsc::channel(1);
                m.insert(id.to_string(), tx);
            }
            m
        };

        let entries = vec![make_entry(
            "bucket(3, col)",
            "0",
            ScalarValue::Int32(Some(0)),
        )];

        let result =
            select_least_loaded_executors(&partitions_by_executor, &senders, &entries).unwrap();
        assert_eq!(result, vec!["exec_a"]);
    }

    #[test]
    fn test_select_least_loaded_fallback() {
        // Non-bucket partition → should pick least loaded.
        let mut partitions_by_executor: HashMap<String, Vec<Expr>> = HashMap::new();
        partitions_by_executor.insert("exec_a".to_string(), vec![datafusion_expr::lit(true); 5]);
        partitions_by_executor.insert("exec_b".to_string(), vec![]);

        let senders: HashMap<String, Sender<RecordBatch>> = {
            let mut m = HashMap::new();
            for id in ["exec_a", "exec_b"] {
                let (tx, _rx) = tokio::sync::mpsc::channel(1);
                m.insert(id.to_string(), tx);
            }
            m
        };

        let entries = vec![make_entry(
            "region",
            "us-east",
            ScalarValue::Utf8(Some("us-east".to_string())),
        )];

        let result =
            select_least_loaded_executors(&partitions_by_executor, &senders, &entries).unwrap();
        assert_eq!(result, vec!["exec_b"]);
    }

    #[test]
    fn test_select_no_executors() {
        let partitions_by_executor: HashMap<String, Vec<Expr>> = HashMap::new();
        let senders: HashMap<String, Sender<RecordBatch>> = HashMap::new();
        let entries = vec![make_entry(
            "bucket(3, col)",
            "0",
            ScalarValue::Int32(Some(0)),
        )];

        let result = select_least_loaded_executors(&partitions_by_executor, &senders, &entries);
        result.unwrap_err();
    }
}
