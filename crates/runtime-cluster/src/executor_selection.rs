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

//! Executor selection optimization for distributed queries.
//!
//! This module provides algorithms to select the minimal set of executors
//! that cover all required partitions for a query, and validates that
//! all required partitions are available.

use std::collections::HashMap;

use crate::metadata::PartitionValue;

/// Errors that can occur during executor selection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    /// One or more required partitions are not assigned to any executor.
    MissingPartitions(Vec<PartitionValue>),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingPartitions(partitions) => {
                write!(
                    f,
                    "Cannot execute query: {} partition(s) not assigned to any executor",
                    partitions.len()
                )
            }
        }
    }
}

impl std::error::Error for Error {}

/// Select the minimal set of executors to cover all required partitions.
///
/// Uses a greedy algorithm that iteratively selects the executor with the most
/// coverage of remaining needed partitions. This provides a good approximation
/// to the minimum set cover problem.
///
/// # Arguments
///
/// * `required_partitions` - Set of partition values that must be available
/// * `executor_partitions` - Map of executor ID to partitions that executor has
///
/// # Returns
///
/// * `Ok(Vec<String>)` - List of executor IDs that together cover all required partitions
/// * `Err(Error::MissingPartitions)` - Some required partitions are not available on any executor
///
/// # Errors
///
/// Returns `Error::MissingPartitions` if any required partition is not covered by the available executors.
///
/// # Example
///
/// ```ignore
/// let required = HashSet::from([partition_a, partition_b, partition_c]);
/// let available = HashMap::from([
///     ("executor1".to_string(), vec![partition_a, partition_b]),
///     ("executor2".to_string(), vec![partition_b, partition_c]),
///     ("executor3".to_string(), vec![partition_b]),
/// ]);
///
/// let selected = select_executors(&required, &available)?;
/// // Returns ["executor1", "executor2"] - minimal set covering all partitions
/// ```
#[expect(clippy::implicit_hasher)]
pub fn select_executors(
    required_partitions: &[PartitionValue],
    executor_partitions: &HashMap<String, Vec<PartitionValue>>,
) -> Result<Vec<String>, Error> {
    if required_partitions.is_empty() {
        return Ok(Vec::new());
    }

    let mut needed: Vec<PartitionValue> = required_partitions.to_vec();
    let mut selected: Vec<String> = Vec::new();

    // Greedy algorithm: repeatedly select executor with most coverage of remaining partitions
    while !needed.is_empty() {
        // Find executor with maximum coverage of remaining needed partitions.
        // Break ties by executor ID to ensure deterministic selection order.
        let best_executor = executor_partitions
            .iter()
            .map(|(exec_id, partitions)| {
                let coverage = partitions.iter().filter(|p| needed.contains(p)).count();
                (exec_id, partitions, coverage)
            })
            .max_by(|(id_a, _, cov_a), (id_b, _, cov_b)| {
                cov_a.cmp(cov_b).then_with(|| id_b.cmp(id_a))
            });

        match best_executor {
            Some((exec_id, partitions, coverage)) if coverage > 0 => {
                // Select this executor
                selected.push(exec_id.clone());

                // Remove covered partitions from needed set
                needed.retain(|needed_p| !partitions.contains(needed_p));

                tracing::debug!(
                    "Selected executor '{}' covering {} partition(s), {} remaining",
                    exec_id,
                    coverage,
                    needed.len()
                );
            }
            _ => {
                // No executor can cover any remaining partitions
                return Err(Error::MissingPartitions(needed));
            }
        }
    }

    tracing::debug!(
        "Selected {} executor(s) to cover {} partition(s)",
        selected.len(),
        required_partitions.len()
    );

    Ok(selected)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_partition(key: &str, value: &str) -> PartitionValue {
        let mut p = HashMap::new();
        p.insert(key.to_string(), Some(value.to_string()));
        p
    }

    #[test]
    fn test_select_executors_empty_required() {
        let required = Vec::new();
        let available = HashMap::new();

        let result = select_executors(&required, &available);
        assert!(result.is_ok());
        assert!(result.expect("should be ok").is_empty());
    }

    #[test]
    fn test_select_executors_single_executor_covers_all() {
        let part_a = make_partition("date", "2024-01-01");
        let part_b = make_partition("date", "2024-01-02");

        let required = vec![part_a.clone(), part_b.clone()];

        let mut available = HashMap::new();
        available.insert("executor1".to_string(), vec![part_a, part_b]);

        let result = select_executors(&required, &available);
        assert!(result.is_ok());
        let selected = result.expect("should be ok");
        assert_eq!(selected.len(), 1);
        assert_eq!(selected[0], "executor1");
    }

    #[test]
    fn test_select_executors_multiple_executors_needed() {
        let part_a = make_partition("date", "2024-01-01");
        let part_b = make_partition("date", "2024-01-02");
        let part_c = make_partition("date", "2024-01-03");

        let required = vec![part_a.clone(), part_b.clone(), part_c.clone()];

        let mut available = HashMap::new();
        available.insert("executor1".to_string(), vec![part_a, part_b]);
        available.insert("executor2".to_string(), vec![part_c]);

        let result = select_executors(&required, &available);
        assert!(result.is_ok());
        let selected = result.expect("should be ok");
        assert_eq!(selected.len(), 2);
        assert!(selected.contains(&"executor1".to_string()));
        assert!(selected.contains(&"executor2".to_string()));
    }

    #[test]
    fn test_select_executors_chooses_optimal_subset() {
        // Executor 1: [A, B, C]
        // Executor 2: [B, C, D]
        // Executor 3: [B, C]
        // Required: [A, B, C, D]
        // Optimal: Executor 1 + Executor 2 (avoid Executor 3)

        let part_a = make_partition("date", "2024-01-01");
        let part_b = make_partition("date", "2024-01-02");
        let part_c = make_partition("date", "2024-01-03");
        let part_d = make_partition("date", "2024-01-04");

        let required = vec![
            part_a.clone(),
            part_b.clone(),
            part_c.clone(),
            part_d.clone(),
        ];

        let mut available = HashMap::new();
        available.insert(
            "executor1".to_string(),
            vec![part_a, part_b.clone(), part_c.clone()],
        );
        available.insert(
            "executor2".to_string(),
            vec![part_b.clone(), part_c.clone(), part_d],
        );
        available.insert("executor3".to_string(), vec![part_b, part_c]);

        let result = select_executors(&required, &available);
        assert!(result.is_ok());
        let selected = result.expect("should be ok");

        // Should select exactly 2 executors (greedy picks executor1 first with 3 partitions,
        // then executor2 to cover D)
        assert_eq!(selected.len(), 2);
        assert!(selected.contains(&"executor1".to_string()));
        assert!(selected.contains(&"executor2".to_string()));
        // executor3 should not be selected (not needed)
        assert!(!selected.contains(&"executor3".to_string()));
    }

    #[test]
    fn test_select_executors_missing_partition() {
        let part_a = make_partition("date", "2024-01-01");
        let part_b = make_partition("date", "2024-01-02");
        let part_c = make_partition("date", "2024-01-03");

        let required = vec![part_a.clone(), part_b.clone(), part_c.clone()];

        let mut available = HashMap::new();
        // Only has A and B, missing C
        available.insert("executor1".to_string(), vec![part_a, part_b]);

        let result = select_executors(&required, &available);
        assert!(result.is_err());
        match result.expect_err("should be err") {
            Error::MissingPartitions(missing) => {
                assert_eq!(missing.len(), 1);
                assert!(missing.contains(&part_c));
            }
        }
    }

    #[test]
    fn test_select_executors_no_executors() {
        let part_a = make_partition("date", "2024-01-01");
        let required = vec![part_a.clone()];
        let available = HashMap::new();

        let result = select_executors(&required, &available);
        assert!(result.is_err());
        match result.expect_err("should be err") {
            Error::MissingPartitions(missing) => {
                assert_eq!(missing.len(), 1);
                assert!(missing.contains(&part_a));
            }
        }
    }

    #[test]
    fn test_select_executors_composite_partitions() {
        let mut part_a = HashMap::new();
        part_a.insert("region".to_string(), Some("us-east".to_string()));
        part_a.insert("date".to_string(), Some("2024-01-01".to_string()));

        let mut part_b = HashMap::new();
        part_b.insert("region".to_string(), Some("us-west".to_string()));
        part_b.insert("date".to_string(), Some("2024-01-01".to_string()));

        let required = vec![part_a.clone(), part_b.clone()];

        let mut available = HashMap::new();
        available.insert("executor1".to_string(), vec![part_a.clone()]);
        available.insert("executor2".to_string(), vec![part_b.clone()]);

        let result = select_executors(&required, &available);
        assert!(result.is_ok());
        let selected = result.expect("should be ok");
        assert_eq!(selected.len(), 2);
    }
}
