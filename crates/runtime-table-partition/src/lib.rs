/*
Copyright 2024-2025 The Spice.ai OSS Authors

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
use std::sync::Arc;

use datafusion::{catalog::TableProvider, scalar::ScalarValue};

pub mod creator;
pub mod expression;
pub mod insert;
pub mod provider;

/// Represents a partition in a partitioned table.
///
/// For single-column partitions, `partition_values` contains one value.
/// For composite/hierarchical partitions (e.g., `partition_by: [year, month]`),
/// `partition_values` contains multiple values in the order they were defined.
#[derive(Debug, Clone)]
pub struct Partition {
    /// The partition key values, one per partition expression.
    /// For hierarchical partitions like `year=2025/month=10`, this would be
    /// `[ScalarValue::Int32(2025), ScalarValue::Int32(10)]`.
    pub partition_values: Vec<ScalarValue>,
    pub table_provider: Arc<dyn TableProvider>,
}

impl Partition {
    /// Creates a new partition with a single partition value.
    /// This is a convenience constructor for single-expression partitions.
    #[must_use]
    pub fn new_single(
        partition_value: ScalarValue,
        table_provider: Arc<dyn TableProvider>,
    ) -> Self {
        Self {
            partition_values: vec![partition_value],
            table_provider,
        }
    }

    /// Creates a new partition with multiple partition values.
    /// Used for hierarchical partitions (e.g., year/month/day).
    #[must_use]
    pub fn new(partition_values: Vec<ScalarValue>, table_provider: Arc<dyn TableProvider>) -> Self {
        Self {
            partition_values,
            table_provider,
        }
    }

    /// Returns the first partition value, if any.
    #[must_use]
    pub fn first_value(&self) -> Option<&ScalarValue> {
        self.partition_values.first()
    }
}
