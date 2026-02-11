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

use std::{collections::HashMap, sync::Arc};

use bytes::Bytes;
use datafusion::{error::DataFusionError, sql::TableReference};
use datafusion_expr::{Expr, lit};
use datafusion_proto::bytes::Serializeable;
use serde::{Deserialize, Serialize};

use crate::datafusion::DataFusion;

/// A specific set of values for partitioning keys.
/// For example, if a table is partitioned by "date" and "region", a `PartitionValue` might be {"date": "2024-01-01", "region": "us-east"}.
pub type PartitionValue = HashMap<String, String>;

/// Metadata for a single partition of an accelerated table
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PartitionMetadata {
    /// Partition value/identifier (e.g., date, id range)
    pub partition_value: PartitionValue,
    /// List of executor URLs assigned to this partition
    #[serde(default)]
    pub assigned_executors: Vec<String>,
    /// Timestamp when partition was last assigned
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_assigned_at: Option<u128>,
}

impl PartitionMetadata {
    #[must_use]
    pub fn new(partition_value: HashMap<String, String>) -> Self {
        Self {
            partition_value,
            assigned_executors: Vec::new(),
            last_assigned_at: None,
        }
    }

    #[must_use]
    pub fn is_assigned_to(&self, executor_url: &str) -> bool {
        self.assigned_executors.contains(&executor_url.to_string())
    }

    #[must_use]
    pub fn is_assigned(&self) -> bool {
        !self.assigned_executors.is_empty()
    }

    pub fn assign_to(&mut self, executor_url: String, timestamp: u128) {
        if !self.assigned_executors.contains(&executor_url) {
            self.assigned_executors.push(executor_url);
        }
        self.last_assigned_at = Some(timestamp);
    }

    pub fn unassign_from(&mut self, executor_url: &str) {
        self.assigned_executors.retain(|e| e != executor_url);
    }
}

pub async fn partition_value_to_bytes(
    p: PartitionValue,
    tbl: &TableReference,
    df: &Arc<DataFusion>,
) -> Result<Bytes, DataFusionError> {
    let mut expr: Option<Expr> = None;
    for (partition_expr, val) in p {
        let partition_by = df.try_parse_expr(tbl, &partition_expr).await?;
        let e = partition_by.eq(lit(val));
        expr = match expr {
            Some(existing) => Some(existing.and(e)),
            None => Some(e),
        };
    }
    expr.ok_or_else(|| DataFusionError::Plan("partition value is empty".to_string()))?
        .to_bytes()
}

/// Metadata for a database table with an acceleration.
///
/// Contains how the table is partitioned and which executors are responsible for each partition (refreshing and handling queries).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TablePartitionMetadata {
    /// Fully qualified table name
    pub table_name: String,
    /// All partitions for this table
    pub partitions: Vec<PartitionMetadata>,
    /// Schema version for migration compatibility
    pub schema_version: u32,
    /// Last updated timestamp (milliseconds since UNIX epoch)
    pub updated_at: u128,
}

impl TablePartitionMetadata {
    #[must_use]
    pub fn new(table_name: String, schema_version: u32, updated_at: u128) -> Self {
        Self {
            table_name,
            partitions: Vec::new(),
            schema_version,
            updated_at,
        }
    }

    #[must_use]
    pub fn blank(table_name: String, now_ms: u128) -> Self {
        Self::new(table_name, 1, now_ms)
    }

    pub fn add_partition(&mut self, partition: PartitionMetadata) {
        self.partitions.push(partition);
    }

    #[must_use]
    pub fn unassigned_partitions(&self) -> Vec<&PartitionMetadata> {
        self.partitions
            .iter()
            .filter(|p| !p.is_assigned())
            .collect()
    }
}
