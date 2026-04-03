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

use arrow_schema::Schema;
use bytes::Bytes;
use datafusion::{
    common::DFSchema, error::DataFusionError, prelude::SessionContext, sql::TableReference,
};
use datafusion_expr::{Expr, ExprSchemable, lit};
use datafusion_proto::bytes::Serializeable;
use serde::{Deserialize, Serialize};

use crate::datafusion::DataFusion;

/// A specific value for partitioning keys.
/// For example, if a table is partitioned by:
///  - "date"
///  - "region"
///
/// Unique `PartitionValue`s might be (i.e. `Vec<PartitionValue>`):
/// ```json
/// {"date": "2024-01-01", "region": "us-east"}
/// {"date": "2024-01-01", "region": "us-west"}
/// {"date": "2024-01-02", "region": "us-east"}
/// ```
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
    pub fn is_assigned_to(&self, executor_id: &str) -> bool {
        self.assigned_executors.iter().any(|e| e == executor_id)
    }

    #[must_use]
    pub fn is_assigned(&self) -> bool {
        !self.assigned_executors.is_empty()
    }

    pub fn assign_to(&mut self, executor_id: String, timestamp: u128) {
        if !self.assigned_executors.contains(&executor_id) {
            self.assigned_executors.push(executor_id);
        }
        self.last_assigned_at = Some(timestamp);
    }

    pub fn unassign_from(&mut self, executor_id: &str) {
        self.assigned_executors.retain(|e| e != executor_id);
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

fn default_schema_version() -> u32 {
    1
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum DistributionMode {
    Partitioned,
    Replicated,
}

fn default_distribution_mode() -> DistributionMode {
    DistributionMode::Partitioned
}

/// Metadata for a database table with an acceleration.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TablePartitionMetadata {
    table_name: String,
    #[serde(default = "default_schema_version")]
    schema_version: u32,
    updated_at: u128,
    #[serde(default = "default_distribution_mode")]
    distribution_mode: DistributionMode,
    #[serde(default)]
    partitions: Vec<PartitionMetadata>,
    #[serde(default)]
    partition_expressions: Vec<String>,
}

impl TablePartitionMetadata {
    #[must_use]
    pub fn new_partitioned(
        table_name: String,
        updated_at: u128,
        partition_expressions: Vec<String>,
    ) -> Self {
        Self {
            table_name,
            schema_version: default_schema_version(),
            updated_at,
            distribution_mode: DistributionMode::Partitioned,
            partitions: Vec::new(),
            partition_expressions,
        }
    }

    #[must_use]
    pub fn new_replicated(table_name: String, updated_at: u128) -> Self {
        Self {
            table_name,
            schema_version: default_schema_version(),
            updated_at,
            distribution_mode: DistributionMode::Replicated,
            partitions: Vec::new(),
            partition_expressions: Vec::new(),
        }
    }

    #[must_use]
    pub fn is_replicated(&self) -> bool {
        matches!(self.distribution_mode, DistributionMode::Replicated)
    }

    #[must_use]
    pub fn partitions(&self) -> &[PartitionMetadata] {
        &self.partitions
    }

    pub fn partitions_mut(&mut self) -> Option<&mut Vec<PartitionMetadata>> {
        if self.is_replicated() {
            None
        } else {
            Some(&mut self.partitions)
        }
    }

    #[must_use]
    pub fn partition_expressions(&self) -> &[String] {
        &self.partition_expressions
    }

    pub fn set_partition_expressions_if_empty(&mut self, partition_expressions: Vec<String>) {
        if !self.is_replicated()
            && self.partition_expressions.is_empty()
            && !partition_expressions.is_empty()
        {
            self.partition_expressions = partition_expressions;
        }
    }

    pub fn set_partitions(&mut self, partitions: Vec<PartitionMetadata>) -> bool {
        if self.is_replicated() {
            false
        } else {
            self.partitions = partitions;
            true
        }
    }

    #[must_use]
    pub fn updated_at(&self) -> u128 {
        self.updated_at
    }

    pub fn set_updated_at(&mut self, updated_at: u128) {
        self.updated_at = updated_at;
    }

    pub fn add_partition(&mut self, partition: PartitionMetadata) -> bool {
        if let Some(partitions) = self.partitions_mut() {
            partitions.push(partition);
            true
        } else {
            false
        }
    }

    #[must_use]
    pub fn unassigned_partitions(&self) -> Vec<&PartitionMetadata> {
        self.partitions()
            .iter()
            .filter(|p| !p.is_assigned())
            .collect()
    }

    /// Returns a mapping of executor IDs to the partition expressions they contain.
    pub fn all_executor_partitions(
        &self,
        ctx: &Arc<SessionContext>,
        table_schema: &Arc<Schema>,
    ) -> Result<HashMap<String, Vec<Expr>>, DataFusionError> {
        let df_schema = DFSchema::try_from(Arc::clone(table_schema))?;
        let mut map: HashMap<String, Vec<Expr>> = HashMap::new();
        for PartitionMetadata {
            partition_value,
            assigned_executors,
            ..
        } in self.partitions()
        {
            // Build a single AND-combined predicate for this partition:
            //   key1 = val1 AND key2 = val2 AND ...
            let partition_predicate = partition_value
                .iter()
                .map(|(proj, lit)| {
                    let col = ctx.parse_sql_expr(proj, &df_schema)?;
                    let col_type = col.get_type(&df_schema)?;
                    let mut lit = ctx.parse_sql_expr(lit, &df_schema)?;
                    if let Expr::Literal(ref s, None) = lit
                        && s.data_type() != col_type
                    {
                        lit = lit.cast_to(&col_type, &df_schema)?;
                    }
                    Ok(col.eq(lit))
                })
                .collect::<Result<Vec<Expr>, DataFusionError>>()?
                .into_iter()
                .reduce(Expr::and);

            let Some(partition_predicate) = partition_predicate else {
                continue;
            };

            for executor in assigned_executors {
                map.entry(executor.clone())
                    .or_default()
                    .push(partition_predicate.clone());
            }
        }
        Ok(map)
    }
}
