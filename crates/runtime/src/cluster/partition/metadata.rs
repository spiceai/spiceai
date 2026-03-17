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
    /// The SQL expression strings for partition-by expressions (e.g. `["bucket(3, c_nationkey)"]`).
    /// Stored so that auto-generated labels like `"expr0"` can be resolved back to the
    /// original SQL expression for query routing.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub partition_expressions: Vec<String>,
}

impl TablePartitionMetadata {
    #[must_use]
    pub fn new(table_name: String, schema_version: u32, updated_at: u128) -> Self {
        Self {
            table_name,
            partitions: Vec::new(),
            schema_version,
            updated_at,
            partition_expressions: Vec::new(),
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
        } in &self.partitions
        {
            // Build a single AND-combined predicate for this partition:
            //   key1 = val1 AND key2 = val2 AND ...
            let partition_predicate = partition_value
                .iter()
                .map(|(proj, lit)| {
                    // Ensure lit is same type as proj
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
