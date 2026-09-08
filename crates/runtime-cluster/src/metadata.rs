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
    common::{DFSchema, Statistics},
    error::DataFusionError,
    prelude::SessionContext,
    sql::TableReference,
};
use datafusion_expr::{Expr, ExprSchemable, lit};
use datafusion_proto::bytes::Serializeable;
use datafusion_proto_common::protobuf_common::Statistics as ProtoStatistics;
use prost::Message;
use runtime_datafusion::{SPICE_DEFAULT_CATALOG, SPICE_DEFAULT_SCHEMA};
use serde::{Deserialize, Serialize};

use crate::context::PartitionExprResolver;

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
pub type PartitionValue = HashMap<String, Option<String>>;

/// Encodes `DataFusion` [`Statistics`] to the `datafusion-proto` prost bytes
/// carried on the `PartitionsLoaded` wire. This is the executor → scheduler
/// encoding for the per-executor table statistics the scheduler then holds in
/// memory (see `ExecutorRegistry`).
#[must_use]
pub fn encode_statistics(stats: &Statistics) -> Vec<u8> {
    ProtoStatistics::from(stats).encode_to_vec()
}

/// Decodes statistics produced by [`encode_statistics`]. Returns `None` if the
/// bytes are malformed or cannot be converted back into [`Statistics`], so a
/// corrupt/forward-incompatible report degrades to "no statistics" rather than
/// failing the ack.
#[must_use]
pub fn decode_statistics(bytes: &[u8]) -> Option<Statistics> {
    ProtoStatistics::decode(bytes)
        .ok()
        .and_then(|proto| Statistics::try_from(&proto).ok())
}

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
    pub fn new(partition_value: HashMap<String, Option<String>>) -> Self {
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

/// Converts a partition value map into a serialized `DataFusion` [`Expr`] byte representation.
///
/// # Errors
///
/// Returns an error if the partition expression cannot be resolved or serialized.
pub async fn partition_value_to_bytes(
    p: PartitionValue,
    tbl: &TableReference,
    resolver: &dyn PartitionExprResolver,
) -> Result<Bytes, DataFusionError> {
    // Sort keys so the resulting AND-tree (and its proto bytes) is
    // independent of HashMap iteration order. The scheduler uses these
    // bytes as a stable identifier for a partition when matching
    // executor PartitionsLoaded acks against assigned partitions, so a
    // non-deterministic encoding would produce false misses.
    let mut entries: Vec<_> = p.into_iter().collect();
    entries.sort_by(|(a, _), (b, _)| a.cmp(b));

    let mut expr: Option<Expr> = None;
    for (partition_expr, val) in entries {
        let partition_by = resolver.try_parse_expr(tbl, &partition_expr).await?;
        let e = match val {
            None => partition_by.is_null(),
            Some(v) => partition_by.eq(lit(v)),
        };
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
    /// Fully qualified table name (always normalized via [`normalized_table_name`]).
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
    pub partition_expressions: Vec<String>,
}

impl TablePartitionMetadata {
    #[must_use]
    pub fn new(
        table: &TableReference,
        updated_at: u128,
        partition_expressions: Vec<String>,
    ) -> Self {
        Self {
            table_name: normalized_table_name(table),
            partitions: Vec::new(),
            schema_version: 1,
            updated_at,
            partition_expressions,
        }
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
    ///
    /// # Errors
    ///
    /// Returns an error if the table schema cannot be converted to a `DataFusion` schema
    /// or if a partition expression cannot be parsed.
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
                .map(|(proj, val)| {
                    let col = ctx.parse_sql_expr(proj, &df_schema)?;
                    let Some(val) = val else {
                        // NULL partition values need IS NULL, not = NULL
                        // (SQL: `col = NULL` is always UNKNOWN, never TRUE)
                        return Ok(col.is_null());
                    };
                    let col_type = col.get_type(&df_schema)?;
                    let mut lit_expr = ctx.parse_sql_expr(val, &df_schema)?;
                    if let Expr::Literal(ref s, None) = lit_expr
                        && s.data_type() != col_type
                    {
                        lit_expr = lit_expr.cast_to(&col_type, &df_schema)?;
                    }
                    Ok(col.eq(lit_expr))
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

/// Normalize a [`TableReference`] to a canonical string key by resolving bare/partial
/// references with the default catalog and schema. This ensures that
/// `my_table`, `public.my_table`, and `spice.public.my_table` all map to the same key.
#[must_use]
pub fn normalized_table_name(table: &TableReference) -> String {
    table
        .clone()
        .resolve(SPICE_DEFAULT_CATALOG, SPICE_DEFAULT_SCHEMA)
        .to_string()
}

/// Serialize each partition `Expr` to its proto byte form for inclusion in a
/// `PartitionsLoaded` ack. Failed encodes are logged and dropped so a single
/// malformed expression doesn't suppress the entire ack — the scheduler will
/// still receive every partition that *did* serialize, which is the encoding
/// the scheduler uses on the assignment side too.
///
/// `context` is included in the warning message so we can tell which table /
/// code path produced the failure.
#[must_use]
pub fn encode_partition_exprs(exprs: &[Expr], context: &str) -> Vec<Vec<u8>> {
    exprs
        .iter()
        .filter_map(|e| match e.to_bytes() {
            Ok(b) => Some(b.to_vec()),
            Err(err) => {
                tracing::warn!(
                    "Failed to encode partition Expr for {context} PartitionsLoaded ack: {err}"
                );
                None
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::common::{ColumnStatistics, stats::Precision};

    #[test]
    fn statistics_round_trip_through_wire_encoding() {
        // The executor → scheduler wire encoding must round-trip full statistics
        // losslessly (per-column min/max and distinct counts included), even
        // though today only `num_rows` is populated.
        let stats = Statistics {
            num_rows: Precision::Inexact(59_986_052),
            total_byte_size: Precision::Exact(1_234_567),
            column_statistics: vec![
                ColumnStatistics {
                    null_count: Precision::Exact(0),
                    distinct_count: Precision::Inexact(15_000_000),
                    ..ColumnStatistics::new_unknown()
                },
                ColumnStatistics::new_unknown(),
            ],
        };

        let decoded = decode_statistics(&encode_statistics(&stats)).expect("decode");
        assert_eq!(decoded, stats);
    }

    #[test]
    fn decode_statistics_rejects_garbage() {
        assert!(decode_statistics(&[0xff, 0x00, 0x13, 0x37]).is_none());
    }
}
