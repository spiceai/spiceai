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
use serde::{Deserialize, Deserializer, Serialize, Serializer};

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

/// A `DataFusion` [`Statistics`] that round-trips through serde via its
/// `datafusion-proto` representation.
///
/// `Statistics` is not itself `serde`-serializable, but it has a canonical
/// protobuf encoding (the same `datafusion-proto` mechanism this module already
/// uses to serialize `Expr`). This newtype delegates to that encoding, so the
/// full statistics — per-column min/max, distinct counts, etc. — round-trip
/// losslessly, even when callers only populate a subset such as `num_rows`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SerializableStatistics(pub Statistics);

impl From<Statistics> for SerializableStatistics {
    fn from(stats: Statistics) -> Self {
        Self(stats)
    }
}

impl Serialize for SerializableStatistics {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        ProtoStatistics::from(&self.0)
            .encode_to_vec()
            .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for SerializableStatistics {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let bytes = <Vec<u8>>::deserialize(deserializer)?;
        let proto = ProtoStatistics::decode(bytes.as_slice()).map_err(serde::de::Error::custom)?;
        let stats = Statistics::try_from(&proto).map_err(serde::de::Error::custom)?;
        Ok(Self(stats))
    }
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
    /// Statistics for the data in this partition, used by the query planner on
    /// the coordinator (e.g. to pick hash-join build sides).
    ///
    /// Carries full `DataFusion` [`Statistics`]; today only `num_rows` is
    /// populated (from the partition's record count), with column-level
    /// statistics left `Absent`. The abstraction is in place so richer
    /// statistics — or statistics from non-cayenne sources — can be filled in
    /// later without a metadata format change.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub statistics: Option<SerializableStatistics>,
}

impl PartitionMetadata {
    #[must_use]
    pub fn new(partition_value: HashMap<String, Option<String>>) -> Self {
        Self {
            partition_value,
            assigned_executors: Vec::new(),
            last_assigned_at: None,
            statistics: None,
        }
    }

    /// Sets the partition statistics to carry just a row-count estimate,
    /// leaving column-level statistics `Absent`. This is the minimal
    /// population used today; richer statistics can be set via
    /// [`PartitionMetadata::statistics`] directly.
    #[must_use]
    pub fn with_row_count(mut self, num_rows: usize) -> Self {
        let stats = Statistics {
            num_rows: datafusion::common::stats::Precision::Inexact(num_rows),
            total_byte_size: datafusion::common::stats::Precision::Absent,
            column_statistics: Vec::new(),
        };
        self.statistics = Some(SerializableStatistics(stats));
        self
    }

    /// Returns the row-count estimate for this partition, if known.
    #[must_use]
    pub fn num_rows(&self) -> Option<usize> {
        self.statistics
            .as_ref()
            .and_then(|s| s.0.num_rows.get_value().copied())
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
    fn serializable_statistics_round_trips_through_json() {
        // Full statistics, including column-level min/max and distinct counts,
        // must survive a JSON round-trip losslessly.
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

        let wrapped = SerializableStatistics(stats.clone());
        let json = serde_json::to_string(&wrapped).expect("serialize");
        let decoded: SerializableStatistics = serde_json::from_str(&json).expect("deserialize");

        assert_eq!(decoded.0, stats);
    }

    #[test]
    fn partition_metadata_row_count_round_trips() {
        let mut pv = HashMap::new();
        pv.insert("bucket".to_string(), Some("3".to_string()));
        let pm = PartitionMetadata::new(pv).with_row_count(42);

        assert_eq!(pm.num_rows(), Some(42));

        let json = serde_json::to_string(&pm).expect("serialize");
        let decoded: PartitionMetadata = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(decoded.num_rows(), Some(42));
        assert_eq!(decoded, pm);
    }

    #[test]
    fn partition_metadata_without_stats_omits_field() {
        let pm = PartitionMetadata::new(HashMap::new());
        assert_eq!(pm.num_rows(), None);
        let json = serde_json::to_string(&pm).expect("serialize");
        assert!(!json.contains("statistics"), "absent stats should be skipped: {json}");
    }
}
