/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

//! [`PartitionOnlyScanRewrite`] answers a `GROUP BY`/`DISTINCT` over only
//! (hive) partition columns from the directory listing instead of opening and
//! parsing every data file.
//!
//! The distinct values of a partition column are fully determined by the
//! directory names (`p=1`, `p=2`, ...), which DataFusion resolves into each
//! [`PartitionedFile::partition_values`] at physical planning time — before any
//! file is opened. When a projection references only partition columns, a file
//! scan still emits one row per record (the partition value repeated), so
//! `SELECT p FROM t GROUP BY p` reads every row of every file even though the
//! answer is the handful of distinct partition values.
//!
//! This rule detects an [`AggregateExec`] that computes a pure grouping (no
//! aggregate expressions — i.e. `GROUP BY`/`DISTINCT`) over a partition-only
//! file scan reachable through multiplicity-agnostic operators, and replaces
//! that scan with an in-memory source of one row per file carrying the file's
//! partition values. The aggregate above collapses the duplicates, so the
//! result is identical while no file contents are touched.
//!
//! Safety rests on three conditions, all required:
//! - the aggregate has **no** aggregate expressions, so its result depends only
//!   on the *set* of partition-column tuples, never on how many rows carry each
//!   tuple (`count(*)`, `sum`, ... would need the real row counts and are left
//!   untouched);
//! - the scan's projected schema contains **only** partition columns, so no
//!   file-content column is needed to answer the query; and
//! - every file has an **exact, non-zero** row count in its statistics. An empty
//!   file (zero rows) contributes no partition tuple to a `DISTINCT`, so emitting
//!   one row per file unconditionally would wrongly surface an empty partition's
//!   value. Files with `Exact(0)` rows are dropped; if any file's row count is
//!   not exactly known (e.g. JSON/CSV without collected statistics), the scan is
//!   left untouched. In practice this fires for formats that carry exact row
//!   counts, such as Parquet.
//!
//! Because a partition value is constant across every row of its (non-empty)
//! file, collapsing that file to a single row cannot change the set of partition
//! tuples the aggregate observes, and therefore cannot change the result.

use std::sync::Arc;

use arrow::array::{ArrayRef, RecordBatch, new_empty_array};
use arrow::compute::cast;
use arrow::datatypes::SchemaRef;
use datafusion::common::Result;
use datafusion::common::stats::Precision;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::config::ConfigOptions;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::aggregates::AggregateExec;
use datafusion::physical_plan::coalesce_batches::CoalesceBatchesExec;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::scalar::ScalarValue;
use datafusion_datasource::PartitionedFile;
use datafusion_datasource::file_scan_config::FileScanConfig;
use datafusion_datasource::memory::MemorySourceConfig;
use datafusion_datasource::source::DataSourceExec;

/// A [`PhysicalOptimizerRule`] that answers a `GROUP BY`/`DISTINCT` over only
/// partition columns from the directory listing instead of scanning file
/// contents. See the module documentation for the correctness argument.
#[derive(Debug, Default)]
pub struct PartitionOnlyScanRewrite {}

impl PartitionOnlyScanRewrite {
    #[must_use]
    pub fn new() -> Self {
        Self {}
    }
}

impl PhysicalOptimizerRule for PartitionOnlyScanRewrite {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_down(|node| {
            let Some(aggregate) = node.as_ref().downcast_ref::<AggregateExec>() else {
                return Ok(Transformed::no(node));
            };

            // Only a pure grouping (`GROUP BY`/`DISTINCT`) is safe: its result
            // depends on the set of partition tuples, not on their row counts.
            if !aggregate.aggr_expr().is_empty() {
                return Ok(Transformed::no(node));
            }

            let input = Arc::clone(aggregate.input());
            match rewrite_partition_only_scan(&input)? {
                Some(new_input) => Ok(Transformed::yes(node.with_new_children(vec![new_input])?)),
                None => Ok(Transformed::no(node)),
            }
        })
        .data()
    }

    fn name(&self) -> &str {
        "PartitionOnlyScanRewrite"
    }

    fn schema_check(&self) -> bool {
        // The replacement preserves the scan's projected schema exactly.
        true
    }
}

/// Walk down from an aggregate's input through multiplicity-agnostic operators,
/// replacing a partition-only file scan with an in-memory source of one row per
/// file. Returns `None` (leaving the plan unchanged) if no such scan is
/// reachable, so the rewrite only ever fires when it is provably safe.
fn rewrite_partition_only_scan(
    plan: &Arc<dyn ExecutionPlan>,
) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    if let Some(replacement) = try_partition_values_source(plan)? {
        return Ok(Some(replacement));
    }

    // Descend only through operators that preserve the set of partition tuples
    // the downstream aggregate observes (order and multiplicity are irrelevant
    // to a pure `GROUP BY`/`DISTINCT`). Any other operator — a join, a limit, a
    // non-distinct aggregate — could change the result, so we stop there.
    if !is_set_preserving(plan) {
        return Ok(None);
    }

    let children = plan.children();
    if children.len() != 1 {
        return Ok(None);
    }

    match rewrite_partition_only_scan(children[0])? {
        Some(new_child) => Ok(Some(Arc::clone(plan).with_new_children(vec![new_child])?)),
        None => Ok(None),
    }
}

/// Whether an operator preserves the set of partition-column tuples reaching a
/// downstream pure `GROUP BY`/`DISTINCT`, so a partition-only scan beneath it
/// can be collapsed to one row per file without changing the result.
///
/// - Repartition / coalesce operators only reshuffle or merge rows.
/// - A `FilterExec` beneath a partition-only scan can only reference partition
///   columns (nothing else is projected); a partition value is constant across
///   a file, so the predicate keeps or drops a file's rows all together — the
///   surviving set of tuples is the same whether the file is one row or many.
/// - A nested pure-grouping `AggregateExec` (a partial `DISTINCT`) only removes
///   duplicates.
fn is_set_preserving(plan: &Arc<dyn ExecutionPlan>) -> bool {
    let plan = plan.as_ref();
    if plan.downcast_ref::<RepartitionExec>().is_some()
        || plan.downcast_ref::<CoalesceBatchesExec>().is_some()
        || plan.downcast_ref::<CoalescePartitionsExec>().is_some()
        || plan.downcast_ref::<FilterExec>().is_some()
    {
        return true;
    }
    if let Some(aggregate) = plan.downcast_ref::<AggregateExec>() {
        return aggregate.aggr_expr().is_empty();
    }
    false
}

/// If `plan` is a file scan whose projected schema contains only partition
/// columns, build an in-memory source of one row per file holding that file's
/// partition values, preserving the scan's partition count. Otherwise `None`.
fn try_partition_values_source(
    plan: &Arc<dyn ExecutionPlan>,
) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    let Some(scan) = plan.as_ref().downcast_ref::<DataSourceExec>() else {
        return Ok(None);
    };
    let Some(config) = scan.data_source().as_ref().downcast_ref::<FileScanConfig>() else {
        return Ok(None);
    };

    let partition_cols = config.table_partition_cols();
    if partition_cols.is_empty() {
        return Ok(None);
    }

    // When the scan is organized by partition value it advertises hash
    // partitioning on the partition columns, which a downstream aggregate may
    // rely on for its input distribution (so no repartition was inserted). The
    // in-memory replacement cannot advertise that partitioning, so leave such a
    // scan untouched rather than risk an unsatisfied distribution requirement.
    if config.partitioned_by_file_group {
        return Ok(None);
    }

    let projected_schema: SchemaRef = config.projected_schema()?;
    if projected_schema.fields().is_empty() {
        return Ok(None);
    }

    // Map each projected column to its position in the partition-value vector.
    // Bail out the moment a projected column is not a partition column: the file
    // contents are required and the scan must not be skipped.
    let mut partition_value_index = Vec::with_capacity(projected_schema.fields().len());
    for field in projected_schema.fields() {
        match partition_cols.iter().position(|c| c.name() == field.name()) {
            Some(idx) => partition_value_index.push(idx),
            None => return Ok(None),
        }
    }

    // One in-memory partition per file group preserves the scan's output
    // partition count, so no downstream distribution assumption is disturbed.
    let mut partitions: Vec<Vec<RecordBatch>> = Vec::with_capacity(config.file_groups.len());
    for group in &config.file_groups {
        // Only files that provably contribute at least one row may become a
        // synthesized row. A `SELECT DISTINCT p` excludes a partition whose file
        // is empty (zero rows), so emitting one row per file unconditionally
        // would wrongly surface an empty partition's value. We therefore require
        // an *exact* per-file row count: a file with `Exact(0)` is dropped, and
        // any file whose count is not exactly known (e.g. JSON/CSV without
        // collected statistics) forces us to leave the whole scan untouched
        // rather than risk a wrong answer.
        let mut included: Vec<&PartitionedFile> = Vec::with_capacity(group.files().len());
        for file in group.files() {
            match file.statistics.as_ref().map(|stats| &stats.num_rows) {
                Some(Precision::Exact(0)) => {}
                Some(Precision::Exact(_)) => included.push(file),
                _ => return Ok(None),
            }
        }

        let mut columns: Vec<ArrayRef> = Vec::with_capacity(projected_schema.fields().len());
        for (column_idx, field) in projected_schema.fields().iter().enumerate() {
            let value_idx = partition_value_index[column_idx];
            let target_type = field.data_type();

            let array: ArrayRef = if included.is_empty() {
                new_empty_array(target_type)
            } else {
                let mut scalars = Vec::with_capacity(included.len());
                for file in &included {
                    let Some(value) = file.partition_values.get(value_idx) else {
                        // A file without the expected partition value would make
                        // the synthesized row wrong; leave the scan untouched.
                        return Ok(None);
                    };
                    scalars.push(value.clone());
                }
                let array = ScalarValue::iter_to_array(scalars)?;
                if array.data_type() == target_type {
                    array
                } else {
                    cast(array.as_ref(), target_type)?
                }
            };
            columns.push(array);
        }

        partitions.push(vec![RecordBatch::try_new(
            Arc::clone(&projected_schema),
            columns,
        )?]);
    }

    if partitions.is_empty() {
        return Ok(None);
    }

    let source = MemorySourceConfig::try_new_exec(&partitions, projected_schema, None)?;
    Ok(Some(source as Arc<dyn ExecutionPlan>))
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};

    /// The rule must leave a plan without a partition-only file-scan aggregate
    /// untouched — here a bare in-memory scan, which is not a file scan and has
    /// no aggregate above it, so there is nothing to rewrite.
    #[test]
    fn leaves_non_file_scan_untouched() {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef],
        )
        .expect("valid batch");
        let plan: Arc<dyn ExecutionPlan> =
            MemorySourceConfig::try_new_exec(&[vec![batch]], schema, None)
                .expect("valid memory exec");

        let optimized = PartitionOnlyScanRewrite::new()
            .optimize(plan, &ConfigOptions::default())
            .expect("optimize succeeds");

        // A non-file-scan leaf is left as an in-memory `DataSourceExec`; the
        // rule only ever replaces a partition-only *file* scan.
        let data_source = optimized
            .as_ref()
            .downcast_ref::<DataSourceExec>()
            .expect("plan remains a DataSourceExec");
        assert!(
            data_source
                .data_source()
                .as_ref()
                .downcast_ref::<MemorySourceConfig>()
                .is_some(),
            "rule must not rewrite a plan with no partition-only file scan"
        );
    }
}
