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

//! Listing-time and in-memory segment pruning via `DataFusion`'s `FilePruner`.
//!
//! Uses footer- or batch-derived min/max statistics to drop whole Vortex files
//! (before `DataSourceExec` planning) and whole inline / RAM-tier segments
//! (before Arrow decode into the scan union) when a predicate is provably
//! unsatisfiable. Conservative: any uncertainty keeps the container.

use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use arrow_schema::SchemaRef;
use datafusion::optimizer::analyzer::type_coercion::TypeCoercionRewriter;
use datafusion_common::ScalarValue;
use datafusion_common::pruning::PrunableStatistics;
use datafusion_common::tree_node::TreeNode;
use datafusion_common::{DFSchema, Result, Statistics};
use datafusion_datasource::PartitionedFile;
use datafusion_expr::utils::conjunction;
use datafusion_expr::{Expr, col, lit};

use super::delete::{InsertRecordHandling, is_pk_visible_i64};
use super::deletion_index::DeletionIndex;
use datafusion_physical_expr::execution_props::ExecutionProps;
use datafusion_physical_expr::{PhysicalExpr, create_physical_expr};
use datafusion_physical_plan::metrics::Count;
use datafusion_pruning::{FilePruner, build_pruning_predicate};

use super::table::ColumnStatsAccumulator;

/// Build a physical conjunction from data-column scan filters for file/segment pruning.
pub(crate) fn build_listing_pruning_predicate(
    schema: &SchemaRef,
    filters: &[Expr],
) -> Result<Option<Arc<dyn PhysicalExpr>>> {
    if filters.is_empty() {
        return Ok(None);
    }

    let df_schema = DFSchema::try_from(schema.as_ref().clone())?;
    let mut coerced_filters = Vec::with_capacity(filters.len());
    for filter in filters {
        let mut rewriter = TypeCoercionRewriter::new(&df_schema);
        coerced_filters.push(filter.clone().rewrite(&mut rewriter)?.data);
    }

    let Some(predicate) = conjunction(coerced_filters) else {
        return Ok(None);
    };

    let execution_props = ExecutionProps::new();
    Ok(Some(create_physical_expr(
        &predicate,
        &df_schema,
        &execution_props,
    )?))
}

/// Returns `true` when the file's statistics prove the predicate cannot match any row.
pub(crate) fn should_prune_partitioned_file(
    file: &PartitionedFile,
    schema: &SchemaRef,
    predicate: &Arc<dyn PhysicalExpr>,
) -> Result<bool> {
    if !file.has_statistics() {
        return Ok(false);
    }

    let Some(mut pruner) =
        FilePruner::try_new(Arc::clone(predicate), schema, file, Count::default())
    else {
        return Ok(false);
    };

    pruner.should_prune()
}

/// Build `pk NOT IN (hidden keys)` for sparse Int64 tombstone sets so Vortex and
/// listing-time pruning can skip rows/files before the deletion filter exec runs.
/// Returns `None` when the set is empty, too large, or not single-column Int64.
pub(crate) fn tombstone_exclusion_filter(
    pk_column_name: &str,
    tombstones: &DeletionIndex,
    insert_record_handling: InsertRecordHandling,
    min_delete_seq_to_apply: Option<i64>,
    max_keys: usize,
) -> Option<Expr> {
    if !tombstones.has_deletions() {
        return None;
    }

    let mut hidden_keys = Vec::new();
    for (pk, _) in tombstones.iter_entries() {
        if !is_pk_visible_i64(
            pk,
            tombstones,
            insert_record_handling,
            min_delete_seq_to_apply,
        ) {
            hidden_keys.push(pk);
            if hidden_keys.len() > max_keys {
                return None;
            }
        }
    }

    if hidden_keys.is_empty() {
        return None;
    }

    let literals: Vec<Expr> = hidden_keys
        .into_iter()
        .map(|pk| lit(ScalarValue::Int64(Some(pk))))
        .collect();
    Some(Expr::InList(datafusion_expr::expr::InList::new(
        Box::new(col(pk_column_name)),
        literals,
        true,
    )))
}

/// Returns `true` when container statistics prove the predicate cannot match any row.
pub(crate) fn should_prune_statistics(
    stats: &Statistics,
    schema: &SchemaRef,
    predicate: &Arc<dyn PhysicalExpr>,
) -> bool {
    if stats.column_statistics.is_empty() {
        return false;
    }

    let prunable = PrunableStatistics::new(vec![Arc::new(stats.clone())], Arc::clone(schema));
    let Some(pruning_predicate) =
        build_pruning_predicate(Arc::clone(predicate), schema, &Count::default())
    else {
        return false;
    };

    match pruning_predicate.prune(&prunable) {
        Ok(values) => values.into_iter().all(|matched| !matched),
        Err(error) => {
            tracing::debug!(
                error = %error,
                "Ignoring error building pruning predicate for in-memory segment"
            );
            false
        }
    }
}

/// Compute exact min, max, and null-count statistics for one or more in-memory batches.
#[must_use]
pub(crate) fn statistics_from_record_batches(
    schema: &SchemaRef,
    batches: &[RecordBatch],
) -> Statistics {
    let num_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
    let column_statistics = (0..schema.fields().len())
        .map(|col_idx| {
            let mut min_value = datafusion_common::stats::Precision::Absent;
            let mut max_value = datafusion_common::stats::Precision::Absent;
            let mut null_count = 0usize;

            for batch in batches {
                if col_idx >= batch.num_columns() || batch.num_rows() == 0 {
                    continue;
                }
                let col = batch.column(col_idx);
                null_count += col.null_count();
                let col_stats = ColumnStatsAccumulator::compute_column_stats(col.as_ref());
                min_value = merge_min(&min_value, &col_stats.min_value);
                max_value = merge_max(&max_value, &col_stats.max_value);
            }

            datafusion_common::ColumnStatistics {
                null_count: datafusion_common::stats::Precision::Exact(null_count),
                min_value,
                max_value,
                sum_value: datafusion_common::stats::Precision::Absent,
                distinct_count: datafusion_common::stats::Precision::Absent,
                byte_size: datafusion_common::stats::Precision::Absent,
            }
        })
        .collect();

    Statistics {
        num_rows: datafusion_common::stats::Precision::Exact(num_rows),
        total_byte_size: datafusion_common::stats::Precision::Absent,
        column_statistics,
    }
}

fn merge_min(
    current: &datafusion_common::stats::Precision<datafusion_common::ScalarValue>,
    next: &datafusion_common::stats::Precision<datafusion_common::ScalarValue>,
) -> datafusion_common::stats::Precision<datafusion_common::ScalarValue> {
    match (current.get_value(), next.get_value()) {
        (None, None) => datafusion_common::stats::Precision::Absent,
        (None, Some(v)) | (Some(v), None) => datafusion_common::stats::Precision::Exact(v.clone()),
        (Some(left), Some(right)) => {
            if left.partial_cmp(right) == Some(std::cmp::Ordering::Greater) {
                datafusion_common::stats::Precision::Exact(right.clone())
            } else {
                datafusion_common::stats::Precision::Exact(left.clone())
            }
        }
    }
}

fn merge_max(
    current: &datafusion_common::stats::Precision<datafusion_common::ScalarValue>,
    next: &datafusion_common::stats::Precision<datafusion_common::ScalarValue>,
) -> datafusion_common::stats::Precision<datafusion_common::ScalarValue> {
    match (current.get_value(), next.get_value()) {
        (None, None) => datafusion_common::stats::Precision::Absent,
        (None, Some(v)) | (Some(v), None) => datafusion_common::stats::Precision::Exact(v.clone()),
        (Some(left), Some(right)) => {
            if left.partial_cmp(right) == Some(std::cmp::Ordering::Less) {
                datafusion_common::stats::Precision::Exact(right.clone())
            } else {
                datafusion_common::stats::Precision::Exact(left.clone())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_common::ScalarValue;
    use datafusion_expr::{col, lit};
    use std::collections::HashMap;

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Int64, true),
        ]))
    }

    fn batch(ids: &[i64], values: &[Option<i64>]) -> RecordBatch {
        RecordBatch::try_new(
            test_schema(),
            vec![
                Arc::new(Int64Array::from(ids.to_vec())),
                Arc::new(Int64Array::from(values.to_vec())),
            ],
        )
        .expect("batch")
    }

    #[test]
    fn statistics_from_batches_tracks_min_max() {
        let stats = statistics_from_record_batches(
            &test_schema(),
            &[
                batch(&[1, 2], &[Some(10), Some(20)]),
                batch(&[5], &[Some(5)]),
            ],
        );

        assert_eq!(
            stats.num_rows,
            datafusion_common::stats::Precision::Exact(3)
        );
        assert_eq!(
            stats.column_statistics[0].min_value,
            datafusion_common::stats::Precision::Exact(ScalarValue::Int64(Some(1)))
        );
        assert_eq!(
            stats.column_statistics[0].max_value,
            datafusion_common::stats::Precision::Exact(ScalarValue::Int64(Some(5)))
        );
    }

    #[test]
    fn prunes_segment_disjoint_from_predicate() {
        let schema = test_schema();
        let stats =
            statistics_from_record_batches(&schema, &[batch(&[1, 2, 3], &[None, None, None])]);
        let predicate = build_listing_pruning_predicate(&schema, &[col("id").eq(lit(99))])
            .expect("predicate")
            .expect("some");

        assert!(
            should_prune_statistics(&stats, &schema, &predicate),
            "segment [1,3] must be pruned for id = 99"
        );
    }

    #[test]
    fn tombstone_exclusion_builds_negated_in_list() {
        let index = DeletionIndex::from_map(HashMap::from([(10, 1), (20, 2)]));
        let filter =
            tombstone_exclusion_filter("id", &index, InsertRecordHandling::Ignore, None, 256)
                .expect("filter");
        let Expr::InList(in_list) = filter else {
            panic!("expected negated IN list");
        };
        assert!(in_list.negated);
        assert_eq!(in_list.list.len(), 2);
    }

    #[test]
    fn tombstone_exclusion_skips_reinserted_upsert_keys() {
        let index = DeletionIndex::from_maps(HashMap::from([(10, 5)]), HashMap::from([(10, 6)]));
        assert!(
            tombstone_exclusion_filter("id", &index, InsertRecordHandling::Apply, None, 256)
                .is_none(),
            "re-inserted keys must not be pushed into NOT IN"
        );
    }

    #[test]
    fn keeps_segment_overlapping_predicate() {
        let schema = test_schema();
        let stats =
            statistics_from_record_batches(&schema, &[batch(&[1, 2, 3], &[None, None, None])]);
        let predicate = build_listing_pruning_predicate(&schema, &[col("id").eq(lit(2))])
            .expect("predicate")
            .expect("some");

        assert!(
            !should_prune_statistics(&stats, &schema, &predicate),
            "segment containing id=2 must be kept"
        );
    }
}
