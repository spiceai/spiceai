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

use super::column_stats::ColumnStatsAccumulator;

/// Build a physical conjunction from data-column scan filters for file/segment pruning.
///
/// # Errors
///
/// Returns an error if a filter cannot be planned as a physical expression
/// against `schema`.
pub fn build_listing_pruning_predicate(
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
        &datafusion::logical_expr::physical_planning_context::PhysicalPlanningContext::default(),
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

/// Fast path for `col = i64` listing prune: compare against Exact/Inexact
/// footer min/max without building a `FilePruner`. Returns `true` when the
/// file cannot contain `value`. Conservative: missing stats keep the file.
pub(crate) fn should_prune_i64_eq(file: &PartitionedFile, column_idx: usize, value: i64) -> bool {
    let Some(stats) = file.statistics.as_ref() else {
        return false;
    };
    let Some(col) = stats.column_statistics.get(column_idx) else {
        return false;
    };
    let min = i64_bound(&col.min_value);
    let max = i64_bound(&col.max_value);
    match (min, max) {
        (Some(lo), Some(hi)) => value < lo || value > hi,
        _ => false,
    }
}

fn i64_bound(precision: &datafusion_common::stats::Precision<ScalarValue>) -> Option<i64> {
    match precision {
        datafusion_common::stats::Precision::Exact(ScalarValue::Int64(Some(v)))
        | datafusion_common::stats::Precision::Inexact(ScalarValue::Int64(Some(v))) => Some(*v),
        _ => None,
    }
}

/// Build `pk NOT IN (hidden keys)` for sparse Int64 tombstone sets so Vortex and
/// listing-time pruning can skip rows/files before the deletion filter exec runs.
/// Returns `None` when the set is empty, too large, or not single-column Int64.
///
/// `deleted_after`, when set, keeps only the keys whose newest delete is newer
/// than it — judged on the key's fused entry, not through
/// `min_delete_seq_to_apply`, whose run skipping can miss a re-insert recorded
/// in a skipped run.
pub(crate) fn tombstone_exclusion_filter(
    pk_column_name: &str,
    tombstones: &DeletionIndex,
    insert_record_handling: InsertRecordHandling,
    min_delete_seq_to_apply: Option<i64>,
    deleted_after: Option<i64>,
    max_keys: usize,
) -> Option<Expr> {
    if !tombstones.has_deletions() {
        return None;
    }

    let mut hidden_keys = Vec::new();
    for (pk, entry) in tombstones.iter_entries() {
        if deleted_after.is_some_and(|after| {
            entry
                .delete_sequence()
                .is_none_or(|delete_sequence| delete_sequence <= after)
        }) {
            continue;
        }
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

/// For each container, whether its statistics leave the predicate able to match:
/// `false` only where they prove no row matches, as [`should_prune_statistics`]
/// decides one container at a time.
///
/// Builds the pruning predicate once and evaluates every container in one pass,
/// where calling [`should_prune_statistics`] per container rebuilds it and copies
/// each container's statistics. A scan prunes every in-memory segment and inline
/// entry this way, so the per-container cost is paid per query.
pub fn statistics_may_match(
    stats: &[&Arc<Statistics>],
    schema: &SchemaRef,
    predicate: &Arc<dyn PhysicalExpr>,
) -> Vec<bool> {
    let mut may_match = vec![true; stats.len()];
    let known: Vec<usize> = (0..stats.len())
        .filter(|&i| !stats[i].column_statistics.is_empty())
        .collect();
    if known.is_empty() {
        return may_match;
    }
    let Some(pruning_predicate) =
        build_pruning_predicate(Arc::clone(predicate), schema, &Count::default())
    else {
        return may_match;
    };
    let prunable = TypedPrunableStatistics::new(
        known.iter().map(|&i| Arc::clone(stats[i])).collect(),
        schema,
    );
    match pruning_predicate.prune(&prunable) {
        Ok(values) => {
            for (&i, matched) in known.iter().zip(values) {
                may_match[i] = matched;
            }
        }
        Err(error) => {
            tracing::debug!(
                error = %error,
                "Ignoring error building pruning predicate for in-memory segments"
            );
        }
    }
    may_match
}

/// [`PrunableStatistics`] with each min/max array built at the schema's column
/// type. `PrunableStatistics` infers the array type from the first container's
/// value, so a first container without an exact bound makes the array fail to
/// build and no container is pruned by that column; a typed null keeps the others
/// prunable.
struct TypedPrunableStatistics {
    inner: PrunableStatistics,
    statistics: Vec<Arc<Statistics>>,
    schema: SchemaRef,
}

impl TypedPrunableStatistics {
    fn new(statistics: Vec<Arc<Statistics>>, schema: &SchemaRef) -> Self {
        Self {
            inner: PrunableStatistics::new(statistics.clone(), Arc::clone(schema)),
            statistics,
            schema: Arc::clone(schema),
        }
    }

    fn exact_bounds(
        &self,
        column: &datafusion_common::Column,
        bound: impl Fn(
            &datafusion_common::ColumnStatistics,
        ) -> &datafusion_common::stats::Precision<ScalarValue>,
    ) -> Option<arrow::array::ArrayRef> {
        let (statistics, schema) = (&self.statistics, &self.schema);
        let index = schema.index_of(column.name()).ok()?;
        let null = ScalarValue::try_from(schema.field(index).data_type()).ok()?;
        let mut has_value = false;
        let values = statistics.iter().map(|container| {
            match container.column_statistics.get(index).map(&bound) {
                Some(datafusion_common::stats::Precision::Exact(value)) => {
                    has_value = true;
                    value.clone()
                }
                _ => null.clone(),
            }
        });
        let array = ScalarValue::iter_to_array(values).ok()?;
        has_value.then_some(array)
    }
}

impl datafusion_common::pruning::PruningStatistics for TypedPrunableStatistics {
    fn min_values(&self, column: &datafusion_common::Column) -> Option<arrow::array::ArrayRef> {
        self.exact_bounds(column, |stat| &stat.min_value)
    }

    fn max_values(&self, column: &datafusion_common::Column) -> Option<arrow::array::ArrayRef> {
        self.exact_bounds(column, |stat| &stat.max_value)
    }

    fn num_containers(&self) -> usize {
        self.inner.num_containers()
    }

    fn null_counts(&self, column: &datafusion_common::Column) -> Option<arrow::array::ArrayRef> {
        self.inner.null_counts(column)
    }

    fn row_counts(&self) -> Option<arrow::array::ArrayRef> {
        self.inner.row_counts()
    }

    fn contained(
        &self,
        column: &datafusion_common::Column,
        values: &std::collections::HashSet<ScalarValue>,
    ) -> Option<arrow::array::BooleanArray> {
        self.inner.contained(column, values)
    }
}

/// Returns `true` when container statistics prove the predicate cannot match any row.
pub fn should_prune_statistics(
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
pub fn statistics_from_record_batches(schema: &SchemaRef, batches: &[RecordBatch]) -> Statistics {
    let num_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
    let column_statistics = (0..schema.fields().len())
        .map(|col_idx| {
            let mut min_value = datafusion_common::stats::Precision::Absent;
            let mut max_value = datafusion_common::stats::Precision::Absent;
            let mut null_count = 0usize;
            // A batch with a non-null value but no bound (a float column holding a
            // NaN, say) leaves the bound unknown, rather than contributing nothing.
            let mut unbounded = false;

            for batch in batches {
                if col_idx >= batch.num_columns() || batch.num_rows() == 0 {
                    continue;
                }
                let col = batch.column(col_idx);
                // Logical nulls: a dictionary key that names a null value, or a `Null`
                // column's rows, carry no null bit of their own.
                let col_nulls = col.logical_null_count();
                null_count += col_nulls;
                if unbounded {
                    continue;
                }
                let col_stats = ColumnStatsAccumulator::compute_column_stats(col.as_ref());
                if col_nulls < col.len()
                    && (col_stats.min_value.get_value().is_none()
                        || col_stats.max_value.get_value().is_none())
                {
                    unbounded = true;
                    min_value = datafusion_common::stats::Precision::Absent;
                    max_value = datafusion_common::stats::Precision::Absent;
                    continue;
                }
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

    /// Batched pruning decides every container exactly as pruning it alone does,
    /// across seeded random segments (including ones with no statistics) and a
    /// spread of predicate shapes.
    #[test]
    fn batched_pruning_matches_per_container_pruning() {
        let schema = test_schema();
        let mut seed: u64 = 0x5eed_1234_abcd_0001;
        let mut next = move |bound: i64| {
            seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
            i64::try_from((seed >> 33) % u64::try_from(bound).expect("positive bound"))
                .expect("fits i64")
        };
        let mut stats: Vec<Arc<Statistics>> = Vec::new();
        for _ in 0..200 {
            match next(10) {
                0 => stats.push(Arc::new(Statistics {
                    num_rows: datafusion_common::stats::Precision::Absent,
                    total_byte_size: datafusion_common::stats::Precision::Absent,
                    column_statistics: Vec::new(),
                })),
                1 => stats.push(Arc::new(Statistics::new_unknown(&schema))),
                _ => {
                    let start = next(1_000);
                    let len = 1 + next(50);
                    let ids: Vec<i64> = (start..start + len).collect();
                    let values: Vec<Option<i64>> = ids
                        .iter()
                        .map(|_| (next(4) != 0).then(|| next(500)))
                        .collect();
                    stats.push(Arc::new(statistics_from_record_batches(
                        &schema,
                        &[batch(&ids, &values)],
                    )));
                }
            }
        }
        let refs: Vec<&Arc<Statistics>> = stats.iter().collect();
        let predicates = vec![
            col("id").eq(lit(500_i64)),
            col("id").eq(lit(5_000_i64)),
            col("id").between(lit(100_i64), lit(140_i64)),
            col("id").gt(lit(900_i64)).or(col("value").lt(lit(10_i64))),
            col("value").is_null(),
            col("id").in_list(vec![lit(3_i64), lit(333_i64), lit(777_i64)], false),
            col("id").gt_eq(lit(0_i64)),
        ];
        let mut pruned = 0usize;
        for filter in predicates {
            let predicate = build_listing_pruning_predicate(&schema, std::slice::from_ref(&filter))
                .expect("predicate")
                .expect("some");
            let batched = statistics_may_match(&refs, &schema, &predicate);
            for (index, container) in stats.iter().enumerate() {
                let alone = !should_prune_statistics(container, &schema, &predicate);
                assert_eq!(
                    batched[index],
                    alone,
                    "container {index} decided differently for {filter}: {container:?}; batched over {} containers",
                    refs.len()
                );
                pruned += usize::from(!alone);
            }
        }
        assert!(
            pruned > 0,
            "the predicates must prune some containers, or nothing was compared"
        );
    }

    #[test]
    fn tombstone_exclusion_builds_negated_in_list() {
        let index = DeletionIndex::from_map(HashMap::from([(10, 1), (20, 2)]));
        let filter =
            tombstone_exclusion_filter("id", &index, InsertRecordHandling::Ignore, None, None, 256)
                .expect("filter");
        let Expr::InList(in_list) = filter else {
            panic!("expected negated IN list");
        };
        assert!(in_list.negated);
        assert_eq!(in_list.list.len(), 2);
    }

    /// Only keys whose newest delete is newer than every datalake file may be
    /// excluded from every branch: an older delete can name a key whose live
    /// copy is in a newer datalake file.
    #[test]
    fn tombstone_exclusion_keeps_only_keys_deleted_after_the_bound() {
        let index = DeletionIndex::from_map(HashMap::from([(10, 1), (20, 5)]));
        let filter = tombstone_exclusion_filter(
            "id",
            &index,
            InsertRecordHandling::Apply,
            None,
            Some(3),
            256,
        )
        .expect("filter");
        let Expr::InList(in_list) = filter else {
            panic!("expected negated IN list");
        };
        assert!(in_list.negated);
        assert_eq!(in_list.list, vec![lit(ScalarValue::Int64(Some(20)))]);
        assert!(
            tombstone_exclusion_filter(
                "id",
                &index,
                InsertRecordHandling::Apply,
                None,
                Some(5),
                256
            )
            .is_none(),
            "no key is deleted after the newest datalake file"
        );
    }

    #[test]
    fn tombstone_exclusion_skips_reinserted_upsert_keys() {
        let index = DeletionIndex::from_maps(HashMap::from([(10, 5)]), HashMap::from([(10, 6)]));
        assert!(
            tombstone_exclusion_filter("id", &index, InsertRecordHandling::Apply, None, None, 256)
                .is_none(),
            "re-inserted keys must not be pushed into NOT IN"
        );
    }

    /// A segment's null count is the column's logical nulls. A dictionary row whose key is
    /// valid but whose dictionary value is null, and every row of a `Null`-typed column, is
    /// null without a null bit of its own; counting only null bits reports none, and
    /// `IS NULL` then prunes a segment that holds a matching row.
    #[test]
    fn segment_statistics_count_logical_nulls() {
        use arrow::array::{ArrayRef, DictionaryArray, Int32Array, NullArray, StringArray};
        use arrow::datatypes::Int32Type;

        let schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new(
                "tag",
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                true,
            ),
            Field::new("nothing", DataType::Null, true),
        ]));
        // Key 0 names the null dictionary value; key 1 names "x".
        let tags = DictionaryArray::<Int32Type>::try_new(
            Int32Array::from(vec![0, 1]),
            Arc::new(StringArray::from(vec![None, Some("x")])) as ArrayRef,
        )
        .expect("dictionary");
        let segment = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(tags),
                Arc::new(NullArray::new(2)),
            ],
        )
        .expect("batch");
        let stats = statistics_from_record_batches(&schema, &[segment]);
        assert_eq!(
            stats.column_statistics[1].null_count,
            datafusion_common::stats::Precision::Exact(1)
        );
        assert_eq!(
            stats.column_statistics[2].null_count,
            datafusion_common::stats::Precision::Exact(2)
        );
        for column in ["tag", "nothing"] {
            let predicate = build_listing_pruning_predicate(&schema, &[col(column).is_null()])
                .expect("predicate")
                .expect("some");
            assert!(
                !should_prune_statistics(&stats, &schema, &predicate),
                "a segment holding a null {column} must be kept for {column} IS NULL"
            );
        }
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
