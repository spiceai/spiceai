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

//! [`PartitionOnlyScanRewrite`] answers a duplicate-insensitive aggregate over
//! only *per-file-constant* columns from the file listing instead of opening and
//! parsing every data file.
//!
//! A per-file-constant column has the same value in every row of a file:
//! - a **hive partition** value, fixed by the directory name (`p=1`, `p=2`, ...);
//!   and
//! - a **file metadata** column — `_location`, `_last_modified`, `_size` — fixed
//!   by the object listing.
//!
//! `DataFusion` resolves both from each [`PartitionedFile`] at physical planning
//! time, before any file is opened. When a projection references only such
//! columns, every row a file emits is identical, yet a file scan still emits one
//! row per record — so `SELECT p FROM t GROUP BY p` and
//! `SELECT MAX(_last_modified) FROM t` both read every row of every file to
//! re-derive the same per-file value, even though the answer needs one row per
//! file.
//!
//! This rule detects an [`AggregateExec`] whose result is unchanged by
//! duplicating input rows — a pure grouping (`GROUP BY`/`DISTINCT`) or
//! `MAX`/`MIN` — over such a scan reachable through multiplicity-agnostic
//! operators, and replaces that scan with a source of one row per file. The
//! aggregate above absorbs the collapse, so the result is identical while file
//! contents are (almost) untouched.
//!
//! Two conditions are always required:
//! - the aggregate is **duplicate-insensitive** — a pure grouping (no aggregate
//!   expressions) or `MAX`/`MIN` — so its result depends only on the *set* of
//!   input rows, never on how many copies of each it sees (`count`/`sum`/`avg`
//!   would need the real row counts and are left untouched); and
//! - the scan's projected schema contains **only** per-file-constant columns, so
//!   no file-content column is needed to answer the query.
//!
//! An empty file (zero rows) contributes nothing, so a file that is empty must
//! not surface a row. How the rule learns whether a file is empty depends on
//! what the format records, giving two replacements:
//!
//! - **Statistics fast path (no I/O).** When every file has an **exact** row
//!   count in its statistics, the rule reads no file at all: it builds an
//!   in-memory source of one row per non-empty file directly from the cached
//!   partition values, dropping any file with `Exact(0)` rows. This fires for
//!   formats that carry exact row counts, such as Parquet.
//! - **First-record probe.** When any file's row count is not exactly known
//!   (e.g. JSON/CSV without collected statistics — formats with no footer to
//!   read a count from), the rule instead replaces the scan with a
//!   [`FirstRecordProbeSource`], which reads at most the first record of each
//!   file. A file that yields a record contributes one row; an empty file
//!   yields none and its partition drops out. This decodes one record per file
//!   instead of every row of every file.
//!
//! Because every projected column is constant across a (non-empty) file, all of
//! that file's rows are identical, so collapsing them to one row cannot change
//! the set of rows a duplicate-insensitive aggregate observes, and therefore
//! cannot change the result. The statistics fast path synthesizes that row from
//! partition values, so it applies only to a partition-only projection; a
//! metadata column (whose value is not in the partition list) takes the probe.
//!
//! A predicate pushed into the file source itself (`FileSource::filter()`, e.g.
//! Parquet row-group/page pruning) is a third reason a file may contribute no
//! row, on top of the file being empty — and the cached statistics describe the
//! *unfiltered* file, so the fast path cannot evaluate it. Such a scan always
//! takes the first-record probe instead, which decides whether a file yields a
//! row through the same filtered decode path a full scan would use.
//!
//! An unresolved [`DynamicFilterPhysicalExpr`] (e.g. a `TopK` pruning filter
//! pushed below a `Sort`/`Limit`) is not such a predicate: at plan time it is
//! still a `lit(true)` placeholder that has not excluded anything, and it never
//! excludes a row this rewrite's output aggregate would otherwise have kept — a
//! `TopK` filter only prunes values that also lose to the final `Sort`/`Limit`.
//! Only its *current* resolved expression is checked against `lit(true)`; a
//! static predicate, or a dynamic filter already narrowed to something else,
//! still bails to the probe.

use std::sync::Arc;

use arrow::array::{ArrayRef, RecordBatch, new_empty_array};
use arrow::compute::cast;
use arrow::datatypes::SchemaRef;
use datafusion::common::Result;
use datafusion::common::stats::Precision;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode, TreeNodeRecursion};
use datafusion::config::ConfigOptions;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::{DynamicFilterPhysicalExpr, Literal};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::aggregates::AggregateExec;
#[expect(
    deprecated,
    reason = "DF53 deprecates CoalesceBatchesExec (arrow BatchCoalescer); the check below still recognizes it where it appears in a plan"
)]
use datafusion::physical_plan::coalesce_batches::CoalesceBatchesExec;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::scalar::ScalarValue;
use datafusion_datasource::PartitionedFile;
use datafusion_datasource::file::FileSource;
use datafusion_datasource::file_scan_config::{FileScanConfig, FileScanConfigBuilder};
use datafusion_datasource::memory::MemorySourceConfig;
use datafusion_datasource::source::DataSourceExec;

use crate::physical_plan::first_record_probe::FirstRecordProbeSource;

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

            // Only a duplicate-insensitive aggregate is safe: its result must
            // depend on the *set* of input rows, not how many copies of each it
            // sees. A pure grouping (`GROUP BY`/`DISTINCT`) and `MAX`/`MIN`
            // qualify; `count`/`sum`/`avg` do not.
            if !is_duplicate_insensitive_aggregate(aggregate) {
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

    fn name(&self) -> &'static str {
        "PartitionOnlyScanRewrite"
    }

    fn schema_check(&self) -> bool {
        // The replacement preserves the scan's projected schema exactly.
        true
    }
}

/// Walk down from an aggregate's input through multiplicity-agnostic operators,
/// replacing a partition-only file scan with a source of one row per file.
/// Returns `None` (leaving the plan unchanged) if no such scan is reachable, so
/// the rewrite only ever fires when it is provably safe.
fn rewrite_partition_only_scan(
    plan: &Arc<dyn ExecutionPlan>,
) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    if let Some(replacement) = try_rewrite_partition_only_leaf(plan)? {
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

/// Whether an operator preserves the set of rows reaching a downstream
/// duplicate-insensitive aggregate, so a constant-only scan beneath it can be
/// collapsed to one row per file without changing the result.
///
/// - Repartition / coalesce operators only reshuffle or merge rows.
/// - A *deterministic* `FilterExec` beneath a constant-only scan can only
///   reference the projected (per-file-constant) columns, so its predicate is
///   constant across a file and keeps or drops all of a file's rows together —
///   the surviving set is the same whether the file is one row or many. A
///   *volatile* predicate (e.g. `random() < 0.5`) references no data column yet
///   is re-evaluated per row, so collapsing a file to one row changes how many
///   of its rows survive; such a filter is not set-preserving.
/// - A nested duplicate-insensitive `AggregateExec` (a partial `DISTINCT` or
///   partial `MAX`/`MIN`) only removes duplicates or keeps an extremum.
#[expect(
    deprecated,
    reason = "DF53 deprecates CoalesceBatchesExec (arrow BatchCoalescer); the check below still recognizes it where it appears in a plan"
)]
fn is_set_preserving(plan: &Arc<dyn ExecutionPlan>) -> bool {
    let plan = plan.as_ref();
    if plan.downcast_ref::<RepartitionExec>().is_some()
        || plan.downcast_ref::<CoalesceBatchesExec>().is_some()
        || plan.downcast_ref::<CoalescePartitionsExec>().is_some()
    {
        return true;
    }
    if let Some(filter) = plan.downcast_ref::<FilterExec>() {
        return !predicate_is_volatile(filter.predicate());
    }
    if let Some(aggregate) = plan.downcast_ref::<AggregateExec>() {
        // A partial `DISTINCT` or partial `MAX`/`MIN` (the first phase of a
        // two-phase aggregate) only removes duplicates or keeps an extremum, so
        // it preserves the set of tuples the final aggregate observes.
        return is_duplicate_insensitive_aggregate(aggregate);
    }
    false
}

/// Whether `predicate` contains any volatile sub-expression (e.g. `random()`).
///
/// Volatility must be checked over the whole expression tree, not just the root
/// node: `random() < 0.5` is a non-volatile comparison whose left operand is the
/// volatile call, so `PhysicalExpr::is_volatile_node` on the root returns
/// `false`. `datafusion_physical_expr_common::is_volatile` does this recursive
/// walk but is not reachable through the `datafusion` facade, so replicate it
/// with the already-imported `TreeNode::apply`.
fn predicate_is_volatile(predicate: &Arc<dyn PhysicalExpr>) -> bool {
    let mut volatile = false;
    // The closure is infallible, so the walk cannot error.
    let _ = predicate.apply(|expr| {
        if expr.is_volatile_node() {
            volatile = true;
            Ok(TreeNodeRecursion::Stop)
        } else {
            Ok(TreeNodeRecursion::Continue)
        }
    });
    volatile
}

/// Whether `aggregate`'s result is unchanged by duplicating input rows. A pure
/// grouping (no aggregate expressions — `GROUP BY`/`DISTINCT`) and `MAX`/`MIN`
/// qualify, because each depends only on the *set* of inputs; `count`/`sum`/`avg`
/// do not, since they count multiplicity.
///
/// This pairs with the leaf check that a scan projects only per-file-constant
/// columns: when every row a file emits is identical, a duplicate-insensitive
/// aggregate above it gives the same result whether the file contributes one row
/// or many, so the scan may be collapsed to one row per file.
fn is_duplicate_insensitive_aggregate(aggregate: &AggregateExec) -> bool {
    aggregate.aggr_expr().iter().all(|expr| {
        let name = expr.fun().name();
        name.eq_ignore_ascii_case("max") || name.eq_ignore_ascii_case("min")
    })
}

/// If `plan` is a partition-only file scan under a pure grouping, replace it
/// with a source of one row per file. Prefers the statistics fast path (no
/// I/O); otherwise falls back to the first-record probe. Returns `None` if
/// `plan` is not such a scan, leaving it unchanged.
fn try_rewrite_partition_only_leaf(
    plan: &Arc<dyn ExecutionPlan>,
) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    let Some(config) = partition_only_file_scan(plan) else {
        return Ok(None);
    };

    // Fast path: exact row-count statistics let us answer from the cached
    // partition values without opening any file.
    if let Some(source) = try_partition_values_memory_source(config)? {
        return Ok(Some(source));
    }

    // Fallback: no exact row counts (e.g. JSON/CSV). Read at most the first
    // record of each file to learn whether its partition contributes a tuple.
    Ok(Some(build_first_record_probe_scan(config)))
}

/// Returns the [`FileScanConfig`] of `plan` when it is a file scan whose
/// projected schema contains **only** per-file-constant columns (hive partition
/// values and/or file metadata) and which is safe to rewrite. Otherwise `None`.
fn partition_only_file_scan(plan: &Arc<dyn ExecutionPlan>) -> Option<&FileScanConfig> {
    let scan = plan.as_ref().downcast_ref::<DataSourceExec>()?;
    let config = scan
        .data_source()
        .as_ref()
        .downcast_ref::<FileScanConfig>()?;

    // When the scan is organized by partition value it advertises hash
    // partitioning on the partition columns, which a downstream aggregate may
    // rely on for its input distribution (so no repartition was inserted). A
    // replacement cannot always advertise that partitioning, so leave such a
    // scan untouched rather than risk an unsatisfied distribution requirement.
    if config.partitioned_by_file_group {
        return None;
    }

    let projected_schema = config.projected_schema().ok()?;
    if projected_schema.fields().is_empty() {
        return None;
    }

    // Every projected column must be constant across a file: a hive partition
    // value (from the directory path) or a file metadata column
    // (`_location`/`_last_modified`/`_size`, from the object listing). Then every
    // row a file emits is identical, so one row per file carries the same tuple.
    // Otherwise file contents are required and the scan must not be collapsed.
    // An empty constant set matches nothing, so a plain data-column projection is
    // correctly left untouched.
    let partition_cols = config.table_partition_cols();
    let metadata_cols = config.file_source().table_schema().metadata_cols();
    let all_constant = projected_schema.fields().iter().all(|field| {
        partition_cols.iter().any(|c| c.name() == field.name())
            || metadata_cols.iter().any(|c| c.name() == field.name())
    });
    if !all_constant {
        return None;
    }

    Some(config)
}

/// Statistics fast path: when every file has an **exact** row count, build an
/// in-memory source of one row per non-empty file from the cached partition
/// values, touching no file. Returns `None` when any file's row count is not
/// exactly known, or when the scan carries a pushed-down source filter (whose
/// effect on which files are non-empty these statistics cannot capture), so the
/// caller falls back to the first-record probe.
fn try_partition_values_memory_source(
    config: &FileScanConfig,
) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    // A predicate pushed into the file source (e.g. Parquet row-group/page
    // pruning) decides which *rows* count, so it decides whether a file
    // contributes a partition tuple at all — but this fast path synthesizes a
    // row from each file's unfiltered exact row count, with no way to evaluate
    // that predicate. Bail to the first-record probe instead, which decides
    // whether a file yields a row by decoding through the identical filtered
    // path a full scan would use.
    //
    // An unresolved `TopK` dynamic filter is exempted: it starts (and, until a
    // downstream `Sort`/`Limit` has seen enough rows to narrow it, remains) a
    // `lit(true)` placeholder that excludes nothing, so it carries no
    // information this fast path would need to ignore.
    if let Some(filter) = config.file_source().filter()
        && !is_unresolved_dynamic_filter(&filter)
    {
        return Ok(None);
    }

    let projected_schema: SchemaRef = config.projected_schema()?;
    let partition_cols = config.table_partition_cols();

    // Map each projected column to its position in the partition-value vector.
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
        // any file whose count is not exactly known forces the fast path to bail
        // so the first-record probe handles the scan instead.
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
                        // the synthesized row wrong; bail to the probe.
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

/// Whether `filter` is a [`DynamicFilterPhysicalExpr`] still at its initial
/// `lit(true)` state — a `TopK` pruning filter that has not (yet) excluded
/// anything. A static predicate, and a dynamic filter already narrowed past
/// `lit(true)`, both return `false` and must bail the fast path.
fn is_unresolved_dynamic_filter(filter: &Arc<dyn PhysicalExpr>) -> bool {
    let Some(dynamic_filter) = filter.as_any().downcast_ref::<DynamicFilterPhysicalExpr>() else {
        return false;
    };
    let Ok(current) = dynamic_filter.current() else {
        return false;
    };
    matches!(
        current.as_any().downcast_ref::<Literal>(),
        Some(literal) if matches!(literal.value(), ScalarValue::Boolean(Some(true)))
    )
}

/// First-record probe fallback: rebuild the scan with a [`FirstRecordProbeSource`]
/// so each file yields at most its first record. Reuses the scan's own file
/// groups, projection, compression, and object store; only the file source (and
/// the batch size that bounds each read to one record) changes.
fn build_first_record_probe_scan(config: &FileScanConfig) -> Arc<dyn ExecutionPlan> {
    let probe: Arc<dyn FileSource> = Arc::new(FirstRecordProbeSource::new(Arc::clone(
        config.file_source(),
    )));

    let new_config = FileScanConfigBuilder::from(config.clone())
        .with_source(probe)
        .with_batch_size(Some(1))
        // A scan `limit` is applied per output partition *across* that
        // partition's files, so it would stop the probe after the first file(s)
        // and miss later partitions' values. A pure aggregate never pushes a
        // limit into its input, so this only guards against surprises.
        .with_limit(None)
        // One row per file is not the scan's original ordering; drop the claim
        // rather than assert an ordering the probe does not produce.
        .with_output_ordering(vec![])
        .build();

    DataSourceExec::from_data_source(new_config)
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::functions::math::random;
    use datafusion::logical_expr::Operator;
    use datafusion::physical_expr::ScalarFunctionExpr;
    use datafusion::physical_expr::expressions::{BinaryExpr, Column};

    /// Build the predicate `random() < 0.5`: a non-volatile comparison whose
    /// left operand is the volatile `random()` call. Its root node is the `<`
    /// `BinaryExpr`, so `is_volatile_node` on the root alone reports it as
    /// non-volatile — the recursive walk is what makes the difference.
    fn random_lt_half() -> Arc<dyn PhysicalExpr> {
        let random_call: Arc<dyn PhysicalExpr> = Arc::new(ScalarFunctionExpr::new(
            "random",
            random(),
            vec![],
            Arc::new(Field::new("random()", DataType::Float64, false)),
            Arc::new(ConfigOptions::default()),
        ));
        Arc::new(BinaryExpr::new(
            random_call,
            Operator::Lt,
            Arc::new(Literal::new(ScalarValue::Float64(Some(0.5)))),
        ))
    }

    /// A static predicate (not a `DynamicFilterPhysicalExpr`) always decides
    /// which rows count, so the fast path must bail regardless of its value.
    #[test]
    fn static_predicate_is_not_an_unresolved_dynamic_filter() {
        let filter: Arc<dyn PhysicalExpr> =
            Arc::new(Literal::new(ScalarValue::Boolean(Some(true))));
        assert!(!is_unresolved_dynamic_filter(&filter));
    }

    /// A `TopK` dynamic filter still at its initial `lit(true)` placeholder has
    /// excluded nothing, so the fast path may proceed as if no filter were
    /// present — this is the regression case for
    /// `SELECT p FROM t GROUP BY p ORDER BY p DESC LIMIT 1`, where DataFusion
    /// attaches a not-yet-resolved dynamic filter to the scan.
    #[test]
    fn unresolved_dynamic_filter_is_exempted() {
        let column = Arc::new(Column::new("p", 0)) as Arc<dyn PhysicalExpr>;
        let filter: Arc<dyn PhysicalExpr> = Arc::new(DynamicFilterPhysicalExpr::new(
            vec![column],
            Arc::new(Literal::new(ScalarValue::Boolean(Some(true)))),
        ));
        assert!(is_unresolved_dynamic_filter(&filter));
    }

    /// Once a `TopK` dynamic filter has been narrowed by the operator that owns
    /// it, it may exclude real rows — the same risk a static predicate poses —
    /// so the fast path must bail.
    #[test]
    fn resolved_dynamic_filter_is_not_exempted() {
        let column = Arc::new(Column::new("p", 0)) as Arc<dyn PhysicalExpr>;
        let dynamic_filter = Arc::new(DynamicFilterPhysicalExpr::new(
            vec![Arc::clone(&column)],
            Arc::new(Literal::new(ScalarValue::Boolean(Some(true)))),
        ));
        dynamic_filter
            .update(Arc::new(BinaryExpr::new(
                column,
                Operator::Gt,
                Arc::new(Literal::new(ScalarValue::Int32(Some(1)))),
            )))
            .expect("dynamic filter update succeeds");
        let filter = dynamic_filter as Arc<dyn PhysicalExpr>;
        assert!(!is_unresolved_dynamic_filter(&filter));
    }

    /// Volatility is a property of the whole predicate tree: `random() < 0.5`
    /// is a non-volatile comparison over a volatile call, so the root's
    /// `is_volatile_node` is `false` while the predicate is volatile overall.
    #[test]
    fn nested_volatile_call_is_detected() {
        let predicate = random_lt_half();
        assert!(
            !predicate.is_volatile_node(),
            "the root `<` node alone is not volatile"
        );
        assert!(
            predicate_is_volatile(&predicate),
            "the recursive walk must find the `random()` call"
        );
    }

    /// A deterministic predicate references no volatile function.
    #[test]
    fn deterministic_predicate_is_not_volatile() {
        let predicate: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("p", 0)),
            Operator::Gt,
            Arc::new(Literal::new(ScalarValue::Int32(Some(1)))),
        ));
        assert!(!predicate_is_volatile(&predicate));
    }

    /// A `FilterExec` with a volatile predicate re-evaluates per row, so
    /// collapsing a file to one row would change how many rows survive: it must
    /// not be treated as set-preserving, which stops the rewrite from descending
    /// through it. A deterministic filter stays set-preserving.
    #[test]
    fn volatile_filter_is_not_set_preserving() {
        let schema = Arc::new(Schema::new(vec![Field::new("p", DataType::Int32, false)]));
        let input: Arc<dyn ExecutionPlan> =
            MemorySourceConfig::try_new_exec(&[vec![]], Arc::clone(&schema), None)
                .expect("valid memory exec");

        let volatile_filter: Arc<dyn ExecutionPlan> = Arc::new(
            FilterExec::try_new(random_lt_half(), Arc::clone(&input)).expect("valid filter exec"),
        );
        assert!(
            !is_set_preserving(&volatile_filter),
            "a volatile filter must not be set-preserving"
        );

        let deterministic: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("p", 0)),
            Operator::Gt,
            Arc::new(Literal::new(ScalarValue::Int32(Some(1)))),
        ));
        let deterministic_filter: Arc<dyn ExecutionPlan> =
            Arc::new(FilterExec::try_new(deterministic, input).expect("valid filter exec"));
        assert!(
            is_set_preserving(&deterministic_filter),
            "a deterministic filter stays set-preserving"
        );
    }

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
