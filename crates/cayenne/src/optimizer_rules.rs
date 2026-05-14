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

//! Physical optimizer rules for Cayenne execution plans.
//!
//! # No-spill build-side memory strategy (q21 / chbench multi-way joins)
//!
//! DataFusion's `HashJoinExec` build side is non-spillable. Under the runtime
//! memory pool (`GreedyMemoryPool` wrapped in `TrackConsumersPool`), wide chbench
//! shapes such as q21 (a 5-way join feeding a correlated `NOT EXISTS` self-join
//! over `order_line`) exhaust the `HashJoinInput[N]` reservations because each
//! build-side hash table independently materializes its full keyspace.
//!
//! The existing `CayenneJoinRewriter` only helps the **probe** side: it swaps
//! the default in-list accumulator for [`ExactLeftAccumulator`], which produces
//! a precise dynamic filter (or falls back to `RangeBounds` + `BloomFilter`)
//! that DataFusion's filter-pushdown phase plants into the right-side
//! `CayenneAccelerationExec`'s `FileSource`. It does nothing to shrink build
//! sides, so q21 is currently excluded from
//! `test_framework::queries::get_chbench_test_queries`.
//!
//! Three follow-on workstreams are tracked for the `lukim/q21` branch:
//!
//! 1. **Cross-scan dynamic filter sharing** (highest leverage for q21). When a
//!    join's `Arc<DynamicFilterPhysicalExpr>` is pushed into one
//!    `CayenneAccelerationExec`, install the *same* `Arc` (which shares its
//!    `Arc<RwLock<Inner>>` state via `DynamicFilterPhysicalExpr` design) on
//!    every sibling `CayenneAccelerationExec` backed by the same underlying
//!    table. This requires:
//!      - A stable table-identity accessor on `CayenneAccelerationExec` (walk
//!        the inner plan to the `DataSourceExec`'s `FileSource` and hash its
//!        object-store paths + table reference).
//!      - A post-pushdown physical optimizer pass that walks the plan, groups
//!        same-source Cayenne scans, and ANDs the union of in-flight dynamic
//!        filters into each sibling's predicate.
//!      - Column remapping when projection ordering differs between siblings.
//!
//! 2. **Bidirectional build-side accumulator pushdown.** Extend
//!    `CayenneJoinRewriter` to also rewrite joins whose **build** side is a
//!    `CayenneAccelerationExec`. The build-vs-probe asymmetry means this needs
//!    either a precursor pass that materializes the probe's filter set first
//!    (semantically reversing the dataflow for anti-joins) or a planner hint
//!    that swaps build/probe when the build side is the dominant cardinality.
//!
//! 3. **Predicate transitive closure across equi-join keys.** Logical optimizer
//!    rule that propagates `IN (...)` and range predicates through equi-join
//!    chains so that a selective filter on one table reaches every transitively
//!    equi-joined column at plan time, not just at runtime.
//!
//! Until at least #1 lands, q21 remains disabled in the chbench query set.
//! See the comment in `crates/test-framework/src/queries/mod.rs` next to
//! `get_chbench_test_queries`.

use datafusion::common::NullEquality;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::config::ConfigOptions;
use datafusion::error::DataFusionError;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::joins::HashJoinExec;
use datafusion::{error::Result, physical_plan::projection::ProjectionExec};
use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_expr::expressions::Column;
use datafusion_physical_plan::coalesce_batches::CoalesceBatchesExec;
use datafusion_physical_plan::repartition::RepartitionExec;
use runtime_datafusion::execution_plan::schema_cast::SchemaCastScanExec;
use runtime_datafusion::extension::bytes_processed::BytesProcessedExec;
use runtime_datafusion::join_accumulator::ExactLeftAccumulator;
use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use crate::provider::CayenneAccelerationExec;
use crate::provider::scan::{IsCayenneAccelerationExec, ScanDynamicFilter, ScanIdentity};

/// Optimizer rule that rewrites `HashJoinExec` nodes to use `ExactLeftAccumulator`
/// when the probe side is a `CayenneAccelerationExec`.
#[derive(Default)]
pub struct CayenneJoinRewriter;

impl CayenneJoinRewriter {
    /// Create a new `CayenneJoinRewriter` optimizer rule.
    #[must_use]
    pub fn new() -> Self {
        Self
    }
}

/// Shares already-pushed hash-join dynamic filters between same-source Cayenne
/// scans when the current hash join proves the relevant columns are equi-joined.
#[derive(Default)]
pub struct CayenneDynamicFilterSharing;

impl CayenneDynamicFilterSharing {
    /// Create a new `CayenneDynamicFilterSharing` optimizer rule.
    #[must_use]
    pub fn new() -> Self {
        Self
    }
}

impl std::fmt::Debug for CayenneDynamicFilterSharing {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CayenneDynamicFilterSharing").finish()
    }
}

impl PhysicalOptimizerRule for CayenneDynamicFilterSharing {
    fn name(&self) -> &'static str {
        "CayenneDynamicFilterSharing"
    }

    fn schema_check(&self) -> bool {
        false
    }

    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        plan.transform_down(|node| {
            let Some(hash_join) = node.as_any().downcast_ref::<HashJoinExec>() else {
                return Ok(Transformed::no(node));
            };

            let (left_additions, right_additions) = filter_additions_for_join(hash_join);
            if left_additions.is_empty() && right_additions.is_empty() {
                return Ok(Transformed::no(node));
            }

            let (left, left_changed) =
                apply_filter_additions(Arc::clone(hash_join.left()), &left_additions, config)?;
            let (right, right_changed) =
                apply_filter_additions(Arc::clone(hash_join.right()), &right_additions, config)?;

            if !left_changed && !right_changed {
                return Ok(Transformed::no(node));
            }

            let new_node = node.with_new_children(vec![left, right])?;
            Ok(Transformed::yes(new_node))
        })
        .data()
    }
}

#[derive(Clone)]
struct CayenneScanSummary {
    identity: ScanIdentity,
    columns: BTreeSet<String>,
    dynamic_filters: Vec<ScanDynamicFilter>,
}

#[derive(Clone)]
struct FilterAddition {
    identity: ScanIdentity,
    filter: Arc<dyn PhysicalExpr>,
}

fn filter_additions_for_join(
    hash_join: &HashJoinExec,
) -> (Vec<FilterAddition>, Vec<FilterAddition>) {
    let left_scans = collect_cayenne_scans(hash_join.left());
    let right_scans = collect_cayenne_scans(hash_join.right());
    if left_scans.is_empty() || right_scans.is_empty() {
        return (Vec::new(), Vec::new());
    }

    let mut pair_columns: HashMap<(usize, usize), BTreeSet<String>> = HashMap::new();
    for (left_key, right_key) in hash_join.on() {
        let Some(left_column) = physical_column_name(left_key) else {
            continue;
        };
        let Some(right_column) = physical_column_name(right_key) else {
            continue;
        };

        if left_column != right_column {
            continue;
        }

        let matching_pairs =
            same_source_pairs_for_column(&left_scans, &right_scans, left_column, right_column);
        let [(left_index, right_index)] = matching_pairs.as_slice() else {
            continue;
        };

        pair_columns
            .entry((*left_index, *right_index))
            .or_default()
            .insert(left_column.to_string());
    }

    let mut left_additions = Vec::new();
    let mut right_additions = Vec::new();

    for ((left_index, right_index), shared_columns) in pair_columns {
        let left_scan = &left_scans[left_index];
        let right_scan = &right_scans[right_index];

        for filter in &left_scan.dynamic_filters {
            if filter.columns().is_subset(&shared_columns) {
                push_filter_addition(
                    &mut right_additions,
                    right_scan.identity.clone(),
                    Arc::clone(filter.filter()),
                );
            }
        }

        for filter in &right_scan.dynamic_filters {
            if filter.columns().is_subset(&shared_columns) {
                push_filter_addition(
                    &mut left_additions,
                    left_scan.identity.clone(),
                    Arc::clone(filter.filter()),
                );
            }
        }
    }

    (left_additions, right_additions)
}

fn collect_cayenne_scans(plan: &Arc<dyn ExecutionPlan>) -> Vec<CayenneScanSummary> {
    let mut scans = Vec::new();
    collect_cayenne_scans_inner(plan, &mut scans);
    scans
}

fn collect_cayenne_scans_inner(plan: &Arc<dyn ExecutionPlan>, scans: &mut Vec<CayenneScanSummary>) {
    if let Some(cayenne) = plan.as_any().downcast_ref::<CayenneAccelerationExec>()
        && let Some(identity) = cayenne.scan_identity()
    {
        let columns = cayenne
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().to_string())
            .collect();
        scans.push(CayenneScanSummary {
            identity,
            columns,
            dynamic_filters: cayenne.dynamic_filters(),
        });
        return;
    }

    for child in plan.children() {
        collect_cayenne_scans_inner(child, scans);
    }
}

fn physical_column_name(expr: &Arc<dyn PhysicalExpr>) -> Option<&str> {
    expr.as_any().downcast_ref::<Column>().map(Column::name)
}

fn same_source_pairs_for_column(
    left_scans: &[CayenneScanSummary],
    right_scans: &[CayenneScanSummary],
    left_column: &str,
    right_column: &str,
) -> Vec<(usize, usize)> {
    let mut pairs = Vec::new();

    for (left_index, left_scan) in left_scans.iter().enumerate() {
        if !left_scan.columns.contains(left_column) {
            continue;
        }

        for (right_index, right_scan) in right_scans.iter().enumerate() {
            if left_scan.identity == right_scan.identity
                && right_scan.columns.contains(right_column)
            {
                pairs.push((left_index, right_index));
            }
        }
    }

    pairs
}

fn push_filter_addition(
    additions: &mut Vec<FilterAddition>,
    identity: ScanIdentity,
    filter: Arc<dyn PhysicalExpr>,
) {
    if additions
        .iter()
        .any(|addition| addition.identity == identity && Arc::ptr_eq(&addition.filter, &filter))
    {
        return;
    }

    additions.push(FilterAddition { identity, filter });
}

fn apply_filter_additions(
    plan: Arc<dyn ExecutionPlan>,
    additions: &[FilterAddition],
    config: &ConfigOptions,
) -> Result<(Arc<dyn ExecutionPlan>, bool), DataFusionError> {
    if additions.is_empty() {
        return Ok((plan, false));
    }

    if let Some(cayenne) = plan.as_any().downcast_ref::<CayenneAccelerationExec>() {
        let Some(identity) = cayenne.scan_identity() else {
            return Ok((plan, false));
        };
        let filters = additions
            .iter()
            .filter(|addition| addition.identity == identity)
            .filter(|addition| !cayenne.has_dynamic_filter(&addition.filter))
            .map(|addition| Arc::clone(&addition.filter))
            .collect::<Vec<_>>();

        let Some(new_plan) = cayenne.with_additional_dynamic_filters(&filters, config)? else {
            return Ok((plan, false));
        };

        return Ok((new_plan, true));
    }

    let children = plan
        .children()
        .into_iter()
        .map(Arc::clone)
        .collect::<Vec<_>>();
    if children.is_empty() {
        return Ok((plan, false));
    }

    let mut changed = false;
    let mut new_children = Vec::with_capacity(children.len());
    for child in children {
        let (new_child, child_changed) = apply_filter_additions(child, additions, config)?;
        changed |= child_changed;
        new_children.push(new_child);
    }

    if !changed {
        return Ok((plan, false));
    }

    plan.with_new_children(new_children)
        .map(|plan| (plan, true))
}

impl std::fmt::Debug for CayenneJoinRewriter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CayenneJoinRewriter").finish()
    }
}

/// Flatten transparent nodes (like `ProjectionExec` that just pass through columns)
/// to find the underlying plan node.
fn flatten_transparent_nodes(plan: &Arc<dyn ExecutionPlan>) -> &Arc<dyn ExecutionPlan> {
    // ProjectionExec is transparent if it just passes through columns
    if let Some(projection) = plan.as_any().downcast_ref::<ProjectionExec>() {
        return flatten_transparent_nodes(projection.input());
    }

    if let Some(bytes_processed_exec) = plan.as_any().downcast_ref::<BytesProcessedExec>() {
        let children = bytes_processed_exec.children();
        let Some(input) = children.first() else {
            return plan;
        };

        return flatten_transparent_nodes(input);
    }

    if let Some(repartitioned) = plan.as_any().downcast_ref::<RepartitionExec>() {
        return flatten_transparent_nodes(repartitioned.input());
    }

    if let Some(coalesce) = plan.as_any().downcast_ref::<CoalesceBatchesExec>() {
        return flatten_transparent_nodes(coalesce.input());
    }

    if let Some(schema_cast_scan) = plan.as_any().downcast_ref::<SchemaCastScanExec>() {
        let children = schema_cast_scan.children();
        let Some(input) = children.first() else {
            return plan;
        };

        return flatten_transparent_nodes(input);
    }

    plan
}

fn hash_join_build_side_is_cayenne(join: &HashJoinExec) -> bool {
    let build_side = flatten_transparent_nodes(join.left());

    if build_side.is_cayenne_acceleration_exec() {
        true
    } else if let Some(nested_join) = build_side.as_any().downcast_ref::<HashJoinExec>() {
        // Recursively check the build side of the nested join
        hash_join_build_side_is_cayenne(nested_join)
    } else {
        false
    }
}

/// Check if the probe side of the first input `HashJoinExec` is either `CayenneAccelerationExec` or another `HashJoinExec`.
///
/// For nested hash joins, the build side of the join must also be a `CayenneAccelerationExec` as the dynamic filter from this `HashJoinExec` will push into the build side of the next join.
///
/// This handles nested join patterns like:
/// ```text
///      HashJoinExec (top)
///         | - DataSourceExec (build)
///         | - HashJoinExec (probe/nested)
///               | - DataSourceExec (build of nested)
///               | - DataSourceExec (probe of nested)
/// ```
fn is_cayenne_backed_join(hash_join: &HashJoinExec) -> bool {
    // Check the probe side first (right child)
    let probe_side = flatten_transparent_nodes(hash_join.right());

    if probe_side
        .as_any()
        .downcast_ref::<CayenneAccelerationExec>()
        .is_some()
    {
        return true;
    }

    // If probe side is another `HashJoinExec`, check the build side of the nested join is Cayenne
    if let Some(nested_join) = probe_side.as_any().downcast_ref::<HashJoinExec>() {
        // The nested join's build side must also be Cayenne
        return hash_join_build_side_is_cayenne(nested_join);
    }

    // Unknown node type on probe side - not Cayenne-backed
    false
}

impl PhysicalOptimizerRule for CayenneJoinRewriter {
    fn name(&self) -> &'static str {
        "CayenneJoinRewriter"
    }

    fn schema_check(&self) -> bool {
        false
    }

    fn optimize(
        &self,
        plan: std::sync::Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        // For each `HashJoinExec`, determine if probe side is a `CayenneAccelerationExec` with a Cayenne accelerator
        // If so, that `HashJoinExec` can be replaced with one which uses a `ExactLeftAccumulator` so we can push down exact dynamic filter bounds into Cayenne
        // The build side is irrelevant for the collection, as we only push the filter down to the probe side
        //
        // This can become more complex for plans like:
        //      `HashJoinExec`
        //         | - `CayenneAccelerationExec`
        //         | - `HashJoinExec`
        //               | - `CayenneAccelerationExec`
        //               | - `CayenneAccelerationExec`
        //
        // In this scenario, the "build side" is the very first `CayenneAccelerationExec` - the probe side becomes the remaining `HashJoinExec`, which includes the other 2 `CayenneAccelerationExec`s.
        // The dynamic filter from the top `CayenneAccelerationExec` will push down into the build side of the second `HashJoinExec`.
        // After that, the dynamic filter from the second `HashJoinExec` will push down into its probe side `CayenneAccelerationExec` - sourced from its own build-side dynamic filter.
        //
        // Therefore, after we encounter a `HashJoinExec` we need to continue traversing down the build side of any subsequent `HashJoinExec`s to ensure it is a `CayenneAccelerationExec`.

        plan.transform_down(|node| {
            let Some(hash_join) = node.as_any().downcast_ref::<HashJoinExec>() else {
                return Ok(Transformed::no(node));
            };

            if !is_cayenne_backed_join(hash_join) {
                return Ok(Transformed::no(node));
            }

            if hash_join.null_equality() != NullEquality::NullEqualsNothing {
                return Ok(Transformed::no(node));
            }

            tracing::debug!(
                "Replacing HashJoinExec with ExactLeftAccumulator for Cayenne acceleration"
            );

            let new_join = hash_join.recreate_with_accumulator::<ExactLeftAccumulator>();

            Ok(Transformed::yes(Arc::new(new_join)))
        })
        .data()
    }
}

#[cfg(test)]
mod tests {
    use super::{CayenneDynamicFilterSharing, CayenneJoinRewriter};
    use crate::provider::CayenneAccelerationExec;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::{JoinType, NullEquality};
    use datafusion::config::ConfigOptions;
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::execution::object_store::ObjectStoreUrl;
    use datafusion::physical_optimizer::PhysicalOptimizerRule;
    use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
    use datafusion::physical_plan::projection::ProjectionExec;
    use datafusion::physical_plan::{ExecutionPlan, displayable};
    use datafusion_common::{DataFusionError, Result as DFResult};
    use datafusion_datasource::file::FileSource;
    use datafusion_datasource::file_groups::FileGroup;
    use datafusion_datasource::file_scan_config::FileScanConfigBuilder;
    use datafusion_datasource::file_stream::FileOpener;
    use datafusion_datasource::source::DataSourceExec;
    use datafusion_datasource::{PartitionedFile, TableSchema};
    use datafusion_physical_expr::expressions::{DynamicFilterPhysicalExpr, col, lit};
    use datafusion_physical_expr::projection::ProjectionExprs;
    use datafusion_physical_expr::{PhysicalExpr, conjunction};
    use datafusion_physical_plan::DisplayFormatType;
    use datafusion_physical_plan::filter_pushdown::{FilterPushdownPropagation, PushedDown};
    use datafusion_physical_plan::metrics::ExecutionPlanMetricsSet;
    use object_store::ObjectMeta;
    use object_store::ObjectStore;
    use object_store::path::Path;
    use runtime_datafusion::join_accumulator::ExactLeftAccumulator;
    use std::any::Any;
    use std::sync::Arc;

    #[derive(Clone)]
    struct TestFileSource {
        table_schema: TableSchema,
        filter: Option<Arc<dyn PhysicalExpr>>,
        metrics: ExecutionPlanMetricsSet,
    }

    impl TestFileSource {
        fn new(table_schema: TableSchema, filter: Option<Arc<dyn PhysicalExpr>>) -> Self {
            Self {
                table_schema,
                filter,
                metrics: ExecutionPlanMetricsSet::new(),
            }
        }
    }

    impl FileSource for TestFileSource {
        fn create_file_opener(
            &self,
            _object_store: Arc<dyn ObjectStore>,
            _base_config: &datafusion_datasource::file_scan_config::FileScanConfig,
            _partition: usize,
        ) -> DFResult<Arc<dyn FileOpener>> {
            Err(DataFusionError::NotImplemented(
                "test source cannot open files".to_string(),
            ))
        }

        fn as_any(&self) -> &dyn Any {
            self
        }

        fn table_schema(&self) -> &TableSchema {
            &self.table_schema
        }

        fn with_batch_size(&self, _batch_size: usize) -> Arc<dyn FileSource> {
            Arc::new(self.clone())
        }

        fn filter(&self) -> Option<Arc<dyn PhysicalExpr>> {
            self.filter.clone()
        }

        fn projection(&self) -> Option<&ProjectionExprs> {
            None
        }

        fn metrics(&self) -> &ExecutionPlanMetricsSet {
            &self.metrics
        }

        fn file_type(&self) -> &str {
            "test"
        }

        fn fmt_extra(
            &self,
            _t: DisplayFormatType,
            _f: &mut std::fmt::Formatter<'_>,
        ) -> std::fmt::Result {
            Ok(())
        }

        fn try_pushdown_filters(
            &self,
            filters: Vec<Arc<dyn PhysicalExpr>>,
            _config: &ConfigOptions,
        ) -> DFResult<FilterPushdownPropagation<Arc<dyn FileSource>>> {
            let filter_count = filters.len();
            let filter = match &self.filter {
                Some(existing) => Some(conjunction(
                    std::iter::once(Arc::clone(existing)).chain(filters),
                )),
                None => Some(conjunction(filters)),
            };
            let source = Self {
                table_schema: self.table_schema.clone(),
                filter,
                metrics: ExecutionPlanMetricsSet::new(),
            };

            Ok(FilterPushdownPropagation::with_parent_pushdown_result(vec![
                PushedDown::Yes;
                filter_count
            ])
            .with_updated_node(Arc::new(source)))
        }
    }

    fn memory_exec(column_name: &str) -> Arc<dyn ExecutionPlan> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            column_name,
            DataType::Int32,
            false,
        )]));
        MemorySourceConfig::try_new_exec(&[vec![]], schema, None)
            .expect("memory exec should be valid")
    }

    fn file_exec(
        schema: Arc<Schema>,
        path: &str,
        filter: Option<Arc<dyn PhysicalExpr>>,
    ) -> Arc<dyn ExecutionPlan> {
        let table_schema = TableSchema::new(Arc::clone(&schema), Vec::new());
        let source = Arc::new(TestFileSource::new(table_schema, filter));
        let file = PartitionedFile::from(ObjectMeta {
            location: Path::from(path),
            last_modified: chrono::DateTime::UNIX_EPOCH,
            size: 1_024,
            e_tag: None,
            version: None,
        });
        let config = FileScanConfigBuilder::new(
            ObjectStoreUrl::parse("file:///").expect("object store url should parse"),
            source,
        )
        .with_file_group(FileGroup::new(vec![file]))
        .build();

        DataSourceExec::from_data_source(config) as Arc<dyn ExecutionPlan>
    }

    fn cayenne_file_exec(
        schema: Arc<Schema>,
        path: &str,
        filter: Option<Arc<dyn PhysicalExpr>>,
    ) -> Arc<dyn ExecutionPlan> {
        Arc::new(CayenneAccelerationExec::new(file_exec(
            schema, path, filter,
        )))
    }

    fn order_line_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("order_id", DataType::Int64, false),
            Field::new("warehouse_id", DataType::Int64, false),
            Field::new("line_number", DataType::Int64, false),
        ]))
    }

    fn dynamic_filter_for(column_name: &str, schema: &Schema) -> Arc<dyn PhysicalExpr> {
        Arc::new(DynamicFilterPhysicalExpr::new(
            vec![col(column_name, schema).expect("filter column should exist")],
            lit(true),
        ))
    }

    fn hash_join(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        left_column: &str,
        right_column: &str,
    ) -> HashJoinExec {
        hash_join_with_null_equality(
            left,
            right,
            left_column,
            right_column,
            NullEquality::NullEqualsNothing,
        )
    }

    fn hash_join_with_null_equality(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        left_column: &str,
        right_column: &str,
        null_equality: NullEquality,
    ) -> HashJoinExec {
        let left_key = col(left_column, &left.schema()).expect("left join key should exist");
        let right_key = col(right_column, &right.schema()).expect("right join key should exist");

        HashJoinExec::try_new(
            left,
            right,
            vec![(left_key, right_key)],
            None,
            &JoinType::Inner,
            None,
            PartitionMode::Partitioned,
            null_equality,
        )
        .expect("hash join should be valid")
    }

    fn join_with_right(right: Arc<dyn ExecutionPlan>) -> HashJoinExec {
        hash_join(memory_exec("left_id"), right, "left_id", "right_id")
    }

    fn optimize(plan: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        CayenneJoinRewriter::new()
            .optimize(plan, &ConfigOptions::default())
            .expect("optimizer should succeed")
    }

    fn optimize_filter_sharing(plan: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        CayenneDynamicFilterSharing::new()
            .optimize(plan, &ConfigOptions::default())
            .expect("filter sharing optimizer should succeed")
    }

    fn plan_snapshot(plan: &Arc<dyn ExecutionPlan>) -> String {
        displayable(plan.as_ref()).indent(true).to_string()
    }

    #[test]
    fn rewrites_hash_join_with_cayenne_probe_side() {
        let right = Arc::new(CayenneAccelerationExec::new(memory_exec("right_id")));
        let join = Arc::new(join_with_right(right));

        let optimized = optimize(join);

        assert!(
            optimized
                .as_any()
                .downcast_ref::<HashJoinExec<ExactLeftAccumulator>>()
                .is_some(),
            "Cayenne-backed joins should use ExactLeftAccumulator"
        );
    }

    #[test]
    fn leaves_hash_join_without_cayenne_probe_side_unchanged() {
        let right = memory_exec("right_id");
        let join = Arc::new(join_with_right(right));

        let optimized = optimize(join);

        assert!(
            optimized.as_any().downcast_ref::<HashJoinExec>().is_some(),
            "Non-Cayenne joins should keep the default accumulator"
        );
    }

    #[test]
    fn leaves_null_equal_hash_join_unchanged() {
        let left = memory_exec("left_id");
        let right = Arc::new(CayenneAccelerationExec::new(memory_exec("right_id")));
        let join = Arc::new(hash_join_with_null_equality(
            left,
            right,
            "left_id",
            "right_id",
            NullEquality::NullEqualsNull,
        ));

        let optimized = optimize(join);

        assert!(
            optimized.as_any().downcast_ref::<HashJoinExec>().is_some(),
            "Null-equal joins should keep the default accumulator to preserve probe NULL matches"
        );
    }

    #[test]
    fn rewrites_hash_join_through_transparent_projection() {
        let right_input = Arc::new(CayenneAccelerationExec::new(memory_exec("right_id")));
        let right_schema = right_input.schema();
        let right = Arc::new(
            ProjectionExec::try_new(
                vec![(
                    col("right_id", &right_schema).expect("projection column should exist"),
                    "right_id".to_string(),
                )],
                right_input,
            )
            .expect("projection should be valid"),
        );
        let join = Arc::new(join_with_right(right));

        let optimized = optimize(join);

        assert!(
            optimized
                .as_any()
                .downcast_ref::<HashJoinExec<ExactLeftAccumulator>>()
                .is_some(),
            "Transparent wrappers over Cayenne scans should still use ExactLeftAccumulator"
        );
    }

    #[test]
    fn rewrites_nested_cayenne_probe_join_chain() {
        let nested_left = Arc::new(CayenneAccelerationExec::new(memory_exec("nested_left_id")));
        let nested_right = Arc::new(CayenneAccelerationExec::new(memory_exec("nested_right_id")));
        let nested_join = Arc::new(hash_join(
            nested_left,
            nested_right,
            "nested_left_id",
            "nested_right_id",
        ));
        let top_join = Arc::new(hash_join(
            memory_exec("top_id"),
            nested_join,
            "top_id",
            "nested_left_id",
        ));

        let optimized = optimize(top_join);
        let snapshot = plan_snapshot(&optimized);

        assert_eq!(
            2,
            snapshot.matches("accumulator=ExactLeftAccumulator").count(),
            "The top join and nested Cayenne probe join should both use ExactLeftAccumulator"
        );
    }

    #[test]
    fn shares_dynamic_filter_across_same_source_equi_joined_cayenne_scans() {
        let schema = order_line_schema();
        let source_filter = dynamic_filter_for("order_id", &schema);
        let left = cayenne_file_exec(
            Arc::clone(&schema),
            "order_line.vortex",
            Some(Arc::clone(&source_filter)),
        );
        let right = cayenne_file_exec(Arc::clone(&schema), "order_line.vortex", None);
        let join = Arc::new(hash_join(left, right, "order_id", "order_id"));

        let optimized = optimize_filter_sharing(join);
        let join = optimized
            .as_any()
            .downcast_ref::<HashJoinExec>()
            .expect("optimized plan should remain a hash join");
        let right = join
            .right()
            .as_any()
            .downcast_ref::<CayenneAccelerationExec>()
            .expect("right side should remain Cayenne");
        let filters = right.dynamic_filters();

        assert_eq!(1, filters.len());
        assert!(Arc::ptr_eq(filters[0].filter(), &source_filter));
    }

    #[test]
    fn does_not_share_dynamic_filter_when_join_does_not_cover_filter_columns() {
        let schema = order_line_schema();
        let source_filter = dynamic_filter_for("line_number", &schema);
        let left = cayenne_file_exec(
            Arc::clone(&schema),
            "order_line.vortex",
            Some(Arc::clone(&source_filter)),
        );
        let right = cayenne_file_exec(Arc::clone(&schema), "order_line.vortex", None);
        let join = Arc::new(hash_join(left, right, "order_id", "order_id"));

        let optimized = optimize_filter_sharing(join);
        let join = optimized
            .as_any()
            .downcast_ref::<HashJoinExec>()
            .expect("optimized plan should remain a hash join");
        let right = join
            .right()
            .as_any()
            .downcast_ref::<CayenneAccelerationExec>()
            .expect("right side should remain Cayenne");

        assert!(right.dynamic_filters().is_empty());
    }

    #[test]
    fn snapshots_cayenne_probe_join_explain_plan() {
        let right = Arc::new(CayenneAccelerationExec::new(memory_exec("right_id")));
        let join = Arc::new(join_with_right(right));

        let optimized = optimize(join);

        insta::assert_snapshot!(
            "cayenne_probe_join_uses_exact_accumulator_explain",
            plan_snapshot(&optimized)
        );
    }

    #[test]
    fn snapshots_nested_cayenne_probe_join_explain_plan() {
        let nested_left = Arc::new(CayenneAccelerationExec::new(memory_exec("nested_left_id")));
        let nested_right = Arc::new(CayenneAccelerationExec::new(memory_exec("nested_right_id")));
        let nested_join = Arc::new(hash_join(
            nested_left,
            nested_right,
            "nested_left_id",
            "nested_right_id",
        ));
        let top_join = Arc::new(hash_join(
            memory_exec("top_id"),
            nested_join,
            "top_id",
            "nested_left_id",
        ));

        let optimized = optimize(top_join);

        insta::assert_snapshot!(
            "nested_cayenne_probe_join_uses_exact_accumulator_explain",
            plan_snapshot(&optimized)
        );
    }
}
