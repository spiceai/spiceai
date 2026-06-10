use super::{
    ANTI_JOIN_SORT_MERGE_MIN_EXACT_ROWS, CayenneAntiJoinSortMergeRewriter,
    CayenneDynamicFilterSharing, CayenneOptimizerConfig, FilterAddition, apply_filter_additions,
    plan_schema_fields,
};
use crate::provider::CayenneAccelerationExec;
use crate::provider::scan::ScanDynamicFilter;
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::{JoinType, NullEquality};
use datafusion::config::ConfigOptions;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode, SortMergeJoinExec};
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::union::UnionExec;
use datafusion::physical_plan::{ExecutionPlan, displayable};
use datafusion_common::stats::Precision;
use datafusion_common::{DataFusionError, Result as DFResult, Statistics};
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

    fn file_type(&self) -> &'static str {
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

fn file_exec(
    schema: &Arc<Schema>,
    path: &str,
    filter: Option<Arc<dyn PhysicalExpr>>,
) -> Arc<dyn ExecutionPlan> {
    file_exec_with_statistics(schema, path, filter, Statistics::new_unknown(schema))
}

fn file_exec_with_statistics(
    schema: &Arc<Schema>,
    path: &str,
    filter: Option<Arc<dyn PhysicalExpr>>,
    statistics: Statistics,
) -> Arc<dyn ExecutionPlan> {
    let table_schema = TableSchema::new(Arc::clone(schema), Vec::new());
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
    .with_statistics(statistics)
    .build();

    DataSourceExec::from_data_source(config) as Arc<dyn ExecutionPlan>
}

fn cayenne_file_exec(
    schema: &Arc<Schema>,
    path: &str,
    filter: Option<Arc<dyn PhysicalExpr>>,
) -> Arc<dyn ExecutionPlan> {
    Arc::new(CayenneAccelerationExec::new(file_exec(
        schema, path, filter,
    )))
}

fn inlined_exec(schema: &Arc<Schema>) -> Arc<dyn ExecutionPlan> {
    MemorySourceConfig::try_new_exec(&[vec![]], Arc::clone(schema), None)
        .expect("inlined memory exec should be valid")
}

fn cayenne_file_with_inlined_exec(
    schema: &Arc<Schema>,
    path: &str,
    filter: Option<Arc<dyn PhysicalExpr>>,
) -> Arc<dyn ExecutionPlan> {
    Arc::new(CayenneAccelerationExec::new(
        UnionExec::try_new(vec![file_exec(schema, path, filter), inlined_exec(schema)])
            .expect("mixed file and inlined union should be valid"),
    ))
}

fn cayenne_file_exec_with_num_rows(
    schema: &Arc<Schema>,
    path: &str,
    row_count: Precision<usize>,
) -> Arc<dyn ExecutionPlan> {
    Arc::new(CayenneAccelerationExec::new(file_exec_with_statistics(
        schema,
        path,
        None,
        Statistics::new_unknown(schema).with_num_rows(row_count),
    )))
}

fn large_exact_cayenne_file_exec(schema: &Arc<Schema>, path: &str) -> Arc<dyn ExecutionPlan> {
    cayenne_file_exec_with_num_rows(
        schema,
        path,
        Precision::Exact(ANTI_JOIN_SORT_MERGE_MIN_EXACT_ROWS + 1),
    )
}

fn order_line_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::Int64, false),
        Field::new("warehouse_id", DataType::Int64, false),
        Field::new("line_number", DataType::Int64, false),
    ]))
}

fn reordered_order_line_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("warehouse_id", DataType::Int64, false),
        Field::new("order_id", DataType::Int64, false),
        Field::new("line_number", DataType::Int64, false),
    ]))
}

fn order_line_schema_with_different_non_key_type() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("order_id", DataType::Int64, false),
        Field::new("warehouse_id", DataType::Int64, false),
        Field::new("line_number", DataType::UInt64, false),
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
    hash_join_with_join_type(
        left,
        right,
        left_column,
        right_column,
        JoinType::Inner,
        null_equality,
    )
}

fn hash_join_with_join_type(
    left: Arc<dyn ExecutionPlan>,
    right: Arc<dyn ExecutionPlan>,
    left_column: &str,
    right_column: &str,
    join_type: JoinType,
    null_equality: NullEquality,
) -> HashJoinExec {
    hash_join_with_join_type_on(
        left,
        right,
        &[(left_column, right_column)],
        join_type,
        null_equality,
    )
}

fn hash_join_with_join_type_on(
    left: Arc<dyn ExecutionPlan>,
    right: Arc<dyn ExecutionPlan>,
    columns: &[(&str, &str)],
    join_type: JoinType,
    null_equality: NullEquality,
) -> HashJoinExec {
    let on = columns
        .iter()
        .map(|(left_column, right_column)| {
            let left_key = col(left_column, &left.schema()).expect("left join key should exist");
            let right_key =
                col(right_column, &right.schema()).expect("right join key should exist");
            (left_key, right_key)
        })
        .collect();

    HashJoinExec::try_new(
        left,
        right,
        on,
        None,
        &join_type,
        None,
        PartitionMode::Partitioned,
        null_equality,
        false,
    )
    .expect("hash join should be valid")
}

fn optimize_filter_sharing(plan: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
    CayenneDynamicFilterSharing::new()
        .optimize(plan, &ConfigOptions::default())
        .expect("filter sharing optimizer should succeed")
}

fn optimize_anti_join_sort_merge(plan: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
    optimize_anti_join_sort_merge_with_config(plan, &ConfigOptions::default())
}

fn optimize_anti_join_sort_merge_with_config(
    plan: Arc<dyn ExecutionPlan>,
    config: &ConfigOptions,
) -> Arc<dyn ExecutionPlan> {
    CayenneAntiJoinSortMergeRewriter::new()
        .optimize(plan, config)
        .expect("anti join sort-merge optimizer should succeed")
}

fn config_with_cayenne_optimizer(
    sort_merge_min_rows: Option<usize>,
    sort_merge_memory_pool_fraction: Option<f64>,
    sort_merge_memory_pool_bytes: Option<usize>,
) -> ConfigOptions {
    let mut config = ConfigOptions::default();
    let mut cayenne_config = CayenneOptimizerConfig::default();
    if let Some(sort_merge_min_rows) = sort_merge_min_rows {
        cayenne_config.sort_merge_min_rows = sort_merge_min_rows;
    }
    if let Some(sort_merge_memory_pool_fraction) = sort_merge_memory_pool_fraction {
        cayenne_config.sort_merge_memory_pool_fraction = sort_merge_memory_pool_fraction;
    }
    cayenne_config.sort_merge_memory_pool_bytes = sort_merge_memory_pool_bytes;
    config.extensions.insert(cayenne_config);
    config
}

#[test]
fn shares_dynamic_filter_across_same_source_equi_joined_cayenne_scans() {
    let schema = order_line_schema();
    let source_filter = dynamic_filter_for("order_id", &schema);
    let left = cayenne_file_exec(
        &schema,
        "order_line.vortex",
        Some(Arc::clone(&source_filter)),
    );
    let right = cayenne_file_exec(&schema, "order_line.vortex", None);
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
fn shares_dynamic_filter_with_vortex_branch_of_mixed_inlined_scan() {
    let schema = order_line_schema();
    let source_filter = dynamic_filter_for("order_id", &schema);
    let left = cayenne_file_with_inlined_exec(
        &schema,
        "order_line.vortex",
        Some(Arc::clone(&source_filter)),
    );
    let right = cayenne_file_with_inlined_exec(&schema, "order_line.vortex", None);
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
        &schema,
        "order_line.vortex",
        Some(Arc::clone(&source_filter)),
    );
    let right = cayenne_file_exec(&schema, "order_line.vortex", None);
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
fn does_not_share_dynamic_filter_across_different_projection_order() {
    let left_schema = order_line_schema();
    let right_schema = reordered_order_line_schema();
    let source_filter = dynamic_filter_for("order_id", &left_schema);
    let left = cayenne_file_exec(
        &left_schema,
        "order_line.vortex",
        Some(Arc::clone(&source_filter)),
    );
    let right = cayenne_file_exec(&right_schema, "order_line.vortex", None);
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
fn does_not_share_dynamic_filter_across_different_schema_types() {
    let left_schema = order_line_schema();
    let right_schema = order_line_schema_with_different_non_key_type();
    let source_filter = dynamic_filter_for("order_id", &left_schema);
    let left = cayenne_file_exec(
        &left_schema,
        "order_line.vortex",
        Some(Arc::clone(&source_filter)),
    );
    let right = cayenne_file_exec(&right_schema, "order_line.vortex", None);
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
fn does_not_apply_filter_addition_to_same_identity_different_projection_order() {
    // `apply_filter_additions` must not push a filter into a scan whose
    // schema fields don't match the source scan exactly (different column
    // ordering / types means the filter's column-by-position indices
    // would refer to wrong columns).
    let source_schema = order_line_schema();
    let target_schema = reordered_order_line_schema();
    let source_filter = dynamic_filter_for("order_id", &source_schema);
    let source = CayenneAccelerationExec::new(file_exec(
        &source_schema,
        "order_line.vortex",
        Some(Arc::clone(&source_filter)),
    ));
    let addition = FilterAddition {
        identity: source
            .scan_identity()
            .expect("source scan should have file identity"),
        schema_fields: plan_schema_fields(&source.schema()),
        filter: Arc::clone(&source_filter),
    };
    let target = cayenne_file_exec(&target_schema, "order_line.vortex", None);

    let (optimized, changed) =
        apply_filter_additions(Arc::clone(&target), &[addition], &ConfigOptions::default())
            .expect("filter addition should be evaluated");

    assert!(!changed);
    let target = optimized
        .as_any()
        .downcast_ref::<CayenneAccelerationExec>()
        .expect("target should remain Cayenne");
    assert!(target.dynamic_filters().is_empty());
}

#[test]
fn does_not_share_dynamic_filter_for_anti_join() {
    // `*Anti` joins must not receive a shared dynamic filter: their
    // output requires the *absence* of a match, so filtering the probe
    // side would drop rows that the anti-join should preserve.
    let schema = order_line_schema();
    let source_filter = dynamic_filter_for("order_id", &schema);
    let left = cayenne_file_exec(
        &schema,
        "order_line.vortex",
        Some(Arc::clone(&source_filter)),
    );
    let right = cayenne_file_exec(&schema, "order_line.vortex", None);
    let join = Arc::new(hash_join_with_join_type(
        left,
        right,
        "order_id",
        "order_id",
        JoinType::RightAnti,
        NullEquality::NullEqualsNothing,
    ));

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
fn does_not_share_dynamic_filter_for_null_equal_inner_join() {
    let schema = order_line_schema();
    let source_filter = dynamic_filter_for("order_id", &schema);
    let left = cayenne_file_exec(
        &schema,
        "order_line.vortex",
        Some(Arc::clone(&source_filter)),
    );
    let right = cayenne_file_exec(&schema, "order_line.vortex", None);
    let join = Arc::new(hash_join_with_null_equality(
        left,
        right,
        "order_id",
        "order_id",
        NullEquality::NullEqualsNull,
    ));

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
fn shares_dynamic_filter_for_left_semi_join() {
    // `LeftSemi` preserves the equi-key domain: a dynamic filter built
    // from the left side is also valid on a same-source equi-joined
    // right scan, since the semi join's output is a subset of the left.
    let schema = order_line_schema();
    let source_filter = dynamic_filter_for("order_id", &schema);
    let left = cayenne_file_exec(
        &schema,
        "order_line.vortex",
        Some(Arc::clone(&source_filter)),
    );
    let right = cayenne_file_exec(&schema, "order_line.vortex", None);
    let join = Arc::new(hash_join_with_join_type(
        left,
        right,
        "order_id",
        "order_id",
        JoinType::LeftSemi,
        NullEquality::NullEqualsNothing,
    ));

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

    assert_eq!(
        1,
        filters.len(),
        "semi join should propagate same-source filter"
    );
    assert!(Arc::ptr_eq(filters[0].filter(), &source_filter));
}

#[test]
fn rewrites_same_source_left_semi_hash_join_to_sort_merge() {
    // Same memory concern as `LeftAnti`: `HashJoinExec` materializes the
    // LEFT input as a non-spillable hash table, and a large same-source
    // semi-join build side risks OOM.
    let schema = order_line_schema();
    let left = large_exact_cayenne_file_exec(&schema, "order_line.vortex");
    let right = large_exact_cayenne_file_exec(&schema, "order_line.vortex");
    let join = Arc::new(hash_join_with_join_type(
        left,
        right,
        "order_id",
        "order_id",
        JoinType::LeftSemi,
        NullEquality::NullEqualsNothing,
    ));

    let optimized = optimize_anti_join_sort_merge(join);
    assert!(
        optimized
            .as_any()
            .downcast_ref::<SortMergeJoinExec>()
            .is_some(),
        "same-source Cayenne LeftSemi join should use sort-merge join"
    );
}

#[test]
fn rewrites_same_source_left_anti_hash_join_to_sort_merge() {
    let schema = order_line_schema();
    let left = large_exact_cayenne_file_exec(&schema, "order_line.vortex");
    let right = large_exact_cayenne_file_exec(&schema, "order_line.vortex");
    let join = Arc::new(hash_join_with_join_type(
        left,
        right,
        "order_id",
        "order_id",
        JoinType::LeftAnti,
        NullEquality::NullEqualsNothing,
    ));

    let optimized = optimize_anti_join_sort_merge(join);
    let sort_merge = optimized
        .as_any()
        .downcast_ref::<SortMergeJoinExec>()
        .expect("same-source Cayenne anti join should use sort-merge join");

    assert_eq!(JoinType::LeftAnti, sort_merge.join_type());
    assert!(
        sort_merge
            .left()
            .as_any()
            .downcast_ref::<SortExec>()
            .is_some(),
        "left anti-join input should be explicitly sorted"
    );
    assert!(
        sort_merge
            .right()
            .as_any()
            .downcast_ref::<SortExec>()
            .is_some(),
        "right anti-join input should be explicitly sorted"
    );
}

#[test]
fn leaves_same_source_inner_hash_join_unchanged_even_when_build_side_is_large() {
    let schema = order_line_schema();
    let left = large_exact_cayenne_file_exec(&schema, "order_line.vortex");
    let right = large_exact_cayenne_file_exec(&schema, "order_line.vortex");
    let join = Arc::new(hash_join_with_join_type(
        left,
        right,
        "order_id",
        "order_id",
        JoinType::Inner,
        NullEquality::NullEqualsNothing,
    ));

    let optimized = optimize_anti_join_sort_merge(join);

    assert!(
        optimized.as_any().downcast_ref::<HashJoinExec>().is_some(),
        "inner joins should stay as hash joins unless a more targeted rule proves a win"
    );
}

#[test]
fn leaves_same_source_left_hash_join_unchanged_even_when_build_side_is_large() {
    let schema = order_line_schema();
    let left = large_exact_cayenne_file_exec(&schema, "order_line.vortex");
    let right = large_exact_cayenne_file_exec(&schema, "order_line.vortex");
    let join = Arc::new(hash_join_with_join_type(
        left,
        right,
        "order_id",
        "order_id",
        JoinType::Left,
        NullEquality::NullEqualsNothing,
    ));

    let optimized = optimize_anti_join_sort_merge(join);

    assert!(
        optimized.as_any().downcast_ref::<HashJoinExec>().is_some(),
        "outer joins should stay as hash joins unless a more targeted rule proves a win"
    );
}

#[test]
fn rewrites_same_source_multi_key_left_anti_hash_join_to_sort_merge() {
    let schema = order_line_schema();
    let left = large_exact_cayenne_file_exec(&schema, "order_line.vortex");
    let right = large_exact_cayenne_file_exec(&schema, "order_line.vortex");
    let join = Arc::new(hash_join_with_join_type_on(
        left,
        right,
        &[("order_id", "order_id"), ("warehouse_id", "warehouse_id")],
        JoinType::LeftAnti,
        NullEquality::NullEqualsNothing,
    ));

    let optimized = optimize_anti_join_sort_merge(join);

    assert!(
        optimized
            .as_any()
            .downcast_ref::<SortMergeJoinExec>()
            .is_some(),
        "multi-key same-source Cayenne anti join should use sort-merge join"
    );
}

#[test]
fn leaves_unrelated_left_anti_hash_join_unchanged() {
    let schema = order_line_schema();
    let left = large_exact_cayenne_file_exec(&schema, "order_line.vortex");
    let right = large_exact_cayenne_file_exec(&schema, "other_order_line.vortex");
    let join = Arc::new(hash_join_with_join_type(
        left,
        right,
        "order_id",
        "order_id",
        JoinType::LeftAnti,
        NullEquality::NullEqualsNothing,
    ));

    let optimized = optimize_anti_join_sort_merge(join);

    assert!(
        optimized.as_any().downcast_ref::<HashJoinExec>().is_some(),
        "anti joins over unrelated sources should stay as hash joins"
    );
}

#[test]
fn leaves_unrelated_inner_hash_join_unchanged() {
    let schema = order_line_schema();
    let left = large_exact_cayenne_file_exec(&schema, "order_line.vortex");
    let right = large_exact_cayenne_file_exec(&schema, "other_order_line.vortex");
    let join = Arc::new(hash_join_with_join_type(
        left,
        right,
        "order_id",
        "order_id",
        JoinType::Inner,
        NullEquality::NullEqualsNothing,
    ));

    let optimized = optimize_anti_join_sort_merge(join);

    assert!(
        optimized.as_any().downcast_ref::<HashJoinExec>().is_some(),
        "inner joins over unrelated sources should stay as hash joins"
    );
}

#[test]
fn leaves_exact_small_same_source_left_anti_hash_join_unchanged() {
    let schema = order_line_schema();
    let left = cayenne_file_exec_with_num_rows(
        &schema,
        "order_line.vortex",
        Precision::Exact(ANTI_JOIN_SORT_MERGE_MIN_EXACT_ROWS),
    );
    let right = large_exact_cayenne_file_exec(&schema, "order_line.vortex");
    let join = Arc::new(hash_join_with_join_type(
        left,
        right,
        "order_id",
        "order_id",
        JoinType::LeftAnti,
        NullEquality::NullEqualsNothing,
    ));

    let optimized = optimize_anti_join_sort_merge(join);

    assert!(
        optimized.as_any().downcast_ref::<HashJoinExec>().is_some(),
        "same-source anti joins at or below the large-input threshold should stay as hash joins"
    );
}

#[test]
fn leaves_null_equal_same_source_left_anti_hash_join_unchanged() {
    let schema = order_line_schema();
    let left = large_exact_cayenne_file_exec(&schema, "order_line.vortex");
    let right = large_exact_cayenne_file_exec(&schema, "order_line.vortex");
    let join = Arc::new(hash_join_with_join_type(
        left,
        right,
        "order_id",
        "order_id",
        JoinType::LeftAnti,
        NullEquality::NullEqualsNull,
    ));

    let optimized = optimize_anti_join_sort_merge(join);

    assert!(
        optimized.as_any().downcast_ref::<HashJoinExec>().is_some(),
        "null-equal anti joins should stay as hash joins"
    );
}

#[test]
fn leaves_same_source_left_anti_hash_join_when_configured_min_rows_is_higher() {
    let schema = order_line_schema();
    let left = large_exact_cayenne_file_exec(&schema, "order_line.vortex");
    let right = large_exact_cayenne_file_exec(&schema, "order_line.vortex");
    let join = Arc::new(hash_join_with_join_type(
        left,
        right,
        "order_id",
        "order_id",
        JoinType::LeftAnti,
        NullEquality::NullEqualsNothing,
    ));
    let config =
        config_with_cayenne_optimizer(Some(ANTI_JOIN_SORT_MERGE_MIN_EXACT_ROWS + 2), None, None);

    let optimized = optimize_anti_join_sort_merge_with_config(join, &config);

    assert!(
        optimized.as_any().downcast_ref::<HashJoinExec>().is_some(),
        "configured min-row threshold should keep smaller build sides as hash joins"
    );
}

/// Wide-build regression: low row count but wide projection produces a
/// build big enough to OOM `HashJoinExec`'s non-spillable hash table. The byte gate
/// must catch this case even though the row count is below
/// `sort_merge_min_rows`.
#[test]
fn rewrites_low_row_count_wide_build_when_byte_estimate_exceeds_memory_gate() {
    let schema = order_line_schema();
    // 200K rows × ~24 bytes/row ≈ 4.8 MB, plus inflated overhead from
    // `build_side_memory_estimate`'s hash-table overhead factor. Well below
    // the 10M row threshold but well above the 64 KB byte gate below.
    let small_rows = Precision::Exact(200_000);
    let left = cayenne_file_exec_with_num_rows(&schema, "order_line.vortex", small_rows);
    let right = cayenne_file_exec_with_num_rows(&schema, "order_line.vortex", small_rows);
    let join = Arc::new(hash_join_with_join_type(
        left,
        right,
        "order_id",
        "order_id",
        JoinType::LeftAnti,
        NullEquality::NullEqualsNothing,
    ));
    // 64 KB pool × 0.125 = 8 KB byte gate. The 200K-row build is enormously
    // above that, so the rewrite should fire despite row count below 10M.
    let config = config_with_cayenne_optimizer(None, Some(0.125), Some(64 * 1024));

    let optimized = optimize_anti_join_sort_merge_with_config(join, &config);

    assert!(
        optimized
            .as_any()
            .downcast_ref::<SortMergeJoinExec>()
            .is_some(),
        "low-row-count + wide-row build exceeding the byte gate should be rewritten to sort-merge"
    );
}

#[test]
fn leaves_same_source_left_anti_hash_join_when_build_estimate_fits_memory_gate() {
    let schema = order_line_schema();
    let left = large_exact_cayenne_file_exec(&schema, "order_line.vortex");
    let right = large_exact_cayenne_file_exec(&schema, "order_line.vortex");
    let join = Arc::new(hash_join_with_join_type(
        left,
        right,
        "order_id",
        "order_id",
        JoinType::LeftAnti,
        NullEquality::NullEqualsNothing,
    ));
    let config = config_with_cayenne_optimizer(None, Some(0.125), Some(4 * 1024 * 1024 * 1024));

    let optimized = optimize_anti_join_sort_merge_with_config(join, &config);

    assert!(
        optimized.as_any().downcast_ref::<HashJoinExec>().is_some(),
        "estimated build side within the configured memory fraction should stay a hash join"
    );
}

#[test]
fn rewrites_same_source_left_anti_hash_join_when_build_estimate_exceeds_memory_gate() {
    let schema = order_line_schema();
    let left = large_exact_cayenne_file_exec(&schema, "order_line.vortex");
    let right = large_exact_cayenne_file_exec(&schema, "order_line.vortex");
    let join = Arc::new(hash_join_with_join_type(
        left,
        right,
        "order_id",
        "order_id",
        JoinType::LeftAnti,
        NullEquality::NullEqualsNothing,
    ));
    let config = config_with_cayenne_optimizer(None, Some(0.125), Some(64 * 1024 * 1024));

    let optimized = optimize_anti_join_sort_merge_with_config(join, &config);

    assert!(
        optimized
            .as_any()
            .downcast_ref::<SortMergeJoinExec>()
            .is_some(),
        "estimated build side above the configured memory fraction should use sort-merge"
    );
}

#[test]
fn leaves_inexact_same_source_left_anti_hash_join_unchanged() {
    let schema = order_line_schema();
    let left = cayenne_file_exec_with_num_rows(
        &schema,
        "order_line.vortex",
        Precision::Inexact(ANTI_JOIN_SORT_MERGE_MIN_EXACT_ROWS + 1),
    );
    let right = large_exact_cayenne_file_exec(&schema, "order_line.vortex");
    let join = Arc::new(hash_join_with_join_type(
        left,
        right,
        "order_id",
        "order_id",
        JoinType::LeftAnti,
        NullEquality::NullEqualsNothing,
    ));

    let optimized = optimize_anti_join_sort_merge(join);

    assert!(
        optimized.as_any().downcast_ref::<HashJoinExec>().is_some(),
        "same-source anti joins with inexact preserved-side stats should stay as hash joins"
    );
}

#[test]
fn leaves_unknown_same_source_left_anti_hash_join_unchanged() {
    let schema = order_line_schema();
    let left = cayenne_file_exec(&schema, "order_line.vortex", None);
    let right = large_exact_cayenne_file_exec(&schema, "order_line.vortex");
    let join = Arc::new(hash_join_with_join_type(
        left,
        right,
        "order_id",
        "order_id",
        JoinType::LeftAnti,
        NullEquality::NullEqualsNothing,
    ));

    let optimized = optimize_anti_join_sort_merge(join);

    assert!(
        optimized.as_any().downcast_ref::<HashJoinExec>().is_some(),
        "same-source anti joins with unknown preserved-side stats should stay as hash joins"
    );
}

#[test]
fn rewrites_right_anti_hash_join_when_build_side_stats_are_exact_large() {
    let schema = order_line_schema();
    let left = large_exact_cayenne_file_exec(&schema, "order_line.vortex");
    let right = cayenne_file_exec(&schema, "order_line.vortex", None);
    let join = Arc::new(hash_join_with_join_type(
        left,
        right,
        "order_id",
        "order_id",
        JoinType::RightAnti,
        NullEquality::NullEqualsNothing,
    ));

    let optimized = optimize_anti_join_sort_merge(join);

    assert!(
        optimized
            .as_any()
            .downcast_ref::<SortMergeJoinExec>()
            .is_some(),
        "RightAnti should gate on the left build side, not the right preserved side"
    );
}

#[test]
fn leaves_right_anti_hash_join_when_build_side_stats_are_unknown() {
    let schema = order_line_schema();
    let left = cayenne_file_exec(&schema, "order_line.vortex", None);
    let right = large_exact_cayenne_file_exec(&schema, "order_line.vortex");
    let join = Arc::new(hash_join_with_join_type(
        left,
        right,
        "order_id",
        "order_id",
        JoinType::RightAnti,
        NullEquality::NullEqualsNothing,
    ));

    let optimized = optimize_anti_join_sort_merge(join);

    assert!(
        optimized.as_any().downcast_ref::<HashJoinExec>().is_some(),
        "RightAnti should stay hash join when the left build side has unknown stats"
    );
}

/// Recursively find the first `CayenneAccelerationExec` in a plan tree.
/// Collects the dynamic filters attached to the first `CayenneAccelerationExec`
/// found anywhere in `plan` (depth-first). Returns an empty vec if there is no
/// Cayenne scan or it carries no dynamic filters.
fn cayenne_scan_dynamic_filters(plan: &Arc<dyn ExecutionPlan>) -> Vec<ScanDynamicFilter> {
    if let Some(cayenne) = plan.as_any().downcast_ref::<CayenneAccelerationExec>() {
        return cayenne.dynamic_filters();
    }
    for child in plan.children() {
        let filters = cayenne_scan_dynamic_filters(child);
        if !filters.is_empty()
            || child
                .as_any()
                .downcast_ref::<CayenneAccelerationExec>()
                .is_some()
        {
            return filters;
        }
    }
    Vec::new()
}

/// Runs `DataFusion`'s Post-phase physical filter pushdown — the phase that
/// plants hash-join dynamic filters into probe-side scans.
fn push_down_filters(
    plan: Arc<dyn ExecutionPlan>,
    config: &ConfigOptions,
) -> Arc<dyn ExecutionPlan> {
    use datafusion::physical_optimizer::filter_pushdown::FilterPushdown;
    FilterPushdown::new_post_optimization()
        .optimize(plan, config)
        .expect("post-optimization filter pushdown should succeed")
}

/// Regression test for the DF53 *native* hash-join dynamic-filter pushdown
/// that replaced the forked `ExactLeftAccumulator` accumulator seam.
///
/// For an inner join whose probe (right) side is a `CayenneAccelerationExec`,
/// `DataFusion`'s Post-phase `FilterPushdown` must plant a
/// `DynamicFilterPhysicalExpr` into the Cayenne scan's underlying file
/// source. This is the effect the old `CayenneJoinRewriter` rule used to
/// secure via a custom accumulator; the native path now provides it (with a
/// min/max bounds + InList/hash-table membership filter) with no
/// Cayenne-specific physical rule.
#[test]
fn native_inner_join_plants_dynamic_filter_into_cayenne_probe_scan() {
    let schema = order_line_schema();
    let build = file_exec(&schema, "build.vortex", None);
    let probe = cayenne_file_exec(&schema, "probe.vortex", None);
    let join: Arc<dyn ExecutionPlan> = Arc::new(hash_join(build, probe, "order_id", "order_id"));

    // Default config keeps `optimizer.enable_join_dynamic_filter_pushdown`
    // (and the umbrella `enable_dynamic_filter_pushdown`) enabled.
    let config = ConfigOptions::default();
    assert!(
        config.optimizer.enable_join_dynamic_filter_pushdown,
        "native join dynamic-filter pushdown is expected to default on"
    );

    let optimized = push_down_filters(join, &config);

    let filters = cayenne_scan_dynamic_filters(&optimized);

    assert!(
        !filters.is_empty(),
        "DataFusion's native inner-join dynamic filter should reach the Cayenne probe scan; \
             got plan: {}",
        displayable(optimized.as_ref()).indent(true)
    );
    assert!(
        filters.iter().any(|f| f.columns().contains("order_id")),
        "the planted dynamic filter should reference the equi-join key column"
    );
}

/// Companion to the inner-join regression: `DataFusion` only pushes
/// join-derived dynamic filters through **inner** joins
/// (`HashJoinExec::allow_join_dynamic_filter_pushdown`). A semi join must
/// therefore *not* plant a dynamic filter into the Cayenne scan — the OOM
/// mitigation for semi-join shapes (e.g. CH-benCH Q18) is handled by the
/// separate logical `CayennePushDownSemiJoin` rule and by
/// `CayenneAntiJoinSortMergeRewriter`, not by this pushdown path.
#[test]
fn native_semi_join_does_not_plant_dynamic_filter_into_cayenne_probe_scan() {
    let schema = order_line_schema();
    let build = file_exec(&schema, "build.vortex", None);
    let probe = cayenne_file_exec(&schema, "probe.vortex", None);
    let join: Arc<dyn ExecutionPlan> = Arc::new(hash_join_with_join_type(
        build,
        probe,
        "order_id",
        "order_id",
        JoinType::RightSemi,
        NullEquality::NullEqualsNothing,
    ));

    let optimized = push_down_filters(join, &ConfigOptions::default());

    assert!(
        cayenne_scan_dynamic_filters(&optimized).is_empty(),
        "DataFusion does not push join dynamic filters through semi joins"
    );
}
