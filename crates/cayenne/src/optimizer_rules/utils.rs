//! Shared helpers for the Cayenne physical optimizer rules: discovery of
//! `CayenneAccelerationExec` scans and grouping by `ScanIdentity`
//! ([`CayenneScanSummary`]), per-type Arrow width estimation backing the
//! build-side memory gates, exact build-side row statistics, flattening of
//! transparent wrapper nodes above scans, and the [`CayenneOptimizerConfig`]
//! accessor. No optimizer rule lives in this file.

use super::{
    Arc, BTreeSet, BytesProcessedExec, CayenneAccelerationExec, CayenneOptimizerConfig, Column,
    ConfigOptions, DataType, ExecutionPlan, HashJoinExec, HashMap, Int64PkDeletionFilterExec,
    IntervalUnit, KeyBasedDeletionFilterExec, PhysicalExpr, PhysicalOptimizerRule, Precision,
    ProjectionExec, RepartitionExec, ScanDynamicFilter, ScanIdentity, SchemaCastScanExec,
    SchemaRef,
};

#[derive(Clone)]
pub(super) struct CayenneScanSummary {
    pub(super) identity: Arc<ScanIdentity>,
    pub(super) columns: BTreeSet<String>,
    pub(super) schema_fields: Vec<(String, DataType)>,
    pub(super) dynamic_filters: Vec<ScanDynamicFilter>,
}

pub(super) fn cayenne_optimizer_config(config: &ConfigOptions) -> CayenneOptimizerConfig {
    config
        .extensions
        .get::<CayenneOptimizerConfig>()
        .cloned()
        .unwrap_or_default()
}

pub(super) fn estimated_arrow_width(data_type: &DataType) -> Option<usize> {
    match data_type {
        DataType::Null => Some(0),
        DataType::Boolean | DataType::Int8 | DataType::UInt8 => Some(1),
        DataType::Int16 | DataType::UInt16 | DataType::Float16 => Some(2),
        DataType::Int32
        | DataType::UInt32
        | DataType::Float32
        | DataType::Date32
        | DataType::Time32(_)
        | DataType::Interval(IntervalUnit::YearMonth)
        | DataType::Decimal32(_, _) => Some(4),
        DataType::Int64
        | DataType::UInt64
        | DataType::Float64
        | DataType::Timestamp(_, _)
        | DataType::Date64
        | DataType::Time64(_)
        | DataType::Duration(_)
        | DataType::Decimal64(_, _)
        | DataType::Interval(IntervalUnit::DayTime) => Some(8),
        DataType::Interval(IntervalUnit::MonthDayNano) | DataType::Decimal128(_, _) => Some(16),
        DataType::Decimal256(_, _) => Some(32),
        DataType::FixedSizeBinary(size) => usize::try_from(*size).ok(),
        DataType::Dictionary(_, value_type) => estimated_arrow_width(value_type)
            .map(|width| width.saturating_add(std::mem::size_of::<u64>())),
        DataType::FixedSizeList(field, length) => {
            let length = usize::try_from(*length).ok()?;
            estimated_arrow_width(field.data_type()).map(|width| width.saturating_mul(length))
        }
        DataType::Struct(fields) => fields.iter().try_fold(0_usize, |acc, field| {
            Some(acc.saturating_add(estimated_arrow_width(field.data_type())?))
        }),
        DataType::RunEndEncoded(_, value_field) => estimated_arrow_width(value_field.data_type())
            .map(|width| width.saturating_add(std::mem::size_of::<u64>())),
        DataType::Binary
        | DataType::LargeBinary
        | DataType::BinaryView
        | DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Utf8View
        | DataType::List(_)
        | DataType::ListView(_)
        | DataType::LargeList(_)
        | DataType::LargeListView(_)
        | DataType::Map(_, _)
        | DataType::Union(_, _) => Some(64),
    }
}

pub(super) fn spillable_rewrite_build_input_exact_rows(hash_join: &HashJoinExec) -> Option<usize> {
    // `HashJoinExec` materializes the LEFT input as the (non-spillable) build
    // hash table regardless of join type.
    let build_input = hash_join.left();

    match build_input.partition_statistics(None).ok()?.num_rows {
        Precision::Exact(row_count) => Some(row_count),
        Precision::Inexact(_) | Precision::Absent => None,
    }
}

pub(super) fn collect_cayenne_scans(plan: &Arc<dyn ExecutionPlan>) -> Vec<CayenneScanSummary> {
    let mut scans = Vec::new();
    collect_cayenne_scans_inner(plan, &mut scans);
    scans
}

pub(super) fn collect_cayenne_scans_inner(
    plan: &Arc<dyn ExecutionPlan>,
    scans: &mut Vec<CayenneScanSummary>,
) {
    if let Some(cayenne) = plan.as_any().downcast_ref::<CayenneAccelerationExec>()
        && let Some(identity) = cayenne.scan_identity()
    {
        let schema_fields = plan_schema_fields(&cayenne.schema());
        let columns = schema_fields.iter().map(|(name, _)| name.clone()).collect();
        scans.push(CayenneScanSummary {
            identity,
            columns,
            schema_fields,
            dynamic_filters: cayenne.dynamic_filters(),
        });
        return;
    }

    for child in plan.children() {
        collect_cayenne_scans_inner(child, scans);
    }
}

pub(super) fn physical_column_name(expr: &Arc<dyn PhysicalExpr>) -> Option<&str> {
    expr.as_any().downcast_ref::<Column>().map(Column::name)
}

pub(super) fn scans_by_identity(
    scans: &[CayenneScanSummary],
) -> HashMap<Arc<ScanIdentity>, Vec<usize>> {
    let mut by_identity: HashMap<Arc<ScanIdentity>, Vec<usize>> = HashMap::new();
    for (index, scan) in scans.iter().enumerate() {
        by_identity
            .entry(Arc::clone(&scan.identity))
            .or_default()
            .push(index);
    }
    by_identity
}

pub(super) fn same_source_pairs_for_column(
    left_scans: &[CayenneScanSummary],
    right_scans: &[CayenneScanSummary],
    right_scans_by_identity: &HashMap<Arc<ScanIdentity>, Vec<usize>>,
    left_column: &str,
    right_column: &str,
) -> Vec<(usize, usize)> {
    let mut pairs = Vec::new();

    for (left_index, left_scan) in left_scans.iter().enumerate() {
        if !left_scan.columns.contains(left_column) {
            continue;
        }

        let Some(right_indices) = right_scans_by_identity.get(&left_scan.identity) else {
            continue;
        };

        for &right_index in right_indices {
            if right_scans[right_index].columns.contains(right_column) {
                pairs.push((left_index, right_index));
            }
        }
    }

    pairs
}

pub(super) fn plan_schema_fields(schema: &SchemaRef) -> Vec<(String, DataType)> {
    schema
        .fields()
        .iter()
        .map(|field| (field.name().clone(), field.data_type().clone()))
        .collect()
}

/// Flatten transparent nodes (like `ProjectionExec` that just pass through columns)
/// to find the underlying plan node.
// `CoalesceBatchesExec` is deprecated in DF53 (superseded by arrow-rs
// `BatchCoalescer`) but the physical planner still emits it, so we keep seeing
// through it here — mirrors `provider::scan::is_identity_preserving_wrapper`.
#[expect(deprecated)]
pub(super) fn flatten_transparent_nodes(plan: &Arc<dyn ExecutionPlan>) -> &Arc<dyn ExecutionPlan> {
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

    if let Some(coalesce) =
        plan.as_any()
            .downcast_ref::<datafusion_physical_plan::coalesce_batches::CoalesceBatchesExec>()
    {
        return flatten_transparent_nodes(coalesce.input());
    }

    // Deletion-filter execs sit directly above the Cayenne scan whenever
    // key-deletes are pending. They preserve the child's schema and
    // partitioning (they only remove deleted rows), so for the purpose of
    // identifying a Cayenne-backed scan on a join build/probe side they are
    // transparent — see through them so the dynamic-filter join rewrite still
    // fires on tables undergoing CDC deletes.
    if let Some(int64_delete) = plan.as_any().downcast_ref::<Int64PkDeletionFilterExec>() {
        let children = int64_delete.children();
        let Some(input) = children.first() else {
            return plan;
        };

        return flatten_transparent_nodes(input);
    }

    if let Some(key_delete) = plan.as_any().downcast_ref::<KeyBasedDeletionFilterExec>() {
        let children = key_delete.children();
        let Some(input) = children.first() else {
            return plan;
        };

        return flatten_transparent_nodes(input);
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
