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

//! Batched index nested-loop execution over pinned covering pages.

use std::{any::Any, fmt, sync::Arc};

use arrow::{
    array::{Array, ArrayRef, BooleanArray, UInt32Array, new_null_array},
    record_batch::{RecordBatch, RecordBatchOptions},
};
use arrow_schema::SchemaRef;
use datafusion::config::ConfigOptions;
use datafusion_common::{DFSchema, DataFusionError, JoinSide, NullEquality, Result, Statistics};
use datafusion_execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use datafusion_expr::JoinType;
use datafusion_physical_expr::{
    EquivalenceProperties, PhysicalExpr, PhysicalExprRef, execution_props::ExecutionProps,
    expressions::Column,
};
use datafusion_physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
    SortOrderPushdownResult,
    execution_plan::{CardinalityEffect, EmissionType, InvariantLevel, check_default_invariants},
    filter_pushdown::{
        ChildPushdownResult, FilterDescription, FilterPushdownPhase, FilterPushdownPropagation,
    },
    joins::utils::{ColumnIndex, JoinFilter, build_join_schema},
    metrics::{BaselineMetrics, Count, ExecutionPlanMetricsSet, MetricBuilder, MetricsSet},
    projection::ProjectionExec,
    stream::RecordBatchStreamAdapter,
};
use futures::{StreamExt, stream};

use super::exec::visible_mask;
use super::{
    CoveringIndexAccess, CoveringIndexCapability, Error, GatherBatch, IndexDefinition, ProbeCursor,
    ProbeMatch, ProbeRequest, ProbeStep, gather_stored, probe_many,
};

const MAX_OUTER_ROWS: usize = 1_024;
const MAX_ENCODED_KEY_BYTES: usize = 1024 * 1024;
const PROBE_ROWS_PER_POLL: usize = 256;
const PROBE_BYTES_PER_POLL: usize = 256 * 1024;
const GATHER_ROWS_PER_POLL: usize = 256;
const GATHER_BYTES_PER_POLL: usize = 512 * 1024;
const OUTPUT_BYTES_TARGET: usize = 1024 * 1024;
const OUTER_SLICE_ACCOUNTED_BYTES: usize = MAX_ENCODED_KEY_BYTES + (MAX_OUTER_ROWS * 64);

/// Maps the executable outer input to its original logical join side.
///
/// Every other mapping is ordinal-based; names are not reliable for self joins.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct IndexJoinMapping {
    outer_is_left: bool,
}

impl IndexJoinMapping {
    #[must_use]
    pub(crate) const fn new(outer_is_left: bool) -> Self {
        Self { outer_is_left }
    }

    #[must_use]
    pub(crate) const fn outer_is_left(self) -> bool {
        self.outer_is_left
    }
}

#[derive(Clone)]
struct IndexJoinMetrics {
    baseline: BaselineMetrics,
    probe_keys: Count,
    candidate_rows: Count,
    key_pages_read: Count,
    payload_pages_read: Count,
}

impl IndexJoinMetrics {
    fn new(metrics: &ExecutionPlanMetricsSet, partition: usize) -> Self {
        Self {
            baseline: BaselineMetrics::new(metrics, partition),
            probe_keys: MetricBuilder::new(metrics).counter("index_probe_keys", partition),
            candidate_rows: MetricBuilder::new(metrics).counter("index_candidate_rows", partition),
            key_pages_read: MetricBuilder::new(metrics).counter("index_key_pages_read", partition),
            payload_pages_read: MetricBuilder::new(metrics)
                .counter("index_payload_pages_read", partition),
        }
    }
}

/// A one-child index nested-loop join. The immutable inner capability is not an
/// executable scan child and cannot be replaced by a later table view.
pub(crate) struct CayenneIndexJoinExec {
    outer: Arc<dyn ExecutionPlan>,
    inner_capability: CoveringIndexCapability,
    inner_access: CoveringIndexAccess,
    outer_key_columns: Arc<[usize]>,
    mapping: IndexJoinMapping,
    projection: Option<Arc<[usize]>>,
    filter: Option<JoinFilter>,
    residual_filters: Arc<[JoinFilter]>,
    additional_inner_filters: Arc<[PhysicalExprRef]>,
    raw_output_candidate_upper_bound: Option<usize>,
    join_type: JoinType,
    null_equality: NullEquality,
    inner_static_filters: Arc<[Arc<dyn PhysicalExpr>]>,
    left_schema: SchemaRef,
    right_schema: SchemaRef,
    join_schema: SchemaRef,
    schema: SchemaRef,
    properties: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
}

impl CayenneIndexJoinExec {
    /// Creates an eligible ordinary-equality index join.
    #[expect(clippy::too_many_arguments)]
    pub(crate) fn try_new(
        outer: Arc<dyn ExecutionPlan>,
        inner_capability: CoveringIndexCapability,
        inner_definition: &IndexDefinition,
        outer_key_expressions: &[PhysicalExprRef],
        mapping: IndexJoinMapping,
        projection: Option<Vec<usize>>,
        filter: Option<JoinFilter>,
        join_type: JoinType,
        null_equality: NullEquality,
    ) -> Result<Self> {
        Self::try_new_with_inner_filters(
            outer,
            inner_capability,
            inner_definition,
            outer_key_expressions,
            mapping,
            projection,
            filter,
            &[],
            &[],
            None,
            join_type,
            null_equality,
        )
    }

    /// Creates a join while retaining filters captured above the inner scan and
    /// residual equijoins not represented by the selected index key.
    #[expect(clippy::too_many_arguments)]
    pub(crate) fn try_new_with_inner_filters(
        outer: Arc<dyn ExecutionPlan>,
        inner_capability: CoveringIndexCapability,
        inner_definition: &IndexDefinition,
        outer_key_expressions: &[PhysicalExprRef],
        mapping: IndexJoinMapping,
        projection: Option<Vec<usize>>,
        filter: Option<JoinFilter>,
        additional_inner_filters: &[PhysicalExprRef],
        residual_filters: &[JoinFilter],
        raw_output_candidate_upper_bound: Option<usize>,
        join_type: JoinType,
        null_equality: NullEquality,
    ) -> Result<Self> {
        if !matches!(
            join_type,
            JoinType::Inner | JoinType::Left | JoinType::Right
        ) {
            return Err(DataFusionError::Plan(format!(
                "CayenneIndexJoinExec supports INNER, LEFT, and RIGHT joins, not {join_type:?}"
            )));
        }
        if join_type == JoinType::Left && !mapping.outer_is_left() {
            return Err(DataFusionError::Plan(
                "CayenneIndexJoinExec LEFT join requires logical left as outer".to_string(),
            ));
        }
        if join_type == JoinType::Right && mapping.outer_is_left() {
            return Err(DataFusionError::Plan(
                "CayenneIndexJoinExec RIGHT join requires logical right as outer".to_string(),
            ));
        }
        if null_equality != NullEquality::NullEqualsNothing {
            return Err(DataFusionError::Plan(
                "CayenneIndexJoinExec supports ordinary equality only; NULL-safe equality is ineligible"
                    .to_string(),
            ));
        }
        if outer_key_expressions.len() != inner_definition.columns().len() {
            return Err(DataFusionError::Plan(format!(
                "CayenneIndexJoinExec received {} outer keys for a {}-column inner index",
                outer_key_expressions.len(),
                inner_definition.columns().len()
            )));
        }

        let inner_access = inner_capability
            .accesses()
            .iter()
            .find(|access| access.definition().matches(inner_definition))
            .cloned()
            .ok_or_else(|| {
                DataFusionError::Plan(
                    "CayenneIndexJoinExec inner definition is not retained by its capability"
                        .to_string(),
                )
            })?;
        let inner_schema = Arc::new(
            inner_access
                .view()
                .query_schema()
                .schema()
                .project(inner_capability.output_columns())?,
        );
        let (left_schema, right_schema) = if mapping.outer_is_left() {
            (outer.schema(), Arc::clone(&inner_schema))
        } else {
            (Arc::clone(&inner_schema), outer.schema())
        };
        validate_join_filter(filter.as_ref(), &left_schema, &right_schema)?;
        for residual in residual_filters {
            validate_join_filter(Some(residual), &left_schema, &right_schema)?;
        }
        let (join_schema, _) = build_join_schema(&left_schema, &right_schema, &join_type);
        let join_schema = Arc::new(join_schema);
        let schema = if let Some(projection) = &projection {
            Arc::new(join_schema.project(projection)?)
        } else {
            Arc::clone(&join_schema)
        };
        let outer_key_columns = validate_outer_keys(
            outer_key_expressions,
            outer.schema().as_ref(),
            inner_definition,
        )?;
        let inner_static_filters = plan_static_filters(&inner_capability, &inner_access)?
            .iter()
            .cloned()
            .chain(additional_inner_filters.iter().cloned())
            .collect::<Vec<_>>()
            .into();
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            datafusion_physical_expr::Partitioning::UnknownPartitioning(
                outer.output_partitioning().partition_count(),
            ),
            if matches!(join_type, JoinType::Left | JoinType::Right) {
                EmissionType::Both
            } else {
                EmissionType::Incremental
            },
            outer.boundedness(),
        ));

        Ok(Self {
            outer,
            inner_capability,
            inner_access,
            outer_key_columns: outer_key_columns.into(),
            mapping,
            projection: projection.map(Into::into),
            filter,
            residual_filters: residual_filters.to_vec().into(),
            additional_inner_filters: additional_inner_filters.to_vec().into(),
            raw_output_candidate_upper_bound,
            join_type,
            null_equality,
            inner_static_filters,
            left_schema,
            right_schema,
            join_schema,
            schema,
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }

    /// Conservative raw bound suitable for an outer index-join candidate.
    #[must_use]
    pub(crate) const fn raw_output_candidate_upper_bound(&self) -> Option<usize> {
        self.raw_output_candidate_upper_bound
    }

    fn with_outer(&self, outer: Arc<dyn ExecutionPlan>) -> Result<Self> {
        if outer.schema().as_ref() != self.outer.schema().as_ref() {
            return Err(DataFusionError::Plan(
                "CayenneIndexJoinExec replacement outer child has a different schema".to_string(),
            ));
        }
        let keys = self
            .outer_key_columns
            .iter()
            .map(|index| {
                let outer_schema = self.outer.schema();
                let field = outer_schema.fields().get(*index).ok_or_else(|| {
                    DataFusionError::Plan(format!(
                        "CayenneIndexJoinExec outer key column {index} is outside its schema"
                    ))
                })?;
                Ok(Arc::new(Column::new(field.name(), *index)) as PhysicalExprRef)
            })
            .collect::<Result<Vec<_>>>()?;
        Self::try_new_with_inner_filters(
            outer,
            self.inner_capability.clone(),
            self.inner_access.definition(),
            &keys,
            self.mapping,
            self.projection
                .as_ref()
                .map(|projection| projection.to_vec()),
            self.filter.clone(),
            self.additional_inner_filters.as_ref(),
            self.residual_filters.as_ref(),
            self.raw_output_candidate_upper_bound,
            self.join_type,
            self.null_equality,
        )
    }
}

impl fmt::Debug for CayenneIndexJoinExec {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CayenneIndexJoinExec")
            .field("outer_key_columns", &self.outer_key_columns)
            .field(
                "inner_index_columns",
                &self.inner_access.definition().columns().len(),
            )
            .field("mapping", &self.mapping)
            .field("projection", &self.projection)
            .field("join_type", &self.join_type)
            .finish_non_exhaustive()
    }
}

impl DisplayAs for CayenneIndexJoinExec {
    fn fmt_as(&self, _display: DisplayFormatType, formatter: &mut fmt::Formatter) -> fmt::Result {
        let index = self
            .inner_access
            .definition()
            .columns()
            .iter()
            .map(super::IndexColumn::name)
            .collect::<Vec<_>>()
            .join(", ");
        write!(
            formatter,
            "CayenneIndexJoinExec: join_type={:?}, index=({index}), covering=all",
            self.join_type
        )
    }
}

#[deny(clippy::missing_trait_methods)]
impl ExecutionPlan for CayenneIndexJoinExec {
    fn downcast_delegate(&self) -> Option<&dyn ExecutionPlan> {
        None
    }

    fn with_preserve_order(&self, _preserve_order: bool) -> Option<Arc<dyn ExecutionPlan>> {
        None
    }

    fn name(&self) -> &'static str {
        "CayenneIndexJoinExec"
    }

    fn static_name() -> &'static str
    where
        Self: Sized,
    {
        "CayenneIndexJoinExec"
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn check_invariants(&self, check: InvariantLevel) -> Result<()> {
        check_default_invariants(self, check)
    }

    fn required_input_distribution(&self) -> Vec<datafusion_physical_expr::Distribution> {
        vec![datafusion_physical_expr::Distribution::UnspecifiedDistribution]
    }

    fn required_input_ordering(
        &self,
    ) -> Vec<Option<datafusion_physical_expr::OrderingRequirements>> {
        vec![None]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![false]
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![true]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.outer]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let [outer]: [Arc<dyn ExecutionPlan>; 1] =
            children.try_into().map_err(|children: Vec<_>| {
                DataFusionError::Plan(format!(
                    "CayenneIndexJoinExec requires exactly one outer child, received {}",
                    children.len()
                ))
            })?;
        Ok(Arc::new(self.with_outer(outer)?))
    }

    fn reset_state(self: Arc<Self>) -> Result<Arc<dyn ExecutionPlan>> {
        Arc::clone(&self).with_new_children(vec![Arc::clone(&self.outer)])
    }

    fn repartitioned(
        &self,
        _target_partitions: usize,
        _config: &ConfigOptions,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        Ok(None)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let partitions = self.outer.output_partitioning().partition_count();
        if partition >= partitions {
            return Err(DataFusionError::Execution(format!(
                "CayenneIndexJoinExec has {partitions} outer partitions, but partition {partition} was requested"
            )));
        }
        let output_batch_size = context.session_config().batch_size().max(1);
        let outer_stream = self.outer.execute(partition, Arc::clone(&context))?;
        let state = IndexJoinState {
            outer_stream,
            outer_input: None,
            active: None,
            pending_output: None,
            access: self.inner_access.clone(),
            outer_key_columns: Arc::clone(&self.outer_key_columns),
            inner_output_columns: self.inner_capability.output_columns().into(),
            inner_static_filters: Arc::clone(&self.inner_static_filters),
            mapping: self.mapping,
            filter: self.filter.clone(),
            residual_filters: Arc::clone(&self.residual_filters),
            join_type: self.join_type,
            left_schema: Arc::clone(&self.left_schema),
            right_schema: Arc::clone(&self.right_schema),
            join_schema: Arc::clone(&self.join_schema),
            schema: Arc::clone(&self.schema),
            projection: self.projection.clone(),
            output_batch_size,
            reservation: MemoryConsumer::new(format!("CayenneIndexJoinExec[{partition}]"))
                .register(context.memory_pool()),
            metrics: IndexJoinMetrics::new(&self.metrics, partition),
            done: false,
        };
        let output_schema = Arc::clone(&self.schema);
        let stream = stream::unfold(state, |mut state| async move {
            match state.next_output().await {
                Ok(Some(batch)) => Some((Ok(batch), state)),
                Ok(None) => None,
                Err(error) => {
                    state.done = true;
                    Some((Err(error), state))
                }
            }
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            output_schema,
            Box::pin(stream),
        )))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn partition_statistics(&self, _partition: Option<usize>) -> Result<Arc<Statistics>> {
        Ok(Arc::new(Statistics::new_unknown(&self.schema)))
    }

    fn cardinality_effect(&self) -> CardinalityEffect {
        CardinalityEffect::Unknown
    }

    fn supports_limit_pushdown(&self) -> bool {
        false
    }

    fn with_fetch(&self, _limit: Option<usize>) -> Option<Arc<dyn ExecutionPlan>> {
        None
    }

    fn fetch(&self) -> Option<usize> {
        None
    }

    fn try_pushdown_sort(
        &self,
        _exprs: &[datafusion_physical_plan::expressions::PhysicalSortExpr],
    ) -> Result<SortOrderPushdownResult<Arc<dyn ExecutionPlan>>> {
        Ok(SortOrderPushdownResult::Unsupported)
    }

    fn try_swapping_with_projection(
        &self,
        _projection: &ProjectionExec,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        Ok(None)
    }

    fn gather_filters_for_pushdown(
        &self,
        _phase: FilterPushdownPhase,
        parent_filters: Vec<Arc<dyn PhysicalExpr>>,
        _config: &ConfigOptions,
    ) -> Result<FilterDescription> {
        Ok(FilterDescription::all_unsupported(
            &parent_filters,
            &self.children(),
        ))
    }

    fn handle_child_pushdown_result(
        &self,
        _phase: FilterPushdownPhase,
        child_pushdown_result: ChildPushdownResult,
        _config: &ConfigOptions,
    ) -> Result<FilterPushdownPropagation<Arc<dyn ExecutionPlan>>> {
        Ok(FilterPushdownPropagation::if_all(child_pushdown_result))
    }

    fn with_new_state(&self, _state: Arc<dyn Any + Send + Sync>) -> Option<Arc<dyn ExecutionPlan>> {
        None
    }
}

fn validate_outer_keys(
    expressions: &[PhysicalExprRef],
    outer_schema: &arrow_schema::Schema,
    definition: &IndexDefinition,
) -> Result<Vec<usize>> {
    expressions
        .iter()
        .zip(definition.columns())
        .map(|(expression, indexed)| {
            let column = expression.downcast_ref::<Column>().ok_or_else(|| {
                DataFusionError::Plan(
                    "CayenneIndexJoinExec supports bare-column join keys only".to_string(),
                )
            })?;
            let field = outer_schema.fields().get(column.index()).ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "CayenneIndexJoinExec outer key '{}' is outside the outer schema",
                    column.name()
                ))
            })?;
            if !key_domains_match(field.data_type(), indexed.field().data_type()) {
                return Err(DataFusionError::Plan(format!(
                    "CayenneIndexJoinExec key '{}' has type {}, incompatible with indexed key '{}' of type {}",
                    column.name(),
                    field.data_type(),
                    indexed.name(),
                    indexed.field().data_type(),
                )));
            }
            Ok(column.index())
        })
        .collect()
}

fn key_domains_match(outer: &arrow_schema::DataType, stored: &arrow_schema::DataType) -> bool {
    outer == stored
        || matches!(
            (outer, stored),
            (
                arrow_schema::DataType::Utf8,
                arrow_schema::DataType::Utf8View
            ) | (
                arrow_schema::DataType::Utf8View,
                arrow_schema::DataType::Utf8
            ) | (
                arrow_schema::DataType::Binary,
                arrow_schema::DataType::BinaryView
            ) | (
                arrow_schema::DataType::BinaryView,
                arrow_schema::DataType::Binary
            )
        )
}

fn validate_join_filter(
    filter: Option<&JoinFilter>,
    left_schema: &SchemaRef,
    right_schema: &SchemaRef,
) -> Result<()> {
    let Some(filter) = filter else {
        return Ok(());
    };
    for column in filter.column_indices() {
        let schema = match column.side {
            JoinSide::Left => left_schema,
            JoinSide::Right => right_schema,
            JoinSide::None => {
                return Err(DataFusionError::Plan(
                    "CayenneIndexJoinExec does not support a JoinFilter column with no input side"
                        .to_string(),
                ));
            }
        };
        if column.index >= schema.fields().len() {
            return Err(DataFusionError::Plan(format!(
                "CayenneIndexJoinExec JoinFilter column {} is outside its {:?} input schema",
                column.index, column.side
            )));
        }
    }
    Ok(())
}

fn plan_static_filters(
    capability: &CoveringIndexCapability,
    access: &CoveringIndexAccess,
) -> Result<Arc<[Arc<dyn PhysicalExpr>]>> {
    let schema = DFSchema::try_from(access.view().query_schema().schema().as_ref().clone())?;
    let properties = ExecutionProps::new();
    capability
        .static_filters()
        .iter()
        .map(|filter| datafusion_physical_expr::create_physical_expr(filter, &schema, &properties))
        .collect::<Result<Vec<_>>>()
        .map(Into::into)
}

struct OuterInput {
    batch: RecordBatch,
    next_row: usize,
}

struct ActiveOuterSlice {
    batch: RecordBatch,
    cursor: ProbeCursor,
    pending_matches: Option<Vec<ProbeMatch>>,
    matched: Vec<bool>,
    next_unmatched: usize,
}

struct PendingOutput {
    batch: RecordBatch,
    next_row: usize,
}

struct IndexJoinState {
    outer_stream: SendableRecordBatchStream,
    outer_input: Option<OuterInput>,
    active: Option<ActiveOuterSlice>,
    pending_output: Option<PendingOutput>,
    access: CoveringIndexAccess,
    outer_key_columns: Arc<[usize]>,
    inner_output_columns: Arc<[usize]>,
    inner_static_filters: Arc<[Arc<dyn PhysicalExpr>]>,
    mapping: IndexJoinMapping,
    filter: Option<JoinFilter>,
    residual_filters: Arc<[JoinFilter]>,
    join_type: JoinType,
    left_schema: SchemaRef,
    right_schema: SchemaRef,
    join_schema: SchemaRef,
    schema: SchemaRef,
    projection: Option<Arc<[usize]>>,
    output_batch_size: usize,
    reservation: MemoryReservation,
    metrics: IndexJoinMetrics,
    done: bool,
}

impl IndexJoinState {
    async fn next_output(&mut self) -> Result<Option<RecordBatch>> {
        if self.done {
            return Ok(None);
        }
        loop {
            if let Some(output) = self.take_pending_output() {
                self.metrics.baseline.record_output(output.num_rows());
                return Ok(Some(output));
            }
            if self.active.is_some() {
                if let Some(matches) = self
                    .active
                    .as_mut()
                    .and_then(|active| active.pending_matches.take())
                {
                    self.gather(matches).await?;
                    continue;
                }
                let step = {
                    let active = self.active.as_mut().ok_or_else(|| {
                        DataFusionError::Internal(
                            "Cayenne index join lost its active outer slice".to_string(),
                        )
                    })?;
                    active
                        .cursor
                        .next_matches(PROBE_ROWS_PER_POLL, PROBE_BYTES_PER_POLL)
                        .await
                };
                let pages = self
                    .active
                    .as_mut()
                    .map_or(0, |active| active.cursor.take_key_pages_read());
                self.metrics.key_pages_read.add(pages);
                match step.map_err(|error| covering_error(&error))? {
                    ProbeStep::Matches(matches) => {
                        self.metrics.candidate_rows.add(matches.len());
                        self.gather(matches).await?;
                    }
                    ProbeStep::Pending => tokio::task::yield_now().await,
                    ProbeStep::Exhausted => {
                        if matches!(self.join_type, JoinType::Left | JoinType::Right)
                            && let Some(batch) = self.unmatched_batch()?
                        {
                            self.pending_output = Some(PendingOutput { batch, next_row: 0 });
                            continue;
                        }
                        self.active = None;
                        self.reservation.resize(0);
                    }
                }
                continue;
            }
            if self
                .outer_input
                .as_ref()
                .is_some_and(|input| input.next_row < input.batch.num_rows())
            {
                self.start_outer_slice()?;
                continue;
            }
            self.outer_input = None;
            match self.outer_stream.next().await {
                Some(Ok(batch)) if batch.num_rows() == 0 => tokio::task::yield_now().await,
                Some(Ok(batch)) => self.outer_input = Some(OuterInput { batch, next_row: 0 }),
                Some(Err(error)) => return Err(error),
                None => {
                    self.done = true;
                    return Ok(None);
                }
            }
        }
    }

    fn start_outer_slice(&mut self) -> Result<()> {
        self.reservation
            .try_resize(OUTER_SLICE_ACCOUNTED_BYTES)
            .map_err(|error| {
                DataFusionError::ResourcesExhausted(format!(
                    "Cayenne index join could not reserve bounded probe scratch: {error}"
                ))
            })?;
        let input = self.outer_input.as_mut().ok_or_else(|| {
            DataFusionError::Internal("Cayenne index join lost its outer input batch".to_string())
        })?;
        let start = input.next_row;
        let maximum = input
            .batch
            .num_rows()
            .min(start.saturating_add(MAX_OUTER_ROWS));
        let keys = self
            .outer_key_columns
            .iter()
            .map(|column| {
                input.batch.columns().get(*column).cloned().ok_or_else(|| {
                    DataFusionError::Internal(format!(
                        "Cayenne index join outer key column {column} disappeared from its batch"
                    ))
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let mut requests = Vec::new();
        let mut encoded_bytes = 0usize;
        let mut end = start;
        while end < maximum {
            let encoded = self
                .access
                .definition()
                .encode_probe_row(&keys, end)
                .map_err(|error| covering_error(&error))?;
            let bytes = encoded.as_ref().map_or(0, |key| key.as_bytes().len());
            if end > start && encoded_bytes.saturating_add(bytes) > MAX_ENCODED_KEY_BYTES {
                break;
            }
            if let Some(key) = encoded {
                requests.push(ProbeRequest::new(end - start, key, Vec::new()));
                encoded_bytes = encoded_bytes.saturating_add(bytes);
            }
            end = end.checked_add(1).ok_or_else(|| {
                DataFusionError::Internal(
                    "Cayenne index join outer-row cursor overflowed".to_string(),
                )
            })?;
        }
        let batch = input.batch.slice(start, end - start);
        input.next_row = end;
        self.metrics.probe_keys.add(requests.len());
        self.active = Some(ActiveOuterSlice {
            batch,
            cursor: probe_many(Arc::clone(self.access.view()), requests),
            pending_matches: None,
            matched: vec![false; end - start],
            next_unmatched: 0,
        });
        Ok(())
    }

    async fn gather(&mut self, mut matches: Vec<ProbeMatch>) -> Result<()> {
        let refs = matches
            .iter()
            .map(|matched| matched.row_ref().clone())
            .collect::<Vec<_>>();
        let gathered = gather_stored(
            self.access.view(),
            &refs,
            GATHER_ROWS_PER_POLL,
            GATHER_BYTES_PER_POLL,
        )
        .await
        .map_err(|error| covering_error(&error))?;
        self.metrics
            .payload_pages_read
            .add(gathered.payload_pages_read());
        let consumed = gathered.consumed();
        if consumed == 0 || consumed > matches.len() {
            return Err(DataFusionError::Internal(
                "Cayenne index join gather returned an invalid candidate count".to_string(),
            ));
        }
        let remaining = matches.split_off(consumed);
        let output = self.process_gather(gathered.batch(), &matches)?;
        let active = self.active.as_mut().ok_or_else(|| {
            DataFusionError::Internal(
                "Cayenne index join gathered rows without an outer slice".to_string(),
            )
        })?;
        active.pending_matches = (!remaining.is_empty()).then_some(remaining);
        if let Some(batch) = output {
            self.pending_output = Some(PendingOutput { batch, next_row: 0 });
        }
        Ok(())
    }

    fn process_gather(
        &mut self,
        batch: &RecordBatch,
        matches: &[ProbeMatch],
    ) -> Result<Option<RecordBatch>> {
        let outer_batch = self
            .active
            .as_ref()
            .map(|active| active.batch.clone())
            .ok_or_else(|| {
                DataFusionError::Internal(
                    "Cayenne index join lost its active outer slice".to_string(),
                )
            })?;
        if batch.num_rows() != matches.len() {
            return Err(DataFusionError::Internal(format!(
                "Cayenne index join gathered {} rows for {} probe matches",
                batch.num_rows(),
                matches.len()
            )));
        }
        let visibility = visible_mask(self.access.view(), batch, matches)
            .map_err(|error| covering_error(&error))?;
        let mut outer_indices = UInt32Array::from(
            matches
                .iter()
                .map(|matched| {
                    u32::try_from(matched.request_ordinal()).map_err(|_| {
                        DataFusionError::Execution(
                            "Cayenne index join outer ordinal does not fit Arrow UInt32"
                                .to_string(),
                        )
                    })
                })
                .collect::<Result<Vec<_>>>()?,
        );
        let mut inner = arrow::compute::filter_record_batch(batch, &visibility)?;
        outer_indices = downcast_u32(&arrow::compute::filter(&outer_indices, &visibility)?)?;
        inner = arrow_tools::record_batch::try_cast_to(
            inner,
            Arc::clone(self.access.view().query_schema().schema()),
        )
        .map_err(|error| {
            DataFusionError::Execution(format!(
                "Failed to adapt covered rows to the captured Cayenne schema: {error}"
            ))
        })?;
        for filter in self.inner_static_filters.iter() {
            let values = filter
                .evaluate(&inner)?
                .into_array_of_size(inner.num_rows())?;
            let keep = values
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| {
                    DataFusionError::Execution(format!(
                        "Cayenne index join static filter returned {:?}, expected Boolean",
                        values.data_type()
                    ))
                })?;
            outer_indices = downcast_u32(&arrow::compute::filter(&outer_indices, keep)?)?;
            inner = arrow::compute::filter_record_batch(&inner, keep)?;
        }
        if inner.num_rows() == 0 {
            return Ok(None);
        }
        let inner = inner.project(&self.inner_output_columns)?;
        let inner_indices = UInt32Array::from(
            (0..inner.num_rows())
                .map(|row| {
                    u32::try_from(row).map_err(|_| {
                        DataFusionError::Execution(
                            "Cayenne index join inner ordinal does not fit Arrow UInt32"
                                .to_string(),
                        )
                    })
                })
                .collect::<Result<Vec<_>>>()?,
        );
        let (outer_indices, inner_indices) =
            self.apply_join_filter(&outer_batch, &inner, outer_indices, inner_indices)?;
        if outer_indices.is_empty() {
            return Ok(None);
        }
        {
            let active = self.active.as_mut().ok_or_else(|| {
                DataFusionError::Internal(
                    "Cayenne index join lost its active outer slice".to_string(),
                )
            })?;
            for outer in outer_indices.values() {
                let outer = usize::try_from(*outer).map_err(|_| {
                    DataFusionError::Execution(
                        "Cayenne index join outer ordinal does not fit usize".to_string(),
                    )
                })?;
                let match_bit = active.matched.get_mut(outer).ok_or_else(|| {
                    DataFusionError::Internal(format!(
                        "Cayenne index join outer match ordinal {outer} is outside its slice"
                    ))
                })?;
                *match_bit = true;
            }
        }
        self.join_pair_batches(&outer_batch, &inner, &outer_indices, &inner_indices)
            .map(Some)
    }

    fn apply_join_filter(
        &self,
        outer: &RecordBatch,
        inner: &RecordBatch,
        outer_indices: UInt32Array,
        inner_indices: UInt32Array,
    ) -> Result<(UInt32Array, UInt32Array)> {
        let mut outer_indices = outer_indices;
        let mut inner_indices = inner_indices;
        for filter in self.filter.iter().chain(self.residual_filters.iter()) {
            let filter_batch =
                self.filter_batch(outer, inner, &outer_indices, &inner_indices, filter)?;
            let values = filter
                .expression()
                .evaluate(&filter_batch)?
                .into_array_of_size(filter_batch.num_rows())?;
            let keep = values
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| {
                    DataFusionError::Execution(format!(
                        "Cayenne index join ON filter returned {:?}, expected Boolean",
                        values.data_type()
                    ))
                })?;
            outer_indices = downcast_u32(&arrow::compute::filter(&outer_indices, keep)?)?;
            inner_indices = downcast_u32(&arrow::compute::filter(&inner_indices, keep)?)?;
        }
        Ok((outer_indices, inner_indices))
    }

    fn filter_batch(
        &self,
        outer: &RecordBatch,
        inner: &RecordBatch,
        outer_indices: &UInt32Array,
        inner_indices: &UInt32Array,
        filter: &JoinFilter,
    ) -> Result<RecordBatch> {
        let (left, right, left_indices, right_indices) = if self.mapping.outer_is_left() {
            (outer, inner, outer_indices, inner_indices)
        } else {
            (inner, outer, inner_indices, outer_indices)
        };
        let columns = filter
            .column_indices()
            .iter()
            .map(|column| gather_filter_column(left, right, left_indices, right_indices, column))
            .collect::<Result<Vec<_>>>()?;
        RecordBatch::try_new_with_options(
            Arc::clone(filter.schema()),
            columns,
            &RecordBatchOptions::new().with_row_count(Some(outer_indices.len())),
        )
        .map_err(DataFusionError::from)
    }

    fn join_pair_batches(
        &self,
        outer: &RecordBatch,
        inner: &RecordBatch,
        outer_indices: &UInt32Array,
        inner_indices: &UInt32Array,
    ) -> Result<RecordBatch> {
        let outer = arrow::compute::take_record_batch(outer, outer_indices)?;
        let inner = arrow::compute::take_record_batch(inner, inner_indices)?;
        self.join_batches(&outer, Some(&inner), outer.num_rows())
    }

    fn unmatched_batch(&mut self) -> Result<Option<RecordBatch>> {
        let active = self.active.as_mut().ok_or_else(|| {
            DataFusionError::Internal("Cayenne index join lost its active outer slice".to_string())
        })?;
        let mut unmatched = Vec::with_capacity(self.output_batch_size);
        while active.next_unmatched < active.matched.len()
            && unmatched.len() < self.output_batch_size
        {
            let row = active.next_unmatched;
            active.next_unmatched += 1;
            if !active.matched[row] {
                unmatched.push(row);
            }
        }
        if unmatched.is_empty() {
            return Ok(None);
        }
        let indices = UInt32Array::from(
            unmatched
                .into_iter()
                .map(|row| {
                    u32::try_from(row).map_err(|_| {
                        DataFusionError::Execution(
                            "Cayenne index join unmatched ordinal does not fit Arrow UInt32"
                                .to_string(),
                        )
                    })
                })
                .collect::<Result<Vec<_>>>()?,
        );
        let outer = arrow::compute::take_record_batch(&active.batch, &indices)?;
        self.join_batches(&outer, None, outer.num_rows()).map(Some)
    }

    fn join_batches(
        &self,
        outer: &RecordBatch,
        inner: Option<&RecordBatch>,
        row_count: usize,
    ) -> Result<RecordBatch> {
        let inner_columns = inner.map_or_else(
            || {
                let schema = if self.mapping.outer_is_left() {
                    &self.right_schema
                } else {
                    &self.left_schema
                };
                schema
                    .fields()
                    .iter()
                    .map(|field| new_null_array(field.data_type(), row_count))
                    .collect::<Vec<_>>()
            },
            |batch| batch.columns().to_vec(),
        );
        let columns = if self.mapping.outer_is_left() {
            outer
                .columns()
                .iter()
                .cloned()
                .chain(inner_columns)
                .collect()
        } else {
            inner_columns
                .into_iter()
                .chain(outer.columns().iter().cloned())
                .collect()
        };
        let joined = RecordBatch::try_new_with_options(
            Arc::clone(&self.join_schema),
            columns,
            &RecordBatchOptions::new().with_row_count(Some(row_count)),
        )?;
        if let Some(projection) = &self.projection {
            return RecordBatch::try_new_with_options(
                Arc::clone(&self.schema),
                joined.project(projection)?.columns().to_vec(),
                &RecordBatchOptions::new().with_row_count(Some(row_count)),
            )
            .map_err(DataFusionError::from);
        }
        Ok(joined)
    }

    fn take_pending_output(&mut self) -> Option<RecordBatch> {
        let pending = self.pending_output.as_mut()?;
        let remaining = pending.batch.num_rows() - pending.next_row;
        if remaining == 0 {
            self.pending_output = None;
            return None;
        }
        let mut rows = remaining.min(self.output_batch_size);
        while rows > 1
            && pending
                .batch
                .slice(pending.next_row, rows)
                .get_array_memory_size()
                > OUTPUT_BYTES_TARGET
        {
            rows = rows.div_ceil(2);
        }
        let output = pending.batch.slice(pending.next_row, rows);
        pending.next_row += rows;
        if pending.next_row == pending.batch.num_rows() {
            self.pending_output = None;
        }
        Some(output)
    }
}

fn gather_filter_column(
    left: &RecordBatch,
    right: &RecordBatch,
    left_indices: &UInt32Array,
    right_indices: &UInt32Array,
    column: &ColumnIndex,
) -> Result<ArrayRef> {
    let (batch, indices) = match column.side {
        JoinSide::Left => (left, left_indices),
        JoinSide::Right => (right, right_indices),
        JoinSide::None => {
            return Err(DataFusionError::Plan(
                "Cayenne index join cannot gather a JoinFilter column without an input side"
                    .to_string(),
            ));
        }
    };
    let values = batch.columns().get(column.index).ok_or_else(|| {
        DataFusionError::Internal(format!(
            "Cayenne index join JoinFilter column {} is outside its input batch",
            column.index
        ))
    })?;
    arrow::compute::take(values.as_ref(), indices, None).map_err(DataFusionError::from)
}

fn downcast_u32(array: &ArrayRef) -> Result<UInt32Array> {
    array
        .as_any()
        .downcast_ref::<UInt32Array>()
        .cloned()
        .ok_or_else(|| {
            DataFusionError::Internal(format!(
                "Cayenne index join expected Arrow filter to preserve UInt32, got {:?}",
                array.data_type()
            ))
        })
}

fn covering_error(error: &Error) -> DataFusionError {
    DataFusionError::Execution(format!("Failed to read Cayenne covering index: {error}"))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{
        array::{Array, ArrayRef, BooleanArray, Int64Array, RecordBatch, StringArray},
        datatypes::{DataType, Field, Schema},
    };
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::execution::{
        context::SessionContext,
        memory_pool::{GreedyMemoryPool, MemoryPool},
    };
    use datafusion_common::{JoinSide, NullEquality};
    use datafusion_physical_expr::{PhysicalExprRef, expressions::Column};
    use datafusion_physical_plan::{
        ExecutionPlan,
        common::collect,
        joins::utils::{ColumnIndex, JoinFilter},
    };

    use super::super::{CoveringPageStore, CoveringReadView, IndexCatalog, SourceId, build_source};
    use super::{
        CayenneIndexJoinExec, CoveringIndexAccess, CoveringIndexCapability, IndexDefinition,
        IndexJoinMapping,
    };
    use crate::provider::{lookup_index::KeySpec, memory_account::CayenneMemoryAccount};

    fn inner_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("key", DataType::Int64, true),
            Field::new("active", DataType::Boolean, true),
            Field::new("inner_value", DataType::Utf8, true),
        ]))
    }

    fn outer_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("key", DataType::Int64, true),
            Field::new("outer_value", DataType::Utf8, false),
        ]))
    }

    fn definition(schema: Arc<Schema>, columns: &[&str]) -> IndexDefinition {
        let spec = KeySpec::new(columns.iter().map(ToString::to_string).collect())
            .expect("test index has a key");
        IndexDefinition::resolve(schema, &spec).expect("test index resolves")
    }

    async fn inner_capability(
        schema: Arc<Schema>,
        definition: IndexDefinition,
        batch: RecordBatch,
    ) -> CoveringIndexCapability {
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(64 * 1024 * 1024));
        let account = Arc::new(CayenneMemoryAccount::new("covering-index-join-test", &pool));
        let source_id = SourceId::new("covering-index-join-test", 1);
        let built = build_source(source_id.clone(), definition.clone(), vec![batch], account)
            .await
            .expect("test inner source builds");
        let (source, runs, store) = built.into_parts();
        let query_schema = source.schema().clone();
        let page_store: Arc<dyn CoveringPageStore> = store;
        let catalog = Arc::new(
            IndexCatalog::new(definition.clone(), vec![source], runs.to_vec(), page_store)
                .expect("test inner catalog builds"),
        );
        let view = Arc::new(CoveringReadView::new(
            catalog,
            vec![source_id],
            query_schema,
        ));
        let access = CoveringIndexAccess::new(view, definition);
        CoveringIndexCapability::new(
            vec![access],
            (0..schema.fields().len()).collect(),
            Vec::new(),
            None,
        )
    }

    async fn execute(
        outer: RecordBatch,
        inner: RecordBatch,
        definition: IndexDefinition,
        join_type: datafusion_expr::JoinType,
        filter: Option<JoinFilter>,
    ) -> Vec<RecordBatch> {
        execute_with_keys(outer, inner, definition, &[0], join_type, filter).await
    }

    async fn execute_with_keys(
        outer: RecordBatch,
        inner: RecordBatch,
        definition: IndexDefinition,
        key_columns: &[usize],
        join_type: datafusion_expr::JoinType,
        filter: Option<JoinFilter>,
    ) -> Vec<RecordBatch> {
        let capability = inner_capability(inner.schema(), definition.clone(), inner).await;
        let outer_schema = outer.schema();
        let outer_plan = MemorySourceConfig::try_new_exec(&[vec![outer]], outer_schema, None)
            .expect("test outer plan builds");
        let key_expressions = key_columns
            .iter()
            .map(|column| Arc::new(Column::new("key", *column)) as PhysicalExprRef)
            .collect::<Vec<_>>();
        let join = CayenneIndexJoinExec::try_new(
            outer_plan,
            capability,
            &definition,
            &key_expressions,
            IndexJoinMapping::new(true),
            None,
            filter,
            join_type,
            NullEquality::NullEqualsNothing,
        )
        .expect("test index join constructs");
        let context = SessionContext::new();
        collect(
            join.execute(0, context.task_ctx())
                .expect("test index join starts"),
        )
        .await
        .expect("test index join succeeds")
    }

    type JoinRow<'a> = (
        Option<i64>,
        Option<&'a str>,
        Option<i64>,
        Option<bool>,
        Option<&'a str>,
    );

    fn rows(batches: &[RecordBatch]) -> Vec<JoinRow<'_>> {
        batches
            .iter()
            .flat_map(|batch| {
                let outer_key = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("outer key");
                let outer_value = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("outer value");
                let inner_key = batch
                    .column(2)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("inner key");
                let active = batch
                    .column(3)
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .expect("active");
                let inner_value = batch
                    .column(4)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("inner value");
                (0..batch.num_rows()).map(move |row| {
                    (
                        (!outer_key.is_null(row)).then(|| outer_key.value(row)),
                        (!outer_value.is_null(row)).then(|| outer_value.value(row)),
                        (!inner_key.is_null(row)).then(|| inner_key.value(row)),
                        (!active.is_null(row)).then(|| active.value(row)),
                        (!inner_value.is_null(row)).then(|| inner_value.value(row)),
                    )
                })
            })
            .collect()
    }

    fn inner_batch() -> RecordBatch {
        RecordBatch::try_new(
            inner_schema(),
            vec![
                Arc::new(Int64Array::from(vec![Some(1), Some(1), Some(2)])) as ArrayRef,
                Arc::new(BooleanArray::from(vec![Some(true), Some(false), None])) as ArrayRef,
                Arc::new(StringArray::from(vec![
                    Some("one-active"),
                    Some("one-inactive"),
                    Some("two-null"),
                ])) as ArrayRef,
            ],
        )
        .expect("test inner batch")
    }

    fn outer_batch() -> RecordBatch {
        RecordBatch::try_new(
            outer_schema(),
            vec![
                Arc::new(Int64Array::from(vec![
                    Some(1),
                    Some(1),
                    Some(2),
                    None,
                    Some(99),
                ])) as ArrayRef,
                Arc::new(StringArray::from(vec![
                    "first", "second", "third", "null-key", "missing",
                ])) as ArrayRef,
            ],
        )
        .expect("test outer batch")
    }

    #[tokio::test]
    async fn inner_preserves_duplicate_outer_and_inner_fanout() {
        let definition = definition(inner_schema(), &["key"]);
        let batches = execute(
            outer_batch(),
            inner_batch(),
            definition,
            datafusion_expr::JoinType::Inner,
            None,
        )
        .await;
        let rows = rows(&batches);
        assert_eq!(
            rows.len(),
            5,
            "two duplicate outer keys and two duplicate inner keys form four pairs"
        );
        assert_eq!(
            rows,
            vec![
                (
                    Some(1),
                    Some("first"),
                    Some(1),
                    Some(true),
                    Some("one-active")
                ),
                (
                    Some(1),
                    Some("second"),
                    Some(1),
                    Some(true),
                    Some("one-active")
                ),
                (
                    Some(1),
                    Some("first"),
                    Some(1),
                    Some(false),
                    Some("one-inactive")
                ),
                (
                    Some(1),
                    Some("second"),
                    Some(1),
                    Some(false),
                    Some("one-inactive")
                ),
                (Some(2), Some("third"), Some(2), None, Some("two-null")),
            ]
        );
    }

    #[tokio::test]
    async fn left_marks_only_on_qualified_pairs_and_keeps_null_keys_unmatched() {
        let filter_schema = Arc::new(Schema::new(vec![Field::new(
            "active",
            DataType::Boolean,
            true,
        )]));
        let filter = JoinFilter::new(
            Arc::new(Column::new("active", 0)),
            vec![ColumnIndex {
                index: 1,
                side: JoinSide::Right,
            }],
            filter_schema,
        );
        let definition = definition(inner_schema(), &["key"]);
        let batches = execute(
            outer_batch(),
            inner_batch(),
            definition,
            datafusion_expr::JoinType::Left,
            Some(filter),
        )
        .await;
        let rows = rows(&batches);
        assert_eq!(
            rows,
            vec![
                (
                    Some(1),
                    Some("first"),
                    Some(1),
                    Some(true),
                    Some("one-active")
                ),
                (
                    Some(1),
                    Some("second"),
                    Some(1),
                    Some(true),
                    Some("one-active")
                ),
                (Some(2), Some("third"), None, None, None),
                (None, Some("null-key"), None, None, None),
                (Some(99), Some("missing"), None, None, None),
            ],
            "a NULL ON result does not qualify a match, so LEFT emits one null extension"
        );
    }

    #[tokio::test]
    async fn composite_probe_keeps_tuple_components_correlated() {
        let inner_schema = Arc::new(Schema::new(vec![
            Field::new("tenant", DataType::Int64, false),
            Field::new("id", DataType::Int64, false),
            Field::new("payload", DataType::Utf8, false),
        ]));
        let outer_schema = Arc::new(Schema::new(vec![
            Field::new("tenant", DataType::Int64, false),
            Field::new("id", DataType::Int64, false),
            Field::new("outer_payload", DataType::Utf8, false),
        ]));
        let inner = RecordBatch::try_new(
            Arc::clone(&inner_schema),
            vec![
                Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef,
                Arc::new(Int64Array::from(vec![10, 20])) as ArrayRef,
                Arc::new(StringArray::from(vec!["one-ten", "two-twenty"])) as ArrayRef,
            ],
        )
        .expect("composite inner batch");
        let outer = RecordBatch::try_new(
            outer_schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 1, 2])) as ArrayRef,
                Arc::new(Int64Array::from(vec![10, 20, 20, 10])) as ArrayRef,
                Arc::new(StringArray::from(vec![
                    "match-one",
                    "match-two",
                    "cross-one",
                    "cross-two",
                ])) as ArrayRef,
            ],
        )
        .expect("composite outer batch");
        let definition = definition(inner_schema, &["tenant", "id"]);
        let batches = execute_with_keys(
            outer,
            inner,
            definition,
            &[0, 1],
            datafusion_expr::JoinType::Inner,
            None,
        )
        .await;
        let pairs = batches
            .iter()
            .flat_map(|batch| {
                let tenant = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("outer tenant");
                let id = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("outer id");
                (0..batch.num_rows()).map(move |row| (tenant.value(row), id.value(row)))
            })
            .collect::<Vec<_>>();
        assert_eq!(pairs, vec![(1, 10), (2, 20)]);
    }

    #[tokio::test]
    async fn high_duplicate_fanout_streams_without_a_candidate_cap() {
        const FANOUT: usize = 10_000;
        let inner = RecordBatch::try_new(
            inner_schema(),
            vec![
                Arc::new(Int64Array::from(vec![7; FANOUT])) as ArrayRef,
                Arc::new(BooleanArray::from(vec![true; FANOUT])) as ArrayRef,
                Arc::new(StringArray::from_iter_values(std::iter::repeat_n(
                    "fanout", FANOUT,
                ))) as ArrayRef,
            ],
        )
        .expect("high-fanout inner batch");
        let outer = RecordBatch::try_new(
            outer_schema(),
            vec![
                Arc::new(Int64Array::from(vec![7])) as ArrayRef,
                Arc::new(StringArray::from(vec!["outer"])) as ArrayRef,
            ],
        )
        .expect("high-fanout outer batch");
        let definition = definition(inner_schema(), &["key"]);
        let batches = execute(
            outer,
            inner,
            definition,
            datafusion_expr::JoinType::Inner,
            None,
        )
        .await;
        assert!(
            batches.len() > 1,
            "fanout resumes across bounded output batches"
        );
        assert_eq!(
            batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
            FANOUT,
            "every duplicate index entry expands to an output pair"
        );
    }
}
