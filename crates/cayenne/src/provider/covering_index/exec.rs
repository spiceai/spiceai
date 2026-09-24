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

//! Physical execution for a covered literal equality lookup.
//!
//! The operator is deliberately independent of Vortex scan construction. A
//! complete [`CoveringReadView`] owns immutable pages and source visibility
//! state, so this path gathers only covered rows and never repurposes a normal
//! file scan as a hidden gather helper.

use std::{
    any::Any,
    collections::BTreeMap,
    fmt,
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use arrow::{
    array::{BooleanArray, UInt32Array},
    record_batch::{RecordBatch, RecordBatchOptions},
};
use arrow_schema::SchemaRef;
use datafusion::config::ConfigOptions;
use datafusion_common::{DataFusionError, Result, Statistics};
use datafusion_expr::Expr;
use datafusion_physical_expr::{
    EquivalenceProperties, PhysicalExpr, PhysicalSortExpr, expressions::Column,
};
use datafusion_physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
    SortOrderPushdownResult,
    execution_plan::{
        Boundedness, CardinalityEffect, EmissionType, InvariantLevel, check_default_invariants,
    },
    filter_pushdown::{
        ChildPushdownResult, FilterDescription, FilterPushdownPhase, FilterPushdownPropagation,
    },
    metrics::{BaselineMetrics, Count, ExecutionPlanMetricsSet, MetricBuilder, MetricsSet},
    projection::ProjectionExec,
};
use futures::Stream;

use super::{
    CoveredRowRef, CoveringReadView, EncodedKey, Error, IndexColumn, IndexDefinition,
    PreparedLiteralSeek, ProbeCursor, ProbeMatch, ProbeStep, Result as CoveringResult,
    gather_stored, probe_prepared_literal,
};

/// Immutable index access information retained at a Cayenne scan boundary.
///
/// It is intentionally a plan-local capability, not a table lookup: the view
/// pins the catalog, source manifest, visibility adapters and snapshot lease
/// that the exact outer scan captured. A later physical join rule can therefore
/// only use the data this scan itself is allowed to read.
#[derive(Clone)]
pub(crate) struct CoveringIndexAccess {
    view: Arc<CoveringReadView>,
    definition: IndexDefinition,
}

impl CoveringIndexAccess {
    /// One complete definition over one captured immutable read view.
    #[must_use]
    pub(crate) fn new(view: Arc<CoveringReadView>, definition: IndexDefinition) -> Self {
        Self { view, definition }
    }

    /// Captured view whose pages and visibility this access may use.
    #[must_use]
    pub(crate) fn view(&self) -> &Arc<CoveringReadView> {
        &self.view
    }

    /// Exact indexed key definition.
    #[must_use]
    pub(crate) fn definition(&self) -> &IndexDefinition {
        &self.definition
    }
}

impl fmt::Debug for CoveringIndexAccess {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CoveringIndexAccess")
            .field("index_columns", &self.definition.columns().len())
            .finish_non_exhaustive()
    }
}

#[derive(Clone)]
pub(crate) struct CoveringIndexCapability {
    accesses: Arc<[CoveringIndexAccess]>,
    /// Query-schema columns emitted by the owning scan, in output ordinal order.
    output_columns: Arc<[usize]>,
    /// Logical predicates already associated with the owning scan.
    static_filters: Arc<[Expr]>,
    /// Exact raw posting bound when this capability arose from a literal seek.
    raw_candidate_upper_bound: Option<usize>,
}

impl CoveringIndexCapability {
    /// Create a capability for one immutable captured scan view.
    #[must_use]
    pub(crate) fn new(
        accesses: Vec<CoveringIndexAccess>,
        output_columns: Vec<usize>,
        static_filters: Vec<Expr>,
        raw_candidate_upper_bound: Option<usize>,
    ) -> Self {
        Self {
            accesses: accesses.into(),
            output_columns: output_columns.into(),
            static_filters: static_filters.into(),
            raw_candidate_upper_bound,
        }
    }

    /// Complete captured indexes this scan may access.
    #[must_use]
    pub(crate) fn accesses(&self) -> &[CoveringIndexAccess] {
        &self.accesses
    }

    /// Captured output-to-query-schema ordinal mapping.
    #[must_use]
    pub(crate) fn output_columns(&self) -> &[usize] {
        &self.output_columns
    }

    /// Static predicates attached to the captured scan.
    #[must_use]
    pub(crate) fn static_filters(&self) -> &[Expr] {
        &self.static_filters
    }

    /// Raw candidate upper bound, if planning performed a literal seek.
    #[must_use]
    pub(crate) const fn raw_candidate_upper_bound(&self) -> Option<usize> {
        self.raw_candidate_upper_bound
    }

    /// Compose a bare-column projection through this capability's ordinal map.
    ///
    /// Expressions, out-of-range ordinals, and aliases that alter the exact
    /// Arrow field contract are deliberately ineligible. A later optimizer must
    /// retain the ordinary scan rather than invent a mapping for them.
    pub(crate) fn project_through(&self, projection: &ProjectionExec) -> Option<Self> {
        let output_columns = projection
            .expr()
            .iter()
            .map(|expr| {
                let column = expr.expr.downcast_ref::<Column>()?;
                self.output_columns.get(column.index()).copied()
            })
            .collect::<Option<Vec<_>>>()?;
        Some(Self {
            accesses: Arc::clone(&self.accesses),
            output_columns: output_columns.into(),
            static_filters: Arc::clone(&self.static_filters),
            raw_candidate_upper_bound: self.raw_candidate_upper_bound,
        })
    }
}

impl fmt::Debug for CoveringIndexCapability {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CoveringIndexCapability")
            .field("complete_indexes", &self.accesses.len())
            .field("output_columns", &self.output_columns)
            .field("static_filters", &self.static_filters.len())
            .field("raw_candidate_upper_bound", &self.raw_candidate_upper_bound)
            .finish_non_exhaustive()
    }
}

/// Per-partition metrics for a covering literal scan.
#[derive(Clone)]
struct IndexScanMetrics {
    baseline: BaselineMetrics,
    probe_keys: Count,
    candidate_rows: Count,
    key_pages_read: Count,
    payload_pages_read: Count,
}

impl IndexScanMetrics {
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

/// One-partition physical plan that evaluates a prepared covered point lookup.
pub(crate) struct CayenneIndexScanExec {
    capability: CoveringIndexCapability,
    access: CoveringIndexAccess,
    prepared: PreparedLiteralSeek,
    static_filters: Arc<[Arc<dyn PhysicalExpr>]>,
    output_columns: Arc<[usize]>,
    schema: SchemaRef,
    limit: Option<usize>,
    properties: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
}

impl CayenneIndexScanExec {
    /// Construct a covered scan from an already-proven complete view and an
    /// already-prepared literal seek. `static_filters` operate on the complete
    /// query schema before `output_columns` is projected.
    pub(crate) fn try_new(
        capability: CoveringIndexCapability,
        access: CoveringIndexAccess,
        prepared: PreparedLiteralSeek,
        static_filters: Vec<Arc<dyn PhysicalExpr>>,
        output_columns: Vec<usize>,
        limit: Option<usize>,
    ) -> Result<Self> {
        if !capability.accesses().iter().any(|candidate| {
            candidate.definition().matches(access.definition())
                && Arc::ptr_eq(candidate.view(), access.view())
        }) {
            return Err(DataFusionError::Internal(
                "covering index scan access is not retained by its scan capability".to_string(),
            ));
        }
        let query_schema = access.view().query_schema().schema();
        let schema = Arc::new(query_schema.project(&output_columns)?);
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            datafusion_physical_expr::Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Ok(Self {
            capability,
            access,
            prepared,
            static_filters: static_filters.into(),
            output_columns: output_columns.into(),
            schema,
            limit,
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }

    fn with_scan_shape(
        &self,
        output_columns: Vec<usize>,
        schema: SchemaRef,
        limit: Option<usize>,
    ) -> Self {
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            datafusion_physical_expr::Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self {
            capability: self.capability.clone(),
            access: self.access.clone(),
            prepared: self.prepared.clone(),
            static_filters: Arc::clone(&self.static_filters),
            output_columns: output_columns.into(),
            schema,
            limit,
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }
}

impl fmt::Debug for CayenneIndexScanExec {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CayenneIndexScanExec")
            .field("index_columns", &self.access.definition().columns().len())
            .field(
                "raw_candidate_upper_bound",
                &self.prepared.raw_entry_count(),
            )
            .field("output_columns", &self.output_columns)
            .field("limit", &self.limit)
            .finish_non_exhaustive()
    }
}

impl DisplayAs for CayenneIndexScanExec {
    fn fmt_as(&self, _t: DisplayFormatType, formatter: &mut fmt::Formatter) -> fmt::Result {
        let index = self
            .access
            .definition()
            .columns()
            .iter()
            .map(IndexColumn::name)
            .collect::<Vec<_>>()
            .join(", ");
        write!(
            formatter,
            "CayenneIndexScanExec: index={index}, covering=all"
        )
    }
}

#[deny(clippy::missing_trait_methods)]
impl ExecutionPlan for CayenneIndexScanExec {
    fn downcast_delegate(&self) -> Option<&dyn ExecutionPlan> {
        None
    }

    fn with_preserve_order(&self, _preserve_order: bool) -> Option<Arc<dyn ExecutionPlan>> {
        None
    }

    fn name(&self) -> &'static str {
        "CayenneIndexScanExec"
    }

    fn static_name() -> &'static str
    where
        Self: Sized,
    {
        "CayenneIndexScanExec"
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
        Vec::new()
    }

    fn required_input_ordering(
        &self,
    ) -> Vec<Option<datafusion_physical_expr::OrderingRequirements>> {
        Vec::new()
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        Vec::new()
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        Vec::new()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        Vec::new()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return Err(DataFusionError::Plan(format!(
                "CayenneIndexScanExec is a leaf but received {} children",
                children.len()
            )));
        }
        Ok(Arc::new(self.with_scan_shape(
            self.output_columns.to_vec(),
            Arc::clone(&self.schema),
            self.limit,
        )))
    }

    fn reset_state(self: Arc<Self>) -> Result<Arc<dyn ExecutionPlan>> {
        self.with_new_children(Vec::new())
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
        _context: Arc<datafusion_execution::TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Execution(format!(
                "CayenneIndexScanExec has one partition, but partition {partition} was requested"
            )));
        }
        let cursor = probe_prepared_literal(Arc::clone(self.access.view()), &self.prepared)
            .map_err(|error| covering_error(&error))?;
        let metrics = IndexScanMetrics::new(&self.metrics, partition);
        metrics.probe_keys.add(1);
        Ok(Box::pin(CayenneIndexScanStream {
            cursor: Some(cursor),
            state: IndexScanState::Initialize,
            access: self.access.clone(),
            static_filters: Arc::clone(&self.static_filters),
            output_columns: Arc::clone(&self.output_columns),
            schema: Arc::clone(&self.schema),
            limit: self.limit,
            emitted_rows: 0,
            metrics,
        }))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn partition_statistics(&self, _partition: Option<usize>) -> Result<Arc<Statistics>> {
        // Posting counts are pre-visibility and pre-filter upper bounds, never
        // exact query statistics.
        Ok(Arc::new(Statistics::new_unknown(&self.schema)))
    }

    fn supports_limit_pushdown(&self) -> bool {
        true
    }

    fn with_fetch(&self, limit: Option<usize>) -> Option<Arc<dyn ExecutionPlan>> {
        let limit = match (self.limit, limit) {
            (Some(current), Some(pushed)) => Some(current.min(pushed)),
            (Some(current), None) => Some(current),
            (None, pushed) => pushed,
        };
        Some(Arc::new(self.with_scan_shape(
            self.output_columns.to_vec(),
            Arc::clone(&self.schema),
            limit,
        )))
    }

    fn fetch(&self) -> Option<usize> {
        self.limit
    }

    fn cardinality_effect(&self) -> CardinalityEffect {
        CardinalityEffect::LowerEqual
    }

    fn try_swapping_with_projection(
        &self,
        projection: &ProjectionExec,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        let Some(capability) = self.capability.project_through(projection) else {
            return Ok(None);
        };
        let output_columns = capability.output_columns().to_vec();
        let expected_schema = Arc::new(
            self.access
                .view()
                .query_schema()
                .schema()
                .project(&output_columns)?,
        );
        if expected_schema.as_ref() != projection.schema().as_ref() {
            return Ok(None);
        }
        let mut rewritten = self.with_scan_shape(output_columns, expected_schema, self.limit);
        rewritten.capability = capability;
        Ok(Some(Arc::new(rewritten)))
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

    fn try_pushdown_sort(
        &self,
        _order: &[PhysicalSortExpr],
    ) -> Result<SortOrderPushdownResult<Arc<dyn ExecutionPlan>>> {
        Ok(SortOrderPushdownResult::Unsupported)
    }
}

const PROBE_ROWS_PER_POLL: usize = 256;
const PROBE_BYTES_PER_POLL: usize = 256 * 1024;
const GATHER_ROWS_PER_POLL: usize = 256;
const GATHER_BYTES_PER_POLL: usize = 1024 * 1024;

type ProbeFuture = Pin<Box<dyn Future<Output = (ProbeCursor, CoveringResult<ProbeStep>)> + Send>>;
type GatherFuture = Pin<
    Box<
        dyn Future<
                Output = (
                    ProbeCursor,
                    Vec<ProbeMatch>,
                    CoveringResult<super::GatherBatch>,
                ),
            > + Send,
    >,
>;

/// Explicit resumable state for a covered scan stream.
enum IndexScanState {
    Initialize,
    Probe(ProbeFuture),
    Gather(GatherFuture),
    Done,
}

/// Candidate-page probe and payload gather stream.
///
/// Each state transition starts at most one bounded page probe or payload gather
/// and yields back to the executor before initiating another. Dropping the
/// stream drops its in-flight future, page leases and temporary candidate rows.
struct CayenneIndexScanStream {
    cursor: Option<ProbeCursor>,
    state: IndexScanState,
    access: CoveringIndexAccess,
    static_filters: Arc<[Arc<dyn PhysicalExpr>]>,
    output_columns: Arc<[usize]>,
    schema: SchemaRef,
    limit: Option<usize>,
    emitted_rows: usize,
    metrics: IndexScanMetrics,
}

impl CayenneIndexScanStream {
    fn begin_probe(&mut self) -> Result<()> {
        let mut cursor = self.cursor.take().ok_or_else(|| {
            DataFusionError::Internal("covering index scan lost its probe cursor".to_string())
        })?;
        self.state = IndexScanState::Probe(Box::pin(async move {
            let result = cursor
                .next_matches(PROBE_ROWS_PER_POLL, PROBE_BYTES_PER_POLL)
                .await;
            (cursor, result)
        }));
        Ok(())
    }

    fn begin_gather(&mut self, matches: Vec<ProbeMatch>) -> Result<()> {
        let cursor = self.cursor.take().ok_or_else(|| {
            DataFusionError::Internal("covering index scan lost its probe cursor".to_string())
        })?;
        let view = Arc::clone(self.access.view());
        self.state = IndexScanState::Gather(Box::pin(async move {
            let refs = matches
                .iter()
                .map(|matched| matched.row_ref().clone())
                .collect::<Vec<_>>();
            let result =
                gather_stored(&view, &refs, GATHER_ROWS_PER_POLL, GATHER_BYTES_PER_POLL).await;
            (cursor, matches, result)
        }));
        Ok(())
    }

    fn process_gather(
        &self,
        batch: &RecordBatch,
        matches: &[ProbeMatch],
    ) -> Result<Option<RecordBatch>> {
        let visible = visible_mask(self.access.view(), batch, matches)
            .map_err(|error| covering_error(&error))?;
        let batch = arrow::compute::filter_record_batch(batch, &visible)?;
        let query_batch = arrow_tools::record_batch::try_cast_to(
            batch,
            Arc::clone(self.access.view().query_schema().schema()),
        )
        .map_err(|error| {
            DataFusionError::Execution(format!(
                "Failed to adapt covered rows to the captured Cayenne schema: {error}"
            ))
        })?;
        let filtered = apply_static_filters(query_batch, &self.static_filters)?;
        if filtered.num_rows() == 0 {
            return Ok(None);
        }
        let projected = project_output(&filtered, &self.output_columns, Arc::clone(&self.schema))?;
        Ok(Some(projected))
    }
}

impl Stream for CayenneIndexScanStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.limit.is_some_and(|limit| self.emitted_rows >= limit) {
            self.state = IndexScanState::Done;
            return Poll::Ready(None);
        }
        match &mut self.state {
            IndexScanState::Initialize => {
                if let Err(error) = self.begin_probe() {
                    self.state = IndexScanState::Done;
                    return Poll::Ready(Some(Err(error)));
                }
                context.waker().wake_by_ref();
                Poll::Pending
            }
            IndexScanState::Probe(future) => match future.as_mut().poll(context) {
                Poll::Pending => Poll::Pending,
                Poll::Ready((mut cursor, result)) => {
                    self.metrics
                        .key_pages_read
                        .add(cursor.take_key_pages_read());
                    self.cursor = Some(cursor);
                    match result.map_err(|error| covering_error(&error)) {
                        Ok(ProbeStep::Matches(matches)) => {
                            self.metrics.candidate_rows.add(matches.len());
                            if let Err(error) = self.begin_gather(matches) {
                                self.state = IndexScanState::Done;
                                return Poll::Ready(Some(Err(error)));
                            }
                            context.waker().wake_by_ref();
                            Poll::Pending
                        }
                        Ok(ProbeStep::Pending) => {
                            self.state = IndexScanState::Initialize;
                            context.waker().wake_by_ref();
                            Poll::Pending
                        }
                        Ok(ProbeStep::Exhausted) => {
                            self.state = IndexScanState::Done;
                            Poll::Ready(None)
                        }
                        Err(error) => {
                            self.state = IndexScanState::Done;
                            Poll::Ready(Some(Err(error)))
                        }
                    }
                }
            },
            IndexScanState::Gather(future) => match future.as_mut().poll(context) {
                Poll::Pending => Poll::Pending,
                Poll::Ready((cursor, mut matches, result)) => {
                    self.cursor = Some(cursor);
                    let gathered = match result.map_err(|error| covering_error(&error)) {
                        Ok(gathered) => gathered,
                        Err(error) => {
                            self.state = IndexScanState::Done;
                            return Poll::Ready(Some(Err(error)));
                        }
                    };
                    let consumed = gathered.consumed();
                    self.metrics
                        .payload_pages_read
                        .add(gathered.payload_pages_read());
                    if consumed == 0 || consumed > matches.len() {
                        self.state = IndexScanState::Done;
                        return Poll::Ready(Some(Err(DataFusionError::Internal(
                            "covering index gather returned an invalid candidate count".to_string(),
                        ))));
                    }
                    let remaining = matches.split_off(consumed);
                    let output = match self.process_gather(gathered.batch(), &matches) {
                        Ok(output) => output,
                        Err(error) => {
                            self.state = IndexScanState::Done;
                            return Poll::Ready(Some(Err(error)));
                        }
                    };
                    if remaining.is_empty() {
                        self.state = IndexScanState::Initialize;
                    } else if let Err(error) = self.begin_gather(remaining) {
                        self.state = IndexScanState::Done;
                        return Poll::Ready(Some(Err(error)));
                    }
                    let Some(mut output) = output else {
                        context.waker().wake_by_ref();
                        return Poll::Pending;
                    };
                    if let Some(limit) = self.limit {
                        let available = limit.saturating_sub(self.emitted_rows);
                        if output.num_rows() > available {
                            output = output.slice(0, available);
                            self.state = IndexScanState::Done;
                        }
                    }
                    self.emitted_rows = self.emitted_rows.saturating_add(output.num_rows());
                    self.metrics.baseline.record_output(output.num_rows());
                    Poll::Ready(Some(Ok(output)))
                }
            },
            IndexScanState::Done => Poll::Ready(None),
        }
    }
}

impl datafusion_physical_plan::RecordBatchStream for CayenneIndexScanStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

fn visible_mask(
    view: &CoveringReadView,
    batch: &RecordBatch,
    matches: &[ProbeMatch],
) -> CoveringResult<BooleanArray> {
    if batch.num_rows() != matches.len() {
        return Err(Error::InvalidContract {
            message: format!(
                "covered gather returned {} rows for {} probe matches",
                batch.num_rows(),
                matches.len()
            ),
        });
    }
    let mut rows_by_source = BTreeMap::<_, Vec<usize>>::new();
    for (row, matched) in matches.iter().enumerate() {
        rows_by_source
            .entry(matched.row_ref().source().clone())
            .or_default()
            .push(row);
    }
    let mut keep = vec![false; matches.len()];
    for (source, rows) in rows_by_source {
        let indices = UInt32Array::from(
            rows.iter()
                .map(|row| {
                    u32::try_from(*row).map_err(|_| Error::Overflow {
                        operation: "covered visibility row index",
                    })
                })
                .collect::<CoveringResult<Vec<_>>>()?,
        );
        let source_batch = arrow::compute::take_record_batch(batch, &indices)
            .map_err(|source| Error::Arrow { source })?;
        let ordinals = rows
            .iter()
            .map(|row| matches[*row].row_ref().source_row_ordinal())
            .collect::<Vec<_>>();
        for source_row in view.select_visible_rows(&source, &source_batch, &ordinals)? {
            let original = *rows.get(source_row).ok_or_else(|| Error::InvalidContract {
                message: "visibility returned a row outside its covered source batch".to_string(),
            })?;
            keep[original] = true;
        }
    }
    Ok(BooleanArray::from(keep))
}

fn apply_static_filters(
    mut batch: RecordBatch,
    filters: &[Arc<dyn PhysicalExpr>],
) -> Result<RecordBatch> {
    for filter in filters {
        if batch.num_rows() == 0 {
            break;
        }
        let value = filter.evaluate(&batch)?;
        let array = value.into_array_of_size(batch.num_rows())?;
        let mask = array
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "Cayenne covering filter returned {:?}, expected Boolean",
                    array.data_type()
                ))
            })?;
        // Arrow's filter kernel treats a NULL mask value as false, matching SQL
        // WHERE semantics: only TRUE admits a row.
        batch = arrow::compute::filter_record_batch(&batch, mask)?;
    }
    Ok(batch)
}

fn project_output(
    batch: &RecordBatch,
    projection: &[usize],
    schema: SchemaRef,
) -> Result<RecordBatch> {
    if projection.is_empty() {
        return RecordBatch::try_new_with_options(
            schema,
            Vec::new(),
            &RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
        )
        .map_err(DataFusionError::from);
    }
    let projected = batch.project(projection)?;
    if projected.schema_ref().as_ref() == schema.as_ref() {
        return Ok(projected);
    }
    RecordBatch::try_new_with_options(
        schema,
        projected.columns().to_vec(),
        &RecordBatchOptions::new().with_row_count(Some(projected.num_rows())),
    )
    .map_err(DataFusionError::from)
}

fn covering_error(error: &Error) -> DataFusionError {
    DataFusionError::Execution(format!("Failed to read Cayenne covering index: {error}"))
}
