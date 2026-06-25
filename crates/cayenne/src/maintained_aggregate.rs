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

//! Maintained aggregate state for append-heavy Cayenne CDC streams.
//!
//! The implementation follows a conservative DBSP-style delta contract: rows
//! are applied as positive deltas only while the view is known fresh. Any
//! operation that needs a retraction but cannot provide the old row values marks
//! the view stale. The physical optimizer may only serve this state when its
//! freshness epoch exactly matches the scan snapshot epoch captured by
//! [`crate::provider::CayenneAccelerationExec`].

use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, BooleanArray, RecordBatch, new_empty_array};
use arrow_schema::{DataType, FieldRef, SchemaRef};
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::error::Result as DataFusionResult;
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode};
use datafusion_common::{DataFusionError, ScalarValue};
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_expr::expressions::{CastExpr, Column, Literal};
use datafusion_physical_expr::{Distribution, OrderingRequirements};
use datafusion_physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use parking_lot::RwLock;

/// A Cayenne-maintained aggregate view declaration.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct MaintainedAggregateSpec {
    /// Input columns used as the `GROUP BY` key, in query output order.
    pub group_by: Vec<String>,
    /// Aggregate expressions maintained for each group.
    pub aggregates: Vec<MaintainedAggregateExpr>,
    /// Optional row predicate (a physical expression over the table/input
    /// schema) that selects which rows contribute to the view — the maintained
    /// equivalent of a query `WHERE`. `None` maintains the aggregate over every
    /// row (the original behavior). When set, maintenance applies only rows the
    /// predicate selects, and the optimizer serves a query from this view only
    /// when the query's filter matches this predicate exactly (see
    /// [`MaintainedAggregateView::matches_query`]). This is what lets the
    /// flagship serve filtered analytical queries (e.g. CH-benCH q1/q6) that
    /// every general-purpose engine must re-scan O(rows) for, while Cayenne
    /// maintains the filtered relation from the CDC delta and serves O(groups).
    pub filter: Option<Arc<dyn PhysicalExpr>>,
}

/// One aggregate expression inside a maintained aggregate view.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct MaintainedAggregateExpr {
    /// Aggregate function to maintain.
    pub function: MaintainedAggregateFunction,
    /// Input column for `SUM`, `AVG`, and `COUNT(column)`. `None` means
    /// `COUNT(*)` and is valid only for [`MaintainedAggregateFunction::Count`].
    pub column: Option<String>,
}

/// Aggregate functions supported by Cayenne maintained aggregate views.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MaintainedAggregateFunction {
    /// SQL `COUNT(*)` or `COUNT(column)`.
    Count,
    /// SQL `SUM(column)` over the signed-integer (`Int8`..`Int64`),
    /// unsigned-integer (`UInt8`..`UInt64`), or floating-point
    /// (`Float32`/`Float64`) families. Narrower widths widen losslessly to the
    /// `BIGINT`/`Float64` sum output, matching `DataFusion`'s `SUM` output type.
    Sum,
    /// SQL `AVG(column)` for `Float32`/`Float64` inputs.
    Avg,
}

/// Shared maintained aggregate state for a single Cayenne table.
#[derive(Debug)]
pub struct MaintainedAggregateRegistry {
    state: RwLock<RegistryState>,
    /// Upper bound on total per-PK index entries across all views. When the
    /// retraction index would exceed this, the registry fails safe to `Stale`
    /// and clears its indexes (queries fall back to the base table until the
    /// next rebuild), keeping memory bounded under `runtime.query.memory_limit`.
    /// `usize::MAX` for registries built without a PK (no index is maintained).
    max_index_entries: usize,
    /// Whether a per-PK index is maintained (a non-empty PK was configured), so
    /// UPDATE/DELETE can be retracted incrementally rather than marking stale.
    has_pk_index: bool,
}

#[derive(Debug)]
struct RegistryState {
    epoch: u64,
    status: RegistryStatus,
    views: Vec<MaintainedAggregateView>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RegistryStatus {
    Fresh,
    Stale,
}

#[derive(Debug)]
struct MaintainedAggregateView {
    spec: ResolvedAggregateSpec,
    /// Optional row predicate over the input schema. When set, only rows the
    /// predicate selects are folded into `groups`/`pk_index`; a non-matching row
    /// is treated exactly as an absent row (not indexed, not accumulated), so all
    /// retraction logic is reused unchanged. See [`MaintainedAggregateSpec::filter`].
    filter: Option<Arc<dyn PhysicalExpr>>,
    groups: HashMap<Vec<ScalarValue>, GroupAccumulator>,
    /// Primary-key column indices in the input batch. Empty means no per-PK
    /// index is maintained, so retraction is unavailable and the legacy
    /// insert-only / mark-stale-on-delete behavior applies.
    pk_columns: Vec<usize>,
    /// Per-PK contribution index: `pk -> (group key, captured per-aggregate
    /// inputs)`. Lets an UPDATE/DELETE retract the exact old contribution in
    /// O(1) WITHOUT a CDC before-image (the old value is read from the index,
    /// keyed by the primary key every CDC source delivers). Empty when
    /// `pk_columns` is empty.
    pk_index: HashMap<Vec<ScalarValue>, RowEntry>,
}

/// One row's retraction record: which group it joined and the per-aggregate
/// input scalars it contributed (so a retraction subtracts exactly).
#[derive(Debug)]
struct RowEntry {
    group_key: Vec<ScalarValue>,
    inputs: Vec<Option<ScalarValue>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ResolvedAggregateSpec {
    group_by: Vec<ResolvedColumn>,
    aggregates: Vec<ResolvedAggregateExpr>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ResolvedColumn {
    name: String,
    index: usize,
    data_type: DataType,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ResolvedAggregateExpr {
    function: MaintainedAggregateFunction,
    column: Option<ResolvedColumn>,
    output_type: AggregateOutputType,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AggregateOutputType {
    Count,
    Int64,
    UInt64,
    Float64,
}

impl AggregateOutputType {
    fn matches_field(self, field: &FieldRef) -> bool {
        match self {
            Self::Count | Self::Int64 => field.data_type() == &DataType::Int64,
            Self::UInt64 => field.data_type() == &DataType::UInt64,
            Self::Float64 => field.data_type() == &DataType::Float64,
        }
    }
}

#[derive(Debug, Clone)]
struct GroupAccumulator {
    /// Live row count for this group. A group is dropped when it reaches 0 so
    /// a fully-retracted group disappears (SQL `GROUP BY` emits no row for an
    /// empty group).
    rows: u64,
    aggregates: Vec<AggregateAccumulator>,
}

#[derive(Debug, Clone)]
enum AggregateAccumulator {
    CountAll {
        value: i64,
    },
    CountColumn {
        column_index: usize,
        value: i64,
    },
    SumInt64 {
        column_index: usize,
        value: Option<i64>,
    },
    SumUInt64 {
        column_index: usize,
        value: Option<u64>,
    },
    SumFloat64 {
        column_index: usize,
        value: Option<f64>,
    },
    AvgFloat64 {
        column_index: usize,
        sum: f64,
        count: i64,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct QueryAggregateSpec {
    group_by: Vec<String>,
    aggregates: Vec<QueryAggregateExpr>,
    /// The query's row predicate (captured from a `FilterExec` between the
    /// aggregate and the Cayenne scan), or `None` for an unfiltered query. A
    /// view serves the query only when this matches the view's own filter
    /// exactly — a filtered view must never answer an unfiltered query, and vice
    /// versa, or the result would be wrong.
    filter: Option<Arc<dyn PhysicalExpr>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct QueryAggregateExpr {
    function: MaintainedAggregateFunction,
    column: Option<String>,
}

enum CountQueryColumn {
    AllRows,
    Column(String),
}

/// Execution plan returned by the maintained aggregate optimizer rewrite.
#[derive(Debug)]
pub struct MaintainedAggregateExec {
    inner: Arc<dyn ExecutionPlan>,
}

impl MaintainedAggregateExec {
    /// Create a maintained aggregate execution plan from a materialized batch.
    ///
    /// # Errors
    ///
    /// Returns an error if the in-memory execution plan cannot be built.
    pub fn try_new(batch: RecordBatch) -> DataFusionResult<Self> {
        let schema = batch.schema();
        let inner = MemorySourceConfig::try_new_exec(&[vec![batch]], schema, None)?;
        Ok(Self { inner })
    }
}

impl DisplayAs for MaintainedAggregateExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "MaintainedAggregateExec")
    }
}

impl ExecutionPlan for MaintainedAggregateExec {
    fn name(&self) -> &'static str {
        "MaintainedAggregateExec"
    }

    fn static_name() -> &'static str
    where
        Self: Sized,
    {
        "MaintainedAggregateExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.inner.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.inner]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Internal(format!(
                "MaintainedAggregateExec expected one child, got {}",
                children.len()
            )));
        }

        let Some(inner) = children.pop() else {
            return Err(DataFusionError::Internal(
                "MaintainedAggregateExec expected one child".to_string(),
            ));
        };
        Ok(Arc::new(Self { inner }))
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::UnspecifiedDistribution; self.children().len()]
    }

    fn required_input_ordering(&self) -> Vec<Option<OrderingRequirements>> {
        vec![None; self.children().len()]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true; self.children().len()]
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false; self.children().len()]
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        self.inner.execute(partition, context)
    }
}

impl MaintainedAggregateRegistry {
    /// Create a registry for the provided aggregate view specs.
    ///
    /// # Errors
    ///
    /// Returns an error when a spec references a missing column or an aggregate
    /// type Cayenne cannot maintain exactly.
    pub fn try_new(
        specs: &[MaintainedAggregateSpec],
        schema: &SchemaRef,
    ) -> DataFusionResult<Self> {
        Self::try_new_inner(specs, schema, &[], usize::MAX)
    }

    /// As [`Self::try_new`], but maintains a per-PK contribution index keyed on
    /// `pk_columns` so UPDATE/DELETE can be retracted incrementally (see
    /// [`Self::apply_pk_deletes`]). `max_index_entries` bounds the index across all
    /// views; exceeding it fails the registry safe to `Stale`.
    ///
    /// # Errors
    ///
    /// Returns an error when a spec references a missing column or an aggregate
    /// type Cayenne cannot maintain exactly.
    pub fn try_new_with_pk(
        specs: &[MaintainedAggregateSpec],
        schema: &SchemaRef,
        pk_columns: &[usize],
        max_index_entries: usize,
    ) -> DataFusionResult<Self> {
        Self::try_new_inner(specs, schema, pk_columns, max_index_entries)
    }

    fn try_new_inner(
        specs: &[MaintainedAggregateSpec],
        schema: &SchemaRef,
        pk_columns: &[usize],
        max_index_entries: usize,
    ) -> DataFusionResult<Self> {
        let has_pk_index = !pk_columns.is_empty();
        let views = specs
            .iter()
            .map(|spec| MaintainedAggregateView::try_new(spec, schema, pk_columns.to_vec()))
            .collect::<DataFusionResult<Vec<_>>>()?;

        Ok(Self {
            state: RwLock::new(RegistryState {
                epoch: 0,
                status: RegistryStatus::Fresh,
                views,
            }),
            max_index_entries,
            has_pk_index,
        })
    }

    /// Returns true when no maintained aggregate views are configured.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.state.read().views.is_empty()
    }

    /// Whether this registry maintains a per-PK index and can therefore retract
    /// UPDATE/DELETE incrementally ([`Self::apply_pk_deletes`] for deletes, the
    /// retract-old-then-insert path in [`Self::apply_insert_batches`] for
    /// updates) rather than falling back to a full rebuild via [`Self::mark_stale`].
    #[must_use]
    pub fn supports_retraction(&self) -> bool {
        self.has_pk_index
    }

    /// Mark all maintained aggregate views stale at `epoch`.
    pub fn mark_stale(&self, epoch: u64) {
        let mut state = self.state.write();
        state.epoch = epoch;
        state.status = RegistryStatus::Stale;
    }

    /// Apply positive row deltas if the state is fresh, otherwise keep it stale.
    /// Bounds memory: if the per-PK index would exceed its cap the indexes are
    /// cleared and the registry fails safe to stale.
    ///
    /// # Errors
    ///
    /// Returns an error (after clearing the indexes and marking the registry
    /// stale) when a maintained accumulator overflows, Arrow scalar extraction
    /// fails, or the per-PK index exceeds its entry cap. Queries then fall back
    /// to base-table scans until the next rebuild.
    pub fn apply_insert_batches(
        &self,
        epoch: u64,
        batches: &[RecordBatch],
    ) -> DataFusionResult<()> {
        let mut state = self.state.write();

        // Async write-path maintenance must apply deltas in strict
        // visibility-epoch order; an out-of-order/skipped epoch or an
        // already-stale registry short-circuits (see `begin_maintenance_pass`).
        if !begin_maintenance_pass(&mut state, epoch) {
            return Ok(());
        }

        let mut failure: Option<DataFusionError> = None;
        'outer: for batch in batches {
            for view in &mut state.views {
                if let Err(error) = view.apply_insert_batch(batch) {
                    failure = Some(error);
                    break 'outer;
                }
            }
        }

        finalize_maintenance_pass(&mut state, self.max_index_entries, failure)
    }

    /// Retract delete rows whose primary-key columns are supplied directly as
    /// the columns of `pk_batch`, in `pk_columns` order (positions `0..n`). The
    /// caller resolves the PK columns BY NAME from the CDC delete batch, so this
    /// is independent of that batch's source-schema column layout. Requires a PK
    /// index ([`Self::try_new_with_pk`]).
    ///
    /// # Errors
    ///
    /// Returns an error (after clearing the indexes and marking the registry
    /// stale) when retraction fails. An out-of-order epoch silently marks stale
    /// and returns `Ok`.
    pub fn apply_pk_deletes(&self, epoch: u64, pk_batch: &RecordBatch) -> DataFusionResult<()> {
        let mut state = self.state.write();

        if !begin_maintenance_pass(&mut state, epoch) {
            return Ok(());
        }

        let mut failure: Option<DataFusionError> = None;
        for view in &mut state.views {
            if let Err(error) = view.retract_pk_batch(pk_batch) {
                failure = Some(error);
                break;
            }
        }

        finalize_maintenance_pass(&mut state, self.max_index_entries, failure)
    }

    /// Rebuild every view from a complete table snapshot. Bounds memory: the
    /// per-PK index is checked against its cap after each batch, so rebuilding a
    /// table larger than `max_index_entries` fails safe to stale (clearing the
    /// indexes) instead of growing the index unbounded.
    ///
    /// # Errors
    ///
    /// Returns an error (after clearing the indexes and marking the registry
    /// stale) if a maintained accumulator overflows, Arrow scalar extraction
    /// fails, or the per-PK index exceeds its entry cap.
    pub fn rebuild_from_batches(
        &self,
        epoch: u64,
        batches: &[RecordBatch],
    ) -> DataFusionResult<()> {
        let mut state = self.state.write();
        state.epoch = epoch;
        state.status = RegistryStatus::Fresh;
        for view in &mut state.views {
            view.clear();
        }
        let mut failure: Option<DataFusionError> = None;
        'outer: for batch in batches {
            for view in &mut state.views {
                if let Err(error) = view.apply_insert_batch(batch) {
                    failure = Some(error);
                    break 'outer;
                }
            }
            // Bail incrementally so a table larger than the cap fails safe to
            // stale before the per-PK index grows unbounded (rather than only
            // after the full rebuild, which could OOM first).
            if state
                .views
                .iter()
                .map(MaintainedAggregateView::index_len)
                .sum::<usize>()
                > self.max_index_entries
            {
                failure = Some(index_cap_exceeded());
                break 'outer;
            }
        }
        finalize_maintenance_pass(&mut state, self.max_index_entries, failure)
    }

    /// Materialize a maintained aggregate batch matching `aggregate`, if fresh.
    ///
    /// Returns `None` when the aggregate shape is unsupported, no declared view
    /// matches it, or the registry is stale for the scan snapshot epoch.
    ///
    /// # Errors
    ///
    /// Returns an error if a matching maintained view cannot be materialized.
    pub fn batch_for_aggregate(
        &self,
        aggregate: &AggregateExec,
        scan_epoch: u64,
    ) -> DataFusionResult<Option<RecordBatch>> {
        self.batch_for_aggregate_with_output(aggregate, aggregate, scan_epoch, None)
    }

    /// Materialize a maintained aggregate batch by matching `query_aggregate`
    /// while using `output_aggregate` as the returned batch schema.
    ///
    /// This is used for `DataFusion`'s split aggregate plans: the partial
    /// aggregate still names the original input columns, while the final
    /// aggregate carries the user-visible output schema.
    ///
    /// # Errors
    ///
    /// Returns an error if a matching maintained view cannot be materialized.
    pub fn batch_for_aggregate_with_output(
        &self,
        query_aggregate: &AggregateExec,
        output_aggregate: &AggregateExec,
        scan_epoch: u64,
        filter: Option<Arc<dyn PhysicalExpr>>,
    ) -> DataFusionResult<Option<RecordBatch>> {
        let Some(mut query) = query_spec_for_aggregate(query_aggregate) else {
            return Ok(None);
        };
        query.filter = filter;
        self.serve(&query, scan_epoch, output_aggregate.schema())
    }

    /// Serve a maintained view directly from a declared [`MaintainedAggregateSpec`]
    /// (group-by + aggregates + optional filter) into `output_schema`, without an
    /// `AggregateExec`. Exercises the exact fresh/epoch gate, view match (incl.
    /// filter equality), and O(groups) materialize the optimizer rewrite uses —
    /// the entry point for benches/tests that measure the maintained serve cost.
    ///
    /// # Errors
    ///
    /// Returns an error if a matching maintained view cannot be materialized.
    pub fn batch_for_spec(
        &self,
        spec: &MaintainedAggregateSpec,
        scan_epoch: u64,
        output_schema: SchemaRef,
    ) -> DataFusionResult<Option<RecordBatch>> {
        let query = QueryAggregateSpec {
            group_by: spec.group_by.clone(),
            aggregates: spec
                .aggregates
                .iter()
                .map(|aggregate| QueryAggregateExpr {
                    function: aggregate.function,
                    column: aggregate.column.clone(),
                })
                .collect(),
            filter: spec.filter.clone(),
        };
        self.serve(&query, scan_epoch, output_schema)
    }

    /// Shared serve path: only answer from a maintained view when the registry is
    /// fresh at the scan epoch and a view matches the query shape exactly.
    fn serve(
        &self,
        query: &QueryAggregateSpec,
        scan_epoch: u64,
        output_schema: SchemaRef,
    ) -> DataFusionResult<Option<RecordBatch>> {
        let state = self.state.read();
        if state.status != RegistryStatus::Fresh || state.epoch != scan_epoch {
            return Ok(None);
        }

        for view in &state.views {
            if view.matches_query(query) {
                // Moved on the diverging return path — at most one view matches,
                // so no clone of the schema is needed.
                return view.materialize(output_schema);
            }
        }

        Ok(None)
    }
}

impl MaintainedAggregateView {
    fn try_new(
        spec: &MaintainedAggregateSpec,
        schema: &SchemaRef,
        pk_columns: Vec<usize>,
    ) -> DataFusionResult<Self> {
        // A filter is a `WHERE` condition, so it must evaluate to Boolean.
        // Validate at construction so a non-Boolean predicate fails fast here with
        // a clear error, rather than later in `evaluate_filter_mask` with an
        // internal error. This guards every caller — config, tests, benches, and
        // any future programmatic construction.
        if let Some(filter) = &spec.filter {
            let data_type = filter.data_type(schema)?;
            if data_type != DataType::Boolean {
                return Err(DataFusionError::Plan(format!(
                    "maintained aggregate filter must be a Boolean predicate, but it evaluates to {data_type}"
                )));
            }
        }
        Ok(Self {
            spec: ResolvedAggregateSpec::try_new(spec, schema)?,
            filter: spec.filter.clone(),
            groups: HashMap::new(),
            pk_columns,
            pk_index: HashMap::new(),
        })
    }

    /// Evaluate the view's row predicate over `batch`, returning a per-row
    /// boolean mask. `None` (no filter configured) means every row contributes.
    /// SQL `WHERE` semantics: a row contributes only when the predicate is
    /// exactly `TRUE` (a `NULL`/`FALSE` result excludes it).
    fn evaluate_filter_mask(&self, batch: &RecordBatch) -> DataFusionResult<Option<BooleanArray>> {
        let Some(filter) = &self.filter else {
            return Ok(None);
        };
        let array = match filter.evaluate(batch)? {
            ColumnarValue::Array(array) => array,
            scalar @ ColumnarValue::Scalar(_) => scalar.into_array(batch.num_rows())?,
        };
        let mask = array
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "maintained aggregate filter must evaluate to Boolean, got {}",
                    array.data_type()
                ))
            })?;
        Ok(Some(mask.clone()))
    }

    /// Fold one row into its group, creating the group accumulator on first use.
    fn insert_into_group(
        &mut self,
        group_key: Vec<ScalarValue>,
        batch: &RecordBatch,
        row: usize,
    ) -> DataFusionResult<()> {
        let group = match self.groups.entry(group_key) {
            Entry::Occupied(entry) => entry.into_mut(),
            Entry::Vacant(entry) => entry.insert(GroupAccumulator::try_new(&self.spec)?),
        };
        group.apply_insert_row(batch, row)
    }

    fn clear(&mut self) {
        self.groups.clear();
        self.pk_index.clear();
    }

    fn index_len(&self) -> usize {
        self.pk_index.len()
    }

    /// Build a key (group key or PK) from the given column indices at `row`.
    fn scalar_key(
        batch: &RecordBatch,
        row: usize,
        indices: impl Iterator<Item = usize>,
    ) -> DataFusionResult<Vec<ScalarValue>> {
        indices
            .map(|index| ScalarValue::try_from_array(batch.column(index), row))
            .collect()
    }

    /// Capture each aggregate's input scalar at `row` for the per-PK index
    /// (`None` for `COUNT(*)`, which has no input column).
    fn capture_inputs(
        &self,
        batch: &RecordBatch,
        row: usize,
    ) -> DataFusionResult<Vec<Option<ScalarValue>>> {
        self.spec
            .aggregates
            .iter()
            .map(|aggregate| match &aggregate.column {
                None => Ok(None),
                Some(column) => {
                    let scalar = ScalarValue::try_from_array(batch.column(column.index), row)?;
                    // A NULL aggregate input contributes nothing; store `None`
                    // rather than a typed-NULL scalar (smaller, and retraction
                    // treats both identically as "contributed nothing").
                    Ok((!scalar.is_null()).then_some(scalar))
                }
            })
            .collect()
    }

    /// Subtract a stored row's contribution from its group, dropping the group
    /// when it becomes empty.
    fn retract_entry(&mut self, entry: &RowEntry) -> DataFusionResult<()> {
        let Some(group) = self.groups.get_mut(&entry.group_key) else {
            return Ok(());
        };
        if group.retract_row(&entry.inputs)? {
            self.groups.remove(&entry.group_key);
        }
        Ok(())
    }

    /// Retract the row currently indexed at `pk`, if any. Idempotent: a PK not
    /// in the index contributed nothing, so retraction is a no-op.
    fn retract_pk(&mut self, pk: &[ScalarValue]) -> DataFusionResult<()> {
        if let Some(entry) = self.pk_index.remove(pk) {
            self.retract_entry(&entry)?;
        }
        Ok(())
    }

    /// Retract every row of `pk_batch`, whose columns ARE this view's primary-key
    /// columns in `pk_columns` order (positions `0..num_columns`). Mirrors the
    /// keys built by [`Self::apply_insert_batch`] (PK scalars in `pk_columns`
    /// order), so the caller must project the CDC delete batch to exactly those
    /// columns, by name, in that order — independent of its source-schema layout.
    /// Requires a PK index.
    fn retract_pk_batch(&mut self, pk_batch: &RecordBatch) -> DataFusionResult<()> {
        if self.pk_columns.is_empty() {
            return Err(DataFusionError::Internal(
                "maintained aggregate retraction requires a configured primary key".to_string(),
            ));
        }
        for row in 0..pk_batch.num_rows() {
            let pk = Self::scalar_key(pk_batch, row, 0..pk_batch.num_columns())?;
            self.retract_pk(&pk)?;
        }
        Ok(())
    }

    fn apply_insert_batch(&mut self, batch: &RecordBatch) -> DataFusionResult<()> {
        if batch.num_rows() == 0 {
            return Ok(());
        }

        // `None` mask => every row contributes (unfiltered view, original path).
        let mask = self.evaluate_filter_mask(batch)?;
        let indexed = !self.pk_columns.is_empty();
        for row in 0..batch.num_rows() {
            let matches = mask
                .as_ref()
                .is_none_or(|mask| mask.is_valid(row) && mask.value(row));

            // Indexed views: an upsert whose PK is already indexed is an UPDATE,
            // so retract the prior contribution first. This runs even when the new
            // row no longer matches the filter, so a row updated OUT of the
            // predicate correctly drops its old contribution. `None` for
            // insert-only (no PK) views, which never retract.
            let pk = if indexed {
                let pk = Self::scalar_key(batch, row, self.pk_columns.iter().copied())?;
                if let Some(old) = self.pk_index.remove(&pk) {
                    self.retract_entry(&old)?;
                }
                Some(pk)
            } else {
                None
            };

            // A non-matching row contributes nothing and is left unindexed —
            // identical to an absent row, so a later DELETE/UPDATE retraction is a
            // correct no-op or re-add (its prior contribution was retracted above).
            if !matches {
                continue;
            }

            let group_key =
                Self::scalar_key(batch, row, self.spec.group_by.iter().map(|c| c.index))?;
            if let Some(pk) = pk {
                let inputs = self.capture_inputs(batch, row)?;
                self.pk_index.insert(
                    pk,
                    RowEntry {
                        group_key: group_key.clone(),
                        inputs,
                    },
                );
            }
            self.insert_into_group(group_key, batch, row)?;
        }

        Ok(())
    }

    fn matches_query(&self, query: &QueryAggregateSpec) -> bool {
        // Filter must match EXACTLY: an unfiltered view (filter `None`) answers
        // only unfiltered queries; a filtered view answers only a query carrying
        // the identical predicate. `Arc<dyn PhysicalExpr>` compares structurally
        // (DataFusion's `DynEq`), so two equivalent predicates over the same
        // schema match. A mismatch (or an unrecognized predicate) falls back to
        // the base-table scan — correct, just not accelerated.
        //
        // BOUNDARY (known limitation): the comparison is index- and type-sensitive
        // (`Column{index}`, typed `Literal`). The view's filter is parsed against
        // the table schema (config time) while the query's filter is the
        // `FilterExec` predicate captured from the physical plan. If a projection
        // or type-coercion sits between the scan and the filter (e.g. a
        // `SchemaCastScanExec` reordering columns or advertising `Utf8View` over a
        // stored `Utf8`), the predicates differ structurally and this returns
        // `false`, so the view SILENTLY does not serve and the query re-scans. A
        // future slice can normalize both predicates to a schema-independent
        // (column-name + canonical-literal) form before comparison; until then,
        // declare the filter so it matches the query's scan-output predicate.
        self.filter == query.filter
            && self
                .spec
                .group_by
                .iter()
                .map(|c| c.name.as_str())
                .eq(query.group_by.iter().map(String::as_str))
            && self.spec.aggregates.len() == query.aggregates.len()
            && self
                .spec
                .aggregates
                .iter()
                .zip(&query.aggregates)
                .all(|(declared, requested)| {
                    declared.function == requested.function
                        && declared.column.as_ref().map(|c| c.name.as_str())
                            == requested.column.as_deref()
                })
    }

    fn materialize(&self, schema: SchemaRef) -> DataFusionResult<Option<RecordBatch>> {
        if !self.output_schema_matches(&schema) {
            return Ok(None);
        }

        let mut rows = Vec::with_capacity(self.groups.len());
        if self.groups.is_empty() && self.spec.group_by.is_empty() {
            rows.push((Vec::new(), GroupAccumulator::try_new(&self.spec)?));
        } else {
            rows.extend(
                self.groups
                    .iter()
                    .map(|(key, acc)| (key.clone(), acc.clone())),
            );
        }

        let output_columns = schema
            .fields()
            .iter()
            .enumerate()
            .map(|(field_index, field)| {
                if rows.is_empty() {
                    return Ok(new_empty_array(field.data_type()));
                }

                let scalars = if field_index < self.spec.group_by.len() {
                    rows.iter()
                        .map(|(key, _)| {
                            key.get(field_index).cloned().ok_or_else(|| {
                                DataFusionError::Internal(
                                    "maintained aggregate group key index out of bounds"
                                        .to_string(),
                                )
                            })
                        })
                        .collect::<DataFusionResult<Vec<_>>>()?
                } else {
                    let aggregate_index = field_index - self.spec.group_by.len();
                    rows.iter()
                        .map(|(_, acc)| acc.scalar_value(aggregate_index, field))
                        .collect::<DataFusionResult<Vec<_>>>()?
                };

                ScalarValue::iter_to_array(scalars)
            })
            .collect::<DataFusionResult<Vec<ArrayRef>>>()?;

        RecordBatch::try_new(schema, output_columns)
            .map(Some)
            .map_err(|source| DataFusionError::ArrowError(Box::new(source), None))
    }

    fn output_schema_matches(&self, schema: &SchemaRef) -> bool {
        if schema.fields().len() != self.spec.group_by.len() + self.spec.aggregates.len() {
            return false;
        }

        for (field, group_column) in schema.fields().iter().zip(&self.spec.group_by) {
            if field.data_type() != &group_column.data_type {
                return false;
            }
        }

        self.spec
            .aggregates
            .iter()
            .zip(schema.fields().iter().skip(self.spec.group_by.len()))
            .all(|(aggregate, field)| aggregate.output_type.matches_field(field))
    }
}

impl ResolvedAggregateSpec {
    fn try_new(spec: &MaintainedAggregateSpec, schema: &SchemaRef) -> DataFusionResult<Self> {
        let group_by = spec
            .group_by
            .iter()
            .map(|column| resolve_column(schema, column))
            .collect::<DataFusionResult<Vec<_>>>()?;
        for column in &group_by {
            if !is_supported_group_key_type(&column.data_type) {
                return Err(DataFusionError::Plan(format!(
                    "Maintained aggregate GROUP BY column '{}' uses unsupported type {}",
                    column.name, column.data_type
                )));
            }
        }

        let aggregates = spec
            .aggregates
            .iter()
            .map(|aggregate| ResolvedAggregateExpr::try_new(aggregate, schema))
            .collect::<DataFusionResult<Vec<_>>>()?;

        Ok(Self {
            group_by,
            aggregates,
        })
    }
}

impl ResolvedAggregateExpr {
    fn try_new(expr: &MaintainedAggregateExpr, schema: &SchemaRef) -> DataFusionResult<Self> {
        let column = expr
            .column
            .as_ref()
            .map(|column| resolve_column(schema, column))
            .transpose()?;

        let output_type = match (expr.function, column.as_ref().map(|c| &c.data_type)) {
            (MaintainedAggregateFunction::Count, _) => AggregateOutputType::Count,
            // SQL `SUM(int)` widens to `BIGINT` (DataFusion's `Int64`/`UInt64`
            // sum output), so the whole signed/unsigned integer family is summed
            // exactly via lossless i64/u64 widening — Postgres `INTEGER` (arrow
            // `Int32`) is the common CDC case, not `BIGINT` (`Int64`).
            (MaintainedAggregateFunction::Sum, Some(data_type))
                if data_type.is_signed_integer() =>
            {
                AggregateOutputType::Int64
            }
            (MaintainedAggregateFunction::Sum, Some(data_type))
                if data_type.is_unsigned_integer() =>
            {
                AggregateOutputType::UInt64
            }
            // `SUM`/`AVG` over floating-point widen to `Float64` (DataFusion's
            // float sum/avg output type); `Float32` widens losslessly.
            (
                MaintainedAggregateFunction::Sum | MaintainedAggregateFunction::Avg,
                Some(data_type),
            ) if is_maintainable_float(data_type) => AggregateOutputType::Float64,
            (MaintainedAggregateFunction::Sum | MaintainedAggregateFunction::Avg, None) => {
                return Err(DataFusionError::Plan(format!(
                    "{:?} maintained aggregate requires a column",
                    expr.function
                )));
            }
            (function, Some(data_type)) => {
                return Err(DataFusionError::Plan(format!(
                    "{function:?} maintained aggregate does not support column type {data_type}"
                )));
            }
        };

        Ok(Self {
            function: expr.function,
            column,
            output_type,
        })
    }
}

impl GroupAccumulator {
    fn try_new(spec: &ResolvedAggregateSpec) -> DataFusionResult<Self> {
        let aggregates = spec
            .aggregates
            .iter()
            .map(AggregateAccumulator::try_new)
            .collect::<DataFusionResult<Vec<_>>>()?;
        Ok(Self {
            rows: 0,
            aggregates,
        })
    }

    fn apply_insert_row(&mut self, batch: &RecordBatch, row: usize) -> DataFusionResult<()> {
        for aggregate in &mut self.aggregates {
            aggregate.apply_insert_row(batch, row)?;
        }
        // `checked_add` (not saturating): a silently-clamped counter would break
        // the "drop the group when its last row is retracted" invariant, so an
        // overflow must fail the registry safe to stale instead.
        self.rows = self.rows.checked_add(1).ok_or_else(count_overflow)?;
        Ok(())
    }

    /// Subtract a previously-captured row's per-aggregate contributions
    /// (inverse of [`Self::apply_insert_row`]). Returns whether the group is
    /// now empty so the caller can drop it.
    fn retract_row(&mut self, inputs: &[Option<ScalarValue>]) -> DataFusionResult<bool> {
        for (aggregate, input) in self.aggregates.iter_mut().zip(inputs) {
            aggregate.retract_row(input.as_ref())?;
        }
        // `checked_sub` (not saturating): if retractions ever outnumber inserts
        // for a group (index/state inconsistency), surface it as an error so the
        // caller fails safe to stale rather than silently clamping at 0 and
        // mis-dropping the group.
        self.rows = self.rows.checked_sub(1).ok_or_else(retract_underflow)?;
        Ok(self.rows == 0)
    }

    fn scalar_value(
        &self,
        aggregate_index: usize,
        field: &FieldRef,
    ) -> DataFusionResult<ScalarValue> {
        let Some(aggregate) = self.aggregates.get(aggregate_index) else {
            return Err(DataFusionError::Internal(
                "maintained aggregate output index out of bounds".to_string(),
            ));
        };
        aggregate.scalar_value(field)
    }
}

impl AggregateAccumulator {
    fn try_new(expr: &ResolvedAggregateExpr) -> DataFusionResult<Self> {
        let accumulator = match (expr.function, expr.output_type, expr.column.as_ref()) {
            (MaintainedAggregateFunction::Count, _, None) => Self::CountAll { value: 0 },
            (MaintainedAggregateFunction::Count, _, Some(column)) => Self::CountColumn {
                column_index: column.index,
                value: 0,
            },
            (MaintainedAggregateFunction::Sum, AggregateOutputType::Int64, Some(column)) => {
                Self::SumInt64 {
                    column_index: column.index,
                    value: None,
                }
            }
            (MaintainedAggregateFunction::Sum, AggregateOutputType::UInt64, Some(column)) => {
                Self::SumUInt64 {
                    column_index: column.index,
                    value: None,
                }
            }
            (MaintainedAggregateFunction::Sum, AggregateOutputType::Float64, Some(column)) => {
                Self::SumFloat64 {
                    column_index: column.index,
                    value: None,
                }
            }
            (MaintainedAggregateFunction::Avg, AggregateOutputType::Float64, Some(column)) => {
                Self::AvgFloat64 {
                    column_index: column.index,
                    sum: 0.0,
                    count: 0,
                }
            }
            _ => {
                return Err(DataFusionError::Internal(format!(
                    "invalid maintained aggregate accumulator state: {expr:?}"
                )));
            }
        };
        Ok(accumulator)
    }

    fn apply_insert_row(&mut self, batch: &RecordBatch, row: usize) -> DataFusionResult<()> {
        match self {
            Self::CountAll { value } => {
                *value = value.checked_add(1).ok_or_else(count_overflow)?;
            }
            Self::CountColumn {
                column_index,
                value,
            } => {
                if !batch.column(*column_index).is_null(row) {
                    *value = value.checked_add(1).ok_or_else(count_overflow)?;
                }
            }
            Self::SumInt64 {
                column_index,
                value,
            } => {
                if !batch.column(*column_index).is_null(row) {
                    let scalar = ScalarValue::try_from_array(batch.column(*column_index), row)?;
                    let delta = scalar_as_i64(&scalar)?;
                    *value = Some(match *value {
                        Some(current) => current.checked_add(delta).ok_or_else(sum_overflow)?,
                        None => delta,
                    });
                }
            }
            Self::SumUInt64 {
                column_index,
                value,
            } => {
                if !batch.column(*column_index).is_null(row) {
                    let scalar = ScalarValue::try_from_array(batch.column(*column_index), row)?;
                    let delta = scalar_as_u64(&scalar)?;
                    *value = Some(match *value {
                        Some(current) => current.checked_add(delta).ok_or_else(sum_overflow)?,
                        None => delta,
                    });
                }
            }
            Self::SumFloat64 {
                column_index,
                value,
            } => {
                if !batch.column(*column_index).is_null(row) {
                    let scalar = ScalarValue::try_from_array(batch.column(*column_index), row)?;
                    let delta = scalar_as_f64(&scalar)?;
                    *value = Some(value.unwrap_or(0.0) + delta);
                }
            }
            Self::AvgFloat64 {
                column_index,
                sum,
                count,
            } => {
                if !batch.column(*column_index).is_null(row) {
                    let scalar = ScalarValue::try_from_array(batch.column(*column_index), row)?;
                    let delta = scalar_as_f64(&scalar)?;
                    *sum += delta;
                    *count = count.checked_add(1).ok_or_else(count_overflow)?;
                }
            }
        }
        Ok(())
    }

    /// Inverse of [`Self::apply_insert_row`], subtracting a previously-captured
    /// input scalar. `COUNT`/`SUM(Int64|UInt64)` are exactly invertible;
    /// `SUM/AVG(Float64)` subtract and rely on a periodic
    /// [`MaintainedAggregateRegistry::rebuild_from_batches`] to bound float
    /// drift. A null input contributed nothing, so it retracts nothing.
    fn retract_row(&mut self, input: Option<&ScalarValue>) -> DataFusionResult<()> {
        match self {
            Self::CountAll { value } => {
                *value = value.checked_sub(1).ok_or_else(retract_underflow)?;
            }
            Self::CountColumn { value, .. } => {
                if input.is_some_and(|scalar| !scalar.is_null()) {
                    *value = value.checked_sub(1).ok_or_else(retract_underflow)?;
                }
            }
            Self::SumInt64 { value, .. } => {
                if let Some(scalar) = input
                    && !scalar.is_null()
                {
                    let delta = scalar_as_i64(scalar)?;
                    let current = (*value).ok_or_else(retract_underflow)?;
                    *value = Some(current.checked_sub(delta).ok_or_else(sum_overflow)?);
                }
            }
            Self::SumUInt64 { value, .. } => {
                if let Some(scalar) = input
                    && !scalar.is_null()
                {
                    let delta = scalar_as_u64(scalar)?;
                    let current = (*value).ok_or_else(retract_underflow)?;
                    *value = Some(current.checked_sub(delta).ok_or_else(retract_underflow)?);
                }
            }
            Self::SumFloat64 { value, .. } => {
                if let Some(scalar) = input
                    && !scalar.is_null()
                {
                    let delta = scalar_as_f64(scalar)?;
                    *value = Some(value.unwrap_or(0.0) - delta);
                }
            }
            Self::AvgFloat64 { sum, count, .. } => {
                if let Some(scalar) = input
                    && !scalar.is_null()
                {
                    let delta = scalar_as_f64(scalar)?;
                    *sum -= delta;
                    *count = count.checked_sub(1).ok_or_else(retract_underflow)?;
                }
            }
        }
        Ok(())
    }

    fn scalar_value(&self, field: &FieldRef) -> DataFusionResult<ScalarValue> {
        match self {
            Self::CountAll { value } | Self::CountColumn { value, .. } => {
                scalar_for_field(field, Some(ScalarValue::Int64(Some(*value))))
            }
            Self::SumInt64 { value, .. } => {
                scalar_for_field(field, Some(ScalarValue::Int64(*value)))
            }
            Self::SumUInt64 { value, .. } => {
                scalar_for_field(field, Some(ScalarValue::UInt64(*value)))
            }
            Self::SumFloat64 { value, .. } => {
                scalar_for_field(field, Some(ScalarValue::Float64(*value)))
            }
            Self::AvgFloat64 { sum, count, .. } => {
                if *count == 0 {
                    scalar_for_field(field, Some(ScalarValue::Float64(None)))
                } else {
                    let count_f64 = exact_i64_to_f64(*count)?;
                    scalar_for_field(field, Some(ScalarValue::Float64(Some(*sum / count_f64))))
                }
            }
        }
    }
}

/// Whether an `AggregateExec`'s shape (independent of its aggregation mode) is one
/// the maintained-aggregate machinery can serve: no LIMIT folded into the aggregate,
/// no per-aggregate FILTER, and a single non-`GROUPING SET` grouping. The accepted
/// `AggregateMode`s differ by call site, so the mode gate is checked separately.
pub(crate) fn aggregate_shape_is_maintainable(aggregate: &AggregateExec) -> bool {
    aggregate.limit_options().is_none()
        && aggregate.filter_expr().iter().all(Option::is_none)
        && !aggregate.group_expr().has_grouping_set()
        && aggregate.group_expr().groups().len() == 1
}

/// The `Column` an aggregate input expression ultimately references, seeing
/// through the numeric-widening `CAST` that `DataFusion`'s type coercion inserts
/// for an aggregate over a narrow column (`SUM(Int32)` is planned as
/// `SUM(CAST(col AS Int64))`). The maintained view widens the same way and
/// `output_schema_matches` still guards the result type, so recovering the
/// column name through the coercion cast is sound.
fn aggregate_input_column(expr: &Arc<dyn PhysicalExpr>) -> Option<&Column> {
    if let Some(column) = expr.downcast_ref::<Column>() {
        return Some(column);
    }
    expr.downcast_ref::<CastExpr>()
        .and_then(|cast| cast.expr().downcast_ref::<Column>())
}

fn query_spec_for_aggregate(aggregate: &AggregateExec) -> Option<QueryAggregateSpec> {
    if !matches!(
        aggregate.mode(),
        AggregateMode::Single | AggregateMode::SinglePartitioned | AggregateMode::Partial
    ) || !aggregate_shape_is_maintainable(aggregate)
    {
        return None;
    }

    let input_schema = aggregate.input().schema();
    let mut group_by = Vec::with_capacity(aggregate.group_expr().expr().len());
    for (expr, _) in aggregate.group_expr().expr() {
        let column = expr.downcast_ref::<Column>()?;
        group_by.push(input_column_name(&input_schema, column)?);
    }

    let mut aggregates = Vec::with_capacity(aggregate.aggr_expr().len());
    for aggregate_expr in aggregate.aggr_expr() {
        if aggregate_expr.is_distinct()
            || !aggregate_expr.order_bys().is_empty()
            || aggregate_expr.is_reversed()
        {
            return None;
        }
        let function = match aggregate_expr.fun().name().to_ascii_lowercase().as_str() {
            "count" => MaintainedAggregateFunction::Count,
            "sum" => MaintainedAggregateFunction::Sum,
            "avg" => MaintainedAggregateFunction::Avg,
            _ => return None,
        };

        let expressions = aggregate_expr.expressions();
        let column = match function {
            MaintainedAggregateFunction::Count => {
                let column = count_column_for_query(&input_schema, &expressions)?;
                match column {
                    CountQueryColumn::AllRows => None,
                    CountQueryColumn::Column(column) => Some(column),
                }
            }
            MaintainedAggregateFunction::Sum | MaintainedAggregateFunction::Avg => {
                if expressions.len() != 1 {
                    return None;
                }
                let expr = expressions.first()?;
                let column = aggregate_input_column(expr)?;
                Some(input_column_name(&input_schema, column)?)
            }
        };

        aggregates.push(QueryAggregateExpr { function, column });
    }

    Some(QueryAggregateSpec {
        group_by,
        aggregates,
        // The aggregate node carries no filter; the optimizer captures any
        // `FilterExec` predicate during plan descent and sets it via
        // `batch_for_aggregate_with_output`.
        filter: None,
    })
}

fn count_column_for_query(
    input_schema: &SchemaRef,
    expressions: &[Arc<dyn datafusion_physical_expr::PhysicalExpr>],
) -> Option<CountQueryColumn> {
    if expressions.len() > 1 {
        return None;
    }
    let Some(expr) = expressions.first() else {
        return Some(CountQueryColumn::AllRows);
    };
    if let Some(column) = expr.downcast_ref::<Column>() {
        return Some(CountQueryColumn::Column(input_column_name(
            input_schema,
            column,
        )?));
    }
    if let Some(literal) = expr.downcast_ref::<Literal>() {
        return (!literal.value().is_null()).then_some(CountQueryColumn::AllRows);
    }
    None
}

fn input_column_name(input_schema: &SchemaRef, column: &Column) -> Option<String> {
    input_schema
        .fields()
        .get(column.index())
        .map(|field| field.name().clone())
}

fn exact_i64_to_f64(value: i64) -> DataFusionResult<f64> {
    const MAX_EXACT_F64_INTEGER: i64 = 1_i64 << f64::MANTISSA_DIGITS;
    if !(-MAX_EXACT_F64_INTEGER..=MAX_EXACT_F64_INTEGER).contains(&value) {
        return Err(DataFusionError::Execution(format!(
            "Maintained aggregate AVG count {value} exceeds the exact Float64 integer range"
        )));
    }

    // `value` is now within ±2^53 (`f64::MANTISSA_DIGITS`), the range over which
    // every integer is exactly representable as `f64`, so the cast is lossless.
    #[expect(
        clippy::cast_precision_loss,
        reason = "value is range-checked to ±2^53 above, where i64 -> f64 is exact"
    )]
    Ok(value as f64)
}

fn resolve_column(schema: &SchemaRef, name: &str) -> DataFusionResult<ResolvedColumn> {
    let Some((index, field)) = schema.column_with_name(name) else {
        return Err(DataFusionError::Plan(format!(
            "Maintained aggregate column '{name}' was not found in table schema"
        )));
    };
    Ok(ResolvedColumn {
        name: name.to_string(),
        index,
        data_type: field.data_type().clone(),
    })
}

fn is_supported_group_key_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Date32
            | DataType::Date64
            | DataType::Timestamp(_, _)
    )
}

/// Floating-point types a maintained `SUM`/`AVG` can fold exactly into an
/// `f64` accumulator. Deliberately NARROWER than arrow's `DataType::is_floating`
/// (which also matches `Float16`): the accumulator path has no `Float16`
/// support, so this must not admit it. (Signed/unsigned-integer acceptance uses
/// arrow's `DataType::is_signed_integer`/`is_unsigned_integer` directly.)
fn is_maintainable_float(data_type: &DataType) -> bool {
    matches!(data_type, DataType::Float32 | DataType::Float64)
}

/// Coerce a non-null signed-integer input scalar to `i64`, widening the
/// `Int8`/`Int16`/`Int32`/`Int64` family losslessly. SQL `SUM(int)` widens to
/// `BIGINT` (`DataFusion`'s `Int64` sum output), so a narrower CDC column
/// (Postgres `INTEGER` → arrow `Int32`) is summed exactly without overflow.
fn scalar_as_i64(scalar: &ScalarValue) -> DataFusionResult<i64> {
    match scalar {
        ScalarValue::Int64(Some(v)) => Ok(*v),
        ScalarValue::Int32(Some(v)) => Ok(i64::from(*v)),
        ScalarValue::Int16(Some(v)) => Ok(i64::from(*v)),
        ScalarValue::Int8(Some(v)) => Ok(i64::from(*v)),
        _ => Err(type_mismatch("a signed integer", scalar)),
    }
}

/// Coerce a non-null unsigned-integer input scalar to `u64`, widening the
/// `UInt8`/`UInt16`/`UInt32`/`UInt64` family losslessly.
fn scalar_as_u64(scalar: &ScalarValue) -> DataFusionResult<u64> {
    match scalar {
        ScalarValue::UInt64(Some(v)) => Ok(*v),
        ScalarValue::UInt32(Some(v)) => Ok(u64::from(*v)),
        ScalarValue::UInt16(Some(v)) => Ok(u64::from(*v)),
        ScalarValue::UInt8(Some(v)) => Ok(u64::from(*v)),
        _ => Err(type_mismatch("an unsigned integer", scalar)),
    }
}

/// Coerce a non-null floating-point input scalar to `f64`, widening `Float32`
/// to `Float64` losslessly (`DataFusion`'s float sum/avg output type).
fn scalar_as_f64(scalar: &ScalarValue) -> DataFusionResult<f64> {
    match scalar {
        ScalarValue::Float64(Some(v)) => Ok(*v),
        ScalarValue::Float32(Some(v)) => Ok(f64::from(*v)),
        _ => Err(type_mismatch("a floating-point", scalar)),
    }
}

fn scalar_for_field(
    field: &FieldRef,
    scalar: Option<ScalarValue>,
) -> DataFusionResult<ScalarValue> {
    let scalar = match scalar {
        Some(scalar) => scalar,
        None => ScalarValue::try_from(field.data_type())?,
    };
    if scalar.data_type() == *field.data_type() {
        return Ok(scalar);
    }
    Err(DataFusionError::Internal(format!(
        "Maintained aggregate scalar type {} did not match output field type {}",
        scalar.data_type(),
        field.data_type()
    )))
}

/// Enforce the strict per-epoch ordering at the start of a mutating pass.
/// Returns `true` to proceed, `false` (after updating `state`) to short-circuit:
/// an out-of-order/skipped epoch clears the indexes and marks the registry stale
/// — freeing the now-unservable index for the rest of the table lifetime, since
/// it will not serve again until a rebuild — and an already-stale or empty
/// registry simply returns. Shared by `apply_insert_batches`/`apply_pk_deletes`.
fn begin_maintenance_pass(state: &mut RegistryState, epoch: u64) -> bool {
    if epoch != state.epoch.saturating_add(1) {
        state.epoch = state.epoch.max(epoch);
        state.status = RegistryStatus::Stale;
        for view in &mut state.views {
            view.clear();
        }
        return false;
    }
    state.epoch = epoch;
    !(state.status == RegistryStatus::Stale || state.views.is_empty())
}

/// Finalize a maintenance pass: if `failure` is set, or the per-PK index now
/// exceeds `max_index_entries`, clear every index, mark the registry stale, and
/// return the reason (so the write-path applier can log it); otherwise the
/// registry stays fresh. Centralizes the fail-safe across the insert, PK-delete,
/// and rebuild paths so memory is bounded on every mutating path.
fn finalize_maintenance_pass(
    state: &mut RegistryState,
    max_index_entries: usize,
    failure: Option<DataFusionError>,
) -> DataFusionResult<()> {
    let over_cap = state
        .views
        .iter()
        .map(MaintainedAggregateView::index_len)
        .sum::<usize>()
        > max_index_entries;
    if failure.is_some() || over_cap {
        for view in &mut state.views {
            view.clear();
        }
        state.status = RegistryStatus::Stale;
        return Err(failure.unwrap_or_else(index_cap_exceeded));
    }
    Ok(())
}

fn index_cap_exceeded() -> DataFusionError {
    DataFusionError::Execution(
        "Maintained aggregate per-PK index exceeded its entry cap; falling back to base table scan"
            .to_string(),
    )
}

fn count_overflow() -> DataFusionError {
    DataFusionError::Execution(
        "Maintained aggregate COUNT overflowed Int64; falling back to base table scan".to_string(),
    )
}

fn sum_overflow() -> DataFusionError {
    DataFusionError::Execution(
        "Maintained aggregate SUM overflowed its output type; falling back to base table scan"
            .to_string(),
    )
}

fn retract_underflow() -> DataFusionError {
    DataFusionError::Execution(
        "Maintained aggregate retraction underflowed its state; falling back to base table scan"
            .to_string(),
    )
}

fn type_mismatch(expected: &'static str, scalar: &ScalarValue) -> DataFusionError {
    DataFusionError::Execution(format!(
        "Maintained aggregate expected {expected} input but received {scalar:?}"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    use arrow::array::{Float64Array, Int32Array, Int64Array, StringArray, UInt64Array};
    use arrow_schema::{Field, Schema};
    use datafusion::physical_expr::aggregate::AggregateExprBuilder;
    use datafusion::physical_expr::expressions::{cast, col, lit};
    use datafusion::physical_plan::aggregates::PhysicalGroupBy;
    use datafusion_common::cast::{
        as_float64_array, as_int64_array, as_string_array, as_uint64_array,
    };
    use datafusion_functions_aggregate::average::avg_udaf;
    use datafusion_functions_aggregate::count::count_udaf;
    use datafusion_functions_aggregate::sum::sum_udaf;

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("i", DataType::Int64, true),
            Field::new("u", DataType::UInt64, true),
            Field::new("f", DataType::Float64, true),
        ]))
    }

    fn batch() -> RecordBatch {
        RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(StringArray::from(vec![
                    Some("a"),
                    Some("a"),
                    Some("b"),
                    None,
                ])),
                Arc::new(Int64Array::from(vec![Some(10), None, Some(-2), Some(3)])),
                Arc::new(UInt64Array::from(vec![Some(10), None, Some(2), Some(3)])),
                Arc::new(Float64Array::from(vec![
                    Some(1.0),
                    None,
                    Some(2.5),
                    Some(4.5),
                ])),
            ],
        )
        .expect("test batch should be valid")
    }

    #[test]
    fn applies_null_aware_count_sum_and_avg() -> DataFusionResult<()> {
        let spec = MaintainedAggregateSpec {
            filter: None,
            group_by: vec!["name".to_string()],
            aggregates: vec![
                MaintainedAggregateExpr {
                    function: MaintainedAggregateFunction::Count,
                    column: None,
                },
                MaintainedAggregateExpr {
                    function: MaintainedAggregateFunction::Count,
                    column: Some("i".to_string()),
                },
                MaintainedAggregateExpr {
                    function: MaintainedAggregateFunction::Sum,
                    column: Some("i".to_string()),
                },
                MaintainedAggregateExpr {
                    function: MaintainedAggregateFunction::Avg,
                    column: Some("f".to_string()),
                },
            ],
        };
        let registry = MaintainedAggregateRegistry::try_new(&[spec], &schema())?;
        registry.apply_insert_batches(1, &[batch()])?;

        let aggregate = aggregate_exec_for(&[
            ("count(*)", MaintainedAggregateFunction::Count, None),
            ("count(i)", MaintainedAggregateFunction::Count, Some("i")),
            ("sum(i)", MaintainedAggregateFunction::Sum, Some("i")),
            ("avg(f)", MaintainedAggregateFunction::Avg, Some("f")),
        ])?;
        let result = registry
            .batch_for_aggregate(&aggregate, 1)?
            .expect("maintained aggregate should match");

        assert_eq!(result.num_rows(), 3);
        let groups = as_string_array(result.column(0))?;
        let count_all = as_int64_array(result.column(1))?;
        let count_i = as_int64_array(result.column(2))?;
        let sum_i = as_int64_array(result.column(3))?;
        let avg = as_float64_array(result.column(4))?;
        let mut rows = BTreeMap::new();
        for row in 0..result.num_rows() {
            let key = if groups.is_null(row) {
                None
            } else {
                Some(groups.value(row).to_string())
            };
            rows.insert(
                key,
                (
                    count_all.value(row),
                    count_i.value(row),
                    (!sum_i.is_null(row)).then_some(sum_i.value(row)),
                    (!avg.is_null(row)).then_some(avg.value(row)),
                ),
            );
        }
        assert_eq!(
            rows.get(&Some("a".to_string())),
            Some(&(2, 1, Some(10), Some(1.0)))
        );
        assert_eq!(
            rows.get(&Some("b".to_string())),
            Some(&(1, 1, Some(-2), Some(2.5)))
        );
        assert_eq!(rows.get(&None), Some(&(1, 1, Some(3), Some(4.5))));
        Ok(())
    }

    #[test]
    fn stale_epoch_does_not_serve() -> DataFusionResult<()> {
        let spec = MaintainedAggregateSpec {
            filter: None,
            group_by: vec!["name".to_string()],
            aggregates: vec![MaintainedAggregateExpr {
                function: MaintainedAggregateFunction::Count,
                column: None,
            }],
        };
        let registry = MaintainedAggregateRegistry::try_new(&[spec], &schema())?;
        registry.apply_insert_batches(1, &[batch()])?;
        registry.mark_stale(2);

        let aggregate =
            aggregate_exec_for(&[("count(*)", MaintainedAggregateFunction::Count, None)])?;
        assert!(registry.batch_for_aggregate(&aggregate, 2)?.is_none());
        assert!(registry.batch_for_aggregate(&aggregate, 1)?.is_none());
        Ok(())
    }

    /// The off-write-path background applier drains deltas in strict epoch order
    /// (channel FIFO). The registry enforces that contract as its safety net: an
    /// in-order epoch chain stays fresh and serves, but a skipped/out-of-order
    /// epoch (as a reordered or dropped delta would produce) fails safe to stale
    /// — queries fall back to the base table rather than serve a partial result.
    #[test]
    fn out_of_order_apply_epoch_falls_back_to_stale() -> DataFusionResult<()> {
        let spec = MaintainedAggregateSpec {
            filter: None,
            group_by: vec!["name".to_string()],
            aggregates: vec![MaintainedAggregateExpr {
                function: MaintainedAggregateFunction::Count,
                column: None,
            }],
        };
        let registry = MaintainedAggregateRegistry::try_new(&[spec], &schema())?;
        let aggregate =
            aggregate_exec_for(&[("count(*)", MaintainedAggregateFunction::Count, None)])?;

        // In-order epoch 1 applies and serves.
        registry.apply_insert_batches(1, &[batch()])?;
        assert!(
            registry.batch_for_aggregate(&aggregate, 1)?.is_some(),
            "in-order epoch 1 serves"
        );

        // Skipping epoch 2 and applying epoch 3 is a gap; the registry fails safe
        // to stale and serves nothing at any epoch.
        registry.apply_insert_batches(3, &[batch()])?;
        assert!(
            registry.batch_for_aggregate(&aggregate, 3)?.is_none(),
            "gapped epoch 3 must not serve"
        );
        assert!(
            registry.batch_for_aggregate(&aggregate, 1)?.is_none(),
            "registry is stale after the gap"
        );
        Ok(())
    }

    #[test]
    fn count_null_literal_is_not_rewritten_as_count_all() -> DataFusionResult<()> {
        let spec = MaintainedAggregateSpec {
            filter: None,
            group_by: vec!["name".to_string()],
            aggregates: vec![MaintainedAggregateExpr {
                function: MaintainedAggregateFunction::Count,
                column: None,
            }],
        };
        let schema = schema();
        let registry = MaintainedAggregateRegistry::try_new(&[spec], &schema)?;
        registry.apply_insert_batches(1, &[batch()])?;
        let input = MemorySourceConfig::try_new_exec(&[vec![batch()]], Arc::clone(&schema), None)?;
        let group_by =
            PhysicalGroupBy::new_single(vec![(col("name", schema.as_ref())?, "name".to_string())]);
        let aggregate_expr =
            AggregateExprBuilder::new(count_udaf(), vec![lit(ScalarValue::Int64(None))])
                .schema(Arc::clone(&schema))
                .alias("count(NULL)".to_string())
                .build()?;
        let aggregate = AggregateExec::try_new(
            AggregateMode::Single,
            group_by,
            vec![Arc::new(aggregate_expr)],
            vec![None],
            input,
            schema,
        )?;

        assert!(registry.batch_for_aggregate(&aggregate, 1)?.is_none());
        Ok(())
    }

    #[test]
    fn global_empty_aggregate_returns_sql_row() -> DataFusionResult<()> {
        let spec = MaintainedAggregateSpec {
            filter: None,
            group_by: vec![],
            aggregates: vec![
                MaintainedAggregateExpr {
                    function: MaintainedAggregateFunction::Count,
                    column: None,
                },
                MaintainedAggregateExpr {
                    function: MaintainedAggregateFunction::Sum,
                    column: Some("u".to_string()),
                },
            ],
        };
        let registry = MaintainedAggregateRegistry::try_new(&[spec], &schema())?;
        let aggregate = aggregate_exec_for(&[
            ("count(*)", MaintainedAggregateFunction::Count, None),
            ("sum(u)", MaintainedAggregateFunction::Sum, Some("u")),
        ])?;

        let result = registry
            .batch_for_aggregate(&aggregate, 0)?
            .expect("global empty aggregate should be served");
        assert_eq!(result.num_rows(), 1);
        assert_eq!(
            ScalarValue::try_from_array(result.column(0), 0)?,
            ScalarValue::Int64(Some(0))
        );
        assert!(result.column(1).is_null(0));
        Ok(())
    }

    #[test]
    fn sum_overflow_returns_error() -> DataFusionResult<()> {
        let spec = MaintainedAggregateSpec {
            filter: None,
            group_by: vec![],
            aggregates: vec![MaintainedAggregateExpr {
                function: MaintainedAggregateFunction::Sum,
                column: Some("i".to_string()),
            }],
        };
        let registry = MaintainedAggregateRegistry::try_new(&[spec], &schema())?;
        let overflow_batch = RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(StringArray::from(vec![Some("a"), Some("b")])),
                Arc::new(Int64Array::from(vec![Some(i64::MAX), Some(1)])),
                Arc::new(UInt64Array::from(vec![Some(1), Some(1)])),
                Arc::new(Float64Array::from(vec![Some(1.0), Some(1.0)])),
            ],
        )
        .expect("test batch should be valid");

        assert!(registry.apply_insert_batches(1, &[overflow_batch]).is_err());
        Ok(())
    }

    #[test]
    fn unsupported_group_key_type_is_rejected() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "group_key",
            DataType::Float64,
            true,
        )]));
        let spec = MaintainedAggregateSpec {
            filter: None,
            group_by: vec!["group_key".to_string()],
            aggregates: vec![MaintainedAggregateExpr {
                function: MaintainedAggregateFunction::Count,
                column: None,
            }],
        };

        MaintainedAggregateRegistry::try_new(&[spec], &schema)
            .expect_err("unsupported group key type should be rejected");
    }

    #[test]
    fn maintained_aggregate_exec_exposes_inner_child() -> DataFusionResult<()> {
        let exec = Arc::new(MaintainedAggregateExec::try_new(batch())?);

        assert_eq!(exec.children().len(), 1);
        let required_distribution = exec.required_input_distribution();
        assert_eq!(required_distribution.len(), 1);
        assert!(matches!(
            required_distribution.as_slice(),
            [Distribution::UnspecifiedDistribution]
        ));
        let required_ordering = exec.required_input_ordering();
        assert_eq!(required_ordering.len(), 1);
        assert!(required_ordering[0].is_none());
        assert_eq!(exec.maintains_input_order(), vec![true]);
        assert_eq!(exec.benefits_from_input_partitioning(), vec![false]);
        Arc::clone(&exec)
            .with_new_children(Vec::new())
            .expect_err("missing maintained aggregate child should be rejected");

        let replacement = MemorySourceConfig::try_new_exec(&[vec![batch()]], schema(), None)?;
        let rewritten = exec.with_new_children(vec![replacement])?;
        assert_eq!(rewritten.children().len(), 1);

        Ok(())
    }

    fn aggregate_exec_for(
        aggregate_defs: &[(&str, MaintainedAggregateFunction, Option<&str>)],
    ) -> DataFusionResult<AggregateExec> {
        let schema = schema();
        let input = MemorySourceConfig::try_new_exec(&[vec![batch()]], Arc::clone(&schema), None)?;
        let group_by = if aggregate_defs.len() == 2
            && aggregate_defs[0].0 == "count(*)"
            && aggregate_defs[1].0 == "sum(u)"
        {
            PhysicalGroupBy::new_single(vec![])
        } else {
            PhysicalGroupBy::new_single(vec![(col("name", schema.as_ref())?, "name".to_string())])
        };
        let aggregate_exprs = aggregate_defs
            .iter()
            .map(|(alias, function, column)| {
                let aggregate_args = match (function, column) {
                    (_, Some(column)) => vec![col(column, schema.as_ref())?],
                    _ => vec![lit(1_i8)],
                };
                let udaf = match function {
                    MaintainedAggregateFunction::Count => count_udaf(),
                    MaintainedAggregateFunction::Sum => sum_udaf(),
                    MaintainedAggregateFunction::Avg => avg_udaf(),
                };
                AggregateExprBuilder::new(udaf, aggregate_args)
                    .schema(Arc::clone(&schema))
                    .alias((*alias).to_string())
                    .build()
                    .map(Arc::new)
            })
            .collect::<DataFusionResult<Vec<_>>>()?;
        let filters = vec![None; aggregate_exprs.len()];
        AggregateExec::try_new(
            AggregateMode::Single,
            group_by,
            aggregate_exprs,
            filters,
            input,
            schema,
        )
    }

    // --- retraction (apply_pk_deletes + per-PK index) ---
    //
    // Column layout for these tests (reusing the module `schema()`):
    //   name (Utf8)  -> GROUP BY key
    //   i    (Int64) -> SUM target  (output parses via `as_int64_array`)
    //   u    (UInt64)-> PRIMARY KEY (pk_columns = [2])
    // Each test row is `(name, pk, value)`.

    fn group_batch(rows: &[(&str, u64, i64)]) -> RecordBatch {
        RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(StringArray::from(
                    rows.iter().map(|(n, _, _)| Some(*n)).collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(
                    rows.iter().map(|(_, _, value)| *value).collect::<Vec<_>>(),
                )),
                Arc::new(UInt64Array::from(
                    rows.iter().map(|(_, pk, _)| *pk).collect::<Vec<_>>(),
                )),
                Arc::new(Float64Array::from(
                    rows.iter().map(|_| None).collect::<Vec<Option<f64>>>(),
                )),
            ],
        )
        .expect("group batch should be valid")
    }

    fn sum_i_spec() -> MaintainedAggregateSpec {
        MaintainedAggregateSpec {
            filter: None,
            group_by: vec!["name".to_string()],
            aggregates: vec![MaintainedAggregateExpr {
                function: MaintainedAggregateFunction::Sum,
                column: Some("i".to_string()),
            }],
        }
    }

    fn sum_i_by_name(
        registry: &MaintainedAggregateRegistry,
    ) -> DataFusionResult<BTreeMap<String, i64>> {
        let aggregate =
            aggregate_exec_for(&[("sum(i)", MaintainedAggregateFunction::Sum, Some("i"))])?;
        let epoch = registry.state.read().epoch;
        let result = registry
            .batch_for_aggregate(&aggregate, epoch)?
            .expect("registry should be fresh");
        let names = as_string_array(result.column(0))?;
        let sums = as_int64_array(result.column(1))?;
        let mut out = BTreeMap::new();
        for row in 0..result.num_rows() {
            if !sums.is_null(row) {
                out.insert(names.value(row).to_string(), sums.value(row));
            }
        }
        Ok(out)
    }

    #[test]
    fn retracts_a_subset_of_a_group_by_pk() -> DataFusionResult<()> {
        let registry = MaintainedAggregateRegistry::try_new_with_pk(
            &[sum_i_spec()],
            &schema(),
            &[2],
            usize::MAX,
        )?;
        registry.apply_insert_batches(
            1,
            &[group_batch(&[
                ("a", 1, 10),
                ("a", 2, 20),
                ("b", 3, 5),
                ("b", 4, 7),
            ])],
        )?;
        // Delete pk=2 (group a) and pk=3 (group b) via a PK-projected batch.
        registry.apply_pk_deletes(2, &group_batch(&[("", 2, 0), ("", 3, 0)]).project(&[2])?)?;

        let sums = sum_i_by_name(&registry)?;
        assert_eq!(sums.get("a"), Some(&10), "a retains only pk=1 (i=10)");
        assert_eq!(sums.get("b"), Some(&7), "b retains only pk=4 (i=7)");
        Ok(())
    }

    /// `apply_pk_deletes` retracts using a batch whose columns ARE the primary
    /// key in `pk_columns` order (positions `0..n`) — NOT the table-schema
    /// layout. This is the path the provider uses after projecting a CDC delete
    /// batch by name, so retraction matches the right keys regardless of the
    /// delete batch's source-schema column order. PK = column index 2 (`u`).
    #[test]
    fn apply_pk_deletes_retracts_by_projected_pk() -> DataFusionResult<()> {
        let registry = MaintainedAggregateRegistry::try_new_with_pk(
            &[sum_i_spec()],
            &schema(),
            &[2],
            usize::MAX,
        )?;
        registry.apply_insert_batches(
            1,
            &[group_batch(&[("a", 1, 10), ("a", 2, 20), ("b", 3, 5)])],
        )?;

        // A PK-only batch: one `u` (UInt64) column at position 0 — the table
        // layout has the PK at column 2, so a positional read of the full table
        // batch would pick the wrong column. apply_pk_deletes reads positions
        // `0..n`, matching the index keys built from the table PK column.
        let pk_only = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("u", DataType::UInt64, true)])),
            vec![Arc::new(UInt64Array::from(vec![2_u64, 3_u64]))],
        )
        .expect("pk batch should be valid");
        registry.apply_pk_deletes(2, &pk_only)?;

        let sums = sum_i_by_name(&registry)?;
        assert_eq!(
            sums.get("a"),
            Some(&10),
            "a keeps only pk=1 (i=10) after pk=2 retracted"
        );
        assert_eq!(
            sums.get("b"),
            None,
            "b fully retracted (pk=3 was its only row)"
        );
        Ok(())
    }

    #[test]
    fn handles_update_as_retract_then_insert() -> DataFusionResult<()> {
        let registry = MaintainedAggregateRegistry::try_new_with_pk(
            &[sum_i_spec()],
            &schema(),
            &[2],
            usize::MAX,
        )?;
        registry.apply_insert_batches(1, &[group_batch(&[("a", 1, 10), ("a", 2, 20)])])?;
        // UPDATE pk=2 in place: i 20 -> 5, so group a = 10 + 5 = 15.
        registry.apply_insert_batches(2, &[group_batch(&[("a", 2, 5)])])?;
        assert_eq!(sum_i_by_name(&registry)?.get("a"), Some(&15));

        // UPDATE pk=1 moving group a -> c, i 10 -> 100.
        registry.apply_insert_batches(3, &[group_batch(&[("c", 1, 100)])])?;
        let sums = sum_i_by_name(&registry)?;
        assert_eq!(sums.get("a"), Some(&5), "a keeps only pk=2 (i=5)");
        assert_eq!(sums.get("c"), Some(&100), "c gains pk=1 (i=100)");
        Ok(())
    }

    #[test]
    fn retracting_last_row_drops_the_group() -> DataFusionResult<()> {
        let registry = MaintainedAggregateRegistry::try_new_with_pk(
            &[sum_i_spec()],
            &schema(),
            &[2],
            usize::MAX,
        )?;
        registry.apply_insert_batches(1, &[group_batch(&[("a", 1, 10), ("b", 2, 20)])])?;
        registry.apply_pk_deletes(2, &group_batch(&[("", 1, 0)]).project(&[2])?)?;
        let sums = sum_i_by_name(&registry)?;
        assert_eq!(
            sums.get("a"),
            None,
            "group a disappears once its last row is retracted"
        );
        assert_eq!(sums.get("b"), Some(&20));
        Ok(())
    }

    #[test]
    fn exceeding_index_cap_falls_back_to_stale() -> DataFusionResult<()> {
        // A 1-entry cap: inserting two distinct PKs overflows the index, so the
        // registry fails safe to Stale and serves nothing (base-table fallback).
        let registry =
            MaintainedAggregateRegistry::try_new_with_pk(&[sum_i_spec()], &schema(), &[2], 1)?;
        // Exceeding the cap surfaces a concrete error (not a silent Ok) so the
        // write-path applier can log the reason.
        let result =
            registry.apply_insert_batches(1, &[group_batch(&[("a", 1, 10), ("a", 2, 20)])]);
        assert!(result.is_err(), "over-cap apply must return an error");
        let aggregate =
            aggregate_exec_for(&[("sum(i)", MaintainedAggregateFunction::Sum, Some("i"))])?;
        assert!(
            registry.batch_for_aggregate(&aggregate, 1)?.is_none(),
            "over-cap registry must not serve"
        );
        Ok(())
    }

    /// Build a (name, i=PK, u, f) batch — exercises every aggregate-input type
    /// so retraction covers all accumulator inverses. Float values are
    /// exact-representable in f64 so retraction stays bit-exact.
    fn typed_batch(rows: &[(&str, i64, u64, f64)]) -> RecordBatch {
        RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(StringArray::from(
                    rows.iter().map(|(n, _, _, _)| Some(*n)).collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(
                    rows.iter().map(|(_, i, _, _)| *i).collect::<Vec<_>>(),
                )),
                Arc::new(UInt64Array::from(
                    rows.iter().map(|(_, _, u, _)| *u).collect::<Vec<_>>(),
                )),
                Arc::new(Float64Array::from(
                    rows.iter().map(|(_, _, _, f)| *f).collect::<Vec<_>>(),
                )),
            ],
        )
        .expect("typed batch should be valid")
    }

    /// Retraction exercises EVERY accumulator inverse: `COUNT(*)` (`CountAll`),
    /// `COUNT(u)` (`CountColumn`), `SUM(u)` (`SumUInt64`), `SUM(f)` (`SumFloat64`),
    /// `AVG(f)` (`AvgFloat64`). `SUM(i)` (`SumInt64`) is covered by
    /// `retracts_a_subset_of_a_group_by_pk`. PK = column `i`.
    #[test]
    fn retracts_all_aggregate_types() -> DataFusionResult<()> {
        let spec = MaintainedAggregateSpec {
            filter: None,
            group_by: vec!["name".to_string()],
            aggregates: vec![
                MaintainedAggregateExpr {
                    function: MaintainedAggregateFunction::Count,
                    column: None,
                },
                MaintainedAggregateExpr {
                    function: MaintainedAggregateFunction::Count,
                    column: Some("u".to_string()),
                },
                MaintainedAggregateExpr {
                    function: MaintainedAggregateFunction::Sum,
                    column: Some("u".to_string()),
                },
                MaintainedAggregateExpr {
                    function: MaintainedAggregateFunction::Sum,
                    column: Some("f".to_string()),
                },
                MaintainedAggregateExpr {
                    function: MaintainedAggregateFunction::Avg,
                    column: Some("f".to_string()),
                },
            ],
        };
        let registry =
            MaintainedAggregateRegistry::try_new_with_pk(&[spec], &schema(), &[1], usize::MAX)?;
        registry.apply_insert_batches(
            1,
            &[typed_batch(&[
                ("a", 1, 10, 2.0),
                ("a", 2, 20, 4.0),
                ("b", 3, 5, 8.0),
            ])],
        )?;
        // Delete pk i=2 from group a via a PK-projected batch (PK = column 1).
        registry.apply_pk_deletes(2, &typed_batch(&[("", 2, 0, 0.0)]).project(&[1])?)?;

        let aggregate = aggregate_exec_for(&[
            ("count(*)", MaintainedAggregateFunction::Count, None),
            ("count(u)", MaintainedAggregateFunction::Count, Some("u")),
            ("sum(u)", MaintainedAggregateFunction::Sum, Some("u")),
            ("sum(f)", MaintainedAggregateFunction::Sum, Some("f")),
            ("avg(f)", MaintainedAggregateFunction::Avg, Some("f")),
        ])?;
        let result = registry
            .batch_for_aggregate(&aggregate, 2)?
            .expect("registry should be fresh");
        assert_eq!(result.num_rows(), 2, "two groups survive");

        let names = as_string_array(result.column(0))?;
        let count_all = as_int64_array(result.column(1))?;
        let count_u = as_int64_array(result.column(2))?;
        let sum_u = as_uint64_array(result.column(3))?;
        let sum_f = as_float64_array(result.column(4))?;
        let avg_f = as_float64_array(result.column(5))?;
        for row in 0..result.num_rows() {
            match names.value(row) {
                // group a kept only pk=1 (u=10, f=2.0) after the retraction.
                "a" => {
                    assert_eq!(count_all.value(row), 1, "CountAll retracted");
                    assert_eq!(count_u.value(row), 1, "CountColumn retracted");
                    assert_eq!(sum_u.value(row), 10, "SumUInt64 retracted");
                    assert!(
                        (sum_f.value(row) - 2.0).abs() < 1e-9,
                        "SumFloat64 retracted"
                    );
                    assert!(
                        (avg_f.value(row) - 2.0).abs() < 1e-9,
                        "AvgFloat64 retracted"
                    );
                }
                // group b untouched (pk=3, u=5, f=8.0).
                "b" => {
                    assert_eq!(count_all.value(row), 1);
                    assert_eq!(sum_u.value(row), 5);
                    assert!((sum_f.value(row) - 8.0).abs() < 1e-9);
                    assert!((avg_f.value(row) - 8.0).abs() < 1e-9);
                }
                other => panic!("unexpected group {other}"),
            }
        }
        Ok(())
    }

    /// Postgres `INTEGER` → arrow `Int32`, the common CDC case (not `BIGINT` →
    /// `Int64`). `SUM` over a narrow signed-integer column must (a) be accepted
    /// at registry construction and (b) widen to `Int64` on both the insert and
    /// the retraction path. Regression guard for the `CH-benCH` `district`
    /// `SUM(d_next_o_id)` config, where `d_next_o_id` is `Int32` — this combo
    /// previously failed planning with "Sum maintained aggregate does not
    /// support column type Int32".
    #[test]
    fn sum_over_int32_widens_on_insert_and_retract() -> DataFusionResult<()> {
        let i32_schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("v", DataType::Int32, true),
            Field::new("pk", DataType::Int64, true),
        ]));
        let i32_batch = |rows: &[(&str, i32, i64)]| {
            RecordBatch::try_new(
                Arc::clone(&i32_schema),
                vec![
                    Arc::new(StringArray::from(
                        rows.iter().map(|(n, _, _)| Some(*n)).collect::<Vec<_>>(),
                    )),
                    Arc::new(Int32Array::from(
                        rows.iter().map(|(_, v, _)| *v).collect::<Vec<_>>(),
                    )),
                    Arc::new(Int64Array::from(
                        rows.iter().map(|(_, _, pk)| *pk).collect::<Vec<_>>(),
                    )),
                ],
            )
            .expect("int32 batch should be valid")
        };

        let spec = MaintainedAggregateSpec {
            filter: None,
            group_by: vec!["name".to_string()],
            aggregates: vec![MaintainedAggregateExpr {
                function: MaintainedAggregateFunction::Sum,
                column: Some("v".to_string()),
            }],
        };
        // PK = column index 2 (`pk`). Construction must succeed for Int32 SUM.
        let registry =
            MaintainedAggregateRegistry::try_new_with_pk(&[spec], &i32_schema, &[2], usize::MAX)?;

        registry
            .apply_insert_batches(1, &[i32_batch(&[("a", 10, 1), ("a", 20, 2), ("b", 5, 3)])])?;
        // UPDATE pk=2 in place (retract-old-then-apply-new): v 20 -> 7, so a = 17.
        registry.apply_insert_batches(2, &[i32_batch(&[("a", 7, 2)])])?;
        // DELETE pk=3 via a PK-projected batch (PK = column 2): retracts group b.
        registry.apply_pk_deletes(3, &i32_batch(&[("", 0, 3)]).project(&[2])?)?;

        // Serve via a real AggregateExec. DataFusion's type coercion plans
        // `SUM(Int32)` as `SUM(CAST(v AS Int64))`, so the aggregate input is a
        // cast over the column — the serve path must see through it.
        let input = MemorySourceConfig::try_new_exec(
            &[vec![i32_batch(&[])]],
            Arc::clone(&i32_schema),
            None,
        )?;
        let sum_arg = cast(
            col("v", i32_schema.as_ref())?,
            i32_schema.as_ref(),
            DataType::Int64,
        )?;
        let sum_v = AggregateExprBuilder::new(sum_udaf(), vec![sum_arg])
            .schema(Arc::clone(&i32_schema))
            .alias("sum(v)")
            .build()
            .map(Arc::new)?;
        let aggregate = AggregateExec::try_new(
            AggregateMode::Single,
            PhysicalGroupBy::new_single(vec![(
                col("name", i32_schema.as_ref())?,
                "name".to_string(),
            )]),
            vec![sum_v],
            vec![None],
            input,
            Arc::clone(&i32_schema),
        )?;
        let result = registry
            .batch_for_aggregate(&aggregate, 3)?
            .expect("registry should be fresh");
        // Output widened to Int64 (matches DataFusion's `sum(Int32)` = `Int64`).
        let names = as_string_array(result.column(0))?;
        let sums = as_int64_array(result.column(1))?;
        let mut by_name = BTreeMap::new();
        for row in 0..result.num_rows() {
            if !sums.is_null(row) {
                by_name.insert(names.value(row).to_string(), sums.value(row));
            }
        }
        assert_eq!(
            by_name.get("a"),
            Some(&17),
            "group a = 10 + updated 7 (Int32 widened)"
        );
        assert_eq!(by_name.get("b"), None, "group b fully retracted by delete");
        Ok(())
    }

    /// Retracting a PK that was never inserted is an idempotent no-op (matches
    /// tombstone semantics — an absent row contributed nothing).
    #[test]
    fn retracting_unseen_pk_is_a_noop() -> DataFusionResult<()> {
        let registry = MaintainedAggregateRegistry::try_new_with_pk(
            &[sum_i_spec()],
            &schema(),
            &[2],
            usize::MAX,
        )?;
        registry.apply_insert_batches(1, &[group_batch(&[("a", 1, 10), ("b", 2, 20)])])?;
        // pk=999 was never inserted; the retraction must leave every group unchanged.
        registry.apply_pk_deletes(2, &group_batch(&[("", 999, 0)]).project(&[2])?)?;
        let sums = sum_i_by_name(&registry)?;
        assert_eq!(sums.get("a"), Some(&10));
        assert_eq!(sums.get("b"), Some(&20));
        Ok(())
    }
}
