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

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, BooleanArray, RecordBatch, new_empty_array};
use arrow::datatypes::Decimal128Type;
use arrow_schema::{DECIMAL128_MAX_PRECISION, DECIMAL128_MAX_SCALE, DataType, FieldRef, SchemaRef};
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::error::Result as DataFusionResult;
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode};
use datafusion_common::{DataFusionError, ScalarValue};
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use datafusion_functions_aggregate_common::utils::DecimalAverager;
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
    /// unsigned-integer (`UInt8`..`UInt64`), floating-point
    /// (`Float32`/`Float64`), or `Decimal128` families. Narrower integer/float
    /// widths widen losslessly to the `BIGINT`/`Float64` sum output, matching
    /// `DataFusion`'s `SUM` output type; `Decimal128(p, s)` sums its `i128`
    /// backing values exactly and widens the precision to
    /// `Decimal128(min(38, p + 10), s)` (`DataFusion`'s decimal `SUM` output).
    Sum,
    /// SQL `AVG(column)` over the signed-integer (`Int8`..`Int64`),
    /// unsigned-integer (`UInt8`..`UInt64`), floating-point
    /// (`Float32`/`Float64`), or non-negative-scale `Decimal128` families.
    /// Integer/float inputs output `Float64` (matching `DataFusion`'s `AVG`
    /// output type); integer inputs fold their running sum exactly into an
    /// `i128` accumulator, floats into an `f64` one. `Decimal128(p, s)` inputs
    /// output `Decimal128(min(38, p + 4), min(38, s + 4))` (`DataFusion`'s
    /// decimal `AVG` output), folding the exact `i128` backing-value sum and
    /// dividing down to the output scale only when served.
    Avg,
    /// SQL `MIN(column)` over signed/unsigned integers, `Date32`/`Date64`,
    /// `Timestamp`, and `Decimal128`. Unlike `SUM` (which widens to `BIGINT`),
    /// the output preserves the input type (`MIN(Int32) -> Int32`).
    /// Retraction-hard: deleting the current minimum needs the next-smallest
    /// value, so a per-group ordered multiset ([`SortedScalarIndex`]) keeps the
    /// live values. Float `MIN`/`MAX` (NaN ordering) is a follow-up.
    Min,
    /// SQL `MAX(column)` — the mirror of [`Self::Min`], reading the largest
    /// live value from the same ordered-multiset structure.
    Max,
}

/// Shared maintained aggregate state for a single Cayenne table.
#[derive(Debug)]
pub struct MaintainedAggregateRegistry {
    state: RwLock<RegistryState>,
    /// Upper bound on approximate resident BYTES retained across all views:
    /// per-PK contributions plus distinct `MIN`/`MAX` multiset values. When the
    /// total would exceed this, the registry fails safe to `Stale` and clears all
    /// retained state.
    ///
    /// Bytes, not entries: entry width varies by orders of magnitude with key and
    /// aggregate-input types, so a count cap bounds memory only for one schema
    /// shape. This is derived from `runtime.query.memory_limit` by the provider,
    /// so the index cannot grow past the operator's budget.
    max_index_bytes: usize,
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
    /// Exact number of distinct ordered-multiset nodes retained by this view.
    /// Updated on every `MIN`/`MAX` insert/retract so cap checks stay O(1)
    /// regardless of group cardinality.
    retained_multiset_entries: usize,
    /// Approximate resident bytes held by `pk_index`, maintained incrementally
    /// on every insert/retract. Tracked rather than computed because summing the
    /// map would be O(live rows) on every CDC batch.
    approx_pk_index_bytes: usize,
}

/// Approximate resident bytes one `MIN`/`MAX` ordered-multiset node costs: the
/// retained `ScalarValue`, its occurrence counter, and the node's container
/// overhead. Deliberately a flat estimate — the nodes are small and uniform,
/// unlike PK entries whose width varies with the key and captured inputs.
const APPROX_MULTISET_NODE_BYTES: usize = std::mem::size_of::<ScalarValue>() + 32;

/// Approximate resident bytes one `pk_index` entry costs: the key scalars, the
/// stored `RowEntry` (its group key and captured aggregate inputs), and the
/// `HashMap` slot overhead. Charges every component the map actually holds — an
/// estimate that drops one bounds the index at a fraction of its real size.
fn approx_pk_index_entry_bytes(pk: &[ScalarValue], entry: &RowEntry) -> usize {
    /// Allocator-dependent per-slot control/allocation overhead; kept next to the
    /// estimate it belongs to, as in `provider::pk_index`.
    const HASHMAP_ENTRY_OVERHEAD_BYTES: usize = 16;

    let pk_bytes = pk
        .iter()
        .fold(0_usize, |total, scalar| total.saturating_add(scalar.size()));
    let group_key_bytes = entry
        .group_key
        .iter()
        .fold(0_usize, |total, scalar| total.saturating_add(scalar.size()));
    let input_bytes = entry.inputs.iter().fold(0_usize, |total, input| {
        total.saturating_add(input.as_ref().map_or(
            std::mem::size_of::<Option<ScalarValue>>(),
            ScalarValue::size,
        ))
    });

    // `size_of::<RowEntry>()` covers the value's own inline width, including the
    // `Vec` headers of its group key and inputs. The *key* needs the same
    // treatment: `pk_bytes` sums only the scalars behind the pointer, so without
    // this the map's `Vec<ScalarValue>` header goes uncharged and every entry is
    // undercounted by a fixed amount — a systematic bias in the one direction
    // that matters, since it lets the index sit over budget while reporting
    // itself under.
    pk_bytes
        .saturating_add(std::mem::size_of::<Vec<ScalarValue>>())
        .saturating_add(group_key_bytes)
        .saturating_add(input_bytes)
        .saturating_add(std::mem::size_of::<RowEntry>())
        .saturating_add(HASHMAP_ENTRY_OVERHEAD_BYTES)
}

/// One row's retraction record: which group it joined and the per-aggregate
/// input scalars it contributed (so a retraction subtracts exactly).
#[derive(Debug)]
struct RowEntry {
    group_key: Vec<ScalarValue>,
    inputs: Vec<Option<ScalarValue>>,
}

type RetiredViewState = (
    HashMap<Vec<ScalarValue>, GroupAccumulator>,
    HashMap<Vec<ScalarValue>, RowEntry>,
);

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
    /// `SUM`/`AVG` over a `Decimal128` input. Unlike the fixed widened outputs
    /// above, the output precision/scale depend on the input type (`SUM(p, s)`
    /// -> `(min(38, p + 10), s)`; `AVG(p, s)` -> `(min(38, p + 4),
    /// min(38, s + 4))`, `DataFusion`'s decimal output types), so the resolved
    /// parameters are carried here.
    Decimal128 {
        precision: u8,
        scale: i8,
    },
    /// The output type equals the aggregate's input column type — `MIN`/`MAX`,
    /// which preserve type rather than widen. The concrete `DataType` lives on
    /// the [`ResolvedAggregateExpr`]'s resolved column, so field matching for
    /// this variant is done by [`ResolvedAggregateExpr::output_matches_field`].
    SameAsInput,
}

impl AggregateOutputType {
    fn matches_field(self, field: &FieldRef) -> bool {
        match self {
            Self::Count | Self::Int64 => field.data_type() == &DataType::Int64,
            Self::UInt64 => field.data_type() == &DataType::UInt64,
            Self::Float64 => field.data_type() == &DataType::Float64,
            Self::Decimal128 { precision, scale } => {
                field.data_type() == &DataType::Decimal128(precision, scale)
            }
            // Matched against the input column type in
            // `ResolvedAggregateExpr::output_matches_field`, never here.
            Self::SameAsInput => false,
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
        non_null_count: u64,
    },
    SumUInt64 {
        column_index: usize,
        value: Option<u64>,
        non_null_count: u64,
    },
    SumFloat64 {
        column_index: usize,
        value: Option<f64>,
        non_null_count: u64,
    },
    /// `SUM` over a `Decimal128(p, s)` column. The output keeps the input scale
    /// (only the precision widens, to `min(38, p + 10)`), so the running sum is
    /// the exact `i128` backing-value sum — exactly invertible on the retract
    /// path, like the integer sums. `precision`/`scale` are the *output* type
    /// parameters, carried so the served scalar is typed exactly. Unlike
    /// `SumInt64`'s `Option<i64>`, the SQL-`NULL` state is encoded as
    /// `non_null_count == 0` rather than an `Option<i128>`: an `Option<i128>`
    /// has no niche, and its extra 16 aligned bytes would grow *every*
    /// [`AggregateAccumulator`] (the enum takes the largest variant's size)
    /// for all views, decimal or not.
    SumDecimal128 {
        column_index: usize,
        value: i128,
        non_null_count: u64,
        precision: u8,
        scale: i8,
    },
    AvgFloat64 {
        column_index: usize,
        sum: f64,
        count: i64,
    },
    /// `AVG` over the signed/unsigned integer family. The running sum is folded
    /// exactly in `i128` (never materialized as an integer — always divided down
    /// to the `Float64` AVG output), so it is exactly invertible on the retract
    /// path and, unlike `SumInt64`'s `i64`, wide enough to average many values
    /// near `i64::MAX`/`u64::MAX` without overflowing.
    AvgInt128 {
        column_index: usize,
        sum: i128,
        count: i64,
    },
    /// `AVG` over a `Decimal128(p, sum_scale)` column. The running sum is the
    /// exact `i128` backing-value sum at the *input* scale (exactly invertible
    /// on the retract path, like [`Self::AvgInt128`]); only when served is it
    /// rescaled to `target_scale` and divided by the count — by `DataFusion`'s
    /// own `DecimalAverager`, so the quotient (truncation, precision
    /// validation, overflow behavior) is identical to a base-table re-scan by
    /// construction.
    AvgDecimal128 {
        column_index: usize,
        sum: i128,
        count: i64,
        sum_scale: i8,
        target_precision: u8,
        target_scale: i8,
    },
    /// SQL `MIN(column)`: the smallest live value in the group, read as the
    /// first key of a retraction-capable ordered multiset.
    Min {
        column_index: usize,
        index: SortedScalarIndex,
    },
    /// SQL `MAX(column)`: the largest live value, read as the last key of the
    /// same ordered-multiset structure.
    Max {
        column_index: usize,
        index: SortedScalarIndex,
    },
}

/// A per-group ordered multiset of the live (non-null) values feeding a
/// maintained `MIN`/`MAX` — the structure that makes those *retraction-hard*
/// aggregates incrementally maintainable. `COUNT`/`SUM`/`AVG` invert a
/// retraction by subtracting; `MIN`/`MAX` cannot — deleting the current
/// extremum needs the next value, which only a kept ordered structure has.
/// Keyed by a lossless `i128` order key over the whole signed/unsigned integer
/// family, so `BTreeMap` gives O(log distinct) insert/retract and O(1)
/// `MIN` (first key) / `MAX` (last key). Only non-null values are stored, so an
/// empty index means the extremum is SQL `NULL`. The exact input-typed
/// [`ScalarValue`] is stored (not the widened key) because the strict
/// [`scalar_for_field`] requires the output scalar to match the column type
/// exactly.
///
/// Memory bound: [`MaintainedAggregateView::index_len`] counts each distinct
/// multiset node in addition to any per-PK contribution record. The exact count
/// is maintained incrementally, so cap checks do not scan every group. The
/// runtime additionally rejects user-configured `MIN`/`MAX` without a primary
/// key because retraction cannot be supported there.
#[derive(Debug, Clone, Default)]
struct SortedScalarIndex {
    entries: BTreeMap<i128, (ScalarValue, u64)>,
}

impl SortedScalarIndex {
    /// Add one live value. `checked_add` on the per-value count matches the
    /// crate-wide "never silently clamp a maintained counter" discipline.
    /// Returns whether this inserted a new distinct map entry.
    fn insert(&mut self, scalar: ScalarValue) -> DataFusionResult<bool> {
        let key = scalar_order_key(&scalar)?;
        if let Some((_, count)) = self.entries.get_mut(&key) {
            *count = count.checked_add(1).ok_or_else(count_overflow)?;
            Ok(false)
        } else {
            self.entries.insert(key, (scalar, 1));
            Ok(true)
        }
    }

    /// Remove one live value (the inverse of [`Self::insert`]). A key that is
    /// absent, or a count that underflows, is a state inconsistency the caller
    /// turns into a fail-safe-to-stale, exactly like the additive retraction path.
    /// Returns whether this removed a distinct map entry.
    fn retract(&mut self, scalar: &ScalarValue) -> DataFusionResult<bool> {
        let key = scalar_order_key(scalar)?;
        let Some((_, count)) = self.entries.get_mut(&key) else {
            return Err(retract_underflow());
        };
        *count = count.checked_sub(1).ok_or_else(retract_underflow)?;
        if *count == 0 {
            self.entries.remove(&key);
            return Ok(true);
        }
        Ok(false)
    }

    /// The smallest live value (SQL `MIN`), or `None` when no non-null value
    /// remains (the group's `MIN` is `NULL`).
    fn min_scalar(&self) -> Option<ScalarValue> {
        self.entries
            .values()
            .next()
            .map(|(scalar, _)| scalar.clone())
    }

    /// The largest live value (SQL `MAX`), or `None` when the index is empty.
    fn max_scalar(&self) -> Option<ScalarValue> {
        self.entries
            .values()
            .next_back()
            .map(|(scalar, _)| scalar.clone())
    }
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
    /// [`Self::apply_pk_deletes`]). `max_index_bytes` bounds all retained index
    /// entries across the views; exceeding it fails the registry safe to `Stale`.
    ///
    /// # Errors
    ///
    /// Returns an error when a spec references a missing column or an aggregate
    /// type Cayenne cannot maintain exactly.
    pub fn try_new_with_pk(
        specs: &[MaintainedAggregateSpec],
        schema: &SchemaRef,
        pk_columns: &[usize],
        max_index_bytes: usize,
    ) -> DataFusionResult<Self> {
        Self::try_new_inner(specs, schema, pk_columns, max_index_bytes)
    }

    fn try_new_inner(
        specs: &[MaintainedAggregateSpec],
        schema: &SchemaRef,
        pk_columns: &[usize],
        max_index_bytes: usize,
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
            max_index_bytes,
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

    /// Whether the registry is currently stale, i.e. serving nothing and
    /// discarding every delta until a rebuild restores it.
    ///
    /// Staleness is a *recoverable* degradation, not a terminal state: every
    /// fail-safe path (cap exceeded, apply-queue overflow, accumulator overflow,
    /// epoch gap) lands here, and only a rebuild clears it. Callers poll this to
    /// drive that rebuild — without one, a single transient failure would disable
    /// maintained aggregates for the provider's whole lifetime.
    #[must_use]
    pub fn is_stale(&self) -> bool {
        self.state.read().status == RegistryStatus::Stale
    }

    /// Approximate resident bytes currently retained across every view, and the
    /// byte budget they are held to. Exposed for observability: an operator
    /// diagnosing a stale registry needs to see how close the indexes are to
    /// their cap.
    #[must_use]
    pub fn retained_bytes_and_budget(&self) -> (usize, usize) {
        (
            retained_index_bytes(&self.state.read().views),
            self.max_index_bytes,
        )
    }

    /// Mark all maintained aggregate views stale at `epoch` and detach their
    /// retained state immediately. When called from a `Tokio` runtime, destruction
    /// of the detached maps runs on the blocking pool so a large stale view does
    /// not stall an async visibility fence.
    pub fn mark_stale(&self, epoch: u64) {
        let retired = {
            let mut state = self.state.write();
            state.epoch = epoch;
            state.status = RegistryStatus::Stale;
            state
                .views
                .iter_mut()
                .map(MaintainedAggregateView::take_retained_state)
                .collect::<Vec<_>>()
        };

        if retired
            .iter()
            .all(|(groups, pk_index)| groups.is_empty() && pk_index.is_empty())
        {
            return;
        }
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            drop(handle.spawn_blocking(move || drop(retired)));
        } else {
            drop(retired);
        }
    }

    /// Apply positive row deltas if the state is fresh, otherwise keep it stale.
    /// Bounds memory: if the retained indexes would exceed their cap, all indexes are
    /// cleared and the registry fails safe to stale.
    ///
    /// # Errors
    ///
    /// Returns an error (after clearing the indexes and marking the registry
    /// stale) when a maintained accumulator overflows, Arrow scalar extraction
    /// fails, or the retained indexes exceed their entry cap. Queries then fall back
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
            // Check after every Arrow batch so a multi-batch CDC envelope cannot
            // accumulate unbounded retained state before the final cap check.
            if retained_index_bytes(&state.views) > self.max_index_bytes {
                failure = Some(index_cap_exceeded(
                    retained_index_entries(&state.views),
                    retained_index_bytes(&state.views),
                    self.max_index_bytes,
                ));
                break 'outer;
            }
        }

        finalize_maintenance_pass(&mut state, self.max_index_bytes, failure)
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

        finalize_maintenance_pass(&mut state, self.max_index_bytes, failure)
    }

    /// Rebuild every view from a complete table snapshot. Bounds memory: the
    /// retained index total is checked against its cap after each batch, so rebuilding a
    /// table larger than `max_index_bytes` fails safe to stale (clearing the
    /// indexes) instead of growing the index unbounded.
    ///
    /// # Errors
    ///
    /// Returns an error (after clearing the indexes and marking the registry
    /// stale) if a maintained accumulator overflows, Arrow scalar extraction
    /// fails, or the retained indexes exceed their entry cap.
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
            // stale before the retained indexes grow unbounded (rather than only
            // after the full rebuild, which could OOM first).
            if retained_index_bytes(&state.views) > self.max_index_bytes {
                failure = Some(index_cap_exceeded(
                    retained_index_entries(&state.views),
                    retained_index_bytes(&state.views),
                    self.max_index_bytes,
                ));
                break 'outer;
            }
        }
        finalize_maintenance_pass(&mut state, self.max_index_bytes, failure)
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
    /// # Returns
    ///
    /// `Ok(Some(batch))` when the registry is fresh at `scan_epoch`, a view
    /// matches `spec` exactly, and it materializes into `output_schema`.
    /// `Ok(None)` is the fallback signal (the caller should run normal
    /// execution) when the registry is stale at `scan_epoch`, no view matches
    /// `spec`, or the matched view does not fit `output_schema`.
    ///
    /// # Errors
    ///
    /// Returns an error only when a matched view fails to build its result
    /// arrays (e.g. a group-key or scalar-value conversion error) — not for the
    /// stale / no-match / schema-mismatch fallbacks above, which are `Ok(None)`.
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
            retained_multiset_entries: 0,
            approx_pk_index_bytes: 0,
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
        let retained_entries_added = group.apply_insert_row(batch, row)?;
        self.retained_multiset_entries = self
            .retained_multiset_entries
            .checked_add(retained_entries_added)
            .ok_or_else(index_entry_overflow)?;
        Ok(())
    }

    fn clear(&mut self) {
        self.groups.clear();
        self.pk_index.clear();
        self.retained_multiset_entries = 0;
        self.approx_pk_index_bytes = 0;
    }

    fn take_retained_state(&mut self) -> RetiredViewState {
        self.retained_multiset_entries = 0;
        self.approx_pk_index_bytes = 0;
        (
            std::mem::take(&mut self.groups),
            std::mem::take(&mut self.pk_index),
        )
    }

    fn index_len(&self) -> usize {
        self.pk_index
            .len()
            .saturating_add(self.retained_multiset_entries)
    }

    /// Approximate resident bytes this view retains for cap accounting: the
    /// per-PK index (tracked incrementally, since walking it would be O(rows) on
    /// every batch) plus the `MIN`/`MAX` multiset nodes.
    ///
    /// The estimate is charged against `runtime.query.memory_limit`, so it must
    /// not under-count — an estimate that drops a component bounds the index at a
    /// fraction of its believed size. Mirrors
    /// `crate::provider::pk_index::approx_pk_keyset_entry_bytes`.
    fn approx_index_bytes(&self) -> usize {
        self.approx_pk_index_bytes.saturating_add(
            self.retained_multiset_entries
                .saturating_mul(APPROX_MULTISET_NODE_BYTES),
        )
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
            return Err(retract_underflow());
        };
        let (group_is_empty, retained_entries_removed) = group.retract_row(&entry.inputs)?;
        self.retained_multiset_entries = self
            .retained_multiset_entries
            .checked_sub(retained_entries_removed)
            .ok_or_else(retract_underflow)?;
        if group_is_empty {
            self.groups.remove(&entry.group_key);
        }
        Ok(())
    }

    /// Retract the row currently indexed at `pk`, if any. Idempotent: a PK not
    /// in the index contributed nothing, so retraction is a no-op.
    fn retract_pk(&mut self, pk: &[ScalarValue]) -> DataFusionResult<()> {
        if let Some(entry) = self.pk_index.remove(pk) {
            self.approx_pk_index_bytes = self
                .approx_pk_index_bytes
                .saturating_sub(approx_pk_index_entry_bytes(pk, &entry));
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
                    self.approx_pk_index_bytes = self
                        .approx_pk_index_bytes
                        .saturating_sub(approx_pk_index_entry_bytes(&pk, &old));
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
                let entry = RowEntry {
                    group_key: group_key.clone(),
                    inputs,
                };
                self.approx_pk_index_bytes = self
                    .approx_pk_index_bytes
                    .saturating_add(approx_pk_index_entry_bytes(&pk, &entry));
                self.pk_index.insert(pk, entry);
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

        // Iterate the live groups BY REFERENCE — never clone the accumulator. A
        // maintained MIN/MAX accumulator owns the whole per-group ordered multiset,
        // so cloning it (as this did) made materialize O(total distinct) ≈ O(rows)
        // and defeated the O(groups) serve the whole lever depends on; for the
        // additive accumulators it was needless allocation. The empty global
        // aggregate (no groups, no GROUP BY) still emits one SQL row from a
        // default accumulator, which must outlive `rows`.
        let default_global;
        let rows: Vec<(&[ScalarValue], &GroupAccumulator)> =
            if self.groups.is_empty() && self.spec.group_by.is_empty() {
                default_global = GroupAccumulator::try_new(&self.spec)?;
                vec![(&[][..], &default_global)]
            } else {
                self.groups
                    .iter()
                    .map(|(key, acc)| (key.as_slice(), acc))
                    .collect()
            };

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
            .all(|(aggregate, field)| aggregate.output_matches_field(field))
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
            // SQL `AVG(int)` outputs `Float64` (DataFusion's AVG output type) for
            // the whole signed/unsigned integer family; the running sum is folded
            // exactly in `i128` (see `AvgInt128`), so a narrow CDC column (Postgres
            // `INTEGER` -> arrow `Int32`) is averaged without overflow.
            (MaintainedAggregateFunction::Avg, Some(data_type))
                if data_type.is_signed_integer() || data_type.is_unsigned_integer() =>
            {
                AggregateOutputType::Float64
            }
            // `SUM`/`AVG` over floating-point widen to `Float64` (DataFusion's
            // float sum/avg output type); `Float32` widens losslessly.
            (
                MaintainedAggregateFunction::Sum | MaintainedAggregateFunction::Avg,
                Some(data_type),
            ) if is_maintainable_float(data_type) => AggregateOutputType::Float64,
            // SQL `SUM(Decimal128(p, s))` keeps the scale and widens the
            // precision to `min(38, p + 10)` (DataFusion's decimal SUM output
            // type), so the running sum is the exact `i128` backing-value sum —
            // the common CDC money-column case (Postgres `NUMERIC(6, 2)` ->
            // arrow `Decimal128(6, 2)`). `Decimal256` (i256 backing) stays a
            // follow-up and falls to the catch-all below.
            (MaintainedAggregateFunction::Sum, Some(&DataType::Decimal128(precision, scale))) => {
                AggregateOutputType::Decimal128 {
                    precision: DECIMAL128_MAX_PRECISION.min(precision.saturating_add(10)),
                    scale,
                }
            }
            // SQL `AVG(Decimal128(p, s))` outputs `Decimal128(min(38, p + 4),
            // min(38, s + 4))` (DataFusion's decimal AVG output type). Restricted
            // to non-negative input scales: the serve-time quotient is computed
            // by DataFusion's `DecimalAverager`, whose `10^scale` factors are
            // only meaningful for `s >= 0`. A negative-scale decimal falls to
            // the catch-all.
            (MaintainedAggregateFunction::Avg, Some(&DataType::Decimal128(precision, scale)))
                if scale >= 0 =>
            {
                AggregateOutputType::Decimal128 {
                    precision: DECIMAL128_MAX_PRECISION.min(precision.saturating_add(4)),
                    scale: DECIMAL128_MAX_SCALE.min(scale.saturating_add(4)),
                }
            }
            (
                MaintainedAggregateFunction::Sum
                | MaintainedAggregateFunction::Avg
                | MaintainedAggregateFunction::Min
                | MaintainedAggregateFunction::Max,
                None,
            ) => {
                return Err(DataFusionError::Plan(format!(
                    "{:?} maintained aggregate requires a column",
                    expr.function
                )));
            }
            // `MIN`/`MAX` preserve the input type (no widening) and are maintained
            // via an ordered multiset. Supported: the signed/unsigned integer
            // families, the integer-backed temporal types (`Date32`/`Date64`/
            // `Timestamp`), and `Decimal128` (its backing value is an `i128`) — all
            // totally ordered by an integer, so the `i128` order key sorts them
            // exactly. Float `MIN`/`MAX` (NaN ordering) and `Decimal256` (i256, too
            // wide for the key) are follow-ups and fall to the catch-all below (the
            // view does not build; the query re-scans — correct, not fast).
            (
                MaintainedAggregateFunction::Min | MaintainedAggregateFunction::Max,
                Some(data_type),
            ) if data_type.is_signed_integer()
                || data_type.is_unsigned_integer()
                || is_maintainable_temporal(data_type)
                || matches!(data_type, DataType::Decimal128(_, _)) =>
            {
                AggregateOutputType::SameAsInput
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

    /// Whether this aggregate's output `field` matches the maintained result
    /// type. For `MIN`/`MAX` ([`AggregateOutputType::SameAsInput`]) the output
    /// preserves the input column type, so it is checked against the resolved
    /// column's `DataType`; every other aggregate has a fixed output type.
    fn output_matches_field(&self, field: &FieldRef) -> bool {
        match self.output_type {
            AggregateOutputType::SameAsInput => self
                .column
                .as_ref()
                .is_some_and(|column| field.data_type() == &column.data_type),
            fixed => fixed.matches_field(field),
        }
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

    fn apply_insert_row(&mut self, batch: &RecordBatch, row: usize) -> DataFusionResult<usize> {
        let mut retained_entries_added = 0_usize;
        for aggregate in &mut self.aggregates {
            retained_entries_added = retained_entries_added
                .checked_add(aggregate.apply_insert_row(batch, row)?)
                .ok_or_else(index_entry_overflow)?;
        }
        // `checked_add` (not saturating): a silently-clamped counter would break
        // the "drop the group when its last row is retracted" invariant, so an
        // overflow must fail the registry safe to stale instead.
        self.rows = self.rows.checked_add(1).ok_or_else(count_overflow)?;
        Ok(retained_entries_added)
    }

    /// Subtract a previously-captured row's per-aggregate contributions
    /// (inverse of [`Self::apply_insert_row`]). Returns whether the group is now
    /// empty and how many distinct multiset entries were removed.
    fn retract_row(&mut self, inputs: &[Option<ScalarValue>]) -> DataFusionResult<(bool, usize)> {
        if inputs.len() != self.aggregates.len() {
            return Err(retract_underflow());
        }
        let mut retained_entries_removed = 0_usize;
        for (aggregate, input) in self.aggregates.iter_mut().zip(inputs) {
            retained_entries_removed = retained_entries_removed
                .checked_add(aggregate.retract_row(input.as_ref())?)
                .ok_or_else(retract_underflow)?;
        }
        // `checked_sub` (not saturating): if retractions ever outnumber inserts
        // for a group (index/state inconsistency), surface it as an error so the
        // caller fails safe to stale rather than silently clamping at 0 and
        // mis-dropping the group.
        self.rows = self.rows.checked_sub(1).ok_or_else(retract_underflow)?;
        Ok((self.rows == 0, retained_entries_removed))
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
                    non_null_count: 0,
                }
            }
            (MaintainedAggregateFunction::Sum, AggregateOutputType::UInt64, Some(column)) => {
                Self::SumUInt64 {
                    column_index: column.index,
                    value: None,
                    non_null_count: 0,
                }
            }
            (MaintainedAggregateFunction::Sum, AggregateOutputType::Float64, Some(column)) => {
                Self::SumFloat64 {
                    column_index: column.index,
                    value: None,
                    non_null_count: 0,
                }
            }
            (
                MaintainedAggregateFunction::Sum,
                AggregateOutputType::Decimal128 { precision, scale },
                Some(column),
            ) => Self::SumDecimal128 {
                column_index: column.index,
                value: 0,
                non_null_count: 0,
                precision,
                scale,
            },
            (
                MaintainedAggregateFunction::Avg,
                AggregateOutputType::Decimal128 { precision, scale },
                Some(column),
            ) => {
                let DataType::Decimal128(_, sum_scale) = column.data_type else {
                    return Err(DataFusionError::Internal(format!(
                        "invalid maintained aggregate accumulator state: {expr:?}"
                    )));
                };
                Self::AvgDecimal128 {
                    column_index: column.index,
                    sum: 0,
                    count: 0,
                    sum_scale,
                    target_precision: precision,
                    target_scale: scale,
                }
            }
            (MaintainedAggregateFunction::Avg, AggregateOutputType::Float64, Some(column))
                if column.data_type.is_signed_integer()
                    || column.data_type.is_unsigned_integer() =>
            {
                Self::AvgInt128 {
                    column_index: column.index,
                    sum: 0,
                    count: 0,
                }
            }
            (MaintainedAggregateFunction::Avg, AggregateOutputType::Float64, Some(column)) => {
                Self::AvgFloat64 {
                    column_index: column.index,
                    sum: 0.0,
                    count: 0,
                }
            }
            (MaintainedAggregateFunction::Min, AggregateOutputType::SameAsInput, Some(column)) => {
                Self::Min {
                    column_index: column.index,
                    index: SortedScalarIndex::default(),
                }
            }
            (MaintainedAggregateFunction::Max, AggregateOutputType::SameAsInput, Some(column)) => {
                Self::Max {
                    column_index: column.index,
                    index: SortedScalarIndex::default(),
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

    fn apply_insert_row(&mut self, batch: &RecordBatch, row: usize) -> DataFusionResult<usize> {
        let inserted_multiset_entry = match self {
            Self::CountAll { value } => {
                *value = value.checked_add(1).ok_or_else(count_overflow)?;
                false
            }
            Self::CountColumn {
                column_index,
                value,
            } => {
                if !batch.column(*column_index).is_null(row) {
                    *value = value.checked_add(1).ok_or_else(count_overflow)?;
                }
                false
            }
            Self::SumInt64 {
                column_index,
                value,
                non_null_count,
            } => {
                if !batch.column(*column_index).is_null(row) {
                    let scalar = ScalarValue::try_from_array(batch.column(*column_index), row)?;
                    let delta = scalar_as_i64(&scalar)?;
                    let next_value = match *value {
                        Some(current) => current.checked_add(delta).ok_or_else(sum_overflow)?,
                        None => delta,
                    };
                    let next_count = non_null_count.checked_add(1).ok_or_else(count_overflow)?;
                    *value = Some(next_value);
                    *non_null_count = next_count;
                }
                false
            }
            Self::SumUInt64 {
                column_index,
                value,
                non_null_count,
            } => {
                if !batch.column(*column_index).is_null(row) {
                    let scalar = ScalarValue::try_from_array(batch.column(*column_index), row)?;
                    let delta = scalar_as_u64(&scalar)?;
                    let next_value = match *value {
                        Some(current) => current.checked_add(delta).ok_or_else(sum_overflow)?,
                        None => delta,
                    };
                    let next_count = non_null_count.checked_add(1).ok_or_else(count_overflow)?;
                    *value = Some(next_value);
                    *non_null_count = next_count;
                }
                false
            }
            Self::SumFloat64 {
                column_index,
                value,
                non_null_count,
            } => {
                if !batch.column(*column_index).is_null(row) {
                    let scalar = ScalarValue::try_from_array(batch.column(*column_index), row)?;
                    let delta = scalar_as_f64(&scalar)?;
                    let next_value = (*value).map_or(delta, |current| current + delta);
                    let next_count = non_null_count.checked_add(1).ok_or_else(count_overflow)?;
                    *value = Some(next_value);
                    *non_null_count = next_count;
                }
                false
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
                false
            }
            Self::AvgInt128 {
                column_index,
                sum,
                count,
            } => {
                if !batch.column(*column_index).is_null(row) {
                    let scalar = ScalarValue::try_from_array(batch.column(*column_index), row)?;
                    let delta = scalar_as_i128(&scalar)?;
                    *sum = sum.checked_add(delta).ok_or_else(avg_overflow)?;
                    *count = count.checked_add(1).ok_or_else(count_overflow)?;
                }
                false
            }
            Self::SumDecimal128 {
                column_index,
                value,
                non_null_count,
                ..
            } => {
                if !batch.column(*column_index).is_null(row) {
                    let scalar = ScalarValue::try_from_array(batch.column(*column_index), row)?;
                    let delta = scalar_as_decimal_i128(&scalar)?;
                    let next_value = value.checked_add(delta).ok_or_else(sum_overflow)?;
                    let next_count = non_null_count.checked_add(1).ok_or_else(count_overflow)?;
                    *value = next_value;
                    *non_null_count = next_count;
                }
                false
            }
            Self::AvgDecimal128 {
                column_index,
                sum,
                count,
                ..
            } => {
                if !batch.column(*column_index).is_null(row) {
                    let scalar = ScalarValue::try_from_array(batch.column(*column_index), row)?;
                    let delta = scalar_as_decimal_i128(&scalar)?;
                    *sum = sum.checked_add(delta).ok_or_else(avg_overflow)?;
                    *count = count.checked_add(1).ok_or_else(count_overflow)?;
                }
                false
            }
            // `MIN`/`MAX` insert identically — add the live value to the ordered
            // multiset; the query reads the min/max end. A null contributes
            // nothing to an extremum, so it is left out of the index.
            Self::Min {
                column_index,
                index,
            }
            | Self::Max {
                column_index,
                index,
            } => {
                if batch.column(*column_index).is_null(row) {
                    false
                } else {
                    let scalar = ScalarValue::try_from_array(batch.column(*column_index), row)?;
                    index.insert(scalar)?
                }
            }
        };
        Ok(usize::from(inserted_multiset_entry))
    }

    /// Inverse of [`Self::apply_insert_row`], subtracting a previously-captured
    /// input scalar. `SUM` also tracks its non-null cardinality so retracting the
    /// last value restores SQL `NULL` even when null-valued rows keep the group
    /// alive. `COUNT`/integer `SUM`/`AVG(int)` are exactly invertible;
    /// floating-point `SUM`/`AVG` subtract and rely on a periodic
    /// [`MaintainedAggregateRegistry::rebuild_from_batches`] to bound drift. A
    /// null input contributed nothing, so it retracts nothing.
    fn retract_row(&mut self, input: Option<&ScalarValue>) -> DataFusionResult<usize> {
        let removed_multiset_entry = match self {
            Self::CountAll { value } => {
                *value = value.checked_sub(1).ok_or_else(retract_underflow)?;
                false
            }
            Self::CountColumn { value, .. } => {
                if input.is_some_and(|scalar| !scalar.is_null()) {
                    *value = value.checked_sub(1).ok_or_else(retract_underflow)?;
                }
                false
            }
            Self::SumInt64 {
                value,
                non_null_count,
                ..
            } => {
                if let Some(scalar) = input
                    && !scalar.is_null()
                {
                    let delta = scalar_as_i64(scalar)?;
                    let current = (*value).ok_or_else(retract_underflow)?;
                    let remaining = current.checked_sub(delta).ok_or_else(sum_overflow)?;
                    let next_count = non_null_count
                        .checked_sub(1)
                        .ok_or_else(retract_underflow)?;
                    if next_count == 0 && remaining != 0 {
                        return Err(retract_underflow());
                    }
                    *value = (next_count != 0).then_some(remaining);
                    *non_null_count = next_count;
                }
                false
            }
            Self::SumUInt64 {
                value,
                non_null_count,
                ..
            } => {
                if let Some(scalar) = input
                    && !scalar.is_null()
                {
                    let delta = scalar_as_u64(scalar)?;
                    let current = (*value).ok_or_else(retract_underflow)?;
                    let remaining = current.checked_sub(delta).ok_or_else(retract_underflow)?;
                    let next_count = non_null_count
                        .checked_sub(1)
                        .ok_or_else(retract_underflow)?;
                    if next_count == 0 && remaining != 0 {
                        return Err(retract_underflow());
                    }
                    *value = (next_count != 0).then_some(remaining);
                    *non_null_count = next_count;
                }
                false
            }
            Self::SumFloat64 {
                value,
                non_null_count,
                ..
            } => {
                if let Some(scalar) = input
                    && !scalar.is_null()
                {
                    let delta = scalar_as_f64(scalar)?;
                    let current = (*value).ok_or_else(retract_underflow)?;
                    let next_count = non_null_count
                        .checked_sub(1)
                        .ok_or_else(retract_underflow)?;
                    *value = (next_count != 0).then_some(current - delta);
                    *non_null_count = next_count;
                }
                false
            }
            Self::AvgFloat64 { sum, count, .. } => {
                if let Some(scalar) = input
                    && !scalar.is_null()
                {
                    let delta = scalar_as_f64(scalar)?;
                    let next_count = count.checked_sub(1).ok_or_else(retract_underflow)?;
                    let remaining = *sum - delta;
                    *sum = if next_count == 0 { 0.0 } else { remaining };
                    *count = next_count;
                }
                false
            }
            Self::AvgInt128 { sum, count, .. } => {
                if let Some(scalar) = input
                    && !scalar.is_null()
                {
                    let delta = scalar_as_i128(scalar)?;
                    let remaining = sum.checked_sub(delta).ok_or_else(retract_underflow)?;
                    let next_count = count.checked_sub(1).ok_or_else(retract_underflow)?;
                    if next_count == 0 && remaining != 0 {
                        return Err(retract_underflow());
                    }
                    *sum = if next_count == 0 { 0 } else { remaining };
                    *count = next_count;
                }
                false
            }
            Self::SumDecimal128 {
                value,
                non_null_count,
                ..
            } => {
                if let Some(scalar) = input
                    && !scalar.is_null()
                {
                    let delta = scalar_as_decimal_i128(scalar)?;
                    let remaining = value.checked_sub(delta).ok_or_else(sum_overflow)?;
                    let next_count = non_null_count
                        .checked_sub(1)
                        .ok_or_else(retract_underflow)?;
                    if next_count == 0 && remaining != 0 {
                        return Err(retract_underflow());
                    }
                    *value = remaining;
                    *non_null_count = next_count;
                }
                false
            }
            Self::AvgDecimal128 { sum, count, .. } => {
                if let Some(scalar) = input
                    && !scalar.is_null()
                {
                    let delta = scalar_as_decimal_i128(scalar)?;
                    let remaining = sum.checked_sub(delta).ok_or_else(retract_underflow)?;
                    let next_count = count.checked_sub(1).ok_or_else(retract_underflow)?;
                    if next_count == 0 && remaining != 0 {
                        return Err(retract_underflow());
                    }
                    *sum = if next_count == 0 { 0 } else { remaining };
                    *count = next_count;
                }
                false
            }
            // `MIN`/`MAX` retract identically — remove the captured live value
            // from the ordered multiset; the extremum falls back to the next
            // value automatically. A null contributed nothing, so it retracts
            // nothing.
            Self::Min { index, .. } | Self::Max { index, .. } => {
                if let Some(scalar) = input
                    && !scalar.is_null()
                {
                    index.retract(scalar)?
                } else {
                    false
                }
            }
        };
        Ok(usize::from(removed_multiset_entry))
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
            Self::SumDecimal128 {
                value,
                non_null_count,
                precision,
                scale,
                ..
            } => {
                // `non_null_count == 0` encodes SQL `NULL` (see the variant doc).
                let value = (*non_null_count != 0).then_some(*value);
                scalar_for_field(
                    field,
                    Some(ScalarValue::Decimal128(value, *precision, *scale)),
                )
            }
            Self::AvgFloat64 { sum, count, .. } => {
                if *count == 0 {
                    scalar_for_field(field, Some(ScalarValue::Float64(None)))
                } else {
                    let count_f64 = exact_i64_to_f64(*count)?;
                    scalar_for_field(field, Some(ScalarValue::Float64(Some(*sum / count_f64))))
                }
            }
            Self::AvgInt128 { sum, count, .. } => {
                if *count == 0 {
                    scalar_for_field(field, Some(ScalarValue::Float64(None)))
                } else {
                    let count_f64 = exact_i64_to_f64(*count)?;
                    // The exact `i128` sum is divided down to the `Float64` AVG
                    // output; the cast rounds to nearest for sums beyond 2^53,
                    // which is inherent to producing an `f64` average and matches
                    // DataFusion's `AVG(int)` -> `Float64` result.
                    #[expect(
                        clippy::cast_precision_loss,
                        reason = "AVG output is Float64; rounding the i128 sum to f64 is intended"
                    )]
                    let sum_f64 = *sum as f64;
                    scalar_for_field(field, Some(ScalarValue::Float64(Some(sum_f64 / count_f64))))
                }
            }
            Self::AvgDecimal128 {
                sum,
                count,
                sum_scale,
                target_precision,
                target_scale,
                ..
            } => {
                // DataFusion's own sum/count -> decimal quotient (rescale to
                // the output scale, truncate toward zero, validate precision),
                // so the maintained result is structurally identical to what a
                // base-table re-scan computes — including erroring the query
                // with DataFusion's "Arithmetic Overflow in `AvgAccumulator`"
                // when the rescale or output precision overflows.
                let avg = if *count == 0 {
                    None
                } else {
                    Some(
                        DecimalAverager::<Decimal128Type>::try_new(
                            *sum_scale,
                            *target_precision,
                            *target_scale,
                        )?
                        .avg(*sum, i128::from(*count))?,
                    )
                };
                scalar_for_field(
                    field,
                    Some(ScalarValue::Decimal128(
                        avg,
                        *target_precision,
                        *target_scale,
                    )),
                )
            }
            // The stored extremum is already the exact input-typed scalar, so
            // `scalar_for_field` passes it through; an empty index yields a typed
            // `NULL` (SQL `MIN`/`MAX` over no non-null rows).
            Self::Min { index, .. } => scalar_for_field(field, index.min_scalar()),
            Self::Max { index, .. } => scalar_for_field(field, index.max_scalar()),
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
            "min" => MaintainedAggregateFunction::Min,
            "max" => MaintainedAggregateFunction::Max,
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
            MaintainedAggregateFunction::Sum
            | MaintainedAggregateFunction::Avg
            | MaintainedAggregateFunction::Min
            | MaintainedAggregateFunction::Max => {
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

/// Temporal types a maintained `MIN`/`MAX` can order via the integer order key:
/// `Date32` (days), `Date64` (millis), and every `Timestamp` unit (instant) are
/// monotonic in their backing integer, so `scalar_order_key` extracts that integer
/// and the `i128` key sorts them exactly. `Time`/`Duration`/`Interval` are omitted
/// (an `Interval` is not a single monotonic integer).
fn is_maintainable_temporal(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Date32 | DataType::Date64 | DataType::Timestamp(_, _)
    )
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

/// Coerce a non-null integer input scalar to `i128`, widening the whole
/// signed (`Int8`..`Int64`) and unsigned (`UInt8`..`UInt64`) family losslessly
/// (`u64` fits in `i128`). Used by maintained `AVG(int)`, whose `i128` running
/// sum has ample headroom to average many values near `i64::MAX`/`u64::MAX`.
fn scalar_as_i128(scalar: &ScalarValue) -> DataFusionResult<i128> {
    match scalar {
        ScalarValue::Int64(Some(v)) => Ok(i128::from(*v)),
        ScalarValue::Int32(Some(v)) => Ok(i128::from(*v)),
        ScalarValue::Int16(Some(v)) => Ok(i128::from(*v)),
        ScalarValue::Int8(Some(v)) => Ok(i128::from(*v)),
        ScalarValue::UInt64(Some(v)) => Ok(i128::from(*v)),
        ScalarValue::UInt32(Some(v)) => Ok(i128::from(*v)),
        ScalarValue::UInt16(Some(v)) => Ok(i128::from(*v)),
        ScalarValue::UInt8(Some(v)) => Ok(i128::from(*v)),
        _ => Err(type_mismatch("an integer", scalar)),
    }
}

/// Extract a non-null `Decimal128` input scalar's `i128` backing value. The
/// column carries one fixed scale, so maintained `SUM`/`AVG` fold the backing
/// values directly: at a shared scale, decimal addition IS integer addition on
/// the backing values, exactly invertible on the retract path.
fn scalar_as_decimal_i128(scalar: &ScalarValue) -> DataFusionResult<i128> {
    match scalar {
        ScalarValue::Decimal128(Some(v), _, _) => Ok(*v),
        _ => Err(type_mismatch("a decimal128", scalar)),
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

/// The order key for a maintained `MIN`/`MAX` value: a lossless `i128` over the
/// signed (`Int8`..`Int64`) and unsigned (`UInt8`..`UInt64`) integer families and
/// the integer-backed temporal types (`Date32` days, `Date64` millis, and every
/// `Timestamp` unit's instant). `i128` holds every `i64` and every `u64`, and its
/// natural order matches SQL ordering within a single fixed-type column — a column
/// carries one temporal unit + timezone, so the backing integers are directly
/// comparable — so it is a correct total order for the `BTreeMap`. The exact
/// input-typed `ScalarValue` is stored alongside the key, preserving the unit and
/// timezone on output. Float `MIN`/`MAX` (NaN ordering) is a deliberate follow-up
/// and errors here — the view then simply does not build, and the query falls back
/// to a base-table scan (correct, not accelerated).
#[expect(
    clippy::match_same_arms,
    reason = "each integer/temporal arm binds a different-width value (&i64, &i32, &i8, &u64, ...) so the identical-looking i128::from(*v) bodies cannot be merged into one | pattern"
)]
fn scalar_order_key(scalar: &ScalarValue) -> DataFusionResult<i128> {
    match scalar {
        ScalarValue::Int64(Some(v)) => Ok(i128::from(*v)),
        ScalarValue::Int32(Some(v)) => Ok(i128::from(*v)),
        ScalarValue::Int16(Some(v)) => Ok(i128::from(*v)),
        ScalarValue::Int8(Some(v)) => Ok(i128::from(*v)),
        ScalarValue::UInt64(Some(v)) => Ok(i128::from(*v)),
        ScalarValue::UInt32(Some(v)) => Ok(i128::from(*v)),
        ScalarValue::UInt16(Some(v)) => Ok(i128::from(*v)),
        ScalarValue::UInt8(Some(v)) => Ok(i128::from(*v)),
        // Integer-backed temporal types share the ordering — a column carries a
        // single unit/timezone, so the backing integers are directly comparable.
        ScalarValue::Date32(Some(v)) => Ok(i128::from(*v)),
        ScalarValue::Date64(Some(v)) => Ok(i128::from(*v)),
        ScalarValue::TimestampSecond(Some(v), _) => Ok(i128::from(*v)),
        ScalarValue::TimestampMillisecond(Some(v), _) => Ok(i128::from(*v)),
        ScalarValue::TimestampMicrosecond(Some(v), _) => Ok(i128::from(*v)),
        ScalarValue::TimestampNanosecond(Some(v), _) => Ok(i128::from(*v)),
        // `Decimal128`'s backing value IS an `i128`; a column carries one fixed
        // scale, so ordering by that integer orders by the decimal value.
        ScalarValue::Decimal128(Some(v), _, _) => Ok(*v),
        _ => Err(type_mismatch(
            "a signed/unsigned integer, temporal, or decimal128",
            scalar,
        )),
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

/// Finalize a maintenance pass: if `failure` is set, or the retained indexes now
/// exceed `max_index_bytes`, clear every index, mark the registry stale, and
/// return the reason (so the write-path applier can log it); otherwise the
/// registry stays fresh. Centralizes the fail-safe across the insert, PK-delete,
/// and rebuild paths so memory is bounded on every mutating path.
fn finalize_maintenance_pass(
    state: &mut RegistryState,
    max_index_bytes: usize,
    failure: Option<DataFusionError>,
) -> DataFusionResult<()> {
    let retained_bytes = retained_index_bytes(&state.views);
    let over_cap = retained_bytes > max_index_bytes;
    if failure.is_some() || over_cap {
        // Capture the size of what is being discarded BEFORE clearing, so the
        // error names what the index actually cost.
        let retained_entries = retained_index_entries(&state.views);
        for view in &mut state.views {
            view.clear();
        }
        state.status = RegistryStatus::Stale;
        return Err(failure.unwrap_or_else(|| {
            index_cap_exceeded(retained_entries, retained_bytes, max_index_bytes)
        }));
    }
    Ok(())
}

/// Total approximate resident bytes retained across every view — the quantity
/// the cap bounds. O(views), not O(rows): each view tracks its own total
/// incrementally.
fn retained_index_bytes(views: &[MaintainedAggregateView]) -> usize {
    views.iter().fold(0_usize, |total, view| {
        total.saturating_add(view.approx_index_bytes())
    })
}

/// Total retained index entries across every view. Reported alongside the byte
/// total when the cap trips, so an operator can see both what was retained and
/// what it cost.
fn retained_index_entries(views: &[MaintainedAggregateView]) -> usize {
    views.iter().fold(0_usize, |total, view| {
        total.saturating_add(view.index_len())
    })
}

/// A retained-entry counter overflowed `usize`. Distinct from
/// [`index_cap_exceeded`]: that is the budget doing its job, this is arithmetic
/// that cannot happen on a 64-bit host and is handled rather than panicked on.
fn index_entry_overflow() -> DataFusionError {
    DataFusionError::Execution(
        "Maintained aggregate retained-entry count overflowed; falling back to base table scan"
            .to_string(),
    )
}

/// Names what the index held and what it was allowed to hold, so an operator can
/// tell "the budget is too small" from "this table is too big to maintain" without
/// reading the code.
fn index_cap_exceeded(
    retained_entries: usize,
    retained_bytes: usize,
    max_index_bytes: usize,
) -> DataFusionError {
    DataFusionError::Execution(format!(
        "Maintained aggregate indexes exceeded their memory budget ({retained_entries} retained entries, ~{retained_bytes} bytes, budget {max_index_bytes} bytes); falling back to base table scan. Raise 'runtime.query.memory_limit' or narrow the maintained aggregate's filter."
    ))
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

/// The `AVG(int)` running sum overflowed its `i128` accumulator on the insert
/// path. AVG-specific (not [`sum_overflow`], whose "SUM" text would misreport the
/// failing aggregate); with `i128` headroom this is effectively unreachable, but
/// it fails safe. The retract path reuses [`retract_underflow`], matching
/// `COUNT`/`SUM(UInt64)`.
fn avg_overflow() -> DataFusionError {
    DataFusionError::Execution(
        "Maintained aggregate AVG running sum overflowed its i128 accumulator; falling back to base table scan"
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

    use arrow::array::{
        Decimal128Array, Float64Array, Int32Array, Int64Array, StringArray,
        TimestampMicrosecondArray, UInt64Array,
    };
    use arrow_schema::{Field, Schema, TimeUnit};
    use datafusion::physical_expr::aggregate::AggregateExprBuilder;
    use datafusion::physical_expr::expressions::{cast, col, lit};
    use datafusion::physical_plan::aggregates::PhysicalGroupBy;
    use datafusion_common::cast::{
        as_float64_array, as_int64_array, as_string_array, as_uint64_array,
    };
    use datafusion_functions_aggregate::average::avg_udaf;
    use datafusion_functions_aggregate::count::count_udaf;
    use datafusion_functions_aggregate::min_max::{max_udaf, min_udaf};
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
    fn stale_epoch_does_not_serve_and_releases_retained_state() -> DataFusionResult<()> {
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
        assert!(
            !registry.state.read().views[0].groups.is_empty(),
            "fresh registry retains group state"
        );
        registry.mark_stale(2);

        let state = registry.state.read();
        assert!(
            state.views[0].groups.is_empty(),
            "stale registry must release group state"
        );
        assert!(
            state.views[0].pk_index.is_empty(),
            "stale registry must release PK contributions"
        );
        assert_eq!(state.views[0].retained_multiset_entries, 0);
        drop(state);

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
                    MaintainedAggregateFunction::Min => min_udaf(),
                    MaintainedAggregateFunction::Max => max_udaf(),
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

    /// The retained-index cap is a BYTE budget, not an entry count: entry width
    /// varies by orders of magnitude with key and aggregate-input types, so a
    /// count cap bounds memory for exactly one schema shape. A budget too small
    /// to hold the index must fail safe to stale rather than grow past it.
    #[test]
    fn retained_index_cap_is_enforced_in_bytes() -> DataFusionResult<()> {
        // One entry cannot fit in 8 bytes, so the very first batch trips the cap.
        let registry =
            MaintainedAggregateRegistry::try_new_with_pk(&[sum_i_spec()], &schema(), &[2], 8)?;
        let result = registry.apply_insert_batches(1, &[group_batch(&[("a", 1, 10)])]);

        assert!(
            result.is_err(),
            "an index that cannot fit its byte budget must fail safe, not grow past it"
        );
        assert!(
            registry.is_stale(),
            "tripping the byte cap must leave the registry stale so queries fall back to base scans"
        );
        let (retained, budget) = registry.retained_bytes_and_budget();
        assert_eq!(retained, 0, "failing safe must clear all retained state");
        assert_eq!(budget, 8, "the configured byte budget is reported as-is");
        Ok(())
    }

    /// Byte accounting must be symmetric: a retraction has to release exactly what
    /// its insert charged, or a steady-state upsert workload leaks budget until it
    /// trips the cap and disables the view for no reason.
    #[test]
    fn retained_index_bytes_return_to_zero_after_full_retraction() -> DataFusionResult<()> {
        let registry = MaintainedAggregateRegistry::try_new_with_pk(
            &[sum_i_spec()],
            &schema(),
            &[2],
            usize::MAX,
        )?;
        let (empty_bytes, _) = registry.retained_bytes_and_budget();

        registry.apply_insert_batches(
            1,
            &[group_batch(&[("a", 1, 10), ("a", 2, 20), ("b", 3, 5)])],
        )?;
        let (loaded_bytes, _) = registry.retained_bytes_and_budget();
        assert!(
            loaded_bytes > empty_bytes,
            "indexing rows must charge bytes (was {loaded_bytes}, empty {empty_bytes})"
        );

        // Retract every indexed row.
        registry.apply_pk_deletes(
            2,
            &group_batch(&[("", 1, 0), ("", 2, 0), ("", 3, 0)]).project(&[2])?,
        )?;
        let (drained_bytes, _) = registry.retained_bytes_and_budget();
        assert_eq!(
            drained_bytes, empty_bytes,
            "retracting every row must release exactly what indexing charged"
        );
        Ok(())
    }

    /// Repeatedly upserting the SAME primary key must not accumulate byte charges:
    /// each upsert retracts the prior entry before re-indexing. A leak here is the
    /// realistic way a long-running CDC table would drift into a false cap trip.
    #[test]
    fn repeated_upsert_of_one_pk_does_not_leak_index_bytes() -> DataFusionResult<()> {
        let registry = MaintainedAggregateRegistry::try_new_with_pk(
            &[sum_i_spec()],
            &schema(),
            &[2],
            usize::MAX,
        )?;
        registry.apply_insert_batches(1, &[group_batch(&[("a", 1, 10)])])?;
        let (after_first, _) = registry.retained_bytes_and_budget();

        for epoch in 2..=16_u64 {
            registry.apply_insert_batches(
                epoch,
                &[group_batch(&[("a", 1, i64::try_from(epoch).unwrap_or(0))])],
            )?;
        }
        let (after_many, _) = registry.retained_bytes_and_budget();

        assert_eq!(
            after_many, after_first,
            "re-upserting one PK must hold steady state, not accumulate byte charges"
        );
        assert!(!registry.is_stale(), "steady-state upserts must stay fresh");
        Ok(())
    }

    /// `is_stale` is what drives the provider's rebuild-to-recover path, so it must
    /// report the state transitions that path keys on.
    #[test]
    fn is_stale_tracks_the_registry_lifecycle() -> DataFusionResult<()> {
        let registry = MaintainedAggregateRegistry::try_new_with_pk(
            &[sum_i_spec()],
            &schema(),
            &[2],
            usize::MAX,
        )?;
        assert!(!registry.is_stale(), "a fresh registry is not stale");

        registry.mark_stale(1);
        assert!(registry.is_stale(), "mark_stale must be observable");

        // A rebuild is the only path back to fresh — this is what the provider's
        // re-arm calls, and why staleness must not be terminal.
        registry.rebuild_from_batches(2, &[group_batch(&[("a", 1, 10)])])?;
        assert!(
            !registry.is_stale(),
            "rebuilding must clear staleness so maintained state serves again"
        );
        Ok(())
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

    #[test]
    fn min_max_multiset_entries_count_toward_cap_without_pk() -> DataFusionResult<()> {
        let spec = MaintainedAggregateSpec {
            group_by: vec!["name".to_string()],
            aggregates: vec![MaintainedAggregateExpr {
                function: MaintainedAggregateFunction::Min,
                column: Some("i".to_string()),
            }],
            filter: None,
        };
        // No PK index, but two distinct extremum values still retain two BTreeMap
        // entries and must exceed this one-entry cap.
        let registry = MaintainedAggregateRegistry::try_new_with_pk(&[spec], &schema(), &[], 1)?;
        let result =
            registry.apply_insert_batches(1, &[group_batch(&[("a", 1, 10), ("a", 2, 20)])]);
        assert!(
            result.is_err(),
            "MIN multiset entries must be bounded even without a PK index"
        );
        let aggregate =
            aggregate_exec_for(&[("min(i)", MaintainedAggregateFunction::Min, Some("i"))])?;
        assert!(
            registry.batch_for_aggregate(&aggregate, 1)?.is_none(),
            "over-cap MIN registry must fall back to the base scan"
        );
        Ok(())
    }

    /// PK contribution records and `MIN`/`MAX` multiset nodes must share ONE
    /// budget — an accounting that charged only the PK index would let a
    /// `MIN`/`MAX` view grow past the operator's memory limit unmeasured.
    ///
    /// Expressed in bytes rather than entries: entry width varies by key and
    /// aggregate-input type, so bytes are what actually bound memory. The test
    /// derives the boundary from the measured footprint instead of hard-coding
    /// one, so it stays honest if `ScalarValue`'s layout changes.
    #[test]
    fn pk_and_min_max_bytes_all_count_toward_cap() -> DataFusionResult<()> {
        let rows = group_batch(&[("a", 1, 10), ("a", 2, 20)]);

        // Measure what two rows of a MIN/MAX view actually retain.
        let unbounded = MaintainedAggregateRegistry::try_new_with_pk(
            &[min_max_i_spec()],
            &schema(),
            &[2],
            usize::MAX,
        )?;
        unbounded.apply_insert_batches(1, std::slice::from_ref(&rows))?;
        let (retained_bytes, _) = unbounded.retained_bytes_and_budget();
        // Two rows retain two PK contribution records plus two `MIN` and two
        // `MAX` multiset nodes: six entries, all charged.
        assert_eq!(
            unbounded.state.read().views[0].index_len(),
            6,
            "two rows retain 2 PK records + 2 MIN + 2 MAX multiset nodes"
        );
        assert!(
            retained_bytes > 0,
            "retained state must be charged in bytes, not silently free"
        );

        // Exactly enough budget: the same load fits and stays fresh.
        let at_cap = MaintainedAggregateRegistry::try_new_with_pk(
            &[min_max_i_spec()],
            &schema(),
            &[2],
            retained_bytes,
        )?;
        at_cap.apply_insert_batches(1, std::slice::from_ref(&rows))?;
        assert!(
            !at_cap.is_stale(),
            "a load that exactly fits its budget must stay fresh"
        );

        // One byte short: the multiset nodes are what push it over, proving they
        // are charged alongside the PK records rather than ignored.
        let over_cap = MaintainedAggregateRegistry::try_new_with_pk(
            &[min_max_i_spec()],
            &schema(),
            &[2],
            retained_bytes.saturating_sub(1),
        )?;
        let result = over_cap.apply_insert_batches(1, &[rows]);
        assert!(
            result.is_err(),
            "PK records and ordered-multiset nodes must share one byte budget"
        );
        assert!(over_cap.is_stale(), "an over-budget load must fail safe");
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

    #[test]
    fn retracting_last_non_null_sum_restores_sql_null() -> DataFusionResult<()> {
        let input_schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("i", DataType::Int64, true),
            Field::new("u", DataType::UInt64, true),
            Field::new("f", DataType::Float64, true),
            Field::new("pk", DataType::Int64, false),
        ]));
        let spec = MaintainedAggregateSpec {
            filter: None,
            group_by: vec!["name".to_string()],
            aggregates: vec![
                MaintainedAggregateExpr {
                    function: MaintainedAggregateFunction::Sum,
                    column: Some("i".to_string()),
                },
                MaintainedAggregateExpr {
                    function: MaintainedAggregateFunction::Sum,
                    column: Some("u".to_string()),
                },
                MaintainedAggregateExpr {
                    function: MaintainedAggregateFunction::Sum,
                    column: Some("f".to_string()),
                },
            ],
        };
        let registry = MaintainedAggregateRegistry::try_new_with_pk(
            std::slice::from_ref(&spec),
            &input_schema,
            &[4],
            usize::MAX,
        )?;
        let input = RecordBatch::try_new(
            Arc::clone(&input_schema),
            vec![
                Arc::new(StringArray::from(vec!["a", "a"])),
                Arc::new(Int64Array::from(vec![None, Some(7)])),
                Arc::new(UInt64Array::from(vec![None, Some(8)])),
                Arc::new(Float64Array::from(vec![None, Some(1.5)])),
                Arc::new(Int64Array::from(vec![1, 2])),
            ],
        )?;
        registry.apply_insert_batches(1, &[input])?;

        let delete_pk = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("pk", DataType::Int64, false)])),
            vec![Arc::new(Int64Array::from(vec![2]))],
        )?;
        registry.apply_pk_deletes(2, &delete_pk)?;

        let output_schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("sum_i", DataType::Int64, true),
            Field::new("sum_u", DataType::UInt64, true),
            Field::new("sum_f", DataType::Float64, true),
        ]));
        let result = registry
            .batch_for_spec(&spec, 2, output_schema)?
            .expect("fresh sum view should serve");
        assert_eq!(result.num_rows(), 1, "the all-NULL row keeps group a live");
        assert_eq!(as_string_array(result.column(0))?.value(0), "a");
        assert!(as_int64_array(result.column(1))?.is_null(0));
        assert!(as_uint64_array(result.column(2))?.is_null(0));
        assert!(as_float64_array(result.column(3))?.is_null(0));
        Ok(())
    }

    #[test]
    fn avg_resets_running_sum_after_last_non_null_retraction() -> DataFusionResult<()> {
        let spec = MaintainedAggregateSpec {
            filter: None,
            group_by: vec!["name".to_string()],
            aggregates: vec![MaintainedAggregateExpr {
                function: MaintainedAggregateFunction::Avg,
                column: Some("f".to_string()),
            }],
        };
        let registry = MaintainedAggregateRegistry::try_new_with_pk(
            std::slice::from_ref(&spec),
            &schema(),
            &[1],
            usize::MAX,
        )?;
        let float_batch = |rows: &[(i64, Option<f64>)]| {
            RecordBatch::try_new(
                schema(),
                vec![
                    Arc::new(StringArray::from(vec![Some("a"); rows.len()])),
                    Arc::new(Int64Array::from(
                        rows.iter().map(|(pk, _)| *pk).collect::<Vec<_>>(),
                    )),
                    Arc::new(UInt64Array::from(vec![None::<u64>; rows.len()])),
                    Arc::new(Float64Array::from(
                        rows.iter().map(|(_, value)| *value).collect::<Vec<_>>(),
                    )),
                ],
            )
        };
        registry.apply_insert_batches(
            1,
            &[float_batch(&[
                (1, None),
                (2, Some(1.0e16)),
                (3, Some(1.0)),
            ])?],
        )?;
        registry.apply_pk_deletes(2, &float_batch(&[(2, None)])?.project(&[1])?)?;
        registry.apply_pk_deletes(3, &float_batch(&[(3, None)])?.project(&[1])?)?;
        registry.apply_insert_batches(4, &[float_batch(&[(4, Some(2.0))])?])?;

        let output_schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("avg_f", DataType::Float64, true),
        ]));
        let result = registry
            .batch_for_spec(&spec, 4, output_schema)?
            .expect("fresh average view should serve");
        let average = as_float64_array(result.column(1))?.value(0);
        assert!((average - 2.0).abs() < f64::EPSILON);
        Ok(())
    }

    /// Serve `MIN(i)`, `MAX(i)` GROUP BY `name` from the registry, returning the
    /// per-group extrema (NULL extrema are omitted). `i` is the module schema's
    /// `Int64` column; the group key is `name`.
    fn min_max_by_name(
        registry: &MaintainedAggregateRegistry,
    ) -> DataFusionResult<(BTreeMap<String, i64>, BTreeMap<String, i64>)> {
        let aggregate = aggregate_exec_for(&[
            ("min(i)", MaintainedAggregateFunction::Min, Some("i")),
            ("max(i)", MaintainedAggregateFunction::Max, Some("i")),
        ])?;
        let epoch = registry.state.read().epoch;
        let result = registry
            .batch_for_aggregate(&aggregate, epoch)?
            .expect("registry should be fresh");
        let names = as_string_array(result.column(0))?;
        let mins = as_int64_array(result.column(1))?;
        let maxs = as_int64_array(result.column(2))?;
        let mut min_out = BTreeMap::new();
        let mut max_out = BTreeMap::new();
        for row in 0..result.num_rows() {
            let name = names.value(row).to_string();
            if !mins.is_null(row) {
                min_out.insert(name.clone(), mins.value(row));
            }
            if !maxs.is_null(row) {
                max_out.insert(name, maxs.value(row));
            }
        }
        Ok((min_out, max_out))
    }

    fn min_max_i_spec() -> MaintainedAggregateSpec {
        MaintainedAggregateSpec {
            filter: None,
            group_by: vec!["name".to_string()],
            aggregates: vec![
                MaintainedAggregateExpr {
                    function: MaintainedAggregateFunction::Min,
                    column: Some("i".to_string()),
                },
                MaintainedAggregateExpr {
                    function: MaintainedAggregateFunction::Max,
                    column: Some("i".to_string()),
                },
            ],
        }
    }

    /// The retraction-hard core: deleting the current group MIN or MAX must
    /// expose the next value from the maintained ordered multiset — the exact
    /// case `COUNT`/`SUM` (invert-by-subtract) cannot do and the reason MIN/MAX
    /// needs the kept ordered structure. PK = `u` (column 2).
    #[test]
    fn maintains_min_max_with_retraction() -> DataFusionResult<()> {
        let registry = MaintainedAggregateRegistry::try_new_with_pk(
            &[min_max_i_spec()],
            &schema(),
            &[2],
            usize::MAX,
        )?;
        // group a: i = {10(pk1), 20(pk2), 5(pk3)} -> min 5, max 20.
        // group b: i = {7(pk4)}                    -> min 7, max 7.
        registry.apply_insert_batches(
            1,
            &[group_batch(&[
                ("a", 1, 10),
                ("a", 2, 20),
                ("a", 3, 5),
                ("b", 4, 7),
            ])],
        )?;
        let (min, max) = min_max_by_name(&registry)?;
        assert_eq!(min.get("a"), Some(&5));
        assert_eq!(max.get("a"), Some(&20));
        assert_eq!(min.get("b"), Some(&7));
        assert_eq!(max.get("b"), Some(&7));

        // Delete pk=3 (the current MIN of a, value 5): MIN falls back to 10, MAX unchanged.
        registry.apply_pk_deletes(2, &group_batch(&[("", 3, 0)]).project(&[2])?)?;
        let (min, max) = min_max_by_name(&registry)?;
        assert_eq!(
            min.get("a"),
            Some(&10),
            "MIN exposes the next value after the extremum is retracted"
        );
        assert_eq!(max.get("a"), Some(&20));

        // Delete pk=2 (the current MAX of a, value 20): a = {10}, min == max == 10.
        registry.apply_pk_deletes(3, &group_batch(&[("", 2, 0)]).project(&[2])?)?;
        let (min, max) = min_max_by_name(&registry)?;
        assert_eq!(min.get("a"), Some(&10));
        assert_eq!(
            max.get("a"),
            Some(&10),
            "MAX exposes the next value after the extremum is retracted"
        );
        Ok(())
    }

    /// NULLs never feed an extremum, but a row still counts toward the group's
    /// existence: an entirely-NULL group survives with NULL MIN/MAX, and a
    /// partially-NULL group reports the extremum of its non-null values.
    #[test]
    fn min_max_ignores_nulls_and_keeps_all_null_group() -> DataFusionResult<()> {
        let registry = MaintainedAggregateRegistry::try_new_with_pk(
            &[min_max_i_spec()],
            &schema(),
            &[2],
            usize::MAX,
        )?;
        // group a: i = {NULL(pk1), 3(pk2), NULL(pk3)} -> min == max == 3.
        // group b: i = {NULL(pk4)}                    -> min == max == NULL (group present).
        let null_i_batch = RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(StringArray::from(vec![
                    Some("a"),
                    Some("a"),
                    Some("a"),
                    Some("b"),
                ])),
                Arc::new(Int64Array::from(vec![None, Some(3_i64), None, None])),
                Arc::new(UInt64Array::from(vec![1_u64, 2, 3, 4])),
                Arc::new(Float64Array::from(
                    vec![None, None, None, None] as Vec<Option<f64>>
                )),
            ],
        )
        .expect("null-i batch should be valid");
        registry.apply_insert_batches(1, &[null_i_batch])?;
        let (min, max) = min_max_by_name(&registry)?;
        assert_eq!(min.get("a"), Some(&3));
        assert_eq!(max.get("a"), Some(&3));
        assert_eq!(min.get("b"), None, "all-NULL group b has NULL MIN");
        assert_eq!(max.get("b"), None, "all-NULL group b has NULL MAX");

        // Retract pk=2 (a's only non-null i): a is now entirely NULL -> NULL MIN/MAX, group still live.
        registry.apply_pk_deletes(2, &group_batch(&[("", 2, 0)]).project(&[2])?)?;
        let (min, max) = min_max_by_name(&registry)?;
        assert_eq!(min.get("a"), None, "a is now all-NULL i -> NULL MIN");
        assert_eq!(max.get("a"), None);
        Ok(())
    }

    /// The dominant real-world MIN/MAX pattern is temporal — "earliest / latest
    /// event per group". `Date`/`Timestamp` are integer-backed and monotonic, so
    /// they reuse the integer order key (no NaN complexity) and preserve the exact
    /// unit/timezone on output. Retraction-hard case included: deleting the current
    /// earliest or latest exposes the next. PK = `pk` (column 2).
    #[test]
    fn maintains_min_max_over_timestamps() -> DataFusionResult<()> {
        let ts_type = DataType::Timestamp(TimeUnit::Microsecond, None);
        let ts_schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("ts", ts_type.clone(), true),
            Field::new("pk", DataType::Int64, false),
        ]));
        let spec = MaintainedAggregateSpec {
            filter: None,
            group_by: vec!["name".to_string()],
            aggregates: vec![
                MaintainedAggregateExpr {
                    function: MaintainedAggregateFunction::Min,
                    column: Some("ts".to_string()),
                },
                MaintainedAggregateExpr {
                    function: MaintainedAggregateFunction::Max,
                    column: Some("ts".to_string()),
                },
            ],
        };
        let registry = MaintainedAggregateRegistry::try_new_with_pk(
            std::slice::from_ref(&spec),
            &ts_schema,
            &[2],
            usize::MAX,
        )?;
        // (ts_micros, pk); all rows are group "a".
        let ts_batch = |rows: &[(i64, i64)]| {
            RecordBatch::try_new(
                Arc::clone(&ts_schema),
                vec![
                    Arc::new(StringArray::from(vec![Some("a"); rows.len()])),
                    Arc::new(TimestampMicrosecondArray::from(
                        rows.iter().map(|(t, _)| *t).collect::<Vec<_>>(),
                    )),
                    Arc::new(Int64Array::from(
                        rows.iter().map(|(_, p)| *p).collect::<Vec<_>>(),
                    )),
                ],
            )
            .expect("timestamp batch should be valid")
        };
        let out_schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("min_ts", ts_type.clone(), true),
            Field::new("max_ts", ts_type, true),
        ]));
        let serve = |epoch: u64| -> DataFusionResult<(i64, i64)> {
            let batch = registry
                .batch_for_spec(&spec, epoch, Arc::clone(&out_schema))?
                .expect("registry fresh and view matches");
            assert_eq!(batch.num_rows(), 1, "single group 'a'");
            let mins = batch
                .column(1)
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .expect("min_ts is TimestampMicrosecond");
            let maxs = batch
                .column(2)
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .expect("max_ts is TimestampMicrosecond");
            Ok((mins.value(0), maxs.value(0)))
        };

        // ts {100(pk1), 300(pk2), 200(pk3)} -> min 100, max 300.
        registry.apply_insert_batches(1, &[ts_batch(&[(100, 1), (300, 2), (200, 3)])])?;
        assert_eq!(serve(1)?, (100, 300));

        // Delete pk2 (ts=300, current MAX) -> MAX exposes the next-latest, 200.
        registry.apply_pk_deletes(2, &ts_batch(&[(0, 2)]).project(&[2])?)?;
        assert_eq!(
            serve(2)?,
            (100, 200),
            "MAX exposes the next-latest timestamp"
        );

        // Delete pk1 (ts=100, current MIN) -> MIN exposes the next-earliest, 200.
        registry.apply_pk_deletes(3, &ts_batch(&[(0, 1)]).project(&[2])?)?;
        assert_eq!(
            serve(3)?,
            (200, 200),
            "MIN exposes the next-earliest timestamp"
        );
        Ok(())
    }

    /// Financial MIN/MAX (min/max amount per group) over `Decimal128`, whose
    /// backing value is an `i128` — the order key IS that integer (a column has
    /// one fixed scale, so no scaling is needed). Also covers a duplicated value
    /// (the multiset keeps a count, so one retraction of a value seen twice does
    /// not drop it). `Decimal256` (i256) stays a follow-up. PK = `pk` (column 2).
    #[test]
    fn maintains_min_max_over_decimal128() -> DataFusionResult<()> {
        let dec_type = DataType::Decimal128(12, 2);
        let dec_schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("amt", dec_type.clone(), true),
            Field::new("pk", DataType::Int64, false),
        ]));
        let spec = MaintainedAggregateSpec {
            filter: None,
            group_by: vec!["name".to_string()],
            aggregates: vec![
                MaintainedAggregateExpr {
                    function: MaintainedAggregateFunction::Min,
                    column: Some("amt".to_string()),
                },
                MaintainedAggregateExpr {
                    function: MaintainedAggregateFunction::Max,
                    column: Some("amt".to_string()),
                },
            ],
        };
        let registry = MaintainedAggregateRegistry::try_new_with_pk(
            std::slice::from_ref(&spec),
            &dec_schema,
            &[2],
            usize::MAX,
        )?;
        // (raw i128 at scale 2, pk); all rows are group "a".
        let dec_batch = |rows: &[(i128, i64)]| {
            let amounts = Decimal128Array::from(rows.iter().map(|(a, _)| *a).collect::<Vec<_>>())
                .with_precision_and_scale(12, 2)
                .expect("valid decimal precision/scale");
            RecordBatch::try_new(
                Arc::clone(&dec_schema),
                vec![
                    Arc::new(StringArray::from(vec![Some("a"); rows.len()])),
                    Arc::new(amounts),
                    Arc::new(Int64Array::from(
                        rows.iter().map(|(_, p)| *p).collect::<Vec<_>>(),
                    )),
                ],
            )
            .expect("decimal batch should be valid")
        };
        let out_schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("min_amt", dec_type.clone(), true),
            Field::new("max_amt", dec_type, true),
        ]));
        let serve = |epoch: u64| -> DataFusionResult<(i128, i128)> {
            let batch = registry
                .batch_for_spec(&spec, epoch, Arc::clone(&out_schema))?
                .expect("registry fresh and view matches");
            assert_eq!(batch.num_rows(), 1, "single group 'a'");
            let mins = batch
                .column(1)
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .expect("min_amt is Decimal128");
            let maxs = batch
                .column(2)
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .expect("max_amt is Decimal128");
            Ok((mins.value(0), maxs.value(0)))
        };

        // amt {1.50(pk1), 9.99(pk2), 1.50(pk3, dup), -0.50(pk4)} -> min -50, max 999 (raw).
        registry
            .apply_insert_batches(1, &[dec_batch(&[(150, 1), (999, 2), (150, 3), (-50, 4)])])?;
        assert_eq!(serve(1)?, (-50, 999));

        // Delete pk2 (9.99, current MAX) -> MAX falls back to 1.50 (raw 150).
        registry.apply_pk_deletes(2, &dec_batch(&[(0, 2)]).project(&[2])?)?;
        assert_eq!(serve(2)?, (-50, 150), "MAX exposes the next-largest amount");

        // Delete pk4 (-0.50, current MIN) -> MIN falls back to 1.50; the dup (pk1,pk3)
        // keeps the value 150 present with count 2, so the group stays non-empty.
        registry.apply_pk_deletes(3, &dec_batch(&[(0, 4)]).project(&[2])?)?;
        assert_eq!(
            serve(3)?,
            (150, 150),
            "MIN exposes the next-smallest amount"
        );
        Ok(())
    }

    // --- decimal SUM/AVG (Postgres NUMERIC → arrow Decimal128, the CDC money
    // column case). Column layout for these tests:
    //   name (Utf8)            -> GROUP BY key
    //   amt  (Decimal128(6,2)) -> SUM/AVG target
    //   pk   (Int64)           -> PRIMARY KEY (pk_columns = [2])

    fn decimal_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("amt", DataType::Decimal128(6, 2), true),
            Field::new("pk", DataType::Int64, false),
        ]))
    }

    /// Rows are `(group, raw backing value at scale 2, pk)`.
    fn decimal_batch(rows: &[(&str, Option<i128>, i64)]) -> RecordBatch {
        let amounts = Decimal128Array::from(rows.iter().map(|(_, a, _)| *a).collect::<Vec<_>>())
            .with_precision_and_scale(6, 2)
            .expect("valid decimal precision/scale");
        RecordBatch::try_new(
            decimal_schema(),
            vec![
                Arc::new(StringArray::from(
                    rows.iter().map(|(n, _, _)| Some(*n)).collect::<Vec<_>>(),
                )),
                Arc::new(amounts),
                Arc::new(Int64Array::from(
                    rows.iter().map(|(_, _, pk)| *pk).collect::<Vec<_>>(),
                )),
            ],
        )
        .expect("decimal batch should be valid")
    }

    fn decimal_sum_avg_spec() -> MaintainedAggregateSpec {
        MaintainedAggregateSpec {
            filter: None,
            group_by: vec!["name".to_string()],
            aggregates: vec![
                MaintainedAggregateExpr {
                    function: MaintainedAggregateFunction::Sum,
                    column: Some("amt".to_string()),
                },
                MaintainedAggregateExpr {
                    function: MaintainedAggregateFunction::Avg,
                    column: Some("amt".to_string()),
                },
            ],
        }
    }

    /// Serve `SUM(amt)`/`AVG(amt) GROUP BY name` through a real `DataFusion`
    /// `AggregateExec` (so the output field types are `DataFusion`'s own decimal
    /// `SUM`/`AVG` return types, not ones this module computed for itself) and
    /// return `group -> (sum raw, avg raw)` backing values, skipping NULLs.
    #[expect(clippy::type_complexity, reason = "test helper return map")]
    fn decimal_sum_avg_by_name(
        registry: &MaintainedAggregateRegistry,
        epoch: u64,
    ) -> DataFusionResult<BTreeMap<String, (Option<i128>, Option<i128>)>> {
        let schema = decimal_schema();
        let input = MemorySourceConfig::try_new_exec(
            &[vec![decimal_batch(&[])]],
            Arc::clone(&schema),
            None,
        )?;
        let aggregate_exprs = [("sum(amt)", sum_udaf()), ("avg(amt)", avg_udaf())]
            .into_iter()
            .map(|(alias, udaf)| {
                AggregateExprBuilder::new(udaf, vec![col("amt", schema.as_ref())?])
                    .schema(Arc::clone(&schema))
                    .alias(alias.to_string())
                    .build()
                    .map(Arc::new)
            })
            .collect::<DataFusionResult<Vec<_>>>()?;
        let aggregate = AggregateExec::try_new(
            AggregateMode::Single,
            PhysicalGroupBy::new_single(vec![(col("name", schema.as_ref())?, "name".to_string())]),
            aggregate_exprs,
            vec![None, None],
            input,
            schema,
        )?;
        // DataFusion's decimal SUM/AVG output types, computed by DataFusion.
        assert_eq!(
            aggregate.schema().field(1).data_type(),
            &DataType::Decimal128(16, 2),
            "sum(Decimal128(6, 2)) widens precision by 10"
        );
        assert_eq!(
            aggregate.schema().field(2).data_type(),
            &DataType::Decimal128(10, 6),
            "avg(Decimal128(6, 2)) widens precision and scale by 4"
        );
        let result = registry
            .batch_for_aggregate(&aggregate, epoch)?
            .expect("registry should be fresh");
        let names = as_string_array(result.column(0))?;
        let sums = result
            .column(1)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .expect("sum(amt) is Decimal128");
        let avgs = result
            .column(2)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .expect("avg(amt) is Decimal128");
        let mut out = BTreeMap::new();
        for row in 0..result.num_rows() {
            out.insert(
                names.value(row).to_string(),
                (
                    (!sums.is_null(row)).then(|| sums.value(row)),
                    (!avgs.is_null(row)).then(|| avgs.value(row)),
                ),
            );
        }
        Ok(out)
    }

    /// `SUM`/`AVG` over a `Decimal128` money column (Postgres `NUMERIC(6, 2)`,
    /// the CH-benCH `SUM(ol_amount)` case) must (a) be accepted at registry
    /// construction and (b) serve exact values through a real `DataFusion`
    /// aggregate, whose decimal output types the maintained view must
    /// reproduce. Before decimal support this failed dataset registration with
    /// "Sum maintained aggregate does not support column type Decimal128(6, 2)".
    #[test]
    fn maintains_sum_avg_over_decimal128() -> DataFusionResult<()> {
        let registry = MaintainedAggregateRegistry::try_new_with_pk(
            &[decimal_sum_avg_spec()],
            &decimal_schema(),
            &[2],
            usize::MAX,
        )?;
        // a: 1.50 + 9.99 + NULL; b: -0.50.
        registry.apply_insert_batches(
            1,
            &[decimal_batch(&[
                ("a", Some(150), 1),
                ("a", Some(999), 2),
                ("a", None, 3),
                ("b", Some(-50), 4),
            ])],
        )?;
        let by_name = decimal_sum_avg_by_name(&registry, 1)?;
        // a: SUM = 11.49 (raw 1149 at scale 2); AVG = 11.49 / 2 = 5.745000
        // (raw 1149 * 10^4 / 2 = 5_745_000 at scale 6) — NULL contributes nothing.
        assert_eq!(by_name.get("a"), Some(&(Some(1149), Some(5_745_000))));
        // b: SUM = -0.50; AVG = -0.500000.
        assert_eq!(by_name.get("b"), Some(&(Some(-50), Some(-500_000))));
        Ok(())
    }

    /// The decimal `AVG` quotient truncates toward zero — exactly `DataFusion`'s
    /// `DecimalAverager` (`div_wrapping`), not floor: `0.04 / 3` is `0.013333`
    /// and `-0.04 / 3` is `-0.013333` (floor would give `-0.013334`).
    #[test]
    fn avg_over_decimal128_truncates_toward_zero_like_datafusion() -> DataFusionResult<()> {
        let registry = MaintainedAggregateRegistry::try_new_with_pk(
            &[decimal_sum_avg_spec()],
            &decimal_schema(),
            &[2],
            usize::MAX,
        )?;
        registry.apply_insert_batches(
            1,
            &[decimal_batch(&[
                ("pos", Some(1), 1),
                ("pos", Some(1), 2),
                ("pos", Some(2), 3),
                ("neg", Some(-1), 4),
                ("neg", Some(-1), 5),
                ("neg", Some(-2), 6),
            ])],
        )?;
        let by_name = decimal_sum_avg_by_name(&registry, 1)?;
        assert_eq!(by_name.get("pos"), Some(&(Some(4), Some(13_333))));
        assert_eq!(by_name.get("neg"), Some(&(Some(-4), Some(-13_333))));
        Ok(())
    }

    /// Decimal `SUM`/`AVG` must retract exactly (the `i128` backing-value sum
    /// is exactly invertible): in-place update, delete, retracting the final
    /// non-null contribution restoring SQL `NULL` while a null-valued row keeps
    /// the group alive, and dropping the group with its last row.
    #[test]
    fn retracts_decimal128_sum_avg_by_pk() -> DataFusionResult<()> {
        let registry = MaintainedAggregateRegistry::try_new_with_pk(
            &[decimal_sum_avg_spec()],
            &decimal_schema(),
            &[2],
            usize::MAX,
        )?;
        // a: 1.50(pk1) + 2.50(pk2) + NULL(pk3).
        registry.apply_insert_batches(
            1,
            &[decimal_batch(&[
                ("a", Some(150), 1),
                ("a", Some(250), 2),
                ("a", None, 3),
            ])],
        )?;
        // UPDATE pk2 in place (retract-old-then-apply-new): 2.50 -> 0.25.
        registry.apply_insert_batches(2, &[decimal_batch(&[("a", Some(25), 2)])])?;
        assert_eq!(
            decimal_sum_avg_by_name(&registry, 2)?.get("a"),
            Some(&(Some(175), Some(875_000))),
            "1.50 + 0.25 = 1.75; AVG = 0.875000"
        );
        // DELETE pk1, then pk2: only the NULL row remains, so the group stays
        // alive and both aggregates restore SQL NULL.
        registry.apply_pk_deletes(3, &decimal_batch(&[("", None, 1)]).project(&[2])?)?;
        registry.apply_pk_deletes(4, &decimal_batch(&[("", None, 2)]).project(&[2])?)?;
        assert_eq!(
            decimal_sum_avg_by_name(&registry, 4)?.get("a"),
            Some(&(None, None)),
            "all non-null contributions retracted -> SQL NULL"
        );
        // DELETE pk3 (the NULL row): the group is now empty and disappears.
        registry.apply_pk_deletes(5, &decimal_batch(&[("", None, 3)]).project(&[2])?)?;
        assert_eq!(decimal_sum_avg_by_name(&registry, 5)?.get("a"), None);
        Ok(())
    }

    /// A decimal `SUM` whose exact `i128` running sum overflows fails the apply
    /// pass (the registry falls safe to stale) instead of silently wrapping.
    #[test]
    fn sum_over_decimal128_overflow_fails_safe_to_stale() -> DataFusionResult<()> {
        let wide_schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("amt", DataType::Decimal128(38, 0), true),
        ]));
        let spec = MaintainedAggregateSpec {
            filter: None,
            group_by: vec![],
            aggregates: vec![MaintainedAggregateExpr {
                function: MaintainedAggregateFunction::Sum,
                column: Some("amt".to_string()),
            }],
        };
        let registry = MaintainedAggregateRegistry::try_new(&[spec], &wide_schema)?;
        // 10^38 - 1 is the largest Decimal128(38, 0) value; two of them exceed
        // i128::MAX (~1.7 * 10^38), so the second insert must fail, not wrap.
        let max_decimal = 10_i128.pow(38) - 1;
        let wide_batch = || {
            RecordBatch::try_new(
                Arc::clone(&wide_schema),
                vec![
                    Arc::new(StringArray::from(vec![Some("a")])),
                    Arc::new(
                        Decimal128Array::from(vec![Some(max_decimal)])
                            .with_precision_and_scale(38, 0)
                            .expect("valid decimal precision/scale"),
                    ),
                ],
            )
            .expect("test batch should be valid")
        };
        registry.apply_insert_batches(1, &[wide_batch()])?;
        assert!(registry.apply_insert_batches(2, &[wide_batch()]).is_err());
        Ok(())
    }

    /// A maintained `AVG(Decimal128)` whose serve-time rescale overflows errors
    /// the query — exactly what `DataFusion`'s `DecimalAverager` does for a
    /// base-table scan of the same data, so this is not a divergence.
    #[test]
    fn avg_over_decimal128_result_overflow_errors_like_datafusion() -> DataFusionResult<()> {
        let wide_schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("amt", DataType::Decimal128(38, 0), true),
        ]));
        let spec = MaintainedAggregateSpec {
            filter: None,
            group_by: vec![],
            aggregates: vec![MaintainedAggregateExpr {
                function: MaintainedAggregateFunction::Avg,
                column: Some("amt".to_string()),
            }],
        };
        let registry =
            MaintainedAggregateRegistry::try_new(std::slice::from_ref(&spec), &wide_schema)?;
        // The running sum (10^37) fits i128, but the serve-time rescale to the
        // AVG output scale multiplies by 10^4 and overflows.
        let big_batch = RecordBatch::try_new(
            Arc::clone(&wide_schema),
            vec![
                Arc::new(StringArray::from(vec![Some("a")])),
                Arc::new(
                    Decimal128Array::from(vec![Some(10_i128.pow(37))])
                        .with_precision_and_scale(38, 0)
                        .expect("valid decimal precision/scale"),
                ),
            ],
        )
        .expect("test batch should be valid");
        registry.apply_insert_batches(1, &[big_batch])?;
        let out_schema = Arc::new(Schema::new(vec![Field::new(
            "avg(amt)",
            DataType::Decimal128(38, 4),
            true,
        )]));
        let error = registry
            .batch_for_spec(&spec, 1, out_schema)
            .expect_err("serve-time rescale must overflow");
        // DataFusion's own `DecimalAverager` error — the maintained serve fails
        // with exactly what a base-table re-scan of the same data raises.
        assert!(
            error
                .to_string()
                .contains("Arithmetic Overflow in AvgAccumulator"),
            "unexpected error: {error}"
        );
        Ok(())
    }

    /// `AVG` over a negative-scale decimal stays unsupported (the serve-time
    /// rescale is only meaningful for non-negative input scales), while `SUM` —
    /// which keeps the input scale — accepts it.
    #[test]
    fn avg_over_negative_scale_decimal128_is_rejected() {
        let neg_scale_schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("amt", DataType::Decimal128(6, -2), true),
        ]));
        let avg_spec = MaintainedAggregateSpec {
            filter: None,
            group_by: vec![],
            aggregates: vec![MaintainedAggregateExpr {
                function: MaintainedAggregateFunction::Avg,
                column: Some("amt".to_string()),
            }],
        };
        let error = MaintainedAggregateRegistry::try_new(&[avg_spec], &neg_scale_schema)
            .expect_err("negative-scale decimal AVG is unsupported");
        assert!(
            error
                .to_string()
                .contains("does not support column type Decimal128(6, -2)"),
            "unexpected error: {error}"
        );

        let sum_spec = MaintainedAggregateSpec {
            filter: None,
            group_by: vec![],
            aggregates: vec![MaintainedAggregateExpr {
                function: MaintainedAggregateFunction::Sum,
                column: Some("amt".to_string()),
            }],
        };
        MaintainedAggregateRegistry::try_new(&[sum_spec], &neg_scale_schema)
            .expect("negative-scale decimal SUM keeps the input scale and is supported");
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

    /// `AVG` over a narrow signed-integer column (Postgres `INTEGER` → arrow
    /// `Int32`, the common CDC case) must (a) be accepted at registry
    /// construction and (b) maintain an exact `i128` running sum + count across
    /// insert, in-place update (retract-then-apply-new), and delete, dividing
    /// down to the `Float64` AVG output. Mirrors
    /// [`sum_over_int32_widens_on_insert_and_retract`] for `AvgInt128` — before
    /// integer support this failed planning with "Avg maintained aggregate does
    /// not support column type Int32".
    #[test]
    fn avg_over_int32_maintains_exactly_on_insert_and_retract() -> DataFusionResult<()> {
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
                function: MaintainedAggregateFunction::Avg,
                column: Some("v".to_string()),
            }],
        };
        // PK = column index 2 (`pk`). Construction must succeed for Int32 AVG.
        let registry =
            MaintainedAggregateRegistry::try_new_with_pk(&[spec], &i32_schema, &[2], usize::MAX)?;

        registry
            .apply_insert_batches(1, &[i32_batch(&[("a", 10, 1), ("a", 20, 2), ("b", 5, 3)])])?;
        // UPDATE pk=2 in place (retract-old-then-apply-new): v 20 -> 7, so
        // group a averages (10 + 7) / 2 = 8.5.
        registry.apply_insert_batches(2, &[i32_batch(&[("a", 7, 2)])])?;
        // DELETE pk=3 via a PK-projected batch (PK = column 2): retracts group b.
        registry.apply_pk_deletes(3, &i32_batch(&[("", 0, 3)]).project(&[2])?)?;

        // Serve via a real AggregateExec. DataFusion's AVG over an integer column
        // outputs `Float64`, so the maintained output field is `Float64` too.
        let input = MemorySourceConfig::try_new_exec(
            &[vec![i32_batch(&[])]],
            Arc::clone(&i32_schema),
            None,
        )?;
        let avg_arg = cast(
            col("v", i32_schema.as_ref())?,
            i32_schema.as_ref(),
            DataType::Int64,
        )?;
        let avg_v = AggregateExprBuilder::new(avg_udaf(), vec![avg_arg])
            .schema(Arc::clone(&i32_schema))
            .alias("avg(v)")
            .build()
            .map(Arc::new)?;
        let aggregate = AggregateExec::try_new(
            AggregateMode::Single,
            PhysicalGroupBy::new_single(vec![(
                col("name", i32_schema.as_ref())?,
                "name".to_string(),
            )]),
            vec![avg_v],
            vec![None],
            input,
            Arc::clone(&i32_schema),
        )?;
        let result = registry
            .batch_for_aggregate(&aggregate, 3)?
            .expect("registry should be fresh");
        let names = as_string_array(result.column(0))?;
        let avgs = as_float64_array(result.column(1))?;
        let mut by_name = BTreeMap::new();
        for row in 0..result.num_rows() {
            if !avgs.is_null(row) {
                by_name.insert(names.value(row).to_string(), avgs.value(row));
            }
        }
        let avg_a = by_name.get("a").copied().expect("group a present");
        assert!(
            (avg_a - 8.5).abs() < 1e-9,
            "group a = avg(10, updated 7) = 8.5, got {avg_a}"
        );
        assert_eq!(by_name.get("b"), None, "group b fully retracted by delete");
        Ok(())
    }

    /// The `i128` running sum has the headroom `SumInt64`'s `i64` lacks: two
    /// `Int64` rows at `i64::MAX` sum to ~1.8e19 (past `i64::MAX`), which would
    /// overflow a `SUM`, but `AVG` accumulates in `i128` and returns their
    /// average (`i64::MAX`) as `Float64` without erroring.
    #[test]
    fn avg_over_int64_near_max_does_not_overflow() -> DataFusionResult<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("v", DataType::Int64, true),
            Field::new("pk", DataType::Int64, true),
        ]));
        let batch = |rows: &[(&str, i64, i64)]| {
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(StringArray::from(
                        rows.iter().map(|(n, _, _)| Some(*n)).collect::<Vec<_>>(),
                    )),
                    Arc::new(Int64Array::from(
                        rows.iter().map(|(_, v, _)| *v).collect::<Vec<_>>(),
                    )),
                    Arc::new(Int64Array::from(
                        rows.iter().map(|(_, _, pk)| *pk).collect::<Vec<_>>(),
                    )),
                ],
            )
            .expect("int64 batch should be valid")
        };

        let spec = MaintainedAggregateSpec {
            filter: None,
            group_by: vec!["name".to_string()],
            aggregates: vec![MaintainedAggregateExpr {
                function: MaintainedAggregateFunction::Avg,
                column: Some("v".to_string()),
            }],
        };
        let registry =
            MaintainedAggregateRegistry::try_new_with_pk(&[spec], &schema, &[2], usize::MAX)?;
        // Sum = 2 * i64::MAX overflows i64 but fits comfortably in i128.
        registry.apply_insert_batches(1, &[batch(&[("a", i64::MAX, 1), ("a", i64::MAX, 2)])])?;

        let input =
            MemorySourceConfig::try_new_exec(&[vec![batch(&[])]], Arc::clone(&schema), None)?;
        let avg_v = AggregateExprBuilder::new(avg_udaf(), vec![col("v", schema.as_ref())?])
            .schema(Arc::clone(&schema))
            .alias("avg(v)")
            .build()
            .map(Arc::new)?;
        let aggregate = AggregateExec::try_new(
            AggregateMode::Single,
            PhysicalGroupBy::new_single(vec![(col("name", schema.as_ref())?, "name".to_string())]),
            vec![avg_v],
            vec![None],
            input,
            Arc::clone(&schema),
        )?;
        let result = registry
            .batch_for_aggregate(&aggregate, 1)?
            .expect("registry should be fresh");
        let avgs = as_float64_array(result.column(1))?;
        let avg_a = avgs.value(0);
        #[expect(
            clippy::cast_precision_loss,
            reason = "test-only reference value; avg of two i64::MAX is i64::MAX"
        )]
        let expected = i64::MAX as f64;
        assert!(
            (avg_a - expected).abs() / expected < 1e-15,
            "avg of two i64::MAX values is i64::MAX, got {avg_a}"
        );
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
