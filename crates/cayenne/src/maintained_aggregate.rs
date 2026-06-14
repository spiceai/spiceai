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

use std::any::Any;
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, RecordBatch, new_empty_array};
use arrow_schema::{DataType, FieldRef, SchemaRef};
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::error::Result as DataFusionResult;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode};
use datafusion_common::{DataFusionError, ScalarValue};
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use datafusion_physical_expr::expressions::{Column, Literal};
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
    /// SQL `SUM(column)` for `Int64`, `UInt64`, or `Float64` inputs.
    Sum,
    /// SQL `AVG(column)` for `Float64` inputs.
    Avg,
}

/// Shared maintained aggregate state for a single Cayenne table.
#[derive(Debug)]
pub struct MaintainedAggregateRegistry {
    state: RwLock<RegistryState>,
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
    groups: HashMap<Vec<ScalarValue>, GroupAccumulator>,
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

    fn as_any(&self) -> &dyn Any {
        self
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
        let views = specs
            .iter()
            .map(|spec| MaintainedAggregateView::try_new(spec, schema))
            .collect::<DataFusionResult<Vec<_>>>()?;

        Ok(Self {
            state: RwLock::new(RegistryState {
                epoch: 0,
                status: RegistryStatus::Fresh,
                views,
            }),
        })
    }

    /// Returns true when no maintained aggregate views are configured.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.state.read().views.is_empty()
    }

    /// Mark all maintained aggregate views stale at `epoch`.
    pub fn mark_stale(&self, epoch: u64) {
        let mut state = self.state.write();
        state.epoch = epoch;
        state.status = RegistryStatus::Stale;
    }

    /// Apply positive row deltas if the state is fresh, otherwise keep it stale.
    ///
    /// # Errors
    ///
    /// Returns an error if a maintained accumulator overflows or Arrow scalar
    /// extraction fails. The caller should then mark the registry stale.
    pub fn apply_insert_batches(
        &self,
        epoch: u64,
        batches: &[RecordBatch],
    ) -> DataFusionResult<()> {
        let mut state = self.state.write();

        // Async write-path maintenance must apply deltas in visibility-epoch
        // order. If a delayed task observes a skipped or already-advanced epoch,
        // fail safe and force queries back to the base table.
        if epoch != state.epoch.saturating_add(1) {
            state.epoch = state.epoch.max(epoch);
            state.status = RegistryStatus::Stale;
            return Ok(());
        }

        state.epoch = epoch;
        if state.status == RegistryStatus::Stale || state.views.is_empty() {
            return Ok(());
        }

        for batch in batches {
            for view in &mut state.views {
                view.apply_insert_batch(batch)?;
            }
        }

        Ok(())
    }

    /// Rebuild every view from a complete table snapshot.
    ///
    /// # Errors
    ///
    /// Returns an error if a maintained accumulator overflows or Arrow scalar
    /// extraction fails.
    pub fn rebuild_from_batches(
        &self,
        epoch: u64,
        batches: &[RecordBatch],
    ) -> DataFusionResult<()> {
        let mut state = self.state.write();
        state.epoch = epoch;
        for view in &mut state.views {
            view.clear();
        }
        for batch in batches {
            for view in &mut state.views {
                view.apply_insert_batch(batch)?;
            }
        }
        state.status = RegistryStatus::Fresh;
        Ok(())
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
        self.batch_for_aggregate_with_output(aggregate, aggregate, scan_epoch)
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
    ) -> DataFusionResult<Option<RecordBatch>> {
        let Some(query) = query_spec_for_aggregate(query_aggregate) else {
            return Ok(None);
        };

        let state = self.state.read();
        if state.status != RegistryStatus::Fresh || state.epoch != scan_epoch {
            return Ok(None);
        }

        for view in &state.views {
            if view.matches_query(&query) {
                return view.materialize(output_aggregate.schema());
            }
        }

        Ok(None)
    }
}

impl MaintainedAggregateView {
    fn try_new(spec: &MaintainedAggregateSpec, schema: &SchemaRef) -> DataFusionResult<Self> {
        Ok(Self {
            spec: ResolvedAggregateSpec::try_new(spec, schema)?,
            groups: HashMap::new(),
        })
    }

    fn clear(&mut self) {
        self.groups.clear();
    }

    fn apply_insert_batch(&mut self, batch: &RecordBatch) -> DataFusionResult<()> {
        if batch.num_rows() == 0 {
            return Ok(());
        }

        for row in 0..batch.num_rows() {
            let mut key = Vec::with_capacity(self.spec.group_by.len());
            for group_col in &self.spec.group_by {
                let scalar = ScalarValue::try_from_array(batch.column(group_col.index), row)?;
                key.push(scalar);
            }

            let group = match self.groups.entry(key) {
                Entry::Occupied(entry) => entry.into_mut(),
                Entry::Vacant(entry) => entry.insert(GroupAccumulator::try_new(&self.spec)?),
            };
            group.apply_insert_row(batch, row)?;
        }

        Ok(())
    }

    fn matches_query(&self, query: &QueryAggregateSpec) -> bool {
        self.spec
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
            (MaintainedAggregateFunction::Sum, Some(DataType::Int64)) => AggregateOutputType::Int64,
            (MaintainedAggregateFunction::Sum, Some(DataType::UInt64)) => {
                AggregateOutputType::UInt64
            }
            (
                MaintainedAggregateFunction::Sum | MaintainedAggregateFunction::Avg,
                Some(DataType::Float64),
            ) => AggregateOutputType::Float64,
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
        Ok(Self { aggregates })
    }

    fn apply_insert_row(&mut self, batch: &RecordBatch, row: usize) -> DataFusionResult<()> {
        for aggregate in &mut self.aggregates {
            aggregate.apply_insert_row(batch, row)?;
        }
        Ok(())
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
                    let ScalarValue::Int64(Some(delta)) = scalar else {
                        return Err(type_mismatch("Int64", &scalar));
                    };
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
                    let ScalarValue::UInt64(Some(delta)) = scalar else {
                        return Err(type_mismatch("UInt64", &scalar));
                    };
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
                    let ScalarValue::Float64(Some(delta)) = scalar else {
                        return Err(type_mismatch("Float64", &scalar));
                    };
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
                    let ScalarValue::Float64(Some(delta)) = scalar else {
                        return Err(type_mismatch("Float64", &scalar));
                    };
                    *sum += delta;
                    *count = count.checked_add(1).ok_or_else(count_overflow)?;
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
        let column = expr.as_any().downcast_ref::<Column>()?;
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
                let column = expr.as_any().downcast_ref::<Column>()?;
                Some(input_column_name(&input_schema, column)?)
            }
        };

        aggregates.push(QueryAggregateExpr { function, column });
    }

    Some(QueryAggregateSpec {
        group_by,
        aggregates,
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
    if let Some(column) = expr.as_any().downcast_ref::<Column>() {
        return Some(CountQueryColumn::Column(input_column_name(
            input_schema,
            column,
        )?));
    }
    if let Some(literal) = expr.as_any().downcast_ref::<Literal>() {
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

fn type_mismatch(expected: &'static str, scalar: &ScalarValue) -> DataFusionError {
    DataFusionError::Execution(format!(
        "Maintained aggregate expected {expected} input but received {scalar:?}"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    use arrow::array::{Float64Array, Int64Array, StringArray, UInt64Array};
    use arrow_schema::{Field, Schema};
    use datafusion::physical_expr::aggregate::AggregateExprBuilder;
    use datafusion::physical_expr::expressions::{col, lit};
    use datafusion::physical_plan::aggregates::PhysicalGroupBy;
    use datafusion_common::cast::{as_float64_array, as_int64_array, as_string_array};
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

    #[test]
    fn count_null_literal_is_not_rewritten_as_count_all() -> DataFusionResult<()> {
        let spec = MaintainedAggregateSpec {
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
}
