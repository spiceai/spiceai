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

//! Rejects a NULL primary key, and a key a single batch repeats, before the
//! write reaches `DuckDB` or `SQLite`.
//!
//! A primary-key table still upserts a key it already stores — that default is
//! unchanged. This layer only refuses what those engines already treat as a
//! constraint error (or, for `SQLite`, silently accept): a null key, and a key
//! repeated inside one batch. A later batch of the same write may repeat a
//! key; that is an upsert, as `test_append_to_accelerator_duckdb` requires.
//!
//! `SQLite` stores a bound NaN as NULL, so under [`NanKey::Null`] a NaN key
//! is refused the same way. `DuckDB` keeps NaN as a value.
//!
//! `SQLite`'s sink reports a failed input stream as `Query is not read-only`
//! (#14757): it writes on a blocking thread and treats a closed batch channel
//! as `InvalidQuery` before the stream's error is read. The guard still
//! streams (one batch at a time, like the sink's `mpsc::channel(1)`), records
//! the first stream or uniqueness error, and prefers that over the sink's
//! `Query is not read-only` lie (#14757) or over the sink wrapping this same
//! guard error (`DuckDB` marks every non-SQL/Plan/Schema stream `Err`
//! retriable). An independent storage failure stays the sink's error. An
//! input-stream error is marked with [`check_and_mark_retriable_error`]
//! before it is stored, matching the `DuckDB` and `SQLite` sinks; a NULL or
//! uniqueness error the guard itself raises stays permanent.

use std::{
    collections::HashSet,
    sync::{Arc, Mutex},
};

use crate::keep_first::NanKey;
use arrow::{
    datatypes::{Schema, SchemaRef},
    record_batch::RecordBatch,
};
use async_trait::async_trait;
use datafusion::{
    catalog::Session,
    common::{Constraint, Constraints},
    datasource::TableProvider,
    error::DataFusionError,
    execution::TaskContext,
    logical_expr::dml::InsertOp,
    physical_plan::{
        DisplayAs, DisplayFormatType, Distribution, ExecutionPlan, ExecutionPlanProperties,
        PlanProperties, SendableRecordBatchStream,
        coalesce_partitions::CoalescePartitionsExec,
        stream::{RecordBatchReceiverStream, RecordBatchStreamAdapter},
    },
    scalar::ScalarValue,
};
use datafusion_table_providers::util::{
    constraints::Error as ConstraintError,
    retriable_error::{check_and_mark_retriable_error, is_retriable_error},
};
use futures::StreamExt;
use spice_table::{LayerWalk, SpiceTable, TableLayer};

/// The first error the guard stream saw, plus whether refresh may retry it.
/// `SQLite`'s sink may replace it with `Query is not read-only` (#14757);
/// [`PreferGuardErrorExec`] puts this back.
#[derive(Debug)]
struct GuardErrorSnapshot {
    message: String,
    retriable: bool,
}

type GuardErrorSlot = Arc<Mutex<Option<GuardErrorSnapshot>>>;

/// Layers [`PkWriteGuard`] over `provider` when `constraints` name a primary
/// key. `nan` is how the engine below stores a NaN in a key column.
/// `rewrite_dataset` maps `DuckDB` `__data_<name>_<n>` names in write
/// errors back to `name` even when the table has no primary key; `None`
/// leaves messages unchanged.
#[must_use]
pub fn wrap_with_pk_write_guard_if_needed(
    provider: Arc<dyn TableProvider>,
    schema: &Schema,
    constraints: &Constraints,
    nan: NanKey,
    rewrite_dataset: Option<String>,
) -> Arc<dyn TableProvider> {
    let key_sets = primary_key_sets(schema, constraints);
    let provider = if key_sets.is_empty() {
        provider
    } else {
        SpiceTable::over(Arc::new(PkWriteGuard { key_sets, nan }), provider)
    };
    wrap_with_internal_name_rewrite(provider, rewrite_dataset)
}

/// Maps `DuckDB` `__data_<dataset>_<n>` names in write errors back to `dataset`.
#[must_use]
pub fn wrap_with_internal_name_rewrite(
    provider: Arc<dyn TableProvider>,
    rewrite_dataset: Option<String>,
) -> Arc<dyn TableProvider> {
    match rewrite_dataset.filter(|dataset| !dataset.is_empty()) {
        Some(dataset) => {
            SpiceTable::over(Arc::new(RewriteInternalTableNames { dataset }), provider)
        }
        None => provider,
    }
}

/// Replaces `DuckDB`'s internal `__data_<dataset>_<unix_ms>` table name with
/// `dataset` so a constraint error names the user table.
#[must_use]
pub fn rewrite_internal_data_table_names(message: &str, dataset: &str) -> String {
    if dataset.is_empty() {
        return message.to_string();
    }
    let needle = format!("__data_{dataset}_");
    let mut rewritten = String::with_capacity(message.len());
    let mut rest = message;
    while let Some(start) = rest.find(&needle) {
        rewritten.push_str(&rest[..start]);
        let after = &rest[start + needle.len()..];
        let digits = after.bytes().take_while(u8::is_ascii_digit).count();
        if digits == 0 {
            rewritten.push_str(&needle);
            rest = after;
            continue;
        }
        rewritten.push_str(dataset);
        rest = &after[digits..];
    }
    rewritten.push_str(rest);
    rewritten
}

/// Rewrites `error`'s display text when it leaks a `__data_<dataset>_*` name.
/// Keeps the retry marker: a refresh that already classified the write as
/// retriable must not become permanent just because the table name changed.
#[must_use]
pub fn rewrite_internal_data_table_error(error: DataFusionError, dataset: &str) -> DataFusionError {
    let message = error.to_string();
    let rewritten = rewrite_internal_data_table_names(&message, dataset);
    if rewritten == message {
        return error;
    }
    let retriable = is_retriable_error(&error);
    let error = DataFusionError::Execution(rewritten);
    if retriable {
        check_and_mark_retriable_error(error)
    } else {
        error
    }
}

fn primary_key_sets(schema: &Schema, constraints: &Constraints) -> Vec<Vec<String>> {
    constraints
        .iter()
        .filter_map(|constraint| {
            let Constraint::PrimaryKey(indices) = constraint else {
                return None;
            };
            indices
                .iter()
                .map(|&index| schema.fields().get(index).map(|field| field.name().clone()))
                .collect::<Option<Vec<_>>>()
                .filter(|columns| !columns.is_empty())
        })
        .collect()
}

const NULL_PK_FIX: &str = "Every primary key column must be non-null: populate it in the source data, or set `primary_key` to columns that are always present. For details, visit https://spiceai.org/docs/features/data-acceleration/constraints";

/// The message a write reports when a primary-key column holds a null.
#[must_use]
pub fn null_primary_key_message(batch: &RecordBatch, pk_indices: &[usize]) -> String {
    let schema = batch.schema();
    let columns: Vec<String> = pk_indices
        .iter()
        .filter(|&&index| {
            batch
                .columns()
                .get(index)
                .is_some_and(|column| column.null_count() > 0)
        })
        .map(|&index| format!("'{}'", schema.field(index).name()))
        .collect();

    match columns.as_slice() {
        [] => format!("Primary key values must be non-null. {NULL_PK_FIX}"),
        [column] => format!("Primary key column {column} has null values. {NULL_PK_FIX}"),
        _ => format!(
            "Primary key columns {} have null values. {NULL_PK_FIX}",
            columns.join(", ")
        ),
    }
}

fn uniqueness_error(schema: &Schema, indices: &[usize]) -> DataFusionError {
    let unique_cols = indices
        .iter()
        .map(|&index| schema.field(index).name().clone())
        .collect();
    DataFusionError::External(Box::new(
        ConstraintError::BatchViolatesUniquenessConstraint { unique_cols },
    ))
}

fn store_guard_error(slot: &GuardErrorSlot, error: &DataFusionError, retriable: bool) {
    let mut stored = slot
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if stored.is_none() {
        *stored = Some(GuardErrorSnapshot {
            message: error.to_string(),
            retriable,
        });
    }
}

/// Marks `error` the way the `DuckDB` and `SQLite` sinks do, records it, and
/// returns the marked error so the stream and the slot agree.
fn remember_input_stream_error(slot: &GuardErrorSlot, error: DataFusionError) -> DataFusionError {
    let error = check_and_mark_retriable_error(error);
    store_guard_error(slot, &error, is_retriable_error(&error));
    error
}

/// Records a NULL or uniqueness error the guard itself raised. Those stay
/// permanent even though they are not SQL/Plan/Schema.
fn remember_constraint_error(slot: &GuardErrorSlot, error: &DataFusionError) {
    store_guard_error(slot, error, false);
}

fn restore_guard_error(snapshot: &GuardErrorSnapshot) -> DataFusionError {
    let error = DataFusionError::Execution(snapshot.message.clone());
    if snapshot.retriable {
        check_and_mark_retriable_error(error)
    } else {
        error
    }
}

fn sink_is_sqlite_closed_channel_lie(error: &DataFusionError) -> bool {
    error.to_string().contains("Query is not read-only")
}

/// The sink forwarded this same guard error (possibly after
/// [`check_and_mark_retriable_error`]).
fn sink_wraps_this_guard_error(sink: &DataFusionError, snapshot: &GuardErrorSnapshot) -> bool {
    !snapshot.message.is_empty() && sink.to_string().contains(&snapshot.message)
}

fn prefer_stored_guard_error(slot: &GuardErrorSlot, sink: DataFusionError) -> DataFusionError {
    let stored = slot
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    match stored.as_ref() {
        // Restore only SQLite's closed-channel lie (#14757) or the sink
        // wrapping this same guard error (`DuckDB` marks every
        // non-SQL/Plan/Schema stream `Err` retriable, including a NULL or
        // uniqueness the guard raised). An independent storage failure stays
        // the sink's error even when the guard later recorded a permanent
        // snapshot.
        Some(snapshot)
            if sink_is_sqlite_closed_channel_lie(&sink)
                || sink_wraps_this_guard_error(&sink, snapshot) =>
        {
            restore_guard_error(snapshot)
        }
        _ => sink,
    }
}

/// Validates primary-key columns on a write; see the module docs.
#[derive(Debug)]
pub struct PkWriteGuard {
    key_sets: Vec<Vec<String>>,
    nan: NanKey,
}

#[async_trait]
impl TableLayer for PkWriteGuard {
    fn route<'a>(
        &'a self,
        walk: LayerWalk,
        below: &'a Arc<dyn TableProvider>,
    ) -> Option<&'a Arc<dyn TableProvider>> {
        match walk {
            LayerWalk::Read
            | LayerWalk::CdcDetection
            | LayerWalk::Source
            | LayerWalk::RetentionDelete
            | LayerWalk::Index
            | LayerWalk::Passthrough => Some(below),
            LayerWalk::Write => None,
        }
    }

    async fn insert_into(
        &self,
        below: &Arc<dyn TableProvider>,
        state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        op: InsertOp,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let key_indices = key_indices(input.schema().as_ref(), &self.key_sets)?;
        let slot: GuardErrorSlot = Arc::new(Mutex::new(None));
        let exec =
            PkWriteGuardExec::new(input, Arc::clone(&key_indices), self.nan, Arc::clone(&slot));
        let plan = below.insert_into(state, Arc::new(exec), op).await?;
        Ok(Arc::new(PreferGuardErrorExec::new(plan, slot)))
    }
}

fn key_indices(
    schema: &Schema,
    key_sets: &[Vec<String>],
) -> datafusion::error::Result<Arc<[Vec<usize>]>> {
    Ok(key_sets
        .iter()
        .map(|columns| {
            columns
                .iter()
                .map(|column| schema.index_of(column))
                .collect::<Result<Vec<_>, _>>()
        })
        .collect::<Result<Arc<[_]>, _>>()?)
}

/// The keys one batch has already admitted. A later batch of the same write
/// may repeat a key; that is an upsert onto the stored row.
struct SeenKeys {
    keys: Vec<KeySet>,
    nan: NanKey,
}

struct KeySet {
    indices: Vec<usize>,
    admitted: HashSet<Vec<ScalarValue>>,
}

impl SeenKeys {
    fn new(key_indices: &[Vec<usize>], nan: NanKey) -> Self {
        Self {
            keys: key_indices
                .iter()
                .map(|indices| KeySet {
                    indices: indices.clone(),
                    admitted: HashSet::new(),
                })
                .collect(),
            nan,
        }
    }

    fn admit(&mut self, batch: &RecordBatch) -> datafusion::error::Result<()> {
        for key in &mut self.keys {
            key.admitted.clear();
            for row in 0..batch.num_rows() {
                let values = key
                    .indices
                    .iter()
                    .map(|&index| ScalarValue::try_from_array(batch.column(index), row))
                    .collect::<datafusion::error::Result<Vec<_>>>()?;
                if values.iter().any(|value| is_null_key(value, self.nan)) {
                    return Err(DataFusionError::Execution(null_primary_key_message(
                        batch,
                        &key.indices,
                    )));
                }
                if !key.admitted.insert(values) {
                    return Err(uniqueness_error(batch.schema().as_ref(), &key.indices));
                }
            }
        }
        Ok(())
    }
}

fn is_null_key(value: &ScalarValue, nan: NanKey) -> bool {
    if value.is_null() {
        return true;
    }
    if nan != NanKey::Null {
        return false;
    }
    match value {
        ScalarValue::Float16(Some(number)) => number.is_nan(),
        ScalarValue::Float32(Some(number)) => number.is_nan(),
        ScalarValue::Float64(Some(number)) => number.is_nan(),
        _ => false,
    }
}

/// Validates each batch, then forwards it to the engine beneath.
#[derive(Debug)]
struct PkWriteGuardExec {
    input: Arc<dyn ExecutionPlan>,
    key_indices: Arc<[Vec<usize>]>,
    nan: NanKey,
    slot: GuardErrorSlot,
    properties: Arc<PlanProperties>,
}

impl PkWriteGuardExec {
    fn new(
        input: Arc<dyn ExecutionPlan>,
        key_indices: Arc<[Vec<usize>]>,
        nan: NanKey,
        slot: GuardErrorSlot,
    ) -> Self {
        let input = if input.output_partitioning().partition_count() > 1 {
            Arc::new(CoalescePartitionsExec::new(input)) as Arc<dyn ExecutionPlan>
        } else {
            input
        };
        Self {
            properties: Arc::clone(input.properties()),
            input,
            key_indices,
            nan,
            slot,
        }
    }
}

impl DisplayAs for PkWriteGuardExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "PkWriteGuardExec")
    }
}

impl ExecutionPlan for PkWriteGuardExec {
    fn name(&self) -> &'static str {
        "PkWriteGuardExec"
    }

    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    /// The key columns are held as indices, not physical expressions.
    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(
            &Arc<dyn datafusion::physical_plan::PhysicalExpr>,
        ) -> datafusion::error::Result<
            datafusion::common::tree_node::TreeNodeRecursion,
        >,
    ) -> datafusion::error::Result<datafusion::common::tree_node::TreeNodeRecursion> {
        Ok(datafusion::common::tree_node::TreeNodeRecursion::Continue)
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::SinglePartition]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let [child] = <[_; 1]>::try_from(children).map_err(|_| {
            DataFusionError::Internal("PkWriteGuardExec requires exactly one child".to_string())
        })?;
        Ok(Arc::new(Self::new(
            child,
            Arc::clone(&self.key_indices),
            self.nan,
            Arc::clone(&self.slot),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion::error::Result<SendableRecordBatchStream> {
        let mut input = self.input.execute(partition, context)?;
        let schema = self.schema();
        let mut seen = SeenKeys::new(&self.key_indices, self.nan);
        let slot = Arc::clone(&self.slot);
        let mut builder = RecordBatchReceiverStream::builder(Arc::clone(&schema), 1);
        let tx = builder.tx();
        builder.spawn(async move {
            while let Some(item) = input.next().await {
                let batch = match item {
                    Ok(batch) => batch,
                    Err(error) => {
                        return Err(remember_input_stream_error(&slot, error));
                    }
                };
                if let Err(error) = seen.admit(&batch) {
                    remember_constraint_error(&slot, &error);
                    return Err(error);
                }
                if tx.send(Ok(batch)).await.is_err() {
                    break;
                }
            }
            Ok(())
        });
        Ok(builder.build())
    }
}

/// Prefers the guard stream's first error over a sink that reports
/// `Query is not read-only` after that stream failed (#14757), or that
/// wrapped this same guard error. An independent sink failure is left alone.
#[derive(Debug)]
struct PreferGuardErrorExec {
    input: Arc<dyn ExecutionPlan>,
    slot: GuardErrorSlot,
    properties: Arc<PlanProperties>,
}

impl PreferGuardErrorExec {
    fn new(input: Arc<dyn ExecutionPlan>, slot: GuardErrorSlot) -> Self {
        Self {
            properties: Arc::clone(input.properties()),
            input,
            slot,
        }
    }
}

impl DisplayAs for PreferGuardErrorExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "PreferGuardErrorExec")
    }
}

impl ExecutionPlan for PreferGuardErrorExec {
    fn name(&self) -> &'static str {
        "PreferGuardErrorExec"
    }

    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    /// This node rewrites errors; it holds no physical expressions.
    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(
            &Arc<dyn datafusion::physical_plan::PhysicalExpr>,
        ) -> datafusion::error::Result<
            datafusion::common::tree_node::TreeNodeRecursion,
        >,
    ) -> datafusion::error::Result<datafusion::common::tree_node::TreeNodeRecursion> {
        Ok(datafusion::common::tree_node::TreeNodeRecursion::Continue)
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let [child] = <[_; 1]>::try_from(children).map_err(|_| {
            DataFusionError::Internal("PreferGuardErrorExec requires exactly one child".to_string())
        })?;
        Ok(Arc::new(Self::new(child, Arc::clone(&self.slot))))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion::error::Result<SendableRecordBatchStream> {
        let schema = self.schema();
        let slot = Arc::clone(&self.slot);
        let stream = match self.input.execute(partition, context) {
            Ok(stream) => stream,
            Err(error) => return Err(prefer_stored_guard_error(&slot, error)),
        };
        let stream =
            stream.map(move |item| item.map_err(|error| prefer_stored_guard_error(&slot, error)));
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

/// Maps `DuckDB` `__data_<dataset>_<n>` names in write errors back to `dataset`.
#[derive(Debug)]
struct RewriteInternalTableNames {
    dataset: String,
}

#[async_trait]
impl TableLayer for RewriteInternalTableNames {
    fn route<'a>(
        &'a self,
        walk: LayerWalk,
        below: &'a Arc<dyn TableProvider>,
    ) -> Option<&'a Arc<dyn TableProvider>> {
        match walk {
            LayerWalk::Read
            | LayerWalk::CdcDetection
            | LayerWalk::Source
            | LayerWalk::RetentionDelete
            | LayerWalk::Index
            | LayerWalk::Passthrough => Some(below),
            LayerWalk::Write => None,
        }
    }

    async fn insert_into(
        &self,
        below: &Arc<dyn TableProvider>,
        state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        op: InsertOp,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let plan = below
            .insert_into(state, input, op)
            .await
            .map_err(|error| rewrite_internal_data_table_error(error, &self.dataset))?;
        Ok(Arc::new(RewriteInternalTableNamesExec::new(
            plan,
            self.dataset.clone(),
        )))
    }
}

/// Rewrites `DuckDB` `__data_<dataset>_<n>` names in the write's errors.
#[derive(Debug)]
struct RewriteInternalTableNamesExec {
    input: Arc<dyn ExecutionPlan>,
    dataset: String,
    properties: Arc<PlanProperties>,
}

impl RewriteInternalTableNamesExec {
    fn new(input: Arc<dyn ExecutionPlan>, dataset: String) -> Self {
        Self {
            properties: Arc::clone(input.properties()),
            input,
            dataset,
        }
    }
}

impl DisplayAs for RewriteInternalTableNamesExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "RewriteInternalTableNamesExec: dataset={}", self.dataset)
    }
}

impl ExecutionPlan for RewriteInternalTableNamesExec {
    fn name(&self) -> &'static str {
        "RewriteInternalTableNamesExec"
    }

    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    /// This node rewrites errors; it holds no physical expressions.
    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(
            &Arc<dyn datafusion::physical_plan::PhysicalExpr>,
        ) -> datafusion::error::Result<
            datafusion::common::tree_node::TreeNodeRecursion,
        >,
    ) -> datafusion::error::Result<datafusion::common::tree_node::TreeNodeRecursion> {
        Ok(datafusion::common::tree_node::TreeNodeRecursion::Continue)
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let [child] = <[_; 1]>::try_from(children).map_err(|_| {
            DataFusionError::Internal(
                "RewriteInternalTableNamesExec requires exactly one child".to_string(),
            )
        })?;
        Ok(Arc::new(Self::new(child, self.dataset.clone())))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion::error::Result<SendableRecordBatchStream> {
        let schema = self.schema();
        let dataset = self.dataset.clone();
        let stream = self.input.execute(partition, context)?.map(move |item| {
            item.map_err(|error| rewrite_internal_data_table_error(error, &dataset))
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

#[cfg(test)]
mod tests {
    use super::{
        SeenKeys, is_null_key, null_primary_key_message, rewrite_internal_data_table_error,
        rewrite_internal_data_table_names, wrap_with_internal_name_rewrite,
        wrap_with_pk_write_guard_if_needed,
    };
    use crate::keep_first::NanKey;
    use arrow::array::{Float64Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use async_trait::async_trait;
    use datafusion::catalog::MemTable;
    use datafusion::common::{Constraint, Constraints};
    use datafusion::datasource::TableProvider;
    use datafusion::logical_expr::dml::InsertOp;
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion::scalar::ScalarValue;
    use spice_table::{LayerWalk, SpiceTable, TableLayer, find_layer};
    use std::sync::Arc;

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("v", DataType::Utf8, false),
        ]))
    }

    fn batch(rows: &[(Option<i64>, &str)]) -> RecordBatch {
        RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(Int64Array::from(
                    rows.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    rows.iter().map(|(_, v)| *v).collect::<Vec<_>>(),
                )),
            ],
        )
        .expect("batch")
    }

    fn pk() -> Constraints {
        Constraints::new_unverified(vec![Constraint::PrimaryKey(vec![0])])
    }

    #[test]
    fn rewrite_replaces_the_internal_table_and_keeps_the_column() {
        let message =
            "Constraint Error: NOT NULL constraint failed: __data_o_duckdb_1730000000000.id";
        assert_eq!(
            rewrite_internal_data_table_names(message, "o_duckdb"),
            "Constraint Error: NOT NULL constraint failed: o_duckdb.id"
        );
    }

    #[test]
    fn rewrite_leaves_unrelated_text_unchanged() {
        let message = "Incoming data violates uniqueness constraint on column(s): id";
        assert_eq!(
            rewrite_internal_data_table_names(message, "o_duckdb"),
            message
        );
    }

    #[test]
    fn rewrite_keeps_a_retriable_error_retriable() {
        use datafusion_table_providers::util::retriable_error::{
            check_and_mark_retriable_error, is_retriable_error,
        };

        let marked = check_and_mark_retriable_error(datafusion::error::DataFusionError::Execution(
            "failed to write __data_o_duckdb_1730000000000: connection reset by peer".to_string(),
        ));
        assert!(
            is_retriable_error(&marked),
            "the fixture must start retriable"
        );
        let rewritten = rewrite_internal_data_table_error(marked, "o_duckdb");
        assert!(
            is_retriable_error(&rewritten),
            "rewriting a leaked table name must not drop the retry marker, got: {rewritten}"
        );
        let message = rewritten.to_string();
        assert!(
            !message.contains("__data_"),
            "the rewrite must hide the internal table, got: {message}"
        );
        assert!(
            message.contains("o_duckdb"),
            "the rewrite must name the dataset, got: {message}"
        );
    }

    #[tokio::test]
    async fn rewrite_exec_maps_a_duckdb_constraint_error() {
        use datafusion::physical_plan::collect;
        use datafusion::prelude::SessionContext;
        use datafusion_table_providers::util::test::MockExec;

        let leaked =
            "Constraint Error: NOT NULL constraint failed: __data_o_duckdb_1730000000000.id";
        let input = Arc::new(MockExec::new(
            vec![Err(datafusion::error::DataFusionError::Execution(
                leaked.to_string(),
            ))],
            schema(),
        ));
        let exec = super::RewriteInternalTableNamesExec::new(input, "o_duckdb".to_string());
        let error = collect(Arc::new(exec), SessionContext::new().task_ctx())
            .await
            .expect_err("the leaked DuckDB name must be rewritten");
        let message = error.to_string();
        assert!(
            !message.contains("__data_"),
            "the rewrite layer must hide the internal table, got: {message}"
        );
        assert!(
            message.contains("o_duckdb.id"),
            "the rewrite layer must name the user dataset, got: {message}"
        );
    }

    #[test]
    fn null_message_names_the_column_and_links_the_docs() {
        let message = null_primary_key_message(&batch(&[(None, "nullpk")]), &[0]);
        assert!(
            message.starts_with("Primary key column 'id' has null values."),
            "{message}"
        );
        assert!(
            message.contains("data-acceleration/constraints"),
            "{message}"
        );
    }

    #[test]
    fn admit_rejects_a_null_primary_key() {
        let mut seen = SeenKeys::new(&[vec![0]], NanKey::Value);
        let error = seen
            .admit(&batch(&[(None, "nullpk")]))
            .expect_err("NULL primary key");
        let message = error.to_string();
        assert!(
            message.contains("Primary key") && message.contains("null"),
            "{message}"
        );
    }

    #[test]
    fn admit_rejects_an_in_batch_duplicate_key() {
        let mut seen = SeenKeys::new(&[vec![0]], NanKey::Value);
        let error = seen
            .admit(&batch(&[(Some(1), "a"), (Some(1), "b")]))
            .expect_err("duplicate key");
        assert!(
            error
                .to_string()
                .contains("Incoming data violates uniqueness constraint on column(s): id"),
            "{error}"
        );
    }

    #[test]
    fn admit_allows_a_key_repeated_across_batches() {
        let mut seen = SeenKeys::new(&[vec![0]], NanKey::Value);
        seen.admit(&batch(&[(Some(1), "a")]))
            .expect("first batch is new");
        seen.admit(&batch(&[(Some(1), "b")]))
            .expect("a later batch may upsert the same key");
    }

    #[test]
    fn admit_rejects_a_nan_key_when_the_engine_stores_nan_as_null() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Float64, true),
            Field::new("v", DataType::Utf8, false),
        ]));
        let nan_batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Float64Array::from(vec![Some(f64::NAN)])),
                Arc::new(StringArray::from(vec!["nan"])),
            ],
        )
        .expect("nan batch");
        let mut sqlite = SeenKeys::new(&[vec![0]], NanKey::Null);
        let error = sqlite
            .admit(&nan_batch)
            .expect_err("SQLite stores a NaN as NULL");
        assert!(
            error.to_string().contains("Primary key") && error.to_string().contains("null"),
            "{error}"
        );

        let mut duckdb = SeenKeys::new(&[vec![0]], NanKey::Value);
        duckdb
            .admit(&nan_batch)
            .expect("DuckDB keeps a NaN as a value");
    }

    #[test]
    fn is_null_key_treats_nan_as_null_only_for_sqlite() {
        let nan = ScalarValue::Float64(Some(f64::NAN));
        assert!(is_null_key(&nan, NanKey::Null));
        assert!(!is_null_key(&nan, NanKey::Value));
        assert!(is_null_key(&ScalarValue::Float64(None), NanKey::Value));
    }

    #[test]
    fn only_a_primary_key_installs_the_guard() {
        let inner = Arc::new(MemTable::try_new(schema(), vec![vec![]]).expect("memtable"))
            as Arc<dyn TableProvider>;
        let with_pk = wrap_with_pk_write_guard_if_needed(
            Arc::clone(&inner),
            &schema(),
            &pk(),
            NanKey::Value,
            None,
        );
        assert!(find_layer::<super::PkWriteGuard>(with_pk.as_ref(), LayerWalk::Write).is_some());

        let without = wrap_with_pk_write_guard_if_needed(
            inner,
            &schema(),
            &Constraints::new_unverified(vec![]),
            NanKey::Value,
            None,
        );
        assert!(find_layer::<super::PkWriteGuard>(without.as_ref(), LayerWalk::Write).is_none());
    }

    #[test]
    fn a_dataset_name_installs_the_rewrite_layer_without_a_primary_key() {
        let inner = Arc::new(MemTable::try_new(schema(), vec![vec![]]).expect("memtable"))
            as Arc<dyn TableProvider>;
        let wrapped = wrap_with_pk_write_guard_if_needed(
            inner,
            &schema(),
            &Constraints::new_unverified(vec![]),
            NanKey::Value,
            Some("o_duckdb".to_string()),
        );
        assert!(find_layer::<super::PkWriteGuard>(wrapped.as_ref(), LayerWalk::Write).is_none());
        assert!(
            find_layer::<super::RewriteInternalTableNames>(wrapped.as_ref(), LayerWalk::Write)
                .is_some()
        );
    }

    #[derive(Debug)]
    struct FailAtPlanTime;

    #[async_trait]
    impl TableLayer for FailAtPlanTime {
        fn route<'a>(
            &'a self,
            walk: LayerWalk,
            below: &'a Arc<dyn TableProvider>,
        ) -> Option<&'a Arc<dyn TableProvider>> {
            match walk {
                LayerWalk::Read
                | LayerWalk::CdcDetection
                | LayerWalk::Source
                | LayerWalk::RetentionDelete
                | LayerWalk::Index
                | LayerWalk::Passthrough => Some(below),
                LayerWalk::Write => None,
            }
        }

        async fn insert_into(
            &self,
            _below: &Arc<dyn TableProvider>,
            _state: &dyn datafusion::catalog::Session,
            _input: Arc<dyn ExecutionPlan>,
            _op: InsertOp,
        ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
            Err(datafusion::error::DataFusionError::Execution(
                "Constraint Error: NOT NULL constraint failed: __data_o_duckdb_1730000000000.id"
                    .to_string(),
            ))
        }
    }

    #[tokio::test]
    async fn rewrite_maps_a_plan_time_insert_into_error() {
        use datafusion::prelude::SessionContext;
        use datafusion_table_providers::util::test::MockExec;

        let inner = Arc::new(MemTable::try_new(schema(), vec![vec![]]).expect("memtable"))
            as Arc<dyn TableProvider>;
        let leaking = SpiceTable::over(Arc::new(FailAtPlanTime), inner);
        let wrapped = wrap_with_internal_name_rewrite(leaking, Some("o_duckdb".to_string()));
        let ctx = SessionContext::new();
        let input = Arc::new(MockExec::new(vec![Ok(batch(&[(Some(1), "a")]))], schema()));
        let error = wrapped
            .insert_into(&ctx.state(), input, InsertOp::Append)
            .await
            .expect_err("a plan-time DuckDB name must be rewritten");
        let message = error.to_string();
        assert!(
            !message.contains("__data_"),
            "plan-time rewrite must hide the internal table, got: {message}"
        );
        assert!(
            message.contains("o_duckdb.id"),
            "plan-time rewrite must name the user dataset, got: {message}"
        );
    }

    #[tokio::test]
    async fn prefer_guard_error_over_query_is_not_read_only() {
        use datafusion::physical_plan::collect;
        use datafusion::prelude::SessionContext;
        use datafusion_table_providers::util::test::MockExec;
        use std::sync::Mutex;

        let slot = Arc::new(Mutex::new(Some(super::GuardErrorSnapshot {
            message: "Incoming data violates uniqueness constraint on column(s): id".to_string(),
            retriable: false,
        })));
        let input = Arc::new(MockExec::new(
            vec![Err(datafusion::error::DataFusionError::Execution(
                "Query is not read-only".to_string(),
            ))],
            schema(),
        ));
        let exec = super::PreferGuardErrorExec::new(input, slot);
        let error = collect(Arc::new(exec), SessionContext::new().task_ctx())
            .await
            .expect_err("the sink's read-only message must yield to the guard");
        let message = error.to_string();
        assert!(
            message.contains("uniqueness"),
            "the guard error must be reported, got: {message}"
        );
        assert!(
            !message.contains("Query is not read-only"),
            "the sink's mis-report must not win, got: {message}"
        );
    }

    #[tokio::test]
    async fn prefer_guard_error_keeps_a_retriable_stream_error() {
        use datafusion::physical_plan::collect;
        use datafusion::prelude::SessionContext;
        use datafusion_table_providers::util::retriable_error::{
            check_and_mark_retriable_error, is_retriable_error,
        };
        use datafusion_table_providers::util::test::MockExec;
        use std::sync::Mutex;

        let source = check_and_mark_retriable_error(datafusion::error::DataFusionError::Execution(
            "source stalled".to_string(),
        ));
        assert!(
            is_retriable_error(&source),
            "the fixture must be a retriable stream error"
        );
        let slot = Arc::new(Mutex::new(None));
        let _ = super::remember_input_stream_error(&slot, source);
        let input = Arc::new(MockExec::new(
            vec![Err(datafusion::error::DataFusionError::Execution(
                "Query is not read-only".to_string(),
            ))],
            schema(),
        ));
        let exec = super::PreferGuardErrorExec::new(input, slot);
        let error = collect(Arc::new(exec), SessionContext::new().task_ctx())
            .await
            .expect_err("the sink's read-only message must yield to the guard");
        assert!(
            is_retriable_error(&error),
            "a source-stall error must stay retriable, got: {error}"
        );
        let message = error.to_string();
        assert!(
            message.contains("source stalled"),
            "the stream error must be reported, got: {message}"
        );
        assert!(
            !message.contains("Query is not read-only"),
            "the sink's mis-report must not win, got: {message}"
        );
    }

    #[tokio::test]
    async fn prefer_guard_error_keeps_uniqueness_permanent() {
        use datafusion::physical_plan::collect;
        use datafusion::prelude::SessionContext;
        use datafusion_table_providers::util::retriable_error::is_retriable_error;
        use datafusion_table_providers::util::test::MockExec;
        use std::sync::Mutex;

        let uniqueness = super::uniqueness_error(&schema(), &[0]);
        assert!(
            !is_retriable_error(&uniqueness),
            "an in-batch duplicate must stay a permanent error"
        );
        let slot = Arc::new(Mutex::new(None));
        super::remember_constraint_error(&slot, &uniqueness);
        let input = Arc::new(MockExec::new(
            vec![Err(datafusion::error::DataFusionError::Execution(
                "Query is not read-only".to_string(),
            ))],
            schema(),
        ));
        let exec = super::PreferGuardErrorExec::new(input, slot);
        let error = collect(Arc::new(exec), SessionContext::new().task_ctx())
            .await
            .expect_err("the sink's read-only message must yield to the guard");
        assert!(
            !is_retriable_error(&error),
            "an in-batch duplicate must stay permanent, got: {error}"
        );
        let message = error.to_string();
        assert!(
            message.contains("uniqueness"),
            "the uniqueness error must be reported, got: {message}"
        );
    }

    #[test]
    fn prefer_guard_error_keeps_uniqueness_permanent_when_sink_marks_it_retriable() {
        use datafusion_table_providers::util::retriable_error::{
            check_and_mark_retriable_error, is_retriable_error,
        };
        use std::sync::Mutex;

        let uniqueness = super::uniqueness_error(&schema(), &[0]);
        let slot = Arc::new(Mutex::new(None));
        super::remember_constraint_error(&slot, &uniqueness);
        let marked = check_and_mark_retriable_error(datafusion::error::DataFusionError::Execution(
            uniqueness.to_string(),
        ));
        assert!(
            is_retriable_error(&marked),
            "the DuckDB sink marks a non-SQL stream error retriable"
        );
        let error = super::prefer_stored_guard_error(&slot, marked);
        assert!(
            !is_retriable_error(&error),
            "an in-batch duplicate must stay permanent after the sink marks it, got: {error}"
        );
        let message = error.to_string();
        assert!(
            message.contains("uniqueness"),
            "the uniqueness error must be reported, got: {message}"
        );
    }

    #[tokio::test]
    async fn unmarked_upstream_execution_error_stays_retriable() {
        use datafusion::physical_plan::collect;
        use datafusion::prelude::SessionContext;
        use datafusion_table_providers::util::retriable_error::is_retriable_error;
        use datafusion_table_providers::util::test::MockExec;
        use std::sync::Mutex;

        let unmarked =
            datafusion::error::DataFusionError::Execution("connection reset by peer".to_string());
        assert!(
            !is_retriable_error(&unmarked),
            "the fixture must be unmarked, as a Flight or connection drop arrives"
        );

        let slot = Arc::new(Mutex::new(None));
        let guard = super::PkWriteGuardExec::new(
            Arc::new(MockExec::new(vec![Err(unmarked)], schema())),
            Arc::from(vec![vec![0]]),
            NanKey::Value,
            Arc::clone(&slot),
        );
        let ctx = SessionContext::new();
        let forwarded = collect(Arc::new(guard), ctx.task_ctx())
            .await
            .expect_err("the unmarked stream error must fail the guard");
        assert!(
            is_retriable_error(&forwarded),
            "the forwarded stream error must be marked retriable, got: {forwarded}"
        );
        assert!(
            forwarded.to_string().contains("connection reset by peer"),
            "the stream error must be reported, got: {forwarded}"
        );

        let prefer = super::PreferGuardErrorExec::new(
            Arc::new(MockExec::new(
                vec![Err(datafusion::error::DataFusionError::Execution(
                    "Query is not read-only".to_string(),
                ))],
                schema(),
            )),
            slot,
        );
        let preferred = collect(Arc::new(prefer), ctx.task_ctx())
            .await
            .expect_err("the sink's read-only message must yield to the guard");
        assert!(
            is_retriable_error(&preferred),
            "PreferGuardErrorExec must not make an unmarked source drop permanent, got: {preferred}"
        );
        let message = preferred.to_string();
        assert!(
            message.contains("connection reset by peer"),
            "the stream error must be reported, got: {message}"
        );
        assert!(
            !message.contains("Query is not read-only"),
            "the sink's mis-report must not win, got: {message}"
        );
    }

    #[tokio::test]
    async fn prefer_guard_error_keeps_an_independent_sink_storage_error() {
        use datafusion::physical_plan::collect;
        use datafusion::prelude::SessionContext;
        use datafusion_table_providers::util::retriable_error::is_retriable_error;
        use datafusion_table_providers::util::test::MockExec;
        use std::sync::Mutex;

        let unmarked =
            datafusion::error::DataFusionError::Execution("connection reset by peer".to_string());
        let slot = Arc::new(Mutex::new(None));
        let _ = super::remember_input_stream_error(&slot, unmarked);
        let input = Arc::new(MockExec::new(
            vec![Err(datafusion::error::DataFusionError::Execution(
                "database or disk is full".to_string(),
            ))],
            schema(),
        ));
        let exec = super::PreferGuardErrorExec::new(input, slot);
        let error = collect(Arc::new(exec), SessionContext::new().task_ctx())
            .await
            .expect_err("an independent sink storage error must still fail the write");
        let message = error.to_string();
        assert!(
            message.contains("database or disk is full"),
            "an independent storage failure must be reported, got: {message}"
        );
        assert!(
            !message.contains("connection reset by peer"),
            "a later input-stream error must not replace an independent sink failure, got: {message}"
        );
        assert!(
            !is_retriable_error(&error),
            "an unmarked storage failure must stay unmarked, got: {error}"
        );
    }

    #[tokio::test]
    async fn prefer_guard_error_keeps_an_independent_sink_storage_error_over_a_permanent_snapshot()
    {
        use datafusion::physical_plan::collect;
        use datafusion::prelude::SessionContext;
        use datafusion_table_providers::util::retriable_error::is_retriable_error;
        use datafusion_table_providers::util::test::MockExec;
        use std::sync::Mutex;

        let ctx = SessionContext::new();
        let permanent = [
            (
                "null",
                datafusion::error::DataFusionError::Execution(null_primary_key_message(
                    &batch(&[(None, "nullpk")]),
                    &[0],
                )),
            ),
            ("uniqueness", super::uniqueness_error(&schema(), &[0])),
        ];
        for (label, stored) in permanent {
            let slot = Arc::new(Mutex::new(None));
            super::remember_constraint_error(&slot, &stored);
            let input = Arc::new(MockExec::new(
                vec![Err(datafusion::error::DataFusionError::Execution(
                    "database or disk is full".to_string(),
                ))],
                schema(),
            ));
            let exec = super::PreferGuardErrorExec::new(input, slot);
            let error = collect(Arc::new(exec), ctx.task_ctx())
                .await
                .expect_err("an independent sink storage error must still fail the write");
            let message = error.to_string();
            assert!(
                message.contains("database or disk is full"),
                "{label}: an independent storage failure must be reported, got: {message}"
            );
            assert!(
                !message.contains("Primary key") && !message.contains("uniqueness"),
                "{label}: a later permanent guard snapshot must not replace an independent sink failure, got: {message}"
            );
            assert!(
                !is_retriable_error(&error),
                "{label}: an unmarked storage failure must stay unmarked, got: {error}"
            );
        }
    }
}
