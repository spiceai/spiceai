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

//! Rejects a NULL primary key, and a key a single write repeats, before the
//! write reaches `DuckDB` or `SQLite`.
//!
//! A primary-key table still upserts a key it already stores — that default is
//! unchanged. This layer only refuses what those engines already treat as a
//! constraint error (or, for `SQLite`, silently accept): a null key, and a key
//! repeated inside one statement.
//!
//! `SQLite`'s sink reports a failed input stream as `Query is not read-only`
//! (#14757): it writes on a blocking thread and treats a closed batch channel
//! as `InvalidQuery` before the stream's error is read. [`GuardStrategy::CollectThenInsert`]
//! validates the stream first and only then plans the insert, so a null key,
//! an in-batch duplicate, or a parse error is the error the caller sees.

use std::{collections::HashSet, sync::Arc};

use arrow::{
    datatypes::{DataType, Field, Schema, SchemaRef},
    record_batch::RecordBatch,
};
use async_trait::async_trait;
use datafusion::{
    catalog::Session,
    common::{Constraint, Constraints},
    datasource::{TableProvider, memory::MemorySourceConfig, source::DataSourceExec},
    error::DataFusionError,
    execution::{
        SessionState, TaskContext,
        memory_pool::{MemoryConsumer, MemoryReservation},
    },
    logical_expr::dml::InsertOp,
    physical_expr::EquivalenceProperties,
    physical_plan::{
        DisplayAs, DisplayFormatType, Distribution, ExecutionPlan, ExecutionPlanProperties,
        Partitioning, PlanProperties, SendableRecordBatchStream,
        coalesce_partitions::CoalescePartitionsExec,
        stream::{RecordBatchReceiverStream, RecordBatchStreamAdapter},
    },
    scalar::ScalarValue,
};
use datafusion_table_providers::util::constraints::Error as ConstraintError;
use futures::{StreamExt, TryStreamExt};
use spice_table::{LayerWalk, SpiceTable, TableLayer};

/// How the guard feeds a validated write to the engine beneath.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GuardStrategy {
    /// Validate each batch as it flows into the engine (`DuckDB` reports a
    /// stream error as itself).
    Stream,
    /// Drain and validate the write, then insert the kept batches (`SQLite`
    /// otherwise mis-reports a failed stream as `Query is not read-only`).
    CollectThenInsert,
}

/// Layers [`PkWriteGuard`] over `provider` when `constraints` name a primary
/// key. `rewrite_dataset` maps `DuckDB` `__data_<name>_<n>` names in write
/// errors back to `name` even when the table has no primary key; `None`
/// leaves messages unchanged.
#[must_use]
pub fn wrap_with_pk_write_guard_if_needed(
    provider: Arc<dyn TableProvider>,
    schema: &Schema,
    constraints: &Constraints,
    strategy: GuardStrategy,
    rewrite_dataset: Option<String>,
) -> Arc<dyn TableProvider> {
    let key_sets = primary_key_sets(schema, constraints);
    let provider = if key_sets.is_empty() {
        provider
    } else {
        SpiceTable::over(Arc::new(PkWriteGuard { key_sets, strategy }), provider)
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
#[must_use]
pub fn rewrite_internal_data_table_error(error: DataFusionError, dataset: &str) -> DataFusionError {
    let message = error.to_string();
    let rewritten = rewrite_internal_data_table_names(&message, dataset);
    if rewritten == message {
        error
    } else {
        DataFusionError::Execution(rewritten)
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

fn session_state(state: &dyn Session) -> datafusion::error::Result<SessionState> {
    state
        .as_any()
        .downcast_ref::<SessionState>()
        .cloned()
        .ok_or_else(|| {
            DataFusionError::Internal(
                "a primary-key write guard requires a DataFusion session state".to_string(),
            )
        })
}

fn count_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        "count",
        DataType::UInt64,
        false,
    )]))
}

fn memory_input(
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
    let partitions = if batches.is_empty() {
        vec![vec![]]
    } else {
        vec![batches]
    };
    let source = MemorySourceConfig::try_new(&partitions, schema, None)?;
    Ok(Arc::new(DataSourceExec::new(Arc::new(source))))
}

/// Validates primary-key columns on a write; see the module docs.
#[derive(Debug)]
pub struct PkWriteGuard {
    key_sets: Vec<Vec<String>>,
    strategy: GuardStrategy,
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
        match self.strategy {
            GuardStrategy::Stream => {
                let exec = PkWriteGuardExec::new(input, Arc::clone(&key_indices));
                below.insert_into(state, Arc::new(exec), op).await
            }
            GuardStrategy::CollectThenInsert => Ok(Arc::new(PkCollectThenInsertExec::new(
                input,
                Arc::clone(below),
                session_state(state)?,
                op,
                key_indices,
            ))),
        }
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

/// The keys one write has already admitted, so a later row repeating any
/// primary key fails rather than racing the engine.
struct SeenKeys {
    keys: Vec<KeySet>,
}

struct KeySet {
    indices: Vec<usize>,
    admitted: HashSet<Vec<ScalarValue>>,
}

impl SeenKeys {
    fn new(key_indices: &[Vec<usize>]) -> Self {
        Self {
            keys: key_indices
                .iter()
                .map(|indices| KeySet {
                    indices: indices.clone(),
                    admitted: HashSet::new(),
                })
                .collect(),
        }
    }

    fn admit(&mut self, batch: &RecordBatch) -> datafusion::error::Result<()> {
        for key in &mut self.keys {
            for row in 0..batch.num_rows() {
                let values = key
                    .indices
                    .iter()
                    .map(|&index| ScalarValue::try_from_array(batch.column(index), row))
                    .collect::<datafusion::error::Result<Vec<_>>>()?;
                if values.iter().any(ScalarValue::is_null) {
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

/// Validates each batch, then forwards it to the engine beneath.
#[derive(Debug)]
struct PkWriteGuardExec {
    input: Arc<dyn ExecutionPlan>,
    key_indices: Arc<[Vec<usize>]>,
    properties: Arc<PlanProperties>,
}

impl PkWriteGuardExec {
    fn new(input: Arc<dyn ExecutionPlan>, key_indices: Arc<[Vec<usize>]>) -> Self {
        let input = if input.output_partitioning().partition_count() > 1 {
            Arc::new(CoalescePartitionsExec::new(input)) as Arc<dyn ExecutionPlan>
        } else {
            input
        };
        Self {
            properties: Arc::clone(input.properties()),
            input,
            key_indices,
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
        Ok(Arc::new(Self::new(child, Arc::clone(&self.key_indices))))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion::error::Result<SendableRecordBatchStream> {
        let mut input = self.input.execute(partition, context)?;
        let schema = self.schema();
        let mut seen = SeenKeys::new(&self.key_indices);
        let mut builder = RecordBatchReceiverStream::builder(Arc::clone(&schema), 2);
        let tx = builder.tx();
        builder.spawn(async move {
            while let Some(batch) = input.next().await {
                let batch = batch?;
                seen.admit(&batch)?;
                if tx.send(Ok(batch)).await.is_err() {
                    break;
                }
            }
            Ok(())
        });
        Ok(builder.build())
    }
}

/// Drains the input, validates it, then inserts into `below` so `SQLite` never
/// sees a failed stream.
#[derive(Debug)]
struct PkCollectThenInsertExec {
    input: Arc<dyn ExecutionPlan>,
    below: Arc<dyn TableProvider>,
    session: SessionState,
    op: InsertOp,
    key_indices: Arc<[Vec<usize>]>,
    properties: Arc<PlanProperties>,
}

impl PkCollectThenInsertExec {
    fn new(
        input: Arc<dyn ExecutionPlan>,
        below: Arc<dyn TableProvider>,
        session: SessionState,
        op: InsertOp,
        key_indices: Arc<[Vec<usize>]>,
    ) -> Self {
        let input = if input.output_partitioning().partition_count() > 1 {
            Arc::new(CoalescePartitionsExec::new(input)) as Arc<dyn ExecutionPlan>
        } else {
            input
        };
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(count_schema()),
            Partitioning::UnknownPartitioning(1),
            input.pipeline_behavior(),
            input.boundedness(),
        ));
        Self {
            input,
            below,
            session,
            op,
            key_indices,
            properties,
        }
    }
}

impl DisplayAs for PkCollectThenInsertExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "PkCollectThenInsertExec")
    }
}

impl ExecutionPlan for PkCollectThenInsertExec {
    fn name(&self) -> &'static str {
        "PkCollectThenInsertExec"
    }

    fn schema(&self) -> SchemaRef {
        count_schema()
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

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let [child] = <[_; 1]>::try_from(children).map_err(|_| {
            DataFusionError::Internal(
                "PkCollectThenInsertExec requires exactly one child".to_string(),
            )
        })?;
        Ok(Arc::new(Self::new(
            child,
            Arc::clone(&self.below),
            self.session.clone(),
            self.op,
            Arc::clone(&self.key_indices),
        )))
    }

    fn execute(
        &self,
        _partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion::error::Result<SendableRecordBatchStream> {
        let input = Arc::clone(&self.input);
        let below = Arc::clone(&self.below);
        let session = self.session.clone();
        let op = self.op;
        let key_indices = Arc::clone(&self.key_indices);
        let schema = input.schema();
        let stream = futures::stream::once(async move {
            let mut reservation =
                MemoryConsumer::new("PkCollectThenInsertExec").register(context.memory_pool());
            let mut input = input.execute(0, Arc::clone(&context))?;
            let mut batches = Vec::new();
            let mut seen = SeenKeys::new(&key_indices);
            while let Some(batch) = input.next().await {
                let batch = batch?;
                seen.admit(&batch)?;
                grow_reservation(&mut reservation, &batch)?;
                batches.push(batch);
            }
            let write = below
                .insert_into(&session, memory_input(schema, batches)?, op)
                .await?;
            write.execute(0, context)
        })
        .try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            count_schema(),
            stream,
        )))
    }
}

fn grow_reservation(
    reservation: &mut MemoryReservation,
    batch: &RecordBatch,
) -> datafusion::error::Result<()> {
    reservation.try_grow(batch.get_array_memory_size())
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
        let plan = below.insert_into(state, input, op).await?;
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
        GuardStrategy, SeenKeys, null_primary_key_message, rewrite_internal_data_table_names,
        wrap_with_pk_write_guard_if_needed,
    };
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::catalog::MemTable;
    use datafusion::common::{Constraint, Constraints};
    use datafusion::datasource::TableProvider;
    use spice_table::{LayerWalk, find_layer};
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
        let mut seen = SeenKeys::new(&[vec![0]]);
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
        let mut seen = SeenKeys::new(&[vec![0]]);
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
    fn admit_rejects_a_key_repeated_across_batches() {
        let mut seen = SeenKeys::new(&[vec![0]]);
        seen.admit(&batch(&[(Some(1), "a")]))
            .expect("first copy is new");
        let error = seen
            .admit(&batch(&[(Some(1), "b")]))
            .expect_err("second copy");
        assert!(error.to_string().contains("uniqueness"), "{error}");
    }

    #[test]
    fn only_a_primary_key_installs_the_guard() {
        let inner = Arc::new(MemTable::try_new(schema(), vec![vec![]]).expect("memtable"))
            as Arc<dyn TableProvider>;
        let with_pk = wrap_with_pk_write_guard_if_needed(
            Arc::clone(&inner),
            &schema(),
            &pk(),
            GuardStrategy::Stream,
            None,
        );
        assert!(find_layer::<super::PkWriteGuard>(with_pk.as_ref(), LayerWalk::Write).is_some());

        let without = wrap_with_pk_write_guard_if_needed(
            inner,
            &schema(),
            &Constraints::new_unverified(vec![]),
            GuardStrategy::Stream,
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
            GuardStrategy::Stream,
            Some("o_duckdb".to_string()),
        );
        assert!(find_layer::<super::PkWriteGuard>(wrapped.as_ref(), LayerWalk::Write).is_none());
        assert!(
            find_layer::<super::RewriteInternalTableNames>(wrapped.as_ref(), LayerWalk::Write)
                .is_some()
        );
    }
}
