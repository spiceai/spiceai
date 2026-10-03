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

//! A table layer that applies `on_conflict: drop` to the keys a single write
//! repeats, before the write reaches the accelerator.
//!
//! `drop` keeps the first copy of a key. An accelerator that resolves it with
//! `INSERT … ON CONFLICT DO NOTHING` only does so against rows already in the
//! table: a key repeated *within* the rows of one statement is either refused
//! outright or resolved in whatever order the engine happens to insert them
//! (`DuckDB` inserts an Arrow scan in parallel). Dropping every repeat after the
//! first one here, in arrival order, leaves the engine only conflicts with rows
//! that were already stored, which it resolves as documented.

use std::{collections::HashMap, sync::Arc};

use arrow::{
    array::{ArrayRef, BooleanArray, RecordBatch},
    buffer::NullBuffer,
    compute::filter_record_batch,
    datatypes::{Schema, SchemaRef},
    row::{RowConverter, SortField},
};
use async_trait::async_trait;
use datafusion::{
    catalog::Session,
    common::{Constraint, Constraints},
    datasource::TableProvider,
    error::DataFusionError,
    execution::{
        SendableRecordBatchStream, TaskContext,
        memory_pool::{MemoryConsumer, MemoryReservation},
    },
    logical_expr::dml::InsertOp,
    physical_plan::{
        DisplayAs, DisplayFormatType, Distribution, ExecutionPlan, ExecutionPlanProperties,
        PlanProperties, coalesce_partitions::CoalescePartitionsExec, metrics::MetricsSet,
        stream::RecordBatchStreamAdapter,
    },
};
use datafusion_table_providers::util::on_conflict::OnConflict;
use futures::StreamExt;
use hashbrown::HashTable;
use spice_table::{LayerWalk, SpiceTable, TableLayer};

/// Layers [`KeepFirst`] over `provider` when `on_conflict` resolves conflicts
/// by dropping the incoming row, so every write keeps only the first copy of
/// each key.
///
/// Returns `provider` unchanged for any other `on_conflict` (or none).
#[must_use]
pub fn wrap_with_keep_first_if_needed<S: std::hash::BuildHasher>(
    provider: Arc<dyn TableProvider>,
    options: &HashMap<String, String, S>,
    schema: &Schema,
    constraints: &Constraints,
) -> Arc<dyn TableProvider> {
    let key_sets = drop_key_sets(options, schema, constraints);
    if key_sets.is_empty() {
        provider
    } else {
        SpiceTable::over(
            Arc::new(KeepFirst {
                key_sets: Arc::from(key_sets),
            }),
            provider,
        )
    }
}

/// The column sets `on_conflict` drops repeats of: the configured target for a
/// single `drop`, or every primary-key and unique constraint when each target
/// is `drop`.
fn drop_key_sets<S: std::hash::BuildHasher>(
    options: &HashMap<String, String, S>,
    schema: &Schema,
    constraints: &Constraints,
) -> Vec<Vec<String>> {
    let Some(Ok(on_conflict)) = options
        .get("on_conflict")
        .map(|value| OnConflict::try_from(value.as_str()))
    else {
        return Vec::new();
    };

    match on_conflict {
        // A target that names no column installs nothing, so the engine
        // refuses it as it would without this layer.
        OnConflict::DoNothing(columns) => columns
            .iter()
            .map(|column| field_name(schema, column))
            .collect::<Option<Vec<_>>>()
            .filter(|columns| !columns.is_empty())
            .into_iter()
            .collect(),
        OnConflict::DoNothingAll => constraints
            .iter()
            .filter_map(|constraint| {
                let (Constraint::PrimaryKey(indices) | Constraint::Unique(indices)) = constraint;
                indices
                    .iter()
                    .map(|&index| schema.fields().get(index).map(|field| field.name().clone()))
                    .collect::<Option<Vec<_>>>()
                    .filter(|columns| !columns.is_empty())
            })
            .collect(),
        OnConflict::Upsert(_) => Vec::new(),
    }
}

/// The field an `on_conflict` target names: the field with exactly that name,
/// else the only one equal to it ignoring ASCII case, which is how `DuckDB` and
/// `SQLite` match the target to a column.
fn field_name(schema: &Schema, column: &str) -> Option<String> {
    if schema.field_with_name(column).is_ok() {
        return Some(column.to_string());
    }
    let mut matches = schema
        .fields()
        .iter()
        .filter(|field| field.name().eq_ignore_ascii_case(column));
    match (matches.next(), matches.next()) {
        (Some(field), None) => Some(field.name().clone()),
        _ => None,
    }
}

/// Keeps the first copy of each key a write repeats; see the module docs.
#[derive(Debug)]
pub struct KeepFirst {
    key_sets: Arc<[Vec<String>]>,
}

#[async_trait]
impl TableLayer for KeepFirst {
    /// Rewrites writes, so the write walk stops here rather than routing a
    /// write past the filter; every other walk sees through it.
    fn route<'a>(
        &'a self,
        walk: LayerWalk,
        below: &'a Arc<dyn TableProvider>,
    ) -> Option<&'a Arc<dyn TableProvider>> {
        // Exhaustive on purpose: a wildcard would answer a future walk kind
        // for this layer without anyone deciding what it should say.
        match walk {
            LayerWalk::Read
            | LayerWalk::CdcDetection
            | LayerWalk::Source
            | LayerWalk::RetentionDelete
            | LayerWalk::Index => Some(below),
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
        // A row the table rejects against a stored row is never written, but
        // this filter cannot see stored rows, so it would still hold that
        // row's keys against later rows. With one key that is harmless — a
        // later copy conflicts with the same stored row — but with several it
        // drops a row whose only conflict was with a row never written. An
        // overwrite starts from an empty table, so it has no stored rows.
        if self.key_sets.len() > 1 && op != InsertOp::Overwrite {
            return below.insert_into(state, input, op).await;
        }
        let exec = KeepFirstExec::try_new(input, &self.key_sets)?;
        below.insert_into(state, Arc::new(exec), op).await
    }
}

/// Drops every row whose key an earlier row of the same write already
/// carried. Runs as one partition, so "earlier" is arrival order across the
/// whole write rather than within one input partition.
#[derive(Debug)]
struct KeepFirstExec {
    input: Arc<dyn ExecutionPlan>,
    key_indices: Arc<[Vec<usize>]>,
    properties: Arc<PlanProperties>,
}

impl KeepFirstExec {
    fn try_new(
        input: Arc<dyn ExecutionPlan>,
        key_sets: &[Vec<String>],
    ) -> datafusion::error::Result<Self> {
        let schema = input.schema();
        let key_indices = key_sets
            .iter()
            .map(|columns| {
                columns
                    .iter()
                    .map(|column| schema.index_of(column).map_err(DataFusionError::from))
                    .collect::<datafusion::error::Result<Vec<_>>>()
            })
            .collect::<datafusion::error::Result<Vec<_>>>()?;
        Ok(Self::with_indices(input, Arc::from(key_indices)))
    }

    fn with_indices(input: Arc<dyn ExecutionPlan>, key_indices: Arc<[Vec<usize>]>) -> Self {
        let input = if input.output_partitioning().partition_count() > 1 {
            Arc::new(CoalescePartitionsExec::new(input)) as Arc<dyn ExecutionPlan>
        } else {
            input
        };
        // Dropping rows keeps the input's order and partitioning.
        Self {
            properties: Arc::clone(input.properties()),
            input,
            key_indices,
        }
    }
}

impl DisplayAs for KeepFirstExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let schema = self.input.schema();
        let keys: Vec<String> = self
            .key_indices
            .iter()
            .map(|indices| {
                let names: Vec<&str> = indices
                    .iter()
                    .map(|&index| schema.field(index).name().as_str())
                    .collect();
                format!("[{}]", names.join(", "))
            })
            .collect();
        write!(f, "KeepFirstExec: keys={}", keys.join(", "))
    }
}

impl ExecutionPlan for KeepFirstExec {
    fn name(&self) -> &'static str {
        "KeepFirstExec"
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

    /// "First" is arrival order across the whole write, so the filter takes
    /// the write as one stream.
    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::SinglePartition]
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let [child] = <[_; 1]>::try_from(children).map_err(|_| {
            DataFusionError::Internal("KeepFirstExec requires exactly one child".to_string())
        })?;
        Ok(Arc::new(Self::with_indices(
            child,
            Arc::clone(&self.key_indices),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion::error::Result<SendableRecordBatchStream> {
        let input = self.input.execute(partition, Arc::clone(&context))?;
        let schema = self.schema();
        let reservation = MemoryConsumer::new(format!("KeepFirstExec[{partition}]"))
            .register(context.memory_pool());
        let mut seen = SeenKeys::try_new(&schema, &self.key_indices, reservation)?;
        let stream = input.map(move |batch| seen.keep_first(batch?));
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        self.input.metrics()
    }
}

/// The keys one write has admitted so far, one set per key.
struct SeenKeys {
    keys: Vec<KeyColumns>,
    reservation: MemoryReservation,
}

/// One key's admitted values. Only admitted keys are held: once its batch is
/// written, a dropped row or a row with a NULL in its key costs nothing.
struct KeyColumns {
    indices: Vec<usize>,
    converter: RowConverter,
    admitted: AdmittedKeys,
    /// Each admitted key's index in `admitted`, by the key's hash.
    table: HashTable<usize>,
    hasher: ahash::RandomState,
}

impl KeyColumns {
    fn contains(&self, hash: u64, key: &[u8]) -> bool {
        self.table
            .find(hash, |&index| self.admitted.get(index) == key)
            .is_some()
    }

    fn insert(&mut self, hash: u64, key: &[u8]) {
        let index = self.admitted.push(key);
        let Self {
            table,
            admitted,
            hasher,
            ..
        } = self;
        table.insert_unique(hash, index, |&index| hasher.hash_one(admitted.get(index)));
    }

    fn allocated_size(&self) -> usize {
        self.admitted.allocated_size() + self.table.allocation_size() + self.converter.size()
    }
}

/// The encoded bytes of every admitted key, back to back.
#[derive(Default)]
struct AdmittedKeys {
    bytes: Vec<u8>,
    len: usize,
    layout: KeyLayout,
}

/// Where each admitted key sits in [`AdmittedKeys::bytes`]. A key of a
/// fixed-width type encodes to the same width every time, so its position
/// follows from that width alone; the first key of another width switches to
/// recording where each key ends.
#[derive(Default)]
enum KeyLayout {
    #[default]
    Empty,
    Fixed(usize),
    /// Where each key ends; a key starts where the one before it ends.
    Variable(Vec<usize>),
}

impl AdmittedKeys {
    fn get(&self, index: usize) -> &[u8] {
        match &self.layout {
            KeyLayout::Empty => &[],
            KeyLayout::Fixed(width) => &self.bytes[index * width..(index + 1) * width],
            KeyLayout::Variable(ends) => {
                let start = index.checked_sub(1).map_or(0, |previous| ends[previous]);
                &self.bytes[start..ends[index]]
            }
        }
    }

    /// Appends `key`, returning its index.
    fn push(&mut self, key: &[u8]) -> usize {
        let index = self.len;
        match &self.layout {
            KeyLayout::Empty => self.layout = KeyLayout::Fixed(key.len()),
            KeyLayout::Fixed(width) if *width != key.len() => {
                let width = *width;
                self.layout = KeyLayout::Variable((1..=index).map(|n| n * width).collect());
            }
            KeyLayout::Fixed(_) | KeyLayout::Variable(_) => {}
        }
        self.bytes.extend_from_slice(key);
        if let KeyLayout::Variable(ends) = &mut self.layout {
            ends.push(self.bytes.len());
        }
        self.len += 1;
        index
    }

    fn allocated_size(&self) -> usize {
        let ends = match &self.layout {
            KeyLayout::Variable(ends) => ends.capacity() * std::mem::size_of::<usize>(),
            KeyLayout::Empty | KeyLayout::Fixed(_) => 0,
        };
        self.bytes.capacity() + ends
    }
}

impl SeenKeys {
    fn try_new(
        schema: &Schema,
        key_indices: &[Vec<usize>],
        reservation: MemoryReservation,
    ) -> datafusion::error::Result<Self> {
        let keys = key_indices
            .iter()
            .map(|indices| {
                let fields = indices
                    .iter()
                    .map(|&index| SortField::new(schema.field(index).data_type().clone()))
                    .collect();
                Ok(KeyColumns {
                    indices: indices.clone(),
                    converter: RowConverter::new(fields)?,
                    admitted: AdmittedKeys::default(),
                    table: HashTable::new(),
                    hasher: ahash::RandomState::new(),
                })
            })
            .collect::<datafusion::error::Result<Vec<_>>>()?;
        Ok(Self { keys, reservation })
    }

    fn keep_first(&mut self, batch: RecordBatch) -> datafusion::error::Result<RecordBatch> {
        let num_rows = batch.num_rows();
        if num_rows == 0 {
            return Ok(batch);
        }

        // This batch's encoded keys, and which of its rows have a NULL in a
        // key: such a key never conflicts, as in SQL.
        let mut encoded = Vec::with_capacity(self.keys.len());
        for key in &self.keys {
            let columns: Vec<ArrayRef> = key
                .indices
                .iter()
                .map(|&index| Arc::clone(batch.column(index)))
                .collect();
            let nulls = columns
                .iter()
                .fold(None, |acc: Option<NullBuffer>, column| {
                    NullBuffer::union(acc.as_ref(), column.logical_nulls().as_ref())
                });
            encoded.push((key.converter.convert_columns(&columns)?, nulls));
        }

        let mut keep = Vec::with_capacity(num_rows);
        let mut dropped = 0;
        let mut hashes = vec![None; self.keys.len()];
        for row in 0..num_rows {
            let mut repeated = false;
            for ((key, (rows, nulls)), hash) in self.keys.iter().zip(&encoded).zip(&mut hashes) {
                *hash = None;
                if nulls.as_ref().is_some_and(|nulls| nulls.is_null(row)) {
                    continue;
                }
                let value = rows.row(row);
                let value_hash = key.hasher.hash_one(value.as_ref());
                *hash = Some(value_hash);
                repeated = repeated || key.contains(value_hash, value.as_ref());
            }
            if repeated {
                dropped += 1;
            } else {
                for ((key, (rows, _)), hash) in self.keys.iter_mut().zip(&encoded).zip(&hashes) {
                    if let Some(hash) = *hash {
                        key.insert(hash, rows.row(row).as_ref());
                    }
                }
            }
            keep.push(!repeated);
        }

        self.reservation.try_resize(
            self.keys
                .iter()
                .map(KeyColumns::allocated_size)
                .sum::<usize>(),
        )?;

        if dropped == 0 {
            return Ok(batch);
        }
        Ok(filter_record_batch(&batch, &BooleanArray::from(keep))?)
    }
}

#[cfg(test)]
mod tests {
    use super::{AdmittedKeys, KeepFirst, KeepFirstExec, SeenKeys, wrap_with_keep_first_if_needed};
    use spice_table::SpiceTable;
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow::array::{Array, Int32Array, RecordBatch, StringArray};
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use datafusion::catalog::MemTable;
    use datafusion::common::{Constraint, Constraints};
    use datafusion::datasource::TableProvider;
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::datasource::source::DataSourceExec;
    use datafusion::execution::TaskContext;
    use datafusion::execution::memory_pool::{MemoryConsumer, MemoryPool, UnboundedMemoryPool};
    use datafusion::execution::runtime_env::RuntimeEnvBuilder;
    use datafusion::logical_expr::dml::InsertOp;
    use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties, collect};
    use datafusion::prelude::{SessionConfig, SessionContext};

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("v", DataType::Utf8, true),
        ]))
    }

    fn batch(rows: &[(Option<i32>, &str)]) -> RecordBatch {
        let ids: Vec<Option<i32>> = rows.iter().map(|(id, _)| *id).collect();
        let vals: Vec<&str> = rows.iter().map(|(_, v)| *v).collect();
        RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(Int32Array::from(ids)),
                Arc::new(StringArray::from(vals)),
            ],
        )
        .expect("build batch")
    }

    fn source(partitions: &[Vec<RecordBatch>]) -> Arc<dyn ExecutionPlan> {
        let src = MemorySourceConfig::try_new(partitions, schema(), None).expect("memory source");
        Arc::new(DataSourceExec::new(Arc::new(src)))
    }

    fn ids(batches: &[RecordBatch]) -> Vec<i32> {
        batches
            .iter()
            .flat_map(|batch| {
                let ids = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .expect("id is Int32");
                (0..ids.len()).map(|row| ids.value(row)).collect::<Vec<_>>()
            })
            .collect()
    }

    fn find_keep_first(plan: &Arc<dyn ExecutionPlan>) -> Option<&KeepFirstExec> {
        plan.downcast_ref::<KeepFirstExec>()
            .or_else(|| plan.children().into_iter().find_map(find_keep_first))
    }

    fn has_keep_first(table: &Arc<dyn TableProvider>) -> bool {
        table
            .downcast_ref::<SpiceTable>()
            .and_then(SpiceTable::layer_as::<KeepFirst>)
            .is_some()
    }

    fn pk() -> Constraints {
        Constraints::new_unverified(vec![Constraint::PrimaryKey(vec![0])])
    }

    fn options(on_conflict: &str) -> HashMap<String, String> {
        [("on_conflict".to_string(), on_conflict.to_string())]
            .into_iter()
            .collect()
    }

    /// Writes `input` through a `drop`-wrapped `MemTable` and returns what was
    /// stored, in storage order.
    async fn write_and_read(
        on_conflict: &str,
        constraints: &Constraints,
        input: Arc<dyn ExecutionPlan>,
        ctx: &SessionContext,
    ) -> datafusion::error::Result<Vec<(Option<i32>, String)>> {
        let inner = Arc::new(MemTable::try_new(schema(), vec![vec![]]).expect("memtable"));
        let table = wrap_with_keep_first_if_needed(
            Arc::clone(&inner) as Arc<dyn TableProvider>,
            &options(on_conflict),
            &schema(),
            constraints,
        );
        let plan = table
            .insert_into(&ctx.state(), input, InsertOp::Append)
            .await?;
        collect(plan, ctx.task_ctx()).await?;

        let scan = inner.scan(&ctx.state(), None, &[], None).await?;
        let mut rows = Vec::new();
        for batch in collect(scan, Arc::new(TaskContext::default())).await? {
            let ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("id is Int32");
            let vals = batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("v is Utf8");
            for row in 0..batch.num_rows() {
                let id = ids.is_valid(row).then(|| ids.value(row));
                rows.push((id, vals.value(row).to_string()));
            }
        }
        Ok(rows)
    }

    #[tokio::test]
    async fn drop_keeps_the_first_copy_of_a_key_repeated_within_a_batch() {
        let input = source(&[vec![batch(&[
            (Some(1), "a"),
            (Some(2), "b"),
            (Some(1), "c"),
        ])]]);
        let rows = write_and_read("do_nothing:id", &pk(), input, &SessionContext::new())
            .await
            .expect("write succeeds");
        assert_eq!(
            rows,
            vec![(Some(1), "a".to_string()), (Some(2), "b".to_string())]
        );
    }

    #[tokio::test]
    async fn drop_keeps_the_first_copy_of_a_key_repeated_across_batches() {
        let input = source(&[vec![
            batch(&[(Some(0), "first"), (Some(1), "first")]),
            batch(&[(Some(0), "last")]),
            batch(&[(Some(1), "last"), (Some(2), "only")]),
        ]]);
        let rows = write_and_read("do_nothing:id", &pk(), input, &SessionContext::new())
            .await
            .expect("write succeeds");
        assert_eq!(
            rows,
            vec![
                (Some(0), "first".to_string()),
                (Some(1), "first".to_string()),
                (Some(2), "only".to_string()),
            ]
        );
    }

    /// Repeats in different input partitions are still caught: the write runs
    /// as one partition, so exactly one copy of each key reaches the table.
    #[tokio::test]
    async fn drop_keeps_one_copy_of_a_key_repeated_across_partitions() {
        let input = source(&[
            vec![batch(&[(Some(0), "p0"), (Some(1), "p0")])],
            vec![batch(&[(Some(0), "p1"), (Some(2), "p1")])],
        ]);
        let mut rows = write_and_read("do_nothing:id", &pk(), input, &SessionContext::new())
            .await
            .expect("write succeeds");
        rows.sort_unstable();
        let ids: Vec<Option<i32>> = rows.iter().map(|(id, _)| *id).collect();
        assert_eq!(ids, vec![Some(0), Some(1), Some(2)]);
    }

    /// A NULL key never conflicts, as in SQL, so every NULL-keyed row is kept.
    #[tokio::test]
    async fn drop_never_treats_null_keys_as_repeats() {
        let input = source(&[vec![batch(&[(None, "a"), (None, "b"), (Some(1), "c")])]]);
        let rows = write_and_read("do_nothing:id", &pk(), input, &SessionContext::new())
            .await
            .expect("write succeeds");
        assert_eq!(rows.len(), 3, "{rows:?}");
    }

    /// Each key is held separately, and a row repeating any one of them is
    /// dropped without admitting its other keys.
    #[tokio::test]
    async fn the_filter_drops_a_row_repeating_any_of_several_keys() {
        let input = source(&[vec![batch(&[
            (Some(1), "a"),
            (Some(1), "b"),
            (Some(2), "a"),
            (Some(3), "b"),
        ])]]);
        let key_sets: Arc<[Vec<String>]> =
            Arc::from(vec![vec!["id".to_string()], vec!["v".to_string()]]);
        let exec = KeepFirstExec::try_new(input, &key_sets).expect("plan");
        let batches = collect(Arc::new(exec), Arc::new(TaskContext::default()))
            .await
            .expect("filter runs");
        // (1, b) repeats id 1; (2, a) repeats v 'a'; (3, b) is new on both,
        // because the dropped (1, b) admitted neither of its keys.
        assert_eq!(ids(&batches), vec![1, 3]);
    }

    /// An append under several `drop` targets is passed through untouched: the
    /// filter cannot tell which incoming rows the table will reject against a
    /// stored row, and holding a rejected row's other keys would drop rows
    /// that conflict with nothing.
    #[tokio::test]
    async fn drop_on_every_target_leaves_an_append_to_the_table() {
        let constraints = Constraints::new_unverified(vec![
            Constraint::PrimaryKey(vec![0]),
            Constraint::Unique(vec![1]),
        ]);
        let input = source(&[vec![batch(&[(Some(1), "a"), (Some(1), "b")])]]);
        let rows = write_and_read(
            "do_nothing_all",
            &constraints,
            input,
            &SessionContext::new(),
        )
        .await
        .expect("write succeeds");
        assert_eq!(
            rows.len(),
            2,
            "both rows reach the table, which resolves them: {rows:?}"
        );
    }

    #[tokio::test]
    async fn an_empty_write_writes_nothing() {
        let input = source(&[vec![]]);
        let rows = write_and_read("do_nothing:id", &pk(), input, &SessionContext::new())
            .await
            .expect("write succeeds");
        assert!(rows.is_empty());
    }

    /// The admitted keys are charged to the memory pool of the session that
    /// runs the write, so under a bounded pool a write whose keys do not fit
    /// fails instead of growing without bound.
    #[tokio::test]
    async fn the_admitted_keys_are_charged_to_the_memory_pool() {
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_limit(1024, 1.0)
            .build_arc()
            .expect("runtime");
        let ctx = SessionContext::new_with_config_rt(SessionConfig::new(), runtime);
        let rows: Vec<(Option<i32>, &str)> = (0..10_000).map(|id| (Some(id), "v")).collect();
        let input = source(&[vec![batch(&rows)]]);
        let error = write_and_read("do_nothing:id", &pk(), input, &ctx)
            .await
            .expect_err("10,000 keys do not fit in 1 KiB");
        assert!(error.to_string().contains("Resources exhausted"), "{error}");
    }

    /// A key of a variable-width type is matched on its full value, whatever
    /// width the keys before it had.
    #[tokio::test]
    async fn the_filter_matches_keys_of_varying_width() {
        let input = source(&[vec![
            batch(&[(Some(1), "a"), (Some(2), "bb"), (Some(3), "a")]),
            batch(&[(Some(4), "ccc"), (Some(5), "bb"), (Some(6), "b")]),
        ]]);
        let key_sets: Arc<[Vec<String>]> = Arc::from(vec![vec!["v".to_string()]]);
        let exec = KeepFirstExec::try_new(input, &key_sets).expect("plan");
        let batches = collect(Arc::new(exec), Arc::new(TaskContext::default()))
            .await
            .expect("filter runs");
        assert_eq!(ids(&batches), vec![1, 2, 4, 6]);
    }

    #[test]
    fn admitted_keys_record_their_ends_from_the_first_key_of_another_width() {
        let mut keys = AdmittedKeys::default();
        assert_eq!(keys.push(b"ab"), 0);
        assert_eq!(keys.push(b"cd"), 1);
        assert_eq!(keys.push(b"efg"), 2);
        assert_eq!(keys.push(b"hi"), 3);
        let stored: Vec<&[u8]> = (0..4).map(|index| keys.get(index)).collect();
        assert_eq!(stored, [b"ab".as_slice(), b"cd", b"efg", b"hi"]);
    }

    /// Only the keys the filter admits are held: a dropped row or a row with a
    /// NULL key leaves nothing behind once its batch is written.
    #[test]
    fn only_admitted_keys_are_held() {
        let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::default());
        let mut seen = SeenKeys::try_new(
            &schema(),
            &[vec![0]],
            MemoryConsumer::new("test").register(&pool),
        )
        .expect("key set");
        let rows: Vec<(Option<i32>, &str)> = (0..10_000)
            .map(|n| ((n % 4 != 3).then_some(n % 4), "v"))
            .collect();
        let kept = seen.keep_first(batch(&rows)).expect("filter");
        // Keys 0, 1 and 2 once each, and all 2,500 NULL-keyed rows.
        assert_eq!(kept.num_rows(), 2_503);
        assert_eq!(seen.keys[0].admitted.len, 3);
    }

    /// `DuckDB` and `SQLite` match an `on_conflict` target to its column
    /// ignoring case, so the filter does too.
    #[tokio::test]
    async fn a_target_spelled_in_another_case_names_its_column() {
        let inner = Arc::new(MemTable::try_new(schema(), vec![vec![]]).expect("memtable"));
        let table =
            wrap_with_keep_first_if_needed(inner, &options("do_nothing:ID"), &schema(), &pk());
        assert!(has_keep_first(&table));

        let input = source(&[vec![batch(&[(Some(1), "a"), (Some(1), "b")])]]);
        let rows = write_and_read("do_nothing:ID", &pk(), input, &SessionContext::new())
            .await
            .expect("write succeeds");
        assert_eq!(rows, vec![(Some(1), "a".to_string())]);
    }

    /// A target that names no column is left to the engine, which refuses it
    /// as it would without the filter.
    #[test]
    fn a_target_naming_no_column_installs_nothing() {
        let inner = Arc::new(MemTable::try_new(schema(), vec![vec![]]).expect("memtable"));
        let table =
            wrap_with_keep_first_if_needed(inner, &options("do_nothing:missing"), &schema(), &pk());
        assert!(!has_keep_first(&table));
    }

    /// A user `INSERT` is planned by the physical optimizer, which must still
    /// hand the filter the whole write as one partition.
    #[tokio::test]
    async fn an_optimized_insert_runs_the_filter_over_one_partition() {
        let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(4));
        let inner = Arc::new(MemTable::try_new(schema(), vec![vec![]]).expect("memtable"));
        let table = wrap_with_keep_first_if_needed(
            Arc::clone(&inner) as Arc<dyn TableProvider>,
            &options("do_nothing:id"),
            &schema(),
            &pk(),
        );
        ctx.register_table("t", table).expect("register t");
        let src = MemTable::try_new(
            schema(),
            vec![
                vec![batch(&[(Some(0), "p0"), (Some(1), "p0")])],
                vec![batch(&[(Some(0), "p1"), (Some(2), "p1")])],
            ],
        )
        .expect("source");
        ctx.register_table("src", Arc::new(src))
            .expect("register src");

        let plan = ctx
            .sql("INSERT INTO t SELECT * FROM src")
            .await
            .expect("insert plans")
            .create_physical_plan()
            .await
            .expect("physical plan");
        let keep_first = find_keep_first(&plan).expect("the plan runs the filter");
        assert_eq!(
            keep_first.children()[0]
                .output_partitioning()
                .partition_count(),
            1
        );
        collect(plan, ctx.task_ctx()).await.expect("insert runs");

        let scan = inner
            .scan(&ctx.state(), None, &[], None)
            .await
            .expect("scan");
        let mut stored = ids(&collect(scan, ctx.task_ctx()).await.expect("read"));
        stored.sort_unstable();
        assert_eq!(stored, vec![0, 1, 2]);
    }

    #[test]
    fn only_drop_installs_the_wrapper() {
        for (on_conflict, wrapped) in [
            ("do_nothing:id", true),
            ("do_nothing_all", true),
            ("upsert:id", false),
            ("not a policy", false),
        ] {
            let inner = Arc::new(MemTable::try_new(schema(), vec![vec![]]).expect("memtable"));
            let table =
                wrap_with_keep_first_if_needed(inner, &options(on_conflict), &schema(), &pk());
            assert_eq!(has_keep_first(&table), wrapped, "{on_conflict}");
        }

        let inner = Arc::new(MemTable::try_new(schema(), vec![vec![]]).expect("memtable"));
        let table = wrap_with_keep_first_if_needed(inner, &HashMap::new(), &schema(), &pk());
        assert!(!has_keep_first(&table), "no on_conflict");
    }
}
