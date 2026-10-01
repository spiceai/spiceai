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
    row::{RowConverter, Rows, SortField},
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
        DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
        coalesce_partitions::CoalescePartitionsExec, metrics::MetricsSet,
        stream::RecordBatchStreamAdapter,
    },
};
use datafusion_table_providers::util::on_conflict::OnConflict;
use futures::StreamExt;
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
        OnConflict::DoNothing(columns) => {
            let columns: Vec<String> = columns.iter().map(str::to_string).collect();
            if columns.is_empty() {
                Vec::new()
            } else {
                vec![columns]
            }
        }
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

/// One key's admitted values. The encoded keys stay in the [`Rows`] of the
/// batch they arrived in, so admitting a key allocates nothing per row: an
/// entry points at its row, and entries whose hashes are equal are chained.
struct KeyColumns {
    indices: Vec<usize>,
    converter: RowConverter,
    batches: Vec<Rows>,
    /// Bytes held by `batches`, kept as they are pushed.
    batch_bytes: usize,
    entries: Vec<KeyEntry>,
    /// Hash of an encoded key to the latest entry with that hash.
    heads: HashMap<u64, usize, ahash::RandomState>,
    hasher: ahash::RandomState,
}

struct KeyEntry {
    batch: usize,
    row: usize,
    /// The previous entry with the same hash.
    next: Option<usize>,
}

impl KeyColumns {
    fn contains(&self, hash: u64, key: &[u8]) -> bool {
        let mut next = self.heads.get(&hash).copied();
        while let Some(index) = next {
            let entry = &self.entries[index];
            if self.batches[entry.batch].row(entry.row).as_ref() == key {
                return true;
            }
            next = entry.next;
        }
        false
    }

    fn insert(&mut self, hash: u64, batch: usize, row: usize) {
        let next = self.heads.insert(hash, self.entries.len());
        self.entries.push(KeyEntry { batch, row, next });
    }

    fn allocated_size(&self) -> usize {
        self.batch_bytes
            + self.batches.capacity() * std::mem::size_of::<Rows>()
            + self.entries.capacity() * std::mem::size_of::<KeyEntry>()
            + self.heads.capacity() * (std::mem::size_of::<(u64, usize)>() + 1)
            + self.converter.size()
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
                    batches: Vec::new(),
                    batch_bytes: 0,
                    entries: Vec::new(),
                    heads: HashMap::default(),
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

        let mut nulls = Vec::with_capacity(self.keys.len());
        for key in &mut self.keys {
            let columns: Vec<ArrayRef> = key
                .indices
                .iter()
                .map(|&index| Arc::clone(batch.column(index)))
                .collect();
            // A key with a NULL in it never conflicts, as in SQL.
            nulls.push(
                columns
                    .iter()
                    .fold(None, |acc: Option<NullBuffer>, column| {
                        NullBuffer::union(acc.as_ref(), column.logical_nulls().as_ref())
                    }),
            );
            let rows = key.converter.convert_columns(&columns)?;
            key.batch_bytes += rows.size();
            key.batches.push(rows);
        }
        // This batch's encoded keys, just pushed above.
        let batch_index = self.keys.first().map_or(0, |key| key.batches.len() - 1);

        let mut keep = Vec::with_capacity(num_rows);
        let mut dropped = 0;
        let mut hashes = vec![None; self.keys.len()];
        for row in 0..num_rows {
            let mut repeated = false;
            for ((key, nulls), hash) in self.keys.iter().zip(&nulls).zip(&mut hashes) {
                *hash = None;
                if nulls.as_ref().is_some_and(|nulls| nulls.is_null(row)) {
                    continue;
                }
                let encoded = key.batches[batch_index].row(row);
                let value = key.hasher.hash_one(encoded.as_ref());
                *hash = Some(value);
                repeated = repeated || key.contains(value, encoded.as_ref());
            }
            if repeated {
                dropped += 1;
            } else {
                for (key, hash) in self.keys.iter_mut().zip(&hashes) {
                    if let Some(hash) = *hash {
                        key.insert(hash, batch_index, row);
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
    use super::{KeepFirst, KeepFirstExec, wrap_with_keep_first_if_needed};
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
    use datafusion::execution::runtime_env::RuntimeEnvBuilder;
    use datafusion::logical_expr::dml::InsertOp;
    use datafusion::physical_plan::{ExecutionPlan, collect};
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
        let ids: Vec<i32> = batches
            .iter()
            .flat_map(|batch| {
                let ids = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .expect("id is Int32");
                (0..ids.len()).map(|row| ids.value(row)).collect::<Vec<_>>()
            })
            .collect();
        // (1, b) repeats id 1; (2, a) repeats v 'a'; (3, b) is new on both,
        // because the dropped (1, b) admitted neither of its keys.
        assert_eq!(ids, vec![1, 3]);
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

    /// The admitted keys are charged to the query memory pool, so a write
    /// whose keys do not fit fails instead of growing without bound.
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
