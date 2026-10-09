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

use std::{collections::HashMap, hash::BuildHasher, sync::Arc};

use arrow::{
    array::{Array, ArrayRef, AsArray, BooleanArray, RecordBatch},
    buffer::{BooleanBuffer, Buffer, NullBuffer},
    compute::filter_record_batch,
    datatypes::{
        ArrowPrimitiveType, DataType, Float16Type, Float32Type, Float64Type, Schema, SchemaRef,
    },
    row::{RowConverter, Rows, SortField},
};
use async_trait::async_trait;
use datafusion::{
    catalog::Session,
    common::{Constraint, Constraints, internal_err},
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
        stream::RecordBatchReceiverStream,
    },
};
use datafusion_table_providers::util::on_conflict::OnConflict;
use futures::StreamExt;
use hashbrown::{DefaultHashBuilder, HashTable};
use spice_table::{LayerWalk, SpiceTable, TableLayer};

/// Layers [`KeepFirst`] over `provider` when `on_conflict` resolves conflicts
/// by dropping the incoming row, so a write keeps only the first copy of each
/// key. An append under several `drop` targets is left to the table; see
/// [`KeepFirst`]'s `insert_into`.
///
/// `nan` is how the engine below stores a NaN in a key column.
///
/// Returns `provider` unchanged for any other `on_conflict` (or none).
#[must_use]
pub fn wrap_with_keep_first_if_needed<S: BuildHasher>(
    provider: Arc<dyn TableProvider>,
    options: &HashMap<String, String, S>,
    schema: &Schema,
    constraints: &Constraints,
    nan: NanKey,
) -> Arc<dyn TableProvider> {
    let key_sets = drop_key_sets(
        options.get("on_conflict").map(String::as_str),
        schema,
        constraints,
    );
    if key_sets.is_empty() {
        provider
    } else {
        SpiceTable::over(Arc::new(KeepFirst { key_sets, nan }), provider)
    }
}

/// How an engine stores a NaN in a key column, and so whether two NaN keys
/// conflict.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NanKey {
    /// A NaN is a value, and every NaN is the same one (`DuckDB`).
    Value,
    /// A NaN is stored as NULL, so like NULL it never conflicts (`SQLite`).
    Null,
}

/// The column sets `on_conflict` drops repeats of: the configured target for a
/// single `drop`, or every primary-key and unique constraint when each target
/// is `drop`.
fn drop_key_sets(
    on_conflict: Option<&str>,
    schema: &Schema,
    constraints: &Constraints,
) -> Vec<Vec<String>> {
    let Some(Ok(on_conflict)) = on_conflict.map(OnConflict::try_from) else {
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
    key_sets: Vec<Vec<String>>,
    nan: NanKey,
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
            // Only writes are filtered: a scan returns the table beneath's rows.
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
        // A row the table rejects against a stored row is never written, but
        // this filter cannot see stored rows, so it would still hold that
        // row's keys against later rows. With one key that is harmless — a
        // later copy conflicts with the same stored row — but with several it
        // drops a row whose only conflict was with a row never written. An
        // overwrite starts from an empty table, so it has no stored rows.
        if self.key_sets.len() > 1 && op != InsertOp::Overwrite {
            return below.insert_into(state, input, op).await;
        }
        let exec = KeepFirstExec::try_new(input, &self.key_sets, self.nan)?;
        below.insert_into(state, Arc::new(exec), op).await
    }
}

/// Drops every row whose key an earlier kept row of the same write carried.
/// Runs as one partition, so "earlier" is arrival order across the whole
/// write rather than within one input partition.
#[derive(Debug)]
struct KeepFirstExec {
    input: Arc<dyn ExecutionPlan>,
    key_indices: Arc<[Vec<usize>]>,
    nan: NanKey,
    properties: Arc<PlanProperties>,
}

impl KeepFirstExec {
    fn try_new(
        input: Arc<dyn ExecutionPlan>,
        key_sets: &[Vec<String>],
        nan: NanKey,
    ) -> datafusion::error::Result<Self> {
        let schema = input.schema();
        let key_indices = key_sets
            .iter()
            .map(|columns| {
                columns
                    .iter()
                    .map(|column| schema.index_of(column))
                    .collect::<Result<Vec<_>, _>>()
            })
            .collect::<Result<Arc<[_]>, _>>()?;
        Ok(Self::with_indices(input, key_indices, nan))
    }

    fn with_indices(
        input: Arc<dyn ExecutionPlan>,
        key_indices: Arc<[Vec<usize>]>,
        nan: NanKey,
    ) -> Self {
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
            nan,
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

    /// The key columns are held as indices, not physical expressions, so there
    /// is nothing to visit.
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

    /// "First" is arrival order across the whole write, so the filter takes
    /// the write as one stream.
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
            DataFusionError::Internal("KeepFirstExec requires exactly one child".to_string())
        })?;
        Ok(Arc::new(Self::with_indices(
            child,
            Arc::clone(&self.key_indices),
            self.nan,
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion::error::Result<SendableRecordBatchStream> {
        let mut input = self.input.execute(partition, Arc::clone(&context))?;
        let schema = self.schema();
        let reservation = MemoryConsumer::new(format!("KeepFirstExec[{partition}]"))
            .register(context.memory_pool());
        let mut seen = SeenKeys::try_new(&schema, &self.key_indices, self.nan, reservation)?;
        // The filter runs on a task of its own, so it filters the next batch
        // while whatever drains this stream (the engine's write) handles the
        // last one, rather than the two taking turns.
        let mut builder = RecordBatchReceiverStream::builder(Arc::clone(&schema), 2);
        let tx = builder.tx();
        builder.spawn(async move {
            while let Some(batch) = input.next().await {
                if tx.send(Ok(seen.keep_first(batch?)?)).await.is_err() {
                    // Nothing reads the stream any more.
                    break;
                }
            }
            Ok(())
        });
        Ok(builder.build())
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
    nan: NanKey,
    hasher: DefaultHashBuilder,
    admitted: Admitted,
}

/// How one key's admitted values are held: as cheaply as its type allows.
enum Admitted {
    /// A single column at most eight bytes wide, held as each value's bits.
    /// Once floats are made canonical (`canonical_key_column`), two values are
    /// equal exactly when their bits are, which is also how arrow's row format
    /// compares them, so this drops the same rows as `Encoded` without
    /// encoding a key or storing it twice.
    Bits {
        data_type: DataType,
        width: usize,
        table: HashTable<u64>,
    },
    /// Any other key, in arrow's row format. The table holds each admitted
    /// key's index in `keys`.
    Encoded {
        converter: RowConverter,
        keys: AdmittedKeys,
        table: HashTable<usize>,
    },
}

impl KeyColumns {
    fn try_new(schema: &Schema, indices: &[usize], nan: NanKey) -> datafusion::error::Result<Self> {
        let bits = match indices {
            [index] => {
                let data_type = schema.field(*index).data_type();
                data_type
                    .primitive_width()
                    .filter(|&width| width <= 8)
                    .map(|width| (data_type.clone(), width))
            }
            _ => None,
        };
        let admitted = match bits {
            Some((data_type, width)) => Admitted::Bits {
                data_type,
                width,
                table: HashTable::new(),
            },
            None => Admitted::Encoded {
                converter: RowConverter::new(
                    indices
                        .iter()
                        .map(|&index| SortField::new(schema.field(index).data_type().clone()))
                        .collect(),
                )?,
                keys: AdmittedKeys::default(),
                table: HashTable::new(),
            },
        };
        Ok(Self {
            indices: indices.to_vec(),
            nan,
            hasher: DefaultHashBuilder::default(),
            admitted,
        })
    }

    /// This batch's values of the key, borrowed with the key's admitted
    /// values, and which rows have a NULL in the key: such a key never
    /// conflicts, as in SQL. Under [`NanKey::Null`] a NaN counts as a NULL.
    fn batch_keys(
        &mut self,
        batch: &RecordBatch,
    ) -> datafusion::error::Result<(BatchKeys<'_>, Option<NullBuffer>)> {
        let columns: Vec<ArrayRef> = self
            .indices
            .iter()
            .map(|&index| canonical_key_column(Arc::clone(batch.column(index))))
            .collect();
        let nulls = columns
            .iter()
            .fold(None, |acc: Option<NullBuffer>, column| {
                let nulls = NullBuffer::union(acc.as_ref(), column.logical_nulls().as_ref());
                match self.nan {
                    NanKey::Value => nulls,
                    NanKey::Null => {
                        NullBuffer::union(nulls.as_ref(), nans_as_nulls(column).as_ref())
                    }
                }
            });
        let keys = match &mut self.admitted {
            Admitted::Bits {
                data_type,
                width,
                table,
            } => {
                let column = columns
                    .first()
                    .ok_or_else(|| DataFusionError::Internal("a key has no column".to_string()))?;
                // Read at another type's width, the column would yield wrong
                // keys, so a batch that does not match the write is refused.
                if column.data_type() != data_type {
                    return internal_err!(
                        "Failed to keep the first copy of each key: the key column is {}, but the write declared {data_type}",
                        column.data_type()
                    );
                }
                let data = column.to_data();
                let values = data.buffers().first().ok_or_else(|| {
                    DataFusionError::Internal("a fixed-width key column has no values".to_string())
                })?;
                BatchKeys::Bits {
                    values: values.slice(data.offset() * *width),
                    width: *width,
                    table,
                    hasher: &self.hasher,
                }
            }
            Admitted::Encoded {
                converter,
                keys,
                table,
            } => BatchKeys::Encoded {
                rows: converter.convert_columns(&columns)?,
                keys,
                table,
                hasher: &self.hasher,
            },
        };
        Ok((keys, nulls))
    }

    fn allocated_size(&self) -> usize {
        match &self.admitted {
            Admitted::Bits { table, .. } => table.allocation_size(),
            Admitted::Encoded {
                converter,
                keys,
                table,
            } => keys.allocated_size() + table.allocation_size() + converter.size(),
        }
    }
}

/// One batch's values of one key, with the key's admitted values, so each row
/// can be looked up and admitted.
enum BatchKeys<'a> {
    Bits {
        /// The column's value bytes, from its first row.
        values: Buffer,
        width: usize,
        table: &'a mut HashTable<u64>,
        hasher: &'a DefaultHashBuilder,
    },
    Encoded {
        rows: Rows,
        keys: &'a mut AdmittedKeys,
        table: &'a mut HashTable<usize>,
        hasher: &'a DefaultHashBuilder,
    },
}

impl BatchKeys<'_> {
    fn hash(&self, row: usize) -> u64 {
        match self {
            Self::Bits {
                values,
                width,
                hasher,
                ..
            } => hasher.hash_one(bits(values, *width, row)),
            Self::Encoded { rows, hasher, .. } => hasher.hash_one(rows.row(row).data()),
        }
    }

    fn contains(&self, hash: u64, row: usize) -> bool {
        match self {
            Self::Bits {
                values,
                width,
                table,
                ..
            } => {
                let value = bits(values, *width, row);
                table.find(hash, |&admitted| admitted == value).is_some()
            }
            Self::Encoded {
                rows, keys, table, ..
            } => {
                let value = rows.row(row).data();
                table
                    .find(hash, |&index| keys.get(index) == value)
                    .is_some()
            }
        }
    }

    fn insert(&mut self, hash: u64, row: usize) {
        match self {
            Self::Bits {
                values,
                width,
                table,
                hasher,
            } => {
                table.insert_unique(hash, bits(values, *width, row), |&admitted| {
                    hasher.hash_one(admitted)
                });
            }
            Self::Encoded {
                rows,
                keys,
                table,
                hasher,
            } => {
                let index = keys.push(rows.row(row).data());
                table.insert_unique(hash, index, |&index| hasher.hash_one(keys.get(index)));
            }
        }
    }
}

/// `column` with each float the engines hold as one key written one way:
/// `-0.0` as `0.0`, and every NaN as the same NaN. `DuckDB` and `SQLite` both
/// treat `-0.0` as a repeat of a stored `0.0`, and `DuckDB` every NaN as one
/// key, while their bits, and arrow's row format, tell them apart. The write
/// still carries the values it was given; only the keys compared change. Any
/// other column is returned as is.
fn canonical_key_column(column: ArrayRef) -> ArrayRef {
    type F16 = <Float16Type as ArrowPrimitiveType>::Native;
    match column.data_type() {
        DataType::Float16 => Arc::new(
            column
                .as_primitive::<Float16Type>()
                .unary::<_, Float16Type>(|value| match value {
                    value if value.is_nan() => F16::NAN,
                    value if value == F16::ZERO => F16::ZERO,
                    value => value,
                }),
        ),
        DataType::Float32 => Arc::new(
            column
                .as_primitive::<Float32Type>()
                .unary::<_, Float32Type>(|value| match value {
                    value if value.is_nan() => f32::NAN,
                    0.0 => 0.0,
                    value => value,
                }),
        ),
        DataType::Float64 => Arc::new(
            column
                .as_primitive::<Float64Type>()
                .unary::<_, Float64Type>(|value| match value {
                    value if value.is_nan() => f64::NAN,
                    0.0 => 0.0,
                    value => value,
                }),
        ),
        _ => column,
    }
}

/// Which rows of a float `column` hold a NaN, as a null buffer: valid where
/// the value is not NaN. `None` for any other column.
fn nans_as_nulls(column: &ArrayRef) -> Option<NullBuffer> {
    fn not_nan<T: ArrowPrimitiveType>(
        column: &ArrayRef,
        is_nan: impl Fn(T::Native) -> bool,
    ) -> NullBuffer {
        let values = column.as_primitive::<T>().values();
        NullBuffer::new(BooleanBuffer::collect_bool(values.len(), |row| {
            !is_nan(values[row])
        }))
    }
    match column.data_type() {
        DataType::Float16 => Some(not_nan::<Float16Type>(
            column,
            <Float16Type as ArrowPrimitiveType>::Native::is_nan,
        )),
        DataType::Float32 => Some(not_nan::<Float32Type>(column, f32::is_nan)),
        DataType::Float64 => Some(not_nan::<Float64Type>(column, f64::is_nan)),
        _ => None,
    }
}

/// The bits of the value at `row` of a column `width` bytes wide.
fn bits(values: &[u8], width: usize, row: usize) -> u64 {
    let mut bits = [0; 8];
    bits[..width].copy_from_slice(&values[row * width..(row + 1) * width]);
    u64::from_le_bytes(bits)
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
        nan: NanKey,
        reservation: MemoryReservation,
    ) -> datafusion::error::Result<Self> {
        let keys = key_indices
            .iter()
            .map(|indices| KeyColumns::try_new(schema, indices, nan))
            .collect::<datafusion::error::Result<_>>()?;
        Ok(Self { keys, reservation })
    }

    fn keep_first(&mut self, batch: RecordBatch) -> datafusion::error::Result<RecordBatch> {
        let num_rows = batch.num_rows();
        if num_rows == 0 {
            return Ok(batch);
        }

        let mut keep = Vec::with_capacity(num_rows);
        {
            let mut keys = self
                .keys
                .iter_mut()
                .map(|key| key.batch_keys(&batch))
                .collect::<datafusion::error::Result<Vec<_>>>()?;
            let mut hashes = vec![None; keys.len()];
            for row in 0..num_rows {
                let mut repeated = false;
                for ((key, nulls), hash) in keys.iter().zip(&mut hashes) {
                    *hash = None;
                    if nulls.as_ref().is_some_and(|nulls| nulls.is_null(row)) {
                        continue;
                    }
                    let value_hash = key.hash(row);
                    *hash = Some(value_hash);
                    repeated = repeated || key.contains(value_hash, row);
                }
                if !repeated {
                    for ((key, _), hash) in keys.iter_mut().zip(&hashes) {
                        if let Some(hash) = *hash {
                            key.insert(hash, row);
                        }
                    }
                }
                keep.push(!repeated);
            }
        }

        self.reservation.try_resize(
            self.keys
                .iter()
                .map(KeyColumns::allocated_size)
                .sum::<usize>(),
        )?;

        if keep.iter().all(|&kept| kept) {
            return Ok(batch);
        }
        Ok(filter_record_batch(&batch, &BooleanArray::from(keep))?)
    }
}

#[cfg(test)]
mod tests {
    use super::{
        Admitted, AdmittedKeys, KeepFirst, KeepFirstExec, KeyColumns, KeyLayout, NanKey, SeenKeys,
        wrap_with_keep_first_if_needed,
    };
    use spice_table::{LayerWalk, find_layer};
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow::array::{
        Array, Float32Array, Float64Array, Int32Array, Int64Array, RecordBatch, StringArray,
    };
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

    /// The ids `KeepFirstExec` keeps from `input`, filtering on `key_sets`.
    async fn filter_ids(input: Arc<dyn ExecutionPlan>, key_sets: &[Vec<String>]) -> Vec<i32> {
        let exec = KeepFirstExec::try_new(input, key_sets, NanKey::Value).expect("plan");
        let batches = collect(Arc::new(exec), Arc::new(TaskContext::default()))
            .await
            .expect("filter runs");
        ids(&batches)
    }

    /// A key set over the columns at `key_indices`, charged to an unbounded pool.
    fn seen_keys(key_indices: &[Vec<usize>]) -> SeenKeys {
        let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::default());
        SeenKeys::try_new(
            &schema(),
            key_indices,
            NanKey::Value,
            MemoryConsumer::new("test").register(&pool),
        )
        .expect("key set")
    }

    fn find_keep_first(plan: &Arc<dyn ExecutionPlan>) -> Option<&KeepFirstExec> {
        plan.downcast_ref::<KeepFirstExec>()
            .or_else(|| plan.children().into_iter().find_map(find_keep_first))
    }

    fn has_keep_first(table: &Arc<dyn TableProvider>) -> bool {
        find_layer::<KeepFirst>(table.as_ref(), LayerWalk::Write).is_some()
    }

    fn pk() -> Constraints {
        Constraints::new_unverified(vec![Constraint::PrimaryKey(vec![0])])
    }

    fn options(on_conflict: &str) -> HashMap<String, String> {
        [("on_conflict".to_string(), on_conflict.to_string())]
            .into_iter()
            .collect()
    }

    /// An empty `MemTable`, and that table with `options` applied.
    fn wrapped(
        options: &HashMap<String, String>,
        constraints: &Constraints,
    ) -> (Arc<MemTable>, Arc<dyn TableProvider>) {
        let inner = Arc::new(MemTable::try_new(schema(), vec![vec![]]).expect("memtable"));
        let table = wrap_with_keep_first_if_needed(
            Arc::clone(&inner) as Arc<dyn TableProvider>,
            options,
            &schema(),
            constraints,
            NanKey::Value,
        );
        (inner, table)
    }

    /// Writes `input` through a `drop`-wrapped `MemTable` and returns what was
    /// stored, in storage order.
    async fn write_and_read(
        on_conflict: &str,
        constraints: &Constraints,
        input: Arc<dyn ExecutionPlan>,
        ctx: &SessionContext,
    ) -> datafusion::error::Result<Vec<(Option<i32>, String)>> {
        let (inner, table) = wrapped(&options(on_conflict), constraints);
        let plan = table
            .insert_into(&ctx.state(), input, InsertOp::Append)
            .await?;
        collect(plan, ctx.task_ctx()).await?;
        stored(&inner, ctx).await
    }

    /// What `table` stores, in storage order.
    async fn stored(
        table: &MemTable,
        ctx: &SessionContext,
    ) -> datafusion::error::Result<Vec<(Option<i32>, String)>> {
        let scan = table.scan(&ctx.state(), None, &[], None).await?;
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

    /// Registers a `do_nothing:id` table `t` and a source `src` holding
    /// `partitions`, and plans `sql` against them as a user statement is
    /// planned, through the physical optimizer.
    async fn plan_insert(
        ctx: &SessionContext,
        partitions: Vec<Vec<RecordBatch>>,
        sql: &str,
    ) -> (Arc<MemTable>, Arc<dyn ExecutionPlan>) {
        let (inner, table) = wrapped(&options("do_nothing:id"), &pk());
        ctx.register_table("t", table).expect("register t");
        let src = MemTable::try_new(schema(), partitions).expect("source");
        ctx.register_table("src", Arc::new(src))
            .expect("register src");
        let plan = ctx
            .sql(sql)
            .await
            .expect("insert plans")
            .create_physical_plan()
            .await
            .expect("physical plan");
        (inner, plan)
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
        let kept = filter_ids(input, &[vec!["id".to_string()], vec!["v".to_string()]]).await;
        // (1, b) repeats id 1; (2, a) repeats v 'a'; (3, b) is new on both,
        // because the dropped (1, b) admitted neither of its keys.
        assert_eq!(kept, vec![1, 3]);
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

    /// A key is matched on its full value whatever the width of the keys
    /// before it. Arrow encodes every string of up to eight bytes to one width,
    /// so the longer value here is the first key of another width.
    #[test]
    fn the_filter_matches_keys_of_varying_width() {
        let mut seen = seen_keys(&[vec![1]]);
        let long = "a value longer than eight bytes";
        let first = seen
            .keep_first(batch(&[(Some(1), "a"), (Some(2), "bb"), (Some(3), "a")]))
            .expect("filter");
        let second = seen
            .keep_first(batch(&[
                (Some(4), long),
                (Some(5), "bb"),
                (Some(6), "b"),
                (Some(7), long),
            ]))
            .expect("filter");
        assert_eq!(ids(&[first, second]), vec![1, 2, 4, 6]);
        let Admitted::Encoded { keys, .. } = &seen.keys[0].admitted else {
            panic!("a string key is held in the row format");
        };
        assert!(matches!(keys.layout, KeyLayout::Variable(_)));
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

    /// How many keys `key` holds, however it holds them.
    fn admitted(key: &KeyColumns) -> usize {
        match &key.admitted {
            Admitted::Bits { table, .. } => table.len(),
            Admitted::Encoded { keys, table, .. } => {
                assert_eq!(keys.len, table.len());
                table.len()
            }
        }
    }

    /// Only the keys the filter admits are held, whether as bits or in the row
    /// format: a dropped row or a row with a NULL key leaves nothing behind
    /// once its batch is written.
    #[test]
    fn only_admitted_keys_are_held() {
        let values: Vec<String> = (0..10_000).map(|n| (n % 4).to_string()).collect();
        let rows: Vec<(Option<i32>, &str)> = (0_i32..)
            .zip(&values)
            .map(|(n, v)| ((n % 4 != 3).then_some(n % 4), v.as_str()))
            .collect();

        let mut by_id = seen_keys(&[vec![0]]);
        assert!(matches!(by_id.keys[0].admitted, Admitted::Bits { .. }));
        let kept = by_id.keep_first(batch(&rows)).expect("filter");
        // Ids 0, 1 and 2 once each, and all 2,500 NULL-keyed rows.
        assert_eq!(kept.num_rows(), 2_503);
        assert_eq!(admitted(&by_id.keys[0]), 3);

        let mut by_v = seen_keys(&[vec![1]]);
        assert!(matches!(by_v.keys[0].admitted, Admitted::Encoded { .. }));
        let kept = by_v.keep_first(batch(&rows)).expect("filter");
        assert_eq!(kept.num_rows(), 4);
        assert_eq!(admitted(&by_v.keys[0]), 4);
    }

    /// A key held as its bits drops exactly the rows the same key drops in
    /// arrow's row format, including floats whose bits differ while the
    /// engines treat them as one key: `-0.0` and `0.0`, and NaNs with
    /// different payloads. A constant second column puts the same key in the
    /// row format.
    #[test]
    fn a_key_held_as_bits_drops_what_the_row_format_drops() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("f", DataType::Float64, true),
            Field::new("c", DataType::Int32, false),
        ]));
        let other_nan = f64::from_bits(f64::NAN.to_bits() ^ 1);
        let values = vec![
            Some(0.0),
            Some(-0.0),
            Some(0.0),
            Some(f64::NAN),
            Some(other_nan),
            Some(f64::NAN),
            Some(1.5),
            Some(-0.0),
            None,
            Some(f64::INFINITY),
            Some(1.5),
            None,
        ];
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Float64Array::from(values)),
                Arc::new(Int32Array::from(vec![7; 12])),
            ],
        )
        .expect("build batch");
        let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::default());
        let mut as_bits = SeenKeys::try_new(
            &schema,
            &[vec![0]],
            NanKey::Value,
            MemoryConsumer::new("bits").register(&pool),
        )
        .expect("key set");
        let mut as_rows = SeenKeys::try_new(
            &schema,
            &[vec![0, 1]],
            NanKey::Value,
            MemoryConsumer::new("rows").register(&pool),
        )
        .expect("key set");
        assert!(matches!(as_bits.keys[0].admitted, Admitted::Bits { .. }));
        assert!(matches!(as_rows.keys[0].admitted, Admitted::Encoded { .. }));

        let kept = as_bits.keep_first(batch.clone()).expect("filter");
        assert_eq!(kept, as_rows.keep_first(batch).expect("filter"));
        // 0.0, NaN, 1.5, infinity, and both NULLs.
        assert_eq!(kept.num_rows(), 6);
    }

    /// `DuckDB` and `SQLite` both treat `-0.0` as a repeat of a stored `0.0`
    /// key, and `DuckDB` treats NaNs with different payloads as one key, so
    /// the filter keeps only the first of them, whether the key is held as
    /// its bits or in the row format.
    #[test]
    fn floats_the_engines_treat_as_one_key_keep_their_first_copy() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("f", DataType::Float64, true),
            Field::new("c", DataType::Int32, false),
        ]));
        let other_nan = f64::from_bits(f64::NAN.to_bits() ^ 1);
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3, 4])),
                Arc::new(Float64Array::from(vec![
                    Some(-0.0),
                    Some(0.0),
                    Some(other_nan),
                    Some(f64::NAN),
                ])),
                Arc::new(Int32Array::from(vec![7; 4])),
            ],
        )
        .expect("build batch");
        let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::default());
        let as_bits = SeenKeys::try_new(
            &schema,
            &[vec![1]],
            NanKey::Value,
            MemoryConsumer::new("bits").register(&pool),
        )
        .expect("key set");
        let as_rows = SeenKeys::try_new(
            &schema,
            &[vec![1, 2]],
            NanKey::Value,
            MemoryConsumer::new("rows").register(&pool),
        )
        .expect("key set");
        assert!(matches!(as_bits.keys[0].admitted, Admitted::Bits { .. }));
        assert!(matches!(as_rows.keys[0].admitted, Admitted::Encoded { .. }));

        for mut seen in [as_bits, as_rows] {
            let kept = seen.keep_first(batch.clone()).expect("filter");
            assert_eq!(ids(std::slice::from_ref(&kept)), vec![1, 3]);
            // The kept rows are the ones written, not their canonical form.
            let floats = kept
                .column(1)
                .as_any()
                .downcast_ref::<Float64Array>()
                .expect("Float64 column");
            assert!(floats.value(0).is_sign_negative());
            assert_eq!(floats.value(1).to_bits(), other_nan.to_bits());
        }
    }

    /// `SQLite` stores a NaN as NULL, so under [`NanKey::Null`] a NaN key never
    /// conflicts, alone or in a composite key, while `-0.0` still repeats
    /// `0.0`.
    #[test]
    fn under_nan_as_null_a_nan_key_never_repeats() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("f", DataType::Float32, true),
            Field::new("c", DataType::Int32, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3, 4])),
                Arc::new(Float32Array::from(vec![
                    Some(f32::NAN),
                    Some(f32::NAN),
                    Some(0.0),
                    Some(-0.0),
                ])),
                Arc::new(Int32Array::from(vec![7; 4])),
            ],
        )
        .expect("build batch");
        let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::default());
        for indices in [vec![1], vec![1, 2]] {
            let mut seen = SeenKeys::try_new(
                &schema,
                std::slice::from_ref(&indices),
                NanKey::Null,
                MemoryConsumer::new("nan").register(&pool),
            )
            .expect("key set");
            let kept = seen.keep_first(batch.clone()).expect("filter");
            assert_eq!(
                ids(std::slice::from_ref(&kept)),
                vec![1, 2, 3],
                "{indices:?}"
            );
        }
    }

    /// A batch whose key column is not the type the write declared is refused
    /// rather than read at the wrong width.
    #[test]
    fn a_key_column_of_another_type_is_refused() {
        let mut seen = seen_keys(&[vec![0]]);
        let other = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, true),
                Field::new("v", DataType::Utf8, true),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["a", "b"])),
            ],
        )
        .expect("build batch");
        let error = seen
            .keep_first(other)
            .expect_err("an Int64 column under an Int32 key");
        assert!(
            error
                .to_string()
                .contains("the key column is Int64, but the write declared Int32"),
            "{error}"
        );
    }

    /// A batch sliced from a larger one is read from its own first row.
    #[test]
    fn a_sliced_batch_is_read_from_its_first_row() {
        let mut seen = seen_keys(&[vec![0]]);
        let rows = batch(&[
            (Some(1), "a"),
            (Some(2), "b"),
            (Some(3), "c"),
            (Some(2), "d"),
        ]);
        let kept = seen.keep_first(rows.slice(1, 3)).expect("filter");
        assert_eq!(ids(&[kept]), vec![2, 3]);
    }

    /// `DuckDB` and `SQLite` match an `on_conflict` target to its column
    /// ignoring case, so the filter does too.
    #[tokio::test]
    async fn a_target_spelled_in_another_case_names_its_column() {
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
        let (_, table) = wrapped(&options("do_nothing:missing"), &pk());
        assert!(!has_keep_first(&table));
    }

    /// A user `INSERT` is planned by the physical optimizer, which must still
    /// hand the filter the whole write as one partition.
    #[tokio::test]
    async fn an_optimized_insert_runs_the_filter_over_one_partition() {
        let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(4));
        let (inner, plan) = plan_insert(
            &ctx,
            vec![
                vec![batch(&[(Some(0), "p0"), (Some(1), "p0")])],
                vec![batch(&[(Some(0), "p1"), (Some(2), "p1")])],
            ],
            "INSERT INTO t SELECT * FROM src",
        )
        .await;
        let keep_first = find_keep_first(&plan).expect("the plan runs the filter");
        assert_eq!(
            keep_first.children()[0]
                .output_partitioning()
                .partition_count(),
            1
        );
        collect(plan, ctx.task_ctx()).await.expect("insert runs");

        let mut rows = stored(&inner, &ctx).await.expect("read");
        rows.sort_unstable();
        let ids: Vec<Option<i32>> = rows.iter().map(|(id, _)| *id).collect();
        assert_eq!(ids, vec![Some(0), Some(1), Some(2)]);
    }

    /// An ordered `INSERT` keeps the copy its `ORDER BY` puts first: the
    /// filter keeps its input's order, so the optimizer leaves the sort below
    /// it in place.
    #[tokio::test]
    async fn an_ordered_insert_keeps_the_copy_its_order_puts_first() {
        let ctx = SessionContext::new();
        let (inner, plan) = plan_insert(
            &ctx,
            vec![vec![batch(&[
                (Some(1), "a"),
                (Some(2), "b"),
                (Some(1), "z"),
            ])]],
            "INSERT INTO t SELECT * FROM src ORDER BY v DESC",
        )
        .await;
        collect(plan, ctx.task_ctx()).await.expect("insert runs");

        let mut rows = stored(&inner, &ctx).await.expect("read");
        rows.sort_unstable();
        assert_eq!(
            rows,
            vec![(Some(1), "z".to_string()), (Some(2), "b".to_string())]
        );
    }

    #[test]
    fn only_drop_installs_the_wrapper() {
        for (on_conflict, installed) in [
            (options("do_nothing:id"), true),
            (options("do_nothing_all"), true),
            (options("upsert:id"), false),
            (options("not a policy"), false),
            (HashMap::new(), false),
        ] {
            let (_, table) = wrapped(&on_conflict, &pk());
            assert_eq!(has_keep_first(&table), installed, "{on_conflict:?}");
        }
    }
}
