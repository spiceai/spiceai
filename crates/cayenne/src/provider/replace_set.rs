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

//! Complete-set preparation for atomic snapshot replacement.
//!
//! The input preserves rows outside the replaced groups and includes every
//! replacement member. The overwrite sink takes its write/checkpoint guards
//! before polling this stream; its scan must remain lazy so a preceding write
//! cannot be lost.
//! Staging and publication use the ordinary overwrite lifecycle, including its
//! memory limit, file spilling, snapshot visibility and checkpoint fences.

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use arrow::array::RecordBatch;
use arrow::compute::{filter_record_batch, not, or};
use arrow_schema::SchemaRef;
use data_components::cdc::mutation::ReplaceSet;
use datafusion::datasource::{TableProvider, sink::DataSink};
use datafusion::execution::context::SessionContext;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion_common::{DataFusionError, Result};
use datafusion_execution::TaskContext;
use datafusion_expr::dml::InsertOp;
use datafusion_physical_plan::execute_stream;
use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
use futures::{StreamExt, TryStreamExt};
use parking_lot::Mutex;

use crate::row_converter::{RowConverter, SortField};

use super::pk_validation::null_primary_key_message;
use super::sink::CayenneDataSink;
use super::table::{CayenneCdcWrite, CayenneTableProvider};

/// Enforces uniqueness across replacement members and retained outside rows.
/// A collision with an outside row is a refusal, never an implicit upsert of
/// that other group. Keyless tables preserve every duplicate member.
struct PrimaryKeys {
    indices: Vec<usize>,
    converter: Option<RowConverter>,
    seen: HashSet<Box<[u8]>>,
    reservation: MemoryReservation,
}

impl PrimaryKeys {
    fn new(table: &CayenneTableProvider, schema: &SchemaRef, task: &TaskContext) -> Result<Self> {
        let indices = table
            .pk_column_names()
            .iter()
            .map(|name| schema.index_of(name))
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let converter = if indices.is_empty() {
            None
        } else {
            Some(RowConverter::new(
                indices
                    .iter()
                    .map(|&i| SortField::new(schema.field(i).data_type().clone()))
                    .collect(),
            )?)
        };
        Ok(Self {
            indices,
            converter,
            seen: HashSet::new(),
            reservation: MemoryConsumer::new("Cayenne replacement primary keys")
                .register(task.memory_pool()),
        })
    }

    async fn validate(&mut self, batch: &RecordBatch) -> Result<()> {
        let Some(converter) = &mut self.converter else {
            return Ok(());
        };
        let columns: Vec<_> = self
            .indices
            .iter()
            .map(|&index| Arc::clone(batch.column(index)))
            .collect();
        if columns.iter().any(|column| column.null_count() != 0) {
            return Err(DataFusionError::Plan(null_primary_key_message(
                batch,
                &self.indices,
            )));
        }
        for offset in (0..batch.num_rows()).step_by(128) {
            let length = (batch.num_rows() - offset).min(128);
            let chunk: Vec<_> = columns
                .iter()
                .map(|array| array.slice(offset, length))
                .collect();
            let rows = converter.convert_columns(&chunk)?;
            for row in &rows {
                let key = row.as_ref();
                if self.seen.contains(key) {
                    return Err(DataFusionError::Plan(
                        "Complete-set replacement violates primary key uniqueness; replacement members must have distinct keys that do not belong to another group".into(),
                    ));
                }
                // Cover owned key bytes and conservative hash-table spare capacity.
                // The batch encoder's temporary buffers have a separate lifetime.
                self.reservation.try_grow(key.len().saturating_add(128))?;
                self.seen.insert(Box::from(key));
            }
            tokio::task::yield_now().await;
        }
        Ok(())
    }
}

pub(super) fn is_primary_key_singleton(
    table: &CayenneTableProvider,
    replacement: &ReplaceSet,
) -> bool {
    let pk_columns = table.pk_column_names();
    let key = replacement.key().values();
    table.supports_rebuildable_ingestion()
        && key.len() == pk_columns.len()
        && pk_columns.iter().all(|name| {
            key.iter()
                .any(|(column, value)| column == name && !value.is_null())
        })
        && replacement
            .batches()
            .iter()
            .try_fold(0_usize, |rows, batch| rows.checked_add(batch.num_rows()))
            == Some(1)
}

pub(super) fn can_coalesce(
    table: &CayenneTableProvider,
    left: &ReplaceSet,
    right: &ReplaceSet,
) -> bool {
    // Disjoint groups can still collide on a separate row primary key. Keep
    // those requests independent so a rejected member cannot reject its peers.
    let compatible = table.pk_column_names().is_empty()
        || (is_primary_key_singleton(table, left) && is_primary_key_singleton(table, right));
    compatible
        && table.metadata().partition_column.is_none()
        && table.mem_tier_shard_count() == 1
        && (left.schema().fields() == table.table_schema().fields()
            || left.schema().fields() == table.read_schema().fields())
        && left.key().is_disjoint_from(right.key()).unwrap_or(false)
}

pub(super) async fn write_snapshot(
    table: &CayenneTableProvider,
    replacements: Vec<ReplaceSet>,
    session: &SessionContext,
    task: &Arc<TaskContext>,
) -> Result<CayenneCdcWrite> {
    let start = Instant::now();
    let rows = replacements
        .iter()
        .flat_map(data_components::cdc::mutation::ReplaceSet::batches)
        .try_fold(0_u64, |rows, batch| {
            rows.checked_add(batch.num_rows() as u64).ok_or_else(|| {
                DataFusionError::Plan("Complete-set replacement row count exceeds u64".into())
            })
        })?;
    let bytes = replacements
        .iter()
        .map(|replacement| replacement.retained_bytes() as u64)
        .fold(0_u64, u64::saturating_add);
    let (keys, batches): (Vec<_>, Vec<_>) =
        replacements.into_iter().map(ReplaceSet::into_parts).unzip();
    let keys = Arc::new(keys);
    let schema = table.table_schema();
    let primary_keys = PrimaryKeys::new(table, &schema, task)?;
    let state = session.state();
    let scan_table = table.clone_for_write();
    let scan_task = Arc::clone(task);
    let scan_schema = Arc::clone(&schema);
    let deleted = Arc::new(AtomicU64::new(0));
    let deleted_rows = Arc::clone(&deleted);

    let retained = futures::stream::once(async move {
        scan_table.ensure_no_incomplete_write().await?;
        if scan_table.table_schema().fields() != scan_schema.fields() {
            return Err(DataFusionError::Plan(
                "Table schema changed before complete-set replacement".into(),
            ));
        }
        let scan = scan_table.scan(&state, None, &[], None).await?;
        execute_stream(scan, scan_task)
    })
    .try_flatten()
    .and_then(move |batch| {
        let keys = Arc::clone(&keys);
        let deleted_rows = Arc::clone(&deleted_rows);
        async move {
            let (first, rest) = keys
                .split_first()
                .ok_or_else(|| DataFusionError::Internal("Missing replacement scope".into()))?;
            let mut matching = first.matching_rows(&batch)?;
            for keys in rest.chunks(8) {
                for key in keys {
                    matching = or(&matching, &key.matching_rows(&batch)?)?;
                }
                tokio::task::yield_now().await;
            }
            let retained = filter_record_batch(&batch, &not(&matching)?)?;
            deleted_rows.fetch_add(matching.true_count() as u64, Ordering::Relaxed);
            Ok::<RecordBatch, DataFusionError>(retained)
        }
    });

    // Validation failures occur before the overwrite can commit. Preserve the
    // original rejection across the storage writer's error context so the
    // ingestion owner can distinguish a refused batch from a failed publish.
    let validation_error = Arc::new(Mutex::new(None));
    let validation_failure = Arc::clone(&validation_error);
    let target_schema = Arc::clone(&schema);
    let input = futures::stream::iter(batches.into_iter().flatten().map(Ok))
        .chain(retained)
        .map(move |batch| {
            arrow_tools::record_batch::try_cast_to(batch?, Arc::clone(&target_schema))
                .map_err(DataFusionError::from)
        });
    let input = futures::stream::try_unfold(
        (Box::pin(input), primary_keys),
        move |(mut input, mut primary_keys)| {
            let validation_failure = Arc::clone(&validation_failure);
            async move {
                let Some(batch) = input.next().await else {
                    return Ok(None);
                };
                let batch = batch?;
                if let Err(error) = primary_keys.validate(&batch).await {
                    *validation_failure.lock() = Some(error);
                    return Err(DataFusionError::Plan(
                        "Complete-set primary key validation failed".into(),
                    ));
                }
                Ok(Some((batch, (input, primary_keys))))
            }
        },
    );
    let input = Box::pin(RecordBatchStreamAdapter::new(Arc::clone(&schema), input));
    let sink = CayenneDataSink::new(
        table.clone_for_write(),
        InsertOp::Overwrite,
        schema,
        Arc::clone(table.context()),
    );
    let result = sink.write_all(input, task).await;
    if let Some(error) = validation_error.lock().take() {
        return Err(error);
    }
    result?;
    table.context().record_ingest(
        rows,
        deleted.load(Ordering::Relaxed),
        bytes,
        start.elapsed(),
        None,
    );
    Ok(CayenneCdcWrite::completed(table.clone_for_write(), rows))
}
