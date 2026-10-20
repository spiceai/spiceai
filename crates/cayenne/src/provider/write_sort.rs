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

//! Stable, memory-accounted primary-key run sorting before file sinks.

use arrow::array::{ArrayRef, RecordBatch, UInt32Array, UInt64Array};
use arrow::compute::{
    SortColumn, SortOptions, concat, interleave_record_batch, lexsort_to_indices,
};
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryPool, MemoryReservation};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion_common::{DataFusionError, Result};
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use futures::StreamExt;
use std::sync::Arc;

static COUNTERS: [std::sync::atomic::AtomicU64; 6] =
    [const { std::sync::atomic::AtomicU64::new(0) }; 6];

/// Admitted batches, refused charges, unsorted passthrough batches, sealed runs,
/// maximum sealed input bytes and maximum sealed rows.
#[doc(hidden)]
#[must_use]
pub fn experiment_write_sort_counters() -> [u64; 6] {
    std::array::from_fn(|i| COUNTERS[i].load(std::sync::atomic::Ordering::Relaxed))
}

fn count(index: usize) {
    COUNTERS[index].fetch_add(1, std::sync::atomic::Ordering::Relaxed);
}

struct Buffered {
    batches: Vec<RecordBatch>,
    rows: usize,
    bytes: usize,
    reservation: MemoryReservation,
    retained: arrow_tools::batch_bytes::RetainedBytes,
}
impl Buffered {
    fn new(pool: &Arc<dyn MemoryPool>) -> Self {
        Self {
            batches: Vec::new(),
            rows: 0,
            bytes: 0,
            reservation: MemoryConsumer::new("Cayenne PK write run").register(pool),
            retained: arrow_tools::batch_bytes::RetainedBytes::new(),
        }
    }
}
struct SortedRun {
    input: Buffered,
    starts: Vec<usize>,
    order: UInt32Array,
    emitted: usize,
}
impl SortedRun {
    fn next(&mut self, batch_size: usize) -> Result<Option<RecordBatch>> {
        if self.emitted == self.input.rows {
            return Ok(None);
        }
        let end = self.emitted.saturating_add(batch_size).min(self.input.rows);
        let positions: Vec<_> = self.order.values()[self.emitted..end]
            .iter()
            .map(|&row| {
                let row = row as usize;
                let batch = self
                    .starts
                    .partition_point(|&start| start <= row)
                    .saturating_sub(1);
                (batch, row - self.starts[batch])
            })
            .collect();
        let batches: Vec<_> = self.input.batches.iter().collect();
        let result = interleave_record_batch(&batches, &positions)?;
        self.emitted = end;
        Ok(Some(result))
    }
}
fn sort_run(input: Buffered, keys: &[usize]) -> Result<SortedRun> {
    let mut columns = Vec::with_capacity(keys.len() + 1);
    for &key in keys {
        let arrays: Vec<_> = input
            .batches
            .iter()
            .map(|b| b.column(key).as_ref())
            .collect();
        columns.push(SortColumn {
            values: concat(&arrays)?,
            options: Some(SortOptions::default()),
        });
    }
    // Arrival order is significant for repeated keys. A unique last column
    // makes lexsort stable even when its implementation uses unstable sorting.
    columns.push(SortColumn {
        values: Arc::new(UInt64Array::from_iter_values(0..input.rows as u64)) as ArrayRef,
        options: Some(SortOptions::default()),
    });
    let order = lexsort_to_indices(&columns, None)?;
    let mut offset = 0;
    let starts = input
        .batches
        .iter()
        .map(|b| {
            let start = offset;
            offset += b.num_rows();
            start
        })
        .collect();
    Ok(SortedRun {
        input,
        starts,
        order,
        emitted: 0,
    })
}
struct State {
    stream: SendableRecordBatchStream,
    pool: Arc<dyn MemoryPool>,
    buffered: Buffered,
    sorted: Option<SortedRun>,
    pending: Option<RecordBatch>,
    passthrough: Option<(RecordBatch, usize)>,
    keys: Arc<[usize]>,
    budget: usize,
    batch_size: Option<usize>,
    ended: bool,
    failed: bool,
    counters: Option<Arc<super::experiment_counters::Counters>>,
}
impl State {
    async fn seal(&mut self) -> Result<()> {
        count(3);
        COUNTERS[4].fetch_max(
            u64::try_from(self.buffered.bytes).unwrap_or(u64::MAX),
            std::sync::atomic::Ordering::Relaxed,
        );
        COUNTERS[5].fetch_max(
            u64::try_from(self.buffered.rows).unwrap_or(u64::MAX),
            std::sync::atomic::Ordering::Relaxed,
        );
        let input = std::mem::replace(&mut self.buffered, Buffered::new(&self.pool));
        let keys = Arc::clone(&self.keys);
        // Move the reservation into the job: cancellation must not release the
        // charge while a detached blocking job still owns the buffered arrays.
        self.sorted = Some(if input.rows > 4096 || input.bytes > 1024 * 1024 {
            tokio::task::spawn_blocking(move || sort_run(input, &keys))
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))??
        } else {
            sort_run(input, &keys)?
        });
        Ok(())
    }
    async fn next(&mut self) -> Result<Option<RecordBatch>> {
        loop {
            if let Some(run) = &mut self.sorted {
                if let Some(batch) = run.next(self.batch_size.unwrap_or(8192))? {
                    if let Some(counters) = &self.counters {
                        counters.add(0, batch.num_rows());
                    }
                    super::experiment_counters::log();
                    return Ok(Some(batch));
                }
                self.sorted = None;
            }
            if let Some((batch, offset)) = &mut self.passthrough {
                let rows = self
                    .batch_size
                    .unwrap_or(8192)
                    .min(batch.num_rows() - *offset);
                let result = batch.slice(*offset, rows);
                if let Some(counters) = &self.counters {
                    counters.add(1, rows);
                }
                super::experiment_counters::log();
                *offset += rows;
                if *offset == batch.num_rows() {
                    self.passthrough = None;
                }
                return Ok(Some(result));
            }
            if self.buffered.rows > 0 && (self.ended || self.buffered.bytes >= self.budget) {
                self.seal().await?;
                continue;
            }
            let batch = match self.pending.take() {
                Some(batch) => Some(batch),
                None => self.stream.next().await.transpose()?,
            };
            let Some(batch) = batch else {
                self.ended = true;
                if self.buffered.rows == 0 {
                    return Ok(None);
                }
                continue;
            };
            if batch.num_rows() == 0 {
                return Ok(Some(batch));
            }
            self.batch_size.get_or_insert(batch.num_rows().min(8192));
            // MemorySource slices can share one large allocation. Bill retained
            // buffers once across the run, while charging per-row sort scratch.
            self.buffered.retained.add(&batch);
            let retained_bytes =
                usize::try_from(self.buffered.retained.total()).unwrap_or(usize::MAX);
            let bytes = retained_bytes.saturating_sub(self.buffered.bytes);
            // Include key concatenation, one output batch, and conservative
            // lexsort/index/interleave scratch in the admission charge.
            let charge = bytes.saturating_mul(3).saturating_add(
                batch
                    .num_rows()
                    .saturating_mul(128_usize.saturating_add(self.keys.len().saturating_mul(16))),
            );
            if self.buffered.rows.saturating_add(batch.num_rows()) > u32::MAX as usize
                || self.buffered.reservation.try_grow(charge).is_err()
            {
                count(1);
                if self.buffered.rows > 0 {
                    self.pending = Some(batch);
                    self.seal().await?;
                } else {
                    // No sort memory was admitted. Preserve the arriving batch
                    // without allocating a sorted copy or failing the write.
                    self.buffered.retained = arrow_tools::batch_bytes::RetainedBytes::new();
                    count(2);
                    self.passthrough = Some((batch, 0));
                }
                continue;
            }
            count(0);
            self.buffered.rows += batch.num_rows();
            self.buffered.bytes = retained_bytes;
            self.buffered.batches.push(batch);
        }
    }
}

/// Emit stable PK-sorted runs, retaining at most `budget` input bytes plus one
/// input batch. Pool refusal seals admitted rows and retries the pending batch;
/// a batch that cannot be admitted alone passes through without sorting.
#[cfg(test)]
pub(crate) fn sort_stream(
    stream: SendableRecordBatchStream,
    keys: &[String],
    task: &Arc<TaskContext>,
    budget: u64,
) -> Result<SendableRecordBatchStream> {
    sort_stream_counted(stream, keys, task, budget, None)
}

pub(crate) fn sort_stream_counted(
    stream: SendableRecordBatchStream,
    keys: &[String],
    task: &Arc<TaskContext>,
    budget: u64,
    counters: Option<Arc<super::experiment_counters::Counters>>,
) -> Result<SendableRecordBatchStream> {
    let schema = stream.schema();
    let indices: Vec<_> = keys
        .iter()
        .map(|key| schema.index_of(key))
        .collect::<std::result::Result<_, _>>()?;
    let pool = Arc::clone(&task.runtime_env().memory_pool);
    let state = State {
        stream,
        buffered: Buffered::new(&pool),
        pool,
        sorted: None,
        pending: None,
        passthrough: None,
        keys: indices.into(),
        budget: usize::try_from(budget).unwrap_or(usize::MAX),
        batch_size: None,
        ended: false,
        failed: false,
        counters,
    };
    let output = futures::stream::unfold(state, |mut state| async move {
        if state.failed {
            return None;
        }
        match state.next().await {
            Ok(Some(batch)) => Some((Ok(batch), state)),
            Ok(None) => None,
            Err(error) => {
                state.failed = true;
                Some((Err(error), state))
            }
        }
    });
    Ok(Box::pin(RecordBatchStreamAdapter::new(schema, output)))
}

/// Count rows handed to a native shard sorter or excluded from the pre-sink sorter.
pub(crate) fn observe_stream(
    stream: SendableRecordBatchStream,
    counters: Arc<super::experiment_counters::Counters>,
    reason: usize,
) -> SendableRecordBatchStream {
    let schema = stream.schema();
    Box::pin(RecordBatchStreamAdapter::new(
        schema,
        stream.map(move |result| {
            if let Ok(batch) = &result {
                counters.add(reason, batch.num_rows());
            }
            super::experiment_counters::log();
            result
        }),
    ))
}

/// Inspect native shard-sort output at the sink's physical-position callback.
/// This distinguishes successful sorting from input merely offered to a sorter.
#[derive(Debug)]
pub(crate) struct NativeSortObserver {
    pub(crate) counters: Arc<super::experiment_counters::Counters>,
    pub(crate) keys: Vec<String>,
    pub(crate) inner: Option<Arc<dyn vortex_datafusion::VortexWriteObserver>>,
}
impl vortex_datafusion::VortexWriteObserver for NativeSortObserver {
    fn batch_written(&self, path: &object_store::path::Path, position: u64, batch: &RecordBatch) {
        let comparators = self
            .keys
            .iter()
            .map(|key| {
                let index = batch.schema().index_of(key)?;
                let column = batch.column(index);
                arrow::array::make_comparator(
                    column.as_ref(),
                    column.as_ref(),
                    SortOptions {
                        descending: false,
                        nulls_first: true,
                    },
                )
            })
            .collect::<std::result::Result<Vec<_>, arrow::error::ArrowError>>();
        match comparators {
            Ok(comparators) => {
                let sorted = (1..batch.num_rows()).all(|row| {
                    comparators
                        .iter()
                        .map(|compare| compare(row - 1, row))
                        .find(|order| !order.is_eq())
                        .is_none_or(|order| !order.is_gt())
                });
                self.counters
                    .add(if sorted { 7 } else { 8 }, batch.num_rows());
            }
            Err(error) => {
                self.counters.add(9, batch.num_rows());
                tracing::info!(%error, "Cayenne prototype could not inspect native shard-sort output");
            }
        }
        if let Some(inner) = &self.inner {
            inner.batch_written(path, position, batch);
        }
        super::experiment_counters::log();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::execution::memory_pool::GreedyMemoryPool;
    use datafusion::execution::runtime_env::RuntimeEnvBuilder;
    use datafusion::prelude::{SessionConfig, SessionContext};
    use futures::TryStreamExt;

    #[tokio::test]
    async fn sorted_write_runs_are_stable_and_conserve_rows_under_refusal() {
        for refused in [false, true] {
            let runtime = RuntimeEnvBuilder::new()
                .with_memory_pool(Arc::new(GreedyMemoryPool::new(if refused {
                    0
                } else {
                    16 * 1024 * 1024
                })))
                .build_arc()
                .expect("sort runtime");
            let context = SessionContext::new_with_config_rt(SessionConfig::new(), runtime);
            let schema = Arc::new(Schema::new(vec![
                Field::new("k", DataType::Int64, true),
                Field::new("v", DataType::Int64, false),
            ]));
            let input: Vec<_> = (0_i64..10000).collect();
            let batch_schema = Arc::clone(&schema);
            let batches = (0_i64..5).map(move |chunk| {
                let rows: Vec<_> = (chunk * 2000..(chunk + 1) * 2000).collect();
                RecordBatch::try_new(
                    Arc::clone(&batch_schema),
                    vec![
                        Arc::new(Int64Array::from(
                            rows.iter()
                                .map(|&i| if i % 29 == 0 { None } else { Some(i % 17) })
                                .collect::<Vec<_>>(),
                        )),
                        Arc::new(Int64Array::from(rows)),
                    ],
                )
                .expect("stable-sort corpus")
            });
            let source = Box::pin(RecordBatchStreamAdapter::new(
                Arc::clone(&schema),
                futures::stream::iter(batches.map(Ok)),
            ));
            let output: Vec<_> = sort_stream(
                source,
                &["k".into()],
                &context.task_ctx(),
                128 * 1024 * 1024,
            )
            .expect("sort stream")
            .try_collect()
            .await
            .expect("sorted batches");
            let mut actual = Vec::new();
            for batch in output {
                assert!(batch.num_rows() <= 2000);
                let values = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("values");
                actual.extend_from_slice(values.values());
            }
            let mut expected = input;
            if !refused {
                expected.sort_by_key(|i| if i % 29 == 0 { None } else { Some(i % 17) });
            }
            assert_eq!(actual, expected, "refused={refused}");
            assert_eq!(context.runtime_env().memory_pool.reserved(), 0);
        }
    }

    #[tokio::test]
    async fn write_sort_realistic_pool_pressure() {
        use arrow::array::Int32Array;
        for held_mb in [0, 63] {
            let pool = Arc::new(GreedyMemoryPool::new(64 * 1024 * 1024)) as Arc<dyn MemoryPool>;
            let other = MemoryConsumer::new("concurrent query").register(&pool);
            other
                .try_grow(held_mb * 1024 * 1024)
                .expect("concurrent reservation");
            let runtime = RuntimeEnvBuilder::new()
                .with_memory_pool(Arc::clone(&pool))
                .build_arc()
                .expect("bounded runtime");
            let context = SessionContext::new_with_config_rt(SessionConfig::new(), runtime);
            let schema = Arc::new(Schema::new(
                (0..4)
                    .map(|i| Field::new(format!("k{i}"), DataType::Int32, false))
                    .collect::<Vec<_>>(),
            ));
            let batch_schema = Arc::clone(&schema);
            let batches = (0..8).map(move |part| {
                let arrays: Vec<ArrayRef> = (0..4)
                    .map(|_| {
                        Arc::new(Int32Array::from_iter_values(
                            (part * 8192..(part + 1) * 8192).rev(),
                        )) as ArrayRef
                    })
                    .collect();
                RecordBatch::try_new(Arc::clone(&batch_schema), arrays).expect("pressure batch")
            });
            let input = Box::pin(RecordBatchStreamAdapter::new(
                Arc::clone(&schema),
                futures::stream::iter(batches.map(Ok)),
            ));
            let output: Vec<_> = sort_stream(
                input,
                &["k0".into(), "k1".into(), "k2".into(), "k3".into()],
                &context.task_ctx(),
                128 * 1024 * 1024,
            )
            .expect("pressure sorter")
            .try_collect()
            .await
            .expect("pressure output");
            let mut values = Vec::new();
            let mut unsorted_batches = 0;
            for batch in output {
                let keys = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .expect("pressure keys");
                unsorted_batches += usize::from(keys.values().windows(2).any(|p| p[0] > p[1]));
                values.extend_from_slice(keys.values());
            }
            println!(
                "WRITE_SORT_PRESSURE pool_mb=64 held_mb={held_mb} batches=8 unsorted_batches={unsorted_batches}"
            );
            assert_eq!(unsorted_batches, if held_mb == 0 { 0 } else { 8 });
            values.sort_unstable();
            assert_eq!(
                values,
                (0..65536).collect::<Vec<_>>(),
                "pressure preserves every row held_mb={held_mb}"
            );
            assert_eq!(pool.reserved(), held_mb * 1024 * 1024);
        }
    }
}
