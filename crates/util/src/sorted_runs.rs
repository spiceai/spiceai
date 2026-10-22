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

//! Ordering a multi-partition plan in bounded memory: every partition cuts its
//! input into fixed-size chunks, sorts each chunk and writes it to local disk as
//! a run, and the runs are stream-merged.
//!
//! A per-partition sort feeding a merge holds every partition's sorted rows
//! until the merge has consumed them, so its memory grows with the whole input.
//! Writing each sorted chunk to disk as soon as it is sorted bounds memory by
//! the chunk size times the partitions, whatever the input's size or how
//! unevenly its partitions split it, and the merge then reads every run a batch
//! at a time.

use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
use std::sync::{Arc, LazyLock, Weak};

use parking_lot::Mutex;
use tokio::sync::{OnceCell, OwnedSemaphorePermit, Semaphore};

use datafusion::arrow::array::{Array, AsArray, UInt32Array};
use datafusion::arrow::compute::{concat_batches, lexsort_to_indices, take_record_batch};
use datafusion::arrow::datatypes::{DataType, SchemaRef, i256};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::row::{OwnedRow, RowConverter, SortField};
use datafusion::common::runtime::SpawnedTask;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::memory_pool::{
    MemoryConsumer, MemoryLimit, MemoryPool, MemoryReservation,
};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::LexOrdering;
use datafusion::physical_plan::EmptyRecordBatchStream;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, SpillMetrics};
use datafusion::physical_plan::sorts::streaming_merge::{SortedSpillFile, StreamingMergeBuilder};
use datafusion::physical_plan::spill::{SpillManager, get_record_batch_memory_size};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use futures::{StreamExt, TryStreamExt};

const MIB: usize = 1024 * 1024;

/// The smallest chunk a partition sorts into one run, so a small pool shared by
/// many partitions still sorts in chunks of a useful size.
const MIN_CHUNK_BYTES: usize = 256 * 1024;

/// A partition's budget, in chunks. A chunk being sorted holds itself and its
/// concatenation; once sorted, while it is written, it holds about one copy, and
/// the partition reads its next chunk alongside only while both fit the budget
/// (see `sort_partition_into_runs`). The sort's working memory is charged beside
/// them (see `chunk_charge`).
const CHUNK_COPIES: usize = 2;

/// The most bytes one column with 32-bit offsets (`Utf8`, `Binary`, `List`, and
/// types holding them) may carry in a chunk: a chunk is concatenated before it
/// is sorted, and a concatenation past `i32::MAX` bytes overflows its offsets.
const MAX_OFFSET_BYTES: usize = 0x7FFF_FFFF;

/// How much of its memory pool one sort may plan to hold.
///
/// A sort reserves from the pool of the context it runs in. A pool shared with
/// queries must keep most of its room for them, so a sort there plans to hold
/// at most a quarter of it, and every sort in it shares at most half of it (see
/// [`Admission`]). A pool set aside for maintenance, such as Cayenne's
/// compaction pool, holds only work like this, so a sort there may plan on all
/// of it: a promotion's 8 GiB run then sorts in memory instead of spilling half
/// of itself to merge back. The pool's limit still binds every reservation, and
/// a sort the pool refuses writes what it holds to disk.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum SortBudget {
    /// A pool shared with queries: a quarter of it.
    #[default]
    Shared,
    /// A pool set aside for maintenance work: all of it.
    Dedicated,
}

impl SortBudget {
    /// The bytes of a pool of `limit` bytes this budget allows one sort.
    fn bytes(self, limit: usize) -> usize {
        match self {
            SortBudget::Shared => limit / 4,
            SortBudget::Dedicated => limit,
        }
    }

    /// The bytes of a pool of `limit` bytes every sort in it shares.
    fn shared_by_all(self, limit: usize) -> usize {
        match self {
            SortBudget::Shared => limit / 2,
            SortBudget::Dedicated => limit,
        }
    }
}

/// Batches the merge reads ahead from each run, as `DataFusion`'s spill merge
/// first tries; it falls back to one when memory is short.
const MERGE_READ_AHEAD: usize = 2;

/// A one-partition plan yielding `input` ordered by `ordering`: every partition
/// of `input` is sorted, a chunk at a time, into runs on local disk and an
/// in-memory last chunk, and those are merged as a stream. All partitions run
/// at once, with chunks sized so they fit the plan's share of the memory pool
/// (see [`Admission`]). Nothing executes until the plan is.
#[must_use]
pub fn sort_in_runs(
    input: Arc<dyn ExecutionPlan>,
    ordering: LexOrdering,
    context: &Arc<TaskContext>,
    budget: SortBudget,
) -> Arc<dyn ExecutionPlan> {
    sort_in_runs_with(
        input,
        ordering,
        context,
        RunsConfig {
            budget,
            ..RunsConfig::default()
        },
    )
}

/// Settings for [`sort_in_runs`], which tests narrow; every `None` takes the
/// default.
#[derive(Debug, Clone, Copy, Default)]
struct RunsConfig {
    /// Rows per batch written to a run. Fewer rows lower what the merge holds per
    /// run, so more runs fit in one pass. Defaults to the session's batch size.
    run_batch_rows: Option<usize>,
    /// Caps how many runs one merge pass reads, below what memory allows.
    max_fan_in: Option<usize>,
    /// The largest chunk sorted at once.
    max_chunk_bytes: Option<usize>,
    /// The most bytes a column with 32-bit offsets carries in one chunk
    /// (see [`MAX_OFFSET_BYTES`]).
    max_offset_bytes: Option<usize>,
    /// How much of the pool the sort may plan to hold.
    budget: SortBudget,
}

fn sort_in_runs_with(
    input: Arc<dyn ExecutionPlan>,
    ordering: LexOrdering,
    context: &Arc<TaskContext>,
    config: RunsConfig,
) -> Arc<dyn ExecutionPlan> {
    let schema = input.schema();
    let ctx = Arc::clone(context);
    let merged = futures::stream::once(sort_then_merge(input, ordering, ctx, config)).try_flatten();
    crate::stream_utils::stream_plan(Box::pin(RecordBatchStreamAdapter::new(schema, merged)))
}

/// Memory admission for every [`sort_in_runs`] sharing one memory pool: their
/// share of the pool (see [`SortBudget`]), in MiB, as semaphore permits. A plan that writes runs holds permits for
/// its partitions' chunks until its merged stream is dropped, so large sorts in
/// the same pool queue behind each other instead of each sizing itself against
/// the whole pool.
struct Admission {
    permits: Arc<Semaphore>,
    total: u32,
    /// The pool's limit, in bytes.
    limit: usize,
}

/// One admission per live pool. Entries hold the pool weakly and are dropped
/// with it, so a pool allocated later at the same address starts afresh.
static ADMISSIONS: LazyLock<Mutex<Vec<PoolAdmission>>> = LazyLock::new(|| Mutex::new(Vec::new()));

/// A pool, held weakly, and its admission.
type PoolAdmission = (Weak<dyn MemoryPool>, Arc<Admission>);

impl Admission {
    /// The admission of `context`'s pool, or `None` for an unbounded pool. The
    /// first sort in a pool sizes it by its budget; a pool is either shared with
    /// queries or set aside for maintenance, so every sort in it has the same one.
    fn of(context: &TaskContext, budget: SortBudget) -> Option<Arc<Admission>> {
        let pool = context.memory_pool();
        let MemoryLimit::Finite(limit) = pool.memory_limit() else {
            return None;
        };
        let mut admissions = ADMISSIONS.lock();
        admissions.retain(|(weak, _)| weak.strong_count() > 0);
        if let Some((_, admission)) = admissions
            .iter()
            .find(|(weak, _)| weak.upgrade().is_some_and(|live| Arc::ptr_eq(&live, pool)))
        {
            return Some(Arc::clone(admission));
        }
        let total = u32::try_from((budget.shared_by_all(limit) / MIB).max(1)).unwrap_or(u32::MAX);
        let admission = Arc::new(Admission {
            permits: Arc::new(Semaphore::new(total as usize)),
            total,
            limit,
        });
        admissions.push((Arc::downgrade(pool), Arc::clone(&admission)));
        Some(admission)
    }

    /// Permits for `partitions` partitions each holding [`CHUNK_COPIES`] chunks of
    /// `chunk` bytes, capped at the whole budget.
    fn permits_for_chunks(&self, chunk: usize, partitions: usize) -> u32 {
        u32::try_from(
            chunk
                .saturating_mul(CHUNK_COPIES)
                .saturating_mul(partitions)
                .div_ceil(MIB)
                .max(1),
        )
        .unwrap_or(u32::MAX)
        .min(self.total)
    }
}

/// The plan's share of the admission budget, taken the first time one of its
/// partitions writes a run. A sort whose every partition fits a chunk in
/// memory never takes it, so a small sort never queues behind a large one.
struct AdmissionGate {
    admission: Option<Arc<Admission>>,
    permits: u32,
    held: OnceCell<Option<OwnedSemaphorePermit>>,
}

impl AdmissionGate {
    fn new(admission: Option<Arc<Admission>>, permits: u32) -> Self {
        Self {
            admission,
            permits,
            held: OnceCell::new(),
        }
    }

    async fn enter(&self) -> Result<()> {
        self.held
            .get_or_try_init(|| async {
                match &self.admission {
                    Some(admission) => Arc::clone(&admission.permits)
                        .acquire_many_owned(self.permits)
                        .await
                        .map(Some)
                        .map_err(|error| DataFusionError::External(Box::new(error))),
                    None => Ok(None),
                }
            })
            .await?;
        Ok(())
    }

    fn entered(&self) -> bool {
        self.held.initialized()
    }
}

/// A pool that forwards every reservation to `inner` but refuses to let them
/// hold more than `cap` bytes in all: a merge grows its reservation for as many
/// runs as the pool allows, and this keeps it to its plan's share.
#[derive(Debug)]
struct CappedPool {
    inner: Arc<dyn MemoryPool>,
    cap: usize,
    held: AtomicUsize,
    peak: AtomicUsize,
}

impl CappedPool {
    fn new(inner: Arc<dyn MemoryPool>, cap: usize) -> Self {
        Self {
            inner,
            cap,
            held: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
        }
    }

    fn add(&self, bytes: usize) {
        let held = self.held.fetch_add(bytes, AtomicOrdering::Relaxed) + bytes;
        self.peak.fetch_max(held, AtomicOrdering::Relaxed);
    }
}

impl std::fmt::Display for CappedPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "capped({}, {})", self.cap, self.inner)
    }
}

impl MemoryPool for CappedPool {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn register(&self, consumer: &MemoryConsumer) {
        self.inner.register(consumer);
    }

    fn unregister(&self, consumer: &MemoryConsumer) {
        self.inner.unregister(consumer);
    }

    fn grow(&self, reservation: &MemoryReservation, additional: usize) {
        self.inner.grow(reservation, additional);
        self.add(additional);
    }

    fn shrink(&self, reservation: &MemoryReservation, shrink: usize) {
        self.inner.shrink(reservation, shrink);
        self.held.fetch_sub(shrink, AtomicOrdering::Relaxed);
    }

    fn try_grow(&self, reservation: &MemoryReservation, additional: usize) -> Result<()> {
        let held = self.held.load(AtomicOrdering::Relaxed);
        if held.saturating_add(additional) > self.cap {
            return Err(DataFusionError::ResourcesExhausted(format!(
                "a sorted-runs merge may hold {} bytes and holds {held}; {additional} more do not fit",
                self.cap
            )));
        }
        self.inner.try_grow(reservation, additional)?;
        self.add(additional);
        Ok(())
    }

    fn reserved(&self) -> usize {
        self.inner.reserved()
    }

    fn memory_limit(&self) -> MemoryLimit {
        match self.inner.memory_limit() {
            MemoryLimit::Finite(limit) => MemoryLimit::Finite(limit.min(self.cap)),
            MemoryLimit::Infinite | MemoryLimit::Unknown => MemoryLimit::Finite(self.cap),
        }
    }
}

/// A sorted run on disk and the rows it holds.
/// Encodes a sorted piece's first and last key so that pieces compare in the
/// sort's order, every column's direction and NULL placement included.
struct KeyEncoder {
    ordering: LexOrdering,
    converter: RowConverter,
}

impl KeyEncoder {
    fn new(schema: &SchemaRef, ordering: &LexOrdering) -> Result<Self> {
        let fields = ordering
            .iter()
            .map(|expr| {
                Ok(SortField::new_with_options(
                    expr.expr.data_type(schema)?,
                    expr.options,
                ))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            ordering: ordering.clone(),
            converter: RowConverter::new(fields)?,
        })
    }

    /// The keys of `batch`'s first and last rows.
    fn bounds(&self, first: &RecordBatch, last: &RecordBatch) -> Result<KeyBounds> {
        let key = |batch: &RecordBatch, row: usize| -> Result<OwnedRow> {
            let one = batch.slice(row, 1);
            let columns = self
                .ordering
                .iter()
                .map(|expr| expr.expr.evaluate(&one)?.into_array(1))
                .collect::<Result<Vec<_>>>()?;
            Ok(self.converter.convert_columns(&columns)?.row(0).owned())
        };
        Ok(KeyBounds {
            first: key(first, 0)?,
            last: key(last, last.num_rows() - 1)?,
        })
    }
}

/// The first and last key of a sorted piece.
#[derive(Clone)]
struct KeyBounds {
    first: OwnedRow,
    last: OwnedRow,
}

impl KeyBounds {
    /// The bounds covering both `self` and `other`.
    fn union(self, other: &KeyBounds) -> KeyBounds {
        KeyBounds {
            first: if other.first < self.first {
                other.first.clone()
            } else {
                self.first
            },
            last: if other.last > self.last {
                other.last.clone()
            } else {
                self.last
            },
        }
    }
}

struct Run {
    file: SortedSpillFile,
    rows: usize,
    bounds: KeyBounds,
}

/// A sorted chunk: its rows and the order to read them in. Batches are taken
/// from it in order, so each owns only its own rows rather than a slice of the
/// whole chunk's buffers.
///
/// The Vortex sink's run sorter (`RunSorter`) instead interleaves each output
/// batch from the buffered batches, holding no copy. That suits a file's runs,
/// which may also be written unsorted when memory runs out; here, where every
/// row must reach the merge in order, taking from one contiguous copy measured
/// 20-30% faster on a single 1 GB partition, which outweighs the copy.
struct SortedChunk {
    chunk: RecordBatch,
    order: UInt32Array,
}

impl SortedChunk {
    /// Concatenates `batches` and orders the result by `ordering`.
    fn sort(schema: &SchemaRef, batches: &[RecordBatch], ordering: &LexOrdering) -> Result<Self> {
        let chunk = concat_batches(schema, batches)?;
        if u32::try_from(chunk.num_rows()).is_err() {
            return Err(DataFusionError::Internal(format!(
                "a sorted chunk of {} rows is more than a u32 row index can address",
                chunk.num_rows()
            )));
        }
        let columns = ordering
            .iter()
            .map(|expr| expr.evaluate_to_sort_column(&chunk))
            .collect::<Result<Vec<_>>>()?;
        let order = lexsort_to_indices(&columns, None)?;
        Ok(Self { chunk, order })
    }

    fn rows(&self) -> usize {
        self.order.len()
    }

    fn bytes(&self) -> usize {
        get_record_batch_memory_size(&self.chunk) + self.order.get_array_memory_size()
    }

    /// The keys of the chunk's first and last rows in order; it holds a row.
    fn bounds(&self, keys: &KeyEncoder) -> Result<KeyBounds> {
        let first = self.batch(0, 1)?;
        let last = self.batch(self.rows() - 1, 1)?;
        keys.bounds(&first, &last)
    }

    /// The rows from `offset`, at most `len` of them, in order.
    fn batch(&self, offset: usize, len: usize) -> Result<RecordBatch> {
        Ok(take_record_batch(
            &self.chunk,
            &self.order.slice(offset, len),
        )?)
    }
}

/// A partition's last chunk, kept in memory rather than written as a run, with
/// the reservation that holds it. A partition whose input fits one chunk is
/// sorted without touching disk.
struct Tail {
    sorted: SortedChunk,
    reservation: MemoryReservation,
    bounds: KeyBounds,
}

impl Tail {
    /// The tail's rows in order, `batch_rows` at a time, read on a task of its
    /// own so that, alongside other tails, the rows are gathered in parallel
    /// rather than on the one task that merges them.
    fn into_merge_input(self, batch_rows: usize, gatherers: usize) -> SendableRecordBatchStream {
        datafusion::physical_plan::common::spawn_buffered(
            self.into_stream(batch_rows, gatherers),
            MERGE_READ_AHEAD,
        )
    }

    /// The tail's rows in order, `batch_rows` at a time; the reservation is held
    /// until the stream is dropped.
    ///
    /// Taking a batch's rows from the chunk reads each column at the rows' places
    /// across all of it, which is slow for a large chunk of many columns on one
    /// task. `gatherers` batches are taken at once, each on a task of its own,
    /// and emitted in order.
    fn into_stream(self, batch_rows: usize, gatherers: usize) -> SendableRecordBatchStream {
        let Tail {
            sorted,
            reservation,
            ..
        } = self;
        let schema = sorted.chunk.schema();
        let rows = sorted.rows();
        let batch_rows = batch_rows.max(1);
        // A chunk of one output batch gains nothing from another task.
        if gatherers <= 1 || rows <= batch_rows {
            let batches = futures::stream::iter((0..rows).step_by(batch_rows)).map(move |offset| {
                let _held_until_the_tail_is_read = &reservation;
                sorted.batch(offset, batch_rows.min(rows - offset))
            });
            return Box::pin(RecordBatchStreamAdapter::new(schema, batches));
        }
        let sorted = Arc::new(sorted);
        let batches = futures::stream::iter((0..rows).step_by(batch_rows))
            .map(move |offset| {
                let sorted = Arc::clone(&sorted);
                SpawnedTask::spawn(
                    async move { sorted.batch(offset, batch_rows.min(rows - offset)) },
                )
                .join()
            })
            .buffered(gatherers)
            .map(move |joined| {
                let _held_until_the_tail_is_read = &reservation;
                joined.map_err(|error| DataFusionError::External(Box::new(error)))?
            });
        Box::pin(RecordBatchStreamAdapter::new(schema, batches))
    }
}

/// The most tasks one in-memory chunk is read with.
const MAX_GATHERERS: usize = 16;

/// The tasks each of `tails` in-memory chunks is read with: the session's
/// parallelism shared among them, so one chunk read alone gets many and many
/// chunks read at once get one each rather than oversubscribing the cores.
fn gatherers_per_tail(ctx: &TaskContext, tails: usize) -> usize {
    (ctx.session_config().target_partitions() / tails.max(1)).clamp(1, MAX_GATHERERS)
}

/// One partition's sorted output: the runs it wrote and its in-memory tail.
struct PartitionRuns {
    tail: Option<Tail>,
    runs: Vec<Run>,
    rows: usize,
}

/// Writes `sorted` to disk as one run, in batches of `batch_rows`.
fn write_run(
    sorted: &SortedChunk,
    batch_rows: usize,
    spill_manager: &SpillManager,
    keys: &KeyEncoder,
) -> Result<Option<Run>> {
    let rows = sorted.rows();
    if rows == 0 {
        return Ok(None);
    }
    let bounds = sorted.bounds(keys)?;
    let mut run = spill_manager.create_in_progress_file("sorted run")?;
    let mut max_record_batch_memory = 0;
    for offset in (0..rows).step_by(batch_rows.max(1)) {
        let batch = sorted.batch(offset, batch_rows.min(rows - offset))?;
        max_record_batch_memory = max_record_batch_memory.max(get_record_batch_memory_size(&batch));
        run.append_batch(&batch)?;
    }
    Ok(run.finish()?.map(|file| Run {
        file: SortedSpillFile {
            file,
            max_record_batch_memory,
        },
        rows,
        bounds,
    }))
}

/// What `array` adds to a concatenation's 32-bit offsets: a string or binary
/// column's value bytes, the whole size of any other type that holds 32-bit
/// offsets (an upper bound), and nothing for a type without them.
fn offset_bytes(array: &dyn Array) -> usize {
    let span = |offsets: &[i32]| match (offsets.first(), offsets.last()) {
        (Some(first), Some(last)) => usize::try_from(last - first).unwrap_or(0),
        _ => 0,
    };
    match array.data_type() {
        DataType::Utf8 => span(array.as_string::<i32>().value_offsets()),
        DataType::Binary => span(array.as_binary::<i32>().value_offsets()),
        data_type if has_offsets_i32(data_type) => array.get_array_memory_size(),
        _ => 0,
    }
}

/// Whether `data_type` holds 32-bit offsets anywhere in it.
fn has_offsets_i32(data_type: &DataType) -> bool {
    match data_type {
        DataType::Utf8
        | DataType::Binary
        | DataType::List(_)
        | DataType::ListView(_)
        | DataType::Map(..) => true,
        DataType::LargeList(field)
        | DataType::LargeListView(field)
        | DataType::FixedSizeList(field, _) => has_offsets_i32(field.data_type()),
        DataType::Struct(fields) => fields
            .iter()
            .any(|field| has_offsets_i32(field.data_type())),
        DataType::Union(fields, _) => fields
            .iter()
            .any(|(_, field)| has_offsets_i32(field.data_type())),
        DataType::Dictionary(_, values) => has_offsets_i32(values),
        DataType::RunEndEncoded(_, values) => has_offsets_i32(values.data_type()),
        _ => false,
    }
}

/// What sorting a chunk allocates per row for a key column of `data_type`,
/// beside the chunk: its `(row index, sort value)` pair and the row-index
/// vectors it partitions the rows into and emits, which overlap the pairs.
fn sort_scratch_bytes_per_row(data_type: &DataType) -> usize {
    use std::mem::size_of;

    let pair = match data_type {
        DataType::Decimal256(..) => size_of::<(u32, i256)>(),
        DataType::Decimal128(..) | DataType::Utf8View | DataType::BinaryView => {
            size_of::<(u32, u128)>()
        }
        DataType::FixedSizeBinary(_) => size_of::<(u32, &[u8])>(),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Binary | DataType::LargeBinary => {
            size_of::<(u32, u32, u64)>()
        }
        // Every other key is commonly a native of at most 8 bytes.
        _ => size_of::<(u32, u64)>(),
    };
    pair + 2 * size_of::<u32>()
}

/// What a batch of `bytes` and `rows` reserves when it joins a chunk: itself and
/// its share of the sort's working memory. The chunk's concatenation is
/// reserved when the chunk is sorted, by which time the chunk sorted before it,
/// and its concatenation, have been released.
fn chunk_charge(bytes: usize, rows: usize, scratch_per_row: usize) -> usize {
    bytes.saturating_add(rows.saturating_mul(scratch_per_row))
}

/// A chunk being sorted and written as a run on a task of its own.
type PendingRun = SpawnedTask<Result<(usize, Option<Run>)>>;

/// Waits for the run `pending` is writing, if any, and adds it to `out`.
async fn finish_pending_run(
    pending: &mut Option<PendingRun>,
    out: &mut PartitionRuns,
) -> Result<()> {
    if let Some(task) = pending.take() {
        let (rows, run) = task
            .join()
            .await
            .map_err(|error| DataFusionError::External(Box::new(error)))??;
        out.rows += rows;
        out.runs.extend(run);
    }
    Ok(())
}

/// Sorts one partition of `input`: it buffers its input up to `chunk_bytes`,
/// then sorts the chunk and writes it to disk as a run on a task of its own
/// while it reads the next, until its input ends; the last chunk stays in
/// memory. At most one chunk is in the background at a time, so its memory is
/// about [`CHUNK_COPIES`] chunks, whatever the partition's size.
#[expect(
    clippy::too_many_arguments,
    reason = "the partition's settings, its context and the plan's shared state"
)]
async fn sort_partition_into_runs(
    input: Arc<dyn ExecutionPlan>,
    partition: usize,
    ordering: LexOrdering,
    chunk_bytes: usize,
    max_offset_bytes: usize,
    run_batch_rows: usize,
    ctx: Arc<TaskContext>,
    spill_manager: SpillManager,
    gate: Arc<AdmissionGate>,
    keys: Arc<KeyEncoder>,
) -> Result<PartitionRuns> {
    let schema = input.schema();
    let mut stream = input.execute(partition, Arc::clone(&ctx))?;
    let mut reservation =
        MemoryConsumer::new(format!("SortedRuns[{partition}]")).register(ctx.memory_pool());
    let mut out = PartitionRuns {
        tail: None,
        runs: Vec::new(),
        rows: 0,
    };
    let mut buffered: Vec<RecordBatch> = Vec::new();
    // What each buffered batch reserved (see `chunk_charge`), and their sum.
    let mut charges: Vec<usize> = Vec::new();
    let mut buffered_bytes = 0;
    // What each buffered batch adds to each column's 32-bit offsets (see
    // `offset_bytes`), and their sums per column.
    let mut offsets: Vec<Vec<usize>> = Vec::new();
    let mut offset_totals = vec![0_usize; schema.fields().len()];
    let mut pending: Option<PendingRun> = None;
    // What the chunk in the background holds: its batches and concatenation
    // while it is sorted, then the sorted chunk while it is written.
    let background = Arc::new(AtomicUsize::new(0));
    let partition_budget = chunk_bytes.saturating_mul(CHUNK_COPIES);
    let scratch_per_row = ordering
        .iter()
        .map(|expr| {
            expr.expr
                .data_type(&schema)
                .map_or(sort_scratch_bytes_per_row(&DataType::Utf8), |data_type| {
                    sort_scratch_bytes_per_row(&data_type)
                })
        })
        .max()
        .unwrap_or_default();
    // Hands the first `$count` buffered batches, and the reservation that holds
    // them, to a task that sorts them and writes them as a run; their
    // concatenation is held, on top of them, only while that task sorts them.
    // Waits for the run handed over before, so at most one is in the background.
    macro_rules! spill_in_background {
        ($count:expr) => {{
            finish_pending_run(&mut pending, &mut out).await?;
            let count = $count;
            let batches: Vec<RecordBatch> = buffered.drain(..count).collect();
            let bytes: usize = charges.drain(..count).sum();
            buffered_bytes -= bytes;
            for batch_offsets in offsets.drain(..count) {
                for (total, added) in offset_totals.iter_mut().zip(batch_offsets) {
                    *total -= added;
                }
            }
            let held = reservation.split(bytes.min(reservation.size()));
            let (schema, ordering, spill_manager, keys) = (
                Arc::clone(&schema),
                ordering.clone(),
                spill_manager.clone(),
                Arc::clone(&keys),
            );
            let concatenated: usize = batches.iter().map(get_record_batch_memory_size).sum();
            background.store(bytes.saturating_add(concatenated), AtomicOrdering::Release);
            let background = Arc::clone(&background);
            pending = Some(SpawnedTask::spawn(async move {
                held.grow(concatenated);
                let sorted = SortedChunk::sort(&schema, &batches, &ordering)?;
                drop(batches);
                // Only the sorted chunk is held while the run is written.
                held.shrink(held.size().saturating_sub(sorted.bytes()));
                background.store(held.size(), AtomicOrdering::Release);
                let run = write_run(&sorted, run_batch_rows, &spill_manager, &keys)?;
                drop(held);
                background.store(0, AtomicOrdering::Release);
                Ok((sorted.rows(), run))
            }));
        }};
    }
    while let Some(batch) = stream.next().await.transpose()? {
        let bytes = chunk_charge(
            get_record_batch_memory_size(&batch),
            batch.num_rows(),
            scratch_per_row,
        );
        // Sort what is buffered first when this batch would take a column past
        // what a concatenation's 32-bit offsets address.
        let batch_offsets: Vec<usize> = batch
            .columns()
            .iter()
            .map(|column| offset_bytes(column.as_ref()))
            .collect();
        if !buffered.is_empty()
            && offset_totals
                .iter()
                .zip(&batch_offsets)
                .any(|(total, added)| total.saturating_add(*added) > max_offset_bytes)
        {
            gate.enter().await?;
            spill_in_background!(buffered.len());
        }
        // The pool may refuse a batch another consumer needs the room for: sort
        // what is buffered first, and hold the batch regardless (a sort must hold
        // at least one batch to make progress).
        if reservation.try_grow(bytes).is_err() {
            if !buffered.is_empty() {
                gate.enter().await?;
                spill_in_background!(buffered.len());
            }
            reservation.grow(bytes);
        }
        buffered_bytes += bytes;
        charges.push(bytes);
        for (total, added) in offset_totals.iter_mut().zip(&batch_offsets) {
            *total += added;
        }
        offsets.push(batch_offsets);
        buffered.push(batch);
        // Read on beside the chunk in the background only while both fit the
        // partition's budget; otherwise wait for it to be written.
        if pending.is_some()
            && background
                .load(AtomicOrdering::Acquire)
                .saturating_add(buffered_bytes)
                > partition_budget
        {
            finish_pending_run(&mut pending, &mut out).await?;
        }
        // A full chunk writes its older half: an input that ends soon after
        // leaves the rest in memory, and a long one settles into writing half a
        // chunk while the next half is read.
        if buffered_bytes >= chunk_bytes {
            gate.enter().await?;
            let mut half = 0;
            let older = charges.iter().take_while(|charge| {
                let before = half;
                half += **charge;
                before < buffered_bytes / 2
            });
            let count = older.count().max(1);
            spill_in_background!(count);
        }
    }
    finish_pending_run(&mut pending, &mut out).await?;
    if !buffered.is_empty() {
        reservation.grow(buffered.iter().map(get_record_batch_memory_size).sum());
        let sorted = SortedChunk::sort(&schema, &buffered, &ordering)?;
        drop(buffered);
        reservation.resize(sorted.bytes());
        out.rows += sorted.rows();
        let bounds = sorted.bounds(&keys)?;
        out.tail = Some(Tail {
            sorted,
            reservation: reservation.take(),
            bounds,
        });
    }
    Ok(out)
}

/// What one merge pass holds for `run`: each batch it reads ahead is reserved
/// at twice its size, as `DataFusion`'s spill merge reserves it.
fn merge_bytes_per_run(run: &SortedSpillFile) -> usize {
    run.max_record_batch_memory
        .saturating_mul(2)
        .saturating_mul(MERGE_READ_AHEAD)
}

/// The sizes of the merge passes that bring `runs` runs down to `fan_in` or
/// fewer, so the last merge reads every remaining run in one pass. Merging the
/// smallest runs each time, the first pass takes only as many as leaves every
/// later pass full, which rewrites the fewest rows (the optimal merge pattern).
fn pre_merge_passes(runs: usize, fan_in: usize) -> Vec<usize> {
    let fan_in = fan_in.max(2);
    let mut passes = Vec::new();
    let mut remaining = runs;
    while remaining > fan_in {
        let take = if passes.is_empty() {
            (remaining - 2) % (fan_in - 1) + 2
        } else {
            fan_in
        };
        passes.push(take);
        remaining = remaining - take + 1;
    }
    passes
}

/// Merges `runs` into one run on disk, its batches `run_batch_rows` rows each.
async fn merge_into_run(
    runs: Vec<Run>,
    spill_manager: &SpillManager,
    schema: &SchemaRef,
    ordering: &LexOrdering,
    metrics: &ExecutionPlanMetricsSet,
    run_batch_rows: usize,
    pool: &Arc<dyn MemoryPool>,
) -> Result<Run> {
    let bounds = runs
        .iter()
        .skip(1)
        .fold(runs.first().map(|run| run.bounds.clone()), |bounds, run| {
            bounds.map(|bounds| bounds.union(&run.bounds))
        })
        .ok_or_else(|| {
            DataFusionError::Internal("a merge of sorted runs was given none".to_string())
        })?;
    let reservation = MemoryConsumer::new("SortedRunsPreMerge").register(pool);
    let mut merged = StreamingMergeBuilder::new()
        .with_sorted_spill_files(runs.into_iter().map(|run| run.file).collect())
        .with_spill_manager(spill_manager.clone())
        .with_schema(Arc::clone(schema))
        .with_expressions(ordering)
        .with_metrics(BaselineMetrics::new(metrics, 1))
        .with_batch_size(run_batch_rows)
        .with_reservation(reservation)
        .build()?;
    let mut out = spill_manager.create_in_progress_file("merged sorted run")?;
    let (mut rows, mut max_record_batch_memory) = (0, 0);
    while let Some(batch) = merged.next().await {
        let batch = batch?;
        rows += batch.num_rows();
        max_record_batch_memory = max_record_batch_memory.max(get_record_batch_memory_size(&batch));
        out.append_batch(&batch)?;
    }
    let file = out.finish()?.ok_or_else(|| {
        DataFusionError::Internal("a merge of sorted runs wrote no rows".to_string())
    })?;
    Ok(Run {
        file: SortedSpillFile {
            file,
            max_record_batch_memory,
        },
        rows,
        bounds,
    })
}

async fn sort_then_merge(
    input: Arc<dyn ExecutionPlan>,
    ordering: LexOrdering,
    ctx: Arc<TaskContext>,
    config: RunsConfig,
) -> Result<SendableRecordBatchStream> {
    let schema = input.schema();
    let partitions = input
        .properties()
        .output_partitioning()
        .partition_count()
        .max(1);
    let admission = Admission::of(&ctx, config.budget);
    let started = std::time::Instant::now();
    let metrics = ExecutionPlanMetricsSet::new();
    let spill_metrics = SpillMetrics::new(&metrics, 0);
    let spill_manager = SpillManager::new(ctx.runtime_env(), spill_metrics, Arc::clone(&schema));
    let output_rows = ctx.session_config().batch_size().max(1);
    let run_batch_rows = config.run_batch_rows.unwrap_or(output_rows).max(1);
    // Every partition runs at once, so a round-robin input is drained evenly and
    // buffers nothing for a partition still waiting. Each partition's chunk is
    // sized so all of them fit the admission budget; an unbounded pool never
    // needs to write a run.
    let (chunk_bytes, permits) = match &admission {
        Some(admission) => {
            let plan_bytes = config
                .budget
                .bytes(admission.limit)
                .min(admission.total as usize * MIB);
            let chunk = (plan_bytes / (partitions * CHUNK_COPIES))
                .min(config.max_chunk_bytes.unwrap_or(usize::MAX))
                .max(MIN_CHUNK_BYTES);
            (chunk, admission.permits_for_chunks(chunk, partitions))
        }
        None => (usize::MAX, 0),
    };
    let gate = Arc::new(AdmissionGate::new(admission.clone(), permits));
    let keys = Arc::new(KeyEncoder::new(&schema, &ordering)?);
    let sort = |partition| {
        sort_partition_into_runs(
            Arc::clone(&input),
            partition,
            ordering.clone(),
            chunk_bytes,
            config.max_offset_bytes.unwrap_or(MAX_OFFSET_BYTES),
            run_batch_rows,
            Arc::clone(&ctx),
            spill_manager.clone(),
            Arc::clone(&gate),
            Arc::clone(&keys),
        )
    };
    // One partition sorts on the caller's task; more each sort on their own.
    let sorted: Vec<PartitionRuns> = if partitions == 1 {
        vec![sort(0).await?]
    } else {
        futures::stream::iter(0..partitions)
            .map(|partition| SpawnedTask::spawn(sort(partition)).join())
            .buffer_unordered(partitions)
            .map(|joined| joined.map_err(|error| DataFusionError::External(Box::new(error)))?)
            .try_collect()
            .await?
    };
    let sort_ms = started.elapsed().as_millis();
    let rows_sorted: usize = sorted.iter().map(|p| p.rows).sum();
    let mut pieces: Vec<Piece> = Vec::new();
    for partition in sorted {
        pieces.extend(partition.tail.map(Piece::Tail));
        pieces.extend(partition.runs.into_iter().map(Piece::Run));
    }
    let merge = MergeContext {
        schema: Arc::clone(&schema),
        ordering,
        ctx,
        spill_manager,
        metrics,
        gate,
        admission,
        permits,
        config,
        output_rows,
        run_batch_rows,
        keys,
        partitions,
        chunk_bytes,
        rows_sorted,
        sort_ms,
    };
    let groups = disjoint_groups(pieces);
    if groups.len() <= 1 {
        return merge_group(
            groups.into_iter().flatten().collect(),
            Arc::new(merge),
            true,
        )
        .await;
    }
    // Pieces whose key ranges do not overlap need no merge between them: each
    // group of overlapping pieces is merged on its own, one after another, and
    // the groups follow each other in key order.
    tracing::debug!(
        groups = groups.len(),
        largest_group = groups.iter().map(Vec::len).max().unwrap_or(0),
        rows_sorted,
        sort_ms,
        "Sorted runs fall into key ranges that do not overlap; merging each range on its own"
    );
    let merge = Arc::new(merge);
    let gate = Arc::clone(&merge.gate);
    // The next group is set up, and starts reading ahead, while this one drains.
    let streams = futures::stream::iter(groups)
        .map(move |group| merge_group(group, Arc::clone(&merge), false))
        .buffered(2)
        .try_flatten()
        .map(move |batch| {
            let _held_until_the_last_group_ends = &gate;
            batch
        });
    Ok(Box::pin(RecordBatchStreamAdapter::new(schema, streams)))
}

/// A sorted piece of the input: a run on disk, or a partition's last chunk in
/// memory.
enum Piece {
    Run(Run),
    Tail(Tail),
}

impl Piece {
    fn bounds(&self) -> &KeyBounds {
        match self {
            Piece::Run(run) => &run.bounds,
            Piece::Tail(tail) => &tail.bounds,
        }
    }
}

/// `pieces` grouped so that no two groups' key ranges overlap, in key order:
/// a piece joins the group before it while it starts below that group's last
/// key. Rows with a key equal to a group's last key may fall in either group,
/// which only changes the relative order of equal keys.
fn disjoint_groups(mut pieces: Vec<Piece>) -> Vec<Vec<Piece>> {
    pieces.sort_by(|a, b| a.bounds().first.cmp(&b.bounds().first));
    let mut groups: Vec<Vec<Piece>> = Vec::new();
    let mut group_last: Option<OwnedRow> = None;
    for piece in pieces {
        match &mut group_last {
            Some(last) if piece.bounds().first < *last => {
                if piece.bounds().last > *last {
                    *last = piece.bounds().last.clone();
                }
            }
            _ => {
                group_last = Some(piece.bounds().last.clone());
                groups.push(Vec::new());
            }
        }
        if let Some(group) = groups.last_mut() {
            group.push(piece);
        }
    }
    groups
}

/// What merging a group of pieces needs from the sort that produced them.
struct MergeContext {
    schema: SchemaRef,
    ordering: LexOrdering,
    ctx: Arc<TaskContext>,
    spill_manager: SpillManager,
    metrics: ExecutionPlanMetricsSet,
    gate: Arc<AdmissionGate>,
    admission: Option<Arc<Admission>>,
    permits: u32,
    config: RunsConfig,
    output_rows: usize,
    run_batch_rows: usize,
    keys: Arc<KeyEncoder>,
    partitions: usize,
    chunk_bytes: usize,
    rows_sorted: usize,
    sort_ms: u128,
}

/// The rows of `pieces`, merged into one stream in key order. `whole_sort` is
/// set when the group is every row of the sort: the merge's plan is logged, and
/// a lone in-memory tail is read inline rather than ahead on a task of its own.
async fn merge_group(
    pieces: Vec<Piece>,
    merge: Arc<MergeContext>,
    whole_sort: bool,
) -> Result<SendableRecordBatchStream> {
    let MergeContext {
        schema,
        ordering,
        ctx,
        spill_manager,
        metrics,
        gate,
        admission,
        permits,
        config,
        output_rows,
        run_batch_rows,
        keys,
        partitions,
        chunk_bytes,
        rows_sorted,
        sort_ms,
    } = merge.as_ref();
    let (schema, output_rows, run_batch_rows, permits) =
        (Arc::clone(schema), *output_rows, *run_batch_rows, *permits);
    let mut tails: Vec<Tail> = Vec::new();
    let mut runs: Vec<Run> = Vec::new();
    for piece in pieces {
        match piece {
            Piece::Tail(tail) => tails.push(tail),
            Piece::Run(run) => runs.push(run),
        }
    }

    // Nothing was written to disk: the tails are the whole group.
    if runs.is_empty() {
        return match tails.len() {
            0 => Ok(Box::pin(EmptyRecordBatchStream::new(schema))),
            1 if whole_sort => Ok(tails
                .remove(0)
                .into_stream(output_rows, gatherers_per_tail(ctx, 1))),
            // One of several groups reads ahead on a task of its own.
            1 => Ok(tails
                .remove(0)
                .into_merge_input(output_rows, gatherers_per_tail(ctx, 1))),
            _ => StreamingMergeBuilder::new()
                .with_streams({
                    let gatherers = gatherers_per_tail(ctx, tails.len());
                    tails
                        .into_iter()
                        .map(|tail| tail.into_merge_input(output_rows, gatherers))
                        .collect()
                })
                .with_schema(schema)
                .with_expressions(ordering)
                .with_metrics(BaselineMetrics::new(metrics, 0))
                .with_batch_size(output_rows)
                .with_reservation(
                    MemoryConsumer::new("SortedRunsMerge").register(ctx.memory_pool()),
                )
                .build(),
        };
    }
    // One run and nothing else is read as it was written.
    if runs.len() == 1 && tails.is_empty() {
        let run = runs.remove(0);
        // `read_spill_as_stream` decodes on a task of its own, reading ahead.
        return spill_manager
            .read_spill_as_stream(run.file.file, Some(run.file.max_record_batch_memory));
    }

    // Plan the merge: as many runs per pass as the merge's memory holds, and when
    // that is fewer than the runs, pre-merge the smallest first so the last pass
    // reads every remaining run at once. The in-memory tails are already held.
    // Tails in memory leave the merge less room to read the runs. While what is
    // free in the pool does not cover a pass over every run, write the largest
    // tail to disk as a run too: it then costs the merge one read-ahead, not
    // its whole chunk.
    let mut tails_written = 0;
    if let MemoryLimit::Finite(limit) = ctx.memory_pool().memory_limit() {
        tails.sort_by_key(|tail| tail.sorted.bytes());
        loop {
            let per_run = runs
                .iter()
                .map(|run| merge_bytes_per_run(&run.file))
                .max()
                .unwrap_or(0);
            let needed = per_run.saturating_mul(runs.len() + tails.len());
            let free = limit.saturating_sub(ctx.memory_pool().reserved());
            if free >= needed {
                break;
            }
            let Some(tail) = tails.pop() else { break };
            runs.extend(write_run(
                &tail.sorted,
                run_batch_rows,
                spill_manager,
                keys,
            )?);
            tails_written += 1;
        }
    }
    let runs_written = runs.len();
    let tails_in_memory = tails.len();
    let merge_cap = (gate.entered() && admission.is_some()).then_some(permits as usize * MIB);
    let merge_pool = Arc::new(CappedPool::new(
        Arc::clone(ctx.memory_pool()),
        merge_cap.unwrap_or(usize::MAX),
    ));
    let merge_pool_dyn: Arc<dyn MemoryPool> = Arc::clone(&merge_pool) as Arc<dyn MemoryPool>;
    let largest_run_merge_bytes = runs
        .iter()
        .map(|run| merge_bytes_per_run(&run.file))
        .max()
        .unwrap_or(0);
    let memory_fan_in = merge_cap
        .map_or(usize::MAX, |cap| cap / largest_run_merge_bytes.max(1))
        .max(2);
    let fan_in = config
        .max_fan_in
        .map_or(memory_fan_in, |limit| limit.min(memory_fan_in))
        .max(2);
    let passes = pre_merge_passes(runs.len(), fan_in);
    let pre_merge_started = std::time::Instant::now();
    let mut pre_merge_rows = 0;
    runs.sort_by_key(|run| run.rows);
    for take in &passes {
        let group: Vec<Run> = runs.drain(..*take).collect();
        pre_merge_rows += group.iter().map(|run| run.rows).sum::<usize>();
        let merged = merge_into_run(
            group,
            spill_manager,
            &schema,
            ordering,
            metrics,
            run_batch_rows,
            &merge_pool_dyn,
        )
        .await?;
        let at = runs.partition_point(|run| run.rows <= merged.rows);
        runs.insert(at, merged);
    }
    let pre_merge_ms = pre_merge_started.elapsed().as_millis();
    if whole_sort {
        tracing::debug!(
            partitions,
            chunk_bytes,
            run_batch_rows,
            runs_written,
            tails_in_memory,
            tails_written,
            rows_sorted,
            merge_cap_bytes = merge_cap.unwrap_or(0),
            largest_run_merge_bytes,
            memory_fan_in,
            fan_in,
            pre_merge_passes = ?passes,
            pre_merge_rows,
            pre_merge_ms,
            final_merge_runs = runs.len(),
            sort_ms,
            "Sorted every partition into runs on disk; merging the runs"
        );
    }

    let reservation = MemoryConsumer::new("SortedRunsMerge").register(&merge_pool_dyn);
    let merged = StreamingMergeBuilder::new()
        .with_streams({
            let gatherers = gatherers_per_tail(ctx, tails.len());
            tails
                .into_iter()
                .map(|tail| tail.into_merge_input(output_rows, gatherers))
                .collect()
        })
        .with_sorted_spill_files(runs.into_iter().map(|run| run.file).collect())
        .with_spill_manager(spill_manager.clone())
        .with_schema(Arc::clone(&schema))
        .with_expressions(ordering)
        .with_metrics(BaselineMetrics::new(metrics, 0))
        .with_batch_size(output_rows)
        .with_reservation(reservation)
        .build()?;
    // The admission held for the plan is released with its merged stream.
    let gate = Arc::clone(gate);
    let merged = merged.map(move |batch| {
        let _held_until_the_merge_ends = &gate;
        batch
    });
    Ok(Box::pin(RecordBatchStreamAdapter::new(schema, merged)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, RecordBatch};
    use arrow::compute::concat_batches;
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::execution::runtime_env::RuntimeEnvBuilder;

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("key", DataType::Int64, false),
            Field::new("payload", DataType::Int64, false),
        ]))
    }

    /// `partitions` partitions of seeded pseudo-random rows with many repeated
    /// keys; partition 2 is empty.
    fn partitions(partitions: usize, rows_per_batch: usize, seed: u64) -> Vec<Vec<RecordBatch>> {
        let mut state = seed;
        let mut next = move || {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            state >> 33
        };
        (0..partitions)
            .map(|partition| {
                if partition == 2 {
                    return Vec::new();
                }
                (0..3)
                    .map(|_| {
                        let keys: Vec<i64> = (0..rows_per_batch)
                            .map(|_| i64::try_from(next() % 997).expect("small"))
                            .collect();
                        let payloads: Vec<i64> = (0..rows_per_batch)
                            .map(|_| i64::try_from(next() % 1_000_000).expect("small"))
                            .collect();
                        RecordBatch::try_new(
                            schema(),
                            vec![
                                Arc::new(Int64Array::from(keys)),
                                Arc::new(Int64Array::from(payloads)),
                            ],
                        )
                        .expect("batch")
                    })
                    .collect()
            })
            .collect()
    }

    /// One `DataFusion` sort of every row, with no code from this module.
    fn oracle(input: Arc<dyn ExecutionPlan>, columns: &[String]) -> Arc<dyn ExecutionPlan> {
        let ordering =
            crate::stream_utils::build_lex_ordering(&input.schema(), columns).expect("ordering");
        Arc::new(datafusion::physical_plan::sorts::sort::SortExec::new(
            ordering,
            Arc::new(
                datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec::new(input),
            ),
        ))
    }

    async fn collect(plan: Arc<dyn ExecutionPlan>, ctx: Arc<TaskContext>) -> RecordBatch {
        let schema = plan.schema();
        let batches = datafusion::physical_plan::collect(plan, ctx)
            .await
            .expect("plan runs");
        concat_batches(&schema, &batches).expect("concat")
    }

    async fn assert_matches_one_sort(ctx: Arc<TaskContext>, rows_per_batch: usize, seed: u64) {
        let data = partitions(6, rows_per_batch, seed);
        let columns = ["key".to_string(), "payload".to_string()];
        let input = MemorySourceConfig::try_new_exec(&data, schema(), None).expect("memory source");
        let in_runs = crate::stream_utils::sort_plan(
            Arc::clone(&input) as Arc<dyn ExecutionPlan>,
            &columns,
            &ctx,
        )
        .expect("plan builds");
        let actual = collect(in_runs, Arc::clone(&ctx)).await;

        // Oracle: one sort of every row, unbounded memory.
        let oracle_ctx = Arc::new(TaskContext::default());
        let expected = collect(oracle(input, &columns), oracle_ctx).await;

        assert_eq!(actual.num_rows(), 5 * 3 * rows_per_batch, "seed {seed}");
        assert_eq!(actual, expected, "seed {seed}");
    }

    #[tokio::test]
    async fn runs_merge_to_the_same_order_as_one_sort() {
        for seed in [1, 7, 42] {
            assert_matches_one_sort(Arc::new(TaskContext::default()), 1_000, seed).await;
        }
    }

    #[tokio::test]
    async fn a_pool_smaller_than_the_input_still_orders_every_row() {
        // 5 partitions x 3 batches x 200k rows x 16 bytes = 48 MB of input in a
        // 32 MiB pool (above the sort's own 10 MB merge reservation): one
        // partition at a time, and a partition's sort may spill.
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_limit(32 * 1024 * 1024, 1.0)
            .build_arc()
            .expect("runtime");
        let ctx = Arc::new(TaskContext::default().with_runtime(runtime));
        assert_matches_one_sort(ctx, 200_000, 3).await;
    }

    #[tokio::test]
    async fn runs_over_a_round_robin_repartition_order_every_row() {
        use datafusion::physical_plan::Partitioning;
        use datafusion::physical_plan::repartition::RepartitionExec;
        let data = partitions(3, 50_000, 11);
        let columns = ["key".to_string(), "payload".to_string()];
        let source: Arc<dyn ExecutionPlan> =
            MemorySourceConfig::try_new_exec(&data, schema(), None).expect("memory source");
        let dealt: Arc<dyn ExecutionPlan> = Arc::new(
            RepartitionExec::try_new(Arc::clone(&source), Partitioning::RoundRobinBatch(16))
                .expect("repartition"),
        );

        let ctx = Arc::new(TaskContext::default());
        let actual = collect(
            crate::stream_utils::sort_plan(dealt, &columns, &ctx).expect("plan builds"),
            Arc::clone(&ctx),
        )
        .await;
        let expected = collect(oracle(source, &columns), Arc::clone(&ctx)).await;
        assert_eq!(actual.num_rows(), 2 * 3 * 50_000);
        assert_eq!(actual, expected);
    }

    /// Six batches of 1,000 rows, each with 8 KB of string payload, keyed by
    /// seeded pseudo-random keys.
    fn string_batches() -> Vec<RecordBatch> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Int64, false),
            Field::new("payload", DataType::Utf8, false),
        ]));
        let mut state = 17_u64;
        (0..6)
            .map(|batch| {
                let keys: Vec<i64> = (0..1_000)
                    .map(|_| {
                        state = state
                            .wrapping_mul(6_364_136_223_846_793_005)
                            .wrapping_add(1_442_695_040_888_963_407);
                        i64::try_from((state >> 33) % 499).expect("small")
                    })
                    .collect();
                let payloads: Vec<String> = (0..1_000)
                    .map(|row| format!("{batch:03}-{row:04}"))
                    .collect();
                RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![
                        Arc::new(Int64Array::from(keys)),
                        Arc::new(arrow::array::StringArray::from(payloads)),
                    ],
                )
                .expect("batch")
            })
            .collect()
    }

    // A chunk is concatenated before it is sorted, so a column with 32-bit
    // offsets must never carry more bytes into one chunk than they address; a
    // sort with GiBs of strings in one partition otherwise fails with an Arrow
    // offset overflow.
    #[tokio::test]
    async fn a_chunk_ends_before_its_string_bytes_pass_what_offsets_address() {
        let data = string_batches();
        let schema = data[0].schema();
        let input: Arc<dyn ExecutionPlan> =
            MemorySourceConfig::try_new_exec(&[data], Arc::clone(&schema), None)
                .expect("memory source");
        // Payloads are unique, so the order is total and one sort fixes it.
        let columns = ["key".to_string(), "payload".to_string()];
        let ordering =
            crate::stream_utils::build_lex_ordering(&schema, &columns).expect("ordering");
        let ctx = Arc::new(TaskContext::default());
        let spill_manager = SpillManager::new(
            ctx.runtime_env(),
            SpillMetrics::new(&ExecutionPlanMetricsSet::new(), 0),
            Arc::clone(&schema),
        );
        // Room for two batches' payloads: memory never fills a chunk, so only
        // the payload's bytes end one.
        let runs = sort_partition_into_runs(
            Arc::clone(&input),
            0,
            ordering.clone(),
            usize::MAX / CHUNK_COPIES,
            2 * 8_000,
            8_192,
            Arc::clone(&ctx),
            spill_manager,
            Arc::new(AdmissionGate::new(None, 0)),
            Arc::new(KeyEncoder::new(&schema, &ordering).expect("encoder")),
        )
        .await
        .expect("runs");
        assert_eq!(runs.rows, 6_000);
        assert_eq!(
            runs.runs.iter().map(|run| run.rows).collect::<Vec<_>>(),
            vec![2_000, 2_000]
        );
        assert_eq!(
            runs.tail.as_ref().map(|tail| tail.sorted.rows()),
            Some(2_000)
        );

        // The whole sort under that bound returns exactly what one sort does.
        let config = RunsConfig {
            max_offset_bytes: Some(2 * 8_000),
            ..RunsConfig::default()
        };
        let actual = collect(
            sort_in_runs_with(Arc::clone(&input), ordering, &ctx, config),
            Arc::clone(&ctx),
        )
        .await;
        let expected = collect(oracle(input, &columns), Arc::new(TaskContext::default())).await;
        assert_eq!(actual.num_rows(), 6_000);
        assert_eq!(actual, expected);
    }

    #[tokio::test]
    async fn a_partition_larger_than_its_chunk_is_written_as_several_bounded_runs() {
        // One partition of 3 batches x 8k rows x 16 bytes (~128 KB each), with
        // ~100 KB chunks: each batch overfills a chunk, so each becomes a run.
        let data = partitions(1, 8_000, 9);
        let input: Arc<dyn ExecutionPlan> =
            MemorySourceConfig::try_new_exec(&data, schema(), None).expect("memory source");
        let ordering = LexOrdering::new(vec![
            datafusion::physical_expr::PhysicalSortExpr::new_default(Arc::new(
                datafusion::physical_expr::expressions::Column::new("key", 0),
            )),
        ])
        .expect("ordering");
        // The partition's reservations go through a capped pool, which records
        // their peak.
        let pool = Arc::new(CappedPool::new(
            Arc::new(datafusion::execution::memory_pool::UnboundedMemoryPool::default()),
            usize::MAX,
        ));
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_pool(Arc::clone(&pool) as Arc<dyn MemoryPool>)
            .build_arc()
            .expect("runtime");
        let ctx = Arc::new(TaskContext::default().with_runtime(runtime));
        let spill_manager = SpillManager::new(
            ctx.runtime_env(),
            SpillMetrics::new(&ExecutionPlanMetricsSet::new(), 0),
            schema(),
        );
        let batch = get_record_batch_memory_size(&data[0][0]);
        let runs = sort_partition_into_runs(
            input,
            0,
            ordering,
            100_000,
            MAX_OFFSET_BYTES,
            8_192,
            ctx,
            spill_manager,
            Arc::new(AdmissionGate::new(None, 0)),
            Arc::new(
                KeyEncoder::new(
                    &schema(),
                    &crate::stream_utils::build_lex_ordering(&schema(), &["key".to_string()])
                        .expect("ordering"),
                )
                .expect("encoder"),
            ),
        )
        .await
        .expect("runs");
        assert_eq!(runs.rows, 3 * 8_000);
        assert_eq!(runs.runs.len(), 3);
        assert!(runs.tail.is_none(), "the last batch filled a chunk too");
        // Each batch fills a chunk alone. At most the chunk being sorted is held,
        // with its concatenation and sort scratch, plus the one batch read before
        // the partition sees it is over budget and waits: never the whole
        // partition.
        let charge = chunk_charge(
            batch,
            data[0][0].num_rows(),
            sort_scratch_bytes_per_row(&DataType::Int64),
        );
        let peak = pool.peak.load(AtomicOrdering::Relaxed);
        let bound = (charge + batch) + charge;
        assert!(peak <= bound, "peak {peak} > {bound}");
        assert!(peak >= batch, "peak {peak} < one batch {batch}");
    }

    /// In-memory pieces of `(key)` rows, sorted by `options`, grouped by
    /// whether their key ranges overlap.
    fn group_sizes(
        ranges: &[&[Option<i64>]],
        options: datafusion::arrow::compute::SortOptions,
    ) -> Vec<usize> {
        let schema = Arc::new(Schema::new(vec![Field::new("key", DataType::Int64, true)]));
        let ordering = LexOrdering::new(vec![datafusion::physical_expr::PhysicalSortExpr::new(
            Arc::new(datafusion::physical_expr::expressions::Column::new(
                "key", 0,
            )),
            options,
        )])
        .expect("ordering");
        let keys = KeyEncoder::new(&schema, &ordering).expect("encoder");
        let pool: Arc<dyn MemoryPool> =
            Arc::new(datafusion::execution::memory_pool::UnboundedMemoryPool::default());
        let pieces = ranges
            .iter()
            .map(|values| {
                let batch = RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![Arc::new(Int64Array::from(values.to_vec()))],
                )
                .expect("batch");
                let sorted = SortedChunk::sort(&schema, &[batch], &ordering).expect("sort");
                let bounds = sorted.bounds(&keys).expect("bounds");
                Piece::Tail(Tail {
                    sorted,
                    reservation: MemoryConsumer::new("test").register(&pool),
                    bounds,
                })
            })
            .collect();
        disjoint_groups(pieces).iter().map(Vec::len).collect()
    }

    #[test]
    fn pieces_group_by_whether_their_key_ranges_overlap() {
        use datafusion::arrow::compute::SortOptions;
        let ascending = SortOptions {
            descending: false,
            nulls_first: false,
        };
        // Disjoint, given out of order: one group each, in key order.
        assert_eq!(
            group_sizes(
                &[
                    &[Some(20), Some(29)],
                    &[Some(0), Some(9)],
                    &[Some(10), Some(19)]
                ],
                ascending
            ),
            vec![1, 1, 1]
        );
        // A piece reaching into the next one's range joins it.
        assert_eq!(
            group_sizes(
                &[
                    &[Some(0), Some(15)],
                    &[Some(10), Some(19)],
                    &[Some(20), Some(29)]
                ],
                ascending
            ),
            vec![2, 1]
        );
        // Sharing only a boundary key is not an overlap.
        assert_eq!(
            group_sizes(&[&[Some(0), Some(10)], &[Some(10), Some(19)]], ascending),
            vec![1, 1]
        );
        // NULLs last, ascending: a piece of NULLs follows every other.
        assert_eq!(
            group_sizes(&[&[None, None], &[Some(0), Some(9)]], ascending),
            vec![1, 1]
        );
        // NULLs and a key in one piece span past the keyed pieces.
        assert_eq!(
            group_sizes(&[&[Some(5), None], &[Some(0), Some(9)]], ascending),
            vec![2]
        );
        // Descending, NULLs first: the same ranges group the same way.
        let descending = SortOptions {
            descending: true,
            nulls_first: true,
        };
        assert_eq!(
            group_sizes(
                &[&[Some(20), Some(29)], &[Some(0), Some(9)], &[None]],
                descending
            ),
            vec![1, 1, 1]
        );
        assert_eq!(
            group_sizes(&[&[Some(0), Some(15)], &[Some(10), Some(19)]], descending),
            vec![2]
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn reading_beside_a_chunk_being_written_stays_within_the_partitions_budget() {
        // One partition of 12 batches; a chunk is four of them, so the next chunk
        // could be read while one is sorted and written. The partition reads on
        // only while both fit its budget of two chunks.
        let data = vec![
            partitions(1, 8_000, 21)
                .remove(0)
                .into_iter()
                .cycle()
                .take(12)
                .collect::<Vec<_>>(),
        ];
        let input: Arc<dyn ExecutionPlan> =
            MemorySourceConfig::try_new_exec(&data, schema(), None).expect("memory source");
        let ordering = crate::stream_utils::build_lex_ordering(&schema(), &["key".to_string()])
            .expect("ordering");
        let pool = Arc::new(CappedPool::new(
            Arc::new(datafusion::execution::memory_pool::UnboundedMemoryPool::default()),
            usize::MAX,
        ));
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_pool(Arc::clone(&pool) as Arc<dyn MemoryPool>)
            .build_arc()
            .expect("runtime");
        let ctx = Arc::new(TaskContext::default().with_runtime(runtime));
        let spill_manager = SpillManager::new(
            ctx.runtime_env(),
            SpillMetrics::new(&ExecutionPlanMetricsSet::new(), 0),
            schema(),
        );
        let batch = get_record_batch_memory_size(&data[0][0]);
        let charge = chunk_charge(
            batch,
            data[0][0].num_rows(),
            sort_scratch_bytes_per_row(&DataType::Int64),
        );
        let chunk = 4 * charge;
        let runs = sort_partition_into_runs(
            input,
            0,
            ordering.clone(),
            chunk,
            MAX_OFFSET_BYTES,
            8_192,
            ctx,
            spill_manager,
            Arc::new(AdmissionGate::new(None, 0)),
            Arc::new(KeyEncoder::new(&schema(), &ordering).expect("encoder")),
        )
        .await
        .expect("runs");
        assert_eq!(runs.rows, 12 * 8_000);
        // Each full chunk of four batches writes its older two: five runs, and
        // the last two batches stay in memory.
        assert_eq!(runs.runs.len(), 5);
        assert_eq!(
            runs.tail.as_ref().map(|tail| tail.sorted.rows()),
            Some(2 * 8_000)
        );
        // The partition's budget, and the one batch read before it waits.
        let bound = CHUNK_COPIES * chunk + charge;
        let peak = pool.peak.load(AtomicOrdering::Relaxed);
        assert!(peak <= bound, "peak {peak} > {bound}");
    }

    #[test]
    fn the_sessions_parallelism_is_shared_among_the_chunks_read_at_once() {
        let ctx = TaskContext::default().with_session_config(
            datafusion::prelude::SessionConfig::new().with_target_partitions(32),
        );
        assert_eq!(gatherers_per_tail(&ctx, 1), MAX_GATHERERS);
        assert_eq!(gatherers_per_tail(&ctx, 8), 4);
        assert_eq!(gatherers_per_tail(&ctx, 32), 1);
        assert_eq!(gatherers_per_tail(&ctx, 100), 1);
    }

    #[test]
    fn pre_merges_leave_one_full_final_pass_and_rewrite_the_fewest_runs() {
        assert_eq!(pre_merge_passes(320, 130), vec![62, 130]);
        assert_eq!(pre_merge_passes(130, 130), Vec::<usize>::new());
        assert_eq!(pre_merge_passes(131, 130), vec![2]);
        assert_eq!(pre_merge_passes(5, 2), vec![2, 2, 2]);
        for runs in 0..400 {
            for fan_in in 2..40 {
                let passes = pre_merge_passes(runs, fan_in);
                let left = passes.iter().fold(runs, |left, take| left - take + 1);
                assert!(left <= fan_in, "{runs} runs, fan-in {fan_in}: {left} left");
                assert!(
                    passes.iter().all(|take| (2..=fan_in).contains(take)),
                    "{runs} runs, fan-in {fan_in}: {passes:?}"
                );
                // Every pass after the first is full, so no pass is wasted.
                assert!(
                    passes.iter().skip(1).all(|take| *take == fan_in),
                    "{runs} runs, fan-in {fan_in}: {passes:?}"
                );
                // The final pass is full whenever any pre-merge ran.
                if !passes.is_empty() {
                    assert_eq!(left, fan_in, "{runs} runs, fan-in {fan_in}");
                }
            }
        }
    }

    #[tokio::test]
    async fn pre_merged_runs_in_small_batches_order_every_row() {
        // Small chunks and a fan-in of 3 force many runs through several
        // pre-merge passes before the final merge.
        let data = partitions(6, 30_000, 5);
        let columns = ["key".to_string(), "payload".to_string()];
        let input: Arc<dyn ExecutionPlan> =
            MemorySourceConfig::try_new_exec(&data, schema(), None).expect("memory source");
        let ordering =
            crate::stream_utils::build_lex_ordering(&input.schema(), &columns).expect("ordering");
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_limit(64 * 1024 * 1024, 1.0)
            .build_arc()
            .expect("runtime");
        let ctx = Arc::new(TaskContext::default().with_runtime(runtime));
        let config = RunsConfig {
            run_batch_rows: Some(1_000),
            max_fan_in: Some(3),
            max_chunk_bytes: None,
            max_offset_bytes: None,
            budget: SortBudget::Shared,
        };
        let actual = collect(
            sort_in_runs_with(Arc::clone(&input), ordering, &ctx, config),
            Arc::clone(&ctx),
        )
        .await;
        let oracle_ctx = Arc::new(TaskContext::default());
        let expected = collect(oracle(input, &columns), oracle_ctx).await;
        assert_eq!(actual.num_rows(), 5 * 3 * 30_000);
        assert_eq!(actual, expected);
    }

    #[test]
    fn a_sort_in_a_shared_pool_plans_on_a_quarter_of_it_and_in_a_dedicated_one_on_all_of_it() {
        assert_eq!(SortBudget::Shared.bytes(400 * MIB), 100 * MIB);
        assert_eq!(SortBudget::Dedicated.bytes(400 * MIB), 400 * MIB);
        // Every sort in a shared pool together leaves half of it to queries.
        assert_eq!(SortBudget::Shared.shared_by_all(400 * MIB), 200 * MIB);
        assert_eq!(SortBudget::Dedicated.shared_by_all(400 * MIB), 400 * MIB);
        assert_eq!(SortBudget::default(), SortBudget::Shared);
    }

    #[test]
    fn a_rewrite_holds_two_chunks_per_partition_capped_at_the_whole_budget() {
        let admission = Admission {
            permits: Arc::new(Semaphore::new(100)),
            total: 100,
            limit: 200 * MIB,
        };
        assert_eq!(admission.permits_for_chunks(10 * MIB, 2), 40);
        assert_eq!(admission.permits_for_chunks(1, 1), 1);
        assert_eq!(admission.permits_for_chunks(64 * MIB, 32), 100);
    }

    #[tokio::test]
    async fn two_sorts_sharing_a_small_pool_both_order_every_row() {
        // Two inputs of 48 MB each sorted at once in one 32 MiB pool: their
        // partitions are admitted against one shared budget.
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_limit(32 * 1024 * 1024, 1.0)
            .build_arc()
            .expect("runtime");
        let ctx = Arc::new(TaskContext::default().with_runtime(runtime));
        futures::join!(
            assert_matches_one_sort(Arc::clone(&ctx), 200_000, 11),
            assert_matches_one_sort(Arc::clone(&ctx), 200_000, 12),
        );
    }
}
