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

//! Resolving the keys a full refresh repeats after it is written, rather than
//! while it is written.
//!
//! The refresh is written once, straight into its new snapshot, with each batch
//! resolving the repeats it holds itself ([`super::key_conflicts::KeyResolver`])
//! and each row stamped with its arrival ordinal in a trailing column
//! ([`ARRIVAL_COLUMN`]) that no scan of the table reads. Once the files are
//! written, a query over their key columns, ordinals and row positions finds
//! every copy of a key other than the one the policy keeps — the highest
//! ordinal under the upsert policies, the lowest under `drop` — and returns its
//! file and position, for position deletes to hide or a fold to remove.
//!
//! The query reads only the key columns, and runs in chunks of the key space,
//! so its hash tables hold one chunk's keys at a time. Its memory follows the
//! rows a chunk covers — each row read back carries its key, ordinal, position,
//! file and key hash, and its key's aggregate state — not the bytes they were
//! written in, which shrink with how well the table's other columns compress.
//! The chunks are sized from the rows written, and from the bytes written for
//! keys too wide for the row estimate; a chunk that still outgrows the memory
//! pool cuts the key space finer.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::Instant;
use std::task::{Context, Poll};

use arrow::array::{ArrayRef, AsArray, BooleanArray, RecordBatch, UInt32Array, UInt64Array};
use arrow::compute::filter_record_batch;
use arrow::datatypes::{DataType, Field, FieldRef, Schema, SchemaRef, UInt32Type, UInt64Type};
use datafusion::catalog::streaming::StreamingTable;
use datafusion::common::{Column, JoinType, ScalarValue};
use datafusion::execution::TaskContext;
use datafusion::functions_aggregate::expr_fn::{count, max, min};
use datafusion::logical_expr::{Expr, lit};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::streaming::PartitionStream;
use datafusion::physical_plan::{RecordBatchStream, SendableRecordBatchStream};
use futures::{Stream, StreamExt, TryStreamExt};
use object_store::ObjectStore;
use vortex::VortexSessionDefault;
use vortex::array::VortexSessionExecute;
use vortex::arrow::ArrowSessionExt;
use vortex::dtype::Nullability;
use vortex::file::OpenOptionsSessionExt;
use vortex::layout::layouts::row_idx::row_idx;
use vortex_session::VortexSession;

use super::key_conflicts::KeyResolver;
use super::overwrite_layers::Survivor;
use super::table::CayenneTableProvider;

/// The trailing column a refresh resolved after its write carries: each row's
/// arrival ordinal. The table schema does not name it, so no scan reads it, and
/// compaction, which rewrites through the table schema, drops it.
pub(crate) const ARRIVAL_COLUMN: &str = "__cayenne_arrival";

const KEY_PREFIX: &str = "__cayenne_key_";
const BEST_KEY_PREFIX: &str = "__cayenne_best_key_";
const POSITION_COLUMN: &str = "__cayenne_position";
const FILE_COLUMN: &str = "__cayenne_file";
const BEST_COLUMN: &str = "__cayenne_best";
const COPIES_COLUMN: &str = "__cayenne_copies";
const HASH_COLUMN: &str = "__cayenne_key_hash";

/// Seeds the hash that assigns each key to a chunk of the duplicate query.
const CHUNK_HASH_SEED: u64 = 0x6361_7965_6e6e_6501;

/// Bytes of written data each chunk of the duplicate query covers. Written
/// files hold far fewer bytes per row than the query does, so a chunk's hash
/// tables hold more than this; see the module documentation.
const CHUNK_BYTES: u64 = 1024 * 1024 * 1024;

/// Whether full refreshes resolve their repeats after the write. A prototype
/// switch for measuring the approach against the layered write.
pub(crate) fn enabled() -> bool {
    static ENABLED: LazyLock<bool> = LazyLock::new(|| {
        std::env::var("SPICE_CAYENNE_REFRESH_DEDUP").is_ok_and(|value| value == "postpass")
    });
    *ENABLED
}

/// The bytes of written data one chunk of the duplicate query covers.
fn chunk_bytes() -> u64 {
    static OVERRIDE_MB: LazyLock<Option<u64>> = LazyLock::new(|| {
        std::env::var("SPICE_CAYENNE_REFRESH_DEDUP_CHUNK_MB")
            .ok()
            .and_then(|value| value.parse().ok())
    });
    match *OVERRIDE_MB {
        Some(mb) => mb.max(1) * 1024 * 1024,
        None => CHUNK_BYTES,
    }
}

/// Rows each chunk of the duplicate query covers. With a single `Int64` key
/// the query holds about 70 bytes per row of its chunk at its peak, so a chunk
/// of this many rows holds a little over 1 GiB.
const CHUNK_ROWS: u64 = 16 * 1024 * 1024;

/// The rows one chunk of the duplicate query covers.
fn chunk_rows() -> u64 {
    static OVERRIDE: LazyLock<Option<u64>> = LazyLock::new(|| {
        std::env::var("SPICE_CAYENNE_REFRESH_DEDUP_CHUNK_ROWS")
            .ok()
            .and_then(|value| value.parse().ok())
    });
    OVERRIDE.unwrap_or(CHUNK_ROWS).max(1)
}

/// Whether the duplicate query chunks the key space by key ranges of the
/// written files (`SPICE_CAYENNE_REFRESH_DEDUP_CHUNKING=range`) rather than by a
/// hash of the key alone. A prototype switch.
fn range_chunking() -> bool {
    static RANGE: LazyLock<bool> = LazyLock::new(|| {
        std::env::var("SPICE_CAYENNE_REFRESH_DEDUP_CHUNKING").is_ok_and(|value| value == "range")
    });
    *RANGE
}

/// Whether a step reading fewer files than the query has partitions splits
/// them into row ranges (`SPICE_CAYENNE_REFRESH_DEDUP_SPLIT=1`). A prototype
/// switch; needs the footer row counts the range chunking reads.
fn split_files() -> bool {
    static SPLIT: LazyLock<bool> = LazyLock::new(|| {
        std::env::var("SPICE_CAYENNE_REFRESH_DEDUP_SPLIT").is_ok_and(|value| value == "1")
    });
    *SPLIT
}

/// Prototype diagnostics (`SPICE_CAYENNE_POSTPASS_DEBUG=1`).
fn debug() -> bool {
    static DEBUG: LazyLock<bool> =
        LazyLock::new(|| std::env::var("SPICE_CAYENNE_POSTPASS_DEBUG").is_ok_and(|v| v == "1"));
    *DEBUG
}

/// One step of the duplicate query: the files it reads, and the hash slice
/// `(slices, slice)` of the key space it keeps from them.
#[derive(Debug, Clone, PartialEq, Eq)]
struct ChunkSpec {
    files: Vec<u32>,
    hash: (u64, u64),
}

/// The steps of the duplicate query, and what they read.
#[derive(Debug)]
struct ChunkPlan {
    specs: Vec<ChunkSpec>,
    /// The key column whose file bounds group the files, if any did.
    column: Option<usize>,
    /// Bytes of files the steps read in all (each step reads its files twice
    /// when it finds repeats; this counts them once).
    bytes_read: u64,
}

/// `chunks` hash slices of the whole key space, each reading every file.
fn hash_plan(sizes: &[u64], chunks: u64) -> ChunkPlan {
    let files: Vec<u32> = (0..sizes.len())
        .map(|file| u32::try_from(file).unwrap_or(u32::MAX))
        .collect();
    ChunkPlan {
        specs: (0..chunks)
            .map(|chunk| ChunkSpec {
                files: files.clone(),
                hash: (chunks, chunk),
            })
            .collect(),
        column: None,
        bytes_read: sizes.iter().sum::<u64>() * chunks,
    }
}

/// Groups the files into clusters whose `[min, max]` bounds on one key column
/// overlap transitively. A key's copies all hold the same value in the column,
/// and every file holding that value has bounds containing it, so all of them
/// fall in one cluster: clusters never share a key. `None` when two bounds do
/// not compare.
fn overlap_clusters(bounds: &[(ScalarValue, ScalarValue)]) -> Option<Vec<Vec<u32>>> {
    let mut order: Vec<usize> = (0..bounds.len()).collect();
    let mut incomparable = false;
    order.sort_by(|&a, &b| {
        bounds[a].0.partial_cmp(&bounds[b].0).unwrap_or_else(|| {
            incomparable = true;
            std::cmp::Ordering::Equal
        })
    });
    if incomparable {
        return None;
    }
    let mut clusters: Vec<Vec<u32>> = Vec::new();
    let mut reach: Option<&ScalarValue> = None;
    for index in order {
        let (min, max) = &bounds[index];
        let id = u32::try_from(index).ok()?;
        let joins = match reach {
            // Closed intervals: a file starting at the cluster's highest key
            // can share that key.
            Some(reach) => min.partial_cmp(reach)? != std::cmp::Ordering::Greater,
            None => false,
        };
        if joins {
            clusters.last_mut()?.push(id);
            if max.partial_cmp(reach?)? == std::cmp::Ordering::Greater {
                reach = Some(max);
            }
        } else {
            clusters.push(vec![id]);
            reach = Some(max);
        }
    }
    Some(clusters)
}

/// The steps of the duplicate query for about `chunks` chunks of the bytes
/// written. With key bounds, consecutive overlap clusters are packed into steps
/// of about a chunk's bytes, each reading only its own files; a cluster larger
/// than a chunk is split by hash, still reading only its own files. The key
/// column chosen is the one whose plan reads the fewest bytes; with no usable
/// bounds every step reads every file.
fn plan_chunks(
    sizes: &[u64],
    bounds: &[Option<Vec<(ScalarValue, ScalarValue)>>],
    chunks: u64,
) -> ChunkPlan {
    let mut best = hash_plan(sizes, chunks);
    let total: u64 = sizes.iter().sum();
    let target = total.div_ceil(chunks.max(1)).max(1);
    for (column, per_file) in bounds.iter().enumerate() {
        let Some(per_file) = per_file else { continue };
        if per_file.len() != sizes.len() {
            continue;
        }
        let Some(clusters) = overlap_clusters(per_file) else {
            continue;
        };
        let bytes = |files: &[u32]| files.iter().map(|&f| sizes[f as usize]).sum::<u64>();
        let mut specs: Vec<ChunkSpec> = Vec::new();
        let mut bytes_read = 0_u64;
        let mut pending: Vec<u32> = Vec::new();
        let flush = |files: Vec<u32>, specs: &mut Vec<ChunkSpec>, bytes_read: &mut u64| {
            if files.is_empty() {
                return;
            }
            let size = bytes(&files);
            let slices = size.div_ceil(target).max(1);
            *bytes_read += size * slices;
            for slice in 0..slices {
                specs.push(ChunkSpec {
                    files: files.clone(),
                    hash: (slices, slice),
                });
            }
        };
        for cluster in clusters {
            if bytes(&cluster) >= target {
                flush(std::mem::take(&mut pending), &mut specs, &mut bytes_read);
                flush(cluster, &mut specs, &mut bytes_read);
                continue;
            }
            if bytes(&pending) + bytes(&cluster) > target {
                flush(std::mem::take(&mut pending), &mut specs, &mut bytes_read);
            }
            pending.extend(cluster);
        }
        flush(pending, &mut specs, &mut bytes_read);
        if bytes_read < best.bytes_read {
            best = ChunkPlan {
                specs,
                column: Some(column),
                bytes_read,
            };
        }
    }
    best
}

/// `schema` followed by the arrival column.
pub(crate) fn with_arrival(schema: &SchemaRef) -> SchemaRef {
    let mut fields: Vec<FieldRef> = schema.fields().iter().cloned().collect();
    fields.push(Arc::new(Field::new(
        ARRIVAL_COLUMN,
        DataType::UInt32,
        false,
    )));
    Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone()))
}

/// Resolves each batch's own repeats per the policy and stamps every surviving
/// row with its batch's arrival sequence number.
pub(crate) struct ArrivalStream {
    input: SendableRecordBatchStream,
    resolver: KeyResolver,
    schema: SchemaRef,
    next: u64,
    stamped: Arc<std::sync::atomic::AtomicU64>,
}

impl ArrivalStream {
    pub(crate) fn new(input: SendableRecordBatchStream, resolver: KeyResolver) -> Self {
        let schema = with_arrival(&input.schema());
        Self {
            input,
            resolver,
            schema,
            next: 0,
            stamped: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        }
    }

    /// The number of batches the stream has stamped so far. A write of at most
    /// one batch repeats no key once that batch resolved its own repeats.
    pub(crate) fn stamped_batches(&self) -> Arc<std::sync::atomic::AtomicU64> {
        Arc::clone(&self.stamped)
    }
}

impl ArrivalStream {
    fn resolve(&self, batch: RecordBatch) -> super::Result<RecordBatch> {
        if !self.resolver.has_null_key(&batch) && !self.resolver.may_repeat_within(&batch)? {
            return Ok(batch);
        }
        Ok(self.resolver.resolve_batch(&batch)?.batch)
    }
}

impl Stream for ArrivalStream {
    type Item = datafusion_common::Result<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        match this.input.poll_next_unpin(cx) {
            Poll::Ready(Some(Ok(batch))) => {
                // Most batches hold no key twice; only one that may pays for the
                // exact resolution. A null key fails either way.
                let resolved = match this.resolve(batch) {
                    Ok(resolved) => resolved,
                    Err(error) => return Poll::Ready(Some(Err(error.into()))),
                };
                // A batch holds each key once, so its sequence number orders the
                // copies of a key as well as a per-row ordinal would.
                let Ok(sequence) = u32::try_from(this.next) else {
                    return Poll::Ready(Some(Err(datafusion_common::DataFusionError::Execution(
                        "a refresh of more than 4,294,967,295 record batches cannot resolve \
                         the primary keys it repeats"
                            .to_string(),
                    ))));
                };
                let arrival = UInt32Array::from_value(sequence, resolved.num_rows());
                this.next += 1;
                this.stamped
                    .store(this.next, std::sync::atomic::Ordering::Relaxed);
                let mut columns = resolved.columns().to_vec();
                columns.push(Arc::new(arrival));
                Poll::Ready(Some(
                    RecordBatch::try_new(Arc::clone(&this.schema), columns).map_err(Into::into),
                ))
            }
            other => other,
        }
    }
}

impl RecordBatchStream for ArrivalStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

/// `(file id, path, row range)` of one file, or part of one, a [`ReadBack`]
/// reads.
type ReadBackFile = (u32, String, Option<std::ops::Range<u64>>);

/// Reads one group of written files back: each row's key columns, arrival
/// ordinal, row position and file, keeping only the rows whose key falls in one
/// chunk of the key space.
#[derive(Debug)]
struct ReadBack {
    store: Arc<dyn ObjectStore>,
    /// `(file id, path)` of each file this partition reads.
    files: Vec<ReadBackFile>,
    key_names: Arc<[String]>,
    /// The key columns as stored, then the arrival column.
    stored: Arc<Field>,
    schema: SchemaRef,
    chunk: (u64, u64),
    /// Rows read back before the chunk filter (diagnostics).
    rows_read: Arc<AtomicU64>,
}

impl ReadBack {
    fn batch(
        &self,
        file: u32,
        stored: &RecordBatch,
    ) -> datafusion_common::Result<Option<RecordBatch>> {
        let keys = self.key_names.len();
        self.rows_read
            .fetch_add(stored.num_rows() as u64, Ordering::Relaxed);
        let positions = stored
            .column(keys + 1)
            .as_primitive_opt::<UInt64Type>()
            .ok_or_else(|| {
                datafusion_common::DataFusionError::Internal(
                    "row positions are not UInt64".to_string(),
                )
            })?
            .clone();
        // One hash of the key columns, which picks the row's chunk and is what
        // the first step groups by. Seeded apart from the hash DataFusion
        // repartitions by, so a chunk's rows still spread over every partition.
        let state = datafusion_common::hash_utils::RandomState::with_seed(CHUNK_HASH_SEED);
        let hashes = datafusion_common::hash_utils::with_hashes(
            stored.columns()[..keys].iter(),
            &state,
            |hashes| Ok(UInt64Array::from(hashes.to_vec())),
        )?;
        let mut columns: Vec<ArrayRef> = stored.columns()[..=keys].to_vec();
        columns.push(Arc::new(positions));
        columns.push(Arc::new(UInt32Array::from_value(file, stored.num_rows())));
        columns.push(Arc::new(hashes));
        let batch = RecordBatch::try_new(Arc::clone(&self.schema), columns)?;
        let (chunks, chunk) = self.chunk;
        if chunks <= 1 {
            return Ok(Some(batch));
        }
        let keep: BooleanArray = batch
            .column(keys + 3)
            .as_primitive::<UInt64Type>()
            .values()
            .iter()
            .map(|hash| Some(hash % chunks == chunk))
            .collect();
        if keep.true_count() == 0 {
            return Ok(None);
        }
        Ok(Some(filter_record_batch(&batch, &keep)?))
    }
}

impl PartitionStream for ReadBack {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        use vortex::expr::{get_item, pack, root};
        let this = Arc::new(Self {
            store: Arc::clone(&self.store),
            files: self.files.clone(),
            key_names: Arc::clone(&self.key_names),
            stored: Arc::clone(&self.stored),
            schema: Arc::clone(&self.schema),
            chunk: self.chunk,
            rows_read: Arc::clone(&self.rows_read),
        });
        let projection = pack(
            this.key_names
                .iter()
                .enumerate()
                .map(|(index, name)| {
                    (
                        format!("{KEY_PREFIX}{index}"),
                        get_item(name.as_str(), root()),
                    )
                })
                .chain([
                    (ARRIVAL_COLUMN.to_string(), get_item(ARRIVAL_COLUMN, root())),
                    (POSITION_COLUMN.to_string(), row_idx()),
                ]),
            Nullability::NonNullable,
        );
        let schema = Arc::clone(&this.schema);
        let files = this.files.clone();
        let stream = futures::stream::iter(files)
            .then(move |(file, path, row_range)| {
                let this = Arc::clone(&this);
                let projection = projection.clone();
                async move {
                    let session = VortexSession::default();
                    let vxf = session
                        .open_options()
                        .open_object_store(&this.store, &path)
                        .await
                        .map_err(|e| datafusion_common::DataFusionError::External(Box::new(e)))?;
                    let mut scan = vxf
                        .scan()
                        .map_err(|e| datafusion_common::DataFusionError::External(Box::new(e)))?;
                    if let Some(row_range) = row_range {
                        scan = scan.with_row_range(row_range);
                    }
                    let chunks = scan
                        .with_projection(projection)
                        .into_stream()
                        .map_err(|e| datafusion_common::DataFusionError::External(Box::new(e)))?;
                    let batches = chunks.map(move |chunk| {
                        let chunk = chunk.map_err(|e| {
                            datafusion_common::DataFusionError::External(Box::new(e))
                        })?;
                        if chunk.is_empty() {
                            return Ok(None);
                        }
                        let mut ctx = session.create_execution_ctx();
                        let array = session
                            .arrow()
                            .execute_arrow(chunk, Some(&this.stored), &mut ctx)
                            .map_err(|e| {
                                datafusion_common::DataFusionError::External(Box::new(e))
                            })?;
                        let stored = RecordBatch::from(array.as_struct_opt().ok_or_else(|| {
                            datafusion_common::DataFusionError::Internal(format!(
                                "{path}: read back did not return a struct"
                            ))
                        })?);
                        this.batch(file, &stored)
                    });
                    Ok::<_, datafusion_common::DataFusionError>(batches)
                }
            })
            .try_flatten()
            .try_filter_map(|batch| futures::future::ready(Ok(batch)));
        Box::pin(RecordBatchStreamAdapter::new(schema, stream))
    }
}

impl CayenneTableProvider {
    /// Each written file's footer `[min, max]` on each key column, by column then
    /// file; a column is `None` when any file lacks a bound on it.
    async fn written_key_bounds(
        &self,
        state: &dyn datafusion::catalog::Session,
        store: &Arc<dyn ObjectStore>,
        files: &[super::lookup_index::IndexedFile],
        key_columns: &[String],
    ) -> (Vec<Option<Vec<(ScalarValue, ScalarValue)>>>, Option<Vec<u64>>) {
        let table_schema = self.table_schema();
        let mut footers = Vec::with_capacity(files.len());
        for file in files {
            footers.push(self.written_file_statistics(state, store, file).await);
        }
        let rows = footers
            .iter()
            .map(|footer| {
                let footer = footer.as_ref().ok()?;
                match footer.num_rows {
                    datafusion_common::stats::Precision::Exact(rows) => Some(rows as u64),
                    _ => None,
                }
            })
            .collect::<Option<Vec<u64>>>();
        let bounds = key_columns
            .iter()
            .map(|name| {
                let index = table_schema.index_of(name).ok()?;
                footers
                    .iter()
                    .map(|footer| {
                        let column = footer.as_ref().ok()?.column_statistics.get(index)?;
                        let min = column.min_value.get_value()?.clone();
                        let max = column.max_value.get_value()?.clone();
                        (!min.is_null() && !max.is_null()).then_some((min, max))
                    })
                    .collect::<Option<Vec<_>>>()
            })
            .collect();
        (bounds, rows)
    }
}

impl CayenneTableProvider {
    /// The file and position of every copy of a key in `snapshot_id`'s files
    /// other than the one `survivor` keeps, by the arrival ordinal the write
    /// stamped on each row.
    ///
    /// # Errors
    ///
    /// Returns an error if the files cannot be listed or read back, or the query
    /// fails.
    pub(crate) async fn find_superseded_by_arrival(
        &self,
        snapshot_id: &str,
        survivor: Survivor,
        key_columns: &[String],
        rows_written: u64,
    ) -> super::Result<HashMap<String, Vec<u32>>> {
        let ctx = self.create_session_context();
        let state = ctx.state();
        let Some((store, files)) = self
            .lookup_index_snapshot_files(&state, snapshot_id, &self.read_schema())
            .await
        else {
            return Err(super::Error::Internal {
                table: self.table_name().to_string(),
                message: "Overwrite: failed to list the written files to resolve repeated keys"
                    .to_string(),
            });
        };
        if files.is_empty() {
            return Ok(HashMap::new());
        }
        let table_schema = self.table_schema();
        let mut stored_fields: Vec<FieldRef> = Vec::with_capacity(key_columns.len() + 2);
        let mut fields: Vec<FieldRef> = Vec::with_capacity(key_columns.len() + 3);
        for (index, name) in key_columns.iter().enumerate() {
            let field =
                table_schema
                    .field_with_name(name)
                    .map_err(|error| super::Error::Internal {
                        table: self.table_name().to_string(),
                        message: format!("Overwrite: key column missing: {error}"),
                    })?;
            let field = Arc::new(Field::new(
                format!("{KEY_PREFIX}{index}"),
                field.data_type().clone(),
                false,
            ));
            stored_fields.push(Arc::clone(&field));
            fields.push(field);
        }
        let arrival = Arc::new(Field::new(ARRIVAL_COLUMN, DataType::UInt32, false));
        stored_fields.push(Arc::clone(&arrival));
        stored_fields.push(Arc::new(Field::new(
            POSITION_COLUMN,
            DataType::UInt64,
            false,
        )));
        fields.push(arrival);
        fields.push(Arc::new(Field::new(
            POSITION_COLUMN,
            DataType::UInt64,
            false,
        )));
        fields.push(Arc::new(Field::new(FILE_COLUMN, DataType::UInt32, false)));
        fields.push(Arc::new(Field::new(HASH_COLUMN, DataType::UInt64, false)));
        let schema: SchemaRef = Arc::new(Schema::new(fields));
        let stored = Arc::new(Field::new_struct("", stored_fields, false));

        let started = Instant::now();
        let total_bytes: u64 = files.iter().map(|file| file.size).sum();
        let mut chunks = total_bytes
            .div_ceil(chunk_bytes())
            .max(rows_written.div_ceil(chunk_rows()))
            .max(1);
        let sizes: Vec<u64> = files.iter().map(|file| file.size).collect();
        // Each file's footer bounds on every key column, when chunking by key
        // ranges: every copy of a key holds the same value in each key column, so
        // only files whose bounds overlap can hold copies of one key.
        let (bounds, file_rows) = if range_chunking() || debug() {
            self.written_key_bounds(&state, &store, &files, key_columns)
                .await
        } else {
            (Vec::new(), None)
        };
        let bounds_elapsed = started.elapsed();
        if debug() {
            for (column, per_file) in key_columns.iter().zip(&bounds) {
                let Some(per_file) = per_file else {
                    eprintln!("POSTPASS file_bounds column={column} missing");
                    continue;
                };
                for (file, ((min, max), size)) in per_file.iter().zip(&sizes).enumerate() {
                    eprintln!(
                        "POSTPASS file_bounds column={column} file={file} bytes={size} min={min} max={max}"
                    );
                }
            }
        }
        let bounds = if range_chunking() { bounds } else { Vec::new() };
        let file_rows = if split_files() { file_rows } else { None };
        let started = Instant::now();
        // Diagnostics: the peak the memory pool reports while the query runs.
        let pool_peak = Arc::new(AtomicU64::new(0));
        let sampler = debug().then(|| {
            let pool = Arc::clone(&ctx.runtime_env().memory_pool);
            let peak = Arc::clone(&pool_peak);
            tokio::spawn(async move {
                loop {
                    peak.fetch_max(pool.reserved() as u64, Ordering::Relaxed);
                    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                }
            })
        });
        let paths: Vec<String> = files.into_iter().map(|file| file.path).collect();
        let key_names: Arc<[String]> = key_columns.to_vec().into();
        let query = DuplicateQuery {
            ctx: &ctx,
            store: &store,
            paths: &paths,
            key_names: &key_names,
            stored: &stored,
            schema: &schema,
            survivor,
            keys: key_columns.len(),
            file_rows: file_rows.as_deref(),
        };
        // A chunk sized from the bytes written can still outgrow a small memory
        // pool; then the key space is cut finer and the query run again.
        let superseded = loop {
            let plan = plan_chunks(&sizes, &bounds, chunks);
            if debug() {
                eprintln!(
                    "POSTPASS plan files={} total_bytes={total_bytes} target_chunks={chunks} specs={} column={:?} bytes_read={} amplification_pct={} bounds_ms={}",
                    paths.len(),
                    plan.specs.len(),
                    plan.column.map(|index| key_columns[index].as_str()),
                    plan.bytes_read,
                    plan.bytes_read * 100 / total_bytes.max(1),
                    bounds_elapsed.as_millis(),
                );
            }
            match query.run(&plan.specs).await {
                Ok(superseded) => break superseded,
                Err(error)
                    if matches!(
                        error.find_root(),
                        datafusion_common::DataFusionError::ResourcesExhausted(_)
                    ) && chunks < MAX_CHUNKS =>
                {
                    chunks = (chunks * 2).min(MAX_CHUNKS);
                }
                Err(error) => return Err(error.into()),
            }
        };
        if let Some(sampler) = sampler {
            sampler.abort();
        }
        if debug() {
            eprintln!(
                "POSTPASS query_done query_ms={} pool_peak_mb={} superseded={}",
                started.elapsed().as_millis(),
                pool_peak.load(Ordering::Relaxed) / (1024 * 1024),
                superseded.iter().map(Vec::len).sum::<usize>()
            );
        }
        // Sort each file's positions on the blocking pool, files in parallel.
        let sorts = superseded
            .into_iter()
            .enumerate()
            .filter(|(_, positions)| !positions.is_empty())
            .map(|(file, mut positions)| {
                let path = paths[file].clone();
                tokio::task::spawn_blocking(move || {
                    positions.sort_unstable();
                    (path, positions)
                })
            });
        futures::future::try_join_all(sorts)
            .await
            .map(|sorted| sorted.into_iter().collect::<HashMap<_, _>>())
            .map_err(|error| super::Error::Internal {
                table: self.table_name().to_string(),
                message: format!("Overwrite: sorting superseded positions failed: {error}"),
            })
    }
}

/// The most chunks the duplicate query cuts the key space into.
const MAX_CHUNKS: u64 = 4096;

/// One run of the duplicate query over a refresh's written files.
struct DuplicateQuery<'a> {
    ctx: &'a datafusion::prelude::SessionContext,
    store: &'a Arc<dyn ObjectStore>,
    paths: &'a [String],
    key_names: &'a Arc<[String]>,
    stored: &'a Arc<Field>,
    schema: &'a SchemaRef,
    survivor: Survivor,
    keys: usize,
    /// Each file's row count, when known: lets a step reading fewer files than
    /// the query has partitions split them into row ranges.
    file_rows: Option<&'a [u64]>,
}

impl DuplicateQuery<'_> {
    /// The positions of every superseded copy, by file id, querying the key
    /// space in `chunks` chunks.
    async fn run(&self, specs: &[ChunkSpec]) -> datafusion_common::Result<Vec<Vec<u32>>> {
        let ctx = self.ctx;
        let survivor = self.survivor;
        let schema = self.schema;
        let partitions = ctx.state().config().target_partitions().max(1);
        // Positions of superseded copies, by file id; each copy is emitted once,
        // since the join's build side holds each repeated key once.
        let mut superseded: Vec<Vec<u32>> = vec![Vec::new(); self.paths.len()];
        for (chunk_index, spec) in specs.iter().enumerate() {
            let chunk_started = Instant::now();
            let rows_read = Arc::new(AtomicU64::new(0));
            let mut groups: Vec<Vec<ReadBackFile>> = vec![Vec::new(); partitions];
            let splits = match self.file_rows {
                Some(_) if spec.files.len() < partitions => {
                    partitions.div_ceil(spec.files.len().max(1)) as u64
                }
                _ => 1,
            };
            let mut slot = 0;
            for &id in &spec.files {
                let path = &self.paths[id as usize];
                match self.file_rows {
                    Some(rows) if splits > 1 => {
                        let rows = rows[id as usize];
                        let step = rows.div_ceil(splits).max(1);
                        let mut start = 0;
                        while start < rows {
                            let end = (start + step).min(rows);
                            groups[slot % partitions].push((id, path.clone(), Some(start..end)));
                            slot += 1;
                            start = end;
                        }
                    }
                    _ => {
                        groups[slot % partitions].push((id, path.clone(), None));
                        slot += 1;
                    }
                }
            }
            let streams: Vec<Arc<dyn PartitionStream>> = groups
                .into_iter()
                .filter(|group| !group.is_empty())
                .map(|files| {
                    Arc::new(ReadBack {
                        store: Arc::clone(self.store),
                        files,
                        key_names: Arc::clone(self.key_names),
                        stored: Arc::clone(self.stored),
                        schema: Arc::clone(schema),
                        chunk: spec.hash,
                        rows_read: Arc::clone(&rows_read),
                    }) as Arc<dyn PartitionStream>
                })
                .collect();
            let table = Arc::new(StreamingTable::try_new(Arc::clone(schema), streams)?);
            let column = |name: &str| Expr::Column(Column::from_name(name));
            let rows = ctx.read_table(Arc::clone(&table) as _)?;
            let keys: Vec<String> = (0..self.keys).map(|i| format!("{KEY_PREFIX}{i}")).collect();
            let best_keys: Vec<String> = (0..self.keys)
                .map(|i| format!("{BEST_KEY_PREFIX}{i}"))
                .collect();
            let best = match survivor {
                Survivor::Latest => max(column(ARRIVAL_COLUMN)),
                Survivor::Earliest => min(column(ARRIVAL_COLUMN)),
            };
            let repeated = ctx
                .read_table(Arc::clone(&table) as _)?
                .aggregate(
                    keys.iter().map(|key| column(key)).collect(),
                    vec![best.alias(BEST_COLUMN), count(lit(1)).alias(COPIES_COLUMN)],
                )?
                .filter(column(COPIES_COLUMN).gt(lit(1_i64)))?
                .select(
                    keys.iter()
                        .zip(&best_keys)
                        .map(|(key, best_key)| column(key).alias(best_key))
                        .chain(std::iter::once(column(BEST_COLUMN))),
                )?;
            // The repeated keys are the join's build side either way; holding them
            // here lets a chunk with none skip reading its rows a second time.
            let repeated_batches = repeated.collect().await?;
            let aggregate_elapsed = chunk_started.elapsed();
            let aggregate_rows = rows_read.load(Ordering::Relaxed);
            let repeated_keys: usize = repeated_batches.iter().map(RecordBatch::num_rows).sum();
            if repeated_keys == 0 {
                if debug() {
                    eprintln!(
                        "POSTPASS chunk={chunk_index} files={} hash={:?} rows_read_agg={aggregate_rows} repeated_keys=0 agg_ms={}",
                        spec.files.len(),
                        spec.hash,
                        aggregate_elapsed.as_millis()
                    );
                }
                continue;
            }
            let repeated = ctx.read_batches(repeated_batches)?;
            let superseded_copy = match survivor {
                Survivor::Latest => column(ARRIVAL_COLUMN).lt(column(BEST_COLUMN)),
                Survivor::Earliest => column(ARRIVAL_COLUMN).gt(column(BEST_COLUMN)),
            };
            let left: Vec<&str> = keys.iter().map(String::as_str).collect();
            let right: Vec<&str> = best_keys.iter().map(String::as_str).collect();
            let joined = rows
                .join(
                    repeated,
                    JoinType::Inner,
                    &left,
                    &right,
                    Some(superseded_copy),
                )?
                .select(vec![column(FILE_COLUMN), column(POSITION_COLUMN)])?;
            let mut stream = joined.execute_stream().await?;
            while let Some(batch) = stream.next().await {
                let batch = batch?;
                let files = batch.column(0).as_primitive::<UInt32Type>();
                let positions = batch.column(1).as_primitive::<UInt64Type>();
                if arrow::compute::max(positions).is_some_and(|max| max > u64::from(u32::MAX)) {
                    return Err(datafusion_common::DataFusionError::Execution(
                        "a row position exceeds the position-delete range".to_string(),
                    ));
                }
                for (file, position) in files.values().iter().zip(positions.values().iter()) {
                    // Checked above: every position of the batch fits.
                    #[expect(clippy::cast_possible_truncation)]
                    superseded[*file as usize].push(*position as u32);
                }
            }
            if debug() {
                eprintln!(
                    "POSTPASS chunk={chunk_index} files={} hash={:?} rows_read_agg={aggregate_rows} rows_read_join={} repeated_keys={repeated_keys} agg_ms={} join_ms={}",
                    spec.files.len(),
                    spec.hash,
                    rows_read.load(Ordering::Relaxed) - aggregate_rows,
                    aggregate_elapsed.as_millis(),
                    chunk_started
                        .elapsed()
                        .saturating_sub(aggregate_elapsed)
                        .as_millis()
                );
            }
        }
        Ok(superseded)
    }
}

/// The column names of the table's primary key, in key order.
pub(crate) fn key_column_names(schema: &Schema, indices: &[usize]) -> Vec<String> {
    indices
        .iter()
        .map(|&index| schema.field(index).name().clone())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn arrival_is_trailing_and_not_null() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let with = with_arrival(&schema);
        assert_eq!(with.fields().len(), 2);
        assert_eq!(with.field(1).name(), ARRIVAL_COLUMN);
        assert!(!with.field(1).is_nullable());
    }

    fn int_bounds(ranges: &[(i64, i64)]) -> Vec<(ScalarValue, ScalarValue)> {
        ranges
            .iter()
            .map(|&(min, max)| (ScalarValue::Int64(Some(min)), ScalarValue::Int64(Some(max))))
            .collect()
    }

    #[test]
    fn overlap_clusters_join_touching_and_overlapping_files() {
        // Files 0 and 2 overlap, 1 starts at 2's max, 3 stands apart.
        let clusters = overlap_clusters(&int_bounds(&[(0, 10), (20, 30), (5, 20), (31, 40)]))
            .expect("comparable bounds");
        assert_eq!(clusters, vec![vec![0, 2, 1], vec![3]]);
    }

    #[test]
    fn disjoint_files_read_once_whatever_the_chunk_count() {
        let sizes = vec![100; 8];
        let bounds = vec![Some(int_bounds(&[
            (0, 9),
            (10, 19),
            (20, 29),
            (30, 39),
            (40, 49),
            (50, 59),
            (60, 69),
            (70, 79),
        ]))];
        let plan = plan_chunks(&sizes, &bounds, 4);
        assert_eq!(plan.column, Some(0));
        assert_eq!(plan.bytes_read, 800);
        assert_eq!(plan.specs.len(), 4);
        let mut files: Vec<u32> = plan.specs.iter().flat_map(|s| s.files.clone()).collect();
        files.sort_unstable();
        assert_eq!(files, (0..8).collect::<Vec<_>>());
        assert!(plan.specs.iter().all(|spec| spec.hash == (1, 0)));
    }

    #[test]
    fn overlapping_files_fall_back_to_hash_slices() {
        let sizes = vec![100; 4];
        let bounds = vec![Some(int_bounds(&[(0, 99), (0, 99), (0, 99), (0, 99)]))];
        let plan = plan_chunks(&sizes, &bounds, 3);
        assert_eq!(plan.bytes_read, 1200);
        assert_eq!(plan.specs.len(), 3);
        assert!(plan.specs.iter().all(|spec| spec.files.len() == 4));
        let hashes: Vec<(u64, u64)> = plan.specs.iter().map(|spec| spec.hash).collect();
        assert_eq!(hashes, vec![(3, 0), (3, 1), (3, 2)]);
    }

    #[test]
    fn missing_bounds_hash_every_file() {
        let plan = plan_chunks(&[100, 100], &[None], 2);
        assert_eq!(plan.column, None);
        assert_eq!(plan.bytes_read, 400);
    }
}
