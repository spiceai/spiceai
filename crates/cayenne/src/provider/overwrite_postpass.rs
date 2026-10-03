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

//! Resolving the keys a refresh repeats after it is written, rather than while
//! it is written: a full refresh's new snapshot, and a streaming append's.
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
//!
//! A chunk reads only the files that can hold its keys. Each file's footer
//! bounds on a key column group the files into clusters whose bounds overlap; a
//! key's copies all fall in one cluster, so a cluster is checked on its own. A
//! cluster larger than a chunk is cut into half-open key ranges — equal-width
//! from an integer column's bounds, or at quantiles of a sample of its rows —
//! pushed into the file scan, so the scan skips the zones outside a range and
//! the table's key columns are read about once in all. The ranges tile the
//! whole domain, and the rows they read must add up to the cluster's: a range
//! that missed rows would leave their repeats unresolved, so it fails the
//! write instead. Files whose bounds give no grouping fall back to hash chunks
//! that each read every file.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
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
use super::key_conflicts::Survivor;
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

/// Rows each chunk of the duplicate query covers. With a single `Int64` key
/// the query holds about 70 bytes per row of its chunk at its peak, so a chunk
/// of this many rows holds a little over 1 GiB.
const CHUNK_ROWS: u64 = 16 * 1024 * 1024;

/// A smaller [`CHUNK_ROWS`] for tests, so small tables still split into many
/// key ranges; `0` keeps the default. It changes only how the work is cut, never
/// the result.
#[cfg(test)]
pub(crate) static TEST_CHUNK_ROWS: AtomicU64 = AtomicU64::new(0);

/// The rows one chunk of the duplicate query covers.
fn chunk_rows() -> u64 {
    #[cfg(test)]
    {
        let rows = TEST_CHUNK_ROWS.load(Ordering::Relaxed);
        if rows > 0 {
            return rows;
        }
    }
    CHUNK_ROWS
}

/// One step of the duplicate query: the files it reads, the hash slice
/// `(slices, slice)` of the key space it keeps from them, and, when it covers
/// a key sub-range of an integer key column, that range — pushed into the file
/// scan, so zones outside it are never read.
#[derive(Debug, Clone, PartialEq)]
struct ChunkSpec {
    files: Vec<u32>,
    hash: (u64, u64),
    range: Option<KeyRange>,
}

/// A half-open range `[lo, hi)` of one key column (by key position); a missing
/// bound is unbounded. The ranges a cluster is split into tile the whole
/// domain, so every row of the cluster falls in exactly one of them, whatever
/// the cut points. `group` names the split cluster, whose rows the ranges must
/// add up to.
#[derive(Debug, Clone, PartialEq)]
struct KeyRange {
    column: usize,
    lo: Option<ScalarValue>,
    hi: Option<ScalarValue>,
    group: u32,
}

/// Steps cutting `files` at `cuts` (sorted, distinct) into ranges of `column`
/// that tile the whole domain.
fn cut_specs(files: &[u32], column: usize, cuts: &[ScalarValue], group: u32) -> Vec<ChunkSpec> {
    let mut bounds: Vec<Option<ScalarValue>> = Vec::with_capacity(cuts.len() + 2);
    bounds.push(None);
    bounds.extend(cuts.iter().cloned().map(Some));
    bounds.push(None);
    bounds
        .windows(2)
        .map(|pair| ChunkSpec {
            files: files.to_vec(),
            hash: (1, 0),
            range: Some(KeyRange {
                column,
                lo: pair[0].clone(),
                hi: pair[1].clone(),
                group,
            }),
        })
        .collect()
}

/// An integer key value as `i128`, or `None` for any other type.
fn integer_value(value: &ScalarValue) -> Option<i128> {
    Some(match value {
        ScalarValue::Int8(Some(v)) => i128::from(*v),
        ScalarValue::Int16(Some(v)) => i128::from(*v),
        ScalarValue::Int32(Some(v)) => i128::from(*v),
        ScalarValue::Int64(Some(v)) => i128::from(*v),
        ScalarValue::UInt8(Some(v)) => i128::from(*v),
        ScalarValue::UInt16(Some(v)) => i128::from(*v),
        ScalarValue::UInt32(Some(v)) => i128::from(*v),
        ScalarValue::UInt64(Some(v)) => i128::from(*v),
        _ => return None,
    })
}

/// `value` as a Vortex literal of the same type, for the integer and string
/// types a key sub-range supports; `None` otherwise.
fn scalar_literal(value: &ScalarValue) -> Option<vortex::expr::Expression> {
    use vortex::expr::lit;
    Some(match value {
        ScalarValue::Int8(Some(v)) => lit(*v),
        ScalarValue::Int16(Some(v)) => lit(*v),
        ScalarValue::Int32(Some(v)) => lit(*v),
        ScalarValue::Int64(Some(v)) => lit(*v),
        ScalarValue::UInt8(Some(v)) => lit(*v),
        ScalarValue::UInt16(Some(v)) => lit(*v),
        ScalarValue::UInt32(Some(v)) => lit(*v),
        ScalarValue::UInt64(Some(v)) => lit(*v),
        ScalarValue::Utf8(Some(v))
        | ScalarValue::LargeUtf8(Some(v))
        | ScalarValue::Utf8View(Some(v)) => lit(v.clone()),
        _ => return None,
    })
}

/// `value` as a scalar of the same integer type as `like`.
fn integer_like(value: i128, like: &ScalarValue) -> Option<ScalarValue> {
    Some(match like {
        ScalarValue::Int8(_) => ScalarValue::Int8(Some(i8::try_from(value).ok()?)),
        ScalarValue::Int16(_) => ScalarValue::Int16(Some(i16::try_from(value).ok()?)),
        ScalarValue::Int32(_) => ScalarValue::Int32(Some(i32::try_from(value).ok()?)),
        ScalarValue::Int64(_) => ScalarValue::Int64(Some(i64::try_from(value).ok()?)),
        ScalarValue::UInt8(_) => ScalarValue::UInt8(Some(u8::try_from(value).ok()?)),
        ScalarValue::UInt16(_) => ScalarValue::UInt16(Some(u16::try_from(value).ok()?)),
        ScalarValue::UInt32(_) => ScalarValue::UInt32(Some(u32::try_from(value).ok()?)),
        ScalarValue::UInt64(_) => ScalarValue::UInt64(Some(u64::try_from(value).ok()?)),
        _ => return None,
    })
}

/// `files` split into `slices` equal-width ranges of an integer `column`,
/// cut between the smallest minimum and the largest maximum their bounds
/// report, or `None` when the column is not an integer.
fn range_slices(
    files: &[u32],
    bounds: &[(ScalarValue, ScalarValue)],
    column: usize,
    slices: u64,
    group: u32,
) -> Option<Vec<ChunkSpec>> {
    let lo = files
        .iter()
        .map(|&f| integer_value(&bounds[f as usize].0))
        .collect::<Option<Vec<_>>>()?
        .into_iter()
        .min()?;
    let hi = files
        .iter()
        .map(|&f| integer_value(&bounds[f as usize].1))
        .collect::<Option<Vec<_>>>()?
        .into_iter()
        .max()?;
    let like = &bounds[*files.first()? as usize].0;
    let width = (hi - lo + 1).max(1);
    let slices = i128::from(slices).min(width).max(1);
    let cuts = (1..slices)
        .map(|slice| integer_like(lo + width * slice / slices, like))
        .collect::<Option<Vec<_>>>()?;
    Some(cut_specs(files, column, &cuts, group))
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
    /// Clusters to split into sampled key ranges, `(files, slices, group)`:
    /// their column is not an integer, so its bounds give no cut points.
    to_sample: Vec<(Vec<u32>, u64, u32)>,
    /// The files of each split cluster, by group.
    groups: Vec<Vec<u32>>,
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
                range: None,
            })
            .collect(),
        column: None,
        bytes_read: sizes.iter().sum::<u64>() * chunks,
        to_sample: Vec::new(),
        groups: Vec::new(),
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

    subsplit_by_range: bool,
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
        let mut to_sample = Vec::new();
        let mut groups: Vec<Vec<u32>> = Vec::new();
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
                    range: None,
                });
            }
        };
        for cluster in clusters {
            if bytes(&cluster) >= target {
                flush(std::mem::take(&mut pending), &mut specs, &mut bytes_read);
                let size = bytes(&cluster);
                if !subsplit_by_range {
                    flush(cluster, &mut specs, &mut bytes_read);
                    continue;
                }
                // Each key range reads about its share of the cluster.
                let group = u32::try_from(groups.len()).unwrap_or(u32::MAX);
                let slices = size.div_ceil(target);
                bytes_read += size;
                match range_slices(&cluster, per_file, column, slices, group) {
                    Some(sub_ranges) => specs.extend(sub_ranges),
                    None => to_sample.push((cluster.clone(), slices, group)),
                }
                groups.push(cluster);
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
                to_sample,
                groups,
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

/// `(file id, path)` of one file a [`ReadBack`] reads.
type ReadBackFile = (u32, String);

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
    /// The key sub-range this step reads, pushed into each file's scan.
    range: Option<KeyRange>,
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
            range: self.range.clone(),
            rows_read: Arc::clone(&self.rows_read),
        });
        // `key >= lo AND key < hi` on the stored column, so the scan prunes the
        // zones outside the step's key range.
        let range_filter = this.range.as_ref().and_then(|range| {
            let name = this.key_names.get(range.column)?;
            let column = get_item(name.as_str(), root());
            let lower = match &range.lo {
                Some(lo) => Some(vortex::expr::gt_eq(column.clone(), scalar_literal(lo)?)),
                None => None,
            };
            let upper = match &range.hi {
                Some(hi) => Some(vortex::expr::lt(column, scalar_literal(hi)?)),
                None => None,
            };
            Some(match (lower, upper) {
                (Some(lower), Some(upper)) => Some(vortex::expr::and(lower, upper)),
                (one, None) | (None, one) => one,
            })
        });
        if this.range.is_some() && range_filter.is_none() {
            return Box::pin(RecordBatchStreamAdapter::new(
                Arc::clone(&this.schema),
                futures::stream::once(async {
                    Err(datafusion_common::DataFusionError::Internal(
                        "a key sub-range could not be expressed as a scan filter".to_string(),
                    ))
                }),
            ));
        }
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
            .then(move |(file, path)| {
                let this = Arc::clone(&this);
                let projection = projection.clone();
                let range_filter = range_filter.clone();
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
                    if let Some(Some(filter)) = range_filter {
                        scan = scan.with_filter(filter);
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
    ) -> (
        Vec<Option<Vec<(ScalarValue, ScalarValue)>>>,
        Option<Vec<u64>>,
    ) {
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
    /// Turns each cluster the plan could not cut from its bounds into key
    /// ranges cut at quantiles of a sample of its rows, or into hash slices
    /// when no sample can be read. The ranges tile the domain whatever the
    /// sample, so a poor one costs only balance.
    async fn sample_key_ranges(
        &self,
        plan: &mut ChunkPlan,
        store: &Arc<dyn ObjectStore>,
        paths: &[String],
        file_rows: &[u64],
        column_name: &str,
        column: usize,
    ) {
        for (files, slices, group) in std::mem::take(&mut plan.to_sample) {
            if let Some(cuts) =
                sample_cut_points(store, paths, &files, file_rows, column_name, slices).await
            {
                plan.specs.extend(cut_specs(&files, column, &cuts, group));
                continue;
            }
            // Unsplit by range: no rows to account for under this group.
            if let Some(group_files) = plan.groups.get_mut(group as usize) {
                group_files.clear();
            }
            plan.specs.extend((0..slices).map(|slice| ChunkSpec {
                files: files.clone(),
                hash: (slices, slice),
                range: None,
            }));
        }
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

        let total_bytes: u64 = files.iter().map(|file| file.size).sum();
        let mut chunks = total_bytes
            .div_ceil(CHUNK_BYTES)
            .max(rows_written.div_ceil(chunk_rows()))
            .max(1);
        let sizes: Vec<u64> = files.iter().map(|file| file.size).collect();
        // Each file's footer bounds on every key column, and its row count: every
        // copy of a key holds the same value in each key column, so only files
        // whose bounds overlap can hold copies of one key.
        let (bounds, file_rows) = self
            .written_key_bounds(&state, &store, &files, key_columns)
            .await;
        let paths: Vec<String> = files.into_iter().map(|file| file.path).collect();
        let key_names: Arc<[String]> = key_columns.to_vec().into();
        // A chunk sized from the bytes and rows written can still outgrow a small
        // memory pool; then the key space is cut finer and the query run again.
        let superseded = loop {
            // Splitting a cluster into key ranges checks that the ranges read every
            // row of it, which needs every file's row count.
            let mut plan = plan_chunks(&sizes, &bounds, chunks, file_rows.is_some());
            if let (Some(column), Some(rows)) = (plan.column, file_rows.as_deref()) {
                self.sample_key_ranges(
                    &mut plan,
                    &store,
                    &paths,
                    rows,
                    &key_columns[column],
                    column,
                )
                .await;
            }
            let group_rows: Vec<u64> = plan
                .groups
                .iter()
                .map(|files| {
                    files
                        .iter()
                        .map(|&f| file_rows.as_deref().map_or(0, |rows| rows[f as usize]))
                        .sum()
                })
                .collect();
            let query = DuplicateQuery {
                ctx: &ctx,
                store: &store,
                paths: &paths,
                key_names: &key_names,
                stored: &stored,
                schema: &schema,
                survivor,
                keys: key_columns.len(),
                group_rows,
            };
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
    /// The rows of each cluster split into key ranges, by group: the ranges'
    /// rows must add up to it, or a row was read by no range.
    group_rows: Vec<u64>,
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
        let mut group_read: Vec<u64> = vec![0; self.group_rows.len()];
        for spec in specs {
            let rows_read = Arc::new(AtomicU64::new(0));
            let mut groups: Vec<Vec<ReadBackFile>> = vec![Vec::new(); partitions];
            for (slot, &id) in spec.files.iter().enumerate() {
                groups[slot % partitions].push((id, self.paths[id as usize].clone()));
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
                        range: spec.range.clone(),
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
            let aggregate_rows = rows_read.load(Ordering::Relaxed);
            if let Some(range) = &spec.range
                && let Some(read) = group_read.get_mut(range.group as usize)
            {
                *read += aggregate_rows;
            }
            let repeated_keys: usize = repeated_batches.iter().map(RecordBatch::num_rows).sum();
            if repeated_keys == 0 {
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
        }
        // Every row of a split cluster falls in exactly one of its key ranges; a
        // shortfall means a range missed rows, whose repeats would go unresolved.
        for (group, (read, expected)) in group_read.iter().zip(&self.group_rows).enumerate() {
            if read != expected {
                return Err(datafusion_common::DataFusionError::Internal(format!(
                    "the key ranges of cluster {group} read {read} of its {expected} rows"
                )));
            }
        }
        Ok(superseded)
    }
}

/// Rows sampled from each file to cut a cluster into key ranges.
const SAMPLE_ROWS_PER_FILE: u64 = 1024;

/// Cut points splitting the rows of `files` into about `slices` key ranges of
/// equal count: quantiles of `column` over evenly spaced rows of each file.
/// Sorted and distinct; `None` when the sample cannot be read or holds no
/// usable value.
async fn sample_cut_points(
    store: &Arc<dyn ObjectStore>,
    paths: &[String],
    files: &[u32],
    file_rows: &[u64],
    column: &str,
    slices: u64,
) -> Option<Vec<ScalarValue>> {
    use vortex::expr::{get_item, root};
    let session = VortexSession::default();
    let mut sampled: Vec<ArrayRef> = Vec::new();
    for &file in files {
        let rows = *file_rows.get(file as usize)?;
        if rows == 0 {
            continue;
        }
        let take = rows.min(SAMPLE_ROWS_PER_FILE);
        let indices: vortex::buffer::Buffer<u64> = (0..take).map(|i| i * rows / take).collect();
        let vxf = session
            .open_options()
            .open_object_store(store, &paths[file as usize])
            .await
            .ok()?;
        let mut chunks = vxf
            .scan()
            .ok()?
            .with_row_indices(indices)
            .with_projection(get_item(column, root()))
            .into_stream()
            .ok()?;
        while let Some(chunk) = chunks.next().await {
            let chunk = chunk.ok()?;
            if chunk.is_empty() {
                continue;
            }
            let mut ctx = session.create_execution_ctx();
            sampled.push(session.arrow().execute_arrow(chunk, None, &mut ctx).ok()?);
        }
    }
    let refs: Vec<&dyn arrow::array::Array> = sampled.iter().map(AsRef::as_ref).collect();
    let all = arrow::compute::concat(&refs).ok()?;
    let sorted = arrow::compute::sort(&all, None).ok()?;
    let len = sorted.len() as u64;
    if len == 0 {
        return None;
    }
    let mut cuts: Vec<ScalarValue> = Vec::new();
    for slice in 1..slices.min(len) {
        #[expect(clippy::cast_possible_truncation)]
        let at = (len * slice / slices) as usize;
        let cut = ScalarValue::try_from_array(&sorted, at).ok()?;
        if cut.is_null() {
            return None;
        }
        if cuts.last() != Some(&cut) {
            cuts.push(cut);
        }
    }
    // A literal must exist for every cut, or the scan could not filter by it.
    cuts.iter()
        .all(|cut| scalar_literal(cut).is_some())
        .then_some(cuts)
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

    fn int_bound(value: Option<&ScalarValue>) -> Option<i64> {
        match value {
            Some(ScalarValue::Int64(Some(v))) => Some(*v),
            None => None,
            other => panic!("unexpected bound {other:?}"),
        }
    }

    /// Key ranges are half-open and tile the whole domain: the first is
    /// unbounded below, the last unbounded above, and each starts where the
    /// one before ends, so every key — and every copy of it — falls in exactly
    /// one step.
    #[test]
    fn range_slices_tile_the_whole_domain() {
        let bounds = vec![
            (ScalarValue::Int64(Some(-7)), ScalarValue::Int64(Some(40))),
            (ScalarValue::Int64(Some(3)), ScalarValue::Int64(Some(92))),
        ];
        for slices in [1, 2, 3, 7, 200] {
            let specs = range_slices(&[0, 1], &bounds, 0, slices, 0).expect("integer bounds");
            let ranges: Vec<(Option<i64>, Option<i64>)> = specs
                .iter()
                .map(|spec| {
                    let range = spec.range.as_ref().expect("range");
                    (int_bound(range.lo.as_ref()), int_bound(range.hi.as_ref()))
                })
                .collect();
            assert_eq!(
                ranges.first().map(|r| r.0),
                Some(None),
                "{slices}: {ranges:?}"
            );
            assert_eq!(
                ranges.last().map(|r| r.1),
                Some(None),
                "{slices}: {ranges:?}"
            );
            for pair in ranges.windows(2) {
                assert_eq!(pair[0].1, pair[1].0, "{slices}: {ranges:?}");
                assert!(pair[0].1.is_some(), "{slices}: {ranges:?}");
            }
        }
        let strings = [(
            ScalarValue::Utf8(Some("a".into())),
            ScalarValue::Utf8(Some("z".into())),
        )];
        assert!(range_slices(&[0], &strings, 0, 4, 0).is_none());
    }

    #[test]
    fn cut_specs_tile_the_domain_at_the_cuts() {
        let cuts = [
            ScalarValue::Utf8(Some("g".into())),
            ScalarValue::Utf8(Some("q".into())),
        ];
        let specs = cut_specs(&[0, 1], 0, &cuts, 3);
        let ranges: Vec<_> = specs
            .iter()
            .map(|spec| {
                let range = spec.range.as_ref().expect("range");
                assert_eq!(range.group, 3);
                (range.lo.clone(), range.hi.clone())
            })
            .collect();
        assert_eq!(
            ranges,
            vec![
                (None, Some(cuts[0].clone())),
                (Some(cuts[0].clone()), Some(cuts[1].clone())),
                (Some(cuts[1].clone()), None),
            ]
        );
    }

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
        let plan = plan_chunks(&sizes, &bounds, 4, false);
        assert_eq!(plan.column, Some(0));
        assert_eq!(plan.bytes_read, 800);
        assert_eq!(plan.specs.len(), 4);
        let mut files: Vec<u32> = plan.specs.iter().flat_map(|s| s.files.clone()).collect();
        files.sort_unstable();
        assert_eq!(files, (0..8).collect::<Vec<_>>());
        assert!(plan.specs.iter().all(|spec| spec.hash == (1, 0)));
    }

    #[test]
    fn overlapping_integer_files_split_into_key_sub_ranges() {
        let sizes = vec![100; 4];
        let bounds = vec![Some(int_bounds(&[(0, 99), (0, 99), (0, 99), (0, 99)]))];
        let plan = plan_chunks(&sizes, &bounds, 3, true);
        assert_eq!(plan.bytes_read, 400, "each sub-range reads its share once");
        assert_eq!(plan.specs.len(), 3);
        assert!(
            plan.specs
                .iter()
                .all(|spec| spec.files.len() == 4 && spec.hash == (1, 0))
        );
        let ranges: Vec<(Option<i64>, Option<i64>)> = plan
            .specs
            .iter()
            .map(|spec| {
                let range = spec.range.as_ref().expect("range");
                (int_bound(range.lo.as_ref()), int_bound(range.hi.as_ref()))
            })
            .collect();
        assert_eq!(
            ranges,
            vec![(None, Some(33)), (Some(33), Some(66)), (Some(66), None)]
        );
        assert_eq!(plan.groups, vec![vec![0, 1, 2, 3]]);
    }

    #[test]
    fn overlapping_files_fall_back_to_hash_slices() {
        let sizes = vec![100; 4];
        let bounds = vec![Some(int_bounds(&[(0, 99), (0, 99), (0, 99), (0, 99)]))];
        let plan = plan_chunks(&sizes, &bounds, 3, false);
        assert_eq!(plan.bytes_read, 1200);
        assert_eq!(plan.specs.len(), 3);
        assert!(plan.specs.iter().all(|spec| spec.files.len() == 4));
        let hashes: Vec<(u64, u64)> = plan.specs.iter().map(|spec| spec.hash).collect();
        assert_eq!(hashes, vec![(3, 0), (3, 1), (3, 2)]);
    }

    #[test]
    fn missing_bounds_hash_every_file() {
        let plan = plan_chunks(&[100, 100], &[None], 2, false);
        assert_eq!(plan.column, None);
        assert_eq!(plan.bytes_read, 400);
    }
}
