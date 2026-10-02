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
//! The query reads only the key columns, and runs in chunks of the key space
//! sized from the bytes written, so its hash tables hold one chunk's keys at a
//! time. A chunk of written bytes expands in the query — each row read back
//! carries its key, ordinal, position, file and key hash, and its key's
//! aggregate state — so a chunk's memory is a small multiple of its bytes; a
//! chunk that still outgrows the memory pool cuts the key space finer.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::{Arc, LazyLock};
use std::task::{Context, Poll};

use arrow::array::{ArrayRef, AsArray, BooleanArray, RecordBatch, UInt32Array, UInt64Array};
use arrow::compute::filter_record_batch;
use arrow::datatypes::{DataType, Field, FieldRef, Schema, SchemaRef, UInt32Type, UInt64Type};
use datafusion::catalog::streaming::StreamingTable;
use datafusion::common::{Column, JoinType};
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
}

impl ArrivalStream {
    pub(crate) fn new(input: SendableRecordBatchStream, resolver: KeyResolver) -> Self {
        let schema = with_arrival(&input.schema());
        Self {
            input,
            resolver,
            schema,
            next: 0,
        }
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

/// Reads one group of written files back: each row's key columns, arrival
/// ordinal, row position and file, keeping only the rows whose key falls in one
/// chunk of the key space.
#[derive(Debug)]
struct ReadBack {
    store: Arc<dyn ObjectStore>,
    /// `(file id, path)` of each file this partition reads.
    files: Vec<(u32, String)>,
    key_names: Arc<[String]>,
    /// The key columns as stored, then the arrival column.
    stored: Arc<Field>,
    schema: SchemaRef,
    chunk: (u64, u64),
}

impl ReadBack {
    fn batch(
        &self,
        file: u32,
        stored: &RecordBatch,
    ) -> datafusion_common::Result<Option<RecordBatch>> {
        let keys = self.key_names.len();
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
            .then(move |(file, path)| {
                let this = Arc::clone(&this);
                let projection = projection.clone();
                async move {
                    let session = VortexSession::default();
                    let vxf = session
                        .open_options()
                        .open_object_store(&this.store, &path)
                        .await
                        .map_err(|e| datafusion_common::DataFusionError::External(Box::new(e)))?;
                    let chunks = vxf
                        .scan()
                        .map_err(|e| datafusion_common::DataFusionError::External(Box::new(e)))?
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
        let mut chunks = total_bytes.div_ceil(chunk_bytes()).max(1);
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
        };
        // A chunk sized from the bytes written can still outgrow a small memory
        // pool; then the key space is cut finer and the query run again.
        let superseded = loop {
            match query.run(chunks).await {
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
}

impl DuplicateQuery<'_> {
    /// The positions of every superseded copy, by file id, querying the key
    /// space in `chunks` chunks.
    async fn run(&self, chunks: u64) -> datafusion_common::Result<Vec<Vec<u32>>> {
        let ctx = self.ctx;
        let survivor = self.survivor;
        let schema = self.schema;
        let partitions = ctx.state().config().target_partitions().max(1);
        // Positions of superseded copies, by file id; each copy is emitted once,
        // since the join's build side holds each repeated key once.
        let mut superseded: Vec<Vec<u32>> = vec![Vec::new(); self.paths.len()];
        for chunk in 0..chunks {
            let mut groups: Vec<Vec<(u32, String)>> = vec![Vec::new(); partitions];
            for (index, path) in self.paths.iter().enumerate() {
                let id = u32::try_from(index).map_err(|_| {
                    datafusion_common::DataFusionError::Internal(
                        "too many files to resolve repeated keys".to_string(),
                    )
                })?;
                groups[index % partitions].push((id, path.clone()));
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
                        chunk: (chunks, chunk),
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
            if repeated_batches.iter().all(|batch| batch.num_rows() == 0) {
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
}
