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

//! How many Vortex files does a checkpoint of a flush smaller than one target
//! file write?
//!
//! A checkpoint sizes its encode fan-out from the bytes it is about to write:
//! `snapshot_shard_count` divides that figure by the encode-shard unit, so a
//! flush below one unit stays a SINGLE file and anything above it fans out to
//! `ceil(bytes / unit)` files, capped at `cayenne_write_concurrency`. The figure
//! therefore has to be the memory the batches hold.
//!
//! Both corpora a checkpoint flushes arrive as Arrow IPC — the inline memtable
//! is decoded out of the metastore's blobs, and the RAM CDC tier retains the
//! batches an apply handed it, which over Flight or Flight SQL come off the
//! wire. An IPC reader decodes a message body into one allocation and points
//! every column and child buffer at a slice of it, so summing
//! `RecordBatch::get_array_memory_size` bills that one allocation once per
//! buffer: on the nineteen-buffer schema here, ~19x. A sub-target flush then
//! mints `write_concurrency` files per checkpoint instead of one, and every one
//! of them is a file compaction has to fold back.
//!
//! Each test writes ~4,000 rows — well under the 1 MiB target file size
//! configured below — and asserts the checkpoint lands them as one file per
//! encoded unit. Every run prints both byte figures for the same rows, so the
//! output documents the ratio whichever way the assertions go.

#![allow(clippy::expect_used, clippy::cast_precision_loss)]

mod common;

use std::collections::HashSet;
use std::io::Cursor;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{Array, ArrayData, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use cayenne::metadata::{CdcDurability, CreateTableOptions, DeletionMode, VortexConfig};
use cayenne::{CayenneTableProvider, MetadataCatalog, SlotAdvancer};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::prelude::SessionContext;
use datafusion_table_providers::util::{
    column_reference::ColumnReference, on_conflict::OnConflict,
};

/// Target Vortex file size. The encode-shard unit is `target / 16` floored at
/// 16 MiB and capped at the target, so at 1 MiB the unit IS 1 MiB: a flush of
/// under a megabyte is one file, and each further megabyte the estimate claims
/// is another.
const TARGET_FILE_SIZE_MB: usize = 1;
/// The fan-out ceiling, pinned so the pre-fix count does not depend on the
/// host's core count.
const WRITE_CONCURRENCY: usize = 8;
const WRITES: usize = 8;
const ROWS_PER_WRITE: usize = 500;
const STRING_WIDTH: usize = 32;

struct NoopSlotAdvancer;
#[async_trait::async_trait]
impl SlotAdvancer for NoopSlotAdvancer {
    async fn on_checkpoint_durable(&self, _durable_epoch: u64) {}
}

/// Eleven columns, nineteen Arrow buffers per batch: one values buffer for each
/// non-nullable `Int64`, a null buffer beside it for each nullable one, and
/// offsets + values for each `Utf8`.
fn wide_schema() -> Arc<Schema> {
    let mut fields = vec![Field::new("id", DataType::Int64, false)];
    for i in 0..2 {
        fields.push(Field::new(format!("plain_{i}"), DataType::Int64, false));
    }
    for i in 0..4 {
        fields.push(Field::new(format!("nullable_{i}"), DataType::Int64, true));
    }
    for i in 0..4 {
        fields.push(Field::new(format!("text_{i}"), DataType::Utf8, false));
    }
    Arc::new(Schema::new(fields))
}

fn rows(schema: &Arc<Schema>, first_id: i64) -> RecordBatch {
    let count = i64::try_from(ROWS_PER_WRITE).expect("the row count fits in i64");
    let ids: Vec<i64> = (first_id..first_id + count).collect();
    let mut columns: Vec<arrow::array::ArrayRef> = vec![Arc::new(Int64Array::from(ids.clone()))];
    for i in 0..2_i64 {
        columns.push(Arc::new(Int64Array::from(
            ids.iter().map(|id| id * (i + 2)).collect::<Vec<_>>(),
        )));
    }
    for i in 0..4_i64 {
        columns.push(Arc::new(Int64Array::from(
            ids.iter()
                .map(|id| (id % (i + 2) == 0).then_some(*id))
                .collect::<Vec<_>>(),
        )));
    }
    let width = STRING_WIDTH;
    for _ in 0..4 {
        columns.push(Arc::new(StringArray::from(
            ids.iter()
                .map(|id| format!("{id:0>width$}"))
                .collect::<Vec<_>>(),
        )));
    }
    RecordBatch::try_new(Arc::clone(schema), columns).expect("the fixture rows should build")
}

/// `batch` as a reader of an Arrow IPC stream hands it back: one allocation per
/// message body, every buffer of every column a slice of it — the shape a batch
/// arrives in over Flight, Flight SQL or a metastore inline blob.
fn through_ipc(batch: &RecordBatch) -> RecordBatch {
    let mut encoded = Vec::new();
    {
        let mut writer =
            StreamWriter::try_new(&mut encoded, batch.schema_ref()).expect("IPC writer");
        writer.write(batch).expect("IPC write");
        writer.finish().expect("IPC finish");
    }
    let mut reader = StreamReader::try_new(Cursor::new(encoded), None).expect("IPC reader");
    reader
        .next()
        .expect("one batch in the stream")
        .expect("the batch should decode")
}

/// Buffer capacity of every distinct allocation `batches` reach, counting one
/// an IPC body shares across its buffers once — the figure the checkpoint sites
/// under test compute with `RetainedBytes`, reproduced here so a run can print
/// it beside the per-reference sum.
fn deduped_bytes(batches: &[RecordBatch]) -> usize {
    fn walk(data: &ArrayData, seen: &mut HashSet<usize>, total: &mut usize) {
        let buffers = data
            .buffers()
            .iter()
            .chain(data.nulls().map(arrow::buffer::NullBuffer::buffer));
        for buffer in buffers {
            if seen.insert(buffer.data_ptr().addr().get()) {
                *total += buffer.capacity();
            }
        }
        for child in data.child_data() {
            walk(child, seen, total);
        }
    }
    let mut seen = HashSet::new();
    let mut total = 0;
    for batch in batches {
        for column in batch.columns() {
            walk(&column.to_data(), &mut seen, &mut total);
        }
    }
    total
}

fn per_reference_bytes(batches: &[RecordBatch]) -> usize {
    batches.iter().map(RecordBatch::get_array_memory_size).sum()
}

/// Print both byte figures for the rows a test is about to flush, and assert the
/// fixture still carries the shape the test exists to cover: if the IPC decode
/// stopped sharing one allocation across a batch's buffers, an unfixed
/// checkpoint would size itself correctly and the assertions below would pass
/// for the wrong reason.
fn report_shape(label: &str, batches: &[RecordBatch]) {
    let deduped = deduped_bytes(batches);
    let per_reference = per_reference_bytes(batches);
    println!(
        "{label}: {} batches, {} rows; resident {deduped} B, get_array_memory_size \
         {per_reference} B ({:.1}x)",
        batches.len(),
        batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
        per_reference as f64 / deduped.max(1) as f64
    );
    let target = TARGET_FILE_SIZE_MB * 1024 * 1024;
    assert!(
        deduped < target,
        "{label}: the fixture no longer fits in one target file ({deduped} B of {target} B), so \
         a correctly-sized checkpoint would fan out too and this test covers nothing"
    );
    assert!(
        per_reference > target * WRITE_CONCURRENCY,
        "{label}: the fixture's buffers no longer share allocations ({per_reference} B \
         per-reference for {deduped} B resident), so the per-reference sum would no longer \
         over-shard and this test covers nothing"
    );
}

/// Count `.vortex` data files anywhere under `dir`. Each test writes into its
/// own base path and checkpoints exactly once, so everything found is that
/// checkpoint's output, wherever the commit chose to put it (the current
/// snapshot dir, or a fresh one).
async fn count_vortex_files(dir: &Path) -> usize {
    let mut pending: Vec<PathBuf> = vec![dir.to_path_buf()];
    let mut count = 0;
    while let Some(next) = pending.pop() {
        let mut entries = match tokio::fs::read_dir(&next).await {
            Ok(entries) => entries,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(e) => panic!("read_dir {} failed: {e}", next.display()),
        };
        while let Some(entry) = entries.next_entry().await.expect("read_dir") {
            let path = entry.path();
            let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
                continue;
            };
            if name.starts_with('.') {
                continue;
            }
            if entry.file_type().await.expect("file_type").is_dir() {
                pending.push(path);
            } else if name.ends_with(".vortex") {
                count += 1;
            }
        }
    }
    count
}

/// Size-rolling is on at 1 MiB and the fan-out ceiling is pinned, so the file
/// count a checkpoint produces is decided by the byte figure it estimates.
/// Compaction is held off entirely — it would fold the extra files away and
/// hide exactly what is being measured.
fn sizing_config() -> VortexConfig {
    VortexConfig {
        target_vortex_file_size_mb: TARGET_FILE_SIZE_MB,
        write_concurrency: Some(WRITE_CONCURRENCY),
        compaction_background_interval_ms: 0,
        compaction_trigger_files: 1_000_000,
        compaction_trigger_protected_snapshots: 1_000_000,
        compaction_trigger_snapshot_age_ms: 0,
        ..VortexConfig::default()
    }
}

fn batch_to_stream(batch: RecordBatch) -> SendableRecordBatchStream {
    let schema = batch.schema();
    Box::pin(RecordBatchStreamAdapter::new(
        schema,
        futures::stream::iter([Ok(batch)]),
    ))
}

/// The inline memtable's corpus lives in the metastore as Arrow IPC blobs and is
/// decoded on read, so its batches carry the shared-allocation shape without
/// anything in the test arranging it.
#[test]
fn an_inline_checkpoint_writes_one_file_for_a_flush_under_the_target_size() -> Result<(), String> {
    common::run_with_backend_blocking(common::BackendType::Sqlite, |fixture| async move {
        let schema = wide_schema();
        let ctx = SessionContext::new();
        let base_path = fixture.data_path.join("inline");
        let table = CayenneTableProvider::create_table(
            Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>,
            CreateTableOptions {
                table_name: "inline".to_string(),
                schema: Arc::clone(&schema),
                primary_key: vec![],
                on_conflict: None,
                base_path: base_path.to_string_lossy().to_string(),
                partition_column: None,
                vortex_config: VortexConfig {
                    // Admit every write to the metastore, and never flush it
                    // there: the test drives the checkpoint itself.
                    inline_max_rows: ROWS_PER_WRITE * 2,
                    inline_max_bytes: 64 * 1024 * 1024,
                    inline_max_buffer_bytes: 64 * 1024 * 1024,
                    inline_flush_max_rows: 1_000_000,
                    inline_flush_max_segments: 1_000_000,
                    inline_flush_max_bytes: 1 << 40,
                    ..sizing_config()
                },
            },
            ctx.runtime_env(),
        )
        .await?;

        let mut written = Vec::with_capacity(WRITES);
        for write in 0..WRITES {
            let batch = rows(&schema, i64::try_from(write * ROWS_PER_WRITE)?);
            written.push(through_ipc(&batch));
            common::insert_batch(&table, batch).await?;
        }
        // Every insert commits its inline entry before it returns, so the corpus
        // is complete here without waiting on anything.
        let inlined_rows = fixture
            .catalog
            .get_inlined_data_count(table.table_id())
            .await?;
        assert_eq!(
            inlined_rows,
            i64::try_from(WRITES * ROWS_PER_WRITE)?,
            "the writes did not all land in the inline memtable, so the checkpoint under test \
             has nothing to size"
        );
        assert_eq!(
            count_vortex_files(&base_path).await,
            0,
            "a write reached Vortex before the checkpoint, so the file count below would not be \
             the checkpoint's"
        );

        // The corpus the checkpoint decodes is IPC-decoded like `written`, which
        // is the same rows through the same reader.
        report_shape("inline corpus", &written);
        let flushed = table.checkpoint_inlined_data().await?;
        assert_eq!(flushed, u64::try_from(WRITES * ROWS_PER_WRITE)?);

        let files = count_vortex_files(&base_path).await;
        println!("inline checkpoint: {files} vortex file(s)");
        assert_eq!(
            files, 1,
            "the inline checkpoint split a flush smaller than one {TARGET_FILE_SIZE_MB} MiB \
             target file into {files} files — it is sizing the write by the per-reference Arrow \
             sum, which bills each IPC body once per buffer"
        );
        Ok::<(), Box<dyn std::error::Error>>(())
    })
}

/// Build the `cdc_durability: memory` upsert table both mem-tier tests drive.
/// `inline` decides whether ordinary appends are admitted to the metastore's
/// inline memtable beside the RAM tier.
async fn memory_cdc_table(
    fixture: &common::TestFixture,
    name: &str,
    base_path: &Path,
    shards: usize,
    inline: bool,
) -> Result<Arc<CayenneTableProvider>, Box<dyn std::error::Error>> {
    let ctx = SessionContext::new();
    let table = Arc::new(
        CayenneTableProvider::create_table(
            Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>,
            CreateTableOptions {
                table_name: name.to_string(),
                schema: wide_schema(),
                primary_key: vec!["id".to_string()],
                on_conflict: Some(OnConflict::Upsert(ColumnReference::new(vec![
                    "id".to_string(),
                ]))),
                base_path: base_path.to_string_lossy().to_string(),
                partition_column: None,
                vortex_config: VortexConfig {
                    // The RAM tier, not a durable staged write, holds the applies.
                    cdc_durability: CdcDurability::Memory,
                    deletion_mode: DeletionMode::Key,
                    cdc_mem_tier_shards: shards,
                    // Every automatic trigger off, so the only checkpoint is the
                    // one the test drives.
                    cdc_mem_tier_max_age_ms: 0,
                    cdc_mem_tier_checkpoint_interval_ms: 0,
                    cdc_mem_tier_seal_age_ms: 0,
                    cdc_mem_tier_max_bytes: 0,
                    inline_max_rows: if inline { ROWS_PER_WRITE * 2 } else { 0 },
                    inline_max_bytes: if inline { 64 * 1024 * 1024 } else { 0 },
                    inline_max_buffer_bytes: if inline { 64 * 1024 * 1024 } else { 0 },
                    inline_flush_max_rows: 1_000_000,
                    inline_flush_max_segments: 1_000_000,
                    inline_flush_max_bytes: 1 << 40,
                    ..sizing_config()
                },
            },
            ctx.runtime_env(),
        )
        .await?,
    );
    table.install_slot_advancer(Arc::new(NoopSlotAdvancer));
    Ok(table)
}

/// CDC-apply `batch` through the RAM tier.
async fn cdc_apply(
    table: &Arc<CayenneTableProvider>,
    batch: RecordBatch,
) -> Result<(), Box<dyn std::error::Error>> {
    let ctx = SessionContext::new();
    let write = table
        .write_cdc_append_stream(batch_to_stream(batch), &ctx.task_ctx())
        .await?;
    if write.has_pending_finalize() {
        write.finish().await?;
    }
    Ok(())
}

/// The RAM CDC tier retains the batches an apply hands it, so a table fed over
/// Flight or Flight SQL checkpoints exactly the allocation sharing that decode
/// produced. At one shard the checkpoint encodes the captured corpus as a single
/// merged stream sized by `estimated_flushed_bytes`.
#[test]
fn a_mem_tier_checkpoint_writes_one_file_for_a_flush_under_the_target_size() -> Result<(), String> {
    common::run_with_backend_blocking(common::BackendType::Sqlite, |fixture| async move {
        let schema = wide_schema();
        let base_path = fixture.data_path.join("mem_tier");
        let table = memory_cdc_table(&fixture, "mem_tier", &base_path, 1, false).await?;

        let mut applied = Vec::with_capacity(WRITES);
        for write in 0..WRITES {
            let batch = through_ipc(&rows(&schema, i64::try_from(write * ROWS_PER_WRITE)?));
            applied.push(batch.clone());
            cdc_apply(&table, batch).await?;
        }
        assert_eq!(
            count_vortex_files(&base_path).await,
            0,
            "an apply reached Vortex before the checkpoint, so the file count below would not be \
             the checkpoint's"
        );

        report_shape("mem tier", &applied);
        let flushed = table.checkpoint_mem_tier().await?;
        assert_eq!(flushed, u64::try_from(WRITES * ROWS_PER_WRITE)?);

        let files = count_vortex_files(&base_path).await;
        println!("mem-tier checkpoint: {files} vortex file(s)");
        assert_eq!(
            files, 1,
            "the mem-tier checkpoint split a flush smaller than one {TARGET_FILE_SIZE_MB} MiB \
             target file into {files} files — it is sizing the write by the per-reference Arrow \
             sum, which bills each IPC body once per buffer"
        );
        Ok::<(), Box<dyn std::error::Error>>(())
    })
}

/// Above one shard the checkpoint encodes each captured unit on its own task,
/// sizing it from `unit_bytes`. A PK-sharded unit is rebuilt by the shard split
/// (`filter_record_batch` allocates), so it never shares allocations and the
/// over-count cannot reach it — but the inline corpus rides that same fan-out as
/// one more unit, and it comes straight out of the metastore's IPC blobs. One
/// file per unit is what the path is for.
#[test]
fn a_sharded_mem_tier_checkpoint_writes_one_file_per_encoded_unit() -> Result<(), String> {
    const SHARDS: usize = 2;
    common::run_with_backend_blocking(common::BackendType::Sqlite, |fixture| async move {
        let schema = wide_schema();
        let base_path = fixture.data_path.join("sharded");
        let table = memory_cdc_table(&fixture, "sharded", &base_path, SHARDS, true).await?;

        // Ordinary appends inline into the metastore; CDC applies land in the
        // RAM tier. A checkpoint flushes both, as one unit per PK shard plus one
        // for the inline corpus.
        let mut inlined = Vec::with_capacity(WRITES);
        for write in 0..WRITES {
            let batch = rows(&schema, i64::try_from(write * ROWS_PER_WRITE)?);
            inlined.push(through_ipc(&batch));
            common::insert_batch(&table, batch).await?;
        }
        assert_eq!(
            fixture
                .catalog
                .get_inlined_data_count(table.table_id())
                .await?,
            i64::try_from(WRITES * ROWS_PER_WRITE)?,
            "the appends did not all land in the inline memtable, so the unit under test is \
             missing from the checkpoint"
        );
        for write in WRITES..2 * WRITES {
            cdc_apply(
                &table,
                through_ipc(&rows(&schema, i64::try_from(write * ROWS_PER_WRITE)?)),
            )
            .await?;
        }
        assert_eq!(
            count_vortex_files(&base_path).await,
            0,
            "a write reached Vortex before the checkpoint, so the file count below would not be \
             the checkpoint's"
        );

        report_shape("inline unit", &inlined);
        // The count returned is the mem tier's rows; the inline corpus rides the
        // same checkpoint but is not counted in it.
        let flushed = table.checkpoint_mem_tier().await?;
        assert_eq!(flushed, u64::try_from(WRITES * ROWS_PER_WRITE)?);

        let files = count_vortex_files(&base_path).await;
        println!("sharded checkpoint ({SHARDS} shard(s) + inline): {files} vortex file(s)");
        assert_eq!(
            files,
            SHARDS + 1,
            "the sharded checkpoint wrote {files} files for {SHARDS} shards and one inline \
             corpus, each smaller than one {TARGET_FILE_SIZE_MB} MiB target file — it is sizing \
             a unit by the per-reference Arrow sum, which bills each IPC body once per buffer"
        );
        Ok::<(), Box<dyn std::error::Error>>(())
    })
}
