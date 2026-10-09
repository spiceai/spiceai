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

//! Does a `mode: memory` table reject writes at its configured memory limit, or
//! well before it?
//!
//! The limit (`cayenne_cdc_mem_tier_max_bytes`) is a hard bound: memory mode never
//! spills, so a breach rejects the write with `MemTierLimitExceeded`. The resident
//! figure it is checked against has to be the memory the tier holds. Batches that
//! arrive over Arrow IPC — Flight, Flight SQL, a Spice-to-Spice source — carry one
//! allocation per message body with every column buffer pointing into it, and
//! `RecordBatch::get_array_memory_size` bills that allocation once per buffer.
//!
//! The same rows are loaded twice, once as builder-built batches (one allocation
//! per buffer) and once as IPC-decoded batches, into identically configured
//! tables, until each rejects a write. The assertion is that the table admits the
//! same data either way, and that what it reports as resident at rejection is
//! memory the process actually gave up.

#![cfg(not(windows))]
#![allow(clippy::expect_used, clippy::cast_precision_loss)]

#[expect(
    dead_code,
    reason = "shared helpers for every Cayenne test binary; this one uses only some"
)]
mod common;

use std::alloc::{GlobalAlloc, Layout, System};
use std::io::Cursor;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use cayenne::metadata::{CdcDurability, CreateTableOptions, VortexConfig};
use cayenne::{CayenneTableProvider, MetadataCatalog};
use datafusion::prelude::SessionContext;

struct CountingAllocator;

static LIVE_BYTES: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            LIVE_BYTES.fetch_add(layout.size(), Ordering::Relaxed);
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        LIVE_BYTES.fetch_sub(layout.size(), Ordering::Relaxed);
        unsafe { System.dealloc(ptr, layout) };
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let new_ptr = unsafe { System.realloc(ptr, layout, new_size) };
        if !new_ptr.is_null() {
            LIVE_BYTES.fetch_add(new_size, Ordering::Relaxed);
            LIVE_BYTES.fetch_sub(layout.size(), Ordering::Relaxed);
        }
        new_ptr
    }
}

#[global_allocator]
static ALLOC: CountingAllocator = CountingAllocator;

fn live_bytes() -> usize {
    LIVE_BYTES.load(Ordering::Relaxed)
}

/// Serializes the tests in this file.
///
/// `LIVE_BYTES` is a process-global counter, so it reports every thread's
/// allocations, not the measuring thread's. `cargo test` runs a file's tests
/// concurrently on one process, so a sibling test allocating inside a
/// measurement window lands in that window's figure — the scan test holds
/// megabytes of decoded batches while the memory test is reading what its
/// table retained, which is the memory test's whole measurement. Observed as a
/// nondeterministic failure of the IPC-decoded arm, reporting more resident
/// bytes than the polluted window said the process held. Every test here
/// takes this lock for its whole body so only one measurement window is ever
/// open. `parking_lot` does not poison, so a failing test still releases it and
/// its sibling reports its own result rather than a lock error.
static MEASUREMENT: parking_lot::Mutex<()> = parking_lot::Mutex::new(());

/// The configured memory limit of both tables.
const LIMIT_BYTES: u64 = 16 * 1024 * 1024;
const ROWS_PER_WRITE: usize = 2_000;
const STRING_WIDTH: usize = 32;
/// Far more writes than either table can admit, so every run ends in a rejection.
const MAX_WRITES: usize = 1_000;

/// Eleven columns, nineteen Arrow buffers per batch.
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
/// message body, every buffer of every column a slice of it.
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

#[derive(Debug)]
struct Outcome {
    admitted_rows: usize,
    /// Live heap growth across the admitted writes, taken after the last one.
    live_growth: u64,
    /// `resident_bytes` from the rejection.
    reported_resident: u64,
    rejection: String,
}

fn resident_from(message: &str) -> u64 {
    message
        .split("resident ")
        .nth(1)
        .and_then(|rest| rest.split(' ').next())
        .and_then(|n| n.parse().ok())
        .unwrap_or_else(|| panic!("no resident figure in the rejection: {message}"))
}

async fn fill_until_rejected(
    fixture: &common::TestFixture,
    name: &str,
    decode_through_ipc: bool,
) -> Result<Outcome, Box<dyn std::error::Error>> {
    let schema = wide_schema();
    let ctx = SessionContext::new();
    let table = CayenneTableProvider::create_table(
        Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>,
        CreateTableOptions {
            table_name: name.to_string(),
            schema: Arc::clone(&schema),
            primary_key: vec![],
            on_conflict: None,
            base_path: fixture.data_path.join(name).to_string_lossy().to_string(),
            partition_column: None,
            vortex_config: VortexConfig {
                memory_mode: true,
                cdc_mem_tier_shards: 1,
                cdc_mem_tier_max_age_ms: 0,
                cdc_mem_tier_checkpoint_interval_ms: 0,
                cdc_mem_tier_seal_age_ms: 0,
                compaction_background_interval_ms: 0,
                inline_max_rows: 0,
                inline_max_bytes: 0,
                inline_max_buffer_bytes: 0,
                cdc_mem_tier_max_bytes: i64::try_from(LIMIT_BYTES)?,
                cdc_durability: CdcDurability::Memory,
                ..VortexConfig::default()
            },
        },
        ctx.runtime_env(),
    )
    .await?;

    let live_before = live_bytes();
    let mut live_after_last_admit = live_before;
    for write in 0..MAX_WRITES {
        let first_id = i64::try_from(write * ROWS_PER_WRITE)?;
        let batch = rows(&schema, first_id);
        let batch = if decode_through_ipc {
            through_ipc(&batch)
        } else {
            batch
        };
        match common::insert_batch(&table, batch).await {
            Ok(_) => live_after_last_admit = live_bytes(),
            Err(e) => {
                let rejection = e.to_string();
                assert!(
                    rejection.contains("memory limit"),
                    "the write failed for a reason other than the memory limit: {rejection}"
                );
                return Ok(Outcome {
                    admitted_rows: write * ROWS_PER_WRITE,
                    live_growth: u64::try_from(live_after_last_admit.saturating_sub(live_before))?,
                    reported_resident: resident_from(&rejection),
                    rejection,
                });
            }
        }
    }
    Err(
        format!("{name}: {MAX_WRITES} writes were all admitted under a {LIMIT_BYTES} B limit")
            .into(),
    )
}

#[test]
fn a_memory_mode_table_admits_the_same_rows_whatever_allocation_they_arrive_in()
-> Result<(), String> {
    let _measurement = MEASUREMENT.lock();
    common::run_with_backend_blocking(common::BackendType::Sqlite, |fixture| async move {
        // Warm the write path so its one-off allocations sit under the baselines.
        drop(fill_until_rejected(&fixture, "warmup", false).await?);

        let built = fill_until_rejected(&fixture, "built", false).await?;
        let decoded = fill_until_rejected(&fixture, "decoded", true).await?;

        for (arm, o) in [("builder-built", &built), ("IPC-decoded", &decoded)] {
            println!(
                "{arm}: admitted {} rows; the process retained {} B; the rejection reported \
                 resident {} B ({:.2}x); {}",
                o.admitted_rows,
                o.live_growth,
                o.reported_resident,
                o.reported_resident as f64 / o.live_growth.max(1) as f64,
                o.rejection
            );
        }

        assert!(
            decoded.reported_resident <= decoded.live_growth,
            "the IPC-decoded table reported {} B resident when the process had retained only \
             {} B for it — the limit is being charged for one allocation once per buffer",
            decoded.reported_resident,
            decoded.live_growth
        );
        assert!(
            decoded.admitted_rows * 10 >= built.admitted_rows * 9,
            "the same {LIMIT_BYTES} B limit admitted {} builder-built rows but only {} \
             IPC-decoded rows",
            built.admitted_rows,
            decoded.admitted_rows
        );
        Ok::<(), Box<dyn std::error::Error>>(())
    })
}

/// The scan's memory-pool charge settles to each decoded batch's
/// `get_array_memory_size`. That is the truth only if the batches Vortex decodes
/// into do not share allocations across buffers; this measures whether they do,
/// over the batches a file-backed scan actually yields.
#[test]
fn a_file_scan_yields_batches_whose_buffers_do_not_share_allocations() -> Result<(), String> {
    let _measurement = MEASUREMENT.lock();
    common::run_with_backend_blocking(common::BackendType::Sqlite, |fixture| async move {
        let schema = wide_schema();
        let ctx = SessionContext::new();
        let table = Arc::new(
            CayenneTableProvider::create_table(
                Arc::clone(&fixture.catalog) as Arc<dyn MetadataCatalog>,
                CreateTableOptions {
                    table_name: "scanned".to_string(),
                    schema: Arc::clone(&schema),
                    primary_key: vec![],
                    on_conflict: None,
                    base_path: fixture.data_path.to_string_lossy().to_string(),
                    partition_column: None,
                    // Every write to a Vortex file, so the scan decodes.
                    vortex_config: VortexConfig {
                        inline_max_rows: 0,
                        ..VortexConfig::default()
                    },
                },
                ctx.runtime_env(),
            )
            .await?,
        );
        ctx.register_table(
            "scanned",
            Arc::clone(&table) as Arc<dyn datafusion::datasource::TableProvider>,
        )?;
        for write in 0..8 {
            common::insert_batch(
                &table,
                rows(&schema, i64::try_from(write * ROWS_PER_WRITE)?),
            )
            .await?;
        }

        let scanned = ctx.sql("SELECT * FROM scanned").await?.collect().await?;
        let per_reference: usize = scanned.iter().map(RecordBatch::get_array_memory_size).sum();
        let mut per_allocation = 0usize;
        for batch in &scanned {
            per_allocation += deduped_bytes(batch);
        }
        let scanned_rows: usize = scanned.iter().map(RecordBatch::num_rows).sum();
        println!(
            "file scan: {} batches, {scanned_rows} rows; get_array_memory_size {per_reference} B, \
             per distinct allocation {per_allocation} B ({:.2}x)",
            scanned.len(),
            per_reference as f64 / per_allocation.max(1) as f64
        );
        assert_eq!(scanned_rows, 8 * ROWS_PER_WRITE);
        assert!(
            per_reference <= per_allocation * 3 / 2,
            "decoded scan batches share allocations across buffers: the scan charge settles \
             to {per_reference} B for batches holding {per_allocation} B"
        );
        Ok::<(), Box<dyn std::error::Error>>(())
    })
}

/// Per-batch `get_array_memory_size` with each distinct allocation counted once
/// (buffer capacity plus the same Arrow bookkeeping it adds).
fn deduped_bytes(batch: &RecordBatch) -> usize {
    use arrow::array::{Array, ArrayData};
    fn walk(data: &ArrayData, seen: &mut std::collections::HashSet<usize>, total: &mut usize) {
        let buffers = data
            .buffers()
            .iter()
            .chain(data.nulls().map(arrow::buffer::NullBuffer::buffer));
        for buffer in buffers {
            if seen.insert(buffer.data_ptr().as_ptr() as usize) {
                *total += buffer.capacity();
            }
        }
        for child in data.child_data() {
            walk(child, seen, total);
        }
    }
    let mut seen = std::collections::HashSet::new();
    let mut total = 0;
    for column in batch.columns() {
        total += column.get_array_memory_size() - column.get_buffer_memory_size();
        walk(&column.to_data(), &mut seen, &mut total);
    }
    total
}
