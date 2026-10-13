/*
Copyright 2024-2026 The Spice.ai OSS Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

//! The footer cache evicts on the size each entry reports, so an entry that
//! reports less than it retains lets the cache hold a multiple of its limit.
//! This measures both sides through the real population and scan path: a
//! counting allocator gives the heap the cache actually frees when cleared,
//! and `list_entries` gives what the cache believed it held.
//!
//! It is its own integration binary because it installs a global allocator,
//! and it is one test because the counter is process-wide.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::Arc;
use std::sync::atomic::{AtomicIsize, Ordering};

use datafusion::arrow::array::{
    ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float64Array, Int32Array,
    Int64Array, ListArray, RecordBatch, StringArray, StructArray, TimestampMicrosecondArray,
};
use datafusion::arrow::datatypes::{DataType, Field, Fields, Int64Type, Schema, TimeUnit};
use datafusion::datasource::provider::DefaultTableFactory;
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::SessionContext;
use datafusion_common::GetExt;
use futures::StreamExt;
use object_store::ObjectStore;
use object_store::local::LocalFileSystem;
use url::Url;
use vortex::VortexSessionDefault;
use vortex::array::ArrayRef as VortexArrayRef;
use vortex::array::stream::ArrayStreamAdapter;
use vortex::arrow::ArrowSessionExt;
use vortex::file::WriteOptionsSessionExt;
use vortex::io::VortexWrite;
use vortex::io::object_store::ObjectStoreWrite;
use vortex::session::VortexSession;
use vortex_datafusion::VortexFormatFactory;

struct Counting;

static LIVE: AtomicIsize = AtomicIsize::new(0);

/// Adds `delta` to the live-byte count when the allocation succeeded.
fn track(ptr: *mut u8, delta: isize) -> *mut u8 {
    if !ptr.is_null() {
        LIVE.fetch_add(delta, Ordering::Relaxed);
    }
    ptr
}

// SAFETY: delegates every call to `System` and only adds bookkeeping.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: forwarded unchanged.
        track(unsafe { System.alloc(layout) }, layout.size().cast_signed())
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: forwarded unchanged.
        unsafe { System.dealloc(ptr, layout) };
        LIVE.fetch_sub(layout.size().cast_signed(), Ordering::Relaxed);
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        // SAFETY: forwarded unchanged.
        track(
            unsafe { System.alloc_zeroed(layout) },
            layout.size().cast_signed(),
        )
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        // SAFETY: forwarded unchanged.
        track(
            unsafe { System.realloc(ptr, layout, new_size) },
            new_size.cast_signed() - layout.size().cast_signed(),
        )
    }
}

#[global_allocator]
static GLOBAL: Counting = Counting;

const FILES: usize = 8;
const ROWS_PER_CHUNK: usize = 2_048;
const CHUNKS: usize = 4;

/// Which columns a file carries. The per-column cost of a footer depends on
/// the column's type — a struct or list column owns a layout subtree per
/// child — so the estimate is held to both shapes.
#[derive(Clone, Copy, Debug)]
enum Shape {
    /// `i64`, `f64` and `utf8` columns, repeated.
    Flat { groups: usize },
    /// Boolean, `i32`, decimal, timestamp, date, binary, list and struct
    /// columns, repeated.
    Mixed { groups: usize },
}

impl Shape {
    fn schema(self) -> Arc<Schema> {
        let mut fields = Vec::new();
        match self {
            Shape::Flat { groups } => {
                for g in 0..groups {
                    fields.push(Field::new(format!("i{g}"), DataType::Int64, true));
                    fields.push(Field::new(format!("f{g}"), DataType::Float64, true));
                    fields.push(Field::new(format!("s{g}"), DataType::Utf8, true));
                }
            }
            Shape::Mixed { groups } => {
                for g in 0..groups {
                    fields.push(Field::new(format!("b{g}"), DataType::Boolean, true));
                    fields.push(Field::new(format!("n{g}"), DataType::Int32, true));
                    fields.push(Field::new(
                        format!("d{g}"),
                        DataType::Decimal128(18, 4),
                        true,
                    ));
                    fields.push(Field::new(
                        format!("t{g}"),
                        DataType::Timestamp(TimeUnit::Microsecond, None),
                        true,
                    ));
                    fields.push(Field::new(format!("y{g}"), DataType::Date32, true));
                    fields.push(Field::new(format!("x{g}"), DataType::Binary, true));
                    fields.push(Field::new(
                        format!("l{g}"),
                        DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
                        true,
                    ));
                    fields.push(Field::new(
                        format!("st{g}"),
                        DataType::Struct(struct_fields()),
                        true,
                    ));
                }
            }
        }
        Arc::new(Schema::new(fields))
    }

    /// A predicate on every column that every row satisfies: the scan still
    /// returns everything, and evaluating it against each column's zone map
    /// expands the cached layouts as far as a query can.
    fn predicate(self) -> String {
        let clauses: Vec<String> = match self {
            Shape::Flat { groups } => (0..groups)
                .map(|g| format!("i{g} >= 0 AND f{g} >= 0 AND s{g} <> 'x'"))
                .collect(),
            Shape::Mixed { groups } => (0..groups)
                .map(|g| {
                    format!(
                        "b{g} IS NOT NULL AND n{g} >= 0 AND d{g} >= 0 AND t{g} IS NOT NULL \
                         AND y{g} IS NOT NULL AND x{g} IS NOT NULL AND l{g} IS NOT NULL \
                         AND st{g} IS NOT NULL"
                    )
                })
                .collect(),
        };
        clauses.join(" AND ")
    }

    fn batch(self, schema: &Arc<Schema>, file: usize, chunk: usize) -> RecordBatch {
        let base = (file * CHUNKS + chunk) * ROWS_PER_CHUNK;
        let rows = || (0..ROWS_PER_CHUNK).map(move |r| base + r);
        let int = |v: usize| i64::try_from(v).expect("row index fits i64");
        let mut columns: Vec<ArrayRef> = Vec::new();
        match self {
            Shape::Flat { groups } => {
                for g in 0..groups {
                    columns.push(Arc::new(Int64Array::from_iter_values(
                        rows().map(|r| int(r * (g + 1))),
                    )));
                    columns.push(Arc::new(Float64Array::from_iter_values(
                        rows().map(|r| f64::from(u32::try_from(r).expect("fits u32")) * 0.5),
                    )));
                    columns.push(Arc::new(StringArray::from_iter_values(
                        rows().map(|r| format!("value-{g}-{}", r % 97)),
                    )));
                }
            }
            Shape::Mixed { groups } => {
                for g in 0..groups {
                    columns.push(Arc::new(
                        rows().map(|r| Some(r % 3 == 0)).collect::<BooleanArray>(),
                    ));
                    columns.push(Arc::new(Int32Array::from_iter_values(
                        rows().map(|r| i32::try_from(r).expect("fits i32")),
                    )));
                    columns.push(Arc::new(
                        Decimal128Array::from_iter_values(rows().map(|r| i128::from(int(r)) * 7))
                            .with_precision_and_scale(18, 4)
                            .expect("valid decimal"),
                    ));
                    columns.push(Arc::new(TimestampMicrosecondArray::from_iter_values(
                        rows().map(|r| int(r) * 1_000),
                    )));
                    columns.push(Arc::new(Date32Array::from_iter_values(
                        rows().map(|r| i32::try_from(r % 400).expect("fits i32")),
                    )));
                    columns.push(Arc::new(BinaryArray::from_iter_values(
                        rows().map(|r| format!("bin{}", r % 13)),
                    )));
                    columns.push(Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>(
                        rows().map(|r| Some(vec![Some(int(r)), Some(int(g))])),
                    )));
                    let a: ArrayRef =
                        Arc::new(Int64Array::from_iter_values(rows().map(|r| int(r * g))));
                    let b: ArrayRef = Arc::new(StringArray::from_iter_values(
                        rows().map(|r| format!("s{}", r % 11)),
                    ));
                    columns.push(Arc::new(StructArray::new(
                        struct_fields(),
                        vec![a, b],
                        None,
                    )));
                }
            }
        }
        RecordBatch::try_new(Arc::clone(schema), columns).expect("batch matches schema")
    }
}

fn struct_fields() -> Fields {
    Fields::from(vec![
        Field::new("a", DataType::Int64, true),
        Field::new("b", DataType::Utf8, true),
    ])
}

fn local_store(dir: &std::path::Path) -> Arc<dyn ObjectStore> {
    Arc::new(LocalFileSystem::new_with_prefix(dir).expect("local store"))
}

async fn write_files(shape: Shape, dir: &std::path::Path) {
    std::fs::create_dir_all(dir).expect("create data dir");
    let session = VortexSession::default();
    let store = local_store(dir);
    let schema = shape.schema();
    for file in 0..FILES {
        let arrays: Vec<VortexArrayRef> = (0..CHUNKS)
            .map(|chunk| {
                session
                    .arrow()
                    .from_arrow_record_batch(shape.batch(&schema, file, chunk), &schema)
                    .expect("arrow batch converts to vortex")
            })
            .collect();
        let dtype = arrays[0].dtype().clone();
        let stream =
            ArrayStreamAdapter::new(dtype, futures::stream::iter(arrays.into_iter().map(Ok)));
        let mut writer =
            ObjectStoreWrite::new(Arc::clone(&store), &format!("part-{file:03}.vortex").into())
                .await
                .expect("open vortex file for writing");
        session
            .write_options()
            .write(&mut writer, stream)
            .await
            .expect("write vortex file");
        writer.shutdown().await.expect("finish vortex file");
    }
}

fn session_context(dir: &std::path::Path) -> SessionContext {
    let factory = Arc::new(VortexFormatFactory::new());
    let mut builder = SessionStateBuilder::new()
        .with_default_features()
        .with_table_factory(
            factory.get_ext().to_uppercase(),
            Arc::new(DefaultTableFactory::new()),
        )
        .with_object_store(
            &Url::try_from("file://").expect("file:// should parse as a URL"),
            local_store(dir),
        );
    if let Some(file_formats) = builder.file_formats() {
        file_formats.push(factory as _);
    }
    SessionContext::new_with_state(builder.build())
}

/// What the footer cache said its entries cost, and the heap they held.
struct Measured {
    src: &'static str,
    entries: usize,
    accounted: usize,
    freed: usize,
}

impl Measured {
    /// How many times its accounted size an entry actually retains.
    #[expect(
        clippy::cast_precision_loss,
        reason = "a ratio of byte counts far below 2^52, printed and compared at two decimals"
    )]
    fn retained_per_accounted(&self) -> f64 {
        self.freed as f64 / self.accounted as f64
    }
}

/// Clears the footer cache, measuring what it accounted and what clearing it
/// gave back to the allocator.
fn measure_and_clear(ctx: &SessionContext, shape: Shape, src: &'static str) -> Measured {
    let cache = ctx.runtime_env().cache_manager.get_file_metadata_cache();
    let listed = cache.list_entries();
    let entries = listed.len();
    let accounted: usize = listed.values().map(|e| e.size_bytes).sum();
    drop(listed);

    let before = LIVE.load(Ordering::SeqCst);
    cache.clear();
    let after = LIVE.load(Ordering::SeqCst);
    let measured = Measured {
        src,
        entries,
        accounted,
        freed: usize::try_from(before - after).unwrap_or_default(),
    };
    eprintln!(
        "footer cache, {shape:?}, {src}: {entries} entries, accounted {accounted} B \
         ({} B/entry), freed on clear {} B ({} B/entry), retained/accounted = {:.2}",
        accounted / entries.max(1),
        measured.freed,
        measured.freed / entries.max(1),
        measured.retained_per_accounted(),
    );
    measured
}

async fn drain(ctx: &SessionContext, sql: &str) -> usize {
    let mut stream = ctx
        .sql(sql)
        .await
        .expect("plan query")
        .execute_stream()
        .await
        .expect("execute query");
    let mut rows = 0;
    while let Some(batch) = stream.next().await {
        rows += batch.expect("query batch").num_rows();
    }
    rows
}

/// Footers cached as each population path leaves them: parsed by schema and
/// statistics inference, the same after a scan has read every column through
/// them, and handed over by the writer.
async fn measure_shape(shape: Shape) -> [Measured; 3] {
    let tmp = tempfile::tempdir().expect("tempdir");
    write_files(shape, &tmp.path().join("read")).await;
    let create = "CREATE EXTERNAL TABLE t STORED AS VORTEX LOCATION '/read/'";

    let ctx = session_context(tmp.path());
    ctx.sql(create).await.expect("create table");
    drain(&ctx, "SELECT count(*) FROM t").await;
    let parsed = measure_and_clear(&ctx, shape, "as parsed");
    assert_eq!(parsed.entries, FILES, "every file's footer is cached");

    // The scan materializes each column's layout subtree and zone map inside
    // the cached footer, so the same entry now retains more than it did when
    // inserted.
    let ctx = session_context(tmp.path());
    ctx.sql(create).await.expect("create table");
    let rows = drain(
        &ctx,
        &format!("SELECT * FROM t WHERE {}", shape.predicate()),
    )
    .await;
    assert_eq!(rows, FILES * CHUNKS * ROWS_PER_CHUNK, "scan read every row");
    let scanned = measure_and_clear(&ctx, shape, "after a full scan");
    assert_eq!(scanned.entries, FILES, "every file's footer is cached");

    let ctx = session_context(tmp.path());
    let schema = shape.schema();
    let batches: Vec<RecordBatch> = (0..FILES)
        .flat_map(|file| (0..CHUNKS).map(move |chunk| (file, chunk)))
        .map(|(file, chunk)| shape.batch(&schema, file, chunk))
        .collect();
    let src = datafusion::datasource::MemTable::try_new(Arc::clone(&schema), vec![batches])
        .expect("memtable");
    ctx.register_table("src", Arc::new(src))
        .expect("register src");
    drain(
        &ctx,
        "COPY (SELECT * FROM src) TO '/write/out.vortex' STORED AS VORTEX",
    )
    .await;
    let written = measure_and_clear(&ctx, shape, "as written");
    assert!(written.entries > 0, "the write path cached its footers");

    [parsed, scanned, written]
}

#[tokio::test(flavor = "current_thread")]
async fn the_footer_cache_accounts_for_what_its_entries_retain() {
    for shape in [Shape::Flat { groups: 16 }, Shape::Mixed { groups: 8 }] {
        let [parsed, scanned, written] = measure_shape(shape).await;

        // The cache evicts on the accounted size, so an entry retaining more
        // than it accounts for lets the cache exceed its limit.
        for measured in [&parsed, &scanned, &written] {
            assert!(
                measured.freed <= measured.accounted,
                "{shape:?} footers {} retain {:.2}x the size they report, so the footer \
                 cache can hold {:.2}x its limit",
                measured.src,
                measured.retained_per_accounted(),
                measured.retained_per_accounted(),
            );
        }

        // The accounted size is the fully-expanded one, charged up front. Bound
        // how far that over-charges an entry nothing has scanned yet, so the
        // estimate cannot pass by being arbitrarily large.
        assert!(
            parsed.accounted <= 4 * parsed.freed,
            "{shape:?} footers as parsed account for {} B but retain {} B: the estimate \
             over-charges an unscanned entry by more than 4x",
            parsed.accounted,
            parsed.freed,
        );
    }
}
