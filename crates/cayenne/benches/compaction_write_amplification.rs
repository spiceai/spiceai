/*
Copyright 2026 The Spice.ai OSS Authors

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

//! Compaction WRITE AMPLIFICATION + re-encode DURATION for the CDC delta →
//! compaction lifecycle — the mechanism behind the observed read/write-amp and
//! compaction-duration growth on the memory-durability SF1000 runs.
//!
//! Every row on this path is written to disk TWICE. A delta / mem-tier
//! checkpoint (`WriteClass::Delta`) writes each protected snapshot with a LIGHT
//! encoding — it skips the `BtrBlocks` per-column strategy search + FSST
//! symbol-table training that dominates encode cost (see
//! `provider::delta_encoding`, `effective_level` returns `AUTO_LIGHT_LEVEL` for
//! every `auto` delta, regardless of size). Those light files are LESS compressed, so
//! they inflate on-disk bytes (read-amp in bytes) until compaction folds them.
//! Compaction (`WriteClass::Maintenance`) then RE-ENCODES the merged corpus with
//! the FULL cascade — the expensive strategy search + FSST it skipped — so the
//! logical data is compressed properly but every byte is rewritten.
//!
//! This bench quantifies both halves that unit tests (which assert the *level
//! selection* in `delta_encoding.rs`, not the byte/latency cost) don't:
//!   - Pre-pass table: light delta bytes vs full compacted bytes vs the raw
//!     Arrow size ⇒ the write-amplification ratio `(light + full) / logical` and
//!     the light-vs-full compression gap that drives read-amp-in-bytes.
//!   - Timed lane `compact_reencode`: the wall-clock of the full-cascade
//!     re-encode of K accumulated light protected snapshots — the compaction
//!     duration that climbs as the drain produces more small files.
//!
//! Bench discipline (Tiger Style): setup outside the timed closure; every loop
//! bounded; every `expect` carries a message; the write/compaction outcomes are
//! asserted; data generation is deterministic (no RNG).

#![allow(clippy::expect_used)]

use std::path::Path;
use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use cayenne::metadata::{CreateTableOptions, VortexConfig};
use cayenne::{CayenneCatalog, CayenneTableProvider, MetadataCatalog};
use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use datafusion::prelude::SessionContext;
use datafusion_table_providers::util::column_reference::ColumnReference;
use datafusion_table_providers::util::on_conflict::OnConflict;
use std::hint::black_box;

/// Rows per delta. Small enough that each write is a light "hot-path" delta, but
/// carrying a mixed-compressibility string so light-vs-full actually diverges.
const ROWS_PER_DELTA: usize = 8_000;
/// Number of light protected snapshots to accumulate before compacting. Above
/// the trigger floor so the merge is real; representative of a handful of
/// mem-tier checkpoints piling up between compaction passes.
const DELTAS: usize = 8;
const COMPACTION_TRIGGER: usize = 4;

fn test_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]))
}

/// Deterministic pseudo-entropy suffix (xorshift on the row index) so the string
/// column carries both a repetitive (FSST/dict-friendly) prefix and a
/// high-entropy tail — the shape where the full BtrBlocks cascade beats the
/// light scheme, i.e. where re-encode write-amp is real.
fn entropy_suffix(row: usize) -> String {
    let mut state = (row as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    let mut out = String::with_capacity(48);
    for _ in 0..4 {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        out.push_str(&format!("{state:016x}"));
    }
    out
}

/// One delta's batch, keyed on a disjoint id range so each write publishes a NEW
/// protected snapshot (no on-conflict merge) — K deltas ⇒ K light files.
fn delta_batch(delta_index: usize) -> RecordBatch {
    let base = (delta_index * ROWS_PER_DELTA) as i64;
    let ids: Vec<i64> = (0..ROWS_PER_DELTA as i64).map(|i| base + i).collect();
    let values: Vec<i64> = ids.iter().map(|id| id * 7).collect();
    let names: Vec<String> = (0..ROWS_PER_DELTA)
        .map(|i| {
            format!(
                "row_prefix_{:03}_{}",
                i % 64,
                entropy_suffix(base as usize + i)
            )
        })
        .collect();
    RecordBatch::try_new(
        test_schema(),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(Int64Array::from(values)),
            Arc::new(StringArray::from(names)),
        ],
    )
    .expect("build delta batch")
}

/// Logical Arrow bytes of the full K-delta corpus (the write-amp denominator).
fn logical_arrow_bytes() -> u64 {
    (0..DELTAS)
        .map(|d| delta_batch(d).get_array_memory_size() as u64)
        .fold(0u64, u64::saturating_add)
}

struct Fixture {
    _temp_dir: tempfile::TempDir,
    catalog: Arc<dyn MetadataCatalog>,
    provider: CayenneTableProvider,
    data_path: std::path::PathBuf,
    table_id: String,
}

/// An upsert table with the inline memtable DISABLED, so every insert lands in a
/// file-backed protected snapshot (compaction's domain) rather than being
/// absorbed inline — the file-tier analog of the mem-tier checkpoint's light
/// delta. The background compactor is pinned far out so only the explicit
/// `compact_protected_snapshots_subset` call runs.
async fn setup_table(
    table_name: &str,
    runtime_env: Arc<datafusion::execution::runtime_env::RuntimeEnv>,
) -> Fixture {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let data_path = temp_dir.path().join("data");
    tokio::fs::create_dir_all(&data_path)
        .await
        .expect("create data dir");
    let db_path = temp_dir.path().join("catalog.db");
    let catalog = Arc::new(
        CayenneCatalog::new(format!("sqlite://{}", db_path.to_string_lossy())).expect("catalog"),
    ) as Arc<dyn MetadataCatalog>;
    catalog.init().await.expect("catalog init");

    let provider = CayenneTableProvider::create_table(
        Arc::clone(&catalog),
        CreateTableOptions {
            table_name: table_name.to_string(),
            schema: test_schema(),
            primary_key: vec!["id".to_string()],
            on_conflict: Some(OnConflict::Upsert(ColumnReference::new(vec![
                "id".to_string(),
            ]))),
            base_path: data_path.to_string_lossy().to_string(),
            partition_column: None,
            vortex_config: VortexConfig {
                inline_max_rows: 0,
                compaction_trigger_protected_snapshots: COMPACTION_TRIGGER,
                compaction_background_interval_ms: 3_600_000,
                ..VortexConfig::default()
            },
        },
        runtime_env,
    )
    .await
    .expect("create table");

    let table_id = catalog
        .get_table(table_name)
        .await
        .expect("get table")
        .table_id;
    Fixture {
        _temp_dir: temp_dir,
        catalog,
        provider,
        data_path,
        table_id,
    }
}

/// Write one delta via the real `insert_into` path (light-encoded protected
/// snapshot). Returns rows written.
async fn write_delta(fixture: &Fixture, batch: RecordBatch) -> u64 {
    use datafusion::datasource::TableProvider;
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::logical_expr::dml::InsertOp;
    use datafusion::physical_plan::collect;

    let ctx = SessionContext::new();
    let schema = batch.schema();
    let input =
        MemorySourceConfig::try_new_exec(&[vec![batch]], schema, None).expect("memory source");
    let plan = fixture
        .provider
        .insert_into(&ctx.state(), input, InsertOp::Append)
        .await
        .expect("insert plan");
    let results = collect(plan, ctx.task_ctx()).await.expect("insert");
    results
        .first()
        .and_then(|b| {
            b.column(0)
                .as_any()
                .downcast_ref::<arrow::array::UInt64Array>()
                .map(|c| c.value(0))
        })
        .unwrap_or(0)
}

/// Total on-disk `.vortex` bytes + file count under a snapshot data dir.
fn vortex_bytes_and_files(dir: &Path) -> (u64, usize) {
    let mut bytes = 0;
    let mut files = 0;
    let Ok(entries) = std::fs::read_dir(dir) else {
        return (0, 0);
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            let (b, f) = vortex_bytes_and_files(&path);
            bytes += b;
            files += f;
        } else if path.extension().is_some_and(|ext| ext == "vortex") {
            bytes += entry.metadata().map(|m| m.len()).unwrap_or(0);
            files += 1;
        }
    }
    (bytes, files)
}

/// Accumulate `DELTAS` light protected snapshots into a fresh table.
async fn accumulate_deltas(
    table: &str,
    runtime_env: Arc<datafusion::execution::runtime_env::RuntimeEnv>,
) -> Fixture {
    let fixture = setup_table(table, runtime_env).await;
    for d in 0..DELTAS {
        let rows = write_delta(&fixture, delta_batch(d)).await;
        assert_eq!(rows as usize, ROWS_PER_DELTA, "delta {d} row count");
    }
    fixture
}

fn bench_compaction_write_amplification(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    let logical = logical_arrow_bytes();

    // --- Pre-pass: the write-amp + compression ledger (bytes half of the
    // matrix; wall-clock alone can't show it). ---
    eprintln!(
        "\n=== compaction_write_amplification: {DELTAS} deltas x {ROWS_PER_DELTA} rows, {logical} logical Arrow bytes ==="
    );
    let (light_bytes, light_files, full_bytes, full_files) = runtime.block_on(async {
        let ctx = SessionContext::new();
        let fixture = accumulate_deltas("wamp_prepass", ctx.runtime_env()).await;
        let dir = fixture.data_path.join(&fixture.table_id);
        let (light_bytes, light_files) = vortex_bytes_and_files(&dir);
        let merged = fixture
            .provider
            .compact_protected_snapshots_subset(usize::MAX)
            .await
            .expect("compaction runs");
        assert!(merged, "the accumulated deltas must merge");
        let (full_bytes, full_files) = vortex_bytes_and_files(&dir);
        // The catalog is used by the compaction commit; keep it alive.
        black_box(&fixture.catalog);
        (light_bytes, light_files, full_bytes, full_files)
    });
    let denom = logical.max(1) as f64;
    #[expect(clippy::cast_precision_loss)]
    let light_ratio = light_bytes as f64 / denom;
    #[expect(clippy::cast_precision_loss)]
    let full_ratio = full_bytes as f64 / denom;
    #[expect(clippy::cast_precision_loss)]
    let write_amp = (light_bytes + full_bytes) as f64 / denom;
    eprintln!(
        "{:<22} {:>14} {:>8} {:>10}",
        "phase", "vortex_bytes", "files", "bytes/logical"
    );
    eprintln!(
        "{:<22} {light_bytes:>14} {light_files:>8} {light_ratio:>10.3}",
        "light deltas (K)"
    );
    eprintln!(
        "{:<22} {full_bytes:>14} {full_files:>8} {full_ratio:>10.3}",
        "full compacted (1)"
    );
    eprintln!(
        "write-amp (light+full)/logical = {write_amp:.3}  |  light-vs-full on-disk = {:.2}x\n",
        if full_bytes > 0 {
            light_bytes as f64 / full_bytes as f64
        } else {
            0.0
        }
    );

    // --- Timed: the full-cascade re-encode (compaction duration). Throughput in
    // logical bytes ⇒ MB/s of compaction re-encode. Setup (accumulate K light
    // deltas) is OUTSIDE the timed closure. ---
    let mut group = c.benchmark_group("compaction_write_amplification");
    group.sample_size(10);
    group.throughput(Throughput::Bytes(logical));
    let mut lane = 0u64;
    group.bench_function("compact_reencode", |b| {
        b.iter_batched(
            || {
                lane += 1;
                let ctx = SessionContext::new();
                runtime.block_on(accumulate_deltas(
                    &format!("wamp_{lane}"),
                    ctx.runtime_env(),
                ))
            },
            |fixture| {
                let merged = runtime.block_on(
                    fixture
                        .provider
                        .compact_protected_snapshots_subset(usize::MAX),
                );
                assert!(
                    matches!(merged, Ok(true)),
                    "timed compaction must merge the accumulated deltas"
                );
                black_box(&fixture.catalog);
            },
            criterion::BatchSize::PerIteration,
        );
    });
    group.finish();
}

criterion_group!(benches, bench_compaction_write_amplification);
criterion_main!(benches);
