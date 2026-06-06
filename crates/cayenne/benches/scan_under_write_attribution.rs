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

//! Attribution harness for the `scan_under_write` gap
//! (`vs_duckdb_concurrent`: cayenne ~8 ms vs duckdb ~144 µs while a
//! background writer is active).
//!
//! Times every scan in FOUR stages so the inflated stage names itself:
//!   1. `logical`  — `ctx.sql(...)` (parse + logical plan)
//!   2. `physical` — `create_physical_plan()` (optimizer rules +
//!      `TableProvider::scan`: listing-fence read, snapshot/deletion capture,
//!      inline-memtable read, listing-table build, file pruning)
//!   3. `first`    — `execute_stream` + first batch (Vortex file opens,
//!      footer/segment cache hits or misses)
//!   4. `drain`    — remaining batches.
//!
//! Modes: writer idle vs active (tight-loop 64-row inserts, the
//! `vs_duckdb_concurrent` shape) × cold session (fresh `SessionContext` per
//! scan, as `cayenne_query` does) vs warm (one long-lived session).
//!
//! Per the profiling-trap lesson: this harness runs ONLY the operation under
//! attribution in its timed region — the fixture is built once, the writer is
//! a separate task, and stages are timed in-process (no whole-process
//! sampler).
//!
//! Run: `cargo bench -p cayenne --bench scan_under_write_attribution`

#![allow(clippy::expect_used)]
#![allow(clippy::cast_precision_loss)]

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use cayenne::metadata::{CreateTableOptions, VortexConfig};
use cayenne::{CayenneCatalog, CayenneTableProvider, MetadataCatalog};
use datafusion::datasource::TableProvider;
use datafusion::prelude::SessionContext;
use futures::StreamExt;

const BASE_ROWS: i64 = 50_000;
const BG_BURST_ROWS: i64 = 64;
const SCANS_PER_MODE: usize = 200;
const SCAN_SQL: &str = "SELECT SUM(value) FROM t WHERE id BETWEEN 1000 AND 11000";

fn test_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
    ]))
}

fn make_batch(start: i64, rows: i64) -> RecordBatch {
    let ids: Vec<i64> = (start..start + rows).collect();
    let names: Vec<String> = ids.iter().map(|id| format!("name_{id}")).collect();
    let values: Vec<i64> = ids.iter().map(|id| id * 7).collect();
    RecordBatch::try_new(
        test_schema(),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(names)),
            Arc::new(Int64Array::from(values)),
        ],
    )
    .expect("build batch")
}

struct Fixture {
    _temp_dir: tempfile::TempDir,
    table: Arc<CayenneTableProvider>,
}

async fn insert(table: &Arc<CayenneTableProvider>, batch: RecordBatch) -> u64 {
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::logical_expr::dml::InsertOp;
    use datafusion::physical_plan::collect;

    let ctx = SessionContext::new();
    let schema = batch.schema();
    let input =
        MemorySourceConfig::try_new_exec(&[vec![batch]], schema, None).expect("memory source");
    let plan = table
        .insert_into(&ctx.state(), input, InsertOp::Append)
        .await
        .expect("insert plan");
    let results = collect(plan, ctx.task_ctx()).await.expect("insert");
    results
        .first()
        .and_then(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::UInt64Array>()
                .map(|counts| counts.value(0))
        })
        .unwrap_or(0)
}

async fn setup() -> Fixture {
    let temp_dir = tempfile::tempdir().expect("temp dir");
    let data_path = temp_dir.path().join("data");
    std::fs::create_dir_all(&data_path).expect("create data dir");
    let db_path = temp_dir.path().join("catalog.db");
    let catalog = Arc::new(
        CayenneCatalog::new(format!("sqlite://{}", db_path.to_string_lossy())).expect("catalog"),
    );
    catalog.init().await.expect("catalog init");

    let table = Arc::new(
        CayenneTableProvider::create_table(
            Arc::clone(&catalog) as Arc<dyn MetadataCatalog>,
            CreateTableOptions {
                table_name: "t".to_string(),
                schema: test_schema(),
                primary_key: vec![],
                on_conflict: None,
                base_path: data_path.to_string_lossy().to_string(),
                partition_column: None,
                vortex_config: VortexConfig::default(),
            },
            SessionContext::new().runtime_env(),
        )
        .await
        .expect("create table"),
    );

    let written = insert(&table, make_batch(0, BASE_ROWS)).await;
    assert_eq!(
        written,
        u64::try_from(BASE_ROWS).expect("rows"),
        "base load"
    );

    Fixture {
        _temp_dir: temp_dir,
        table,
    }
}

#[derive(Default, Clone, Copy)]
struct StageSample {
    logical_us: u64,
    physical_us: u64,
    first_us: u64,
    drain_us: u64,
}

/// One four-stage scan. `session` is reused in warm mode, rebuilt per call in
/// cold mode (the `cayenne_query` shape the original bench measures).
async fn staged_scan(
    table: &Arc<CayenneTableProvider>,
    warm: Option<&SessionContext>,
) -> StageSample {
    let cold;
    let ctx = match warm {
        Some(ctx) => ctx,
        None => {
            cold = SessionContext::new();
            cold.register_table("t", Arc::clone(table) as Arc<dyn TableProvider>)
                .expect("register");
            &cold
        }
    };

    let t0 = Instant::now();
    let df = ctx.sql(SCAN_SQL).await.expect("logical plan");
    let logical_us = u64::try_from(t0.elapsed().as_micros()).unwrap_or(u64::MAX);

    let t1 = Instant::now();
    let plan = df.create_physical_plan().await.expect("physical plan");
    let physical_us = u64::try_from(t1.elapsed().as_micros()).unwrap_or(u64::MAX);

    let t2 = Instant::now();
    let mut stream =
        datafusion::physical_plan::execute_stream(plan, ctx.task_ctx()).expect("execute stream");
    let first = stream.next().await;
    let first_us = u64::try_from(t2.elapsed().as_micros()).unwrap_or(u64::MAX);
    assert!(first.is_some(), "scan must yield at least one batch");

    let t3 = Instant::now();
    while let Some(batch) = stream.next().await {
        let _ = batch.expect("scan batch");
    }
    let drain_us = u64::try_from(t3.elapsed().as_micros()).unwrap_or(u64::MAX);

    StageSample {
        logical_us,
        physical_us,
        first_us,
        drain_us,
    }
}

fn percentile(sorted: &[u64], pct: usize) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let index = (sorted.len().saturating_sub(1)) * pct / 100;
    sorted[index]
}

fn report(label: &str, samples: &[StageSample]) {
    let stages: [(&str, fn(&StageSample) -> u64); 4] = [
        ("logical", |s| s.logical_us),
        ("physical", |s| s.physical_us),
        ("first", |s| s.first_us),
        ("drain", |s| s.drain_us),
    ];
    println!("--- {label} (n={}) ---", samples.len());
    println!(
        "{:<10} {:>10} {:>10} {:>10}",
        "stage", "mean_us", "p50_us", "p99_us"
    );
    let mut total_mean = 0_u64;
    for (name, get) in stages {
        let mut values: Vec<u64> = samples.iter().map(get).collect();
        values.sort_unstable();
        let mean = values.iter().sum::<u64>() / u64::try_from(values.len().max(1)).expect("len");
        total_mean += mean;
        println!(
            "{name:<10} {mean:>10} {:>10} {:>10}",
            percentile(&values, 50),
            percentile(&values, 99)
        );
    }
    println!("{:<10} {total_mean:>10}", "TOTAL");
    println!();
}

fn main() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");

    runtime.block_on(async {
        let fixture = setup().await;
        let warm_ctx = SessionContext::new();
        warm_ctx
            .register_table("t", Arc::clone(&fixture.table) as Arc<dyn TableProvider>)
            .expect("register warm");

        // Mode A/B: writer idle.
        let mut idle_cold = Vec::with_capacity(SCANS_PER_MODE);
        let mut idle_warm = Vec::with_capacity(SCANS_PER_MODE);
        for _ in 0..SCANS_PER_MODE {
            idle_cold.push(staged_scan(&fixture.table, None).await);
            idle_warm.push(staged_scan(&fixture.table, Some(&warm_ctx)).await);
        }

        // Modes C/D: writer active — the vs_duckdb_concurrent shape: a tight
        // loop of 64-row inserts on a background task.
        let stop = Arc::new(AtomicBool::new(false));
        let writes_done = Arc::new(AtomicU64::new(0));
        let writer = {
            let stop = Arc::clone(&stop);
            let writes_done = Arc::clone(&writes_done);
            let table = Arc::clone(&fixture.table);
            tokio::spawn(async move {
                let mut cursor = BASE_ROWS;
                while !stop.load(Ordering::Relaxed) {
                    let batch = make_batch(cursor, BG_BURST_ROWS);
                    cursor += BG_BURST_ROWS;
                    let _ = insert(&table, batch).await;
                    writes_done.fetch_add(1, Ordering::Relaxed);
                }
            })
        };
        // Let the writer reach steady state before measuring.
        tokio::time::sleep(Duration::from_millis(500)).await;

        let mut active_cold = Vec::with_capacity(SCANS_PER_MODE);
        let mut active_warm = Vec::with_capacity(SCANS_PER_MODE);
        for _ in 0..SCANS_PER_MODE {
            active_cold.push(staged_scan(&fixture.table, None).await);
            active_warm.push(staged_scan(&fixture.table, Some(&warm_ctx)).await);
        }

        stop.store(true, Ordering::Relaxed);
        let _ = writer.await;
        let writes = writes_done.load(Ordering::Relaxed);
        // Validity gate: the writer must actually have been writing during
        // the active window, or the "active" numbers are idle numbers.
        assert!(
            writes > 50,
            "background writer completed only {writes} inserts — the active \
             modes did not measure scans under write"
        );

        println!("=== scan_under_write attribution (µs per stage) ===");
        println!("bg writer inserts completed during active window: {writes}\n");
        report("idle / cold-session", &idle_cold);
        report("idle / warm-session", &idle_warm);
        report("ACTIVE / cold-session", &active_cold);
        report("ACTIVE / warm-session", &active_warm);
    });
}
