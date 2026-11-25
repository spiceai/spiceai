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

//! Before/after A/B for the adaptive-layout precedence fix, measured on the REAL
//! engine path (`files_scanned` from `EXPLAIN ANALYZE`), not a simulation.
//!
//! **How the two arms map to the two code paths.** The fix added
//! [`SortColumnsOrigin`]; the arms select the semantics directly:
//!
//! - `User` (= **BEFORE**) — an authoritative sort order always wins and
//!   observations are never consulted. This is exactly what the pre-fix code did
//!   with an inference-filled `cayenne_sort_columns`, because it had no way to
//!   tell a guess from operator intent.
//! - `Inferred` (= **AFTER**) — the sort order is a guess, so observed filter
//!   columns outrank it.
//!
//! Same binary, same data, same queries — only the precedence differs, so the
//! delta is attributable to the fix and nothing else.
//!
//! **Shape: TPC-H `lineitem`.** Primary key `(l_orderkey, l_linenumber)`;
//! selective predicate on `l_shipdate`, which is NOT a PK prefix. TPC-H's dbgen
//! draws `o_orderdate` uniformly at random over the ~7-year window rather than
//! monotonically in orderkey, so PK-major order carries no date locality — every
//! PK-ordered file spans the whole range and its zone map excludes nothing. That
//! is modeled here by generating `l_shipdate` independently of `l_orderkey`.
//!
//! Deterministic (seeded); the reported ratio is a layout property, so it holds
//! as the table grows — only the absolute file counts scale.
//!
//! The payoff is checked against what clustering guarantees rather than a ratio:
//! after the rewrite, each output file's footer `l_shipdate` range must follow
//! the one before it, and the probe must read exactly the files whose range
//! meets the window. The primary-key arm must read every file. Row counts are
//! checked against an answer computed from the generated rows, not from either
//! arm.

#![allow(clippy::expect_used)]

use crate::common;

// `SplitMix64` so the generated data is identical across arms and across runs.
use common::lookup_index::SplitMix64;

use std::sync::Arc;

use arrow::array::{Int32Array, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use cayenne::metadata::{CreateTableOptions, SortColumnsOrigin, VortexConfig};
use cayenne::provider::CayenneContext;
use cayenne::{CayenneTableProvider, CayenneTableProviderBuilder, MetadataCatalog};

use datafusion::datasource::TableProvider;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::prelude::{SessionContext, col, lit};

/// Rows loaded per arm. With the smallest configurable target file size (1 MB,
/// set in [`build_table`]) this spans several files: pruning is a per-file
/// zone-map property, so a table that fits in one file has no pruning to
/// measure at all. Production uses 256 MB with tables far larger than one file;
/// a small target with fewer rows is the scale-invariant proxy for that ratio.
const ROWS: usize = 250_000;
/// ~7 years of ship dates, as days-since-epoch offsets — the TPC-H window.
const DATE_SPAN_DAYS: i32 = 2557;
/// Selective probe window, ~1.2% of the span — the TPC-H q14 shape (one month).
const PROBE_WINDOW_DAYS: i32 = 30;
const PROBE_LO: i32 = 900;

fn lineitem_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("l_orderkey", DataType::Int64, false),
        Field::new("l_linenumber", DataType::Int64, false),
        Field::new("l_shipdate", DataType::Int32, false),
        Field::new("l_quantity", DataType::Int64, false),
    ]))
}

async fn build_table(
    fixture: &common::TestFixture,
    table_name: &str,
    origin: SortColumnsOrigin,
    runtime_env: Arc<RuntimeEnv>,
) -> Arc<CayenneTableProvider> {
    // What schema inference produces for a CDC'd lineitem: the primary key as the
    // sort order. `origin` selects whether the engine treats that as authoritative
    // (pre-fix behavior) or as a guess (post-fix).
    let vortex_config = VortexConfig {
        sort_columns: vec!["l_orderkey".to_string(), "l_linenumber".to_string()],
        sort_columns_origin: origin,
        target_vortex_file_size_mb: 1,
        ..VortexConfig::default()
    };
    let context = CayenneContext::new(
        &vortex_config,
        Arc::clone(&runtime_env),
        "layout_pruning_ab",
    );
    let options = CreateTableOptions {
        table_name: table_name.to_string(),
        schema: lineitem_schema(),
        primary_key: vec!["l_orderkey".to_string(), "l_linenumber".to_string()],
        on_conflict: None,
        base_path: fixture.data_path.to_string_lossy().to_string(),
        partition_column: None,
        vortex_config,
    };
    let catalog_arc = Arc::clone(&fixture.catalog);
    let catalog_arc: Arc<dyn MetadataCatalog> = catalog_arc;
    Arc::new(
        CayenneTableProviderBuilder::new(catalog_arc, runtime_env)
            .with_context(context)
            .create(options)
            .await
            .expect("create table"),
    )
}

/// Load the generated rows and return how many of them fall in the probe
/// window — the oracle the probe's answer is checked against.
async fn insert_lineitem(provider: &Arc<CayenneTableProvider>, table_name: &str) -> i64 {
    let ctx = SessionContext::new();
    ctx.register_table(table_name, Arc::clone(provider) as Arc<dyn TableProvider>)
        .expect("register");

    let schema = lineitem_schema();
    let mut rng = SplitMix64(0x5EED_CAFE_2026);
    let (mut ok, mut ln, mut sd, mut qty) = (
        Vec::with_capacity(ROWS),
        Vec::with_capacity(ROWS),
        Vec::with_capacity(ROWS),
        Vec::with_capacity(ROWS),
    );
    for i in 0..ROWS {
        let row = i64::try_from(i).expect("fits i64");
        // Rows arrive in PRIMARY-KEY order, as a CDC/append stream would.
        ok.push(row / 4);
        ln.push(row % 4);
        // Ship date INDEPENDENT of orderkey — TPC-H dbgen draws orderdate
        // uniformly over the window, so PK order carries no date locality.
        let d = i32::try_from(rng.next_u64() % u64::try_from(DATE_SPAN_DAYS).expect("span fits"))
            .expect("date fits i32");
        sd.push(d);
        qty.push(row % 50);
    }
    let probe_hi = PROBE_LO + PROBE_WINDOW_DAYS;
    let in_window = sd
        .iter()
        .filter(|&&d| (PROBE_LO..probe_hi).contains(&d))
        .count();
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Int64Array::from(ok)),
            Arc::new(Int64Array::from(ln)),
            Arc::new(Int32Array::from(sd)),
            Arc::new(Int64Array::from(qty)),
        ],
    )
    .expect("batch");
    let mem = datafusion::datasource::MemTable::try_new(schema, vec![vec![batch]]).expect("mem");
    ctx.register_table("src", Arc::new(mem)).expect("src");
    ctx.sql(&format!("INSERT INTO {table_name} SELECT * FROM src"))
        .await
        .expect("insert plan")
        .collect()
        .await
        .expect("insert");
    i64::try_from(in_window).expect("count fits i64")
}

/// Run the selective probe through the real scan path and return
/// (`files_scanned`, `matching_rows`).
async fn probe(provider: &Arc<CayenneTableProvider>, table_name: &str) -> (usize, i64) {
    let ctx = SessionContext::new();
    ctx.register_table(table_name, Arc::clone(provider) as Arc<dyn TableProvider>)
        .expect("register");
    let hi = PROBE_LO + PROBE_WINDOW_DAYS;
    let sql = format!(
        "SELECT COUNT(*) AS c FROM {table_name} \
         WHERE l_shipdate >= {PROBE_LO} AND l_shipdate < {hi}"
    );

    let rows = ctx
        .sql(&sql)
        .await
        .expect("probe plan")
        .collect()
        .await
        .expect("probe run");
    let matching = rows[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("i64")
        .value(0);

    let explain = ctx
        .sql(&format!("EXPLAIN ANALYZE {sql}"))
        .await
        .expect("explain plan")
        .collect()
        .await
        .expect("explain run");
    let text = arrow::util::pretty::pretty_format_batches(&explain)
        .expect("format")
        .to_string();
    // Sum every `files_scanned=N` the plan reports (one per scan branch).
    let files: usize = text
        .split("files_scanned=")
        .skip(1)
        .filter_map(|tail| {
            tail.chars()
                .take_while(char::is_ascii_digit)
                .collect::<String>()
                .parse::<usize>()
                .ok()
        })
        .sum();
    (files, matching)
}

/// Files the snapshot holds in total — the ceiling on any pruning ratio, and the
/// number that says whether the "before" arm pruned nothing or merely pruned less.
async fn total_files(provider: &Arc<CayenneTableProvider>, table_name: &str) -> usize {
    let ctx = SessionContext::new();
    ctx.register_table(table_name, Arc::clone(provider) as Arc<dyn TableProvider>)
        .expect("register");
    let explain = ctx
        // A bare COUNT(*) is answered from statistics and never opens a file, so
        // it reports zero. Use an always-true predicate the optimizer cannot
        // satisfy from metadata, forcing every file to be listed.
        .sql(&format!(
            "EXPLAIN ANALYZE SELECT COUNT(*) FROM {table_name} \
             WHERE l_shipdate >= 0 AND l_shipdate < {DATE_SPAN_DAYS}"
        ))
        .await
        .expect("plan")
        .collect()
        .await
        .expect("run");
    let text = arrow::util::pretty::pretty_format_batches(&explain)
        .expect("format")
        .to_string();
    text.split("files_scanned=")
        .skip(1)
        .filter_map(|t| {
            t.chars()
                .take_while(char::is_ascii_digit)
                .collect::<String>()
                .parse::<usize>()
                .ok()
        })
        .sum()
}

/// The `[min, max]` of `l_shipdate` each data file of the current snapshot
/// records in its footer — the statistics listing-time pruning reads — in file
/// name order, which is the order the rewrite's single writer emitted them.
async fn file_shipdate_ranges(
    fixture: &common::TestFixture,
    provider: &Arc<CayenneTableProvider>,
) -> Vec<(String, i32, i32)> {
    use vortex::VortexSessionDefault;
    use vortex::expr::stats::Stat;
    use vortex::file::OpenOptionsSessionExt;
    use vortex_session::VortexSession;

    let dir = fixture
        .data_path
        .join(provider.table_id())
        .join(provider.current_snapshot_id());
    let mut paths: Vec<std::path::PathBuf> = std::fs::read_dir(&dir)
        .expect("read snapshot dir")
        .map(|entry| entry.expect("dir entry").path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "vortex"))
        .collect();
    paths.sort();
    let column = lineitem_schema()
        .index_of("l_shipdate")
        .expect("l_shipdate in schema");
    let mut ranges = Vec::with_capacity(paths.len());
    for path in paths {
        let session = VortexSession::default();
        let file = session
            .open_options()
            .open_path(&path)
            .await
            .expect("open data file");
        let stats = file.file_stats().expect("data file has footer statistics");
        let (aggregates, dtype) = stats.get(column);
        let set = aggregates.to_stats_set();
        let min = set
            .get_as::<i32>(Stat::Min, dtype)
            .into_inner()
            .expect("footer min of l_shipdate");
        let max = set
            .get_as::<i32>(Stat::Max, dtype)
            .into_inner()
            .expect("footer max of l_shipdate");
        let name = path
            .file_name()
            .expect("file name")
            .to_string_lossy()
            .into_owned();
        ranges.push((name, min, max));
    }
    ranges
}

/// What one arm observed after its rewrite.
struct ArmResult {
    files_scanned: usize,
    total_files: usize,
    matching_rows: i64,
    expected_rows: i64,
    sort_key: Vec<String>,
    file_ranges: Vec<(String, i32, i32)>,
}

/// Drive one arm end to end: build → load → observe → rewrite → probe.
async fn run_arm(origin: SortColumnsOrigin, table_name: &str) -> ArmResult {
    let fixture = common::TestFixture::new(common::BackendType::Sqlite)
        .await
        .expect("fixture");
    let runtime_env = Arc::new(RuntimeEnv::default());
    let provider = build_table(&fixture, table_name, origin, runtime_env).await;
    let expected_rows = insert_lineitem(&provider, table_name).await;

    // Recurring selective workload: the engine observes `l_shipdate` on every scan.
    let ctx = SessionContext::new();
    let state = ctx.state();
    for _ in 0..25 {
        let _plan = provider
            .scan(&state, None, &[col("l_shipdate").lt(lit(PROBE_LO))], None)
            .await
            .expect("scan");
    }

    let chosen = provider.effective_sort_columns_for_rewrite();

    // Run the full compaction now. Under sustained query load the background
    // compactor never reaches the full-rewrite path (protected snapshots keep the
    // protected-subset path winning), so forcing it here is what makes the layout
    // observable at all — see the bench recipe.
    assert!(
        provider
            .force_full_compaction()
            .await
            .expect("full compaction"),
        "the full compaction must commit"
    );

    let file_ranges = file_shipdate_ranges(&fixture, &provider).await;
    let (files_scanned, matching_rows) = probe(&provider, table_name).await;
    let total = total_files(&provider, table_name).await;
    println!("    [{table_name}] total files in snapshot = {total}");
    for (name, min, max) in &file_ranges {
        println!("    [{table_name}] {name}: l_shipdate {min}..={max}");
    }
    ArmResult {
        files_scanned,
        total_files: total,
        matching_rows,
        expected_rows,
        sort_key: chosen,
        file_ranges,
    }
}

#[tokio::test]
async fn pruning_ab_inferred_vs_authoritative_sort() {
    // BEFORE: pre-fix semantics — the inference-set PK sort is treated as
    // authoritative, so observations are ignored and the rewrite clusters by PK.
    let before = run_arm(SortColumnsOrigin::User, "lineitem_before").await;

    // AFTER: post-fix semantics — the same PK sort is a guess, so the observed
    // `l_shipdate` outranks it and the rewrite clusters by date.
    let after = run_arm(SortColumnsOrigin::Inferred, "lineitem_after").await;

    println!(
        "\n=== layout pruning A/B ({ROWS} rows, TPC-H lineitem shape, \
         ~{:.1}% selective l_shipdate window) ===",
        f64::from(PROBE_WINDOW_DAYS) * 100.0 / f64::from(DATE_SPAN_DAYS)
    );
    println!(
        "  BEFORE (origin=User,     pre-fix): sort_key={:?} files_scanned={} of {} rows={}",
        before.sort_key, before.files_scanned, before.total_files, before.matching_rows
    );
    println!(
        "  AFTER  (origin=Inferred, post-fix): sort_key={:?} files_scanned={} of {} rows={}",
        after.sort_key, after.files_scanned, after.total_files, after.matching_rows
    );

    // Correctness gate FIRST: each arm must return the answer computed from the
    // generated rows. A layout change that alters results is a data-correctness
    // bug, not a speedup.
    assert!(
        before.expected_rows > 0,
        "probe must match some rows to be meaningful"
    );
    assert_eq!(
        before.matching_rows, before.expected_rows,
        "the primary-key layout must return every row in the window"
    );
    assert_eq!(
        after.matching_rows, after.expected_rows,
        "the l_shipdate layout must return every row in the window"
    );

    // The mechanism: the chosen clustering key differs between the two arms.
    assert_eq!(
        before.sort_key,
        vec!["l_orderkey".to_string(), "l_linenumber".to_string()],
        "pre-fix semantics must cluster by the (inference-set) primary key"
    );
    assert_eq!(
        after.sort_key,
        vec!["l_shipdate".to_string()],
        "post-fix semantics must cluster by the observed hot filter column"
    );

    // PRECONDITION. Pruning is a per-file property, so a table that lands in a
    // single file has no pruning to measure.
    assert!(
        before.total_files > 1 && after.total_files > 1,
        "fixture must span multiple files for the pruning measurement to mean \
         anything (before={} after={}); raise ROWS",
        before.total_files,
        after.total_files
    );
    assert_eq!(after.file_ranges.len(), after.total_files);

    // Ship dates are independent of the primary key, so every PK-ordered file
    // spans the date range and no file can be pruned.
    assert_eq!(
        before.files_scanned, before.total_files,
        "a primary-key layout carries no date locality, so the probe reads every file"
    );

    // The sorted rewrite's guarantee: its files, in the order it wrote them,
    // hold consecutive l_shipdate ranges (adjacent files may share the one
    // boundary value).
    for pair in after.file_ranges.windows(2) {
        let ((prev_name, _, prev_max), (next_name, next_min, _)) = (&pair[0], &pair[1]);
        assert!(
            prev_max <= next_min,
            "the rewrite sorted by l_shipdate, so {prev_name} (max {prev_max}) must end \
             at or before {next_name} (min {next_min}) begins: {:?}",
            after.file_ranges
        );
    }

    // So the probe reads exactly the files whose range meets the window, and
    // that is fewer than the primary-key layout reads.
    let probe_last = PROBE_LO + PROBE_WINDOW_DAYS - 1;
    let overlapping = after
        .file_ranges
        .iter()
        .filter(|(_, min, max)| *min <= probe_last && *max >= PROBE_LO)
        .count();
    assert_eq!(
        after.files_scanned, overlapping,
        "the probe must read exactly the files whose l_shipdate range meets \
         {PROBE_LO}..={probe_last}: {:?}",
        after.file_ranges
    );
    assert!(
        after.files_scanned < before.files_scanned,
        "clustering by the observed filter column must read fewer files than the \
         primary-key layout (before={} after={})",
        before.files_scanned,
        after.files_scanned
    );
}
