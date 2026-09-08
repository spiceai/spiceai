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

#![allow(clippy::expect_used)]

mod common;

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

const ROWS: usize = 1_000_000;
/// Target Vortex file size for the rewrite. Deliberately small so the table
/// spans MANY files: pruning is a per-file zone-map property, so a table that
/// fits in one file has no pruning to measure at all (the first run of this test
/// reported `files_scanned=1` in both arms for exactly that reason). Production
/// uses 256 MB with tables far larger than one file; a small target with fewer
/// rows is the scale-invariant proxy for that ratio.
const TARGET_FILE_BYTES: usize = 512 * 1024;
/// ~7 years of ship dates, as days-since-epoch offsets — the TPC-H window.
const DATE_SPAN_DAYS: i32 = 2557;
/// Selective probe window, ~1.2% of the span — the TPC-H q14 shape (one month).
const PROBE_WINDOW_DAYS: i32 = 30;
const PROBE_LO: i32 = 900;

/// `SplitMix64` so the generated data is identical across arms and across runs.
struct SplitMix64(u64);

impl SplitMix64 {
    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
}

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

async fn insert_lineitem(provider: &Arc<CayenneTableProvider>, table_name: &str) {
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

/// Drive one arm end to end: build → load → observe → rewrite → probe.
async fn run_arm(origin: SortColumnsOrigin, table_name: &str) -> (usize, i64, Vec<String>) {
    let fixture = common::TestFixture::new(common::BackendType::Sqlite)
        .await
        .expect("fixture");
    let runtime_env = Arc::new(RuntimeEnv::default());
    let provider = build_table(&fixture, table_name, origin, runtime_env).await;
    insert_lineitem(&provider, table_name).await;

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

    // Force the sorted rewrite directly. Under sustained query load the background
    // compactor never reaches the full-rewrite path (protected snapshots keep the
    // protected-subset path winning), so calling it here is what makes the layout
    // observable at all — see the bench recipe.
    provider
        .sort_and_rewrite_data(TARGET_FILE_BYTES)
        .await
        .expect("sort_and_rewrite");

    let (files, matching) = probe(&provider, table_name).await;
    let total = total_files(&provider, table_name).await;
    println!("    [{table_name}] total files in snapshot = {total}");
    (files, matching, chosen)
}

#[tokio::test]
async fn pruning_ab_inferred_vs_authoritative_sort() {
    // BEFORE: pre-fix semantics — the inference-set PK sort is treated as
    // authoritative, so observations are ignored and the rewrite clusters by PK.
    let (before_files, before_rows, before_key) =
        run_arm(SortColumnsOrigin::User, "lineitem_before").await;

    // AFTER: post-fix semantics — the same PK sort is a guess, so the observed
    // `l_shipdate` outranks it and the rewrite clusters by date.
    let (after_files, after_rows, after_key) =
        run_arm(SortColumnsOrigin::Inferred, "lineitem_after").await;

    println!(
        "\n=== layout pruning A/B ({ROWS} rows, TPC-H lineitem shape, \
         ~{:.1}% selective l_shipdate window) ===",
        f64::from(PROBE_WINDOW_DAYS) * 100.0 / f64::from(DATE_SPAN_DAYS)
    );
    println!(
        "  BEFORE (origin=User,     pre-fix): sort_key={before_key:?} files_scanned={before_files} rows={before_rows}"
    );
    println!(
        "  AFTER  (origin=Inferred, post-fix): sort_key={after_key:?} files_scanned={after_files} rows={after_rows}"
    );
    if after_files > 0 {
        #[expect(clippy::cast_precision_loss, reason = "file counts are small")]
        let ratio = before_files as f64 / after_files as f64;
        println!("  prune ratio (before ÷ after) = {ratio:.1}x");
    }

    // Correctness gate FIRST: both arms must return the same answer. A layout
    // change that alters results is a data-correctness bug, not a speedup.
    assert_eq!(
        before_rows, after_rows,
        "both layouts must return identical results for the same predicate"
    );
    assert!(
        before_rows > 0,
        "probe must match some rows to be meaningful"
    );

    // PRECONDITION GATE. Pruning is a per-file property, so a table that lands in
    // a single file has no pruning to measure and both arms would trivially read
    // 1 file — which is what the first version of this test actually did. Fail
    // loudly rather than report a meaningless 1.0x ratio.
    assert!(
        before_files > 1,
        "fixture must span multiple files for the pruning measurement to mean \
         anything (got {before_files}); lower TARGET_FILE_BYTES or raise ROWS"
    );

    // The mechanism: the chosen clustering key differs between the two arms.
    assert_eq!(
        before_key,
        vec!["l_orderkey".to_string(), "l_linenumber".to_string()],
        "pre-fix semantics must cluster by the (inference-set) primary key"
    );
    assert_eq!(
        after_key,
        vec!["l_shipdate".to_string()],
        "post-fix semantics must cluster by the observed hot filter column"
    );

    // The payoff: clustering by the filtered column must not read MORE files.
    assert!(
        after_files <= before_files,
        "clustering by the observed filter column must not increase files scanned \
         (before={before_files} after={after_files})"
    );
}
