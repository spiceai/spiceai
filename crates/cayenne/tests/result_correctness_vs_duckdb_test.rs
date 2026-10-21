// Copyright 2024-2026 The Spice.ai OSS Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! # Result correctness (not performance)
//!
//! Asserts **Spice Cayenne** and **standalone DuckDB** (out-of-Spice `duckdb`
//! crate) return **equivalent query results** for the same SQL on identical data.
//! Separate from Criterion `vs_duckdb_*` benches and from
//! `tools/testoperator/dispatch/perf-cayenne-vs-duckdb/` (latency/throughput).
//!
//! Requires `--features result-correctness-duckdb` (not `duckdb-bench`).
//! See `tests/correctness/README.md`.
//!
//! Suites: TPC-H SF1, TPC-DS SF1, ClickBench, CH-benCHmark SF1, SSB, SpiceBench
//! (TPC-H scenario) SF1, SQLLancer corpus, micro SQL shapes.
//! Scale defaults SF1 (`CAYENNE_PARITY_*_SF`). ClickBench: `CLICKBENCH_HITS_PARQUET`
//! or ranking-deterministic fixture + env-failure log under `CAYENNE_PARITY_SCRATCH`.

// Same set the sibling `..._vs_sqlite_test.rs` carries. These went unenforced
// while the binary's `required-features` were unmet — clippy never built the
// target, so it never linted it either.
#![allow(clippy::expect_used)]
#![allow(clippy::unwrap_used)]
#![allow(clippy::cast_possible_wrap)]
#![allow(clippy::cast_possible_truncation)]
#![allow(clippy::cast_sign_loss)]
#![allow(clippy::too_many_lines)]
#![allow(clippy::doc_markdown)]
#![allow(clippy::format_push_string)]
#![allow(clippy::map_unwrap_or)]
#![allow(clippy::single_match_else)]
#![allow(clippy::clone_on_ref_ptr)]
#![allow(clippy::used_underscore_binding)]

use crate::support;

use std::path::{Path, PathBuf};

use arrow::array::RecordBatch;
use duckdb::Connection;
use support::inventory::{build_inventory, fixture};
use support::report::{RunResult, summary_line, write_coverage_report};
use support::{
    CayenneHarness, ParityOutcome, TPCH_TABLES, assert_all_pass_or_excluded,
    assert_modes_agree_on_actual_results, compare_actual_results, execute_cayenne, make_dim_batch,
    make_fact_batch, micro_bench_queries, write_parquet,
};
use test_framework::queries::{
    Query, get_clickbench_test_queries, get_tpcds_test_queries, get_tpch_test_queries,
};

fn scratch_dir() -> PathBuf {
    std::env::var_os("CAYENNE_PARITY_SCRATCH")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/cayenne_parity_scratch")
        })
}

fn env_f64(name: &str, default: f64) -> f64 {
    std::env::var(name)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

fn duckdb_query_batches(conn: &Connection, sql: &str) -> Result<Vec<RecordBatch>, String> {
    let mut stmt = conn.prepare(sql).map_err(|e| format!("prepare: {e}"))?;
    let batches: Vec<RecordBatch> = stmt
        .query_arrow([])
        .map_err(|e| format!("query_arrow: {e}"))?
        .collect();
    Ok(batches)
}

fn load_duckdb_from_parquet(
    parquet_dir: &Path,
    tables: &[&str],
) -> (tempfile::TempDir, Connection) {
    let temp = tempfile::tempdir().expect("duckdb temp");
    let db_path = temp.path().join("parity.duckdb");
    let conn = support::standalone_engines::open_duckdb_oracle(&db_path);
    for table in tables {
        let path = parquet_dir.join(format!("{table}.parquet"));
        conn.execute_batch(&format!(
            "CREATE TABLE {table} AS SELECT * FROM read_parquet('{}');",
            path.display()
        ))
        .unwrap_or_else(|e| panic!("duckdb load {table}: {e}"));
    }
    (temp, conn)
}

async fn load_cayenne_from_parquet(parquet_dir: &Path, tables: &[&str]) -> CayenneHarness {
    load_cayenne_from_parquet_with_mode(parquet_dir, tables, support::LoadMode::Full).await
}

async fn load_cayenne_from_parquet_with_mode(
    parquet_dir: &Path,
    tables: &[&str],
    mode: support::LoadMode,
) -> CayenneHarness {
    let mut harness = CayenneHarness::new().await;
    for table in tables {
        let path = parquet_dir.join(format!("{table}.parquet"));
        harness
            .load_parquet_table_with_mode(table, &path, mode)
            .await;
    }
    harness
}

async fn run_pair(
    _suite: &str,
    query: &Query,
    cayenne: &CayenneHarness,
    duck: &Connection,
    duck_sql: Option<&str>,
) -> ParityOutcome {
    run_pair_with_df_baseline(_suite, query, cayenne, duck, duck_sql, None).await
}

/// Execute SQL on Cayenne and DuckDB, then compare **actual returned batches**
/// via the shared harness (shipped `compare_query_result_batches` only).
///
/// When Cayenne and DuckDB disagree, the harness also executes the same SQL on
/// a DataFusion parquet baseline and names the result in the failure, as triage
/// context. It never turns the disagreement into a pass: Cayenne is built on
/// DataFusion, so matching DataFusion cannot clear it — a DataFusion bug Cayenne
/// inherits agrees with DataFusion too (spiceai/spiceai#13277's bounded `EXISTS`
/// is one). The SQLite and chDB lanes run the same suites against engines that
/// share no code with DataFusion, and are where such a cell is decided.
async fn run_pair_with_df_baseline(
    _suite: &str,
    query: &Query,
    cayenne: &CayenneHarness,
    duck: &Connection,
    duck_sql: Option<&str>,
    parquet_dir: Option<&Path>,
) -> ParityOutcome {
    let sql_c = query.sql.as_ref();
    let sql_d = duck_sql.unwrap_or(sql_c);

    // --- Execute real engines ---
    let cayenne_res = execute_cayenne(cayenne, sql_c).await;
    let duck_res = duckdb_query_batches(duck, sql_d);

    match (cayenne_res, duck_res) {
        (Ok(c), Ok(d)) => {
            // --- Harness compares actual result batches ---
            let direct = compare_actual_results(query, &c, &d);
            if matches!(
                direct,
                ParityOutcome::Pass
                    | ParityOutcome::OrderUnchecked { .. }
                    | ParityOutcome::Vacuous { .. }
            ) {
                return direct;
            }
            let Some(dir) = parquet_dir else {
                return direct;
            };
            let baseline = match datafusion_query_parquet(dir, cayenne.tables.keys(), sql_c).await {
                Ok(df_batches) => format!("{:?}", compare_actual_results(query, &c, &df_batches)),
                Err(e) => format!("could not run: {e}"),
            };
            ParityOutcome::Fail {
                detail: format!(
                    "harness: Cayenne vs DuckDB actual results {direct:?}; \
                     Cayenne vs DataFusion baseline (triage context only) {baseline}"
                ),
            }
        }
        (Err(e), Ok(_)) => ParityOutcome::EngineError {
            side: "cayenne",
            detail: e,
        },
        (Ok(_), Err(e)) => {
            if e.contains("Parser Error") || e.contains("syntax error") {
                ParityOutcome::Excluded {
                    reason: format!("DuckDB dialect/parser rejects Spice SQL: {e}"),
                }
            } else {
                ParityOutcome::EngineError {
                    side: "duckdb",
                    detail: e,
                }
            }
        }
        // DuckDB failing too does not make Cayenne's error an answer: a suite
        // query Cayenne cannot run is excluded in the inventory, with its reason,
        // or it fails here.
        (Err(ce), Err(de)) => ParityOutcome::EngineError {
            side: "cayenne",
            detail: format!("{ce}; DuckDB failed as well: {de}"),
        },
    }
}

/// Run SQL against parquet files via plain DataFusion (no Cayenne) as a baseline.
/// Verify Cayenne on its own when DuckDB is the side that cannot run the query.
///
/// A DuckDB binder rejection is a fact about DuckDB, not about Cayenne. The
/// DataFusion baseline resolves the same SQL over the same parquet, so Cayenne's
/// rows — and, through the shared compare path, the order it returned them in —
/// can still be checked. Recording the exclusion without doing that left these
/// queries with no verification of Cayenne at all, in the lane whose job is to
/// provide it.
async fn verify_cayenne_against_baseline(
    query: &Query,
    cayenne: &CayenneHarness,
    parquet_dir: &Path,
    duckdb_reason: &str,
) -> ParityOutcome {
    let rows = match execute_cayenne(cayenne, query.sql.as_ref()).await {
        Ok(rows) => rows,
        Err(detail) => {
            return ParityOutcome::EngineError {
                side: "cayenne",
                detail,
            };
        }
    };
    match datafusion_query_parquet(parquet_dir, cayenne.tables.keys(), query.sql.as_ref()).await {
        // A pass here is still an exclusion from the *DuckDB* comparison, and is
        // recorded as one so the inventory and the census keep agreeing; what
        // changes is that Cayenne was actually checked before it was recorded.
        Ok(baseline) => match compare_actual_results(query, &rows, &baseline) {
            ParityOutcome::Pass => ParityOutcome::Excluded {
                reason: format!(
                    "{duckdb_reason}; Cayenne verified against the DataFusion baseline instead"
                ),
            },
            judged => judged,
        },
        Err(e) => ParityOutcome::Excluded {
            reason: format!(
                "{duckdb_reason}; the DataFusion baseline could not run it either: {e}"
            ),
        },
    }
}

async fn datafusion_query_parquet(
    parquet_dir: &Path,
    table_names: impl Iterator<Item = &String>,
    sql: &str,
) -> Result<Vec<RecordBatch>, String> {
    use datafusion::prelude::{ParquetReadOptions, SessionContext};
    let ctx = SessionContext::new();
    for name in table_names {
        let path = parquet_dir.join(format!("{name}.parquet"));
        if !path.exists() {
            continue;
        }
        let path_str = path.to_string_lossy().into_owned();
        ctx.register_parquet(name.as_str(), &path_str, ParquetReadOptions::default())
            .await
            .map_err(|e| format!("register {name}: {e}"))?;
    }
    let df = ctx.sql(sql).await.map_err(|e| format!("sql: {e}"))?;
    df.collect().await.map_err(|e| format!("collect: {e}"))
}

#[tokio::test(flavor = "multi_thread")]
async fn micro_bench_shapes_full_result_parity_vs_duckdb() {
    let scratch = scratch_dir();
    std::fs::create_dir_all(&scratch).ok();

    let rows = 2_048usize;
    let fact = make_fact_batch(rows, 64);
    let dim = make_dim_batch(256);

    let parquet_dir = tempfile::tempdir().expect("parquet dir");
    let fact_path = parquet_dir.path().join("t.parquet");
    let dim_path = parquet_dir.path().join("d.parquet");
    write_parquet(&fact, &fact_path);
    write_parquet(&dim, &dim_path);

    let mut cayenne = CayenneHarness::new().await;
    cayenne.load_batch("t", fact).await;
    cayenne.load_batch("d", dim).await;

    let (duck_temp, duck) = load_duckdb_from_parquet(parquet_dir.path(), &["t", "d"]);
    let _keep = duck_temp;

    let mut results = Vec::new();
    for q in micro_bench_queries() {
        // DuckDB uses same table names for micro fixtures.
        let outcome = run_pair("micro", &q, &cayenne, &duck, None).await;
        eprintln!("micro/{} -> {outcome:?}", q.name);
        results.push(RunResult {
            suite: "micro".into(),
            name: q.name.to_string(),
            engine_pair: "cayenne-duckdb",
            outcome,
        });
    }

    let fails = support::report::unexplained(&results, &build_inventory(), &[]);
    let report_path = scratch.join("cayenne_duckdb_micro_parity.log");
    let mut log = String::new();
    for r in &results {
        log.push_str(&format!("{:?} {:?}\n", r.name, r.outcome));
    }
    log.push_str(&summary_line(&results));
    log.push('\n');
    std::fs::write(&report_path, &log).expect("write micro log");
    eprintln!("{}", summary_line(&results));

    assert!(
        fails.is_empty(),
        "micro-bench full-result parity failures: {fails:#?}"
    );
}

/// Make sure the TPC-H fixture is on disk. Shared by the TPC-H and SpiceBench
/// lanes, which load the same generated tables.
fn ensure_tpch_fixture(dir: &Path, sf: f64) {
    support::tpch_data::ensure_tpch_fixture(dir, sf);
}

#[tokio::test(flavor = "multi_thread")]
async fn tpch_full_result_parity_vs_duckdb() {
    let scratch = scratch_dir();
    std::fs::create_dir_all(&scratch).ok();
    let sf = env_f64("CAYENNE_PARITY_TPCH_SF", 1.0);
    eprintln!("TPC-H parity at SF={sf}");

    let parquet_dir = scratch.join(format!("tpch_sf{sf}"));
    ensure_tpch_fixture(&parquet_dir, sf);

    let cayenne = load_cayenne_from_parquet(&parquet_dir, TPCH_TABLES).await;
    let (duck_temp, duck) = load_duckdb_from_parquet(&parquet_dir, TPCH_TABLES);
    let _keep = duck_temp;

    let inventory = build_inventory();
    let mut results = Vec::new();

    for q in get_tpch_test_queries(None) {
        let inv = inventory.iter().find(|e| e.name == q.name.as_ref());
        if let Some(e) = inv
            && let Some(reason) = e.duckdb_exclusion
        {
            results.push(RunResult {
                suite: "tpch".into(),
                name: q.name.to_string(),
                engine_pair: "cayenne-duckdb",
                outcome: ParityOutcome::Excluded {
                    reason: reason.to_string(),
                },
            });
            continue;
        }

        let outcome =
            run_pair_with_df_baseline("tpch", &q, &cayenne, &duck, None, Some(&parquet_dir)).await;
        eprintln!("tpch/{} -> {outcome:?}", q.name);
        results.push(RunResult {
            suite: "tpch".into(),
            name: q.name.to_string(),
            engine_pair: "cayenne-duckdb",
            outcome,
        });
    }

    let log_path = scratch.join("cayenne_duckdb_tpch_parity.log");
    let mut log = format!("TPC-H SF={sf}\n");
    for r in &results {
        log.push_str(&format!("{}: {:?}\n", r.name, r.outcome));
    }
    log.push_str(&summary_line(&results));
    log.push('\n');
    std::fs::write(&log_path, &log).expect("write tpch log");
    eprintln!("{}", summary_line(&results));

    let fails = support::report::unexplained(&results, &build_inventory(), &[]);
    assert!(
        fails.is_empty(),
        "TPC-H full-result parity failures (SF={sf}): {fails:#?}\nsee {}",
        log_path.display()
    );
}

/// TPC-DS tables commonly referenced by the query set (DuckDB `dbgen` names).
const TPCDS_TABLES: &[&str] = &[
    "call_center",
    "catalog_page",
    "catalog_returns",
    "catalog_sales",
    "customer",
    "customer_address",
    "customer_demographics",
    "date_dim",
    "household_demographics",
    "income_band",
    "inventory",
    "item",
    "promotion",
    "reason",
    "ship_mode",
    "store",
    "store_returns",
    "store_sales",
    "time_dim",
    "warehouse",
    "web_page",
    "web_returns",
    "web_sales",
    "web_site",
];

/// Needs network for `INSTALL tpcds`, unlike the TPC-H fixture `support::tpch_data`
/// generates in-process.
fn generate_tpcds_parquet(out_dir: &Path, sf: f64) -> Option<PathBuf> {
    // The generation database is temporary and fresh per run. `dsdgen` populates
    // a schema and cannot be run twice against the same database — a reused one
    // fails with `Table with name "call_center" already exists` the moment
    // regeneration actually happens, which it never did while any leftover
    // fixture was trusted.
    let gen_home = tempfile::tempdir().expect("tpcds gen dir");
    let conn =
        Connection::open(gen_home.path().join("gen.duckdb")).expect("duckdb open for tpcds gen");
    // Only `INSTALL` reaches DuckDB's extension repository, so only it can fail
    // for want of a network and be reported as an environment that cannot supply
    // the fixture. Everything after it is local: a `LOAD` that fails means the
    // installed extension is unusable, and a `dsdgen` that fails means the
    // generator is broken. Running the three as one batch made either of those
    // indistinguishable from having no network, which turns a regression in the
    // fixture into a passing exclusion.
    if let Err(e) = conn.execute_batch("INSTALL tpcds;") {
        eprintln!("TPC-DS fixture unavailable: {e}");
        return None;
    }
    conn.execute_batch("LOAD tpcds;")
        .expect("load DuckDB's tpcds extension, which installed successfully");
    conn.execute_batch(&format!("CALL dsdgen(sf={sf});"))
        .expect("generate the TPC-DS fixture with dsdgen");

    // Replace what is on disk only now that generation has succeeded, so a
    // machine that could not reach the extension repository keeps the fixture it
    // already had instead of losing it to a run that was never going to finish.
    let _ = std::fs::remove_dir_all(out_dir);
    std::fs::create_dir_all(out_dir).expect("tpcds out dir");

    // Export every base table that exists after dsdgen.
    let mut stmt = conn
        .prepare(
            "SELECT table_name FROM information_schema.tables \
             WHERE table_schema = 'main' AND table_type = 'BASE TABLE'",
        )
        .expect("list tables");
    let names: Vec<String> = stmt
        .query_map([], |row| row.get(0))
        .expect("query tables")
        .filter_map(Result::ok)
        .collect();
    for table in names {
        let path = out_dir.join(format!("{table}.parquet"));
        // Skipping a failed export would leave a partial fixture behind, which
        // the next run reads as a complete one because the directory is not
        // empty. Every name here came from `information_schema` a moment ago.
        conn.execute_batch(&format!(
            "COPY {table} TO '{}' (FORMAT PARQUET);",
            path.display()
        ))
        .unwrap_or_else(|e| panic!("export TPC-DS table {table}: {e}"));
    }
    // The stamp says a run finished; this says it finished with the tables the
    // suite expects. They catch different things: an interrupted export, and a
    // `dsdgen` that quietly stops emitting one — which would otherwise surface
    // as queries failing on both engines, far from the cause.
    let exported: std::collections::BTreeSet<String> = std::fs::read_dir(out_dir)
        .expect("read tpcds fixture dir")
        .filter_map(Result::ok)
        .filter_map(|e| {
            e.file_name()
                .to_string_lossy()
                .strip_suffix(".parquet")
                .map(str::to_string)
        })
        .collect();
    let missing: Vec<&str> = TPCDS_TABLES
        .iter()
        .copied()
        .filter(|t| !exported.contains(*t))
        .collect();
    assert!(
        missing.is_empty(),
        "dsdgen produced no parquet for TPC-DS tables {missing:?} in {}",
        out_dir.display()
    );

    // Stamped only now, with every table exported. A run killed part-way leaves
    // the directory populated but unstamped, so the next one regenerates instead
    // of reading a fixture that is missing tables — where the queries against
    // those tables fail on both engines, far from the cause.
    support::mark_fixture_complete(out_dir, TPCDS_FIXTURE_REVISION);
    Some(out_dir.to_path_buf())
}

/// Revision for the TPC-DS fixture. Unlike SSB and TPC-H the generator is
/// DuckDB's `dsdgen`, not code in this repo, so there is no source to digest;
/// the stamp is carried for its completeness half, and this bumps only if the
/// export set or the extension pin changes.
const TPCDS_FIXTURE_REVISION: &str = "duckdb-dsdgen-1";

#[tokio::test(flavor = "multi_thread")]
async fn tpcds_and_clickbench_parity_vs_duckdb() {
    let scratch = scratch_dir();
    std::fs::create_dir_all(&scratch).ok();
    let mut results = Vec::new();

    // --- TPC-DS (default SF1 per acceptance criteria) ---
    let sf = env_f64("CAYENNE_PARITY_TPCDS_SF", 1.0);
    eprintln!("TPC-DS parity at SF={sf}");
    let tpcds_dir = scratch.join(format!("tpcds_sf{sf}"));
    // Reuse only a stamped fixture. Two sentinel files said nothing about the
    // other twenty-two, so a directory left behind by an interrupted dsdgen was
    // read as complete and its missing tables became `Excluded` — a pass.
    let tpcds_fixture_missing = !support::fixture_is_current(&tpcds_dir, TPCDS_FIXTURE_REVISION)
        && generate_tpcds_parquet(&tpcds_dir, sf).is_none();

    // Discover exported tables.
    let exported: Vec<String> = std::fs::read_dir(&tpcds_dir)
        .map(|rd| {
            rd.filter_map(Result::ok)
                .filter_map(|e| {
                    let name = e.file_name().to_string_lossy().into_owned();
                    name.strip_suffix(".parquet").map(str::to_string)
                })
                .filter(|n| n != "gen")
                .collect()
        })
        .unwrap_or_default();
    let table_refs: Vec<&str> = exported.iter().map(String::as_str).collect();

    if tpcds_fixture_missing {
        results.push(RunResult {
            suite: "tpcds".into(),
            name: "*".into(),
            engine_pair: "cayenne-duckdb",
            outcome: ParityOutcome::Excluded {
                reason: "TPC-DS fixture unavailable: DuckDB's tpcds extension could not be \
                         installed in this environment (needs network)"
                    .into(),
            },
        });
    } else {
        // Generation succeeded, so tables must exist. Treating their absence as
        // another environmental exclusion would let a silent generator failure
        // count as a pass.
        assert!(
            !table_refs.is_empty(),
            "TPC-DS generation reported success but exported no tables into {}",
            tpcds_dir.display()
        );
        let cayenne = load_cayenne_from_parquet(&tpcds_dir, &table_refs).await;
        let (duck_temp, duck) = load_duckdb_from_parquet(&tpcds_dir, &table_refs);
        let _keep = duck_temp;

        let inventory = build_inventory();
        for q in get_tpcds_test_queries(None, Some(1.0)) {
            // Reviewed exclusions live in the inventory, so the census counts them.
            let outcome = if let Some(reason) = inventory
                .iter()
                .find(|e| e.suite == "tpcds" && e.name == q.name.as_ref())
                .and_then(|e| e.duckdb_exclusion)
            {
                verify_cayenne_against_baseline(&q, &cayenne, &tpcds_dir, reason).await
            } else {
                run_pair_with_df_baseline("tpcds", &q, &cayenne, &duck, None, Some(&tpcds_dir))
                    .await
            };
            eprintln!("tpcds/{} -> {outcome:?}", q.name);
            results.push(RunResult {
                suite: "tpcds".into(),
                name: q.name.to_string(),
                engine_pair: "cayenne-duckdb",
                outcome,
            });
        }
    }

    // --- ClickBench ---
    // Prefer full SF1 hits parquet when provided (CLICKBENCH_HITS_PARQUET). The
    // public ClickBench dump is not vendored in-repo and S3 spicepods need
    // credentials — capture that absence, then fall back to a ranking-
    // deterministic local fixture that still exercises full-content equality.
    let hits_dir = tempfile::tempdir().expect("hits dir");
    let (hits_path, clickbench_fixture_note) = match std::env::var_os("CLICKBENCH_HITS_PARQUET") {
        Some(p) => {
            let path = PathBuf::from(p);
            assert!(
                path.exists(),
                "CLICKBENCH_HITS_PARQUET set but file missing: {}",
                path.display()
            );
            (
                path,
                "full SF1 hits via CLICKBENCH_HITS_PARQUET".to_string(),
            )
        }
        None => {
            let note =
                "CLICKBENCH_HITS_PARQUET unset; S3 spicepod clickbench/sf1 requires credentials \
                 not available in this environment. Using ranking-deterministic local fixture \
                 (power-law group counts, unique top-K ORDER BY keys) for full-content parity."
                    .to_string();
            let capture = scratch.join("clickbench_sf1_env_failure.log");
            std::fs::write(
                &capture,
                format!(
                    "environmental blocker for ClickBench SF1 full dump:\n{note}\n\
                     spicepod path would be: test/spicepods/clickbench/sf1/accelerated/\n\
                     set CLICKBENCH_HITS_PARQUET=/path/to/hits.parquet to use the real dataset.\n"
                ),
            )
            .expect("write clickbench env failure");
            eprintln!("{note}");
            let hits = support::clickbench_data::make_reduced_hits(
                support::clickbench_data::REDUCED_HITS_ROWS,
            );
            let path = hits_dir.path().join("hits.parquet");
            write_parquet(&hits, &path);
            (path, note)
        }
    };

    let mut cayenne_hits = CayenneHarness::new().await;
    cayenne_hits.load_parquet_table("hits", &hits_path).await;

    let duck_temp = tempfile::tempdir().expect("duck hits");
    let duck_path = duck_temp.path().join("hits.duckdb");
    let duck = support::standalone_engines::open_duckdb_oracle(&duck_path);
    duck.execute_batch(&format!(
        "CREATE TABLE hits AS SELECT * FROM read_parquet('{}');",
        hits_path.display()
    ))
    .expect("duck load hits");

    // DF baseline dir: register_parquet expects `{table}.parquet` beside peers.
    let hits_baseline_dir =
        if hits_path.file_name().and_then(|s| s.to_str()) == Some("hits.parquet") {
            hits_path.parent().expect("hits parent").to_path_buf()
        } else {
            // Symlink/copy into temp dir under the canonical name.
            let link = hits_dir.path().join("hits.parquet");
            if !link.exists() {
                std::fs::copy(&hits_path, &link).expect("copy hits for baseline");
            }
            hits_dir.path().to_path_buf()
        };

    eprintln!("clickbench fixture: {clickbench_fixture_note}");

    let inventory = build_inventory();
    for q in get_clickbench_test_queries(None) {
        let exclusion = inventory
            .iter()
            .find(|e| e.suite == "clickbench" && e.name == q.name.as_ref())
            .and_then(|e| e.duckdb_exclusion);
        let outcome = match exclusion {
            Some(reason) => ParityOutcome::Excluded {
                reason: reason.to_string(),
            },
            None => {
                run_pair_with_df_baseline(
                    "clickbench",
                    &q,
                    &cayenne_hits,
                    &duck,
                    None,
                    Some(&hits_baseline_dir),
                )
                .await
            }
        };
        eprintln!("clickbench/{} -> {outcome:?}", q.name);
        results.push(RunResult {
            suite: "clickbench".into(),
            name: q.name.to_string(),
            engine_pair: "cayenne-duckdb",
            outcome,
        });
    }

    let log_path = scratch.join("cayenne_duckdb_tpcds_clickbench_parity.log");
    let mut log = String::new();
    for r in &results {
        log.push_str(&format!("{}/{}: {:?}\n", r.suite, r.name, r.outcome));
    }
    log.push_str(&summary_line(&results));
    log.push('\n');
    std::fs::write(&log_path, &log).expect("write tpcds/clickbench log");

    let coverage_path = scratch.join("parity_coverage.md");
    // Merge micro + tpch outcomes if present from prior tests in same process? No —
    // write what we have plus inventory dump.
    write_coverage_report(&coverage_path, &results).expect("coverage report");
    eprintln!("{}", summary_line(&results));
    eprintln!("coverage report: {}", coverage_path.display());

    let tpcds_fixture = fixture::tpcds_dsdgen(sf);
    let unexplained = support::report::unexplained(
        &results,
        &build_inventory(),
        &[
            ("tpcds", &tpcds_fixture),
            ("clickbench", fixture::clickbench_hits()),
        ],
    );
    assert!(
        unexplained.is_empty(),
        "unexplained TPC-DS/ClickBench parity failures: {unexplained:#?}\nsee {}",
        log_path.display()
    );
}

// Silence unused constant warning when tables list is for documentation only.
fn _tpcds_tables_doc() -> &'static [&'static str] {
    TPCDS_TABLES
}

/// CH-benCHmark SF1: harness executes each query on Cayenne (full/append/changes)
/// and DuckDB, compares **actual result batches**, and also compares modes to
/// each other — all via `compare_actual_results` (shipped validation path).
#[tokio::test(flavor = "multi_thread")]
async fn chbench_sf1_load_mode_matrix_vs_duckdb() {
    use support::LoadMode;
    use support::chbench_data::CHBENCH_TABLES;
    use test_framework::queries::get_chbench_test_queries;

    let scratch = scratch_dir();
    std::fs::create_dir_all(&scratch).ok();
    let warehouses = env_f64("CAYENNE_PARITY_CHBENCH_SF", 1.0) as i64;
    eprintln!(
        "CH-benCHmark SF={warehouses} harness matrix: full|append|changes vs DuckDB + cross-mode"
    );

    let chbench_dir = scratch.join(format!("chbench_sf{warehouses}"));
    support::chbench_data::ensure_chbench_fixture(&chbench_dir, warehouses);

    let (duck_temp, duck) = load_duckdb_from_parquet(&chbench_dir, CHBENCH_TABLES);
    let _keep = duck_temp;

    // Load all three Cayenne modes once; harness reuses them per query.
    let mut cayenne_by_mode = Vec::new();
    for &mode in LoadMode::all() {
        eprintln!("loading Cayenne mode={}", mode.as_str());
        let h = load_cayenne_from_parquet_with_mode(&chbench_dir, CHBENCH_TABLES, mode).await;
        cayenne_by_mode.push((mode, h));
    }

    let mut results = Vec::new();
    let mut labeled: Vec<(String, ParityOutcome)> = Vec::new();

    for q in get_chbench_test_queries(None) {
        let cayenne_sql = support::chbench_data::chbench_sql_for_datafusion(&q.sql);
        let duck_sql = q.sql.as_ref();
        let q_c = Query::new(q.name.clone(), cayenne_sql.clone().into(), false);

        // 1) Execute DuckDB once — actual result batches from the engine.
        let duck_batches = match duckdb_query_batches(&duck, duck_sql) {
            Ok(b) => b,
            Err(e) => {
                let outcome = if e.contains("Parser Error") || e.contains("syntax error") {
                    ParityOutcome::Excluded {
                        reason: format!("DuckDB dialect/parser: {e}"),
                    }
                } else {
                    ParityOutcome::EngineError {
                        side: "duckdb",
                        detail: e,
                    }
                };
                for (mode, _) in &cayenne_by_mode {
                    results.push(RunResult {
                        suite: format!("chbench[{}]", mode.as_str()),
                        name: q.name.to_string(),
                        engine_pair: "cayenne-duckdb",
                        outcome: outcome.clone(),
                    });
                    labeled.push((format!("{}/{}", mode.as_str(), q.name), outcome.clone()));
                }
                continue;
            }
        };

        // 2) Execute each Cayenne mode; harness compares actual batches to DuckDB.
        let mut mode_owned: Vec<(String, Vec<RecordBatch>)> = Vec::new();
        for (mode, cayenne) in &cayenne_by_mode {
            let cayenne_batches = match execute_cayenne(cayenne, &cayenne_sql).await {
                Ok(b) => b,
                Err(e) => {
                    let outcome = ParityOutcome::EngineError {
                        side: "cayenne",
                        detail: e,
                    };
                    eprintln!("chbench/{}/{} -> {outcome:?}", mode.as_str(), q.name);
                    results.push(RunResult {
                        suite: format!("chbench[{}]", mode.as_str()),
                        name: q.name.to_string(),
                        engine_pair: "cayenne-duckdb",
                        outcome: outcome.clone(),
                    });
                    labeled.push((format!("{}/{}", mode.as_str(), q.name), outcome));
                    continue;
                }
            };

            // Harness: compare actual Cayenne batches to actual DuckDB batches.
            let vs_duck = compare_actual_results(&q_c, &cayenne_batches, &duck_batches);
            eprintln!(
                "chbench/{}/{} vs DuckDB -> {vs_duck:?}",
                mode.as_str(),
                q.name
            );
            results.push(RunResult {
                suite: format!("chbench[{}]", mode.as_str()),
                name: q.name.to_string(),
                engine_pair: "cayenne-duckdb",
                outcome: vs_duck.clone(),
            });
            labeled.push((format!("{}/{}", mode.as_str(), q.name), vs_duck));
            mode_owned.push((mode.as_str().to_string(), cayenne_batches));
        }

        // 3) Harness: cross-mode compare of actual Cayenne results (not transitive).
        if mode_owned.len() >= 2 {
            let refs: Vec<(&str, &[RecordBatch])> = mode_owned
                .iter()
                .map(|(m, b)| (m.as_str(), b.as_slice()))
                .collect();
            let cross = assert_modes_agree_on_actual_results(&q_c, &refs);
            eprintln!("chbench/cross-mode/{} -> {cross:?}", q.name);
            results.push(RunResult {
                suite: "chbench[cross-mode]".into(),
                name: q.name.to_string(),
                engine_pair: "cayenne-modes",
                outcome: cross.clone(),
            });
            labeled.push((format!("cross-mode/{}", q.name), cross));
        }
    }

    let log_path = scratch.join("cayenne_duckdb_chbench_mode_matrix.log");
    let mut log = format!(
        "CH-benCHmark SF={warehouses} harness: execute SQL + compare actual batches\n\
         modes=full,append,changes vs DuckDB + cross-mode\n"
    );
    for r in &results {
        log.push_str(&format!("{}/{}: {:?}\n", r.suite, r.name, r.outcome));
    }
    log.push_str(&summary_line(&results));
    log.push('\n');
    std::fs::write(&log_path, &log).expect("write chbench mode matrix log");
    std::fs::write(scratch.join("cayenne_duckdb_chbench_parity.log"), &log).ok();
    eprintln!("{}", summary_line(&results));

    // Harness assertion — tests fail in CI without human analysis of logs.
    assert_all_pass_or_excluded(&labeled, "CH-benCHmark load-mode matrix");
}

/// Star Schema Benchmark: classic Q1.1–Q4.3 on deterministic reduced-scale data.
#[tokio::test(flavor = "multi_thread")]
async fn ssb_full_result_parity_vs_duckdb() {
    use support::ssb_data::{SSB_TABLES, ensure_ssb_fixture, ssb_queries};

    let scratch = scratch_dir();
    std::fs::create_dir_all(&scratch).ok();
    let scale = env_f64("CAYENNE_PARITY_SSB_SCALE", 1.0) as i64;
    eprintln!("SSB parity vs DuckDB at scale={scale}");

    let ssb_dir = scratch.join(format!("ssb_scale{scale}"));
    ensure_ssb_fixture(&ssb_dir, scale);

    let cayenne = load_cayenne_from_parquet(&ssb_dir, SSB_TABLES).await;
    let (duck_temp, duck) = load_duckdb_from_parquet(&ssb_dir, SSB_TABLES);
    let _keep = duck_temp;

    let mut results = Vec::new();
    for q in ssb_queries() {
        let outcome =
            run_pair_with_df_baseline("ssb", &q, &cayenne, &duck, None, Some(&ssb_dir)).await;
        eprintln!("ssb/{} -> {outcome:?}", q.name);
        results.push(RunResult {
            suite: "ssb".into(),
            name: q.name.to_string(),
            engine_pair: "cayenne-duckdb",
            outcome,
        });
    }

    let log_path = scratch.join("cayenne_duckdb_ssb_parity.log");
    let mut log = format!("SSB scale={scale}\n");
    for r in &results {
        log.push_str(&format!("{}: {:?}\n", r.name, r.outcome));
    }
    log.push_str(&summary_line(&results));
    log.push('\n');
    std::fs::write(&log_path, &log).expect("write ssb log");
    eprintln!("{}", summary_line(&results));

    let fails = support::report::unexplained(&results, &build_inventory(), &[]);
    assert!(
        fails.is_empty(),
        "SSB full-result parity failures: {fails:#?}\nsee {}",
        log_path.display()
    );
}

/// SpiceBench SF1 built-in scenario is TPC-H — same data/SQL as TPC-H SF1 with
/// inventory names under the `spicebench` suite.
#[tokio::test(flavor = "multi_thread")]
async fn spicebench_sf1_tpch_scenario_parity_vs_duckdb() {
    let scratch = scratch_dir();
    std::fs::create_dir_all(&scratch).ok();
    let sf = env_f64("CAYENNE_PARITY_TPCH_SF", 1.0);
    eprintln!("SpiceBench SF1 (TPC-H scenario) parity at SF={sf}");

    let parquet_dir = scratch.join(format!("tpch_sf{sf}"));
    ensure_tpch_fixture(&parquet_dir, sf);

    let cayenne = load_cayenne_from_parquet(&parquet_dir, TPCH_TABLES).await;
    let (duck_temp, duck) = load_duckdb_from_parquet(&parquet_dir, TPCH_TABLES);
    let _keep = duck_temp;
    let inventory = build_inventory();

    let mut results = Vec::new();
    for q in get_tpch_test_queries(None) {
        let sb_name = q.name.replacen("tpch_", "spicebench_", 1);
        let sb_query = Query::new(sb_name.clone().into(), std::sync::Arc::clone(&q.sql), false);
        if let Some(e) = inventory.iter().find(|e| e.name == sb_name)
            && let Some(reason) = e.duckdb_exclusion
        {
            results.push(RunResult {
                suite: "spicebench".into(),
                name: sb_name,
                engine_pair: "cayenne-duckdb",
                outcome: ParityOutcome::Excluded {
                    reason: reason.to_string(),
                },
            });
            continue;
        }
        let outcome = run_pair_with_df_baseline(
            "spicebench",
            &sb_query,
            &cayenne,
            &duck,
            None,
            Some(&parquet_dir),
        )
        .await;
        eprintln!("spicebench/{sb_name} -> {outcome:?}");
        results.push(RunResult {
            suite: "spicebench".into(),
            name: sb_name,
            engine_pair: "cayenne-duckdb",
            outcome,
        });
    }

    let log_path = scratch.join("cayenne_duckdb_spicebench_parity.log");
    let mut log = format!("SpiceBench SF1 TPC-H scenario SF={sf}\n");
    for r in &results {
        log.push_str(&format!("{}: {:?}\n", r.name, r.outcome));
    }
    log.push_str(&summary_line(&results));
    log.push('\n');
    std::fs::write(&log_path, &log).expect("write spicebench log");
    eprintln!("{}", summary_line(&results));

    let fails = support::report::unexplained(&results, &build_inventory(), &[]);
    assert!(
        fails.is_empty(),
        "SpiceBench SF1 parity failures: {fails:#?}\nsee {}",
        log_path.display()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn sqllancer_corpus_parity_vs_duckdb() {
    use support::sqllancer::{
        SQLLANCER_TABLES, make_t0_batch, make_t1_batch, sqllancer_queries, t0_schema, t1_schema,
    };

    let scratch = scratch_dir();
    std::fs::create_dir_all(&scratch).ok();
    eprintln!("SQLLancer corpus parity Cayenne↔DuckDB");

    let rows = 200usize;
    let t0 = make_t0_batch(rows);
    let t1 = make_t1_batch(rows / 2);
    let parquet_dir = tempfile::tempdir().expect("sqllancer parquet");
    let t0_path = parquet_dir.path().join("sqllancer_t0.parquet");
    let t1_path = parquet_dir.path().join("sqllancer_t1.parquet");
    write_parquet(&t0, &t0_path);
    write_parquet(&t1, &t1_path);

    let mut cayenne = CayenneHarness::new().await;
    cayenne.load_batch("sqllancer_t0", t0).await;
    cayenne.load_batch("sqllancer_t1", t1).await;

    let (duck_temp, duck) = load_duckdb_from_parquet(parquet_dir.path(), SQLLANCER_TABLES);
    let _keep = duck_temp;
    let _ = (t0_schema(), t1_schema()); // schemas used by batch builders

    let mut results = Vec::new();
    for q in sqllancer_queries() {
        let outcome = run_pair_with_df_baseline(
            "sqllancer",
            &q,
            &cayenne,
            &duck,
            None,
            Some(parquet_dir.path()),
        )
        .await;
        eprintln!("sqllancer/{} -> {outcome:?}", q.name);
        results.push(RunResult {
            suite: "sqllancer".into(),
            name: q.name.to_string(),
            engine_pair: "cayenne-duckdb",
            outcome,
        });
    }

    let log_path = scratch.join("cayenne_duckdb_sqllancer_parity.log");
    let mut log = String::from("SQLLancer corpus Cayenne↔DuckDB\n");
    for r in &results {
        log.push_str(&format!("{}: {:?}\n", r.name, r.outcome));
    }
    log.push_str(&summary_line(&results));
    log.push('\n');
    std::fs::write(&log_path, &log).expect("write sqllancer log");
    write_coverage_report(&scratch.join("parity_coverage.md"), &results).ok();
    eprintln!("{}", summary_line(&results));

    let fails = support::report::unexplained(&results, &build_inventory(), &[]);
    assert!(
        fails.is_empty(),
        "SQLLancer corpus parity failures: {fails:#?}\nsee {}",
        log_path.display()
    );
}
