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

//! # Result correctness vs SQLite (not performance)
//!
//! Asserts **Spice Cayenne** and **standalone SQLite** (out-of-Spice `rusqlite`,
//! not the Spice SQLite accelerator) return **equivalent query results** for the
//! same SQL on identical data. Always available — no feature gate.
//!
//! Suites: SSB (star-schema), SQLLancer corpus, micro SQL shapes.
//! Spice SQLite **accelerator** vs standalone is in
//! `runtime`’s `result_correctness` test. See `tests/correctness/README.md`.

#![allow(clippy::expect_used)]
#![allow(clippy::unwrap_used)]
#![allow(clippy::too_many_lines)]
#![allow(clippy::cast_possible_wrap)]
#![allow(clippy::cast_possible_truncation)]
#![allow(clippy::cast_sign_loss)]
#![allow(clippy::doc_markdown)]
#![allow(clippy::format_push_string)]
#![allow(clippy::map_unwrap_or)]

#[path = "correctness/support/mod.rs"]
mod support;

use std::path::PathBuf;

use rusqlite::Connection;
use support::inventory::build_inventory;
use support::report::{RunResult, summary_line};
use support::sqlite_engine::{
    load_sqlite_from_batches, load_sqlite_from_parquet, sqlite_query_batches,
};
use support::ssb_data::{SSB_TABLES, ssb_queries, write_ssb_parquet};
use support::{
    CayenneHarness, ParityOutcome, assert_all_pass_or_excluded, compare_actual_results,
    execute_cayenne, make_dim_batch, make_fact_batch, micro_bench_queries, write_parquet,
};
use test_framework::queries::Query;

fn scratch_dir() -> PathBuf {
    std::env::var_os("CAYENNE_PARITY_SCRATCH")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/cayenne_parity_scratch")
        })
}

fn env_i64(name: &str, default: i64) -> i64 {
    std::env::var(name)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

async fn run_pair(
    query: &Query,
    cayenne: &CayenneHarness,
    sqlite: &Connection,
    sqlite_sql: Option<&str>,
) -> ParityOutcome {
    let sql_c = query.sql.as_ref();
    let sql_s = sqlite_sql.unwrap_or(sql_c);

    let cayenne_res = execute_cayenne(cayenne, sql_c).await;
    let sqlite_res = sqlite_query_batches(sqlite, sql_s);

    match (cayenne_res, sqlite_res) {
        (Ok(c), Ok(s)) => compare_actual_results(query, &c, &s),
        (Err(e), Ok(_)) => ParityOutcome::EngineError {
            side: "cayenne",
            detail: e,
        },
        (Ok(_), Err(e)) => {
            if e.contains("syntax error") || e.contains("no such function") || e.contains("near \"")
            {
                ParityOutcome::Excluded {
                    reason: format!("SQLite dialect/parser rejects SQL: {e}"),
                }
            } else {
                ParityOutcome::EngineError {
                    side: "sqlite",
                    detail: e,
                }
            }
        }
        (Err(ce), Err(se)) => ParityOutcome::Excluded {
            reason: format!("both engines error: cayenne={ce}; sqlite={se}"),
        },
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn micro_bench_shapes_full_result_parity_vs_sqlite() {
    let scratch = scratch_dir();
    std::fs::create_dir_all(&scratch).ok();

    let rows = 2_048usize;
    let fact = make_fact_batch(rows, 64);
    let dim = make_dim_batch(256);

    let mut cayenne = CayenneHarness::new().await;
    cayenne.load_batch("t", fact.clone()).await;
    cayenne.load_batch("d", dim.clone()).await;

    let (sqlite_temp, sqlite) = load_sqlite_from_batches(&[("t", fact), ("d", dim)]);
    let _keep = sqlite_temp;

    let mut results = Vec::new();
    let mut labeled = Vec::new();
    for q in micro_bench_queries() {
        let outcome = run_pair(&q, &cayenne, &sqlite, None).await;
        eprintln!("micro/{} -> {outcome:?}", q.name);
        results.push(RunResult {
            suite: "micro".into(),
            name: q.name.to_string(),
            engine_pair: "cayenne-sqlite",
            outcome: outcome.clone(),
        });
        labeled.push((q.name.to_string(), outcome));
    }

    let report_path = scratch.join("cayenne_sqlite_micro_parity.log");
    let mut log = String::new();
    for r in &results {
        log.push_str(&format!("{:?} {:?}\n", r.name, r.outcome));
    }
    log.push_str(&summary_line(&results));
    log.push('\n');
    std::fs::write(&report_path, &log).expect("write micro log");
    eprintln!("{}", summary_line(&results));

    assert_all_pass_or_excluded(&labeled, "micro vs SQLite");
}

#[tokio::test(flavor = "multi_thread")]
async fn ssb_full_result_parity_vs_sqlite() {
    let scratch = scratch_dir();
    std::fs::create_dir_all(&scratch).ok();
    let scale = env_i64("CAYENNE_PARITY_SSB_SCALE", 1);
    eprintln!("SSB parity vs SQLite at scale={scale}");

    let ssb_dir = scratch.join(format!("ssb_scale{scale}"));
    if !ssb_dir.join("lineorder.parquet").exists() {
        write_ssb_parquet(&ssb_dir, scale);
    }

    let mut cayenne = CayenneHarness::new().await;
    for table in SSB_TABLES {
        let path = ssb_dir.join(format!("{table}.parquet"));
        cayenne.load_parquet_table(table, &path).await;
    }
    let (sqlite_temp, sqlite) = load_sqlite_from_parquet(&ssb_dir, SSB_TABLES).await;
    let _keep = sqlite_temp;

    let inventory = build_inventory();
    let mut results = Vec::new();
    let mut labeled = Vec::new();

    for q in ssb_queries() {
        if let Some(e) = inventory.iter().find(|e| e.name == q.name.as_ref())
            && let Some(reason) = e.sqlite_exclusion
        {
            let outcome = ParityOutcome::Excluded {
                reason: reason.to_string(),
            };
            results.push(RunResult {
                suite: "ssb".into(),
                name: q.name.to_string(),
                engine_pair: "cayenne-sqlite",
                outcome: outcome.clone(),
            });
            labeled.push((q.name.to_string(), outcome));
            continue;
        }

        let outcome = run_pair(&q, &cayenne, &sqlite, None).await;
        eprintln!("ssb/{} -> {outcome:?}", q.name);
        results.push(RunResult {
            suite: "ssb".into(),
            name: q.name.to_string(),
            engine_pair: "cayenne-sqlite",
            outcome: outcome.clone(),
        });
        labeled.push((q.name.to_string(), outcome));
    }

    let log_path = scratch.join("cayenne_sqlite_ssb_parity.log");
    let mut log = format!("SSB scale={scale}\n");
    for r in &results {
        log.push_str(&format!("{}: {:?}\n", r.name, r.outcome));
    }
    log.push_str(&summary_line(&results));
    log.push('\n');
    std::fs::write(&log_path, &log).expect("write ssb log");
    eprintln!("{}", summary_line(&results));

    assert_all_pass_or_excluded(&labeled, "SSB vs SQLite");
}

#[tokio::test(flavor = "multi_thread")]
async fn sqllancer_corpus_parity_vs_sqlite() {
    use support::sqllancer::{SQLLANCER_TABLES, make_t0_batch, make_t1_batch, sqllancer_queries};

    let scratch = scratch_dir();
    std::fs::create_dir_all(&scratch).ok();
    eprintln!("SQLLancer corpus parity Cayenne↔SQLite");

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

    let (sqlite_temp, sqlite) =
        load_sqlite_from_parquet(parquet_dir.path(), SQLLANCER_TABLES).await;
    let _keep = sqlite_temp;

    let inventory = build_inventory();
    let mut results = Vec::new();
    let mut labeled = Vec::new();

    for q in sqllancer_queries() {
        if let Some(e) = inventory.iter().find(|e| e.name == q.name.as_ref())
            && let Some(reason) = e.sqlite_exclusion
        {
            let outcome = ParityOutcome::Excluded {
                reason: reason.to_string(),
            };
            results.push(RunResult {
                suite: "sqllancer".into(),
                name: q.name.to_string(),
                engine_pair: "cayenne-sqlite",
                outcome: outcome.clone(),
            });
            labeled.push((q.name.to_string(), outcome));
            continue;
        }

        let outcome = run_pair(&q, &cayenne, &sqlite, None).await;
        eprintln!("sqllancer/{} -> {outcome:?}", q.name);
        results.push(RunResult {
            suite: "sqllancer".into(),
            name: q.name.to_string(),
            engine_pair: "cayenne-sqlite",
            outcome: outcome.clone(),
        });
        labeled.push((q.name.to_string(), outcome));
    }

    let log_path = scratch.join("cayenne_sqlite_sqllancer_parity.log");
    let mut log = String::new();
    for r in &results {
        log.push_str(&format!("{}: {:?}\n", r.name, r.outcome));
    }
    log.push_str(&summary_line(&results));
    log.push('\n');
    std::fs::write(&log_path, &log).expect("write sqllancer log");
    eprintln!("{}", summary_line(&results));

    assert_all_pass_or_excluded(&labeled, "SQLLancer vs SQLite");
}
