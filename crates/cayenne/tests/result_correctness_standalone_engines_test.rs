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

//! # Standalone engines only (no Spice, no Cayenne)
//!
//! Proves that **out-of-Spice** DuckDB and SQLite return equivalent results for
//! the same SQL on identical data. This is the external-oracle baseline used
//! when comparing Spice accelerators (Cayenne, DuckDB accel, SQLite accel).
//!
//! Requires `--features result-correctness-duckdb` (DuckDB side). SQLite uses
//! always-on `rusqlite`.
//!
//! Suites: micro, SSB, SQLLancer (portable SQL only).
//! See `tests/correctness/README.md`.

#![allow(clippy::expect_used)]
#![allow(clippy::unwrap_used)]
#![allow(clippy::too_many_lines)]
#![allow(clippy::doc_markdown)]
#![allow(clippy::format_push_string)]
#![allow(clippy::map_unwrap_or)]

#[path = "correctness/support/mod.rs"]
mod support;

use std::path::PathBuf;

use support::inventory::build_inventory;
use support::report::{RunResult, summary_line};
use support::ssb_data::{SSB_TABLES, ssb_queries, write_ssb_parquet};
use support::standalone_engines::{
    STANDALONE_DUCKDB, STANDALONE_SQLITE, duckdb_query_batches, load_duckdb_from_batches,
    load_duckdb_from_parquet, load_sqlite_from_batches, load_sqlite_from_parquet,
    sqlite_query_batches,
};
use support::{
    ParityOutcome, assert_all_pass_or_excluded, compare_actual_results, make_dim_batch,
    make_fact_batch, micro_bench_queries, write_parquet,
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

fn compare_standalone(
    query: &Query,
    duck: &duckdb::Connection,
    sqlite: &rusqlite::Connection,
) -> ParityOutcome {
    let sql = query.sql.as_ref();
    let duck_res = duckdb_query_batches(duck, sql);
    let sqlite_res = sqlite_query_batches(sqlite, sql);

    match (duck_res, sqlite_res) {
        (Ok(d), Ok(s)) => compare_actual_results(query, &d, &s),
        (Err(e), Ok(_)) => {
            if e.contains("Parser Error") || e.contains("syntax error") {
                ParityOutcome::Excluded {
                    reason: format!("standalone DuckDB dialect rejects SQL: {e}"),
                }
            } else {
                ParityOutcome::EngineError {
                    side: STANDALONE_DUCKDB,
                    detail: e,
                }
            }
        }
        (Ok(_), Err(e)) => {
            if e.contains("syntax error") || e.contains("no such function") || e.contains("near \"")
            {
                ParityOutcome::Excluded {
                    reason: format!("standalone SQLite dialect rejects SQL: {e}"),
                }
            } else {
                ParityOutcome::EngineError {
                    side: STANDALONE_SQLITE,
                    detail: e,
                }
            }
        }
        (Err(de), Err(se)) => ParityOutcome::Excluded {
            reason: format!("both standalone engines error: duckdb={de}; sqlite={se}"),
        },
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn standalone_duckdb_vs_sqlite_micro() {
    let scratch = scratch_dir();
    std::fs::create_dir_all(&scratch).ok();

    let fact = make_fact_batch(2_048, 64);
    let dim = make_dim_batch(256);
    let (duck_temp, duck) = load_duckdb_from_batches(&[("t", fact.clone()), ("d", dim.clone())]);
    let (sqlite_temp, sqlite) = load_sqlite_from_batches(&[("t", fact), ("d", dim)]);
    let _keep = (duck_temp, sqlite_temp);

    let mut results = Vec::new();
    let mut labeled = Vec::new();
    for q in micro_bench_queries() {
        let outcome = compare_standalone(&q, &duck, &sqlite);
        eprintln!("standalone micro/{} -> {outcome:?}", q.name);
        results.push(RunResult {
            suite: "micro".into(),
            name: q.name.to_string(),
            engine_pair: "standalone-duckdb-sqlite",
            outcome: outcome.clone(),
        });
        labeled.push((q.name.to_string(), outcome));
    }

    let path = scratch.join("standalone_duckdb_sqlite_micro.log");
    let mut log = String::new();
    for r in &results {
        log.push_str(&format!("{}: {:?}\n", r.name, r.outcome));
    }
    log.push_str(&summary_line(&results));
    log.push('\n');
    std::fs::write(&path, &log).expect("write log");
    eprintln!("{}", summary_line(&results));
    assert_all_pass_or_excluded(&labeled, "standalone DuckDB vs SQLite micro");
}

#[tokio::test(flavor = "multi_thread")]
async fn standalone_duckdb_vs_sqlite_ssb() {
    let scratch = scratch_dir();
    std::fs::create_dir_all(&scratch).ok();
    let scale = env_i64("CAYENNE_PARITY_SSB_SCALE", 1);
    eprintln!("standalone SSB DuckDB vs SQLite scale={scale}");

    let ssb_dir = scratch.join(format!("ssb_scale{scale}"));
    if !ssb_dir.join("lineorder.parquet").exists() {
        write_ssb_parquet(&ssb_dir, scale);
    }

    let (duck_temp, duck) = load_duckdb_from_parquet(&ssb_dir, SSB_TABLES);
    let (sqlite_temp, sqlite) = load_sqlite_from_parquet(&ssb_dir, SSB_TABLES).await;
    let _keep = (duck_temp, sqlite_temp);

    let mut results = Vec::new();
    let mut labeled = Vec::new();
    for q in ssb_queries() {
        let outcome = compare_standalone(&q, &duck, &sqlite);
        eprintln!("standalone ssb/{} -> {outcome:?}", q.name);
        results.push(RunResult {
            suite: "ssb".into(),
            name: q.name.to_string(),
            engine_pair: "standalone-duckdb-sqlite",
            outcome: outcome.clone(),
        });
        labeled.push((q.name.to_string(), outcome));
    }

    let path = scratch.join("standalone_duckdb_sqlite_ssb.log");
    let mut log = format!("SSB scale={scale} standalone DuckDB vs SQLite\n");
    for r in &results {
        log.push_str(&format!("{}: {:?}\n", r.name, r.outcome));
    }
    log.push_str(&summary_line(&results));
    log.push('\n');
    std::fs::write(&path, &log).expect("write log");
    eprintln!("{}", summary_line(&results));
    assert_all_pass_or_excluded(&labeled, "standalone DuckDB vs SQLite SSB");
}

#[tokio::test(flavor = "multi_thread")]
async fn standalone_duckdb_vs_sqlite_sqllancer() {
    use support::sqllancer::{SQLLANCER_TABLES, make_t0_batch, make_t1_batch, sqllancer_queries};

    let scratch = scratch_dir();
    std::fs::create_dir_all(&scratch).ok();

    let t0 = make_t0_batch(200);
    let t1 = make_t1_batch(100);
    let parquet_dir = tempfile::tempdir().expect("parquet");
    write_parquet(&t0, &parquet_dir.path().join("sqllancer_t0.parquet"));
    write_parquet(&t1, &parquet_dir.path().join("sqllancer_t1.parquet"));

    let (duck_temp, duck) = load_duckdb_from_parquet(parquet_dir.path(), SQLLANCER_TABLES);
    let (sqlite_temp, sqlite) =
        load_sqlite_from_parquet(parquet_dir.path(), SQLLANCER_TABLES).await;
    let _keep = (duck_temp, sqlite_temp);

    let inventory = build_inventory();
    let mut results = Vec::new();
    let mut labeled = Vec::new();
    for q in sqllancer_queries() {
        // Reuse SQLite inventory exclusions (DataFusion-only SQL).
        if let Some(e) = inventory.iter().find(|e| e.name == q.name.as_ref())
            && let Some(reason) = e.sqlite_exclusion
        {
            let outcome = ParityOutcome::Excluded {
                reason: reason.to_string(),
            };
            results.push(RunResult {
                suite: "sqllancer".into(),
                name: q.name.to_string(),
                engine_pair: "standalone-duckdb-sqlite",
                outcome: outcome.clone(),
            });
            labeled.push((q.name.to_string(), outcome));
            continue;
        }
        let outcome = compare_standalone(&q, &duck, &sqlite);
        eprintln!("standalone sqllancer/{} -> {outcome:?}", q.name);
        results.push(RunResult {
            suite: "sqllancer".into(),
            name: q.name.to_string(),
            engine_pair: "standalone-duckdb-sqlite",
            outcome: outcome.clone(),
        });
        labeled.push((q.name.to_string(), outcome));
    }

    let path = scratch.join("standalone_duckdb_sqlite_sqllancer.log");
    let mut log = String::new();
    for r in &results {
        log.push_str(&format!("{}: {:?}\n", r.name, r.outcome));
    }
    log.push_str(&summary_line(&results));
    log.push('\n');
    std::fs::write(&path, &log).expect("write log");
    eprintln!("{}", summary_line(&results));
    assert_all_pass_or_excluded(&labeled, "standalone DuckDB vs SQLite SQLLancer");
}
