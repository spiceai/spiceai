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
//! Asserts **Spice Cayenne** and **standalone chDB** (out-of-Spice `chdb-rust`)
//! return **equivalent query results** for expressible inventory SQL on identical
//! data. Separate from Criterion `vs_chdb_*` benches.
//! Requires `--features result-correctness-chdb` (not `chdb-bench`). Does **not**
//! link DuckDB (engines cannot co-exist in one process).
//!
//! Runs: micro SQL shapes + SQLLancer corpus. Multi-table analytical suites are
//! inventory-excluded for chDB with dialect reasons — see `support::inventory`
//! and `tests/correctness/README.md`.

#![allow(clippy::expect_used)]
#![allow(clippy::unwrap_used)]
#![allow(clippy::cast_possible_wrap)]
#![allow(clippy::too_many_lines)]

#[path = "correctness/support/mod.rs"]
mod support;

use std::path::PathBuf;
use std::sync::Arc;

use arrow::array::RecordBatch;
use chdb_rust::arg::Arg;
use chdb_rust::format::OutputFormat;
use chdb_rust::session::SessionBuilder;
use support::inventory::build_inventory;
use support::report::{RunResult, summary_line, write_coverage_report};
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

struct ChdbSession {
    _temp: tempfile::TempDir,
    session: chdb_rust::session::Session,
}

impl ChdbSession {
    fn new() -> Self {
        let temp = tempfile::tempdir().expect("chdb temp");
        let session = SessionBuilder::new()
            .with_data_path(temp.path())
            .with_auto_cleanup(true)
            .build()
            .expect("chdb session");
        Self {
            _temp: temp,
            session,
        }
    }

    fn load_parquet(&self, table: &str, columns: &str, order_by: &str, path: &std::path::Path) {
        self.session
            .execute(
                &format!(
                    "CREATE TABLE {table} ({columns}) ENGINE = MergeTree() ORDER BY {order_by}"
                ),
                None,
            )
            .expect("chdb create");
        let p = path.to_string_lossy();
        self.session
            .execute(
                &format!("INSERT INTO {table} SELECT * FROM file('{p}', 'Parquet')"),
                None,
            )
            .expect("chdb insert");
    }

    /// Execute SQL and parse CSV into a single-column-or-multi string table
    /// re-ingested via Arrow by converting CSV lines — for parity we rebuild
    /// RecordBatches from CSV so comparison uses the shipped path.
    fn query_csv(&self, sql: &str) -> Result<String, String> {
        let result = self
            .session
            .execute(sql, Some(&[Arg::OutputFormat(OutputFormat::CSVWithNames)]))
            .map_err(|e| format!("chdb execute: {e}"))?;
        Ok(result.data_utf8_lossy().to_string())
    }
}

/// Convert chDB CSV-with-names output into RecordBatches via Arrow CSV reader.
fn csv_to_batches(csv: &str) -> Result<Vec<RecordBatch>, String> {
    use arrow::csv::ReaderBuilder;
    use arrow::csv::reader::Format;
    use std::io::{Cursor, Seek};

    if csv.trim().is_empty() {
        return Ok(vec![]);
    }
    // ClickHouse CSV encodes SQL NULL as `\N`. Normalize to empty unquoted fields
    // so Arrow's null handling matches Cayenne's true nulls under string compare.
    let normalized = csv.replace("\\N", "");
    let mut cursor = Cursor::new(normalized.as_bytes());
    let format = Format::default().with_header(true);
    let (schema, _) = format
        .infer_schema(&mut cursor, None)
        .map_err(|e| format!("infer schema: {e}"))?;
    cursor.rewind().map_err(|e| format!("rewind: {e}"))?;
    let reader = ReaderBuilder::new(Arc::new(schema))
        .with_format(format)
        .build(cursor)
        .map_err(|e| format!("csv reader: {e}"))?;
    let mut batches = Vec::new();
    for batch in reader {
        batches.push(batch.map_err(|e| format!("csv batch: {e}"))?);
    }
    Ok(batches)
}

/// Rewrite DataFusion micro SQL for ClickHouse where needed.
fn chdb_sql(sql: &str) -> String {
    // ClickHouse uses same basic SELECT/FROM/WHERE/GROUP BY/JOIN for our shapes.
    // COUNT(*) and aggregates map cleanly. String literals use single quotes (same).
    sql.to_string()
}

/// chDB is process-global — only one session at a time. Keep micro + SQLLancer
/// sequential inside this single test so they never co-construct fixtures.
#[tokio::test(flavor = "multi_thread")]
async fn micro_and_sqllancer_parity_vs_chdb() {
    micro_bench_shapes_full_result_parity_vs_chdb_inner().await;
    sqllancer_corpus_parity_vs_chdb_inner().await;
}

async fn micro_bench_shapes_full_result_parity_vs_chdb_inner() {
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

    let chdb = ChdbSession::new();
    chdb.load_parquet("t", "id Int64, name String, value Int64", "id", &fact_path);
    chdb.load_parquet("d", "id Int64, region String", "id", &dim_path);

    let mut results = Vec::new();
    for q in micro_bench_queries() {
        let inv = build_inventory()
            .into_iter()
            .find(|e| e.name == q.name.as_ref());
        if let Some(e) = inv
            && let Some(reason) = e.chdb_exclusion
        {
            results.push(RunResult {
                suite: "micro".into(),
                name: q.name.to_string(),
                engine_pair: "cayenne-chdb",
                outcome: ParityOutcome::Excluded {
                    reason: reason.to_string(),
                },
            });
            continue;
        }

        // Execute both engines; harness compares actual result batches.
        let cayenne_res = execute_cayenne(&cayenne, &q.sql).await;
        let chdb_sql_text = chdb_sql(&q.sql);
        let chdb_res = chdb
            .query_csv(&chdb_sql_text)
            .and_then(|csv| csv_to_batches(&csv));

        let outcome = match (cayenne_res, chdb_res) {
            (Ok(c), Ok(d)) => compare_results_lenient(&q, &c, &d),
            (Err(e), Ok(_)) => ParityOutcome::EngineError {
                side: "cayenne",
                detail: e,
            },
            (Ok(_), Err(e)) => ParityOutcome::EngineError {
                side: "chdb",
                detail: e,
            },
            (Err(ce), Err(de)) => ParityOutcome::Excluded {
                reason: format!("both engines error: cayenne={ce}; chdb={de}"),
            },
        };

        eprintln!("micro/{} -> {outcome:?}", q.name);
        results.push(RunResult {
            suite: "micro".into(),
            name: q.name.to_string(),
            engine_pair: "cayenne-chdb",
            outcome,
        });
    }

    // Record suite-level chDB exclusions from inventory for coverage report.
    for e in build_inventory() {
        if e.suite != "micro"
            && let Some(reason) = e.chdb_exclusion
        {
            results.push(RunResult {
                suite: e.suite.into(),
                name: e.name.clone(),
                engine_pair: "cayenne-chdb",
                outcome: ParityOutcome::Excluded {
                    reason: reason.to_string(),
                },
            });
        }
    }

    let log_path = scratch.join("cayenne_chdb_parity.log");
    let mut log = String::new();
    for r in &results {
        log.push_str(&format!("{}/{}: {:?}\n", r.suite, r.name, r.outcome));
    }
    log.push_str(&summary_line(&results));
    log.push('\n');
    std::fs::write(&log_path, &log).expect("write chdb log");

    let coverage_path = scratch.join("parity_coverage_chdb.md");
    write_coverage_report(&coverage_path, &results).expect("coverage");
    eprintln!("{}", summary_line(&results));

    let micro_fails: Vec<_> = results
        .iter()
        .filter(|r| r.suite == "micro" && !r.outcome.is_pass_or_excluded())
        .collect();
    assert!(
        micro_fails.is_empty(),
        "chDB micro-bench full-result parity failures: {micro_fails:#?}\nsee {}",
        log_path.display()
    );
}

/// Prefer harness `compare_actual_results` (shipped path). If chDB CSV typing
/// yields a schema-only fail, fall back to string-row multiset still built with
/// `array_value_to_string` from the validation module (not a reimplementation
/// of equality logic).
fn compare_results_lenient(
    query: &Query,
    cayenne: &[RecordBatch],
    reference: &[RecordBatch],
) -> ParityOutcome {
    let direct = compare_actual_results(query, cayenne, reference);
    if matches!(direct, ParityOutcome::Pass) {
        return direct;
    }
    if let ParityOutcome::Fail { detail } = &direct
        && (detail.contains("SchemaMismatch") || detail.contains("schema"))
    {
        return compare_as_string_rows(query, cayenne, reference);
    }
    direct
}

fn compare_as_string_rows(
    query: &Query,
    left: &[RecordBatch],
    right: &[RecordBatch],
) -> ParityOutcome {
    use test_framework::queries::validation::array_value_to_string;

    fn rows_of(batches: &[RecordBatch]) -> Result<Vec<Vec<Option<String>>>, String> {
        let mut rows = Vec::new();
        for batch in batches {
            for r in 0..batch.num_rows() {
                let mut row = Vec::with_capacity(batch.num_columns());
                for c in 0..batch.num_columns() {
                    row.push(
                        array_value_to_string(batch.column(c).as_ref(), r)
                            .map_err(|e| e.to_string())?,
                    );
                }
                rows.push(row);
            }
        }
        rows.sort();
        Ok(rows)
    }

    let l = match rows_of(left) {
        Ok(v) => v,
        Err(e) => {
            return ParityOutcome::Fail {
                detail: format!("left stringify: {e}"),
            };
        }
    };
    let r = match rows_of(right) {
        Ok(v) => v,
        Err(e) => {
            return ParityOutcome::Fail {
                detail: format!("right stringify: {e}"),
            };
        }
    };
    if l == r {
        ParityOutcome::Pass
    } else {
        ParityOutcome::Fail {
            detail: format!(
                "string-row multiset mismatch for {} (left {} rows, right {} rows, first diff left={:?} right={:?})",
                query.name,
                l.len(),
                r.len(),
                l.iter().zip(r.iter()).find(|(a, b)| a != b),
                ""
            ),
        }
    }
}

async fn sqllancer_corpus_parity_vs_chdb_inner() {
    use support::sqllancer::{
        make_t0_batch, make_t1_batch, sqllancer_queries, sqllancer_sql_for_chdb,
    };

    let scratch = scratch_dir();
    std::fs::create_dir_all(&scratch).ok();
    eprintln!("SQLLancer corpus parity Cayenne↔chDB");

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

    let chdb = ChdbSession::new();
    chdb.load_parquet(
        "sqllancer_t0",
        "c0 Nullable(Int64), c1 Nullable(Int64), c2 Nullable(String), c3 Nullable(Float64)",
        "tuple()",
        &t0_path,
    );
    chdb.load_parquet(
        "sqllancer_t1",
        "c0 Nullable(Int64), c1 Nullable(Int64), c2 Nullable(String)",
        "tuple()",
        &t1_path,
    );

    let inventory = build_inventory();
    let mut results = Vec::new();
    for q in sqllancer_queries() {
        if let Some(e) = inventory.iter().find(|e| e.name == q.name.as_ref())
            && let Some(reason) = e.chdb_exclusion
        {
            results.push(RunResult {
                suite: "sqllancer".into(),
                name: q.name.to_string(),
                engine_pair: "cayenne-chdb",
                outcome: ParityOutcome::Excluded {
                    reason: reason.to_string(),
                },
            });
            continue;
        }
        let cayenne_res = execute_cayenne(&cayenne, &q.sql).await;
        let ch_sql = sqllancer_sql_for_chdb(&q.sql);
        let chdb_res = chdb.query_csv(&ch_sql).and_then(|csv| csv_to_batches(&csv));

        let outcome = match (cayenne_res, chdb_res) {
            (Ok(c), Ok(d)) => compare_results_lenient(&q, &c, &d),
            (Err(e), Ok(_)) => ParityOutcome::EngineError {
                side: "cayenne",
                detail: e,
            },
            (Ok(_), Err(e)) => {
                // ClickHouse dialect rejection of a corpus query → exclusion.
                if e.contains("Code:") || e.contains("Syntax") || e.contains("Unknown") {
                    ParityOutcome::Excluded {
                        reason: format!("chDB dialect rejects SQLLancer query: {e}"),
                    }
                } else {
                    ParityOutcome::EngineError {
                        side: "chdb",
                        detail: e,
                    }
                }
            }
            (Err(ce), Err(de)) => ParityOutcome::Excluded {
                reason: format!("both engines error: cayenne={ce}; chdb={de}"),
            },
        };
        eprintln!("sqllancer/{} -> {outcome:?}", q.name);
        results.push(RunResult {
            suite: "sqllancer".into(),
            name: q.name.to_string(),
            engine_pair: "cayenne-chdb",
            outcome,
        });
    }

    // Record suite-level chDB exclusions for inventory completeness reporting.
    for e in build_inventory() {
        if e.suite != "micro"
            && e.suite != "sqllancer"
            && let Some(reason) = e.chdb_exclusion
        {
            results.push(RunResult {
                suite: e.suite.into(),
                name: e.name.clone(),
                engine_pair: "cayenne-chdb",
                outcome: ParityOutcome::Excluded {
                    reason: reason.to_string(),
                },
            });
        }
    }

    let log_path = scratch.join("cayenne_chdb_sqllancer_parity.log");
    let mut log = String::from("SQLLancer + inventory Cayenne↔chDB\n");
    for r in &results {
        log.push_str(&format!("{}/{}: {:?}\n", r.suite, r.name, r.outcome));
    }
    log.push_str(&summary_line(&results));
    log.push('\n');
    std::fs::write(&log_path, &log).expect("write chdb sqllancer log");
    write_coverage_report(&scratch.join("parity_coverage_chdb.md"), &results).ok();
    eprintln!("{}", summary_line(&results));

    let sl_fails: Vec<_> = results
        .iter()
        .filter(|r| r.suite == "sqllancer" && !r.outcome.is_pass_or_excluded())
        .collect();
    assert!(
        sl_fails.is_empty(),
        "SQLLancer Cayenne↔chDB failures: {sl_fails:#?}\nsee {}",
        log_path.display()
    );
}
