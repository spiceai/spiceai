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

//! The driver every standalone-oracle lane shares: translate a suite query for
//! the oracle, run it there and on Cayenne, and compare the actual batches with
//! the shipped compare path.
//!
//! A lane built on this driver records no exclusion of its own. The only cells it
//! skips are the ones the inventory names for its oracle — a static, reviewed
//! reason the census counts. Everything else is compared: a query the oracle
//! refuses to run, or one the translator cannot rewrite, is an `EngineError`
//! that fails the lane, because a rejection nobody reviewed is a hole that would
//! otherwise read as a pass.

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::time::{Duration, Instant};

use arrow::array::RecordBatch;
use test_framework::queries::Query;

use super::dialect::{ColumnKinds, Oracle, translate};
use super::inventory::{InventoryEntry, build_inventory};
use super::report::RunResult;
use super::{
    CayenneHarness, LoadMode, ParityOutcome, compare_actual_results, execute_cayenne,
    fixture_column_kinds,
};

/// A standalone engine answering the suites' queries.
pub trait OracleEngine {
    /// The engine-pair label the report and census use.
    fn pair(&self) -> &'static str;
    /// The dialect [`translate`] rewrites the suites' SQL into.
    fn dialect(&self) -> Oracle;
    /// Run `sql`, returning the result as Arrow batches.
    fn execute(&self, sql: &str) -> Result<Vec<RecordBatch>, String>;
    /// The inventory's reviewed exclusion of `entry` for this engine.
    fn exclusion(&self, entry: &InventoryEntry) -> Option<&'static str>;
}

/// One compared cell, with the row counts behind its outcome.
pub struct Compared {
    pub outcome: ParityOutcome,
    /// `(cayenne, oracle)` row counts; `None` where that side did not run.
    pub rows: (Option<usize>, Option<usize>),
    /// `(cayenne, oracle)` time to answer, where that side ran.
    pub elapsed: (Option<Duration>, Option<Duration>),
}

/// An oracle's answer to one suite query.
struct OracleAnswer {
    /// The rewritten SQL the oracle ran; `None` when there is no translation.
    sql: Option<String>,
    rows: Result<Vec<RecordBatch>, String>,
    elapsed: Option<Duration>,
}

/// Compares Cayenne against one oracle holding the same rows.
///
/// The oracle's answer to a query depends on the SQL and the rows, not on how
/// Cayenne loaded them, so it is computed once and every load mode compares
/// against it.
pub struct Lane<'a> {
    engine: &'a dyn OracleEngine,
    /// The kinds of the loaded tables' columns, which [`translate`] needs.
    columns: &'a ColumnKinds,
    answers: RefCell<HashMap<String, Rc<OracleAnswer>>>,
}

impl<'a> Lane<'a> {
    #[must_use]
    pub fn new(engine: &'a dyn OracleEngine, columns: &'a ColumnKinds) -> Self {
        Self {
            engine,
            columns,
            answers: RefCell::new(HashMap::new()),
        }
    }

    /// The oracle's answer to the suite query `name`, whose text is `source`.
    fn answer(&self, name: &str, source: &str) -> Rc<OracleAnswer> {
        if let Some(answer) = self.answers.borrow().get(name) {
            return Rc::clone(answer);
        }
        let answer = Rc::new(
            match translate(source, self.engine.dialect(), self.columns) {
                Ok(sql) => {
                    let started = Instant::now();
                    let rows = self.engine.execute(&sql).map(non_empty);
                    OracleAnswer {
                        sql: Some(sql),
                        rows,
                        elapsed: Some(started.elapsed()),
                    }
                }
                Err(error) => OracleAnswer {
                    sql: None,
                    rows: Err(format!(
                        "no translation for {:?}: {error}",
                        self.engine.dialect()
                    )),
                    elapsed: None,
                },
            },
        );
        self.answers
            .borrow_mut()
            .insert(name.to_string(), Rc::clone(&answer));
        answer
    }

    /// Compare `query` on `cayenne` with the oracle's answer to `oracle_source`.
    ///
    /// Cayenne runs `query.sql`, and the sort check reads that statement's
    /// `ORDER BY`. `oracle_source` is the suite's own text, which is what gets
    /// translated: for CH-benCHmark Cayenne runs a `mod()`-free rewrite of it.
    pub async fn compare(
        &self,
        cayenne: &CayenneHarness,
        query: &Query,
        oracle_source: &str,
    ) -> Compared {
        let engine = self.engine;
        let answer = self.answer(query.name.as_ref(), oracle_source);
        let Some(oracle_sql) = &answer.sql else {
            return Compared {
                outcome: ParityOutcome::EngineError {
                    side: engine.pair(),
                    detail: answer.rows.as_ref().err().cloned().unwrap_or_default(),
                },
                rows: (None, None),
                elapsed: (None, None),
            };
        };
        let started = Instant::now();
        let cayenne_rows = execute_cayenne(cayenne, &query.sql).await.map(non_empty);
        let cayenne_elapsed = started.elapsed();
        let rows = (
            cayenne_rows.as_ref().ok().map(|b| row_count(b)),
            answer.rows.as_ref().ok().map(|b| row_count(b)),
        );
        let outcome = match (cayenne_rows, &answer.rows) {
            (Ok(left), Ok(right)) => match compare_actual_results(query, &left, right) {
                ParityOutcome::Fail { detail } => ParityOutcome::Fail {
                    detail: format!("{detail}; oracle SQL: {oracle_sql}"),
                },
                other => other,
            },
            (Err(detail), _) => ParityOutcome::EngineError {
                side: "cayenne",
                detail,
            },
            (Ok(_), Err(detail)) => ParityOutcome::EngineError {
                side: engine.pair(),
                detail: format!("{detail}; oracle SQL: {oracle_sql}"),
            },
        };
        Compared {
            outcome,
            rows,
            elapsed: (Some(cayenne_elapsed), answer.elapsed),
        }
    }

    /// Run every query of a suite through [`Self::compare`], skipping only what
    /// the inventory excludes for this oracle.
    ///
    /// `suite` names the queries' inventory suite and `label` the report row
    /// (`chbench[append]`); `cayenne_query` gives the query Cayenne runs for each
    /// suite query, whose own text is translated for the oracle.
    pub async fn run_suite(
        &self,
        cayenne: &CayenneHarness,
        suite: &str,
        label: &str,
        queries: &[Query],
        inventory: &[InventoryEntry],
        cayenne_query: impl Fn(&Query) -> Query,
    ) -> Vec<RunResult> {
        let engine = self.engine;
        let mut results = Vec::with_capacity(queries.len());
        for query in queries {
            let entry = inventory
                .iter()
                .find(|e| e.suite == suite && e.name == query.name.as_ref())
                .unwrap_or_else(|| panic!("{suite} query {} is not in the inventory", query.name));
            let compared = match engine.exclusion(entry) {
                Some(reason) => Compared {
                    outcome: ParityOutcome::Excluded {
                        reason: reason.to_string(),
                    },
                    rows: (None, None),
                    elapsed: (None, None),
                },
                None => {
                    self.compare(cayenne, &cayenne_query(query), &query.sql)
                        .await
                }
            };
            let (outcome, rows, elapsed) = (compared.outcome, compared.rows, compared.elapsed);
            eprintln!(
                "{label}/{} -> {outcome:?} (rows cayenne={:?} {}={:?}; ms cayenne={:?} oracle={:?})",
                query.name,
                rows.0,
                engine.pair(),
                rows.1,
                elapsed.0.map(|d| d.as_millis()),
                elapsed.1.map(|d| d.as_millis()),
            );
            results.push(RunResult {
                suite: label.to_string(),
                name: query.name.to_string(),
                engine_pair: engine.pair(),
                outcome,
            });
        }
        results
    }
}

/// Drop zero-row batches. Engines disagree on whether an empty result is no
/// batch or one empty batch, and the compare path reads `[]` against `[empty]`
/// as a missing answer.
fn non_empty(batches: Vec<RecordBatch>) -> Vec<RecordBatch> {
    batches.into_iter().filter(|b| b.num_rows() > 0).collect()
}

fn row_count(batches: &[RecordBatch]) -> usize {
    batches.iter().map(RecordBatch::num_rows).sum()
}

/// Run a suite against `engine` over the parquet fixture in `parquet_dir`,
/// loading Cayenne from the same files once per load mode in `modes`.
///
/// `engine` must already hold `tables` from that directory. Rows for more than
/// one mode are labelled `suite[mode]`, as the DuckDB lane labels them; the
/// oracle answers each query once for all of them.
pub async fn run_fixture_suite(
    engine: &dyn OracleEngine,
    parquet_dir: &std::path::Path,
    tables: &[&str],
    suite: &str,
    queries: &[Query],
    modes: &[LoadMode],
    cayenne_query: impl Fn(&Query) -> Query,
) -> Vec<RunResult> {
    let columns = fixture_column_kinds(parquet_dir, tables);
    let inventory = build_inventory();
    let lane = Lane::new(engine, &columns);
    let mut results = Vec::new();
    for &mode in modes {
        let cayenne = CayenneHarness::from_parquet_dir(parquet_dir, tables, mode).await;
        let label = if modes.len() > 1 {
            format!("{suite}[{}]", mode.as_str())
        } else {
            suite.to_string()
        };
        results.extend(
            lane.run_suite(&cayenne, suite, &label, queries, &inventory, &cayenne_query)
                .await,
        );
    }
    results
}
