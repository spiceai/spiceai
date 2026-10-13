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

//! Machine-checkable coverage inventory for Cayenne query-result parity.
//!
//! Suites (all SF1 unless noted):
//! - TPC-H, TPC-DS, ClickBench, CH-benCHmark, SSB, SpiceBench (TPC-H scenario),
//!   SQLLancer corpus, micro-bench shapes.
//!
//! Engines: Cayenne, DuckDB, chDB, SQLite (pairwise — DuckDB and chDB cannot co-link).

use std::collections::BTreeMap;
use std::sync::Arc;

use test_framework::queries::{
    Query, get_chbench_test_queries, get_clickbench_test_queries, get_tpcds_test_queries,
    get_tpch_test_queries,
};

use super::dialect::{Oracle, untranslatable};
use super::micro_bench_queries;
use super::sqllancer::sqllancer_queries;
use super::ssb_data::ssb_queries;

/// One inventory entry: suite query + per-engine status.
#[derive(Debug, Clone)]
pub struct InventoryEntry {
    pub suite: &'static str,
    pub name: String,
    pub sql: String,
    /// `None` means the engine runs the query; `Some(reason)` is a justified exclusion.
    pub duckdb_exclusion: Option<&'static str>,
    /// `None` means expressible in chDB; `Some(reason)` means not compared vs chDB.
    pub chdb_exclusion: Option<&'static str>,
    /// `None` means compared vs SQLite; `Some(reason)` is a justified exclusion.
    pub sqlite_exclusion: Option<&'static str>,
    /// `Some(reason)` names why this query's own `ORDER BY` cannot be fully
    /// verified — an `ORDER BY` term the result columns do not carry, so no
    /// engine's row order can be checked against it. Reviewed here rather than
    /// tolerated at the gate: an unverified order that nobody has looked at is
    /// the outcome the sort check exists to surface, so it fails instead.
    pub order_unchecked_review: Option<&'static str>,
    /// The fixtures on which this query's answer holds no value — no rows, or
    /// only NULL — and why. Without a review naming the fixture a lane loaded,
    /// an empty agreement fails as vacuous: two engines returning nothing
    /// compared nothing.
    pub empty_result_review: Option<EmptyResultReview>,
}

/// A reviewed empty answer: the fixtures a query was run on and found to
/// select nothing from, and why.
#[derive(Debug, Clone, Copy)]
pub struct EmptyResultReview {
    /// The fixtures, as [`fixture`] names them, on which the answer holds no
    /// value. An empty answer on any other fixture fails.
    pub fixtures: &'static [&'static str],
    pub reason: &'static str,
}

impl EmptyResultReview {
    /// Whether the review accepts an empty answer on `fixture`.
    #[must_use]
    pub fn covers(&self, fixture: &str) -> bool {
        self.fixtures.contains(&fixture)
    }
}

/// Names for the rows a lane loads, which decide what a query's answer holds.
///
/// Empty answers are reviewed per fixture. A query that selects nothing from
/// one generator's rows, or at one scale, can answer on another, and a review
/// that outlived its fixture would pass an empty answer where rows belong.
pub mod fixture {
    /// TPC-DS at SF1 as DuckDB's `dsdgen` writes it: the DuckDB lane's default.
    pub const TPCDS_DSDGEN_SF1: &str = "TPC-DS dsdgen SF1";
    /// TPC-DS at SF1 as `tpcdsgen` writes it: the chDB lane's default.
    pub const TPCDS_TPCDSGEN_SF1: &str = "TPC-DS tpcdsgen SF1";
    /// TPC-DS at SF 0.1 as `tpcdsgen` writes it: the SQLite lane's default.
    pub const TPCDS_TPCDSGEN_SF0_1: &str = "TPC-DS tpcdsgen SF0.1";
    /// The reduced ClickBench `hits` table the lanes build by default.
    pub const REDUCED_HITS: &str = "reduced ClickBench hits";
    /// The ClickBench `hits` dump `CLICKBENCH_HITS_PARQUET` names.
    pub const HITS_DUMP: &str = "ClickBench hits dump";

    /// TPC-DS as DuckDB's `dsdgen` writes it at `sf`.
    #[must_use]
    pub fn tpcds_dsdgen(sf: f64) -> String {
        format!("TPC-DS dsdgen SF{sf}")
    }

    /// TPC-DS as `tpcdsgen` writes it at `sf`.
    #[must_use]
    pub fn tpcds_tpcdsgen(sf: f64) -> String {
        format!("TPC-DS tpcdsgen SF{sf}")
    }

    /// The ClickBench `hits` rows a lane loads: the dump when
    /// `CLICKBENCH_HITS_PARQUET` names one, the reduced table otherwise.
    #[must_use]
    pub fn clickbench_hits() -> &'static str {
        if std::env::var_os("CLICKBENCH_HITS_PARQUET").is_some() {
            HITS_DUMP
        } else {
            REDUCED_HITS
        }
    }
}

/// Why a query's `ORDER BY` cannot be verified against its own result columns.
///
/// Every entry is an `ORDER BY` over an expression the projection does not
/// carry, so there is no output column holding the values the engine sorted by.
/// The mappable leading terms are still enforced where there are any; the list
/// records what remains unverified so a *new* hole cannot hide among them.
fn order_unchecked_review(suite: &str, name: &str) -> Option<&'static str> {
    match (suite, name) {
        ("tpcds", "tpcds_q36" | "tpcds_q70" | "tpcds_q86") => Some(
            "ORDER BY over `CASE WHEN lochierarchy = 0 THEN …`, which the projection \
             does not carry; the leading `lochierarchy` term is still enforced",
        ),
        ("tpcds", "tpcds_q47" | "tpcds_q57" | "tpcds_q89") => Some(
            "ORDER BY over the derived `sum_sales - avg_monthly_sales`, which the \
             projection does not carry, so no result column holds the sort key",
        ),
        ("clickbench", "clickbench_q25" | "clickbench_q27") => Some(
            "ORDER BY over a `to_timestamp(…)` expression absent from the projection, \
             so no result column holds the sort key",
        ),
        _ => None,
    }
}

/// The fixtures on which a query's answer holds no value, and so compares
/// nothing.
///
/// Each entry is a hole, not a pass: the census counts it, and naming it here is
/// what lets a lane accept the empty agreement instead of failing on it.
/// Emptiness is a property of the fixture's rows, so every fixture listed is one
/// a lane loaded and saw the query select nothing from.
fn empty_result_review(suite: &str, name: &str) -> Option<EmptyResultReview> {
    use fixture::{REDUCED_HITS, TPCDS_DSDGEN_SF1, TPCDS_TPCDSGEN_SF0_1, TPCDS_TPCDSGEN_SF1};
    const EVERY_TPCDS: &[&str] = &[TPCDS_DSDGEN_SF1, TPCDS_TPCDSGEN_SF1, TPCDS_TPCDSGEN_SF0_1];
    let (fixtures, reason): (&'static [&'static str], &'static str) = match (suite, name) {
        (
            "tpcds",
            "tpcds_q8" | "tpcds_q37" | "tpcds_q41" | "tpcds_q44" | "tpcds_q54" | "tpcds_q58",
        ) => (
            EVERY_TPCDS,
            "no rows on either generator's data at either scale: the query's parameters \
             select none",
        ),
        ("tpcds", "tpcds_q61" | "tpcds_q92") => (
            EVERY_TPCDS,
            "one all-NULL aggregate row on either generator's data at either scale: the \
             query's filters select no rows",
        ),
        // SQLite has no standard deviation, so neither query runs at SF 0.1.
        ("tpcds", "tpcds_q29") => (
            &[TPCDS_DSDGEN_SF1, TPCDS_TPCDSGEN_SF1],
            "no rows on either generator's SF1 data: the query's parameters select none",
        ),
        ("tpcds", "tpcds_q17") => (
            &[TPCDS_TPCDSGEN_SF1],
            "no rows on tpcdsgen's SF1 data; the dsdgen rows the DuckDB lane loads answer it",
        ),
        ("tpcds", "tpcds_q25" | "tpcds_q85") => (
            &[TPCDS_TPCDSGEN_SF1, TPCDS_TPCDSGEN_SF0_1],
            "no rows on tpcdsgen's data; the dsdgen rows the DuckDB lane loads answer it",
        ),
        (
            "tpcds",
            "tpcds_q31" | "tpcds_q64" | "tpcds_q65" | "tpcds_q73" | "tpcds_q82" | "tpcds_q83"
            | "tpcds_q84",
        ) => (
            &[TPCDS_TPCDSGEN_SF0_1],
            "no rows at SF 0.1, where the SQLite lane runs; both SF1 lanes answer it",
        ),
        ("tpcds", "tpcds_q13" | "tpcds_q32") => (
            &[TPCDS_TPCDSGEN_SF0_1],
            "one all-NULL aggregate row at SF 0.1, where the SQLite lane runs; both SF1 \
             lanes answer it",
        ),
        ("clickbench", "clickbench_q20") => (
            &[REDUCED_HITS],
            "the reduced hits fixture has no row with UserID 435090932899640449; compared \
             only with CLICKBENCH_HITS_PARQUET",
        ),
        ("clickbench", "clickbench_q22" | "clickbench_q23" | "clickbench_q24") => (
            &[REDUCED_HITS],
            "the reduced hits fixture has no URL containing 'google' or Title containing \
             'Google'; compared only with CLICKBENCH_HITS_PARQUET",
        ),
        ("clickbench", "clickbench_q28" | "clickbench_q29") => (
            &[REDUCED_HITS],
            "HAVING COUNT(*) > 100000 selects nothing from the 50,000-row reduced hits \
             fixture; compared only with CLICKBENCH_HITS_PARQUET",
        ),
        ("clickbench", "clickbench_q39" | "clickbench_q40" | "clickbench_q43") => (
            &[REDUCED_HITS],
            "OFFSET 1000 skips past every group CounterID 62 has in the reduced hits \
             fixture; compared only with CLICKBENCH_HITS_PARQUET",
        ),
        ("clickbench", "clickbench_q41") => (
            &[REDUCED_HITS],
            "the reduced hits fixture has no row with RefererHash 3594120000172545465; \
             compared only with CLICKBENCH_HITS_PARQUET",
        ),
        ("clickbench", "clickbench_q42") => (
            &[REDUCED_HITS],
            "the reduced hits fixture has no row with URLHash 2868770270353813622; compared \
             only with CLICKBENCH_HITS_PARQUET",
        ),
        _ => return None,
    };
    Some(EmptyResultReview { fixtures, reason })
}

/// Build the full inventory from suite sources + micro + SQLLancer.
#[must_use]
pub fn build_inventory() -> Vec<InventoryEntry> {
    let mut entries = Vec::new();

    for q in get_tpch_test_queries(None) {
        entries.push(InventoryEntry {
            suite: "tpch",
            name: q.name.to_string(),
            sql: q.sql.to_string(),
            duckdb_exclusion: tpch_duckdb_exclusion(&q),
            chdb_exclusion: oracle_exclusion(&q, Oracle::ClickHouse),
            sqlite_exclusion: oracle_exclusion(&q, Oracle::Sqlite),
            order_unchecked_review: order_unchecked_review("tpch", q.name.as_ref()),
            empty_result_review: empty_result_review("tpch", q.name.as_ref()),
        });
    }

    for q in get_tpcds_test_queries(None, Some(1.0)) {
        entries.push(InventoryEntry {
            suite: "tpcds",
            name: q.name.to_string(),
            sql: q.sql.to_string(),
            duckdb_exclusion: tpcds_duckdb_exclusion(&q),
            chdb_exclusion: oracle_exclusion(&q, Oracle::ClickHouse),
            sqlite_exclusion: oracle_exclusion(&q, Oracle::Sqlite),
            order_unchecked_review: order_unchecked_review("tpcds", q.name.as_ref()),
            empty_result_review: empty_result_review("tpcds", q.name.as_ref()),
        });
    }

    for q in get_clickbench_test_queries(None) {
        entries.push(InventoryEntry {
            suite: "clickbench",
            name: q.name.to_string(),
            sql: q.sql.to_string(),
            duckdb_exclusion: unanswerable(&q),
            chdb_exclusion: oracle_exclusion(&q, Oracle::ClickHouse),
            sqlite_exclusion: oracle_exclusion(&q, Oracle::Sqlite),
            order_unchecked_review: order_unchecked_review("clickbench", q.name.as_ref()),
            empty_result_review: empty_result_review("clickbench", q.name.as_ref()),
        });
    }

    for q in get_chbench_test_queries(None) {
        entries.push(InventoryEntry {
            suite: "chbench",
            name: q.name.to_string(),
            sql: q.sql.to_string(),
            duckdb_exclusion: None,
            chdb_exclusion: oracle_exclusion(&q, Oracle::ClickHouse),
            sqlite_exclusion: oracle_exclusion(&q, Oracle::Sqlite),
            order_unchecked_review: order_unchecked_review("chbench", q.name.as_ref()),
            empty_result_review: empty_result_review("chbench", q.name.as_ref()),
        });
    }

    for q in ssb_queries() {
        entries.push(InventoryEntry {
            suite: "ssb",
            name: q.name.to_string(),
            sql: q.sql.to_string(),
            duckdb_exclusion: None,
            chdb_exclusion: Some(
                "SSB multi-table star-schema SQL is covered vs DuckDB and SQLite; \
                 chDB runs SQLLancer + micro",
            ),
            sqlite_exclusion: None,
            order_unchecked_review: order_unchecked_review("ssb", q.name.as_ref()),
            empty_result_review: empty_result_review("ssb", q.name.as_ref()),
        });
    }

    // SpiceBench SF1 scenario is TPC-H (spiceai/spicebench built-in scenario).
    for q in get_tpch_test_queries(None) {
        let name = q.name.replacen("tpch_", "spicebench_", 1);
        let sq = Query::new(name.clone().into(), Arc::clone(&q.sql), false);
        let review = order_unchecked_review("spicebench", &name);
        let empty_review = empty_result_review("spicebench", &name);
        entries.push(InventoryEntry {
            suite: "spicebench",
            name,
            sql: q.sql.to_string(),
            duckdb_exclusion: tpch_duckdb_exclusion(&sq).or(tpch_duckdb_exclusion(&q)),
            chdb_exclusion: Some(SPICEBENCH_IS_TPCH),
            sqlite_exclusion: Some(SPICEBENCH_IS_TPCH),
            order_unchecked_review: review,
            empty_result_review: empty_review,
        });
    }

    for q in sqllancer_queries() {
        entries.push(InventoryEntry {
            suite: "sqllancer",
            name: q.name.to_string(),
            sql: q.sql.to_string(),
            duckdb_exclusion: None,
            chdb_exclusion: sqllancer_chdb_exclusion(&q),
            sqlite_exclusion: sqllancer_sqlite_exclusion(&q),
            order_unchecked_review: order_unchecked_review("sqllancer", q.name.as_ref()),
            empty_result_review: empty_result_review("sqllancer", q.name.as_ref()),
        });
    }

    for q in micro_bench_queries() {
        entries.push(InventoryEntry {
            suite: "micro",
            name: q.name.to_string(),
            sql: q.sql.to_string(),
            duckdb_exclusion: None,
            chdb_exclusion: None,
            sqlite_exclusion: None,
            order_unchecked_review: order_unchecked_review("micro", q.name.as_ref()),
            empty_result_review: empty_result_review("micro", q.name.as_ref()),
        });
    }

    entries
}

/// SpiceBench's standalone-oracle cells would repeat TPC-H's exactly.
const SPICEBENCH_IS_TPCH: &str = "SpiceBench's SF1 scenario is TPC-H: the same SQL over the same \
     rows, compared against this engine in the TPC-H lane";

/// Why a suite query is not compared against a standalone oracle — a property
/// of the SQL, checkable by running it.
///
/// First the reasons that hold for every engine ([`unanswerable`]), then the
/// constructs the oracle has no faithful spelling for, which the dialect
/// translator names ([`untranslatable`]). Nothing here is a blanket per-suite
/// reason: a query that translates is compared.
fn oracle_exclusion(q: &Query, oracle: Oracle) -> Option<&'static str> {
    unanswerable(q).or_else(|| untranslatable(&q.sql, oracle))
}

/// Queries with no single answer to compare, or that Cayenne cannot run at all.
fn unanswerable(q: &Query) -> Option<&'static str> {
    match q.name.as_ref() {
        "tpch_simple_q3" => Some(
            "ORDER BY l_linenumber DESC LIMIT 10 keeps 10 of the thousands of rows tied at the \
             top line number, and the result does not return the sort key: any 10 of them are \
             the answer",
        ),
        "tpch_simple_q6" | "tpch_simple_q7" | "clickbench_q18" => Some(
            "LIMIT without ORDER BY: which rows come back is unspecified, so no two engines \
             need agree",
        ),
        "clickbench_q30" => Some(
            "Cayenne cannot plan it: DataFusion's simplify_expressions rewrites the 90 \
             `SUM(\"ResolutionWidth\" + n)` columns into duplicate field names and fails",
        ),
        _ => None,
    }
}

fn sqllancer_chdb_exclusion(q: &Query) -> Option<&'static str> {
    match q.name.as_ref() {
        // ClickHouse NULL semantics in MIN/aggregates and three-valued logic
        // differ from SQL standard / DataFusion for some scalar subqueries.
        "sl_subquery_scalar" => Some(
            "chDB NULL/MIN three-valued logic differs from DataFusion on scalar subquery filter",
        ),
        _ => None,
    }
}

fn sqllancer_sqlite_exclusion(q: &Query) -> Option<&'static str> {
    // SQLite lacks several DataFusion/Postgres scalar functions / clauses.
    let sql = q.sql.to_ascii_lowercase();
    if sql.contains("regexp_match")
        || sql.contains("date_trunc")
        || sql.contains("make_date")
        || sql.contains("arrow_cast")
        || sql.contains("extract(")
        || sql.contains("nulls last")
        || sql.contains("nulls first")
    {
        Some("SQLLancer query uses DataFusion-only SQL not supported by SQLite")
    } else {
        None
    }
}

fn tpcds_duckdb_exclusion(q: &Query) -> Option<&'static str> {
    // Both queries name a column that two relations in scope expose, and leave it
    // unqualified. DuckDB's binder rejects the ambiguity; DataFusion resolves it.
    // A property of the SQL, checkable by running either query against DuckDB — not
    // a disagreement about results.
    match q.name.as_ref() {
        "tpcds_q58" => Some(
            "TPC-DS q58 orders by an unqualified `item_id` that both the `ss_items` and \
             `cs_items` subqueries expose; DuckDB's binder rejects the ambiguous reference",
        ),
        "tpcds_q72" => Some(
            "TPC-DS q72 references an unqualified `d_week_seq` that both the `d1` and `d2` \
             aliases of date_dim expose; DuckDB's binder rejects the ambiguous reference",
        ),
        _ => None,
    }
}

/// A TPC-H (or SpiceBench) query's DuckDB exclusion: the ones no engine can be
/// compared on. `simple_q4`'s `ORDER BY … LIMIT` ties are left in: the compare
/// path checks only the sort keys of a tie group a `LIMIT` cuts.
fn tpch_duckdb_exclusion(q: &Query) -> Option<&'static str> {
    let tpch_name = q.name.replacen("spicebench_", "tpch_", 1);
    unanswerable(&Query::new(tpch_name.into(), Arc::clone(&q.sql), false))
}

/// Assert inventory is complete relative to suite sources.
pub fn assert_inventory_complete() {
    let inv = build_inventory();
    let inv_names: std::collections::BTreeSet<_> =
        inv.iter().map(|e| (e.suite, e.name.as_str())).collect();

    for q in get_tpch_test_queries(None) {
        assert!(
            inv_names.contains(&("tpch", q.name.as_ref())),
            "inventory missing TPC-H query {}",
            q.name
        );
        let sb = q.name.replacen("tpch_", "spicebench_", 1);
        assert!(
            inv_names.contains(&("spicebench", sb.as_str())),
            "inventory missing SpiceBench query {sb}"
        );
    }
    for q in get_tpcds_test_queries(None, Some(1.0)) {
        assert!(
            inv_names.contains(&("tpcds", q.name.as_ref())),
            "inventory missing TPC-DS query {}",
            q.name
        );
    }
    for q in get_clickbench_test_queries(None) {
        assert!(
            inv_names.contains(&("clickbench", q.name.as_ref())),
            "inventory missing ClickBench query {}",
            q.name
        );
    }
    for q in get_chbench_test_queries(None) {
        assert!(
            inv_names.contains(&("chbench", q.name.as_ref())),
            "inventory missing CH-benCHmark query {}",
            q.name
        );
    }
    for q in ssb_queries() {
        assert!(
            inv_names.contains(&("ssb", q.name.as_ref())),
            "inventory missing SSB query {}",
            q.name
        );
    }
    for q in sqllancer_queries() {
        assert!(
            inv_names.contains(&("sqllancer", q.name.as_ref())),
            "inventory missing SQLLancer query {}",
            q.name
        );
    }
    for q in micro_bench_queries() {
        assert!(
            inv_names.contains(&("micro", q.name.as_ref())),
            "inventory missing micro query {}",
            q.name
        );
    }
}

#[must_use]
pub fn inventory_by_suite() -> BTreeMap<&'static str, Vec<String>> {
    let mut map: BTreeMap<&'static str, Vec<String>> = BTreeMap::new();
    for e in build_inventory() {
        map.entry(e.suite).or_default().push(e.name);
    }
    map
}
