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
            chdb_exclusion: Some(
                "TPC-H multi-table SQL targets DataFusion/DuckDB dialect; \
                 chDB runs SQLLancer + micro on all three engines",
            ),
            sqlite_exclusion: Some(
                "TPC-H SQL uses DataFusion/DuckDB dialect (EXTRACT, INTERVAL, …); \
                 SQLite lane covers SSB + SQLLancer + micro",
            ),
        });
    }

    for q in get_tpcds_test_queries(None, Some(1.0)) {
        entries.push(InventoryEntry {
            suite: "tpcds",
            name: q.name.to_string(),
            sql: q.sql.to_string(),
            duckdb_exclusion: tpcds_duckdb_exclusion(&q),
            chdb_exclusion: Some(
                "TPC-DS multi-table SQL targets DataFusion/DuckDB dialect; \
                 chDB runs SQLLancer + micro on all three engines",
            ),
            sqlite_exclusion: Some(
                "TPC-DS SQL targets DataFusion/DuckDB dialect; \
                 SQLite lane covers SSB + SQLLancer + micro",
            ),
        });
    }

    for q in get_clickbench_test_queries(None) {
        entries.push(InventoryEntry {
            suite: "clickbench",
            name: q.name.to_string(),
            sql: q.sql.to_string(),
            duckdb_exclusion: None,
            chdb_exclusion: Some(
                "ClickBench full hits loaded in DuckDB lane; chDB runs SQLLancer + micro",
            ),
            sqlite_exclusion: Some(
                "ClickBench hits schema/SQL surface is DataFusion-oriented; \
                 SQLite lane covers SSB + SQLLancer + micro",
            ),
        });
    }

    for q in get_chbench_test_queries(None) {
        entries.push(InventoryEntry {
            suite: "chbench",
            name: q.name.to_string(),
            sql: q.sql.to_string(),
            duckdb_exclusion: None,
            chdb_exclusion: Some(
                "CH-benCHmark multi-table TPC-C/H hybrid SQL targets DataFusion/DuckDB dialect",
            ),
            sqlite_exclusion: Some(
                "CH-benCHmark SQL uses mod()/dialect forms; SQLite lane covers SSB + SQLLancer + micro",
            ),
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
        });
    }

    // SpiceBench SF1 scenario is TPC-H (spiceai/spicebench built-in scenario).
    for q in get_tpch_test_queries(None) {
        let name = q.name.replacen("tpch_", "spicebench_", 1);
        let sq = Query::new(name.clone().into(), Arc::clone(&q.sql), false);
        entries.push(InventoryEntry {
            suite: "spicebench",
            name,
            sql: q.sql.to_string(),
            duckdb_exclusion: tpch_duckdb_exclusion(&sq).or(tpch_duckdb_exclusion(&q)),
            chdb_exclusion: Some(
                "SpiceBench SF1 scenario is TPC-H; chDB dialect exclusion same as TPC-H suite",
            ),
            sqlite_exclusion: Some(
                "SpiceBench SF1 is TPC-H dialect; SQLite lane covers SSB + SQLLancer + micro",
            ),
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
        });
    }

    entries
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

fn tpch_duckdb_exclusion(q: &Query) -> Option<&'static str> {
    match q.name.as_ref() {
        "tpch_simple_q3" | "tpch_simple_q4" | "spicebench_simple_q3" | "spicebench_simple_q4" => {
            Some(
                "ORDER BY non-unique key + LIMIT yields engine-dependent tied-row sets; \
                 not a content correctness defect",
            )
        }
        "tpch_simple_q6" | "tpch_simple_q7" | "spicebench_simple_q6" | "spicebench_simple_q7" => {
            Some(
                "LIMIT without ORDER BY is nondeterministic across engines and scale factors; \
                 not a content correctness defect",
            )
        }
        _ => None,
    }
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
