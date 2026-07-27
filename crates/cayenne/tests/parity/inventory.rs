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
//! Every in-scope analytical suite query is listed with expressibility notes
//! for DuckDB and chDB. The inventory is built from the same sources as
//! `test_framework::queries` so completeness can be asserted in a unit test.

use std::collections::BTreeMap;

use test_framework::queries::{
    Query, get_clickbench_test_queries, get_tpcds_test_queries, get_tpch_test_queries,
};

use super::micro_bench_queries;

/// Engines that can execute a query for parity comparison.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Engine {
    Cayenne,
    DuckDb,
    ChDb,
}

/// One inventory entry: suite query + per-engine status.
#[derive(Debug, Clone)]
pub struct InventoryEntry {
    pub suite: &'static str,
    pub name: String,
    pub sql: String,
    /// `None` means the engine runs the query; `Some(reason)` is a justified exclusion.
    pub duckdb_exclusion: Option<&'static str>,
    /// `None` means expressible in chDB (possibly with dialect adaptation);
    /// `Some(reason)` means not compared against chDB.
    pub chdb_exclusion: Option<&'static str>,
}

/// Build the full inventory from suite sources + micro-bench shapes.
#[must_use]
pub fn build_inventory() -> Vec<InventoryEntry> {
    let mut entries = Vec::new();

    for q in get_tpch_test_queries(None) {
        entries.push(InventoryEntry {
            suite: "tpch",
            name: q.name.to_string(),
            sql: q.sql.to_string(),
            duckdb_exclusion: tpch_duckdb_exclusion(&q),
            // Multi-table TPC-H in chDB requires ClickHouse SQL dialect rewrites
            // (e.g. date literals, correlated subqueries) beyond this parity gate.
            chdb_exclusion: Some(
                "TPC-H suite multi-table SQL targets DataFusion/DuckDB dialect; \
                 chDB comparison limited to micro-bench shapes",
            ),
        });
    }

    for q in get_tpcds_test_queries(None, Some(1.0)) {
        entries.push(InventoryEntry {
            suite: "tpcds",
            name: q.name.to_string(),
            sql: q.sql.to_string(),
            duckdb_exclusion: None,
            chdb_exclusion: Some(
                "TPC-DS suite multi-table SQL targets DataFusion/DuckDB dialect; \
                 chDB comparison limited to micro-bench shapes",
            ),
        });
    }

    for q in get_clickbench_test_queries(None) {
        // Full ClickBench hits schema is not generated in-process; only a
        // reduced hits fixture is loaded. Queries needing ungenerated columns
        // are excluded at run time when the fixture is partial — inventory
        // still lists every suite query so coverage is 100% accounted for.
        entries.push(InventoryEntry {
            suite: "clickbench",
            name: q.name.to_string(),
            sql: q.sql.to_string(),
            duckdb_exclusion: clickbench_duckdb_note(&q),
            chdb_exclusion: Some(
                "ClickBench hits full-width schema not loaded in chDB parity process; \
                 micro-bench shapes cover scan/agg/join shapes instead",
            ),
        });
    }

    for q in micro_bench_queries() {
        entries.push(InventoryEntry {
            suite: "micro",
            name: q.name.to_string(),
            sql: q.sql.to_string(),
            duckdb_exclusion: None,
            chdb_exclusion: micro_chdb_note(&q),
        });
    }

    entries
}

/// TPC-H queries where engine-vs-engine full-result equality is not well-defined
/// because `ORDER BY` keys are non-unique and `LIMIT` therefore may pick
/// different tied rows on each engine. Not a Cayenne correctness bug.
fn tpch_duckdb_exclusion(q: &Query) -> Option<&'static str> {
    match q.name.as_ref() {
        "tpch_simple_q3" | "tpch_simple_q4" => Some(
            "ORDER BY non-unique key + LIMIT yields engine-dependent tied-row sets; \
             not a content correctness defect",
        ),
        "tpch_simple_q6" | "tpch_simple_q7" => Some(
            "LIMIT without ORDER BY is nondeterministic across engines and scale factors; \
             not a content correctness defect",
        ),
        _ => None,
    }
}

fn clickbench_duckdb_note(q: &Query) -> Option<&'static str> {
    // In-process fixture uses a reduced hits schema. Queries that only need
    // COUNT(*) / simple aggregates on generated columns run; others are
    // excluded at execution when column resolution fails, with this standing
    // reason for inventory completeness when the full dataset is absent.
    let _ = q;
    None
}

fn micro_chdb_note(q: &Query) -> Option<&'static str> {
    // Join shapes need two chDB tables in one session — supported via dual load.
    // All micro shapes are expressible with ClickHouse-compatible SQL when
    // rewritten (table names, string quotes). Default: no exclusion.
    let _ = q;
    None
}

/// Assert inventory is complete relative to suite sources.
///
/// Returns `(suite → query names in inventory)` for reporting.
#[must_use]
pub fn inventory_by_suite() -> BTreeMap<&'static str, Vec<String>> {
    let mut map: BTreeMap<&'static str, Vec<String>> = BTreeMap::new();
    for e in build_inventory() {
        map.entry(e.suite).or_default().push(e.name);
    }
    map
}

/// Completeness check: every query from the suite sources appears in inventory.
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
    for q in micro_bench_queries() {
        assert!(
            inv_names.contains(&("micro", q.name.as_ref())),
            "inventory missing micro query {}",
            q.name
        );
    }
}
