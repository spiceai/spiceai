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

//! # Result correctness — STATIC comparison-cell census
//!
//! Not a performance test, and not a gate: it asserts nothing about engines. It
//! reports the suite's **planned** coverage — the cells the inventory does not
//! statically exclude.
//!
//! The inventory test prints per-suite **query** counts. A query only tests
//! something when it is executed on two engines and the results compared, so the
//! honest unit is the **comparison cell** — (query, engine). This binary counts
//! those from the inventory's static exclusion fields.
//!
//! ## What this CANNOT tell you
//!
//! **A cell counted here as "not statically excluded" is an upper bound, not a
//! comparison that happened.** This binary links no engines and reads no
//! `ParityOutcome`, and the test binaries create `Excluded` at *run time* for
//! cases the inventory never sees:
//!
//! - a DuckDB parser/syntax rejection (`result_correctness_vs_duckdb_test.rs`,
//!   the `(Ok, Err)` arm)
//! - both engines erroring on the same query (the `(Err, Err)` arm — "both
//!   engines error")
//! - **a Cayenne/DuckDB result disagreement that is reclassified as `Excluded`
//!   whenever Cayenne matches the DataFusion baseline** (the `parquet_dir` arm)
//!
//! So read this number as the **static ceiling**, and get observed outcomes from
//! an actual run via `support::report::{write_coverage_report, summary_line}`,
//! which count real `ParityOutcome`s including runtime exclusions. Reporting a
//! census delta without the run report overstates coverage.
//!
//! Links no engines (reads `build_inventory()` only), so it is cheap to run first.
//!
//! ```bash
//! cargo test -p cayenne --test result_correctness_census_test -- --nocapture
//! ```
//!
//! Install: copy to `crates/cayenne/tests/result_correctness_census_test.rs` and add
//!
//! ```toml
//! [[test]]
//! name = "result_correctness_census_test"
//! path = "tests/result_correctness_census_test.rs"
//! ```
//!
//! See `tests/correctness/README.md`.

// The workspace denies `clippy::pedantic` and `clippy::allow_attributes`, so these
// live at crate level as inner attributes — an item-level `#[allow]` fails the gate.
// Run `make lint-rust` after installing; this repo's lint gate has rejected new test
// files for exactly these lints before.
#![allow(clippy::expect_used)]
#![allow(clippy::unwrap_used)]
#![allow(clippy::doc_markdown)]
#![allow(clippy::cast_precision_loss)]

#[path = "correctness/support/mod.rs"]
mod support;

use std::collections::BTreeMap;

use support::inventory::{InventoryEntry, build_inventory};
use test_framework::queries::validation::{has_top_level_limit, has_top_level_order_by};

/// Engines the inventory carries an exclusion field for.
const ENGINES: [&str; 3] = ["duckdb", "sqlite", "chdb"];

/// Load modes each suite exercises today.
///
/// `SKILL.md` defines a comparison cell as (query, engine, load mode), but the
/// inventory models only the first two — so a query x engine count cannot move
/// when a suite gains a load mode, and on its own it cannot measure expansion
/// axis 2. This table supplies the third dimension.
///
/// **Maintained by hand against the test binaries** (`InsertOp::Overwrite`,
/// repeated `InsertOp::Append`, `write_cdc_append_stream` + `finish()`). Update it
/// when a suite gains a mode, or the axis-2 delta it reports goes stale. Today
/// only CH-benCHmark runs the matrix.
const SUITE_LOAD_MODES: &[(&str, &[&str])] = &[("chbench", &["full", "append", "changes"])];

/// Every suite not named in `SUITE_LOAD_MODES` loads exactly one way.
const DEFAULT_LOAD_MODES: &[&str] = &["full"];

fn load_modes_for(suite: &str) -> &'static [&'static str] {
    SUITE_LOAD_MODES
        .iter()
        .find(|(s, _)| *s == suite)
        .map_or(DEFAULT_LOAD_MODES, |(_, modes)| *modes)
}

fn exclusion_for(entry: &InventoryEntry, engine: &str) -> Option<&'static str> {
    match engine {
        "duckdb" => entry.duckdb_exclusion,
        "sqlite" => entry.sqlite_exclusion,
        "chdb" => entry.chdb_exclusion,
        _ => panic!("unknown engine {engine}"),
    }
}

/// Compared / excluded counts for one bucket.
#[derive(Default, Clone, Copy)]
struct Cells {
    compared: usize,
    excluded: usize,
}

impl Cells {
    fn total(self) -> usize {
        self.compared + self.excluded
    }

    /// Share of cells in this bucket not statically excluded.
    fn pct(self) -> f64 {
        if self.total() == 0 {
            0.0
        } else {
            self.compared as f64 / self.total() as f64 * 100.0
        }
    }
}

#[test]
fn print_comparison_cell_census() {
    let inv = build_inventory();

    let mut by_suite: BTreeMap<&'static str, BTreeMap<&'static str, Cells>> = BTreeMap::new();
    let mut by_engine: BTreeMap<&'static str, Cells> = BTreeMap::new();
    let mut reasons: BTreeMap<(&'static str, &'static str), usize> = BTreeMap::new();

    for entry in &inv {
        for engine in ENGINES {
            let cell = by_suite
                .entry(entry.suite)
                .or_default()
                .entry(engine)
                .or_default();
            let totals = by_engine.entry(engine).or_default();

            match exclusion_for(entry, engine) {
                None => {
                    cell.compared += 1;
                    totals.compared += 1;
                }
                Some(reason) => {
                    cell.excluded += 1;
                    totals.excluded += 1;
                    *reasons.entry((engine, reason)).or_default() += 1;
                }
            }
        }
    }

    let queries = inv.len();
    let total: usize = by_engine.values().map(|c| c.total()).sum();
    let compared: usize = by_engine.values().map(|c| c.compared).sum();
    let overall = Cells {
        compared,
        excluded: total - compared,
    };

    println!("\n## Comparison-cell census (STATIC — inventory exclusions only)\n");
    println!("Queries in inventory: **{queries}**");
    println!("Engine lanes: **{}**", ENGINES.len());
    println!("Comparison cells (query x engine): **{total}**");
    println!(
        "Cells not statically excluded: **{compared}** ({:.1}%)",
        overall.pct()
    );
    println!("Cells statically excluded: **{}**", overall.excluded);
    println!(
        "\n> Upper bound. Runtime exclusions (parser rejects, both-engine errors, and\n> Cayenne-matches-DataFusion disagreements) are NOT counted here — take observed\n> outcomes from a run via support::report::summary_line.\n"
    );

    println!("### Cells not statically excluded, per suite\n");
    print!("| suite | queries |");
    for engine in ENGINES {
        print!(" {engine} |");
    }
    println!(" not-excluded % |");
    print!("|---|---|");
    for _ in ENGINES {
        print!("---|");
    }
    println!("---|");

    for (suite, engines) in &by_suite {
        let suite_queries = inv.iter().filter(|e| e.suite == *suite).count();
        print!("| {suite} | {suite_queries} |");
        let mut roll = Cells::default();
        for engine in ENGINES {
            let c = engines.get(engine).copied().unwrap_or_default();
            roll.compared += c.compared;
            roll.excluded += c.excluded;
            print!(" {}/{} |", c.compared, c.total());
        }
        println!(" {:.0}% |", roll.pct());
    }

    println!("\n### Per-engine totals\n");
    println!("| engine | not statically excluded | statically excluded | % |");
    println!("|---|---|---|---|");
    for engine in ENGINES {
        let c = by_engine.get(engine).copied().unwrap_or_default();
        println!(
            "| {engine} | {} | {} | {:.0}% |",
            c.compared,
            c.excluded,
            c.pct()
        );
    }

    // Axis 2 lives in a dimension the inventory does not model, so it gets its own
    // report — the query x engine total above is fixed under a load-mode change.
    println!("\n### Load-mode cells (axis 2)\n");
    println!("| suite | modes | query x engine | query x engine x mode |");
    println!("|---|---|---|---|");
    let mut mode_cells_total = 0usize;
    let mut flat_cells_total = 0usize;
    for (suite, engines) in &by_suite {
        let modes = load_modes_for(suite);
        let flat: usize = engines.values().map(|c| c.compared).sum();
        let with_modes = flat * modes.len();
        mode_cells_total += with_modes;
        flat_cells_total += flat;
        println!("| {suite} | {} | {flat} | {with_modes} |", modes.join("/"));
    }
    println!("| **total** | | **{flat_cells_total}** | **{mode_cells_total}** |");
    println!(
        "\n> Load modes come from a hand-maintained table in this file, not the\n> inventory. Only CH-benCHmark runs full/append/changes today; every other\n> suite loads one way, so the two totals differ only by that suite.\n"
    );

    // Row order is a third thing a cell can check. Content compared as a multiset
    // canonically sorts both sides first, so for a query without a LIMIT it says
    // nothing about the order an engine returned — a wrong sort over the right rows
    // compared equal. `compare_query_result_batches_with_sort_check` verifies each
    // side against its own ORDER BY, which reaches every query in the first column;
    // the second is the subset that was already order-sensitive without it.
    println!("\n### Row-order coverage\n");
    let mut ordered = 0usize;
    let mut ordered_with_limit = 0usize;
    let mut by_suite_ordered: BTreeMap<&'static str, (usize, usize)> = BTreeMap::new();
    for entry in &inv {
        if !has_top_level_order_by(&entry.sql) {
            continue;
        }
        ordered += 1;
        let counts = by_suite_ordered.entry(entry.suite).or_default();
        counts.0 += 1;
        // Content is compared positionally only when a LIMIT makes the row set
        // itself order-dependent. Without the sort check, everything else had its
        // order compared not at all. Parser-backed, so this matches the predicate
        // the comparison itself uses rather than a substring search that would also
        // fire on a LIMIT inside a subquery.
        if has_top_level_limit(&entry.sql) {
            ordered_with_limit += 1;
            counts.1 += 1;
        }
    }
    println!(
        "| suite | top-level ORDER BY | also compared positionally (+ LIMIT) | order checked by the sort check alone |"
    );
    println!("|---|---|---|---|");
    for (suite, (total_ordered, with_limit)) in &by_suite_ordered {
        println!(
            "| {suite} | {total_ordered} | {with_limit} | {} |",
            total_ordered - with_limit
        );
    }
    println!(
        "| **total** | **{ordered}** | **{ordered_with_limit}** | **{}** |",
        ordered - ordered_with_limit
    );
    println!(
        "\n> Counted per query, not per cell: the sort check is a self-check on one\n> engine's output, so it runs on every engine lane the query reaches.\n"
    );

    // The ranking that picks the expansion axis: one reason blocking many cells is
    // one dialect shim away from becoming that many cells of real coverage.
    println!("\n### Static exclusion reasons by cells blocked\n");
    let mut ranked: Vec<_> = reasons.into_iter().collect();
    ranked.sort_by_key(|&(_, count)| std::cmp::Reverse(count));
    println!("| cells | engine | reason |");
    println!("|---|---|---|");
    for ((engine, reason), count) in ranked {
        let flat = reason.split_whitespace().collect::<Vec<_>>().join(" ");
        println!("| {count} | {engine} | {flat} |");
    }
    println!();
}
