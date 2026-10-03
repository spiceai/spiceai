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

//! Markdown coverage report writer for parity runs.

use std::fmt::Write as _;
use std::path::Path;

use super::ParityOutcome;
use super::inventory::{InventoryEntry, build_inventory};

/// One executed comparison result tied to an inventory query.
#[derive(Debug, Clone)]
pub struct RunResult {
    pub suite: String,
    pub name: String,
    pub engine_pair: &'static str,
    pub outcome: ParityOutcome,
}

/// The results a lane must not accept.
///
/// A `Pass` needs no explanation and an `Excluded` carries its own. An
/// `OrderUnchecked` is accepted only where the inventory names why that query's
/// `ORDER BY` cannot be verified against its own result columns — otherwise it
/// lands here, because an order nothing verified and nobody reviewed is exactly
/// what the sort check was added to stop passing quietly.
///
/// A `Vacuous` agreement — both sides answered with no value — is accepted only
/// where the inventory reviews that query as empty on the fixture `fixtures`
/// names for its suite, as `("tpcds", fixture::TPCDS_DSDGEN_SF1)`. A suite the
/// lane names no fixture for accepts no empty answer.
///
/// A reviewed name does not blanket the query: it accepts an unverified order,
/// not a violation or a content mismatch, both of which stay failures.
#[must_use]
pub fn unexplained<'a>(
    results: &'a [RunResult],
    inventory: &[InventoryEntry],
    fixtures: &[(&str, &str)],
) -> Vec<&'a RunResult> {
    results
        .iter()
        .filter(|r| {
            if r.outcome.is_pass_or_excluded() {
                return false;
            }
            // `chbench[append]` reviews as `chbench`.
            let suite = r.suite.split('[').next().unwrap_or(&r.suite);
            let entries = || {
                inventory
                    .iter()
                    .filter(|e| e.suite == suite && e.name == r.name)
            };
            match r.outcome {
                ParityOutcome::OrderUnchecked { .. } => {
                    !entries().any(|e| e.order_unchecked_review.is_some())
                }
                ParityOutcome::Vacuous { .. } => {
                    let fixture = fixtures
                        .iter()
                        .find(|(fixture_suite, _)| *fixture_suite == suite)
                        .map(|(_, fixture)| *fixture);
                    !fixture.is_some_and(|fixture| {
                        entries().any(|e| {
                            e.empty_result_review
                                .is_some_and(|review| review.covers(fixture))
                        })
                    })
                }
                _ => true,
            }
        })
        .collect()
}

/// Write a machine-readable + human coverage report.
pub fn write_coverage_report(path: &Path, results: &[RunResult]) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    let inventory = build_inventory();
    let mut md = String::new();
    writeln!(md, "# Result-correctness coverage").ok();
    writeln!(md).ok();
    writeln!(
        md,
        "> Correctness only — not a performance / Criterion benchmark report."
    )
    .ok();
    writeln!(md).ok();
    writeln!(
        md,
        "Engine roles: **standalone-*** = out-of-Spice oracle crates \
         (`duckdb`, `rusqlite`, `chdb-rust`); **spice-*** = Spice accelerators \
         (Cayenne, DuckDB accel, SQLite accel)."
    )
    .ok();
    writeln!(md).ok();
    writeln!(
        md,
        "Generated inventory size: **{}** queries across suites.",
        inventory.len()
    )
    .ok();
    writeln!(md).ok();

    writeln!(md, "## Inventory by suite").ok();
    writeln!(md).ok();
    let mut by_suite: std::collections::BTreeMap<&str, Vec<&InventoryEntry>> =
        std::collections::BTreeMap::new();
    for e in &inventory {
        by_suite.entry(e.suite).or_default().push(e);
    }
    for (suite, entries) in &by_suite {
        writeln!(md, "- **{suite}**: {} queries", entries.len()).ok();
    }
    writeln!(md).ok();

    writeln!(md, "## Run results").ok();
    writeln!(md).ok();
    writeln!(md, "| Suite | Query | Engine pair | Status | Detail |").ok();
    writeln!(md, "|-------|-------|-------------|--------|--------|").ok();

    let mut pass = 0usize;
    let mut vacuous = 0usize;
    let mut excluded = 0usize;
    let mut fail = 0usize;
    let mut engine_err = 0usize;
    let mut order_unchecked = 0usize;

    for r in results {
        let (status, detail) = match &r.outcome {
            ParityOutcome::Pass => {
                pass += 1;
                ("PASS", String::new())
            }
            ParityOutcome::OrderUnchecked { reasons } => {
                order_unchecked += 1;
                ("ORDER_UNCHECKED", reasons.join("; "))
            }
            ParityOutcome::Vacuous { detail } => {
                vacuous += 1;
                ("VACUOUS", detail.clone())
            }
            ParityOutcome::Excluded { reason } => {
                excluded += 1;
                ("EXCLUDED", reason.clone())
            }
            ParityOutcome::Fail { detail } => {
                fail += 1;
                ("FAIL", detail.clone())
            }
            ParityOutcome::EngineError { side, detail } => {
                engine_err += 1;
                ("ENGINE_ERROR", format!("{side}: {detail}"))
            }
        };
        let detail_esc = detail.replace('|', "\\|").replace('\n', " ");
        writeln!(
            md,
            "| {} | {} | {} | {} | {} |",
            r.suite, r.name, r.engine_pair, status, detail_esc
        )
        .ok();
    }

    writeln!(md).ok();
    writeln!(md, "## Summary").ok();
    writeln!(md).ok();
    writeln!(md, "- pass: {pass}").ok();
    writeln!(
        md,
        "- vacuous (both sides empty, nothing compared): {vacuous}"
    )
    .ok();
    writeln!(md, "- excluded (justified): {excluded}").ok();
    writeln!(
        md,
        "- order unchecked (content compared, sort not): {order_unchecked}"
    )
    .ok();
    writeln!(md, "- fail: {fail}").ok();
    writeln!(md, "- engine_error: {engine_err}").ok();
    writeln!(
        md,
        "- total reported: {}",
        pass + vacuous + excluded + order_unchecked + fail + engine_err
    )
    .ok();
    writeln!(md, "- inventory size: {}", inventory.len()).ok();
    writeln!(md).ok();

    // Full inventory dump for machine completeness checks.
    writeln!(md, "## Full inventory").ok();
    writeln!(md).ok();
    writeln!(
        md,
        "| Suite | Query | DuckDB exclusion | chDB exclusion | SQLite exclusion |"
    )
    .ok();
    writeln!(
        md,
        "|-------|-------|------------------|----------------|------------------|"
    )
    .ok();
    for e in &inventory {
        writeln!(
            md,
            "| {} | {} | {} | {} | {} |",
            e.suite,
            e.name,
            e.duckdb_exclusion.unwrap_or(""),
            e.chdb_exclusion.unwrap_or(""),
            e.sqlite_exclusion.unwrap_or(""),
        )
        .ok();
    }

    std::fs::write(path, md)
}

/// Write a lane's log under the scratch dir, print its summary, and fail on any
/// result [`unexplained`] does not accept. `fixtures` names the rows the lane
/// loaded for each suite, as [`unexplained`] takes them.
pub fn finish_lane(results: &[RunResult], fixtures: &[(&str, &str)], log_name: &str, header: &str) {
    let log_path = super::scratch_dir().join(log_name);
    let mut log = format!("{header}\n");
    for r in results {
        writeln!(log, "{}/{}: {:?}", r.suite, r.name, r.outcome).ok();
    }
    writeln!(log, "{}", summary_line(results)).ok();
    std::fs::write(&log_path, &log).unwrap_or_else(|e| panic!("write {}: {e}", log_path.display()));
    eprintln!("{header}: {}", summary_line(results));
    let failures = unexplained(results, &build_inventory(), fixtures);
    assert!(
        failures.is_empty(),
        "{header}: {} unexplained result(s): {failures:#?}\nsee {}",
        failures.len(),
        log_path.display()
    );
}

/// Format a short console summary.
#[must_use]
pub fn summary_line(results: &[RunResult]) -> String {
    let pass = results
        .iter()
        .filter(|r| matches!(r.outcome, ParityOutcome::Pass))
        .count();
    let vacuous = results
        .iter()
        .filter(|r| matches!(r.outcome, ParityOutcome::Vacuous { .. }))
        .count();
    let excluded = results
        .iter()
        .filter(|r| matches!(r.outcome, ParityOutcome::Excluded { .. }))
        .count();
    let order_unchecked = results
        .iter()
        .filter(|r| matches!(r.outcome, ParityOutcome::OrderUnchecked { .. }))
        .count();
    let fail = results
        .iter()
        .filter(|r| {
            matches!(
                r.outcome,
                ParityOutcome::Fail { .. } | ParityOutcome::EngineError { .. }
            )
        })
        .count();
    format!(
        "correctness summary: pass={pass} vacuous={vacuous} excluded={excluded} \
         order_unchecked={order_unchecked} fail={fail} total={}",
        results.len()
    )
}
