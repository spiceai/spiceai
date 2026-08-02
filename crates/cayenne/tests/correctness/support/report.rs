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
    let mut excluded = 0usize;
    let mut fail = 0usize;
    let mut engine_err = 0usize;

    for r in results {
        let (status, detail) = match &r.outcome {
            ParityOutcome::Pass => {
                pass += 1;
                ("PASS", String::new())
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
    writeln!(md, "- excluded (justified): {excluded}").ok();
    writeln!(md, "- fail: {fail}").ok();
    writeln!(md, "- engine_error: {engine_err}").ok();
    writeln!(
        md,
        "- total reported: {}",
        pass + excluded + fail + engine_err
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

/// Format a short console summary.
#[must_use]
pub fn summary_line(results: &[RunResult]) -> String {
    let pass = results
        .iter()
        .filter(|r| matches!(r.outcome, ParityOutcome::Pass))
        .count();
    let excluded = results
        .iter()
        .filter(|r| matches!(r.outcome, ParityOutcome::Excluded { .. }))
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
        "correctness summary: pass={pass} excluded={excluded} fail={fail} total={}",
        results.len()
    )
}
