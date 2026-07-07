/*
Copyright 2024-2026 The Spice.ai OSS Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

use std::path::PathBuf;

use clap::Parser;

use super::DatasetTestArgs;

/// Arguments for the HTAP (Hybrid Transactional/Analytical Processing) test.
///
/// Runs a TPC-C OLTP workload against the source
/// concurrently with CH-benCH analytical queries against spiced,
/// measuring analytical query freshness under write load.
///
/// CH-benCH convention: `--scale-factor` maps to warehouses (SF1 = 1 warehouse
/// ≈ 100 MB seed data). The TPC-C terminal count defaults to `warehouses * 10`,
/// matching the spec's requirement of 10 terminals per warehouse.
/// Use `--terminals` to override.
#[derive(Parser, Debug, Clone)]
pub struct HtapArgs {
    #[command(flatten)]
    pub(crate) test_args: DatasetTestArgs,

    /// Override the number of concurrent OLTP terminals (default: `scale_factor` * 10).
    #[arg(long)]
    pub(crate) terminals: Option<usize>,

    /// Target OLTP transaction rate for the OLTP workload. Omit for unlimited (maximum-throughput).
    #[arg(long)]
    pub(crate) rate: Option<u32>,

    /// Seed the source (schema + data) and exit, WITHOUT starting spiced or
    /// running the workload. Used to materialise a pristine source that an
    /// external harness can snapshot (e.g. into a Postgres template database)
    /// for fast reuse across runs. Mutually exclusive with `--skip-prepare`.
    #[arg(long, conflicts_with = "skip_prepare")]
    pub(crate) prepare_only: bool,

    /// Skip seeding the source: connect to an already-prepared source and run
    /// the workload directly. Use when the harness has pre-populated the source
    /// (e.g. restored it from a template) so the ~minutes-to-an-hour seed is not
    /// repeated. Mutually exclusive with `--prepare-only`.
    #[arg(long, conflicts_with = "prepare_only")]
    pub(crate) skip_prepare: bool,

    /// Write the full scraped metrics time-series plus run metadata (commit,
    /// config) to this path as JSON when the run completes. This is the durable,
    /// machine-readable artifact the `scripts/chbench-waterfall.py` backpressure
    /// analysis consumes; CI uploads it as a workflow artifact.
    #[arg(long)]
    pub(crate) metrics_dump: Option<PathBuf>,

    /// Fail the run if any changes-mode table's apply-phase coverage (instrumented
    /// write-phase time ÷ apply-burst wall time) falls below this fraction (0.0–1.0).
    /// A low ratio means a CDC apply bottleneck hides in un-instrumented code. Default
    /// 0.0 = report only (no gate); set e.g. 0.85 on the HTAP smoke to catch regressions.
    #[arg(long, default_value_t = 0.0, value_parser = parse_phase_coverage)]
    pub(crate) min_phase_coverage: f64,
}

/// Parse and validate `--min-phase-coverage`: a fraction in the inclusive range
/// `0.0..=1.0`. clap only provides ranged value parsers for integer types, so
/// float bounds are enforced here.
fn parse_phase_coverage(value: &str) -> Result<f64, String> {
    let parsed: f64 = value
        .parse()
        .map_err(|_| format!("`{value}` is not a valid number"))?;
    if (0.0..=1.0).contains(&parsed) {
        Ok(parsed)
    } else {
        Err(format!(
            "phase coverage must be between 0.0 and 1.0 (inclusive), got {parsed}"
        ))
    }
}
