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

use clap::{Parser, ValueEnum};

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

    /// Write a human-readable Markdown results summary (headline tpmC, QPH, worst
    /// replication lag, and the per-query latency table) to this path when the run
    /// completes. CI appends it to the GitHub Actions job summary so the headline
    /// results are visible without opening the run log.
    #[arg(long)]
    pub(crate) summary_out: Option<PathBuf>,

    /// Whether to record the `EXPLAIN` plan of every analytical query as an
    /// insta snapshot (`{spicepod}_{query}_explain_sf{scale_factor}`) - the
    /// capture `run bench` always performs but an HTAP run does not.
    ///
    /// Opt-in rather than always-on for the reason results snapshots are off
    /// here: the run is under concurrent OLTP load. A plan is far more stable
    /// than a result set, but it is not guaranteed stable - accelerator
    /// statistics move as rows land, so a join order can legitimately differ
    /// between two healthy runs and a committed snapshot would then fail.
    /// Enable it to capture the plan for a specific investigation (e.g. why one
    /// query exhausts the query memory pool at a high scale factor), where the
    /// plan is the artifact and a mismatch is the signal rather than a flake.
    ///
    /// This records the plan only. Per-operator *actual* cardinalities and
    /// timings need `EXPLAIN ANALYZE`, whose output is by construction not
    /// snapshot-stable; that capture is the sampling probe behind the
    /// workflow's `collect_diagnostics` input (`scripts/htap-explain-probe.sh`).
    #[arg(long, value_enum, default_value_t = ExplainPlans::Off)]
    pub(crate) explain_plans: ExplainPlans,

    /// Fail the run if any changes-mode table's apply-phase coverage (instrumented
    /// write-phase time ÷ apply-burst wall time) falls below this fraction (0.0–1.0).
    /// A low ratio means a CDC apply bottleneck hides in un-instrumented code. Default
    /// 0.0 = report only (no gate); set e.g. 0.85 on the HTAP smoke to catch regressions.
    #[arg(long, default_value_t = 0.0, value_parser = parse_phase_coverage)]
    pub(crate) min_phase_coverage: f64,

    /// Skip the analytical-query correctness gate (re-running every CH-benCH
    /// analytical query against both source and Spice and diffing results).
    /// It is the most expensive correctness check and is not part of the
    /// measurement, so experiments that only care about tpmC/QPH/lag can skip
    /// it to save time; the row-count gate (replication drain + per-table
    /// parity) always still runs. Default `false` (gate runs) to keep data
    /// correctness the default — scheduled/monitoring runs should never set
    /// this.
    #[arg(long, default_value_t = false)]
    pub(crate) skip_analytic_gate: bool,

    /// Number of analytical-query-gate queries to evaluate concurrently. Each
    /// worker runs its query's source and Spice sides in parallel, so the gate
    /// keeps up to this many queries in flight against each engine. Bounded to
    /// at least 1 and never more than the query count. Raise it to shorten the
    /// gate on fast sources; lower it to ease load on a small source connection
    /// pool. Default 4.
    #[arg(long, default_value_t = 4)]
    pub(crate) analytic_gate_concurrency: usize,
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

/// Whether an HTAP run records analytical-query `EXPLAIN` plan snapshots.
///
/// An enum rather than a flag so the capture can name a third mode (an
/// `analyze` variant writing non-snapshot artifacts) without changing the
/// existing surface.
#[derive(ValueEnum, Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ExplainPlans {
    /// Record no plans (default): keeps the run's query stream to the
    /// measured queries alone.
    #[default]
    #[value(name = "off")]
    Off,
    /// Record one `EXPLAIN` snapshot per analytical query.
    #[value(name = "explain")]
    Explain,
}
