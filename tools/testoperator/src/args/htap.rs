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

    /// After the workload finishes — while spiced is still running with the
    /// fully-loaded acceleration in memory — capture per-query EXPLAIN /
    /// EXPLAIN ANALYZE and the eager-aggregation decline map into this
    /// directory (`explain_structure.txt`, `explain_analyze.txt`,
    /// `decline_map.txt`, `spiced.log`).
    ///
    /// This runs against the live, hot instance, so the plans and the rule's
    /// accept/decline decisions reflect the real benchmarked dataset — unlike
    /// re-launching a fresh spiced, which starts with empty (not-yet-replicated)
    /// accelerated tables. Best-effort; never fails the run. For the decline
    /// reasons to appear, spiced must log the rule — set
    /// `SPICED_LOG=info,eager_aggregation=debug` (and `SPICED_EAGER_AGGREGATION=1`).
    #[arg(long, value_name = "DIR")]
    pub(crate) capture_explain: Option<PathBuf>,
}
