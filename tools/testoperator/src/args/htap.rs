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

use clap::Parser;

use super::DatasetTestArgs;

/// Arguments for the HTAP (Hybrid Transactional/Analytical Processing) test.
///
/// Runs a TPC-C OLTP workload against the source
/// concurrently with CH-benCH analytical queries against spiced,
/// measuring analytical query freshness under write load.
#[derive(Parser, Debug, Clone)]
pub struct HtapArgs {
    #[command(flatten)]
    pub(crate) test_args: DatasetTestArgs,

    /// Number of TPC-C warehouses (scale factor). Each warehouse ≈ 100 MB of seed data.
    #[arg(long, default_value = "1")]
    pub(crate) warehouses: usize,

    /// Number of concurrent OLTP terminals writing to the source database.
    /// TPC-C spec recommends 10 per warehouse for realistic contention.
    #[arg(long, default_value = "10")]
    pub(crate) terminals: usize,
}
