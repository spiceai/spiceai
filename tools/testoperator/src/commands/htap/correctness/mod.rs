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

//! Correctness gates for HTAP benchmarks. Run sequentially after the OLTP
//! workload stops: `row_count` waits for replication to drain and verifies
//! per-table `MAX(_bench_ts)` + `COUNT(*)` parity, then `analytical`
//! re-runs every CH-benCH analytical query against both source and Spice
//! and compares the results.

pub mod analytical;
pub mod row_count;

pub use analytical::verify_analytical_results;
pub use row_count::verify_after_drain;
