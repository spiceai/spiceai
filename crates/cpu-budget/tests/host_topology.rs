/*
Copyright 2024-2026 The Spice.ai OSS Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

use cpu_budget::{CpuBudget, CpuConfig, HostReadings};

/// Exercise real host detection, including Linux containers with CPU sysfs
/// hidden: a logical CPU count alone cannot establish the physical-core minimum.
#[test]
fn auto_position_deletes_require_known_host_topology() {
    let host = HostReadings::detect();
    let budget = CpuBudget::resolve(&CpuConfig::default(), &host).expect("valid detected budget");
    let keeps_compacted = budget.cayenne_keeps_small_tables_compacted();
    println!(
        "detected_smt={:?} logical_budget={} compact_small_tables={keeps_compacted}",
        host.smt,
        budget.cores(),
    );
    assert!(
        host.smt.is_some() || !keeps_compacted,
        "unknown topology must not establish eight physical cores"
    );
}
