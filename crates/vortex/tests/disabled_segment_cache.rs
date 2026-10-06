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

//! `runtime.params.cayenne_segment_cache_mb=0` has to mean *off*, not "off
//! globally, on per table". Its own binary because the decision is a process-wide
//! `OnceLock` and cannot be undone.

use vortex::VortexSessionDefault;
use vortex::session::VortexSession;
use vortex_datafusion::{VortexFormat, VortexTableOptions, install_process_segment_cache};

#[test]
fn a_disabled_process_cache_is_not_replaced_by_private_per_table_caches() {
    assert!(
        install_process_segment_cache(0),
        "zero installs the disabled decision"
    );

    // Cayenne always passes a non-zero hardware-derived size through
    // `VortexTableOptions`, which is exactly the input that used to resurrect a
    // private cache per table — each one invisible to the runtime's memory
    // accounting, which counts nothing when the cache is disabled.
    let opts = VortexTableOptions {
        segment_cache_size_bytes: Some(512 * 1024 * 1024),
        ..Default::default()
    };

    let format =
        VortexFormat::new_with_process_segment_cache(VortexSession::default(), opts.clone());
    assert_eq!(
        format.segment_cache_capacity_bytes(),
        None,
        "a per-format size must not re-enable caching the process switched off"
    );

    // Every table, not just the first.
    let second =
        VortexFormat::new_with_process_segment_cache(VortexSession::default(), opts.clone());
    assert_eq!(second.segment_cache_capacity_bytes(), None);

    // A format that never opts in is not governed by the Cayenne setting: a
    // listing table over external files keeps its own explicitly-sized cache,
    // which is the behaviour it had before the shared cache existed.
    let independent = VortexFormat::new_with_options(VortexSession::default(), opts);
    assert_eq!(
        independent.segment_cache_capacity_bytes(),
        Some(512 * 1024 * 1024),
        "an opt-out format keeps its own cache"
    );

    // And the decision stands: a later install cannot turn caching back on.
    assert!(
        !install_process_segment_cache(64 * 1024 * 1024),
        "the disabled decision is final"
    );
    let after =
        VortexFormat::new_with_options(VortexSession::default(), VortexTableOptions::default());
    assert_eq!(after.segment_cache_capacity_bytes(), None);
}
