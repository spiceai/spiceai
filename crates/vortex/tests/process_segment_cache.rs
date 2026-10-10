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

//! The process-wide segment cache lives in a `OnceLock`, so this needs a process
//! of its own rather than a unit test that would leak the install into every
//! other test in the library binary (see `crate::require_process_per_test`).
//!
//! All of it is one test on purpose: install order is process state.

use vortex::VortexSessionDefault;
use vortex::session::VortexSession;
use vortex_datafusion::{VortexFormat, VortexTableOptions, install_process_segment_cache};

const INSTALLED_BYTES: u64 = 8 * 1024 * 1024;

fn format() -> VortexFormat {
    // Neither caller asks for a cache of its own; the shared one is not opted
    // into per format.
    let opts = VortexTableOptions::default();
    assert_eq!(opts.segment_cache_size_bytes, None);
    VortexFormat::new_with_process_segment_cache(VortexSession::default(), opts)
}

#[test]
fn the_installed_cache_is_shared_by_every_format_and_installed_once() {
    crate::require_process_per_test(
        "this test installs the process-wide segment cache, which cannot be undone",
    );
    // Before any install, and with no per-format size, scans run uncached.
    assert_eq!(
        format().segment_cache_capacity_bytes(),
        None,
        "nothing installed means no cache"
    );

    // With no decision made, a format may still build a cache of its own from
    // `segment_cache_size_bytes` — that is how an embedded host which skips the
    // runtime builder keeps working. This has to be asserted before the install
    // below, hence one test rather than two.
    let private = VortexFormat::new_with_process_segment_cache(
        VortexSession::default(),
        VortexTableOptions {
            segment_cache_size_bytes: Some(4 * 1024 * 1024),
            ..Default::default()
        },
    );
    assert_eq!(
        private.segment_cache_capacity_bytes(),
        Some(4 * 1024 * 1024),
        "an uninitialized process leaves the per-format size in charge"
    );

    assert!(
        install_process_segment_cache(INSTALLED_BYTES),
        "the first real install takes effect"
    );

    // Two formats — standing in for two tables — both report the installed
    // budget, which is the whole point: one cache, one budget, no per-table
    // reservation.
    let first = format();
    let second = format();
    assert_eq!(
        first.segment_cache_capacity_bytes(),
        Some(INSTALLED_BYTES),
        "a format built after the install uses it"
    );
    assert_eq!(
        second.segment_cache_capacity_bytes(),
        first.segment_cache_capacity_bytes(),
        "every format reports the same budget because they share one cache"
    );

    // A later install must not resize the cache tables are already reading from.
    assert!(
        !install_process_segment_cache(64 * 1024 * 1024),
        "a second install is refused"
    );
    assert_eq!(
        format().segment_cache_capacity_bytes(),
        Some(INSTALLED_BYTES),
        "the first budget survives a second install attempt"
    );
}
