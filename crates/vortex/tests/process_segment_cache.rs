// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! The process-wide segment cache lives in a `OnceLock`, so it is exercised from
//! its own integration binary rather than a unit test that would leak the install
//! into every other test in the library binary.
//!
//! All of it is one test on purpose: install order is process state, and the
//! default test harness runs a binary's tests on threads of one process.

use vortex::VortexSessionDefault;
use vortex::session::VortexSession;
use vortex_datafusion::{VortexFormat, VortexTableOptions, install_process_segment_cache};

const INSTALLED_BYTES: u64 = 8 * 1024 * 1024;

fn format() -> VortexFormat {
    // Neither caller asks for a cache of its own; the shared one is not opted
    // into per format.
    let opts = VortexTableOptions::default();
    assert_eq!(opts.segment_cache_size_bytes, None);
    VortexFormat::new_with_options(VortexSession::default(), opts)
}

#[test]
fn the_installed_cache_is_shared_by_every_format_and_installed_once() {
    // Before any install, and with no per-format size, scans run uncached.
    assert_eq!(
        format().segment_cache_capacity_bytes(),
        None,
        "nothing installed means no cache"
    );

    // A zero budget is a disable, not an install of an unbounded cache.
    assert!(
        !install_process_segment_cache(0),
        "zero must not install a cache"
    );
    assert_eq!(format().segment_cache_capacity_bytes(), None);

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
