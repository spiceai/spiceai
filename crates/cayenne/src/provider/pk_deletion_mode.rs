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

//! How a primary-key table records the delete of a row: fixed by
//! `deletion_mode: key` / `position`, or chosen by `auto` as the table grows.
//!
//! The two modes reclaim their deletion state differently, and that is what the
//! choice is about. A position-mode table reclaims by rewriting its whole current
//! snapshot, folding in every protected snapshot and every tombstone, which leaves
//! one compact snapshot, an empty deletion index and a bloom keyset. That is the
//! cheapest state to scan and to hold in memory, and the rewrite costs time in
//! proportion to the table. A key-mode table reclaims with the seq-prefix bake,
//! which rewrites only the settled prefix of its protected snapshots — cheap at any
//! size, but it leaves the table in several snapshots behind a deletion filter.
//!
//! `auto` therefore keeps a table in position mode while rewriting it whole stays
//! cheap, and moves it to key mode once it outgrows that: see
//! [`AutoDeletionMode::max_position_bytes`].

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::metadata::{DeletionMode, ResolvedDeletionMode};

/// How a primary-key table records the delete of a row right now.
#[derive(Clone, Debug)]
pub(crate) enum PkDeletionMode {
    /// Key deletes: `deletion_mode: key`, or `auto` on a table it does not adapt.
    Key,
    /// Position deletes, configured explicitly with `deletion_mode: position`.
    Position,
    /// `deletion_mode: auto` choosing between the two as the table grows.
    Auto(Arc<AutoDeletionMode>),
}

impl PkDeletionMode {
    /// Resolve the configured `mode` for one table.
    ///
    /// `adapts` says whether `auto` may choose position deletes for this table at
    /// all: it may not for a table whose rows live in RAM (`mode: memory`), a
    /// partitioned table, or a table with a datalake tier, which keep key deletes.
    /// `table_bytes` is the table's on-disk size when it was opened, `None` when it
    /// could not be read; an unknown size starts the table in key mode, the choice
    /// that cannot cost it a whole-table rewrite.
    pub(crate) fn for_table(
        mode: DeletionMode,
        has_primary_key: bool,
        adapts: bool,
        table_bytes: Option<u64>,
        target_file_size_bytes: usize,
    ) -> Self {
        match mode.resolved(has_primary_key) {
            ResolvedDeletionMode::Key => Self::Key,
            ResolvedDeletionMode::Position => Self::Position,
            ResolvedDeletionMode::Adaptive if !adapts => Self::Key,
            ResolvedDeletionMode::Adaptive => Self::Auto(Arc::new(AutoDeletionMode::new(
                table_bytes,
                AutoDeletionMode::max_position_bytes_for(target_file_size_bytes),
            ))),
        }
    }

    /// Whether the table records position deletes where a row's position is
    /// known (and reclaims by rewriting itself), rather than key deletes only.
    pub(crate) fn uses_positions(&self) -> bool {
        match self {
            Self::Key => false,
            Self::Position => true,
            Self::Auto(auto) => auto.uses_positions(),
        }
    }

    /// The `auto` state, when this table's mode is `auto`.
    pub(crate) fn auto(&self) -> Option<&Arc<AutoDeletionMode>> {
        match self {
            Self::Auto(auto) => Some(auto),
            Self::Key | Self::Position => None,
        }
    }
}

/// `deletion_mode: auto` on a primary-key table: position deletes while the table
/// is small enough to rewrite whole, key deletes once it is not.
///
/// While on position deletes, every rewrite of the table — the whole-table
/// reclaim and the protected-snapshot merges — runs alongside writers, as on a
/// key-delete table (see `CayenneTableProvider::position_rewrites_exclude_writers`).
///
/// The switch goes one way. A table that outgrows the bound stays on key deletes
/// for the life of this provider; the next open sizes it again.
#[derive(Debug)]
pub(crate) struct AutoDeletionMode {
    /// Whether the table still records position deletes.
    position: AtomicBool,
    /// Set by a check that found the table past its bound where it cannot take
    /// `compaction_lock`. The next compaction trigger performs the switch.
    switch_requested: AtomicBool,
    /// See [`Self::max_position_bytes`].
    max_position_bytes: u64,
}

impl AutoDeletionMode {
    /// A table at most this many on-disk bytes keeps position deletes.
    pub(crate) fn new(table_bytes: Option<u64>, max_position_bytes: u64) -> Self {
        let position = table_bytes.is_some_and(|bytes| bytes <= max_position_bytes);
        Self {
            position: AtomicBool::new(position),
            switch_requested: AtomicBool::new(false),
            max_position_bytes,
        }
    }

    /// The bound for a table whose target file size is `target_file_size_bytes`:
    /// a quarter of it, the size below which the compactor already counts a file
    /// as small and rewrites it as routine maintenance. A table no larger than one
    /// small file costs no more to rewrite whole than the compactor already spends,
    /// and the target size it derives from follows the storage the table is on.
    pub(crate) fn max_position_bytes_for(target_file_size_bytes: usize) -> u64 {
        u64::try_from(target_file_size_bytes / 4).unwrap_or(u64::MAX)
    }

    /// Largest on-disk size, in bytes, at which the table keeps position deletes.
    pub(crate) fn max_position_bytes(&self) -> u64 {
        self.max_position_bytes
    }

    /// Whether the table records position deletes.
    pub(crate) fn uses_positions(&self) -> bool {
        self.position.load(Ordering::Acquire)
    }

    /// Whether a table of `table_bytes` on-disk bytes is within the bound.
    pub(crate) fn fits(&self, table_bytes: u64) -> bool {
        table_bytes <= self.max_position_bytes
    }

    /// Ask the next compaction trigger to move the table to key deletes. Returns
    /// `true` when this call raised the request (the table was still on position
    /// deletes and no request was pending).
    pub(crate) fn request_switch_to_key(&self) -> bool {
        self.uses_positions() && !self.switch_requested.swap(true, Ordering::AcqRel)
    }

    /// Whether a switch to key deletes is pending.
    pub(crate) fn switch_requested(&self) -> bool {
        self.switch_requested.load(Ordering::Acquire)
    }

    /// Move the table to key deletes. Returns `true` when this call switched it.
    pub(crate) fn switch_to_key(&self) -> bool {
        self.switch_requested.store(false, Ordering::Release);
        self.position.swap(false, Ordering::AcqRel)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TARGET: usize = 256 * 1024 * 1024;
    const BOUND: u64 = 64 * 1024 * 1024;

    #[test]
    fn the_bound_is_a_quarter_of_the_target_file_size() {
        assert_eq!(AutoDeletionMode::max_position_bytes_for(TARGET), BOUND);
        assert_eq!(AutoDeletionMode::max_position_bytes_for(0), 0);
    }

    #[test]
    fn a_table_starts_on_position_deletes_only_within_the_bound() {
        assert!(AutoDeletionMode::new(Some(0), BOUND).uses_positions());
        assert!(AutoDeletionMode::new(Some(BOUND), BOUND).uses_positions());
        assert!(!AutoDeletionMode::new(Some(BOUND + 1), BOUND).uses_positions());
        assert!(
            !AutoDeletionMode::new(None, BOUND).uses_positions(),
            "an unknown size must start on key deletes"
        );
    }

    #[test]
    fn the_switch_to_key_is_one_way_and_reported_once() {
        let auto = AutoDeletionMode::new(Some(1), BOUND);
        assert!(auto.request_switch_to_key());
        assert!(
            !auto.request_switch_to_key(),
            "a pending request is not raised twice"
        );
        assert!(auto.switch_requested());
        assert!(auto.switch_to_key());
        assert!(!auto.uses_positions());
        assert!(!auto.switch_requested());
        assert!(!auto.switch_to_key(), "a second switch changes nothing");
        assert!(
            !auto.request_switch_to_key(),
            "a key-mode table has nothing to switch"
        );
    }

    #[test]
    fn fits_is_inclusive_of_the_bound() {
        let auto = AutoDeletionMode::new(Some(0), BOUND);
        assert!(auto.fits(BOUND));
        assert!(!auto.fits(BOUND + 1));
    }

    #[test]
    fn for_table_resolves_each_configuration() {
        let resolve = |mode, has_pk, adapts, bytes| {
            PkDeletionMode::for_table(mode, has_pk, adapts, bytes, TARGET).uses_positions()
        };
        // Explicit modes ignore size and eligibility.
        assert!(!resolve(DeletionMode::Key, true, true, Some(0)));
        assert!(resolve(DeletionMode::Position, true, true, Some(u64::MAX)));
        // `auto` adapts by size on an eligible primary-key table.
        assert!(resolve(DeletionMode::Auto, true, true, Some(BOUND)));
        assert!(!resolve(DeletionMode::Auto, true, true, Some(BOUND + 1)));
        // `auto` on a table it does not adapt keeps key deletes.
        assert!(!resolve(DeletionMode::Auto, true, false, Some(0)));
        assert!(matches!(
            PkDeletionMode::for_table(DeletionMode::Auto, true, false, Some(0), TARGET),
            PkDeletionMode::Key
        ));
        assert!(matches!(
            PkDeletionMode::for_table(DeletionMode::Auto, true, true, Some(0), TARGET),
            PkDeletionMode::Auto(_)
        ));
    }
}
