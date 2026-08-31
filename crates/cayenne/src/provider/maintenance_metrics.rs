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

//! The label vocabulary every Cayenne maintenance operation reports itself
//! with, and the thin emitters that carry it to the operational meter.
//!
//! Maintenance is a set of passes that mostly decide NOT to run: a compaction
//! declines because a lock is held or a budget is exceeded, a deletion-vector
//! sweep declines because it cannot prove the current snapshot is empty. Each
//! decline is a correct decision that nonetheless leaves the table larger, so
//! every one is countable — otherwise "the table is growing and nothing
//! reclaims it" cannot be told apart from "reclamation ran and found nothing to
//! reclaim".
//!
//! Every exit of an instrumented pass records exactly one [`CompactionOutcome`]
//! or [`MaintenanceOutcome`], so a table's counter series is its complete
//! decision history. Reasons are `declined_`-prefixed so one `PromQL` selector
//! (`outcome=~"declined_.*"`) separates refusals from work.

use telemetry::KeyValue;

/// Which compaction path a pass belongs to.
///
/// The strings match the `kind` label already carried by
/// `cayenne_compaction_duration_ms` and `cayenne_compaction_merged_bytes`, so
/// one label value joins an attempt to its duration and the bytes it wrote.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CompactionKind {
    /// Full re-encode of the current snapshot (also folds protected snapshots).
    Full,
    /// Current-snapshot small-file rewrite of a proper subset, hard-linking the
    /// files it did not pick.
    SubsetCurrent,
    /// Size-tiered merge over the protected-snapshot set.
    ProtectedSubset,
    /// Seq-prefix bake: consolidate the clean older protected prefix and prune
    /// the in-memory deletion index.
    Bake,
    /// Cold-tier (object-store) graduation.
    Datalake,
}

impl CompactionKind {
    /// Every variant — see `CompactionOutcome::ALL`.
    #[cfg(test)]
    pub(crate) const ALL: &'static [Self] = &[
        Self::Full,
        Self::SubsetCurrent,
        Self::ProtectedSubset,
        Self::Bake,
        Self::Datalake,
    ];

    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Full => "full",
            Self::SubsetCurrent => "subset_current",
            Self::ProtectedSubset => "subset",
            Self::Bake => "bake",
            Self::Datalake => "datalake",
        }
    }
}

/// How a compaction pass ended.
///
/// `NoOp` and the `Declined*` variants are deliberately distinct: a no-op ran
/// its selection and found nothing worth merging, while a decline never got
/// that far. They call for opposite responses — the first is a healthy idle
/// table, the second is a pass that is being prevented from running.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CompactionOutcome {
    /// A new snapshot was published.
    Committed,
    /// The pass ran its selection and had nothing to merge.
    NoOp,
    /// The pass merged, then found at commit time that a concurrent change had
    /// invalidated its inputs — another compaction consumed them, an append
    /// landed inside the fence, or an overwrite/cold-tier move replaced the
    /// snapshot — so it discarded its output.
    ///
    /// Distinct from both a decline and a no-op: the write amplification was
    /// paid and thrown away. A sustained rate here is contention, not idleness.
    AbortedConcurrentChange,
    /// The pass errored. The error is logged at the call site.
    Failed,
    /// A staged append is mid-finalization, so files would be neither cleanly
    /// in nor out of the scan.
    DeclinedStagingInflight,
    /// Neither the file-count nor the protected-snapshot trigger has fired.
    DeclinedBelowTrigger,
    /// Another pass already holds the per-table compaction lock.
    DeclinedLockBusy,
    /// A writer holds the write lock on a position-delete table, whose rewrite
    /// must serialize against writers.
    DeclinedWriterActive,
    /// Position-delete table: out of scope for the seq-prefix bake and the
    /// cold-tier promotion, neither of which can carry file-scoped tombstones.
    DeclinedNotKeyMode,
    /// The pass is not configured for this table (no cold-tier location).
    /// A permanent state rather than a transient one, so a steady count here is
    /// the expected shape, not a problem.
    DeclinedNotConfigured,
    /// The CDC apply is at or over capacity, so the bake yields the shared write
    /// path to the foreground writer.
    DeclinedApplyBackpressure,
    /// Fewer than two inputs qualified.
    DeclinedNoCandidates,
    /// A pending mem-tier delete caps the fence below the candidates' deletion
    /// thresholds, so folding them would mask a deletion.
    DeclinedAboveDeleteFence,
    /// The qualifying inputs exceed the per-pass memory budget.
    DeclinedOverPassBudget,
    /// No size tier has accumulated enough runs to merge.
    DeclinedNoQualifyingTier,
    /// An input could not be sized, and an unknown size cannot be counted as
    /// free against a memory ceiling.
    DeclinedSizingFailed,
    /// A live snapshot is not clean past the prefix cutoff, so the bake's prune
    /// would be withheld and the merge write-amplification wasted.
    DeclinedNoCleanPrefix,
}

impl CompactionOutcome {
    /// Every variant. The uniqueness and `declined_`-prefix tests iterate this,
    /// so a variant added without touching it would escape both — which is
    /// exactly what those tests exist to prevent, and what a hand-maintained
    /// list inside the test module cannot stop.
    ///
    /// `cfg(test)` because the tests are the only consumer; the list is a
    /// testing aid, not part of the vocabulary the emitters use.
    #[cfg(test)]
    pub(crate) const ALL: &'static [Self] = &[
        Self::Committed,
        Self::NoOp,
        Self::AbortedConcurrentChange,
        Self::Failed,
        Self::DeclinedStagingInflight,
        Self::DeclinedBelowTrigger,
        Self::DeclinedLockBusy,
        Self::DeclinedWriterActive,
        Self::DeclinedNotKeyMode,
        Self::DeclinedNotConfigured,
        Self::DeclinedApplyBackpressure,
        Self::DeclinedNoCandidates,
        Self::DeclinedAboveDeleteFence,
        Self::DeclinedOverPassBudget,
        Self::DeclinedNoQualifyingTier,
        Self::DeclinedSizingFailed,
        Self::DeclinedNoCleanPrefix,
    ];

    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Committed => "committed",
            Self::NoOp => "no_op",
            Self::AbortedConcurrentChange => "aborted_concurrent_change",
            Self::Failed => "failed",
            Self::DeclinedStagingInflight => "declined_staging_inflight",
            Self::DeclinedBelowTrigger => "declined_below_trigger",
            Self::DeclinedLockBusy => "declined_lock_busy",
            Self::DeclinedWriterActive => "declined_writer_active",
            Self::DeclinedNotKeyMode => "declined_not_key_mode",
            Self::DeclinedNotConfigured => "declined_not_configured",
            Self::DeclinedApplyBackpressure => "declined_apply_backpressure",
            Self::DeclinedNoCandidates => "declined_no_candidates",
            Self::DeclinedAboveDeleteFence => "declined_above_delete_fence",
            Self::DeclinedOverPassBudget => "declined_over_pass_budget",
            Self::DeclinedNoQualifyingTier => "declined_no_qualifying_tier",
            Self::DeclinedSizingFailed => "declined_sizing_failed",
            Self::DeclinedNoCleanPrefix => "declined_no_clean_prefix",
        }
    }
}

/// Which threshold asked for a compaction pass.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CompactionTrigger {
    /// Enough new small files accumulated in the current snapshot.
    SmallFileCount,
    /// The protected-snapshot set reached its count trigger.
    ProtectedSnapshotCount,
    /// The oldest protected snapshot reached its age trigger.
    ProtectedSnapshotAge,
    /// The in-memory deletion index reached the bake trigger.
    DeletionIndex,
    /// The deletion index crossed its hard memory ceiling, which forces a bake
    /// regardless of the count trigger or apply back-pressure.
    DeletionIndexMemoryCeiling,
}

impl CompactionTrigger {
    /// Every variant — see `CompactionOutcome::ALL`.
    #[cfg(test)]
    pub(crate) const ALL: &'static [Self] = &[
        Self::SmallFileCount,
        Self::ProtectedSnapshotCount,
        Self::ProtectedSnapshotAge,
        Self::DeletionIndex,
        Self::DeletionIndexMemoryCeiling,
    ];

    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::SmallFileCount => "small_file_count",
            Self::ProtectedSnapshotCount => "protected_snapshot_count",
            Self::ProtectedSnapshotAge => "protected_snapshot_age",
            Self::DeletionIndex => "deletion_index",
            Self::DeletionIndexMemoryCeiling => "deletion_index_memory_ceiling",
        }
    }
}

/// A maintenance operation that is not part of the compaction family.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MaintenanceOp {
    /// Reclaim of key-based deletion vectors that shadow nothing live.
    OrphanDvSweep,
    /// Application of the table's retention filters.
    Retention,
    /// Physical deletion of retired snapshot directories past their grace.
    RetiredDirSweep,
}

impl MaintenanceOp {
    /// Every variant. Iterated by `register_all` as well as the tests, so this
    /// one is not `cfg(test)` — a new operation is registered at zero the moment
    /// it is added to the list.
    pub(crate) const ALL: &'static [Self] =
        &[Self::OrphanDvSweep, Self::Retention, Self::RetiredDirSweep];

    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::OrphanDvSweep => "orphan_dv_sweep",
            Self::Retention => "retention",
            Self::RetiredDirSweep => "retired_dir_sweep",
        }
    }
}

/// How a non-compaction maintenance pass ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MaintenanceOutcome {
    /// The pass physically reclaimed something. Pair with [`track_reclaimed`]
    /// for how much.
    Reclaimed,
    /// The pass did its work, but that work does not itself return space.
    ///
    /// Retention is the case: it writes tombstones, and the bytes come back
    /// later when a compaction rewrites without the dead rows and the
    /// deletion-vector sweep unlinks the vectors. Reporting it as `reclaimed`
    /// would make reclamation dashboards climb on a pass that freed nothing.
    Applied,
    /// The pass ran and found nothing to reclaim.
    NoOp,
    /// A pass of this operation was already in flight, so this request was
    /// folded into it.
    Coalesced,
    /// The operation is not configured for this table (no retention filters).
    DeclinedNotConfigured,
    /// Fewer candidates than the pass's amortization threshold, so they are
    /// left for a later pass.
    DeclinedBelowThreshold,
    /// The current snapshot's manifest is empty but its directory is not, so
    /// the sweep cannot prove no live row is shadowed and must not remove
    /// anything.
    DeclinedManifestUnprovable,
    /// No candidate had reached its grace window, or every one is still pinned by
    /// an in-flight scan. Nothing was examined.
    DeclinedNotDue,
    /// Candidates WERE examined and none could be removed: their files are still
    /// referenced in place by a live snapshot, or a non-data sidecar keeps the
    /// directory alive.
    ///
    /// Distinct from [`Self::DeclinedNotDue`] because the two have opposite
    /// prognoses. Not-due resolves itself when the grace window elapses; a live
    /// reference resolves only when the referencing snapshot is itself retired,
    /// which may be never — so a directory reported this way indefinitely is
    /// space that is not coming back on its own.
    DeclinedLiveReference,
    /// The pass errored. The error is logged at the call site.
    Failed,
}

impl MaintenanceOutcome {
    /// Every variant — see `CompactionOutcome::ALL`.
    #[cfg(test)]
    pub(crate) const ALL: &'static [Self] = &[
        Self::Reclaimed,
        Self::Applied,
        Self::NoOp,
        Self::Coalesced,
        Self::DeclinedNotConfigured,
        Self::DeclinedBelowThreshold,
        Self::DeclinedManifestUnprovable,
        Self::DeclinedNotDue,
        Self::DeclinedLiveReference,
        Self::Failed,
    ];

    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Reclaimed => "reclaimed",
            Self::Applied => "applied",
            Self::NoOp => "no_op",
            Self::Coalesced => "coalesced",
            Self::DeclinedNotConfigured => "declined_not_configured",
            Self::DeclinedBelowThreshold => "declined_below_threshold",
            Self::DeclinedManifestUnprovable => "declined_manifest_unprovable",
            Self::DeclinedNotDue => "declined_not_due",
            Self::DeclinedLiveReference => "declined_live_reference",
            Self::Failed => "failed",
        }
    }
}

/// Record how one compaction pass ended.
pub(crate) fn track_compaction(table: &str, kind: CompactionKind, outcome: CompactionOutcome) {
    telemetry::cayenne::track_compaction_outcome(&[
        KeyValue::new("table", table.to_string()),
        KeyValue::new("kind", kind.as_str()),
        KeyValue::new("outcome", outcome.as_str()),
    ]);
}

/// Record which threshold asked for a compaction pass.
pub(crate) fn track_trigger(table: &str, kind: CompactionKind, trigger: CompactionTrigger) {
    telemetry::cayenne::track_compaction_trigger(&[
        KeyValue::new("table", table.to_string()),
        KeyValue::new("kind", kind.as_str()),
        KeyValue::new("trigger", trigger.as_str()),
    ]);
}

/// Record how one non-compaction maintenance pass ended.
pub(crate) fn track_maintenance(table: &str, op: MaintenanceOp, outcome: MaintenanceOutcome) {
    telemetry::cayenne::track_maintenance_outcome(&[
        KeyValue::new("table", table.to_string()),
        KeyValue::new("op", op.as_str()),
        KeyValue::new("outcome", outcome.as_str()),
    ]);
}

/// Register every non-compaction maintenance operation's counters at zero for
/// this table.
///
/// Without this, an operation that has never run emits no series, and "the
/// deletion-vector sweep has reclaimed nothing" is indistinguishable from "the
/// deletion-vector sweep is not instrumented" — the first being the finding and
/// the second being a reason to disbelieve the query. Idempotent: adding zero to
/// a counter leaves its value alone, so the registration can ride any tick.
pub(crate) fn register_all(table: &str) {
    for op in MaintenanceOp::ALL.iter().copied() {
        telemetry::cayenne::register_maintenance_counters(&[
            KeyValue::new("table", table.to_string()),
            KeyValue::new("op", op.as_str()),
        ]);
    }
}

/// Record what one maintenance pass physically gave back.
pub(crate) fn track_reclaimed(table: &str, op: MaintenanceOp, files: u64, bytes: u64, rows: u64) {
    telemetry::cayenne::track_maintenance_reclaimed(
        files,
        bytes,
        rows,
        &[
            KeyValue::new("table", table.to_string()),
            KeyValue::new("op", op.as_str()),
        ],
    );
}

#[cfg(test)]
mod tests {
    use super::{
        CompactionKind, CompactionOutcome, CompactionTrigger, MaintenanceOp, MaintenanceOutcome,
    };
    use std::collections::HashSet;

    /// Two outcomes sharing a label would silently merge two different
    /// diagnoses into one time series.
    #[test]
    fn every_label_value_is_distinct() {
        let mut seen: HashSet<&'static str> = HashSet::new();
        for outcome in CompactionOutcome::ALL.iter().copied() {
            assert!(
                seen.insert(outcome.as_str()),
                "duplicate compaction outcome label: {}",
                outcome.as_str()
            );
        }

        let mut seen: HashSet<&'static str> = HashSet::new();
        for outcome in MaintenanceOutcome::ALL.iter().copied() {
            assert!(
                seen.insert(outcome.as_str()),
                "duplicate maintenance outcome label: {}",
                outcome.as_str()
            );
        }

        let mut seen: HashSet<&'static str> = HashSet::new();
        for kind in CompactionKind::ALL.iter().copied() {
            assert!(
                seen.insert(kind.as_str()),
                "duplicate compaction kind label: {}",
                kind.as_str()
            );
        }
    }

    /// `outcome=~"declined_.*"` is the selector operators use to ask "what is
    /// stopping maintenance from running", so a decline that does not carry the
    /// prefix drops out of that answer, and a non-decline that does adds a
    /// phantom refusal.
    #[test]
    fn declines_are_exactly_the_declined_prefixed_labels() {
        // BOTH families, because the `outcome=~"declined_.*"` selector spans
        // them: one unprefixed decline in either drops silently out of the
        // answer to "what is stopping maintenance". `not_configured` was exactly
        // that — a pass that never ran, invisible to the selector.
        for outcome in MaintenanceOutcome::ALL.iter().copied() {
            let is_decline = !matches!(
                outcome,
                MaintenanceOutcome::Reclaimed
                    | MaintenanceOutcome::Applied
                    | MaintenanceOutcome::NoOp
                    | MaintenanceOutcome::Coalesced
                    | MaintenanceOutcome::Failed
            );
            assert_eq!(
                is_decline,
                outcome.as_str().starts_with("declined_"),
                "{} is labelled inconsistently with whether it is a decline",
                outcome.as_str()
            );
        }
        for outcome in CompactionOutcome::ALL.iter().copied() {
            let is_decline = !matches!(
                outcome,
                CompactionOutcome::Committed
                    | CompactionOutcome::NoOp
                    | CompactionOutcome::AbortedConcurrentChange
                    | CompactionOutcome::Failed
            );
            assert_eq!(
                is_decline,
                outcome.as_str().starts_with("declined_"),
                "{} is labelled inconsistently with whether it is a decline",
                outcome.as_str()
            );
        }
    }

    /// The `kind` values must stay identical to the ones
    /// `cayenne_compaction_duration_ms` and `cayenne_compaction_merged_bytes`
    /// already emit, or an outcome cannot be joined to the duration and bytes of
    /// the pass that produced it.
    #[test]
    fn compaction_kinds_match_the_existing_duration_labels() {
        assert_eq!(CompactionKind::Full.as_str(), "full");
        assert_eq!(CompactionKind::SubsetCurrent.as_str(), "subset_current");
        assert_eq!(CompactionKind::ProtectedSubset.as_str(), "subset");
        assert_eq!(CompactionKind::Datalake.as_str(), "datalake");
    }

    #[test]
    fn trigger_and_op_labels_are_distinct() {
        let mut seen: HashSet<&'static str> = HashSet::new();
        for trigger in CompactionTrigger::ALL.iter().copied() {
            assert!(seen.insert(trigger.as_str()));
        }

        let mut seen: HashSet<&'static str> = HashSet::new();
        for op in MaintenanceOp::ALL.iter().copied() {
            assert!(seen.insert(op.as_str()));
        }
    }
}
