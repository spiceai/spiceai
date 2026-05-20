/*
Copyright 2026 The Spice.ai OSS Authors

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

//! Tiered merge-tree compaction picker and background scheduler.
//!
//! Steady streaming ingestion produces many small Vortex files in the current
//! snapshot directory: each inline-memtable checkpoint emits one ~8 MB file,
//! and each non-inline write emits at least one Vortex file. Read fan-out and
//! object-store listing cost both grow linearly with file count.
//!
//! The picker buckets files by size into tiers — small, mid, large — and emits
//! a [`CompactionCandidate`] when the smallest non-empty tier has enough files
//! whose combined size is worth a rewrite. The current runner (in
//! [`crate::provider::table`]) uses that candidate as an eligibility and
//! observability signal, then atomically rewrites the entire current snapshot.
//! The rewrite goes through `write_to_snapshot`, which honors `target_partitions`
//! and the configured target file size, so a pass typically produces one or a
//! small number of consolidated Vortex files rather than guaranteeing exactly
//! one.
//!
//! The module also owns [`BackgroundCompactor`], a per-table tokio task that
//! periodically invokes the runner. The task is `Semaphore`-gated so a fleet of
//! tables can't overwhelm the writer pool.

use std::sync::{Arc, Weak};
use std::time::Duration;

use tokio::sync::{Notify, Semaphore};

/// Tier thresholds derived from `target_vortex_file_size_mb`.
///
/// `small_max_bytes` = `target_vortex_file_size_bytes` / 4 — anything below
///   counts as "small" and is eligible for L0 → L1 compaction.
/// `mid_max_bytes` = `target_vortex_file_size_bytes` — anything below counts as
///   "mid" and is eligible for L1 → L2 compaction.
/// Files at or above `mid_max_bytes` are considered settled.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CompactionTiers {
    pub small_max_bytes: u64,
    pub mid_max_bytes: u64,
}

impl CompactionTiers {
    #[must_use]
    pub(crate) fn from_target_file_size_bytes(target_file_size_bytes: u64) -> Self {
        // target / 4 is the small/mid boundary. A misconfigured target of 0
        // still produces deterministic tiers.
        let small_max_bytes = target_file_size_bytes / 4;
        Self {
            small_max_bytes,
            mid_max_bytes: target_file_size_bytes,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Tier {
    Small,
    Mid,
}

impl Tier {
    fn classify(size_bytes: u64, tiers: &CompactionTiers) -> Option<Self> {
        if size_bytes < tiers.small_max_bytes {
            Some(Self::Small)
        } else if size_bytes < tiers.mid_max_bytes {
            Some(Self::Mid)
        } else {
            // Settled — not a compaction candidate.
            None
        }
    }

    #[must_use]
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Small => "small",
            Self::Mid => "mid",
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct CompactionPickerConfig {
    /// Minimum number of files in a tier required to consider compaction.
    pub trigger_files: usize,
    /// Maximum number of file paths retained in the candidate for tracing and
    /// selection. The current runner still rewrites the whole snapshot once a
    /// candidate is found.
    pub max_files_per_pick: usize,
    /// Tier thresholds derived from `target_vortex_file_size_mb`.
    pub tiers: CompactionTiers,
}

impl CompactionPickerConfig {
    /// Convenience constructor matching the config fields surfaced on
    /// `VortexConfig`.
    #[must_use]
    pub(crate) fn new(
        trigger_files: usize,
        max_files_per_pick: usize,
        target_file_size_bytes: u64,
    ) -> Self {
        Self {
            trigger_files: trigger_files.max(2),
            max_files_per_pick: max_files_per_pick.max(2),
            tiers: CompactionTiers::from_target_file_size_bytes(target_file_size_bytes),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct FileEntry<P> {
    pub path: P,
    pub size_bytes: u64,
}

#[derive(Debug, Clone)]
pub(crate) struct CompactionCandidate<P> {
    pub tier: Tier,
    pub paths: Vec<P>,
    pub total_bytes: u64,
}

/// Pick a compaction candidate from a list of files and their sizes.
///
/// Pure function — no IO. Algorithm:
/// 1. Bucket files into `Small` and `Mid` tiers (anything at/above
///    `mid_max_bytes` is settled).
/// 2. For each tier in order Small → Mid:
///    - if `count >= trigger_files` AND tier bytes reach that tier's threshold,
///      sort ascending by size, take the first `max_files_per_pick`, return
///      them as the candidate.
/// 3. Otherwise return `None`.
///
/// Picking the smallest files first keeps the candidate focused on the tier
/// with the most file-count pressure; the current runner still performs a
/// whole-snapshot rewrite after the candidate is selected.
#[must_use]
pub(crate) fn pick_candidates<P: Clone>(
    files: impl IntoIterator<Item = FileEntry<P>>,
    cfg: &CompactionPickerConfig,
) -> Option<CompactionCandidate<P>> {
    let mut small = Vec::new();
    let mut mid = Vec::new();

    for entry in files {
        match Tier::classify(entry.size_bytes, &cfg.tiers) {
            Some(Tier::Small) => small.push(entry),
            Some(Tier::Mid) => mid.push(entry),
            None => {}
        }
    }

    pick_from_bucket(Tier::Small, &mut small, cfg)
        .or_else(|| pick_from_bucket(Tier::Mid, &mut mid, cfg))
}

fn pick_from_bucket<P: Clone>(
    tier: Tier,
    bucket: &mut [FileEntry<P>],
    cfg: &CompactionPickerConfig,
) -> Option<CompactionCandidate<P>> {
    if bucket.len() < cfg.trigger_files {
        return None;
    }

    // Threshold check uses the WHOLE tier's bytes.
    //
    // For the Small tier the primary goal (as documented) is to relieve
    // *file-count* pressure (many tiny objects hurt LIST performance, scan
    // overhead, and S3 costs). We therefore trigger on count (`>= trigger_files`)
    // as long as the tier has accumulated at least one "full small file" worth
    // of data (`>= small_max_bytes`). This is much more responsive to file
    // count than requiring `small_max * trigger_files` total bytes.
    //
    // For Mid we keep the higher `mid_max_bytes` threshold because those
    // files are already closer to the target size and the goal is more about
    // reaching good file sizes.
    let tier_total_bytes: u64 = bucket.iter().map(|entry| entry.size_bytes).sum();
    let byte_threshold = match tier {
        Tier::Small => cfg.tiers.small_max_bytes,
        Tier::Mid => cfg.tiers.mid_max_bytes,
    };
    if tier_total_bytes < byte_threshold {
        return None;
    }

    bucket.sort_by_key(|entry| entry.size_bytes);
    let max_pick = cfg.max_files_per_pick.min(bucket.len());
    let picked = &bucket[..max_pick];
    let picked_bytes: u64 = picked.iter().map(|entry| entry.size_bytes).sum();
    let paths = picked.iter().map(|entry| entry.path.clone()).collect();
    Some(CompactionCandidate {
        tier,
        paths,
        total_bytes: picked_bytes,
    })
}

/// Trait the background compactor uses to invoke a per-table compaction pass.
///
/// Implemented by `CayenneTableProvider`. Decouples the scheduler from the
/// provider so we can unit-test the scheduler with a stub.
#[async_trait::async_trait]
pub(crate) trait CompactionRunner: Send + Sync {
    /// Run one compaction trigger. Returns `Ok(true)` if any compaction
    /// occurred. Errors are reported via the return value; the scheduler logs
    /// and continues on Err.
    async fn run_compaction_trigger(&self) -> Result<bool, String>;

    /// Identifier used in log messages.
    fn compaction_target_name(&self) -> &str;
}

/// Per-table background compactor.
///
/// Owns a tokio task that wakes every `interval`, acquires a permit from the
/// shared semaphore, and calls `runner.run_compaction_trigger()`. Cancellation
/// happens via [`Drop`]: dropping the `BackgroundCompactor` fires the shutdown
/// `Notify` and aborts the task's `JoinHandle`.
///
/// The runner is held via `Weak` so the task does not keep the
/// `CayenneTableProvider` alive past its caller's `Arc` lifetime.
pub(crate) struct BackgroundCompactor {
    handle: Option<tokio::task::JoinHandle<()>>,
    shutdown: Arc<Notify>,
}

impl BackgroundCompactor {
    /// Spawn a background compaction task. Returns `None` if `interval` is
    /// zero, indicating the task is disabled.
    pub(crate) fn spawn(
        runner: Weak<dyn CompactionRunner>,
        interval: Duration,
        semaphore: Arc<Semaphore>,
    ) -> Option<Self> {
        if interval.is_zero() {
            return None;
        }

        let shutdown = Arc::new(Notify::new());
        let shutdown_task = Arc::clone(&shutdown);

        let handle = tokio::spawn(async move {
            loop {
                tokio::select! {
                    () = tokio::time::sleep(interval) => {}
                    () = shutdown_task.notified() => break,
                }

                let Some(runner) = runner.upgrade() else {
                    // Provider dropped — task exits naturally.
                    break;
                };

                // Acquire a permit, gating concurrent background compactions
                // across all tables sharing the semaphore.
                let Ok(_permit) = Arc::clone(&semaphore).acquire_owned().await else {
                    // Semaphore closed — provider tree shutting down.
                    break;
                };

                match runner.run_compaction_trigger().await {
                    Ok(true) => {
                        tracing::debug!(
                            target: "cayenne::compaction",
                            table = runner.compaction_target_name(),
                            "Background compaction pass completed"
                        );
                    }
                    Ok(false) => {}
                    Err(e) => {
                        tracing::warn!(
                            target: "cayenne::compaction",
                            table = runner.compaction_target_name(),
                            "Background compaction failed: {e}"
                        );
                    }
                }
            }
        });

        Some(Self {
            handle: Some(handle),
            shutdown,
        })
    }
}

// Cleanup happens entirely in `Drop`: the shutdown signal is fired and the
// JoinHandle is aborted. Callers don't need explicit `shutdown` / `join`
// methods — when the provider's last `Arc` drops, the `OnceLock<BackgroundCompactor>`
// inside drops too, which runs the impl below.

impl Drop for BackgroundCompactor {
    fn drop(&mut self) {
        self.shutdown.notify_one();
        if let Some(handle) = self.handle.take() {
            handle.abort();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entries(sizes: &[u64]) -> Vec<FileEntry<String>> {
        sizes
            .iter()
            .enumerate()
            .map(|(idx, &size)| FileEntry {
                path: format!("file_{idx:04}.vortex"),
                size_bytes: size,
            })
            .collect()
    }

    /// Helper: target file size of 256 MiB, matching the default.
    fn default_cfg() -> CompactionPickerConfig {
        CompactionPickerConfig::new(8, 32, 256 * 1024 * 1024)
    }

    #[test]
    fn tiers_derived_from_target_size() {
        let tiers = CompactionTiers::from_target_file_size_bytes(128 * 1024 * 1024);
        assert_eq!(tiers.small_max_bytes, 32 * 1024 * 1024);
        assert_eq!(tiers.mid_max_bytes, 128 * 1024 * 1024);
    }

    #[test]
    fn tier_classify_assigns_correct_buckets() {
        let tiers = CompactionTiers::from_target_file_size_bytes(128 * 1024 * 1024);
        assert_eq!(Tier::classify(1, &tiers), Some(Tier::Small));
        assert_eq!(
            Tier::classify(32 * 1024 * 1024 - 1, &tiers),
            Some(Tier::Small)
        );
        assert_eq!(Tier::classify(32 * 1024 * 1024, &tiers), Some(Tier::Mid));
        assert_eq!(
            Tier::classify(128 * 1024 * 1024 - 1, &tiers),
            Some(Tier::Mid)
        );
        assert_eq!(Tier::classify(128 * 1024 * 1024, &tiers), None);
        assert_eq!(Tier::classify(u64::MAX, &tiers), None);
    }

    #[test]
    fn picker_handles_empty_input() {
        let cfg = default_cfg();
        assert!(pick_candidates(std::iter::empty::<FileEntry<String>>(), &cfg).is_none());
    }

    #[test]
    fn picker_returns_none_when_below_trigger_count() {
        let cfg = default_cfg();
        // 7 small files of 5 MiB each — below trigger_files = 8.
        let files = entries(&[5 * 1024 * 1024; 7]);
        assert!(pick_candidates(files.iter().cloned(), &cfg).is_none());
    }

    #[test]
    fn picker_returns_none_when_total_bytes_below_target() {
        let cfg = default_cfg();
        // 8 small files of 1 MiB each — meets trigger_files but total = 8 MiB,
        // well below the 64 MiB Small-tier byte threshold (target_size / 4).
        let files = entries(&[1024 * 1024; 8]);
        assert!(pick_candidates(files.iter().cloned(), &cfg).is_none());
    }

    #[test]
    fn picker_picks_small_tier_first() {
        let cfg = default_cfg();
        // 8 small (16 MiB) + 8 mid (64 MiB). Both tiers are eligible (total
        // 128 MiB and 512 MiB respectively). The picker should choose Small
        // first.
        let mut sizes = vec![16 * 1024 * 1024; 8];
        sizes.extend(vec![64 * 1024 * 1024; 8]);
        let files = entries(&sizes);
        let candidate = pick_candidates(files.iter().cloned(), &cfg).expect("expected a candidate");
        assert_eq!(candidate.tier, Tier::Small);
        assert_eq!(candidate.paths.len(), 8);
        assert_eq!(candidate.total_bytes, 8 * 16 * 1024 * 1024);
    }

    #[test]
    fn picker_caps_at_max_files_per_pick() {
        // Target = 64 MiB → mid_max = 64 MiB, small_max = 16 MiB.
        // 10 small files of 10 MiB each. The whole Small tier totals 100 MiB,
        // which is above the 16 MiB Small-tier threshold, so the picker has
        // work and then caps the retained candidate paths at max_files_per_pick.
        let cfg = CompactionPickerConfig::new(2, 8, 64 * 1024 * 1024);
        let files = entries(&[10 * 1024 * 1024; 10]);
        let candidate = pick_candidates(files.iter().cloned(), &cfg).expect("expected a candidate");
        assert_eq!(
            candidate.paths.len(),
            8,
            "picker should grab exactly max_files_per_pick files"
        );
        assert_eq!(candidate.total_bytes, 8 * 10 * 1024 * 1024);
    }

    #[test]
    fn picker_returns_none_when_only_one_file_above_target() {
        let cfg = default_cfg();
        let files = entries(&[512 * 1024 * 1024]);
        assert!(pick_candidates(files.iter().cloned(), &cfg).is_none());
    }

    #[test]
    fn picker_picks_smallest_files_first_within_tier() {
        // Cap max_files_per_pick = 8 so the picker MUST choose, and pick
        // sizes that make the smallest 8 exceed mid_max — otherwise the picker
        // correctly skips. Target = 128 MiB → small_max = 32 MiB.
        // Sizes 17..28 MiB are all in Small (< 32 MiB); smallest 8 sum to
        // 17+18+19+20+21+22+23+24 = 164 MiB > 128.
        let cfg = CompactionPickerConfig::new(8, 8, 128 * 1024 * 1024);
        let sizes_mib: [u64; 12] = [25, 17, 27, 19, 28, 21, 23, 18, 26, 20, 22, 24];
        let sizes: Vec<u64> = sizes_mib.iter().map(|m| m * 1024 * 1024).collect();
        let files = entries(&sizes);
        let candidate = pick_candidates(files.iter().cloned(), &cfg).expect("expected a candidate");
        assert_eq!(candidate.tier, Tier::Small);
        assert_eq!(candidate.paths.len(), 8);

        // The 8 smallest by size: 17..24 (MiB).
        let expected_bytes: u64 = (17_u64..=24).map(|mb| mb * 1024 * 1024).sum();
        assert_eq!(candidate.total_bytes, expected_bytes);
    }

    #[test]
    fn picker_promotes_to_mid_tier_when_small_tier_drained() {
        let cfg = default_cfg();
        // Simulate post-merge state: small tier is empty, mid tier has 8 files
        // totaling > 256 MiB.
        let files = entries(&[64 * 1024 * 1024; 8]);
        let candidate = pick_candidates(files.iter().cloned(), &cfg).expect("expected a candidate");
        assert_eq!(candidate.tier, Tier::Mid);
    }

    #[test]
    fn picker_skips_settled_files() {
        let cfg = default_cfg();
        // All files at exactly target size — none are candidates.
        let files = entries(&[256 * 1024 * 1024; 16]);
        assert!(pick_candidates(files.iter().cloned(), &cfg).is_none());
    }

    #[test]
    fn picker_threshold_uses_tier_total_not_picked_subset() {
        // Regression: 100 files of 2 MiB each (200 MiB tier total) used to be
        // skipped because the smallest 32 only sum to 64 MiB. The eligibility
        // check should consider the whole tier's bytes, not just the picked
        // subset — otherwise tiny-but-numerous files would never trigger
        // compaction.
        let cfg = CompactionPickerConfig::new(8, 32, 128 * 1024 * 1024);
        let files = entries(&[2 * 1024 * 1024; 100]);
        let candidate = pick_candidates(files.iter().cloned(), &cfg)
            .expect("expected a candidate from 100 small files");
        assert_eq!(candidate.tier, Tier::Small);
        assert_eq!(candidate.paths.len(), 32);
        // `total_bytes` on the candidate reports the picked subset, not the
        // whole tier — 32 * 2 MiB.
        assert_eq!(candidate.total_bytes, 32 * 2 * 1024 * 1024);
    }

    #[test]
    fn picker_config_enforces_minimum_trigger_files() {
        // trigger_files=0 should be clamped to 2 (a single file can't be
        // compacted).
        let cfg = CompactionPickerConfig::new(0, 32, 128 * 1024 * 1024);
        assert!(cfg.trigger_files >= 2);
    }

    #[test]
    fn picker_config_enforces_minimum_max_files_per_pick() {
        // max_files_per_pick=0 should be clamped to 2 as well.
        let cfg = CompactionPickerConfig::new(8, 0, 128 * 1024 * 1024);
        assert!(cfg.max_files_per_pick >= 2);
    }

    // ------------------------------------------------------------------
    // BackgroundCompactor smoke tests
    // ------------------------------------------------------------------

    struct CountingRunner {
        name: String,
        calls: Arc<std::sync::atomic::AtomicU32>,
    }

    #[async_trait::async_trait]
    impl CompactionRunner for CountingRunner {
        async fn run_compaction_trigger(&self) -> Result<bool, String> {
            self.calls
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            Ok(false)
        }

        fn compaction_target_name(&self) -> &str {
            &self.name
        }
    }

    #[tokio::test(start_paused = true)]
    async fn background_compactor_ticks_at_interval_and_stops_on_shutdown() {
        let calls = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let runner = Arc::new(CountingRunner {
            name: "test_table".to_string(),
            calls: Arc::clone(&calls),
        });

        let weak: Weak<dyn CompactionRunner> =
            Arc::downgrade(&runner) as Weak<dyn CompactionRunner>;
        let semaphore = Arc::new(Semaphore::new(1));
        let compactor = BackgroundCompactor::spawn(weak, Duration::from_secs(1), semaphore)
            .expect("scheduler should spawn with non-zero interval");

        // Advance a few intervals.
        for _ in 0..3 {
            tokio::time::advance(Duration::from_secs(1)).await;
            tokio::task::yield_now().await;
            tokio::task::yield_now().await;
        }

        // Dropping the compactor signals shutdown and aborts the task.
        drop(compactor);

        let observed = calls.load(std::sync::atomic::Ordering::Relaxed);
        assert!(
            (1..=5).contains(&observed),
            "expected background task to fire between 1 and 5 times, got {observed}"
        );
    }

    #[test]
    fn background_compactor_returns_none_when_interval_is_zero() {
        let runner = Arc::new(CountingRunner {
            name: "test_table".to_string(),
            calls: Arc::new(std::sync::atomic::AtomicU32::new(0)),
        });
        let weak: Weak<dyn CompactionRunner> =
            Arc::downgrade(&runner) as Weak<dyn CompactionRunner>;
        let semaphore = Arc::new(Semaphore::new(1));
        assert!(BackgroundCompactor::spawn(weak, Duration::ZERO, semaphore).is_none());
    }
}
