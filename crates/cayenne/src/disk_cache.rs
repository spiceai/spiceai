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

//! The node's disk cache of the data files Cayenne keeps in an object-store
//! location: one cache per process, shared by every table whose data is there.
//!
//! The runtime opens the cache and [installs](install) it before any dataset
//! loads; the accelerator wraps the location's data store with it, and each
//! table keeps what the cache holds of it in step with its committed state
//! (see `CayenneTableProvider::reconcile_disk_cache`).

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::future::Future;
use std::ops::Range;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use object_store::ObjectStore;
use object_store::path::Path;
use object_store_cache::{Cacheable, NodeCache};
use parking_lot::{Mutex, RwLock};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

use crate::CAYENNE_DOCS_URL as CAYENNE_DOCS;
use crate::dataset_storage::DataFileKind;

/// How often cache errors since the last report are reported, at most once.
const FALLBACK_REPORT_PERIOD: Duration = Duration::from_secs(60);

/// The bytes at the end of a data file the first read of a scan fetches.
pub const TAIL_BYTES: u64 = vortex_datafusion::DEFAULT_FOOTER_INITIAL_READ_SIZE_BYTES as u64;

/// The share of the cache's capacity, in tenths, prefetched file tails may
/// take before whole files are prefetched.
const TAIL_SHARE_TENTHS: u64 = 1;

/// The least time a shutdown gives the cache to close, however little of its
/// timeout is left: closing writes one index file.
const CLOSE_FLOOR: Duration = Duration::from_secs(1);

/// How often a table's failed reconciliation rounds are reported, at most once.
const RECONCILE_WARN_PERIOD: Duration = Duration::from_secs(60);

struct Installed {
    cache: Arc<NodeCache>,
    dir: PathBuf,
    /// Cancelled once, by [`close_process_disk_cache`] under the write lock of
    /// [`PROCESS_DISK_CACHE`].
    closed: CancellationToken,
}

/// The process's disk cache. A cache closed by a runtime's shutdown is
/// replaced by the next runtime that installs one.
static PROCESS_DISK_CACHE: RwLock<Option<Arc<Installed>>> = RwLock::new(None);

/// Make `cache`, opened in `dir`, the process's disk cache, and start
/// reporting its errors. Returns false, and changes nothing, when an open
/// cache is installed already: a second runtime in one process keeps the
/// first one's. A closed one is replaced.
pub fn install(cache: Arc<NodeCache>, dir: PathBuf) -> bool {
    let installed = Arc::new(Installed {
        cache,
        dir,
        closed: CancellationToken::new(),
    });
    {
        let mut current = PROCESS_DISK_CACHE.write();
        if current
            .as_ref()
            .is_some_and(|open| !open.closed.is_cancelled())
        {
            return false;
        }
        *current = Some(Arc::clone(&installed));
    }
    if let Ok(runtime) = tokio::runtime::Handle::try_current() {
        runtime.spawn(report_fallbacks(installed));
    }
    true
}

/// The process's disk cache, when an open one is installed.
#[must_use]
pub fn process_disk_cache() -> Option<Arc<NodeCache>> {
    PROCESS_DISK_CACHE
        .read()
        .as_ref()
        .filter(|installed| !installed.closed.is_cancelled())
        .map(|installed| Arc::clone(&installed.cache))
}

/// Close the process's disk cache, once, giving it `within` (at least
/// [`CLOSE_FLOOR`]): prefetch stops, running entry writes finish, and the slab
/// being written is sealed so the next start does not rescan it. Reads keep
/// working afterwards, without filling the cache.
pub async fn close_process_disk_cache(within: Duration) {
    let installed = {
        let current = PROCESS_DISK_CACHE.write();
        let Some(installed) = current.as_ref() else {
            return;
        };
        if installed.closed.is_cancelled() {
            return;
        }
        installed.closed.cancel();
        Arc::clone(installed)
    };
    let within = within.max(CLOSE_FLOOR);
    let dir = installed.dir.display().to_string();
    match tokio::time::timeout(within, installed.cache.close()).await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => tracing::warn!("{}", close_failure_warning(&dir, &error.to_string())),
        Err(_) => tracing::warn!("{}", close_timeout_warning(&dir, within)),
    }
}

/// The WARN for a close that failed with `cause`.
fn close_failure_warning(dir: &str, cause: &str) -> String {
    format!(
        "Failed to close the Cayenne disk cache at '{dir}' cleanly, so the next start rescans its last file and takes longer; no data is lost. Cause: {cause}. See: {CAYENNE_DOCS}"
    )
}

/// The WARN for a close that did not finish within `waited`.
fn close_timeout_warning(dir: &str, waited: Duration) -> String {
    format!(
        "Failed to close the Cayenne disk cache at '{dir}' within {:.1} s of shutdown, so the next start rescans its last file and takes longer; no data is lost. See: {CAYENNE_DOCS}",
        waited.as_secs_f64()
    )
}

/// Every minute, one WARN when `installed` had errors since the last one,
/// until it is closed.
async fn report_fallbacks(installed: Arc<Installed>) {
    let mut reported = 0_u64;
    loop {
        tokio::select! {
            () = installed.closed.cancelled() => return,
            () = tokio::time::sleep(FALLBACK_REPORT_PERIOD) => {}
        }
        let fallbacks = installed.cache.stats().fallbacks;
        if fallbacks > reported {
            tracing::warn!(
                "{}",
                fallback_warning(
                    &installed.dir.display().to_string(),
                    fallbacks - reported,
                    installed.cache.last_error().as_deref(),
                )
            );
            reported = fallbacks;
        }
    }
}

/// The WARN for `count` cache errors in the last report period. An error can
/// come from a read, a fill or an eviction; only a read that hits one goes to
/// the object store.
fn fallback_warning(dir: &str, count: u64, cause: Option<&str>) -> String {
    let cause = cause.unwrap_or("a cached block did not verify");
    format!(
        "The Cayenne disk cache at '{dir}' had {count} error(s) in the last minute reading, writing or evicting cached blocks, so the reads it could not serve went to the object store: results are unchanged, but those queries were slower. Check the disk that holds '{dir}'. Last cause: {cause}. See: {CAYENNE_DOCS}"
    )
}

/// [`DiskCacheBinding::cacheable`] for a location that keeps its datasets
/// under `datasets`.
fn cacheable(datasets: &Path) -> Cacheable {
    let datasets = datasets.clone();
    Arc::new(move |path: &Path| {
        crate::metastore::object_native::under_dataset_data(&datasets, path)
            .and_then(DataFileKind::of)
            .is_some()
    })
}

/// The process's cache, and the namespace under which it holds one location's
/// data files.
#[derive(Clone)]
pub struct DiskCacheBinding {
    cache: Arc<NodeCache>,
    namespace: Arc<str>,
    datasets: Path,
}

impl DiskCacheBinding {
    /// `cache`, holding one location's data files under `namespace`; the
    /// location keeps its datasets under `datasets`.
    #[must_use]
    pub fn new(cache: Arc<NodeCache>, namespace: &str, datasets: Path) -> Self {
        Self {
            cache,
            namespace: Arc::from(namespace),
            datasets,
        }
    }

    /// The paths the cache may hold, judged by the path alone: data files and
    /// deletion vectors, which are written once and never change, under the
    /// data path of a dataset folder in the location's datasets
    /// (`cayenne/v1/datasets`). Only the part of a path under a data path is
    /// classified, so the location's own prefix never makes an object
    /// cacheable. Staged files, lookup-index runs and every other object pass
    /// through uncached. Whether a file is committed is not checked here: the
    /// table's reconciler evicts what its committed state no longer names.
    #[must_use]
    pub fn cacheable(&self) -> Cacheable {
        cacheable(&self.datasets)
    }

    /// The process's cache.
    #[must_use]
    pub fn cache(&self) -> &Arc<NodeCache> {
        &self.cache
    }

    /// The namespace the location's data files are held under.
    #[must_use]
    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    /// `store`, with reads of [`Self::cacheable`] paths served through the
    /// cache.
    #[must_use]
    pub fn wrap(&self, store: Arc<dyn ObjectStore>) -> Arc<dyn ObjectStore> {
        self.cache.wrap(&self.namespace, store, self.cacheable())
    }
}

impl fmt::Debug for DiskCacheBinding {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DiskCacheBinding")
            .field("namespace", &self.namespace)
            .finish_non_exhaustive()
    }
}

/// A file of a table's committed state the reconciler may prefetch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PrefetchCandidate {
    pub path: Path,
    pub size: u64,
    /// The newest sequence the file holds; newer files are prefetched first.
    pub seq: i64,
    pub deletion: bool,
}

/// One prefetch of the `size`-byte file at `path`: `range` of it, or the whole
/// file when `range` is `None`.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Prefetch {
    pub path: Path,
    pub size: u64,
    pub range: Option<Range<u64>>,
}

/// The last [`TAIL_BYTES`] of a file of `size` bytes.
pub(crate) fn tail_range(size: u64) -> Range<u64> {
    size.saturating_sub(TAIL_BYTES)..size
}

/// The bytes of the `block_bytes` blocks that hold `range` of a `size`-byte
/// file: what a prefetch of `range` fetches.
fn block_cost(range: &Range<u64>, size: u64, block_bytes: u64) -> u64 {
    if range.is_empty() || block_bytes == 0 {
        return 0;
    }
    let first = range.start / block_bytes * block_bytes;
    let last_end = (range.end - 1) / block_bytes * block_bytes + block_bytes;
    last_end.min(size) - first
}

/// The prefetches for `candidates`, in the order they run, newest first: the
/// tail of each data file while the tails' blocks stay within a tenth of
/// `capacity`, so a scan opens a file without a store read; then each deletion
/// vector whole and each data file whole, into whatever room the cache has.
pub(crate) fn plan_prefetch(
    mut candidates: Vec<PrefetchCandidate>,
    block_bytes: u64,
    capacity: u64,
) -> Vec<Prefetch> {
    candidates.retain(|candidate| candidate.size > 0);
    // Files of one sequence (appends to a table without a key all have one)
    // are ordered by name, newest first: Cayenne names a file with a
    // time-ordered id.
    candidates.sort_by(|a, b| b.seq.cmp(&a.seq).then_with(|| b.path.cmp(&a.path)));
    let cap = capacity / 10 * TAIL_SHARE_TENTHS;
    let mut plan = Vec::with_capacity(candidates.len() * 2);
    let mut tail_bytes = 0_u64;
    for candidate in candidates.iter().filter(|candidate| !candidate.deletion) {
        let range = tail_range(candidate.size);
        let cost = block_cost(&range, candidate.size, block_bytes);
        if tail_bytes.saturating_add(cost) > cap {
            break;
        }
        tail_bytes += cost;
        plan.push(Prefetch {
            path: candidate.path.clone(),
            size: candidate.size,
            range: Some(range),
        });
    }
    let (deletions, data): (Vec<_>, Vec<_>) = candidates
        .into_iter()
        .partition(|candidate| candidate.deletion);
    plan.extend(deletions.into_iter().chain(data).map(|candidate| Prefetch {
        path: candidate.path,
        size: candidate.size,
        range: None,
    }));
    plan
}

/// Runs a table's reconciliation rounds: `round(full)` runs one and returns
/// what wakes the next, or `None` to stop. The first round is full; a wake (a
/// reload from the catalog) runs a round for the new files, and a full round
/// runs every `period` however often wakes come; rounds start at most one a
/// `gap`.
pub(crate) async fn reconcile_rounds<F, Fut>(mut round: F, period: Duration, gap: Duration)
where
    F: FnMut(bool) -> Fut,
    Fut: Future<Output = Option<Arc<Notify>>>,
{
    let mut full = true;
    let mut next_full = tokio::time::Instant::now() + period;
    loop {
        let started = tokio::time::Instant::now();
        if full {
            next_full = started + period;
        }
        let Some(wake) = round(full).await else {
            return;
        };
        tokio::select! {
            () = wake.notified() => {}
            () = tokio::time::sleep_until(next_full) => {}
        }
        tokio::time::sleep(gap.saturating_sub(started.elapsed())).await;
        full = tokio::time::Instant::now() >= next_full;
    }
}

/// What a table's reconciliation round failed to do.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReconcileFailure {
    /// Read the files the table's committed state names.
    ReadFiles,
    /// Queue a prefetch of them.
    Prefetch,
}

/// A table's view of the node disk cache: the cache its data store reads
/// through, and what its reconciler keeps between rounds.
pub(crate) struct TableDiskCache {
    pub binding: DiskCacheBinding,
    /// Wakes the reconciler after a reload from the catalog.
    pub wake: Arc<Notify>,
    /// The live file keys the last round saw: a round after a reload
    /// prefetches only keys not in it.
    pub seen: Mutex<HashSet<String>>,
    /// When a failed round was last reported at WARN.
    warned: Mutex<Option<Instant>>,
}

impl TableDiskCache {
    pub(crate) fn new(binding: DiskCacheBinding) -> Self {
        Self {
            binding,
            wake: Arc::new(Notify::new()),
            seen: Mutex::new(HashSet::new()),
            warned: Mutex::new(None),
        }
    }

    /// Report dataset `dataset`'s failed round: at WARN at most once a
    /// [`RECONCILE_WARN_PERIOD`], at DEBUG otherwise.
    pub(crate) fn report_failure(&self, dataset: &str, failure: ReconcileFailure, cause: &str) {
        let message = reconcile_warning(dataset, failure, cause);
        if reconcile_warning_due(&mut self.warned.lock(), Instant::now()) {
            tracing::warn!("{message}");
        } else {
            tracing::debug!("{message}");
        }
    }
}

impl fmt::Debug for TableDiskCache {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TableDiskCache")
            .field("binding", &self.binding)
            .finish_non_exhaustive()
    }
}

/// True when a failed round is reported now: none was within
/// [`RECONCILE_WARN_PERIOD`] before `now`, the last one being `last`.
fn reconcile_warning_due(last: &mut Option<Instant>, now: Instant) -> bool {
    if last.is_some_and(|at| now.saturating_duration_since(at) < RECONCILE_WARN_PERIOD) {
        return false;
    }
    *last = Some(now);
    true
}

/// The WARN for dataset `dataset`'s failed reconciliation round, for `cause`.
fn reconcile_warning(dataset: &str, failure: ReconcileFailure, cause: &str) -> String {
    match failure {
        ReconcileFailure::ReadFiles => format!(
            "Failed to read the committed files of dataset '{dataset}' for the Cayenne disk cache, so the cache neither prefetches the dataset's new files nor evicts the ones it no longer uses until a later attempt succeeds; query results are unchanged. Cause: {cause}. See: {CAYENNE_DOCS}"
        ),
        ReconcileFailure::Prefetch => format!(
            "Failed to prefetch the files of dataset '{dataset}' into the Cayenne disk cache, so queries read them from the object store until a later attempt succeeds; query results are unchanged. Cause: {cause}. See: {CAYENNE_DOCS}"
        ),
    }
}

/// The keys of a table's files the cache keeps: every live one, and every
/// retired one whose retention, timed from `timers` (when this process first
/// saw it retired), has not passed. A retired key with no timer is kept.
pub(crate) fn kept_keys<'a>(
    live: impl IntoIterator<Item = &'a str>,
    retired: &[String],
    timers: &HashMap<String, Instant>,
    now: Instant,
    retention: Duration,
) -> HashSet<String> {
    let mut keep: HashSet<String> = live.into_iter().map(str::to_string).collect();
    keep.extend(
        retired
            .iter()
            .filter(|key| {
                timers.get(key.as_str()).is_none_or(|&observed| {
                    crate::metastore_layout::retention_left(observed, now, retention).is_some()
                })
            })
            .cloned(),
    );
    keep
}

#[cfg(test)]
mod tests {
    use super::*;

    fn path(p: &str) -> Path {
        Path::from(p)
    }

    #[test]
    fn cacheable_admits_committed_data_files_and_deletion_vectors_only() {
        let cacheable = cacheable(&path("state/cayenne/v1/datasets"));
        for (p, expected) in [
            ("state/cayenne/v1/datasets/t-1/data/9/a.vortex", true),
            (
                "state/cayenne/v1/datasets/t-1/data/s1/deletions/d.arrow",
                true,
            ),
            (
                "state/cayenne/v1/datasets/t-1/data/9/_staging/w/a.vortex",
                false,
            ),
            (
                "state/cayenne/v1/datasets/t-1/data/9/_staging_wal/a.vortex",
                false,
            ),
            (
                "state/cayenne/v1/datasets/t-1/data/9/_lookup_index/k/r.run",
                false,
            ),
            ("state/cayenne/v1/datasets/t-1/data/9/a.arrow", false),
            ("state/cayenne/v1/datasets/t-1/head", false),
            ("state/cayenne/v1/catalog/log/00000001", false),
        ] {
            assert_eq!(cacheable(&path(p)), expected, "{p}");
        }
    }

    /// Only what is under a dataset folder's data path is classified: the
    /// location's prefix, a `deletions` segment in it included, and objects
    /// beside the data path never make an object cacheable.
    #[test]
    fn cacheable_classifies_only_paths_under_a_dataset_data_path() {
        let cacheable = cacheable(&path("b/deletions/cayenne/v1/datasets"));
        for (p, expected) in [
            ("b/deletions/cayenne/v1/datasets/t-1/data/9/a.vortex", true),
            (
                "b/deletions/cayenne/v1/datasets/t-1/data/s1/deletions/d.arrow",
                true,
            ),
            (
                "b/deletions/cayenne/v1/datasets/t-1/data/9/_lookup_index/k/r.run",
                false,
            ),
            (
                "b/deletions/cayenne/v1/datasets/t-1/data/9/_staging/w/a.vortex",
                false,
            ),
            ("b/deletions/cayenne/v1/datasets/t-1/data/9/a.arrow", false),
            ("b/deletions/cayenne/v1/datasets/t-1/schema.vortex", false),
            ("b/deletions/cayenne/v1/datasets/t-1/head.arrow", false),
            ("b/deletions/cayenne/v1/catalog/log/00000001.arrow", false),
            ("b/deletions/cayenne/v1/datasets/data/a.vortex", false),
            ("b/deletions/d.arrow", false),
            ("other/cayenne/v1/datasets/t-1/data/9/a.vortex", false),
        ] {
            assert_eq!(cacheable(&path(p)), expected, "{p}");
        }
    }

    /// The fallback reporter stops when the cache closes, not at its next
    /// report.
    #[cfg(unix)]
    #[tokio::test(start_paused = true)]
    async fn fallback_reporter_exits_when_the_cache_closes() {
        let dir = tempfile::tempdir().expect("temp dir");
        let cache = NodeCache::open(object_store_cache::NodeCacheConfig::new(
            dir.path(),
            64 << 20,
        ))
        .await
        .expect("open the cache");
        let installed = Arc::new(Installed {
            cache,
            dir: dir.path().to_path_buf(),
            closed: CancellationToken::new(),
        });
        let reporter = tokio::spawn(report_fallbacks(Arc::clone(&installed)));
        tokio::task::yield_now().await;
        let closed_at = tokio::time::Instant::now();
        installed.closed.cancel();
        reporter.await.expect("the reporter task");
        assert_eq!(closed_at.elapsed(), Duration::ZERO);
    }

    /// With reloads every minute, a full round still runs every period.
    #[tokio::test(start_paused = true)]
    async fn reconcile_rounds_run_a_full_round_every_period_despite_wakes() {
        let wake = Arc::new(Notify::new());
        let start = tokio::time::Instant::now();
        let waker = {
            let wake = Arc::clone(&wake);
            tokio::spawn(async move {
                loop {
                    tokio::time::sleep(Duration::from_secs(60)).await;
                    wake.notify_one();
                }
            })
        };
        let rounds = Arc::new(parking_lot::Mutex::new(Vec::new()));
        reconcile_rounds(
            |full| {
                let at = start.elapsed().as_secs();
                rounds.lock().push((at, full));
                let wake = Arc::clone(&wake);
                async move { (at < 16 * 60).then_some(wake) }
            },
            Duration::from_secs(300),
            Duration::from_secs(1),
        )
        .await;
        waker.abort();
        let rounds = rounds.lock().clone();
        // A full round every five minutes; a reload at the same instant still
        // gets a round of its own, one gap later.
        let mut expected = Vec::new();
        for minute in 0..=16_u64 {
            let at = minute * 60;
            let full = at % 300 == 0;
            expected.push((at, full));
            if full && at > 0 {
                expected.push((at + 1, false));
            }
        }
        assert_eq!(rounds, expected);
    }

    #[test]
    fn reconcile_warnings_name_the_dataset_and_come_once_a_minute() {
        assert_eq!(
            reconcile_warning(
                "orders",
                ReconcileFailure::ReadFiles,
                "the head is unreadable"
            ),
            "Failed to read the committed files of dataset 'orders' for the Cayenne disk cache, so the cache neither prefetches the dataset's new files nor evicts the ones it no longer uses until a later attempt succeeds; query results are unchanged. Cause: the head is unreadable. See: https://spiceai.org/docs/components/data-accelerators/cayenne"
        );
        assert_eq!(
            reconcile_warning("orders", ReconcileFailure::Prefetch, "no store"),
            "Failed to prefetch the files of dataset 'orders' into the Cayenne disk cache, so queries read them from the object store until a later attempt succeeds; query results are unchanged. Cause: no store. See: https://spiceai.org/docs/components/data-accelerators/cayenne"
        );
        let now = Instant::now();
        let mut last = None;
        let due: Vec<bool> = [0, 30, 59, 60, 61, 125]
            .into_iter()
            .map(|secs| reconcile_warning_due(&mut last, now + Duration::from_secs(secs)))
            .collect();
        assert_eq!(due, [true, false, false, true, false, true]);
    }

    #[test]
    fn close_timeout_warning_names_the_dir_and_the_wait() {
        assert_eq!(
            close_timeout_warning("/data/cache/objects", Duration::from_millis(1500)),
            "Failed to close the Cayenne disk cache at '/data/cache/objects' within 1.5 s of shutdown, so the next start rescans its last file and takes longer; no data is lost. See: https://spiceai.org/docs/components/data-accelerators/cayenne"
        );
    }

    #[test]
    fn close_failure_warning_names_the_dir_the_cause_and_the_docs() {
        assert_eq!(
            close_failure_warning("/data/cache/objects", "disk full"),
            "Failed to close the Cayenne disk cache at '/data/cache/objects' cleanly, so the next start rescans its last file and takes longer; no data is lost. Cause: disk full. See: https://spiceai.org/docs/components/data-accelerators/cayenne"
        );
    }

    #[test]
    fn kept_keys_hold_live_files_and_retired_files_within_retention() {
        let now = Instant::now();
        let retention = Duration::from_secs(3600);
        let old = now.checked_sub(retention * 2).expect("instant in the past");
        let timers = HashMap::from([
            ("fresh.vortex".to_string(), now),
            ("expired.vortex".to_string(), old),
        ]);
        let retired = [
            "fresh.vortex".to_string(),
            "expired.vortex".to_string(),
            "untimed.vortex".to_string(),
        ];
        let mut kept: Vec<String> = kept_keys(["live.vortex"], &retired, &timers, now, retention)
            .into_iter()
            .collect();
        kept.sort();
        assert_eq!(kept, ["fresh.vortex", "live.vortex", "untimed.vortex"]);
    }

    #[test]
    fn plan_prefetch_orders_newest_first_and_caps_tails_at_a_tenth() {
        const BLOCK: u64 = 64 << 10;
        let candidate = |p: &str, size, seq, deletion| PrefetchCandidate {
            path: path(p),
            size,
            seq,
            deletion,
        };
        // Each 1 MiB file's tail (64 KiB less a byte) is in its last 64 KiB
        // block. A 1.25 MiB capacity allows 128 KiB of tails: two.
        let plan = plan_prefetch(
            vec![
                candidate("old.vortex", 1 << 20, 1, false),
                candidate("new.vortex", 1 << 20, 3, false),
                candidate("mid.vortex", 1 << 20, 2, false),
                candidate("empty.vortex", 0, 9, false),
                candidate("deletions/d.arrow", 100, 2, true),
                candidate("a.vortex", 1 << 20, 1, false),
            ],
            BLOCK,
            (5 << 20) / 4,
        );
        assert_eq!(TAIL_BYTES, (64 << 10) - 1);
        let tail = (1 << 20) - TAIL_BYTES..1 << 20;
        let prefetch = |p: &str, size, range| Prefetch {
            path: path(p),
            size,
            range,
        };
        assert_eq!(
            plan,
            vec![
                prefetch("new.vortex", 1 << 20, Some(tail.clone())),
                prefetch("mid.vortex", 1 << 20, Some(tail)),
                prefetch("deletions/d.arrow", 100, None),
                prefetch("new.vortex", 1 << 20, None),
                prefetch("mid.vortex", 1 << 20, None),
                prefetch("old.vortex", 1 << 20, None),
                prefetch("a.vortex", 1 << 20, None),
            ]
        );
        // A tail across a block boundary costs both blocks; a file smaller
        // than its tail costs its own bytes only.
        assert_eq!(
            block_cost(&(BLOCK - 1..BLOCK + 1), 4 * BLOCK, BLOCK),
            2 * BLOCK
        );
        assert_eq!(block_cost(&tail_range(10), 10, BLOCK), 10);
        assert_eq!(tail_range(10), 0..10);
    }

    #[test]
    fn fallback_warning_names_the_dir_the_count_and_the_cause() {
        assert_eq!(
            fallback_warning(
                "/data/cache/objects",
                3,
                Some("Failed to read cache file x")
            ),
            "The Cayenne disk cache at '/data/cache/objects' had 3 error(s) in the last minute reading, writing or evicting cached blocks, so the reads it could not serve went to the object store: results are unchanged, but those queries were slower. Check the disk that holds '/data/cache/objects'. Last cause: Failed to read cache file x. See: https://spiceai.org/docs/components/data-accelerators/cayenne"
        );
    }
}
