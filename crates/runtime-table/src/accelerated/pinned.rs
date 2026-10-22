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

//! Serving a pinned snapshot on an instance that refreshes the dataset from its
//! source. While the dataset is pinned, queries read a second local copy loaded from
//! the pinned snapshot, and refreshes and publishes keep using the live acceleration.

use std::any::Any;
use std::sync::{Arc, Weak};
use std::time::Duration;

use async_trait::async_trait;
use datafusion::datasource::TableProvider;
use tokio::sync::{Notify, watch};

pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// Held by a scan of a pinned copy until the scan's output streams are dropped.
pub type CopyLease = Arc<dyn Any + Send + Sync>;

/// How often a stopped watcher's copies are checked for scans that still read them.
const RELEASE_CHECK_INTERVAL: Duration = Duration::from_secs(10);

/// Why an instance does not serve the snapshot its dataset is pinned to. It keeps
/// serving what it served.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NotServed {
    /// The disk has too little free space to restore the pinned snapshot.
    NotEnoughDisk {
        snapshot_id: u64,
        needed: u64,
        free: u64,
    },
    /// The pinned snapshot could not be loaded.
    LoadFailed { reason: String },
    /// The dataset uses `refresh_mode: caching`, which serves no pin.
    CachingMode,
}

impl std::fmt::Display for NotServed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotEnoughDisk {
                snapshot_id,
                needed,
                free,
            } => write!(
                f,
                "not enough disk for snapshot {snapshot_id} ({} needed, {} free)",
                human_bytes(*needed),
                human_bytes(*free)
            ),
            Self::LoadFailed { reason } => write!(f, "the pinned copy failed to load: {reason}"),
            Self::CachingMode => write!(f, "caching mode"),
        }
    }
}

impl NotServed {
    /// Whether `other` is the same reason, whatever its details: the free disk space or a
    /// failed load's message can change from one check to the next.
    fn same_reason(&self, other: &Self) -> bool {
        match (self, other) {
            (
                Self::NotEnoughDisk { snapshot_id, .. },
                Self::NotEnoughDisk {
                    snapshot_id: other, ..
                },
            ) => snapshot_id == other,
            (Self::LoadFailed { .. }, Self::LoadFailed { .. })
            | (Self::CachingMode, Self::CachingMode) => true,
            _ => false,
        }
    }
}

/// `bytes` in the largest binary unit it reaches, to one decimal: `2.1 GiB`.
fn human_bytes(bytes: u64) -> String {
    const UNITS: [(&str, u64); 3] = [("GiB", 1 << 30), ("MiB", 1 << 20), ("KiB", 1 << 10)];
    for (unit, size) in UNITS {
        if bytes >= size {
            let tenths = (u128::from(bytes) * 10 + u128::from(size) / 2) / u128::from(size);
            return format!("{}.{} {unit}", tenths / 10, tenths % 10);
        }
    }
    format!("{bytes} B")
}

/// What an accelerated table serves of its dataset's pin.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PinState {
    /// The snapshot the dataset is pinned to, as the table last read it.
    pub pinned: Option<u64>,
    /// The snapshot the table serves: the pinned copy's on an instance that refreshes
    /// from the source, the loaded one on a reader. `None` while an instance that
    /// refreshes from the source serves its live data.
    pub serves: Option<u64>,
    /// Why the table does not serve `pinned`.
    pub not_served: Option<NotServed>,
}

impl PinState {
    /// Whether the table serves the snapshot its dataset is pinned to.
    #[must_use]
    pub fn serves_pin(&self) -> bool {
        self.pinned.is_some() && self.serves == self.pinned
    }

    /// Whether `other` is pinned to or serves another snapshot, or does not serve the pin
    /// for another reason.
    fn differs_from(&self, other: &Self) -> bool {
        self.pinned != other.pinned
            || self.serves != other.serves
            || match (&self.not_served, &other.not_served) {
                (Some(reason), Some(other)) => !reason.same_reason(other),
                (None, None) => false,
                _ => true,
            }
    }
}

/// A table's [`PinState`], shared by what loads its pinned snapshot, its scans, and
/// what reports it.
#[derive(Debug)]
pub struct PinStatus {
    state: watch::Sender<PinState>,
    /// The pinned snapshot the table last served, while it still serves it: a table
    /// that cannot load the snapshot the pin moves to keeps serving this one.
    served_pin: parking_lot::Mutex<Option<u64>>,
}

impl Default for PinStatus {
    fn default() -> Self {
        Self {
            state: watch::Sender::new(PinState::default()),
            served_pin: parking_lot::Mutex::new(None),
        }
    }
}

impl PinStatus {
    #[must_use]
    pub fn get(&self) -> PinState {
        self.state.borrow().clone()
    }

    /// Whether the table serves the snapshot its dataset is pinned to.
    #[must_use]
    pub fn serves_pin(&self) -> bool {
        self.state.borrow().serves_pin()
    }

    /// Whether the table serves a snapshot it served as its dataset's pin: the pinned
    /// one, or the one it keeps serving because it cannot load the snapshot the pin
    /// moved to.
    #[must_use]
    pub fn serves_pinned_snapshot(&self) -> bool {
        let serves = self.state.borrow().serves;
        serves.is_some() && serves == *self.served_pin.lock()
    }

    /// The state, and each change to it.
    #[must_use]
    pub fn subscribe(&self) -> watch::Receiver<PinState> {
        self.state.subscribe()
    }

    /// Replaces the state. Returns whether it changed other than in a reason's details,
    /// and notifies subscribers only then.
    #[must_use]
    pub fn set(&self, state: PinState) -> bool {
        {
            let mut served_pin = self.served_pin.lock();
            if state.serves_pin() {
                *served_pin = state.serves;
            } else if state.serves != *served_pin {
                *served_pin = None;
            }
        }
        self.state.send_if_modified(|current| {
            let changed = current.differs_from(&state);
            *current = state;
            changed
        })
    }
}

/// Loads and releases the local copies of a dataset's pinned snapshot.
#[async_trait]
pub trait PinnedCopyLoader: Send + Sync {
    /// The snapshot the dataset is pinned to, or `None` when it is not pinned.
    async fn pinned_snapshot(&self) -> Result<Option<u64>, BoxError>;

    /// Downloads snapshot `snapshot_id` into a new local copy and opens it.
    async fn load(&self, snapshot_id: u64) -> Result<Arc<dyn TableProvider>, NotServed>;

    /// Closes and deletes the copy of `snapshot_id`, including a partial one left by a
    /// failed [`Self::load`].
    async fn release(&self, snapshot_id: u64);
}

/// A loaded copy of a pinned snapshot.
struct PinnedCopy {
    snapshot_id: u64,
    provider: Arc<dyn TableProvider>,
    /// Held by each scan of the copy until its output streams are dropped.
    lease: Arc<()>,
}

/// A copy no longer served, kept until no scan holds its lease.
struct RetiredCopy {
    snapshot_id: u64,
    provider: Arc<dyn TableProvider>,
    lease: Weak<()>,
}

impl RetiredCopy {
    fn of(copy: &PinnedCopy) -> Self {
        Self {
            snapshot_id: copy.snapshot_id,
            provider: Arc::clone(&copy.provider),
            lease: Arc::downgrade(&copy.lease),
        }
    }
}

/// Releases each copy no scan holds, and returns the others.
async fn release_unread(
    loader: &dyn PinnedCopyLoader,
    copies: Vec<RetiredCopy>,
) -> Vec<RetiredCopy> {
    let (unread, read) = copies
        .into_iter()
        .partition::<Vec<_>, _>(|copy| copy.lease.strong_count() == 0);
    for copy in unread {
        loader.release(copy.snapshot_id).await;
    }
    read
}

/// The copy of the pinned snapshot an accelerated table serves, when its dataset is
/// pinned and the copy is loaded.
#[derive(Default)]
pub struct PinnedServing {
    copy: parking_lot::RwLock<Option<Arc<PinnedCopy>>>,
}

impl std::fmt::Debug for PinnedServing {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PinnedServing")
            .field("snapshot_id", &self.snapshot_id())
            .finish()
    }
}

impl PinnedServing {
    /// The provider queries read instead of the live acceleration, and the lease a scan
    /// of it holds until its output streams are dropped: the copy's files are deleted
    /// only once no scan holds it.
    #[must_use]
    pub fn provider(&self) -> Option<(Arc<dyn TableProvider>, CopyLease)> {
        self.copy.read().as_ref().map(|copy| {
            (
                Arc::clone(&copy.provider),
                Arc::clone(&copy.lease) as CopyLease,
            )
        })
    }

    /// The snapshot being served.
    #[must_use]
    pub fn snapshot_id(&self) -> Option<u64> {
        self.copy.read().as_ref().map(|copy| copy.snapshot_id)
    }

    fn current(&self) -> Option<Arc<PinnedCopy>> {
        self.copy.read().clone()
    }

    fn replace(&self, copy: Option<PinnedCopy>) {
        *self.copy.write() = copy.map(Arc::new);
    }
}

/// Keeps a table's [`PinnedServing`] in step with its dataset's pin.
pub struct PinWatcher {
    dataset_name: String,
    loader: Arc<dyn PinnedCopyLoader>,
    serving: Arc<PinnedServing>,
    status: Arc<PinStatus>,
    /// Copies no longer served that a query may still read.
    retired: Vec<RetiredCopy>,
    /// Called after the served data changes, to drop what was cached from the old data.
    on_change: Box<dyn Fn() -> futures::future::BoxFuture<'static, ()> + Send + Sync>,
}

impl PinWatcher {
    pub fn new(
        dataset_name: impl Into<String>,
        loader: Arc<dyn PinnedCopyLoader>,
        serving: Arc<PinnedServing>,
        status: Arc<PinStatus>,
        on_change: Box<dyn Fn() -> futures::future::BoxFuture<'static, ()> + Send + Sync>,
    ) -> Self {
        Self {
            dataset_name: dataset_name.into(),
            loader,
            serving,
            status,
            retired: Vec::new(),
            on_change,
        }
    }

    /// Reads the dataset's pin and serves the copy it names: loads a newly pinned
    /// snapshot, and serves the live data when the dataset is unpinned. A pinned snapshot
    /// that cannot be loaded leaves the table serving what it served. Then releases each
    /// retired copy no scan reads.
    pub async fn check(&mut self) {
        self.follow_pin().await;
        self.retired =
            release_unread(self.loader.as_ref(), std::mem::take(&mut self.retired)).await;
    }

    async fn follow_pin(&mut self) {
        let pinned = match self.loader.pinned_snapshot().await {
            Ok(pinned) => pinned,
            Err(err) => {
                tracing::warn!(
                    dataset = %self.dataset_name,
                    "Could not read whether dataset '{}' is pinned, so it keeps serving what it served: {err}",
                    self.dataset_name
                );
                return;
            }
        };
        let current = self.serving.current();

        match (pinned, current) {
            (None, None) => {
                self.report(None, None, None);
            }
            (Some(pinned), Some(current)) if current.snapshot_id == pinned => {
                self.report(Some(pinned), Some(pinned), None);
            }
            (None, Some(current)) => {
                self.report(None, None, None);
                self.serving.replace(None);
                self.retire(&current);
                tracing::info!(
                    dataset = %self.dataset_name,
                    "Dataset '{}' is no longer pinned: serving its live data instead of snapshot {}.",
                    self.dataset_name,
                    current.snapshot_id
                );
                (self.on_change)().await;
            }
            (Some(pinned), current) => {
                let copy = match self
                    .retired
                    .iter()
                    .position(|copy| copy.snapshot_id == pinned)
                {
                    // A copy a query still reads is served again rather than loaded over.
                    Some(index) => {
                        let retired = self.retired.swap_remove(index);
                        Ok(PinnedCopy {
                            snapshot_id: pinned,
                            provider: retired.provider,
                            lease: retired.lease.upgrade().unwrap_or_default(),
                        })
                    }
                    None => self.loader.load(pinned).await.map(|provider| PinnedCopy {
                        snapshot_id: pinned,
                        provider,
                        lease: Arc::default(),
                    }),
                };
                match copy {
                    Ok(copy) => {
                        self.serving.replace(Some(copy));
                        self.report(Some(pinned), Some(pinned), None);
                        if let Some(current) = &current {
                            self.retire(current);
                        }
                        tracing::info!(
                            dataset = %self.dataset_name,
                            "Dataset '{}' is pinned to snapshot {pinned}: serving it from a second local copy, while refreshes and publishes continue from the live data.",
                            self.dataset_name
                        );
                        (self.on_change)().await;
                    }
                    Err(not_served) => {
                        self.loader.release(pinned).await;
                        let serves = current.as_ref().map(|current| current.snapshot_id);
                        if self.report(Some(pinned), serves, Some(not_served.clone())) {
                            let kept = serves.map_or_else(
                                || "its live data".to_string(),
                                |id| format!("snapshot {id}"),
                            );
                            tracing::warn!(
                                dataset = %self.dataset_name,
                                "Dataset '{}' is pinned to snapshot {pinned}, but this instance does not serve it, so it keeps serving {kept} and tries again at its next refresh: {not_served}",
                                self.dataset_name
                            );
                        }
                    }
                }
            }
        }
    }

    /// Records what the table serves of the pin. Returns whether that changed.
    fn report(
        &self,
        pinned: Option<u64>,
        serves: Option<u64>,
        not_served: Option<NotServed>,
    ) -> bool {
        self.status.set(PinState {
            pinned,
            serves,
            not_served,
        })
    }

    fn retire(&mut self, copy: &PinnedCopy) {
        self.retired.push(RetiredCopy::of(copy));
    }

    /// Checks at once, then again each time `wake` is notified and, when `period` is
    /// set, at least that often.
    pub async fn run(mut self, wake: Arc<Notify>, period: Option<Duration>) {
        loop {
            self.check().await;
            match period {
                Some(period) => {
                    tokio::select! {
                        () = wake.notified() => {}
                        () = tokio::time::sleep(period) => {}
                    }
                }
                None => wake.notified().await,
            }
        }
    }
}

impl Drop for PinWatcher {
    /// Hands a stopped watcher's copies, such as those of a table a reload removed, to a
    /// task that releases each once its table no longer serves it and no scan reads it.
    fn drop(&mut self) {
        let mut copies = std::mem::take(&mut self.retired);
        copies.extend(self.serving.current().as_deref().map(RetiredCopy::of));
        if copies.is_empty() {
            return;
        }
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let loader = Arc::clone(&self.loader);
        runtime.spawn(async move {
            loop {
                copies = release_unread(loader.as_ref(), copies).await;
                if copies.is_empty() {
                    return;
                }
                tokio::time::sleep(RELEASE_CHECK_INTERVAL).await;
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::empty::EmptyTable;

    use super::*;

    /// A loader over a pin the test sets, whose loads fail for the snapshots it lists.
    #[derive(Default)]
    struct FakeLoader {
        pinned: parking_lot::Mutex<Option<u64>>,
        failing: parking_lot::Mutex<HashSet<u64>>,
        loaded: parking_lot::Mutex<Vec<u64>>,
        released: parking_lot::Mutex<Vec<u64>>,
    }

    #[async_trait]
    impl PinnedCopyLoader for FakeLoader {
        async fn pinned_snapshot(&self) -> Result<Option<u64>, BoxError> {
            Ok(*self.pinned.lock())
        }

        async fn load(&self, snapshot_id: u64) -> Result<Arc<dyn TableProvider>, NotServed> {
            self.loaded.lock().push(snapshot_id);
            if self.failing.lock().contains(&snapshot_id) {
                return Err(NotServed::LoadFailed {
                    reason: format!("snapshot {snapshot_id} is unreadable"),
                });
            }
            let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
            Ok(Arc::new(EmptyTable::new(schema)))
        }

        async fn release(&self, snapshot_id: u64) {
            self.released.lock().push(snapshot_id);
        }
    }

    fn watcher(loader: &Arc<FakeLoader>) -> (PinWatcher, Arc<PinnedServing>, Arc<AtomicUsize>) {
        let (watcher, serving, _, changes) = watcher_reporting(loader);
        (watcher, serving, changes)
    }

    fn watcher_reporting(
        loader: &Arc<FakeLoader>,
    ) -> (
        PinWatcher,
        Arc<PinnedServing>,
        Arc<PinStatus>,
        Arc<AtomicUsize>,
    ) {
        let serving = Arc::new(PinnedServing::default());
        let status = Arc::new(PinStatus::default());
        let changes = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&changes);
        let watcher = PinWatcher::new(
            "taxi_trips",
            Arc::clone(loader) as Arc<dyn PinnedCopyLoader>,
            Arc::clone(&serving),
            Arc::clone(&status),
            Box::new(move || {
                counter.fetch_add(1, Ordering::Relaxed);
                Box::pin(async {})
            }),
        );
        (watcher, serving, status, changes)
    }

    #[tokio::test]
    async fn a_pin_is_served_until_the_dataset_is_unpinned() {
        let loader = Arc::new(FakeLoader::default());
        let (mut watcher, serving, changes) = watcher(&loader);

        watcher.check().await;
        assert_eq!(serving.snapshot_id(), None, "not pinned: serve live data");

        *loader.pinned.lock() = Some(41);
        watcher.check().await;
        assert_eq!(serving.snapshot_id(), Some(41));
        assert!(serving.provider().is_some());

        watcher.check().await;
        assert_eq!(*loader.loaded.lock(), vec![41], "a served pin loads once");

        *loader.pinned.lock() = None;
        watcher.check().await;
        assert_eq!(serving.snapshot_id(), None, "unpinned: serve live data");
        assert_eq!(
            *loader.released.lock(),
            vec![41],
            "a copy no query reads is released"
        );
        assert_eq!(changes.load(Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn a_copy_a_query_still_reads_is_released_once_the_query_ends() {
        let loader = Arc::new(FakeLoader::default());
        let (mut watcher, serving, _) = watcher(&loader);
        *loader.pinned.lock() = Some(41);
        watcher.check().await;
        let (_, scan) = serving.provider().expect("served");

        *loader.pinned.lock() = None;
        watcher.check().await;
        watcher.check().await;
        assert_eq!(serving.snapshot_id(), None);
        assert!(
            loader.released.lock().is_empty(),
            "the copy is kept while a scan holds its lease"
        );

        drop(scan);
        watcher.check().await;
        assert_eq!(*loader.released.lock(), vec![41]);
    }

    #[tokio::test]
    async fn pinning_a_copy_a_query_still_reads_serves_it_again() {
        let loader = Arc::new(FakeLoader::default());
        let (mut watcher, serving, _) = watcher(&loader);
        *loader.pinned.lock() = Some(41);
        watcher.check().await;
        let (_, scan) = serving.provider().expect("served");
        *loader.pinned.lock() = None;
        watcher.check().await;

        *loader.pinned.lock() = Some(41);
        watcher.check().await;
        assert_eq!(serving.snapshot_id(), Some(41));
        assert_eq!(
            *loader.loaded.lock(),
            vec![41],
            "served again, not loaded over"
        );

        *loader.pinned.lock() = None;
        watcher.check().await;
        assert!(
            loader.released.lock().is_empty(),
            "the scan planned before it was served again still reads it"
        );
        drop(scan);
        watcher.check().await;
        assert_eq!(*loader.released.lock(), vec![41]);
    }

    #[tokio::test(start_paused = true)]
    async fn a_stopped_watcher_releases_each_copy_once_its_table_and_scans_are_done() {
        let loader = Arc::new(FakeLoader::default());
        let (mut watcher, serving, _) = watcher(&loader);
        *loader.pinned.lock() = Some(41);
        watcher.check().await;
        let (_, scan) = serving.provider().expect("served");
        *loader.pinned.lock() = Some(40);
        watcher.check().await;
        drop(watcher);

        tokio::time::sleep(RELEASE_CHECK_INTERVAL * 2).await;
        assert!(
            loader.released.lock().is_empty(),
            "a scan reads 41 and the table serves 40"
        );

        drop(scan);
        tokio::time::sleep(RELEASE_CHECK_INTERVAL * 2).await;
        assert_eq!(*loader.released.lock(), vec![41]);

        drop(serving);
        tokio::time::sleep(RELEASE_CHECK_INTERVAL * 2).await;
        assert_eq!(*loader.released.lock(), vec![41, 40]);
    }

    #[tokio::test]
    async fn a_new_pin_replaces_the_copy_served() {
        let loader = Arc::new(FakeLoader::default());
        let (mut watcher, serving, _) = watcher(&loader);

        *loader.pinned.lock() = Some(41);
        watcher.check().await;
        *loader.pinned.lock() = Some(40);
        watcher.check().await;
        assert_eq!(serving.snapshot_id(), Some(40));
        assert_eq!(*loader.released.lock(), vec![41]);
    }

    #[tokio::test]
    async fn a_pin_that_cannot_be_loaded_keeps_what_is_served_and_is_retried_at_each_check() {
        let loader = Arc::new(FakeLoader::default());
        let (mut watcher, serving, status, _) = watcher_reporting(&loader);
        *loader.pinned.lock() = Some(41);
        watcher.check().await;

        loader.failing.lock().insert(40);
        *loader.pinned.lock() = Some(40);
        watcher.check().await;
        assert_eq!(
            serving.snapshot_id(),
            Some(41),
            "the copy served is kept while the new pin cannot be loaded"
        );
        assert_eq!(
            *loader.released.lock(),
            vec![40],
            "the failed load's partial copy is released"
        );
        assert_eq!(
            status.get(),
            PinState {
                pinned: Some(40),
                serves: Some(41),
                not_served: Some(NotServed::LoadFailed {
                    reason: "snapshot 40 is unreadable".to_string()
                }),
            }
        );

        watcher.check().await;
        assert_eq!(serving.snapshot_id(), Some(41));
        assert_eq!(
            *loader.loaded.lock(),
            vec![41, 40, 40],
            "each check retries"
        );

        loader.failing.lock().clear();
        watcher.check().await;
        assert_eq!(serving.snapshot_id(), Some(40));
        assert!(status.serves_pin());
        assert_eq!(*loader.released.lock(), vec![40, 40, 41]);
    }

    #[tokio::test]
    async fn the_status_reports_the_pin_served_and_live_data_once_unpinned() {
        let loader = Arc::new(FakeLoader::default());
        let (mut watcher, _, status, _) = watcher_reporting(&loader);
        loader.failing.lock().insert(41);
        *loader.pinned.lock() = Some(41);
        watcher.check().await;
        assert_eq!(
            status.get().serves,
            None,
            "live data while the pin cannot load"
        );
        assert!(!status.serves_pin());

        loader.failing.lock().clear();
        watcher.check().await;
        assert_eq!(
            status.get(),
            PinState {
                pinned: Some(41),
                serves: Some(41),
                not_served: None,
            }
        );

        *loader.pinned.lock() = None;
        watcher.check().await;
        assert_eq!(status.get(), PinState::default());
    }

    #[test]
    fn a_reason_names_the_disk_space_needed_and_free() {
        let not_served = NotServed::NotEnoughDisk {
            snapshot_id: 41,
            needed: 2_254_857_830,
            free: 1_503_238_553,
        };
        assert_eq!(
            not_served.to_string(),
            "not enough disk for snapshot 41 (2.1 GiB needed, 1.4 GiB free)"
        );
        assert_eq!(human_bytes(512), "512 B");
        assert_eq!(human_bytes(64 * 1024 * 1024), "64.0 MiB");
    }

    #[test]
    fn a_reason_whose_details_change_is_kept_but_not_reported_as_a_change() {
        let status = PinStatus::default();
        let no_disk = |free| PinState {
            pinned: Some(41),
            serves: Some(44),
            not_served: Some(NotServed::NotEnoughDisk {
                snapshot_id: 41,
                needed: 100,
                free,
            }),
        };
        let failed = |reason: &str| PinState {
            not_served: Some(NotServed::LoadFailed {
                reason: reason.to_string(),
            }),
            ..no_disk(0)
        };

        assert!(status.set(no_disk(10)));
        assert!(!status.set(no_disk(11)), "only the free space changed");
        assert_eq!(status.get(), no_disk(11), "the latest details are kept");
        assert!(status.set(failed("a")), "another reason");
        assert!(!status.set(failed("b")), "only the message changed");
        assert_eq!(status.get(), failed("b"));
        assert!(
            status.set(PinState {
                serves: Some(41),
                not_served: None,
                ..failed("b")
            }),
            "serves the pin"
        );
    }
}
