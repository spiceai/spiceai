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
use tokio::sync::Notify;

pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// Held by a scan of a pinned copy until the scan's output streams are dropped.
pub type CopyLease = Arc<dyn Any + Send + Sync>;

/// Loads and releases the local copies of a dataset's pinned snapshot.
#[async_trait]
pub trait PinnedCopyLoader: Send + Sync {
    /// The snapshot the dataset is pinned to, or `None` when it is not pinned.
    async fn pinned_snapshot(&self) -> Result<Option<u64>, BoxError>;

    /// Downloads snapshot `snapshot_id` into a new local copy and opens it.
    async fn load(&self, snapshot_id: u64) -> Result<Arc<dyn TableProvider>, BoxError>;

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
        on_change: Box<dyn Fn() -> futures::future::BoxFuture<'static, ()> + Send + Sync>,
    ) -> Self {
        Self {
            dataset_name: dataset_name.into(),
            loader,
            serving,
            retired: Vec::new(),
            on_change,
        }
    }

    /// Reads the dataset's pin and serves the copy it names: loads a newly pinned
    /// snapshot, and serves the live data when the dataset is unpinned or the pinned
    /// snapshot cannot be loaded. Then releases each retired copy no scan reads.
    pub async fn check(&mut self) {
        self.follow_pin().await;
        let (unread, read) = std::mem::take(&mut self.retired)
            .into_iter()
            .partition::<Vec<_>, _>(|copy| copy.lease.strong_count() == 0);
        self.retired = read;
        for copy in unread {
            self.loader.release(copy.snapshot_id).await;
        }
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
            (None, None) => {}
            (Some(pinned), Some(current)) if current.snapshot_id == pinned => {}
            (None, Some(current)) => {
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
                    Err(err) => {
                        self.loader.release(pinned).await;
                        if let Some(current) = &current {
                            self.serving.replace(None);
                            self.retire(current);
                            (self.on_change)().await;
                        }
                        tracing::error!(
                            dataset = %self.dataset_name,
                            "Dataset '{}' is pinned to snapshot {pinned}, but the snapshot could not be loaded, so the dataset serves its live data until the next refresh tries again: {err}",
                            self.dataset_name
                        );
                    }
                }
            }
        }
    }

    fn retire(&mut self, copy: &PinnedCopy) {
        self.retired.push(RetiredCopy {
            snapshot_id: copy.snapshot_id,
            provider: Arc::clone(&copy.provider),
            lease: Arc::downgrade(&copy.lease),
        });
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

        async fn load(&self, snapshot_id: u64) -> Result<Arc<dyn TableProvider>, BoxError> {
            self.loaded.lock().push(snapshot_id);
            if self.failing.lock().contains(&snapshot_id) {
                return Err(format!("snapshot {snapshot_id} is unreadable").into());
            }
            let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
            Ok(Arc::new(EmptyTable::new(schema)))
        }

        async fn release(&self, snapshot_id: u64) {
            self.released.lock().push(snapshot_id);
        }
    }

    fn watcher(loader: &Arc<FakeLoader>) -> (PinWatcher, Arc<PinnedServing>, Arc<AtomicUsize>) {
        let serving = Arc::new(PinnedServing::default());
        let changes = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&changes);
        let watcher = PinWatcher::new(
            "taxi_trips",
            Arc::clone(loader) as Arc<dyn PinnedCopyLoader>,
            Arc::clone(&serving),
            Box::new(move || {
                counter.fetch_add(1, Ordering::Relaxed);
                Box::pin(async {})
            }),
        );
        (watcher, serving, changes)
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
    async fn a_pin_that_cannot_be_loaded_serves_live_data_and_is_retried_at_each_check() {
        let loader = Arc::new(FakeLoader::default());
        let (mut watcher, serving, _) = watcher(&loader);
        *loader.pinned.lock() = Some(41);
        watcher.check().await;

        loader.failing.lock().insert(40);
        *loader.pinned.lock() = Some(40);
        watcher.check().await;
        assert_eq!(
            serving.snapshot_id(),
            None,
            "the old pin is not served once the dataset is pinned elsewhere"
        );
        assert_eq!(
            *loader.released.lock(),
            vec![40, 41],
            "the failed load's partial copy and the replaced copy are released"
        );

        watcher.check().await;
        assert_eq!(serving.snapshot_id(), None);
        assert_eq!(
            *loader.loaded.lock(),
            vec![41, 40, 40],
            "each check retries"
        );

        loader.failing.lock().clear();
        watcher.check().await;
        assert_eq!(serving.snapshot_id(), Some(40));
    }
}
