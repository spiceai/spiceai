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

//! Tracks the dataset loads that are still retrying, so a Spicepod change can
//! stop the load of a configuration it replaces or removes.
//!
//! `Runtime::load_dataset` retries a failed load for the life of the process.
//! Without a way to stop it, a load of a configuration that was later corrected
//! or removed keeps retrying, and the first attempt that succeeds registers the
//! old configuration over the one the Spicepod now declares (#1458).
//!
//! [`DatasetLoads::supersede`] cancels the loads of a dataset and then waits for
//! the attempt lock. `Runtime::load_dataset` drops its retry loop when its load
//! is cancelled, which drops any attempt in progress and releases the lock, so
//! once `supersede` returns no attempt of the replaced configuration is running
//! and none will start. It cannot register afterwards, even from a poll that was
//! already under way when the cancellation landed. The wait is bounded by how
//! long the superseded task takes to be polled once, not by the source: an
//! attempt stuck on a source that never answers is dropped, not waited for.
//!
//! Dropping an attempt part-way is what shutdown already does. An
//! `AcceleratedTable` the attempt built but had not registered aborts its
//! refresh tasks when dropped; one it had registered is the registration the
//! Spicepod change goes on to replace or remove, like any loaded dataset's.

use std::{collections::HashMap, sync::Arc};

use datafusion::sql::{ResolvedTableReference, TableReference};
use tokio::sync::{Mutex, OwnedMutexGuard};
use tokio_util::sync::CancellationToken;

use crate::datafusion::resolve_table_reference;

#[derive(Default)]
pub(crate) struct DatasetLoads {
    loads: parking_lot::Mutex<HashMap<ResolvedTableReference, Entry>>,
}

struct Entry {
    /// Held for the whole of each load attempt. Shared by every load of the
    /// dataset, including one that begins after an earlier load was superseded,
    /// so a new load's first attempt still waits for an old attempt to finish.
    attempt_lock: Arc<Mutex<()>>,
    /// Cancelled when the loads sharing it are superseded.
    token: CancellationToken,
    /// Loads holding a [`DatasetLoad`] for this dataset; the entry is removed
    /// when the last one ends.
    active: usize,
}

/// One running load of a dataset, from [`DatasetLoads::begin`] until it is
/// dropped.
pub(crate) struct DatasetLoad {
    loads: Arc<DatasetLoads>,
    name: ResolvedTableReference,
    attempt_lock: Arc<Mutex<()>>,
    token: CancellationToken,
}

impl DatasetLoads {
    /// Registers a load of `name`. A load that begins after an earlier one was
    /// superseded is not itself superseded.
    pub(crate) fn begin(self: &Arc<Self>, name: &TableReference) -> DatasetLoad {
        let name = resolve_table_reference(name.clone());
        let mut loads = self.loads.lock();
        let entry = loads.entry(name.clone()).or_insert_with(|| Entry {
            attempt_lock: Arc::new(Mutex::new(())),
            token: CancellationToken::new(),
            active: 0,
        });
        if entry.token.is_cancelled() {
            entry.token = CancellationToken::new();
        }
        entry.active += 1;
        DatasetLoad {
            loads: Arc::clone(self),
            name,
            attempt_lock: Arc::clone(&entry.attempt_lock),
            token: entry.token.clone(),
        }
    }

    /// Stops every running load of `name`, and returns once none of them is
    /// attempting or will attempt again. A no-op when `name` has no load
    /// running.
    pub(crate) async fn supersede(&self, name: &TableReference) {
        let name = resolve_table_reference(name.clone());
        let Some((attempt_lock, token)) = self
            .loads
            .lock()
            .get(&name)
            .map(|entry| (Arc::clone(&entry.attempt_lock), entry.token.clone()))
        else {
            return;
        };
        token.cancel();
        // Released when the superseded load drops the attempt it was running.
        let _no_attempt_running = attempt_lock.lock().await;
    }
}

impl DatasetLoad {
    /// Waits for the dataset's attempt lock and returns it, or `None` if this
    /// load was superseded. The attempt must hold the guard until it ends.
    pub(crate) async fn start_attempt(&self) -> Option<OwnedMutexGuard<()>> {
        let guard = Arc::clone(&self.attempt_lock).lock_owned().await;
        (!self.token.is_cancelled()).then_some(guard)
    }

    /// Resolves when this load is superseded. The load must then be dropped,
    /// together with any attempt it is running: [`DatasetLoads::supersede`]
    /// waits for that attempt's guard.
    pub(crate) async fn superseded(&self) {
        self.token.cancelled().await;
    }
}

impl Drop for DatasetLoad {
    fn drop(&mut self) {
        let mut loads = self.loads.loads.lock();
        if let Some(entry) = loads.get_mut(&self.name) {
            entry.active = entry.active.saturating_sub(1);
            if entry.active == 0 {
                loads.remove(&self.name);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{future::Future, time::Duration};

    fn name(table: &str) -> TableReference {
        TableReference::bare(table)
    }

    #[tokio::test]
    async fn a_superseded_load_attempts_no_more() {
        let loads = Arc::new(DatasetLoads::default());
        let load = loads.begin(&name("t"));
        drop(
            load.start_attempt()
                .await
                .expect("the first attempt starts"),
        );

        loads.supersede(&name("t")).await;

        assert!(
            load.start_attempt().await.is_none(),
            "a superseded load must not start another attempt"
        );
        tokio::time::timeout(Duration::from_secs(1), load.superseded())
            .await
            .expect("a superseded load must be told so");
    }

    /// What `Runtime::load_dataset` does with a load: attempts run under the
    /// guard, and the whole retry loop is dropped once the load is superseded.
    fn spawn_load(
        load: DatasetLoad,
        attempt: impl Future<Output = ()> + Send + 'static,
    ) -> tokio::task::JoinHandle<bool> {
        tokio::spawn(async move {
            let attempt_loop = async {
                let Some(_attempt) = load.start_attempt().await else {
                    return false;
                };
                attempt.await;
                true
            };
            tokio::select! {
                completed = attempt_loop => completed,
                () = load.superseded() => false,
            }
        })
    }

    /// The source that accepts a connection and never answers: superseding its
    /// load must not wait for it, or the apply that supersedes it never returns.
    #[tokio::test]
    async fn supersede_drops_an_attempt_that_never_returns() {
        let loads = Arc::new(DatasetLoads::default());
        let load = spawn_load(loads.begin(&name("t")), std::future::pending());
        tokio::task::yield_now().await;

        tokio::time::timeout(Duration::from_secs(5), loads.supersede(&name("t")))
            .await
            .expect("superseding a stuck attempt must not wait for its source");
        assert!(
            !load.await.expect("the load does not panic"),
            "the stuck attempt must not complete"
        );
    }

    /// An attempt that has not yet reached the point it registers at is dropped,
    /// so its registration never happens.
    #[tokio::test]
    async fn a_superseded_attempt_never_reaches_its_registration() {
        let loads = Arc::new(DatasetLoads::default());
        let registered = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let (release, released) = tokio::sync::oneshot::channel::<()>();
        let load = spawn_load(loads.begin(&name("t")), {
            let registered = Arc::clone(&registered);
            async move {
                let _ = released.await;
                registered.store(true, std::sync::atomic::Ordering::SeqCst);
            }
        });
        tokio::task::yield_now().await;

        loads.supersede(&name("t")).await;
        let _ = release.send(());

        assert!(!load.await.expect("the load does not panic"));
        assert!(
            !registered.load(std::sync::atomic::Ordering::SeqCst),
            "a superseded attempt must not register its configuration"
        );
    }

    #[tokio::test]
    async fn a_load_begun_after_supersede_runs() {
        let loads = Arc::new(DatasetLoads::default());
        let old = loads.begin(&name("t"));
        loads.supersede(&name("t")).await;

        let new = loads.begin(&name("t"));
        assert!(
            new.start_attempt().await.is_some(),
            "the load of the new configuration must not inherit the old one's cancellation"
        );
        assert!(old.start_attempt().await.is_none());
    }

    #[tokio::test]
    async fn supersede_reaches_every_running_load_and_only_that_dataset() {
        let loads = Arc::new(DatasetLoads::default());
        let first = loads.begin(&name("t"));
        let second = loads.begin(&name("t"));
        let other = loads.begin(&name("u"));

        loads.supersede(&name("t")).await;

        assert!(first.start_attempt().await.is_none());
        assert!(second.start_attempt().await.is_none());
        assert!(
            other.start_attempt().await.is_some(),
            "superseding one dataset must not stop another's load"
        );
    }

    #[tokio::test]
    async fn names_are_resolved_before_they_are_compared() {
        let loads = Arc::new(DatasetLoads::default());
        let load = loads.begin(&name("t"));

        loads
            .supersede(&TableReference::full("spice", "public", "t"))
            .await;

        assert!(load.start_attempt().await.is_none());
    }

    #[tokio::test]
    async fn the_entry_is_removed_when_the_last_load_ends() {
        let loads = Arc::new(DatasetLoads::default());
        let first = loads.begin(&name("t"));
        let second = loads.begin(&name("t"));
        drop(first);
        assert_eq!(loads.loads.lock().len(), 1);
        drop(second);
        assert!(loads.loads.lock().is_empty());

        // With nothing running, superseding is a no-op and a later load is unaffected.
        loads.supersede(&name("t")).await;
        let later = loads.begin(&name("t"));
        assert!(later.start_attempt().await.is_some());
    }
}
