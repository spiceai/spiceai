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

//! Serializes storage-generation lifetimes, including unpublished construction.
//!
//! Hold a permit from before storage mutation through catalog installation or
//! removal. A prepared generation carries that permit through initial refresh
//! and preloaded registration. Schema publication by the current owner must not
//! acquire a permit, nor may a child acquire its parent's permit during setup.
//! Drains run without catalog, schema-evolution, or cache-registry locks held.

use std::{
    collections::HashMap,
    future::Future,
    sync::{
        Arc, OnceLock,
        atomic::{AtomicBool, Ordering},
    },
};

use datafusion::{
    common::{ResolvedTableReference, TableReference},
    error::{DataFusionError, Result},
};
use runtime_acceleration::change_sink::Publication;
use tokio::{
    runtime::Handle,
    sync::{Mutex, OwnedMutexGuard, oneshot},
};

use super::resolve_table_reference;

type BeginDrain = Box<dyn FnOnce() -> Publication + Send + Sync>;

#[derive(Default)]
pub(crate) struct ChangeGenerations {
    slots: parking_lot::Mutex<HashMap<ResolvedTableReference, Arc<Mutex<State>>>>,
    closing: Arc<AtomicBool>,
    shutdown: OnceLock<Publication>,
}

#[derive(Default)]
enum State {
    #[default]
    Vacant,
    Live(BeginDrain),
    Draining(Publication),
    /// No successful cleanup proof exists. Never admit another storage owner.
    Fenced(String),
}

impl State {
    fn begin_drain(&mut self) {
        if matches!(self, Self::Live(_)) {
            // Leave a fence even if the synchronous drain hook panics.
            let old = std::mem::replace(
                self,
                Self::Fenced("generation drain did not start successfully".into()),
            );
            if let Self::Live(begin) = old {
                *self = Self::Draining(begin());
            }
        }
    }
}

/// Exclusive lifecycle access, not a write-admission or schema-callback lock.
#[must_use]
pub(crate) struct GenerationPermit {
    name: ResolvedTableReference,
    state: OwnedMutexGuard<State>,
    installed: bool,
    closing: Arc<AtomicBool>,
}

/// A constructed owner and its cancellation-independent, repeatable drain.
///
/// `begin_drain` must synchronously fence admission and return the completion of
/// *all* producer/cache work and accepted sink writes, without taking lifecycle
/// locks. Capture a strong owner reference in the hook. Do not return a timeout
/// as completion: timeouts belong to waiters, not to the owned drain.
pub(crate) struct GenerationOwner<T> {
    value: T,
    begin_drain: BeginDrain,
}

impl<T> GenerationOwner<T> {
    pub(crate) fn new(
        value: T,
        begin_drain: impl FnOnce() -> Publication + Send + Sync + 'static,
    ) -> Self {
        Self {
            value,
            begin_drain: Box::new(begin_drain),
        }
    }
}

/// Must travel with a preloaded table until that exact table is installed.
/// Dropping it fences and drains the unpublished generation.
#[must_use]
pub(crate) struct PreparedGeneration<T> {
    value: T,
    permit: GenerationPermit,
}

impl<T> PreparedGeneration<T> {
    pub(crate) fn value(&self) -> &T {
        &self.value
    }

    pub(crate) fn value_mut(&mut self) -> &mut T {
        &mut self.value
    }

    pub(crate) fn is_for(&self, name: &TableReference) -> bool {
        self.permit.name == resolve_table_reference(name.clone())
    }

    /// Call only after synchronous catalog installation and required bookkeeping
    /// have succeeded. The registry retains the owner after the permit is freed.
    #[cfg(test)]
    fn installed(self) -> T {
        self.installed_with_permit().expect("install while open").0
    }

    /// Transfer an unpublished owner's existing permit into its next lifecycle
    /// phase without releasing the dataset slot or declaring installation.
    pub(crate) fn continue_generation(self) -> Result<(T, GenerationPermit)> {
        self.permit.ensure_open()?;
        Ok((self.value, self.permit))
    }

    /// Commit to synchronous installation after asynchronous preparation.
    /// The caller must finish publication and bookkeeping without awaiting, or
    /// consume the permit with `installation_failed` if publication fails.
    pub(crate) fn installed_with_permit(mut self) -> Result<(T, GenerationPermit)> {
        self.permit.ensure_open()?;
        self.permit.installed = true;
        Ok((self.value, self.permit))
    }

    pub(crate) fn ensure_open(&self) -> Result<()> {
        self.permit.ensure_open()
    }

    /// Keep the complete owner's cleanup when construction returns a recoverable
    /// post-build error, such as a failed registration hook.
    pub(crate) fn try_map<U, E>(
        self,
        map: impl FnOnce(T) -> std::result::Result<U, E>,
    ) -> std::result::Result<PreparedGeneration<U>, E> {
        Ok(PreparedGeneration {
            value: map(self.value)?,
            permit: self.permit,
        })
    }
}

impl ChangeGenerations {
    /// Serializes lifecycle decisions without stopping the current owner.
    /// Schema-rebind callers can inspect the schema, release their schema lock,
    /// then drain under this permit only if a replacement is required.
    pub(crate) async fn lock(&self, name: &TableReference) -> Result<GenerationPermit> {
        let name = resolve_table_reference(name.clone());
        let slot = {
            let mut slots = self.slots.lock();
            if self.closing.load(Ordering::Acquire) {
                return Err(fenced(&name, "runtime is shutting down"));
            }
            // Only empty, unreferenced slots can be forgotten. In particular,
            // preserve pending/failed drains even without a catalog entry.
            if slots.len() >= 1024 {
                slots.retain(|_, slot| {
                    Arc::strong_count(slot) != 1
                        || slot
                            .try_lock()
                            .map_or(true, |state| !matches!(*state, State::Vacant))
                });
            }
            Arc::clone(slots.entry(name.clone()).or_default())
        };
        let state = slot.lock_owned().await;
        let permit = GenerationPermit {
            name,
            state,
            installed: true,
            closing: Arc::clone(&self.closing),
        };
        permit.ensure_open()?;
        Ok(permit)
    }

    /// Fence new lifecycle operations and own drains for every tracked slot,
    /// including constructors and prepared owners without a catalog entry.
    pub(crate) fn begin_shutdown(&self, runtime: &Handle) -> Publication {
        self.shutdown
            .get_or_init(|| {
                let slots = {
                    let slots = self.slots.lock();
                    self.closing.store(true, Ordering::Release);
                    slots
                        .iter()
                        .map(|(name, slot)| (name.clone(), Arc::clone(slot)))
                        .collect::<Vec<_>>()
                };
                let closing = Arc::clone(&self.closing);
                let (sender, receiver) = tokio::sync::watch::channel(None);
                runtime.spawn(async move {
                    let results =
                        futures::future::join_all(slots.into_iter().map(|(name, slot)| {
                            let closing = Arc::clone(&closing);
                            async move {
                                let mut permit = GenerationPermit {
                                    name,
                                    state: slot.lock_owned().await,
                                    installed: true,
                                    closing,
                                };
                                permit.drain_previous().await
                            }
                        }))
                        .await;
                    let result = results
                        .into_iter()
                        .find_map(Result::err)
                        .map_or(Ok(()), Err);
                    sender.send_replace(Some(result.map_err(Arc::new)));
                });
                Publication::Pending(receiver)
            })
            .clone()
    }
}

impl GenerationPermit {
    pub(crate) fn is_for(&self, name: &TableReference) -> bool {
        self.name == resolve_table_reference(name.clone())
    }

    fn ensure_open(&self) -> Result<()> {
        if self.closing.load(Ordering::Acquire) {
            return Err(fenced(&self.name, "runtime is shutting down"));
        }
        Ok(())
    }

    /// Drain a constructed owner when synchronous catalog installation fails.
    pub(crate) fn installation_failed(mut self) {
        self.installed = false;
    }

    /// Wait for completion before mutating storage. Do not hold a lock needed
    /// by the draining owner's callbacks, producer tasks, or cache fanout.
    /// Cancellation releases this waiter, not the registry's drain ownership.
    pub(crate) async fn drain_previous(&mut self) -> Result<()> {
        self.state.begin_drain();
        let result = match &*self.state {
            State::Draining(publication) => publication.wait().await,
            State::Fenced(reason) => return Err(fenced(&self.name, reason)),
            State::Vacant => Ok(()),
            State::Live(_) => unreachable!("begin_drain consumes the live owner"),
        };
        if let Err(error) = result {
            *self.state = State::Fenced(format!("generation drain failed: {error}"));
            return Err(error);
        }
        *self.state = State::Vacant;
        Ok(())
    }
    /// Owns construction independently of the caller. The future must contain
    /// every operation that can create producers or touch the new storage, and
    /// must not acquire this dataset's permit again. It must not spawn work
    /// before being polled here.
    ///
    /// Cancellation before delivery drains the result instead of installing it.
    /// A constructor that returns an error must leave nothing running that can
    /// touch the storage; its error releases the slot so a later attempt can
    /// construct again. A panicked constructor stays fenced because nothing
    /// proves its cleanup. A stuck constructor retains the permit; later
    /// lifecycle operations wait rather than replacing its storage.
    pub(crate) async fn construct<T, F>(
        mut self,
        runtime: &Handle,
        build: F,
    ) -> Result<PreparedGeneration<T>>
    where
        T: Send + 'static,
        F: Future<Output = Result<GenerationOwner<T>>> + Send + 'static,
    {
        self.ensure_open()?;
        let name = self.name.clone();
        if !matches!(*self.state, State::Vacant) {
            return Err(fenced(
                &name,
                "construction requires a successful previous drain",
            ));
        }
        self.installed = false;
        *self.state = State::Fenced("construction has no successful cleanup proof".into());
        let (sender, receiver) = oneshot::channel();
        // The task owns the permit until it transfers it into the prepared
        // result. Dropping the receiver cannot release construction early.
        runtime.spawn(async move {
            let result = match build.await {
                Ok(GenerationOwner { value, begin_drain }) => {
                    *self.state = State::Live(begin_drain);
                    Ok(PreparedGeneration {
                        value,
                        permit: self,
                    })
                }
                Err(error) => {
                    *self.state = State::Vacant;
                    Err(error)
                }
            };
            // An undelivered prepared result drops its permit and starts drain.
            let _ = sender.send(result);
        });
        receiver
            .await
            .map_err(|_| fenced(&name, "construction stopped without returning an owner"))?
    }
}

impl Drop for GenerationPermit {
    fn drop(&mut self) {
        if !self.installed {
            self.state.begin_drain();
        }
    }
}

fn fenced(name: &ResolvedTableReference, reason: &str) -> DataFusionError {
    DataFusionError::Execution(format!(
        "Change generation for dataset '{name}' remains fenced: {reason}. Restart the runtime to load the dataset again"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        sync::atomic::{AtomicBool, Ordering},
        time::Duration,
    };
    use tokio::sync::watch;

    const WAIT: Duration = Duration::from_secs(5);

    impl ChangeGenerations {
        pub(in crate::datafusion) async fn acquire(
            &self,
            name: &TableReference,
            timeout: Duration,
        ) -> Result<GenerationPermit> {
            tokio::time::timeout(timeout, async {
                let mut permit = self.lock(name).await?;
                permit.drain_previous().await?;
                Ok(permit)
            })
            .await
            .map_err(|_| {
                fenced(
                    &resolve_table_reference(name.clone()),
                    "test observer timed out",
                )
            })?
        }
    }

    fn name() -> TableReference {
        TableReference::bare("events")
    }

    async fn prepare(
        registry: &ChangeGenerations,
        stopped: Arc<AtomicBool>,
        publication: Publication,
    ) -> PreparedGeneration<()> {
        registry
            .acquire(&name(), WAIT)
            .await
            .expect("permit")
            .construct(&Handle::current(), async move {
                Ok(GenerationOwner::new((), move || {
                    stopped.store(true, Ordering::SeqCst);
                    publication
                }))
            })
            .await
            .expect("constructed owner")
    }

    #[tokio::test]
    async fn lifecycle_waits_for_completion_beyond_thirty_seconds() {
        tokio::time::timeout(Duration::from_secs(40), async {
            let constructing = ChangeGenerations::default();
            let prepared = prepare(
                &constructing,
                Arc::new(AtomicBool::new(false)),
                Publication::Ready,
            )
            .await;
            let dataset = name();
            let mut lock_waiter = Box::pin(constructing.lock(&dataset));
            assert!(futures::poll!(lock_waiter.as_mut()).is_pending());

            let draining = ChangeGenerations::default();
            let stopped = Arc::new(AtomicBool::new(false));
            let (sender, receiver) = watch::channel(None);
            prepare(
                &draining,
                Arc::clone(&stopped),
                Publication::Pending(receiver),
            )
            .await
            .installed();
            let mut drain_waiter = Box::pin(async {
                let mut permit = draining.lock(&dataset).await?;
                permit.drain_previous().await
            });
            assert!(futures::poll!(drain_waiter.as_mut()).is_pending());
            assert!(stopped.load(Ordering::SeqCst));

            // Time itself is under test: neither lifecycle wait has a deadline.
            tokio::time::sleep(Duration::from_secs(31)).await;
            assert!(futures::poll!(lock_waiter.as_mut()).is_pending());
            assert!(futures::poll!(drain_waiter.as_mut()).is_pending());
            drop(prepared);
            lock_waiter
                .await
                .expect("construction released its permit")
                .drain_previous()
                .await
                .expect("unpublished owner drained");
            sender.send_replace(Some(Ok(())));
            drain_waiter
                .await
                .expect("storage completion releases the waiter");
        })
        .await
        .expect("controlled lifecycle operations must complete");
    }

    #[tokio::test]
    async fn prepared_owner_covers_preloaded_install_interval() {
        let registry = ChangeGenerations::default();
        let stopped = Arc::new(AtomicBool::new(false));
        let prepared = prepare(&registry, Arc::clone(&stopped), Publication::Ready).await;
        assert!(prepared.is_for(&TableReference::full("spice", "public", "events")));
        assert!(!prepared.is_for(&TableReference::partial("other", "events")));
        assert!(registry.acquire(&name(), Duration::ZERO).await.is_err());
        assert!(!stopped.load(Ordering::SeqCst));
        // Another schema has an independent lifecycle even with the same name.
        drop(
            registry
                .acquire(&TableReference::partial("other", "events"), WAIT)
                .await
                .expect("different schema"),
        );
        let () = *prepared.value();
        prepared.installed();
        assert!(!stopped.load(Ordering::SeqCst));
        drop(registry.acquire(&name(), WAIT).await.expect("drained"));
        assert!(stopped.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn cancelled_preloaded_owner_retains_drain_after_timeout() {
        let registry = ChangeGenerations::default();
        let stopped = Arc::new(AtomicBool::new(false));
        let (sender, receiver) = watch::channel(None);
        let prepared = prepare(
            &registry,
            Arc::clone(&stopped),
            Publication::Pending(receiver),
        )
        .await;
        drop(prepared);
        assert!(stopped.load(Ordering::SeqCst));
        assert!(registry.acquire(&name(), Duration::ZERO).await.is_err());
        assert!(registry.acquire(&name(), Duration::ZERO).await.is_err());
        sender.send_replace(Some(Ok(())));
        drop(
            registry
                .acquire(&name(), WAIT)
                .await
                .expect("actual drain completed"),
        );
    }

    #[tokio::test]
    async fn failed_catalog_installation_retains_the_drain() {
        let registry = ChangeGenerations::default();
        let stopped = Arc::new(AtomicBool::new(false));
        let (sender, receiver) = watch::channel(None);
        let prepared = prepare(
            &registry,
            Arc::clone(&stopped),
            Publication::Pending(receiver),
        )
        .await;
        let ((), permit) = prepared
            .installed_with_permit()
            .expect("commit to installation");
        permit.installation_failed();
        assert!(stopped.load(Ordering::SeqCst));
        assert!(registry.acquire(&name(), Duration::ZERO).await.is_err());
        sender.send_replace(Some(Ok(())));
        drop(
            registry
                .acquire(&name(), WAIT)
                .await
                .expect("actual drain completed"),
        );
    }

    #[tokio::test]
    async fn failed_drain_never_releases_the_generation() {
        let registry = ChangeGenerations::default();
        let (sender, receiver) = watch::channel(None);
        prepare(
            &registry,
            Arc::new(AtomicBool::new(false)),
            Publication::Pending(receiver),
        )
        .await
        .installed();
        sender.send_replace(Some(Err(Arc::new(DataFusionError::Execution(
            "failed".into(),
        )))));
        assert!(registry.acquire(&name(), WAIT).await.is_err());
        // A failed terminal result is latched even if a faulty notifier changes.
        sender.send_replace(Some(Ok(())));
        assert!(registry.acquire(&name(), WAIT).await.is_err());
    }

    #[tokio::test]
    async fn inspection_does_not_close_a_live_owner() {
        let registry = ChangeGenerations::default();
        let stopped = Arc::new(AtomicBool::new(false));
        prepare(&registry, Arc::clone(&stopped), Publication::Ready)
            .await
            .installed();
        drop(registry.lock(&name()).await.expect("inspection"));
        assert!(!stopped.load(Ordering::SeqCst));
        let mut permit = registry.lock(&name()).await.expect("replacement");
        permit.drain_previous().await.expect("drain");
        assert!(stopped.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn construction_cannot_bypass_a_live_owner() {
        let registry = ChangeGenerations::default();
        let stopped = Arc::new(AtomicBool::new(false));
        prepare(&registry, Arc::clone(&stopped), Publication::Ready)
            .await
            .installed();
        let permit = registry.lock(&name()).await.expect("inspection");
        let result = permit
            .construct::<(), _>(&Handle::current(), async {
                panic!("the constructor must not be polled before drain")
            })
            .await;
        assert!(result.is_err());
        assert!(!stopped.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn cancelled_drain_waiter_does_not_discard_ownership() {
        let registry = Arc::new(ChangeGenerations::default());
        let stopped = Arc::new(AtomicBool::new(false));
        let (sender, receiver) = watch::channel(None);
        prepare(
            &registry,
            Arc::clone(&stopped),
            Publication::Pending(receiver),
        )
        .await
        .installed();
        let waiter = tokio::spawn({
            let registry = Arc::clone(&registry);
            async move { registry.acquire(&name(), WAIT).await }
        });
        while !stopped.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }
        waiter.abort();
        assert!(matches!(waiter.await, Err(error) if error.is_cancelled()));
        assert!(registry.acquire(&name(), Duration::ZERO).await.is_err());
        sender.send_replace(Some(Ok(())));
        drop(
            registry
                .acquire(&name(), WAIT)
                .await
                .expect("actual drain completed"),
        );
    }

    #[tokio::test]
    async fn cancelled_construction_waiter_drains_the_unpublished_result() {
        let registry = Arc::new(ChangeGenerations::default());
        let stopped = Arc::new(AtomicBool::new(false));
        let (started, start) = oneshot::channel();
        let (finish, finished) = oneshot::channel();
        let caller = tokio::spawn({
            let registry = Arc::clone(&registry);
            let stopped = Arc::clone(&stopped);
            async move {
                registry
                    .acquire(&name(), WAIT)
                    .await
                    .expect("permit")
                    .construct(&Handle::current(), async move {
                        let _ = started.send(());
                        let _ = finished.await;
                        Ok(GenerationOwner::new((), move || {
                            stopped.store(true, Ordering::SeqCst);
                            Publication::Ready
                        }))
                    })
                    .await
            }
        });
        start.await.expect("construction started");
        caller.abort();
        assert!(caller.await.is_err());
        assert!(registry.acquire(&name(), Duration::ZERO).await.is_err());
        finish.send(()).expect("constructor survived cancellation");
        drop(
            registry
                .acquire(&name(), WAIT)
                .await
                .expect("orphan drained"),
        );
        assert!(stopped.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn completed_owner_error_retains_cleanup_proof() {
        let registry = ChangeGenerations::default();
        let stopped = Arc::new(AtomicBool::new(false));
        let (sender, receiver) = watch::channel(None);
        let prepared = prepare(
            &registry,
            Arc::clone(&stopped),
            Publication::Pending(receiver),
        )
        .await;
        let result = prepared.try_map::<(), _>(|()| Err("registration hook failed"));
        assert!(result.is_err());
        assert!(stopped.load(Ordering::SeqCst));
        assert!(registry.acquire(&name(), Duration::ZERO).await.is_err());
        sender.send_replace(Some(Ok(())));
        drop(
            registry
                .acquire(&name(), WAIT)
                .await
                .expect("known owner drained"),
        );
    }

    #[tokio::test]
    async fn shutdown_owns_unpublished_construction_and_rejects_install() {
        let registry = Arc::new(ChangeGenerations::default());
        let stopped = Arc::new(AtomicBool::new(false));
        let (started, start) = oneshot::channel();
        let (finish, finished) = oneshot::channel();
        let constructor = tokio::spawn({
            let registry = Arc::clone(&registry);
            let stopped = Arc::clone(&stopped);
            async move {
                registry
                    .acquire(&name(), WAIT)
                    .await
                    .expect("permit")
                    .construct(&Handle::current(), async move {
                        let _ = started.send(());
                        let _ = finished.await;
                        Ok(GenerationOwner::new((), move || {
                            stopped.store(true, Ordering::SeqCst);
                            Publication::Ready
                        }))
                    })
                    .await
            }
        });
        start.await.expect("constructor started");
        let drain = registry.begin_shutdown(&Handle::current());
        assert!(
            registry
                .acquire(&TableReference::bare("new"), WAIT)
                .await
                .is_err()
        );
        tokio::time::timeout(Duration::ZERO, drain.wait())
            .await
            .expect_err("drain remains pending");
        finish
            .send(())
            .expect("owned constructor survives shutdown wait timeout");
        let prepared = constructor.await.expect("task").expect("complete owner");
        assert!(prepared.installed_with_permit().is_err());
        tokio::time::timeout(WAIT, drain.wait())
            .await
            .expect("shutdown completes")
            .expect("drained");
        assert!(stopped.load(Ordering::SeqCst));
        registry
            .begin_shutdown(&Handle::current())
            .wait()
            .await
            .expect("repeatable shutdown");
    }

    #[tokio::test]
    async fn constructor_failure_releases_the_generation() {
        let registry = ChangeGenerations::default();
        let result = registry
            .acquire(&name(), WAIT)
            .await
            .expect("permit")
            .construct::<(), _>(&Handle::current(), async {
                Err(DataFusionError::Execution("constructor failure".into()))
            })
            .await;
        assert!(result.is_err());
        let stopped = Arc::new(AtomicBool::new(false));
        prepare(&registry, Arc::clone(&stopped), Publication::Ready)
            .await
            .installed();
        drop(registry.acquire(&name(), WAIT).await.expect("drained"));
        assert!(stopped.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn constructor_panic_leaves_a_fence() {
        let registry = ChangeGenerations::default();
        let result = registry
            .acquire(&name(), WAIT)
            .await
            .expect("permit")
            .construct::<(), _>(&Handle::current(), async { panic!("constructor panic") })
            .await;
        assert!(result.is_err());
        for _ in 0..2 {
            assert!(registry.acquire(&name(), WAIT).await.is_err());
        }
    }
}
