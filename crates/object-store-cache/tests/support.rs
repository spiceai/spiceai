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

//! Test doubles and helpers the test modules share: a block store with
//! injectable faults, a volume with plenty of room, polling, and reclaiming
//! one slab at a time.

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use object_store_cache::testing::{
    BlockHeat, BlockKey, BlockStore, ObjectInfo, ObjectKey, Reclaimed, StoreCounters, StoredMeta,
};
use object_store_cache::{Error, Space, VolumeSpace};
use tokio::sync::Notify;

/// A block store over another that forwards every call, with faults a test
/// turns on:
///
/// - each `put` waits `put_delay` first, and, while `gate_puts` is set,
///   signals `put_started` and waits for `release_puts`;
/// - each `get` waits `get_delay` first while `slow_gets` is set, and fails
///   with an I/O error while `fail_gets` is set;
/// - `remove_objects` fails, removing nothing, while `fail_remove` is set.
#[derive(Debug)]
pub(crate) struct FaultStore {
    pub(crate) inner: Arc<dyn BlockStore>,
    pub(crate) put_delay: Duration,
    pub(crate) gate_puts: AtomicBool,
    pub(crate) put_started: Notify,
    pub(crate) release_puts: Notify,
    /// `put` calls made.
    pub(crate) puts_started: AtomicU64,
    /// `put` calls that waited at the gate.
    pub(crate) gated_puts: AtomicU64,
    pub(crate) get_delay: Duration,
    pub(crate) slow_gets: AtomicBool,
    pub(crate) fail_gets: AtomicBool,
    pub(crate) fail_remove: AtomicBool,
}

impl FaultStore {
    /// `inner` with every fault off.
    pub(crate) fn new(inner: Arc<dyn BlockStore>) -> Self {
        Self {
            inner,
            put_delay: Duration::ZERO,
            gate_puts: AtomicBool::new(false),
            put_started: Notify::new(),
            release_puts: Notify::new(),
            puts_started: AtomicU64::new(0),
            gated_puts: AtomicU64::new(0),
            get_delay: Duration::ZERO,
            slow_gets: AtomicBool::new(false),
            fail_gets: AtomicBool::new(false),
            fail_remove: AtomicBool::new(false),
        }
    }

    /// Opens the gate and releases the puts waiting at it.
    pub(crate) fn open_gate(&self) {
        self.gate_puts.store(false, Ordering::SeqCst);
        self.release_puts.notify_waiters();
    }
}

fn injected(action: &'static str, path: &str, what: &str) -> Error {
    Error::Io {
        action,
        path: PathBuf::from(path),
        source: std::io::Error::other(format!("injected {what} failure")),
    }
}

#[async_trait]
impl BlockStore for FaultStore {
    async fn put(
        &self,
        key: &BlockKey,
        bytes: Bytes,
        meta: &StoredMeta,
    ) -> object_store_cache::Result<()> {
        self.puts_started.fetch_add(1, Ordering::SeqCst);
        if self.gate_puts.load(Ordering::SeqCst) {
            let released = self.release_puts.notified();
            let mut released = std::pin::pin!(released);
            released.as_mut().enable();
            self.gated_puts.fetch_add(1, Ordering::SeqCst);
            self.put_started.notify_waiters();
            released.await;
        }
        if !self.put_delay.is_zero() {
            tokio::time::sleep(self.put_delay).await;
        }
        self.inner.put(key, bytes, meta).await
    }
    async fn get(&self, key: &BlockKey) -> object_store_cache::Result<Option<Bytes>> {
        if self.slow_gets.load(Ordering::SeqCst) {
            tokio::time::sleep(self.get_delay).await;
        }
        if self.fail_gets.load(Ordering::SeqCst) {
            return Err(injected("read", "slabs/slab-00000000.dat", "read"));
        }
        self.inner.get(key).await
    }
    fn contains(&self, key: &BlockKey) -> bool {
        self.inner.contains(key)
    }
    async fn remove(&self, key: &BlockKey) -> object_store_cache::Result<()> {
        self.inner.remove(key).await
    }
    async fn remove_objects(&self, objects: &[ObjectKey]) -> object_store_cache::Result<u64> {
        if self.fail_remove.load(Ordering::SeqCst) {
            return Err(injected("write", "slabs/slab-00000000.del", "tombstone"));
        }
        self.inner.remove_objects(objects).await
    }
    fn resident_blocks(&self, object: &ObjectKey) -> u64 {
        self.inner.resident_blocks(object)
    }
    fn resident_bytes(&self) -> u64 {
        self.inner.resident_bytes()
    }
    fn len(&self) -> u64 {
        self.inner.len()
    }
    fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
    fn disk_bytes(&self) -> u64 {
        self.inner.disk_bytes()
    }
    fn pending_bytes(&self) -> u64 {
        self.inner.pending_bytes()
    }
    async fn reclaim(&self, heat: BlockHeat, over: &(dyn Fn() -> bool + Send + Sync)) -> Reclaimed {
        self.inner.reclaim(heat, over).await
    }
    fn paths_under(&self, store: &str, prefix: &str) -> Vec<Arc<str>> {
        self.inner.paths_under(store, prefix)
    }
    fn object_keys(&self, store: &str, path: &str) -> Vec<ObjectKey> {
        self.inner.object_keys(store, path)
    }
    fn object_info(&self, store: &str, path: &str, block_size: u64) -> Option<ObjectInfo> {
        self.inner.object_info(store, path, block_size)
    }
    fn counters(&self) -> StoreCounters {
        self.inner.counters()
    }
    async fn close(&self) -> object_store_cache::Result<()> {
        self.inner.close().await
    }
}

/// A volume with plenty of room, so the configured budget alone applies.
#[derive(Debug)]
pub(crate) struct RoomyVolume;

impl VolumeSpace for RoomyVolume {
    fn space(&self, _dir: &std::path::Path) -> std::io::Result<Space> {
        Ok(Space {
            total: 1 << 40,
            available: 1 << 40,
        })
    }
}

/// Polls `cond` every 5 ms until it holds; fails after `timeout`.
pub(crate) async fn poll_until(timeout: Duration, what: &str, mut cond: impl FnMut() -> bool) {
    let deadline = tokio::time::Instant::now() + timeout;
    while !cond() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "{what}: not reached within {timeout:?}"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

/// Reclaims one unit of `store`, every block equally cold: the bytes freed,
/// `None` when nothing could be freed, or the error.
pub(crate) async fn reclaim_one(store: &dyn BlockStore) -> object_store_cache::Result<Option<u64>> {
    let once = AtomicBool::new(true);
    let reclaimed = store
        .reclaim(Arc::new(|_| 0), &|| once.swap(false, Ordering::SeqCst))
        .await;
    match reclaimed.error {
        Some(e) => Err(e),
        None => Ok((reclaimed.freed > 0).then_some(reclaimed.freed)),
    }
}
