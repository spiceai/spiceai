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

use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;

use object_store::path::Path;
use object_store::{
    Error as ObjectStoreError, GetOptions, ObjectStore, PutMode, PutOptions, UpdateVersion,
};
use parking_lot::RwLock;
use serde::Serialize;
use serde::de::DeserializeOwned;
use snafu::ResultExt;
use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;

use crate::{DeserializationSnafu, Error, ObjectStoreSnafu, Result, SerializationSnafu};

/// Result of an `insert()` operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InsertResult {
    /// Object was created.
    Ok,
    /// Object already exists.
    AlreadyExists,
}

/// Result of an `update()` operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UpdateResult<T> {
    /// Update succeeded (`ETag` matched).
    Ok,
    /// Object doesn't exist.
    NotFound,
    /// Concurrent modification - contains current value.
    Conflict { current: T },
}

/// Result of an `insert_or_update()` operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WriteResult<T> {
    /// Object was newly created.
    Inserted,
    /// Object was successfully updated (`ETag` matched).
    Updated,
    /// Concurrent modification detected - contains current remote value.
    Conflict { current: T },
}

struct CachedEntry<T> {
    value: T,
    version: UpdateVersion,
}

/// Manages typed objects in an object store with optimistic concurrency control.
///
/// `ObjectState<T>` provides a type-safe interface for storing and retrieving
/// serializable structs with automatic conflict detection via `ETag`s.
pub struct ObjectState<T> {
    store: Arc<dyn ObjectStore>,
    prefix: String,
    cache: RwLock<HashMap<String, CachedEntry<T>>>,
    _marker: PhantomData<T>,
    /// When set, successful writes auto-publish a change signal to this path.
    /// Set by `spawn_change_watcher`.
    signal_path: RwLock<Option<Path>>,
}

impl<T> std::fmt::Debug for ObjectState<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ObjectState")
            .field("prefix", &self.prefix)
            .field("store", &"Arc<dyn ObjectStore>")
            .field("cache_size", &self.cache.read().len())
            .finish_non_exhaustive()
    }
}

impl<T> ObjectState<T>
where
    T: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    /// Creates a new `ObjectState` with the given object store.
    #[must_use]
    pub fn new(store: Arc<dyn ObjectStore>) -> Self {
        Self {
            store,
            prefix: String::new(),
            cache: RwLock::new(HashMap::new()),
            signal_path: RwLock::new(None),
            _marker: PhantomData,
        }
    }

    /// Sets a prefix for all keys stored by this `ObjectState`.
    #[must_use]
    pub fn with_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.prefix = prefix.into();
        self
    }

    /// Returns the full path for a given key.
    fn path(&self, key: &str) -> Path {
        Path::from(format!("{}{key}.json", self.prefix))
    }

    /// Insert a new object. Returns `AlreadyExists` if key already exists.
    ///
    /// # Errors
    ///
    /// Returns an error if serialization fails or the object store operation fails.
    pub async fn insert(&self, key: &str, value: &T) -> Result<InsertResult> {
        let path = self.path(key);
        let payload = serde_json::to_vec(value).context(SerializationSnafu { key })?;

        match self
            .store
            .put_opts(&path, payload.into(), PutOptions::from(PutMode::Create))
            .await
        {
            Ok(result) => {
                let version = UpdateVersion::from(result);
                self.update_cache(key, value.clone(), version);
                self.notify_change().await;
                Ok(InsertResult::Ok)
            }
            Err(ObjectStoreError::AlreadyExists { .. }) => Ok(InsertResult::AlreadyExists),
            Err(source) => Err(Error::ObjectStore {
                key: key.to_string(),
                operation: "insert",
                source,
            }),
        }
    }

    /// Update an existing object with OCC. Returns `NotFound` if key doesn't exist.
    ///
    /// # Errors
    ///
    /// Returns an error if serialization fails or the object store operation fails.
    pub async fn update(&self, key: &str, value: &T) -> Result<UpdateResult<T>> {
        let path = self.path(key);

        // Get the current version from cache or fetch it
        let version = match self.get_cached_version(key) {
            Some(v) => v,
            None => {
                // Fetch current value to get ETag
                match self.get_with_version(key).await? {
                    Some((_, v)) => v,
                    None => return Ok(UpdateResult::NotFound),
                }
            }
        };

        let payload = serde_json::to_vec(value).context(SerializationSnafu { key })?;

        match self
            .store
            .put_opts(
                &path,
                payload.into(),
                PutOptions::from(PutMode::Update(version)),
            )
            .await
        {
            Ok(result) => {
                let new_version = UpdateVersion::from(result);
                self.update_cache(key, value.clone(), new_version);
                self.notify_change().await;
                Ok(UpdateResult::Ok)
            }
            Err(ObjectStoreError::Precondition { .. }) => {
                // Conflict - fetch the current value
                let current = self.get(key).await?.ok_or_else(|| Error::ObjectStore {
                    key: key.to_string(),
                    operation: "get",
                    source: ObjectStoreError::NotFound {
                        path: path.to_string(),
                        source: "Object deleted during update".into(),
                    },
                })?;
                Ok(UpdateResult::Conflict { current })
            }
            Err(source) => Err(Error::ObjectStore {
                key: key.to_string(),
                operation: "update",
                source,
            }),
        }
    }

    /// Insert a new object or update existing with OCC.
    ///
    /// If insert fails due to existing object, tries update. If update fails due to not found,
    ///
    /// # Errors
    ///
    /// Returns an error if serialization fails or the object store operation fails.
    pub async fn insert_or_update(&self, key: &str, value: &T) -> Result<WriteResult<T>> {
        // Try insert first
        match self.insert(key, value).await? {
            InsertResult::Ok => return Ok(WriteResult::Inserted),
            InsertResult::AlreadyExists => {}
        }

        // Object exists, try update
        match self.update(key, value).await? {
            UpdateResult::Ok => Ok(WriteResult::Updated),
            UpdateResult::NotFound => match self.get(key).await? {
                Some(current) => Ok(WriteResult::Conflict { current }),
                None => Err(Error::UnexpectedDeletionError {
                    key: key.to_string(),
                }),
            },
            UpdateResult::Conflict { current } => Ok(WriteResult::Conflict { current }),
        }
    }

    /// Get object directly from object store (fresh read).
    ///
    /// # Errors
    ///
    /// Returns an error if the object store operation fails or deserialization fails.
    pub async fn get(&self, key: &str) -> Result<Option<T>> {
        self.get_with_version(key)
            .await
            .map(|opt| opt.map(|(v, _)| v))
    }

    async fn get_with_version(&self, key: &str) -> Result<Option<(T, UpdateVersion)>> {
        let path = self.path(key);

        let result = match self.store.get(&path).await {
            Ok(r) => r,
            Err(ObjectStoreError::NotFound { .. }) => {
                self.remove_from_cache(key);
                return Ok(None);
            }
            Err(source) => {
                return Err(Error::ObjectStore {
                    operation: "get",
                    key: key.to_string(),
                    source,
                });
            }
        };
        let version = UpdateVersion {
            e_tag: result.meta.e_tag.clone(),
            version: result.meta.version.clone(),
        };
        let bytes = result.bytes().await.context(ObjectStoreSnafu {
            key,
            operation: "get",
        })?;

        let value: T = serde_json::from_slice(&bytes).context(DeserializationSnafu { key })?;

        self.update_cache(key, value.clone(), version.clone());
        Ok(Some((value, version)))
    }

    /// Get object from local cache (fast, may be stale).
    #[must_use]
    pub fn get_cached(&self, key: &str) -> Option<T> {
        self.cache.read().get(key).map(|entry| entry.value.clone())
    }

    /// Returns all cached key-value pairs.
    #[must_use]
    pub fn cached_entries(&self) -> HashMap<String, T> {
        self.cache
            .read()
            .iter()
            .map(|(k, v)| (k.clone(), v.value.clone()))
            .collect()
    }

    /// List all keys with the configured prefix.
    ///
    /// # Errors
    ///
    /// Returns an error if the object store list operation fails.
    pub async fn list_keys(&self) -> Result<Vec<String>> {
        use futures::StreamExt;

        let prefix = if self.prefix.is_empty() {
            None
        } else {
            Some(Path::from(self.prefix.clone()))
        };

        let mut keys = Vec::new();
        let mut stream = self.store.list(prefix.as_ref());

        while let Some(entry) = stream.next().await {
            let meta = entry.map_err(|source| Error::ObjectStore {
                key: String::new(),
                operation: "list",
                source,
            })?;

            // Extract key from path by removing prefix and .json suffix
            let path_str = meta.location.to_string();
            if let Some(key) = path_str
                .strip_prefix(&self.prefix)
                .and_then(|s| s.strip_suffix(".json"))
            {
                keys.push(key.to_string());
            }
        }

        Ok(keys)
    }

    /// Refresh local cache from object store. Retrieves all keys.
    ///
    /// # Errors
    ///
    /// Returns an error if listing keys fails.
    pub async fn refresh(&self) -> Result<()> {
        let keys = self.list_keys().await?;

        for key in keys {
            // Ignore errors during refresh - just skip failed entries
            let _ = self.get(&key).await;
        }

        Ok(())
    }

    /// Conditionally refresh the local cache using `If-None-Match`.
    ///
    /// For each key, sends a single `GET` with the cached `ETag`. The server returns
    /// `304 Not Modified` (no body) if unchanged, or `200` with the new data if
    /// changed — one round-trip per key either way.
    ///
    /// # Errors
    ///
    /// Returns an error if listing keys fails.
    pub async fn conditional_refresh(&self) -> Result<()> {
        let keys = self.list_keys().await?;

        for key in keys {
            // Ignore per-key errors — just skip failed entries
            let _ = self.get_if_changed(&key).await;
        }

        Ok(())
    }

    /// Fetch a key only if it has changed since the cached version.
    ///
    /// Uses a conditional GET with `If-None-Match`: if the remote `ETag` matches
    /// the cached one, the server returns `304 Not Modified` (no body transfer).
    /// Otherwise, the full object is returned and the cache is updated.
    ///
    /// Returns `true` if the entry was updated, `false` if unchanged.
    async fn get_if_changed(&self, key: &str) -> Result<bool> {
        let path = self.path(key);
        let cached_etag = self.get_cached_version(key).and_then(|v| v.e_tag);

        let opts = GetOptions {
            if_none_match: cached_etag,
            ..Default::default()
        };

        match self.store.get_opts(&path, opts).await {
            Ok(result) => {
                let version = UpdateVersion {
                    e_tag: result.meta.e_tag.clone(),
                    version: result.meta.version.clone(),
                };
                let bytes = result.bytes().await.context(ObjectStoreSnafu {
                    key,
                    operation: "get",
                })?;
                let value: T =
                    serde_json::from_slice(&bytes).context(DeserializationSnafu { key })?;
                self.update_cache(key, value, version);
                Ok(true)
            }
            Err(ObjectStoreError::NotModified { .. }) => Ok(false),
            Err(ObjectStoreError::NotFound { .. }) => Ok(false),
            Err(source) => Err(Error::ObjectStore {
                key: key.to_string(),
                operation: "get",
                source,
            }),
        }
    }

    fn update_cache(&self, key: &str, value: T, version: UpdateVersion) {
        self.cache
            .write()
            .insert(key.to_string(), CachedEntry { value, version });
    }

    fn remove_from_cache(&self, key: &str) {
        self.cache.write().remove(key);
    }

    fn get_cached_version(&self, key: &str) -> Option<UpdateVersion> {
        self.cache
            .read()
            .get(key)
            .map(|entry| entry.version.clone())
    }

    /// Best-effort publish of a change signal when `signal_path` is configured.
    async fn notify_change(&self) {
        let path = self.signal_path.read().clone();
        let Some(signal_path) = path else {
            return;
        };
        if let Err(e) = publish_change_signal(&*self.store, &signal_path).await {
            tracing::warn!(error = %e, "Failed to publish change signal");
        }
    }

    /// Spawns a background task that polls a signal file and conditionally refreshes
    /// the cache when the signal changes.
    ///
    /// The signal file is a lightweight change beacon: any write to it (by any
    /// scheduler) changes its `ETag`. When the watcher detects an `ETag` change via a
    /// `HEAD` request, it calls [`conditional_refresh`] to update only the entries
    /// whose remote `ETag` differs from the cached version.
    ///
    /// Returns a [`ChangeWatchHandle`] that cancels the watcher when dropped.
    ///
    /// # Arguments
    ///
    /// * `poll_interval` — How often to poll the signal file.
    ///
    /// [`conditional_refresh`]: Self::conditional_refresh
    pub fn spawn_change_watcher(self: &Arc<Self>, poll_interval: Duration) -> ChangeWatchHandle {
        let signal_path = Path::from(format!("{}__signal.json", self.prefix));
        // Enable signaling: subsequent writes will auto-publish to this path
        *self.signal_path.write() = Some(signal_path.clone());

        let cancel = CancellationToken::new();
        let state = Arc::clone(self);
        let cancel_clone = cancel.clone();
        let join_handle = tokio::spawn(async move {
            change_watch_loop(state, signal_path, poll_interval, cancel_clone).await;
        });
        ChangeWatchHandle {
            cancel,
            join_handle,
        }
    }
}

/// Handle returned by [`ObjectState::spawn_change_watcher`].
///
/// The watcher runs until the handle is dropped (which cancels the background task)
/// or explicitly cancelled via [`cancel`](Self::cancel).
pub struct ChangeWatchHandle {
    cancel: CancellationToken,
    join_handle: tokio::task::JoinHandle<()>,
}

impl ChangeWatchHandle {
    /// Cancel the watcher explicitly.
    pub fn cancel(&self) {
        self.cancel.cancel();
    }

    /// Returns a reference to the cancellation token.
    #[must_use]
    pub fn cancellation_token(&self) -> &CancellationToken {
        &self.cancel
    }
}

impl Drop for ChangeWatchHandle {
    fn drop(&mut self) {
        self.cancel.cancel();
        self.join_handle.abort();
    }
}

/// Internal watcher loop: HEAD the signal, on change call `conditional_refresh`.
async fn change_watch_loop<T>(
    state: Arc<ObjectState<T>>,
    signal_path: Path,
    poll_interval: Duration,
    cancel: CancellationToken,
) where
    T: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    let mut interval = tokio::time::interval(poll_interval);
    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut last_known_etag: Option<String> = None;

    loop {
        tokio::select! {
            () = cancel.cancelled() => {
                tracing::info!("Change watcher shutting down");
                break;
            }
            _ = interval.tick() => {
                match check_signal_and_refresh(&state, &signal_path, &mut last_known_etag).await {
                    Ok(()) => {}
                    Err(e) => {
                        tracing::debug!(error = %e, "Change watcher poll error");
                    }
                }
            }
        }
    }
}

/// Single poll cycle: conditional GET on signal file, if changed trigger `conditional_refresh`.
async fn check_signal_and_refresh<T>(
    state: &Arc<ObjectState<T>>,
    signal_path: &Path,
    last_known_etag: &mut Option<String>,
) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>>
where
    T: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    let opts = GetOptions {
        if_none_match: last_known_etag.clone(),
        ..Default::default()
    };

    match state.store.get_opts(signal_path, opts).await {
        Ok(result) => {
            // Signal changed — update ETag and conditionally refresh all entries
            *last_known_etag = result.meta.e_tag.clone();
            if let Err(e) = state.conditional_refresh().await {
                tracing::debug!(error = %e, "Conditional refresh from change signal failed");
            } else {
                tracing::debug!("Refreshed cache from change signal");
            }
        }
        Err(ObjectStoreError::NotModified { .. }) => {} // No change
        Err(ObjectStoreError::NotFound { .. }) => {}    // No signal yet
        Err(e) => return Err(e.into()),
    }

    Ok(())
}

/// Publish a change signal to the given path.
///
/// Writes a random UUID so the object's `ETag` changes on every write.
/// Uses `PutMode::Overwrite` (last-writer-wins) to avoid OCC overhead.
async fn publish_change_signal(
    store: &dyn ObjectStore,
    signal_path: &Path,
) -> std::result::Result<(), object_store::Error> {
    let payload = uuid::Uuid::new_v4().to_string();
    store
        .put_opts(
            signal_path,
            payload.into(),
            PutOptions::from(PutMode::Overwrite),
        )
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;
    use serde::Deserialize;

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
    struct TestData {
        name: String,
        value: i32,
    }

    #[tokio::test]
    async fn test_insert_new_object() {
        let store = Arc::new(InMemory::new());
        let state: ObjectState<TestData> = ObjectState::new(store).with_prefix("test/");

        let data = TestData {
            name: "test".to_string(),
            value: 42,
        };

        let result = state.insert("key1", &data).await.expect("insert failed");
        assert_eq!(result, InsertResult::Ok);

        let retrieved = state.get("key1").await.expect("get failed");
        assert_eq!(retrieved, Some(data));
    }

    #[tokio::test]
    async fn test_insert_already_exists() {
        let store = Arc::new(InMemory::new());
        let state: ObjectState<TestData> = ObjectState::new(store).with_prefix("test/");

        let data = TestData {
            name: "test".to_string(),
            value: 42,
        };

        state
            .insert("key1", &data)
            .await
            .expect("first insert failed");

        let result = state
            .insert("key1", &data)
            .await
            .expect("second insert failed");
        assert_eq!(result, InsertResult::AlreadyExists);
    }

    #[tokio::test]
    async fn test_update_existing() {
        let store = Arc::new(InMemory::new());
        let state: ObjectState<TestData> = ObjectState::new(store).with_prefix("test/");

        let data = TestData {
            name: "test".to_string(),
            value: 42,
        };

        state.insert("key1", &data).await.expect("insert failed");

        let updated = TestData {
            name: "updated".to_string(),
            value: 100,
        };

        let result = state.update("key1", &updated).await.expect("update failed");
        assert_eq!(result, UpdateResult::Ok);

        let retrieved = state.get("key1").await.expect("get failed");
        assert_eq!(retrieved, Some(updated));
    }

    #[tokio::test]
    async fn test_update_not_found() {
        let store = Arc::new(InMemory::new());
        let state: ObjectState<TestData> = ObjectState::new(store).with_prefix("test/");

        let data = TestData {
            name: "test".to_string(),
            value: 42,
        };

        let result = state
            .update("nonexistent", &data)
            .await
            .expect("update failed");
        assert_eq!(result, UpdateResult::NotFound);
    }

    #[tokio::test]
    async fn test_insert_or_update_insert() {
        let store = Arc::new(InMemory::new());
        let state: ObjectState<TestData> = ObjectState::new(store).with_prefix("test/");

        let data = TestData {
            name: "test".to_string(),
            value: 42,
        };

        let result = state
            .insert_or_update("key1", &data)
            .await
            .expect("insert_or_update failed");
        assert_eq!(result, WriteResult::Inserted);
    }

    #[tokio::test]
    async fn test_insert_or_update_update() {
        let store = Arc::new(InMemory::new());
        let state: ObjectState<TestData> = ObjectState::new(store).with_prefix("test/");

        let data = TestData {
            name: "test".to_string(),
            value: 42,
        };

        state.insert("key1", &data).await.expect("insert failed");

        let updated = TestData {
            name: "updated".to_string(),
            value: 100,
        };

        let result = state
            .insert_or_update("key1", &updated)
            .await
            .expect("insert_or_update failed");
        assert_eq!(result, WriteResult::Updated);
    }

    #[tokio::test]
    async fn test_get_cached() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let state: ObjectState<TestData> =
            ObjectState::new(Arc::clone(&store)).with_prefix("test/");

        let data = TestData {
            name: "test".to_string(),
            value: 42,
        };

        // Not in cache yet
        assert!(state.get_cached("key1").is_none());

        // Insert directly to store, bypassing cache
        let path = Path::from("test/direct.json");
        let payload = serde_json::to_vec(&data).expect("serialize failed");
        store.put(&path, payload.into()).await.expect("put failed");

        // Still not in cache (inserted directly to store)
        assert!(state.get_cached("direct").is_none());

        // Test insert updates cache
        state.insert("key1", &data).await.expect("insert failed");
        assert_eq!(state.get_cached("key1"), Some(data.clone()));

        // Test get populates cache
        let fetched = state.get("direct").await.expect("get failed");
        assert_eq!(fetched, Some(data.clone()));
        assert_eq!(state.get_cached("direct"), Some(data));
    }

    #[tokio::test]
    async fn test_list_keys() {
        let store = Arc::new(InMemory::new());
        let state: ObjectState<TestData> = ObjectState::new(store).with_prefix("test/");

        let data = TestData {
            name: "test".to_string(),
            value: 42,
        };

        state
            .insert("key1", &data)
            .await
            .expect("insert key1 failed");
        state
            .insert("key2", &data)
            .await
            .expect("insert key2 failed");
        state
            .insert("key3", &data)
            .await
            .expect("insert key3 failed");

        let mut keys = state.list_keys().await.expect("list_keys failed");
        keys.sort();

        assert_eq!(keys, vec!["key1", "key2", "key3"]);
    }

    #[tokio::test]
    async fn test_refresh() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let state: ObjectState<TestData> =
            ObjectState::new(Arc::clone(&store)).with_prefix("test/");

        let data = TestData {
            name: "test".to_string(),
            value: 42,
        };

        // Insert directly to store, bypassing cache
        let path = Path::from("test/external.json");
        let payload = serde_json::to_vec(&data).expect("serialize failed");
        store.put(&path, payload.into()).await.expect("put failed");

        // Not in cache
        assert!(state.get_cached("external").is_none());

        // Refresh populates cache
        state.refresh().await.expect("refresh failed");

        assert_eq!(state.get_cached("external"), Some(data));
    }

    #[tokio::test]
    async fn test_get_if_changed_returns_false_when_unchanged() {
        let store = Arc::new(InMemory::new());
        let state: ObjectState<TestData> = ObjectState::new(store).with_prefix("test/");

        let data = TestData {
            name: "test".to_string(),
            value: 42,
        };

        state.insert("key1", &data).await.expect("insert failed");

        // Nothing changed — should return false
        let changed = state
            .get_if_changed("key1")
            .await
            .expect("get_if_changed failed");
        assert!(!changed);
    }

    #[tokio::test]
    async fn test_get_if_changed_returns_true_when_modified() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let state: ObjectState<TestData> =
            ObjectState::new(Arc::clone(&store)).with_prefix("test/");

        let data = TestData {
            name: "test".to_string(),
            value: 42,
        };

        state.insert("key1", &data).await.expect("insert failed");

        // Modify directly in store (simulating a peer scheduler write)
        let updated = TestData {
            name: "updated".to_string(),
            value: 100,
        };
        let path = Path::from("test/key1.json");
        let payload = serde_json::to_vec(&updated).expect("serialize failed");
        store.put(&path, payload.into()).await.expect("put failed");

        // Should detect change and update cache
        let changed = state
            .get_if_changed("key1")
            .await
            .expect("get_if_changed failed");
        assert!(changed);
        assert_eq!(state.get_cached("key1"), Some(updated));
    }

    #[tokio::test]
    async fn test_conditional_refresh_only_updates_changed() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let state: ObjectState<TestData> =
            ObjectState::new(Arc::clone(&store)).with_prefix("test/");

        let data1 = TestData {
            name: "a".to_string(),
            value: 1,
        };
        let data2 = TestData {
            name: "b".to_string(),
            value: 2,
        };

        state.insert("key1", &data1).await.expect("insert failed");
        state.insert("key2", &data2).await.expect("insert failed");

        // Modify only key2 directly in store
        let updated2 = TestData {
            name: "b_updated".to_string(),
            value: 20,
        };
        let path = Path::from("test/key2.json");
        let payload = serde_json::to_vec(&updated2).expect("serialize failed");
        store.put(&path, payload.into()).await.expect("put failed");

        state
            .conditional_refresh()
            .await
            .expect("conditional_refresh failed");

        // key1 unchanged, key2 updated
        assert_eq!(state.get_cached("key1"), Some(data1));
        assert_eq!(state.get_cached("key2"), Some(updated2));
    }

    #[tokio::test]
    async fn test_write_publishes_signal_when_watcher_active() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let state: Arc<ObjectState<TestData>> =
            Arc::new(ObjectState::new(Arc::clone(&store)).with_prefix("test/"));

        // No signal file before watcher is spawned
        let signal_path = Path::from("test/__signal.json");
        assert!(store.head(&signal_path).await.is_err());

        // Spawn watcher — this enables signaling
        let handle = state.spawn_change_watcher(Duration::from_secs(60));

        // Insert should now publish a signal
        let data = TestData {
            name: "test".to_string(),
            value: 1,
        };
        state.insert("key1", &data).await.expect("insert failed");

        // Signal file should exist
        assert!(store.head(&signal_path).await.is_ok());

        handle.cancel();
    }

    #[tokio::test]
    async fn test_change_watcher_detects_cross_instance_update() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

        // Instance A — the writer
        let state_a: Arc<ObjectState<TestData>> =
            Arc::new(ObjectState::new(Arc::clone(&store)).with_prefix("test/"));
        let _handle_a = state_a.spawn_change_watcher(Duration::from_secs(60));

        // Instance B — the reader/watcher
        let state_b: Arc<ObjectState<TestData>> =
            Arc::new(ObjectState::new(Arc::clone(&store)).with_prefix("test/"));
        let _handle_b = state_b.spawn_change_watcher(Duration::from_millis(50));

        // A writes data
        let data = TestData {
            name: "from_a".to_string(),
            value: 42,
        };
        state_a
            .insert("shared_key", &data)
            .await
            .expect("insert failed");

        // B doesn't see it in cache yet
        assert!(state_b.get_cached("shared_key").is_none());

        // Wait for B's watcher to detect the signal and refresh
        tokio::time::sleep(Duration::from_millis(200)).await;

        // B should now have the data in cache
        assert_eq!(state_b.get_cached("shared_key"), Some(data));
    }
}
