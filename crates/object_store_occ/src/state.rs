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
use std::time::Instant;

use object_store::path::Path;
use object_store::{Error as ObjectStoreError, ObjectStore, PutMode, PutOptions, UpdateVersion};
use parking_lot::RwLock;
use serde::Serialize;
use serde::de::DeserializeOwned;
use snafu::ResultExt;

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
    #[expect(dead_code)]
    cached_at: Instant,
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
}

impl<T> ObjectState<T>
where
    T: Serialize + DeserializeOwned + Clone + Send + Sync,
{
    /// Creates a new `ObjectState` with the given object store.
    #[must_use]
    pub fn new(store: Arc<dyn ObjectStore>) -> Self {
        Self {
            store,
            prefix: String::new(),
            cache: RwLock::new(HashMap::new()),
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
                Ok(InsertResult::Ok)
            }
            Err(ObjectStoreError::AlreadyExists { .. }) => Ok(InsertResult::AlreadyExists),
            Err(source) => Err(Error::ObjectStore {
                key: key.to_string(),
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
                Ok(UpdateResult::Ok)
            }
            Err(ObjectStoreError::Precondition { .. }) => {
                // Conflict - fetch the current value
                let current = self.get(key).await?.ok_or_else(|| Error::ObjectStore {
                    key: key.to_string(),
                    source: ObjectStoreError::NotFound {
                        path: path.to_string(),
                        source: "Object deleted during update".into(),
                    },
                })?;
                Ok(UpdateResult::Conflict { current })
            }
            Err(source) => Err(Error::ObjectStore {
                key: key.to_string(),
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
                    key: key.to_string(),
                    source,
                });
            }
        };

        let version = UpdateVersion {
            e_tag: result.meta.e_tag.clone(),
            version: result.meta.version.clone(),
        };

        let bytes = result.bytes().await.context(ObjectStoreSnafu { key })?;
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

    fn update_cache(&self, key: &str, value: T, version: UpdateVersion) {
        self.cache.write().insert(
            key.to_string(),
            CachedEntry {
                value,
                version,
                cached_at: Instant::now(),
            },
        );
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
}
