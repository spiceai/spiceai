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

use std::collections::{HashMap, HashSet};
use std::marker::PhantomData;
use std::sync::Arc;

use object_store::path::Path;
use object_store::{Error as ObjectStoreError, ObjectStore, ObjectStoreExt, UpdateVersion};
use parking_lot::{Mutex, RwLock};
use serde::Serialize;
use serde::de::DeserializeOwned;
use snafu::{OptionExt, ResultExt};

use crate::conditional::{ConditionalWriteError, Expected, conditional_put};
use crate::retry::{Attempt, ConflictRetry, RetryOnConflictError, retry_on_conflict};
use crate::{
    DeserializationSnafu, Error, ObjectStoreSnafu, Result, SerializationSnafu, UnversionedSnafu,
};

/// Result of an `insert()` operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InsertResult {
    /// Object was created.
    Ok,
    /// Object already exists.
    AlreadyExists,
}

/// Result of an `update_with_version()` operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UpdateResult<T> {
    /// Update succeeded (`ETag` matched).
    Ok,
    /// Object doesn't exist.
    NotFound,
    /// Concurrent modification - contains current value.
    Conflict { current: T },
}

/// What a [`ObjectState::modify`] change does with the object it was given.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Change<T, R> {
    /// Write the value in place of the object that was read, then return `R`.
    Write(T, R),
    /// Leave the object as it is and return `R`.
    Keep(R),
}

/// Why [`ObjectState::modify`] did not complete.
#[derive(Debug)]
pub enum ModifyError<E> {
    /// The change refused the object it was given.
    Rejected(E),
    /// Another writer changed the object before each of `attempts` attempts.
    Contention { key: String, attempts: usize },
    /// Reading or writing the object failed.
    State(Error),
}

impl<E: std::fmt::Display> std::fmt::Display for ModifyError<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Rejected(err) => write!(f, "{err}"),
            Self::Contention { key, attempts } => write!(
                f,
                "Another writer changed the object for key {key} before each of {attempts} attempts"
            ),
            Self::State(err) => write!(f, "{err}"),
        }
    }
}

impl<E: std::error::Error + 'static> std::error::Error for ModifyError<E> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Rejected(err) => Some(err),
            Self::Contention { .. } => None,
            Self::State(err) => Some(err),
        }
    }
}

/// Manages typed objects in an object store with optimistic concurrency control.
///
/// `ObjectState<T>` provides a type-safe interface for storing and retrieving
/// serializable structs. Every write is conditional: it lands only if the object
/// is still absent, or still at the version the writer read, so a writer that
/// loses a race never overwrites the winner.
pub struct ObjectState<T> {
    store: Arc<dyn ObjectStore>,
    prefix: String,
    cache: RwLock<HashMap<String, T>>,
    _marker: PhantomData<T>,
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
    ///
    /// A leading slash is removed. Object paths do not keep one, so listing
    /// must use the same string that [`Self::path`] writes. The rest of the
    /// prefix is unchanged.
    #[must_use]
    pub fn with_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.prefix = prefix.into().trim_start_matches('/').to_string();
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
        if self.write(key, value, &Expected::Absent).await? {
            Ok(InsertResult::Ok)
        } else {
            Ok(InsertResult::AlreadyExists)
        }
    }

    /// Update only if `version` still matches the object.
    ///
    /// Pass the version returned by a preceding [`Self::get_with_version`], so
    /// the write is bound to the read it was computed from.
    ///
    /// # Errors
    ///
    /// Returns an error if `version` carries neither an `ETag` nor a version,
    /// if serialization fails, or if the object store operation fails.
    pub async fn update_with_version(
        &self,
        key: &str,
        value: &T,
        version: UpdateVersion,
    ) -> Result<UpdateResult<T>> {
        let expected = Expected::at_version(version).context(UnversionedSnafu { key })?;
        if self.write(key, value, &expected).await? {
            return Ok(UpdateResult::Ok);
        }
        Ok(match self.get(key).await? {
            Some(current) => UpdateResult::Conflict { current },
            None => UpdateResult::NotFound,
        })
    }

    /// Applies `change` to the object stored under `key`, and writes what it
    /// returns only if no other writer changed the object in between.
    ///
    /// Each attempt reads the object, passes it to `change` (`None` when it does
    /// not exist), and writes the result conditionally on the version read. When
    /// another writer wins, the next attempt starts again from a fresh read, so
    /// `change` runs once per attempt and must compute its result from the value
    /// it is given. `policy` bounds the attempts and the waits between them.
    ///
    /// # Errors
    ///
    /// Returns [`ModifyError::Rejected`] with the error `change` returned,
    /// [`ModifyError::Contention`] when every attempt lost to another writer, and
    /// [`ModifyError::State`] when a read or write failed.
    pub async fn modify<R, E, F>(
        &self,
        key: &str,
        policy: &ConflictRetry,
        change: F,
    ) -> Result<R, ModifyError<E>>
    where
        F: FnMut(Option<T>) -> Result<Change<T, R>, E>,
    {
        let change = Mutex::new(change);
        retry_on_conflict(policy, || async {
            let (current, expected) = match self
                .get_with_version(key)
                .await
                .map_err(ModifyError::State)?
            {
                Some((value, version)) => (
                    Some(value),
                    Expected::at_version(version)
                        .context(UnversionedSnafu { key })
                        .map_err(ModifyError::State)?,
                ),
                None => (None, Expected::Absent),
            };
            let decision = (&mut *change.lock())(current).map_err(ModifyError::Rejected)?;
            match decision {
                Change::Keep(result) => Ok(Attempt::Done(result)),
                Change::Write(value, result) => {
                    if self
                        .write(key, &value, &expected)
                        .await
                        .map_err(ModifyError::State)?
                    {
                        Ok(Attempt::Done(result))
                    } else {
                        Ok(Attempt::Conflict)
                    }
                }
            }
        })
        .await
        .map_err(|err| match err {
            RetryOnConflictError::ConflictsExhausted { attempts } => ModifyError::Contention {
                key: key.to_string(),
                attempts,
            },
            RetryOnConflictError::Failed(err) => err,
        })
    }

    /// Writes `value` under `key` if the object is in the `expected` state.
    /// Returns `false` when another writer changed it first.
    async fn write(&self, key: &str, value: &T, expected: &Expected) -> Result<bool> {
        let payload = serde_json::to_vec(value).context(SerializationSnafu { key })?;
        match conditional_put(
            self.store.as_ref(),
            &self.path(key),
            payload.into(),
            expected,
        )
        .await
        {
            Ok(_) => {
                self.update_cache(key, value.clone());
                Ok(true)
            }
            Err(ConditionalWriteError::Conflict { .. }) => Ok(false),
            Err(source) => Err(Error::ConditionalWrite {
                key: key.to_string(),
                source,
            }),
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

    /// Get object and the store version to pass to [`Self::update_with_version`].
    ///
    /// # Errors
    ///
    /// Returns an error if the object store operation fails or deserialization fails.
    pub async fn get_with_version(&self, key: &str) -> Result<Option<(T, UpdateVersion)>> {
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

        self.update_cache(key, value.clone());
        Ok(Some((value, version)))
    }

    /// Get object from local cache (fast, may be stale).
    #[must_use]
    pub fn get_cached(&self, key: &str) -> Option<T> {
        self.cache.read().get(key).cloned()
    }

    /// Returns all cached key-value pairs.
    #[must_use]
    pub fn cached_entries(&self) -> HashMap<String, T> {
        self.cache
            .read()
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
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

    /// Delete the object for a given key. Treats a missing object as success.
    ///
    /// # Errors
    ///
    /// Returns an error if the object store delete operation fails.
    pub async fn delete(&self, key: &str) -> Result<()> {
        match self.store.delete(&self.path(key)).await {
            Ok(()) | Err(ObjectStoreError::NotFound { .. }) => {
                self.remove_from_cache(key);
                Ok(())
            }
            Err(source) => Err(Error::ObjectStore {
                key: key.to_string(),
                operation: "delete",
                source,
            }),
        }
    }

    /// Refresh local cache from object store. Retrieves all keys.
    ///
    /// # Errors
    ///
    /// Returns an error if listing keys fails.
    pub async fn refresh(&self) -> Result<()> {
        let keys = self.list_keys().await?;
        let live_keys: HashSet<String> = keys.iter().cloned().collect();

        for key in &keys {
            // Ignore errors during refresh - just skip failed entries.
            if self.get(key).await.is_err() {
                self.remove_from_cache(key);
            }
        }

        self.cache.write().retain(|key, _| live_keys.contains(key));

        Ok(())
    }

    fn update_cache(&self, key: &str, value: T) {
        self.cache.write().insert(key.to_string(), value);
    }

    fn remove_from_cache(&self, key: &str) {
        self.cache.write().remove(key);
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
    async fn test_delete() {
        let store = Arc::new(InMemory::new());
        let state: ObjectState<TestData> = ObjectState::new(store).with_prefix("test/");

        let data = TestData {
            name: "test".to_string(),
            value: 42,
        };
        state.insert("key1", &data).await.expect("insert failed");
        assert_eq!(state.get("key1").await.expect("get failed"), Some(data));

        state.delete("key1").await.expect("delete failed");
        assert_eq!(state.get("key1").await.expect("get failed"), None);
        assert!(state.list_keys().await.expect("list failed").is_empty());

        // Deleting a missing key is a no-op.
        state.delete("key1").await.expect("delete missing failed");
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
        let (_, version) = state
            .get_with_version("key1")
            .await
            .expect("get failed")
            .expect("key1 exists");

        let updated = TestData {
            name: "updated".to_string(),
            value: 100,
        };

        let result = state
            .update_with_version("key1", &updated, version)
            .await
            .expect("update failed");
        assert_eq!(result, UpdateResult::Ok);

        let retrieved = state.get("key1").await.expect("get failed");
        assert_eq!(retrieved, Some(updated));
    }

    #[tokio::test]
    async fn update_with_version_of_a_deleted_object_is_not_found() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let state: ObjectState<TestData> =
            ObjectState::new(Arc::clone(&store)).with_prefix("test/");

        let data = TestData {
            name: "test".to_string(),
            value: 42,
        };
        state.insert("key1", &data).await.expect("insert failed");
        let (_, version) = state
            .get_with_version("key1")
            .await
            .expect("get failed")
            .expect("key1 exists");
        state.delete("key1").await.expect("delete failed");

        let result = state
            .update_with_version("key1", &data, version)
            .await
            .expect("update failed");
        assert_eq!(result, UpdateResult::NotFound);
        assert_eq!(state.get("key1").await.expect("get failed"), None);
    }

    #[tokio::test]
    async fn update_with_version_conflicts_when_read_etag_is_stale() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let state: ObjectState<TestData> =
            ObjectState::new(Arc::clone(&store)).with_prefix("test/");

        let seed = TestData {
            name: "seed".to_string(),
            value: 0,
        };
        let only_a = TestData {
            name: "A".to_string(),
            value: 1,
        };
        let a_and_b = TestData {
            name: "A,B".to_string(),
            value: 2,
        };

        assert_eq!(
            state.insert("key1", &seed).await.expect("insert seed"),
            InsertResult::Ok
        );
        let (_, stale_version) = state
            .get_with_version("key1")
            .await
            .expect("stale get")
            .expect("seed exists");
        let (_, fresh_version) = state
            .get_with_version("key1")
            .await
            .expect("fresh get")
            .expect("seed exists");
        assert_eq!(
            state
                .update_with_version("key1", &a_and_b, fresh_version)
                .await
                .expect("newer writer"),
            UpdateResult::Ok
        );

        match state
            .update_with_version("key1", &only_a, stale_version)
            .await
            .expect("stale versioned update")
        {
            UpdateResult::Conflict { current } => assert_eq!(current, a_and_b),
            other => panic!("expected Conflict, got {other:?}"),
        }
        assert_eq!(
            state.get("key1").await.expect("final get"),
            Some(a_and_b.clone()),
            "stale writer must not drop the newer A,B catalog"
        );
        assert_eq!(state.get_cached("key1"), Some(a_and_b));
    }

    #[tokio::test]
    async fn update_with_version_refuses_a_version_without_an_etag() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let state: ObjectState<TestData> = ObjectState::new(store).with_prefix("test/");
        let data = TestData {
            name: "test".to_string(),
            value: 1,
        };
        state.insert("key1", &data).await.expect("insert failed");

        let err = state
            .update_with_version(
                "key1",
                &data,
                UpdateVersion {
                    e_tag: None,
                    version: None,
                },
            )
            .await
            .expect_err("an empty version cannot condition a write");
        assert!(matches!(err, Error::Unversioned { .. }), "{err:?}");
    }

    fn fast_retry() -> ConflictRetry {
        ConflictRetry {
            max_attempts: 64,
            initial_delay: std::time::Duration::from_millis(1),
            max_delay: std::time::Duration::from_millis(8),
        }
    }

    #[tokio::test]
    async fn modify_creates_an_absent_object_then_changes_it() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let state: ObjectState<TestData> = ObjectState::new(store).with_prefix("test/");

        let created = state
            .modify("key1", &fast_retry(), |current| {
                assert_eq!(current, None);
                Ok::<_, std::convert::Infallible>(Change::Write(
                    TestData {
                        name: "first".to_string(),
                        value: 1,
                    },
                    "created",
                ))
            })
            .await
            .expect("create lands");
        assert_eq!(created, "created");

        let value = state
            .modify("key1", &fast_retry(), |current| {
                let mut current = current.expect("key1 exists");
                current.value += 1;
                let value = current.value;
                Ok::<_, std::convert::Infallible>(Change::Write(current, value))
            })
            .await
            .expect("update lands");
        assert_eq!(value, 2);
        assert_eq!(
            state.get("key1").await.expect("get failed"),
            Some(TestData {
                name: "first".to_string(),
                value: 2,
            })
        );
    }

    #[tokio::test]
    async fn modify_keep_and_rejected_do_not_write() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let state: ObjectState<TestData> = ObjectState::new(store).with_prefix("test/");

        let kept = state
            .modify("key1", &fast_retry(), |_| {
                Ok::<_, std::convert::Infallible>(Change::Keep(7))
            })
            .await
            .expect("keep succeeds");
        assert_eq!(kept, 7);
        assert_eq!(state.get("key1").await.expect("get failed"), None);

        let err = state
            .modify("key1", &fast_retry(), |_| {
                Err::<Change<TestData, ()>, _>("refused")
            })
            .await
            .expect_err("the change refused");
        assert!(matches!(err, ModifyError::Rejected("refused")), "{err:?}");
        assert_eq!(state.get("key1").await.expect("get failed"), None);
    }

    /// A writer that loses every race gives up after the policy's attempts and
    /// leaves the winners' value in place.
    #[tokio::test]
    async fn modify_gives_up_after_the_policy_and_keeps_the_winners_value() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let state: ObjectState<TestData> =
            ObjectState::new(Arc::clone(&store)).with_prefix("test/");
        let rival: ObjectState<TestData> =
            ObjectState::new(Arc::clone(&store)).with_prefix("test/");
        state
            .insert(
                "key1",
                &TestData {
                    name: "seed".to_string(),
                    value: 0,
                },
            )
            .await
            .expect("insert failed");

        let policy = ConflictRetry {
            max_attempts: 3,
            ..fast_retry()
        };
        let calls = std::sync::atomic::AtomicUsize::new(0);
        let rival_writes = parking_lot::Mutex::new(Vec::new());
        let err = state
            .modify("key1", &policy, |current| {
                calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                let current = current.expect("key1 exists");
                // Another writer changes the object between this read and write.
                let winner = TestData {
                    name: "rival".to_string(),
                    value: current.value + 1,
                };
                rival_writes.lock().push(winner.clone());
                let path = Path::from("test/key1.json");
                let payload = serde_json::to_vec(&winner).expect("serialize");
                futures::executor::block_on(rival.store.put(&path, payload.into()))
                    .expect("rival write");
                Ok::<_, std::convert::Infallible>(Change::Write(
                    TestData {
                        name: "loser".to_string(),
                        value: -1,
                    },
                    (),
                ))
            })
            .await
            .expect_err("every attempt loses");
        assert!(
            matches!(err, ModifyError::Contention { attempts: 3, .. }),
            "{err:?}"
        );
        assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 3);
        assert_eq!(
            state.get("key1").await.expect("get failed").as_ref(),
            rival_writes.lock().last(),
            "the losing write must not land"
        );
    }

    /// Concurrent writers through separate `ObjectState`s on one local directory
    /// lose no update.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_modify_on_a_local_directory_loses_no_update() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store: Arc<dyn ObjectStore> =
            Arc::new(crate::LocalConditionalPut::new(dir.path()).expect("local conditional store"));
        let writers = 6;
        let per_writer = 10;
        let mut tasks = Vec::new();
        for _ in 0..writers {
            let state: ObjectState<TestData> =
                ObjectState::new(Arc::clone(&store)).with_prefix("test/");
            tasks.push(tokio::spawn(async move {
                for _ in 0..per_writer {
                    state
                        .modify("counter", &fast_retry(), |current| {
                            let mut next = current.unwrap_or(TestData {
                                name: "counter".to_string(),
                                value: 0,
                            });
                            next.value += 1;
                            Ok::<_, std::convert::Infallible>(Change::Write(next, ()))
                        })
                        .await
                        .expect("increment lands within the retry budget");
                }
            }));
        }
        for task in tasks {
            task.await.expect("writer task");
        }
        let state: ObjectState<TestData> = ObjectState::new(store).with_prefix("test/");
        assert_eq!(
            state
                .get("counter")
                .await
                .expect("get failed")
                .map(|d| d.value),
            Some(writers * per_writer)
        );
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
    async fn test_list_keys_scopes_prefix_and_json_suffix() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let state: ObjectState<TestData> =
            ObjectState::new(Arc::clone(&store)).with_prefix("test/");

        let data = TestData {
            name: "test".to_string(),
            value: 42,
        };
        let payload = serde_json::to_vec(&data).expect("serialize failed");

        store
            .put(&Path::from("test/key1.json"), payload.clone().into())
            .await
            .expect("put key1 failed");
        store
            .put(&Path::from("test/nested/key2.json"), payload.clone().into())
            .await
            .expect("put nested key2 failed");
        store
            .put(&Path::from("test/raw.txt"), payload.clone().into())
            .await
            .expect("put raw file failed");
        store
            .put(&Path::from("other/key3.json"), payload.into())
            .await
            .expect("put other prefix failed");

        let mut keys = state.list_keys().await.expect("list_keys failed");
        keys.sort();

        assert_eq!(keys, vec!["key1", "nested/key2"]);
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
    async fn test_refresh_removes_deleted_cached_entries() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let state: ObjectState<TestData> =
            ObjectState::new(Arc::clone(&store)).with_prefix("test/");

        let data = TestData {
            name: "test".to_string(),
            value: 42,
        };

        state.insert("key1", &data).await.expect("insert failed");
        assert_eq!(state.get_cached("key1"), Some(data));

        store
            .delete(&Path::from("test/key1.json"))
            .await
            .expect("delete failed");

        state.refresh().await.expect("refresh failed");

        assert!(state.get_cached("key1").is_none());
        assert!(
            state
                .list_keys()
                .await
                .expect("list_keys failed")
                .is_empty()
        );
    }

    #[tokio::test]
    async fn test_get_missing_removes_cached_entry() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let state: ObjectState<TestData> =
            ObjectState::new(Arc::clone(&store)).with_prefix("test/");

        let data = TestData {
            name: "test".to_string(),
            value: 42,
        };

        state.insert("key1", &data).await.expect("insert failed");
        assert!(state.get_cached("key1").is_some());

        store
            .delete(&Path::from("test/key1.json"))
            .await
            .expect("delete failed");

        assert_eq!(state.get("key1").await.expect("get failed"), None);
        assert!(state.get_cached("key1").is_none());
    }

    #[tokio::test]
    async fn test_refresh_populates_valid_entries_and_skips_invalid_json() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let state: ObjectState<TestData> =
            ObjectState::new(Arc::clone(&store)).with_prefix("test/");

        let valid = TestData {
            name: "valid".to_string(),
            value: 7,
        };
        let payload = serde_json::to_vec(&valid).expect("serialize failed");

        store
            .put(&Path::from("test/valid.json"), payload.into())
            .await
            .expect("put valid failed");
        store
            .put(
                &Path::from("test/invalid.json"),
                b"not valid json".as_slice().into(),
            )
            .await
            .expect("put invalid failed");

        state.refresh().await.expect("refresh failed");

        assert_eq!(state.get_cached("valid"), Some(valid));
        assert!(state.get_cached("invalid").is_none());
    }
}
