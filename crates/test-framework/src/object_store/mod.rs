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

use async_trait::async_trait;
use bytes::Bytes;
use chrono::Utc;
use futures::StreamExt;
use futures::stream::BoxStream;
use object_store::{
    Attributes, GetOptions, GetResult, GetResultPayload, ListResult, ObjectMeta, ObjectStore,
    PutMode, PutMultipartOptions, PutOptions, PutPayload, PutResult, UpdateVersion, path::Path,
};
use std::collections::HashMap;
use std::fmt::Display;
use std::sync::Arc;
use tokio::sync::Mutex;

/// Stored object with metadata and version tracking for conditional operations.
#[derive(Debug, Clone)]
pub struct StoredObject {
    /// The data payload.
    data: Bytes,
    /// `ETag` for conditional operations (simple incrementing counter as string).
    e_tag: String,
    /// Last modified timestamp.
    last_modified: chrono::DateTime<Utc>,
}

/// Internal state for `MemoryObjectStore`.
#[derive(Debug)]
pub struct MemoryObjectStoreState {
    store: Mutex<HashMap<String, StoredObject>>,
    /// Counter for generating unique `ETags`.
    etag_counter: std::sync::atomic::AtomicU64,
}

/// A simple in-memory `ObjectStore` implementation for testing.
/// Stores everything in a `HashMap` and supports get/put/list with conditional operations.
#[derive(Debug, Clone)]
pub struct MemoryObjectStore {
    state: Arc<MemoryObjectStoreState>,
}

impl Display for MemoryObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "MemoryObjectStore")
    }
}

impl Default for MemoryObjectStore {
    fn default() -> Self {
        Self::new()
    }
}

impl MemoryObjectStore {
    #[must_use]
    pub fn new() -> Self {
        Self {
            state: Arc::new(MemoryObjectStoreState {
                store: Mutex::new(HashMap::new()),
                etag_counter: std::sync::atomic::AtomicU64::new(1),
            }),
        }
    }

    fn next_etag(&self) -> String {
        let counter = self
            .state
            .etag_counter
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        format!("\"{counter}\"")
    }
}

#[async_trait]
impl ObjectStore for MemoryObjectStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        let bytes: Bytes = payload.into();
        let key = location.to_string();

        let mut store = self.state.store.lock().await;

        match opts.mode {
            PutMode::Overwrite => {
                // Always overwrite
                let e_tag = self.next_etag();
                store.insert(
                    key,
                    StoredObject {
                        data: bytes,
                        e_tag: e_tag.clone(),
                        last_modified: Utc::now(),
                    },
                );
                Ok(PutResult {
                    e_tag: Some(e_tag),
                    version: None,
                })
            }
            PutMode::Create => {
                // Only create if not exists
                if store.contains_key(&key) {
                    return Err(object_store::Error::AlreadyExists {
                        path: key,
                        source: "Object already exists".into(),
                    });
                }
                let e_tag = self.next_etag();
                store.insert(
                    key,
                    StoredObject {
                        data: bytes,
                        e_tag: e_tag.clone(),
                        last_modified: Utc::now(),
                    },
                );
                Ok(PutResult {
                    e_tag: Some(e_tag),
                    version: None,
                })
            }
            PutMode::Update(UpdateVersion { e_tag, version: _ }) => {
                // Only update if ETag matches
                let Some(existing) = store.get(&key) else {
                    return Err(object_store::Error::Precondition {
                        path: key,
                        source: "Object does not exist for update".into(),
                    });
                };

                if let Some(expected_etag) = e_tag
                    && existing.e_tag != expected_etag
                {
                    return Err(object_store::Error::Precondition {
                        path: key,
                        source: format!(
                            "ETag mismatch: expected {}, found {}",
                            expected_etag, existing.e_tag
                        )
                        .into(),
                    });
                }

                let new_etag = self.next_etag();
                store.insert(
                    key,
                    StoredObject {
                        data: bytes,
                        e_tag: new_etag.clone(),
                        last_modified: Utc::now(),
                    },
                );
                Ok(PutResult {
                    e_tag: Some(new_etag),
                    version: None,
                })
            }
        }
    }

    async fn put_multipart_opts(
        &self,
        _location: &Path,
        _opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        Err(object_store::Error::NotSupported {
            source: "Multipart upload not supported in MemoryObjectStore".into(),
        })
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        let key = location.to_string();
        let store = self.state.store.lock().await;

        let Some(obj) = store.get(&key) else {
            return Err(object_store::Error::NotFound {
                path: key,
                source: "Object not found".into(),
            });
        };

        // Handle conditional get (if-match, if-none-match)
        if let Some(if_match) = &options.if_match
            && &obj.e_tag != if_match
        {
            return Err(object_store::Error::Precondition {
                path: key,
                source: format!("ETag mismatch: expected {}, found {}", if_match, obj.e_tag).into(),
            });
        }

        if let Some(if_none_match) = &options.if_none_match
            && &obj.e_tag == if_none_match
        {
            return Err(object_store::Error::NotModified {
                path: key,
                source: "Object not modified".into(),
            });
        }

        let data = obj.data.clone();
        let size = u64::try_from(data.len()).unwrap_or(0);

        // Handle range requests
        let (range_start, range_end, data_slice) =
            resolve_range(options.range.as_ref(), size, &data);

        let meta = ObjectMeta {
            location: location.clone(),
            size,
            last_modified: obj.last_modified,
            e_tag: Some(obj.e_tag.clone()),
            version: None,
        };

        let stream = futures::stream::once(async move { Ok(data_slice) });

        Ok(GetResult {
            meta,
            payload: GetResultPayload::Stream(Box::pin(stream)),
            range: range_start..range_end,
            attributes: Attributes::default(),
        })
    }

    async fn delete(&self, location: &Path) -> object_store::Result<()> {
        let key = location.to_string();
        let mut store = self.state.store.lock().await;

        if store.remove(&key).is_some() {
            Ok(())
        } else {
            Err(object_store::Error::NotFound {
                path: key,
                source: "Object not found".into(),
            })
        }
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        let prefix_str = prefix.map(std::string::ToString::to_string);
        let state = Arc::clone(&self.state);

        let fut = async move {
            let store = state.store.lock().await;
            let entries: Vec<_> = store
                .iter()
                .filter(|(key, _)| {
                    prefix_str
                        .as_ref()
                        .is_none_or(|prefix| key.starts_with(prefix))
                })
                .map(|(key, obj)| {
                    Ok(ObjectMeta {
                        location: Path::from(key.clone()),
                        size: u64::try_from(obj.data.len()).unwrap_or(0),
                        last_modified: obj.last_modified,
                        e_tag: Some(obj.e_tag.clone()),
                        version: None,
                    })
                })
                .collect();
            futures::stream::iter(entries)
        };

        futures::stream::once(fut).flatten().boxed()
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        let prefix_str = prefix.map_or(String::new(), |p| {
            let s = p.to_string();
            if s.is_empty() { s } else { format!("{s}/") }
        });

        let store = self.state.store.lock().await;

        let mut objects = Vec::new();
        let mut common_prefixes = std::collections::HashSet::new();

        for (key, obj) in store.iter() {
            if !key.starts_with(&prefix_str) && !prefix_str.is_empty() {
                continue;
            }

            let suffix = if prefix_str.is_empty() {
                key.as_str()
            } else {
                &key[prefix_str.len()..]
            };

            if let Some(slash_pos) = suffix.find('/') {
                // This is a "directory" - add to common_prefixes
                let dir_prefix = format!("{}{}", prefix_str, &suffix[..=slash_pos]);
                common_prefixes.insert(dir_prefix);
            } else {
                // This is a direct child object
                objects.push(ObjectMeta {
                    location: Path::from(key.clone()),
                    size: u64::try_from(obj.data.len()).unwrap_or(0),
                    last_modified: obj.last_modified,
                    e_tag: Some(obj.e_tag.clone()),
                    version: None,
                });
            }
        }

        Ok(ListResult {
            common_prefixes: common_prefixes
                .into_iter()
                .map(|s| Path::from(s.trim_end_matches('/')))
                .collect(),
            objects,
        })
    }

    async fn copy(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        let from_key = from.to_string();
        let to_key = to.to_string();

        let mut store = self.state.store.lock().await;

        let Some(obj) = store.get(&from_key).cloned() else {
            return Err(object_store::Error::NotFound {
                path: from_key,
                source: "Source object not found".into(),
            });
        };

        let new_obj = StoredObject {
            data: obj.data,
            e_tag: self.next_etag(),
            last_modified: Utc::now(),
        };
        store.insert(to_key, new_obj);
        Ok(())
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        let from_key = from.to_string();
        let to_key = to.to_string();

        let mut store = self.state.store.lock().await;

        if store.contains_key(&to_key) {
            return Err(object_store::Error::AlreadyExists {
                path: to_key,
                source: "Destination object already exists".into(),
            });
        }

        let Some(obj) = store.get(&from_key).cloned() else {
            return Err(object_store::Error::NotFound {
                path: from_key,
                source: "Source object not found".into(),
            });
        };

        let new_obj = StoredObject {
            data: obj.data,
            e_tag: self.next_etag(),
            last_modified: Utc::now(),
        };
        store.insert(to_key, new_obj);
        Ok(())
    }
}

/// Resolve a `GetRange` option to a concrete byte range and slice the data.
fn resolve_range(
    range: Option<&object_store::GetRange>,
    file_size: u64,
    data: &Bytes,
) -> (u64, u64, Bytes) {
    let (start, end) = match range {
        Some(object_store::GetRange::Bounded(r)) => {
            let end = r.end.min(file_size);
            let start = r.start.min(end);
            (start, end)
        }
        Some(object_store::GetRange::Offset(offset)) => {
            let start = (*offset).min(file_size);
            (start, file_size)
        }
        Some(object_store::GetRange::Suffix(n)) => {
            let start = file_size.saturating_sub(*n);
            (start, file_size)
        }
        None => (0, file_size),
    };

    let start_usize = usize::try_from(start).unwrap_or(0);
    let end_usize = usize::try_from(end).unwrap_or(data.len());
    let slice = data.slice(start_usize..end_usize);

    (start, end, slice)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use futures::TryStreamExt;
    use object_store::{GetRange, ObjectStore, PutMode, PutOptions, UpdateVersion};

    #[tokio::test]
    async fn test_put_and_get_basic() {
        let store = MemoryObjectStore::new();
        let path = Path::from("test/file.txt");
        let data = Bytes::from("hello world");

        // Put the data
        let result = store.put(&path, data.clone().into()).await;
        assert!(result.is_ok(), "put should succeed");
        let put_result = result.expect("put succeeded");
        assert!(put_result.e_tag.is_some(), "should have etag");

        // Get the data back
        let get_result = store.get(&path).await.expect("get should succeed");
        assert_eq!(get_result.meta.location, path);
        assert_eq!(get_result.meta.size, 11);

        let bytes = get_result.bytes().await.expect("should read bytes");
        assert_eq!(bytes, data);
    }

    #[tokio::test]
    async fn test_get_not_found() {
        let store = MemoryObjectStore::new();
        let path = Path::from("nonexistent/file.txt");

        let result = store.get(&path).await;
        assert!(result.is_err(), "get should fail for nonexistent file");
        assert!(
            matches!(result, Err(object_store::Error::NotFound { .. })),
            "should be NotFound error"
        );
    }

    #[tokio::test]
    async fn test_put_mode_create() {
        let store = MemoryObjectStore::new();
        let path = Path::from("test/create.txt");
        let data = Bytes::from("first");

        // First create should succeed
        let opts = PutOptions::from(PutMode::Create);
        let result = store
            .put_opts(&path, data.clone().into(), opts.clone())
            .await;
        assert!(result.is_ok(), "first create should succeed");

        // Second create should fail
        let result = store
            .put_opts(&path, Bytes::from("second").into(), opts)
            .await;
        assert!(result.is_err(), "second create should fail");
        assert!(
            matches!(result, Err(object_store::Error::AlreadyExists { .. })),
            "should be AlreadyExists error"
        );

        // Original data should be preserved
        let get_result = store.get(&path).await.expect("get should succeed");
        let bytes = get_result.bytes().await.expect("should read bytes");
        assert_eq!(bytes, data);
    }

    #[tokio::test]
    async fn test_put_mode_overwrite() {
        let store = MemoryObjectStore::new();
        let path = Path::from("test/overwrite.txt");

        // Put first version
        store
            .put(&path, Bytes::from("first").into())
            .await
            .expect("first put should succeed");

        // Overwrite with second version
        let opts = PutOptions::from(PutMode::Overwrite);
        store
            .put_opts(&path, Bytes::from("second").into(), opts)
            .await
            .expect("overwrite should succeed");

        // Should have new data
        let get_result = store.get(&path).await.expect("get should succeed");
        let bytes = get_result.bytes().await.expect("should read bytes");
        assert_eq!(bytes, Bytes::from("second"));
    }

    #[tokio::test]
    async fn test_put_mode_update_with_matching_etag() {
        let store = MemoryObjectStore::new();
        let path = Path::from("test/update.txt");

        // Put first version
        let put_result = store
            .put(&path, Bytes::from("first").into())
            .await
            .expect("first put should succeed");
        let etag = put_result.e_tag.expect("should have etag");

        // Update with matching ETag should succeed
        let opts = PutOptions::from(PutMode::Update(UpdateVersion {
            e_tag: Some(etag),
            version: None,
        }));
        let result = store
            .put_opts(&path, Bytes::from("second").into(), opts)
            .await;
        assert!(result.is_ok(), "update with matching etag should succeed");

        // Should have new data
        let get_result = store.get(&path).await.expect("get should succeed");
        let bytes = get_result.bytes().await.expect("should read bytes");
        assert_eq!(bytes, Bytes::from("second"));
    }

    #[tokio::test]
    async fn test_put_mode_update_with_wrong_etag() {
        let store = MemoryObjectStore::new();
        let path = Path::from("test/update_wrong.txt");

        // Put first version
        store
            .put(&path, Bytes::from("first").into())
            .await
            .expect("first put should succeed");

        // Update with wrong ETag should fail
        let opts = PutOptions::from(PutMode::Update(UpdateVersion {
            e_tag: Some("\"wrong\"".to_string()),
            version: None,
        }));
        let result = store
            .put_opts(&path, Bytes::from("second").into(), opts)
            .await;
        assert!(result.is_err(), "update with wrong etag should fail");
        assert!(
            matches!(result, Err(object_store::Error::Precondition { .. })),
            "should be Precondition error"
        );

        // Original data should be preserved
        let get_result = store.get(&path).await.expect("get should succeed");
        let bytes = get_result.bytes().await.expect("should read bytes");
        assert_eq!(bytes, Bytes::from("first"));
    }

    #[tokio::test]
    async fn test_put_mode_update_nonexistent() {
        let store = MemoryObjectStore::new();
        let path = Path::from("test/nonexistent.txt");

        // Update on nonexistent file should fail
        let opts = PutOptions::from(PutMode::Update(UpdateVersion {
            e_tag: Some("\"1\"".to_string()),
            version: None,
        }));
        let result = store
            .put_opts(&path, Bytes::from("data").into(), opts)
            .await;
        assert!(result.is_err(), "update on nonexistent should fail");
        assert!(
            matches!(result, Err(object_store::Error::Precondition { .. })),
            "should be Precondition error"
        );
    }

    #[tokio::test]
    async fn test_get_opts_if_match() {
        let store = MemoryObjectStore::new();
        let path = Path::from("test/if_match.txt");

        let put_result = store
            .put(&path, Bytes::from("data").into())
            .await
            .expect("put should succeed");
        let etag = put_result.e_tag.expect("should have etag");

        // Get with matching ETag should succeed
        let opts = GetOptions {
            if_match: Some(etag.clone()),
            ..Default::default()
        };
        let result = store.get_opts(&path, opts).await;
        assert!(result.is_ok(), "get with matching etag should succeed");

        // Get with wrong ETag should fail
        let opts = GetOptions {
            if_match: Some("\"wrong\"".to_string()),
            ..Default::default()
        };
        let result = store.get_opts(&path, opts).await;
        assert!(result.is_err(), "get with wrong etag should fail");
        assert!(
            matches!(result, Err(object_store::Error::Precondition { .. })),
            "should be Precondition error"
        );
    }

    #[tokio::test]
    async fn test_get_opts_if_none_match() {
        let store = MemoryObjectStore::new();
        let path = Path::from("test/if_none_match.txt");

        let put_result = store
            .put(&path, Bytes::from("data").into())
            .await
            .expect("put should succeed");
        let etag = put_result.e_tag.expect("should have etag");

        // Get with non-matching ETag should succeed
        let opts = GetOptions {
            if_none_match: Some("\"other\"".to_string()),
            ..Default::default()
        };
        let result = store.get_opts(&path, opts).await;
        assert!(result.is_ok(), "get with non-matching etag should succeed");

        // Get with matching ETag should return NotModified
        let opts = GetOptions {
            if_none_match: Some(etag),
            ..Default::default()
        };
        let result = store.get_opts(&path, opts).await;
        assert!(result.is_err(), "get with matching etag should fail");
        assert!(
            matches!(result, Err(object_store::Error::NotModified { .. })),
            "should be NotModified error"
        );
    }

    #[tokio::test]
    async fn test_get_range_bounded() {
        let store = MemoryObjectStore::new();
        let path = Path::from("test/range.txt");
        let data = Bytes::from("hello world");

        store
            .put(&path, data.into())
            .await
            .expect("put should succeed");

        // Get bytes 0-5 ("hello")
        let opts = GetOptions {
            range: Some(GetRange::Bounded(0..5)),
            ..Default::default()
        };
        let result = store
            .get_opts(&path, opts)
            .await
            .expect("get should succeed");
        let bytes = result.bytes().await.expect("should read bytes");
        assert_eq!(bytes, Bytes::from("hello"));

        // Get bytes 6-11 ("world")
        let opts = GetOptions {
            range: Some(GetRange::Bounded(6..11)),
            ..Default::default()
        };
        let result = store
            .get_opts(&path, opts)
            .await
            .expect("get should succeed");
        let bytes = result.bytes().await.expect("should read bytes");
        assert_eq!(bytes, Bytes::from("world"));
    }

    #[tokio::test]
    async fn test_get_range_offset() {
        let store = MemoryObjectStore::new();
        let path = Path::from("test/offset.txt");
        let data = Bytes::from("hello world");

        store
            .put(&path, data.into())
            .await
            .expect("put should succeed");

        // Get from offset 6 to end ("world")
        let opts = GetOptions {
            range: Some(GetRange::Offset(6)),
            ..Default::default()
        };
        let result = store
            .get_opts(&path, opts)
            .await
            .expect("get should succeed");
        let bytes = result.bytes().await.expect("should read bytes");
        assert_eq!(bytes, Bytes::from("world"));
    }

    #[tokio::test]
    async fn test_get_range_suffix() {
        let store = MemoryObjectStore::new();
        let path = Path::from("test/suffix.txt");
        let data = Bytes::from("hello world");

        store
            .put(&path, data.into())
            .await
            .expect("put should succeed");

        // Get last 5 bytes ("world")
        let opts = GetOptions {
            range: Some(GetRange::Suffix(5)),
            ..Default::default()
        };
        let result = store
            .get_opts(&path, opts)
            .await
            .expect("get should succeed");
        let bytes = result.bytes().await.expect("should read bytes");
        assert_eq!(bytes, Bytes::from("world"));
    }

    #[tokio::test]
    async fn test_delete() {
        let store = MemoryObjectStore::new();
        let path = Path::from("test/delete.txt");

        store
            .put(&path, Bytes::from("data").into())
            .await
            .expect("put should succeed");

        // Delete should succeed
        let result = store.delete(&path).await;
        assert!(result.is_ok(), "delete should succeed");

        // Get should now fail
        let result = store.get(&path).await;
        assert!(result.is_err(), "get after delete should fail");
        assert!(
            matches!(result, Err(object_store::Error::NotFound { .. })),
            "should be NotFound error"
        );
    }

    #[tokio::test]
    async fn test_delete_not_found() {
        let store = MemoryObjectStore::new();
        let path = Path::from("test/nonexistent.txt");

        let result = store.delete(&path).await;
        assert!(result.is_err(), "delete nonexistent should fail");
        assert!(
            matches!(result, Err(object_store::Error::NotFound { .. })),
            "should be NotFound error"
        );
    }

    #[tokio::test]
    async fn test_list() {
        let store = MemoryObjectStore::new();

        // Put some files
        store
            .put(&Path::from("a/file1.txt"), Bytes::from("1").into())
            .await
            .expect("put should succeed");
        store
            .put(&Path::from("a/file2.txt"), Bytes::from("2").into())
            .await
            .expect("put should succeed");
        store
            .put(&Path::from("b/file3.txt"), Bytes::from("3").into())
            .await
            .expect("put should succeed");

        // List all
        let entries: Vec<_> = store
            .list(None)
            .try_collect()
            .await
            .expect("list should succeed");
        assert_eq!(entries.len(), 3);

        // List with prefix "a/"
        let entries: Vec<_> = store
            .list(Some(&Path::from("a/")))
            .try_collect()
            .await
            .expect("list should succeed");
        assert_eq!(entries.len(), 2);
        assert!(
            entries
                .iter()
                .all(|e| e.location.as_ref().starts_with("a/"))
        );
    }

    #[tokio::test]
    async fn test_list_with_delimiter() {
        let store = MemoryObjectStore::new();

        // Put files in a hierarchy
        store
            .put(&Path::from("root.txt"), Bytes::from("root").into())
            .await
            .expect("put should succeed");
        store
            .put(&Path::from("dir1/file1.txt"), Bytes::from("1").into())
            .await
            .expect("put should succeed");
        store
            .put(
                &Path::from("dir1/subdir/file2.txt"),
                Bytes::from("2").into(),
            )
            .await
            .expect("put should succeed");
        store
            .put(&Path::from("dir2/file3.txt"), Bytes::from("3").into())
            .await
            .expect("put should succeed");

        // List root level
        let result = store
            .list_with_delimiter(None)
            .await
            .expect("list_with_delimiter should succeed");

        // Should have root.txt as object
        assert_eq!(result.objects.len(), 1);
        assert_eq!(result.objects[0].location, Path::from("root.txt"));

        // Should have dir1 and dir2 as common prefixes
        assert_eq!(result.common_prefixes.len(), 2);
        let prefixes: Vec<_> = result
            .common_prefixes
            .iter()
            .map(std::string::ToString::to_string)
            .collect();
        assert!(prefixes.contains(&"dir1".to_string()));
        assert!(prefixes.contains(&"dir2".to_string()));
    }

    #[tokio::test]
    async fn test_copy() {
        let store = MemoryObjectStore::new();
        let src = Path::from("src/file.txt");
        let dst = Path::from("dst/file.txt");
        let data = Bytes::from("copy me");

        store
            .put(&src, data.clone().into())
            .await
            .expect("put should succeed");

        // Copy should succeed
        let result = store.copy(&src, &dst).await;
        assert!(result.is_ok(), "copy should succeed");

        // Both files should exist with same content
        let src_bytes = store
            .get(&src)
            .await
            .expect("get src should succeed")
            .bytes()
            .await
            .expect("should read bytes");
        let dst_bytes = store
            .get(&dst)
            .await
            .expect("get dst should succeed")
            .bytes()
            .await
            .expect("should read bytes");

        assert_eq!(src_bytes, data);
        assert_eq!(dst_bytes, data);
    }

    #[tokio::test]
    async fn test_copy_not_found() {
        let store = MemoryObjectStore::new();
        let src = Path::from("nonexistent.txt");
        let dst = Path::from("dst.txt");

        let result = store.copy(&src, &dst).await;
        assert!(result.is_err(), "copy nonexistent should fail");
        assert!(
            matches!(result, Err(object_store::Error::NotFound { .. })),
            "should be NotFound error"
        );
    }

    #[tokio::test]
    async fn test_copy_if_not_exists() {
        let store = MemoryObjectStore::new();
        let src = Path::from("src.txt");
        let dst = Path::from("dst.txt");

        store
            .put(&src, Bytes::from("source").into())
            .await
            .expect("put should succeed");

        // First copy should succeed
        let result = store.copy_if_not_exists(&src, &dst).await;
        assert!(result.is_ok(), "first copy_if_not_exists should succeed");

        // Second copy should fail
        let result = store.copy_if_not_exists(&src, &dst).await;
        assert!(result.is_err(), "second copy_if_not_exists should fail");
        assert!(
            matches!(result, Err(object_store::Error::AlreadyExists { .. })),
            "should be AlreadyExists error"
        );
    }

    #[tokio::test]
    async fn test_etag_increments() {
        let store = MemoryObjectStore::new();

        let result1 = store
            .put(&Path::from("file1.txt"), Bytes::from("1").into())
            .await
            .expect("put should succeed");
        let result2 = store
            .put(&Path::from("file2.txt"), Bytes::from("2").into())
            .await
            .expect("put should succeed");

        let etag1 = result1.e_tag.expect("should have etag");
        let etag2 = result2.e_tag.expect("should have etag");

        // ETags should be different
        assert_ne!(etag1, etag2);

        // ETags should be incrementing
        assert_eq!(etag1, "\"1\"");
        assert_eq!(etag2, "\"2\"");
    }

    #[tokio::test]
    async fn test_multipart_not_supported() {
        let store = MemoryObjectStore::new();
        let path = Path::from("multipart.txt");

        let result = store.put_multipart(&path).await;
        assert!(result.is_err(), "multipart should not be supported");
        assert!(
            matches!(result, Err(object_store::Error::NotSupported { .. })),
            "should be NotSupported error"
        );
    }
}
