/*
Copyright 2024-2025 The Spice.ai OSS Authors

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

use std::ops::Range;
use std::path::Path as StdPath;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::StreamExt;
use futures::stream::BoxStream;
use libnfs::{EntryType, Nfs};
use nix::fcntl::OFlag;
use object_store::{
    Attributes, GetOptions, GetRange, GetResult, GetResultPayload, ListResult, MultipartUpload,
    ObjectMeta, ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult, path::Path,
};

use super::common::{DirEntry, generic_error, process_directory_entries};

const STORE_NAME: &str = "NFS";

fn handle_error<T: Into<Box<dyn std::error::Error + Sync + Send>>>(
    error: T,
) -> object_store::Error {
    generic_error(STORE_NAME, error)
}

#[derive(Debug, Clone)]
struct NFSClientConfig {
    server: String,
    export_path: String,
    // Note: The libnfs Rust bindings (v0.1.1) do not expose timeout configuration.
    // The underlying C library (libnfs) supports nfs_set_timeout(), but this is not
    // wrapped in the Rust bindings. This field is kept for API consistency and future use.
    #[expect(dead_code)]
    timeout: Option<Duration>,
}

impl NFSClientConfig {
    fn new(server: String, export_path: String, timeout: Option<Duration>) -> Self {
        Self {
            server,
            export_path,
            timeout,
        }
    }

    fn connect(&self) -> object_store::Result<Nfs> {
        let mut nfs = Nfs::new().map_err(handle_error)?;
        nfs.mount(&self.server, &self.export_path)
            .map_err(handle_error)?;
        Ok(nfs)
    }
}

#[derive(Debug)]
pub struct NFSObjectStore {
    config: Arc<NFSClientConfig>,
}

impl std::fmt::Display for NFSObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "NFS")
    }
}

impl NFSObjectStore {
    #[must_use]
    pub fn new(server: String, export_path: String, timeout: Option<Duration>) -> Self {
        Self {
            config: Arc::new(NFSClientConfig::new(server, export_path, timeout)),
        }
    }

    /// List all files recursively starting from a given path
    async fn list_all_files(
        &self,
        prefix: Option<String>,
    ) -> object_store::Result<Vec<ObjectMeta>> {
        let config = Arc::clone(&self.config);
        let prefix = prefix.unwrap_or_default();

        tokio::task::spawn_blocking(move || {
            let mut nfs = config.connect()?;
            let mut results = Vec::new();
            let mut queue = vec![prefix];

            while let Some(current_path) = queue.pop() {
                let dir_path = if current_path.is_empty() {
                    "/".to_string()
                } else {
                    format!("/{current_path}")
                };

                let dir = match nfs.opendir(StdPath::new(&dir_path)) {
                    Ok(d) => d,
                    Err(e) => {
                        tracing::warn!("Failed to open NFS directory {dir_path}: {e}");
                        continue;
                    }
                };

                // Convert NFS entries to common DirEntry format
                let entries = dir.filter_map(|e| e.ok()).filter_map(|entry| {
                    let name = entry.path.to_string_lossy().to_string();
                    match entry.d_type {
                        EntryType::Directory => Some(DirEntry::directory(name)),
                        EntryType::File => {
                            let last_modified = DateTime::<Utc>::from_timestamp(
                                entry.mtime.tv_sec,
                                u32::try_from(entry.mtime.tv_usec).unwrap_or(0) * 1000,
                            )
                            .unwrap_or_else(Utc::now);
                            Some(DirEntry::file(name, entry.size, last_modified))
                        }
                        _ => None, // Skip symlinks, sockets, etc.
                    }
                });

                // Use common utility to process entries
                let (files, dirs) = process_directory_entries(&current_path, entries);
                results.extend(files);
                queue.extend(dirs);
            }

            Ok(results)
        })
        .await
        .map_err(|e| generic_error(STORE_NAME, e))?
    }
}

#[async_trait]
impl ObjectStore for NFSObjectStore {
    async fn put_opts(
        &self,
        _location: &Path,
        _payload: PutPayload,
        _opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        Err(object_store::Error::NotSupported {
            source: "NFS put_opts not implemented".into(),
        })
    }

    async fn put_multipart_opts(
        &self,
        _location: &Path,
        _opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        Err(object_store::Error::NotSupported {
            source: "NFS put_multipart_opts not implemented".into(),
        })
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        let config = Arc::clone(&self.config);
        let location = location.clone();

        let (object_meta, start, end, data) = tokio::task::spawn_blocking({
            let location = location.clone();
            let config = config.clone();
            move || -> object_store::Result<(ObjectMeta, u64, u64, Vec<u8>)> {
                let mut nfs = config.connect()?;
                let location_string = format!("/{location}");

                let stat = nfs.stat64(StdPath::new(&location_string)).map_err(|e| {
                    object_store::Error::NotFound {
                        path: location_string.clone(),
                        source: e.into(),
                    }
                })?;

                let size = stat.nfs_size;
                let object_meta = ObjectMeta {
                    location: location.clone(),
                    size,
                    last_modified: DateTime::<Utc>::from_timestamp(
                        stat.nfs_mtime,
                        stat.nfs_mtime_nsec,
                    )
                    .unwrap_or_else(Utc::now),
                    e_tag: None,
                    version: None,
                };

                let mut start = 0u64;
                let mut end = size;
                let mut data_to_read = size;

                if let Some(GetRange::Bounded(range)) = options.range {
                    data_to_read = range.end - range.start;
                    start = range.start;
                    end = range.end;
                }

                let file = nfs
                    .open(StdPath::new(&location_string), OFlag::O_RDONLY)
                    .map_err(handle_error)?;
                let data = file.pread(data_to_read, start).map_err(handle_error)?;

                Ok((object_meta, start, end, data))
            }
        })
        .await
        .map_err(|e| generic_error(STORE_NAME, e))??;

        let stream = futures::stream::once(async move { Ok(Bytes::from(data)) });

        Ok(GetResult {
            meta: object_meta,
            payload: GetResultPayload::Stream(Box::pin(stream)),
            range: Range { start, end },
            attributes: Attributes::default(),
        })
    }

    async fn delete(&self, _location: &Path) -> object_store::Result<()> {
        Err(object_store::Error::NotSupported {
            source: "NFS delete not implemented".into(),
        })
    }

    fn delete_stream<'a>(
        &'a self,
        _locations: BoxStream<'a, object_store::Result<Path>>,
    ) -> BoxStream<'a, object_store::Result<Path>> {
        futures::stream::once(async {
            Err(object_store::Error::NotSupported {
                source: "NFS delete_stream not implemented".into(),
            })
        })
        .boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        let config = Arc::clone(&self.config);
        let prefix_str = prefix.map(ToString::to_string);

        let store = Self { config };

        let fut = async move {
            match store.list_all_files(prefix_str).await {
                Ok(files) => futures::stream::iter(files.into_iter().map(Ok)).boxed(),
                Err(e) => futures::stream::once(async move { Err(e) }).boxed(),
            }
        };

        futures::stream::once(fut).flatten().boxed()
    }

    fn list_with_offset(
        &self,
        _prefix: Option<&Path>,
        _offset: &Path,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        futures::stream::once(async {
            Err(object_store::Error::NotSupported {
                source: "NFS list_with_offset not implemented".into(),
            })
        })
        .boxed()
    }

    async fn list_with_delimiter(
        &self,
        _prefix: Option<&Path>,
    ) -> object_store::Result<ListResult> {
        Err(object_store::Error::NotSupported {
            source: "NFS list_with_delimiter not implemented".into(),
        })
    }

    async fn copy(&self, _from: &Path, _to: &Path) -> object_store::Result<()> {
        Err(object_store::Error::NotSupported {
            source: "NFS copy not implemented".into(),
        })
    }

    async fn copy_if_not_exists(&self, _from: &Path, _to: &Path) -> object_store::Result<()> {
        Err(object_store::Error::NotSupported {
            source: "NFS copy_if_not_exists not implemented".into(),
        })
    }
}
