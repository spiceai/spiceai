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
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::StreamExt;
use futures::stream::BoxStream;
use object_store::{
    Attributes, GetOptions, GetRange, GetResult, GetResultPayload, ListResult, MultipartUpload,
    ObjectMeta, ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult, path::Path,
};
use smb::resource::file_util::ReadAt;
use smb::{
    Client, ClientConfig, ConnectionConfig, CreateDisposition, CreateOptions, FileAccessMask,
    FileAttributes, FileBothDirectoryInformation, FileStandardInformation, Resource, UncPath,
    resource::{Directory, FileCreateArgs},
};

use super::common::{DirEntry, generic_error, process_directory_entries};

const STORE_NAME: &str = "SMB";

fn handle_error<T: Into<Box<dyn std::error::Error + Sync + Send>>>(
    error: T,
) -> object_store::Error {
    generic_error(STORE_NAME, error)
}

/// Convert Windows FILETIME (100-nanosecond intervals since Jan 1, 1601)
/// to Unix timestamp (seconds since Jan 1, 1970)
fn filetime_to_datetime(filetime: u64) -> DateTime<Utc> {
    // FILETIME is 100-nanosecond intervals since Jan 1, 1601
    // Unix epoch is Jan 1, 1970
    // Difference is 11644473600 seconds (369 years)
    let unix_secs = (filetime / 10_000_000).saturating_sub(11_644_473_600);
    let secs_i64 = i64::try_from(unix_secs).unwrap_or(i64::MAX);
    DateTime::<Utc>::from_timestamp(secs_i64, 0).unwrap_or_else(Utc::now)
}

#[derive(Debug, Clone)]
struct SMBClientConfig {
    server: String,
    share: String,
    username: String,
    password: String,
    timeout: Option<Duration>,
}

impl SMBClientConfig {
    fn new(
        server: String,
        share: String,
        username: String,
        password: String,
        timeout: Option<Duration>,
    ) -> Self {
        Self {
            server,
            share,
            username,
            password,
            timeout,
        }
    }

    fn unc_path(&self) -> object_store::Result<UncPath> {
        let unc_string = format!(r"\\{}\{}", self.server, self.share);
        UncPath::from_str(&unc_string).map_err(|e| object_store::Error::Generic {
            store: "SMB",
            source: format!("Invalid UNC path {unc_string}: {e}").into(),
        })
    }

    fn unc_path_with_subpath(&self, subpath: &str) -> object_store::Result<UncPath> {
        let base = self.unc_path()?;
        if subpath.is_empty() {
            return Ok(base);
        }
        // Convert forward slashes to backslashes for SMB paths
        let smb_path = subpath.replace('/', r"\");
        let unc_string = format!(r"{base}\{smb_path}");
        UncPath::from_str(&unc_string).map_err(|e| object_store::Error::Generic {
            store: "SMB",
            source: format!("Invalid UNC path {unc_string}: {e}").into(),
        })
    }

    async fn connect(&self) -> object_store::Result<Client> {
        let client_config = ClientConfig {
            connection: ConnectionConfig {
                timeout: self.timeout,
                ..ConnectionConfig::default()
            },
            ..ClientConfig::default()
        };
        let client = Client::new(client_config);
        let target_path = self.unc_path()?;

        client
            .share_connect(&target_path, &self.username, self.password.clone())
            .await
            .map_err(handle_error)?;

        Ok(client)
    }
}

#[derive(Debug)]
pub struct SMBObjectStore {
    config: Arc<SMBClientConfig>,
}

impl std::fmt::Display for SMBObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SMB")
    }
}

impl SMBObjectStore {
    #[must_use]
    pub fn new(
        server: String,
        share: String,
        username: String,
        password: String,
        timeout: Option<Duration>,
    ) -> Self {
        Self {
            config: Arc::new(SMBClientConfig::new(
                server, share, username, password, timeout,
            )),
        }
    }

    /// List all files recursively starting from a given path
    async fn list_all_files(
        &self,
        prefix: Option<String>,
    ) -> object_store::Result<Vec<ObjectMeta>> {
        let client = self.config.connect().await?;
        let mut results = Vec::new();
        let mut queue = vec![prefix.unwrap_or_default()];

        while let Some(current_path) = queue.pop() {
            let dir_path = self.config.unc_path_with_subpath(&current_path)?;

            // Open directory for listing
            let dir_open_args = FileCreateArgs {
                desired_access: FileAccessMask::new().with_generic_read(true),
                disposition: CreateDisposition::Open,
                options: CreateOptions::new().with_directory_file(true),
                attributes: FileAttributes::default(),
            };

            let resource = match client.create_file(&dir_path, &dir_open_args).await {
                Ok(r) => r,
                Err(e) => {
                    tracing::warn!("Failed to open directory {dir_path}: {e}");
                    continue;
                }
            };

            let Resource::Directory(directory) = resource else {
                tracing::warn!("Expected directory but got different resource type for {dir_path}");
                continue;
            };

            // Query directory contents
            let dir_arc = Arc::new(directory);
            let query_stream =
                match Directory::query::<FileBothDirectoryInformation>(&dir_arc, "*").await {
                    Ok(s) => s,
                    Err(e) => {
                        tracing::warn!("Failed to query directory {dir_path}: {e}");
                        continue;
                    }
                };

            // Collect entries from the stream
            let smb_entries: Vec<_> = query_stream
                .filter_map(|r| async {
                    match r {
                        Ok(entry) => Some(entry),
                        Err(e) => {
                            tracing::warn!("Error reading directory entry: {e}");
                            None
                        }
                    }
                })
                .collect()
                .await;

            // Convert SMB entries to common DirEntry format
            let entries = smb_entries.into_iter().map(|e| {
                if e.file_attributes.directory() {
                    DirEntry::directory(e.file_name.to_string())
                } else {
                    DirEntry::file(
                        e.file_name.to_string(),
                        e.end_of_file,
                        filetime_to_datetime(*e.last_write_time),
                    )
                }
            });

            // Use common utility to process entries
            let (files, dirs) = process_directory_entries(&current_path, entries);
            results.extend(files);
            queue.extend(dirs);

            // Close the directory handle
            let _ = dir_arc.close().await;
        }

        Ok(results)
    }
}

#[async_trait]
impl ObjectStore for SMBObjectStore {
    async fn put_opts(
        &self,
        _location: &Path,
        _payload: PutPayload,
        _opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        Err(object_store::Error::NotSupported {
            source: "SMB put_opts not implemented".into(),
        })
    }

    async fn put_multipart_opts(
        &self,
        _location: &Path,
        _opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        Err(object_store::Error::NotSupported {
            source: "SMB put_multipart_opts not implemented".into(),
        })
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        let client = self.config.connect().await?;
        let location_str = location.to_string();
        let file_path = self.config.unc_path_with_subpath(&location_str)?;

        // Open file for reading
        let file_open_args = FileCreateArgs {
            desired_access: FileAccessMask::new()
                .with_generic_read(true)
                .with_file_read_data(true)
                .with_file_read_attributes(true),
            disposition: CreateDisposition::Open,
            options: CreateOptions::new().with_non_directory_file(true),
            attributes: FileAttributes::default(),
        };

        let resource = client
            .create_file(&file_path, &file_open_args)
            .await
            .map_err(|e| object_store::Error::NotFound {
                path: location_str.clone(),
                source: e.into(),
            })?;

        let Resource::File(file) = resource else {
            return Err(object_store::Error::NotFound {
                path: location_str,
                source: "Path is not a file".into(),
            });
        };

        // Get file info for size and modification time
        let file_info: FileStandardInformation = file.query_info().await.map_err(handle_error)?;

        let size = file_info.end_of_file;
        let object_meta = ObjectMeta {
            location: location.clone(),
            size,
            last_modified: Utc::now(), // Standard info doesn't include times
            e_tag: None,
            version: None,
        };

        let mut start = 0u64;
        let mut end = size;

        if let Some(GetRange::Bounded(range)) = options.range {
            start = range.start;
            end = range.end;
        }

        let data_to_read = end.saturating_sub(start);
        #[expect(clippy::cast_possible_truncation)]
        let mut buffer = vec![0u8; data_to_read as usize];
        file.read_at(&mut buffer, start)
            .await
            .map_err(handle_error)?;

        let _ = file.close().await;

        let bytes_data = Bytes::from(buffer);
        let stream = futures::stream::once(async move { Ok(bytes_data) });

        Ok(GetResult {
            meta: object_meta,
            payload: GetResultPayload::Stream(Box::pin(stream)),
            range: Range { start, end },
            attributes: Attributes::default(),
        })
    }

    async fn delete(&self, _location: &Path) -> object_store::Result<()> {
        Err(object_store::Error::NotSupported {
            source: "SMB delete not implemented".into(),
        })
    }

    fn delete_stream<'a>(
        &'a self,
        _locations: BoxStream<'a, object_store::Result<Path>>,
    ) -> BoxStream<'a, object_store::Result<Path>> {
        futures::stream::once(async {
            Err(object_store::Error::NotSupported {
                source: "SMB delete_stream not implemented".into(),
            })
        })
        .boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        let config = Arc::clone(&self.config);
        let prefix_str = prefix.map(ToString::to_string);

        // Create a future that lists all files, then convert to stream
        let store = Self { config };

        let fut = async move {
            match store.list_all_files(prefix_str).await {
                Ok(files) => futures::stream::iter(files.into_iter().map(Ok)).boxed(),
                Err(e) => futures::stream::once(async move { Err(e) }).boxed(),
            }
        };

        // Use try_flatten_stream pattern to convert Future<Stream> to Stream
        futures::stream::once(fut).flatten().boxed()
    }

    fn list_with_offset(
        &self,
        _prefix: Option<&Path>,
        _offset: &Path,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        futures::stream::once(async {
            Err(object_store::Error::NotSupported {
                source: "SMB list_with_offset not implemented".into(),
            })
        })
        .boxed()
    }

    async fn list_with_delimiter(
        &self,
        _prefix: Option<&Path>,
    ) -> object_store::Result<ListResult> {
        Err(object_store::Error::NotSupported {
            source: "SMB list_with_delimiter not implemented".into(),
        })
    }

    async fn copy(&self, _from: &Path, _to: &Path) -> object_store::Result<()> {
        Err(object_store::Error::NotSupported {
            source: "SMB copy not implemented".into(),
        })
    }

    async fn copy_if_not_exists(&self, _from: &Path, _to: &Path) -> object_store::Result<()> {
        Err(object_store::Error::NotSupported {
            source: "SMB copy_if_not_exists not implemented".into(),
        })
    }
}
