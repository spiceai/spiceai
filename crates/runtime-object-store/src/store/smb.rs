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

use std::future::Future;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bb8::{Pool, PooledConnection};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::StreamExt;
use futures::stream::BoxStream;
use object_store::{
    Attributes, GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult, path::Path,
};
use smb::resource::file_util::ReadAt;
use smb::{
    Client, ClientConfig, ConnectionConfig, CreateDisposition, CreateOptions, FileAccessMask,
    FileAttributes, FileBothDirectoryInformation, FileStandardInformation, Resource, UncPath,
    resource::{Directory, FileCreateArgs},
};
use tokio::sync::OnceCell;

use super::common::{
    DirEntry, build_byte_range, build_object_meta, generic_error, process_directory_entries,
    process_directory_entries_shallow, resolve_range,
};

const STORE_NAME: &str = "SMB";
/// Maximum number of concurrent directory listings for parallel traversal.
const MAX_CONCURRENT_LISTINGS: usize = 8;
/// Default connection pool size.
const DEFAULT_POOL_SIZE: u32 = 4;

fn handle_error<T: Into<Box<dyn std::error::Error + Sync + Send>>>(
    error: T,
) -> object_store::Error {
    generic_error(STORE_NAME, error)
}

/// Convert Windows FILETIME (100-nanosecond intervals since Jan 1, 1601)
/// to Unix timestamp (seconds since Jan 1, 1970)
fn filetime_to_datetime(filetime: u64) -> DateTime<Utc> {
    let unix_secs = (filetime / 10_000_000).saturating_sub(11_644_473_600);
    let secs_i64 = i64::try_from(unix_secs).unwrap_or(i64::MAX);
    DateTime::<Utc>::from_timestamp(secs_i64, 0).unwrap_or_else(Utc::now)
}

/// Connection manager for bb8 connection pool.
#[derive(Clone)]
struct SMBConnectionManager {
    server: String,
    share: String,
    username: String,
    password: String,
    timeout: Option<Duration>,
}

impl std::fmt::Debug for SMBConnectionManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SMBConnectionManager")
            .field("server", &self.server)
            .field("share", &self.share)
            .field("username", &self.username)
            .field("password", &"[REDACTED]")
            .field("timeout", &self.timeout)
            .finish()
    }
}

impl bb8::ManageConnection for SMBConnectionManager {
    type Connection = Client;
    type Error = object_store::Error;

    fn connect(&self) -> impl Future<Output = Result<Self::Connection, Self::Error>> + Send {
        let server = self.server.clone();
        let share = self.share.clone();
        let username = self.username.clone();
        let password = self.password.clone();
        let timeout = self.timeout;

        Box::pin(async move {
            let client_config = ClientConfig {
                connection: ConnectionConfig {
                    timeout,
                    ..ConnectionConfig::default()
                },
                ..ClientConfig::default()
            };
            let client = Client::new(client_config);

            let unc_string = format!(r"\\{server}\{share}");
            let target_path =
                UncPath::from_str(&unc_string).map_err(|e| object_store::Error::Generic {
                    store: STORE_NAME,
                    source: format!("Invalid UNC path {unc_string}: {e}").into(),
                })?;

            client
                .share_connect(&target_path, &username, password)
                .await
                .map_err(handle_error)?;

            Ok(client)
        })
    }

    fn is_valid(
        &self,
        _conn: &mut Self::Connection,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        Box::pin(async move { Ok(()) })
    }

    fn has_broken(&self, _conn: &mut Self::Connection) -> bool {
        false
    }
}

#[derive(Clone)]
struct SMBClientConfig {
    server: String,
    share: String,
    username: String,
    password: String,
    timeout: Option<Duration>,
}

impl std::fmt::Debug for SMBClientConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SMBClientConfig")
            .field("server", &self.server)
            .field("share", &self.share)
            .field("username", &self.username)
            .field("password", &"[REDACTED]")
            .field("timeout", &self.timeout)
            .finish()
    }
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
            store: STORE_NAME,
            source: format!("Invalid UNC path {unc_string}: {e}").into(),
        })
    }

    fn unc_path_with_subpath(&self, subpath: &str) -> object_store::Result<UncPath> {
        let base = self.unc_path()?;
        if subpath.is_empty() {
            return Ok(base);
        }
        let smb_path = subpath.replace('/', r"\");
        let unc_string = format!(r"{base}\{smb_path}");
        UncPath::from_str(&unc_string).map_err(|e| object_store::Error::Generic {
            store: STORE_NAME,
            source: format!("Invalid UNC path {unc_string}: {e}").into(),
        })
    }

    fn create_pool_manager(&self) -> SMBConnectionManager {
        SMBConnectionManager {
            server: self.server.clone(),
            share: self.share.clone(),
            username: self.username.clone(),
            password: self.password.clone(),
            timeout: self.timeout,
        }
    }
}

/// Inner state holding the lazily-initialized connection pool.
struct SMBInner {
    config: Arc<SMBClientConfig>,
    pool: OnceCell<Pool<SMBConnectionManager>>,
}

impl SMBInner {
    fn new(config: Arc<SMBClientConfig>) -> Self {
        Self {
            config,
            pool: OnceCell::new(),
        }
    }

    async fn get_pool(&self) -> object_store::Result<&Pool<SMBConnectionManager>> {
        self.pool
            .get_or_try_init(|| async {
                let manager = self.config.create_pool_manager();
                Pool::builder()
                    .max_size(DEFAULT_POOL_SIZE)
                    .build(manager)
                    .await
                    .map_err(handle_error)
            })
            .await
    }

    async fn get_connection(
        &self,
    ) -> object_store::Result<PooledConnection<'_, SMBConnectionManager>> {
        let pool = self.get_pool().await?;
        pool.get().await.map_err(handle_error)
    }
}

impl std::fmt::Debug for SMBInner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SMBInner")
            .field("config", &self.config)
            .field("pool_initialized", &self.pool.initialized())
            .finish()
    }
}

#[derive(Debug, Clone)]
pub struct SMBObjectStore {
    inner: Arc<SMBInner>,
}

impl std::fmt::Display for SMBObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SMB")
    }
}

impl SMBObjectStore {
    /// Create a new SMB object store with lazy connection pooling.
    /// The connection pool is initialized on first use.
    #[must_use]
    pub fn new(
        server: String,
        share: String,
        username: String,
        password: String,
        timeout: Option<Duration>,
    ) -> Self {
        let config = Arc::new(SMBClientConfig::new(
            server, share, username, password, timeout,
        ));
        Self {
            inner: Arc::new(SMBInner::new(config)),
        }
    }

    async fn list_directory(
        client: &Client,
        config: &SMBClientConfig,
        dir_path: &str,
    ) -> object_store::Result<Vec<DirEntry>> {
        let unc_path = config.unc_path_with_subpath(dir_path)?;

        let dir_open_args = FileCreateArgs {
            desired_access: FileAccessMask::new().with_generic_read(true),
            disposition: CreateDisposition::Open,
            options: CreateOptions::new().with_directory_file(true),
            attributes: FileAttributes::default(),
        };

        let resource = match client.create_file(&unc_path, &dir_open_args).await {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!("Failed to open directory {unc_path}: {e}");
                return Ok(Vec::new());
            }
        };

        let Resource::Directory(directory) = resource else {
            tracing::warn!("Expected directory but got different resource type for {unc_path}");
            return Ok(Vec::new());
        };

        let dir_arc = Arc::new(directory);
        let query_stream =
            match Directory::query::<FileBothDirectoryInformation>(&dir_arc, "*").await {
                Ok(s) => s,
                Err(e) => {
                    tracing::warn!("Failed to query directory {unc_path}: {e}");
                    let _ = dir_arc.close().await;
                    return Ok(Vec::new());
                }
            };

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

        let _ = dir_arc.close().await;

        let entries = smb_entries
            .into_iter()
            .map(|e| {
                if e.file_attributes.directory() {
                    DirEntry::directory(e.file_name.to_string())
                } else {
                    DirEntry::file(
                        e.file_name.to_string(),
                        e.end_of_file,
                        filetime_to_datetime(*e.last_write_time),
                    )
                }
            })
            .collect();

        Ok(entries)
    }

    async fn list_all_files(
        &self,
        prefix: Option<String>,
    ) -> object_store::Result<Vec<ObjectMeta>> {
        let conn = self.inner.get_connection().await?;
        let config = Arc::clone(&self.inner.config);
        let mut results = Vec::new();
        let mut queue = vec![prefix.unwrap_or_default()];

        while !queue.is_empty() {
            let batch: Vec<_> = queue
                .drain(..queue.len().min(MAX_CONCURRENT_LISTINGS))
                .collect();

            let futures: Vec<_> = batch
                .iter()
                .map(|dir_path| Self::list_directory(&conn, &config, dir_path))
                .collect();

            let batch_results = futures::future::join_all(futures).await;

            for (dir_path, result) in batch.into_iter().zip(batch_results) {
                match result {
                    Ok(entries) => {
                        let (files, dirs) = process_directory_entries(&dir_path, entries);
                        results.extend(files);
                        queue.extend(dirs);
                    }
                    Err(e) => {
                        tracing::warn!("Failed to list directory {dir_path}: {e}");
                    }
                }
            }
        }

        Ok(results)
    }

    async fn list_directory_shallow(
        &self,
        prefix: Option<&Path>,
    ) -> object_store::Result<ListResult> {
        let conn = self.inner.get_connection().await?;
        let prefix_str = prefix.map_or(String::new(), Path::to_string);

        let entries = Self::list_directory(&conn, &self.inner.config, &prefix_str).await?;
        Ok(process_directory_entries_shallow(&prefix_str, entries))
    }

    async fn get_file_metadata(&self, location: &Path) -> object_store::Result<ObjectMeta> {
        let conn = self.inner.get_connection().await?;
        let location_str = location.to_string();
        let file_path = self.inner.config.unc_path_with_subpath(&location_str)?;

        let file_open_args = FileCreateArgs {
            desired_access: FileAccessMask::new().with_file_read_attributes(true),
            disposition: CreateDisposition::Open,
            options: CreateOptions::new().with_non_directory_file(true),
            attributes: FileAttributes::default(),
        };

        let resource = conn
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

        let file_info: FileStandardInformation = file.query_info().await.map_err(handle_error)?;
        let _ = file.close().await;

        Ok(build_object_meta(
            location.clone(),
            file_info.end_of_file,
            Utc::now(),
        ))
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
        let conn = self.inner.get_connection().await?;
        let location_str = location.to_string();
        let file_path = self.inner.config.unc_path_with_subpath(&location_str)?;

        let file_open_args = FileCreateArgs {
            desired_access: FileAccessMask::new()
                .with_generic_read(true)
                .with_file_read_data(true)
                .with_file_read_attributes(true),
            disposition: CreateDisposition::Open,
            options: CreateOptions::new().with_non_directory_file(true),
            attributes: FileAttributes::default(),
        };

        let resource = conn
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

        let file_info: FileStandardInformation = file.query_info().await.map_err(handle_error)?;

        let size = file_info.end_of_file;
        let object_meta = build_object_meta(location.clone(), size, Utc::now());

        let (start, end, data_to_read) = resolve_range(options.range.as_ref(), size);

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
            range: build_byte_range(start, end),
            attributes: Attributes::default(),
        })
    }

    async fn head(&self, location: &Path) -> object_store::Result<ObjectMeta> {
        self.get_file_metadata(location).await
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
        let store = self.clone();
        let prefix_str = prefix.map(ToString::to_string);

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
                source: "SMB list_with_offset not implemented".into(),
            })
        })
        .boxed()
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.list_directory_shallow(prefix).await
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_filetime_to_datetime() {
        // Windows FILETIME epoch is Jan 1, 1601.
        // 11644473600 seconds between 1601 and 1970 (Unix epoch)
        // For 2024-01-01 00:00:00 UTC: Unix timestamp = 1704067200
        // FILETIME = (1704067200 + 11644473600) * 10_000_000
        let unix_ts = 1704067200u64; // 2024-01-01 00:00:00 UTC
        let filetime = (unix_ts + 11_644_473_600) * 10_000_000;
        let dt = filetime_to_datetime(filetime);
        assert_eq!(dt.format("%Y-%m-%d").to_string(), "2024-01-01");
    }

    #[test]
    fn test_filetime_to_datetime_zero() {
        let dt = filetime_to_datetime(0);
        // Should return a valid DateTime (possibly before Unix epoch)
        assert!(dt.timestamp() <= 0);
    }

    #[test]
    fn test_smb_object_store_display() {
        let store = SMBObjectStore::new(
            "server.local".to_string(),
            "share".to_string(),
            "user".to_string(),
            "pass".to_string(),
            None,
        );
        assert_eq!(format!("{store}"), "SMB");
    }

    #[test]
    fn test_dir_entry_from_file_info() {
        // Test that file information creates correct DirEntry
        let entry = DirEntry::file(
            "test.txt".to_string(),
            1024,
            DateTime::<Utc>::from_timestamp(1700000000, 0).expect("valid timestamp"),
        );
        assert_eq!(entry.name, "test.txt");
        assert!(!entry.is_dir);
        assert_eq!(entry.size, 1024);
    }
}
