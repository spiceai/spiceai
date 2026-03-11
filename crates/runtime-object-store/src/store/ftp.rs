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
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bb8::{Pool, PooledConnection};
use bytes::Bytes;
use futures::AsyncReadExt;
use futures::StreamExt;
use futures::stream::BoxStream;
use object_store::{Attributes, ListResult, MultipartUpload, PutMultipartOptions, PutPayload};
use object_store::{
    GetOptions, GetResult, GetResultPayload, ObjectMeta, ObjectStore, PutOptions, PutResult,
    path::Path,
};
use suppaftp::AsyncFtpStream;
use suppaftp::types::FileType;
use tokio::sync::OnceCell;

use super::common::{
    DirEntry, build_byte_range, build_object_meta, generic_error, process_directory_entries,
    process_directory_entries_shallow, resolve_range, should_skip_entry,
};

const STORE_NAME: &str = "FTP";
/// Maximum number of concurrent directory listings for parallel traversal.
const MAX_CONCURRENT_LISTINGS: usize = 4;
/// Default connection pool size.
const DEFAULT_POOL_SIZE: u32 = 4;

/// Connection manager for bb8 connection pool.
#[derive(Clone)]
struct FTPConnectionManager {
    user: String,
    password: String,
    host: String,
    port: String,
    timeout: Option<Duration>,
}

impl std::fmt::Debug for FTPConnectionManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FTPConnectionManager")
            .field("user", &self.user)
            .field("password", &"[REDACTED]")
            .field("host", &self.host)
            .field("port", &self.port)
            .field("timeout", &self.timeout)
            .finish()
    }
}

impl bb8::ManageConnection for FTPConnectionManager {
    type Connection = AsyncFtpStream;
    type Error = object_store::Error;

    fn connect(&self) -> impl Future<Output = Result<Self::Connection, Self::Error>> + Send {
        let user = self.user.clone();
        let password = self.password.clone();
        let host = self.host.clone();
        let port = self.port.clone();
        let timeout = self.timeout;

        Box::pin(async move {
            let mut client = match timeout {
                Some(timeout) => {
                    AsyncFtpStream::connect_timeout(
                        format!("{host}:{port}").parse().map_err(
                            |e: std::net::AddrParseError| object_store::Error::Generic {
                                store: STORE_NAME,
                                source: e.into(),
                            },
                        )?,
                        timeout,
                    )
                    .await
                }
                None => AsyncFtpStream::connect(format!("{host}:{port}")).await,
            }
            .map_err(|e| object_store::Error::Generic {
                store: STORE_NAME,
                source: e.into(),
            })?;

            client
                .login(&user, &password)
                .await
                .map_err(|e| object_store::Error::Generic {
                    store: STORE_NAME,
                    source: e.into(),
                })?;

            Ok(client)
        })
    }

    fn is_valid(
        &self,
        conn: &mut Self::Connection,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        let noop_future = conn.noop();
        Box::pin(async move { noop_future.await.map_err(|e| generic_error(STORE_NAME, e)) })
    }

    fn has_broken(&self, conn: &mut Self::Connection) -> bool {
        // Use the underlying TCP stream as a simple, non-blocking health heuristic.
        // If we cannot obtain the peer address, treat the connection as broken so
        // that the pool can proactively discard it.
        conn.get_ref().peer_addr().is_err()
    }
}

#[derive(Clone)]
struct FTPClientConfig {
    user: String,
    password: String,
    host: String,
    port: String,
    timeout: Option<Duration>,
}

impl std::fmt::Debug for FTPClientConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FTPClientConfig")
            .field("user", &self.user)
            .field("password", &"[REDACTED]")
            .field("host", &self.host)
            .field("port", &self.port)
            .field("timeout", &self.timeout)
            .finish()
    }
}

impl FTPClientConfig {
    fn new(
        user: String,
        password: String,
        host: String,
        port: String,
        timeout: Option<Duration>,
    ) -> Self {
        Self {
            user,
            password,
            host,
            port,
            timeout,
        }
    }

    fn create_pool_manager(&self) -> FTPConnectionManager {
        FTPConnectionManager {
            user: self.user.clone(),
            password: self.password.clone(),
            host: self.host.clone(),
            port: self.port.clone(),
            timeout: self.timeout,
        }
    }

    /// Create a fresh non-pooled connection for operations that modify connection state.
    async fn get_fresh_client(&self) -> object_store::Result<AsyncFtpStream> {
        let mut client = match self.timeout {
            Some(timeout) => {
                AsyncFtpStream::connect_timeout(
                    format!("{}:{}", self.host, self.port).parse().map_err(
                        |e: std::net::AddrParseError| object_store::Error::Generic {
                            store: STORE_NAME,
                            source: e.into(),
                        },
                    )?,
                    timeout,
                )
                .await
            }
            None => AsyncFtpStream::connect(format!("{}:{}", self.host, self.port)).await,
        }
        .map_err(|e| generic_error(STORE_NAME, e))?;

        client
            .login(&self.user, &self.password)
            .await
            .map_err(|e| generic_error(STORE_NAME, e))?;

        Ok(client)
    }
}

/// Inner state holding the lazily-initialized connection pool.
struct FTPInner {
    config: Arc<FTPClientConfig>,
    pool: OnceCell<Pool<FTPConnectionManager>>,
}

impl FTPInner {
    fn new(config: Arc<FTPClientConfig>) -> Self {
        Self {
            config,
            pool: OnceCell::new(),
        }
    }

    async fn get_pool(&self) -> object_store::Result<&Pool<FTPConnectionManager>> {
        self.pool
            .get_or_try_init(|| async {
                let manager = self.config.create_pool_manager();
                Pool::builder()
                    .max_size(DEFAULT_POOL_SIZE)
                    .build(manager)
                    .await
                    .map_err(|e| generic_error(STORE_NAME, e))
            })
            .await
    }

    async fn get_connection(
        &self,
    ) -> object_store::Result<PooledConnection<'_, FTPConnectionManager>> {
        let pool = self.get_pool().await?;
        pool.get().await.map_err(|e| generic_error(STORE_NAME, e))
    }
}

impl std::fmt::Debug for FTPInner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FTPInner")
            .field("config", &self.config)
            .field("pool_initialized", &self.pool.initialized())
            .finish()
    }
}

#[derive(Debug, Clone)]
pub struct FTPObjectStore {
    inner: Arc<FTPInner>,
}

impl std::fmt::Display for FTPObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "FTP")
    }
}

impl FTPObjectStore {
    /// Create a new FTP object store with lazy connection pooling.
    /// The connection pool is initialized on first use.
    #[must_use]
    pub fn new(
        user: String,
        password: String,
        host: String,
        port: String,
        timeout: Option<Duration>,
    ) -> Self {
        let config = Arc::new(FTPClientConfig::new(user, password, host, port, timeout));
        Self {
            inner: Arc::new(FTPInner::new(config)),
        }
    }

    /// List a single directory and return its entries.
    async fn list_directory(
        conn: &mut AsyncFtpStream,
        dir_path: &str,
    ) -> object_store::Result<Vec<DirEntry>> {
        let path = if dir_path.is_empty() {
            None
        } else {
            Some(dir_path)
        };

        let list = conn
            .nlst(path)
            .await
            .map_err(|e| object_store::Error::NotFound {
                path: dir_path.to_string(),
                source: e.into(),
            })?;

        let mut entries = Vec::new();

        for item in list {
            let name = item.rsplit('/').next().unwrap_or(&item);
            if should_skip_entry(name) {
                continue;
            }

            // Check if it's a directory by listing it
            let children =
                conn.nlst(Some(&item))
                    .await
                    .map_err(|e| object_store::Error::NotFound {
                        path: item.clone(),
                        source: e.into(),
                    })?;

            if children.is_empty() {
                continue;
            }

            if children[0] == item {
                // It's a file
                let size = conn
                    .size(&item)
                    .await
                    .map_err(|e| object_store::Error::NotFound {
                        path: item.clone(),
                        source: e.into(),
                    })?;
                let last_modified =
                    conn.mdtm(&item)
                        .await
                        .map_err(|e| object_store::Error::NotFound {
                            path: item.clone(),
                            source: e.into(),
                        })?;

                entries.push(DirEntry::file(
                    name.to_string(),
                    u64::try_from(size).unwrap_or(0),
                    last_modified.and_utc(),
                ));
            } else {
                // It's a directory
                entries.push(DirEntry::directory(name.to_string()));
            }
        }

        Ok(entries)
    }

    /// List all files recursively using sequential directory traversal.
    ///
    /// Note: FTP is a stateful protocol where commands like NLST, SIZE, and MDTM modify
    /// connection state. We must use fresh connections for each directory to avoid race
    /// conditions. The batching here is for queue management, not parallelism.
    async fn list_all_files(
        &self,
        location: Option<Path>,
    ) -> object_store::Result<Vec<ObjectMeta>> {
        let path = location.map(|v| v.to_string());
        let mut queue = vec![path.unwrap_or_default()];
        let mut results = Vec::new();

        while !queue.is_empty() {
            // Drain up to MAX_CONCURRENT_LISTINGS from the queue for processing.
            // Note: directories are processed sequentially (not in parallel) because FTP
            // is a stateful protocol and each operation requires its own fresh connection.
            let batch: Vec<_> = queue
                .drain(..queue.len().min(MAX_CONCURRENT_LISTINGS))
                .collect();

            let mut batch_results = Vec::with_capacity(batch.len());
            for dir_path in &batch {
                let mut client = self.inner.config.get_fresh_client().await?;
                let result = Self::list_directory(&mut client, dir_path).await;
                batch_results.push(result);
            }

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

    /// List a single directory level (for `list_with_delimiter`).
    async fn list_directory_shallow(
        &self,
        prefix: Option<&Path>,
    ) -> object_store::Result<ListResult> {
        let mut conn = self.inner.get_connection().await?;
        let prefix_str = prefix.map_or(String::new(), Path::to_string);

        let entries = Self::list_directory(&mut conn, &prefix_str).await?;
        Ok(process_directory_entries_shallow(&prefix_str, entries))
    }

    /// Get file metadata without reading content.
    async fn get_file_metadata(&self, location: &Path) -> object_store::Result<ObjectMeta> {
        let mut conn = self.inner.get_connection().await?;
        let location_string = location.to_string();

        let size: u64 = u64::try_from(conn.size(&location_string).await.map_err(|e| {
            object_store::Error::NotFound {
                path: location_string.clone(),
                source: e.into(),
            }
        })?)
        .unwrap_or(0);

        let last_modified = conn
            .mdtm(&location_string)
            .await
            .map_err(|e| object_store::Error::NotFound {
                path: location_string.clone(),
                source: e.into(),
            })?
            .and_utc();

        Ok(build_object_meta(location.clone(), size, last_modified))
    }
}

/// Read data from an FTP stream asynchronously
async fn read_ftp_data(
    mut client: AsyncFtpStream,
    location: String,
    start: usize,
    read_size: usize,
) -> object_store::Result<Vec<u8>> {
    client
        .transfer_type(FileType::Binary)
        .await
        .map_err(|e| generic_error(STORE_NAME, e))?;

    client
        .resume_transfer(start)
        .await
        .map_err(|e| generic_error(STORE_NAME, e))?;

    let mut stream = client
        .retr_as_stream(location)
        .await
        .map_err(|e| generic_error(STORE_NAME, e))?;

    let mut result = Vec::with_capacity(read_size);
    let mut buf = vec![0; 4096];
    let mut total = 0;

    loop {
        if total >= read_size {
            break;
        }

        let n = stream
            .read(&mut buf)
            .await
            .map_err(|e| generic_error(STORE_NAME, e))?;

        if n == 0 {
            break;
        }

        let bytes_to_take = (read_size - total).min(n);
        result.extend_from_slice(&buf[..bytes_to_take]);
        total += n;
    }

    Ok(result)
}

#[async_trait]
impl ObjectStore for FTPObjectStore {
    async fn put_opts(
        &self,
        _location: &Path,
        _payload: PutPayload,
        _opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        Err(object_store::Error::NotSupported {
            source: "FTP put_opts not implemented".into(),
        })
    }

    async fn put_multipart_opts(
        &self,
        _location: &Path,
        _opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        Err(object_store::Error::NotSupported {
            source: "FTP put_multipart_opts not implemented".into(),
        })
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        // Use fresh client for data transfer (state-modifying operation)
        let client = self.inner.config.get_fresh_client().await?;

        let location_string = location.to_string();

        // Get metadata using pooled connection
        let mut meta_client = self.inner.get_connection().await?;

        let size: u64 = u64::try_from(meta_client.size(&location_string).await.map_err(|e| {
            object_store::Error::NotFound {
                path: location_string.clone(),
                source: e.into(),
            }
        })?)
        .unwrap_or(0);

        let last_modified = meta_client
            .mdtm(&location_string)
            .await
            .map_err(|e| object_store::Error::NotFound {
                path: location_string.clone(),
                source: e.into(),
            })?
            .and_utc();

        let object_meta = build_object_meta(location.clone(), size, last_modified);

        let (start, end, data_to_read) = resolve_range(options.range.as_ref(), size);

        #[expect(clippy::cast_possible_truncation)]
        let data = read_ftp_data(
            client,
            location_string,
            start as usize,
            data_to_read as usize,
        )
        .await?;

        let stream = futures::stream::once(async move { Ok(Bytes::from(data)) });

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
            source: "FTP delete not implemented".into(),
        })
    }

    fn delete_stream<'a>(
        &'a self,
        _locations: BoxStream<'a, object_store::Result<Path>>,
    ) -> BoxStream<'a, object_store::Result<Path>> {
        futures::stream::once(async {
            Err(object_store::Error::NotSupported {
                source: "FTP delete_stream not implemented".into(),
            })
        })
        .boxed()
    }

    fn list(
        &self,
        location: Option<&Path>,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        let store = self.clone();
        let location = location.map(ToOwned::to_owned);

        let fut = async move {
            match store.list_all_files(location).await {
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
                source: "FTP list_with_offset not implemented".into(),
            })
        })
        .boxed()
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.list_directory_shallow(prefix).await
    }

    async fn copy(&self, _from: &Path, _to: &Path) -> object_store::Result<()> {
        Err(object_store::Error::NotSupported {
            source: "FTP copy not implemented".into(),
        })
    }

    async fn copy_if_not_exists(&self, _from: &Path, _to: &Path) -> object_store::Result<()> {
        Err(object_store::Error::NotSupported {
            source: "FTP copy_if_not_exists not implemented".into(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    #[test]
    fn test_ftp_object_store_display() {
        let store = FTPObjectStore::new(
            "anonymous".to_string(),
            "anonymous@example.com".to_string(),
            "ftp.example.com".to_string(),
            "21".to_string(),
            None,
        );
        assert_eq!(format!("{store}"), "FTP");
    }

    #[test]
    fn test_ftp_client_config_clone() {
        let config = FTPClientConfig {
            user: "user".to_string(),
            password: "pass".to_string(),
            host: "localhost".to_string(),
            port: "21".to_string(),
            timeout: Some(Duration::from_secs(30)),
        };
        let cloned = config;
        assert_eq!(cloned.host, "localhost");
        assert_eq!(cloned.port, "21");
    }

    #[test]
    fn test_dir_entry_file_creation() {
        let ts = Utc::now();
        let entry = DirEntry::file("data.csv".to_string(), 2048, ts);
        assert!(!entry.is_dir);
        assert_eq!(entry.size, 2048);
    }

    #[test]
    fn test_dir_entry_directory_creation() {
        let entry = DirEntry::directory("subdir".to_string());
        assert!(entry.is_dir);
        assert_eq!(entry.size, 0);
    }
}
