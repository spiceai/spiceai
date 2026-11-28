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
use futures::FutureExt;
use futures::StreamExt;
use futures::stream::BoxStream;
use object_store::{
    Attributes, CopyOptions, ListResult, MultipartUpload, PutMultipartOptions, PutPayload,
};
use object_store::{
    GetOptions, GetResult, GetResultPayload, ObjectMeta, ObjectStore, PutOptions, PutResult,
    path::Path,
};
use suppaftp::tokio::AsyncFtpStream;
use suppaftp::types::FileType;
use tokio::io::AsyncReadExt;
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
/// Wall-clock bound for one connection attempt — name resolution, TCP connect, greeting
/// and login together — when `client_timeout` is not configured.
///
/// `bb8` consults its own `connection_timeout` (30s by default) only *after*
/// `ManageConnection::connect` returns, so an attempt that never returns is never
/// abandoned: its pending pool slot stays taken, which suppresses later `Pool::get`
/// calls even once the server recovers. 20s leaves the attempt failing inside the
/// window the pool is already willing to wait.
const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(20);

/// Apply a connection deadline to `attempt`, reporting expiry against `addr`.
async fn with_connect_deadline<T>(
    addr: &str,
    bound: Duration,
    attempt: impl Future<Output = object_store::Result<T>>,
) -> object_store::Result<T> {
    match tokio::time::timeout(bound, attempt).await {
        Ok(result) => result,
        Err(_) => Err(generic_error(
            STORE_NAME,
            format!("connecting to {addr} timed out after {bound:?}"),
        )),
    }
}

/// Open a session to `host:port` and log in, under a single deadline.
///
/// The deadline covers the whole sequence rather than each stage, so a peer that
/// accepts TCP and then stalls cannot spend the budget once on the greeting and again
/// on the login.
///
/// The address is passed to `AsyncFtpStream::connect` as text so that it resolves the
/// name itself. Handing a pre-parsed `SocketAddr` to `connect_timeout` instead cannot
/// work for a named host: `"host:port".parse::<SocketAddr>()` accepts only a literal
/// IP, so configuring `client_timeout` failed the connection rather than bounding it.
///
/// Resolving inside `attempt` is also what puts the lookup under the deadline, which the
/// SFTP store has to arrange for itself because its resolver is the blocking one.
async fn connect_and_login(
    host: &str,
    port: &str,
    user: &str,
    password: &str,
    timeout: Option<Duration>,
) -> object_store::Result<AsyncFtpStream> {
    let addr = format!("{host}:{port}");
    let bound = timeout.unwrap_or(DEFAULT_CONNECT_TIMEOUT);
    let connect_addr = addr.clone();

    let attempt = async move {
        let mut client = AsyncFtpStream::connect(connect_addr)
            .await
            .map_err(|e| generic_error(STORE_NAME, e))?;

        client
            .login(user, password)
            .await
            .map_err(|e| generic_error(STORE_NAME, e))?;

        Ok::<_, object_store::Error>(client)
    };

    with_connect_deadline(&addr, bound, attempt).await
}

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

        Box::pin(async move { connect_and_login(&host, &port, &user, &password, timeout).await })
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
        // that the pool can proactively discard it. The control socket sits behind
        // an async lock that is uncontended here (the pool holds the only
        // reference); if it cannot be taken without waiting, discard the
        // connection rather than block.
        conn.get_ref()
            .now_or_never()
            .is_none_or(|socket| socket.peer_addr().is_err())
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
        connect_and_login(
            &self.host,
            &self.port,
            &self.user,
            &self.password,
            self.timeout,
        )
        .await
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
}

/// Read `read_size` bytes starting at `start` from `location` on a single-use connection.
///
/// `read_to_eof` is set when the range ends at the file size. Such a download is read
/// to end of file and finished: the server's completion reply is read, and a reply
/// other than success fails the read, because the bytes received may be truncated.
/// A range that ends before the file does is aborted with `ABOR` once `read_size` bytes
/// have arrived; those bytes already hold the whole range, so a failed abort is only
/// logged. So is a download that turns out longer than `read_size` (the file grew after
/// its size was read): it is cut at `read_size`, the size the object reports.
/// Both replies are awaited for at most `reply_timeout`, so an unresponsive server
/// cannot hold the read.
async fn read_ftp_data(
    mut client: AsyncFtpStream,
    location: String,
    start: usize,
    read_size: usize,
    read_to_eof: bool,
    reply_timeout: Duration,
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
        .retr_as_stream(&location)
        .await
        .map_err(|e| generic_error(STORE_NAME, e))?;

    let mut result = Vec::with_capacity(read_size);
    let mut buf = vec![0; 4096];
    let mut total = 0;
    let mut reached_eof = false;

    loop {
        if total > read_size || (total == read_size && !read_to_eof) {
            break;
        }

        let n = stream
            .read(&mut buf)
            .await
            .map_err(|e| generic_error(STORE_NAME, e))?;

        if n == 0 {
            reached_eof = true;
            break;
        }

        let bytes_to_take = read_size.saturating_sub(total).min(n);
        result.extend_from_slice(&buf[..bytes_to_take]);
        total += n;
    }

    if reached_eof {
        match tokio::time::timeout(reply_timeout, stream.finish()).await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => return Err(generic_error(STORE_NAME, e)),
            Err(_) => {
                return Err(generic_error(
                    STORE_NAME,
                    format!(
                        "reading '{location}': the server did not confirm the download within {reply_timeout:?}"
                    ),
                ));
            }
        }
    } else {
        match tokio::time::timeout(reply_timeout, client.abort(stream)).await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                tracing::debug!(
                    "FTP: aborting the rest of the download of '{location}' failed: {e}"
                );
            }
            Err(_) => {
                tracing::debug!(
                    "FTP: the server did not acknowledge aborting the download of '{location}' within {reply_timeout:?}"
                );
            }
        }
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

        if options.head {
            let stream = futures::stream::empty();
            return Ok(GetResult {
                meta: object_meta,
                payload: GetResultPayload::Stream(Box::pin(stream)),
                range: 0..0,
                attributes: Attributes::default(),
            });
        }

        let (start, end, data_to_read) = resolve_range(options.range.as_ref(), size);

        #[expect(clippy::cast_possible_truncation)]
        let data = read_ftp_data(
            client,
            location_string,
            start as usize,
            data_to_read as usize,
            end == size,
            self.inner.config.timeout.unwrap_or(DEFAULT_CONNECT_TIMEOUT),
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

    fn delete_stream(
        &self,
        _locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
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

    async fn copy_opts(
        &self,
        _from: &Path,
        _to: &Path,
        _options: CopyOptions,
    ) -> object_store::Result<()> {
        Err(object_store::Error::NotSupported {
            source: "FTP copy_opts not implemented".into(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;
    use std::sync::{Arc, Mutex};

    use chrono::Utc;
    use object_store::{GetOptions, GetRange, GetResult, ObjectStore};

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

    #[tokio::test]
    async fn connect_deadline_expires_and_names_the_address() {
        // Stands in for a peer that accepts TCP and then never answers: the attempt
        // never resolves, so only the deadline can end it.
        let stalled = std::future::pending::<object_store::Result<()>>();

        let err = with_connect_deadline("ftp.example.com:21", Duration::from_millis(50), stalled)
            .await
            .expect_err("a connection attempt that never completes must not be waited on forever");

        let message = err.to_string();
        assert!(
            message.contains("ftp.example.com:21"),
            "the error should name the address that stalled, got: {message}"
        );
    }

    #[tokio::test]
    async fn connect_deadline_passes_a_completed_attempt_through() {
        let value = with_connect_deadline("ftp.example.com:21", Duration::from_secs(30), async {
            Ok(7u8)
        })
        .await
        .expect("an attempt that completes inside the bound should be returned as-is");

        assert_eq!(value, 7);
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

    /// One scripted FTP session on loopback, to drive `read_ftp_data` over real sockets.
    ///
    /// `RETR` sends `payload` from the `REST` offset, closes the data connection, then
    /// replies `completion` on the control connection. The handle yields every command
    /// the client sent, once the client disconnects.
    async fn scripted_ftp_session(
        payload: Vec<u8>,
        completion: &'static str,
    ) -> (String, tokio::task::JoinHandle<Vec<String>>) {
        use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
        use tokio::net::TcpListener;

        let control = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind the control listener");
        let port = control
            .local_addr()
            .expect("control listener address")
            .port()
            .to_string();
        let server = tokio::spawn(async move {
            let (socket, _) = control
                .accept()
                .await
                .expect("accept the control connection");
            let (read, mut write) = socket.into_split();
            let mut lines = BufReader::new(read).lines();
            let mut commands = Vec::new();
            let mut offset = 0_usize;
            let mut data_listener: Option<TcpListener> = None;
            write
                .write_all(b"220 ready\r\n")
                .await
                .expect("send the greeting");
            while let Ok(Some(line)) = lines.next_line().await {
                let verb = line
                    .split(' ')
                    .next()
                    .unwrap_or_default()
                    .to_ascii_uppercase();
                commands.push(verb.clone());
                let reply = match verb.as_str() {
                    "USER" => "331 password required".to_string(),
                    "PASS" => "230 logged in".to_string(),
                    "TYPE" => "200 type set".to_string(),
                    "REST" => {
                        offset = line[5..].trim().parse().expect("REST offset");
                        "350 restarting".to_string()
                    }
                    "PASV" => {
                        let listener = TcpListener::bind("127.0.0.1:0")
                            .await
                            .expect("bind the data listener");
                        let port = listener.local_addr().expect("data listener address").port();
                        data_listener = Some(listener);
                        format!(
                            "227 Entering Passive Mode (127,0,0,1,{},{})",
                            port >> 8,
                            port & 0xff
                        )
                    }
                    "RETR" => {
                        write.write_all(b"150 sending\r\n").await.expect("send 150");
                        let listener = data_listener.take().expect("PASV before RETR");
                        let (mut data, _) =
                            listener.accept().await.expect("accept the data connection");
                        data.write_all(&payload[offset.min(payload.len())..])
                            .await
                            .expect("send the file");
                        let _ = data.shutdown().await;
                        drop(data);
                        completion.to_string()
                    }
                    "ABOR" => "226 abort done".to_string(),
                    "QUIT" => "221 bye".to_string(),
                    _ => "502 not implemented".to_string(),
                };
                write
                    .write_all(format!("{reply}\r\n").as_bytes())
                    .await
                    .expect("send the reply");
            }
            commands
        });
        (port, server)
    }

    /// Read `read_size` bytes at `start` from a scripted session serving `payload`.
    async fn read_from_scripted_server(
        payload: Vec<u8>,
        completion: &'static str,
        start: usize,
        read_size: usize,
        read_to_eof: bool,
    ) -> (object_store::Result<Vec<u8>>, Vec<String>) {
        let (port, server) = scripted_ftp_session(payload, completion).await;
        let client = connect_and_login(
            "127.0.0.1",
            &port,
            "user",
            "pass",
            Some(Duration::from_secs(5)),
        )
        .await
        .expect("log in to the scripted server");
        let result = read_ftp_data(
            client,
            "file.parquet".to_string(),
            start,
            read_size,
            read_to_eof,
            Duration::from_secs(5),
        )
        .await;
        let commands = tokio::time::timeout(Duration::from_secs(5), server)
            .await
            .expect("the scripted session ends when the client disconnects")
            .expect("the scripted session does not panic");
        (result, commands)
    }

    fn file_bytes(len: usize) -> Vec<u8> {
        (0..len)
            .map(|i| u8::try_from(i % 251).expect("below 256"))
            .collect()
    }

    #[tokio::test]
    async fn full_file_read_checks_the_completion_reply_instead_of_aborting() {
        let file = file_bytes(10_000);
        let (result, commands) =
            read_from_scripted_server(file.clone(), "226 transfer complete", 0, 10_000, true).await;
        assert_eq!(result.expect("the full read succeeds"), file);
        // No `REST` from offset 0, and no `ABOR`: the completion reply is read instead.
        assert_eq!(commands, ["USER", "PASS", "TYPE", "PASV", "RETR"]);
    }

    #[tokio::test]
    async fn full_file_read_fails_when_the_server_reports_a_failed_transfer() {
        let (result, commands) = read_from_scripted_server(
            file_bytes(10_000),
            "451 local error in processing",
            0,
            10_000,
            true,
        )
        .await;
        let error = result.expect_err("a failed transfer must not return its bytes");
        assert!(
            error.to_string().contains("451"),
            "the error names the server's reply: {error}"
        );
        // No `REST` from offset 0, and no `ABOR`: the completion reply is read instead.
        assert_eq!(commands, ["USER", "PASS", "TYPE", "PASV", "RETR"]);
    }

    #[tokio::test]
    async fn empty_file_read_reaches_eof_without_aborting() {
        let (result, commands) =
            read_from_scripted_server(Vec::new(), "226 transfer complete", 0, 0, true).await;
        assert_eq!(result.expect("the empty read succeeds"), Vec::<u8>::new());
        // No `REST` from offset 0, and no `ABOR`: the completion reply is read instead.
        assert_eq!(commands, ["USER", "PASS", "TYPE", "PASV", "RETR"]);
    }

    #[tokio::test]
    async fn range_ending_before_eof_returns_its_bytes_and_aborts() {
        let file = file_bytes(10_000);
        let (result, commands) =
            read_from_scripted_server(file.clone(), "226 transfer complete", 100, 1_000, false)
                .await;
        assert_eq!(result.expect("the ranged read succeeds"), file[100..1_100]);
        assert_eq!(
            commands,
            ["USER", "PASS", "TYPE", "PASV", "REST", "RETR", "ABOR"]
        );
    }

    /// Same bound SMB uses for a buffered object read (`MAX_BUFFERED_READ`).
    const BUFFERED_READ_CAP: u64 = 2 * 1024 * 1024 * 1024;

    fn size_refusal(reported: u64) -> String {
        format!(
            "Generic FTP error: FTP read of {reported} bytes exceeds {BUFFERED_READ_CAP}-byte cap; reduce range or stream"
        )
    }

    /// Bytes the scripted peer actually sends. The `SIZE` reply is independent of this.
    fn payload() -> Vec<u8> {
        b"abcdefghij".to_vec()
    }

    struct SizeScript {
        pending: VecDeque<u64>,
        repeat: u64,
    }

    impl SizeScript {
        fn from_replies(replies: &[u64]) -> Self {
            let repeat = *replies.last().expect("at least one SIZE reply");
            Self {
                pending: VecDeque::from(replies.to_vec()),
                repeat,
            }
        }

        fn next(&mut self) -> u64 {
            self.pending.pop_front().unwrap_or(self.repeat)
        }
    }

    /// Control-connection commands, in the order each connection received them.
    type CommandLog = Arc<Mutex<Vec<String>>>;

    /// One cleartext FTP peer. Every `SIZE` answer comes from `replies`, and once
    /// that list is exhausted the last answer is repeated, so a second lookup
    /// cannot observe a smaller length than the script's last reply.
    ///
    /// `RETR` sends `payload` and closes the data connection. It does not send
    /// `reported` bytes.
    async fn scripted_peer(replies: Vec<u64>, file_payload: Vec<u8>) -> (String, CommandLog) {
        use tokio::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind the control listener");
        let port = listener
            .local_addr()
            .expect("control listener address")
            .port()
            .to_string();
        let commands = Arc::new(Mutex::new(Vec::new()));
        let sizes = Arc::new(Mutex::new(SizeScript::from_replies(&replies)));
        let file_payload = Arc::new(file_payload);
        let accept_log = Arc::clone(&commands);
        let accept_sizes = Arc::clone(&sizes);
        let accept_payload = Arc::clone(&file_payload);

        tokio::spawn(async move {
            loop {
                let Ok((socket, _)) = listener.accept().await else {
                    break;
                };
                let commands = Arc::clone(&accept_log);
                let sizes = Arc::clone(&accept_sizes);
                let file_payload = Arc::clone(&accept_payload);
                tokio::spawn(async move {
                    serve_control(socket, commands, sizes, file_payload).await;
                });
            }
        });

        (port, commands)
    }

    async fn serve_control(
        socket: tokio::net::TcpStream,
        commands: CommandLog,
        sizes: Arc<Mutex<SizeScript>>,
        file_payload: Arc<Vec<u8>>,
    ) {
        use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
        use tokio::net::TcpListener;

        let (read, mut write) = socket.into_split();
        let mut lines = BufReader::new(read).lines();
        let mut offset = 0_usize;
        let mut data_listener: Option<TcpListener> = None;
        write
            .write_all(b"220 ready\r\n")
            .await
            .expect("send the greeting");
        while let Ok(Some(line)) = lines.next_line().await {
            let verb = line
                .split(' ')
                .next()
                .unwrap_or_default()
                .to_ascii_uppercase();
            {
                let mut log = commands.lock().expect("command log");
                log.push(verb.clone());
            }
            let reply = match verb.as_str() {
                "USER" => "331 password required".to_string(),
                "PASS" => "230 logged in".to_string(),
                "NOOP" | "TYPE" => "200 ok".to_string(),
                "REST" => {
                    offset = line[5..].trim().parse().expect("REST offset");
                    "350 restarting".to_string()
                }
                "SIZE" => {
                    let reported = sizes.lock().expect("SIZE script").next();
                    format!("213 {reported}")
                }
                "MDTM" => "213 20200101120000".to_string(),
                "PASV" => {
                    let data = TcpListener::bind("127.0.0.1:0")
                        .await
                        .expect("bind the data listener");
                    let data_port = data.local_addr().expect("data listener address").port();
                    data_listener = Some(data);
                    format!(
                        "227 Entering Passive Mode (127,0,0,1,{},{})",
                        data_port >> 8,
                        data_port & 0xff
                    )
                }
                "RETR" => {
                    write.write_all(b"150 sending\r\n").await.expect("send 150");
                    let listener = data_listener.take().expect("PASV before RETR");
                    let (mut data, _) =
                        listener.accept().await.expect("accept the data connection");
                    let start = offset.min(file_payload.len());
                    data.write_all(&file_payload[start..])
                        .await
                        .expect("send the file");
                    let _ = data.shutdown().await;
                    drop(data);
                    "226 transfer complete".to_string()
                }
                "ABOR" => "226 abort done".to_string(),
                "QUIT" => "221 bye".to_string(),
                _ => "502 not implemented".to_string(),
            };
            write
                .write_all(format!("{reply}\r\n").as_bytes())
                .await
                .expect("send the reply");
        }
    }

    fn store_for(port: &str) -> FTPObjectStore {
        FTPObjectStore::new(
            "user".to_string(),
            "pass".to_string(),
            "127.0.0.1".to_string(),
            port.to_string(),
            Some(Duration::from_secs(5)),
        )
    }

    fn command_log(commands: &CommandLog) -> Vec<String> {
        commands.lock().expect("command log").clone()
    }

    /// Virtual size of this process, used only to show a refused read did not
    /// reserve the reported length. `None` where `/proc` is unavailable.
    fn virtual_size_bytes() -> Option<u64> {
        let status = std::fs::read_to_string("/proc/self/status").ok()?;
        for line in status.lines() {
            let Some(rest) = line.strip_prefix("VmSize:") else {
                continue;
            };
            let kib: u64 = rest.split_whitespace().next()?.parse().ok()?;
            return Some(kib.saturating_mul(1024));
        }
        None
    }

    async fn read_with(
        reported: &[u64],
        options: GetOptions,
    ) -> (object_store::Result<GetResult>, Vec<String>, Option<u64>) {
        let (port, commands) = scripted_peer(reported.to_vec(), payload()).await;
        let store = store_for(&port);
        let before = virtual_size_bytes();
        let result = tokio::time::timeout(
            Duration::from_secs(10),
            store.get_opts(&object_store::path::Path::from("file.bin"), options),
        )
        .await
        .expect("the read returns; it must not wait for the reported length to arrive");
        let after = virtual_size_bytes();
        let grown = match (before, after) {
            (Some(start), Some(end)) => Some(end.saturating_sub(start)),
            _ => None,
        };
        (result, command_log(&commands), grown)
    }

    fn refusal_failure(
        result: object_store::Result<GetResult>,
        commands: &[String],
        grown: Option<u64>,
        reported: u64,
    ) -> Option<String> {
        let Err(error) = result else {
            return Some(format!(
                "SIZE {reported} was reserved and the object was returned (virtual size grew by {grown:?} bytes; the peer sent {} bytes). Commands: {commands:?}",
                payload().len()
            ));
        };
        if error.to_string() != size_refusal(reported) {
            return Some(format!("expected {}, got {error}", size_refusal(reported)));
        }
        if commands
            .iter()
            .any(|command| command == "PASV" || command == "RETR")
        {
            return Some(format!(
                "the data connection was opened before the SIZE was refused, commands: {commands:?}"
            ));
        }
        if !commands.iter().any(|command| command == "SIZE") {
            return Some(format!(
                "the read never asked for SIZE, commands: {commands:?}"
            ));
        }
        None
    }

    #[tokio::test]
    async fn object_reads_reject_a_size_one_byte_over_the_buffered_read_cap() {
        let reported = BUFFERED_READ_CAP + 1;
        let cases = [
            ("full object", GetOptions::default()),
            (
                "offset to end of file",
                GetOptions::default().with_range(Some(GetRange::Offset(0))),
            ),
            (
                "suffix of the whole reported length",
                GetOptions::default().with_range(Some(GetRange::Suffix(u64::MAX))),
            ),
            (
                "bounded range of the reported length",
                GetOptions::default().with_range(Some(GetRange::Bounded(0..reported))),
            ),
            (
                "bounded range shorter than the cap",
                GetOptions::default().with_range(Some(GetRange::Bounded(0..4))),
            ),
        ];

        let mut failures = Vec::new();
        for (name, options) in cases {
            let (result, commands, grown) = read_with(&[reported], options).await;
            if let Some(failure) = refusal_failure(result, &commands, grown, reported) {
                failures.push(format!("{name}: {failure}"));
            }
        }
        assert!(
            failures.is_empty(),
            "a SIZE one byte over the buffered-read cap must be refused before the file is downloaded:\n{}",
            failures.join("\n")
        );
    }

    #[tokio::test]
    async fn read_inside_the_cap_returns_only_the_bytes_the_peer_sent() {
        let reported = u64::try_from(payload().len()).expect("payload length fits in u64");
        let (result, commands, _) = read_with(&[reported], GetOptions::default()).await;
        let body = result
            .expect("a SIZE inside the cap is readable")
            .bytes()
            .await
            .expect("the object body");
        assert_eq!(body.as_ref(), payload().as_slice());
        assert!(
            commands.iter().any(|command| command == "RETR"),
            "the file is read from the data connection, commands: {commands:?}"
        );
        assert_eq!(
            commands.iter().filter(|command| *command == "SIZE").count(),
            1,
            "the length that is reserved is the SIZE reply already read, commands: {commands:?}"
        );
    }

    #[tokio::test]
    async fn a_later_size_reply_cannot_choose_the_reserved_length() {
        let reported = u64::try_from(payload().len()).expect("payload length fits in u64");
        // The first reply is inside the cap. A second `SIZE`, if the read asks
        // again after checking the first, is past `isize::MAX` and cannot be
        // reserved. The read must keep the first reply.
        let (result, commands, _) = read_with(
            &[
                reported,
                u64::try_from(isize::MAX).expect("isize::MAX fits in u64") + 1,
            ],
            GetOptions::default(),
        )
        .await;
        let body = result
            .expect("the first SIZE is inside the cap, so the object is readable")
            .bytes()
            .await
            .expect("the object body");
        assert_eq!(body.as_ref(), payload().as_slice());
        assert_eq!(
            commands.iter().filter(|command| *command == "SIZE").count(),
            1,
            "the reserved length is the SIZE that was read, not a later reply, commands: {commands:?}"
        );
    }

    #[tokio::test]
    async fn size_past_isize_max_is_refused_instead_of_panicking() {
        // A `u8` buffer longer than `isize::MAX` makes `Vec::with_capacity` panic
        // ("capacity overflow") before the allocator runs. That SIZE must be
        // refused instead.
        let reported = u64::try_from(isize::MAX).expect("isize::MAX fits in u64") + 1;
        let (result, commands, grown) = read_with(&[reported], GetOptions::default()).await;
        if let Some(failure) = refusal_failure(result, &commands, grown, reported) {
            panic!("{failure}");
        }
    }
}
