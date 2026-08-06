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
use object_store::{
    Attributes, CopyOptions, ListResult, MultipartUpload, PutMultipartOptions, PutPayload,
};
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
/// Deadline applied to establishing a session when `client_timeout` is unset.
/// Matches the documented default for the parameter, and sits inside `bb8`'s
/// own 30s `connection_timeout` so a stuck attempt cannot outlive the pool's
/// willingness to wait for it.
const DEFAULT_CLIENT_TIMEOUT: Duration = Duration::from_secs(30);

/// Connection manager for bb8 connection pool.
#[derive(Clone, Debug)]
struct FTPConnectionManager {
    config: Arc<FTPClientConfig>,
}

/// Surfaces the pool's own connection failures, which it reports to a sink
/// rather than to a caller. `bb8`'s default sink discards them, so a connection
/// discarded in the background — including one abandoned at its deadline — would
/// otherwise leave no trace of why.
#[derive(Debug, Clone, Copy)]
struct LogErrorSink;

impl bb8::ErrorSink<object_store::Error> for LogErrorSink {
    fn sink(&self, error: object_store::Error) {
        tracing::warn!("FTP connection pool discarded a connection: {error}");
    }

    fn boxed_clone(&self) -> Box<dyn bb8::ErrorSink<object_store::Error>> {
        Box::new(*self)
    }
}

impl bb8::ManageConnection for FTPConnectionManager {
    type Connection = AsyncFtpStream;
    type Error = object_store::Error;

    fn connect(&self) -> impl Future<Output = Result<Self::Connection, Self::Error>> + Send {
        let config = Arc::clone(&self.config);
        Box::pin(async move { config.connect_and_login().await })
    }

    fn is_valid(
        &self,
        conn: &mut Self::Connection,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        let config = Arc::clone(&self.config);
        let deadline = config.deadline();
        let noop_future = conn.noop();
        Box::pin(async move {
            match tokio::time::timeout(deadline, noop_future).await {
                Ok(result) => result.map_err(|e| generic_error(STORE_NAME, e)),
                // Reporting the connection invalid has the pool discard it. Waiting
                // instead would hand the same unresponsive connection back on every
                // checkout, so a peer that goes quiet mid-session would keep the
                // pool unusable after the server recovered.
                Err(_elapsed) => Err(object_store::Error::Generic {
                    store: STORE_NAME,
                    source: format!(
                        "FTP server ftp://{}:{} did not answer a liveness check within {deadline:?}; discarding the connection.",
                        config.host, config.port
                    )
                    .into(),
                }),
            }
        })
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

    /// Wall-clock deadline for establishing one usable session.
    fn deadline(&self) -> Duration {
        self.timeout.unwrap_or(DEFAULT_CLIENT_TIMEOUT)
    }

    /// Connect and log in, as one operation under a single deadline.
    ///
    /// The login is what a peer that completes the TCP handshake and then goes
    /// quiet holds open, so bounding only the connect leaves the attempt able to
    /// hang forever: the pool slot it occupies is never released and later
    /// `Pool::get` calls stay suppressed even once the server recovers. One
    /// deadline over both stages also stops them from each spending the budget.
    async fn connect_and_login(&self) -> object_store::Result<AsyncFtpStream> {
        let deadline = self.deadline();
        let addr = format!("{}:{}", self.host, self.port);

        // `AsyncFtpStream::connect` resolves `host:port`, so a hostname works
        // here; `connect_timeout` takes an already-resolved `SocketAddr` and
        // rejects one.
        let established = tokio::time::timeout(deadline, async {
            let mut client = AsyncFtpStream::connect(addr.as_str())
                .await
                .map_err(|e| generic_error(STORE_NAME, e))?;

            client
                .login(&self.user, &self.password)
                .await
                .map_err(|e| generic_error(STORE_NAME, e))?;

            Ok(client)
        })
        .await;

        match established {
            Ok(result) => result,
            Err(_elapsed) => Err(object_store::Error::Generic {
                store: STORE_NAME,
                source: format!(
                    "Failed to connect to FTP server ftp://{addr}: the connection and login did not complete within {deadline:?}. The server may be unreachable, or accepting connections without answering. Increase 'client_timeout' if the server is simply slow to respond. See: https://spiceai.org/docs/components/data-connectors/ftp"
                )
                .into(),
            }),
        }
    }

    /// Create a fresh non-pooled connection for operations that modify connection state.
    async fn get_fresh_client(&self) -> object_store::Result<AsyncFtpStream> {
        self.connect_and_login().await
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
                let manager = FTPConnectionManager {
                    config: Arc::clone(&self.config),
                };
                Pool::builder()
                    .max_size(DEFAULT_POOL_SIZE)
                    .error_sink(Box::new(LogErrorSink))
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
    use bb8::ManageConnection;
    use chrono::Utc;
    use std::time::Instant;

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

    /// Bind a listener on an ephemeral loopback port.
    fn loopback_listener() -> (std::net::TcpListener, u16) {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind loopback");
        let port = listener.local_addr().expect("local addr").port();
        (listener, port)
    }

    /// A peer that completes the TCP handshake and then never answers, so the
    /// FTP greeting and the `USER`/`PASS` exchange stall. Returns the port.
    ///
    /// The accept loop runs on a detached OS thread rather than via
    /// `spawn_blocking`, because a blocking task that never returns also blocks
    /// the test runtime's shutdown.
    fn stalled_ftp_peer() -> u16 {
        let (listener, port) = loopback_listener();
        std::thread::spawn(move || {
            // Hold every accepted connection open without writing a byte.
            let mut accepted = Vec::new();
            while let Ok((stream, _)) = listener.accept() {
                accepted.push(stream);
            }
        });
        port
    }

    /// A peer that answers the greeting and the `USER`/`PASS` exchange, and then
    /// never answers another command. Returns the port.
    fn logged_in_then_quiet_peer() -> u16 {
        use std::io::{BufRead, BufReader, Write};

        let (listener, port) = loopback_listener();
        std::thread::spawn(move || {
            let mut held = Vec::new();
            while let Ok((stream, _)) = listener.accept() {
                let mut writer = stream.try_clone().expect("clone stream");
                let mut reader = BufReader::new(stream);
                // The greeting, then one reply per credential line.
                let exchange = [
                    "220 ready\r\n",
                    "331 need password\r\n",
                    "230 logged in\r\n",
                ];
                let mut sent = 0;
                while sent < exchange.len() {
                    if writer.write_all(exchange[sent].as_bytes()).is_err() {
                        break;
                    }
                    let _flushed = writer.flush();
                    sent += 1;
                    if sent < exchange.len() {
                        let mut line = String::new();
                        if reader.read_line(&mut line).unwrap_or(0) == 0 {
                            break;
                        }
                    }
                }
                // Hold the session open without answering anything further.
                held.push(reader.into_inner());
            }
        });
        port
    }

    fn config_for(host: &str, port: String, timeout: Option<Duration>) -> FTPClientConfig {
        FTPClientConfig::new(
            "user".to_string(),
            "pass".to_string(),
            host.to_string(),
            port,
            timeout,
        )
    }

    /// Regression test for #12647: a stalled login must not hang forever.
    #[tokio::test]
    async fn fresh_client_gives_up_on_a_peer_that_never_answers() {
        let port = stalled_ftp_peer();
        let config = config_for(
            "127.0.0.1",
            port.to_string(),
            Some(Duration::from_millis(250)),
        );

        let start = Instant::now();
        let err = config
            .get_fresh_client()
            .await
            .map(|_| ())
            .expect_err("a peer that never answers must not produce a session");

        assert!(
            start.elapsed() < Duration::from_secs(5),
            "connect+login should be abandoned at the deadline, took {:?}",
            start.elapsed()
        );
        let message = err.to_string();
        assert!(
            message.contains("did not complete within"),
            "error should name the deadline, got: {message}"
        );
    }

    /// The pooled path shares the same deadline as the non-pooled one, so a
    /// stalled attempt cannot occupy a pool slot indefinitely.
    #[tokio::test]
    async fn pool_manager_gives_up_on_a_peer_that_never_answers() {
        let port = stalled_ftp_peer();
        let manager = FTPConnectionManager {
            config: Arc::new(config_for(
                "127.0.0.1",
                port.to_string(),
                Some(Duration::from_millis(250)),
            )),
        };

        let start = Instant::now();
        let err = manager
            .connect()
            .await
            .map(|_| ())
            .expect_err("a peer that never answers must not produce a session");

        assert!(
            start.elapsed() < Duration::from_secs(5),
            "pooled connect should be abandoned at the deadline, took {:?}",
            start.elapsed()
        );
        assert!(
            err.to_string().contains("did not complete within"),
            "error should name the deadline, got: {err}"
        );
    }

    /// Regression test for #12654: setting `client_timeout` used to route the
    /// connect through `connect_timeout`, which takes an already-resolved
    /// `SocketAddr`, so any host given as a name failed with "invalid socket
    /// address syntax" before a packet was sent.
    #[tokio::test]
    async fn a_hostname_is_resolved_when_client_timeout_is_set() {
        let port = stalled_ftp_peer();
        let config = config_for(
            "localhost",
            port.to_string(),
            Some(Duration::from_millis(250)),
        );

        let err = config
            .get_fresh_client()
            .await
            .map(|_| ())
            .expect_err("the stalled peer cannot complete a login");

        let message = err.to_string();
        assert!(
            !message.contains("invalid socket address syntax"),
            "a hostname must be resolved, not parsed as an address: {message}"
        );
        assert!(
            message.contains("did not complete within"),
            "the connect should reach the peer and then hit the deadline, got: {message}"
        );
    }

    /// Control: a refused port must surface as a connection failure quickly,
    /// rather than being masked by the deadline.
    #[tokio::test]
    async fn a_refused_port_fails_without_waiting_for_the_deadline() {
        // Bind and drop, so the port is almost certainly unused.
        let (listener, port) = loopback_listener();
        drop(listener);

        let config = config_for("127.0.0.1", port.to_string(), Some(Duration::from_secs(30)));

        let start = Instant::now();
        let err = config
            .get_fresh_client()
            .await
            .map(|_| ())
            .expect_err("nothing is listening");

        assert!(
            start.elapsed() < Duration::from_secs(5),
            "a refused connect should fail immediately, took {:?}",
            start.elapsed()
        );
        assert!(
            !err.to_string().contains("did not complete within"),
            "a refused connect is not a timeout: {err}"
        );
    }

    /// The deadline must not cost the happy path: a server that answers still
    /// yields a logged-in session.
    #[tokio::test]
    async fn connect_and_login_succeeds_against_a_server_that_answers() {
        let port = logged_in_then_quiet_peer();
        let config = config_for("127.0.0.1", port.to_string(), Some(Duration::from_secs(10)));

        let client = config.connect_and_login().await;
        assert!(
            client.is_ok(),
            "a server that completes the login should yield a session"
        );
    }

    /// A connection that stops answering after login must be discarded rather
    /// than handed back on every checkout, which would leave the pool unusable
    /// even after the server recovered.
    #[tokio::test]
    async fn a_liveness_check_against_a_quiet_server_invalidates_the_connection() {
        let port = logged_in_then_quiet_peer();
        let manager = FTPConnectionManager {
            config: Arc::new(config_for(
                "127.0.0.1",
                port.to_string(),
                Some(Duration::from_millis(250)),
            )),
        };
        let mut conn = manager.connect().await.expect("the login is answered");

        let start = Instant::now();
        let err = manager
            .is_valid(&mut conn)
            .await
            .expect_err("a quiet server cannot answer a liveness check");

        assert!(
            start.elapsed() < Duration::from_secs(5),
            "the liveness check should be abandoned at the deadline, took {:?}",
            start.elapsed()
        );
        assert!(err.to_string().contains("liveness check"), "got: {err}");
    }

    #[test]
    fn an_unset_client_timeout_still_has_a_deadline() {
        assert_eq!(
            config_for("ftp.example.com", "21".to_string(), None).deadline(),
            DEFAULT_CLIENT_TIMEOUT
        );
        assert_eq!(
            config_for(
                "ftp.example.com",
                "21".to_string(),
                Some(Duration::from_secs(3))
            )
            .deadline(),
            Duration::from_secs(3)
        );
    }

    #[test]
    fn the_pool_manager_debug_output_redacts_the_password() {
        let manager = FTPConnectionManager {
            config: Arc::new(FTPClientConfig::new(
                "user".to_string(),
                "s3cr3t-value".to_string(),
                "ftp.example.com".to_string(),
                "21".to_string(),
                None,
            )),
        };
        let rendered = format!("{manager:?}");
        assert!(rendered.contains("[REDACTED]"), "got: {rendered}");
        assert!(!rendered.contains("s3cr3t-value"), "got: {rendered}");
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
