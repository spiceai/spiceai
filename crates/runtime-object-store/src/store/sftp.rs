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

use std::{
    io::{Read, Seek, SeekFrom},
    net::{SocketAddr, TcpStream, ToSocketAddrs},
    sync::Arc,
    time::{Duration, Instant},
};

use async_trait::async_trait;
use bytes::Bytes;
use chrono::DateTime;
use futures::StreamExt;
use futures::stream::BoxStream;
use object_store::{
    Attributes, CopyOptions, GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload,
    ObjectMeta, ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult, path::Path,
};
use ssh2::Session;

use super::common::{
    DirEntry, build_byte_range, build_object_meta, generic_error, process_directory_entries,
    process_directory_entries_shallow, resolve_range,
};

const STORE_NAME: &str = "SFTP";
/// Deadline applied to establishing a session when `client_timeout` is unset.
/// Matches the documented default for the parameter.
const DEFAULT_CLIENT_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Clone)]
struct SFTPClientConfig {
    user: String,
    password: String,
    host: String,
    port: String,
    timeout: Option<Duration>,
}

impl std::fmt::Debug for SFTPClientConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SFTPClientConfig")
            .field("user", &self.user)
            .field("password", &"[REDACTED]")
            .field("host", &self.host)
            .field("port", &self.port)
            .field("timeout", &self.timeout)
            .finish()
    }
}

impl SFTPClientConfig {
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

    fn connect(&self) -> object_store::Result<Session> {
        let deadline = self.deadline();
        let addr = format!("{}:{}", self.host, self.port);
        let stream = connect_within(&addr, deadline)?;

        let mut session = Session::new().map_err(handle_error)?;
        // The SSH banner exchange and the password exchange are what a peer that
        // completes the TCP handshake and then goes quiet holds open, so bounding
        // only the connect leaves this blocking-pool thread parked forever. This
        // bounds every blocking libssh2 wait, including the handshake, the
        // authentication, and later SFTP reads on the returned session — libssh2
        // applies it per wait for the socket to become ready, so a transfer that
        // keeps making progress is not cut off.
        session.set_timeout(u32::try_from(deadline.as_millis()).unwrap_or(u32::MAX));
        session.set_tcp_stream(stream);
        session.handshake().map_err(handle_error)?;
        session
            .userauth_password(&self.user, &self.password)
            .map_err(handle_error)?;

        Ok(session)
    }
}

/// Resolve `host:port` and connect within `deadline`.
///
/// `TcpStream::connect_timeout` takes an already-resolved `SocketAddr` and so
/// cannot accept a hostname; resolving first keeps hostnames working while
/// still bounding the connect. The deadline covers all resolved candidates
/// together rather than resetting per candidate, so a host with several
/// addresses cannot multiply the wait the operator asked for — at the cost of
/// possibly not reaching a later address, which is why the error reports how
/// many were tried. Name resolution itself is bounded by the system resolver,
/// not by this deadline.
fn connect_within(addr: &str, deadline: Duration) -> object_store::Result<TcpStream> {
    let start = Instant::now();
    let candidates: Vec<SocketAddr> = addr
        .to_socket_addrs()
        .map_err(|e| {
            generic_error(
                STORE_NAME,
                format!("Failed to resolve SFTP server {addr}: {e}"),
            )
        })?
        .collect();
    let total = candidates.len();

    let mut tried = 0;
    let mut last_error: Option<String> = None;
    for candidate in candidates {
        // `connect_timeout` rejects a zero duration, and an exhausted deadline
        // is the answer anyway.
        let remaining = deadline.saturating_sub(start.elapsed());
        if remaining.is_zero() {
            break;
        }
        tried += 1;
        match TcpStream::connect_timeout(&candidate, remaining) {
            Ok(stream) => return Ok(stream),
            Err(e) => last_error = Some(format!("{candidate}: {e}")),
        }
    }

    let detail = last_error.unwrap_or_else(|| format!("no address answered within {deadline:?}"));
    Err(generic_error(
        STORE_NAME,
        format!(
            "Failed to connect to SFTP server sftp://{addr}: {detail} (tried {tried} of {total} resolved addresses within {deadline:?}). Increase 'client_timeout' if the server is simply slow to respond. See: https://spiceai.org/docs/components/data-connectors/sftp"
        ),
    ))
}

#[derive(Debug, Clone)]
pub struct SFTPObjectStore {
    config: Arc<SFTPClientConfig>,
}

impl std::fmt::Display for SFTPObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SFTP")
    }
}

impl SFTPObjectStore {
    #[must_use]
    pub fn new(
        user: String,
        password: String,
        host: String,
        port: String,
        timeout: Option<Duration>,
    ) -> Self {
        Self {
            config: Arc::new(SFTPClientConfig::new(user, password, host, port, timeout)),
        }
    }

    /// List a single directory and return its entries (blocking).
    fn list_directory_blocking(
        session: &Session,
        dir_path: &str,
    ) -> object_store::Result<Vec<DirEntry>> {
        let sftp = session.sftp().map_err(handle_error)?;
        let entries = sftp
            .readdir(std::path::Path::new(dir_path))
            .map_err(handle_error)?;

        let mut result = Vec::new();
        for (path, stat) in entries {
            let name = path
                .file_name()
                .and_then(|n| n.to_str())
                .map(ToString::to_string)
                .unwrap_or_default();

            if stat.is_dir() {
                result.push(DirEntry::directory(name));
            } else if stat.is_file() {
                let size = stat.size.unwrap_or(0);
                #[expect(clippy::cast_possible_wrap)]
                let last_modified = DateTime::from_timestamp(stat.mtime.unwrap_or(0) as i64, 0)
                    .unwrap_or_else(chrono::Utc::now);
                result.push(DirEntry::file(name, size, last_modified));
            }
        }
        Ok(result)
    }

    /// List all files recursively starting from a given path.
    async fn list_all_files(
        &self,
        prefix: Option<String>,
    ) -> object_store::Result<Vec<ObjectMeta>> {
        let config = Arc::clone(&self.config);
        let prefix = prefix.unwrap_or_else(|| "/".to_string());

        tokio::task::spawn_blocking(move || {
            let session = config.connect()?;
            let mut results = Vec::new();
            let mut queue = vec![prefix];

            while let Some(current_path) = queue.pop() {
                let entries = Self::list_directory_blocking(&session, &current_path)?;
                let (files, dirs) = process_directory_entries(&current_path, entries);
                results.extend(files);
                queue.extend(dirs);
            }

            Ok(results)
        })
        .await
        .map_err(|e| generic_error(STORE_NAME, e))?
    }

    /// List a single directory level (for `list_with_delimiter`).
    async fn list_directory_shallow(
        &self,
        prefix: Option<&Path>,
    ) -> object_store::Result<ListResult> {
        let config = Arc::clone(&self.config);
        let prefix_str = prefix.map_or("/".to_string(), |p| {
            let s = p.to_string();
            if s.is_empty() {
                "/".to_string()
            } else {
                format!("/{s}")
            }
        });

        tokio::task::spawn_blocking(move || {
            let session = config.connect()?;
            let entries = Self::list_directory_blocking(&session, &prefix_str)?;
            Ok(process_directory_entries_shallow(&prefix_str, entries))
        })
        .await
        .map_err(|e| generic_error(STORE_NAME, e))?
    }
}

fn handle_error<T: Into<Box<dyn std::error::Error + Sync + Send>>>(
    error: T,
) -> object_store::Error {
    generic_error(STORE_NAME, error)
}

#[async_trait]
impl ObjectStore for SFTPObjectStore {
    async fn put_opts(
        &self,
        _location: &Path,
        _payload: PutPayload,
        _opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        Err(object_store::Error::NotSupported {
            source: "SFTP put_opts not implemented".into(),
        })
    }

    async fn put_multipart_opts(
        &self,
        _location: &Path,
        _opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        Err(object_store::Error::NotSupported {
            source: "SFTP put_multipart_opts not implemented".into(),
        })
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        let config = Arc::clone(&self.config);
        let location = location.clone();

        // Perform all blocking operations in spawn_blocking, including reading the data
        let (object_meta, start, end, data) = tokio::task::spawn_blocking(move || {
            let session = config.connect()?;
            let location_string = format!("/{location}");
            let mut file = session
                .sftp()
                .map_err(handle_error)?
                .open(std::path::Path::new(&location_string))
                .map_err(handle_error)?;

            let file_stat = file.stat().map_err(handle_error)?;
            let size = file_stat.size.ok_or_else(|| object_store::Error::Generic {
                store: STORE_NAME,
                source: "No size found for file".into(),
            })?;

            #[expect(clippy::cast_possible_wrap)]
            let last_modified = DateTime::from_timestamp(
                file_stat
                    .mtime
                    .ok_or_else(|| object_store::Error::Generic {
                        store: STORE_NAME,
                        source: "No modification time found for file".into(),
                    })? as i64,
                0,
            )
            .ok_or_else(|| object_store::Error::Generic {
                store: STORE_NAME,
                source: "Failed to construct DateTime".into(),
            })?;

            let object_meta = build_object_meta(location.clone(), size, last_modified);

            if options.head {
                return Ok((object_meta, 0, 0, Vec::new()));
            }

            let (start, end, data_to_read) = resolve_range(options.range.as_ref(), size);

            // Seek to start position
            file.seek(SeekFrom::Start(start)).map_err(handle_error)?;

            // Read all requested data
            #[expect(clippy::cast_possible_truncation)]
            let mut buffer = vec![0u8; data_to_read as usize];
            let mut total_read = 0;
            while total_read < buffer.len() {
                let n = file.read(&mut buffer[total_read..]).map_err(handle_error)?;
                if n == 0 {
                    break;
                }
                total_read += n;
            }
            buffer.truncate(total_read);

            Ok::<_, object_store::Error>((object_meta, start, end, buffer))
        })
        .await
        .map_err(|e| generic_error(STORE_NAME, e))??;

        let stream = futures::stream::once(async move { Ok(Bytes::from(data)) });

        Ok(GetResult {
            payload: GetResultPayload::Stream(Box::pin(stream)),
            meta: object_meta,
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
                source: "SFTP delete_stream not implemented".into(),
            })
        })
        .boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        let store = self.clone();
        let prefix_str = prefix.map(|p| {
            let s = p.to_string();
            if s.is_empty() {
                "/".to_string()
            } else {
                format!("/{s}")
            }
        });

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
                source: "SFTP list_with_offset not implemented".into(),
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
            source: "SFTP copy_opts not implemented".into(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    #[test]
    fn test_sftp_object_store_display() {
        let store = SFTPObjectStore::new(
            "user".to_string(),
            "password".to_string(),
            "sftp.example.com".to_string(),
            "22".to_string(),
            None,
        );
        assert_eq!(format!("{store}"), "SFTP");
    }

    #[test]
    fn test_sftp_client_config_with_timeout() {
        let config = SFTPClientConfig::new(
            "user".to_string(),
            "pass".to_string(),
            "localhost".to_string(),
            "22".to_string(),
            Some(Duration::from_mins(1)),
        );
        assert_eq!(config.host, "localhost");
        assert!(config.timeout.is_some());
    }

    /// Bind a listener on an ephemeral loopback port.
    fn loopback_listener() -> (std::net::TcpListener, u16) {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind loopback");
        let port = listener.local_addr().expect("local addr").port();
        (listener, port)
    }

    /// A peer that completes the TCP handshake and then never sends the SSH
    /// banner. Returns the port; the accepted sockets are held by the thread.
    fn stalled_ssh_peer() -> u16 {
        let (listener, port) = loopback_listener();
        std::thread::spawn(move || {
            let mut accepted = Vec::new();
            while let Ok((stream, _)) = listener.accept() {
                accepted.push(stream);
            }
        });
        port
    }

    fn config_for(host: &str, port: String, timeout: Option<Duration>) -> SFTPClientConfig {
        SFTPClientConfig::new(
            "user".to_string(),
            "pass".to_string(),
            host.to_string(),
            port,
            timeout,
        )
    }

    /// Regression test for #12647: the SSH banner exchange and the password
    /// exchange must be bounded, not just the TCP connect. Without a session
    /// timeout this parks a blocking-pool thread for as long as the peer stays
    /// quiet, and the thread cannot be reclaimed.
    #[test]
    fn connect_gives_up_on_a_peer_that_never_sends_a_banner() {
        let port = stalled_ssh_peer();
        let config = config_for(
            "127.0.0.1",
            port.to_string(),
            Some(Duration::from_millis(250)),
        );

        let start = Instant::now();
        let err = config
            .connect()
            .map(|_| ())
            .expect_err("a peer that never answers must not produce a session");

        assert!(
            start.elapsed() < Duration::from_secs(10),
            "the handshake should be abandoned at the deadline, took {:?}",
            start.elapsed()
        );
        // libssh2 reports its own timeout, so assert on the outcome rather than
        // on wording it owns.
        assert!(
            format!("{err}").contains("SFTP"),
            "the error should name the store, got: {err}"
        );
    }

    /// Regression test for #12654: setting `client_timeout` used to route the
    /// connect through `TcpStream::connect_timeout`, which takes an
    /// already-resolved `SocketAddr`, so any host given as a name failed with
    /// "invalid socket address syntax" before a packet was sent.
    #[test]
    fn a_hostname_is_resolved_when_client_timeout_is_set() {
        let port = stalled_ssh_peer();
        let config = config_for(
            "localhost",
            port.to_string(),
            Some(Duration::from_millis(250)),
        );

        let err = config
            .connect()
            .map(|_| ())
            .expect_err("the stalled peer cannot complete a handshake");

        assert!(
            !format!("{err}").contains("invalid socket address syntax"),
            "a hostname must be resolved, not parsed as an address: {err}"
        );
    }

    /// Control: a refused port must surface as a connection failure quickly,
    /// rather than being masked by the deadline.
    #[test]
    fn a_refused_port_fails_without_waiting_for_the_deadline() {
        let (listener, port) = loopback_listener();
        drop(listener);

        let config = config_for("127.0.0.1", port.to_string(), Some(Duration::from_secs(30)));

        let start = Instant::now();
        let err = config
            .connect()
            .map(|_| ())
            .expect_err("nothing is listening");

        assert!(
            start.elapsed() < Duration::from_secs(5),
            "a refused connect should fail immediately, took {:?}",
            start.elapsed()
        );
        assert!(
            format!("{err}").contains("Failed to connect to SFTP server"),
            "a refused connect should name the server, got: {err}"
        );
    }

    #[test]
    fn an_unresolvable_host_is_reported_as_a_resolution_failure() {
        // `.invalid` is reserved by RFC 2606 and never resolves.
        let config = config_for(
            "no-such-host.invalid",
            "22".to_string(),
            Some(Duration::from_millis(250)),
        );

        let err = config
            .connect()
            .map(|_| ())
            .expect_err("the host cannot resolve");
        assert!(
            format!("{err}").contains("Failed to resolve SFTP server"),
            "got: {err}"
        );
    }

    #[test]
    fn an_unset_client_timeout_still_has_a_deadline() {
        assert_eq!(
            config_for("sftp.example.com", "22".to_string(), None).deadline(),
            DEFAULT_CLIENT_TIMEOUT
        );
        assert_eq!(
            config_for(
                "sftp.example.com",
                "22".to_string(),
                Some(Duration::from_secs(3))
            )
            .deadline(),
            Duration::from_secs(3)
        );
    }

    #[test]
    fn an_exhausted_deadline_stops_trying_candidates() {
        let port = stalled_ssh_peer();
        let err = connect_within(&format!("127.0.0.1:{port}"), Duration::ZERO)
            .expect_err("a zero deadline cannot connect");
        assert!(
            format!("{err}").contains("no address answered within"),
            "got: {err}"
        );
    }

    #[test]
    fn test_dir_entry_file_creation() {
        let ts = Utc::now();
        let entry = DirEntry::file("report.pdf".to_string(), 4096, ts);
        assert!(!entry.is_dir);
        assert_eq!(entry.size, 4096);
        assert_eq!(entry.name, "report.pdf");
    }

    #[test]
    fn test_generic_error_creation() {
        let err = generic_error(STORE_NAME, "test error");
        let err_str = format!("{err}");
        assert!(err_str.contains("SFTP"));
    }
}
