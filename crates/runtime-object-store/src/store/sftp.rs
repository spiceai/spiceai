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
    future::Future,
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

/// Wall-clock bound for one connection attempt — name resolution, TCP connect, SSH handshake
/// and password authentication together — when `client_timeout` is not configured.
///
/// `connect` runs inside `tokio::task::spawn_blocking`, and the blocking pool is shared
/// process-wide, so an attempt that never returns costs a thread every other blocking
/// caller could have used. Matches the FTP store's default so the two connectors give up
/// in the same window.
const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(20);

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

    /// Resolve this endpoint's name, charging the lookup against the connection bound.
    ///
    /// Kept out of [`Self::connect`] so that the blocking section is handed addresses rather
    /// than a name: resolution has no deadline of its own, so it can only be bounded from an
    /// async caller.
    async fn resolve(&self) -> object_store::Result<ResolvedEndpoint> {
        resolve_within(
            &self.host,
            &self.port,
            self.timeout.unwrap_or(DEFAULT_CONNECT_TIMEOUT),
        )
        .await
    }

    fn connect(&self, endpoint: &ResolvedEndpoint) -> object_store::Result<Session> {
        let started = Instant::now();
        // Whatever `resolve` spent, plus however long this attempt waited for a thread of the
        // shared blocking pool, has already come off the bound the caller asked for.
        let Some(bound) = endpoint.deadline.checked_duration_since(started) else {
            return Err(handle_error(format!(
                "connecting to {} timed out before the connection could be attempted",
                endpoint.addr
            )));
        };
        let stream = connect_within(&endpoint.addrs, &endpoint.addr, bound)?;

        let mut session = Session::new().map_err(handle_error)?;
        session.set_tcp_stream(stream);
        // `handshake` and `userauth_password` are blocking reads on the session, and a
        // session inherits no deadline from the stream it was given. Without this, a peer
        // that completes the TCP handshake and then sends no SSH version string is waited
        // on for as long as it keeps the socket open.
        //
        // ssh2 applies the timeout to each blocking operation rather than as one
        // cumulative deadline, so every stage is armed with what is left of `bound` — the
        // connect that already happened, and then the handshake — instead of a fresh copy
        // of it. That is what keeps the attempt inside the single wall-clock bound
        // `DEFAULT_CONNECT_TIMEOUT` and `client_timeout` describe.
        session.set_timeout(remaining_session_timeout_ms(bound, started.elapsed())?);
        session.handshake().map_err(handle_error)?;
        session.set_timeout(remaining_session_timeout_ms(bound, started.elapsed())?);
        session
            .userauth_password(&self.user, &self.password)
            .map_err(handle_error)?;

        Ok(session)
    }
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
        let endpoint = self.config.resolve().await?;
        let config = Arc::clone(&self.config);
        let prefix = prefix.unwrap_or_else(|| "/".to_string());

        tokio::task::spawn_blocking(move || {
            let session = config.connect(&endpoint)?;
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
        let endpoint = self.config.resolve().await?;
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
            let session = config.connect(&endpoint)?;
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

/// A resolved endpoint and the deadline the rest of its connection attempt has left.
///
/// Produced in the async caller and handed to the blocking section, so that one wall-clock
/// bound covers resolution and connection together rather than starting afresh once the
/// addresses are known.
#[derive(Debug)]
struct ResolvedEndpoint {
    /// The configured `host:port`, for diagnostics — an address list does not name it.
    addr: String,
    addrs: Vec<SocketAddr>,
    /// When the attempt as a whole runs out of time, counted from before resolution began.
    deadline: Instant,
}

/// Apply `bound` to `attempt`, reporting expiry against `addr`.
///
/// The FTP store's `with_connect_deadline` is the same shape over its whole connect — there,
/// resolution is already inside the bounded future, so the two are not one helper with two
/// callers: this one exists because only the lookup can be bounded from outside the blocking
/// section, and it names resolution in the error it raises.
async fn with_resolve_deadline<T>(
    addr: &str,
    bound: Duration,
    attempt: impl Future<Output = object_store::Result<T>>,
) -> object_store::Result<T> {
    match tokio::time::timeout(bound, attempt).await {
        Ok(result) => result,
        Err(_) => Err(handle_error(format!(
            "resolving {addr} timed out after {bound:?}"
        ))),
    }
}

/// Resolve `host:port` for [`TcpStream::connect_timeout`], which unlike
/// [`TcpStream::connect`] takes an already-resolved address.
///
/// Parsing the text as a [`SocketAddr`] instead accepts only a literal IP, so it
/// rejected every named host — which made configuring `client_timeout` fail the
/// connection outright rather than bound it.
fn resolve_addrs(host: &str, port: &str) -> object_store::Result<Vec<SocketAddr>> {
    let addr = format!("{host}:{port}");
    let candidates: Vec<SocketAddr> = addr.to_socket_addrs().map_err(handle_error)?.collect();

    if candidates.is_empty() {
        return Err(handle_error(format!("{addr} resolved to no addresses")));
    }

    Ok(candidates)
}

/// Resolve `host:port` within `bound`, returning the addresses and the deadline left for the
/// connection itself.
///
/// [`resolve_addrs`] is the blocking libc resolver and accepts no deadline, so it is run as its
/// own task and awaited under a timeout. That bounds *this caller*: a lookup that never returns
/// still holds the blocking-pool thread it was given until libc gives up, which no timeout here
/// can shorten. What it buys is that the wall clock `client_timeout` describes now covers
/// resolution, so a stalled lookup can no longer push the attempt past it.
async fn resolve_within(
    host: &str,
    port: &str,
    bound: Duration,
) -> object_store::Result<ResolvedEndpoint> {
    let addr = format!("{host}:{port}");
    if bound.is_zero() {
        return Err(handle_error(format!(
            "connecting to {addr} was given no time to resolve"
        )));
    }

    let started = Instant::now();
    // The deadline is fixed before the lookup so that resolution is charged against the bound
    // exactly once: taking the remainder off `started` afterwards would deduct it twice.
    let Some(deadline) = started.checked_add(bound) else {
        return Err(handle_error(format!(
            "connecting to {addr} within {bound:?} is further ahead than time can be measured"
        )));
    };

    let (lookup_host, lookup_port) = (host.to_string(), port.to_string());
    let lookup = async move {
        tokio::task::spawn_blocking(move || resolve_addrs(&lookup_host, &lookup_port))
            .await
            .map_err(|error| generic_error(STORE_NAME, error))?
    };

    let addrs = with_resolve_deadline(&addr, bound, lookup).await?;

    // A lookup can return just as the budget expires, which leaves nothing to connect within.
    // Reporting that beats handing the blocking section a deadline it has already passed.
    if Instant::now() >= deadline {
        return Err(handle_error(format!(
            "resolving {addr} took the whole {bound:?} bound, leaving none to connect within"
        )));
    }

    Ok(ResolvedEndpoint {
        addr,
        addrs,
        deadline,
    })
}

/// Open a TCP connection to one of `addrs` within `bound`, reporting failure against `addr`.
///
/// A name can resolve to several addresses — commonly an IPv6 and an IPv4 form of the
/// same host — and only some of them may be listening, so each is tried in turn. They
/// share one budget rather than getting `bound` each, which is what keeps the whole
/// attempt inside the wall-clock the caller asked for.
fn connect_within(
    addrs: &[SocketAddr],
    addr: &str,
    bound: Duration,
) -> object_store::Result<TcpStream> {
    let started = Instant::now();
    let mut last_error = None;

    for candidate in addrs {
        let Some(remaining) = bound
            .checked_sub(started.elapsed())
            .filter(|remaining| !remaining.is_zero())
        else {
            break;
        };

        match TcpStream::connect_timeout(candidate, remaining) {
            Ok(stream) => return Ok(stream),
            Err(error) => last_error = Some(error),
        }
    }

    match last_error {
        Some(error) => Err(handle_error(format!(
            "connecting to {addr} failed: {error}"
        ))),
        None => Err(handle_error(format!(
            "connecting to {addr} timed out after {bound:?}"
        ))),
    }
}

/// Express `bound` as the millisecond count [`Session::set_timeout`] takes.
///
/// ssh2 reads `0` as "wait forever", so a sub-millisecond bound is raised to 1ms rather
/// than being passed through as the one value that means unbounded, and a bound past
/// `u32::MAX` milliseconds saturates instead of wrapping.
fn session_timeout_ms(bound: Duration) -> u32 {
    u32::try_from(bound.as_millis()).unwrap_or(u32::MAX).max(1)
}

/// Express what is left of `bound` after `elapsed` in the form [`Session::set_timeout`] takes.
///
/// A budget already spent is an error rather than a timeout of zero: ssh2 reads `0` as "wait
/// forever", so clamping an overrun up to 1ms would keep the attempt bounded while clamping
/// it down to 0 would remove the bound at exactly the point the attempt has run out of time.
fn remaining_session_timeout_ms(bound: Duration, elapsed: Duration) -> object_store::Result<u32> {
    let Some(remaining) = bound
        .checked_sub(elapsed)
        .filter(|remaining| !remaining.is_zero())
    else {
        return Err(handle_error(format!(
            "connecting timed out after {bound:?}: no budget left for the SSH handshake"
        )));
    };

    Ok(session_timeout_ms(remaining))
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
        let endpoint = self.config.resolve().await?;
        let config = Arc::clone(&self.config);
        let location = location.clone();

        // Perform all blocking operations in spawn_blocking, including reading the data
        let (object_meta, start, end, data) = tokio::task::spawn_blocking(move || {
            let session = config.connect(&endpoint)?;
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
    use std::net::TcpListener;
    use std::sync::mpsc;
    use std::thread;

    #[test]
    fn resolve_addrs_accepts_a_named_host() {
        // A named host is the ordinary case, and the address form this feeds
        // `connect_timeout` cannot be reached by parsing the text as a `SocketAddr`.
        let addrs = resolve_addrs("localhost", "22").expect("localhost should resolve");

        assert!(!addrs.is_empty());
        for addr in addrs {
            assert_eq!(addr.port(), 22);
            assert!(addr.ip().is_loopback(), "got {addr}");
        }
    }

    #[test]
    fn resolve_addrs_accepts_a_literal_ip() {
        let addrs = resolve_addrs("127.0.0.1", "2222").expect("a literal IP should resolve");

        assert_eq!(
            addrs.iter().map(ToString::to_string).collect::<Vec<_>>(),
            vec!["127.0.0.1:2222".to_string()]
        );
    }

    #[test]
    fn session_timeout_is_never_handed_to_ssh2_as_unbounded() {
        // ssh2 reads 0 as "wait forever", so rounding a short bound down to it would turn
        // the tightest request into the absence of a deadline.
        assert_eq!(session_timeout_ms(Duration::ZERO), 1);
        assert_eq!(session_timeout_ms(Duration::from_micros(500)), 1);
        assert_eq!(session_timeout_ms(Duration::from_secs(20)), 20_000);
        assert_eq!(
            session_timeout_ms(Duration::from_secs(u64::from(u32::MAX))),
            u32::MAX
        );
    }

    #[test]
    fn each_stage_is_armed_with_what_is_left_of_the_bound() {
        // ssh2 times out each blocking operation separately, so arming a stage with the
        // whole bound after part of it has been spent makes the attempt additive — the
        // wall clock the caller asked for, once per stage — rather than one deadline.
        let bound = Duration::from_secs(20);

        assert_eq!(
            remaining_session_timeout_ms(bound, Duration::ZERO)
                .expect("an unspent budget should arm the full bound"),
            20_000
        );
        assert_eq!(
            remaining_session_timeout_ms(bound, Duration::from_secs(15))
                .expect("a partly spent budget should arm only the remainder"),
            5_000
        );
    }

    #[test]
    fn a_spent_budget_fails_rather_than_arming_an_unbounded_wait() {
        let bound = Duration::from_secs(20);

        for elapsed in [bound, Duration::from_secs(21)] {
            let error = remaining_session_timeout_ms(bound, elapsed)
                .expect_err("a budget with nothing left must not arm another wait");

            assert!(
                error.to_string().contains("no budget left"),
                "the error should name the exhausted budget, got {error}"
            );
        }

        // A remainder under a millisecond still has time left, so it is armed as the
        // shortest deadline ssh2 accepts instead of `0`, which it reads as no deadline.
        let nearly_spent = bound
            .checked_sub(Duration::from_micros(500))
            .expect("the bound is longer than the amount taken off it");

        assert_eq!(
            remaining_session_timeout_ms(bound, nearly_spent)
                .expect("a sub-millisecond remainder is still a remainder"),
            1
        );
    }

    #[tokio::test]
    async fn connect_within_reaches_a_listener_through_its_name() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("a loopback listener should bind");
        let port = listener
            .local_addr()
            .expect("the listener should report its address")
            .port();

        let endpoint = resolve_within("localhost", &port.to_string(), Duration::from_secs(5))
            .await
            .expect("localhost should resolve");

        connect_within(&endpoint.addrs, &endpoint.addr, Duration::from_secs(5))
            .expect("a named host that resolves to the listening address should connect");
    }

    #[tokio::test]
    async fn resolving_leaves_the_rest_of_the_bound_to_connect_within() {
        // The point of resolving in the caller is that the lookup is paid for out of the same
        // budget: the deadline handed on has to be inside the bound the attempt started with,
        // not a fresh copy of it taken once the addresses were known.
        let bound = Duration::from_secs(5);
        let before = Instant::now();

        let endpoint = resolve_within("localhost", "22", bound)
            .await
            .expect("localhost should resolve");
        let after = Instant::now();

        assert_eq!(endpoint.addr, "localhost:22");
        assert!(!endpoint.addrs.is_empty());

        let left_to_connect = endpoint
            .deadline
            .checked_duration_since(after)
            .expect("a lookup that resolved promptly must leave time to connect");
        assert!(
            left_to_connect < bound,
            "resolution has to be charged against the bound rather than granted on top of it, \
             but {left_to_connect:?} of {bound:?} was left afterwards"
        );

        let whole_bound_from_the_start = before
            .checked_add(bound)
            .expect("five seconds ahead is measurable");
        assert!(
            endpoint.deadline >= whole_bound_from_the_start,
            "the deadline is the bound counted once from the start of the attempt, so charging \
             resolution must not also shorten it"
        );
    }

    #[tokio::test]
    async fn a_bound_of_zero_is_reported_rather_than_resolved_under() {
        // `client_timeout: 0s` is configurable, and a zero bound is the one value that cannot
        // bound anything — the lookup would be started only to be abandoned on the first poll.
        let error = resolve_within("localhost", "22", Duration::ZERO)
            .await
            .expect_err("a bound of zero leaves no time to resolve in");

        assert!(
            error.to_string().contains("no time to resolve"),
            "the error should say the bound left no room, got {error}"
        );
    }

    #[tokio::test]
    async fn a_lookup_that_outlives_the_bound_is_abandoned_by_its_caller() {
        // The libc resolver takes no deadline of its own, so the guarantee is that the caller
        // stops waiting — asserted here against a lookup that never completes at all.
        let stalled = std::future::pending::<object_store::Result<Vec<SocketAddr>>>();

        let error =
            with_resolve_deadline("sftp.example.com:22", Duration::from_millis(50), stalled)
                .await
                .expect_err("a lookup that never returns must not be waited on forever");

        let message = error.to_string();
        assert!(
            message.contains("sftp.example.com:22") && message.contains("timed out"),
            "the error should name the address it gave up on, got {message}"
        );
    }

    #[tokio::test]
    async fn a_deadline_already_passed_is_not_connected_against() {
        // A resolved endpoint can wait for a thread of the shared blocking pool, so the
        // connection has to re-check the deadline rather than assume the remainder resolution
        // measured is still available.
        let config = SFTPClientConfig::new(
            "user".to_string(),
            "password".to_string(),
            "127.0.0.1".to_string(),
            "22".to_string(),
            None,
        );
        let endpoint = ResolvedEndpoint {
            addr: "127.0.0.1:22".to_string(),
            addrs: vec![([127, 0, 0, 1], 22).into()],
            deadline: Instant::now()
                .checked_sub(Duration::from_secs(1))
                .expect("the process has been running for over a second"),
        };

        // `ssh2::Session` is not `Debug`, so the success arm cannot be unwrapped into a panic
        // message; naming it here keeps the failure legible.
        let error = config
            .connect(&endpoint)
            .err()
            .map(|error| error.to_string())
            .expect("an expired deadline must not start a connection");

        assert!(
            error.contains("timed out before"),
            "the error should say the budget was spent before connecting, got {error}"
        );
    }

    #[tokio::test]
    async fn connect_reports_a_peer_that_never_sends_an_ssh_banner() {
        // Bound but never accepted: the kernel completes the TCP handshake from the
        // backlog while nothing ever writes an SSH version string, which is the shape
        // that used to hold a blocking-pool thread for as long as the peer allowed.
        let listener = TcpListener::bind("127.0.0.1:0").expect("a loopback listener should bind");
        let addr = listener
            .local_addr()
            .expect("the listener should report its address");

        let config = SFTPClientConfig::new(
            "user".to_string(),
            "password".to_string(),
            addr.ip().to_string(),
            addr.port().to_string(),
            Some(Duration::from_millis(500)),
        );
        let endpoint = config
            .resolve()
            .await
            .expect("a literal loopback address should resolve");

        // The attempt is blocking, so it is run off-thread: a missing deadline has to
        // surface as a failed assertion here rather than as a test that never returns.
        let (tx, rx) = mpsc::channel();
        thread::spawn(move || {
            let _ = tx.send(
                config
                    .connect(&endpoint)
                    .err()
                    .map(|error| error.to_string()),
            );
        });

        let outcome = rx
            .recv_timeout(Duration::from_secs(30))
            .expect("the handshake must observe a deadline rather than wait on a silent peer");

        assert!(
            outcome.is_some(),
            "a peer that sends no SSH banner must not yield a session"
        );

        drop(listener);
    }

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
