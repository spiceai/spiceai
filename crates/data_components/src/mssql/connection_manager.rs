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

use std::time::Duration;

use async_trait::async_trait;
use bb8::{ErrorSink, Pool};
use snafu::ResultExt;
use tiberius::{
    Client, Config,
    error::{Error as TiberiusError, IoErrorKind},
};
use tokio::net::TcpStream;
use tokio::time::{Instant, timeout_at};

use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};

use super::SqlServerAccessSnafu;

pub type SqlServerConnectionPool = Pool<SqlServerConnectionManager>;

/// How many read-only routing redirects to follow before reporting the chain as broken.
///
/// A SQL Server availability group answers a read-intent login with a routing
/// `ENVCHANGE`, which tiberius surfaces as [`TiberiusError::Routing`]. The login
/// itself succeeded — the server is naming the replica the session belongs on — so a
/// connection only exists once the client re-dials that address. One hop is what a
/// well-formed routing list needs, and bounding the chain keeps a list that routes
/// back to the listener from redirecting for as long as the pool will wait.
const MAX_ROUTING_REDIRECTS: usize = 3;

/// How long one `connect` may take, whatever the peer does.
///
/// `bb8` consults its own `connection_timeout` only after
/// [`bb8::ManageConnection::connect`] returns, so an attempt that never returns is
/// never abandoned: the pending pool slot stays taken, later `Pool::get` calls are
/// suppressed even once the server recovers, and timing out `Pool::get` does not
/// cancel the replenishment task that is stuck. A peer that completes the TCP
/// handshake and then stops answering is enough to reach that state — the OS-level
/// connect timeout no longer applies once the connection has been accepted.
///
/// The bound covers the whole sequence — DNS, TCP, TLS, login, and every routing
/// redirect — so a redirect chain cannot multiply it, and it sits inside `bb8`'s 30s
/// default `connection_timeout` so an attempt fails within the window the pool is
/// already willing to wait.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(20);

/// How much of a server-supplied diagnostic to keep when reporting it.
///
/// Long enough for a real login rejection, short enough that a peer cannot decide
/// how much of an operator's log one failed connection occupies.
const MAX_REPORTED_CHARS: usize = 512;

/// Renders server-supplied diagnostic text as one bounded log line.
///
/// A TDS error message and a routing target are both chosen by the peer, and
/// `tiberius` surfaces them verbatim. Every log record here has to stay on a single
/// line, so control characters are replaced rather than passed through — otherwise a
/// peer emitting newlines splits one failure across several records, each of which
/// reads like an independent event.
fn as_one_line(text: &str) -> String {
    let mut reported: String = text
        .chars()
        .take(MAX_REPORTED_CHARS)
        .map(|character| {
            if character.is_control() {
                ' '
            } else {
                character
            }
        })
        .collect();
    // `nth` rather than `count`: asking how long the whole text is would walk all of it,
    // which is the work the cap exists to avoid.
    if text.chars().nth(MAX_REPORTED_CHARS).is_some() {
        reported.push('…');
    }
    reported
}

/// Reports a connection attempt abandoned because it passed its deadline.
///
/// [`IoErrorKind::TimedOut`] rather than a protocol error: nothing about the exchange
/// was malformed, the peer simply never finished it. The address is named because a
/// routing redirect means the attempt that stalled need not be the one the dataset
/// configured, and it is bounded because that address can be server-supplied.
///
/// `bound` is the deadline for the attempt as a whole, not a budget the named address
/// received on its own: one deadline is shared by every dial in a routing chain, so a
/// redirected replica can expire it having been given only what earlier dials left. The
/// report says so, because naming an address next to a duration it never had would
/// implicate a replica that was answering as fast as it was allowed to.
fn stalled(addr: &str, bound: Duration) -> TiberiusError {
    TiberiusError::Io {
        kind: IoErrorKind::TimedOut,
        message: format!(
            "connecting to SQL Server did not complete within {}s, so the attempt was abandoned rather than left holding a connection pool slot. That deadline covers the attempt as a whole, including any routing redirects, and it expired while dialling {}. A peer that accepts the connection and then stops answering the TDS login reaches this; check that the address is a reachable SQL Server instance. For details, visit: https://spiceai.org/docs/components/data-connectors/mssql",
            bound.as_secs(),
            as_one_line(addr)
        ),
    }
}

#[derive(Clone, Debug)]
pub struct SqlServerConnectionManager {
    config: Config,
    /// The wall-clock bound on one connection attempt; [`CONNECT_TIMEOUT`] in production.
    connect_timeout: Duration,
}

impl SqlServerConnectionManager {
    fn new(config: Config) -> SqlServerConnectionManager {
        Self {
            config,
            connect_timeout: CONNECT_TIMEOUT,
        }
    }

    pub async fn create(config: Config) -> super::Result<SqlServerConnectionPool> {
        let manager = SqlServerConnectionManager::new(config);
        let pool = bb8::Pool::builder()
            .error_sink(Box::new(LogConnectionErrors))
            .build(manager)
            .await
            .context(SqlServerAccessSnafu)?;
        Ok(pool)
    }

    /// Connects with this manager's configuration, following any read-only routing
    /// redirect the server answers with.
    ///
    /// Each redirect re-dials a *clone* of the configured settings with only the host
    /// and port replaced, so the credentials, database, encryption and application name
    /// the routed replica needs are the ones the dataset supplied.
    ///
    /// The deadline is taken once and shared by every attempt, so following a chain of
    /// redirects costs the caller no more wall-clock time than dialling one address
    /// that never answers.
    async fn connect_with<C, F, Fut>(&self, connect: F) -> Result<C, TiberiusError>
    where
        F: Fn(Config) -> Fut,
        Fut: Future<Output = Result<C, TiberiusError>>,
    {
        let deadline = Instant::now() + self.connect_timeout;
        let mut config = self.config.clone();
        let mut redirects = 0;
        loop {
            match timeout_at(deadline, connect(config.clone())).await {
                // `config` still names the address this attempt was dialled at: only the
                // routing arm below rewrites it, and only after an attempt has returned.
                Err(_elapsed) => return Err(stalled(&config.get_addr(), self.connect_timeout)),
                Ok(Err(TiberiusError::Routing { host, port })) => {
                    if redirects == MAX_ROUTING_REDIRECTS {
                        return Err(TiberiusError::Protocol(
                            format!(
                                "the server redirected the connection more than {MAX_ROUTING_REDIRECTS} times without completing a login, most recently to {}:{port}. Check the availability group's read-only routing list: a list that routes back to the listener redirects for as long as the connection is retried. For details, visit: https://spiceai.org/docs/components/data-connectors/mssql",
                                as_one_line(&host)
                            )
                            .into(),
                        ));
                    }
                    redirects += 1;
                    tracing::debug!(
                        "SQL Server routed the connection to {}:{port}",
                        as_one_line(&host)
                    );
                    config.host(host);
                    config.port(port);
                }
                Ok(result) => return result,
            }
        }
    }

    /// Builds a manager whose attempts are bounded by `connect_timeout` rather than by
    /// [`CONNECT_TIMEOUT`], so a test can drive the deadline on the real clock.
    #[cfg(test)]
    fn with_connect_timeout(config: Config, connect_timeout: Duration) -> Self {
        Self {
            config,
            connect_timeout,
        }
    }
}

/// Reports the per-attempt connection failures the pool retries through.
///
/// `bb8` re-dials a failed connection on a backoff until its connection timeout
/// expires and hands each intermediate error to the pool's error sink, which
/// defaults to `NopErrorSink`. Under that default a caller sees only
/// `RunError::TimedOut`, so a login rejected for a specific, reportable reason —
/// wrong credentials, an unreachable replica, a routing list that never settles —
/// is indistinguishable from a server that is simply not answering.
#[derive(Clone, Copy, Debug)]
struct LogConnectionErrors;

impl ErrorSink<TiberiusError> for LogConnectionErrors {
    fn sink(&self, error: TiberiusError) {
        tracing::warn!(
            "Failed to connect to SQL Server: {}. Retrying.",
            as_one_line(&error.to_string())
        );
    }

    fn boxed_clone(&self) -> Box<dyn ErrorSink<TiberiusError>> {
        Box::new(*self)
    }
}

/// Opens a TDS connection to the address `config` names.
async fn connect_over_tcp(config: Config) -> Result<Client<Compat<TcpStream>>, TiberiusError> {
    let tcp = TcpStream::connect(config.get_addr()).await?;
    tcp.set_nodelay(true)?;
    Client::connect(config, tcp.compat_write()).await
}

#[async_trait]
impl bb8::ManageConnection for SqlServerConnectionManager {
    type Connection = Client<Compat<TcpStream>>;
    type Error = tiberius::error::Error;

    fn connect(&self) -> impl Future<Output = Result<Self::Connection, Self::Error>> + Send {
        Box::pin(self.connect_with(connect_over_tcp))
    }

    fn is_valid(
        &self,
        conn: &mut Self::Connection,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        Box::pin(async move {
            conn.simple_query("SELECT 1").await?.into_row().await?;
            Ok(())
        })
    }

    fn has_broken(&self, _: &mut Self::Connection) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::VecDeque,
        future::{Ready, pending, ready},
        io::Write,
        sync::Arc,
        time::Duration,
    };

    use bb8::ManageConnection;
    use parking_lot::Mutex;
    use tokio::{
        net::TcpListener,
        time::{Instant, sleep},
    };

    use super::{
        CONNECT_TIMEOUT, Config, ErrorSink, IoErrorKind, LogConnectionErrors, MAX_REPORTED_CHARS,
        MAX_ROUTING_REDIRECTS, SqlServerConnectionManager, TiberiusError,
    };

    const LISTENER: &str = "ag-listener";
    const LISTENER_PORT: u16 = 1433;

    fn listener_config() -> Config {
        let mut config = Config::new();
        config.host(LISTENER);
        config.port(LISTENER_PORT);
        config
    }

    fn routing_to(host: &str, port: u16) -> TiberiusError {
        TiberiusError::Routing {
            host: host.to_string(),
            port,
        }
    }

    /// Answers each connection attempt from a script, recording the address it was dialled at.
    ///
    /// The connection value is the address itself, so a test can assert which replica the
    /// returned connection was established against rather than only that one was returned.
    struct ScriptedServer {
        dialled: Mutex<Vec<String>>,
        replies: Mutex<VecDeque<Result<(), TiberiusError>>>,
    }

    impl ScriptedServer {
        /// Answers the first attempts from `replies`; once they run out, every further
        /// attempt is redirected back at the listener — the misconfiguration the
        /// redirect bound exists to terminate.
        fn new(replies: Vec<Result<(), TiberiusError>>) -> Arc<Self> {
            Arc::new(Self {
                dialled: Mutex::new(Vec::new()),
                replies: Mutex::new(replies.into()),
            })
        }

        fn connect(self: Arc<Self>, config: &Config) -> Ready<Result<String, TiberiusError>> {
            let addr = config.get_addr();
            self.dialled.lock().push(addr.clone());
            let reply = self
                .replies
                .lock()
                .pop_front()
                .unwrap_or_else(|| Err(routing_to(LISTENER, LISTENER_PORT)));
            ready(reply.map(|()| addr))
        }

        fn dialled(&self) -> Vec<String> {
            self.dialled.lock().clone()
        }
    }

    /// Drives the manager's own connect path, so a regression that stops it consulting
    /// the routing logic fails here rather than only inside a helper nothing calls.
    async fn connect_via(server: &Arc<ScriptedServer>) -> Result<String, TiberiusError> {
        let server = Arc::clone(server);
        SqlServerConnectionManager::new(listener_config())
            .connect_with(move |config| Arc::clone(&server).connect(&config))
            .await
    }

    #[tokio::test]
    async fn a_routing_redirect_is_followed_to_the_named_replica() {
        let server = ScriptedServer::new(vec![Err(routing_to("read-replica", 1444)), Ok(())]);

        let addr = connect_via(&server)
            .await
            .expect("the routed replica accepted the login");

        assert_eq!(addr, "read-replica:1444");
        assert_eq!(
            server.dialled(),
            vec!["ag-listener:1433", "read-replica:1444"]
        );
    }

    #[tokio::test]
    async fn a_login_that_is_not_routed_is_dialled_once() {
        let server = ScriptedServer::new(vec![Ok(())]);

        let addr = connect_via(&server).await.expect("the listener accepted");

        assert_eq!(addr, "ag-listener:1433");
        assert_eq!(server.dialled(), vec!["ag-listener:1433"]);
    }

    #[tokio::test]
    async fn a_non_routing_error_is_returned_unchanged() {
        let server = ScriptedServer::new(vec![Err(TiberiusError::Protocol("login failed".into()))]);

        let Err(error) = connect_via(&server).await else {
            panic!("expected the connection to fail");
        };

        assert_eq!(error, TiberiusError::Protocol("login failed".into()));
        assert_eq!(server.dialled(), vec!["ag-listener:1433"]);
    }

    #[tokio::test]
    async fn a_chain_of_redirects_is_bounded_and_reported() {
        let server = ScriptedServer::new(Vec::new());

        let Err(error) = connect_via(&server).await else {
            panic!("expected a routing list that never settles to fail");
        };

        let TiberiusError::Protocol(message) = &error else {
            panic!("expected the exhausted chain to be reported as a protocol error, got {error}");
        };
        assert!(
            message.contains("ag-listener:1433"),
            "the message should name the address it was last redirected to: {message}"
        );
        assert!(
            message.contains("read-only routing list"),
            "the message should name the configuration to check: {message}"
        );
        let dialled = server.dialled().len();
        assert_eq!(dialled, MAX_ROUTING_REDIRECTS + 1);
        // Asserted against a literal as well as the constant: the equality above moves with
        // `MAX_ROUTING_REDIRECTS`, so on its own it would accept a bound raised high enough
        // that a routing loop still dials for as long as the caller is willing to wait.
        assert!(
            dialled <= 8,
            "a routing chain must terminate within a few dials, not {dialled}"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_connect_that_is_never_answered_ends_at_its_deadline() {
        let started = Instant::now();

        let Err(error) = SqlServerConnectionManager::new(listener_config())
            .connect_with(|_config| pending::<Result<String, TiberiusError>>())
            .await
        else {
            panic!("expected a peer that never answers to fail");
        };

        let TiberiusError::Io { kind, message } = &error else {
            panic!("expected the abandoned attempt to be reported as an I/O timeout, got {error}");
        };
        assert_eq!(*kind, IoErrorKind::TimedOut);
        assert!(
            message.contains("ag-listener:1433"),
            "the report should name the address that stalled: {message}"
        );
        let elapsed = started.elapsed();
        assert!(
            elapsed >= CONNECT_TIMEOUT && elapsed < 2 * CONNECT_TIMEOUT,
            "the attempt should end at its deadline, not after {elapsed:?}"
        );
    }

    /// Every replica in the chain stalls for most of the bound before redirecting again.
    /// A deadline taken per attempt rather than once would let a routing list multiply
    /// the wait by `MAX_ROUTING_REDIRECTS + 1`, which is the whole window the pool has.
    #[tokio::test(start_paused = true)]
    async fn a_chain_of_stalling_redirects_shares_one_deadline() {
        let stall = CONNECT_TIMEOUT * 2 / 3;
        let started = Instant::now();

        let Err(error) = SqlServerConnectionManager::new(listener_config())
            .connect_with(|_config| async move {
                sleep(stall).await;
                Err::<String, _>(routing_to("read-replica", 1444))
            })
            .await
        else {
            panic!("expected a chain of stalling replicas to fail");
        };

        let TiberiusError::Io { kind, message } = &error else {
            panic!("expected the abandoned attempt to be reported as an I/O timeout, got {error}");
        };
        assert_eq!(*kind, IoErrorKind::TimedOut);
        assert!(
            message.contains("read-replica:1444"),
            "the report should name the redirected address that stalled: {message}"
        );
        let elapsed = started.elapsed();
        assert!(
            elapsed <= CONNECT_TIMEOUT,
            "following a redirect must not extend the bound: {elapsed:?}"
        );
    }

    /// Drives the manager's own `connect` — DNS, TCP, and the TDS login — against a peer
    /// that completes the TCP handshake and then answers nothing. The OS-level connect
    /// timeout never applies to that shape, because the connection was accepted.
    #[tokio::test]
    async fn a_peer_that_accepts_tcp_and_then_stalls_fails_within_the_bound() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("a loopback listener");
        let addr = listener.local_addr().expect("the bound address");
        let peer = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("the client to connect");
            // Hold the accepted socket open without answering the pre-login: a peer that
            // closed it would fail the attempt on its own, before any deadline.
            sleep(Duration::from_secs(60)).await;
            drop(stream);
        });

        let mut config = Config::new();
        config.host(addr.ip().to_string());
        config.port(addr.port());
        let manager =
            SqlServerConnectionManager::with_connect_timeout(config, Duration::from_millis(500));
        let started = Instant::now();

        let Err(error) = manager.connect().await else {
            panic!("expected a peer that never answers the login to fail");
        };
        peer.abort();

        let TiberiusError::Io { kind, .. } = &error else {
            panic!("expected the stalled login to be reported as an I/O timeout, got {error}");
        };
        assert_eq!(*kind, IoErrorKind::TimedOut);
        let elapsed = started.elapsed();
        assert!(
            elapsed < Duration::from_secs(10),
            "the attempt should end at its own bound, not the peer's: {elapsed:?}"
        );
    }

    /// Collects everything a subscriber writes, so a test can count emitted events.
    #[derive(Clone, Default)]
    struct CapturedLogs(Arc<Mutex<Vec<u8>>>);

    impl Write for CapturedLogs {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl tracing_subscriber::fmt::MakeWriter<'_> for CapturedLogs {
        type Writer = CapturedLogs;

        fn make_writer(&self) -> Self::Writer {
            self.clone()
        }
    }

    impl CapturedLogs {
        fn contents(&self) -> String {
            String::from_utf8(self.0.lock().clone()).expect("captured logs are UTF-8")
        }
    }

    /// Runs `emit` against a subscriber that keeps only warnings, and returns what it wrote.
    fn warnings_from(emit: impl FnOnce()) -> String {
        let logs = CapturedLogs::default();
        let subscriber = tracing_subscriber::fmt()
            .with_writer(logs.clone())
            .with_max_level(tracing::Level::WARN)
            .finish();
        tracing::subscriber::with_default(subscriber, emit);
        logs.contents()
    }

    #[test]
    fn every_connection_failure_the_pool_retries_is_reported_once() {
        let contents = warnings_from(|| {
            LogConnectionErrors.sink(routing_to("read-replica", 1444));
        });
        assert_eq!(
            contents.matches("Failed to connect to SQL Server").count(),
            1,
            "each retried failure should be reported exactly once: {contents}"
        );
        assert!(
            contents.contains("read-replica:1444"),
            "the report should carry the underlying error: {contents}"
        );
    }

    #[test]
    fn a_server_supplied_diagnostic_stays_on_one_bounded_line() {
        // What a hostile — or merely broken — server can put in a TDS error token: line
        // breaks that would split one failure across several records, and a body long
        // enough to let the peer decide how much of the log it occupies.
        let hostile = format!("first line\r\nsecond line\t{}", "A".repeat(4096));
        let contents = warnings_from(|| {
            LogConnectionErrors.sink(TiberiusError::Protocol(hostile.into()));
        });
        assert_eq!(
            contents.lines().count(),
            1,
            "one failure must be one record: {contents}"
        );
        assert!(
            contents.contains("first line  second line "),
            "the text should survive with its control characters replaced: {contents}"
        );
        assert!(
            contents.contains('…'),
            "an over-long diagnostic should be marked as truncated: {contents}"
        );
        assert!(
            contents.chars().count() < 2 * MAX_REPORTED_CHARS,
            "the record should be bounded by the cap, not by what the peer sent ({} chars)",
            contents.chars().count()
        );
    }
}
