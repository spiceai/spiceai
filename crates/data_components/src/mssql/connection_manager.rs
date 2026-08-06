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

use async_trait::async_trait;
use bb8::{ErrorSink, Pool};
use snafu::ResultExt;
use tiberius::{Client, Config, error::Error as TiberiusError};
use tokio::net::TcpStream;

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

#[derive(Clone, Debug)]
pub struct SqlServerConnectionManager {
    config: Config,
}

impl SqlServerConnectionManager {
    fn new(config: Config) -> SqlServerConnectionManager {
        Self { config }
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
    async fn connect_with<C, F, Fut>(&self, connect: F) -> Result<C, TiberiusError>
    where
        F: Fn(Config) -> Fut,
        Fut: Future<Output = Result<C, TiberiusError>>,
    {
        let mut config = self.config.clone();
        let mut redirects = 0;
        loop {
            match connect(config.clone()).await {
                Err(TiberiusError::Routing { host, port }) => {
                    if redirects == MAX_ROUTING_REDIRECTS {
                        return Err(TiberiusError::Protocol(
                            format!(
                                "the server redirected the connection more than {MAX_ROUTING_REDIRECTS} times without completing a login, most recently to {host}:{port}. Check the availability group's read-only routing list: a list that routes back to the listener redirects for as long as the connection is retried. For details, visit: https://spiceai.org/docs/components/data-connectors/mssql"
                            )
                            .into(),
                        ));
                    }
                    redirects += 1;
                    tracing::debug!("SQL Server routed the connection to {host}:{port}");
                    config.host(host);
                    config.port(port);
                }
                result => return result,
            }
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
        tracing::warn!("Failed to connect to SQL Server: {error}. Retrying.");
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
        future::{Ready, ready},
        io::Write,
        sync::Arc,
    };

    use parking_lot::Mutex;

    use super::{
        Config, ErrorSink, LogConnectionErrors, MAX_ROUTING_REDIRECTS, SqlServerConnectionManager,
        TiberiusError,
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
        fn new(replies: Vec<Result<(), TiberiusError>>) -> Arc<Self> {
            Arc::new(Self {
                dialled: Mutex::new(Vec::new()),
                replies: Mutex::new(replies.into()),
            })
        }

        /// Replies every attempt with a routing redirect that points back at the listener,
        /// which is the misconfiguration the redirect bound exists to terminate.
        fn always_routing() -> Arc<Self> {
            Arc::new(Self {
                dialled: Mutex::new(Vec::new()),
                replies: Mutex::new(VecDeque::new()),
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
        let server = ScriptedServer::always_routing();

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

    #[test]
    fn every_connection_failure_the_pool_retries_is_reported_once() {
        let logs = CapturedLogs::default();
        let subscriber = tracing_subscriber::fmt()
            .with_writer(logs.clone())
            .with_max_level(tracing::Level::WARN)
            .finish();

        tracing::subscriber::with_default(subscriber, || {
            LogConnectionErrors.sink(routing_to("read-replica", 1444));
        });

        let contents = logs.contents();
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
}
