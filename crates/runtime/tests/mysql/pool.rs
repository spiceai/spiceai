/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

//! The `MySQL` connection pool against a real server.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::Context;
use data_components::mysql::pool::{self, PoolDefaults, PoolParams, PoolSettings};
use datafusion_table_providers::sql::db_connection_pool::DbConnectionPool;
use mysql_async::prelude::Queryable;
use secrecy::SecretString;
use util::{RetryError, fibonacci_backoff::FibonacciBackoffBuilder, retry};

use crate::init_tracing;
use crate::mysql::common;

/// How many statements each pool runs, each on a connection checked out as soon as the
/// previous one was returned.
const QUERIES: usize = 200;

/// Creates `user`, password `pw`, with read access to `mysqldb`.
async fn create_user(root: &mysql_async::Pool, user: &str) -> anyhow::Result<()> {
    let retry_strategy = FibonacciBackoffBuilder::new().max_retries(Some(10)).build();
    retry(retry_strategy, || async {
        let mut admin = root.get_conn().await.map_err(RetryError::transient)?;
        admin
            .query_drop(format!(
                "CREATE USER IF NOT EXISTS '{user}'@'%' IDENTIFIED BY 'pw'"
            ))
            .await
            .map_err(RetryError::transient)?;
        admin
            .query_drop(format!("GRANT SELECT ON mysqldb.* TO '{user}'@'%'"))
            .await
            .map_err(RetryError::transient)
    })
    .await
    .with_context(|| format!("create user {user}"))
}

/// How many connections `user` has opened, as the server counts them.
async fn total_connections(root: &mysql_async::Pool, user: &str) -> anyhow::Result<u64> {
    let mut admin = root.get_conn().await?;
    admin
        .exec_first::<u64, _, _>(
            "SELECT TOTAL_CONNECTIONS FROM performance_schema.users WHERE USER = ?",
            (user,),
        )
        .await?
        .with_context(|| format!("no connection count for {user}"))
}

/// The connection parameters for `user` on `port`.
fn user_params(port: u16, user: &str) -> [(String, SecretString); 6] {
    [
        ("host", "127.0.0.1".to_string()),
        ("tcp_port", port.to_string()),
        ("user", user.to_string()),
        ("pass", "pw".to_string()),
        ("db", "mysqldb".to_string()),
        ("sslmode", "disabled".to_string()),
    ]
    .map(|(key, value)| (key.to_string(), SecretString::from(value)))
}

/// Opens a pool as a new `user` with `defaults`, runs `queries` statements back to back, and
/// returns how many connections `MySQL` saw that user open.
async fn connections_opened(
    port: u16,
    root: &mysql_async::Pool,
    user: &str,
    defaults: PoolDefaults,
    queries: usize,
) -> anyhow::Result<u64> {
    create_user(root, user).await?;
    let settings = PoolSettings::resolve(PoolParams::default(), Some(defaults))?;
    let mysql = pool::connect(
        user_params(port, user),
        settings,
        format!("dataset '{user}'"),
    )
    .await
    .with_context(|| format!("open the pool for {user}"))?;
    for _ in 0..queries {
        let connection = mysql.connect().await.map_err(|e| anyhow::anyhow!(e))?;
        connection
            .as_async()
            .context("an async MySQL connection")?
            .execute("SELECT 1", &[])
            .await
            .map_err(|e| anyhow::anyhow!(e))?;
    }
    total_connections(root, user).await
}

/// Statements that each start as soon as the previous one ends reuse the pool's connections.
/// `mysql_async` resets a returned connection before it can be used again, so a statement that
/// starts meanwhile opens another connection. The pool keeps that connection while it is idle,
/// so it never opens more than its maximum; a pool that closes idle connections above its
/// minimum at once instead opens a connection for a large share of the statements. `MySQL`'s
/// own count of each user's connections is the oracle.
#[tokio::test]
async fn back_to_back_queries_reuse_pooled_connections() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(None);
    let container = common::start_mysql_docker_container().await?;
    let port = container.host_port(3306)?;
    let root = common::get_mysql_conn(port)?;

    let max = 5;
    let kept = connections_opened(
        port,
        &root,
        "idle_kept",
        PoolDefaults {
            min: 1,
            max,
            idle_ttl: pool::DEFAULT_IDLE_CONNECTION_TTL,
        },
        QUERIES,
    )
    .await?;
    let closed = connections_opened(
        port,
        &root,
        "idle_closed",
        PoolDefaults {
            min: 1,
            max,
            idle_ttl: Duration::ZERO,
        },
        QUERIES,
    )
    .await?;
    eprintln!(
        "{QUERIES} back-to-back statements opened {kept} connections with idle connections kept, {closed} with them closed at once"
    );

    assert!(
        kept <= max as u64,
        "{QUERIES} back-to-back statements opened {kept} connections, more than the pool maximum of {max}"
    );
    // The pool under test keeps idle connections; without that, this path churns.
    assert!(
        closed >= (QUERIES / 4) as u64,
        "a pool that closes idle connections at once opened only {closed} connections for {QUERIES} back-to-back statements, so this test no longer exercises the churn it guards against"
    );
    Ok(())
}

/// A new pool's first statement runs on the connection the pool opened to check the server,
/// however soon after the pool is opened it starts: a pool that has run one statement has
/// opened one connection. `MySQL`'s own count of the user's connections is the oracle.
#[tokio::test]
async fn a_new_pool_opens_one_connection_for_its_first_statement() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(None);
    let container = common::start_mysql_docker_container().await?;
    let port = container.host_port(3306)?;
    let root = common::get_mysql_conn(port)?;

    let opened = connections_opened(
        port,
        &root,
        "first_statement",
        PoolDefaults {
            min: 1,
            max: 5,
            idle_ttl: pool::DEFAULT_IDLE_CONNECTION_TTL,
        },
        1,
    )
    .await?;
    assert_eq!(
        opened, 1,
        "a new pool opened {opened} connections to run one statement"
    );
    Ok(())
}

/// A TCP proxy on a local port whose upstream port can be switched while it runs, as a
/// load-balanced hostname moves new connections to another server. Each connection it accepts
/// goes to the upstream set when it is accepted, so connections already open stay where they
/// are.
struct SwitchingProxy {
    port: u16,
    upstream: Arc<AtomicU16>,
    accept: tokio::task::JoinHandle<()>,
}

impl SwitchingProxy {
    async fn start(upstream: u16) -> anyhow::Result<Self> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let port = listener.local_addr()?.port();
        let upstream = Arc::new(AtomicU16::new(upstream));
        let accept = tokio::spawn({
            let upstream = Arc::clone(&upstream);
            async move {
                while let Ok((mut client, _)) = listener.accept().await {
                    let to = upstream.load(Ordering::SeqCst);
                    tokio::spawn(async move {
                        if let Ok(mut server) =
                            tokio::net::TcpStream::connect(("127.0.0.1", to)).await
                        {
                            let _ = tokio::io::copy_bidirectional(&mut client, &mut server).await;
                        }
                    });
                }
            }
        });
        Ok(Self {
            port,
            upstream,
            accept,
        })
    }

    fn switch_to(&self, upstream: u16) {
        self.upstream.store(upstream, Ordering::SeqCst);
    }
}

impl Drop for SwitchingProxy {
    fn drop(&mut self) {
        self.accept.abort();
    }
}

/// What is logged while it is installed, as text.
#[derive(Clone, Default)]
struct CapturedLogs(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for CapturedLogs {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0
            .lock()
            .map_err(|_| std::io::Error::other("log buffer poisoned"))?
            .extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for CapturedLogs {
    type Writer = Self;

    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

/// The id of the session `connection` is on.
async fn connection_id(
    connection: &datafusion_table_providers::sql::db_connection_pool::dbconnection::mysqlconn::MySQLConnection,
) -> anyhow::Result<u64> {
    let mut conn = connection.conn.lock().await;
    conn.query_first("SELECT CONNECTION_ID()")
        .await?
        .context("CONNECTION_ID() returned no row")
}

/// Opens and closes connections to the server `root` reaches until the next one it opens gets
/// an id of at least `target`. `next_id` is what the caller knows of the server's next id, and
/// nothing is opened when it has already reached `target`: opening a connection to find out
/// would take the target.
async fn advance_connection_ids(
    root: &mysql_async::Pool,
    mut next_id: u64,
    target: u64,
) -> anyhow::Result<()> {
    let opts = mysql_async::OptsBuilder::from_opts(root.get_conn().await?.opts().clone())
        .prefer_socket(false);
    while next_id < target {
        let conn = mysql_async::Conn::new(opts.clone()).await?;
        next_id = u64::from(conn.id()) + 1;
        conn.disconnect().await?;
    }
    Ok(())
}

/// A dataset's pool moves to another server behind the same address, as behind a load-balanced
/// hostname, and that server gives its connections ids the first server gave connections the
/// pool still holds. Each connection is logged once all the same: each server's own count of
/// the dataset user's connections is the oracle for the log lines naming that server.
#[tokio::test]
async fn each_connection_is_logged_once_across_servers() -> Result<(), anyhow::Error> {
    // Health checks stop once each server is up, so that only this test takes ids.
    let server_a = common::start_quiet_mysql_docker_container().await?;
    let server_b = common::start_quiet_mysql_docker_container().await?;
    let (port_a, port_b) = (server_a.host_port(3306)?, server_b.host_port(3306)?);
    let (root_a, root_b) = (
        common::get_mysql_conn(port_a)?,
        common::get_mysql_conn(port_b)?,
    );
    let user = "moved";
    create_user(&root_a, user).await?;
    create_user(&root_b, user).await?;
    // Server A's ids start above B's, so B can be brought up to each id A gives the pool.
    let mut b_next = u64::from(root_b.get_conn().await?.id()) + 1;
    advance_connection_ids(&root_a, 0, b_next + 20).await?;

    let proxy = SwitchingProxy::start(port_a).await?;
    let logs = CapturedLogs::default();
    let subscriber = tracing_subscriber::fmt()
        .with_ansi(false)
        .with_env_filter(tracing_subscriber::EnvFilter::new(
            "data_components::mysql::pool=debug",
        ))
        .with_writer(logs.clone())
        .finish();
    let ids_on_a = {
        // `set_default` scopes the subscriber to this thread, where this test's
        // current-thread runtime opens every connection.
        let _guard = tracing::subscriber::set_default(subscriber);
        let settings = PoolSettings::resolve(
            PoolParams::default(),
            Some(PoolDefaults {
                min: 0,
                max: 32,
                idle_ttl: pool::DEFAULT_IDLE_CONNECTION_TTL,
            }),
        )?;
        let mysql = pool::connect(
            user_params(proxy.port, user),
            settings,
            format!("dataset '{user}'"),
        )
        .await?;
        // Connections held open, so that each checkout opens a new one.
        let mut held = Vec::new();
        let mut ids_on_a = Vec::new();
        for _ in 0..8 {
            let connection = mysql
                .connect_direct()
                .await
                .map_err(|e| anyhow::anyhow!(e))?;
            ids_on_a.push(connection_id(&connection).await?);
            held.push(connection);
        }
        ids_on_a.sort_unstable();
        // Bring B up to each id A gave, and open a connection to B, which takes that id:
        // nothing else connects to B.
        proxy.switch_to(port_b);
        for id in &ids_on_a {
            anyhow::ensure!(b_next <= *id, "B is already past {id}: {ids_on_a:?}");
            advance_connection_ids(&root_b, b_next, *id).await?;
            let connection = mysql
                .connect_direct()
                .await
                .map_err(|e| anyhow::anyhow!(e))?;
            let b_id = connection_id(&connection).await?;
            anyhow::ensure!(
                b_id == *id,
                "B's connection took {b_id}, not the id {id} A gave"
            );
            b_next = b_id + 1;
            held.push(connection);
        }
        ids_on_a
    };

    let logs = String::from_utf8(
        logs.0
            .lock()
            .map_err(|_| anyhow::anyhow!("log buffer poisoned"))?
            .clone(),
    )?;
    let line = regex::Regex::new(
        r"MySQL dataset 'moved' opened connection (\d+) to '127\.0\.0\.1:\d+', which reached server '([^']+):3306'\.",
    )?;
    let mut logged: HashMap<String, Vec<u64>> = HashMap::new();
    for captures in line.captures_iter(&logs) {
        logged
            .entry(captures[2].to_string())
            .or_default()
            .push(captures[1].parse()?);
    }
    let mut ids_on_b = Vec::new();
    for (root, server) in [(&root_a, "A"), (&root_b, "B")] {
        let hostname: String = root
            .get_conn()
            .await?
            .query_first("SELECT @@hostname")
            .await?
            .context("no @@hostname")?;
        let mut ids = logged.get(&hostname).cloned().unwrap_or_default();
        let lines = ids.len();
        ids.sort_unstable();
        ids.dedup();
        assert_eq!(
            lines,
            ids.len(),
            "a connection to server {server} was logged twice: {logs}"
        );
        let opened = total_connections(root, user).await?;
        assert_eq!(
            lines as u64, opened,
            "server {server} saw {opened} connections from '{user}', and {lines} were logged: {logs}"
        );
        if server == "B" {
            ids_on_b = ids;
        }
    }
    // Non-vacuous: server B gave the pool an id that A had given a connection still open.
    assert!(
        ids_on_b.iter().any(|id| ids_on_a.contains(id)),
        "no connection to B took an id A had given ({ids_on_a:?} on A, {ids_on_b:?} on B), so this test did not exercise reused ids"
    );
    Ok(())
}
