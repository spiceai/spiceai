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

/// Opens a pool as a new `user` with `defaults`, runs [`QUERIES`] statements back to back, and
/// returns how many connections `MySQL` saw that user open.
async fn connections_opened(
    port: u16,
    root: &mysql_async::Pool,
    user: &str,
    defaults: PoolDefaults,
) -> anyhow::Result<u64> {
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
    .with_context(|| format!("create user {user}"))?;

    let settings = PoolSettings::resolve(PoolParams::default(), Some(defaults))?;
    let params = [
        ("host", "127.0.0.1".to_string()),
        ("tcp_port", port.to_string()),
        ("user", user.to_string()),
        ("pass", "pw".to_string()),
        ("db", "mysqldb".to_string()),
        ("sslmode", "disabled".to_string()),
    ]
    .map(|(key, value)| (key.to_string(), SecretString::from(value)));
    let mysql = pool::connect(params, settings, format!("dataset '{user}'"))
        .await
        .with_context(|| format!("open the pool for {user}"))?;
    for _ in 0..QUERIES {
        let connection = mysql.connect().await.map_err(|e| anyhow::anyhow!(e))?;
        connection
            .as_async()
            .context("an async MySQL connection")?
            .execute("SELECT 1", &[])
            .await
            .map_err(|e| anyhow::anyhow!(e))?;
    }

    let mut admin = root.get_conn().await?;
    admin
        .exec_first::<u64, _, _>(
            "SELECT TOTAL_CONNECTIONS FROM performance_schema.users WHERE USER = ?",
            (user,),
        )
        .await?
        .with_context(|| format!("no connection count for {user}"))
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
