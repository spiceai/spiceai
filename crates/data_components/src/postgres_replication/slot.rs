/*
Copyright 2026 The Spice.ai OSS Authors

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

//! Idempotent publication + replication slot setup.
//!
//! Uses a regular (non-replication) Postgres connection because `CREATE
//! PUBLICATION` is not allowed on replication connections and the slot state
//! queries live in normal catalog tables.

use snafu::ResultExt;
use tokio_postgres::NoTls;

use super::{
    MissingPrimaryKeySnafu, Result, SetupConnectSnafu, SetupExecSnafu, SourceTableNotFoundSnafu,
    TlsConfigSnafu, UnsupportedReplicaIdentitySnafu, config::ReplicationParams,
};

/// Info about a slot after `setup_slot_and_publication` returns.
#[derive(Clone, Debug)]
pub struct SlotInfo {
    pub slot_name: String,
    pub publication_name: String,
    pub consistent_lsn: u64,
    pub snapshot_name: Option<String>,
    pub created_fresh: bool,
}

pub type SlotSetupOutcome = SlotInfo;

/// Create publication and replication slot if they don't exist.
///
/// Returns information about the slot including the LSN to start replication
/// from. If `created_fresh = true`, the caller is expected to run a snapshot
/// bootstrap using `snapshot_name` before starting WAL streaming.
///
/// For existing slots we return `consistent_lsn = 0` which instructs
/// `START_REPLICATION` to resume from the server-side `confirmed_flush_lsn` —
/// this is Postgres' built-in durable checkpoint.
pub async fn setup_slot_and_publication(
    params: &ReplicationParams,
    schema_name: &str,
    table_name: &str,
) -> Result<SlotInfo> {
    // Retry the setup-path on transient connect/exec failures — catalog queries
    // are idempotent and slot/publication creation is guarded by `IF EXISTS`
    // checks. Fatal errors (permission denied, syntax) are propagated on the
    // first attempt.
    super::resilience::retry_async(
        "postgres_replication::setup",
        super::resilience::DEFAULT_SETUP_MAX_ELAPSED,
        is_transient_setup_error,
        || async { setup_once(params, schema_name, table_name).await },
    )
    .await
}

async fn setup_once(
    params: &ReplicationParams,
    schema_name: &str,
    table_name: &str,
) -> Result<SlotInfo> {
    let cfg = params.setup_pg_config();
    let tls = params.native_tls_connector().context(TlsConfigSnafu)?;

    let (client, conn_task) = match tls {
        Some(connector) => {
            let (client, connection) = cfg.connect(connector).await.context(SetupConnectSnafu)?;
            let task = tokio::spawn(async move {
                if let Err(e) = connection.await {
                    tracing::warn!("postgres setup connection terminated: {e}");
                }
            });
            (client, task)
        }
        None => {
            let (client, connection) = cfg.connect(NoTls).await.context(SetupConnectSnafu)?;
            let task = tokio::spawn(async move {
                if let Err(e) = connection.await {
                    tracing::warn!("postgres setup connection terminated: {e}");
                }
            });
            (client, task)
        }
    };

    let outcome = do_setup(&client, params, schema_name, table_name).await;

    drop(client);
    let _ = conn_task.await;

    outcome
}

fn is_transient_setup_error(e: &super::Error) -> bool {
    match e {
        super::Error::SetupConnect { source } | super::Error::SetupExec { source } => {
            super::resilience::is_transient_pg(source)
        }
        super::Error::Bootstrap { source } => super::resilience::is_transient_pg(source),
        // Config, schema, permission errors are fatal.
        _ => false,
    }
}

async fn do_setup(
    client: &tokio_postgres::Client,
    params: &ReplicationParams,
    schema_name: &str,
    table_name: &str,
) -> Result<SlotInfo> {
    validate_replica_identity(client, schema_name, table_name).await?;
    ensure_publication(client, &params.publication_name, schema_name, table_name).await?;

    let existing_slot = slot_exists(client, &params.slot_name).await?;
    if existing_slot {
        tracing::info!(
            slot = %params.slot_name,
            publication = %params.publication_name,
            "Resuming from existing replication slot"
        );
        return Ok(SlotInfo {
            slot_name: params.slot_name.clone(),
            publication_name: params.publication_name.clone(),
            consistent_lsn: 0, // sentinel: pgwire-replication treats 0 as "server decides"
            snapshot_name: None,
            created_fresh: false,
        });
    }

    let (consistent_lsn, snapshot_name) =
        create_logical_slot(client, &params.slot_name, params.temporary_slot).await?;

    tracing::info!(
        slot = %params.slot_name,
        publication = %params.publication_name,
        consistent_lsn = %format_lsn(consistent_lsn),
        snapshot = %snapshot_name,
        "Created new replication slot"
    );

    Ok(SlotInfo {
        slot_name: params.slot_name.clone(),
        publication_name: params.publication_name.clone(),
        consistent_lsn,
        snapshot_name: Some(snapshot_name),
        created_fresh: true,
    })
}

async fn validate_replica_identity(
    client: &tokio_postgres::Client,
    schema_name: &str,
    table_name: &str,
) -> Result<()> {
    let row = client
        .query_opt(
            "SELECT c.relreplident::text, \
             (SELECT count(*) FROM pg_index i WHERE i.indrelid = c.oid AND i.indisprimary) \
             FROM pg_class c \
             JOIN pg_namespace n ON n.oid = c.relnamespace \
             WHERE n.nspname = $1 AND c.relname = $2",
            &[&schema_name, &table_name],
        )
        .await
        .context(SetupExecSnafu)?;

    let Some(row) = row else {
        return SourceTableNotFoundSnafu {
            schema: schema_name.to_string(),
            table: table_name.to_string(),
        }
        .fail();
    };

    let relreplident: String = row.get(0);
    let pk_count: i64 = row.get(1);

    match relreplident.as_str() {
        "n" => UnsupportedReplicaIdentitySnafu {
            schema: schema_name.to_string(),
            table: table_name.to_string(),
        }
        .fail(),
        "d" if pk_count == 0 => MissingPrimaryKeySnafu {
            schema: schema_name.to_string(),
            table: table_name.to_string(),
        }
        .fail(),
        _ => Ok(()),
    }
}

async fn ensure_publication(
    client: &tokio_postgres::Client,
    publication_name: &str,
    schema_name: &str,
    table_name: &str,
) -> Result<()> {
    let exists: bool = client
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM pg_publication WHERE pubname = $1)",
            &[&publication_name],
        )
        .await
        .context(SetupExecSnafu)?
        .get(0);

    if exists {
        // Verify the publication includes our table; if not, add it.
        let has_table: bool = client
            .query_one(
                "SELECT EXISTS(SELECT 1 FROM pg_publication_tables \
                 WHERE pubname = $1 AND schemaname = $2 AND tablename = $3)",
                &[&publication_name, &schema_name, &table_name],
            )
            .await
            .context(SetupExecSnafu)?
            .get(0);
        if !has_table {
            let stmt = format!(
                "ALTER PUBLICATION {pub} ADD TABLE {schema}.{table}",
                pub = quote_ident(publication_name),
                schema = quote_ident(schema_name),
                table = quote_ident(table_name),
            );
            client.simple_query(&stmt).await.context(SetupExecSnafu)?;
        }
        return Ok(());
    }

    let stmt = format!(
        "CREATE PUBLICATION {pub} FOR TABLE {schema}.{table}",
        pub = quote_ident(publication_name),
        schema = quote_ident(schema_name),
        table = quote_ident(table_name),
    );
    client.simple_query(&stmt).await.context(SetupExecSnafu)?;
    Ok(())
}

async fn slot_exists(client: &tokio_postgres::Client, slot_name: &str) -> Result<bool> {
    let row = client
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)",
            &[&slot_name],
        )
        .await
        .context(SetupExecSnafu)?;
    Ok(row.get(0))
}

/// Executes `CREATE_REPLICATION_SLOT` via a regular SQL function call rather
/// than the replication-connection command because we want the snapshot and
/// LSN to be usable from a normal (non-replication) connection afterwards for
/// the initial snapshot COPY.
///
/// Returns (`consistent_lsn`, `snapshot_name`).
async fn create_logical_slot(
    client: &tokio_postgres::Client,
    slot_name: &str,
    temporary: bool,
) -> Result<(u64, String)> {
    // pg_create_logical_replication_slot doesn't support EXPORT_SNAPSHOT directly
    // via SQL on older versions, but pg_create_logical_replication_slot with
    // temporary=false returns (slot_name, lsn). For a fully-featured snapshot
    // export we'd need to go through the replication protocol, but in PG 16+ the
    // SQL function exposes (slot_name, lsn). We capture the LSN and use it as
    // the starting position; for the initial snapshot we rely on a REPEATABLE
    // READ transaction instead of the exported snapshot — simpler and works on
    // any connection.
    let row = client
        .query_one(
            "SELECT slot_name, lsn::text FROM pg_create_logical_replication_slot($1, 'pgoutput', $2)",
            &[&slot_name, &temporary],
        )
        .await
        .context(SetupExecSnafu)?;

    let lsn_str: String = row.get(1);
    let consistent_lsn = parse_lsn(&lsn_str)?;

    // Use the LSN itself as a pseudo-snapshot-name — it's unused downstream when
    // we do REPEATABLE READ bootstrap rather than SET TRANSACTION SNAPSHOT.
    Ok((consistent_lsn, lsn_str))
}

/// Parses a Postgres LSN string like "16/B374D848" into a u64.
/// Errors on malformed input rather than defaulting to 0, because 0 is also
/// the "server decides" sentinel downstream — silently coercing invalid input
/// would change the replication start position.
pub fn parse_lsn(s: &str) -> Result<u64> {
    let (hi_str, lo_str) = s
        .split_once('/')
        .ok_or_else(|| super::Error::InvalidLsn { lsn: s.to_string() })?;
    let hi = u64::from_str_radix(hi_str, 16)
        .map_err(|_| super::Error::InvalidLsn { lsn: s.to_string() })?;
    let lo = u64::from_str_radix(lo_str, 16)
        .map_err(|_| super::Error::InvalidLsn { lsn: s.to_string() })?;
    Ok((hi << 32) | lo)
}

// Keep an infallible helper for the tests that previously relied on `.unwrap_or(0)`
// semantics. Inline in tests only — prefer `parse_lsn` at call sites.
#[cfg(test)]
#[allow(dead_code)]
fn parse_lsn_or_zero(s: &str) -> u64 {
    parse_lsn(s).unwrap_or(0)
}

#[must_use]
pub fn format_lsn(lsn: u64) -> String {
    format!("{:X}/{:X}", lsn >> 32, lsn as u32)
}

/// Minimal identifier quoting: double-quote and escape any embedded quotes.
/// Callers should already have passed identifiers through `sanitize`, but this
/// is belt-and-braces.
fn quote_ident(ident: &str) -> String {
    let escaped = ident.replace('"', "\"\"");
    format!("\"{escaped}\"")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lsn_round_trip() {
        assert_eq!(parse_lsn("0/0").expect("parse"), 0);
        assert_eq!(
            parse_lsn("16/B374D848").expect("parse"),
            (0x16u64 << 32) | 0xB374_D848
        );
        assert_eq!(format_lsn((0x16u64 << 32) | 0xB374_D848), "16/B374D848");
    }

    #[test]
    fn parse_lsn_rejects_malformed() {
        assert!(matches!(
            parse_lsn("not-an-lsn"),
            Err(super::super::Error::InvalidLsn { .. })
        ));
        assert!(matches!(
            parse_lsn("16ZZ/00"),
            Err(super::super::Error::InvalidLsn { .. })
        ));
        // Missing slash.
        assert!(matches!(
            parse_lsn("16B374D848"),
            Err(super::super::Error::InvalidLsn { .. })
        ));
    }

    // `parse_lsn_or_zero` exists for one test-only call site; silence dead-code
    // warning by exercising it here.
    #[test]
    fn parse_lsn_or_zero_fallback() {
        assert_eq!(parse_lsn_or_zero("not-an-lsn"), 0);
        assert_eq!(parse_lsn_or_zero("16/1"), (0x16u64 << 32) | 0x1);
    }

    #[test]
    fn quote_ident_escapes_quotes() {
        assert_eq!(quote_ident("users"), "\"users\"");
        assert_eq!(quote_ident("we\"ird"), "\"we\"\"ird\"");
    }
}
