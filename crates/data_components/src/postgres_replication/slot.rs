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
/// For existing slots we query `pg_replication_slots.confirmed_flush_lsn` and
/// return it as `consistent_lsn` so the caller can seed its in-memory
/// `confirmed_flush` atomic (and metrics) from the server's own durable
/// checkpoint. That value is also passed to `START_REPLICATION`, so we resume
/// from the same LSN Postgres already knows about. If the catalog hasn't
/// initialized the LSN yet (NULL on brand-new slots, rare race) we fall back
/// to 0 — pgwire-replication treats that as "server decides".
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
    let tls = params
        .native_tls_connector()
        .await
        .context(TlsConfigSnafu)?;

    let (client, conn_task) = if let Some(connector) = tls {
        let (client, connection) = cfg.connect(connector).await.context(SetupConnectSnafu)?;
        let task = tokio::spawn(async move {
            if let Err(e) = connection.await {
                tracing::warn!("postgres setup connection terminated: {e}");
            }
        });
        (client, task)
    } else {
        let (client, connection) = cfg.connect(NoTls).await.context(SetupConnectSnafu)?;
        let task = tokio::spawn(async move {
            if let Err(e) = connection.await {
                tracing::warn!("postgres setup connection terminated: {e}");
            }
        });
        (client, task)
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

    // Distinguish three catalog states for the named slot:
    //   * None            — no slot exists; we create one and need a bootstrap.
    //   * Some(0)         — slot exists but confirmed_flush_lsn is NULL. This
    //                       happens when a previous run crashed between slot
    //                       creation and the first StandbyStatusUpdate, i.e.
    //                       before any bootstrap rows were applied on the
    //                       accelerator. Treat it as bootstrap-required so we
    //                       don't silently skip the initial snapshot and leave
    //                       the accelerator missing historical rows.
    //   * Some(lsn) lsn>0 — normal resume from the durable checkpoint.
    match read_slot_confirmed_flush(client, &params.slot_name).await? {
        Some(confirmed_flush_lsn) if confirmed_flush_lsn != 0 => {
            tracing::info!(
                slot = %params.slot_name,
                publication = %params.publication_name,
                confirmed_flush_lsn = %format_lsn(confirmed_flush_lsn),
                "Resuming from existing replication slot"
            );
            return Ok(SlotInfo {
                slot_name: params.slot_name.clone(),
                publication_name: params.publication_name.clone(),
                consistent_lsn: confirmed_flush_lsn,
                snapshot_name: None,
                created_fresh: false,
            });
        }
        Some(_) => {
            tracing::warn!(
                slot = %params.slot_name,
                publication = %params.publication_name,
                "Existing replication slot has no confirmed_flush_lsn — \
                 treating as bootstrap-required so the accelerator is not \
                 left missing historical rows from a crashed first run"
            );
            return Ok(SlotInfo {
                slot_name: params.slot_name.clone(),
                publication_name: params.publication_name.clone(),
                consistent_lsn: 0,
                snapshot_name: None,
                created_fresh: true,
            });
        }
        None => {}
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
            ignore_duplicate_object(client.simple_query(&stmt).await)?;
        }
        return Ok(());
    }

    let stmt = format!(
        "CREATE PUBLICATION {pub} FOR TABLE {schema}.{table}",
        pub = quote_ident(publication_name),
        schema = quote_ident(schema_name),
        table = quote_ident(table_name),
    );
    // In multi-replica deployments two replicas can both observe `exists =
    // false` and race into `CREATE PUBLICATION` / `ALTER PUBLICATION`. The
    // loser gets SQLSTATE 42710 (`duplicate_object`); treat that as success
    // since the desired state is already achieved.
    ignore_duplicate_object(client.simple_query(&stmt).await)?;
    Ok(())
}

/// Treats a `duplicate_object` SQLSTATE (42710) as success — some replica beat
/// us to the publication/ALTER. Any other Postgres error is surfaced through
/// `SetupExecSnafu`.
fn ignore_duplicate_object<T>(
    res: std::result::Result<T, tokio_postgres::Error>,
) -> Result<Option<T>> {
    match res {
        Ok(v) => Ok(Some(v)),
        Err(e) => {
            if e.as_db_error()
                .is_some_and(|db| db.code().code() == "42710")
            {
                Ok(None)
            } else {
                Err(e).context(SetupExecSnafu)
            }
        }
    }
}

/// Look up an existing slot's `confirmed_flush_lsn`.
///
/// Returns:
/// - `Ok(None)` if no slot exists with that name — caller should create one.
/// - `Ok(Some(0))` if the slot exists but its catalog `confirmed_flush_lsn`
///   is NULL (brand-new slot before the first keepalive). This is rare but
///   valid; 0 is pgwire-replication's "server decides" sentinel so downstream
///   behavior is preserved.
/// - `Ok(Some(lsn))` with the catalog's LSN on the normal resume path.
async fn read_slot_confirmed_flush(
    client: &tokio_postgres::Client,
    slot_name: &str,
) -> Result<Option<u64>> {
    let row = client
        .query_opt(
            "SELECT confirmed_flush_lsn::text \
             FROM pg_replication_slots \
             WHERE slot_name = $1",
            &[&slot_name],
        )
        .await
        .context(SetupExecSnafu)?;
    let Some(row) = row else {
        return Ok(None);
    };
    let lsn_str: Option<String> = row.get(0);
    match lsn_str {
        Some(s) => Ok(Some(parse_lsn(&s)?)),
        None => Ok(Some(0)),
    }
}

/// Executes `CREATE_REPLICATION_SLOT` via a regular SQL function call rather
/// than the replication-connection command because we want the snapshot and
/// LSN to be usable from a normal (non-replication) connection afterwards for
/// the initial snapshot query.
///
/// Returns (`consistent_lsn`, `snapshot_name`).
async fn create_logical_slot(
    client: &tokio_postgres::Client,
    slot_name: &str,
    temporary: bool,
) -> Result<(u64, String)> {
    // `pg_create_logical_replication_slot(...)` has long returned
    // `(slot_name, lsn)` via SQL, and this query relies only on that stable
    // shape. The limitation here is not the returned columns but that the SQL
    // function path does not give us the equivalent of replication-protocol
    // `EXPORT_SNAPSHOT` for bootstrapping from an exported snapshot. We
    // capture the LSN as the starting position and use a REPEATABLE READ
    // transaction for the initial snapshot instead, which keeps this path
    // usable from a normal connection.
    let row = client
        .query_one(
            "SELECT slot_name, lsn::text FROM pg_create_logical_replication_slot($1, 'pgoutput', $2)",
            &[&slot_name, &temporary],
        )
        .await
        .map_err(|e| {
            // SQLSTATE 55000 (object_not_in_prerequisite_state) is returned by Postgres when
            // wal_level is not 'logical'. Surface a clear, actionable message instead of the
            // raw "logical replication not enabled" Postgres error.
            if e.as_db_error()
                .is_some_and(|db| db.code().code() == "55000")
            {
                return super::Error::LogicalReplicationNotEnabled;
            }
            super::Error::SetupExec { source: e }
        })?;

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
fn parse_lsn_or_zero(s: &str) -> u64 {
    parse_lsn(s).unwrap_or(0)
}

#[must_use]
pub fn format_lsn(lsn: u64) -> String {
    // Postgres LSN strings are intentionally "high32/low32" in hex, so truncating
    // the low 32 bits is exactly what we want here.
    #[expect(
        clippy::cast_possible_truncation,
        reason = "intentional: Postgres LSN format separates the low 32 bits"
    )]
    let low = lsn as u32;
    format!("{high:X}/{low:X}", high = lsn >> 32)
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
