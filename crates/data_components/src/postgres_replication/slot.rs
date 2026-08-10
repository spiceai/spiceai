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
    /// `GENERATED` columns of the source table. Postgres does not publish
    /// generated columns over logical replication (they are absent from
    /// pgoutput `Relation` messages), so the WAL path must tolerate their
    /// absence — the initial snapshot still captures their values, but
    /// replicated changes apply them as NULL.
    pub generated_columns: Vec<String>,
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

/// Setup outcome for one member table of a *shared* replication slot.
#[derive(Clone, Debug)]
pub struct SharedMemberSetup {
    pub slot: SlotInfo,
    /// Whether this call newly added the member's table to the shared
    /// publication (or created the publication with it). When `true`, the
    /// member's table has no WAL history on this slot before now and needs an
    /// initial snapshot.
    pub table_added: bool,
    /// `GENERATED` columns of this member's table — see
    /// [`SlotInfo::generated_columns`].
    pub generated_columns: Vec<String>,
    /// Every table in the shared publication as `(schema, table)`, read after
    /// this member was added. On a resuming slot this is the set of tables
    /// whose changes the slot is still accumulating, whether or not a dataset
    /// has subscribed yet — the caller holds the ack floor for the ones that
    /// have not, so their changes are not acked away before they join.
    pub publication_tables: Vec<(String, String)>,
}

/// Idempotent setup for one member of a shared slot: validates the table's
/// replica identity, adds it to the shared publication, and creates the slot
/// if this is the first member to arrive.
///
/// Unlike [`setup_slot_and_publication`], an existing slot is never treated as
/// owned by the member — `created_fresh` describes the slot itself, and the
/// caller combines it with `table_added` to decide whether this member needs
/// a snapshot.
pub async fn setup_shared_member(
    params: &ReplicationParams,
    schema_name: &str,
    table_name: &str,
) -> Result<SharedMemberSetup> {
    super::resilience::retry_async(
        "postgres_replication::setup_shared_member",
        super::resilience::DEFAULT_SETUP_MAX_ELAPSED,
        is_transient_setup_error,
        || async {
            let (client, conn_task) = connect_setup(params).await?;
            let outcome = async {
                validate_replica_identity(&client, schema_name, table_name).await?;
                let generated_columns =
                    fetch_generated_columns(&client, schema_name, table_name).await?;
                let table_added =
                    ensure_publication(&client, &params.publication_name, schema_name, table_name)
                        .await?;
                // After `ensure_publication`, which both adds this member's
                // table and repairs a publication missing
                // `publish_via_partition_root` — with that option set,
                // `pg_publication_tables` reports a partitioned table under its
                // root, which is the name members subscribe with.
                let publication_tables =
                    list_publication_tables(&client, &params.publication_name).await?;
                let slot = ensure_slot(&client, params).await?;
                Ok(SharedMemberSetup {
                    slot,
                    table_added,
                    generated_columns,
                    publication_tables,
                })
            }
            .await;
            drop(client);
            let _ = conn_task.await;
            outcome
        },
    )
    .await
}

async fn setup_once(
    params: &ReplicationParams,
    schema_name: &str,
    table_name: &str,
) -> Result<SlotInfo> {
    let (client, conn_task) = connect_setup(params).await?;

    let outcome = do_setup(&client, params, schema_name, table_name).await;

    drop(client);
    let _ = conn_task.await;

    outcome
}

/// Open a regular (non-replication) connection for setup queries, spawning the
/// connection driver task.
async fn connect_setup(
    params: &ReplicationParams,
) -> Result<(tokio_postgres::Client, tokio::task::JoinHandle<()>)> {
    let cfg = params.setup_pg_config();
    let tls = params
        .native_tls_connector()
        .await
        .context(TlsConfigSnafu)?;

    if let Some(connector) = tls {
        let (client, connection) = cfg.connect(connector).await.context(SetupConnectSnafu)?;
        let task = tokio::spawn(async move {
            if let Err(e) = connection.await {
                tracing::warn!("postgres setup connection terminated: {e}");
            }
        });
        Ok((client, task))
    } else {
        let (client, connection) = cfg.connect(NoTls).await.context(SetupConnectSnafu)?;
        let task = tokio::spawn(async move {
            if let Err(e) = connection.await {
                tracing::warn!("postgres setup connection terminated: {e}");
            }
        });
        Ok((client, task))
    }
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
    let generated_columns = fetch_generated_columns(client, schema_name, table_name).await?;
    ensure_publication(client, &params.publication_name, schema_name, table_name).await?;
    let mut slot = ensure_slot(client, params).await?;
    slot.generated_columns = generated_columns;
    Ok(slot)
}

/// List the table's `GENERATED` columns (`pg_attribute.attgenerated`).
/// Postgres omits these from pgoutput `Relation` messages, so the WAL path
/// needs to know which dataset columns to expect to be absent.
async fn fetch_generated_columns(
    client: &tokio_postgres::Client,
    schema_name: &str,
    table_name: &str,
) -> Result<Vec<String>> {
    let rows = client
        .query(
            "SELECT a.attname \
             FROM pg_attribute a \
             JOIN pg_class c ON c.oid = a.attrelid \
             JOIN pg_namespace n ON n.oid = c.relnamespace \
             WHERE n.nspname = $1 AND c.relname = $2 \
               AND a.attnum > 0 AND NOT a.attisdropped \
               AND a.attgenerated::text <> ''",
            &[&schema_name, &table_name],
        )
        .await
        .context(SetupExecSnafu)?;
    Ok(rows.iter().map(|r| r.get::<_, String>(0)).collect())
}

/// Look up the named slot, creating it if absent. See [`setup_slot_and_publication`]
/// for the semantics of the returned `consistent_lsn` / `created_fresh`.
async fn ensure_slot(
    client: &tokio_postgres::Client,
    params: &ReplicationParams,
) -> Result<SlotInfo> {
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
            // An accelerator that boots empty is about to re-snapshot the whole
            // table, so every byte of WAL between the pre-restart checkpoint and
            // that snapshot is redundant -- after a long outage, that is the
            // entire downtime, decoded and applied only to be overwritten by the
            // snapshot. Skip it by moving the slot forward first.
            if let Some(advanced_lsn) = advance_slot_for_rebootstrap(client, params).await {
                return Ok(SlotInfo {
                    slot_name: params.slot_name.clone(),
                    publication_name: params.publication_name.clone(),
                    consistent_lsn: advanced_lsn,
                    snapshot_name: None,
                    generated_columns: Vec::new(),
                    // The old history is gone, so this slot carries none for any
                    // member -- the same contract as a slot created this process.
                    created_fresh: true,
                });
            }

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
                generated_columns: Vec::new(),
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
                generated_columns: Vec::new(),
                created_fresh: true,
            });
        }
        None => {}
    }

    let (consistent_lsn, snapshot_name) = match create_logical_slot(client, &params.slot_name).await
    {
        Ok(created) => created,
        // Lost a creation race: another consumer (a second replica, or a
        // concurrent stream on an older build) created the slot between
        // our catalog read and the CREATE. The slot exists now — treat it
        // exactly like the existing-slot paths above instead of failing
        // the dataset with "replication slot already exists".
        Err(e) if is_duplicate_slot_error(&e) => {
            tracing::info!(
                slot = %params.slot_name,
                "Lost replication-slot creation race; resuming from the winner's slot"
            );
            let confirmed = read_slot_confirmed_flush(client, &params.slot_name)
                .await?
                .unwrap_or(0);
            return Ok(SlotInfo {
                slot_name: params.slot_name.clone(),
                publication_name: params.publication_name.clone(),
                consistent_lsn: confirmed,
                snapshot_name: None,
                // A raced slot with no durable checkpoint yet still needs
                // a bootstrap (same reasoning as the Some(0) path above).
                generated_columns: Vec::new(),
                created_fresh: confirmed == 0,
            });
        }
        Err(e) => return Err(e),
    };

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
        generated_columns: Vec::new(),
        created_fresh: true,
    })
}

/// SQLSTATE `55006` (`object_in_use`) — the slot is still held by an active
/// walsender, so it cannot be advanced or dropped.
const SQLSTATE_OBJECT_IN_USE_ADVANCE: &str = "55006";
/// SQLSTATE `42883` (`undefined_function`) — `pg_replication_slot_advance` is
/// `PostgreSQL` 11+. On an older server the advance is simply skipped.
const SQLSTATE_UNDEFINED_FUNCTION: &str = "42883";

/// Move `params.slot_name` forward to the current WAL position, for a member
/// that is about to re-snapshot anyway, and return the LSN streaming should
/// start from. `None` means "do not advance" — the caller resumes from
/// `confirmed_flush_lsn` exactly as before.
///
/// The win is skipping *re-delivery*: after an outage the slot's checkpoint can
/// be hours behind, and every one of those changes would be decoded and applied
/// only to be overwritten by the snapshot. Advancing sets `confirmed_flush_lsn`
/// to the current position so `START_REPLICATION` never re-reads them. Note that
/// `restart_lsn` — what actually governs WAL *retention* on the source — trails
/// `confirmed_flush_lsn` and catches up on subsequent slot activity rather than
/// immediately, so this is not a prompt way to release retained WAL (dropping
/// the slot at shutdown is; see [`drop_slot_after_shutdown`]).
///
/// # Why this is safe
///
/// Streaming must start at an LSN **at or before** the bootstrap snapshot's
/// visibility point. Undershooting only replays WAL the snapshot already
/// covers, which the primary-key upsert absorbs; overshooting would silently
/// skip changes. This reads `pg_current_wal_lsn()` and advances *before* the
/// caller opens its `REPEATABLE READ` bootstrap transaction (`ensure_slot` runs
/// inside setup, which strictly precedes bootstrap), so the snapshot is always
/// taken at or after the returned LSN.
///
/// # Why both gate conditions are required
///
/// * `ephemeral_accelerator` — the accelerator starts empty, so the snapshot
///   reconstructs the entire table. There is no pre-existing accelerator state
///   for the upsert to merge into, and therefore no rows deleted at the source
///   during the outage that could survive it. A *durable* accelerator fails this:
///   its snapshot merges into existing rows, so discarding the WAL that carried
///   the deletes would leave them behind.
/// * `snapshot_on_resume` — a snapshot is definitely going to run. Advancing
///   without one would skip WAL with nothing to fill the gap.
///
/// On a shared slot the advance discards history for every member;
/// [`super::Error::SharedSlotDurabilityMismatch`] keeps a durable member off such
/// a slot in the first place.
async fn advance_slot_for_rebootstrap(
    client: &tokio_postgres::Client,
    params: &ReplicationParams,
) -> Option<u64> {
    if !params.slot_is_disposable() {
        return None;
    }

    // Read the target BEFORE advancing (and before the caller's bootstrap
    // transaction), so the snapshot can only ever be at or after it.
    let target = match current_wal_lsn(client).await {
        Ok(lsn) => lsn,
        Err(e) => {
            tracing::warn!(
                slot = %params.slot_name,
                "could not read the current WAL position to fast-forward the replication slot; \
                 resuming from the existing checkpoint and replaying the backlog instead: {e}"
            );
            return None;
        }
    };

    match slot_advance(client, &params.slot_name, target).await {
        Ok(end_lsn) => {
            tracing::info!(
                slot = %params.slot_name,
                advanced_to = %format_lsn(end_lsn),
                "Fast-forwarded the replication slot past the accumulated backlog: this \
                 accelerator starts empty and re-snapshots on every start, so the skipped WAL \
                 would only have been overwritten by the snapshot"
            );
            Some(end_lsn)
        }
        Err(e) => {
            // Every failure falls back to a plain resume, which is exactly the
            // previous behavior: correct, just slower.
            let sqlstate = match &e {
                super::Error::SetupExec { source } => {
                    source.as_db_error().map(|db| db.code().code().to_string())
                }
                _ => None,
            };
            match sqlstate.as_deref() {
                // A later member joining a slot the pump is already streaming.
                // Nothing to fast-forward past -- the slot is current.
                Some(SQLSTATE_OBJECT_IN_USE_ADVANCE) => tracing::debug!(
                    slot = %params.slot_name,
                    "replication slot is active; skipping the fast-forward"
                ),
                Some(SQLSTATE_UNDEFINED_FUNCTION) => tracing::debug!(
                    slot = %params.slot_name,
                    "pg_replication_slot_advance is unavailable (PostgreSQL 11+); \
                     resuming from the existing checkpoint"
                ),
                _ => tracing::warn!(
                    slot = %params.slot_name,
                    "could not fast-forward the replication slot; resuming from the existing \
                     checkpoint and replaying the backlog instead: {e}"
                ),
            }
            None
        }
    }
}

/// The server's current WAL insert position.
async fn current_wal_lsn(client: &tokio_postgres::Client) -> Result<u64> {
    let row = client
        .query_one("SELECT pg_current_wal_lsn()::text", &[])
        .await
        .context(SetupExecSnafu)?;
    parse_lsn(&row.get::<_, String>(0))
}

/// `pg_replication_slot_advance`, returning the position the server actually
/// moved to — it may stop short of `target`, and that value (never `target`) is
/// what streaming must start from.
async fn slot_advance(
    client: &tokio_postgres::Client,
    slot_name: &str,
    target: u64,
) -> Result<u64> {
    let target_lsn = format_lsn(target);
    // Both arguments are cast explicitly: the function takes (name, pg_lsn), and
    // binding Rust `String`s without the casts leaves parameter-type inference to
    // resolve `text` against those, which fails.
    let row = client
        .query_one(
            "SELECT end_lsn::text FROM pg_replication_slot_advance($1::name, $2::pg_lsn)",
            &[&slot_name, &target_lsn],
        )
        .await
        .context(SetupExecSnafu)?;
    parse_lsn(&row.get::<_, String>(0))
}

/// SQLSTATE 42710 (`duplicate_object`) from `pg_create_logical_replication_slot`
/// — someone else created the slot first.
fn is_duplicate_slot_error(e: &super::Error) -> bool {
    matches!(
        e,
        super::Error::SetupExec { source }
            if source.as_db_error().is_some_and(|db| db.code().code() == "42710")
    )
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

/// Create the publication / add the table to it as needed.
///
/// Returns `true` when the table was *not* already a member of the publication
/// before this call (i.e. we created the publication with it, or `ALTER
/// PUBLICATION ... ADD TABLE`d it). Shared-slot members use this to decide
/// whether they need an initial snapshot: a table newly added to the
/// publication has no WAL history on the slot.
async fn ensure_publication(
    client: &tokio_postgres::Client,
    publication_name: &str,
    schema_name: &str,
    table_name: &str,
) -> Result<bool> {
    let exists: bool = client
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM pg_publication WHERE pubname = $1)",
            &[&publication_name],
        )
        .await
        .context(SetupExecSnafu)?
        .get(0);

    if exists {
        // Repair publications created by an older Spice build (or a DBA)
        // without `publish_via_partition_root`. Without it, pgoutput attributes
        // a partitioned table's changes to each leaf partition, which the
        // shared router — keyed by the parent's name — silently drops (#11290).
        ensure_publish_via_partition_root(client, publication_name).await?;

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
            // A lost 42710 race still means the table is newly in the
            // publication relative to this slot's history — report `true`
            // either way so the caller's snapshot decision stays safe.
            ignore_duplicate_object(client.simple_query(&stmt).await)?;
        }
        return Ok(!has_table);
    }

    let stmt = format!(
        // `publish_via_partition_root = true` makes pgoutput report a
        // partitioned table's changes under the parent relation (matching the
        // shared router's parent-keyed members and the parent-table initial
        // snapshot) instead of each leaf partition. No effect on regular
        // tables. See #11290.
        "CREATE PUBLICATION {pub} FOR TABLE {schema}.{table} \
         WITH (publish_via_partition_root = true)",
        pub = quote_ident(publication_name),
        schema = quote_ident(schema_name),
        table = quote_ident(table_name),
    );
    // In multi-replica deployments two replicas can both observe `exists =
    // false` and race into `CREATE PUBLICATION`. The loser gets SQLSTATE
    // 42710 (`duplicate_object`) — but on a *shared* publication the winner
    // may have created it for a different table, so losing the race does NOT
    // imply our table is a member. Enforce membership explicitly with a
    // follow-up ADD TABLE (itself 42710-tolerant for the same-table race).
    if ignore_duplicate_object(client.simple_query(&stmt).await)?.is_none() {
        let add = format!(
            "ALTER PUBLICATION {pub} ADD TABLE {schema}.{table}",
            pub = quote_ident(publication_name),
            schema = quote_ident(schema_name),
            table = quote_ident(table_name),
        );
        ignore_duplicate_object(client.simple_query(&add).await)?;
    }
    Ok(true)
}

/// Ensure an existing publication publishes partitioned-table changes under the
/// root (parent) relation. `CREATE PUBLICATION` sets this for publications we
/// create; this repairs a publication created without it by an older Spice
/// build or a DBA. It is a no-op once the option is set, and harmless for
/// publications carrying only regular tables.
///
/// The `ALTER` requires ownership of the publication. When Spice does not own a
/// pre-created publication the alter is skipped with a warning rather than
/// failing setup: leaving `publish_via_partition_root` off is only a problem
/// for partitioned source tables, so a non-owner with regular tables keeps
/// working exactly as before.
async fn ensure_publish_via_partition_root(
    client: &tokio_postgres::Client,
    publication_name: &str,
) -> Result<()> {
    let via_root: bool = client
        .query_one(
            "SELECT pubviaroot FROM pg_publication WHERE pubname = $1",
            &[&publication_name],
        )
        .await
        .context(SetupExecSnafu)?
        .get(0);
    if via_root {
        return Ok(());
    }

    let stmt = format!(
        "ALTER PUBLICATION {pub} SET (publish_via_partition_root = true)",
        pub = quote_ident(publication_name),
    );
    match client.simple_query(&stmt).await {
        Ok(_) => Ok(()),
        // 42501 insufficient_privilege: a publication Spice does not own.
        // Don't fail setup — only partitioned sources need the option.
        Err(e)
            if e.as_db_error()
                .is_some_and(|db| db.code().code() == "42501") =>
        {
            tracing::warn!(
                publication = %publication_name,
                "Cannot set publish_via_partition_root on a publication Spice \
                 does not own; changes to partitioned source tables may be \
                 dropped. Recreate the publication with \
                 WITH (publish_via_partition_root = true) or grant ownership."
            );
            Ok(())
        }
        Err(e) => Err(e).context(SetupExecSnafu),
    }
}

/// Every table in a publication, as `(schema, table)`. An absent publication
/// yields an empty list.
async fn list_publication_tables(
    client: &tokio_postgres::Client,
    publication_name: &str,
) -> Result<Vec<(String, String)>> {
    let rows = client
        .query(
            "SELECT schemaname, tablename FROM pg_publication_tables WHERE pubname = $1",
            &[&publication_name],
        )
        .await
        .context(SetupExecSnafu)?;
    Ok(rows
        .iter()
        .map(|row| (row.get::<_, String>(0), row.get::<_, String>(1)))
        .collect())
}

/// Best-effort removal of a table from a (shared) publication. Used when a
/// member detaches while its initial snapshot is still running: tearing the
/// table out of the publication forces any future rejoin — in-process or after
/// a restart — back through the ADD TABLE + fresh-snapshot path, instead of
/// resuming over an accelerator that is missing base rows.
///
/// "Already absent" outcomes (publication or membership gone) are success.
pub async fn remove_table_from_publication(
    params: &ReplicationParams,
    schema_name: &str,
    table_name: &str,
) -> Result<()> {
    let (client, conn_task) = connect_setup(params).await?;
    let stmt = format!(
        "ALTER PUBLICATION {pub} DROP TABLE {schema}.{table}",
        pub = quote_ident(&params.publication_name),
        schema = quote_ident(schema_name),
        table = quote_ident(table_name),
    );
    let outcome = match client.simple_query(&stmt).await {
        Ok(_) => Ok(()),
        // 42704 undefined_object: table is not a member; 42P01 undefined_table:
        // the publication (or table) no longer exists. Both mean the desired
        // state — "table not published" — already holds.
        Err(e)
            if e.as_db_error()
                .is_some_and(|db| matches!(db.code().code(), "42704" | "42P01")) =>
        {
            Ok(())
        }
        Err(e) => Err(e).context(SetupExecSnafu),
    };
    drop(client);
    let _ = conn_task.await;
    outcome
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
/// The slot is always durable, which is a consequence of creating it here. A
/// temporary slot is owned by the session that creates it, and this is the
/// setup connection, which closes before the replication connection issues
/// `START_REPLICATION` — Postgres would drop the slot in between, leaving
/// nothing to attach to. Supporting a temporary slot would mean creating it
/// over the replication protocol (`CREATE_REPLICATION_SLOT … TEMPORARY`),
/// recreating it on every reconnect, and re-snapshotting every member each
/// time, since a session-scoped slot does not survive a transient blip.
///
/// Returns (`consistent_lsn`, `snapshot_name`).
async fn create_logical_slot(
    client: &tokio_postgres::Client,
    slot_name: &str,
) -> Result<(u64, String)> {
    /// Bound as the `temporary` argument below.
    const DURABLE_SLOT: bool = false;

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
            &[&slot_name, &DURABLE_SLOT],
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

/// Total time [`drop_slot_after_shutdown`] will spend waiting for the server to
/// mark a just-released slot inactive. `PostgreSQL` clears the flag within
/// milliseconds of the walsender exiting; this only covers scheduling jitter,
/// and is deliberately short so a slow or unreachable source cannot stall a
/// graceful shutdown.
const DROP_SLOT_BUDGET: std::time::Duration = std::time::Duration::from_secs(5);
/// How long to wait for the setup connection used to drop the slot. Separate
/// from [`DROP_SLOT_BUDGET`], which only covers the retry loop *after* a
/// connection exists; without this an unreachable source would stall shutdown
/// for the OS connect timeout.
const DROP_SLOT_CONNECT_BUDGET: std::time::Duration = std::time::Duration::from_secs(5);
/// How often to retry the drop while the server still reports the slot active.
const DROP_SLOT_POLL_INTERVAL: std::time::Duration = std::time::Duration::from_millis(250);
/// SQLSTATE `55006` (`object_in_use`) — the slot is still marked active because
/// the walsender we just disconnected has not fully exited yet.
const SQLSTATE_OBJECT_IN_USE: &str = "55006";
/// SQLSTATE `42704` (`undefined_object`) — the slot is already gone.
const SQLSTATE_UNDEFINED_OBJECT: &str = "42704";

/// Drop `params.slot_name` on graceful shutdown, for a stream whose accelerator
/// does not survive a restart (see [`ReplicationParams::ephemeral_accelerator`]).
///
/// Such a slot has no resume value — the accelerator boots empty and re-snapshots
/// — but left behind it keeps pinning WAL on the source for as long as Spice is
/// down. Dropping it releases that WAL immediately.
///
/// Best-effort by construction: shutdown must not block on the source, and a slot
/// that survives (ungraceful exit, unreachable server, insufficient privilege)
/// costs only retained WAL, never correctness — the next start re-snapshots
/// either way. Every failure is therefore logged, not propagated.
///
/// Bounded end to end: [`DROP_SLOT_CONNECT_BUDGET`] caps establishing the
/// connection and [`DROP_SLOT_BUDGET`] caps the retry loop after it, so an
/// unreachable source delays shutdown by at most their sum. Bounding only the
/// retries would leave the connect itself free to hang for the OS timeout —
/// exactly the case where the source is gone and this cleanup matters least.
///
/// Call only *after* the replication connection has been dropped; `PostgreSQL`
/// refuses to drop a slot an active walsender still holds.
pub async fn drop_slot_after_shutdown(params: &ReplicationParams) {
    // Bound the connect explicitly. Everything below is governed by
    // `DROP_SLOT_BUDGET`, but that budget only starts once a connection exists —
    // and a source that is unreachable at shutdown (the very case in which this
    // cleanup matters least) would otherwise hang here for the OS connect
    // timeout, blocking the pump's exit. Shutdown must never wait on the source.
    let connected = tokio::time::timeout(DROP_SLOT_CONNECT_BUDGET, connect_setup(params)).await;
    let (client, connection_task) = match connected {
        Ok(Ok(pair)) => pair,
        Ok(Err(e)) => {
            tracing::warn!(
                slot = %params.slot_name,
                "could not connect to drop the replication slot on shutdown; it will keep retaining WAL on the source until dropped manually: {e}"
            );
            return;
        }
        Err(_) => {
            tracing::warn!(
                slot = %params.slot_name,
                "timed out after {}s connecting to drop the replication slot on shutdown; it will keep retaining WAL on the source until dropped manually (DROP: `SELECT pg_drop_replication_slot('{}')`)",
                DROP_SLOT_CONNECT_BUDGET.as_secs(),
                params.slot_name,
            );
            return;
        }
    };

    let deadline = tokio::time::Instant::now() + DROP_SLOT_BUDGET;
    loop {
        let Err(error) = client
            .execute("SELECT pg_drop_replication_slot($1)", &[&params.slot_name])
            .await
        else {
            tracing::info!(
                slot = %params.slot_name,
                "dropped the replication slot on shutdown (non-persistent accelerator; the slot has no resume value and would otherwise retain WAL on the source)"
            );
            break;
        };

        match error.as_db_error().map(|db| db.code().code()) {
            // Already gone — a concurrent drop raced us.
            Some(SQLSTATE_UNDEFINED_OBJECT) => break,
            // The walsender has not finished exiting; retry within the budget.
            Some(SQLSTATE_OBJECT_IN_USE) if tokio::time::Instant::now() < deadline => {
                tokio::time::sleep(DROP_SLOT_POLL_INTERVAL).await;
            }
            _ => {
                // `pg_error_detail` rather than `{error}`: tokio_postgres renders
                // a server error as the opaque string "db error", which would
                // leave this line -- the only diagnostic an operator gets for a
                // slot still retaining WAL -- with nothing actionable in it.
                tracing::warn!(
                    slot = %params.slot_name,
                    "could not drop the replication slot on shutdown; it will keep retaining WAL on the source until dropped manually (DROP: `SELECT pg_drop_replication_slot('{}')`): {}",
                    params.slot_name,
                    super::pg_error_detail(&error),
                );
                break;
            }
        }
    }

    drop(client);
    connection_task.abort();
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

    /// The advance target is bound as `pg_lsn`, so it must round-trip through
    /// the exact textual form Postgres accepts.
    #[test]
    fn advance_target_renders_as_a_pg_lsn_literal() {
        let target = parse_lsn("1B/4E300F8").expect("parse");
        assert_eq!(format_lsn(target), "1B/4E300F8");
    }

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
