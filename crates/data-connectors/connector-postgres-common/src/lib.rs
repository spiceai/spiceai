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

//! Shared `PostgreSQL` catalog/schema introspection utilities.
//!
//! Kept in a lightweight, standalone crate (only `tokio-postgres`,
//! `datafusion-common`, and `datafusion-table-providers`'s connection pool --
//! no `data_components`, no `runtime`) so it can be depended on by every
//! crate that needs the same `information_schema`/`pg_catalog` queries
//! (currently `data_components::postgres::provider`, which
//! `runtime::catalogconnector::postgres_accelerated` uses in turn) without
//! pulling in the full `TableProvider`/connector machinery those crates carry
//! -- and without `runtime` ever needing to depend on a `connector-*` crate
//! directly.

use datafusion_common::utils::quote_identifier;
use datafusion_table_providers::sql::db_connection_pool::postgrespool::PostgresConnectionPool;
use snafu::prelude::*;

/// Connection parameters and role grants behave identically for a dataset using
/// the `PostgreSQL` data connector, and are documented with the connector, so the
/// connector's page is the one that answers these for a catalog user too -- the
/// same page the CDC prerequisite errors below already link.
const POSTGRES_CONNECTOR_DOCS: &str =
    "https://spiceai.org/docs/components/data-connectors/postgres";

/// Every variant is worded to read as the `Cause:` clause of the message that
/// reports it: the caller states the problem -- naming the catalog, schema or
/// table and the step that failed -- and its impact, and these supply the
/// specific failure and its fix. A variant that named a resource or a step
/// itself would duplicate the caller's.
#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Failed to connect to PostgreSQL: {source}. Check the `pg_host`, `pg_port`, `pg_user`, `pg_pass` and `pg_sslmode` parameters, and that the database is reachable from Spice. Docs: {POSTGRES_CONNECTOR_DOCS}"
    ))]
    ConnectionFailed {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    // The queries this reports are Spice's own discovery queries against
    // `information_schema` and `pg_catalog`, never anything the user wrote, so
    // the fix is a grant -- not "check your SQL". The step being performed is
    // named by the message reporting this, and the relation the server objected
    // to by `source`; the query text itself is debug detail and stays out.
    #[snafu(display(
        "A PostgreSQL query failed: {source}. Check that the connected role can read `information_schema` and `pg_catalog`. Docs: {POSTGRES_CONNECTOR_DOCS}"
    ))]
    QueryFailed { source: tokio_postgres::Error },

    #[snafu(display(
        "Cannot start CDC catalog acceleration: PostgreSQL `wal_level` is '{wal_level}', but 'logical' is required. Run `ALTER SYSTEM SET wal_level = 'logical';` and restart PostgreSQL. Docs: https://spiceai.org/docs/components/data-connectors/postgres"
    ))]
    WalLevelNotLogical { wal_level: String },

    #[snafu(display(
        "Cannot start CDC catalog acceleration: PostgreSQL role '{role}' is not permitted to start replication. Grant it with `ALTER ROLE \"{role}\" REPLICATION;`, or connect as a superuser. Docs: https://spiceai.org/docs/components/data-connectors/postgres"
    ))]
    MissingReplicationPrivilege { role: String },

    #[snafu(display(
        "Cannot start CDC catalog acceleration: PostgreSQL has no free replication slots ({used} of {max} in use; `max_replication_slots` = {max}). Drop an unused slot (inspect `pg_replication_slots`, then `SELECT pg_drop_replication_slot('<slot_name>');`), or raise `max_replication_slots` and restart PostgreSQL. Docs: https://spiceai.org/docs/components/data-connectors/postgres"
    ))]
    ReplicationSlotsExhausted { used: i64, max: i64 },

    #[snafu(display(
        "PostgreSQL table {schema}.{table} was not found. It may have been dropped after catalog discovery; it will be retried on the next refresh. Docs: https://spiceai.org/docs/components/data-connectors/postgres"
    ))]
    TableNotFound { schema: String, table: String },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// System schemas to exclude from discovery.
const SYSTEM_SCHEMAS: &[&str] = &["information_schema", "pg_catalog", "pg_toast"];

/// Query `information_schema.schemata` for all user schemas, excluding system
/// schemas (`information_schema`, `pg_catalog`, `pg_toast`, `pg_temp*`).
///
/// # Errors
///
/// Returns an error if a connection can't be obtained from `pool`, or the
/// query fails.
pub async fn list_schemas(pool: &PostgresConnectionPool) -> Result<Vec<String>> {
    let conn = pool.connect_direct().await.context(ConnectionFailedSnafu)?;

    let rows = conn
        .conn
        .query(
            "SELECT schema_name FROM information_schema.schemata ORDER BY schema_name",
            &[],
        )
        .await
        .context(QueryFailedSnafu)?;

    let names: Vec<String> = rows
        .iter()
        .filter_map(|row| {
            let name: String = row.get(0);
            if SYSTEM_SCHEMAS.contains(&name.as_str()) || name.starts_with("pg_temp") {
                None
            } else {
                Some(name)
            }
        })
        .collect();

    Ok(names)
}

/// Query `pg_catalog.pg_class` for the relations in `schema_name`.
///
/// When `include_views` is true, view-like relations (views, materialized
/// views, foreign tables) are returned alongside ordinary and partitioned
/// tables -- the set a read-only schema provider can serve as federated
/// tables. When false, only CDC-able base tables (ordinary `r` and
/// partitioned-parent `p`) are returned, since views, materialized views, and
/// foreign tables can't be primary-keyed or CDC-accelerated.
///
/// Discovery goes through `pg_catalog.pg_class` rather than
/// `information_schema.tables` (#11725): `information_schema` omits
/// materialized views (relkind 'm') entirely and reports foreign tables under
/// a separate `table_type`, so an `information_schema`-based query silently
/// dropped both. Only relations the current role holds `SELECT` on
/// (`has_table_privilege`) are returned, so a schema provider never registers
/// a relation it can't actually read.
///
/// A declaratively-partitioned parent (relkind 'p') and every one of its leaf
/// partitions (relkind 'r') would otherwise both be discovered. Registering
/// both the parent and its children would double-count the data (the parent is
/// a union over its children) and clutter the catalog for tables with many
/// partitions (#11726). It would also diverge from how the CDC path treats
/// these tables: Spice publishes partitioned-table changes under the parent
/// relation (`publish_via_partition_root = true`, see
/// `postgres_replication::slot`), so the parent is the coherent unit either
/// way. We therefore exclude any relation that is a child in `pg_inherits`
/// (covering both declarative partitions and legacy table inheritance) and
/// keep only the parent. The `pg_inherits` catalog exists on every supported
/// `PostgreSQL` version and on Redshift (where it is empty), so this degrades
/// to the prior behaviour on engines without partitioning.
///
/// # Errors
///
/// Returns an error if a connection can't be obtained from `pool`, or the
/// query fails.
pub async fn list_tables(
    pool: &PostgresConnectionPool,
    schema_name: &str,
    include_views: bool,
) -> Result<Vec<String>> {
    let conn = pool.connect_direct().await.context(ConnectionFailedSnafu)?;

    // 'r' = ordinary table, 'p' = partitioned-table parent (both CDC-able);
    // 'v' = view, 'm' = materialized view, 'f' = foreign table (view-like,
    // read-only, not CDC-able).
    let relkinds: &[&str] = if include_views {
        &["r", "p", "v", "m", "f"]
    } else {
        &["r", "p"]
    };

    let rows = conn
        .conn
        .query(
            "SELECT c.relname FROM pg_catalog.pg_class c \
             JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace \
             WHERE n.nspname = $1 \
             AND c.relkind::text = ANY($2) \
             AND pg_catalog.has_table_privilege(c.oid, 'SELECT') \
             AND NOT EXISTS ( \
                 SELECT 1 FROM pg_catalog.pg_inherits inh \
                 WHERE inh.inhrelid = c.oid \
             ) \
             ORDER BY c.relname",
            &[&schema_name, &relkinds],
        )
        .await
        .context(QueryFailedSnafu)?;

    let names: Vec<String> = rows.iter().map(|row| row.get(0)).collect();
    Ok(names)
}

/// A view-like relation that cannot be CDC-accelerated, paired with a
/// human-readable label for the kind of relation it is.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ViewRelation {
    /// The relation name.
    pub name: String,
    /// A human-readable label for its `pg_class.relkind` (`view`,
    /// `materialized view`, or `foreign table`).
    pub kind: &'static str,
}

/// Query `pg_catalog.pg_class` for the view-like relations (views `v`,
/// materialized views `m`, foreign tables `f`) in `schema_name` -- exactly the
/// relations [`list_tables`] with `include_views = false` deliberately omits
/// because they cannot be primary-keyed or CDC-accelerated.
///
/// The accelerated catalog uses this to *warn* that these relations will not be
/// replicated, rather than dropping them silently. Uses the same
/// `has_table_privilege` filter as [`list_tables`] so it never names a relation
/// the current role cannot read.
///
/// # Errors
///
/// Returns an error if a connection can't be obtained from `pool`, or the
/// query fails.
pub async fn list_views(
    pool: &PostgresConnectionPool,
    schema_name: &str,
) -> Result<Vec<ViewRelation>> {
    let conn = pool.connect_direct().await.context(ConnectionFailedSnafu)?;

    // 'v' = view, 'm' = materialized view, 'f' = foreign table -- the view-like
    // relations `list_tables` omits. Bound to a named slice and passed by
    // reference, matching `list_tables`' relkind pattern.
    let relkinds: &[&str] = &["v", "m", "f"];
    let rows = conn
        .conn
        .query(
            "SELECT c.relname, c.relkind::text FROM pg_catalog.pg_class c \
             JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace \
             WHERE n.nspname = $1 \
             AND c.relkind::text = ANY($2) \
             AND pg_catalog.has_table_privilege(c.oid, 'SELECT') \
             ORDER BY c.relname",
            &[&schema_name, &relkinds],
        )
        .await
        .context(QueryFailedSnafu)?;

    let views: Vec<ViewRelation> = rows
        .iter()
        .map(|row| {
            let name: String = row.get(0);
            let relkind: String = row.get(1);
            let kind = match relkind.as_str() {
                "m" => "materialized view",
                "f" => "foreign table",
                // "v" and any forward-compatibility surprise map to the generic
                // label; the query only ever returns v/m/f.
                _ => "view",
            };
            ViewRelation { name, kind }
        })
        .collect();

    Ok(views)
}

/// Query `pg_catalog` for the primary-key columns of `schema_name.table_name`,
/// in key order. Empty when the table has no primary key.
///
/// This is deliberately a minimal, self-contained lookup (primary key only) —
/// not the fuller index/sort/statistics inference `connector-postgres` does
/// for a dataset's own schema-inference pipeline.
///
/// # Errors
///
/// Returns an error if a connection can't be obtained from `pool`, or the
/// query fails.
pub async fn primary_key_columns(
    pool: &PostgresConnectionPool,
    schema_name: &str,
    table_name: &str,
) -> Result<Vec<String>> {
    let conn = pool.connect_direct().await.context(ConnectionFailedSnafu)?;
    // `to_regclass` parses its argument as a (possibly qualified, possibly
    // quoted) SQL identifier, not a literal string match -- an unquoted
    // mixed-case or special-character name would resolve to the wrong
    // relation (or nothing), so each component is quoted the same way
    // `TableReference` quoting round-trips identifiers elsewhere in the
    // codebase (see #11727).
    let table_path = format!(
        "{}.{}",
        quote_identifier(schema_name),
        quote_identifier(table_name)
    );

    // `indkey` is an `int2vector`, not a plain array, so it's converted via
    // `string_to_array(...)::int2[]` before unnesting `WITH ORDINALITY` to
    // preserve key-column order — the same technique used by
    // `connector-postgres`'s fuller schema-inference query.
    let rows = conn
        .conn
        .query(
            "SELECT a.attname AS column_name \
             FROM pg_catalog.pg_index ix \
             JOIN LATERAL unnest(string_to_array(ix.indkey::text, ' ')::int2[]) \
                 WITH ORDINALITY AS k(attnum, ord) ON true \
             JOIN pg_catalog.pg_attribute a \
                 ON a.attrelid = ix.indrelid \
                 AND a.attnum = k.attnum \
             WHERE ix.indrelid = to_regclass($1) \
             AND ix.indisprimary \
             ORDER BY k.ord",
            &[&table_path],
        )
        .await
        .context(QueryFailedSnafu)?;

    let columns: Vec<String> = rows.iter().map(|row| row.get(0)).collect();
    Ok(columns)
}

/// Validate the `PostgreSQL` prerequisites CDC catalog acceleration needs
/// before discovering or accelerating any tables, returning a specific,
/// actionable error naming the exact fix: `wal_level = logical`, and the
/// connecting role can start replication (either `REPLICATION` directly, or
/// transitively via superuser).
///
/// Deliberately just a clear pass/fail at the connection level -- not a
/// per-table CDC-readiness report (e.g. `REPLICA IDENTITY`), which is out
/// of scope for now.
///
/// # Errors
///
/// Returns an error if a connection can't be obtained from `pool`, a query
/// fails, `wal_level` is not `logical`, or the connecting role can't start
/// replication.
pub async fn check_cdc_prerequisites(pool: &PostgresConnectionPool) -> Result<()> {
    let conn = pool.connect_direct().await.context(ConnectionFailedSnafu)?;

    let wal_level: String = conn
        .conn
        .query_one("SHOW wal_level", &[])
        .await
        .context(QueryFailedSnafu)?
        .get(0);
    ensure!(
        wal_level == "logical",
        WalLevelNotLogicalSnafu { wal_level }
    );

    let row = conn
        .conn
        .query_one(
            "SELECT current_user::text, (rolreplication OR rolsuper) \
             FROM pg_roles WHERE rolname = current_user",
            &[],
        )
        .await
        .context(QueryFailedSnafu)?;
    let role: String = row.get(0);
    let can_replicate: bool = row.get(1);
    ensure!(can_replicate, MissingReplicationPrivilegeSnafu { role });

    Ok(())
}

/// The activity status of a `PostgreSQL` replication slot, read from
/// `pg_catalog.pg_replication_slots`. `active` is true while a consumer holds
/// the slot; `active_pid` is that consumer's backend PID when the server exposes
/// it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReplicationSlotStatus {
    pub active: bool,
    pub active_pid: Option<i32>,
}

/// Look up the status of the replication slot named `slot_name`, returning `None`
/// if no such slot exists on the server.
///
/// Used by CDC catalog acceleration to decide, before it starts streaming,
/// whether a slot with its deterministic name is already **actively** held by
/// another consumer (fail loudly) versus merely present-and-inactive (safe to
/// reuse on restart) versus absent (create fresh).
///
/// # Errors
///
/// Returns an error if a connection can't be obtained from `pool`, or the
/// query fails.
pub async fn replication_slot_status(
    pool: &PostgresConnectionPool,
    slot_name: &str,
) -> Result<Option<ReplicationSlotStatus>> {
    let conn = pool.connect_direct().await.context(ConnectionFailedSnafu)?;
    let rows = conn
        .conn
        .query(
            "SELECT active, active_pid FROM pg_catalog.pg_replication_slots WHERE slot_name = $1",
            &[&slot_name],
        )
        .await
        .context(QueryFailedSnafu)?;
    Ok(rows.first().map(|row| ReplicationSlotStatus {
        active: row.get(0),
        active_pid: row.get(1),
    }))
}

/// The server's `wal_sender_timeout` in milliseconds (`0` means disabled).
///
/// This bounds how long `PostgreSQL` keeps a slot marked `active` after its
/// consumer's connection drops ungracefully, so the catalog acceleration path
/// uses it to size how long it waits for a stale slot to free (e.g. after its
/// own crash-restart) before deciding another live consumer holds it and failing
/// loudly.
///
/// # Errors
///
/// Returns an error if a connection can't be obtained from `pool`, or the
/// query fails.
pub async fn wal_sender_timeout_ms(pool: &PostgresConnectionPool) -> Result<i64> {
    let conn = pool.connect_direct().await.context(ConnectionFailedSnafu)?;
    // `pg_settings.setting` for `wal_sender_timeout` is expressed in
    // milliseconds (its `unit` is `ms`), so `::bigint` yields the raw ms value.
    let row = conn
        .conn
        .query_one(
            "SELECT setting::bigint FROM pg_catalog.pg_settings WHERE name = 'wal_sender_timeout'",
            &[],
        )
        .await
        .context(QueryFailedSnafu)?;
    Ok(row.get(0))
}

/// Ensure the server can create at least one more replication slot -- i.e. the
/// current slot count is below `max_replication_slots`.
///
/// CDC catalog acceleration needs one shared slot; creating it on a server whose
/// `max_replication_slots` is already exhausted otherwise fails deep in the
/// replication setup with a cryptic error. Checking up front turns that into an
/// actionable [`Error::ReplicationSlotsExhausted`] naming the fix.
///
/// The caller should skip this when the catalog's slot *already exists* (it will
/// be reused, not created, so no capacity is consumed) -- see
/// `AcceleratedCatalogProvider::ensure_catalog_slot_available`.
///
/// # Errors
///
/// Returns an error if a connection can't be obtained from `pool`, the query
/// fails, or the server has no free replication slots.
pub async fn ensure_replication_slot_capacity(pool: &PostgresConnectionPool) -> Result<()> {
    let conn = pool.connect_direct().await.context(ConnectionFailedSnafu)?;
    // `current_setting('max_replication_slots')` is text (e.g. "10"); cast to
    // bigint to compare against the live slot count.
    let row = conn
        .conn
        .query_one(
            "SELECT (SELECT count(*) FROM pg_catalog.pg_replication_slots)::bigint, \
             current_setting('max_replication_slots')::bigint",
            &[],
        )
        .await
        .context(QueryFailedSnafu)?;
    let used: i64 = row.get(0);
    let max: i64 = row.get(1);
    ensure!(used < max, ReplicationSlotsExhaustedSnafu { used, max });
    Ok(())
}

/// A table's `PostgreSQL` `REPLICA IDENTITY` mode -- the per-table property that
/// controls what the WAL carries in the *old tuple* of an `UPDATE`/`DELETE`,
/// which is what any logical-replication consumer uses to identify the affected
/// row. Decoded from the `pg_class.relreplident` byte.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicaIdentityMode {
    /// `d` -- keyed by the primary key (the `PostgreSQL` default).
    Default,
    /// `n` -- nothing is logged; `UPDATE`/`DELETE` cannot be replicated.
    Nothing,
    /// `f` -- the entire old row image is logged (heaviest).
    Full,
    /// `i` -- keyed by a nominated unique index (`USING INDEX`).
    Index,
    /// An unrecognized `relreplident` byte (forward-compatibility guard).
    Unknown,
}

impl ReplicaIdentityMode {
    fn from_relreplident(byte: &str) -> Self {
        match byte {
            "d" => Self::Default,
            "n" => Self::Nothing,
            "f" => Self::Full,
            "i" => Self::Index,
            _ => Self::Unknown,
        }
    }
}

/// A table's replica identity: its [`ReplicaIdentityMode`] plus the columns of
/// the primary key and (for `USING INDEX`) the nominated identity index, each in
/// key order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicaIdentity {
    pub mode: ReplicaIdentityMode,
    /// Primary-key columns in key order; empty when the table has no primary key.
    pub primary_key: Vec<String>,
    /// The `REPLICA IDENTITY USING INDEX` index's columns in key order. Empty
    /// when the table is not in `USING INDEX` mode, or the nominated index is
    /// unusable as an upsert key (e.g. an expression index -- `PostgreSQL`
    /// disallows those as replica identities, so this is a defensive guard).
    pub identity_index: Vec<String>,
}

/// Fold the per-index-column rows returned by [`replica_identity`]'s query into
/// a [`ReplicaIdentity`]. Factored out as a pure function so the accumulation
/// (multi-column key ordering, expression-index detection) is unit-testable
/// without a live `PostgreSQL` connection. Each row is
/// `(is_primary, is_identity, column_name)`; a `USING INDEX` key part with no
/// column name (an expression) marks the identity index unusable.
fn accumulate_replica_identity(
    mode: ReplicaIdentityMode,
    rows: impl IntoIterator<Item = (Option<bool>, Option<bool>, Option<String>)>,
) -> ReplicaIdentity {
    let mut primary_key = Vec::new();
    let mut identity_index = Vec::new();
    let mut identity_unusable = false;
    for (is_primary, is_identity, column_name) in rows {
        if is_primary == Some(true)
            && let Some(name) = column_name.clone()
        {
            primary_key.push(name);
        }
        if is_identity == Some(true) {
            match column_name {
                Some(name) => identity_index.push(name),
                None => identity_unusable = true,
            }
        }
    }
    if identity_unusable {
        identity_index.clear();
    }
    ReplicaIdentity {
        mode,
        primary_key,
        identity_index,
    }
}

/// Read a table's [`ReplicaIdentity`] (mode + primary-key + `USING INDEX`
/// identity columns) in a single round-trip.
///
/// One query gathers the `relreplident` mode byte alongside the columns of any
/// primary-key index (`indisprimary`) and any replica-identity index
/// (`indisreplident`), reusing the `int2vector -> int2[] WITH ORDINALITY`
/// key-ordering technique from [`primary_key_columns`]. Schema/table are matched
/// by literal name (`pg_namespace.nspname` / `pg_class.relname`), which -- unlike
/// `to_regclass` identifier parsing -- needs no quoting.
///
/// # Errors
///
/// Returns an error if a connection can't be obtained from `pool`, the query
/// fails, or the table no longer exists.
pub async fn replica_identity(
    pool: &PostgresConnectionPool,
    schema_name: &str,
    table_name: &str,
) -> Result<ReplicaIdentity> {
    let conn = pool.connect_direct().await.context(ConnectionFailedSnafu)?;

    // A table with no primary-key and no replica-identity index still returns a
    // single row (the mode, with NULL index columns) via the LEFT JOINs. Order
    // primary-key rows before identity-index rows, each by key position, so the
    // accumulator preserves per-index column order.
    let rows = conn
        .conn
        .query(
            "SELECT \
                 c.relreplident::text, \
                 ix.indisprimary, \
                 ix.indisreplident, \
                 a.attname \
             FROM pg_catalog.pg_class c \
             JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace \
             LEFT JOIN pg_catalog.pg_index ix \
                 ON ix.indrelid = c.oid \
                 AND (ix.indisprimary OR ix.indisreplident) \
                 AND ix.indisvalid \
             LEFT JOIN LATERAL unnest(string_to_array(ix.indkey::text, ' ')::int2[]) \
                 WITH ORDINALITY AS k(attnum, ord) ON true \
             LEFT JOIN pg_catalog.pg_attribute a \
                 ON a.attrelid = ix.indrelid \
                 AND a.attnum = k.attnum \
                 AND a.attnum > 0 \
                 AND NOT a.attisdropped \
             WHERE n.nspname = $1 AND c.relname = $2 \
             ORDER BY ix.indisprimary DESC NULLS LAST, k.ord",
            &[&schema_name, &table_name],
        )
        .await
        .context(QueryFailedSnafu)?;

    let Some(first) = rows.first() else {
        return TableNotFoundSnafu {
            schema: schema_name.to_string(),
            table: table_name.to_string(),
        }
        .fail();
    };

    let mode = ReplicaIdentityMode::from_relreplident(&first.get::<_, String>(0));
    Ok(accumulate_replica_identity(
        mode,
        rows.iter()
            .map(|row| (row.get(1), row.get(2), row.get::<_, Option<String>>(3))),
    ))
}

/// Why a table cannot be CDC-accelerated (see [`classify_replica_identity`]).
/// Each variant's [`SkipReason::explanation`] names the operator's fix.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SkipReason {
    /// `REPLICA IDENTITY NOTHING` -- no identity is logged at all.
    NoReplicaIdentity,
    /// `REPLICA IDENTITY DEFAULT` but the table has no primary key.
    KeylessDefault,
    /// `REPLICA IDENTITY USING INDEX` but the nominated index is unusable.
    UnusableIdentityIndex,
    /// `REPLICA IDENTITY FULL` but the table has no primary key to upsert on.
    FullWithoutKey,
    /// An unrecognized `relreplident` byte.
    UnknownMode,
}

impl SkipReason {
    /// A short, actionable explanation naming the operator's fix, for the
    /// per-table warning emitted when a table is skipped.
    #[must_use]
    pub fn explanation(self) -> &'static str {
        match self {
            Self::NoReplicaIdentity => {
                "REPLICA IDENTITY NOTHING logs no row identity, so UPDATE/DELETE cannot be replicated -- add a primary key, or set REPLICA IDENTITY FULL / USING INDEX"
            }
            Self::KeylessDefault => {
                "no primary key (REPLICA IDENTITY DEFAULT) -- add a primary key, or a unique NOT NULL index with REPLICA IDENTITY USING INDEX"
            }
            Self::UnusableIdentityIndex => {
                "the REPLICA IDENTITY index is not usable as an upsert key -- use a unique, non-partial index on NOT NULL columns"
            }
            Self::FullWithoutKey => {
                "REPLICA IDENTITY FULL but no primary key to upsert on -- add a primary key, or a unique NOT NULL index with REPLICA IDENTITY USING INDEX"
            }
            Self::UnknownMode => {
                "unrecognized REPLICA IDENTITY -- set REPLICA IDENTITY DEFAULT (with a primary key), USING INDEX, or FULL"
            }
        }
    }
}

/// The catalog-acceleration eligibility decision for a table, derived purely
/// from its [`ReplicaIdentity`]. The `Accelerate*` variants carry the resolved
/// upsert `key` the synthesized dataset must declare (schema inference will not
/// derive a `USING INDEX` key, so the caller declares it explicitly).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplicaIdentityOutcome {
    /// `DEFAULT` + primary key: replicate keyed by the primary key.
    AccelerateViaPrimaryKey { key: Vec<String> },
    /// `USING INDEX`: replicate keyed by the nominated unique index's columns.
    AccelerateViaUniqueIndex { key: Vec<String> },
    /// `FULL` + primary key: replicate keyed by the primary key. Heavier -- the
    /// caller should warn (full old-row image per UPDATE/DELETE).
    AccelerateFullReplicaIdentity { key: Vec<String> },
    /// Not CDC-replicable; the caller skips it with a warning.
    Skip { reason: SkipReason },
}

/// Decide, from a table's [`ReplicaIdentity`] alone, whether it can be
/// CDC-accelerated and by which key. Pure and exhaustive over the mode x
/// key-presence matrix so it can be unit-tested without a live database.
///
/// The accelerator always needs a unique routing key for its upsert, so a table
/// with no usable key -- `NOTHING`, keyless `DEFAULT`, `FULL` without a primary
/// key, or `USING INDEX` with an unusable index -- is skipped regardless of mode.
#[must_use]
pub fn classify_replica_identity(identity: &ReplicaIdentity) -> ReplicaIdentityOutcome {
    match identity.mode {
        ReplicaIdentityMode::Nothing => ReplicaIdentityOutcome::Skip {
            reason: SkipReason::NoReplicaIdentity,
        },
        ReplicaIdentityMode::Default => {
            if identity.primary_key.is_empty() {
                ReplicaIdentityOutcome::Skip {
                    reason: SkipReason::KeylessDefault,
                }
            } else {
                ReplicaIdentityOutcome::AccelerateViaPrimaryKey {
                    key: identity.primary_key.clone(),
                }
            }
        }
        ReplicaIdentityMode::Index => {
            if identity.identity_index.is_empty() {
                ReplicaIdentityOutcome::Skip {
                    reason: SkipReason::UnusableIdentityIndex,
                }
            } else {
                ReplicaIdentityOutcome::AccelerateViaUniqueIndex {
                    key: identity.identity_index.clone(),
                }
            }
        }
        ReplicaIdentityMode::Full => {
            if identity.primary_key.is_empty() {
                ReplicaIdentityOutcome::Skip {
                    reason: SkipReason::FullWithoutKey,
                }
            } else {
                ReplicaIdentityOutcome::AccelerateFullReplicaIdentity {
                    key: identity.primary_key.clone(),
                }
            }
        }
        ReplicaIdentityMode::Unknown => ReplicaIdentityOutcome::Skip {
            reason: SkipReason::UnknownMode,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::{
        Error, ReplicaIdentity, ReplicaIdentityMode, ReplicaIdentityOutcome, SkipReason,
        accumulate_replica_identity, classify_replica_identity,
    };

    fn cols(names: &[&str]) -> Vec<String> {
        names.iter().map(ToString::to_string).collect()
    }

    fn identity(mode: ReplicaIdentityMode, pk: &[&str], idx: &[&str]) -> ReplicaIdentity {
        ReplicaIdentity {
            mode,
            primary_key: cols(pk),
            identity_index: cols(idx),
        }
    }

    #[test]
    fn classify_covers_full_matrix() {
        // DEFAULT + PK -> accelerate via the primary key.
        assert_eq!(
            classify_replica_identity(&identity(ReplicaIdentityMode::Default, &["id"], &[])),
            ReplicaIdentityOutcome::AccelerateViaPrimaryKey { key: cols(&["id"]) }
        );
        // DEFAULT, no PK -> skip.
        assert_eq!(
            classify_replica_identity(&identity(ReplicaIdentityMode::Default, &[], &[])),
            ReplicaIdentityOutcome::Skip {
                reason: SkipReason::KeylessDefault
            }
        );
        // USING INDEX, usable index -> accelerate via the index columns.
        assert_eq!(
            classify_replica_identity(&identity(
                ReplicaIdentityMode::Index,
                &[],
                &["tenant", "sku"]
            )),
            ReplicaIdentityOutcome::AccelerateViaUniqueIndex {
                key: cols(&["tenant", "sku"])
            }
        );
        // USING INDEX, unusable/empty index -> skip.
        assert_eq!(
            classify_replica_identity(&identity(ReplicaIdentityMode::Index, &[], &[])),
            ReplicaIdentityOutcome::Skip {
                reason: SkipReason::UnusableIdentityIndex
            }
        );
        // FULL + PK -> accelerate (caller warns), keyed by the primary key.
        assert_eq!(
            classify_replica_identity(&identity(ReplicaIdentityMode::Full, &["id"], &[])),
            ReplicaIdentityOutcome::AccelerateFullReplicaIdentity { key: cols(&["id"]) }
        );
        // FULL, no PK -> skip (no usable upsert key).
        assert_eq!(
            classify_replica_identity(&identity(ReplicaIdentityMode::Full, &[], &[])),
            ReplicaIdentityOutcome::Skip {
                reason: SkipReason::FullWithoutKey
            }
        );
        // NOTHING -> skip regardless of any key columns present.
        assert_eq!(
            classify_replica_identity(&identity(ReplicaIdentityMode::Nothing, &["id"], &[])),
            ReplicaIdentityOutcome::Skip {
                reason: SkipReason::NoReplicaIdentity
            }
        );
        // Unrecognized mode -> skip.
        assert_eq!(
            classify_replica_identity(&identity(ReplicaIdentityMode::Unknown, &["id"], &[])),
            ReplicaIdentityOutcome::Skip {
                reason: SkipReason::UnknownMode
            }
        );
    }

    #[test]
    fn from_relreplident_decodes_known_bytes() {
        assert_eq!(
            ReplicaIdentityMode::from_relreplident("d"),
            ReplicaIdentityMode::Default
        );
        assert_eq!(
            ReplicaIdentityMode::from_relreplident("n"),
            ReplicaIdentityMode::Nothing
        );
        assert_eq!(
            ReplicaIdentityMode::from_relreplident("f"),
            ReplicaIdentityMode::Full
        );
        assert_eq!(
            ReplicaIdentityMode::from_relreplident("i"),
            ReplicaIdentityMode::Index
        );
        assert_eq!(
            ReplicaIdentityMode::from_relreplident("x"),
            ReplicaIdentityMode::Unknown
        );
    }

    #[test]
    fn accumulate_preserves_multi_column_key_order() {
        // Two-column PK arriving in key order; no identity index.
        let id = accumulate_replica_identity(
            ReplicaIdentityMode::Default,
            vec![
                (Some(true), Some(false), Some("tenant".to_string())),
                (Some(true), Some(false), Some("sku".to_string())),
            ],
        );
        assert_eq!(id.primary_key, cols(&["tenant", "sku"]));
        assert!(id.identity_index.is_empty());
    }

    #[test]
    fn accumulate_collects_identity_index_columns() {
        let id = accumulate_replica_identity(
            ReplicaIdentityMode::Index,
            vec![
                (Some(false), Some(true), Some("a".to_string())),
                (Some(false), Some(true), Some("b".to_string())),
            ],
        );
        assert!(id.primary_key.is_empty());
        assert_eq!(id.identity_index, cols(&["a", "b"]));
    }

    #[test]
    fn accumulate_marks_expression_identity_index_unusable() {
        // A NULL column name for an identity key part (an expression) clears the
        // whole identity index -- it can't be used as an upsert key.
        let id = accumulate_replica_identity(
            ReplicaIdentityMode::Index,
            vec![
                (Some(false), Some(true), Some("a".to_string())),
                (Some(false), Some(true), None),
            ],
        );
        assert!(id.identity_index.is_empty());
    }

    #[test]
    fn accumulate_no_index_rows_yields_empty_keys() {
        // A keyless table returns a single all-NULL index row (just the mode).
        let id =
            accumulate_replica_identity(ReplicaIdentityMode::Default, vec![(None, None, None)]);
        assert!(id.primary_key.is_empty());
        assert!(id.identity_index.is_empty());
    }

    #[test]
    fn skip_reason_explanations_are_actionable() {
        // Every skip reason drives a per-table warning, so each must name the
        // property at fault (REPLICA IDENTITY) and a concrete fix the operator
        // can apply.
        for reason in [
            SkipReason::NoReplicaIdentity,
            SkipReason::KeylessDefault,
            SkipReason::UnusableIdentityIndex,
            SkipReason::FullWithoutKey,
            SkipReason::UnknownMode,
        ] {
            let explanation = reason.explanation();
            assert!(
                explanation.contains("REPLICA IDENTITY"),
                "{reason:?} explanation should name REPLICA IDENTITY: {explanation}"
            );
            assert!(
                explanation.contains("primary key")
                    || explanation.contains("USING INDEX")
                    || explanation.contains("unique"),
                "{reason:?} explanation should name a concrete fix: {explanation}"
            );
        }
    }

    #[test]
    fn cdc_prerequisite_errors_are_actionable_with_docs_links() {
        // The wal_level and replication-privilege errors are the first thing an
        // operator hits when a source can't do CDC -- each must name the exact
        // problem, the exact fix, and a docs link.
        let wal = Error::WalLevelNotLogical {
            wal_level: "replica".to_string(),
        }
        .to_string();
        assert!(wal.contains("wal_level"), "{wal}");
        assert!(wal.contains("logical"), "{wal}");
        assert!(wal.contains("ALTER SYSTEM SET wal_level"), "{wal}");
        assert!(wal.contains("https://spiceai.org/docs"), "{wal}");

        let role = Error::MissingReplicationPrivilege {
            role: "app_ro".to_string(),
        }
        .to_string();
        assert!(role.contains("app_ro"), "{role}");
        assert!(role.contains("replication"), "{role}");
        assert!(role.contains("ALTER ROLE"), "{role}");
        assert!(role.contains("https://spiceai.org/docs"), "{role}");

        let exhausted = Error::ReplicationSlotsExhausted { used: 10, max: 10 }.to_string();
        assert!(exhausted.contains("10 of 10 in use"), "{exhausted}");
        assert!(exhausted.contains("max_replication_slots"), "{exhausted}");
        assert!(
            exhausted.contains("pg_drop_replication_slot"),
            "{exhausted}"
        );
        assert!(
            exhausted.contains("https://spiceai.org/docs"),
            "{exhausted}"
        );
    }
}
