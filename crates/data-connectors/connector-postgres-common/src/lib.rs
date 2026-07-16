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

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Failed to get connection from PostgreSQL pool: {source}. Check `pg_host`/`pg_port`/`pg_user`/`pg_pass`/`pg_sslmode` in the dataset params and that the server is reachable. Docs: https://spiceai.org/docs/components/data-connectors/postgres"
    ))]
    ConnectionFailed {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display(
        "PostgreSQL query failed: {source}. Check SQL syntax and that referenced tables exist. Docs: https://spiceai.org/docs/components/data-connectors/postgres"
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
