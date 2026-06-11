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

//! `PostgreSQL` data connector for Spice.ai runtime.
//!
//! This crate provides the `PostgreSQL` connector implementation, allowing
//! Spice.ai to connect to `PostgreSQL` databases as data sources. It also
//! exposes a direct WAL-based `ChangesStream`, so users can set
//! `acceleration.refresh_mode: changes` on a Postgres dataset and get
//! change-by-change replication into the local accelerator without Debezium.

use async_trait::async_trait;
use data_components::federation::create_spice_federated_table_provider;
use data_components::inferred_schema::{
    InferredColumnStats, InferredIndex, InferredSchema, InferredSortColumn,
};
use datafusion::datasource::TableProvider;
use datafusion::sql::TableReference;
use datafusion::sql::unparser::dialect::PostgreSqlDialect;
use datafusion_table_providers::postgres::{DynPostgresConnectionPool, PostgresTableFactory};
use datafusion_table_providers::sql::db_connection_pool::dbconnection;
use datafusion_table_providers::sql::db_connection_pool::{
    Error as DbConnectionPoolError,
    postgrespool::{self, PostgresConnectionPool},
};
use datafusion_table_providers::sql::sql_provider_datafusion::{SqlTable, expr::Engine};
use runtime::component::dataset::Dataset;
use runtime::component::metrics::MetricsProvider;
use runtime::dataconnector::{
    ConnectorComponent, ConnectorParams, DataConnector, DataConnectorError, DataConnectorFactory,
    DataConnectorResult, NewDataConnectorResult,
};
use runtime::datafusion::udf::deny_spice_specific_functions;
use runtime::parameters::ParameterSpec;
use secrecy::SecretBox;
use snafu::prelude::*;
use std::any::Any;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

mod replication;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to create Postgres connection pool: {source}"))]
    UnableToCreatePostgresConnectionPool { source: DbConnectionPoolError },
}

/// `PostgreSQL` data connector.
pub struct Postgres {
    factory: PostgresTableFactory,
    pool: Arc<PostgresConnectionPool>,
    params: runtime::parameters::Parameters,
    replication_metrics:
        std::sync::Arc<data_components::postgres_replication::ReplicationMetricsCollector>,
}

impl std::fmt::Debug for Postgres {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Postgres").finish_non_exhaustive()
    }
}

/// Factory for creating `PostgreSQL` connector instances.
#[derive(Default, Copy, Clone)]
pub struct PostgresFactory {}

impl PostgresFactory {
    #[must_use]
    pub fn new() -> Self {
        Self {}
    }

    #[must_use]
    pub fn new_arc() -> Arc<dyn DataConnectorFactory> {
        Arc::new(Self {}) as Arc<dyn DataConnectorFactory>
    }
}

const POSTGRES_DOCS: &str = "https://spiceai.org/docs/components/data-connectors/postgres";

const PARAMETERS: &[ParameterSpec] = &[
    ParameterSpec::component("connection_string")
        .description(
            "Full libpq-style connection string. Overrides other connection params if set.",
        )
        .examples(&["host=db.example.com port=5432 dbname=app user=ro sslmode=require"])
        .help_link(POSTGRES_DOCS)
        .secret(),
    ParameterSpec::component("user")
        .description("PostgreSQL username.")
        .examples(&["postgres", "spice_reader"])
        .help_link(POSTGRES_DOCS)
        .secret(),
    ParameterSpec::component("pass")
        .description("PostgreSQL password.")
        .help_link(POSTGRES_DOCS)
        .secret(),
    ParameterSpec::component("host")
        .description("PostgreSQL server hostname or IP.")
        .examples(&["db.internal", "10.0.0.5"])
        .help_link(POSTGRES_DOCS),
    ParameterSpec::component("port")
        .description("PostgreSQL TCP port.")
        .examples(&["5432"])
        .help_link(POSTGRES_DOCS),
    ParameterSpec::component("db")
        .description("Database name.")
        .examples(&["app", "analytics"])
        .help_link(POSTGRES_DOCS),
    ParameterSpec::component("sslmode")
        .description("libpq SSL mode: disable, allow, prefer, require, verify-ca, verify-full.")
        .one_of(&[
            "disable",
            "allow",
            "prefer",
            "require",
            "verify-ca",
            "verify-full",
        ])
        .help_link(POSTGRES_DOCS),
    ParameterSpec::component("sslrootcert")
        .description(
            "Path to, or inline PEM content for, a CA certificate used when sslmode is verify-ca/verify-full.",
        )
        .help_link(POSTGRES_DOCS),
    ParameterSpec::component("connection_pool_min_idle")
        .description(
            "The minimum number of idle connections to keep open in the pool. \
             Default: 1 (0 for `refresh_mode: changes` datasets, which use the pool only \
             for schema probes — replication runs over dedicated connections).",
        )
        .default("1")
        .help_link(POSTGRES_DOCS),
    ParameterSpec::runtime("connection_pool_size")
        .description(
            "The maximum number of connections created in the connection pool. \
             Default: 5 (2 for `refresh_mode: changes` datasets).",
        )
        .default("5")
        .help_link(POSTGRES_DOCS),
    // --- Logical replication (WAL streaming) ---
    ParameterSpec::component("replication_slot").description(
        "Name of the Postgres replication slot to create/reuse for this dataset. \
         Defaults to `spice_<dataset>_<dataset-hash>_<instance-hash>`. Datasets on the \
         same connection that name the same slot SHARE it: one replication connection, \
         one publication, with decoded changes routed per table. Each Spice replica \
         MUST have its own unique slot.",
    ),
    ParameterSpec::component("publication").description(
        "Name of the Postgres publication to create/reuse for this dataset. \
         Defaults to `spice_<dataset>_<dataset-hash>_pub`, or `<slot>_pub` when \
         `pg_replication_slot` is set. Shared across replicas for the same dataset; \
         datasets sharing a slot must use the same publication.",
    ),
    ParameterSpec::component("replication_initial_snapshot")
        .description(
            "Whether to take an initial snapshot of the table's existing rows on first \
             connection, before streaming WAL changes. Default: true.",
        )
        .default("true"),
    ParameterSpec::component("replication_temporary_slot")
        .description(
            "If true, create a temporary replication slot that is dropped when the \
             Spice process disconnects. Default: false (durable slot).",
        )
        .default("false"),
    ParameterSpec::component("replication_status_interval")
        .description(
            "How often to send StandbyStatusUpdate to Postgres (e.g. '10s'). \
             Default: 10s.",
        )
        .default("10s"),
    ParameterSpec::component("replication_bootstrap_batch_size")
        .description(
            "Rows per emitted batch during the initial replication snapshot. \
             Default: 8192. Maximum: 1048576.",
        )
        .default("8192"),
];

impl DataConnectorFactory for PostgresFactory {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn create(
        &self,
        params: ConnectorParams,
    ) -> Pin<Box<dyn Future<Output = NewDataConnectorResult> + Send>> {
        Box::pin(async move {
            let mut param_map = params.parameters.to_secret_map();

            param_map.insert(
                "application_name".to_string(),
                SecretBox::from(format!("Spice.ai {}", env!("CARGO_PKG_VERSION"))),
            );

            // `refresh_mode: changes` datasets use this pool only for schema
            // probes at initialization — replication runs over its own
            // dedicated connections. Unless the user sized the pool
            // explicitly, keep it minimal: no idle connections held for the
            // lifetime of the dataset (`min_idle: 0`), and a small max. This
            // matters at N CDC datasets per source database, and keeps a
            // dataset stuck in an init retry loop from multiplying held
            // connections.
            if let ConnectorComponent::Dataset(dataset) = &params.component {
                let is_changes_mode = dataset.acceleration.as_ref().is_some_and(|acceleration| {
                    acceleration.refresh_mode
                        == Some(runtime::component::dataset::acceleration::RefreshMode::Changes)
                });
                if is_changes_mode {
                    // The injected spec defaults are indistinguishable from
                    // user-set values here, so consult the raw spicepod
                    // params for whether the user chose a size.
                    let user_set = |key: &str| {
                        dataset.params.contains_key(&format!("pg_{key}"))
                            || dataset.params.contains_key(key)
                    };
                    if !user_set("connection_pool_size") {
                        param_map.insert("connection_pool_size".to_string(), SecretBox::from("2"));
                    }
                    if !user_set("connection_pool_min_idle") {
                        param_map
                            .insert("connection_pool_min_idle".to_string(), SecretBox::from("0"));
                    }
                }
            }

            let params_for_replication = params.parameters.clone();

            match PostgresConnectionPool::new(param_map).await {
                Ok(pool) => {
                    let unsupported_type_action = params
                        .unsupported_type_action
                        .unwrap_or(datafusion_table_providers::UnsupportedTypeAction::String);
                    let pool = pool.with_unsupported_type_action(unsupported_type_action);

                    let pool = Arc::new(pool);
                    let factory = PostgresTableFactory::new(Arc::clone(&pool));
                    Ok(Arc::new(Postgres {
                        factory,
                        pool,
                        params: params_for_replication,
                        replication_metrics:
                            data_components::postgres_replication::ReplicationMetricsCollector::new(
                            ),
                    }) as Arc<dyn DataConnector>)
                }
                Err(e) => match e {
                    postgrespool::Error::InvalidUsernameOrPassword { .. } => Err(
                        DataConnectorError::UnableToConnectInvalidUsernameOrPassword {
                            dataconnector: "postgres".to_string(),
                            connector_component: params.component.clone(),
                        }
                        .into(),
                    ),

                    postgrespool::Error::InvalidHostOrPortError {
                        host,
                        port,
                        source: _,
                    } => Err(DataConnectorError::UnableToConnectInvalidHostOrPort {
                        dataconnector: "postgres".to_string(),
                        connector_component: params.component.clone(),
                        host,
                        port: format!("{port}"),
                    }
                    .into()),

                    _ => Err(DataConnectorError::UnableToConnectInternal {
                        dataconnector: "postgres".to_string(),
                        connector_component: params.component.clone(),
                        source: Box::new(e),
                    }
                    .into()),
                },
            }
        })
    }

    fn supports_unsupported_type_action(&self) -> bool {
        true
    }

    fn prefix(&self) -> &'static str {
        "pg"
    }

    fn parameters(&self) -> &'static [ParameterSpec] {
        PARAMETERS
    }
}

async fn postgres_comment_metadata(
    pool: &Arc<PostgresConnectionPool>,
    table_path: &str,
) -> std::result::Result<
    (HashMap<String, String>, data_components::FieldMetadata),
    Box<dyn std::error::Error + Send + Sync>,
> {
    let conn = pool.connect_direct().await?;
    let rows = conn
        .conn
        .query(
            "SELECT \
                 obj_description(c.oid, 'pg_class') AS table_comment, \
                 a.attname AS column_name, \
                 col_description(c.oid, a.attnum) AS column_comment, \
                 format_type(a.atttypid, a.atttypmod) AS column_source_type \
             FROM pg_catalog.pg_class c \
             JOIN pg_catalog.pg_attribute a \
                 ON a.attrelid = c.oid \
                 AND a.attnum > 0 \
                 AND NOT a.attisdropped \
             WHERE c.oid = to_regclass($1) \
             ORDER BY a.attnum",
            &[&table_path],
        )
        .await?;
    let rows = rows
        .iter()
        .map(|row| (row.get(0), row.get(1), row.get(2), row.get(3)));

    Ok(data_components::postgres::provider::postgres_metadata_from_rows(rows))
}

/// One row per index column for the target table, carrying the flags needed to
/// reconstruct the primary key, secondary indexes, and clustered sort order.
///
/// `indkey`/`indoption` are `int2vector`s; routing them through text to `int2[]`
/// yields standard 1-based arrays that line up with `WITH ORDINALITY`. Expression
/// index keys have `attnum` 0 (no matching `pg_attribute` row, so `column_name` is
/// NULL); partial indexes have a non-null `indpred`. `is_btree` carries the access
/// method: only b-tree indexes translate to accelerator index/sort semantics (the
/// per-column DESC bit in `indoption` is itself only meaningful for b-tree), so
/// GIN/`GiST`/BRIN/hash indexes are reported and skipped during derivation —
/// mirroring the `MongoDB` connector's non-b-tree key-type rule. `k.ord <=
/// ix.indnkeyatts` bounds the unnest to *key* columns, excluding PG 11+ `INCLUDE`
/// (non-key) columns from the inferred key/sort; on servers without `indnkeyatts`
/// (< 11) the query errors and enrichment is skipped (warn-and-continue).
const INFERRED_SCHEMA_SQL: &str = "\
    SELECT \
        i.relname AS index_name, \
        ix.indisprimary AS is_primary, \
        ix.indisunique AS is_unique, \
        ix.indisclustered AS is_clustered, \
        (ix.indpred IS NOT NULL) AS is_partial, \
        (ix.indexprs IS NOT NULL) AS has_expressions, \
        (am.amname = 'btree') AS is_btree, \
        k.ord AS column_ordinal, \
        a.attname AS column_name, \
        COALESCE(((string_to_array(ix.indoption::text, ' ')::int[])[k.ord] & 1) = 1, false) AS is_desc, \
        COALESCE(((string_to_array(ix.indoption::text, ' ')::int[])[k.ord] & 2) = 2, false) AS nulls_first \
    FROM pg_catalog.pg_index ix \
    JOIN pg_catalog.pg_class i ON i.oid = ix.indexrelid \
    JOIN pg_catalog.pg_am am ON am.oid = i.relam \
    JOIN LATERAL unnest(string_to_array(ix.indkey::text, ' ')::int2[]) \
        WITH ORDINALITY AS k(attnum, ord) ON true \
    LEFT JOIN pg_catalog.pg_attribute a \
        ON a.attrelid = ix.indrelid \
        AND a.attnum = k.attnum \
        AND a.attnum > 0 \
        AND NOT a.attisdropped \
    WHERE ix.indrelid = to_regclass($1) \
        AND ix.indisvalid \
        AND k.ord <= ix.indnkeyatts \
    ORDER BY i.relname, k.ord";

/// Rough table sizing from the catalog — estimates only, no scan.
///
/// Regular relations report the planner's estimated row count (`reltuples`;
/// `NULL`ed via `-1` handling when never analyzed) and `pg_table_size` (heap +
/// TOAST + maps — `pg_relation_size`'s main fork alone badly undercounts wide
/// `TOASTed` tables). A partitioned parent has no storage of its own
/// (`pg_relation_size` = 0, which would masquerade as "tiny table" in planner
/// statistics), so its leaves are aggregated via `pg_partition_tree` instead:
/// bytes summed over all leaves, rows summed over *analyzed* leaves (`NULL` when
/// none was analyzed). Views/foreign tables yield `NULL` bytes rather than a
/// misleading 0.
const TABLE_SIZE_SQL: &str = "\
    SELECT \
        CASE WHEN c.relkind = 'p' THEN ( \
            SELECT sum(GREATEST(pc.reltuples, 0))::bigint \
            FROM pg_catalog.pg_partition_tree(c.oid) t \
            JOIN pg_catalog.pg_class pc ON pc.oid = t.relid \
            WHERE t.isleaf AND pc.reltuples >= 0 \
        ) ELSE c.reltuples::bigint END AS row_estimate, \
        CASE WHEN c.relkind = 'p' THEN ( \
            SELECT sum(pg_table_size(t.relid))::bigint \
            FROM pg_catalog.pg_partition_tree(c.oid) t \
            WHERE t.isleaf \
        ) WHEN c.relkind IN ('r', 'm', 't') THEN pg_table_size(c.oid) \
        ELSE NULL END AS table_bytes \
    FROM pg_catalog.pg_class c \
    WHERE c.oid = to_regclass($1)";

/// Rough per-column statistics from `pg_stats` for the target table: the
/// planner's distinct-value estimate (`n_distinct`; positive = absolute count,
/// negative = fraction of rows) and the physical-order correlation
/// (`[-1.0, 1.0]`; near `±1` ⇒ the heap is (reverse-)ordered by the column,
/// e.g. an append-only table with a `created_at` column). Rows exist only for
/// analyzed tables, and `pg_stats` row-level security restricts them to columns
/// the connecting role can read. `inherited` stats (partitioned/inheritance
/// parents) sort last so they win when both flavors exist.
const COLUMN_STATS_SQL: &str = "\
    SELECT \
        s.attname AS column_name, \
        s.n_distinct, \
        s.correlation \
    FROM pg_catalog.pg_stats s \
    JOIN pg_catalog.pg_class c ON c.oid = to_regclass($1) \
    JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace \
    WHERE s.schemaname = n.nspname \
        AND s.tablename = c.relname \
    ORDER BY s.inherited, s.attname";

/// The target table's declared partition key (range/list/hash), one row per key
/// column. Expression key elements have `attnum` 0 (NULL `column_name`), which
/// marks the whole key unusable — same rule as expression indexes.
const PARTITION_KEY_SQL: &str = "\
    SELECT \
        pt.partstrat::text AS strategy, \
        k.ord AS column_ordinal, \
        a.attname AS column_name \
    FROM pg_catalog.pg_partitioned_table pt \
    JOIN LATERAL unnest(string_to_array(pt.partattrs::text, ' ')::int2[]) \
        WITH ORDINALITY AS k(attnum, ord) ON true \
    LEFT JOIN pg_catalog.pg_attribute a \
        ON a.attrelid = pt.partrelid \
        AND a.attnum = k.attnum \
        AND a.attnum > 0 \
        AND NOT a.attisdropped \
    WHERE pt.partrelid = to_regclass($1) \
    ORDER BY k.ord";

/// Accumulates the columns and flags for a single index while scanning rows.
#[expect(
    clippy::struct_excessive_bools,
    reason = "mirrors the distinct boolean flags of a pg_index catalog row"
)]
struct IndexAccumulator {
    is_primary: bool,
    is_unique: bool,
    is_clustered: bool,
    is_partial: bool,
    has_expressions: bool,
    is_btree: bool,
    columns: Vec<IndexKeyColumn>,
}

/// One key column of an index, as read from the `pg_catalog` row.
#[derive(Debug, Clone)]
struct IndexKeyColumn {
    /// 1-based position within the index key.
    ordinal: i64,
    /// Column name; `None` for expression key elements.
    name: Option<String>,
    /// `indoption` bit 1: the column sorts descending.
    desc: bool,
    /// `indoption` bit 2: NULLs sort first.
    nulls_first: bool,
}

/// The table's declared partition key, when the source table is partitioned.
struct PartitionKey {
    /// `pg_partitioned_table.partstrat`: `r` (range), `l` (list), `h` (hash).
    strategy: String,
    /// Key columns in key order; `None` for expression key elements.
    columns: Vec<Option<String>>,
}

impl PartitionKey {
    /// The partition key as a sort prefix, or `None` when it does not describe a
    /// physical/logical ordering: hash partitioning scatters rather than orders,
    /// and an expression key element has no plain column to sort by.
    fn sort_prefix(&self) -> Option<Vec<String>> {
        if self.strategy != "r" && self.strategy != "l" {
            return None;
        }
        self.columns.iter().cloned().collect()
    }

    /// The partition key as the table's declared distribution/shard key, or
    /// `None` when any element is an expression. Unlike [`Self::sort_prefix`],
    /// hash partitioning qualifies: it does not order rows, but its columns are
    /// still the dimension the source distributes by.
    fn shard_columns(&self) -> Option<Vec<String>> {
        self.columns.iter().cloned().collect()
    }
}

/// A raw `pg_stats` row for one column of the target table.
struct RawColumnStats {
    column: String,
    /// `pg_stats.n_distinct`: positive = absolute distinct count, negative =
    /// `-fraction` of the row count, `0`/`NULL` = unknown.
    n_distinct: Option<f32>,
    /// `pg_stats.correlation` in `[-1.0, 1.0]`.
    correlation: Option<f32>,
}

impl RawColumnStats {
    /// Normalize to the connector-agnostic [`InferredColumnStats`], resolving
    /// ratio-style `n_distinct` against the table's row estimate.
    fn normalize(&self, row_estimate: Option<u64>) -> InferredColumnStats {
        let distinct_count = match self.n_distinct {
            Some(n) if n > 0.0 => float_to_count(f64::from(n)),
            Some(n) if n < 0.0 => {
                row_estimate.and_then(|rows| float_to_count(f64::from(-n) * precise_f64_from(rows)))
            }
            _ => None,
        };
        InferredColumnStats {
            column: self.column.clone(),
            distinct_count,
            correlation: self.correlation.map(f64::from),
        }
    }
}

/// Convert a non-negative float estimate to a count, refusing values outside
/// f64's exact-integer range rather than truncating.
fn float_to_count(value: f64) -> Option<u64> {
    if !value.is_finite() || !(0.0..=9_007_199_254_740_992.0).contains(&value) {
        return None;
    }
    #[expect(
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        reason = "guarded to non-negative values within f64's exact-integer range"
    )]
    let count = value.round() as u64;
    Some(count)
}

/// `u64` → `f64` for estimate math; row estimates are far below 2^53, so the
/// conversion is exact in practice.
#[expect(
    clippy::cast_precision_loss,
    reason = "row estimates are catalog estimates; precision loss above 2^53 is immaterial"
)]
fn precise_f64_from(value: u64) -> f64 {
    value as f64
}

/// Query `pg_catalog` for the target table's primary key, secondary indexes, and
/// clustered sort order. Returns an [`InferredSchema`]; empty when nothing usable
/// was found (e.g. a heap table with no indexes).
async fn postgres_inferred_schema_metadata(
    pool: &Arc<PostgresConnectionPool>,
    table_path: &str,
) -> std::result::Result<InferredSchema, Box<dyn std::error::Error + Send + Sync>> {
    let conn = pool.connect_direct().await?;
    let rows = conn.conn.query(INFERRED_SCHEMA_SQL, &[&table_path]).await?;

    // Group rows by index name; BTreeMap keeps iteration deterministic.
    let mut by_index: BTreeMap<String, IndexAccumulator> = BTreeMap::new();
    for row in &rows {
        let index_name: String = row.get("index_name");
        let acc = by_index
            .entry(index_name)
            .or_insert_with(|| IndexAccumulator {
                is_primary: row.get("is_primary"),
                is_unique: row.get("is_unique"),
                is_clustered: row.get("is_clustered"),
                is_partial: row.get("is_partial"),
                has_expressions: row.get("has_expressions"),
                is_btree: row.get("is_btree"),
                columns: Vec::new(),
            });
        acc.columns.push(IndexKeyColumn {
            ordinal: row.get("column_ordinal"),
            name: row.get("column_name"),
            desc: row.get("is_desc"),
            nulls_first: row.get("nulls_first"),
        });
    }

    // Declared partition key (best-effort; absent on unpartitioned tables).
    let partition_key = match conn.conn.query(PARTITION_KEY_SQL, &[&table_path]).await {
        Ok(rows) if !rows.is_empty() => Some(PartitionKey {
            strategy: rows[0].get("strategy"),
            columns: rows
                .iter()
                .map(|row| row.get::<_, Option<String>>("column_name"))
                .collect(),
        }),
        Ok(_) => None,
        Err(error) => {
            tracing::debug!(%error, "Failed to query PostgreSQL partition key; continuing without it");
            None
        }
    };

    // Rough table sizing (best-effort: a failure here must not fail inference).
    // Fetched before the column stats, whose ratio-style `n_distinct` estimates
    // are resolved against the row estimate.
    let mut row_count: Option<u64> = None;
    let mut table_bytes: Option<u64> = None;
    if let Ok(size_rows) = conn.conn.query(TABLE_SIZE_SQL, &[&table_path]).await
        && let Some(row) = size_rows.first()
    {
        row_count = row
            .get::<_, Option<i64>>("row_estimate")
            .and_then(|rows| u64::try_from(rows).ok());
        table_bytes = row
            .get::<_, Option<i64>>("table_bytes")
            .and_then(|bytes| u64::try_from(bytes).ok());
    }

    // Per-column statistics (best-effort; empty when the table was never
    // analyzed). `ORDER BY inherited` makes parent-level (inherited) stats
    // overwrite per-relation ones in the map when both exist.
    let mut stats_by_column: BTreeMap<String, InferredColumnStats> = BTreeMap::new();
    match conn.conn.query(COLUMN_STATS_SQL, &[&table_path]).await {
        Ok(rows) => {
            for row in &rows {
                let raw = RawColumnStats {
                    column: row.get("column_name"),
                    n_distinct: row.get("n_distinct"),
                    correlation: row.get("correlation"),
                };
                stats_by_column.insert(raw.column.clone(), raw.normalize(row_count));
            }
        }
        Err(error) => {
            tracing::debug!(%error, "Failed to query PostgreSQL column statistics; continuing without them");
        }
    }

    let mut schema =
        inferred_schema_from_indexes(&by_index, partition_key.as_ref(), &stats_by_column);
    schema.row_count = row_count;
    schema.table_bytes = table_bytes;

    Ok(schema)
}

/// Minimum `|pg_stats.correlation|` for a column to count as the table's natural
/// physical order. `CLUSTER` and freshly-loaded sorted data report `±1.0`;
/// append-mostly time columns settle in the high `0.9`s.
const NATURAL_ORDER_MIN_CORRELATION: f64 = 0.90;

/// Minimum distinct-count for a natural-order sort candidate. Filters out
/// flag-like columns whose correlation is high only because they rarely change
/// (e.g. a boolean backfill marker) — too coarse to be a useful leading sort key.
const NATURAL_ORDER_MIN_DISTINCT: u64 = 100;

/// Derive an [`InferredSchema`] from the per-index accumulators built from the
/// `pg_catalog` rows, plus the table's declared partition key and per-column
/// statistics (if any). Pure (no I/O) so the grouping/derivation rules are
/// unit-tested against synthetic catalog rows.
fn inferred_schema_from_indexes(
    by_index: &BTreeMap<String, IndexAccumulator>,
    partition_key: Option<&PartitionKey>,
    stats_by_column: &BTreeMap<String, InferredColumnStats>,
) -> InferredSchema {
    let mut primary_key: Vec<String> = Vec::new();
    let mut indexes: Vec<InferredIndex> = Vec::new();
    let mut clustered_sort: Option<Vec<InferredSortColumn>> = None;

    for acc in by_index.values() {
        let mut columns = acc.columns.clone();
        columns.sort_by_key(|column| column.ordinal);

        // Every key part must map to a real column. Expression, partial, and
        // non-b-tree indexes are skipped: a partial unique index is not a
        // table-wide guarantee, an expression key has no column to apply, and a
        // GIN/GiST/BRIN/hash index has no accelerator index/sort analog (its
        // `indoption` DESC bits are b-tree-only and read as garbage).
        let column_names: Option<Vec<String>> =
            columns.iter().map(|column| column.name.clone()).collect();
        let usable =
            column_names.is_some() && !acc.has_expressions && !acc.is_partial && acc.is_btree;

        if acc.is_clustered && usable {
            clustered_sort = Some(
                columns
                    .iter()
                    .filter_map(|key_column| {
                        key_column.name.clone().map(|column| InferredSortColumn {
                            column,
                            desc: key_column.desc,
                            nulls_first: Some(key_column.nulls_first),
                        })
                    })
                    .collect(),
            );
        }

        if acc.is_primary {
            if let Some(names) = &column_names {
                primary_key.clone_from(names);
            }
            continue;
        }

        if usable && let Some(names) = &column_names {
            indexes.push(InferredIndex {
                columns: names.clone(),
                unique: acc.is_unique,
            });
        }
    }

    // The primary key is also reported as a unique index — drop the duplicate.
    if !primary_key.is_empty() {
        indexes.retain(|index| index.columns != primary_key);
    }

    // Sort heuristic, best signal first:
    // 1. clustered index (an explicit `CLUSTER`, with per-column direction and
    //    NULLS placement);
    // 2. range/list partition key, then any remaining primary-key columns — the
    //    partition key is the DBA's declaration of the table's dominant access
    //    dimension (commonly time), so leading with it clusters accelerated files
    //    along that dimension for pruning. PostgreSQL requires a partitioned
    //    table's primary key to contain the partition key, so this typically
    //    reorders the PK fallback rather than adding columns;
    // 3. a natural-order column from `pg_stats` — a high-cardinality column whose
    //    physical-order correlation is near `±1` (an append-mostly heap ordered by
    //    `created_at` without any `CLUSTER`), then the remaining primary key;
    // 4. the primary key, ascending.
    let partition_sort_prefix = partition_key.and_then(PartitionKey::sort_prefix);
    let natural_order_prefix = natural_order_sort_candidate(stats_by_column);
    let sort_columns = clustered_sort.unwrap_or_else(|| {
        let mut sort: Vec<InferredSortColumn> = match (partition_sort_prefix, natural_order_prefix)
        {
            (Some(partition_columns), _) => partition_columns
                .into_iter()
                .map(|column| InferredSortColumn {
                    column,
                    desc: false,
                    nulls_first: None,
                })
                .collect(),
            (None, Some(natural)) => vec![natural],
            (None, None) => Vec::new(),
        };
        for column in &primary_key {
            if !sort.iter().any(|sc| &sc.column == column) {
                sort.push(InferredSortColumn {
                    column: column.clone(),
                    desc: false,
                    nulls_first: None,
                });
            }
        }
        sort
    });

    // The declared distribution key: partition-key columns under any strategy
    // (hash included — it scatters rather than orders, but its columns are still
    // the dimension the source distributes by).
    let shard_key = partition_key
        .and_then(PartitionKey::shard_columns)
        .unwrap_or_default();

    // Emit stats only for acceleration-relevant columns (keys, indexes, sort,
    // shard key) — schema metadata rides on every provider schema, so a wide
    // table must not bloat it with stats for every column.
    let mut relevant: BTreeSet<&str> = BTreeSet::new();
    relevant.extend(primary_key.iter().map(String::as_str));
    relevant.extend(
        indexes
            .iter()
            .flat_map(|index| index.columns.iter().map(String::as_str)),
    );
    relevant.extend(sort_columns.iter().map(|sc| sc.column.as_str()));
    relevant.extend(shard_key.iter().map(String::as_str));
    let column_stats: Vec<InferredColumnStats> = stats_by_column
        .values()
        .filter(|stats| relevant.contains(stats.column.as_str()))
        .cloned()
        .collect();

    InferredSchema {
        primary_key,
        indexes,
        sort_columns,
        // Table sizing is added by the caller (it needs a second catalog query).
        row_count: None,
        table_bytes: None,
        shard_key,
        column_stats,
    }
}

/// Pick the strongest natural-order column from the table's statistics: the
/// highest `|correlation|` at or above [`NATURAL_ORDER_MIN_CORRELATION`] among
/// columns with at least [`NATURAL_ORDER_MIN_DISTINCT`] distinct values. A
/// negative correlation means the heap is reverse-ordered, so the sort is
/// descending. NULLS placement is unknown (`pg_stats` ignores NULLs).
fn natural_order_sort_candidate(
    stats_by_column: &BTreeMap<String, InferredColumnStats>,
) -> Option<InferredSortColumn> {
    stats_by_column
        .values()
        .filter_map(|stats| {
            let correlation = stats.correlation?;
            let distinct = stats.distinct_count?;
            (correlation.abs() >= NATURAL_ORDER_MIN_CORRELATION
                && distinct >= NATURAL_ORDER_MIN_DISTINCT)
                .then_some((stats, correlation))
        })
        .max_by(|(_, a), (_, b)| {
            a.abs()
                .partial_cmp(&b.abs())
                .unwrap_or(std::cmp::Ordering::Equal)
        })
        .map(|(stats, correlation)| InferredSortColumn {
            column: stats.column.clone(),
            desc: correlation < 0.0,
            nulls_first: None,
        })
}

/// Enrich the provider's schema with `PostgreSQL` metadata: column/table comments
/// and source types (always), plus inferred primary key / indexes / sort columns
/// when the dataset opts into `schema_inference: extended`.
async fn enrich_with_postgres_metadata(
    pool: &Arc<PostgresConnectionPool>,
    dataset: &Dataset,
    provider: Arc<dyn TableProvider>,
) -> Arc<dyn TableProvider> {
    let (mut table_metadata, field_metadata) =
        match postgres_comment_metadata(pool, dataset.path()).await {
            Ok(metadata) => metadata,
            Err(error) => {
                tracing::warn!(
                    dataset = %dataset.name,
                    source = %dataset.path(),
                    error = %error,
                    "Failed to query PostgreSQL comments; registering without comment metadata"
                );
                (HashMap::new(), data_components::FieldMetadata::new())
            }
        };

    if dataset.schema_inference.is_extended() {
        match postgres_inferred_schema_metadata(pool, dataset.path()).await {
            Ok(inferred) => {
                if !inferred.is_empty() {
                    tracing::debug!(
                        dataset = %dataset.name,
                        source = %dataset.path(),
                        primary_key = ?inferred.primary_key,
                        indexes = inferred.indexes.len(),
                        sort_columns = inferred.sort_columns.len(),
                        row_count = ?inferred.row_count,
                        table_bytes = ?inferred.table_bytes,
                        "Inferred extended schema metadata from PostgreSQL catalog"
                    );
                }
                table_metadata.extend(inferred.to_metadata());
            }
            Err(error) => {
                tracing::warn!(
                    dataset = %dataset.name,
                    source = %dataset.path(),
                    error = %error,
                    "Failed to infer extended schema from PostgreSQL catalog; registering without inferred metadata"
                );
            }
        }
    }

    if table_metadata.is_empty() && field_metadata.is_empty() {
        provider
    } else {
        data_components::metadata_enriched_table_provider(provider, table_metadata, field_metadata)
    }
}

/// Build a federated `PostgreSQL` read provider with the Spice function deny-list
/// installed, so Spice-only UDFs (`json_get_str`, etc.) are evaluated locally
/// instead of being unparsed into the SQL sent to `PostgreSQL`, which would
/// reject them. This mirrors the upstream `PostgresTableFactory::table_provider`
/// internals but routes pushdown decisions through
/// [`create_spice_federated_table_provider`] (the upstream `SqlTable`'s
/// `can_execute_plan` defaults to always-federate and ignores the deny-list). See
/// issue #10703.
async fn federated_postgres_table_provider(
    pool: Arc<PostgresConnectionPool>,
    table_reference: TableReference,
) -> std::result::Result<Arc<dyn TableProvider + 'static>, Box<dyn std::error::Error + Send + Sync>>
{
    let dyn_pool: Arc<DynPostgresConnectionPool> = pool;
    let sql_table = Arc::new(
        SqlTable::new(
            "postgres",
            &dyn_pool,
            table_reference.clone(),
            Some(Engine::Postgres),
        )
        .await
        .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?
        .with_dialect(Arc::new(PostgreSqlDialect {})),
    );

    let schema = sql_table.schema();
    Ok(Arc::new(create_spice_federated_table_provider(
        sql_table,
        schema,
        table_reference,
        Some(deny_spice_specific_functions().as_ref().clone()),
    )))
}

#[async_trait]
impl DataConnector for Postgres {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn read_write_provider(
        &self,
        dataset: &Dataset,
    ) -> Option<DataConnectorResult<Arc<dyn TableProvider>>> {
        match self
            .factory
            .read_write_table_provider(dataset.path().into())
            .await
        {
            Ok(provider) => Some(Ok(enrich_with_postgres_metadata(
                &self.pool, dataset, provider,
            )
            .await)),
            Err(e) => {
                if let Some(err_source) = e.source() {
                    match err_source.downcast_ref::<dbconnection::Error>() {
                        Some(dbconnection::Error::UndefinedTable {
                            table_name,
                            source: _,
                        }) => {
                            return Some(Err(DataConnectorError::InvalidTableName {
                                dataconnector: "postgres".to_string(),
                                connector_component: ConnectorComponent::from(dataset),
                                table_name: table_name.clone(),
                            }));
                        }
                        Some(dbconnection::Error::UnsupportedDataType {
                            data_type,
                            field_name,
                        }) => {
                            return Some(Err(DataConnectorError::UnsupportedDataType {
                                dataconnector: "postgres".to_string(),
                                connector_component: ConnectorComponent::from(dataset),
                                data_type: data_type.clone(),
                                field_name: field_name.clone(),
                            }));
                        }
                        _ => {}
                    }
                }

                Some(Err(DataConnectorError::UnableToGetReadWriteProvider {
                    dataconnector: "postgres".to_string(),
                    connector_component: ConnectorComponent::from(dataset),
                    source: e,
                }))
            }
        }
    }

    async fn read_provider(
        &self,
        dataset: &Dataset,
    ) -> DataConnectorResult<Arc<dyn TableProvider>> {
        match federated_postgres_table_provider(Arc::clone(&self.pool), dataset.path().into()).await
        {
            Ok(provider) => Ok(enrich_with_postgres_metadata(&self.pool, dataset, provider).await),
            Err(e) => {
                if let Some(err_source) = e.source() {
                    match err_source.downcast_ref::<dbconnection::Error>() {
                        Some(dbconnection::Error::UndefinedTable {
                            table_name,
                            source: _,
                        }) => {
                            return Err(DataConnectorError::InvalidTableName {
                                dataconnector: "postgres".to_string(),
                                connector_component: ConnectorComponent::from(dataset),
                                table_name: table_name.clone(),
                            });
                        }
                        Some(dbconnection::Error::UnsupportedDataType {
                            data_type,
                            field_name,
                        }) => {
                            return Err(DataConnectorError::UnsupportedDataType {
                                dataconnector: "postgres".to_string(),
                                connector_component: ConnectorComponent::from(dataset),
                                data_type: data_type.clone(),
                                field_name: field_name.clone(),
                            });
                        }
                        _ => {}
                    }
                }

                Err(DataConnectorError::UnableToGetReadProvider {
                    dataconnector: "postgres".to_string(),
                    connector_component: ConnectorComponent::from(dataset),
                    source: e,
                })
            }
        }
    }

    fn supports_changes_stream(&self) -> bool {
        true
    }

    fn changes_stream(
        &self,
        federated_table: Arc<runtime::federated_table::FederatedTable>,
        dataset: &Dataset,
        _accelerated_table_provider: Arc<dyn TableProvider>,
        _accelerator_write_mutex: Arc<tokio::sync::Mutex<()>>,
        _cpu_runtime: Option<tokio::runtime::Handle>,
    ) -> Option<data_components::cdc::ChangesStream> {
        Some(replication::build_changes_stream(
            &self.params,
            dataset,
            federated_table,
            Arc::clone(&self.replication_metrics),
        ))
    }

    fn metrics_provider(&self) -> Option<Arc<dyn MetricsProvider>> {
        Some(Arc::new(replication::PostgresMetricsProvider::new(
            data_components::postgres_replication::ReplicationMetrics::new(Arc::clone(
                &self.replication_metrics,
            )),
        )))
    }
}

/// The name used to identify this connector in configuration.
pub const CONNECTOR_NAME: &str = "postgres";

/// Returns a new instance of the `PostgreSQL` connector factory.
#[must_use]
pub fn factory() -> Arc<dyn DataConnectorFactory> {
    PostgresFactory::new_arc()
}

#[cfg(test)]
mod inferred_schema_tests {
    use super::{
        IndexAccumulator, IndexKeyColumn, InferredColumnStats, InferredIndex, InferredSortColumn,
        PartitionKey, RawColumnStats, inferred_schema_from_indexes,
    };
    use std::collections::BTreeMap;

    /// Build index key columns from `(name, descending)` pairs (NULLs placement
    /// defaults to last, i.e. the bit unset).
    fn cols(items: &[(&str, bool)]) -> Vec<IndexKeyColumn> {
        items
            .iter()
            .enumerate()
            .map(|(i, (name, desc))| IndexKeyColumn {
                ordinal: i64::try_from(i).expect("ordinal fits in i64"),
                name: Some((*name).to_string()),
                desc: *desc,
                nulls_first: false,
            })
            .collect()
    }

    /// A plain (non-primary, non-unique, non-clustered) b-tree index; tests flip
    /// flags as needed.
    fn index(columns: Vec<IndexKeyColumn>) -> IndexAccumulator {
        IndexAccumulator {
            is_primary: false,
            is_unique: false,
            is_clustered: false,
            is_partial: false,
            has_expressions: false,
            is_btree: true,
            columns,
        }
    }

    fn by_index(entries: Vec<(&str, IndexAccumulator)>) -> BTreeMap<String, IndexAccumulator> {
        entries
            .into_iter()
            .map(|(name, acc)| (name.to_string(), acc))
            .collect()
    }

    fn partition_key(strategy: &str, columns: &[&str]) -> PartitionKey {
        PartitionKey {
            strategy: strategy.to_string(),
            columns: columns.iter().map(|c| Some((*c).to_string())).collect(),
        }
    }

    fn no_stats() -> BTreeMap<String, InferredColumnStats> {
        BTreeMap::new()
    }

    fn stats(
        entries: &[(&str, Option<u64>, Option<f64>)],
    ) -> BTreeMap<String, InferredColumnStats> {
        entries
            .iter()
            .map(|(column, distinct_count, correlation)| {
                (
                    (*column).to_string(),
                    InferredColumnStats {
                        column: (*column).to_string(),
                        distinct_count: *distinct_count,
                        correlation: *correlation,
                    },
                )
            })
            .collect()
    }

    /// Ascending sort column with no declared NULLS placement (the fallback form).
    fn asc(column: &str) -> InferredSortColumn {
        InferredSortColumn {
            column: column.to_string(),
            desc: false,
            nulls_first: None,
        }
    }

    #[test]
    fn composite_primary_key_with_pk_fallback_sort() {
        let mut pk = index(cols(&[("warehouse_id", false), ("sku", false)]));
        pk.is_primary = true;
        pk.is_unique = true;

        let schema =
            inferred_schema_from_indexes(&by_index(vec![("orders_pkey", pk)]), None, &no_stats());

        assert_eq!(
            schema.primary_key,
            vec!["warehouse_id".to_string(), "sku".to_string()]
        );
        assert!(schema.indexes.is_empty());
        // No clustered index → sort falls back to the primary key, ascending.
        assert_eq!(
            schema.sort_columns,
            vec![
                InferredSortColumn {
                    column: "warehouse_id".to_string(),
                    desc: false,
                    nulls_first: None,
                },
                InferredSortColumn {
                    column: "sku".to_string(),
                    desc: false,
                    nulls_first: None,
                },
            ]
        );
    }

    #[test]
    fn unique_and_non_unique_secondary_indexes() {
        let mut pk = index(cols(&[("id", false)]));
        pk.is_primary = true;
        pk.is_unique = true;
        let mut uq = index(cols(&[("email", false)]));
        uq.is_unique = true;
        let plain = index(cols(&[("quantity", false)]));

        let schema = inferred_schema_from_indexes(
            &by_index(vec![
                ("orders_pkey", pk),
                ("uq_email", uq),
                ("idx_qty", plain),
            ]),
            None,
            &no_stats(),
        );

        assert_eq!(schema.primary_key, vec!["id".to_string()]);
        assert_eq!(schema.indexes.len(), 2);
        assert!(schema.indexes.contains(&InferredIndex {
            columns: vec!["email".to_string()],
            unique: true
        }));
        assert!(schema.indexes.contains(&InferredIndex {
            columns: vec!["quantity".to_string()],
            unique: false
        }));
    }

    #[test]
    fn skips_partial_and_expression_indexes() {
        let mut pk = index(cols(&[("id", false)]));
        pk.is_primary = true;
        pk.is_unique = true;
        let mut partial = index(cols(&[("sku", false)]));
        partial.is_unique = true;
        partial.is_partial = true;
        // Expression key → NULL column.
        let mut expr = index(vec![IndexKeyColumn {
            ordinal: 0,
            name: None,
            desc: false,
            nulls_first: false,
        }]);
        expr.has_expressions = true;

        let schema = inferred_schema_from_indexes(
            &by_index(vec![
                ("orders_pkey", pk),
                ("uq_partial", partial),
                ("idx_expr", expr),
            ]),
            None,
            &no_stats(),
        );

        assert_eq!(schema.primary_key, vec!["id".to_string()]);
        assert!(
            schema.indexes.is_empty(),
            "partial and expression indexes must be skipped"
        );
    }

    #[test]
    fn drops_index_duplicating_primary_key() {
        let mut pk = index(cols(&[("id", false)]));
        pk.is_primary = true;
        pk.is_unique = true;
        let mut dup = index(cols(&[("id", false)]));
        dup.is_unique = true;

        let schema = inferred_schema_from_indexes(
            &by_index(vec![("orders_pkey", pk), ("uq_id", dup)]),
            None,
            &no_stats(),
        );

        assert_eq!(schema.primary_key, vec!["id".to_string()]);
        assert!(
            schema.indexes.is_empty(),
            "the primary key's own unique index must not be re-listed"
        );
    }

    #[test]
    fn clustered_index_drives_sort_with_direction() {
        let mut pk = index(cols(&[("id", false)]));
        pk.is_primary = true;
        pk.is_unique = true;
        let mut clustered = index(cols(&[("updated_at", true), ("id", false)]));
        clustered.is_clustered = true;

        let schema = inferred_schema_from_indexes(
            &by_index(vec![("orders_pkey", pk), ("idx_updated", clustered)]),
            None,
            &no_stats(),
        );

        assert_eq!(schema.primary_key, vec!["id".to_string()]);
        // The clustered index defines the physical sort order, preserving DESC.
        // The NULLS placement bit is declared by the index (here: unset = last).
        assert_eq!(
            schema.sort_columns,
            vec![
                InferredSortColumn {
                    column: "updated_at".to_string(),
                    desc: true,
                    nulls_first: Some(false),
                },
                InferredSortColumn {
                    column: "id".to_string(),
                    desc: false,
                    nulls_first: Some(false),
                },
            ]
        );
        // It is also surfaced as a usable secondary index.
        assert!(schema.indexes.contains(&InferredIndex {
            columns: vec!["updated_at".to_string(), "id".to_string()],
            unique: false
        }));
    }

    #[test]
    fn empty_when_no_indexes() {
        let schema = inferred_schema_from_indexes(&BTreeMap::new(), None, &no_stats());
        assert!(schema.is_empty());
    }

    #[test]
    fn skips_non_btree_indexes_including_clustered() {
        let mut pk = index(cols(&[("id", false)]));
        pk.is_primary = true;
        pk.is_unique = true;
        // GIN on a jsonb column: a real column (not an expression), but not b-tree —
        // it has no accelerator index analog and its indoption bits are garbage.
        let mut gin = index(cols(&[("payload", false)]));
        gin.is_btree = false;
        // A clustered GiST index must not drive the sort order either.
        let mut gist = index(cols(&[("region", false)]));
        gist.is_btree = false;
        gist.is_clustered = true;

        let schema = inferred_schema_from_indexes(
            &by_index(vec![
                ("orders_pkey", pk),
                ("idx_payload_gin", gin),
                ("idx_region_gist", gist),
            ]),
            None,
            &no_stats(),
        );

        assert_eq!(schema.primary_key, vec!["id".to_string()]);
        assert!(
            schema.indexes.is_empty(),
            "non-b-tree indexes must be skipped"
        );
        // Sort falls back to the primary key, not the non-b-tree clustered index.
        assert_eq!(
            schema.sort_columns,
            vec![InferredSortColumn {
                column: "id".to_string(),
                desc: false,
                nulls_first: None,
            }]
        );
    }

    #[test]
    fn range_partition_key_leads_fallback_sort() {
        // PG requires the PK of a partitioned table to contain the partition key;
        // the fallback sort should lead with the partition dimension (time) and
        // follow with the remaining PK columns.
        let mut pk = index(cols(&[("id", false), ("created_at", false)]));
        pk.is_primary = true;
        pk.is_unique = true;

        let schema = inferred_schema_from_indexes(
            &by_index(vec![("orders_pkey", pk)]),
            Some(&partition_key("r", &["created_at"])),
            &no_stats(),
        );

        assert_eq!(
            schema.primary_key,
            vec!["id".to_string(), "created_at".to_string()]
        );
        assert_eq!(
            schema.sort_columns,
            vec![
                InferredSortColumn {
                    column: "created_at".to_string(),
                    desc: false,
                    nulls_first: None,
                },
                InferredSortColumn {
                    column: "id".to_string(),
                    desc: false,
                    nulls_first: None,
                },
            ]
        );
    }

    #[test]
    fn hash_partition_key_does_not_affect_sort() {
        // Hash partitioning scatters rather than orders — the fallback stays PK.
        let mut pk = index(cols(&[("id", false)]));
        pk.is_primary = true;
        pk.is_unique = true;

        let schema = inferred_schema_from_indexes(
            &by_index(vec![("orders_pkey", pk)]),
            Some(&partition_key("h", &["id"])),
            &no_stats(),
        );

        assert_eq!(
            schema.sort_columns,
            vec![InferredSortColumn {
                column: "id".to_string(),
                desc: false,
                nulls_first: None,
            }]
        );
    }

    #[test]
    fn expression_partition_key_is_ignored() {
        // An expression key element (attnum 0 → NULL column) makes the partition
        // key unusable as a sort prefix; the fallback stays PK.
        let mut pk = index(cols(&[("id", false)]));
        pk.is_primary = true;
        pk.is_unique = true;
        let expr_key = PartitionKey {
            strategy: "r".to_string(),
            columns: vec![None],
        };

        let schema = inferred_schema_from_indexes(
            &by_index(vec![("orders_pkey", pk)]),
            Some(&expr_key),
            &no_stats(),
        );

        assert_eq!(
            schema.sort_columns,
            vec![InferredSortColumn {
                column: "id".to_string(),
                desc: false,
                nulls_first: None,
            }]
        );
    }

    #[test]
    fn clustered_index_wins_over_partition_key() {
        // An explicit CLUSTER is a stronger signal than the partition key.
        let mut pk = index(cols(&[("id", false), ("created_at", false)]));
        pk.is_primary = true;
        pk.is_unique = true;
        let mut clustered = index(cols(&[("updated_at", true)]));
        clustered.is_clustered = true;

        let schema = inferred_schema_from_indexes(
            &by_index(vec![("orders_pkey", pk), ("idx_updated", clustered)]),
            Some(&partition_key("r", &["created_at"])),
            &no_stats(),
        );

        assert_eq!(
            schema.sort_columns,
            vec![InferredSortColumn {
                column: "updated_at".to_string(),
                desc: true,
                nulls_first: Some(false),
            }]
        );
    }

    #[test]
    fn list_partition_key_without_primary_key_drives_sort() {
        let schema = inferred_schema_from_indexes(
            &BTreeMap::new(),
            Some(&partition_key("l", &["region"])),
            &no_stats(),
        );

        assert!(schema.primary_key.is_empty());
        assert_eq!(schema.sort_columns, vec![asc("region")]);
    }

    #[test]
    fn clustered_index_preserves_nulls_first_bit() {
        let mut clustered = index(vec![IndexKeyColumn {
            ordinal: 0,
            name: Some("updated_at".to_string()),
            desc: true,
            nulls_first: true, // DESC NULLS FIRST — the Postgres DESC default
        }]);
        clustered.is_clustered = true;

        let schema = inferred_schema_from_indexes(
            &by_index(vec![("idx_updated", clustered)]),
            None,
            &no_stats(),
        );

        assert_eq!(
            schema.sort_columns,
            vec![InferredSortColumn {
                column: "updated_at".to_string(),
                desc: true,
                nulls_first: Some(true),
            }]
        );
    }

    #[test]
    fn natural_order_correlation_drives_fallback_sort() {
        // No clustered index and no partition key, but pg_stats says the heap is
        // physically ordered by created_at (an append-mostly table) — it leads
        // the fallback sort, followed by the remaining primary key.
        let mut pk = index(cols(&[("id", false)]));
        pk.is_primary = true;
        pk.is_unique = true;

        let schema = inferred_schema_from_indexes(
            &by_index(vec![("orders_pkey", pk)]),
            None,
            &stats(&[
                ("created_at", Some(50_000), Some(0.98)),
                ("id", Some(50_000), Some(0.42)),
            ]),
        );

        assert_eq!(schema.sort_columns, vec![asc("created_at"), asc("id")]);
    }

    #[test]
    fn negative_correlation_sorts_descending() {
        let schema = inferred_schema_from_indexes(
            &BTreeMap::new(),
            None,
            &stats(&[("created_at", Some(50_000), Some(-0.97))]),
        );

        assert_eq!(
            schema.sort_columns,
            vec![InferredSortColumn {
                column: "created_at".to_string(),
                desc: true,
                nulls_first: None,
            }]
        );
    }

    #[test]
    fn weak_or_low_cardinality_correlation_is_ignored() {
        let mut pk = index(cols(&[("id", false)]));
        pk.is_primary = true;
        pk.is_unique = true;

        let schema = inferred_schema_from_indexes(
            &by_index(vec![("orders_pkey", pk)]),
            None,
            &stats(&[
                // Below the correlation threshold.
                ("created_at", Some(50_000), Some(0.5)),
                // High correlation but flag-like cardinality (a backfill marker).
                ("active", Some(2), Some(1.0)),
                // High correlation, unknown cardinality (never resolved).
                ("batch_no", None, Some(0.99)),
            ]),
        );

        assert_eq!(schema.sort_columns, vec![asc("id")]);
    }

    #[test]
    fn partition_key_beats_correlation() {
        let schema = inferred_schema_from_indexes(
            &BTreeMap::new(),
            Some(&partition_key("r", &["created_at"])),
            &stats(&[("updated_at", Some(50_000), Some(0.99))]),
        );

        assert_eq!(schema.sort_columns, vec![asc("created_at")]);
    }

    #[test]
    fn partition_key_is_emitted_as_shard_key_for_all_strategies() {
        for strategy in ["r", "l", "h"] {
            let schema = inferred_schema_from_indexes(
                &BTreeMap::new(),
                Some(&partition_key(strategy, &["region", "tenant_id"])),
                &no_stats(),
            );
            assert_eq!(
                schema.shard_key,
                vec!["region".to_string(), "tenant_id".to_string()],
                "strategy {strategy} should emit a shard key"
            );
        }

        // Expression key elements make the shard key unusable.
        let expr_key = PartitionKey {
            strategy: "h".to_string(),
            columns: vec![Some("region".to_string()), None],
        };
        let schema = inferred_schema_from_indexes(&BTreeMap::new(), Some(&expr_key), &no_stats());
        assert!(schema.shard_key.is_empty());
    }

    #[test]
    fn column_stats_are_filtered_to_relevant_columns() {
        let mut pk = index(cols(&[("id", false)]));
        pk.is_primary = true;
        pk.is_unique = true;

        let schema = inferred_schema_from_indexes(
            &by_index(vec![("orders_pkey", pk)]),
            None,
            &stats(&[
                ("id", Some(50_000), Some(0.3)),
                // Not a key/index/sort/shard column and not a natural-order
                // candidate — must not bloat the schema metadata.
                ("description", Some(40_000), Some(0.1)),
            ]),
        );

        assert_eq!(schema.column_stats.len(), 1);
        assert_eq!(schema.column_stats[0].column, "id");
    }

    #[test]
    fn raw_stats_normalize_resolves_ratios() {
        // Positive n_distinct: absolute count.
        let absolute = RawColumnStats {
            column: "a".to_string(),
            n_distinct: Some(1234.0),
            correlation: Some(0.5),
        };
        assert_eq!(absolute.normalize(Some(10_000)).distinct_count, Some(1234));

        // Negative n_distinct: fraction of the row estimate.
        let ratio = RawColumnStats {
            column: "b".to_string(),
            n_distinct: Some(-0.5),
            correlation: None,
        };
        assert_eq!(ratio.normalize(Some(10_000)).distinct_count, Some(5_000));
        // ... unresolvable without a row estimate.
        assert_eq!(ratio.normalize(None).distinct_count, None);

        // Zero / unknown.
        let unknown = RawColumnStats {
            column: "c".to_string(),
            n_distinct: Some(0.0),
            correlation: None,
        };
        assert_eq!(unknown.normalize(Some(10_000)).distinct_count, None);
    }
}
