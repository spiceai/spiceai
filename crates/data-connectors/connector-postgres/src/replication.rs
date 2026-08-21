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

//! Glue between Spice's connector params and the `postgres_replication` module.
//!
//! Responsibilities:
//!   - Parse connection & replication params out of `runtime_parameters::Parameters`.
//!   - Fall back to sensible per-replica defaults for slot & publication names.
//!   - Look up the source table schema (via the federated table) and hand everything
//!     off to `data_components::postgres_replication::start_replication_stream`.

use std::sync::Arc;
use std::time::Duration;

use crate::write_back::PG13_SERVER_VERSION_NUM;
use async_stream::try_stream;
use data_components::cdc::{AccelerationContents, ChangesStream, InitialSnapshotMode, StreamError};
use data_components::postgres_replication::{
    AppliedLsn, AppliedLsnStore, NoopAppliedLsnStore, PgOutputFormat, RecordedPosition,
    ReplicationMetrics, ReplicationMetricsCollector, ReplicationParams, ReplicationStreamInput,
    SchemaEvolutionPolicy, XactStatus, XidRegistry, config, start_replication_stream,
};
use data_connector_api::federated::FederatedTableProvider;
use data_connector_api::parameters::ConnectorContext;
use datafusion::sql::TableReference;
use datafusion_table_providers::sql::db_connection_pool::postgrespool::PostgresConnectionPool;
use futures::StreamExt;
use opentelemetry::KeyValue;
use runtime_api_types::v1::ComponentType;
use runtime_checkpoint_api::BlobCheckpointStore;
use runtime_component::dataset::DatasetSpec;
use runtime_metrics::component::{MetricSpec, MetricType, MetricsProvider, ObserveMetricCallback};
use runtime_parameters::{ExposedParamLookup, Parameters};
use secrecy::SecretString;

// Standby status feedback cadence. Kept well below Postgres's default
// `wal_sender_timeout` (60s) so that — combined with the worker's
// feedback-while-backpressured behavior — a slow apply loop never lets the
// server's liveness window lapse and reset the walsender. ~1/6 of the default
// timeout leaves margin under CPU pressure.
const DEFAULT_STATUS_INTERVAL: Duration = Duration::from_secs(5);
const DEFAULT_BOOTSTRAP_BATCH_SIZE: usize = 8192;
const MAX_BOOTSTRAP_BATCH_SIZE: usize = 1_048_576;
// Upper bound on the configurable shared-slot member mailbox capacity. Matches
// the bootstrap-batch ceiling — a bounded, backpressure-preserving queue in
// front of the accelerator prefetch, not an unbounded buffer.
const MAX_MEMBER_CHANNEL_CAPACITY: usize = 1_048_576;

/// Sidecar table, in the dataset's own accelerator, holding the serialized applied-LSN
/// watermark.
const WATERMARK_TABLE: &str = "spice_sys_postgres_replication";

/// Resolve the applied-LSN watermark store over the dataset's own accelerator.
///
/// `None` means nothing durable can record a position — there is no usable accelerator
/// connection. The caller treats that as "never loaded", which is correct: an
/// acceleration that cannot persist a watermark cannot have persisted rows for one to
/// describe.
async fn resolve_watermark_store(
    context: &dyn ConnectorContext,
    dataset: &DatasetSpec,
) -> Option<Arc<dyn BlobCheckpointStore>> {
    context
        .blob_checkpoint_store(dataset, WATERMARK_TABLE)
        .await
}

/// [`AppliedLsnStore`] over the accelerator's `spice_sys_postgres_replication`
/// sidecar.
///
/// The payload is a single `{"lsn": <u64>}` object rather than the bare number,
/// so the record can gain fields (a slot identity, a snapshot as-of marker)
/// without a migration.
struct SidecarAppliedLsnStore {
    blobs: Arc<dyn BlobCheckpointStore>,
    /// Which source the recorded position belongs to (see [`source_identity`]).
    ///
    /// LSNs are only comparable within one source's history. Without this, a
    /// dataset repointed to another server, database, or table while keeping its
    /// acceleration files would compare the new source's LSNs against a
    /// watermark describing the old one's contents — and a low new LSN reads as
    /// "already covered", leaving the old rows in place and never loading the
    /// new source's.
    identity: String,
}

/// Identity of the source a watermark was recorded against: endpoint, database,
/// and table.
///
/// Deliberately not the slot name — a different slot on the same server shares
/// its LSN space, so slot changes stay comparable. Note this does not detect a
/// same-endpoint cluster restored from a backup or rewound by PITR, whose LSNs
/// can move backwards; the resume-position clamp in `postgres_replication` is
/// what keeps that from silently skipping changes.
fn source_identity(params: &ReplicationParams, schema: &str, table: &str) -> String {
    format!(
        "{}:{}/{}/{}.{}",
        params.host, params.port, params.database, schema, table
    )
}

/// Serialized form of [`AppliedLsn`] in the sidecar.
#[derive(serde::Serialize, serde::Deserialize)]
struct StoredAppliedLsn {
    lsn: u64,
    /// The source this position was recorded against. Absent in records written
    /// before the field existed, which are treated as belonging to a different
    /// source — the conservative reading, since they cannot be verified.
    #[serde(default)]
    source: Option<String>,
}

#[async_trait::async_trait]
impl AppliedLsnStore for SidecarAppliedLsnStore {
    async fn load(
        &self,
    ) -> std::result::Result<RecordedPosition, Box<dyn std::error::Error + Send + Sync>> {
        let Some(checkpoint) = self.blobs.get().await? else {
            return Ok(RecordedPosition::Absent);
        };
        // A row that exists but cannot be parsed is surfaced, not swallowed:
        // treating corruption as "no watermark" would silently resume as if this
        // were a first load, and the caller's fallback for an unreadable
        // watermark is a rebuild — the safe direction.
        let stored: StoredAppliedLsn = serde_json::from_str(&checkpoint.data)?;
        if stored.source.as_deref() != Some(self.identity.as_str()) {
            tracing::warn!(
                recorded_for = stored.source.as_deref().unwrap_or("an unrecorded source"),
                streaming_from = %self.identity,
                "this acceleration's recorded position belongs to a different source, so its contents cannot be resumed against this one; it will be rebuilt from the source"
            );
            return Ok(RecordedPosition::ForeignSource);
        }
        Ok(RecordedPosition::At(AppliedLsn { lsn: stored.lsn }))
    }

    async fn save(
        &self,
        applied: AppliedLsn,
    ) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let payload = serde_json::to_string(&StoredAppliedLsn {
            lsn: applied.lsn,
            source: Some(self.identity.clone()),
        })?;
        self.blobs.upsert(&payload).await?;
        Ok(())
    }

    async fn clear(&self) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // The sidecar exposes upsert-only semantics, so "forget" is recorded as a
        // zero watermark: it precedes every real LSN, so the comparison against
        // what the slot can supply resolves to a rebuild, which is what clearing
        // is for.
        self.save(AppliedLsn { lsn: 0 }).await
    }
}

/// Load the outstanding-write-back-transaction registry for a dataset.
///
/// The registry persists into `spice_sys_postgres_write_back_xids`, a sibling of
/// the applied-LSN watermark in the dataset's own accelerator, keyed by the same
/// [`source_identity`] so a repointed accelerator discards a foreign set. This is
/// the single construction site: the deliverer takes the returned `Arc`, and a
/// follow-up can hand the *same* `Arc` to the replication member registration for
/// the pump's echo filter.
///
/// `None` when there is no usable accelerator connection to persist into, or the
/// connection params cannot be parsed — the caller then disables connector-owned
/// delivery for the dataset and falls back to the worker's `TableProvider` path.
pub(crate) async fn load_write_back_xid_registry(
    params: &Parameters,
    dataset: &DatasetSpec,
    context: &dyn ConnectorContext,
) -> Option<Arc<XidRegistry>> {
    let dataset_name = dataset.name.to_string();
    let repl_params = match replication_params_from_connector_params(params, &dataset_name) {
        Ok(p) => p,
        Err(e) => {
            tracing::warn!(
                dataset = %dataset_name,
                "durable write-back for dataset '{dataset_name}' could not resolve its source identity, so change-echo suppression is disabled for it: {e}"
            );
            return None;
        }
    };
    let (schema_name, table_name) = split_schema_table(&dataset.from);
    let identity = source_identity(&repl_params, &schema_name, &table_name);

    let store = context
        .blob_checkpoint_store(dataset, crate::write_back::WRITE_BACK_XID_TABLE)
        .await?;

    match XidRegistry::load(store, identity, dataset_name.clone()).await {
        Ok(registry) => Some(registry),
        Err(e) => {
            tracing::warn!(
                dataset = %dataset_name,
                error = %e,
                "durable write-back for dataset '{dataset_name}' could not load its change-echo suppression registry, so suppression is disabled for it until restart"
            );
            None
        }
    }
}

/// Startup garbage collection for a freshly-loaded write-back registry.
///
/// Runs once, before the registry is shared with the delivery path or the pump.
/// Resolves what the registry cannot on its own — each outstanding entry's
/// `pg_xact_status` and the server's current transaction id — and hands them to
/// [`XidRegistry::gc`] together with the dataset's durably-applied LSN, so an
/// aborted delivery, a lost unregister, or an entry stranded far behind the
/// server is dropped rather than lingering into a 32-bit xid wraparound.
///
/// Best-effort: a resolution failure is logged and leaves the entries in place
/// (steady-state pruning still bounds them); it never blocks or fails setup.
pub(crate) async fn run_write_back_registry_gc(
    pool: &Arc<PostgresConnectionPool>,
    params: &Parameters,
    dataset: &DatasetSpec,
    context: &dyn ConnectorContext,
    registry: &Arc<XidRegistry>,
) {
    let dataset_name = dataset.name.to_string();
    let outstanding = registry.outstanding_xid8s().await;
    if outstanding.is_empty() {
        // Nothing to reconcile, so skip the server round trip entirely.
        return;
    }

    let db = match pool.connect_direct().await {
        Ok(db) => db,
        Err(e) => {
            tracing::warn!(
                dataset = %dataset_name,
                error = %e,
                "durable write-back for dataset '{dataset_name}' could not open a connection to garbage-collect its change-echo suppression registry, so stale entries are left for steady-state pruning"
            );
            return;
        }
    };

    let server_version_num: i32 = match db
        .conn
        .query_one("SELECT current_setting('server_version_num')::int4", &[])
        .await
    {
        Ok(row) => row.get(0),
        Err(e) => {
            tracing::warn!(
                dataset = %dataset_name,
                error = %e,
                "durable write-back for dataset '{dataset_name}' could not read the source server version to garbage-collect its change-echo suppression registry, so stale entries are left for steady-state pruning"
            );
            return;
        }
    };
    // `pg_xact_status`/`pg_snapshot_xmax` are PG13+; PG10-12 use the `txid_*`
    // equivalents (same semantics). `pg_snapshot_xmax` reads the current xid8
    // without assigning one, so garbage collection never consumes an xid.
    let pg13_plus = server_version_num >= PG13_SERVER_VERSION_NUM;
    let (current_xid_sql, status_sql) = if pg13_plus {
        (
            "SELECT pg_snapshot_xmax(pg_current_snapshot())::text",
            "SELECT pg_xact_status($1::xid8)::text",
        )
    } else {
        (
            "SELECT txid_snapshot_xmax(txid_current_snapshot())::text",
            "SELECT txid_status($1::bigint)::text",
        )
    };

    let current_xid8 = match read_u64_text(&db.conn, current_xid_sql).await {
        Ok(value) => value,
        Err(e) => {
            tracing::warn!(
                dataset = %dataset_name,
                error = %e,
                "durable write-back for dataset '{dataset_name}' could not read the source's current transaction id to garbage-collect its change-echo suppression registry, so stale entries are left for steady-state pruning"
            );
            return;
        }
    };

    let mut statuses: std::collections::HashMap<u64, XactStatus> =
        std::collections::HashMap::with_capacity(outstanding.len());
    for xid8 in outstanding {
        // Bind the id as its decimal text and cast in SQL (`$1::xid8` /
        // `$1::bigint`), so no `xid8` `ToSql`/`FromSql` is needed; the status is
        // likewise read as text.
        let param = xid8.to_string();
        let status = match db.conn.query_one(status_sql, &[&param]).await {
            Ok(row) => {
                let raw: Option<String> = row.get(0);
                xact_status_from_text(raw.as_deref())
            }
            Err(e) => {
                tracing::warn!(
                    dataset = %dataset_name,
                    xid8,
                    error = %e,
                    "durable write-back for dataset '{dataset_name}' could not read a transaction's status while garbage-collecting its change-echo suppression registry; that entry is left to the epoch-distance safety valve"
                );
                XactStatus::Unknown
            }
        };
        statuses.insert(xid8, status);
    }

    // The durably-applied LSN, so garbage collection can drop an entry whose echo
    // the applied floor has provably consumed. An unresolvable watermark reads as
    // 0, which only makes that one rule conservative (the aborted and
    // epoch-distance rules still apply).
    let applied_lsn = resolve_applied_lsn(params, dataset, context).await;

    registry.gc(&statuses, current_xid8, applied_lsn).await;
}

/// Read a single decimal `u64` returned as text (used for xid8 values, whose
/// `FromSql` the driver does not provide).
async fn read_u64_text(
    conn: &tokio_postgres::Client,
    sql: &str,
) -> std::result::Result<u64, String> {
    let row = conn.query_one(sql, &[]).await.map_err(|e| e.to_string())?;
    let raw: String = row.get(0);
    raw.trim().parse::<u64>().map_err(|e| e.to_string())
}

/// Map `pg_xact_status`/`txid_status` text to the registry's [`XactStatus`]. A
/// NULL result (too old to resolve) is [`XactStatus::Unknown`], handled by the
/// epoch-distance safety valve rather than the aborted rule.
fn xact_status_from_text(status: Option<&str>) -> XactStatus {
    match status {
        Some("committed") => XactStatus::Committed,
        Some("aborted") => XactStatus::Aborted,
        Some("in progress") => XactStatus::InProgress,
        _ => XactStatus::Unknown,
    }
}

/// The dataset's durably-applied LSN, read from the same sidecar watermark the
/// pump resumes from. Returns 0 when nothing durable is recorded (ephemeral
/// accelerator, unreachable store, foreign source) — the conservative input for
/// the applied-floor garbage-collection rule.
async fn resolve_applied_lsn(
    params: &Parameters,
    dataset: &DatasetSpec,
    context: &dyn ConnectorContext,
) -> u64 {
    let Ok(repl_params) =
        replication_params_from_connector_params(params, &dataset.name.to_string())
    else {
        return 0;
    };
    let (schema_name, table_name) = split_schema_table(&dataset.from);
    let Some(blobs) = resolve_watermark_store(context, dataset).await else {
        return 0;
    };
    let store = SidecarAppliedLsnStore {
        blobs,
        identity: source_identity(&repl_params, &schema_name, &table_name),
    };
    match store.load().await {
        Ok(RecordedPosition::At(applied)) => applied.lsn,
        _ => 0,
    }
}

/// `async` so the watermark store is resolved here, before the stream is built: the
/// generator then holds only the resolved store, which owns a connection pool and no
/// runtime and so cannot pin the runtime for as long as the stream lives.
pub async fn build_changes_stream(
    params: &Parameters,
    dataset: &DatasetSpec,
    context: &dyn ConnectorContext,
    federated_table: Arc<dyn FederatedTableProvider>,
    metrics: Arc<ReplicationMetricsCollector>,
    acceleration: AccelerationContents,
    write_back_registry: Option<Arc<XidRegistry>>,
) -> ChangesStream {
    let dataset_name = dataset.name.to_string();
    let (schema_name, table_name) = split_schema_table(&dataset.from);

    let mut params_for_stream =
        match replication_params_from_connector_params(params, &dataset_name) {
            Ok(p) => p,
            Err(e) => {
                let msg = format!("postgres replication: {e}");
                return Box::pin(futures::stream::once(async move {
                    Err(StreamError::External(msg))
                }));
            }
        };

    // A non-persistent accelerator starts empty on every boot, so resuming
    // from an existing slot without a snapshot would silently serve only the
    // rows touched after startup. Force the snapshot on every start for such
    // accelerators (snapshot + WAL resume converges via the PK upsert). Only
    // applies when snapshots are enabled at all — `disabled` opts out entirely.
    //
    // Recorded on the params either way (independently of `initial_snapshot`,
    // which only governs snapshotting): such a slot also has no resume value
    // across restarts, so the stream drops it on graceful shutdown instead of
    // leaving it pinning WAL on the source.
    let ephemeral = dataset
        .acceleration
        .as_ref()
        .is_some_and(accelerator_is_ephemeral);
    params_for_stream.ephemeral_accelerator = ephemeral;
    // Observed by the runtime just before this stream was built, and only ever
    // read to decide whether a *missing* watermark is evidence of a gap.
    params_for_stream.acceleration = acceleration;
    if params_for_stream.initial_snapshot && ephemeral {
        params_for_stream.snapshot_on_resume = true;
        tracing::info!(
            dataset = %dataset_name,
            "non-persistent accelerator with `refresh_mode: changes`: the initial snapshot \
             will run on every start, including replication-slot resume"
        );
    }

    // Where this dataset's applied-LSN watermark lives. An ephemeral acceleration
    // gets the no-op store: it boots empty and re-snapshots every start, so a
    // recorded position would describe rows the restart already threw away, and
    // resuming on it would skip everything before it.
    //
    // A durable acceleration with no reachable store also records nothing, which
    // reads as "never loaded" — correct, since an acceleration that cannot persist a
    // watermark cannot have persisted the rows one would describe.
    let applied_lsn_store: Arc<dyn AppliedLsnStore> = if ephemeral {
        Arc::new(NoopAppliedLsnStore)
    } else {
        match resolve_watermark_store(context, dataset).await {
            Some(blobs) => Arc::new(SidecarAppliedLsnStore {
                blobs,
                identity: source_identity(&params_for_stream, &schema_name, &table_name),
            }),
            None => Arc::new(NoopAppliedLsnStore),
        }
    };

    // Prefer the dataset's explicitly-declared acceleration `primary_key` —
    // that's what the accelerator write path uses for upsert/delete, and it's
    // what the operator configured. Fall back to the source TableProvider's
    // constraints only if acceleration didn't declare one.
    let declared_pks: Vec<String> = dataset
        .acceleration
        .as_ref()
        .and_then(|a| a.primary_key.as_ref())
        .map(|pk| pk.iter().map(ToString::to_string).collect())
        .unwrap_or_default();

    // UPDATE events rely on `on_conflict: upsert` (or an upsert-dedup
    // variant) to mutate the existing row in place, and the conflict target
    // MUST match the dataset's primary key — otherwise the accelerator's
    // write path falls through to append and silently inserts duplicate rows
    // on every UPDATE. DuckDB / SQLite / Postgres / Cayenne all need this;
    // the Arrow engine genuinely can't support upsert and is documented as
    // append-only for UPDATEs — skip the check there.
    let engine = dataset
        .acceleration
        .as_ref()
        .map(|a| a.engine.to_unpartitioned())
        .unwrap_or_default();
    let engine_supports_upsert = !matches!(
        engine,
        runtime_component::dataset::acceleration::Engine::Arrow
            | runtime_component::dataset::acceleration::Engine::PartitionedArrow
    );
    // The on_conflict map is keyed on a ColumnReference (same type as
    // primary_key), so checking whether the PK has an Upsert entry is a
    // direct lookup. This is a much tighter check than "any value is
    // Upsert" — a misconfigured `on_conflict` targeting a non-PK column
    // would previously pass and still let UPDATEs append duplicate rows.
    let has_upsert_on_pk = dataset.acceleration.as_ref().is_some_and(|a| {
        a.primary_key.as_ref().is_some_and(|pk| {
            matches!(
                a.on_conflict.get(pk),
                Some(runtime_component::dataset::acceleration::OnConflictBehavior::Upsert(_))
            )
        })
    });

    // Map the dataset-level `on_schema_change` policy onto the replication
    // stream. `Block` (the default) preserves today's behavior verbatim; the
    // other policies let the stream adopt widening source-relation changes
    // mid-stream (the runtime apply loop still enforces the per-policy
    // evolution set). `OnSchemaChange` is `Copy`, so capture it by value.
    let schema_evolution_policy = match dataset.on_schema_change {
        runtime_component::dataset::OnSchemaChange::Block => SchemaEvolutionPolicy::Block,
        runtime_component::dataset::OnSchemaChange::Fail => SchemaEvolutionPolicy::Fail,
        runtime_component::dataset::OnSchemaChange::AppendNewColumns => {
            SchemaEvolutionPolicy::AppendNewColumns
        }
        // A CDC stream cannot drop-and-recreate without losing un-replayable history, so
        // `drop_and_recreate` adopts widening changes like `sync_all_columns` and rejects
        // incompatible changes mid-stream. The accelerated table is recreated only on a
        // `refresh_mode: full` registration, not from the replication stream.
        runtime_component::dataset::OnSchemaChange::SyncAllColumns
        | runtime_component::dataset::OnSchemaChange::DropAndRecreate => {
            SchemaEvolutionPolicy::SyncAllColumns
        }
    };

    Box::pin(try_stream! {
        let table_provider = federated_table.table_provider().await;
        let schema = table_provider.schema();

        // Seed bootstrap progress from the inferred rough row count (extended schema
        // inference), if available, so the initial snapshot can report progress/ETA.
        if let Some(expected) =
            data_components::inferred_schema::InferredSchema::from_metadata(schema.metadata())
                .row_count
        {
            metrics.set_bootstrap_rows_expected(expected);
        }

        let primary_keys = if declared_pks.is_empty() {
            extract_primary_keys(&table_provider)
        } else {
            declared_pks.clone()
        };

        // refresh_mode: changes is useless without a PK — DELETE and UPDATE
        // require one to route the change to a row. Fail fast with a clear
        // message instead of erroring cryptically later in the refresh loop.
        if primary_keys.is_empty() {
            Err(StreamError::External(format!(
                "postgres replication for dataset `{dataset_name}`: no primary key available. \
                 Set `acceleration.primary_key` on the dataset (and a matching \
                 `acceleration.on_conflict` entry) — `refresh_mode: changes` cannot route \
                 UPDATE/DELETE events without one."
            )))?;
        }

        // Now that primary_keys is resolved, report the upsert-config error
        // with a concrete PK hint. Two cases produce this error:
        //   1. `acceleration.primary_key` is set but `on_conflict` is missing
        //      the Upsert entry keyed on the PK.
        //   2. `acceleration.primary_key` is unset (we're relying on the
        //      source table's PK), in which case the user must also set
        //      `acceleration.primary_key` — `on_conflict` can only be keyed
        //      on a ColumnReference, and acceleration's write path consults
        //      only its own `primary_key` / `on_conflict` config.
        if engine_supports_upsert && !has_upsert_on_pk {
            let pk_hint = primary_keys.first().cloned().unwrap_or_else(|| "<pk>".to_string());
            let msg = if declared_pks.is_empty() {
                format!(
                    "postgres replication for dataset `{dataset_name}`: the source table's \
                     primary key (`{pk_hint}`) is not declared on the dataset. \
                     `refresh_mode: changes` requires BOTH `acceleration.primary_key: {pk_hint}` \
                     AND `acceleration.on_conflict: {{ {pk_hint}: upsert }}` so UPDATE events \
                     apply as upserts on the `{engine}` engine — without the declaration, \
                     the accelerator's write path falls through to append and produces \
                     duplicate rows. (The `arrow` engine is exempt — documented append-only \
                     semantics.)"
                )
            } else {
                format!(
                    "postgres replication for dataset `{dataset_name}`: `refresh_mode: changes` \
                     requires an `acceleration.on_conflict` entry keyed on the dataset's \
                     `primary_key` with an `upsert` (or `upsert_dedup*`) behavior so UPDATE \
                     events apply as upserts instead of duplicate inserts. Add: \
                     `on_conflict: {{ {pk_hint}: upsert }}` on the `{engine}` engine. \
                     (The `arrow` engine is exempt — documented append-only semantics.)"
                )
            };
            Err(StreamError::External(msg))?;
        }

        let input = ReplicationStreamInput {
            dataset_name: dataset_name.clone(),
            params: params_for_stream,
            schema,
            primary_keys,
            schema_name,
            table_name,
            metrics,
            policy: schema_evolution_policy,
            applied_lsn_store,
            write_back_registry,
        };

        let mut inner = start_replication_stream(input);
        while let Some(item) = inner.next().await {
            yield item?;
        }
    })
}

// ---------------------------------------------------------------------------
// MetricsProvider — exposes replication counters/gauges under
// `dataset_postgres_*` in OpenTelemetry.
// ---------------------------------------------------------------------------

const METRICS: &[MetricSpec] = &[
    MetricSpec::new("replication_lag_ms", MetricType::ObservableGaugeU64)
        .description(
            "Milliseconds between now() and the Postgres commit timestamp of the most \
             recent transaction replicated into the accelerator. Primary freshness metric \
             for CDC; uses the source's commit_time from pgoutput, not local ingest time.",
        )
        .unit("ms")
        .auto_register(),
    MetricSpec::new("replication_lag_bytes", MetricType::ObservableGaugeU64)
        .description(
            "Bytes of WAL between the server's latest reported position and our last \
             confirmed flush LSN.",
        )
        .unit("By") // UCUM code for bytes (OpenTelemetry spec §Unit)
        .auto_register(),
    MetricSpec::new(
        "replication_confirmed_flush_lsn",
        MetricType::ObservableGaugeU64,
    )
    .description(
        "Most recent LSN Spice has acknowledged to Postgres. Matches \
             `pg_replication_slots.confirmed_flush_lsn`. Compare its advance rate \
             against the applied watermark to spot slot-ack racing ahead of apply.",
    )
    .auto_register(),
    MetricSpec::new(
        "replication_server_wal_end_lsn",
        MetricType::ObservableGaugeU64,
    )
    .description(
        "Most recent WAL end LSN reported by the Postgres server (via keepalive or WAL data).",
    )
    .auto_register(),
    MetricSpec::new(
        "replication_reader_input_wait_micros_total",
        MetricType::ObservableCounterU64,
    )
    .description(
        "Cumulative microseconds the replication reader spent BLOCKED awaiting the \
         next event from the source socket. High relative to \
         `reader_processing_micros_total` ⇒ source/network can't deliver fast \
         enough (source-bound); low ⇒ our decode/build is the limiter.",
    )
    .unit("us")
    .auto_register(),
    MetricSpec::new(
        "replication_reader_processing_micros_total",
        MetricType::ObservableCounterU64,
    )
    .description(
        "Cumulative microseconds the replication reader spent decoding + building \
         change batches (and yielding downstream) after a socket event. The \
         source-vs-our-decode discriminator, paired with reader_input_wait_micros_total.",
    )
    .unit("us")
    .auto_register(),
    MetricSpec::new(
        "replication_transactions_total",
        MetricType::ObservableCounterU64,
    )
    .description("Total number of transactions committed and applied to the accelerator.")
    .auto_register(),
    MetricSpec::new(
        "replication_inserts_total",
        MetricType::ObservableCounterU64,
    )
    .description("Total INSERT operations received from WAL.")
    .auto_register(),
    MetricSpec::new(
        "replication_updates_total",
        MetricType::ObservableCounterU64,
    )
    .description("Total UPDATE operations received from WAL.")
    .auto_register(),
    MetricSpec::new(
        "replication_deletes_total",
        MetricType::ObservableCounterU64,
    )
    .description("Total DELETE operations received from WAL.")
    .auto_register(),
    MetricSpec::new(
        "replication_truncates_total",
        MetricType::ObservableCounterU64,
    )
    .description("Total TRUNCATE operations received from WAL and applied to the accelerator."),
    MetricSpec::new(
        "replication_bootstrap_rows_total",
        MetricType::ObservableCounterU64,
    )
    .description("Total rows loaded during the initial-snapshot bootstrap phase."),
    MetricSpec::new(
        "replication_bootstrap_rows_expected",
        MetricType::ObservableGaugeU64,
    )
    .description(
        "Estimated total rows for the initial-snapshot bootstrap, from extended schema \
             inference. Not reported when no estimate is available (`0` means a known-empty \
             source table). Progress = bootstrap_rows_total / bootstrap_rows_expected.",
    )
    .auto_register(),
    MetricSpec::new(
        "replication_bootstrap_complete",
        MetricType::ObservableGaugeU64,
    )
    .description(
        "1 once the initial-snapshot bootstrap has finished (or was skipped on resume); \
             0 while the snapshot is still running.",
    )
    .auto_register(),
    MetricSpec::new(
        "replication_decode_errors_total",
        MetricType::ObservableCounterU64,
    )
    .description("Total pgoutput-decoding errors encountered while parsing WAL events.")
    .auto_register(),
    MetricSpec::new(
        "replication_schema_mismatch_errors_total",
        MetricType::ObservableCounterU64,
    )
    .description(
        "Total errors where the source relation no longer matches the declared \
             accelerator schema.",
    )
    .auto_register(),
    MetricSpec::new(
        "replication_recv_errors_total",
        MetricType::ObservableCounterU64,
    )
    .description(
        "Total transport-level errors while receiving from the Postgres replication \
             connection (TCP drops, auth failures after reconnect, etc).",
    )
    .auto_register(),
    MetricSpec::new(
        "replication_reconnects_total",
        MetricType::ObservableCounterU64,
    )
    .description(
        "Number of times the stream reconnected after a transient failure \
         (network drop, Postgres restart). A non-zero value with no user-visible \
         error just means the connection wobbled and we recovered.",
    )
    .auto_register(),
    MetricSpec::new(
        "replication_disconnected_ms_total",
        MetricType::ObservableCounterU64,
    )
    .description(
        "Cumulative milliseconds the replication stream was disconnected across all \
         reconnects (drop → successful resume, including backoff). Paired with \
         replication_reconnects_total it quantifies the DURATION cost of a reconnect \
         storm — no changes are delivered and lag grows while disconnected.",
    )
    .unit("ms")
    .auto_register(),
    MetricSpec::new(
        "replication_member_send_stalled_seconds_total",
        MetricType::ObservableCounterU64,
    )
    .description(
        "Cumulative seconds the shared-slot pump spent blocked delivering committed \
         changes into this dataset's mailbox because its sink was not draining \
         (downstream backpressure). The server replication connection stays alive \
         throughout; a rising value indicates a slow apply loop stalling the shared \
         pump. Only reported for datasets on a shared (explicitly-named) slot.",
    )
    .auto_register(),
    MetricSpec::new(
        "replication_member_attached",
        MetricType::ObservableGaugeU64,
    )
    .description(
        "1 while this dataset is an attached member of its shared replication slot, \
         0 once it has detached. A detached member freezes its ack floor and pins WAL \
         retention for the WHOLE shared slot until it rejoins or spiced restarts, so a \
         value of 0 is the unambiguous signal for which dataset stalled the slot (the \
         lag metric grows on the surviving slot-mates instead). Only reported for \
         datasets on a shared (explicitly-named) slot; a dedicated slot reports no series. \
         Carries a `slot` label for shared-slot grouping.",
    )
    .auto_register(),
    MetricSpec::new(
        "replication_member_send_wait_micros_total",
        MetricType::ObservableCounterU64,
    )
    .description(
        "Cumulative microseconds the shared-slot pump spent awaiting this dataset's \
         delivery mailbox while applying committed changes. Unlike \
         member_send_stalled_seconds_total, this accrues the full per-commit wait \
         (including sub-second waits). The pump subtracts this wait from \
         reader_processing_micros_total at the source, so that counter stays \
         decode-only; this metric exports the subtracted amount for attribution. \
         Only meaningful for datasets on a shared slot; dedicated-slot datasets will export 0.",
    )
    .unit("us")
    .auto_register(),
    MetricSpec::new(
        "replication_member_envelopes_delivered_total",
        MetricType::ObservableCounterU64,
    )
    .description(
        "Change envelopes the shared-slot pump delivered to this dataset as distinct units \
         of work. Divide replication_wal_transactions_total by this to get the coalescing \
         factor the accelerator's apply loop actually sees: adjacent transactions for the \
         same table are folded into one envelope, so this counter rises more slowly than \
         the transaction count. Only reported for datasets on a shared (explicitly-named) \
         slot.",
    )
    .auto_register(),
    MetricSpec::new(
        "replication_member_envelope_eager_merges_total",
        MetricType::ObservableCounterU64,
    )
    .description(
        "Committed transactions folded into an envelope the shared-slot pump was still \
         holding back, before it crossed into this dataset's delivery mailbox. Paired with \
         member_envelope_mailbox_merges_total, this attributes envelope reduction between \
         the pump's short hold and mailbox back-pressure. Only reported for datasets on a \
         shared (explicitly-named) slot.",
    )
    .auto_register(),
    MetricSpec::new(
        "replication_member_envelope_mailbox_merges_total",
        MetricType::ObservableCounterU64,
    )
    .description(
        "Committed transactions folded into an envelope already sitting unclaimed in this \
         dataset's delivery mailbox. This is the back-pressure-driven half of coalescing: \
         it rises when the sink is not keeping up, which is when collapsing envelopes \
         matters most, so a rising value alongside a flat \
         member_send_stalled_seconds_total means back-pressure is being absorbed rather \
         than stalling the slot. Only reported for datasets on a shared (explicitly-named) \
         slot.",
    )
    .auto_register(),
    MetricSpec::new(
        "replication_member_mailbox_coalesce_limited_total",
        MetricType::ObservableCounterU64,
    )
    .description(
        "Times a committed transaction could not be folded into this dataset's unclaimed \
         delivery-mailbox tail because a configured bound refused it, rather than because the \
         changes were not foldable. The mailbox bounds ship deliberately low, since mailbox \
         folding absorbs back-pressure rather than adding throughput. A value that stays at 0 \
         means the bounds never bind and there is nothing to tune; a rising value alongside a \
         rising member_envelope_mailbox_merges_total is the evidence that raising them would \
         absorb more. Only reported for datasets on a shared (explicitly-named) slot.",
    )
    .auto_register(),
];

#[derive(Debug, Clone)]
pub struct PostgresMetricsProvider {
    metrics: ReplicationMetrics,
}

impl PostgresMetricsProvider {
    #[must_use]
    pub fn new(metrics: ReplicationMetrics) -> Self {
        Self { metrics }
    }
}

impl MetricsProvider for PostgresMetricsProvider {
    fn component_type(&self) -> ComponentType {
        ComponentType::Dataset
    }

    fn component_name(&self) -> &'static str {
        "postgres"
    }

    fn available_metrics(&self) -> &'static [MetricSpec] {
        METRICS
    }

    fn callback_to_observe_metric(
        &self,
        metric: &MetricSpec,
        attributes: Vec<KeyValue>,
    ) -> Option<ObserveMetricCallback> {
        let m = self.metrics.clone();
        match metric.name {
            "replication_lag_ms" => Some(ObserveMetricCallback::U64(Box::new(move |instrument| {
                if let Some(v) = m.replication_lag_ms() {
                    instrument.observe(v, &attributes);
                }
            }))),
            "replication_lag_bytes" => {
                Some(ObserveMetricCallback::U64(Box::new(move |instrument| {
                    instrument.observe(m.replication_lag_bytes(), &attributes);
                })))
            }
            "replication_confirmed_flush_lsn" => {
                Some(ObserveMetricCallback::U64(Box::new(move |instrument| {
                    instrument.observe(m.confirmed_flush_lsn(), &attributes);
                })))
            }
            "replication_server_wal_end_lsn" => {
                Some(ObserveMetricCallback::U64(Box::new(move |instrument| {
                    instrument.observe(m.server_wal_end_lsn(), &attributes);
                })))
            }
            "replication_reader_input_wait_micros_total" => {
                Some(ObserveMetricCallback::U64(Box::new(move |instrument| {
                    instrument.observe(m.reader_input_wait_micros_total(), &attributes);
                })))
            }
            "replication_reader_processing_micros_total" => {
                Some(ObserveMetricCallback::U64(Box::new(move |instrument| {
                    instrument.observe(m.reader_processing_micros_total(), &attributes);
                })))
            }
            "replication_transactions_total" => {
                Some(ObserveMetricCallback::U64(Box::new(move |instrument| {
                    instrument.observe(m.wal_transactions_total(), &attributes);
                })))
            }
            "replication_inserts_total" => {
                Some(ObserveMetricCallback::U64(Box::new(move |instrument| {
                    instrument.observe(m.wal_inserts_total(), &attributes);
                })))
            }
            "replication_updates_total" => {
                Some(ObserveMetricCallback::U64(Box::new(move |instrument| {
                    instrument.observe(m.wal_updates_total(), &attributes);
                })))
            }
            "replication_deletes_total" => {
                Some(ObserveMetricCallback::U64(Box::new(move |instrument| {
                    instrument.observe(m.wal_deletes_total(), &attributes);
                })))
            }
            "replication_truncates_total" => {
                Some(ObserveMetricCallback::U64(Box::new(move |instrument| {
                    instrument.observe(m.wal_truncates_total(), &attributes);
                })))
            }
            "replication_bootstrap_rows_total" => {
                Some(ObserveMetricCallback::U64(Box::new(move |instrument| {
                    instrument.observe(m.bootstrap_rows_total(), &attributes);
                })))
            }
            "replication_bootstrap_rows_expected" => {
                Some(ObserveMetricCallback::U64(Box::new(move |instrument| {
                    // Observe only when an estimate exists, so an absent series (rather
                    // than `0`) means "unknown" and `0` is a known-empty source table.
                    if let Some(expected) = m.bootstrap_rows_expected() {
                        instrument.observe(expected, &attributes);
                    }
                })))
            }
            "replication_bootstrap_complete" => {
                Some(ObserveMetricCallback::U64(Box::new(move |instrument| {
                    instrument.observe(m.bootstrap_complete(), &attributes);
                })))
            }
            "replication_decode_errors_total" => {
                Some(ObserveMetricCallback::U64(Box::new(move |instrument| {
                    instrument.observe(m.wal_decode_errors_total(), &attributes);
                })))
            }
            "replication_schema_mismatch_errors_total" => {
                Some(ObserveMetricCallback::U64(Box::new(move |instrument| {
                    instrument.observe(m.schema_mismatch_errors_total(), &attributes);
                })))
            }
            "replication_recv_errors_total" => {
                Some(ObserveMetricCallback::U64(Box::new(move |instrument| {
                    instrument.observe(m.replication_recv_errors_total(), &attributes);
                })))
            }
            "replication_reconnects_total" => {
                Some(ObserveMetricCallback::U64(Box::new(move |instrument| {
                    instrument.observe(m.replication_reconnects_total(), &attributes);
                })))
            }
            "replication_disconnected_ms_total" => {
                Some(ObserveMetricCallback::U64(Box::new(move |instrument| {
                    instrument.observe(m.replication_disconnected_ms_total(), &attributes);
                })))
            }
            "replication_member_attached" => {
                Some(ObserveMetricCallback::U64(Box::new(move |instrument| {
                    // Observe only for shared-slot members (`Some`); a dedicated slot has
                    // no member-detach concept, so its series stays absent rather than a
                    // misleading constant `0`. Append the shared-slot label so the
                    // analysis can group datasets by slot + join authoritative backlog.
                    if let Some(v) = m.member_attached() {
                        let mut attrs = attributes.clone();
                        if let Some(slot) = m.slot_name() {
                            attrs.push(KeyValue::new("slot", slot));
                        }
                        instrument.observe(v, &attrs);
                    }
                })))
            }
            "replication_member_send_stalled_seconds_total" => {
                Some(ObserveMetricCallback::U64(Box::new(move |instrument| {
                    instrument.observe(m.member_send_stalled_seconds_total(), &attributes);
                })))
            }
            "replication_member_send_wait_micros_total" => {
                Some(ObserveMetricCallback::U64(Box::new(move |instrument| {
                    instrument.observe(m.member_send_wait_micros_total(), &attributes);
                })))
            }
            "replication_member_envelopes_delivered_total" => {
                Some(ObserveMetricCallback::U64(Box::new(move |instrument| {
                    instrument.observe(m.member_envelopes_delivered_total(), &attributes);
                })))
            }
            "replication_member_envelope_eager_merges_total" => {
                Some(ObserveMetricCallback::U64(Box::new(move |instrument| {
                    instrument.observe(m.member_envelope_eager_merges_total(), &attributes);
                })))
            }
            "replication_member_envelope_mailbox_merges_total" => {
                Some(ObserveMetricCallback::U64(Box::new(move |instrument| {
                    instrument.observe(m.member_envelope_mailbox_merges_total(), &attributes);
                })))
            }
            "replication_member_mailbox_coalesce_limited_total" => {
                Some(ObserveMetricCallback::U64(Box::new(move |instrument| {
                    instrument.observe(m.member_mailbox_coalesce_limited_total(), &attributes);
                })))
            }
            _ => None,
        }
    }
}

/// Whether the accelerator's state survives a process restart. Non-persistent
/// accelerators must re-snapshot on every start — WAL replay from the slot's
/// checkpoint can never reconstruct an accelerator that booted empty.
fn accelerator_is_ephemeral(
    acceleration: &runtime_component::dataset::acceleration::Acceleration,
) -> bool {
    use runtime_component::dataset::acceleration::{Engine, Mode};
    // Matched exhaustively (no `_` arm) so a newly added engine has to make an
    // explicit durability claim here: defaulting a non-persistent engine to
    // "persistent" silently skips its resume snapshot and leaves the accelerator
    // missing every row written before startup.
    match acceleration.engine.to_unpartitioned() {
        // Always in-memory. `to_unpartitioned` already folded `PartitionedArrow`
        // into `Arrow`, so it cannot reach the match; it is listed only to keep
        // the match exhaustive.
        Engine::Arrow | Engine::PartitionedArrow => true,
        // In-memory unless file-backed; `file_create` truncates on startup,
        // which is just as empty as memory from replication's point of view.
        //
        // Cayenne belongs here too: `mode: memory` is fully in-RAM (an in-memory
        // `memdb` metastore and no data directory at all — see the accelerator's
        // `memory_mode` branch), so nothing about it survives a restart. Only its
        // file modes (local disk or S3 Express One Zone, both of which require a
        // file mode) persist independently of this process.
        Engine::DuckDB | Engine::Sqlite | Engine::Turso | Engine::Cayenne => {
            matches!(acceleration.mode, Mode::Memory | Mode::FileCreate)
        }
        // External storage (another Postgres) persists independently of this
        // process.
        Engine::PostgreSQL => false,
    }
}

fn replication_params_from_connector_params(
    params: &Parameters,
    dataset_name: &str,
) -> std::result::Result<ReplicationParams, String> {
    // Same override rule / parser as `PostgresConnectionPool` (see
    // `crate::connection`): `pg_connection_string` wins over discrete
    // `pg_host`/`pg_user`/`pg_db`/…; discrete `pg_sslmode` / `pg_sslrootcert`
    // still override values embedded in the connection string.
    let identity = crate::connection::connection_identity_from_params(params)?;
    let sslmode = config::SslMode::from_str_strict(identity.sslmode.as_deref())
        .map_err(|reason| format!("parameter `{}` {reason}", params.user_param("sslmode")))?;
    let sslrootcert = identity
        .sslrootcert
        .as_deref()
        .map(config::ca_certificate_from_param);

    // An explicitly-named slot is shareable: every dataset on the same
    // connection naming the same slot is multiplexed onto one replication
    // connection (see `data_components::postgres_replication::shared`). The
    // default publication is then derived from the slot — not the dataset —
    // so all members land on the same publication.
    let explicit_slot = optional_string(params, "replication_slot");
    let shared = explicit_slot.is_some();
    let (slot_name, publication_name) = match explicit_slot {
        Some(slot) => {
            config::validate_replication_slot_name(&slot).map_err(|reason| {
                format!(
                    "parameter `{}` {reason}",
                    params.user_param("replication_slot")
                )
            })?;
            let publication = optional_string(params, "publication")
                .unwrap_or_else(|| config::publication_name_for_slot(&slot));
            (slot, publication)
        }
        None => (
            config::default_slot_name(dataset_name),
            optional_string(params, "publication")
                .unwrap_or_else(|| config::default_publication_name(dataset_name)),
        ),
    };
    let (initial_snapshot, snapshot_on_resume) = parse_initial_snapshot(params)?;
    let status_interval = optional_duration(
        params,
        "replication_status_interval",
        DEFAULT_STATUS_INTERVAL,
    )?;
    let bootstrap_batch_size = optional_usize_in_range(
        params,
        "replication_bootstrap_batch_size",
        DEFAULT_BOOTSTRAP_BATCH_SIZE,
        MAX_BOOTSTRAP_BATCH_SIZE,
    )?;
    let ready_lag = optional_duration(
        params,
        "replication_ready_lag",
        data_components::cdc::DEFAULT_READY_LAG,
    )?;
    // Only meaningful on the shared path, but parsed unconditionally so a
    // misconfigured value is rejected up front regardless of slot mode.
    let member_channel_capacity = optional_usize_in_range(
        params,
        "replication_member_channel_capacity",
        data_components::postgres_replication::shared::DEFAULT_MEMBER_CHANNEL_CAPACITY,
        MAX_MEMBER_CHANNEL_CAPACITY,
    )?;

    Ok(ReplicationParams {
        host: identity.host,
        port: identity.port,
        user: identity.user,
        password: SecretString::from(identity.password),
        database: identity.database,
        sslmode,
        sslrootcert,
        slot_name,
        publication_name,
        initial_snapshot,
        snapshot_on_resume,
        // Derived from the dataset's accelerator, which this function does not
        // see; `build_changes_stream` sets it right after.
        ephemeral_accelerator: false,
        acceleration: AccelerationContents::Unknown,
        status_interval,
        ready_lag,
        bootstrap_batch_size,
        shared,
        member_channel_capacity,
        // Binary pgoutput on every stream — faster decode, no source-side text
        // formatting. Not a user-facing parameter; the per-column text fallback
        // still handles types Postgres emits as text.
        pg_output_format: PgOutputFormat::Binary,
        unclaimed_reservation_grace:
            data_components::postgres_replication::shared::DEFAULT_UNCLAIMED_RESERVATION_GRACE,
        watermark_flush_interval:
            data_components::postgres_replication::shared::DEFAULT_WATERMARK_FLUSH_INTERVAL,
    })
}

fn optional_string(params: &Parameters, key: &str) -> Option<String> {
    match params.get(key).expose() {
        ExposedParamLookup::Present(v) => Some(v.to_string()),
        ExposedParamLookup::Absent(_) => None,
    }
}

fn optional_usize_in_range(
    params: &Parameters,
    key: &str,
    default: usize,
    max: usize,
) -> std::result::Result<usize, String> {
    let Some(raw) = optional_string(params, key) else {
        return Ok(default);
    };

    match raw.trim().parse::<usize>() {
        Ok(value) if (1..=max).contains(&value) => Ok(value),
        Ok(value) => {
            let user_param = params.user_param(key);
            Err(format!(
                "parameter `{user_param}` must be between 1 and {max}, got {value}"
            ))
        }
        Err(parse_error) => {
            let user_param = params.user_param(key);
            Err(format!(
                "parameter `{user_param}` must be a positive integer, got {raw:?}: {parse_error}"
            ))
        }
    }
}

/// Map the shared [`InitialSnapshotMode`] onto Postgres's two internal flags
/// `(initial_snapshot, snapshot_on_resume)`:
///
/// - `Auto` -> `(true, false)`: snapshot a freshly-created slot; the caller still
///   forces a resume snapshot for a non-persistent accelerator.
/// - `Always` -> `(true, true)`: snapshot on every start, including slot resume.
/// - `Disabled` -> `(false, false)`: never snapshot.
fn snapshot_flags(mode: InitialSnapshotMode) -> (bool, bool) {
    match mode {
        InitialSnapshotMode::Auto => (true, false),
        InitialSnapshotMode::Always => (true, true),
        InitialSnapshotMode::Disabled => (false, false),
    }
}

/// Resolve `pg_replication_initial_snapshot` into the two internal snapshot
/// flags. Accepts the shared canonical vocabulary (`auto|always|disabled`, via
/// [`InitialSnapshotMode::from_canonical`]) and, for backward compatibility, the
/// legacy booleans `true|false` (mapped to `auto|disabled`).
///
/// A typo is rejected rather than silently falling back: a lenient parse that
/// collapsed an unrecognized value to `disabled` would skip the bootstrap
/// snapshot, leaving the accelerator serving only post-subscription changes and
/// missing every pre-existing row with no error.
fn parse_initial_snapshot(params: &Parameters) -> std::result::Result<(bool, bool), String> {
    let Some(raw) = optional_string(params, "replication_initial_snapshot") else {
        return Ok(snapshot_flags(InitialSnapshotMode::Auto));
    };
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Ok(snapshot_flags(InitialSnapshotMode::Auto));
    }
    if let Some(mode) = InitialSnapshotMode::from_canonical(trimmed) {
        return Ok(snapshot_flags(mode));
    }
    // Deprecated boolean spellings map to auto / disabled.
    match trimmed.to_ascii_lowercase().as_str() {
        legacy @ ("true" | "1" | "yes" | "y") => {
            tracing::warn!(
                "parameter `{}` uses the deprecated boolean value {legacy:?}; use 'auto' instead (or 'always'/'disabled')",
                params.user_param("replication_initial_snapshot")
            );
            Ok(snapshot_flags(InitialSnapshotMode::Auto))
        }
        legacy @ ("false" | "0" | "no" | "n") => {
            tracing::warn!(
                "parameter `{}` uses the deprecated boolean value {legacy:?}; use 'disabled' instead",
                params.user_param("replication_initial_snapshot")
            );
            Ok(snapshot_flags(InitialSnapshotMode::Disabled))
        }
        other => {
            let user_param = params.user_param("replication_initial_snapshot");
            Err(format!(
                "parameter `{user_param}` must be 'auto', 'always', or 'disabled', got {other:?}"
            ))
        }
    }
}

/// Parses an optional value via `FromStr`. An absent or empty value uses
/// `default`; a parse failure is reported with the user-facing parameter name
/// and `expected` description rather than silently substituting the default.
#[cfg(test)]
fn optional_parse<T>(
    params: &Parameters,
    key: &str,
    default: T,
    expected: &str,
) -> std::result::Result<T, String>
where
    T: std::str::FromStr,
{
    let Some(raw) = optional_string(params, key) else {
        return Ok(default);
    };
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Ok(default);
    }
    trimmed.parse::<T>().map_err(|_| {
        let user_param = params.user_param(key);
        format!("parameter `{user_param}` must be {expected}, got {raw:?}")
    })
}

/// Parses an optional duration parameter strictly. An absent or empty value
/// uses `default`; an unparseable value is rejected rather than silently
/// substituting the default.
fn optional_duration(
    params: &Parameters,
    key: &str,
    default: Duration,
) -> std::result::Result<Duration, String> {
    let Some(raw) = optional_string(params, key) else {
        return Ok(default);
    };
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Ok(default);
    }
    fundu::parse_duration(trimmed).map_err(|parse_error| {
        let user_param = params.user_param(key);
        format!("parameter `{user_param}` must be a duration, got {raw:?}: {parse_error}")
    })
}

/// Splits `dataset.from` like `"postgres:public.users"` into (schema, table).
/// Falls back to ("public", <rest>) if unqualified.
fn split_schema_table(from: &str) -> (String, String) {
    let path = from.strip_prefix("postgres:").unwrap_or(from);
    // Use TableReference to respect quoting.
    let r = TableReference::from(path);
    match (r.schema(), r.table()) {
        (Some(schema), table) => (schema.to_string(), table.to_string()),
        (None, table) => ("public".to_string(), table.to_string()),
    }
}

fn extract_primary_keys(provider: &Arc<dyn datafusion::datasource::TableProvider>) -> Vec<String> {
    use datafusion::common::Constraint;
    let Some(constraints) = provider.constraints() else {
        return Vec::new();
    };
    let schema = provider.schema();
    for c in constraints.iter() {
        if let Constraint::PrimaryKey(indices) = c {
            return indices
                .iter()
                .filter_map(|i| schema.fields().get(*i).map(|f| f.name().clone()))
                .collect();
        }
    }
    Vec::new()
}

#[cfg(test)]
mod tests {
    use super::*;
    use data_components::postgres_replication::CaCertificate;

    fn params_with_bootstrap_batch_size(value: &str) -> Parameters {
        Parameters::new(
            vec![(
                "replication_bootstrap_batch_size".to_string(),
                SecretString::from(value),
            )],
            "pg",
            crate::PARAMETERS,
        )
    }

    fn params_with(key: &str, value: &str) -> Parameters {
        Parameters::new(
            vec![(key.to_string(), SecretString::from(value))],
            "pg",
            crate::PARAMETERS,
        )
    }

    fn empty_params() -> Parameters {
        Parameters::new(vec![], "pg", crate::PARAMETERS)
    }

    /// Self-signed CA (`CN=Spice Replication Test CA`, `CA:TRUE`), expiring in 2126.
    const TEST_CA_PEM: &str = "-----BEGIN CERTIFICATE-----
MIIC4DCCAcigAwIBAgIJAODHR+uzOPBvMA0GCSqGSIb3DQEBCwUAMCQxIjAgBgNV
BAMMGVNwaWNlIFJlcGxpY2F0aW9uIFRlc3QgQ0EwIBcNMjYwNzI4MDU0MDQ0WhgP
MjEyNjA3MDQwNTQwNDRaMCQxIjAgBgNVBAMMGVNwaWNlIFJlcGxpY2F0aW9uIFRl
c3QgQ0EwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQCzoou00DrTAevF
RZ6+PFmSBUhzZXsABQFztlPigZzJ1m8hnja66hnkWKyIid9DcitnjkWgtQZCVxm6
s05tM6QAy5lI2wlfWD7hQi+yIWKv2dcVuD/J4hWPjmG5a5VtRAInV0yBymkCRI6Z
68JYfvKh+Rku1y6H3dUfNm8dxCbo589L1U8ucJqlQv9Iy/X7Lze+pj2JFU/L1g3t
k/5ziVgJjdh3VetrHkU1YOiHRPFsqXOxXc2lpzUjd23QR3FfkZkVgLUfEvPWHRSf
xipaPFhllw9WUWEl6bVqAGO0btPO1OKKqBlIcizf2YO2+lFs/o0e7bApGzI3l5HP
VZr/e6ZLAgMBAAGjEzARMA8GA1UdEwEB/wQFMAMBAf8wDQYJKoZIhvcNAQELBQAD
ggEBACC1XMNpbA+172MQks9R7cqRY5I0HObJRX3dpIsOqrm3EUcHMt9kx7QrO1Af
gzAWC0ZNHppeU/cuq9ZKZQiFrSmr5fKtXzsxkvgLYRCFO+ZCKZl9k3z9j0AQbTPR
klJa4bo2SS6WbmoATimD6e0moT++neRIDx7MlijtWB8grfhuH7yFN9xoTRDgdYBU
KLeFNAIi+S5cVzUwjMiOQnmljphKSRoQnihpA/c6WAVAN3VqMdoPpfmR2pTi7rio
38busw0nt/y+JCVWzNDr/i5f3mvNi5SaHZ5PTOVnocyMUw+ysx5eQOrJwrirW9XD
TXTE85+Or9IUwDI9543jsyCvuQ8=
-----END CERTIFICATE-----
";

    /// A complete connection, so parsing reaches `sslrootcert` instead of
    /// aborting on a missing required parameter.
    fn params_with_sslrootcert(value: &str) -> Parameters {
        Parameters::new(
            vec![
                ("host".to_string(), SecretString::from("pg.internal")),
                ("user".to_string(), SecretString::from("spice")),
                ("db".to_string(), SecretString::from("myapp")),
                ("sslmode".to_string(), SecretString::from("verify-full")),
                ("sslrootcert".to_string(), SecretString::from(value)),
            ],
            "pg",
            crate::PARAMETERS,
        )
    }

    /// `pg_sslrootcert` is documented as accepting a path *or* inline PEM
    /// content, and a CA injected as a secret arrives as content. Both spellings
    /// must reach the replication stream as a usable trust anchor.
    #[test]
    fn inline_pem_sslrootcert_reaches_replication_params_as_content() {
        let repl =
            replication_params_from_connector_params(&params_with_sslrootcert(TEST_CA_PEM), "hits")
                .expect("inline PEM sslrootcert should parse");

        assert_eq!(repl.sslmode, config::SslMode::VerifyFull);
        assert_eq!(
            repl.sslrootcert,
            Some(CaCertificate::Pem(TEST_CA_PEM.as_bytes().to_vec())),
            "inline PEM must not be reinterpreted as a filesystem path"
        );
    }

    #[test]
    fn inline_pem_sslrootcert_survives_a_single_line_secret() {
        let repl = replication_params_from_connector_params(
            &params_with_sslrootcert(&TEST_CA_PEM.replace('\n', "\\n")),
            "hits",
        )
        .expect("single-line inline PEM sslrootcert should parse");

        assert_eq!(
            repl.sslrootcert,
            Some(CaCertificate::Pem(TEST_CA_PEM.as_bytes().to_vec()))
        );
    }

    #[test]
    fn path_sslrootcert_reaches_replication_params_as_a_path() {
        let repl = replication_params_from_connector_params(
            &params_with_sslrootcert("/etc/ssl/pg-ca.pem"),
            "hits",
        )
        .expect("sslrootcert path should parse");

        assert_eq!(
            repl.sslrootcert,
            Some(CaCertificate::Path("/etc/ssl/pg-ca.pem".into()))
        );
    }

    /// `accelerator_is_ephemeral` decides whether a slot resume re-snapshots.
    /// Getting it wrong in the "persistent" direction is silent data loss: the
    /// accelerator boots empty, the slot resumes from `confirmed_flush_lsn`, and
    /// the dataset then serves only rows touched after startup. Each engine's
    /// durability is asserted for every mode so a wrong answer fails here rather
    /// than in production.
    #[test]
    fn accelerator_ephemerality_is_classified_per_engine_and_mode() {
        use runtime_component::dataset::acceleration::{Acceleration, Engine, Mode};

        let ephemeral = |engine: Engine, mode: Mode| {
            accelerator_is_ephemeral(&Acceleration {
                engine,
                mode,
                ..Acceleration::default()
            })
        };

        // Cayenne `mode: memory` is fully in-RAM (in-memory `memdb` metastore, no
        // data directory), so it must re-snapshot on every start. This is also
        // the mode catalog-level CDC acceleration runs in, and `mode: memory` is
        // the default for an acceleration block that doesn't name one.
        assert!(
            ephemeral(Engine::Cayenne, Mode::Memory),
            "in-memory Cayenne does not survive a restart"
        );
        assert!(
            ephemeral(Engine::Cayenne, Mode::FileCreate),
            "`file_create` truncates on startup, which is as empty as memory"
        );
        // File-backed Cayenne (local disk, or S3 Express One Zone — which also
        // requires a file mode) persists, so a plain slot resume is correct.
        assert!(!ephemeral(Engine::Cayenne, Mode::File));
        assert!(!ephemeral(Engine::Cayenne, Mode::FileUpdate));

        for engine in [Engine::DuckDB, Engine::Sqlite, Engine::Turso] {
            assert!(ephemeral(engine, Mode::Memory), "{engine} memory");
            assert!(ephemeral(engine, Mode::FileCreate), "{engine} file_create");
            assert!(!ephemeral(engine, Mode::File), "{engine} file");
            assert!(!ephemeral(engine, Mode::FileUpdate), "{engine} file_update");
        }

        // Arrow is in-memory whatever `mode` says; Postgres is external storage.
        for mode in [Mode::Memory, Mode::File, Mode::FileCreate, Mode::FileUpdate] {
            assert!(ephemeral(Engine::Arrow, mode), "arrow {mode}");
            assert!(ephemeral(Engine::PartitionedArrow, mode), "arrow {mode}");
            assert!(!ephemeral(Engine::PostgreSQL, mode), "postgres {mode}");
        }
    }

    /// `snapshot_on_resume` is only forced when snapshots are enabled at all;
    /// `pg_replication_initial_snapshot: disabled` is an explicit opt-out that a
    /// non-persistent accelerator must not override.
    #[test]
    fn disabled_initial_snapshot_is_not_overridden_by_ephemerality() {
        let (initial_snapshot, snapshot_on_resume) =
            parse_initial_snapshot(&params_with("replication_initial_snapshot", "disabled"))
                .expect("`disabled` is a canonical value");
        assert!(!initial_snapshot);
        assert!(!snapshot_on_resume);
    }

    // Regression for #11994: CDC must honor `pg_connection_string` the same way
    // as the federated read pool (libpq key=value; default sslmode verify-full).
    #[test]
    fn connection_string_flows_into_replication_params() {
        let params = Parameters::new(
            vec![
                (
                    "connection_string".to_string(),
                    SecretString::from(
                        "host=db.internal port=5433 dbname=csdb user=csuser password=secret",
                    ),
                ),
                ("host".to_string(), SecretString::from("ignored")),
            ],
            "pg",
            crate::PARAMETERS,
        );
        let repl = replication_params_from_connector_params(&params, "hits")
            .expect("valid connection_string should parse");
        assert_eq!(repl.host, "db.internal");
        assert_eq!(repl.port, 5433);
        assert_eq!(repl.user, "csuser");
        assert_eq!(repl.database, "csdb");
        assert_eq!(repl.sslmode, config::SslMode::VerifyFull);
        assert_eq!(
            secrecy::ExposeSecret::expose_secret(&repl.password),
            "secret"
        );
    }

    #[test]
    fn uri_connection_string_flows_into_replication_params() {
        let params = Parameters::new(
            vec![
                (
                    "connection_string".to_string(),
                    SecretString::from("postgresql://csuser:secret@db.internal:5433/csdb"),
                ),
                ("host".to_string(), SecretString::from("ignored")),
            ],
            "pg",
            crate::PARAMETERS,
        );
        let repl = replication_params_from_connector_params(&params, "hits")
            .expect("URI connection_string should parse for CDC");
        assert_eq!(repl.host, "db.internal");
        assert_eq!(repl.port, 5433);
        assert_eq!(repl.user, "csuser");
        assert_eq!(repl.database, "csdb");
        assert_eq!(repl.sslmode, config::SslMode::VerifyFull);
        assert_eq!(
            secrecy::ExposeSecret::expose_secret(&repl.password),
            "secret"
        );
    }

    /// Regression test for #12213. `pg_replication_temporary_slot` is retained
    /// only as a deprecated spec: it must stay declared, so an operator who
    /// still sets it is told it is ignored, and it must stay deprecated, so it
    /// is struck through in the Spicepod schema and cannot be mistaken for a
    /// working knob. A temporary slot is owned by the session that creates it,
    /// and creation happens on the short-lived setup connection, so the slot
    /// was always gone before `START_REPLICATION` ran.
    #[test]
    fn temporary_slot_parameter_is_declared_and_deprecated() {
        let spec = crate::PARAMETERS
            .iter()
            .find(|p| p.name == "replication_temporary_slot")
            .expect("the deprecated spec must remain declared so setting it is not silent");
        assert!(
            spec.deprecation_message.is_some(),
            "pg_replication_temporary_slot cannot be honoured and must stay deprecated"
        );
        assert!(
            spec.default.is_none(),
            "an ignored parameter must not advertise a default"
        );
    }

    /// Setting the deprecated parameter must not fail the dataset: it is warned
    /// about and ignored, so a pod carrying it loads and streams with a durable
    /// slot instead of breaking at `START_REPLICATION`.
    #[test]
    fn deprecated_temporary_slot_is_accepted_and_ignored() {
        for value in ["true", "false", "ture"] {
            let params = Parameters::new(
                vec![
                    ("host".to_string(), SecretString::from("pg.internal")),
                    ("user".to_string(), SecretString::from("spice")),
                    ("db".to_string(), SecretString::from("myapp")),
                    (
                        "replication_temporary_slot".to_string(),
                        SecretString::from(value),
                    ),
                ],
                "pg",
                crate::PARAMETERS,
            );
            if let Err(e) = replication_params_from_connector_params(&params, "hits") {
                panic!("pg_replication_temporary_slot={value} must be ignored, not rejected: {e}");
            }
        }
    }

    #[test]
    fn parse_initial_snapshot_maps_enum_and_legacy_booleans() {
        // Canonical enum values -> (initial_snapshot, snapshot_on_resume).
        for (raw, expected) in [
            ("auto", (true, false)),
            ("AUTO", (true, false)),
            ("always", (true, true)),
            ("disabled", (false, false)),
            // Deprecated boolean spellings map to auto / disabled.
            ("true", (true, false)),
            ("1", (true, false)),
            ("false", (false, false)),
            ("no", (false, false)),
        ] {
            assert_eq!(
                parse_initial_snapshot(&params_with("replication_initial_snapshot", raw)),
                Ok(expected),
                "raw: {raw}"
            );
        }
        // Absent -> auto default.
        assert_eq!(parse_initial_snapshot(&empty_params()), Ok((true, false)));
    }

    #[test]
    fn parse_initial_snapshot_rejects_unrecognized_value() {
        let err = parse_initial_snapshot(&params_with("replication_initial_snapshot", "sometimes"))
            .expect_err("typo must error");
        assert_eq!(
            err,
            "parameter `pg_replication_initial_snapshot` must be 'auto', 'always', or 'disabled', got \"sometimes\"".to_string()
        );
    }

    #[test]
    fn optional_parse_port_rejects_non_numeric() {
        let result = optional_parse::<u16>(
            &params_with("port", "not-a-port"),
            "port",
            5432,
            "a port number (0-65535)",
        );
        assert_eq!(
            result,
            Err(
                "parameter `pg_port` must be a port number (0-65535), got \"not-a-port\""
                    .to_string()
            )
        );
        // Valid + absent paths.
        assert_eq!(
            optional_parse::<u16>(&params_with("port", "6543"), "port", 5432, "x"),
            Ok(6543)
        );
        assert_eq!(
            optional_parse::<u16>(&empty_params(), "port", 5432, "x"),
            Ok(5432)
        );
    }

    #[test]
    fn optional_duration_rejects_unparseable_value() {
        let result = optional_duration(
            &params_with("replication_status_interval", "soon"),
            "replication_status_interval",
            DEFAULT_STATUS_INTERVAL,
        );
        let err = result.expect_err("invalid duration should error");
        assert!(
            err.starts_with(
                "parameter `pg_replication_status_interval` must be a duration, got \"soon\""
            ),
            "unexpected error: {err}"
        );
        assert_eq!(
            optional_duration(
                &empty_params(),
                "replication_status_interval",
                DEFAULT_STATUS_INTERVAL
            ),
            Ok(DEFAULT_STATUS_INTERVAL)
        );
    }

    #[test]
    fn sslmode_strict_rejects_unknown_mode() {
        // A typo must not silently downgrade to `prefer` (TLS/MITM downgrade).
        match config::SslMode::from_str_strict(Some("verify-ful")) {
            Err(_) => {}
            Ok(mode) => panic!("typo `verify-ful` must be rejected, got {mode:?}"),
        }
        assert_eq!(
            config::SslMode::from_str_strict(Some("verify-full")),
            Ok(config::SslMode::VerifyFull)
        );
        assert_eq!(
            config::SslMode::from_str_strict(None),
            Ok(config::SslMode::Prefer)
        );
    }

    #[test]
    fn optional_usize_in_range_uses_user_facing_name_for_out_of_range_error() {
        let params = params_with_bootstrap_batch_size("0");

        let result = optional_usize_in_range(
            &params,
            "replication_bootstrap_batch_size",
            DEFAULT_BOOTSTRAP_BATCH_SIZE,
            MAX_BOOTSTRAP_BATCH_SIZE,
        );

        assert_eq!(
            result,
            Err("parameter `pg_replication_bootstrap_batch_size` must be between 1 and 1048576, got 0".to_string())
        );
    }

    #[test]
    fn optional_usize_in_range_uses_user_facing_name_for_parse_error() {
        let params = params_with_bootstrap_batch_size("many");

        let result = optional_usize_in_range(
            &params,
            "replication_bootstrap_batch_size",
            DEFAULT_BOOTSTRAP_BATCH_SIZE,
            MAX_BOOTSTRAP_BATCH_SIZE,
        );

        assert!(
            result
                .expect_err("invalid bootstrap batch size should return an error")
                .starts_with(
                    "parameter `pg_replication_bootstrap_batch_size` must be a positive integer, got \"many\""
                )
        );
    }

    fn replication_connection_params(slot: Option<&str>) -> Parameters {
        let mut entries = vec![
            ("host".to_string(), SecretString::from("localhost")),
            ("user".to_string(), SecretString::from("spice")),
            ("db".to_string(), SecretString::from("spice")),
        ];
        if let Some(slot) = slot {
            entries.push(("replication_slot".to_string(), SecretString::from(slot)));
        }
        Parameters::new(entries, "pg", crate::PARAMETERS)
    }

    #[test]
    fn replication_params_rejects_invalid_slot_name() {
        let params =
            replication_connection_params(Some("scp-onboarding-realtime-analytics-prod-us-east-1"));
        let err = replication_params_from_connector_params(&params, "hits")
            .expect_err("hyphenated slot must be rejected");
        assert!(
            err.starts_with("parameter `pg_replication_slot` must contain only lowercase"),
            "unexpected error: {err}"
        );
        assert!(
            err.contains("invalid character '-'"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn replication_params_accepts_valid_explicit_slot_name() {
        let params = replication_connection_params(Some("spice_hits_cdc"));
        let parsed = replication_params_from_connector_params(&params, "hits")
            .expect("valid slot must parse");
        assert_eq!(parsed.slot_name, "spice_hits_cdc");
        assert!(parsed.shared);
    }
}
