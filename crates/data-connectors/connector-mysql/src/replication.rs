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

//! Glue between Spice's connector params and the `mysql_replication` module.
//!
//! Responsibilities:
//!   - Parse connection & replication params out of `runtime::parameters::Parameters`.
//!   - Validate the dataset's PK/upsert configuration up front.
//!   - Provide the [`PositionStore`] implementation over the accelerator's
//!     `spice_sys_mysql_binlog` sidecar table.
//!   - Look up the source table schema (via the federated table) and hand
//!     everything off to `data_components::mysql_replication::start_replication_stream`.

use std::sync::Arc;
use std::time::Duration;

use async_stream::try_stream;
use async_trait::async_trait;
use data_components::cdc::{ChangesStream, StreamError};
use data_components::mysql_replication::{
    BinlogPosition, InvalidPositionBehavior, NoopPositionStore, PersistedPosition, PositionStore,
    ReplicationMetrics, ReplicationMetricsCollector, ReplicationParams, ReplicationStreamInput,
    SnapshotMode, StoreError, derive_server_id, process_nonce, start_replication_stream,
};
use datafusion::sql::TableReference;
use futures::StreamExt;
use mysql_async::{Opts, OptsBuilder, SslOpts};
use opentelemetry::KeyValue;
use runtime::component::dataset::Dataset;
use runtime::component::metrics::{MetricSpec, MetricType, ObserveMetricCallback};
use runtime::dataaccelerator::spice_sys::{
    OpenOption,
    mysql_binlog::{MySqlBinlogCheckpoint, MySqlBinlogSys},
};
use runtime::federated_table::FederatedTable;
use runtime::parameters::Parameters;

const DEFAULT_CHECKPOINT_INTERVAL: Duration = Duration::from_secs(10);
const DEFAULT_BOOTSTRAP_BATCH_SIZE: usize = 8192;
const MAX_BOOTSTRAP_BATCH_SIZE: usize = 1_048_576;

pub fn build_changes_stream(
    params: &Parameters,
    dataset: &Dataset,
    federated_table: Arc<FederatedTable>,
    metrics: Arc<ReplicationMetricsCollector>,
) -> ChangesStream {
    let dataset_name = dataset.name.to_string();

    let (database, table) = match split_database_table(&dataset.from, params) {
        Ok(parts) => parts,
        Err(e) => {
            let msg = format!("mysql replication: {e}");
            return Box::pin(futures::stream::once(async move {
                Err(StreamError::External(msg))
            }));
        }
    };

    let params_for_stream = match replication_params_from_connector_params(params, &dataset_name) {
        Ok(p) => p,
        Err(e) => {
            let msg = format!("mysql replication: {e}");
            return Box::pin(futures::stream::once(async move {
                Err(StreamError::External(msg))
            }));
        }
    };

    // TIMESTAMP columns scan through the read pool in the session time zone
    // (`mysql_time_zone`, default UTC) while binlog values are always UTC.
    // A non-UTC scan zone would make live changes disagree with federated
    // reads of the same rows.
    if let Some(tz) = optional_string(params, "time_zone")
        && !matches!(tz.trim(), "+00:00" | "+0:00" | "UTC" | "utc")
    {
        tracing::warn!(
            dataset = %dataset_name,
            time_zone = %tz,
            "`refresh_mode: changes` replicates TIMESTAMP columns in UTC regardless of \
             `mysql_time_zone`; federated reads using this non-UTC zone will disagree \
             with accelerated values. Set `mysql_time_zone: '+00:00'` for consistency."
        );
    }

    // Note: unlike Postgres (whose cursor lives server-side in the slot and
    // can outlive an empty accelerator), no ephemeral-accelerator special
    // case is needed here — the binlog position persists inside the
    // accelerator's own sidecar, so a non-persistent accelerator can never
    // present a stale resumable position.

    // Prefer the dataset's explicitly-declared acceleration `primary_key` —
    // that's what the accelerator write path uses for upsert/delete. Fall
    // back to the source TableProvider's constraints only if acceleration
    // didn't declare one.
    let declared_pks: Vec<String> = dataset
        .acceleration
        .as_ref()
        .and_then(|a| a.primary_key.as_ref())
        .map(|pk| pk.iter().map(ToString::to_string).collect())
        .unwrap_or_default();

    // UPDATE events rely on `on_conflict: upsert` to mutate the existing row
    // in place, and the conflict target MUST match the dataset's primary key
    // — otherwise the accelerator's write path falls through to append and
    // silently inserts duplicate rows on every UPDATE. The Arrow engine
    // genuinely can't support upsert and is documented as append-only for
    // UPDATEs — skip the check there. (Same contract as the Postgres
    // replication connector.)
    let engine = dataset
        .acceleration
        .as_ref()
        .map(|a| a.engine.to_unpartitioned())
        .unwrap_or_default();
    let engine_supports_upsert = !matches!(
        engine,
        runtime::component::dataset::acceleration::Engine::Arrow
            | runtime::component::dataset::acceleration::Engine::PartitionedArrow
    );
    let has_upsert_on_pk = dataset.acceleration.as_ref().is_some_and(|a| {
        a.primary_key.as_ref().is_some_and(|pk| {
            matches!(
                a.on_conflict.get(pk),
                Some(runtime::component::dataset::acceleration::OnConflictBehavior::Upsert(_))
            )
        })
    });

    let dataset = dataset.clone();

    Box::pin(try_stream! {
        let table_provider = federated_table.table_provider().await;
        let schema = table_provider.schema();

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
                "mysql replication for dataset `{dataset_name}`: no primary key available. \
                 Set `acceleration.primary_key` on the dataset (and a matching \
                 `acceleration.on_conflict` entry) — `refresh_mode: changes` cannot route \
                 UPDATE/DELETE events without one."
            )))?;
        }

        if engine_supports_upsert && !has_upsert_on_pk {
            // Composite keys hint the full parenthesized column list — a
            // single-column suggestion would mis-route UPDATE/DELETE events.
            let pk_hint = match primary_keys.as_slice() {
                [] => "<pk>".to_string(),
                [single] => single.clone(),
                composite => format!("({})", composite.join(", ")),
            };
            let msg = if declared_pks.is_empty() {
                format!(
                    "mysql replication for dataset `{dataset_name}`: the source table's \
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
                    "mysql replication for dataset `{dataset_name}`: `refresh_mode: changes` \
                     requires an `acceleration.on_conflict` entry keyed on the dataset's \
                     `primary_key` with an `upsert` (or `upsert_dedup*`) behavior so UPDATE \
                     events apply as upserts instead of duplicate inserts. Add: \
                     `on_conflict: {{ {pk_hint}: upsert }}` on the `{engine}` engine. \
                     (The `arrow` engine is exempt — documented append-only semantics.)"
                )
            };
            Err(StreamError::External(msg))?;
        }

        // File-accelerated datasets persist their binlog position in the
        // accelerator sidecar; everything else re-bootstraps on each start.
        let position_store: Arc<dyn PositionStore> = if dataset.is_file_accelerated() {
            match MySqlBinlogSys::try_new(&dataset, OpenOption::CreateIfNotExists).await {
                Ok(sys) => Arc::new(SidecarPositionStore { sys }),
                Err(e) => {
                    tracing::error!(
                        dataset = %dataset_name,
                        error = %e,
                        "failed to initialize the binlog-position sidecar; the position will \
                         not be persisted and the stream will re-bootstrap on every restart"
                    );
                    Arc::new(NoopPositionStore)
                }
            }
        } else {
            tracing::info!(
                dataset = %dataset_name,
                "dataset is not file-accelerated; the binlog position will not be persisted \
                 across restarts and the stream will re-bootstrap on every start"
            );
            Arc::new(NoopPositionStore)
        };

        let schema_json = match MySqlBinlogSys::serialize_schema(&schema) {
            Ok(json) => Some(json),
            Err(e) => {
                tracing::warn!(
                    dataset = %dataset_name,
                    error = %e,
                    "failed to serialize the dataset schema for the binlog-position sidecar; \
                     schema drift detection will be disabled for this run"
                );
                None
            }
        };

        let input = ReplicationStreamInput {
            dataset_name: dataset_name.clone(),
            params: params_for_stream,
            schema,
            primary_keys,
            database,
            table,
            position_store,
            schema_json,
            metrics,
        };

        let mut inner = start_replication_stream(input);
        while let Some(item) = inner.next().await {
            yield item?;
        }
    })
}

/// [`PositionStore`] over the accelerator's `spice_sys_mysql_binlog` sidecar.
struct SidecarPositionStore {
    sys: MySqlBinlogSys,
}

#[async_trait]
impl PositionStore for SidecarPositionStore {
    async fn load(&self) -> Result<Option<PersistedPosition>, StoreError> {
        // `MySqlBinlogSys::get` swallows read errors into `None`, matching the
        // MongoDB sidecar: an unreadable checkpoint re-bootstraps rather than
        // wedging the dataset.
        Ok(self.sys.get().await.map(|cp| PersistedPosition {
            position: BinlogPosition::new(cp.binlog_file, cp.binlog_pos),
            schema_json: cp.schema_json,
        }))
    }

    async fn save(&self, position: &PersistedPosition) -> Result<(), StoreError> {
        self.sys
            .upsert(&MySqlBinlogCheckpoint {
                binlog_file: position.position.file.clone(),
                binlog_pos: position.position.pos,
                schema_json: position.schema_json.clone(),
                updated_at: None,
            })
            .await
            .map_err(|e| Box::new(e) as StoreError)
    }

    async fn clear(&self) -> Result<(), StoreError> {
        self.sys
            .delete()
            .await
            .map_err(|e| Box::new(e) as StoreError)
    }
}

// ---------------------------------------------------------------------------
// Replication metric specs + observation, merged into the connector's
// `MetricsProvider` (see `lib.rs`) under `dataset_mysql_*`.
// ---------------------------------------------------------------------------

pub(crate) const REPLICATION_METRICS: &[MetricSpec] = &[
    MetricSpec::new("replication_lag_ms", MetricType::ObservableGaugeU64)
        .description(
            "Milliseconds between now() and the MySQL commit timestamp of the most recent \
             transaction replicated into the accelerator. Primary freshness metric for CDC; \
             binlog timestamps have 1-second granularity.",
        )
        .unit("ms")
        .auto_register(),
    MetricSpec::new("replication_lag_bytes", MetricType::ObservableGaugeU64)
        .description(
            "Bytes of binlog between the source's head and Spice's resume position. Reported \
             only while both are within the same binlog file; absent otherwise (more than a \
             file behind, or the head has not been polled yet).",
        )
        .unit("By")
        .auto_register(),
    MetricSpec::new(
        "replication_source_head_pos",
        MetricType::ObservableGaugeU64,
    )
    .description("Byte offset of the source server's binlog head, from the periodic status poll."),
    MetricSpec::new(
        "replication_source_head_file",
        MetricType::ObservableGaugeU64,
    )
    .description("Numeric suffix of the source server's head binlog file (binlog.000042 → 42)."),
    MetricSpec::new(
        "replication_committed_binlog_pos",
        MetricType::ObservableGaugeU64,
    )
    .description(
        "Byte offset within the current binlog file of the most recent position Spice has \
         durably checkpointed.",
    ),
    MetricSpec::new(
        "replication_committed_binlog_file",
        MetricType::ObservableGaugeU64,
    )
    .description(
        "Numeric suffix of the binlog file of the most recent checkpointed position \
         (binlog.000042 → 42).",
    ),
    MetricSpec::new(
        "replication_transactions_total",
        MetricType::ObservableCounterU64,
    )
    .description("Total number of source transactions observed on the binlog stream.")
    .auto_register(),
    MetricSpec::new(
        "replication_inserts_total",
        MetricType::ObservableCounterU64,
    )
    .description("Total INSERT row events received from the binlog.")
    .auto_register(),
    MetricSpec::new(
        "replication_updates_total",
        MetricType::ObservableCounterU64,
    )
    .description("Total UPDATE row events received from the binlog.")
    .auto_register(),
    MetricSpec::new(
        "replication_deletes_total",
        MetricType::ObservableCounterU64,
    )
    .description("Total DELETE row events received from the binlog.")
    .auto_register(),
    MetricSpec::new(
        "replication_truncates_total",
        MetricType::ObservableCounterU64,
    )
    .description("Total TRUNCATE statements received from the binlog and applied."),
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
         inference. Not reported when no estimate is available.",
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
    .description("Total binlog-decoding errors encountered while parsing row events.")
    .auto_register(),
    MetricSpec::new(
        "replication_schema_mismatch_errors_total",
        MetricType::ObservableCounterU64,
    )
    .description(
        "Total errors where the source table no longer matches the declared accelerator \
         schema (mid-stream DDL).",
    )
    .auto_register(),
    MetricSpec::new(
        "replication_recv_errors_total",
        MetricType::ObservableCounterU64,
    )
    .description(
        "Total transport-level errors while receiving from the binlog connection (TCP \
         drops, server restarts, etc).",
    )
    .auto_register(),
    MetricSpec::new(
        "replication_reconnects_total",
        MetricType::ObservableCounterU64,
    )
    .description(
        "Number of times the binlog stream reconnected after a transient failure. A \
         non-zero value with no user-visible error just means the connection wobbled and \
         we recovered.",
    )
    .auto_register(),
    MetricSpec::new(
        "replication_checkpoint_persists_total",
        MetricType::ObservableCounterU64,
    )
    .description("Number of binlog-position checkpoints persisted to the accelerator sidecar."),
    MetricSpec::new(
        "replication_checkpoint_persist_errors_total",
        MetricType::ObservableCounterU64,
    )
    .description(
        "Number of failed binlog-position checkpoint writes (retried on the next interval).",
    )
    .auto_register(),
];

/// Observation callback for one of the [`REPLICATION_METRICS`], or `None`
/// when `name` isn't a replication metric.
pub(crate) fn observe_replication_metric(
    metrics: &ReplicationMetrics,
    name: &str,
    attributes: Vec<KeyValue>,
) -> Option<ObserveMetricCallback> {
    let m = metrics.clone();
    let callback: ObserveMetricCallback = match name {
        "replication_lag_ms" => ObserveMetricCallback::U64(Box::new(move |instrument| {
            if let Some(v) = m.replication_lag_ms() {
                instrument.observe(v, &attributes);
            }
        })),
        "replication_lag_bytes" => ObserveMetricCallback::U64(Box::new(move |instrument| {
            // Observe only when computable, so an absent series (rather than
            // `0`) means "unknown".
            if let Some(lag) = m.replication_lag_bytes() {
                instrument.observe(lag, &attributes);
            }
        })),
        "replication_source_head_pos" => ObserveMetricCallback::U64(Box::new(move |instrument| {
            instrument.observe(m.source_head_pos(), &attributes);
        })),
        "replication_source_head_file" => ObserveMetricCallback::U64(Box::new(move |instrument| {
            instrument.observe(m.source_head_file_ordinal(), &attributes);
        })),
        "replication_committed_binlog_pos" => {
            ObserveMetricCallback::U64(Box::new(move |instrument| {
                instrument.observe(m.committed_binlog_pos(), &attributes);
            }))
        }
        "replication_committed_binlog_file" => {
            ObserveMetricCallback::U64(Box::new(move |instrument| {
                instrument.observe(m.committed_binlog_file_ordinal(), &attributes);
            }))
        }
        "replication_transactions_total" => {
            ObserveMetricCallback::U64(Box::new(move |instrument| {
                instrument.observe(m.transactions_total(), &attributes);
            }))
        }
        "replication_inserts_total" => ObserveMetricCallback::U64(Box::new(move |instrument| {
            instrument.observe(m.inserts_total(), &attributes);
        })),
        "replication_updates_total" => ObserveMetricCallback::U64(Box::new(move |instrument| {
            instrument.observe(m.updates_total(), &attributes);
        })),
        "replication_deletes_total" => ObserveMetricCallback::U64(Box::new(move |instrument| {
            instrument.observe(m.deletes_total(), &attributes);
        })),
        "replication_truncates_total" => ObserveMetricCallback::U64(Box::new(move |instrument| {
            instrument.observe(m.truncates_total(), &attributes);
        })),
        "replication_bootstrap_rows_total" => {
            ObserveMetricCallback::U64(Box::new(move |instrument| {
                instrument.observe(m.bootstrap_rows_total(), &attributes);
            }))
        }
        "replication_bootstrap_rows_expected" => {
            ObserveMetricCallback::U64(Box::new(move |instrument| {
                // Observe only when an estimate exists, so an absent series
                // (rather than `0`) means "unknown".
                if let Some(expected) = m.bootstrap_rows_expected() {
                    instrument.observe(expected, &attributes);
                }
            }))
        }
        "replication_bootstrap_complete" => {
            ObserveMetricCallback::U64(Box::new(move |instrument| {
                instrument.observe(m.bootstrap_complete(), &attributes);
            }))
        }
        "replication_decode_errors_total" => {
            ObserveMetricCallback::U64(Box::new(move |instrument| {
                instrument.observe(m.decode_errors_total(), &attributes);
            }))
        }
        "replication_schema_mismatch_errors_total" => {
            ObserveMetricCallback::U64(Box::new(move |instrument| {
                instrument.observe(m.schema_mismatch_errors_total(), &attributes);
            }))
        }
        "replication_recv_errors_total" => {
            ObserveMetricCallback::U64(Box::new(move |instrument| {
                instrument.observe(m.recv_errors_total(), &attributes);
            }))
        }
        "replication_reconnects_total" => ObserveMetricCallback::U64(Box::new(move |instrument| {
            instrument.observe(m.reconnects_total(), &attributes);
        })),
        "replication_checkpoint_persists_total" => {
            ObserveMetricCallback::U64(Box::new(move |instrument| {
                instrument.observe(m.checkpoint_persists_total(), &attributes);
            }))
        }
        "replication_checkpoint_persist_errors_total" => {
            ObserveMetricCallback::U64(Box::new(move |instrument| {
                instrument.observe(m.checkpoint_persist_errors_total(), &attributes);
            }))
        }
        _ => return None,
    };
    Some(callback)
}

fn replication_params_from_connector_params(
    params: &Parameters,
    dataset_name: &str,
) -> Result<ReplicationParams, String> {
    let opts = build_mysql_opts(params)?;
    let server_id = match optional_string(params, "replication_server_id") {
        Some(raw) => raw.trim().parse::<u32>().map_err(|e| {
            let user_param = params.user_param("replication_server_id");
            format!("parameter `{user_param}` must be a u32 server id, got {raw:?}: {e}")
        })?,
        None => derive_server_id(dataset_name, process_nonce()),
    };
    let snapshot_mode = match optional_string(params, "replication_snapshot_mode")
        .as_deref()
        .map(str::trim)
    {
        None | Some("") => SnapshotMode::Auto,
        Some(value) => match value.to_ascii_lowercase().as_str() {
            "auto" => SnapshotMode::Auto,
            "never" => SnapshotMode::Never,
            "always" => SnapshotMode::Always,
            other => {
                let user_param = params.user_param("replication_snapshot_mode");
                return Err(format!(
                    "parameter `{user_param}` must be 'auto', 'never', or 'always', got {other:?}"
                ));
            }
        },
    };
    let checkpoint_interval = optional_duration(
        params,
        "replication_checkpoint_interval",
        DEFAULT_CHECKPOINT_INTERVAL,
    )?;
    let bootstrap_batch_size = optional_usize_in_range(
        params,
        "replication_bootstrap_batch_size",
        DEFAULT_BOOTSTRAP_BATCH_SIZE,
        MAX_BOOTSTRAP_BATCH_SIZE,
    )?;
    let invalid_position_behavior =
        match optional_string(params, "replication_invalid_position_behavior")
            .as_deref()
            .map(str::trim)
        {
            None | Some("") => InvalidPositionBehavior::Error,
            Some(value) => match value.to_ascii_lowercase().as_str() {
                "error" => InvalidPositionBehavior::Error,
                "rebootstrap" => InvalidPositionBehavior::Rebootstrap,
                other => {
                    let user_param = params.user_param("replication_invalid_position_behavior");
                    return Err(format!(
                        "parameter `{user_param}` must be 'error' or 'rebootstrap', got {other:?}"
                    ));
                }
            },
        };

    Ok(ReplicationParams {
        opts,
        server_id,
        snapshot_mode,
        bootstrap_batch_size,
        checkpoint_interval,
        invalid_position_behavior,
    })
}

/// Build `mysql_async::Opts` for the replication connections from the same
/// connector params the read pool uses (`connection_string` overrides the
/// discrete params; `sslmode`/`sslrootcert` semantics match
/// `datafusion-table-providers`' `MySQL` pool).
fn build_mysql_opts(params: &Parameters) -> Result<Opts, String> {
    let mut builder = if let Some(connection_string) = optional_string(params, "connection_string")
    {
        OptsBuilder::from_opts(
            Opts::from_url(&connection_string)
                .map_err(|e| format!("invalid `connection_string`: {e}"))?,
        )
    } else {
        let mut builder = OptsBuilder::default();
        if let Some(host) = optional_string(params, "host") {
            builder = builder.ip_or_hostname(host);
        }
        if let Some(user) = optional_string(params, "user") {
            builder = builder.user(Some(user));
        }
        if let Some(pass) = optional_string(params, "pass") {
            builder = builder.pass(Some(pass));
        }
        if let Some(db) = optional_string(params, "db") {
            builder = builder.db_name(Some(db));
        }
        if let Some(port_raw) = optional_string(params, "tcp_port") {
            let port = port_raw.trim().parse::<u16>().map_err(|e| {
                let user_param = params.user_param("tcp_port");
                format!("parameter `{user_param}` must be a port number, got {port_raw:?}: {e}")
            })?;
            builder = builder.tcp_port(port);
        }
        builder
    };

    let sslmode = optional_string(params, "sslmode")
        .map_or_else(|| "required".to_string(), |s| s.to_lowercase());
    match sslmode.as_str() {
        "disabled" | "required" | "preferred" => {}
        other => {
            let user_param = params.user_param("sslmode");
            return Err(format!(
                "parameter `{user_param}` must be one of 'disabled', 'preferred', 'required', \
                 got {other:?}"
            ));
        }
    }
    let sslrootcert = optional_string(params, "sslrootcert");
    if let Some(path) = &sslrootcert
        && !std::path::Path::new(path).exists()
    {
        let user_param = params.user_param("sslrootcert");
        return Err(format!(
            "parameter `{user_param}` path does not exist: {path}"
        ));
    }
    builder = builder.ssl_opts(ssl_opts(&sslmode, sslrootcert));

    Ok(Opts::from(builder))
}

/// Same mapping as the read pool: `disabled` → no TLS; `preferred` → TLS with
/// certificate/hostname verification disabled; `required` → verified TLS
/// (against `sslrootcert` if provided, else system roots).
fn ssl_opts(sslmode: &str, rootcert_path: Option<String>) -> Option<SslOpts> {
    if sslmode == "disabled" {
        return None;
    }
    let mut opts = SslOpts::default();
    if let Some(path) = rootcert_path {
        opts = opts.with_root_certs(vec![std::path::PathBuf::from(path).into()]);
    }
    if sslmode == "preferred" {
        opts = opts
            .with_danger_accept_invalid_certs(true)
            .with_danger_skip_domain_validation(true);
    }
    Some(opts)
}

/// Splits `dataset.from` like `"mysql:mydb.orders"` into (database, table).
/// An unqualified table name falls back to the connection's `db` param.
fn split_database_table(from: &str, params: &Parameters) -> Result<(String, String), String> {
    let path = from.strip_prefix("mysql:").unwrap_or(from);
    let r = TableReference::from(path);
    match (r.schema(), r.table()) {
        (Some(db), table) => Ok((db.to_string(), table.to_string())),
        (None, table) => {
            let db = optional_string(params, "db").or_else(|| {
                optional_string(params, "connection_string")
                    .and_then(|cs| Opts::from_url(&cs).ok())
                    .and_then(|opts| opts.db_name().map(str::to_string))
            });
            match db {
                Some(db) => Ok((db, table.to_string())),
                None => Err(format!(
                    "cannot determine the source database for table `{table}`: qualify the \
                     dataset path (`mysql:<database>.{table}`) or set the `db` parameter"
                )),
            }
        }
    }
}

fn optional_string(params: &Parameters, key: &str) -> Option<String> {
    params.get(key).expose().ok().map(ToString::to_string)
}

fn optional_duration(
    params: &Parameters,
    key: &str,
    default: Duration,
) -> Result<Duration, String> {
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

fn optional_usize_in_range(
    params: &Parameters,
    key: &str,
    default: usize,
    max: usize,
) -> Result<usize, String> {
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
    use secrecy::SecretString;

    fn params_with(pairs: &[(&str, &str)]) -> Parameters {
        Parameters::new(
            pairs
                .iter()
                .map(|(k, v)| (k.to_string(), SecretString::from(v.to_string())))
                .collect(),
            "mysql",
            crate::PARAMETERS,
        )
    }

    #[test]
    fn splits_qualified_dataset_path() {
        let params = params_with(&[]);
        assert_eq!(
            split_database_table("mysql:mydb.orders", &params),
            Ok(("mydb".to_string(), "orders".to_string()))
        );
    }

    #[test]
    fn unqualified_path_falls_back_to_db_param() {
        let params = params_with(&[("db", "appdb")]);
        assert_eq!(
            split_database_table("mysql:orders", &params),
            Ok(("appdb".to_string(), "orders".to_string()))
        );
    }

    #[test]
    fn unqualified_path_falls_back_to_connection_string_db() {
        let params = params_with(&[("connection_string", "mysql://user:pass@localhost:3306/csdb")]);
        assert_eq!(
            split_database_table("mysql:orders", &params),
            Ok(("csdb".to_string(), "orders".to_string()))
        );
    }

    #[test]
    fn unqualified_path_without_db_errors() {
        let params = params_with(&[]);
        let err =
            split_database_table("mysql:orders", &params).expect_err("missing database must error");
        assert!(err.contains("mysql:<database>.orders"), "got: {err}");
    }

    #[test]
    fn replication_params_defaults() {
        let params = params_with(&[
            ("host", "localhost"),
            ("user", "root"),
            ("pass", "pw"),
            ("db", "mydb"),
            ("sslmode", "disabled"),
        ]);
        let repl = replication_params_from_connector_params(&params, "orders")
            .expect("valid params parse");
        assert_eq!(repl.snapshot_mode, SnapshotMode::Auto);
        assert_eq!(repl.bootstrap_batch_size, DEFAULT_BOOTSTRAP_BATCH_SIZE);
        assert_eq!(repl.checkpoint_interval, DEFAULT_CHECKPOINT_INTERVAL);
        assert_eq!(
            repl.invalid_position_behavior,
            InvalidPositionBehavior::Error
        );
        assert!(
            repl.server_id >= 100_000,
            "derived id clears reserved range"
        );
        assert_eq!(repl.opts.ip_or_hostname(), "localhost");
        assert_eq!(repl.opts.db_name(), Some("mydb"));
        assert!(repl.opts.ssl_opts().is_none());
    }

    #[test]
    fn explicit_server_id_wins() {
        let params = params_with(&[
            ("host", "localhost"),
            ("sslmode", "disabled"),
            ("replication_server_id", "4242"),
        ]);
        let repl = replication_params_from_connector_params(&params, "orders")
            .expect("valid params parse");
        assert_eq!(repl.server_id, 4242);
    }

    #[test]
    fn invalid_server_id_is_rejected() {
        let params = params_with(&[("replication_server_id", "not-a-number")]);
        let err = replication_params_from_connector_params(&params, "orders")
            .expect_err("bad server id must error");
        assert!(err.contains("mysql_replication_server_id"), "got: {err}");
    }

    #[test]
    fn invalid_position_behavior_parses_strictly() {
        let params = params_with(&[("replication_invalid_position_behavior", "rebootstrap")]);
        let repl = replication_params_from_connector_params(&params, "orders")
            .expect("valid params parse");
        assert_eq!(
            repl.invalid_position_behavior,
            InvalidPositionBehavior::Rebootstrap
        );

        let params = params_with(&[("replication_invalid_position_behavior", "reboot")]);
        let err = replication_params_from_connector_params(&params, "orders")
            .expect_err("typo must error");
        assert!(err.contains("'error' or 'rebootstrap'"), "got: {err}");
    }

    #[test]
    fn snapshot_mode_parses_strictly() {
        for (raw, expected) in [
            ("auto", SnapshotMode::Auto),
            ("never", SnapshotMode::Never),
            ("ALWAYS", SnapshotMode::Always),
        ] {
            let params = params_with(&[("replication_snapshot_mode", raw)]);
            let repl = replication_params_from_connector_params(&params, "orders")
                .expect("valid params parse");
            assert_eq!(repl.snapshot_mode, expected, "raw: {raw}");
        }

        // A typo must error loudly, not silently fall back to `auto` —
        // a misread `never` would otherwise snapshot a table the operator
        // explicitly opted out of copying.
        let params = params_with(&[("replication_snapshot_mode", "nevr")]);
        let err = replication_params_from_connector_params(&params, "orders")
            .expect_err("typo'd mode must error");
        assert!(
            err.contains("mysql_replication_snapshot_mode"),
            "got: {err}"
        );
    }

    #[test]
    fn unknown_sslmode_is_rejected() {
        let params = params_with(&[("sslmode", "verify-full")]);
        let err = replication_params_from_connector_params(&params, "orders")
            .expect_err("unsupported sslmode must error");
        assert!(err.contains("mysql_sslmode"), "got: {err}");
    }

    #[test]
    fn connection_string_overrides_discrete_params() {
        let params = params_with(&[
            ("connection_string", "mysql://u:p@db.internal:3307/csdb"),
            ("host", "ignored"),
            ("sslmode", "disabled"),
        ]);
        let repl = replication_params_from_connector_params(&params, "orders")
            .expect("valid params parse");
        assert_eq!(repl.opts.ip_or_hostname(), "db.internal");
        assert_eq!(repl.opts.tcp_port(), 3307);
        assert_eq!(repl.opts.db_name(), Some("csdb"));
    }

    #[test]
    fn every_replication_metric_spec_has_an_observe_callback() {
        // A spec without a callback registers a metric that never reports —
        // a silent observability hole. Lock the two lists together.
        let metrics = ReplicationMetrics::new(ReplicationMetricsCollector::new());
        for spec in REPLICATION_METRICS {
            assert!(
                observe_replication_metric(&metrics, spec.name, Vec::new()).is_some(),
                "metric spec `{}` has no observe callback",
                spec.name
            );
        }
    }
}
