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
//!   - Parse connection & replication params out of `runtime::parameters::Parameters`.
//!   - Fall back to sensible per-replica defaults for slot & publication names.
//!   - Look up the source table schema (via the federated table) and hand everything
//!     off to `data_components::postgres_replication::start_replication_stream`.

use std::sync::Arc;
use std::time::Duration;

use async_stream::try_stream;
use data_components::cdc::{ChangesStream, StreamError};
use data_components::postgres_replication::{
    ReplicationMetrics, ReplicationMetricsCollector, ReplicationParams, ReplicationStreamInput,
    config, start_replication_stream,
};
use datafusion::sql::TableReference;
use futures::StreamExt;
use opentelemetry::KeyValue;
use runtime::component::ComponentType;
use runtime::component::dataset::Dataset;
use runtime::component::metrics::{MetricSpec, MetricType, MetricsProvider, ObserveMetricCallback};
use runtime::federated_table::FederatedTable;
use runtime::parameters::{ExposedParamLookup, Parameters};
use secrecy::SecretString;

const DEFAULT_STATUS_INTERVAL: Duration = Duration::from_secs(10);
const DEFAULT_BOOTSTRAP_BATCH_SIZE: usize = 8192;
const MAX_BOOTSTRAP_BATCH_SIZE: usize = 1_048_576;

pub fn build_changes_stream(
    params: &Parameters,
    dataset: &Dataset,
    federated_table: Arc<FederatedTable>,
    metrics: Arc<ReplicationMetricsCollector>,
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
    // accelerators (snapshot + WAL resume converges via the PK upsert).
    if dataset
        .acceleration
        .as_ref()
        .is_some_and(accelerator_is_ephemeral)
    {
        params_for_stream.snapshot_on_resume = true;
        tracing::info!(
            dataset = %dataset_name,
            "non-persistent accelerator with `refresh_mode: changes`: the initial snapshot \
             will run on every start, including replication-slot resume"
        );
    }

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
        runtime::component::dataset::acceleration::Engine::Arrow
            | runtime::component::dataset::acceleration::Engine::PartitionedArrow
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
                Some(runtime::component::dataset::acceleration::OnConflictBehavior::Upsert(_))
            )
        })
    });

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
             `pg_replication_slots.confirmed_flush_lsn`.",
    ),
    MetricSpec::new(
        "replication_server_wal_end_lsn",
        MetricType::ObservableGaugeU64,
    )
    .description(
        "Most recent WAL end LSN reported by the Postgres server (via keepalive or WAL data).",
    ),
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
            _ => None,
        }
    }
}

/// Whether the accelerator's state survives a process restart. Non-persistent
/// accelerators must re-snapshot on every start — WAL replay from the slot's
/// checkpoint can never reconstruct an accelerator that booted empty.
fn accelerator_is_ephemeral(
    acceleration: &runtime::component::dataset::acceleration::Acceleration,
) -> bool {
    use runtime::component::dataset::acceleration::{Engine, Mode};
    match acceleration.engine.to_unpartitioned() {
        // Always in-memory.
        Engine::Arrow => true,
        // In-memory unless file-backed; `file_create` truncates on startup,
        // which is just as empty as memory from replication's point of view.
        Engine::DuckDB | Engine::Sqlite | Engine::Turso => {
            matches!(acceleration.mode, Mode::Memory | Mode::FileCreate)
        }
        // External storage (another Postgres, object-store-backed Cayenne)
        // persists independently of this process. `to_unpartitioned` already
        // folded the partitioned variants into their base engines.
        _ => false,
    }
}

fn replication_params_from_connector_params(
    params: &Parameters,
    dataset_name: &str,
) -> std::result::Result<ReplicationParams, String> {
    let host = required_string(params, "host")?;
    let port = optional_string(params, "port")
        .and_then(|s| s.parse::<u16>().ok())
        .unwrap_or(5432);
    let user = required_string(params, "user")?;
    let password_str = required_secret(params, "pass")?;
    let database = required_string(params, "db")?;
    let sslmode =
        config::SslMode::from_str_or_default(optional_string(params, "sslmode").as_deref());
    let sslrootcert = optional_string(params, "sslrootcert").map(std::path::PathBuf::from);

    // An explicitly-named slot is shareable: every dataset on the same
    // connection naming the same slot is multiplexed onto one replication
    // connection (see `data_components::postgres_replication::shared`). The
    // default publication is then derived from the slot — not the dataset —
    // so all members land on the same publication.
    //
    // `pg_replication_slot_scope: instance` opts into the same multiplexer with
    // a *generated* instance-scoped slot name (`spice_inst_<instance>_<source>`)
    // shared across every changes-mode dataset on this source — collapsing
    // `datasets × replicas` slots down to `replicas` (one per replica per
    // source), while staying distinct per replica via the instance hash. An
    // explicit `pg_replication_slot` takes precedence and is used verbatim.
    let explicit_slot = optional_string(params, "replication_slot");
    let instance_scoped = matches!(
        optional_string(params, "replication_slot_scope").as_deref(),
        Some("instance")
    );
    let explicit_publication = optional_string(params, "publication");
    let (slot_name, publication_name, shared) = match explicit_slot {
        Some(slot) => {
            if instance_scoped {
                tracing::warn!(
                    dataset = %dataset_name,
                    "both `pg_replication_slot` and `pg_replication_slot_scope: instance` are \
                     set; using the explicit slot name verbatim and ignoring the scope. Remove \
                     `pg_replication_slot` to get an instance-scoped (per-replica) slot."
                );
            }
            let publication =
                explicit_publication.unwrap_or_else(|| config::publication_name_for_slot(&slot));
            (slot, publication, true)
        }
        None if instance_scoped => {
            let slot = config::instance_slot_name(&host, port, &database, &user);
            let publication =
                explicit_publication.unwrap_or_else(|| config::publication_name_for_slot(&slot));
            (slot, publication, true)
        }
        None => (
            config::default_slot_name(dataset_name),
            explicit_publication.unwrap_or_else(|| config::default_publication_name(dataset_name)),
            false,
        ),
    };
    let initial_snapshot = optional_string(params, "replication_initial_snapshot")
        .is_none_or(|s| parse_bool_default_true(&s));
    let temporary_slot = optional_string(params, "replication_temporary_slot")
        .is_some_and(|s| parse_bool_default_false(&s));
    let status_interval = optional_string(params, "replication_status_interval")
        .and_then(|s| fundu::parse_duration(&s).ok())
        .unwrap_or(DEFAULT_STATUS_INTERVAL);
    let bootstrap_batch_size = optional_usize_in_range(
        params,
        "replication_bootstrap_batch_size",
        DEFAULT_BOOTSTRAP_BATCH_SIZE,
        MAX_BOOTSTRAP_BATCH_SIZE,
    )?;

    Ok(ReplicationParams {
        host,
        port,
        user,
        password: SecretString::from(password_str),
        database,
        sslmode,
        sslrootcert,
        slot_name,
        publication_name,
        initial_snapshot,
        // Set by the caller from the dataset's acceleration config.
        snapshot_on_resume: false,
        temporary_slot,
        status_interval,
        bootstrap_batch_size,
        shared,
    })
}

fn required_string(params: &Parameters, key: &str) -> std::result::Result<String, String> {
    match params.get(key).expose() {
        ExposedParamLookup::Present(v) => Ok(v.to_string()),
        ExposedParamLookup::Absent(name) => Err(format!("missing required parameter `{name}`")),
    }
}

fn required_secret(params: &Parameters, key: &str) -> std::result::Result<String, String> {
    match params.get(key).expose() {
        ExposedParamLookup::Present(v) => Ok(v.to_string()),
        ExposedParamLookup::Absent(name) => Err(format!("missing required secret `{name}`")),
    }
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

fn parse_bool_default_true(s: &str) -> bool {
    matches!(s.to_ascii_lowercase().as_str(), "true" | "1" | "yes" | "y") || s.is_empty()
}

fn parse_bool_default_false(s: &str) -> bool {
    matches!(s.to_ascii_lowercase().as_str(), "true" | "1" | "yes" | "y")
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

    /// Build a full connector param set (the connection params are required by
    /// `replication_params_from_connector_params`) plus any extra entries.
    fn conn_params(db: &str, extra: &[(&str, &str)]) -> Parameters {
        let mut kv: Vec<(String, SecretString)> = vec![
            ("host".to_string(), SecretString::from("db.example.com")),
            ("port".to_string(), SecretString::from("5432")),
            ("user".to_string(), SecretString::from("spice")),
            ("pass".to_string(), SecretString::from("secret")),
            ("db".to_string(), SecretString::from(db.to_string())),
        ];
        for (k, v) in extra {
            kv.push(((*k).to_string(), SecretString::from((*v).to_string())));
        }
        Parameters::new(kv, "pg", crate::PARAMETERS)
    }

    #[test]
    fn default_scope_is_per_dataset_and_unshared() {
        let p = replication_params_from_connector_params(&conn_params("appdb", &[]), "public.apps")
            .expect("params build");
        assert!(
            !p.shared,
            "default scope must keep a dedicated per-dataset stream"
        );
        assert_eq!(p.slot_name, config::default_slot_name("public.apps"));
        assert_eq!(
            p.publication_name,
            config::default_publication_name("public.apps")
        );
    }

    #[test]
    fn scope_instance_produces_a_shared_instance_slot() {
        let p = replication_params_from_connector_params(
            &conn_params("appdb", &[("replication_slot_scope", "instance")]),
            "public.apps",
        )
        .expect("params build");
        assert!(p.shared, "instance scope must use the shared multiplexer");
        assert!(
            p.slot_name.starts_with("spice_inst_"),
            "got slot `{}`",
            p.slot_name
        );
        // Publication is derived from the (shared) slot, not the dataset, so
        // every member lands on the same publication.
        assert_eq!(
            p.publication_name,
            config::publication_name_for_slot(&p.slot_name)
        );
    }

    #[test]
    fn scope_instance_shares_one_slot_across_datasets_on_the_same_source() {
        let apps = replication_params_from_connector_params(
            &conn_params("appdb", &[("replication_slot_scope", "instance")]),
            "public.apps",
        )
        .expect("params build");
        let orgs = replication_params_from_connector_params(
            &conn_params("appdb", &[("replication_slot_scope", "instance")]),
            "public.orgs",
        )
        .expect("params build");
        // Identical slot + publication → the multiplexer collapses them onto one
        // replication connection (datasets × replicas → replicas).
        assert_eq!(apps.slot_name, orgs.slot_name);
        assert_eq!(apps.publication_name, orgs.publication_name);
    }

    #[test]
    fn scope_instance_distinguishes_different_sources() {
        let appdb = replication_params_from_connector_params(
            &conn_params("appdb", &[("replication_slot_scope", "instance")]),
            "public.apps",
        )
        .expect("params build");
        let otherdb = replication_params_from_connector_params(
            &conn_params("otherdb", &[("replication_slot_scope", "instance")]),
            "public.apps",
        )
        .expect("params build");
        // Logical slot names are unique cluster-wide; two databases on the same
        // server must not collide on one physical slot.
        assert_ne!(appdb.slot_name, otherdb.slot_name);
    }

    #[test]
    fn explicit_slot_overrides_instance_scope() {
        let p = replication_params_from_connector_params(
            &conn_params(
                "appdb",
                &[
                    ("replication_slot", "my_slot"),
                    ("replication_slot_scope", "instance"),
                ],
            ),
            "public.apps",
        )
        .expect("params build");
        assert!(p.shared);
        assert_eq!(p.slot_name, "my_slot", "explicit slot is used verbatim");
        assert_eq!(p.publication_name, "my_slot_pub");
    }
}
