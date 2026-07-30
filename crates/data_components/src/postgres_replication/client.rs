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

//! Drive the `pgwire_replication::ReplicationClient` and emit
//! [`crate::cdc::ChangeEnvelope`]s per transaction.

use std::time::Duration;

use arrow::datatypes::SchemaRef;
use secrecy::ExposeSecret;

use pgwire_replication::{Lsn, ReplicationConfig, TlsConfig};

use super::{
    ReplicationMetricsCollector, Result, SchemaMismatchSnafu,
    config::{ReplicationParams, SslMode},
};

pub(crate) fn build_replication_config(
    params: &ReplicationParams,
    slot_name: &str,
    publication_name: &str,
    start_lsn: u64,
) -> ReplicationConfig {
    // Map our `SslMode` to pgwire-replication's `TlsConfig`. The crate uses
    // rustls and its own SslMode enum (Disabled / Require / VerifyCa /
    // VerifyFull), so we pick the matching constructor and hand over the CA
    // bundle as-is — path or PEM content, the same value the setup and
    // bootstrap connections verify against.
    let tls = match params.sslmode {
        // Prefer maps to plaintext for WAL streaming. Rationale:
        // pgwire-replication does not expose a safe "try TLS then fall back
        // to plaintext" path, so the only two honest mappings are Disabled
        // or Require. Since `Prefer` is our parsing default, silently
        // strengthening it into Require would break non-TLS dev/test
        // Postgres instances that the regular connector happily talks to
        // (the setup connection uses tokio_postgres's real Prefer
        // semantics). Matching libpq's "don't block on missing TLS" intent
        // and staying symmetric with the setup path is the safer default —
        // operators who want TLS on replication must pick Require,
        // VerifyCa, or VerifyFull explicitly.
        SslMode::Disable | SslMode::Prefer => TlsConfig::disabled(),
        SslMode::Require => TlsConfig::require(),
        SslMode::VerifyCa => TlsConfig::verify_ca(params.sslrootcert.clone()),
        SslMode::VerifyFull => TlsConfig::verify_full(params.sslrootcert.clone()),
    };
    ReplicationConfig {
        host: params.host.clone(),
        port: params.port,
        user: params.user.clone(),
        password: params.password.expose_secret().to_string(),
        database: params.database.clone(),
        tls,
        slot: slot_name.to_string(),
        publication: publication_name.to_string(),
        start_lsn: Lsn(start_lsn),
        stop_at_lsn: None,
        status_interval: params.status_interval,
        idle_wakeup_interval: Duration::from_secs(1),
        buffer_events: 1024,
        // Decouple server-liveness feedback from downstream consumption: a slow
        // apply loop (or a slow shared-slot member) must never stall standby
        // status updates long enough for Postgres to hit `wal_sender_timeout`
        // and reset the walsender. See `pgwire_replication` worker `send_event`.
        feedback_while_backpressured: true,
        // pgoutput column output format. The connector always sets Binary (see
        // `ReplicationParams::pg_output_format`); tests may force Text to exercise
        // the fallback. Postgres still tags each column text/binary per-value, so
        // types without a binary send function (and the fixed Begin/Commit
        // framing) arrive as text and are decoded by the text fallback — but the
        // common int/float/date/timestamp/numeric columns arrive binary, skipping
        // text formatting + reparsing on both ends. The decoder handles either tag.
        format: params.pg_output_format,
        // Keep the crate default (~1 GiB) max_message_size so large TOAST-row
        // changes are never rejected; the reader allocates incrementally
        // regardless of this cap.
        ..Default::default()
    }
}

/// Convert a Postgres-epoch microsecond timestamp (from pgoutput Commit) into a
/// `SystemTime`. Postgres' epoch is 2000-01-01T00:00:00 UTC, not the Unix epoch.
pub(crate) fn pg_epoch_to_system_time(pg_micros: i64) -> std::time::SystemTime {
    // 30 years = 946_684_800 seconds between 1970-01-01 and 2000-01-01.
    const PG_EPOCH_UNIX_SECS: i64 = 946_684_800;
    let total_micros = pg_micros + PG_EPOCH_UNIX_SECS * 1_000_000;
    match u64::try_from(total_micros) {
        Ok(pos) => std::time::UNIX_EPOCH + std::time::Duration::from_micros(pos),
        Err(_) => std::time::UNIX_EPOCH,
    }
}

/// Threshold at which we stop logging individual reconnect attempts at WARN
/// level. The first failure is WARN so an outage is visible immediately; on
/// every subsequent failure within the same outage we drop to DEBUG to keep
/// the log volume sublinear in outage duration. The recovery INFO log is the
/// signal operators should grep for at the end of an outage.
const RECONNECT_WARN_THRESHOLD: u32 = 1;

/// Whether a reconnect attempt at `attempt` should log at WARN level. Above
/// the threshold, attempts log at DEBUG to keep log volume sublinear in outage
/// duration. Extracted as a pure function so the level transition can be
/// unit-tested without standing up a tracing subscriber.
fn reconnect_logs_at_warn(attempt: u32) -> bool {
    attempt <= RECONNECT_WARN_THRESHOLD
}

/// Emit a per-attempt log for a transient connect/recv failure. The first
/// attempt of an outage cycle is WARN (so an outage is loud and greppable);
/// subsequent attempts are DEBUG to avoid flooding logs during long outages.
pub(crate) fn log_transient_reconnect(attempt: u32, dataset: &str, error: &str, retry_in_ms: u128) {
    if reconnect_logs_at_warn(attempt) {
        tracing::warn!(
            dataset = %dataset,
            attempt,
            retry_in_ms = %retry_in_ms,
            error = %error,
            "replication connection lost; reconnecting"
        );
    } else {
        tracing::debug!(
            dataset = %dataset,
            attempt,
            retry_in_ms = %retry_in_ms,
            error = %error,
            "replication connection still down; reconnecting"
        );
    }
}

pub(crate) fn validate_relation_against_schema(
    dataset_schema: &SchemaRef,
    rel: &super::pgoutput::Relation,
    declared_pks: &[String],
    generated_columns: &[String],
) -> Result<()> {
    for field in dataset_schema.fields() {
        if !rel.columns.iter().any(|c| c.name == *field.name()) {
            // GENERATED columns are absent from pgoutput Relation messages by
            // Postgres design — they're catalog-confirmed at setup, so their
            // absence here is expected (applied as NULL downstream). Any
            // OTHER missing column means the source schema really changed.
            if generated_columns.iter().any(|g| g == field.name()) {
                continue;
            }
            return SchemaMismatchSnafu {
                message: format!(
                    "column `{}` from dataset schema is missing in source relation {}.{}",
                    field.name(),
                    rel.namespace,
                    rel.name
                ),
            }
            .fail();
        }
    }
    validate_relation_primary_keys(rel, declared_pks)
}

/// Validate that every dataset-declared primary key exists on the relation and
/// is part of the source replica identity. Runs for every policy — UPDATE and
/// DELETE events cannot be routed without the key columns.
pub(crate) fn validate_relation_primary_keys(
    rel: &super::pgoutput::Relation,
    declared_pks: &[String],
) -> Result<()> {
    for pk in declared_pks {
        let Some(col) = rel.columns.iter().find(|c| c.name == *pk) else {
            return SchemaMismatchSnafu {
                message: format!(
                    "declared primary_key `{pk}` not found on source relation {}.{}",
                    rel.namespace, rel.name
                ),
            }
            .fail();
        };
        if !col.is_key {
            return SchemaMismatchSnafu {
                message: format!(
                    "column `{pk}` is not part of source replica identity on {}.{}",
                    rel.namespace, rel.name
                ),
            }
            .fail();
        }
    }
    Ok(())
}

/// Under `on_schema_change: block`, a mid-stream column add is silently
/// ignored (the legacy behavior). Surface that narrowing loudly: warn once per
/// relation change naming the dropped columns, counted as a rejected schema
/// evolution. The first relation of a connection only seeds the baseline —
/// columns intentionally excluded from the dataset schema must not warn.
pub(crate) fn warn_on_new_relation_columns(
    rel: &super::pgoutput::Relation,
    known_relation_columns: &mut Option<std::collections::HashSet<String>>,
    dataset_name: &str,
    metrics: &ReplicationMetricsCollector,
) {
    let current: std::collections::HashSet<String> =
        rel.columns.iter().map(|c| c.name.clone()).collect();
    if let Some(known) = known_relation_columns.as_ref() {
        let added: Vec<&str> = current
            .iter()
            .filter(|name| !known.contains(*name))
            .map(String::as_str)
            .collect();
        if !added.is_empty() {
            metrics.inc_schema_evolution_rejected();
            tracing::warn!(
                dataset = %dataset_name,
                columns = ?added,
                "source relation {}.{} gained columns whose values are being silently dropped. Set `on_schema_change: append_new_columns` (or `sync_all_columns`) on the dataset to adopt new columns",
                rel.namespace,
                rel.name
            );
        }
    }
    *known_relation_columns = Some(current);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validation_tolerates_generated_columns_but_not_dropped_ones() {
        use crate::postgres_replication::pgoutput::{Column, Relation};
        let schema: SchemaRef = std::sync::Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int32, false),
            arrow::datatypes::Field::new("name", arrow::datatypes::DataType::Utf8, true),
            arrow::datatypes::Field::new("name_lower", arrow::datatypes::DataType::Utf8, true),
        ]));
        // pgoutput omits GENERATED columns: the relation carries only id+name.
        let rel = Relation {
            relation_id: 7,
            namespace: "public".into(),
            name: "apps".into(),
            replica_identity: b'd',
            columns: vec![
                Column {
                    is_key: true,
                    name: "id".into(),
                    type_oid: 23,
                    type_modifier: -1,
                },
                Column {
                    is_key: false,
                    name: "name".into(),
                    type_oid: 25,
                    type_modifier: -1,
                },
            ],
        };
        let pks = vec!["id".to_string()];

        // Catalog-confirmed generated column → tolerated.
        validate_relation_against_schema(&schema, &rel, &pks, &["name_lower".to_string()])
            .expect("generated column absence must validate");

        // Same absence WITHOUT catalog confirmation = a real schema change.
        let err = validate_relation_against_schema(&schema, &rel, &pks, &[])
            .expect_err("non-generated missing column must fail validation");
        assert!(err.to_string().contains("name_lower"), "got: {err}");
    }

    #[test]
    fn reconnect_first_attempt_logs_at_warn() {
        // The first failure of an outage cycle must stay at WARN so the
        // outage is visible in default-level logs. Demoting all attempts to
        // DEBUG would mean an outage is silent unless DEBUG is enabled.
        assert!(reconnect_logs_at_warn(1));
    }

    #[test]
    fn reconnect_subsequent_attempts_drop_to_debug() {
        // Every attempt after the first within the same outage cycle drops
        // to DEBUG. This is the volume-suppression behavior that #10971
        // requested: a 1-hour outage no longer floods the log with 3600+
        // WARN lines per dataset.
        for attempt in 2..=100 {
            assert!(
                !reconnect_logs_at_warn(attempt),
                "attempt {attempt} should log at DEBUG, not WARN",
            );
        }
    }

    #[test]
    fn reconnect_log_helper_handles_saturating_attempt_count() {
        // Helper should be callable across the full u32 range (including
        // the saturated max value) without panicking — the production
        // counter uses `saturating_add` so it can sit at u32::MAX for an
        // arbitrarily long outage.
        log_transient_reconnect(u32::MAX, "events", "connection refused", 500);
    }
}
