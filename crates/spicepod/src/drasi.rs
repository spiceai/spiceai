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

#[cfg(feature = "schemars")]
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::param::Params;

/// Dataset-level [Drasi](https://drasi.io) change-forwarding configuration.
///
/// **Alpha** — in preview, and should not be used in production.
///
/// Publishes the dataset's change-data-capture stream to a Drasi source, so Drasi
/// continuous queries observe the same changes the runtime applies to the local
/// accelerator. Each changed row becomes one Drasi graph node: the row's primary
/// key derives the element id, the source table name becomes the node label, and
/// the row's columns become node properties.
///
/// Requires `acceleration.refresh_mode: changes` on the same dataset — the
/// forwarder taps the CDC stream, so a dataset without one has nothing to forward.
///
/// # Example
///
/// ```yaml
/// datasets:
///   - from: postgres:public.orders
///     name: orders
///     acceleration:
///       enabled: true
///       engine: cayenne
///       refresh_mode: changes
///     drasi:
///       source_id: spice-cdc
///       forwarding: enabled   # or `disabled`, to park the config
///       labels: [public.orders]
///       params:
///         drasi_http_endpoint: http://localhost:9000
/// ```
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "schemars", derive(JsonSchema))]
pub struct Drasi {
    /// The id of the Drasi source to publish into.
    ///
    /// Must string-equal the `id` of a source already declared on the Drasi side;
    /// Drasi never auto-creates one, and a mismatch is rejected by the server.
    pub source_id: String,

    /// Whether this block forwards. Defaults to `enabled`; set `disabled` to
    /// keep the configuration in place without publishing anything.
    #[serde(default, skip_serializing_if = "crate::component::is_default")]
    pub forwarding: DrasiForwarding,

    /// Node labels applied to every element from this dataset. Drasi continuous
    /// queries match on these (`MATCH (o:public.orders)`).
    ///
    /// Defaults to the dataset's source table name, matching the convention used
    /// by Drasi's own relational sources (schema-qualified when the source
    /// qualifies it, e.g. `public.orders`).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub labels: Vec<String>,

    /// How change events reach Drasi. Defaults to `http`.
    #[serde(default, skip_serializing_if = "crate::component::is_default")]
    pub transport: DrasiTransport,

    /// When a change counts as handed off. Defaults to `acknowledged`, which
    /// never loses a change but paces replication at Drasi's speed. Use
    /// `queued` to decouple them.
    #[serde(default, skip_serializing_if = "crate::component::is_default")]
    pub delivery: DrasiDelivery,

    /// What the CDC stream does when Drasi cannot accept a change event.
    /// Defaults to `block`. Applies only to `delivery: acknowledged`.
    #[serde(default, skip_serializing_if = "crate::component::is_default")]
    pub on_delivery_error: OnDeliveryError,

    /// Transport connection parameters.
    ///
    /// For `transport: http`:
    /// - `drasi_http_endpoint` (required) — the Drasi HTTP source's own listener,
    ///   e.g. `http://localhost:9000`. This is the port the source binds, not the
    ///   Drasi Server management API port.
    /// - `drasi_request_timeout` (optional) — per-request timeout, e.g. `30s`.
    ///
    /// For `transport: redis`:
    /// - `drasi_redis_url` (required) — e.g. `redis://localhost:6379`, or
    ///   `rediss://` for TLS.
    /// - `drasi_stream_key` (required) — the Redis stream the Drasi platform
    ///   source reads, e.g. `drasi-events`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub params: Option<Params>,
}

/// When a change is considered handed off to Drasi.
///
/// This is the throughput/durability trade for the CDC path. It exists because
/// the strict choice makes a reaction engine's availability a ceiling on
/// replication throughput, which is rarely what an operator wants from a
/// downstream consumer.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum DrasiDelivery {
    /// A change reaches Drasi before its replication position is acknowledged.
    ///
    /// Nothing is lost — a stall or a crash replays from the unacknowledged
    /// position — but replication only advances as fast as Drasi accepts, so a
    /// slow or unreachable Drasi slows or stops it, and the source's replication
    /// log grows behind it. See `on_delivery_error` for what a failure does.
    #[default]
    Acknowledged,
    /// A change is queued locally and its replication position acknowledged
    /// immediately; delivery is retried in the background.
    ///
    /// Replication never waits for Drasi. A change Drasi will not accept is
    /// written to a durable dead-letter store under `.spice/data/drasi` and
    /// retried until it lands, surviving a restart — so the replication log is
    /// no longer what replays a failure, this is.
    ///
    /// Because an insert or update is a full-state replace keyed by element id,
    /// redelivery must not be overtaken: once anything is pending, later changes
    /// for that component queue behind it and delivery resumes only once the
    /// store drains. Drasi's view therefore advances in order or not at all.
    ///
    /// A change Drasi will *never* accept — a rejected payload, or an operation
    /// with no Drasi equivalent — is counted and discarded rather than retained,
    /// since retaining it would block every later change behind something that
    /// cannot succeed.
    ///
    /// `on_delivery_error` does not apply and is ignored here: there is no
    /// replication position left to hold, and the queue is what decides a
    /// failure's fate.
    Queued,
}

/// Whether a `drasi:` block actually forwards.
///
/// Lets a complete, valid configuration stay in the Spicepod while forwarding is
/// turned off — so switching it back on is a one-word edit rather than
/// reconstructing the endpoint, labels and key columns from memory.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum DrasiForwarding {
    /// Changes are published to the configured Drasi source.
    #[default]
    Enabled,
    /// Nothing is published, and the rest of the block is left unvalidated —
    /// including the `acceleration.refresh_mode: changes` requirement — so a
    /// dataset can be parked without its Drasi settings being removed.
    Disabled,
}

/// The transport a [`Drasi`] forwarder publishes over.
///
/// The two reach different Drasi deployments and speak different wire formats;
/// they are not interchangeable against one server.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum DrasiTransport {
    /// POST batches to a Drasi Server HTTP source's ingestion route.
    #[default]
    Http,
    /// `XADD` `CloudEvents` envelopes onto the Redis stream a Drasi platform
    /// source consumes. Reaches a Kubernetes-deployed Drasi without building
    /// custom source containers.
    Redis,
}

/// What the change stream does when a change event cannot be delivered to Drasi.
///
/// Delivery is attempted *before* the change envelope acknowledges the source's
/// replication position, so `block` — the default — cannot lose an event.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum OnDeliveryError {
    /// Retry with backoff until delivery succeeds. No event is ever lost, but
    /// acceleration stalls for as long as Drasi is unreachable, and the source's
    /// replication log grows behind the unacknowledged position.
    #[default]
    Block,
    /// Retry with backoff up to a bounded attempt budget, then log the failure,
    /// count it, and let the change through. Acceleration is never held up by a
    /// downstream outage, at the cost of Drasi missing those changes.
    Skip,
    /// Retry with backoff up to a bounded attempt budget, then fail the change
    /// stream. The dataset is marked in error and stops applying changes.
    Fail,
}

/// Runtime-level Drasi forwarding for the tables Spice generates itself.
///
/// **Alpha** — in preview, and should not be used in production.
///
/// Publishes writes to the runtime's own tables — `task_history`, `metrics` —
/// so Drasi continuous queries can react to Spice's operational events: a query
/// that ran too long, a dataset refresh that failed. These tables are not
/// change-data-captured from an external source, so this is configured
/// separately from a dataset's `drasi:` block.
///
/// It deliberately has no `on_delivery_error`. That setting trades a CDC stall
/// for zero loss, which works because an unacknowledged replication position
/// replays. There is no such position here: these rows are the runtime's own
/// telemetry, so delivery is queued per table, and an unreachable Drasi is
/// counted and dropped rather than allowed to stall the writer.
///
/// # Example
///
/// ```yaml
/// runtime:
///   drasi:
///     source_id: spice-runtime
///     params:
///       drasi_http_endpoint: http://localhost:9000
///     tables:
///       - name: task_history
/// ```
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "schemars", derive(JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct RuntimeDrasi {
    /// The id of the Drasi source to publish into.
    pub source_id: String,

    /// Whether this block forwards. Defaults to `enabled`; set `disabled` to
    /// keep the configuration in place without publishing anything.
    #[serde(default, skip_serializing_if = "crate::component::is_default")]
    pub forwarding: DrasiForwarding,

    /// The runtime tables to forward. Nothing is forwarded until a table is
    /// listed, so enabling this never publishes a table the operator did not
    /// name.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tables: Vec<RuntimeDrasiTable>,

    /// How change events reach Drasi. Defaults to `http`.
    #[serde(default, skip_serializing_if = "crate::component::is_default")]
    pub transport: DrasiTransport,

    /// Transport connection parameters — the same keys a dataset's `drasi:`
    /// block accepts.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub params: Option<Params>,
}

/// One runtime table to forward.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "schemars", derive(JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct RuntimeDrasiTable {
    /// Table name within the `runtime` schema, e.g. `task_history`.
    pub name: String,

    /// Columns that identify a row, used to derive the Drasi element id.
    ///
    /// Defaults to the table's declared primary key. A table with neither — the
    /// runtime's `metrics` table declares none — is refused rather than
    /// forwarded under a synthesized id, which would publish a duplicate node
    /// every time a delivery is retried.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub key: Vec<String>,

    /// Node labels. Defaults to the qualified table name, e.g.
    /// `runtime.task_history`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub labels: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runtime_block_parses_a_minimal_table_list() {
        let drasi: RuntimeDrasi = yaml::from_str(
            r"
source_id: spice-runtime
tables:
  - name: task_history
",
        )
        .expect("valid runtime drasi config");

        assert_eq!(drasi.transport, DrasiTransport::Http);
        assert_eq!(drasi.tables.len(), 1);
        assert_eq!(drasi.tables[0].name, "task_history");
        assert!(
            drasi.tables[0].key.is_empty(),
            "key defaults to the table PK"
        );
    }

    /// The runtime surface deliberately has no `on_delivery_error`: blocking the
    /// telemetry writer buys nothing, so the knob is not offered at all.
    #[test]
    fn runtime_block_rejects_on_delivery_error() {
        yaml::from_str::<RuntimeDrasi>(
            r"
source_id: spice-runtime
on_delivery_error: block
tables:
  - name: task_history
",
        )
        .expect_err("on_delivery_error is not part of the runtime surface");
    }

    /// Nothing is forwarded until a table is named.
    #[test]
    fn runtime_block_forwards_no_table_by_default() {
        let drasi: RuntimeDrasi =
            yaml::from_str("source_id: spice-runtime").expect("valid runtime drasi config");
        assert!(drasi.tables.is_empty());
    }

    #[test]
    fn runtime_table_accepts_an_explicit_key_and_labels() {
        let drasi: RuntimeDrasi = yaml::from_str(
            r"
source_id: spice-runtime
tables:
  - name: metrics
    key: [time_unix_nano, name]
    labels: [Metric]
",
        )
        .expect("valid runtime drasi config");

        assert_eq!(drasi.tables[0].key, vec!["time_unix_nano", "name"]);
        assert_eq!(drasi.tables[0].labels, vec!["Metric".to_string()]);
    }

    #[test]
    fn transport_and_error_policy_parse_from_snake_case() {
        let drasi: Drasi = yaml::from_str(
            r"
source_id: cdc-feed
labels: [public.orders]
transport: redis
on_delivery_error: skip
",
        )
        .expect("valid drasi config");

        assert_eq!(drasi.source_id, "cdc-feed");
        assert_eq!(drasi.labels, vec!["public.orders".to_string()]);
        assert_eq!(drasi.transport, DrasiTransport::Redis);
        assert_eq!(drasi.on_delivery_error, OnDeliveryError::Skip);
    }

    /// The conservative defaults are what a minimal block gets: HTTP transport,
    /// and a delivery failure that stalls rather than drops.
    #[test]
    fn minimal_config_defaults_to_http_and_block() {
        let drasi: Drasi = yaml::from_str("source_id: cdc-feed").expect("valid drasi config");

        assert_eq!(drasi.transport, DrasiTransport::Http);
        assert_eq!(drasi.on_delivery_error, OnDeliveryError::Block);
        assert!(drasi.labels.is_empty(), "labels default to the table name");
        assert!(drasi.params.is_none());
    }

    /// A block is live unless it says otherwise, so adding one does what it
    /// looks like it does.
    #[test]
    fn forwarding_defaults_to_enabled_on_both_surfaces() {
        let dataset: Drasi = yaml::from_str("source_id: cdc-feed").expect("valid drasi config");
        assert_eq!(dataset.forwarding, DrasiForwarding::Enabled);

        let runtime: RuntimeDrasi =
            yaml::from_str("source_id: spice-runtime").expect("valid runtime drasi config");
        assert_eq!(runtime.forwarding, DrasiForwarding::Enabled);
    }

    /// The point of the toggle: the rest of the block survives being turned off.
    #[test]
    fn disabling_keeps_the_rest_of_the_configuration() {
        let drasi: Drasi = yaml::from_str(
            r"
source_id: cdc-feed
forwarding: disabled
labels: [public.orders]
transport: redis
params:
  drasi_redis_url: redis://localhost:6379
  drasi_stream_key: drasi-events
",
        )
        .expect("valid drasi config");

        assert_eq!(drasi.forwarding, DrasiForwarding::Disabled);
        assert_eq!(drasi.labels, vec!["public.orders".to_string()]);
        assert_eq!(drasi.transport, DrasiTransport::Redis);
        assert!(drasi.params.is_some(), "transport params are preserved");
    }

    #[test]
    fn runtime_block_can_be_disabled() {
        let runtime: RuntimeDrasi = yaml::from_str(
            r"
source_id: spice-runtime
forwarding: disabled
tables:
  - name: task_history
",
        )
        .expect("valid runtime drasi config");

        assert_eq!(runtime.forwarding, DrasiForwarding::Disabled);
        assert_eq!(runtime.tables.len(), 1, "table list is preserved");
    }

    /// The default keeps every change, at the cost of pacing replication at
    /// Drasi's speed.
    #[test]
    fn delivery_defaults_to_acknowledged() {
        let drasi: Drasi = yaml::from_str("source_id: cdc-feed").expect("valid drasi config");
        assert_eq!(drasi.delivery, DrasiDelivery::Acknowledged);
    }

    #[test]
    fn delivery_can_be_queued() {
        let drasi: Drasi = yaml::from_str(
            r"
source_id: cdc-feed
delivery: queued
",
        )
        .expect("valid drasi config");
        assert_eq!(drasi.delivery, DrasiDelivery::Queued);
    }

    #[test]
    fn source_id_is_required() {
        yaml::from_str::<Drasi>("labels: [orders]")
            .expect_err("source_id has no default and must be rejected when absent");
    }
}
