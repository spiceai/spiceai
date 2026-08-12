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

//! Forwards a dataset's change stream to a [Drasi](https://drasi.io) source.
//!
//! [`DrasiConnector`](connector::DrasiConnector) wraps the dataset's own data
//! connector and decorates its change stream, so every change the accelerator
//! applies is also published to Drasi.

pub mod connector;
pub(crate) mod dead_letter;
pub(crate) mod internal;
pub(crate) mod queue;

use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Array, ListArray, StringArray};
use arrow::datatypes::ArrowNativeType;
use data_components::cdc::{ChangeBatch, ChangeEnvelope, StreamError};
use runtime_drasi::config::DEFAULT_REQUEST_TIMEOUT;
use runtime_drasi::{
    DrasiChangeRows, DrasiSink, DrasiSinkConfig, ElementMapping, OnDeliveryError, TransportConfig,
};

use crate::drasi::dead_letter::{DEFAULT_MAX_BATCHES, DeadLetterStore, store_path};

/// The policy every sink behind a [`DeliveryQueue`] is built with.
///
/// Bounded retry then **surface** the error: a transient blip is absorbed
/// without touching the disk, and anything that outlives the budget becomes an
/// `Err` the queue can retain. `Skip` would swallow it into `Ok(())` — the
/// queue would then treat a lost batch as delivered and never retain it, which
/// made the dead-letter store dead code on every path that used it.
pub(crate) const QUEUED_SINK_POLICY: OnDeliveryError = OnDeliveryError::Fail;
use crate::drasi::queue::{DEFAULT_QUEUE_DEPTH, DeliveryQueue, QueuedBatch};
use spicepod::drasi::{Drasi as DrasiSpec, DrasiTransport as DrasiTransportSpec};

use crate::component::dataset::Dataset;

const HTTP_ENDPOINT_PARAM: &str = "drasi_http_endpoint";
const REQUEST_TIMEOUT_PARAM: &str = "drasi_request_timeout";
const REDIS_URL_PARAM: &str = "drasi_redis_url";
const STREAM_KEY_PARAM: &str = "drasi_stream_key";

/// Builds the sink for `dataset` from its `drasi:` block.
///
/// # Errors
///
/// Returns an error if a required transport parameter is missing or unusable.
pub(crate) async fn sink_for_dataset(
    dataset: &Dataset,
    spec: &DrasiSpec,
) -> runtime_drasi::Result<DeliveryMode> {
    let name = dataset.name.to_string();
    let sink = build_sink(
        name.clone(),
        &spec.source_id,
        labels_for(dataset, spec),
        spec.transport,
        // `on_delivery_error` describes what a failure does to the *change
        // stream*, which only means anything while the stream is still holding
        // the replication position — that is, under `acknowledged`.
        //
        // Under `queued` the position is already released and the queue and its
        // dead-letter store are the durability mechanism, so the sink must
        // *surface* a failure for them to act on. Honouring `block` here instead
        // parked the single delivery task on an unbounded retry, filled the
        // queue behind it, and dropped the overflow — losing changes in the mode
        // documented as the one that cannot lose them.
        match spec.delivery {
            spicepod::drasi::DrasiDelivery::Acknowledged => {
                on_delivery_error(spec.on_delivery_error)
            }
            spicepod::drasi::DrasiDelivery::Queued => QUEUED_SINK_POLICY,
        },
        spec.params.as_ref(),
    )?;

    Ok(match spec.delivery {
        spicepod::drasi::DrasiDelivery::Acknowledged => DeliveryMode::Acknowledged(sink),
        spicepod::drasi::DrasiDelivery::Queued => {
            let store = open_dead_letter_store(&name).await;
            DeliveryMode::Queued(Arc::new(DeliveryQueue::spawn(
                sink,
                name,
                DEFAULT_QUEUE_DEPTH,
                store,
            )))
        }
    })
}

/// Opens the durable store that retains what Drasi will not accept.
///
/// A store that cannot be opened is reported and skipped rather than failing the
/// component: forwarding without durable retry is a degraded mode, but refusing
/// to start the dataset over it would be worse.
pub(crate) async fn open_dead_letter_store(component: &str) -> Option<Arc<DeadLetterStore>> {
    match DeadLetterStore::open(
        store_path(component),
        component.to_string(),
        DEFAULT_MAX_BATCHES,
    )
    .await
    {
        Ok(store) => Some(Arc::new(store)),
        Err(e) => {
            tracing::warn!(
                "Drasi changes for {component} will not be retained for redelivery: {e}"
            );
            None
        }
    }
}

/// How a dataset's changes reach Drasi relative to its replication position.
#[derive(Debug, Clone)]
pub(crate) enum DeliveryMode {
    /// Deliver before the change envelope is passed on, so the source's
    /// replication position is only acknowledged once Drasi has the change.
    Acknowledged(Arc<DrasiSink>),
    /// Hand the change to a local queue and pass the envelope on immediately.
    /// Replication never waits for Drasi.
    Queued(Arc<DeliveryQueue>),
}

/// Builds a sink for one forwarded component — a dataset or a runtime table.
///
/// `component` names it in log lines and error messages, and prefixes the Drasi
/// element ids when `labels` is empty.
///
/// # Errors
///
/// Returns an error if a required transport parameter is missing or unusable.
pub(crate) fn build_sink(
    component: String,
    source_id: &str,
    labels: Vec<String>,
    transport: DrasiTransportSpec,
    on_delivery_error: OnDeliveryError,
    params: Option<&spicepod::param::Params>,
) -> runtime_drasi::Result<Arc<DrasiSink>> {
    let params = params
        .map(spicepod::param::Params::as_string_map)
        .unwrap_or_default();

    let transport = match transport {
        DrasiTransportSpec::Http => TransportConfig::Http {
            endpoint: parse_http_endpoint(&component, &params)?,
            request_timeout: request_timeout_or(&component, &params, DEFAULT_REQUEST_TIMEOUT)?,
        },
        DrasiTransportSpec::Redis => TransportConfig::Redis {
            url: required_param(&component, &params, REDIS_URL_PARAM)?.to_string(),
            stream_key: required_param(&component, &params, STREAM_KEY_PARAM)?.to_string(),
        },
    };

    Ok(Arc::new(DrasiSink::try_new(DrasiSinkConfig {
        dataset: component.clone(),
        source_id: source_id.to_string(),
        mapping: ElementMapping::new(component, labels),
        transport,
        on_delivery_error,
    })?))
}

/// The node labels for `dataset`.
///
/// Defaults to the source table name, which is what Drasi's own relational
/// sources label rows with — so a continuous query written against a Drasi
/// `PostgreSQL` source matches rows forwarded from here without a rewrite.
fn labels_for(dataset: &Dataset, spec: &DrasiSpec) -> Vec<String> {
    if !spec.labels.is_empty() {
        return spec.labels.clone();
    }

    // `path()` is the authoritative `from:` split — it takes the *earliest* of
    // `://`, `:` and `/` as the delimiter, which a hand-rolled split in
    // preference order gets wrong for a value like `file/path:name`.
    vec![dataset.path().to_string()]
}

pub(crate) fn on_delivery_error(spec: spicepod::drasi::OnDeliveryError) -> OnDeliveryError {
    match spec {
        spicepod::drasi::OnDeliveryError::Block => OnDeliveryError::Block,
        spicepod::drasi::OnDeliveryError::Skip => OnDeliveryError::Skip,
        spicepod::drasi::OnDeliveryError::Fail => OnDeliveryError::Fail,
    }
}

fn required_param<'a>(
    dataset: &str,
    params: &'a std::collections::HashMap<String, String>,
    name: &'static str,
) -> runtime_drasi::Result<&'a str> {
    params
        .get(name)
        .map(String::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| runtime_drasi::Error::MissingParameter {
            dataset: dataset.to_string(),
            parameter: name,
        })
}

/// Parses the configured HTTP endpoint.
///
/// # Errors
///
/// Returns an error if the parameter is absent, empty, or not a URL.
fn parse_http_endpoint(
    component: &str,
    params: &std::collections::HashMap<String, String>,
) -> runtime_drasi::Result<url::Url> {
    let raw = required_param(component, params, HTTP_ENDPOINT_PARAM)?;
    url::Url::parse(raw).map_err(|source| runtime_drasi::Error::InvalidUrl {
        dataset: component.to_string(),
        parameter: HTTP_ENDPOINT_PARAM,
        // The value failed to parse but may still carry `user:pass@`, and this
        // error is logged and surfaced as the component's status.
        value: runtime_drasi::redact_url(raw),
        source,
    })
}

/// Parses the configured request timeout, or `default` when unset.
///
/// # Errors
///
/// Returns an error if the parameter is not a duration.
fn request_timeout_or(
    dataset: &str,
    params: &std::collections::HashMap<String, String>,
    default: Duration,
) -> runtime_drasi::Result<Duration> {
    let Some(raw) = params.get(REQUEST_TIMEOUT_PARAM) else {
        return Ok(default);
    };

    fundu::parse_duration(raw).map_err(|e| runtime_drasi::Error::InvalidConfiguration {
        dataset: dataset.to_string(),
        message: format!(
            "Parameter '{REQUEST_TIMEOUT_PARAM}' is not a valid duration ('{raw}'): {e}. \
            Expected a value like '30s' or '1m'."
        ),
    })
}

/// Borrows the per-row operation codes from a change batch's `op` column.
///
/// `changes_schema` declares `op` as a non-nullable `Utf8`, so anything else is
/// a corrupt batch rather than a condition to paper over.
fn op_codes(batch: &ChangeBatch) -> Result<Vec<&str>, StreamError> {
    let column = batch
        .record
        .column_by_name("op")
        .ok_or_else(|| StreamError::Arrow("change batch has no 'op' column".to_string()))?;

    let ops = column
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            StreamError::Arrow(format!(
                "change batch 'op' column is {:?}, expected Utf8",
                column.data_type()
            ))
        })?;

    Ok((0..ops.len()).map(|row| ops.value(row)).collect())
}

/// Borrows the per-row primary-key column names from a change batch.
///
/// [`ChangeBatch::primary_keys`] returns an owned `Vec<String>`, cloning every
/// key name — once per row. The names are read straight off the shared values
/// array here instead, so a batch costs one small `Vec` per row and no string
/// copies at all.
///
/// The list is nullable and its length varies per row (a delete under a partial
/// replica identity may carry none), so this stays per-row rather than reading
/// row 0 and assuming the rest match.
fn primary_key_columns(batch: &ChangeBatch) -> Result<Vec<Vec<&str>>, StreamError> {
    let column = batch.record.column_by_name("primary_keys").ok_or_else(|| {
        StreamError::Arrow("change batch has no 'primary_keys' column".to_string())
    })?;

    let lists = column.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
        StreamError::Arrow(format!(
            "change batch 'primary_keys' column is {:?}, expected List<Utf8>",
            column.data_type()
        ))
    })?;

    let names = lists
        .values()
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            StreamError::Arrow(format!(
                "change batch 'primary_keys' values are {:?}, expected Utf8",
                lists.values().data_type()
            ))
        })?;

    let offsets = lists.offsets();
    Ok((0..lists.len())
        .map(|row| {
            if lists.is_null(row) {
                return Vec::new();
            }
            // Offsets index the shared values array, so a `&str` here borrows
            // from the batch rather than allocating.
            let start = offsets[row].as_usize();
            let end = offsets[row + 1].as_usize();
            (start..end).map(|i| names.value(i)).collect()
        })
        .collect())
}

/// Forwards one change envelope to Drasi, then passes it through unchanged.
///
/// Under [`DeliveryMode::Acknowledged`] this runs before the envelope is
/// committed, so the source's replication position is only acknowledged after
/// Drasi has the change — a stall or a crash replays it rather than losing it,
/// at the cost of pacing replication at Drasi's speed. Under
/// [`DeliveryMode::Queued`] the change is handed to a local queue and the
/// envelope passed on immediately, so replication never waits.
pub(crate) async fn forward_change_envelope(
    maybe_envelope: Result<ChangeEnvelope, StreamError>,
    delivery: DeliveryMode,
) -> Result<ChangeEnvelope, StreamError> {
    let envelope = maybe_envelope?;

    // Skip heartbeats and readiness signals before materializing anything: they
    // carry no rows, and building their (empty) batch would be pure overhead on
    // an idle stream.
    if envelope.is_empty() {
        return Ok(envelope);
    }

    // Materialize a deferred batch here, off this async worker — the same
    // trade-off the index decorator makes, for the same reason: a large deferred
    // burst would otherwise stall the stream while it is built.
    let (committer, batch, is_dataset_ready) = envelope.into_parts_offloaded().await?;

    let data = batch.data_batch();

    // Scoped so the borrows of `batch` end before it is moved back into the
    // envelope below.
    {
        // Read the `op` column directly rather than going through
        // `ChangeBatch::op`, which maps an unrecognized code to
        // `ChangeOperation::Unknown` and renders it as `Unknown(x)` — the raw
        // code is what an "unsupported operation" error needs to name.
        let op_codes = op_codes(&batch)?;
        let primary_key_columns = primary_key_columns(&batch)?;

        match &delivery {
            DeliveryMode::Acknowledged(sink) => {
                let rows = DrasiChangeRows {
                    op_codes,
                    primary_key_columns,
                    data: &data,
                    source_commit_ts_ms: batch.source_commit_ts_ms(),
                };

                // Awaited, so the envelope — and with it the source's
                // replication position — is only passed on once Drasi has the
                // change.
                sink.forward(&rows)
                    .await
                    .map_err(|e| StreamError::External(e.to_string()))?;
            }
            DeliveryMode::Queued(queue) => {
                // Hand over owned copies and return: the replication position
                // advances without waiting for Drasi.
                queue
                    .enqueue(QueuedBatch {
                        op_codes: op_codes.into_iter().map(ToString::to_string).collect(),
                        primary_key_columns: primary_key_columns
                            .into_iter()
                            .map(|key| key.into_iter().map(ToString::to_string).collect())
                            .collect(),
                        data: data.clone(),
                        source_commit_ts_ms: batch.source_commit_ts_ms(),
                    })
                    .await;
            }
        }
    }

    // Re-wrap with the original committer: acknowledging the source's position
    // stays the accelerator's job, and it must happen after the accelerator
    // applies the change, not after Drasi accepts it.
    Ok(ChangeEnvelope::new(committer, batch, is_dataset_ready))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_required_param_names_the_parameter() {
        let params = std::collections::HashMap::new();
        let err = required_param("orders", &params, HTTP_ENDPOINT_PARAM)
            .expect_err("the endpoint is required for the http transport");

        let message = err.to_string();
        assert!(message.contains("drasi_http_endpoint"), "{message}");
        assert!(message.contains("orders"), "{message}");
    }

    /// An empty value is as unusable as an absent one, and would otherwise reach
    /// URL parsing as a confusing secondary error.
    #[test]
    fn empty_required_param_is_treated_as_missing() {
        let params =
            std::collections::HashMap::from([(HTTP_ENDPOINT_PARAM.to_string(), String::new())]);
        required_param("orders", &params, HTTP_ENDPOINT_PARAM)
            .expect_err("an empty endpoint is not a usable endpoint");
    }

    #[test]
    fn request_timeout_defaults_and_parses() {
        let empty = std::collections::HashMap::new();
        assert_eq!(
            request_timeout_or("orders", &empty, DEFAULT_REQUEST_TIMEOUT).expect("defaults"),
            DEFAULT_REQUEST_TIMEOUT
        );

        let set = std::collections::HashMap::from([(
            REQUEST_TIMEOUT_PARAM.to_string(),
            "5s".to_string(),
        )]);
        assert_eq!(
            request_timeout_or("orders", &set, DEFAULT_REQUEST_TIMEOUT).expect("parses"),
            Duration::from_secs(5)
        );

        let bad = std::collections::HashMap::from([(
            REQUEST_TIMEOUT_PARAM.to_string(),
            "soon".to_string(),
        )]);
        request_timeout_or("orders", &bad, DEFAULT_REQUEST_TIMEOUT)
            .expect_err("an unparseable duration is rejected");
    }
}
