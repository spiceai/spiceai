/*
Copyright 2024-2025 The Spice.ai OSS Authors

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

//! The `spice.ai` connector's append stream: a `DoExchange` subscription to
//! an upstream dataset, read into change envelopes for the acceleration.
//!
//! A producer that marks its snapshot (see [`crate::flight::append_stream`])
//! keeps the dataset not-ready until the snapshot's `end`, and its updates are
//! skipped when the snapshot already holds their commit. A producer that does
//! not mark its snapshot is read as before: every batch is ready.

use std::fmt::Display;
use std::future::Future;
use std::time::Duration;

use arrow::datatypes::SchemaRef;
use arrow_flight::decode::{DecodedFlightData, DecodedPayload};
use arrow_flight::error::FlightError;
use arrow_tools::map_entries::StreamNormalizer;
use async_stream::stream;
use data_components::cdc::{self, ChangeBatch, ChangeEnvelope};
use futures::{Stream, StreamExt};

use super::SpiceAIChangeCommiter;
use crate::flight::append_stream::{Step, StreamMetadata, Subscription};

/// Most times an append stream resubscribes in a row before it gives up.
const MAX_APPEND_STREAM_RESUBSCRIBES: u32 = 10;

/// The longest wait between two resubscribes.
const MAX_APPEND_STREAM_RESUBSCRIBE_BACKOFF: Duration = Duration::from_secs(30);

/// A subscription that lasted this long counts as healthy, so the next failure
/// starts the resubscribe budget again.
const APPEND_STREAM_HEALTHY_SUBSCRIPTION: Duration = Duration::from_secs(60);

/// How long a marked snapshot may load before its missing `end` is logged as
/// an error. The dataset stays not-ready either way.
const SNAPSHOT_END_ERROR_AFTER: Duration = Duration::from_secs(300);

/// Whether the upstream ended a subscription because the subscriber missed
/// updates. A new subscription starts with a truncate and a full snapshot, so
/// resubscribing reconciles the acceleration.
pub(super) fn is_resubscribable(error: &FlightError) -> bool {
    matches!(error, FlightError::Tonic(status) if status.code() == tonic::Code::DataLoss)
}

fn resubscribe_backoff(attempt: u32) -> Duration {
    Duration::from_millis(250)
        .saturating_mul(2_u32.saturating_pow(attempt.saturating_sub(1)))
        .min(MAX_APPEND_STREAM_RESUBSCRIBE_BACKOFF)
}

/// Why a subscription ended.
enum Ended {
    /// The stream ended or failed in a way that does not call for another
    /// subscription.
    Done,
    /// Take a fresh snapshot.
    Resubscribe(String),
}

/// Reads the append stream of `table_reference`, opening each subscription with
/// `subscribe`.
pub(super) fn append_stream<F, Fut, S, E>(
    mut subscribe: F,
    table_reference: String,
) -> impl Stream<Item = Result<ChangeEnvelope, cdc::StreamError>>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<S, E>>,
    E: Display,
    S: Stream<Item = Result<DecodedFlightData, FlightError>> + Unpin,
{
    stream! {
        let mut resubscribes = 0_u32;
        let mut last_epoch: Option<String> = None;
        'subscription: loop {
            // The subscription carries one schema per stream, so the normalizer is resolved from the
            // first batch and reused for the rest. Without it an accelerated dataset that started on
            // the conformed scan schema would ingest the producer's non-conforming MAP declaration.
            let mut normalizer = StreamNormalizer::new();
            let mut subscription = Subscription::default();
            let mut data_schema: Option<SchemaRef> = None;
            let subscribed_at = tokio::time::Instant::now();
            let mut stream = match subscribe().await {
                Ok(stream) => stream,
                Err(e) if resubscribes > 0 && resubscribes < MAX_APPEND_STREAM_RESUBSCRIBES => {
                    resubscribes += 1;
                    tracing::warn!(
                        "The append stream for dataset '{table_reference}' could not resubscribe to its source ({e}); retrying (attempt {resubscribes} of {MAX_APPEND_STREAM_RESUBSCRIBES})."
                    );
                    tokio::time::sleep(resubscribe_backoff(resubscribes)).await;
                    continue 'subscription;
                }
                Err(e) => {
                    yield Err(cdc::StreamError::Flight(e.to_string()));
                    break 'subscription;
                }
            };
            let snapshot_end_error_at = subscribed_at + SNAPSHOT_END_ERROR_AFTER;
            let mut snapshot_end_error_logged = false;

            let ended = loop {
                let next = if subscription.awaiting_end() && !snapshot_end_error_logged {
                    tokio::select! {
                        next = stream.next() => next,
                        () = tokio::time::sleep_until(snapshot_end_error_at) => {
                            snapshot_end_error_logged = true;
                            tracing::error!(
                                dataset = %table_reference,
                                "The append stream for dataset '{table_reference}' has not received the end of its initial snapshot after {}s, so the dataset stays not ready. Check that the source runtime is healthy and can read the dataset.",
                                SNAPSHOT_END_ERROR_AFTER.as_secs()
                            );
                            continue;
                        }
                    }
                } else {
                    stream.next().await
                };

                let Some(decoded_data) = next else {
                    break if subscription.epoch().is_some() {
                        Ended::Resubscribe("the source ended the stream".to_string())
                    } else {
                        Ended::Done
                    };
                };
                let decoded_data = match decoded_data {
                    Ok(decoded_data) => decoded_data,
                    Err(e) if is_resubscribable(&e) => {
                        break Ended::Resubscribe(format!("it missed updates from its source ({e})"));
                    }
                    // A producer that marks its stream restarts it from a fresh
                    // snapshot, so a failed stream is resubscribed; one that does
                    // not keeps the old behavior.
                    Err(e) if subscription.epoch().is_some() => {
                        break Ended::Resubscribe(format!("the stream from its source failed ({e})"));
                    }
                    Err(e) => {
                        yield Err(cdc::StreamError::Flight(e.to_string()));
                        continue;
                    }
                };
                let metadata = StreamMetadata::parse(&decoded_data.inner.app_metadata);
                let batch = match decoded_data.payload {
                    DecodedPayload::None | DecodedPayload::Schema(_) => {
                        match subscription.on_marker(metadata) {
                            Step::SnapshotComplete => {
                                let Some(schema) = data_schema.as_ref() else {
                                    continue;
                                };
                                match cdc::build_ready_signal_envelope(schema) {
                                    Ok(envelope) => yield Ok(envelope),
                                    Err(e) => yield Err(cdc::StreamError::SerdeJsonError(e.to_string())),
                                }
                            }
                            Step::Resnapshot(reason) => break Ended::Resubscribe(reason),
                            Step::Apply { .. } | Step::Skip | Step::Ignore => {}
                        }
                        continue;
                    }
                    DecodedPayload::RecordBatch(batch) => batch,
                };
                let ready = match subscription.on_batch(metadata) {
                    Step::Apply { ready } => ready,
                    Step::Resnapshot(reason) => break Ended::Resubscribe(reason),
                    Step::Skip | Step::Ignore | Step::SnapshotComplete => continue,
                };
                if let Some(epoch) = subscription.epoch()
                    && last_epoch.as_deref() != Some(epoch)
                {
                    if last_epoch.is_some() {
                        tracing::info!(
                            "The source of dataset '{table_reference}' restarted, so the dataset is reloading from the source's new snapshot."
                        );
                    }
                    last_epoch = Some(epoch.to_string());
                }
                let batch = match normalizer.normalize(batch) {
                    Ok(batch) => batch,
                    Err(source) => {
                        yield Err(cdc::StreamError::Arrow(format!(
                            "Failed to read the change stream from Arrow Flight for dataset '{table_reference}' ({source}), so the dataset stops receiving updates. Remove the null map entries at the source, or expose the column as a string with `to_json(<column>)`. See: https://spiceai.org/docs/components/data-connectors"
                        )));
                        // End the subscription rather than resuming it. The CDC
                        // apply loop keeps running after a fatal stream error, so
                        // a resumed subscription would apply every later envelope
                        // on top of the change this one dropped and leave the
                        // acceleration permanently diverged from its source.
                        break 'subscription;
                    }
                };
                match ChangeBatch::try_new(batch) {
                    Ok(change_batch) => {
                        data_schema.get_or_insert_with(|| change_batch.data_schema());
                        yield Ok(ChangeEnvelope::new(
                            Box::new(SpiceAIChangeCommiter {}),
                            change_batch,
                            ready,
                        ));
                    }
                    Err(e) => yield Err(cdc::StreamError::SerdeJsonError(e.to_string())),
                }
            };

            let reason = match ended {
                Ended::Done => break 'subscription,
                Ended::Resubscribe(reason) => reason,
            };
            if subscription.awaiting_end() {
                tracing::error!(
                    dataset = %table_reference,
                    "The append stream for dataset '{table_reference}' ended before its initial snapshot was complete ({reason}), so the dataset stays not ready while it resubscribes."
                );
            }
            if subscribed_at.elapsed() >= APPEND_STREAM_HEALTHY_SUBSCRIPTION {
                resubscribes = 0;
            }
            if resubscribes >= MAX_APPEND_STREAM_RESUBSCRIBES {
                yield Err(cdc::StreamError::Flight(format!(
                    "The append stream for dataset '{table_reference}' gave up after {MAX_APPEND_STREAM_RESUBSCRIBES} resubscribes: {reason}"
                )));
                break 'subscription;
            }
            resubscribes += 1;
            // A new subscription opens with a truncate and the full snapshot,
            // so applying it reconciles whatever this one missed.
            tracing::warn!(
                "The append stream for dataset '{table_reference}' is resubscribing to reload the dataset from a fresh snapshot, because {reason} (attempt {resubscribes} of {MAX_APPEND_STREAM_RESUBSCRIBES})."
            );
            tokio::time::sleep(resubscribe_backoff(resubscribes)).await;
        }
    }
}
