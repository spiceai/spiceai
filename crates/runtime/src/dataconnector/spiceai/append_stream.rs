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

use std::collections::HashMap;
use std::fmt::Display;
use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::Duration;

use arrow::datatypes::SchemaRef;
use arrow_flight::decode::{DecodedFlightData, DecodedPayload};
use arrow_flight::error::FlightError;
use arrow_tools::map_entries::StreamNormalizer;
use async_stream::stream;
use data_components::cdc::{self, ChangeBatch, ChangeEnvelope};
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryPool, MemoryReservation};
use futures::{Stream, StreamExt};
use opentelemetry::KeyValue;
use opentelemetry::metrics::{ObservableCounter, ObservableGauge};

use super::SpiceAIChangeCommiter;
use crate::flight::append_stream::{Step, StreamMarker, StreamMetadata, Subscription};

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

/// The first wait before retrying a reload that was refused for lack of memory.
const REFUSED_RELOAD_FIRST_RETRY: Duration = Duration::from_secs(30);

/// The longest wait before retrying a reload that was refused for lack of memory.
const REFUSED_RELOAD_MAX_RETRY: Duration = Duration::from_mins(15);

fn refused_reload_backoff(refusals: u32) -> Duration {
    REFUSED_RELOAD_FIRST_RETRY
        .saturating_mul(2_u32.saturating_pow(refusals.saturating_sub(1)))
        .min(REFUSED_RELOAD_MAX_RETRY)
}

/// What a reload may use: memory reserved from the runtime's query memory
/// pool, and where to report a dataset left serving stale data.
#[derive(Clone)]
pub(super) struct ReloadBudget {
    memory_pool: Arc<dyn MemoryPool>,
    stale: Arc<StaleDatasets>,
}

impl ReloadBudget {
    /// A budget drawn from `memory_pool` that reports staleness through the
    /// `dataset_append_stream_stale_seconds` and
    /// `dataset_append_stream_reloads_refused` metrics.
    pub(super) fn new(memory_pool: Arc<dyn MemoryPool>) -> Self {
        Self {
            memory_pool,
            stale: StaleDatasets::published(),
        }
    }
}

/// The datasets whose append stream keeps serving data older than its source,
/// because a reload was refused or discarded, and how often each reload was
/// refused.
///
/// A dataset can have more than one stream running, such as the outgoing and
/// the incoming stream of a Spicepod reload, so each stream's staleness is
/// tracked on its own: one stream's reload cannot report another's current,
/// and a stream that stops takes its staleness with it.
#[derive(Default)]
pub(super) struct StaleDatasets {
    datasets: parking_lot::Mutex<HashMap<String, Staleness>>,
    /// The id the next registered stream gets.
    next_stream: AtomicU64,
}

#[derive(Default)]
struct Staleness {
    /// The append streams of the dataset that are running, each with when the
    /// data it serves last changed while a reload of it is outstanding (`None`
    /// while it is current). The dataset is reported, at 0 while every stream
    /// is current, for as long as one runs.
    streams: HashMap<u64, Option<tokio::time::Instant>>,
    /// Reloads refused because staging them needed more memory than the pool had.
    refused: u64,
}

impl Staleness {
    /// When the data of the dataset's stalest stream last changed.
    fn stale_since(&self) -> Option<tokio::time::Instant> {
        self.streams.values().flatten().min().copied()
    }
}

/// The process-wide [`StaleDatasets`] and the instruments that publish it.
static PUBLISHED_STALE_DATASETS: LazyLock<(
    Arc<StaleDatasets>,
    ObservableGauge<u64>,
    ObservableCounter<u64>,
)> = LazyLock::new(|| {
    let stale = Arc::new(StaleDatasets::default());
    let meter = &runtime_metrics::datasets::DATASETS_METER;
    let gauge = meter
        .u64_observable_gauge("dataset_append_stream_stale_seconds")
        .with_description(
            "Seconds since a spice.ai append-stream dataset last changed, while a reload of it was refused or discarded and it keeps serving its previous data; 0 once a reload succeeds.",
        )
        .with_unit("s")
        .with_callback({
            let stale = Arc::clone(&stale);
            move |observer| {
                for (dataset, seconds) in stale.stale_seconds() {
                    observer.observe(seconds, &[KeyValue::new("dataset", dataset)]);
                }
            }
        })
        .build();
    let refused = meter
        .u64_observable_counter("dataset_append_stream_reloads_refused")
        .with_description(
            "Reloads of a spice.ai append-stream dataset refused because staging them needed more memory than the query memory pool had free.",
        )
        .with_callback({
            let stale = Arc::clone(&stale);
            move |observer| {
                for (dataset, refused) in stale.refusals() {
                    observer.observe(refused, &[KeyValue::new("dataset", dataset)]);
                }
            }
        })
        .build();
    (stale, gauge, refused)
});

impl StaleDatasets {
    fn published() -> Arc<Self> {
        Arc::clone(&PUBLISHED_STALE_DATASETS.0)
    }

    /// Reports a new append stream of `dataset`, current, until the returned
    /// guard drops, so the dataset's metrics read 0 from the moment its first
    /// stream starts rather than appearing only once it goes stale.
    fn register(self: &Arc<Self>, dataset: &str) -> Registration {
        let stream = self.next_stream.fetch_add(1, Ordering::Relaxed);
        self.datasets
            .lock()
            .entry(dataset.to_string())
            .or_default()
            .streams
            .insert(stream, None);
        Registration(StreamStaleness {
            stale: Arc::clone(self),
            dataset: dataset.to_string(),
            stream,
        })
    }

    /// Whether `dataset` is reported at all.
    #[cfg(test)]
    pub(super) fn is_reported(&self, dataset: &str) -> bool {
        self.datasets.lock().contains_key(dataset)
    }

    /// How long ago `dataset` last changed, while it is stale.
    #[cfg(test)]
    pub(super) fn stale_for(&self, dataset: &str) -> Option<Duration> {
        self.datasets
            .lock()
            .get(dataset)
            .and_then(Staleness::stale_since)
            .map(|since| since.elapsed())
    }

    /// How many reloads of `dataset` were refused.
    #[cfg(test)]
    pub(super) fn refused(&self, dataset: &str) -> u64 {
        self.datasets
            .lock()
            .get(dataset)
            .map_or(0, |entry| entry.refused)
    }

    fn stale_seconds(&self) -> Vec<(String, u64)> {
        self.datasets
            .lock()
            .iter()
            .map(|(dataset, entry)| {
                (
                    dataset.clone(),
                    entry
                        .stale_since()
                        .map_or(0, |since| since.elapsed().as_secs()),
                )
            })
            .collect()
    }

    fn refusals(&self) -> Vec<(String, u64)> {
        self.datasets
            .lock()
            .iter()
            .map(|(dataset, entry)| (dataset.clone(), entry.refused))
            .collect()
    }
}

/// One append stream's entry in [`StaleDatasets`].
#[derive(Clone)]
struct StreamStaleness {
    stale: Arc<StaleDatasets>,
    dataset: String,
    stream: u64,
}

impl StreamStaleness {
    /// The stream keeps serving data that last changed at `last_changed`, or
    /// now when it never changed. A stream that has stopped is not reported
    /// again.
    fn mark_stale(&self, last_changed: Option<tokio::time::Instant>, refused: bool) {
        let mut datasets = self.stale.datasets.lock();
        let Some(entry) = datasets.get_mut(&self.dataset) else {
            return;
        };
        if refused {
            entry.refused += 1;
        }
        if let Some(stale_since) = entry.streams.get_mut(&self.stream) {
            stale_since
                .get_or_insert_with(|| last_changed.unwrap_or_else(tokio::time::Instant::now));
        }
    }

    /// The stream was reloaded and is current again.
    fn mark_current(&self) {
        if let Some(stale_since) = self
            .stale
            .datasets
            .lock()
            .get_mut(&self.dataset)
            .and_then(|entry| entry.streams.get_mut(&self.stream))
        {
            *stale_since = None;
        }
    }
}

/// Keeps an append stream reported in [`StaleDatasets`] while it runs, and its
/// dataset for as long as one of its streams does.
struct Registration(StreamStaleness);

impl Registration {
    fn stream(&self) -> &StreamStaleness {
        &self.0
    }
}

impl Drop for Registration {
    fn drop(&mut self) {
        let StreamStaleness {
            stale,
            dataset,
            stream,
        } = &self.0;
        let mut datasets = stale.datasets.lock();
        if let Some(entry) = datasets.get_mut(dataset) {
            entry.streams.remove(stream);
            if entry.streams.is_empty() {
                datasets.remove(dataset);
            }
        }
    }
}

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

/// A reload of a dataset that is already ready: the new subscription's
/// snapshot, and any update that arrives before its end, are staged here rather
/// than applied, and are swapped in at the snapshot's end as one atomic
/// overwrite. Queries keep seeing the previous data until then, and a reload
/// that does not reach its end is discarded.
///
/// The staged rows are reserved from the runtime's query memory pool, twice
/// over: once as staged and once for the copy that concatenates them at the
/// swap. A reload the pool cannot hold is refused.
struct Reload {
    /// Each staged batch with the commit sequence number its update carried;
    /// `None` for a snapshot batch.
    staged: Vec<(Option<u64>, ChangeBatch)>,
    reservation: MemoryReservation,
}

impl Reload {
    fn new(memory_pool: &Arc<dyn MemoryPool>, dataset: &str) -> Self {
        Self {
            staged: Vec::new(),
            reservation: MemoryConsumer::new(format!("append_stream_reload[{dataset}]"))
                .register(memory_pool),
        }
    }

    /// Stages `batch`, or refuses it when the pool cannot hold it and its copy.
    fn stage(
        &mut self,
        sequence: Option<u64>,
        batch: ChangeBatch,
    ) -> Result<(), datafusion::error::DataFusionError> {
        self.reservation
            .try_grow(batch.record.get_array_memory_size().saturating_mul(2))?;
        self.staged.push((sequence, batch));
        Ok(())
    }

    /// The overwrite that replaces the dataset with the staged rows. An update
    /// whose commit the snapshot holds (`sequence <= snapshot_sequence`) does
    /// not survive, and a truncate drops what was staged before it. `stream`
    /// is reported current once the replacement commits, or stale since
    /// `last_changed`, when the dataset last changed, if it is dropped first.
    fn into_replacement(
        self,
        snapshot_sequence: u64,
        data_schema: &SchemaRef,
        stream: &StreamStaleness,
        last_changed: Option<tokio::time::Instant>,
    ) -> Result<ChangeEnvelope, String> {
        let mut rows: Vec<arrow::array::RecordBatch> = Vec::new();
        for (sequence, batch) in self.staged {
            if sequence.is_some_and(|sequence| sequence <= snapshot_sequence) {
                continue;
            }
            if batch.is_heartbeat() {
                continue;
            }
            if matches!(batch.op(0), cdc::ChangeOperation::Truncate) {
                rows.clear();
                continue;
            }
            rows.push(batch.data_batch());
        }
        let schema = rows.first().map_or_else(
            || Arc::clone(data_schema),
            arrow::array::RecordBatch::schema,
        );
        let data = arrow::compute::concat_batches(&schema, &rows).map_err(|e| e.to_string())?;
        drop(rows);
        let batch = cdc::wrap_data_as_change_batch(&schema, &data)
            .map_err(|e| e.to_string())?
            .with_rebuild_from_this_batch(true);
        // The staged batches are freed; the replacement stays charged to the
        // pool until the apply loop commits or drops it.
        let reservation = self.reservation;
        reservation.resize(batch.record.get_array_memory_size());
        Ok(ChangeEnvelope::from_parts(
            Box::new(ReplacementCommitter {
                _reservation: reservation,
                stream: stream.clone(),
                last_changed,
                committed: AtomicBool::new(false),
            }),
            batch,
            true,
            true,
        ))
    }
}

/// The committer of a reload's replacement: it holds the replacement's memory
/// reservation for as long as the envelope lives, and reports its stream
/// current only once the apply loop has written the replacement. A replacement
/// that fails to apply or is dropped reports its stream stale since the dataset
/// last changed before the reload.
struct ReplacementCommitter {
    _reservation: MemoryReservation,
    stream: StreamStaleness,
    /// When the dataset last changed before this reload.
    last_changed: Option<tokio::time::Instant>,
    committed: AtomicBool,
}

#[async_trait::async_trait]
impl cdc::CommitChange for ReplacementCommitter {
    async fn commit(&self) -> Result<(), cdc::CommitError> {
        self.stream.mark_current();
        self.committed.store(true, Ordering::Release);
        Ok(())
    }
}

impl Drop for ReplacementCommitter {
    fn drop(&mut self) {
        if !*self.committed.get_mut() {
            self.stream.mark_stale(self.last_changed, false);
        }
    }
}

/// Why a subscription ended.
enum Ended {
    /// The stream ended or failed in a way that does not call for another
    /// subscription.
    Done,
    /// Take a fresh snapshot.
    Resubscribe(String),
    /// A reload needed more memory than the pool had; retry it later.
    Refused(String),
}

/// Reads the append stream of the dataset named `table_reference` (unquoted, as
/// it appears in logs and metric labels), opening each subscription with
/// `subscribe`.
pub(super) fn append_stream<F, Fut, S, E>(
    mut subscribe: F,
    table_reference: String,
    budget: ReloadBudget,
) -> impl Stream<Item = Result<ChangeEnvelope, cdc::StreamError>>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<S, E>>,
    E: Display,
    S: Stream<Item = Result<DecodedFlightData, FlightError>> + Unpin,
{
    stream! {
        // Report the dataset's staleness metrics, at 0, for as long as the
        // stream runs.
        let registered = budget.stale.register(&table_reference);
        let mut resubscribes = 0_u32;
        // Reloads refused in a row for lack of memory.
        let mut refusals = 0_u32;
        let mut last_epoch: Option<String> = None;
        // Whether the producer marks its stream: set once a subscription
        // carries an epoch, and cleared when one applies data without it. A
        // reconnect can end before its `begin` marker, so it carries no epoch
        // of its own; it is still resubscribed rather than treated as the end
        // of an unmarked producer's stream.
        let mut marked_producer = false;
        // Whether a snapshot of this stream has made the dataset ready, so a
        // later snapshot is a reload to stage and swap in.
        let mut been_ready = false;
        // When the dataset last took a change, to report how stale it is.
        let mut last_applied_at: Option<tokio::time::Instant> = None;
        'subscription: loop {
            // The subscription carries one schema per stream, so the normalizer is resolved from the
            // first batch and reused for the rest. Without it an accelerated dataset that started on
            // the conformed scan schema would ingest the producer's non-conforming MAP declaration.
            let mut normalizer = StreamNormalizer::new();
            let mut subscription = Subscription::default();
            let mut data_schema: Option<SchemaRef> = None;
            let mut reload: Option<Reload> = None;
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
            // A reload is discarded only when it stops making progress, so the
            // deadline moves with every message it receives; a large reload that
            // keeps delivering batches is never cut off.
            let mut snapshot_end_error_at = subscribed_at + SNAPSHOT_END_ERROR_AFTER;
            let mut snapshot_end_error_logged = false;
            // Whether this subscription completed a snapshot, the only thing that
            // makes it healthy enough to reset the retry budget for a marked
            // producer.
            let mut completed_snapshot = false;

            let ended = loop {
                let next = if subscription.awaiting_end() && !snapshot_end_error_logged {
                    tokio::select! {
                        next = stream.next() => next,
                        () = tokio::time::sleep_until(snapshot_end_error_at) => {
                            snapshot_end_error_logged = true;
                            if reload.is_some() {
                                break Ended::Resubscribe(format!(
                                    "its snapshot made no progress for {}s",
                                    SNAPSHOT_END_ERROR_AFTER.as_secs()
                                ));
                            }
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
                if reload.is_some() {
                    snapshot_end_error_at = tokio::time::Instant::now() + SNAPSHOT_END_ERROR_AFTER;
                }

                let marked = marked_producer || subscription.epoch().is_some();
                let Some(decoded_data) = next else {
                    break if marked {
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
                    Err(e) if marked => {
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
                                let snapshot_sequence = subscription.snapshot_sequence().unwrap_or(0);
                                let envelope = match reload.take() {
                                    Some(reload) => {
                                        let replacement = reload.into_replacement(
                                            snapshot_sequence,
                                            schema,
                                            registered.stream(),
                                            last_applied_at,
                                        );
                                        if replacement.is_ok() {
                                            refusals = 0;
                                        }
                                        replacement
                                    }
                                    None => cdc::build_ready_signal_envelope(schema).map_err(|e| e.to_string()),
                                };
                                match envelope {
                                    Ok(envelope) => {
                                        been_ready = true;
                                        completed_snapshot = true;
                                        last_applied_at = Some(tokio::time::Instant::now());
                                        yield Ok(envelope);
                                    }
                                    Err(e) => yield Err(cdc::StreamError::SerdeJsonError(e)),
                                }
                            }
                            Step::Resnapshot(reason) => break Ended::Resubscribe(reason),
                            Step::Apply { .. } | Step::Skip | Step::Ignore => {}
                        }
                        continue;
                    }
                    DecodedPayload::RecordBatch(batch) => batch,
                };
                let sequence = metadata.as_ref().and_then(|metadata| metadata.sequence);
                if been_ready
                    && metadata
                        .as_ref()
                        .is_some_and(|metadata| metadata.marker == StreamMarker::Begin)
                {
                    reload = Some(Reload::new(&budget.memory_pool, &table_reference));
                    snapshot_end_error_at = tokio::time::Instant::now() + SNAPSHOT_END_ERROR_AFTER;
                }
                let ready = match subscription.on_batch(metadata) {
                    Step::Apply { ready } => ready,
                    Step::Resnapshot(reason) => break Ended::Resubscribe(reason),
                    Step::Skip | Step::Ignore | Step::SnapshotComplete => continue,
                };
                match subscription.epoch() {
                    Some(epoch) if last_epoch.as_deref() != Some(epoch) => {
                        if last_epoch.is_some() {
                            tracing::info!(
                                "The source of dataset '{table_reference}' restarted, so the dataset is reloading from the source's new snapshot."
                            );
                        }
                        last_epoch = Some(epoch.to_string());
                    }
                    Some(_) => {}
                    // A producer that sends data without a `begin` does not
                    // mark its stream, such as one rolled back to a release
                    // without markers, so it is followed as an unmarked
                    // producer again.
                    None => marked_producer = false,
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
                        if let Some(reload) = reload.as_mut() {
                            if let Err(e) = reload.stage(sequence, change_batch) {
                                break Ended::Refused(e.to_string());
                            }
                            continue;
                        }
                        if ready {
                            been_ready = true;
                        }
                        last_applied_at = Some(tokio::time::Instant::now());
                        yield Ok(ChangeEnvelope::new(
                            Box::new(SpiceAIChangeCommiter {}),
                            change_batch,
                            ready,
                        ));
                    }
                    Err(e) => yield Err(cdc::StreamError::SerdeJsonError(e.to_string())),
                }
            };

            marked_producer |= subscription.epoch().is_some();
            let stale_for = last_applied_at.map_or_else(
                || "an unknown time".to_string(),
                |at| format!("{}s", at.elapsed().as_secs()),
            );
            let reason = match ended {
                Ended::Done => break 'subscription,
                Ended::Resubscribe(reason) => reason,
                Ended::Refused(reason) => {
                    // Dropping the staged reload returns its reservation.
                    drop(reload.take());
                    drop(stream);
                    registered.stream().mark_stale(last_applied_at, true);
                    refusals += 1;
                    let retry_in = refused_reload_backoff(refusals);
                    tracing::error!(
                        dataset = %table_reference,
                        "The append stream for dataset '{table_reference}' refused to reload from its source, because staging the reload needs more memory than the runtime's query memory pool has free ({reason}). The dataset keeps serving its previous data, which last changed {stale_for} ago; the reload is retried in {}s.",
                        retry_in.as_secs()
                    );
                    tokio::time::sleep(retry_in).await;
                    continue 'subscription;
                }
            };
            if reload.take().is_some() {
                registered.stream().mark_stale(last_applied_at, false);
                tracing::error!(
                    dataset = %table_reference,
                    "The append stream for dataset '{table_reference}' could not finish reloading from its source ({reason}), so the reload was discarded and the dataset keeps serving its previous data, which last changed {stale_for} ago."
                );
            } else if subscription.awaiting_end() {
                tracing::error!(
                    dataset = %table_reference,
                    "The append stream for dataset '{table_reference}' ended before its initial snapshot was complete ({reason}), so the dataset stays not ready while it resubscribes."
                );
            }
            // A marked producer's subscription counts as healthy only once it
            // completed its snapshot, so one that keeps failing to finish a
            // snapshot still exhausts the retry budget.
            if subscribed_at.elapsed() >= APPEND_STREAM_HEALTHY_SUBSCRIPTION
                && (completed_snapshot || !marked_producer)
            {
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

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::Arc;

    use arrow::array::{Int32Array, RecordBatch};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow_flight::FlightData;
    use arrow_flight::decode::FlightDataDecoder;
    use datafusion::common::TableReference;
    use datafusion::datasource::{MemTable, TableProvider};
    use datafusion::execution::memory_pool::{GreedyMemoryPool, UnboundedMemoryPool};
    use datafusion::prelude::SessionContext;
    use futures::stream::BoxStream;
    use futures::{StreamExt, TryStreamExt};

    use super::*;
    use crate::datafusion::DataFusion;
    use crate::dataupdate::{DataUpdate, DataUpdateBroadcaster, UpdateType};
    use crate::flight::append_stream::StreamMarker;
    use crate::flight::do_exchange::open_subscription;

    const DATASET: &str = "orders";

    type Messages = BoxStream<'static, Result<FlightData, FlightError>>;

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]))
    }

    fn batch(ids: Vec<i32>) -> RecordBatch {
        RecordBatch::try_new(schema(), vec![Arc::new(Int32Array::from(ids))]).expect("batch")
    }

    fn table(ids: Vec<i32>) -> Arc<MemTable> {
        Arc::new(MemTable::try_new(schema(), vec![vec![batch(ids)]]).expect("mem table"))
    }

    fn update(ids: Vec<i32>) -> DataUpdate {
        DataUpdate {
            schema: schema(),
            data: vec![batch(ids)],
            update_type: UpdateType::Append,
        }
    }

    /// The messages of a real `DoExchange` subscription to `table`.
    async fn producer(
        datafusion: &Arc<DataFusion>,
        broadcaster: &DataUpdateBroadcaster,
        table: Arc<MemTable>,
    ) -> Messages {
        open_subscription(
            broadcaster,
            datafusion,
            table as Arc<dyn TableProvider>,
            TableReference::bare(DATASET),
        )
        .await
        .expect("subscription opens")
        .map_err(|status| FlightError::Tonic(Box::new(status)))
        .boxed()
    }

    /// The messages up to and including the snapshot's end marker.
    async fn through_end(mut messages: Messages) -> (Vec<FlightData>, Messages) {
        let mut taken = Vec::new();
        while let Some(message) = messages.next().await {
            let message = message.expect("message");
            let end = StreamMetadata::parse(&message.app_metadata)
                .is_some_and(|metadata| metadata.marker == StreamMarker::End);
            taken.push(message);
            if end {
                return (taken, messages);
            }
        }
        panic!("the producer sent no end marker");
    }

    /// `messages`, then a stream that stays open.
    fn held_open(messages: Vec<FlightData>) -> Messages {
        futures::stream::iter(messages.into_iter().map(Ok))
            .chain(futures::stream::pending())
            .boxed()
    }

    /// The last record batch message of a snapshot: a snapshot row batch.
    fn row_message(snapshot: &[FlightData]) -> FlightData {
        snapshot[snapshot.len() - 2].clone()
    }

    /// The consumer, reading each subscription from the next of `subscriptions`.
    fn consumer(
        subscriptions: Vec<Messages>,
    ) -> BoxStream<'static, Result<ChangeEnvelope, cdc::StreamError>> {
        let mut subscriptions: VecDeque<Messages> = subscriptions.into();
        append_stream(
            move || {
                let next = subscriptions.pop_front();
                async move {
                    next.map(FlightDataDecoder::new)
                        .ok_or("no more subscriptions")
                }
            },
            DATASET.to_string(),
            unbounded_budget(),
        )
        .boxed()
    }

    /// A reload budget with no memory limit and its own staleness record.
    fn unbounded_budget() -> ReloadBudget {
        ReloadBudget {
            memory_pool: Arc::new(UnboundedMemoryPool::default()),
            stale: Arc::new(StaleDatasets::default()),
        }
    }

    /// One subscription's envelopes as the consumer yields them.
    #[derive(Debug, Default, PartialEq, Eq)]
    struct Snapshot {
        /// Rows applied before the dataset is ready, the truncate included.
        rows_before_ready: usize,
        /// Whether any of them was marked ready.
        any_ready_before: bool,
    }

    /// Reads envelopes until a ready envelope, for `snapshots` snapshots.
    async fn snapshots(
        stream: &mut BoxStream<'static, Result<ChangeEnvelope, cdc::StreamError>>,
        snapshots: usize,
    ) -> Vec<Snapshot> {
        tokio::time::timeout(Duration::from_secs(30), async {
            let mut read = Vec::new();
            let mut current = Snapshot::default();
            while read.len() < snapshots {
                let envelope = stream
                    .next()
                    .await
                    .expect("an envelope")
                    .expect("not an error");
                if envelope.is_heartbeat() && envelope.is_dataset_ready() {
                    read.push(std::mem::take(&mut current));
                    continue;
                }
                current.rows_before_ready += envelope.num_rows_hint();
                current.any_ready_before |= envelope.is_dataset_ready();
            }
            read
        })
        .await
        .expect("the snapshots are read")
    }

    /// The rows of the next envelope, once the snapshot is read.
    async fn next_rows(
        stream: &mut BoxStream<'static, Result<ChangeEnvelope, cdc::StreamError>>,
    ) -> Option<(usize, bool)> {
        tokio::time::timeout(Duration::from_secs(1), stream.next())
            .await
            .ok()
            .flatten()
            .map(|envelope| {
                let envelope = envelope.expect("not an error");
                (envelope.num_rows_hint(), envelope.is_dataset_ready())
            })
    }

    /// The ids an envelope carries, sorted.
    fn ids(envelope: &ChangeEnvelope) -> Vec<i32> {
        let rows = envelope.change_batch().expect("built").data_batch();
        let mut ids = rows
            .column_by_name("id")
            .expect("an id column")
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("id is Int32")
            .values()
            .to_vec();
        ids.sort_unstable();
        ids
    }

    /// The ids of the next envelope, once the snapshot is read, and whether it
    /// is marked ready.
    async fn next_ids(
        stream: &mut BoxStream<'static, Result<ChangeEnvelope, cdc::StreamError>>,
    ) -> Option<(Vec<i32>, bool)> {
        tokio::time::timeout(Duration::from_secs(1), stream.next())
            .await
            .ok()
            .flatten()
            .map(|envelope| {
                let envelope = envelope.expect("not an error");
                (ids(&envelope), envelope.is_dataset_ready())
            })
    }

    /// The producer's message for an update of `ids`, published now on its
    /// subscription's `published` stream, then tagged as the commit at
    /// `sequence` of `epoch` instead of the sequence it was published at.
    async fn published_update(
        broadcaster: &DataUpdateBroadcaster,
        published: &mut Messages,
        epoch: &str,
        sequence: u64,
        ids: Vec<i32>,
    ) -> FlightData {
        broadcaster
            .publish(&TableReference::bare(DATASET), update(ids))
            .await;
        let mut message = tokio::time::timeout(Duration::from_secs(5), published.next())
            .await
            .expect("the producer sends the update")
            .expect("a message")
            .expect("not an error");
        assert!(
            StreamMetadata::parse(&message.app_metadata)
                .is_some_and(|metadata| metadata.marker == StreamMarker::Update),
            "the producer's next message is the update"
        );
        message.app_metadata = StreamMetadata::update(epoch, sequence).to_bytes();
        message
    }

    /// The next envelope as a staged reload's replacement: its row count.
    async fn replacement(
        stream: &mut BoxStream<'static, Result<ChangeEnvelope, cdc::StreamError>>,
    ) -> usize {
        replacement_envelope(stream)
            .await
            .change_batch()
            .expect("built")
            .data_batch()
            .num_rows()
    }

    /// The next envelope, asserted to be a reload's atomic replacement.
    async fn replacement_envelope(
        stream: &mut BoxStream<'static, Result<ChangeEnvelope, cdc::StreamError>>,
    ) -> ChangeEnvelope {
        // Long enough for a reload to time out under paused time.
        let envelope = tokio::time::timeout(SNAPSHOT_END_ERROR_AFTER * 4, stream.next())
            .await
            .expect("a replacement")
            .expect("an envelope")
            .expect("not an error");
        assert!(
            envelope.history_unavailable() && envelope.is_dataset_ready(),
            "a reload is swapped in as one atomic replacement"
        );
        assert!(
            envelope
                .change_batch()
                .expect("built")
                .rebuild_from_this_batch()
        );
        envelope
    }

    /// The snapshot's sequence number, from its end marker.
    fn snapshot_sequence(snapshot: &[FlightData]) -> u64 {
        snapshot
            .last()
            .and_then(|message| StreamMetadata::parse(&message.app_metadata))
            .and_then(|metadata| metadata.sequence)
            .expect("an end marker with a sequence")
    }

    fn snapshot_of(rows: usize) -> Snapshot {
        // One row for the truncate.
        Snapshot {
            rows_before_ready: rows + 1,
            any_ready_before: false,
        }
    }

    #[tokio::test]
    async fn ready_waits_for_the_end_of_the_snapshot() {
        let runtime = crate::Runtime::builder().build().await;
        let broadcaster = DataUpdateBroadcaster::new();
        let (snapshot, _) =
            through_end(producer(&runtime.datafusion(), &broadcaster, table(vec![1, 2, 3])).await)
                .await;
        let mut stream = consumer(vec![held_open(snapshot)]);
        assert_eq!(snapshots(&mut stream, 1).await, vec![snapshot_of(3)]);
    }

    /// A dataset's staleness metrics read 0 from the moment its append stream
    /// starts, and it stops being reported once the stream is dropped.
    #[tokio::test]
    async fn the_staleness_metrics_report_a_running_stream_at_zero() {
        let runtime = crate::Runtime::builder().build().await;
        let broadcaster = DataUpdateBroadcaster::new();
        let (snapshot, _) =
            through_end(producer(&runtime.datafusion(), &broadcaster, table(vec![1])).await).await;
        let budget = unbounded_budget();
        let stale = Arc::clone(&budget.stale);
        let mut subscriptions: VecDeque<Messages> = vec![held_open(snapshot)].into();
        let mut stream = append_stream(
            move || {
                let next = subscriptions.pop_front();
                async move {
                    next.map(FlightDataDecoder::new)
                        .ok_or("no more subscriptions")
                }
            },
            DATASET.to_string(),
            budget,
        )
        .boxed();
        assert!(
            !stale.is_reported(DATASET),
            "nothing runs before the first poll"
        );
        assert_eq!(snapshots(&mut stream, 1).await, vec![snapshot_of(1)]);
        assert_eq!(stale.stale_seconds(), vec![(DATASET.to_string(), 0)]);
        assert_eq!(stale.refusals(), vec![(DATASET.to_string(), 0)]);
        drop(stream);
        assert!(
            !stale.is_reported(DATASET),
            "a dropped stream is no longer reported"
        );
    }

    /// Two streams of one dataset, such as across a Spicepod reload, keep it
    /// reported until both stop.
    #[test]
    fn a_dataset_stays_reported_until_its_last_stream_stops() {
        let stale = Arc::new(StaleDatasets::default());
        let first = stale.register(DATASET);
        let second = stale.register(DATASET);
        drop(first);
        assert!(stale.is_reported(DATASET));
        drop(second);
        assert!(!stale.is_reported(DATASET));
    }

    #[tokio::test]
    async fn a_producer_without_markers_is_ready_on_every_batch() {
        let runtime = crate::Runtime::builder().build().await;
        let broadcaster = DataUpdateBroadcaster::new();
        let (mut snapshot, _) =
            through_end(producer(&runtime.datafusion(), &broadcaster, table(vec![1, 2, 3])).await)
                .await;
        for message in &mut snapshot {
            message.app_metadata = bytes::Bytes::new();
        }
        let mut stream = consumer(vec![held_open(snapshot)]);
        assert_eq!(
            next_rows(&mut stream).await,
            Some((1, true)),
            "the truncate"
        );
        assert_eq!(next_rows(&mut stream).await, Some((3, true)), "the rows");
        assert_eq!(next_rows(&mut stream).await, None, "no ready marker");
    }

    /// A subscriber that arrives while a commit is between its write and its
    /// publish waits for the publish, then reads the rows once, in its
    /// snapshot.
    #[tokio::test]
    async fn a_subscriber_arriving_between_commit_and_publish_reads_the_rows_once() {
        let runtime = crate::Runtime::builder().build().await;
        let datafusion = runtime.datafusion();
        let broadcaster = DataUpdateBroadcaster::new();
        let path = TableReference::bare(DATASET);
        let accelerated = table(vec![1]);
        let _earlier = broadcaster.subscribe(&path).await;

        // The commit: its rows are written, not yet published.
        let guard = broadcaster.begin_commit(&path).await;
        let ctx = SessionContext::new();
        ctx.register_table(DATASET, Arc::clone(&accelerated) as Arc<dyn TableProvider>)
            .expect("register");
        ctx.sql("INSERT INTO orders VALUES (2)")
            .await
            .expect("plan insert")
            .collect()
            .await
            .expect("insert");

        let subscribing = tokio::spawn({
            let broadcaster = broadcaster.clone();
            let accelerated = Arc::clone(&accelerated);
            let datafusion = Arc::clone(&datafusion);
            async move { producer(&datafusion, &broadcaster, accelerated).await }
        });
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(
            !subscribing.is_finished(),
            "the subscriber waits for the publish"
        );
        broadcaster
            .publish_committed(&path, guard, update(vec![2]))
            .await;
        let (snapshot, mut messages) = through_end(subscribing.await.expect("subscriber")).await;

        // A later commit, which the subscriber is sent.
        broadcaster.publish(&path, update(vec![3])).await;
        let live = messages.next().await.expect("an update").expect("message");
        let mut replay = snapshot;
        replay.push(live);
        let mut stream = consumer(vec![held_open(replay)]);
        assert_eq!(
            snapshots(&mut stream, 1).await,
            vec![snapshot_of(2)],
            "the snapshot holds the commit"
        );
        assert_eq!(
            next_rows(&mut stream).await,
            Some((1, true)),
            "only the later commit follows"
        );
        assert_eq!(next_rows(&mut stream).await, None);
    }

    /// An update the snapshot already holds is skipped by its sequence number.
    #[tokio::test]
    async fn an_update_the_snapshot_holds_is_skipped() {
        let runtime = crate::Runtime::builder().build().await;
        let broadcaster = DataUpdateBroadcaster::new();
        let (mut snapshot, _) =
            through_end(producer(&runtime.datafusion(), &broadcaster, table(vec![1])).await).await;
        let row = row_message(&snapshot);
        let epoch = broadcaster.epoch().to_string();
        let with = |sequence| {
            let mut message = row.clone();
            message.app_metadata = StreamMetadata::update(&epoch, sequence).to_bytes();
            message
        };
        // The snapshot holds commit 0: a replay of it, then commit 1.
        snapshot.push(with(0));
        snapshot.push(with(1));
        let mut stream = consumer(vec![held_open(snapshot)]);
        assert_eq!(snapshots(&mut stream, 1).await, vec![snapshot_of(1)]);
        assert_eq!(next_rows(&mut stream).await, Some((1, true)), "commit 1");
        assert_eq!(next_rows(&mut stream).await, None, "commit 0 is skipped");
    }

    /// A producer that restarts has a new epoch: the consumer reloads from the
    /// new producer's snapshot, and does not compare sequence numbers across.
    #[tokio::test]
    async fn a_restarted_producer_is_reloaded_from_its_new_snapshot() {
        let runtime = crate::Runtime::builder().build().await;
        let datafusion = runtime.datafusion();
        let before = DataUpdateBroadcaster::new();
        let (first, _) = through_end(producer(&datafusion, &before, table(vec![1, 2])).await).await;
        let after = DataUpdateBroadcaster::new();
        assert_ne!(before.epoch(), after.epoch());
        let (second, _) =
            through_end(producer(&datafusion, &after, table(vec![1, 2, 3])).await).await;
        // The first stream fails as its producer goes away.
        let failed: Messages = futures::stream::iter(first.into_iter().map(Ok))
            .chain(futures::stream::once(async {
                Err(FlightError::Tonic(Box::new(tonic::Status::unavailable(
                    "connection reset",
                ))))
            }))
            .boxed();
        let mut stream = consumer(vec![failed, held_open(second)]);
        assert_eq!(snapshots(&mut stream, 1).await, vec![snapshot_of(2)]);
        assert_eq!(replacement(&mut stream).await, 3, "the new producer's rows");
        assert_eq!(next_rows(&mut stream).await, None);
    }

    /// An update from another epoch on a subscription sends it back for a
    /// fresh snapshot.
    #[tokio::test]
    async fn an_update_from_another_epoch_reloads_the_snapshot() {
        let runtime = crate::Runtime::builder().build().await;
        let datafusion = runtime.datafusion();
        let broadcaster = DataUpdateBroadcaster::new();
        let (mut first, _) =
            through_end(producer(&datafusion, &broadcaster, table(vec![1])).await).await;
        let mut stray = row_message(&first);
        stray.app_metadata = StreamMetadata::update("another-epoch", 1).to_bytes();
        first.push(stray);
        let (again, _) =
            through_end(producer(&datafusion, &broadcaster, table(vec![1, 2])).await).await;
        let mut stream = consumer(vec![held_open(first), held_open(again)]);
        assert_eq!(snapshots(&mut stream, 1).await, vec![snapshot_of(1)]);
        assert_eq!(
            replacement(&mut stream).await,
            2,
            "the stray update is not applied; the snapshot is reloaded"
        );
    }

    /// A reload that never reaches its end is discarded: nothing of it is
    /// applied, the dataset keeps its previous data, and the next complete
    /// reload is swapped in.
    #[tokio::test]
    async fn a_reload_without_its_end_is_discarded() {
        let runtime = crate::Runtime::builder().build().await;
        let datafusion = runtime.datafusion();
        let broadcaster = DataUpdateBroadcaster::new();
        let (first, _) =
            through_end(producer(&datafusion, &broadcaster, table(vec![1])).await).await;
        let failed_after_ready: Messages = futures::stream::iter(first.into_iter().map(Ok))
            .chain(futures::stream::once(async {
                Err(FlightError::Tonic(Box::new(tonic::Status::unavailable(
                    "connection reset",
                ))))
            }))
            .boxed();
        // The producer dies mid-snapshot: the end never comes.
        let (mut partial, _) =
            through_end(producer(&datafusion, &broadcaster, table(vec![1, 2, 3, 4])).await).await;
        partial.pop();
        let partial: Messages = futures::stream::iter(partial.into_iter().map(Ok))
            .chain(futures::stream::once(async {
                Err(FlightError::Tonic(Box::new(tonic::Status::unavailable(
                    "connection reset",
                ))))
            }))
            .boxed();
        let (complete, _) =
            through_end(producer(&datafusion, &broadcaster, table(vec![1, 2])).await).await;

        let logs = CapturedLogs::default();
        let _guard = tracing::subscriber::set_default(
            tracing_subscriber::fmt()
                .with_writer(logs.clone())
                .with_ansi(false)
                .finish(),
        );
        let mut stream = consumer(vec![failed_after_ready, partial, held_open(complete)]);
        assert_eq!(snapshots(&mut stream, 1).await, vec![snapshot_of(1)]);
        assert_eq!(
            replacement(&mut stream).await,
            2,
            "the partial reload is never swapped in; the complete one is"
        );
        let logs = String::from_utf8(logs.0.lock().clone()).expect("utf8");
        assert!(
            logs.contains("ERROR")
                && logs.contains("'orders' could not finish reloading")
                && logs.contains("keeps serving its previous data, which last changed"),
            "{logs}"
        );
    }

    /// A reload whose end does not arrive in time is discarded too.
    #[tokio::test]
    async fn a_reload_whose_end_times_out_is_discarded() {
        let runtime = crate::Runtime::builder().build().await;
        let datafusion = runtime.datafusion();
        let broadcaster = DataUpdateBroadcaster::new();
        let (first, _) =
            through_end(producer(&datafusion, &broadcaster, table(vec![1])).await).await;
        let failed_after_ready: Messages = futures::stream::iter(first.into_iter().map(Ok))
            .chain(futures::stream::once(async {
                Err(FlightError::Tonic(Box::new(tonic::Status::unavailable(
                    "connection reset",
                ))))
            }))
            .boxed();
        let (mut stalled, _) =
            through_end(producer(&datafusion, &broadcaster, table(vec![1, 2, 3, 4])).await).await;
        stalled.pop();
        let (complete, _) =
            through_end(producer(&datafusion, &broadcaster, table(vec![1, 2])).await).await;

        tokio::time::pause();
        let mut stream = consumer(vec![
            failed_after_ready,
            held_open(stalled),
            held_open(complete),
        ]);
        assert_eq!(snapshots(&mut stream, 1).await, vec![snapshot_of(1)]);
        assert_eq!(replacement(&mut stream).await, 2);
    }

    /// A reload that keeps making progress is not cut off, however long it
    /// takes in total.
    #[tokio::test]
    async fn a_slow_reload_that_keeps_progressing_is_swapped_in() {
        let runtime = crate::Runtime::builder().build().await;
        let datafusion = runtime.datafusion();
        let broadcaster = DataUpdateBroadcaster::new();
        let (first, _) =
            through_end(producer(&datafusion, &broadcaster, table(vec![1])).await).await;
        let failed_after_ready: Messages = futures::stream::iter(first.into_iter().map(Ok))
            .chain(futures::stream::once(async {
                Err(FlightError::Tonic(Box::new(tonic::Status::unavailable(
                    "connection reset",
                ))))
            }))
            .boxed();
        let (slow, _) =
            through_end(producer(&datafusion, &broadcaster, table(vec![1, 2, 3])).await).await;
        let gap = SNAPSHOT_END_ERROR_AFTER * 2 / 3;
        assert!(gap * u32::try_from(slow.len()).expect("few messages") > SNAPSHOT_END_ERROR_AFTER);
        let slow: Messages = futures::stream::iter(slow)
            .then(move |message| async move {
                tokio::time::sleep(gap).await;
                Ok(message)
            })
            .chain(futures::stream::pending())
            .boxed();

        tokio::time::pause();
        let mut stream = consumer(vec![failed_after_ready, slow]);
        assert_eq!(snapshots(&mut stream, 1).await, vec![snapshot_of(1)]);
        assert_eq!(replacement(&mut stream).await, 3);
    }

    /// A reload's replacement stays charged to the query memory pool until the
    /// apply loop is done with it.
    #[test]
    fn a_replacement_stays_reserved_until_it_is_dropped() {
        let memory_pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::default());
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        let data = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int32Array::from((1..=1_000).collect::<Vec<_>>()))],
        )
        .expect("batch");
        let mut reload = Reload::new(&memory_pool, DATASET);
        reload
            .stage(
                None,
                cdc::wrap_data_as_change_batch(&schema, &data).expect("change batch"),
            )
            .expect("staged");
        assert!(memory_pool.reserved() > 0, "staging is reserved");
        let stale = Arc::new(StaleDatasets::default());
        let registered = stale.register(DATASET);
        let replacement = reload
            .into_replacement(0, &schema, registered.stream(), None)
            .expect("replacement");
        assert!(memory_pool.reserved() > 0, "the replacement is reserved");
        drop(replacement);
        assert_eq!(memory_pool.reserved(), 0);
        assert!(
            stale.stale_for(DATASET).is_some(),
            "a replacement dropped before it commits leaves the dataset stale"
        );
    }

    /// A reload the query memory pool cannot hold is refused: the dataset keeps
    /// its data, an ERROR and the staleness metrics report it, its reservation
    /// is returned, and the reload is retried until one fits.
    #[tokio::test]
    async fn a_reload_the_memory_pool_cannot_hold_is_refused_and_retried() {
        let runtime = crate::Runtime::builder().build().await;
        let datafusion = runtime.datafusion();
        let broadcaster = DataUpdateBroadcaster::new();
        let (first, _) =
            through_end(producer(&datafusion, &broadcaster, table(vec![1])).await).await;
        let failed_after_ready: Messages = futures::stream::iter(first.into_iter().map(Ok))
            .chain(futures::stream::once(async {
                Err(FlightError::Tonic(Box::new(tonic::Status::unavailable(
                    "connection reset",
                ))))
            }))
            .boxed();
        let (too_large, _) =
            through_end(producer(&datafusion, &broadcaster, table((1..=50_000).collect())).await)
                .await;
        let (fits, _) =
            through_end(producer(&datafusion, &broadcaster, table(vec![1, 2])).await).await;

        let memory_pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(64 * 1024));
        let stale = Arc::new(StaleDatasets::default());
        let budget = ReloadBudget {
            memory_pool: Arc::clone(&memory_pool),
            stale: Arc::clone(&stale),
        };
        // At each subscribe: whether the dataset was reported stale, how many
        // reloads were refused, and what the pool still had reserved.
        let seen = Arc::new(parking_lot::Mutex::new(Vec::new()));
        let mut subscriptions: VecDeque<Messages> =
            vec![failed_after_ready, held_open(too_large), held_open(fits)].into();
        let subscribe = {
            let seen = Arc::clone(&seen);
            let stale = Arc::clone(&stale);
            let memory_pool = Arc::clone(&memory_pool);
            move || {
                seen.lock().push((
                    stale.stale_for(DATASET).is_some(),
                    stale.refused(DATASET),
                    memory_pool.reserved(),
                ));
                let next = subscriptions.pop_front();
                async move {
                    next.map(FlightDataDecoder::new)
                        .ok_or("no more subscriptions")
                }
            }
        };

        let logs = CapturedLogs::default();
        let _guard = tracing::subscriber::set_default(
            tracing_subscriber::fmt()
                .with_writer(logs.clone())
                .with_ansi(false)
                .finish(),
        );
        tokio::time::pause();
        let mut stream = append_stream(subscribe, DATASET.to_string(), budget).boxed();
        assert_eq!(snapshots(&mut stream, 1).await, vec![snapshot_of(1)]);
        let fits = replacement_envelope(&mut stream).await;
        assert_eq!(
            fits.change_batch().expect("built").data_batch().num_rows(),
            2,
            "the reload that fits"
        );
        assert_eq!(
            *seen.lock(),
            vec![(false, 0, 0), (false, 0, 0), (true, 1, 0)],
            "the refused reload is reported stale and returns its reservation before the retry"
        );
        assert!(
            stale.stale_for(DATASET).is_some(),
            "still stale while the swap is pending"
        );
        fits.commit().await.expect("commit the swap");
        assert_eq!(
            stale.stale_for(DATASET),
            None,
            "current again once the swap commits"
        );
        assert_eq!(stale.refused(DATASET), 1);
        assert_eq!(memory_pool.reserved(), 0);
        let logs = String::from_utf8(logs.0.lock().clone()).expect("utf8");
        assert!(
            logs.contains("ERROR")
                && logs.contains("'orders' refused to reload from its source")
                && logs.contains("keeps serving its previous data, which last changed"),
            "{logs}"
        );
    }

    /// After a marked producer's stream fails, a reconnect that ends, or
    /// fails, before its `begin` marker carries no epoch of its own; it is
    /// still resubscribed within the retry budget rather than ending the
    /// stream as if the producer never marked it.
    #[tokio::test]
    async fn a_reconnect_that_ends_before_begin_after_a_marked_stream_is_retried() {
        let runtime = crate::Runtime::builder().build().await;
        let datafusion = runtime.datafusion();
        let broadcaster = DataUpdateBroadcaster::new();
        let (first, _) =
            through_end(producer(&datafusion, &broadcaster, table(vec![1])).await).await;
        let failed_after_ready: Messages = futures::stream::iter(first.into_iter().map(Ok))
            .chain(futures::stream::once(async {
                Err(FlightError::Tonic(Box::new(tonic::Status::unavailable(
                    "connection reset",
                ))))
            }))
            .boxed();
        let ended_before_begin: Messages = futures::stream::empty().boxed();
        let failed_before_begin: Messages = futures::stream::once(async {
            Err(FlightError::Tonic(Box::new(tonic::Status::unavailable(
                "connection reset",
            ))))
        })
        .boxed();
        let (recovered, _) =
            through_end(producer(&datafusion, &broadcaster, table(vec![1, 2])).await).await;

        tokio::time::pause();
        let mut stream = consumer(vec![
            failed_after_ready,
            ended_before_begin,
            failed_before_begin,
            held_open(recovered),
        ]);
        assert_eq!(snapshots(&mut stream, 1).await, vec![snapshot_of(1)]);
        assert_eq!(
            replacement(&mut stream).await,
            2,
            "the stream recovers from the snapshot after the reconnects"
        );
    }

    /// `messages`, then the connection resets.
    fn reset_after(messages: Vec<FlightData>) -> Messages {
        futures::stream::iter(messages.into_iter().map(Ok))
            .chain(futures::stream::once(async {
                Err(FlightError::Tonic(Box::new(tonic::Status::unavailable(
                    "connection reset",
                ))))
            }))
            .boxed()
    }

    /// A marked producer whose reconnects stay up past a healthy minute but
    /// never begin a snapshot still exhausts the retry budget: only a completed
    /// snapshot makes such a subscription healthy.
    #[tokio::test]
    async fn reconnects_that_never_begin_a_snapshot_exhaust_the_retry_budget() {
        let runtime = crate::Runtime::builder().build().await;
        let broadcaster = DataUpdateBroadcaster::new();
        let (first, _) =
            through_end(producer(&runtime.datafusion(), &broadcaster, table(vec![1])).await).await;
        let schema_message = first[0].clone();
        let mut subscriptions = vec![reset_after(first)];
        for _ in 0..MAX_APPEND_STREAM_RESUBSCRIBES {
            let schema_message = schema_message.clone();
            subscriptions.push(
                futures::stream::once(async move { Ok(schema_message) })
                    .chain(futures::stream::once(async {
                        tokio::time::sleep(APPEND_STREAM_HEALTHY_SUBSCRIPTION * 2).await;
                        Err(FlightError::Tonic(Box::new(tonic::Status::unavailable(
                            "connection reset",
                        ))))
                    }))
                    .boxed(),
            );
        }

        tokio::time::pause();
        let mut stream = consumer(subscriptions);
        assert_eq!(snapshots(&mut stream, 1).await, vec![snapshot_of(1)]);
        let error = tokio::time::timeout(APPEND_STREAM_HEALTHY_SUBSCRIPTION * 100, stream.next())
            .await
            .expect("the stream gives up")
            .expect("an envelope")
            .err()
            .expect("an error");
        let reset = FlightError::Tonic(Box::new(tonic::Status::unavailable("connection reset")));
        match error {
            cdc::StreamError::Flight(message) => assert_eq!(
                message,
                format!(
                    "The append stream for dataset '{DATASET}' gave up after {MAX_APPEND_STREAM_RESUBSCRIBES} resubscribes: the stream from its source failed ({reset})"
                )
            ),
            other => panic!("expected the retry budget to run out, got {other:?}"),
        }
    }

    /// A producer that stops marking its stream, such as one rolled back to a
    /// release without markers, is followed as an unmarked producer again: its
    /// stream ending ends the append stream, as it always has.
    #[tokio::test]
    async fn a_producer_that_stops_marking_its_stream_is_followed_unmarked() {
        let runtime = crate::Runtime::builder().build().await;
        let datafusion = runtime.datafusion();
        let broadcaster = DataUpdateBroadcaster::new();
        let (first, _) =
            through_end(producer(&datafusion, &broadcaster, table(vec![1])).await).await;
        let (mut unmarked, _) =
            through_end(producer(&datafusion, &broadcaster, table(vec![1, 2])).await).await;
        for message in &mut unmarked {
            message.app_metadata = bytes::Bytes::new();
        }
        let ended = futures::stream::iter(unmarked.into_iter().map(Ok)).boxed();

        tokio::time::pause();
        let mut stream = consumer(vec![reset_after(first), ended]);
        assert_eq!(snapshots(&mut stream, 1).await, vec![snapshot_of(1)]);
        assert_eq!(
            next_rows(&mut stream).await,
            Some((1, true)),
            "the truncate"
        );
        assert_eq!(next_rows(&mut stream).await, Some((2, true)), "the rows");
        assert!(
            tokio::time::timeout(Duration::from_secs(1), stream.next())
                .await
                .expect("the append stream ends without retrying")
                .is_none(),
            "an unmarked stream that ends is not resubscribed"
        );
    }

    /// Two local datasets reading the same upstream table are tracked apart: a
    /// reload that commits for one leaves the other, whose reload was refused,
    /// reported stale.
    #[tokio::test]
    async fn two_local_datasets_over_one_upstream_table_are_tracked_apart() {
        const EAST: &str = "east";
        const WEST: &str = "west";
        let runtime = crate::Runtime::builder().build().await;
        let datafusion = runtime.datafusion();
        let broadcaster = DataUpdateBroadcaster::new();
        let failed_after_ready = || async {
            let (first, _) =
                through_end(producer(&datafusion, &broadcaster, table(vec![1])).await).await;
            futures::stream::iter(first.into_iter().map(Ok))
                .chain(futures::stream::once(async {
                    Err(FlightError::Tonic(Box::new(tonic::Status::unavailable(
                        "connection reset",
                    ))))
                }))
                .boxed()
        };
        let east_first: Messages = failed_after_ready().await;
        let west_first: Messages = failed_after_ready().await;
        let (too_large, _) =
            through_end(producer(&datafusion, &broadcaster, table((1..=50_000).collect())).await)
                .await;
        let (fits, _) =
            through_end(producer(&datafusion, &broadcaster, table(vec![1, 2])).await).await;

        // One staleness record, as the published metrics share one.
        let stale = Arc::new(StaleDatasets::default());
        let budget = |memory_pool: Arc<dyn MemoryPool>| ReloadBudget {
            memory_pool,
            stale: Arc::clone(&stale),
        };
        let reading = |subscriptions: Vec<Messages>, dataset: &str, budget| {
            let mut subscriptions: VecDeque<Messages> = subscriptions.into();
            append_stream(
                move || {
                    let next = subscriptions.pop_front();
                    async move {
                        next.map(FlightDataDecoder::new)
                            .ok_or("no more subscriptions")
                    }
                },
                dataset.to_string(),
                budget,
            )
            .boxed()
        };

        tokio::time::pause();
        let mut east = reading(
            vec![east_first, held_open(too_large)],
            EAST,
            budget(Arc::new(GreedyMemoryPool::new(64 * 1024))),
        );
        let mut west = reading(
            vec![west_first, held_open(fits)],
            WEST,
            budget(Arc::new(UnboundedMemoryPool::default())),
        );
        assert_eq!(snapshots(&mut east, 1).await, vec![snapshot_of(1)]);
        assert_eq!(snapshots(&mut west, 1).await, vec![snapshot_of(1)]);
        // East's reload is refused, and it waits to retry.
        assert!(
            tokio::time::timeout(Duration::from_secs(1), east.next())
                .await
                .is_err()
        );
        assert_eq!(stale.refused(EAST), 1);
        assert!(stale.stale_for(EAST).is_some());

        replacement_envelope(&mut west)
            .await
            .commit()
            .await
            .expect("commit the swap");
        assert_eq!(stale.stale_for(WEST), None, "west is current");
        assert_eq!(stale.refused(WEST), 0);
        assert!(
            stale.stale_for(EAST).is_some(),
            "west's reload does not clear east's stale flag"
        );
        assert_eq!(stale.refused(EAST), 1);
    }

    /// An append stream of [`DATASET`] over `subscriptions`, reporting to `stale`.
    fn stream_of(
        subscriptions: Vec<Messages>,
        memory_pool: Arc<dyn MemoryPool>,
        stale: &Arc<StaleDatasets>,
    ) -> BoxStream<'static, Result<ChangeEnvelope, cdc::StreamError>> {
        let mut subscriptions: VecDeque<Messages> = subscriptions.into();
        append_stream(
            move || {
                let next = subscriptions.pop_front();
                async move {
                    next.map(FlightDataDecoder::new)
                        .ok_or("no more subscriptions")
                }
            },
            DATASET.to_string(),
            ReloadBudget {
                memory_pool,
                stale: Arc::clone(stale),
            },
        )
        .boxed()
    }

    /// Two streams of one dataset, such as the outgoing and the incoming stream
    /// of a Spicepod reload, are tracked apart: a reload that commits for one
    /// does not report the other's refused reload current.
    #[tokio::test]
    async fn a_reload_of_one_stream_does_not_clear_another_streams_refused_reload() {
        let runtime = crate::Runtime::builder().build().await;
        let datafusion = runtime.datafusion();
        let broadcaster = DataUpdateBroadcaster::new();
        let (refused_first, _) =
            through_end(producer(&datafusion, &broadcaster, table(vec![1])).await).await;
        let (current_first, _) =
            through_end(producer(&datafusion, &broadcaster, table(vec![1])).await).await;
        let (too_large, _) =
            through_end(producer(&datafusion, &broadcaster, table((1..=50_000).collect())).await)
                .await;
        let (fits, _) =
            through_end(producer(&datafusion, &broadcaster, table(vec![1, 2])).await).await;

        let stale = Arc::new(StaleDatasets::default());
        tokio::time::pause();
        let mut refused = stream_of(
            vec![reset_after(refused_first), held_open(too_large)],
            Arc::new(GreedyMemoryPool::new(64 * 1024)),
            &stale,
        );
        let mut current = stream_of(
            vec![reset_after(current_first), held_open(fits)],
            Arc::new(UnboundedMemoryPool::default()),
            &stale,
        );
        assert_eq!(snapshots(&mut refused, 1).await, vec![snapshot_of(1)]);
        assert_eq!(snapshots(&mut current, 1).await, vec![snapshot_of(1)]);
        // The first stream's reload is refused, and it waits to retry.
        assert!(
            tokio::time::timeout(Duration::from_secs(1), refused.next())
                .await
                .is_err()
        );
        assert_eq!(stale.refused(DATASET), 1);
        assert!(stale.stale_for(DATASET).is_some());

        replacement_envelope(&mut current)
            .await
            .commit()
            .await
            .expect("commit the swap");
        assert!(
            stale.stale_for(DATASET).is_some(),
            "the other stream's reload does not report the refused one current"
        );
        drop(refused);
        assert_eq!(
            stale.stale_for(DATASET),
            None,
            "current once the stream whose reload was refused stops"
        );
        assert!(stale.is_reported(DATASET));
    }

    /// A stream does not inherit another stream's refused reload: once the
    /// outgoing stream of a Spicepod reload stops, the dataset reads as current
    /// as the incoming stream that replaced it.
    #[tokio::test]
    async fn a_stopped_streams_refused_reload_leaves_with_it() {
        let runtime = crate::Runtime::builder().build().await;
        let datafusion = runtime.datafusion();
        let broadcaster = DataUpdateBroadcaster::new();
        let (outgoing_first, _) =
            through_end(producer(&datafusion, &broadcaster, table(vec![1])).await).await;
        let (too_large, _) =
            through_end(producer(&datafusion, &broadcaster, table((1..=50_000).collect())).await)
                .await;
        let (incoming_first, _) =
            through_end(producer(&datafusion, &broadcaster, table(vec![1, 2])).await).await;

        let stale = Arc::new(StaleDatasets::default());
        tokio::time::pause();
        let mut outgoing = stream_of(
            vec![reset_after(outgoing_first), held_open(too_large)],
            Arc::new(GreedyMemoryPool::new(64 * 1024)),
            &stale,
        );
        assert_eq!(snapshots(&mut outgoing, 1).await, vec![snapshot_of(1)]);
        assert!(
            tokio::time::timeout(Duration::from_secs(1), outgoing.next())
                .await
                .is_err()
        );
        assert!(stale.stale_for(DATASET).is_some());

        let mut incoming = stream_of(
            vec![held_open(incoming_first)],
            Arc::new(UnboundedMemoryPool::default()),
            &stale,
        );
        assert_eq!(snapshots(&mut incoming, 1).await, vec![snapshot_of(2)]);
        assert!(
            stale.stale_for(DATASET).is_some(),
            "the outgoing stream still serves its previous data"
        );
        drop(outgoing);
        assert_eq!(
            stale.stale_for(DATASET),
            None,
            "the incoming stream is current"
        );
        assert_eq!(
            stale.refused(DATASET),
            1,
            "the dataset's refused reloads are counted while it is reported"
        );
    }

    /// A reload's replacement that is dropped before it commits, as the apply
    /// loop drops one it fails to write, leaves the dataset reported stale since
    /// it last changed, until a later reload commits.
    #[tokio::test]
    async fn a_replacement_dropped_before_it_commits_leaves_the_dataset_stale() {
        let runtime = crate::Runtime::builder().build().await;
        let datafusion = runtime.datafusion();
        let broadcaster = DataUpdateBroadcaster::new();
        let (first, _) =
            through_end(producer(&datafusion, &broadcaster, table(vec![1])).await).await;
        let (dropped, _) =
            through_end(producer(&datafusion, &broadcaster, table(vec![1, 2])).await).await;
        let (committed, _) =
            through_end(producer(&datafusion, &broadcaster, table(vec![1, 2, 3])).await).await;

        let stale = Arc::new(StaleDatasets::default());
        tokio::time::pause();
        let mut stream = stream_of(
            vec![
                reset_after(first),
                reset_after(dropped),
                held_open(committed),
            ],
            Arc::new(UnboundedMemoryPool::default()),
            &stale,
        );
        assert_eq!(snapshots(&mut stream, 1).await, vec![snapshot_of(1)]);
        drop(replacement_envelope(&mut stream).await);
        assert!(
            stale
                .stale_for(DATASET)
                .is_some_and(|stale_for| stale_for >= resubscribe_backoff(1)),
            "stale since the first snapshot, before the reload: {:?}",
            stale.stale_for(DATASET)
        );
        replacement_envelope(&mut stream)
            .await
            .commit()
            .await
            .expect("commit the swap");
        assert_eq!(
            stale.stale_for(DATASET),
            None,
            "current once a later reload commits"
        );
    }

    /// Updates that arrive during a reload are staged with it, and their
    /// sequence numbers decide what survives the swap: one the snapshot holds
    /// is dropped, a later one is kept, and after the swap only commits past
    /// the snapshot are applied.
    #[tokio::test]
    async fn updates_during_a_reload_are_staged_and_deduplicated() {
        let runtime = crate::Runtime::builder().build().await;
        let datafusion = runtime.datafusion();
        let broadcaster = DataUpdateBroadcaster::new();
        let path = TableReference::bare(DATASET);
        let _earlier = broadcaster.subscribe(&path).await;
        let (first, _) =
            through_end(producer(&datafusion, &broadcaster, table(vec![1])).await).await;
        let failed_after_ready: Messages = futures::stream::iter(first.into_iter().map(Ok))
            .chain(futures::stream::once(async {
                Err(FlightError::Tonic(Box::new(tonic::Status::unavailable(
                    "connection reset",
                ))))
            }))
            .boxed();
        // Two commits before the reload's snapshot: it holds them.
        broadcaster.publish(&path, update(vec![10])).await;
        broadcaster.publish(&path, update(vec![11])).await;
        let (mut reload, mut published) =
            through_end(producer(&datafusion, &broadcaster, table(vec![1, 2])).await).await;
        let snapshot_sequence = snapshot_sequence(&reload);
        assert_eq!(snapshot_sequence, 2);
        let epoch = broadcaster.epoch().to_string();
        let mut commit = async |sequence, ids| {
            published_update(&broadcaster, &mut published, &epoch, sequence, ids).await
        };
        // Each commit carries rows of its own, so keeping the wrong one shows in
        // the rows, not only in their count.
        let held = commit(snapshot_sequence, vec![20]).await;
        let later = commit(snapshot_sequence + 1, vec![21]).await;
        let replayed = commit(snapshot_sequence, vec![30]).await;
        let new = commit(snapshot_sequence + 2, vec![31]).await;
        let end = reload.pop().expect("end marker");
        // During the reload: a commit the snapshot holds, and a later one.
        reload.push(held);
        reload.push(later);
        reload.push(end);
        // After the swap: a replay of a held commit, and a new one.
        reload.push(replayed);
        reload.push(new);

        let mut stream = consumer(vec![failed_after_ready, held_open(reload)]);
        assert_eq!(snapshots(&mut stream, 1).await, vec![snapshot_of(1)]);
        assert_eq!(
            ids(&replacement_envelope(&mut stream).await),
            vec![1, 2, 21],
            "the snapshot's rows and the staged commit past it"
        );
        assert_eq!(
            next_ids(&mut stream).await,
            Some((vec![31], true)),
            "the new commit"
        );
        assert_eq!(next_ids(&mut stream).await, None, "the replay is skipped");
    }

    #[derive(Clone, Default)]
    struct CapturedLogs(Arc<parking_lot::Mutex<Vec<u8>>>);

    impl std::io::Write for CapturedLogs {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for CapturedLogs {
        type Writer = Self;
        fn make_writer(&'a self) -> Self::Writer {
            self.clone()
        }
    }

    /// Without its end marker, a snapshot never makes the dataset ready, and
    /// the wait is logged as an error naming the dataset.
    #[tokio::test]
    async fn a_snapshot_without_its_end_logs_an_error_and_stays_not_ready() {
        let runtime = crate::Runtime::builder().build().await;
        let broadcaster = DataUpdateBroadcaster::new();
        let (mut snapshot, _) =
            through_end(producer(&runtime.datafusion(), &broadcaster, table(vec![1, 2])).await)
                .await;
        snapshot.pop();

        let logs = CapturedLogs::default();
        let _guard = tracing::subscriber::set_default(
            tracing_subscriber::fmt()
                .with_writer(logs.clone())
                .with_ansi(false)
                .finish(),
        );
        tokio::time::pause();
        let mut stream = consumer(vec![held_open(snapshot)]);
        assert_eq!(
            next_rows(&mut stream).await,
            Some((1, false)),
            "the truncate"
        );
        assert_eq!(next_rows(&mut stream).await, Some((2, false)), "the rows");
        assert!(
            tokio::time::timeout(SNAPSHOT_END_ERROR_AFTER * 3, stream.next())
                .await
                .is_err(),
            "nothing makes the dataset ready"
        );
        let logs = String::from_utf8(logs.0.lock().clone()).expect("utf8");
        assert!(
            logs.contains("ERROR")
                && logs.contains("'orders' has not received the end of its initial snapshot"),
            "{logs}"
        );
    }
}
