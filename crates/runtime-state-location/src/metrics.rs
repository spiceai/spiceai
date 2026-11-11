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

//! Metrics of the requests each feature makes to its state location, of the
//! conditional writes the store refuses because another writer changed the
//! object first, and of the objects cleanup deletes.

use std::fmt::{Display, Formatter};
use std::ops::Range;
use std::pin::Pin;
use std::sync::{Arc, OnceLock};
use std::task::{Context, Poll};
use std::time::Instant;

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use futures::{Stream, StreamExt};
use object_store::path::Path;
use object_store::{
    CopyOptions, Error as ObjectStoreError, GetOptions, GetResult, ListResult, MultipartUpload,
    ObjectMeta, ObjectStore, PutMode, PutMultipartOptions, PutOptions, PutPayload, PutResult,
    RenameOptions, UploadPart,
};
use object_store_occ::{is_probe_object, refused_as_conflict};
use opentelemetry::KeyValue;
use opentelemetry::metrics::{Counter, Histogram, Meter};
use telemetry::DURATION_MS_HISTOGRAM_BUCKETS;

use crate::StateFeature;

static METRICS: OnceLock<StateStoreMetrics> = OnceLock::new();

/// Creates the state-store instruments from the global meter provider and
/// publishes both counters at zero for every feature. Requests made before
/// this runs are not recorded, so call it once the provider is installed.
pub fn init() {
    METRICS
        .get_or_init(|| StateStoreMetrics::new(&opentelemetry::global::meter("state_store")))
        .publish_counters_at_zero();
}

/// Records that cleanup deleted `objects` objects of `feature`'s state.
pub fn record_cleanup_deleted(feature: StateFeature, objects: usize) {
    if let (Some(metrics), Some(label)) = (METRICS.get(), feature.metric_label()) {
        metrics.record_cleanup_deleted(label, objects);
    }
}

/// Wraps `store` so that every request `feature` makes through it is measured.
/// Returns `store` itself for a feature without a metric label.
pub(crate) fn metered(store: Arc<dyn ObjectStore>, feature: StateFeature) -> Arc<dyn ObjectStore> {
    match feature.metric_label() {
        Some(feature) => Arc::new(MeteredStore {
            inner: store,
            feature,
            metrics: Metrics::Global,
        }),
        None => store,
    }
}

/// The kind of a request, the `operation` label.
#[derive(Debug, Clone, Copy)]
enum Operation {
    Read,
    ConditionalWrite,
    Write,
    List,
    Delete,
}

impl Operation {
    fn label(self) -> &'static str {
        match self {
            Self::Read => "read",
            Self::ConditionalWrite => "conditional_write",
            Self::Write => "write",
            Self::List => "list",
            Self::Delete => "delete",
        }
    }

    fn of_put(mode: &PutMode) -> Self {
        match mode {
            PutMode::Overwrite => Self::Write,
            PutMode::Create | PutMode::Update(_) => Self::ConditionalWrite,
        }
    }
}

#[derive(Debug)]
struct StateStoreMetrics {
    request_duration_ms: Histogram<f64>,
    conflicts: Counter<u64>,
    cleanup_deleted: Counter<u64>,
}

impl StateStoreMetrics {
    fn new(meter: &Meter) -> Self {
        Self {
            request_duration_ms: meter
                .f64_histogram("state_store_request_duration_ms")
                .with_description(
                    "Time in milliseconds a feature waited for the state location to answer each request.",
                )
                .with_unit("ms")
                .with_boundaries(DURATION_MS_HISTOGRAM_BUCKETS.to_vec())
                .build(),
            conflicts: meter
                .u64_counter("state_store_conflicts_total")
                .with_description(
                    "Conditional writes to the state location that the store refused because another writer created or changed the object first.",
                )
                .build(),
            cleanup_deleted: meter
                .u64_counter("state_store_cleanup_deleted_total")
                .with_description(
                    "Objects that cleanup deleted from the state location because no instance needs them.",
                )
                .build(),
        }
    }

    fn publish_counters_at_zero(&self) {
        for feature in StateFeature::ALL {
            let Some(label) = feature.metric_label() else {
                continue;
            };
            let labels = feature_labels(label);
            self.conflicts.add(0, &labels);
            if feature.reports_cleanup() {
                self.cleanup_deleted.add(0, &labels);
            }
        }
    }

    fn record_request(&self, feature: &'static str, operation: Operation, started: Instant) {
        self.request_duration_ms.record(
            started.elapsed().as_secs_f64() * 1000.0,
            &[
                KeyValue::new("feature", feature),
                KeyValue::new("operation", operation.label()),
            ],
        );
    }

    fn record_conflict(&self, feature: &'static str) {
        self.conflicts.add(1, &feature_labels(feature));
    }

    fn record_cleanup_deleted(&self, feature: &'static str, objects: usize) {
        self.cleanup_deleted.add(
            u64::try_from(objects).unwrap_or(u64::MAX),
            &feature_labels(feature),
        );
    }
}

fn feature_labels(feature: &'static str) -> [KeyValue; 1] {
    [KeyValue::new("feature", feature)]
}

/// The instruments a [`MeteredStore`] records into.
#[derive(Debug, Clone, Copy)]
enum Metrics {
    /// The instruments [`init`] creates. Nothing is recorded before it runs.
    Global,
    #[cfg(test)]
    Fixed(&'static StateStoreMetrics),
}

impl Metrics {
    fn get(self) -> Option<&'static StateStoreMetrics> {
        match self {
            Self::Global => METRICS.get(),
            #[cfg(test)]
            Self::Fixed(metrics) => Some(metrics),
        }
    }
}

/// One request in flight, recorded by [`Request::finish`].
#[derive(Debug, Clone, Copy)]
struct Request {
    feature: &'static str,
    operation: Operation,
    metrics: Metrics,
    started: Instant,
}

impl Request {
    fn finish(self) {
        if let Some(metrics) = self.metrics.get() {
            metrics.record_request(self.feature, self.operation, self.started);
        }
    }
}

/// A store that measures every request a feature makes to its state location.
/// A request takes until the store answers: a read until the response starts,
/// a list or delete stream until it ends or is dropped, a multipart upload
/// until it completes or aborts.
#[derive(Debug)]
struct MeteredStore {
    inner: Arc<dyn ObjectStore>,
    /// The `feature` label.
    feature: &'static str,
    metrics: Metrics,
}

impl MeteredStore {
    fn start(&self, operation: Operation) -> Request {
        Request {
            feature: self.feature,
            operation,
            metrics: self.metrics,
            started: Instant::now(),
        }
    }

    fn timed<T: Send + 'static>(
        &self,
        operation: Operation,
        stream: BoxStream<'static, T>,
    ) -> BoxStream<'static, T> {
        TimedStream {
            inner: stream,
            request: Some(self.start(operation)),
        }
        .boxed()
    }
}

impl Display for MeteredStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.inner.fmt(f)
    }
}

#[deny(clippy::missing_trait_methods)]
#[async_trait]
impl ObjectStore for MeteredStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult, ObjectStoreError> {
        let mode = opts.mode.clone();
        let request = self.start(Operation::of_put(&mode));
        let result = self.inner.put_opts(location, payload, opts).await;
        request.finish();
        if let Err(err) = &result
            && refused_as_conflict(&mode, err)
            && !is_probe_object(location)
            && let Some(metrics) = self.metrics.get()
        {
            metrics.record_conflict(self.feature);
        }
        result
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>, ObjectStoreError> {
        let request = self.start(Operation::Write);
        match self.inner.put_multipart_opts(location, opts).await {
            Ok(upload) => Ok(Box::new(TimedUpload {
                inner: upload,
                request: Some(request),
            })),
            Err(err) => {
                request.finish();
                Err(err)
            }
        }
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> Result<GetResult, ObjectStoreError> {
        let request = self.start(Operation::Read);
        let result = self.inner.get_opts(location, options).await;
        request.finish();
        result
    }

    async fn get_ranges(
        &self,
        location: &Path,
        ranges: &[Range<u64>],
    ) -> Result<Vec<Bytes>, ObjectStoreError> {
        let request = self.start(Operation::Read);
        let result = self.inner.get_ranges(location, ranges).await;
        request.finish();
        result
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path, ObjectStoreError>>,
    ) -> BoxStream<'static, Result<Path, ObjectStoreError>> {
        self.timed(Operation::Delete, self.inner.delete_stream(locations))
    }

    fn list(
        &self,
        prefix: Option<&Path>,
    ) -> BoxStream<'static, Result<ObjectMeta, ObjectStoreError>> {
        self.timed(Operation::List, self.inner.list(prefix))
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta, ObjectStoreError>> {
        self.timed(Operation::List, self.inner.list_with_offset(prefix, offset))
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&Path>,
    ) -> Result<ListResult, ObjectStoreError> {
        let request = self.start(Operation::List);
        let result = self.inner.list_with_delimiter(prefix).await;
        request.finish();
        result
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> Result<(), ObjectStoreError> {
        let request = self.start(Operation::Write);
        let result = self.inner.copy_opts(from, to, options).await;
        request.finish();
        result
    }

    async fn rename_opts(
        &self,
        from: &Path,
        to: &Path,
        options: RenameOptions,
    ) -> Result<(), ObjectStoreError> {
        let request = self.start(Operation::Write);
        let result = self.inner.rename_opts(from, to, options).await;
        request.finish();
        result
    }
}

/// A stream that records its request once it ends or is dropped.
struct TimedStream<T> {
    inner: BoxStream<'static, T>,
    request: Option<Request>,
}

impl<T> Stream for TimedStream<T> {
    type Item = T;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<T>> {
        let polled = self.inner.poll_next_unpin(cx);
        if matches!(polled, Poll::Ready(None))
            && let Some(request) = self.request.take()
        {
            request.finish();
        }
        polled
    }
}

impl<T> Drop for TimedStream<T> {
    fn drop(&mut self) {
        if let Some(request) = self.request.take() {
            request.finish();
        }
    }
}

/// A multipart upload that records its request once it completes or aborts.
#[derive(Debug)]
struct TimedUpload {
    inner: Box<dyn MultipartUpload>,
    request: Option<Request>,
}

impl TimedUpload {
    fn finish(&mut self) {
        if let Some(request) = self.request.take() {
            request.finish();
        }
    }
}

#[async_trait]
impl MultipartUpload for TimedUpload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        self.inner.put_part(data)
    }

    async fn complete(&mut self) -> Result<PutResult, ObjectStoreError> {
        let result = self.inner.complete().await;
        self.finish();
        result
    }

    async fn abort(&mut self) -> Result<(), ObjectStoreError> {
        let result = self.inner.abort().await;
        self.finish();
        result
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use futures::TryStreamExt;
    use object_store::memory::InMemory;
    use object_store::{ObjectStoreExt, UpdateVersion};
    use object_store_occ::probe_conditional_writes;
    use opentelemetry::metrics::MeterProvider;
    use opentelemetry_sdk::metrics::SdkMeterProvider;
    use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData, ResourceMetrics};
    use opentelemetry_sdk::metrics::reader::MetricReader;
    use telemetry::reader::InitialReader;

    use super::*;

    /// State-store instruments on their own provider, so tests running in
    /// parallel do not share series.
    struct Recorded {
        reader: InitialReader,
        _provider: SdkMeterProvider,
        metrics: &'static StateStoreMetrics,
    }

    impl Recorded {
        fn new() -> Self {
            let reader = InitialReader::new();
            let provider = SdkMeterProvider::builder()
                .with_reader(reader.clone())
                .build();
            let metrics = Box::leak(Box::new(StateStoreMetrics::new(
                &provider.meter("state_store"),
            )));
            Self {
                reader,
                _provider: provider,
                metrics,
            }
        }

        fn store(&self, feature: StateFeature) -> MeteredStore {
            MeteredStore {
                inner: Arc::new(InMemory::new()),
                feature: feature.metric_label().expect("a metered feature"),
                metrics: Metrics::Fixed(self.metrics),
            }
        }

        /// Every series as `name{label=value,...}` to its value: a counter's sum,
        /// or a histogram's sample count.
        fn series(&self) -> BTreeMap<String, u64> {
            let mut collected = ResourceMetrics::default();
            self.reader
                .collect(&mut collected)
                .expect("collect the state-store metrics");
            let mut series = BTreeMap::new();
            for metric in collected
                .scope_metrics()
                .flat_map(opentelemetry_sdk::metrics::data::ScopeMetrics::metrics)
            {
                let name = |attributes: Vec<&KeyValue>| {
                    let mut labels: Vec<String> = attributes
                        .iter()
                        .map(|kv| format!("{}={}", kv.key.as_str(), kv.value.as_str()))
                        .collect();
                    labels.sort();
                    format!("{}{{{}}}", metric.name(), labels.join(","))
                };
                match metric.data() {
                    AggregatedMetrics::U64(MetricData::Sum(sum)) => {
                        for point in sum.data_points() {
                            series.insert(name(point.attributes().collect()), point.value());
                        }
                    }
                    AggregatedMetrics::F64(MetricData::Histogram(histogram)) => {
                        for point in histogram.data_points() {
                            series.insert(name(point.attributes().collect()), point.count());
                        }
                    }
                    _ => {}
                }
            }
            series
        }
    }

    fn requests(feature: &str, operation: &str) -> String {
        format!("state_store_request_duration_ms{{feature={feature},operation={operation}}}")
    }

    #[tokio::test]
    async fn every_request_is_recorded_by_feature_and_operation() {
        let recorded = Recorded::new();
        let store = recorded.store(StateFeature::Scheduler);
        let path = Path::from("cluster/cluster.json");

        store
            .put_opts(&path, "{}".into(), PutMode::Create.into())
            .await
            .expect("create");
        store.put(&path, "{}".into()).await.expect("overwrite");
        store.get(&path).await.expect("get");
        store.head(&path).await.expect("head");
        let listed: Vec<ObjectMeta> = store.list(None).try_collect().await.expect("list");
        assert_eq!(listed.len(), 1);
        store.list_with_delimiter(None).await.expect("list");
        let mut upload = store
            .put_multipart(&Path::from("cluster/upload.bin"))
            .await
            .expect("start upload");
        upload.put_part("part".into()).await.expect("upload part");
        upload.complete().await.expect("complete upload");
        store.delete(&path).await.expect("delete");

        let series = recorded.series();
        assert_eq!(
            series.get(&requests("scheduler", "conditional_write")),
            Some(&1)
        );
        assert_eq!(series.get(&requests("scheduler", "write")), Some(&2));
        assert_eq!(series.get(&requests("scheduler", "read")), Some(&2));
        assert_eq!(series.get(&requests("scheduler", "list")), Some(&2));
        assert_eq!(series.get(&requests("scheduler", "delete")), Some(&1));
    }

    #[tokio::test]
    async fn a_conditional_write_refused_for_another_writer_counts_one_conflict() {
        let recorded = Recorded::new();
        let store = recorded.store(StateFeature::RateControl);
        let path = Path::from("rate-control/pod/origin.json");
        let conflicts = || {
            recorded
                .series()
                .get("state_store_conflicts_total{feature=rate_control}")
                .copied()
        };

        let first = store
            .put_opts(&path, "1".into(), PutMode::Create.into())
            .await
            .expect("create");
        let read = UpdateVersion::from(first);
        store
            .put_opts(&path, "2".into(), PutMode::Update(read.clone()).into())
            .await
            .expect("update at the version read");
        assert_eq!(conflicts(), None, "writes that land are not conflicts");

        store
            .put_opts(&path, "3".into(), PutMode::Create.into())
            .await
            .expect_err("the object exists");
        assert_eq!(conflicts(), Some(1));
        store
            .put_opts(&path, "4".into(), PutMode::Update(read).into())
            .await
            .expect_err("another writer changed the object");
        assert_eq!(conflicts(), Some(2));

        let support = probe_conditional_writes(&store, &Path::from("rate-control")).await;
        assert!(support.is_enforced(), "the probe sees its writes refused");
        assert_eq!(
            conflicts(),
            Some(2),
            "the probe's refused writes are not conflicts"
        );
    }

    #[test]
    fn counters_are_published_at_zero_for_every_metered_feature() {
        let recorded = Recorded::new();
        recorded.metrics.publish_counters_at_zero();

        let series = recorded.series();
        for label in [
            "scheduler",
            "rate_control",
            "results_cache_warmup",
            "snapshots",
        ] {
            for counter in [
                "state_store_conflicts_total",
                "state_store_cleanup_deleted_total",
            ] {
                assert_eq!(
                    series.get(&format!("{counter}{{feature={label}}}")),
                    Some(&0),
                    "{counter} for {label}"
                );
            }
        }
        assert_eq!(
            series.get("state_store_conflicts_total{feature=sql_definitions}"),
            Some(&0)
        );
        assert_eq!(
            series.get("state_store_cleanup_deleted_total{feature=sql_definitions}"),
            None,
            "cleanup never deletes the SQL definitions"
        );
        assert_eq!(series.len(), 9, "no series for an unmetered feature");
    }

    #[test]
    fn the_startup_check_store_is_not_metered() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let built = metered(Arc::clone(&store), StateFeature::StartupCheck);
        assert!(Arc::ptr_eq(&built, &store));
    }

    #[test]
    fn cleanup_deletes_add_to_their_feature() {
        let recorded = Recorded::new();
        recorded.metrics.record_cleanup_deleted("snapshots", 3);
        recorded.metrics.record_cleanup_deleted("snapshots", 2);

        let series = recorded.series();
        assert_eq!(
            series.get("state_store_cleanup_deleted_total{feature=snapshots}"),
            Some(&5)
        );
        assert_eq!(
            series.get("state_store_cleanup_deleted_total{feature=scheduler}"),
            None
        );
    }
}
