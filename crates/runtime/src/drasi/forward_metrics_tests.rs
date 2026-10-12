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

//! Drives the shipped delivery queue and dead-letter store against a real HTTP
//! sink. The instruments are the ones [`DrasiForwardMetrics::from_meter`]
//! registers; production [`DeliveryQueue::spawn`] uses that same constructor
//! on the global meter.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{
    Array, ArrayBuilder, ArrayRef, BinaryBuilder, BooleanBuilder, Float64Builder, Int64Builder,
    ListArray, RecordBatch, StringArray, StringBuilder, StructArray, TimestampNanosecondArray,
    UInt8Array, UInt8Builder,
};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{DataType, Field, SchemaRef};
use opentelemetry::metrics::MeterProvider;
use opentelemetry_sdk::error::OTelSdkResult;
use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData, ResourceMetrics};
use opentelemetry_sdk::metrics::reader::MetricReader;
use opentelemetry_sdk::metrics::{ManualReader, Pipeline, SdkMeterProvider, Temporality};
use runtime_drasi::element::{element_id, rows_to_json};
use runtime_drasi::{DrasiSink, DrasiSinkConfig, ElementMapping, TransportConfig};
use runtime_metrics::drasi::{
    BATCHES, DEAD_LETTER_BATCHES, DrasiForwardMetrics, LABEL_COMPONENT, LABEL_OUTCOME,
    LABEL_TRANSPORT, LATENCY_MS, OUTCOME_DEAD_LETTERED, OUTCOME_DELIVERED, OUTCOME_DISCARDED,
    OUTCOME_RETRIED, QUEUE_DEPTH,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use super::QUEUED_SINK_POLICY;
use super::dead_letter::{DEFAULT_MAX_BATCHES, DeadLetterStore};
use super::queue::{DeliveryQueue, QueuedBatch, now_ms};

const COMPONENT: &str = "runtime.task_history";
const ALLOWED_LABELS: &[&str] = &[LABEL_COMPONENT, LABEL_TRANSPORT, LABEL_OUTCOME];
const OUTCOMES: &[&str] = &[
    OUTCOME_DELIVERED,
    OUTCOME_RETRIED,
    OUTCOME_DEAD_LETTERED,
    OUTCOME_DISCARDED,
];

#[derive(Clone, Debug)]
struct SharedManualReader(Arc<ManualReader>);

impl MetricReader for SharedManualReader {
    fn register_pipeline(&self, pipeline: std::sync::Weak<Pipeline>) {
        self.0.register_pipeline(pipeline);
    }

    fn collect(&self, metrics: &mut ResourceMetrics) -> OTelSdkResult {
        self.0.collect(metrics)
    }

    fn force_flush(&self) -> OTelSdkResult {
        self.0.force_flush()
    }

    fn shutdown_with_timeout(&self, timeout: Duration) -> OTelSdkResult {
        self.0.shutdown_with_timeout(timeout)
    }

    fn temporality(&self, kind: opentelemetry_sdk::metrics::InstrumentKind) -> Temporality {
        self.0.temporality(kind)
    }
}

struct Harness {
    /// Kept alive: dropping the provider shuts the reader down.
    _provider: SdkMeterProvider,
    reader: SharedManualReader,
    metrics: DrasiForwardMetrics,
}

impl Harness {
    fn new() -> Self {
        let reader = SharedManualReader(Arc::new(ManualReader::builder().build()));
        let provider = SdkMeterProvider::builder()
            .with_reader(reader.clone())
            .build();
        let metrics = DrasiForwardMetrics::from_meter(&provider.meter("drasi"));
        Self {
            _provider: provider,
            reader,
            metrics,
        }
    }

    fn collect(&self) -> Vec<Series> {
        let mut collected = ResourceMetrics::default();
        self.reader
            .collect(&mut collected)
            .expect("collect drasi metrics");
        let mut series = Vec::new();
        for metric in collected
            .scope_metrics()
            .filter(|scope| scope.scope().name() == "drasi")
            .flat_map(opentelemetry_sdk::metrics::data::ScopeMetrics::metrics)
        {
            match metric.data() {
                AggregatedMetrics::U64(MetricData::Sum(sum)) => {
                    for point in sum.data_points() {
                        series.push(Series {
                            name: metric.name().to_string(),
                            labels: labels_of(point.attributes()),
                            value: point.value(),
                            histogram_count: None,
                            histogram_sum: None,
                        });
                    }
                }
                AggregatedMetrics::U64(MetricData::Gauge(gauge)) => {
                    for point in gauge.data_points() {
                        series.push(Series {
                            name: metric.name().to_string(),
                            labels: labels_of(point.attributes()),
                            value: point.value(),
                            histogram_count: None,
                            histogram_sum: None,
                        });
                    }
                }
                AggregatedMetrics::F64(MetricData::Histogram(histogram)) => {
                    for point in histogram.data_points() {
                        series.push(Series {
                            name: metric.name().to_string(),
                            labels: labels_of(point.attributes()),
                            value: 0,
                            histogram_count: Some(point.count()),
                            histogram_sum: Some(point.sum()),
                        });
                    }
                }
                other => panic!(
                    "unexpected drasi metric shape for {}: {other:?}",
                    metric.name()
                ),
            }
        }
        series
    }
}

#[derive(Debug)]
struct Series {
    name: String,
    labels: BTreeMap<String, String>,
    value: u64,
    histogram_count: Option<u64>,
    histogram_sum: Option<f64>, // `None` when the series is not a histogram.
}

fn labels_of<'a>(
    attributes: impl Iterator<Item = &'a opentelemetry::KeyValue>,
) -> BTreeMap<String, String> {
    attributes
        .map(|kv| (kv.key.as_str().to_string(), kv.value.as_str().to_string()))
        .collect()
}

impl Harness {
    fn counter(&self, outcome: &str) -> u64 {
        self.collect()
            .into_iter()
            .filter(|series| {
                series.name == BATCHES
                    && series
                        .labels
                        .get(LABEL_OUTCOME)
                        .is_some_and(|value| value == outcome)
            })
            .map(|series| series.value)
            .sum()
    }

    fn gauge(&self, name: &str) -> u64 {
        let values: Vec<u64> = self
            .collect()
            .into_iter()
            .filter(|series| series.name == name)
            .map(|series| series.value)
            .collect();
        assert!(
            values.len() <= 1,
            "{name} should be one series per component, got {values:?}"
        );
        values.first().copied().unwrap_or(0)
    }

    fn latency(&self) -> (u64, f64) {
        let points: Vec<_> = self
            .collect()
            .into_iter()
            .filter(|series| series.name == LATENCY_MS)
            .collect();
        if points.is_empty() {
            return (0, 0.0);
        }
        assert_eq!(points.len(), 1, "one latency series: {points:?}");
        let point = &points[0];
        assert!(
            !point.labels.contains_key(LABEL_OUTCOME),
            "latency is recorded only on acknowledgement, with no outcome label: {point:?}"
        );
        (
            point.histogram_count.expect("histogram count"),
            point.histogram_sum.unwrap_or(0.0),
        )
    }

    fn assert_label_set(&self) {
        for series in self.collect() {
            for key in series.labels.keys() {
                assert!(
                    ALLOWED_LABELS.contains(&key.as_str()),
                    "{:?} carries label {key}, which is not component, transport, or outcome",
                    series.name
                );
            }
            if series.name == BATCHES {
                let outcome = series
                    .labels
                    .get(LABEL_OUTCOME)
                    .expect("batches are labelled with an outcome");
                assert!(
                    OUTCOMES.contains(&outcome.as_str()),
                    "outcome {outcome} is not delivered, retried, dead_lettered, or discarded"
                );
                assert_eq!(
                    series.labels.get(LABEL_TRANSPORT).map(String::as_str),
                    Some("http")
                );
            }
            if series.name == QUEUE_DEPTH || series.name == DEAD_LETTER_BATCHES {
                assert_eq!(
                    series.labels.keys().map(String::as_str).collect::<Vec<_>>(),
                    vec![LABEL_COMPONENT],
                    "gauges are one series per component"
                );
            }
        }
    }
}

fn row(id: &str) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(arrow::datatypes::Schema::new(vec![Field::new(
            "id",
            DataType::Utf8,
            false,
        )])),
        vec![Arc::new(StringArray::from(vec![id]))],
    )
    .expect("row")
}

fn sink(port: u16) -> Arc<DrasiSink> {
    let sink = DrasiSink::try_new(DrasiSinkConfig {
        dataset: COMPONENT.to_string(),
        source_id: "spice-runtime".to_string(),
        mapping: ElementMapping::new(COMPONENT, vec![COMPONENT.to_string()]),
        transport: TransportConfig::Http {
            endpoint: url::Url::parse(&format!("http://127.0.0.1:{port}")).expect("url"),
            request_timeout: Duration::from_millis(200),
        },
        on_delivery_error: QUEUED_SINK_POLICY,
    })
    .expect("sink");
    // The production attempt count still applies. Only the wait between
    // attempts is capped, so a closed port fails the attempt in milliseconds
    // instead of a minute of Fibonacci backoff.
    sink.set_retry_max_duration(Some(Duration::from_millis(1)));
    Arc::new(sink)
}

async fn closed_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("reserve a port");
    let port = listener.local_addr().expect("local addr").port();
    drop(listener);
    port
}

async fn rebind(port: u16) -> TcpListener {
    let start = Instant::now();
    loop {
        match TcpListener::bind(("127.0.0.1", port)).await {
            Ok(listener) => return listener,
            Err(error) if start.elapsed() < Duration::from_secs(2) => {
                tokio::time::sleep(Duration::from_millis(20)).await;
                let _ = error;
            }
            Err(error) => panic!("rebind 127.0.0.1:{port}: {error}"),
        }
    }
}

fn http_response(status: &str, body: &str) -> String {
    format!(
        "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    )
}

async fn read_request(stream: &mut TcpStream) -> Vec<u8> {
    let mut captured = Vec::new();
    let mut buf = [0_u8; 1024];
    let mut expected = None;
    loop {
        let read = match tokio::time::timeout(Duration::from_secs(2), stream.read(&mut buf)).await {
            Ok(Ok(0) | Err(_)) | Err(_) => break,
            Ok(Ok(n)) => n,
        };
        captured.extend_from_slice(&buf[..read]);
        if expected.is_none() {
            expected = request_len(&captured);
        }
        if let Some(len) = expected
            && captured.len() >= len
        {
            break;
        }
    }
    captured
}

fn request_len(request: &[u8]) -> Option<usize> {
    let header_end = request
        .windows(4)
        .position(|window| window == b"\r\n\r\n")?
        + 4;
    let headers = String::from_utf8_lossy(&request[..header_end]);
    let length = headers.lines().find_map(|line| {
        let (name, value) = line.split_once(':')?;
        if name.eq_ignore_ascii_case("content-length") {
            value.trim().parse::<usize>().ok()
        } else {
            None
        }
    })?;
    Some(header_end + length)
}

fn spawn_accept(listener: TcpListener, respond: impl Fn(&[u8]) -> String + Send + Sync + 'static) {
    let respond = Arc::new(respond);
    tokio::spawn(async move {
        loop {
            let Ok((mut stream, _)) = listener.accept().await else {
                break;
            };
            let respond = Arc::clone(&respond);
            tokio::spawn(async move {
                let request = read_request(&mut stream).await;
                let response = respond(&request);
                let _ = stream.write_all(response.as_bytes()).await;
            });
        }
    });
}

async fn queue_for(
    harness: &Harness,
    port: u16,
    dir: &std::path::Path,
) -> (DeliveryQueue, Arc<DeadLetterStore>) {
    let store = Arc::new(
        DeadLetterStore::open_with(
            dir.to_path_buf(),
            COMPONENT.to_string(),
            DEFAULT_MAX_BATCHES,
            "http",
            harness.metrics.clone(),
        )
        .await
        .expect("open the dead-letter store"),
    );
    let queue = DeliveryQueue::spawn_measuring(
        sink(port),
        COMPONENT.to_string(),
        8,
        Some(Arc::clone(&store)),
        Duration::from_millis(30),
        harness.metrics.clone(),
    );
    (queue, store)
}

async fn wait_for(mut ready: impl FnMut() -> bool, what: &str) {
    let start = Instant::now();
    while !ready() {
        if start.elapsed() > Duration::from_secs(15) {
            panic!("timed out waiting until {what}");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// A closed port retains the batch. Opening a listener on that port drains it.
#[tokio::test]
async fn a_closed_port_retains_the_batch_until_the_endpoint_accepts() {
    let harness = Harness::new();
    let port = closed_port().await;
    let dir = tempfile::tempdir().expect("tempdir");
    let (queue, store) = queue_for(&harness, port, dir.path()).await;

    let commit_ms = now_ms() - 3_000;
    queue
        .enqueue(QueuedBatch::uniform(
            "u",
            &["id".to_string()],
            row("kept"),
            Some(commit_ms),
        ))
        .await;

    wait_for(
        || {
            harness.counter(OUTCOME_DELIVERED) == 0
                && harness.gauge(DEAD_LETTER_BATCHES) > 0
                && harness.latency().0 == 0
        },
        "the closed port retains the batch without counting it delivered",
    )
    .await;
    assert!(
        harness.gauge(QUEUE_DEPTH) > 0 || harness.gauge(DEAD_LETTER_BATCHES) > 0,
        "while the endpoint does not accept, a gauge stays above zero"
    );
    assert!(
        !store.is_empty().await,
        "the batch is in the dead-letter store, not only an in-memory counter"
    );
    harness.assert_label_set();

    let listener = rebind(port).await;
    spawn_accept(listener, |_| http_response("200 OK", r#"{"success":true}"#));

    wait_for(
        || {
            harness.counter(OUTCOME_DELIVERED) >= 1
                && harness.gauge(QUEUE_DEPTH) == 0
                && harness.gauge(DEAD_LETTER_BATCHES) == 0
        },
        "the endpoint accepts the retained batch and both gauges return to zero",
    )
    .await;
    assert!(store.is_empty().await, "the store drains");

    let (count, sum) = harness.latency();
    assert_eq!(
        count,
        harness.counter(OUTCOME_DELIVERED),
        "latency is recorded once per acknowledgement"
    );
    assert!(
        (2_000.0..600_000.0).contains(&sum),
        "latency {sum} ms is the age since the source commit, not the queue time and not the epoch"
    );
    harness.assert_label_set();
}

/// A permanent rejection is counted as discarded and does not sit at the head
/// of the store, so the next batch is still delivered.
#[tokio::test]
async fn a_permanent_rejection_is_discarded_and_does_not_block_the_store() {
    let harness = Harness::new();
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("listen");
    let port = listener.local_addr().expect("addr").port();
    spawn_accept(listener, |request| {
        let body = String::from_utf8_lossy(request);
        if body.contains("poison") {
            http_response("400 Bad Request", r#"{"success":false}"#)
        } else {
            http_response("200 OK", r#"{"success":true}"#)
        }
    });

    let dir = tempfile::tempdir().expect("tempdir");
    let (queue, store) = queue_for(&harness, port, dir.path()).await;
    queue
        .enqueue(QueuedBatch::uniform(
            "u",
            &["id".to_string()],
            row("poison"),
            None,
        ))
        .await;
    queue
        .enqueue(QueuedBatch::uniform(
            "u",
            &["id".to_string()],
            row("kept"),
            None,
        ))
        .await;

    wait_for(
        || {
            harness.counter(OUTCOME_DISCARDED) >= 1
                && harness.counter(OUTCOME_DELIVERED) >= 1
                && harness.gauge(QUEUE_DEPTH) == 0
                && harness.gauge(DEAD_LETTER_BATCHES) == 0
        },
        "the permanent rejection is discarded and the following batch is delivered",
    )
    .await;
    assert!(
        store.is_empty().await,
        "a permanent rejection is not retained at the head of the store"
    );
    assert_eq!(store.discarded(), 0, "the store did not keep the rejection");
    let (count, _) = harness.latency();
    assert_eq!(
        count, 1,
        "the discarded batch does not record a delivery latency"
    );
    harness.assert_label_set();
}

fn attribute_list(rows: &[Vec<(&str, &str)>]) -> ListArray {
    let schema = otel_arrow::schema();
    let attributes = schema.field_with_name("attributes").expect("attributes");
    let DataType::List(item) = attributes.data_type() else {
        panic!("attributes is a list");
    };
    let DataType::Struct(fields) = item.data_type() else {
        panic!("attribute values are a struct");
    };

    let mut keys = StringBuilder::new();
    let mut types = UInt8Builder::new();
    let mut strings = StringBuilder::new();
    let mut ints = Int64Builder::new();
    let mut doubles = Float64Builder::new();
    let mut bools = BooleanBuilder::new();
    let mut bytes = BinaryBuilder::new();
    let mut serialized = BinaryBuilder::new();
    let mut offsets = vec![0_i32];
    for row in rows {
        for (key, value) in row {
            keys.append_value(*key);
            types.append_value(otel_arrow::AttributeValueType::Str.to_u8());
            strings.append_value(*value);
            ints.append_null();
            doubles.append_null();
            bools.append_null();
            bytes.append_null();
            serialized.append_null();
        }
        offsets.push(i32::try_from(keys.len()).expect("offsets fit"));
    }
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(keys.finish()),
        Arc::new(types.finish()),
        Arc::new(strings.finish()),
        Arc::new(ints.finish()),
        Arc::new(doubles.finish()),
        Arc::new(bools.finish()),
        Arc::new(bytes.finish()),
        Arc::new(serialized.finish()),
    ];
    let values = StructArray::try_new(fields.clone(), arrays, None).expect("attribute struct");
    ListArray::try_new(
        Arc::clone(item),
        OffsetBuffer::new(offsets.into()),
        Arc::new(values),
        None,
    )
    .expect("attribute list")
}

fn metrics_rows(attributes: ListArray) -> RecordBatch {
    let schema: SchemaRef = otel_arrow::schema();
    let series = otel_arrow::series_ids(&attributes).expect("series ids");
    let len = attributes.len();
    let mut columns: Vec<ArrayRef> = schema
        .fields()
        .iter()
        .map(|field| arrow::array::new_null_array(field.data_type(), len))
        .collect();
    let set = |columns: &mut Vec<ArrayRef>, name: &str, array: ArrayRef| {
        let index = schema.index_of(name).expect(name);
        columns[index] = array;
    };
    let timestamp = TimestampNanosecondArray::from(vec![1_700_000_000_000_000_000_i64; len])
        .with_timezone("UTC");
    set(&mut columns, "time_unix_nano", Arc::new(timestamp));
    set(
        &mut columns,
        "metric_type",
        Arc::new(UInt8Array::from(vec![2_u8; len])),
    );
    set(
        &mut columns,
        "name",
        Arc::new(StringArray::from(vec!["requests"; len])),
    );
    set(&mut columns, "attributes", Arc::new(attributes));
    set(&mut columns, otel_arrow::SERIES_ID_COLUMN, Arc::new(series));
    RecordBatch::try_new(schema, columns).expect("metrics rows")
}

/// Two `runtime.metrics` rows that share a time and a name and differ in
/// attributes get different element ids when keyed the way the spicepod
/// example keys `metrics`. Keying only time and name collides.
#[test]
fn metrics_series_identity_keeps_attribute_series_distinct() {
    let schema = otel_arrow::schema();
    let series = schema
        .field_with_name(otel_arrow::SERIES_ID_COLUMN)
        .expect("series_id");
    assert_eq!(series.data_type(), &DataType::Utf8);
    assert!(!series.is_nullable());
    assert!(matches!(
        schema
            .field_with_name("attributes")
            .expect("attributes")
            .data_type(),
        DataType::List(_)
    ));

    let batch = metrics_rows(attribute_list(&[
        vec![("status", "200")],
        vec![("status", "500")],
    ]));
    let rows = rows_to_json("runtime.metrics", &batch).expect("json rows");
    assert_eq!(rows.len(), 2);

    let mapping = ElementMapping::new("runtime.metrics", vec!["runtime.metrics".to_string()]);
    let key = ["time_unix_nano", "name", otel_arrow::SERIES_ID_COLUMN];
    let colliding = ["time_unix_nano", "name"];
    let with_series = [
        element_id(&mapping, &key, &rows[0]).expect("id"),
        element_id(&mapping, &key, &rows[1]).expect("id"),
    ];
    let without_series = [
        element_id(&mapping, &colliding, &rows[0]).expect("id"),
        element_id(&mapping, &colliding, &rows[1]).expect("id"),
    ];
    assert_eq!(
        without_series[0], without_series[1],
        "time and name alone give both attribute series one element id"
    );
    assert_ne!(
        with_series[0], with_series[1],
        "series_id keeps the two attribute series distinct: {with_series:?}"
    );
}

#[test]
fn latency_counter_names_match_the_issue() {
    // The names are the issue's, including the gauges without a `_count` suffix.
    assert_eq!(BATCHES, "drasi_forward_batches");
    assert_eq!(LATENCY_MS, "drasi_forward_latency_ms");
    assert_eq!(QUEUE_DEPTH, "drasi_queue_depth");
    assert_eq!(DEAD_LETTER_BATCHES, "drasi_dead_letter_batches");
}
