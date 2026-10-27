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

//! The OTLP logs store: one Cayenne table with a narrow, fixed schema that holds every log
//! record of an export.
//!
//! A `sink:` dataset opts in with the `otlp_logs: all` param. Each log record becomes one row
//! whose columns are the OTLP `LogRecord` fields. Record, resource and scope attributes are
//! JSON objects in the metrics store's encoding, so a new attribute key never changes the
//! schema.
//!
//! Logs have no natural key and OTLP delivery is at least once, so the store appends without
//! deduplicating: an export the exporter retries is stored again, as the OTLP specification
//! accepts for telemetry.
//!
//! A dataset that only names the param gets every other setting that has one correct value
//! ([`apply_dataset_defaults`]): writable, Cayenne file acceleration appending, and the
//! `timestamp` column as its time column.

use std::borrow::Cow;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{Arc, LazyLock, Weak};
use std::time::{SystemTime, UNIX_EPOCH};

use arrow::array::{
    ArrayBuilder, ArrayRef, Int32Builder, StringBuilder, TimestampNanosecondBuilder, UInt32Builder,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use datafusion::common::TableReference;
use opentelemetry::metrics::{Counter, Meter};
use opentelemetry::{KeyValue as MetricLabel, global};
use opentelemetry_proto::tonic::collector::logs::v1::logs_service_server::LogsService;
pub use opentelemetry_proto::tonic::collector::logs::v1::logs_service_server::LogsServiceServer;
use opentelemetry_proto::tonic::collector::logs::v1::{
    ExportLogsPartialSuccess, ExportLogsServiceRequest, ExportLogsServiceResponse,
};
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value};
use opentelemetry_proto::tonic::logs::v1::LogRecord;
use runtime_query_engine::query_engine::{QueryEngine, UpdateType};
use spicepod::acceleration::{Acceleration, Mode, RefreshMode};
use spicepod::component::access::AccessMode;
use spicepod::component::dataset::{Dataset as SpicepodDataset, TimeFormat};
use tonic::codec::CompressionEncoding;
use tonic::{Request, Response, Status, async_trait};
use util::tracers::OnceTracer;
use util::warn_once;

use crate::Runtime;
use crate::opentelemetry_store::{any_value_to_json, attributes_to_json};

/// Dataset param that routes OTLP logs into a dataset. The only accepted value is `all`.
pub const OTLP_LOGS_PARAM: &str = "otlp_logs";
/// The value of [`OTLP_LOGS_PARAM`] that routes every log record of an export to the dataset.
pub const OTLP_LOGS_PARAM_ALL: &str = "all";

/// Whether a dataset's `otlp_logs` param value routes every log record to it.
#[must_use]
pub fn is_otlp_logs_store_param(value: &str) -> bool {
    // Matches the sink connector's `one_of_ignore_ascii_case` check of the same param.
    value.eq_ignore_ascii_case(OTLP_LOGS_PARAM_ALL)
}

/// Rows per logs store batch. Bounds the memory one batch takes, since resource and scope
/// columns repeat on every row.
pub const MAX_LOGS_BATCH_ROWS: usize = 8192;

/// Expanded string bytes per logs store batch. This keeps every `Utf8` column well below
/// its 32-bit Arrow offset limit when resource and scope values repeat across many rows.
const MAX_LOGS_BATCH_STRING_BYTES: usize = 256 * 1024 * 1024;

/// Largest OTLP logs message the gRPC receiver accepts, the same bound as an OTLP/HTTP body.
/// The gRPC default of 4 MiB rejects a Collector batch of 8,192 records of about 500 bytes.
pub const MAX_LOGS_MESSAGE_BYTES: usize = 128 * 1024 * 1024;

/// What an export is told when no dataset stores logs. Exporters treat `UNIMPLEMENTED` as
/// permanent, so they drop the export instead of retrying it until the configuration changes.
pub const NO_LOGS_STORE_MESSAGE: &str = "No dataset stores OpenTelemetry logs, so this export was not stored. Add a sink dataset with `params.otlp_logs: all` and Cayenne acceleration. See: https://spiceai.org/docs/features/observability";

/// What an export is told while a logs dataset is configured but not loaded, as during
/// startup. `UNAVAILABLE` is retryable, so exporters send the export again once it is.
fn logs_dataset_not_ready_message(table: &TableReference) -> String {
    format!(
        "The OpenTelemetry logs dataset '{table}' is not loaded yet, so this export was not stored; retry the export"
    )
}

/// The logs store's time column: when the event occurred.
pub const LOGS_TIME_COLUMN: &str = "timestamp";

/// Time zone of the logs store's timestamp columns.
const UTC: &str = "UTC";

fn timestamp_type() -> DataType {
    DataType::Timestamp(TimeUnit::Nanosecond, Some(UTC.into()))
}

/// The fixed schema of the OTLP logs store table.
pub static LOGS_STORE_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Field::new(LOGS_TIME_COLUMN, timestamp_type(), false),
        Field::new("observed_timestamp", timestamp_type(), true),
        Field::new("severity_number", DataType::Int32, true),
        Field::new("severity_text", DataType::Utf8, true),
        Field::new("body", DataType::Utf8, true),
        Field::new("attributes", DataType::Utf8, true),
        Field::new("trace_id", DataType::Utf8, true),
        Field::new("span_id", DataType::Utf8, true),
        Field::new("flags", DataType::UInt32, true),
        Field::new("event_name", DataType::Utf8, true),
        Field::new("service_name", DataType::Utf8, true),
        Field::new("resource_attributes", DataType::Utf8, true),
        Field::new("resource_schema_url", DataType::Utf8, true),
        Field::new("scope_name", DataType::Utf8, true),
        Field::new("scope_version", DataType::Utf8, true),
        Field::new("scope_attributes", DataType::Utf8, true),
        Field::new("scope_schema_url", DataType::Utf8, true),
    ]))
});

/// Whether a spicepod dataset routes OTLP logs to it (`otlp_logs: all`).
#[must_use]
pub fn is_logs_dataset(dataset: &SpicepodDataset) -> bool {
    dataset
        .params
        .as_ref()
        .and_then(|params| params.data.get(OTLP_LOGS_PARAM))
        .is_some_and(|value| is_otlp_logs_store_param(&value.as_string()))
}

/// Fills in the settings of an `otlp_logs: all` dataset that have one correct value, so the
/// dataset needs only its `from`, `name` and the param:
///
/// - `access: read_write`, since the receiver writes every record to it;
/// - `time_column: timestamp` with `time_format: timestamptz`, the store's fixed time column;
/// - with no `acceleration` block, Cayenne in `mode: file` appending, so logs survive a
///   restart; with one, an unset `engine` is `cayenne` and an unset `refresh_mode` is `append`.
///
/// An explicit non-Cayenne engine is kept, and the sink connector rejects it when the dataset
/// loads. Other datasets are left as they are.
pub fn apply_dataset_defaults(dataset: &mut SpicepodDataset) {
    if !is_logs_dataset(dataset) {
        return;
    }
    dataset.access = AccessMode::ReadWrite;
    if dataset.time_column.is_none() {
        dataset.time_column = Some(LOGS_TIME_COLUMN.to_string());
        if dataset.time_format.is_none() {
            dataset.time_format = Some(TimeFormat::Timestamptz);
        }
    }
    let acceleration = dataset.acceleration.get_or_insert_with(|| Acceleration {
        mode: Mode::File,
        ..Acceleration::default()
    });
    acceleration
        .engine
        .get_or_insert_with(|| "cayenne".to_string());
    acceleration.refresh_mode.get_or_insert(RefreshMode::Append);
}

static METER: LazyLock<Meter> = LazyLock::new(|| global::meter("otlp"));

/// Log records committed to each logs store dataset. Ingest throughput is its rate.
static LOG_RECORDS_WRITTEN: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("otlp_log_records_written")
        .with_description(
            "Cumulative number of OpenTelemetry log records written to each logs dataset.",
        )
        .build()
});

/// The resource attribute promoted to the `service_name` column.
const SERVICE_NAME_ATTRIBUTE: &str = "service.name";

fn non_empty(s: &str) -> Option<&str> {
    (!s.is_empty()).then_some(s)
}

/// Columns shared by every record of one resource.
struct ResourceColumns<'a> {
    attributes: String,
    service_name: Option<String>,
    schema_url: Option<&'a str>,
}

impl<'a> ResourceColumns<'a> {
    fn new(attributes: &[KeyValue], schema_url: &'a str) -> Self {
        // A repeated key keeps its last value, as in the attributes JSON.
        let service_name = attributes
            .iter()
            .rev()
            .find(|kv| kv.key == SERVICE_NAME_ATTRIBUTE)
            .and_then(
                |kv| match kv.value.as_ref().and_then(|v| v.value.as_ref()) {
                    Some(any_value::Value::StringValue(s)) => Some(s.clone()),
                    _ => None,
                },
            );
        Self {
            attributes: attributes_to_json(attributes).to_string(),
            service_name,
            schema_url: non_empty(schema_url),
        }
    }
}

/// Columns shared by every record of one instrumentation scope.
struct ScopeColumns<'a> {
    name: Option<&'a str>,
    version: Option<&'a str>,
    attributes: String,
    schema_url: Option<&'a str>,
}

/// Values that need conversion or validation before a record can be appended.
struct PreparedRecord<'a> {
    timestamp: i64,
    observed_timestamp: Option<i64>,
    body: Option<Cow<'a, str>>,
    attributes: String,
    string_bytes: usize,
}

impl<'a> PreparedRecord<'a> {
    fn new(
        resource: &ResourceColumns<'_>,
        scope: &ScopeColumns<'_>,
        record: &'a LogRecord,
        received_unix_nano: u64,
    ) -> Option<Self> {
        let timestamp = i64::try_from(record_time(record, received_unix_nano)).ok()?;
        let observed_timestamp = match record.observed_time_unix_nano {
            0 => None,
            observed => Some(i64::try_from(observed).ok()?),
        };
        let body = prepared_body(record.body.as_ref());
        let attributes = attributes_to_json(&record.attributes).to_string();
        let string_bytes = [
            non_empty(&record.severity_text).map_or(0, str::len),
            body.as_deref().map_or(0, str::len),
            attributes.len(),
            encoded_id_len(&record.trace_id),
            encoded_id_len(&record.span_id),
            non_empty(&record.event_name).map_or(0, str::len),
            resource.service_name.as_deref().map_or(0, str::len),
            resource.attributes.len(),
            resource.schema_url.map_or(0, str::len),
            scope.name.map_or(0, str::len),
            scope.version.map_or(0, str::len),
            scope.attributes.len(),
            scope.schema_url.map_or(0, str::len),
        ]
        .into_iter()
        .fold(0_usize, usize::saturating_add);
        Some(Self {
            timestamp,
            observed_timestamp,
            body,
            attributes,
            string_bytes,
        })
    }
}

/// Builders for every column of [`LOGS_STORE_SCHEMA`].
struct LogsBuilders {
    timestamp: TimestampNanosecondBuilder,
    observed_timestamp: TimestampNanosecondBuilder,
    severity_number: Int32Builder,
    severity_text: StringBuilder,
    body: StringBuilder,
    attributes: StringBuilder,
    trace_id: StringBuilder,
    span_id: StringBuilder,
    flags: UInt32Builder,
    event_name: StringBuilder,
    service_name: StringBuilder,
    resource_attributes: StringBuilder,
    resource_schema_url: StringBuilder,
    scope_name: StringBuilder,
    scope_version: StringBuilder,
    scope_attributes: StringBuilder,
    scope_schema_url: StringBuilder,
    /// Scratch for a trace or span id's hex, reused across records.
    id_scratch: String,
}

impl LogsBuilders {
    fn with_capacity(rows: usize) -> Self {
        let strings = || StringBuilder::with_capacity(rows, 0);
        let timestamps = || TimestampNanosecondBuilder::with_capacity(rows).with_timezone(UTC);
        Self {
            timestamp: timestamps(),
            observed_timestamp: timestamps(),
            severity_number: Int32Builder::with_capacity(rows),
            severity_text: strings(),
            body: strings(),
            attributes: strings(),
            trace_id: strings(),
            span_id: strings(),
            flags: UInt32Builder::with_capacity(rows),
            event_name: strings(),
            service_name: strings(),
            resource_attributes: strings(),
            resource_schema_url: strings(),
            scope_name: strings(),
            scope_version: strings(),
            scope_attributes: strings(),
            scope_schema_url: strings(),
            id_scratch: String::with_capacity(32),
        }
    }

    fn len(&self) -> usize {
        self.timestamp.len()
    }

    fn append(
        &mut self,
        resource: &ResourceColumns<'_>,
        scope: &ScopeColumns<'_>,
        record: &LogRecord,
        prepared: &PreparedRecord<'_>,
    ) {
        self.timestamp.append_value(prepared.timestamp);
        self.observed_timestamp
            .append_option(prepared.observed_timestamp);
        self.severity_number
            .append_option((record.severity_number != 0).then_some(record.severity_number));
        self.severity_text
            .append_option(non_empty(&record.severity_text));
        self.body.append_option(prepared.body.as_deref());
        self.attributes.append_value(&prepared.attributes);
        append_id(&mut self.trace_id, &record.trace_id, &mut self.id_scratch);
        append_id(&mut self.span_id, &record.span_id, &mut self.id_scratch);
        self.flags.append_value(record.flags);
        self.event_name.append_option(non_empty(&record.event_name));
        self.service_name
            .append_option(resource.service_name.as_deref());
        self.resource_attributes.append_value(&resource.attributes);
        self.resource_schema_url.append_option(resource.schema_url);
        self.scope_name.append_option(scope.name);
        self.scope_version.append_option(scope.version);
        self.scope_attributes.append_value(&scope.attributes);
        self.scope_schema_url.append_option(scope.schema_url);
    }

    /// The rows appended since the last call, as a batch. The builders are left empty.
    fn finish(&mut self) -> Result<RecordBatch, ArrowError> {
        let columns: Vec<ArrayRef> = vec![
            Arc::new(self.timestamp.finish()),
            Arc::new(self.observed_timestamp.finish()),
            Arc::new(self.severity_number.finish()),
            Arc::new(self.severity_text.finish()),
            Arc::new(self.body.finish()),
            Arc::new(self.attributes.finish()),
            Arc::new(self.trace_id.finish()),
            Arc::new(self.span_id.finish()),
            Arc::new(self.flags.finish()),
            Arc::new(self.event_name.finish()),
            Arc::new(self.service_name.finish()),
            Arc::new(self.resource_attributes.finish()),
            Arc::new(self.resource_schema_url.finish()),
            Arc::new(self.scope_name.finish()),
            Arc::new(self.scope_version.finish()),
            Arc::new(self.scope_attributes.finish()),
            Arc::new(self.scope_schema_url.finish()),
        ];
        RecordBatch::try_new(Arc::clone(&LOGS_STORE_SCHEMA), columns)
    }
}

/// When the event occurred: the record's time, else its observed time, else when Spice
/// received it. Never 0, so time filters and retention see every record.
fn record_time(record: &LogRecord, received_unix_nano: u64) -> u64 {
    if record.time_unix_nano != 0 {
        record.time_unix_nano
    } else if record.observed_time_unix_nano != 0 {
        record.observed_time_unix_nano
    } else {
        received_unix_nano
    }
}

/// A string body as is; any other body as JSON in the attributes encoding. A string-table
/// reference is a profiling-signal encoding that has no meaning in a log, so it is stored as
/// absent, like a body that is not set.
fn prepared_body(body: Option<&AnyValue>) -> Option<Cow<'_, str>> {
    match body.and_then(|b| b.value.as_ref()) {
        None | Some(any_value::Value::StringValueStrindex(_)) => None,
        Some(any_value::Value::StringValue(s)) => Some(Cow::Borrowed(s)),
        Some(_) => Some(Cow::Owned(any_value_to_json(body).to_string())),
    }
}

const HEX_DIGITS: &[u8; 16] = b"0123456789abcdef";

/// A trace or span id as lowercase hex, or null when it is absent or all zero (an invalid id
/// in W3C Trace Context, which some SDKs send in place of none). An id of an unexpected length
/// is stored as sent. `scratch` is reused across records.
fn append_id(builder: &mut StringBuilder, id: &[u8], scratch: &mut String) {
    if id.iter().all(|b| *b == 0) {
        builder.append_null();
        return;
    }
    scratch.clear();
    for byte in id {
        scratch.push(char::from(HEX_DIGITS[usize::from(byte >> 4)]));
        scratch.push(char::from(HEX_DIGITS[usize::from(byte & 0x0f)]));
    }
    builder.append_value(scratch.as_str());
}

fn encoded_id_len(id: &[u8]) -> usize {
    if id.iter().all(|byte| *byte == 0) {
        0
    } else {
        id.len().saturating_mul(2)
    }
}

/// Number of log records in an export.
#[must_use]
pub fn log_record_count(request: &ExportLogsServiceRequest) -> usize {
    request
        .resource_logs
        .iter()
        .flat_map(|r| &r.scope_logs)
        .map(|s| s.log_records.len())
        .sum()
}

/// The logs store batches of an export, and how many of its records could not be stored.
pub struct LogsBatches {
    pub batches: Vec<RecordBatch>,
    /// Records whose time is past the last instant a nanosecond timestamp holds.
    pub rejected: usize,
}

/// Every log record of `request`, in order, as logs store batches of at most `max_rows` rows.
/// `received_unix_nano` is the time of a record that has neither a time nor an observed time.
///
/// # Errors
///
/// Returns an error if a batch cannot be assembled from the built columns.
pub fn export_to_batches(
    request: &ExportLogsServiceRequest,
    received_unix_nano: u64,
    max_rows: usize,
) -> Result<LogsBatches, ArrowError> {
    export_to_batches_with_limits(
        request,
        received_unix_nano,
        max_rows,
        MAX_LOGS_BATCH_STRING_BYTES,
    )
}

fn export_to_batches_with_limits(
    request: &ExportLogsServiceRequest,
    received_unix_nano: u64,
    max_rows: usize,
    max_string_bytes: usize,
) -> Result<LogsBatches, ArrowError> {
    let max_rows = max_rows.max(1);
    let max_string_bytes = max_string_bytes.max(1);
    let total = log_record_count(request);
    let mut batches = Vec::with_capacity(total.div_ceil(max_rows));
    let mut rejected = 0;
    let mut batch_string_bytes = 0_usize;
    let mut builders = LogsBuilders::with_capacity(total.min(max_rows));
    for resource_logs in &request.resource_logs {
        let resource = ResourceColumns::new(
            resource_logs
                .resource
                .as_ref()
                .map_or(&[][..], |r| r.attributes.as_slice()),
            &resource_logs.schema_url,
        );
        for scope_logs in &resource_logs.scope_logs {
            let scope = scope_logs.scope.as_ref();
            let scope = ScopeColumns {
                name: scope.and_then(|s| non_empty(&s.name)),
                version: scope.and_then(|s| non_empty(&s.version)),
                attributes: attributes_to_json(scope.map_or(&[][..], |s| s.attributes.as_slice()))
                    .to_string(),
                schema_url: non_empty(&scope_logs.schema_url),
            };
            for record in &scope_logs.log_records {
                let Some(prepared) =
                    PreparedRecord::new(&resource, &scope, record, received_unix_nano)
                else {
                    rejected += 1;
                    continue;
                };
                if prepared.string_bytes > max_string_bytes {
                    return Err(ArrowError::InvalidArgumentError(format!(
                        "The OpenTelemetry logs export contains one record that expands to {} string bytes, above the per-batch limit of {max_string_bytes} bytes; reduce the record's body or attributes",
                        prepared.string_bytes
                    )));
                }
                if builders.len() > 0
                    && batch_string_bytes.saturating_add(prepared.string_bytes) > max_string_bytes
                {
                    batches.push(builders.finish()?);
                    batch_string_bytes = 0;
                }
                builders.append(&resource, &scope, record, &prepared);
                batch_string_bytes = batch_string_bytes.saturating_add(prepared.string_bytes);
                if builders.len() == max_rows {
                    batches.push(builders.finish()?);
                    batch_string_bytes = 0;
                }
            }
        }
    }
    if builders.len() > 0 {
        batches.push(builders.finish()?);
    }
    Ok(LogsBatches { batches, rejected })
}

/// The partial-success message for records whose time a timestamp cannot hold.
const OUT_OF_RANGE_TIME_MESSAGE: &str = "Log records with a time_unix_nano or observed_time_unix_nano after 2262-04-11T23:47:16Z were rejected: no timestamp can hold them.";

fn now_unix_nanos() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_nanos()).unwrap_or(u64::MAX))
}

/// Builds the export's batches on the rayon pool: an export can hold hundreds of thousands of
/// records, and converting them on the gRPC or HTTP runtime would stall every other request
/// it serves, including `/health`.
async fn build_batches(request: ExportLogsServiceRequest) -> Result<LogsBatches, Status> {
    let received_unix_nano = now_unix_nanos();
    run_batch_builder(move || export_to_batches(&request, received_unix_nano, MAX_LOGS_BATCH_ROWS))
        .await
}

const BATCH_BUILD_FAILED_MESSAGE: &str =
    "Failed to convert the OTLP logs export: the batch builder failed";

async fn run_batch_builder(
    builder: impl FnOnce() -> Result<LogsBatches, ArrowError> + Send + 'static,
) -> Result<LogsBatches, Status> {
    let (tx, rx) = tokio::sync::oneshot::channel();
    rayon::spawn(move || {
        let result = match catch_unwind(AssertUnwindSafe(builder)) {
            Ok(Ok(batches)) => Ok(batches),
            Ok(Err(ArrowError::InvalidArgumentError(message))) => {
                Err(Status::invalid_argument(message))
            }
            Ok(Err(error)) => Err(Status::internal(format!(
                "Failed to convert the OTLP logs export: {error}"
            ))),
            Err(_) => Err(Status::internal(BATCH_BUILD_FAILED_MESSAGE)),
        };
        // The receiver is gone only when the export was cancelled; nothing waits for the result.
        let _ = tx.send(result);
    });
    rx.await
        .map_err(|_| Status::internal(BATCH_BUILD_FAILED_MESSAGE))?
}

/// The OpenTelemetry logs ingest service (the `LogsService::export` handler).
pub struct LogsIngestService {
    datafusion: Arc<dyn QueryEngine>,
    /// Weak handle to the runtime, used to find the `otlp_logs: all` datasets of its app.
    runtime: Option<Weak<Runtime>>,
    once_tracer: OnceTracer,
    /// Logs store tables to use instead of the `otlp_logs: all` datasets of the runtime's
    /// app. Only tests set this, since they drive the service without a runtime.
    store_tables_override: Option<Vec<TableReference>>,
}

impl LogsIngestService {
    /// The datasets with the `otlp_logs: all` param.
    async fn store_tables(&self) -> Vec<TableReference> {
        if let Some(tables) = &self.store_tables_override {
            return tables.clone();
        }
        let Some(runtime) = self.runtime.as_ref().and_then(Weak::upgrade) else {
            return Vec::new();
        };
        let app = runtime.app();
        let app = app.read().await;
        let Some(app) = app.as_ref() else {
            return Vec::new();
        };
        app.datasets
            .iter()
            .filter(|dataset| is_logs_dataset(dataset))
            .map(|dataset| TableReference::from(dataset.name.as_str()))
            .collect()
    }
}

#[async_trait]
impl LogsService for LogsIngestService {
    async fn export(
        &self,
        request: Request<ExportLogsServiceRequest>,
    ) -> std::result::Result<Response<ExportLogsServiceResponse>, Status> {
        crate::http::v1::ensure_otlp_grpc_write_access().await?;
        let request = request.into_inner();
        let records = log_record_count(&request);
        if records == 0 {
            return Ok(Response::new(ExportLogsServiceResponse::default()));
        }
        let tables = self.store_tables().await;
        if tables.is_empty() {
            warn_once!(self.once_tracer, "{}", NO_LOGS_STORE_MESSAGE);
            return Err(Status::unimplemented(NO_LOGS_STORE_MESSAGE));
        }
        // A dataset is writable once it has loaded. Until then — at startup, or while it
        // fails to load, which its own load error explains — the export is refused as
        // retryable rather than stored in only some of the logs datasets, or dropped.
        if let Some(table) = tables
            .iter()
            .find(|table| !self.datafusion.is_writable(table))
        {
            tracing::debug!(
                "OpenTelemetry export refused: logs dataset '{table}' is not loaded yet"
            );
            return Err(Status::unavailable(logs_dataset_not_ready_message(table)));
        }
        let LogsBatches { batches, rejected } = build_batches(request).await?;
        if batches.is_empty() {
            return Err(Status::invalid_argument(format!(
                "All {rejected} log records were rejected. {OUT_OF_RANGE_TIME_MESSAGE}"
            )));
        }
        let stored = records - rejected;
        for table in &tables {
            // One write per dataset holds the whole export, so a failure stores none of it and
            // the retry the exporter is asked for writes each record once.
            if let Err(e) = self
                .datafusion
                .write_data(
                    table,
                    Arc::clone(&LOGS_STORE_SCHEMA),
                    batches.clone(),
                    UpdateType::Append,
                )
                .await
            {
                tracing::warn!(
                    "Failed to write OpenTelemetry logs to dataset '{table}', so the export was refused and the exporter will retry it. Cause: {e}"
                );
                return Err(Status::unavailable(format!(
                    "Failed to write to the OpenTelemetry logs dataset '{table}'; retry the export"
                )));
            }
            LOG_RECORDS_WRITTEN.add(
                stored as u64,
                &[MetricLabel::new("dataset", table.to_string())],
            );
        }
        tracing::debug!(
            "OpenTelemetry export: wrote {stored} log record(s) to {} logs dataset(s)",
            tables.len()
        );
        let partial_success = (rejected > 0).then(|| ExportLogsPartialSuccess {
            rejected_log_records: i64::try_from(rejected).unwrap_or(i64::MAX),
            error_message: OUT_OF_RANGE_TIME_MESSAGE.to_string(),
        });
        Ok(Response::new(ExportLogsServiceResponse { partial_success }))
    }
}

/// Builds the OpenTelemetry logs ingest service.
///
/// Exposed so the handler can be driven directly (e.g. in tests) without a gRPC transport;
/// production wiring goes through [`create_logs_service`].
#[must_use]
pub fn build_logs_service(
    datafusion: Arc<dyn QueryEngine>,
    runtime: Option<Weak<Runtime>>,
) -> LogsIngestService {
    LogsIngestService {
        datafusion,
        runtime,
        once_tracer: OnceTracer::new(),
        store_tables_override: None,
    }
}

/// Creates the OpenTelemetry `LogsService` server for the Flight gRPC port.
#[must_use]
pub fn create_logs_service(
    datafusion: Arc<dyn QueryEngine>,
    runtime: Option<Weak<Runtime>>,
) -> LogsServiceServer<LogsIngestService> {
    LogsServiceServer::new(build_logs_service(datafusion, runtime))
        .accept_compressed(CompressionEncoding::Gzip)
        .max_decoding_message_size(MAX_LOGS_MESSAGE_BYTES)
}

#[cfg(test)]
mod tests {
    use super::*;
    use app::spicepod::component::runtime::ApiKey;
    use arrow::array::{Array, AsArray};
    use arrow::datatypes::{Int32Type, TimestampNanosecondType, UInt32Type};
    use datafusion::datasource::TableProvider;
    use datafusion::error::DataFusionError;
    use datafusion::execution::SendableRecordBatchStream;
    use datafusion::logical_expr::LogicalPlan;
    use datafusion::prelude::SessionContext;
    use opentelemetry_proto::tonic::common::v1::{ArrayValue, InstrumentationScope, KeyValueList};
    use opentelemetry_proto::tonic::logs::v1::{ResourceLogs, ScopeLogs};
    use opentelemetry_proto::tonic::resource::v1::Resource;
    use parking_lot::Mutex;
    use runtime_auth::{AuthPrincipalRef, AuthRequestContext};
    use runtime_query_engine::query_engine::Error as QueryEngineError;
    use runtime_query_engine::query_engine::QueryRequest;
    use runtime_query_engine::query_engine::Result as QueryEngineResult;
    use runtime_request_context::{Protocol, RequestContext, RequestContextBuilder};

    const RECEIVED: u64 = 1_700_000_000_000_000_000;

    fn kv(key: &str, value: any_value::Value) -> KeyValue {
        KeyValue {
            key: key.to_string(),
            value: Some(AnyValue { value: Some(value) }),
            key_strindex: 0,
        }
    }

    fn s(key: &str, value: &str) -> KeyValue {
        kv(key, any_value::Value::StringValue(value.to_string()))
    }

    fn string_body(body: &str) -> AnyValue {
        AnyValue {
            value: Some(any_value::Value::StringValue(body.to_string())),
        }
    }

    fn request_with(
        resource: Vec<KeyValue>,
        scope: Option<InstrumentationScope>,
        log_records: Vec<LogRecord>,
    ) -> ExportLogsServiceRequest {
        ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                resource: Some(Resource {
                    attributes: resource,
                    dropped_attributes_count: 0,
                    entity_refs: vec![],
                }),
                scope_logs: vec![ScopeLogs {
                    scope,
                    log_records,
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        }
    }

    fn single_batch(request: &ExportLogsServiceRequest) -> RecordBatch {
        let LogsBatches {
            mut batches,
            rejected,
        } = export_to_batches(request, RECEIVED, usize::MAX).expect("batches");
        assert_eq!(rejected, 0, "no record should be rejected");
        assert_eq!(batches.len(), 1, "expected one batch");
        batches.pop().expect("one batch")
    }

    fn timestamps(batch: &RecordBatch, column: &str) -> Vec<Option<i64>> {
        batch
            .column_by_name(column)
            .expect("column")
            .as_primitive::<TimestampNanosecondType>()
            .iter()
            .collect()
    }

    fn strings(batch: &RecordBatch, column: &str) -> Vec<Option<String>> {
        batch
            .column_by_name(column)
            .expect("column")
            .as_string::<i32>()
            .iter()
            .map(|v| v.map(ToString::to_string))
            .collect()
    }

    fn full_record() -> LogRecord {
        LogRecord {
            time_unix_nano: 1_000,
            observed_time_unix_nano: 2_000,
            severity_number: 17,
            severity_text: "ERROR".to_string(),
            body: Some(string_body("connection reset by peer")),
            attributes: vec![
                s("http.route", "/checkout"),
                kv("attempt", any_value::Value::IntValue(3)),
            ],
            dropped_attributes_count: 0,
            flags: 1,
            trace_id: vec![
                0x5b, 0x8e, 0xff, 0xf7, 0x98, 0x03, 0x81, 0x03, 0xd2, 0x69, 0xb6, 0x33, 0x81, 0x3f,
                0xc6, 0x0c,
            ],
            span_id: vec![0xee, 0xe1, 0x9b, 0x7e, 0xc3, 0xc1, 0xb1, 0x74],
            event_name: "checkout.failed".to_string(),
        }
    }

    #[test]
    fn every_log_record_field_lands_in_its_column() {
        let mut request = request_with(
            vec![s("service.name", "checkout"), s("host.name", "h1")],
            Some(InstrumentationScope {
                name: "app.logger".to_string(),
                version: "2.1".to_string(),
                attributes: vec![s("scope.attr", "x")],
                dropped_attributes_count: 0,
            }),
            vec![full_record()],
        );
        request.resource_logs[0].schema_url = "https://opentelemetry.io/schemas/1.26.0".to_string();
        request.resource_logs[0].scope_logs[0].schema_url =
            "https://opentelemetry.io/schemas/1.24.0".to_string();
        let batch = single_batch(&request);
        assert_eq!(batch.schema(), *LOGS_STORE_SCHEMA);
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(timestamps(&batch, "timestamp"), vec![Some(1_000)]);
        assert_eq!(timestamps(&batch, "observed_timestamp"), vec![Some(2_000)]);
        let severity = batch
            .column_by_name("severity_number")
            .expect("column")
            .as_primitive::<Int32Type>();
        assert_eq!(severity.value(0), 17);
        assert_eq!(strings(&batch, "severity_text"), vec![Some("ERROR".into())]);
        assert_eq!(
            strings(&batch, "body"),
            vec![Some("connection reset by peer".into())]
        );
        assert_eq!(
            strings(&batch, "attributes"),
            vec![Some(r#"{"attempt":3,"http.route":"/checkout"}"#.into())]
        );
        assert_eq!(
            strings(&batch, "trace_id"),
            vec![Some("5b8efff798038103d269b633813fc60c".into())]
        );
        assert_eq!(
            strings(&batch, "span_id"),
            vec![Some("eee19b7ec3c1b174".into())]
        );
        let flags = batch
            .column_by_name("flags")
            .expect("column")
            .as_primitive::<UInt32Type>();
        assert_eq!(flags.value(0), 1);
        assert_eq!(
            strings(&batch, "event_name"),
            vec![Some("checkout.failed".into())]
        );
        assert_eq!(
            strings(&batch, "service_name"),
            vec![Some("checkout".into())]
        );
        assert_eq!(
            strings(&batch, "resource_attributes"),
            vec![Some(
                r#"{"host.name":"h1","service.name":"checkout"}"#.into()
            )]
        );
        assert_eq!(
            strings(&batch, "scope_name"),
            vec![Some("app.logger".into())]
        );
        assert_eq!(strings(&batch, "scope_version"), vec![Some("2.1".into())]);
        assert_eq!(
            strings(&batch, "scope_attributes"),
            vec![Some(r#"{"scope.attr":"x"}"#.into())]
        );
        assert_eq!(
            strings(&batch, "resource_schema_url"),
            vec![Some("https://opentelemetry.io/schemas/1.26.0".into())]
        );
        assert_eq!(
            strings(&batch, "scope_schema_url"),
            vec![Some("https://opentelemetry.io/schemas/1.24.0".into())]
        );
    }

    #[test]
    fn unset_fields_are_null_and_time_falls_back() {
        let record = LogRecord::default();
        let observed_only = LogRecord {
            observed_time_unix_nano: 2_000,
            ..LogRecord::default()
        };
        let request = request_with(vec![], None, vec![record, observed_only]);
        let batch = single_batch(&request);
        // No time: the observed time; neither: the time Spice received the export.
        assert_eq!(
            timestamps(&batch, "timestamp"),
            vec![Some(i64::try_from(RECEIVED).expect("fits")), Some(2_000)]
        );
        assert_eq!(
            timestamps(&batch, "observed_timestamp"),
            vec![None, Some(2_000)]
        );
        for column in [
            "severity_number",
            "severity_text",
            "body",
            "trace_id",
            "span_id",
            "event_name",
            "service_name",
            "scope_name",
            "scope_version",
            "resource_schema_url",
            "scope_schema_url",
        ] {
            let array = batch.column_by_name(column).expect("column");
            assert_eq!(array.null_count(), 2, "{column} must be null when unset");
        }
        assert_eq!(strings(&batch, "attributes"), vec![Some("{}".into()); 2]);
        assert_eq!(
            strings(&batch, "resource_attributes"),
            vec![Some("{}".into()); 2]
        );
        assert_eq!(
            strings(&batch, "scope_attributes"),
            vec![Some("{}".into()); 2]
        );
    }

    #[test]
    fn a_string_body_is_stored_as_is_and_any_other_body_as_json() {
        let body = |value: any_value::Value| LogRecord {
            time_unix_nano: 1,
            body: Some(AnyValue { value: Some(value) }),
            ..LogRecord::default()
        };
        let records = vec![
            body(any_value::Value::StringValue(
                r#"{"looks":"like json"}"#.to_string(),
            )),
            body(any_value::Value::StringValue(String::new())),
            body(any_value::Value::IntValue(7)),
            body(any_value::Value::DoubleValue(1.5)),
            body(any_value::Value::BoolValue(true)),
            body(any_value::Value::BytesValue(vec![1, 2])),
            body(any_value::Value::DoubleValue(f64::NAN)),
            body(any_value::Value::ArrayValue(ArrayValue {
                values: vec![
                    AnyValue {
                        value: Some(any_value::Value::IntValue(1)),
                    },
                    AnyValue {
                        value: Some(any_value::Value::StringValue("a".to_string())),
                    },
                ],
            })),
            body(any_value::Value::KvlistValue(KeyValueList {
                values: vec![s("z", "last"), kv("a", any_value::Value::IntValue(1))],
            })),
            body(any_value::Value::StringValueStrindex(4)),
            LogRecord {
                time_unix_nano: 1,
                body: Some(AnyValue { value: None }),
                ..LogRecord::default()
            },
        ];
        let batch = single_batch(&request_with(vec![], None, records));
        assert_eq!(
            strings(&batch, "body"),
            vec![
                Some(r#"{"looks":"like json"}"#.into()),
                Some(String::new()),
                Some("7".into()),
                Some("1.5".into()),
                Some("true".into()),
                Some(r#"{"bytes":"AQI="}"#.into()),
                Some(r#"{"double":"NaN"}"#.into()),
                Some(r#"[1,"a"]"#.into()),
                Some(r#"{"a":1,"z":"last"}"#.into()),
                None,
                None,
            ]
        );
    }

    #[test]
    fn ids_are_lowercase_hex_and_absent_or_zero_ids_are_null() {
        let with_ids = |trace_id: Vec<u8>, span_id: Vec<u8>| LogRecord {
            time_unix_nano: 1,
            trace_id,
            span_id,
            ..LogRecord::default()
        };
        let records = vec![
            with_ids(vec![0xAB; 16], vec![0x0F; 8]),
            with_ids(vec![0; 16], vec![0; 8]),
            with_ids(vec![], vec![]),
            // Malformed lengths are stored as sent.
            with_ids(vec![0x01, 0x02, 0x03], vec![0xFF; 20]),
        ];
        let batch = single_batch(&request_with(vec![], None, records));
        assert_eq!(
            strings(&batch, "trace_id"),
            vec![Some("ab".repeat(16)), None, None, Some("010203".into())]
        );
        assert_eq!(
            strings(&batch, "span_id"),
            vec![Some("0f".repeat(8)), None, None, Some("ff".repeat(20))]
        );
    }

    #[test]
    fn service_name_is_the_last_string_service_name_resource_attribute() {
        let service = |resource: Vec<KeyValue>| {
            let batch = single_batch(&request_with(
                resource,
                None,
                vec![LogRecord {
                    time_unix_nano: 1,
                    ..LogRecord::default()
                }],
            ));
            strings(&batch, "service_name").remove(0)
        };
        assert_eq!(service(vec![s("service.name", "a")]), Some("a".into()));
        assert_eq!(
            service(vec![s("service.name", "a"), s("service.name", "b")]),
            Some("b".into())
        );
        assert_eq!(
            service(vec![
                s("service.name", "a"),
                kv("service.name", any_value::Value::IntValue(1))
            ]),
            None
        );
        assert_eq!(service(vec![s("service.namespace", "a")]), None);
    }

    #[test]
    fn exports_are_split_into_bounded_batches_in_record_order() {
        let record = |time| LogRecord {
            time_unix_nano: time,
            ..LogRecord::default()
        };
        let mut request = request_with(
            vec![s("service.name", "a")],
            None,
            vec![record(1), record(2)],
        );
        // A second scope and a second resource continue the same batches.
        request.resource_logs[0].scope_logs.push(ScopeLogs {
            scope: None,
            log_records: vec![record(3)],
            schema_url: String::new(),
        });
        request.resource_logs.push(ResourceLogs {
            resource: Some(Resource {
                attributes: vec![s("service.name", "b")],
                dropped_attributes_count: 0,
                entity_refs: vec![],
            }),
            scope_logs: vec![ScopeLogs {
                scope: None,
                log_records: vec![record(4), record(5)],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        });
        assert_eq!(log_record_count(&request), 5);
        let batches = export_to_batches(&request, RECEIVED, 2)
            .expect("batches")
            .batches;
        let sizes: Vec<usize> = batches.iter().map(RecordBatch::num_rows).collect();
        assert_eq!(sizes, vec![2, 2, 1]);
        let times: Vec<Option<i64>> = batches
            .iter()
            .flat_map(|b| timestamps(b, "timestamp"))
            .collect();
        assert_eq!(times, vec![Some(1), Some(2), Some(3), Some(4), Some(5)]);
        let services: Vec<Option<String>> = batches
            .iter()
            .flat_map(|b| strings(b, "service_name"))
            .collect();
        assert_eq!(
            services,
            vec![
                Some("a".into()),
                Some("a".into()),
                Some("a".into()),
                Some("b".into()),
                Some("b".into())
            ]
        );
    }

    #[test]
    fn expanded_strings_split_batches_at_the_byte_limit() {
        let request = request_with(vec![], None, vec![LogRecord::default(); 5]);
        // Each empty record adds "{}" to its record, resource and scope attribute columns.
        let batches = export_to_batches_with_limits(&request, RECEIVED, usize::MAX, 12)
            .expect("batches")
            .batches;
        let sizes: Vec<usize> = batches.iter().map(RecordBatch::num_rows).collect();
        assert_eq!(sizes, vec![2, 2, 1]);
    }

    #[test]
    fn a_record_larger_than_the_byte_limit_is_invalid() {
        let request = request_with(vec![], None, vec![LogRecord::default()]);
        let Err(error) = export_to_batches_with_limits(&request, RECEIVED, usize::MAX, 5) else {
            panic!("three empty attribute objects use six string bytes");
        };
        let ArrowError::InvalidArgumentError(message) = error else {
            panic!("an oversized record must be an invalid argument");
        };
        assert_eq!(
            message,
            "The OpenTelemetry logs export contains one record that expands to 6 string bytes, above the per-batch limit of 5 bytes; reduce the record's body or attributes"
        );
    }

    #[tokio::test]
    async fn batch_builder_panics_become_request_errors() {
        let result = run_batch_builder(|| panic!("injected batch-builder failure")).await;
        let Err(status) = result else {
            panic!("a batch-builder panic must fail the request");
        };
        assert_eq!(status.code(), tonic::Code::Internal);
        assert_eq!(status.message(), BATCH_BUILD_FAILED_MESSAGE);
    }

    #[test]
    fn new_attribute_keys_do_not_change_the_schema() {
        let a = request_with(vec![], None, vec![full_record()]);
        let mut wider = full_record();
        wider.attributes.push(s("brand_new_key", "x"));
        let b = request_with(vec![s("new.resource.key", "y")], None, vec![wider]);
        assert_eq!(single_batch(&a).schema(), single_batch(&b).schema());
    }

    #[test]
    fn an_export_with_no_records_builds_no_batch() {
        let request = request_with(vec![s("service.name", "a")], None, vec![]);
        assert_eq!(log_record_count(&request), 0);
        assert!(
            export_to_batches(&request, RECEIVED, MAX_LOGS_BATCH_ROWS)
                .expect("batches")
                .batches
                .is_empty()
        );
    }

    #[test]
    fn records_with_times_past_2262_are_rejected_not_wrapped() {
        let last = u64::try_from(i64::MAX).expect("fits");
        let record = |time_unix_nano, observed_time_unix_nano| LogRecord {
            time_unix_nano,
            observed_time_unix_nano,
            ..LogRecord::default()
        };
        let request = request_with(
            vec![],
            None,
            vec![
                record(last, 0),
                record(last + 1, 0),
                record(1, last + 1),
                record(0, u64::MAX),
                record(5, 6),
            ],
        );
        let LogsBatches { batches, rejected } =
            export_to_batches(&request, RECEIVED, MAX_LOGS_BATCH_ROWS).expect("batches");
        assert_eq!(rejected, 3);
        let times: Vec<Option<i64>> = batches
            .iter()
            .flat_map(|b| timestamps(b, "timestamp"))
            .collect();
        assert_eq!(times, vec![Some(i64::MAX), Some(5)]);
    }

    fn dataset(yaml: &str) -> SpicepodDataset {
        yaml::from_str(yaml).expect("dataset")
    }

    #[test]
    fn a_logs_dataset_needs_only_its_param() {
        let mut ds = dataset("from: sink:otel_logs\nname: otel_logs\nparams:\n  otlp_logs: all\n");
        apply_dataset_defaults(&mut ds);
        assert_eq!(ds.access, AccessMode::ReadWrite);
        assert_eq!(ds.time_column.as_deref(), Some("timestamp"));
        assert_eq!(ds.time_format, Some(TimeFormat::Timestamptz));
        let acceleration = ds.acceleration.expect("acceleration");
        assert!(acceleration.enabled);
        assert_eq!(acceleration.engine.as_deref(), Some("cayenne"));
        assert_eq!(acceleration.mode, Mode::File);
        assert_eq!(acceleration.refresh_mode, Some(RefreshMode::Append));
    }

    #[test]
    fn explicit_logs_dataset_settings_are_kept() {
        let mut ds = dataset(
            "from: sink:otel_logs\nname: otel_logs\nparams:\n  otlp_logs: ALL\n\
             acceleration:\n  mode: memory\n  retention_period: 7d\n",
        );
        apply_dataset_defaults(&mut ds);
        let acceleration = ds.acceleration.expect("acceleration");
        assert_eq!(acceleration.mode, Mode::Memory);
        assert_eq!(acceleration.retention_period.as_deref(), Some("7d"));
        assert_eq!(acceleration.engine.as_deref(), Some("cayenne"));
        assert_eq!(acceleration.refresh_mode, Some(RefreshMode::Append));

        let mut ds = dataset(
            "from: sink:otel_logs\nname: otel_logs\nparams:\n  otlp_logs: all\n\
             acceleration:\n  engine: duckdb\n  refresh_mode: full\n",
        );
        apply_dataset_defaults(&mut ds);
        let acceleration = ds.acceleration.expect("acceleration");
        assert_eq!(acceleration.engine.as_deref(), Some("duckdb"));
        assert_eq!(acceleration.refresh_mode, Some(RefreshMode::Full));
    }

    #[test]
    fn other_datasets_get_no_logs_defaults() {
        for yaml in [
            "from: sink:otel_metrics\nname: otel_metrics\nparams:\n  otlp_metrics: all\n",
            "from: sink:t\nname: t\nparams:\n  otlp_logs: some\n",
            "from: sink:t\nname: t\n",
        ] {
            let mut ds = dataset(yaml);
            let before = ds.clone();
            apply_dataset_defaults(&mut ds);
            assert_eq!(ds, before, "{yaml}");
        }
    }

    #[test]
    fn store_param_accepts_only_all() {
        assert!(is_otlp_logs_store_param("all"));
        assert!(is_otlp_logs_store_param("ALL"));
        assert!(!is_otlp_logs_store_param(" all "));
        assert!(!is_otlp_logs_store_param("none"));
        assert!(!is_otlp_logs_store_param(""));
    }

    /// Records every write the export path makes, failing writes to `failing` tables.
    struct FakeQueryEngine {
        session: Arc<SessionContext>,
        writable: Vec<String>,
        failing: Vec<String>,
        /// Each accepted write: the table and the batches it carried.
        writes: Mutex<Vec<(String, Vec<RecordBatch>)>>,
    }

    impl std::fmt::Debug for FakeQueryEngine {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("FakeQueryEngine").finish_non_exhaustive()
        }
    }

    impl FakeQueryEngine {
        fn new(writable: &[&str], failing: &[&str]) -> Arc<Self> {
            Arc::new(Self {
                session: Arc::new(SessionContext::new()),
                writable: writable.iter().map(ToString::to_string).collect(),
                failing: failing.iter().map(ToString::to_string).collect(),
                writes: Mutex::new(Vec::new()),
            })
        }
    }

    #[async_trait]
    impl QueryEngine for FakeQueryEngine {
        fn session_context(&self) -> &Arc<SessionContext> {
            &self.session
        }

        async fn get_table(&self, _table_ref: &TableReference) -> Option<Arc<dyn TableProvider>> {
            None
        }

        fn get_table_sync(&self, _table_ref: &TableReference) -> Option<Arc<dyn TableProvider>> {
            None
        }

        fn table_exists(&self, _table_ref: &TableReference) -> bool {
            false
        }

        async fn get_arrow_schema(&self, table_ref: TableReference) -> QueryEngineResult<Schema> {
            Err(QueryEngineError::GetSchema {
                table_ref: table_ref.to_string(),
                source: DataFusionError::Plan("not used by the logs export".to_string()),
            })
        }

        fn get_user_table_names(&self) -> Vec<TableReference> {
            Vec::new()
        }

        fn get_public_table_names(&self) -> QueryEngineResult<Vec<String>> {
            Ok(Vec::new())
        }

        fn is_writable(&self, table_ref: &TableReference) -> bool {
            self.writable.contains(&table_ref.to_string())
        }

        fn is_path_catalog_writable(&self, _table_ref: &TableReference) -> bool {
            false
        }

        async fn execute_query(
            &self,
            _request: QueryRequest,
        ) -> QueryEngineResult<SendableRecordBatchStream> {
            unimplemented!("the logs export path does not run queries")
        }

        async fn execute_plan(
            &self,
            _plan: LogicalPlan,
        ) -> QueryEngineResult<SendableRecordBatchStream> {
            unimplemented!("the logs export path does not run plans")
        }

        async fn write_data(
            &self,
            table_ref: &TableReference,
            schema: Arc<Schema>,
            data: Vec<RecordBatch>,
            update_type: UpdateType,
        ) -> QueryEngineResult<()> {
            assert_eq!(schema, *LOGS_STORE_SCHEMA);
            assert!(matches!(update_type, UpdateType::Append));
            if self.failing.contains(&table_ref.to_string()) {
                return Err(QueryEngineError::WriteData {
                    table_ref: table_ref.to_string(),
                    source: DataFusionError::Plan("write failed".to_string()),
                });
            }
            self.writes.lock().push((table_ref.to_string(), data));
            Ok(())
        }
    }

    fn service(engine: &Arc<FakeQueryEngine>, tables: &[&str]) -> LogsIngestService {
        let mut service = build_logs_service(Arc::clone(engine) as Arc<dyn QueryEngine>, None);
        service.store_tables_override =
            Some(tables.iter().map(|t| TableReference::from(*t)).collect());
        service
    }

    fn records(n: u64) -> ExportLogsServiceRequest {
        request_with(
            vec![s("service.name", "checkout")],
            None,
            (1..=n)
                .map(|time| LogRecord {
                    time_unix_nano: time,
                    ..LogRecord::default()
                })
                .collect(),
        )
    }

    #[tokio::test]
    async fn every_store_dataset_receives_the_whole_export_in_one_write() {
        let engine = FakeQueryEngine::new(&["logs_a", "logs_b"], &[]);
        let service = service(&engine, &["logs_a", "logs_b"]);
        let count = u64::try_from(MAX_LOGS_BATCH_ROWS).expect("fits") + 5;
        let response = service
            .export(Request::new(records(count)))
            .await
            .expect("stored")
            .into_inner();
        assert_eq!(response.partial_success, None);
        let writes = engine.writes.lock();
        let tables: Vec<&str> = writes.iter().map(|(t, _)| t.as_str()).collect();
        assert_eq!(tables, vec!["logs_a", "logs_b"], "one write per dataset");
        for (table, batches) in writes.iter() {
            let sizes: Vec<usize> = batches.iter().map(RecordBatch::num_rows).collect();
            assert_eq!(sizes, vec![MAX_LOGS_BATCH_ROWS, 5], "{table}");
        }
    }

    #[tokio::test]
    async fn no_store_dataset_is_unimplemented_and_names_the_fix() {
        let engine = FakeQueryEngine::new(&[], &[]);
        let service = service(&engine, &[]);
        let status = service
            .export(Request::new(records(3)))
            .await
            .expect_err("nothing can store the export");
        assert_eq!(status.code(), tonic::Code::Unimplemented);
        assert_eq!(status.message(), NO_LOGS_STORE_MESSAGE);
        assert!(engine.writes.lock().is_empty());
    }

    /// An export that arrives while a logs dataset is still loading, as during startup, is
    /// retried rather than dropped, and is not stored in the datasets that already loaded.
    #[tokio::test]
    async fn a_logs_dataset_still_loading_asks_the_exporter_to_retry() {
        let engine = FakeQueryEngine::new(&["logs_a"], &[]);
        let service = service(&engine, &["logs_a", "logs_b"]);
        let status = service
            .export(Request::new(records(3)))
            .await
            .expect_err("logs_b cannot store the export yet");
        assert_eq!(status.code(), tonic::Code::Unavailable);
        assert_eq!(
            status.message(),
            "The OpenTelemetry logs dataset 'logs_b' is not loaded yet, so this export was not stored; retry the export"
        );
        assert!(engine.writes.lock().is_empty());
    }

    #[tokio::test]
    async fn an_export_with_no_records_succeeds_without_a_store() {
        let engine = FakeQueryEngine::new(&[], &[]);
        let service = service(&engine, &[]);
        let response = service
            .export(Request::new(records(0)))
            .await
            .expect("an empty export stores nothing and fails nothing")
            .into_inner();
        assert_eq!(response.partial_success, None);
    }

    #[tokio::test]
    async fn out_of_range_records_are_reported_as_a_partial_success() {
        let engine = FakeQueryEngine::new(&["otel_logs"], &[]);
        let service = service(&engine, &["otel_logs"]);
        let mut request = records(2);
        request.resource_logs[0].scope_logs[0].log_records[0].time_unix_nano = u64::MAX;
        let response = service
            .export(Request::new(request))
            .await
            .expect("one record is stored")
            .into_inner();
        assert_eq!(
            response.partial_success,
            Some(ExportLogsPartialSuccess {
                rejected_log_records: 1,
                error_message: OUT_OF_RANGE_TIME_MESSAGE.to_string(),
            })
        );
        let writes = engine.writes.lock();
        assert_eq!(
            writes[0].1.iter().map(RecordBatch::num_rows).sum::<usize>(),
            1
        );
    }

    #[tokio::test]
    async fn an_export_with_only_out_of_range_records_is_invalid() {
        let engine = FakeQueryEngine::new(&["otel_logs"], &[]);
        let service = service(&engine, &["otel_logs"]);
        let mut request = records(1);
        request.resource_logs[0].scope_logs[0].log_records[0].time_unix_nano = u64::MAX;
        let status = service
            .export(Request::new(request))
            .await
            .expect_err("nothing can be stored");
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        assert_eq!(
            status.message(),
            format!("All 1 log records were rejected. {OUT_OF_RANGE_TIME_MESSAGE}")
        );
        assert!(engine.writes.lock().is_empty());
    }

    #[tokio::test]
    async fn a_failed_store_write_asks_the_exporter_to_retry() {
        let engine = FakeQueryEngine::new(&["otel_logs"], &["otel_logs"]);
        let service = service(&engine, &["otel_logs"]);
        let status = service
            .export(Request::new(records(3)))
            .await
            .expect_err("the write failed");
        assert_eq!(status.code(), tonic::Code::Unavailable);
        assert_eq!(
            status.message(),
            "Failed to write to the OpenTelemetry logs dataset 'otel_logs'; retry the export"
        );
        assert!(engine.writes.lock().is_empty());
    }

    fn context_with_api_key(key: ApiKey) -> Arc<RequestContext> {
        let context = Arc::new(RequestContextBuilder::new(Protocol::Flight).build());
        context
            .set_auth_principal(Arc::new(key) as AuthPrincipalRef)
            .expect("auth principal should be set");
        context
    }

    /// A read-only API key must not persist an OTLP logs export. The logs dataset is writable,
    /// so a missing principal check would store the records.
    #[tokio::test]
    async fn otlp_grpc_read_only_api_key_is_denied_and_writes_nothing() {
        let engine = FakeQueryEngine::new(&["otel_logs"], &[]);
        let service = service(&engine, &["otel_logs"]);

        let status = context_with_api_key(ApiKey::ReadOnly {
            key: "read-only".to_string(),
        })
        .scope(service.export(Request::new(records(3))))
        .await
        .expect_err("a read-only API key must not export logs");

        assert_eq!(status.code(), tonic::Code::PermissionDenied);
        assert_eq!(status.message(), "API key does not allow write access");
        assert!(engine.writes.lock().is_empty());
    }
}
