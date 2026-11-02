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

//! OTLP logs stored in a Cayenne `otlp_logs: all` dataset, read back with SQL.
//!
//! The oracle is the export itself: every row the table returns is compared, column by
//! column, with the log record it came from, rendered here independently of the receiver's
//! builders (hex with `format!`, attributes through `serde_json` with sorted keys). The
//! comparison covers a seeded mix of severities, every body kind, present, absent and
//! all-zero trace ids, records with no time, several resources and scopes, and exports large
//! enough to span several write batches. A restart must keep every acknowledged record and
//! serve it before the next export arrives.
//!
//! The dataset names only its param and where Cayenne keeps its files: `access`, the time
//! column and the rest of the acceleration come from the logs dataset defaults.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;

use app::AppBuilder;
use arrow::array::AsArray;
use arrow::datatypes::{Int32Type, TimestampNanosecondType, UInt32Type};
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::collector::logs::v1::logs_service_server::LogsService;
use opentelemetry_proto::tonic::common::v1::{
    AnyValue, ArrayValue, InstrumentationScope, KeyValue, KeyValueList, any_value::Value,
};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use opentelemetry_proto::tonic::resource::v1::Resource;
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use runtime::Runtime;
use spicepod::acceleration::Acceleration;
use spicepod::component::dataset::Dataset;
use spicepod::param::Params;

use crate::{
    RecordBatch, configure_test_datafusion, init_tracing,
    utils::{register_test_connectors, run_query, runtime_ready_check},
};

const TABLE: &str = "otel_logs";

/// `otlp_logs: all` and Cayenne's file locations; everything else is the logs defaults.
fn logs_dataset(data_dir: &str, metadata_dir: &str) -> Dataset {
    let mut ds = Dataset::new(format!("sink:{TABLE}"), TABLE.to_string());
    ds.params = Some(Params::from_string_map(HashMap::from([(
        "otlp_logs".to_string(),
        "all".to_string(),
    )])));
    ds.acceleration = Some(Acceleration {
        mode: spicepod::acceleration::Mode::File,
        params: Some(Params::from_string_map(HashMap::from([
            ("cayenne_file_path".to_string(), data_dir.to_string()),
            ("cayenne_metadata_dir".to_string(), metadata_dir.to_string()),
        ]))),
        ..Acceleration::default()
    });
    ds
}

async fn start_runtime(ds: &Dataset) -> Arc<Runtime> {
    let app = AppBuilder::new("otel_logs")
        .with_dataset(ds.clone())
        .build();
    configure_test_datafusion();
    let rt = Arc::new(Runtime::builder().with_app(app).build().await);
    let cloned = Arc::clone(&rt);
    tokio::select! {
        () = tokio::time::sleep(Duration::from_mins(1)) => {
            panic!("timed out loading components");
        }
        () = cloned.load_components() => {}
    }
    runtime_ready_check(&rt).await;
    rt
}

/// Sends `request` through the same `LogsService::export` handler the gRPC receiver serves.
async fn export(rt: &Arc<Runtime>, request: ExportLogsServiceRequest) -> Result<(), anyhow::Error> {
    let service =
        runtime::opentelemetry_logs::build_logs_service(rt.datafusion(), Some(Arc::downgrade(rt)));
    let response = service
        .export(tonic::Request::new(request))
        .await
        .map_err(|status| anyhow::anyhow!("OTLP logs export failed: {status}"))?
        .into_inner();
    anyhow::ensure!(
        response.partial_success.is_none(),
        "OTLP logs export reported a partial success: {:?}",
        response.partial_success
    );
    Ok(())
}

fn string(value: &str) -> AnyValue {
    AnyValue {
        value: Some(Value::StringValue(value.to_string())),
    }
}

fn kv(key: &str, value: AnyValue) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(value),
        key_strindex: 0,
    }
}

/// The expected JSON of an attribute value, built with `serde_json` rather than the
/// receiver's encoder.
fn expected_json(value: &AnyValue) -> serde_json::Value {
    match value.value.as_ref() {
        None | Some(Value::StringValueStrindex(_)) => serde_json::Value::Null,
        Some(Value::StringValue(s)) => serde_json::json!(s),
        Some(Value::BoolValue(b)) => serde_json::json!(b),
        Some(Value::IntValue(i)) => serde_json::json!(i),
        Some(Value::DoubleValue(d)) => serde_json::json!(d),
        Some(Value::BytesValue(b)) => {
            use base64::Engine;
            serde_json::json!({ "bytes": base64::engine::general_purpose::STANDARD.encode(b) })
        }
        Some(Value::ArrayValue(a)) => {
            serde_json::Value::Array(a.values.iter().map(expected_json).collect())
        }
        Some(Value::KvlistValue(kv)) => expected_attributes(&kv.values),
    }
}

fn expected_attributes(attributes: &[KeyValue]) -> serde_json::Value {
    // A BTreeMap sorts the keys; a later repeat of a key replaces the earlier one.
    let map: BTreeMap<String, serde_json::Value> = attributes
        .iter()
        .map(|kv| {
            (
                kv.key.clone(),
                kv.value
                    .as_ref()
                    .map_or(serde_json::Value::Null, expected_json),
            )
        })
        .collect();
    serde_json::Value::Object(map.into_iter().collect())
}

fn hex(bytes: &[u8]) -> Option<String> {
    use std::fmt::Write as _;
    (!bytes.iter().all(|b| *b == 0)).then(|| {
        bytes.iter().fold(String::new(), |mut hex, b| {
            let _ = write!(hex, "{b:02x}");
            hex
        })
    })
}

/// One row as the table should return it.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct ExpectedRow {
    timestamp: Option<i64>,
    observed_timestamp: Option<i64>,
    severity_number: Option<i32>,
    severity_text: Option<String>,
    body: Option<String>,
    attributes: Option<String>,
    trace_id: Option<String>,
    span_id: Option<String>,
    flags: Option<u32>,
    event_name: Option<String>,
    service_name: Option<String>,
    resource_attributes: Option<String>,
    resource_schema_url: Option<String>,
    scope_name: Option<String>,
    scope_version: Option<String>,
    scope_attributes: Option<String>,
    scope_schema_url: Option<String>,
}

fn non_empty(s: &str) -> Option<String> {
    (!s.is_empty()).then(|| s.to_string())
}

fn expected_rows(request: &ExportLogsServiceRequest) -> Vec<ExpectedRow> {
    let mut rows = Vec::new();
    for resource_logs in &request.resource_logs {
        let resource = resource_logs
            .resource
            .as_ref()
            .map_or(&[][..], |r| r.attributes.as_slice());
        let service_name = resource
            .iter()
            .rev()
            .find(|kv| kv.key == "service.name")
            .and_then(
                |kv| match kv.value.as_ref().and_then(|v| v.value.as_ref()) {
                    Some(Value::StringValue(s)) => Some(s.clone()),
                    _ => None,
                },
            );
        for scope_logs in &resource_logs.scope_logs {
            let scope = scope_logs.scope.as_ref();
            for record in &scope_logs.log_records {
                // Every record in these exports has a time or an observed time; the
                // receive-time fallback is pinned by the receiver's unit tests.
                let time = if record.time_unix_nano == 0 {
                    record.observed_time_unix_nano
                } else {
                    record.time_unix_nano
                };
                let nanos = |t: u64| i64::try_from(t).expect("test times fit a timestamp");
                rows.push(ExpectedRow {
                    timestamp: Some(nanos(time)),
                    observed_timestamp: (record.observed_time_unix_nano != 0)
                        .then(|| nanos(record.observed_time_unix_nano)),
                    severity_number: (record.severity_number != 0)
                        .then_some(record.severity_number),
                    severity_text: non_empty(&record.severity_text),
                    body: record.body.as_ref().and_then(|b| match b.value.as_ref() {
                        None | Some(Value::StringValueStrindex(_)) => None,
                        Some(Value::StringValue(s)) => Some(s.clone()),
                        Some(_) => Some(expected_json(b).to_string()),
                    }),
                    attributes: Some(expected_attributes(&record.attributes).to_string()),
                    trace_id: hex(&record.trace_id),
                    span_id: hex(&record.span_id),
                    flags: Some(record.flags),
                    event_name: non_empty(&record.event_name),
                    service_name: service_name.clone(),
                    resource_attributes: Some(expected_attributes(resource).to_string()),
                    resource_schema_url: non_empty(&resource_logs.schema_url),
                    scope_name: scope.and_then(|s| non_empty(&s.name)),
                    scope_version: scope.and_then(|s| non_empty(&s.version)),
                    scope_attributes: Some(
                        expected_attributes(scope.map_or(&[][..], |s| s.attributes.as_slice()))
                            .to_string(),
                    ),
                    scope_schema_url: non_empty(&scope_logs.schema_url),
                });
            }
        }
    }
    rows
}

fn string_column(batch: &RecordBatch, name: &str) -> Vec<Option<String>> {
    batch
        .column_by_name(name)
        .unwrap_or_else(|| panic!("column {name}"))
        .as_string::<i32>()
        .iter()
        .map(|v| v.map(ToString::to_string))
        .collect()
}

fn actual_rows(batches: &[RecordBatch]) -> Vec<ExpectedRow> {
    let mut rows = Vec::new();
    for batch in batches {
        let timestamps = |name: &str| -> Vec<Option<i64>> {
            batch
                .column_by_name(name)
                .unwrap_or_else(|| panic!("column {name}"))
                .as_primitive::<TimestampNanosecondType>()
                .iter()
                .collect()
        };
        let time = timestamps("timestamp");
        let observed = timestamps("observed_timestamp");
        let severity: Vec<Option<i32>> = batch
            .column_by_name("severity_number")
            .expect("severity_number")
            .as_primitive::<Int32Type>()
            .iter()
            .collect();
        let flags: Vec<Option<u32>> = batch
            .column_by_name("flags")
            .expect("flags")
            .as_primitive::<UInt32Type>()
            .iter()
            .collect();
        let columns: HashMap<&str, Vec<Option<String>>> = [
            "severity_text",
            "body",
            "attributes",
            "trace_id",
            "span_id",
            "event_name",
            "service_name",
            "resource_attributes",
            "resource_schema_url",
            "scope_name",
            "scope_version",
            "scope_attributes",
            "scope_schema_url",
        ]
        .into_iter()
        .map(|name| (name, string_column(batch, name)))
        .collect();
        for i in 0..batch.num_rows() {
            let s = |name: &str| columns[name][i].clone();
            rows.push(ExpectedRow {
                timestamp: time[i],
                observed_timestamp: observed[i],
                severity_number: severity[i],
                severity_text: s("severity_text"),
                body: s("body"),
                attributes: s("attributes"),
                trace_id: s("trace_id"),
                span_id: s("span_id"),
                flags: flags[i],
                event_name: s("event_name"),
                service_name: s("service_name"),
                resource_attributes: s("resource_attributes"),
                resource_schema_url: s("resource_schema_url"),
                scope_name: s("scope_name"),
                scope_version: s("scope_version"),
                scope_attributes: s("scope_attributes"),
                scope_schema_url: s("scope_schema_url"),
            });
        }
    }
    rows
}

/// A seeded export of `records` log records over two resources and three scopes, starting
/// at `first_time` and using every body kind and id shape.
fn seeded_export(rng: &mut StdRng, first_time: u64, records: usize) -> ExportLogsServiceRequest {
    let bodies = |rng: &mut StdRng, i: usize| -> Option<AnyValue> {
        match rng.random_range(0..9) {
            0 => None,
            1 => Some(string("")),
            2 => Some(string(&format!(
                "request {i} failed: connection reset by peer"
            ))),
            3 => Some(AnyValue {
                value: Some(Value::IntValue(rng.random_range(-1_000..1_000))),
            }),
            4 => Some(AnyValue {
                value: Some(Value::DoubleValue(
                    f64::from(rng.random_range(0_u32..1_000)) / 8.0,
                )),
            }),
            5 => Some(AnyValue {
                value: Some(Value::BoolValue(rng.random_bool(0.5))),
            }),
            6 => Some(AnyValue {
                value: Some(Value::BytesValue(vec![rng.random(), rng.random()])),
            }),
            7 => Some(AnyValue {
                value: Some(Value::ArrayValue(ArrayValue {
                    values: vec![string("a"), string(&i.to_string())],
                })),
            }),
            _ => Some(AnyValue {
                value: Some(Value::KvlistValue(KeyValueList {
                    values: vec![kv("z", string("last")), kv("a", string(&i.to_string()))],
                })),
            }),
        }
    };
    let mut time = first_time;
    let mut next_record = |rng: &mut StdRng, i: usize| {
        time += 1;
        let no_time = rng.random_bool(0.1);
        LogRecord {
            time_unix_nano: if no_time { 0 } else { time },
            observed_time_unix_nano: if no_time || rng.random_bool(0.5) {
                time + 1_000
            } else {
                0
            },
            severity_number: rng.random_range(0..=24),
            severity_text: if rng.random_bool(0.8) {
                ["TRACE", "DEBUG", "INFO", "WARN", "ERROR", "FATAL"][rng.random_range(0..6)]
                    .to_string()
            } else {
                String::new()
            },
            body: bodies(rng, i),
            attributes: (0..rng.random_range(0..4))
                .map(|k| kv(&format!("attr.{k}"), string(&format!("v{i}"))))
                .collect(),
            dropped_attributes_count: 0,
            flags: rng.random_range(0..2),
            trace_id: match rng.random_range(0..3) {
                0 => Vec::new(),
                1 => vec![0; 16],
                _ => (0..16).map(|_| rng.random()).collect(),
            },
            span_id: match rng.random_range(0..3) {
                0 => Vec::new(),
                1 => vec![0; 8],
                _ => (0..8).map(|_| rng.random()).collect(),
            },
            event_name: if rng.random_bool(0.2) {
                "app.event".to_string()
            } else {
                String::new()
            },
        }
    };
    let scopes = [
        Some(InstrumentationScope {
            name: "app.logger".to_string(),
            version: "1.2".to_string(),
            attributes: vec![kv("scope.k", string("v"))],
            dropped_attributes_count: 0,
        }),
        None,
        Some(InstrumentationScope {
            name: "db".to_string(),
            version: String::new(),
            attributes: vec![],
            dropped_attributes_count: 0,
        }),
    ];
    let resources = [
        vec![
            kv("service.name", string("checkout")),
            kv("host.name", string("h1")),
        ],
        vec![
            kv("service.name", string("cart")),
            kv(
                "process.pid",
                AnyValue {
                    value: Some(Value::IntValue(42)),
                },
            ),
        ],
    ];
    let mut request = ExportLogsServiceRequest::default();
    let mut i = 0;
    // `records` spread over the 2 x 3 resource and scope slots, the first slots taking the
    // remainder.
    let slots = resources.len() * scopes.len();
    let mut slot = 0;
    for resource in resources {
        let mut scope_logs = Vec::new();
        for scope in &scopes {
            let n = records / slots + usize::from(slot < records % slots);
            slot += 1;
            let log_records = (0..n)
                .map(|_| {
                    i += 1;
                    next_record(rng, i)
                })
                .collect();
            scope_logs.push(ScopeLogs {
                scope: scope.clone(),
                log_records,
                schema_url: if scope.is_some() {
                    "https://opentelemetry.io/schemas/1.24.0".to_string()
                } else {
                    String::new()
                },
            });
        }
        request.resource_logs.push(ResourceLogs {
            resource: Some(Resource {
                attributes: resource,
                dropped_attributes_count: 0,
                entity_refs: vec![],
            }),
            scope_logs,
            schema_url: "https://opentelemetry.io/schemas/1.26.0".to_string(),
        });
    }
    request
}

/// Rows in a total order over every column, so expected and actual compare as multisets.
fn sorted(mut rows: Vec<ExpectedRow>) -> Vec<ExpectedRow> {
    rows.sort();
    rows
}

const SELECT_ALL: &str = "SELECT timestamp, observed_timestamp, severity_number, \
    severity_text, body, attributes, trace_id, span_id, flags, event_name, service_name, \
    resource_attributes, resource_schema_url, scope_name, scope_version, scope_attributes, \
    scope_schema_url FROM otel_logs";

async fn stored_rows(rt: &Arc<Runtime>) -> Result<Vec<ExpectedRow>, anyhow::Error> {
    Ok(sorted(actual_rows(&run_query(rt, SELECT_ALL).await?)))
}

#[tokio::test]
async fn otlp_logs_round_trip_through_cayenne_and_survive_a_restart() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    let seed: u64 = std::env::var("OTEL_LOGS_TEST_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0x5eed_1095);
    eprintln!("seed = {seed}");
    let mut rng = StdRng::seed_from_u64(seed);

    let temp_dir = tempfile::tempdir()?;
    let data_dir = temp_dir.path().join("data").to_string_lossy().to_string();
    let metadata_dir = temp_dir
        .path()
        .join("metadata")
        .to_string_lossy()
        .to_string();
    let ds = logs_dataset(&data_dir, &metadata_dir);

    // Exports of 1 record, a typical SDK batch, and one past a write batch (8,192 rows).
    let exports: Vec<ExportLogsServiceRequest> = [1_usize, 512, 9_000]
        .into_iter()
        .enumerate()
        .map(|(n, records)| {
            seeded_export(
                &mut rng,
                1_700_000_000_000_000_000 + (n as u64) * 1_000_000,
                records,
            )
        })
        .collect();
    let expected = sorted(exports.iter().flat_map(expected_rows).collect());
    assert_eq!(expected.len(), 1 + 512 + 9_000);

    {
        register_test_connectors().await;
        let rt = start_runtime(&ds).await;

        // The schema is fixed, so the dataset is registered and queryable before any export.
        let before = stored_rows(&rt).await.map_err(|e| {
            anyhow::anyhow!("the logs dataset must be queryable before its first export: {e}")
        })?;
        assert_eq!(before, Vec::new());

        for request in exports {
            export(&rt, request).await?;
        }
        let actual = stored_rows(&rt).await?;
        assert_eq!(actual.len(), expected.len(), "row count");
        for (i, (actual, expected)) in actual.iter().zip(&expected).enumerate() {
            assert_eq!(actual, expected, "row {i} (seed {seed})");
        }

        // The time column filters as a timestamp, with no conversion function.
        let cutoff = expected[expected.len() / 2].timestamp.expect("time");
        let in_range = expected
            .iter()
            .filter(|row| row.timestamp.is_some_and(|t| t >= cutoff))
            .count();
        let counted = run_query(
            &rt,
            &format!(
                "SELECT count(*) FROM otel_logs WHERE timestamp >= to_timestamp_nanos({cutoff})"
            ),
        )
        .await?;
        assert_eq!(
            counted[0]
                .column(0)
                .as_primitive::<arrow::datatypes::Int64Type>()
                .value(0),
            i64::try_from(in_range).expect("fits"),
            "time-range count"
        );

        rt.shutdown().await;
        drop(rt);
    }

    // Every acknowledged record survives a restart and is served before the next export.
    {
        register_test_connectors().await;
        let rt = start_runtime(&ds).await;
        let actual = stored_rows(&rt).await?;
        assert_eq!(actual.len(), expected.len(), "row count after restart");
        assert!(
            actual == expected,
            "rows after restart differ (seed {seed})"
        );

        // And the restarted dataset still appends.
        let more = seeded_export(&mut rng, 1_800_000_000_000_000_000, 10);
        let mut expected_more = expected.clone();
        expected_more.extend(expected_rows(&more));
        export(&rt, more).await?;
        assert_eq!(stored_rows(&rt).await?, sorted(expected_more));

        rt.shutdown().await;
        drop(rt);
    }
    Ok(())
}

/// A logs dataset needs Cayenne: with another engine it fails to load with the fix, and the
/// receiver refuses exports as retryable while it is not loaded.
#[tokio::test]
async fn otlp_logs_dataset_without_cayenne_fails_to_load() -> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));
    let temp_dir = tempfile::tempdir()?;
    let mut ds = logs_dataset(
        &temp_dir.path().join("data").to_string_lossy(),
        &temp_dir.path().join("metadata").to_string_lossy(),
    );
    if let Some(acceleration) = ds.acceleration.as_mut() {
        acceleration.engine = Some("arrow".to_string());
        acceleration.mode = spicepod::acceleration::Mode::Memory;
        acceleration.params = None;
    }

    register_test_connectors().await;
    let app = AppBuilder::new("otel_logs_no_cayenne")
        .with_dataset(ds)
        .build();
    configure_test_datafusion();
    let rt = Arc::new(Runtime::builder().with_app(app).build().await);
    let load = tokio::spawn(Arc::clone(&rt).load_components());
    let name = datafusion::common::TableReference::from(TABLE);
    let refused = crate::utils::wait_until_true(Duration::from_secs(30), || async {
        matches!(
            rt.status().get_dataset_statuses().get(&name),
            Some(runtime::status::ComponentStatus::Error(_))
        )
    })
    .await;
    let status = rt.status().get_dataset_statuses().get(&name).cloned();
    assert!(refused, "the dataset should fail to load, got {status:?}");
    let Some(runtime::status::ComponentStatus::Error(Some(message))) = status else {
        panic!("expected an error message, got {status:?}");
    };
    assert!(
        message.contains(
            "An OpenTelemetry logs dataset (`otlp_logs: all`) is stored by Cayenne. Set `acceleration.enabled: true` and `acceleration.engine: cayenne`."
        ),
        "{message}"
    );

    // The dataset is configured but not loaded, so an export is refused as retryable: the
    // exporter keeps it while the configuration is fixed, instead of dropping it.
    let service =
        runtime::opentelemetry_logs::build_logs_service(rt.datafusion(), Some(Arc::downgrade(&rt)));
    let status = service
        .export(tonic::Request::new(seeded_export(
            &mut StdRng::seed_from_u64(1),
            1,
            3,
        )))
        .await
        .expect_err("the logs dataset is not loaded");
    assert_eq!(status.code(), tonic::Code::Unavailable);
    assert_eq!(
        status.message(),
        "The OpenTelemetry logs dataset 'otel_logs' is not loaded yet, so this export was not stored; retry the export"
    );

    load.abort();
    rt.shutdown().await;
    Ok(())
}
