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

//! Regression tests for races and deadlocks on the OpenTelemetry-metric ingest path into
//! `from: sink:` datasets with file-based (Cayenne) acceleration, mirroring the production
//! OTLP metric dataset shape (`refresh_mode: append`, `primary_key: time_unix_nano`,
//! `on_schema_change: append_new_columns`, `access: read_write`).
//!
//! Five flows are covered:
//!
//! - **Concurrent first writes.** A sink dataset is registered by its first write. One
//!   writer does that while the rest wait, instead of looking the table up before it exists
//!   and failing.
//! - **Concurrent exports adding different columns.** Adding a column replaces the table's
//!   provider. An export whose batch predates that is rejected by the schema check, and must
//!   rebuild and retry rather than lose its data points.
//! - **A restart whose first export adds a column.** The dataset has no table until its
//!   first write, so it must be registered from the acceleration checkpoint first.
//! - **A first write that names the dataset differently.** A qualified name, as a Flight
//!   `DoPut` produces, must still find and register the dataset.
//! - **More exports adding columns at once than a write gets retries.** A write must not have
//!   to retry once per column another write added behind its back.
//!
//! Each concurrent phase has a timeout, so a deadlock fails the test instead of hanging it.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use app::AppBuilder;
use arrow::array::{Float64Array, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use datafusion::sql::TableReference;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::collector::metrics::v1::metrics_service_server::MetricsService;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value::Value as AnyVal};
use opentelemetry_proto::tonic::metrics::v1::{
    Gauge, Metric as OtlpMetric, NumberDataPoint, ResourceMetrics, ScopeMetrics, metric::Data,
    number_data_point::Value as NumberValue,
};
use runtime::Runtime;
use runtime::dataupdate::{DataUpdate, UpdateType};
use spicepod::acceleration::{Acceleration, Mode, RefreshMode};
use spicepod::component::access::AccessMode;
use spicepod::component::dataset::{Dataset, OnSchemaChange, TimeFormat};
use spicepod::param::Params;

use crate::{
    RecordBatch, configure_test_datafusion, init_tracing,
    utils::{register_test_connectors, run_query, runtime_ready_check},
};

/// The dataset shape production OTLP metrics use: a `sink` source with Cayenne file
/// acceleration, keyed on the data point time, that accepts new columns.
fn make_dataset(metric: &str, data_dir: &str, metadata_dir: &str) -> Dataset {
    let mut ds = Dataset::new(format!("sink:{metric}"), metric.to_string());
    ds.access = AccessMode::ReadWrite;
    ds.on_schema_change = OnSchemaChange::AppendNewColumns;
    ds.time_column = Some("time_unix_nano".to_string());
    ds.time_format = Some(TimeFormat::UnixNanos);
    ds.acceleration = Some(Acceleration {
        enabled: true,
        engine: Some("cayenne".to_string()),
        mode: Mode::File,
        refresh_mode: Some(RefreshMode::Append),
        primary_key: Some("time_unix_nano".to_string()),
        params: Some(Params::from_string_map(HashMap::from([
            ("cayenne_file_path".to_string(), data_dir.to_string()),
            ("cayenne_metadata_dir".to_string(), metadata_dir.to_string()),
        ]))),
        ..Acceleration::default()
    });
    ds
}

async fn start_runtime(ds: &Dataset) -> Arc<Runtime> {
    let app = AppBuilder::new("otel_ingest_races")
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

fn string_attr(key: &str, value: &str) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue {
            value: Some(AnyVal::StringValue(value.to_string())),
        }),
        ..Default::default()
    }
}

/// A single-data-point gauge export for `metric` at `time_unix_nano` carrying `attrs` as
/// dimensions.
fn gauge_export(
    metric: &str,
    value: f64,
    time_unix_nano: u64,
    attrs: Vec<KeyValue>,
) -> ExportMetricsServiceRequest {
    ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            resource: None,
            scope_metrics: vec![ScopeMetrics {
                scope: None,
                metrics: vec![OtlpMetric {
                    name: metric.to_string(),
                    description: String::new(),
                    unit: String::new(),
                    metadata: Vec::new(),
                    data: Some(Data::Gauge(Gauge {
                        data_points: vec![NumberDataPoint {
                            attributes: attrs,
                            start_time_unix_nano: 0,
                            time_unix_nano,
                            value: Some(NumberValue::AsDouble(value)),
                            exemplars: Vec::new(),
                            flags: 0,
                        }],
                    })),
                }],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    }
}

/// Drive an export through the real OTLP ingest handler (the same `export` the gRPC server
/// serves), failing if any data point was rejected.
async fn ingest(rt: &Arc<Runtime>, req: ExportMetricsServiceRequest) -> Result<(), anyhow::Error> {
    let service =
        runtime::opentelemetry::build_metrics_service(rt.datafusion(), Some(Arc::downgrade(rt)));

    let response = service
        .export(tonic::Request::new(req))
        .await
        .map_err(|status| anyhow::anyhow!("OTLP export returned an error status: {status}"))?
        .into_inner();

    if let Some(partial) = response.partial_success
        && partial.rejected_data_points > 0
    {
        anyhow::bail!(
            "OTLP export rejected {} data point(s): {}",
            partial.rejected_data_points,
            partial.error_message
        );
    }
    Ok(())
}

async fn row_count(rt: &Arc<Runtime>, metric: &str) -> Result<usize, anyhow::Error> {
    let rows = run_query(rt, &format!("SELECT time_unix_nano FROM {metric}")).await?;
    Ok(rows.iter().map(RecordBatch::num_rows).sum())
}

/// A sink dataset has no table until its first write creates one. Concurrent first writes
/// race that: one does the work and the others must wait for it, or they fail with a
/// missing-table error and lose their data points. The timeout also catches a deadlock here.
#[cfg(not(windows))]
#[tokio::test]
async fn concurrent_first_writes_to_a_parked_sink_all_land() -> Result<(), anyhow::Error> {
    const METRIC: &str = "otel_race_first_write";
    const WRITERS: u64 = 8;

    let _tracing = init_tracing(Some("integration=debug,info"));

    let temp_dir = tempfile::tempdir()?;
    let data_dir = temp_dir.path().join("data").to_string_lossy().to_string();
    let metadata_dir = temp_dir
        .path()
        .join("metadata")
        .to_string_lossy()
        .to_string();
    let ds = make_dataset(METRIC, &data_dir, &metadata_dir);

    register_test_connectors().await;
    let rt = start_runtime(&ds).await;

    let mut tasks = tokio::task::JoinSet::new();
    for i in 0..WRITERS {
        let rt = Arc::clone(&rt);
        tasks.spawn(async move {
            ingest(
                &rt,
                gauge_export(METRIC, 1.0, 100 + i, vec![string_attr("region", "us")]),
            )
            .await
        });
    }

    let results = tokio::time::timeout(Duration::from_mins(2), tasks.join_all())
        .await
        .map_err(|_| {
            anyhow::anyhow!(
                "timed out waiting for concurrent first writes — possible deadlock in the sink registration path"
            )
        })?;
    for result in results {
        result?;
    }

    let n = u64::try_from(row_count(&rt, METRIC).await?)?;
    assert_eq!(
        n, WRITERS,
        "every concurrent first write must land; a missing row means a writer lost the \
         registration race and was rejected"
    );

    rt.shutdown().await;
    Ok(())
}

/// Two exports each add a different column at once. The slower one built its batch before
/// the other changed the table, so the schema check refuses it. It must rebuild against the
/// current schema and retry so its data points still land.
#[cfg(not(windows))]
#[tokio::test]
async fn concurrent_exports_adding_distinct_dimensions_all_land() -> Result<(), anyhow::Error> {
    const METRIC: &str = "otel_race_evolution";
    const ROUNDS: u64 = 3;

    let _tracing = init_tracing(Some("integration=debug,info"));

    let temp_dir = tempfile::tempdir()?;
    let data_dir = temp_dir.path().join("data").to_string_lossy().to_string();
    let metadata_dir = temp_dir
        .path()
        .join("metadata")
        .to_string_lossy()
        .to_string();
    let ds = make_dataset(METRIC, &data_dir, &metadata_dir);

    register_test_connectors().await;
    let rt = start_runtime(&ds).await;

    // Establish the base schema and register the sink.
    ingest(
        &rt,
        gauge_export(METRIC, 0.0, 100, vec![string_attr("region", "us")]),
    )
    .await?;

    // Each round fires two concurrent exports, each carrying a brand-new dimension, so the
    // slower export's write races the faster one's evolution + provider rebind.
    let mut dimensions: Vec<String> = Vec::new();
    for round in 0..ROUNDS {
        let mut tasks = tokio::task::JoinSet::new();
        for lane in 0..2u64 {
            let dimension = format!("dim_{round}_{lane}");
            dimensions.push(dimension.clone());
            let rt = Arc::clone(&rt);
            tasks.spawn(async move {
                ingest(
                    &rt,
                    gauge_export(
                        METRIC,
                        1.0,
                        200 + round * 2 + lane,
                        vec![
                            string_attr("region", "us"),
                            string_attr(&dimension, "present"),
                        ],
                    ),
                )
                .await
            });
        }
        let results = tokio::time::timeout(Duration::from_mins(2), tasks.join_all())
            .await
            .map_err(|_| {
                anyhow::anyhow!(
                    "timed out waiting for concurrent evolving exports — possible deadlock in the schema-evolution path"
                )
            })?;
        for result in results {
            result?;
        }
    }

    let n = u64::try_from(row_count(&rt, METRIC).await?)?;
    assert_eq!(
        n,
        1 + ROUNDS * 2,
        "every concurrently evolving export must land; a missing row means an export lost \
         the evolution race and was rejected instead of retried"
    );

    // Every raced-in dimension must exist as a column.
    for dimension in &dimensions {
        run_query(&rt, &format!("SELECT {dimension} FROM {METRIC}"))
            .await
            .map_err(|e| {
                anyhow::anyhow!("dimension column {dimension} must exist after the races: {e}")
            })?;
    }

    rt.shutdown().await;
    Ok(())
}

/// After a restart the dataset has no table until its first write. When that write also adds
/// a column, the dataset must first be registered from the acceleration checkpoint, or the
/// export is rejected.
#[cfg(not(windows))]
#[tokio::test]
async fn restart_first_export_with_a_new_dimension_lands() -> Result<(), anyhow::Error> {
    const METRIC: &str = "otel_restart_new_dimension";

    let _tracing = init_tracing(Some("integration=debug,info"));

    let temp_dir = tempfile::tempdir()?;
    let data_dir = temp_dir.path().join("data").to_string_lossy().to_string();
    let metadata_dir = temp_dir
        .path()
        .join("metadata")
        .to_string_lossy()
        .to_string();
    let ds = make_dataset(METRIC, &data_dir, &metadata_dir);

    // Phase 1: establish the schema, then widen it once so the acceleration checkpoint is
    // persisted (schema evolution is what writes the checkpoint).
    {
        register_test_connectors().await;
        let rt = start_runtime(&ds).await;

        ingest(
            &rt,
            gauge_export(METRIC, 1.0, 100, vec![string_attr("region", "us")]),
        )
        .await?;
        ingest(
            &rt,
            gauge_export(
                METRIC,
                2.0,
                101,
                vec![string_attr("region", "eu"), string_attr("tenant", "acme")],
            ),
        )
        .await?;
        assert_eq!(row_count(&rt, METRIC).await?, 2);

        rt.shutdown().await;
        drop(rt);
    }

    // Phase 2: restart, and make the very first export carry a brand-new dimension. The
    // parked sink has no registered provider yet; evolution must register it from the
    // checkpoint and widen it, landing the export.
    {
        register_test_connectors().await;
        let rt = start_runtime(&ds).await;

        ingest(
            &rt,
            gauge_export(
                METRIC,
                3.0,
                102,
                vec![string_attr("region", "apac"), string_attr("team", "sre")],
            ),
        )
        .await?;

        assert_eq!(
            row_count(&rt, METRIC).await?,
            3,
            "the first post-restart export carrying a new dimension must land against the \
             checkpoint-registered provider, not be rejected"
        );
        run_query(&rt, &format!("SELECT team FROM {METRIC}"))
            .await
            .map_err(|e| anyhow::anyhow!("the new dimension column must exist: {e}"))?;

        rt.shutdown().await;
        drop(rt);
    }

    Ok(())
}

/// A writer can name the dataset differently: a Flight `DoPut` turns `foo` into
/// `spice.public.foo`, and the runtime accepts that as writable. Registration has to find the
/// dataset by that name too, or the write fails against a table that was never registered.
#[cfg(not(windows))]
#[tokio::test]
async fn qualified_reference_write_registers_a_parked_sink() -> Result<(), anyhow::Error> {
    const METRIC: &str = "otel_alias_registration";

    let _tracing = init_tracing(Some("integration=debug,info"));

    let temp_dir = tempfile::tempdir()?;
    let data_dir = temp_dir.path().join("data").to_string_lossy().to_string();
    let metadata_dir = temp_dir
        .path()
        .join("metadata")
        .to_string_lossy()
        .to_string();
    let ds = make_dataset(METRIC, &data_dir, &metadata_dir);

    register_test_connectors().await;
    let rt = start_runtime(&ds).await;

    // The dataset is registered as the bare `METRIC`; write through its fully-qualified
    // alias, exactly as a Flight `DoPut` does after normalizing the path it was given.
    let qualified = TableReference::full("spice", "public", METRIC);
    let schema = Arc::new(Schema::new(vec![
        Field::new("value", DataType::Float64, true),
        Field::new("time_unix_nano", DataType::UInt64, true),
        Field::new("start_time_unix_nano", DataType::UInt64, true),
        Field::new("region", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(Float64Array::from(vec![1.0])),
            Arc::new(UInt64Array::from(vec![100_u64])),
            Arc::new(UInt64Array::from(vec![0_u64])),
            Arc::new(StringArray::from(vec!["us"])),
        ],
    )?;

    rt.datafusion()
        .write_data(
            &qualified,
            DataUpdate {
                schema,
                data: vec![batch],
                update_type: UpdateType::Append,
            },
        )
        .await
        .map_err(|e| {
            anyhow::anyhow!(
                "a write through a qualified alias must register the parked sink and land: {e}"
            )
        })?;

    assert_eq!(
        row_count(&rt, METRIC).await?,
        1,
        "the aliased write must land in the now-registered dataset"
    );

    rt.shutdown().await;
    Ok(())
}

/// Many exports each adding a different column at once. Each one builds its batch from the
/// schema it read, so by the time it evolves, several columns it never saw already exist. If
/// that counted as removing them, every export would advance one column per retry and the last
/// ones would run out of retries and lose their data points.
#[cfg(not(windows))]
#[tokio::test]
async fn many_concurrent_exports_adding_distinct_columns_all_land() -> Result<(), anyhow::Error> {
    const METRIC: &str = "otel_race_many_dimensions";
    // Deliberately more than the per-metric write attempt cap.
    const WRITERS: u64 = 8;

    let _tracing = init_tracing(Some("integration=debug,info"));

    let temp_dir = tempfile::tempdir()?;
    let data_dir = temp_dir.path().join("data").to_string_lossy().to_string();
    let metadata_dir = temp_dir
        .path()
        .join("metadata")
        .to_string_lossy()
        .to_string();
    let ds = make_dataset(METRIC, &data_dir, &metadata_dir);

    register_test_connectors().await;
    let rt = start_runtime(&ds).await;

    // Establish the base schema and register the sink.
    ingest(
        &rt,
        gauge_export(METRIC, 0.0, 100, vec![string_attr("region", "us")]),
    )
    .await?;

    let mut tasks = tokio::task::JoinSet::new();
    for i in 0..WRITERS {
        let rt = Arc::clone(&rt);
        tasks.spawn(async move {
            ingest(
                &rt,
                gauge_export(
                    METRIC,
                    1.0,
                    200 + i,
                    vec![
                        string_attr("region", "us"),
                        string_attr(&format!("dim_{i}"), "present"),
                    ],
                ),
            )
            .await
        });
    }

    let results = tokio::time::timeout(Duration::from_mins(2), tasks.join_all())
        .await
        .map_err(|_| {
            anyhow::anyhow!(
                "timed out waiting for concurrent evolving exports — possible deadlock in the schema-evolution path"
            )
        })?;
    for result in results {
        result?;
    }

    let n = u64::try_from(row_count(&rt, METRIC).await?)?;
    assert_eq!(
        n,
        1 + WRITERS,
        "every export must land, however many columns were added behind its back"
    );
    for i in 0..WRITERS {
        run_query(&rt, &format!("SELECT dim_{i} FROM {METRIC}"))
            .await
            .map_err(|e| anyhow::anyhow!("column dim_{i} must exist after the race: {e}"))?;
    }

    rt.shutdown().await;
    Ok(())
}
