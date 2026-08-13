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

//! Regression test for OpenTelemetry-metric ingest into `from: sink:` datasets with
//! file-based (Cayenne) acceleration across a restart.
//!
//! A `sink` dataset stores everything in its acceleration and is parked (pending) on restart
//! until its first write re-registers it. Until then the ingest cannot look up the table
//! schema, so a data point that omits a NULL dimension yields a batch narrower than the stored
//! (wide) acceleration schema. Reopening the wide acceleration then rejects that narrow write
//! as a removed column:
//!
//! ```text
//! Schema change detected that cannot be evolved under `on_schema_change: append_new_columns`:
//!   The column `tenant` is missing. The new schema is not applied ...
//! Cayenne table schema changed but cannot be evolved in place: The column `tenant` is missing.
//! Schema mismatch: Expected and actual number of fields ... don't match: expected N, received N-1
//! ```
//!
//! The ingest must instead build the batch against the acceleration checkpoint schema when the
//! table is not yet registered, materializing the omitted dimension as NULL so the write lands.
//! Cayenne is the engine the production OTLP metric datasets use: it is CDC-backed, so its
//! file acceleration persists across restarts (reaching the reconciliation this bug lives in).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use app::AppBuilder;
use arrow::datatypes::Schema;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::collector::metrics::v1::metrics_service_server::MetricsService;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value::Value as AnyVal};
use opentelemetry_proto::tonic::metrics::v1::{
    Gauge, Metric as OtlpMetric, NumberDataPoint, ResourceMetrics, ScopeMetrics, metric::Data,
    number_data_point::Value as NumberValue,
};
use runtime::Runtime;
use runtime::dataaccelerator::spice_sys::{OpenOption, dataset_checkpoint::DatasetCheckpoint};
use spicepod::acceleration::{Acceleration, Mode};
use spicepod::component::access::AccessMode;
use spicepod::component::dataset::{Dataset, OnSchemaChange};
use spicepod::param::Params;

use crate::{
    RecordBatch, configure_test_datafusion, init_tracing,
    utils::{register_test_connectors, run_query, runtime_ready_check},
};

const METRIC: &str = "otel_query_duration_ms";

fn make_dataset(data_dir: &str, metadata_dir: &str) -> Dataset {
    let mut ds = Dataset::new(format!("sink:{METRIC}"), METRIC.to_string());
    // Required for the runtime to admit writes (the OTLP ingest path only writes to a
    // dataset that `is_writable`).
    ds.access = AccessMode::ReadWrite;
    ds.on_schema_change = OnSchemaChange::AppendNewColumns;
    ds.acceleration = Some(Acceleration {
        enabled: true,
        engine: Some("cayenne".to_string()),
        mode: Mode::File,
        params: Some(Params::from_string_map(HashMap::from([
            ("cayenne_file_path".to_string(), data_dir.to_string()),
            ("cayenne_metadata_dir".to_string(), metadata_dir.to_string()),
        ]))),
        ..Acceleration::default()
    });
    ds
}

async fn start_runtime(ds: &Dataset) -> Arc<Runtime> {
    let app = AppBuilder::new("otel_restart_regression")
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

/// A single-data-point gauge export for `METRIC` carrying `attrs` as dimensions.
fn gauge_export(value: f64, attrs: Vec<KeyValue>) -> ExportMetricsServiceRequest {
    ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            resource: None,
            scope_metrics: vec![ScopeMetrics {
                scope: None,
                metrics: vec![OtlpMetric {
                    name: METRIC.to_string(),
                    description: String::new(),
                    unit: String::new(),
                    metadata: Vec::new(),
                    data: Some(Data::Gauge(Gauge {
                        data_points: vec![NumberDataPoint {
                            attributes: attrs,
                            start_time_unix_nano: 0,
                            time_unix_nano: 100,
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

/// Drive the metric through the real OTLP ingest handler (the same `export` the gRPC server
/// serves), and fail if any data point was rejected.
async fn ingest(rt: &Arc<Runtime>, req: ExportMetricsServiceRequest) -> Result<(), anyhow::Error> {
    // `rt.datafusion()` (an `Arc<DataFusion>`) coerces to the `Arc<dyn QueryEngine>` the
    // builder expects at the call site.
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

async fn row_count(rt: &Arc<Runtime>) -> Result<usize, anyhow::Error> {
    let rows = run_query(rt, &format!("SELECT region FROM {METRIC}")).await?;
    Ok(rows.iter().map(RecordBatch::num_rows).sum())
}

/// Load the acceleration checkpoint schema the same way the runtime does on restart.
async fn checkpoint_schema(rt: &Arc<Runtime>, ds: &Dataset) -> Option<Arc<Schema>> {
    let app_ref = rt.app();
    let app_lock = app_ref.read().await;
    let app = app_lock.as_ref()?;
    let runtime_dataset =
        runtime::component::dataset::builder::DatasetBuilder::try_from(ds.clone())
            .ok()?
            .with_app(Arc::clone(app))
            .with_runtime(Arc::clone(rt))
            .build()
            .ok()?;
    let checkpoint = DatasetCheckpoint::try_new(
        &runtime_dataset,
        runtime_dataset.runtime.accelerator_engine_registry(),
        OpenOption::OpenExisting,
    )
    .await
    .ok()?;
    checkpoint.get_schema().await.ok().flatten()
}

#[cfg(not(windows))]
#[tokio::test]
async fn sink_accelerated_metric_survives_restart_without_schema_mismatch()
-> Result<(), anyhow::Error> {
    let _tracing = init_tracing(Some("integration=debug,info"));

    let temp_dir = tempfile::tempdir()?;
    let data_dir = temp_dir.path().join("data").to_string_lossy().to_string();
    let metadata_dir = temp_dir
        .path()
        .join("metadata")
        .to_string_lossy()
        .to_string();

    let ds = make_dataset(&data_dir, &metadata_dir);

    // --- Phase 1: ingest metrics that establish, then widen, the acceleration schema. ---
    {
        register_test_connectors().await;
        let rt = start_runtime(&ds).await;

        // First export establishes the base schema (`region`) and registers the sink.
        ingest(&rt, gauge_export(1.0, vec![string_attr("region", "us")])).await?;
        // A later export carries a new dimension (`tenant`), widening the acceleration and
        // persisting the evolved schema to the checkpoint.
        ingest(
            &rt,
            gauge_export(
                2.0,
                vec![string_attr("region", "eu"), string_attr("tenant", "acme")],
            ),
        )
        .await?;

        let n = row_count(&rt).await?;
        eprintln!("phase 1: row_count = {n}");
        assert_eq!(n, 2, "phase 1 should have ingested two data points");

        let cp = checkpoint_schema(&rt, &ds).await;
        let cp_cols: Vec<String> = cp
            .as_ref()
            .map(|s| s.fields().iter().map(|f| f.name().clone()).collect())
            .unwrap_or_default();
        eprintln!("phase 1: checkpoint columns = {cp_cols:?}");
        let cp = cp.expect("phase 1 must persist an acceleration checkpoint");
        assert!(
            cp.field_with_name("region").is_ok() && cp.field_with_name("tenant").is_ok(),
            "checkpoint must carry the wide dimension columns, got {cp_cols:?}"
        );

        rt.shutdown().await;
        drop(rt);
    }

    // --- Phase 2: restart, then ingest a data point that omits the `tenant` dimension. ---
    // The sink dataset is parked until this first write. The ingest must build the batch
    // against the checkpoint (materializing `tenant` as NULL) so it matches the reopened wide
    // acceleration and lands, rather than being rejected as a removed column.
    {
        register_test_connectors().await;
        let rt = start_runtime(&ds).await;

        ingest(&rt, gauge_export(3.0, vec![string_attr("region", "apac")])).await?;

        let n = row_count(&rt).await?;
        eprintln!("phase 2: row_count after restart ingest = {n}");
        assert_eq!(
            n, 3,
            "a post-restart data point that omits a known dimension must land against the wide \
             checkpoint (2 existing + 1 new), not be rejected as a schema change"
        );

        rt.shutdown().await;
        drop(rt);
    }

    Ok(())
}
