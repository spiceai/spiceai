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

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::{Duration, Instant},
};

use crate::args::{CommonArgs, DatasetTestArgs};
use test_framework::{
    anyhow,
    app::{App, AppBuilder},
    opentelemetry_sdk::Resource,
    queries::QuerySet,
    spiced::{SpicedInstance, StartRequest},
    spicepod::Spicepod,
    spicepod_utils::from_app,
    spicetest::datasets::NotStarted,
    telemetry::{OtlpExporterConfig, Telemetry},
};

const AUTOMATIC_REFERENCE_SCHEMA: &str = "__test_reference";

#[cfg(feature = "append")]
pub(crate) mod append;
pub(crate) mod bench;
pub(crate) mod data_consistency;
pub(crate) mod dispatch;
pub(crate) mod htap;
pub(crate) mod load;
pub(crate) mod query;
pub(crate) mod schema;
pub(crate) mod search;
pub(crate) mod streaming;
pub(crate) mod text_to_sql;
pub(crate) mod throughput;
pub(crate) type RowCounts = BTreeMap<Arc<str>, usize>;

/// Create telemetry with resource attributes known upfront.
///
/// This ensures the `SdkMeterProvider` is created with the correct resource,
/// so metrics recorded after this call will have the proper resource attributes.
#[must_use]
pub(crate) fn create_telemetry_with_resource(common: &CommonArgs, resource: Resource) -> Telemetry {
    if let Some(endpoint) = &common.otlp_endpoint {
        return Telemetry::with_otlp_resource(
            OtlpExporterConfig {
                endpoint: endpoint.clone().into(),
                headers: common.otlp_header.clone(),
                timeout: Duration::from_secs(10),
            },
            resource,
        );
    }

    Telemetry::new_with_resource(&resource, "SPICEAI_BENCHMARK_METRICS_KEY")
}

/// Build a test configuration with validation data if applicable
///
/// This is a common helper for bench, throughput, and load tests that:
/// 1. Loads the query set from args
/// 2. Applies query overrides if specified
/// 3. Adds validation data for scenario queries when validation is enabled
/// 4. Adds reference schema for validation against known good tables
///
/// # Returns
/// Tuple of (`QuerySet`, `NotStarted` builder)
pub(crate) async fn build_test_with_validation(
    args: &DatasetTestArgs,
    test_builder: NotStarted,
) -> anyhow::Result<(QuerySet, NotStarted)> {
    let query_set = args.load_query_set()?;
    let query_overrides = args
        .query_overrides
        .clone()
        .map(test_framework::queries::QueryOverrides::from);
    let queries = query_set
        .get_queries(query_overrides, None, None, args.scale_factor)
        .await?;

    let mut test_builder = test_builder
        .with_query_set(queries)
        .with_query_set_type(query_set.clone())
        .with_query_overrides(query_overrides);

    // Add validation data if this is a scenario query set with validation enabled
    if args.validate
        && let Some(validation_data) =
            query_set.get_validation_data(args.scenario_query_file.as_deref())?
    {
        test_builder = test_builder.with_validation_data(validation_data);
    }

    // Add reference schema for validation against known good tables
    if let Some(ref_schema) = validation_reference_schema(args)? {
        test_builder = test_builder.with_reference_schema(Some(ref_schema));
    }

    Ok((query_set, test_builder))
}

fn supports_automatic_reference_validation(query_set: &QuerySet) -> bool {
    matches!(query_set, QuerySet::Tpch | QuerySet::ParameterizedTpch)
}

fn validation_reference_schema(args: &DatasetTestArgs) -> anyhow::Result<Option<String>> {
    if let Some(reference_schema) = &args.reference_schema {
        return Ok(Some(reference_schema.clone()));
    }

    if !args.validate || args.common.is_external_instance() || args.common.is_system_adapter() {
        return Ok(None);
    }

    let query_set = args.load_query_set()?;
    if supports_automatic_reference_validation(&query_set) {
        Ok(Some(AUTOMATIC_REFERENCE_SCHEMA.to_string()))
    } else {
        Ok(None)
    }
}

fn add_automatic_reference_datasets(args: &DatasetTestArgs, app: &mut App) -> anyhow::Result<()> {
    if args.reference_schema.is_some() || validation_reference_schema(args)?.is_none() {
        return Ok(());
    }

    let existing_dataset_names = app
        .datasets
        .iter()
        .map(|dataset| dataset.name.clone())
        .collect::<BTreeSet<_>>();

    let reference_datasets = app
        .datasets
        .iter()
        .filter(|dataset| !dataset.name.contains('.'))
        .filter_map(|dataset| {
            let reference_name = format!("{AUTOMATIC_REFERENCE_SCHEMA}.{}", dataset.name);
            if existing_dataset_names.contains(reference_name.as_str()) {
                return None;
            }

            let mut reference_dataset = dataset.clone();
            reference_dataset.name = reference_name;
            reference_dataset.acceleration = None;
            reference_dataset.depends_on.clear();
            Some(reference_dataset)
        })
        .collect::<Vec<_>>();

    if !reference_datasets.is_empty() {
        println!(
            "Adding {} unaccelerated reference datasets under {AUTOMATIC_REFERENCE_SCHEMA}.* for TPCH validation",
            reference_datasets.len()
        );
        app.datasets.extend(reference_datasets);
    }

    Ok(())
}

pub(crate) async fn run_or_connect_spiced(
    args: &CommonArgs,
) -> anyhow::Result<(App, SpicedInstance)> {
    let (app, mut instance) = if args.is_external_instance() {
        println!(
            "Connecting to external spiced instance at: {}",
            args.spiced_path
        );
        let spicepod = Spicepod::load_exact(args.spicepod_path.clone()).await?;
        let app = AppBuilder::new(spicepod.name.clone())
            .with_spicepod(spicepod)
            .build();
        let instance = SpicedInstance::external(&args.spiced_path);
        (app, instance)
    } else {
        let (app, start_request) = get_app_and_start_request(args).await?;
        let instance = SpicedInstance::start(start_request).await?;
        (app, instance)
    };
    instance
        .wait_for_ready(std::time::Duration::from_secs(args.ready_wait))
        .await?;

    Ok((app, instance))
}

/// Load the test [`App`] from the configured spicepod (and its declared
/// dependencies). Shared by both the local-spawn and system-adapter SUT paths,
/// since testoperator always needs the spicepod-derived dataset info for query
/// construction regardless of where the SUT actually runs.
/// Build resource attributes that describe how this run acquired its SUT and
/// which query transport it exercised. Lets downstream dashboards distinguish
/// cluster benchmarks (via a system adapter) from the traditional single-node
/// `spiced` runs, and split the cluster results by query path (Flight SQL for
/// distributed accelerations vs HTTP `/v1/queries` for Ballista).
pub(crate) fn run_mode_attributes(
    common: &CommonArgs,
    dataset_args: &DatasetTestArgs,
) -> Vec<test_framework::opentelemetry::KeyValue> {
    use test_framework::opentelemetry::KeyValue;

    let mut attrs = Vec::with_capacity(4);

    let (mode, transport) = if common.is_system_adapter() {
        let transport = if dataset_args.distributed {
            "http_v1_queries"
        } else if dataset_args.http_clients {
            "http_v1_sql"
        } else {
            "flightsql"
        };
        ("cluster", transport)
    } else if common.is_external_instance() {
        ("external", "flightsql")
    } else {
        ("single_node", "flightsql")
    };

    attrs.push(KeyValue::new("mode", mode));
    attrs.push(KeyValue::new("query_transport", transport));

    if common.is_system_adapter() {
        attrs.push(KeyValue::new(
            "system_adapter_name",
            common.system_adapter_name.clone(),
        ));
        // Mirror any executor-count hint the user passed via
        // --system-adapter-param so dashboards can group runs by cluster size
        // without inspecting the spicepod or the adapter's reply.
        if let Some(replicas) = common
            .system_adapter_param
            .iter()
            .find(|(k, _)| k == "executor_replicas")
            .map(|(_, v)| v.clone())
        {
            attrs.push(KeyValue::new("cluster_executor_replicas", replicas));
        }
    }

    attrs
}

pub(crate) async fn load_app(args: &CommonArgs) -> anyhow::Result<App> {
    let mut spicepod = Spicepod::load_exact(args.spicepod_path.clone()).await?;

    // Resolve dependencies up-front from the original list, then strip them
    // before handing the spicepod to the builder — otherwise the App carries a
    // dangling `dependencies:` list that no longer matches what got loaded.
    let mut dependencies = Vec::new();
    if let Some(dependencies_root) = &args.spicepod_dependencies {
        for dependency in &spicepod.dependencies {
            dependencies.push(Spicepod::load(&dependencies_root.join(dependency)).await?);
        }
    }
    spicepod.dependencies = vec![];

    let mut app_builder = AppBuilder::new(spicepod.name.clone()).with_spicepod(spicepod);
    for dependent_spicepod in dependencies {
        app_builder = app_builder.with_spicepod_dependency(dependent_spicepod);
    }
    Ok(app_builder.build())
}

pub(crate) async fn get_app_and_start_request(
    args: &CommonArgs,
) -> anyhow::Result<(App, StartRequest)> {
    // When metrics are disabled, no Telemetry is created, so METER_PROVIDER_ONCE
    // remains unset and all metric operations are no-ops.

    let app = load_app(args).await?;
    let start_request = start_request_from_app(args, app.clone())?;

    Ok((app, start_request))
}

pub(crate) async fn get_dataset_app_and_start_request(
    args: &DatasetTestArgs,
) -> anyhow::Result<(App, StartRequest)> {
    let mut app = load_app(&args.common).await?;
    add_automatic_reference_datasets(args, &mut app)?;
    let start_request = start_request_from_app(&args.common, app.clone())?;

    Ok((app, start_request))
}

fn start_request_from_app(args: &CommonArgs, app: App) -> anyhow::Result<StartRequest> {
    let mut start_request = StartRequest::new(args.spiced_path_buf(), from_app(app))?;

    if let Some(ref data_dir) = args.data_dir {
        start_request = start_request.with_data_dir(data_dir.clone());
    }

    // If scrape_spiced_metrics is enabled, add --metrics flag to spiced
    if args.scrape_spiced_metrics {
        start_request = start_request
            .with_additional_args(vec!["--metrics".to_string(), "0.0.0.0:9090".to_string()]);
    }

    Ok(start_request)
}

pub(crate) async fn env_export(args: &CommonArgs) -> anyhow::Result<()> {
    let (_, mut start_request) = get_app_and_start_request(args).await?;

    start_request.prepare()?;
    let tempdir_path = start_request.get_tempdir_path();

    println!(
        "Exported spicepod environment to: {}",
        tempdir_path.to_string_lossy()
    );

    // Wait for input before exiting
    println!("Press Enter to exit...");
    std::io::stdin().read_line(&mut String::new())?;

    Ok(())
}

/// Create the appropriate query executor based on command-line arguments
///
/// This helper function centralizes the executor creation logic to avoid duplication
/// across different test commands (bench, throughput, load, query).
pub(crate) async fn create_query_executor(
    args: &DatasetTestArgs,
    spiced_instance: &test_framework::spiced::SpicedInstance,
) -> anyhow::Result<Box<dyn test_framework::execution::QueryExecutor>> {
    let executor: Box<dyn test_framework::execution::QueryExecutor> = if args.distributed {
        let http_client = spiced_instance.http_client()?;
        let base_url = spiced_instance.http_base_url().to_string();
        Box::new(test_framework::execution::DistributedExecutor::new(
            http_client,
            base_url,
        ))
    } else if args.http_clients {
        let http_client = spiced_instance.http_client()?;
        let base_url = spiced_instance.http_base_url().to_string();
        Box::new(test_framework::execution::HttpExecutor::new(
            http_client,
            base_url,
        ))
    } else {
        let spice_client = spiced_instance
            .spice_client(None, args.disable_caching)
            .await?;
        Box::new(test_framework::execution::FlightExecutor::new(
            std::sync::Arc::new(spice_client),
        ))
    };

    Ok(executor)
}

pub(crate) fn duration_millis_between(end: Instant, start: Instant) -> anyhow::Result<u64> {
    let duration = end
        .checked_duration_since(start)
        .ok_or_else(|| anyhow::anyhow!("End time was earlier than start time"))?;
    Ok(u64::try_from(duration.as_millis())?)
}

#[macro_export]
macro_rules! wait_test_and_memory {
    ($test:expr, $memory_token:expr, $memory_readings:expr) => {
        match $test.wait().await {
            Ok(test) => test,
            Err(e) => {
                observe_memory($memory_token, $memory_readings).await?;
                return Err(e);
            }
        }
    };
}

/// Process and display metrics from the spiced metrics scraper
///
/// # Arguments
/// * `scraper` - Optional metrics scraper to stop and process
/// * `emit_to_telemetry` - Whether to emit metrics to OpenTelemetry
/// * `attributes` - Optional attributes to attach to emitted metrics (e.g., test name)
///
/// # Returns
/// The collected `SpicedMetrics` if scraper was present, None otherwise
pub(crate) async fn process_spiced_metrics(
    scraper: Option<crate::spiced_metrics::MetricsScraper>,
    emit_to_telemetry: bool,
    attributes: &[test_framework::opentelemetry::KeyValue],
) -> Option<crate::spiced_metrics::SpicedMetrics> {
    let scraper = scraper?;

    match scraper.stop().await {
        Ok(metrics) => {
            println!("\n{}", vec!["="; 30].join(""));
            println!("Spiced Runtime Metrics:");
            println!("{}", vec!["="; 30].join(""));

            // Display and optionally emit key metrics
            // Note: Prometheus exporter appends _total to counter metrics
            if let Some(query_count) = metrics.get_counter_value("query_executions_total") {
                println!("Total Queries Executed: {query_count}");

                if emit_to_telemetry {
                    crate::metrics::SPICED_QUERY_COUNT.record(query_count, attributes);
                }
            }

            if let Some(cache_hits) = metrics.get_counter_value("results_cache_hits_total")
                && let Some(cache_requests) =
                    metrics.get_counter_value("results_cache_requests_total")
                && cache_requests > 0.0
            {
                let hit_rate = cache_hits / cache_requests;
                println!("Cache Hit Rate: {:.2}%", hit_rate * 100.0);

                if emit_to_telemetry {
                    crate::metrics::SPICED_CACHE_HIT_RATE.record(hit_rate, attributes);
                }
            }

            if let Some(active_conns) = metrics.get_gauge_max("query_active_count") {
                println!("Peak Active Connections: {active_conns}");

                if emit_to_telemetry {
                    crate::metrics::SPICED_ACTIVE_CONNECTIONS.record(active_conns, attributes);
                }
            }

            println!("{}", vec!["="; 30].join(""));
            Some(metrics)
        }
        Err(e) => {
            println!("Warning: Failed to collect spiced metrics: {e}");
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use clap::Parser;
    use test_framework::spicepod::component::dataset::Dataset;

    use super::*;

    #[test]
    fn automatic_reference_datasets_clone_base_datasets() {
        let args = DatasetTestArgs::parse_from([
            "testoperator",
            "--query-set",
            "tpch",
            "--validate",
            "--scale-factor",
            "100",
        ]);

        let mut app = App::default();
        app.datasets
            .push(Dataset::new("s3://bucket/customer.parquet", "customer"));
        app.datasets.push(Dataset::new(
            "s3://bucket/lineitem.parquet",
            "existing.lineitem",
        ));

        add_automatic_reference_datasets(&args, &mut app).expect("should add reference datasets");

        assert!(
            app.datasets
                .iter()
                .any(|dataset| dataset.name == "__test_reference.customer")
        );
        assert!(
            app.datasets
                .iter()
                .all(|dataset| dataset.name != "__test_reference.existing.lineitem")
        );
    }
}
