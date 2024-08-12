/*
Copyright 2024 The Spice.ai OSS Authors

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

use std::{borrow::Cow, sync::Arc};

use app::spicepod::component::runtime::TracingConfig;
use opentelemetry_sdk::{
    trace::{Config, TracerProvider},
    Resource,
};
use runtime::{datafusion::DataFusion, task_history};
use tracing::Subscriber;
use tracing_subscriber::{filter, fmt, layer::Layer, prelude::*, registry::LookupSpan, EnvFilter};

pub(crate) fn init_tracing(
    app_name: Option<String>,
    config: Option<&TracingConfig>,
    df: Arc<DataFusion>,
) -> Result<(), Box<dyn std::error::Error>> {
    let filter = if let Ok(env_log) = std::env::var("SPICED_LOG") {
        EnvFilter::new(env_log)
    } else {
        EnvFilter::new("task_history=INFO,spiced=INFO,runtime=INFO,secrets=INFO,data_components=INFO,cache=INFO,extensions=INFO,spice_cloud=INFO")
    };

    let subscriber = tracing_subscriber::registry()
        .with(filter)
        //.with(zipkin_task_history_tracing(app_name, config)?)
        .with(datafusion_task_history_tracing(df))
        .with(
            fmt::layer()
                .with_ansi(true)
                .with_filter(filter::filter_fn(|metadata| {
                    metadata.target() != "task_history"
                })),
        );

    tracing::subscriber::set_global_default(subscriber)?;

    Ok(())
}

fn datafusion_task_history_tracing<S>(df: Arc<DataFusion>) -> impl Layer<S>
where
    S: Subscriber + for<'span> LookupSpan<'span>,
{
    let trace_config = Config::default().with_resource(Resource::empty());

    let exporter = task_history::otel_exporter::TaskHistoryExporter::new(df);

    let mut provider_builder =
        TracerProvider::builder().with_batch_exporter(exporter, opentelemetry_sdk::runtime::Tokio);
    provider_builder = provider_builder.with_config(trace_config);
    let provider = provider_builder.build();
    let tracer = opentelemetry::trace::TracerProvider::tracer_builder(&provider, "task_history")
        .with_version(env!("CARGO_PKG_VERSION"))
        .build();

    let layer = tracing_opentelemetry::layer()
        .with_tracer(tracer)
        .with_filter(filter::filter_fn(|metadata| {
            metadata.target() == "task_history"
        }));

    layer
}

fn zipkin_task_history_tracing<S>(
    app_name: Option<String>,
    config: Option<&TracingConfig>,
) -> Result<Option<impl Layer<S>>, Box<dyn std::error::Error>>
where
    S: Subscriber + for<'span> LookupSpan<'span>,
{
    let Some(config) = config else {
        return Ok(None);
    };
    if !config.zipkin_enabled {
        return Ok(None);
    }

    let Some(zipkin_endpoint) = config.zipkin_endpoint.as_ref() else {
        return Err("zipkin_endpoint is required when zipkin_enabled is true".into());
    };

    let service_name: Cow<'static, str> = match app_name {
        Some(name) => Cow::Owned(name),
        None => Cow::Borrowed("Spice.ai"),
    };

    let tracer = opentelemetry_zipkin::new_pipeline()
        .with_service_name(service_name)
        .with_collector_endpoint(zipkin_endpoint)
        .with_http_client(reqwest::Client::new())
        .install_batch(opentelemetry_sdk::runtime::Tokio)?;

    let layer = tracing_opentelemetry::layer()
        .with_tracer(tracer)
        .with_filter(filter::filter_fn(|metadata| {
            metadata.target() == "task_history"
        }));

    Ok(Some(layer))
}
