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

use app::{
    spicepod::component::runtime::{TaskHistoryCapturedOutput, TracingConfig},
    App,
};
use futures::future::BoxFuture;
use opentelemetry_sdk::{
    export::trace::{ExportResult, SpanData, SpanExporter},
    trace::{Config, TracerProvider},
    Resource,
};
use runtime::{datafusion::DataFusion, task_history};
use tracing::Subscriber;
use tracing_subscriber::{filter, fmt, layer::Layer, prelude::*, registry::LookupSpan, EnvFilter};

pub enum LogVerbosity {
    Default,
    Verbose,
    VeryVerbose,
    Specific(String),
}

impl LogVerbosity {
    pub(crate) fn from_flags_and_env(verbose: bool, very_verbose: bool, env_var: &str) -> Self {
        if very_verbose {
            return LogVerbosity::VeryVerbose;
        }

        if verbose {
            return LogVerbosity::Verbose;
        }

        if let Ok(filter) = std::env::var(env_var) {
            return LogVerbosity::Specific(filter);
        }

        LogVerbosity::Default
    }
}
impl From<LogVerbosity> for EnvFilter {
    fn from(v: LogVerbosity) -> Self {
        match v {
            LogVerbosity::Default => EnvFilter::new("task_history=INFO,spiced=INFO,runtime=INFO,secrets=INFO,data_components=INFO,cache=INFO,extensions=INFO,spice_cloud=INFO,WARN"),
            LogVerbosity::Verbose => EnvFilter::new("task_history=DEBUG,spiced=DEBUG,runtime=DEBUG,secrets=DEBUG,data_components=DEBUG,cache=DEBUG,extensions=DEBUG,spice_cloud=DEBUG,INFO"),
            LogVerbosity::VeryVerbose => EnvFilter::new("task_history=TRACE,spiced=TRACE,runtime=TRACE,secrets=TRACE,data_components=TRACE,cache=TRACE,extensions=TRACE,spice_cloud=TRACE,DEBUG"),
            LogVerbosity::Specific(filter) => EnvFilter::new(filter),
        }
    }
}

pub(crate) fn init_tracing(
    app: &Option<Arc<App>>,
    config: Option<&TracingConfig>,
    df: Arc<DataFusion>,
    verbosity: LogVerbosity,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let filter: EnvFilter = verbosity.into();

    if let Some(app) = app.as_ref() {
        if !app.runtime.task_history.enabled {
            let subscriber = tracing_subscriber::registry().with(filter).with(
                fmt::layer()
                    .with_ansi(true)
                    .with_filter(filter::filter_fn(|metadata| {
                        metadata.target() != "task_history"
                    })),
            );

            tracing::subscriber::set_global_default(subscriber)?;

            return Ok(());
        }
    }

    let subscriber = tracing_subscriber::registry()
        .with(filter)
        .with(datafusion_task_history_tracing(df, app, config)?)
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

fn datafusion_task_history_tracing<S>(
    df: Arc<DataFusion>,
    app: &Option<Arc<App>>,
    config: Option<&TracingConfig>,
) -> Result<impl Layer<S>, Box<dyn std::error::Error + Send + Sync>>
where
    S: Subscriber + for<'span> LookupSpan<'span>,
{
    let trace_config = Config::default().with_resource(Resource::empty());
    let app_name = app.as_ref().map(|app| app.name.clone());

    let captured_output = if let Some(app) = app.as_ref() {
        app.runtime.task_history.get_captured_output()?
    } else {
        TaskHistoryCapturedOutput::Truncated
    };

    let mut exporters: Vec<Box<dyn SpanExporter>> = vec![Box::new(
        task_history::otel_exporter::TaskHistoryExporter::new(df, captured_output),
    )];

    if let Ok(Some(zipkin_exporter)) = zipkin_task_history_otel_exporter(app_name, config) {
        exporters.push(zipkin_exporter);
    }

    let exporter = OtelExportMultiplexer::new(exporters);

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

    Ok(layer)
}

fn zipkin_task_history_otel_exporter(
    app_name: Option<String>,
    config: Option<&TracingConfig>,
) -> Result<Option<Box<dyn SpanExporter>>, Box<dyn std::error::Error>> {
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

    Ok(Some(Box::new(
        opentelemetry_zipkin::new_pipeline()
            .with_service_name(service_name)
            .with_collector_endpoint(zipkin_endpoint)
            .with_http_client(reqwest::Client::new())
            .init_exporter()?,
    )))
}

#[derive(Debug)]
struct OtelExportMultiplexer {
    exporters: Vec<Box<dyn SpanExporter>>,
}

impl OtelExportMultiplexer {
    pub fn new(exporters: Vec<Box<dyn SpanExporter>>) -> Self {
        Self { exporters }
    }
}

impl SpanExporter for OtelExportMultiplexer {
    fn export(&mut self, batch: Vec<SpanData>) -> BoxFuture<'static, ExportResult> {
        let mut futures = Vec::new();
        for exporter in &mut self.exporters {
            futures.push(exporter.export(batch.clone()));
        }

        Box::pin(async move {
            futures::future::join_all(futures).await;

            Ok(())
        })
    }

    fn shutdown(&mut self) {
        for exporter in &mut self.exporters {
            exporter.shutdown();
        }
    }

    fn force_flush(&mut self) -> BoxFuture<'static, ExportResult> {
        let mut futures = Vec::new();
        for exporter in &mut self.exporters {
            futures.push(exporter.force_flush());
        }

        Box::pin(async move {
            futures::future::join_all(futures).await;

            Ok(())
        })
    }

    fn set_resource(&mut self, resource: &Resource) {
        for exporter in &mut self.exporters {
            exporter.set_resource(resource);
        }
    }
}
