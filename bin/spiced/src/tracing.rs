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

use std::sync::Arc;

use app::spicepod::component::runtime::OutputLevel;
use app::{App, spicepod::component::runtime::TracingConfig};
use opentelemetry::{InstrumentationScope, trace::TracerProvider as _};
use opentelemetry_sdk::{
    Resource,
    error::OTelSdkResult,
    trace::{
        SdkTracerProvider, SpanData, SpanExporter,
        span_processor_with_async_runtime::BatchSpanProcessor,
    },
};
use opentelemetry_zipkin::ZipkinExporter;
use reqwest::Client;
use runtime::{datafusion::DataFusion, task_history};
use std::time::Duration;
use tracing::Subscriber;
use tracing_log::LogTracer;
use tracing_subscriber::{EnvFilter, filter, fmt, layer::Layer, prelude::*, registry::LookupSpan};

#[derive(PartialEq, Debug)]
pub enum LogVerbosity {
    Default,
    Verbose,
    VeryVerbose,
    Specific(String),
}

impl LogVerbosity {
    pub(crate) fn from_flags_and_env_and_config(
        verbose: bool,
        very_verbose: bool,
        env_var: &str,
        config_output_level: Option<OutputLevel>,
    ) -> Self {
        if very_verbose {
            return LogVerbosity::VeryVerbose;
        }

        if verbose {
            return LogVerbosity::Verbose;
        }

        if let Ok(filter) = std::env::var(env_var) {
            return LogVerbosity::Specific(filter);
        }

        match config_output_level {
            Some(OutputLevel::VeryVerbose) => LogVerbosity::VeryVerbose,
            Some(OutputLevel::Verbose) => LogVerbosity::Verbose,
            None | Some(OutputLevel::Info) => LogVerbosity::Default,
        }
    }
}

const INTERNAL_COMPONENTS: &[&str] = &[
    "app",
    "task_history",
    "spiced",
    "runtime",
    "secrets",
    "data_components",
    "cache",
    "extensions",
    "spice_cloud",
    "llms",
    "tpc_extension",
    "workers",
    "search",
    "ballista",
    "datafusion",
];

const OFF_FILTERS: &str = "reqwest_retry::middleware=off,opentelemetry_sdk=off,delta_kernel::log_segment=off,delta_kernel::listed_log_files=off,aws_config::imds::region=off,aws_config::meta::credentials::chain=off,tower::buffer=off,h2::codec=off";

impl From<LogVerbosity> for EnvFilter {
    fn from(v: LogVerbosity) -> Self {
        fn internal_components(level: &str) -> String {
            INTERNAL_COMPONENTS
                .iter()
                .map(|component| format!("{component}={level}"))
                .collect::<Vec<_>>()
                .join(",")
        }

        match v {
            LogVerbosity::Default => EnvFilter::new(format!(
                "{},{OFF_FILTERS},WARN",
                internal_components("INFO")
            )),
            LogVerbosity::Verbose => EnvFilter::new(format!(
                "{},{OFF_FILTERS},INFO",
                internal_components("DEBUG")
            )),
            LogVerbosity::VeryVerbose => EnvFilter::new(format!(
                "{},{OFF_FILTERS},DEBUG",
                internal_components("TRACE")
            )),
            LogVerbosity::Specific(filter) => EnvFilter::new(filter),
        }
    }
}

pub(crate) async fn init_tracing(
    app: Option<&Arc<App>>,
    config: Option<&TracingConfig>,
    df: Arc<DataFusion>,
    verbosity: LogVerbosity,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let filter: EnvFilter = verbosity.into();

    if let Some(app) = app.as_ref()
        && !app.runtime.task_history.enabled
    {
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

    let subscriber = tracing_subscriber::registry()
        .with(filter)
        .with(datafusion_task_history_tracing(df, app, config).await?)
        .with(
            event_stream::EventStreamLayer::new("progress").with_filter(filter::filter_fn(
                |metadata| metadata.target() == "task_history",
            )),
        )
        .with(
            fmt::layer()
                .with_ansi(true)
                .with_filter(filter::filter_fn(|metadata| {
                    metadata.target() != "task_history"
                })),
        );

    tracing::subscriber::set_global_default(subscriber)?;
    LogTracer::init()?;

    Ok(())
}

async fn datafusion_task_history_tracing<S>(
    df: Arc<DataFusion>,
    app: Option<&Arc<App>>,
    config: Option<&TracingConfig>,
) -> Result<impl Layer<S> + use<S>, Box<dyn std::error::Error + Send + Sync>>
where
    S: Subscriber + for<'span> LookupSpan<'span>,
{
    let app_name = app.as_ref().map(|app| app.name.clone());

    let captured_output = app
        .as_ref()
        .map(|app| app.runtime.task_history.get_captured_output())
        .transpose()?
        .unwrap_or_default();

    let min_sql_duration_ms = app
        .as_ref()
        .map(|app| app.runtime.task_history.min_sql_duration_as_millis())
        .transpose()?
        .flatten();

    let captured_plan = app
        .as_ref()
        .map(|app| app.runtime.task_history.get_captured_plan())
        .transpose()?
        .unwrap_or_default();

    let min_plan_duration_ms = app
        .as_ref()
        .map(|app| app.runtime.task_history.min_plan_duration_as_millis())
        .transpose()?
        .flatten();

    let task_history_exporter = task_history::otel_exporter::TaskHistoryExporter::new(
        df,
        captured_output,
        min_sql_duration_ms,
        captured_plan,
        min_plan_duration_ms,
    );

    let zipkin_exporter = zipkin_task_history_otel_exporter(config).await?;

    let exporter = OtelExportMultiplexer::new(task_history_exporter, zipkin_exporter);

    let service_name = app_name
        .as_ref()
        .map_or_else(|| "Spice.ai".to_string(), Clone::clone);

    let processor =
        BatchSpanProcessor::builder(exporter, opentelemetry_sdk::runtime::Tokio).build();

    let provider = SdkTracerProvider::builder()
        .with_span_processor(processor)
        .with_resource(Resource::builder().with_service_name(service_name).build())
        .build();
    let scope = InstrumentationScope::builder("task_history")
        .with_version(env!("CARGO_PKG_VERSION"))
        .build();
    let tracer = provider.tracer_with_scope(scope);

    let layer = tracing_opentelemetry::layer()
        .with_tracer(tracer)
        .with_filter(filter::filter_fn(|metadata| {
            metadata.target() == "task_history"
        }));

    Ok(layer)
}

async fn zipkin_task_history_otel_exporter(
    config: Option<&TracingConfig>,
) -> Result<Option<ZipkinExporter>, Box<dyn std::error::Error + Send + Sync>> {
    let Some(config) = config else {
        return Ok(None);
    };
    if !config.zipkin_enabled {
        return Ok(None);
    }

    let Some(zipkin_endpoint) = config.zipkin_endpoint.as_ref() else {
        return Err("zipkin_endpoint is required when zipkin_enabled is true".into());
    };

    if !is_zipkin_endpoint_reachable(zipkin_endpoint).await {
        eprintln!(
            "Zipkin endpoint '{zipkin_endpoint}' is not reachable. Skipping Zipkin exporter initialization."
        );
        return Ok(None);
    }

    let collector_endpoint: String = zipkin_endpoint.to_string();

    Ok(Some(
        ZipkinExporter::builder()
            .with_collector_endpoint(collector_endpoint)
            .with_http_client(Client::new())
            .build()?,
    ))
}

async fn is_zipkin_endpoint_reachable(endpoint: &str) -> bool {
    let client = Client::new();
    let timeout = Duration::from_secs(5);

    let url = format!("{endpoint}?serviceName=test");

    match client.get(&url).timeout(timeout).send().await {
        Ok(response) => response.status().is_success(),
        Err(_) => false,
    }
}

#[derive(Debug)]
struct OtelExportMultiplexer {
    task_history: task_history::otel_exporter::TaskHistoryExporter,
    zipkin: Option<ZipkinExporter>,
}

impl OtelExportMultiplexer {
    pub fn new(
        task_history: task_history::otel_exporter::TaskHistoryExporter,
        zipkin: Option<ZipkinExporter>,
    ) -> Self {
        Self {
            task_history,
            zipkin,
        }
    }
}

impl SpanExporter for OtelExportMultiplexer {
    fn export(&self, batch: Vec<SpanData>) -> impl futures::Future<Output = OTelSdkResult> + Send {
        let history_future = self.task_history.export(batch.clone());
        let zipkin_future = self.zipkin.as_ref().map(|exporter| exporter.export(batch));

        async move {
            if let Some(zipkin_future) = zipkin_future {
                let _ = zipkin_future.await;
            }

            let _ = history_future.await;

            Ok(())
        }
    }

    fn shutdown(&mut self) -> OTelSdkResult {
        if let Some(exporter) = &mut self.zipkin {
            let _ = exporter.shutdown();
        }

        let _ = self.task_history.shutdown();

        Ok(())
    }

    fn force_flush(&mut self) -> OTelSdkResult {
        if let Some(exporter) = &mut self.zipkin {
            let _ = exporter.force_flush();
        }

        let _ = self.task_history.force_flush();

        Ok(())
    }

    fn set_resource(&mut self, resource: &Resource) {
        if let Some(exporter) = &mut self.zipkin {
            exporter.set_resource(resource);
        }

        self.task_history.set_resource(resource);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn returns_very_verbose_if_flag_set() {
        unsafe {
            std::env::set_var("TEST_LOG_ENV", "custom");
        }
        let result = LogVerbosity::from_flags_and_env_and_config(
            false,
            true,
            "TEST_LOG_ENV",
            Some(OutputLevel::Verbose),
        );
        unsafe {
            std::env::remove_var("TEST_LOG_ENV");
        }

        assert_eq!(result, LogVerbosity::VeryVerbose);
    }

    #[test]
    fn returns_specific_if_env_set() {
        unsafe {
            std::env::set_var("TEST_LOG_ENV", "custom");
        }
        let result = LogVerbosity::from_flags_and_env_and_config(
            false,
            false,
            "TEST_LOG_ENV",
            Some(OutputLevel::VeryVerbose),
        );
        unsafe {
            std::env::remove_var("TEST_LOG_ENV");
        }

        assert_eq!(result, LogVerbosity::Specific("custom".to_string()));
    }

    #[test]
    fn returns_very_verbose_from_config() {
        let result = LogVerbosity::from_flags_and_env_and_config(
            false,
            false,
            "NON_EXISTENT_ENV",
            Some(OutputLevel::VeryVerbose),
        );
        assert_eq!(result, LogVerbosity::VeryVerbose);
    }

    #[test]
    fn returns_default_when_none() {
        let result =
            LogVerbosity::from_flags_and_env_and_config(false, false, "NON_EXISTENT_ENV", None);
        assert_eq!(result, LogVerbosity::Default);
    }
}
