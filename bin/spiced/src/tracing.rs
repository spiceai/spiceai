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

// OpenTelemetry 0.32 deprecated the Zipkin exporter in favor of OTLP. Spice still
// supports Zipkin task-history export; migrating to OTLP is tracked separately.
#![expect(
    deprecated,
    reason = "Zipkin exporter deprecated in opentelemetry 0.32; Spice still supports Zipkin export"
)]

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
use runtime_query_engine::query_engine::QueryEngine;
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
    "cayenne",
    "cache",
    "extensions",
    "spice_cloud",
    "llms",
    "tpc_extension",
    "workers",
    "search",
    "ballista",
    "datafusion",
    "runtime_rate_control",
];

const OFF_FILTERS: &str = "reqwest_retry::middleware=off,opentelemetry=warn,opentelemetry_sdk=off,delta_kernel::log_segment=off,delta_kernel::listed_log_files=off,aws_config::imds::region=off,aws_config::meta::credentials::chain=off,tower::buffer=off,h2::codec=off";
const OFF_UNLESS_VERY_VERBOSE_FILTERS: &str = "datafusion_datasource::source=off,datafusion_optimizer::utils=off,datafusion_optimizer::optimizer=off,datafusion::physical_planner=off,tantivy=warn,text_embeddings_backend_candle=error";

fn specific_env_filter(filter: &str) -> String {
    format!("{OFF_FILTERS},{filter}")
}

fn env_filter_string(v: &LogVerbosity) -> String {
    fn internal_components(level: &str) -> String {
        INTERNAL_COMPONENTS
            .iter()
            .map(|component| format!("{component}={level}"))
            .collect::<Vec<_>>()
            .join(",")
    }

    match v {
        LogVerbosity::Default => format!(
            "{},{OFF_FILTERS},{OFF_UNLESS_VERY_VERBOSE_FILTERS},WARN",
            internal_components("INFO")
        ),
        LogVerbosity::Verbose => format!(
            "{},{OFF_FILTERS},{OFF_UNLESS_VERY_VERBOSE_FILTERS},INFO",
            internal_components("DEBUG")
        ),
        LogVerbosity::VeryVerbose => {
            format!("{},{OFF_FILTERS},DEBUG", internal_components("TRACE"))
        }
        LogVerbosity::Specific(filter) => specific_env_filter(filter),
    }
}

impl From<LogVerbosity> for EnvFilter {
    fn from(v: LogVerbosity) -> Self {
        EnvFilter::new(env_filter_string(&v))
    }
}

fn specific_filter_enables_trace_logging(filter: &str) -> bool {
    filter.split(',').map(str::trim).any(|directive| {
        matches!(
            directive.rsplit('=').next().map(str::trim),
            Some(level) if level.eq_ignore_ascii_case("trace")
        )
    })
}

fn should_include_otel_location(is_release_build: bool, verbosity: &LogVerbosity) -> bool {
    if !is_release_build {
        return true;
    }

    match verbosity {
        LogVerbosity::Default | LogVerbosity::Verbose => false,
        LogVerbosity::VeryVerbose => true,
        LogVerbosity::Specific(filter) => specific_filter_enables_trace_logging(filter),
    }
}

/// Build the Cloud Connect log-capture layer, or `None` when Cloud Connect
/// is not configured for this instance.
///
/// When present, the layer mirrors console output into a bounded in-memory
/// ring buffer (ANSI stripped) that the `GetLogs` control message reads.
/// It is added *alongside* the terminal `fmt` layer, so normal logging is
/// unchanged. The same `task_history` exclusion as the console layer is
/// applied so span-only records don't pollute the log tail.
fn cloud_connect_log_capture_layer<S>(
    cloud_connect_flag: bool,
) -> Option<Box<dyn Layer<S> + Send + Sync>>
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    if !crate::cloud_connect::is_configured(cloud_connect_flag) {
        return None;
    }
    let ring = crate::log_capture::install(crate::log_capture::DEFAULT_CAPACITY);
    Some(
        fmt::layer()
            .with_ansi(false)
            .with_writer(ring)
            .with_filter(filter::filter_fn(|metadata| {
                metadata.target() != "task_history"
            }))
            .boxed(),
    )
}

/// The layer that writes the human-readable log to `writer`, `spiced`'s stdout
/// in production.
///
/// `ansi` decides whether each line carries SGR escapes. It is a parameter
/// rather than a literal because the answer depends on where the writer points:
/// escapes are what make an interactive log readable, and what make a redirected
/// one unreadable — a captured `spice.log` full of `\x1b[2m` defeats any pattern
/// written the way the line reads.
fn console_layer<S, W>(ansi: bool, writer: W) -> impl Layer<S>
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    W: for<'a> fmt::MakeWriter<'a> + Send + Sync + 'static,
{
    fmt::layer()
        .with_ansi(ansi)
        .with_writer(writer)
        .with_filter(filter::filter_fn(|metadata| {
            metadata.target() != "task_history"
        }))
}

/// Whether the task-history sink layer is installed, i.e. whether spans and
/// events on the `task_history` target are persisted to the
/// `runtime.task_history` table.
///
/// This is the *only* thing `runtime.task_history.enabled` governs. Every other
/// layer is installed either way — see [`init_tracing`].
fn task_history_sink_enabled(app: Option<&Arc<App>>) -> bool {
    app.is_none_or(|app| app.runtime.task_history.enabled)
}

/// Republishes events on the `task_history` target to the in-memory event
/// stream keyed by their span, which is what `/v1/chat/completions` reads to
/// stream a completion's intermediate progress (`event_stream::get_event_stream`).
///
/// It shares the `task_history` target but writes no task-history row, so it is
/// independent of `runtime.task_history.enabled`.
fn progress_layer<S>() -> impl Layer<S>
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    event_stream::EventStreamLayer::new("progress").with_filter(filter::filter_fn(|metadata| {
        metadata.target() == "task_history"
    }))
}

pub(crate) async fn init_tracing(
    app: Option<&Arc<App>>,
    config: Option<&TracingConfig>,
    df: Arc<DataFusion>,
    verbosity: LogVerbosity,
    cloud_connect_flag: bool,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let include_otel_location = should_include_otel_location(!cfg!(debug_assertions), &verbosity);
    let filter: EnvFilter = verbosity.into();
    // The console layer writes to stdout, so stdout is the stream whose
    // terminal-ness decides this — not stderr. `NO_COLOR` and `FORCE_COLOR`
    // override it, and the answer is cached process-wide, which keeps this
    // subscriber and the CLI's own painted output in agreement.
    let ansi = ansi_colors::colors_enabled_for(ansi_colors::Target::Stdout);

    // One stack, one place that installs the `log` bridge. Only the
    // task-history sink is conditional: the progress event stream and
    // `LogTracer` serve consumers that never read the task-history table, so
    // gating them on the same setting silently turns off dependency logging and
    // chat progress streaming for anyone who disables the table.
    let task_history_layer = if task_history_sink_enabled(app) {
        Some(datafusion_task_history_tracing(df, app, config, include_otel_location).await?)
    } else {
        None
    };

    let subscriber = tracing_subscriber::registry()
        .with(filter)
        .with(task_history_layer)
        .with(progress_layer())
        .with(console_layer(ansi, std::io::stdout))
        .with(cloud_connect_log_capture_layer(cloud_connect_flag));

    tracing::subscriber::set_global_default(subscriber)?;
    // Routes `log` records — which most of the dependency graph, DataFusion
    // included, emits instead of `tracing` — into the subscriber above. Nothing
    // else installs a global `log` logger, so without this every one of them is
    // discarded.
    LogTracer::init()?;

    Ok(())
}

async fn datafusion_task_history_tracing<S>(
    df: Arc<DataFusion>,
    app: Option<&Arc<App>>,
    config: Option<&TracingConfig>,
    include_otel_location: bool,
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

    let captured_context = app
        .as_ref()
        .map(|app| app.runtime.task_history.get_captured_context())
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

    df.set_plan_capture_config(
        runtime::datafusion::query::plan_capture::PlanCaptureConfig {
            captured_plan: captured_plan.clone(),
            min_plan_duration_ms,
            min_sql_duration_ms,
        },
    );

    // Compute node_id for cluster mode: "host:port"
    let node_id: Option<Arc<str>> = df.cluster_config.effective_role().and_then(|_| {
        let host = df.cluster_config.node_advertise_address()?;
        let port = df.cluster_config.node_bind_address().port();
        Some(format!("{host}:{port}").into())
    });

    let (ballista_transform, ballista_retention) =
        runtime::datafusion::query::stage_history::BallistaStageMiddleware::pair();
    let query_engine = std::sync::Arc::clone(&df) as std::sync::Arc<dyn QueryEngine>;
    let task_history_exporter = task_history::otel_exporter::TaskHistoryExporter::new(
        query_engine,
        captured_output,
        captured_context,
        min_sql_duration_ms,
        captured_plan,
        min_plan_duration_ms,
        node_id,
    )
    .with_transform(ballista_transform)
    .with_retention(ballista_retention);

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
        .with_location(include_otel_location)
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

    let collector_endpoint: String = zipkin_endpoint.clone();

    Ok(Some(
        ZipkinExporter::builder()
            .with_collector_endpoint(collector_endpoint)
            .with_http_client(
                Client::builder()
                    .connect_timeout(Duration::from_secs(10))
                    .timeout(Duration::from_secs(30))
                    .build()?,
            )
            .build()?,
    ))
}

async fn is_zipkin_endpoint_reachable(endpoint: &str) -> bool {
    let Ok(client) = Client::builder()
        .connect_timeout(Duration::from_secs(5))
        .timeout(Duration::from_secs(5))
        .build()
    else {
        return false;
    };

    let url = format!("{endpoint}?serviceName=test");

    match client.get(&url).send().await {
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
            if let Some(zipkin_future) = zipkin_future
                && let Err(e) = zipkin_future.await
            {
                tracing::warn!("Failed to send traces to Zipkin: {e}");
            }

            if let Err(e) = history_future.await {
                tracing::warn!("Failed to write to task history: {e}");
            }

            Ok(())
        }
    }

    fn shutdown(&self) -> OTelSdkResult {
        if let Some(exporter) = self.zipkin.as_ref() {
            let _ = exporter.shutdown();
        }

        let _ = self.task_history.shutdown();

        Ok(())
    }

    fn force_flush(&self) -> OTelSdkResult {
        if let Some(exporter) = self.zipkin.as_ref() {
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
    use futures::StreamExt as _;
    use std::sync::Mutex;
    use tracing_subscriber::fmt::MakeWriter;

    /// Sink for a probe subscriber's console output, so a test can assert on
    /// what the `fmt` layer actually wrote.
    #[derive(Clone, Default)]
    struct ProbeWriter(Arc<Mutex<Vec<u8>>>);

    impl ProbeWriter {
        fn contents(&self) -> String {
            String::from_utf8_lossy(&self.0.lock().expect("probe buffer poisoned")).into_owned()
        }
    }

    impl std::io::Write for ProbeWriter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0
                .lock()
                .expect("probe buffer poisoned")
                .extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl<'a> MakeWriter<'a> for ProbeWriter {
        type Writer = Self;

        fn make_writer(&'a self) -> Self::Writer {
            self.clone()
        }
    }

    /// Emits a `WARN` and an `ERROR` event on the `text_embeddings_backend_candle`
    /// target through a subscriber assembled the way `init_tracing` assembles it —
    /// the `EnvFilter` for `$verbosity` in front of the console `fmt` layer — and
    /// evaluates to everything that reached the console.
    ///
    /// A macro rather than a function because `tracing` bakes the event target into
    /// a `static` callsite, so it cannot be passed in as a value; each expansion
    /// then also gets its own callsite, which keeps the process-wide callsite
    /// interest cache from carrying one verbosity's verdict into the next.
    macro_rules! candle_console_output {
        ($verbosity:expr) => {{
            let probe = ProbeWriter::default();
            let env_filter: EnvFilter = $verbosity.into();
            let subscriber = tracing_subscriber::registry()
                .with(env_filter)
                .with(console_layer(false, probe.clone()));

            tracing::subscriber::with_default(subscriber, || {
                tracing::warn!(
                    target: "text_embeddings_backend_candle",
                    "the config.json contains hidden_act=gelu"
                );
                tracing::error!(
                    target: "text_embeddings_backend_candle",
                    "could not load model weights"
                );
            });

            probe.contents()
        }};
    }

    #[test]
    fn candle_gelu_notice_is_hidden_unless_very_verbose() {
        let default = candle_console_output!(LogVerbosity::Default);
        assert!(
            !default.contains("hidden_act=gelu"),
            "default verbosity should drop the candle GeLU notice, got: {default}"
        );
        assert!(
            default.contains("could not load model weights"),
            "default verbosity must still surface candle errors, got: {default}"
        );

        let verbose = candle_console_output!(LogVerbosity::Verbose);
        assert!(
            !verbose.contains("hidden_act=gelu"),
            "verbose should drop the candle GeLU notice, got: {verbose}"
        );
        assert!(
            verbose.contains("could not load model weights"),
            "verbose must still surface candle errors, got: {verbose}"
        );

        let very_verbose = candle_console_output!(LogVerbosity::VeryVerbose);
        assert!(
            very_verbose.contains("hidden_act=gelu"),
            "very verbose should reveal the candle GeLU notice, got: {very_verbose}"
        );
        assert!(
            very_verbose.contains("could not load model weights"),
            "very verbose must still surface candle errors, got: {very_verbose}"
        );
    }

    /// Runs `console_layer` the way `init_tracing` runs it and returns what
    /// reached the writer: a `runtime` event, plus a `task_history` event that
    /// the layer's filter must keep out of the console.
    fn console_layer_output(ansi: bool) -> String {
        let probe = ProbeWriter::default();
        let subscriber = tracing_subscriber::registry().with(console_layer(ansi, probe.clone()));

        tracing::subscriber::with_default(subscriber, || {
            tracing::warn!(
                target: "runtime::accelerated::refresh_task",
                "Failed to load data for dataset taxi_trips"
            );
            tracing::info!(target: "task_history", "sql_query");
        });

        probe.contents()
    }

    /// A redirected `spice.log` must be plain text. Colouring it unconditionally
    /// puts escapes between the level and the target, so a pattern written the
    /// way the line reads cannot match it.
    #[test]
    fn console_layer_omits_ansi_escapes_when_the_sink_is_not_a_terminal() {
        let plain = console_layer_output(false);

        assert!(
            !plain.contains('\x1b'),
            "an uncoloured console layer must emit no escape sequences, got: {plain:?}"
        );
        assert!(
            plain.contains("Failed to load data for dataset taxi_trips"),
            "the event itself must still reach the console, got: {plain}"
        );
        assert!(
            !plain.contains("sql_query"),
            "task_history records belong to the task-history table, not the console, got: {plain}"
        );
    }

    /// The other half: the flag is what decides, so an interactive terminal keeps
    /// the colours it had before. A layer that dropped `ansi` on the floor would
    /// pass the test above for the wrong reason.
    #[test]
    fn console_layer_emits_ansi_escapes_when_the_sink_is_a_terminal() {
        let coloured = console_layer_output(true);

        assert!(
            coloured.contains('\x1b'),
            "a coloured console layer must emit escape sequences, got: {coloured:?}"
        );
        assert!(
            coloured.contains("Failed to load data for dataset taxi_trips"),
            "the event itself must still reach the console, got: {coloured}"
        );
    }

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

    #[test]
    fn includes_otel_location_for_non_release_builds() {
        assert!(should_include_otel_location(false, &LogVerbosity::Default));
    }

    #[test]
    fn excludes_otel_location_for_release_builds_without_trace_logging() {
        assert!(!should_include_otel_location(true, &LogVerbosity::Default));
        assert!(!should_include_otel_location(true, &LogVerbosity::Verbose));
        assert!(!should_include_otel_location(
            true,
            &LogVerbosity::Specific("warn,task_history=info".to_string())
        ));
        assert!(!should_include_otel_location(
            true,
            &LogVerbosity::Specific("warn,task_history=debug".to_string())
        ));
    }

    #[test]
    fn includes_otel_location_for_release_trace_logging() {
        assert!(should_include_otel_location(
            true,
            &LogVerbosity::VeryVerbose
        ));
        assert!(should_include_otel_location(
            true,
            &LogVerbosity::Specific("trace".to_string())
        ));
        assert!(should_include_otel_location(
            true,
            &LogVerbosity::Specific("warn,task_history=trace".to_string())
        ));
    }

    #[test]
    fn specific_filter_keeps_off_filters() {
        assert_eq!(specific_env_filter("trace"), format!("{OFF_FILTERS},trace"));
    }

    #[test]
    fn verbose_filter_suppresses_opentelemetry_info() {
        assert!(env_filter_string(&LogVerbosity::Default).contains("opentelemetry=warn"));
        assert!(env_filter_string(&LogVerbosity::Verbose).contains("opentelemetry=warn"));
        assert!(env_filter_string(&LogVerbosity::VeryVerbose).contains("opentelemetry=warn"));
    }

    #[test]
    fn filters_tantivy_to_error() {
        assert!(env_filter_string(&LogVerbosity::Default).contains("tantivy=warn"));
        assert!(env_filter_string(&LogVerbosity::Verbose).contains("tantivy=warn"));
        assert!(!env_filter_string(&LogVerbosity::VeryVerbose).contains("tantivy=warn"));
    }

    fn app_with_task_history(enabled: bool) -> Arc<App> {
        let mut app = app::AppBuilder::new("tracing-test").build();
        app.runtime.task_history.enabled = enabled;
        Arc::new(app)
    }

    #[test]
    fn task_history_sink_follows_only_its_own_setting() {
        assert!(
            task_history_sink_enabled(None),
            "no app yet means no opt-out to honour"
        );
        assert!(task_history_sink_enabled(Some(&app_with_task_history(
            true
        ))));
        assert!(!task_history_sink_enabled(Some(&app_with_task_history(
            false
        ))));
    }

    /// The chat streaming API's progress events must survive
    /// `runtime.task_history.enabled: false`: they are read from an in-memory
    /// stream keyed by the span, not from the task-history table. This asserts
    /// the layer that publishes them, which [`init_tracing`] installs whatever
    /// the setting is.
    #[tokio::test]
    async fn progress_layer_publishes_task_history_events_to_the_event_stream() {
        let subscriber = tracing_subscriber::registry().with(progress_layer());

        // The span is dropped with the closure, which closes the channel, so
        // the stream below terminates on the events already buffered.
        let events = tracing::subscriber::with_default(subscriber, || {
            let span = tracing::info_span!(target: "task_history", "sql_query");
            span.in_scope(|| {
                let events =
                    event_stream::get_event_stream().expect("an entered span has an event stream");
                tracing::info!(target: "task_history", progress = "loading model");
                // Same span and same field, but not the task_history target.
                tracing::info!(target: "spiced", progress = "not progress");
                events
            })
        });

        // A deadlock guard, not a wait: the channel is already closed, so the
        // stream terminates on the buffered events. Keep it short so a
        // regression that leaves the stream open fails fast.
        let published: Vec<String> =
            tokio::time::timeout(std::time::Duration::from_secs(1), events.collect())
                .await
                .expect("the progress stream must end when its span closes");
        assert_eq!(
            published,
            vec!["loading model".to_string()],
            "the task_history target — and only it — feeds the progress stream"
        );
    }
}
