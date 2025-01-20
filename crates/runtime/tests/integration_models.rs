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

#![allow(clippy::large_futures)]

use opentelemetry::InstrumentationScope;
use opentelemetry_sdk::{runtime::TokioCurrentThread, trace::TracerProvider};
use runtime::{task_history::otel_exporter::TaskHistoryExporter, Runtime};
use spicepod::component::runtime::TaskHistoryCapturedOutput;
use tracing::subscriber::DefaultGuard;
use tracing_subscriber::{filter, fmt, layer::SubscriberExt, EnvFilter, Layer};

#[cfg(feature = "models")]
mod models;
mod utils;

fn init_tracing(default_level: Option<&str>) -> DefaultGuard {
    let filter = match (default_level, std::env::var("SPICED_LOG").ok()) {
        (_, Some(log)) => EnvFilter::new(log),
        (Some(level), None) => EnvFilter::new(level),
        _ => EnvFilter::new("runtime=TRACE,llms=TRACE,model_components=TRACE,task_history=WARN,runtime::embeddings=INFO,INFO"),
    };

    let subscriber = tracing_subscriber::FmtSubscriber::builder()
        .with_env_filter(filter)
        .with_ansi(true)
        .finish();
    tracing::subscriber::set_default(subscriber)
}

fn init_tracing_with_task_history(
    default_level: Option<&str>,
    rt: &Runtime,
) -> (DefaultGuard, TracerProvider) {
    let filter = match (default_level, std::env::var("SPICED_LOG").ok()) {
        (_, Some(log)) => EnvFilter::new(log),
        (Some(level), None) => EnvFilter::new(level),
        _ => EnvFilter::new("runtime=TRACE,llms=TRACE,model_components=TRACE,INFO"),
    };

    let fmt_layer = fmt::layer().with_ansi(true).with_filter(filter);

    let task_history_exporter =
        TaskHistoryExporter::new(rt.datafusion(), TaskHistoryCapturedOutput::Truncated);

    // Tests hang if we don't use TokioCurrentThread here (similar to https://github.com/open-telemetry/opentelemetry-rust/issues/868)
    let provider = TracerProvider::builder()
        .with_batch_exporter(task_history_exporter, TokioCurrentThread)
        .build();

    let scope = InstrumentationScope::builder("task_history")
        .with_version(env!("CARGO_PKG_VERSION"))
        .build();
    let tracer = opentelemetry::trace::TracerProvider::tracer_with_scope(&provider, scope);

    let task_history_layer = tracing_opentelemetry::layer()
        .with_tracer(tracer)
        .with_filter(filter::filter_fn(|metadata| {
            metadata.target() == "task_history"
        }));

    let subscriber = tracing_subscriber::registry()
        .with(fmt_layer)
        .with(task_history_layer);

    let guard = tracing::subscriber::set_default(subscriber);

    (guard, provider)
}
