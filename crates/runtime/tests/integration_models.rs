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

use tracing::subscriber::DefaultGuard;
use tracing_subscriber::EnvFilter;
#[cfg(feature = "models")]
mod models;
mod utils;
#[cfg(feature = "models")]
mod workers;

pub(crate) const DEFAULT_TRACING_MODELS: Option<&str> = Some(
    "runtime=TRACE,search=TRACE,llms=TRACE,model_components=TRACE,task_history=WARN,runtime::embeddings=INFO,INFO",
);

/// Modifies the `DataFusion` configuration to make test results reproducible across all machines.
///
/// 1) Sets the number of `target_partitions` to 3, by default its the number of CPU cores available.
fn configure_test_datafusion(df: &mut runtime::datafusion::DataFusion) {
    let state = df.ctx.state_ref();
    let mut state_lock = state.write();
    state_lock
        .config_mut()
        .options_mut()
        .execution
        .target_partitions = 3;
}

fn init_tracing(default_level: Option<&str>) -> DefaultGuard {
    let filter = match (default_level, std::env::var("SPICED_LOG").ok()) {
        (_, Some(log)) => EnvFilter::new(log),
        (Some(level), None) => EnvFilter::new(level),
        _ => EnvFilter::new(DEFAULT_TRACING_MODELS.unwrap_or_default()),
    };

    let subscriber = tracing_subscriber::FmtSubscriber::builder()
        .with_env_filter(filter)
        .with_ansi(true)
        .finish();
    tracing::subscriber::set_default(subscriber)
}
