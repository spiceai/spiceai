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

#![recursion_limit = "256"]

use runtime::datafusion::builder::DEFAULT_DATAFUSION_CONFIG;
use tracing::subscriber::DefaultGuard;
use tracing_subscriber::EnvFilter;

mod docker;
#[cfg(feature = "kafka")]
mod kafka;
#[cfg(feature = "models")]
mod models;
#[cfg(feature = "models")]
mod search;
mod utils;
#[cfg(feature = "models")]
mod workers;

pub(crate) const DEFAULT_TRACING_MODELS: Option<&str> = Some(
    "integration_models=debug,runtime=TRACE,search=TRACE,llms=TRACE,task_history=WARN,runtime::embeddings=INFO,INFO",
);

/// The CPU entitlement every test in this binary is pinned to.
///
/// Sizing derived from the CPU budget — `target_partitions` above all, but also
/// worker-thread counts and encode permits — would otherwise follow the host and
/// make explain-plan snapshots machine-dependent.
const TEST_CPU_CORES: usize = 3;

/// Modifies the `DataFusion` configuration to make test results reproducible across all machines.
///
/// 1) Pins the CPU budget, and with it `target_partitions`, to [`TEST_CPU_CORES`].
/// 2) Disables coalesce batches and repartition joins for terser plans.
fn configure_test_datafusion() {
    pin_test_cpu_budget();

    match DEFAULT_DATAFUSION_CONFIG.write() {
        Ok(mut config) => {
            config.options_mut().execution.target_partitions = TEST_CPU_CORES;

            config.options_mut().execution.coalesce_batches = false;

            config.options_mut().optimizer.repartition_joins = false;
        }
        _ => panic!("Must obtain write lock to defaults"),
    }
}

/// Pin the process-wide CPU budget to [`TEST_CPU_CORES`].
///
/// Setting `target_partitions` on the default session config is not enough on its
/// own: with `runtime.query.target_partitions` unset the session builder sizes
/// partitions from the CPU budget, overwriting whatever the config carried. Both
/// are pinned to the same constant so they cannot disagree.
///
/// Installing is idempotent by intent — the budget is a process-wide `OnceLock`
/// and every caller asks for the same value, so each call after the first is an
/// expected no-op rather than an error worth surfacing.
fn pin_test_cpu_budget() {
    let config = cpu_budget::CpuConfig::from_sources(None, None, Some(&TEST_CPU_CORES.to_string()));
    match cpu_budget::CpuBudget::resolve(&config, &cpu_budget::HostReadings::detect()) {
        Ok(budget) => drop(budget.install()),
        Err(e) => panic!("{TEST_CPU_CORES} must be a valid CPU quantity: {e}"),
    }
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
