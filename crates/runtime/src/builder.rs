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

use std::{collections::HashMap, net::SocketAddr, str::FromStr, sync::Arc, time::Duration};

use app::App;
use tokio::sync::{Mutex, RwLock};

use crate::{
    Runtime, catalogconnector,
    dataaccelerator::AcceleratorEngineRegistry,
    dataconnector,
    datafusion::DataFusion,
    datasets_health_monitor::DatasetsHealthMonitor,
    extension::{Extension, ExtensionFactory},
    flight::RateLimits,
    metrics, podswatcher,
    secrets::{self, Secrets},
    status,
    timing::TimeMeasurement,
    tracers,
};

type DatafusionConfigurationCallback = fn(&mut DataFusion);

pub struct RuntimeBuilder {
    app: Option<Arc<app::App>>,
    autoload_extensions: HashMap<String, Box<dyn ExtensionFactory>>,
    extensions: Vec<Box<dyn ExtensionFactory>>,
    pods_watcher: Option<podswatcher::PodsWatcher>,
    datasets_health_monitor_enabled: bool,
    metrics_endpoint: Option<SocketAddr>,
    prometheus_registry: Option<prometheus::Registry>,
    runtime_status: Arc<status::RuntimeStatus>,
    rate_limits: Option<Arc<RateLimits>>,
    accelerator_engine_registry: Arc<AcceleratorEngineRegistry>,
    datafusion_configuration_fn: Option<DatafusionConfigurationCallback>,
}

impl RuntimeBuilder {
    pub fn new() -> Self {
        RuntimeBuilder {
            app: None,
            extensions: vec![],
            pods_watcher: None,
            datasets_health_monitor_enabled: false,
            metrics_endpoint: None,
            prometheus_registry: None,
            autoload_extensions: HashMap::new(),
            runtime_status: status::RuntimeStatus::new(),
            rate_limits: None,
            accelerator_engine_registry: Arc::new(AcceleratorEngineRegistry::new()),
            datafusion_configuration_fn: None,
        }
    }

    pub fn with_app(mut self, app: app::App) -> Self {
        self.app = Some(Arc::new(app));
        self
    }

    pub fn with_app_opt(mut self, app: Option<Arc<app::App>>) -> Self {
        self.app = app;
        self
    }

    pub fn with_extensions(mut self, extensions: Vec<Box<dyn ExtensionFactory>>) -> Self {
        self.extensions = extensions;
        self
    }

    /// Extensions that will be automatically loaded if a component requests them and the user hasn't explicitly loaded it.
    pub fn with_autoload_extensions(
        mut self,
        extensions: HashMap<String, Box<dyn ExtensionFactory>>,
    ) -> Self {
        self.autoload_extensions = extensions;
        self
    }

    pub fn with_pods_watcher(mut self, pods_watcher: podswatcher::PodsWatcher) -> Self {
        self.pods_watcher = Some(pods_watcher);
        self
    }

    pub fn with_datasets_health_monitor(mut self) -> Self {
        self.datasets_health_monitor_enabled = true;
        self
    }

    pub fn with_metrics_server(
        mut self,
        metrics_endpoint: SocketAddr,
        prometheus_registry: prometheus::Registry,
    ) -> Self {
        self.metrics_endpoint = Some(metrics_endpoint);
        self.prometheus_registry = Some(prometheus_registry);
        self
    }

    pub fn with_metrics_server_opt(
        mut self,
        metrics_endpoint: Option<SocketAddr>,
        prometheus_registry: Option<prometheus::Registry>,
    ) -> Self {
        self.metrics_endpoint = metrics_endpoint;
        self.prometheus_registry = prometheus_registry;
        self
    }

    /// Used to configure `DataFusion` in integration tests & test CI
    pub fn with_datafusion_configuration_fn(
        mut self,
        callback: DatafusionConfigurationCallback,
    ) -> Self {
        self.datafusion_configuration_fn = Some(callback);
        self
    }

    pub fn with_rate_limits(mut self, rate_limits: RateLimits) -> Self {
        self.rate_limits = Some(Arc::new(rate_limits));
        self
    }

    pub async fn build(self) -> Runtime {
        self.accelerator_engine_registry.register_all().await;
        dataconnector::register_all().await;
        catalogconnector::register_all().await;
        document_parse::register_all().await;

        let memory_limit = self
            .app
            .as_ref()
            .and_then(|app| parse_memory_limit(app.runtime.memory_limit.clone()));

        let temp_directory = self
            .app
            .as_ref()
            .and_then(|app| app.runtime.temp_directory.clone());

        let mut df = DataFusion::builder(
            Arc::clone(&self.runtime_status),
            Arc::clone(&self.accelerator_engine_registry),
        )
        .memory_limit(memory_limit)
        .temp_directory(temp_directory)
        .build();

        if let Some(callback) = self.datafusion_configuration_fn {
            callback(&mut df);
        }

        let df = Arc::new(df);

        let datasets_health_monitor = if self.datasets_health_monitor_enabled {
            let is_task_history_enabled = self
                .app
                .as_ref()
                .is_some_and(|app| app.runtime.task_history.enabled);
            let datasets_health_monitor = DatasetsHealthMonitor::new(Arc::clone(&df))
                .with_task_history_enabled(is_task_history_enabled);
            datasets_health_monitor.start();
            Some(Arc::new(datasets_health_monitor))
        } else {
            None
        };

        let secrets = Self::load_secrets(self.app.as_ref()).await;

        let evals = self
            .app
            .as_ref()
            .map(|a| a.evals.clone())
            .unwrap_or_default();

        let mut rt = Runtime {
            app: Arc::new(RwLock::new(self.app)),
            df,
            models: Arc::new(RwLock::new(HashMap::new())),
            llms: Arc::new(RwLock::new(HashMap::new())),
            embeds: Arc::new(RwLock::new(HashMap::new())),
            evals: Arc::new(RwLock::new(evals)),
            eval_scorers: Arc::new(RwLock::new(HashMap::new())),
            tools: Arc::new(RwLock::new(HashMap::new())),
            tool_factories: Arc::new(Mutex::new(HashMap::new())),
            pods_watcher: Arc::new(RwLock::new(self.pods_watcher)),
            secrets: Arc::new(RwLock::new(secrets)),
            spaced_tracer: Arc::new(tracers::SpacedTracer::new(Duration::from_secs(15))),
            autoload_extensions: Arc::new(self.autoload_extensions),
            extensions: Arc::new(RwLock::new(HashMap::new())),
            datasets_health_monitor,
            metrics_endpoint: self.metrics_endpoint,
            prometheus_registry: self.prometheus_registry,
            rate_limits: self.rate_limits.unwrap_or_default(),
            status: self.runtime_status,
            runtime_tasks: Arc::new(RwLock::new(HashMap::new())),
            accelerator_engine_registry: self.accelerator_engine_registry,
        };

        let mut extensions: HashMap<String, Arc<dyn Extension>> = HashMap::new();
        for factory in self.extensions {
            let mut extension = factory.create();
            let extension_name = extension.name();
            if let Err(err) = extension.initialize(&rt).await {
                eprintln!("Failed to initialize extension {extension_name}: {err}");
            } else {
                extensions.insert(extension_name.into(), extension.into());
            }
        }
        rt.extensions = Arc::new(RwLock::new(extensions));

        rt
    }

    async fn load_secrets(app: Option<&Arc<App>>) -> Secrets {
        let _guard = TimeMeasurement::new(&metrics::secrets::STORES_LOAD_DURATION_MS, &[]);
        let mut secrets = secrets::Secrets::new();

        if let Some(app) = app {
            if let Err(e) = secrets.load_from(&app.secrets).await {
                eprintln!("Error loading secret stores: {e}");
            }
        }

        secrets
    }
}

impl Default for RuntimeBuilder {
    fn default() -> Self {
        Self::new()
    }
}

fn parse_memory_limit(memory_limit: Option<String>) -> Option<u64> {
    let memory_limit = memory_limit?;
    let original_memory_limit = memory_limit.clone();

    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    let memory_limit = byte_unit::Byte::from_str(&memory_limit)
        .ok()
        // losing the fractional part of a byte is not a problem
        .map(|v| v.get_adjusted_unit(byte_unit::Unit::B).get_value() as u64);

    if memory_limit.is_none() {
        tracing::warn!(
            "An invalid Runtime memory limit was specified: {original_memory_limit}\n A memory limit must be specified as an integer in GB, MB, or KB size."
        );
    }

    if memory_limit == Some(0) {
        tracing::warn!(
            "A Runtime memory limit of 0 was specified: {original_memory_limit}\n A memory limit must be greater than 0."
        );

        None
    } else {
        memory_limit
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_parse_memory_limit() {
        let test_cases: Vec<(Option<&str>, Option<u64>)> = vec![
            // bytes
            (Some("1GB"), Some(1_000_000_000)),
            (Some("1G"), Some(1_000_000_000)),
            (Some("1MB"), Some(1_000_000)),
            (Some("1M"), Some(1_000_000)),
            (Some("1KB"), Some(1_000)),
            (Some("1K"), Some(1_000)),
            (Some("1B"), Some(1)),
            // bits
            (Some("1gb"), Some(125_000_000)),
            (Some("1mb"), Some(125_000)),
            (Some("1kb"), Some(125)),
            (Some("1b"), Some(1)),
            // kibi, gibi, mebi
            (Some("1GiB"), Some(1_073_741_824)),
            (Some("1Gi"), Some(1_073_741_824)),
            (Some("1MiB"), Some(1_048_576)),
            (Some("1Mi"), Some(1_048_576)),
            (Some("1KiB"), Some(1024)),
            (Some("1Ki"), Some(1024)),
            // without a b identifier, defaults to bytes
            (Some("1g"), Some(1_000_000_000)),
            (Some("1m"), Some(1_000_000)),
            (Some("1k"), Some(1_000)),
            (Some("1"), Some(1)),
            (Some("0"), None),
            (Some("-1"), None),
            (Some("invalid"), None),
            (None, None),
        ];

        for (input, expected) in test_cases {
            let result = parse_memory_limit(input.map(ToString::to_string));
            assert_eq!(result, expected, "Input: {input:?}");
        }
    }
}
