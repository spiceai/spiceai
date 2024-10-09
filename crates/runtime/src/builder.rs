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

use std::{collections::HashMap, net::SocketAddr, sync::Arc, time::Duration};

use app::App;
use tokio::sync::RwLock;

use crate::{
    dataaccelerator, dataconnector,
    datafusion::DataFusion,
    datasets_health_monitor::DatasetsHealthMonitor,
    extension::{Extension, ExtensionFactory},
    metrics, podswatcher,
    secrets::{self, Secrets},
    status,
    timing::TimeMeasurement,
    tools, tracers, Runtime,
};

pub struct RuntimeBuilder {
    app: Option<Arc<app::App>>,
    autoload_extensions: HashMap<String, Box<dyn ExtensionFactory>>,
    extensions: Vec<Box<dyn ExtensionFactory>>,
    pods_watcher: Option<podswatcher::PodsWatcher>,
    datasets_health_monitor_enabled: bool,
    metrics_endpoint: Option<SocketAddr>,
    prometheus_registry: Option<prometheus::Registry>,
    datafusion: Option<Arc<DataFusion>>,
    runtime_status: Option<Arc<status::RuntimeStatus>>,
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
            datafusion: None,
            autoload_extensions: HashMap::new(),
            runtime_status: None,
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

    pub fn with_datafusion(mut self, datafusion: Arc<DataFusion>) -> Self {
        self.datafusion = Some(datafusion);
        self
    }

    pub fn with_runtime_status(mut self, runtime_status: Arc<status::RuntimeStatus>) -> Self {
        self.runtime_status = Some(runtime_status);
        self
    }

    pub async fn build(self) -> Runtime {
        dataconnector::register_all().await;
        dataaccelerator::register_all().await;
        tools::factory::register_all().await;
        document_parse::register_all().await;

        let status = match self.runtime_status {
            Some(status) => status,
            None => status::RuntimeStatus::new(),
        };

        let df = if let Some(df) = self.datafusion {
            df
        } else {
            let builder = DataFusion::builder(Arc::clone(&status)).keep_partition_by_columns(
                get_bool_param(&self.app, "sql_query_keep_partition_by_columns", true),
            );

            Arc::new(builder.build())
        };

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

        let secrets = Self::load_secrets(&self.app).await;

        let mut rt = Runtime {
            app: Arc::new(RwLock::new(self.app)),
            df,
            models: Arc::new(RwLock::new(HashMap::new())),
            llms: Arc::new(RwLock::new(HashMap::new())),
            embeds: Arc::new(RwLock::new(HashMap::new())),
            tools: Arc::new(RwLock::new(HashMap::new())),
            pods_watcher: Arc::new(RwLock::new(self.pods_watcher)),
            secrets: Arc::new(RwLock::new(secrets)),
            spaced_tracer: Arc::new(tracers::SpacedTracer::new(Duration::from_secs(15))),
            autoload_extensions: Arc::new(self.autoload_extensions),
            extensions: Arc::new(RwLock::new(HashMap::new())),
            datasets_health_monitor,
            metrics_endpoint: self.metrics_endpoint,
            prometheus_registry: self.prometheus_registry,
            status,
        };

        let mut extensions: HashMap<String, Arc<dyn Extension>> = HashMap::new();
        for factory in self.extensions {
            let mut extension = factory.create();
            let extension_name = extension.name();
            if let Err(err) = extension.initialize(&rt).await {
                eprintln!("Failed to initialize extension {extension_name}: {err}");
            } else {
                extensions.insert(extension_name.into(), extension.into());
            };
        }
        rt.extensions = Arc::new(RwLock::new(extensions));

        rt
    }

    async fn load_secrets(app: &Option<Arc<App>>) -> Secrets {
        let _guard = TimeMeasurement::new(&metrics::secrets::STORES_LOAD_DURATION_MS, &[]);
        let mut secrets = secrets::Secrets::new();

        if let Some(app) = app {
            if let Err(e) = secrets.load_from(&app.secrets).await {
                eprintln!("Error loading secret stores: {e}");
            };
        }

        secrets
    }
}

impl Default for RuntimeBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Get a boolean parameter from the app's runtime params, with a default value if the parameter is not set or is not a valid boolean.
///
/// Returns `default_value` if the parameter is not set or is not a valid boolean.
///
/// If the parameter is set but is not a valid boolean, logs a warning and returns `default_value`.
fn get_bool_param(app: &Option<Arc<App>>, param: &str, default_value: bool) -> bool {
    let Some(value) = app.as_ref().and_then(|app| app.runtime.params.get(param)) else {
        return default_value;
    };

    if let Ok(b) = value.parse::<bool>() {
        b
    } else {
        eprintln!("runtime.params.{param} is not a valid boolean, defaulting to {default_value}");
        default_value
    }
}

#[cfg(test)]
mod tests {
    use app::AppBuilder;

    use super::*;
    use std::collections::HashMap;

    fn create_app_with_params(params: HashMap<String, String>) -> Arc<App> {
        Arc::new(AppBuilder::new("test").with_runtime_params(params).build())
    }

    #[test]
    fn test_get_bool_param() {
        // Test case 1: Parameter is not set
        let app = Some(create_app_with_params(HashMap::new()));
        assert!(get_bool_param(&app, "test_param", true));
        assert!(!get_bool_param(&app, "test_param", false));

        // Test case 2: Parameter is set to "true"
        let mut params = HashMap::new();
        params.insert("test_param".to_string(), "true".to_string());
        let app = Some(create_app_with_params(params));
        assert!(get_bool_param(&app, "test_param", false));

        // Test case 3: Parameter is set to "false"
        let mut params = HashMap::new();
        params.insert("test_param".to_string(), "false".to_string());
        let app = Some(create_app_with_params(params));
        assert!(!get_bool_param(&app, "test_param", true));

        // Test case 4: Parameter is set to an invalid boolean value
        let mut params = HashMap::new();
        params.insert("test_param".to_string(), "not_a_bool".to_string());
        let app = Some(create_app_with_params(params));
        assert!(get_bool_param(&app, "test_param", true));
        assert!(!get_bool_param(&app, "test_param", false));

        // Test case 5: App is None
        assert!(get_bool_param(&None, "test_param", true));
        assert!(!get_bool_param(&None, "test_param", false));
    }
}
