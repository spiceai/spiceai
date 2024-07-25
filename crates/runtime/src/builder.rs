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

use metrics_exporter_prometheus::PrometheusHandle;
use tokio::sync::RwLock;
use uuid::Uuid;

use crate::{
    dataaccelerator, dataconnector,
    datafusion::DataFusion,
    datasets_health_monitor::DatasetsHealthMonitor,
    extension::{Extension, ExtensionFactory},
    podswatcher, secrets, tracers, Runtime,
};

pub struct RuntimeBuilder {
    app: Option<app::App>,
    autoload_extensions: HashMap<String, Box<dyn ExtensionFactory>>,
    extensions: Vec<Box<dyn ExtensionFactory>>,
    pods_watcher: Option<podswatcher::PodsWatcher>,
    datasets_health_monitor_enabled: bool,
    metrics_endpoint: Option<SocketAddr>,
    metrics_handle: Option<PrometheusHandle>,
    datafusion: Option<Arc<DataFusion>>,
}

impl RuntimeBuilder {
    pub fn new() -> Self {
        RuntimeBuilder {
            app: None,
            extensions: vec![],
            pods_watcher: None,
            datasets_health_monitor_enabled: false,
            metrics_endpoint: None,
            metrics_handle: None,
            datafusion: None,
            autoload_extensions: HashMap::new(),
        }
    }

    pub fn with_app(mut self, app: app::App) -> Self {
        self.app = Some(app);
        self
    }

    pub fn with_app_opt(mut self, app: Option<app::App>) -> Self {
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
        metrics_handle: PrometheusHandle,
    ) -> Self {
        self.metrics_endpoint = Some(metrics_endpoint);
        self.metrics_handle = Some(metrics_handle);
        self
    }

    pub fn with_metrics_server_opt(
        mut self,
        metrics_endpoint: Option<SocketAddr>,
        metrics_handle: Option<PrometheusHandle>,
    ) -> Self {
        self.metrics_endpoint = metrics_endpoint;
        self.metrics_handle = metrics_handle;
        self
    }

    pub fn with_datafusion(mut self, datafusion: Arc<DataFusion>) -> Self {
        self.datafusion = Some(datafusion);
        self
    }

    pub async fn build(self) -> Runtime {
        dataconnector::register_all().await;
        dataaccelerator::register_all().await;

        let hash = Uuid::new_v4().to_string()[..8].to_string();
        let name = match &self.app {
            Some(app) => app.name.clone(),
            None => "spice".to_string(),
        };

        let df = match self.datafusion {
            Some(df) => df,
            None => Arc::new(DataFusion::new()),
        };

        let datasets_health_monitor = if self.datasets_health_monitor_enabled {
            let datasets_health_monitor = DatasetsHealthMonitor::new(Arc::clone(&df.ctx));
            datasets_health_monitor.start();
            Some(Arc::new(datasets_health_monitor))
        } else {
            None
        };

        let mut rt = Runtime {
            instance_name: format!("{name}-{hash}").to_string(),
            app: Arc::new(RwLock::new(self.app)),
            df,
            models: Arc::new(RwLock::new(HashMap::new())),
            llms: Arc::new(RwLock::new(HashMap::new())),
            embeds: Arc::new(RwLock::new(HashMap::new())),
            pods_watcher: Arc::new(RwLock::new(self.pods_watcher)),
            secrets: Arc::new(RwLock::new(secrets::Secrets::new())),
            spaced_tracer: Arc::new(tracers::SpacedTracer::new(Duration::from_secs(15))),
            autoload_extensions: Arc::new(self.autoload_extensions),
            extensions: Arc::new(RwLock::new(HashMap::new())),
            datasets_health_monitor,
            metrics_endpoint: self.metrics_endpoint,
            metrics_handle: self.metrics_handle,
        };

        let mut extensions: HashMap<String, Arc<dyn Extension>> = HashMap::new();
        for factory in self.extensions {
            let mut extension = factory.create();
            let extension_name = extension.name();
            if let Err(err) = extension.initialize(&rt).await {
                tracing::warn!("Failed to initialize extension {extension_name}: {err}");
            } else {
                extensions.insert(extension_name.into(), extension.into());
            };
        }
        rt.extensions = Arc::new(RwLock::new(extensions));

        rt
    }
}

impl Default for RuntimeBuilder {
    fn default() -> Self {
        Self::new()
    }
}
