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

use async_trait::async_trait;
use runtime::accelerated_table::AcceleratedTable;
use runtime::component::dataset::Dataset;
use runtime::dataconnector::ConnectorComponent;
use runtime::dataconnector::listing::LISTING_TABLE_PARAMETERS;

use notify::{RecommendedWatcher, RecursiveMode, Watcher};
use snafu::prelude::*;
use std::future::Future;
use std::path::Path;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;
use std::{any::Any, env};
use tokio::runtime::Handle;
use tokio::sync::mpsc;
use url::Url;

use runtime::dataconnector::ConnectorParams;
use runtime::dataconnector::{
    DataConnector, DataConnectorFactory, DataConnectorResult, InvalidConfigurationSnafu,
    ParameterSpec, listing::ListingTableConnector,
};
use runtime::parameters::Parameters;

#[derive(Debug)]
pub struct File {
    params: Parameters,
    tokio_io_runtime: Handle,
}

impl std::fmt::Display for File {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "file")
    }
}

#[derive(Default, Debug, Copy, Clone)]
pub struct FileFactory {}

impl FileFactory {
    #[must_use]
    pub fn new() -> Self {
        Self {}
    }

    #[must_use]
    pub fn new_arc() -> Arc<dyn DataConnectorFactory> {
        Arc::new(Self {}) as Arc<dyn DataConnectorFactory>
    }
}

impl DataConnectorFactory for FileFactory {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn create(
        &self,
        params: ConnectorParams,
    ) -> Pin<Box<dyn Future<Output = runtime::dataconnector::NewDataConnectorResult> + Send>> {
        Box::pin(async move {
            Ok(Arc::new(File {
                params: params.parameters,
                tokio_io_runtime: params.io_runtime,
            }) as Arc<dyn DataConnector>)
        })
    }

    fn prefix(&self) -> &'static str {
        "file"
    }

    fn parameters(&self) -> &'static [ParameterSpec] {
        LISTING_TABLE_PARAMETERS
    }
}

#[async_trait]
impl ListingTableConnector for File {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_params(&self) -> &Parameters {
        &self.params
    }

    fn get_tokio_io_runtime(&self) -> Handle {
        self.tokio_io_runtime.clone()
    }

    /// Creates a valid file [`url::Url`], from the dataset, supporting both
    ///   1. Relative paths
    ///   2. Datasets prefixed with `file://` (not just `file:/`). This is to mirror the UX of [`Url::parse`].
    fn get_object_store_url(
        &self,
        dataset: &Dataset,
        path: Option<&str>,
    ) -> DataConnectorResult<Url> {
        let path = match path {
            Some(p) => PathBuf::from(p.trim_start_matches("file:"))
                .to_string_lossy()
                .into_owned(),
            None => get_path(dataset).to_string_lossy().into_owned(),
        };
        // Convert relative path to absolute path
        let url_str = if path.starts_with('/') {
            format!("file:{path}")
        } else {
            let absolute_path = env::current_dir()
                .boxed()
                .context(InvalidConfigurationSnafu {
                    dataconnector: "file".to_string(),
                    message: "Could not identify current directory for a relative file path. Does the running user have the right filesystem permissions?".to_string(),
                    connector_component: ConnectorComponent::from(dataset),
                })?
                .join(&path)
                .to_string_lossy()
                .to_string();

            format!("file:{absolute_path}")
        };

        Url::parse(&url_str)
            .boxed()
            .context(InvalidConfigurationSnafu {
                dataconnector: "file".to_string(),
                message: format!("The specified file path {path} created an invalid URL. Check your file path and try again. For details, visit: https://spiceai.org/docs/components/data-connectors/file"),
                connector_component: ConnectorComponent::from(dataset),
            })
    }

    /// Set up a file watcher to refresh the accelerated table when the file is updated.
    ///
    /// Spawns an async top-level Tokio task to watch the file(s) and adds it to the join
    /// handles of the `AcceleratedTable`. When the `AcceleratedTable` is dropped, the file
    /// watcher is aborted.
    async fn on_accelerated_table_registration(
        &self,
        dataset: &Dataset,
        accelerated_table: &mut AcceleratedTable,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Only enable the file watcher if the acceleration has the file_watcher parameter set to "enabled"
        let enabled = dataset.acceleration.as_ref().is_some_and(|acceleration| {
            acceleration
                .params
                .get("file_watcher")
                .is_some_and(|v| v == "enabled")
        });

        if !enabled {
            tracing::debug!("File watcher disabled for dataset {}", dataset.name);
            return Ok(());
        }

        let path = get_path(dataset);
        let (tx, mut rx) = mpsc::channel(100);
        let Some(refresh_trigger) = accelerated_table.refresh_trigger().cloned() else {
            return Ok(());
        };

        tracing::info!("Watching changes to {}", path.display());

        let watcher_task = tokio::spawn(async move {
            let mut watcher: RecommendedWatcher = match notify::recommended_watcher(
                move |res: Result<notify::Event, notify::Error>| match res {
                    Ok(event) if event.kind.is_modify() => {
                        let _ = tx.blocking_send(());
                    }
                    _ => {}
                },
            ) {
                Ok(watcher) => watcher,
                Err(e) => {
                    tracing::error!("Failed to create file watcher: {e}");
                    return;
                }
            };

            let watch_path = Path::new(&path);
            let mode = if watch_path.is_dir() {
                RecursiveMode::Recursive
            } else {
                RecursiveMode::NonRecursive
            };

            match watcher.watch(watch_path, mode) {
                Ok(()) => (),
                Err(e) => {
                    tracing::error!("Failed to watch file: {e}");
                    return;
                }
            }

            let mut last_refresh = Instant::now();
            loop {
                tokio::select! {
                    Some(()) = rx.recv() => {
                        if last_refresh.elapsed() < Duration::from_millis(100) {
                            tracing::debug!("Skipping refresh for file {}, last refresh was too recent", path.display());
                            continue;
                        }
                        tracing::debug!("Triggering refresh for file {}", path.display());
                        if let Err(e) = refresh_trigger.send(None).await {
                            tracing::error!("Failed to trigger refresh: {e}");
                        }
                        last_refresh = Instant::now();
                    }
                    else => break,
                }
            }
        });

        accelerated_table.add_background_handler(watcher_task);

        Ok(())
    }
}

pub const CONNECTOR_NAME: &str = "file";

#[must_use]
pub fn factory() -> Arc<dyn runtime::dataconnector::DataConnectorFactory> {
    FileFactory::new_arc()
}

fn get_path(dataset: &Dataset) -> PathBuf {
    PathBuf::from(dataset.path())
}
