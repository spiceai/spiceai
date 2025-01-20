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

use crate::accelerated_table::AcceleratedTable;
use crate::component::dataset::Dataset;
use crate::dataconnector::listing::LISTING_TABLE_PARAMETERS;
use crate::dataconnector::ConnectorComponent;
use async_trait::async_trait;

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
use tokio::sync::mpsc;
use url::Url;

use super::ConnectorParams;
use super::{
    listing::ListingTableConnector, DataConnector, DataConnectorFactory, DataConnectorResult,
    InvalidConfigurationSnafu, ParameterSpec, Parameters,
};

pub struct File {
    params: Parameters,
}

impl std::fmt::Display for File {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "file")
    }
}

#[derive(Default, Copy, Clone)]
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
    ) -> Pin<Box<dyn Future<Output = super::NewDataConnectorResult> + Send>> {
        Box::pin(async move {
            Ok(Arc::new(File {
                params: params.parameters,
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

    /// Creates a valid file [`url::Url`], from the dataset, supporting both
    ///   1. Relative paths
    ///   2. Datasets prefixed with `file://` (not just `file:/`). This is to mirror the UX of [`Url::parse`].
    fn get_object_store_url(&self, dataset: &Dataset) -> DataConnectorResult<Url> {
        let path = get_path(dataset).to_string_lossy().into_owned();

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
                .join(path)
                .to_string_lossy()
                .to_string();

            format!("file:{absolute_path}")
        };

        Url::parse(&url_str)
            .boxed()
            .context(InvalidConfigurationSnafu {
                dataconnector: "file".to_string(),
                message: "The specified file path created an invalid URL. Check your file path and try again.\nFor details, visit: https://spiceai.org/docs/components/data-connectors/file".to_string(),
                connector_component: ConnectorComponent::from(dataset),
            })
    }

    /// Set up a file watcher to refresh the accelerated table when the file is updated.
    ///
    /// Spawns an async top-level Tokio task to watch the file(s) and adds it to the join
    /// handles of the AcceleratedTable. When the AcceleratedTable is dropped, the file
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
            };

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

        accelerated_table.handlers.push(watcher_task);

        Ok(())
    }
}

fn get_path(dataset: &Dataset) -> PathBuf {
    PathBuf::from(dataset.path())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::component::dataset::Dataset;

    #[test]
    fn test_get_path() {
        let test_cases = vec![
            ("file:/path/to/file.csv", PathBuf::from("/path/to/file.csv")),
            ("file://path/to/file.csv", PathBuf::from("path/to/file.csv")),
            (
                "file:///path/to/file.csv",
                PathBuf::from("/path/to/file.csv"),
            ),
            (
                "file:relative/path/to/file.csv",
                PathBuf::from("relative/path/to/file.csv"),
            ),
        ];

        for (input, expected) in test_cases {
            let dataset = Dataset::try_new(input.to_string(), "foo").expect("valid dataset");

            let result = get_path(&dataset);
            assert_eq!(result, expected, "Failed for input: {input}");
        }
    }
}
