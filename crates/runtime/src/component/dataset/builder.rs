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

use std::{collections::HashMap, sync::Arc};

use super::{
    CheckAvailability, Dataset, Error, Mode, ReadyState, Result, TimeFormat, UnsupportedTypeAction,
    acceleration, replication, validate_identifier,
};
use crate::Runtime;
use app::App;
use datafusion::sql::TableReference;
use serde_json::Value;
use snafu::prelude::*;
use spicepod::{
    component::{
        dataset::{self as spicepod_dataset},
        embeddings::ColumnEmbeddingConfig,
    },
    metric::Metrics,
    param::Params,
    semantic::Column,
    vector::VectorStore,
};

pub struct DatasetBuilder {
    pub from: String,
    pub name: TableReference,
    pub mode: Mode,
    pub params: HashMap<String, String>,
    pub metadata: HashMap<String, String>,
    pub columns: Vec<Column>,
    pub has_metadata_table: bool,
    pub replication: Option<replication::Replication>,
    pub time_column: Option<String>,
    pub time_format: Option<TimeFormat>,
    pub time_partition_column: Option<String>,
    pub time_partition_format: Option<TimeFormat>,
    pub acceleration: Option<acceleration::Acceleration>,
    pub embeddings: Vec<ColumnEmbeddingConfig>,
    pub app: Option<Arc<App>>,
    pub unsupported_type_action: Option<UnsupportedTypeAction>,
    pub ready_state: ReadyState,
    pub metrics: Metrics,
    pub runtime: Option<Arc<Runtime>>,
    pub vectors: Option<VectorStore>,
    pub check_availability: CheckAvailability,
}

impl TryFrom<spicepod_dataset::Dataset> for DatasetBuilder {
    type Error = crate::Error;

    fn try_from(dataset: spicepod_dataset::Dataset) -> std::result::Result<Self, Self::Error> {
        #[allow(deprecated)]
        let ready_state = match dataset.acceleration.as_ref().map(|a| a.ready_state) {
            Some(Some(ready_state)) => {
                tracing::warn!(
                    "{}: `dataset.acceleration.ready_state` is deprecated, use `dataset.ready_state` instead.",
                    dataset.name
                );
                ReadyState::from(ready_state)
            }
            _ => ReadyState::from(dataset.ready_state),
        };

        let acceleration = dataset
            .acceleration
            .map(acceleration::Acceleration::try_from)
            .transpose()?;

        validate_identifier(&dataset.name).context(crate::ComponentSnafu)?;

        let table_reference = Dataset::parse_table_reference(&dataset.name)?;

        // If the dataset is enabled for a vector engine, use this instead of JIT.
        if let Some(vector_engine) = &dataset.vectors {
            // We have a vector engine configured with no explicit acceleration - no indexing will happen.
            if vector_engine.enabled && acceleration.is_none() {
                tracing::debug!(
                    "Dataset {} configured for vector engine and no acceleration is defined - indexing will not occur.",
                    dataset.name
                );
            }

            // Chunking with vector engines is not supported (yet).
            for column in &dataset.columns {
                for embedding in &column.embeddings {
                    if embedding.chunking.is_some() {
                        return Err(crate::Error::InvalidSpicepodDataset {
                            source: Error::ChunkingNotSupportedForVectorEngine {
                                column: column.name.clone(),
                            },
                        });
                    }
                }
            }
        }

        Ok(DatasetBuilder {
            from: dataset.from,
            name: table_reference,
            mode: Mode::from(dataset.mode),
            params: dataset
                .params
                .as_ref()
                .map(Params::as_string_map)
                .unwrap_or_default(),
            metadata: dataset
                .metadata
                .iter()
                .map(|(k, v)| (k.clone(), value_to_string(v)))
                .collect(),
            columns: dataset.columns,
            has_metadata_table: dataset
                .has_metadata_table
                .unwrap_or(DatasetBuilder::have_metadata_table_by_default()),
            replication: dataset.replication.map(replication::Replication::from),
            time_column: dataset.time_column,
            time_format: dataset.time_format.map(TimeFormat::from),
            time_partition_column: dataset.time_partition_column,
            time_partition_format: dataset.time_partition_format.map(TimeFormat::from),
            embeddings: dataset.embeddings,
            acceleration,
            app: None,
            unsupported_type_action: dataset
                .unsupported_type_action
                .map(UnsupportedTypeAction::from),
            ready_state,
            metrics: dataset.metrics.unwrap_or_default(),
            runtime: None,
            vectors: dataset.vectors,
            check_availability: CheckAvailability::from(dataset.check_availability),
        })
    }
}

impl DatasetBuilder {
    #[allow(clippy::result_large_err)]
    pub fn try_new(from: String, name: &str) -> std::result::Result<Self, crate::Error> {
        Ok(DatasetBuilder {
            from,
            name: Self::parse_table_reference(name)?,
            mode: Mode::default(),
            params: HashMap::default(),
            metadata: HashMap::default(),
            columns: Vec::default(),
            has_metadata_table: Self::have_metadata_table_by_default(),
            replication: None,
            time_column: None,
            time_format: None,
            time_partition_column: None,
            time_partition_format: None,
            acceleration: None,
            embeddings: Vec::default(),
            app: None,
            unsupported_type_action: None,
            ready_state: ReadyState::default(),
            metrics: Metrics::default(),
            runtime: None,
            vectors: None,
            check_availability: CheckAvailability::default(),
        })
    }

    #[allow(clippy::result_large_err)]
    pub(crate) fn parse_table_reference(
        name: &str,
    ) -> std::result::Result<TableReference, crate::Error> {
        match TableReference::parse_str(name) {
            table_ref @ (TableReference::Bare { .. } | TableReference::Partial { .. }) => {
                Ok(table_ref)
            }
            TableReference::Full { catalog, .. } => crate::DatasetNameIncludesCatalogSnafu {
                catalog,
                name: name.to_string(),
            }
            .fail(),
        }
    }

    #[must_use]
    /// Returns whether the dataset should enable metadata by default.
    fn have_metadata_table_by_default() -> bool {
        false
    }

    #[must_use]
    pub fn with_time_column(mut self, time_column: String) -> Self {
        self.time_column = Some(time_column);
        self
    }

    #[must_use]
    pub fn with_time_partition_column(mut self, time_partition_column: String) -> Self {
        self.time_partition_column = Some(time_partition_column);
        self
    }

    #[must_use]
    pub fn with_metadata(mut self, metadata: HashMap<String, String>) -> Self {
        self.metadata = metadata;
        self
    }

    #[must_use]
    pub fn with_app(mut self, app: Arc<App>) -> Self {
        self.app = Some(app);
        self
    }

    #[must_use]
    pub fn with_runtime(mut self, runtime: Arc<Runtime>) -> Self {
        self.runtime = Some(runtime);
        self
    }

    pub fn build(self) -> Result<Dataset> {
        let app = self.app.ok_or(Error::UnableToBuildDataset {
            dataset: self.name.to_string(),
            missing_component: "app".to_string(),
        })?;
        let runtime = self.runtime.ok_or(Error::UnableToBuildDataset {
            dataset: self.name.to_string(),
            missing_component: "runtime".to_string(),
        })?;

        let dataset = Dataset {
            from: self.from,
            name: self.name,
            mode: self.mode,
            params: self.params,
            metadata: self.metadata,
            columns: self.columns,
            has_metadata_table: self.has_metadata_table,
            replication: self.replication,
            time_column: self.time_column,
            time_format: self.time_format,
            time_partition_column: self.time_partition_column,
            time_partition_format: self.time_partition_format,
            acceleration: self.acceleration,
            embeddings: self.embeddings,
            app,
            schema: None,
            unsupported_type_action: self.unsupported_type_action,
            ready_state: self.ready_state,
            metrics: self.metrics,
            runtime,
            vectors: self.vectors,
            check_availability: self.check_availability,
        };

        Ok(dataset)
    }
}

fn value_to_string(value: &Value) -> String {
    match value {
        Value::String(s) => s.clone(),
        _ => value.to_string(),
    }
}
