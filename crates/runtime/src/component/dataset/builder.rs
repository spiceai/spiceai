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
    CheckAvailability, Dataset, Error, InvalidConfigurationSnafu, OnSchemaChange, ReadyState,
    Result, SchemaInference, TimeFormat, UnsupportedTypeAction, acceleration, replication,
    validate_identifier,
};
use crate::Runtime;
use crate::component::access::AccessMode;
use app::App;
use datafusion::sql::TableReference;
use runtime_acceleration::snapshot::SnapshotBehavior;
use snafu::prelude::*;
use spicepod::{
    acceleration as spicepod_acceleration,
    component::{
        dataset::{self as spicepod_dataset},
        embeddings::ColumnEmbeddingConfig,
    },
    fts::FtsStore,
    metric::Metrics,
    param::{Params, merge_params},
    semantic::Column,
    vector::VectorStore,
};

pub struct DatasetBuilder {
    pub from: String,
    pub name: TableReference,
    pub access: AccessMode,
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
    pub acceleration_snapshot_behavior: spicepod_acceleration::SnapshotBehavior,
    pub acceleration_snapshot_compaction: spicepod_acceleration::SnapshotsCompaction,
    pub embeddings: Vec<ColumnEmbeddingConfig>,
    pub app: Option<Arc<App>>,
    pub unsupported_type_action: Option<UnsupportedTypeAction>,
    pub on_schema_change: OnSchemaChange,
    pub ready_state: ReadyState,
    pub metrics: Metrics,
    pub runtime: Option<Arc<Runtime>>,
    pub vectors: Option<VectorStore>,
    pub full_text_search: Option<FtsStore>,
    pub check_availability: CheckAvailability,
    pub schema_inference: SchemaInference,
}

impl TryFrom<spicepod_dataset::Dataset> for DatasetBuilder {
    type Error = crate::Error;

    fn try_from(dataset: spicepod_dataset::Dataset) -> std::result::Result<Self, Self::Error> {
        #[expect(deprecated)]
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

        let acceleration_snapshot_behavior = dataset
            .acceleration
            .as_ref()
            .map_or(spicepod_acceleration::SnapshotBehavior::Disabled, |a| {
                a.snapshots
            });

        let acceleration_snapshot_compaction = dataset
            .acceleration
            .as_ref()
            .map_or(spicepod_acceleration::SnapshotsCompaction::Disabled, |a| {
                a.snapshots_compaction
            });

        let metadata = dataset.metadata();

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
                tracing::warn!(
                    "Dataset {} configured with 'vector_engine: enabled' but acceleration is disabled. Vector indexing will not occur. Enable acceleration with `acceleration.enabled: true` to use vector search.",
                    dataset.name
                );
            }
        }

        Ok(DatasetBuilder {
            from: dataset.from,
            name: table_reference,
            access: AccessMode::from(dataset.access),
            params: dataset
                .params
                .as_ref()
                .map(Params::as_string_map)
                .unwrap_or_default(),
            metadata,
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
            acceleration_snapshot_behavior,
            acceleration_snapshot_compaction,
            app: None,
            unsupported_type_action: dataset
                .unsupported_type_action
                .map(UnsupportedTypeAction::from),
            on_schema_change: OnSchemaChange::from(dataset.on_schema_change),
            ready_state,
            metrics: dataset.metrics.unwrap_or_default(),
            runtime: None,
            vectors: dataset.vectors,
            full_text_search: dataset.full_text_search,
            check_availability: CheckAvailability::from(dataset.check_availability),
            schema_inference: SchemaInference::from(dataset.schema_inference),
        })
    }
}

impl DatasetBuilder {
    #[expect(clippy::result_large_err)]
    pub fn try_new(from: String, name: &str) -> std::result::Result<Self, crate::Error> {
        Ok(DatasetBuilder {
            from,
            name: Self::parse_table_reference(name)?,
            access: AccessMode::default(),
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
            acceleration_snapshot_behavior: spicepod_acceleration::SnapshotBehavior::Disabled,
            acceleration_snapshot_compaction: spicepod_acceleration::SnapshotsCompaction::Disabled,
            embeddings: Vec::default(),
            app: None,
            unsupported_type_action: None,
            on_schema_change: OnSchemaChange::default(),
            ready_state: ReadyState::default(),
            metrics: Metrics::default(),
            runtime: None,
            vectors: None,
            full_text_search: None,
            check_availability: CheckAvailability::default(),
            schema_inference: SchemaInference::default(),
        })
    }

    #[expect(clippy::result_large_err)]
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

    pub fn build(mut self) -> Result<Dataset> {
        let app = self.app.ok_or(Error::UnableToBuildDataset {
            dataset: self.name.to_string(),
            missing_component: "app".to_string(),
        })?;
        let runtime = self.runtime.ok_or(Error::UnableToBuildDataset {
            dataset: self.name.to_string(),
            missing_component: "runtime".to_string(),
        })?;

        if let Some(acceleration) = self.acceleration.as_mut() {
            acceleration.snapshot_behavior = SnapshotBehavior::from(
                app.snapshots.clone(),
                self.acceleration_snapshot_behavior,
                runtime.secrets_weak(),
                runtime.tokio_io_runtime(),
                self.acceleration_snapshot_compaction,
            );
        }

        self.vectors = enable_vector_store_from_column_overrides(self.vectors, &self.columns);
        self.full_text_search =
            fts_store_from_column_overrides(self.full_text_search, &self.columns, &self.name)?;

        let dataset = Dataset {
            from: self.from,
            name: self.name,
            access: self.access,
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
            unsupported_type_action: self.unsupported_type_action,
            on_schema_change: self.on_schema_change,
            ready_state: self.ready_state,
            metrics: self.metrics,
            runtime,
            vectors: self.vectors,
            full_text_search: self.full_text_search,
            check_availability: self.check_availability,
            schema_inference: self.schema_inference,
        };

        Ok(dataset)
    }
}

fn enable_vector_store_from_column_overrides(
    vector_store: Option<VectorStore>,
    columns: &[Column],
) -> Option<VectorStore> {
    if vector_store.is_some() {
        return vector_store;
    }

    let has_column_vector_engine = columns
        .iter()
        .flat_map(|column| &column.embeddings)
        .any(|embedding| embedding.engine.is_some());

    has_column_vector_engine.then(|| VectorStore {
        enabled: true,
        engine: None,
        partition_by: Vec::new(),
        params: None,
    })
}

fn fts_store_from_column_overrides(
    mut fts_store: Option<FtsStore>,
    columns: &[Column],
    dataset_name: &TableReference,
) -> Result<Option<FtsStore>> {
    let mut column_engine: Option<(String, Option<Params>)> = None;

    for column in columns {
        let Some(fts) = column.full_text_search.as_ref().filter(|fts| fts.enabled) else {
            continue;
        };
        let Some(engine) = fts.engine.as_ref() else {
            continue;
        };

        if let Some((first_engine, first_params)) = &column_engine {
            ensure!(
                first_engine == engine,
                InvalidConfigurationSnafu {
                    config_key: "columns[].full_text_search.engine".to_string(),
                    message: format!(
                        "Dataset '{dataset_name}' has full-text columns that reference different text search engines ('{first_engine}' and '{engine}'). Configure a single dataset-level `full_text_search.engine` or use matching column engines."
                    )
                }
            );
            ensure!(
                first_params == &fts.params,
                InvalidConfigurationSnafu {
                    config_key: "columns[].full_text_search.params".to_string(),
                    message: format!(
                        "Dataset '{dataset_name}' has full-text columns that use the same text search engine '{engine}' with different parameters. Configure a single dataset-level `full_text_search.params` or use matching column parameters."
                    )
                }
            );
        } else {
            column_engine = Some((engine.clone(), fts.params.clone()));
        }
    }

    let Some((column_engine, column_params)) = column_engine else {
        return Ok(fts_store);
    };

    if let Some(store) = fts_store.as_mut() {
        if let Some(dataset_engine) = store.engine.as_ref() {
            ensure!(
                dataset_engine == &column_engine,
                InvalidConfigurationSnafu {
                    config_key: "columns[].full_text_search.engine".to_string(),
                    message: format!(
                        "Dataset '{dataset_name}' has full-text column engine '{column_engine}' that does not match dataset full_text_search.engine '{dataset_engine}'."
                    )
                }
            );
        }

        let mut params = store.params.clone().unwrap_or_default();
        merge_params(&mut params, column_params.as_ref());
        store.params = if params.data.is_empty() {
            None
        } else {
            Some(params)
        };
        return Ok(fts_store);
    }

    Ok(Some(FtsStore {
        enabled: true,
        engine: Some(column_engine),
        params: column_params,
    }))
}
