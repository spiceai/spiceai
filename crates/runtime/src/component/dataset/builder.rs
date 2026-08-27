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

use std::{collections::HashMap, sync::Arc, time::Duration};

use super::{
    CheckAvailability, Dataset, Error, InvalidColumnTypeSnafu, InvalidConfigurationSnafu,
    OnSchemaChange, ReadyState, Result, TimeFormat, UnsupportedTypeAction, acceleration,
    declared_schema, replication, validate_identifier,
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
    pub drasi: Option<spicepod::drasi::Drasi>,
    pub check_availability: CheckAvailability,
    pub check_availability_interval: Option<Duration>,
}

/// What to tell an operator whose dataset sets `acceleration.enabled: false` and
/// leaves settings in the block that the runtime will not apply.
///
/// A function rather than an inline `tracing::warn!` so the wording — which is
/// the whole of this feature for the person reading the log — is assertable.
///
/// Single quotes around the name the operator chose, backticks around the config
/// keys they are being told to act on, per the repo's message convention — and
/// the name escaped, since a quoted Spicepod identifier can carry a newline
/// through validation and would otherwise forge a second log line.
///
/// It names only the fields it was given, and does not say "the rest of the
/// block": `ready_state` is read out of a disabled block and applied, so a claim
/// about everything under `enabled` would be untrue for it.
fn disabled_acceleration_warning(dataset: &str, ignored: &[String]) -> String {
    let keys = ignored
        .iter()
        .map(|field| format!("`{field}`"))
        .collect::<Vec<_>>()
        .join(", ");
    // `escape_debug` rather than the raw name: `validate_identifier` accepts a
    // *quoted* identifier, and a quoted one may legally contain a newline, so a
    // validated name can still break this line in two and forge a second one.
    let dataset = dataset.escape_debug();
    format!(
        "Dataset '{dataset}' sets `acceleration.enabled: false`, so these settings in its acceleration block are read and then ignored: {keys}. Remove `enabled: false` to apply them, or remove them to keep the dataset unaccelerated. See: https://spiceai.org/docs/reference/spicepod/datasets#acceleration"
    )
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

        // `enabled: false` turns the whole block off, so anything else set in it
        // is read, accepted and then never applied — with one exception, which
        // is above: `ready_state` is pulled out of this block and applied to the
        // dataset whether or not acceleration is enabled, and
        // `fields_ignored_when_disabled` leaves it out for exactly that reason.
        //
        // Collect the rest here, while the Spicepod block is still in hand and
        // before the conversion below resolves its defaults away (#13514); it is
        // reported after the name is validated.
        let ignored_acceleration_fields = dataset
            .acceleration
            .as_ref()
            .map(spicepod_acceleration::Acceleration::fields_ignored_when_disabled)
            .unwrap_or_default();

        let acceleration = dataset
            .acceleration
            .map(acceleration::Acceleration::try_from)
            .transpose()?;

        validate_identifier(&dataset.name).context(crate::ComponentSnafu)?;

        // After the name is validated, deliberately: an identifier that reaches
        // here has passed the tokenizer, so it holds no newline or other control
        // character that could forge a second log line out of this one.
        if !ignored_acceleration_fields.is_empty() {
            tracing::warn!(
                "{}",
                disabled_acceleration_warning(&dataset.name, &ignored_acceleration_fields)
            );
        }

        let table_reference = Dataset::parse_table_reference(&dataset.name)?;

        // Parse the duration string once here (raw string stays in the Spicepod
        // representation; the runtime component holds the typed value). An
        // invalid value fails dataset construction rather than silently
        // disabling the check.
        let check_availability_interval = dataset
            .check_availability_interval
            .as_deref()
            .map(|raw| {
                fundu::parse_duration(raw).map_err(|source| {
                    crate::component::dataset::Error::UnableToParseFieldAsDuration {
                        field: "check_availability_interval".to_string(),
                        source,
                    }
                })
            })
            .transpose()
            .context(crate::InvalidSpicepodDatasetSnafu)?;

        // Availability monitoring only applies to non-accelerated datasets, so
        // warn (rather than silently ignore) when it is configured on an
        // accelerated one.
        if check_availability_interval.is_some() && acceleration.as_ref().is_some_and(|a| a.enabled)
        {
            tracing::warn!(
                "Dataset {} sets `check_availability_interval` but is accelerated; availability monitoring applies only to non-accelerated datasets and will be ignored. An accelerated dataset keeps serving from the accelerator even when its source is unavailable.",
                dataset.name
            );
        }

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
            drasi: dataset.drasi,
            check_availability: CheckAvailability::from(dataset.check_availability),
            check_availability_interval,
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
            drasi: None,
            check_availability: CheckAvailability::default(),
            check_availability_interval: None,
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

        let schema = declared_schema::schema_from_columns(&self.name.to_string(), &self.columns)
            .context(InvalidColumnTypeSnafu {
                dataset: self.name.to_string(),
            })?;

        let dataset = Dataset {
            spec: super::DatasetSpec {
                from: self.from,
                name: self.name,
                access: self.access,
                params: self.params,
                metadata: self.metadata,
                columns: self.columns,
                schema,
                has_metadata_table: self.has_metadata_table,
                replication: self.replication,
                time_column: self.time_column,
                time_format: self.time_format,
                time_partition_column: self.time_partition_column,
                time_partition_format: self.time_partition_format,
                acceleration: self.acceleration,
                embeddings: self.embeddings,
                unsupported_type_action: self.unsupported_type_action,
                on_schema_change: self.on_schema_change,
                ready_state: self.ready_state,
                metrics: self.metrics,
                vectors: self.vectors,
                full_text_search: self.full_text_search,
                drasi: self.drasi,
                check_availability: self.check_availability,
                check_availability_interval: self.check_availability_interval,
            },
            app,
            runtime,
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

#[cfg(test)]
mod tests {
    use super::disabled_acceleration_warning;

    #[test]
    fn the_warning_names_the_dataset_the_fields_and_the_remedy() {
        // Everything a reader needs to act, in one line: which dataset, what is
        // being dropped, and the two ways out. Asserted because the message is
        // the entire user-visible behaviour of this path (#13514).
        let warning = disabled_acceleration_warning(
            "api_data",
            &["engine".to_string(), "refresh_mode".to_string()],
        );
        // Quoting and backticking are the repo's convention, not decoration: an
        // unquoted name vanishes when it is empty and reads as prose when it is
        // a word like `orders`.
        assert!(warning.contains("'api_data'"), "{warning}");
        assert!(warning.contains("`engine`, `refresh_mode`"), "{warning}");
        assert!(warning.contains("acceleration.enabled: false"), "{warning}");
        assert!(warning.contains("Remove `enabled: false`"), "{warning}");
        assert!(warning.contains("https://spiceai.org/docs/"), "{warning}");
    }

    #[test]
    fn a_control_character_in_the_name_cannot_break_the_line_in_two() {
        // `validate_identifier` accepts a quoted identifier, and a quoted one
        // may contain a newline — so the name reaching this message is not
        // guaranteed to be one line, and an unescaped one would let a dataset
        // name write a second log line of its own choosing.
        let warning = disabled_acceleration_warning("api\nWARN forged", &["engine".to_string()]);
        assert!(
            !warning.contains('\n'),
            "the message must stay one line: {warning}"
        );
        assert!(
            warning.contains("api\\nWARN forged"),
            "the name must still be readable, escaped: {warning}"
        );
    }

    #[test]
    fn the_warning_claims_only_the_fields_it_was_given() {
        // `ready_state` is read out of a disabled block and applied, so this
        // message must not claim the whole block is ignored — a reader who sees
        // that goes looking for a `ready_state` that is working correctly.
        let warning = disabled_acceleration_warning("api_data", &["engine".to_string()]);
        assert!(
            !warning.contains("the rest of"),
            "the message must scope itself to the listed fields: {warning}"
        );
        assert!(!warning.contains("ready_state"), "{warning}");
    }
}
