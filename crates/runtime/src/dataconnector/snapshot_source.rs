/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

//! The source of a dataset that reads acceleration snapshots (`file_format: snapshot`).
//!
//! Such a dataset is served only from its acceleration, which restores and polls the
//! snapshots; nothing is ever read from a source. What the acceleration needs from
//! this connector is the schema to register, and that is the schema of the snapshot
//! the acceleration holds.

use std::{any::Any, sync::Arc};

use arrow::datatypes::SchemaRef;
use arrow_tools::map_entries::conforming_schema;
use async_trait::async_trait;
use datafusion::{
    catalog::Session,
    common::TableReference,
    datasource::{TableProvider, TableType},
    error::{DataFusionError, Result as DataFusionResult},
    logical_expr::Expr,
    physical_plan::ExecutionPlan,
};
use runtime_acceleration::sidecar::OpenOption;
use runtime_acceleration::snapshot::{SnapshotBehavior, SnapshotManager, snapshots_enabled};
use runtime_datafusion::refresh_sql::parse_refresh_sql;

use super::{ConnectorComponent, ConnectorContext, DataConnector, DataConnectorError};
use crate::component::dataset::{
    Dataset, DatasetSpec, acceleration::RefreshMode, snapshot_source::SNAPSHOT_SOURCE_DOCS,
};
use crate::dataaccelerator::spice_sys::dataset_checkpointer;
use runtime_acceleration::dataset_checkpoint::DatasetCheckpointer;

/// The connector the dataset's `from` names; snapshots are read from S3.
const CONNECTOR_NAME: &str = "s3";

/// The source of a dataset that reads acceleration snapshots. See the module docs.
#[derive(Debug)]
pub(crate) struct SnapshotSourceConnector {
    dataset: Arc<Dataset>,
}

impl SnapshotSourceConnector {
    pub(crate) fn new(dataset: Arc<Dataset>) -> Self {
        Self { dataset }
    }

    /// The schema of the snapshot the acceleration holds: the restored snapshot's own,
    /// when one has been restored, and otherwise the schema the metadata records for the
    /// current snapshot, which the first refresh restores.
    ///
    /// The restored snapshot comes first because it describes the data actually on disk.
    /// A writer can publish a snapshot with a wider schema between the restore and this
    /// call, and registering that schema over the narrower file would leave the table
    /// unable to load; registered with the file's schema, the dataset serves it and
    /// reports the newer snapshot's schema change when its refresh checks it.
    async fn snapshot_schema(&self) -> Result<SchemaRef, Box<dyn std::error::Error + Send + Sync>> {
        if let Some(checkpoint) = restored_checkpoint(&self.dataset).await
            && let Some(schema) = checkpoint.get_schema().await.ok().flatten()
        {
            return Ok(conforming_schema(schema));
        }

        let acceleration = self.dataset.acceleration.as_ref().ok_or_else(|| {
            format!(
                "the engine of the snapshots of dataset '{}' is not known yet",
                self.dataset.name
            )
        })?;
        let manager = SnapshotManager::try_new_for_metadata_queries(
            self.dataset.name.to_string(),
            acceleration.snapshot_behavior.clone(),
        )
        .await
        .ok_or_else(|| {
            format!(
                "the snapshot location '{}' of dataset '{}' could not be opened",
                self.dataset.from, self.dataset.name
            )
        })?;
        Ok(manager.current_snapshot().await?.schema)
    }
}

/// The checkpoint restored with `dataset`'s snapshot, or `None` when nothing has been
/// restored.
async fn restored_checkpoint(dataset: &Dataset) -> Option<Arc<dyn DatasetCheckpointer>> {
    if !dataset.is_file_accelerated() {
        return None;
    }
    dataset_checkpointer(
        dataset,
        dataset.runtime.accelerator_engine_registry(),
        OpenOption::OpenExisting,
        SnapshotBehavior::Disabled,
    )
    .await
    .ok()
}

/// The `refresh_sql` the publisher of `dataset`'s restored snapshot was created with, when
/// that SQL stores only some of the source's columns — which a snapshot dataset cannot
/// serve. The publisher checkpoints, and records in the snapshot metadata, every column of
/// its source, including those its `refresh_sql` leaves out, so no schema the dataset
/// could register describes both the stored table and the newer snapshots it must accept.
///
/// Only the `DuckDB`, `SQLite` and Turso snapshots carry the publisher's checkpoint, and
/// with it the SQL; a Cayenne snapshot's is rebuilt from the metadata, which does not.
pub(crate) async fn publisher_column_projection(dataset: &Dataset) -> Option<String> {
    let checkpoint = restored_checkpoint(dataset).await?;
    let schema = conforming_schema(checkpoint.get_schema().await.ok().flatten()?);
    let refresh_sql = checkpoint.get_refresh_sql().await.ok().flatten()?;
    let stores_every_column =
        parse_refresh_sql(dataset.name.clone(), &refresh_sql, Arc::clone(&schema)).is_ok_and(
            |(_, stored)| {
                schema
                    .fields()
                    .iter()
                    .all(|field| stored.field_with_name(field.name()).is_ok())
            },
        );
    (!stores_every_column).then_some(refresh_sql)
}

/// The error for snapshots whose publisher's `refresh_sql` leaves columns out.
pub(crate) fn projected_publisher_message(dataset: &TableReference, refresh_sql: &str) -> String {
    format!(
        "Dataset '{dataset}' reads snapshots of a dataset whose `refresh_sql` stores only some of its source's columns ({refresh_sql}), which a snapshot dataset cannot serve yet. Select every column in the publishing dataset's `refresh_sql`; filtering rows is supported. See: {SNAPSHOT_SOURCE_DOCS}"
    )
}

#[async_trait]
impl DataConnector for SnapshotSourceConnector {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn resolve_refresh_mode(&self, _refresh_mode: Option<RefreshMode>) -> RefreshMode {
        // Newer snapshots are the only refresh a snapshot dataset has.
        RefreshMode::Snapshot
    }

    async fn read_provider(
        &self,
        _context: &dyn ConnectorContext,
        dataset: &DatasetSpec,
    ) -> super::DataConnectorResult<Arc<dyn TableProvider>> {
        if !snapshots_enabled() {
            return Err(DataConnectorError::InvalidConfigurationNoSource {
                dataconnector: CONNECTOR_NAME.to_string(),
                connector_component: ConnectorComponent::from(dataset),
                message: runtime_acceleration::snapshot::SNAPSHOTS_ENTERPRISE_ONLY_MESSAGE
                    .to_string(),
            });
        }

        let schema = self.snapshot_schema().await.map_err(|source| {
            DataConnectorError::UnableToGetReadProvider {
                dataconnector: CONNECTOR_NAME.to_string(),
                connector_component: ConnectorComponent::from(dataset),
                source,
            }
        })?;

        Ok(Arc::new(SnapshotSourceTable {
            dataset: dataset.name.clone(),
            location: dataset.from.clone(),
            schema,
        }))
    }
}

/// What a snapshot dataset registers as its source: the snapshot's schema, and no data.
/// A query reaches it only when the acceleration cannot answer, which for a snapshot
/// dataset means no snapshot has been loaded yet.
#[derive(Debug)]
struct SnapshotSourceTable {
    dataset: TableReference,
    location: String,
    schema: SchemaRef,
}

#[async_trait]
impl TableProvider for SnapshotSourceTable {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        Err(DataFusionError::Execution(not_loaded_message(
            &self.dataset,
            &self.location,
        )))
    }
}

/// The error a query gets from a snapshot dataset that has not loaded a snapshot yet.
fn not_loaded_message(dataset: &TableReference, location: &str) -> String {
    format!(
        "Dataset '{dataset}' is served from the snapshots at '{location}' and has not loaded one yet, so it has no data to return. Query it once it is ready. See: {SNAPSHOT_SOURCE_DOCS}"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::prelude::SessionContext;

    #[test]
    fn a_publisher_that_stores_some_columns_is_refused_by_name() {
        assert_eq!(
            projected_publisher_message(
                &TableReference::bare("modules"),
                "SELECT id, name FROM modules"
            ),
            format!(
                "Dataset 'modules' reads snapshots of a dataset whose `refresh_sql` stores only some of its source's columns (SELECT id, name FROM modules), which a snapshot dataset cannot serve yet. Select every column in the publishing dataset's `refresh_sql`; filtering rows is supported. See: {SNAPSHOT_SOURCE_DOCS}"
            )
        );
    }

    #[tokio::test]
    async fn a_query_the_acceleration_cannot_answer_is_refused_rather_than_answered_empty() {
        let table = SnapshotSourceTable {
            dataset: TableReference::bare("modules"),
            location: "s3://bucket-b/snapshots/".to_string(),
            schema: Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
        };
        let ctx = SessionContext::new();

        let err = table
            .scan(&ctx.state(), None, &[], None)
            .await
            .expect_err("the source holds no data");

        assert_eq!(
            err.to_string(),
            format!(
                "Execution error: Dataset 'modules' is served from the snapshots at 's3://bucket-b/snapshots/' and has not loaded one yet, so it has no data to return. Query it once it is ready. See: {SNAPSHOT_SOURCE_DOCS}"
            )
        );
    }
}
