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

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion_datasource::sink::{DataSink, DataSinkExec};

use std::{any::Any, fmt, pin::Pin, sync::Arc};

use crate::component::dataset::{Dataset, acceleration::RefreshMode};
use crate::dataaccelerator::spice_sys::OpenOption;
use crate::dataaccelerator::spice_sys::dataset_checkpoint::DatasetCheckpoint;
use datafusion::{
    catalog::Session,
    common::{Constraint, Constraints, project_schema},
    datasource::{TableProvider, TableType},
    execution::{SendableRecordBatchStream, TaskContext},
    logical_expr::{Expr, dml::InsertOp},
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, empty::EmptyExec, metrics::MetricsSet,
    },
};
use futures::Future;

use super::{
    ConnectorComponent, ConnectorParams, DataConnector, DataConnectorFactory, ParameterSpec,
};

/// The schema a `sink` source advertises when it has no acceleration to inherit from.
///
/// A `sink` produces no data of its own; the single `placeholder` column exists only to give
/// it a non-empty, well-formed schema.
fn placeholder_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        "placeholder",
        DataType::Utf8,
        false,
    )]))
}

/// The schema an accelerated `sink` dataset should advertise as its (no-op) source.
///
/// A `sink` dataset stores everything in its acceleration, so on restart the acceleration
/// checkpoint — e.g. the schema grown by the OpenTelemetry metric-dimension ingest — is the
/// authoritative schema, not the bare `placeholder`. Advertising `placeholder` instead makes
/// the federated-table reconciliation report every accelerated column as missing (and
/// `placeholder` as unexpected), deferring the dataset with a schema-mismatch warning on
/// every restart even though no source schema actually changed.
///
/// Returns `None` when there is no existing checkpoint to inherit — a first run, or a
/// non-file accelerator — so the caller falls back to [`placeholder_schema`], preserving the
/// pre-acceleration behavior.
pub(crate) async fn accelerated_checkpoint_schema(dataset: &Dataset) -> Option<SchemaRef> {
    if !dataset.is_file_accelerated() {
        return None;
    }
    let registry = dataset.runtime.accelerator_engine_registry();
    let checkpoint = DatasetCheckpoint::try_new(dataset, registry, OpenOption::OpenExisting)
        .await
        .ok()?;
    checkpoint.get_schema().await.ok().flatten()
}

/// Connector name for the [`SinkConnector`], as it appears in a dataset's `from: sink:...`.
pub const SINK_DATACONNECTOR: &str = "sink";

/// A no-op connector that allows for Spice to act as a "sink" for data.
///
/// Configure an accelerator to store data - the sink connector itself does nothing.
#[derive(Debug, Clone)]
pub struct SinkConnector {
    schema: SchemaRef,
    table_constraints: Constraints,
}

impl SinkConnector {
    #[must_use]
    pub fn new(schema: SchemaRef) -> Self {
        Self {
            schema,
            table_constraints: Constraints::new_unverified(vec![]),
        }
    }

    #[must_use]
    pub fn with_primary_key(mut self, primary_key: &[String]) -> Self {
        let primary_key_idxs = primary_key
            .iter()
            .filter_map(|p| self.schema.column_with_name(p.as_str()))
            .map(|(idx, _)| idx)
            .collect::<Vec<_>>();

        self.table_constraints =
            Constraints::new_unverified(vec![Constraint::PrimaryKey(primary_key_idxs)]);
        self
    }
}

#[derive(Default, Copy, Clone)]
pub struct SinkConnectorFactory {}

impl SinkConnectorFactory {
    #[must_use]
    pub fn new() -> Self {
        Self {}
    }

    #[must_use]
    pub fn new_arc() -> Arc<dyn DataConnectorFactory> {
        Arc::new(Self {}) as Arc<dyn DataConnectorFactory>
    }
}

impl DataConnectorFactory for SinkConnectorFactory {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn create(
        &self,
        params: ConnectorParams,
    ) -> Pin<Box<dyn Future<Output = super::NewDataConnectorResult> + Send>> {
        Box::pin(async move {
            // Inherit the acceleration checkpoint schema when the dataset is accelerated, so a
            // restart re-advertises the stored (e.g. OTLP-evolved) schema instead of the bare
            // `placeholder` and the federated-table reconciliation sees no spurious change.
            // Reading the checkpoint needs the accelerator engine registry and the secrets, so
            // the spec is rebound to the runtime handles from the connector context; without a
            // context (connector unit tests) there is no accelerator to inherit from.
            let schema = match &params.component {
                ConnectorComponent::Dataset(spec) => {
                    params.accelerated_checkpoint_schema(spec).await
                }
                ConnectorComponent::Catalog(_) => None,
            }
            .unwrap_or_else(placeholder_schema);

            Ok(Arc::new(SinkConnector::new(schema)) as Arc<dyn DataConnector>)
        })
    }

    fn prefix(&self) -> &'static str {
        SINK_DATACONNECTOR
    }

    fn parameters(&self) -> &'static [ParameterSpec] {
        &[]
    }
}

#[async_trait]
impl DataConnector for SinkConnector {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn resolve_refresh_mode(&self, refresh_mode: Option<RefreshMode>) -> RefreshMode {
        refresh_mode.unwrap_or(RefreshMode::Disabled)
    }

    async fn read_provider(
        &self,
        _dataset: &Dataset,
    ) -> super::DataConnectorResult<Arc<dyn TableProvider>> {
        Ok(Arc::new(self.clone()))
    }

    async fn read_write_provider(
        &self,
        _dataset: &Dataset,
    ) -> Option<super::DataConnectorResult<Arc<dyn TableProvider>>> {
        Some(Ok(Arc::new(self.clone())))
    }
}

#[async_trait]
impl TableProvider for SinkConnector {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn constraints(&self) -> Option<&Constraints> {
        Some(&self.table_constraints)
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(EmptyExec::new(project_schema(
            &self.schema,
            projection,
        )?)))
    }

    async fn insert_into(
        &self,
        _state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        _overwrite: InsertOp,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(DataSinkExec::new(
            input,
            Arc::new(SinkDataSink::new(self.schema())),
            None,
        )) as _)
    }
}

#[derive(Clone)]
struct SinkDataSink {
    schema: SchemaRef,
}

#[async_trait]
impl DataSink for SinkDataSink {
    fn metrics(&self) -> Option<MetricsSet> {
        None
    }

    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    async fn write_all(
        &self,
        mut data: SendableRecordBatchStream,
        _context: &Arc<TaskContext>,
    ) -> datafusion::common::Result<u64> {
        use futures::StreamExt as _;
        // Drain the stream to satisfy the streaming contract even though
        // the sink discards the data.
        let mut rows: u64 = 0;
        while let Some(batch) = data.next().await {
            rows += batch?.num_rows() as u64;
        }
        Ok(rows)
    }
}

impl SinkDataSink {
    #[must_use]
    pub fn new(schema: SchemaRef) -> Self {
        Self { schema }
    }
}

impl std::fmt::Debug for SinkDataSink {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "SinkDataSink")
    }
}

impl DisplayAs for SinkDataSink {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> std::fmt::Result {
        write!(f, "SinkDataSink")
    }
}

register_data_connector!("sink", SinkConnectorFactory);
