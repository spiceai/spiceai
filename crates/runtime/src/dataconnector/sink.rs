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

use crate::dataconnector::ConnectorContext;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion_datasource::sink::{DataSink, DataSinkExec};

use std::{any::Any, fmt, pin::Pin, sync::Arc};

use crate::component::dataset::{
    Dataset, DatasetSpec,
    acceleration::{Acceleration, Engine, RefreshMode},
};
use crate::dataaccelerator::spice_sys::dataset_checkpointer;
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
use runtime_acceleration::sidecar::OpenOption;
use runtime_acceleration::snapshot::SnapshotBehavior;

use super::{
    ConnectorComponent, ConnectorParams, DataConnector, DataConnectorError, DataConnectorFactory,
    ParameterSpec,
};
use crate::opentelemetry_logs::{LOGS_STORE_SCHEMA, OTLP_LOGS_PARAM, is_otlp_logs_store_param};

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
    recorded_checkpoint_schema(dataset)
        .await
        .map(runtime_table::accelerated::checkpoint_primary_key::without_acceleration_primary_key)
}

/// The acceleration checkpoint's schema as stored, including the primary key it
/// records for registering without the source (see
/// `runtime_table::accelerated::checkpoint_primary_key`). Everything that registers
/// a table from the checkpoint schema uses [`accelerated_checkpoint_schema`], which
/// leaves that key out.
pub(crate) async fn recorded_checkpoint_schema(dataset: &Dataset) -> Option<SchemaRef> {
    if !dataset.is_file_accelerated() {
        return None;
    }
    let registry = dataset.runtime.accelerator_engine_registry();
    let checkpoint = dataset_checkpointer(
        dataset,
        registry,
        OpenOption::OpenExisting,
        SnapshotBehavior::Disabled,
    )
    .await
    .ok()?;
    checkpoint.get_schema().await.ok().flatten()
}

/// Whether an accelerated `sink` dataset can be registered when the runtime loads it, rather
/// than parked until its first write supplies a schema.
///
/// A `sink` produces no rows of its own, so there is normally nothing to build a table from
/// until a write arrives. An acceleration that already holds this dataset's data is the
/// exception: its schema checkpoint is what `source` advertises (see
/// [`accelerated_checkpoint_schema`]), so the stored rows can be served from startup instead
/// of only once the next write registers the dataset.
///
/// Scoped to Cayenne, the engine whose files and metastore *are* the dataset — every other
/// engine keeps the wait-for-first-write behavior.
pub(crate) fn registers_from_acceleration(
    acceleration: Option<&Acceleration>,
    source: &dyn DataConnector,
) -> bool {
    acceleration.is_some_and(|settings| settings.engine == Engine::Cayenne)
        && source
            .as_any()
            .downcast_ref::<SinkConnector>()
            .is_some_and(SinkConnector::has_known_schema)
}

/// Whether a dataset's spec routes OTLP logs to it (`otlp_logs: all`).
fn is_otlp_logs_dataset(spec: &DatasetSpec) -> bool {
    spec.params
        .get(OTLP_LOGS_PARAM)
        .is_some_and(|value| is_otlp_logs_store_param(value))
}

/// The configuration error for an `otlp_logs: all` dataset that Cayenne does not accelerate,
/// or `None` when it does. The logs store is built for Cayenne's append path, and a sink with
/// no acceleration would discard every record.
fn otlp_logs_acceleration_error(spec: &DatasetSpec) -> Option<String> {
    let cayenne = spec
        .acceleration
        .as_ref()
        .is_some_and(|acceleration| acceleration.enabled && acceleration.engine == Engine::Cayenne);
    (!cayenne).then(|| {
        "An OpenTelemetry logs dataset (`otlp_logs: all`) is stored by Cayenne. Set `acceleration.enabled: true` and `acceleration.engine: cayenne`. See: https://spiceai.org/docs/features/observability".to_string()
    })
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
    /// Whether [`Self::schema`] is known before the first write: the one the acceleration
    /// already stores for this dataset, or a fixed schema such as the OTLP logs store's, rather
    /// than the bare [`placeholder_schema`]. Read by [`registers_from_acceleration`].
    schema_known: bool,
}

impl SinkConnector {
    #[must_use]
    pub fn new(schema: SchemaRef) -> Self {
        Self {
            schema,
            table_constraints: Constraints::new_unverified(vec![]),
            schema_known: false,
        }
    }

    /// A connector advertising a schema known before the first write: the one the dataset's
    /// acceleration has checkpointed, or a fixed schema, as opposed to one whose schema came
    /// from a write or from [`placeholder_schema`].
    #[must_use]
    fn with_known_schema(schema: SchemaRef) -> Self {
        Self {
            schema_known: true,
            ..Self::new(schema)
        }
    }

    /// Whether this connector's schema is known before the first write, which is what makes
    /// the dataset registrable before its first write.
    #[must_use]
    pub(crate) fn has_known_schema(&self) -> bool {
        self.schema_known
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

    fn create<'a>(
        &'a self,
        params: ConnectorParams,
        context: &'a dyn ConnectorContext,
    ) -> Pin<Box<dyn Future<Output = super::NewDataConnectorResult> + Send + 'a>> {
        Box::pin(async move {
            // Inherit the acceleration checkpoint schema when the dataset is accelerated, so a
            // restart re-advertises the stored (e.g. OTLP-evolved) schema instead of the bare
            // `placeholder` and the federated-table reconciliation sees no spurious change.
            // Reading the checkpoint needs the accelerator engine registry and the secrets, so
            // the spec is rebound to the runtime handles from the connector context; without a
            // context (connector unit tests) there is no accelerator to inherit from.
            // An OTLP logs dataset has a fixed schema, so it is registered when it loads, and
            // is queryable before its first export, instead of waiting for a write.
            if let ConnectorComponent::Dataset(spec) = &params.component
                && is_otlp_logs_dataset(spec)
            {
                if let Some(message) = otlp_logs_acceleration_error(spec) {
                    return Err(DataConnectorError::InvalidConfigurationNoSource {
                        dataconnector: SINK_DATACONNECTOR.to_string(),
                        connector_component: params.component.clone(),
                        message,
                    }
                    .into());
                }
                return Ok(Arc::new(SinkConnector::with_known_schema(Arc::clone(
                    &LOGS_STORE_SCHEMA,
                ))) as Arc<dyn DataConnector>);
            }

            let checkpoint_schema = match &params.component {
                ConnectorComponent::Dataset(spec) => {
                    context.accelerated_checkpoint_schema(spec).await
                }
                ConnectorComponent::Catalog(_) => None,
            };

            let connector = match checkpoint_schema {
                Some(schema) => SinkConnector::with_known_schema(schema),
                None => SinkConnector::new(placeholder_schema()),
            };

            Ok(Arc::new(connector) as Arc<dyn DataConnector>)
        })
    }

    fn prefix(&self) -> &'static str {
        SINK_DATACONNECTOR
    }

    fn parameters(&self) -> &'static [ParameterSpec] {
        PARAMETERS
    }
}

const PARAMETERS: &[ParameterSpec] = &[
    // Read by the OTLP metrics service from the dataset's params. Listing it here keeps
    // parameter processing from warning that it is unsupported, and rejects any other value
    // when the dataset is built.
    ParameterSpec::runtime(crate::opentelemetry_store::OTLP_METRICS_PARAM)
        .one_of_ignore_ascii_case(&[crate::opentelemetry_store::OTLP_METRICS_PARAM_ALL])
        .description(
            "Set to `all` to store every metric received on `/v1/metrics` in this dataset.",
        ),
    // Read by the OTLP logs service from the dataset's params, as `otlp_metrics` is.
    ParameterSpec::runtime(OTLP_LOGS_PARAM)
        .one_of_ignore_ascii_case(&[crate::opentelemetry_logs::OTLP_LOGS_PARAM_ALL])
        .description(
            "Set to `all` to store every log record received on `/v1/logs` in this dataset. Requires Cayenne acceleration.",
        ),
];

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
        _context: &dyn ConnectorContext,
        _dataset: &DatasetSpec,
    ) -> super::DataConnectorResult<Arc<dyn TableProvider>> {
        Ok(Arc::new(self.clone()))
    }

    async fn read_write_provider(
        &self,
        _context: &dyn ConnectorContext,
        _dataset: &DatasetSpec,
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

data_connector_api::register_data_connector!("sink", SinkConnectorFactory);

#[cfg(test)]
mod tests {
    use super::*;

    fn acceleration(engine: Engine) -> Acceleration {
        Acceleration {
            engine,
            ..Acceleration::default()
        }
    }

    fn stored_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("time_unix_nano", DataType::UInt64, true),
            Field::new("value", DataType::Float64, true),
        ]))
    }

    #[test]
    fn cayenne_sink_carrying_its_acceleration_schema_registers_immediately() {
        let source = SinkConnector::with_known_schema(stored_schema());

        assert!(registers_from_acceleration(
            Some(&acceleration(Engine::Cayenne)),
            &source
        ));
    }

    #[test]
    fn sink_without_a_stored_schema_waits_for_its_first_write() {
        let source = SinkConnector::new(placeholder_schema());

        assert!(
            !registers_from_acceleration(Some(&acceleration(Engine::Cayenne)), &source),
            "a sink with nothing but the placeholder schema has no table to register yet"
        );
    }

    #[test]
    fn only_cayenne_registers_a_sink_from_its_acceleration() {
        let source = SinkConnector::with_known_schema(stored_schema());

        for engine in [
            Engine::Arrow,
            Engine::PartitionedArrow,
            Engine::DuckDB,
            Engine::Sqlite,
            Engine::Turso,
            Engine::PostgreSQL,
        ] {
            assert!(
                !registers_from_acceleration(Some(&acceleration(engine)), &source),
                "{engine} keeps the wait-for-first-write behavior"
            );
        }
    }

    #[test]
    fn an_unaccelerated_sink_has_no_acceleration_to_register_from() {
        let source = SinkConnector::with_known_schema(stored_schema());

        assert!(!registers_from_acceleration(None, &source));
    }

    #[test]
    fn a_sink_advertising_its_acceleration_schema_serves_that_schema() {
        let source = SinkConnector::with_known_schema(stored_schema());

        assert!(source.has_known_schema());
        assert_eq!(source.schema(), stored_schema());
        assert!(!SinkConnector::new(placeholder_schema()).has_known_schema());
    }
}
