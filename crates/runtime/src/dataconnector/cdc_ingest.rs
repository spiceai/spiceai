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

//! Push-based CDC ingest connector (`from: cdc:<name>`).
//!
//! Debezium source plugins (or any producer) POST change events directly to
//! Spice via `POST /v1/datasets/{name}/cdc`. Events are decoded (JSON or Avro)
//! and applied through the shared `refresh_mode: changes` path — no Kafka.

use std::{any::Any, pin::Pin, sync::Arc, time::Duration};

use arrow::datatypes::SchemaRef;
use async_stream::stream;
use async_trait::async_trait;
use dashmap::DashMap;
use data_components::cdc::{
    self, ChangeBatch, ChangeEnvelope, ChangesStream, CommitChange, CommitError,
    build_ready_signal_envelope,
};
use data_components::debezium::avro::AvroDecodeOptions;
use data_components::debezium::decode::{self, CdcFormat};
use datafusion::{
    catalog::Session,
    common::{Constraint, Constraints, DFSchema, project_schema},
    datasource::{TableProvider, TableType},
    error::Result as DataFusionResult,
    logical_expr::Expr,
    physical_plan::{ExecutionPlan, empty::EmptyExec},
};
use futures::StreamExt;
use snafu::prelude::*;
use tokio::sync::{mpsc, oneshot};

use crate::component::dataset::{
    DatasetSpec,
    acceleration::{Engine, RefreshMode},
};
use data_connector_api::federated::FederatedTableProvider;

use super::{
    ConnectorComponent, ConnectorParams, DataConnector, DataConnectorFactory, ParameterSpec,
    Parameters,
};

const DEFAULT_APPLY_TIMEOUT: Duration = Duration::from_mins(1);
const CHANNEL_CAPACITY: usize = 256;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "No CDC ingest listener is registered for dataset '{dataset}'. Ensure the dataset uses `from: cdc:…` with acceleration refresh_mode: changes and has finished initializing"
    ))]
    NotRegistered { dataset: String },

    #[snafu(display(
        "CDC ingest for dataset '{dataset}' is not accepting events: the change stream has stopped (dataset unloaded or reloading). Retry once the dataset is ready again"
    ))]
    ChannelClosed { dataset: String },

    #[snafu(display("Timed out waiting for CDC changes to apply on dataset '{dataset}'"))]
    ApplyTimeout { dataset: String },

    #[snafu(display("Failed to apply CDC changes on dataset '{dataset}': {message}"))]
    ApplyFailed { dataset: String, message: String },

    #[snafu(display("Failed to decode CDC body for dataset '{dataset}': {message}"))]
    Decode { dataset: String, message: String },

    #[snafu(display(
        "Unsupported CDC Content-Type for dataset '{dataset}'. Use application/json (or application/vnd.debezium+json) or application/avro (or application/vnd.debezium+avro)"
    ))]
    UnsupportedFormat { dataset: String },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Work item handed from the HTTP ingest path to the dataset's changes stream.
struct IngestWork {
    batch: ChangeBatch,
    result_tx: oneshot::Sender<std::result::Result<(), String>>,
}

/// Registered push target for one dataset.
#[derive(Clone)]
pub struct CdcIngestHandle {
    pub schema: SchemaRef,
    pub primary_keys: Vec<String>,
    pub schema_registry_url: Option<String>,
    pub avro_schema_json: Option<String>,
    tx: mpsc::Sender<IngestWork>,
}

impl CdcIngestHandle {
    /// Decode a request body and push it onto the dataset's changes stream,
    /// waiting until the batch has been applied (commit ack).
    pub async fn ingest(
        &self,
        dataset: &str,
        format: CdcFormat,
        body: &[u8],
        avro_schema_override: Option<String>,
        timeout: Duration,
    ) -> Result<usize> {
        let batch = match format {
            CdcFormat::Json => {
                decode::json_body_to_change_batch(&self.schema, &self.primary_keys, body).map_err(
                    |e| Error::Decode {
                        dataset: dataset.to_string(),
                        message: e.to_string(),
                    },
                )?
            }
            CdcFormat::Avro => {
                let options = AvroDecodeOptions {
                    schema_registry_url: self.schema_registry_url.clone(),
                    avro_schema_json: avro_schema_override
                        .or_else(|| self.avro_schema_json.clone()),
                };
                data_components::debezium::avro::avro_body_to_change_batch(
                    &self.schema,
                    &self.primary_keys,
                    body,
                    &options,
                )
                .await
                .map_err(|e| Error::Decode {
                    dataset: dataset.to_string(),
                    message: e.to_string(),
                })?
            }
        };

        let rows = batch.record.num_rows();
        let (result_tx, result_rx) = oneshot::channel();
        // Single end-to-end budget shared by both waits (channel backpressure +
        // apply ack), so the request is bounded by `timeout` overall rather than
        // up to 2× when the channel is near-saturated but still eventually
        // accepts. `send` waits for capacity, so without a bound a saturated
        // channel would hang the request indefinitely and never return 503.
        let deadline = tokio::time::Instant::now() + timeout;
        match tokio::time::timeout_at(deadline, self.tx.send(IngestWork { batch, result_tx })).await
        {
            Ok(Ok(())) => {}
            Ok(Err(_)) => {
                return ChannelClosedSnafu {
                    dataset: dataset.to_string(),
                }
                .fail();
            }
            Err(_) => {
                return ApplyTimeoutSnafu {
                    dataset: dataset.to_string(),
                }
                .fail();
            }
        }

        match tokio::time::timeout_at(deadline, result_rx).await {
            Ok(Ok(Ok(()))) => Ok(rows),
            Ok(Ok(Err(message))) => ApplyFailedSnafu {
                dataset: dataset.to_string(),
                message,
            }
            .fail(),
            Ok(Err(_)) => ApplyFailedSnafu {
                dataset: dataset.to_string(),
                message: "CDC apply worker dropped the ack channel".to_string(),
            }
            .fail(),
            Err(_) => ApplyTimeoutSnafu {
                dataset: dataset.to_string(),
            }
            .fail(),
        }
    }
}

/// Process-wide registry of CDC ingest push targets, keyed by dataset name
/// (as used in `/v1/datasets/{name}/cdc`).
static REGISTRY: std::sync::LazyLock<DashMap<String, CdcIngestHandle>> =
    std::sync::LazyLock::new(DashMap::new);

/// Look up a registered CDC ingest handle by dataset name.
#[must_use]
pub fn lookup(dataset_name: &str) -> Option<CdcIngestHandle> {
    REGISTRY.get(dataset_name).map(|e| e.value().clone())
}

fn register_handle(dataset_name: &str, handle: CdcIngestHandle) {
    // Register under the full table reference and the bare table name so HTTP
    // paths can use either `orders` or a catalog-qualified form.
    REGISTRY.insert(dataset_name.to_string(), handle.clone());
    if let Some(bare) = dataset_name.rsplit('.').next()
        && bare != dataset_name
    {
        REGISTRY.insert(bare.to_string(), handle);
    }
}

/// Remove this dataset's registry entries, but only where the stored handle is
/// still the one `tx` belongs to. On hot reload the replacement stream registers
/// before the outgoing one finishes draining, so an unconditional remove-by-name
/// would delete the *new* handle and 404 a healthy dataset.
fn unregister_handle_owned(dataset_name: &str, tx: &mpsc::Sender<IngestWork>) {
    REGISTRY.remove_if(dataset_name, |_, handle| handle.tx.same_channel(tx));
    if let Some(bare) = dataset_name.rsplit('.').next()
        && bare != dataset_name
    {
        REGISTRY.remove_if(bare, |_, handle| handle.tx.same_channel(tx));
    }
}

/// Unregisters on drop, so the entry cannot outlive the stream that owns it —
/// including when the consumer drops the stream early (dataset unload, refresh
/// task abort), where the stream body's trailing cleanup never runs.
struct RegistryGuard {
    dataset_name: String,
    tx: mpsc::Sender<IngestWork>,
}

impl Drop for RegistryGuard {
    fn drop(&mut self) {
        unregister_handle_owned(&self.dataset_name, &self.tx);
    }
}

#[cfg(test)]
fn unregister_handle(dataset_name: &str) {
    REGISTRY.remove(dataset_name);
    if let Some(bare) = dataset_name.rsplit('.').next()
        && bare != dataset_name
    {
        REGISTRY.remove(bare);
    }
}

/// Commit hook that acks the HTTP caller after the change batch is applied.
struct IngestCommitter {
    result_tx: MutexOption<oneshot::Sender<std::result::Result<(), String>>>,
}

/// `oneshot::Sender` is not `Sync` in a way we need for simple Option — use mutex.
struct MutexOption<T>(parking_lot::Mutex<Option<T>>);

impl<T> MutexOption<T> {
    fn new(v: T) -> Self {
        Self(parking_lot::Mutex::new(Some(v)))
    }

    fn take(&self) -> Option<T> {
        self.0.lock().take()
    }
}

#[async_trait]
impl CommitChange for IngestCommitter {
    async fn commit(&self) -> std::result::Result<(), CommitError> {
        if let Some(tx) = self.result_tx.take() {
            let _ = tx.send(Ok(()));
        }
        Ok(())
    }
}

impl Drop for IngestCommitter {
    fn drop(&mut self) {
        if let Some(tx) = self.result_tx.take() {
            let _ = tx.send(Err("CDC change was dropped before apply commit".to_string()));
        }
    }
}

#[derive(Debug)]
pub struct CdcIngest {
    schema_registry_url: Option<String>,
    avro_schema_json: Option<String>,
}

impl CdcIngest {
    pub fn new(params: &Parameters) -> Self {
        Self {
            schema_registry_url: params
                .get("schema_registry_url")
                .expose()
                .ok()
                .map(ToString::to_string),
            avro_schema_json: params
                .get("avro_schema")
                .expose()
                .ok()
                .map(ToString::to_string),
        }
    }
}

#[derive(Default, Debug, Copy, Clone)]
pub struct CdcIngestFactory {}

impl CdcIngestFactory {
    #[must_use]
    pub fn new() -> Self {
        Self {}
    }

    #[must_use]
    pub fn new_arc() -> Arc<dyn DataConnectorFactory> {
        Arc::new(Self {}) as Arc<dyn DataConnectorFactory>
    }
}

const PARAMETERS: &[ParameterSpec] = &[
    ParameterSpec::component("schema_registry_url").description(
        "Confluent-compatible Schema Registry base URL for Avro CDC ingest (Confluent wire format).",
    ),
    ParameterSpec::component("avro_schema").description(
        "Avro schema JSON used when the request body is raw Avro (not Confluent wire format). Can also be sent per-request via the X-Avro-Schema header.",
    ),
];

impl DataConnectorFactory for CdcIngestFactory {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn create(
        &self,
        params: ConnectorParams,
    ) -> Pin<Box<dyn std::future::Future<Output = super::NewDataConnectorResult> + Send>> {
        Box::pin(async move {
            Ok(Arc::new(CdcIngest::new(&params.parameters)) as Arc<dyn DataConnector>)
        })
    }

    fn prefix(&self) -> &'static str {
        "cdc"
    }

    fn parameters(&self) -> &'static [ParameterSpec] {
        PARAMETERS
    }
}

data_connector_api::register_data_connector!("cdc", CdcIngestFactory);

#[async_trait]
impl DataConnector for CdcIngest {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn resolve_refresh_mode(&self, refresh_mode: Option<RefreshMode>) -> RefreshMode {
        refresh_mode.unwrap_or(RefreshMode::Changes)
    }

    async fn read_provider(
        &self,
        dataset: &DatasetSpec,
    ) -> super::DataConnectorResult<Arc<dyn TableProvider>> {
        let Some(acceleration) = dataset
            .acceleration
            .as_ref()
            .filter(|acceleration| acceleration.enabled)
        else {
            return super::InvalidConfigurationNoSourceSnafu {
                dataconnector: "cdc",
                message: "The CDC ingest connector requires an accelerated dataset with refresh_mode: changes. For details, visit: https://spiceai.org/docs/features/cdc",
                connector_component: ConnectorComponent::from(dataset),
            }
            .fail();
        };

        ensure!(
            self.resolve_refresh_mode(acceleration.refresh_mode) == RefreshMode::Changes,
            super::InvalidConfigurationNoSourceSnafu {
                dataconnector: "cdc",
                message: "The CDC ingest connector requires refresh_mode: changes",
                connector_component: ConnectorComponent::from(dataset),
            }
        );

        let schema = dataset.schema.clone().ok_or_else(|| {
            super::DataConnectorError::InvalidConfigurationNoSource {
                dataconnector: "cdc".to_string(),
                message: "The CDC ingest connector requires a declared schema. Add `columns` with types on the dataset so change events can be applied without peeking a message bus".to_string(),
                connector_component: ConnectorComponent::from(dataset),
            }
        })?;

        let primary_keys: Vec<String> = acceleration
            .primary_key
            .as_ref()
            .map(|pk| pk.iter().map(ToString::to_string).collect())
            .unwrap_or_default();

        ensure!(
            !primary_keys.is_empty()
                || matches!(acceleration.engine.to_unpartitioned(), Engine::Arrow),
            super::InvalidConfigurationNoSourceSnafu {
                dataconnector: "cdc",
                message: "The CDC ingest connector requires acceleration.primary_key (except for the Arrow engine). Configure primary_key and on_conflict upsert for UPDATE/DELETE apply",
                connector_component: ConnectorComponent::from(dataset),
            }
        );

        let constraints = constraints_from_keys(&schema, &primary_keys);

        Ok(Arc::new(CdcIngestTable {
            schema,
            primary_keys,
            constraints,
            dataset_name: dataset.name.to_string(),
            schema_registry_url: self.schema_registry_url.clone(),
            avro_schema_json: self.avro_schema_json.clone(),
        }))
    }

    fn supports_changes_stream(&self) -> bool {
        true
    }

    fn changes_stream(
        &self,
        federated_table: Arc<dyn FederatedTableProvider>,
        dataset: &DatasetSpec,
    ) -> Option<ChangesStream> {
        let dataset_name = dataset.name.to_string();
        Some(Box::pin(stream! {
            let table_provider = federated_table.table_provider().await;
            // Search the provider chain rather than downcasting the outermost layer:
            // spicepod metadata, embeddings and full-text search each wrap the
            // connector's own provider before it reaches the changes stream.
            let Some(table) =
                crate::search::util::find_concrete_table_provider::<CdcIngestTable>(&table_provider)
            else {
                tracing::error!(
                    dataset = %dataset_name,
                    "CDC ingest could not resolve its table provider, so no change events will be accepted. This dataset will not become ready"
                );
                return;
            };

            let mut changes_stream = table.stream_changes();
            while let Some(item) = changes_stream.next().await {
                yield item;
            }
        }))
    }
}

struct CdcIngestTable {
    schema: SchemaRef,
    primary_keys: Vec<String>,
    constraints: Option<Constraints>,
    dataset_name: String,
    schema_registry_url: Option<String>,
    avro_schema_json: Option<String>,
}

impl CdcIngestTable {
    fn stream_changes(&self) -> ChangesStream {
        let (tx, mut rx) = mpsc::channel::<IngestWork>(CHANNEL_CAPACITY);
        register_handle(
            &self.dataset_name,
            CdcIngestHandle {
                schema: Arc::clone(&self.schema),
                primary_keys: self.primary_keys.clone(),
                schema_registry_url: self.schema_registry_url.clone(),
                avro_schema_json: self.avro_schema_json.clone(),
                tx: tx.clone(),
            },
        );

        let schema = Arc::clone(&self.schema);
        let dataset_name = self.dataset_name.clone();
        // Moved into the stream, so every exit path — normal end, early return,
        // or the consumer dropping the stream mid-await — unregisters exactly
        // this registration.
        let guard = RegistryGuard {
            dataset_name: dataset_name.clone(),
            tx,
        };

        Box::pin(stream! {
            let _guard = guard;
            match build_ready_signal_envelope(&schema) {
                Ok(ready) => yield Ok(ready),
                Err(e) => {
                    tracing::error!(
                        dataset = %dataset_name,
                        "Failed to build CDC ready signal: {e}"
                    );
                    yield Err(cdc::StreamError::External(e.to_string()));
                    return;
                }
            }

            tracing::info!(
                dataset = %dataset_name,
                "CDC ingest listening for Debezium change events on POST /v1/datasets/{dataset_name}/cdc"
            );

            while let Some(work) = rx.recv().await {
                let committer = IngestCommitter {
                    result_tx: MutexOption::new(work.result_tx),
                };
                yield Ok(ChangeEnvelope::new(
                    Box::new(committer),
                    work.batch,
                    true,
                ));
            }
        })
    }
}

impl std::fmt::Debug for CdcIngestTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CdcIngestTable")
            .field("dataset_name", &self.dataset_name)
            .field("schema", &self.schema)
            .field("primary_keys", &self.primary_keys)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl TableProvider for CdcIngestTable {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn constraints(&self) -> Option<&Constraints> {
        self.constraints.as_ref()
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(EmptyExec::new(project_schema(
            &self.schema,
            projection,
        )?)))
    }
}

/// Public helper used by the HTTP layer.
pub async fn ingest_http_body(
    dataset_name: &str,
    content_type: Option<&str>,
    body: &[u8],
    avro_schema_header: Option<String>,
) -> Result<usize> {
    let handle = lookup(dataset_name).context(NotRegisteredSnafu {
        dataset: dataset_name.to_string(),
    })?;

    let format = content_type
        .and_then(CdcFormat::from_content_type)
        .unwrap_or(CdcFormat::Json);

    // octet-stream without registry/schema is ambiguous — require explicit avro markers
    if matches!(format, CdcFormat::Avro)
        && content_type.is_some_and(|ct| {
            ct.to_ascii_lowercase()
                .split(';')
                .next()
                .is_some_and(|t| t.trim() == "application/octet-stream")
        })
        && handle.schema_registry_url.is_none()
        && handle.avro_schema_json.is_none()
        && avro_schema_header.is_none()
    {
        return UnsupportedFormatSnafu {
            dataset: dataset_name.to_string(),
        }
        .fail();
    }

    if content_type.is_some()
        && CdcFormat::from_content_type(content_type.unwrap_or_default()).is_none()
    {
        // Unknown content-type — still try JSON if it looks like JSON. Skip
        // leading whitespace first: pretty-printed bodies and NDJSON with a
        // leading newline are valid JSON but do not start with `{`/`[`.
        let first_non_ws = body.iter().find(|b| !b.is_ascii_whitespace());
        if matches!(first_non_ws, Some(&b'{' | &b'[')) {
            return handle
                .ingest(
                    dataset_name,
                    CdcFormat::Json,
                    body,
                    None,
                    DEFAULT_APPLY_TIMEOUT,
                )
                .await;
        }
        return UnsupportedFormatSnafu {
            dataset: dataset_name.to_string(),
        }
        .fail();
    }

    handle
        .ingest(
            dataset_name,
            format,
            body,
            avro_schema_header,
            DEFAULT_APPLY_TIMEOUT,
        )
        .await
}

fn constraints_from_keys(schema: &SchemaRef, primary_keys: &[String]) -> Option<Constraints> {
    let Ok(df_schema) = DFSchema::try_from(Arc::clone(schema)) else {
        return None;
    };
    let pk_indices: Vec<usize> = primary_keys
        .iter()
        .filter_map(|pk| df_schema.index_of_column_by_name(None, pk))
        .collect();
    if pk_indices.is_empty() {
        None
    } else {
        Some(Constraints::new_unverified(vec![Constraint::PrimaryKey(
            pk_indices,
        )]))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use data_components::cdc::changes_schema;

    #[tokio::test]
    async fn json_ingest_applies_and_acks() {
        let data_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
        ]));
        // ChangeBatch schema wraps the table schema; stream_changes uses table
        // schema for ready signal — use the data schema as the table schema.
        let (tx, mut rx) = mpsc::channel::<IngestWork>(4);
        register_handle(
            "test_orders",
            CdcIngestHandle {
                schema: Arc::clone(&data_schema),
                primary_keys: vec!["id".to_string()],
                schema_registry_url: None,
                avro_schema_json: None,
                tx,
            },
        );

        let body = br#"{"before":null,"after":{"id":1,"name":"a"},"op":"c","ts_ms":1,"source":{}}"#;
        let handle = lookup("test_orders").expect("registered");

        let ingest = tokio::spawn(async move {
            handle
                .ingest(
                    "test_orders",
                    CdcFormat::Json,
                    body,
                    None,
                    Duration::from_secs(5),
                )
                .await
        });

        let work = rx.recv().await.expect("work");
        assert_eq!(work.batch.record.num_rows(), 1);
        // Simulate successful apply.
        work.result_tx.send(Ok(())).expect("ack");

        let applied = ingest.await.expect("join").expect("ingest ok");
        assert_eq!(applied, 1);

        unregister_handle("test_orders");
        assert!(lookup("test_orders").is_none());

        // changes_schema is used by the real path; keep a smoke check.
        let _ = changes_schema(&data_schema);
    }

    #[tokio::test]
    async fn not_registered_returns_error() {
        let err = ingest_http_body("missing_ds", Some("application/json"), b"{}", None)
            .await
            .expect_err("should fail");
        assert!(matches!(err, Error::NotRegistered { .. }));
    }
}
