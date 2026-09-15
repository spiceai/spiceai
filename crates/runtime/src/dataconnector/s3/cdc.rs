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

//! S3 event-driven CDC: SQS long-poll → object read → `ChangesStream`.
//!
//! `try_stream!` keeps the snapshot, long-poll, and per-message apply path in
//! one backpressured generator (the same shape as MongoDB / Kafka CDC). A
//! manual `Stream` impl would split that state machine across poll/yield
//! points without changing behavior.

use super::event::{
    ObjectEventKind, S3ObjectEvent, matches_dataset, parse_notification_body, s3_object_from,
};
use super::{S3, S3_DOCS};
use crate::dataconnector::federated::FederatedTableProvider;
use crate::dataconnector::listing::ListingTableConnector;
use crate::dataconnector::parameters::ConnectorContext;
use crate::dataconnector::{ConnectorComponent, DataConnectorError, DataConnectorResult};
use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use async_stream::try_stream;
use async_trait::async_trait;
use data_components::cdc::{
    AccelerationContents, ChangeEnvelope, ChangesStream, CommitChange, CommitError, NoOpCommitter,
    StreamError, build_history_unavailable_envelope, build_ready_signal_envelope, shutdown_epoch,
    wrap_data_as_change_batch,
};
use datafusion::datasource::TableProvider;
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::prelude::SessionContext;
use futures::StreamExt;
use runtime_component::dataset::DatasetSpec;
use runtime_component::dataset::acceleration::RefreshMode;
use runtime_parameters::{ExposedParamLookup, Parameters};
use snafu::prelude::*;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;

const SQS_LONG_POLL_SECONDS: i32 = 20;
const SQS_MAX_MESSAGES: i32 = 10;
const SQS_VISIBILITY_TIMEOUT_SECONDS: i32 = 300;
const RECEIVE_ERROR_BACKOFF_CAP: Duration = Duration::from_secs(30);

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Failed to register dataset {dataset_name} (s3): `refresh_mode: changes` requires `s3_cdc_queue_url` set to an SQS queue subscribed to S3 event notifications. Set `s3_cdc_queue_url` to the queue URL (not ARN). See: {S3_DOCS}"
    ))]
    MissingQueueUrl { dataset_name: String },

    #[snafu(display(
        "Failed to register dataset {dataset_name} (s3): `s3_cdc_queue_url` is set, but `acceleration.refresh_mode` is not `changes`, so the queue would never be consumed. Set `refresh_mode: changes` or remove `s3_cdc_queue_url`. See: {S3_DOCS}"
    ))]
    QueueWithoutChanges { dataset_name: String },

    #[snafu(display(
        "Failed to register dataset {dataset_name} (s3): `s3_cdc_queue_url` must be an SQS queue URL (https://sqs.<region>.amazonaws.com/...), not an ARN. See: {S3_DOCS}"
    ))]
    QueueUrlIsArn { dataset_name: String },

    #[snafu(display(
        "Failed to register dataset {dataset_name} (s3): `s3_cdc_queue_url` is empty. Set it to the SQS queue URL that receives S3 event notifications. See: {S3_DOCS}"
    ))]
    EmptyQueueUrl { dataset_name: String },

    #[snafu(display(
        "Failed to register dataset {dataset_name} (s3): `s3_cdc_events` value '{value}' is not supported. Use `object_created` or `object_created_and_removed`. See: {S3_DOCS}"
    ))]
    InvalidEvents { dataset_name: String, value: String },

    #[snafu(display(
        "Failed to register dataset {dataset_name} (s3): `s3_cdc_key_prefix` '{configured}' is not under the dataset path prefix '{dataset_prefix}'. Use a prefix equal to or nested under the `from` path. See: {S3_DOCS}"
    ))]
    KeyPrefixOutsideDataset {
        dataset_name: String,
        configured: String,
        dataset_prefix: String,
    },

    #[snafu(display(
        "Failed to register dataset {dataset_name} (s3): `s3_auth: public` cannot receive from SQS. Use `iam_role` or `key` credentials that are allowed to call `sqs:ReceiveMessage` and `sqs:DeleteMessage`. See: {S3_DOCS}"
    ))]
    PublicAuthCannotConsumeSqs { dataset_name: String },

    #[snafu(display(
        "Failed to register dataset {dataset_name} (s3): no AWS region for the SQS queue. Set `s3_cdc_region` or `s3_region`, or use a queue URL that includes the region (https://sqs.<region>.amazonaws.com/...). See: {S3_DOCS}"
    ))]
    MissingRegion { dataset_name: String },

    #[snafu(display(
        "Failed to register dataset {dataset_name} (s3): dataset `from` '{from}' does not include an S3 bucket. Use `s3://bucket/prefix`. See: {S3_DOCS}"
    ))]
    MissingBucket { dataset_name: String, from: String },

    #[snafu(display(
        "Failed to create an SQS client for dataset {dataset_name} (s3): {source}. Check AWS credentials and `s3_cdc_region`. See: {S3_DOCS}"
    ))]
    SqsClient {
        dataset_name: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CdcEvents {
    ObjectCreated,
    ObjectCreatedAndRemoved,
}

impl CdcEvents {
    fn parse(value: &str) -> Option<Self> {
        match value {
            "object_created" => Some(Self::ObjectCreated),
            "object_created_and_removed" => Some(Self::ObjectCreatedAndRemoved),
            _ => None,
        }
    }

    fn includes_removed(self) -> bool {
        matches!(self, Self::ObjectCreatedAndRemoved)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct S3CdcConfig {
    pub queue_url: String,
    pub region: String,
    pub events: CdcEvents,
    pub bucket: String,
    pub key_prefix: String,
}

#[async_trait]
pub trait MessageQueue: Send + Sync {
    async fn receive(&self) -> std::result::Result<Vec<QueueMessage>, QueueError>;
    async fn delete(&self, receipt_handle: &str) -> std::result::Result<(), QueueError>;
}

#[derive(Debug, Clone)]
pub struct QueueMessage {
    pub body: String,
    pub receipt_handle: String,
}

#[derive(Debug, Snafu)]
pub enum QueueError {
    #[snafu(display("Failed to receive messages from SQS queue '{queue_url}': {source}"))]
    Receive {
        queue_url: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },
    #[snafu(display("Failed to delete an SQS message from queue '{queue_url}': {source}"))]
    Delete {
        queue_url: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

#[async_trait]
pub trait ObjectReader: Send + Sync {
    async fn read_object(
        &self,
        bucket: &str,
        key: &str,
    ) -> std::result::Result<Vec<RecordBatch>, StreamError>;
}

struct SqsDeleteCommitter {
    queue: Arc<dyn MessageQueue>,
    receipt_handle: String,
}

#[async_trait]
impl CommitChange for SqsDeleteCommitter {
    async fn commit(&self) -> std::result::Result<(), CommitError> {
        self.queue
            .delete(&self.receipt_handle)
            .await
            .map_err(|source| CommitError::UnableToCommitChange {
                source: Box::new(source),
            })
    }
}

struct SqsQueue {
    client: aws_sdk_sqs::Client,
    queue_url: String,
}

#[async_trait]
impl MessageQueue for SqsQueue {
    async fn receive(&self) -> std::result::Result<Vec<QueueMessage>, QueueError> {
        let output = self
            .client
            .receive_message()
            .queue_url(&self.queue_url)
            .max_number_of_messages(SQS_MAX_MESSAGES)
            .wait_time_seconds(SQS_LONG_POLL_SECONDS)
            .visibility_timeout(SQS_VISIBILITY_TIMEOUT_SECONDS)
            .send()
            .await
            .map_err(|source| QueueError::Receive {
                queue_url: self.queue_url.clone(),
                source: Box::new(source),
            })?;

        Ok(output
            .messages
            .unwrap_or_default()
            .into_iter()
            .filter_map(|message| {
                Some(QueueMessage {
                    body: message.body?,
                    receipt_handle: message.receipt_handle?,
                })
            })
            .collect())
    }

    async fn delete(&self, receipt_handle: &str) -> std::result::Result<(), QueueError> {
        self.client
            .delete_message()
            .queue_url(&self.queue_url)
            .receipt_handle(receipt_handle)
            .send()
            .await
            .map_err(|source| QueueError::Delete {
                queue_url: self.queue_url.clone(),
                source: Box::new(source),
            })?;
        Ok(())
    }
}

struct ListingObjectReader {
    connector: S3,
    dataset: DatasetSpec,
}

#[async_trait]
impl ObjectReader for ListingObjectReader {
    async fn read_object(
        &self,
        bucket: &str,
        key: &str,
    ) -> std::result::Result<Vec<RecordBatch>, StreamError> {
        let object_from = s3_object_from(bucket, key).map_err(|error| {
            StreamError::External(format!(
                "S3 CDC cannot build an object URL for s3://{bucket}/{key}: {error}"
            ))
        })?;
        let url = self
            .connector
            .get_object_store_url(&self.dataset, Some(&object_from))
            .map_err(|e| StreamError::Connector {
                connector: "S3",
                source: Box::new(e),
            })?;
        let (format_opt, extension) = self
            .connector
            .get_file_format_and_extension(&self.dataset)
            .await
            .map_err(|e| StreamError::Connector {
                connector: "S3",
                source: Box::new(e),
            })?;
        let file_format = format_opt.ok_or_else(|| {
            StreamError::External(format!(
                "S3 CDC cannot read unstructured text object s3://{bucket}/{key} for dataset '{}'. Set `file_format` to parquet, csv, or json. See: {S3_DOCS}",
                self.dataset.name
            ))
        })?;
        let table = self
            .connector
            .create_listing_table(&self.dataset, &url, &extension, file_format)
            .await
            .map_err(|e| StreamError::Connector {
                connector: "S3",
                source: Box::new(e),
            })?;
        let ctx = self.connector.get_session_context();
        let df = ctx
            .read_table(table)
            .map_err(|e| StreamError::Arrow(e.to_string()))?;
        df.collect()
            .await
            .map_err(|e| StreamError::Arrow(e.to_string()))
    }
}

/// Fail closed on S3 CDC misconfiguration before the listing table is built.
///
/// # Errors
///
/// Returns [`DataConnectorError::InvalidConfigurationNoSource`] when
/// `refresh_mode: changes` is missing `s3_cdc_queue_url` (or the reverse), the
/// queue value is an ARN, `s3_auth` is `public`, `s3_cdc_events` is unknown,
/// `s3_cdc_key_prefix` is outside the dataset path, or no SQS region can be
/// resolved.
pub fn validate_s3_cdc_config(
    params: &Parameters,
    dataset: &DatasetSpec,
) -> DataConnectorResult<()> {
    match S3CdcConfig::try_from_params(params, dataset) {
        Ok(_) => Ok(()),
        Err(error) => Err(DataConnectorError::InvalidConfigurationNoSource {
            dataconnector: "s3".to_string(),
            connector_component: ConnectorComponent::from(dataset),
            message: error.to_string(),
        }),
    }
}

impl S3CdcConfig {
    /// `Ok(None)` when CDC is not configured (no queue, not `changes`).
    fn try_from_params(params: &Parameters, dataset: &DatasetSpec) -> Result<Option<Self>> {
        let dataset_name = dataset.name.to_string();
        let queue_raw = params.get("cdc_queue_url").expose().ok();
        let empty_queue = queue_raw.is_some_and(|url| url.trim().is_empty());
        let queue_url = queue_raw
            .map(str::trim)
            .filter(|url| !url.is_empty())
            .map(ToString::to_string);
        let is_changes = dataset.acceleration.as_ref().is_some_and(|acceleration| {
            acceleration.enabled && acceleration.refresh_mode == Some(RefreshMode::Changes)
        });

        if empty_queue {
            return EmptyQueueUrlSnafu { dataset_name }.fail();
        }

        match (queue_url.as_deref(), is_changes) {
            (None, false) => Ok(None),
            (None, true) => MissingQueueUrlSnafu { dataset_name }.fail(),
            (Some(_), false) => QueueWithoutChangesSnafu { dataset_name }.fail(),
            (Some(url), true) => {
                if url.starts_with("arn:") {
                    return QueueUrlIsArnSnafu { dataset_name }.fail();
                }
                if params.get("auth").expose().ok() == Some("public") {
                    return PublicAuthCannotConsumeSqsSnafu { dataset_name }.fail();
                }

                let events = match params.get("cdc_events").expose().ok() {
                    None => CdcEvents::ObjectCreated,
                    Some(value) => CdcEvents::parse(value).context(InvalidEventsSnafu {
                        dataset_name: dataset_name.clone(),
                        value,
                    })?,
                };

                let (bucket, dataset_prefix) = bucket_and_key_prefix(dataset)?;
                let key_prefix = match params.get("cdc_key_prefix").expose().ok() {
                    None => dataset_prefix,
                    Some(configured) => {
                        let normalized = normalize_prefix(configured);
                        ensure!(
                            prefix_is_nested_under(&normalized, &dataset_prefix),
                            KeyPrefixOutsideDatasetSnafu {
                                dataset_name: dataset_name.clone(),
                                configured: configured.to_string(),
                                dataset_prefix,
                            }
                        );
                        normalized
                    }
                };

                let region =
                    resolve_region(params, url).context(MissingRegionSnafu { dataset_name })?;

                Ok(Some(Self {
                    queue_url: url.to_string(),
                    region,
                    events,
                    bucket,
                    key_prefix,
                }))
            }
        }
    }
}

fn bucket_and_key_prefix(dataset: &DatasetSpec) -> Result<(String, String)> {
    let path = dataset.path().trim_start_matches('/');
    let (bucket, rest) = match path.split_once('/') {
        Some((bucket, rest)) => (bucket, rest),
        None => (path, ""),
    };
    ensure!(
        !bucket.is_empty(),
        MissingBucketSnafu {
            dataset_name: dataset.name.to_string(),
            from: dataset.from.clone(),
        }
    );
    Ok((bucket.to_string(), normalize_prefix(rest)))
}

fn normalize_prefix(prefix: &str) -> String {
    let trimmed = prefix.trim().trim_start_matches('/');
    if trimmed.is_empty() {
        String::new()
    } else if trimmed.ends_with('/') {
        trimmed.to_string()
    } else {
        format!("{trimmed}/")
    }
}

fn prefix_is_nested_under(child: &str, parent: &str) -> bool {
    parent.is_empty() || child == parent || child.starts_with(parent)
}

fn resolve_region(params: &Parameters, queue_url: &str) -> Option<String> {
    params
        .get("cdc_region")
        .expose()
        .ok()
        .map(ToString::to_string)
        .or_else(|| region_from_queue_url(queue_url))
        .or_else(|| params.get("region").expose().ok().map(ToString::to_string))
}

#[must_use]
pub fn region_from_queue_url(queue_url: &str) -> Option<String> {
    let parsed = url::Url::parse(queue_url).ok()?;
    let host = parsed.host_str()?;
    // `sqs.us-east-1.amazonaws.com` or `sqs.cn-north-1.amazonaws.com.cn`
    let mut labels = host.split('.');
    let service = labels.next()?;
    let region = labels.next()?;
    if service == "sqs" && region != "amazonaws" && region != "localhost" {
        Some(region.to_string())
    } else {
        None
    }
}

async fn build_sqs_client(
    params: &Parameters,
    region: &str,
    dataset_name: &str,
) -> Result<aws_sdk_sqs::Client> {
    let auth = params.get("auth").expose().ok();
    if matches!(auth, Some("key")) {
        let access_key = params
            .get("key")
            .expose()
            .ok()
            .ok_or_else(|| Error::SqsClient {
                dataset_name: dataset_name.to_string(),
                source: "s3_auth is `key` but `s3_key` is not set".into(),
            })?;
        let secret_key = params
            .get("secret")
            .expose()
            .ok()
            .ok_or_else(|| Error::SqsClient {
                dataset_name: dataset_name.to_string(),
                source: "s3_auth is `key` but `s3_secret` is not set".into(),
            })?;
        let session_token = params
            .get("session_token")
            .expose()
            .ok()
            .map(ToString::to_string);
        let credentials = aws_credential_types::Credentials::new(
            access_key,
            secret_key,
            session_token,
            None,
            "spice-s3-cdc",
        );
        let sdk_config = aws_sdk_credential_bridge::default_aws_config()
            .region(aws_config::Region::new(region.to_string()))
            .credentials_provider(credentials)
            .load()
            .await;
        return Ok(aws_sdk_sqs::Client::new(&sdk_config));
    }

    let sdk_config = aws_sdk_credential_bridge::get_or_init_sdk_config_with_region(Some(region))
        .await
        .map_err(|source| Error::SqsClient {
            dataset_name: dataset_name.to_string(),
            source: Box::new(source),
        })?
        .ok_or_else(|| Error::SqsClient {
            dataset_name: dataset_name.to_string(),
            source: "no AWS credentials were resolved for SQS".into(),
        })?;
    let sqs_config = aws_sdk_sqs::config::Builder::from(sdk_config.as_ref())
        .region(aws_config::Region::new(region.to_string()))
        .build();
    Ok(aws_sdk_sqs::Client::from_conf(sqs_config))
}

fn error_stream(error: Error) -> ChangesStream {
    Box::pin(futures::stream::once(async move {
        Err(StreamError::Connector {
            connector: "S3",
            source: Box::new(error),
        })
    }))
}

pub async fn s3_changes_stream(
    connector: &S3,
    _context: &dyn ConnectorContext,
    federated_table: Arc<dyn FederatedTableProvider>,
    dataset: &DatasetSpec,
    acceleration: AccelerationContents,
) -> Option<ChangesStream> {
    let config = match S3CdcConfig::try_from_params(&connector.params, dataset) {
        Ok(Some(config)) => config,
        Ok(None) => return None,
        Err(error) => return Some(error_stream(error)),
    };
    let client = match build_sqs_client(
        &connector.params,
        &config.region,
        &dataset.name.to_string(),
    )
    .await
    {
        Ok(client) => client,
        Err(error) => return Some(error_stream(error)),
    };
    let queue = Arc::new(SqsQueue {
        client,
        queue_url: config.queue_url.clone(),
    });
    let object_reader = Arc::new(ListingObjectReader {
        connector: connector.clone(),
        dataset: dataset.clone(),
    });
    Some(stream_s3_changes(
        dataset.clone(),
        federated_table,
        acceleration,
        queue,
        object_reader,
        config,
        connector.get_session_context(),
    ))
}

#[derive(Debug)]
enum ProcessOutcome {
    Creates {
        batches: Vec<RecordBatch>,
        receipt_handle: String,
    },
    Rebuild {
        receipt_handle: String,
    },
    Ack {
        receipt_handle: String,
    },
    Retry,
}

async fn process_message(
    dataset: &DatasetSpec,
    config: &S3CdcConfig,
    object_reader: &dyn ObjectReader,
    message: &QueueMessage,
) -> ProcessOutcome {
    let receipt_handle = message.receipt_handle.clone();
    let events = match parse_notification_body(&message.body) {
        Ok(events) => events,
        Err(error) => {
            tracing::warn!(
                "Dataset '{}' dropped an SQS message that is not a valid S3 event notification, so that payload was not applied. Cause: {error}. Fix the bucket notification target or remove the poison message. See: {S3_DOCS}",
                dataset.name
            );
            return ProcessOutcome::Ack { receipt_handle };
        }
    };

    if events.is_empty() {
        return ProcessOutcome::Ack { receipt_handle };
    }

    let matching: Vec<&S3ObjectEvent> = events
        .iter()
        .filter(|event| matches_dataset(event, &config.bucket, &config.key_prefix))
        .collect();

    if matching.is_empty() {
        tracing::debug!(
            "Dataset '{}' skipped SQS S3 notifications that are outside s3://{}{}, so those messages were deleted without applying rows. Use a dedicated queue per dataset; sharing a queue across datasets is not supported. See: {S3_DOCS}",
            dataset.name,
            config.bucket,
            if config.key_prefix.is_empty() {
                String::new()
            } else {
                format!("/{}", config.key_prefix.trim_end_matches('/'))
            }
        );
        return ProcessOutcome::Ack { receipt_handle };
    }

    let removed: Vec<&&S3ObjectEvent> = matching
        .iter()
        .filter(|event| event.kind == ObjectEventKind::Removed)
        .collect();
    if !removed.is_empty() && config.events.includes_removed() {
        tracing::info!(
            "Dataset '{}' received S3 ObjectRemoved for s3://{}/{}, so the accelerator will be rebuilt from the listing prefix. See: {S3_DOCS}",
            dataset.name,
            removed[0].bucket,
            removed[0].key
        );
        return ProcessOutcome::Rebuild { receipt_handle };
    }
    for event in &removed {
        tracing::warn!(
            "Dataset '{}' ignored an S3 ObjectRemoved notification for s3://{}/{}, so queries will still return rows from that object. Set `s3_cdc_events: object_created_and_removed` to rebuild the accelerator from the listing prefix after deletes. See: {S3_DOCS}",
            dataset.name,
            event.bucket,
            event.key
        );
    }

    let created: Vec<&S3ObjectEvent> = matching
        .iter()
        .copied()
        .filter(|event| event.kind == ObjectEventKind::Created)
        .collect();
    if created.is_empty() {
        return ProcessOutcome::Ack { receipt_handle };
    }

    let mut batches = Vec::new();
    for event in created {
        match object_reader.read_object(&event.bucket, &event.key).await {
            Ok(object_batches) => batches.extend(
                object_batches
                    .into_iter()
                    .filter(|batch| batch.num_rows() > 0),
            ),
            Err(error) => {
                tracing::warn!(
                    "Dataset '{}' failed to read s3://{}/{} after an ObjectCreated notification, so the SQS message will retry. Cause: {error}. See: {S3_DOCS}",
                    dataset.name,
                    event.bucket,
                    event.key
                );
                return ProcessOutcome::Retry;
            }
        }
    }

    if batches.is_empty() {
        return ProcessOutcome::Ack { receipt_handle };
    }

    ProcessOutcome::Creates {
        batches,
        receipt_handle,
    }
}

async fn snapshot_stream(
    session: &SessionContext,
    table_provider: Arc<dyn TableProvider>,
) -> std::result::Result<SendableRecordBatchStream, StreamError> {
    let df = session
        .read_table(table_provider)
        .map_err(|error| StreamError::Arrow(error.to_string()))?;
    df.execute_stream()
        .await
        .map_err(|error| StreamError::Arrow(error.to_string()))
}

fn rebuild_envelope(
    schema: &SchemaRef,
    queue: &Arc<dyn MessageQueue>,
    receipt_handle: String,
) -> std::result::Result<ChangeEnvelope, StreamError> {
    let (_, batch, is_dataset_ready, _) =
        build_history_unavailable_envelope(schema)?.into_parts()?;
    Ok(ChangeEnvelope::from_parts(
        Box::new(SqsDeleteCommitter {
            queue: Arc::clone(queue),
            receipt_handle,
        }),
        batch,
        is_dataset_ready,
        true,
    ))
}

fn create_envelopes(
    schema: &SchemaRef,
    batches: Vec<RecordBatch>,
    queue: &Arc<dyn MessageQueue>,
    receipt_handle: String,
) -> std::result::Result<Vec<ChangeEnvelope>, StreamError> {
    let last = batches.len().saturating_sub(1);
    batches
        .into_iter()
        .enumerate()
        .map(|(i, batch)| {
            let change_batch = wrap_data_as_change_batch(schema, &batch)?;
            let committer: Box<dyn CommitChange + Send + Sync> = if i == last {
                Box::new(SqsDeleteCommitter {
                    queue: Arc::clone(queue),
                    receipt_handle: receipt_handle.clone(),
                })
            } else {
                Box::new(NoOpCommitter)
            };
            Ok(ChangeEnvelope::new(committer, change_batch, true))
        })
        .collect()
}

/// SQS long-poll change stream for one S3 listing dataset.
///
/// `try_stream!` keeps snapshot, SQS long-poll, and per-message apply in one
/// backpressured generator. A channel would buffer SQS deletes ahead of
/// accelerator commits.
#[must_use]
pub fn stream_s3_changes(
    dataset: DatasetSpec,
    federated_table: Arc<dyn FederatedTableProvider>,
    acceleration: AccelerationContents,
    queue: Arc<dyn MessageQueue>,
    object_reader: Arc<dyn ObjectReader>,
    config: S3CdcConfig,
    session: SessionContext,
) -> ChangesStream {
    Box::pin(try_stream! {
        let epoch = shutdown_epoch();
        let table_provider = federated_table.table_provider().await;
        let schema = table_provider.schema();

        if acceleration.is_provably_empty() {
            tracing::info!(
                "Dataset '{}' is starting S3 change capture from an empty accelerator, so existing objects under s3://{}/{} will be snapshotted before SQS events are applied. See: {S3_DOCS}",
                dataset.name,
                config.bucket,
                config.key_prefix
            );
            let mut snapshot = snapshot_stream(&session, Arc::clone(&table_provider)).await?;
            while let Some(batch) = snapshot.next().await {
                let batch = batch.map_err(|error| StreamError::Arrow(error.to_string()))?;
                if batch.num_rows() == 0 {
                    continue;
                }
                let change_batch = wrap_data_as_change_batch(&schema, &batch)?;
                yield ChangeEnvelope::new(Box::new(NoOpCommitter), change_batch, false);
            }
        }

        yield build_ready_signal_envelope(&schema)?;

        let mut receive_backoff = Duration::from_secs(1);
        loop {
            if shutdown_epoch() != epoch {
                break;
            }

            let messages = match queue.receive().await {
                Ok(messages) => {
                    receive_backoff = Duration::from_secs(1);
                    messages
                }
                Err(error) => {
                    tracing::warn!(
                        "Dataset '{}' failed to long-poll SQS for S3 change notifications, so new objects will not be applied until the next successful poll. Cause: {error}. Check `s3_cdc_queue_url` and SQS permissions. See: {S3_DOCS}",
                        dataset.name
                    );
                    sleep(receive_backoff).await;
                    receive_backoff = (receive_backoff * 2).min(RECEIVE_ERROR_BACKOFF_CAP);
                    continue;
                }
            };

            for message in messages {
                if shutdown_epoch() != epoch {
                    break;
                }
                match process_message(&dataset, &config, object_reader.as_ref(), &message).await {
                    ProcessOutcome::Creates { batches, receipt_handle } => {
                        match create_envelopes(&schema, batches, &queue, receipt_handle) {
                            Ok(envelopes) => {
                                for envelope in envelopes {
                                    yield envelope;
                                }
                            }
                            Err(error) => {
                                tracing::warn!(
                                    "Dataset '{}' failed to wrap S3 object rows as CDC creates, so the SQS message will retry. Cause: {error}. See: {S3_DOCS}",
                                    dataset.name
                                );
                            }
                        }
                    }
                    ProcessOutcome::Rebuild { receipt_handle } => {
                        yield rebuild_envelope(&schema, &queue, receipt_handle)?;
                    }
                    ProcessOutcome::Ack { receipt_handle } => {
                        if let Err(error) = queue.delete(&receipt_handle).await {
                            tracing::warn!(
                                "Dataset '{}' applied no rows for an SQS message but failed to delete it, so the message will retry. Cause: {error}. See: {S3_DOCS}",
                                dataset.name
                            );
                        }
                    }
                    ProcessOutcome::Retry => {}
                }
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dataconnector::s3::{PARAMETERS, PREFIX};
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use data_components::cdc::ChangeOperation;
    use datafusion::datasource::memory::MemTable;
    use runtime_component::dataset::acceleration::Acceleration;
    use runtime_secrets::Secrets;
    use std::collections::HashMap;
    use tokio::sync::{Mutex, RwLock};

    const QUEUE_URL: &str = "https://sqs.us-east-1.amazonaws.com/123456789012/s3-events";

    #[derive(Debug)]
    struct StaticFederated(Arc<dyn TableProvider>);

    #[async_trait]
    impl FederatedTableProvider for StaticFederated {
        async fn table_provider(&self) -> Arc<dyn TableProvider> {
            Arc::clone(&self.0)
        }

        fn try_table_provider_sync(&self) -> Option<Arc<dyn TableProvider>> {
            Some(Arc::clone(&self.0))
        }
    }

    struct MockQueue {
        incoming: Mutex<Vec<QueueMessage>>,
        deleted: Mutex<Vec<String>>,
    }

    impl MockQueue {
        fn with_messages(messages: Vec<QueueMessage>) -> Self {
            Self {
                incoming: Mutex::new(messages),
                deleted: Mutex::new(Vec::new()),
            }
        }
    }

    #[async_trait]
    impl MessageQueue for MockQueue {
        async fn receive(&self) -> std::result::Result<Vec<QueueMessage>, QueueError> {
            let mut incoming = self.incoming.lock().await;
            if incoming.is_empty() {
                drop(incoming);
                std::future::pending::<()>().await;
            }
            Ok(std::mem::take(&mut *incoming))
        }

        async fn delete(&self, receipt_handle: &str) -> std::result::Result<(), QueueError> {
            self.deleted.lock().await.push(receipt_handle.to_string());
            Ok(())
        }
    }

    struct MapObjectReader {
        objects: HashMap<String, Vec<RecordBatch>>,
        fail_keys: Vec<String>,
    }

    #[async_trait]
    impl ObjectReader for MapObjectReader {
        async fn read_object(
            &self,
            bucket: &str,
            key: &str,
        ) -> std::result::Result<Vec<RecordBatch>, StreamError> {
            let id = format!("{bucket}/{key}");
            if self.fail_keys.contains(&id) {
                return Err(StreamError::External(format!(
                    "simulated read failure for s3://{id}"
                )));
            }
            self.objects
                .get(&id)
                .cloned()
                .ok_or_else(|| StreamError::External(format!("no fixture for s3://{id}")))
        }
    }

    fn events_dataset() -> DatasetSpec {
        let mut spec = DatasetSpec::new("s3://my-bucket/events/", "events".into());
        spec.acceleration = Some(Acceleration {
            refresh_mode: Some(RefreshMode::Changes),
            ..Acceleration::default()
        });
        spec
    }

    fn federated_table(batch: RecordBatch) -> Arc<dyn FederatedTableProvider> {
        let schema = batch.schema();
        let table = MemTable::try_new(schema, vec![vec![batch]])
            .expect("MemTable should build from a single batch");
        Arc::new(StaticFederated(Arc::new(table)))
    }

    fn id_name_batch(ids: &[i32], names: &[&str]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(ids.to_vec())),
                Arc::new(StringArray::from(names.to_vec())),
            ],
        )
        .expect("id/name batch should build")
    }

    fn created_put_body(key: &str) -> String {
        format!(
            r#"{{"Records":[{{"eventName":"ObjectCreated:Put","s3":{{"bucket":{{"name":"my-bucket"}},"object":{{"key":"{key}"}}}}}}]}}"#
        )
    }

    fn removed_body(key: &str) -> String {
        format!(
            r#"{{"Records":[{{"eventName":"ObjectRemoved:Delete","s3":{{"bucket":{{"name":"my-bucket"}},"object":{{"key":"{key}"}}}}}}]}}"#
        )
    }

    async fn test_params(pairs: Vec<(&str, &str)>) -> Parameters {
        let params = pairs
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.to_string().into()))
            .collect();
        Parameters::try_new(
            "s3_cdc_test",
            params,
            PREFIX,
            Arc::new(RwLock::new(Secrets::new())),
            PARAMETERS.as_ref(),
        )
        .await
        .expect("valid S3 CDC test parameters")
    }

    fn default_config() -> S3CdcConfig {
        S3CdcConfig {
            queue_url: QUEUE_URL.to_string(),
            region: "us-east-1".to_string(),
            events: CdcEvents::ObjectCreated,
            bucket: "my-bucket".to_string(),
            key_prefix: "events/".to_string(),
        }
    }

    async fn collect_until_idle(stream: ChangesStream, expected: usize) -> Vec<ChangeEnvelope> {
        let mut stream = stream;
        let mut envelopes = Vec::new();
        while envelopes.len() < expected {
            match tokio::time::timeout(Duration::from_secs(2), stream.next()).await {
                Ok(Some(Ok(envelope))) => envelopes.push(envelope),
                Ok(Some(Err(error))) => panic!("change stream error: {error}"),
                Ok(None) => break,
                Err(_) => break,
            }
        }
        envelopes
    }

    #[test]
    fn region_from_standard_and_china_queue_urls() {
        assert_eq!(
            region_from_queue_url(QUEUE_URL).as_deref(),
            Some("us-east-1")
        );
        assert_eq!(
            region_from_queue_url("https://sqs.cn-north-1.amazonaws.com.cn/123/queue").as_deref(),
            Some("cn-north-1")
        );
        assert_eq!(
            region_from_queue_url("https://localhost:4566/000000000000/queue"),
            None
        );
    }

    #[tokio::test]
    async fn validate_accepts_queue_url_with_changes() {
        let params = test_params(vec![
            ("s3_cdc_queue_url", QUEUE_URL),
            ("s3_auth", "iam_role"),
        ])
        .await;
        let config = S3CdcConfig::try_from_params(&params, &events_dataset())
            .expect("valid CDC config")
            .expect("CDC should be enabled");
        assert_eq!(config.bucket, "my-bucket");
        assert_eq!(config.key_prefix, "events/");
        assert_eq!(config.region, "us-east-1");
        assert_eq!(config.events, CdcEvents::ObjectCreated);
    }

    #[tokio::test]
    async fn validate_skips_when_cdc_is_not_configured() {
        let params = test_params(vec![]).await;
        let mut dataset = DatasetSpec::new("s3://my-bucket/events/", "events".into());
        dataset.acceleration = Some(Acceleration::default());
        assert_eq!(
            S3CdcConfig::try_from_params(&params, &dataset).expect("no CDC"),
            None
        );
    }

    #[tokio::test]
    async fn validate_fails_closed_without_queue_on_changes() {
        let params = test_params(vec![]).await;
        let error = S3CdcConfig::try_from_params(&params, &events_dataset())
            .expect_err("changes requires a queue URL");
        assert!(error.to_string().contains("s3_cdc_queue_url"));
    }

    #[tokio::test]
    async fn validate_fails_closed_on_queue_without_changes() {
        let params = test_params(vec![("s3_cdc_queue_url", QUEUE_URL)]).await;
        let dataset = DatasetSpec::new("s3://my-bucket/events/", "events".into());
        let error = S3CdcConfig::try_from_params(&params, &dataset)
            .expect_err("queue without changes is refused");
        assert!(error.to_string().contains("refresh_mode"));
    }

    #[tokio::test]
    async fn validate_rejects_queue_arn() {
        let params = test_params(vec![(
            "s3_cdc_queue_url",
            "arn:aws:sqs:us-east-1:123456789012:s3-events",
        )])
        .await;
        let error = S3CdcConfig::try_from_params(&params, &events_dataset())
            .expect_err("ARN must be refused");
        assert!(error.to_string().contains("not an ARN"));
    }

    #[tokio::test]
    async fn validate_rejects_public_auth() {
        let params =
            test_params(vec![("s3_cdc_queue_url", QUEUE_URL), ("s3_auth", "public")]).await;
        let error = S3CdcConfig::try_from_params(&params, &events_dataset())
            .expect_err("public auth cannot consume SQS");
        assert!(error.to_string().contains("public"));
    }

    #[tokio::test]
    async fn validate_rejects_invalid_events_and_outside_prefix() {
        let params = test_params(vec![
            ("s3_cdc_queue_url", QUEUE_URL),
            ("s3_cdc_events", "everything"),
        ])
        .await;
        let error = S3CdcConfig::try_from_params(&params, &events_dataset())
            .expect_err("invalid events value");
        assert!(error.to_string().contains("object_created"));

        let params = test_params(vec![
            ("s3_cdc_queue_url", QUEUE_URL),
            ("s3_cdc_key_prefix", "other/"),
        ])
        .await;
        let error = S3CdcConfig::try_from_params(&params, &events_dataset())
            .expect_err("prefix outside dataset");
        assert!(error.to_string().contains("s3_cdc_key_prefix"));
    }

    #[tokio::test]
    async fn validate_nested_prefix_and_events_enum() {
        let params = test_params(vec![
            ("s3_cdc_queue_url", QUEUE_URL),
            ("s3_cdc_key_prefix", "events/year=2026"),
            ("s3_cdc_events", "object_created_and_removed"),
        ])
        .await;
        let config = S3CdcConfig::try_from_params(&params, &events_dataset())
            .expect("nested prefix is valid")
            .expect("CDC enabled");
        assert_eq!(config.key_prefix, "events/year=2026/");
        assert_eq!(config.events, CdcEvents::ObjectCreatedAndRemoved);
    }

    #[tokio::test]
    async fn process_created_reads_object() {
        let batch = id_name_batch(&[1], &["a"]);
        let reader = MapObjectReader {
            objects: HashMap::from([("my-bucket/events/a.parquet".to_string(), vec![batch])]),
            fail_keys: vec![],
        };
        let outcome = process_message(
            &events_dataset(),
            &default_config(),
            &reader,
            &QueueMessage {
                body: created_put_body("events/a.parquet"),
                receipt_handle: "rh-1".into(),
            },
        )
        .await;
        match outcome {
            ProcessOutcome::Creates {
                batches,
                receipt_handle,
            } => {
                assert_eq!(batches.len(), 1);
                assert_eq!(batches[0].num_rows(), 1);
                assert_eq!(receipt_handle, "rh-1");
            }
            other => panic!("expected Creates, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn process_removed_is_acked_by_default_and_rebuilds_when_enabled() {
        let reader = MapObjectReader {
            objects: HashMap::new(),
            fail_keys: vec![],
        };
        let outcome = process_message(
            &events_dataset(),
            &default_config(),
            &reader,
            &QueueMessage {
                body: removed_body("events/a.parquet"),
                receipt_handle: "rh-del".into(),
            },
        )
        .await;
        assert!(matches!(outcome, ProcessOutcome::Ack { .. }));

        let mut config = default_config();
        config.events = CdcEvents::ObjectCreatedAndRemoved;
        let outcome = process_message(
            &events_dataset(),
            &config,
            &reader,
            &QueueMessage {
                body: removed_body("events/a.parquet"),
                receipt_handle: "rh-del".into(),
            },
        )
        .await;
        assert!(matches!(outcome, ProcessOutcome::Rebuild { .. }));
    }

    #[tokio::test]
    async fn process_unmatched_prefix_and_poison_are_acked() {
        let reader = MapObjectReader {
            objects: HashMap::new(),
            fail_keys: vec![],
        };
        let unmatched = process_message(
            &events_dataset(),
            &default_config(),
            &reader,
            &QueueMessage {
                body: created_put_body("other/a.parquet"),
                receipt_handle: "rh-other".into(),
            },
        )
        .await;
        assert!(matches!(unmatched, ProcessOutcome::Ack { .. }));

        let poison = process_message(
            &events_dataset(),
            &default_config(),
            &reader,
            &QueueMessage {
                body: "not-json".into(),
                receipt_handle: "rh-poison".into(),
            },
        )
        .await;
        assert!(matches!(poison, ProcessOutcome::Ack { .. }));
    }

    #[tokio::test]
    async fn process_read_failure_retries() {
        let reader = MapObjectReader {
            objects: HashMap::new(),
            fail_keys: vec!["my-bucket/events/a.parquet".into()],
        };
        let outcome = process_message(
            &events_dataset(),
            &default_config(),
            &reader,
            &QueueMessage {
                body: created_put_body("events/a.parquet"),
                receipt_handle: "rh-fail".into(),
            },
        )
        .await;
        assert!(matches!(outcome, ProcessOutcome::Retry));
    }

    #[tokio::test]
    async fn stream_object_created_yields_create_and_commit_deletes_sqs_message() {
        let object_batch = id_name_batch(&[7], &["created"]);
        let snapshot_batch = id_name_batch(&[1], &["snap"]);
        let queue = Arc::new(MockQueue::with_messages(vec![QueueMessage {
            body: created_put_body("events/new.parquet"),
            receipt_handle: "rh-new".into(),
        }]));
        let reader = Arc::new(MapObjectReader {
            objects: HashMap::from([(
                "my-bucket/events/new.parquet".to_string(),
                vec![object_batch],
            )]),
            fail_keys: vec![],
        });
        let stream = stream_s3_changes(
            events_dataset(),
            federated_table(snapshot_batch),
            AccelerationContents::NonEmpty,
            Arc::clone(&queue) as Arc<dyn MessageQueue>,
            reader,
            default_config(),
            SessionContext::new(),
        );
        let mut envelopes = collect_until_idle(stream, 2).await;
        assert!(
            envelopes.len() >= 2,
            "expected ready + create, got {}",
            envelopes.len()
        );
        assert!(envelopes[0].is_dataset_ready());
        assert!(envelopes[0].is_empty());

        let create = envelopes.remove(1);
        assert!(!create.is_empty());
        assert!(matches!(
            create.change_batch().expect("create batch").op(0),
            ChangeOperation::Create
        ));
        create
            .commit()
            .await
            .expect("SQS delete should succeed after the create is applied");
        assert_eq!(*queue.deleted.lock().await, vec!["rh-new".to_string()]);
    }

    #[tokio::test]
    async fn stream_object_removed_rebuilds_when_configured() {
        let queue = Arc::new(MockQueue::with_messages(vec![QueueMessage {
            body: removed_body("events/gone.parquet"),
            receipt_handle: "rh-gone".into(),
        }]));
        let reader = Arc::new(MapObjectReader {
            objects: HashMap::new(),
            fail_keys: vec![],
        });
        let mut config = default_config();
        config.events = CdcEvents::ObjectCreatedAndRemoved;
        let stream = stream_s3_changes(
            events_dataset(),
            federated_table(id_name_batch(&[1], &["snap"])),
            AccelerationContents::NonEmpty,
            Arc::clone(&queue) as Arc<dyn MessageQueue>,
            reader,
            config,
            SessionContext::new(),
        );
        let mut envelopes = collect_until_idle(stream, 2).await;
        assert!(
            envelopes.len() >= 2,
            "expected ready + rebuild, got {}",
            envelopes.len()
        );
        let rebuild = envelopes.remove(1);
        assert!(rebuild.history_unavailable());
        rebuild
            .commit()
            .await
            .expect("SQS delete should succeed after the rebuild signal");
        assert_eq!(*queue.deleted.lock().await, vec!["rh-gone".to_string()]);
    }

    #[tokio::test]
    async fn stream_empty_accelerator_snapshots_then_ready() {
        let queue = Arc::new(MockQueue::with_messages(vec![]));
        let reader = Arc::new(MapObjectReader {
            objects: HashMap::new(),
            fail_keys: vec![],
        });
        let stream = stream_s3_changes(
            events_dataset(),
            federated_table(id_name_batch(&[1], &["snap"])),
            AccelerationContents::Empty,
            queue,
            reader,
            default_config(),
            SessionContext::new(),
        );
        let envelopes = collect_until_idle(stream, 2).await;
        assert_eq!(
            envelopes.len(),
            2,
            "expected snapshot create + ready, got {}",
            envelopes.len()
        );
        assert!(!envelopes[0].is_dataset_ready());
        assert!(matches!(
            envelopes[0].change_batch().expect("snapshot batch").op(0),
            ChangeOperation::Create
        ));
        assert!(envelopes[1].is_dataset_ready());
        assert!(envelopes[1].is_empty());
    }
}
