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

//! S3 event notifications → SQS → listing backfill → `ChangesStream`.
//!
//! `try_stream!` keeps the snapshot, long-poll, backfill, and per-message apply
//! path in one backpressured generator (the same shape as MongoDB / Kafka
//! change streams). A manual `Stream` impl would split that state machine
//! across poll/yield points without changing behavior.

use super::event::{
    ObjectEventKind, S3ObjectEvent, matches_dataset, parse_notification_body, s3_object_from,
};
use super::{S3, S3_DOCS};
use crate::dataconnector::federated::FederatedTableProvider;
use crate::dataconnector::listing::ListingTableConnector;
use crate::dataconnector::parameters::ConnectorContext;
use crate::dataconnector::{ConnectorComponent, DataConnectorError, DataConnectorResult};
use arrow::array::{ArrayRef, RecordBatch, StringArray, new_null_array};
use arrow::datatypes::{DataType, SchemaRef};
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
use object_store::ObjectStore;
use object_store::path::Path as ObjectPath;
use runtime_component::dataset::DatasetSpec;
use runtime_component::dataset::acceleration::RefreshMode;
use runtime_parameters::Parameters;
use snafu::prelude::*;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time::sleep;

const SQS_LONG_POLL_SECONDS: i32 = 20;
const SQS_MAX_MESSAGES: i32 = 10;
const SQS_VISIBILITY_TIMEOUT_SECONDS: i32 = 300;
const RECEIVE_ERROR_BACKOFF_CAP: Duration = Duration::from_secs(30);
const DEFAULT_BACKFILL_INTERVAL: Duration = Duration::from_secs(60 * 60);

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Failed to register dataset {dataset_name} (s3): `refresh_mode: changes` requires `s3_changes_queue_url` set to an SQS queue subscribed to S3 event notifications. Set `s3_changes_queue_url` to the queue URL (https://sqs.<region>.amazonaws.com/...), not an ARN. See: {S3_DOCS}"
    ))]
    MissingQueueUrl { dataset_name: String },

    #[snafu(display(
        "Failed to register dataset {dataset_name} (s3): `s3_changes_queue_url` is set, but `acceleration.refresh_mode` is not `changes`, so the queue would never be consumed. Set `refresh_mode: changes` or remove `s3_changes_queue_url`. See: {S3_DOCS}"
    ))]
    QueueWithoutChanges { dataset_name: String },

    #[snafu(display(
        "Failed to register dataset {dataset_name} (s3): `s3_changes_queue_url` must be an SQS queue URL (https://sqs.<region>.amazonaws.com/...), not an ARN. See: {S3_DOCS}"
    ))]
    QueueUrlIsArn { dataset_name: String },

    #[snafu(display(
        "Failed to register dataset {dataset_name} (s3): `s3_changes_queue_url` is empty. Set it to the SQS queue URL that receives S3 event notifications. See: {S3_DOCS}"
    ))]
    EmptyQueueUrl { dataset_name: String },

    #[snafu(display(
        "Failed to register dataset {dataset_name} (s3): `s3_on_object_removed` value '{value}' is not supported. Use `ignore` (leave deleted objects in the accelerator) or `rebuild` (replace the accelerator from the listing prefix; this is not a row-level delete). See: {S3_DOCS}"
    ))]
    InvalidOnObjectRemoved { dataset_name: String, value: String },

    #[snafu(display(
        "Failed to register dataset {dataset_name} (s3): `s3_changes_key_prefix` '{configured}' is not under the dataset path prefix '{dataset_prefix}'. Use a prefix equal to or nested under the `from` path. See: {S3_DOCS}"
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
        "Failed to register dataset {dataset_name} (s3): no AWS region for the SQS queue. Set `s3_changes_region` or `s3_region`, or use a queue URL that includes the region (https://sqs.<region>.amazonaws.com/...). See: {S3_DOCS}"
    ))]
    MissingRegion { dataset_name: String },

    #[snafu(display(
        "Failed to register dataset {dataset_name} (s3): dataset `from` '{from}' does not include an S3 bucket. Use `s3://bucket/prefix`. See: {S3_DOCS}"
    ))]
    MissingBucket { dataset_name: String, from: String },

    #[snafu(display(
        "Failed to register dataset {dataset_name} (s3): `s3_changes_backfill_interval` value '{value}' is not a duration greater than 0. Use a value like `1h` or `30m`. See: {S3_DOCS}"
    ))]
    InvalidBackfillInterval { dataset_name: String, value: String },

    #[snafu(display(
        "Failed to create an SQS client for dataset {dataset_name} (s3): {source}. Check AWS credentials and `s3_changes_region`. See: {S3_DOCS}"
    ))]
    SqsClient {
        dataset_name: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// What to do with `s3:ObjectRemoved:*` notifications.
///
/// `Rebuild` is a full listing-prefix replacement (`history_unavailable`), not a
/// row-level CDC delete of the object's contents.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OnObjectRemoved {
    Ignore,
    Rebuild,
}

impl OnObjectRemoved {
    fn parse(value: &str) -> Option<Self> {
        match value {
            "ignore" => Some(Self::Ignore),
            "rebuild" => Some(Self::Rebuild),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct S3ChangesConfig {
    pub queue_url: String,
    pub region: String,
    pub on_object_removed: OnObjectRemoved,
    pub bucket: String,
    pub key_prefix: String,
    pub backfill_interval: Duration,
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
    #[snafu(display("Failed to receive messages from the configured SQS queue: {source}"))]
    Receive {
        source: Box<dyn std::error::Error + Send + Sync>,
    },
    #[snafu(display("Failed to delete an SQS message from the configured SQS queue: {source}"))]
    Delete {
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

#[async_trait]
pub trait ObjectLister: Send + Sync {
    async fn list_keys(&self) -> std::result::Result<Vec<String>, StreamError>;
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
                "S3 changes cannot build an object URL for s3://{bucket}/{key}: {error}"
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
                "S3 changes cannot read unstructured text object s3://{bucket}/{key} for dataset '{}'. Set `file_format` to parquet, csv, or json. See: {S3_DOCS}",
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

/// `create_listing_table` infers Hive partition columns only when the path is a
/// collection. An exact-object URL is not, so the file scan omits `key=value`
/// columns that the federated dataset schema still has. Reconstruct those
/// Utf8 constants from the object key so `wrap_data_as_change_batch` can apply.
fn align_object_batch(
    table_schema: &SchemaRef,
    object_key: &str,
    batch: &RecordBatch,
) -> std::result::Result<RecordBatch, StreamError> {
    if batch.schema().as_ref() == table_schema.as_ref() {
        return Ok(batch.clone());
    }

    let hive = hive_partition_values(object_key);
    let num_rows = batch.num_rows();
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(table_schema.fields().len());
    for field in table_schema.fields() {
        if let Ok(index) = batch.schema().index_of(field.name()) {
            let column = batch.column(index);
            if column.data_type() != field.data_type() {
                return Err(StreamError::External(format!(
                    "S3 changes cannot apply object '{object_key}': column '{}' has type {} but the dataset schema is {}. See: {S3_DOCS}",
                    field.name(),
                    column.data_type(),
                    field.data_type()
                )));
            }
            columns.push(Arc::clone(column));
            continue;
        }
        if let Some(value) = hive.get(field.name()) {
            columns.push(constant_partition_array(field, value, num_rows)?);
            continue;
        }
        if field.is_nullable() {
            columns.push(new_null_array(field.data_type(), num_rows));
            continue;
        }
        return Err(StreamError::External(format!(
            "S3 changes cannot apply object '{object_key}': required column '{}' is missing from the object and is not a Hive `key=value` path segment. See: {S3_DOCS}",
            field.name()
        )));
    }
    RecordBatch::try_new(Arc::clone(table_schema), columns)
        .map_err(|error| StreamError::Arrow(error.to_string()))
}

fn hive_partition_values(object_key: &str) -> std::collections::HashMap<String, String> {
    object_key
        .split('/')
        .filter_map(|segment| {
            let (name, value) = segment.split_once('=')?;
            if name.is_empty() || value.is_empty() {
                None
            } else {
                Some((name.to_string(), value.to_string()))
            }
        })
        .collect()
}

fn constant_partition_array(
    field: &arrow::datatypes::Field,
    value: &str,
    num_rows: usize,
) -> std::result::Result<ArrayRef, StreamError> {
    match field.data_type() {
        DataType::Utf8 => Ok(Arc::new(StringArray::from(vec![
            value.to_string();
            num_rows
        ]))),
        other => Err(StreamError::External(format!(
            "S3 changes cannot reconstruct Hive partition column '{}' as {other} from object key '{value}'. Listing Hive partitions are Utf8. See: {S3_DOCS}",
            field.name()
        ))),
    }
}

fn align_object_batches(
    table_schema: &SchemaRef,
    object_key: &str,
    batches: Vec<RecordBatch>,
) -> std::result::Result<Vec<RecordBatch>, StreamError> {
    batches
        .into_iter()
        .filter(|batch| batch.num_rows() > 0)
        .map(|batch| align_object_batch(table_schema, object_key, &batch))
        .collect()
}

struct ListingPrefixScanner {
    connector: S3,
    dataset: DatasetSpec,
    key_prefix: String,
}

#[async_trait]
impl ObjectLister for ListingPrefixScanner {
    async fn list_keys(&self) -> std::result::Result<Vec<String>, StreamError> {
        let store = self
            .connector
            .get_object_store(&self.dataset)
            .map_err(|e| StreamError::Connector {
                connector: "S3",
                source: Box::new(e),
            })?;
        let prefix = if self.key_prefix.is_empty() {
            None
        } else {
            Some(ObjectPath::from(self.key_prefix.as_str()))
        };
        let mut listing = store.list(prefix.as_ref());
        let mut keys = Vec::new();
        while let Some(meta) = listing.next().await {
            let meta = meta.map_err(|error| StreamError::External(error.to_string()))?;
            let key = meta.location.as_ref().trim_start_matches('/').to_string();
            if key.is_empty() || key.ends_with('/') {
                continue;
            }
            keys.push(key);
        }
        Ok(keys)
    }
}

/// Fail closed on S3 changes misconfiguration before the listing table is built.
///
/// # Errors
///
/// Returns [`DataConnectorError::InvalidConfigurationNoSource`] when
/// `refresh_mode: changes` is missing `s3_changes_queue_url` (or the reverse),
/// the queue value is an ARN, `s3_auth` is `public`, `s3_on_object_removed` is
/// unknown, `s3_changes_key_prefix` is outside the dataset path,
/// `s3_changes_backfill_interval` is not a positive duration, or no SQS region
/// can be resolved.
pub fn validate_s3_changes_config(
    params: &Parameters,
    dataset: &DatasetSpec,
) -> DataConnectorResult<()> {
    match S3ChangesConfig::try_from_params(params, dataset) {
        Ok(_) => Ok(()),
        Err(error) => Err(DataConnectorError::InvalidConfigurationNoSource {
            dataconnector: "s3".to_string(),
            connector_component: ConnectorComponent::from(dataset),
            message: error.to_string(),
        }),
    }
}

impl S3ChangesConfig {
    /// `Ok(None)` when changes-mode SQS is not configured (no queue, not `changes`).
    fn try_from_params(params: &Parameters, dataset: &DatasetSpec) -> Result<Option<Self>> {
        let dataset_name = dataset.name.to_string();
        let queue_raw = params.get("changes_queue_url").expose().ok();
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

                let on_object_removed = match params.get("on_object_removed").expose().ok() {
                    None => OnObjectRemoved::Ignore,
                    Some(value) => {
                        OnObjectRemoved::parse(value).context(InvalidOnObjectRemovedSnafu {
                            dataset_name: dataset_name.clone(),
                            value,
                        })?
                    }
                };

                let (bucket, dataset_prefix) = bucket_and_key_prefix(dataset)?;
                let key_prefix = match params.get("changes_key_prefix").expose().ok() {
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

                let backfill_interval = parse_backfill_interval(params, &dataset_name)?;

                let region =
                    resolve_region(params, url).context(MissingRegionSnafu { dataset_name })?;

                Ok(Some(Self {
                    queue_url: url.to_string(),
                    region,
                    on_object_removed,
                    bucket,
                    key_prefix,
                    backfill_interval,
                }))
            }
        }
    }
}

fn parse_backfill_interval(params: &Parameters, dataset_name: &str) -> Result<Duration> {
    let Some(raw) = params.get("changes_backfill_interval").expose().ok() else {
        return Ok(DEFAULT_BACKFILL_INTERVAL);
    };
    let parsed = fundu::parse_duration(raw)
        .ok()
        .filter(|d| *d > Duration::ZERO);
    parsed.context(InvalidBackfillIntervalSnafu {
        dataset_name,
        value: raw.to_string(),
    })
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
        .get("changes_region")
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

fn replace_applied_keys(applied_keys: &mut HashSet<String>, listed: Vec<String>) {
    *applied_keys = listed.into_iter().collect();
}

fn prefix_display(bucket: &str, key_prefix: &str) -> String {
    if key_prefix.is_empty() {
        format!("s3://{bucket}")
    } else {
        format!("s3://{bucket}/{}", key_prefix.trim_end_matches('/'))
    }
}

/// How SQS credentials are selected — the same rules as the S3 object store
/// (`determine_s3_credential_config`): explicit `s3_key`/`s3_secret` win even
/// without `s3_auth: key`; `s3_iam_role_source: metadata|env` restricts the chain.
#[derive(Debug, Clone, PartialEq, Eq)]
enum SqsAuth {
    ExplicitKeys,
    RestrictedIam { source: String },
    DefaultChain,
}

fn sqs_auth_from_params(params: &Parameters, dataset_name: &str) -> Result<SqsAuth> {
    let key = params.get("key").expose().ok();
    let secret = params.get("secret").expose().ok();
    let auth = params.get("auth").expose().ok();
    let iam_role_source = params.get("iam_role_source").expose().ok();
    let cred_config = aws_sdk_credential_bridge::determine_s3_credential_config(
        key,
        secret,
        auth,
        iam_role_source,
    )
    .map_err(|message| Error::SqsClient {
        dataset_name: dataset_name.to_string(),
        source: message.into(),
    })?;

    if cred_config.skip_signature {
        return PublicAuthCannotConsumeSqsSnafu {
            dataset_name: dataset_name.to_string(),
        }
        .fail();
    }

    if matches!(auth, Some("key")) && (key.is_none() || secret.is_none()) {
        let missing = if key.is_none() { "s3_key" } else { "s3_secret" };
        return Err(Error::SqsClient {
            dataset_name: dataset_name.to_string(),
            source: format!("s3_auth is `key` but `{missing}` is not set").into(),
        });
    }

    if key.is_some() && secret.is_some() {
        return Ok(SqsAuth::ExplicitKeys);
    }

    match cred_config.iam_role_source.as_deref() {
        Some(source @ ("metadata" | "env")) => Ok(SqsAuth::RestrictedIam {
            source: source.to_string(),
        }),
        _ => Ok(SqsAuth::DefaultChain),
    }
}

async fn build_sqs_client(
    params: &Parameters,
    region: &str,
    dataset_name: &str,
) -> Result<aws_sdk_sqs::Client> {
    match sqs_auth_from_params(params, dataset_name)? {
        SqsAuth::ExplicitKeys => {
            let access_key = params
                .get("key")
                .expose()
                .ok()
                .ok_or_else(|| Error::SqsClient {
                    dataset_name: dataset_name.to_string(),
                    source: "explicit S3 key credentials were selected but `s3_key` is not set"
                        .into(),
                })?;
            let secret_key =
                params
                    .get("secret")
                    .expose()
                    .ok()
                    .ok_or_else(|| Error::SqsClient {
                        dataset_name: dataset_name.to_string(),
                        source:
                            "explicit S3 key credentials were selected but `s3_secret` is not set"
                                .into(),
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
                "spice-s3-changes",
            );
            let sdk_config = aws_sdk_credential_bridge::default_aws_config()
                .region(aws_config::Region::new(region.to_string()))
                .credentials_provider(credentials)
                .load()
                .await;
            Ok(aws_sdk_sqs::Client::new(&sdk_config))
        }
        SqsAuth::RestrictedIam { source } => {
            let sdk_config = aws_sdk_credential_bridge::build_restricted_sdk_config(
                &source,
                Some(region.to_string()),
            )
            .await
            .map_err(|error| Error::SqsClient {
                dataset_name: dataset_name.to_string(),
                source: Box::new(error),
            })?;
            Ok(aws_sdk_sqs::Client::new(&sdk_config))
        }
        SqsAuth::DefaultChain => {
            let sdk_config =
                aws_sdk_credential_bridge::get_or_init_sdk_config_with_region(Some(region))
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
    }
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
    let config = match S3ChangesConfig::try_from_params(&connector.params, dataset) {
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
    let object_lister = Arc::new(ListingPrefixScanner {
        connector: connector.clone(),
        dataset: dataset.clone(),
        key_prefix: config.key_prefix.clone(),
    });
    Some(stream_s3_changes(S3ChangesStreamParts {
        dataset: dataset.clone(),
        federated_table,
        acceleration,
        queue,
        object_reader,
        object_lister,
        config,
        session: connector.get_session_context(),
    }))
}

struct S3ChangesStreamParts {
    dataset: DatasetSpec,
    federated_table: Arc<dyn FederatedTableProvider>,
    acceleration: AccelerationContents,
    queue: Arc<dyn MessageQueue>,
    object_reader: Arc<dyn ObjectReader>,
    object_lister: Arc<dyn ObjectLister>,
    config: S3ChangesConfig,
    session: SessionContext,
}

#[derive(Debug)]
enum ProcessOutcome {
    Creates {
        batches: Vec<RecordBatch>,
        keys: Vec<String>,
        receipt_handle: String,
    },
    Rebuild {
        receipt_handle: String,
    },
    Ack {
        receipt_handle: String,
    },
    Leave,
    Retry,
}

async fn process_message(
    dataset: &DatasetSpec,
    config: &S3ChangesConfig,
    table_schema: &SchemaRef,
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

    if matching.len() != events.len() {
        let sample = events
            .iter()
            .find(|event| !matches_dataset(event, &config.bucket, &config.key_prefix))
            .unwrap_or(&events[0]);
        tracing::error!(
            "Dataset '{}' received an S3 notification for s3://{}/{} that is outside this dataset's prefix {}, so the entire SQS message was left on the queue (not deleted) and will retry until visibility timeout. The queue must be exclusive to this dataset — fan out with SNS to a per-dataset queue, or set a bucket notification prefix filter. Sharing one queue across datasets is not supported. See: {S3_DOCS}",
            dataset.name,
            sample.bucket,
            sample.key,
            prefix_display(&config.bucket, &config.key_prefix)
        );
        return ProcessOutcome::Leave;
    }

    let removed: Vec<&&S3ObjectEvent> = matching
        .iter()
        .filter(|event| event.kind == ObjectEventKind::Removed)
        .collect();
    if !removed.is_empty() && config.on_object_removed == OnObjectRemoved::Rebuild {
        tracing::info!(
            "Dataset '{}' received S3 ObjectRemoved for s3://{}/{}, so the accelerator will be rebuilt from the listing prefix (`s3_on_object_removed: rebuild` is not a row-level delete). See: {S3_DOCS}",
            dataset.name,
            removed[0].bucket,
            removed[0].key
        );
        return ProcessOutcome::Rebuild { receipt_handle };
    }
    for event in &removed {
        tracing::warn!(
            "Dataset '{}' ignored an S3 ObjectRemoved notification for s3://{}/{}, so queries will still return rows from that object. Set `s3_on_object_removed: rebuild` to replace the accelerator from the listing prefix (not a row-level delete). See: {S3_DOCS}",
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
    let mut keys = Vec::new();
    for event in created {
        match object_reader.read_object(&event.bucket, &event.key).await {
            Ok(object_batches) => {
                match align_object_batches(table_schema, &event.key, object_batches) {
                    Ok(aligned) => {
                        keys.push(event.key.clone());
                        batches.extend(aligned);
                    }
                    Err(error) => {
                        tracing::warn!(
                            "Dataset '{}' failed to align s3://{}/{} to the dataset schema after an ObjectCreated notification, so the SQS message will retry. Cause: {error}. See: {S3_DOCS}",
                            dataset.name,
                            event.bucket,
                            event.key
                        );
                        return ProcessOutcome::Retry;
                    }
                }
            }
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
        keys,
        receipt_handle,
    }
}

struct BackfillCreates {
    batches: Vec<RecordBatch>,
    keys: Vec<String>,
}

async fn backfill_unapplied(
    dataset: &DatasetSpec,
    config: &S3ChangesConfig,
    table_schema: &SchemaRef,
    object_lister: &dyn ObjectLister,
    object_reader: &dyn ObjectReader,
    applied_keys: &HashSet<String>,
) -> std::result::Result<BackfillCreates, StreamError> {
    let listed = object_lister.list_keys().await?;
    let mut batches = Vec::new();
    let mut keys = Vec::new();
    for key in listed {
        let event = S3ObjectEvent {
            event_name: "listing-backfill".into(),
            kind: ObjectEventKind::Created,
            bucket: config.bucket.clone(),
            key: key.clone(),
        };
        if !matches_dataset(&event, &config.bucket, &config.key_prefix) {
            continue;
        }
        if applied_keys.contains(&key) {
            continue;
        }
        match object_reader.read_object(&config.bucket, &key).await {
            Ok(object_batches) => match align_object_batches(table_schema, &key, object_batches) {
                Ok(aligned) => {
                    if aligned.is_empty() {
                        keys.push(key);
                        continue;
                    }
                    batches.extend(aligned);
                    keys.push(key);
                }
                Err(error) => {
                    tracing::warn!(
                        "Dataset '{}' failed to align s3://{}/{} to the dataset schema during a listing backfill, so that object will be retried on the next `s3_changes_backfill_interval`. Cause: {error}. See: {S3_DOCS}",
                        dataset.name,
                        config.bucket,
                        key
                    );
                }
            },
            Err(error) => {
                tracing::warn!(
                    "Dataset '{}' failed to read s3://{}/{} during a listing backfill, so that object will be retried on the next `s3_changes_backfill_interval`. Cause: {error}. See: {S3_DOCS}",
                    dataset.name,
                    config.bucket,
                    key
                );
            }
        }
    }
    Ok(BackfillCreates { batches, keys })
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

fn backfill_envelopes(
    schema: &SchemaRef,
    batches: Vec<RecordBatch>,
    is_dataset_ready: bool,
) -> std::result::Result<Vec<ChangeEnvelope>, StreamError> {
    batches
        .into_iter()
        .map(|batch| {
            let change_batch = wrap_data_as_change_batch(schema, &batch)?;
            Ok(ChangeEnvelope::new(
                Box::new(NoOpCommitter),
                change_batch,
                is_dataset_ready,
            ))
        })
        .collect()
}

/// SQS long-poll change stream for one S3 listing dataset, with a periodic
/// listing backfill so objects missed while the runtime was down still apply.
#[must_use]
fn stream_s3_changes(parts: S3ChangesStreamParts) -> ChangesStream {
    Box::pin(try_stream! {
        let S3ChangesStreamParts {
            dataset,
            federated_table,
            acceleration,
            queue,
            object_reader,
            object_lister,
            config,
            session,
        } = parts;
        let epoch = shutdown_epoch();
        let table_provider = federated_table.table_provider().await;
        let schema = table_provider.schema();
        let mut applied_keys: HashSet<String> = HashSet::new();

        if acceleration.is_provably_empty() {
            tracing::info!(
                "Dataset '{}' is starting S3 event capture from an empty accelerator, so existing objects under {} will be snapshotted before SQS events are applied. See: {S3_DOCS}",
                dataset.name,
                prefix_display(&config.bucket, &config.key_prefix)
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
            match object_lister.list_keys().await {
                Ok(keys) => applied_keys.extend(keys),
                Err(error) => {
                    tracing::warn!(
                        "Dataset '{}' snapshotted the listing prefix but could not record object keys for backfill skip, so the next listing backfill may re-apply those objects. Cause: {error}. See: {S3_DOCS}",
                        dataset.name
                    );
                }
            }
        } else {
            tracing::info!(
                "Dataset '{}' is starting S3 event capture with a non-empty accelerator, so the accelerator will be replaced from the listing prefix {} before SQS events are applied. An in-memory applied-key set cannot prove which objects are already present after a restart. See: {S3_DOCS}",
                dataset.name,
                prefix_display(&config.bucket, &config.key_prefix)
            );
            yield build_history_unavailable_envelope(&schema)?;
            match object_lister.list_keys().await {
                Ok(keys) => applied_keys.extend(keys),
                Err(error) => {
                    tracing::warn!(
                        "Dataset '{}' will replace the accelerator from the listing prefix but could not record object keys for backfill skip, so the next listing backfill may re-apply those objects. Cause: {error}. See: {S3_DOCS}",
                        dataset.name
                    );
                }
            }
        }

        yield build_ready_signal_envelope(&schema)?;

        let mut receive_backoff = Duration::from_secs(1);
        let mut last_backfill = Instant::now();
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
                        "Dataset '{}' failed to long-poll SQS for S3 event notifications, so new objects will not be applied until the next successful poll. Cause: {error}. Check `s3_changes_queue_url` and SQS permissions. See: {S3_DOCS}",
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
                match process_message(&dataset, &config, &schema, object_reader.as_ref(), &message).await {
                    ProcessOutcome::Creates { batches, keys, receipt_handle } => {
                        match create_envelopes(&schema, batches, &queue, receipt_handle) {
                            Ok(envelopes) => {
                                applied_keys.extend(keys);
                                for envelope in envelopes {
                                    yield envelope;
                                }
                            }
                            Err(error) => {
                                tracing::warn!(
                                    "Dataset '{}' failed to wrap S3 object rows as change-stream creates, so the SQS message will retry. Cause: {error}. See: {S3_DOCS}",
                                    dataset.name
                                );
                            }
                        }
                    }
                    ProcessOutcome::Rebuild { receipt_handle } => {
                        match object_lister.list_keys().await {
                            Ok(keys) => {
                                replace_applied_keys(&mut applied_keys, keys);
                            }
                            Err(error) => {
                                tracing::warn!(
                                    "Dataset '{}' will rebuild the accelerator from the listing prefix but could not refresh the applied-object set, so the next listing backfill may re-apply current objects. Cause: {error}. See: {S3_DOCS}",
                                    dataset.name
                                );
                            }
                        }
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
                    ProcessOutcome::Leave | ProcessOutcome::Retry => {}
                }
            }

            if last_backfill.elapsed() >= config.backfill_interval {
                match backfill_unapplied(
                    &dataset,
                    &config,
                    &schema,
                    object_lister.as_ref(),
                    object_reader.as_ref(),
                    &applied_keys,
                )
                .await
                {
                    Ok(backfill) => {
                        match backfill_envelopes(&schema, backfill.batches, true) {
                            Ok(envelopes) => {
                                applied_keys.extend(backfill.keys);
                                for envelope in envelopes {
                                    yield envelope;
                                }
                            }
                            Err(error) => {
                                tracing::warn!(
                                    "Dataset '{}' failed to wrap listing-backfill rows as change-stream creates, so those objects will be retried on the next `s3_changes_backfill_interval`. Cause: {error}. See: {S3_DOCS}",
                                    dataset.name
                                );
                            }
                        }
                    }
                    Err(error) => {
                        tracing::warn!(
                            "Dataset '{}' failed to list s3://{}/{} for the completeness backfill, so objects missed by SQS will not be applied until the next `s3_changes_backfill_interval`. Cause: {error}. See: {S3_DOCS}",
                            dataset.name,
                            config.bucket,
                            config.key_prefix
                        );
                    }
                }
                last_backfill = Instant::now();
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dataconnector::s3::{PARAMETERS, PREFIX};
    use arrow::array::{Array, Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
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
            let messages = {
                let mut incoming = self.incoming.lock().await;
                std::mem::take(&mut *incoming)
            };
            if messages.is_empty() {
                std::future::pending::<()>().await;
            }
            Ok(messages)
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

    struct MockLister {
        keys: Vec<String>,
    }

    #[async_trait]
    impl ObjectLister for MockLister {
        async fn list_keys(&self) -> std::result::Result<Vec<String>, StreamError> {
            Ok(self.keys.clone())
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

    fn id_name_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]))
    }

    fn id_name_batch(ids: &[i32], names: &[&str]) -> RecordBatch {
        let schema = id_name_schema();
        RecordBatch::try_new(
            schema,
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
            "s3_changes_test",
            params,
            PREFIX,
            Arc::new(RwLock::new(Secrets::new())),
            PARAMETERS.as_ref(),
        )
        .await
        .expect("valid S3 changes test parameters")
    }

    fn default_config() -> S3ChangesConfig {
        S3ChangesConfig {
            queue_url: QUEUE_URL.to_string(),
            region: "us-east-1".to_string(),
            on_object_removed: OnObjectRemoved::Ignore,
            bucket: "my-bucket".to_string(),
            key_prefix: "events/".to_string(),
            backfill_interval: Duration::from_secs(60 * 60),
        }
    }

    fn empty_lister() -> Arc<dyn ObjectLister> {
        Arc::new(MockLister { keys: vec![] })
    }

    fn start_stream(
        acceleration: AccelerationContents,
        queue: Arc<MockQueue>,
        reader: Arc<MapObjectReader>,
        lister: Arc<dyn ObjectLister>,
        config: S3ChangesConfig,
        snapshot: RecordBatch,
    ) -> ChangesStream {
        stream_s3_changes(S3ChangesStreamParts {
            dataset: events_dataset(),
            federated_table: federated_table(snapshot),
            acceleration,
            queue: queue as Arc<dyn MessageQueue>,
            object_reader: reader,
            object_lister: lister,
            config,
            session: SessionContext::new(),
        })
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
            ("s3_changes_queue_url", QUEUE_URL),
            ("s3_auth", "iam_role"),
        ])
        .await;
        let config = S3ChangesConfig::try_from_params(&params, &events_dataset())
            .expect("valid changes config")
            .expect("changes should be enabled");
        assert_eq!(config.bucket, "my-bucket");
        assert_eq!(config.key_prefix, "events/");
        assert_eq!(config.region, "us-east-1");
        assert_eq!(config.on_object_removed, OnObjectRemoved::Ignore);
        assert_eq!(config.backfill_interval, Duration::from_secs(60 * 60));
    }

    #[tokio::test]
    async fn validate_skips_when_changes_is_not_configured() {
        let params = test_params(vec![]).await;
        let mut dataset = DatasetSpec::new("s3://my-bucket/events/", "events".into());
        dataset.acceleration = Some(Acceleration::default());
        assert_eq!(
            S3ChangesConfig::try_from_params(&params, &dataset).expect("no changes"),
            None
        );
    }

    #[tokio::test]
    async fn validate_fails_closed_without_queue_on_changes() {
        let params = test_params(vec![]).await;
        let error = S3ChangesConfig::try_from_params(&params, &events_dataset())
            .expect_err("changes requires a queue URL");
        let message = error.to_string();
        assert!(
            message.contains("s3_changes_queue_url"),
            "S3-specific error must name the param, got: {message}"
        );
        assert!(
            message.contains("not an ARN"),
            "S3-specific error must say queue URL not ARN, got: {message}"
        );
        assert!(
            message.contains(S3_DOCS),
            "S3-specific error must include the docs pointer, got: {message}"
        );
    }

    #[tokio::test]
    async fn validate_fails_closed_on_queue_without_changes() {
        let params = test_params(vec![("s3_changes_queue_url", QUEUE_URL)]).await;
        let dataset = DatasetSpec::new("s3://my-bucket/events/", "events".into());
        let error = S3ChangesConfig::try_from_params(&params, &dataset)
            .expect_err("queue without changes is refused");
        assert!(error.to_string().contains("refresh_mode"));
        assert!(error.to_string().contains("s3_changes_queue_url"));
    }

    #[tokio::test]
    async fn validate_rejects_queue_arn() {
        let params = test_params(vec![(
            "s3_changes_queue_url",
            "arn:aws:sqs:us-east-1:123456789012:s3-events",
        )])
        .await;
        let error = S3ChangesConfig::try_from_params(&params, &events_dataset())
            .expect_err("ARN must be refused");
        assert!(error.to_string().contains("not an ARN"));
        assert!(error.to_string().contains("s3_changes_queue_url"));
    }

    #[tokio::test]
    async fn validate_rejects_public_auth() {
        let params = test_params(vec![
            ("s3_changes_queue_url", QUEUE_URL),
            ("s3_auth", "public"),
        ])
        .await;
        let error = S3ChangesConfig::try_from_params(&params, &events_dataset())
            .expect_err("public auth cannot consume SQS");
        assert!(error.to_string().contains("public"));
    }

    #[tokio::test]
    async fn validate_rejects_invalid_removed_action_at_param_parse() {
        let error = Parameters::try_new(
            "s3_changes_test",
            vec![
                (
                    "s3_changes_queue_url".to_string(),
                    QUEUE_URL.to_string().into(),
                ),
                (
                    "s3_on_object_removed".to_string(),
                    "delete".to_string().into(),
                ),
            ],
            PREFIX,
            Arc::new(RwLock::new(Secrets::new())),
            PARAMETERS.as_ref(),
        )
        .await
        .expect_err("`delete` is not a valid `s3_on_object_removed` value");
        let message = error.to_string();
        assert!(
            message.contains("s3_on_object_removed"),
            "invalid ObjectRemoved action must name the param, got: {message}"
        );
        assert!(
            message.contains("ignore") && message.contains("rebuild"),
            "invalid ObjectRemoved action must list ignore|rebuild, got: {message}"
        );
    }

    #[tokio::test]
    async fn validate_rejects_prefix_outside_dataset() {
        let params = test_params(vec![
            ("s3_changes_queue_url", QUEUE_URL),
            ("s3_changes_key_prefix", "other/"),
        ])
        .await;
        let error = S3ChangesConfig::try_from_params(&params, &events_dataset())
            .expect_err("prefix outside dataset");
        assert!(error.to_string().contains("s3_changes_key_prefix"));
    }

    #[tokio::test]
    async fn validate_nested_prefix_rebuild_and_backfill_interval() {
        let params = test_params(vec![
            ("s3_changes_queue_url", QUEUE_URL),
            ("s3_changes_key_prefix", "events/year=2026"),
            ("s3_on_object_removed", "rebuild"),
            ("s3_changes_backfill_interval", "30m"),
        ])
        .await;
        let config = S3ChangesConfig::try_from_params(&params, &events_dataset())
            .expect("nested prefix is valid")
            .expect("changes enabled");
        assert_eq!(config.key_prefix, "events/year=2026/");
        assert_eq!(config.on_object_removed, OnObjectRemoved::Rebuild);
        assert_eq!(config.backfill_interval, Duration::from_secs(30 * 60));
    }

    #[tokio::test]
    async fn validate_rejects_zero_backfill_interval() {
        let params = test_params(vec![
            ("s3_changes_queue_url", QUEUE_URL),
            ("s3_changes_backfill_interval", "0s"),
        ])
        .await;
        let error = S3ChangesConfig::try_from_params(&params, &events_dataset())
            .expect_err("zero interval is refused");
        assert!(error.to_string().contains("s3_changes_backfill_interval"));
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
            &id_name_schema(),
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
                keys,
                receipt_handle,
            } => {
                assert_eq!(batches.len(), 1);
                assert_eq!(batches[0].num_rows(), 1);
                assert_eq!(keys, vec!["events/a.parquet".to_string()]);
                assert_eq!(receipt_handle, "rh-1");
            }
            other => panic!("expected Creates, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn process_removed_is_acked_by_default_and_rebuilds_when_configured() {
        let reader = MapObjectReader {
            objects: HashMap::new(),
            fail_keys: vec![],
        };
        let outcome = process_message(
            &events_dataset(),
            &default_config(),
            &id_name_schema(),
            &reader,
            &QueueMessage {
                body: removed_body("events/a.parquet"),
                receipt_handle: "rh-del".into(),
            },
        )
        .await;
        assert!(matches!(outcome, ProcessOutcome::Ack { .. }));

        let mut config = default_config();
        config.on_object_removed = OnObjectRemoved::Rebuild;
        let outcome = process_message(
            &events_dataset(),
            &config,
            &id_name_schema(),
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
    async fn process_unmatched_prefix_is_left_on_queue_poison_is_acked() {
        let reader = MapObjectReader {
            objects: HashMap::new(),
            fail_keys: vec![],
        };
        let unmatched = process_message(
            &events_dataset(),
            &default_config(),
            &id_name_schema(),
            &reader,
            &QueueMessage {
                body: created_put_body("other/a.parquet"),
                receipt_handle: "rh-other".into(),
            },
        )
        .await;
        assert!(
            matches!(unmatched, ProcessOutcome::Leave),
            "unmatched messages must not be deleted, got {unmatched:?}"
        );

        let other_bucket = process_message(
            &events_dataset(),
            &default_config(),
            &id_name_schema(),
            &reader,
            &QueueMessage {
                body: r#"{"Records":[{"eventName":"ObjectCreated:Put","s3":{"bucket":{"name":"other-bucket"},"object":{"key":"events/a.parquet"}}}]}"#.into(),
                receipt_handle: "rh-bucket".into(),
            },
        )
        .await;
        assert!(
            matches!(other_bucket, ProcessOutcome::Leave),
            "other-bucket messages must not be deleted, got {other_bucket:?}"
        );

        let poison = process_message(
            &events_dataset(),
            &default_config(),
            &id_name_schema(),
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
            &id_name_schema(),
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
    async fn backfill_skips_already_applied_keys() {
        let reader = MapObjectReader {
            objects: HashMap::from([
                (
                    "my-bucket/events/old.parquet".to_string(),
                    vec![id_name_batch(&[1], &["old"])],
                ),
                (
                    "my-bucket/events/new.parquet".to_string(),
                    vec![id_name_batch(&[2], &["new"])],
                ),
            ]),
            fail_keys: vec![],
        };
        let lister = MockLister {
            keys: vec![
                "events/old.parquet".into(),
                "events/new.parquet".into(),
                "other/skip.parquet".into(),
            ],
        };
        let applied = HashSet::from(["events/old.parquet".to_string()]);
        let result = backfill_unapplied(
            &events_dataset(),
            &default_config(),
            &id_name_schema(),
            &lister,
            &reader,
            &applied,
        )
        .await
        .expect("backfill should succeed");
        assert_eq!(result.keys, vec!["events/new.parquet".to_string()]);
        assert_eq!(result.batches.len(), 1);
        assert_eq!(result.batches[0].num_rows(), 1);
    }

    #[tokio::test]
    async fn stream_object_created_yields_create_and_commit_deletes_sqs_message() {
        let queue = Arc::new(MockQueue::with_messages(vec![QueueMessage {
            body: created_put_body("events/new.parquet"),
            receipt_handle: "rh-new".into(),
        }]));
        let reader = Arc::new(MapObjectReader {
            objects: HashMap::from([(
                "my-bucket/events/new.parquet".to_string(),
                vec![id_name_batch(&[7], &["created"])],
            )]),
            fail_keys: vec![],
        });
        let stream = start_stream(
            AccelerationContents::NonEmpty,
            Arc::clone(&queue),
            reader,
            empty_lister(),
            default_config(),
            id_name_batch(&[1], &["snap"]),
        );
        let mut envelopes = collect_until_idle(stream, 3).await;
        assert!(
            envelopes.len() >= 3,
            "expected rebuild + ready + create, got {}",
            envelopes.len()
        );
        assert!(envelopes[0].history_unavailable());
        assert!(envelopes[1].is_dataset_ready());
        assert!(envelopes[1].is_empty());

        let create = envelopes.remove(2);
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
    async fn stream_unmatched_message_is_not_deleted() {
        let queue = Arc::new(MockQueue::with_messages(vec![QueueMessage {
            body: created_put_body("other/a.parquet"),
            receipt_handle: "rh-other".into(),
        }]));
        let reader = Arc::new(MapObjectReader {
            objects: HashMap::new(),
            fail_keys: vec![],
        });
        let stream = start_stream(
            AccelerationContents::NonEmpty,
            Arc::clone(&queue),
            reader,
            empty_lister(),
            default_config(),
            id_name_batch(&[1], &["snap"]),
        );
        let envelopes = collect_until_idle(stream, 3).await;
        assert_eq!(
            envelopes.len(),
            2,
            "unmatched SQS must not yield a create, got {}",
            envelopes.len()
        );
        assert!(envelopes[0].history_unavailable());
        assert!(envelopes[1].is_dataset_ready());
        assert!(
            queue.deleted.lock().await.is_empty(),
            "unmatched SQS messages must stay on the queue"
        );
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
        config.on_object_removed = OnObjectRemoved::Rebuild;
        let stream = start_stream(
            AccelerationContents::NonEmpty,
            Arc::clone(&queue),
            reader,
            empty_lister(),
            config,
            id_name_batch(&[1], &["snap"]),
        );
        let mut envelopes = collect_until_idle(stream, 3).await;
        assert!(
            envelopes.len() >= 3,
            "expected startup rebuild + ready + ObjectRemoved rebuild, got {}",
            envelopes.len()
        );
        assert!(envelopes[0].history_unavailable());
        assert!(envelopes[1].is_dataset_ready());
        let rebuild = envelopes.remove(2);
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
        let stream = start_stream(
            AccelerationContents::Empty,
            queue,
            reader,
            empty_lister(),
            default_config(),
            id_name_batch(&[1], &["snap"]),
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

    #[tokio::test]
    async fn stream_nonempty_rebuilds_from_listing_before_ready() {
        let queue = Arc::new(MockQueue::with_messages(vec![]));
        let reader = Arc::new(MapObjectReader {
            objects: HashMap::from([(
                "my-bucket/events/missed.parquet".to_string(),
                vec![id_name_batch(&[9], &["missed"])],
            )]),
            fail_keys: vec![],
        });
        let lister = Arc::new(MockLister {
            keys: vec!["events/missed.parquet".into()],
        });
        let stream = start_stream(
            AccelerationContents::NonEmpty,
            queue,
            reader,
            lister,
            default_config(),
            id_name_batch(&[1], &["snap"]),
        );
        let envelopes = collect_until_idle(stream, 3).await;
        assert_eq!(
            envelopes.len(),
            2,
            "expected history_unavailable + ready, not an append of listed objects, got {}",
            envelopes.len()
        );
        assert!(envelopes[0].history_unavailable());
        assert!(!envelopes[0].is_dataset_ready());
        assert!(envelopes[1].is_dataset_ready());
        assert!(envelopes[1].is_empty());
    }

    #[tokio::test]
    async fn process_mixed_notification_is_left_on_queue() {
        let reader = MapObjectReader {
            objects: HashMap::from([(
                "my-bucket/events/a.parquet".to_string(),
                vec![id_name_batch(&[1], &["a"])],
            )]),
            fail_keys: vec![],
        };
        let mixed = process_message(
            &events_dataset(),
            &default_config(),
            &id_name_schema(),
            &reader,
            &QueueMessage {
                body: r#"{"Records":[{"eventName":"ObjectCreated:Put","s3":{"bucket":{"name":"my-bucket"},"object":{"key":"events/a.parquet"}}},{"eventName":"ObjectCreated:Put","s3":{"bucket":{"name":"my-bucket"},"object":{"key":"other/b.parquet"}}}]}"#.into(),
                receipt_handle: "rh-mixed".into(),
            },
        )
        .await;
        assert!(
            matches!(mixed, ProcessOutcome::Leave),
            "a mixed notification must leave the entire message, got {mixed:?}"
        );
    }

    #[test]
    fn queue_errors_do_not_include_the_secret_queue_url() {
        let receive = QueueError::Receive {
            source: "access denied".into(),
        };
        let delete = QueueError::Delete {
            source: "access denied".into(),
        };
        for message in [receive.to_string(), delete.to_string()] {
            assert!(
                !message.contains("amazonaws")
                    && !message.contains("123456789012")
                    && !message.contains(QUEUE_URL),
                "SQS errors must not interpolate the queue URL, got: {message}"
            );
        }
    }

    #[tokio::test]
    async fn sqs_auth_uses_explicit_keys_without_auth_key_and_restricts_iam_source() {
        let explicit = test_params(vec![
            ("s3_changes_queue_url", QUEUE_URL),
            ("s3_key", "AKIAEXAMPLE"),
            ("s3_secret", "secret"),
        ])
        .await;
        assert_eq!(
            sqs_auth_from_params(&explicit, "events").expect("explicit keys"),
            SqsAuth::ExplicitKeys
        );

        let metadata = test_params(vec![
            ("s3_changes_queue_url", QUEUE_URL),
            ("s3_auth", "iam_role"),
            ("s3_iam_role_source", "metadata"),
        ])
        .await;
        assert_eq!(
            sqs_auth_from_params(&metadata, "events").expect("metadata"),
            SqsAuth::RestrictedIam {
                source: "metadata".into(),
            }
        );

        let env = test_params(vec![
            ("s3_changes_queue_url", QUEUE_URL),
            ("s3_auth", "iam_role"),
            ("s3_iam_role_source", "env"),
        ])
        .await;
        assert_eq!(
            sqs_auth_from_params(&env, "events").expect("env"),
            SqsAuth::RestrictedIam {
                source: "env".into(),
            }
        );

        let default_chain = test_params(vec![
            ("s3_changes_queue_url", QUEUE_URL),
            ("s3_auth", "iam_role"),
        ])
        .await;
        assert_eq!(
            sqs_auth_from_params(&default_chain, "events").expect("default chain"),
            SqsAuth::DefaultChain
        );
    }

    #[test]
    fn align_object_batch_adds_hive_partition_columns_from_key() {
        let table_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("year", DataType::Utf8, true),
            Field::new("month", DataType::Utf8, true),
        ]));
        let aligned = align_object_batch(
            &table_schema,
            "events/year=2026/month=09/a.parquet",
            &id_name_batch(&[1], &["a"]),
        )
        .expect("hive columns should be reconstructed from the object key");
        assert_eq!(aligned.schema().as_ref(), table_schema.as_ref());
        let year = aligned
            .column_by_name("year")
            .expect("year")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("year is Utf8");
        let month = aligned
            .column_by_name("month")
            .expect("month")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("month is Utf8");
        assert_eq!(year.value(0), "2026");
        assert_eq!(month.value(0), "09");
    }

    #[test]
    fn align_object_batch_is_noop_when_schemas_match() {
        let batch = id_name_batch(&[1], &["a"]);
        let aligned = align_object_batch(&id_name_schema(), "events/a.parquet", &batch)
            .expect("matching schema is a no-op");
        assert_eq!(aligned.schema().as_ref(), batch.schema().as_ref());
        assert_eq!(aligned.num_rows(), 1);
    }

    #[test]
    fn replace_applied_keys_uses_current_listing_not_clear() {
        let mut applied = HashSet::from(["events/gone.parquet".to_string()]);
        replace_applied_keys(
            &mut applied,
            vec![
                "events/a.parquet".to_string(),
                "events/b.parquet".to_string(),
            ],
        );
        assert!(!applied.contains("events/gone.parquet"));
        assert!(applied.contains("events/a.parquet"));
        assert!(applied.contains("events/b.parquet"));
    }
}
