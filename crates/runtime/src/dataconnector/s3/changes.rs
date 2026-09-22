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
//! path in one backpressured generator (the same shape as `MongoDB` / Kafka
//! change streams). A manual `Stream` impl would split that state machine
//! across poll/yield points without changing behavior.

use super::event::{
    ObjectEventKind, S3ObjectEvent, decode_from_path_key, matches_dataset, parse_notification_body,
    s3_object_from,
};
use super::{S3, S3_DOCS};
use crate::dataconnector::federated::FederatedTableProvider;
use crate::dataconnector::listing::{
    ListingTableConnector, detect_file_extension_from_url_or_path, file_matches_extension,
    parse_file_extension_param,
};
use crate::dataconnector::parameters::ConnectorContext;
use crate::dataconnector::{ConnectorComponent, DataConnectorError, DataConnectorResult};
use arrow::array::{ArrayRef, RecordBatch, StringArray, new_null_array};
use arrow::compute::concat_batches;
use arrow::datatypes::{DataType, SchemaRef};
use async_stream::try_stream;
use async_trait::async_trait;
use data_components::cdc::{
    AccelerationContents, ChangeEnvelope, ChangesStream, CommitChange, CommitError, NoOpCommitter,
    StreamError, build_ready_signal_envelope, shutdown_epoch, wrap_data_as_change_batch,
};
use futures::StreamExt;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, ObjectStoreExt};
use parking_lot::Mutex;
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
const LISTING_RETRY_BACKOFF: Duration = Duration::from_millis(200);
const LISTING_RETRY_BACKOFF_CAP: Duration = Duration::from_secs(30);
const DEFAULT_BACKFILL_INTERVAL: Duration = Duration::from_hours(1);

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

    #[snafu(display(
        "Failed to register dataset {dataset_name} (s3): `refresh_mode: changes` needs `from` to name an S3 prefix, but '{from}' contains a wildcard. Point `from` at the prefix that holds the objects (for example 's3://bucket/events/') and narrow it with `s3_changes_key_prefix`. See: {S3_DOCS}"
    ))]
    FromIsGlob { dataset_name: String, from: String },

    #[snafu(display(
        "Failed to register dataset {dataset_name} (s3): `refresh_mode: changes` needs `from` to name an S3 prefix, but '{from}' names a single object. Point `from` at the prefix that holds the objects (for example 's3://bucket/events/'), or keep this object on `refresh_mode: full`. See: {S3_DOCS}"
    ))]
    FromNamesAnObject { dataset_name: String, from: String },

    #[snafu(display(
        "Failed to register dataset {dataset_name} (s3): `refresh_mode: changes` does not support unstructured text objects. Set `file_format` to a listing format such as `parquet`, `csv`, `json`, or `orc`, set `file_extension` to one of those, or point `from` at objects with one of those extensions. See: {S3_DOCS}"
    ))]
    UnstructuredTextUnsupported { dataset_name: String },

    #[snafu(display(
        "Failed to register dataset {dataset_name} (s3): `s3_changes_queue_url` is not an SQS queue URL. Use a URL like https://sqs.<region>.amazonaws.com/<account>/<queue>. See: {S3_DOCS}"
    ))]
    QueueUrlNotHttp { dataset_name: String },
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
    /// Dataset `from:` key prefix. Snapshot and `history_unavailable` rebuild
    /// use this scope so they match the federated listing table.
    pub dataset_prefix: String,
    /// SQS / listing-backfill filter. Equal to or nested under `dataset_prefix`.
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

/// Object keys that have been applied, or yielded and not yet committed.
///
/// Keys are marked in-flight when their envelopes are yielded so a prefetch
/// cannot append the same object twice. They move to `committed` only from a
/// post-apply committer. An in-flight SQS notification is left on the queue
/// (not deleted) so a failed apply can still retry.
///
/// A listing rebuild replaces the whole set and takes ownership of every key it
/// lists, so each claim carries the `generation` it was made in. The consumer
/// trims a run to its last rebuild signal and drops the superseded envelopes
/// unapplied, and that drop must not release a key the rebuild now owns:
/// a redelivered notification for it would be applied on top of the replacement.
#[derive(Debug, Default, Clone)]
struct AppliedKeySet {
    committed: HashSet<String>,
    in_flight: HashSet<String>,
    generation: u64,
}

impl AppliedKeySet {
    fn is_committed(&self, key: &str) -> bool {
        self.committed.contains(key)
    }

    fn is_in_flight(&self, key: &str) -> bool {
        self.in_flight.contains(key)
    }

    fn is_known(&self, key: &str) -> bool {
        self.is_committed(key) || self.is_in_flight(key)
    }

    /// The generation a claim made now belongs to. Only a listing rebuild
    /// advances it, and only from the stream task, so a caller that reads it
    /// before yielding its envelopes still holds the generation it claimed in.
    fn generation(&self) -> u64 {
        self.generation
    }

    fn mark_in_flight(&mut self, keys: impl IntoIterator<Item = String>) {
        for key in keys {
            if !self.committed.contains(&key) {
                self.in_flight.insert(key);
            }
        }
    }

    fn commit(&mut self, generation: u64, keys: &[String]) {
        if generation != self.generation {
            return;
        }
        for key in keys {
            self.in_flight.remove(key);
            self.committed.insert(key.clone());
        }
    }

    fn abort_in_flight(&mut self, generation: u64, keys: &[String]) {
        if generation != self.generation {
            return;
        }
        for key in keys {
            self.in_flight.remove(key);
        }
    }

    fn replace_in_flight(&mut self, listed: Vec<String>) -> u64 {
        self.committed.clear();
        self.in_flight = listed.into_iter().collect();
        self.generation = self.generation.saturating_add(1);
        self.generation
    }
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

/// Advances [`AppliedKeySet`] after the consumer applies the envelope. Drop
/// without `commit` releases in-flight keys so a retry or backfill can apply
/// them again. Both are no-ops once a listing rebuild has superseded the
/// generation these keys were claimed in; the inner commit (the SQS delete)
/// still runs, because the rows it acknowledges were applied.
struct AppliedKeysCommitter {
    applied: Arc<Mutex<AppliedKeySet>>,
    generation: u64,
    keys: Vec<String>,
    inner: Box<dyn CommitChange + Send + Sync>,
}

#[async_trait]
impl CommitChange for AppliedKeysCommitter {
    async fn commit(&self) -> std::result::Result<(), CommitError> {
        self.applied.lock().commit(self.generation, &self.keys);
        self.inner.commit().await
    }
}

impl Drop for AppliedKeysCommitter {
    fn drop(&mut self) {
        self.applied
            .lock()
            .abort_in_flight(self.generation, &self.keys);
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
                "S3 changes cannot read unstructured text object s3://{bucket}/{key} for dataset '{}'. Set `file_format` to a listing format such as parquet, csv, json, or orc. See: {S3_DOCS}",
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

/// The objects under the dataset prefix that its listing table reads, selected
/// with the same [`file_matches_extension`] filter. Any other object, such as a
/// `_SUCCESS` job marker or a `.crc` checksum sidecar, holds none of the
/// dataset's rows and fails to read as one: a snapshot or rebuild that counted
/// it as unread would never complete, and a notification naming it would be
/// retried until the queue's retention period expires.
#[derive(Debug, Clone)]
struct ListingFileFilter {
    extension: String,
}

impl ListingFileFilter {
    fn matches(&self, key: &str) -> bool {
        file_matches_extension(&ObjectPath::from(key), &self.extension)
    }
}

struct ListingPrefixScanner {
    connector: S3,
    dataset: DatasetSpec,
    prefix: String,
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
        let prefix = if self.prefix.is_empty() {
            None
        } else {
            Some(ObjectPath::from(self.prefix.as_str()))
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
/// the queue value is an ARN or is not an HTTPS SQS queue URL, `s3_auth` is
/// `public`, `s3_on_object_removed` is unknown, `s3_changes_key_prefix` is
/// outside the dataset path, `s3_changes_backfill_interval` is not a positive
/// duration, or no SQS region can be resolved.
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
                if !is_sqs_queue_url(url) {
                    return QueueUrlNotHttpSnafu { dataset_name }.fail();
                }
                if params.get("auth").expose().ok() == Some("public") {
                    return PublicAuthCannotConsumeSqsSnafu { dataset_name }.fail();
                }
                ensure_structured_file_format(params, dataset)?;

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
                    None => dataset_prefix.clone(),
                    Some(configured) => {
                        let normalized = normalize_prefix(&decode_from_path_key(configured));
                        ensure!(
                            prefix_is_nested_under(&normalized, &dataset_prefix),
                            KeyPrefixOutsideDatasetSnafu {
                                dataset_name,
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
                    dataset_prefix,
                    key_prefix,
                    backfill_interval,
                }))
            }
        }
    }
}

/// Changes-mode object reads go through `create_listing_table`, which cannot
/// open unstructured text. Refuse at validate time so startup does not accept
/// a config that then errors on every notification.
///
/// Format is resolved the same way the listing table does: explicit
/// `file_format`, then `file_extension` (`parse_file_extension_param`), then
/// the `from` path (`detect_file_extension_from_url_or_path`). A prefix
/// dataset with `file_extension: .parquet` and no `file_format` is therefore
/// accepted here, matching `get_file_format_and_extension`. ORC (and Vortex
/// on non-Windows) are listing formats, so they are accepted here too.
fn ensure_structured_file_format(params: &Parameters, dataset: &DatasetSpec) -> Result<()> {
    let file_format = params
        .get("file_format")
        .expose()
        .ok()
        .map(str::to_ascii_lowercase)
        .filter(|v| !v.is_empty());
    let file_extension = params
        .get("file_extension")
        .expose()
        .ok()
        .and_then(parse_file_extension_param)
        .and_then(|parsed| parsed.format_extension);
    let path_extension = detect_file_extension_from_url_or_path(&dataset.from)
        .and_then(|parsed| parsed.format_extension);

    let explicit_ok = file_format
        .as_deref()
        .is_some_and(is_structured_listing_format);
    let extension_ok = file_extension
        .as_deref()
        .is_some_and(is_structured_listing_format);
    let inferred_ok = path_extension
        .as_deref()
        .is_some_and(is_structured_listing_format);

    // `file_format: auto` is not itself structured; a bare prefix still needs
    // `file_extension` or a structured `from` path.
    if explicit_ok || extension_ok || inferred_ok {
        return Ok(());
    }

    UnstructuredTextUnsupportedSnafu {
        dataset_name: dataset.name.to_string(),
    }
    .fail()
}

/// Formats `get_file_format_and_extension` turns into a listing `FileFormat`.
/// Vortex is not linked on Windows; keep the allowlist in lockstep.
fn is_structured_listing_format(name: &str) -> bool {
    matches!(
        name,
        "parquet" | "csv" | "json" | "tsv" | "jsonl" | "ndjson" | "ldjson" | "orc"
    ) || {
        #[cfg(not(windows))]
        {
            name == "vortex"
        }
        #[cfg(windows)]
        {
            false
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
    // `DatasetSpec::path()` keeps URI escapes. Notification keys (and the
    // object store) use the decoded key, so derive the prefix from that.
    let rest = decode_from_path_key(rest);
    // Every key under the prefix is the dataset, so a `from` the listing table
    // resolves to one object or to a glob has no prefix to derive: appending a
    // `/` to it produces a prefix nothing is under, which would snapshot an
    // empty accelerator and put every notification for the object outside the
    // dataset. Refuse both instead. Check after decode so `%2A` is a glob too.
    ensure!(
        !rest.contains(['*', '?', '[']),
        FromIsGlobSnafu {
            dataset_name: dataset.name.to_string(),
            from: dataset.from.clone(),
        }
    );
    Ok((bucket.to_string(), normalize_prefix(&rest)))
}

/// The object key a `from` names outright, if it can name one: S3 has no
/// directories, so only a trailing `/` marks a path as a prefix for certain.
/// `None` when `from` already ends in `/`, and for a bucket root.
fn key_from_may_name(dataset: &DatasetSpec) -> Option<String> {
    let path = dataset.path().trim_start_matches('/');
    if path.ends_with('/') {
        return None;
    }
    let (_, rest) = path.split_once('/')?;
    (!rest.is_empty()).then(|| decode_from_path_key(rest))
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
    region_from_sqs_host(parsed.host_str()?)
}

/// Region embedded in an SQS queue-URL host, including FIPS and VPC endpoints.
fn region_from_sqs_host(host: &str) -> Option<String> {
    let host = host.to_ascii_lowercase();
    let labels: Vec<&str> = host.split('.').collect();
    let region = match labels.as_slice() {
        ["sqs" | "sqs-fips", region, "amazonaws", "com"]
        | ["sqs", region, "amazonaws", "com", "cn"]
        | ["sqs", region, "vpce", "amazonaws", "com"]
        | [_, "sqs", region, "vpce", "amazonaws", "com"] => *region,
        _ => return None,
    };
    is_aws_region(region).then(|| region.to_string())
}

/// An HTTPS SQS queue URL: AWS partition host and `/account/queue` path.
///
/// Loopback and instance-metadata URLs are not SQS queues and must fail at
/// registration. Custom SQS endpoints are not a parameter.
#[must_use]
fn is_sqs_queue_url(url: &str) -> bool {
    let Ok(parsed) = url::Url::parse(url) else {
        return false;
    };
    if parsed.scheme() != "https" {
        return false;
    }
    if !parsed.username().is_empty() || parsed.password().is_some() {
        return false;
    }
    if parsed.query().is_some() || parsed.fragment().is_some() {
        return false;
    }
    let Some(host) = parsed.host_str() else {
        return false;
    };
    region_from_sqs_host(host).is_some() && sqs_queue_path_is_allowed(parsed.path())
}

fn is_aws_region(region: &str) -> bool {
    let bytes = region.as_bytes();
    (2..=32).contains(&bytes.len())
        && bytes[0].is_ascii_lowercase()
        && bytes.contains(&b'-')
        && bytes
            .iter()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || *b == b'-')
        && !region.starts_with('-')
        && !region.ends_with('-')
        && !region.contains("--")
}

fn sqs_queue_path_is_allowed(path: &str) -> bool {
    let path = path.trim_end_matches('/');
    let Some((account, queue)) = path.strip_prefix('/').and_then(|p| p.split_once('/')) else {
        return false;
    };
    account.len() == 12
        && account.bytes().all(|b| b.is_ascii_digit())
        && !queue.is_empty()
        && !queue.contains('/')
        && queue.len() <= 80
        && {
            let name = queue.strip_suffix(".fifo").unwrap_or(queue);
            !name.is_empty()
                && name
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
        }
}

fn applied_keys_committer(
    applied: &Arc<Mutex<AppliedKeySet>>,
    generation: u64,
    keys: Vec<String>,
    inner: Box<dyn CommitChange + Send + Sync>,
) -> Box<dyn CommitChange + Send + Sync> {
    Box::new(AppliedKeysCommitter {
        applied: Arc::clone(applied),
        generation,
        keys,
        inner,
    })
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

fn error_stream(error: impl std::error::Error + Send + Sync + 'static) -> ChangesStream {
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
    let listing_files = match connector.get_file_format_and_extension(dataset).await {
        Ok((_, extension)) => ListingFileFilter { extension },
        Err(error) => return Some(error_stream(error)),
    };
    // Only the object store can say whether a `from` without a trailing `/`
    // names an object or a prefix, and the two are the same string in S3.
    if let Some(key) = key_from_may_name(dataset) {
        match connector.get_object_store(dataset) {
            Ok(store) => match store.head(&ObjectPath::from(key.as_str())).await {
                // Only NotFound proves the path can be treated as a prefix.
                // Auth / timeout / transient HEAD failures must not become
                // "object absent" — that path marks an empty accelerator ready.
                Ok(_) => {
                    return Some(error_stream(Error::FromNamesAnObject {
                        dataset_name: dataset.name.to_string(),
                        from: dataset.from.clone(),
                    }));
                }
                Err(object_store::Error::NotFound { .. }) => {}
                Err(error) => return Some(error_stream(error)),
            },
            Err(error) => return Some(error_stream(error)),
        }
    }
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
        prefix: config.dataset_prefix.clone(),
    });
    Some(stream_s3_changes(S3ChangesStreamParts {
        dataset: dataset.clone(),
        federated_table,
        acceleration,
        queue,
        object_reader,
        object_lister,
        config,
        listing_files,
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
    listing_files: ListingFileFilter,
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
    listing_files: &ListingFileFilter,
    table_schema: &SchemaRef,
    object_reader: &dyn ObjectReader,
    applied_keys: &Mutex<AppliedKeySet>,
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
            "Dataset '{}' received an S3 notification for s3://{}/{} that is outside this dataset's prefix {}, so the entire SQS message was left on the queue (not deleted) and will become visible again after each visibility timeout until it is deleted or the queue retention period expires. The queue must be exclusive to this dataset — fan out with SNS to a per-dataset queue, or set a bucket notification prefix filter. Sharing one queue across datasets is not supported. See: {S3_DOCS}",
            dataset.name,
            sample.bucket,
            sample.key,
            prefix_display(&config.bucket, &config.key_prefix)
        );
        return ProcessOutcome::Leave;
    }

    // An object the listing table does not read holds none of the dataset's
    // rows, so creating or removing it changes nothing.
    let matching: Vec<&S3ObjectEvent> = matching
        .into_iter()
        .filter(|event| listing_files.matches(&event.key))
        .collect();
    if matching.is_empty() {
        tracing::debug!(
            "Dataset '{}' acknowledged an S3 notification that names only objects its listing table does not read (file extension '{}').",
            dataset.name,
            listing_files.extension
        );
        return ProcessOutcome::Ack { receipt_handle };
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

    // A key can appear in more than one record of a notification, and the
    // applied-key set learns of it only once its envelope is yielded, so each
    // key is read at most once here.
    let mut seen_keys = HashSet::new();
    let created: Vec<&S3ObjectEvent> = matching
        .iter()
        .copied()
        .filter(|event| {
            event.kind == ObjectEventKind::Created && !applied_keys.lock().is_known(&event.key)
        })
        .filter(|&event| seen_keys.insert(event.key.as_str()))
        .collect();
    if created.is_empty() {
        let in_flight = matching.iter().any(|event| {
            event.kind == ObjectEventKind::Created && applied_keys.lock().is_in_flight(&event.key)
        });
        if in_flight {
            // Apply is still pending. Do not delete — a failed apply must retry.
            return ProcessOutcome::Leave;
        }
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
    /// Prefix-matching keys that failed to read or align. Snapshot and rebuild
    /// refuse the pass when this is non-empty; periodic backfill skips them.
    unread_keys: Vec<String>,
}

impl BackfillCreates {
    fn is_complete(&self) -> bool {
        self.unread_keys.is_empty()
    }
}

fn unread_keys_display(keys: &[String]) -> String {
    const LIMIT: usize = 3;
    let shown: Vec<String> = keys
        .iter()
        .take(LIMIT)
        .map(|key| format!("'{key}'"))
        .collect();
    match keys.len() {
        0 => "(none)".to_string(),
        n if n <= LIMIT => shown.join(", "),
        n => format!("{}, and {} more", shown.join(", "), n - LIMIT),
    }
}

fn incomplete_listing_warning(
    dataset_name: impl std::fmt::Display,
    pass: &str,
    unread_keys: &[String],
    impact: &str,
) -> String {
    format!(
        "Dataset '{dataset_name}' could not read every listed object during {pass}, so {impact}. Unread objects: {}. See: {S3_DOCS}",
        unread_keys_display(unread_keys)
    )
}

fn unread_object_warning(
    dataset_name: impl std::fmt::Display,
    bucket: &str,
    key: &str,
    pass: &str,
    error: &impl std::fmt::Display,
    best_effort: bool,
) -> String {
    let impact = if best_effort {
        "that object will be retried on the next `s3_changes_backfill_interval`"
    } else {
        "that object was not applied and this pass is incomplete"
    };
    format!(
        "Dataset '{dataset_name}' failed to read s3://{bucket}/{key} during {pass}, so {impact}. Cause: {error}. See: {S3_DOCS}"
    )
}

#[expect(
    clippy::too_many_arguments,
    reason = "snapshot, backfill, and listing rebuild share one listing apply; `pass` names the user-facing warn"
)]
async fn apply_unapplied_objects(
    dataset: &DatasetSpec,
    config: &S3ChangesConfig,
    listing_files: &ListingFileFilter,
    table_schema: &SchemaRef,
    object_lister: &dyn ObjectLister,
    object_reader: &dyn ObjectReader,
    applied_keys: &Mutex<AppliedKeySet>,
    scope_prefix: &str,
    pass: &'static str,
    skip_known: bool,
    best_effort: bool,
) -> std::result::Result<BackfillCreates, StreamError> {
    let listed = object_lister.list_keys().await?;
    let mut batches = Vec::new();
    let mut keys = Vec::new();
    let mut unread_keys = Vec::new();
    for key in listed {
        let event = S3ObjectEvent {
            event_name: pass.to_string(),
            kind: ObjectEventKind::Created,
            bucket: config.bucket.clone(),
            key: key.clone(),
        };
        if !matches_dataset(&event, &config.bucket, scope_prefix) || !listing_files.matches(&key) {
            continue;
        }
        if skip_known && applied_keys.lock().is_known(&key) {
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
                        "{}",
                        unread_object_warning(
                            &dataset.name,
                            &config.bucket,
                            &key,
                            pass,
                            &error,
                            best_effort
                        )
                    );
                    unread_keys.push(key);
                }
            },
            Err(error) => {
                tracing::warn!(
                    "{}",
                    unread_object_warning(
                        &dataset.name,
                        &config.bucket,
                        &key,
                        pass,
                        &error,
                        best_effort
                    )
                );
                unread_keys.push(key);
            }
        }
    }
    if !best_effort && !unread_keys.is_empty() {
        batches.clear();
        keys.clear();
    }
    Ok(BackfillCreates {
        batches,
        keys,
        unread_keys,
    })
}

fn concat_listing_batches(
    schema: &SchemaRef,
    batches: Vec<RecordBatch>,
) -> std::result::Result<RecordBatch, StreamError> {
    if batches.is_empty() {
        return Ok(RecordBatch::new_empty(Arc::clone(schema)));
    }
    if batches.len() == 1 {
        let Some(batch) = batches.into_iter().next() else {
            return Ok(RecordBatch::new_empty(Arc::clone(schema)));
        };
        return Ok(batch);
    }
    concat_batches(schema, &batches).map_err(|error| StreamError::Arrow(error.to_string()))
}

fn listing_rebuild_envelope(
    schema: &SchemaRef,
    applied: &Arc<Mutex<AppliedKeySet>>,
    keys: Vec<String>,
    batches: Vec<RecordBatch>,
    inner: Box<dyn CommitChange + Send + Sync>,
) -> std::result::Result<ChangeEnvelope, StreamError> {
    let data = concat_listing_batches(schema, batches)?;
    let batch = wrap_data_as_change_batch(schema, &data)?.with_rebuild_from_this_batch(true);
    let generation = applied.lock().replace_in_flight(keys.clone());
    Ok(ChangeEnvelope::from_parts(
        applied_keys_committer(applied, generation, keys, inner),
        batch,
        false,
        true,
    ))
}

fn listing_rebuild_from_objects(
    schema: &SchemaRef,
    applied: &Arc<Mutex<AppliedKeySet>>,
    listed: BackfillCreates,
    inner: Box<dyn CommitChange + Send + Sync>,
) -> std::result::Result<Option<ChangeEnvelope>, StreamError> {
    if !listed.is_complete() {
        return Ok(None);
    }
    listing_rebuild_envelope(schema, applied, listed.keys, listed.batches, inner).map(Some)
}

#[expect(
    clippy::too_many_arguments,
    reason = "startup snapshot and restart replace share one complete-listing retry"
)]
async fn retry_until_complete_listing(
    dataset: &DatasetSpec,
    config: &S3ChangesConfig,
    listing_files: &ListingFileFilter,
    table_schema: &SchemaRef,
    object_lister: &dyn ObjectLister,
    object_reader: &dyn ObjectReader,
    applied_keys: &Mutex<AppliedKeySet>,
    scope_prefix: &str,
    pass: &'static str,
    skip_known: bool,
    epoch: u64,
) -> std::result::Result<Option<BackfillCreates>, StreamError> {
    let mut backoff = LISTING_RETRY_BACKOFF;
    loop {
        if shutdown_epoch() != epoch {
            return Ok(None);
        }
        match apply_unapplied_objects(
            dataset,
            config,
            listing_files,
            table_schema,
            object_lister,
            object_reader,
            applied_keys,
            scope_prefix,
            pass,
            skip_known,
            false,
        )
        .await
        {
            Ok(listed) if listed.is_complete() => return Ok(Some(listed)),
            Ok(listed) => {
                tracing::warn!(
                    "{}",
                    incomplete_listing_warning(
                        &dataset.name,
                        pass,
                        &listed.unread_keys,
                        "that pass was not applied and will retry. The dataset will not be marked ready and the accelerator will not be overwritten from a partial listing"
                    )
                );
            }
            Err(error) => {
                tracing::warn!(
                    "Dataset '{}' failed to list objects during {pass}, so that pass was not applied and will retry. The dataset will not be marked ready. Cause: {error}. See: {S3_DOCS}",
                    dataset.name
                );
            }
        }
        sleep(backoff).await;
        backoff = (backoff * 2).min(LISTING_RETRY_BACKOFF_CAP);
    }
}

/// One write, one applied-key/SQS commit, for every batch that belongs to the
/// same snapshot / notification / backfill pass.
///
/// The consumer's default `max_coalesce_age_ms` of 0 applies the first buffered
/// envelope immediately, and `max_coalesced_envelopes` / `max_coalesced_bytes`
/// can split later envelopes into their own writes. A committer on only the last
/// envelope then leaves earlier writes durable when a later write fails, aborts
/// the key, and leaves the SQS message unacked — a retry appends those rows
/// again. Concatenate first, matching [`listing_rebuild_envelope`].
fn concat_object_envelopes(
    schema: &SchemaRef,
    batches: Vec<RecordBatch>,
    is_dataset_ready: bool,
    applied: &Arc<Mutex<AppliedKeySet>>,
    keys: Vec<String>,
    inner: Box<dyn CommitChange + Send + Sync>,
) -> std::result::Result<Vec<ChangeEnvelope>, StreamError> {
    let generation = applied.lock().generation();
    if batches.is_empty() {
        applied.lock().commit(generation, &keys);
        return Ok(Vec::new());
    }
    let data = concat_listing_batches(schema, batches)?;
    let change_batch = wrap_data_as_change_batch(schema, &data)?;
    let committer = applied_keys_committer(applied, generation, keys.clone(), inner);
    applied.lock().mark_in_flight(keys);
    Ok(vec![ChangeEnvelope::new(
        committer,
        change_batch,
        is_dataset_ready,
    )])
}

fn create_envelopes(
    schema: &SchemaRef,
    batches: Vec<RecordBatch>,
    applied: &Arc<Mutex<AppliedKeySet>>,
    keys: Vec<String>,
    queue: &Arc<dyn MessageQueue>,
    receipt_handle: &str,
) -> std::result::Result<Vec<ChangeEnvelope>, StreamError> {
    concat_object_envelopes(
        schema,
        batches,
        true,
        applied,
        keys,
        Box::new(SqsDeleteCommitter {
            queue: Arc::clone(queue),
            receipt_handle: receipt_handle.to_string(),
        }),
    )
}

fn backfill_envelopes(
    schema: &SchemaRef,
    batches: Vec<RecordBatch>,
    is_dataset_ready: bool,
    applied: &Arc<Mutex<AppliedKeySet>>,
    keys: Vec<String>,
) -> std::result::Result<Vec<ChangeEnvelope>, StreamError> {
    concat_object_envelopes(
        schema,
        batches,
        is_dataset_ready,
        applied,
        keys,
        Box::new(NoOpCommitter),
    )
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
            listing_files,
        } = parts;
        let epoch = shutdown_epoch();
        let schema = federated_table.table_provider().await.schema();
        let applied_keys = Arc::new(Mutex::new(AppliedKeySet::default()));

        if acceleration.is_provably_empty() {
            tracing::info!(
                "Dataset '{}' is starting S3 event capture from an empty accelerator, so existing objects under {} will be snapshotted before SQS events are applied. See: {S3_DOCS}",
                dataset.name,
                prefix_display(&config.bucket, &config.key_prefix)
            );
            // One listing is both the snapshot and the applied-key manifest.
            // A later listing can include objects that were never read here; those
            // must stay eligible for the completeness backfill. A partial read
            // is not a snapshot: unread keys stay eligible and ready waits.
            if let Some(snapshot) = retry_until_complete_listing(
                &dataset,
                &config,
                &listing_files,
                &schema,
                object_lister.as_ref(),
                object_reader.as_ref(),
                applied_keys.as_ref(),
                &config.dataset_prefix,
                "the empty-accelerator snapshot",
                true,
                epoch,
            )
            .await?
            {
                let envelopes = backfill_envelopes(
                    &schema,
                    snapshot.batches,
                    false,
                    &applied_keys,
                    snapshot.keys,
                )?;
                for envelope in envelopes {
                    yield envelope;
                }
                yield build_ready_signal_envelope(&schema)?;
            }
        } else {
            tracing::info!(
                "Dataset '{}' is starting S3 event capture with a non-empty accelerator, so the accelerator will be replaced from the listing prefix {} before SQS events are applied. An in-memory applied-key set cannot prove which objects are already present after a restart. See: {S3_DOCS}",
                dataset.name,
                prefix_display(&config.bucket, &config.key_prefix)
            );
            // One listing is both the replacement rows and the applied-key
            // manifest. A later federated scan can include objects this listing
            // never read; those must stay eligible for the completeness backfill.
            // A partial read is not a replace: do not overwrite from a subset.
            if let Some(listed) = retry_until_complete_listing(
                &dataset,
                &config,
                &listing_files,
                &schema,
                object_lister.as_ref(),
                object_reader.as_ref(),
                applied_keys.as_ref(),
                &config.dataset_prefix,
                "the non-empty restart replace",
                false,
                epoch,
            )
            .await?
            {
                if let Some(envelope) = listing_rebuild_from_objects(
                    &schema,
                    &applied_keys,
                    listed,
                    Box::new(NoOpCommitter),
                )? {
                    yield envelope;
                }
                yield build_ready_signal_envelope(&schema)?;
            }
        }

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
                        "Dataset '{}' failed to long-poll SQS for S3 event notifications, so those notifications will not be applied until the next successful poll. The listing backfill still runs on `s3_changes_backfill_interval`. Cause: {error}. Check `s3_changes_queue_url` and SQS permissions. See: {S3_DOCS}",
                        dataset.name
                    );
                    sleep(receive_backoff).await;
                    receive_backoff = (receive_backoff * 2).min(RECEIVE_ERROR_BACKOFF_CAP);
                    Vec::new()
                }
            };

            for message in messages {
                if shutdown_epoch() != epoch {
                    break;
                }
                match process_message(&dataset, &config, &listing_files, &schema, object_reader.as_ref(), applied_keys.as_ref(), &message).await {
                    ProcessOutcome::Creates { batches, keys, receipt_handle } => {
                        match create_envelopes(&schema, batches, &applied_keys, keys, &queue, &receipt_handle) {
                            Ok(envelopes) => {
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
                        let sqs: Box<dyn CommitChange + Send + Sync> = Box::new(SqsDeleteCommitter {
                            queue: Arc::clone(&queue),
                            receipt_handle,
                        });
                        match apply_unapplied_objects(
                            &dataset,
                            &config,
                            &listing_files,
                            &schema,
                            object_lister.as_ref(),
                            object_reader.as_ref(),
                            applied_keys.as_ref(),
                            &config.dataset_prefix,
                            "an ObjectRemoved rebuild",
                            false,
                            false,
                        )
                        .await
                        {
                            Ok(listed) if listed.is_complete() => {
                                if let Some(envelope) = listing_rebuild_from_objects(
                                    &schema,
                                    &applied_keys,
                                    listed,
                                    sqs,
                                )? {
                                    yield envelope;
                                }
                            }
                            Ok(listed) => {
                                tracing::warn!(
                                    "{}",
                                    incomplete_listing_warning(
                                        &dataset.name,
                                        "an ObjectRemoved rebuild",
                                        &listed.unread_keys,
                                        "the accelerator was not overwritten and the SQS message was left on the queue (not deleted)"
                                    )
                                );
                            }
                            Err(error) => {
                                tracing::warn!(
                                    "Dataset '{}' could not list the S3 objects for an ObjectRemoved rebuild, so the accelerator was not overwritten and the SQS message was left on the queue (not deleted). Cause: {error}. See: {S3_DOCS}",
                                    dataset.name
                                );
                            }
                        }
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
                match apply_unapplied_objects(
                    &dataset,
                    &config,
                    &listing_files,
                    &schema,
                    object_lister.as_ref(),
                    object_reader.as_ref(),
                    applied_keys.as_ref(),
                    &config.key_prefix,
                    "a listing backfill",
                    true,
                    true,
                )
                .await
                {
                    Ok(backfill) => {
                        match backfill_envelopes(
                            &schema,
                            backfill.batches,
                            true,
                            &applied_keys,
                            backfill.keys,
                        ) {
                            Ok(envelopes) => {
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
    use datafusion::datasource::TableProvider;
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

    struct TransientFailReader {
        inner: MapObjectReader,
        remaining: Mutex<HashMap<String, usize>>,
    }

    #[async_trait]
    impl ObjectReader for TransientFailReader {
        async fn read_object(
            &self,
            bucket: &str,
            key: &str,
        ) -> std::result::Result<Vec<RecordBatch>, StreamError> {
            let id = format!("{bucket}/{key}");
            {
                let mut remaining = self.remaining.lock().await;
                if let Some(count) = remaining.get_mut(&id)
                    && *count > 0
                {
                    *count -= 1;
                    return Err(StreamError::External(format!(
                        "simulated transient read failure for s3://{id}"
                    )));
                }
            }
            self.inner.read_object(bucket, key).await
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

    struct SequenceLister {
        listings: Mutex<Vec<Vec<String>>>,
    }

    #[async_trait]
    impl ObjectLister for SequenceLister {
        async fn list_keys(&self) -> std::result::Result<Vec<String>, StreamError> {
            let mut listings = self.listings.lock().await;
            if listings.is_empty() {
                return Ok(Vec::new());
            }
            Ok(listings.remove(0))
        }
    }

    /// Delivers one batch of messages per `receive`, then parks — the shape of an
    /// SQS redelivery arriving after an earlier batch has been processed.
    struct SequenceQueue {
        batches: Mutex<Vec<Vec<QueueMessage>>>,
    }

    #[async_trait]
    impl MessageQueue for SequenceQueue {
        async fn receive(&self) -> std::result::Result<Vec<QueueMessage>, QueueError> {
            let batch = {
                let mut batches = self.batches.lock().await;
                if batches.is_empty() {
                    None
                } else {
                    Some(batches.remove(0))
                }
            };
            if let Some(messages) = batch {
                Ok(messages)
            } else {
                std::future::pending::<()>().await;
                Ok(Vec::new())
            }
        }

        async fn delete(&self, _receipt_handle: &str) -> std::result::Result<(), QueueError> {
            Ok(())
        }
    }

    struct FailingQueue {
        remaining_failures: Mutex<usize>,
    }

    #[async_trait]
    impl MessageQueue for FailingQueue {
        async fn receive(&self) -> std::result::Result<Vec<QueueMessage>, QueueError> {
            let mut remaining = self.remaining_failures.lock().await;
            if *remaining > 0 {
                *remaining -= 1;
                return Err(QueueError::Receive {
                    source: "simulated SQS receive failure".into(),
                });
            }
            std::future::pending::<()>().await;
            Ok(Vec::new())
        }

        async fn delete(&self, _receipt_handle: &str) -> std::result::Result<(), QueueError> {
            Ok(())
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
            dataset_prefix: "events/".to_string(),
            key_prefix: "events/".to_string(),
            backfill_interval: Duration::from_hours(1),
        }
    }

    fn parquet_files() -> ListingFileFilter {
        ListingFileFilter {
            extension: ".parquet".to_string(),
        }
    }

    fn empty_lister() -> Arc<dyn ObjectLister> {
        Arc::new(MockLister { keys: vec![] })
    }

    fn applied_mutex(
        committed: impl IntoIterator<Item = String>,
    ) -> parking_lot::Mutex<AppliedKeySet> {
        let mut set = AppliedKeySet::default();
        let keys: Vec<String> = committed.into_iter().collect();
        let generation = set.generation();
        set.commit(generation, &keys);
        parking_lot::Mutex::new(set)
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
            listing_files: parquet_files(),
        })
    }

    async fn next_envelope(
        stream: &mut ChangesStream,
        timeout: Duration,
    ) -> Option<ChangeEnvelope> {
        match tokio::time::timeout(timeout, stream.next()).await {
            Ok(Some(Ok(envelope))) => Some(envelope),
            Ok(Some(Err(error))) => panic!("change stream error: {error}"),
            Ok(None) | Err(_) => None,
        }
    }

    async fn collect_until_idle(stream: ChangesStream, expected: usize) -> Vec<ChangeEnvelope> {
        let mut stream = stream;
        let mut envelopes = Vec::new();
        while envelopes.len() < expected {
            match tokio::time::timeout(Duration::from_secs(2), stream.next()).await {
                Ok(Some(Ok(envelope))) => envelopes.push(envelope),
                Ok(Some(Err(error))) => panic!("change stream error: {error}"),
                Ok(None) | Err(_) => break,
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
        assert_eq!(
            region_from_queue_url(
                "https://sqs-fips.us-east-1.amazonaws.com/123456789012/s3-events"
            )
            .as_deref(),
            Some("us-east-1")
        );
        assert_eq!(
            region_from_queue_url(
                "https://sqs.us-west-2.vpce.amazonaws.com/123456789012/s3-events"
            )
            .as_deref(),
            Some("us-west-2")
        );
        assert_eq!(
            region_from_queue_url(
                "https://vpce-abc.sqs.us-west-2.vpce.amazonaws.com/123456789012/s3-events"
            )
            .as_deref(),
            Some("us-west-2")
        );
    }

    #[tokio::test]
    async fn validate_accepts_queue_url_with_changes() {
        let params = test_params(vec![
            ("s3_changes_queue_url", QUEUE_URL),
            ("s3_auth", "iam_role"),
            ("file_format", "parquet"),
        ])
        .await;
        let config = S3ChangesConfig::try_from_params(&params, &events_dataset())
            .expect("valid changes config")
            .expect("changes should be enabled");
        assert_eq!(config.bucket, "my-bucket");
        assert_eq!(config.dataset_prefix, "events/");
        assert_eq!(config.key_prefix, "events/");
        assert_eq!(config.region, "us-east-1");
        assert_eq!(config.on_object_removed, OnObjectRemoved::Ignore);
        assert_eq!(config.backfill_interval, Duration::from_hours(1));
    }

    #[tokio::test]
    async fn validate_resolves_region_from_a_fips_queue_url() {
        let params = test_params(vec![
            (
                "s3_changes_queue_url",
                "https://sqs-fips.us-east-1.amazonaws.com/123456789012/s3-events",
            ),
            ("s3_auth", "iam_role"),
            ("file_format", "parquet"),
        ])
        .await;
        let config = S3ChangesConfig::try_from_params(&params, &events_dataset())
            .expect("FIPS queue URL is valid")
            .expect("changes should be enabled");
        assert_eq!(config.region, "us-east-1");
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
    async fn validate_rejects_a_non_url_queue_value() {
        let params = test_params(vec![
            ("s3_changes_queue_url", "not-a-url"),
            ("s3_auth", "iam_role"),
            ("s3_changes_region", "us-east-1"),
            ("file_format", "parquet"),
        ])
        .await;
        let error = S3ChangesConfig::try_from_params(&params, &events_dataset())
            .expect_err("non-URL queue values must be refused");
        let message = error.to_string();
        assert!(
            message.contains("not an SQS queue URL"),
            "must reject non-URL queue values, got: {message}"
        );
        assert!(
            !message.contains("not-a-url"),
            "`s3_changes_queue_url` is secret; the error must not interpolate the value, got: {message}"
        );
    }

    #[test]
    fn queue_url_not_http_error_does_not_include_the_secret_value() {
        let error = Error::QueueUrlNotHttp {
            dataset_name: "events".into(),
        };
        let message = error.to_string();
        assert!(
            message.contains("s3_changes_queue_url") && message.contains("not an SQS queue URL"),
            "must name the param and the problem, got: {message}"
        );
        assert!(
            !message.contains("TOP-SECRET")
                && !message.contains("123456789012")
                && !message.contains("private-events")
                && !message.contains("sqs://"),
            "must not interpolate the configured queue value, got: {message}"
        );
    }

    #[test]
    fn is_sqs_queue_url_accepts_aws_partition_urls() {
        assert!(is_sqs_queue_url(QUEUE_URL));
        assert!(is_sqs_queue_url(
            "https://sqs.cn-north-1.amazonaws.com.cn/123456789012/s3-events"
        ));
        assert!(is_sqs_queue_url(
            "https://sqs-fips.us-east-1.amazonaws.com/123456789012/s3-events"
        ));
        assert!(is_sqs_queue_url(
            "https://sqs.us-east-1.vpce.amazonaws.com/123456789012/s3-events"
        ));
        assert!(is_sqs_queue_url(
            "https://vpce-abc.sqs.us-east-1.vpce.amazonaws.com/123456789012/s3-events"
        ));
        assert!(is_sqs_queue_url(
            "https://sqs.us-east-1.amazonaws.com/123456789012/s3-events.fifo"
        ));
    }

    /// Scheme-only validation accepted loopback and instance-metadata URLs
    /// (Copilot reproduction on #14121). Those must fail closed at registration.
    #[test]
    fn is_sqs_queue_url_rejects_non_sqs_hosts() {
        assert!(!is_sqs_queue_url("https://127.0.0.1/admin"));
        assert!(!is_sqs_queue_url("http://169.254.169.254/latest/meta-data"));
        assert!(!is_sqs_queue_url(
            "https://localhost:4566/000000000000/queue"
        ));
        assert!(!is_sqs_queue_url(
            "http://sqs.us-east-1.amazonaws.com/123456789012/s3-events"
        ));
        assert!(!is_sqs_queue_url(
            "https://example.com/123456789012/s3-events"
        ));
        assert!(!is_sqs_queue_url(
            "https://sqs.us-east-1.amazonaws.com/123/s3-events"
        ));
    }

    #[tokio::test]
    async fn validate_rejects_a_non_sqs_queue_host() {
        let params = test_params(vec![
            ("s3_changes_queue_url", "https://127.0.0.1/admin"),
            ("s3_auth", "iam_role"),
            ("s3_changes_region", "us-east-1"),
            ("file_format", "parquet"),
        ])
        .await;
        let error = S3ChangesConfig::try_from_params(&params, &events_dataset())
            .expect_err("loopback is not an SQS queue URL");
        let message = error.to_string();
        assert!(
            message.contains("not an SQS queue URL"),
            "must reject a non-SQS host, got: {message}"
        );
        assert!(
            !message.contains("127.0.0.1") && !message.contains("admin"),
            "`s3_changes_queue_url` is secret; the error must not interpolate the value, got: {message}"
        );
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
    async fn validate_rejects_unstructured_text_without_file_format() {
        let params = test_params(vec![
            ("s3_changes_queue_url", QUEUE_URL),
            ("s3_auth", "iam_role"),
        ])
        .await;
        let error = S3ChangesConfig::try_from_params(&params, &events_dataset())
            .expect_err("unstructured text must fail closed for changes mode");
        let message = error.to_string();
        assert!(
            message.contains("unstructured text"),
            "must name unstructured text, got: {message}"
        );
        assert!(
            message.contains("file_format"),
            "must tell the operator to set file_format, got: {message}"
        );
    }

    #[tokio::test]
    async fn validate_accepts_structured_file_format() {
        let params = test_params(vec![
            ("s3_changes_queue_url", QUEUE_URL),
            ("s3_auth", "iam_role"),
            ("file_format", "parquet"),
        ])
        .await;
        S3ChangesConfig::try_from_params(&params, &events_dataset())
            .expect("parquet changes config")
            .expect("changes enabled");
    }

    #[tokio::test]
    async fn validate_accepts_a_structured_file_extension_without_file_format() {
        let params = test_params(vec![
            ("s3_changes_queue_url", QUEUE_URL),
            ("s3_auth", "iam_role"),
            ("file_extension", ".parquet"),
        ])
        .await;
        S3ChangesConfig::try_from_params(&params, &events_dataset())
            .expect("file_extension .parquet is structured, matching listing inference")
            .expect("changes enabled");
    }

    #[tokio::test]
    async fn validate_accepts_a_compressed_file_extension_without_file_format() {
        let params = test_params(vec![
            ("s3_changes_queue_url", QUEUE_URL),
            ("s3_auth", "iam_role"),
            ("file_extension", ".parquet.gz"),
        ])
        .await;
        S3ChangesConfig::try_from_params(&params, &events_dataset())
            .expect("file_extension .parquet.gz is structured")
            .expect("changes enabled");
    }

    #[tokio::test]
    async fn validate_accepts_a_compressed_from_path_without_file_format() {
        let mut dataset = events_dataset();
        dataset.from = "s3://my-bucket/events/part.parquet.gz".to_string();
        let params = test_params(vec![
            ("s3_changes_queue_url", QUEUE_URL),
            ("s3_auth", "iam_role"),
        ])
        .await;
        S3ChangesConfig::try_from_params(&params, &dataset)
            .expect("a .parquet.gz from path is structured, matching listing inference")
            .expect("changes enabled");
    }

    #[tokio::test]
    async fn validate_accepts_orc_file_format() {
        let params = test_params(vec![
            ("s3_changes_queue_url", QUEUE_URL),
            ("s3_auth", "iam_role"),
            ("file_format", "orc"),
        ])
        .await;
        S3ChangesConfig::try_from_params(&params, &events_dataset())
            .expect("orc is a listing FileFormat")
            .expect("changes enabled");
    }

    #[tokio::test]
    async fn validate_accepts_orc_file_extension_without_file_format() {
        let params = test_params(vec![
            ("s3_changes_queue_url", QUEUE_URL),
            ("s3_auth", "iam_role"),
            ("file_extension", ".orc"),
        ])
        .await;
        S3ChangesConfig::try_from_params(&params, &events_dataset())
            .expect("file_extension .orc is structured, matching listing inference")
            .expect("changes enabled");
    }

    #[tokio::test]
    async fn validate_accepts_orc_from_path_without_file_format() {
        let mut dataset = events_dataset();
        dataset.from = "s3://my-bucket/events/part.orc".to_string();
        let params = test_params(vec![
            ("s3_changes_queue_url", QUEUE_URL),
            ("s3_auth", "iam_role"),
        ])
        .await;
        S3ChangesConfig::try_from_params(&params, &dataset)
            .expect("a .orc from path is structured, matching listing inference")
            .expect("changes enabled");
    }

    #[cfg(not(windows))]
    #[tokio::test]
    async fn validate_accepts_vortex_file_format() {
        let params = test_params(vec![
            ("s3_changes_queue_url", QUEUE_URL),
            ("s3_auth", "iam_role"),
            ("file_format", "vortex"),
        ])
        .await;
        S3ChangesConfig::try_from_params(&params, &events_dataset())
            .expect("vortex is a listing FileFormat on non-Windows")
            .expect("changes enabled");
    }

    #[cfg(not(windows))]
    #[tokio::test]
    async fn validate_accepts_vortex_file_extension_without_file_format() {
        let params = test_params(vec![
            ("s3_changes_queue_url", QUEUE_URL),
            ("s3_auth", "iam_role"),
            ("file_extension", ".vortex"),
        ])
        .await;
        S3ChangesConfig::try_from_params(&params, &events_dataset())
            .expect("file_extension .vortex is structured, matching listing inference")
            .expect("changes enabled");
    }

    #[test]
    fn listing_structured_formats_are_accepted_for_changes() {
        for name in [
            "parquet", "csv", "json", "tsv", "jsonl", "ndjson", "ldjson", "orc",
        ] {
            assert!(
                is_structured_listing_format(name),
                "{name} is a listing FileFormat and must pass changes validation"
            );
        }
        #[cfg(not(windows))]
        assert!(
            is_structured_listing_format("vortex"),
            "vortex is a listing FileFormat on non-Windows"
        );
        assert!(
            !is_structured_listing_format("txt"),
            "txt remains unstructured text"
        );
        assert!(
            !is_structured_listing_format("auto"),
            "file_format: auto is not itself structured"
        );
    }

    #[tokio::test]
    async fn validate_rejects_an_unstructured_file_extension() {
        let params = test_params(vec![
            ("s3_changes_queue_url", QUEUE_URL),
            ("s3_auth", "iam_role"),
            ("file_extension", ".txt"),
        ])
        .await;
        let error = S3ChangesConfig::try_from_params(&params, &events_dataset())
            .expect_err("file_extension .txt is unstructured text");
        let message = error.to_string();
        assert!(
            message.contains("unstructured text"),
            "must name unstructured text, got: {message}"
        );
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
    async fn validate_rejects_a_glob_from() {
        let params = test_params(vec![
            ("s3_changes_queue_url", QUEUE_URL),
            ("s3_auth", "iam_role"),
            ("file_format", "parquet"),
        ])
        .await;
        let mut dataset = DatasetSpec::new("s3://my-bucket/events/year=*/", "events".into());
        dataset.acceleration = Some(Acceleration {
            refresh_mode: Some(RefreshMode::Changes),
            ..Acceleration::default()
        });
        let error = S3ChangesConfig::try_from_params(&params, &dataset)
            .expect_err("a glob has no prefix to derive");
        let message = error.to_string();
        assert!(
            message.contains("wildcard") && message.contains("s3_changes_key_prefix"),
            "the error must say what is wrong and what to do instead, got: {message}"
        );
    }

    /// Only [`object_store::Error::NotFound`] may be treated as "this `from` is a
    /// prefix". Every other HEAD failure must propagate, or an empty accelerator
    /// can be marked ready after an auth/timeout/transient miss.
    fn head_result_means_from_names_an_object(
        result: Result<object_store::ObjectMeta, object_store::Error>,
    ) -> Result<bool, object_store::Error> {
        match result {
            Ok(_) => Ok(true),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(error) => Err(error),
        }
    }

    #[test]
    fn only_not_found_head_allows_a_prefix_from() {
        let meta = object_store::ObjectMeta {
            location: ObjectPath::from("events/part.parquet"),
            last_modified: chrono::Utc::now(),
            size: 1,
            e_tag: None,
            version: None,
        };
        assert!(
            head_result_means_from_names_an_object(Ok(meta)).expect("Ok is an object"),
            "a successful HEAD means `from` names an object"
        );
        assert!(
            !head_result_means_from_names_an_object(Err(object_store::Error::NotFound {
                path: "events/part.parquet".into(),
                source: Box::new(std::io::Error::new(std::io::ErrorKind::NotFound, "missing",)),
            }))
            .expect("NotFound is a prefix"),
            "NotFound alone may continue as a prefix"
        );
        let err = head_result_means_from_names_an_object(Err(object_store::Error::Generic {
            store: "S3",
            source: Box::new(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "request timed out",
            )),
        }))
        .expect_err("timeout must not look like a prefix");
        assert!(
            matches!(err, object_store::Error::Generic { .. }),
            "non-NotFound HEAD errors must propagate, got {err:?}"
        );
    }

    /// A `from` that names one object derives the prefix `<key>/`, which nothing
    /// is under: the snapshot would mark an empty accelerator ready and every
    /// notification for the object itself would count as outside the dataset.
    /// Only the object store can tell the two apart, so the refusal needs the
    /// key this returns.
    #[test]
    fn a_from_without_a_trailing_slash_may_name_an_object() {
        let object = DatasetSpec::new("s3://my-bucket/events/part-00000.parquet", "events".into());
        assert_eq!(
            key_from_may_name(&object).as_deref(),
            Some("events/part-00000.parquet")
        );

        let prefix = DatasetSpec::new("s3://my-bucket/events/", "events".into());
        assert_eq!(
            key_from_may_name(&prefix),
            None,
            "a trailing slash marks a prefix, so there is nothing to check"
        );

        let bucket_root = DatasetSpec::new("s3://my-bucket", "events".into());
        assert_eq!(key_from_may_name(&bucket_root), None);
    }

    /// `DatasetSpec::path()` keeps URI escapes, but notification keys are
    /// decoded. Matching the raw `from` path would leave every object under an
    /// encoded prefix on the queue forever. Regression test for the Copilot
    /// finding on #14121.
    #[test]
    fn an_encoded_from_prefix_matches_a_decoded_notification_key() {
        let dataset = DatasetSpec::new("s3://my-bucket/events/data%20files/", "events".into());
        let (bucket, dataset_prefix) =
            bucket_and_key_prefix(&dataset).expect("encoded from is a valid prefix");
        assert_eq!(bucket, "my-bucket");
        assert_eq!(dataset_prefix, "events/data files/");
        assert_eq!(
            key_from_may_name(&DatasetSpec::new(
                "s3://my-bucket/events/data%20files/part.parquet",
                "events".into()
            ))
            .as_deref(),
            Some("events/data files/part.parquet")
        );

        let events = parse_notification_body(&created_put_body("events/data%20files/part.parquet"))
            .expect("valid notification");
        assert_eq!(events[0].key, "events/data files/part.parquet");
        assert!(
            matches_dataset(&events[0], &bucket, &dataset_prefix),
            "decoded notification key must match the decoded from prefix, prefix={dataset_prefix:?} key={:?}",
            events[0].key
        );
    }

    #[test]
    fn a_from_path_keeps_a_literal_plus() {
        let dataset = DatasetSpec::new("s3://my-bucket/events/foo+bar/", "events".into());
        let (_, prefix) = bucket_and_key_prefix(&dataset).expect("plus is a valid path character");
        assert_eq!(prefix, "events/foo+bar/");
    }

    #[tokio::test]
    async fn validate_rejects_prefix_outside_dataset() {
        let params = test_params(vec![
            ("s3_changes_queue_url", QUEUE_URL),
            ("s3_changes_key_prefix", "other/"),
            ("file_format", "parquet"),
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
            ("file_format", "parquet"),
        ])
        .await;
        let config = S3ChangesConfig::try_from_params(&params, &events_dataset())
            .expect("nested prefix is valid")
            .expect("changes enabled");
        assert_eq!(config.dataset_prefix, "events/");
        assert_eq!(config.key_prefix, "events/year=2026/");
        assert_eq!(config.on_object_removed, OnObjectRemoved::Rebuild);
        assert_eq!(config.backfill_interval, Duration::from_mins(30));
    }

    #[tokio::test]
    async fn validate_decodes_a_percent_encoded_from_prefix() {
        let params = test_params(vec![
            ("s3_changes_queue_url", QUEUE_URL),
            ("file_format", "parquet"),
        ])
        .await;
        let mut dataset = DatasetSpec::new("s3://my-bucket/events/data%20files/", "events".into());
        dataset.acceleration = Some(Acceleration {
            refresh_mode: Some(RefreshMode::Changes),
            ..Acceleration::default()
        });
        let config = S3ChangesConfig::try_from_params(&params, &dataset)
            .expect("encoded from is valid")
            .expect("changes enabled");
        assert_eq!(config.dataset_prefix, "events/data files/");
        assert_eq!(config.key_prefix, "events/data files/");

        let nested = test_params(vec![
            ("s3_changes_queue_url", QUEUE_URL),
            ("s3_changes_key_prefix", "events/data files/2026"),
            ("file_format", "parquet"),
        ])
        .await;
        let nested_config = S3ChangesConfig::try_from_params(&nested, &dataset)
            .expect("decoded nested prefix is under the decoded from")
            .expect("changes enabled");
        assert_eq!(nested_config.key_prefix, "events/data files/2026/");
    }

    #[tokio::test]
    async fn validate_rejects_zero_backfill_interval() {
        let params = test_params(vec![
            ("s3_changes_queue_url", QUEUE_URL),
            ("s3_changes_backfill_interval", "0s"),
            ("file_format", "parquet"),
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
            &parquet_files(),
            &id_name_schema(),
            &reader,
            &applied_mutex([]),
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
    async fn process_created_skips_already_applied_key() {
        let reader = MapObjectReader {
            objects: HashMap::from([(
                "my-bucket/events/a.parquet".to_string(),
                vec![id_name_batch(&[1], &["a"])],
            )]),
            fail_keys: vec![],
        };
        let applied = applied_mutex(["events/a.parquet".to_string()]);
        let outcome = process_message(
            &events_dataset(),
            &default_config(),
            &parquet_files(),
            &id_name_schema(),
            &reader,
            &applied,
            &QueueMessage {
                body: created_put_body("events/a.parquet"),
                receipt_handle: "rh-dup".into(),
            },
        )
        .await;
        assert!(
            matches!(outcome, ProcessOutcome::Ack { .. }),
            "a queued ObjectCreated for a committed key must not append again, got {outcome:?}"
        );
    }

    #[tokio::test]
    async fn process_created_leaves_in_flight_key() {
        let reader = MapObjectReader {
            objects: HashMap::from([(
                "my-bucket/events/a.parquet".to_string(),
                vec![id_name_batch(&[1], &["a"])],
            )]),
            fail_keys: vec![],
        };
        let applied = parking_lot::Mutex::new(AppliedKeySet::default());
        applied
            .lock()
            .mark_in_flight(["events/a.parquet".to_string()]);
        let outcome = process_message(
            &events_dataset(),
            &default_config(),
            &parquet_files(),
            &id_name_schema(),
            &reader,
            &applied,
            &QueueMessage {
                body: created_put_body("events/a.parquet"),
                receipt_handle: "rh-inflight".into(),
            },
        )
        .await;
        assert!(
            matches!(outcome, ProcessOutcome::Leave),
            "an in-flight ObjectCreated must stay on the queue until apply commits, got {outcome:?}"
        );
    }

    /// One notification whose records name the same key twice must append that
    /// object's rows once. The applied-key set only learns of a key once its
    /// envelope is yielded, so it cannot catch a repeat within the message.
    #[tokio::test]
    async fn process_created_reads_a_key_named_twice_in_one_notification_once() {
        let reader = MapObjectReader {
            objects: HashMap::from([(
                "my-bucket/events/a.parquet".to_string(),
                vec![id_name_batch(&[1], &["a"])],
            )]),
            fail_keys: vec![],
        };
        let outcome = process_message(
            &events_dataset(),
            &default_config(),
            &parquet_files(),
            &id_name_schema(),
            &reader,
            &applied_mutex([]),
            &QueueMessage {
                body: r#"{"Records":[{"eventName":"ObjectCreated:Put","s3":{"bucket":{"name":"my-bucket"},"object":{"key":"events/a.parquet"}}},{"eventName":"ObjectCreated:Put","s3":{"bucket":{"name":"my-bucket"},"object":{"key":"events/a.parquet"}}}]}"#.into(),
                receipt_handle: "rh-twice".into(),
            },
        )
        .await;
        match outcome {
            ProcessOutcome::Creates { batches, keys, .. } => {
                assert_eq!(keys, vec!["events/a.parquet".to_string()]);
                let rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
                assert_eq!(
                    rows, 1,
                    "a key named twice in one notification must be appended once, got {rows} rows"
                );
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
            &parquet_files(),
            &id_name_schema(),
            &reader,
            &applied_mutex([]),
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
            &parquet_files(),
            &id_name_schema(),
            &reader,
            &applied_mutex([]),
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
            &parquet_files(),
            &id_name_schema(),
            &reader,
            &applied_mutex([]),
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
            &parquet_files(),
            &id_name_schema(),
            &reader,
            &applied_mutex([]),
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
            &parquet_files(),
            &id_name_schema(),
            &reader,
            &applied_mutex([]),
            &QueueMessage {
                body: "not-json".into(),
                receipt_handle: "rh-poison".into(),
            },
        )
        .await;
        assert!(matches!(poison, ProcessOutcome::Ack { .. }));
    }

    /// A job marker such as `_SUCCESS` is under the dataset prefix but is not an
    /// object the listing table reads: its notification is acknowledged rather
    /// than read (which fails and retries the message), and removing it does not
    /// rebuild the accelerator.
    #[tokio::test]
    async fn process_acks_notifications_for_objects_the_listing_table_does_not_read() {
        let reader = MapObjectReader {
            objects: HashMap::new(),
            fail_keys: vec![],
        };
        let created = process_message(
            &events_dataset(),
            &default_config(),
            &parquet_files(),
            &id_name_schema(),
            &reader,
            &applied_mutex([]),
            &QueueMessage {
                body: created_put_body("events/_SUCCESS"),
                receipt_handle: "rh-marker".into(),
            },
        )
        .await;
        assert!(
            matches!(created, ProcessOutcome::Ack { .. }),
            "an ObjectCreated for a job marker must be acknowledged, not read and retried, got {created:?}"
        );

        let mut config = default_config();
        config.on_object_removed = OnObjectRemoved::Rebuild;
        let removed = process_message(
            &events_dataset(),
            &config,
            &parquet_files(),
            &id_name_schema(),
            &reader,
            &applied_mutex([]),
            &QueueMessage {
                body: removed_body("events/_SUCCESS"),
                receipt_handle: "rh-marker-removed".into(),
            },
        )
        .await;
        assert!(
            matches!(removed, ProcessOutcome::Ack { .. }),
            "removing an object the dataset never read must not rebuild the accelerator, got {removed:?}"
        );
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
            &parquet_files(),
            &id_name_schema(),
            &reader,
            &applied_mutex([]),
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
        let applied = applied_mutex(["events/old.parquet".to_string()]);
        let result = apply_unapplied_objects(
            &events_dataset(),
            &default_config(),
            &parquet_files(),
            &id_name_schema(),
            &lister,
            &reader,
            &applied,
            "events/",
            "a listing backfill",
            true,
            true,
        )
        .await
        .expect("backfill should succeed");
        assert_eq!(result.keys, vec!["events/new.parquet".to_string()]);
        assert_eq!(result.batches.len(), 1);
        assert_eq!(result.batches[0].num_rows(), 1);
        assert!(result.unread_keys.is_empty());
    }

    #[tokio::test]
    async fn apply_unapplied_objects_strict_pass_discards_partial_reads() {
        let reader = MapObjectReader {
            objects: HashMap::from([(
                "my-bucket/events/a.parquet".to_string(),
                vec![id_name_batch(&[1], &["a"])],
            )]),
            fail_keys: vec!["my-bucket/events/b.parquet".into()],
        };
        let lister = MockLister {
            keys: vec!["events/a.parquet".into(), "events/b.parquet".into()],
        };
        let applied = applied_mutex([]);
        let result = apply_unapplied_objects(
            &events_dataset(),
            &default_config(),
            &parquet_files(),
            &id_name_schema(),
            &lister,
            &reader,
            &applied,
            "events/",
            "the empty-accelerator snapshot",
            true,
            false,
        )
        .await
        .expect("strict listing should return the unread set");
        assert!(
            result.keys.is_empty() && result.batches.is_empty(),
            "a strict pass must not apply a subset of the listing, got keys {:?}",
            result.keys
        );
        assert_eq!(result.unread_keys, vec!["events/b.parquet".to_string()]);
        assert!(!result.is_complete());
    }

    #[tokio::test]
    async fn apply_unapplied_objects_best_effort_keeps_readable_objects() {
        let reader = MapObjectReader {
            objects: HashMap::from([(
                "my-bucket/events/a.parquet".to_string(),
                vec![id_name_batch(&[1], &["a"])],
            )]),
            fail_keys: vec!["my-bucket/events/b.parquet".into()],
        };
        let lister = MockLister {
            keys: vec!["events/a.parquet".into(), "events/b.parquet".into()],
        };
        let applied = applied_mutex([]);
        let result = apply_unapplied_objects(
            &events_dataset(),
            &default_config(),
            &parquet_files(),
            &id_name_schema(),
            &lister,
            &reader,
            &applied,
            "events/",
            "a listing backfill",
            true,
            true,
        )
        .await
        .expect("best-effort backfill should succeed");
        assert_eq!(result.keys, vec!["events/a.parquet".to_string()]);
        assert_eq!(result.unread_keys, vec!["events/b.parquet".to_string()]);
        assert_eq!(result.batches.len(), 1);
    }

    #[test]
    fn incomplete_listing_warning_names_dataset_unread_keys_and_impact() {
        let message = incomplete_listing_warning(
            "events",
            "the empty-accelerator snapshot",
            &["events/b.parquet".to_string()],
            "that pass was not applied and will retry. The dataset will not be marked ready and the accelerator will not be overwritten from a partial listing",
        );
        assert!(
            message.contains("Dataset 'events'")
                && message.contains("'events/b.parquet'")
                && message.contains("will not be marked ready")
                && message.contains("will not be overwritten")
                && message.contains(S3_DOCS),
            "warning must name the dataset, unread key, impact, and docs link, got {message}"
        );
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

    fn names_in(envelope: &ChangeEnvelope) -> Vec<String> {
        let batch = envelope.change_batch().expect("change batch").data_batch();
        let names = batch
            .column_by_name("name")
            .expect("name")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("name is Utf8");
        (0..names.len())
            .map(|i| names.value(i).to_string())
            .collect()
    }

    #[tokio::test]
    async fn stream_empty_accelerator_snapshots_then_ready() {
        let queue = Arc::new(MockQueue::with_messages(vec![]));
        let reader = Arc::new(MapObjectReader {
            objects: HashMap::from([(
                "my-bucket/events/snap.parquet".to_string(),
                vec![id_name_batch(&[1], &["snap"])],
            )]),
            fail_keys: vec![],
        });
        let lister = Arc::new(MockLister {
            keys: vec!["events/snap.parquet".into()],
        });
        let stream = start_stream(
            AccelerationContents::Empty,
            queue,
            reader,
            lister,
            default_config(),
            id_name_batch(&[99], &["stale-federated"]),
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
        assert_eq!(names_in(&envelopes[0]), vec!["snap".to_string()]);
        assert!(envelopes[1].is_dataset_ready());
        assert!(envelopes[1].is_empty());
    }

    #[tokio::test]
    async fn stream_empty_snapshot_concats_a_multi_batch_object() {
        let queue = Arc::new(MockQueue::with_messages(vec![]));
        let reader = Arc::new(MapObjectReader {
            objects: HashMap::from([(
                "my-bucket/events/snap.parquet".to_string(),
                multi_batch_object(),
            )]),
            fail_keys: vec![],
        });
        let lister = Arc::new(MockLister {
            keys: vec!["events/snap.parquet".into()],
        });
        let stream = start_stream(
            AccelerationContents::Empty,
            queue,
            reader,
            lister,
            default_config(),
            id_name_batch(&[99], &["stale-federated"]),
        );
        let envelopes = collect_until_idle(stream, 3).await;
        assert_eq!(
            envelopes.len(),
            2,
            "a multi-batch snapshot object must be one create + ready, got {}",
            envelopes.len()
        );
        assert_eq!(
            names_in(&envelopes[0]),
            vec!["batch-1".to_string(), "batch-2".to_string()]
        );
        assert!(envelopes[1].is_dataset_ready());
    }

    #[tokio::test]
    async fn stream_object_created_concats_a_multi_batch_object() {
        let queue = Arc::new(MockQueue::with_messages(vec![QueueMessage {
            body: created_put_body("events/part.parquet"),
            receipt_handle: "rh-part".into(),
        }]));
        let reader = Arc::new(MapObjectReader {
            objects: HashMap::from([(
                "my-bucket/events/part.parquet".to_string(),
                multi_batch_object(),
            )]),
            fail_keys: vec![],
        });
        let stream = start_stream(
            AccelerationContents::Empty,
            Arc::clone(&queue),
            reader,
            empty_lister(),
            default_config(),
            id_name_batch(&[99], &["stale-federated"]),
        );
        let envelopes = collect_until_idle(stream, 3).await;
        assert_eq!(
            envelopes.len(),
            2,
            "a multi-batch ObjectCreated must be ready + one create, got {}",
            envelopes.len()
        );
        assert!(envelopes[0].is_dataset_ready());
        assert_eq!(
            names_in(&envelopes[1]),
            vec!["batch-1".to_string(), "batch-2".to_string()]
        );
        envelopes
            .into_iter()
            .nth(1)
            .expect("create envelope")
            .commit()
            .await
            .expect("SQS delete should succeed after the concat apply");
        assert_eq!(*queue.deleted.lock().await, vec!["rh-part".to_string()]);
    }

    /// Copilot harness: listed keys `a,b`, only `a` readable, must not mark
    /// ready or treat `b` as applied. `python3` fallback predicate
    /// `listed_matching > 0 && keys.is_empty()` produced
    /// `replacement_rows=['a']` and `missing_rows=['b']` on the old warn-and-
    /// continue path.
    #[tokio::test]
    async fn stream_empty_snapshot_does_not_ready_on_partial_listing() {
        let queue = Arc::new(MockQueue::with_messages(vec![]));
        let reader = Arc::new(MapObjectReader {
            objects: HashMap::from([(
                "my-bucket/events/a.parquet".to_string(),
                vec![id_name_batch(&[1], &["a"])],
            )]),
            fail_keys: vec!["my-bucket/events/b.parquet".into()],
        });
        let lister = Arc::new(MockLister {
            keys: vec!["events/a.parquet".into(), "events/b.parquet".into()],
        });
        let stream = start_stream(
            AccelerationContents::Empty,
            queue,
            reader,
            lister,
            default_config(),
            id_name_batch(&[99], &["stale-federated"]),
        );
        let envelopes = collect_until_idle(stream, 2).await;
        assert!(
            envelopes.is_empty(),
            "a partial snapshot must not emit creates or ready, got {} envelopes",
            envelopes.len()
        );
    }

    /// Objects under the prefix that the listing table does not read (job
    /// markers, checksum sidecars) have no fixture here, so reading one fails as
    /// it does against S3. The snapshot must skip them rather than count them as
    /// unread and never mark the dataset ready.
    #[tokio::test]
    async fn stream_empty_snapshot_skips_objects_the_listing_table_does_not_read() {
        let queue = Arc::new(MockQueue::with_messages(vec![]));
        let reader = Arc::new(MapObjectReader {
            objects: HashMap::from([(
                "my-bucket/events/part-00000.parquet".to_string(),
                vec![id_name_batch(&[1], &["part"])],
            )]),
            fail_keys: vec![],
        });
        let lister = Arc::new(MockLister {
            keys: vec![
                "events/part-00000.parquet".into(),
                "events/_SUCCESS".into(),
                "events/part-00000.parquet.crc".into(),
            ],
        });
        let stream = start_stream(
            AccelerationContents::Empty,
            queue,
            reader,
            lister,
            default_config(),
            id_name_batch(&[99], &["stale-federated"]),
        );
        let envelopes = collect_until_idle(stream, 2).await;
        assert_eq!(
            envelopes.len(),
            2,
            "objects the listing table does not read must not hold back the snapshot, got {} envelopes",
            envelopes.len()
        );
        assert_eq!(names_in(&envelopes[0]), vec!["part".to_string()]);
        assert!(envelopes[1].is_dataset_ready());
    }

    #[tokio::test]
    async fn stream_empty_snapshot_retries_until_every_listed_object_is_read() {
        let queue: Arc<dyn MessageQueue> = Arc::new(MockQueue::with_messages(vec![]));
        let reader: Arc<dyn ObjectReader> = Arc::new(TransientFailReader {
            inner: MapObjectReader {
                objects: HashMap::from([
                    (
                        "my-bucket/events/a.parquet".to_string(),
                        vec![id_name_batch(&[1], &["a"])],
                    ),
                    (
                        "my-bucket/events/b.parquet".to_string(),
                        vec![id_name_batch(&[2], &["b"])],
                    ),
                ]),
                fail_keys: vec![],
            },
            remaining: Mutex::new(HashMap::from([(
                "my-bucket/events/b.parquet".to_string(),
                1,
            )])),
        });
        let lister: Arc<dyn ObjectLister> = Arc::new(MockLister {
            keys: vec!["events/a.parquet".into(), "events/b.parquet".into()],
        });
        let stream = stream_s3_changes(S3ChangesStreamParts {
            dataset: events_dataset(),
            federated_table: federated_table(id_name_batch(&[99], &["stale-federated"])),
            acceleration: AccelerationContents::Empty,
            queue,
            object_reader: reader,
            object_lister: lister,
            config: default_config(),
            listing_files: parquet_files(),
        });
        let envelopes = collect_until_idle(stream, 2).await;
        assert_eq!(
            envelopes.len(),
            2,
            "snapshot must retry until every listed object is read, then mark ready as one concat create, got {}",
            envelopes.len()
        );
        assert_eq!(
            names_in(&envelopes[0]),
            vec!["a".to_string(), "b".to_string()],
            "retry must apply both listed objects in one envelope"
        );
        assert!(envelopes[1].is_dataset_ready());
    }

    #[tokio::test]
    async fn stream_nonempty_rebuild_does_not_overwrite_on_partial_listing() {
        let queue = Arc::new(MockQueue::with_messages(vec![]));
        let reader = Arc::new(MapObjectReader {
            objects: HashMap::from([(
                "my-bucket/events/a.parquet".to_string(),
                vec![id_name_batch(&[1], &["a"])],
            )]),
            fail_keys: vec!["my-bucket/events/b.parquet".into()],
        });
        let lister = Arc::new(MockLister {
            keys: vec!["events/a.parquet".into(), "events/b.parquet".into()],
        });
        let stream = start_stream(
            AccelerationContents::NonEmpty,
            queue,
            reader,
            lister,
            default_config(),
            id_name_batch(&[99], &["stale-federated"]),
        );
        let envelopes = collect_until_idle(stream, 2).await;
        assert!(
            envelopes.is_empty(),
            "a partial rebuild must not overwrite or mark ready, got {} envelopes",
            envelopes.len()
        );
    }

    #[tokio::test]
    async fn stream_object_removed_partial_listing_leaves_message() {
        let queue = Arc::new(MockQueue::with_messages(vec![QueueMessage {
            body: removed_body("events/gone.parquet"),
            receipt_handle: "rh-gone".into(),
        }]));
        let reader = Arc::new(MapObjectReader {
            objects: HashMap::from([(
                "my-bucket/events/a.parquet".to_string(),
                vec![id_name_batch(&[1], &["a"])],
            )]),
            fail_keys: vec!["my-bucket/events/b.parquet".into()],
        });
        let lister: Arc<dyn ObjectLister> = Arc::new(SequenceLister {
            listings: Mutex::new(vec![
                Vec::new(),
                vec!["events/a.parquet".into(), "events/b.parquet".into()],
            ]),
        });
        let mut config = default_config();
        config.on_object_removed = OnObjectRemoved::Rebuild;
        let stream = start_stream(
            AccelerationContents::NonEmpty,
            Arc::clone(&queue),
            reader,
            lister,
            config,
            id_name_batch(&[1], &["snap"]),
        );
        let envelopes = collect_until_idle(stream, 3).await;
        assert_eq!(
            envelopes.len(),
            2,
            "partial ObjectRemoved must not overwrite, got {}",
            envelopes.len()
        );
        assert!(envelopes[0].history_unavailable());
        assert!(envelopes[1].is_dataset_ready());
        assert!(
            queue.deleted.lock().await.is_empty(),
            "partial ObjectRemoved must leave the SQS message on the queue"
        );
    }

    #[tokio::test]
    async fn stream_empty_snapshot_applies_every_key_from_the_listing_manifest() {
        let queue: Arc<dyn MessageQueue> = Arc::new(FailingQueue {
            remaining_failures: Mutex::new(2),
        });
        let reader = Arc::new(MapObjectReader {
            objects: HashMap::from([
                (
                    "my-bucket/events/existing.parquet".to_string(),
                    vec![id_name_batch(&[1], &["existing"])],
                ),
                (
                    "my-bucket/events/arrived_during_snapshot.parquet".to_string(),
                    vec![id_name_batch(&[2], &["arrived"])],
                ),
            ]),
            fail_keys: vec![],
        });
        let lister: Arc<dyn ObjectLister> = Arc::new(SequenceLister {
            listings: Mutex::new(vec![
                vec![
                    "events/existing.parquet".into(),
                    "events/arrived_during_snapshot.parquet".into(),
                ],
                vec![
                    "events/existing.parquet".into(),
                    "events/arrived_during_snapshot.parquet".into(),
                ],
            ]),
        });
        let mut config = default_config();
        config.backfill_interval = Duration::from_millis(1);
        let stream = stream_s3_changes(S3ChangesStreamParts {
            dataset: events_dataset(),
            federated_table: federated_table(id_name_batch(&[99], &["stale-federated"])),
            acceleration: AccelerationContents::Empty,
            queue,
            object_reader: reader,
            object_lister: lister,
            config,
            listing_files: parquet_files(),
        });
        let envelopes = collect_until_idle(stream, 2).await;
        let snapshot_names: Vec<String> = envelopes
            .iter()
            .filter(|envelope| !envelope.is_empty() && !envelope.is_dataset_ready())
            .flat_map(names_in)
            .collect();
        assert!(
            snapshot_names.contains(&"existing".to_string())
                && snapshot_names.contains(&"arrived".to_string()),
            "snapshot must apply the listing manifest, not a later federated-table scan, got {snapshot_names:?} from {} envelopes",
            envelopes.len()
        );
        assert!(
            !snapshot_names.contains(&"stale-federated".to_string()),
            "federated-table rows must not substitute for the listing manifest, got {snapshot_names:?}"
        );
    }

    #[tokio::test]
    async fn stream_empty_snapshot_backfills_keys_absent_from_the_snapshot_listing() {
        let queue: Arc<dyn MessageQueue> = Arc::new(FailingQueue {
            remaining_failures: Mutex::new(2),
        });
        let reader = Arc::new(MapObjectReader {
            objects: HashMap::from([
                (
                    "my-bucket/events/existing.parquet".to_string(),
                    vec![id_name_batch(&[1], &["existing"])],
                ),
                (
                    "my-bucket/events/arrived_during_snapshot.parquet".to_string(),
                    vec![id_name_batch(&[2], &["arrived"])],
                ),
            ]),
            fail_keys: vec![],
        });
        let lister: Arc<dyn ObjectLister> = Arc::new(SequenceLister {
            listings: Mutex::new(vec![
                vec!["events/existing.parquet".into()],
                vec![
                    "events/existing.parquet".into(),
                    "events/arrived_during_snapshot.parquet".into(),
                ],
            ]),
        });
        let mut config = default_config();
        config.backfill_interval = Duration::from_millis(1);
        let stream = stream_s3_changes(S3ChangesStreamParts {
            dataset: events_dataset(),
            federated_table: federated_table(id_name_batch(&[99], &["stale-federated"])),
            acceleration: AccelerationContents::Empty,
            queue,
            object_reader: reader,
            object_lister: lister,
            config,
            listing_files: parquet_files(),
        });
        let envelopes = collect_until_idle(stream, 3).await;
        assert!(
            envelopes.len() >= 3,
            "objects absent from the snapshot listing must be backfilled, got {}",
            envelopes.len()
        );
        assert_eq!(names_in(&envelopes[0]), vec!["existing".to_string()]);
        assert!(!envelopes[0].is_dataset_ready());
        assert!(envelopes[1].is_dataset_ready());
        assert!(envelopes[1].is_empty());
        assert_eq!(names_in(&envelopes[2]), vec!["arrived".to_string()]);
        assert!(envelopes[2].is_dataset_ready());
    }

    #[tokio::test]
    async fn stream_nonempty_rebuild_backfills_keys_absent_from_the_pre_rebuild_listing() {
        let queue: Arc<dyn MessageQueue> = Arc::new(FailingQueue {
            remaining_failures: Mutex::new(2),
        });
        let reader = Arc::new(MapObjectReader {
            objects: HashMap::from([
                (
                    "my-bucket/events/existing.parquet".to_string(),
                    vec![id_name_batch(&[1], &["existing"])],
                ),
                (
                    "my-bucket/events/arrived_during_rebuild.parquet".to_string(),
                    vec![id_name_batch(&[2], &["arrived"])],
                ),
            ]),
            fail_keys: vec![],
        });
        let lister: Arc<dyn ObjectLister> = Arc::new(SequenceLister {
            listings: Mutex::new(vec![
                vec!["events/existing.parquet".into()],
                vec![
                    "events/existing.parquet".into(),
                    "events/arrived_during_rebuild.parquet".into(),
                ],
            ]),
        });
        let mut config = default_config();
        config.backfill_interval = Duration::from_millis(1);
        let stream = stream_s3_changes(S3ChangesStreamParts {
            dataset: events_dataset(),
            federated_table: federated_table(id_name_batch(&[99], &["stale-federated"])),
            acceleration: AccelerationContents::NonEmpty,
            queue,
            object_reader: reader,
            object_lister: lister,
            config,
            listing_files: parquet_files(),
        });
        let envelopes = collect_until_idle(stream, 3).await;
        assert!(
            envelopes.len() >= 3,
            "objects absent from the pre-rebuild listing must be backfilled, got {}",
            envelopes.len()
        );
        assert!(envelopes[0].history_unavailable());
        assert!(
            envelopes[0].rebuild_from_this_batch(),
            "non-empty rebuild must carry the listing snapshot, not ask for a later federated scan"
        );
        assert_eq!(names_in(&envelopes[0]), vec!["existing".to_string()]);
        assert!(envelopes[1].is_dataset_ready());
        assert_eq!(names_in(&envelopes[2]), vec!["arrived".to_string()]);
        let arrived_count = envelopes
            .iter()
            .flat_map(names_in)
            .filter(|name| name == "arrived")
            .count();
        assert_eq!(
            arrived_count,
            1,
            "an object that arrived after the rebuild listing must be backfilled once, not duplicated by a later scan, got {arrived_count} from {} envelopes",
            envelopes.len()
        );
        assert!(
            !envelopes
                .iter()
                .flat_map(names_in)
                .any(|name| name == "stale-federated"),
            "federated-table rows must not substitute for the listing snapshot"
        );
    }

    /// Copilot harness: `captured={'a'}; rebuild={'a','b'}; candidates=rebuild-captured`
    /// must not mark `b` applied before its rows are emitted, and must not put
    /// `b` on the rebuild envelope from a later listing or federated scan.
    #[tokio::test]
    async fn stream_nonempty_rebuild_snapshot_is_the_listing_not_a_later_federated_scan() {
        let queue: Arc<dyn MessageQueue> = Arc::new(FailingQueue {
            remaining_failures: Mutex::new(2),
        });
        let reader = Arc::new(MapObjectReader {
            objects: HashMap::from([
                (
                    "my-bucket/events/existing.parquet".to_string(),
                    vec![id_name_batch(&[1], &["existing"])],
                ),
                (
                    "my-bucket/events/arrived_during_rebuild.parquet".to_string(),
                    vec![id_name_batch(&[2], &["arrived"])],
                ),
            ]),
            fail_keys: vec![],
        });
        let lister: Arc<dyn ObjectLister> = Arc::new(SequenceLister {
            listings: Mutex::new(vec![
                vec!["events/existing.parquet".into()],
                vec![
                    "events/existing.parquet".into(),
                    "events/arrived_during_rebuild.parquet".into(),
                ],
            ]),
        });
        let mut config = default_config();
        config.backfill_interval = Duration::from_millis(1);
        let stream = stream_s3_changes(S3ChangesStreamParts {
            dataset: events_dataset(),
            federated_table: federated_table(id_name_batch(&[99], &["stale-federated"])),
            acceleration: AccelerationContents::NonEmpty,
            queue,
            object_reader: reader,
            object_lister: lister,
            config,
            listing_files: parquet_files(),
        });
        let envelopes = collect_until_idle(stream, 3).await;
        assert!(
            envelopes.len() >= 3,
            "expected listing rebuild + ready + backfill of the post-listing object, got {}",
            envelopes.len()
        );
        assert!(envelopes[0].history_unavailable());
        assert!(envelopes[0].rebuild_from_this_batch());
        assert!(!envelopes[0].is_dataset_ready());
        assert_eq!(names_in(&envelopes[0]), vec!["existing".to_string()]);
        assert!(envelopes[1].is_dataset_ready());
        assert_eq!(names_in(&envelopes[2]), vec!["arrived".to_string()]);
        let all_names: Vec<String> = envelopes.iter().flat_map(names_in).collect();
        assert_eq!(
            all_names.iter().filter(|name| *name == "arrived").count(),
            1,
            "arrived must appear once (backfill only), got {all_names:?}"
        );
        assert!(
            !all_names.contains(&"stale-federated".to_string()),
            "replacement rows must be the first listing, not the federated table, got {all_names:?}"
        );
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
        assert!(
            envelopes[0].rebuild_from_this_batch(),
            "non-empty rebuild must overwrite from the listing snapshot"
        );
        assert!(!envelopes[0].is_dataset_ready());
        assert_eq!(names_in(&envelopes[0]), vec!["missed".to_string()]);
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
            &parquet_files(),
            &id_name_schema(),
            &reader,
            &applied_mutex([]),
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

    /// A rebuild takes ownership of every key in its listing. The consumer trims
    /// the run to that rebuild signal and drops the envelopes before it unapplied
    /// (`trim_to_rebuild_signal`), so an earlier create's drop must not release a
    /// key the rebuild now owns — a redelivered notification for it would then be
    /// applied on top of the replacement.
    #[tokio::test]
    async fn stream_rebuild_keeps_its_claim_when_a_superseded_create_is_dropped() {
        let queue: Arc<dyn MessageQueue> = Arc::new(SequenceQueue {
            batches: Mutex::new(vec![
                vec![
                    QueueMessage {
                        body: created_put_body("events/a.parquet"),
                        receipt_handle: "rh-create".into(),
                    },
                    QueueMessage {
                        body: removed_body("events/gone.parquet"),
                        receipt_handle: "rh-removed".into(),
                    },
                ],
                vec![QueueMessage {
                    body: created_put_body("events/a.parquet"),
                    receipt_handle: "rh-redelivered".into(),
                }],
            ]),
        });
        let reader = Arc::new(MapObjectReader {
            objects: HashMap::from([(
                "my-bucket/events/a.parquet".to_string(),
                vec![id_name_batch(&[1], &["a"])],
            )]),
            fail_keys: vec![],
        });
        // Empty at startup, so the create below is the first claim on the key;
        // the ObjectRemoved rebuild then lists that same key.
        let lister: Arc<dyn ObjectLister> = Arc::new(SequenceLister {
            listings: Mutex::new(vec![Vec::new(), vec!["events/a.parquet".into()]]),
        });
        let mut config = default_config();
        config.on_object_removed = OnObjectRemoved::Rebuild;
        let mut stream = stream_s3_changes(S3ChangesStreamParts {
            dataset: events_dataset(),
            federated_table: federated_table(id_name_batch(&[99], &["stale-federated"])),
            acceleration: AccelerationContents::Empty,
            queue,
            object_reader: reader,
            object_lister: lister,
            config,
            listing_files: parquet_files(),
        });

        let ready = next_envelope(&mut stream, Duration::from_secs(2))
            .await
            .expect("empty snapshot marks the dataset ready");
        assert!(ready.is_dataset_ready());
        let create = next_envelope(&mut stream, Duration::from_secs(2))
            .await
            .expect("the ObjectCreated notification yields a create");
        assert_eq!(names_in(&create), vec!["a".to_string()]);
        let rebuild = next_envelope(&mut stream, Duration::from_secs(2))
            .await
            .expect("the ObjectRemoved notification yields a listing rebuild");
        assert!(rebuild.history_unavailable());

        // The consumer trims the create out of the run and drops it unapplied.
        drop(create);

        assert!(
            next_envelope(&mut stream, Duration::from_secs(2))
                .await
                .is_none(),
            "a redelivered create for a key the rebuild owns must not be applied on top of the replacement"
        );
    }

    #[test]
    fn replace_applied_keys_uses_current_listing_not_clear() {
        let mut applied = AppliedKeySet::default();
        let generation = applied.generation();
        applied.commit(generation, &["events/gone.parquet".to_string()]);
        applied.replace_in_flight(vec![
            "events/a.parquet".to_string(),
            "events/b.parquet".to_string(),
        ]);
        assert!(!applied.is_known("events/gone.parquet"));
        assert!(applied.is_in_flight("events/a.parquet"));
        assert!(applied.is_in_flight("events/b.parquet"));
    }

    /// A committer from before a listing rebuild must not move keys the rebuild
    /// now owns: its commit and its drop both belong to a superseded generation.
    #[test]
    fn a_superseded_committer_does_not_move_a_rebuilds_keys() {
        let applied = Arc::new(parking_lot::Mutex::new(AppliedKeySet::default()));
        let superseded = applied.lock().generation();
        applied
            .lock()
            .mark_in_flight(["events/a.parquet".to_string()]);
        applied
            .lock()
            .replace_in_flight(vec!["events/a.parquet".to_string()]);
        {
            let _committer = AppliedKeysCommitter {
                applied: Arc::clone(&applied),
                generation: superseded,
                keys: vec!["events/a.parquet".to_string()],
                inner: Box::new(NoOpCommitter),
            };
        }
        assert!(
            applied.lock().is_in_flight("events/a.parquet"),
            "the rebuild's claim must survive a superseded envelope's drop"
        );
    }

    #[tokio::test]
    async fn applied_keys_committer_advances_only_after_commit() {
        let applied = Arc::new(parking_lot::Mutex::new(AppliedKeySet::default()));
        applied
            .lock()
            .mark_in_flight(["events/a.parquet".to_string()]);
        let committer = AppliedKeysCommitter {
            applied: Arc::clone(&applied),
            generation: applied.lock().generation(),
            keys: vec!["events/a.parquet".to_string()],
            inner: Box::new(NoOpCommitter),
        };
        assert!(applied.lock().is_in_flight("events/a.parquet"));
        assert!(!applied.lock().is_committed("events/a.parquet"));
        committer
            .commit()
            .await
            .expect("noop commit should succeed");
        assert!(applied.lock().is_committed("events/a.parquet"));
        assert!(!applied.lock().is_in_flight("events/a.parquet"));
    }

    #[test]
    fn dropping_uncommitted_envelope_releases_in_flight_keys() {
        let applied = Arc::new(parking_lot::Mutex::new(AppliedKeySet::default()));
        applied
            .lock()
            .mark_in_flight(["events/a.parquet".to_string()]);
        {
            let _committer = AppliedKeysCommitter {
                applied: Arc::clone(&applied),
                generation: applied.lock().generation(),
                keys: vec!["events/a.parquet".to_string()],
                inner: Box::new(NoOpCommitter),
            };
        }
        assert!(
            !applied.lock().is_known("events/a.parquet"),
            "drop without commit must release in-flight keys so apply can retry"
        );
    }

    fn multi_batch_object() -> Vec<RecordBatch> {
        vec![
            id_name_batch(&[1], &["batch-1"]),
            id_name_batch(&[2], &["batch-2"]),
        ]
    }

    /// One object (or one notification / backfill pass) that decodes into
    /// several record batches must be one envelope. The consumer's default
    /// `max_coalesce_age_ms` of 0 applies the first buffered envelope
    /// immediately, and envelope/byte caps can split later ones into their own
    /// writes.
    #[tokio::test]
    async fn create_envelopes_concats_object_batches_into_one_envelope() {
        let applied = Arc::new(parking_lot::Mutex::new(AppliedKeySet::default()));
        let queue: Arc<dyn MessageQueue> = Arc::new(MockQueue::with_messages(vec![]));
        let envelopes = create_envelopes(
            &id_name_schema(),
            multi_batch_object(),
            &applied,
            vec!["events/part.parquet".to_string()],
            &queue,
            "rh-part",
        )
        .expect("create envelopes");
        assert_eq!(
            envelopes.len(),
            1,
            "a multi-batch object must be one envelope so the consumer cannot split it across writes"
        );
        assert_eq!(
            names_in(&envelopes[0]),
            vec!["batch-1".to_string(), "batch-2".to_string()]
        );
        assert!(applied.lock().is_in_flight("events/part.parquet"));
        assert!(!applied.lock().is_committed("events/part.parquet"));
    }

    /// Last-envelope committer plus a split write left the first batch durable,
    /// aborted the key, and a retry appended `batch-1` twice. Concatenate first
    /// so a failed apply drops the only envelope and retry appends the object
    /// once.
    #[tokio::test]
    async fn create_envelopes_failed_apply_retries_the_object_once() {
        let applied = Arc::new(parking_lot::Mutex::new(AppliedKeySet::default()));
        let queue = Arc::new(MockQueue::with_messages(vec![]));
        let queue_dyn: Arc<dyn MessageQueue> = Arc::clone(&queue) as Arc<dyn MessageQueue>;
        let keys = vec!["events/part.parquet".to_string()];

        {
            let envelopes = create_envelopes(
                &id_name_schema(),
                multi_batch_object(),
                &applied,
                keys.clone(),
                &queue_dyn,
                "rh-part",
            )
            .expect("first apply");
            assert_eq!(envelopes.len(), 1);
            drop(envelopes);
        }
        assert!(
            !applied.lock().is_known("events/part.parquet"),
            "failed apply must release the key so retry can read it"
        );
        assert!(
            queue.deleted.lock().await.is_empty(),
            "failed apply must leave the SQS message unacked"
        );

        let envelopes = create_envelopes(
            &id_name_schema(),
            multi_batch_object(),
            &applied,
            keys,
            &queue_dyn,
            "rh-part",
        )
        .expect("retry");
        assert_eq!(envelopes.len(), 1);
        let durable = names_in(&envelopes[0]);
        envelopes
            .into_iter()
            .next()
            .expect("retry envelope")
            .commit()
            .await
            .expect("retry commit should succeed");
        assert_eq!(
            durable,
            vec!["batch-1".to_string(), "batch-2".to_string()],
            "retry must append the object once, not duplicate the first batch"
        );
        assert!(applied.lock().is_committed("events/part.parquet"));
        assert_eq!(*queue.deleted.lock().await, vec!["rh-part".to_string()]);
    }

    #[test]
    fn backfill_envelopes_concats_object_batches_into_one_envelope() {
        let applied = Arc::new(parking_lot::Mutex::new(AppliedKeySet::default()));
        let envelopes = backfill_envelopes(
            &id_name_schema(),
            multi_batch_object(),
            true,
            &applied,
            vec!["events/part.parquet".to_string()],
        )
        .expect("backfill envelopes");
        assert_eq!(
            envelopes.len(),
            1,
            "a multi-batch backfill object must be one envelope"
        );
        assert_eq!(
            names_in(&envelopes[0]),
            vec!["batch-1".to_string(), "batch-2".to_string()]
        );
        assert!(applied.lock().is_in_flight("events/part.parquet"));
    }

    #[tokio::test]
    async fn stream_lists_backfill_after_sqs_receive_failures() {
        let queue: Arc<dyn MessageQueue> = Arc::new(FailingQueue {
            remaining_failures: Mutex::new(2),
        });
        let reader = Arc::new(MapObjectReader {
            objects: HashMap::from([(
                "my-bucket/events/missed.parquet".to_string(),
                vec![id_name_batch(&[3], &["missed"])],
            )]),
            fail_keys: vec![],
        });
        let lister: Arc<dyn ObjectLister> = Arc::new(SequenceLister {
            listings: Mutex::new(vec![Vec::new(), vec!["events/missed.parquet".into()]]),
        });
        let mut config = default_config();
        config.backfill_interval = Duration::from_millis(1);
        let stream = stream_s3_changes(S3ChangesStreamParts {
            dataset: events_dataset(),
            federated_table: federated_table(id_name_batch(&[1], &["snap"])),
            acceleration: AccelerationContents::NonEmpty,
            queue,
            object_reader: reader,
            object_lister: lister,
            config,
            listing_files: parquet_files(),
        });
        let envelopes = collect_until_idle(stream, 3).await;
        assert!(
            envelopes.len() >= 3,
            "SQS receive failures must not skip listing backfill, got {}",
            envelopes.len()
        );
        assert!(envelopes[0].history_unavailable());
        assert!(envelopes[1].is_dataset_ready());
        assert!(matches!(
            envelopes[2]
                .change_batch()
                .expect("backfill after SQS failure")
                .op(0),
            ChangeOperation::Create
        ));
    }

    #[tokio::test]
    async fn stream_empty_snapshot_does_not_reapply_queued_object_created() {
        let queue = Arc::new(MockQueue::with_messages(vec![QueueMessage {
            body: created_put_body("events/snap.parquet"),
            receipt_handle: "rh-snap".into(),
        }]));
        let reader = Arc::new(MapObjectReader {
            objects: HashMap::from([(
                "my-bucket/events/snap.parquet".to_string(),
                vec![id_name_batch(&[1], &["snap"])],
            )]),
            fail_keys: vec![],
        });
        let lister = Arc::new(MockLister {
            keys: vec!["events/snap.parquet".into()],
        });
        let stream = start_stream(
            AccelerationContents::Empty,
            Arc::clone(&queue),
            reader,
            lister,
            default_config(),
            id_name_batch(&[99], &["stale-federated"]),
        );
        let envelopes = collect_until_idle(stream, 3).await;
        assert_eq!(
            envelopes.len(),
            2,
            "queued ObjectCreated for a snapshotted key must not append again, got {}",
            envelopes.len()
        );
        assert_eq!(names_in(&envelopes[0]), vec!["snap".to_string()]);
        assert!(envelopes[1].is_dataset_ready());
        assert!(
            queue.deleted.lock().await.is_empty(),
            "queued ObjectCreated for an in-flight snapshot key must stay on the queue until apply commits"
        );
        envelopes
            .into_iter()
            .next()
            .expect("snapshot envelope")
            .commit()
            .await
            .expect("snapshot commit should succeed");
        assert!(
            queue.deleted.lock().await.is_empty(),
            "the overlapping SQS notification is left, not acked, while apply is pending; it is not deleted by snapshot commit"
        );
    }

    #[tokio::test]
    async fn stream_empty_snapshot_uses_dataset_prefix_not_nested_key_prefix() {
        let queue = Arc::new(MockQueue::with_messages(vec![]));
        let reader = Arc::new(MapObjectReader {
            objects: HashMap::from([
                (
                    "my-bucket/events/year=2026/a.parquet".to_string(),
                    vec![id_name_batch(&[1], &["y2026"])],
                ),
                (
                    "my-bucket/events/year=2025/b.parquet".to_string(),
                    vec![id_name_batch(&[2], &["y2025"])],
                ),
            ]),
            fail_keys: vec![],
        });
        let lister = Arc::new(MockLister {
            keys: vec![
                "events/year=2026/a.parquet".into(),
                "events/year=2025/b.parquet".into(),
            ],
        });
        let mut config = default_config();
        config.key_prefix = "events/year=2026/".to_string();
        let stream = start_stream(
            AccelerationContents::Empty,
            queue,
            reader,
            lister,
            config,
            id_name_batch(&[99], &["stale-federated"]),
        );
        let envelopes = collect_until_idle(stream, 2).await;
        let snapshot_names: Vec<String> = envelopes
            .iter()
            .filter(|envelope| !envelope.is_empty() && !envelope.is_dataset_ready())
            .flat_map(names_in)
            .collect();
        assert!(
            snapshot_names.contains(&"y2026".to_string())
                && snapshot_names.contains(&"y2025".to_string()),
            "empty snapshot must use the dataset from: prefix, not a nested s3_changes_key_prefix, got {snapshot_names:?}"
        );
    }
}
