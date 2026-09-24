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

//! Reload `refresh_mode: snapshot` datasets when S3 reports a new snapshot.
//!
//! A snapshot becomes current when its writer rewrites the location's
//! `metadata.json`, so an `ObjectCreated` notification for that object means a
//! dataset reading the location may have a newer snapshot to load. The
//! notifications arrive on an SQS queue subscribed to the location's S3 event
//! notifications.
//!
//! One consumer per queue serves every snapshot-mode dataset in the process.
//! SQS hands each message to a single receiver, so a consumer per dataset would
//! split the notifications between them. On a commit notification the consumer
//! reads `metadata.json` once and announces every dataset's current snapshot
//! id. Each dataset asks for a reload only when its own id advances, so one
//! dataset's publish does not restart another dataset's reload.
//!
//! An announcement only decides when to ask. What a dataset loads is still
//! decided by [`super::SnapshotManager::download_if_newer`], which compares
//! snapshot ids, checks the schema and verifies the checksum, so a duplicate,
//! reordered or forged notification cannot change what is served.

use std::collections::HashMap;
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use runtime_secrets::Secrets;
use s3_event_notifications::client::{SqsCredentials, build_sqs_client};
use s3_event_notifications::event::{
    ObjectEventKind, matches_prefix, parse_notification_body, prefix_display,
};
use s3_event_notifications::queue::{MessageQueue, QueueMessage, SqsQueue};
use s3_event_notifications::queue_url::{is_sqs_queue_url, region_from_queue_url};
use snafu::prelude::*;
use spicepod::component::snapshot::Snapshots;
use spicepod::param::Params;
use tokio::runtime::Handle;
use tokio::sync::{RwLock, watch};
use tokio::task::JoinHandle;
use url::Url;
use util::retry_strategy::{Backoff, BackoffMethod, RetryBackoff, RetryBackoffBuilder};

use super::{
    METADATA_FILE_NAME, SNAPSHOTS_DOCS, SnapshotManager, format_duration, s3_location_path,
};

/// The snapshot location parameter naming the SQS queue, as registered in the
/// location's parameter spec. Users write it with the `s3_` prefix, as
/// [`QUEUE_URL_KEY`].
pub(super) const QUEUE_URL_PARAM: &str = "queue_url";
const QUEUE_URL_KEY: &str = "s3_queue_url";

const RETRY_BACKOFF_CAP: Duration = Duration::from_secs(30);
/// How long SQS can keep failing before the outage is logged as an error
/// rather than a warning. Datasets keep checking the location on
/// `refresh_check_interval` throughout, so this marks when an operator should
/// act, not when data goes stale.
const SQS_OUTAGE_ERROR_AFTER: Duration = Duration::from_mins(5);

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "`snapshots.params.s3_queue_url` is empty. Set it to the URL of the SQS queue that receives the snapshot location's S3 event notifications, or remove it. See: {SNAPSHOTS_DOCS}"
    ))]
    EmptyQueueUrl,

    #[snafu(display(
        "`snapshots.params.s3_queue_url` must be an SQS queue URL (https://sqs.<region>.amazonaws.com/<account>/<queue>), not an ARN. See: {SNAPSHOTS_DOCS}"
    ))]
    QueueUrlIsArn,

    #[snafu(display(
        "`snapshots.params.s3_queue_url` is not an SQS queue URL. Use a URL like https://sqs.<region>.amazonaws.com/<account>/<queue>. See: {SNAPSHOTS_DOCS}"
    ))]
    QueueUrlNotSqs,

    #[snafu(display(
        "`snapshots.params.s3_queue_url` requires an `s3://` snapshot location, because only S3 sends S3 event notifications, but the location is '{location}'. Remove `s3_queue_url` or move the snapshots to S3. See: {SNAPSHOTS_DOCS}"
    ))]
    LocationNotS3 { location: String },

    #[snafu(display(
        "`snapshots.location` '{location}' is not a valid URL, so snapshot notifications cannot be matched to it. Use a location like 's3://my-bucket/spice/snapshots/'. See: {SNAPSHOTS_DOCS}"
    ))]
    InvalidLocation { location: String },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Where a snapshot location's S3 event notifications arrive, resolved from
/// the top-level `snapshots` configuration. It holds the secret queue URL and
/// may hold keys, so it deliberately has no `Debug`.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct NotificationConfig {
    queue_url: String,
    region: String,
    credentials: SqsCredentials,
    location: NotificationLocation,
}

/// The bucket and key prefix a snapshot location covers.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct NotificationLocation {
    bucket: String,
    /// The location's key prefix with a trailing `/`, or empty for a bucket
    /// root. Every object the snapshot writer creates is under it.
    key_prefix: String,
}

impl NotificationLocation {
    fn from_url(url: &Url) -> Option<Self> {
        let bucket = url.host_str().filter(|bucket| !bucket.is_empty())?;
        let path = s3_location_path(url);
        let key_prefix = if path.as_ref().is_empty() {
            String::new()
        } else {
            format!("{}/", path.as_ref())
        };
        Some(Self {
            bucket: bucket.to_string(),
            key_prefix,
        })
    }

    /// Whether `key` is the location's `metadata.json`, the object whose
    /// rewrite makes a snapshot current.
    fn is_metadata_key(&self, key: &str) -> bool {
        key.strip_prefix(&self.key_prefix) == Some(METADATA_FILE_NAME)
    }
}

impl std::fmt::Display for NotificationLocation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&prefix_display(&self.bucket, &self.key_prefix))
    }
}

impl NotificationConfig {
    /// Resolve the notification queue for `snapshots`, or `Ok(None)` when the
    /// snapshot location has no queue configured.
    ///
    /// # Errors
    ///
    /// Returns an [`Error`] naming `s3_queue_url` when the queue URL is empty,
    /// an ARN, or not an SQS queue URL, and when the snapshot location is not
    /// on S3.
    pub async fn resolve(
        snapshots: &Snapshots,
        secrets: Arc<RwLock<Secrets>>,
    ) -> Result<Option<Self>> {
        // Checked on the raw params first, so a location without a queue
        // resolves no secrets here.
        let Some(raw_params) = snapshots
            .params
            .as_ref()
            .map(Params::as_string_map)
            .filter(|params| params.contains_key(QUEUE_URL_KEY))
        else {
            return Ok(None);
        };
        let params = super::build_s3_parameters(secrets, Some(&raw_params)).await;
        let queue_url = params
            .get(QUEUE_URL_PARAM)
            .expose()
            .ok()
            .map(str::trim)
            .unwrap_or_default();
        ensure!(!queue_url.is_empty(), EmptyQueueUrlSnafu);
        ensure!(!queue_url.starts_with("arn:"), QueueUrlIsArnSnafu);
        let region = region_from_queue_url(queue_url)
            .filter(|_| is_sqs_queue_url(queue_url))
            .context(QueueUrlNotSqsSnafu)?;

        let location = snapshots.location.as_deref().unwrap_or_default();
        let url = Url::parse(location)
            .ok()
            .context(InvalidLocationSnafu { location })?;
        ensure!(url.scheme() == "s3", LocationNotS3Snafu { location });
        let location =
            NotificationLocation::from_url(&url).context(InvalidLocationSnafu { location })?;

        // The same selection the snapshot object store makes: an explicit key
        // pair when both halves are set, the default AWS credential chain
        // otherwise.
        let credentials = match (
            params.get("key").expose().ok(),
            params.get("secret").expose().ok(),
        ) {
            (Some(access_key), Some(secret_key)) => SqsCredentials::Static {
                access_key: access_key.to_string(),
                secret_key: secret_key.to_string(),
                session_token: params
                    .get("session_token")
                    .expose()
                    .ok()
                    .map(ToString::to_string),
            },
            _ => SqsCredentials::DefaultChain,
        };

        Ok(Some(Self {
            queue_url: queue_url.to_string(),
            region,
            credentials,
            location,
        }))
    }
}

/// Each dataset's current snapshot id, keyed by the dataset name the snapshot
/// metadata uses, as last read after a commit notification.
type Announced = HashMap<String, u64>;

/// The SQS consumers that announce new snapshots, shared by every dataset in
/// the process.
///
/// A consumer starts when the first dataset on its queue subscribes and stops
/// when the last [`Subscription`] to it is dropped.
#[derive(Default)]
pub struct SnapshotNotifications {
    consumers: Mutex<HashMap<NotificationConfig, Weak<Consumer>>>,
}

impl SnapshotNotifications {
    /// Subscribe `manager`'s dataset to the announcements of the snapshot
    /// location in `config`. The first subscription to a queue starts its
    /// consumer on `runtime`, which reads the location's metadata through
    /// `manager`.
    pub fn subscribe(
        &self,
        config: &NotificationConfig,
        manager: &Arc<SnapshotManager>,
        runtime: &Handle,
    ) -> Subscription {
        self.subscribe_with(config, manager, |announce| {
            runtime.spawn(run_consumer(config.clone(), Arc::clone(manager), announce))
        })
    }

    fn subscribe_with(
        &self,
        config: &NotificationConfig,
        manager: &SnapshotManager,
        spawn: impl FnOnce(watch::Sender<Announced>) -> JoinHandle<()>,
    ) -> Subscription {
        let consumer = {
            let mut consumers = self.consumers.lock();
            consumers.retain(|_, consumer| consumer.strong_count() > 0);
            if let Some(consumer) = consumers.get(config).and_then(Weak::upgrade) {
                consumer
            } else {
                let (announce, announced) = watch::channel(Announced::new());
                let consumer = Arc::new(Consumer {
                    announced,
                    task: spawn(announce),
                });
                consumers.insert(config.clone(), Arc::downgrade(&consumer));
                consumer
            }
        };
        Subscription {
            announced: consumer.announced.clone(),
            dataset: manager.dataset_name().to_string(),
            _consumer: consumer,
        }
    }
}

struct Consumer {
    announced: watch::Receiver<Announced>,
    task: JoinHandle<()>,
}

impl Drop for Consumer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// One dataset's view of its snapshot location's announcements. Holding it
/// keeps the location's consumer running.
#[must_use = "dropping the subscription stops its announcements"]
pub struct Subscription {
    announced: watch::Receiver<Announced>,
    dataset: String,
    _consumer: Arc<Consumer>,
}

impl Subscription {
    /// Waits for the location to announce a current snapshot for this dataset
    /// and returns its id. It also returns when only another dataset's snapshot
    /// changed, with the id already returned, so callers compare it with what
    /// they have.
    pub async fn next_snapshot(&mut self) -> Option<u64> {
        loop {
            self.announced.changed().await.ok()?;
            if let Some(id) = self.announced.borrow_and_update().get(&self.dataset) {
                return Some(*id);
            }
        }
    }
}

/// Feeds a [`Subscription`] directly, without an SQS consumer, so code that
/// reacts to announcements can be tested. Test-only: behind `test-support`.
#[cfg(any(test, feature = "test-support"))]
pub struct TestAnnouncer {
    announce: watch::Sender<Announced>,
}

#[cfg(any(test, feature = "test-support"))]
impl TestAnnouncer {
    /// A subscription for `dataset` and the announcer that feeds it. Call it
    /// from within a Tokio runtime.
    pub fn subscribe(dataset: &str) -> (Self, Subscription) {
        let (announce, announced) = watch::channel(Announced::new());
        let consumer = Arc::new(Consumer {
            announced: announced.clone(),
            task: tokio::spawn(std::future::pending()),
        });
        let subscription = Subscription {
            announced,
            dataset: dataset.to_string(),
            _consumer: consumer,
        };
        (Self { announce }, subscription)
    }

    /// Announce `snapshot_id` as `dataset`'s current snapshot.
    pub fn announce(&self, dataset: &str, snapshot_id: u64) {
        self.announce.send_modify(|announced| {
            announced.insert(dataset.to_string(), snapshot_id);
        });
    }
}

/// What to do with one SQS message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Route {
    /// A new snapshot was committed: announce the metadata, then delete it.
    Commit,
    /// Nothing to announce: delete it.
    Ack,
    /// Names an object outside the snapshot location: leave it on the queue.
    Leave,
}

fn route(location: &NotificationLocation, message: &QueueMessage) -> Route {
    let events = match parse_notification_body(&message.body) {
        Ok(events) => events,
        Err(error) => {
            tracing::warn!("{}", invalid_notification_warning(location, &error));
            return Route::Ack;
        }
    };
    if let Some(outside) = events
        .iter()
        .find(|event| !matches_prefix(event, &location.bucket, &location.key_prefix))
    {
        tracing::error!(
            "{}",
            outside_location_error(location, &outside.bucket, &outside.key)
        );
        return Route::Leave;
    }
    // The snapshot files land before `metadata.json` names them, so only the
    // metadata rewrite means a new snapshot can be loaded.
    if events
        .iter()
        .any(|event| event.kind == ObjectEventKind::Created && location.is_metadata_key(&event.key))
    {
        Route::Commit
    } else {
        Route::Ack
    }
}

fn retry_backoff() -> RetryBackoff {
    RetryBackoffBuilder::new()
        .method(BackoffMethod::Exponential)
        .max_duration(Some(RETRY_BACKOFF_CAP))
        .build()
}

async fn run_consumer(
    config: NotificationConfig,
    manager: Arc<SnapshotManager>,
    announce: watch::Sender<Announced>,
) {
    let queue = connect(&config).await;
    tracing::info!("{}", listening_message(&config.location));
    consume(&queue, &config.location, &manager, &announce).await;
}

/// Build the SQS client, retrying while it cannot be built (for example while
/// credentials are not yet available).
async fn connect(config: &NotificationConfig) -> SqsQueue {
    let mut backoff = retry_backoff();
    let mut outage = SqsOutage::default();
    loop {
        match build_sqs_client(&config.credentials, &config.region).await {
            Ok(client) => return SqsQueue::new(client, config.queue_url.clone()),
            Err(error) => {
                outage.log_failure(&config.location, SqsCall::Connect, &error, Instant::now());
                tokio::time::sleep(backoff.next_backoff().unwrap_or(RETRY_BACKOFF_CAP)).await;
            }
        }
    }
}

async fn consume(
    queue: &dyn MessageQueue,
    location: &NotificationLocation,
    manager: &SnapshotManager,
    announce: &watch::Sender<Announced>,
) {
    let mut backoff = retry_backoff();
    let mut outage = SqsOutage::default();
    loop {
        match queue.receive().await {
            Ok(messages) => {
                backoff.reset();
                if let Some(lasted) = outage.recover(Instant::now()) {
                    tracing::info!("{}", receive_recovered_message(location, lasted));
                }
                process_batch(queue, location, manager, announce, messages).await;
            }
            Err(error) => {
                outage.log_failure(location, SqsCall::Receive, &error, Instant::now());
                tokio::time::sleep(backoff.next_backoff().unwrap_or(RETRY_BACKOFF_CAP)).await;
            }
        }
    }
}

/// An SQS request the consumer retries while it fails.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SqsCall {
    Connect,
    Receive,
}

/// Where an ongoing SQS outage stands, so it is logged when it starts, again
/// if it lasts past [`SQS_OUTAGE_ERROR_AFTER`], and when it ends, rather than
/// on every retry.
#[derive(Debug, Default)]
struct SqsOutage {
    since: Option<Instant>,
    escalated: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OutageStep {
    /// The first failure: log a warning.
    Started,
    /// Still failing past the threshold, reported once: log an error.
    Escalated(Duration),
    /// Still failing, already reported.
    Continuing,
}

impl SqsOutage {
    fn fail(&mut self, now: Instant) -> OutageStep {
        let Some(since) = self.since else {
            self.since = Some(now);
            return OutageStep::Started;
        };
        let lasted = now.saturating_duration_since(since);
        if !self.escalated && lasted >= SQS_OUTAGE_ERROR_AFTER {
            self.escalated = true;
            OutageStep::Escalated(lasted)
        } else {
            OutageStep::Continuing
        }
    }

    /// Ends the outage, returning how long it lasted, or `None` when there was
    /// none.
    fn recover(&mut self, now: Instant) -> Option<Duration> {
        self.escalated = false;
        self.since
            .take()
            .map(|since| now.saturating_duration_since(since))
    }

    fn log_failure(
        &mut self,
        location: &NotificationLocation,
        call: SqsCall,
        error: &dyn std::error::Error,
        now: Instant,
    ) {
        match self.fail(now) {
            OutageStep::Started => {
                tracing::warn!("{}", sqs_failure_message(location, call, error, None));
            }
            OutageStep::Escalated(lasted) => {
                tracing::error!(
                    "{}",
                    sqs_failure_message(location, call, error, Some(lasted))
                );
            }
            OutageStep::Continuing => {
                tracing::debug!("{}", sqs_failure_message(location, call, error, None));
            }
        }
    }
}

/// Route a received batch. When any message announces a commit, read the
/// location's metadata once and announce the current snapshot ids, then
/// delete the handled messages. A commit whose metadata cannot be read stays
/// on the queue, so it is delivered again.
async fn process_batch(
    queue: &dyn MessageQueue,
    location: &NotificationLocation,
    manager: &SnapshotManager,
    announce: &watch::Sender<Announced>,
    messages: Vec<QueueMessage>,
) {
    let mut acks = Vec::with_capacity(messages.len());
    let mut commits = Vec::new();
    for message in messages {
        match route(location, &message) {
            Route::Commit => commits.push(message.receipt_handle),
            Route::Ack => acks.push(message.receipt_handle),
            Route::Leave => {}
        }
    }

    if !commits.is_empty() {
        match manager.current_snapshot_ids().await {
            Ok(current) => {
                announce.send_if_modified(|announced| {
                    let changed = *announced != current;
                    *announced = current;
                    changed
                });
                acks.append(&mut commits);
            }
            Err(error) => {
                tracing::warn!("{}", metadata_read_warning(location, &error));
            }
        }
    }

    for receipt_handle in acks {
        if let Err(error) = queue.delete(&receipt_handle).await {
            tracing::warn!(
                "Snapshot location '{location}' handled an S3 event notification but could not delete it from the SQS queue in `s3_queue_url`, so it will be delivered again. Cause: {error}. Grant `sqs:DeleteMessage` on the queue. See: {SNAPSHOTS_DOCS}"
            );
        }
    }
}

/// The warning for a snapshot location with `s3_queue_url` set when no
/// dataset uses `refresh_mode: snapshot`, which is the only mode that reads the
/// queue. `None` when the queue is not set or a dataset reads it.
#[must_use]
pub fn unread_queue_warning(
    snapshots: &Snapshots,
    any_snapshot_mode_dataset: bool,
) -> Option<String> {
    let configured = snapshots
        .params
        .as_ref()
        .is_some_and(|params| params.data.contains_key(QUEUE_URL_KEY));
    (snapshots.enabled && configured && !any_snapshot_mode_dataset).then(|| {
        format!(
            "`snapshots.params.s3_queue_url` is set, but no dataset uses `refresh_mode: snapshot`, so the SQS queue is not read. Set `refresh_mode: snapshot` on the datasets that load snapshots, or remove `s3_queue_url`. See: {SNAPSHOTS_DOCS}"
        )
    })
}

fn listening_message(location: &NotificationLocation) -> String {
    format!(
        "Snapshot location '{location}' is listening for S3 event notifications on the SQS queue in `s3_queue_url`, so datasets with `refresh_mode: snapshot` reload as soon as a new snapshot is published."
    )
}

/// The line for a failing SQS call: the start of an outage (`lasted` is
/// `None`), or one that has lasted past [`SQS_OUTAGE_ERROR_AFTER`].
fn sqs_failure_message(
    location: &NotificationLocation,
    call: SqsCall,
    error: &dyn std::error::Error,
    lasted: Option<Duration>,
) -> String {
    let (action, fix) = match call {
        SqsCall::Connect => (
            "connect to the SQS queue in `s3_queue_url`",
            "Check the AWS credentials for the snapshot location.",
        ),
        SqsCall::Receive => (
            "receive S3 event notifications from the SQS queue in `s3_queue_url`",
            "Check the queue URL and the `sqs:ReceiveMessage` permission.",
        ),
    };
    let lasted = lasted.map_or_else(String::new, |lasted| {
        format!(" for {}", format_duration(lasted))
    });
    format!(
        "Failed to {action} for snapshot location '{location}'{lasted}, so datasets with `refresh_mode: snapshot` fall back to checking the location every `refresh_check_interval` until it recovers. Cause: {error}. {fix} See: {SNAPSHOTS_DOCS}"
    )
}

fn receive_recovered_message(location: &NotificationLocation, lasted: Duration) -> String {
    format!(
        "Snapshot location '{location}' is receiving S3 event notifications from the SQS queue in `s3_queue_url` again after {}, so datasets with `refresh_mode: snapshot` reload as soon as a new snapshot is published.",
        format_duration(lasted)
    )
}

fn metadata_read_warning(location: &NotificationLocation, error: &dyn std::error::Error) -> String {
    format!(
        "Failed to read the snapshot metadata of snapshot location '{location}' after an S3 event notification, so the notification was left on the SQS queue and datasets reload when it is delivered again or on `refresh_check_interval`. Cause: {error}. See: {SNAPSHOTS_DOCS}"
    )
}

fn invalid_notification_warning(
    location: &NotificationLocation,
    error: &dyn std::error::Error,
) -> String {
    format!(
        "Snapshot location '{location}' dropped an SQS message that is not an S3 event notification, so it was deleted without reloading any dataset. Cause: {error}. Point only the location's S3 event notifications at the queue in `s3_queue_url`. See: {SNAPSHOTS_DOCS}"
    )
}

fn outside_location_error(location: &NotificationLocation, bucket: &str, key: &str) -> String {
    format!(
        "Snapshot location '{location}' received an S3 event notification for 's3://{bucket}/{key}', which is outside the location, so the SQS message was left on the queue (not deleted) and will be delivered again after each visibility timeout until it expires. The queue in `s3_queue_url` must receive only this location's notifications: filter the bucket notification by the location's prefix, or fan out through SNS to a queue per consumer. See: {SNAPSHOTS_DOCS}"
    )
}

#[cfg(test)]
mod tests {
    use super::super::tests::build_manager_for_api_tests;
    use super::super::{DatasetMetadata, SnapshotMetadata};
    use super::*;
    use object_store::ObjectStoreExt;
    use object_store::memory::InMemory;
    use s3_event_notifications::queue::QueueError;
    use std::sync::atomic::{AtomicUsize, Ordering};

    const QUEUE_URL: &str = "https://sqs.us-east-1.amazonaws.com/123456789012/spice-snapshots";

    fn location() -> NotificationLocation {
        NotificationLocation::from_url(
            &Url::parse("s3://my-bucket/spice/snapshots/").expect("valid location"),
        )
        .expect("an s3 location")
    }

    fn config() -> NotificationConfig {
        NotificationConfig {
            queue_url: QUEUE_URL.to_string(),
            region: "us-east-1".to_string(),
            credentials: SqsCredentials::DefaultChain,
            location: location(),
        }
    }

    fn message(body: String, receipt_handle: &str) -> QueueMessage {
        QueueMessage {
            body,
            receipt_handle: receipt_handle.to_string(),
        }
    }

    fn s3_body(event_name: &str, bucket: &str, key: &str) -> String {
        serde_json::json!({
            "Records": [{
                "eventSource": "aws:s3",
                "eventName": event_name,
                "s3": { "bucket": { "name": bucket }, "object": { "key": key } }
            }]
        })
        .to_string()
    }

    fn metadata_created() -> String {
        s3_body(
            "ObjectCreated:Put",
            "my-bucket",
            "spice/snapshots/metadata.json",
        )
    }

    /// A manager over an in-memory snapshot location whose metadata names
    /// `current` as each dataset's current snapshot.
    async fn manager_with_current(current: &[(&str, u64)]) -> Arc<SnapshotManager> {
        let manager = build_manager_for_api_tests(Arc::new(InMemory::new()));
        let mut metadata = SnapshotMetadata::empty("memory://snapshots".to_string(), 0);
        for (dataset, id) in current {
            metadata.datasets.insert(
                (*dataset).to_string(),
                DatasetMetadata {
                    name: (*dataset).to_string(),
                    current_snapshot_id: Some(*id),
                    ..DatasetMetadata::default()
                },
            );
        }
        manager
            .object_store
            .put(
                &manager.metadata_path(),
                serde_json::to_vec(&metadata)
                    .expect("metadata serializes")
                    .into(),
            )
            .await
            .expect("metadata is written");
        Arc::new(manager)
    }

    /// A queue that records deletes.
    #[derive(Default)]
    struct RecordingQueue {
        deleted: Mutex<Vec<String>>,
    }

    #[async_trait::async_trait]
    impl MessageQueue for RecordingQueue {
        async fn receive(&self) -> std::result::Result<Vec<QueueMessage>, QueueError> {
            Ok(Vec::new())
        }

        async fn delete(&self, receipt_handle: &str) -> std::result::Result<(), QueueError> {
            self.deleted.lock().push(receipt_handle.to_string());
            Ok(())
        }
    }

    #[test]
    fn a_metadata_rewrite_is_a_commit() {
        assert_eq!(
            route(&location(), &message(metadata_created(), "r")),
            Route::Commit
        );
    }

    #[test]
    fn a_snapshot_file_or_a_removal_is_acknowledged_without_a_commit() {
        let file = s3_body(
            "ObjectCreated:CompleteMultipartUpload",
            "my-bucket",
            "spice/snapshots/month=2026-09/day=2026-09-23/dataset=orders/orders_20260923T000000Z.duckdb",
        );
        assert_eq!(route(&location(), &message(file, "r")), Route::Ack);

        let removed = s3_body(
            "ObjectRemoved:Delete",
            "my-bucket",
            "spice/snapshots/metadata.json",
        );
        assert_eq!(route(&location(), &message(removed, "r")), Route::Ack);

        // Renewing a snapshot writer lease is not a new snapshot either.
        let lease = s3_body(
            "ObjectCreated:Put",
            "my-bucket",
            "spice/snapshots/leases/orders.json",
        );
        assert_eq!(route(&location(), &message(lease, "r")), Route::Ack);
    }

    #[test]
    fn a_metadata_json_in_a_nested_prefix_is_not_the_locations_metadata() {
        let nested = s3_body(
            "ObjectCreated:Put",
            "my-bucket",
            "spice/snapshots/other/metadata.json",
        );
        assert_eq!(route(&location(), &message(nested, "r")), Route::Ack);
    }

    #[test]
    fn a_notification_outside_the_location_is_left_on_the_queue() {
        let other_prefix = s3_body("ObjectCreated:Put", "my-bucket", "events/metadata.json");
        assert_eq!(
            route(&location(), &message(other_prefix, "r")),
            Route::Leave
        );

        let other_bucket = s3_body(
            "ObjectCreated:Put",
            "other-bucket",
            "spice/snapshots/metadata.json",
        );
        assert_eq!(
            route(&location(), &message(other_bucket, "r")),
            Route::Leave
        );

        // A sibling prefix that only starts with the location's name.
        let sibling = s3_body(
            "ObjectCreated:Put",
            "my-bucket",
            "spice/snapshots-old/metadata.json",
        );
        assert_eq!(route(&location(), &message(sibling, "r")), Route::Leave);
    }

    #[test]
    fn a_body_that_is_not_a_notification_is_acknowledged() {
        assert_eq!(
            route(&location(), &message("not json".to_string(), "r")),
            Route::Ack
        );
        let test_event = serde_json::json!({ "Event": "s3:TestEvent" }).to_string();
        assert_eq!(route(&location(), &message(test_event, "r")), Route::Ack);
    }

    #[test]
    fn sns_and_eventbridge_notifications_are_commits() {
        let sns = serde_json::json!({ "Type": "Notification", "Message": metadata_created() })
            .to_string();
        assert_eq!(route(&location(), &message(sns, "r")), Route::Commit);

        let eventbridge = serde_json::json!({
            "source": "aws.s3",
            "detail-type": "Object Created",
            "detail": {
                "bucket": { "name": "my-bucket" },
                "object": { "key": "spice/snapshots/metadata.json" }
            }
        })
        .to_string();
        assert_eq!(
            route(&location(), &message(eventbridge, "r")),
            Route::Commit
        );
    }

    #[test]
    fn the_locations_keys_match_the_snapshot_writers_paths() {
        let location = location();
        assert_eq!(location.key_prefix, "spice/snapshots/");
        assert!(location.is_metadata_key("spice/snapshots/metadata.json"));
        assert_eq!(location.to_string(), "s3://my-bucket/spice/snapshots");

        let no_trailing_slash = NotificationLocation::from_url(
            &Url::parse("s3://my-bucket/spice/snapshots").expect("url"),
        )
        .expect("an s3 location");
        assert_eq!(no_trailing_slash, location);

        let root = NotificationLocation::from_url(&Url::parse("s3://my-bucket").expect("url"))
            .expect("an s3 location");
        assert_eq!(root.key_prefix, "");
        assert!(root.is_metadata_key("metadata.json"));
        assert_eq!(root.to_string(), "s3://my-bucket");
    }

    #[tokio::test]
    async fn a_commit_announces_every_datasets_current_snapshot_and_deletes_the_batch() {
        let manager = manager_with_current(&[("orders", 7), ("customers", 3)]).await;
        let queue = RecordingQueue::default();
        let (announce, mut announced) = watch::channel(Announced::new());

        process_batch(
            &queue,
            &location(),
            &manager,
            &announce,
            vec![
                message(metadata_created(), "metadata-1"),
                message(metadata_created(), "metadata-2"),
                message("not json".to_string(), "poison"),
                message(
                    s3_body("ObjectCreated:Put", "my-bucket", "events/a.parquet"),
                    "outside",
                ),
            ],
        )
        .await;

        assert!(announced.has_changed().expect("sender alive"));
        assert_eq!(
            *announced.borrow_and_update(),
            Announced::from([("orders".to_string(), 7), ("customers".to_string(), 3)])
        );
        let mut deleted = queue.deleted.lock().clone();
        deleted.sort();
        assert_eq!(
            deleted,
            vec!["metadata-1", "metadata-2", "poison"],
            "the outside-location message must stay on the queue"
        );
    }

    #[tokio::test]
    async fn an_unchanged_metadata_announces_nothing_new() {
        let manager = manager_with_current(&[("orders", 7)]).await;
        let queue = RecordingQueue::default();
        let (announce, mut announced) = watch::channel(Announced::new());
        let batch = || vec![message(metadata_created(), "metadata")];

        process_batch(&queue, &location(), &manager, &announce, batch()).await;
        assert!(announced.has_changed().expect("sender alive"));
        announced.borrow_and_update();

        process_batch(&queue, &location(), &manager, &announce, batch()).await;
        assert!(
            !announced.has_changed().expect("sender alive"),
            "a metadata rewrite that moved no dataset must not wake the subscribers"
        );
    }

    #[tokio::test]
    async fn a_commit_whose_metadata_cannot_be_read_stays_on_the_queue() {
        let manager = build_manager_for_api_tests(Arc::new(InMemory::new()));
        manager
            .object_store
            .put(&manager.metadata_path(), b"not json".to_vec().into())
            .await
            .expect("metadata is written");
        let queue = RecordingQueue::default();
        let (announce, announced) = watch::channel(Announced::new());

        process_batch(
            &queue,
            &location(),
            &manager,
            &announce,
            vec![message(metadata_created(), "metadata")],
        )
        .await;

        assert!(!announced.has_changed().expect("sender alive"));
        assert!(queue.deleted.lock().is_empty());
    }

    #[tokio::test]
    async fn subscriptions_share_a_consumer_that_stops_with_the_last_one() {
        let notifications = SnapshotNotifications::default();
        let orders = build_manager_for_api_tests(Arc::new(InMemory::new()));
        let spawns = AtomicUsize::new(0);
        let subscribe = || {
            notifications.subscribe_with(&config(), &orders, |_announce| {
                spawns.fetch_add(1, Ordering::SeqCst);
                tokio::spawn(futures::future::pending())
            })
        };
        let running = || {
            notifications
                .consumers
                .lock()
                .values()
                .filter(|consumer| consumer.upgrade().is_some())
                .count()
        };

        let first = subscribe();
        let second = subscribe();
        assert_eq!(
            spawns.load(Ordering::SeqCst),
            1,
            "subscriptions to one queue share its consumer"
        );
        assert_eq!(running(), 1);

        drop(first);
        assert_eq!(running(), 1, "still used by `second`");
        drop(second);
        assert_eq!(
            running(),
            0,
            "the consumer stops with its last subscription"
        );

        let _again = subscribe();
        assert_eq!(
            spawns.load(Ordering::SeqCst),
            2,
            "a later subscription starts a new consumer"
        );
    }

    #[tokio::test]
    async fn a_subscription_sees_only_its_own_datasets_snapshots() {
        let notifications = SnapshotNotifications::default();
        let orders = build_manager_for_api_tests(Arc::new(InMemory::new()));
        let mut announce = None;
        let mut subscription = notifications.subscribe_with(&config(), &orders, |sender| {
            announce = Some(sender);
            tokio::spawn(futures::future::pending())
        });
        let announce = announce.expect("the consumer was started");

        announce.send_replace(Announced::from([("customers".to_string(), 4)]));
        announce.send_replace(Announced::from([
            ("customers".to_string(), 4),
            (orders.dataset_name().to_string(), 9),
        ]));
        let id = tokio::time::timeout(Duration::from_secs(5), subscription.next_snapshot())
            .await
            .expect("the dataset's snapshot is announced");
        assert_eq!(id, Some(9));
    }

    #[test]
    fn a_queue_no_dataset_reads_is_warned_about() {
        let with_queue = snapshots(
            "s3://my-bucket/spice/snapshots/",
            &[("s3_queue_url", QUEUE_URL)],
        );
        let warning = unread_queue_warning(&with_queue, false).expect("nothing reads the queue");
        assert!(warning.contains("`s3_queue_url`"));
        assert!(warning.contains("`refresh_mode: snapshot`"));
        assert!(warning.contains(SNAPSHOTS_DOCS));
        assert!(!warning.contains(QUEUE_URL), "never log the queue URL");

        assert!(unread_queue_warning(&with_queue, true).is_none());
        let without_queue = snapshots("s3://my-bucket/spice/snapshots/", &[]);
        assert!(unread_queue_warning(&without_queue, false).is_none());
        let disabled = Snapshots {
            enabled: false,
            ..with_queue
        };
        assert!(unread_queue_warning(&disabled, false).is_none());
    }

    #[test]
    fn log_lines_name_the_location_the_param_and_the_impact() {
        let location = location();
        let listening = listening_message(&location);
        assert!(listening.contains("'s3://my-bucket/spice/snapshots'"));
        assert!(listening.contains("`s3_queue_url`"));
        assert!(listening.contains("`refresh_mode: snapshot`"));

        let error = std::io::Error::other("AccessDenied");
        let receive = sqs_failure_message(&location, SqsCall::Receive, &error, None);
        assert!(receive.contains("'s3://my-bucket/spice/snapshots'"));
        assert!(
            receive.contains("fall back to checking the location every `refresh_check_interval`")
        );
        assert!(receive.contains("Cause: AccessDenied"));
        assert!(receive.contains("`sqs:ReceiveMessage`"));
        assert!(receive.contains(SNAPSHOTS_DOCS));

        let lasting = sqs_failure_message(
            &location,
            SqsCall::Receive,
            &error,
            Some(Duration::from_secs(301)),
        );
        assert!(lasting.contains("'s3://my-bucket/spice/snapshots' for 5m,"));

        let connect = sqs_failure_message(&location, SqsCall::Connect, &error, None);
        assert!(connect.contains("Failed to connect to the SQS queue in `s3_queue_url`"));
        assert!(connect.contains("AWS credentials"));

        let recovered = receive_recovered_message(&location, Duration::from_secs(42));
        assert!(recovered.contains("again after 42s"));

        let outside = outside_location_error(&location, "my-bucket", "events/a.parquet");
        assert!(outside.contains("'s3://my-bucket/events/a.parquet'"));
        assert!(outside.contains("left on the queue (not deleted)"));
        assert!(outside.contains(SNAPSHOTS_DOCS));

        for line in [
            listening,
            receive,
            lasting,
            connect,
            recovered,
            outside,
            metadata_read_warning(&location, &error),
            invalid_notification_warning(&location, &error),
        ] {
            assert!(!line.contains(QUEUE_URL), "never log the queue URL: {line}");
            assert!(!line.contains('\n'), "log lines are single-line: {line}");
        }
    }

    #[test]
    fn an_sqs_outage_is_warned_once_escalated_once_and_ends_on_recovery() {
        let start = Instant::now();
        let mut outage = SqsOutage::default();
        assert_eq!(outage.recover(start), None, "no outage to end");

        assert_eq!(outage.fail(start), OutageStep::Started);
        assert_eq!(
            outage.fail(start + Duration::from_secs(30)),
            OutageStep::Continuing
        );
        let past = start + SQS_OUTAGE_ERROR_AFTER;
        assert_eq!(
            outage.fail(past),
            OutageStep::Escalated(SQS_OUTAGE_ERROR_AFTER)
        );
        assert_eq!(
            outage.fail(past + Duration::from_secs(30)),
            OutageStep::Continuing,
            "the error is logged once per outage"
        );

        let ended = past + Duration::from_mins(1);
        assert_eq!(
            outage.recover(ended),
            Some(SQS_OUTAGE_ERROR_AFTER + Duration::from_mins(1))
        );
        assert_eq!(outage.recover(ended), None);
        assert_eq!(
            outage.fail(ended),
            OutageStep::Started,
            "the next outage is reported afresh"
        );
    }

    fn snapshots(location: &str, params: &[(&str, &str)]) -> Snapshots {
        let params: HashMap<String, String> = params
            .iter()
            .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
            .collect();
        Snapshots {
            location: Some(location.to_string()),
            params: Some(Params::from_string_map(params)),
            ..Snapshots::default()
        }
    }

    async fn resolve(snapshots: &Snapshots) -> Result<Option<NotificationConfig>> {
        NotificationConfig::resolve(snapshots, Arc::new(RwLock::new(Secrets::default()))).await
    }

    #[tokio::test]
    async fn resolve_reads_the_queue_region_and_location() {
        let resolved = resolve(&snapshots(
            "s3://my-bucket/spice/snapshots/",
            &[("s3_region", "us-west-2"), ("s3_queue_url", QUEUE_URL)],
        ))
        .await
        .expect("valid config")
        .expect("a queue is configured");
        assert_eq!(
            resolved.region, "us-east-1",
            "the region comes from the queue URL"
        );
        assert_eq!(resolved.location, location());
        assert_eq!(resolved.credentials, SqsCredentials::DefaultChain);
    }

    #[tokio::test]
    async fn resolve_uses_the_locations_explicit_keys() {
        let resolved = resolve(&snapshots(
            "s3://my-bucket/spice/snapshots/",
            &[
                ("s3_queue_url", QUEUE_URL),
                ("s3_auth", "key"),
                ("s3_key", "AKIAEXAMPLE"),
                ("s3_secret", "secret"),
            ],
        ))
        .await
        .expect("valid config")
        .expect("a queue is configured");
        assert_eq!(
            resolved.credentials,
            SqsCredentials::Static {
                access_key: "AKIAEXAMPLE".to_string(),
                secret_key: "secret".to_string(),
                session_token: None,
            }
        );
    }

    #[tokio::test]
    async fn resolve_without_a_queue_is_none() {
        assert!(
            resolve(&snapshots("s3://my-bucket/spice/snapshots/", &[]))
                .await
                .expect("valid config")
                .is_none()
        );
    }

    #[tokio::test]
    async fn resolve_fails_closed_on_a_bad_queue_or_location() {
        let arn = resolve(&snapshots(
            "s3://my-bucket/snapshots/",
            &[(
                "s3_queue_url",
                "arn:aws:sqs:us-east-1:123456789012:spice-snapshots",
            )],
        ))
        .await
        .err()
        .expect("an ARN is not a queue URL");
        assert!(matches!(arn, Error::QueueUrlIsArn), "{arn}");

        let loopback = resolve(&snapshots(
            "s3://my-bucket/snapshots/",
            &[(
                "s3_queue_url",
                "https://127.0.0.1/123456789012/spice-snapshots",
            )],
        ))
        .await
        .err()
        .expect("loopback is not an SQS queue");
        assert!(matches!(loopback, Error::QueueUrlNotSqs), "{loopback}");

        let empty = resolve(&snapshots(
            "s3://my-bucket/snapshots/",
            &[("s3_queue_url", " ")],
        ))
        .await
        .err()
        .expect("an empty queue URL");
        assert!(matches!(empty, Error::EmptyQueueUrl), "{empty}");

        let gcs = resolve(&snapshots(
            "gs://my-bucket/snapshots/",
            &[("s3_queue_url", QUEUE_URL)],
        ))
        .await
        .err()
        .expect("S3 notifications only come from S3");
        assert!(
            matches!(gcs, Error::LocationNotS3 { ref location } if location == "gs://my-bucket/snapshots/"),
            "{gcs}"
        );
        assert!(!gcs.to_string().contains(QUEUE_URL));
    }
}
