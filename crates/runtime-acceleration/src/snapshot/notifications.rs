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
//! One consumer per queue serves every snapshot location that names the queue.
//! SQS hands each message to a single receiver, so a consumer per location or
//! per dataset would split the notifications between them. On a commit
//! notification the consumer reads that location's `metadata.json` once and
//! announces the current snapshot id of every dataset in it. Each dataset asks
//! for a reload only when its own id advances, so one dataset's publish does
//! not restart another dataset's reload.
//!
//! A queue subscribed to the whole `runtime.state` prefix also receives
//! notifications for the location's other folders (`cluster/`, `rate-control/`,
//! ...). When a snapshot location is the `snapshots/` folder of
//! `runtime.state.location`, those are Spice's own state and are deleted
//! without a log. A notification for any other object is logged as an error
//! and left on the queue.
//!
//! An announcement only decides when to ask. What a dataset loads is still
//! decided by [`super::SnapshotManager::download_if_newer`], which compares
//! snapshot ids, checks the schema and verifies the checksum, so a duplicate,
//! reordered or forged notification cannot change what is served.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use runtime_secrets::Secrets;
use s3_event_notifications::client::{SqsCredentials, build_sqs_client};
use s3_event_notifications::event::{
    ObjectEventKind, S3ObjectEvent, matches_prefix, parse_notification_body, prefix_display,
};
use s3_event_notifications::queue::{MessageQueue, QueueMessage, SqsQueue};
use s3_event_notifications::queue_url::{is_sqs_queue_url, region_from_queue_url};
use snafu::prelude::*;
use spicepod::component::runtime::runtime_state_folder;
use spicepod::component::snapshot::{SNAPSHOTS_QUEUE_URL_PARAM, SNAPSHOTS_STATE_FOLDER, Snapshots};
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
/// [`SNAPSHOTS_QUEUE_URL_PARAM`].
pub(super) const QUEUE_URL_PARAM: &str = runtime_state_location::SNAPSHOT_QUEUE_URL_PARAM;

const RETRY_BACKOFF_CAP: Duration = Duration::from_secs(30);
/// How long SQS can keep failing before the outage is logged as an error
/// rather than a warning. Datasets keep checking the location on
/// `refresh_check_interval` throughout, so this marks when an operator should
/// act, not when data goes stale.
const SQS_OUTAGE_ERROR_AFTER: Duration = Duration::from_mins(5);

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "`s3_queue_url` is empty. Set it to the URL of the SQS queue that receives the snapshot location's S3 event notifications, or remove it. See: {SNAPSHOTS_DOCS}"
    ))]
    EmptyQueueUrl,

    #[snafu(display(
        "`s3_queue_url` must be an SQS queue URL (https://sqs.<region>.amazonaws.com/<account>/<queue>), not an ARN. See: {SNAPSHOTS_DOCS}"
    ))]
    QueueUrlIsArn,

    #[snafu(display(
        "`s3_queue_url` is not an SQS queue URL. Use a URL like https://sqs.<region>.amazonaws.com/<account>/<queue>. See: {SNAPSHOTS_DOCS}"
    ))]
    QueueUrlNotSqs,

    #[snafu(display(
        "`s3_queue_url` requires an `s3://` snapshot location, because only S3 sends S3 event notifications, but the location is '{location}'. Remove `s3_queue_url` or move the snapshots to S3. See: {SNAPSHOTS_DOCS}"
    ))]
    LocationNotS3 { location: String },

    #[snafu(display(
        "The snapshot location '{location}' is not a valid URL, so snapshot notifications cannot be matched to it. Use a location like 's3://my-bucket/spice/snapshots/'. See: {SNAPSHOTS_DOCS}"
    ))]
    InvalidLocation { location: String },

    #[snafu(display(
        "The snapshot location's params are invalid, so its S3 event notifications cannot be set up. Cause: {source}. Fix the parameter and restart. See: {SNAPSHOTS_DOCS}"
    ))]
    InvalidParams {
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Where a snapshot location's S3 event notifications arrive, resolved from a
/// dataset's snapshot configuration. It holds the secret queue URL and may hold
/// keys, so it deliberately has no `Debug`.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct NotificationConfig {
    queue: Queue,
    location: NotificationLocation,
    /// The `runtime.state` location when `location` is its `snapshots/` folder.
    state_root: Option<NotificationLocation>,
}

/// An SQS queue, and the region and credentials that reach it. Snapshot
/// locations that name the same queue URL share its consumer.
#[derive(Clone, PartialEq, Eq, Hash)]
struct Queue {
    url: String,
    region: String,
    credentials: SqsCredentials,
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

    /// Whether `event` names an object under the location.
    fn contains(&self, event: &S3ObjectEvent) -> bool {
        matches_prefix(event, &self.bucket, &self.key_prefix)
    }

    /// Whether `event` names the location's `metadata.json`, the object whose
    /// rewrite makes a snapshot current.
    fn is_metadata(&self, event: &S3ObjectEvent) -> bool {
        event.bucket == self.bucket
            && event.key.strip_prefix(&self.key_prefix) == Some(METADATA_FILE_NAME)
    }
}

impl std::fmt::Display for NotificationLocation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&prefix_display(&self.bucket, &self.key_prefix))
    }
}

impl NotificationConfig {
    /// Resolve the notification queue for `snapshots`, or `Ok(None)` when the
    /// snapshot location has no queue configured. `runtime_state_location` is
    /// the spicepod's `runtime.state.location`, if any.
    ///
    /// # Errors
    ///
    /// Returns an [`Error`] naming `s3_queue_url` when the queue URL is empty,
    /// an ARN, or not an SQS queue URL, and when the snapshot location is not
    /// on S3.
    pub async fn resolve(
        snapshots: &Snapshots,
        runtime_state_location: Option<&str>,
        secrets: Arc<RwLock<Secrets>>,
    ) -> Result<Option<Self>> {
        // Checked on the raw params first, so a location without a queue
        // resolves no secrets here.
        let Some(raw_params) = snapshots
            .params
            .as_ref()
            .map(Params::as_string_map)
            .filter(|params| params.contains_key(SNAPSHOTS_QUEUE_URL_PARAM))
        else {
            return Ok(None);
        };
        let params = super::build_s3_parameters(secrets, Some(&raw_params))
            .await
            .context(InvalidParamsSnafu)?;
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
        let state_root = runtime_state_location.and_then(|state_location| {
            let state_root = s3_notification_location(state_location)?;
            let snapshots_folder = s3_notification_location(&runtime_state_folder(
                state_location,
                SNAPSHOTS_STATE_FOLDER,
            ))?;
            (snapshots_folder == location).then_some(state_root)
        });

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
            queue: Queue {
                url: queue_url.to_string(),
                region,
                credentials,
            },
            location,
            state_root,
        }))
    }
}

/// The bucket and prefix of an `s3://` location, or `None` for any other.
fn s3_notification_location(location: &str) -> Option<NotificationLocation> {
    let url = Url::parse(location)
        .ok()
        .filter(|url| url.scheme() == "s3")?;
    NotificationLocation::from_url(&url)
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
    runtime_state_location: Option<String>,
    /// Keyed by queue URL alone: SQS gives each message to one receiver, so a
    /// second consumer of the queue would take messages its locations do not hold.
    consumers: Mutex<HashMap<String, Weak<Consumer>>>,
}

impl SnapshotNotifications {
    /// The consumers of a runtime whose `runtime.state.location` is
    /// `runtime_state_location`.
    #[must_use]
    pub fn new(runtime_state_location: Option<String>) -> Self {
        Self {
            runtime_state_location,
            consumers: Mutex::default(),
        }
    }

    /// Subscribe to the queue configured for a bootstrapping or refreshing
    /// acceleration. Sources without notification configuration keep polling.
    ///
    /// # Errors
    /// Returns an error when the configured queue or snapshot location is invalid.
    pub async fn subscribe_for_behavior(
        &self,
        behavior: &super::SnapshotBehavior,
        manager: &Arc<SnapshotManager>,
    ) -> Result<Option<Subscription>> {
        let (super::SnapshotBehavior::Enabled(snapshots, secrets, runtime, _)
        | super::SnapshotBehavior::BootstrapOnly(snapshots, secrets, runtime)) = behavior
        else {
            return Ok(None);
        };
        let Some(secrets) = secrets.upgrade() else {
            return Ok(None);
        };
        Ok(
            NotificationConfig::resolve(snapshots, self.runtime_state_location.as_deref(), secrets)
                .await?
                .map(|config| self.subscribe(&config, manager, runtime)),
        )
    }

    /// Subscribe `manager`'s dataset to the announcements of the snapshot
    /// location in `config`. The first subscription to a queue starts its
    /// consumer on `runtime`. The first subscription to a location gives the
    /// consumer `manager` to read the location's metadata.
    pub fn subscribe(
        &self,
        config: &NotificationConfig,
        manager: &Arc<SnapshotManager>,
        runtime: &Handle,
    ) -> Subscription {
        self.subscribe_with(config, manager, |feeds| {
            runtime.spawn(run_consumer(config.queue.clone(), feeds))
        })
    }

    fn subscribe_with(
        &self,
        config: &NotificationConfig,
        manager: &Arc<SnapshotManager>,
        spawn: impl FnOnce(Arc<Feeds>) -> JoinHandle<()>,
    ) -> Subscription {
        let consumer = {
            let mut consumers = self.consumers.lock();
            consumers.retain(|_, consumer| consumer.strong_count() > 0);
            if let Some(consumer) = consumers.get(&config.queue.url).and_then(Weak::upgrade) {
                if consumer.credentials != config.queue.credentials {
                    tracing::warn!(
                        "{}",
                        shared_queue_credentials_warning(&config.location, &consumer.opened_for)
                    );
                }
                consumer
            } else {
                let feeds = Arc::new(Feeds::default());
                let consumer = Arc::new(Consumer {
                    credentials: config.queue.credentials.clone(),
                    opened_for: config.location.clone(),
                    feeds: Arc::clone(&feeds),
                    task: spawn(feeds),
                });
                consumers.insert(config.queue.url.clone(), Arc::downgrade(&consumer));
                consumer
            }
        };
        let feed = consumer.feeds.feed(config, manager, &consumer);
        Subscription {
            announced: feed.announced.clone(),
            dataset: manager.dataset_name().to_string(),
            _feed: Some(feed),
        }
    }
}

/// One queue's consumer task. Dropping the last [`Feed`] that holds it stops
/// the task.
struct Consumer {
    /// The credentials it reads the queue with: those of `opened_for`.
    credentials: SqsCredentials,
    /// The location whose subscription started it.
    opened_for: NotificationLocation,
    feeds: Arc<Feeds>,
    task: JoinHandle<()>,
}

impl Drop for Consumer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// The snapshot locations a consumer serves, each while a subscription holds
/// its [`Feed`].
#[derive(Default)]
struct Feeds(Mutex<HashMap<NotificationLocation, Weak<Feed>>>);

impl Feeds {
    /// The feed of `config`'s location, created with `manager` when the
    /// location has none.
    fn feed(
        &self,
        config: &NotificationConfig,
        manager: &Arc<SnapshotManager>,
        consumer: &Arc<Consumer>,
    ) -> Arc<Feed> {
        let mut feeds = self.0.lock();
        feeds.retain(|_, feed| feed.strong_count() > 0);
        if let Some(feed) = feeds.get(&config.location).and_then(Weak::upgrade) {
            return feed;
        }
        let (announce, announced) = watch::channel(Announced::new());
        let feed = Arc::new(Feed {
            location: config.location.clone(),
            state_root: config.state_root.clone(),
            manager: Arc::clone(manager),
            announce,
            announced,
            _consumer: Arc::clone(consumer),
        });
        feeds.insert(config.location.clone(), Arc::downgrade(&feed));
        feed
    }

    /// The locations served now, in a stable order.
    fn live(&self) -> Vec<Arc<Feed>> {
        let mut live: Vec<_> = self.0.lock().values().filter_map(Weak::upgrade).collect();
        live.sort_by_key(|feed| feed.location.to_string());
        live
    }
}

/// One snapshot location's announcements.
struct Feed {
    location: NotificationLocation,
    /// The `runtime.state` location when `location` is its `snapshots/` folder.
    state_root: Option<NotificationLocation>,
    /// Reads the location's metadata.
    manager: Arc<SnapshotManager>,
    announce: watch::Sender<Announced>,
    /// Never read, so a subscription cloned from it sees the latest
    /// announcement, even one made before it subscribed.
    announced: watch::Receiver<Announced>,
    _consumer: Arc<Consumer>,
}

/// One dataset's view of its snapshot location's announcements. Holding it
/// keeps the location's consumer running.
#[must_use = "dropping the subscription stops its announcements"]
#[derive(Clone)]
pub struct Subscription {
    announced: watch::Receiver<Announced>,
    dataset: String,
    /// `None` for a [`TestAnnouncer`] subscription, which has no consumer.
    _feed: Option<Arc<Feed>>,
}

impl std::fmt::Debug for Subscription {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Subscription")
            .field("dataset", &self.dataset)
            .finish_non_exhaustive()
    }
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
    /// A subscription for `dataset` and the announcer that feeds it.
    pub fn subscribe(dataset: &str) -> (Self, Subscription) {
        let (announce, announced) = watch::channel(Announced::new());
        let subscription = Subscription {
            announced,
            dataset: dataset.to_string(),
            _feed: None,
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

/// The locations a consumer routes notifications to.
#[derive(Debug, Default)]
struct Routes {
    /// The snapshot locations served, by index.
    locations: Vec<NotificationLocation>,
    /// The `runtime.state` locations whose `snapshots/` folder is served.
    state_roots: Vec<NotificationLocation>,
}

impl Routes {
    fn of(feeds: &[Arc<Feed>]) -> Self {
        Self {
            locations: feeds.iter().map(|feed| feed.location.clone()).collect(),
            state_roots: feeds
                .iter()
                .filter_map(|feed| feed.state_root.clone())
                .collect(),
        }
    }

    /// The served locations for log lines: `location 's3://a'` or
    /// `locations 's3://a', 's3://b'`.
    fn served(&self) -> String {
        let quoted: Vec<String> = self
            .locations
            .iter()
            .map(|location| format!("'{location}'"))
            .collect();
        match quoted.as_slice() {
            [one] => format!("location {one}"),
            _ => format!("locations {}", quoted.join(", ")),
        }
    }
}

/// What to do with one SQS message.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Route {
    /// New snapshots were committed at these locations (indexes into
    /// [`Routes::locations`]): announce their metadata, then delete it.
    Commit(BTreeSet<usize>),
    /// Nothing to announce: delete it without a log.
    Ack,
    /// Not an S3 event notification, for this reason: delete it with a warning.
    Invalid(String),
    /// Names this object, outside every served location: log an error and
    /// leave it on the queue.
    Leave { bucket: String, key: String },
}

fn route(routes: &Routes, message: &QueueMessage) -> Route {
    let events = match parse_notification_body(&message.body) {
        Ok(events) => events,
        Err(error) => return Route::Invalid(error.to_string()),
    };
    let mut commits = BTreeSet::new();
    for event in &events {
        // The snapshot files land before `metadata.json` names them, so only the
        // metadata rewrite means a new snapshot can be loaded.
        if let Some(index) = routes.locations.iter().position(|location| {
            event.kind == ObjectEventKind::Created && location.is_metadata(event)
        }) {
            commits.insert(index);
        } else if !routes
            .locations
            .iter()
            .chain(&routes.state_roots)
            .any(|location| location.contains(event))
        {
            return Route::Leave {
                bucket: event.bucket.clone(),
                key: event.key.clone(),
            };
        }
    }
    if commits.is_empty() {
        Route::Ack
    } else {
        Route::Commit(commits)
    }
}

fn retry_backoff() -> RetryBackoff {
    RetryBackoffBuilder::new()
        .method(BackoffMethod::Exponential)
        .max_duration(Some(RETRY_BACKOFF_CAP))
        .build()
}

async fn run_consumer(queue: Queue, feeds: Arc<Feeds>) {
    let sqs = connect(&queue, &feeds).await;
    consume(&sqs, &feeds).await;
}

/// Build the SQS client, retrying while it cannot be built (for example while
/// credentials are not yet available).
async fn connect(queue: &Queue, feeds: &Feeds) -> SqsQueue {
    let mut backoff = retry_backoff();
    let mut outage = SqsOutage::default();
    loop {
        match build_sqs_client(&queue.credentials, &queue.region).await {
            Ok(client) => return SqsQueue::new(client, queue.url.clone()),
            Err(error) => {
                let served = Routes::of(&feeds.live()).served();
                outage.log_failure(&served, SqsCall::Connect, &error, Instant::now());
                tokio::time::sleep(backoff.next_backoff().unwrap_or(RETRY_BACKOFF_CAP)).await;
            }
        }
    }
}

async fn consume(queue: &dyn MessageQueue, feeds: &Feeds) {
    let mut backoff = retry_backoff();
    let mut outage = SqsOutage::default();
    let mut listening = HashSet::new();
    loop {
        // Taken only while handling a batch: a feed held across the receive
        // would keep a location, and the consumer, alive after its last
        // subscription is dropped.
        log_new_locations(&mut listening, &feeds.live());
        let received = queue.receive().await;
        let served = feeds.live();
        match received {
            Ok(messages) => {
                backoff.reset();
                if let Some(lasted) = outage.recover(Instant::now()) {
                    let served = Routes::of(&served).served();
                    tracing::info!("{}", receive_recovered_message(&served, lasted));
                }
                process_batch(queue, &served, messages).await;
            }
            Err(error) => {
                let served = Routes::of(&served).served();
                outage.log_failure(&served, SqsCall::Receive, &error, Instant::now());
                tokio::time::sleep(backoff.next_backoff().unwrap_or(RETRY_BACKOFF_CAP)).await;
            }
        }
    }
}

/// Log that each location in `served` not yet in `listening` is listening, and
/// forget the locations no longer served.
fn log_new_locations(listening: &mut HashSet<NotificationLocation>, served: &[Arc<Feed>]) {
    listening.retain(|location| served.iter().any(|feed| feed.location == *location));
    for feed in served {
        if listening.insert(feed.location.clone()) {
            tracing::info!("{}", listening_message(&feed.location));
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
        served: &str,
        call: SqsCall,
        error: &dyn std::error::Error,
        now: Instant,
    ) {
        match self.fail(now) {
            OutageStep::Started => {
                tracing::warn!("{}", sqs_failure_message(served, call, error, None));
            }
            OutageStep::Escalated(lasted) => {
                tracing::error!("{}", sqs_failure_message(served, call, error, Some(lasted)));
            }
            OutageStep::Continuing => {
                tracing::debug!("{}", sqs_failure_message(served, call, error, None));
            }
        }
    }
}

/// Route a received batch. For each location with a commit, read its metadata
/// once and announce the current snapshot ids, then delete the handled
/// messages. A commit whose metadata cannot be read stays on the queue, so it
/// is delivered again.
async fn process_batch(queue: &dyn MessageQueue, feeds: &[Arc<Feed>], messages: Vec<QueueMessage>) {
    let routes = Routes::of(feeds);
    let mut acks = Vec::with_capacity(messages.len());
    let mut commits = Vec::new();
    for message in messages {
        match route(&routes, &message) {
            Route::Commit(locations) => commits.push((message.receipt_handle, locations)),
            Route::Ack => acks.push(message.receipt_handle),
            Route::Invalid(error) => {
                tracing::warn!("{}", invalid_notification_warning(&routes.served(), &error));
                acks.push(message.receipt_handle);
            }
            Route::Leave { bucket, key } => {
                tracing::error!(
                    "{}",
                    outside_location_error(&routes.served(), &bucket, &key)
                );
            }
        }
    }

    let committed: BTreeSet<usize> = commits
        .iter()
        .flat_map(|(_, locations)| locations.iter().copied())
        .collect();
    let mut announced = HashSet::new();
    for index in committed {
        let feed = &feeds[index];
        match feed.manager.current_snapshot_ids().await {
            Ok(current) => {
                feed.announce.send_if_modified(|announced| {
                    let changed = *announced != current;
                    *announced = current;
                    changed
                });
                announced.insert(index);
            }
            Err(error) => {
                tracing::warn!("{}", metadata_read_warning(&feed.location, &error));
            }
        }
    }
    acks.extend(
        commits
            .into_iter()
            .filter(|(_, locations)| locations.iter().all(|index| announced.contains(index)))
            .map(|(receipt_handle, _)| receipt_handle),
    );

    for receipt_handle in acks {
        if let Err(error) = queue.delete(&receipt_handle).await {
            tracing::warn!(
                "Snapshot {} handled an S3 event notification but could not delete it from the SQS queue in `s3_queue_url`, so it will be delivered again. Cause: {error}. Grant `sqs:DeleteMessage` on the queue. See: {SNAPSHOTS_DOCS}",
                routes.served()
            );
        }
    }
}

/// The warning for an `s3_queue_url` in `setting` that no dataset with
/// `refresh_mode: snapshot`, the only mode that reads the queue, uses.
#[must_use]
pub fn unread_queue_warning(setting: &str) -> String {
    format!(
        "{setting} is set, but no dataset with `refresh_mode: snapshot` uses it, so the SQS queue is not read. Set `refresh_mode: snapshot` on the datasets that load snapshots, or remove `s3_queue_url`. See: {SNAPSHOTS_DOCS}"
    )
}

fn shared_queue_credentials_warning(
    location: &NotificationLocation,
    opened_for: &NotificationLocation,
) -> String {
    format!(
        "Snapshot location '{location}' names the same SQS queue in `s3_queue_url` as snapshot location '{opened_for}', but with other AWS credentials. A queue has one consumer, so it is read with the credentials of '{opened_for}'. Use the same credentials for every location on the queue, or a queue per location. See: {SNAPSHOTS_DOCS}"
    )
}

fn listening_message(location: &NotificationLocation) -> String {
    format!(
        "Snapshot location '{location}' is listening for S3 event notifications on the SQS queue in `s3_queue_url`, so datasets with `refresh_mode: snapshot` reload as soon as a new snapshot is published."
    )
}

/// The line for a failing SQS call: the start of an outage (`lasted` is
/// `None`), or one that has lasted past [`SQS_OUTAGE_ERROR_AFTER`].
fn sqs_failure_message(
    served: &str,
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
        "Failed to {action} for snapshot {served}{lasted}, so datasets with `refresh_mode: snapshot` fall back to checking the location every `refresh_check_interval` until it recovers. Cause: {error}. {fix} See: {SNAPSHOTS_DOCS}"
    )
}

fn receive_recovered_message(served: &str, lasted: Duration) -> String {
    format!(
        "Snapshot {served} receives S3 event notifications from the SQS queue in `s3_queue_url` again after {}, so datasets with `refresh_mode: snapshot` reload as soon as a new snapshot is published.",
        format_duration(lasted)
    )
}

fn metadata_read_warning(location: &NotificationLocation, error: &dyn std::error::Error) -> String {
    format!(
        "Failed to read the snapshot metadata of snapshot location '{location}' after an S3 event notification, so the notification was left on the SQS queue and datasets reload when it is delivered again or on `refresh_check_interval`. Cause: {error}. See: {SNAPSHOTS_DOCS}"
    )
}

fn invalid_notification_warning(served: &str, error: &str) -> String {
    format!(
        "Snapshot {served} dropped an SQS message that is not an S3 event notification, so it was deleted without reloading any dataset. Cause: {error}. Point only the location's S3 event notifications at the queue in `s3_queue_url`. See: {SNAPSHOTS_DOCS}"
    )
}

/// An S3 key is percent-decoded before it is logged, so a `%0A` in the
/// notification is a real newline. Keep the log line one line.
fn log_single_line(value: &str) -> String {
    use std::fmt::Write;
    let mut single = String::with_capacity(value.len());
    for character in value.chars() {
        match character {
            '\n' => single.push_str("\\n"),
            '\r' => single.push_str("\\r"),
            '\t' => single.push_str("\\t"),
            control if control.is_control() => {
                let _ = write!(single, "\\u{{{:x}}}", u32::from(control));
            }
            other => single.push(other),
        }
    }
    single
}

fn outside_location_error(served: &str, bucket: &str, key: &str) -> String {
    let bucket = log_single_line(bucket);
    let key = log_single_line(key);
    format!(
        "Snapshot {served} received an S3 event notification for 's3://{bucket}/{key}', which is outside the location, so the SQS message was left on the queue (not deleted) and will be delivered again after each visibility timeout until it expires. The queue in `s3_queue_url` must receive only this location's notifications: filter the bucket notification by the location's prefix, or fan out through SNS to a queue per consumer. See: {SNAPSHOTS_DOCS}"
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

    fn at(location: &str) -> NotificationLocation {
        NotificationLocation::from_url(&Url::parse(location).expect("valid location"))
            .expect("an s3 location")
    }

    fn location() -> NotificationLocation {
        at("s3://my-bucket/spice/snapshots/")
    }

    fn routes() -> Routes {
        Routes {
            locations: vec![location()],
            state_roots: Vec::new(),
        }
    }

    /// The routes of `s3://my-bucket/spice/snapshots/` when it is the
    /// `snapshots/` folder of the `runtime.state` location `s3://my-bucket/spice/`.
    fn runtime_state_routes() -> Routes {
        Routes {
            locations: vec![location()],
            state_roots: vec![at("s3://my-bucket/spice/")],
        }
    }

    fn commit(locations: &[usize]) -> Route {
        Route::Commit(locations.iter().copied().collect())
    }

    fn queue() -> Queue {
        Queue {
            url: QUEUE_URL.to_string(),
            region: "us-east-1".to_string(),
            credentials: SqsCredentials::DefaultChain,
        }
    }

    fn config_at(location: NotificationLocation) -> NotificationConfig {
        NotificationConfig {
            queue: queue(),
            location,
            state_root: None,
        }
    }

    fn config() -> NotificationConfig {
        config_at(location())
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

    fn created(bucket: &str, key: &str) -> S3ObjectEvent {
        parse_notification_body(&s3_body("ObjectCreated:Put", bucket, key))
            .expect("an S3 notification")
            .remove(0)
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

    /// The feed of `location`, read through `manager`, on a consumer that does
    /// nothing.
    fn feed(
        location: NotificationLocation,
        state_root: Option<NotificationLocation>,
        manager: Arc<SnapshotManager>,
    ) -> Arc<Feed> {
        let (announce, announced) = watch::channel(Announced::new());
        let consumer = Arc::new(Consumer {
            credentials: SqsCredentials::DefaultChain,
            opened_for: location.clone(),
            feeds: Arc::default(),
            task: tokio::spawn(futures::future::pending()),
        });
        Arc::new(Feed {
            location,
            state_root,
            manager,
            announce,
            announced,
            _consumer: consumer,
        })
    }

    /// A queue that records deletes.
    #[derive(Default)]
    struct RecordingQueue {
        deleted: Mutex<Vec<String>>,
    }

    impl RecordingQueue {
        fn deleted(&self) -> Vec<String> {
            let mut deleted = self.deleted.lock().clone();
            deleted.sort();
            deleted
        }
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

    fn leave(bucket: &str, key: &str) -> Route {
        Route::Leave {
            bucket: bucket.to_string(),
            key: key.to_string(),
        }
    }

    #[test]
    fn a_metadata_rewrite_is_a_commit() {
        assert_eq!(
            route(&routes(), &message(metadata_created(), "r")),
            commit(&[0])
        );
    }

    #[test]
    fn a_snapshot_file_or_a_removal_is_acknowledged_without_a_commit() {
        let file = s3_body(
            "ObjectCreated:CompleteMultipartUpload",
            "my-bucket",
            "spice/snapshots/month=2026-09/day=2026-09-23/dataset=orders/orders_20260923T000000Z.duckdb",
        );
        assert_eq!(route(&routes(), &message(file, "r")), Route::Ack);

        let removed = s3_body(
            "ObjectRemoved:Delete",
            "my-bucket",
            "spice/snapshots/metadata.json",
        );
        assert_eq!(route(&routes(), &message(removed, "r")), Route::Ack);

        // Renewing a snapshot writer lease is not a new snapshot either.
        let lease = s3_body(
            "ObjectCreated:Put",
            "my-bucket",
            "spice/snapshots/leases/orders.json",
        );
        assert_eq!(route(&routes(), &message(lease, "r")), Route::Ack);
    }

    #[test]
    fn a_metadata_json_in_a_nested_prefix_is_not_the_locations_metadata() {
        let nested = s3_body(
            "ObjectCreated:Put",
            "my-bucket",
            "spice/snapshots/other/metadata.json",
        );
        assert_eq!(route(&routes(), &message(nested, "r")), Route::Ack);
    }

    #[test]
    fn a_notification_outside_the_location_is_left_on_the_queue() {
        let other_prefix = s3_body("ObjectCreated:Put", "my-bucket", "events/metadata.json");
        assert_eq!(
            route(&routes(), &message(other_prefix, "r")),
            leave("my-bucket", "events/metadata.json")
        );

        let other_bucket = s3_body(
            "ObjectCreated:Put",
            "other-bucket",
            "spice/snapshots/metadata.json",
        );
        assert_eq!(
            route(&routes(), &message(other_bucket, "r")),
            leave("other-bucket", "spice/snapshots/metadata.json")
        );

        // A sibling prefix that only starts with the location's name.
        let sibling = s3_body(
            "ObjectCreated:Put",
            "my-bucket",
            "spice/snapshots-old/metadata.json",
        );
        assert_eq!(
            route(&routes(), &message(sibling, "r")),
            leave("my-bucket", "spice/snapshots-old/metadata.json")
        );

        // Without `runtime.state`, a folder next to the location is not Spice's state.
        let heartbeat = s3_body(
            "ObjectCreated:Put",
            "my-bucket",
            "spice/cluster/heartbeats/scheduler-1.json",
        );
        assert_eq!(
            route(&routes(), &message(heartbeat, "r")),
            leave("my-bucket", "spice/cluster/heartbeats/scheduler-1.json")
        );
    }

    /// A queue subscribed to the whole `runtime.state` prefix also receives the
    /// scheduler, rate-control and results-cache files. They are Spice's own
    /// state: acknowledged, which deletes them without a log line.
    #[test]
    fn another_runtime_state_folder_is_deleted_without_a_log() {
        for key in [
            "spice/cluster/heartbeats/scheduler-1.json",
            "spice/cluster.json",
            "spice/rate-control/origin.json",
            "spice/results-cache/warmup.json",
            "spice/snapshots-old/metadata.json",
        ] {
            let body = s3_body("ObjectCreated:Put", "my-bucket", key);
            assert_eq!(
                route(&runtime_state_routes(), &message(body, "r")),
                Route::Ack,
                "{key}"
            );
        }
        assert_eq!(
            route(&runtime_state_routes(), &message(metadata_created(), "r")),
            commit(&[0])
        );
    }

    #[test]
    fn a_notification_outside_runtime_state_is_logged_and_left_on_the_queue() {
        for (bucket, key) in [
            ("my-bucket", "events/a.parquet"),
            ("my-bucket", "spice-other/cluster.json"),
            ("other-bucket", "spice/cluster.json"),
        ] {
            let body = s3_body("ObjectCreated:Put", bucket, key);
            assert_eq!(
                route(&runtime_state_routes(), &message(body, "r")),
                leave(bucket, key)
            );
        }
    }

    #[test]
    fn a_body_that_is_not_a_notification_is_acknowledged() {
        assert!(matches!(
            route(&routes(), &message("not json".to_string(), "r")),
            Route::Invalid(_)
        ));
        let test_event = serde_json::json!({ "Event": "s3:TestEvent" }).to_string();
        assert_eq!(route(&routes(), &message(test_event, "r")), Route::Ack);
    }

    #[test]
    fn sns_and_eventbridge_notifications_are_commits() {
        let sns = serde_json::json!({ "Type": "Notification", "Message": metadata_created() })
            .to_string();
        assert_eq!(route(&routes(), &message(sns, "r")), commit(&[0]));

        let eventbridge = serde_json::json!({
            "source": "aws.s3",
            "detail-type": "Object Created",
            "detail": {
                "bucket": { "name": "my-bucket" },
                "object": { "key": "spice/snapshots/metadata.json" }
            }
        })
        .to_string();
        assert_eq!(route(&routes(), &message(eventbridge, "r")), commit(&[0]));
    }

    /// Two snapshot locations that name one queue share its consumer; each
    /// commit is routed to its own location.
    #[test]
    fn each_location_on_a_shared_queue_gets_its_own_commits() {
        let shared = Routes {
            locations: vec![location(), at("s3://archive-bucket/snapshots/")],
            state_roots: vec![at("s3://my-bucket/spice/")],
        };
        let outside = s3_body("ObjectCreated:Put", "archive-bucket", "events/a.parquet");
        assert_eq!(
            route(&shared, &message(outside, "r")),
            leave("archive-bucket", "events/a.parquet")
        );
        let archive = s3_body(
            "ObjectCreated:Put",
            "archive-bucket",
            "snapshots/metadata.json",
        );
        assert_eq!(route(&shared, &message(archive, "r")), commit(&[1]));
        assert_eq!(
            route(&shared, &message(metadata_created(), "r")),
            commit(&[0])
        );
        let archive_file = s3_body(
            "ObjectCreated:Put",
            "archive-bucket",
            "snapshots/dataset=events/events.duckdb",
        );
        assert_eq!(route(&shared, &message(archive_file, "r")), Route::Ack);
    }

    #[test]
    fn the_locations_keys_match_the_snapshot_writers_paths() {
        let location = location();
        assert_eq!(location.key_prefix, "spice/snapshots/");
        assert!(location.is_metadata(&created("my-bucket", "spice/snapshots/metadata.json")));
        assert!(!location.is_metadata(&created("other-bucket", "spice/snapshots/metadata.json")));
        assert_eq!(location.to_string(), "s3://my-bucket/spice/snapshots");

        assert_eq!(at("s3://my-bucket/spice/snapshots"), location);

        let root = at("s3://my-bucket");
        assert_eq!(root.key_prefix, "");
        assert!(root.is_metadata(&created("my-bucket", "metadata.json")));
        assert_eq!(root.to_string(), "s3://my-bucket");
    }

    #[tokio::test]
    async fn a_commit_announces_every_datasets_current_snapshot_and_deletes_the_batch() {
        let feed = feed(
            location(),
            None,
            manager_with_current(&[("orders", 7), ("customers", 3)]).await,
        );
        let mut announced = feed.announced.clone();
        let queue = RecordingQueue::default();

        process_batch(
            &queue,
            &[Arc::clone(&feed)],
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
        assert_eq!(
            queue.deleted(),
            vec!["metadata-1", "metadata-2", "poison"],
            "the outside-location message must stay on the queue"
        );
    }

    #[tokio::test]
    async fn a_batch_deletes_other_runtime_state_folders_and_leaves_outside_objects() {
        let feed = feed(
            location(),
            Some(at("s3://my-bucket/spice/")),
            manager_with_current(&[("orders", 7)]).await,
        );
        let queue = RecordingQueue::default();

        process_batch(
            &queue,
            &[feed],
            vec![
                message(
                    s3_body(
                        "ObjectCreated:Put",
                        "my-bucket",
                        "spice/cluster/heartbeats/scheduler-1.json",
                    ),
                    "heartbeat",
                ),
                message(
                    s3_body(
                        "ObjectCreated:Put",
                        "my-bucket",
                        "spice/rate-control/a.json",
                    ),
                    "rate-control",
                ),
                message(metadata_created(), "metadata"),
                message(
                    s3_body("ObjectCreated:Put", "my-bucket", "events/a.parquet"),
                    "outside",
                ),
            ],
        )
        .await;

        assert_eq!(
            queue.deleted(),
            vec!["heartbeat", "metadata", "rate-control"],
            "only the message outside `runtime.state` stays on the queue"
        );
    }

    #[tokio::test]
    async fn a_commit_is_announced_only_to_its_own_location() {
        let state = feed(
            location(),
            None,
            manager_with_current(&[("orders", 7)]).await,
        );
        let archive = feed(
            at("s3://archive-bucket/snapshots/"),
            None,
            manager_with_current(&[("orders", 70)]).await,
        );
        let state_announced = state.announced.clone();
        let mut archive_announced = archive.announced.clone();
        let queue = RecordingQueue::default();

        process_batch(
            &queue,
            &[Arc::clone(&state), Arc::clone(&archive)],
            vec![message(
                s3_body(
                    "ObjectCreated:Put",
                    "archive-bucket",
                    "snapshots/metadata.json",
                ),
                "archive",
            )],
        )
        .await;

        assert!(
            !state_announced.has_changed().expect("sender alive"),
            "the archive's `orders` is not the state location's `orders`"
        );
        assert_eq!(
            *archive_announced.borrow_and_update(),
            Announced::from([("orders".to_string(), 70)])
        );
        assert_eq!(queue.deleted(), vec!["archive"]);
    }

    #[tokio::test]
    async fn an_unchanged_metadata_announces_nothing_new() {
        let feed = feed(
            location(),
            None,
            manager_with_current(&[("orders", 7)]).await,
        );
        let mut announced = feed.announced.clone();
        let queue = RecordingQueue::default();
        let batch = || vec![message(metadata_created(), "metadata")];

        process_batch(&queue, &[Arc::clone(&feed)], batch()).await;
        assert!(announced.has_changed().expect("sender alive"));
        announced.borrow_and_update();

        process_batch(&queue, &[Arc::clone(&feed)], batch()).await;
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
        let feed = feed(location(), None, Arc::new(manager));
        let announced = feed.announced.clone();
        let queue = RecordingQueue::default();

        process_batch(
            &queue,
            &[Arc::clone(&feed)],
            vec![message(metadata_created(), "metadata")],
        )
        .await;

        assert!(!announced.has_changed().expect("sender alive"));
        assert!(queue.deleted().is_empty());
    }

    #[tokio::test]
    async fn subscriptions_share_a_consumer_that_stops_with_the_last_one() {
        let notifications = SnapshotNotifications::default();
        let orders = Arc::new(build_manager_for_api_tests(Arc::new(InMemory::new())));
        let spawns = AtomicUsize::new(0);
        let subscribe = |config: &NotificationConfig| {
            notifications.subscribe_with(config, &orders, |_feeds| {
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

        let first = subscribe(&config());
        let second = subscribe(&config());
        let other_location = subscribe(&config_at(at("s3://archive-bucket/snapshots/")));
        let other_credentials = subscribe(&NotificationConfig {
            queue: Queue {
                credentials: SqsCredentials::Static {
                    access_key: "other-key".to_string(),
                    secret_key: "other-secret".to_string(),
                    session_token: None,
                },
                ..queue()
            },
            ..config_at(at("s3://other-bucket/snapshots/"))
        });
        assert_eq!(
            spawns.load(Ordering::SeqCst),
            1,
            "subscriptions to one queue URL share its consumer, whatever their location or credentials"
        );
        assert_eq!(running(), 1);

        drop(first);
        drop(second);
        drop(other_credentials);
        assert_eq!(running(), 1, "still used by `other_location`");
        drop(other_location);
        assert_eq!(
            running(),
            0,
            "the consumer stops with its last subscription"
        );

        let _again = subscribe(&config());
        assert_eq!(
            spawns.load(Ordering::SeqCst),
            2,
            "a later subscription starts a new consumer"
        );
    }

    #[tokio::test]
    async fn a_subscription_sees_only_its_own_datasets_snapshots() {
        let notifications = SnapshotNotifications::default();
        let orders = Arc::new(build_manager_for_api_tests(Arc::new(InMemory::new())));
        let mut feeds = None;
        let mut subscription = notifications.subscribe_with(&config(), &orders, |served| {
            feeds = Some(served);
            tokio::spawn(futures::future::pending())
        });
        let feed = feeds.expect("the consumer was started").live().remove(0);

        feed.announce
            .send_replace(Announced::from([("customers".to_string(), 4)]));
        feed.announce.send_replace(Announced::from([
            ("customers".to_string(), 4),
            (orders.dataset_name().to_string(), 9),
        ]));
        let id = tokio::time::timeout(Duration::from_secs(5), subscription.next_snapshot())
            .await
            .expect("the dataset's snapshot is announced");
        assert_eq!(id, Some(9));
    }

    #[tokio::test]
    async fn bootstrap_handoff_preserves_announcements_received_during_download() {
        let (announcer, mut bootstrap) = TestAnnouncer::subscribe("orders");
        announcer.announce("orders", 7);
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(5), bootstrap.next_snapshot())
                .await
                .expect("initial announcement wakes bootstrap"),
            Some(7)
        );

        // Dataset initialization clones its bootstrap result for load retries.
        // The refresh task must see changes received after bootstrap woke up.
        announcer.announce("orders", 8);
        announcer.announce("orders", 9);
        let mut refresh = bootstrap.clone();
        drop(bootstrap);
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(5), refresh.next_snapshot())
                .await
                .expect("pending announcement survives handoff"),
            Some(9)
        );
    }

    #[tokio::test]
    async fn a_late_dataset_subscriber_observes_the_current_announcement() {
        let notifications = SnapshotNotifications::default();
        let orders = Arc::new(build_manager_for_api_tests(Arc::new(InMemory::new())));
        let mut feeds = None;
        let first = notifications.subscribe_with(&config(), &orders, |served| {
            feeds = Some(served);
            tokio::spawn(futures::future::pending())
        });
        let feed = feeds.expect("consumer started").live().remove(0);
        feed.announce
            .send_replace(Announced::from([(orders.dataset_name().to_string(), 9)]));
        drop(feed);
        let mut second = notifications.subscribe_with(&config(), &orders, |_| {
            panic!("a late subscriber must reuse the queue consumer")
        });
        drop(first);
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(5), second.next_snapshot())
                .await
                .expect("late subscription observes the current snapshot"),
            Some(9)
        );
    }

    #[test]
    fn a_queue_no_dataset_reads_names_its_setting() {
        let warning = unread_queue_warning("`runtime.state.params.s3_queue_url`");
        assert!(
            warning.starts_with("`runtime.state.params.s3_queue_url` is set, but no dataset with `refresh_mode: snapshot` uses it, so the SQS queue is not read."),
            "{warning}"
        );
        assert!(warning.contains(SNAPSHOTS_DOCS));
    }

    #[test]
    fn log_lines_name_the_location_the_param_and_the_impact() {
        let location = location();
        let served = routes().served();
        assert_eq!(served, "location 's3://my-bucket/spice/snapshots'");
        let listening = listening_message(&location);
        assert!(listening.contains("'s3://my-bucket/spice/snapshots'"));
        assert!(listening.contains("`s3_queue_url`"));
        assert!(listening.contains("`refresh_mode: snapshot`"));

        let error = std::io::Error::other("AccessDenied");
        let receive = sqs_failure_message(&served, SqsCall::Receive, &error, None);
        assert!(receive.contains("'s3://my-bucket/spice/snapshots'"));
        assert!(
            receive.contains("fall back to checking the location every `refresh_check_interval`")
        );
        assert!(receive.contains("Cause: AccessDenied"));
        assert!(receive.contains("`sqs:ReceiveMessage`"));
        assert!(receive.contains(SNAPSHOTS_DOCS));

        let lasting = sqs_failure_message(
            &served,
            SqsCall::Receive,
            &error,
            Some(Duration::from_secs(301)),
        );
        assert!(lasting.contains("'s3://my-bucket/spice/snapshots' for 5m,"));

        let connect = sqs_failure_message(&served, SqsCall::Connect, &error, None);
        assert!(connect.contains("Failed to connect to the SQS queue in `s3_queue_url`"));
        assert!(connect.contains("AWS credentials"));

        let recovered = receive_recovered_message(&served, Duration::from_secs(42));
        assert!(recovered.contains("again after 42s"));

        let outside = outside_location_error(&served, "my-bucket", "events/a.parquet");
        assert!(outside.contains("'s3://my-bucket/events/a.parquet'"));
        assert!(outside.contains("left on the queue (not deleted)"));
        assert!(outside.contains(SNAPSHOTS_DOCS));

        let other_credentials =
            shared_queue_credentials_warning(&at("s3://archive-bucket/snapshots/"), &location);
        assert!(
            other_credentials.starts_with("Snapshot location 's3://archive-bucket/snapshots' names the same SQS queue in `s3_queue_url` as snapshot location 's3://my-bucket/spice/snapshots', but with other AWS credentials."),
            "{other_credentials}"
        );

        let shared = Routes {
            locations: vec![location.clone(), at("s3://archive-bucket/snapshots/")],
            state_roots: Vec::new(),
        };
        assert_eq!(
            shared.served(),
            "locations 's3://my-bucket/spice/snapshots', 's3://archive-bucket/snapshots'"
        );

        let encoded = "events%2Fok%0AFORGED%3A+entry.parquet";
        let parsed = parse_notification_body(&s3_body("ObjectCreated:Put", "my-bucket", encoded))
            .expect("encoded key parses");
        let injected_key = &parsed[0].key;
        assert!(
            injected_key.contains('\n'),
            "the parser decodes %0A before the key is logged"
        );
        let injected = outside_location_error(&served, &parsed[0].bucket, injected_key);
        assert!(
            injected.contains("events/ok\\nFORGED: entry.parquet"),
            "{injected}"
        );
        assert!(!injected.contains('\n'), "a decoded newline stays escaped");

        for line in [
            listening,
            receive,
            lasting,
            connect,
            recovered,
            outside,
            metadata_read_warning(&location, &error),
            invalid_notification_warning(&served, "expected value at line 1 column 1"),
            other_credentials,
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

    async fn resolve_in(
        snapshots: &Snapshots,
        runtime_state_location: Option<&str>,
    ) -> Result<Option<NotificationConfig>> {
        NotificationConfig::resolve(
            snapshots,
            runtime_state_location,
            Arc::new(RwLock::new(Secrets::default())),
        )
        .await
    }

    async fn resolve(snapshots: &Snapshots) -> Result<Option<NotificationConfig>> {
        resolve_in(snapshots, None).await
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
            resolved.queue.region, "us-east-1",
            "the region comes from the queue URL"
        );
        assert_eq!(resolved.location, location());
        assert_eq!(resolved.queue.credentials, SqsCredentials::DefaultChain);
        assert_eq!(resolved.state_root, None);
    }

    /// Only the `snapshots/` folder of `runtime.state.location` routes the
    /// location's other folders as Spice's own state.
    #[tokio::test]
    async fn resolve_marks_the_runtime_state_snapshots_folder() {
        let queue = [("s3_queue_url", QUEUE_URL)];
        for state_location in ["s3://my-bucket/spice/", "s3://my-bucket/spice"] {
            let resolved = resolve_in(
                &snapshots("s3://my-bucket/spice/snapshots", &queue),
                Some(state_location),
            )
            .await
            .expect("valid config")
            .expect("a queue is configured");
            assert_eq!(
                resolved.state_root,
                Some(at("s3://my-bucket/spice/")),
                "{state_location}"
            );
        }

        for (location, state_location) in [
            (
                "s3://archive-bucket/snapshots/",
                Some("s3://my-bucket/spice/"),
            ),
            ("s3://my-bucket/spice/snaps/", Some("s3://my-bucket/spice/")),
            (
                "s3://my-bucket/spice/snapshots/",
                Some("file:///var/spice/"),
            ),
            ("s3://my-bucket/spice/snapshots/", None),
        ] {
            let resolved = resolve_in(&snapshots(location, &queue), state_location)
                .await
                .expect("valid config")
                .expect("a queue is configured");
            assert_eq!(
                resolved.state_root, None,
                "{location} in {state_location:?}"
            );
        }
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
            resolved.queue.credentials,
            SqsCredentials::Static {
                access_key: "AKIAEXAMPLE".to_string(),
                secret_key: "secret".to_string(),
                session_token: None,
            }
        );
    }

    /// An invalid parameter is reported as itself. Falling back to the default parameters
    /// would also drop `s3_queue_url` and report it as empty.
    #[tokio::test]
    async fn resolve_reports_an_invalid_parameter() {
        let invalid = resolve(&snapshots(
            "s3://my-bucket/snapshots/",
            &[("s3_queue_url", QUEUE_URL), ("s3_auth", "public")],
        ))
        .await
        .err()
        .expect("`s3_auth: public` is not a valid snapshot auth mode");
        assert!(matches!(invalid, Error::InvalidParams { .. }), "{invalid}");
        let message = invalid.to_string();
        assert!(
            message.contains("s3_auth"),
            "names the parameter: {message}"
        );
        assert!(
            !message.contains(QUEUE_URL),
            "keeps the queue URL out: {message}"
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
