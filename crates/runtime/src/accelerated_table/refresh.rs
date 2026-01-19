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

use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, Weak};

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use super::refresh_task_runner::RefreshTaskRunner;
use super::synchronized_table::SynchronizedTable;
use super::{SnapshotCreateTrigger, SnapshotCreationConfig, metrics};
use crate::accelerated_table::refresh_task::RefreshTask;
use crate::accelerated_table::snapshots::{
    SnapshotCallback, create_checkpoint_and_snapshot, create_periodic_snapshot_callback,
    spawn_snapshot_interval_task,
};
use crate::component::dataset::TimeFormat;
use crate::component::dataset::acceleration::{RefreshMode, RefreshOnStartup};
use crate::dataaccelerator::BootstrapStatus;
use crate::federated_table::FederatedTable;
use crate::status;
use arrow::datatypes::Schema;
use cache::Caching;
use data_components::cdc::ChangesStream;
use datafusion::common::TableReference;
use datafusion::datasource::TableProvider;
use futures::future::BoxFuture;
use opentelemetry::KeyValue;
use rand::Rng;
use runtime_acceleration::dataset_checkpoint::DatasetCheckpointer;
use serde::{Deserialize, Serialize};
use snafu::prelude::*;
use spicepod::metric::Metrics;
use tokio::runtime::Handle;
use tokio::select;
use tokio::sync::mpsc::Receiver;
use tokio::sync::{Mutex, Notify};
use tokio::sync::{RwLock, Semaphore};
use tokio::time::sleep;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "time_column '{time_column}' in dataset {table_name} has data type '{actual_time_format}', but time_format is configured as '{expected_time_format}'"
    ))]
    TimeFormatMismatch {
        table_name: String,
        time_column: String,
        expected_time_format: String,
        actual_time_format: String,
    },

    #[snafu(display("time_column '{time_column}' was not found in dataset {table_name}"))]
    NoTimeColumnFound {
        table_name: String,
        time_column: String,
    },
}

#[derive(Clone, Debug)]
pub struct Refresh {
    pub(crate) time_column: Option<String>,
    pub(crate) time_format: Option<TimeFormat>,
    pub(crate) time_partition_column: Option<String>,
    pub(crate) time_partition_format: Option<TimeFormat>,
    pub(crate) check_interval: Option<Duration>,
    pub(crate) max_jitter: Option<Duration>,
    pub(crate) sql: Option<String>,
    pub(crate) mode: RefreshMode,
    pub(crate) period: Option<Duration>,
    pub(crate) append_overlap: Option<Duration>,
    pub(crate) retry_enabled: bool,
    pub(crate) retry_max_attempts: Option<usize>,
    /// TTL for cache entries. Data older than this is considered stale.
    pub(crate) caching_ttl: Option<Duration>,
}

/// [`RefreshOverrides`] specifies the configurable options for a individual run of a refresh task.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct RefreshOverrides {
    /// The SQL statement used for this refresh. Defaults to the `refresh_sql` specified in the spicepod, if any.
    #[serde(default, rename = "refresh_sql")]
    pub sql: Option<String>,

    /// The refresh mode to use for this refresh. Defaults to the `refresh_mode` specified in the spicepod, or `full`.
    #[serde(default, rename = "refresh_mode")]
    pub mode: Option<RefreshMode>,

    /// The maximum amount of jitter to add to the refresh. Defaults to the `refresh_jitter_max` specified in the spicepod, or 10% of the `refresh_check_interval`.
    #[serde(
        default,
        rename = "refresh_jitter_max",
        deserialize_with = "parse_max_jitter"
    )]
    #[cfg_attr(feature = "openapi", schema(value_type = Option<String>, example = "10s"))]
    pub max_jitter: Option<Duration>,
}

fn parse_max_jitter<'de, D>(deserializer: D) -> Result<Option<Duration>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let opt: Option<String> = Option::deserialize(deserializer)?;
    match opt {
        Some(s) => fundu::parse_duration(&s)
            .map(Some)
            .map_err(serde::de::Error::custom),
        None => Ok(None),
    }
}

pub(crate) enum NextRefresh {
    WaitFor(Duration),
    Disabled,
}

impl Refresh {
    #[must_use]
    pub fn new(mode: RefreshMode) -> Self {
        Self {
            mode,
            ..Default::default()
        }
    }

    #[must_use]
    pub fn time_column(mut self, time_column: String) -> Self {
        self.time_column = Some(time_column);
        self
    }

    #[must_use]
    pub fn time_format(mut self, time_format: TimeFormat) -> Self {
        self.time_format = Some(time_format);
        self
    }

    #[must_use]
    pub fn time_partition_column(mut self, time_partition_column: String) -> Self {
        self.time_partition_column = Some(time_partition_column);
        self
    }

    #[must_use]
    pub fn time_partition_format(mut self, time_partition_format: TimeFormat) -> Self {
        self.time_partition_format = Some(time_partition_format);
        self
    }

    #[must_use]
    pub fn check_interval(mut self, check_interval: Duration) -> Self {
        self.check_interval = Some(check_interval);
        self
    }

    #[must_use]
    pub fn max_jitter(mut self, max_jitter: Duration) -> Self {
        self.max_jitter = Some(max_jitter);
        self
    }

    #[must_use]
    pub fn sql(mut self, sql: String) -> Self {
        self.sql = Some(sql);
        self
    }

    #[must_use]
    pub fn period(mut self, period: Duration) -> Self {
        self.period = Some(period);
        self
    }

    #[must_use]
    pub fn append_overlap(mut self, append_overlap: Duration) -> Self {
        self.append_overlap = Some(append_overlap);
        self
    }

    #[must_use]
    pub fn caching_ttl(mut self, caching_ttl: Duration) -> Self {
        self.caching_ttl = Some(caching_ttl);
        self
    }

    #[must_use]
    pub fn with_retry(mut self, enabled: bool, max_attempts: Option<usize>) -> Self {
        self.retry_enabled = enabled;
        self.retry_max_attempts = max_attempts;
        self
    }

    #[must_use]
    pub fn with_overrides(mut self, overrides: &RefreshOverrides) -> Self {
        if let Some(sql) = &overrides.sql {
            self.sql = Some(sql.clone());
        }
        if let Some(mode) = overrides.mode {
            self.mode = mode;
        }
        if let Some(max_jitter) = overrides.max_jitter {
            self.max_jitter = Some(max_jitter);
        }
        self
    }

    pub(crate) fn validate_time_format(
        &self,
        dataset_name: String,
        schema: &Arc<Schema>,
    ) -> Result<(), Error> {
        let Some(time_column) = self.time_column.clone() else {
            return Ok(());
        };

        let Some((_, field)) = schema.column_with_name(&time_column) else {
            return Err(Error::NoTimeColumnFound {
                table_name: dataset_name,
                time_column,
            });
        };

        let time_format = self.time_format.unwrap_or(TimeFormat::Timestamp);
        let data_type = field.data_type().clone();

        validate_time_partition_format(&data_type, &dataset_name, &time_column, time_format)?;

        if let Some(time_partition_column) = self.time_partition_column.clone() {
            let Some((_, field)) = schema.column_with_name(&time_partition_column) else {
                return Err(Error::NoTimeColumnFound {
                    table_name: dataset_name,
                    time_column: time_partition_column,
                });
            };

            let time_partition_format = self.time_partition_format.unwrap_or(TimeFormat::Timestamp);
            let partition_data_type = field.data_type().clone();
            validate_time_partition_format(
                &partition_data_type,
                &dataset_name,
                &time_partition_column,
                time_partition_format,
            )?;
        }

        Ok(())
    }

    /// Determine the next refresh when Spice starts based on the refresh mode and the last checkpoint.
    pub(crate) async fn startup_next_refresh(
        &self,
        refresh_on_startup: RefreshOnStartup,
        last_checkpoint: Option<Arc<dyn DatasetCheckpointer>>,
    ) -> NextRefresh {
        tracing::debug!(
            "startup_next_refresh called with mode: {:?}, check_interval: {:?}",
            self.mode,
            self.check_interval
        );
        let previous_checkpoint = match self.mode {
            RefreshMode::Full => {
                // If there is no checkpoint, we need to start a refresh.
                let Some(last_checkpoint) = last_checkpoint else {
                    return NextRefresh::WaitFor(Duration::ZERO);
                };
                last_checkpoint.last_checkpoint_time().await.ok().flatten()
            }
            // Append and Changes modes are always refreshed since they stream changes from the source table.
            RefreshMode::Append | RefreshMode::Changes => {
                return NextRefresh::WaitFor(Duration::ZERO);
            }
            // Caching mode handles refreshes in two ways:
            // 1. On-demand through cache misses (primary)
            // 2. Periodic background refresh of stale data (if refresh_check_interval is set)
            RefreshMode::Caching => {
                // If refresh_check_interval is set, enable periodic refresh for stale data
                if let Some(check_interval) = self.check_interval {
                    tracing::info!(
                        "Caching mode with refresh_check_interval={:?} - enabling periodic stale data refresh",
                        check_interval
                    );
                    // Start the periodic timer - the first refresh will happen after check_interval
                    return NextRefresh::WaitFor(check_interval);
                }
                tracing::debug!(
                    "Caching mode without refresh_check_interval - on-demand refresh only"
                );
                return NextRefresh::Disabled;
            }
            RefreshMode::Disabled => return NextRefresh::Disabled,
        };

        // If there is no previous checkpoint, we need to start a refresh.
        let Some(prev_checkpoint_time) = previous_checkpoint else {
            return NextRefresh::WaitFor(Duration::ZERO);
        };

        // If the refresh interval is set, we need to start a refresh if the elapsed time since the last checkpoint is greater than the refresh interval.
        // Otherwise, we don't need to start a refresh.
        if let Some(check_interval) = self.check_interval {
            let elapsed_time_since_checkpoint = SystemTime::now()
                .duration_since(prev_checkpoint_time)
                .unwrap_or(Duration::ZERO);
            if elapsed_time_since_checkpoint > check_interval {
                // The elapsed time since the last checkpoint is greater than the refresh interval, so we need to refresh now.
                NextRefresh::WaitFor(Duration::ZERO)
            } else {
                match refresh_on_startup {
                    // The elapsed time since the last checkpoint is less than the refresh interval, so we need to wait for the refresh interval to pass.
                    RefreshOnStartup::Auto => {
                        NextRefresh::WaitFor(check_interval - elapsed_time_since_checkpoint)
                    }
                    // The refresh mode is `Always`, so we need to refresh now.
                    RefreshOnStartup::Always => NextRefresh::WaitFor(Duration::ZERO),
                }
            }
        } else {
            match refresh_on_startup {
                // We have a previous checkpoint, but no refresh interval, so we don't need to refresh.
                RefreshOnStartup::Auto => NextRefresh::Disabled,
                // We have a previous checkpoint, but the refresh mode is `Always`, so we need to refresh now.
                RefreshOnStartup::Always => NextRefresh::WaitFor(Duration::ZERO),
            }
        }
    }
}

fn validate_time_partition_format(
    data_type: &arrow::datatypes::DataType,
    dataset_name: &str,
    time_column: &str,
    time_format: TimeFormat,
) -> Result<(), Error> {
    let mut invalid = false;
    match data_type {
        arrow::datatypes::DataType::Utf8 | arrow::datatypes::DataType::LargeUtf8 => {
            if time_format != TimeFormat::ISO8601 {
                invalid = true;
            }
        }
        arrow::datatypes::DataType::Int8
        | arrow::datatypes::DataType::Int16
        | arrow::datatypes::DataType::Int32
        | arrow::datatypes::DataType::Int64
        | arrow::datatypes::DataType::UInt8
        | arrow::datatypes::DataType::UInt16
        | arrow::datatypes::DataType::UInt32
        | arrow::datatypes::DataType::UInt64
        | arrow::datatypes::DataType::Float16
        | arrow::datatypes::DataType::Float32
        | arrow::datatypes::DataType::Float64 => {
            if time_format != TimeFormat::UnixSeconds && time_format != TimeFormat::UnixMillis {
                invalid = true;
            }
        }
        arrow::datatypes::DataType::Timestamp(_, None) => {
            if time_format != TimeFormat::Timestamp {
                invalid = true;
            }
        }
        arrow::datatypes::DataType::Timestamp(_, Some(_)) => {
            if time_format != TimeFormat::Timestamptz {
                invalid = true;
            }
        }
        arrow::datatypes::DataType::Date32 => {
            if time_format != TimeFormat::Date {
                invalid = true;
            }
        }
        arrow::datatypes::DataType::Null
        | arrow::datatypes::DataType::Boolean
        | arrow::datatypes::DataType::Date64
        | arrow::datatypes::DataType::Time32(_)
        | arrow::datatypes::DataType::Time64(_)
        | arrow::datatypes::DataType::Duration(_)
        | arrow::datatypes::DataType::Interval(_)
        | arrow::datatypes::DataType::Binary
        | arrow::datatypes::DataType::FixedSizeBinary(_)
        | arrow::datatypes::DataType::LargeBinary
        | arrow::datatypes::DataType::BinaryView
        | arrow::datatypes::DataType::Utf8View
        | arrow::datatypes::DataType::List(_)
        | arrow::datatypes::DataType::ListView(_)
        | arrow::datatypes::DataType::FixedSizeList(_, _)
        | arrow::datatypes::DataType::LargeList(_)
        | arrow::datatypes::DataType::LargeListView(_)
        | arrow::datatypes::DataType::Struct(_)
        | arrow::datatypes::DataType::Union(_, _)
        | arrow::datatypes::DataType::Dictionary(_, _)
        | arrow::datatypes::DataType::Decimal32(_, _)
        | arrow::datatypes::DataType::Decimal64(_, _)
        | arrow::datatypes::DataType::Decimal128(_, _)
        | arrow::datatypes::DataType::Decimal256(_, _)
        | arrow::datatypes::DataType::Map(_, _)
        | arrow::datatypes::DataType::RunEndEncoded(_, _) => {
            invalid = true;
        }
    }

    if invalid {
        return Err(Error::TimeFormatMismatch {
            table_name: dataset_name.to_string(),
            time_column: time_column.to_string(),
            expected_time_format: time_format.to_string(),
            actual_time_format: data_type.to_string(),
        });
    }

    Ok(())
}

impl Default for Refresh {
    fn default() -> Self {
        Self {
            time_column: None,
            time_format: None,
            time_partition_column: None,
            time_partition_format: None,
            check_interval: None,
            max_jitter: None,
            sql: None,
            mode: RefreshMode::Full,
            period: None,
            append_overlap: None,
            retry_enabled: false,
            retry_max_attempts: None,
            caching_ttl: None,
        }
    }
}

pub enum AccelerationRefreshMode {
    Disabled,
    Full(Receiver<Option<RefreshOverrides>>),
    Append(Receiver<Option<RefreshOverrides>>),
    Changes(ChangesStream),
    Caching(Receiver<Option<RefreshOverrides>>),
}

pub struct Refresher {
    runtime_status: Arc<status::RuntimeStatus>,
    dataset_name: TableReference,
    metrics: Option<Metrics>,
    federated: Arc<FederatedTable>,
    federated_source: Option<String>,
    refresh: Arc<RwLock<Refresh>>,
    accelerator: Arc<dyn TableProvider>,
    // `Weak` reference to `Caching` is used to prevent blocking cache cleanup during runtime termination.
    caching: Option<Weak<Caching>>,
    refresh_task_runner: Option<RefreshTaskRunner>,
    checkpointer: Option<Arc<dyn DatasetCheckpointer>>,
    refresh_on_startup: RefreshOnStartup,
    synchronize_with: Option<SynchronizedTable>,
    snapshot_config: Option<SnapshotCreationConfig>,
    snapshot_interval_task: Option<tokio::task::JoinHandle<()>>,

    initial_load_completed: Arc<AtomicBool>,
    disable_federation: bool,
    semaphore: Option<Arc<Semaphore>>,
    /// Notification for completion of refresh operation
    on_complete_notification: Option<Arc<Notify>>,
    cpu_runtime: Option<Handle>,
    io_runtime: Handle,
    resource_monitor: Option<crate::resource_monitor::ResourceMonitor>,
    /// Mutex to protect concurrent access to the accelerator during cache/snapshot operations
    /// Shared with `CachingAccelerationScanExec`.
    accelerator_write_mutex: Arc<Mutex<()>>,
    /// The bootstrap status from dataset initialization.
    bootstrap_status: BootstrapStatus,
    /// Timestamp (milliseconds since epoch) of the last `insert_into` operation.
    /// Shared with `AcceleratedTable`.
    last_updated_at: Option<Arc<AtomicI64>>,
}

impl std::fmt::Debug for Refresher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Refresher")
            .field("dataset_name", &self.dataset_name)
            .field("federated", &self.federated)
            .field("federated_source", &self.federated_source)
            .field("refresh", &self.refresh)
            .field("accelerator", &self.accelerator)
            .finish_non_exhaustive()
    }
}

impl Refresher {
    #[expect(clippy::too_many_arguments)]
    pub fn new(
        runtime_status: Arc<status::RuntimeStatus>,
        dataset_name: TableReference,
        federated: Arc<FederatedTable>,
        federated_source: Option<String>,
        refresh: Arc<RwLock<Refresh>>,
        accelerator: Arc<dyn TableProvider>,
        cpu_runtime: Option<Handle>,
        io_runtime: Handle,
        accelerator_write_mutex: Arc<Mutex<()>>,
    ) -> Self {
        Self {
            runtime_status,
            dataset_name,
            federated,
            federated_source,
            refresh,
            accelerator,
            caching: None,
            refresh_task_runner: None,
            checkpointer: None,
            refresh_on_startup: RefreshOnStartup::default(),
            synchronize_with: None,
            initial_load_completed: Arc::new(AtomicBool::new(false)),
            disable_federation: false,
            semaphore: None,
            on_complete_notification: None,
            snapshot_config: None,
            snapshot_interval_task: None,
            metrics: None,
            cpu_runtime,
            io_runtime,
            resource_monitor: None,
            accelerator_write_mutex,
            bootstrap_status: BootstrapStatus::none(),
            last_updated_at: None,
        }
    }

    pub fn caching(&mut self, caching: &Option<Arc<Caching>>) -> &mut Self {
        self.caching = caching.as_ref().map(Arc::downgrade);
        self
    }

    pub fn checkpointer(
        &mut self,
        checkpointer: Option<Arc<dyn DatasetCheckpointer>>,
    ) -> &mut Self {
        self.checkpointer = checkpointer;
        self
    }

    pub fn refresh_on_startup(&mut self, refresh_on_startup: RefreshOnStartup) -> &mut Self {
        self.refresh_on_startup = refresh_on_startup;
        self
    }

    /// Synchronize further refreshes with an existing accelerated table after the initial load completes
    pub fn synchronize_with(&mut self, synchronized_table: SynchronizedTable) -> &mut Self {
        self.synchronize_with = Some(synchronized_table);
        self
    }

    /// Disable refresh queries federation for this refresher
    pub fn disable_federation(&mut self, disable: bool) -> &mut Self {
        self.disable_federation = disable;
        self
    }

    pub fn semaphore(&mut self, semaphore: Arc<Semaphore>) -> &mut Self {
        self.semaphore = Some(semaphore);
        self
    }

    pub fn with_last_updated_at(&mut self, last_updated_at: Option<Arc<AtomicI64>>) -> &mut Self {
        self.last_updated_at = last_updated_at;
        self
    }

    pub fn with_completion_notifier(&mut self, on_complete_notification: Arc<Notify>) -> &mut Self {
        self.on_complete_notification = Some(on_complete_notification);
        self
    }

    pub fn with_metrics(&mut self, metrics: Option<Metrics>) -> &mut Self {
        self.metrics = metrics;
        self
    }

    pub fn with_snapshot_creation_config(
        &mut self,
        snapshot_config: Option<SnapshotCreationConfig>,
    ) -> &mut Self {
        self.snapshot_config = snapshot_config;
        self
    }

    /// Set the bootstrap status from dataset initialization.
    pub fn set_bootstrap_status(&mut self, bootstrap_status: BootstrapStatus) -> &mut Self {
        self.bootstrap_status = bootstrap_status;
        self
    }

    #[must_use]
    pub fn on_complete_notification(&self) -> Option<Arc<Notify>> {
        self.on_complete_notification.clone()
    }

    pub fn set_initial_load_completed(&self, initial_load_completed: bool) {
        self.initial_load_completed
            .store(initial_load_completed, Ordering::Relaxed);
    }

    #[must_use]
    pub fn initial_load_completed(&self) -> bool {
        self.initial_load_completed.load(Ordering::Relaxed)
    }

    pub fn with_resource_monitor(
        &mut self,
        monitor: crate::resource_monitor::ResourceMonitor,
    ) -> &mut Self {
        self.resource_monitor = Some(monitor);
        self
    }

    /// Compute a specific delay based on `period +- rand(0, max_jitter)`.
    fn compute_delay(period: Duration, max_jitter: Option<Duration>) -> Duration {
        match max_jitter {
            Some(max_jitter) if !max_jitter.is_zero() => {
                let jitter = rand::rng().random_range(Duration::from_secs(0)..max_jitter);
                if rand::rng().random_bool(0.5) {
                    period + jitter
                } else {
                    period.saturating_sub(jitter)
                }
            }
            Some(_) | None => period,
        }
    }

    pub async fn start(
        &mut self,
        acceleration_refresh_mode: AccelerationRefreshMode,
    ) -> super::Result<Option<tokio::task::JoinHandle<()>>> {
        let dataset_name = self.dataset_name.clone();
        let time_column = self.refresh.read().await.time_column.clone();
        let initial_refresh_delay = {
            let refresh = self.refresh.read().await;

            // If the table already has an existing acceleration and the refresh options wouldn't start a new refresh,
            // we can exit early.
            match refresh
                .startup_next_refresh(self.refresh_on_startup, self.checkpointer.clone())
                .await
            {
                NextRefresh::Disabled => {
                    tracing::debug!(
                        "Skipped refresh for {}: existing acceleration is available",
                        self.dataset_name
                    );
                    None
                }
                NextRefresh::WaitFor(duration) => {
                    if !duration.is_zero() {
                        tracing::info!(
                            "{dataset_name}: Waiting {}s until next refresh",
                            duration.as_secs()
                        );
                    }
                    Some(duration)
                }
            }
        };

        let (snapshot_manager, snapshot_trigger) = match self.snapshot_config.as_ref() {
            Some(SnapshotCreationConfig {
                manager,
                create_trigger,
            }) => (Some(Arc::clone(manager)), Some(create_trigger)),
            None => (None, None),
        };

        let checkpointer = self.checkpointer.clone();
        let federated_schema = self.federated.schema();

        let mut on_start_refresh_external = match (acceleration_refresh_mode, time_column) {
            (AccelerationRefreshMode::Disabled, _) => return Ok(None),
            (
                AccelerationRefreshMode::Append(receiver)
                | AccelerationRefreshMode::Full(receiver)
                | AccelerationRefreshMode::Caching(receiver),
                _,
            ) => receiver,
            (AccelerationRefreshMode::Changes(stream), _) => {
                let (snapshot_interval_task, on_batch_process_callback) = match snapshot_trigger {
                    None | Some(SnapshotCreateTrigger::RefreshComplete) => (None, None),
                    Some(SnapshotCreateTrigger::Interval(duration)) => (
                        spawn_snapshot_interval_task(
                            Some(*duration),
                            checkpointer.clone(),
                            snapshot_manager.clone(),
                            Arc::clone(&self.accelerator_write_mutex),
                            dataset_name.clone(),
                            Arc::clone(&federated_schema),
                            Arc::clone(&self.runtime_status),
                            self.bootstrap_status.clone(),
                            self.last_updated_at.clone(),
                        ),
                        None,
                    ),
                    Some(SnapshotCreateTrigger::Batches(batches)) => (
                        None,
                        create_periodic_snapshot_callback(
                            *batches,
                            checkpointer.clone(),
                            snapshot_manager,
                            Arc::clone(&self.accelerator_write_mutex),
                            &self.dataset_name,
                            self.federated.schema(),
                            Arc::clone(&self.runtime_status),
                            self.last_updated_at.clone(),
                        ),
                    ),
                };
                self.snapshot_interval_task = snapshot_interval_task;

                return Ok(Some(
                    self.start_changes_stream(stream, on_batch_process_callback),
                ));
            }
        };

        let mut refresh_task_runner = RefreshTaskRunner::builder(
            Arc::clone(&self.runtime_status),
            self.dataset_name.clone(),
            Arc::clone(&self.federated),
            self.federated_source.clone(),
            Arc::clone(&self.refresh),
            Arc::clone(&self.accelerator),
            self.io_runtime.clone(),
            Arc::clone(&self.accelerator_write_mutex),
        )
        .with_disable_federation(self.disable_federation);

        if let Some(semaphore) = &self.semaphore {
            refresh_task_runner = refresh_task_runner.with_semaphore(Arc::clone(semaphore));
        }

        refresh_task_runner = refresh_task_runner.with_metrics(self.metrics.clone());

        refresh_task_runner = refresh_task_runner.with_cpu_runtime(self.cpu_runtime.clone());

        if let Some(ref resource_monitor) = self.resource_monitor {
            refresh_task_runner =
                refresh_task_runner.with_resource_monitor(resource_monitor.clone());
        }

        let mut refresh_task_runner = refresh_task_runner.build();

        let (start_refresh, mut on_refresh_complete) = refresh_task_runner.start()?;
        self.refresh_task_runner = Some(refresh_task_runner);

        let notifier = self.on_complete_notification.clone();
        let refresh = Arc::clone(&self.refresh);

        let caching = self.caching.clone();
        let refresh_check_interval = self.refresh.read().await.check_interval;
        let max_jitter = self.refresh.read().await.max_jitter;
        let snapshot_mutex = Arc::clone(&self.accelerator_write_mutex);

        let initial_load_completed = Arc::clone(&self.initial_load_completed);
        let last_updated_at = self.last_updated_at.clone();

        let synchronize_with = self.synchronize_with.clone();

        let (snapshot_interval_task, create_checkpoint_snapshot_after_refresh) =
            match snapshot_trigger {
                // This will only create checkpoint - default behavior when snapshots are not configured
                #[expect(clippy::match_same_arms)]
                None => (None, true),
                Some(SnapshotCreateTrigger::Batches(_)) => (None, false),
                Some(SnapshotCreateTrigger::RefreshComplete) => (None, true),
                Some(SnapshotCreateTrigger::Interval(duration)) => (
                    spawn_snapshot_interval_task(
                        Some(*duration),
                        checkpointer.clone(),
                        snapshot_manager.clone(),
                        Arc::clone(&self.accelerator_write_mutex),
                        dataset_name.clone(),
                        Arc::clone(&federated_schema),
                        Arc::clone(&self.runtime_status),
                        self.bootstrap_status.clone(),
                        self.last_updated_at.clone(),
                    ),
                    false,
                ),
            };
        self.snapshot_interval_task = snapshot_interval_task;

        if create_checkpoint_snapshot_after_refresh && snapshot_manager.is_some() {
            tracing::info!(
                "Snapshots for dataset {dataset_name} will be created after every refresh"
            );
        }

        // Spawns a tasks that both periodically refreshes the dataset, and upon request, will manually refresh the dataset.
        // The `select!` block handle waiting on both
        //   1. The manual refresh [`Receiver`] channel `on_start_refresh_external`
        //   2. The sleep [`future`] `scheduled_refresh_future`.
        //
        // Doing it in this way stops
        //   1. Periodic and manual refreshes happening at the same time
        //   2. The periodic refresh happening less than `refresh_check_interval` after a manual
        //        refresh (the sleep future is reset when a manual refresh completes).
        Ok(Some(tokio::spawn(async move {
            let mut next_scheduled_refresh_timer =
                initial_refresh_delay.map(|delay| sleep(Self::compute_delay(delay, max_jitter)));

            loop {
                let scheduled_refresh_future: BoxFuture<()> =
                    if let Some(timer) = next_scheduled_refresh_timer.take() {
                        Box::pin(timer)
                    } else {
                        Box::pin(std::future::pending())
                    };

                select! {
                    () = scheduled_refresh_future => {
                        tracing::debug!("Starting scheduled refresh");
                        if let Err(err) = start_refresh.send(None).await {
                            tracing::error!("Failed to execute refresh: {err}");
                        }
                    },
                    Some(overrides_opt) = on_start_refresh_external.recv() => {
                        tracing::debug!("Received external trigger to start refresh");

                        // Apply jitter on manual refreshes. For periodic refreshes, jitter
                        // is added to the timer, `next_scheduled_refresh_timer`.
                        let override_jitter = overrides_opt.as_ref().and_then(|o| o.max_jitter);
                        if let Some(max_jitter) = override_jitter.or(max_jitter) {
                            sleep(Self::compute_delay(Duration::from_secs(0), Some(max_jitter))).await;
                        }

                        if let Err(err) = start_refresh.send(overrides_opt).await {
                            tracing::error!("Failed to execute refresh: {err}");
                        }
                    },
                    Some(res) = on_refresh_complete.recv() => {
                        tracing::debug!("Received refresh task completion callback: {res:?}");

                        if matches!(res, Ok(())) {
                            if let Some(notifier) = &notifier {
                                notify_refresh_done(&dataset_name, &refresh, Arc::clone(notifier)).await;
                            }
                            initial_load_completed.store(true, Ordering::Relaxed);

                            if let Some(cache_provider_ref) = caching.as_ref() {
                                // No cache provider means runtime is shutting down and cache is already cleaned up
                                if let Some(cache_provider) = cache_provider_ref.upgrade()
                                    && let Err(e) = cache_provider.invalidate_for_table(dataset_name.clone()) {
                                        tracing::warn!("Failed to invalidate cached results for dataset {dataset_name}: {e}");
                                    }
                            }

                            if create_checkpoint_snapshot_after_refresh && let Some(checkpointer) = &checkpointer {
                                create_checkpoint_and_snapshot(
                                    checkpointer,
                                    snapshot_manager.as_ref(),
                                    &federated_schema,
                                    &snapshot_mutex,
                                    &dataset_name,
                                    last_updated_at.as_ref(),
                                ).await;
                            }
                        }

                        // The initial load has completed, let's synchronize further refreshes with the existing table and shutdown this refresher
                        if let Some(synchronize_with) = &synchronize_with {
                            synchronize_with
                                .refresher()
                                .add_synchronized_table(synchronize_with.clone())
                                .await;
                            return;
                        }

                        // Restart periodic refresh timer (after either cron or manual dataset refresh).
                        // For datasets with no periodic refresh, this will be a no-op.
                        if let Some(refresh_check_interval) = refresh_check_interval {
                            next_scheduled_refresh_timer = Some(sleep(Self::compute_delay(
                                refresh_check_interval,
                                max_jitter,
                            )));
                        }
                    }
                }
            }
        })))
    }

    /// Subscribes a new table provider to receive refresh notifications from an existing full refresh mode accelerated table
    ///
    /// # Panics
    ///
    /// Panics if this function is called on an accelerated table that is not configured with a full refresh mode
    pub async fn add_synchronized_table(&self, synchronized_table: SynchronizedTable) {
        if !matches!(self.refresh.read().await.mode, RefreshMode::Full) {
            unreachable!(
                "Only tables configured with a full refresh mode can subscribe to new table providers - this is an implementation bug"
            );
        }

        if let Some(refresh_task_runner) = &self.refresh_task_runner {
            refresh_task_runner
                .add_synchronized_table(synchronized_table)
                .await;
        } else {
            unreachable!(
                "Only tables configured with a full refresh mode can subscribe to new table providers - this is an implementation bug"
            );
        }
    }

    fn start_changes_stream(
        &mut self,
        changes_stream: ChangesStream,
        on_batch_process_callback: Option<SnapshotCallback>,
    ) -> tokio::task::JoinHandle<()> {
        let refresh_task = Arc::new(
            RefreshTask::builder(
                Arc::clone(&self.runtime_status),
                self.dataset_name.clone(),
                Arc::clone(&self.federated),
                self.federated_source.clone(),
                Arc::clone(&self.accelerator),
                self.io_runtime.clone(),
                Arc::clone(&self.accelerator_write_mutex),
            )
            .with_disable_federation(self.disable_federation)
            .with_cpu_runtime(self.cpu_runtime.clone())
            .with_metrics(self.metrics.clone())
            .with_on_stream_batch_process_callback(on_batch_process_callback)
            .build(),
        );

        let caching = self.caching.clone();
        let refresh = Arc::clone(&self.refresh);
        let initial_load_completed = Arc::clone(&self.initial_load_completed);

        let notifier = self.on_complete_notification.clone();
        tokio::spawn(async move {
            if let Err(err) = refresh_task
                .start_changes_stream(
                    refresh,
                    changes_stream,
                    caching,
                    notifier,
                    initial_load_completed,
                )
                .await
            {
                tracing::error!("Changes stream failed with error: {err}");
            }
        })
    }
}

impl Drop for Refresher {
    fn drop(&mut self) {
        if let Some(mut refresh_task_runner) = self.refresh_task_runner.take() {
            refresh_task_runner.abort();
        }
        if let Some(task) = self.snapshot_interval_task.take() {
            task.abort();
        }
    }
}

pub(crate) fn get_timestamp(time: SystemTime) -> u128 {
    time.duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
}

async fn notify_refresh_done(
    dataset_name: &TableReference,
    refresh: &Arc<RwLock<Refresh>>,
    ready_sender: Arc<Notify>,
) {
    ready_sender.notify_waiters();

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();

    let mut labels = vec![KeyValue::new("dataset", dataset_name.to_string())];
    let refresh_guard = refresh.read().await;
    if let Some(sql) = &refresh_guard.sql {
        labels.push(KeyValue::new("sql", sql.clone()));
    }

    metrics::LAST_REFRESH_TIME_MS.record(now.as_secs_f64() * 1000.0, &labels);
}

#[cfg(test)]
mod tests {
    use arrow::{
        array::{ArrowNativeTypeOp, RecordBatch, StringArray, StructArray, UInt64Array},
        datatypes::{DataType, Field, Fields, Schema},
    };
    use data_components::arrow::write::MemTable;
    use datafusion::{physical_plan::collect, prelude::SessionContext};
    use opentelemetry::global;
    use opentelemetry_sdk::{Resource, metrics::SdkMeterProvider};
    use prometheus::proto::MetricType;
    use tokio::{sync::mpsc, time::timeout};

    use crate::dataaccelerator::spice_sys::Result;
    use crate::status;
    use arrow::datatypes::SchemaRef;
    use async_trait::async_trait;

    use super::*;

    // Mock implementation of DatasetCheckpointer trait
    struct MockCheckpointer {
        exists_value: bool,
        last_checkpoint_time: Option<SystemTime>,
    }

    impl MockCheckpointer {
        fn new_arc(
            exists_value: bool,
            last_checkpoint_time: Option<SystemTime>,
        ) -> Arc<dyn DatasetCheckpointer> {
            Arc::new(Self {
                exists_value,
                last_checkpoint_time,
            })
        }
    }

    #[async_trait]
    impl DatasetCheckpointer for MockCheckpointer {
        async fn exists(&self) -> bool {
            self.exists_value
        }

        async fn checkpoint(
            &self,
            _schema: &SchemaRef,
        ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
            // Not needed for this test
            Ok(())
        }

        async fn get_schema(
            &self,
        ) -> Result<Option<SchemaRef>, Box<dyn std::error::Error + Send + Sync>> {
            // Not needed for this test
            Ok(None)
        }

        async fn last_checkpoint_time(
            &self,
        ) -> Result<Option<SystemTime>, Box<dyn std::error::Error + Send + Sync>> {
            Ok(self.last_checkpoint_time)
        }
    }

    async fn setup_and_test(
        status: Arc<status::RuntimeStatus>,
        source_data: Vec<&str>,
        existing_data: Vec<&str>,
        expected_size: usize,
    ) {
        let schema = Arc::new(Schema::new(vec![arrow::datatypes::Field::new(
            "time_in_string",
            DataType::Utf8,
            false,
        )]));
        let arr = StringArray::from(source_data);

        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(arr)])
            .expect("data should be created");

        let mem_table = Arc::new(
            MemTable::try_new(Arc::clone(&schema), vec![vec![batch]])
                .expect("mem table should be created"),
        );
        let federated = Arc::new(FederatedTable::new_unchecked(mem_table));

        let arr = StringArray::from(existing_data);

        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(arr)])
            .expect("data should be created");

        let accelerator = Arc::new(
            MemTable::try_new(schema, vec![vec![batch]]).expect("mem table should be created"),
        ) as Arc<dyn TableProvider>;

        let refresh = Refresh::new(RefreshMode::Full);

        let notifier = Arc::new(Notify::new());
        let mut refresher = Refresher::new(
            status,
            TableReference::bare("test"),
            federated,
            Some("mem_table".to_string()),
            Arc::new(RwLock::new(refresh)),
            Arc::clone(&accelerator),
            None,
            Handle::current(),
            Arc::new(Mutex::new(())),
        );

        refresher.with_completion_notifier(Arc::clone(&notifier));
        let (trigger, receiver) = mpsc::channel::<Option<RefreshOverrides>>(1);
        let acceleration_refresh_mode = AccelerationRefreshMode::Full(receiver);
        let refresh_handle = refresher
            .start(acceleration_refresh_mode)
            .await
            .expect("Should start refresh task");

        trigger
            .send(None)
            .await
            .expect("trigger sent correctly to refresh");

        timeout(
            Duration::from_secs(2),
            async move { notifier.notified().await },
        )
        .await
        .expect("finish before the timeout");

        let ctx = SessionContext::new();
        let state = ctx.state();

        let plan = accelerator
            .scan(&state, None, &[], None)
            .await
            .expect("Scan plan can be constructed");

        let result = collect(plan, ctx.task_ctx())
            .await
            .expect("Query successful");

        assert_eq!(expected_size, result.first().expect("result").num_rows());

        drop(refresh_handle);
    }

    #[tokio::test]
    async fn test_refresh_full() {
        let status = status::RuntimeStatus::new();
        setup_and_test(
            Arc::clone(&status),
            vec!["1970-01-01", "2012-12-01T11:11:11Z", "2012-12-01T11:11:12Z"],
            vec![],
            3,
        )
        .await;
        setup_and_test(
            Arc::clone(&status),
            vec!["1970-01-01", "2012-12-01T11:11:11Z", "2012-12-01T11:11:12Z"],
            vec![
                "1970-01-01",
                "2012-12-01T11:11:11Z",
                "2012-12-01T11:11:12Z",
                "2012-12-01T11:11:15Z",
            ],
            3,
        )
        .await;
        setup_and_test(
            Arc::clone(&status),
            vec![],
            vec![
                "1970-01-01",
                "2012-12-01T11:11:11Z",
                "2012-12-01T11:11:12Z",
                "2012-12-01T11:11:15Z",
            ],
            0,
        )
        .await;
    }

    #[tokio::test]
    async fn test_refresh_status_change_to_ready() {
        async fn wait_until_ready_status(
            registry: &prometheus::Registry,
            desired: status::ComponentStatus,
            max_attempts: usize,
            delay: Duration,
        ) -> bool {
            for _attempt in 0..max_attempts {
                let metrics = registry.gather();
                if let Some(metric) = metrics.iter().find(|m| {
                    m.name() == "dataset_load_state" && m.get_field_type() == MetricType::GAUGE
                }) && let Some(gauge) = metric.get_metric()[0].get_gauge().as_ref()
                    && gauge.value().is_eq(f64::from(desired as i32))
                {
                    return true;
                }
                tokio::time::sleep(delay).await;
            }
            false
        }

        let registry = prometheus::Registry::new();

        let resource = Resource::builder().build();

        let prometheus_exporter = opentelemetry_prometheus::exporter()
            .with_registry(registry.clone())
            .without_scope_info()
            .without_units()
            .without_counter_suffixes()
            .without_target_info()
            .build()
            .expect("to build prometheus exporter");

        let provider = SdkMeterProvider::builder()
            .with_resource(resource)
            .with_reader(prometheus_exporter)
            .build();
        global::set_meter_provider(provider);

        let status = status::RuntimeStatus::new();
        status.update_dataset(
            &TableReference::bare("test"),
            status::ComponentStatus::Refreshing,
        );

        setup_and_test(
            Arc::clone(&status),
            vec!["1970-01-01", "2012-12-01T11:11:11Z", "2012-12-01T11:11:12Z"],
            vec![],
            3,
        )
        .await;

        // Use more attempts with shorter delays for better test performance
        assert!(
            wait_until_ready_status(
                &registry,
                status::ComponentStatus::Ready,
                60,
                Duration::from_millis(50)
            )
            .await,
            "Status did not change to Ready within timeout"
        );

        status.update_dataset(
            &TableReference::bare("test"),
            status::ComponentStatus::Refreshing,
        );

        setup_and_test(Arc::clone(&status), vec![], vec![], 0).await;

        assert!(
            wait_until_ready_status(
                &registry,
                status::ComponentStatus::Ready,
                60,
                Duration::from_millis(50)
            )
            .await,
            "Status did not change to Ready within timeout"
        );
    }

    #[tokio::test]
    async fn test_refresh_append_batch_for_iso8601() {
        async fn test(
            source_data: Vec<&str>,
            existing_data: Vec<&str>,
            expected_size: usize,
            message: &str,
        ) {
            let schema = Arc::new(Schema::new(vec![arrow::datatypes::Field::new(
                "time_in_string",
                DataType::Utf8,
                false,
            )]));
            let arr = StringArray::from(source_data);

            let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(arr)])
                .expect("data should be created");

            let mem_table = Arc::new(
                MemTable::try_new(Arc::clone(&schema), vec![vec![batch]])
                    .expect("mem table should be created"),
            );
            let federated = Arc::new(FederatedTable::new_unchecked(mem_table));

            let arr = StringArray::from(existing_data);

            let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(arr)])
                .expect("data should be created");

            let accelerator = Arc::new(
                MemTable::try_new(schema, vec![vec![batch]]).expect("mem table should be created"),
            ) as Arc<dyn TableProvider>;

            let refresh = Refresh::new(RefreshMode::Append)
                .time_column("time_in_string".to_string())
                .time_format(TimeFormat::ISO8601);

            let notifier = Arc::new(Notify::new());
            let mut refresher = Refresher::new(
                status::RuntimeStatus::new(),
                TableReference::bare("test"),
                federated,
                Some("mem_table".to_string()),
                Arc::new(RwLock::new(refresh)),
                Arc::clone(&accelerator),
                None,
                Handle::current(),
                Arc::new(Mutex::new(())),
            );

            refresher.with_completion_notifier(Arc::clone(&notifier));
            let (trigger, receiver) = mpsc::channel::<Option<RefreshOverrides>>(1);
            let acceleration_refresh_mode = AccelerationRefreshMode::Append(receiver);
            let refresh_handle = refresher
                .start(acceleration_refresh_mode)
                .await
                .expect("Should start refresh task");

            trigger
                .send(None)
                .await
                .expect("trigger sent correctly to refresh");

            timeout(
                Duration::from_secs(2),
                async move { notifier.notified().await },
            )
            .await
            .expect("finish before the timeout");

            let ctx = SessionContext::new();
            let state = ctx.state();

            let plan = accelerator
                .scan(&state, None, &[], None)
                .await
                .expect("Scan plan can be constructed");

            let result = collect(plan, ctx.task_ctx())
                .await
                .expect("Query successful");

            assert_eq!(
                expected_size,
                result.into_iter().map(|f| f.num_rows()).sum::<usize>(),
                "{message}"
            );

            drop(refresh_handle);
        }

        test(
            vec!["1970-01-01", "2012-12-01T11:11:11Z", "2012-12-01T11:11:12Z"],
            vec![],
            3,
            "should insert all data into empty accelerator",
        )
        .await;
        test(
            vec!["1970-01-01", "2012-12-01T11:11:11Z", "2012-12-01T11:11:12Z"],
            vec![
                "1970-01-01",
                "2012-12-01T11:11:11Z",
                "2012-12-01T11:11:12Z",
                "2012-12-01T11:11:15Z",
            ],
            4,
            "should not insert any stale data and keep original size",
        )
        .await;
        test(
            vec![],
            vec![
                "1970-01-01",
                "2012-12-01T11:11:11Z",
                "2012-12-01T11:11:12Z",
                "2012-12-01T11:11:15Z",
            ],
            4,
            "should keep original data of accelerator when no new data is found",
        )
        .await;
        test(
            vec!["2012-12-01T11:11:16Z", "2012-12-01T11:11:17Z"],
            vec![
                "1970-01-01",
                "2012-12-01T11:11:11Z",
                "2012-12-01T11:11:12Z",
                "2012-12-01T11:11:15Z",
            ],
            6,
            "should apply new data onto existing data",
        )
        .await;

        // Known limitation, doesn't dedup
        test(
            vec!["2012-12-01T11:11:15Z", "2012-12-01T11:11:15Z"],
            vec![
                "1970-01-01",
                "2012-12-01T11:11:11Z",
                "2012-12-01T11:11:12Z",
                "2012-12-01T11:11:15Z",
            ],
            4,
            "should not apply same timestamp data",
        )
        .await;
    }

    #[tokio::test]
    async fn test_refresh_append_batch_for_timestamp() {
        async fn test(
            source_data: Vec<u64>,
            existing_data: Vec<u64>,
            expected_size: usize,
            time_format: Option<TimeFormat>,
            append_overlap: Option<Duration>,
            message: &str,
        ) {
            let schema = Arc::new(Schema::new(vec![arrow::datatypes::Field::new(
                "time",
                DataType::UInt64,
                false,
            )]));
            let arr = UInt64Array::from(source_data);

            let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(arr)])
                .expect("data should be created");

            let mem_table = Arc::new(
                MemTable::try_new(Arc::clone(&schema), vec![vec![batch]])
                    .expect("mem table should be created"),
            );
            let federated = Arc::new(FederatedTable::new_unchecked(mem_table));

            let arr = UInt64Array::from(existing_data);

            let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(arr)])
                .expect("data should be created");

            let accelerator = Arc::new(
                MemTable::try_new(schema, vec![vec![batch]]).expect("mem table should be created"),
            ) as Arc<dyn TableProvider>;

            let mut refresh = Refresh::new(RefreshMode::Append).time_column("time".to_string());

            if let Some(time_format) = time_format {
                refresh = refresh.time_format(time_format);
            }

            if let Some(append_overlap) = append_overlap {
                refresh = refresh.append_overlap(append_overlap);
            }

            let notifier = Arc::new(Notify::new());
            let mut refresher = Refresher::new(
                status::RuntimeStatus::new(),
                TableReference::bare("test"),
                federated,
                Some("mem_table".to_string()),
                Arc::new(RwLock::new(refresh)),
                Arc::clone(&accelerator),
                None,
                Handle::current(),
                Arc::new(Mutex::new(())),
            );

            refresher.with_completion_notifier(Arc::clone(&notifier));
            let (trigger, receiver) = mpsc::channel::<Option<RefreshOverrides>>(1);
            let acceleration_refresh_mode = AccelerationRefreshMode::Append(receiver);
            let refresh_handle = refresher
                .start(acceleration_refresh_mode)
                .await
                .expect("Should start refresh task");

            trigger
                .send(None)
                .await
                .expect("trigger sent correctly to refresh");

            timeout(
                Duration::from_secs(2),
                async move { notifier.notified().await },
            )
            .await
            .expect("finish before the timeout");

            let ctx = SessionContext::new();
            let state = ctx.state();

            let plan = accelerator
                .scan(&state, None, &[], None)
                .await
                .expect("Scan plan can be constructed");

            let result = collect(plan, ctx.task_ctx())
                .await
                .expect("Query successful");

            assert_eq!(
                expected_size,
                result.into_iter().map(|f| f.num_rows()).sum::<usize>(),
                "{message}"
            );

            drop(refresh_handle);
        }

        test(
            vec![1, 2, 3],
            vec![],
            3,
            Some(TimeFormat::UnixSeconds),
            None,
            "should insert all data into empty accelerator",
        )
        .await;
        test(
            vec![1, 2, 3],
            vec![2, 3, 4, 5],
            4,
            Some(TimeFormat::UnixSeconds),
            None,
            "should not insert any stale data and keep original size",
        )
        .await;
        test(
            vec![],
            vec![1, 2, 3, 4],
            4,
            Some(TimeFormat::UnixSeconds),
            None,
            "should keep original data of accelerator when no new data is found",
        )
        .await;
        test(
            vec![5, 6],
            vec![1, 2, 3, 4],
            6,
            Some(TimeFormat::UnixSeconds),
            None,
            "should apply new data onto existing data",
        )
        .await;

        // Known limitation, doesn't dedup
        test(
            vec![4, 4],
            vec![1, 2, 3, 4],
            4,
            Some(TimeFormat::UnixSeconds),
            None,
            "should not apply same timestamp data",
        )
        .await;

        test(
            vec![4, 5, 6, 7, 8, 9, 10],
            vec![1, 2, 3, 9],
            10,
            Some(TimeFormat::UnixSeconds),
            Some(Duration::from_secs(10)),
            "should apply late arrival and new data onto existing data",
        )
        .await;

        test(
            vec![4, 5, 6, 7, 8, 9, 10],
            vec![1, 2, 3, 9],
            7, // 1, 2, 3, 7, 8, 9, 10
            Some(TimeFormat::UnixSeconds),
            Some(Duration::from_secs(3)),
            "should apply late arrival within the append overlap period and new data onto existing data",
        )
        .await;

        test(
            vec![4, 5, 6, 7, 8, 9, 10],
            vec![1, 2, 3, 9],
            10, // all the data
            Some(TimeFormat::UnixMillis),
            Some(Duration::from_secs(3)),
            "should fetch all data as 3 seconds is enough to cover all time span in source with millis",
        )
        .await;
    }

    #[tokio::test]
    async fn test_refresh_append_batch_for_timestamp_with_more_complicated_structs() {
        async fn test(
            source_data: Vec<u64>,
            existing_data: Vec<u64>,
            expected_size: usize,
            time_format: Option<TimeFormat>,
            append_overlap: Option<Duration>,
            duplicated_incoming_data: bool,
            message: &str,
        ) {
            let original_schema = Arc::new(Schema::new(vec![arrow::datatypes::Field::new(
                "time",
                DataType::UInt64,
                false,
            )]));
            let arr = UInt64Array::from(source_data);
            let batch =
                RecordBatch::try_new(Arc::clone(&original_schema), vec![Arc::new(arr.clone())])
                    .expect("data should be created");

            let struct_array = StructArray::from(batch);
            let schema = Arc::new(Schema::new(vec![
                arrow::datatypes::Field::new("time", DataType::UInt64, false),
                arrow::datatypes::Field::new(
                    "struct",
                    DataType::Struct(Fields::from(vec![arrow::datatypes::Field::new(
                        "time",
                        DataType::UInt64,
                        false,
                    )])),
                    false,
                ),
            ]));
            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(arr), Arc::new(struct_array)],
            )
            .expect("data should be created");

            let mut data = vec![vec![batch.clone()]];
            if duplicated_incoming_data {
                data = vec![vec![batch.clone()], vec![batch]];
            }

            let mem_table = Arc::new(
                MemTable::try_new(Arc::clone(&schema), data).expect("mem table should be created"),
            );
            let federated = Arc::new(FederatedTable::new_unchecked(mem_table));

            let arr = UInt64Array::from(existing_data);
            let batch =
                RecordBatch::try_new(Arc::clone(&original_schema), vec![Arc::new(arr.clone())])
                    .expect("data should be created");
            let struct_array = StructArray::from(batch);
            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(arr), Arc::new(struct_array)],
            )
            .expect("data should be created");

            let accelerator = Arc::new(
                MemTable::try_new(schema, vec![vec![batch]]).expect("mem table should be created"),
            ) as Arc<dyn TableProvider>;

            let mut refresh = Refresh::new(RefreshMode::Append).time_column("time".to_string());

            if let Some(time_format) = time_format {
                refresh = refresh.time_format(time_format);
            }

            if let Some(append_overlap) = append_overlap {
                refresh = refresh.append_overlap(append_overlap);
            }

            let notifier = Arc::new(Notify::new());
            let mut refresher = Refresher::new(
                status::RuntimeStatus::new(),
                TableReference::bare("test"),
                federated,
                Some("mem_table".to_string()),
                Arc::new(RwLock::new(refresh)),
                Arc::clone(&accelerator),
                None,
                Handle::current(),
                Arc::new(Mutex::new(())),
            );

            refresher.with_completion_notifier(Arc::clone(&notifier));
            let (trigger, receiver) = mpsc::channel::<Option<RefreshOverrides>>(1);
            let acceleration_refresh_mode = AccelerationRefreshMode::Append(receiver);
            let refresh_handle = refresher
                .start(acceleration_refresh_mode)
                .await
                .expect("Should start refresh task");
            trigger
                .send(None)
                .await
                .expect("trigger sent correctly to refresh");

            timeout(
                Duration::from_secs(2),
                async move { notifier.notified().await },
            )
            .await
            .expect("finish before the timeout");

            let ctx = SessionContext::new();
            let state = ctx.state();

            let plan = accelerator
                .scan(&state, None, &[], None)
                .await
                .expect("Scan plan can be constructed");

            let result = collect(plan, ctx.task_ctx())
                .await
                .expect("Query successful");

            assert_eq!(
                expected_size,
                result.into_iter().map(|f| f.num_rows()).sum::<usize>(),
                "{message}"
            );

            drop(refresh_handle);
        }

        test(
            vec![1, 2, 3],
            vec![],
            3,
            Some(TimeFormat::UnixSeconds),
            None,
            false,
            "should insert all data into empty accelerator",
        )
        .await;
        test(
            vec![1, 2, 3],
            vec![2, 3, 4, 5],
            4,
            Some(TimeFormat::UnixSeconds),
            None,
            false,
            "should not insert any stale data and keep original size",
        )
        .await;
        test(
            vec![],
            vec![1, 2, 3, 4],
            4,
            Some(TimeFormat::UnixSeconds),
            None,
            false,
            "should keep original data of accelerator when no new data is found",
        )
        .await;
        test(
            vec![5, 6],
            vec![1, 2, 3, 4],
            6,
            Some(TimeFormat::UnixSeconds),
            None,
            false,
            "should apply new data onto existing data",
        )
        .await;

        // Known limitation, doesn't dedup
        test(
            vec![4, 4],
            vec![1, 2, 3, 4],
            4,
            Some(TimeFormat::UnixSeconds),
            None,
            false,
            "should not apply same timestamp data",
        )
        .await;

        test(
            vec![4, 5, 6, 7, 8, 9, 10],
            vec![1, 2, 3, 9],
            10,
            Some(TimeFormat::UnixSeconds),
            Some(Duration::from_secs(10)),
            false,
            "should apply late arrival and new data onto existing data",
        )
        .await;

        test(
            vec![4, 5, 6, 7, 8, 9, 10],
            vec![1, 2, 3, 9],
            7, // 1, 2, 3, 7, 8, 9, 10
            Some(TimeFormat::UnixSeconds),
            Some(Duration::from_secs(3)),
            false,
            "should apply late arrival within the append overlap period and new data onto existing data",
        )
        .await;

        test(
            vec![4, 5, 6, 7, 8, 9, 10],
            vec![1, 2, 3, 9],
            10, // all the data
            Some(TimeFormat::UnixMillis),
            Some(Duration::from_secs(3)),
            false,
            "should fetch all data as 3 seconds is enough to cover all time span in source with millis",
        )
        .await;
        test(
            vec![4, 5, 6, 7, 8, 9, 10],
            vec![1, 2, 3, 9],
            16, // all the data
            Some(TimeFormat::UnixMillis),
            Some(Duration::from_secs(3)),
            true,
            "should fetch all data from all fetched record batches as 3 seconds is enough to cover all time span in source with millis",
        )
        .await;
    }

    #[test]
    fn test_validate_time_column_when_no_time_column() {
        let refresh = Refresh::new(RefreshMode::Full);
        let schema = Arc::new(Schema::empty());
        refresh
            .validate_time_format("dataset_name".to_string(), &schema)
            .expect("should validate successfully");
    }

    #[test]
    fn test_validate_time_column_when_time_column_not_found() {
        let refresh = Refresh::new(RefreshMode::Append).time_column("time".to_string());

        let schema = Arc::new(Schema::empty());
        assert!(matches!(
            refresh.validate_time_format("test_dataset".to_string(), &schema),
            Err(Error::NoTimeColumnFound { .. })
        ));
    }

    #[test]
    fn test_validate_time_column_when_iso8601_mismatch() {
        for format in [
            TimeFormat::UnixSeconds,
            TimeFormat::UnixMillis,
            TimeFormat::Timestamp,
            TimeFormat::Timestamptz,
            TimeFormat::Date,
        ] {
            let refresh = Refresh::new(RefreshMode::Full)
                .time_column("time".to_string())
                .time_format(format);
            let schema = Arc::new(Schema::new(vec![Field::new("time", DataType::Utf8, false)]));
            assert!(matches!(
                refresh.validate_time_format("test_dataset".to_string(), &schema),
                Err(Error::TimeFormatMismatch { .. })
            ));
        }
    }

    #[test]
    fn test_validate_time_column_when_unix_timestamp_mismatch() {
        for format in [
            TimeFormat::Timestamp,
            TimeFormat::Timestamptz,
            TimeFormat::ISO8601,
            TimeFormat::Date,
        ] {
            let refresh = Refresh::new(RefreshMode::Full)
                .time_column("time".to_string())
                .time_format(format);

            let schema = Arc::new(Schema::new(vec![Field::new(
                "time",
                DataType::Int64,
                false,
            )]));
            assert!(matches!(
                refresh.validate_time_format("test_dataset".to_string(), &schema),
                Err(Error::TimeFormatMismatch { .. })
            ));
        }
    }

    #[test]
    fn test_validate_time_column_when_timestamp_mismatch() {
        for format in [
            TimeFormat::UnixMillis,
            TimeFormat::UnixSeconds,
            TimeFormat::Timestamptz,
            TimeFormat::ISO8601,
            TimeFormat::Date,
        ] {
            let refresh = Refresh::new(RefreshMode::Full)
                .time_column("time".to_string())
                .time_format(format);

            let schema = Arc::new(Schema::new(vec![Field::new(
                "time",
                DataType::Timestamp(arrow::datatypes::TimeUnit::Second, None),
                false,
            )]));
            assert!(matches!(
                refresh.validate_time_format("test_dataset".to_string(), &schema),
                Err(Error::TimeFormatMismatch { .. })
            ));
        }
    }

    #[test]
    fn test_validate_time_column_when_timestamptz_mismatch() {
        for format in [
            TimeFormat::UnixMillis,
            TimeFormat::UnixSeconds,
            TimeFormat::Timestamp,
            TimeFormat::ISO8601,
            TimeFormat::Date,
        ] {
            let refresh = Refresh::new(RefreshMode::Full)
                .time_column("time".to_string())
                .time_format(format);

            let schema = Arc::new(Schema::new(vec![Field::new(
                "time",
                DataType::Timestamp(arrow::datatypes::TimeUnit::Second, Some("+00:00".into())),
                false,
            )]));
            assert!(matches!(
                refresh.validate_time_format("test_dataset".to_string(), &schema),
                Err(Error::TimeFormatMismatch { .. })
            ));
        }
    }

    #[test]
    fn test_validate_time_column_when_iso8601_match() {
        let refresh = Refresh::new(RefreshMode::Full)
            .time_column("time".to_string())
            .time_format(TimeFormat::ISO8601);

        let schema = Arc::new(Schema::new(vec![Field::new("time", DataType::Utf8, false)]));
        refresh
            .validate_time_format("dataset_name".to_string(), &schema)
            .expect("should validate successfully");
    }

    #[test]
    fn test_validate_time_column_when_unix_timestamp_match() {
        for format in [TimeFormat::UnixMillis, TimeFormat::UnixSeconds] {
            let refresh = Refresh::new(RefreshMode::Full)
                .time_column("time".to_string())
                .time_format(format);

            let schema = Arc::new(Schema::new(vec![Field::new(
                "time",
                DataType::Int64,
                false,
            )]));
            refresh
                .validate_time_format("dataset_name".to_string(), &schema)
                .expect("should validate successfully");
        }
    }

    #[test]
    fn test_validate_time_column_when_timestamp_match() {
        let refresh = Refresh::new(RefreshMode::Full)
            .time_column("time".to_string())
            .time_format(TimeFormat::Timestamp);

        let schema = Arc::new(Schema::new(vec![Field::new(
            "time",
            DataType::Timestamp(arrow::datatypes::TimeUnit::Second, None),
            false,
        )]));
        refresh
            .validate_time_format("dataset_name".to_string(), &schema)
            .expect("should validate successfully");
    }

    #[test]
    fn test_validate_time_column_when_timestamptz_match() {
        let refresh = Refresh::new(RefreshMode::Full)
            .time_column("time".to_string())
            .time_format(TimeFormat::Timestamptz);

        let schema = Arc::new(Schema::new(vec![Field::new(
            "time",
            DataType::Timestamp(arrow::datatypes::TimeUnit::Second, Some("+00:00".into())),
            false,
        )]));
        refresh
            .validate_time_format("dataset_name".to_string(), &schema)
            .expect("should validate successfully");
    }

    #[test]
    fn test_validate_time_column_when_date_match() {
        let refresh = Refresh::new(RefreshMode::Full)
            .time_column("time".to_string())
            .time_format(TimeFormat::Date);

        let schema = Arc::new(Schema::new(vec![Field::new(
            "time",
            DataType::Date32,
            false,
        )]));
        refresh
            .validate_time_format("dataset_name".to_string(), &schema)
            .expect("should validate successfully");
    }

    #[test]
    fn test_validate_time_column_when_date_mismatch() {
        for format in [
            TimeFormat::UnixMillis,
            TimeFormat::UnixSeconds,
            TimeFormat::Timestamp,
            TimeFormat::Timestamptz,
            TimeFormat::ISO8601,
        ] {
            let refresh = Refresh::new(RefreshMode::Full)
                .time_column("time".to_string())
                .time_format(format);

            let schema = Arc::new(Schema::new(vec![Field::new(
                "time",
                DataType::Date32,
                false,
            )]));
            assert!(matches!(
                refresh.validate_time_format("test_dataset".to_string(), &schema),
                Err(Error::TimeFormatMismatch { .. })
            ));
        }
    }

    #[tokio::test]
    async fn test_startup_next_refresh() {
        struct TestCase {
            description: &'static str,
            refresh_mode: RefreshMode,
            refresh_on_startup: RefreshOnStartup,
            checkpoint: Option<Arc<dyn DatasetCheckpointer>>,
            check_interval: Option<Duration>,
            assert_fn: Box<dyn Fn(NextRefresh) -> bool>,
        }

        let now = SystemTime::now();
        let checkpoint = MockCheckpointer::new_arc(true, Some(now));
        let non_existing_checkpoint = MockCheckpointer::new_arc(false, None);

        let test_cases = vec![
            TestCase {
                description: "No checkpoint, Full mode should refresh immediately",
                refresh_mode: RefreshMode::Full,
                refresh_on_startup: RefreshOnStartup::Auto,
                checkpoint: None,
                check_interval: None,
                assert_fn: Box::new(
                    |result| matches!(result, NextRefresh::WaitFor(duration) if duration.is_zero()),
                ),
            },
            TestCase {
                description: "No checkpoint, Append mode should refresh immediately",
                refresh_mode: RefreshMode::Append,
                refresh_on_startup: RefreshOnStartup::Auto,
                checkpoint: None,
                check_interval: None,
                assert_fn: Box::new(
                    |result| matches!(result, NextRefresh::WaitFor(duration) if duration.is_zero()),
                ),
            },
            TestCase {
                description: "No checkpoint, Changes mode should refresh immediately",
                refresh_mode: RefreshMode::Changes,
                refresh_on_startup: RefreshOnStartup::Auto,
                checkpoint: None,
                check_interval: None,
                assert_fn: Box::new(
                    |result| matches!(result, NextRefresh::WaitFor(duration) if duration.is_zero()),
                ),
            },
            TestCase {
                description: "No checkpoint, Disabled mode should be disabled",
                refresh_mode: RefreshMode::Disabled,
                refresh_on_startup: RefreshOnStartup::Auto,
                checkpoint: None,
                check_interval: None,
                assert_fn: Box::new(|result| matches!(result, NextRefresh::Disabled)),
            },
            TestCase {
                description: "Checkpoint exists, Append mode should refresh immediately",
                refresh_mode: RefreshMode::Append,
                refresh_on_startup: RefreshOnStartup::Auto,
                checkpoint: Some(Arc::clone(&checkpoint)),
                check_interval: None,
                assert_fn: Box::new(
                    |result| matches!(result, NextRefresh::WaitFor(duration) if duration.is_zero()),
                ),
            },
            TestCase {
                description: "Checkpoint exists, Changes mode should refresh immediately",
                refresh_mode: RefreshMode::Changes,
                refresh_on_startup: RefreshOnStartup::Auto,
                checkpoint: Some(Arc::clone(&checkpoint)),
                check_interval: None,
                assert_fn: Box::new(
                    |result| matches!(result, NextRefresh::WaitFor(duration) if duration.is_zero()),
                ),
            },
            TestCase {
                description: "Checkpoint exists, Disabled mode should be disabled",
                refresh_mode: RefreshMode::Disabled,
                refresh_on_startup: RefreshOnStartup::Auto,
                checkpoint: Some(Arc::clone(&checkpoint)),
                check_interval: None,
                assert_fn: Box::new(|result| matches!(result, NextRefresh::Disabled)),
            },
            TestCase {
                description: "Checkpoint exists, Full mode with no check interval should be disabled",
                refresh_mode: RefreshMode::Full,
                refresh_on_startup: RefreshOnStartup::Auto,
                checkpoint: Some(Arc::clone(&checkpoint)),
                check_interval: None,
                assert_fn: Box::new(|result| matches!(result, NextRefresh::Disabled)),
            },
            TestCase {
                description: "Checkpoint exists, Full mode with check interval should wait appropriate time",
                refresh_mode: RefreshMode::Full,
                refresh_on_startup: RefreshOnStartup::Auto,
                checkpoint: Some(Arc::clone(&checkpoint)),
                check_interval: Some(Duration::from_secs(60)),
                assert_fn: Box::new(|result| {
                    if let NextRefresh::WaitFor(duration) = result {
                        duration <= Duration::from_secs(60) && duration > Duration::ZERO
                    } else {
                        false
                    }
                }),
            },
            TestCase {
                description: "Non-existent checkpoint, Full mode should refresh immediately",
                refresh_mode: RefreshMode::Full,
                refresh_on_startup: RefreshOnStartup::Auto,
                checkpoint: Some(non_existing_checkpoint),
                check_interval: None,
                assert_fn: Box::new(
                    |result| matches!(result, NextRefresh::WaitFor(duration) if duration.is_zero()),
                ),
            },
            TestCase {
                description: "Checkpoint exists, Full mode with RefreshOnStartup::Always should refresh immediately",
                refresh_mode: RefreshMode::Full,
                refresh_on_startup: RefreshOnStartup::Always,
                checkpoint: Some(Arc::clone(&checkpoint)),
                check_interval: None,
                assert_fn: Box::new(
                    |result| matches!(result, NextRefresh::WaitFor(duration) if duration.is_zero()),
                ),
            },
            TestCase {
                description: "No checkpoint, Full mode with RefreshOnStartup::Always should refresh immediately",
                refresh_mode: RefreshMode::Full,
                refresh_on_startup: RefreshOnStartup::Always,
                checkpoint: None,
                check_interval: None,
                assert_fn: Box::new(
                    |result| matches!(result, NextRefresh::WaitFor(duration) if duration.is_zero()),
                ),
            },
            TestCase {
                description: "Checkpoint exists, Full mode with check interval and RefreshOnStartup::Always should refresh immediately",
                refresh_mode: RefreshMode::Full,
                refresh_on_startup: RefreshOnStartup::Always,
                checkpoint: Some(checkpoint),
                check_interval: Some(Duration::from_secs(60)),
                assert_fn: Box::new(
                    |result| matches!(result, NextRefresh::WaitFor(duration) if duration.is_zero()),
                ),
            },
        ];

        for case in test_cases {
            let mut refresh = Refresh::new(case.refresh_mode);
            if let Some(check_interval) = case.check_interval {
                refresh = refresh.check_interval(check_interval);
            }

            let result = refresh
                .startup_next_refresh(case.refresh_on_startup, case.checkpoint)
                .await;

            assert!(
                (case.assert_fn)(result),
                "Test case failed: {}",
                case.description
            );
        }
    }

    #[tokio::test]
    async fn test_startup_next_refresh_wait_time() {
        struct TestCase {
            description: &'static str,
            last_checkpoint_time: SystemTime,
            check_interval: Duration,
            expected_wait_time: Duration,
        }

        let now = SystemTime::now();
        let test_cases = vec![
            TestCase {
                description: "Checkpoint just happened, should wait full interval",
                last_checkpoint_time: now,
                check_interval: Duration::from_secs(60),
                expected_wait_time: Duration::from_secs(60),
            },
            TestCase {
                description: "Checkpoint happened 30 seconds ago, should wait 30 seconds",
                last_checkpoint_time: now - Duration::from_secs(30),
                check_interval: Duration::from_secs(60),
                expected_wait_time: Duration::from_secs(30),
            },
            TestCase {
                description: "Checkpoint happened 45 seconds ago, should wait 15 seconds",
                last_checkpoint_time: now - Duration::from_secs(45),
                check_interval: Duration::from_secs(60),
                expected_wait_time: Duration::from_secs(15),
            },
            TestCase {
                description: "Checkpoint happened 59 seconds ago, should wait 1 second",
                last_checkpoint_time: now - Duration::from_secs(59),
                check_interval: Duration::from_secs(60),
                expected_wait_time: Duration::from_secs(1),
            },
            TestCase {
                description: "Checkpoint happened more than interval ago, should refresh immediately",
                last_checkpoint_time: now - Duration::from_secs(61),
                check_interval: Duration::from_secs(60),
                expected_wait_time: Duration::ZERO,
            },
        ];

        for case in test_cases {
            let checkpoint = MockCheckpointer::new_arc(true, Some(case.last_checkpoint_time));
            let refresh = Refresh::new(RefreshMode::Full).check_interval(case.check_interval);

            let result = refresh
                .startup_next_refresh(RefreshOnStartup::Auto, Some(Arc::clone(&checkpoint)))
                .await;

            match result {
                NextRefresh::WaitFor(duration) => {
                    // Allow for a small margin of error due to test execution time
                    let margin = Duration::from_millis(100);
                    let min_expected = case.expected_wait_time.saturating_sub(margin);
                    let max_expected = case.expected_wait_time + margin;
                    assert!(
                        duration >= min_expected && duration <= max_expected,
                        "Test case failed: {}. Expected wait time between {:?} and {:?}, got {:?}",
                        case.description,
                        min_expected,
                        max_expected,
                        duration
                    );
                }
                NextRefresh::Disabled => {
                    assert!(
                        case.expected_wait_time.is_zero(),
                        "Test case failed: {}. Expected wait time of {:?}, got Disabled",
                        case.description,
                        case.expected_wait_time
                    );
                }
            }
        }
    }
}
