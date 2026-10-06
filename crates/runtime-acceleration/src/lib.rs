/*
Copyright 2025 The Spice.ai OSS Authors
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
use snafu::Snafu;
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use util::fibonacci_backoff::FibonacciBackoffBuilder;

pub mod acceleration;
pub mod acceleration_source;
pub mod change_sink;
pub mod dataset_checkpoint;
mod engine;
pub mod layout;
pub mod memory_budget;
pub mod schema_change;
pub mod sidecar;
pub mod snapshot;
// Test-only; behind a feature so it never reaches a shipped build.
#[cfg(feature = "test-support")]
pub mod testing;

pub use acceleration::Acceleration;
pub use acceleration::ParseError as AccelerationParseError;
pub use acceleration_source::AccelerationSource;
pub use engine::Engine;
pub use schema_change::OnSchemaChange;
pub use sidecar::{AcceleratorSidecar, OpenOption};
pub use snapshot::{SnapshotDownloadInfo, SnapshotPoll};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display(
        "Unknown acceleration engine '{name}'. Valid engines are: arrow, duckdb, sqlite, turso, postgres/postgresql, cayenne/vortex. Docs: https://spiceai.org/docs/components/data-accelerators"
    ))]
    AcceleratorEngineNotAvailable { name: String },
}

/// Indicates whether a data accelerator was bootstrapped (initialized from existing data)
/// during initialization, and carries any metadata from the snapshot.
#[derive(Debug, Clone)]
pub enum BootstrapStatus {
    /// A snapshot reader whose first download belongs to its cancellable load task.
    Pending {
        manager: Arc<snapshot::SnapshotManager>,
        subscription: Option<snapshot::notifications::Subscription>,
        poll_interval: Duration,
    },
    Bootstrapped {
        info: SnapshotDownloadInfo,
        subscription: Option<snapshot::notifications::Subscription>,
    },
    None,
}

// A subscription is runtime ownership, not part of the downloaded snapshot's
// identity. Cloning status preserves it through dataset initialization retries.
impl PartialEq for BootstrapStatus {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::None, Self::None) => true,
            (
                Self::Pending {
                    manager: left,
                    poll_interval: left_interval,
                    ..
                },
                Self::Pending {
                    manager: right,
                    poll_interval: right_interval,
                    ..
                },
            ) => Arc::ptr_eq(left, right) && left_interval == right_interval,
            (Self::Bootstrapped { info: left, .. }, Self::Bootstrapped { info: right, .. }) => {
                left == right
            }
            _ => false,
        }
    }
}

impl Eq for BootstrapStatus {}

impl BootstrapStatus {
    /// Complete a reader's bootstrap before constructing its table. The caller
    /// must run this inside its dataset load task's shutdown cancellation scope,
    /// without holding the application lock or a dataset load permit.
    pub async fn complete(self) -> Self {
        let mut status = self;
        let mut backoff = FibonacciBackoffBuilder::new().max_retries(None).build();
        loop {
            status = status.restore_once().await;
            let Self::Pending {
                subscription,
                poll_interval,
                ..
            } = &mut status
            else {
                return status;
            };
            // A zero refresh interval must still back off when there is no data,
            // as the regular refresh task does after a failed first refresh.
            let delay = if poll_interval.is_zero() {
                backoff.next_duration().unwrap_or(Duration::from_mins(5))
            } else {
                *poll_interval
            };
            if let Some(notifications) = subscription.as_mut() {
                tokio::select! {
                    announced = notifications.next_snapshot() => {
                        if announced.is_none() {
                            *subscription = None;
                            tokio::time::sleep(delay).await;
                        }
                    }
                    () = tokio::time::sleep(delay) => {}
                }
            } else {
                tokio::time::sleep(delay).await;
            }
        }
    }

    /// Makes one attempt to restore a pending bootstrap: bootstrapped when a snapshot
    /// was restored, and otherwise still pending, for [`Self::complete`] to keep waiting.
    pub async fn restore_once(self) -> Self {
        let Self::Pending {
            manager,
            subscription,
            poll_interval,
        } = self
        else {
            return self;
        };
        // Times the download that succeeds, not the wait for a snapshot to exist.
        let start = Instant::now();
        match manager.download_latest_snapshot().await {
            Ok(Some(info)) => {
                snapshot::metrics::record_bootstrap_metrics(
                    manager.dataset_name(),
                    start.elapsed().as_secs_f64() * 1000.0,
                    info.bytes_downloaded,
                    &info.checksum,
                );
                return Self::bootstrapped(info, subscription);
            }
            Ok(None) => {
                tracing::warn!(
                    "Snapshot acceleration for dataset '{}' is waiting for its first snapshot. Ensure the snapshot location '{}' is correct and that the writer has published a snapshot. See: https://spiceai.org/docs/features/data-acceleration/snapshots",
                    manager.dataset_name(),
                    manager.snapshot_location()
                );
            }
            Err(error) => {
                tracing::warn!(
                    "Failed to restore snapshot acceleration for dataset '{}', so snapshot data is unavailable and will be retried. Cause: {error}",
                    manager.dataset_name()
                );
            }
        }
        Self::Pending {
            manager,
            subscription,
            poll_interval,
        }
    }

    #[must_use]
    pub const fn bootstrapped(
        info: SnapshotDownloadInfo,
        subscription: Option<snapshot::notifications::Subscription>,
    ) -> Self {
        Self::Bootstrapped { info, subscription }
    }

    #[must_use]
    pub const fn none() -> Self {
        Self::None
    }

    #[must_use]
    pub fn is_bootstrapped(&self) -> bool {
        matches!(self, Self::Bootstrapped { .. })
    }

    #[must_use]
    pub const fn last_updated_at(&self) -> Option<i64> {
        match self {
            Self::None | Self::Pending { .. } => None,
            Self::Bootstrapped { info, .. } => info.last_updated_at,
        }
    }

    /// The `snapshot_id` of the snapshot that was loaded at bootstrap, if any.
    /// `None` when no bootstrap occurred (no snapshot, or snapshots disabled).
    #[must_use]
    pub const fn loaded_snapshot_id(&self) -> Option<u64> {
        match self {
            Self::None | Self::Pending { .. } => None,
            Self::Bootstrapped { info, .. } => Some(info.snapshot_id),
        }
    }

    /// Hand the bootstrap subscription to the table's refresh task without
    /// stopping its consumer or losing announcements received during download.
    pub fn take_snapshot_subscription(&mut self) -> Option<snapshot::notifications::Subscription> {
        match self {
            Self::Bootstrapped { subscription, .. } => subscription.take(),
            Self::None | Self::Pending { .. } => None,
        }
    }
}
pub mod dataupdate;
