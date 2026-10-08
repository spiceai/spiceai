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

//! Presents a [`DataConnector`] to the accelerated-table crate as a
//! [`RefreshSource`].
//!
//! The accelerated-table crate cannot name [`DataConnector`] (the trait lives
//! here) or [`Dataset`] (it holds an `Arc<Runtime>`), so it declares the narrow
//! interface it actually uses and this type satisfies it. Binding the dataset
//! here is what keeps it off that interface — see
//! `runtime_table::refresh_source` for why, and for how this adapter
//! retires once `DataConnector` itself moves down.

use crate::dataconnector::parameters::RuntimeConnectorContext;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::datasource::TableProvider;
use runtime_acceleration::dataset_checkpoint::DatasetCheckpointer;
use runtime_component::dataset::acceleration::RefreshMode;
use runtime_table::refresh_source::{RefreshSource, RefreshSourceError};

use crate::component::dataset::Dataset;
use crate::dataaccelerator::spice_sys::dataset_checkpointer;
use crate::dataconnector::DataConnector;
use runtime_acceleration::sidecar::OpenOption;
use runtime_acceleration::snapshot::SnapshotBehavior;

/// A [`DataConnector`] bound to the dataset it resolves, as a [`RefreshSource`].
#[derive(Debug)]
pub struct ConnectorRefreshSource {
    connector: Arc<dyn DataConnector>,
    dataset: Arc<Dataset>,
}

impl ConnectorRefreshSource {
    /// Binds `connector` to `dataset`.
    ///
    /// Returns the trait object directly: the accelerated table only ever holds
    /// this as an `Arc<dyn RefreshSource>`, and `new_arc` matches the convention
    /// `DataConnectorFactory` already uses.
    #[must_use]
    pub fn new_arc(
        connector: Arc<dyn DataConnector>,
        dataset: Arc<Dataset>,
    ) -> Arc<dyn RefreshSource> {
        Arc::new(Self { connector, dataset })
    }
}

#[async_trait]
impl RefreshSource for ConnectorRefreshSource {
    fn resolve_refresh_mode(&self, requested: Option<RefreshMode>) -> RefreshMode {
        self.connector.resolve_refresh_mode(requested)
    }

    async fn read_provider(&self) -> Result<Arc<dyn TableProvider>, RefreshSourceError> {
        self.connector
            .read_provider(
                &RuntimeConnectorContext::for_dataset(&self.dataset),
                &self.dataset,
            )
            .await
            .map_err(|source| Box::new(source) as RefreshSourceError)
    }

    async fn checkpointer(&self) -> Option<Arc<dyn DatasetCheckpointer>> {
        if !self.dataset.is_file_accelerated() {
            return None;
        }

        let registry = self.dataset.runtime.accelerator_engine_registry();
        dataset_checkpointer(
            self.dataset.as_ref(),
            registry,
            OpenOption::OpenExisting,
            SnapshotBehavior::Disabled,
        )
        .await
        .ok()
    }
}

/// How often a dataset served from its existing acceleration reports that its
/// source still cannot be reached.
const SOURCE_RETRY_REPORT_INTERVAL: std::time::Duration = std::time::Duration::from_mins(5);

struct SourceFailureReport {
    reported_at: std::time::Instant,
    configuration_error: bool,
}

/// A [`RefreshSource`] for a dataset served from its existing acceleration while
/// its source is unavailable, which reports the failed attempts to reach the
/// source: the dataset's status is `Error`, with a message saying it is still
/// served, until an attempt succeeds, and each failure is logged at most every
/// [`SOURCE_RETRY_REPORT_INTERVAL`], except when a transient failure escalates to
/// a configuration error. Reports are errors for a configuration error
/// (rejected credentials, TLS), which no retry clears, and a warning otherwise.
/// Queries keep being served from the acceleration either way.
///
/// The status is set on every failure, not only when a report is due, so a status
/// written over it (such as registration's) is restored by the next attempt.
pub(crate) struct ReportingRefreshSource {
    inner: Arc<dyn RefreshSource>,
    dataset: Arc<Dataset>,
    status: Arc<crate::status::RuntimeStatus>,
    last_report: parking_lot::Mutex<Option<SourceFailureReport>>,
    /// Whether the dataset's status is an `Error` this source set, to clear once the
    /// source is reached.
    status_is_error: std::sync::atomic::AtomicBool,
}

impl ReportingRefreshSource {
    /// Wraps `inner`. `already_reported` retains the classification of a failure
    /// just logged at registration, so the first retry does not repeat it unless
    /// a transient failure escalates to a configuration error.
    pub(crate) fn new_arc(
        inner: Arc<dyn RefreshSource>,
        dataset: Arc<Dataset>,
        status: Arc<crate::status::RuntimeStatus>,
        already_reported: Option<bool>,
    ) -> Arc<dyn RefreshSource> {
        Arc::new(Self {
            inner,
            dataset,
            status,
            last_report: parking_lot::Mutex::new(already_reported.map(|configuration_error| {
                SourceFailureReport {
                    reported_at: std::time::Instant::now(),
                    configuration_error,
                }
            })),
            status_is_error: std::sync::atomic::AtomicBool::new(false),
        })
    }

    fn report(&self, err: &RefreshSourceError) {
        let configuration_error = err
            .downcast_ref::<crate::dataconnector::DataConnectorError>()
            .is_some_and(|err| !err.is_retriable());
        let message = if configuration_error {
            source_configuration_error(&self.dataset.name, &err.to_string())
        } else {
            super::reconnecting::unreachable_source_warning(&self.dataset.name, &err.to_string())
        };
        self.status.update_dataset(
            &self.dataset.name,
            crate::status::ComponentStatus::error_with_message(message.clone()),
        );
        self.status_is_error
            .store(true, std::sync::atomic::Ordering::Release);
        if self.due(configuration_error) {
            if configuration_error {
                tracing::error!("{message}");
            } else {
                tracing::warn!("{message}");
            }
        }
    }

    /// Clears the `Error` this source set once the source has been reached.
    fn reached(&self) {
        if self
            .status_is_error
            .swap(false, std::sync::atomic::Ordering::AcqRel)
        {
            self.status
                .update_dataset(&self.dataset.name, crate::status::ComponentStatus::Ready);
        }
    }

    /// Whether a report is due, recording it if so.
    fn due(&self, configuration_error: bool) -> bool {
        let mut last_report = self.last_report.lock();
        if last_report.as_ref().is_some_and(|last| {
            last.reported_at.elapsed() < SOURCE_RETRY_REPORT_INTERVAL
                && (!configuration_error || last.configuration_error)
        }) {
            return false;
        }
        *last_report = Some(SourceFailureReport {
            reported_at: std::time::Instant::now(),
            configuration_error,
        });
        true
    }
}

impl std::fmt::Debug for ReportingRefreshSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReportingRefreshSource")
            .field("dataset", &self.dataset.name)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl RefreshSource for ReportingRefreshSource {
    fn resolve_refresh_mode(&self, requested: Option<RefreshMode>) -> RefreshMode {
        self.inner.resolve_refresh_mode(requested)
    }

    async fn read_provider(&self) -> Result<Arc<dyn TableProvider>, RefreshSourceError> {
        let result = self.inner.read_provider().await;
        match &result {
            Ok(_) => self.reached(),
            Err(err) => self.report(err),
        }
        result
    }

    async fn checkpointer(&self) -> Option<Arc<dyn DatasetCheckpointer>> {
        self.inner.checkpointer().await
    }
}

/// The error logged, and set as the dataset's status, when a dataset served from
/// its existing acceleration cannot reach its source because of its configuration.
pub(crate) fn source_configuration_error(
    dataset: &datafusion::common::TableReference,
    cause: &str,
) -> String {
    format!(
        "Dataset '{dataset}' cannot connect to its source because of its configuration, so it is served from its existing acceleration and will not refresh until the configuration is fixed. {cause}"
    )
}

#[cfg(test)]
mod tests {
    use datafusion::common::TableReference;

    use super::source_configuration_error;

    #[test]
    fn the_configuration_error_names_the_dataset_what_is_served_and_the_cause() {
        assert_eq!(
            source_configuration_error(
                &TableReference::bare("orders"),
                "Invalid username or password for the dataset orders (postgres)."
            ),
            "Dataset 'orders' cannot connect to its source because of its configuration, so it is served from its existing acceleration and will not refresh until the configuration is fixed. Invalid username or password for the dataset orders (postgres)."
        );
    }
}
