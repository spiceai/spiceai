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
