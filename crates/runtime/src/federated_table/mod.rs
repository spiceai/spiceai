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

//! A representation of a federated table in Spice.
//!
//! A federated table is mainly just a wrapper around an `Arc<dyn TableProvider>`. However,
//! in the event that we cannot connect to the table provider, we can create a task
//! to keep trying to connect to the table provider until it is available.
//!
//! Combined with the ability to retrieve the schema of the table from an existing acceleration,
//! this allows us to register accelerated tables and serve data from them while waiting
//! for the table provider to become available.
//!
//! Unlike the `AcceleratedTable` struct, this struct does not implement the `TableProvider` trait itself.
//! It only provides a way to get the underlying table provider and schema.

use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use datafusion::catalog::TableProvider;
use tokio::sync::{oneshot, Mutex};
use util::{fibonacci_backoff::FibonacciBackoffBuilder, retry, RetryError};

use crate::{
    component::dataset::Dataset, dataaccelerator::spice_sys::dataset_checkpoint::DatasetCheckpoint,
    dataconnector::DataConnector,
};

#[derive(Debug)]
pub enum FederatedTable {
    // To optimize the common case where the table provider is available immediately.
    Immediate(Arc<dyn TableProvider>),

    // If the table provider is not available immediately, we wait for it to become
    // available and store it here.
    Deferred(DeferredTableProvider),
}

#[derive(Debug)]
enum DeferredState {
    Waiting(oneshot::Receiver<Arc<dyn TableProvider>>),
    InProgress,
    Done(Arc<dyn TableProvider>),
}

#[derive(Debug)]
pub struct DeferredTableProvider {
    state: Mutex<DeferredState>,
    schema: SchemaRef,
}

impl DeferredTableProvider {
    pub fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

impl FederatedTable {
    pub fn new(table_provider: Arc<dyn TableProvider>) -> Self {
        Self::Immediate(table_provider)
    }

    /// If the table provider is not available immediately and this is an accelerated table with a previous acceleration checkpoint,
    /// we can create a deferred task to keep trying to connect to the table provider until it is available.
    ///
    /// Returns `None` if the dataset isn't a valid file-accelerated dataset.
    pub async fn new_deferred(
        dataset: Arc<Dataset>,
        data_connector: Arc<dyn DataConnector>,
    ) -> Option<Self> {
        if !dataset.is_file_accelerated() {
            return None;
        }

        let checkpoint = DatasetCheckpoint::try_new(&dataset).await.ok()?;
        let federated_schema = checkpoint.get_schema().await.ok()??;
        let dataset_name = dataset.name.clone();

        let (tx, rx) = oneshot::channel();
        tokio::spawn(async move {
            let retry_strategy = FibonacciBackoffBuilder::new().max_retries(None).build();

            let data_connector = Arc::clone(&data_connector);
            let table_provider_result = retry(retry_strategy, || async {
                match data_connector.read_provider(&dataset).await {
                    Ok(table_provider) => Ok(table_provider),
                    Err(e) => Err(RetryError::transient(e)),
                }
            })
            .await;

            match table_provider_result {
                Ok(table_provider) => {
                    if tx.send(table_provider).is_err() {
                        tracing::error!(
                            "Failed to send deferred table provider for dataset '{}': Channel closed.",
                            dataset.name,
                        );
                    }
                    tracing::info!("Connection to source re-established for {dataset_name}.",);
                }
                Err(e) => {
                    tracing::error!(
                        "Failed to connect to table provider for dataset '{}': {e}",
                        dataset.name,
                    );
                }
            }
        });
        Some(Self::Deferred(DeferredTableProvider {
            state: Mutex::new(DeferredState::Waiting(rx)),
            schema: federated_schema,
        }))
    }

    pub async fn table_provider(&self) -> Arc<dyn TableProvider> {
        let deferred_table_provider = match self {
            Self::Immediate(table_provider) => return Arc::clone(table_provider),
            Self::Deferred(deferred_table_provider) => deferred_table_provider,
        };

        // If the table provider is not available immediately, see if we already have it from the deferred task.
        let mut deferred_state_guard = deferred_table_provider.state.lock().await;

        // If the table provider is available now, return it.
        if let DeferredState::Done(table_provider) = &*deferred_state_guard {
            return Arc::clone(table_provider);
        }

        // We need to own the deferred state to be able to wait on the receiver. Temporarily replace it with InProgress.
        let deferred_state_owned =
            std::mem::replace(&mut *deferred_state_guard, DeferredState::InProgress);

        // The only valid state at this point is Waiting, we've already checked Done above and we always set the state back to Done before exiting.
        match deferred_state_owned {
            DeferredState::Waiting(rx) => {
                // If the table provider is not available yet, wait for it to become available.
                let Ok(table_provider) = rx.await else {
                    unreachable!(
                        "deferred task should not be dropped before sending the table provider"
                    );
                };
                *deferred_state_guard = DeferredState::Done(Arc::clone(&table_provider));
                table_provider
            }
            DeferredState::InProgress | DeferredState::Done(_) => {
                unreachable!("deferred state should only be Waiting at this point");
            }
        }
    }

    pub fn schema(&self) -> SchemaRef {
        match self {
            Self::Immediate(table_provider) => table_provider.schema(),
            Self::Deferred(deferred_table_provider) => Arc::clone(&deferred_table_provider.schema),
        }
    }
}
