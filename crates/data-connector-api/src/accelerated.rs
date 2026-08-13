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

//! What a connector needs of the accelerated table built for its dataset.
//!
//! A connector gets two chances to touch a dataset's acceleration: once before
//! the accelerated table is built, to wrap the provider the accelerator will
//! write to ([`AcceleratorSetup`]), and once after it is registered, to attach
//! background work to it ([`RegisteredAcceleratedTable`]).
//!
//! Both hooks used to take the accelerated-table types themselves — the builder
//! and the table. Those live beside the runtime that orchestrates them, so a
//! connector naming either would depend on the orchestrator for the sake of two
//! getters and a `Vec::push`. Each trait here is the slice of one of those types
//! that connectors actually reach for, so the contract names only Arrow,
//! `DataFusion` and Tokio types. The runtime side satisfies them by forwarding.

use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::datasource::TableProvider;
use snafu::Snafu;
use tokio::task::JoinHandle;

/// The accelerator being prepared for a dataset, before its accelerated table
/// is built.
///
/// A connector wraps the provider here — rather than after registration — when
/// the wrapper has to be visible to the refresh pipeline, which is handed the
/// same provider when the table is built.
///
/// `Send` but not `Sync`: the hook takes `&mut dyn AcceleratorSetup`, which is
/// `Send` on `Send` alone, and the runtime-side implementor carries a changes
/// stream that is not `Sync`.
pub trait AcceleratorSetup: Send {
    /// The provider the accelerator will read and write.
    fn accelerator(&self) -> Arc<dyn TableProvider>;

    /// Replace the accelerator's provider, usually with a wrapper around the
    /// provider [`accelerator`](Self::accelerator) returned.
    fn set_accelerator(&mut self, accelerator: Arc<dyn TableProvider>);
}

/// A dataset's accelerated table, once it is registered and running.
///
/// `Send` but not `Sync`, for the same reason as [`AcceleratorSetup`].
pub trait RegisteredAcceleratedTable: Send {
    /// A handle that asks this table to refresh, or `None` when it has no
    /// on-demand refresh to ask for — it is not refreshed on a schedule, or it
    /// is synchronized with another table that drives its data.
    fn refresh_requester(&self) -> Option<Arc<dyn RefreshRequester>>;

    /// Tie `task` to this table's lifetime. The task is aborted when the table
    /// is dropped, so a connector can spawn a watcher without having to track
    /// the table's shutdown itself.
    fn attach_task(&mut self, task: JoinHandle<()>);
}

/// Asks an accelerated table to refresh now.
#[async_trait]
pub trait RefreshRequester: Debug + Send + Sync {
    /// Request a refresh, returning once the request is accepted — not once the
    /// refresh completes.
    ///
    /// # Errors
    ///
    /// Returns [`RefreshRequestError`] if the table is no longer listening,
    /// which means it has been dropped or replaced.
    async fn request_refresh(&self) -> Result<(), RefreshRequestError>;
}

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum RefreshRequestError {
    #[snafu(display(
        "The accelerated table is no longer accepting refresh requests. It has been dropped or replaced."
    ))]
    TableGone,
}
