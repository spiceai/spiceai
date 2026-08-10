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

//! What a federated table needs from the source behind it.
//!
//! A [`FederatedTable`](crate::federated::FederatedTable) resolves a
//! dataset against one source and needs exactly three things of it: the refresh
//! mode the source prefers, the dataset's `TableProvider`, and the checkpointer
//! that says whether a stored acceleration schema still matches. It has no
//! interest in the rest of a connector — changes streams, object stores,
//! metadata providers, accelerator hooks — so it asks for the narrow thing it
//! uses.
//!
//! The dataset is bound when the source is built, rather than passed per call,
//! and that is what makes the inversion work. `runtime`'s `Dataset` holds an
//! `Arc<Runtime>`, so a signature naming it would drag the orchestrator back in;
//! the rest of this crate needs only the config fields and so speaks
//! `DatasetSpec`. Binding the full `Dataset` on the runtime side of the seam
//! keeps it off the seam itself.
//!
//! This is a dependency inversion, not a new abstraction: every method mirrors
//! the `DataConnector` method it stands for, so `runtime` satisfies it by
//! forwarding. What it buys is that the accelerated-table crate compiles without
//! the `DataConnector` trait, and therefore without `runtime`.
//!
//! When `DataConnector` itself moves below `runtime` (plan step 5.4 — it needs
//! `&Dataset` retyped to `&DatasetSpec` across 8 of its 16 methods and 45
//! implementors, and its two accelerated-table hooks split into a runtime-side
//! extension trait), this trait either collapses into a plain `DataConnector`
//! bound or stays on as the narrower interface at the point of use. Either way
//! the runtime-side adapter goes away, and nothing here has to change first.

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::datasource::TableProvider;
use runtime_acceleration::dataset_checkpoint::DatasetCheckpointer;
use runtime_component::dataset::acceleration::RefreshMode;

/// The source a federated table reads from, narrowed to what it uses.
#[async_trait]
pub trait RefreshSource: std::fmt::Debug + Send + Sync {
    /// The refresh mode to use, given what the dataset asked for. A source may
    /// override the request — a CDC source can insist on `Changes`, for example.
    fn resolve_refresh_mode(&self, requested: Option<RefreshMode>) -> RefreshMode;

    /// The provider for the bound dataset.
    ///
    /// # Errors
    ///
    /// Returns the source's own error, boxed, if the provider cannot be built:
    /// the source is unreachable, misconfigured, or the table is missing.
    async fn read_provider(&self) -> Result<Arc<dyn TableProvider>, RefreshSourceError>;

    /// The checkpointer holding the bound dataset's accelerated schema, if it
    /// has one.
    ///
    /// `None` means there is no checkpoint to compare against — the dataset is
    /// not file-accelerated, or the checkpoint has not been created yet — so the
    /// caller has nothing to detect a schema change from.
    async fn checkpointer(&self) -> Option<Arc<dyn DatasetCheckpointer>>;
}

/// Failure building a source's provider. Boxed rather than typed, so this crate
/// does not have to name every connector's error.
pub type RefreshSourceError = Box<dyn std::error::Error + Send + Sync>;
