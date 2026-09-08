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

//! The federated table a connector's change stream reads from.

use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::datasource::TableProvider;

/// A dataset's federated table, narrowed to the provider behind it.
///
/// A connector opening a changes or append stream needs one thing of the table
/// it is streaming into: the provider, so it can recover its own concrete
/// provider type and start reading. The table itself may not be resolved yet —
/// a deferred dataset registers before its source is contacted — so resolving it
/// is asynchronous.
///
/// This is a dependency inversion in the same spirit as `runtime-table`'s
/// `RefreshSource`, pointing the other way: the federated table lives beside the
/// runtime that builds it, and naming it in the connector contract would pull the
/// orchestrator into every CDC connector for a single getter. The runtime side
/// satisfies this by forwarding.
#[async_trait]
pub trait FederatedTableProvider: Debug + Send + Sync {
    /// The provider for this table, awaiting resolution if the dataset was
    /// registered before its source was contacted.
    async fn table_provider(&self) -> Arc<dyn TableProvider>;

    /// The provider if it is already resolved, without awaiting.
    ///
    /// `None` means resolution is still pending, so a caller that cannot await
    /// — a synchronous stream-construction path — has nothing to inspect yet.
    fn try_table_provider_sync(&self) -> Option<Arc<dyn TableProvider>>;
}
