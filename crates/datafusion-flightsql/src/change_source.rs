/*
Copyright 2026 The Spice.ai OSS Authors

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

//! Abstraction over a live feed of data changes used to serve `do_exchange`
//! (CDC-style) subscriptions.

use datafusion::sql::TableReference;
use futures::stream::BoxStream;
use runtime_query_engine::query_engine::DataUpdate;
use tonic::Status;

/// A source of live change updates for a table.
///
/// Implementations bridge whatever change-propagation mechanism the host uses
/// (e.g. a broadcast channel) into a simple stream of [`DataUpdate`]s.
#[async_trait::async_trait]
pub trait ChangeSource: Send + Sync {
    /// Subscribe to change updates for `table`.
    ///
    /// Yields [`DataUpdate`]s until the source ends. A terminal `None` on the
    /// returned stream is treated as the source closing.
    async fn subscribe(
        &self,
        table: &TableReference,
    ) -> BoxStream<'static, Result<DataUpdate, Status>>;
}
