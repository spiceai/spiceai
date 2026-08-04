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

use crate::acceleration::Acceleration;
use datafusion::common::TableReference;
use runtime_secrets::Secrets;
use std::{future::Future, pin::Pin, sync::Arc};
use tokio::sync::RwLock;

pub type InitializedSourcesFuture<'a> =
    Pin<Box<dyn Future<Output = Vec<Arc<dyn AccelerationSource>>> + Send + 'a>>;

/// Represents an acceleration source component, such as a dataset or a view.
/// Provides additional information about the source, such as its name and associated metadata.
pub trait AccelerationSource: Send + Sync {
    /// Returns a clone of the source as an `Arc<dyn AccelerationSource>`
    fn clone_arc(&self) -> Arc<dyn AccelerationSource>;

    /// Returns true if the source uses file-based acceleration
    fn is_file_accelerated(&self) -> bool;

    /// Returns the application associated with this source
    fn app(&self) -> Arc<app::App>;

    /// Returns the secrets store associated with this source
    fn secrets(&self) -> Arc<RwLock<Secrets>>;

    /// Returns the acceleration configuration if it exists
    fn acceleration(&self) -> Option<&Acceleration>;

    /// Returns the name of this source
    fn name(&self) -> &TableReference;

    /// The name of the connector this source's rows arrive from — the `from:`
    /// prefix (`debezium`, `cdc`, `sink`, `postgres`, …) — or `None` for a source
    /// with no connector.
    ///
    /// Load-bearing because `DataConnector::resolve_refresh_mode` fills in an unset
    /// `refresh_mode` and its result is never written back into [`Acceleration`]:
    /// `acceleration().refresh_mode` is still `None` for a genuine `debezium:`/`cdc:`
    /// stream. A consumer that must know the mode the source will actually run with
    /// maps this name through the connector-default table instead of reading the
    /// field raw (see `runtime::builder::unset_refresh_mode_for_connector`).
    ///
    /// Deliberately has NO default implementation: every impl states its own answer,
    /// so a new source cannot silently inherit a wrong `None` and misclassify itself.
    fn connector_name(&self) -> Option<&str>;

    /// Returns the time column name if configured, None otherwise.
    /// Views always return None as they don't support time-based append mode.
    fn time_column(&self) -> Option<&str>;

    /// Returns a reference to `Any` for downcasting
    fn as_any(&self) -> &dyn std::any::Any;

    /// Returns all initialized acceleration sources (datasets and views) known to this source's
    /// runtime. Used by `DuckDB` to attach peer file-mode databases. Default returns empty.
    fn initialized_sources(&self) -> InitializedSourcesFuture<'_> {
        Box::pin(async { vec![] })
    }
}
