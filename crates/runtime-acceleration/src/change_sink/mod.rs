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

//! Table-bound ownership of ordered accelerator changes.
//!
//! A submission transfers work to the table owner. Publication and durability
//! are separate from source acknowledgement; sources retain their committers.

pub mod batch;
pub mod batching;
mod driver;
pub mod provider;
pub mod source_policy;

#[cfg(test)]
mod tests;

use std::sync::Arc;

use arrow_tools::schema_evolution::WideningPlan;
use async_trait::async_trait;
use datafusion::common::TableReference;
use datafusion::datasource::TableProvider;
use datafusion::error::Result;
use datafusion::execution::context::SessionContext;
use futures::future::BoxFuture;
use tokio::sync::Mutex;

pub use batch::{AppendValidation, CdcRows, ChangeBatch, ChangePayload, SetKey};
pub use driver::{ChangePermit, ChangeSink, Publication, Submission, WriteReceipt};

/// The producer's recovery contract. This does not contain a source offset.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Recovery {
    /// Use the target's synchronous write path; do not admit replay-dependent RAM writes.
    #[default]
    Durable,
    /// A real source can replay changes that have not passed their durability fence.
    Replayable,
    /// Lost cache contents can be fetched again; no upstream acknowledgement exists.
    Rebuildable,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReplacementSupport {
    Unsupported,
    /// The delete and append can become visible separately.
    Ordered,
    Atomic,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SchemaEvolutionSupport {
    Live,
    Restart,
    Recreate,
}

#[derive(Clone, Copy, Debug)]
pub struct ChangeCapabilities {
    pub replacement: ReplacementSupport,
    pub deferred_durability: bool,
    pub deferred_deletes: bool,
    pub schema_evolution: SchemaEvolutionSupport,
}

/// Storage completion for a successful operation. Deferred fences are local to
/// this table generation, not replication offsets.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StorageDurability {
    Durable,
    Deferred(u64),
    NotPromised,
}

/// Receives durable storage fences, including idle checkpoint progress.
/// Implementations must preserve ordering when notifying a source.
#[async_trait]
pub trait DurabilityObserver: Send + Sync {
    async fn on_durable(&self, fence: u64);
}

#[derive(Clone, Copy, Debug)]
pub struct WriteOptions {
    pub recovery: Recovery,
    pub delete_batch_size: usize,
}

impl Default for WriteOptions {
    fn default() -> Self {
        Self {
            recovery: Recovery::Durable,
            delete_batch_size: 2048,
        }
    }
}

pub type ChangeIndexes = Vec<Arc<dyn spice_table::Index + Send + Sync>>;

/// The write target and its external index effects. The callback resolves the
/// current source-side index chain without capturing source orchestration types.
#[derive(Clone)]
pub struct ChangeSinkContext {
    pub dataset_name: TableReference,
    pub table: Arc<dyn TableProvider>,
    pub write_lock: Arc<Mutex<()>>,
    pub external_indexes: Arc<dyn Fn() -> ChangeIndexes + Send + Sync>,
}

impl ChangeSinkContext {
    #[must_use]
    pub fn new(dataset_name: TableReference, table: Arc<dyn TableProvider>) -> Self {
        Self {
            dataset_name,
            table,
            write_lock: Arc::new(Mutex::new(())),
            external_indexes: Arc::new(Vec::new),
        }
    }
}

/// Prepared execution result owned by the sink driver. A pending finalizer
/// must run even if every producer drops its receipt.
#[doc(hidden)]
pub struct BackendWrite {
    pub changed: bool,
    pub durability: StorageDurability,
    pub finalizer: Option<BoxFuture<'static, Result<()>>>,
}

impl BackendWrite {
    #[must_use]
    pub fn complete(changed: bool, durability: StorageDurability) -> Self {
        Self {
            changed,
            durability,
            finalizer: None,
        }
    }
}

/// Engine integration for the shared owner. Producers use `ChangeSink`, not
/// this interface. Calls are ordered by the driver on the selected apply runtime.
#[doc(hidden)]
#[async_trait]
pub trait ChangeSinkBackend: Send + Sync {
    /// Schema exposed by the composed write target, including wrapper rewrites.
    fn schema(&self) -> arrow::datatypes::SchemaRef;

    fn capabilities(&self) -> ChangeCapabilities;

    async fn apply(
        &self,
        batch: ChangeBatch,
        options: WriteOptions,
        context: &SessionContext,
    ) -> Result<BackendWrite>;

    fn set_durability_observer(&self, observer: Arc<dyn DurabilityObserver>);

    async fn flush(&self) -> Result<()>;

    async fn evolve_schema(&self, plan: &WideningPlan) -> Result<()>;
}
