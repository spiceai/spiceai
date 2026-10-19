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
//! A `ChangeSink` is the single ordered write owner for one generation of one
//! accelerated table. The runtime creates one for the `changes` and `append`
//! refresh modes, and for `caching` when the engine supplies its own sink
//! (Cayenne). CDC streams and the caching-mode writer send their row changes
//! through it. A full-refresh overwrite does not submit rows through the
//! sink; it flushes the sink first.
//!
//! A submission transfers work to the table owner. Publication and durability
//! are separate from source acknowledgement; sources retain their committers.
//!
//! # Write kinds
//!
//! A producer submits one `ChangeBatch` per logical write. There are four
//! kinds: CDC rows, a plain append, a scoped append (rows for a declared set,
//! a `SetKey` scope, that must not collide with rows outside it), and a set
//! replacement (delete the scope, then append its new rows). Row writes can
//! carry an `AppendIngress` lane to opt into coalescing.
//!
//! A backend applies a `ChangeWrite`, which has no lane. The owner merges the
//! row writes of one lane into one `RowsWrite`: plain appends into
//! `RowsWrite::Unscoped`, and scoped appends and replacements into
//! `RowsWrite::Scoped`, which keeps each input's scope, chunk range and
//! `ScopeKind`. A scoped burst deletes every replaced scope once, then
//! appends all rows.
//!
//! # Producer flow
//!
//! ```text
//!  producer task                     owner task (apply runtime)
//!  -------------                     --------------------------
//!  sink.reserve().await  -> ChangePermit   (queue capacity only)
//!  permit.submit(batch, options)
//!        |  -> Submission                  (work now belongs to owner)
//!        |                                 dequeue, coalesce, prepare
//!        |                                 backend.apply(...)
//!  submission.wait().await  <------------- WriteReceipt
//!        |                                   changed, batch_end
//!        |                                   publication: Ready | Pending
//!        |                                   durability: Durable |
//!        |                                     Deferred(fence) | NotPromised
//!  receipt.published().await <------------ finalizer done (rows visible)
//!  acknowledge the source, per `durability`
//! ```
//!
//! `ChangeSink::submit` and `ChangeSink::enqueue` combine these steps.
//! `enqueue` replaces the receipt with a completion callback. Dropping a
//! `Submission` or a receipt cancels no accepted work.
//!
//! # Owner task
//!
//! One task per sink takes commands from a bounded queue in order:
//!
//! - It coalesces CDC inputs from one `CdcIngress` lane, and row writes from
//!   one `AppendIngress` lane, into one burst. `CoalescingLimits` bound the
//!   burst. A command that does not fit is carried to the next turn.
//! - It admits a scoped row write to a burst only while no replaced scope in
//!   the burst can hold another member's rows. Then deleting every replaced
//!   scope before one append stores the same rows as applying the members in
//!   order.
//! - It prepares a whole CDC burst before any mutation: it decodes every
//!   input, rejects unknown operations and rebuild markers, classifies
//!   schemas with the lane's `CdcPolicy`, and refuses a non-live schema
//!   evolution for the whole burst.
//! - It stops on the first failure. Every later write, flush and evolution
//!   gets the same error, and the generation needs recovery. One exception:
//!   a `Recovery::Rebuildable` write that fails with a `before_mutation`
//!   refusal fails only that write, because storage did not change.
//! - When a backend refuses a coalesced scoped write before mutation and names
//!   the first member that would be refused on its own, the owner applies the
//!   members before it, refuses that member, and coalesces the rest again.
//!
//! # Backends
//!
//! ```text
//!                      +--------------------+
//!   CDC stream ------->|                    |    CayenneChangeSinkBackend
//!   caching writer --->|  ChangeSink owner  |--> (native Cayenne writes,
//!                      |  (one per table    |     deferred durability)
//!                      |   generation)      |        or
//!                      +--------------------+    ProviderChangeSinkBackend
//!                                                (TableProvider insert/delete)
//! ```
//!
//! An accelerator engine can supply its own sink. The Cayenne engine uses its
//! native backend when it can find its table through the write layers, and
//! the provider backend otherwise (for example, a partitioned table). Other
//! engines use `provider::ProviderChangeSinkBackend`, which writes through the
//! composed provider's `insert_into` and `delete_from` and promises no storage
//! durability.
//!
//! # Deferred durability and source acknowledgement
//!
//! Cayenne with `cdc_durability: memory` appends `Recovery::Replayable` CDC
//! rows to an in-memory tier and returns `StorageDurability::Deferred(epoch)`.
//! The rows are visible, but they are not durable yet. The source must not
//! acknowledge its replication position until a checkpoint makes that epoch
//! durable:
//!
//! ```text
//!  time -->
//!  owner:    apply e=7 ----- apply e=8 ------- checkpoint_mem_tier (covers 8)
//!  receipt:  Deferred(7)     Deferred(8)              |
//!  source:   queue (7, c7)   queue (8, c8)            |
//!  observer:                                   on_durable(8)
//!  slot ack:                                   commit c7 and c8, in order
//! ```
//!
//! The source registers a `DurabilityObserver` with the sink. The source
//! queues committers by fence after publication, and commits them in order
//! when `on_durable` reports a fence at or above theirs. `Durable` and
//! `NotPromised` receipts let the source acknowledge after publication.

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

pub use batch::{
    CdcRows, ChangeBatch, ChangeWrite, Rows, RowsWrite, ScopeKind, ScopedInput, ScopedRows,
    ScopedWrite, SetKey, replaced_scope_filters,
};
pub use driver::{ChangePermit, ChangeSink, Publication, Submission, WriteReceipt};

/// The producer's recovery contract. This does not contain a source offset.
///
/// The producer sets it in `WriteOptions`. The backend uses it to select a
/// write path, and the owner uses it to decide if a refused write stops the
/// sink.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Recovery {
    /// Use the target's synchronous write path; do not admit replay-dependent RAM writes.
    ///
    /// The default. CDC uses it when a source committer cannot defer its
    /// acknowledgement, or when the batch has a truncate or a delete that the
    /// backend cannot defer.
    #[default]
    Durable,
    /// A real source can replay changes that have not passed their durability fence.
    ///
    /// CDC uses it when every source committer supports deferral. Only this
    /// value lets Cayenne use its in-memory CDC tier.
    Replayable,
    /// Lost cache contents can be fetched again; no upstream acknowledgement exists.
    ///
    /// The caching-mode writer uses it. A `before_mutation` refusal of such a
    /// write fails only that write; the sink continues.
    Rebuildable,
}

/// How a backend executes a set replacement (`ChangeBatch::replace_set`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReplacementSupport {
    /// The backend refuses set replacement before mutation. The default for
    /// `provider::ProviderChangeSinkBackend`.
    Unsupported,
    /// The delete and append can become visible separately.
    ///
    /// Readers can see the deletion gap. The Cayenne backends report this.
    Ordered,
    /// The delete and append become visible together. No current backend
    /// reports this value.
    Atomic,
}

/// When a backend can apply a widening schema change from a CDC stream.
///
/// The source `CdcPolicy` reads it to classify a new schema. The owner
/// refuses a whole CDC burst that needs evolution unless the value is `Live`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SchemaEvolutionSupport {
    /// The backend evolves the table in place through `evolve_schema`
    /// (the native Cayenne backend).
    Live,
    /// The backend cannot evolve mid-stream. Values are cast to the current
    /// schema until a restart applies the new schema. The default for
    /// `provider::ProviderChangeSinkBackend`.
    Restart,
    /// A partitioned Cayenne table: the backend refuses the change before
    /// mutation, and the dataset must be rebuilt to adopt the new schema.
    Recreate,
}

/// What the bound backend can do. Producers read it through
/// `ChangeSink::capabilities` before they choose options or install observers.
#[derive(Clone, Copy, Debug)]
pub struct ChangeCapabilities {
    /// How the backend executes a set replacement.
    pub replacement: ReplacementSupport,
    /// The backend can return `StorageDurability::Deferred`. The CDC source
    /// installs a `DurabilityObserver` only when this is true. True only for
    /// Cayenne with `cdc_durability: memory` and not `mode: memory`.
    pub deferred_durability: bool,
    /// The backend can apply keyed CDC deletes on the deferred path. When
    /// false, a burst with a delete uses `Recovery::Durable`.
    pub deferred_deletes: bool,
    /// When the backend can apply a widening schema change.
    pub schema_evolution: SchemaEvolutionSupport,
}

/// Storage completion for a successful operation. Deferred fences are local to
/// this table generation, not replication offsets.
///
/// The backend returns it in `BackendWrite`, and the producer reads it from
/// `WriteReceipt::durability` to decide when to acknowledge its source.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StorageDurability {
    /// The write is on durable storage when it is published.
    Durable,
    /// The write is visible but in memory. It becomes durable when the
    /// backend reports this fence, or a later one, to its
    /// `DurabilityObserver`. The CDC source queues its committers until then.
    /// Only Cayenne with `cdc_durability: memory` returns this.
    Deferred(u64),
    /// The backend makes no storage claim. The source acknowledges after
    /// publication. Examples: the provider backend, Cayenne `mode: memory`
    /// (which reloads from the source on restart), and caching-mode writes.
    NotPromised,
}

/// Receives durable storage fences, including idle checkpoint progress.
/// Implementations must preserve ordering when notifying a source.
///
/// The CDC source installs one through `ChangeSink::set_durability_observer`
/// when `ChangeCapabilities::deferred_durability` is true. Cayenne calls it
/// after `checkpoint_mem_tier` makes an epoch durable.
#[async_trait]
pub trait DurabilityObserver: Send + Sync {
    /// Every write with a `Deferred` fence at or below `fence` is durable.
    async fn on_durable(&self, fence: u64);
}

/// Per-submission options that the producer passes with each `ChangeBatch`.
/// They also set coalescing boundaries: a different `delete_batch_size`
/// ends a burst, and a `Durable` CDC input makes its whole burst durable.
#[derive(Clone, Copy, Debug)]
pub struct WriteOptions {
    /// How the producer can recover this write if it is lost.
    pub recovery: Recovery,
    /// Maximum number of keys in one delete predicate or one scoped-write
    /// key probe.
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

/// Indexes that a CDC delete must also update.
pub type ChangeIndexes = Vec<Arc<dyn spice_table::Index + Send + Sync>>;

/// The write target and its external index effects. The callback resolves the
/// current source-side index chain without capturing source orchestration types.
///
/// The runtime builds one when it binds a sink, and gives it to the engine's
/// `change_sink` hook or to `provider::ProviderChangeSinkBackend::new`.
#[derive(Clone)]
pub struct ChangeSinkContext {
    /// Dataset name for errors, logs and metrics.
    pub dataset_name: TableReference,
    /// The composed accelerated table that receives the writes.
    pub table: Arc<dyn TableProvider>,
    /// Serializes sink writes with other writers of the same accelerated
    /// table. The runtime passes the table's shared write mutex.
    pub write_lock: Arc<Mutex<()>>,
    /// Returns the source-side indexes that CDC deletes must update. Empty by
    /// default and in caching mode.
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
///
/// A backend returns it from `ChangeSinkBackend::apply`. The owner turns it
/// into the `WriteReceipt` of every input in the write.
#[doc(hidden)]
pub struct BackendWrite {
    /// The write changed table contents. Copied to `WriteReceipt::changed`.
    pub changed: bool,
    /// The storage guarantee once the write is published.
    pub durability: StorageDurability,
    /// Work that makes the write visible, such as a staged Cayenne append.
    /// The owner runs it in a separate task, and the receipt's publication
    /// stays pending until it completes. `None` means already published.
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
///
/// Implementations: `provider::ProviderChangeSinkBackend` (any engine) and the
/// native Cayenne backend.
#[doc(hidden)]
#[async_trait]
pub trait ChangeSinkBackend: Send + Sync {
    /// Schema exposed by the composed write target, including wrapper rewrites.
    fn schema(&self) -> arrow::datatypes::SchemaRef;

    /// What this backend can do. The owner reads it for each CDC burst.
    fn capabilities(&self) -> ChangeCapabilities;

    /// Execute one prepared write. Mark a refusal that changed nothing with
    /// `provider::refusal::before_mutation`, so that a refused `Rebuildable`
    /// write does not stop the owner. Every other failure stops the owner.
    async fn apply(
        &self,
        write: ChangeWrite,
        options: WriteOptions,
        context: &SessionContext,
    ) -> Result<BackendWrite>;

    /// Store the observer that receives `Deferred` fences. A backend that
    /// never defers durability can ignore it.
    fn set_durability_observer(&self, observer: Arc<dyn DurabilityObserver>);

    /// Make all published writes as durable as the backend promises. The
    /// owner calls it after earlier writes are published.
    async fn flush(&self) -> Result<()>;

    /// Apply a widening plan in place, or refuse it before mutation when
    /// `schema_evolution` is not `Live`.
    async fn evolve_schema(&self, plan: &WideningPlan) -> Result<()>;
}
