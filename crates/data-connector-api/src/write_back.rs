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

//! Connector-owned durable write-back delivery.
//!
//! The durable write-back worker reconciles a committed accelerator row to its
//! source. For most sources the worker drives that through the source's
//! `TableProvider` (`INSERT ... ON CONFLICT` for the upsert leg, `DELETE` for
//! the absent-key leg). A source that must observe something about its *own*
//! delivery transaction — `PostgreSQL` records each delivery's transaction id so
//! the CDC pump can drop the echo it will later stream back — cannot do so from
//! behind the `TableProvider`, whose upsert commits inside the
//! `datafusion-table-providers` fork where the connector never sees the
//! transaction.
//!
//! A connector that needs to own its delivery transaction returns a
//! [`WriteBackDeliverer`] from
//! [`DataConnector::write_back_deliverer`](crate::DataConnector::write_back_deliverer).
//! When present, the worker hands each pass's upsert rows and absent keys to the
//! deliverer instead of the `TableProvider` path; when absent (`None`), the
//! worker keeps its existing `TableProvider` delivery unchanged.

use arrow::array::ArrayRef;
use arrow::record_batch::RecordBatch;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use snafu::Snafu;

/// Failure of a connector-owned write-back delivery. Opaque to the worker, which
/// only distinguishes success from failure: a failed pass is retried whole, so
/// the worker needs the message for its log, not the variant.
#[derive(Debug, Snafu)]
pub enum DeliveryError {
    #[snafu(display("{message}"))]
    Delivery {
        message: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

pub type DeliveryResult<T = ()> = std::result::Result<T, DeliveryError>;

/// A connector that owns the transaction each durable write-back delivery runs
/// in, so it can observe or stamp that transaction (see the module docs).
///
/// The delivery worker calls these exactly as it would drive its own
/// `TableProvider` path: the present-key upsert rows for a pass go through
/// [`deliver_upserts`](Self::deliver_upserts) in a single source transaction,
/// and the absent keys through [`deliver_deletes`](Self::deliver_deletes). Both
/// must be idempotent: the worker retries the entire pass on any error and only
/// clears its dirty-key markers after a fully successful pass, so a delivery may
/// be replayed.
#[async_trait]
pub trait WriteBackDeliverer: Send + Sync {
    /// The schema the deliverer expects `rows` in — the source table's schema.
    /// The worker casts each pass's rows to this before calling
    /// [`deliver_upserts`](Self::deliver_upserts), so a type or column difference
    /// between the accelerator and the source is reconciled up front rather than
    /// left to fail mid-delivery. Exposing it here keeps the worker from having to
    /// resolve the source `TableProvider`'s schema itself.
    fn target_schema(&self) -> SchemaRef;

    /// Upsert every batch of `rows` into the source in **one** transaction. The
    /// rows are already cast to [`target_schema`](Self::target_schema) by the
    /// worker.
    ///
    /// # Errors
    ///
    /// Returns an error if the transaction cannot be prepared, executed, or
    /// committed; the worker retries the whole pass.
    async fn deliver_upserts(&self, rows: Vec<RecordBatch>) -> DeliveryResult;

    /// Delete the rows whose single-column primary key is in `keys` from the
    /// source, in its own transaction. `pk_column` names the primary-key column.
    ///
    /// # Errors
    ///
    /// Returns an error if the transaction cannot be prepared, executed, or
    /// committed; the worker retries the whole pass.
    async fn deliver_deletes(&self, keys: ArrayRef, pk_column: &str) -> DeliveryResult;
}
