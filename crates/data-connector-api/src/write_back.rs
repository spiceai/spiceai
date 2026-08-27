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
//! `TableProvider` (`INSERT ... ON CONFLICT`). A source that must observe
//! something about its *own* delivery transaction — `PostgreSQL` records each
//! delivery's transaction id so the CDC pump can drop the echo it will later
//! stream back — cannot do so from behind the `TableProvider`, whose upsert
//! commits inside the `datafusion-table-providers` fork where the connector
//! never sees the transaction.
//!
//! A connector that needs to own its delivery transaction returns a
//! [`WriteBackDeliverer`] from
//! [`DataConnector::write_back_deliverer`](crate::DataConnector::write_back_deliverer).
//! When present, the worker hands each pass's rows to the deliverer instead of
//! the `TableProvider` path; when absent (`None`), the worker keeps its
//! `TableProvider` delivery.

use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use snafu::Snafu;

/// Failure of a connector-owned write-back delivery. Opaque to the worker, which
/// only distinguishes success from failure: a failed pass is retried whole, so
/// the worker needs the message for its log, not the variant.
#[derive(Debug, Snafu)]
pub enum DeliveryError {
    #[snafu(display("{message}: {source}"))]
    Delivery {
        message: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

pub type DeliveryResult<T = ()> = std::result::Result<T, DeliveryError>;

/// A connector that owns the transaction each durable write-back delivery runs
/// in, so it can observe or stamp that transaction (see the module docs).
///
/// The delivery worker calls this exactly as it would drive its own
/// `TableProvider` path: a pass's rows go through
/// [`deliver_upserts`](Self::deliver_upserts) in a single source transaction. It
/// must be idempotent — the worker retries the entire pass on any error and only
/// clears its dirty-key markers after a fully successful pass, so a delivery may
/// be replayed.
///
/// There is deliberately no delete primitive. A marker records that a key was
/// committed and has not yet reached the source; it can never record a deletion,
/// because a write-back dataset refuses `DELETE` and only a transaction — which
/// accepts INSERT/UPDATE alone — writes markers. Delivering a deletion inferred
/// from a key being unreadable is what destroyed rows at the system of record
/// (#13398), so the seam that made it expressible is gone.
#[async_trait]
pub trait WriteBackDeliverer: Send + Sync {
    /// Upsert every batch of `rows` into the source in **one** transaction. `rows`
    /// are in the accelerator's schema; casting them to the source table's schema
    /// (if needed) is the deliverer's own concern, not the worker's.
    ///
    /// # Errors
    ///
    /// Returns an error if the transaction cannot be prepared, executed, or
    /// committed; the worker retries the whole pass.
    async fn deliver_upserts(&self, rows: Vec<RecordBatch>) -> DeliveryResult;
}
