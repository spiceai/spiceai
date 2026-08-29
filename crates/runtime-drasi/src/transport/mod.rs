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

pub mod http;
pub mod redis_stream;

use async_trait::async_trait;

use crate::element::ChangeOp;
use crate::error::{Error, Result, Retryable};
use crate::model::NodeElement;

/// Who a transport is delivering to, for error messages.
///
/// Shared by every transport so a change to [`Error::Delivery`] is made once.
#[derive(Debug, Clone)]
pub struct DeliveryTarget {
    pub dataset: String,
    pub source_id: String,
    /// Human-readable description of the target, safe to log — a transport that
    /// carries credentials in its address must redact them before storing it
    /// here.
    pub endpoint: String,
}

impl DeliveryTarget {
    #[must_use]
    pub fn error(&self, message: String, retryable: Retryable) -> Error {
        Error::Delivery {
            dataset: self.dataset.clone(),
            source_id: self.source_id.clone(),
            endpoint: self.endpoint.clone(),
            message,
            retryable,
        }
    }
}

/// One change, mapped and ready for either wire format.
#[derive(Debug, Clone, PartialEq)]
pub struct PreparedChange {
    pub op: ChangeOp,
    pub node: NodeElement,
    /// Nanoseconds since the Unix epoch, taken from the source's commit
    /// timestamp. `None` when the source did not report one — the HTTP format
    /// then lets Drasi stamp arrival time, and the platform format substitutes
    /// the current time because its `ts_ns` is mandatory.
    pub timestamp_ns: Option<u64>,
}

/// Publishes prepared changes to a Drasi source.
///
/// Implementations deliver a slice **in order** and must not reorder it; the
/// change stream is ordered per key, and Drasi applies changes in the order it
/// receives them.
#[async_trait]
pub trait DrasiTransport: Send + Sync + std::fmt::Debug {
    /// Delivers `changes`, or fails with an error whose
    /// [`retryable`](crate::error::Error::retryable) classification tells the
    /// caller whether waiting could help.
    ///
    /// Delivery is at-least-once: a retry may redeliver changes the target
    /// already accepted. That is safe for both formats — an insert or update is
    /// a full-state replace keyed by element id, and a delete of an absent
    /// element is a no-op.
    async fn deliver(&self, changes: &[PreparedChange]) -> Result<()>;
}
