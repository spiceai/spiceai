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

//! Forwards change-data-capture events to a [Drasi](https://drasi.io) source.
//!
//! **Alpha** — in preview, and should not be used in production.
//!
//! Drasi runs continuous queries over a property graph and reacts when their
//! results change. This crate maps the runtime's CDC stream onto that graph so a
//! dataset accelerated with `refresh_mode: changes` also drives Drasi: one row
//! becomes one node, keyed by its primary key and labelled with its table name.
//!
//! # Delivery
//!
//! [`DrasiSink::forward`] is called *before* the change envelope acknowledges
//! the source's replication position, so a change reaches Drasi at least once:
//! a failure that stalls the stream leaves the position unacknowledged and the
//! change is replayed. The cost is duplicates on retry, which both wire formats
//! absorb — an insert or update is a full-state replace keyed by element id, and
//! deleting an absent element is a no-op.
//!
//! What a failure *does* to the stream is set by
//! [`OnDeliveryError`](config::OnDeliveryError), except that a permanent failure
//! never retries under any policy: an identical retry of a rejected payload
//! produces an identical rejection.

pub mod config;
pub mod element;
pub mod error;
pub mod model;
pub mod sink;
pub mod transport;

pub use config::{DrasiSinkConfig, OnDeliveryError, TransportConfig, redact_url};
pub use element::ElementMapping;
pub use error::{Error, Result};
pub use sink::{DrasiChangeRows, DrasiSink};
