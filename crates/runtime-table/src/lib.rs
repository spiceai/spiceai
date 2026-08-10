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

//! The accelerated table and the federated table it wraps.
//!
//! An accelerated dataset is served by an `AcceleratedTable`, which owns the
//! refresh, retention, CDC-apply and caching machinery, and which wraps a
//! `FederatedTable` over the connector's own provider — the
//! `AcceleratedTable -> FederatedTable -> connector provider` chain.
//!
//! This sits below the `runtime` crate: it names the component *configuration*
//! (`runtime-component`), the accelerator contract (`data-accelerator-api`), the
//! query helpers (`runtime-datafusion`) and status reporting
//! (`runtime-status`) — but not the orchestrator itself.

pub mod accelerated;
pub mod federated;
pub mod filter_converter;
pub mod refresh_source;
pub mod table_layers;
pub mod table_metadata;

pub use accelerated::*;
pub use refresh_source::{RefreshSource, RefreshSourceError};
pub use table_metadata::table_provider_with_spicepod_metadata;
