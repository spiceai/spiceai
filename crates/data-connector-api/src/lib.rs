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

//! The data-connector contract shared by the runtime and every connector.
//!
//! A connector is built for a *component* — a dataset or a catalog — and needs
//! that component's spicepod configuration to do its job. This crate holds the
//! [`DataConnector`] trait itself, the [`DataConnectorFactory`] that builds one,
//! the link-time registration that makes it discoverable, the
//! [`ConnectorParams`] a connector is handed and the [`ConnectorContext`] it
//! reaches runtime capabilities through — so a connector crate can name all of
//! them without depending on the runtime that orchestrates it.
//!
//! The shared vocabulary ([`ConnectorComponent`], [`DataConnectorError`]) lives
//! one crate lower, in `data-connector-types`, because connector *building
//! blocks* need it too and sit below this contract. It is re-exported here so a
//! connector names one crate rather than two.

pub mod accelerated;
mod connector;
pub mod federated;
pub mod listing;
pub mod parameters;
pub mod schema_projection;

pub use connector::{
    DATA_CONNECTOR_REGISTRATIONS, DataConnector, DataConnectorFactory, DataConnectorRegistration,
    MetricsProviderComponent, NewDataConnectorResult, default_spice_client,
};
/// Re-exported so a crate invoking [`register_data_connector!`] can bring
/// `linkme` into scope with `use data_connector_api::linkme;` instead of taking
/// its own dependency. `$crate` does not resolve inside an attribute-macro path,
/// so the expansion has to name `linkme` unqualified.
pub use linkme;
// Glob rather than a named list: SNAFU generates ~25 context selectors
// (`InvalidConfigurationSnafu`, …) alongside `DataConnectorError`, and every
// connector builds its errors through them.
pub use data_connector_types::*;
pub use parameters::{ConnectorContext, ConnectorParams, Validator};
