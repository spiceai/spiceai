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

//! Spice.ai Cloud Platform data connector for Spice.ai runtime.
//!
//! Re-exports the connector from `runtime` and provides factory functions
//! for registration in the connector registry.

pub use runtime::dataconnector::spiceai::{
    SpiceAI, SpiceAIChangeCommiter, SpiceAIDatasetPath, SpiceAIFactory, SpiceCloudPlatformDialect,
    subscribe_to_append_stream,
};

use runtime::dataconnector::DataConnectorFactory;
use std::sync::Arc;

/// The name used to identify this connector in configuration.
pub const CONNECTOR_NAME: &str = "spice.ai";

/// The legacy connector name for backwards compatibility.
pub const LEGACY_CONNECTOR_NAME: &str = "spiceai";

/// Returns a new instance of the Spice.ai connector factory.
#[must_use]
pub fn factory() -> Arc<dyn DataConnectorFactory> {
    SpiceAIFactory::new_arc()
}

/// Returns a new instance of the Spice.ai connector factory using the legacy name.
#[must_use]
pub fn legacy_factory() -> Arc<dyn DataConnectorFactory> {
    SpiceAIFactory::new_arc()
}

// Self-register into runtime's linkme `DATA_CONNECTOR_REGISTRATIONS` slice. Any binary/tool that
// should see this connector must force-link the crate (`use connector_spiceai as _;`) -- a plain
// Cargo dependency won't link the slice static. See `register_data_connector!` docs.
runtime::register_data_connector!(
    register_spiceai_connector,
    SPICEAI_CONNECTOR_REGISTRATION,
    CONNECTOR_NAME,
    SpiceAIFactory
);
