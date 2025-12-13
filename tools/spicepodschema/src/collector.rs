/*
Copyright 2024-2025 The Spice.ai OSS Authors

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

//! Collector module for gathering ParameterSpecs from all registered connectors and accelerators.

use runtime::dataaccelerator::DATA_ACCELERATOR_REGISTRATIONS;
use runtime::dataconnector::DATA_CONNECTOR_REGISTRATIONS;
use runtime_parameters::ParameterSpec;

/// Schema information for a connector or accelerator.
#[derive(Debug, Clone)]
pub struct ConnectorSchema {
    /// The name of the connector/accelerator (e.g., "postgres", "duckdb").
    pub name: String,
    /// The prefix used for component parameters (e.g., "pg" for postgres).
    pub prefix: &'static str,
    /// The parameter specifications for this connector/accelerator.
    pub parameters: &'static [ParameterSpec],
}

/// Schema information for a catalog connector.
#[derive(Debug, Clone)]
pub struct CatalogConnectorSchema {
    /// The name of the catalog connector (e.g., "databricks", "iceberg").
    pub name: &'static str,
    /// The prefix used for component parameters.
    pub prefix: &'static str,
    /// The parameter specifications for this catalog connector.
    pub parameters: &'static [ParameterSpec],
}

/// Collects schema information from all registered data connectors.
///
/// This function iterates over the distributed slice of data connector registrations
/// and extracts the name, prefix, and parameters from each connector factory.
#[must_use]
pub fn collect_data_connectors() -> Vec<ConnectorSchema> {
    DATA_CONNECTOR_REGISTRATIONS
        .iter()
        .map(|reg| {
            let factory = (reg.constructor)();
            ConnectorSchema {
                name: reg.name.to_string(),
                prefix: factory.prefix(),
                parameters: factory.parameters(),
            }
        })
        .collect()
}

/// Collects schema information from all registered data accelerators.
///
/// This function iterates over the distributed slice of data accelerator registrations
/// and extracts the engine name, prefix, and parameters from each accelerator.
#[must_use]
pub fn collect_data_accelerators() -> Vec<ConnectorSchema> {
    DATA_ACCELERATOR_REGISTRATIONS
        .iter()
        .map(|reg| {
            let accelerator = (reg.constructor)();
            ConnectorSchema {
                // Use Display trait to get the string representation
                name: reg.engine.to_string(),
                prefix: accelerator.prefix(),
                parameters: accelerator.parameters(),
            }
        })
        .collect()
}

/// Collects schema information from all catalog connectors.
///
/// Since catalog connectors use a manual registry (not linkme distributed slices),
/// we access their PARAMETERS constants directly.
#[must_use]
pub fn collect_catalog_connectors() -> Vec<CatalogConnectorSchema> {
    let mut catalogs = Vec::new();

    // Unity Catalog (requires delta_lake feature in runtime)
    #[cfg(feature = "delta_lake")]
    catalogs.push(CatalogConnectorSchema {
        name: "unity_catalog",
        prefix: "unity_catalog",
        parameters: runtime::catalogconnector::unity_catalog::PARAMETERS,
    });

    // Databricks (requires databricks feature in runtime)
    #[cfg(feature = "databricks")]
    catalogs.push(CatalogConnectorSchema {
        name: "databricks",
        prefix: "databricks",
        parameters: runtime::catalogconnector::databricks::PARAMETERS,
    });

    // Iceberg (always available)
    catalogs.push(CatalogConnectorSchema {
        name: "iceberg",
        prefix: "iceberg",
        parameters: &runtime::catalogconnector::iceberg::PARAMETERS,
    });

    // Spice Cloud (always available)
    catalogs.push(CatalogConnectorSchema {
        name: "spice.ai",
        prefix: "spiceai",
        parameters: runtime::catalogconnector::spice_cloud::PARAMETERS,
    });

    catalogs
}
