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

//! Collector module for gathering `ParameterSpecs` from all registered connectors, accelerators,
//! and model sources.
//!
//! This module explicitly references all connector modules to ensure they are linked into the
//! binary and their `linkme` distributed slice registrations are included.

use runtime::dataaccelerator::DATA_ACCELERATOR_REGISTRATIONS;
use runtime::dataconnector::DATA_CONNECTOR_REGISTRATIONS;
use runtime::model::params::{
    anthropic, azure, bedrock, databricks, file, google, huggingface, openai, perplexity, xai,
};
use runtime_parameters::ParameterSpec;

// Force linkage of all data connector modules by referencing their factory types.
// Without these references, the linker may not include the modules and their
// `register_data_connector!` registrations won't appear in DATA_CONNECTOR_REGISTRATIONS.
#[expect(unused_imports)]
use runtime::dataconnector::abfs as _;
#[expect(unused_imports)]
use runtime::dataconnector::clickhouse as _;
#[expect(unused_imports)]
use runtime::dataconnector::databricks as _;
#[expect(unused_imports)]
use runtime::dataconnector::debezium as _;
#[expect(unused_imports)]
use runtime::dataconnector::delta_lake as _;
#[expect(unused_imports)]
use runtime::dataconnector::dremio as _;
#[expect(unused_imports)]
use runtime::dataconnector::duckdb as _;
#[expect(unused_imports)]
use runtime::dataconnector::dynamodb as _;
#[expect(unused_imports)]
use runtime::dataconnector::file as _;
#[expect(unused_imports)]
use runtime::dataconnector::flightsql as _;
#[expect(unused_imports)]
use runtime::dataconnector::ftp as _;
#[expect(unused_imports)]
use runtime::dataconnector::git as _;
#[expect(unused_imports)]
use runtime::dataconnector::github as _;
#[expect(unused_imports)]
use runtime::dataconnector::glue as _;
#[expect(unused_imports)]
use runtime::dataconnector::graphql as _;
#[expect(unused_imports)]
use runtime::dataconnector::https as _;
#[expect(unused_imports)]
use runtime::dataconnector::iceberg as _;
#[expect(unused_imports)]
use runtime::dataconnector::imap as _;
#[expect(unused_imports)]
use runtime::dataconnector::kafka as _;
#[expect(unused_imports)]
use runtime::dataconnector::localpod as _;
#[expect(unused_imports)]
use runtime::dataconnector::memory as _;
#[expect(unused_imports)]
use runtime::dataconnector::mongodb as _;
#[expect(unused_imports)]
use runtime::dataconnector::mssql as _;
#[expect(unused_imports)]
use runtime::dataconnector::mysql as _;
// #[expect(unused_imports)]
// use runtime::dataconnector::odbc as _;
#[expect(unused_imports)]
use runtime::dataconnector::oracle as _;
#[expect(unused_imports)]
use runtime::dataconnector::postgres as _;
#[expect(unused_imports)]
use runtime::dataconnector::s3 as _;
#[expect(unused_imports)]
use runtime::dataconnector::sftp as _;
#[expect(unused_imports)]
use runtime::dataconnector::sharepoint as _;
#[expect(unused_imports)]
use runtime::dataconnector::sink as _;
#[expect(unused_imports)]
use runtime::dataconnector::snowflake as _;
#[expect(unused_imports)]
use runtime::dataconnector::spark as _;
#[expect(unused_imports)]
use runtime::dataconnector::spiceai as _;

// Force linkage of all data accelerator modules
#[expect(unused_imports)]
use runtime::dataaccelerator::arrow as _;
#[cfg(not(windows))]
#[expect(unused_imports)]
use runtime::dataaccelerator::cayenne as _;
#[expect(unused_imports)]
use runtime::dataaccelerator::duckdb as _;
#[expect(unused_imports)]
use runtime::dataaccelerator::partitioned_duckdb as _;
#[expect(unused_imports)]
use runtime::dataaccelerator::postgres as _;
#[expect(unused_imports)]
use runtime::dataaccelerator::sqlite as _;
#[expect(unused_imports)]
use runtime::dataaccelerator::turso as _;

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

/// Schema information for a model source.
#[derive(Debug, Clone)]
pub struct ModelSourceSchema {
    /// The name of the model source (e.g., "openai", "anthropic").
    pub name: &'static str,
    /// The prefix used for component parameters.
    pub prefix: &'static str,
    /// The parameter specifications for this model source.
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
    vec![
        // Iceberg (always available)
        CatalogConnectorSchema {
            name: "iceberg",
            prefix: "iceberg",
            parameters: &runtime::catalogconnector::iceberg::PARAMETERS,
        },
        // Spice Cloud (always available)
        CatalogConnectorSchema {
            name: "spice.ai",
            prefix: "spiceai",
            parameters: runtime::catalogconnector::spice_cloud::PARAMETERS,
        },
        // Unity Catalog
        CatalogConnectorSchema {
            name: "unity_catalog",
            prefix: "unity_catalog",
            parameters: runtime::catalogconnector::unity_catalog::PARAMETERS,
        },
        // Databricks
        CatalogConnectorSchema {
            name: "databricks",
            prefix: "databricks",
            parameters: runtime::catalogconnector::databricks::PARAMETERS,
        },
    ]
}

/// Collects schema information from all model sources.
///
/// Model sources define their parameters in `crates/runtime/src/model/params/`.
/// This function enumerates them directly to avoid adding schema-generation-only
/// code to the runtime.
#[must_use]
pub fn collect_model_sources() -> Vec<ModelSourceSchema> {
    vec![
        ModelSourceSchema {
            name: "openai",
            prefix: "openai",
            parameters: openai::PARAMETERS,
        },
        ModelSourceSchema {
            name: "azure",
            prefix: "azure",
            parameters: azure::PARAMETERS,
        },
        ModelSourceSchema {
            name: "file",
            prefix: "file",
            parameters: file::PARAMETERS,
        },
        ModelSourceSchema {
            name: "databricks",
            prefix: "databricks",
            parameters: databricks::PARAMETERS,
        },
        ModelSourceSchema {
            name: "huggingface",
            prefix: "huggingface",
            parameters: huggingface::PARAMETERS,
        },
        ModelSourceSchema {
            name: "anthropic",
            prefix: "anthropic",
            parameters: anthropic::PARAMETERS,
        },
        ModelSourceSchema {
            name: "perplexity",
            prefix: "perplexity",
            parameters: perplexity::PARAMETERS,
        },
        ModelSourceSchema {
            name: "xai",
            prefix: "xai",
            parameters: xai::PARAMETERS,
        },
        ModelSourceSchema {
            name: "bedrock",
            prefix: "bedrock",
            parameters: bedrock::PARAMETERS,
        },
        ModelSourceSchema {
            name: "google",
            prefix: "google",
            parameters: google::PARAMETERS,
        },
    ]
}
