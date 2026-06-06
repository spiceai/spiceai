/*
Copyright 2026 The Spice.ai OSS Authors

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

//! Shared types for the Spice.ai data connector.
//!
//! Lives in its own crate so that both `connector-spiceai` and
//! `runtime::catalogconnector::spice_cloud` can depend on it without
//! a circular dependency (`connector-spiceai` depends on `runtime`;
//! `runtime` cannot depend on `connector-spiceai`).

use std::sync::Arc;

use data_components::flight::FlightFactory;
use datafusion::sql::TableReference;
use datafusion::sql::unparser::dialect::{Dialect, IntervalStyle, PostgreSqlDialect};
use flight_client::{Credentials, FlightClient, tls::ClientTlsOptions};
use snafu::prelude::*;
use tonic::metadata::errors::InvalidMetadataValue;
use tonic::metadata::{Ascii, MetadataMap, MetadataValue};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to create Spice.ai Flight client: {source}"))]
    UnableToCreateFlightClient { source: flight_client::Error },

    #[snafu(display(
        "Could not parse <org> or <app> as ASCII: {value} Ensure the org and app are valid ASCII strings and retry."
    ))]
    InvalidMetadataValue {
        value: Arc<str>,
        source: InvalidMetadataValue,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

const HEADER_ORG: &str = "spiceai-org";
const HEADER_APP: &str = "spiceai-app";

/// The SQL dialect used when pushing queries to Spice Cloud.
pub struct SpiceCloudDialect {}

impl Dialect for SpiceCloudDialect {
    fn use_timestamp_for_date64(&self) -> bool {
        true
    }
    fn interval_style(&self) -> IntervalStyle {
        IntervalStyle::PostgresVerbose
    }
    fn identifier_quote_style(&self, identifier: &str) -> Option<char> {
        PostgreSqlDialect {}.identifier_quote_style(identifier)
    }
    fn supports_subquery_in_join_predicate(&self) -> bool {
        false
    }
}

/// The Spice.ai data connector instance.
///
/// Holds a [`FlightFactory`] pre-configured for the Spice.ai Flight endpoint.
/// The catalog connector downcasts to this type to obtain a `FlightFactory`
/// scoped to a specific org/app path.
#[derive(Clone, Debug)]
pub struct SpiceAI {
    pub(crate) flight_factory: FlightFactory,
}

impl SpiceAI {
    /// Create a new [`SpiceAI`] wrapping the given [`FlightFactory`].
    #[must_use]
    pub fn new(flight_factory: FlightFactory) -> Self {
        Self { flight_factory }
    }

    /// Build a [`SpiceAI`] from raw connection parameters.
    ///
    /// This is the shared constructor used by both the data connector factory
    /// (`SpiceAIFactory::create`) and the Spice Cloud catalog connector, so
    /// neither needs to go through the `DataConnector` trait.
    pub async fn from_raw(
        url: String,
        credentials: Credentials,
        tls_options: ClientTlsOptions,
        max_message_size: Option<usize>,
    ) -> Result<Self> {
        use data_components::flight::FlightFactory;

        let mut client =
            FlightClient::try_new_with_tls_options(url.into(), credentials, None, &tls_options)
                .await
                .context(UnableToCreateFlightClientSnafu)?;

        if let Some(max_size) = max_message_size {
            client = client.with_max_message_size(max_size, max_size);
        }

        Ok(Self::new(FlightFactory::new(
            "spice.ai",
            client,
            std::sync::Arc::new(SpiceCloudDialect {}),
        )))
    }

    /// Returns a [`FlightFactory`] scoped to the given [`SpiceAIDatasetPath`],
    /// together with the resolved [`TableReference`].
    #[must_use]
    pub fn flight_factory(
        &self,
        dataset_path: SpiceAIDatasetPath,
    ) -> (FlightFactory, TableReference) {
        match dataset_path {
            SpiceAIDatasetPath::OrgAppPath { org, app, path } => {
                let mut map = MetadataMap::new();
                let spiceai_context = format!(
                    "org={},app={}",
                    org.to_str().unwrap_or_default(),
                    app.to_str().unwrap_or_default()
                );
                map.insert(HEADER_ORG, org);
                map.insert(HEADER_APP, app);
                (
                    self.flight_factory
                        .clone()
                        .with_metadata(map)
                        .with_extra_compute_context(spiceai_context.as_str()),
                    path,
                )
            }
            SpiceAIDatasetPath::Path(path) => (self.flight_factory.clone(), path),
        }
    }

    /// Parses a dataset path from a Spice AI dataset definition.
    ///
    /// Spice AI datasets have the following format for `dataset.path()`:
    /// `<org>/<app>/datasets/<dataset_name>`.
    pub fn spice_dataset_path(name: &TableReference, path: &str) -> Result<SpiceAIDatasetPath> {
        if is_flight_endpoint_path(path) {
            return Ok(SpiceAIDatasetPath::Path(name.clone()));
        }

        let path_parts: Vec<&str> = path.split('/').collect();

        match path_parts.as_slice() {
            [org, app, "datasets", dataset_name] => {
                let org: MetadataValue<Ascii> =
                    MetadataValue::try_from(*org).context(InvalidMetadataValueSnafu {
                        value: Arc::from(*org),
                    })?;
                let app: MetadataValue<Ascii> =
                    MetadataValue::try_from(*app).context(InvalidMetadataValueSnafu {
                        value: Arc::from(*app),
                    })?;
                Ok(SpiceAIDatasetPath::OrgAppPath {
                    org,
                    app,
                    path: TableReference::parse_str(dataset_name),
                })
            }
            _ => Ok(SpiceAIDatasetPath::Path(TableReference::parse_str(path))),
        }
    }
}

fn is_flight_endpoint_path(path: &str) -> bool {
    path.starts_with("http://")
        || path.starts_with("https://")
        || path.starts_with("grpc://")
        || path.starts_with("grpc+tls://")
}
/// Describes how a Spice.ai dataset or catalog path is addressed.
#[derive(Debug, PartialEq, Eq)]
pub enum SpiceAIDatasetPath {
    /// A fully-qualified org/app path used by the catalog connector.
    OrgAppPath {
        org: MetadataValue<Ascii>,
        app: MetadataValue<Ascii>,
        path: TableReference,
    },
    /// A simple table reference path used for regular datasets.
    Path(TableReference),
}
