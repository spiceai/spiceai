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

use super::ConnectorComponent;
use super::ConnectorParams;
use super::DataConnector;
use super::DataConnectorError;
use super::DataConnectorFactory;
use super::ParameterSpec;
use crate::component::dataset::Dataset;
use crate::federated_table::FederatedTable;
use arrow_flight::decode::DecodedPayload;
use async_stream::stream;
use async_trait::async_trait;
use data_components::cdc::{
    self, ChangeBatch, ChangeEnvelope, ChangesStream, CommitChange, CommitError,
};
use data_components::flight::FlightFactory;
use data_components::flight::FlightTable;
use data_components::{Read, ReadWrite};
use datafusion::datasource::TableProvider;
use datafusion::sql::unparser::dialect::Dialect;
use datafusion::sql::unparser::dialect::IntervalStyle;
use datafusion::sql::unparser::dialect::PostgreSqlDialect;
use datafusion::sql::TableReference;
use datafusion_federation::FederatedTableProviderAdaptor;
use flight_client::Credentials;
use flight_client::FlightClient;
use futures::{Stream, StreamExt};
use ns_lookup::verify_endpoint_connection;
use snafu::prelude::*;
use std::any::Any;
use std::borrow::Borrow;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tonic::metadata::errors::InvalidMetadataValue;
use tonic::metadata::Ascii;
use tonic::metadata::MetadataMap;
use tonic::metadata::MetadataValue;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Missing required parameter: {parameter}. Specify a value.\nFor details, visit: https://spiceai.org/docs/components/data-connectors/spiceai#configuration"))]
    MissingRequiredParameter { parameter: String },

    #[snafu(display(r#"Failed to connect to SpiceAI endpoint "{endpoint}".\n{source}\nEnsure the endpoint is valid and reachable"#))]
    UnableToVerifyEndpointConnection {
        source: ns_lookup::Error,
        endpoint: String,
    },

    #[snafu(display("Failed to create flight client.\n{source}"))]
    UnableToCreateFlightClient { source: flight_client::Error },

    #[snafu(display("Failed to get append stream schema.\n{source}"))]
    UnableToGetAppendSchema { source: flight_client::Error },

    #[snafu(display("Could not parse <org> or <app> as ASCII: {value}\nEnsure the org and app are valid ASCII strings and retry."))]
    InvalidMetadataValue {
        value: Arc<str>,
        source: InvalidMetadataValue,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Clone)]
pub struct SpiceAI {
    flight_factory: FlightFactory,
}

impl SpiceAI {
    pub fn flight_factory(
        &self,
        dataset_path: SpiceAIDatasetPath,
    ) -> (FlightFactory, TableReference) {
        let (flight_factory, table_reference) = match dataset_path {
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
        };

        (flight_factory, table_reference)
    }
}

pub struct SpiceCloudPlatformDialect {}

impl Dialect for SpiceCloudPlatformDialect {
    fn use_timestamp_for_date64(&self) -> bool {
        true
    }

    fn interval_style(&self) -> IntervalStyle {
        IntervalStyle::PostgresVerbose
    }

    fn identifier_quote_style(&self, identifier: &str) -> Option<char> {
        PostgreSqlDialect {}.identifier_quote_style(identifier)
    }
}

#[derive(Default, Copy, Clone)]
pub struct SpiceAIFactory {}

impl SpiceAIFactory {
    #[must_use]
    pub fn new() -> Self {
        Self {}
    }

    #[must_use]
    pub fn new_arc() -> Arc<dyn DataConnectorFactory> {
        Arc::new(Self {}) as Arc<dyn DataConnectorFactory>
    }
}

const PARAMETERS: &[ParameterSpec] = &[
    ParameterSpec::connector("api_key").secret(),
    ParameterSpec::connector("token").secret(),
    ParameterSpec::connector("endpoint"),
];

const HEADER_ORG: &str = "spiceai-org";
const HEADER_APP: &str = "spiceai-app";

impl DataConnectorFactory for SpiceAIFactory {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn create(
        &self,
        params: ConnectorParams,
    ) -> Pin<Box<dyn Future<Output = super::NewDataConnectorResult> + Send>> {
        let default_flight_url: Arc<str> = if cfg!(feature = "dev") {
            "https://dev-flight.spiceai.io".into()
        } else {
            "https://flight.spiceai.io".into()
        };
        Box::pin(async move {
            let url: Arc<str> = params
                .parameters
                .get("endpoint")
                .expose()
                .ok()
                .map_or(default_flight_url, Into::into);
            tracing::trace!("Connecting to SpiceAI with flight url: {url}");

            verify_endpoint_connection(&url).await.with_context(|_| {
                UnableToVerifyEndpointConnectionSnafu {
                    endpoint: url.to_string(),
                }
            })?;

            let api_key = params
                .parameters
                .get("api_key")
                .expose()
                .ok_or_else(|p| MissingRequiredParameterSnafu { parameter: p.0 }.build())?;
            let credentials = Credentials::new("", api_key);

            let flight_client = FlightClient::try_new(url, credentials, None)
                .await
                .context(UnableToCreateFlightClientSnafu)?;
            let flight_factory = FlightFactory::new(
                "spice.ai",
                flight_client,
                Arc::new(SpiceCloudPlatformDialect {}),
                false,
            );
            let spiceai = SpiceAI { flight_factory };
            Ok(Arc::new(spiceai) as Arc<dyn DataConnector>)
        })
    }

    fn prefix(&self) -> &'static str {
        "spiceai"
    }

    fn parameters(&self) -> &'static [ParameterSpec] {
        PARAMETERS
    }
}

#[async_trait]
impl DataConnector for SpiceAI {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn read_provider(
        &self,
        dataset: &Dataset,
    ) -> super::DataConnectorResult<Arc<dyn TableProvider>> {
        let dataset_schema = dataset.schema();

        let dataset_path = match SpiceAI::spice_dataset_path(dataset) {
            Ok(dataset_path) => dataset_path,
            Err(e) => {
                return Err(DataConnectorError::UnableToGetReadProvider {
                    dataconnector: "spice.ai".to_string(),
                    connector_component: ConnectorComponent::from(dataset),
                    source: Box::new(e),
                });
            }
        };

        let (flight_factory, table_reference) = self.flight_factory(dataset_path);

        match Read::table_provider(&flight_factory, table_reference, dataset_schema).await {
            Ok(provider) => Ok(provider),
            Err(e) => {
                if let Some(data_components::flight::Error::UnableToGetSchema {
                    source: _,
                    table,
                }) = e.downcast_ref::<data_components::flight::Error>()
                {
                    tracing::debug!("{e}");
                    return Err(DataConnectorError::UnableToGetSchema {
                        dataconnector: "spice.ai".to_string(),
                        connector_component: ConnectorComponent::from(dataset),
                        table_name: table.clone(),
                    });
                }

                return Err(DataConnectorError::UnableToGetReadProvider {
                    dataconnector: "spice.ai".to_string(),
                    connector_component: ConnectorComponent::from(dataset),
                    source: e,
                });
            }
        }
    }

    async fn read_write_provider(
        &self,
        dataset: &Dataset,
    ) -> Option<super::DataConnectorResult<Arc<dyn TableProvider>>> {
        let dataset_path = match SpiceAI::spice_dataset_path(dataset) {
            Ok(dataset_path) => dataset_path,
            Err(e) => {
                return Some(Err(DataConnectorError::UnableToGetReadProvider {
                    dataconnector: "spice.ai".to_string(),
                    connector_component: ConnectorComponent::from(dataset),
                    source: Box::new(e),
                }));
            }
        };
        let (flight_factory, table_reference) = match dataset_path {
            SpiceAIDatasetPath::OrgAppPath { org, app, path } => {
                let mut map = MetadataMap::new();
                map.insert(HEADER_ORG, org);
                map.insert(HEADER_APP, app);
                (self.flight_factory.clone().with_metadata(map), path)
            }
            SpiceAIDatasetPath::Path(path) => (self.flight_factory.clone(), path),
        };

        let read_write_result =
            ReadWrite::table_provider(&flight_factory, table_reference, dataset.schema())
                .await
                .context(super::UnableToGetReadWriteProviderSnafu {
                    dataconnector: "spice.ai",
                    connector_component: ConnectorComponent::from(dataset),
                });

        Some(read_write_result)
    }

    fn supports_append_stream(&self) -> bool {
        true
    }

    fn append_stream(&self, federated_table: Arc<FederatedTable>) -> Option<ChangesStream> {
        Some(Box::pin(stream! {
            let table_provider = federated_table.table_provider().await;
            let Some(federated_table_provider_adaptor) = table_provider
            .as_any()
            .downcast_ref::<FederatedTableProviderAdaptor>() else {
                return;
            };
            let Some(federated_adaptor) = federated_table_provider_adaptor.table_provider.as_ref() else {
                return;
            };
            let Some(flight_table) = federated_adaptor
            .as_any()
            .downcast_ref::<FlightTable>() else {
                return;
            };

            let mut stream = Box::pin(subscribe_to_append_stream(
                flight_table.get_flight_client(),
                flight_table.get_table_reference(),
            ));

            while let Some(item) = stream.next().await {
                yield item;
            }
        }))
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum SpiceAIDatasetPath {
    OrgAppPath {
        org: MetadataValue<Ascii>,
        app: MetadataValue<Ascii>,
        path: TableReference,
    },
    Path(TableReference),
}

impl SpiceAI {
    /// Parses a dataset path from a Spice AI dataset definition.
    ///
    /// Spice AI datasets have the following format for `dataset.path()`:
    /// `<org>/<app>/datasets/<dataset_name>`.
    fn spice_dataset_path<T: Borrow<Dataset>>(dataset: T) -> Result<SpiceAIDatasetPath> {
        let dataset = dataset.borrow();
        let path = dataset.path();
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

pub fn subscribe_to_append_stream(
    mut client: FlightClient,
    table_reference: String,
) -> impl Stream<Item = Result<ChangeEnvelope, cdc::StreamError>> {
    stream! {
        match client.subscribe(&table_reference).await {
            Ok(mut stream) => {
                while let Some(decoded_data) = stream.next().await {
                    match decoded_data {
                        Ok(decoded_data) => match decoded_data.payload {
                            DecodedPayload::None | DecodedPayload::Schema(_) => continue,
                            DecodedPayload::RecordBatch(batch) => {
                                match ChangeBatch::try_new(batch).map(|rb| {
                                    ChangeEnvelope::new(Box::new(SpiceAIChangeCommiter {}), rb)
                                }) {
                                    Ok(change_batch) => yield Ok(change_batch),
                                    Err(e) => {
                                        yield Err(cdc::StreamError::SerdeJsonError(e.to_string()))
                                    }
                                };
                            }
                        },
                        Err(e) => {
                            yield Err(cdc::StreamError::Flight(e.to_string()));
                        }
                    }
                }
            }
            Err(e) => {
                yield Err(cdc::StreamError::Flight(e.to_string()));
            }
        }
    }
}

pub struct SpiceAIChangeCommiter {}

impl CommitChange for SpiceAIChangeCommiter {
    fn commit(&self) -> Result<(), CommitError> {
        // Noop
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[allow(clippy::too_many_lines)]
    fn test_spice_dataset_path() {
        let tests = vec![
            (
                "spice.ai:lukekim/demo/datasets/my_data".to_string(),
                SpiceAIDatasetPath::OrgAppPath {
                    org: MetadataValue::try_from("lukekim").expect("failed to parse org"),
                    app: MetadataValue::try_from("demo").expect("failed to parse app"),
                    path: TableReference::parse_str("my_data"),
                },
            ),
            (
                "spice.ai://lukekim/demo/datasets/my_data".to_string(),
                SpiceAIDatasetPath::OrgAppPath {
                    org: MetadataValue::try_from("lukekim").expect("failed to parse org"),
                    app: MetadataValue::try_from("demo").expect("failed to parse app"),
                    path: TableReference::parse_str("my_data"),
                },
            ),
            (
                "spice.ai/lukekim/demo/datasets/my_data".to_string(),
                SpiceAIDatasetPath::OrgAppPath {
                    org: MetadataValue::try_from("lukekim").expect("failed to parse org"),
                    app: MetadataValue::try_from("demo").expect("failed to parse app"),
                    path: TableReference::parse_str("my_data"),
                },
            ),
            (
                "spice.ai/eth.recent_blocks".to_string(),
                SpiceAIDatasetPath::Path(TableReference::parse_str("eth.recent_blocks")),
            ),
            (
                "spice.ai/eth.transactions".to_string(),
                SpiceAIDatasetPath::Path(TableReference::parse_str("eth.transactions")),
            ),
            (
                "spice.ai/public.users".to_string(),
                SpiceAIDatasetPath::Path(TableReference::parse_str("public.users")),
            ),
            (
                "spice.ai/org1/app1/datasets/table_with_underscore".to_string(),
                SpiceAIDatasetPath::OrgAppPath {
                    org: MetadataValue::try_from("org1").expect("failed to parse org"),
                    app: MetadataValue::try_from("app1").expect("failed to parse app"),
                    path: TableReference::parse_str("table_with_underscore"),
                },
            ),
            (
                "spice.ai/org-name/app-name/datasets/table-name".to_string(),
                SpiceAIDatasetPath::OrgAppPath {
                    org: MetadataValue::try_from("org-name").expect("failed to parse org"),
                    app: MetadataValue::try_from("app-name").expect("failed to parse app"),
                    path: TableReference::parse_str("table-name"),
                },
            ),
            (
                "spice.ai/complex.table.name".to_string(),
                SpiceAIDatasetPath::Path(TableReference::parse_str("complex.table.name")),
            ),
            (
                "spice.ai/org.name/app.id/datasets/table.name".to_string(),
                SpiceAIDatasetPath::OrgAppPath {
                    org: MetadataValue::try_from("org.name").expect("failed to parse org"),
                    app: MetadataValue::try_from("app.id").expect("failed to parse app"),
                    path: TableReference::parse_str("table.name"),
                },
            ),
            (
                "spice.ai/my.org/my.app/datasets/data.table.name".to_string(),
                SpiceAIDatasetPath::OrgAppPath {
                    org: MetadataValue::try_from("my.org").expect("failed to parse org"),
                    app: MetadataValue::try_from("my.app").expect("failed to parse app"),
                    path: TableReference::parse_str("data.table.name"),
                },
            ),
            (
                "spice.ai/schema.name.table".to_string(),
                SpiceAIDatasetPath::Path(TableReference::parse_str("schema.name.table")),
            ),
            (
                "spice.ai/org.with.dots/app.with.dots/datasets/table.with.dots".to_string(),
                SpiceAIDatasetPath::OrgAppPath {
                    org: MetadataValue::try_from("org.with.dots").expect("failed to parse org"),
                    app: MetadataValue::try_from("app.with.dots").expect("failed to parse app"),
                    path: TableReference::parse_str("table.with.dots"),
                },
            ),
            (
                "spice.ai/a.b.c/x.y.z/datasets/t1.t2.t3".to_string(),
                SpiceAIDatasetPath::OrgAppPath {
                    org: MetadataValue::try_from("a.b.c").expect("failed to parse org"),
                    app: MetadataValue::try_from("x.y.z").expect("failed to parse app"),
                    path: TableReference::parse_str("t1.t2.t3"),
                },
            ),
        ];

        for (input, expected) in tests {
            let dataset = Dataset::try_new(input.clone(), "bar").expect("a valid dataset");
            let dataset_path = SpiceAI::spice_dataset_path(&dataset).expect("a valid dataset path");
            assert_eq!(dataset_path, expected, "Failed for input: {input}");
        }
    }
}
