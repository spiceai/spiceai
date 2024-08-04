/*
Copyright 2024 The Spice.ai OSS Authors

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

use super::DataConnector;
use super::DataConnectorError;
use super::DataConnectorFactory;
use super::ParameterSpec;
use super::Parameters;
use super::UnableToGetReadProviderSnafu;
use crate::component::catalog::Catalog;
use crate::component::dataset::acceleration::RefreshMode;
use crate::component::dataset::Dataset;
use crate::Runtime;
use arrow::datatypes::Schema;
use arrow_flight::decode::DecodedPayload;
use async_stream::stream;
use async_trait::async_trait;
use data_components::cdc::{
    self, ChangeBatch, ChangeEnvelope, ChangesStream, CommitChange, CommitError,
};
use data_components::flight::FlightFactory;
use data_components::flight::FlightTable;
use data_components::{Read, ReadWrite};
use datafusion::catalog::CatalogProvider;
use datafusion::datasource::TableProvider;
use datafusion::sql::unparser::dialect::DefaultDialect;
use datafusion::sql::unparser::dialect::Dialect;
use datafusion::sql::unparser::dialect::IntervalStyle;
use datafusion::sql::TableReference;
use datafusion_federation::FederatedTableProviderAdaptor;
use flight_client::FlightClient;
use futures::{Stream, StreamExt};
use ns_lookup::verify_endpoint_connection;
use snafu::prelude::*;
use std::any::Any;
use std::borrow::Borrow;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to parse SpiceAI dataset path: {dataset_path}"))]
    UnableToParseDatasetPath { dataset_path: String },

    #[snafu(display("Unable to publish data to SpiceAI: {source}"))]
    UnableToPublishData { source: flight_client::Error },

    #[snafu(display("Missing required parameter: {parameter}"))]
    MissingRequiredParameter { parameter: String },

    #[snafu(display(r#"Unable to connect to endpoint "{endpoint}": {source}"#))]
    UnableToVerifyEndpointConnection {
        source: ns_lookup::Error,
        endpoint: String,
    },

    #[snafu(display("Unable to create flight client: {source}"))]
    UnableToCreateFlightClient { source: flight_client::Error },

    #[snafu(display("Unable to get append stream schema: {source}"))]
    UnableToGetAppendSchema { source: flight_client::Error },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Clone)]
pub struct SpiceAI {
    flight_factory: FlightFactory,
}

pub struct SpiceCloudPlatformDialect {}

impl Dialect for SpiceCloudPlatformDialect {
    fn use_timestamp_for_date64(&self) -> bool {
        true
    }

    fn interval_style(&self) -> IntervalStyle {
        IntervalStyle::SQLStandard
    }

    fn identifier_quote_style(&self, identifier: &str) -> Option<char> {
        DefaultDialect {}.identifier_quote_style(identifier)
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

impl DataConnectorFactory for SpiceAIFactory {
    fn create(
        &self,
        params: Parameters,
    ) -> Pin<Box<dyn Future<Output = super::NewDataConnectorResult> + Send>> {
        let default_flight_url = if cfg!(feature = "dev") {
            "https://dev-flight.spiceai.io".to_string()
        } else {
            "https://flight.spiceai.io".to_string()
        };
        Box::pin(async move {
            let url: String = params
                .get("endpoint")
                .expose()
                .ok()
                .map_or(default_flight_url, str::to_string);
            tracing::trace!("Connecting to SpiceAI with flight url: {url}");

            verify_endpoint_connection(&url).await.with_context(|_| {
                UnableToVerifyEndpointConnectionSnafu {
                    endpoint: url.to_string(),
                }
            })?;

            let api_key = params
                .get("api_key")
                .expose()
                .ok_or_else(|p| MissingRequiredParameterSnafu { parameter: p.0 }.build())?;
            let flight_client = FlightClient::try_new(url.as_str(), "", api_key)
                .await
                .context(UnableToCreateFlightClientSnafu)?;
            let flight_factory = FlightFactory::new(
                "spiceai",
                flight_client,
                Arc::new(SpiceCloudPlatformDialect {}),
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
        let mut dataset_schema = dataset.schema();
        if let Some(acceleration) = &dataset.acceleration {
            if acceleration.refresh_mode == Some(RefreshMode::Append)
                && dataset.time_column.is_none()
            {
                dataset_schema = Some(Arc::new(
                    append_stream_schema(self.flight_factory.client(), dataset.name.clone())
                        .await
                        .boxed()
                        .context(UnableToGetReadProviderSnafu {
                            dataconnector: "spiceai",
                        })?,
                ));
            }
        }

        match Read::table_provider(
            &self.flight_factory,
            SpiceAI::spice_dataset_path(dataset).into(),
            dataset_schema,
        )
        .await
        {
            Ok(provider) => Ok(provider),
            Err(e) => {
                if let Some(data_components::flight::Error::UnableToGetSchema {
                    source: _,
                    table,
                }) = e.downcast_ref::<data_components::flight::Error>()
                {
                    tracing::debug!("{e}");
                    return Err(DataConnectorError::UnableToGetSchema {
                        dataconnector: "spiceai".to_string(),
                        dataset_name: dataset.name.to_string(),
                        table_name: table.clone(),
                    });
                }

                return Err(DataConnectorError::UnableToGetReadProvider {
                    dataconnector: "spiceai".to_string(),
                    source: e,
                });
            }
        }
    }

    async fn read_write_provider(
        &self,
        dataset: &Dataset,
    ) -> Option<super::DataConnectorResult<Arc<dyn TableProvider>>> {
        let read_write_result = ReadWrite::table_provider(
            &self.flight_factory,
            SpiceAI::spice_dataset_path(dataset).into(),
            dataset.schema(),
        )
        .await
        .context(super::UnableToGetReadWriteProviderSnafu {
            dataconnector: "spiceai",
        });

        Some(read_write_result)
    }

    fn supports_append_stream(&self) -> bool {
        true
    }

    fn append_stream(&self, table_provider: Arc<dyn TableProvider>) -> Option<ChangesStream> {
        let federated_table_provider_adaptor = table_provider
            .as_any()
            .downcast_ref::<FederatedTableProviderAdaptor>()?;
        let flight_table = federated_table_provider_adaptor
            .table_provider
            .as_ref()?
            .as_any()
            .downcast_ref::<FlightTable>()?;

        let stream = Box::pin(subscribe_to_append_stream(
            flight_table.get_flight_client(),
            flight_table.get_table_reference(),
        ));

        Some(stream)
    }

    async fn catalog_provider(
        self: Arc<Self>,
        runtime: &Runtime,
        catalog: &Catalog,
    ) -> Option<super::DataConnectorResult<Arc<dyn CatalogProvider>>> {
        if catalog.catalog_id.is_some() {
            return Some(Err(
                super::DataConnectorError::InvalidConfigurationNoSource {
                    dataconnector: "spiceai".into(),
                    message: "Catalog ID is not supported for SpiceAI data connector".into(),
                },
            ));
        }

        let spice_extension = runtime.extension("spice_cloud").await?;
        let catalog_provider = spice_extension
            .catalog_provider(self, catalog.include.clone())
            .await?
            .ok()?;

        Some(Ok(catalog_provider))
    }
}

impl SpiceAI {
    /// Parses a dataset path from a Spice AI dataset definition.
    ///
    /// Spice AI datasets have several possible formats for `dataset.path()`:
    /// 1. `<org>/<app>/datasets/<dataset_name>` or `spice.ai/<org>/<app>/datasets/<dataset_name>`.
    /// 2. `<org>/<app>` or `spice.ai/<org>/<app>`.
    /// 3. `some.blessed.dataset` or `spice.ai/some.blessed.dataset`.
    ///
    /// The second format is a shorthand for the first format, where the dataset name
    /// is the same as the local table name specified in `name`.
    ///
    /// The third format is a path to a "blessed" Spice AI dataset (i.e. a dataset that is
    /// defined and provided by Spice). If the dataset path does not match the first two formats,
    /// then it is assumed to be a path to a blessed dataset.
    ///
    /// This function returns the full dataset path for the given dataset as you would query for it in Spice.
    /// i.e. `<org>.<app>.<dataset_name>`
    #[allow(clippy::match_same_arms)]
    fn spice_dataset_path<T: Borrow<Dataset>>(dataset: T) -> String {
        let dataset = dataset.borrow();
        let path = dataset.path();
        let path = path.trim_start_matches("spice.ai/");
        let path_parts: Vec<&str> = path.split('/').collect();

        match path_parts.as_slice() {
            [org, app] => format!("{org}.{app}.{dataset_name}", dataset_name = dataset.name),
            [org, app, "datasets", dataset_name] => format!("{org}.{app}.{dataset_name}"),
            [org, app, dataset_name] => format!("{org}.{app}.{dataset_name}"),
            _ => path.to_string(),
        }
    }
}

async fn append_stream_schema(
    client: FlightClient,
    table_reference: TableReference,
) -> Result<Schema> {
    let table_paths = match table_reference {
        TableReference::Bare { table } => vec![table.to_string()],
        TableReference::Partial { schema, table } => {
            vec![schema.to_string(), table.to_string()]
        }
        TableReference::Full {
            catalog,
            schema,
            table,
        } => {
            vec![catalog.to_string(), schema.to_string(), table.to_string()]
        }
    };
    let schema = client
        .get_schema(table_paths)
        .await
        .context(UnableToGetAppendSchemaSnafu)?;

    Ok(schema)
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
    fn test_spice_dataset_path() {
        let tests = vec![
            (
                "spiceai:spice.ai/lukekim/demo/datasets/my_data".to_string(),
                "lukekim.demo.my_data",
            ),
            (
                "spiceai:spice.ai/lukekim/demo/my_data".to_string(),
                "lukekim.demo.my_data",
            ),
            (
                "spiceai:lukekim/demo/datasets/my_data".to_string(),
                "lukekim.demo.my_data",
            ),
            (
                "spiceai:lukekim/demo/my_data".to_string(),
                "lukekim.demo.my_data",
            ),
            (
                "spice.ai/lukekim/demo/datasets/my_data".to_string(),
                "lukekim.demo.my_data",
            ),
            (
                "spice.ai/lukekim/demo/my_data".to_string(),
                "lukekim.demo.my_data",
            ),
            (
                "lukekim/demo/datasets/my_data".to_string(),
                "lukekim.demo.my_data",
            ),
            ("lukekim/demo/my_data".to_string(), "lukekim.demo.my_data"),
            ("eth.recent_blocks".to_string(), "eth.recent_blocks"),
        ];

        for (input, expected) in tests {
            let dataset = Dataset::try_new(input.clone(), "bar").expect("a valid dataset");
            assert_eq!(SpiceAI::spice_dataset_path(dataset), expected, "{input}");
        }
    }
}
