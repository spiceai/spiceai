use std::any::Any;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use async_trait::async_trait;
use datafusion::catalog::TableProvider;
use mongodb::Client;
use mongodb::options::{ClientOptions, Credential, ServerAddress};
use data_components::mongodb::MongoDBTableProvider;
use crate::component::dataset::Dataset;
use crate::dataconnector::{ConnectorComponent, ConnectorParams, DataConnector, DataConnectorError, DataConnectorFactory, DataConnectorResult};
use crate::parameters::{ParameterSpec, Parameters};

pub struct MongoDB {
    params: Parameters,
}

pub struct MongoDBFactory {}

impl MongoDBFactory {
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
    ParameterSpec::component("connection_string").secret(),
    ParameterSpec::component("username").secret(),
    ParameterSpec::component("password").secret(),
    ParameterSpec::component("host"),
    ParameterSpec::component("port"),
    ParameterSpec::component("auth_source"), // `authSource` in connection string
    ParameterSpec::component("query_body"),
];

impl DataConnectorFactory for MongoDBFactory {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn create(
        &self,
        params: ConnectorParams,
    ) -> Pin<Box<dyn Future<Output = super::NewDataConnectorResult> + Send>> {
        Box::pin(async move {
            let mongodb = MongoDB {
                params: params.parameters,
            };
            Ok(Arc::new(mongodb) as Arc<dyn DataConnector>)
        })
    }

    fn prefix(&self) -> &'static str {
        "mongodb"
    }

    fn parameters(&self) -> &'static [ParameterSpec] {
        PARAMETERS
    }
}

#[async_trait]
impl DataConnector for MongoDB {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn read_provider(&self, dataset: &Dataset) -> DataConnectorResult<Arc<dyn TableProvider>> {
        let path = dataset.path();
        let mut db_and_collection = path.split(".");

        let (database, collection) = match (db_and_collection.next(), db_and_collection.next()) {
            (Some(database), Some(collection)) => (database, collection),
            _ => return Err(DataConnectorError::InvalidConfigurationNoSource {
                dataconnector: "mongodb".to_string(),
                connector_component: ConnectorComponent::from(dataset),
                message: "failed to parse database_name and collection_name. The format of `from` field is `mongodb:{database_name}.{collection_name}`".to_string(),
            }),
        };

        let connection_string = self.params
            .get("connection_string")
            .expose()
            .ok()
            .map(ToString::to_string);

        let client_options = if let Some(connection_string) = connection_string {
            ClientOptions::parse(connection_string).await
                .map_err(|e| DataConnectorError::InvalidConfiguration {
                    dataconnector: "mongodb".to_string(),
                    connector_component: ConnectorComponent::from(dataset),
                    message: "failed to parse connection string".to_string(),
                    source: Box::new(e),
                })?
        } else {
            let host = self.params
                .get("host")
                .expose()
                .ok_or_else(|_| DataConnectorError::InvalidConfigurationNoSource {
                    dataconnector: "mongodb".to_string(),
                    connector_component: ConnectorComponent::from(dataset),
                    message: "host is required".to_string(),
                })?
                .to_string();

            let port = self.params
                .get("port")
                .expose()
                .ok()
                .and_then(|s| s.parse::<u16>().ok())
                .unwrap_or(27017); // default port of mongodb

            let username = self.params
                .get("username")
                .expose()
                .ok()
                .map(ToString::to_string);

            let password = self.params
                .get("password")
                .expose()
                .ok()
                .map(ToString::to_string);

            let auth_source = self.params
                .get("auth_source")
                .expose()
                .ok()
                .map(ToString::to_string);

            ClientOptions::builder()
                .hosts(
                    vec![ServerAddress::Tcp {
                        host,
                        port: Some(port),
                    }])
                .credential(
                    Credential::builder()
                        .username(username)
                        .password(password)
                        .source(auth_source)// `--authenticationDatabase` in mongodb shell, `authSource` in connection string
                        .build()
                ).build()
        };

        let client = Client::with_options(client_options)
            .map_err(|e| DataConnectorError::UnableToConnectInternal {
                dataconnector: "mongodb".to_string(),
                connector_component: ConnectorComponent::from(dataset),
                source: Box::new(e),
            })?;

        let query_body = self.params
            .get("query_body")
            .expose()
            .ok()
            .unwrap_or("{}"); // empty query body means selecting all fields in collection

        let provider = MongoDBTableProvider::try_new(Arc::new(client), Arc::from(database), Arc::from(collection), Arc::from(query_body))
            .await
            .map_err(|e| DataConnectorError::UnableToGetReadProvider {
                dataconnector: "mongodb".to_string(),
                connector_component: ConnectorComponent::from(dataset),
                source: Box::new(e),
            })?;

        Ok(Arc::new(provider))
    }
}