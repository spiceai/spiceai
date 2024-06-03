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

use arrow::array::RecordBatch;
use arrow::datatypes::{Schema, SchemaRef};
use arrow::error::ArrowError;
use arrow_json::reader::infer_json_schema_from_iterator;
use arrow_json::ReaderBuilder;
use async_trait::async_trait;
use datafusion::error::DataFusionError;
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::ExecutionPlan;
use itertools::Itertools;
use reqwest::header::{HeaderMap, HeaderValue};
use reqwest::header::{CONTENT_TYPE, USER_AGENT};
use secrets::{get_secret_or_param, Secret};
use serde_json::Value;
use url::Url;

use crate::component::dataset::Dataset;
use datafusion::datasource::{MemTable, TableProvider, TableType};
use snafu::prelude::*;
use std::any::Any;
use std::io::Cursor;
use std::pin::Pin;
use std::sync::Arc;
use std::{collections::HashMap, future::Future};

use super::{DataConnector, DataConnectorFactory};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to handle request: {source}"))]
    UnableToHandleRequest { source: reqwest::Error },

    #[snafu(display("Failed to request data:\n {response}"))]
    FailedToRequestData { response: String },

    #[snafu(display("Unable to parse response: {source}"))]
    UnableToParseResponse { source: reqwest::Error },

    #[snafu(display("Unable to read batch: {source}"))]
    UnableToReadBatch { source: ArrowError },

    #[snafu(display("Unable to collect batch: {source}"))]
    UnableToCollectBatch { source: ArrowError },

    #[snafu(display("Unable to infer schema: {source}"))]
    UnableToInferSchema { source: ArrowError },

    #[snafu(display("Invalid object access: {message}"))]
    InvalidObjectAccess { message: String },
}

pub struct GraphQL {
    secret: Option<Secret>,
    params: Arc<HashMap<String, String>>,
}

impl DataConnectorFactory for GraphQL {
    fn create(
        secret: Option<Secret>,
        params: Arc<HashMap<String, String>>,
    ) -> Pin<Box<dyn Future<Output = super::NewDataConnectorResult> + Send>> {
        Box::pin(async move {
            let graphql = Self { secret, params };
            Ok(Arc::new(graphql) as Arc<dyn DataConnector>)
        })
    }
}

enum Auth {
    Basic(String, Option<String>),
    Bearer(String),
}

struct GraphQLClient {
    client: reqwest::Client,
    endpoint: Url,
    query: String,
    json_path: String,
    auth: Option<Auth>,
}

impl GraphQLClient {
    async fn execute(&self) -> datafusion::error::Result<(Vec<Vec<RecordBatch>>, SchemaRef)> {
        let body = format!(r#"{{"query": "{}"}}"#, self.query.lines().join(" "));
        let mut request = self.client.post(self.endpoint.clone()).body(body);

        match &self.auth {
            Some(Auth::Basic(user, pass)) => {
                request = request.basic_auth(user, pass.clone());
            }
            Some(Auth::Bearer(token)) => {
                request = request.bearer_auth(token);
            }
            _ => {}
        }

        let response = request
            .send()
            .await
            .context(UnableToHandleRequestSnafu)
            .map_err(to_execution_error)?;

        if !response.status().is_success() {
            return Err(Error::FailedToRequestData {
                response: response
                    .json::<serde_json::Value>()
                    .await
                    .map(|v| serde_json::to_string_pretty(&v).unwrap_or_default())
                    .unwrap_or_default(),
            })
            .map_err(to_execution_error);
        }

        let mut response: serde_json::Value = response
            .json()
            .await
            .context(UnableToParseResponseSnafu)
            .map_err(to_execution_error)?;

        for key in self.json_path.split('.') {
            response = response[key].clone();
        }

        let unwrapped = match response {
            Value::Array(val) => Ok(val.clone()),
            obj @ Value::Object(_) => Ok(vec![obj.clone()]),
            Value::Null => Err(Error::InvalidObjectAccess {
                message: "Found null value".to_string(),
            }),
            _ => Err(Error::InvalidObjectAccess {
                message: "Found primitive value".to_string(),
            }),
        };

        let unwrapped = unwrapped.map_err(to_execution_error)?;

        let schema = Arc::new(
            infer_json_schema_from_iterator(unwrapped.clone().into_iter().map(Result::Ok))
                .context(UnableToInferSchemaSnafu)
                .map_err(to_execution_error)?,
        );

        let mut res = vec![];
        for v in unwrapped {
            let buf = v.to_string();
            let batch = ReaderBuilder::new(Arc::clone(&schema))
                .with_batch_size(1024)
                .build(Cursor::new(buf.as_bytes()))
                .context(UnableToReadBatchSnafu)
                .map_err(to_execution_error)?
                .collect::<Result<Vec<_>, _>>()
                .context(UnableToCollectBatchSnafu)
                .map_err(to_execution_error)?;
            res.push(batch);
        }

        Ok((res, schema))
    }
}

#[allow(clippy::needless_pass_by_value)]
fn to_execution_error(e: Error) -> DataFusionError {
    DataFusionError::Execution(format!("{e}"))
}

impl GraphQL {
    fn get_client(&self, dataset: &Dataset) -> super::DataConnectorResult<GraphQLClient> {
        let mut client_builder = reqwest::Client::builder();
        let token = get_secret_or_param(&self.params, &self.secret, "auth_token_key", "auth_token");
        let user = get_secret_or_param(&self.params, &self.secret, "auth_user_key", "auth_user");
        let pass = get_secret_or_param(&self.params, &self.secret, "auth_pass_key", "auth_pass");

        let query = self
            .params
            .get("query")
            .ok_or("Query not found in params".into())
            .context(super::InvalidConfigurationSnafu {
                dataconnector: "GraphQL",
                message: "Query not found",
            })?
            .to_owned();
        let endpoint = Url::parse(&dataset.path()).map_err(Into::into).context(
            super::InvalidConfigurationSnafu {
                dataconnector: "GraphQL",
                message: "Invalid URL",
            },
        )?;
        let json_path = self
            .params
            .get("json_path")
            .map_or("data".to_string(), ToOwned::to_owned);

        let mut headers = HeaderMap::new();
        headers.append(USER_AGENT, HeaderValue::from_static("spice"));
        headers.append(CONTENT_TYPE, HeaderValue::from_static("application/json"));

        let mut auth = None;
        if let Some(token) = token {
            auth = Some(Auth::Bearer(token));
        }
        if let Some(user) = user {
            auth = Some(Auth::Basic(user, pass));
        }

        client_builder = client_builder.default_headers(headers);

        Ok(GraphQLClient {
            client: client_builder.build().map_err(|e| {
                super::DataConnectorError::InvalidConfiguration {
                    dataconnector: "GraphQL".to_string(),
                    message: "Failed to set token".to_string(),
                    source: e.into(),
                }
            })?,
            query,
            endpoint,
            json_path,
            auth,
        })
    }
}

struct GraphQLTableProvider {
    client: GraphQLClient,
    schema: SchemaRef,
}

impl GraphQLTableProvider {
    pub async fn new(client: GraphQLClient) -> super::DataConnectorResult<Self> {
        let (_, schema) = client.execute().await.map_err(Into::into).context(
            super::UnableToHandleRequestSnafu {
                dataconnector: "GraphQL",
            },
        )?;

        Ok(Self { client, schema })
    }
}

#[async_trait]
impl TableProvider for GraphQLTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &SessionState,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let (res, schema) = self.client.execute().await?;
        let table = MemTable::try_new(schema, res)?;

        table.scan(state, projection, filters, limit).await
    }
}

#[async_trait]
impl DataConnector for GraphQL {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn read_provider(
        &self,
        dataset: &Dataset,
    ) -> super::DataConnectorResult<Arc<dyn TableProvider>> {
        let client = self.get_client(dataset)?;

        Ok(Arc::new(GraphQLTableProvider::new(client).await?))
    }
}
