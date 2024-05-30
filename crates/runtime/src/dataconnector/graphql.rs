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
use arrow_json::reader::infer_json_schema_from_iterator;
use arrow_json::ReaderBuilder;
use async_trait::async_trait;
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::ExecutionPlan;
use itertools::Itertools;
use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION};
use reqwest::header::{CONTENT_TYPE, USER_AGENT};
use secrets::{get_secret_or_param, Secret};
use serde_json::Value;

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
pub enum Error {}

pub type Result<T, E = Error> = std::result::Result<T, E>;

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

struct GraphQLClient {
    client: reqwest::Client,
    endpoint: String,
    query: String,
    json_path: String,
}

impl GraphQLClient {
    async fn execute(&self) -> (Vec<Vec<RecordBatch>>, SchemaRef) {
        let body = format!(r#"{{"query": "{}"}}"#, self.query.lines().join(" "));
        let response = self
            .client
            .post(&self.endpoint)
            .body(body)
            .send()
            .await
            .unwrap();

        let mut response: serde_json::Value = response.json().await.unwrap();
        for key in self.json_path.split('.') {
            response = response[key].clone();
        }

        let unwraped = match response {
            Value::Array(val) => val.clone(),
            obj @ Value::Object(_) => vec![obj.clone()],
            _ => unimplemented!(),
        };

        let schema = Arc::new(
            infer_json_schema_from_iterator(unwraped.clone().into_iter().map(Result::Ok)).unwrap(),
        );

        let mut res = vec![];
        for v in unwraped {
            let buf = v.to_string();
            let batch = ReaderBuilder::new(schema.clone())
                .with_batch_size(1024)
                .build(Cursor::new(buf.as_bytes()))
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            res.push(batch);
        }

        (res, schema)
    }
}

impl GraphQL {
    async fn get_client(&self, dataset: &Dataset) -> GraphQLClient {
        let mut client_builder = reqwest::Client::builder();
        let token = get_secret_or_param(&self.params, &self.secret, "auth_token_key", "auth_token")
            .unwrap();
        let query = self.params.get("query").unwrap().clone();
        let endpoint = dataset.path().clone();
        let json_path = self.params.get("json_path").unwrap().clone();

        let mut headers = HeaderMap::new();
        headers.append(USER_AGENT, HeaderValue::from_static("spice"));
        headers.append(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        headers.append(
            AUTHORIZATION,
            HeaderValue::from_str(&format!("bearer {token}")).unwrap(),
        );

        client_builder = client_builder.default_headers(headers);

        GraphQLClient {
            client: client_builder.build().unwrap(),
            query,
            endpoint,
            json_path,
        }
    }
}

struct GraphQLTableProvider {
    client: GraphQLClient,
    schema: SchemaRef,
}

impl GraphQLTableProvider {
    pub async fn new(client: GraphQLClient) -> Result<Self> {
        let (_, schema) = client.execute().await;

        Ok(Self { client, schema })
    }
}

#[async_trait]
impl TableProvider for GraphQLTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
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
        let (res, schema) = self.client.execute().await;
        let table = MemTable::try_new(schema, res).unwrap();

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
        let client = self.get_client(&dataset).await;

        Ok(Arc::new(GraphQLTableProvider::new(client).await.unwrap()))
    }
}
