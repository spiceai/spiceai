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
use arrow::datatypes::Schema;
use arrow_json::reader::infer_json_schema_from_iterator;
use arrow_json::ReaderBuilder;
use async_trait::async_trait;
use datafusion::execution::context::SessionContext;
use secrets::Secret;

use crate::component::dataset::Dataset;
use datafusion::datasource::{MemTable, TableProvider};
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

impl GraphQL {
    pub async fn connect(&self, dataset: &Dataset) -> (Arc<Schema>, Vec<Vec<RecordBatch>>) {
        let host = dataset.path();
        let client = reqwest::Client::new();

        let response = client
            .post(host)
            .header("Content-Type", "application/json")
            .header("User-Agent", "spice")
            .bearer_auth("token")
            .body(r#"{"query": "query {viewer {repositories(first: 100) {nodes {id, name}}}}"}"#)
            .send()
            .await
            .unwrap();
        let response: serde_json::Value = response.json().await.unwrap();
        let response = &response["data"]["viewer"]["repositories"]["nodes"];
        let first = response[0].clone();

        let schema =
            Arc::new(infer_json_schema_from_iterator(vec![Ok(first.clone())].into_iter()).unwrap());

        let mut res = vec![];
        if let serde_json::Value::Array(value) = response {
            for v in value {
                let buf = v.to_string();
                let batch = ReaderBuilder::new(schema.clone())
                    .with_batch_size(1024)
                    .build(Cursor::new(buf.as_bytes()))
                    .unwrap()
                    .collect::<Result<Vec<_>, _>>()
                    .unwrap();
                res.push(batch);
            }
        }

        (schema, res)
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
        let ctx = SessionContext::new();
        let (schema, partitions) = self.connect(dataset).await;
        let table = Arc::new(MemTable::try_new(schema, partitions).unwrap());

        let df = ctx.read_table(table).unwrap();

        Ok(df.into_view())
    }
}
