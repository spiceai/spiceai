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
use opentelemetry_proto::tonic::resource;
use regex::{NoExpand, Regex};
use reqwest::header::{HeaderMap, HeaderValue};
use reqwest::header::{CONTENT_TYPE, USER_AGENT};
use secrets::{get_secret_or_param, Secret};
use serde_json::{json, Value};
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
    #[snafu(display("{source}"))]
    ReqwestInternal { source: reqwest::Error },

    #[snafu(display("HTTP {status}: {message}"))]
    ReqwestError {
        status: reqwest::StatusCode,
        message: String,
    },

    #[snafu(display("{source}"))]
    ArrowInternal { source: ArrowError },

    #[snafu(display("Invalid object access. {message}"))]
    InvalidObjectAccess { message: String },

    #[snafu(display(
        r#"GraphQL Query Error:
Details:
- Error: {message}
- Location: Line {line}, Column {column}
- Query:

{query}

Please verify the syntax of your GraphQL query."#
    ))]
    GraphQLError {
        message: String,
        line: usize,
        column: usize,
        query: String,
    },
}

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

enum Auth {
    Basic(String, Option<String>),
    Bearer(String),
}

struct GraphQLClient {
    client: reqwest::Client,
    endpoint: Url,
    query: String,
    json_path: String,
    paginated_resource_name: Option<String>,
    auth: Option<Auth>,
}

fn format_query_with_context(query: &str, line: usize, column: usize) -> String {
    let query_lines: Vec<&str> = query.split('\n').collect();
    let error_line = query_lines.get(line - 1).unwrap_or(&"");
    let marker = " ".repeat(column - 1) + "^";
    if line > 1 {
        format!(
            "{:>4} | {}\n{:>4} | {}\n{:>4} | {}",
            line - 1,
            query_lines[line - 2],
            line,
            error_line,
            "",
            marker
        )
    } else {
        format!("{:>4} | {}\n{:>4} | {}", line, error_line, "", marker)
    }
}

impl GraphQLClient {
    async fn execute(
        &self,
        schema: Option<SchemaRef>,
        limit: Option<usize>,
        cursor: Option<String>,
    ) -> Result<(Vec<RecordBatch>, SchemaRef, Option<String>)> {
        let query = match (cursor, self.paginated_resource_name.as_ref()) {
            (Some(cursor), Some(resource)) => {
                let pattern = format!(r#"{resource}\s*\(.*\)"#,);
                let regex = Regex::new(&pattern).unwrap();
                let count_pattern = format!(r#"{resource}\s*\(.*first:.*?(\d+).*\)"#,);

                let regex_count = Regex::new(&count_pattern).unwrap();
                let captures = regex_count.captures(&self.query);
                let count = captures
                    .map(|c| c.get(1).map(|m| m.as_str().to_owned()))
                    .flatten()
                    .unwrap();

                let new_query = regex.replace(
                    &self.query,
                    format!(r#"{resource} (first: {count}, after: "{cursor}")"#).as_str(),
                );
                new_query.to_string()
            }
            _ => self.query.clone(),
        };

        let body = format!(r#"{{"query": {}}}"#, json!(query));

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

        let response = request.send().await.context(ReqwestInternalSnafu)?;
        let status = response.status();

        let response: serde_json::Value = response.json().await.context(ReqwestInternalSnafu)?;

        if status.is_client_error() | status.is_server_error() {
            return Err(Error::ReqwestError {
                status,
                message: response["message"]
                    .as_str()
                    .unwrap_or("No message provided")
                    .to_string(),
            });
        }

        let graphql_error = &response["errors"][0];
        if !graphql_error.is_null() {
            let line = usize::try_from(graphql_error["locations"][0]["line"].as_u64().unwrap_or(0))
                .unwrap_or_default();
            let column = usize::try_from(
                graphql_error["locations"][0]["column"]
                    .as_u64()
                    .unwrap_or(0),
            )
            .unwrap_or_default();
            return Err(Error::GraphQLError {
                message: graphql_error["message"]
                    .as_str()
                    .unwrap_or_default()
                    .split(" at [")
                    .next()
                    .unwrap_or_default()
                    .to_string(),
                line,
                column,
                query: format_query_with_context(&self.query, line, column),
            });
        }

        let pointer = format!("/{}", self.json_path.replace(".", "/"));

        let next_cursor = match &self.paginated_resource_name {
            Some(resource) => {
                let page_info = {
                    // trim path to paginated resource
                    let pattern = format!(r"^(.*?{resource})");
                    let regex = Regex::new(pattern.as_str()).unwrap();
                    let captures = regex.captures(&pointer);
                    let path = captures.map_or(None, |c| {
                        c.get(1).map(|m| m.as_str().to_owned() + "/pageInfo")
                    });

                    match path {
                        Some(path) => response.pointer(path.as_str()).unwrap_or(&Value::Null),
                        None => &Value::Null,
                    }
                };
                let end_cursor = page_info
                    .pointer("/endCursor")
                    .map(|v| v.as_str().to_owned())
                    .flatten()
                    .map(|v| v.to_string());
                let has_next_page = page_info
                    .pointer("/hasNextPage")
                    .map(|v| v.as_bool().unwrap_or(false));
                if has_next_page.unwrap_or(false) {
                    end_cursor
                } else {
                    None
                }
            }
            None => None,
        };

        let response = response
            .pointer(pointer.as_str())
            .unwrap_or(&Value::Null)
            .to_owned();

        let unwrapped = match response {
            Value::Array(val) => Ok(val.clone()),
            obj @ Value::Object(_) => Ok(vec![obj]),
            Value::Null => Err(Error::InvalidObjectAccess {
                message: "Null value access.".to_string(),
            }),
            _ => Err(Error::InvalidObjectAccess {
                message: "Primitive value access.".to_string(),
            }),
        }?;

        let schema = schema.unwrap_or(Arc::new(
            infer_json_schema_from_iterator(unwrapped.iter().map(Result::Ok))
                .context(ArrowInternalSnafu)?,
        ));

        let mut res = vec![];
        for v in unwrapped {
            let buf = v.to_string();
            let batch = ReaderBuilder::new(Arc::clone(&schema))
                .with_batch_size(1024)
                .build(Cursor::new(buf.as_bytes()))
                .context(ArrowInternalSnafu)?
                .collect::<Result<Vec<_>, _>>()
                .context(ArrowInternalSnafu)?;
            res.extend(batch);
        }

        Ok((res, schema, next_cursor))
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
            .ok_or("`query` not found in params".into())
            .context(super::InvalidConfigurationSnafu {
                dataconnector: "GraphQL",
                message: "`query` not found in params",
            })?
            .to_owned();
        let endpoint = Url::parse(&dataset.path()).map_err(Into::into).context(
            super::InvalidConfigurationSnafu {
                dataconnector: "GraphQL",
                message: "Invalid URL in dataset `from` definition",
            },
        )?;
        let json_path = self
            .params
            .get("json_path")
            .ok_or("`json_path` not found in params".into())
            .context(super::InvalidConfigurationSnafu {
                dataconnector: "GraphQL",
                message: "`json_path` not found in params",
            })?
            .to_owned();

        // Check if first: and pageInfo are in the query and on the same level, capture the name of the query
        // Check if the name of the query is in the json_path
        let paginated_resource_name = {
            let pagination_pattern =
                r"(?xsm)(\w*)\(first:.*\).*\{.*pageInfo.*\{.*hasNextPage.*endCursor.*?\}.*?\}";
            let regex = unsafe { Regex::new(pagination_pattern).unwrap_unchecked() };
            match regex.captures(query.as_str()) {
                Some(captures) => {
                    let query_name = captures.get(1).map(|m| m.as_str().to_owned());
                    query_name.map_or(None, |name| {
                        if json_path.contains(&name) {
                            Some(name)
                        } else {
                            None
                        }
                    })
                }
                None => None,
            }
        };

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
            paginated_resource_name,
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
        let (_, schema, _) = client
            .execute(None, None, None)
            .await
            .map_err(Into::into)
            .context(super::InternalWithSourceSnafu {
                dataconnector: "GraphQL",
            })?;

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
        let (first_batch, _, mut next_cursor) = self
            .client
            .execute(Some(Arc::clone(&self.schema)), limit, None)
            .await
            .map_err(to_execution_error)?;
        let mut res = vec![first_batch];
        while let Some(next_cursor_val) = next_cursor {
            let (next_batch, _, new_cursor) = self
                .client
                .execute(Some(Arc::clone(&self.schema)), limit, Some(next_cursor_val))
                .await
                .map_err(to_execution_error)?;
            next_cursor = new_cursor;
            res.push(next_batch);
        }

        let table = MemTable::try_new(Arc::clone(&self.schema), res)?;

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
