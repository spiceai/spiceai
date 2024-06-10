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

use crate::component::dataset::Dataset;
use arrow::{array::RecordBatch, datatypes::SchemaRef, error::ArrowError};
use arrow_json::{reader::infer_json_schema_from_iterator, ReaderBuilder};
use async_trait::async_trait;
use data_components::arrow::write::MemTable;
use datafusion::{
    datasource::{TableProvider, TableType},
    error::DataFusionError,
    execution::context::SessionState,
    logical_expr::Expr,
    physical_plan::ExecutionPlan,
};
use futures::TryFutureExt;
use regex::Regex;
use reqwest::{
    header::{HeaderMap, HeaderValue, CONTENT_TYPE, USER_AGENT},
    RequestBuilder, StatusCode,
};
use secrets::{get_secret_or_param, Secret};
use serde_json::{json, Value};
use snafu::{ResultExt, Snafu};
use std::{any::Any, collections::HashMap, future::Future, io::Cursor, pin::Pin, sync::Arc};
use url::Url;

use super::{DataConnector, DataConnectorFactory};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("{source}"))]
    ReqwestInternal { source: reqwest::Error },

    #[snafu(display("HTTP {status}: {message}"))]
    InvalidReqwestStatus {
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
    InvalidGraphQLQuery {
        message: String,
        line: usize,
        column: usize,
        query: String,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, PartialEq, Eq)]
struct PaginationParameters {
    resource_name: String,
    count: usize,
    page_info_path: String,
}

impl PaginationParameters {
    fn parse(query: &str, pointer: &str) -> Option<Self> {
        let pagination_pattern = r"(?xsm)(\w+)\s*\([^)]*first:\s*(\d+)[^)]*\)\s*\{.*pageInfo\s*\{.*(?:hasNextPage.*endCursor|endCursor.*hasNextPage).*\}.*\}";
        let regex = Regex::new(pagination_pattern)
            .unwrap_or_else(|_| panic!("Invalid regex pagination pattern"));
        match regex.captures(query) {
            Some(captures) => {
                let resource_name = captures.get(1).map(|m| m.as_str().to_owned());
                let count = captures
                    .get(2)
                    .map(|m| m.as_str().parse::<usize>())
                    .transpose()
                    .unwrap_or(None);

                match (resource_name, count) {
                    (Some(resource_name), Some(count)) => {
                        let pattern = format!(r"^(.*?{resource_name})");
                        let regex = Regex::new(pattern.as_str())
                            .unwrap_or_else(|_| panic!("Invalid regex query resource pattern"));

                        let captures = regex.captures(pointer);

                        let page_info_path = captures
                            .and_then(|c| c.get(1).map(|m| m.as_str().to_owned() + "/pageInfo"));

                        page_info_path.map(|page_info_path| Self {
                            resource_name,
                            count,
                            page_info_path,
                        })
                    }
                    _ => None,
                }
            }
            None => None,
        }
    }

    fn apply(&self, query: &str, limit: Option<usize>, cursor: Option<String>) -> (String, bool) {
        let mut limit_reached = false;

        let mut count = self.count;

        if let Some(limit) = limit {
            if limit <= count {
                count = limit;
                limit_reached = true;
            }
        }
        let pattern = format!(r#"{}\s*\(.*\)"#, self.resource_name);
        let regex =
            Regex::new(&pattern).unwrap_or_else(|_| panic!("Invalid regex query resource pattern"));

        let replace_query = if let Some(cursor) = cursor {
            format!(
                r#"{} (first: {count}, after: "{cursor}")"#,
                self.resource_name
            )
        } else {
            format!(r#"{} (first: {count})"#, self.resource_name)
        };

        let new_query = regex.replace(query, replace_query.as_str());
        (new_query.to_string(), limit_reached)
    }

    fn get_next_cursor_from_response(&self, response: &Value) -> Option<String> {
        let page_info = response
            .pointer(&self.page_info_path)
            .unwrap_or(&Value::Null);
        let end_cursor = page_info["endCursor"].as_str().map(ToString::to_string);
        let has_next_page = page_info["hasNextPage"].as_bool().unwrap_or(false);

        if has_next_page {
            end_cursor
        } else {
            None
        }
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
    pointer: String,
    pagination_parameters: Option<PaginationParameters>,
    auth: Option<Auth>,
}

impl GraphQLClient {
    fn new(
        client: reqwest::Client,
        endpoint: Url,
        query: String,
        pointer: String,
        pagination_parameters: Option<PaginationParameters>,
        auth: Option<Auth>,
    ) -> Self {
        Self {
            client,
            endpoint,
            query,
            pointer,
            pagination_parameters,
            auth,
        }
    }
}

impl GraphQLClient {
    async fn execute(
        &self,
        schema: Option<SchemaRef>,
        limit: Option<usize>,
        cursor: Option<String>,
    ) -> Result<(Vec<RecordBatch>, SchemaRef, Option<String>)> {
        let (query, limit_reached) = self
            .pagination_parameters
            .as_ref()
            .map(|x| x.apply(&self.query, limit, cursor))
            .unwrap_or((self.query.to_string(), false));

        let body = format!(r#"{{"query": {}}}"#, json!(query));

        let mut request = self.client.post(self.endpoint.clone()).body(body);
        request = request_with_auth(request, &self.auth);

        let response = request.send().await.context(ReqwestInternalSnafu)?;
        let status = response.status();
        let response: serde_json::Value = response.json().await.context(ReqwestInternalSnafu)?;

        handle_http_error(status, &response)?;
        handle_graphql_query_error(&response, &query)?;

        let exctracted_data = response
            .pointer(self.pointer.as_str())
            .unwrap_or(&Value::Null)
            .to_owned();

        let next_cursor = match limit_reached {
            true => None,
            false => self
                .pagination_parameters
                .as_ref()
                .and_then(|x| x.get_next_cursor_from_response(&response)),
        };

        let unwrapped = match exctracted_data {
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

    async fn execute_paginated(
        &self,
        schema: SchemaRef,
        limit: Option<usize>,
    ) -> Result<Vec<Vec<RecordBatch>>> {
        let (first_batch, _, mut next_cursor) =
            self.execute(Some(Arc::clone(&schema)), limit, None).await?;
        let mut res = vec![first_batch];
        let mut limit = limit;

        while let Some(next_cursor_val) = next_cursor {
            limit = limit.map(|l| {
                l.saturating_sub(self.pagination_parameters.as_ref().map_or(0, |x| x.count))
            });
            let (next_batch, _, new_cursor) = self
                .execute(Some(Arc::clone(&schema)), limit, Some(next_cursor_val))
                .await?;
            next_cursor = new_cursor;
            res.push(next_batch);
        }

        Ok(res)
    }
}

fn request_with_auth(request_builder: RequestBuilder, auth: &Option<Auth>) -> RequestBuilder {
    match &auth {
        Some(Auth::Basic(user, pass)) => request_builder.basic_auth(user, pass.clone()),
        Some(Auth::Bearer(token)) => request_builder.bearer_auth(token),
        _ => request_builder,
    }
}

fn handle_http_error(status: StatusCode, response: &Value) -> Result<()> {
    if status.is_client_error() | status.is_server_error() {
        let message = [
            &response["message"],
            &response["error"]["message"],
            &response["errors"][0]["message"],
        ]
        .iter()
        .map(|x| x.as_str())
        .find(Option::is_some)
        .flatten()
        .unwrap_or("No message provided")
        .to_string();

        return Err(Error::InvalidReqwestStatus { status, message });
    }
    Ok(())
}

fn handle_graphql_query_error(response: &Value, query: &str) -> Result<()> {
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
        return Err(Error::InvalidGraphQLQuery {
            message: graphql_error["message"]
                .as_str()
                .unwrap_or_default()
                .split(" at [")
                .next()
                .unwrap_or_default()
                .to_string(),
            line,
            column,
            query: format_query_with_context(query, line, column),
        });
    }
    Ok(())
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

struct GraphQLTableProvider {
    client: GraphQLClient,
    schema: SchemaRef,
}

impl GraphQLTableProvider {
    async fn new(client: GraphQLClient) -> Result<Self> {
        let (_, schema, _) = client.execute(None, None, None).await?;

        Ok(Self { client, schema })
    }
}

#[async_trait]
impl TableProvider for GraphQLTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
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
        let res = self
            .client
            .execute_paginated(Arc::clone(&self.schema), limit)
            .map_err(|e| DataFusionError::Execution(format!("{e}")))
            .await?;

        let table = MemTable::try_new(Arc::clone(&self.schema), res)?;

        table.scan(state, projection, filters, limit).await
    }
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
        let pointer = format!("/{}", json_path.replace('.', "/"));

        let pagination_parameters = PaginationParameters::parse(&query, &pointer);

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

        Ok(GraphQLClient::new(
            client_builder.build().map_err(|e| {
                super::DataConnectorError::InvalidConfiguration {
                    dataconnector: "GraphQL".to_string(),
                    message: "Failed to set token".to_string(),
                    source: e.into(),
                }
            })?,
            endpoint,
            query,
            pointer,
            pagination_parameters,
            auth,
        ))
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

        Ok(Arc::new(
            GraphQLTableProvider::new(client)
                .await
                .map_err(Into::into)
                .context(super::InternalWithSourceSnafu {
                    dataconnector: "GraphQL".to_string(),
                })?,
        ))
    }
}

#[cfg(test)]
mod tests {
    use reqwest::StatusCode;

    use super::handle_http_error;
    use super::PaginationParameters;

    #[test]
    fn test_pagination_parse() {
        let query = r"query {
      users(first: 10) {
        pageInfo {
          hasNextPage
          endCursor
        }
        }
    }";
        let pointer = r"/data/users";
        let pagination_parameters = PaginationParameters::parse(query, pointer);
        assert_eq!(
            pagination_parameters,
            Some(PaginationParameters {
                resource_name: "users".to_owned(),
                count: 10,
                page_info_path: "/data/users/pageInfo".to_owned(),
            })
        );

        let query = r"query {
            users(first: 10) {
              pageInfo {
                endCursor
                hasNextPage
              }
              }
          }";
        let pointer = r"/data/users";
        let pagination_parameters = PaginationParameters::parse(query, pointer);
        assert_eq!(
            pagination_parameters,
            Some(PaginationParameters {
                resource_name: "users".to_owned(),
                count: 10,
                page_info_path: "/data/users/pageInfo".to_owned(),
            })
        );

        let query = r"query {
            users(first: 10) {
                name
              }
          }";
        let pointer = r"/data/users";
        let pagination_parameters = PaginationParameters::parse(query, pointer);
        assert_eq!(pagination_parameters, None);

        let query = r"query { paginatedUsers(first: 2) { users { id name posts { id title content } } pageInfo { hasNextPage endCursor } } }";
        let pointer = r"/data/paginatedUsers/users";
        let pagination_parameters = PaginationParameters::parse(query, pointer);
        assert_eq!(
            pagination_parameters,
            Some(PaginationParameters {
                resource_name: "paginatedUsers".to_owned(),
                count: 2,
                page_info_path: "/data/paginatedUsers/pageInfo".to_owned(),
            })
        );
    }

    #[test]
    fn test_pagination_apply() {
        let query = r"query {
            users(first: 10) {
                name
                pageInfo {
                    hasNextPage
                    endCursor
                }
            }
        }";
        let pointer = r"/data/users";
        let pagination_parameters =
            PaginationParameters::parse(query, pointer).expect("Failed to get pagination params");
        let (new_query, limit_reached) =
            pagination_parameters.apply(query, None, Some("new_cursor".to_string()));
        let expected_query = r#"query {
            users (first: 10, after: "new_cursor") {
                name
                pageInfo {
                    hasNextPage
                    endCursor
                }
            }
        }"#;
        assert!(!limit_reached);
        assert_eq!(new_query, expected_query);

        let query = r#"query {
            users(after: "user_cursor", first: 10) {
                name
                pageInfo {
                    hasNextPage
                    endCursor
                }
            }
        }"#;
        let pointer = r"/data/users";
        let pagination_parameters =
            PaginationParameters::parse(query, pointer).expect("Failed to get pagination params");
        let (new_query, limit_reached) =
            pagination_parameters.apply(query, None, Some("new_cursor".to_string()));
        let expected_query = r#"query {
            users (first: 10, after: "new_cursor") {
                name
                pageInfo {
                    hasNextPage
                    endCursor
                }
            }
        }"#;
        assert!(!limit_reached);
        assert_eq!(new_query, expected_query);

        let query = r"query {
            users(first: 10) {
                name
                pageInfo {
                    hasNextPage
                    endCursor
                }
            }
        }";
        let pointer = r"/data/users";
        let pagination_parameters =
            PaginationParameters::parse(query, pointer).expect("Failed to get pagination params");
        let (new_query, limit_reached) =
            pagination_parameters.apply(query, Some(5), Some("new_cursor".to_string()));
        let expected_query = r#"query {
            users (first: 5, after: "new_cursor") {
                name
                pageInfo {
                    hasNextPage
                    endCursor
                }
            }
        }"#;
        assert!(limit_reached);
        assert_eq!(new_query, expected_query);
    }

    #[test]
    fn test_pagination_get_next_cursor_from_response() {
        let query = r"query {
            users(first: 10) {
                name
                pageInfo {
                    hasNextPage
                    endCursor
                }
            }
        }";
        let pointer = r"/data/users";
        let pagination_parameters =
            PaginationParameters::parse(query, pointer).expect("Failed to get pagination params");

        let response = serde_json::from_str(
            r#"{
            "data": {
                "users": {
                    "pageInfo": {
                        "hasNextPage": true,
                        "endCursor": "new_cursor"
                    }
                }
            }
        }"#,
        )
        .expect("Invalid json");

        let next_cursor = pagination_parameters.get_next_cursor_from_response(&response);
        assert_eq!(
            next_cursor,
            Some("new_cursor".to_string()),
            "Expected next cursor to be new_cursor"
        );

        let response = serde_json::from_str(
            r#"{
            "data": {
                "users": {

                }
            }
        }"#,
        )
        .expect("Invalid json");
        let next_cursor = pagination_parameters.get_next_cursor_from_response(&response);
        assert_eq!(next_cursor, None, "Should be None if no value returned");
    }

    #[test]
    fn test_handle_http_error() {
        let message = "test message";
        let response = serde_json::from_str(&format!(r#"{{"message": "{message}"}}"#))
            .expect("Failed to consturuct json");
        let status = StatusCode::BAD_REQUEST;
        let result = handle_http_error(status, &response);
        match result {
            Ok(()) => panic!("Expected error"),
            Err(e) => {
                assert!(e.to_string().contains(message));
            }
        }

        let response =
            serde_json::from_str(&format!(r#"{{ "error": {{"message": "{message}"}} }}"#))
                .expect("Failed to consturuct json");
        let status = StatusCode::BAD_REQUEST;
        let result = handle_http_error(status, &response);
        match result {
            Ok(()) => panic!("Expected error"),
            Err(e) => {
                assert!(e.to_string().contains(message));
            }
        }

        let response =
            serde_json::from_str(&format!(r#"{{ "errors": [{{"message": "{message}"}}] }}"#))
                .expect("Failed to consturuct json");
        let status = StatusCode::BAD_REQUEST;
        let result = handle_http_error(status, &response);
        match result {
            Ok(()) => panic!("Expected error"),
            Err(e) => {
                assert!(e.to_string().contains(message));
            }
        }
    }
}
