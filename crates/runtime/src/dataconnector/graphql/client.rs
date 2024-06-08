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

use std::{io::Cursor, sync::Arc};

use arrow::{array::RecordBatch, datatypes::SchemaRef, error::ArrowError};
use arrow_json::{reader::infer_json_schema_from_iterator, ReaderBuilder};
use reqwest::{RequestBuilder, StatusCode};
use serde_json::{json, Value};
use snafu::{ResultExt, Snafu};
use url::Url;

use super::pagination::PaginationParameters;

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

pub enum Auth {
    Basic(String, Option<String>),
    Bearer(String),
}

#[allow(clippy::module_name_repetitions)]
pub struct GraphQLClient {
    client: reqwest::Client,
    endpoint: Url,
    query: String,
    json_path: String,
    pagination_parameters: Option<PaginationParameters>,
    auth: Option<Auth>,
}

impl GraphQLClient {
    pub fn new(
        client: reqwest::Client,
        endpoint: Url,
        query: String,
        json_path: String,
        pagination_parameters: Option<PaginationParameters>,
        auth: Option<Auth>,
    ) -> Self {
        Self {
            client,
            endpoint,
            query,
            json_path,
            pagination_parameters,
            auth,
        }
    }
}

impl GraphQLClient {
    pub async fn execute(
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

        let pointer = format!("/{}", self.json_path.replace('.', "/"));

        let exctracted_data = response
            .pointer(pointer.as_str())
            .unwrap_or(&Value::Null)
            .to_owned();
        let next_cursor = self
            .pagination_parameters
            .as_ref()
            .and_then(|x| x.get_next_cursor_from_response(&response, limit_reached));

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

    pub async fn execute_paginated(
        &self,
        schema: SchemaRef,
        limit: Option<usize>,
    ) -> Result<Vec<Vec<RecordBatch>>> {
        let (first_batch, _, mut next_cursor) =
            self.execute(Some(Arc::clone(&schema)), limit, None).await?;
        let mut res = vec![first_batch];

        while let Some(next_cursor_val) = next_cursor {
            let (next_batch, _, new_cursor) = self
                .execute(
                    Some(Arc::clone(&schema)),
                    limit.map(|l| l - self.pagination_parameters.as_ref().map_or(0, |x| x.count)),
                    Some(next_cursor_val),
                )
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
        return Err(Error::InvalidReqwestStatus {
            status,
            message: response["message"]
                .as_str()
                .unwrap_or("No message provided")
                .to_string(),
        });
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
