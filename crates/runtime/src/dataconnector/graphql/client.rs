use std::{io::Cursor, sync::Arc};

use arrow::{array::RecordBatch, datatypes::SchemaRef, error::ArrowError};
use arrow_json::{reader::infer_json_schema_from_iterator, ReaderBuilder};
use regex::Regex;
use serde_json::{json, Value};
use snafu::{ResultExt, Snafu};
use url::Url;

use super::pagination::PaginationParameters;

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

pub enum Auth {
    Basic(String, Option<String>),
    Bearer(String),
}

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
    pub async fn execute(
        &self,
        schema: Option<SchemaRef>,
        limit: Option<usize>,
        cursor: Option<String>,
    ) -> Result<(Vec<RecordBatch>, SchemaRef, Option<String>)> {
        let mut limit_reached = false;

        let query = match (cursor, self.pagination_parameters.as_ref()) {
            (Some(cursor), Some(params)) => {
                let mut count = self
                    .pagination_parameters
                    .as_ref()
                    .map(|p| p.count)
                    .unwrap_or_default();

                if let Some(limit) = limit {
                    if limit < count {
                        count = limit;
                        limit_reached = true;
                    }
                }
                let pattern = format!(r#"{}\s*\(.*\)"#, params.resource_name);
                let regex = Regex::new(&pattern).unwrap();

                let new_query = regex.replace(
                    &self.query,
                    format!(
                        r#"{} (first: {count}, after: "{cursor}")"#,
                        params.resource_name
                    )
                    .as_str(),
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

        let next_cursor = match (&self.pagination_parameters, limit_reached) {
            (Some(params), false) => {
                let page_info = {
                    // trim path to paginated resource
                    let pattern = format!(r"^(.*?{})", params.resource_name);
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
            _ => None,
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
                    limit.map(|l| l - self.pagination_parameters.as_ref().unwrap().count),
                    Some(next_cursor_val),
                )
                .await?;
            next_cursor = new_cursor;
            res.push(next_batch);
        }

        Ok(res)
    }
}
