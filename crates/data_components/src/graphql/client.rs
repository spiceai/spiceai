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

use super::{ArrowInternalSnafu, Error, ReqwestInternalSnafu, Result};
use arrow::{
    array::RecordBatch,
    datatypes::SchemaRef,
    json::{reader::infer_json_schema_from_iterator, ReaderBuilder},
};
use regex::Regex;
use reqwest::{RequestBuilder, StatusCode};
use serde_json::{json, Map, Value};
use snafu::ResultExt;
use std::{
    io::Cursor,
    sync::{Arc, LazyLock},
};
use url::Url;

pub enum Auth {
    Basic(String, Option<String>),
    Bearer(String),
}

#[derive(Debug, PartialEq, Eq)]
enum DuplicateBehavior {
    Error,
}

#[derive(Debug)]
pub struct UnnestParameters {
    depth: usize,
    duplicate_behavior: DuplicateBehavior,
}

#[derive(Debug, PartialEq, Eq)]
pub struct PaginationParameters {
    resource_name: String,
    count: usize,
    page_info_path: String,
}

static PAGINATION_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?xsm)(\w+)\s*\([^)]*first:\s*(\d+)[^)]*\)\s*\{.*pageInfo\s*\{.*(?:hasNextPage.*endCursor|endCursor.*hasNextPage).*\}.*\}").unwrap_or_else(|_| {
        unreachable!("Invalid regex pagination pattern defined at compile time")
    })
});

impl PaginationParameters {
    fn parse(query: &str, pointer: &str) -> Option<Self> {
        match PAGINATION_REGEX.captures(query) {
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

fn unnest_json_object_duplicate_columns(
    unnest_parameters: &UnnestParameters,
    new_object: &mut Map<String, Value>,
    key: &str,
) -> Result<String> {
    match unnest_parameters.duplicate_behavior {
        DuplicateBehavior::Error => {
            if new_object.contains_key(key) {
                return Err(Error::InvalidObjectAccess {
                    message: format!("Column '{key}' already exists in the object."),
                });
            }

            Ok(key.to_string())
        }
    }
}

fn unnest_json_object(unnest_parameters: &UnnestParameters, object: &Value) -> Result<Vec<Value>> {
    let mut new_objects = Vec::new();
    if let Value::Object(obj) = object {
        let mut new_object = obj.clone();

        // setup some loop controls
        let mut depth_counter = 0;

        loop {
            if depth_counter >= unnest_parameters.depth {
                break; // break if we've hit the unnest depth limit
            }

            // store additions and deletions
            let mut additions = vec![];

            new_object.retain(|_, value| {
                match value {
                    Value::Object(ref mut inner_obj) => {
                        inner_obj.retain(|inner_key, inner_value| {
                            additions.push((inner_key.clone(), inner_value.clone())); // add the inner key to the additions list
                            false
                        });

                        // don't retain the inner object, because we're about to bump it up to the root object
                        false
                    }
                    _ => true, // we don't need to do anything for non-object inner types
                }
            });

            if additions.is_empty() {
                break; // break if there's nothing else to do
            }

            // add the staged additions back to the root object
            for (key, value) in additions {
                let new_key =
                    unnest_json_object_duplicate_columns(unnest_parameters, &mut new_object, &key)?;

                new_object.insert(new_key, value);
            }

            // increment the depth counter
            depth_counter += 1;
        }

        new_objects.push(Value::Object(new_object));
    } else if let Value::Array(arr) = object {
        new_objects.extend(arr.clone());
    } else {
        return Err(Error::InvalidObjectAccess {
            // unnesting any other type is invalid
            message: format!("Unsupported unnest type: {object}"),
        });
    }

    Ok(new_objects)
}

fn unnest_json_objects(
    unnest_parameters: &UnnestParameters,
    objects: &[Value],
) -> Result<Vec<Value>> {
    Ok(objects
        .iter()
        .map(|obj| unnest_json_object(unnest_parameters, obj))
        .collect::<Result<Vec<Vec<_>>>>()?
        .into_iter()
        .flatten()
        .collect())
}

pub struct GraphQLClient {
    client: reqwest::Client,
    endpoint: Url,
    query: Arc<str>,
    pointer: Arc<str>,
    unnest_parameters: UnnestParameters,
    pagination_parameters: Option<PaginationParameters>,
    auth: Option<Auth>,
}

impl GraphQLClient {
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub fn new(
        client: reqwest::Client,
        endpoint: Url,
        query: Arc<str>,
        json_pointer: Arc<str>,
        token: Option<&str>,
        user: Option<String>,
        pass: Option<String>,
        unnest_depth: usize,
    ) -> Self {
        let pagination_parameters = PaginationParameters::parse(&query, &json_pointer);
        tracing::debug!(
            "Parsed pagination parameters for {:?}: {pagination_parameters:?}",
            endpoint.to_string()
        );

        let auth = match (token, user, pass) {
            (Some(token), _, _) => Some(Auth::Bearer(token.to_string())),
            (None, Some(user), pass) => Some(Auth::Basic(user, pass)),
            _ => None,
        };

        let unnest_parameters = UnnestParameters {
            depth: unnest_depth,
            duplicate_behavior: DuplicateBehavior::Error,
        };

        Self {
            client,
            endpoint,
            query,
            pointer: json_pointer,
            unnest_parameters,
            pagination_parameters,
            auth,
        }
    }

    pub(crate) async fn execute(
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

        let extracted_data = response
            .pointer(&self.pointer)
            .unwrap_or(&Value::Null)
            .to_owned();

        let next_cursor = if limit_reached {
            None
        } else {
            self.pagination_parameters
                .as_ref()
                .and_then(|x| x.get_next_cursor_from_response(&response))
        };

        let mut unwrapped = match extracted_data {
            Value::Array(val) => Ok(val.clone()),
            obj @ Value::Object(_) => Ok(vec![obj]),
            Value::Null => Err(Error::InvalidObjectAccess {
                message: "Null value access.".to_string(),
            }),
            _ => Err(Error::InvalidObjectAccess {
                message: "Primitive value access.".to_string(),
            }),
        }?;

        if self.unnest_parameters.depth > 0 {
            unwrapped = unnest_json_objects(&self.unnest_parameters, &unwrapped)?;
        }

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

    pub(crate) async fn execute_paginated(
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

        return match status {
            StatusCode::UNAUTHORIZED => {
                Err(Error::InvalidCredentialsOrPermissions { message: format!("The API failed with status code {status}; Please check if provided credentials are correct.") })
            },
            StatusCode::FORBIDDEN => {
                Err(Error::InvalidCredentialsOrPermissions { message: format!("The API failed with status code {status}; Please check if provided credentials have the necessary permissions.") })
            },
            _ => Err(Error::InvalidReqwestStatus { status, message })
        };
    }
    Ok(())
}

fn handle_graphql_query_error(response: &Value, query: &str) -> Result<()> {
    let graphql_error = &response["errors"][0];

    if !graphql_error.is_null() {
        let line = graphql_error["locations"][0]["line"].as_u64();
        let column = graphql_error["locations"][0]["column"].as_u64();
        let error_type = graphql_error["type"].as_str();

        let location = match (line, column) {
            (Some(line), Some(column)) => Some((
                usize::try_from(line).unwrap_or_default(),
                usize::try_from(column).unwrap_or_default(),
            )),
            _ => None,
        };

        let message = graphql_error["message"]
            .as_str()
            .unwrap_or_default()
            .split(" at [")
            .next()
            .unwrap_or_default()
            .to_string();

        if let Some(error_type) = error_type {
            if error_type.to_lowercase() == "forbidden" {
                return Err(Error::InvalidCredentialsOrPermissions { message: format!("The API returned a 'FORBIDDEN' error. Please check if the credentials have the necessary permissions. {message}") });
            }
        }

        return match location {
            Some((line, column)) => Err(Error::InvalidGraphQLQuery {
                message,
                line,
                column,
                query: format_query_with_context(query, line, column),
            }),
            _ => Err(Error::InvalidGraphQLQuery {
                message,
                line: 0,
                column: 0,
                query: query.to_string(),
            }),
        };
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

#[cfg(test)]
mod tests {
    use reqwest::StatusCode;
    use serde_json::Value;

    use super::{handle_http_error, DuplicateBehavior, PaginationParameters};

    struct TestPaginationParseCase {
        name: &'static str,
        query: &'static str,
        pointer: &'static str,
        expected: Option<PaginationParameters>,
    }

    #[test]
    #[allow(clippy::too_many_lines, clippy::needless_raw_string_hashes)]
    fn test_pagination_parse() {
        let test_cases = vec![
            TestPaginationParseCase {
                name: "Basic query with pageInfo",
                query: r#"
                    query {
                        users(first: 10) {
                            pageInfo {
                                hasNextPage
                                endCursor
                            }
                        }
                    }
                "#,
                pointer: "/data/users",
                expected: Some(PaginationParameters {
                    resource_name: "users".to_owned(),
                    count: 10,
                    page_info_path: "/data/users/pageInfo".to_owned(),
                }),
            },
            TestPaginationParseCase {
                name: "Query with reversed pageInfo fields",
                query: r#"
                    query {
                        users(first: 10) {
                            pageInfo {
                                endCursor
                                hasNextPage
                            }
                        }
                    }
                "#,
                pointer: "/data/users",
                expected: Some(PaginationParameters {
                    resource_name: "users".to_owned(),
                    count: 10,
                    page_info_path: "/data/users/pageInfo".to_owned(),
                }),
            },
            TestPaginationParseCase {
                name: "Query without pageInfo",
                query: r#"
                    query {
                        users(first: 10) {
                            name
                        }
                    }
                "#,
                pointer: "/data/users",
                expected: None,
            },
            TestPaginationParseCase {
                name: "Nested query with pageInfo",
                query: r#"
                    query {
                        paginatedUsers(first: 2) {
                            users {
                                id
                                name
                                posts {
                                    id
                                    title
                                    content
                                }
                            }
                            pageInfo {
                                hasNextPage
                                endCursor
                            }
                        }
                    }
                "#,
                pointer: "/data/paginatedUsers/users",
                expected: Some(PaginationParameters {
                    resource_name: "paginatedUsers".to_owned(),
                    count: 2,
                    page_info_path: "/data/paginatedUsers/pageInfo".to_owned(),
                }),
            },
        ];

        for case in test_cases {
            let result = PaginationParameters::parse(case.query, case.pointer);
            assert_eq!(result, case.expected, "Failed test case: {}", case.name);
        }
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

    #[test]
    fn test_json_object_unnesting() {
        let unnest_parameters = super::UnnestParameters {
            depth: 100,
            duplicate_behavior: DuplicateBehavior::Error,
        };
        let object = serde_json::from_str(r#"{"a": {"b": 1}}"#).expect("Valid json");
        let result =
            super::unnest_json_object(&unnest_parameters, &object).expect("To unnest JSON object");
        assert_eq!(result.len(), 1);

        let obj = result.first().expect("To get first unnested object");
        assert_eq!(
            obj,
            &Value::Object(serde_json::Map::from_iter(vec![(
                "b".to_string(),
                Value::Number(1.into())
            )]))
        );

        let unnest_parameters = super::UnnestParameters {
            depth: 100,
            duplicate_behavior: DuplicateBehavior::Error,
        };
        let object =
            serde_json::from_str(r#"{"a": {"b": {"c": {"d": "1"}}}}"#).expect("Valid json");
        let result =
            super::unnest_json_object(&unnest_parameters, &object).expect("To unnest JSON object");
        assert_eq!(result.len(), 1);

        let obj = result.first().expect("To get first unnested object");
        assert_eq!(
            obj,
            &Value::Object(serde_json::Map::from_iter(vec![(
                "d".to_string(),
                Value::String("1".to_string())
            )]))
        );
    }

    #[test]
    fn test_json_object_unnesting_respects_unnest_depth() {
        let unnest_parameters = super::UnnestParameters {
            depth: 0,
            duplicate_behavior: DuplicateBehavior::Error,
        };
        let object = serde_json::from_str(r#"{"a": {"b": 1}}"#).expect("Valid json");
        let result =
            super::unnest_json_object(&unnest_parameters, &object).expect("To unnest JSON object");
        assert_eq!(result.len(), 1);

        let obj = result.first().expect("To get first unnested object");
        assert_eq!(
            obj,
            &Value::Object(serde_json::Map::from_iter(vec![(
                "a".to_string(),
                Value::Object(serde_json::Map::from_iter(vec![(
                    "b".to_string(),
                    Value::Number(1.into())
                )]))
            )]))
        );

        let unnest_parameters = super::UnnestParameters {
            depth: 1,
            duplicate_behavior: DuplicateBehavior::Error,
        };
        let object =
            serde_json::from_str(r#"{"a": {"b": {"c": {"d": "1"}}}}"#).expect("Valid json");
        let result =
            super::unnest_json_object(&unnest_parameters, &object).expect("To unnest JSON object");
        assert_eq!(result.len(), 1);

        let obj = result.first().expect("To get first unnested object");
        assert_eq!(
            obj,
            &Value::Object(serde_json::Map::from_iter(vec![(
                "b".to_string(),
                Value::Object(serde_json::Map::from_iter(vec![(
                    "c".to_string(),
                    Value::Object(serde_json::Map::from_iter(vec![(
                        "d".to_string(),
                        Value::String("1".to_string())
                    )]))
                )]))
            )]))
        );
    }

    #[test]
    fn test_json_array_unnesting() {
        let unnest_parameters = super::UnnestParameters {
            depth: 100,
            duplicate_behavior: DuplicateBehavior::Error,
        };
        let object = serde_json::from_str("[1, 2, 3]").expect("Valid json");
        let result =
            super::unnest_json_object(&unnest_parameters, &object).expect("To unnest json array");
        assert_eq!(result.len(), 3);

        let obj = result.first().expect("To get first unnested object");
        assert_eq!(obj, &Value::Number(1.into()));

        let obj = result.get(1).expect("To get second unnested object");
        assert_eq!(obj, &Value::Number(2.into()));

        let obj = result.get(2).expect("To get third unnested object");
        assert_eq!(obj, &Value::Number(3.into()));
    }

    #[test]
    fn test_unnesting_duplicate_column_names_errors() {
        let unnest_parameters = super::UnnestParameters {
            depth: 100,
            duplicate_behavior: DuplicateBehavior::Error,
        };
        let object = serde_json::from_str(r#"{"a": 1, "c": {"b": {"a": 2}}}"#).expect("Valid json");
        let result = super::unnest_json_object(&unnest_parameters, &object);

        assert!(result.is_err());

        #[allow(clippy::unwrap_used)]
        let err = result.unwrap_err();
        assert_eq!(
            err.to_string(),
            "Invalid object access. Column 'a' already exists in the object."
        );
    }
}
