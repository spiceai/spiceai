/*
Copyright 2024-2025 The Spice.ai OSS Authors

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

use crate::{rate_limit::RateLimiter, token_provider::TokenProvider};

use super::{ArrowInternalSnafu, Error, ErrorChecker, ReqwestInternalSnafu, Result};
use arrow::{
    array::RecordBatch,
    datatypes::SchemaRef,
    json::{reader::infer_json_schema_from_iterator, ReaderBuilder},
};
use graphql_parser::query::{
    parse_query, Definition, Document, Field, InlineFragment, OperationDefinition, Query,
    Selection, SelectionSet, Text,
};
use regex::Regex;
use reqwest::{RequestBuilder, StatusCode};
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use snafu::ResultExt;
use std::{cmp::min, fmt::Display, io::Cursor, sync::Arc};

use url::Url;

use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::{error::DataFusionError, physical_plan::stream::RecordBatchReceiverStream};

pub enum Auth {
    Basic(String, Option<String>),
    Bearer(Arc<dyn TokenProvider>),
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

/// [`PageInfo`] for pagination, following the [GraphQL Cursor Connections Specification](https://relay.dev/graphql/connections.htm#sec-undefined.PageInfo).
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct PageInfo {
    #[serde(default)]
    pub has_next_page: bool,

    #[serde(default)]
    pub has_previous_page: bool,
    pub start_cursor: Option<String>,
    pub end_cursor: Option<String>,
}

impl PageInfo {
    /// Based on the pagination, returns the appropriate cursor.
    ///
    /// Example:
    /// ```rust
    /// use serde_json;
    /// use data_components::graphql::client::PageInfo;
    ///
    /// let info = serde_json::from_str(r#"{"hasNextPage": true, "endCursor": "cursor_abc"}"#).unwrap();
    /// assert_eq!(
    ///  info.cursor_from_pagination(&PaginationArgument::First(10)),
    ///  Some("cursor_abc".to_string())
    /// );
    ///
    /// assert_eq!(
    ///  info.cursor_from_pagination(&PaginationArgument::Last(10)),
    ///  None
    /// );
    /// ```
    fn cursor_from_pagination(&self, arg: &PaginationArgument) -> Option<String> {
        match arg {
            PaginationArgument::First(_) => {
                if self.has_next_page {
                    self.end_cursor.clone()
                } else {
                    None
                }
            }
            PaginationArgument::Last(_) => {
                if self.has_previous_page {
                    self.start_cursor.clone()
                } else {
                    None
                }
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PaginationArgument {
    /// paginating via `fn(first: usize, after: String)`
    First(usize),
    /// paginating via `fn(last: usize, before: String)`
    Last(usize),
}

impl std::fmt::Display for PaginationArgument {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PaginationArgument::First(z) => write!(f, "first: {z}"),
            PaginationArgument::Last(z) => write!(f, "last: {z}"),
        }
    }
}

impl PaginationArgument {
    /// Formats the pagination arguments to be inserted into a Graphql variable.
    ///
    /// Example:
    /// ```rust
    /// use data_components::graphql::client::PaginationArgument;
    /// assert_eq!(
    ///   PaginationArgument::Last(10).format_arguments(None),
    ///   "last: 10"
    /// );
    /// assert_eq!(
    ///   PaginationArgument::First(10).format_arguments(Some("cursor_abc".to_string())),
    ///   "first: 10, after: \"cursor_abc\""
    /// );
    /// ```
    fn format_arguments(&self, cursor: Option<String>) -> String {
        match (self, cursor) {
            (PaginationArgument::First(z), Some(c)) => {
                format!(r#"first: {z}, after: "{c}""#,)
            }
            (PaginationArgument::First(z), None) => {
                format!("first: {z}")
            }
            (PaginationArgument::Last(z), Some(c)) => {
                format!("last: {z}, before: \"{c}\"")
            }
            (PaginationArgument::Last(z), None) => {
                format!("last: {z}")
            }
        }
    }

    fn with_limit(&self, limit: usize) -> Self {
        match self {
            PaginationArgument::First(z) => PaginationArgument::First(min(*z, limit)),
            PaginationArgument::Last(z) => PaginationArgument::Last(min(*z, limit)),
        }
    }

    fn size(&self) -> usize {
        match self {
            PaginationArgument::First(z) | PaginationArgument::Last(z) => *z,
        }
    }

    /// Validates that a `pageInfo` field is valid for [`PaginationArgument`].
    fn validate_page_info<'a, T: Text<'a>>(&self, f: &Field<'a, T>) -> Result<(), String> {
        let mut has_next_page = false;
        let mut has_previous_page = false;
        let mut start_cursor = false;
        let mut end_cursor = false;

        // Find which values are present in the pageInfo field
        f.selection_set.items.iter().for_each(|s| {
            if let Selection::Field(f) = s {
                match f.name.as_ref() {
                    "hasNextPage" => has_next_page = true,
                    "hasPreviousPage" => has_previous_page = true,
                    "startCursor" => start_cursor = true,
                    "endCursor" => end_cursor = true,
                    _ => (),
                }
            }
        });

        // Check present fields are consistent with the pagination argument.
        match &self {
            PaginationArgument::First(_) => {
                if !has_next_page || !end_cursor {
                    return Err("'pageInfo' field needs both 'hasNextPage' and 'endCursor' for forward pagination.".to_string());
                }
                Ok(())
            }
            PaginationArgument::Last(_) => {
                if !has_previous_page || !start_cursor {
                    return Err("'pageInfo' field needs both 'hasPreviousPage' and 'startCursor' for backward pagination.".to_string());
                }
                Ok(())
            }
        }
    }
}

/// Try to convert a [`Field`] into a [`PaginationArgument`]. Assumes the field has a valid pagination
/// argument with one of the following arguments: `first`, `last`.
impl<'a, T: Text<'a>> TryInto<PaginationArgument> for &Field<'a, T> {
    type Error = String;

    fn try_into(self) -> std::result::Result<PaginationArgument, Self::Error> {
        let pag_arg_opt = self.arguments.iter().find_map(|(arg, v)| {
            let z = match v {
                graphql_parser::query::Value::Int(z) => z.as_i64(),
                _ => None,
            }?;

            let n: usize = z.try_into().ok()?;

            match arg.as_ref() {
                "first" => Some(PaginationArgument::First(n)),
                "last" => Some(PaginationArgument::Last(n)),
                _ => None,
            }
        });
        match pag_arg_opt {
            Some(page_arg) => Ok(page_arg),
            None => Err("Invalid pagination argument".to_string()),
        }
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
struct FieldArgument {
    name: String,
    value: String,
}

impl Display for FieldArgument {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{name}: {value}", name = self.name, value = self.value)
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct PaginationParameters {
    resource_name: String,
    pub pagination_argument: PaginationArgument,
    other_arguments: Vec<FieldArgument>,
    page_info_path: Option<String>,
}

struct FieldArguments {
    args: String,
}

impl PaginationParameters {
    #[must_use]
    fn parameters_string(&self, limit: Option<usize>, cursor: Option<String>) -> FieldArguments {
        let pagination_argument = self
            .pagination_argument
            .with_limit(limit.unwrap_or(usize::MAX));

        if self.other_arguments.is_empty() {
            FieldArguments {
                args: pagination_argument.format_arguments(cursor),
            }
        } else {
            let mut args = self
                .other_arguments
                .iter()
                .map(std::string::ToString::to_string)
                .collect::<Vec<String>>();

            tracing::debug!("GraphQL found other arguments: {}", args.join(", "));

            args.push(pagination_argument.format_arguments(cursor));

            let args = args.join(", ");

            FieldArguments { args }
        }
    }
}

impl PaginationParameters {
    fn reduce_limit(&self, l: usize) -> usize {
        l.saturating_sub(self.pagination_argument.size())
    }

    /// Parses the GraphQL query and returns the appropriate [`PaginationParameters`] if the query
    /// contains a `pageInfo` field (and therefore involved pagination). Alongside the parameters,
    /// it also infers the standard JSON pointer to the paginated data as expected when
    /// [streaming over HTTP](https://graphql.org/learn/serving-over-http/#response).
    ///
    /// If the query does not contain a `pageInfo` field, it returns `None` and cannot infer the JSON pointer.
    ///
    /// The GraphQL query should be the only other field at the depth of the `pageInfo` field.
    ///
    /// ### Example:
    ///
    /// **Valid**:
    /// ```graphql
    /// query {
    ///    users(first: 10) {
    ///      node {
    ///        id
    ///        name
    ///        email   
    ///      }
    ///      pageInfo {
    ///        hasNextPage
    ///        endCursor
    ///      }
    ///    }
    /// }
    /// ```
    ///
    /// **Invalid**:
    /// ```graphql
    /// query {
    ///    users(first: 10) {
    ///      node {
    ///        id
    ///        name
    ///        email   
    ///      }
    ///      edges {
    ///        friends {
    ///          id
    ///          name
    ///      }
    ///      pageInfo {
    ///        hasNextPage
    ///        endCursor
    ///      }
    ///    }
    /// }
    /// ```
    ///
    /// A user must explicitly provider the JSON pointer for the latter example (e.g. when calling [`GraphQLClient::new`]).
    ///
    #[must_use]
    pub fn parse(ast: &Document<'_, String>) -> (Option<Self>, Option<String>) {
        // Start traversing the query's operation definitions
        for def in ast.definitions.clone() {
            let selections = match def {
                Definition::Operation(OperationDefinition::Query(Query {
                    selection_set, ..
                })) => selection_set.items,
                Definition::Operation(OperationDefinition::SelectionSet(SelectionSet {
                    items,
                    ..
                })) => items,
                _ => continue,
            };

            if let (Some(found_path), inferred_json_pointer) =
                Self::find_in_selection_set(&selections, "", None)
            {
                return (Some(found_path), inferred_json_pointer);
            }
        }
        (None, None)
    }

    // Recursive function to traverse the AST and find the pageInfo field
    fn find_in_selection_set<'a, T: Text<'a> + std::fmt::Debug>(
        selections: &[Selection<'a, T>],
        current_path: &str,
        parent_field: Option<&Field<'a, T>>,
    ) -> (Option<PaginationParameters>, Option<String>) {
        tracing::trace!("For PaginationParameters, searching json_pointer path: {current_path}");
        for selection in selections {
            match selection {
                graphql_parser::query::Selection::FragmentSpread(_) => continue,
                graphql_parser::query::Selection::InlineFragment(InlineFragment {
                    selection_set,
                    ..
                }) => {
                    if let (Some(solution), inferred_json_pointer) = Self::find_in_selection_set(
                        &selection_set.items,
                        current_path,
                        parent_field,
                    ) {
                        return (Some(solution), inferred_json_pointer);
                    }
                }
                graphql_parser::query::Selection::Field(field) => {
                    let field_name = field.name.as_ref();
                    let new_path = format!("{current_path}/{field_name}");

                    // End of recursion, `pageInfo` field found
                    if field_name == "pageInfo" {
                        tracing::debug!("For PaginationParameters, found `pageInfo` at {new_path}");
                        let Some(parent_field) = parent_field else {
                            tracing::warn!("Invalid parent field");
                            return (None, None);
                        };

                        // Find the JSON pointer to the data field next to the `pageInfo` field.
                        let data_field = selections.iter().find_map(|s| match s {
                            Selection::Field(f) => {
                                if f.name == "pageInfo".into() {
                                    None
                                } else {
                                    Some(f)
                                }
                            }
                            _ => None,
                        });

                        if data_field.is_none() {
                            tracing::debug!(
                                "No appropriate data field found next to pageInfo field."
                            );
                        }

                        let json_pointer =
                            data_field.map(|f| format!("/data{current_path}/{}", f.name.as_ref()));

                        let pagination_argument =
                            match TryInto::<PaginationArgument>::try_into(parent_field) {
                                Ok(pagination_argument) => pagination_argument,
                                Err(e) => {
                                    tracing::warn!("Invalid pagination argument from field: {e}");
                                    return (None, None);
                                }
                            };

                        tracing::debug!("pagination_argument: {pagination_argument}");

                        let other_arguments = parent_field
                            .arguments
                            .iter()
                            .filter_map(|(k, v)| match k.as_ref() {
                                "first" | "last" | "after" | "before" => None,
                                _ => Some(FieldArgument {
                                    name: k.as_ref().to_string(),
                                    value: v.to_string(),
                                }),
                            })
                            .collect();

                        // Check [`PaginationArgument`] and `pageInfo` fields are consistent.
                        if let Err(e) = pagination_argument.validate_page_info(field) {
                            tracing::warn!("GraphQL query has pagination specified ({pagination_argument}), but invalid pagination fields: {e}");
                            return (None, None);
                        }

                        return (
                            Some(PaginationParameters {
                                resource_name: parent_field.name.as_ref().to_string(),
                                page_info_path: Some(new_path),
                                pagination_argument,
                                other_arguments,
                            }),
                            json_pointer,
                        );
                    }

                    // Recurse into nested selection sets
                    if let (Some(solution), inferred_json_pointer) = Self::find_in_selection_set(
                        &field.selection_set.items,
                        &new_path,
                        Some(field),
                    ) {
                        return (Some(solution), inferred_json_pointer);
                    }
                }
            }
        }
        (None, None)
    }

    fn apply(&self, query: &str, limit: Option<usize>, cursor: Option<String>) -> String {
        #[allow(clippy::needless_raw_string_hashes)]
        let pattern = format!(r#"{}\s*\(.*\)"#, self.resource_name);
        let regex =
            Regex::new(&pattern).unwrap_or_else(|_| panic!("Invalid regex query resource pattern"));

        let arguments = self.parameters_string(limit, cursor);

        let new_query = regex.replace(
            query,
            format!(
                "{resource_name} ({arguments})",
                arguments = arguments.args,
                resource_name = self.resource_name,
            ),
        );

        new_query.to_string()
    }

    fn get_next_cursor_from_response(&self, response: &Value) -> Option<String> {
        let Some(page_info_path) = &self.page_info_path else {
            return None;
        };

        let page_info: PageInfo = response
            .pointer(&format!("/data{page_info_path}"))
            .cloned()
            .map(serde_json::from_value)
            .transpose()
            .ok()
            .flatten()?;

        page_info.cursor_from_pagination(&self.pagination_argument)
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
    pub(crate) json_pointer: Option<Arc<str>>,
    unnest_parameters: UnnestParameters,
    auth: Option<Auth>,
    schema: Option<SchemaRef>,
    rate_limiter: Option<Arc<dyn RateLimiter>>,
}

#[derive(Clone)]
pub struct GraphQLQuery {
    _source_query: Arc<str>,
    ast: Document<'static, String>,
    pub json_pointer: Option<Arc<str>>,
    pub pagination_parameters: Option<PaginationParameters>,
}

impl TryFrom<Arc<str>> for GraphQLQuery {
    type Error = super::Error;

    fn try_from(query: Arc<str>) -> Result<Self, self::Error> {
        // SAFETY: We're transmuting the lifetime to 'static and this is safe because:
        // 1. The reference won't outlive the GraphQLQuery struct and we don't give it out as a static reference
        // 2. The source Arc is kept alive as long as the GraphQLQuery exists
        // 3. Arc guarantees the data remains at the same address
        //
        // This wouldn't be required if Rust had proper support for self-referencing structs.
        let query_ref: &'static str = unsafe { std::mem::transmute::<&str, &'static str>(&query) };

        let ast =
            parse_query::<String>(query_ref).map_err(|_| super::Error::InvalidGraphQLQuery {
                message: "Failed to parse GraphQL query".to_string(),
                line: 0,
                column: 0,
                query: query.to_string(),
            })?;

        let (pagination_parameters, json_pointer) = PaginationParameters::parse(&ast);

        Ok(Self {
            _source_query: query,
            ast,
            json_pointer: json_pointer.map(Arc::from),
            pagination_parameters,
        })
    }
}

impl GraphQLQuery {
    #[must_use]
    pub fn to_string(&self, limit: Option<usize>, cursor: Option<String>) -> String {
        let query = self.ast.to_string();

        if let Some(pagination_parameters) = &self.pagination_parameters {
            pagination_parameters.apply(&query, limit, cursor)
        } else {
            query
        }
    }

    pub fn limit_reached(&mut self, limit: Option<usize>, record_count: usize) -> bool {
        if let Some(limit) = limit {
            record_count >= limit
        } else {
            false
        }
    }

    #[must_use]
    pub fn ast(&self) -> &Document<'_, String> {
        // SAFETY: We can safely transmute back to a shorter lifetime
        unsafe {
            std::mem::transmute::<&Document<'static, String>, &Document<'_, String>>(&self.ast)
        }
    }

    #[must_use]
    pub fn ast_mut(&mut self) -> &mut Document<'_, String> {
        // SAFETY: We can safely transmute back to a shorter lifetime
        unsafe {
            std::mem::transmute::<&mut Document<'static, String>, &mut Document<'_, String>>(
                &mut self.ast,
            )
        }
    }
}

pub(crate) struct GraphQLQueryResult {
    pub(crate) records: Vec<RecordBatch>,
    record_count: usize,
    limit_reached: bool,
    pub(crate) schema: SchemaRef,
    cursor: Option<String>,
}

impl GraphQLClient {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        client: reqwest::Client,
        endpoint: Url,
        json_pointer: Option<&str>,
        token: Option<Arc<dyn TokenProvider>>,
        user: Option<String>,
        pass: Option<String>,
        unnest_depth: usize,
        schema: Option<SchemaRef>,
        rate_limiter: Option<Arc<dyn RateLimiter>>,
    ) -> Result<Self> {
        let auth = match (token, user, pass) {
            (None, Some(user), pass) => Some(Auth::Basic(user, pass)),
            (Some(token), _, _) => Some(Auth::Bearer(token)),
            _ => None,
        };

        let unnest_parameters = UnnestParameters {
            depth: unnest_depth,
            duplicate_behavior: DuplicateBehavior::Error,
        };

        let json_pointer = json_pointer.map(Arc::from);

        Ok(Self {
            client,
            endpoint,
            json_pointer,
            unnest_parameters,
            auth,
            schema,
            rate_limiter,
        })
    }

    pub(crate) async fn execute(
        &self,
        query: &mut GraphQLQuery,
        schema: Option<SchemaRef>,
        limit: Option<usize>,
        cursor: Option<String>,
        error_checker: Option<ErrorChecker>,
    ) -> Result<GraphQLQueryResult> {
        // Check rate limit before executing the query
        if let Some(rate_limiter) = &self.rate_limiter {
            rate_limiter
                .check_rate_limit()
                .await
                .map_err(|e| Error::RateLimited {
                    message: format!("{e}"),
                })?;
        }

        let query_string = query.to_string(limit, cursor);

        let body = format!(r#"{{"query": {}}}"#, json!(query_string));

        let mut request = self.client.post(self.endpoint.clone()).body(body);
        request = request_with_auth(request, self.auth.as_ref()).await;

        let response = request.send().await.context(ReqwestInternalSnafu)?;
        let response_headers = response.headers().clone();

        // Update rate limiter with response headers
        if let Some(rate_limiter) = &self.rate_limiter {
            rate_limiter.update_from_headers(&response_headers).await;
        }

        let status = response.status();
        let response: serde_json::Value = response.json().await.context(ReqwestInternalSnafu)?;

        error_checker
            .map(|p| p(&response_headers, &response))
            .transpose()?;
        handle_http_error(status, &response)?;
        handle_graphql_query_error(&response, &query_string)?;

        let json_pointer = query
            .json_pointer
            .as_ref()
            .or(self.json_pointer.as_ref())
            .ok_or(Error::NoJsonPointerFound {})?;

        let extracted_data = response
            .pointer(json_pointer)
            .ok_or(Error::InvalidJsonPointer {
                pointer: json_pointer.to_string(),
            })?
            .to_owned();

        let next_cursor = query
            .pagination_parameters
            .as_ref()
            .and_then(|x| x.get_next_cursor_from_response(&response));

        let mut unwrapped = match extracted_data {
            Value::Array(val) => Ok(val.clone()),
            obj @ Value::Object(_) => Ok(vec![obj]),
            _ => Err(Error::InvalidObjectAccess {
                message: format!("GraphQL response has unexpected format. Response {response:?}"),
            }),
        }?;

        if self.unnest_parameters.depth > 0 {
            unwrapped = unnest_json_objects(&self.unnest_parameters, &unwrapped)?;
        }

        let schema = get_json_schema(self.schema.as_ref(), schema.as_ref(), &unwrapped)?;

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

        let record_count = res.len();

        let limit_reached = query.limit_reached(limit, record_count);

        Ok(GraphQLQueryResult {
            records: res,
            record_count,
            limit_reached,
            schema: Arc::clone(&schema),
            cursor: next_cursor,
        })
    }

    #[must_use]
    pub fn execute_paginated(
        self: Arc<Self>,
        mut query: GraphQLQuery,
        gql_schema: SchemaRef,
        table_schema: SchemaRef,
        limit: Option<usize>,
        error_checker: Option<ErrorChecker>,
    ) -> SendableRecordBatchStream {
        let mut builder = RecordBatchReceiverStream::builder(table_schema, 2);
        let tx = builder.tx();

        // Spawn the task that will fetch and send the GraphQL record batches
        builder.spawn(async move {
            let mut result = self
                .execute(
                    &mut query,
                    Some(Arc::clone(&gql_schema)),
                    limit,
                    None,
                    error_checker.clone(),
                )
                .await
                .map_err(|e| DataFusionError::Execution(e.to_string()))?;
            let mut limit = limit;

            for batch in result.records {
                tx.send(Ok(batch)).await.map_err(|_| {
                    DataFusionError::Execution("Failed to send record batch".to_string())
                })?;
            }

            if result.limit_reached {
                return Ok(());
            }

            while let Some(next_cursor_val) = result.cursor {
                if let Some(p) = query.pagination_parameters.as_ref() {
                    if limit.is_some() {
                        limit = Some(p.reduce_limit(result.record_count));
                    }
                }

                result = self
                    .execute(
                        &mut query,
                        Some(Arc::clone(&gql_schema)),
                        limit,
                        Some(next_cursor_val),
                        error_checker.clone(),
                    )
                    .await
                    .map_err(|e| DataFusionError::Execution(e.to_string()))?;

                for batch in result.records {
                    tx.send(Ok(batch)).await.map_err(|_| {
                        DataFusionError::Execution("Failed to send record batch".to_string())
                    })?;
                }

                if result.limit_reached {
                    break;
                }
            }
            Ok(())
        });

        builder.build()
    }
}

fn get_json_schema(
    client_schema: Option<&SchemaRef>,
    schema_override: Option<&SchemaRef>,
    json_iter: &[Value],
) -> Result<SchemaRef> {
    if let Some(schema) = schema_override {
        return Ok(Arc::clone(schema));
    }

    if let Some(schema) = client_schema {
        return Ok(Arc::clone(schema));
    }

    let schema = infer_json_schema_from_iterator(json_iter.iter().map(Result::Ok))
        .context(ArrowInternalSnafu)?;

    Ok(Arc::new(schema))
}

async fn request_with_auth(request_builder: RequestBuilder, auth: Option<&Auth>) -> RequestBuilder {
    match auth {
        Some(Auth::Basic(user, pass)) => request_builder.basic_auth(user, pass.clone()),
        Some(Auth::Bearer(token_provider)) => {
            if let Ok(token) = token_provider.get_token().await {
                return request_builder.bearer_auth(&token);
            }
            request_builder
        }
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
                Err(Error::InvalidCredentialsOrPermissions { message: format!("The API failed with status code {status}.\nVerify the provided credentials are correct.") })
            },
            StatusCode::FORBIDDEN => {
                Err(Error::InvalidCredentialsOrPermissions { message: format!("The API failed with status code {status}.\nVerify the provided credentials have the necessary permissions.") })
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
                return Err(Error::InvalidCredentialsOrPermissions { message: format!("The API returned a 'FORBIDDEN' error.\nVerify the credentials have the necessary permissions.\n{message}") });
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
    use std::sync::Arc;

    use reqwest::StatusCode;
    use serde_json::Value;

    use crate::graphql::client::GraphQLQuery;

    use super::{handle_http_error, DuplicateBehavior, PaginationParameters};

    struct TestPaginationParseCase {
        name: &'static str,
        query: &'static str,
        expected: (Option<PaginationParameters>, Option<String>),
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
                expected: (
                    Some(PaginationParameters {
                        resource_name: "users".to_owned(),
                        pagination_argument: super::PaginationArgument::First(10),
                        page_info_path: Some("/users/pageInfo".into()),
                        other_arguments: vec![],
                    }),
                    None,
                ),
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
                expected: (
                    Some(PaginationParameters {
                        resource_name: "users".to_owned(),
                        pagination_argument: super::PaginationArgument::First(10),
                        page_info_path: Some("/users/pageInfo".into()),
                        other_arguments: vec![],
                    }),
                    None,
                ),
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
                expected: (None, None),
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
                expected: (
                    Some(PaginationParameters {
                        resource_name: "paginatedUsers".to_owned(),
                        pagination_argument: super::PaginationArgument::First(2),
                        page_info_path: Some("/paginatedUsers/pageInfo".to_owned()),
                        other_arguments: vec![],
                    }),
                    Some("/data/paginatedUsers/users".into()),
                ),
            },
            TestPaginationParseCase {
                name: "Pagination with other fields",
                query: r#"
                    query {
                        paginatedUsers(first: 2, some_field: "value", integer_field: 10, boolean_field: true) {
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
                expected: (
                    Some(PaginationParameters {
                        resource_name: "paginatedUsers".to_owned(),
                        pagination_argument: super::PaginationArgument::First(2),
                        page_info_path: Some("/paginatedUsers/pageInfo".to_owned()),
                        other_arguments: vec![
                            super::FieldArgument {
                                name: "some_field".to_owned(),
                                value: r#""value""#.to_owned(),
                            },
                            super::FieldArgument {
                                name: "integer_field".to_owned(),
                                value: "10".to_owned(),
                            },
                            super::FieldArgument {
                                name: "boolean_field".to_owned(),
                                value: "true".to_owned(),
                            },
                        ],
                    }),
                    Some("/data/paginatedUsers/users".into()),
                ),
            },
        ];

        for case in test_cases {
            let query = GraphQLQuery::try_from(Arc::from(case.query)).expect("Should parse query");
            let result = PaginationParameters::parse(&query.ast);
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

        let query = GraphQLQuery::try_from(Arc::from(query)).expect("Should parse query");
        let (pagination_parameters_opt, _) = PaginationParameters::parse(&query.ast);
        pagination_parameters_opt.expect("Should get pagination params");
        let new_query = query.to_string(None, Some("new_cursor".to_string()));
        let expected_query = r#"query {
  users (first: 10, after: "new_cursor") {
    name
    pageInfo {
      hasNextPage
      endCursor
    }
  }
}
"#;
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

        let query = GraphQLQuery::try_from(Arc::from(query)).expect("Should parse query");
        let (pagination_parameters_opt, _) = PaginationParameters::parse(&query.ast);
        pagination_parameters_opt.expect("Should get pagination params");
        let new_query = query.to_string(None, Some("new_cursor".to_string()));
        let expected_query = r#"query {
  users (first: 10, after: "new_cursor") {
    name
    pageInfo {
      hasNextPage
      endCursor
    }
  }
}
"#;
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

        let query = GraphQLQuery::try_from(Arc::from(query)).expect("Should parse query");
        let (pagination_parameters_opt, _) = PaginationParameters::parse(&query.ast);
        pagination_parameters_opt.expect("Should get pagination params");
        let new_query = query.to_string(Some(5), Some("new_cursor".to_string()));
        let expected_query = r#"query {
  users (first: 5, after: "new_cursor") {
    name
    pageInfo {
      hasNextPage
      endCursor
    }
  }
}
"#;
        assert_eq!(new_query, expected_query);
    }

    #[test]
    fn test_pagination_get_next_cursor_from_response() {
        // Forward cursor, with next page
        let query = r"query {
            users(first: 10) {
                name
                pageInfo {
                    hasNextPage
                    endCursor
                }
            }
        }";

        let query = GraphQLQuery::try_from(Arc::from(query)).expect("Should parse query");
        let (pagination_parameters_opt, _) = PaginationParameters::parse(&query.ast);
        let pagination_parameters =
            pagination_parameters_opt.expect("Failed to get pagination params");

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

        // Backwards cursor, with previous page
        let query = r"query {
            users(last: 10) {
                name
                pageInfo {
                    hasPreviousPage
                    startCursor
                }
            }
        }";

        let query = GraphQLQuery::try_from(Arc::from(query)).expect("Should parse query");
        let (pagination_parameters_opt, _) = PaginationParameters::parse(&query.ast);
        let pagination_parameters =
            pagination_parameters_opt.expect("Failed to get pagination params");

        let response = serde_json::from_str(
            r#"{
            "data": {
                "users": {
                    "pageInfo": {
                        "hasPreviousPage": true,
                        "startCursor": "new_cursor"
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

        // Backwards cursor, no pagination left
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
