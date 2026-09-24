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

use runtime_rate_control::RateController;
use token_provider::TokenProvider;
use tokio::sync::Semaphore;
use {crate::graphql::InvalidPaginationRegexSnafu, data_components::rate_limit::RateLimiter};

use super::{
    ArrowInternalSnafu, Error, ErrorChecker, PAGE_RETRY_MAX_ATTEMPTS, ReqwestInternalSnafu, Result,
    is_gateway_error, is_retriable_error,
};
use arrow::{
    array::RecordBatch,
    datatypes::SchemaRef,
    json::{ReaderBuilder, reader::infer_json_schema_from_iterator},
};
use graphql_parser::query::{
    Definition, Document, Field, InlineFragment, OperationDefinition, Query, Selection,
    SelectionSet, Text, parse_query,
};
use regex::Regex;
use reqwest::{RequestBuilder, StatusCode};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use snafu::ResultExt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::{cmp::min, fmt::Display, io::Cursor, sync::Arc, time::Instant};
use util::fibonacci_backoff::FibonacciBackoffBuilder;
use util::{RetryError, retry};

use url::Url;

use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::{error::DataFusionError, physical_plan::stream::RecordBatchReceiverStream};
use futures::future::try_join_all;

pub enum Auth {
    Basic(String, Option<String>),
    Bearer(Arc<dyn TokenProvider>),
    CustomHeader(reqwest::header::HeaderName, Arc<dyn TokenProvider>),
}

#[derive(Debug, PartialEq, Eq)]
pub enum DuplicateBehavior {
    Error,
}

/// Fallback page size used by the gateway-error shrink path when the query
/// has no declared pagination argument. In practice this should never be hit
/// because the shrink path is only entered for paginated queries, but it
/// provides a safe upper bound if it ever is.
const GATEWAY_SHRINK_DEFAULT_PAGE_SIZE: usize = 100;

/// Absolute lower bound the gateway-error shrink path will apply. GitHub's
/// GraphQL API requires a pagination value of at least 1.
const GATEWAY_SHRINK_MIN_PAGE_SIZE: usize = 1;

/// Returns the next smaller page size along a reverse-Fibonacci sequence.
///
/// Mapping highest-to-lowest: `100 -> 55 -> 34 -> 21 -> 13 -> 8 -> 5 -> 3 -> 2 -> 1`.
///
/// Input values that don't exactly match a step land on the nearest smaller
/// step; values at or below the minimum return the minimum. The sequence is
/// chosen to shrink aggressively on the first retry and then taper off as the
/// page size approaches 1, since GitHub's GraphQL backend typically succeeds
/// well before reaching the lower bound.
#[must_use]
fn reverse_fibonacci_shrink(current: usize) -> usize {
    // Reverse-Fibonacci ladder (descending).
    const LADDER: [usize; 10] = [100, 55, 34, 21, 13, 8, 5, 3, 2, 1];

    for &step in &LADDER {
        if step < current {
            return step;
        }
    }
    GATEWAY_SHRINK_MIN_PAGE_SIZE
}

pub(crate) type UnnestHandler = Box<dyn Fn(&Value) -> Result<Vec<Value>> + Send + Sync>;

pub enum UnnestBehavior {
    Depth(usize),
    Custom(UnnestHandler),
}

/// Follow-up pages for a connection nested under a GraphQL `node(id:)` parent.
///
/// GitHub (and similar APIs) cap a nested `first:` at 100 and will not paginate
/// that connection inside the parent list query. When `pageInfo.hasNextPage` is
/// set, the client fetches the remaining child pages one parent at a time so
/// the scan stays complete without re-reading every sibling.
#[derive(Debug, Clone)]
pub struct NestedConnectionPager {
    /// Response key of the nested connection, e.g. `reviews`.
    pub connection_key: &'static str,
    /// Parent field holding the GraphQL node id, e.g. `pull_request_id`.
    pub parent_id_key: &'static str,
    /// GraphQL type used in `... on Type`.
    pub type_condition: &'static str,
    /// Selection set inside `nodes { ... }` of a follow-up page.
    pub node_selection: &'static str,
    /// `first:` of each follow-up page. Must be the API's nested-connection max.
    pub page_size: u32,
}

impl NestedConnectionPager {
    fn next_page_query(&self, node_id: &str, cursor: &str) -> String {
        format!(
            r#"{{
                node(id: "{id}") {{
                    ... on {ty} {{
                        {conn}(first: {n}, after: "{cursor}") {{
                            pageInfo {{
                                hasNextPage
                                endCursor
                            }}
                            totalCount
                            nodes {{
                                {fields}
                            }}
                        }}
                    }}
                }}
            }}"#,
            id = escape_graphql_string(node_id),
            ty = self.type_condition,
            conn = self.connection_key,
            n = self.page_size,
            cursor = escape_graphql_string(cursor),
            fields = self.node_selection,
        )
    }
}

fn escape_graphql_string(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"")
}

/// Cursor pages to follow before calling it a loop, for an outer connection or a
/// nested one. A server that never clears `hasNextPage` must not hang the scan.
const MAX_PAGINATION_ITERATIONS: usize = 1000;

fn nested_connection<'a>(parent: &'a Value, pager: &NestedConnectionPager) -> Option<&'a Value> {
    parent.get(pager.connection_key)
}

fn nested_page_info<'a>(connection: &'a Value, field: &str) -> Option<&'a Value> {
    connection.get("pageInfo").and_then(|info| info.get(field))
}

fn nested_has_next(connection: &Value) -> bool {
    nested_page_info(connection, "hasNextPage")
        .and_then(Value::as_bool)
        .unwrap_or(false)
}

fn nested_end_cursor(connection: &Value) -> Option<&str> {
    nested_page_info(connection, "endCursor")
        .and_then(Value::as_str)
        .filter(|cursor| !cursor.is_empty())
}

impl std::fmt::Debug for UnnestBehavior {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            UnnestBehavior::Depth(depth) => write!(f, "Depth({depth})"),
            UnnestBehavior::Custom(_) => write!(f, "Custom"),
        }
    }
}

#[derive(Debug)]
pub struct UnnestParameters {
    behavior: UnnestBehavior,
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
    /// use connector_graphql::graphql::client::PageInfo;
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
    /// use connector_graphql::graphql::client::PaginationArgument;
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
                format!(r#"first: {z}, after: "{c}""#)
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
    fn parameters_string(
        &self,
        limit: Option<usize>,
        cursor: Option<String>,
        page_size_override: Option<usize>,
    ) -> FieldArguments {
        // Apply the page size override (if any) BEFORE the user-visible limit so we
        // never ask for more rows per page than `page_size_override`, even when the
        // caller's remaining `limit` is larger.
        //
        // Clamp the page-size-override path to a minimum of 1 (GitHub GraphQL and
        // most connection-style APIs reject `first: 0` / `last: 0`), but never
        // clamp an explicit user-provided `limit` — in particular, `Some(0)`
        // must be preserved to honor `LIMIT 0` semantics rather than silently
        // fetching a row.
        let effective_limit = match (limit, page_size_override) {
            (Some(0), _) => Some(0),
            (Some(l), Some(p)) => Some(std::cmp::max(std::cmp::min(l, p), 1)),
            (Some(l), None) => Some(l),
            (None, Some(p)) => Some(std::cmp::max(p, 1)),
            (None, None) => None,
        };

        let pagination_argument = self
            .pagination_argument
            .with_limit(effective_limit.unwrap_or(usize::MAX));

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
                graphql_parser::query::Selection::FragmentSpread(_) => {}
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
                    // Use alias when present — the JSON response uses aliases as keys.
                    let response_key = field
                        .alias
                        .as_ref()
                        .map_or_else(|| field_name, |a| a.as_ref());
                    let new_path = format!("{current_path}/{response_key}");

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

                        let json_pointer = data_field.map(|f| {
                            let key = f
                                .alias
                                .as_ref()
                                .map_or_else(|| f.name.as_ref(), |a| a.as_ref());
                            format!("/data{current_path}/{key}")
                        });

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
                            tracing::warn!(
                                "GraphQL query has pagination specified ({pagination_argument}), but invalid pagination fields: {e}"
                            );
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

    fn apply(
        &self,
        query: &str,
        limit: Option<usize>,
        cursor: Option<String>,
        page_size_override: Option<usize>,
    ) -> Result<String> {
        let pattern = format!(r"{}\s*\(.*\)", self.resource_name);
        let regex = Regex::new(&pattern).context(InvalidPaginationRegexSnafu {
            resource_name: self.resource_name.clone(),
        })?;

        let arguments = self.parameters_string(limit, cursor, page_size_override);

        let new_query = regex.replace(
            query,
            format!(
                "{resource_name} ({arguments})",
                arguments = arguments.args,
                resource_name = self.resource_name,
            ),
        );

        Ok(new_query.to_string())
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
    new_object: &mut Map<String, Value>,
    key: String,
    duplicate_behavior: &DuplicateBehavior,
) -> Result<String> {
    match duplicate_behavior {
        DuplicateBehavior::Error => {
            if new_object.contains_key(&key) {
                return Err(Error::InvalidObjectAccess {
                    message: format!("Column '{key}' already exists in the object."),
                });
            }

            Ok(key)
        }
    }
}

pub fn unnest_json_object_to_depth(
    object: Value,
    depth: usize,
    duplicate_behavior: &DuplicateBehavior,
) -> Result<Vec<Value>> {
    match object {
        Value::Object(mut new_object) => {
            // setup some loop controls
            let mut depth_counter = 0;

            loop {
                if depth_counter >= depth {
                    break; // break if we've hit the unnest depth limit
                }

                // Move nested object entries up into the root; drop the parent keys.
                let mut additions = Vec::new();
                let mut remaining = Map::with_capacity(new_object.len());

                for (key, value) in new_object {
                    match value {
                        Value::Object(inner_obj) => {
                            additions.reserve(inner_obj.len());
                            for (inner_key, inner_value) in inner_obj {
                                additions.push((inner_key, inner_value));
                            }
                        }
                        other => {
                            remaining.insert(key, other);
                        }
                    }
                }

                new_object = remaining;

                if additions.is_empty() {
                    break; // break if there's nothing else to do
                }

                // add the staged additions back to the root object
                for (key, value) in additions {
                    let new_key = unnest_json_object_duplicate_columns(
                        &mut new_object,
                        key,
                        duplicate_behavior,
                    )?;

                    new_object.insert(new_key, value);
                }

                // increment the depth counter
                depth_counter += 1;
            }

            Ok(vec![Value::Object(new_object)])
        }
        Value::Array(arr) => Ok(arr),
        other => Err(Error::InvalidObjectAccess {
            // unnesting any other type is invalid
            message: format!("Unsupported unnest type: {other}"),
        }),
    }
}

fn unnest_json_object(unnest_parameters: &UnnestParameters, object: Value) -> Result<Vec<Value>> {
    match unnest_parameters.behavior {
        UnnestBehavior::Depth(depth) => {
            unnest_json_object_to_depth(object, depth, &unnest_parameters.duplicate_behavior)
        }
        UnnestBehavior::Custom(ref func) => func(&object),
    }
}

fn unnest_json_objects(
    unnest_parameters: &UnnestParameters,
    objects: Vec<Value>,
) -> Result<Vec<Value>> {
    Ok(objects
        .into_iter()
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
    rate_controller: Option<Arc<RateController>>,
    semaphore: Option<Arc<Semaphore>>,
    nested_pager: Option<NestedConnectionPager>,
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
        // Validate query is not empty or whitespace only
        if query.trim().is_empty() {
            tracing::debug!("GraphQL query validation failed: Query is empty");
            return Err(super::Error::InvalidGraphQLQuery {
                message: "Query cannot be empty".to_string(),
                line: 0,
                column: 0,
                query: query.to_string(),
            });
        }

        // SAFETY: We're transmuting the lifetime to 'static and this is safe because:
        // 1. The reference won't outlive the GraphQLQuery struct and we don't give it out as a static reference
        // 2. The source Arc is kept alive as long as the GraphQLQuery exists
        // 3. Arc guarantees the data remains at the same address
        //
        // This wouldn't be required if Rust had proper support for self-referencing structs.
        let query_ref: &'static str = unsafe { std::mem::transmute::<&str, &'static str>(&query) };

        let ast = parse_query::<String>(query_ref).map_err(|_| {
            tracing::debug!("GraphQL query parse failed. Query:\n{query}");
            super::Error::InvalidGraphQLQuery {
                message: "Failed to parse GraphQL query".to_string(),
                line: 0,
                column: 0,
                query: query.to_string(),
            }
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
    pub fn with_json_pointer(mut self, json_pointer: Arc<str>) -> Self {
        // Validate JSON pointer format (should start with / or be empty)
        if !json_pointer.is_empty() && !json_pointer.starts_with('/') {
            tracing::warn!("JSON pointer '{}' should start with '/'.", json_pointer);
        }
        self.json_pointer = Some(json_pointer);
        self
    }

    pub fn to_string(&self, limit: Option<usize>, cursor: Option<String>) -> Result<String> {
        self.to_string_with_page_size(limit, cursor, None)
    }

    /// Render the query to a string, optionally overriding the per-page size
    /// of the top-level paginated connection.
    ///
    /// `page_size_override` clamps the effective `first:` / `last:` value so
    /// that a single page never requests more than `page_size_override` rows.
    /// Unlike `limit`, it does not bound the total number of rows returned
    /// across all pages; cursor-driven pagination still continues normally.
    pub fn to_string_with_page_size(
        &self,
        limit: Option<usize>,
        cursor: Option<String>,
        page_size_override: Option<usize>,
    ) -> Result<String> {
        let query = self.ast.to_string();

        Ok(
            if let Some(pagination_parameters) = &self.pagination_parameters {
                pagination_parameters.apply(&query, limit, cursor, page_size_override)?
            } else {
                query
            },
        )
    }

    #[must_use]
    pub fn limit_reached(&self, limit: Option<usize>, record_count: usize) -> bool {
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
    limit_reached: bool,
    pub(crate) schema: SchemaRef,
    cursor: Option<String>,
}

impl GraphQLClient {
    #[expect(clippy::too_many_arguments)]
    pub fn new(
        client: reqwest::Client,
        endpoint: Url,
        json_pointer: Option<&str>,
        token: Option<Arc<dyn TokenProvider>>,
        user: Option<String>,
        pass: Option<String>,
        unnest_behavior: UnnestBehavior,
        schema: Option<SchemaRef>,
        rate_limiter: Option<Arc<dyn RateLimiter>>,
        rate_controller: Option<Arc<RateController>>,
        semaphore: Option<Arc<Semaphore>>,
        auth_header: Option<reqwest::header::HeaderName>,
    ) -> Result<Self> {
        // Validate unnest depth to prevent excessive recursion
        if let UnnestBehavior::Depth(depth) = &unnest_behavior
            && *depth > 50
        {
            return Err(Error::InvalidObjectAccess {
                message: format!("Unnest depth of {depth} exceeds maximum allowed depth of 50"),
            });
        }

        let auth = match (auth_header, token, user, pass) {
            // Custom header with token takes precedence when both are configured
            (Some(header_name), Some(token), _, _) => Some(Auth::CustomHeader(header_name, token)),
            // Bearer token without custom header
            (None, Some(token), _, _) => Some(Auth::Bearer(token)),
            // When no token is available but a username is provided, use Basic auth
            // regardless of whether a custom auth header was configured
            (Some(_), None, Some(user), pass) => {
                tracing::warn!(
                    "Custom auth header is configured without an auth token; falling back to Basic auth"
                );
                Some(Auth::Basic(user, pass))
            }
            (_, None, Some(user), pass) => Some(Auth::Basic(user, pass)),
            // Custom auth header configured without any credentials
            (Some(_), None, None, _) => {
                tracing::warn!(
                    "Custom auth header is configured but no credentials are provided; requests will be unauthenticated"
                );
                None
            }
            // No authentication configured
            _ => None,
        };

        let unnest_parameters = UnnestParameters {
            behavior: unnest_behavior,
            duplicate_behavior: DuplicateBehavior::Error,
        };

        let json_pointer = json_pointer.map(|p| {
            // Validate JSON pointer format
            if !p.is_empty() && !p.starts_with('/') {
                tracing::warn!("JSON pointer '{}' should start with '/'.", p);
            }
            Arc::from(p)
        });

        Ok(Self {
            client,
            endpoint,
            json_pointer,
            unnest_parameters,
            auth,
            schema,
            rate_limiter,
            rate_controller,
            semaphore,
            nested_pager: None,
        })
    }

    #[must_use]
    pub(crate) fn with_nested_pager(mut self, pager: Option<NestedConnectionPager>) -> Self {
        self.nested_pager = pager;
        self
    }

    #[must_use]
    pub(crate) fn configured_schema(&self) -> Option<SchemaRef> {
        self.schema.as_ref().map(Arc::clone)
    }

    pub(crate) async fn execute(
        &self,
        query: &GraphQLQuery,
        schema: Option<SchemaRef>,
        limit: Option<usize>,
        cursor: Option<String>,
        error_checker: Option<ErrorChecker>,
        query_cost: Option<u32>,
    ) -> Result<GraphQLQueryResult> {
        self.execute_inner(
            query,
            schema,
            limit,
            cursor,
            error_checker,
            query_cost,
            false,
            None,
        )
        .await
    }

    /// Runs a query for its error signal alone and discards the payload.
    ///
    /// A health check asks one question: does the resource exist, and can these
    /// credentials see it. Its response is a probe — an id and a name — not a
    /// row, so parsing it with the table's schema and unnest can only invent
    /// failures. The probe carries none of the columns the table declares, and
    /// one the table declares non-null then reads back as an unmasked null,
    /// failing a dataset whose data is fine. Every check that answers the
    /// health question runs before the payload is parsed, so stopping there is
    /// the whole check.
    pub(crate) async fn execute_health_check(
        &self,
        query: &GraphQLQuery,
        error_checker: Option<ErrorChecker>,
    ) -> Result<()> {
        let response = self
            .fetch_checked(query, None, None, error_checker, None, false, None)
            .await?;

        check_health_payload(self.resolve_json_pointer(query)?, &response)
    }

    #[expect(clippy::too_many_arguments)]
    async fn execute_inner(
        &self,
        query: &GraphQLQuery,
        schema: Option<SchemaRef>,
        limit: Option<usize>,
        cursor: Option<String>,
        error_checker: Option<ErrorChecker>,
        query_cost: Option<u32>,
        close_connection: bool,
        page_size_override: Option<usize>,
    ) -> Result<GraphQLQueryResult> {
        let response = self
            .fetch_checked(
                query,
                limit,
                cursor.as_deref(),
                error_checker.clone(),
                query_cost,
                close_connection,
                page_size_override,
            )
            .await?;

        self.process_response(
            query,
            schema.as_ref(),
            limit,
            cursor.as_deref(),
            &response,
            error_checker,
            query_cost,
        )
        .await
    }

    /// Sends one query and returns its decoded response once every check that can
    /// fail the request as a whole has passed: the HTTP status, the GraphQL
    /// `errors` array, and the connector's own error checker.
    ///
    /// Split from [`Self::execute_inner`] because a health check wants exactly
    /// this and nothing after it — see [`Self::execute_health_check`].
    #[expect(clippy::too_many_arguments)]
    async fn fetch_checked(
        &self,
        query: &GraphQLQuery,
        limit: Option<usize>,
        cursor: Option<&str>,
        error_checker: Option<ErrorChecker>,
        query_cost: Option<u32>,
        close_connection: bool,
        page_size_override: Option<usize>,
    ) -> Result<serde_json::Value> {
        // Validate cursor if present
        if let Some(cursor_val) = cursor {
            if cursor_val.is_empty() {
                tracing::warn!("Empty cursor provided, this may cause unexpected behavior");
            }
            if cursor_val.len() > 10000 {
                return Err(Error::InvalidObjectAccess {
                    message: format!(
                        "Cursor is too long ({} bytes). This may indicate a malformed cursor.",
                        cursor_val.len()
                    ),
                });
            }
        }

        // Check rate limit before executing the query
        let github_rate_limit_started = Instant::now();
        if let Some(rate_limiter) = &self.rate_limiter {
            rate_limiter
                .check_rate_limit()
                .await
                .map_err(|e| Error::RateLimited {
                    message: format!("{e}"),
                })?;
        }
        let github_rate_limit_wait = github_rate_limit_started.elapsed();

        let weighted_limiter_started = Instant::now();
        let rate_controller_permit = if let Some(rate_controller) = &self.rate_controller {
            Some(
                rate_controller
                    .acquire_weighted_opt(query_cost)
                    .await
                    .map_err(|e| Error::RateLimited {
                        message: format!("{e}"),
                    })?,
            )
        } else {
            None
        };
        let weighted_limiter_wait = weighted_limiter_started.elapsed();

        let query_string = query.to_string_with_page_size(
            limit,
            cursor.map(ToString::to_string),
            page_size_override,
        )?;

        // Validate query string is not empty
        if query_string.trim().is_empty() {
            tracing::debug!("GraphQL query validation failed: Generated query string is empty");
            return Err(Error::InvalidGraphQLQuery {
                message: "Generated query string is empty".to_string(),
                line: 0,
                column: 0,
                query: query_string,
            });
        }

        let body = format!(r#"{{"query": {}}}"#, json!(query_string));

        // When close_connection is true (after a gateway error like 502), build a
        // fresh reqwest::Client so the retry goes out on a new TCP connection instead
        // of reusing the (possibly broken) pooled connection. Preserve user-agent and
        // timeouts to match the original client — GitHub requires a User-Agent header.
        let http_client = if close_connection {
            reqwest::Client::builder()
                .user_agent(util::spiceai_user_agent())
                .pool_max_idle_per_host(0)
                .build()
                .context(ReqwestInternalSnafu)?
        } else {
            self.client.clone()
        };

        let mut request = http_client.post(self.endpoint.clone()).body(body);
        request = request_with_auth(request, self.auth.as_ref());

        // Replace separated semaphore with RateController semaphore: https://github.com/spiceai/spiceai/issues/8636
        let semaphore_started = Instant::now();
        let permit = if let Some(semaphore) = &self.semaphore {
            Some(
                semaphore
                    .acquire()
                    .await
                    .map_err(|e| Error::InternalError {
                        message: e.to_string(),
                    })?,
            )
        } else {
            None
        };
        let semaphore_wait = semaphore_started.elapsed();

        let http_started = Instant::now();
        let response = request.send().await.context(ReqwestInternalSnafu)?;

        if let Some(permit) = permit {
            drop(permit);
        }

        if let Some(rate_controller_permit) = rate_controller_permit {
            drop(rate_controller_permit);
        }

        let response_headers = response.headers().clone();

        // Update rate limiter with response headers
        if let Some(rate_limiter) = &self.rate_limiter {
            rate_limiter.update_from_headers(&response_headers).await;
        }

        let status = response.status();

        // Get the response body as text first, so we can log it if JSON parsing fails
        let response_text = response.text().await.context(ReqwestInternalSnafu)?;
        let http_elapsed = http_started.elapsed();

        let header = |name: &str| response_headers.get(name).and_then(|v| v.to_str().ok());
        tracing::debug!(
            endpoint = %self.endpoint,
            query_cost,
            page_size_override,
            has_cursor = cursor.is_some(),
            github_rate_limit_wait_ms = github_rate_limit_wait.as_millis(),
            weighted_limiter_wait_ms = weighted_limiter_wait.as_millis(),
            semaphore_wait_ms = semaphore_wait.as_millis(),
            http_ms = http_elapsed.as_millis(),
            response_bytes = response_text.len(),
            http_status = status.as_u16(),
            ratelimit_limit = header("x-ratelimit-limit"),
            ratelimit_remaining = header("x-ratelimit-remaining"),
            ratelimit_used = header("x-ratelimit-used"),
            ratelimit_resource = header("x-ratelimit-resource"),
            ratelimit_reset = header("x-ratelimit-reset"),
            "GraphQL page fetch"
        );

        // Try to parse as JSON
        let response: serde_json::Value = serde_json::from_str(&response_text)
            .map_err(|e| {
                let preview = response_text.chars().take(1000).collect::<String>();
                tracing::error!(
                    "Failed to decode response body as JSON.\nHTTP Status: {}\nJSON Parse Error: {}\nResponse body preview (first 1000 chars):\n{}",
                    status,
                    e,
                    preview
                );

                // For server errors returning HTML (e.g., upstream gateway/proxy errors),
                // provide a clear message instead of exposing the JSON parse error.
                let detail = if status.is_server_error() {
                    "The server returned a non-JSON response (likely an upstream proxy error). This is a temporary issue and will be retried automatically. If the problem persists, contact support or check the API status page.".to_string()
                } else {
                    format!(
                        "The response could not be parsed as JSON. Technical details: {e}"
                    )
                };

                Error::JsonDecodeError {
                    status,
                    detail,
                    response_preview: preview,
                }
            })?;

        // Full payload is only useful when debugging a malformed page; per-page
        // timing lives on the `GraphQL page fetch` line above.
        tracing::trace!(
            "GraphQL response: {}",
            serde_json::to_string_pretty(&response).unwrap_or_else(|_| format!("{response:?}"))
        );

        // Check for errors before processing data
        handle_http_error(status, &response)?;
        handle_graphql_query_error(&response, &query_string)?;

        // Custom error checker (e.g., for GitHub rate limits)
        error_checker
            .map(|p| p(&response_headers, &response))
            .transpose()?;

        Ok(response)
    }

    /// The result for a page that yielded no rows, keeping whichever schema the table is
    /// configured with so an empty page cannot narrow it.
    ///
    /// Every early exit out of [`Self::process_response`] goes through here: the schema a rowless
    /// page reports is the one rule they must agree on, so it lives in one place.
    fn empty_page(
        &self,
        schema: Option<&SchemaRef>,
        cursor: Option<String>,
    ) -> Result<GraphQLQueryResult> {
        Ok(GraphQLQueryResult {
            records: vec![],
            limit_reached: false,
            // No rows to infer from, so this resolves to the override or the configured schema.
            schema: get_json_schema(self.schema.as_ref(), schema, &[])?,
            cursor,
        })
    }

    /// The data path a response is read at: the query's own pointer, else the one
    /// configured on the client. Both the row path and the health check resolve it
    /// here so they cannot drift.
    fn resolve_json_pointer<'a>(&'a self, query: &'a GraphQLQuery) -> Result<&'a str> {
        let json_pointer = query
            .json_pointer
            .as_ref()
            .or(self.json_pointer.as_ref())
            .ok_or(Error::NoJsonPointerFound {})?;

        // Validate JSON pointer is not empty
        if json_pointer.is_empty() {
            return Err(Error::InvalidJsonPointer {
                pointer: "JSON pointer cannot be empty".to_string(),
            });
        }

        Ok(json_pointer)
    }

    /// Remaining nested-connection pages for parents whose first page was truncated.
    ///
    /// Each overflow parent is fetched independently so two truncated children
    /// on one outer page do not serialize. Pages of one parent stay sequential
    /// because GitHub's `after:` cursor requires the previous page.
    async fn fetch_nested_overflow_pages(
        &self,
        parents: &[Value],
        pager: &NestedConnectionPager,
        error_checker: Option<ErrorChecker>,
        query_cost: Option<u32>,
    ) -> Result<Vec<Value>> {
        let fetches = parents.iter().filter_map(|parent| {
            nested_connection(parent, pager)
                .filter(|connection| nested_has_next(connection))
                .map(|_| {
                    self.fetch_remaining_nested_pages(
                        parent,
                        pager,
                        error_checker.clone(),
                        query_cost,
                    )
                })
        });

        let extra_parents = try_join_all(fetches).await?;
        Ok(extra_parents.into_iter().flatten().collect())
    }

    async fn fetch_remaining_nested_pages(
        &self,
        parent: &Value,
        pager: &NestedConnectionPager,
        error_checker: Option<ErrorChecker>,
        query_cost: Option<u32>,
    ) -> Result<Vec<Value>> {
        let Some(connection) = nested_connection(parent, pager) else {
            return Ok(Vec::new());
        };
        let Some(cursor) = nested_end_cursor(connection) else {
            return Err(Error::InvalidObjectAccess {
                message: format!(
                    "Nested connection '{}' reported more pages but no endCursor.",
                    pager.connection_key
                ),
            });
        };
        let Some(parent_id) = parent.get(pager.parent_id_key).and_then(Value::as_str) else {
            return Err(Error::InvalidObjectAccess {
                message: format!(
                    "Nested connection '{}' needs parent id field '{}'.",
                    pager.connection_key, pager.parent_id_key
                ),
            });
        };

        // The parent's own fields, without the first page this loop replaces on
        // every iteration.
        let mut parent_fields = parent.as_object().cloned().unwrap_or_default();
        parent_fields.remove(pager.connection_key);

        let mut extras = Vec::new();
        let mut cursor = cursor.to_string();
        for _ in 0..MAX_PAGINATION_ITERATIONS {
            let mut query: GraphQLQuery =
                Arc::<str>::from(pager.next_page_query(parent_id, &cursor)).try_into()?;
            // The follow-up query already names `after:`; do not let outer-page
            // pagination rewrite it.
            query.pagination_parameters = None;
            let mut response = self
                .fetch_checked(
                    &query,
                    None,
                    None,
                    error_checker.clone(),
                    query_cost,
                    false,
                    None,
                )
                .await?;

            let next_connection = response
                .get_mut("data")
                .and_then(|data| data.get_mut("node"))
                .and_then(|node| node.get_mut(pager.connection_key))
                .map(Value::take);
            let Some(next_connection) = next_connection else {
                return Err(Error::InvalidObjectAccess {
                    message: format!(
                        "Follow-up page for '{}' returned no connection.",
                        pager.connection_key
                    ),
                });
            };

            let has_next = nested_has_next(&next_connection);
            let next_cursor = nested_end_cursor(&next_connection).map(str::to_string);

            let mut synthetic = parent_fields.clone();
            synthetic.insert(pager.connection_key.to_string(), next_connection);
            extras.push(Value::Object(synthetic));

            if !has_next {
                return Ok(extras);
            }
            cursor = next_cursor.ok_or_else(|| Error::InvalidObjectAccess {
                message: format!(
                    "Follow-up page for '{}' was truncated without an endCursor.",
                    pager.connection_key
                ),
            })?;
        }

        Err(Error::InvalidObjectAccess {
            message: format!(
                "Nested connection '{}' exceeded {MAX_PAGINATION_ITERATIONS} follow-up pages.",
                pager.connection_key
            ),
        })
    }

    /// Turn a decoded GraphQL response body into record batches, resolving the schema each page is
    /// parsed with. Split out from `execute_inner` so the page-shape handling — null payload, empty
    /// page, repeated cursor — is reachable without an HTTP round trip.
    #[expect(clippy::too_many_arguments)]
    async fn process_response(
        &self,
        query: &GraphQLQuery,
        schema: Option<&SchemaRef>,
        limit: Option<usize>,
        cursor: Option<&str>,
        response: &serde_json::Value,
        error_checker: Option<ErrorChecker>,
        query_cost: Option<u32>,
    ) -> Result<GraphQLQueryResult> {
        let json_pointer = self.resolve_json_pointer(query)?;

        let extracted_data = response
            .pointer(json_pointer)
            .ok_or_else(|| {
                // If we can't find the data at the expected path, check if there are errors in the response
                let error_msg = if let Some(errors) = response.get("errors") {
                    format!("GraphQL query failed. Errors: {errors}")
                } else {
                    format!("Invalid JSON pointer: '{json_pointer}'. The expected data path was not found in the response.")
                };
                tracing::error!("Failed to extract data from response. Full response: {}", serde_json::to_string_pretty(response).unwrap_or_else(|_| format!("{response:?}")));
                Error::InvalidJsonPointer {
                    pointer: error_msg,
                }
            })?
            .to_owned();

        // Handle null data explicitly
        if extracted_data.is_null() {
            tracing::debug!("Extracted data at pointer '{json_pointer}' is null");
            return self.empty_page(schema, None);
        }

        let next_cursor = query
            .pagination_parameters
            .as_ref()
            .and_then(|x| x.get_next_cursor_from_response(response));

        // Validate next cursor if present
        if let Some(ref next_cursor_val) = next_cursor {
            if next_cursor_val.is_empty() {
                tracing::warn!("Empty cursor returned from pagination, stopping pagination");
            }
            // Detect potential infinite loop - same cursor returned
            if cursor == Some(next_cursor_val.as_str()) {
                tracing::warn!(
                    "Same cursor returned from pagination, stopping to prevent infinite loop"
                );
                // Use limit_reached: false for loop protection exits, not data limit exhaustion
                return self.empty_page(schema, None);
            }
        }

        let mut unwrapped = match extracted_data {
            Value::Array(val) => Ok(val),
            obj @ Value::Object(_) => Ok(vec![obj]),
            _ => Err(Error::InvalidObjectAccess {
                message: format!("GraphQL response has unexpected format. Response {response:?}"),
            }),
        }?;

        // Validate we have data to process
        if unwrapped.is_empty() {
            tracing::debug!("No data to process after extraction");
            return self.empty_page(schema, next_cursor);
        }

        if let Some(pager) = &self.nested_pager {
            let extra_parents = self
                .fetch_nested_overflow_pages(&unwrapped, pager, error_checker, query_cost)
                .await?;
            unwrapped.extend(extra_parents);
        }

        unwrapped = match self.unnest_parameters.behavior {
            UnnestBehavior::Depth(0) => unwrapped,
            UnnestBehavior::Depth(_) | UnnestBehavior::Custom(_) => {
                unnest_json_objects(&self.unnest_parameters, unwrapped)?
            }
        };

        let schema = get_json_schema(self.schema.as_ref(), schema, &unwrapped)?;

        let mut res = vec![];
        for v in unwrapped {
            let buf = v.to_string();

            // Validate JSON is not too large
            if buf.len() > 100_000_000 {
                tracing::warn!(
                    "JSON object is very large ({} bytes), this may cause memory issues",
                    buf.len()
                );
            }

            let batch_result = ReaderBuilder::new(Arc::clone(&schema))
                .with_batch_size(1024)
                .build(Cursor::new(buf.as_bytes()))
                .context(ArrowInternalSnafu)?
                .collect::<Result<Vec<_>, _>>();

            match batch_result {
                Ok(batch) => res.extend(batch),
                Err(e) => {
                    // Check if there are errors in the original response that might explain the schema mismatch
                    let error_context = if let Some(errors) = response.get("errors") {
                        format!(
                            "The API returned errors: {errors}. This may have caused the data schema to be incomplete or malformed."
                        )
                    } else {
                        "The response data does not match the expected schema. This may indicate an API error or unexpected response format.".to_string()
                    };

                    let sample = serde_json::to_string_pretty(&v).unwrap_or_else(|_| v.to_string());
                    let error_msg = format!(
                        "Failed to parse response into record batch. {error_context}\n\nOriginal error: {e}\n\nResponse data sample: {sample}"
                    );

                    tracing::error!("{}", error_msg);
                    tracing::debug!("Schema being used: {:?}", schema);

                    // Preserve the original ArrowError to maintain error classification
                    return Err(Error::ArrowInternal { source: e });
                }
            }
        }

        let limit_reached = query.limit_reached(limit, res.len());

        Ok(GraphQLQueryResult {
            records: res,
            limit_reached,
            schema: Arc::clone(&schema),
            cursor: next_cursor,
        })
    }

    #[must_use]
    pub fn execute_paginated(
        self: Arc<Self>,
        query: GraphQLQuery,
        gql_schema: SchemaRef,
        table_schema: SchemaRef,
        limit: Option<usize>,
        error_checker: Option<ErrorChecker>,
        query_cost: Option<u32>,
    ) -> SendableRecordBatchStream {
        let mut builder = RecordBatchReceiverStream::builder(table_schema, 2);
        let tx = builder.tx();

        // Spawn the task that will fetch and send the GraphQL record batches
        builder.spawn(async move {
            let scan_started = Instant::now();
            let mut total_rows = 0usize;

            // Track pagination iterations to prevent infinite loops
            let mut pagination_count = 0;

            // Execute initial page with retry
            let mut result = Self::execute_with_retry(
                &self,
                &query,
                Some(Arc::clone(&gql_schema)),
                limit,
                None,
                error_checker.clone(),
                query_cost,
            )
            .await
            .map_err(|e| DataFusionError::Execution(e.to_string()))?;
            let mut limit = limit;

            let first_page_rows: usize = result.records.iter().map(RecordBatch::num_rows).sum();
            total_rows += first_page_rows;
            // What the remaining `limit` is debited by before the next page is
            // requested. It is the rows that arrived, never the page size the
            // query declares: a gateway-error retry shrinks the page below that
            // size, and a connection-style API may return a short page for
            // reasons of its own. Debiting the declared size retires rows that
            // were never fetched, so a `LIMIT n` scan ends early and answers
            // with fewer than `n` rows and no error (#14308).
            let mut last_page_rows = first_page_rows;
            tracing::debug!(
                page = 0,
                page_rows = first_page_rows,
                total_rows,
                has_next = result.cursor.is_some(),
                query_cost,
                elapsed_ms = scan_started.elapsed().as_millis(),
                "GraphQL page"
            );

            for batch in result.records {
                tx.send(Ok(batch)).await.map_err(|_| {
                    DataFusionError::Execution("Failed to send record batch".to_string())
                })?;
            }

            if result.limit_reached {
                tracing::debug!(
                    pages = 1,
                    total_rows,
                    elapsed_ms = scan_started.elapsed().as_millis(),
                    "GraphQL scan complete"
                );
                return Ok(());
            }

            let mut previous_cursor: Option<String> = None;

            while let Some(next_cursor_val) = result.cursor {
                pagination_count += 1;

                // Prevent infinite pagination loops
                if pagination_count > MAX_PAGINATION_ITERATIONS {
                    tracing::error!(
                        "Maximum pagination iterations ({}) exceeded, stopping pagination",
                        MAX_PAGINATION_ITERATIONS
                    );
                    return Err(DataFusionError::Execution(format!(
                        "Maximum pagination iterations ({MAX_PAGINATION_ITERATIONS}) exceeded"
                    )));
                }

                // Detect cursor loops
                if previous_cursor.as_ref() == Some(&next_cursor_val) {
                    tracing::warn!("Cursor loop detected, stopping pagination");
                    break;
                }

                if let Some(value) = limit {
                    limit = Some(value.saturating_sub(last_page_rows));

                    // Stop if limit is exhausted
                    if limit == Some(0) {
                        break;
                    }
                }

                previous_cursor = Some(next_cursor_val.clone());

                // Execute subsequent pages with retry
                result = Self::execute_with_retry(
                    &self,
                    &query,
                    Some(Arc::clone(&gql_schema)),
                    limit,
                    Some(next_cursor_val),
                    error_checker.clone(),
                    query_cost,
                )
                .await
                .map_err(|e| DataFusionError::Execution(e.to_string()))?;

                let page_rows: usize = result.records.iter().map(RecordBatch::num_rows).sum();
                total_rows += page_rows;
                last_page_rows = page_rows;
                tracing::debug!(
                    page = pagination_count,
                    page_rows,
                    total_rows,
                    has_next = result.cursor.is_some(),
                    query_cost,
                    elapsed_ms = scan_started.elapsed().as_millis(),
                    "GraphQL page"
                );

                for batch in result.records {
                    tx.send(Ok(batch)).await.map_err(|_| {
                        DataFusionError::Execution("Failed to send record batch".to_string())
                    })?;
                }

                if result.limit_reached {
                    break;
                }
            }
            tracing::debug!(
                pages = pagination_count + 1,
                total_rows,
                elapsed_ms = scan_started.elapsed().as_millis(),
                "GraphQL scan complete"
            );
            Ok(())
        });

        builder.build()
    }

    /// Executes a GraphQL query with page-level retry for transient errors.
    ///
    /// Note: Rate limit handling (waiting until reset time) is done proactively by the
    /// `RateLimiter` trait via `check_rate_limit()` before each request.
    ///
    /// On gateway errors (HTTP 502/504) the next retry is sent with a smaller
    /// per-page size, shrinking along a reverse-Fibonacci sequence. A 502 from
    /// an upstream proxy commonly means the GitHub GraphQL backend timed out
    /// while resolving an oversized query; requesting a smaller page gives the
    /// backend a chance to complete within its per-request deadline instead of
    /// replaying the exact same failing query.
    async fn execute_with_retry(
        client: &Arc<Self>,
        query: &GraphQLQuery,
        schema: Option<SchemaRef>,
        limit: Option<usize>,
        cursor: Option<String>,
        error_checker: Option<ErrorChecker>,
        query_cost: Option<u32>,
    ) -> Result<GraphQLQueryResult> {
        let backoff = FibonacciBackoffBuilder::new()
            .max_retries(Some(PAGE_RETRY_MAX_ATTEMPTS as usize))
            .build();

        let close_connection = Arc::new(AtomicBool::new(false));

        // Seed the shrink sequence with the query's declared page size (if any).
        // `None` means "no override" for the first attempt; the query's own
        // hard-coded `first:` value is used. On the first gateway error we seed
        // the override from `pagination_argument.size()` and then reverse-Fib
        // downward on subsequent gateway errors.
        let page_size_override: Arc<std::sync::Mutex<Option<usize>>> =
            Arc::new(std::sync::Mutex::new(None));

        retry(backoff, || {
            let schema = schema.clone();
            let cursor = cursor.clone();
            let error_checker = error_checker.clone();
            let should_close = close_connection.swap(false, Ordering::Relaxed);
            let close_conn = Arc::clone(&close_connection);
            let page_size_override_current = {
                let guard = page_size_override
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                *guard
            };
            let page_size_override_ref = Arc::clone(&page_size_override);

            async move {
                client
                    .execute_inner(
                        query,
                        schema,
                        limit,
                        cursor,
                        error_checker,
                        query_cost,
                        should_close,
                        page_size_override_current,
                    )
                    .await
                    .map_err(|e| {
                        if is_retriable_error(&e) {
                            if matches!(
                                &e,
                                Error::JsonDecodeError { status, .. } if status.is_success()
                            ) {
                                // Truncated HTTP 200: the pooled connection is
                                // likely half-closed. Retry on a new TCP stream.
                                close_conn.store(true, Ordering::Relaxed);
                            }
                            if is_gateway_error(&e) {
                                close_conn.store(true, Ordering::Relaxed);
                                // Shrink the per-page size for the next retry.
                                // Seed from the query's declared page size on
                                // the first gateway error, then reverse-Fib.
                                let mut guard = page_size_override_ref
                                    .lock()
                                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                                let current = guard.unwrap_or_else(|| {
                                    query
                                        .pagination_parameters
                                        .as_ref()
                                        .map_or(GATEWAY_SHRINK_DEFAULT_PAGE_SIZE, |p| {
                                            p.pagination_argument.size()
                                        })
                                });
                                let next = reverse_fibonacci_shrink(current);
                                tracing::warn!(
                                    "Gateway error; shrinking GraphQL page size for retry: {current} -> {next}"
                                );
                                *guard = Some(next);
                            }
                            tracing::warn!("Page fetch failed, will retry: {e}");
                            RetryError::transient(e)
                        } else {
                            RetryError::permanent(e)
                        }
                    })
            }
        })
        .await
    }
}

/// The health check's claim on the payload: the data path the query declares has
/// to be present, and it has to name something.
///
/// The two ways it can fail are different faults and are reported as such. A path
/// that does not resolve means the query and the pointer disagree — a
/// misconfiguration no response will ever fix. A path that resolves to `null` is
/// the API answering "no such resource", and it has to fail here: the row query
/// reads a null payload as an empty page, so a name that does not exist would
/// otherwise attach as a valid, permanently empty dataset instead of telling the
/// user their path is wrong.
fn check_health_payload(json_pointer: &str, response: &Value) -> Result<()> {
    match response.pointer(json_pointer) {
        Some(Value::Null) => {
            tracing::debug!(
                "Health check resolved '{json_pointer}' to null. Full response: {}",
                serde_json::to_string_pretty(response).unwrap_or_else(|_| format!("{response:?}"))
            );

            Err(Error::ResourceNotFound {
                message: "The API answered with no such resource. Verify every name in the dataset's 'from' path exists and the credentials can see it. For details, visit: https://spiceai.org/docs/components/data-connectors/graphql".to_string(),
            })
        }
        Some(_) => Ok(()),
        None => {
            tracing::error!(
                "Health check response has no data at '{json_pointer}'. Full response: {}",
                serde_json::to_string_pretty(response).unwrap_or_else(|_| format!("{response:?}"))
            );

            Err(Error::InvalidJsonPointer {
                pointer: format!(
                    "Invalid JSON pointer: '{json_pointer}'. The expected data path was not found in the response."
                ),
            })
        }
    }
}

/// Resolve the schema a page's rows are parsed with: per-query override, then the schema
/// configured on the client, then inference from the rows themselves.
///
/// An empty `json_iter` means there are no rows to infer from, so the last step yields an empty
/// schema rather than guessing one.
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

    // Handle empty array case
    if json_iter.is_empty() {
        tracing::debug!("Cannot infer schema from empty array, using empty schema");
        return Ok(Arc::new(arrow::datatypes::Schema::empty()));
    }

    let schema = infer_json_schema_from_iterator(json_iter.iter().map(Result::Ok))
        .context(ArrowInternalSnafu)?;

    Ok(Arc::new(schema))
}

fn request_with_auth(request_builder: RequestBuilder, auth: Option<&Auth>) -> RequestBuilder {
    match auth {
        Some(Auth::Basic(user, pass)) => request_builder.basic_auth(user, pass.clone()),
        Some(Auth::Bearer(token_provider)) => {
            request_builder.bearer_auth(token_provider.get_token())
        }
        Some(Auth::CustomHeader(header_name, token_provider)) => {
            request_builder.header(header_name.clone(), token_provider.get_token())
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

        let message_lower = message.to_ascii_lowercase();

        if status == StatusCode::TOO_MANY_REQUESTS || message_lower.contains("rate limit") {
            return Err(Error::RateLimited {
                message: format!(
                    "The API rate limited the request (HTTP {status}). Retry later or reduce request concurrency. Details: {message}"
                ),
            });
        }

        return match status {
            StatusCode::UNAUTHORIZED => Err(Error::InvalidCredentialsOrPermissions {
                message: format!(
                    "The API failed with status code {status}. Verify the provided credentials are correct."
                ),
            }),
            StatusCode::FORBIDDEN => Err(Error::InvalidCredentialsOrPermissions {
                message: format!(
                    "The API failed with status code {status}. Verify the provided credentials have the necessary permissions."
                ),
            }),
            StatusCode::GATEWAY_TIMEOUT | StatusCode::REQUEST_TIMEOUT => {
                Err(Error::InvalidReqwestStatus {
                    status,
                    message: format!(
                        "The API request timed out (HTTP {status}). This is often a transient issue. The data refresh will be retried automatically. If the problem persists, consider reducing query complexity or page size. Details: {message}"
                    ),
                })
            }
            StatusCode::BAD_GATEWAY | StatusCode::SERVICE_UNAVAILABLE => {
                Err(Error::InvalidReqwestStatus {
                    status,
                    message: format!(
                        "The API service is temporarily unavailable (HTTP {status}). This is often a transient issue. The data refresh will be retried automatically. Details: {message}"
                    ),
                })
            }
            _ if status.is_server_error() => Err(Error::InvalidReqwestStatus {
                status,
                message: format!(
                    "The API server returned an error (HTTP {status}). This may be a transient issue. The data refresh will be retried automatically. Details: {message}"
                ),
            }),
            _ => Err(Error::InvalidReqwestStatus { status, message }),
        };
    }
    Ok(())
}

fn handle_graphql_query_error(response: &Value, query: &str) -> Result<()> {
    // Check if there are any errors in the response
    if let Some(errors) = response.get("errors") {
        if let Some(errors_array) = errors.as_array() {
            if errors_array.is_empty() {
                return Ok(());
            }

            // GitHub bug: When the app doesn't have access to Projects v2, GitHub sometimes
            // returns "Something went wrong while executing your query" instead of a proper
            // permission error. This appears to be a GitHub API bug where lack of permissions
            // triggers an internal error rather than returning a proper authorization error.
            // Check for this before processing other GraphQL errors.
            for error in errors_array {
                if let Some(message) = error.get("message").and_then(|m| m.as_str())
                    && message.contains("Something went wrong while executing your query")
                {
                    tracing::debug!(
                        "Detected GitHub 'Something went wrong' error, likely a permissions issue: {}",
                        message
                    );
                    return Err(Error::InvalidCredentialsOrPermissions {
                        message: "GitHub returned an internal error. This may indicate the GitHub App does not have permission to access the requested resource. Verify the app has the required permissions.".to_string(),
                    });
                }
            }
        } else if errors.is_null() {
            return Ok(());
        }
    } else {
        return Ok(());
    }

    // Safely access the first error with bounds checking
    let graphql_error = response
        .get("errors")
        .and_then(|e| e.as_array())
        .and_then(|arr| arr.first())
        .unwrap_or(&Value::Null);

    if !graphql_error.is_null() {
        let line = graphql_error
            .get("locations")
            .and_then(|l| l.as_array())
            .and_then(|arr| arr.first())
            .and_then(|loc| loc.get("line"))
            .and_then(serde_json::Value::as_u64);

        let column = graphql_error
            .get("locations")
            .and_then(|l| l.as_array())
            .and_then(|arr| arr.first())
            .and_then(|loc| loc.get("column"))
            .and_then(serde_json::Value::as_u64);

        let error_type = graphql_error.get("type").and_then(|t| t.as_str());

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
                return Err(Error::InvalidCredentialsOrPermissions {
                    message: format!(
                        "The API returned a 'FORBIDDEN' error. Verify the credentials have the necessary permissions. {message}"
                    ),
                });
            }
            if error_type.to_lowercase() == "not_found" {
                return Err(Error::ResourceNotFound {
                    message: format!(
                        "The API returned a 'NOT_FOUND' error. Verify the requested resource exists and is accessible. {message}"
                    ),
                });
            }
        }

        return if let Some((line, column)) = location {
            tracing::debug!(
                "GraphQL error at line {line}, column {column}: {message}\nQuery:\n{}",
                format_query_with_context(query, line, column)
            );
            Err(Error::InvalidGraphQLQuery {
                message,
                line,
                column,
                query: format_query_with_context(query, line, column),
            })
        } else {
            tracing::debug!("GraphQL error: {message}\nQuery:\n{}", query.to_string());
            Err(Error::InvalidGraphQLQuery {
                message,
                line: 0,
                column: 0,
                query: query.to_string(),
            })
        };
    }
    Ok(())
}

fn format_query_with_context(query: &str, line: usize, column: usize) -> String {
    if line == 0 || column == 0 {
        return query.to_string();
    }
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
    use url::Url;

    use crate::graphql::client::GraphQLQuery;

    use super::{DuplicateBehavior, PaginationParameters, UnnestBehavior, handle_http_error};

    mod health_check_payload {
        use serde_json::json;

        use crate::graphql::client::check_health_payload;

        /// The probe carries whatever the health-check query asked for, which is
        /// never the table's row shape. All the check asks is that the path the
        /// query declares is there.
        #[test]
        fn a_probe_that_resolves_passes() {
            check_health_payload(
                "/data/healthCheckProbe",
                &json!({"data": {"healthCheckProbe": {"id": "ORG_1", "login": "spiceai"}}}),
            )
            .expect("a resolved probe is a healthy resource");
        }

        /// A `null` there is the API answering "no such resource", and it has to
        /// fail here: the row path reads a null payload as an empty page, so a
        /// name that does not exist would otherwise attach as a valid,
        /// permanently empty dataset.
        #[test]
        fn a_null_probe_is_a_resource_that_does_not_exist() {
            let err = check_health_payload(
                "/data/healthCheckProbe",
                &json!({"data": {"healthCheckProbe": null}}),
            )
            .expect_err("a name that resolves to nothing is not a healthy dataset");

            assert!(
                matches!(err, super::super::Error::ResourceNotFound { .. }),
                "a missing resource is reported as such, not as a pointer fault: {err}"
            );
        }

        /// A path that does not resolve means the query and the pointer disagree,
        /// which no response will fix.
        #[test]
        fn a_pointer_the_query_does_not_answer_fails() {
            let err = check_health_payload(
                "/data/healthCheckProbe",
                &json!({"data": {"somethingElse": {"id": "ORG_1"}}}),
            )
            .expect_err("a pointer the query does not answer is a misconfiguration");

            assert!(
                err.to_string().contains("/data/healthCheckProbe"),
                "the message names the path that did not resolve: {err}"
            );
        }
    }

    /// The nested-pagination path end to end over a mock server: an outer page
    /// whose child connection is truncated, the `node(id:)` follow-ups, and the
    /// terminal page. The live full-history tests are `#[ignore]`, so without
    /// this nothing guards the feature in CI.
    mod nested_pagination {
        use std::sync::Arc;

        use arrow::array::{Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use serde_json::{Value, json};
        use url::Url;
        use wiremock::matchers::{body_string_contains, method};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        use crate::graphql::builder::GraphQLClientBuilder;
        use crate::graphql::client::{
            GraphQLQuery, NestedConnectionPager, UnnestBehavior, UnnestHandler,
        };

        const OUTER_QUERY: &str = "query { view(first: 10) {
            nodes { id reviews(first: 2) { totalCount pageInfo { hasNextPage endCursor } nodes { id } } }
            pageInfo { hasNextPage endCursor }
        } }";

        fn reviews_page(nodes: &[&str], has_next: bool, end_cursor: &str) -> Value {
            json!({
                "totalCount": 5,
                "pageInfo": {"hasNextPage": has_next, "endCursor": end_cursor},
                "nodes": nodes.iter().map(|id| json!({"id": id})).collect::<Vec<_>>(),
            })
        }

        #[tokio::test]
        async fn follow_up_pages_emit_every_child_once_and_request_cursors_in_order() {
            let server = MockServer::start().await;

            // Most specific first: the follow-ups are keyed on their cursor.
            Mock::given(method("POST"))
                .and(body_string_contains(r#"after: \"c2\""#))
                .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                    "data": {"node": {"reviews": reviews_page(&["R5"], false, "c3")}}
                })))
                .mount(&server)
                .await;

            Mock::given(method("POST"))
                .and(body_string_contains(r#"after: \"c1\""#))
                .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                    "data": {"node": {"reviews": reviews_page(&["R3", "R4"], true, "c2")}}
                })))
                .mount(&server)
                .await;

            Mock::given(method("POST"))
                .and(body_string_contains("view(first: 10)"))
                .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                    "data": {"view": {
                        "nodes": [{"id": "PR_1", "reviews": reviews_page(&["R1", "R2"], true, "c1")}],
                        "pageInfo": {"hasNextPage": false, "endCursor": Value::Null},
                    }}
                })))
                .mount(&server)
                .await;

            // Flatten each parent into one row per review, the way the GitHub
            // connector's fan-out does, so every child is countable.
            let unnest_reviews: UnnestHandler = Box::new(|parent: &Value| {
                Ok(parent
                    .get("reviews")
                    .and_then(|c| c.get("nodes"))
                    .and_then(Value::as_array)
                    .cloned()
                    .unwrap_or_default())
            });

            let client = GraphQLClientBuilder::new(
                Url::parse(&format!("{}/graphql", server.uri())).expect("valid URL"),
                UnnestBehavior::Custom(unnest_reviews),
            )
            .with_json_pointer(Some("/data/view/nodes"))
            .with_schema(Some(Arc::new(Schema::new(vec![Field::new(
                "id",
                DataType::Utf8,
                true,
            )]))))
            .with_nested_pager(Some(NestedConnectionPager {
                connection_key: "reviews",
                parent_id_key: "id",
                type_condition: "PullRequest",
                node_selection: "id",
                page_size: 2,
            }))
            .build(reqwest::Client::new())
            .expect("client to build");

            // The provider normally stamps the client's pointer onto the query;
            // inference would otherwise walk into the nested connection.
            let query = GraphQLQuery::try_from(Arc::<str>::from(OUTER_QUERY))
                .expect("query to parse")
                .with_json_pointer(Arc::from("/data/view/nodes"));
            let result = client
                .execute(&query, None, None, None, None, None)
                .await
                .expect("the nested connection to be completed");

            let mut ids: Vec<String> = Vec::new();
            for batch in &result.records {
                let column = batch
                    .column_by_name("id")
                    .expect("the id column")
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("id to be a string column");
                for i in 0..column.len() {
                    ids.push(column.value(i).to_string());
                }
            }

            assert_eq!(
                ids,
                vec!["R1", "R2", "R3", "R4", "R5"],
                "every child of the truncated connection must be emitted exactly once"
            );

            // `after:` forces the pages of one parent to be sequential.
            let bodies: Vec<String> = server
                .received_requests()
                .await
                .expect("recorded requests")
                .iter()
                .map(|r| String::from_utf8_lossy(&r.body).to_string())
                .collect();
            let cursor_order: Vec<&str> = bodies
                .iter()
                .filter_map(|b| {
                    if b.contains(r#"after: \"c1\""#) {
                        Some("c1")
                    } else if b.contains(r#"after: \"c2\""#) {
                        Some("c2")
                    } else {
                        None
                    }
                })
                .collect();
            assert_eq!(
                cursor_order,
                vec!["c1", "c2"],
                "follow-up pages of one parent must be requested in cursor order"
            );
        }
    }

    mod empty_page_schema {
        use std::sync::Arc;

        use arrow::array::RecordBatch;
        use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
        use serde_json::json;
        use url::Url;

        use crate::graphql::builder::GraphQLClientBuilder;
        use crate::graphql::client::{
            GraphQLClient, GraphQLQuery, GraphQLQueryResult, UnnestBehavior,
        };

        const UNPAGINATED_QUERY: &str = "query { view { nodes { id } } }";
        const PAGINATED_QUERY: &str =
            "query { view(first: 10) { nodes { id } pageInfo { hasNextPage endCursor } } }";

        fn configured_schema() -> SchemaRef {
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Utf8, true),
                Field::new("title", DataType::Utf8, true),
            ]))
        }

        fn client_with_schema(schema: Option<SchemaRef>) -> GraphQLClient {
            GraphQLClientBuilder::new(
                Url::parse("https://example.com/graphql").expect("valid URL"),
                UnnestBehavior::Depth(0),
            )
            .with_json_pointer(Some("/data/view/nodes"))
            .with_schema(schema)
            .build(reqwest::Client::new())
            .expect("client to build")
        }

        fn process(
            client: &GraphQLClient,
            query_str: &str,
            schema_override: Option<&SchemaRef>,
            cursor: Option<&str>,
            response: &serde_json::Value,
        ) -> GraphQLQueryResult {
            let query =
                GraphQLQuery::try_from(Arc::<str>::from(query_str)).expect("query to parse");

            futures::executor::block_on(client.process_response(
                &query,
                schema_override,
                None,
                cursor,
                response,
                None,
                None,
            ))
            .expect("an empty page is a valid response, not an error")
        }

        /// Regression test for #13004: a first page with no rows must not discard the configured
        /// schema, or preflight builds the table from `Schema::empty()` and the dataset fails to
        /// connect.
        #[test]
        fn empty_page_keeps_the_configured_schema() {
            let schema = configured_schema();
            let client = client_with_schema(Some(Arc::clone(&schema)));

            let result = process(
                &client,
                UNPAGINATED_QUERY,
                None,
                None,
                &json!({"data": {"view": {"nodes": []}}}),
            );

            assert!(result.records.is_empty(), "an empty page yields no rows");
            assert_eq!(result.schema, schema);
        }

        /// A null payload is the same shape as an empty page: GitHub returns `nodes: null` rather
        /// than `[]` for some resources.
        #[test]
        fn null_payload_keeps_the_configured_schema() {
            let schema = configured_schema();
            let client = client_with_schema(Some(Arc::clone(&schema)));

            let result = process(
                &client,
                UNPAGINATED_QUERY,
                None,
                None,
                &json!({"data": {"view": {"nodes": null}}}),
            );

            assert!(result.records.is_empty());
            assert_eq!(result.schema, schema);
        }

        /// The pagination loop-guard exit is the third early return out of the page handler, and it
        /// dropped the configured schema the same way the other two did.
        #[test]
        fn repeated_cursor_keeps_the_configured_schema() {
            let schema = configured_schema();
            let client = client_with_schema(Some(Arc::clone(&schema)));

            let result = process(
                &client,
                PAGINATED_QUERY,
                None,
                Some("cursor-1"),
                &json!({
                    "data": {
                        "view": {
                            "nodes": [{"id": "1", "title": "a"}],
                            "pageInfo": {"hasNextPage": true, "endCursor": "cursor-1"}
                        }
                    }
                }),
            );

            assert!(
                result.records.is_empty(),
                "the loop guard stops before parsing rows"
            );
            assert!(result.cursor.is_none(), "the loop guard clears the cursor");
            assert_eq!(result.schema, schema);
        }

        /// A per-query schema override outranks the client's configured schema on an empty page,
        /// matching the precedence a non-empty page already used.
        #[test]
        fn schema_override_outranks_the_configured_schema_on_an_empty_page() {
            let override_schema: SchemaRef = Arc::new(Schema::new(vec![Field::new(
                "only_field",
                DataType::Int64,
                true,
            )]));
            let client = client_with_schema(Some(configured_schema()));

            let result = process(
                &client,
                UNPAGINATED_QUERY,
                Some(&override_schema),
                None,
                &json!({"data": {"view": {"nodes": []}}}),
            );

            assert_eq!(result.schema, override_schema);
        }

        /// With nothing configured there is still nothing to infer from, so an empty page keeps
        /// reporting an empty schema.
        #[test]
        fn empty_page_without_a_configured_schema_stays_empty() {
            let client = client_with_schema(None);

            let result = process(
                &client,
                UNPAGINATED_QUERY,
                None,
                None,
                &json!({"data": {"view": {"nodes": []}}}),
            );

            assert_eq!(result.schema.fields().len(), 0);
        }

        /// A page that does have rows is unaffected: the configured schema is what its batches are
        /// parsed with.
        #[test]
        fn non_empty_page_still_parses_with_the_configured_schema() {
            let schema = configured_schema();
            let client = client_with_schema(Some(Arc::clone(&schema)));

            let result = process(
                &client,
                UNPAGINATED_QUERY,
                None,
                None,
                &json!({"data": {"view": {"nodes": [{"id": "1", "title": "a"}]}}}),
            );

            assert_eq!(result.schema, schema);
            assert_eq!(
                result
                    .records
                    .iter()
                    .map(RecordBatch::num_rows)
                    .sum::<usize>(),
                1
            );
        }
    }

    mod nested_connection_pager {
        use std::sync::Arc;

        use crate::graphql::client::{GraphQLQuery, NestedConnectionPager};

        fn pager() -> NestedConnectionPager {
            NestedConnectionPager {
                connection_key: "reviews",
                parent_id_key: "pull_request_id",
                type_condition: "PullRequest",
                node_selection: "id\nstate",
                page_size: 100,
            }
        }

        #[test]
        fn follow_up_query_parses_and_keeps_the_after_cursor() {
            let query_str = pager().next_page_query("PR_1", "Y3Vyc29y");
            let mut query = GraphQLQuery::try_from(Arc::<str>::from(query_str.as_str()))
                .expect("follow-up query must parse");
            query.pagination_parameters = None;
            let rendered = query
                .to_string_with_page_size(None, None, None)
                .expect("follow-up query must render");

            assert!(
                rendered.contains("after:") && rendered.contains("Y3Vyc29y"),
                "clearing pagination_parameters must keep the after cursor, got:\n{rendered}"
            );
            assert!(rendered.contains("... on PullRequest") || rendered.contains("PullRequest"));
        }

        #[test]
        fn follow_up_query_escapes_quotes_in_ids_and_cursors() {
            let query_str = pager().next_page_query(r#"id"x"#, r#"cur"sor"#);
            assert!(
                query_str.contains(r#"id\"x"#) && query_str.contains(r#"cur\"sor"#),
                "quotes in node ids and cursors must be escaped, got:\n{query_str}"
            );
            GraphQLQuery::try_from(Arc::<str>::from(query_str.as_str()))
                .expect("escaped follow-up query must parse");
        }
    }

    struct TestPaginationParseCase {
        name: &'static str,
        query: &'static str,
        expected: (Option<PaginationParameters>, Option<String>),
    }

    #[test]
    #[expect(clippy::needless_raw_string_hashes)]
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
            TestPaginationParseCase {
                name: "Aliased field with pageInfo uses alias in path",
                query: r#"
                    query {
                        repository(owner: "org", name: "repo") {
                            selected_ref: defaultBranchRef {
                                target {
                                    ... on Commit {
                                        history(first: 100) {
                                            pageInfo {
                                                hasNextPage
                                                endCursor
                                            }
                                            nodes {
                                                oid
                                                message
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                "#,
                expected: (
                    Some(PaginationParameters {
                        resource_name: "history".to_owned(),
                        pagination_argument: super::PaginationArgument::First(100),
                        page_info_path: Some(
                            "/repository/selected_ref/target/history/pageInfo".to_owned(),
                        ),
                        other_arguments: vec![],
                    }),
                    Some("/data/repository/selected_ref/target/history/nodes".into()),
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
        let new_query = query
            .to_string(None, Some("new_cursor".to_string()))
            .expect("Should build query");
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
        let new_query = query
            .to_string(None, Some("new_cursor".to_string()))
            .expect("Should build query");
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
        let new_query = query
            .to_string(Some(5), Some("new_cursor".to_string()))
            .expect("Should build query");
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
    fn test_page_size_override_reduces_first_below_query_default() {
        // Page size override should reduce a hard-coded `first: 100` down to 25
        // without requiring the caller to pass a `limit`.
        let query = r"query {
            users(first: 100) {
                name
                pageInfo {
                    hasNextPage
                    endCursor
                }
            }
        }";

        let query = GraphQLQuery::try_from(Arc::from(query)).expect("Should parse query");
        let new_query = query
            .to_string_with_page_size(None, None, Some(25))
            .expect("Should build query");
        assert!(
            new_query.contains("first: 25"),
            "Expected first: 25, got: {new_query}"
        );
        assert!(
            !new_query.contains("first: 100"),
            "Expected first: 100 to be replaced, got: {new_query}"
        );
    }

    #[test]
    fn test_page_size_override_does_not_exceed_limit() {
        // When both a limit and an override are given, the smaller wins.
        let query = r"query {
            users(first: 100) {
                name
                pageInfo {
                    hasNextPage
                    endCursor
                }
            }
        }";

        let query = GraphQLQuery::try_from(Arc::from(query)).expect("Should parse query");
        let new_query = query
            .to_string_with_page_size(Some(10), None, Some(50))
            .expect("Should build query");
        assert!(
            new_query.contains("first: 10"),
            "limit should win over override when smaller; got: {new_query}"
        );
    }

    #[test]
    fn test_reverse_fibonacci_shrink() {
        use super::reverse_fibonacci_shrink;

        // Exact ladder steps should descend to the next smaller step.
        assert_eq!(reverse_fibonacci_shrink(100), 55);
        assert_eq!(reverse_fibonacci_shrink(55), 34);
        assert_eq!(reverse_fibonacci_shrink(34), 21);
        assert_eq!(reverse_fibonacci_shrink(21), 13);
        assert_eq!(reverse_fibonacci_shrink(13), 8);
        assert_eq!(reverse_fibonacci_shrink(8), 5);
        assert_eq!(reverse_fibonacci_shrink(5), 3);
        assert_eq!(reverse_fibonacci_shrink(3), 2);
        assert_eq!(reverse_fibonacci_shrink(2), 1);
        assert_eq!(reverse_fibonacci_shrink(1), 1);
        assert_eq!(reverse_fibonacci_shrink(0), 1);

        // Non-exact values should snap down to the nearest smaller step.
        assert_eq!(reverse_fibonacci_shrink(200), 100);
        assert_eq!(reverse_fibonacci_shrink(60), 55);
        assert_eq!(reverse_fibonacci_shrink(25), 21);
        assert_eq!(reverse_fibonacci_shrink(10), 8);
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

        let rate_limited_response =
            serde_json::from_str(r#"{"message": "API rate limit exceeded for user"}"#)
                .expect("Failed to construct json");
        let rate_limited_result = handle_http_error(StatusCode::FORBIDDEN, &rate_limited_response);
        match rate_limited_result {
            Ok(()) => panic!("Expected rate-limited error"),
            Err(super::Error::RateLimited { message }) => {
                assert!(message.contains("rate limited"));
                assert!(message.contains("HTTP 403"));
            }
            Err(other) => panic!("Expected rate-limited error, got {other}"),
        }
    }

    #[test]
    fn test_json_object_unnesting() {
        let unnest_parameters = super::UnnestParameters {
            behavior: UnnestBehavior::Depth(100),
            duplicate_behavior: DuplicateBehavior::Error,
        };
        let object = serde_json::from_str(r#"{"a": {"b": 1}}"#).expect("Valid json");
        let result =
            super::unnest_json_object(&unnest_parameters, object).expect("To unnest JSON object");
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
            behavior: UnnestBehavior::Depth(100),
            duplicate_behavior: DuplicateBehavior::Error,
        };
        let object =
            serde_json::from_str(r#"{"a": {"b": {"c": {"d": "1"}}}}"#).expect("Valid json");
        let result =
            super::unnest_json_object(&unnest_parameters, object).expect("To unnest JSON object");
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
            behavior: UnnestBehavior::Depth(0),
            duplicate_behavior: DuplicateBehavior::Error,
        };
        let object = serde_json::from_str(r#"{"a": {"b": 1}}"#).expect("Valid json");
        let result =
            super::unnest_json_object(&unnest_parameters, object).expect("To unnest JSON object");
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
            behavior: UnnestBehavior::Depth(1),
            duplicate_behavior: DuplicateBehavior::Error,
        };
        let object =
            serde_json::from_str(r#"{"a": {"b": {"c": {"d": "1"}}}}"#).expect("Valid json");
        let result =
            super::unnest_json_object(&unnest_parameters, object).expect("To unnest JSON object");
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
            behavior: UnnestBehavior::Depth(100),
            duplicate_behavior: DuplicateBehavior::Error,
        };
        let object = serde_json::from_str("[1, 2, 3]").expect("Valid json");
        let result =
            super::unnest_json_object(&unnest_parameters, object).expect("To unnest json array");
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
            behavior: UnnestBehavior::Depth(100),
            duplicate_behavior: DuplicateBehavior::Error,
        };
        let object = serde_json::from_str(r#"{"a": 1, "c": {"b": {"a": 2}}}"#).expect("Valid json");
        let result = super::unnest_json_object(&unnest_parameters, object);

        assert!(result.is_err());

        let err = result.expect_err("Failed to unnest JSON object");
        assert_eq!(
            err.to_string(),
            "Invalid GraphQL object access: Column 'a' already exists in the object."
        );
    }

    #[test]
    fn test_custom_unnesting_behavior_success() {
        // Takes any array values and creates a new object with keys as the array items and values as the original key.
        // Leaves any keys with values that aren't arrays as is
        fn custom_unnester(obj: &Value) -> super::super::Result<Vec<Value>> {
            if let Value::Object(map) = obj {
                let mut result = vec![];
                let mut resulting_map = serde_json::Map::new();
                for (key, value) in map {
                    if let Value::Array(arr) = value {
                        for item in arr {
                            resulting_map.insert(item.clone().to_string(), key.clone().into());
                        }
                    } else {
                        resulting_map.insert(key.clone(), value.clone());
                    }
                }
                result.push(Value::Object(resulting_map));
                Ok(result)
            } else {
                Err(super::Error::InvalidObjectAccess {
                    message: "Expected an object".to_string(),
                })
            }
        }

        let unnest_parameters = super::UnnestParameters {
            behavior: UnnestBehavior::Custom(Box::new(custom_unnester)),
            duplicate_behavior: DuplicateBehavior::Error,
        };

        let object: Value =
            serde_json::from_str(r#"{"a": [1, 2], "b": {"c": [3, 4]}}"#).expect("Valid json");

        let result = super::unnest_json_object(&unnest_parameters, object)
            .expect("To unnest JSON object with custom behavior");

        assert_eq!(result.len(), 1);
        let obj = result.first().expect("To get first unnested object");
        assert!(
            matches!(obj, Value::Object(ob) if ob.contains_key("1") && ob.contains_key("b") && ob.contains_key("2"))
        );
        assert_eq!(obj.get("1"), Some(&Value::String("a".to_string())));
        assert_eq!(obj.get("2"), Some(&Value::String("a".to_string())));
    }

    #[test]
    fn test_auth_custom_header_with_token() {
        let token = Arc::new(token_provider::StaticTokenProvider::new(
            secrecy::SecretString::from("my_secret"),
        )) as Arc<dyn token_provider::TokenProvider>;
        let header = reqwest::header::HeaderName::from_static("x-shopify-access-token");

        let client = super::GraphQLClient::new(
            reqwest::Client::new(),
            Url::parse("https://example.com/graphql").expect("valid url"),
            Some("/data"),
            Some(token),
            None,
            None,
            UnnestBehavior::Depth(0),
            None,
            None,
            None,
            None,
            Some(header),
        )
        .expect("Should create client");

        assert!(
            matches!(&client.auth, Some(super::Auth::CustomHeader(name, _)) if name.as_str() == "x-shopify-access-token"),
            "Expected CustomHeader auth, got {:?}",
            client.auth.as_ref().map(|a| match a {
                super::Auth::Basic(_, _) => "Basic",
                super::Auth::Bearer(_) => "Bearer",
                super::Auth::CustomHeader(_, _) => "CustomHeader",
            })
        );
    }

    #[test]
    fn test_auth_bearer_without_custom_header() {
        let token = Arc::new(token_provider::StaticTokenProvider::new(
            secrecy::SecretString::from("my_secret"),
        )) as Arc<dyn token_provider::TokenProvider>;

        let client = super::GraphQLClient::new(
            reqwest::Client::new(),
            Url::parse("https://example.com/graphql").expect("valid url"),
            None,
            Some(token),
            None,
            None,
            UnnestBehavior::Depth(0),
            None,
            None,
            None,
            None,
            None,
        )
        .expect("Should create client");

        assert!(
            matches!(&client.auth, Some(super::Auth::Bearer(_))),
            "Expected Bearer auth"
        );
    }

    #[test]
    fn test_auth_basic_when_no_token() {
        let client = super::GraphQLClient::new(
            reqwest::Client::new(),
            Url::parse("https://example.com/graphql").expect("valid url"),
            None,
            None,
            Some("user".to_string()),
            Some("pass".to_string()),
            UnnestBehavior::Depth(0),
            None,
            None,
            None,
            None,
            None,
        )
        .expect("Should create client");

        assert!(
            matches!(&client.auth, Some(super::Auth::Basic(u, Some(p))) if u == "user" && p == "pass"),
            "Expected Basic auth with user and pass"
        );
    }

    #[test]
    fn test_auth_basic_fallback_when_auth_header_set_without_token() {
        let header = reqwest::header::HeaderName::from_static("x-custom");

        let client = super::GraphQLClient::new(
            reqwest::Client::new(),
            Url::parse("https://example.com/graphql").expect("valid url"),
            None,
            None,
            Some("user".to_string()),
            None,
            UnnestBehavior::Depth(0),
            None,
            None,
            None,
            None,
            Some(header),
        )
        .expect("Should create client");

        assert!(
            matches!(&client.auth, Some(super::Auth::Basic(u, None)) if u == "user"),
            "Expected Basic auth fallback when auth_header is set but token is missing"
        );
    }

    #[test]
    fn test_auth_header_without_credentials_warns_and_returns_none() {
        let header = reqwest::header::HeaderName::from_static("x-custom");

        let client = super::GraphQLClient::new(
            reqwest::Client::new(),
            Url::parse("https://example.com/graphql").expect("valid url"),
            None,
            None,
            None,
            None,
            UnnestBehavior::Depth(0),
            None,
            None,
            None,
            None,
            Some(header),
        )
        .expect("Should create client");

        assert!(
            client.auth.is_none(),
            "Expected no auth when auth_header is set but no token or user"
        );
    }

    #[test]
    fn test_auth_none_when_nothing_set() {
        let client = super::GraphQLClient::new(
            reqwest::Client::new(),
            Url::parse("https://example.com/graphql").expect("valid url"),
            None,
            None,
            None,
            None,
            UnnestBehavior::Depth(0),
            None,
            None,
            None,
            None,
            None,
        )
        .expect("Should create client");

        assert!(client.auth.is_none(), "Expected no auth");
    }

    #[test]
    fn test_request_with_auth_custom_header() {
        let token = Arc::new(token_provider::StaticTokenProvider::new(
            secrecy::SecretString::from("secret_token_value"),
        )) as Arc<dyn token_provider::TokenProvider>;
        let header = reqwest::header::HeaderName::from_static("x-api-key");
        let auth = super::Auth::CustomHeader(header, token);

        let client = reqwest::Client::new();
        let request_builder = client.post("https://example.com/graphql");
        let request_builder = super::request_with_auth(request_builder, Some(&auth));
        let request = request_builder.build().expect("Should build request");

        assert_eq!(
            request
                .headers()
                .get("x-api-key")
                .expect("Should have x-api-key header")
                .to_str()
                .expect("valid str"),
            "secret_token_value"
        );
    }

    #[test]
    fn test_request_with_auth_bearer() {
        let token = Arc::new(token_provider::StaticTokenProvider::new(
            secrecy::SecretString::from("bearer_token"),
        )) as Arc<dyn token_provider::TokenProvider>;
        let auth = super::Auth::Bearer(token);

        let client = reqwest::Client::new();
        let request_builder = client.post("https://example.com/graphql");
        let request_builder = super::request_with_auth(request_builder, Some(&auth));
        let request = request_builder.build().expect("Should build request");

        assert_eq!(
            request
                .headers()
                .get("authorization")
                .expect("Should have authorization header")
                .to_str()
                .expect("valid str"),
            "Bearer bearer_token"
        );
    }

    #[test]
    fn test_request_with_auth_basic() {
        let auth = super::Auth::Basic("user".to_string(), Some("pass".to_string()));

        let client = reqwest::Client::new();
        let request_builder = client.post("https://example.com/graphql");
        let request_builder = super::request_with_auth(request_builder, Some(&auth));
        let request = request_builder.build().expect("Should build request");

        let auth_header = request
            .headers()
            .get("authorization")
            .expect("Should have authorization header")
            .to_str()
            .expect("valid str");
        assert!(
            auth_header.starts_with("Basic "),
            "Expected Basic auth header, got: {auth_header}"
        );
    }

    #[test]
    fn test_request_with_auth_none() {
        let client = reqwest::Client::new();
        let request_builder = client.post("https://example.com/graphql");
        let request_builder = super::request_with_auth(request_builder, None);
        let request = request_builder.build().expect("Should build request");

        assert!(
            request.headers().get("authorization").is_none(),
            "Expected no authorization header"
        );
    }

    // -----------------------------------------------------------------------
    // format_query_with_context regression tests
    // -----------------------------------------------------------------------

    #[test]
    fn format_query_with_context_zero_line_does_not_panic() {
        let query = "{ users { name } }";
        // line=0 or column=0 should not panic, just return the raw query
        let result = super::format_query_with_context(query, 0, 1);
        assert_eq!(result, query);
    }

    #[test]
    fn format_query_with_context_zero_column_does_not_panic() {
        let query = "{ users { name } }";
        let result = super::format_query_with_context(query, 1, 0);
        assert_eq!(result, query);
    }

    #[test]
    fn format_query_with_context_valid_position() {
        let query = "{\n  users {\n    name\n  }\n}";
        let result = super::format_query_with_context(query, 2, 3);
        assert!(result.contains("  users {"), "should show the error line");
        assert!(result.contains('^'), "should show the caret marker");
    }

    /// A gateway error shrinks the per-page size for the retry, so a page can
    /// come back with fewer rows than the query's declared page size. The
    /// remaining-`LIMIT` counter has to follow the rows that actually arrived:
    /// debiting the declared size instead exhausts the limit early and ends the
    /// scan with a short result that reports success (regression test for
    /// #14308).
    mod limit_accounting {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicBool, Ordering};

        use arrow::array::RecordBatch;
        use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
        use datafusion::catalog::TableProvider;
        use datafusion::physical_plan::collect;
        use datafusion::prelude::SessionContext;
        use serde_json::{Value, json};
        use url::Url;
        use wiremock::matchers::method;
        use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate};

        use crate::graphql::builder::GraphQLClientBuilder;
        use crate::graphql::client::UnnestBehavior;
        use crate::graphql::provider::GraphQLTableProviderBuilder;

        /// The page size the query declares, and the one the shrink ladder is
        /// seeded from: `100 -> 55` on the first gateway error.
        const DECLARED_PAGE_SIZE: usize = 100;
        /// Rows the endpoint can serve — far more than the requested limit, so
        /// a short answer is never end-of-data.
        const SOURCE_ROWS: usize = 500;
        /// The `LIMIT` from the failing CI query.
        const REQUESTED_LIMIT: usize = 125;

        const QUERY: &str = "query { commits(first: 100) {
            pageInfo { hasNextPage endCursor }
            nodes { id }
        } }";

        /// A connection-style endpoint: it serves the `first:` each request
        /// asks for — capped by `page_cap`, the way an API free to short a page
        /// does — hands back a cursor while rows remain, and can fail its very
        /// first request with a 502 so the client shrinks its page size once,
        /// the sequence logged in the run this issue was filed from.
        struct Connection {
            gateway_error_pending: AtomicBool,
            source_rows: usize,
            page_cap: Option<usize>,
        }

        impl Connection {
            fn new(source_rows: usize) -> Self {
                Self {
                    gateway_error_pending: AtomicBool::new(false),
                    source_rows,
                    page_cap: None,
                }
            }

            fn with_one_gateway_error(mut self) -> Self {
                self.gateway_error_pending = AtomicBool::new(true);
                self
            }

            fn with_page_cap(mut self, cap: usize) -> Self {
                self.page_cap = Some(cap);
                self
            }

            /// `first: N` / `after: "cN"` out of the GraphQL document the client
            /// sent, which is what decides the page this request gets.
            fn page_request(query: &str) -> (usize, usize) {
                let leading_number = |text: &str| {
                    text.split(|c: char| !c.is_ascii_digit())
                        .next()
                        .and_then(|digits| digits.parse::<usize>().ok())
                };

                let first = query
                    .split_once("first: ")
                    .and_then(|(_, rest)| leading_number(rest))
                    .expect("the client always names a page size");

                let offset = query
                    .split_once("after: ")
                    .and_then(|(_, rest)| rest.split_once('c'))
                    .and_then(|(_, rest)| leading_number(rest))
                    .unwrap_or(0);

                (first, offset)
            }
        }

        impl Respond for Connection {
            fn respond(&self, request: &Request) -> ResponseTemplate {
                if self.gateway_error_pending.swap(false, Ordering::SeqCst) {
                    return ResponseTemplate::new(502).set_body_string("<html>Bad gateway</html>");
                }

                let body: Value =
                    serde_json::from_slice(&request.body).expect("a JSON request body");
                let query = body
                    .get("query")
                    .and_then(Value::as_str)
                    .expect("the request carries its query");

                let (first, offset) = Self::page_request(query);
                let wanted = self.page_cap.map_or(first, |cap| std::cmp::min(first, cap));
                let served = std::cmp::min(wanted, self.source_rows.saturating_sub(offset));
                let next = offset + served;
                let nodes: Vec<Value> = (offset..next)
                    .map(|i| json!({"id": format!("r{i}")}))
                    .collect();

                ResponseTemplate::new(200).set_body_json(json!({"data": {"commits": {
                    "pageInfo": {
                        "hasNextPage": next < self.source_rows,
                        "endCursor": format!("c{next}"),
                    },
                    "nodes": nodes,
                }}}))
            }
        }

        /// Runs `QUERY` against `endpoint` through the table provider, the way a
        /// `SELECT … LIMIT n` reaches it, and returns the rows it answered with.
        async fn scan_rows(endpoint: Connection, limit: Option<usize>) -> (usize, usize) {
            let server = MockServer::start().await;
            Mock::given(method("POST"))
                .respond_with(endpoint)
                .mount(&server)
                .await;

            let schema: SchemaRef =
                Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, true)]));

            let client = GraphQLClientBuilder::new(
                Url::parse(&format!("{}/graphql", server.uri())).expect("valid URL"),
                UnnestBehavior::Depth(0),
            )
            .with_json_pointer(Some("/data/commits/nodes"))
            .with_schema(Some(Arc::clone(&schema)))
            .build(reqwest::Client::new())
            .expect("client to build");

            let provider = GraphQLTableProviderBuilder::new(client)
                .build_without_validation(QUERY)
                .expect("provider to build without validation");

            let ctx = SessionContext::new();
            let plan = provider
                .scan(&ctx.state(), None, &[], limit)
                .await
                .expect("scan to plan");
            let batches: Vec<RecordBatch> = collect(plan, ctx.task_ctx())
                .await
                .expect("the scan to succeed");

            let requests = server
                .received_requests()
                .await
                .expect("recorded requests")
                .len();

            (batches.iter().map(RecordBatch::num_rows).sum(), requests)
        }

        /// The reported failure: one 502 shrinks the page to 55 rows, and the
        /// scan stops 45 rows short of the `LIMIT` with a successful result.
        #[tokio::test]
        async fn a_limit_spanning_a_shrunk_page_returns_every_requested_row() {
            let (rows, _) = scan_rows(
                Connection::new(SOURCE_ROWS).with_one_gateway_error(),
                Some(REQUESTED_LIMIT),
            )
            .await;

            assert_eq!(
                rows, REQUESTED_LIMIT,
                "a source holding {SOURCE_ROWS} rows must answer LIMIT {REQUESTED_LIMIT} in full; \
                 a page shrunk below the declared size of {DECLARED_PAGE_SIZE} must not retire \
                 rows the scan never fetched"
            );
        }

        /// The shrink makes it systematic, but the defect is the accounting: any
        /// page shorter than the declared size over-debits the remaining limit,
        /// with no error in sight.
        #[tokio::test]
        async fn a_short_page_with_no_gateway_error_still_answers_the_limit() {
            let (rows, _) = scan_rows(
                Connection::new(SOURCE_ROWS).with_page_cap(40),
                Some(REQUESTED_LIMIT),
            )
            .await;

            assert_eq!(
                rows, REQUESTED_LIMIT,
                "an endpoint that serves 40 rows per page still owes every one of the \
                 {REQUESTED_LIMIT} requested rows"
            );
        }

        /// The counter must not run the other way either: a limit inside a
        /// single page still costs exactly one request.
        #[tokio::test]
        async fn a_limit_inside_one_page_stops_after_one_request() {
            let (rows, requests) = scan_rows(Connection::new(SOURCE_ROWS), Some(10)).await;

            assert_eq!(rows, 10, "LIMIT 10 is answered by the first page");
            assert_eq!(
                requests, 1,
                "a limit the first page already satisfies must not fetch another page"
            );
        }

        /// A source with fewer rows than the limit is end-of-data, not a short
        /// page: the scan ends when the cursor does, with what exists.
        #[tokio::test]
        async fn a_source_smaller_than_the_limit_ends_the_scan() {
            let (rows, _) = scan_rows(Connection::new(30), Some(REQUESTED_LIMIT)).await;

            assert_eq!(
                rows, 30,
                "a scan that runs out of rows answers with the rows there are"
            );
        }
    }
}
