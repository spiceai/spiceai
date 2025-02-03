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

use std::sync::Arc;

use arrow::error::ArrowError;
use client::GraphQLQuery;
use datafusion::{logical_expr::TableProviderFilterPushDown, prelude::Expr};
use http::{HeaderMap, HeaderValue};
use serde_json::Value;
use snafu::Snafu;

pub mod builder;
pub mod client;
pub mod provider;
pub mod rate_limit;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("{source}"))]
    ReqwestInternal { source: reqwest::Error },

    #[snafu(display("HTTP {status}: {message}"))]
    InvalidReqwestStatus {
        status: reqwest::StatusCode,
        message: String,
    },

    #[snafu(display("JSON pointer could not be inferred, and none provided"))]
    NoJsonPointerFound {},

    #[snafu(display("Invalid GraphQL 'json_pointer': '{pointer}'"))]
    InvalidJsonPointer { pointer: String },

    #[snafu(display("{source}"))]
    ArrowInternal { source: ArrowError },

    #[snafu(display("Invalid object access. {message}"))]
    InvalidObjectAccess { message: String },

    #[snafu(display("{message}"))]
    InvalidCredentialsOrPermissions { message: String },

    #[snafu(display("{message}"))]
    RateLimited { message: String },

    #[snafu(display("Query response transformation failed.\n{source}"))]
    ResultTransformError {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display(
        "GraphQL Query Error:
Details:
- Error: {message}
- Location: Line {line}, Column {column}
- Query:

{query}

Verify the syntax of your GraphQL query."
    ))]
    InvalidGraphQLQuery {
        message: String,
        line: usize,
        column: usize,
        query: String,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Clone)]
pub struct FilterPushdownResult {
    pub filter_pushdown: TableProviderFilterPushDown,
    pub expr: Expr,
    pub context: Option<String>,
}

pub type ValuePreprocessor = Arc<dyn Fn(&mut Value) -> Result<()>>;
pub type ErrorChecker = Arc<dyn Fn(&HeaderMap<HeaderValue>, &Value) -> Result<()> + Send + Sync>;

/// A trait optionally provided to GraphQL ``TableProvider``s to alter the behavior of filter push down
pub trait GraphQLContext: Send + Sync + std::fmt::Debug {
    /// A function executed for each filter push down requested from the ``TableProvider``
    /// A custom implementation can override this function to implement custom filter pushdown logic
    fn filter_pushdown(
        &self,
        expr: &Expr,
    ) -> Result<FilterPushdownResult, datafusion::error::DataFusionError> {
        Ok(FilterPushdownResult {
            filter_pushdown: TableProviderFilterPushDown::Unsupported,
            expr: expr.clone(),
            context: None,
        })
    }

    /// This function receives the ``FilterPushdownResult``s from the ``filter_pushdown`` function, before execution of the GraphQL query
    /// A custom implementation can override this function to inject parameters for custom filter pushdown into the GraphQL query
    fn inject_parameters(
        &self,
        _filters: &[FilterPushdownResult],
        _query: &mut GraphQLQuery,
    ) -> Result<(), datafusion::error::DataFusionError> {
        Ok(())
    }

    /// Return a function that will receive the headers from the GraphQL response
    /// A custom implementation can override this function to process the headers and response, and return custom errors or warnings
    fn error_checker(&self) -> Option<ErrorChecker> {
        None
    }
}
