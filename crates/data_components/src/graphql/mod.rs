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

use arrow::error::ArrowError;
use datafusion::{logical_expr::TableProviderFilterPushDown, prelude::Expr};
use graphql_parser::query::Document;
use snafu::Snafu;

pub mod client;
pub mod provider;

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

    #[snafu(display("Query response transformation failed: {source}"))]
    ResultTransformError {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

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

pub type FilterPushdownFn =
    fn(&Expr) -> Result<FilterPushdownResult, datafusion::error::DataFusionError>;

pub type ParameterInjectionFn =
    fn(
        &[FilterPushdownResult],
        &Document<'static, String>,
    ) -> Result<Document<'static, String>, datafusion::error::DataFusionError>;

#[derive(Debug, Clone)]
pub struct FilterPushdownResult {
    pub filter_pushdown: TableProviderFilterPushDown,
    pub expr: Expr,
    pub context: Option<String>,
}

pub struct GraphQLOptimizer {
    filter_pushdown_fn: FilterPushdownFn,
    parameter_injection_fn: ParameterInjectionFn,
}

impl GraphQLOptimizer {
    pub fn new(
        filter_pushdown_fn: FilterPushdownFn,
        parameter_injection_fn: ParameterInjectionFn,
    ) -> Self {
        Self {
            filter_pushdown_fn,
            parameter_injection_fn,
        }
    }
}
