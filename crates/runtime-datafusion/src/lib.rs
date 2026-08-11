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

pub mod analyzer_rule;
pub mod composed_catalog;
pub mod config;
pub mod dialect;
pub mod error;
pub mod execution_plan;
pub mod extension;
pub mod join_accumulator;
pub mod managed_runtime;
pub mod optimizer_rule;
pub mod param_utils;
pub mod pg_catalog;
pub mod query_cancel_registry;
pub mod refresh_scan;
pub mod refresh_sql;
pub mod retention_sql;
pub mod schema_provider;
pub mod session_config;

pub use runtime_query_engine::allowlist;
pub use runtime_query_engine::query_engine;
pub mod sort_columns;
pub mod udf;
pub mod url_table;
use snafu::prelude::*;

pub const SPICE_DEFAULT_CATALOG: &str = "spice";

/// Schemas Spice reserves for its own tables under [`SPICE_DEFAULT_CATALOG`].
pub const SPICE_RUNTIME_SCHEMA: &str = "runtime";
pub const SPICE_EVAL_SCHEMA: &str = "eval";
pub const SPICE_METADATA_SCHEMA: &str = "metadata";
pub const SPICE_SCP_SCHEMA: &str = "scp";

/// Whether `catalog`.`schema` is one of Spice's own reserved schemas rather than
/// user data.
#[must_use]
pub fn is_spice_internal_schema(catalog: &str, schema: &str) -> bool {
    catalog == SPICE_DEFAULT_CATALOG
        && (schema == SPICE_RUNTIME_SCHEMA
            || schema == SPICE_METADATA_SCHEMA
            || schema == SPICE_SCP_SCHEMA
            || schema == SPICE_EVAL_SCHEMA)
}

/// Whether `dataset` names a table in one of Spice's reserved schemas. A
/// reference with no catalog is resolved against [`SPICE_DEFAULT_CATALOG`].
#[must_use]
pub fn is_spice_internal_dataset(dataset: &datafusion::sql::TableReference) -> bool {
    match (dataset.catalog(), dataset.schema()) {
        (Some(catalog), Some(schema)) => is_spice_internal_schema(catalog, schema),
        (None, Some(schema)) => is_spice_internal_schema(SPICE_DEFAULT_CATALOG, schema),
        _ => false,
    }
}
pub const SPICE_DEFAULT_SCHEMA: &str = "public";

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Invalid children count. Expected only one input, got {children_count}."))]
    InvalidChildrenCount { children_count: usize },
}
