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

use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::sql::unparser::dialect::{Dialect, DuckDBDialect, ScalarFnToSqlHandler};

use runtime_datafusion_udfs::cosine_distance::COSINE_DISTANCE_UDF_NAME;
use runtime_datafusion_udfs::inner_product::INNER_PRODUCT_UDF_NAME;

mod bigquery;
mod duckdb;

pub use bigquery::SpiceBigQueryDialect;

const REGEXP_LIKE_FLAGS_POSITION: usize = 2; // The position of the flags argument in regexp_like function calls
const REGEXP_MATCH_FLAGS_POSITION: usize = 2; // The position of the flags argument in regexp_match function calls
const REGEXP_REPLACE_FLAGS_POSITION: usize = 3; // The position of the flags argument in regexp_replace function calls
const REGEXP_COUNT_FLAGS_POSITION: usize = 3; // The position of the flags argument in regexp_count function calls

const REGEXP_LIKE_NAME: &str = "regexp_like";
const REGEXP_MATCH_NAME: &str = "regexp_match";
const REGEXP_REPLACE_NAME: &str = "regexp_replace";
const REGEXP_COUNT_NAME: &str = "regexp_count";

/// The scalar functions the `DuckDB` unparser dialect rewrites to native
/// `DuckDB` SQL, paired with their handlers.
///
/// This is the single source of truth for both [`new_duckdb_dialect`] (which
/// installs the handlers) and [`duckdb_native_function_names`] (which the
/// federation deny-list consults to decide what can be pushed down). Keeping
/// them derived from one list guarantees the dialect's translation capability
/// and the deny-list carve-out can never drift apart.
fn duckdb_scalar_overrides() -> Vec<(&'static str, ScalarFnToSqlHandler)> {
    vec![
        (
            COSINE_DISTANCE_UDF_NAME,
            Box::new(duckdb::cosine_distance_to_sql) as ScalarFnToSqlHandler,
        ),
        (
            INNER_PRODUCT_UDF_NAME,
            Box::new(duckdb::inner_product_to_sql) as ScalarFnToSqlHandler,
        ),
        (
            "array_distance",
            Box::new(duckdb::array_distance_to_sql) as ScalarFnToSqlHandler,
        ),
        (
            "rand",
            Box::new(duckdb::rand_to_random) as ScalarFnToSqlHandler,
        ),
        (
            // DuckDB dialect: regexp_matches(string, pattern[, options])
            // DataFusion dialect: regexp_like(str, regexp[, flags])
            REGEXP_LIKE_NAME,
            Box::new(
                duckdb::DuckDBRegexpFunction::Like
                    .to_datafusion_function(REGEXP_LIKE_FLAGS_POSITION),
            ) as ScalarFnToSqlHandler,
        ),
        (
            // DuckDB dialect: regexp_extract(string, pattern[, group = 0, options])
            // DataFusion dialect: regexp_match(str, regexp[, flags])
            REGEXP_MATCH_NAME,
            Box::new(
                duckdb::DuckDBRegexpFunction::Match
                    .to_datafusion_function(REGEXP_MATCH_FLAGS_POSITION),
            ) as ScalarFnToSqlHandler,
        ),
        (
            // DuckDB dialect: regexp_replace(string, pattern, replacement[, options])
            // DataFusion dialect: regexp_replace(str, regexp, replacement[, flags])
            REGEXP_REPLACE_NAME,
            Box::new(
                duckdb::DuckDBRegexpFunction::Replace
                    .to_datafusion_function(REGEXP_REPLACE_FLAGS_POSITION),
            ) as ScalarFnToSqlHandler,
        ),
        (
            // DuckDB dialect: len(regex_extract_all(string, pattern[, group = 0, options]))
            // DataFusion dialect: regexp_count(str, regexp[, start, flags])
            REGEXP_COUNT_NAME,
            Box::new(
                duckdb::DuckDBRegexpFunction::Count
                    .to_datafusion_function(REGEXP_COUNT_FLAGS_POSITION),
            ) as ScalarFnToSqlHandler,
        ),
    ]
}

/// Names of the functions [`new_duckdb_dialect`] rewrites to native `DuckDB`
/// SQL.
///
/// Any Spice-specific function in this list has a real `DuckDB` equivalent and
/// can therefore be federated (pushed down) to `DuckDB` rather than denied. The
/// federation deny-list derives its `DuckDB` carve-out from this list (see
/// [`crate::function_support::deny_spice_functions_for_duckdb`]), so the dialect
/// and the deny-list stay in sync automatically.
#[must_use]
pub fn duckdb_native_function_names() -> Vec<&'static str> {
    duckdb_scalar_overrides()
        .into_iter()
        .map(|(name, _)| name)
        .collect()
}

/// Creates a new instance of the `DuckDB` dialect with support for Spice internal UDFs
#[must_use]
pub fn new_duckdb_dialect() -> Arc<dyn Dialect> {
    let dialect = DuckDBDialect::new().with_custom_scalar_overrides(duckdb_scalar_overrides());

    Arc::new(dialect) as Arc<dyn Dialect>
}

/// The scalar functions the `BigQuery` unparser dialect rewrites to native
/// `BigQuery` SQL, paired with their handlers.
///
/// The same single-source-of-truth rule as [`duckdb_scalar_overrides`], with
/// one more consumer: [`bigquery_can_translate`] answers from the same path
/// builder these handlers render with, so what the dialect can render, what the
/// deny-list lets through, and what the per-call check refuses all come from one
/// place.
///
/// The other JSON functions stay denied. `json_get_str` needs a string-type
/// guard around `JSON_VALUE`, which renders a JSON number as its digits where
/// `json_get_str` returns NULL, and `json_get_bool` would diverge because
/// `BigQuery`'s cast to `BOOL` is case-insensitive and Rust's `bool::from_str`
/// is not. `json_get` and its relatives return a union that has no SQL surface.
fn bigquery_scalar_overrides() -> Vec<(&'static str, ScalarFnToSqlHandler)> {
    vec![
        (
            bigquery::JSON_GET_INT_NAME,
            Box::new(bigquery::json_get_int_to_sql) as ScalarFnToSqlHandler,
        ),
        (
            bigquery::JSON_GET_FLOAT_NAME,
            Box::new(bigquery::json_get_float_to_sql) as ScalarFnToSqlHandler,
        ),
    ]
}

/// Names of the functions [`new_bigquery_dialect`] rewrites to native
/// `BigQuery` SQL. The federation deny-list derives its `BigQuery` carve-out
/// from this list; see [`crate::function_support::deny_spice_functions_for_bigquery_table_providers`].
#[must_use]
pub fn bigquery_native_function_names() -> Vec<&'static str> {
    bigquery_scalar_overrides()
        .into_iter()
        .map(|(name, _)| name)
        .collect()
}

/// Whether the `BigQuery` dialect can translate this particular call.
///
/// A name in [`bigquery_native_function_names`] is not enough on its own: the
/// JSON functions take a variadic path, and a path element that is not a
/// literal has no `BigQuery` equivalent, because its JSON path argument must be
/// a constant. The deny-list installs this so such a call is left to evaluate
/// locally instead of being unparsed.
#[must_use]
pub fn bigquery_can_translate(call: &ScalarFunction) -> bool {
    bigquery::can_translate(call)
}

/// Creates a `BigQuery` dialect that also rewrites the Spice JSON functions
/// [`bigquery_native_function_names`] lists.
#[must_use]
pub fn new_bigquery_dialect() -> Arc<dyn Dialect> {
    let dialect =
        SpiceBigQueryDialect::new().with_custom_scalar_overrides(bigquery_scalar_overrides());

    Arc::new(dialect) as Arc<dyn Dialect>
}

#[cfg(test)]
mod tests {
    use super::bigquery_native_function_names;

    #[test]
    fn every_carved_out_bigquery_name_is_a_function_the_deny_list_knows() {
        let json = runtime_udfs_api::json_function_names();
        for name in bigquery_native_function_names() {
            assert!(
                json.iter().any(|known| known == name),
                "`{name}` is not a name `datafusion-functions-json` registers, so carving it out \
                 of the deny-list does nothing"
            );
        }
    }
}
