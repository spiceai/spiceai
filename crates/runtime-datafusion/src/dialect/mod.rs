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

use datafusion::sql::unparser::dialect::{Dialect, DuckDBDialect, ScalarFnToSqlHandler};

use runtime_datafusion_udfs::inner_product::INNER_PRODUCT_UDF_NAME;

mod duckdb;

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
///
/// **An entry here asserts value equivalence, not just that a similarly-named
/// `DuckDB` function exists.** Adding one makes the same call answer from two
/// implementations depending only on where the table lives, so the two have to
/// agree on every input — including the ones no formula mentions: a zero-
/// magnitude vector, and an element that is `NaN` or an infinity. A function
/// whose `DuckDB` counterpart disagrees anywhere belongs out of this list, where
/// `DataFusion` evaluates it locally. `duckdb_vector_udf_pushdown_matches_local`
/// in `accelerators/accelerator-duckdb` measures that agreement against a real
/// `DuckDB` for every vector UDF listed here.
fn duckdb_scalar_overrides() -> Vec<(&'static str, ScalarFnToSqlHandler)> {
    vec![
        // `cosine_distance` is deliberately absent. `DuckDB`'s
        // `array_cosine_distance` returns `1 - cosine_similarity` over `[0, 2]`,
        // while this UDF returns `(1 - cosine_similarity) / 2` over `[0, 1]`, so
        // the rewrite reported twice the distance for every non-identical pair.
        // It also answers `2.0` where the UDF answers `0.5` for a zero-magnitude
        // vector and `NULL` for a non-finite element — and `2.0` is
        // simultaneously the legitimate value for opposite vectors, so no screen
        // over the result can tell the three apart. See issue #13088.
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
/// Any Spice-specific function in this list has a `DuckDB` equivalent that
/// returns the same value for the same input, and can therefore be federated
/// (pushed down) to `DuckDB` rather than denied. The federation deny-list
/// derives its `DuckDB` carve-out from this list (see
/// [`crate::function_support::deny_spice_functions_for_duckdb`]), so the dialect
/// and the deny-list stay in sync automatically.
///
/// "Equivalent" is the load-bearing word: a name `DuckDB` also happens to have
/// is not enough, because a mismatch here is not an error but a different
/// answer. See [`duckdb_scalar_overrides`] for what the entry asserts.
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
