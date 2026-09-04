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
const REGEXP_REPLACE_FLAGS_POSITION: usize = 3; // The position of the flags argument in regexp_replace function calls
const REGEXP_COUNT_FLAGS_POSITION: usize = 3; // The position of the flags argument in regexp_count function calls

const BTRIM_NAME: &str = "btrim";
const TO_HEX_NAME: &str = "to_hex";

pub(crate) const REGEXP_LIKE_NAME: &str = "regexp_like";
pub(crate) const REGEXP_MATCH_NAME: &str = "regexp_match";
pub(crate) const REGEXP_INSTR_NAME: &str = "regexp_instr";
const REGEXP_REPLACE_NAME: &str = "regexp_replace";
pub(crate) const REGEXP_COUNT_NAME: &str = "regexp_count";

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

/// The `DataFusion` built-ins the `DuckDB` dialect rewrites to native `DuckDB`
/// SQL, paired with their handlers.
///
/// Separate from [`duckdb_scalar_overrides`], and deliberately absent from
/// [`duckdb_native_function_names`]: that list is the federation deny-list's
/// carve-out, and a built-in federates unless it is denied, so carving one out
/// would do nothing. What a built-in needs is the handler — without one the
/// unparser emits the `DataFusion` call verbatim, and `DuckDB` either rejects
/// the name (`btrim`) or accepts it and answers differently (`to_hex`, whose
/// digits come back upper-case). The second is the worse of the two: it is a
/// silently different result rather than a query error.
fn duckdb_builtin_scalar_overrides() -> Vec<(&'static str, ScalarFnToSqlHandler)> {
    vec![
        (
            // DuckDB dialect: trim(string[, characters])
            // DataFusion dialect: btrim(str[, trim_str]) — `trim` is only its alias
            BTRIM_NAME,
            Box::new(duckdb::btrim_to_trim) as ScalarFnToSqlHandler,
        ),
        (
            // DuckDB dialect: to_hex(int) — upper-case digits
            // DataFusion dialect: to_hex(int) — lower-case digits
            TO_HEX_NAME,
            Box::new(duckdb::to_hex_to_lowercase_hex) as ScalarFnToSqlHandler,
        ),
    ]
}

/// Names of the Spice functions [`new_duckdb_dialect`] rewrites to native
/// `DuckDB` SQL. The `DataFusion` built-ins it also rewrites are not here — see
/// [`duckdb_builtin_scalar_overrides`] for why.
///
/// Any Spice-specific function in this list has a real `DuckDB` equivalent and
/// can therefore be federated (pushed down) to `DuckDB` rather than denied. The
/// federation deny-list derives its `DuckDB` carve-out from this list (see
/// [`crate::function_support::deny_spice_functions_for_duckdb`]), so the dialect
/// and the deny-list stay in sync automatically.
///
/// A name in [`crate::function_support::DUCKDB_DENIED_BUILTINS`] is filtered out
/// rather than trusted not to appear: a handler whose rendering is not
/// value-preserving stays in the dialect so a later fix has it to work from
/// (`regexp_count`, #13870), and "has a handler" must not be read as "may be
/// pushed down" while that is true.
#[must_use]
pub fn duckdb_native_function_names() -> Vec<&'static str> {
    duckdb_scalar_overrides()
        .into_iter()
        .map(|(name, _)| name)
        .filter(|name| !crate::function_support::DUCKDB_DENIED_BUILTINS.contains(name))
        .collect()
}

/// Creates a new instance of the `DuckDB` dialect with support for Spice
/// internal UDFs ([`duckdb_scalar_overrides`]) and for the `DataFusion`
/// built-ins `DuckDB` spells differently ([`duckdb_builtin_scalar_overrides`]).
#[must_use]
pub fn new_duckdb_dialect() -> Arc<dyn Dialect> {
    let overrides = duckdb_scalar_overrides()
        .into_iter()
        .chain(duckdb_builtin_scalar_overrides())
        .collect();
    let dialect = DuckDBDialect::new().with_custom_scalar_overrides(overrides);

    Arc::new(dialect) as Arc<dyn Dialect>
}

/// Names of the functions [`new_bigquery_dialect`] rewrites to native
/// `BigQuery` SQL. The federation deny-list derives its `BigQuery` carve-out
/// from this list; see [`crate::function_support::deny_spice_functions_for_bigquery_table_providers`].
///
/// This, the dialect's handlers and [`bigquery_can_translate`] are all derived
/// from `bigquery::SCALAR_OVERRIDES`, so the three cannot drift: a function
/// cannot be allowed to federate that the dialect has no handler for, and a
/// handler cannot be added without saying which call shapes it can render.
///
/// The dialect also rewrites some `DataFusion` built-ins
/// (`bigquery::BUILTIN_SCALAR_OVERRIDES`, e.g. `regexp_like` →
/// `REGEXP_CONTAINS`). Those are deliberately **not** in this list: a built-in
/// federates unless denied, so a carve-out would do nothing — what it needs is
/// the handler and the per-call check, which [`new_bigquery_dialect`] and
/// [`bigquery_can_translate`] carry.
///
/// The rest stay denied, each for something `BigQuery` cannot be talked out of.
/// `json_get_json` and `json_as_text` return the matched node's own bytes,
/// spacing and number spelling intact, where `JSON_QUERY` re-renders it — a
/// document holding `{"b": -1}` comes back as `{"b":-1}`. `json_contains`
/// counts a JSON `null` as present, and `BigQuery` returns SQL NULL for such a
/// node exactly as it does for a missing key, so the two cannot be told apart.
/// `json_get`, `json_get_array` and the union helpers carry the crate's JSON
/// union, which has no SQL type to unparse into.
#[must_use]
pub fn bigquery_native_function_names() -> Vec<&'static str> {
    bigquery::SCALAR_OVERRIDES
        .iter()
        .map(|entry| entry.name)
        .collect()
}

/// Whether the `BigQuery` dialect can translate this particular call.
///
/// A name with a handler is not enough on its own: the JSON functions take a
/// variadic path whose elements must be literal, and `regexp_like` needs a
/// constant pattern both regex engines read identically. The deny-list installs
/// this so an untranslatable call is left to evaluate locally instead of being
/// unparsed.
#[must_use]
pub fn bigquery_can_translate(call: &ScalarFunction) -> bool {
    bigquery::can_translate(call)
}

/// Creates a `BigQuery` dialect that also rewrites the Spice JSON functions
/// [`bigquery_native_function_names`] lists, and the `DataFusion` built-ins
/// `bigquery::BUILTIN_SCALAR_OVERRIDES` lists.
#[must_use]
pub fn new_bigquery_dialect() -> Arc<dyn Dialect> {
    let handlers: Vec<(&str, ScalarFnToSqlHandler)> = bigquery::SCALAR_OVERRIDES
        .iter()
        .chain(bigquery::BUILTIN_SCALAR_OVERRIDES)
        .map(|entry| (entry.name, Box::new(entry.handler) as ScalarFnToSqlHandler))
        .collect();

    Arc::new(SpiceBigQueryDialect::new().with_custom_scalar_overrides(handlers)) as Arc<dyn Dialect>
}

#[cfg(test)]
mod tests {
    use super::{
        REGEXP_COUNT_NAME, bigquery, bigquery_native_function_names,
        duckdb_builtin_scalar_overrides, duckdb_native_function_names, new_duckdb_dialect,
    };
    use datafusion::prelude::col;
    use datafusion::sql::unparser::Unparser;

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

    #[test]
    fn no_denied_builtin_is_advertised_as_a_native_duckdb_function() {
        // `duckdb_native_function_names` is what the deny-list reads as its
        // carve-out, so a denied name appearing there would un-deny it and push
        // down a call DuckDB answers differently (#13809, #13870). Driven from
        // the deny-list itself rather than a hardcoded name, so denying another
        // built-in cannot skip this check.
        for name in crate::function_support::DUCKDB_DENIED_BUILTINS {
            assert!(
                !duckdb_native_function_names().contains(name),
                "`{name}` is denied for DuckDB and must not be advertised as native"
            );
        }
    }

    #[test]
    fn the_constructed_duckdb_dialect_renders_no_denied_builtin_except_regexp_count() {
        // Asserted against the dialect `new_duckdb_dialect` actually builds, not
        // against `duckdb_scalar_overrides` alone: the constructor chains
        // `duckdb_builtin_scalar_overrides` too, so checking one list would leave
        // this test green while a handler was restored in the other.
        //
        // `regexp_match` had one, rendering `ARRAY[regexp_extract(s, p, 0)] AS
        // item` — the whole match rather than the capture groups, the empty
        // string rather than NULL, and an `AS item` DuckDB's parser rejects
        // wherever the expression is aliased (#13809).
        let dialect = new_duckdb_dialect();
        let unparser = Unparser::new(dialect.as_ref());
        let args = [col("c0"), col("c1")];

        for name in crate::function_support::DUCKDB_DENIED_BUILTINS {
            // `regexp_count` keeps its handler on purpose (#13870), so it is
            // exempt from this one; the carve-out check above still covers it.
            if *name == REGEXP_COUNT_NAME {
                continue;
            }
            assert!(
                matches!(
                    dialect.scalar_function_to_sql_overrides(&unparser, name, &args),
                    Ok(None)
                ),
                "the constructed DuckDB dialect must render no handler for the denied \
                 `{name}`; one here would send DuckDB a call it answers differently"
            );
        }
    }

    #[test]
    fn no_builtin_override_is_in_the_deny_list_carve_out() {
        // The carve-out un-denies Spice functions. A DataFusion built-in is
        // never denied by the Spice deny-list, so a built-in appearing in the
        // carve-out means someone put it in the wrong table — and its per-call
        // check may then be skipped by a consumer that only reads one list.
        let carved_out = bigquery_native_function_names();
        for entry in bigquery::BUILTIN_SCALAR_OVERRIDES {
            assert!(
                !carved_out.contains(&entry.name),
                "`{name}` is a DataFusion built-in and must not be in the Spice carve-out",
                name = entry.name
            );
        }

        let carved_out = duckdb_native_function_names();
        for (name, _) in duckdb_builtin_scalar_overrides() {
            assert!(
                !carved_out.contains(&name),
                "`{name}` is a DataFusion built-in and must not be in the Spice carve-out"
            );
        }
    }
}
