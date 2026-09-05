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

use std::sync::{Arc, LazyLock};

use datafusion::logical_expr::expr::ScalarFunction;
use datafusion::sql::unparser::Unparser;
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

pub(crate) const BTRIM_NAME: &str = "btrim";
const TO_HEX_NAME: &str = "to_hex";
const SHA256_NAME: &str = "sha256";

pub(crate) const REGEXP_LIKE_NAME: &str = "regexp_like";
pub(crate) const REGEXP_MATCH_NAME: &str = "regexp_match";
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

/// The `DataFusion` built-ins the `DuckDB` dialect rewrites to native `DuckDB`
/// SQL, paired with their handlers.
///
/// Separate from [`duckdb_scalar_overrides`], and deliberately absent from
/// [`duckdb_native_function_names`]: that list is the federation deny-list's
/// carve-out, and a built-in federates unless it is denied, so carving one out
/// would do nothing. What a built-in needs is the handler — without one the
/// unparser emits the `DataFusion` call verbatim, and `DuckDB` either rejects
/// the name (`btrim`) or accepts it and answers differently (`to_hex`, whose
/// digits come back upper-case; `sha256`, which returns the digest's hex text
/// where the kernel returns its bytes). The second is the worse of the two: it
/// is a silently different result rather than a query error.
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
        (
            // DuckDB dialect: sha256(x) — the digest's hex text, as VARCHAR
            // DataFusion dialect: sha256(x) — the 32-byte digest, as Binary
            SHA256_NAME,
            Box::new(duckdb::sha256_to_digest_bytes) as ScalarFnToSqlHandler,
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
#[must_use]
pub fn duckdb_native_function_names() -> Vec<&'static str> {
    duckdb_scalar_overrides()
        .into_iter()
        .map(|(name, _)| name)
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

/// One `DuckDB` dialect, built once, for [`duckdb_can_translate`] to ask
/// whether a call renders. It is consulted per scalar call during federation
/// planning, and building the override table each time would be the expensive
/// part of an otherwise trivial check.
static DUCKDB_DIALECT: LazyLock<Arc<dyn Dialect>> = LazyLock::new(new_duckdb_dialect);

/// Whether the `DuckDB` dialect can render this particular call.
///
/// A handler renders a *call*, not a name, and several of the `DuckDB`
/// handlers refuse a call they cannot render faithfully — the regex family
/// refuses the `U` and `R` flags, which `DuckDB` has no equivalent of, and
/// `regexp_count` refuses a start position that is not an integer literal,
/// because the rewrite has to turn it into a `substring` offset. Refusing is
/// right; what was wrong is where the refusal landed. Federation asks for the
/// SQL after it has already decided to federate the plan, so the refusal came
/// back as a planning error and failed a query `DataFusion` can answer on its
/// own (issue #13900).
///
/// The deny-list installs this so the decision is made while it is still a
/// decision: a call the dialect cannot render is not federated, and evaluates
/// locally above the federated scan instead. That costs the pushdown for those
/// plans and returns the right rows, which is the trade the deny-list exists
/// to make.
///
/// The answer comes from running the dialect's own handler rather than from a
/// second table describing it, so the check cannot drift from what the dialect
/// does: whatever [`new_duckdb_dialect`] installs is what is asked. A name the
/// dialect has no handler for renders as `Ok(None)` and is deferred to, which
/// is why an ordinary function is unaffected.
#[must_use]
pub fn duckdb_can_translate(call: &ScalarFunction) -> bool {
    let unparser = Unparser::new(DUCKDB_DIALECT.as_ref());
    DUCKDB_DIALECT
        .scalar_function_to_sql_overrides(&unparser, call.func.name(), &call.args)
        .is_ok()
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
        bigquery, bigquery_native_function_names, duckdb_builtin_scalar_overrides,
        duckdb_can_translate, duckdb_native_function_names, new_duckdb_dialect,
    };
    use datafusion::functions::expr_fn::upper;
    use datafusion::functions::regex::expr_fn::{regexp_count, regexp_like, regexp_replace};
    use datafusion::logical_expr::expr::ScalarFunction;
    use datafusion::prelude::{Expr, col, lit};
    use datafusion::sql::unparser::Unparser;

    /// The [`ScalarFunction`] inside a call built by `DataFusion`'s own
    /// `expr_fn` helpers, so these guards run against the real UDFs rather
    /// than a stub that only shares their name.
    fn call_of(expr: Expr) -> ScalarFunction {
        match expr {
            Expr::ScalarFunction(call) => call,
            other => panic!("expected a scalar function call, got {other:?}"),
        }
    }

    /// Regression test for #13900: the `U` flag has no `DuckDB` equivalent, so
    /// the dialect's regex handler refuses to render the call. Before this
    /// check the refusal surfaced as a planning error for the whole query;
    /// declining to federate leaves the call for `DataFusion` to evaluate.
    #[test]
    fn duckdb_declines_a_regex_flag_duckdb_has_no_equivalent_of() {
        for flag in ["U", "R", "gU", "iR"] {
            assert!(
                !duckdb_can_translate(&call_of(regexp_replace(
                    col("s"),
                    lit("a"),
                    lit("X"),
                    Some(lit(flag)),
                ))),
                "regexp_replace with flags `{flag}` has no DuckDB rendering"
            );
            assert!(
                !duckdb_can_translate(&call_of(regexp_like(col("s"), lit("a"), Some(lit(flag))))),
                "regexp_like with flags `{flag}` has no DuckDB rendering"
            );
        }

        // The flags DuckDB does have keep federating.
        for flag in ["g", "i", "gi"] {
            assert!(
                duckdb_can_translate(&call_of(regexp_replace(
                    col("s"),
                    lit("a"),
                    lit("X"),
                    Some(lit(flag)),
                ))),
                "regexp_replace with flags `{flag}` renders as DuckDB SQL"
            );
        }

        // No flags argument at all is the common shape and must federate.
        assert!(duckdb_can_translate(&call_of(regexp_replace(
            col("s"),
            lit("a"),
            lit("X"),
            None,
        ))));
    }

    /// Regression test for #13900: `regexp_count`'s start position becomes a
    /// `substring` offset in the `DuckDB` rewrite, which needs the value at
    /// unparse time. A column cannot supply one, and neither can a start
    /// below 1.
    #[test]
    fn duckdb_declines_a_regexp_count_start_it_cannot_turn_into_an_offset() {
        assert!(
            !duckdb_can_translate(&call_of(regexp_count(
                col("s"),
                lit("a"),
                Some(col("start")),
                None,
            ))),
            "a column start position has no DuckDB rendering"
        );
        assert!(
            !duckdb_can_translate(&call_of(regexp_count(
                col("s"),
                lit("a"),
                Some(lit(0)),
                None,
            ))),
            "a start position below 1 has no DuckDB rendering"
        );
        assert!(
            duckdb_can_translate(&call_of(regexp_count(
                col("s"),
                lit("a"),
                Some(lit(1)),
                None,
            ))),
            "an integer start position renders as a DuckDB substring offset"
        );
    }

    /// A function the dialect installs no handler for is deferred to, so an
    /// ordinary call keeps federating.
    #[test]
    fn duckdb_defers_on_a_function_the_dialect_does_not_rewrite() {
        assert!(duckdb_can_translate(&call_of(upper(col("s")))));
    }

    /// The check must answer exactly what the unparser does, or a call it
    /// admits still fails the query and a call it refuses loses its pushdown
    /// for nothing. Asking through `expr_to_sql` reaches the handler by the
    /// unparser's own dispatch rather than by the accessor the check uses.
    #[test]
    fn duckdb_can_translate_agrees_with_what_the_unparser_renders() {
        let dialect = new_duckdb_dialect();
        let unparser = Unparser::new(dialect.as_ref());

        for expr in [
            regexp_replace(col("s"), lit("a"), lit("X"), Some(lit("U"))),
            regexp_replace(col("s"), lit("a"), lit("X"), Some(lit("g"))),
            regexp_replace(col("s"), lit("a"), lit("X"), None),
            regexp_like(col("s"), lit("a"), Some(lit("R"))),
            regexp_like(col("s"), lit("a"), None),
            regexp_count(col("s"), lit("a"), Some(col("start")), None),
            regexp_count(col("s"), lit("a"), Some(lit(0)), None),
            regexp_count(col("s"), lit("a"), Some(lit(2)), None),
            upper(col("s")),
        ] {
            let renders = unparser.expr_to_sql(&expr).is_ok();
            assert_eq!(
                duckdb_can_translate(&call_of(expr.clone())),
                renders,
                "the per-call check and the unparser disagree about {expr:?}"
            );
        }
    }

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
