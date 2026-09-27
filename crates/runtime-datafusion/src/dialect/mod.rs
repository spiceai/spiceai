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

use datafusion::common::DFSchema;
use datafusion::logical_expr::expr::{AggregateFunction, ScalarFunction, WindowFunction};
use datafusion::sql::unparser::Unparser;
use datafusion::sql::unparser::dialect::{Dialect, DuckDBDialect, ScalarFnToSqlHandler};

use runtime_datafusion_udfs::cosine_distance::COSINE_DISTANCE_UDF_NAME;
use runtime_datafusion_udfs::inner_product::INNER_PRODUCT_UDF_NAME;

mod bigquery;
mod duckdb;
mod re2;

pub use bigquery::SpiceBigQueryDialect;

pub(crate) const BTRIM_NAME: &str = "btrim";
const TO_HEX_NAME: &str = "to_hex";
const CONCAT_NAME: &str = "concat";
const SHA256_NAME: &str = "sha256";

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
            Box::new(duckdb::DuckDBRegexpFunction::Like.to_datafusion_function())
                as ScalarFnToSqlHandler,
        ),
        (
            // DuckDB dialect: regexp_replace(string, pattern, replacement[, options])
            // DataFusion dialect: regexp_replace(str, regexp, replacement[, flags])
            REGEXP_REPLACE_NAME,
            Box::new(duckdb::DuckDBRegexpFunction::Replace.to_datafusion_function())
                as ScalarFnToSqlHandler,
        ),
        (
            // DuckDB dialect: coalesce(len(regexp_extract_all(string, pattern)), 0)
            // DataFusion dialect: regexp_count(str, regexp[, start, flags])
            REGEXP_COUNT_NAME,
            Box::new(duckdb::DuckDBRegexpFunction::Count.to_datafusion_function())
                as ScalarFnToSqlHandler,
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
/// digits come back upper-case; `concat`, which skips a NULL argument where
/// the kernel returns NULL for the whole call; `sha256`, which returns the
/// digest's hex text where the kernel returns its bytes). The second is the
/// worse of the two: it is a silently different result rather than a query
/// error.
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
            // DuckDB dialect: a || b || … — NULL propagates
            // Spice: `concat` resolves to datafusion-spark's SparkConcat,
            // which returns NULL if any argument is NULL — unlike DuckDB's
            // function of the same name, which skips it
            CONCAT_NAME,
            Box::new(duckdb::concat_to_string_concat) as ScalarFnToSqlHandler,
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
///
/// A name in [`crate::function_support::DUCKDB_DENIED_BUILTINS`] is filtered out
/// rather than trusted not to appear. The dialect carries no handler for a
/// denied name (`the_constructed_duckdb_dialect_renders_no_denied_builtin`
/// asserts it), and a handler whose rendering is unfaithful for some call
/// shapes refuses those shapes per call instead (#13870 is the precedent); the
/// filter is defence in depth, so "has a handler" can never be read as "may be
/// pushed down".
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
///
/// Running the handler answers whether the call *renders*, which is not the
/// whole question: `concat_to_string_concat` renders every call it is given,
/// and its `||` rendering is faithful only while no operand is binary. A
/// rendering whose correctness turns on an operand's declared type cannot be
/// checked by running it, because the handler sees `&[Expr]` with no schema —
/// so `scope` is consulted for those, and is `None` where the type cannot be
/// proven (see
/// [`datafusion_table_providers::util::supported_functions::ScalarCallSupport`]).
#[must_use]
pub fn duckdb_can_translate(call: &ScalarFunction, scope: Option<&DFSchema>) -> bool {
    if call.func.name() == CONCAT_NAME
        && !duckdb::concat_arguments_are_renderable(&call.args, scope)
    {
        return false;
    }
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
/// `json_get_json` returns the matched node's own bytes, including spacing,
/// where `JSON_QUERY` serializes containers with different whitespace.
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
pub fn bigquery_can_translate(
    call: &ScalarFunction,
    scope: Option<&datafusion::common::DFSchema>,
) -> bool {
    bigquery::can_translate(call, scope)
}

/// Whether the `BigQuery` dialect can translate this particular aggregate call.
///
/// An aggregate call carries its `FILTER`, `ORDER BY` and `DISTINCT`, and the
/// dialect can rewrite some of those shapes and not others. The deny-list
/// installs this so a shape it cannot rewrite faithfully is left to evaluate
/// locally instead of being unparsed into a different answer.
#[must_use]
pub fn bigquery_can_translate_aggregate(call: &AggregateFunction) -> bool {
    bigquery::can_translate_aggregate(call)
}

/// Whether the `BigQuery` dialect can translate this particular window call.
///
/// A `FILTER` on a window call reaches no rewriting at all, so it renders
/// verbatim into SQL `BigQuery` refuses. The deny-list installs this so the window
/// evaluates locally instead.
#[must_use]
pub fn bigquery_can_translate_window(call: &WindowFunction) -> bool {
    bigquery::can_translate_window(call)
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
        bigquery, bigquery_native_function_names, duckdb, duckdb_builtin_scalar_overrides,
        duckdb_can_translate, duckdb_native_function_names, new_duckdb_dialect,
    };
    use arrow_schema::{DataType, Field, Schema};
    use datafusion::common::DFSchema;
    use datafusion::functions::expr_fn::{concat, upper};
    use datafusion::functions::regex::expr_fn::{regexp_count, regexp_like, regexp_replace};
    use datafusion::logical_expr::expr::ScalarFunction;
    use datafusion::prelude::{Expr, cast, col, lit, try_cast};
    use datafusion::scalar::ScalarValue;
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

    /// A scope declaring these columns, for the checks that read an operand's
    /// declared type.
    fn scope_of(columns: &[(&str, DataType)]) -> DFSchema {
        let fields: Vec<Field> = columns
            .iter()
            .map(|(name, data_type)| Field::new(*name, data_type.clone(), true))
            .collect();
        DFSchema::try_from(Schema::new(fields)).expect("a schema of plain columns")
    }

    /// Regression test for #13915: `concat_to_string_concat` renders `||`,
    /// which `DuckDB` types by its operands — `BLOB || BLOB` is a `BLOB`, where
    /// the registered `SparkConcat` always returns a string. A binary operand
    /// therefore has to keep the call local, and the decision needs the scope,
    /// because the type of a column reference is not in the call.
    #[test]
    fn duckdb_declines_a_concat_over_a_binary_column() {
        for binary in [
            DataType::Binary,
            DataType::LargeBinary,
            DataType::BinaryView,
            DataType::FixedSizeBinary(3),
        ] {
            let scope = scope_of(&[("a", binary.clone()), ("s", DataType::Utf8)]);
            assert!(
                !duckdb_can_translate(&call_of(concat(vec![col("a"), col("s")])), Some(&scope)),
                "a concat over a {binary:?} column must stay local"
            );
            // The binary argument is refused wherever it sits, not only first.
            assert!(
                !duckdb_can_translate(&call_of(concat(vec![col("s"), col("a")])), Some(&scope)),
                "a concat whose second argument is {binary:?} must stay local"
            );
        }
    }

    /// A nested `concat` over binary columns is refused at the *outer* call,
    /// because the check searches the whole operand tree rather than reading
    /// the argument's final type.
    ///
    /// That distinction is the whole point: `SparkConcat` reports `Utf8` for
    /// the inner call whatever it was handed, so a check reading only the outer
    /// arguments sees two strings and admits it. `contains_unsupported_functions`
    /// would still have refused the plan — it walks every expression node with
    /// `Expr::apply`, so the inner call is visited in its own right — but that
    /// is the caller's property, not this check's, and this pins both.
    #[test]
    fn duckdb_declines_a_nested_concat_over_a_binary_column() {
        let scope = scope_of(&[("a", DataType::Binary), ("b", DataType::Binary)]);
        let nested = concat(vec![concat(vec![col("a"), col("b")]), lit("z")]);

        assert!(
            !duckdb_can_translate(&call_of(nested.clone()), Some(&scope)),
            "the outer call reaches binary columns through its operand tree"
        );

        // And the inner call on its own, which is what the caller's own walk
        // reaches independently.
        let Expr::ScalarFunction(outer) = &nested else {
            panic!("expected a scalar function call");
        };
        let Some(Expr::ScalarFunction(inner)) = outer.args.first() else {
            panic!("expected the inner call to be a scalar function");
        };
        assert!(
            !duckdb_can_translate(inner, Some(&scope)),
            "the inner concat over binary columns must be refused"
        );
    }

    /// Regression test for the explicit-cast bypass @copilot found on #14333:
    /// `ExprSchemable::get_type` reports a cast's *target*, so
    /// `concat(CAST(bin AS Utf8), 'z')` reads as a string concat. Measured on a
    /// DuckDB-accelerated dataset, that rendering answers
    /// `CAST("a" AS VARCHAR) || 'z'`, which for bytes that are not valid UTF-8
    /// returns the 12-character escaped literal as a row while the same query
    /// evaluated locally raises `Encountered non UTF-8 data`.
    #[test]
    fn duckdb_declines_a_concat_over_a_cast_away_binary_column() {
        let scope = scope_of(&[("a", DataType::Binary), ("s", DataType::Utf8)]);
        for laundered in [
            cast(col("a"), DataType::Utf8),
            cast(col("a"), DataType::Utf8View),
            try_cast(col("a"), DataType::Utf8),
            // A cast of a cast still originates in the binary column.
            cast(cast(col("a"), DataType::Utf8), DataType::LargeUtf8),
        ] {
            assert!(
                !duckdb_can_translate(
                    &call_of(concat(vec![laundered.clone(), lit("z")])),
                    Some(&scope)
                ),
                "a cast does not make {laundered:?} renderable"
            );
        }

        // A cast that has nothing binary under it is untouched.
        assert!(duckdb_can_translate(
            &call_of(concat(vec![cast(col("s"), DataType::LargeUtf8), lit("z")])),
            Some(&scope)
        ));
    }

    /// The refusal is scoped to binary operands: an ordinary string `concat` is
    /// the common case the `||` rewrite exists to keep pushed down (#13849), so
    /// it must still federate.
    #[test]
    fn duckdb_federates_a_concat_over_string_columns() {
        let scope = scope_of(&[
            ("s", DataType::Utf8),
            ("t", DataType::LargeUtf8),
            ("n", DataType::Int64),
        ]);
        for args in [
            vec![col("s"), lit("z")],
            vec![col("s"), col("t")],
            // A non-string, non-binary argument keeps the implicit cast the
            // un-rewritten call already relied on.
            vec![col("s"), col("n")],
            vec![lit("a"), lit("b")],
        ] {
            assert!(
                duckdb_can_translate(&call_of(concat(args.clone())), Some(&scope)),
                "concat({args:?}) has a faithful DuckDB rendering and must federate"
            );
        }
    }

    /// With no scope a column's type cannot be proven, and
    /// `ScalarCallSupport` says a rendering that depends on the type must
    /// refuse rather than assume one — a physical filter is checked with no
    /// scope, so guessing `Utf8` there would push down exactly the call this
    /// issue is about.
    #[test]
    fn duckdb_declines_a_concat_whose_column_type_cannot_be_read() {
        assert!(!duckdb_can_translate(
            &call_of(concat(vec![col("a"), lit("z")])),
            None
        ));
    }

    /// A literal carries its own type, so an all-literal call still federates
    /// with no scope: the refusal must cost only the calls it is about.
    ///
    /// A binary *literal* is refused on two independent paths, and each is
    /// asserted separately because only one of them is this check's.
    /// `duckdb_can_translate` consults the type guard *before* the unparser,
    /// and a `ScalarValue::Binary` reports `Binary` with or without a scope, so
    /// the guard is what answers `false` here. The `expr_to_sql` assertion
    /// establishes the other path — the renderer would have refused it too,
    /// with `NotImplemented("Unsupported scalar: Binary")` — so neither can be
    /// removed on the assumption that the other still covers a binary literal.
    ///
    /// A binary *column* has neither: it renders cleanly as `"a" || 'z'`, and
    /// its type is readable only against a scope. That is why the scope is what
    /// closes #13915 and an inspection of the arguments alone would not have.
    #[test]
    fn duckdb_reads_a_literal_argument_without_a_scope() {
        assert!(duckdb_can_translate(
            &call_of(concat(vec![lit("a"), lit("b")])),
            None
        ));

        let binary_literal = concat(vec![lit("a"), lit(ScalarValue::Binary(Some(vec![0xff])))]);
        assert!(!duckdb_can_translate(
            &call_of(binary_literal.clone()),
            None
        ));
        // The guard, which runs first, is the path that refuses it.
        assert!(
            !duckdb::concat_arguments_are_renderable(&call_of(binary_literal.clone()).args, None),
            "a binary literal reads as binary with no scope, so the type guard refuses it"
        );
        let dialect = new_duckdb_dialect();
        let unparser = Unparser::new(dialect.as_ref());
        assert!(
            unparser.expr_to_sql(&binary_literal).is_err(),
            "and the renderer behind the guard refuses it as well"
        );
    }

    /// Regression test for #13900: the `U` flag has no `DuckDB` equivalent, so
    /// the dialect's regex handler refuses to render the call. Before this
    /// check the refusal surfaced as a planning error for the whole query;
    /// declining to federate leaves the call for `DataFusion` to evaluate.
    ///
    /// `i` joined the refused set for #14148: the two engines case-fold by
    /// their own Unicode tables, so `regexp_like(s, '\x{1C89}', 'i')` over `ᲊ`
    /// is `true` locally and `false` federated (measured on the bundled
    /// `DuckDB`). `g` stays, measured to agree row for row.
    #[test]
    fn duckdb_declines_every_regexp_flag_but_the_global_replace() {
        for flag in ["U", "R", "gU", "iR", "i", "gi", "m", "s"] {
            assert!(
                !duckdb_can_translate(
                    &call_of(regexp_replace(
                        col("s"),
                        lit("a"),
                        lit("X"),
                        Some(lit(flag)),
                    )),
                    None
                ),
                "regexp_replace with flags `{flag}` has no DuckDB rendering"
            );
            assert!(
                !duckdb_can_translate(
                    &call_of(regexp_like(col("s"), lit("a"), Some(lit(flag)))),
                    None
                ),
                "regexp_like with flags `{flag}` has no DuckDB rendering"
            );
        }

        // The one flag both engines were measured to act on alike keeps
        // federating, and only for the function that takes it.
        assert!(
            duckdb_can_translate(
                &call_of(regexp_replace(col("s"), lit("a"), lit("X"), Some(lit("g")),)),
                None
            ),
            "regexp_replace with flags `g` renders as DuckDB SQL"
        );
        assert!(
            !duckdb_can_translate(
                &call_of(regexp_like(col("s"), lit("a"), Some(lit("g")))),
                None
            ),
            "regexp_like takes no `g`, so the flag has no DuckDB rendering there"
        );

        // No flags argument at all is the common shape and must federate.
        assert!(duckdb_can_translate(
            &call_of(regexp_replace(col("s"), lit("a"), lit("X"), None,)),
            None
        ));
    }

    /// Regression test for #13900: `regexp_count`'s start position becomes a
    /// `substring` offset in the `DuckDB` rewrite, which needs the value at
    /// unparse time. A column cannot supply one, and neither can a start
    /// below 1.
    #[test]
    fn duckdb_declines_a_regexp_count_start_it_cannot_turn_into_an_offset() {
        assert!(
            !duckdb_can_translate(
                &call_of(regexp_count(col("s"), lit("a"), Some(col("start")), None,)),
                None
            ),
            "a column start position has no DuckDB rendering"
        );
        assert!(
            !duckdb_can_translate(
                &call_of(regexp_count(col("s"), lit("a"), Some(lit(0)), None,)),
                None
            ),
            "a start position below 1 has no DuckDB rendering"
        );
        assert!(
            duckdb_can_translate(
                &call_of(regexp_count(col("s"), lit("a"), Some(lit(1)), None,)),
                None
            ),
            "an integer start position renders as a DuckDB substring offset"
        );
        assert!(
            duckdb_can_translate(
                &call_of(regexp_count(
                    col("s"),
                    lit("a"),
                    Some(lit(4_294_967_295_i64)),
                    None,
                )),
                None
            ),
            "the last offset DuckDB's SUBSTRING accepts still renders"
        );
        assert!(
            !duckdb_can_translate(
                &call_of(regexp_count(
                    col("s"),
                    lit("a"),
                    Some(lit(4_294_967_296_i64)),
                    None,
                )),
                None
            ),
            "a start past DuckDB's SUBSTRING range has no rendering and stays local"
        );
    }

    /// `regexp_count` is rendered only for the call shapes `DuckDB` has been
    /// measured to count as the kernel does (#13870): a string-literal pattern
    /// that cannot match the empty string and uses only syntax both engines
    /// read alike, and no flags. Every other shape stays local
    /// rather than answering differently.
    #[test]
    fn duckdb_declines_a_regexp_count_it_cannot_count_faithfully() {
        for (pattern, why) in [
            ("a*", "a pattern that can match the empty string"),
            ("a|\\b", "an alternation with a zero-width branch"),
            ("", "the empty pattern"),
            ("(", "a pattern the kernel cannot compile"),
            (
                "\\d",
                "a Perl class, Unicode-aware in the kernel and ASCII-only in RE2",
            ),
            (
                "\\ba",
                "a word boundary, which the two engines read differently",
            ),
            (
                "[a&&a]",
                "a class intersection, which RE2 reads as a class of `a` and `&`",
            ),
            ("(?x)a b", "the `x` flag, which RE2 rejects"),
            (
                "(a{100}){11}",
                "nested counted repetitions whose product passes RE2's limit of 1000",
            ),
            (
                "a++",
                "a quantifier applied to a quantifier, which RE2 rejects",
            ),
            (
                "a{01}",
                "a counted bound with a leading zero, which RE2 reads literally",
            ),
            (
                "([Kk]|a)",
                "a two-character class of case variants, which RE2 folds across Unicode when it factors an alternation",
            ),
            (
                "(?i)a",
                "case-insensitive matching, whose folding tables differ by Unicode version",
            ),
        ] {
            assert!(
                !duckdb_can_translate(
                    &call_of(regexp_count(col("s"), lit(pattern), None, None)),
                    None
                ),
                "{why} (`{pattern}`) has no faithful DuckDB rendering"
            );
        }
        assert!(
            !duckdb_can_translate(&call_of(regexp_count(col("s"), col("p"), None, None)), None),
            "a pattern read from a column cannot be inspected and stays local"
        );
        for flags in ["i", "m", "s", "c", "gi"] {
            assert!(
                !duckdb_can_translate(
                    &call_of(regexp_count(
                        col("s"),
                        lit("a"),
                        Some(lit(1)),
                        Some(lit(flags)),
                    )),
                    None
                ),
                "flags `{flags}` are refused (case folding differs by Unicode version, the rest RE2 reads differently) and stay local"
            );
        }
        assert!(
            !duckdb_can_translate(
                &call_of(regexp_count(
                    col("s"),
                    lit("a"),
                    Some(lit(1)),
                    Some(col("f")),
                )),
                None
            ),
            "a flags column is not a constant DuckDB accepts and stays local"
        );

        // The shapes that are rendered: the plain call, an anchored pattern
        // (zero-width anchors do not make the match itself empty), and a start.
        for expr in [
            regexp_count(col("s"), lit("a"), None, None),
            regexp_count(col("s"), lit("^a+$"), None, None),
            regexp_count(col("s"), lit("[0-9]{2,}"), Some(lit(3)), None),
        ] {
            assert!(
                duckdb_can_translate(&call_of(expr.clone()), None),
                "{expr:?} has a faithful DuckDB rendering and must federate"
            );
        }
    }

    /// A function the dialect installs no handler for is deferred to, so an
    /// ordinary call keeps federating.
    #[test]
    fn duckdb_defers_on_a_function_the_dialect_does_not_rewrite() {
        assert!(duckdb_can_translate(&call_of(upper(col("s"))), None));
    }

    /// The check must not *admit* a call the unparser cannot render, or the
    /// call still fails the query; and outside the type-dependent exception
    /// below it must not refuse one it can, or the pushdown is lost for
    /// nothing. Asking through `expr_to_sql` reaches the handler by the
    /// unparser's own dispatch rather than by the accessor the check uses.
    ///
    /// `concat` is deliberately not in this list. Its handler renders every
    /// call it is given, including the binary-operand calls whose rendering
    /// answers something else (#13915) — so for `concat` the check is
    /// *stricter* than the unparser by design, and asserting agreement here
    /// would assert the bug back in.
    /// `duckdb_declines_a_concat_over_a_binary_column` pins that direction.
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
            regexp_count(col("s"), lit("a*"), None, None),
            regexp_count(col("s"), lit("\\d"), None, None),
            regexp_count(col("s"), col("p"), None, None),
            regexp_count(col("s"), lit("a"), Some(lit(1)), Some(lit("i"))),
            regexp_count(col("s"), lit("a"), Some(lit(1)), Some(lit("m"))),
            upper(col("s")),
        ] {
            let renders = unparser.expr_to_sql(&expr).is_ok();
            assert_eq!(
                duckdb_can_translate(&call_of(expr.clone()), None),
                renders,
                "the per-call check and the unparser disagree about {expr:?}"
            );
        }
    }

    #[test]
    fn every_carved_out_bigquery_name_is_a_function_the_deny_list_knows() {
        let spice = runtime_udfs_api::spice_function_names();
        for name in bigquery_native_function_names() {
            assert!(
                spice.iter().any(|known| known == name),
                "`{name}` is not registered with the Spice deny-list, so carving it out \
                 of the deny-list does nothing"
            );
        }
    }

    #[test]
    fn no_denied_builtin_is_advertised_as_a_native_duckdb_function() {
        // `duckdb_native_function_names` is what the deny-list reads as its
        // carve-out, so a denied name appearing there would un-deny it and push
        // down a call DuckDB answers differently (#13809). Driven from the
        // deny-list itself rather than a hardcoded name, so denying another
        // built-in cannot skip this check.
        for name in crate::function_support::DUCKDB_DENIED_BUILTINS {
            assert!(
                !duckdb_native_function_names().contains(name),
                "`{name}` is denied for DuckDB and must not be advertised as native"
            );
        }
    }

    #[test]
    fn the_constructed_duckdb_dialect_renders_no_denied_builtin() {
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
