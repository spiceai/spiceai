/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

//! Backend-flavored [`FunctionSupport`] deny-lists.
//!
//! `runtime_udfs_api` owns the registry and the backend-agnostic builders. The
//! deny-lists here are the ones that need a *dialect* to derive what a backend
//! can evaluate natively, which is why they live beside [`crate::dialect`]
//! rather than in the interface crate.

use std::sync::Arc;

use arrow::datatypes::DataType;
use arrow_tools::schema_evolution::is_widening_cast;
use datafusion::{
    common::DFSchema,
    logical_expr::{Expr, ExprSchemable as _, expr::WindowFunctionDefinition},
    scalar::ScalarValue,
};
use datafusion_table_providers::util::supported_functions::{ExpressionSupport, FunctionSupport};
use runtime_udfs_api::{FunctionSupportBuilder, datafusion_nested_function_names};

/// The [`FunctionSupport`] for `DuckDB` connectors and accelerators: allows
/// every function the `DuckDB` dialect can rewrite into native SQL (e.g.
/// `cosine_distance` → `array_cosine_distance`, `rand` → `random()`), derived
/// from the dialect so it tracks it automatically.
///
/// On top of that carve-out, the built-ins in [`DUCKDB_DENIED_BUILTINS`] are
/// denied outright — see there for which, and why.
#[must_use]
pub fn deny_spice_functions_for_duckdb() -> Arc<FunctionSupport> {
    Arc::new(deny_spice_functions_for_duckdb_table_providers())
}

/// `DuckDB` deny-list as a value, for
/// `DuckDBTableFactory::with_function_support`. See issue #10703.
///
/// Two layers, both derived from [`crate::dialect`] so they cannot drift from
/// what the dialect can actually render:
///
/// 1. the name carve-out, so the Spice functions the `DuckDB` dialect rewrites
///    into native SQL federate instead of being denied;
/// 2. a per-call check, because a *name* the dialect handles is not a *call*
///    the dialect can render. `regexp_replace(s, p, r, 'U')` has no `DuckDB`
///    rendering — `DuckDB` has no `U` flag — and without this check the
///    refusal reaches the user as a planning error for a query `DataFusion`
///    can answer locally (issue #13900). The check also gates the
///    `DataFusion` built-ins the dialect rewrites, whose untranslatable shapes
///    must stay local the same way.
#[must_use]
pub fn deny_spice_functions_for_duckdb_table_providers() -> FunctionSupport {
    duckdb_function_support()
}

/// The `DataFusion` built-ins `DuckDB` must not be handed, because `DuckDB`
/// cannot evaluate them faithfully: one has a function that looks like the one
/// asked for but answers a different question, and one has no such function at
/// all.
///
/// `regexp_match` returns the first match's *capture groups* as a list, and
/// NULL when nothing matches. `DuckDB` has no function with those semantics:
/// `regexp_extract(s, p, 0)` returns the whole match as a plain string, and the
/// empty string — not NULL — when nothing matches. Translating one into the
/// other answered a different question on both counts (issue #13809), so the
/// call now evaluates locally, above the federated scan, where `DataFusion`'s
/// own implementation runs. That is the same treatment, and for the same
/// reason, that `regexp_match` already gets for `BigQuery`
/// (see [`deny_spice_functions_for_bigquery_table_providers`]).
///
/// The idiom that only asks "does it match at all" —
/// `regexp_match(…) IS [NOT] NULL` — is rewritten into `regexp_like` by
/// [`crate::optimizer_rule::RegexpMatchNullCheckRewrite`], and `regexp_like`
/// the `DuckDB` dialect does render natively (`regexp_matches`), so that shape
/// keeps a boolean instead of a list either way.
///
/// `regexp_instr` is here for the plainer reason that `DuckDB` has no function
/// of that name and the dialect renders none, so a federated call failed
/// remotely with `Catalog Error: Scalar Function with name regexp_instr does not
/// exist!` — the unknown-function failure the deny-list exists to prevent
/// (issue #10703).
///
/// `regexp_like`, `regexp_replace` and `regexp_count` are the three
/// `DataFusion` regexp built-ins the dialect renders, and all three are
/// screened per call by the handler rather than by name: only a literal
/// pattern built from syntax RE2 and the kernel's `regex` crate read alike
/// renders, because the divergences are silent ones that change which rows
/// match (`regexp_like('xy١', '\d')` was `false` federated and `true` locally
/// — issue #14148).
/// `regexp_count` was here until its rendering was made NULL-preserving
/// (issue #13870); the call shapes it still cannot render faithfully are
/// refused by the handler and evaluated locally through the per-call check
/// below, not by name. A denied name is never advertised as native, which
/// [`crate::dialect::duckdb_native_function_names`] enforces should a handled
/// name ever join this list.
pub const DUCKDB_DENIED_BUILTINS: &[&str] = &[
    crate::dialect::REGEXP_MATCH_NAME,
    crate::dialect::REGEXP_INSTR_NAME,
];

/// The deny-list for a consumer that installs the `DuckDB` dialect but wants
/// the **plain** Spice deny-list rather than the `DuckDB` carve-out — today the
/// `DuckLake` catalog connector, which withholds the vector UDFs because the
/// dialect's `cosine_distance` rewrite is not value-preserving (issue #13728).
///
/// It still needs [`DUCKDB_DENIED_BUILTINS`]. Those are `DataFusion` built-ins,
/// so the plain deny-list does not withhold them, and the dialect is the
/// `DuckDB` one — which no longer renders `regexp_match` at all, so without this
/// the call would be unparsed under its `DataFusion` name and fail remotely as
/// an unknown function.
///
/// It also carries the same per-call gate [`duckdb_function_support`] does, for
/// the same reason and by the same route: the gate belongs in
/// [`FunctionSupportBuilder::scalar_call`] so `build` composes it with the live
/// user-function check, because `FunctionSupport::with_scalar_call_support`
/// would replace that check instead. Applying it after `build` at the call site
/// is what regressed #13726/#13868 on this route.
#[must_use]
pub fn deny_spice_functions_for_duckdb_dialect_without_carve_out() -> FunctionSupport {
    FunctionSupportBuilder::new()
        .deny_also(DUCKDB_DENIED_BUILTINS.iter().map(|n| (*n).to_string()))
        .scalar_call(Arc::new(crate::dialect::duckdb_can_translate))
        .build()
        .with_expression_support(Arc::new(duckdb_can_evaluate_expression))
}

/// The one `DuckDB` policy both public accessors return, so the connector and
/// the accelerator cannot be given different pushdown rules.
fn duckdb_function_support() -> FunctionSupport {
    FunctionSupportBuilder::new()
        .native(&crate::dialect::duckdb_native_function_names())
        .deny_also(DUCKDB_DENIED_BUILTINS.iter().map(|n| (*n).to_string()))
        .scalar_call(Arc::new(crate::dialect::duckdb_can_translate))
        .build()
        .with_expression_support(Arc::new(duckdb_can_evaluate_expression))
}

/// Whether `DuckDB` evaluates this non-function expression node the way
/// `DataFusion` does: neither the casts the dialect refuses
/// ([`crate::dialect::duckdb_can_evaluate_expression`]) nor a decimal `avg`.
///
/// `DuckDB`'s `avg` over a `DECIMAL` answers a `DOUBLE`, which carries about
/// 16 significant digits and rounds, where `DataFusion` divides the exact
/// decimal sum and truncates to `Decimal128(p + 4, s + 4)`. The double comes
/// back cast to that type, so at TPC-H scale it reads one unit high in the
/// last place, and for a large `DECIMAL(38, 2)` it is wrong in the integer
/// digits. A decimal `avg` therefore stays local (issue #14492). `DuckDB`'s
/// decimal `sum` is exact and keeps its pushdown.
#[must_use]
pub fn duckdb_can_evaluate_expression(expr: &Expr, schema: Option<&DFSchema>) -> bool {
    crate::dialect::duckdb_can_evaluate_expression(expr, schema)
        && !aggregates_a_decimal(expr, schema, &["avg"])
}

/// Whether `expr` is a call of one of the aggregates `names`, plain or as a
/// window function, over an operand that is a decimal or whose type cannot
/// be read (no scope), which is refused rather than assumed.
fn aggregates_a_decimal(expr: &Expr, schema: Option<&DFSchema>, names: &[&str]) -> bool {
    let (name, args) = match expr {
        Expr::AggregateFunction(aggregate) => (aggregate.func.name(), &aggregate.params.args),
        Expr::WindowFunction(window) => match &window.fun {
            WindowFunctionDefinition::AggregateUDF(aggregate) => {
                (aggregate.name(), &window.params.args)
            }
            WindowFunctionDefinition::WindowUDF(_) => return false,
        },
        _ => return false,
    };
    if !names
        .iter()
        .any(|candidate| name.eq_ignore_ascii_case(candidate))
    {
        return false;
    }
    let scope = schema.unwrap_or_else(|| DFSchema::empty_ref());
    args.iter().any(|arg| {
        arg.get_type(scope)
            .map_or(true, |data_type| data_type.is_decimal())
    })
}

/// The [`FunctionSupport`] for `BigQuery` over ADBC, as a value for
/// `AdbcTableFactory::with_function_support`.
///
/// Four layers. Function decisions are derived from [`crate::dialect`] so they
/// cannot drift from what the dialect can actually render; the expression
/// restriction records a `GoogleSQL` capability boundary:
///
/// 1. the name carve-out, so the JSON extraction functions the `BigQuery`
///    dialect rewrites into `JSON_VALUE` federate instead of being denied;
/// 2. a per-call check, because a carved-out *name* is not a carved-out *call*.
///    `json_get_int(doc, key_col)` is legal and has no `BigQuery` translation —
///    its JSON path argument must be a constant — and without this check that
///    call would federate and be unparsed verbatim, which is the
///    unknown-function failure the deny-list exists to prevent (issue #10703).
///    The check also gates the `DataFusion` built-ins the dialect rewrites
///    (e.g. `regexp_like` → `REGEXP_CONTAINS`), whose untranslatable shapes
///    must stay local the same way;
/// 3. case-insensitive [`Expr::Like`] is denied because `GoogleSQL` has no
///    `ILIKE` operator. Both positive and negated forms evaluate locally while
///    ordinary case-sensitive `LIKE` remains pushable;
/// 4. `regexp_match` is denied outright. `BigQuery` has no function of that
///    name — a federated call fails remotely with `Function not found:
///    regexp_match` — and no faithful rendering exists to rewrite it into: its
///    list-of-matches result has no `BigQuery` counterpart that survives the
///    result boundary (`BigQuery` documents that a NULL top-level `ARRAY`
///    comes back as an empty one, where `regexp_match` is NULL for a
///    non-matching row), and `REGEXP_EXTRACT` refuses a pattern with more than
///    one capturing group. The common reason to call it — a NULL-check asking
///    "does it match at all" — is rewritten into `regexp_like` before
///    the `BigQuery` capability check by
///    [`crate::optimizer_rule::RegexpMatchNullCheckRewrite`], which the dialect
///    *can* translate; every remaining shape evaluates locally above the
///    federated scan.
#[must_use]
pub fn deny_spice_functions_for_bigquery_table_providers() -> FunctionSupport {
    FunctionSupportBuilder::new()
        .native(&crate::dialect::bigquery_native_function_names())
        .deny_also([crate::dialect::REGEXP_MATCH_NAME.to_string()])
        .scalar_call(Arc::new(crate::dialect::bigquery_can_translate))
        .build()
        .with_expression_support(Arc::new(bigquery_can_evaluate_expression))
        // The builder carries the scalar hook; the aggregate and window hooks
        // have no builder method yet, so they are installed on the built value.
        .with_aggregate_call_support(Arc::new(crate::dialect::bigquery_can_translate_aggregate))
        .with_window_call_support(Arc::new(crate::dialect::bigquery_can_translate_window))
}

/// Whether `BigQuery` can evaluate this non-function expression shape without
/// changing `DataFusion` semantics.
///
/// `GoogleSQL` has case-sensitive `LIKE` but no `ILIKE`. Rewriting through
/// `LOWER` is not known to preserve Unicode, collation, pattern, and escape
/// semantics, so either positive or negated case-insensitive `LIKE` stays
/// local. Binary operator variants are intentionally outside this policy.
///
/// A cast from a fractional value into an integer stays local too: `BigQuery`
/// documents that it rounds one where `DataFusion` truncates
/// ([`crate::dialect::integer_cast_is_renderable`]).
///
/// So does a cast `BigQuery` renders at a lower precision than `DataFusion`
/// evaluates it — text into a nanosecond timestamp, or a decimal scale past
/// `BIGNUMERIC` ([`crate::dialect::bigquery_cast_is_renderable`]).
#[must_use]
pub fn bigquery_can_evaluate_expression(expr: &Expr, schema: Option<&DFSchema>) -> bool {
    !matches!(expr, Expr::Like(like) if like.case_insensitive)
        && crate::dialect::integer_cast_is_renderable(expr, schema)
        && crate::dialect::bigquery_cast_is_renderable(expr, schema)
}

/// The per-expression gate for an engine reached through a generic driver,
/// keyed by the name the driver is configured with — an ADBC `adbc_driver`
/// value, or an ODBC profile — for a connector whose dialect is generic and
/// whose policy is therefore the plain deny-list. The gate is what keeps a
/// cast the engine evaluates differently from `DataFusion` (a fractional value
/// into an integer, which these engines round where `DataFusion` truncates)
/// out of the pushdown on that route too; without it the same statement
/// answered differently through ADBC or ODBC than through the engine's own
/// connector (issue #14482). `SQLite` is keyed for a different reason: it has
/// no date, time or interval types, so its gate also keeps every temporal value
/// local ([`sqlite_driver_can_evaluate_expression`]). `None` for an engine with
/// no such shape, or one this crate has no gate for, which keeps the plain
/// policy.
#[must_use]
pub fn expression_support_for_engine(engine: &str) -> Option<ExpressionSupport> {
    match engine {
        "bigquery" => Some(Arc::new(bigquery_can_evaluate_expression)),
        "duckdb" => Some(Arc::new(crate::dialect::duckdb_can_evaluate_expression)),
        "postgres" | "postgresql" => {
            Some(Arc::new(crate::dialect::postgres_can_evaluate_expression))
        }
        "mysql" => Some(Arc::new(crate::dialect::mysql_can_evaluate_expression)),
        "sqlite" => Some(Arc::new(sqlite_driver_can_evaluate_expression)),
        // Documented to round a fractional value cast into an integer, like the
        // engines above; the gate costs them only the cast's pushdown.
        "snowflake" | "athena" => Some(Arc::new(crate::dialect::integer_cast_is_renderable)),
        _ => None,
    }
}

/// `SQLite`-flavored deny-list as a value, for
/// `SqliteTableProviderFactory::with_function_support`.
///
/// Every Spice function, plus `btrim`. `SQLite` has no `btrim` — a pushed-down
/// `trim` reaches it under `DataFusion`'s canonical name and fails the query
/// with `no such function: btrim`, the same defect issue #13794 reports against
/// `DuckDB`. `SQLite` does have a `trim` that matches, but its unparser dialect
/// is constructed inside `datafusion-table-providers` with no seam to install a
/// rewrite through, so the call is denied and evaluates locally above the
/// federated scan instead. That costs the pushdown for those plans and returns
/// the right rows, which is the trade the deny-list exists to make.
///
/// Casts and decimal aggregates are gated by [`sqlite_can_evaluate_expression`].
#[must_use]
pub fn deny_spice_functions_for_sqlite_table_providers() -> FunctionSupport {
    FunctionSupportBuilder::new()
        .deny_also([crate::dialect::BTRIM_NAME.to_string()])
        .build()
        .with_expression_support(Arc::new(sqlite_can_evaluate_expression))
}

/// Whether `SQLite` evaluates this non-function expression node the way
/// `DataFusion` does.
///
/// `SQLite` has no `TRY_CAST`, so a federated one fails the query with a
/// syntax error; it always stays local (issue #14398).
///
/// `SQLite`'s `CAST` never fails: it converts the longest numeric prefix of
/// its operand and answers `0` when there is none, and saturates a float too
/// large for an integer. `DataFusion` refuses those values, so a federated cast
/// answers rows where the local query raises — `CAST('abc' AS BIGINT)` is `0`,
/// `CAST('12abc' AS BIGINT)` is `12`, `CAST(1e30 AS BIGINT)` is
/// `9223372036854775807`, and every binary value casts to `0`. In a filter the
/// wrong value then selects rows. The formatting of a cast into text differs
/// too (`1.0e+30` for `1e30`, `1` for `true`), and a binary value cast into
/// text is not validated as UTF-8, so a filter over it matches bytes
/// `DataFusion` refuses. A `SQLite` accelerator stores dates and timestamps
/// in its own representation, and a cast between them selects different rows.
///
/// So a cast federates only when it is one `SQLite` is known to evaluate the
/// same way — see [`sqlite_cast_is_faithful`] — and every other cast stays
/// local, which costs the pushdown and cannot return a wrong row. An operand
/// whose type cannot be read (no scope) is refused rather than assumed. A
/// literal carries its own type; a date literal is the one literal shape
/// admitted beyond that set (see [`sqlite_date_literal_is_canonical`]).
#[must_use]
pub fn sqlite_can_evaluate_expression(expr: &Expr, schema: Option<&DFSchema>) -> bool {
    match expr {
        Expr::TryCast(_) => false,
        // SQLite has no decimal type: it stores a decimal as a REAL or an
        // INTEGER and computes `avg` and `sum` over it in floating point or in
        // 64-bit integers. `avg` reads one unit high in the last place against
        // DataFusion's truncating decimal division, and a `sum` past `i64`
        // fails with `integer overflow`, so both stay local (issue #14492).
        Expr::AggregateFunction(_) | Expr::WindowFunction(_) => {
            !aggregates_a_decimal(expr, schema, &["avg", "sum"])
        }
        Expr::Cast(cast) => {
            let to = cast.field.data_type();
            if let Expr::Literal(value, _) = cast.expr.as_ref() {
                return sqlite_cast_is_faithful(&value.data_type(), to)
                    || sqlite_date_literal_is_canonical(value, to);
            }
            cast.expr
                .get_type(schema.unwrap_or_else(|| DFSchema::empty_ref()))
                .is_ok_and(|from| sqlite_cast_is_faithful(&from, to))
        }
        _ => true,
    }
}

/// Whether `SQLite` evaluates this expression node the way `DataFusion` does
/// when the SQL reaches it through a generic driver — an ADBC `sqlite` driver,
/// or an ODBC `SQLite` profile — rather than through Spice's own `SQLite`
/// connector.
///
/// Everything [`sqlite_can_evaluate_expression`] refuses, for the same reasons,
/// and every date, time, timestamp, duration or interval value besides. `SQLite`
/// has none of those types. The ADBC route unparses with the generic dialect,
/// which renders `TIMESTAMP '2026-01-30 23:00:00'` as
/// `CAST('2026-01-30 23:00:00' AS TIMESTAMP)`: `SQLite` gives that cast NUMERIC
/// affinity and answers the integer `2026`, and since every text value compares
/// greater than every number, a filter against it selects every row (issue
/// #14753). A `DATE` literal becomes `2026` the same way, so the canonical date
/// literal [`sqlite_can_evaluate_expression`] admits — safe only where the
/// `SQLite` dialect renders it as text — is refused here. An interval renders as
/// `INTERVAL '1 HOURS'`, which `SQLite` cannot parse (issue #14754). Each of
/// these stays local, where `DataFusion` evaluates it over the values the driver
/// returns.
#[must_use]
pub fn sqlite_driver_can_evaluate_expression(expr: &Expr, schema: Option<&DFSchema>) -> bool {
    match expr {
        Expr::Literal(value, _) => !value.data_type().is_temporal(),
        Expr::Cast(cast) if cast.field.data_type().is_temporal() => false,
        _ => sqlite_can_evaluate_expression(expr, schema),
    }
}

/// Whether a string literal cast into a date is one `SQLite` compares the way
/// `DataFusion` does.
///
/// `DATE '1994-01-01'` reaches federation as a cast of a string literal, which
/// the `SQLite` dialect renders as `CAST('1994-01-01' AS TEXT)`: the string
/// itself, compared as text against the stored `YYYY-MM-DD` values. That is
/// the same comparison exactly when the literal is already written in that
/// form. `DataFusion` also parses `'1994-1-5'` as 1994-01-05, which as text
/// matches no stored date, so a literal in any other spelling stays local.
fn sqlite_date_literal_is_canonical(value: &ScalarValue, to: &DataType) -> bool {
    let (Some(Some(literal)), DataType::Date32) = (value.try_as_str(), to) else {
        return false;
    };
    value
        .cast_to(&DataType::Date32)
        .and_then(|date| date.cast_to(&DataType::Utf8))
        .is_ok_and(|canonical| canonical.try_as_str() == Some(Some(literal)))
}

/// The casts `SQLite` evaluates exactly as `DataFusion` does: between string
/// types, between binary types, an integer widened to an integer that holds
/// every value of it, an integer or `Float32` to `Float64`, and an integer
/// into text.
fn sqlite_cast_is_faithful(from: &DataType, to: &DataType) -> bool {
    if from == to || (from.is_string() && to.is_string()) || (from.is_binary() && to.is_binary()) {
        return true;
    }
    if from.is_integer() {
        // Lossless widening, and only into an integer: `is_widening_cast` also
        // admits floats, which SQLite stores as a double whatever the width.
        return to.is_string()
            || *to == DataType::Float64
            || (to.is_integer() && is_widening_cast(from, to));
    }
    *from == DataType::Float32 && *to == DataType::Float64
}

/// MySQL-flavored deny-list as a value, for
/// `MySQLTableFactory::with_function_support`.
///
/// Every Spice function, plus `btrim`, for the reason
/// [`deny_spice_functions_for_sqlite_table_providers`] gives — `MySQL` answers a
/// federated `trim` with `FUNCTION <db>.btrim does not exist`. Denying is the
/// only faithful option here rather than merely the available one: `MySQL`'s
/// `TRIM` has no two-argument form at all, and its `TRIM(BOTH chars FROM str)`
/// strips repetitions of `chars` as a *string*, where `btrim` strips any
/// character in it — so a rewrite would trade a failed query for wrong rows.
///
/// A cast from a fractional value into an integer stays local too, because
/// `MySQL` rounds it where `DataFusion` truncates
/// ([`crate::dialect::mysql_can_evaluate_expression`]).
#[must_use]
pub fn deny_spice_functions_for_mysql_table_providers() -> FunctionSupport {
    FunctionSupportBuilder::new()
        .deny_also([crate::dialect::BTRIM_NAME.to_string()])
        .build()
        .with_expression_support(Arc::new(crate::dialect::mysql_can_evaluate_expression))
}

/// `DataFusion`'s nested array/list/map functions that `PostgreSQL` cannot
/// evaluate are denied for `PostgreSQL` and PostgreSQL-wire backends (e.g.
/// Redshift). The ones that match `PostgreSQL` exactly are listed here so they
/// keep pushing down — this is dialect-level knowledge of what the backend can
/// run, which is why it sits beside [`crate::dialect`] rather than in the UDF
/// registry, whose entries are backend-agnostic.
///
/// Public because it *is* the pushdown allowlist: a caller assembling its own
/// PostgreSQL-flavored [`FunctionSupport`] needs to know which array functions
/// federate, and `runtime`'s deny-list tests assert against it by name.
pub const POSTGRES_PUSHABLE_ARRAY_FUNCTIONS: &[&str] = &[
    "array_append",    // (array, element) — identical to PostgreSQL
    "array_prepend",   // (element, array) — identical to PostgreSQL
    "array_ndims",     // (array) -> int
    "array_position",  // (array, element[, start]) -> int
    "array_positions", // (array, element) -> int[]
    "array_to_string", // (array, delimiter[, null_string]) -> text
    "cardinality",     // (array) -> int
    "string_to_array", // (string, delimiter[, null_string]) -> text[]
];

/// Postgres-flavored deny-list as a value: every Spice function, plus the
/// `DataFusion` array functions `PostgreSQL` can't execute. Used with
/// `PostgresTableProviderFactory::with_function_support` (accelerator) and the
/// `PostgreSQL` connector's federation deny-list. See issue #10703.
///
/// A cast from a fractional value into an integer stays local too, because
/// `PostgreSQL` rounds it where `DataFusion` truncates
/// ([`crate::dialect::postgres_can_evaluate_expression`]).
#[must_use]
pub fn deny_spice_functions_for_postgres_table_providers() -> FunctionSupport {
    let unsupported_arrays = datafusion_nested_function_names()
        .iter()
        .filter(|name| !POSTGRES_PUSHABLE_ARRAY_FUNCTIONS.contains(&name.as_str()))
        .cloned();
    FunctionSupportBuilder::new()
        .deny_also(unsupported_arrays)
        .build()
        .with_expression_support(Arc::new(crate::dialect::postgres_can_evaluate_expression))
}

#[cfg(test)]
mod tests {
    use super::{
        FunctionSupport, deny_spice_functions_for_bigquery_table_providers,
        deny_spice_functions_for_duckdb_dialect_without_carve_out,
        deny_spice_functions_for_duckdb_table_providers,
        deny_spice_functions_for_mysql_table_providers,
        deny_spice_functions_for_postgres_table_providers,
        deny_spice_functions_for_sqlite_table_providers, expression_support_for_engine,
    };
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use datafusion::common::DFSchema;
    use datafusion::functions::core::expr_fn::{
        arrow_cast, arrow_field, arrow_metadata, arrow_try_cast, arrow_typeof, cast_to_type,
        try_cast_to_type, with_metadata,
    };
    use datafusion::functions::regex::expr_fn::{regexp_count, regexp_replace};
    use datafusion::logical_expr::{LogicalPlan, LogicalPlanBuilder, table_scan};
    use datafusion::prelude::{Expr, cast, col, lit, try_cast};
    use datafusion::scalar::ScalarValue;
    use datafusion_table_providers::util::supported_functions::contains_unsupported_functions;
    use runtime_udfs_api::{add_user_function, function_support, remove_user_function};

    /// A scan of `t(s, start)` projecting `expr`, which is the shape federation
    /// is asked to decide about.
    fn plan_projecting(expr: Expr) -> LogicalPlan {
        scan_t()
            .project(vec![expr])
            .expect("project")
            .build()
            .expect("build plan")
    }

    /// A scan of `t(s, start)` filtered by `predicate`.
    fn plan_filtering(predicate: Expr) -> LogicalPlan {
        scan_t()
            .filter(predicate)
            .expect("filter")
            .build()
            .expect("build plan")
    }

    fn scan_t() -> LogicalPlanBuilder {
        let schema = Schema::new(vec![
            Field::new("s", DataType::Utf8, true),
            Field::new("start", DataType::Int64, true),
        ]);
        table_scan(Some("t"), &schema, None).expect("scan t")
    }

    /// Regression test for #14444. `DataFusion`'s own cast built-ins must be
    /// evaluated locally under every backend policy — as a projection and as a
    /// filter — because each backend either lacks the function (`DuckDB` and
    /// `SQLite` failed the query as an unknown function) or, like `DuckDB`'s
    /// `cast_to_type`, casts by its own rules rather than Arrow's.
    #[test]
    fn a_datafusion_cast_builtin_stays_local_on_every_backend() {
        let plans: Vec<(&str, LogicalPlan)> = [
            ("arrow_cast", arrow_cast(col("start"), lit("LargeUtf8"))),
            ("arrow_try_cast", arrow_try_cast(col("s"), lit("Int64"))),
            ("cast_to_type", cast_to_type(col("start"), lit(1_i32))),
            ("try_cast_to_type", try_cast_to_type(col("s"), lit(1_i64))),
        ]
        .into_iter()
        .flat_map(|(name, cast)| {
            [
                (name, plan_projecting(cast.clone())),
                (name, plan_filtering(cast.is_not_null())),
            ]
        })
        .collect();
        // The column itself still federates: the refusal costs only the casts
        // it is about.
        let plain_column = plan_projecting(col("start"));
        let policies = [
            ("plain", function_support()),
            ("DuckDB", deny_spice_functions_for_duckdb_table_providers()),
            (
                "DuckLake",
                deny_spice_functions_for_duckdb_dialect_without_carve_out(),
            ),
            (
                "BigQuery",
                deny_spice_functions_for_bigquery_table_providers(),
            ),
            (
                "PostgreSQL",
                deny_spice_functions_for_postgres_table_providers(),
            ),
            ("SQLite", deny_spice_functions_for_sqlite_table_providers()),
            ("MySQL", deny_spice_functions_for_mysql_table_providers()),
        ];

        for (policy, support) in &policies {
            for (name, plan) in &plans {
                assert!(
                    contains_unsupported_functions(plan, support)
                        .expect("the support check must not error"),
                    "the {policy} policy must evaluate {name} locally rather than federate it:\n{plan}"
                );
            }
            assert!(
                !contains_unsupported_functions(&plain_column, support)
                    .expect("the support check must not error"),
                "the {policy} policy must still federate a plain column"
            );
        }
    }

    /// Whether federation would push this plan into `DuckDB`, which is what
    /// `SqlTable::can_execute_plan` asks of the deny-list.
    fn federates(expr: Expr) -> bool {
        pushes(
            &plan_projecting(expr),
            &deny_spice_functions_for_duckdb_table_providers(),
        )
    }

    /// Regression test for #13900. Federation is an optimization: a call the
    /// `DuckDB` dialect cannot render must not be federated, so `DataFusion`
    /// evaluates it locally instead of the query failing at planning.
    #[test]
    fn a_duckdb_untranslatable_call_is_not_federated() {
        assert!(
            !federates(regexp_replace(col("s"), lit("a"), lit("X"), Some(lit("U")),)),
            "the `U` flag has no DuckDB rendering, so this plan must stay local"
        );
        assert!(
            !federates(regexp_count(col("s"), lit("a"), Some(col("start")), None)),
            "a column start position has no DuckDB rendering, so this plan must stay local"
        );
    }

    /// Regression test for #14334. `arrow_typeof` must not be unparsed into the
    /// SQL sent to a `DuckDB` accelerator, which has no function of that name;
    /// the call describes the `DataFusion` plan's type, so no backend can
    /// answer it and every policy must keep it local. The three siblings from
    /// the same `DataFusion` module are the same class, and the plain policy
    /// every catalog connector takes is covered alongside the backend ones.
    #[test]
    fn a_plan_introspection_builtin_stays_local_on_every_backend() {
        let calls = [
            ("arrow_typeof", arrow_typeof(col("s"))),
            ("arrow_field", arrow_field(col("s"))),
            ("arrow_metadata", arrow_metadata(vec![col("s")])),
            (
                "with_metadata",
                with_metadata(vec![col("s"), lit("k"), lit("v")]),
            ),
        ];
        let policies = [
            ("plain", function_support()),
            ("DuckDB", deny_spice_functions_for_duckdb_table_providers()),
            (
                "DuckLake",
                deny_spice_functions_for_duckdb_dialect_without_carve_out(),
            ),
            (
                "BigQuery",
                deny_spice_functions_for_bigquery_table_providers(),
            ),
            (
                "PostgreSQL",
                deny_spice_functions_for_postgres_table_providers(),
            ),
            ("SQLite", deny_spice_functions_for_sqlite_table_providers()),
            ("MySQL", deny_spice_functions_for_mysql_table_providers()),
        ];

        for (policy, support) in &policies {
            for (name, call) in &calls {
                assert!(
                    contains_unsupported_functions(&plan_projecting(call.clone()), support)
                        .expect("the support check must not error"),
                    "{name} answers about the DataFusion plan, not the data, so the {policy} \
                     policy must evaluate it locally rather than federate it"
                );
            }
        }
    }

    /// The complement: the per-call check must not cost a pushdown that works.
    #[test]
    fn a_duckdb_renderable_call_still_federates() {
        assert!(federates(regexp_replace(
            col("s"),
            lit("a"),
            lit("X"),
            Some(lit("g")),
        )));
        assert!(federates(col("s")));
    }

    #[test]
    fn bigquery_refuses_only_case_insensitive_like_expressions() {
        let support = deny_spice_functions_for_bigquery_table_providers();
        for denied in [col("s").ilike(lit("u%")), col("s").not_ilike(lit("u%"))] {
            assert!(
                contains_unsupported_functions(&plan_projecting(denied), &support)
                    .expect("the support check must not error"),
                "BigQuery has no ILIKE operator, including its negated form"
            );
        }

        assert!(
            !contains_unsupported_functions(&plan_projecting(col("s").like(lit("u%"))), &support,)
                .expect("the support check must not error"),
            "ordinary LIKE is valid GoogleSQL and must keep federating"
        );
    }

    fn sqlite_scope() -> DFSchema {
        DFSchema::try_from(Schema::new(vec![
            Field::new("bin", DataType::Binary, true),
            Field::new("s", DataType::Utf8, true),
            Field::new("i8", DataType::Int8, true),
            Field::new("i32", DataType::Int32, true),
            Field::new("i64", DataType::Int64, true),
            Field::new("u8", DataType::UInt8, true),
            Field::new("u64", DataType::UInt64, true),
            Field::new("f32", DataType::Float32, true),
            Field::new("f64", DataType::Float64, true),
            Field::new("b", DataType::Boolean, true),
            Field::new("d", DataType::Date32, true),
            Field::new("ts", DataType::Timestamp(TimeUnit::Microsecond, None), true),
        ]))
        .expect("scope schema")
    }

    /// Regression test for #14398: `SQLite` has no `TRY_CAST`, and its `CAST`
    /// answers a value where `DataFusion` refuses one, so every `TRY_CAST` and
    /// every cast outside the faithful set stays local.
    #[test]
    fn sqlite_keeps_unfaithful_casts_local() {
        let scope = sqlite_scope();
        let support = deny_spice_functions_for_sqlite_table_providers();
        let mut refused = vec![
            // No `TRY_CAST` in SQLite, whatever the types.
            try_cast(col("bin"), DataType::Utf8),
            try_cast(col("s"), DataType::Int64),
            try_cast(col("i64"), DataType::Utf8),
            try_cast(col("i32"), DataType::Int64),
        ];
        for (operand, target) in [
            // Binary into anything but binary, text included.
            ("bin", DataType::Int64),
            ("bin", DataType::Float64),
            ("bin", DataType::Utf8),
            ("bin", DataType::Utf8View),
            // Text into a number, a boolean, or a date.
            ("s", DataType::Int64),
            ("s", DataType::Float64),
            ("s", DataType::Boolean),
            ("s", DataType::Date32),
            // A float into an integer saturates; into text it formats differently.
            ("f64", DataType::Int64),
            ("f64", DataType::Int32),
            ("f64", DataType::Utf8),
            ("f64", DataType::Float32),
            // Narrowing, and a signed value into an unsigned type.
            ("i64", DataType::Int32),
            ("i32", DataType::UInt64),
            ("u64", DataType::Int64),
            // An integer into Float32: SQLite's REAL is a double.
            ("i32", DataType::Float32),
            ("i32", DataType::Decimal128(10, 2)),
            ("i32", DataType::Boolean),
            // Booleans and temporals.
            ("b", DataType::Utf8),
            ("b", DataType::Int32),
            ("d", DataType::Utf8),
            ("d", DataType::Timestamp(TimeUnit::Microsecond, None)),
            ("ts", DataType::Date32),
        ] {
            refused.push(cast(col(operand), target));
        }
        for expr in refused {
            assert!(
                !support.supports(&expr, Some(&scope)),
                "{expr} has no faithful SQLite rendering and must stay local"
            );
        }
    }

    /// The complement: the casts `SQLite` evaluates the same way still
    /// federate, and a node that is not a cast is not this check's to refuse.
    #[test]
    fn sqlite_federates_faithful_casts() {
        let scope = sqlite_scope();
        let support = deny_spice_functions_for_sqlite_table_providers();
        for (operand, target) in [
            ("s", DataType::LargeUtf8),
            ("s", DataType::Utf8View),
            ("bin", DataType::LargeBinary),
            ("i8", DataType::Int32),
            ("i32", DataType::Int64),
            ("u8", DataType::Int16),
            ("u8", DataType::UInt64),
            ("i32", DataType::Float64),
            ("i64", DataType::Float64),
            ("f32", DataType::Float64),
            ("i64", DataType::Utf8),
            ("u8", DataType::Utf8View),
        ] {
            let expr = cast(col(operand), target);
            assert!(
                support.supports(&expr, Some(&scope)),
                "{expr} is faithful on SQLite and must federate"
            );
        }
        for expr in [col("bin"), col("s").like(lit("a%")), col("i32").is_null()] {
            assert!(
                support.supports(&expr, Some(&scope)),
                "{expr} must federate"
            );
        }
    }

    /// With no scope a column's type cannot be read, so a cast of it stays
    /// local; a literal carries its own type.
    #[test]
    fn sqlite_refuses_a_cast_whose_operand_type_cannot_be_read() {
        let support = deny_spice_functions_for_sqlite_table_providers();
        assert!(!support.supports(&cast(col("i32"), DataType::Int64), None));
        assert!(support.supports(&cast(lit(1_i32), DataType::Int64), None));
        assert!(!support.supports(&cast(lit("abc"), DataType::Int64), None));
    }

    /// `DATE '…'` is a cast of a string literal, which `SQLite` compares as
    /// text: only a literal already spelled `YYYY-MM-DD` compares the same way.
    #[test]
    fn sqlite_federates_only_a_canonical_date_literal() {
        let support = deny_spice_functions_for_sqlite_table_providers();
        for canonical in ["1994-01-01", "2024-02-29", "0001-01-01"] {
            assert!(
                support.supports(&cast(lit(canonical), DataType::Date32), None),
                "{canonical} is canonical and must federate"
            );
        }
        for other in [
            "1994-1-5",
            "1994-01-01T00:00:00",
            " 1994-01-01",
            "not a date",
        ] {
            assert!(
                !support.supports(&cast(lit(other), DataType::Date32), None),
                "{other:?} compares differently as text and must stay local"
            );
        }
        assert!(
            !support.supports(
                &cast(
                    lit("1994-01-01"),
                    DataType::Timestamp(TimeUnit::Microsecond, None)
                ),
                None
            ),
            "a timestamp literal is not a date literal"
        );
        assert!(
            !support.supports(&cast(col("s"), DataType::Date32), Some(&sqlite_scope())),
            "a string column cast into a date is not a literal"
        );
    }

    /// The check reaches a cast nested in a filter predicate of a real plan,
    /// which is where a wrong value selects wrong rows.
    #[test]
    fn a_sqlite_plan_filtering_on_an_unfaithful_cast_is_not_federated() {
        let support = deny_spice_functions_for_sqlite_table_providers();
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("s", DataType::Utf8, true),
        ]);
        let plan = |predicate: Expr| {
            table_scan(Some("t"), &schema, None)
                .expect("scan t")
                .filter(predicate)
                .expect("filter")
                .project(vec![col("id")])
                .expect("project")
                .build()
                .expect("build plan")
        };
        assert!(
            contains_unsupported_functions(
                &plan(cast(col("s"), DataType::Int64).eq(lit(0_i64))),
                &support
            )
            .expect("the support check must not error"),
            "CAST(s AS BIGINT) = 0 selects 'abc' on SQLite and must stay local"
        );
        assert!(
            !contains_unsupported_functions(
                &plan(cast(col("id"), DataType::Utf8).eq(lit("1"))),
                &support
            )
            .expect("the support check must not error"),
            "an integer cast into text is faithful and must keep federating"
        );
    }

    /// The two layers rank: a name in [`DUCKDB_DENIED_BUILTINS`] does not
    /// federate whatever the per-call check says, and a name that is not
    /// denied federates exactly when the dialect renders the call.
    /// `regexp_count` with an integer start is the second case: the rendering
    /// is value-preserving (#13870), so it is not denied by name and the call
    /// federates; `regexp_match` is the first, denied by name because `DuckDB`
    /// has no faithful rendering of it at all (#13809).
    #[test]
    fn a_denied_name_stays_local_and_a_rendered_name_federates() {
        assert!(
            federates(regexp_count(col("s"), lit("a"), Some(lit(1)), None)),
            "regexp_count is rendered faithfully and not denied by name, so it federates"
        );
        assert!(
            !federates(datafusion::functions::regex::expr_fn::regexp_match(
                col("s"),
                lit("a"),
                None,
            )),
            "regexp_match is denied by name, so no call of it may federate"
        );
    }

    /// A one-argument call of `name`, standing in for a user-registered scalar
    /// function reaching the deny-list.
    fn user_call(name: &str) -> Expr {
        Expr::ScalarFunction(datafusion::logical_expr::expr::ScalarFunction::new_udf(
            std::sync::Arc::new(datafusion::logical_expr::create_udf(
                name,
                vec![DataType::Utf8],
                DataType::Utf8,
                datafusion::logical_expr::Volatility::Immutable,
                std::sync::Arc::new(|_| {
                    Ok(datafusion::logical_expr::ColumnarValue::Scalar(
                        ScalarValue::Utf8(None),
                    ))
                }),
            )),
            vec![col("s")],
        ))
    }

    /// Regression guard for #13726/#13868 on the `DuckDB` route.
    ///
    /// The denied *names* `build` freezes are a snapshot, and providers are
    /// built before the spicepod's `functions:` entries register — so a
    /// function registered afterwards (a hot reload, a tool-backed SQL UDF) is
    /// absent from it. `build` therefore also installs a check that reads the
    /// registry live, and that check is the only thing standing between a
    /// late-registered function and a remote asked to evaluate a function it
    /// does not have.
    ///
    /// `FunctionSupport::with_scalar_call_support` **replaces** that check
    /// rather than composing with it, so supplying a backend's per-call gate
    /// after `build` silently discards it. The gate must go through
    /// [`FunctionSupportBuilder::scalar_call`] instead, which `build` composes
    /// with its own. This test registers the function *after* taking the
    /// support, which is the only ordering that can tell the two apart.
    #[test]
    fn a_late_registered_user_function_does_not_federate_to_duckdb() {
        let support = deny_spice_functions_for_duckdb_table_providers();

        // Registered after the snapshot was frozen, so only the live check can
        // refuse it.
        let name = "late_user_fn_duckdb_function_support";
        add_user_function(name);

        let federates =
            !contains_unsupported_functions(&plan_projecting(user_call(name)), &support)
                .expect("the support check must not error");

        remove_user_function(name);

        assert!(
            !federates,
            "a user function registered after the provider was built must not be pushed into DuckDB"
        );
    }

    /// The `DuckLake` catalog route's accessor must carry the per-call gate
    /// itself, because its one consumer must not add one: applying a gate to
    /// this value with `FunctionSupport::with_scalar_call_support` would replace
    /// the live user-function check `build` installs, which is the #13726/#13868
    /// regression. Asserting the gate is already here is what makes that call
    /// site's setter unnecessary rather than merely discouraged.
    ///
    /// `regexp_replace` is the probe because no name-based layer withholds it —
    /// it is neither a Spice function nor one of [`DUCKDB_DENIED_BUILTINS`] — so
    /// only a per-call gate can refuse the `U` flag, which `DuckDB` has no
    /// rendering for (#13900).
    #[test]
    fn the_ducklake_accessor_carries_the_per_call_gate() {
        let support = deny_spice_functions_for_duckdb_dialect_without_carve_out();

        // `contains_unsupported_functions` is true exactly when the plan holds a
        // call the backend must not be given, so refusing is the true case.
        assert!(
            contains_unsupported_functions(
                &plan_projecting(regexp_replace(col("s"), lit("a"), lit("X"), Some(lit("U")))),
                &support,
            )
            .expect("the support check must not error"),
            "the DuckLake accessor must refuse a call the DuckDB dialect cannot render, so its \
             call site does not have to install a gate that would discard the live \
             user-function check"
        );

        assert!(
            !contains_unsupported_functions(
                &plan_projecting(regexp_replace(col("s"), lit("a"), lit("X"), None)),
                &support,
            )
            .expect("the support check must not error"),
            "the gate must not cost a pushdown DuckDB can render"
        );
    }

    fn decimal_scan() -> datafusion::logical_expr::LogicalPlanBuilder {
        let schema = Schema::new(vec![
            Field::new("g", DataType::Int32, true),
            Field::new("dec", DataType::Decimal128(15, 2), true),
            Field::new("i", DataType::Int64, true),
            Field::new("f", DataType::Float64, true),
        ]);
        table_scan(Some("t"), &schema, None).expect("scan t")
    }

    /// `SELECT g, <aggregate> FROM t GROUP BY g`.
    fn plan_aggregating(aggregate: Expr) -> LogicalPlan {
        decimal_scan()
            .aggregate(vec![col("g")], vec![aggregate])
            .expect("aggregate")
            .build()
            .expect("build plan")
    }

    /// `SELECT <aggregate>(<column>) OVER (PARTITION BY g) FROM t`.
    fn plan_windowing(
        aggregate: Arc<datafusion::logical_expr::AggregateUDF>,
        column: &str,
    ) -> LogicalPlan {
        use datafusion::logical_expr::{
            ExprFunctionExt as _, WindowFunctionDefinition, expr::WindowFunction,
        };
        let window = Expr::from(WindowFunction::new(
            WindowFunctionDefinition::AggregateUDF(aggregate),
            vec![col(column)],
        ))
        .partition_by(vec![col("g")])
        .build()
        .expect("window expression");
        decimal_scan()
            .window(vec![window])
            .expect("window")
            .build()
            .expect("build plan")
    }

    fn pushes(plan: &LogicalPlan, support: &FunctionSupport) -> bool {
        !contains_unsupported_functions(plan, support).expect("the support check must not error")
    }

    /// Regression test for #14492: `SQLite` computes a decimal `avg` and `sum`
    /// in floating point or 64-bit integers, so neither federates; the same
    /// aggregates over integers and floats keep their pushdown.
    #[test]
    fn sqlite_keeps_decimal_avg_and_sum_local() {
        use datafusion::functions_aggregate::average::avg_udaf;
        use datafusion::functions_aggregate::expr_fn::{avg, count, max, min, sum};
        use datafusion::functions_aggregate::sum::sum_udaf;
        let support = deny_spice_functions_for_sqlite_table_providers();
        for refused in [avg(col("dec")), sum(col("dec"))] {
            assert!(
                !pushes(&plan_aggregating(refused.clone()), &support),
                "{refused} must stay local on SQLite"
            );
        }
        for refused in [avg_udaf(), sum_udaf()] {
            assert!(
                !pushes(&plan_windowing(Arc::clone(&refused), "dec"), &support),
                "a decimal windowed {} must stay local on SQLite",
                refused.name()
            );
        }
        assert!(
            pushes(&plan_windowing(avg_udaf(), "f"), &support),
            "a float windowed avg must keep its SQLite pushdown"
        );
        for allowed in [
            avg(col("i")),
            avg(col("f")),
            sum(col("i")),
            sum(col("f")),
            min(col("dec")),
            max(col("dec")),
            count(col("dec")),
        ] {
            assert!(
                pushes(&plan_aggregating(allowed.clone()), &support),
                "{allowed} must keep its SQLite pushdown"
            );
        }
    }

    /// Regression test for #14492: `DuckDB`'s decimal `avg` is a `DOUBLE`, so
    /// it stays local, on the connector, the accelerator and `DuckLake`; its
    /// exact decimal `sum` and a non-decimal `avg` keep their pushdown.
    #[test]
    fn duckdb_keeps_decimal_avg_local() {
        use datafusion::functions_aggregate::average::avg_udaf;
        use datafusion::functions_aggregate::expr_fn::{avg, sum};
        for (route, support) in [
            ("duckdb", deny_spice_functions_for_duckdb_table_providers()),
            (
                "ducklake",
                deny_spice_functions_for_duckdb_dialect_without_carve_out(),
            ),
        ] {
            assert!(
                !pushes(&plan_aggregating(avg(col("dec"))), &support),
                "a decimal avg must stay local on {route}"
            );
            assert!(
                !pushes(&plan_windowing(avg_udaf(), "dec"), &support),
                "a decimal windowed avg must stay local on {route}"
            );
            for allowed in [sum(col("dec")), avg(col("i")), avg(col("f"))] {
                assert!(
                    pushes(&plan_aggregating(allowed.clone()), &support),
                    "{allowed} must keep its {route} pushdown"
                );
            }
        }
    }

    /// An aggregate whose operand type cannot be read is refused, not assumed
    /// to be a non-decimal.
    #[test]
    fn a_decimal_aggregate_check_refuses_an_unreadable_operand() {
        use datafusion::functions_aggregate::expr_fn::{avg, sum};
        assert!(!super::sqlite_can_evaluate_expression(
            &avg(col("dec")),
            None
        ));
        assert!(!super::sqlite_can_evaluate_expression(
            &sum(col("dec")),
            None
        ));
        assert!(!super::duckdb_can_evaluate_expression(
            &avg(col("dec")),
            None
        ));
        assert!(super::duckdb_can_evaluate_expression(
            &sum(col("dec")),
            None
        ));
    }

    /// A scan of `t(id, s, a)` with `s` text and `a` binary, filtered by
    /// `predicate` and projecting `projection` — the shapes #14355 and #14397
    /// measured.
    fn plan_over_binary(predicate: Option<Expr>, projection: Expr) -> LogicalPlan {
        plan_over(
            vec![
                Field::new("id", DataType::Int64, true),
                Field::new("s", DataType::Utf8, true),
                Field::new("a", DataType::Binary, true),
            ],
            predicate,
            projection,
        )
    }

    /// A scan of `t` with `fields`, filtered by `predicate` and projecting
    /// `projection`.
    fn plan_over(fields: Vec<Field>, predicate: Option<Expr>, projection: Expr) -> LogicalPlan {
        let mut plan = table_scan(Some("t"), &Schema::new(fields), None).expect("scan t");
        if let Some(predicate) = predicate {
            plan = plan.filter(predicate).expect("filter");
        }
        plan.project(vec![projection])
            .expect("project")
            .build()
            .expect("build plan")
    }

    /// Asserts, through both `DuckDB` accessors, that every plan in `local`
    /// stays local and every plan in `federated` still federates.
    fn assert_duckdb_federation(local: &[LogicalPlan], federated: &[LogicalPlan]) {
        for (accessor, support) in [
            (
                "table providers",
                deny_spice_functions_for_duckdb_table_providers(),
            ),
            (
                "DuckLake catalog",
                deny_spice_functions_for_duckdb_dialect_without_carve_out(),
            ),
        ] {
            for plan in local {
                assert!(
                    contains_unsupported_functions(plan, &support)
                        .expect("the support check must not error"),
                    "the {accessor} accessor must keep this plan local:\n{plan}"
                );
            }
            for plan in federated {
                assert!(
                    !contains_unsupported_functions(plan, &support)
                        .expect("the support check must not error"),
                    "the {accessor} accessor must still federate:\n{plan}"
                );
            }
        }
    }

    /// Regression test for #14397, through both `DuckDB` accessors: no cast into
    /// a binary type has a `DuckDB` rendering that answers what `DataFusion`
    /// does, so a plan holding one, in a projection or a filter, stays local.
    #[test]
    fn a_duckdb_cast_into_binary_is_not_federated() {
        assert_duckdb_federation(
            &[
                plan_over_binary(None, cast(col("s"), DataType::Binary)),
                plan_over_binary(None, try_cast(col("s"), DataType::Binary)),
                plan_over_binary(
                    Some(cast(col("s"), DataType::Binary).eq(col("a"))),
                    col("id"),
                ),
                // A literal cast is sent as the bare string, which `DuckDB`
                // reads under its own escape rules.
                plan_over_binary(
                    Some(col("a").eq(cast(lit("\\xFF"), DataType::Binary))),
                    col("id"),
                ),
            ],
            // Casts into other types, and the binary column itself, still
            // federate: the refusal costs only the casts it is about.
            &[
                plan_over_binary(None, col("a")),
                plan_over_binary(None, cast(col("s"), DataType::Utf8View)),
                plan_over_binary(Some(col("a").is_not_null()), col("id")),
            ],
        );
    }

    /// Regression test for #14355, through both `DuckDB` accessors: a cast of a
    /// binary column into text answers with a row on `DuckDB` where
    /// `DataFusion` raises (`CAST`) or answers NULL (`TRY_CAST`), so a plan
    /// holding one, in a projection or a filter, must stay local.
    #[test]
    fn a_duckdb_text_cast_over_a_binary_column_is_not_federated() {
        assert_duckdb_federation(
            &[
                plan_over_binary(None, cast(col("a"), DataType::Utf8)),
                plan_over_binary(None, try_cast(col("a"), DataType::Utf8)),
                plan_over_binary(None, cast(col("a"), DataType::Utf8View)),
                plan_over_binary(
                    Some(cast(col("a"), DataType::Utf8).like(lit("%bad%"))),
                    col("id"),
                ),
            ],
            // The binary column itself, and a text cast over a non-binary
            // column, still federate: the refusal costs only the casts it is
            // about.
            &[
                plan_over_binary(None, col("a")),
                plan_over_binary(None, cast(col("id"), DataType::Utf8)),
                plan_over_binary(Some(col("a").is_not_null()), col("id")),
            ],
        );
    }

    /// A scan of `t(id, f, d)` with `f` a float and `d` a decimal, filtered by
    /// `predicate` and projecting `projection` — the shapes #14482 measured.
    fn plan_over_fractions(predicate: Option<Expr>, projection: Expr) -> LogicalPlan {
        plan_over(
            vec![
                Field::new("id", DataType::Int64, true),
                Field::new("f", DataType::Float64, true),
                Field::new("d", DataType::Decimal128(10, 2), true),
            ],
            predicate,
            projection,
        )
    }

    /// Regression test for #14482, through every policy whose engine rounds a
    /// fractional value cast into an integer where `DataFusion` truncates: a
    /// plan holding such a cast, in a projection or a filter, must stay local
    /// on `DuckDB` (both accessors), `PostgreSQL` and `MySQL`. The same policies
    /// decide the scan-level filter pushdown, so a filter over the cast is kept
    /// out of the pushed-down scan too rather than pre-applied by the engine
    /// with its own rounding.
    #[test]
    fn a_fractional_to_integer_cast_is_not_federated_to_an_engine_that_rounds() {
        for (policy, support) in [
            (
                "DuckDB table providers",
                deny_spice_functions_for_duckdb_table_providers(),
            ),
            (
                "DuckLake catalog",
                deny_spice_functions_for_duckdb_dialect_without_carve_out(),
            ),
            (
                "PostgreSQL",
                deny_spice_functions_for_postgres_table_providers(),
            ),
            ("MySQL", deny_spice_functions_for_mysql_table_providers()),
            (
                "BigQuery",
                deny_spice_functions_for_bigquery_table_providers(),
            ),
        ] {
            for plan in [
                plan_over_fractions(None, cast(col("f"), DataType::Int32)),
                plan_over_fractions(None, try_cast(col("f"), DataType::Int64)),
                plan_over_fractions(None, cast(col("d"), DataType::Int16)),
                plan_over_fractions(None, cast(col("id") * lit(1.5_f64), DataType::Int32)),
                plan_over_fractions(
                    Some(cast(col("id") * lit(1.5_f64), DataType::Int32).eq(lit(2))),
                    col("id"),
                ),
                plan_over_fractions(
                    Some(try_cast(col("d"), DataType::Int64).gt(lit(1))),
                    col("id"),
                ),
            ] {
                assert!(
                    contains_unsupported_functions(&plan, &support)
                        .expect("the support check must not error"),
                    "the {policy} policy must keep this plan local:\n{plan}"
                );
            }

            // The fractional columns themselves, a cast between integers, a
            // cast into a fractional type and a comparison over a fraction
            // still federate: the refusal costs only the casts it is about.
            for plan in [
                plan_over_fractions(None, col("f")),
                plan_over_fractions(None, col("d")),
                plan_over_fractions(None, cast(col("id"), DataType::Int32)),
                plan_over_fractions(None, cast(col("id"), DataType::Float64)),
                plan_over_fractions(None, cast(col("f"), DataType::Float32)),
                plan_over_fractions(Some(col("f").gt(lit(1.5_f64))), col("id")),
            ] {
                assert!(
                    !contains_unsupported_functions(&plan, &support)
                        .expect("the support check must not error"),
                    "the {policy} policy must still federate:\n{plan}"
                );
            }
        }
    }

    /// Regression test for #14482 on the generic-driver routes: the gate an ADBC
    /// driver name or an ODBC profile resolves to must refuse the same cast the
    /// engine's own connector refuses, and an engine that truncates like
    /// `DataFusion`, or one with no gate, keeps the plain policy.
    #[test]
    fn the_engine_gate_refuses_a_fractional_to_integer_cast_where_the_engine_rounds() {
        use datafusion::prelude::cast;
        let rounding = cast(lit(1.5_f64), DataType::Int64);
        let harmless = cast(lit(1_i64), DataType::Int32);
        for engine in [
            "bigquery",
            "duckdb",
            "postgres",
            "postgresql",
            "mysql",
            "snowflake",
            "athena",
        ] {
            let gate = expression_support_for_engine(engine)
                .unwrap_or_else(|| panic!("{engine} rounds the cast, so it needs a gate"));
            assert!(
                !gate(&rounding, None),
                "{engine} rounds {rounding}, so its gate must keep it local"
            );
            assert!(
                gate(&harmless, None),
                "{engine} evaluates {harmless} as DataFusion does, so its gate must let it federate"
            );
        }
        for engine in ["databricks", "flightsql", "unknown"] {
            assert!(
                expression_support_for_engine(engine).is_none(),
                "{engine} has no gate: it truncates like DataFusion, or no policy exists for it"
            );
        }
    }

    /// Regression test for #14753 and #14754: through a generic driver,
    /// `SQLite` reads a date or timestamp literal as the number its year
    /// parses to and cannot parse an interval, so every temporal value stays
    /// local — including the canonical date literal the `SQLite` connector's
    /// own dialect renders faithfully — while the plain comparisons around
    /// them keep federating.
    #[test]
    fn the_sqlite_driver_gate_keeps_every_temporal_value_local() {
        let gate = expression_support_for_engine("sqlite")
            .expect("SQLite reached through a driver needs a gate");
        let timestamp_literal = cast(
            lit("2026-01-30 23:00:00"),
            DataType::Timestamp(TimeUnit::Nanosecond, None),
        );
        let date_literal = cast(lit("2026-01-31"), DataType::Date32);
        let refused = [
            ("a string cast into a timestamp (#14753)", timestamp_literal),
            ("a canonical string cast into a date", date_literal),
            (
                "a timestamp value",
                lit(ScalarValue::TimestampNanosecond(
                    Some(1_769_814_000_000_000_000),
                    None,
                )),
            ),
            ("a date value", lit(ScalarValue::Date32(Some(20_484)))),
            (
                "an interval (#14754)",
                lit(ScalarValue::new_interval_mdn(0, 0, 3_600_000_000_000)),
            ),
            ("a TRY_CAST", try_cast(col("v"), DataType::Int64)),
        ];
        let schema = DFSchema::try_from(Schema::new(vec![
            Field::new("v", DataType::Utf8, true),
            Field::new("n", DataType::Int32, true),
        ]))
        .expect("build the test schema");
        for (what, expr) in refused {
            assert!(
                !gate(&expr, Some(&schema)),
                "{what} must stay local: {expr}"
            );
        }
        for expr in [
            col("v"),
            lit("2026-01-31"),
            lit(1_i64),
            cast(col("n"), DataType::Int64),
        ] {
            assert!(gate(&expr, Some(&schema)), "{expr} must still federate");
        }
    }

    /// The local half of #14482, pinned so the refusal above cannot outlive
    /// the divergence it exists for: `DataFusion` truncates a fractional value
    /// cast into an integer — toward zero for a float, and by integer division
    /// for a decimal — where the engines above round it.
    #[tokio::test]
    async fn datafusion_truncates_a_fractional_value_cast_into_an_integer() {
        let ctx = datafusion::prelude::SessionContext::new();
        let batches = ctx
            .sql(
                "SELECT CAST(1.5 AS INT) AS a, CAST(-1.5 AS INT) AS b, CAST(2.5 AS BIGINT) AS c, \
                 TRY_CAST(2.49 AS SMALLINT) AS d, CAST(CAST(2.49 AS DECIMAL(4, 2)) AS INT) AS e, \
                 CAST(CAST(-2.5 AS DECIMAL(4, 1)) AS INT) AS f",
            )
            .await
            .expect("the casts plan")
            .collect()
            .await
            .expect("the casts run");
        datafusion::assert_batches_eq!(
            [
                "+---+----+---+---+---+----+",
                "| a | b  | c | d | e | f  |",
                "+---+----+---+---+---+----+",
                "| 1 | -1 | 2 | 2 | 2 | -2 |",
                "+---+----+---+---+---+----+",
            ],
            &batches
        );
    }

    /// Regression test for #13887, through the `BigQuery` policy: a plan
    /// casting text into a nanosecond timestamp, in a projection or a filter,
    /// stays local, while the same cast into a microsecond timestamp — the
    /// precision `BigQuery`'s own `TIMESTAMP` holds — federates as before.
    #[test]
    fn a_text_cast_into_a_nanosecond_timestamp_is_not_federated_to_bigquery() {
        use arrow::datatypes::TimeUnit;
        use datafusion::prelude::cast;
        let support = deny_spice_functions_for_bigquery_table_providers();
        let text_and_id = || {
            vec![
                Field::new("s", DataType::Utf8, true),
                Field::new("id", DataType::Int64, true),
            ]
        };
        let nanos = DataType::Timestamp(TimeUnit::Nanosecond, None);
        let micros = DataType::Timestamp(TimeUnit::Microsecond, None);
        let nanosecond_instant = lit(ScalarValue::TimestampNanosecond(
            Some(1_768_473_000_390_436_170),
            None,
        ));
        let microsecond_instant = lit(ScalarValue::TimestampMicrosecond(
            Some(1_768_473_000_390_436),
            None,
        ));
        for plan in [
            plan_projecting(cast(col("s"), nanos.clone())),
            plan_over(
                text_and_id(),
                Some(cast(col("s"), nanos).gt(nanosecond_instant)),
                col("id"),
            ),
        ] {
            assert!(
                contains_unsupported_functions(&plan, &support)
                    .expect("the support check must not error"),
                "the BigQuery policy must keep this plan local:\n{plan}"
            );
        }
        for plan in [
            plan_projecting(cast(col("s"), micros.clone())),
            plan_over(
                text_and_id(),
                Some(cast(col("s"), micros).gt(microsecond_instant)),
                col("id"),
            ),
        ] {
            assert!(
                !contains_unsupported_functions(&plan, &support)
                    .expect("the support check must not error"),
                "the BigQuery policy must still federate:\n{plan}"
            );
        }
    }
}
