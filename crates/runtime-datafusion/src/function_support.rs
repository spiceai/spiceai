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

use datafusion_table_providers::util::supported_functions::FunctionSupport;
use runtime_udfs_api::{FunctionSupportBuilder, datafusion_nested_function_names};

/// The [`FunctionSupport`] for `DuckDB` connectors and accelerators: allows
/// every function the `DuckDB` dialect can rewrite into native SQL (e.g.
/// `cosine_distance` → `array_cosine_distance`, `rand` → `random()`), derived
/// from the dialect so it tracks it automatically.
///
/// On top of that carve-out, `regexp_match` is denied outright — see
/// [`DUCKDB_DENIED_BUILTINS`] for why.
#[must_use]
pub fn deny_spice_functions_for_duckdb() -> Arc<FunctionSupport> {
    Arc::new(duckdb_function_support())
}

/// `DuckDB` deny-list as a value, for
/// `DuckDBTableFactory::with_function_support`. See issue #10703.
#[must_use]
pub fn deny_spice_functions_for_duckdb_table_providers() -> FunctionSupport {
    duckdb_function_support()
}

/// The `DataFusion` built-ins `DuckDB` must not be handed, despite having a
/// function that looks like the one asked for.
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
/// `regexp_count` is here because its translation is not value-preserving
/// either, on a narrower input: the dialect renders it
/// `len(regexp_extract_all(x, p))`, and `regexp_extract_all(NULL, p)` is NULL in
/// `DuckDB`, so `len(NULL)` is NULL where `DataFusion` counts zero matches and
/// answers `0`. A count that is NULL rather than `0` propagates differently
/// through `SUM`, through `= 0`, and through a `WHERE` built on it, so an
/// accelerated dataset gained or lost rows against an unaccelerated one
/// (issue #13870). The dialect keeps its handler — the rewrite is right for
/// non-NULL input and #13870 is about making it NULL-preserving so the pushdown
/// can come back — but a denied name is never advertised as native, which
/// [`crate::dialect::duckdb_native_function_names`] enforces.
///
/// `regexp_like` and `regexp_replace` are the two `DataFusion` regexp built-ins
/// left, and both agreed with local evaluation on every input measured,
/// including a NULL one.
pub const DUCKDB_DENIED_BUILTINS: &[&str] = &[
    crate::dialect::REGEXP_MATCH_NAME,
    crate::dialect::REGEXP_INSTR_NAME,
    crate::dialect::REGEXP_COUNT_NAME,
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
#[must_use]
pub fn deny_spice_functions_for_duckdb_dialect_without_carve_out() -> FunctionSupport {
    FunctionSupportBuilder::new()
        .deny_also(DUCKDB_DENIED_BUILTINS.iter().map(|n| (*n).to_string()))
        .build()
}

/// The one `DuckDB` policy both public accessors return, so the connector and
/// the accelerator cannot be given different pushdown rules.
fn duckdb_function_support() -> FunctionSupport {
    FunctionSupportBuilder::new()
        .native(&crate::dialect::duckdb_native_function_names())
        .deny_also(DUCKDB_DENIED_BUILTINS.iter().map(|n| (*n).to_string()))
        .build()
}

/// The [`FunctionSupport`] for `BigQuery` over ADBC, as a value for
/// `AdbcTableFactory::with_function_support`.
///
/// Three layers, all derived from [`crate::dialect`] so they cannot drift from
/// what the dialect can actually render:
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
/// 3. `regexp_match` is denied outright. `BigQuery` has no function of that
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
        .build()
        .with_scalar_call_support(Arc::new(crate::dialect::bigquery_can_translate))
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
#[must_use]
pub fn deny_spice_functions_for_postgres_table_providers() -> FunctionSupport {
    let unsupported_arrays = datafusion_nested_function_names()
        .iter()
        .filter(|name| !POSTGRES_PUSHABLE_ARRAY_FUNCTIONS.contains(&name.as_str()))
        .cloned();
    FunctionSupportBuilder::new()
        .deny_also(unsupported_arrays)
        .build()
}
