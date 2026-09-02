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
use runtime_udfs_api::{
    FunctionSupportBuilder, datafusion_nested_function_names,
    deny_spice_specific_functions_excluding,
};

/// The [`FunctionSupport`] for `DuckDB` connectors and accelerators: allows
/// every function the `DuckDB` dialect can rewrite into native SQL (e.g.
/// `cosine_distance` → `array_cosine_distance`, `rand` → `random()`), derived
/// from the dialect so it tracks it automatically.
#[must_use]
pub fn deny_spice_functions_for_duckdb() -> Arc<FunctionSupport> {
    deny_spice_specific_functions_excluding(&crate::dialect::duckdb_native_function_names())
}

/// `DuckDB` deny-list as a value, for
/// `DuckDBTableFactory::with_function_support`. See issue #10703.
#[must_use]
pub fn deny_spice_functions_for_duckdb_table_providers() -> FunctionSupport {
    FunctionSupportBuilder::new()
        .native(&crate::dialect::duckdb_native_function_names())
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
#[must_use]
pub fn deny_spice_functions_for_sqlite_table_providers() -> FunctionSupport {
    FunctionSupportBuilder::new()
        .deny_also([crate::dialect::BTRIM_NAME.to_string()])
        .build()
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
#[must_use]
pub fn deny_spice_functions_for_mysql_table_providers() -> FunctionSupport {
    FunctionSupportBuilder::new()
        .deny_also([crate::dialect::BTRIM_NAME.to_string()])
        .build()
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
