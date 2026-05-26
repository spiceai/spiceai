/*
Copyright 2026 The Spice.ai OSS Authors

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

//! Factory and dispatch for user-defined functions declared in a
//! spicepod's `functions:` section.
//!
//! Today this module supports:
//!
//!   * **T0 SQL** via `from: sql` - inline SQL body compiled to a
//!     `DataFusion` expression or table query.
//!   * **T2 Remote** via `from: http://...` and `from: https://...` when the
//!     `http-functions` feature is enabled.
//!   * **WASM** via `from: wasm` when the `wasm-functions` feature is enabled,
//!     using Arrow IPC streams as the host/guest data ABI.
//!
//! Unsupported schemes are rejected at build time with
//! [`UserFunctionError::UnsupportedScheme`].

use std::collections::{BTreeSet, HashSet};
use std::sync::Arc;

use datafusion::catalog::TableFunctionImpl;
use datafusion::logical_expr::ScalarUDF;
use datafusion::sql::{
    TableReference,
    parser::{self, DFParser},
    sqlparser::{ast, dialect::PostgreSqlDialect},
};
use snafu::Snafu;
use spicepod::component::function::{Function, FunctionKind};

mod args_inliner;
mod arrow_type;
#[cfg(feature = "http-functions")]
pub mod remote;
pub mod sql;
#[cfg(feature = "wasm-functions")]
pub mod wasm;

/// What a factory produces once a [`Function`] declaration has been
/// compiled and validated.
///
/// Today only [`BuiltFunction::Scalar`] is produced; aggregate, window,
/// table, and higher-order variants will land alongside their tier-specific
/// factories. Marked `#[non_exhaustive]` so adding variants is not a breaking
/// change for downstream callers — they must already use a wildcard arm.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum BuiltFunction {
    Scalar(Arc<ScalarUDF>),
    Table(Arc<dyn TableFunctionImpl>),
}

/// Errors produced while building a user-defined function.
///
/// Every variant reports the function `name` so the caller (the runtime
/// startup path) can include it in the user-facing error message.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum UserFunctionError {
    #[snafu(display(
        "Failed to register function {name}: the `from` scheme '{scheme}' is unsupported. \
        Supported schemes in this build: {supported_schemes}. \
        See: https://spiceai.org/docs/reference/spicepod/functions"
    ))]
    UnsupportedScheme {
        name: String,
        scheme: String,
        supported_schemes: &'static str,
    },

    #[cfg(not(feature = "http-functions"))]
    #[snafu(display(
        "Failed to register function {name}: HTTP-backed user-defined functions require the `http-functions` feature. This build supports inline SQL functions only (`from: sql`)."
    ))]
    HttpFunctionsDisabled { name: String },

    #[cfg(not(feature = "wasm-functions"))]
    #[snafu(display(
        "Failed to register function {name}: WASM user-defined functions require the `wasm-functions` feature. This build supports inline SQL functions only (`from: sql`) unless other function features are enabled."
    ))]
    WasmFunctionsDisabled { name: String },

    #[snafu(display(
        "Failed to register function {name}: one of `body:` or `body_ref:` is required when `from: sql` but neither was provided."
    ))]
    MissingBody { name: String },

    #[snafu(display(
        "Failed to register function {name}: `body:` and `body_ref:` are mutually exclusive; provide exactly one."
    ))]
    ConflictingBody { name: String },

    #[snafu(display(
        "Failed to register function {name}: `body:` / `body_ref:` must not be set when using a non-SQL `from:` scheme."
    ))]
    UnexpectedBody { name: String },

    #[snafu(display(
        "Failed to register function {name}: failed to read body_ref '{path}': {source}"
    ))]
    BodyRefRead {
        name: String,
        path: String,
        source: std::io::Error,
    },

    #[snafu(display("Failed to register function {name}: {source}"))]
    Sql {
        name: String,
        source: sql::SqlBuildError,
    },

    #[cfg(feature = "http-functions")]
    #[snafu(display("Failed to register function {name}: {source}"))]
    Remote {
        name: String,
        source: remote::RemoteBuildError,
    },

    #[cfg(feature = "wasm-functions")]
    #[snafu(display("Failed to register function {name}: {source}"))]
    Wasm {
        name: String,
        source: wasm::WasmBuildError,
    },
}

pub type Result<T, E = UserFunctionError> = std::result::Result<T, E>;

#[must_use]
pub fn supported_schemes() -> &'static str {
    match (
        cfg!(feature = "http-functions"),
        cfg!(feature = "wasm-functions"),
    ) {
        (true, true) => "`sql`, `http://`, `https://`, `wasm`",
        (true, false) => "`sql`, `http://`, `https://`",
        (false, true) => "`sql`, `wasm`",
        (false, false) => "`sql`",
    }
}

/// Split `from:` into `(scheme, tail)` — e.g. `sql` → (`"sql"`, `""`),
/// `http://host/p` →
/// (`"http"`, `"//host/p"`). The scheme is lowercased for matching.
fn split_scheme(from: &str) -> (String, &str) {
    match from.find(':') {
        Some(i) => (from[..i].to_ascii_lowercase(), &from[i + 1..]),
        None => (from.to_ascii_lowercase(), ""),
    }
}

/// Build a single user-defined function declaration into its registered
/// form. Caller is responsible for inserting the result into the
/// `DataFusion` session context.
///
/// # Errors
///
/// Returns [`UserFunctionError`] when the `from:` scheme is unsupported,
/// the SQL body is missing for a SQL function, or the tier-specific factory
/// (`sql` / `remote`) returns a build error.
pub async fn build_function(decl: &Function) -> Result<BuiltFunction> {
    let (scheme, _tail) = split_scheme(&decl.from);

    match scheme.as_str() {
        "sql" => build_sql(decl).await,
        #[cfg(feature = "http-functions")]
        "http" | "https" => build_remote(decl),
        #[cfg(not(feature = "http-functions"))]
        "http" | "https" => HttpFunctionsDisabledSnafu {
            name: decl.name.clone(),
        }
        .fail(),
        #[cfg(feature = "wasm-functions")]
        "wasm" => build_wasm(decl).await,
        #[cfg(not(feature = "wasm-functions"))]
        "wasm" => WasmFunctionsDisabledSnafu {
            name: decl.name.clone(),
        }
        .fail(),
        other => UnsupportedSchemeSnafu {
            name: decl.name.clone(),
            scheme: other.to_string(),
            supported_schemes: supported_schemes(),
        }
        .fail(),
    }
}

#[cfg(feature = "wasm-functions")]
async fn build_wasm(decl: &Function) -> Result<BuiltFunction> {
    let input_sql = resolve_optional_body(decl).await?;
    match decl.kind {
        FunctionKind::Scalar => {
            let udf = wasm::build_scalar_udf(decl, input_sql)
                .await
                .map_err(|source| UserFunctionError::Wasm {
                    name: decl.name.clone(),
                    source,
                })?;
            Ok(BuiltFunction::Scalar(udf))
        }
        FunctionKind::Table => {
            let udtf = wasm::build_table_udtf(decl, input_sql)
                .await
                .map_err(|source| UserFunctionError::Wasm {
                    name: decl.name.clone(),
                    source,
                })?;
            Ok(BuiltFunction::Table(udtf))
        }
    }
}

#[cfg(feature = "http-functions")]
fn build_remote(decl: &Function) -> Result<BuiltFunction> {
    if decl.body.is_some() || decl.body_ref.is_some() {
        return UnexpectedBodySnafu {
            name: decl.name.clone(),
        }
        .fail();
    }
    match decl.kind {
        FunctionKind::Scalar => {
            let udf =
                remote::build_scalar_udf(decl).map_err(|source| UserFunctionError::Remote {
                    name: decl.name.clone(),
                    source,
                })?;
            Ok(BuiltFunction::Scalar(udf))
        }
        FunctionKind::Table => {
            let udtf =
                remote::build_table_udtf(decl).map_err(|source| UserFunctionError::Remote {
                    name: decl.name.clone(),
                    source,
                })?;
            Ok(BuiltFunction::Table(udtf))
        }
    }
}

async fn build_sql(decl: &Function) -> Result<BuiltFunction> {
    let body = resolve_body(decl).await?;
    match decl.kind {
        FunctionKind::Scalar => {
            let udf =
                sql::build_scalar_udf(decl, &body).map_err(|source| UserFunctionError::Sql {
                    name: decl.name.clone(),
                    source,
                })?;
            Ok(BuiltFunction::Scalar(udf))
        }
        FunctionKind::Table => {
            let udtf = sql::build_table_udtf(decl, &body).await.map_err(|source| {
                UserFunctionError::Sql {
                    name: decl.name.clone(),
                    source,
                }
            })?;
            Ok(BuiltFunction::Table(udtf))
        }
    }
}

/// Resolve the effective body for a SQL-tier function, reading from
/// [`Function::body_ref`] when set. Enforces the "exactly one of `body` /
/// `body_ref`" invariant.
async fn resolve_body(decl: &Function) -> Result<String> {
    match (&decl.body, &decl.body_ref) {
        (Some(_), Some(_)) => ConflictingBodySnafu {
            name: decl.name.clone(),
        }
        .fail(),
        (Some(s), None) => Ok(s.clone()),
        (None, Some(path)) => {
            tokio::fs::read_to_string(path)
                .await
                .map_err(|source| UserFunctionError::BodyRefRead {
                    name: decl.name.clone(),
                    path: path.clone(),
                    source,
                })
        }
        (None, None) => MissingBodySnafu {
            name: decl.name.clone(),
        }
        .fail(),
    }
}

async fn resolve_optional_body(decl: &Function) -> Result<Option<String>> {
    match (&decl.body, &decl.body_ref) {
        (Some(_), Some(_)) => ConflictingBodySnafu {
            name: decl.name.clone(),
        }
        .fail(),
        (Some(s), None) => Ok(Some(s.clone())),
        (None, Some(path)) => tokio::fs::read_to_string(path)
            .await
            .map(Some)
            .map_err(|source| UserFunctionError::BodyRefRead {
                name: decl.name.clone(),
                path: path.clone(),
                source,
            }),
        (None, None) => Ok(None),
    }
}

/// Build every function in `decls`, returning a vector of built
/// functions paired with their source declaration (for diagnostics) and
/// a vector of any per-function build errors. The caller decides whether
/// to fail the startup on partial failure or log and continue.
pub async fn build_all(
    decls: &[Function],
) -> (Vec<(Function, BuiltFunction)>, Vec<UserFunctionError>) {
    let mut built = Vec::with_capacity(decls.len());
    let mut errors = Vec::new();
    for decl in decls {
        let decl = match function_with_inferred_dependencies(decl).await {
            Ok(decl) => decl,
            Err(err) => {
                errors.push(err);
                continue;
            }
        };
        match build_function(&decl).await {
            Ok(f) => built.push((decl, f)),
            Err(e) => errors.push(e),
        }
    }
    (built, errors)
}

async fn function_with_inferred_dependencies(decl: &Function) -> Result<Function> {
    if !decl.depends_on.is_empty() {
        return Ok(decl.clone());
    }

    let dependencies = infer_dependencies(decl).await?;
    if dependencies.is_empty() {
        return Ok(decl.clone());
    }

    let mut decl = decl.clone();
    decl.depends_on = dependencies;
    Ok(decl)
}

async fn infer_dependencies(decl: &Function) -> Result<Vec<String>> {
    let (scheme, _) = split_scheme(&decl.from);
    let mut dependencies = Vec::new();

    match scheme.as_str() {
        "sql" => {
            if let Some(body) = resolve_optional_body(decl).await? {
                dependencies.extend(dependencies_from_sql(&body));
            }
        }
        "wasm" => {
            if let Some(body) = resolve_optional_body(decl).await? {
                dependencies.extend(dependencies_from_sql(&body));
            } else if let Some(table) = string_param(decl, "input_table") {
                dependencies.push(TableReference::parse_str(table));
            }
        }
        "http" | "https" => {
            if let Some(table) = string_param(decl, "input_table") {
                dependencies.push(TableReference::parse_str(table));
            }
        }
        _ => {}
    }

    let excluded_dependencies = inferred_dependency_exclusions(decl);
    dependencies.retain(|dependency| !excluded_dependencies.contains(dependency));

    Ok(deduplicate_dependencies(dependencies))
}

fn inferred_dependency_exclusions(decl: &Function) -> HashSet<TableReference> {
    let mut exclusions = HashSet::new();
    exclusions.insert(TableReference::bare("args"));
    exclusions.extend(
        decl.signature
            .tables
            .iter()
            .map(|table| TableReference::parse_str(&table.name)),
    );
    exclusions
}

fn string_param<'a>(decl: &'a Function, key: &str) -> Option<&'a str> {
    decl.params.get(key).and_then(serde_json::Value::as_str)
}

fn dependencies_from_sql(sql: &str) -> Vec<TableReference> {
    let Ok(statements) = DFParser::parse_sql_with_dialect(sql, &PostgreSqlDialect {}) else {
        return Vec::new();
    };
    if statements.len() != 1 {
        return Vec::new();
    }
    get_dependent_table_names(&statements[0])
}

fn deduplicate_dependencies(dependencies: Vec<TableReference>) -> Vec<String> {
    dependencies
        .into_iter()
        .map(|table| table.to_string())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn get_dependent_table_names(statement: &parser::Statement) -> Vec<TableReference> {
    let mut table_names = Vec::new();
    let mut cte_names = HashSet::new();

    if let parser::Statement::Statement(statement) = statement.clone()
        && let ast::Statement::Query(statement) = *statement
    {
        if let Some(with) = statement.with {
            for table in with.cte_tables {
                cte_names.insert(TableReference::bare(table.alias.name.to_string()));
                let cte_table_names = get_dependent_table_names(&parser::Statement::Statement(
                    Box::new(ast::Statement::Query(table.query)),
                ));
                table_names.extend(cte_table_names);
            }
        }
        table_names.extend(extract_tables_from_set_expr(&statement.body, &cte_names));
    }

    table_names
        .into_iter()
        .filter(|name| !cte_names.contains(name))
        .collect()
}

fn extract_tables_from_set_expr(
    expr: &ast::SetExpr,
    cte_names: &HashSet<TableReference>,
) -> Vec<TableReference> {
    match expr {
        ast::SetExpr::Select(select_statement) => {
            let mut table_names = vec![];
            for from in &select_statement.from {
                let mut relations = vec![from.relation.clone()];
                for join in &from.joins {
                    relations.push(join.relation.clone());
                }

                for relation in relations {
                    match relation {
                        ast::TableFactor::Table { name, .. } => {
                            let table_ref = TableReference::parse_str(&name.to_string());
                            if !cte_names.contains(&table_ref) {
                                table_names.push(table_ref);
                            }
                        }
                        ast::TableFactor::Derived { subquery, .. } => {
                            table_names.extend(get_dependent_table_names(
                                &parser::Statement::Statement(Box::new(ast::Statement::Query(
                                    subquery,
                                ))),
                            ));
                        }
                        _ => {}
                    }
                }
            }
            table_names
        }
        ast::SetExpr::SetOperation { left, right, .. } => {
            let mut table_names = extract_tables_from_set_expr(left, cte_names);
            table_names.extend(extract_tables_from_set_expr(right, cte_names));
            table_names
        }
        _ => vec![],
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn split_scheme_variants() {
        assert_eq!(split_scheme("sql"), ("sql".to_string(), ""));
        assert_eq!(
            split_scheme("HTTP://host/p"),
            ("http".to_string(), "//host/p")
        );
    }

    fn decl(from: &str, body: Option<&str>) -> Function {
        use spicepod::component::function::FunctionReturns;

        Function {
            name: "f".into(),
            from: from.into(),
            enabled: true,
            description: None,
            kind: spicepod::component::function::FunctionKind::Scalar,
            volatility: spicepod::component::function::Volatility::Immutable,
            signature: spicepod::component::function::Signature {
                tables: vec![],
                args: vec![spicepod::component::function::FunctionArg {
                    name: "x".into(),
                    arrow_type: "int64".into(),
                }],
                returns: Some(FunctionReturns::Scalar("int64".into())),
            },
            body: body.map(str::to_string),
            body_ref: None,
            metadata: HashMap::default(),
            params: HashMap::default(),
            depends_on: vec![],
            metrics: None,
            as_tool: true,
        }
    }

    #[tokio::test]
    async fn unsupported_scheme_rejected() {
        let d = decl("file:///tmp/f", None);
        let err = build_function(&d)
            .await
            .expect_err("file scheme unsupported in beta");
        let msg = err.to_string();
        assert!(msg.contains("unsupported"), "{msg}");
        assert!(msg.contains("file"), "{msg}");
    }

    #[tokio::test]
    async fn sql_missing_body_rejected() {
        let d = decl("sql", None);
        let err = build_function(&d).await.expect_err("sql without body");
        let msg = err.to_string();
        assert!(msg.contains("`body:` or `body_ref:`"), "{msg}");
    }

    #[tokio::test]
    async fn sql_conflicting_body_and_ref_rejected() {
        let mut d = decl("sql", Some("x"));
        d.body_ref = Some("./ignored.sql".into());
        let err = build_function(&d)
            .await
            .expect_err("both body and body_ref");
        assert!(err.to_string().contains("mutually exclusive"));
    }

    #[tokio::test]
    async fn sql_body_ref_reads_from_file() {
        // Write a tiny SQL body to a temp file and point body_ref at it.
        let tmp = std::env::temp_dir().join("spice_udf_body_ref_test.sql");
        std::fs::write(&tmp, "x + 1").expect("write tmp body");

        let mut d = decl("sql", None);
        d.body_ref = Some(tmp.to_string_lossy().into_owned());
        // Force known return type to avoid needing SQL planner in the test.
        let built = build_function(&d).await.expect("builds");
        match built {
            BuiltFunction::Scalar(udf) => {
                assert_eq!(udf.name(), "f");
            }
            BuiltFunction::Table(_) => panic!("expected scalar function"),
        }

        std::fs::remove_file(&tmp).ok();
    }

    #[tokio::test]
    async fn sql_body_ref_missing_file_surfaces_io_error() {
        let mut d = decl("sql", None);
        d.body_ref = Some("/nonexistent/path/to/body.sql".into());
        let err = build_function(&d).await.expect_err("missing file");
        let msg = err.to_string();
        assert!(msg.contains("body_ref"), "{msg}");
    }

    #[tokio::test]
    async fn infers_sql_dependencies_when_depends_on_is_missing() {
        let d = decl(
            "sql",
            Some(
                "WITH recent_orders AS (SELECT * FROM orders) SELECT * FROM recent_orders JOIN customers ON recent_orders.customer_id = customers.id",
            ),
        );

        let inferred = function_with_inferred_dependencies(&d)
            .await
            .expect("dependencies inferred");

        assert_eq!(inferred.depends_on, vec!["customers", "orders"]);
    }

    #[tokio::test]
    async fn sql_dependency_inference_excludes_function_inputs() {
        let mut d = decl(
            "sql",
            Some(
                "SELECT input.id, args.x FROM input JOIN real_source ON input.id = real_source.id CROSS JOIN args",
            ),
        );
        d.signature.tables = vec![spicepod::component::function::FunctionTableArg {
            name: "input".into(),
            columns: vec![],
        }];

        let dependencies = infer_dependencies(&d).await.expect("dependencies inferred");

        assert_eq!(dependencies, vec!["real_source"]);
    }

    #[tokio::test]
    async fn wasm_sql_dependency_inference_excludes_function_inputs() {
        let mut d = decl(
            "wasm",
            Some(
                "SELECT input.id, args.x FROM input JOIN wasm_source ON input.id = wasm_source.id CROSS JOIN args",
            ),
        );
        d.signature.tables = vec![spicepod::component::function::FunctionTableArg {
            name: "input".into(),
            columns: vec![],
        }];

        let dependencies = infer_dependencies(&d).await.expect("dependencies inferred");

        assert_eq!(dependencies, vec!["wasm_source"]);
    }

    #[tokio::test]
    async fn explicit_depends_on_overrides_inference() {
        let mut d = decl("sql", Some("SELECT * FROM orders"));
        d.depends_on = vec!["manual_dependency".into()];

        let inferred = function_with_inferred_dependencies(&d)
            .await
            .expect("explicit dependencies preserved");

        assert_eq!(inferred.depends_on, vec!["manual_dependency"]);
    }

    #[tokio::test]
    async fn infers_wasm_dependency_from_input_table_param() {
        let mut d = decl("wasm", None);
        d.params.insert(
            "input_table".into(),
            serde_json::Value::String("catalog.schema.orders".into()),
        );

        let inferred = function_with_inferred_dependencies(&d)
            .await
            .expect("wasm input table dependency inferred");

        assert_eq!(inferred.depends_on, vec!["catalog.schema.orders"]);
    }

    #[tokio::test]
    async fn infers_wasm_dependency_from_sql_body() {
        let d = decl("wasm", Some("SELECT * FROM wasm_source"));

        let inferred = function_with_inferred_dependencies(&d)
            .await
            .expect("wasm SQL body dependency inferred");

        assert_eq!(inferred.depends_on, vec!["wasm_source"]);
    }

    #[tokio::test]
    async fn infers_http_dependency_from_input_table_param() {
        let mut d = decl("https://example.com/function", None);
        d.params.insert(
            "input_table".into(),
            serde_json::Value::String("orders".into()),
        );

        let inferred = function_with_inferred_dependencies(&d)
            .await
            .expect("http input table dependency inferred");

        assert_eq!(inferred.depends_on, vec!["orders"]);
    }

    #[cfg(feature = "http-functions")]
    #[tokio::test]
    async fn non_sql_with_body_rejected() {
        let d = decl("http://example.com/f", Some("x + 1"));
        let err = build_function(&d)
            .await
            .expect_err("body forbidden on remote");
        assert!(err.to_string().contains("must not be set"));
    }

    #[cfg(not(feature = "http-functions"))]
    #[tokio::test]
    async fn http_scheme_rejected_without_feature() {
        let d = decl("http://example.com/f", None);
        let err = build_function(&d)
            .await
            .expect_err("http functions require feature");
        let msg = err.to_string();
        assert!(msg.contains("http-functions"), "{msg}");
        assert!(msg.contains("inline SQL"), "{msg}");
    }

    /// End-to-end: declare a remote UDF pointing at a local axum HTTP server,
    /// register it into `DataFusion`, project it with the `DataFrame` API,
    /// and verify the values round-tripped through JSON.
    #[cfg(feature = "http-functions")]
    #[tokio::test]
    async fn remote_udf_round_trips_via_dataframe_api() {
        use arrow::array::Int64Array;
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use axum::{Router, extract::Json as AxJson, routing::post};
        use datafusion::datasource::MemTable;
        use datafusion::prelude::{SessionContext, col};
        use serde_json::Value;
        use spicepod::component::function::{
            FunctionArg, FunctionKind, FunctionReturns, Signature as YamlSig, Volatility,
        };
        use std::sync::Arc;
        use tokio::net::TcpListener;

        // Start a tiny HTTP server that doubles every int64 row it receives.
        async fn handler(AxJson(body): AxJson<Value>) -> AxJson<Value> {
            let rows = body
                .get("rows")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default();
            let values: Vec<Value> = rows
                .into_iter()
                .map(|r| {
                    let x = r.get("x").and_then(Value::as_i64).expect("row has int64 x");
                    Value::Number((x * 2).into())
                })
                .collect();
            AxJson(serde_json::json!({ "values": values }))
        }

        let app = Router::new().route("/double", post(handler));
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        tokio::spawn(async move {
            axum::serve(listener, app).await.expect("serve");
        });

        // Declare + build the remote UDF.
        let mut params = HashMap::default();
        params.insert(
            "allowed_endpoint_ranges".into(),
            serde_json::Value::Array(vec![serde_json::Value::String("127.0.0.1/32".into())]),
        );
        let decl = Function {
            name: "remote_double".into(),
            from: format!("http://{addr}/double"),
            enabled: true,
            description: None,
            kind: FunctionKind::Scalar,
            volatility: Volatility::Volatile,
            signature: YamlSig {
                tables: vec![],
                args: vec![FunctionArg {
                    name: "x".into(),
                    arrow_type: "int64".into(),
                }],
                returns: Some(FunctionReturns::Scalar("int64".into())),
            },
            body: None,
            body_ref: None,
            metadata: HashMap::default(),
            params,
            depends_on: vec![],
            metrics: None,
            as_tool: true,
        };
        let built = build_function(&decl).await.expect("builds");

        let ctx = SessionContext::new();
        let udf = match built {
            BuiltFunction::Scalar(udf) => udf,
            BuiltFunction::Table(_) => panic!("expected scalar function"),
        };
        ctx.register_udf(udf.as_ref().clone());

        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, true)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3, 4]))],
        )
        .expect("batch");
        let table = MemTable::try_new(Arc::clone(&schema), vec![vec![batch]]).expect("memtable");
        ctx.register_table("t", Arc::new(table)).expect("register");

        let df = ctx
            .table("t")
            .await
            .expect("table exists")
            .sort_by(vec![col("x")])
            .expect("sorts")
            .select(vec![udf.call(vec![col("x")]).alias("y")])
            .expect("projects remote UDF");
        let results = df.collect().await.expect("runs");

        assert_eq!(results.len(), 1);
        let col = results[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("int64");
        assert_eq!(col.values(), &[2_i64, 4, 6, 8]);
    }

    /// End-to-end: a Function declaration with a valid SQL body builds into a
    /// `ScalarUDF` that evaluates correctly through the `DataFrame` API.
    #[tokio::test]
    async fn sql_udf_registered_and_queried_via_dataframe_api() {
        use arrow::array::Int64Array;
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use datafusion::datasource::MemTable;
        use datafusion::prelude::{SessionContext, col};
        use std::sync::Arc;

        let mut d = decl("sql", Some("x * 2"));
        d.name = "double_it".into();
        let built = build_function(&d).await.expect("builds");

        let ctx = SessionContext::new();
        let udf = match built {
            BuiltFunction::Scalar(udf) => udf,
            BuiltFunction::Table(_) => panic!("expected scalar function"),
        };
        ctx.register_udf(udf.as_ref().clone());

        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, true)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3, 4]))],
        )
        .expect("batch");
        let table = MemTable::try_new(Arc::clone(&schema), vec![vec![batch]]).expect("memtable");
        ctx.register_table("t", Arc::new(table)).expect("register");

        let df = ctx
            .table("t")
            .await
            .expect("table exists")
            .sort_by(vec![col("x")])
            .expect("sorts")
            .select(vec![udf.call(vec![col("x")]).alias("y")])
            .expect("projects SQL UDF");
        let results = df.collect().await.expect("runs");
        assert_eq!(results.len(), 1);
        let col = results[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("int64");
        assert_eq!(col.values(), &[2_i64, 4, 6, 8]);
    }
}
