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
//!   * **T0 SQL** via `from: sql` — inline SQL body compiled to a
//!     `DataFusion` expression.
//!   * **T2 Remote** via `from: http://…` and `from: https://…` —
//!     HTTP + JSON endpoint invoked through [`AsyncScalarUDFImpl`].
//!
//! On the roadmap:
//!
//!   * **T1 WASM** via `from: wasm:…` — WebAssembly component running
//!     under `wasmtime`.
//!   * Additional remote transports (`flight://…`, `grpc://…`) and
//!     UDAF / UDWF / UDTF kinds.
//!
//! Unsupported schemes are rejected at build time with
//! [`UserFunctionError::UnsupportedScheme`].

use std::sync::Arc;

use datafusion::logical_expr::ScalarUDF;
use snafu::Snafu;
use spicepod::component::function::{Function, FunctionKind};

pub mod remote;
pub mod sql;

/// What a factory produces once a [`Function`] declaration has been
/// compiled and validated.
///
/// Today only [`BuiltFunction::Scalar`] is produced; aggregate, window,
/// and table variants will land alongside their tier-specific factories.
#[derive(Clone, Debug)]
pub enum BuiltFunction {
    Scalar(Arc<ScalarUDF>),
}

/// Errors produced while building a user-defined function.
///
/// Every variant reports the function `name` so the caller (the runtime
/// startup path) can include it in the user-facing error message.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum UserFunctionError {
    #[snafu(display(
        "Failed to register function {name}: the `from` scheme '{scheme}' is not yet supported. \
        Supported schemes: `sql`, `http://`, `https://`. WASM (`wasm:...`) ships in a later phase. \
        See: https://spiceai.org/docs/reference/spicepod/functions"
    ))]
    UnsupportedScheme { name: String, scheme: String },

    #[snafu(display(
        "Failed to register function {name}: kind '{kind:?}' is not yet supported. \
        Only `kind: scalar` is supported today; aggregate, window, and table UDFs ship with later phases."
    ))]
    UnsupportedKind { name: String, kind: FunctionKind },

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

    #[snafu(display("Failed to register function {name}: {source}"))]
    Remote {
        name: String,
        source: remote::RemoteBuildError,
    },
}

pub type Result<T, E = UserFunctionError> = std::result::Result<T, E>;

/// Split `from:` into `(scheme, tail)` — e.g. `sql` → (`"sql"`, `""`),
/// `wasm:./x.wasm` → (`"wasm"`, `"./x.wasm"`), `http://host/p` →
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
/// the [`FunctionKind`] is not yet implemented, the SQL body is missing
/// for a SQL function, or the tier-specific factory (`sql` / `remote`)
/// returns a build error.
pub fn build_function(decl: &Function) -> Result<BuiltFunction> {
    let (scheme, _tail) = split_scheme(&decl.from);

    match scheme.as_str() {
        "sql" => build_sql(decl),
        "http" | "https" => build_remote(decl),
        other => UnsupportedSchemeSnafu {
            name: decl.name.clone(),
            scheme: other.to_string(),
        }
        .fail(),
    }
}

fn build_remote(decl: &Function) -> Result<BuiltFunction> {
    if decl.kind != FunctionKind::Scalar {
        return UnsupportedKindSnafu {
            name: decl.name.clone(),
            kind: decl.kind,
        }
        .fail();
    }
    if decl.body.is_some() || decl.body_ref.is_some() {
        return UnexpectedBodySnafu {
            name: decl.name.clone(),
        }
        .fail();
    }
    let udf = remote::build_scalar_udf(decl).map_err(|source| UserFunctionError::Remote {
        name: decl.name.clone(),
        source,
    })?;
    Ok(BuiltFunction::Scalar(udf))
}

fn build_sql(decl: &Function) -> Result<BuiltFunction> {
    if decl.kind != FunctionKind::Scalar {
        return UnsupportedKindSnafu {
            name: decl.name.clone(),
            kind: decl.kind,
        }
        .fail();
    }
    let body = resolve_body(decl)?;
    let udf = sql::build_scalar_udf(decl, &body).map_err(|source| UserFunctionError::Sql {
        name: decl.name.clone(),
        source,
    })?;
    Ok(BuiltFunction::Scalar(udf))
}

/// Resolve the effective body for a SQL-tier function, reading from
/// [`Function::body_ref`] when set. Enforces the "exactly one of `body` /
/// `body_ref`" invariant.
fn resolve_body(decl: &Function) -> Result<String> {
    match (&decl.body, &decl.body_ref) {
        (Some(_), Some(_)) => ConflictingBodySnafu {
            name: decl.name.clone(),
        }
        .fail(),
        (Some(s), None) => Ok(s.clone()),
        (None, Some(path)) => {
            std::fs::read_to_string(path).map_err(|source| UserFunctionError::BodyRefRead {
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

/// Build every function in `decls`, returning a vector of built
/// functions paired with their source declaration (for diagnostics) and
/// a vector of any per-function build errors. The caller decides whether
/// to fail the startup on partial failure or log and continue.
#[must_use]
pub fn build_all(decls: &[Function]) -> (Vec<(Function, BuiltFunction)>, Vec<UserFunctionError>) {
    let mut built = Vec::with_capacity(decls.len());
    let mut errors = Vec::new();
    for decl in decls {
        match build_function(decl) {
            Ok(f) => built.push((decl.clone(), f)),
            Err(e) => errors.push(e),
        }
    }
    (built, errors)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn split_scheme_variants() {
        assert_eq!(split_scheme("sql"), ("sql".to_string(), ""));
        assert_eq!(
            split_scheme("wasm:./x.wasm"),
            ("wasm".to_string(), "./x.wasm")
        );
        assert_eq!(
            split_scheme("HTTP://host/p"),
            ("http".to_string(), "//host/p")
        );
    }

    fn decl(from: &str, kind: FunctionKind, body: Option<&str>) -> Function {
        Function {
            name: "f".into(),
            from: from.into(),
            description: None,
            kind,
            volatility: spicepod::component::function::Volatility::Immutable,
            signature: spicepod::component::function::Signature {
                args: vec![spicepod::component::function::FunctionArg {
                    name: "x".into(),
                    arrow_type: "int64".into(),
                }],
                returns: Some("int64".into()),
                returns_schema: vec![],
                null_aware: false,
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

    #[test]
    fn unsupported_scheme_rejected() {
        let d = decl("wasm:./x.wasm", FunctionKind::Scalar, None);
        let err = build_function(&d).expect_err("wasm unsupported in phase 1");
        let msg = err.to_string();
        assert!(msg.contains("not yet supported"), "{msg}");
        assert!(msg.contains("wasm"), "{msg}");
    }

    #[test]
    fn unsupported_kind_rejected() {
        let d = decl("sql", FunctionKind::Aggregate, Some("sum(x)"));
        let err = build_function(&d).expect_err("aggregate unsupported in phase 1");
        assert!(err.to_string().contains("Aggregate"));
    }

    #[test]
    fn sql_missing_body_rejected() {
        let d = decl("sql", FunctionKind::Scalar, None);
        let err = build_function(&d).expect_err("sql without body");
        let msg = err.to_string();
        assert!(msg.contains("`body:` or `body_ref:`"), "{msg}");
    }

    #[test]
    fn sql_conflicting_body_and_ref_rejected() {
        let mut d = decl("sql", FunctionKind::Scalar, Some("x"));
        d.body_ref = Some("./ignored.sql".into());
        let err = build_function(&d).expect_err("both body and body_ref");
        assert!(err.to_string().contains("mutually exclusive"));
    }

    #[test]
    fn sql_body_ref_reads_from_file() {
        // Write a tiny SQL body to a temp file and point body_ref at it.
        let tmp = std::env::temp_dir().join("spice_udf_body_ref_test.sql");
        std::fs::write(&tmp, "x + 1").expect("write tmp body");

        let mut d = decl("sql", FunctionKind::Scalar, None);
        d.body_ref = Some(tmp.to_string_lossy().into_owned());
        // Force known return type to avoid needing SQL planner in the test.
        let built = build_function(&d).expect("builds");
        match built {
            BuiltFunction::Scalar(udf) => {
                assert_eq!(udf.name(), "f");
            }
        }

        std::fs::remove_file(&tmp).ok();
    }

    #[test]
    fn sql_body_ref_missing_file_surfaces_io_error() {
        let mut d = decl("sql", FunctionKind::Scalar, None);
        d.body_ref = Some("/nonexistent/path/to/body.sql".into());
        let err = build_function(&d).expect_err("missing file");
        let msg = err.to_string();
        assert!(msg.contains("body_ref"), "{msg}");
    }

    #[test]
    fn non_sql_with_body_rejected() {
        let d = decl("http://example.com/f", FunctionKind::Scalar, Some("x + 1"));
        let err = build_function(&d).expect_err("body forbidden on remote");
        assert!(err.to_string().contains("must not be set"));
    }

    /// End-to-end: declare a remote UDF pointing at a local axum HTTP server,
    /// register it into `DataFusion`, run `SELECT remote_double(x) FROM t`,
    /// and verify the values round-tripped through JSON.
    #[tokio::test]
    async fn remote_udf_round_trips_via_http_json() {
        use arrow::array::Int64Array;
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use axum::{Router, extract::Json as AxJson, routing::post};
        use datafusion::datasource::MemTable;
        use datafusion::prelude::SessionContext;
        use serde_json::Value;
        use spicepod::component::function::{
            FunctionArg, FunctionKind, Signature as YamlSig, Volatility,
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
        let decl = Function {
            name: "remote_double".into(),
            from: format!("http://{addr}/double"),
            description: None,
            kind: FunctionKind::Scalar,
            volatility: Volatility::Volatile,
            signature: YamlSig {
                args: vec![FunctionArg {
                    name: "x".into(),
                    arrow_type: "int64".into(),
                }],
                returns: Some("int64".into()),
                returns_schema: vec![],
                null_aware: false,
            },
            body: None,
            body_ref: None,
            metadata: HashMap::default(),
            params: HashMap::default(),
            depends_on: vec![],
            metrics: None,
            as_tool: true,
        };
        let built = build_function(&decl).expect("builds");

        let ctx = SessionContext::new();
        match built {
            BuiltFunction::Scalar(udf) => ctx.register_udf(udf.as_ref().clone()),
        }

        // MemTable with four rows, query through the SQL layer.
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, true)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3, 4]))],
        )
        .expect("batch");
        let table = MemTable::try_new(Arc::clone(&schema), vec![vec![batch]]).expect("memtable");
        ctx.register_table("t", Arc::new(table)).expect("register");

        let df = ctx
            .sql("SELECT remote_double(x) AS y FROM t ORDER BY x")
            .await
            .expect("sql compiles");
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
    /// `ScalarUDF` that evaluates correctly through `DataFusion`'s SQL layer.
    #[tokio::test]
    async fn sql_udf_registered_and_queried_via_sql() {
        use arrow::array::Int64Array;
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use datafusion::datasource::MemTable;
        use datafusion::prelude::SessionContext;
        use std::sync::Arc;

        let mut d = decl("sql", FunctionKind::Scalar, Some("x * 2"));
        d.name = "double_it".into();
        let built = build_function(&d).expect("builds");

        let ctx = SessionContext::new();
        match built {
            BuiltFunction::Scalar(udf) => {
                ctx.register_udf(udf.as_ref().clone());
            }
        }

        // Register a tiny table so we can SELECT double_it(col) FROM t.
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, true)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3, 4]))],
        )
        .expect("batch");
        let table = MemTable::try_new(Arc::clone(&schema), vec![vec![batch]]).expect("memtable");
        ctx.register_table("t", Arc::new(table)).expect("register");

        let df = ctx
            .sql("SELECT double_it(x) AS y FROM t ORDER BY x")
            .await
            .expect("sql compiles");
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
