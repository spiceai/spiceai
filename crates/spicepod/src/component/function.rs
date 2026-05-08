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

use std::collections::HashMap;

use crate::metric::Metrics;

use super::{Nameable, WithDependsOn};
#[cfg(feature = "schemars")]
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// A user-defined function registered into the `DataFusion` session context.
///
/// The `from` field selects the execution tier:
///   * `sql` - inline SQL body (tier T0, in-process, no sandbox).
///   * `http://...` | `https://...` - remote endpoint invoked over HTTP + JSON (tier T2).
///   * `wasm` - WebAssembly module invoked with Arrow IPC batches.
///
/// Supported at runtime by default: `sql`. HTTP-backed functions require a
/// runtime built with the `http-functions` feature. WebAssembly functions
/// require a runtime built with the `wasm-functions` feature.
///
/// Registration is disabled by default. Set `runtime.functions.enabled: true`
/// in the spicepod to activate declared functions.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "schemars", derive(JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct Function {
    /// The identifier the function is registered under in the `DataFusion`
    /// session context and referenced by in SQL queries.
    pub name: String,

    /// Source URI selecting the execution tier (e.g. `sql`, `http://host/path`).
    /// See [`Function`] docs for the full scheme list.
    pub from: String,

    /// Whether this function should be registered. Defaults to `true`.
    ///
    /// Set to `false` to keep the declaration in the spicepod without making
    /// it callable through SQL, tool exposure, `list_udfs()`, or
    /// `/v1/functions`.
    #[serde(default = "crate::component::default_true")]
    pub enabled: bool,

    /// Free-form description surfaced in `list_udfs()` and the
    /// `/v1/functions` HTTP endpoint.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,

    /// Defaults to [`FunctionKind::Scalar`].
    #[serde(default)]
    pub kind: FunctionKind,

    /// Volatility class — governs caching, acceleration pushdown, and
    /// whether the optimizer may evaluate the function at plan time.
    /// Defaults to [`Volatility::Volatile`] — the safe choice.
    #[serde(default)]
    pub volatility: Volatility,

    /// Typed argument list and return type.
    pub signature: Signature,

    /// Inline function body. Mutually exclusive with [`body_ref`].
    /// Exactly one of the two must be set when `from: sql`; neither may be
    /// set for non-SQL `from:` schemes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub body: Option<String>,

    /// Reference to a local filesystem file whose contents are the function
    /// body. Path is resolved relative to the runtime's current working
    /// directory. `body_ref` is not read from object stores when a spicepod is
    /// loaded remotely; use inline [`body`] for portable remote spicepods.
    /// Lets authors keep non-trivial SQL in its own file with proper editor
    /// support instead of embedding it inline.
    ///
    /// Mutually exclusive with [`body`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub body_ref: Option<String>,

    /// Free-form metadata surfaced alongside the function definition.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub metadata: HashMap<String, Value>,

    /// Tier-specific parameters (e.g. transport settings for remote tiers).
    /// Supports `${ secrets:KEY }` / `${ env:KEY }` interpolation at registration time.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub params: HashMap<String, Value>,

    /// Names of other spicepod components that must be loaded before this
    /// function (e.g. a dataset the function queries internally).
    #[serde(default, rename = "dependsOn", skip_serializing_if = "Vec::is_empty")]
    pub depends_on: Vec<String>,

    /// Metrics configuration for this function.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metrics: Option<Metrics>,

    /// Whether this scalar function is also exposed as an LLM tool. Defaults
    /// to `true` — scalar functions automatically become callable both via SQL
    /// (`SELECT my_fn(x)`) and via the tool registry (LLM tool-calling,
    /// `POST /v1/tools/<name>`, `/v1/tools` listing). Table functions are
    /// always SQL-only.
    ///
    /// Set to `false` to keep the function SQL-only.
    #[serde(default = "crate::component::default_true")]
    pub as_tool: bool,
}

/// Function kind.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[cfg_attr(feature = "schemars", derive(JsonSchema))]
#[serde(rename_all = "lowercase")]
pub enum FunctionKind {
    #[default]
    Scalar,
    Table,
}

impl FunctionKind {
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            FunctionKind::Scalar => "scalar",
            FunctionKind::Table => "table",
        }
    }
}

/// Function volatility — mirrors [`datafusion_expr::Volatility`].
///
/// The default is [`Volatility::Volatile`] because it is the safest:
/// volatile functions are never constant-folded, never cached, and never
/// pushed across distributed executors without pinning. Authors opt into
/// stronger guarantees explicitly by declaring `immutable` or `stable`.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[cfg_attr(feature = "schemars", derive(JsonSchema))]
#[serde(rename_all = "lowercase")]
pub enum Volatility {
    /// Same inputs always yield the same output (e.g. `abs`, `upper`). Safe
    /// to constant-fold and cache.
    Immutable,
    /// Stable within a single query but may change across queries (e.g.
    /// `now()`). Safe to cache per query.
    Stable,
    /// Unpredictable on every call (e.g. `random()`). Never cached.
    #[default]
    Volatile,
}

/// Typed function signature.
///
/// `returns` names the output Arrow type for scalar functions, or the
/// output columns for table functions.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "schemars", derive(JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct Signature {
    /// Table inputs consumed by function backends that accept relational input.
    /// When a function accepts table and scalar arguments, table inputs
    /// are the leading arguments and scalar arguments follow them.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tables: Vec<FunctionTableArg>,

    /// Positional scalar argument list. Empty for niladic functions.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub args: Vec<FunctionArg>,

    /// Return Arrow type for scalar functions, or output columns for table functions.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub returns: Option<FunctionReturns>,
}

/// A declared table input for function backends.
///
/// The `name` is the logical input name exposed to the backend. `columns`
/// declares the Arrow schema expected by the backend. The actual input data can
/// come from a backend-specific parameter, such as `params.input_table`, or from
/// a SQL `body`/`body_ref` query that produces the declared columns.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "schemars", derive(JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct FunctionTableArg {
    pub name: String,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub columns: Vec<FunctionArg>,
}

impl Signature {
    #[must_use]
    pub fn scalar_return_type(&self) -> Option<&str> {
        match self.returns.as_ref()? {
            FunctionReturns::Scalar(arrow_type) => Some(arrow_type),
            FunctionReturns::Table(_) => None,
        }
    }

    #[must_use]
    pub fn table_return_columns(&self) -> Option<&[FunctionArg]> {
        match self.returns.as_ref()? {
            FunctionReturns::Scalar(_) => None,
            FunctionReturns::Table(columns) => Some(columns),
        }
    }
}

/// Function return declaration.
///
/// Scalar functions use a single Arrow type string:
///
/// ```yaml
/// returns: int64
/// ```
///
/// Table functions use a list of named output columns:
///
/// ```yaml
/// returns:
///   - { name: value, type: int64 }
///   - { name: label, type: utf8 }
/// ```
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "schemars", derive(JsonSchema))]
#[serde(untagged)]
pub enum FunctionReturns {
    Scalar(String),
    Table(Vec<FunctionArg>),
}

/// A single named argument or output column.
///
/// The `type` field is an Arrow logical-type string (e.g. `float64`,
/// `utf8`, `list<int32>`, `decimal(38, 10)`, `timestamp(us, utc)`). The
/// parser retains it verbatim; tier-specific function factories validate
/// accepted type strings when the function is registered.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "schemars", derive(JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct FunctionArg {
    pub name: String,
    #[serde(rename = "type")]
    pub arrow_type: String,
}

impl Nameable for Function {
    fn name(&self) -> &str {
        &self.name
    }
}

impl WithDependsOn<Function> for Function {
    fn depends_on(&self, depends_on: &[String]) -> Function {
        Function {
            name: self.name.clone(),
            from: self.from.clone(),
            enabled: self.enabled,
            description: self.description.clone(),
            kind: self.kind,
            volatility: self.volatility,
            signature: self.signature.clone(),
            body: self.body.clone(),
            body_ref: self.body_ref.clone(),
            metadata: self.metadata.clone(),
            params: self.params.clone(),
            depends_on: depends_on.to_vec(),
            metrics: self.metrics.clone(),
            as_tool: self.as_tool,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_minimal_sql_scalar_function() {
        let src = r#"
            name: haversine_km
            from: sql
            signature:
              args:
                - { name: lat1, type: float64 }
                - { name: lon1, type: float64 }
                - { name: lat2, type: float64 }
                - { name: lon2, type: float64 }
              returns: float64
            body: "6371 * acos(cos(radians(lat1)) * cos(radians(lat2)) * cos(radians(lon2) - radians(lon1)) + sin(radians(lat1)) * sin(radians(lat2)))"
            volatility: immutable
        "#;
        let f: Function = yaml::from_str(src).expect("parses");
        assert_eq!(f.name, "haversine_km");
        assert_eq!(f.from, "sql");
        assert!(f.enabled);
        assert_eq!(f.kind, FunctionKind::Scalar);
        assert_eq!(f.volatility, Volatility::Immutable);
        assert_eq!(f.signature.args.len(), 4);
        assert_eq!(f.signature.args[0].name, "lat1");
        assert_eq!(f.signature.args[0].arrow_type, "float64");
        assert_eq!(f.signature.scalar_return_type(), Some("float64"));
        assert!(f.body.is_some());
    }

    #[test]
    fn parse_sql_table_function() {
        let src = r"
name: split_lines
from: sql
kind: table
signature:
    args: [{ name: doc, type: utf8 }]
    returns:
      - { name: line, type: utf8 }
      - { name: line_no, type: int64 }
body: SELECT doc AS line, 1 AS line_no FROM args
";
        let f = yaml::from_str::<Function>(src).expect("table functions parse");
        assert_eq!(f.kind, FunctionKind::Table);
        let columns = f
            .signature
            .table_return_columns()
            .expect("table return columns");
        assert_eq!(columns.len(), 2);
        assert_eq!(columns[0].name, "line");
        assert_eq!(columns[0].arrow_type, "utf8");
    }

    #[test]
    fn defaults_are_safe() {
        let src = r#"
            name: f
            from: sql
            signature:
              args: []
              returns: int64
            body: "42"
        "#;
        let f: Function = yaml::from_str(src).expect("parses");
        assert_eq!(f.kind, FunctionKind::Scalar);
        assert!(f.enabled);
        assert_eq!(f.volatility, Volatility::Volatile);
    }

    #[test]
    fn can_disable_function() {
        let src = r#"
            name: f
            from: sql
            enabled: false
            signature:
              args: []
              returns: int64
            body: "42"
        "#;
        let f: Function = yaml::from_str(src).expect("parses");
        assert!(!f.enabled);
    }

    #[test]
    fn rejects_unknown_field() {
        let src = r#"
            name: f
            from: sql
            signature: { args: [], returns: int64 }
            body: "1"
            bogus: true
        "#;
        let err = yaml::from_str::<Function>(src).expect_err("should reject unknown field");
        assert!(err.to_string().contains("bogus"));
    }

    #[test]
    fn depends_on_replaces_existing() {
        let f = Function {
            name: "f".into(),
            from: "sql".into(),
            enabled: true,
            description: None,
            kind: FunctionKind::Scalar,
            volatility: Volatility::Immutable,
            signature: Signature {
                tables: vec![],
                args: vec![],
                returns: Some(FunctionReturns::Scalar("int64".into())),
            },
            body: Some("1".into()),
            body_ref: None,
            metadata: HashMap::new(),
            params: HashMap::new(),
            depends_on: vec!["a".into()],
            metrics: None,
            as_tool: true,
        };
        let g = f.depends_on(&["b".into(), "c".into()]);
        assert_eq!(g.depends_on, vec!["b".to_string(), "c".to_string()]);
        assert_eq!(g.name, "f");
        assert!(g.enabled);
    }

    #[test]
    fn parse_function_with_body_ref() {
        let src = r"
            name: complex_fn
            from: sql
            signature:
              args: [{ name: x, type: int64 }]
              returns: int64
            body_ref: ./funcs/complex.sql
        ";
        let f: Function = yaml::from_str(src).expect("parses");
        assert!(f.body.is_none());
        assert_eq!(f.body_ref.as_deref(), Some("./funcs/complex.sql"));
    }
}
