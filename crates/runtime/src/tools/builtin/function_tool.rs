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

//! Adapter that re-exposes a user-defined SQL function as an LLM tool.
//!
//! A [`FunctionAsTool`] implements [`SpiceModelTool`] on top of a
//! previously-registered spicepod `functions:` entry. Tool invocation:
//!
//!   1. Parse the incoming JSON object into one scalar per declared arg.
//!   2. Build a `SELECT fn_name(arg0, arg1, …) AS result` query that
//!      embeds the arg values as typed SQL literals (integers, floats,
//!      strings, booleans). The literals are type-safe by construction
//!      — no free-form SQL from the caller is interpolated.
//!   3. Execute the query on the runtime's [`DataFusion`] session so
//!      both sync (T0 SQL) and async (T2 Remote) tiers dispatch
//!      transparently.
//!   4. Return the single-cell result as a JSON value.
//!
//! The JSON Schema surfaced via [`SpiceModelTool::parameters`] is
//! derived from the typed Arrow signature — primitive types map cleanly
//! to JSON types. Functions declaring unsupported types fail at tool
//! registration with a clear error, not at call time.

use std::borrow::Cow;
use std::sync::Weak;

use async_trait::async_trait;
use datafusion::arrow::array::{Array, BooleanArray, Float64Array, Int64Array, StringArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::utils::quote_identifier;
use futures::TryStreamExt;
use serde_json::{Map, Value};
use spicepod::component::function::{Function, FunctionArg, FunctionKind};
use tools::SpiceModelTool;

use runtime_datafusion::query_engine::{QueryEngine, QueryRequest};

/// Build a [`FunctionAsTool`] from a user-declared [`Function`]. Fails
/// when the function's signature references Arrow types the tool bridge
/// cannot encode into JSON.
///
/// # Errors
///
/// Returns [`FunctionToolBuildError`] when any arg or return type is not
/// JSON-encodable, or when `signature.returns` is missing.
pub fn build(
    decl: &Function,
    df: Weak<dyn QueryEngine>,
) -> Result<FunctionAsTool, FunctionToolBuildError> {
    if decl.kind != FunctionKind::Scalar {
        return Err(FunctionToolBuildError::UnsupportedFunctionKind {
            function: decl.name.clone(),
            kind: decl.kind.as_str().to_string(),
        });
    }
    for arg in &decl.signature.args {
        map_arrow_to_json(&arg.arrow_type).ok_or_else(|| {
            FunctionToolBuildError::UnsupportedArgType {
                function: decl.name.clone(),
                arg: arg.name.clone(),
                arrow_type: arg.arrow_type.clone(),
            }
        })?;
    }
    let Some(ret) = decl.signature.scalar_return_type() else {
        return Err(FunctionToolBuildError::MissingReturnType {
            function: decl.name.clone(),
        });
    };
    map_arrow_to_json(ret).ok_or_else(|| FunctionToolBuildError::UnsupportedReturnType {
        function: decl.name.clone(),
        arrow_type: ret.to_string(),
    })?;

    Ok(FunctionAsTool {
        name: decl.name.clone(),
        description: decl.description.clone(),
        args: decl.signature.args.clone(),
        df,
    })
}

/// Build the OpenAI-style JSON Schema `parameters` object from a typed
/// arg list. A closed `object` schema with all args required — matches
/// how Spice's built-in tools render via `tools::utils::parameters<T>`.
///
/// Unsupported arg types fall back to `"string"` so a mis-typed arg
/// doesn't panic — [`build`] validates types up-front, so this branch
/// is only reachable if an adapter is constructed via means other than
/// [`build`] (e.g. a future programmatic constructor).
fn build_parameters_schema(args: &[FunctionArg]) -> Value {
    let mut properties = Map::new();
    let mut required = Vec::with_capacity(args.len());
    for a in args {
        let json_ty = map_arrow_to_json(&a.arrow_type).unwrap_or("string");
        let mut prop = Map::new();
        prop.insert("type".to_string(), Value::String(json_ty.to_string()));
        properties.insert(a.name.clone(), Value::Object(prop));
        required.push(Value::String(a.name.clone()));
    }
    let mut schema = Map::new();
    schema.insert("type".to_string(), Value::String("object".to_string()));
    schema.insert("properties".to_string(), Value::Object(properties));
    schema.insert("required".to_string(), Value::Array(required));
    schema.insert("additionalProperties".to_string(), Value::Bool(false));
    Value::Object(schema)
}

/// A tool adapter that re-exposes a registered user-defined function
/// to the LLM tool registry. See module docs.
pub struct FunctionAsTool {
    name: String,
    description: Option<String>,
    args: Vec<FunctionArg>,
    df: Weak<dyn QueryEngine>,
}

impl std::fmt::Debug for FunctionAsTool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FunctionAsTool")
            .field("name", &self.name)
            .field("description", &self.description)
            .field("args", &self.args)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl SpiceModelTool for FunctionAsTool {
    fn name(&self) -> Cow<'_, str> {
        Cow::Borrowed(&self.name)
    }

    fn description(&self) -> Option<Cow<'_, str>> {
        self.description.as_deref().map(Cow::Borrowed)
    }

    fn parameters(&self) -> Option<Value> {
        Some(build_parameters_schema(&self.args))
    }

    async fn call(&self, arg: &str) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        let req: Value = serde_json::from_str(arg)?;
        let obj = req.as_object().ok_or_else(|| {
            Box::<dyn std::error::Error + Send + Sync>::from("expected a JSON object of arguments")
        })?;

        let mut literals = Vec::with_capacity(self.args.len());
        for a in &self.args {
            let v = obj.get(&a.name).ok_or_else(|| {
                Box::<dyn std::error::Error + Send + Sync>::from(format!(
                    "missing argument '{}'",
                    a.name
                ))
            })?;
            literals.push(json_to_sql_literal(v, &a.arrow_type)?);
        }

        let sql = function_call_sql(&self.name, &literals);

        let df = self.df.upgrade().ok_or_else(|| {
            Box::<dyn std::error::Error + Send + Sync>::from(
                "runtime DataFusion reference is no longer valid",
            )
        })?;

        let read_only = crate::http::v1::current_principal_requires_read_only().await;

        let batches = df
            .execute_query(QueryRequest::new(&sql).read_only(read_only))
            .await
            .map_err(|e| {
                Box::<dyn std::error::Error + Send + Sync>::from(format!("query failed: {e}"))
            })?
            .try_collect::<Vec<_>>()
            .await?;

        extract_single_cell_as_json(&batches)
    }
}

fn function_call_sql(function_name: &str, literals: &[String]) -> String {
    format!(
        "SELECT {}({}) AS result",
        quote_identifier(function_name),
        literals.join(", ")
    )
}

fn extract_single_cell_as_json(
    batches: &[datafusion::arrow::record_batch::RecordBatch],
) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
    let Some(batch) = batches.iter().find(|b| b.num_rows() > 0) else {
        return Ok(Value::Null);
    };
    let column = batch.column(0);
    if column.is_null(0) {
        return Ok(Value::Null);
    }
    let data_type = column.data_type();
    match data_type {
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
            let cast = datafusion::arrow::compute::cast(column, &DataType::Int64)?;
            let arr = cast
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or("cast to Int64 failed")?;
            Ok(Value::Number(arr.value(0).into()))
        }
        DataType::Float32 | DataType::Float64 => {
            let cast = datafusion::arrow::compute::cast(column, &DataType::Float64)?;
            let arr = cast
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or("cast to Float64 failed")?;
            serde_json::Number::from_f64(arr.value(0))
                .map(Value::Number)
                .ok_or_else(|| {
                    Box::<dyn std::error::Error + Send + Sync>::from(
                        "float result is NaN/Inf and cannot be represented in JSON",
                    )
                })
        }
        DataType::Utf8 | DataType::LargeUtf8 => {
            let cast = datafusion::arrow::compute::cast(column, &DataType::Utf8)?;
            let arr = cast
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or("cast to Utf8 failed")?;
            Ok(Value::String(arr.value(0).to_string()))
        }
        DataType::Boolean => {
            let arr = column
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or("downcast to Boolean failed")?;
            Ok(Value::Bool(arr.value(0)))
        }
        other => Err(Box::<dyn std::error::Error + Send + Sync>::from(format!(
            "function returned {other:?}, which is not yet JSON-encodable"
        ))),
    }
}

#[derive(Debug, snafu::Snafu)]
pub enum FunctionToolBuildError {
    #[snafu(display(
        "cannot expose function '{function}' as a tool: arg '{arg}' has Arrow type '{arrow_type}', which is not yet JSON-encodable. \
        Supported: signed integer aliases/widths (int, int8, int16, int32, int64), float aliases/widths (float, double, float32, float64), utf8/string, boolean/bool. \
        Set `as_tool: false` on the function to suppress."
    ))]
    UnsupportedArgType {
        function: String,
        arg: String,
        arrow_type: String,
    },
    #[snafu(display(
        "cannot expose function '{function}' as a tool: return Arrow type '{arrow_type}' is not yet JSON-encodable. \
        Supported: signed integer aliases/widths (int, int8, int16, int32, int64), float aliases/widths (float, double, float32, float64), utf8/string, boolean/bool. \
        Set `as_tool: false` to suppress."
    ))]
    UnsupportedReturnType {
        function: String,
        arrow_type: String,
    },
    #[snafu(display(
        "cannot expose function '{function}' as a tool: signature.returns is required for tool-bridged functions"
    ))]
    MissingReturnType { function: String },

    #[snafu(display(
        "cannot expose function '{function}' as a tool: function kind '{kind}' is not supported for tool exposure. Set `as_tool: false` on the function to suppress."
    ))]
    UnsupportedFunctionKind { function: String, kind: String },
}

/// Map an Arrow-type-string (as used in spicepod `signature.args`) to
/// the JSON Schema primitive type name, or None if unsupported for JSON.
fn map_arrow_to_json(arrow: &str) -> Option<&'static str> {
    match arrow.trim().to_ascii_lowercase().as_str() {
        "int8" | "int16" | "int32" | "int64" | "int" => Some("integer"),
        "float32" | "float64" | "float" | "double" => Some("number"),
        "utf8" | "string" => Some("string"),
        "boolean" | "bool" => Some("boolean"),
        _ => None,
    }
}

/// Turn a JSON value into a typed SQL literal fragment. Strings get
/// escaped with `'` → `''`; numbers and booleans are rendered directly.
/// Only primitive types supported; complex values return an error.
fn json_to_sql_literal(
    v: &Value,
    arrow_type: &str,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let expected = map_arrow_to_json(arrow_type)
        .ok_or_else(|| format!("unsupported arg type {arrow_type}"))?;
    match (v, expected) {
        (Value::Null, _) => Ok("NULL".to_string()),
        (Value::Bool(b), "boolean") => Ok(if *b { "TRUE" } else { "FALSE" }.to_string()),
        (Value::Number(n), "integer") => {
            let i = n.as_i64().ok_or_else(|| {
                format!("arg expected integer, got non-i64-representable number {n}")
            })?;
            Ok(i.to_string())
        }
        (Value::Number(n), "number") => {
            let fl = n.as_f64().ok_or_else(|| {
                format!("arg expected number, got non-f64-representable number {n}")
            })?;
            if !fl.is_finite() {
                return Err(Box::<dyn std::error::Error + Send + Sync>::from(
                    "function arguments must be finite (NaN/Inf are not valid in SQL)",
                ));
            }
            Ok(format!("CAST({fl} AS DOUBLE)"))
        }
        (Value::String(s), "string") => {
            let escaped = s.replace('\'', "''");
            Ok(format!("'{escaped}'"))
        }
        (other, expected) => Err(Box::<dyn std::error::Error + Send + Sync>::from(format!(
            "arg type mismatch: expected {expected}, got {other:?}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_for_two_args() {
        let args = vec![
            FunctionArg {
                name: "x".into(),
                arrow_type: "int64".into(),
            },
            FunctionArg {
                name: "name".into(),
                arrow_type: "utf8".into(),
            },
        ];
        let schema = build_parameters_schema(&args);
        let obj = schema.as_object().expect("object");
        assert_eq!(obj["type"], "object");
        assert_eq!(obj["additionalProperties"], false);
        let props = obj["properties"].as_object().expect("properties");
        assert_eq!(props["x"]["type"], "integer");
        assert_eq!(props["name"]["type"], "string");
        assert_eq!(obj["required"].as_array().expect("array").len(), 2);
    }

    #[test]
    fn json_to_sql_literal_escapes_strings() {
        let v = Value::String("it's fine".into());
        assert_eq!(json_to_sql_literal(&v, "utf8").expect("ok"), "'it''s fine'");
    }

    #[test]
    fn json_to_sql_literal_integer() {
        let v = Value::Number(42.into());
        assert_eq!(json_to_sql_literal(&v, "int64").expect("ok"), "42");
    }

    #[test]
    fn json_to_sql_literal_boolean() {
        assert_eq!(
            json_to_sql_literal(&Value::Bool(true), "boolean").expect("ok"),
            "TRUE"
        );
        assert_eq!(
            json_to_sql_literal(&Value::Bool(false), "boolean").expect("ok"),
            "FALSE"
        );
    }

    #[test]
    fn function_call_sql_quotes_function_name() {
        let sql = function_call_sql(
            "fn_name); SELECT secret FROM secrets; --",
            &["1".to_string()],
        );
        assert_eq!(
            sql,
            "SELECT \"fn_name); SELECT secret FROM secrets; --\"(1) AS result"
        );
    }

    #[test]
    fn map_arrow_to_json_coverage() {
        assert_eq!(map_arrow_to_json("int64"), Some("integer"));
        assert_eq!(map_arrow_to_json("uint64"), None);
        assert_eq!(map_arrow_to_json("UTF8"), Some("string"));
        assert_eq!(map_arrow_to_json("boolean"), Some("boolean"));
        assert_eq!(map_arrow_to_json("float64"), Some("number"));
        assert_eq!(map_arrow_to_json("decimal(10,2)"), None);
    }

    #[test]
    fn unsigned_integer_return_is_not_json_encoded() {
        use datafusion::arrow::array::UInt64Array;
        use datafusion::arrow::datatypes::{Field, Schema};
        use datafusion::arrow::record_batch::RecordBatch;
        use std::sync::Arc;

        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("x", DataType::UInt64, false)])),
            vec![Arc::new(UInt64Array::from(vec![u64::MAX]))],
        )
        .expect("batch");

        let err = extract_single_cell_as_json(&[batch]).expect_err("uint64 unsupported");
        assert!(err.to_string().contains("UInt64"), "{err}");
    }
}
