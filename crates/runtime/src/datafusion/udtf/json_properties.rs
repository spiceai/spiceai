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

//! `flatten_json_properties` UDTF — M1 skeleton.
//!
//! Walks a JSON-Schema-shaped document's `properties` tree and emits one row per
//! field. This is the first milestone: `properties` recursion only. Arrays
//! (`items.properties`), `$ref`, `allOf` / `oneOf` / `anyOf`, `additionalProperties`
//! maps, options struct, cycle detection, and limits are M2.
//!
//! See `docs/PRINCIPLES.md` and GitHub issue #10399 for the full spec.
//!
//! ```text
//! flatten_json_properties(input Utf8) -> TABLE(
//!     path         Utf8,
//!     parent_path  Utf8,
//!     name         Utf8,
//!     description  Utf8,
//!     type         Utf8,
//!     required     Boolean,
//!     format       Utf8,
//!     enum_values  List<Utf8>,
//!     metadata     Utf8
//! )
//! ```
//!
//! Semantics in M1:
//! - Input is a JSON object with a top-level `properties` key.
//! - Output is one row per property at any depth reachable through nested
//!   `properties` (descends into objects).
//! - `required` is true when the ancestor whose `properties` contains this
//!   field lists the field name in its `required` array.
//! - Malformed input yields zero rows — never fails the query.

use std::collections::HashSet;
use std::fmt::{Debug, Formatter};
use std::sync::{Arc, LazyLock};

use arrow::array::{ArrayRef, BooleanBuilder, ListBuilder, StringBuilder};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::{Session, TableFunctionImpl, TableProvider};
use datafusion::common::Result as DataFusionResult;
use datafusion::datasource::TableType;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::scalar::ScalarValue;
use datafusion_datasource::memory::MemorySourceConfig;
use datafusion_datasource::source::DataSourceExec;
use serde_json::Value;

pub const FLATTEN_JSON_PROPERTIES_UDTF_NAME: &str = "flatten_json_properties";

/// Maximum recursion depth while walking `properties`. Matches the spec's
/// `max_depth` default; the configurable option and full cycle-detection story
/// land in M2.
const M1_MAX_DEPTH: usize = 32;

static OUTPUT_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    let enum_item = Arc::new(Field::new("item", DataType::Utf8, true));
    Arc::new(Schema::new(vec![
        Field::new("path", DataType::Utf8, false),
        Field::new("parent_path", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("description", DataType::Utf8, true),
        Field::new("type", DataType::Utf8, false),
        Field::new("required", DataType::Boolean, false),
        Field::new("format", DataType::Utf8, true),
        Field::new("enum_values", DataType::List(enum_item), true),
        Field::new("metadata", DataType::Utf8, true),
    ]))
});

/// One row of the flattened output. Public within the crate so tests and future
/// milestones (e.g. cycle-detection, options) can assert on structured output
/// without going through the Arrow batch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PropertyRow {
    pub path: String,
    pub parent_path: String,
    pub name: String,
    pub description: Option<String>,
    pub type_name: String,
    pub required: bool,
    pub format: Option<String>,
    pub enum_values: Option<Vec<String>>,
    pub metadata: Option<String>,
}

#[derive(Clone, Default)]
pub struct FlattenJsonPropertiesTableFunc;

impl FlattenJsonPropertiesTableFunc {
    #[must_use]
    pub fn new() -> Self {
        Self
    }
}

impl Debug for FlattenJsonPropertiesTableFunc {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FlattenJsonPropertiesTableFunc").finish()
    }
}

impl TableFunctionImpl for FlattenJsonPropertiesTableFunc {
    fn call(&self, exprs: &[Expr]) -> DataFusionResult<Arc<dyn TableProvider>> {
        let input = parse_input_arg(exprs)?;
        let rows = input.as_deref().map(flatten).unwrap_or_default();
        Ok(Arc::new(FlattenJsonPropertiesTable {
            schema: Arc::clone(&OUTPUT_SCHEMA),
            rows,
        }))
    }
}

fn parse_input_arg(exprs: &[Expr]) -> DataFusionResult<Option<String>> {
    let Some(first) = exprs.first() else {
        return Err(DataFusionError::Plan(format!(
            "{FLATTEN_JSON_PROPERTIES_UDTF_NAME}() requires a JSON string argument."
        )));
    };
    match first {
        Expr::Literal(ScalarValue::Utf8(v) | ScalarValue::LargeUtf8(v), _) => Ok(v.clone()),
        Expr::Literal(ScalarValue::Null, _) => Ok(None),
        other => Err(DataFusionError::NotImplemented(format!(
            "{FLATTEN_JSON_PROPERTIES_UDTF_NAME}() currently supports only literal JSON string arguments. Per-row LATERAL invocation with a column reference will land in a later milestone. Got: {other:?}."
        ))),
    }
}

/// Walk a JSON-Schema-shaped document and return one [`PropertyRow`] per field
/// reachable via nested `properties`. Returns an empty `Vec` for any input that
/// cannot be parsed or does not expose a `properties` object — this matches the
/// spec's "malformed input yields zero rows" guarantee.
#[must_use]
pub fn flatten(input: &str) -> Vec<PropertyRow> {
    let Ok(root) = serde_json::from_str::<Value>(input) else {
        return Vec::new();
    };
    let mut rows = Vec::new();
    walk_properties(&root, "", 0, &mut rows);
    rows
}

fn walk_properties(node: &Value, parent_path: &str, depth: usize, rows: &mut Vec<PropertyRow>) {
    if depth > M1_MAX_DEPTH {
        return;
    }
    let Some(properties) = node.get("properties").and_then(Value::as_object) else {
        return;
    };
    let required_set: HashSet<&str> = node
        .get("required")
        .and_then(Value::as_array)
        .map(|arr| arr.iter().filter_map(Value::as_str).collect())
        .unwrap_or_default();

    for (name, spec) in properties {
        let path = if parent_path.is_empty() {
            name.clone()
        } else {
            format!("{parent_path}.{name}")
        };
        let type_name = infer_type(spec);
        let description = spec
            .get("description")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned);
        let format = spec
            .get("format")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned);
        let enum_values = spec.get("enum").and_then(Value::as_array).map(|arr| {
            arr.iter()
                .map(|v| match v {
                    Value::String(s) => s.clone(),
                    _ => v.to_string(),
                })
                .collect()
        });

        rows.push(PropertyRow {
            path: path.clone(),
            parent_path: parent_path.to_owned(),
            name: name.clone(),
            description,
            type_name: type_name.clone(),
            required: required_set.contains(name.as_str()),
            format,
            enum_values,
            metadata: Some(spec.to_string()),
        });

        if type_name == "object" {
            walk_properties(spec, &path, depth + 1, rows);
        }
    }
}

fn infer_type(spec: &Value) -> String {
    match spec.get("type") {
        Some(Value::String(s)) => s.clone(),
        Some(Value::Array(arr)) => arr
            .iter()
            .find_map(Value::as_str)
            .unwrap_or("unknown")
            .to_owned(),
        _ => {
            if spec.get("properties").is_some() {
                "object".to_owned()
            } else if spec.get("items").is_some() {
                "array".to_owned()
            } else if let Some(first_enum) = spec
                .get("enum")
                .and_then(Value::as_array)
                .and_then(|a| a.first())
            {
                match first_enum {
                    Value::String(_) => "string",
                    Value::Bool(_) => "boolean",
                    Value::Number(n) if n.is_i64() || n.is_u64() => "integer",
                    Value::Number(_) => "number",
                    Value::Null => "null",
                    _ => "unknown",
                }
                .to_owned()
            } else {
                "unknown".to_owned()
            }
        }
    }
}

#[derive(Debug)]
pub struct FlattenJsonPropertiesTable {
    schema: SchemaRef,
    rows: Vec<PropertyRow>,
}

#[async_trait]
impl TableProvider for FlattenJsonPropertiesTable {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let batch = rows_to_batch(&self.rows, Arc::clone(&self.schema))?;
        let memory_source =
            MemorySourceConfig::try_new(&[vec![batch]], Arc::clone(&self.schema), None)?;
        Ok(Arc::new(DataSourceExec::new(Arc::new(memory_source))))
    }
}

fn rows_to_batch(rows: &[PropertyRow], schema: SchemaRef) -> DataFusionResult<RecordBatch> {
    let mut path = StringBuilder::with_capacity(rows.len(), rows.len() * 16);
    let mut parent_path = StringBuilder::with_capacity(rows.len(), rows.len() * 8);
    let mut name = StringBuilder::with_capacity(rows.len(), rows.len() * 8);
    let mut description = StringBuilder::with_capacity(rows.len(), rows.len() * 32);
    let mut type_name = StringBuilder::with_capacity(rows.len(), rows.len() * 4);
    let mut required = BooleanBuilder::with_capacity(rows.len());
    let mut format = StringBuilder::with_capacity(rows.len(), 0);
    let mut metadata = StringBuilder::with_capacity(rows.len(), rows.len() * 64);
    let mut enum_values = ListBuilder::new(StringBuilder::new());

    for row in rows {
        path.append_value(&row.path);
        parent_path.append_value(&row.parent_path);
        name.append_value(&row.name);
        match &row.description {
            Some(v) => description.append_value(v),
            None => description.append_null(),
        }
        type_name.append_value(&row.type_name);
        required.append_value(row.required);
        match &row.format {
            Some(v) => format.append_value(v),
            None => format.append_null(),
        }
        match &row.metadata {
            Some(v) => metadata.append_value(v),
            None => metadata.append_null(),
        }
        match &row.enum_values {
            Some(vs) => {
                for v in vs {
                    enum_values.values().append_value(v);
                }
                enum_values.append(true);
            }
            None => enum_values.append(false),
        }
    }

    let columns: Vec<ArrayRef> = vec![
        Arc::new(path.finish()),
        Arc::new(parent_path.finish()),
        Arc::new(name.finish()),
        Arc::new(description.finish()),
        Arc::new(type_name.finish()),
        Arc::new(required.finish()),
        Arc::new(format.finish()),
        Arc::new(enum_values.finish()),
        Arc::new(metadata.finish()),
    ];

    RecordBatch::try_new(schema, columns)
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn by_path(rows: &[PropertyRow]) -> std::collections::HashMap<&str, &PropertyRow> {
        rows.iter().map(|r| (r.path.as_str(), r)).collect()
    }

    #[test]
    fn flat_primitives() {
        let json = r#"{
            "properties": {
                "name": {"type": "string", "description": "User's full name"},
                "age":  {"type": "integer"}
            },
            "required": ["name"]
        }"#;
        let rows = flatten(json);
        assert_eq!(rows.len(), 2);

        let by = by_path(&rows);
        let name = by["name"];
        assert_eq!(name.parent_path, "");
        assert_eq!(name.type_name, "string");
        assert!(name.required);
        assert_eq!(name.description.as_deref(), Some("User's full name"));

        let age = by["age"];
        assert_eq!(age.type_name, "integer");
        assert!(!age.required);
    }

    #[test]
    fn nested_two_levels_covers_containers_and_leaves() {
        let json = r#"{
            "properties": {
                "user": {
                    "type": "object",
                    "properties": {
                        "name": {"type": "string"},
                        "address": {
                            "type": "object",
                            "properties": {
                                "street": {"type": "string"}
                            }
                        }
                    }
                }
            }
        }"#;
        let rows = flatten(json);
        let paths: Vec<_> = rows.iter().map(|r| r.path.as_str()).collect();
        assert!(paths.contains(&"user"));
        assert!(paths.contains(&"user.name"));
        assert!(paths.contains(&"user.address"));
        assert!(paths.contains(&"user.address.street"));

        let by = by_path(&rows);
        assert_eq!(by["user.address.street"].parent_path, "user.address");
        assert_eq!(by["user.address"].type_name, "object");
        assert_eq!(by["user.address.street"].type_name, "string");
    }

    #[test]
    fn required_is_inherited_from_the_containing_required_array() {
        let json = r#"{
            "properties": {
                "outer": {
                    "type": "object",
                    "required": ["inner_req"],
                    "properties": {
                        "inner_req": {"type": "string"},
                        "inner_opt": {"type": "string"}
                    }
                }
            },
            "required": ["outer"]
        }"#;
        let rows = flatten(json);
        let by = by_path(&rows);
        assert!(by["outer"].required, "outer listed in root required");
        assert!(
            by["outer.inner_req"].required,
            "inner_req listed in outer.required"
        );
        assert!(
            !by["outer.inner_opt"].required,
            "inner_opt not listed in outer.required"
        );
    }

    #[test]
    fn enum_values_are_captured_as_strings() {
        let json = r#"{
            "properties": {
                "status": {"type": "string", "enum": ["active", "pending", "disabled"]}
            }
        }"#;
        let rows = flatten(json);
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].enum_values.as_deref().unwrap(),
            &[
                "active".to_string(),
                "pending".to_string(),
                "disabled".to_string(),
            ]
        );
    }

    #[test]
    fn format_is_captured() {
        let json = r#"{
            "properties": {
                "created_at": {"type": "string", "format": "date-time"}
            }
        }"#;
        let rows = flatten(json);
        assert_eq!(rows[0].format.as_deref(), Some("date-time"));
    }

    #[test]
    fn metadata_round_trips_the_field_spec() {
        let json = r#"{
            "properties": {
                "x": {"type": "integer", "x-custom": {"team": "platform"}}
            }
        }"#;
        let rows = flatten(json);
        let meta: serde_json::Value = serde_json::from_str(rows[0].metadata.as_ref().unwrap())
            .expect("metadata round-trips as JSON");
        assert_eq!(meta["x-custom"]["team"], "platform");
    }

    #[test]
    fn malformed_json_yields_zero_rows() {
        assert!(flatten("not json").is_empty());
        assert!(flatten("{broken").is_empty());
        assert!(flatten("").is_empty());
    }

    #[test]
    fn documents_without_properties_yield_zero_rows() {
        assert!(flatten(r#"{"foo": "bar"}"#).is_empty());
        assert!(flatten(r#"{"properties": {}}"#).is_empty());
        assert!(flatten(r#"[1, 2, 3]"#).is_empty());
    }

    #[test]
    fn pathological_deep_nesting_terminates_without_stack_overflow() {
        // Build a document nested just past `M1_MAX_DEPTH` so we hit the walker's
        // guard, while staying under serde_json's parse-side recursion limit (128).
        // Each wrapper adds two levels of JSON nesting (`properties`, then `p`).
        const NESTING: usize = M1_MAX_DEPTH + 5;
        let mut doc = String::from(r#"{"type":"string"}"#);
        for _ in 0..NESTING {
            doc = format!(r#"{{"type":"object","properties":{{"p":{doc}}}}}"#);
        }
        let root = format!(r#"{{"properties":{{"p":{doc}}}}}"#);
        let rows = flatten(&root);
        // The walk stops at `M1_MAX_DEPTH`; we assert only that it terminates and
        // emits a bounded number of rows.
        assert!(!rows.is_empty());
        assert!(rows.len() <= M1_MAX_DEPTH + 2);
    }

    #[test]
    fn type_inferred_when_declared_as_array_of_types() {
        let json = r#"{"properties": {"x": {"type": ["integer", "null"]}}}"#;
        let rows = flatten(json);
        assert_eq!(rows[0].type_name, "integer");
    }

    #[test]
    fn type_inferred_from_properties_or_items_when_type_missing() {
        let json = r#"{
            "properties": {
                "obj":  {"properties": {"leaf": {"type": "string"}}},
                "arr":  {"items": {"type": "string"}},
                "enm":  {"enum": ["a", "b"]}
            }
        }"#;
        let rows = flatten(json);
        let by = by_path(&rows);
        assert_eq!(by["obj"].type_name, "object");
        assert_eq!(by["arr"].type_name, "array");
        assert_eq!(by["enm"].type_name, "string");
        // We recursed into `obj.properties` because inferred type is object.
        assert!(by.contains_key("obj.leaf"));
    }

    #[tokio::test]
    async fn table_provider_emits_schema_and_batch() {
        use datafusion::prelude::SessionContext;
        let ctx = SessionContext::new();
        let func = FlattenJsonPropertiesTableFunc::new();
        let provider = func
            .call(&[Expr::Literal(
                ScalarValue::Utf8(Some(
                    r#"{"properties":{"a":{"type":"string"}}}"#.to_string(),
                )),
                None,
            )])
            .expect("call succeeds for literal");

        let schema = provider.schema();
        assert_eq!(schema.fields().len(), 9);
        assert_eq!(schema.field(0).name(), "path");

        let state = ctx.state();
        let plan = provider.scan(&state, None, &[], None).await.expect("scan");
        let results = datafusion::physical_plan::collect(plan, ctx.task_ctx())
            .await
            .expect("collect");
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].num_rows(), 1);
    }

    #[test]
    fn scalar_null_input_yields_zero_rows() {
        let func = FlattenJsonPropertiesTableFunc::new();
        let provider = func
            .call(&[Expr::Literal(ScalarValue::Null, None)])
            .expect("null is accepted");
        assert_eq!(provider.schema().fields().len(), 9);
    }

    #[test]
    fn non_literal_argument_is_rejected_in_m1() {
        use datafusion::common::Column;
        let func = FlattenJsonPropertiesTableFunc::new();
        let err = func
            .call(&[Expr::Column(Column::new_unqualified("body"))])
            .expect_err("per-row invocation not yet supported");
        assert!(
            err.to_string().contains("literal JSON string"),
            "error should explain the M1 limitation: {err}"
        );
    }
}
