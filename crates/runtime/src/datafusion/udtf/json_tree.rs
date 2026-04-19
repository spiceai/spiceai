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

//! `json_tree` UDTF — recursive depth-first walk of an arbitrary JSON document.
//!
//! Schema-agnostic sibling to `flatten_json_properties`. Mirrors the well-known
//! DuckDB / SQLite `json_tree` table function: one row per node (interior and
//! leaf), in depth-first order, with JSON-Path addresses and a parent pointer.
//!
//! ```text
//! json_tree(input Utf8) -> TABLE(
//!     key       Utf8,        -- key within parent object; null for array elements and root
//!     value     Utf8,        -- JSON-encoded value of this node
//!     type      Utf8,        -- "object" | "array" | "string" | "integer" | "real" | "boolean" | "null"
//!     atom      Utf8,        -- scalar text at a leaf; null for interior nodes
//!     id        Int64,       -- unique id within this document (depth-first order)
//!     parent    Int64,       -- parent id; null for root
//!     fullkey   Utf8,        -- JSON-Path to this node, e.g. `$.a.b[2]`
//!     path      Utf8         -- JSON-Path to parent (fullkey minus the leaf step)
//! )
//! ```
//!
//! Design notes:
//! - Column names and semantics follow DuckDB
//!   (<https://duckdb.org/docs/current/data/json/json_functions.html>) so that
//!   existing recipes and user muscle memory port over. DuckDB also exposes
//!   `rowid`; we omit it as it duplicates `id` for single-document input.
//! - Ordering is deterministic: object members are emitted in insertion order
//!   (`serde_json::Map` preserves insertion order when the `preserve_order`
//!   feature is enabled; otherwise alphabetical — either is stable per input).
//! - Malformed input yields zero rows, matching the `flatten_json_properties`
//!   convention — never fails the query.

use std::fmt::{Debug, Formatter};
use std::sync::{Arc, LazyLock};

use arrow::array::{ArrayRef, Int64Builder, StringBuilder};
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

pub const JSON_TREE_UDTF_NAME: &str = "json_tree";

/// Guardrail on recursion depth. A follow-up milestone can expose this as an
/// option once the surrounding UDTF framework supports struct arguments. Kept
/// below `serde_json`'s 128-level parse limit so that any document we accept
/// we can also walk to completion.
const MAX_DEPTH: usize = 64;

static OUTPUT_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("key", DataType::Utf8, true),
        Field::new("value", DataType::Utf8, true),
        Field::new("type", DataType::Utf8, false),
        Field::new("atom", DataType::Utf8, true),
        Field::new("id", DataType::Int64, false),
        Field::new("parent", DataType::Int64, true),
        Field::new("fullkey", DataType::Utf8, false),
        Field::new("path", DataType::Utf8, false),
    ]))
});

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TreeRow {
    pub key: Option<String>,
    pub value: Option<String>,
    pub type_name: String,
    pub atom: Option<String>,
    pub id: i64,
    pub parent: Option<i64>,
    pub fullkey: String,
    pub path: String,
}

#[derive(Clone, Default)]
pub struct JsonTreeTableFunc;

impl JsonTreeTableFunc {
    #[must_use]
    pub fn new() -> Self {
        Self
    }
}

impl Debug for JsonTreeTableFunc {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JsonTreeTableFunc").finish()
    }
}

impl TableFunctionImpl for JsonTreeTableFunc {
    fn call(&self, exprs: &[Expr]) -> DataFusionResult<Arc<dyn TableProvider>> {
        let input = parse_input_arg(exprs)?;
        let rows = input.as_deref().map(json_tree).unwrap_or_default();
        Ok(Arc::new(JsonTreeTable {
            schema: Arc::clone(&OUTPUT_SCHEMA),
            rows,
        }))
    }
}

fn parse_input_arg(exprs: &[Expr]) -> DataFusionResult<Option<String>> {
    let Some(first) = exprs.first() else {
        return Err(DataFusionError::Plan(format!(
            "{JSON_TREE_UDTF_NAME}() requires a JSON string argument."
        )));
    };
    match first {
        Expr::Literal(ScalarValue::Utf8(v) | ScalarValue::LargeUtf8(v), _) => Ok(v.clone()),
        Expr::Literal(ScalarValue::Null, _) => Ok(None),
        other => Err(DataFusionError::NotImplemented(format!(
            "{JSON_TREE_UDTF_NAME}() currently supports only literal JSON string arguments. Per-row LATERAL invocation with a column reference will land in a later milestone. Got: {other:?}."
        ))),
    }
}

/// Walk an arbitrary JSON document and return one [`TreeRow`] per node in
/// depth-first order. Returns an empty `Vec` for input that is not valid JSON.
#[must_use]
pub fn json_tree(input: &str) -> Vec<TreeRow> {
    let Ok(root) = serde_json::from_str::<Value>(input) else {
        return Vec::new();
    };
    let mut ctx = WalkCtx {
        rows: Vec::new(),
        next_id: 0,
    };
    visit(&root, None, None, "$", "", 0, &mut ctx);
    ctx.rows
}

struct WalkCtx {
    rows: Vec<TreeRow>,
    next_id: i64,
}

fn visit(
    node: &Value,
    key: Option<String>,
    parent: Option<i64>,
    fullkey: &str,
    path: &str,
    depth: usize,
    ctx: &mut WalkCtx,
) {
    if depth > MAX_DEPTH {
        return;
    }
    let id = ctx.next_id;
    ctx.next_id += 1;

    let type_name = type_of(node).to_owned();
    let value = Some(node.to_string());
    let atom = atom_of(node);

    ctx.rows.push(TreeRow {
        key,
        value,
        type_name,
        atom,
        id,
        parent,
        fullkey: fullkey.to_owned(),
        path: path.to_owned(),
    });

    match node {
        Value::Object(map) => {
            for (child_key, child) in map {
                let child_fullkey = format!("{fullkey}.{}", escape_object_key(child_key));
                visit(
                    child,
                    Some(child_key.clone()),
                    Some(id),
                    &child_fullkey,
                    fullkey,
                    depth + 1,
                    ctx,
                );
            }
        }
        Value::Array(items) => {
            for (idx, child) in items.iter().enumerate() {
                let child_fullkey = format!("{fullkey}[{idx}]");
                visit(
                    child,
                    None,
                    Some(id),
                    &child_fullkey,
                    fullkey,
                    depth + 1,
                    ctx,
                );
            }
        }
        _ => {}
    }
}

fn type_of(v: &Value) -> &'static str {
    match v {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(n) if n.is_i64() || n.is_u64() => "integer",
        Value::Number(_) => "real",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

fn atom_of(v: &Value) -> Option<String> {
    match v {
        Value::Null => Some("null".to_owned()),
        Value::Bool(b) => Some(b.to_string()),
        Value::Number(n) => Some(n.to_string()),
        Value::String(s) => Some(s.clone()),
        Value::Array(_) | Value::Object(_) => None,
    }
}

/// Escape a JSON object key so the resulting JSON-Path expression parses
/// unambiguously. For simple identifiers we emit `$.name`; for keys with
/// special characters we fall back to bracket-with-quotes notation.
fn escape_object_key(key: &str) -> String {
    let simple = !key.is_empty()
        && key
            .chars()
            .enumerate()
            .all(|(i, c)| c.is_ascii_alphanumeric() || c == '_' || (i > 0 && c == '-'));
    if simple {
        key.to_owned()
    } else {
        format!("[{}]", serde_json::Value::String(key.to_owned()))
    }
}

#[derive(Debug)]
pub struct JsonTreeTable {
    schema: SchemaRef,
    rows: Vec<TreeRow>,
}

#[async_trait]
impl TableProvider for JsonTreeTable {
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

fn rows_to_batch(rows: &[TreeRow], schema: SchemaRef) -> DataFusionResult<RecordBatch> {
    let mut key = StringBuilder::with_capacity(rows.len(), rows.len() * 8);
    let mut value = StringBuilder::with_capacity(rows.len(), rows.len() * 32);
    let mut type_name = StringBuilder::with_capacity(rows.len(), rows.len() * 4);
    let mut atom = StringBuilder::with_capacity(rows.len(), rows.len() * 8);
    let mut id = Int64Builder::with_capacity(rows.len());
    let mut parent = Int64Builder::with_capacity(rows.len());
    let mut fullkey = StringBuilder::with_capacity(rows.len(), rows.len() * 16);
    let mut path = StringBuilder::with_capacity(rows.len(), rows.len() * 16);

    for row in rows {
        match &row.key {
            Some(v) => key.append_value(v),
            None => key.append_null(),
        }
        match &row.value {
            Some(v) => value.append_value(v),
            None => value.append_null(),
        }
        type_name.append_value(&row.type_name);
        match &row.atom {
            Some(v) => atom.append_value(v),
            None => atom.append_null(),
        }
        id.append_value(row.id);
        match row.parent {
            Some(p) => parent.append_value(p),
            None => parent.append_null(),
        }
        fullkey.append_value(&row.fullkey);
        path.append_value(&row.path);
    }

    let columns: Vec<ArrayRef> = vec![
        Arc::new(key.finish()),
        Arc::new(value.finish()),
        Arc::new(type_name.finish()),
        Arc::new(atom.finish()),
        Arc::new(id.finish()),
        Arc::new(parent.finish()),
        Arc::new(fullkey.finish()),
        Arc::new(path.finish()),
    ];

    RecordBatch::try_new(schema, columns)
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn by_fullkey(rows: &[TreeRow]) -> std::collections::HashMap<&str, &TreeRow> {
        rows.iter().map(|r| (r.fullkey.as_str(), r)).collect()
    }

    #[test]
    fn scalar_root_emits_single_row() {
        let rows = json_tree("42");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].type_name, "integer");
        assert_eq!(rows[0].atom.as_deref(), Some("42"));
        assert_eq!(rows[0].fullkey, "$");
        assert_eq!(rows[0].path, "");
        assert!(rows[0].parent.is_none());
        assert!(rows[0].key.is_none());
    }

    #[test]
    fn root_object_is_interior_and_children_reference_it() {
        let rows = json_tree(r#"{"a": 1, "b": "two"}"#);
        assert_eq!(rows.len(), 3);
        let root = &rows[0];
        assert_eq!(root.type_name, "object");
        assert!(root.atom.is_none());
        assert_eq!(root.fullkey, "$");
        assert_eq!(root.id, 0);
        assert!(root.parent.is_none());

        let by = by_fullkey(&rows);
        let a = by["$.a"];
        assert_eq!(a.parent, Some(0));
        assert_eq!(a.key.as_deref(), Some("a"));
        assert_eq!(a.type_name, "integer");
        assert_eq!(a.atom.as_deref(), Some("1"));

        let b = by["$.b"];
        assert_eq!(b.type_name, "string");
        assert_eq!(b.atom.as_deref(), Some("two"));
    }

    #[test]
    fn arrays_index_paths_numerically_and_parent_links_through() {
        let rows = json_tree(r#"{"xs": [10, 20, 30]}"#);
        let by = by_fullkey(&rows);
        assert_eq!(by["$.xs"].type_name, "array");
        assert_eq!(by["$.xs[0]"].atom.as_deref(), Some("10"));
        assert_eq!(by["$.xs[2]"].atom.as_deref(), Some("30"));
        // Array elements have no key name.
        assert!(by["$.xs[0]"].key.is_none());
        // Their `path` points to the containing array, not to root.
        assert_eq!(by["$.xs[0]"].path, "$.xs");
    }

    #[test]
    fn depth_first_order_and_monotonic_ids() {
        let rows = json_tree(r#"{"a": {"b": 1}, "c": 2}"#);
        let ids: Vec<i64> = rows.iter().map(|r| r.id).collect();
        assert_eq!(ids, vec![0, 1, 2, 3]);
        let fullkeys: Vec<&str> = rows.iter().map(|r| r.fullkey.as_str()).collect();
        assert_eq!(fullkeys, vec!["$", "$.a", "$.a.b", "$.c"]);
    }

    #[test]
    fn keys_with_special_characters_are_quoted_in_fullkey() {
        let rows = json_tree(r#"{"with space": 1, "with.dot": 2}"#);
        let fullkeys: Vec<&str> = rows.iter().map(|r| r.fullkey.as_str()).collect();
        assert!(fullkeys.contains(&r#"$.["with space"]"#));
        assert!(fullkeys.contains(&r#"$.["with.dot"]"#));
    }

    #[test]
    fn null_and_boolean_and_real_types_are_distinguished() {
        let rows = json_tree(r#"{"a": null, "b": true, "c": 1.5}"#);
        let by = by_fullkey(&rows);
        assert_eq!(by["$.a"].type_name, "null");
        assert_eq!(by["$.a"].atom.as_deref(), Some("null"));
        assert_eq!(by["$.b"].type_name, "boolean");
        assert_eq!(by["$.b"].atom.as_deref(), Some("true"));
        assert_eq!(by["$.c"].type_name, "real");
    }

    #[test]
    fn malformed_input_yields_zero_rows() {
        assert!(json_tree("not json").is_empty());
        assert!(json_tree("{broken").is_empty());
        assert!(json_tree("").is_empty());
    }

    #[test]
    fn deeply_nested_terminates_at_max_depth() {
        // Nest just past `MAX_DEPTH` (64) while staying under serde_json's
        // 128-level parse limit. Each wrapper adds one level of JSON nesting.
        const NESTING: usize = MAX_DEPTH + 20;
        let mut doc = String::from("0");
        for _ in 0..NESTING {
            doc = format!("[{doc}]");
        }
        let rows = json_tree(&doc);
        assert!(!rows.is_empty());
        // We stop recursing past MAX_DEPTH, so we emit at most MAX_DEPTH+1 rows
        // (root at depth 0 through depth MAX_DEPTH inclusive).
        assert!(rows.len() <= MAX_DEPTH + 1);
    }

    #[tokio::test]
    async fn table_provider_roundtrips_through_arrow() {
        use datafusion::prelude::SessionContext;
        let ctx = SessionContext::new();
        let func = JsonTreeTableFunc::new();
        let provider = func
            .call(&[Expr::Literal(
                ScalarValue::Utf8(Some(r#"{"a": [1, 2]}"#.to_string())),
                None,
            )])
            .expect("call succeeds");
        assert_eq!(provider.schema().fields().len(), 8);

        let state = ctx.state();
        let plan = provider.scan(&state, None, &[], None).await.expect("scan");
        let results = datafusion::physical_plan::collect(plan, ctx.task_ctx())
            .await
            .expect("collect");
        assert_eq!(results.len(), 1);
        // Root object + array + 2 ints = 4 rows.
        assert_eq!(results[0].num_rows(), 4);
    }

    #[test]
    fn scalar_null_input_accepted() {
        let func = JsonTreeTableFunc::new();
        let provider = func
            .call(&[Expr::Literal(ScalarValue::Null, None)])
            .expect("null is accepted");
        assert_eq!(provider.schema().fields().len(), 8);
    }
}
