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

//! `json_tree` — recursive depth-first walk of an arbitrary JSON document.
//!
//! Schema-agnostic sibling of `flatten_json_properties`. Mirrors `DuckDB` /
//! `SQLite`'s table function of the same name: one row per node (interior and
//! leaf), in depth-first order, with JSON-Path addresses and a parent pointer.
//!
//! ```text
//! json_tree(input Utf8 [, max_depth => UInt, max_rows => UInt, max_bytes => UInt]) -> TABLE(
//!     key       Utf8,
//!     value     Utf8,
//!     type      Utf8,
//!     atom      Utf8,
//!     id        Int64,
//!     parent    Int64,
//!     fullkey   Utf8,
//!     path      Utf8
//! )
//! ```
//!
//! Registered twice:
//! - As a UDTF for `SELECT * FROM json_tree('{...}')`. Named options
//!   (`max_depth`, `max_rows`, `max_bytes`) are only accepted in this form.
//! - As a scalar UDF returning `List<Struct<...>>` for per-row /
//!   `LATERAL json_tree(s.body)` usage via `UNNEST`. The scalar form takes
//!   only the JSON argument and always runs with default caps.

use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::sync::{Arc, LazyLock};

use arrow::array::{
    Array, ArrayRef, Int64Builder, LargeListArray, StringBuilder, StructArray, as_string_array,
};
use arrow::buffer::{OffsetBuffer, ScalarBuffer};
use arrow::compute::kernels::cast::cast;
use arrow_schema::{DataType, Field, Fields, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::{Session, TableFunctionImpl, TableProvider};
use datafusion::common::Result as DataFusionResult;
use datafusion::datasource::TableType;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::Expr;
use datafusion::scalar::ScalarValue;
use datafusion_datasource::memory::MemorySourceConfig;
use datafusion_datasource::source::DataSourceExec;
use opentelemetry::KeyValue;
use opentelemetry::global;
use opentelemetry::metrics::{Counter, Meter};
use serde_json::Value;

pub const JSON_TREE_UDTF_NAME: &str = "json_tree";

const DEFAULT_MAX_DEPTH: usize = 64;
const DEFAULT_MAX_ROWS: usize = 1_000_000;
const DEFAULT_MAX_BYTES: usize = 8 * 1024 * 1024;

/// Scalar UDF ceiling across a single evaluated batch. Per-document caps
/// already bound individual rows, but a wide input batch could still
/// accumulate `number_rows * max_rows` entries in memory. Error out loudly
/// past this watermark so operators see the condition rather than OOM.
const SCALAR_BATCH_MAX_ROWS: usize = 10_000_000;

// -------- Metrics --------

static METER: LazyLock<Meter> = LazyLock::new(|| global::meter("json_tree"));

static INVOCATIONS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("json_tree_invocations_total")
        .with_description("Invocations of json_tree.")
        .build()
});

static ROWS_EMITTED: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("json_tree_rows_emitted_total")
        .with_description("Rows emitted by json_tree.")
        .build()
});

static ERRORS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("json_tree_errors_total")
        .with_description(
            "Errors inside json_tree, labelled by kind (parse|depth_exceeded|input_too_large|row_cap_hit).",
        )
        .build()
});

fn record_error(kind: &'static str) {
    ERRORS.add(1, &[KeyValue::new("kind", kind)]);
}

// -------- Options + Output schema --------

#[derive(Debug, Clone)]
pub struct JsonTreeOptions {
    pub max_depth: usize,
    pub max_rows: usize,
    pub max_bytes: usize,
}

impl Default for JsonTreeOptions {
    fn default() -> Self {
        Self {
            max_depth: DEFAULT_MAX_DEPTH,
            max_rows: DEFAULT_MAX_ROWS,
            max_bytes: DEFAULT_MAX_BYTES,
        }
    }
}

static TREE_FIELDS: LazyLock<Fields> = LazyLock::new(|| {
    Fields::from(vec![
        Field::new("key", DataType::Utf8, true),
        Field::new("value", DataType::Utf8, true),
        Field::new("type", DataType::Utf8, false),
        Field::new("atom", DataType::Utf8, true),
        Field::new("id", DataType::Int64, false),
        Field::new("parent", DataType::Int64, true),
        Field::new("fullkey", DataType::Utf8, false),
        // `path` is the parent JSON-Path. The root row has no parent, so it's
        // emitted as NULL (matches DuckDB / SQLite `json_tree` semantics).
        Field::new("path", DataType::Utf8, true),
    ])
});

static OUTPUT_SCHEMA: LazyLock<SchemaRef> =
    LazyLock::new(|| Arc::new(Schema::new(TREE_FIELDS.clone())));

/// Return type of the scalar UDF form. Uses `LargeList` (i64 offsets)
/// instead of `List` so a large batch can't overflow the offset range and
/// silently drop rows. `UNNEST` works on both variants, so the change is
/// transparent to downstream SQL.
static ROW_LIST_TYPE: LazyLock<DataType> = LazyLock::new(|| {
    DataType::LargeList(Arc::new(Field::new(
        "item",
        DataType::Struct(TREE_FIELDS.clone()),
        true,
    )))
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
    pub path: Option<String>,
}

// -------- Public entry points --------

#[must_use]
pub fn json_tree(input: &str) -> Vec<TreeRow> {
    json_tree_with_options(input, &JsonTreeOptions::default())
}

#[must_use]
pub fn json_tree_with_options(input: &str, opts: &JsonTreeOptions) -> Vec<TreeRow> {
    INVOCATIONS.add(1, &[]);

    if input.len() > opts.max_bytes {
        record_error("input_too_large");
        return Vec::new();
    }

    let Ok(root) = serde_json::from_str::<Value>(input) else {
        record_error("parse");
        return Vec::new();
    };
    let mut ctx = WalkCtx {
        rows: Vec::new(),
        next_id: 0,
        depth_cap_hit: false,
        row_cap_hit: false,
    };
    visit(&root, None, None, "$", None, 0, opts, &mut ctx);
    ROWS_EMITTED.add(ctx.rows.len() as u64, &[]);
    ctx.rows
}

struct WalkCtx {
    rows: Vec<TreeRow>,
    next_id: i64,
    depth_cap_hit: bool,
    row_cap_hit: bool,
}

#[expect(
    clippy::too_many_arguments,
    reason = "walker threads per-node state; collapsing into a struct adds indirection without clarity"
)]
fn visit(
    node: &Value,
    key: Option<String>,
    parent: Option<i64>,
    fullkey: &str,
    path: Option<&str>,
    depth: usize,
    opts: &JsonTreeOptions,
    ctx: &mut WalkCtx,
) {
    if ctx.row_cap_hit {
        return;
    }
    if depth > opts.max_depth {
        if !ctx.depth_cap_hit {
            ctx.depth_cap_hit = true;
            record_error("depth_exceeded");
        }
        return;
    }
    if ctx.rows.len() >= opts.max_rows {
        if !ctx.row_cap_hit {
            ctx.row_cap_hit = true;
            record_error("row_cap_hit");
        }
        return;
    }
    let id = ctx.next_id;
    ctx.next_id += 1;

    ctx.rows.push(TreeRow {
        key,
        value: Some(node.to_string()),
        type_name: type_of(node).to_owned(),
        atom: atom_of(node),
        id,
        parent,
        fullkey: fullkey.to_owned(),
        path: path.map(ToOwned::to_owned),
    });

    match node {
        Value::Object(map) => {
            for (child_key, child) in map {
                if ctx.row_cap_hit {
                    return;
                }
                let child_fullkey = format!("{fullkey}.{}", escape_object_key(child_key));
                visit(
                    child,
                    Some(child_key.clone()),
                    Some(id),
                    &child_fullkey,
                    Some(fullkey),
                    depth + 1,
                    opts,
                    ctx,
                );
            }
        }
        Value::Array(items) => {
            for (idx, child) in items.iter().enumerate() {
                if ctx.row_cap_hit {
                    return;
                }
                let child_fullkey = format!("{fullkey}[{idx}]");
                // DuckDB / SQLite `json_tree` sets `key` to the array index as
                // a string so consumers can distinguish array siblings.
                visit(
                    child,
                    Some(idx.to_string()),
                    Some(id),
                    &child_fullkey,
                    Some(fullkey),
                    depth + 1,
                    opts,
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

fn escape_object_key(key: &str) -> String {
    // SQLite / DuckDB JSON-path shorthand (`$.a.b`) accepts identifier-style
    // keys only — anything else, including hyphens, must be bracket-quoted so
    // consumers can re-parse the `fullkey`.
    let first = key.chars().next();
    let simple = first.is_some_and(|c| !c.is_ascii_digit())
        && key.chars().all(|c| c.is_ascii_alphanumeric() || c == '_');
    if simple {
        key.to_owned()
    } else {
        format!("[{}]", serde_json::Value::String(key.to_owned()))
    }
}

// -------- UDTF --------

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
        let (input, opts) = parse_udtf_args(exprs)?;
        let rows = input
            .as_deref()
            .map(|s| json_tree_with_options(s, &opts))
            .unwrap_or_default();
        Ok(Arc::new(JsonTreeTable {
            schema: Arc::clone(&OUTPUT_SCHEMA),
            rows,
        }))
    }
}

fn parse_udtf_args(exprs: &[Expr]) -> DataFusionResult<(Option<String>, JsonTreeOptions)> {
    let mut iter = exprs.iter();
    let first = iter.next().ok_or_else(|| {
        DataFusionError::Plan(format!(
            "{JSON_TREE_UDTF_NAME}() requires a JSON string argument."
        ))
    })?;
    let input = match first {
        Expr::Literal(ScalarValue::Utf8(v) | ScalarValue::LargeUtf8(v), _) => v.clone(),
        Expr::Literal(ScalarValue::Null, _) => None,
        other => {
            return Err(DataFusionError::NotImplemented(format!(
                "{JSON_TREE_UDTF_NAME}() currently supports a literal JSON string as the first \
                 argument. For per-row / LATERAL invocation, use \
                 `UNNEST({JSON_TREE_UDTF_NAME}(<column>))`. Got: {other:?}."
            )));
        }
    };

    let mut opts = JsonTreeOptions::default();
    for arg in iter {
        if let Expr::Literal(scalar, Some(meta)) = arg
            && let Some(name) = meta.inner().get("spice.parameter_name")
        {
            let name = name.clone();
            match name.as_str() {
                "max_depth" => opts.max_depth = parse_usize(&name, scalar)?,
                "max_rows" => opts.max_rows = parse_usize(&name, scalar)?,
                "max_bytes" => opts.max_bytes = parse_usize(&name, scalar)?,
                other => {
                    return Err(DataFusionError::Plan(format!(
                        "Unknown option '{other}'. Supported: max_depth, max_rows, max_bytes."
                    )));
                }
            }
            continue;
        }
        return Err(DataFusionError::Plan(format!(
            "Arguments after the JSON string must be named, e.g. `max_depth => 64`. Got: {arg:?}."
        )));
    }

    Ok((input, opts))
}

fn parse_usize(name: &str, v: &ScalarValue) -> DataFusionResult<usize> {
    let n: i64 = match v {
        ScalarValue::Int8(Some(n)) => i64::from(*n),
        ScalarValue::Int16(Some(n)) => i64::from(*n),
        ScalarValue::Int32(Some(n)) => i64::from(*n),
        ScalarValue::Int64(Some(n)) => *n,
        ScalarValue::UInt8(Some(n)) => i64::from(*n),
        ScalarValue::UInt16(Some(n)) => i64::from(*n),
        ScalarValue::UInt32(Some(n)) => i64::from(*n),
        ScalarValue::UInt64(Some(n)) => i64::try_from(*n)
            .map_err(|_| DataFusionError::Plan(format!("{name} must fit in i64, got {n}")))?,
        other => {
            return Err(DataFusionError::Plan(format!(
                "{name} must be an integer, got {other:?}"
            )));
        }
    };
    usize::try_from(n)
        .map_err(|_| DataFusionError::Plan(format!("{name} must be non-negative, got {n}")))
}

#[derive(Debug)]
pub struct JsonTreeTable {
    schema: SchemaRef,
    rows: Vec<TreeRow>,
}

#[async_trait]
impl TableProvider for JsonTreeTable {
    fn as_any(&self) -> &dyn Any {
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
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        // Single-node only: a bare `DataSourceExec(MemorySourceConfig)` is
        // rejected by `EnsureSupportedFileScan` in cluster mode. Distributed
        // support requires a dedicated `UdtfArgs` proto variant + codec so
        // remote executors can re-invoke the walker; that's follow-up scope.
        let batch = rows_to_batch(&self.rows, Arc::clone(&self.schema))?;
        let src = MemorySourceConfig::try_new(
            &[vec![batch]],
            Arc::clone(&self.schema),
            projection.cloned(),
        )?;
        Ok(Arc::new(DataSourceExec::new(Arc::new(src))))
    }
}

fn rows_to_batch(rows: &[TreeRow], schema: SchemaRef) -> DataFusionResult<RecordBatch> {
    RecordBatch::try_new(schema, build_tree_arrays(rows))
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
}

fn build_tree_arrays(rows: &[TreeRow]) -> Vec<ArrayRef> {
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
        match &row.path {
            Some(v) => path.append_value(v),
            None => path.append_null(),
        }
    }

    vec![
        Arc::new(key.finish()),
        Arc::new(value.finish()),
        Arc::new(type_name.finish()),
        Arc::new(atom.finish()),
        Arc::new(id.finish()),
        Arc::new(parent.finish()),
        Arc::new(fullkey.finish()),
        Arc::new(path.finish()),
    ]
}

// -------- Scalar UDF --------

#[derive(Debug, Clone)]
pub struct JsonTreeScalar {
    signature: Signature,
}

impl Default for JsonTreeScalar {
    fn default() -> Self {
        Self::new()
    }
}

impl JsonTreeScalar {
    #[must_use]
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![DataType::Utf8]),
                    TypeSignature::Exact(vec![DataType::LargeUtf8]),
                    TypeSignature::Exact(vec![DataType::Utf8View]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl PartialEq for JsonTreeScalar {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for JsonTreeScalar {}

impl std::hash::Hash for JsonTreeScalar {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

impl ScalarUDFImpl for JsonTreeScalar {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        JSON_TREE_UDTF_NAME
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> DataFusionResult<DataType> {
        Ok(ROW_LIST_TYPE.clone())
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DataFusionResult<ColumnarValue> {
        let input_col = args
            .args
            .first()
            .ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "{JSON_TREE_UDTF_NAME}() requires a JSON string argument."
                ))
            })?
            .clone();

        let opts = JsonTreeOptions::default();
        let array = input_col.into_array(args.number_rows)?;
        // Signature restricts input to Utf8/LargeUtf8/Utf8View; normalize to
        // Utf8 so `as_string_array` below always succeeds.
        let normalized = if matches!(array.data_type(), DataType::Utf8) {
            array
        } else {
            cast(&array, &DataType::Utf8)
                .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?
        };
        let strings = as_string_array(&normalized);

        // Using `LargeListArray` (i64 offsets) so a large evaluated batch
        // cannot overflow and silently drop rows. Per-document caps inside
        // `json_tree_with_options` still bound memory use.
        let mut all_rows: Vec<TreeRow> = Vec::new();
        let mut offsets: Vec<i64> = Vec::with_capacity(strings.len() + 1);
        offsets.push(0);

        for idx in 0..strings.len() {
            if !strings.is_null(idx) {
                let rows = json_tree_with_options(strings.value(idx), &opts);
                all_rows.extend(rows);
                if all_rows.len() > SCALAR_BATCH_MAX_ROWS {
                    record_error("batch_cap_hit");
                    return Err(DataFusionError::Execution(format!(
                        "{JSON_TREE_UDTF_NAME}(): batch produced more than {SCALAR_BATCH_MAX_ROWS} rows; lower `max_rows` or split the input."
                    )));
                }
            }
            // Walker caps bound the row count well under `i64::MAX`, but if
            // somehow they didn't, silently saturating would misalign list
            // offsets. Fail loud instead so the condition is visible.
            let len = i64::try_from(all_rows.len()).map_err(|_| {
                DataFusionError::Execution(format!(
                    "{JSON_TREE_UDTF_NAME}(): flattened row count exceeds LargeList i64 offset range."
                ))
            })?;
            offsets.push(len);
        }

        let struct_array =
            StructArray::new(TREE_FIELDS.clone(), build_tree_arrays(&all_rows), None);
        let list_array = LargeListArray::new(
            Arc::new(Field::new(
                "item",
                DataType::Struct(TREE_FIELDS.clone()),
                true,
            )),
            OffsetBuffer::new(ScalarBuffer::from(offsets)),
            Arc::new(struct_array),
            None,
        );
        Ok(ColumnarValue::Array(Arc::new(list_array)))
    }
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
        assert!(rows[0].parent.is_none());
        assert!(rows[0].key.is_none());
    }

    #[test]
    fn root_object_is_interior_and_children_reference_it() {
        let rows = json_tree(r#"{"a": 1, "b": "two"}"#);
        assert_eq!(rows.len(), 3);
        let by = by_fullkey(&rows);
        assert_eq!(by["$.a"].parent, Some(0));
        assert_eq!(by["$.a"].type_name, "integer");
        assert_eq!(by["$.b"].type_name, "string");
    }

    #[test]
    fn arrays_index_paths_numerically() {
        let rows = json_tree(r#"{"xs": [10, 20, 30]}"#);
        let by = by_fullkey(&rows);
        assert_eq!(by["$.xs"].type_name, "array");
        assert_eq!(by["$.xs[0]"].atom.as_deref(), Some("10"));
        assert_eq!(by["$.xs[2]"].atom.as_deref(), Some("30"));
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
    fn keys_with_special_characters_are_quoted() {
        let rows = json_tree(r#"{"with space": 1, "has-hyphen": 2, "_ok": 3, "plain": 4}"#);
        let fullkeys: Vec<&str> = rows.iter().map(|r| r.fullkey.as_str()).collect();
        // Space and hyphen both force bracket-quoting so consumers can re-parse.
        assert!(fullkeys.contains(&r#"$.["with space"]"#));
        assert!(fullkeys.contains(&r#"$.["has-hyphen"]"#));
        // Identifier-safe keys stay in shorthand form.
        assert!(fullkeys.contains(&"$._ok"));
        assert!(fullkeys.contains(&"$.plain"));
    }

    #[test]
    fn malformed_input_yields_zero_rows() {
        assert!(json_tree("not json").is_empty());
        assert!(json_tree("").is_empty());
    }

    #[test]
    fn deeply_nested_terminates_at_max_depth() {
        const NESTING: usize = DEFAULT_MAX_DEPTH + 20;
        let mut doc = String::from("0");
        for _ in 0..NESTING {
            doc = format!("[{doc}]");
        }
        let rows = json_tree(&doc);
        assert!(!rows.is_empty());
        assert!(rows.len() <= DEFAULT_MAX_DEPTH + 1);
    }

    #[test]
    fn max_depth_option_is_honoured() {
        let opts = JsonTreeOptions {
            max_depth: 2,
            ..Default::default()
        };
        // depth 0 → root object, depth 1 → "a", depth 2 → "a.b", depth 3 → stop.
        let rows = json_tree_with_options(r#"{"a": {"b": {"c": 1}}}"#, &opts);
        let fullkeys: Vec<_> = rows.iter().map(|r| r.fullkey.as_str()).collect();
        assert!(fullkeys.contains(&"$.a.b"));
        assert!(!fullkeys.contains(&"$.a.b.c"));
    }

    #[test]
    fn max_rows_caps_output() {
        // 50 elements but cap of 10 → 10 rows total (cap includes root).
        let doc = "[".to_string() + &(0..49).map(|_| "0,").collect::<String>() + "0]";
        let opts = JsonTreeOptions {
            max_rows: 10,
            ..Default::default()
        };
        let rows = json_tree_with_options(&doc, &opts);
        assert_eq!(rows.len(), 10);
    }

    #[test]
    fn max_bytes_rejects_oversized_input() {
        let opts = JsonTreeOptions {
            max_bytes: 4,
            ..Default::default()
        };
        assert!(json_tree_with_options(r#"{"a": 1}"#, &opts).is_empty());
    }

    #[tokio::test]
    async fn udtf_table_provider_roundtrips() {
        use datafusion::prelude::SessionContext;
        let ctx = SessionContext::new();
        let func = JsonTreeTableFunc::new();
        let provider = func
            .call(&[Expr::Literal(
                ScalarValue::Utf8(Some(r#"{"a": [1, 2]}"#.to_string())),
                None,
            )])
            .expect("call succeeds");
        let state = ctx.state();
        let plan = provider.scan(&state, None, &[], None).await.expect("scan");
        let results = datafusion::physical_plan::collect(plan, ctx.task_ctx())
            .await
            .expect("collect");
        // root object + array + 2 ints = 4 rows.
        assert_eq!(results[0].num_rows(), 4);
    }

    #[test]
    fn scalar_udf_return_type() {
        let udf = JsonTreeScalar::new();
        let ty = udf.return_type(&[DataType::Utf8]).expect("return type");
        assert!(matches!(ty, DataType::LargeList(_)));
    }

    #[tokio::test]
    async fn scan_with_projection_returns_only_requested_columns() {
        use datafusion::prelude::SessionContext;
        let ctx = SessionContext::new();
        let func = JsonTreeTableFunc::new();
        let provider = func
            .call(&[Expr::Literal(
                ScalarValue::Utf8(Some(r#"{"a": [1, 2]}"#.to_string())),
                None,
            )])
            .expect("call succeeds");

        // Full schema has 8 columns; request only columns 0 (key), 2 (type), 6 (fullkey).
        let projection = vec![0usize, 2, 6];
        let state = ctx.state();
        let plan = provider
            .scan(&state, Some(&projection), &[], None)
            .await
            .expect("scan with projection");
        let results = datafusion::physical_plan::collect(plan, ctx.task_ctx())
            .await
            .expect("collect");
        assert_eq!(results[0].num_columns(), 3, "expected 3 projected columns");
        assert_eq!(results[0].schema().field(0).name(), "key");
        assert_eq!(results[0].schema().field(1).name(), "type");
        assert_eq!(results[0].schema().field(2).name(), "fullkey");
    }
}
