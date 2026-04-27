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

//! `flatten_json` — generic JSON-*data* flattener.
//!
//! Sibling of `flatten_json_properties` (which walks JSON *Schema*). This UDTF
//! takes an arbitrary JSON *value* and emits one row per reachable leaf (by
//! default), with dotted or JSON-pointer paths that match the conventions
//! `flatten_json_properties` produces.
//!
//! ```text
//! flatten_json(input Utf8 [, options...]) -> TABLE(
//!     path         Utf8,
//!     parent_path  Utf8,
//!     key          Utf8,
//!     value        Utf8,   -- leaf value as string; NULL for JSON null;
//!                          -- containers emit compact JSON when
//!                          -- `include_internal` / empty-container
//!                          -- fallbacks surface them as rows
//!     type         Utf8    -- "object"|"array"|"string"|"number"|"integer"|"boolean"|"null"
//! )
//! ```
//!
//! Two entry points are registered:
//!
//! - **UDTF** — `SELECT * FROM flatten_json('{...}')` (named options supported).
//! - **Scalar UDF** — returns `LargeList<Struct<...>>` for per-row use. Access
//!   struct fields via `UNNEST` in SELECT position:
//!   ```sql
//!   SELECT rows.path, rows.value, rows.type
//!   FROM (SELECT UNNEST(flatten_json(body)) AS rows FROM docs);
//!   ```
//!   Note: the cross-join form `FROM docs d, UNNEST(flatten_json(d.body)) AS r`
//!   does NOT expose struct fields as `r.path` — use the SELECT-position form above.
//!
//! Options (named args):
//! - `max_depth` (`UInt`, default `64`).
//! - `max_rows` (`UInt`, default `1_000_000`) per-document cap.
//! - `max_bytes` (`UInt`, default `8_388_608`) input size limit.
//! - `path_style` (`Utf8`, `"dot"` | `"json-pointer"`, default `"dot"`).
//! - `include_internal` (`Bool`, default `false`) — also emit interior
//!   object/array rows.
//! - `array_wildcard` (`Bool`, default `false`) — when `true`, arrays collapse
//!   to a single wildcard segment (`a[*]` for dot paths, `/a/[*]` for
//!   json-pointer) instead of per-index (`a[0]`, `a[1]`, …). Matches
//!   `JSONPath` storage conventions.
//!
//! Telemetry: OpenTelemetry counters `flatten_json_invocations_total`,
//! `flatten_json_rows_emitted_total`, and
//! `flatten_json_errors_total{kind}`. For the UDTF entry point, malformed
//! input or a hit cap emits an error-kind metric and yields an empty /
//! truncated batch instead of a query-level error. The scalar UDF entry
//! point additionally returns `DataFusionError::Execution` if a single
//! batch would exceed `SCALAR_BATCH_MAX_ROWS` flattened rows or if the
//! resulting `LargeList` offsets would overflow `i64`; callers in those
//! paths should reduce `max_rows` or split the input.

use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::sync::{Arc, LazyLock};

use arrow::array::{
    Array, ArrayRef, LargeListArray, StringBuilder, StructArray, as_largestring_array,
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

pub const FLATTEN_JSON_UDTF_NAME: &str = "flatten_json";

const DEFAULT_MAX_DEPTH: usize = 64;
const DEFAULT_MAX_ROWS: usize = 1_000_000;
const DEFAULT_MAX_BYTES: usize = 8 * 1024 * 1024;

/// Scalar UDF ceiling across a single evaluated batch. Per-document caps
/// already bound individual rows, but a wide input batch could still
/// accumulate `number_rows * max_rows` entries in memory. Error out loudly
/// past this watermark so operators see the condition rather than OOM.
const SCALAR_BATCH_MAX_ROWS: usize = 10_000_000;

// -------- Metrics --------

static METER: LazyLock<Meter> = LazyLock::new(|| global::meter("flatten_json"));

static INVOCATIONS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("flatten_json_invocations_total")
        .with_description("Invocations of flatten_json.")
        .build()
});

static ROWS_EMITTED: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("flatten_json_rows_emitted_total")
        .with_description("Rows emitted by flatten_json.")
        .build()
});

static ERRORS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("flatten_json_errors_total")
        .with_description(
            "Errors inside flatten_json, labelled by kind \
             (parse|depth_exceeded|row_cap_hit|input_too_large|batch_cap_hit).",
        )
        .build()
});

fn record_error(kind: &'static str) {
    ERRORS.add(1, &[KeyValue::new("kind", kind)]);
}

// -------- Output schema --------

static ROW_FIELDS: LazyLock<Fields> = LazyLock::new(|| {
    Fields::from(vec![
        Field::new("path", DataType::Utf8, false),
        Field::new("parent_path", DataType::Utf8, false),
        Field::new("key", DataType::Utf8, false),
        Field::new("value", DataType::Utf8, true),
        Field::new("type", DataType::Utf8, false),
    ])
});

static OUTPUT_SCHEMA: LazyLock<SchemaRef> =
    LazyLock::new(|| Arc::new(Schema::new(ROW_FIELDS.clone())));

static ROW_LIST_TYPE: LazyLock<DataType> = LazyLock::new(|| {
    DataType::LargeList(Arc::new(Field::new(
        "item",
        DataType::Struct(ROW_FIELDS.clone()),
        true,
    )))
});

// -------- Row + Options --------

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JsonRow {
    pub path: String,
    pub parent_path: String,
    pub key: String,
    pub value: Option<String>,
    pub type_name: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PathStyle {
    Dot,
    JsonPointer,
}

impl PathStyle {
    fn parse(s: &str) -> Option<Self> {
        match s {
            "dot" => Some(Self::Dot),
            "json-pointer" | "jsonpointer" => Some(Self::JsonPointer),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct FlattenOptions {
    pub max_depth: usize,
    pub max_rows: usize,
    pub max_bytes: usize,
    pub path_style: PathStyle,
    pub include_internal: bool,
    /// When `true`, arrays collapse to a single wildcard segment instead of
    /// per-index paths. Matches `JSONPath` storage conventions.
    pub array_wildcard: bool,
}

impl Default for FlattenOptions {
    fn default() -> Self {
        Self {
            max_depth: DEFAULT_MAX_DEPTH,
            max_rows: DEFAULT_MAX_ROWS,
            max_bytes: DEFAULT_MAX_BYTES,
            path_style: PathStyle::Dot,
            include_internal: false,
            array_wildcard: false,
        }
    }
}

// -------- Public entry points --------

/// Walk with default options.
#[must_use]
pub fn flatten(input: &str) -> Vec<JsonRow> {
    flatten_with_options(input, &FlattenOptions::default())
}

/// Walk an arbitrary JSON value and return one [`JsonRow`] per reachable leaf
/// (and interior node when `include_internal` is set). Never errors: returns
/// an empty / truncated `Vec` for malformed input or caps being hit, emitting
/// the corresponding metric.
#[must_use]
pub fn flatten_with_options(input: &str, opts: &FlattenOptions) -> Vec<JsonRow> {
    INVOCATIONS.add(1, &[]);

    if input.len() > opts.max_bytes {
        record_error("input_too_large");
        return Vec::new();
    }

    let Ok(root) = serde_json::from_str::<Value>(input) else {
        record_error("parse");
        return Vec::new();
    };

    let mut walker = Walker {
        opts,
        rows: Vec::new(),
        row_cap_hit: false,
        depth_cap_hit: false,
    };
    walker.walk(&root, "", "", 0);
    ROWS_EMITTED.add(walker.rows.len() as u64, &[]);
    walker.rows
}

// -------- Walker --------

struct Walker<'a> {
    opts: &'a FlattenOptions,
    rows: Vec<JsonRow>,
    row_cap_hit: bool,
    depth_cap_hit: bool,
}

impl Walker<'_> {
    fn walk(&mut self, value: &Value, path: &str, key: &str, depth: usize) {
        if self.row_cap_hit {
            return;
        }
        if depth > self.opts.max_depth {
            // Only record once per invocation so deeply-nested inputs
            // don't inflate the `depth_exceeded` counter by O(nodes).
            if !self.depth_cap_hit {
                record_error("depth_exceeded");
                self.depth_cap_hit = true;
            }
            return;
        }

        let type_name = value_type(value);
        let parent_path = parent_of(path, self.opts.path_style);
        let is_container = matches!(value, Value::Object(_) | Value::Array(_));

        // Emit interior rows only when `include_internal` is set.
        // Emit leaves always. The root gets no row unless it is a leaf
        // (depth 0 container with no children suppressed) or
        // `include_internal` is set.
        let emit_now = if is_container {
            self.opts.include_internal
        } else {
            true
        };
        let rows_before = self.rows.len();
        if emit_now {
            self.push_row(path, &parent_path, key, value, &type_name);
            if self.row_cap_hit {
                return;
            }
        }

        match value {
            Value::Object(map) => {
                for (child_key, child) in map {
                    let child_path = make_path_segment(path, child_key, self.opts.path_style);
                    self.walk(child, &child_path, child_key, depth + 1);
                    if self.row_cap_hit {
                        return;
                    }
                }
            }
            Value::Array(arr) => {
                if self.opts.array_wildcard {
                    // Collapse all elements onto a single `[*]` path. Each
                    // element still gets walked, so an array of objects
                    // produces one row per element's leaf (with duplicate
                    // path strings for leaves at the same depth across
                    // siblings, matching JSONPath semantics).
                    let child_path = make_array_wildcard(path, self.opts.path_style);
                    for child in arr {
                        self.walk(child, &child_path, "[*]", depth + 1);
                        if self.row_cap_hit {
                            return;
                        }
                    }
                } else {
                    for (idx, child) in arr.iter().enumerate() {
                        let child_path = make_array_index(path, idx, self.opts.path_style);
                        // Match the last path segment: bracketed for dot
                        // paths (`[0]`), bare index for json-pointer
                        // (`0`). Keeps `key` consistent with the trailing
                        // segment of `path` so callers can reconstruct
                        // `path` from `parent_path` + `key`.
                        let idx_key = match self.opts.path_style {
                            PathStyle::JsonPointer => idx.to_string(),
                            PathStyle::Dot => format!("[{idx}]"),
                        };
                        self.walk(child, &child_path, &idx_key, depth + 1);
                        if self.row_cap_hit {
                            return;
                        }
                    }
                }
            }
            _ => {}
        }

        // Empty containers (object/array with zero elements) would otherwise
        // produce no row at all in leaves-only mode. Surface them as a single
        // container row so the field still appears.
        if is_container && !emit_now && self.rows.len() == rows_before {
            self.push_row(path, &parent_path, key, value, &type_name);
        }
    }

    fn push_row(
        &mut self,
        path: &str,
        parent_path: &str,
        key: &str,
        value: &Value,
        type_name: &str,
    ) {
        if self.rows.len() >= self.opts.max_rows {
            if !self.row_cap_hit {
                record_error("row_cap_hit");
                self.row_cap_hit = true;
            }
            return;
        }
        self.rows.push(JsonRow {
            path: path.to_owned(),
            parent_path: parent_path.to_owned(),
            key: key.to_owned(),
            value: leaf_value_string(value),
            type_name: type_name.to_owned(),
        });
    }
}

fn value_type(v: &Value) -> String {
    match v {
        Value::Null => "null".to_owned(),
        Value::Bool(_) => "boolean".to_owned(),
        Value::Number(n) => {
            if n.is_i64() || n.is_u64() {
                "integer".to_owned()
            } else {
                "number".to_owned()
            }
        }
        Value::String(_) => "string".to_owned(),
        Value::Array(_) => "array".to_owned(),
        Value::Object(_) => "object".to_owned(),
    }
}

/// Render leaf values as a compact string. Strings are unquoted, numbers and
/// booleans are stringified, JSON `null` yields SQL `NULL`, containers yield
/// their JSON-compact serialization when they are emitted as a row.
fn leaf_value_string(v: &Value) -> Option<String> {
    match v {
        Value::Null => None,
        Value::Bool(b) => Some(b.to_string()),
        Value::Number(n) => Some(n.to_string()),
        Value::String(s) => Some(s.clone()),
        // For interior rows (only emitted with `include_internal`), surface
        // the container's compact JSON form so downstream consumers can
        // re-parse it if needed.
        Value::Array(_) | Value::Object(_) => serde_json::to_string(v).ok(),
    }
}

/// Extract the parent of the given path without re-computing it during the
/// walk. Empty input returns empty — the root has no parent.
fn parent_of(path: &str, style: PathStyle) -> String {
    if path.is_empty() {
        return String::new();
    }
    match style {
        PathStyle::Dot => {
            // The last segment is either `.name` or `[idx]` / `[*]`.
            // Find the last separator that starts a new segment.
            let last_dot = path.rfind('.');
            let last_bracket = path.rfind('[');
            match (last_dot, last_bracket) {
                (Some(d), Some(b)) if d > b => path[..d].to_owned(),
                (Some(d), None) => path[..d].to_owned(),
                (_, Some(b)) => path[..b].to_owned(),
                (None, None) => String::new(),
            }
        }
        PathStyle::JsonPointer => path
            .rfind('/')
            .map_or(String::new(), |i| path[..i].to_owned()),
    }
}

fn make_path_segment(parent: &str, name: &str, style: PathStyle) -> String {
    match style {
        PathStyle::Dot => {
            if parent.is_empty() {
                name.to_owned()
            } else {
                format!("{parent}.{name}")
            }
        }
        PathStyle::JsonPointer => {
            // RFC 6901 escaping: `~` → `~0`, `/` → `~1`.
            let escaped = name.replace('~', "~0").replace('/', "~1");
            if parent.is_empty() {
                format!("/{escaped}")
            } else {
                format!("{parent}/{escaped}")
            }
        }
    }
}

fn make_array_index(parent: &str, idx: usize, style: PathStyle) -> String {
    match style {
        PathStyle::Dot => format!("{parent}[{idx}]"),
        PathStyle::JsonPointer => {
            if parent.is_empty() {
                format!("/{idx}")
            } else {
                format!("{parent}/{idx}")
            }
        }
    }
}

fn make_array_wildcard(parent: &str, style: PathStyle) -> String {
    match style {
        PathStyle::Dot => format!("{parent}[*]"),
        PathStyle::JsonPointer => {
            if parent.is_empty() {
                "/[*]".to_owned()
            } else {
                format!("{parent}/[*]")
            }
        }
    }
}

// -------- UDTF --------

#[derive(Clone, Default)]
pub struct FlattenJsonTableFunc;

impl FlattenJsonTableFunc {
    #[must_use]
    pub fn new() -> Self {
        Self
    }
}

impl Debug for FlattenJsonTableFunc {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FlattenJsonTableFunc").finish()
    }
}

impl TableFunctionImpl for FlattenJsonTableFunc {
    fn call(&self, exprs: &[Expr]) -> DataFusionResult<Arc<dyn TableProvider>> {
        let parsed = parse_udtf_args(exprs)?;
        let rows = parsed
            .input
            .as_deref()
            .map(|s| flatten_with_options(s, &parsed.options))
            .unwrap_or_default();
        Ok(Arc::new(FlattenJsonTable {
            schema: Arc::clone(&OUTPUT_SCHEMA),
            rows,
        }))
    }
}

struct ParsedUdtfArgs {
    input: Option<String>,
    options: FlattenOptions,
}

fn parse_udtf_args(exprs: &[Expr]) -> DataFusionResult<ParsedUdtfArgs> {
    let mut positional = exprs.iter();
    let mut options = FlattenOptions::default();

    let first = positional.next().ok_or_else(|| {
        DataFusionError::Plan(format!(
            "{FLATTEN_JSON_UDTF_NAME}() requires a JSON string argument."
        ))
    })?;

    let input = literal_string(first).map_err(|e| {
        DataFusionError::NotImplemented(format!(
            "{FLATTEN_JSON_UDTF_NAME}() currently supports a literal JSON string as the \
             first argument. For per-row usage, use the scalar UDF form with UNNEST in \
             SELECT position: `SELECT rows.path, rows.value, rows.type FROM \
             (SELECT UNNEST({FLATTEN_JSON_UDTF_NAME}(<column>)) AS rows FROM <table>)`. \
             Details: {e}"
        ))
    })?;

    for arg in positional {
        let (name, value) = named_arg(arg).ok_or_else(|| {
            DataFusionError::Plan(format!(
                "Arguments after the JSON string must be named, e.g. `max_depth => 32`. Got: {arg:?}."
            ))
        })?;
        apply_named_option(&name, value, &mut options)?;
    }

    Ok(ParsedUdtfArgs { input, options })
}

fn literal_string(expr: &Expr) -> Result<Option<String>, String> {
    match expr {
        Expr::Literal(ScalarValue::Utf8(v) | ScalarValue::LargeUtf8(v), _) => Ok(v.clone()),
        Expr::Literal(ScalarValue::Null, _) => Ok(None),
        other => Err(format!("expected Utf8, got {other:?}")),
    }
}

fn named_arg(expr: &Expr) -> Option<(String, &ScalarValue)> {
    if let Expr::Literal(scalar, Some(meta)) = expr
        && let Some(name) = meta.inner().get("spice.parameter_name")
    {
        return Some((name.clone(), scalar));
    }
    None
}

fn apply_named_option(
    name: &str,
    value: &ScalarValue,
    opts: &mut FlattenOptions,
) -> DataFusionResult<()> {
    match name {
        "max_depth" => opts.max_depth = parse_usize(name, value)?,
        "max_rows" => opts.max_rows = parse_usize(name, value)?,
        "max_bytes" => opts.max_bytes = parse_usize(name, value)?,
        "path_style" => {
            let s = parse_utf8(name, value)?;
            opts.path_style = PathStyle::parse(&s).ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "Unknown path_style '{s}'. Expected 'dot' or 'json-pointer'."
                ))
            })?;
        }
        "include_internal" => opts.include_internal = parse_bool(name, value)?,
        "array_wildcard" => opts.array_wildcard = parse_bool(name, value)?,
        other => {
            return Err(DataFusionError::Plan(format!(
                "Unknown option '{other}'. Supported: max_depth, max_rows, max_bytes, \
                 path_style, include_internal, array_wildcard."
            )));
        }
    }
    Ok(())
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

fn parse_bool(name: &str, v: &ScalarValue) -> DataFusionResult<bool> {
    match v {
        ScalarValue::Boolean(Some(b)) => Ok(*b),
        other => Err(DataFusionError::Plan(format!(
            "{name} must be a boolean, got {other:?}"
        ))),
    }
}

fn parse_utf8(name: &str, v: &ScalarValue) -> DataFusionResult<String> {
    match v {
        ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => Ok(s.clone()),
        other => Err(DataFusionError::Plan(format!(
            "{name} must be a string, got {other:?}"
        ))),
    }
}

#[derive(Debug)]
pub struct FlattenJsonTable {
    schema: SchemaRef,
    rows: Vec<JsonRow>,
}

#[async_trait]
impl TableProvider for FlattenJsonTable {
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

fn rows_to_batch(rows: &[JsonRow], schema: SchemaRef) -> DataFusionResult<RecordBatch> {
    let arrays = build_row_arrays(rows);
    RecordBatch::try_new(schema, arrays).map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
}

fn build_row_arrays(rows: &[JsonRow]) -> Vec<ArrayRef> {
    let mut path = StringBuilder::with_capacity(rows.len(), rows.len() * 16);
    let mut parent_path = StringBuilder::with_capacity(rows.len(), rows.len() * 8);
    let mut key = StringBuilder::with_capacity(rows.len(), rows.len() * 8);
    let mut value = StringBuilder::with_capacity(rows.len(), rows.len() * 16);
    let mut type_name = StringBuilder::with_capacity(rows.len(), rows.len() * 4);

    for row in rows {
        path.append_value(&row.path);
        parent_path.append_value(&row.parent_path);
        key.append_value(&row.key);
        match &row.value {
            Some(v) => value.append_value(v),
            None => value.append_null(),
        }
        type_name.append_value(&row.type_name);
    }

    vec![
        Arc::new(path.finish()),
        Arc::new(parent_path.finish()),
        Arc::new(key.finish()),
        Arc::new(value.finish()),
        Arc::new(type_name.finish()),
    ]
}

// -------- ScalarUDF variant --------

#[derive(Debug, Clone)]
pub struct FlattenJsonScalar {
    signature: Signature,
}

impl Default for FlattenJsonScalar {
    fn default() -> Self {
        Self::new()
    }
}

impl FlattenJsonScalar {
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

impl PartialEq for FlattenJsonScalar {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for FlattenJsonScalar {}

impl std::hash::Hash for FlattenJsonScalar {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

impl ScalarUDFImpl for FlattenJsonScalar {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        FLATTEN_JSON_UDTF_NAME
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
                    "{FLATTEN_JSON_UDTF_NAME}() requires a JSON string argument."
                ))
            })?
            .clone();

        // Named args are stripped of their metadata for scalar invocations;
        // callers needing non-default options should use the UDTF form.
        let opts = FlattenOptions::default();

        let array = input_col.into_array(args.number_rows)?;
        // Normalize to `LargeUtf8` rather than `Utf8` so inputs whose
        // cumulative string bytes exceed the 32-bit offset limit still
        // work (casting `LargeUtf8` -> `Utf8` can fail for truly large
        // inputs even though the UDF signature advertises `LargeUtf8`).
        let normalized = if matches!(array.data_type(), DataType::LargeUtf8) {
            array
        } else {
            cast(&array, &DataType::LargeUtf8)
                .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?
        };
        let strings = as_largestring_array(&normalized);

        let mut all_rows: Vec<JsonRow> = Vec::new();
        let mut offsets: Vec<i64> = Vec::with_capacity(strings.len() + 1);
        offsets.push(0);

        for idx in 0..strings.len() {
            if !strings.is_null(idx) {
                let rows = flatten_with_options(strings.value(idx), &opts);
                all_rows.extend(rows);
                if all_rows.len() > SCALAR_BATCH_MAX_ROWS {
                    record_error("batch_cap_hit");
                    return Err(DataFusionError::Execution(format!(
                        "{FLATTEN_JSON_UDTF_NAME}(): batch produced more than {SCALAR_BATCH_MAX_ROWS} flattened rows; split the input or use the UDTF form with `max_rows => N`."
                    )));
                }
            }
            let len = i64::try_from(all_rows.len()).map_err(|_| {
                DataFusionError::Execution(format!(
                    "{FLATTEN_JSON_UDTF_NAME}(): flattened row count exceeds LargeList i64 offset range."
                ))
            })?;
            offsets.push(len);
        }

        let struct_arrays = build_row_arrays(&all_rows);
        let struct_array = StructArray::new(ROW_FIELDS.clone(), struct_arrays, None);
        let list_array = LargeListArray::new(
            Arc::new(Field::new(
                "item",
                DataType::Struct(ROW_FIELDS.clone()),
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

    fn by_path(rows: &[JsonRow]) -> std::collections::HashMap<&str, &JsonRow> {
        rows.iter().map(|r| (r.path.as_str(), r)).collect()
    }

    #[test]
    fn leaves_only_by_default() {
        let json = r#"{"user":{"name":"Alice","age":30}}"#;
        let rows = flatten(json);
        let by = by_path(&rows);
        assert_eq!(by["user.name"].value.as_deref(), Some("Alice"));
        assert_eq!(by["user.name"].type_name, "string");
        assert_eq!(by["user.age"].value.as_deref(), Some("30"));
        assert_eq!(by["user.age"].type_name, "integer");
        // No interior row for "user" by default.
        assert!(!by.contains_key("user"));
        assert!(!by.contains_key(""));
    }

    #[test]
    fn include_internal_emits_containers() {
        let json = r#"{"user":{"name":"Alice"}}"#;
        let opts = FlattenOptions {
            include_internal: true,
            ..FlattenOptions::default()
        };
        let rows = flatten_with_options(json, &opts);
        let paths: Vec<_> = rows.iter().map(|r| r.path.as_str()).collect();
        // Root is emitted at path "" (empty) with type "object".
        assert!(paths.contains(&""));
        assert!(paths.contains(&"user"));
        assert!(paths.contains(&"user.name"));
        let by = by_path(&rows);
        assert_eq!(by[""].type_name, "object");
        assert_eq!(by["user"].type_name, "object");
        // Container rows serialize their compact JSON into `value`.
        assert_eq!(by["user"].value.as_deref(), Some(r#"{"name":"Alice"}"#));
    }

    #[test]
    fn arrays_use_bracket_indices_by_default() {
        let json = r#"{"tags":["a","b","c"]}"#;
        let rows = flatten(json);
        let by = by_path(&rows);
        assert_eq!(by["tags[0]"].value.as_deref(), Some("a"));
        assert_eq!(by["tags[1]"].value.as_deref(), Some("b"));
        assert_eq!(by["tags[2]"].value.as_deref(), Some("c"));
        assert_eq!(by["tags[0]"].parent_path, "tags");
        assert_eq!(by["tags[0]"].key, "[0]");
    }

    #[test]
    fn array_wildcard_collapses_indices() {
        let json = r#"{"tags":["a","b"]}"#;
        let opts = FlattenOptions {
            array_wildcard: true,
            ..FlattenOptions::default()
        };
        let rows = flatten_with_options(json, &opts);
        // Both elements collapse to the same path.
        let paths: Vec<_> = rows.iter().map(|r| r.path.as_str()).collect();
        assert_eq!(paths, vec!["tags[*]", "tags[*]"]);
        assert_eq!(rows[0].value.as_deref(), Some("a"));
        assert_eq!(rows[1].value.as_deref(), Some("b"));
        assert_eq!(rows[0].key, "[*]");
    }

    #[test]
    fn array_of_objects_emits_leaf_per_field() {
        let json = r#"{"users":[{"id":1},{"id":2}]}"#;
        let rows = flatten(json);
        let paths: Vec<_> = rows.iter().map(|r| r.path.as_str()).collect();
        assert_eq!(paths, vec!["users[0].id", "users[1].id"]);
        assert_eq!(rows[0].value.as_deref(), Some("1"));
        assert_eq!(rows[0].type_name, "integer");
    }

    #[test]
    fn array_wildcard_map_of_array_of_object_shape() {
        let json = r#"{
            "identityMap": {
                "EMAIL": [{"id":"a@b.com","primary":true}],
                "CRM":   [{"id":"x","primary":false}, {"id":"y","primary":true}]
            }
        }"#;
        let opts = FlattenOptions {
            array_wildcard: true,
            ..FlattenOptions::default()
        };
        let rows = flatten_with_options(json, &opts);
        let paths: Vec<_> = rows.iter().map(|r| r.path.as_str()).collect();
        assert!(paths.contains(&"identityMap.EMAIL[*].id"));
        assert!(paths.contains(&"identityMap.CRM[*].id"));
        assert!(paths.contains(&"identityMap.CRM[*].primary"));
        let id_rows = rows
            .iter()
            .filter(|r| {
                r.path
                    .rsplit_once('.')
                    .is_some_and(|(_, suffix)| suffix == "id")
            })
            .count();
        assert_eq!(id_rows, 3);
    }

    #[test]
    fn json_pointer_path_style() {
        let json = r#"{"a":{"b":[true,false]}}"#;
        let opts = FlattenOptions {
            path_style: PathStyle::JsonPointer,
            ..FlattenOptions::default()
        };
        let rows = flatten_with_options(json, &opts);
        let paths: Vec<_> = rows.iter().map(|r| r.path.as_str()).collect();
        assert_eq!(paths, vec!["/a/b/0", "/a/b/1"]);
        assert_eq!(rows[0].parent_path, "/a/b");
        assert_eq!(rows[0].type_name, "boolean");
    }

    #[test]
    fn json_pointer_escapes_tilde_and_slash() {
        let json = r#"{"a/b":1,"c~d":2}"#;
        let opts = FlattenOptions {
            path_style: PathStyle::JsonPointer,
            ..FlattenOptions::default()
        };
        let rows = flatten_with_options(json, &opts);
        let paths: Vec<_> = rows.iter().map(|r| r.path.as_str()).collect();
        assert!(paths.contains(&"/a~1b"));
        assert!(paths.contains(&"/c~0d"));
    }

    #[test]
    fn types_distinguish_integer_and_number() {
        let json = r#"{"i":42,"f":4.2,"s":"x","b":true,"n":null}"#;
        let rows = flatten(json);
        let by = by_path(&rows);
        assert_eq!(by["i"].type_name, "integer");
        assert_eq!(by["f"].type_name, "number");
        assert_eq!(by["s"].type_name, "string");
        assert_eq!(by["b"].type_name, "boolean");
        assert_eq!(by["n"].type_name, "null");
        // JSON null surfaces as SQL NULL in the `value` column.
        assert!(by["n"].value.is_none());
    }

    #[test]
    fn empty_containers_are_surfaced_in_leaves_only_mode() {
        let json = r#"{"empty_obj":{},"empty_arr":[]}"#;
        let rows = flatten(json);
        let by = by_path(&rows);
        assert_eq!(by["empty_obj"].type_name, "object");
        assert_eq!(by["empty_arr"].type_name, "array");
    }

    #[test]
    fn malformed_input_yields_zero_rows() {
        assert!(flatten("not json").is_empty());
        assert!(flatten("{unterminated").is_empty());
    }

    #[test]
    fn max_depth_truncates_walk() {
        let json = r#"{"a":{"b":{"c":{"d":1}}}}"#;
        let opts = FlattenOptions {
            max_depth: 2,
            ..FlattenOptions::default()
        };
        let rows = flatten_with_options(json, &opts);
        let paths: Vec<_> = rows.iter().map(|r| r.path.as_str()).collect();
        assert!(!paths.contains(&"a.b.c.d"));
        assert!(paths.iter().any(|p| p.starts_with("a.b")));
    }

    #[test]
    fn max_rows_caps_output() {
        let json = r#"{"a":1,"b":2,"c":3,"d":4,"e":5}"#;
        let opts = FlattenOptions {
            max_rows: 3,
            ..FlattenOptions::default()
        };
        let rows = flatten_with_options(json, &opts);
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn oversized_input_is_rejected_without_parsing() {
        let opts = FlattenOptions {
            max_bytes: 4,
            ..FlattenOptions::default()
        };
        let rows = flatten_with_options(r#"{"a":1}"#, &opts);
        assert!(rows.is_empty());
    }

    #[test]
    fn scalar_udf_return_type_is_large_list_of_struct() {
        let udf = FlattenJsonScalar::new();
        let return_type = udf
            .return_type(&[DataType::Utf8])
            .expect("return_type should succeed");
        match return_type {
            DataType::LargeList(inner) => {
                assert!(matches!(inner.data_type(), DataType::Struct(_)));
            }
            other => panic!("expected LargeList<Struct<..>>, got {other:?}"),
        }
    }
}
