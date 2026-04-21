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

//! `flatten_json_properties` — decompose a JSON-Schema-shaped document into one
//! row per field. See issue #10399 for the full specification.
//!
//! ```text
//! flatten_json_properties(input Utf8 [, options...]) -> TABLE(
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
//! Two entry points are registered:
//!
//! - **UDTF** (`register_udtf`) — accepts a literal JSON string and any number
//!   of named options. Use in the `FROM` clause:
//!   `SELECT * FROM flatten_json_properties('{...}')`.
//! - **Scalar UDF** (`register_udf`) — accepts a `Utf8` column and returns
//!   `List<Struct<...>>`. Use with `UNNEST` for per-row / LATERAL semantics:
//!   `FROM schemas s, UNNEST(flatten_json_properties(s.body)) AS a`.
//!
//! The walker handles:
//! - `properties` recursion (object → nested objects).
//! - `items.properties` (arrays of objects; leaves appear at `array.field`).
//! - `additionalProperties` maps (the map field emits `type = "map"`, children
//!   appear at `map.child`).
//! - `allOf`, `oneOf`, `anyOf` merge — fields from every branch are emitted;
//!   duplicate names across branches are deduped.
//! - Local `$ref` pointers (`#/$defs/*`, `#/definitions/*`, `#/properties/*`)
//!   with cycle detection.
//! - External `$ref` URIs — emitted as `type = "ref"`, never dereferenced (no IO).
//!
//! Options (passed as named arguments):
//! - `max_depth` (`UInt`, default `32`) — walk stops past this depth.
//! - `max_rows` (`UInt`, default `100_000`) — per-document row cap.
//! - `max_bytes` (`UInt`, default `8_388_608`) — input size limit.
//! - `dialect` (`Utf8`, `"json-schema"` | `"openapi"`, default `"json-schema"`) —
//!   tags invocation metrics so operators can split `openapi` traffic from
//!   `json-schema` traffic. The walker does not currently vary its behavior
//!   based on dialect; `OpenAPI`-specific handling (e.g. `nullable: true`) is
//!   future scope tracked with the rest of this UDTF.
//! - `include_internal` (`Bool`, default `false`) — include container rows.
//! - `path_style` (`Utf8`, `"dot"` | `"json-pointer"`, default `"dot"`).
//!
//! Telemetry: the walker emits OpenTelemetry counters
//! `flatten_json_properties_invocations_total`,
//! `flatten_json_properties_rows_emitted_total`, and
//! `flatten_json_properties_errors_total{kind}`. Malformed input or a hit
//! depth / row / size limit emits an error-kind metric and yields zero or a
//! truncated-but-valid batch — never a query-level error.

use std::any::Any;
use std::collections::HashSet;
use std::fmt::{Debug, Formatter};
use std::sync::{Arc, LazyLock};

use arrow::array::{
    Array, ArrayRef, BooleanBuilder, LargeListArray, ListBuilder, StringBuilder, StructArray,
    as_string_array,
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

pub const FLATTEN_JSON_PROPERTIES_UDTF_NAME: &str = "flatten_json_properties";

/// Default caps. Configurable per-call via named args.
const DEFAULT_MAX_DEPTH: usize = 32;
const DEFAULT_MAX_ROWS: usize = 100_000;
const DEFAULT_MAX_BYTES: usize = 8 * 1024 * 1024;

/// Scalar UDF ceiling across a single evaluated batch. Per-document caps
/// already bound individual rows, but a wide input batch could still
/// accumulate `number_rows * max_rows` entries in memory. Error out loudly
/// past this watermark so operators see the condition rather than OOM.
const SCALAR_BATCH_MAX_ROWS: usize = 10_000_000;

// -------- Metrics --------

static METER: LazyLock<Meter> = LazyLock::new(|| global::meter("flatten_json_properties"));

static INVOCATIONS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("flatten_json_properties_invocations_total")
        .with_description("Invocations of flatten_json_properties, labelled by dialect.")
        .build()
});

static ROWS_EMITTED: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("flatten_json_properties_rows_emitted_total")
        .with_description("Total rows emitted by flatten_json_properties.")
        .build()
});

static ERRORS: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("flatten_json_properties_errors_total")
        .with_description(
            "Errors inside flatten_json_properties, labelled by kind \
             (parse|depth_exceeded|row_cap_hit|cycle|input_too_large).",
        )
        .build()
});

fn record_error(kind: &'static str) {
    ERRORS.add(1, &[KeyValue::new("kind", kind)]);
}

// -------- Output schema --------

static PROPERTY_FIELDS: LazyLock<Fields> = LazyLock::new(|| {
    let enum_item = Arc::new(Field::new("item", DataType::Utf8, true));
    Fields::from(vec![
        Field::new("path", DataType::Utf8, false),
        Field::new("parent_path", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("description", DataType::Utf8, true),
        Field::new("type", DataType::Utf8, false),
        Field::new("required", DataType::Boolean, false),
        Field::new("format", DataType::Utf8, true),
        Field::new("enum_values", DataType::List(enum_item), true),
        Field::new("metadata", DataType::Utf8, true),
    ])
});

static OUTPUT_SCHEMA: LazyLock<SchemaRef> =
    LazyLock::new(|| Arc::new(Schema::new(PROPERTY_FIELDS.clone())));

/// Return type of the scalar UDF form. Uses `LargeList` (i64 offsets)
/// instead of `List` so a large batch can't overflow the offset range and
/// silently drop rows. `UNNEST` works on both variants, so the change is
/// transparent to downstream SQL.
static ROW_LIST_TYPE: LazyLock<DataType> = LazyLock::new(|| {
    DataType::LargeList(Arc::new(Field::new(
        "item",
        DataType::Struct(PROPERTY_FIELDS.clone()),
        true,
    )))
});

// -------- Row + Options --------

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

/// Dialect tag carried through options. Currently only affects the metric
/// label on `flatten_json_properties_invocations_total`; walker behavior does
/// not yet diverge. Retained so callers (and metrics) can distinguish traffic
/// when dialect-specific behavior lands later.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Dialect {
    JsonSchema,
    OpenApi,
}

impl Dialect {
    fn label(self) -> &'static str {
        match self {
            Self::JsonSchema => "json-schema",
            Self::OpenApi => "openapi",
        }
    }
    fn parse(s: &str) -> Option<Self> {
        match s {
            "json-schema" | "jsonschema" => Some(Self::JsonSchema),
            "openapi" => Some(Self::OpenApi),
            _ => None,
        }
    }
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
    pub dialect: Dialect,
    pub include_internal: bool,
    pub path_style: PathStyle,
}

impl Default for FlattenOptions {
    fn default() -> Self {
        Self {
            max_depth: DEFAULT_MAX_DEPTH,
            max_rows: DEFAULT_MAX_ROWS,
            max_bytes: DEFAULT_MAX_BYTES,
            dialect: Dialect::JsonSchema,
            include_internal: false,
            path_style: PathStyle::Dot,
        }
    }
}

// -------- Public entry points --------

/// Walk with default options. See [`flatten_with_options`] for configurable caps.
#[must_use]
pub fn flatten(input: &str) -> Vec<PropertyRow> {
    flatten_with_options(input, &FlattenOptions::default())
}

/// Walk a JSON-Schema-shaped document and return one [`PropertyRow`] per
/// reachable field. Never errors: returns an empty / truncated `Vec` for
/// malformed input or caps being hit, emitting the corresponding metric.
#[must_use]
pub fn flatten_with_options(input: &str, opts: &FlattenOptions) -> Vec<PropertyRow> {
    INVOCATIONS.add(1, &[KeyValue::new("dialect", opts.dialect.label())]);

    if input.len() > opts.max_bytes {
        record_error("input_too_large");
        return Vec::new();
    }

    let Ok(root) = serde_json::from_str::<Value>(input) else {
        record_error("parse");
        return Vec::new();
    };

    let mut walker = Walker::new(&root, opts);
    // Capture the root lifetime as a free variable so `walk_schema` sees it as
    // `&'a Value` — letting ref resolution return `&'a Value` without cloning.
    let root_ref: &Value = &root;
    walker.walk_schema(root_ref, "", 0);
    ROWS_EMITTED.add(walker.rows.len() as u64, &[]);
    walker.rows
}

// -------- Walker --------

struct Walker<'a> {
    root: &'a Value,
    opts: &'a FlattenOptions,
    rows: Vec<PropertyRow>,
    /// Active `$ref` pointers on the walk stack, for cycle detection.
    visited_refs: HashSet<String>,
    depth_cap_hit: bool,
    row_cap_hit: bool,
}

impl<'a> Walker<'a> {
    fn new(root: &'a Value, opts: &'a FlattenOptions) -> Self {
        Self {
            root,
            opts,
            rows: Vec::new(),
            visited_refs: HashSet::new(),
            depth_cap_hit: false,
            row_cap_hit: false,
        }
    }

    fn walk_schema(&mut self, schema: &'a Value, parent_path: &str, depth: usize) {
        if self.check_caps(depth) {
            return;
        }
        let effective = self.effective_schemas(schema);

        // `collect_effective` handles cycles during a single resolution
        // pass, but once control returns here we recurse into the resolved
        // schema's own children — any `$ref` back to this node would look
        // "fresh" to the next `collect_effective` call. Re-insert the ref
        // (if there was one) so the whole walk-chain sees it.
        let chain_ref: Option<String> = schema
            .get("$ref")
            .and_then(Value::as_str)
            .filter(|r| is_local_ref(r))
            .and_then(|r| self.visited_refs.insert(r.to_owned()).then(|| r.to_owned()));

        let required: HashSet<&str> = effective
            .iter()
            .flat_map(|s| {
                s.get("required")
                    .and_then(Value::as_array)
                    .into_iter()
                    .flatten()
            })
            .filter_map(Value::as_str)
            .collect();

        let mut seen_names: HashSet<&str> = HashSet::new();
        for eff in &effective {
            if let Some(properties) = eff.get("properties").and_then(Value::as_object) {
                for (name, spec) in properties {
                    if !seen_names.insert(name.as_str()) {
                        continue;
                    }
                    self.handle_field(
                        name,
                        spec,
                        parent_path,
                        required.contains(name.as_str()),
                        depth,
                    );
                    if self.row_cap_hit {
                        if let Some(r) = chain_ref {
                            self.visited_refs.remove(&r);
                        }
                        return;
                    }
                }
            }
        }

        if let Some(r) = chain_ref {
            self.visited_refs.remove(&r);
        }
    }

    fn handle_field(
        &mut self,
        name: &str,
        spec: &'a Value,
        parent_path: &str,
        required: bool,
        depth: usize,
    ) {
        let path = make_path(parent_path, name, self.opts.path_style);
        let effective_specs = self.effective_schemas(spec);

        let type_name = effective_specs
            .iter()
            .map(|s| compute_type(s))
            .find(|t| t != "unknown")
            .unwrap_or_else(|| "unknown".to_owned());

        let is_container = matches!(type_name.as_str(), "object" | "array" | "map");
        let emit_container_now = !is_container || self.opts.include_internal;
        if emit_container_now {
            self.emit_row(
                spec,
                &effective_specs,
                &path,
                parent_path,
                name,
                &type_name,
                required,
            );
            if self.row_cap_hit {
                return;
            }
        }

        // Recurse once on the original `spec`; `walk_schema` re-expands
        // effective branches internally with a single `seen_names` set, so
        // overlapping `properties` across allOf/oneOf/anyOf / $ref branches
        // are de-duplicated rather than emitted once per branch.
        let rows_before = self.rows.len();
        match type_name.as_str() {
            "object" => {
                self.walk_schema(spec, &path, depth + 1);
            }
            "array" => {
                // Items may itself be typed / composite; reuse walk_schema at
                // the same path so leaves appear as `array.child` rather than
                // `array[].child`. Look across effective branches so `items`
                // declared under a combinator is still found, but use the
                // first matching `items` as the single recursion point.
                if let Some(items) = effective_specs
                    .iter()
                    .find_map(|s| s.get("items"))
                    .filter(|v| v.is_object())
                {
                    self.walk_schema(items, &path, depth + 1);
                }
            }
            "map" => {
                if let Some(ap) = effective_specs
                    .iter()
                    .find_map(|s| s.get("additionalProperties"))
                    .filter(|v| v.is_object())
                {
                    self.walk_schema(ap, &path, depth + 1);
                }
            }
            _ => {}
        }

        // Leaf-only mode would otherwise drop container fields whose children
        // are primitives (array of strings, map of ints, empty object). If
        // the recursion produced nothing and we haven't already emitted the
        // container, surface it now so the field still appears in the output.
        if is_container && !emit_container_now && self.rows.len() == rows_before {
            self.emit_row(
                spec,
                &effective_specs,
                &path,
                parent_path,
                name,
                &type_name,
                required,
            );
        }
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "internal helper threads per-row metadata; splitting into a struct adds noise without clarity"
    )]
    fn emit_row(
        &mut self,
        raw_spec: &'a Value,
        effective: &[&'a Value],
        path: &str,
        parent_path: &str,
        name: &str,
        type_name: &str,
        required: bool,
    ) {
        // `effective` contains the raw_spec when no $ref was followed, and only
        // the resolved target(s) when one was. The `or_else` arm preserves
        // description / format / enum annotations declared alongside a $ref
        // (JSON Schema 2020-12 lets them coexist; earlier drafts ignored them).
        let description = first_str(effective, "description")
            .or_else(|| raw_spec.get("description").and_then(Value::as_str))
            .map(ToOwned::to_owned);

        let format = first_str(effective, "format")
            .or_else(|| raw_spec.get("format").and_then(Value::as_str))
            .map(ToOwned::to_owned);

        let enum_values = effective
            .iter()
            .find_map(|s| s.get("enum").and_then(Value::as_array))
            .or_else(|| raw_spec.get("enum").and_then(Value::as_array))
            .map(|arr| {
                arr.iter()
                    .map(|v| match v {
                        Value::String(s) => s.clone(),
                        _ => v.to_string(),
                    })
                    .collect::<Vec<_>>()
            });

        self.rows.push(PropertyRow {
            path: path.to_owned(),
            parent_path: parent_path.to_owned(),
            name: name.to_owned(),
            description,
            type_name: type_name.to_owned(),
            required,
            format,
            enum_values,
            metadata: Some(raw_spec.to_string()),
        });

        if self.rows.len() >= self.opts.max_rows {
            self.row_cap_hit = true;
            record_error("row_cap_hit");
        }
    }

    /// Resolve `$ref`, `allOf`, `oneOf`, `anyOf` into the list of contributing
    /// schemas. External and unresolvable refs pass through as-is so callers
    /// can still read shape metadata from them.
    fn effective_schemas(&mut self, schema: &'a Value) -> Vec<&'a Value> {
        let mut out = Vec::new();
        self.collect_effective(schema, &mut out, 0);
        if out.is_empty() {
            out.push(schema);
        }
        out
    }

    /// `ref_depth` tracks how deep we've recursed through `$ref` and
    /// `allOf`/`oneOf`/`anyOf` expansion at a single schema node. Capped at
    /// `opts.max_depth` so pathological combinator / ref chains can't blow the
    /// stack or iterate unboundedly (`DoS`).
    fn collect_effective(&mut self, schema: &'a Value, out: &mut Vec<&'a Value>, ref_depth: usize) {
        if ref_depth > self.opts.max_depth {
            if !self.depth_cap_hit {
                self.depth_cap_hit = true;
                record_error("depth_exceeded");
            }
            out.push(schema);
            return;
        }
        if let Some(ref_str) = schema.get("$ref").and_then(Value::as_str) {
            if is_local_ref(ref_str) {
                if self.visited_refs.contains(ref_str) {
                    record_error("cycle");
                    return;
                }
                // Copy out `self.root: &'a Value` (references are Copy) so the
                // returned `Option<&'a Value>` survives past `self`'s borrow.
                let root: &'a Value = self.root;
                if let Some(target) = root.pointer(ref_str.trim_start_matches('#')) {
                    self.visited_refs.insert(ref_str.to_owned());
                    self.collect_effective(target, out, ref_depth + 1);
                    self.visited_refs.remove(ref_str);
                    return;
                }
                // Local ref that doesn't resolve — fall through and treat the
                // schema itself as the contribution.
            }
            // External ref — `compute_type` will classify as `ref`; the URI
            // remains in `metadata`. Never dereferenced.
            out.push(schema);
            return;
        }

        out.push(schema);
        for comb in ["allOf", "oneOf", "anyOf"] {
            if let Some(arr) = schema.get(comb).and_then(Value::as_array) {
                for entry in arr {
                    self.collect_effective(entry, out, ref_depth + 1);
                }
            }
        }
    }

    fn check_caps(&mut self, depth: usize) -> bool {
        if depth > self.opts.max_depth {
            if !self.depth_cap_hit {
                self.depth_cap_hit = true;
                record_error("depth_exceeded");
            }
            return true;
        }
        if self.rows.len() >= self.opts.max_rows {
            if !self.row_cap_hit {
                self.row_cap_hit = true;
                record_error("row_cap_hit");
            }
            return true;
        }
        false
    }
}

// -------- Helpers --------

fn first_str<'a>(schemas: &[&'a Value], key: &str) -> Option<&'a str> {
    schemas
        .iter()
        .find_map(|s| s.get(key).and_then(Value::as_str))
}

fn is_local_ref(ref_str: &str) -> bool {
    ref_str.starts_with('#')
}

/// Classify a schema node into one of the emitted type labels.
fn compute_type(spec: &Value) -> String {
    // External $ref → "ref"
    if let Some(ref_str) = spec.get("$ref").and_then(Value::as_str)
        && !is_local_ref(ref_str)
    {
        return "ref".to_owned();
    }
    // Explicit additionalProperties without own properties → map.
    let has_ap = spec
        .get("additionalProperties")
        .is_some_and(Value::is_object);
    // Require `properties` / `items` to be well-formed before treating the node
    // as an object/array. A non-object `properties` or a non-object/array
    // `items` shouldn't silently flip the type.
    let has_props = spec.get("properties").is_some_and(Value::is_object);
    if has_ap && !has_props {
        return "map".to_owned();
    }
    match spec.get("type") {
        Some(Value::String(s)) => s.clone(),
        Some(Value::Array(arr)) => {
            // Type unions with `"null"` express optional/nullable in JSON
            // Schema; the "real" type is the first non-null entry. Only fall
            // back to `"null"` (or `"unknown"`) when no other type is present.
            let strs: Vec<&str> = arr.iter().filter_map(Value::as_str).collect();
            strs.iter()
                .find(|t| **t != "null")
                .copied()
                .or_else(|| strs.first().copied())
                .unwrap_or("unknown")
                .to_owned()
        }
        _ => {
            if has_props {
                "object".to_owned()
            } else if spec
                .get("items")
                .is_some_and(|v| v.is_object() || v.is_array())
            {
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

fn make_path(parent: &str, name: &str, style: PathStyle) -> String {
    match style {
        PathStyle::Dot => {
            if parent.is_empty() {
                name.to_owned()
            } else {
                format!("{parent}.{name}")
            }
        }
        PathStyle::JsonPointer => {
            // Escape `/` and `~` per RFC 6901.
            let escaped = name.replace('~', "~0").replace('/', "~1");
            if parent.is_empty() {
                format!("/{escaped}")
            } else {
                format!("{parent}/{escaped}")
            }
        }
    }
}

// -------- UDTF --------

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
        let parsed = parse_udtf_args(exprs)?;
        let rows = parsed
            .input
            .as_deref()
            .map(|s| flatten_with_options(s, &parsed.options))
            .unwrap_or_default();
        Ok(Arc::new(FlattenJsonPropertiesTable {
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
            "{FLATTEN_JSON_PROPERTIES_UDTF_NAME}() requires a JSON string argument."
        ))
    })?;

    let input = literal_string(first).map_err(|e| {
        DataFusionError::NotImplemented(format!(
            "{FLATTEN_JSON_PROPERTIES_UDTF_NAME}() currently supports a literal JSON string as the \
             first argument. For per-row / LATERAL invocation, use \
             `UNNEST({FLATTEN_JSON_PROPERTIES_UDTF_NAME}(<column>))`. Details: {e}"
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

/// Extract a Utf8/LargeUtf8 string literal. Returns `Ok(None)` for NULL.
fn literal_string(expr: &Expr) -> Result<Option<String>, String> {
    match expr {
        Expr::Literal(ScalarValue::Utf8(v) | ScalarValue::LargeUtf8(v), _) => Ok(v.clone()),
        Expr::Literal(ScalarValue::Null, _) => Ok(None),
        other => Err(format!("expected Utf8, got {other:?}")),
    }
}

/// Recognise a `name => value` named-argument expression. `DataFusion` surfaces
/// these as a literal tagged with `spice.parameter_name` metadata.
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
        "dialect" => {
            let s = parse_utf8(name, value)?;
            opts.dialect = Dialect::parse(&s).ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "Unknown dialect '{s}'. Expected 'json-schema' or 'openapi'."
                ))
            })?;
        }
        "include_internal" => opts.include_internal = parse_bool(name, value)?,
        "path_style" => {
            let s = parse_utf8(name, value)?;
            opts.path_style = PathStyle::parse(&s).ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "Unknown path_style '{s}'. Expected 'dot' or 'json-pointer'."
                ))
            })?;
        }
        other => {
            return Err(DataFusionError::Plan(format!(
                "Unknown option '{other}'. Supported: max_depth, max_rows, max_bytes, dialect, \
                 include_internal, path_style."
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
pub struct FlattenJsonPropertiesTable {
    schema: SchemaRef,
    rows: Vec<PropertyRow>,
}

#[async_trait]
impl TableProvider for FlattenJsonPropertiesTable {
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

fn rows_to_batch(rows: &[PropertyRow], schema: SchemaRef) -> DataFusionResult<RecordBatch> {
    let (arrays, _) = build_property_arrays(rows);
    RecordBatch::try_new(schema, arrays).map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
}

fn build_property_arrays(rows: &[PropertyRow]) -> (Vec<ArrayRef>, usize) {
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

    let arrays: Vec<ArrayRef> = vec![
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
    (arrays, rows.len())
}

// -------- ScalarUDF variant --------
//
// Exposes the same walker as a scalar that returns `List<Struct<...>>` per row.
// Composes with `UNNEST` to give per-row / LATERAL semantics.

#[derive(Debug, Clone)]
pub struct FlattenJsonPropertiesScalar {
    signature: Signature,
}

impl Default for FlattenJsonPropertiesScalar {
    fn default() -> Self {
        Self::new()
    }
}

impl FlattenJsonPropertiesScalar {
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

impl PartialEq for FlattenJsonPropertiesScalar {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for FlattenJsonPropertiesScalar {}

impl std::hash::Hash for FlattenJsonPropertiesScalar {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.name().hash(state);
    }
}

impl ScalarUDFImpl for FlattenJsonPropertiesScalar {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        FLATTEN_JSON_PROPERTIES_UDTF_NAME
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
                    "{FLATTEN_JSON_PROPERTIES_UDTF_NAME}() requires a JSON string argument."
                ))
            })?
            .clone();

        // Named args are stripped of their metadata when the scalar form is
        // invoked; users who want non-default options should use the UDTF form.
        let opts = FlattenOptions::default();

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

        // Collect all rows into a single flat vec, with offsets delineating
        // which span belongs to which input row. NULL inputs produce empty
        // (but non-NULL) list slots so the output row count matches input.
        //
        // Using `LargeListArray` (i64 offsets) so a large evaluated batch
        // cannot overflow and silently drop rows. Per-document caps inside
        // `flatten_with_options` still bound memory use.
        let mut all_rows: Vec<PropertyRow> = Vec::new();
        let mut offsets: Vec<i64> = Vec::with_capacity(strings.len() + 1);
        offsets.push(0);

        for idx in 0..strings.len() {
            if !strings.is_null(idx) {
                let rows = flatten_with_options(strings.value(idx), &opts);
                all_rows.extend(rows);
                if all_rows.len() > SCALAR_BATCH_MAX_ROWS {
                    record_error("batch_cap_hit");
                    return Err(DataFusionError::Execution(format!(
                        "{FLATTEN_JSON_PROPERTIES_UDTF_NAME}(): batch produced more than {SCALAR_BATCH_MAX_ROWS} flattened rows; lower `max_rows` or split the input."
                    )));
                }
            }
            // Walker caps bound the row count well under `i64::MAX`, but if
            // somehow they didn't, silently saturating would misalign list
            // offsets. Fail loud instead so the condition is visible.
            let len = i64::try_from(all_rows.len()).map_err(|_| {
                DataFusionError::Execution(format!(
                    "{FLATTEN_JSON_PROPERTIES_UDTF_NAME}(): flattened row count exceeds LargeList i64 offset range."
                ))
            })?;
            offsets.push(len);
        }

        let (struct_arrays, _) = build_property_arrays(&all_rows);
        let struct_array = StructArray::new(PROPERTY_FIELDS.clone(), struct_arrays, None);
        let list_array = LargeListArray::new(
            Arc::new(Field::new(
                "item",
                DataType::Struct(PROPERTY_FIELDS.clone()),
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

    fn by_path(rows: &[PropertyRow]) -> std::collections::HashMap<&str, &PropertyRow> {
        rows.iter().map(|r| (r.path.as_str(), r)).collect()
    }

    fn with_internal() -> FlattenOptions {
        FlattenOptions {
            include_internal: true,
            ..FlattenOptions::default()
        }
    }

    #[test]
    fn leaves_only_by_default() {
        let json = r#"{
            "properties": {
                "user": {
                    "type": "object",
                    "properties": {
                        "name": {"type": "string"}
                    }
                }
            }
        }"#;
        let rows = flatten(json);
        // "user" is a container; by default containers are not emitted.
        let paths: Vec<_> = rows.iter().map(|r| r.path.as_str()).collect();
        assert_eq!(paths, vec!["user.name"]);
    }

    #[test]
    fn include_internal_emits_containers() {
        let json = r#"{
            "properties": {
                "user": {
                    "type": "object",
                    "properties": {
                        "name": {"type": "string"}
                    }
                }
            }
        }"#;
        let rows = flatten_with_options(json, &with_internal());
        let by = by_path(&rows);
        assert_eq!(by["user"].type_name, "object");
        assert_eq!(by["user.name"].type_name, "string");
    }

    #[test]
    fn flat_primitives_with_required() {
        let json = r#"{
            "properties": {
                "name": {"type": "string", "description": "User's full name"},
                "age":  {"type": "integer"}
            },
            "required": ["name"]
        }"#;
        let rows = flatten(json);
        let by = by_path(&rows);
        assert!(by["name"].required);
        assert_eq!(by["name"].description.as_deref(), Some("User's full name"));
        assert!(!by["age"].required);
    }

    #[test]
    fn items_properties_of_object_arrays() {
        let json = r#"{
            "properties": {
                "orders": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "id":   {"type": "integer"},
                            "name": {"type": "string"}
                        },
                        "required": ["id"]
                    }
                }
            }
        }"#;
        let rows = flatten(json);
        let by = by_path(&rows);
        assert_eq!(by["orders.id"].type_name, "integer");
        assert!(by["orders.id"].required);
        assert_eq!(by["orders.name"].type_name, "string");
        // Array container itself is not emitted by default.
        assert!(!by.contains_key("orders"));
    }

    #[test]
    fn additional_properties_map() {
        let json = r#"{
            "properties": {
                "labels": {
                    "type": "object",
                    "additionalProperties": {
                        "type": "object",
                        "properties": {"value": {"type": "string"}}
                    }
                }
            }
        }"#;
        let rows = flatten_with_options(json, &with_internal());
        let by = by_path(&rows);
        assert_eq!(by["labels"].type_name, "map");
        // Child properties under additionalProperties are emitted at labels.value.
        assert_eq!(by["labels.value"].type_name, "string");
    }

    #[test]
    fn all_of_merges_fields() {
        let json = r#"{
            "properties": {
                "user": {
                    "allOf": [
                        {"properties": {"name": {"type": "string"}},
                         "required": ["name"]},
                        {"properties": {"age":  {"type": "integer"}}}
                    ]
                }
            }
        }"#;
        let rows = flatten(json);
        let by = by_path(&rows);
        assert!(by["user.name"].required);
        assert_eq!(by["user.age"].type_name, "integer");
    }

    #[test]
    fn nullable_type_union_picks_non_null() {
        // JSON Schema expresses nullable fields as `"type": ["null", "string"]`
        // (or any ordering). Pick the first non-null type so the output row
        // reflects the real type rather than `"null"`.
        let json = r#"{
            "properties": {
                "leading_null":  {"type": ["null", "string"]},
                "trailing_null": {"type": ["integer", "null"]},
                "all_null":      {"type": ["null"]}
            }
        }"#;
        let rows = flatten(json);
        let by = by_path(&rows);
        assert_eq!(by["leading_null"].type_name, "string");
        assert_eq!(by["trailing_null"].type_name, "integer");
        assert_eq!(by["all_null"].type_name, "null");
    }

    #[test]
    fn one_of_any_of_union_fields() {
        let json = r#"{
            "properties": {
                "payload": {
                    "oneOf": [
                        {"properties": {"text":  {"type": "string"}}},
                        {"properties": {"count": {"type": "integer"}}}
                    ]
                }
            }
        }"#;
        let rows = flatten(json);
        let by = by_path(&rows);
        assert!(by.contains_key("payload.text"));
        assert!(by.contains_key("payload.count"));
    }

    #[test]
    fn local_ref_resolves() {
        let json = r##"{
            "$defs": {
                "Address": {"type": "object", "properties": {"street": {"type": "string"}}}
            },
            "properties": {
                "home": {"$ref": "#/$defs/Address"}
            }
        }"##;
        let rows = flatten(json);
        let by = by_path(&rows);
        assert_eq!(by["home.street"].type_name, "string");
    }

    #[test]
    fn local_ref_cycle_terminates() {
        let json = r##"{
            "$defs": {
                "Node": {
                    "type": "object",
                    "properties": {
                        "next": {"$ref": "#/$defs/Node"}
                    }
                }
            },
            "properties": {
                "root": {"$ref": "#/$defs/Node"}
            }
        }"##;
        let rows = flatten(json);
        let by = by_path(&rows);
        // First resolution of Node happens at `root`; the second hop into
        // `root.next` must recognise it's re-entering the same `$ref` chain
        // and stop without a third level of expansion.
        assert!(by.contains_key("root.next"));
        assert!(!by.contains_key("root.next.next"));
    }

    #[test]
    fn external_ref_emits_ref_type_row() {
        let json = r#"{
            "properties": {
                "ext": {"$ref": "https://example.com/schema.json"}
            }
        }"#;
        let rows = flatten_with_options(json, &with_internal());
        let by = by_path(&rows);
        assert_eq!(by["ext"].type_name, "ref");
        let meta: serde_json::Value =
            serde_json::from_str(by["ext"].metadata.as_ref().unwrap()).unwrap();
        assert_eq!(meta["$ref"], "https://example.com/schema.json");
    }

    #[test]
    fn enum_and_format_are_captured() {
        let json = r#"{
            "properties": {
                "status":     {"type": "string", "enum": ["active", "pending"]},
                "created_at": {"type": "string", "format": "date-time"}
            }
        }"#;
        let rows = flatten(json);
        let by = by_path(&rows);
        assert_eq!(
            by["status"].enum_values.as_deref(),
            Some(&["active".to_string(), "pending".to_string()][..])
        );
        assert_eq!(by["created_at"].format.as_deref(), Some("date-time"));
    }

    #[test]
    fn malformed_input_yields_zero_rows() {
        assert!(flatten("not json").is_empty());
        assert!(flatten("{broken").is_empty());
        assert!(flatten("").is_empty());
    }

    #[test]
    fn oversized_input_is_rejected_without_parsing() {
        let opts = FlattenOptions {
            max_bytes: 32,
            ..FlattenOptions::default()
        };
        let big = r#"{"properties": {"a": {"type": "string"}, "b": {"type": "integer"}}}"#;
        assert!(big.len() > 32);
        assert!(flatten_with_options(big, &opts).is_empty());
    }

    #[test]
    fn max_depth_truncates_walk() {
        let opts = FlattenOptions {
            max_depth: 2,
            include_internal: true,
            ..FlattenOptions::default()
        };
        let json = r#"{
            "properties": {
                "a": {"type": "object", "properties": {
                    "b": {"type": "object", "properties": {
                        "c": {"type": "object", "properties": {
                            "d": {"type": "string"}
                        }}
                    }}
                }}
            }
        }"#;
        let rows = flatten_with_options(json, &opts);
        let paths: Vec<_> = rows.iter().map(|r| r.path.as_str()).collect();
        // We saw up to depth 2 (a.b); "a.b.c" lives at depth 3 which is capped.
        // Exact path set depends on when the cap trips, so we only assert that
        // the deepest path ("a.b.c.d") is absent.
        assert!(!paths.contains(&"a.b.c.d"));
    }

    #[test]
    fn max_rows_caps_output() {
        let opts = FlattenOptions {
            max_rows: 2,
            ..FlattenOptions::default()
        };
        let json = r#"{
            "properties": {
                "a": {"type": "string"},
                "b": {"type": "string"},
                "c": {"type": "string"},
                "d": {"type": "string"}
            }
        }"#;
        let rows = flatten_with_options(json, &opts);
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn json_pointer_path_style() {
        let opts = FlattenOptions {
            path_style: PathStyle::JsonPointer,
            ..FlattenOptions::default()
        };
        let json = r#"{
            "properties": {
                "user": {"type": "object", "properties": {
                    "name": {"type": "string"}
                }}
            }
        }"#;
        let rows = flatten_with_options(json, &opts);
        let paths: Vec<_> = rows.iter().map(|r| r.path.as_str()).collect();
        assert_eq!(paths, vec!["/user/name"]);
    }

    #[test]
    fn documents_without_properties_yield_zero_rows() {
        assert!(flatten(r#"{"foo": "bar"}"#).is_empty());
        assert!(flatten(r#"{"properties": {}}"#).is_empty());
        assert!(flatten(r#"[1, 2, 3]"#).is_empty());
    }

    #[tokio::test]
    async fn udtf_emits_schema_and_batch() {
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

        let state = ctx.state();
        let plan = provider.scan(&state, None, &[], None).await.expect("scan");
        let results = datafusion::physical_plan::collect(plan, ctx.task_ctx())
            .await
            .expect("collect");
        assert_eq!(results[0].num_rows(), 1);
    }

    #[test]
    fn udtf_rejects_non_literal_first_arg() {
        use datafusion::common::Column;
        let func = FlattenJsonPropertiesTableFunc::new();
        let err = func
            .call(&[Expr::Column(Column::new_unqualified("body"))])
            .expect_err("column argument must be rejected");
        assert!(err.to_string().contains("UNNEST"));
    }

    #[test]
    fn scalar_udf_return_type_is_large_list_of_struct() {
        let udf = FlattenJsonPropertiesScalar::new();
        let ty = udf.return_type(&[DataType::Utf8]).expect("return type");
        match ty {
            DataType::LargeList(field) => {
                assert!(matches!(field.data_type(), DataType::Struct(_)));
            }
            other => panic!("expected LargeList<Struct>, got {other:?}"),
        }
    }

    #[test]
    fn scalar_udf_invokes_per_row() {
        use arrow::array::StringArray;

        let udf = FlattenJsonPropertiesScalar::new();
        let input = Arc::new(StringArray::from(vec![
            Some(r#"{"properties":{"a":{"type":"string"}}}"#),
            Some(r#"{"properties":{"b":{"type":"integer"},"c":{"type":"boolean"}}}"#),
            None,
        ])) as ArrayRef;

        let arg_field = Arc::new(Field::new("body", DataType::Utf8, true));
        let return_field = Arc::new(Field::new("result", ROW_LIST_TYPE.clone(), true));

        let result = udf
            .invoke_with_args(ScalarFunctionArgs {
                args: vec![ColumnarValue::Array(input)],
                arg_fields: vec![arg_field],
                number_rows: 3,
                return_field,
                config_options: Arc::new(datafusion::config::ConfigOptions::default()),
            })
            .expect("invoke succeeds");

        let arr = match result {
            ColumnarValue::Array(a) => a,
            other => panic!("expected array, got {other:?}"),
        };
        let list = arr
            .as_any()
            .downcast_ref::<arrow::array::LargeListArray>()
            .expect("large-list array");
        assert_eq!(list.len(), 3);
        // Row 0 has 1 flattened property; row 1 has 2; row 2 is NULL-valued but
        // still emits an (empty) list slot per row.
        assert_eq!(list.value(0).len(), 1);
        assert_eq!(list.value(1).len(), 2);
    }
}
