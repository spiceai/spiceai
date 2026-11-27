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

//! Exact extraction of the values under a JSON object's top-level keys, the
//! foundation of storing a JSON column's frequent keys as columns of their own.
//!
//! A query such as `json_get_str(attributes, 'url.path')` parses every row's whole
//! document to find one key. If the text of that key's value is stored beside the
//! document, the same query reads `json_get_str(url_path_value)` instead, a short
//! string that is often the same few values. That rewrite is exact only if
//! `f(document, key, rest...)` equals `f(value_text, rest...)` for every function
//! `f` it applies to and every document, which [`top_level_value`] guarantees by
//! walking the document the way those functions do:
//!
//! - the document must be an object; the first key equal to `key` in document
//!   order is the match, so a duplicate key later in the document is ignored;
//! - keys compare after their escapes are decoded, so `"url"` matches `url`;
//! - the walk stops at the match: whatever follows it, malformed or not, is not
//!   read, by the functions or here.
//!
//! The value text runs from the value's first byte to its last when it parses. When
//! it does not, the functions still read a prefix of it (`json_get_int(doc, 'k', 0)`
//! of `{"k": [1, oops]}` is `1`), so the text is then the rest of the document from
//! the value's first byte, from which they read exactly the same prefix.
//!
//! A key that parses as an integer is never extracted: those functions use such a
//! key as an index when the document is an array, which a top-level value cannot
//! reproduce.

mod adapter;
mod rewrite;

use std::ops::Range;
use std::sync::{Arc, LazyLock};

use arrow::array::builder::{NullBufferBuilder, make_view};
use arrow::array::{
    Array, ArrayRef, AsArray, ByteView, RecordBatch, RecordBatchOptions, StringViewArray,
    StringViewBuilder,
};
use arrow::datatypes::{DataType, Field, FieldRef, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::common::config::ConfigOptions;
use datafusion::common::{Result, ScalarValue, exec_err, internal_datafusion_err, plan_err};
use datafusion::logical_expr::{
    ColumnarValue, Expr, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};
use datafusion::physical_expr::ScalarFunctionExpr;
use datafusion::physical_expr::expressions::{Column, Literal};
use datafusion::physical_expr::projection::{ProjectionExpr, ProjectionExprs};
use datafusion::physical_plan::{ExecutionPlan, PhysicalExpr};
use jiter::{Jiter, Peek};

pub(crate) use adapter::JsonKeyExprAdapterFactory;
pub use rewrite::{CayenneJsonKeyRewrite, JsonKeyScanResolver, JsonKeyScanTable};

/// The text of the value under top-level `key` in `json`, as the `json_get_*`
/// functions find it; `None` when they find nothing: `json` is not an object, it
/// has no such key, or it fails to parse before the key is reached.
///
/// See the module documentation for why `f(json, key, rest...)` equals
/// `f(value, rest...)`, and why `key` must not parse as an integer.
#[must_use]
pub(crate) fn top_level_value<'j>(json: &'j str, key: &str) -> Option<&'j str> {
    top_level_value_range(json, key).and_then(|range| json.get(range))
}

/// Where [`top_level_value`] lies in `json`, as a byte range on char boundaries.
fn top_level_value_range(json: &str, key: &str) -> Option<Range<usize>> {
    debug_assert!(
        is_shreddable_key(key),
        "an integer key is an array index to json_get_*"
    );
    let mut jiter = Jiter::new(json.as_bytes());
    if jiter.peek().ok()? != Peek::Object {
        return None;
    }
    let mut next_key = jiter.known_object().ok()??;
    while next_key != key {
        jiter.next_skip().ok()?;
        next_key = jiter.next_key().ok()??;
    }
    let peek = jiter.peek().ok()?;
    let start = jiter.current_index();
    let end = match jiter.known_skip(peek) {
        Ok(()) => jiter.current_index(),
        Err(_) => json.len(),
    };
    // jiter stops on ASCII bytes, so both ends are on char boundaries; `get`
    // checks rather than trusts it.
    json.get(start..end).map(|_| start..end)
}

/// Prefix of the name of every column Cayenne derives from a JSON column's key.
const JSON_KEY_COLUMN_PREFIX: &str = "__cayenne_json_key";

/// The name of the column holding [`top_level_value`] of `key` in JSON column
/// `json_column`: what a scan calls the column it derives, and what a data file
/// calls the column that stores those values for its rows.
///
/// Distinct `(json_column, key)` pairs never share a name: the column's byte
/// length precedes it, so where the column ends and the key starts is fixed.
#[must_use]
pub(crate) fn hidden_column_name(json_column: &str, key: &str) -> String {
    format!(
        "{JSON_KEY_COLUMN_PREFIX}:{}:{json_column}:{key}",
        json_column.len()
    )
}

/// A column a Cayenne scan derives from one of its table's JSON columns: per row,
/// the text of the value under one top-level key ([`top_level_value`]), as
/// `Utf8View`, NULL where the key is not found.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct JsonKeyColumn {
    /// Index of the JSON column in the table's schema.
    source: usize,
    /// Name of the JSON column, which the scan checks against its own schema.
    source_name: String,
    key: String,
    name: String,
}

impl JsonKeyColumn {
    /// The values of `key` in the JSON column `source_name` at `source`, or `None`
    /// for a key that parses as an integer (see [`is_shreddable_key`]).
    #[must_use]
    pub(crate) fn new(source: usize, source_name: &str, key: &str) -> Option<Self> {
        is_shreddable_key(key).then(|| Self {
            source,
            source_name: source_name.to_string(),
            key: key.to_string(),
            name: hidden_column_name(source_name, key),
        })
    }

    /// Index of the JSON column in the table's schema.
    #[must_use]
    pub fn source(&self) -> usize {
        self.source
    }

    /// Name of the JSON column.
    #[must_use]
    pub fn source_name(&self) -> &str {
        &self.source_name
    }

    /// The top-level key.
    #[must_use]
    pub fn key(&self) -> &str {
        &self.key
    }

    /// The column's name in a scan's output.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The column's field in a scan's output.
    #[must_use]
    pub fn field(&self) -> FieldRef {
        Arc::new(Field::new(&self.name, DataType::Utf8View, true))
    }

    /// Whether `schema` (a table's schema) holds this column's JSON column at its
    /// index, as a string column.
    #[must_use]
    pub(crate) fn matches(&self, schema: &Schema) -> bool {
        schema.fields().get(self.source).is_some_and(|field| {
            field.name() == &self.source_name && is_json_text_type(field.data_type())
        })
    }

    /// `__cayenne_json_top_level_value(<json column>, '<key>')` over `schema`,
    /// where the JSON column is at [`Self::source`].
    fn computed(&self, schema: &Schema) -> Result<Arc<dyn PhysicalExpr>> {
        let json: Arc<dyn PhysicalExpr> = Arc::new(Column::new(&self.source_name, self.source));
        let key: Arc<dyn PhysicalExpr> =
            Arc::new(Literal::new(ScalarValue::Utf8(Some(self.key.clone()))));
        Ok(Arc::new(ScalarFunctionExpr::try_new(
            Arc::clone(&JSON_TOP_LEVEL_VALUE_UDF),
            vec![json, key],
            schema,
            Arc::new(ConfigOptions::default()),
        )?))
    }
}

/// Whether a column of `data_type` can hold the JSON documents
/// [`top_level_value`] reads.
pub(crate) fn is_json_text_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Utf8 | DataType::Utf8View | DataType::LargeUtf8
    )
}

/// A table whose scans can return [`JsonKeyColumn`]s after the table columns they
/// project: Cayenne, or a table that reads it in its place.
#[async_trait]
pub trait JsonKeyColumnScan: Send + Sync {
    /// Whether a scan planned now would return the key columns itself. When it
    /// would not, a rewrite to key columns only adds work: the caller computes
    /// them from the JSON columns.
    fn serves_json_keys(&self) -> bool;

    /// Plans a scan whose output is the table columns `projection` selects, in
    /// that order, then one column per entry of `keys`, named and typed as
    /// [`JsonKeyColumn::field`]. `None` when this table cannot produce them now.
    ///
    /// # Errors
    ///
    /// Any error planning the scan.
    async fn scan_with_json_keys(
        &self,
        state: &dyn Session,
        projection: &[usize],
        keys: &[JsonKeyColumn],
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>>;
}

/// `schema`'s fields selected by `projection`, then `keys`' fields.
pub(crate) fn schema_with_json_keys(
    schema: &Schema,
    projection: Option<&[usize]>,
    keys: &[JsonKeyColumn],
) -> Result<SchemaRef> {
    let mut fields: Vec<FieldRef> = match projection {
        Some(projection) => projection
            .iter()
            .map(|&index| {
                schema.fields().get(index).cloned().ok_or_else(|| {
                    internal_datafusion_err!(
                        "Projection index {index} is out of bounds for a schema of {} fields",
                        schema.fields().len()
                    )
                })
            })
            .collect::<Result<_>>()?,
        None => schema.fields().iter().cloned().collect(),
    };
    fields.extend(keys.iter().map(JsonKeyColumn::field));
    Ok(Arc::new(Schema::new_with_metadata(
        fields,
        schema.metadata().clone(),
    )))
}

/// The projection a file scan pushes into its source for `keys`: the columns of
/// `table_schema` (the source's table schema) that `projection` selects, then each
/// key column computed from its JSON column, then the `trailing` columns. The
/// scan's expression adapter ([`JsonKeyExprAdapterFactory`]) reads a file's stored
/// values in place of a computed key column when the file has them.
pub(crate) fn file_projection_with_json_keys(
    table_schema: &Schema,
    projection: &[usize],
    keys: &[JsonKeyColumn],
    trailing: &[usize],
) -> Result<ProjectionExprs> {
    let mut exprs: Vec<ProjectionExpr> = ProjectionExprs::from_indices(projection, table_schema)
        .iter()
        .cloned()
        .collect();
    for key in keys {
        if !key.matches(table_schema) {
            return exec_err!(
                "Cannot read key '{}' of JSON column '{}': the scanned table has no such string column at index {}",
                key.key,
                key.source_name,
                key.source
            );
        }
        exprs.push(ProjectionExpr::new(
            key.computed(table_schema)?,
            key.name.clone(),
        ));
    }
    exprs.extend(
        ProjectionExprs::from_indices(trailing, table_schema)
            .iter()
            .cloned(),
    );
    Ok(ProjectionExprs::new(exprs))
}

/// `projected` (the columns a scan selected from `source`) followed by `keys`,
/// computed from `source`, the same rows with every table column.
pub(crate) fn with_json_key_columns(
    source: &RecordBatch,
    projected: &RecordBatch,
    keys: &[JsonKeyColumn],
    schema: &SchemaRef,
) -> Result<RecordBatch> {
    let rows = projected.num_rows();
    let mut columns = projected.columns().to_vec();
    for key in keys {
        let source_matches = source
            .schema_ref()
            .fields()
            .get(key.source)
            .is_some_and(|field| field.name() == &key.source_name);
        let Some(json) = source.columns().get(key.source).filter(|_| source_matches) else {
            return exec_err!(
                "Cannot read key '{}' of JSON column '{}': the in-memory rows have no such column at index {}",
                key.key,
                key.source_name,
                key.source
            );
        };
        columns.push(top_level_values(json.as_ref(), &key.key)?);
    }
    Ok(RecordBatch::try_new_with_options(
        Arc::clone(schema),
        columns,
        &RecordBatchOptions::new().with_row_count(Some(rows)),
    )?)
}

/// Whether `key` can be extracted with [`top_level_value`]: a key that parses as
/// an integer is an array index to the `json_get_*` functions.
#[must_use]
pub(crate) fn is_shreddable_key(key: &str) -> bool {
    key.parse::<isize>().is_err()
}

/// Name of [`JsonTopLevelValue`] in plans. Not registered with any session, so SQL
/// cannot call it: Cayenne builds it into the plans it rewrites.
pub(crate) const JSON_TOP_LEVEL_VALUE_NAME: &str = "__cayenne_json_top_level_value";

/// `__cayenne_json_top_level_value(json, key)`: [`top_level_value`] over a column,
/// as text the `json_get_*` functions read exactly as they read `key` in `json`.
/// `key` must be a non-NULL string literal that does not parse as an integer.
#[derive(Debug, PartialEq, Eq, Hash)]
pub(crate) struct JsonTopLevelValue;

static JSON_TOP_LEVEL_VALUE_SIGNATURE: LazyLock<Signature> = LazyLock::new(|| {
    Signature::one_of(
        [DataType::Utf8, DataType::Utf8View, DataType::LargeUtf8]
            .into_iter()
            .map(|json| TypeSignature::Exact(vec![json, DataType::Utf8]))
            .collect(),
        Volatility::Immutable,
    )
});

/// The shared [`ScalarUDF`] for [`JsonTopLevelValue`].
pub(crate) static JSON_TOP_LEVEL_VALUE_UDF: LazyLock<Arc<ScalarUDF>> =
    LazyLock::new(|| Arc::new(ScalarUDF::new_from_impl(JsonTopLevelValue)));

impl ScalarUDFImpl for JsonTopLevelValue {
    fn name(&self) -> &str {
        JSON_TOP_LEVEL_VALUE_NAME
    }

    fn signature(&self) -> &Signature {
        &JSON_TOP_LEVEL_VALUE_SIGNATURE
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8View)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [json, key] = args.args.as_slice() else {
            return exec_err!("{JSON_TOP_LEVEL_VALUE_NAME} takes a JSON column and a key");
        };
        let ColumnarValue::Scalar(ScalarValue::Utf8(Some(key))) = key else {
            return plan_err!(
                "{JSON_TOP_LEVEL_VALUE_NAME} takes its key as a non-NULL string literal"
            );
        };
        if !is_shreddable_key(key) {
            return plan_err!(
                "{JSON_TOP_LEVEL_VALUE_NAME} cannot extract key '{key}': a key that parses as an integer is an array index to the JSON functions"
            );
        }
        match json {
            ColumnarValue::Array(array) => {
                Ok(ColumnarValue::Array(top_level_values(array.as_ref(), key)?))
            }
            ColumnarValue::Scalar(scalar) => {
                let array = scalar.to_array_of_size(1)?;
                let values = top_level_values(array.as_ref(), key)?;
                Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                    &values, 0,
                )?))
            }
        }
    }
}

/// [`top_level_value`] of every row of a string array, as a string view array.
fn top_level_values(json: &dyn Array, key: &str) -> Result<ArrayRef> {
    if let Some(views) = json.as_string_view_opt()
        && let Some(values) = top_level_values_in_place(views, key)
    {
        return Ok(Arc::new(values));
    }
    let mut out = StringViewBuilder::with_capacity(json.len());
    macro_rules! extract {
        ($array:expr) => {{
            for doc in $array.iter() {
                out.append_option(doc.and_then(|doc| top_level_value(doc, key)));
            }
        }};
    }
    match json.data_type() {
        DataType::Utf8 => extract!(json.as_string::<i32>()),
        DataType::LargeUtf8 => extract!(json.as_string::<i64>()),
        DataType::Utf8View => extract!(json.as_string_view()),
        other => {
            return exec_err!("{JSON_TOP_LEVEL_VALUE_NAME} takes a string column, not {other}");
        }
    }
    Ok(Arc::new(out.finish()))
}

/// [`top_level_values`] of a string view array without copying a value: each
/// value is a substring of its document, so its view points into the document's
/// own buffer. `None` when a value lies beyond what a view can address (an offset
/// past `u32::MAX`), and the caller copies instead.
fn top_level_values_in_place(json: &StringViewArray, key: &str) -> Option<StringViewArray> {
    let mut views: Vec<u128> = Vec::with_capacity(json.len());
    let mut nulls = NullBufferBuilder::new(json.len());
    for (row, &view) in json.views().iter().enumerate() {
        let document = (!json.is_null(row)).then(|| json.value(row));
        let Some((document, range)) = document
            .and_then(|document| top_level_value_range(document, key).map(|r| (document, r)))
        else {
            views.push(0);
            nulls.append_null();
            continue;
        };
        let value = document.get(range.clone())?.as_bytes();
        // A value longer than a view's inline capacity lies in a document longer
        // than that, which the view keeps in one of the array's data buffers.
        let value_view = if value.len() <= MAX_INLINE_VIEW_BYTES {
            make_view(value, 0, 0)
        } else {
            let view = ByteView::from(view);
            let offset = u64::from(view.offset).checked_add(u64::try_from(range.start).ok()?)?;
            make_view(value, view.buffer_index, u32::try_from(offset).ok()?)
        };
        views.push(value_view);
        nulls.append_non_null();
    }
    StringViewArray::try_new(views.into(), json.data_buffers().to_vec(), nulls.finish()).ok()
}

/// The longest string a view stores inline rather than in a data buffer.
const MAX_INLINE_VIEW_BYTES: usize = 12;

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::StringArray;
    use arrow::datatypes::{Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion::datasource::MemTable;
    use datafusion::prelude::SessionContext;

    /// A small seeded generator, so a failure names the seed that reproduces it.
    struct Rng(u64);

    impl Rng {
        fn next(&mut self) -> u64 {
            // xorshift64*
            self.0 ^= self.0 >> 12;
            self.0 ^= self.0 << 25;
            self.0 ^= self.0 >> 27;
            self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
        }

        fn below(&mut self, n: usize) -> usize {
            usize::try_from(self.next() % u64::try_from(n).expect("n fits")).expect("fits")
        }

        fn pick<'a, T>(&mut self, items: &'a [T]) -> &'a T {
            &items[self.below(items.len())]
        }
    }

    /// Key spellings, including escaped forms of the keys the tests extract, so a
    /// match must compare decoded keys.
    const KEYS: [&str; 9] = [
        r#""a""#,
        r#""a""#,
        r#""url.path""#,
        r#""url.path""#,
        r#""dup""#,
        r#""ké""#,
        r#""ké""#,
        r#""b""#,
        r#""0""#,
    ];

    fn value(rng: &mut Rng, depth: u32) -> String {
        let choice = if depth > 2 {
            rng.below(8)
        } else {
            rng.below(11)
        };
        match choice {
            0 => r#""plain""#.to_string(),
            1 => r#""esc\"aped\\ é \n""#.to_string(),
            2 => (*rng.pick(&[
                "0",
                "-7",
                "200",
                "9223372036854775807",
                "9223372036854775808",
            ]))
            .to_string(),
            3 => (*rng.pick(&["1.5", "-0.0", "1e5", "2E-3", "1.0"])).to_string(),
            4 => "true".to_string(),
            5 => "false".to_string(),
            6 => "null".to_string(),
            7 => r#""/api/v1/items""#.to_string(),
            8 => {
                let n = rng.below(4);
                let items: Vec<String> = (0..n).map(|_| value(rng, depth + 1)).collect();
                format!("[{}]", items.join(", "))
            }
            _ => object(rng, depth + 1),
        }
    }

    fn object(rng: &mut Rng, depth: u32) -> String {
        let n = rng.below(5);
        let ws = *rng.pick(&["", " ", "\n\t "]);
        let members: Vec<String> = (0..n)
            .map(|_| format!("{ws}{}{ws}:{ws}{}", rng.pick(&KEYS), value(rng, depth)))
            .collect();
        format!("{{{}{ws}}}", members.join(","))
    }

    /// A document: usually an object, sometimes another JSON value, truncated, with
    /// garbage after it, or not JSON at all.
    fn document(rng: &mut Rng) -> Option<String> {
        let doc = match rng.below(20) {
            0 => return None,
            1 => String::new(),
            2 => value(rng, 0),
            3 => "not json".to_string(),
            4 => format!("{} trailing", object(rng, 0)),
            5 | 6 => {
                let full = object(rng, 0);
                let cut = rng.below(full.len() + 1);
                // Cut on a char boundary.
                let cut = (0..=cut)
                    .rev()
                    .find(|&i| full.is_char_boundary(i))
                    .unwrap_or(0);
                full[..cut].to_string()
            }
            7 => {
                // A malformed value inside an otherwise well-formed object.
                r#"{"a": [1, 2, oops], "url.path": "x"}"#.to_string()
            }
            _ => object(rng, 0),
        };
        Some(doc)
    }

    const EXTRACTED_KEYS: [&str; 5] = ["a", "url.path", "dup", "ké", "missing"];
    const FUNCTIONS: [&str; 9] = [
        "json_get_str",
        "json_get_int",
        "json_get_float",
        "json_get_bool",
        "json_get_json",
        "json_as_text",
        "json_length",
        "json_get",
        "json_get_array",
    ];
    const REST_PATHS: [&str; 5] = ["", ", 'a'", ", 0", ", 'a', 0", ", 'b'"];

    /// `f(document, key, rest...)` equals `f(top_level_value(document, key), rest...)`
    /// for every function, key and remaining path, over generated documents that
    /// include escaped and duplicate keys, nested and malformed values, non-object
    /// documents, truncated documents and trailing garbage.
    #[tokio::test]
    async fn extracting_a_key_leaves_every_json_function_s_answer_unchanged() {
        // Answers that are not NULL, per function: the comparison must not be
        // vacuous, two NULLs agree whatever the extraction did.
        let mut answered = std::collections::BTreeMap::<&str, usize>::new();
        for seed in [1_u64, 0x5eed, 20_261_127] {
            let mut rng = Rng(seed);
            let docs: Vec<Option<String>> = (0..1500).map(|_| document(&mut rng)).collect();
            let ctx = SessionContext::new();
            let mut ctx_state = ctx.state();
            datafusion_functions_json::register_all(&mut ctx_state)
                .expect("register json functions");
            let ctx = SessionContext::new_with_state(ctx_state);
            // Test-only: the function is never registered outside Cayenne's plans.
            ctx.register_udf(ScalarUDF::clone(&JSON_TOP_LEVEL_VALUE_UDF));
            for key in EXTRACTED_KEYS {
                let values: StringArray = docs
                    .iter()
                    .map(|d| d.as_deref().and_then(|d| top_level_value(d, key)))
                    .collect();
                let docs_array: StringArray = docs.iter().map(Option::as_deref).collect();
                let schema = Arc::new(Schema::new(vec![
                    Field::new("doc", DataType::Utf8, true),
                    Field::new("val", DataType::Utf8, true),
                ]));
                let batch = RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![
                        Arc::new(docs_array) as ArrayRef,
                        Arc::new(values) as ArrayRef,
                    ],
                )
                .expect("batch");
                let table = MemTable::try_new(schema, vec![vec![batch]]).expect("table");
                ctx.deregister_table("t").expect("deregister");
                ctx.register_table("t", Arc::new(table)).expect("register");
                for f in FUNCTIONS {
                    for rest in REST_PATHS {
                        let sql = format!(
                            "SELECT doc, {f}(doc, '{key}'{rest}) AS whole, {f}(val{rest}) AS extracted, \
                             {f}({JSON_TOP_LEVEL_VALUE_NAME}(doc, '{key}'){rest}) AS via_udf, \
                             {f}({JSON_TOP_LEVEL_VALUE_NAME}(arrow_cast(doc, 'Utf8View'), '{key}'){rest}) AS via_views \
                             FROM t"
                        );
                        let batches = ctx
                            .sql(&sql)
                            .await
                            .unwrap_or_else(|e| panic!("plan {sql}: {e}"))
                            .collect()
                            .await
                            .unwrap_or_else(|e| panic!("run {sql}: {e}"));
                        for b in &batches {
                            for row in 0..b.num_rows() {
                                let whole =
                                    ScalarValue::try_from_array(b.column(1), row).expect("whole");
                                let extracted = ScalarValue::try_from_array(b.column(2), row)
                                    .expect("extracted");
                                if !whole.is_null() {
                                    *answered.entry(f).or_default() += 1;
                                }
                                let via_udf =
                                    ScalarValue::try_from_array(b.column(3), row).expect("via_udf");
                                assert_eq!(
                                    whole, via_udf,
                                    "seed {seed}: {f}(doc, '{key}'{rest}) differs through {JSON_TOP_LEVEL_VALUE_NAME}"
                                );
                                // Over string views the values are views into the
                                // documents' own buffers rather than copies.
                                let via_views = ScalarValue::try_from_array(b.column(4), row)
                                    .expect("via_views");
                                assert_eq!(
                                    whole, via_views,
                                    "seed {seed}: {f}(doc, '{key}'{rest}) differs through {JSON_TOP_LEVEL_VALUE_NAME} over string views"
                                );
                                assert_eq!(
                                    whole,
                                    extracted,
                                    "seed {seed}: {f}(doc, '{key}'{rest}) differs from {f}(value{rest}) for doc {:?}",
                                    b.column(0).as_any().downcast_ref::<StringArray>().map(|a| {
                                        (!a.is_null(row)).then(|| a.value(row).to_string())
                                    })
                                );
                            }
                        }
                    }
                }
            }
        }
        // Every function answered, many times over, so the comparisons were not vacuous.
        for f in FUNCTIONS {
            let n = answered.get(f).copied().unwrap_or(0);
            assert!(
                n >= 200,
                "{f} answered only {n} times: the documents exercise too little"
            );
        }
    }

    #[test]
    fn values_of_string_views_point_into_the_documents() {
        let documents = StringViewArray::from(vec![
            Some(r#"{"k": "short"}"#),
            Some(r#"{"other": 1, "k": "a value longer than twelve bytes", "z": 2}"#),
            None,
            Some(r#"{"k": 1}"#),
            Some("[1, 2, 3, 4, 5, 6, 7, 8, 9]"),
        ]);
        let values = top_level_values_in_place(&documents, "k").expect("views into the documents");
        let expected: Vec<Option<&str>> = vec![
            Some(r#""short""#),
            Some(r#""a value longer than twelve bytes""#),
            None,
            Some("1"),
            None,
        ];
        assert_eq!(values.iter().collect::<Vec<_>>(), expected);
        // No value was copied: the result holds the documents' buffers, and the
        // long value's view points into one of them.
        assert_eq!(values.data_buffers().len(), documents.data_buffers().len());
        for (value, document) in values.data_buffers().iter().zip(documents.data_buffers()) {
            assert_eq!(value.as_ptr(), document.as_ptr());
        }
    }

    #[test]
    fn the_value_is_the_first_match_after_decoding_keys() {
        let doc = r#"{"dup": 1, "url.path": "/a", "dup": 2, "x": {"y": [1, 2]}}"#;
        assert_eq!(top_level_value(doc, "dup"), Some("1"));
        assert_eq!(top_level_value(doc, "url.path"), Some(r#""/a""#));
        assert_eq!(top_level_value(doc, "x"), Some(r#"{"y": [1, 2]}"#));
        assert_eq!(top_level_value(doc, "missing"), None);
        assert_eq!(top_level_value(r#"["a"]"#, "a"), None);
        assert_eq!(top_level_value("", "a"), None);
        // A malformed value: the rest of the document, which the functions read a
        // prefix of exactly as they would in place.
        assert_eq!(
            top_level_value(r#"{"a": [1, oops], "b": 2}"#, "a"),
            Some(r#"[1, oops], "b": 2}"#)
        );
        assert!(!is_shreddable_key("0") && !is_shreddable_key("-3"));
        assert!(is_shreddable_key("url.path") && is_shreddable_key("0x1"));
    }
}
