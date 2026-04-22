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

//! [`TableProvider`] that queries an Elasticsearch index.
//!
//! Translates scans into Elasticsearch `_search` requests,
//! converting the JSON hits into Arrow [`RecordBatch`]es.

use std::any::Any;
use std::sync::Arc;

use chrono::DateTime;

use arrow::array::{
    ArrayRef, BooleanArray, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array,
    Int64Array, LargeStringBuilder, ListBuilder, RecordBatch, StringArray, StringBuilder,
    TimestampMicrosecondArray,
};
use arrow::datatypes::{DataType, SchemaRef, TimeUnit};
use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::project_schema;
use datafusion::datasource::TableType;
use datafusion::error::DataFusionError;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::TableProviderFilterPushDown;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
};
use datafusion::prelude::Expr;
use elasticsearch::{Elasticsearch, SearchRequest};

/// A [`TableProvider`] backed by an Elasticsearch index.
///
/// Each scan issues a `_search` request and streams the results as Arrow batches.
#[derive(Debug)]
pub struct ElasticsearchQueryTable {
    client: Arc<dyn Elasticsearch>,
    index: String,
    schema: SchemaRef,
}

impl ElasticsearchQueryTable {
    #[must_use]
    pub fn new(client: Arc<dyn Elasticsearch>, index: String, schema: SchemaRef) -> Self {
        Self {
            client,
            index,
            schema,
        }
    }
}

#[async_trait]
impl TableProvider for ElasticsearchQueryTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> datafusion::error::Result<Vec<TableProviderFilterPushDown>> {
        // For now, we don't push filters down to ES; DataFusion handles filtering.
        Ok(vec![
            TableProviderFilterPushDown::Unsupported;
            filters.len()
        ])
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let projected_schema = project_schema(&self.schema, projection)?;
        Ok(Arc::new(ElasticsearchQueryExec {
            client: Arc::clone(&self.client),
            index: self.index.clone(),
            full_schema: Arc::clone(&self.schema),
            projected_schema,
            projection: projection.cloned(),
            limit,
            properties: PlanProperties::new(
                EquivalenceProperties::new(project_schema(&self.schema, projection)?),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Incremental,
                Boundedness::Bounded,
            ),
        }))
    }
}

#[derive(Debug)]
struct ElasticsearchQueryExec {
    client: Arc<dyn Elasticsearch>,
    index: String,
    full_schema: SchemaRef,
    projected_schema: SchemaRef,
    projection: Option<Vec<usize>>,
    limit: Option<usize>,
    properties: PlanProperties,
}

impl DisplayAs for ElasticsearchQueryExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "ElasticsearchQueryExec: index={}, limit={:?}",
            self.index, self.limit
        )
    }
}

impl ExecutionPlan for ElasticsearchQueryExec {
    fn name(&self) -> &'static str {
        "ElasticsearchQueryExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> datafusion::error::Result<SendableRecordBatchStream> {
        let client = Arc::clone(&self.client);
        let index = self.index.clone();
        let full_schema = Arc::clone(&self.full_schema);
        let projected_schema = Arc::clone(&self.projected_schema);
        let projection = self.projection.clone();
        let limit = self.limit;

        let stream = futures::stream::once(async move {
            let size = limit.unwrap_or(10_000).min(10_000);
            let req = SearchRequest {
                query: Some(elasticsearch::match_all_query()),
                size: Some(size),
                ..Default::default()
            };

            let response = client
                .search(&index, &req)
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

            let batch = hits_to_record_batch(&response.hits.hits, &full_schema)?;

            if let Some(proj) = &projection {
                Ok(batch.project(proj)?)
            } else {
                Ok(batch)
            }
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            projected_schema,
            stream,
        )))
    }
}

/// Convert Elasticsearch hits to an Arrow [`RecordBatch`].
pub fn hits_to_record_batch(
    hits: &[elasticsearch::Hit],
    schema: &SchemaRef,
) -> Result<RecordBatch, DataFusionError> {
    let num_rows = hits.len();
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());

    for field in schema.fields() {
        let array = build_array_from_hits(hits, field.name(), field.data_type(), num_rows)?;
        columns.push(array);
    }

    RecordBatch::try_new(Arc::clone(schema), columns)
        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
}

fn build_array_from_hits(
    hits: &[elasticsearch::Hit],
    field_name: &str,
    data_type: &DataType,
    _num_rows: usize,
) -> Result<ArrayRef, DataFusionError> {
    match data_type {
        DataType::Utf8 => {
            let values: Vec<Option<String>> = hits
                .iter()
                .map(|h| extract_field(&h.source, field_name).map(json_value_to_string))
                .collect();
            Ok(Arc::new(StringArray::from(values)) as ArrayRef)
        }
        DataType::Int64 => {
            let values: Vec<Option<i64>> = hits
                .iter()
                .map(|h| extract_field(&h.source, field_name).and_then(serde_json::Value::as_i64))
                .collect();
            Ok(Arc::new(Int64Array::from(values)) as ArrayRef)
        }
        DataType::Int32 => {
            let values: Vec<Option<i32>> = hits
                .iter()
                .map(|h| {
                    extract_field(&h.source, field_name)
                        .and_then(serde_json::Value::as_i64)
                        .and_then(|n| i32::try_from(n).ok())
                })
                .collect();
            Ok(Arc::new(Int32Array::from(values)) as ArrayRef)
        }
        DataType::Int16 => {
            let values: Vec<Option<i16>> = hits
                .iter()
                .map(|h| {
                    extract_field(&h.source, field_name)
                        .and_then(serde_json::Value::as_i64)
                        .and_then(|n| i16::try_from(n).ok())
                })
                .collect();
            Ok(Arc::new(Int16Array::from(values)) as ArrayRef)
        }
        DataType::Int8 => {
            let values: Vec<Option<i8>> = hits
                .iter()
                .map(|h| {
                    extract_field(&h.source, field_name)
                        .and_then(serde_json::Value::as_i64)
                        .and_then(|n| i8::try_from(n).ok())
                })
                .collect();
            Ok(Arc::new(Int8Array::from(values)) as ArrayRef)
        }
        DataType::Float64 => {
            let values: Vec<Option<f64>> = hits
                .iter()
                .map(|h| extract_field(&h.source, field_name).and_then(serde_json::Value::as_f64))
                .collect();
            Ok(Arc::new(Float64Array::from(values)) as ArrayRef)
        }
        DataType::Float32 => {
            #[expect(clippy::cast_possible_truncation)]
            let values: Vec<Option<f32>> = hits
                .iter()
                .map(|h| {
                    extract_field(&h.source, field_name).and_then(|v| v.as_f64().map(|n| n as f32))
                })
                .collect();
            Ok(Arc::new(Float32Array::from(values)) as ArrayRef)
        }
        DataType::Boolean => {
            let values: Vec<Option<bool>> = hits
                .iter()
                .map(|h| extract_field(&h.source, field_name).and_then(serde_json::Value::as_bool))
                .collect();
            Ok(Arc::new(BooleanArray::from(values)) as ArrayRef)
        }
        DataType::FixedSizeList(_inner_field, dim) => {
            build_dense_vector_array(hits, field_name, *dim)
        }
        // List<Utf8> or List<LargeUtf8>: ES returns JSON arrays of strings.
        // Use `.with_field` so the inner field name matches the schema exactly.
        DataType::List(inner_field)
            if matches!(
                inner_field.data_type(),
                DataType::Utf8 | DataType::LargeUtf8
            ) =>
        {
            if inner_field.data_type() == &DataType::LargeUtf8 {
                let mut builder =
                    ListBuilder::new(LargeStringBuilder::new()).with_field(Arc::clone(inner_field));
                for hit in hits {
                    match extract_field(&hit.source, field_name).and_then(|v| v.as_array()) {
                        Some(arr) => {
                            for val in arr {
                                builder.values().append_option(val.as_str());
                            }
                            builder.append(true);
                        }
                        None => builder.append(false),
                    }
                }
                Ok(Arc::new(builder.finish()) as ArrayRef)
            } else {
                let mut builder =
                    ListBuilder::new(StringBuilder::new()).with_field(Arc::clone(inner_field));
                for hit in hits {
                    match extract_field(&hit.source, field_name).and_then(|v| v.as_array()) {
                        Some(arr) => {
                            for val in arr {
                                builder.values().append_option(val.as_str());
                            }
                            builder.append(true);
                        }
                        None => builder.append(false),
                    }
                }
                Ok(Arc::new(builder.finish()) as ArrayRef)
            }
        }
        DataType::Timestamp(TimeUnit::Microsecond, tz) => {
            // Elasticsearch timestamps are ISO 8601 strings or epoch-millis integers.
            let values: Vec<Option<i64>> = hits
                .iter()
                .map(|h| {
                    let v = extract_field(&h.source, field_name)?;
                    if let Some(ms) = v.as_i64() {
                        // epoch-millis → epoch-microseconds
                        return Some(ms * 1_000);
                    }
                    if let Some(s) = v.as_str() {
                        return DateTime::parse_from_rfc3339(s)
                            .ok()
                            .map(|dt| dt.timestamp_micros());
                    }
                    None
                })
                .collect();
            let arr = TimestampMicrosecondArray::from(values);
            let arr = if let Some(tz_str) = tz {
                arr.with_timezone(tz_str.as_ref())
            } else {
                arr
            };
            Ok(Arc::new(arr) as ArrayRef)
        }
        _ => {
            // Fallback: serialize as JSON string.
            let values: Vec<Option<String>> = hits
                .iter()
                .map(|h| extract_field(&h.source, field_name).map(json_value_to_string))
                .collect();
            Ok(Arc::new(StringArray::from(values)) as ArrayRef)
        }
    }
}

/// Navigate dot-separated field names (e.g. "address.city") into a JSON value.
fn extract_field<'a>(
    source: &'a serde_json::Value,
    field_name: &str,
) -> Option<&'a serde_json::Value> {
    let mut current = source;
    for part in field_name.split('.') {
        current = current.get(part)?;
    }
    Some(current)
}

#[expect(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
fn build_dense_vector_array(
    hits: &[elasticsearch::Hit],
    field_name: &str,
    dim: i32,
) -> Result<ArrayRef, DataFusionError> {
    let dim_usize = dim as usize;
    let mut flat_values: Vec<f32> = Vec::with_capacity(hits.len() * dim_usize);
    let mut null_mask: Vec<bool> = Vec::with_capacity(hits.len());

    for hit in hits {
        if let Some(arr) = extract_field(&hit.source, field_name).and_then(|v| v.as_array()) {
            if arr.len() != dim_usize {
                return Err(DataFusionError::Execution(format!(
                    "dense_vector field '{field_name}' has {len} elements, expected {dim_usize}",
                    len = arr.len(),
                )));
            }
            for val in arr {
                let f = val.as_f64().ok_or_else(|| {
                    DataFusionError::Execution(format!(
                        "dense_vector field '{field_name}' contains a non-numeric element"
                    ))
                })? as f32;
                flat_values.push(f);
            }
            null_mask.push(true);
        } else {
            // Missing vector -> NULL row; fill placeholder values.
            flat_values.extend(std::iter::repeat_n(0.0f32, dim_usize));
            null_mask.push(false);
        }
    }

    let values_array = Arc::new(Float32Array::from(flat_values)) as ArrayRef;
    let nulls = arrow::buffer::NullBuffer::from(null_mask);
    let list_array = arrow::array::FixedSizeListArray::try_new(
        Arc::new(arrow::datatypes::Field::new(
            "item",
            DataType::Float32,
            false,
        )),
        dim,
        values_array,
        Some(nulls),
    )
    .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;

    Ok(Arc::new(list_array) as ArrayRef)
}

fn json_value_to_string(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        Array, LargeStringArray, ListArray, StringArray, TimestampMicrosecondArray,
    };
    use arrow::datatypes::{Field, Schema, TimeUnit};
    use elasticsearch::Hit;
    use serde_json::json;

    fn make_hit(source: serde_json::Value) -> Hit {
        Hit {
            id: "id".to_string(),
            score: Some(1.0),
            source,
        }
    }

    // ── Timestamp ──────────────────────────────────────────────────────────────

    #[test]
    fn test_timestamp_iso8601_parses_correctly() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "created_at",
            DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC"))),
            true,
        )]));
        let hits = vec![
            make_hit(json!({"created_at": "2024-01-15T10:30:00Z"})),
            make_hit(json!({})), // missing → null
        ];
        let batch = hits_to_record_batch(&hits, &schema).expect("hits_to_record_batch failed");
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .expect("column 0 should be TimestampMicrosecondArray");

        assert!(!col.is_null(0));
        assert!(col.is_null(1));
        let expected = chrono::DateTime::parse_from_rfc3339("2024-01-15T10:30:00Z")
            .expect("valid RFC 3339 literal")
            .timestamp_micros();
        assert_eq!(col.value(0), expected);
    }

    #[test]
    fn test_timestamp_epoch_millis_parses_correctly() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC"))),
            true,
        )]));
        let epoch_ms: i64 = 1_705_311_000_000;
        let hits = vec![make_hit(json!({"ts": epoch_ms}))];
        let batch = hits_to_record_batch(&hits, &schema).expect("hits_to_record_batch failed");
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .expect("column 0 should be TimestampMicrosecondArray");

        assert_eq!(col.value(0), epoch_ms * 1_000);
    }

    // ── List<LargeUtf8> ────────────────────────────────────────────────────────

    #[test]
    fn test_list_large_utf8_builds_correct_array() {
        let inner = Arc::new(Field::new("element", DataType::LargeUtf8, true));
        let schema = Arc::new(Schema::new(vec![Field::new(
            "sentences",
            DataType::List(Arc::clone(&inner)),
            true,
        )]));
        let hits = vec![
            make_hit(json!({"sentences": ["hello", "world"]})),
            make_hit(json!({})),                // missing → null
            make_hit(json!({"sentences": []})), // empty list → non-null empty
        ];
        let batch = hits_to_record_batch(&hits, &schema).expect("hits_to_record_batch failed");
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<ListArray>()
            .expect("column 0 should be ListArray");

        assert!(!col.is_null(0));
        assert!(col.is_null(1));
        assert!(!col.is_null(2));

        let row0 = col.value(0);
        let strings = row0
            .as_any()
            .downcast_ref::<LargeStringArray>()
            .expect("list values should be LargeStringArray");
        assert_eq!(strings.len(), 2);
        assert_eq!(strings.value(0), "hello");
        assert_eq!(strings.value(1), "world");

        assert_eq!(col.value(2).len(), 0);
    }

    // ── List<Utf8> ─────────────────────────────────────────────────────────────

    #[test]
    fn test_list_utf8_builds_correct_array() {
        let inner = Arc::new(Field::new("item", DataType::Utf8, true));
        let schema = Arc::new(Schema::new(vec![Field::new(
            "tags",
            DataType::List(Arc::clone(&inner)),
            true,
        )]));
        let hits = vec![make_hit(json!({"tags": ["a", "b", "c"]}))];
        let batch = hits_to_record_batch(&hits, &schema).expect("hits_to_record_batch failed");
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<ListArray>()
            .expect("column 0 should be ListArray");

        let row0 = col.value(0);
        let strings = row0
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("list values should be StringArray");
        assert_eq!(strings.len(), 3);
        assert_eq!(strings.value(0), "a");
        assert_eq!(strings.value(1), "b");
        assert_eq!(strings.value(2), "c");
    }
}
