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

use arrow::array::{
    ArrayRef, BooleanArray, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array,
    Int64Array, RecordBatch, StringArray,
};
use arrow::datatypes::{DataType, SchemaRef};
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
    let mut flat_values: Vec<f32> = Vec::with_capacity(hits.len() * dim as usize);

    for hit in hits {
        if let Some(arr) = extract_field(&hit.source, field_name).and_then(|v| v.as_array()) {
            for val in arr.iter().take(dim as usize) {
                flat_values.push(val.as_f64().unwrap_or(0.0) as f32);
            }
            // Pad if shorter than expected.
            let remaining = dim as usize - arr.len().min(dim as usize);
            flat_values.extend(std::iter::repeat_n(0.0f32, remaining));
        } else {
            flat_values.extend(std::iter::repeat_n(0.0f32, dim as usize));
        }
    }

    let values_array = Arc::new(Float32Array::from(flat_values)) as ArrayRef;
    let list_array = arrow::array::FixedSizeListArray::try_new(
        Arc::new(arrow::datatypes::Field::new(
            "item",
            DataType::Float32,
            false,
        )),
        dim,
        values_array,
        None,
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
