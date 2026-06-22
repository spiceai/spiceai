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

use std::sync::Arc;

use chrono::DateTime;

use arrow::array::{
    ArrayRef, BooleanArray, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array,
    Int64Array, LargeStringBuilder, ListBuilder, RecordBatch, StringArray, StringBuilder,
    TimestampMicrosecondArray, UInt64Array,
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

const ELASTICSEARCH_PAGE_SIZE: usize = 10_000;
const ELASTICSEARCH_PIT_KEEP_ALIVE: &str = "1m";

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
        if limit == Some(0) {
            return Ok(Arc::new(datafusion::physical_plan::empty::EmptyExec::new(
                projected_schema,
            )));
        }

        Ok(Arc::new(ElasticsearchQueryExec {
            client: Arc::clone(&self.client),
            index: self.index.clone(),
            full_schema: Arc::clone(&self.schema),
            projected_schema,
            projection: projection.cloned(),
            limit,
            properties: Arc::new(PlanProperties::new(
                EquivalenceProperties::new(project_schema(&self.schema, projection)?),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Incremental,
                Boundedness::Bounded,
            )),
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
    properties: Arc<PlanProperties>,
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

    fn properties(&self) -> &Arc<PlanProperties> {
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

        let stream = futures::stream::try_unfold(
            ElasticsearchScanState::new(client, index, full_schema, projection, limit),
            |mut state| async move {
                state
                    .next_batch()
                    .await
                    .map(|batch| batch.map(|batch| (batch, state)))
            },
        );

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            projected_schema,
            stream,
        )))
    }
}

struct ElasticsearchScanState {
    client: Arc<dyn Elasticsearch>,
    index: String,
    full_schema: SchemaRef,
    projection: Option<Vec<usize>>,
    remaining: Option<usize>,
    pit_id: Option<String>,
    search_after: Option<Vec<serde_json::Value>>,
    use_point_in_time: bool,
    done: bool,
}

impl ElasticsearchScanState {
    fn new(
        client: Arc<dyn Elasticsearch>,
        index: String,
        full_schema: SchemaRef,
        projection: Option<Vec<usize>>,
        limit: Option<usize>,
    ) -> Self {
        Self {
            client,
            index,
            full_schema,
            projection,
            remaining: limit,
            pit_id: None,
            search_after: None,
            use_point_in_time: limit.is_none_or(|limit| limit > ELASTICSEARCH_PAGE_SIZE),
            done: false,
        }
    }

    async fn next_batch(&mut self) -> Result<Option<RecordBatch>, DataFusionError> {
        if self.done {
            return Ok(None);
        }

        if self.remaining == Some(0) {
            self.close_point_in_time().await?;
            self.done = true;
            return Ok(None);
        }

        if self.use_point_in_time {
            self.fetch_next_point_in_time_batch().await
        } else {
            self.fetch_single_batch().await
        }
    }

    async fn fetch_single_batch(&mut self) -> Result<Option<RecordBatch>, DataFusionError> {
        let size = self.remaining.unwrap_or(ELASTICSEARCH_PAGE_SIZE);
        let req = SearchRequest {
            query: Some(elasticsearch::match_all_query()),
            size: Some(size),
            ..Default::default()
        };

        let response = self
            .client
            .search(&self.index, &req)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        self.remaining = Some(0);
        self.done = true;

        self.hits_to_projected_batch(&response.hits.hits).map(Some)
    }

    async fn fetch_next_point_in_time_batch(
        &mut self,
    ) -> Result<Option<RecordBatch>, DataFusionError> {
        match self.fetch_next_point_in_time_batch_inner().await {
            Ok(batch) => Ok(batch),
            Err(err) => {
                self.close_point_in_time_best_effort().await;
                self.done = true;
                Err(err)
            }
        }
    }

    async fn fetch_next_point_in_time_batch_inner(
        &mut self,
    ) -> Result<Option<RecordBatch>, DataFusionError> {
        let page_size = self.remaining.map_or(ELASTICSEARCH_PAGE_SIZE, |remaining| {
            remaining.min(ELASTICSEARCH_PAGE_SIZE)
        });

        let pit_id = self.open_point_in_time().await?;
        let mut req = serde_json::json!({
            "query": elasticsearch::match_all_query(),
            "pit": {
                "id": pit_id,
                "keep_alive": ELASTICSEARCH_PIT_KEEP_ALIVE,
            },
            "size": page_size,
            "sort": [{ "_shard_doc": "asc" }],
            "track_total_hits": false,
        });

        if let Some(search_after) = &self.search_after {
            req["search_after"] = serde_json::Value::Array(search_after.clone());
        }

        let response = self
            .client
            .search_point_in_time(&req)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        if let Some(pit_id) = response.pit_id.as_ref() {
            self.pit_id = Some(pit_id.clone());
        }

        let hits = response.hits.hits;
        if hits.is_empty() {
            self.close_point_in_time().await?;
            self.done = true;
            return Ok(None);
        }

        let hits_len = hits.len();
        let batch = self.hits_to_projected_batch(&hits)?;

        if let Some(remaining) = self.remaining.as_mut() {
            *remaining = remaining.saturating_sub(hits_len);
        }

        let exhausted = hits_len < page_size || self.remaining == Some(0);
        if exhausted {
            self.close_point_in_time().await?;
            self.done = true;
        } else {
            let sort = hits
                .last()
                .and_then(|hit| hit.sort.as_ref())
                .cloned()
                .ok_or_else(|| {
                    DataFusionError::Execution(String::from(
                        "Elasticsearch point-in-time scan did not return hit sort values for search_after pagination",
                    ))
                })?;
            self.search_after = Some(sort);
        }

        Ok(Some(batch))
    }

    async fn open_point_in_time(&mut self) -> Result<String, DataFusionError> {
        if let Some(pit_id) = &self.pit_id {
            return Ok(pit_id.clone());
        }

        let pit_id = self
            .client
            .open_point_in_time(&self.index, ELASTICSEARCH_PIT_KEEP_ALIVE)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        self.pit_id = Some(pit_id.clone());
        Ok(pit_id)
    }

    async fn close_point_in_time(&mut self) -> Result<(), DataFusionError> {
        let Some(pit_id) = self.pit_id.take() else {
            return Ok(());
        };

        self.client
            .close_point_in_time(&pit_id)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))
    }

    async fn close_point_in_time_best_effort(&mut self) {
        let _ = self.close_point_in_time().await;
    }

    fn hits_to_projected_batch(
        &self,
        hits: &[elasticsearch::Hit],
    ) -> Result<RecordBatch, DataFusionError> {
        let batch = hits_to_record_batch(hits, &self.full_schema)?;
        if let Some(proj) = &self.projection {
            Ok(batch.project(proj)?)
        } else {
            Ok(batch)
        }
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
        // ES `unsigned_long` maps to Arrow `UInt64` in schema.rs. Decode using
        // `as_u64` so values up to u64::MAX round-trip without being clipped
        // through i64. JS clients commonly serialize values > 2^53-1 as digit
        // strings (since JSON `number` can't represent them safely), and ES
        // preserves that representation in `_source`, so also accept a numeric
        // string. Values outside u64 range (incl. negative numerics) yield NULL.
        DataType::UInt64 => {
            let values: Vec<Option<u64>> = hits
                .iter()
                .map(|h| {
                    extract_field(&h.source, field_name).and_then(|v| {
                        v.as_u64()
                            .or_else(|| v.as_str().and_then(|s| s.parse::<u64>().ok()))
                    })
                })
                .collect();
            Ok(Arc::new(UInt64Array::from(values)) as ArrayRef)
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
                        return ms.checked_mul(1_000);
                    }
                    if let Some(ms) = v.as_u64() {
                        // epoch-millis → epoch-microseconds
                        return i64::try_from(ms).ok().and_then(|ms| ms.checked_mul(1_000));
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
    use datafusion::physical_plan::collect;
    use datafusion::prelude::SessionContext;
    use elasticsearch::{Hit, HitsEnvelope, HitsTotal, MappingResponse, SearchResponse};
    use serde_json::json;
    use std::collections::VecDeque;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn make_hit(source: serde_json::Value) -> Hit {
        Hit {
            id: "id".to_string(),
            score: Some(1.0),
            sort: None,
            source,
        }
    }

    fn make_sorted_hit(id: usize) -> Hit {
        Hit {
            id: id.to_string(),
            score: Some(1.0),
            sort: Some(vec![json!(id)]),
            source: json!({ "title": format!("title-{id}") }),
        }
    }

    fn search_response(hits: Vec<Hit>, pit_id: Option<&str>) -> SearchResponse {
        SearchResponse {
            pit_id: pit_id.map(ToString::to_string),
            hits: HitsEnvelope {
                total: Some(HitsTotal {
                    value: u64::try_from(hits.len()).expect("hit count should fit in u64"),
                }),
                hits,
            },
        }
    }

    fn title_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("title", DataType::Utf8, true)]))
    }

    fn unexpected_call_error(method: &str) -> elasticsearch::Error {
        elasticsearch::Error::ElasticsearchError {
            status: 500,
            message: format!("unexpected call to {method}"),
        }
    }

    #[derive(Debug)]
    struct MockElasticsearch {
        single_search_response: Mutex<Option<SearchResponse>>,
        single_search_requests: Mutex<Vec<SearchRequest>>,
        point_in_time_responses: Mutex<VecDeque<SearchResponse>>,
        point_in_time_requests: Mutex<Vec<serde_json::Value>>,
        opened_point_in_times: AtomicUsize,
        closed_point_in_times: Mutex<Vec<String>>,
    }

    impl MockElasticsearch {
        fn with_single_response(response: SearchResponse) -> Self {
            Self {
                single_search_response: Mutex::new(Some(response)),
                single_search_requests: Mutex::new(Vec::new()),
                point_in_time_responses: Mutex::new(VecDeque::new()),
                point_in_time_requests: Mutex::new(Vec::new()),
                opened_point_in_times: AtomicUsize::new(0),
                closed_point_in_times: Mutex::new(Vec::new()),
            }
        }

        fn with_point_in_time_responses(responses: Vec<SearchResponse>) -> Self {
            Self {
                single_search_response: Mutex::new(None),
                single_search_requests: Mutex::new(Vec::new()),
                point_in_time_responses: Mutex::new(VecDeque::from(responses)),
                point_in_time_requests: Mutex::new(Vec::new()),
                opened_point_in_times: AtomicUsize::new(0),
                closed_point_in_times: Mutex::new(Vec::new()),
            }
        }
    }

    #[async_trait::async_trait]
    impl Elasticsearch for MockElasticsearch {
        async fn get_mapping(&self, _index: &str) -> elasticsearch::Result<MappingResponse> {
            Err(unexpected_call_error("get_mapping"))
        }

        async fn search(
            &self,
            _index: &str,
            body: &SearchRequest,
        ) -> elasticsearch::Result<SearchResponse> {
            self.single_search_requests
                .lock()
                .expect("single search requests mutex should not be poisoned")
                .push(body.clone());

            self.single_search_response
                .lock()
                .expect("single search response mutex should not be poisoned")
                .take()
                .ok_or_else(|| unexpected_call_error("search"))
        }

        async fn search_raw(
            &self,
            _index: &str,
            _body: &serde_json::Value,
        ) -> elasticsearch::Result<SearchResponse> {
            Err(unexpected_call_error("search_raw"))
        }

        async fn open_point_in_time(
            &self,
            _index: &str,
            _keep_alive: &str,
        ) -> elasticsearch::Result<String> {
            let next_id = self.opened_point_in_times.fetch_add(1, Ordering::SeqCst) + 1;
            Ok(format!("pit-{next_id}"))
        }

        async fn search_point_in_time(
            &self,
            body: &serde_json::Value,
        ) -> elasticsearch::Result<SearchResponse> {
            self.point_in_time_requests
                .lock()
                .expect("point-in-time requests mutex should not be poisoned")
                .push(body.clone());

            self.point_in_time_responses
                .lock()
                .expect("point-in-time responses mutex should not be poisoned")
                .pop_front()
                .ok_or_else(|| unexpected_call_error("search_point_in_time"))
        }

        async fn close_point_in_time(&self, pit_id: &str) -> elasticsearch::Result<()> {
            self.closed_point_in_times
                .lock()
                .expect("closed point-in-time mutex should not be poisoned")
                .push(pit_id.to_string());
            Ok(())
        }

        async fn index_exists(&self, _index: &str) -> elasticsearch::Result<bool> {
            Err(unexpected_call_error("index_exists"))
        }

        async fn create_index(
            &self,
            _index: &str,
            _body: &serde_json::Value,
        ) -> elasticsearch::Result<serde_json::Value> {
            Err(unexpected_call_error("create_index"))
        }

        async fn put_mapping(
            &self,
            _index: &str,
            _body: &serde_json::Value,
        ) -> elasticsearch::Result<serde_json::Value> {
            Err(unexpected_call_error("put_mapping"))
        }

        async fn get_index_refresh_interval(
            &self,
            _index: &str,
        ) -> elasticsearch::Result<Option<String>> {
            Err(unexpected_call_error("get_index_refresh_interval"))
        }

        async fn put_index_settings(
            &self,
            _index: &str,
            _body: &serde_json::Value,
        ) -> elasticsearch::Result<serde_json::Value> {
            Err(unexpected_call_error("put_index_settings"))
        }

        async fn refresh_index(&self, _index: &str) -> elasticsearch::Result<serde_json::Value> {
            Err(unexpected_call_error("refresh_index"))
        }

        async fn force_merge(
            &self,
            _index: &str,
            _max_num_segments: u32,
        ) -> elasticsearch::Result<serde_json::Value> {
            Err(unexpected_call_error("force_merge"))
        }

        async fn index_document(
            &self,
            _index: &str,
            _id: &str,
            _doc: &serde_json::Value,
        ) -> elasticsearch::Result<serde_json::Value> {
            Err(unexpected_call_error("index_document"))
        }

        async fn bulk_index(
            &self,
            _index: &str,
            _docs: &[(Option<String>, serde_json::Value)],
        ) -> elasticsearch::Result<serde_json::Value> {
            Err(unexpected_call_error("bulk_index"))
        }
    }

    async fn collect_query_table(
        client: Arc<dyn Elasticsearch>,
        limit: Option<usize>,
    ) -> Vec<RecordBatch> {
        try_collect_query_table(client, limit)
            .await
            .expect("scan should collect successfully")
    }

    async fn try_collect_query_table(
        client: Arc<dyn Elasticsearch>,
        limit: Option<usize>,
    ) -> datafusion::error::Result<Vec<RecordBatch>> {
        let table = ElasticsearchQueryTable::new(client, "test-index".to_string(), title_schema());
        let ctx = SessionContext::new();
        let plan = table
            .scan(&ctx.state(), None, &[], limit)
            .await
            .expect("scan should create an execution plan");

        collect(plan, ctx.task_ctx()).await
    }

    #[tokio::test]
    async fn query_table_scan_paginates_with_point_in_time() {
        let first_page = (0..ELASTICSEARCH_PAGE_SIZE)
            .map(make_sorted_hit)
            .collect::<Vec<_>>();
        let second_page = vec![make_sorted_hit(ELASTICSEARCH_PAGE_SIZE)];
        let mock = Arc::new(MockElasticsearch::with_point_in_time_responses(vec![
            search_response(first_page, Some("pit-2")),
            search_response(second_page, Some("pit-3")),
        ]));
        let client: Arc<dyn Elasticsearch> = Arc::<MockElasticsearch>::clone(&mock);

        let batches = collect_query_table(client, None).await;
        let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(total_rows, ELASTICSEARCH_PAGE_SIZE + 1);

        let requests = mock
            .point_in_time_requests
            .lock()
            .expect("point-in-time requests mutex should not be poisoned");
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0]["pit"]["id"], "pit-1");
        assert_eq!(requests[1]["pit"]["id"], "pit-2");
        assert_eq!(
            requests[1]["search_after"],
            json!([ELASTICSEARCH_PAGE_SIZE - 1])
        );

        let closed = mock
            .closed_point_in_times
            .lock()
            .expect("closed point-in-time mutex should not be poisoned");
        assert_eq!(closed.as_slice(), ["pit-3"]);
    }

    #[tokio::test]
    async fn query_table_scan_respects_limit_without_point_in_time() {
        let mock = Arc::new(MockElasticsearch::with_single_response(search_response(
            (0..5).map(make_sorted_hit).collect(),
            None,
        )));
        let client: Arc<dyn Elasticsearch> = Arc::<MockElasticsearch>::clone(&mock);

        let batches = collect_query_table(client, Some(5)).await;
        let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(total_rows, 5);

        assert_eq!(mock.opened_point_in_times.load(Ordering::SeqCst), 0);
        let requests = mock
            .single_search_requests
            .lock()
            .expect("single search requests mutex should not be poisoned");
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].size, Some(5));
    }

    #[tokio::test]
    async fn query_table_scan_closes_point_in_time_on_error() {
        let mock = Arc::new(MockElasticsearch::with_point_in_time_responses(Vec::new()));
        let client: Arc<dyn Elasticsearch> = Arc::<MockElasticsearch>::clone(&mock);

        let result = try_collect_query_table(client, None).await;
        assert!(result.is_err(), "scan should fail when PIT search fails");

        let closed = mock
            .closed_point_in_times
            .lock()
            .expect("closed point-in-time mutex should not be poisoned");
        assert_eq!(closed.as_slice(), ["pit-1"]);
    }

    // ── UInt64 (ES `unsigned_long`) ────────────────────────────────────────────

    /// schema.rs maps ES `unsigned_long` to Arrow `UInt64`. Without a dedicated
    /// decoder arm, the schema would say `UInt64` while the decoder fell into
    /// the JSON-string fallback, blowing up at `RecordBatch` construction with a
    /// schema/data type mismatch.
    #[test]
    fn test_unsigned_long_decodes_to_uint64() {
        use arrow::array::UInt64Array;

        let schema = Arc::new(Schema::new(vec![Field::new("big", DataType::UInt64, true)]));
        // u64::MAX would silently lose the high bit if we routed through i64;
        // include it explicitly to lock in the as_u64 decoding path.
        let max = u64::MAX;
        let hits = vec![
            make_hit(json!({"big": 0_u64})),
            make_hit(json!({"big": max})),
            make_hit(json!({})),              // missing → null
            make_hit(json!({"big": -1_i64})), // negative → null (out of u64 range)
            // JS-style stringified large values land in _source as strings.
            make_hit(json!({"big": "18446744073709551614"})),
            make_hit(json!({"big": "not a number"})), // unparseable → null
        ];
        let batch = hits_to_record_batch(&hits, &schema).expect("hits_to_record_batch failed");
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("column 0 should be UInt64Array");

        assert_eq!(col.value(0), 0);
        assert_eq!(col.value(1), max);
        assert!(col.is_null(2));
        assert!(col.is_null(3));
        assert_eq!(col.value(4), 18_446_744_073_709_551_614_u64);
        assert!(col.is_null(5));
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
