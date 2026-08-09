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

use chrono::{DateTime, NaiveDate, NaiveDateTime};

use arrow::array::{
    ArrayRef, BooleanArray, Date32Array, Date64Array, Float32Array, Float64Array, Int8Array,
    Int16Array, Int32Array, Int64Array, LargeStringBuilder, ListBuilder, RecordBatch, StringArray,
    StringBuilder, TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
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
use elasticsearch_datafusion_filter::{EsFilterSchema, classify_filter, translate_filter};

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
    /// Which columns can be filtered inside Elasticsearch, derived from the Arrow schema.
    filter_schema: EsFilterSchema,
}

impl ElasticsearchQueryTable {
    #[must_use]
    pub fn new(client: Arc<dyn Elasticsearch>, index: String, schema: SchemaRef) -> Self {
        let filter_schema = EsFilterSchema::from_connector_schema(&schema);
        Self {
            client,
            index,
            schema,
            filter_schema,
        }
    }

    /// Translate the filters `DataFusion` is pushing into a single non-scoring `bool.filter`
    /// clause list. Every filter reaching this point was reported pushable by
    /// `supports_filters_pushdown`, so a translation miss is an internal inconsistency and is
    /// surfaced as an error rather than silently dropping the predicate (which would over-return
    /// rows for an `Exact` filter `DataFusion` is no longer re-checking).
    fn build_filter_clauses(
        &self,
        filters: &[Expr],
    ) -> datafusion::error::Result<Vec<serde_json::Value>> {
        let mut clauses = Vec::with_capacity(filters.len());
        for filter in filters {
            match translate_filter(&self.filter_schema, filter) {
                Some(clause) => clauses.push(clause),
                None => {
                    return Err(DataFusionError::External(Box::new(
                        elasticsearch_datafusion_filter::Error::PushableFilterNotTranslated {
                            column: filter.to_string(),
                        },
                    )));
                }
            }
        }
        Ok(clauses)
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
        Ok(filters
            .iter()
            .map(|filter| classify_filter(&self.filter_schema, filter))
            .collect())
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let projected_schema = project_schema(&self.schema, projection)?;
        if limit == Some(0) {
            return Ok(Arc::new(datafusion::physical_plan::empty::EmptyExec::new(
                projected_schema,
            )));
        }

        // Push the pushable filters into a non-scoring `bool.filter` query; unpushable and
        // `Inexact` filters remain (re-)applied by DataFusion above this scan.
        let filter_clauses = self.build_filter_clauses(filters)?;
        let query = if filter_clauses.is_empty() {
            elasticsearch::match_all_query()
        } else {
            serde_json::json!({ "bool": { "filter": filter_clauses } })
        };

        // Restrict `_source` to the projected columns so `SELECT id` does not fetch whole
        // documents. The scan reads every column from `_source` before projecting, so a column
        // absent from `_source` simply reads back NULL and is then dropped by the projection.
        let source = projection.map(|proj| {
            let names: Vec<&str> = proj
                .iter()
                .filter_map(|&i| self.schema.fields().get(i).map(|f| f.name().as_str()))
                .collect();
            serde_json::json!(names)
        });

        Ok(Arc::new(ElasticsearchQueryExec {
            client: Arc::clone(&self.client),
            index: self.index.clone(),
            full_schema: Arc::clone(&self.schema),
            projected_schema,
            projection: projection.cloned(),
            query,
            source,
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
    /// The Elasticsearch query body: `match_all` or a `bool.filter` of the pushed predicates.
    query: serde_json::Value,
    /// `_source` restriction (the projected column names), or `None` for the full document.
    source: Option<serde_json::Value>,
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
        let query = self.query.clone();
        let source = self.source.clone();
        let limit = self.limit;

        let stream = futures::stream::try_unfold(
            ElasticsearchScanState::new(
                client,
                index,
                full_schema,
                projection,
                query,
                source,
                limit,
            ),
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
    query: serde_json::Value,
    source: Option<serde_json::Value>,
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
        query: serde_json::Value,
        source: Option<serde_json::Value>,
        limit: Option<usize>,
    ) -> Self {
        Self {
            client,
            index,
            full_schema,
            projection,
            query,
            source,
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
            query: Some(self.query.clone()),
            size: Some(size),
            source: self.source.clone(),
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
            "query": self.query.clone(),
            "pit": {
                "id": pit_id,
                "keep_alive": ELASTICSEARCH_PIT_KEEP_ALIVE,
            },
            "size": page_size,
            "sort": [{ "_shard_doc": "asc" }],
            "track_total_hits": false,
        });

        if let Some(source) = &self.source {
            req["_source"] = source.clone();
        }

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

/// Build the timestamp `ArrayRef` for a given concrete array type, attaching the timezone
/// from the Arrow `Timestamp(_, tz)` field when present.
macro_rules! build_timestamp_array {
    ($arr_ty:ident, $values:expr, $tz:expr) => {{
        let arr = $arr_ty::from($values);
        let arr = if let Some(tz_str) = $tz {
            arr.with_timezone(tz_str.as_ref())
        } else {
            arr
        };
        Arc::new(arr) as ArrayRef
    }};
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
        // ES `integer`/`long` mappings back a range of Arrow unsigned widths
        // (see `arrow_type_to_es_mapping`). `_source` stores them as JSON numbers.
        DataType::UInt32 => {
            let values: Vec<Option<u32>> = hits
                .iter()
                .map(|h| {
                    extract_field(&h.source, field_name)
                        .and_then(serde_json::Value::as_u64)
                        .and_then(|n| u32::try_from(n).ok())
                })
                .collect();
            Ok(Arc::new(UInt32Array::from(values)) as ArrayRef)
        }
        DataType::UInt16 => {
            let values: Vec<Option<u16>> = hits
                .iter()
                .map(|h| {
                    extract_field(&h.source, field_name)
                        .and_then(serde_json::Value::as_u64)
                        .and_then(|n| u16::try_from(n).ok())
                })
                .collect();
            Ok(Arc::new(UInt16Array::from(values)) as ArrayRef)
        }
        DataType::UInt8 => {
            let values: Vec<Option<u8>> = hits
                .iter()
                .map(|h| {
                    extract_field(&h.source, field_name)
                        .and_then(serde_json::Value::as_u64)
                        .and_then(|n| u8::try_from(n).ok())
                })
                .collect();
            Ok(Arc::new(UInt8Array::from(values)) as ArrayRef)
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
        // A `FixedSizeList` is either the dense embedding vector (`Float32`, written as a
        // JSON array of floats) or the chunk offset pair (`Int32`, written as a JSON array
        // of two ints). Route by inner type so the offset column round-trips as integers
        // instead of being coerced into a float dense-vector.
        DataType::FixedSizeList(inner_field, dim) => match inner_field.data_type() {
            DataType::Float32 | DataType::Float64 => {
                build_dense_vector_array(hits, field_name, *dim)
            }
            DataType::Int32 => build_int32_fixed_size_list_array(hits, field_name, *dim),
            other => Err(DataFusionError::NotImplemented(format!(
                "Elasticsearch _source reader cannot decode field '{field_name}': unsupported FixedSizeList inner type {other}."
            ))),
        },
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
        // ES `date` mappings back Arrow `Timestamp` for every `TimeUnit` (arrow-json writes
        // them as RFC 3339 / ISO 8601 strings; ES may also return epoch-millis integers).
        DataType::Timestamp(unit, tz) => {
            let values: Vec<Option<i64>> = hits
                .iter()
                .map(|h| {
                    extract_field(&h.source, field_name)
                        .and_then(|v| parse_timestamp_to_unit(v, *unit))
                })
                .collect();
            let arr: ArrayRef = match unit {
                TimeUnit::Second => build_timestamp_array!(TimestampSecondArray, values, tz),
                TimeUnit::Millisecond => {
                    build_timestamp_array!(TimestampMillisecondArray, values, tz)
                }
                TimeUnit::Microsecond => {
                    build_timestamp_array!(TimestampMicrosecondArray, values, tz)
                }
                TimeUnit::Nanosecond => {
                    build_timestamp_array!(TimestampNanosecondArray, values, tz)
                }
            };
            Ok(arr)
        }
        // ES `date` mappings also back Arrow date types. arrow-json writes `Date32` as a
        // `YYYY-MM-DD` string and `Date64` as an ISO 8601 datetime string.
        DataType::Date32 => {
            let values: Vec<Option<i32>> = hits
                .iter()
                .map(|h| extract_field(&h.source, field_name).and_then(parse_date32))
                .collect();
            Ok(Arc::new(Date32Array::from(values)) as ArrayRef)
        }
        DataType::Date64 => {
            let values: Vec<Option<i64>> = hits
                .iter()
                .map(|h| extract_field(&h.source, field_name).and_then(parse_date64_millis))
                .collect();
            Ok(Arc::new(Date64Array::from(values)) as ArrayRef)
        }
        // Remaining string-typed fields serialize as JSON strings (plain `Utf8` is handled
        // above). Anything else is a type `arrow_type_to_es_mapping` only round-trips through
        // `_source` as an opaque `keyword`; decoding it into a concrete Arrow array here is
        // not implemented, so fail loudly rather than silently producing a wrong-typed
        // `Utf8` column.
        DataType::LargeUtf8 | DataType::Utf8View => {
            let values: Vec<Option<String>> = hits
                .iter()
                .map(|h| extract_field(&h.source, field_name).map(json_value_to_string))
                .collect();
            Ok(Arc::new(StringArray::from(values)) as ArrayRef)
        }
        other => Err(DataFusionError::NotImplemented(format!(
            "Elasticsearch _source reader cannot decode field '{field_name}' of type {other}. \
            Declare this metadata column with a supported type (boolean, integer/float, date/timestamp, string, or list of strings)."
        ))),
    }
}

/// Parse a JSON `_source` value into an epoch count expressed in `unit`.
///
/// Strings are parsed as RFC 3339 / ISO 8601 (arrow-json's write format); numeric values
/// follow the Elasticsearch convention of epoch-milliseconds.
fn parse_timestamp_to_unit(v: &serde_json::Value, unit: TimeUnit) -> Option<i64> {
    let nanos: i64 = if let Some(s) = v.as_str() {
        parse_datetime_to_nanos(s)?
    } else if let Some(ms) = v.as_i64() {
        ms.checked_mul(1_000_000)?
    } else if let Some(ms) = v.as_u64() {
        i64::try_from(ms).ok()?.checked_mul(1_000_000)?
    } else {
        return None;
    };
    Some(match unit {
        TimeUnit::Second => nanos.div_euclid(1_000_000_000),
        TimeUnit::Millisecond => nanos.div_euclid(1_000_000),
        TimeUnit::Microsecond => nanos.div_euclid(1_000),
        TimeUnit::Nanosecond => nanos,
    })
}

/// Parse an RFC 3339 / ISO 8601 datetime (or bare date) string into nanoseconds since the
/// Unix epoch. Handles both timezone-aware strings and the naive form arrow-json emits for
/// timezone-less `Timestamp`/`Date64` columns.
fn parse_datetime_to_nanos(s: &str) -> Option<i64> {
    if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
        return dt.timestamp_nanos_opt();
    }
    for fmt in [
        "%Y-%m-%dT%H:%M:%S%.f",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S%.f",
        "%Y-%m-%d %H:%M:%S",
    ] {
        if let Ok(ndt) = NaiveDateTime::parse_from_str(s, fmt) {
            return ndt.and_utc().timestamp_nanos_opt();
        }
    }
    NaiveDate::parse_from_str(s, "%Y-%m-%d")
        .ok()
        .and_then(|d| d.and_hms_opt(0, 0, 0))
        .and_then(|ndt| ndt.and_utc().timestamp_nanos_opt())
}

/// Parse a `_source` value into an Arrow `Date32` (days since the Unix epoch).
fn parse_date32(v: &serde_json::Value) -> Option<i32> {
    if let Some(s) = v.as_str() {
        let date = NaiveDate::parse_from_str(s, "%Y-%m-%d").ok().or_else(|| {
            DateTime::parse_from_rfc3339(s)
                .ok()
                .map(|dt| dt.date_naive())
        })?;
        let epoch = NaiveDate::from_ymd_opt(1970, 1, 1)?;
        return i32::try_from((date - epoch).num_days()).ok();
    }
    // Numeric values are interpreted as Arrow's native epoch-days representation.
    v.as_i64().and_then(|n| i32::try_from(n).ok())
}

/// Parse a `_source` value into an Arrow `Date64` (milliseconds since the Unix epoch).
fn parse_date64_millis(v: &serde_json::Value) -> Option<i64> {
    if let Some(s) = v.as_str() {
        return parse_datetime_to_nanos(s).map(|ns| ns.div_euclid(1_000_000));
    }
    // Numeric values follow the Elasticsearch epoch-milliseconds convention.
    v.as_i64().or_else(|| i64::try_from(v.as_u64()?).ok())
}

/// Build a `FixedSizeList(Int32, dim)` array from `_source` JSON int arrays (e.g. the chunk
/// `{start, end}` offset pair). A missing value yields a NULL row.
fn build_int32_fixed_size_list_array(
    hits: &[elasticsearch::Hit],
    field_name: &str,
    dim: i32,
) -> Result<ArrayRef, DataFusionError> {
    let dim_usize = usize::try_from(dim).map_err(|_| {
        DataFusionError::Execution(format!(
            "FixedSizeList field '{field_name}' has a negative dimension {dim}"
        ))
    })?;
    let mut flat_values: Vec<i32> = Vec::with_capacity(hits.len() * dim_usize);
    let mut null_mask: Vec<bool> = Vec::with_capacity(hits.len());

    for hit in hits {
        if let Some(arr) = extract_field(&hit.source, field_name).and_then(|v| v.as_array()) {
            if arr.len() != dim_usize {
                return Err(DataFusionError::Execution(format!(
                    "FixedSizeList field '{field_name}' has {len} elements, expected {dim_usize}",
                    len = arr.len(),
                )));
            }
            for val in arr {
                let n = val
                    .as_i64()
                    .and_then(|n| i32::try_from(n).ok())
                    .ok_or_else(|| {
                        DataFusionError::Execution(format!(
                            "FixedSizeList field '{field_name}' contains a non-Int32 element"
                        ))
                    })?;
                flat_values.push(n);
            }
            null_mask.push(true);
        } else {
            flat_values.extend(std::iter::repeat_n(0, dim_usize));
            null_mask.push(false);
        }
    }

    let values_array = Arc::new(Int32Array::from(flat_values)) as ArrayRef;
    let nulls = arrow::buffer::NullBuffer::from(null_mask);
    let list_array = arrow::array::FixedSizeListArray::try_new(
        Arc::new(arrow::datatypes::Field::new("item", DataType::Int32, false)),
        dim,
        values_array,
        Some(nulls),
    )
    .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;

    Ok(Arc::new(list_array) as ArrayRef)
}

/// Look up a field in an Elasticsearch `_source` document.
///
/// Tries the literal (flat) key first — Elasticsearch preserves the original document shape
/// in `_source`, so a dotted field name written as a flat key (e.g. `"_spice.chunk_id"`)
/// stays flat. Falls back to dot-separated nested navigation (e.g. `"address.city"` →
/// `source["address"]["city"]`) so genuinely nested objects still resolve, and to cover
/// deployments/configs where Elasticsearch expands dotted names into nested objects.
fn extract_field<'a>(
    source: &'a serde_json::Value,
    field_name: &str,
) -> Option<&'a serde_json::Value> {
    if let Some(value) = source.get(field_name) {
        return Some(value);
    }
    if !field_name.contains('.') {
        return None;
    }
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

        async fn delete_by_query(
            &self,
            _index: &str,
            _query: &serde_json::Value,
        ) -> elasticsearch::Result<serde_json::Value> {
            Err(unexpected_call_error("delete_by_query"))
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

    // ── Chunked columns: flat dotted chunk_id + Int32 offset pair ────────────────

    /// The chunked warm-index fallback reads `_spice.chunk_id` (written as a flat dotted
    /// key in `_source`) and the `{col}_offset` `FixedSizeList(Int32, 2)` back out of
    /// Elasticsearch. `extract_field` must find the flat key rather than dot-navigating into
    /// a non-existent `{"_spice": {"chunk_id": ..}}`, and the offset must decode as Int32,
    /// not as a Float32 dense vector.
    #[test]
    fn test_chunked_chunk_id_and_offset_round_trip() {
        use arrow::array::{FixedSizeListArray, Int32Array, UInt64Array};

        let schema = Arc::new(Schema::new(vec![
            Field::new("_spice.chunk_id", DataType::UInt64, false),
            Field::new(
                "content_offset",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Int32, false)), 2),
                false,
            ),
        ]));
        let hits = vec![
            make_hit(json!({"_spice.chunk_id": 0_u64, "content_offset": [0, 27]})),
            make_hit(json!({"_spice.chunk_id": 5_u64, "content_offset": [27, 45]})),
        ];
        let batch = hits_to_record_batch(&hits, &schema).expect("hits_to_record_batch failed");

        let chunk_ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("chunk_id column should be UInt64Array");
        assert_eq!(chunk_ids.value(0), 0);
        assert_eq!(chunk_ids.value(1), 5);

        let offsets = batch
            .column(1)
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .expect("offset column should be FixedSizeListArray");
        assert_eq!(offsets.values().data_type(), &DataType::Int32);
        let row0 = offsets.value(0);
        let pair0 = row0
            .as_any()
            .downcast_ref::<Int32Array>()
            .expect("offset values should be Int32Array");
        assert_eq!(pair0.value(0), 0);
        assert_eq!(pair0.value(1), 27);
    }

    /// Nested dotted names (`address.city`) must still resolve via dot-navigation when the
    /// flat key is absent — the flat-first lookup must not regress genuine nesting.
    #[test]
    fn test_extract_field_prefers_flat_then_nested() {
        let flat = json!({"_spice.chunk_id": 7});
        assert_eq!(
            extract_field(&flat, "_spice.chunk_id").and_then(serde_json::Value::as_u64),
            Some(7)
        );

        let nested = json!({"address": {"city": "Denver"}});
        assert_eq!(
            extract_field(&nested, "address.city").and_then(serde_json::Value::as_str),
            Some("Denver")
        );
    }

    // ── Unsigned integer widths ──────────────────────────────────────────────────

    #[test]
    fn test_unsigned_int_widths_decode() {
        use arrow::array::{UInt8Array, UInt16Array, UInt32Array};

        let schema = Arc::new(Schema::new(vec![
            Field::new("u8", DataType::UInt8, true),
            Field::new("u16", DataType::UInt16, true),
            Field::new("u32", DataType::UInt32, true),
        ]));
        let hits = vec![
            make_hit(json!({"u8": 255, "u16": 65535, "u32": 4_294_967_295_u32})),
            make_hit(json!({"u8": 256, "u16": -1, "u32": "nope"})), // out of range / wrong type → null
        ];
        let batch = hits_to_record_batch(&hits, &schema).expect("hits_to_record_batch failed");

        let u8s = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt8Array>()
            .expect("u8 column");
        assert_eq!(u8s.value(0), 255);
        assert!(u8s.is_null(1));

        let u16s = batch
            .column(1)
            .as_any()
            .downcast_ref::<UInt16Array>()
            .expect("u16 column");
        assert_eq!(u16s.value(0), 65535);
        assert!(u16s.is_null(1));

        let u32s = batch
            .column(2)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .expect("u32 column");
        assert_eq!(u32s.value(0), 4_294_967_295);
        assert!(u32s.is_null(1));
    }

    // ── Date32 / Date64 ──────────────────────────────────────────────────────────

    #[test]
    fn test_date32_parses_from_string() {
        use arrow::array::Date32Array;

        let schema = Arc::new(Schema::new(vec![Field::new("d", DataType::Date32, true)]));
        // arrow-json writes Date32 as a `YYYY-MM-DD` string.
        let hits = vec![
            make_hit(json!({"d": "1970-01-01"})),
            make_hit(json!({"d": "2024-01-15"})),
            make_hit(json!({})), // missing → null
        ];
        let batch = hits_to_record_batch(&hits, &schema).expect("hits_to_record_batch failed");
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Date32Array>()
            .expect("date32 column");
        assert_eq!(col.value(0), 0);
        assert_eq!(col.value(1), 19_737);
        assert!(col.is_null(2));
    }

    #[test]
    fn test_date64_parses_from_datetime_string() {
        use arrow::array::Date64Array;

        let schema = Arc::new(Schema::new(vec![Field::new("d", DataType::Date64, true)]));
        // arrow-json writes Date64 as an ISO 8601 datetime string.
        let hits = vec![make_hit(json!({"d": "2024-01-15T10:30:00"}))];
        let batch = hits_to_record_batch(&hits, &schema).expect("hits_to_record_batch failed");
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Date64Array>()
            .expect("date64 column");
        let expected =
            chrono::NaiveDateTime::parse_from_str("2024-01-15T10:30:00", "%Y-%m-%dT%H:%M:%S")
                .expect("valid datetime literal")
                .and_utc()
                .timestamp_millis();
        assert_eq!(col.value(0), expected);
    }

    // ── Timestamp: all TimeUnits ─────────────────────────────────────────────────

    #[test]
    fn test_timestamp_all_units_from_naive_string() {
        use arrow::array::{
            TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray,
        };

        let dt = "2024-01-15T10:30:00";
        let expected_nanos = chrono::NaiveDateTime::parse_from_str(dt, "%Y-%m-%dT%H:%M:%S")
            .expect("valid datetime literal")
            .and_utc()
            .timestamp_nanos_opt()
            .expect("in range");

        let sec_schema = Arc::new(Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Second, None),
            true,
        )]));
        let sec = hits_to_record_batch(&[make_hit(json!({"ts": dt}))], &sec_schema)
            .expect("second batch");
        assert_eq!(
            sec.column(0)
                .as_any()
                .downcast_ref::<TimestampSecondArray>()
                .expect("second array")
                .value(0),
            expected_nanos / 1_000_000_000
        );

        let ms_schema = Arc::new(Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            true,
        )]));
        let ms =
            hits_to_record_batch(&[make_hit(json!({"ts": dt}))], &ms_schema).expect("ms batch");
        assert_eq!(
            ms.column(0)
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .expect("ms array")
                .value(0),
            expected_nanos / 1_000_000
        );

        let ns_schema = Arc::new(Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            true,
        )]));
        let ns =
            hits_to_record_batch(&[make_hit(json!({"ts": dt}))], &ns_schema).expect("ns batch");
        assert_eq!(
            ns.column(0)
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .expect("ns array")
                .value(0),
            expected_nanos
        );
    }

    // ── Unsupported types fail loudly ────────────────────────────────────────────

    /// A declared metadata column of a type the reader cannot decode must produce a
    /// structured error rather than a silently wrong `Utf8` column.
    #[test]
    fn test_unsupported_type_errors() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "amount",
            DataType::Decimal128(10, 2),
            true,
        )]));
        let hits = vec![make_hit(json!({"amount": "1.23"}))];
        let err =
            hits_to_record_batch(&hits, &schema).expect_err("decimal decode should be unsupported");
        assert!(
            matches!(err, DataFusionError::NotImplemented(_)),
            "expected NotImplemented, got {err:?}"
        );
    }
}
