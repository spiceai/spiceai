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

use arrow::array::RecordBatch;
use datafusion::common::ScalarValue;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use elasticsearch::Elasticsearch;
use serde_json::{Value, json};
use snafu::ResultExt;
/// Chunk size for `_delete_by_query` requests — keeps each request's `bool.should` clause count
/// comfortably under Elasticsearch's default `indices.query.bool.max_clause_count` (1024) and
/// request-size limits, regardless of how many keys the caller is deleting in one call.
const DELETE_CHUNK_ROWS: usize = 512;

/// Deletes every document whose `key_columns` match a row of `keys` — an exact-key delete when
/// `key_columns` is every `primary_key` column, a prefix delete when it's a strict subset (the
/// chunked-index case). Elasticsearch's `_delete_by_query` filters by field value directly, so
/// both cases are the same operation — there is no separate "exact" vs "prefix" code path.
///
/// Only reads `key_columns` from `keys`, ignoring any other column present — `keys` may be
/// shaped by [`runtime_datafusion_index::Index::required_columns`] (a superset of the primary
/// key) rather than the primary key alone, since that's what the default
/// [`runtime_datafusion_index::Index::resolve_delete_keys`] resolves against.
///
/// Issues one `_delete_by_query` request per [`DELETE_CHUNK_ROWS`]-row slice of `keys` rather
/// than a single request for the whole batch, so a large delete can't build an unbounded
/// `bool.should` clause list.
///
/// Shared by [`super::ElasticsearchIndex`] and [`super::ElasticsearchTextIndex`], which both
/// address documents the same way (client + index name + primary key columns).
pub async fn delete_by_keys(
    client: &dyn Elasticsearch,
    es_index: &str,
    key_columns: &[String],
    keys: &RecordBatch,
) -> DataFusionResult<()> {
    let mut offset = 0;
    while offset < keys.num_rows() {
        let len = DELETE_CHUNK_ROWS.min(keys.num_rows() - offset);
        let chunk = keys.slice(offset, len);
        offset += len;

        let Some(query) = build_or_of_row_term_queries(key_columns, &chunk)? else {
            continue;
        };

        client
            .delete_by_query(es_index, &query)
            .await
            .boxed()
            .map_err(DataFusionError::External)?;
    }

    Ok(())
}

/// Builds `{"bool": {"should": [{"bool": {"filter": [{"term": {...}}, ...]}}, ...], "minimum_should_match": 1}}`
/// — one `should` clause (`key_columns` ANDed) per row of `keys`, rows ORed together.
fn build_or_of_row_term_queries(
    key_columns: &[String],
    keys: &RecordBatch,
) -> DataFusionResult<Option<Value>> {
    if keys.num_rows() == 0 || key_columns.is_empty() {
        return Ok(None);
    }

    let arrays: Vec<_> = key_columns
        .iter()
        .map(|name| keys.column_by_name(name).cloned())
        .collect::<Option<Vec<_>>>()
        .ok_or_else(|| {
            DataFusionError::Plan(format!(
                "delete key batch is missing one of the requested key columns: {key_columns:?}"
            ))
        })?;

    let mut row_clauses = Vec::with_capacity(keys.num_rows());
    for row in 0..keys.num_rows() {
        let mut terms = Vec::with_capacity(key_columns.len());
        for (name, array) in key_columns.iter().zip(&arrays) {
            let value = ScalarValue::try_from_array(array.as_ref(), row)?;
            let Some(json_value) = scalar_to_term_value(&value) else {
                // A NULL/unsupported key column can never equal anything via `term` — skip this
                // row's clause entirely rather than emit a filter that matches everything.
                terms.clear();
                break;
            };
            terms.push(json!({ "term": { name.as_str(): json_value } }));
        }
        if !terms.is_empty() {
            row_clauses.push(json!({ "bool": { "filter": terms } }));
        }
    }

    if row_clauses.is_empty() {
        return Ok(None);
    }

    Ok(Some(json!({
        "bool": {
            "should": row_clauses,
            "minimum_should_match": 1
        }
    })))
}

fn scalar_to_term_value(value: &ScalarValue) -> Option<Value> {
    match value {
        ScalarValue::Utf8(Some(s))
        | ScalarValue::LargeUtf8(Some(s))
        | ScalarValue::Utf8View(Some(s)) => Some(Value::String(s.clone())),
        ScalarValue::Boolean(Some(b)) => Some(Value::Bool(*b)),
        ScalarValue::Int8(Some(v)) => Some(json!(v)),
        ScalarValue::Int16(Some(v)) => Some(json!(v)),
        ScalarValue::Int32(Some(v)) => Some(json!(v)),
        ScalarValue::Int64(Some(v)) => Some(json!(v)),
        ScalarValue::UInt8(Some(v)) => Some(json!(v)),
        ScalarValue::UInt16(Some(v)) => Some(json!(v)),
        ScalarValue::UInt32(Some(v)) => Some(json!(v)),
        ScalarValue::UInt64(Some(v)) => Some(json!(v)),
        // Anything else (NULL, or a type not expected on a primary-key column) — the caller
        // treats `None` as "cannot express this row's key as an exact-match filter".
        _ => None,
    }
}
