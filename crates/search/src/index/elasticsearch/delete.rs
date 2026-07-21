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

/// Deletes every document whose columns match a row of `keys`, on exactly the columns `keys`
/// has (its own schema names them) — an exact-key delete when `keys` carries every
/// `primary_key` column, a prefix delete when it carries a strict subset (the chunked-index
/// case). Elasticsearch's `_delete_by_query` filters by field value directly, so both cases are
/// the same operation — there is no separate "exact" vs "prefix" code path. Shared by
/// [`super::ElasticsearchIndex`] and [`super::ElasticsearchTextIndex`], which both address
/// documents the same way (client + index name + primary key columns).
pub async fn delete_by_keys(
    client: &dyn Elasticsearch,
    es_index: &str,
    keys: &RecordBatch,
) -> DataFusionResult<()> {
    let Some(query) = build_or_of_row_term_queries(keys)? else {
        return Ok(());
    };

    client
        .delete_by_query(es_index, &query)
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    Ok(())
}

/// Builds `{"bool": {"should": [{"bool": {"filter": [{"term": {...}}, ...]}}, ...], "minimum_should_match": 1}}`
/// — one `should` clause (all columns ANDed) per row of `keys`, rows ORed together.
fn build_or_of_row_term_queries(keys: &RecordBatch) -> DataFusionResult<Option<Value>> {
    if keys.num_rows() == 0 {
        return Ok(None);
    }

    let schema = keys.schema();
    let mut row_clauses = Vec::with_capacity(keys.num_rows());
    for row in 0..keys.num_rows() {
        let mut terms = Vec::with_capacity(schema.fields().len());
        for (col_idx, field) in schema.fields().iter().enumerate() {
            let value = ScalarValue::try_from_array(keys.column(col_idx).as_ref(), row)?;
            let Some(json_value) = scalar_to_term_value(&value) else {
                // A NULL/unsupported key column can never equal anything via `term` — skip this
                // row's clause entirely rather than emit a filter that matches everything.
                terms.clear();
                break;
            };
            terms.push(json!({ "term": { field.name().as_str(): json_value } }));
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
        ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) | ScalarValue::Utf8View(Some(s)) => {
            Some(Value::String(s.clone()))
        }
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
