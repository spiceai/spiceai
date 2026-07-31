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
use arrow::datatypes::Field;
use datafusion::common::ScalarValue;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use elasticsearch::Elasticsearch;
use serde_json::{Value, json};
use snafu::ResultExt;

use crate::index::chunking::ChunkedSearchIndex;

/// The columns to address documents by in [`delete_by_keys`], given an index's `primary_key`.
///
/// Drops the chunk key, which is only present when the index is the inner index of a
/// [`ChunkedSearchIndex`]. There, one source row is stored as one document per chunk, and a
/// source row is deleted (or re-chunked on upsert) as a whole — so the base key, not the
/// chunk-keyed composite, is what identifies the documents to remove. Deleting on the base key
/// also means the caller never has to know how many chunks a row produced.
///
/// For a non-chunked index the chunk key isn't in `primary_key`, so this is the full key.
pub fn document_key_columns(primary_key: &[Field]) -> Vec<String> {
    ChunkedSearchIndex::base_key_columns(primary_key)
}

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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Int64Array, RecordBatch, UInt64Array};
    use arrow::datatypes::{DataType, Field, Schema};

    use super::{build_or_of_row_term_queries, document_key_columns};
    use crate::index::chunking::{CHUNKED_INDEX_CHUNK_KEY, ChunkedSearchIndex};

    fn id_field() -> Field {
        Field::new("id", DataType::Int64, false)
    }

    /// A key batch carrying only the base key, as a chunked index hands it over.
    fn base_keys(ids: &[i64]) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![id_field()])),
            vec![Arc::new(Int64Array::from(ids.to_vec())) as ArrayRef],
        )
        .expect("valid batch")
    }

    #[test]
    fn document_key_columns_leaves_a_plain_primary_key_alone() {
        assert_eq!(document_key_columns(&[id_field()]), vec!["id".to_string()]);
    }

    #[test]
    fn document_key_columns_drops_the_chunk_key() {
        let chunked = ChunkedSearchIndex::augment_primary_key(vec![id_field()]);
        assert!(chunked.iter().any(|f| f.name() == CHUNKED_INDEX_CHUNK_KEY));
        assert_eq!(document_key_columns(&chunked), vec!["id".to_string()]);
    }

    /// The base-key query filters on the base key only, so it matches — and so deletes — every
    /// chunk document stored under it.
    #[test]
    fn a_chunked_index_deletes_every_chunk_of_a_base_key() {
        let chunked = ChunkedSearchIndex::augment_primary_key(vec![id_field()]);
        let query = build_or_of_row_term_queries(&document_key_columns(&chunked), &base_keys(&[7]))
            .expect("query builds")
            .expect("non-empty batch produces a query");

        let clauses = query["bool"]["should"]
            .as_array()
            .expect("one should clause per key row");
        assert_eq!(clauses.len(), 1);
        assert_eq!(
            clauses[0]["bool"]["filter"],
            serde_json::json!([{ "term": { "id": 7 } }]),
            "only the base key is filtered on: {query}"
        );
    }

    /// Why the chunk key has to be dropped: a chunked index knows the base key but not the chunk
    /// ids under it, so a query addressing the full composite key cannot be built at all.
    #[test]
    fn the_full_composite_key_cannot_address_a_base_key_batch() {
        let full: Vec<String> = ChunkedSearchIndex::augment_primary_key(vec![id_field()])
            .iter()
            .map(|f| f.name().clone())
            .collect();

        let err = build_or_of_row_term_queries(&full, &base_keys(&[7]))
            .expect_err("the chunk id is not in the batch");
        assert!(
            err.to_string().contains(CHUNKED_INDEX_CHUNK_KEY),
            "unexpected error: {err}"
        );
    }

    /// A chunk id that *is* present is still ignored — one delete removes the whole group.
    #[test]
    fn a_present_chunk_id_is_not_filtered_on() {
        let chunked = ChunkedSearchIndex::augment_primary_key(vec![id_field()]);
        let keys = RecordBatch::try_new(
            Arc::new(Schema::new(chunked.clone())),
            vec![
                Arc::new(Int64Array::from(vec![7_i64])) as ArrayRef,
                Arc::new(UInt64Array::from(vec![3_u64])) as ArrayRef,
            ],
        )
        .expect("valid batch");

        let query = build_or_of_row_term_queries(&document_key_columns(&chunked), &keys)
            .expect("query builds")
            .expect("non-empty batch produces a query");

        assert_eq!(
            query["bool"]["should"][0]["bool"]["filter"],
            serde_json::json!([{ "term": { "id": 7 } }]),
            "the chunk id must not narrow the delete: {query}"
        );
    }
}
