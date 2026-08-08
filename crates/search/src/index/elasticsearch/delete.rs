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
use arrow_schema::Field;
use datafusion::common::ScalarValue;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use elasticsearch::Elasticsearch;
use serde_json::{Value, json};
use snafu::{ResultExt, Snafu};

use super::write;
use crate::index::chunking::ChunkedSearchIndex;

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display(
        "Failed to delete rows from the search index '{index}' (elasticsearch): Elasticsearch applied the delete only partially — {failures} document failure(s), {version_conflicts} version conflict(s), {undeleted} of {total} matched document(s) left in place, timed out: {timed_out}. First failure: {first}. The index still holds documents for rows the dataset no longer has, so a search can return them. Reconcile the index against the source before re-running this delete: a version conflict means the row was written concurrently, so re-issuing the delete can remove the document that write just produced. Any other failure needs its reported error class resolved on the Elasticsearch index first. See: https://spiceai.org/docs/features/search"
    ))]
    DeleteByQueryPartiallyApplied {
        index: String,
        failures: usize,
        version_conflicts: u64,
        undeleted: u64,
        total: u64,
        timed_out: bool,
        first: String,
    },

    #[snafu(display(
        "Failed to delete rows from the search index '{index}' (elasticsearch): the _delete_by_query response carries no usable `{field}`, so it cannot be confirmed that every matching document was deleted; got {shape}. Check whether a proxy sits in front of Elasticsearch and is rewriting the response. See: https://spiceai.org/docs/features/search"
    ))]
    UnexpectedDeleteResponse {
        index: String,
        field: &'static str,
        shape: String,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

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
/// `key_columns` covers every `primary_key` column, a prefix delete when it's a strict subset
/// (the chunked-index case).
///
/// The two cases address documents differently, because filtering on a key *column* is not
/// reliable. Nothing maps the primary-key columns: the index mapping the runtime creates covers
/// the vector field, the configured text fields, and declared metadata columns only, so a string
/// key is dynamically mapped `text` and its inverted index holds *analyzed* tokens. A `term`
/// query is not analyzed, so `{"term": {"id": "ORDER-1024"}}` looks for one token in an index
/// holding `[order, 1024]` and matches nothing — a delete that reports success having removed
/// no documents (#12267).
///
/// So when `key_columns` covers the whole primary key, this addresses documents by `_id` via an
/// `ids` query. `_id` is the value the write path already stores for the row, derived by the
/// same [`write::extract_primary_key_from_fields`], so the delete matches exactly the documents
/// the write produced — no field mapping, no analysis, and no dependence on the key's type.
///
/// A strict subset of the key (the chunked-index case) cannot use `_id`, because the chunk id is
/// part of it and is unknown at delete time; that case still filters on the key columns and so
/// remains subject to the mapping gap above.
///
/// Only reads `key_columns` from `keys`, ignoring any other column present — `keys` may be
/// shaped by [`runtime_datafusion_index::Index::required_columns`] (a superset of the primary
/// key) rather than the primary key alone, since that's what the default
/// [`runtime_datafusion_index::Index::resolve_delete_keys`] resolves against.
///
/// Issues one `_delete_by_query` request per [`DELETE_CHUNK_ROWS`]-row slice of `keys` rather
/// than a single request for the whole batch, so a large delete can't build an unbounded
/// clause or id list.
///
/// Shared by [`super::ElasticsearchIndex`] and [`super::ElasticsearchTextIndex`], which both
/// address documents the same way (client + index name + primary key columns).
pub async fn delete_by_keys(
    client: &dyn Elasticsearch,
    es_index: &str,
    primary_key: &[Field],
    key_columns: &[String],
    keys: &RecordBatch,
) -> DataFusionResult<()> {
    // Derive `_id`s only when every primary-key column is available to derive them from; a
    // partial key yields a different `_id` than the write path stored, which would delete
    // nothing.
    let addresses_whole_key = !primary_key.is_empty()
        && primary_key
            .iter()
            .all(|f| key_columns.iter().any(|c| c == f.name()));

    let mut offset = 0;
    while offset < keys.num_rows() {
        let len = DELETE_CHUNK_ROWS.min(keys.num_rows() - offset);
        let chunk = keys.slice(offset, len);
        offset += len;

        let query = if addresses_whole_key {
            build_ids_query(primary_key, es_index, &chunk)?
        } else {
            build_or_of_row_term_queries(key_columns, &chunk)?
        };
        let Some(query) = query else {
            continue;
        };

        let resp = client
            .delete_by_query(es_index, &query)
            .await
            .boxed()
            .map_err(DataFusionError::External)?;
        inspect_delete_response(&resp, es_index)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
    }

    Ok(())
}

/// Check a `_delete_by_query` response body and return an error unless every matching document
/// was deleted.
///
/// A `2xx` only means the request ran. `_delete_by_query` snapshots the index when it starts and
/// then deletes document by document, reporting per-document outcomes in the body: `failures`
/// carries the ones that errored, and `version_conflicts` counts the ones skipped because their
/// version moved after the snapshot — which a concurrent write cycle over the same rows produces.
/// Ignoring the body reports a partial delete as a success, leaving documents behind for rows the
/// dataset no longer has (#12364), the same observable symptom as #12267 and #12272.
///
/// Reports rather than retries. Re-issuing the query here is not a safe repair: the delete
/// addresses documents by the `_id` derived from the row's primary key, so an upsert that rewrote
/// that row under the same `_id` is exactly what raises the conflict, and an automatic retry would
/// delete the document that write just produced. Every caller drives this from a delete it has
/// already applied to the accelerator and logs the error rather than propagating it, so surfacing
/// it makes the divergence visible where the retry decision can be made with the source in hand.
///
/// Success is positively confirmed, never assumed from the absence of a complaint. `failures` and
/// `version_conflicts` name only the outcomes Elasticsearch chose to itemise; a request can also
/// leave documents behind by running out of time (`timed_out`), which it reports as a flag rather
/// than as a failure entry. So the counts have to agree as well: `deleted` must reach `total`, the
/// number of documents the initial search matched. A body reporting `timed_out: true, total: 2,
/// deleted: 1` with no conflicts and an empty `failures` array is a partial delete that every
/// itemised signal calls clean.
fn inspect_delete_response(resp: &Value, es_index: &str) -> Result<()> {
    // Elasticsearch and OpenSearch both always include these in a synchronous `_delete_by_query`
    // response. A missing one means the body is not one — a `wait_for_completion` task handle, or
    // a proxy's envelope — and neither confirms the delete applied. `total` and `deleted` are
    // required for the same reason `failures` is: without them the delete cannot be confirmed,
    // and coercing an absent count to a convenient default would manufacture that confirmation.
    //
    // Each arm reports `shape` rather than the body itself: the body can carry document ids (its
    // `failures` entries do), and the shape is what distinguishes an async task handle from a
    // proxy's error envelope.
    let Some(failures) = resp.get("failures").and_then(Value::as_array) else {
        return UnexpectedDeleteResponseSnafu {
            index: es_index.to_string(),
            field: "failures",
            shape: write::describe_unexpected_response(resp),
        }
        .fail();
    };
    let Some(total) = resp.get("total").and_then(Value::as_u64) else {
        return UnexpectedDeleteResponseSnafu {
            index: es_index.to_string(),
            field: "total",
            shape: write::describe_unexpected_response(resp),
        }
        .fail();
    };
    let Some(deleted) = resp.get("deleted").and_then(Value::as_u64) else {
        return UnexpectedDeleteResponseSnafu {
            index: es_index.to_string(),
            field: "deleted",
            shape: write::describe_unexpected_response(resp),
        }
        .fail();
    };
    // Present but not a number is a rewritten body, not a zero — reading it as one would report a
    // conflicted delete as clean.
    let version_conflicts = match resp.get("version_conflicts").map(Value::as_u64) {
        None => 0,
        Some(Some(count)) => count,
        Some(None) => {
            return UnexpectedDeleteResponseSnafu {
                index: es_index.to_string(),
                field: "version_conflicts",
                shape: write::describe_unexpected_response(resp),
            }
            .fail();
        }
    };
    // Absent means the request did not report a timeout, which is the claim being tested — unlike
    // the counts above, reading it as `false` asserts nothing that the body denies. Present but not
    // a boolean is a rewritten body rather than that silence, and reading it as `false` would
    // assert the one thing this field exists to deny, so it is rejected like `version_conflicts`.
    let timed_out = match resp.get("timed_out").map(Value::as_bool) {
        None => false,
        Some(Some(flag)) => flag,
        Some(None) => {
            return UnexpectedDeleteResponseSnafu {
                index: es_index.to_string(),
                field: "timed_out",
                shape: write::describe_unexpected_response(resp),
            }
            .fail();
        }
    };

    // `deleted` counts documents drawn from the `total` the initial search matched, so it cannot
    // exceed it. A body where it does is not a response this function can read a verdict from;
    // saturating the difference to zero would turn that contradiction into a clean delete.
    let Some(undeleted) = total.checked_sub(deleted) else {
        return UnexpectedDeleteResponseSnafu {
            index: es_index.to_string(),
            field: "deleted",
            shape: write::describe_unexpected_response(resp),
        }
        .fail();
    };

    if failures.is_empty() && version_conflicts == 0 && undeleted == 0 && !timed_out {
        return Ok(());
    }

    let first = match failures.first() {
        Some(failure) => describe_delete_failure(failure),
        // Conflicts alone: `conflicts=abort` (the default) stops the request and reports the
        // count, and older versions report it without a matching `failures` entry. A timeout or a
        // short `deleted` count has no failure entry to describe either.
        None => "no failure entry; the delete stopped before every matching document was deleted"
            .to_string(),
    };

    DeleteByQueryPartiallyAppliedSnafu {
        index: es_index.to_string(),
        failures: failures.len(),
        version_conflicts,
        undeleted,
        total,
        timed_out,
        first,
    }
    .fail()
}

/// Describe a `_delete_by_query` failure entry from a whitelist of non-identifying fields.
///
/// A failure entry names the document it belongs to (`id` is the row's primary key, see
/// [`write::extract_primary_key_from_fields`]) and Elasticsearch's free-form `cause.reason` quotes
/// it too — a version conflict reads `[<_id>]: version conflict, current version [2] is different
/// than the one provided [1]`. This error is logged by every caller and recorded in
/// `runtime.task_history`, so only fixed vocabulary is reported: the HTTP `status` and the
/// exception class names, each through [`write::categorical_token`] — never `reason`, never `id`,
/// never the entry itself.
fn describe_delete_failure(failure: &Value) -> String {
    let mut parts = Vec::with_capacity(3);

    if let Some(status) = failure.get("status").and_then(Value::as_u64) {
        parts.push(format!("status {status}"));
    }

    let cause = failure.get("cause");
    if let Some(kind) = cause.and_then(|c| c.get("type")).and_then(Value::as_str) {
        parts.push(write::categorical_token(kind).to_string());
    }
    if let Some(caused_by) = cause
        .and_then(|c| c.get("caused_by"))
        .and_then(|c| c.get("type"))
        .and_then(Value::as_str)
    {
        parts.push(format!("caused by {}", write::categorical_token(caused_by)));
    }

    if parts.is_empty() {
        // Neither a status nor a typed cause: say so rather than falling back to stringifying the
        // entry, which would name the document directly.
        parts.push("no status or cause type reported".to_string());
    }

    parts.join(", ")
}

/// Builds `{"ids": {"values": ["<_id>", ...]}}` — the documents written for `keys`, addressed by
/// the `_id` the write path derives for each row.
///
/// Rows whose key is NULL (any component, for a composite key) yield no `_id`: the write path
/// skips them rather than writing under a generated `_id`, so there is no document to delete.
/// Returns `None` when that leaves nothing to address, so the caller issues no request.
fn build_ids_query(
    primary_key: &[Field],
    es_index: &str,
    keys: &RecordBatch,
) -> DataFusionResult<Option<Value>> {
    let ids = write::extract_primary_key_from_fields(primary_key, es_index, keys)
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    let values: Vec<Value> = ids.into_iter().flatten().map(Value::String).collect();
    if values.is_empty() {
        return Ok(None);
    }

    Ok(Some(json!({ "ids": { "values": values } })))
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
    use std::sync::Mutex;

    use arrow::array::{Int64Array, StringArray};
    use arrow_schema::{DataType, Schema};
    use elasticsearch::{
        Error as EsError, MappingResponse, Result as EsResult, SearchRequest, SearchResponse,
    };
    use std::sync::Arc;

    use super::*;
    use crate::index::chunking::{CHUNKED_INDEX_CHUNK_KEY, ChunkedSearchIndex};
    use arrow::array::{ArrayRef, UInt64Array};

    /// A fully-applied `_delete_by_query` response, in the shape Elasticsearch returns it.
    fn clean_delete_response(deleted: u64) -> Value {
        json!({
            "took": 1,
            "timed_out": false,
            "total": deleted,
            "deleted": deleted,
            "batches": 1,
            "version_conflicts": 0,
            "noops": 0,
            "retries": {"bulk": 0, "search": 0},
            "throttled_millis": 0,
            "failures": [],
        })
    }

    /// Records the `_delete_by_query` bodies it is asked to issue and answers each with a
    /// configured response; every other trait method is an error, so a test that reaches one
    /// fails loudly rather than silently passing.
    #[derive(Debug)]
    struct RecordingClient {
        queries: Mutex<Vec<Value>>,
        /// One response per request, in order; the last one answers every request beyond it, so a
        /// single-element list answers a whole multi-request delete the same way.
        responses: Vec<Value>,
    }

    impl Default for RecordingClient {
        fn default() -> Self {
            Self::answering(clean_delete_response(0))
        }
    }

    impl RecordingClient {
        /// Answers every request with `response` instead of a fully-applied one.
        fn answering(response: Value) -> Self {
            Self::answering_in_turn(vec![response])
        }

        /// Answers the nth request with the nth response, so a test can make one request in a
        /// multi-request delete differ from the rest.
        fn answering_in_turn(responses: Vec<Value>) -> Self {
            Self {
                queries: Mutex::new(Vec::new()),
                responses,
            }
        }

        fn queries(&self) -> Vec<Value> {
            self.queries
                .lock()
                .expect("queries mutex should not be poisoned")
                .clone()
        }
    }

    fn unexpected(method: &str) -> EsError {
        EsError::ElasticsearchError {
            status: 500,
            message: format!("unexpected call to {method}"),
        }
    }

    #[async_trait::async_trait]
    impl Elasticsearch for RecordingClient {
        async fn delete_by_query(&self, _index: &str, query: &Value) -> EsResult<Value> {
            let issued = {
                let mut queries = self
                    .queries
                    .lock()
                    .expect("queries mutex should not be poisoned");
                queries.push(query.clone());
                queries.len()
            };
            let response = self
                .responses
                .get(issued - 1)
                .or_else(|| self.responses.last())
                .expect("the stub should be configured with at least one response");
            Ok(response.clone())
        }

        async fn get_mapping(&self, _index: &str) -> EsResult<MappingResponse> {
            Err(unexpected("get_mapping"))
        }
        async fn search(&self, _index: &str, _body: &SearchRequest) -> EsResult<SearchResponse> {
            Err(unexpected("search"))
        }
        async fn search_raw(&self, _index: &str, _body: &Value) -> EsResult<SearchResponse> {
            Err(unexpected("search_raw"))
        }
        async fn open_point_in_time(&self, _index: &str, _keep_alive: &str) -> EsResult<String> {
            Err(unexpected("open_point_in_time"))
        }
        async fn search_point_in_time(&self, _body: &Value) -> EsResult<SearchResponse> {
            Err(unexpected("search_point_in_time"))
        }
        async fn close_point_in_time(&self, _pit_id: &str) -> EsResult<()> {
            Err(unexpected("close_point_in_time"))
        }
        async fn index_exists(&self, _index: &str) -> EsResult<bool> {
            Err(unexpected("index_exists"))
        }
        async fn create_index(&self, _index: &str, _body: &Value) -> EsResult<Value> {
            Err(unexpected("create_index"))
        }
        async fn put_mapping(&self, _index: &str, _body: &Value) -> EsResult<Value> {
            Err(unexpected("put_mapping"))
        }
        async fn get_index_refresh_interval(&self, _index: &str) -> EsResult<Option<String>> {
            Err(unexpected("get_index_refresh_interval"))
        }
        async fn put_index_settings(&self, _index: &str, _body: &Value) -> EsResult<Value> {
            Err(unexpected("put_index_settings"))
        }
        async fn refresh_index(&self, _index: &str) -> EsResult<Value> {
            Err(unexpected("refresh_index"))
        }
        async fn force_merge(&self, _index: &str, _max_num_segments: u32) -> EsResult<Value> {
            Err(unexpected("force_merge"))
        }
        async fn index_document(&self, _index: &str, _id: &str, _doc: &Value) -> EsResult<Value> {
            Err(unexpected("index_document"))
        }
        async fn bulk_index(
            &self,
            _index: &str,
            _docs: &[(Option<String>, Value)],
        ) -> EsResult<Value> {
            Err(unexpected("bulk_index"))
        }
    }

    fn string_key_batch(values: Vec<Option<&str>>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, true)]));
        RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(values))])
            .expect("string key batch should build")
    }

    fn pk(name: &str, data_type: DataType) -> Field {
        Field::new(name, data_type, true)
    }

    /// The reported bug: a string key that the standard analyzer would split into several tokens
    /// (`ORDER-1024` → `[order, 1024]`) can never be matched by an unanalyzed `term` query, so
    /// the delete must address the document by `_id` instead.
    #[tokio::test]
    async fn string_primary_key_deletes_by_document_id_not_by_term_filter() {
        let client = RecordingClient::default();
        let primary_key = vec![pk("id", DataType::Utf8)];

        delete_by_keys(
            &client,
            "idx",
            &primary_key,
            &["id".to_string()],
            &string_key_batch(vec![Some("ORDER-1024"), Some("a716-446655440000")]),
        )
        .await
        .expect("delete should succeed");

        assert_eq!(
            client.queries(),
            vec![json!({"ids": {"values": ["ORDER-1024", "a716-446655440000"]}})],
            "an exact-key delete must address documents by _id; a `term` filter on the key \
             column matches nothing for an analyzed string key"
        );
    }

    /// `_id` for a composite key is the JSON encoding the write path stores. Pinning the literal
    /// here (and not re-deriving it) is what catches the two paths drifting apart.
    #[tokio::test]
    async fn composite_primary_key_uses_the_json_encoded_document_id() {
        let client = RecordingClient::default();
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, true),
            Field::new("region", DataType::Utf8, true),
        ]));
        let keys = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![Some("ORDER-1024")])),
                Arc::new(StringArray::from(vec![Some("emea")])),
            ],
        )
        .expect("composite key batch should build");

        delete_by_keys(
            &client,
            "idx",
            &[pk("id", DataType::Utf8), pk("region", DataType::Utf8)],
            &["id".to_string(), "region".to_string()],
            &keys,
        )
        .await
        .expect("delete should succeed");

        assert_eq!(
            client.queries(),
            vec![json!({"ids": {"values": [r#"{"id":"ORDER-1024","region":"emea"}"#]}})],
        );
    }

    /// The write path derives the same `_id`s it deletes — the invariant the fix rests on.
    #[test]
    fn delete_ids_match_the_document_ids_the_write_path_derives() {
        let keys = string_key_batch(vec![Some("ORDER-1024"), Some("x")]);
        let primary_key = vec![pk("id", DataType::Utf8)];

        let written = write::extract_primary_key_from_fields(&primary_key, "idx", &keys)
            .expect("write path should derive ids");
        let query = build_ids_query(&primary_key, "idx", &keys)
            .expect("ids query should build")
            .expect("ids query should be present");

        let addressed: Vec<Value> = written.into_iter().flatten().map(Value::String).collect();
        assert_eq!(query, json!({"ids": {"values": addressed}}));
    }

    /// A strict subset of the primary key (the chunked-index case) cannot use `_id`, because the
    /// chunk id is part of it and unknown at delete time — that path still filters on columns.
    #[tokio::test]
    async fn partial_key_falls_back_to_term_filters() {
        let client = RecordingClient::default();

        delete_by_keys(
            &client,
            "idx",
            &[
                pk("id", DataType::Utf8),
                pk("_spice.chunk_id", DataType::Int64),
            ],
            &["id".to_string()],
            &string_key_batch(vec![Some("ORDER-1024")]),
        )
        .await
        .expect("delete should succeed");

        assert_eq!(
            client.queries(),
            vec![json!({
                "bool": {
                    "should": [{"bool": {"filter": [{"term": {"id": "ORDER-1024"}}]}}],
                    "minimum_should_match": 1
                }
            })],
        );
    }

    /// A NULL key has no stable identity, so the write path never stored a document for it and
    /// there is nothing to address. A batch of only such rows must issue no request at all —
    /// never an unconstrained query that would match everything.
    #[tokio::test]
    async fn null_keys_issue_no_request() {
        let client = RecordingClient::default();

        delete_by_keys(
            &client,
            "idx",
            &[pk("id", DataType::Utf8)],
            &["id".to_string()],
            &string_key_batch(vec![None, None]),
        )
        .await
        .expect("delete should succeed");

        assert!(client.queries().is_empty());
    }

    #[tokio::test]
    async fn null_keys_are_skipped_but_present_keys_are_still_deleted() {
        let client = RecordingClient::default();

        delete_by_keys(
            &client,
            "idx",
            &[pk("id", DataType::Utf8)],
            &["id".to_string()],
            &string_key_batch(vec![None, Some("kept"), None]),
        )
        .await
        .expect("delete should succeed");

        assert_eq!(client.queries(), vec![json!({"ids": {"values": ["kept"]}})],);
    }

    #[tokio::test]
    async fn empty_batch_issues_no_request() {
        let client = RecordingClient::default();

        delete_by_keys(
            &client,
            "idx",
            &[pk("id", DataType::Utf8)],
            &["id".to_string()],
            &string_key_batch(vec![]),
        )
        .await
        .expect("delete should succeed");

        assert!(client.queries().is_empty());
    }

    /// A large delete is split so no single request carries an unbounded id list.
    #[tokio::test]
    async fn large_batch_is_split_into_bounded_requests() {
        let client = RecordingClient::default();
        let rows = DELETE_CHUNK_ROWS + 3;
        let values: Vec<String> = (0..rows).map(|i| format!("key-{i}")).collect();
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, true)]));
        let keys = RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(values.clone()))])
            .expect("large key batch should build");

        delete_by_keys(
            &client,
            "idx",
            &[pk("id", DataType::Utf8)],
            &["id".to_string()],
            &keys,
        )
        .await
        .expect("delete should succeed");

        let queries = client.queries();
        assert_eq!(queries.len(), 2);

        let ids_of = |q: &Value| -> Vec<String> {
            q["ids"]["values"]
                .as_array()
                .expect("ids.values should be an array")
                .iter()
                .map(|v| v.as_str().expect("each id should be a string").to_string())
                .collect()
        };
        assert_eq!(ids_of(&queries[0]).len(), DELETE_CHUNK_ROWS);
        assert_eq!(ids_of(&queries[1]).len(), 3);

        // Every key is addressed exactly once, across the split.
        let mut seen: Vec<String> = queries.iter().flat_map(ids_of).collect();
        seen.sort();
        let mut expected = values;
        expected.sort();
        assert_eq!(seen, expected);
    }

    /// An integer key was never broken by the mapping gap (it maps to `long`, which `term`
    /// matches), but it must keep working now that it goes through `_id` too.
    #[tokio::test]
    async fn integer_primary_key_still_deletes() {
        let client = RecordingClient::default();
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
        let keys = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![7, 8]))])
            .expect("int key batch should build");

        delete_by_keys(
            &client,
            "idx",
            &[pk("id", DataType::Int64)],
            &["id".to_string()],
            &keys,
        )
        .await
        .expect("delete should succeed");

        assert_eq!(
            client.queries(),
            vec![json!({"ids": {"values": ["7", "8"]}})]
        );
    }

    /// An index with no primary key writes documents under generated `_id`s, so there is no id
    /// to address and no key column to filter on; the delete must not emit a match-everything
    /// query.
    #[tokio::test]
    async fn empty_primary_key_issues_no_request() {
        let client = RecordingClient::default();

        delete_by_keys(
            &client,
            "idx",
            &[],
            &[],
            &string_key_batch(vec![Some("ORDER-1024")]),
        )
        .await
        .expect("delete should succeed");

        assert!(client.queries().is_empty());
    }

    /// The reported bug (#12364): `_delete_by_query` answers `200` with a populated `failures`
    /// array when individual documents could not be deleted, so discarding the body reports a
    /// delete that left documents behind as a success — search keeps returning rows the dataset
    /// no longer has.
    #[tokio::test]
    async fn a_document_failure_is_not_reported_as_a_successful_delete() {
        let client = RecordingClient::answering(json!({
            "total": 2,
            "deleted": 1,
            "version_conflicts": 0,
            "failures": [{
                "index": "idx",
                "id": "ORDER-1024",
                "cause": {
                    "type": "mapper_parsing_exception",
                    "reason": "[ORDER-1024] failed to parse",
                    "caused_by": {"type": "illegal_argument_exception"},
                },
                "status": 400,
            }],
        }));

        let err = delete_by_keys(
            &client,
            "idx",
            &[pk("id", DataType::Utf8)],
            &["id".to_string()],
            &string_key_batch(vec![Some("ORDER-1024"), Some("ORDER-1025")]),
        )
        .await
        .expect_err("a partially applied delete must not report success");

        let message = err.to_string();
        assert!(
            message.contains("1 document failure(s)"),
            "the error should count the failures: {message}"
        );
        assert!(
            message.contains("status 400")
                && message.contains("mapper_parsing_exception")
                && message.contains("caused by illegal_argument_exception"),
            "the error should name the failure's fixed vocabulary: {message}"
        );
    }

    /// A version conflict is the common partial-delete shape: `_delete_by_query` snapshots the
    /// index at the start of the request and skips a document whose version moved since, which a
    /// concurrent write cycle over the same rows produces. Elasticsearch reports it as a count,
    /// so a body with no `failures` entry at all is still a partial delete.
    #[tokio::test]
    async fn a_version_conflict_alone_is_not_reported_as_a_successful_delete() {
        let client = RecordingClient::answering(json!({
            "total": 3,
            "deleted": 1,
            "version_conflicts": 2,
            "failures": [],
        }));

        let err = delete_by_keys(
            &client,
            "idx",
            &[pk("id", DataType::Utf8)],
            &["id".to_string()],
            &string_key_batch(vec![Some("ORDER-1024")]),
        )
        .await
        .expect_err("a delete stopped by a version conflict must not report success");

        assert!(
            err.to_string().contains("2 version conflict(s)"),
            "the error should count the conflicts: {err}"
        );
    }

    /// The document `_id` is the row's primary key and Elasticsearch's free-form `cause.reason`
    /// quotes it. Every caller of `delete_by_keys` logs its error and it reaches
    /// `runtime.task_history`, so neither may appear in the message.
    #[tokio::test]
    async fn a_failure_never_reports_the_document_id_or_the_free_form_reason() {
        let client = RecordingClient::answering(json!({
            "total": 1,
            "deleted": 0,
            "version_conflicts": 1,
            "failures": [{
                "index": "idx",
                "id": "SENTINEL-ROW-VALUE-9F3A",
                "cause": {
                    "type": "version_conflict_engine_exception",
                    "reason": "[SENTINEL-ROW-VALUE-9F3A]: version conflict, current version [2] is different than the one provided [1]",
                },
                "status": 409,
            }],
        }));

        let err = delete_by_keys(
            &client,
            "idx",
            &[pk("id", DataType::Utf8)],
            &["id".to_string()],
            &string_key_batch(vec![Some("SENTINEL-ROW-VALUE-9F3A")]),
        )
        .await
        .expect_err("a partially applied delete must not report success");

        let message = err.to_string();
        assert!(
            !message.contains("SENTINEL-ROW-VALUE-9F3A"),
            "the document id must not reach the error: {message}"
        );
        assert!(
            !message.contains("version conflict, current version"),
            "the free-form reason must not reach the error: {message}"
        );
        assert!(
            message.contains("version_conflict_engine_exception"),
            "the exception class is the part that may be reported: {message}"
        );
        assert!(
            !message.contains('\n'),
            "the message must stay on one line: {message}"
        );
    }

    /// A failure entry carrying neither a status nor a typed cause must still be described from
    /// the whitelist — falling back to stringifying the entry would name the document.
    #[tokio::test]
    async fn a_failure_with_no_typed_cause_is_still_described_without_the_entry() {
        let client = RecordingClient::answering(json!({
            "total": 1,
            "deleted": 0,
            "version_conflicts": 0,
            "failures": [{"id": "SENTINEL-ROW-VALUE-9F3A"}],
        }));

        let err = delete_by_keys(
            &client,
            "idx",
            &[pk("id", DataType::Utf8)],
            &["id".to_string()],
            &string_key_batch(vec![Some("SENTINEL-ROW-VALUE-9F3A")]),
        )
        .await
        .expect_err("a partially applied delete must not report success");

        let message = err.to_string();
        assert!(
            message.contains("no status or cause type reported"),
            "the error should say the entry carried nothing reportable: {message}"
        );
        assert!(
            !message.contains("SENTINEL-ROW-VALUE-9F3A"),
            "the document id must not reach the error: {message}"
        );
    }

    /// A cause type that does not have the shape of an Elasticsearch exception class is
    /// network-provided text, so it is replaced wholesale rather than copied into the error.
    #[tokio::test]
    async fn a_non_categorical_cause_type_is_replaced_not_copied() {
        let client = RecordingClient::answering(json!({
            "total": 1,
            "deleted": 0,
            "version_conflicts": 0,
            "failures": [{
                "status": 400,
                "cause": {"type": "rejected: SENTINEL-ROW-VALUE-9F3A"},
            }],
        }));

        let err = delete_by_keys(
            &client,
            "idx",
            &[pk("id", DataType::Utf8)],
            &["id".to_string()],
            &string_key_batch(vec![Some("ORDER-1024")]),
        )
        .await
        .expect_err("a partially applied delete must not report success");

        let message = err.to_string();
        assert!(
            !message.contains("SENTINEL-ROW-VALUE-9F3A"),
            "a non-categorical cause type must not be copied into the error: {message}"
        );
        assert!(
            message.contains("<unrecognized>"),
            "the rejected token should be replaced: {message}"
        );
    }

    /// A body with no `failures` array is not a synchronous `_delete_by_query` response — an
    /// async task handle, or a proxy's envelope. Neither confirms the delete applied, so it
    /// cannot be reported as a success.
    #[tokio::test]
    async fn a_response_without_a_failures_array_is_not_reported_as_a_successful_delete() {
        let client = RecordingClient::answering(json!({"task": "node-1:12345"}));

        let err = delete_by_keys(
            &client,
            "idx",
            &[pk("id", DataType::Utf8)],
            &["id".to_string()],
            &string_key_batch(vec![Some("ORDER-1024")]),
        )
        .await
        .expect_err("an unconfirmable delete must not report success");

        let message = err.to_string();
        assert!(
            message.contains("no usable `failures`")
                && message.contains("a JSON object with keys: task"),
            "the error should describe the response by its shape alone: {message}"
        );
    }

    /// `timed_out` is the one partial-delete signal Elasticsearch reports as a flag rather than
    /// as a failure entry: the request stopped early, so documents the query matched are still
    /// indexed even though every itemised count reads clean.
    #[tokio::test]
    async fn a_timed_out_delete_is_not_reported_as_a_successful_delete() {
        let client = RecordingClient::answering(json!({
            "timed_out": true,
            "total": 2,
            "deleted": 1,
            "version_conflicts": 0,
            "failures": [],
        }));

        let err = delete_by_keys(
            &client,
            "idx",
            &[pk("id", DataType::Utf8)],
            &["id".to_string()],
            &string_key_batch(vec![Some("ORDER-1024"), Some("ORDER-1025")]),
        )
        .await
        .expect_err("a delete that timed out must not report success");

        let message = err.to_string();
        assert!(
            message.contains("timed out: true"),
            "the error should report the timeout: {message}"
        );
        assert!(
            message.contains("1 of 2 matched document(s) left in place"),
            "the error should count the documents left behind: {message}"
        );
    }

    /// `deleted` falling short of `total` means documents the query matched were not removed,
    /// whatever the itemised signals say. Nothing else in the body reports this shape.
    #[tokio::test]
    async fn a_short_deleted_count_is_not_reported_as_a_successful_delete() {
        let client = RecordingClient::answering(json!({
            "timed_out": false,
            "total": 5,
            "deleted": 3,
            "version_conflicts": 0,
            "failures": [],
        }));

        let err = delete_by_keys(
            &client,
            "idx",
            &[pk("id", DataType::Utf8)],
            &["id".to_string()],
            &string_key_batch(vec![Some("ORDER-1024")]),
        )
        .await
        .expect_err("a delete that left documents behind must not report success");

        assert!(
            err.to_string()
                .contains("2 of 5 matched document(s) left in place"),
            "the error should count the documents left behind: {err}"
        );
    }

    /// The counts are required, not defaulted: a body without them cannot confirm the delete, and
    /// reading an absent `deleted` as zero — or as "as many as matched" — would invent a verdict
    /// the response never gave.
    #[tokio::test]
    async fn a_response_without_the_document_counts_is_not_reported_as_a_successful_delete() {
        for missing in ["total", "deleted"] {
            // The same fully-applied body, minus the one count under test.
            let body = match missing {
                "total" => json!({"deleted": 1, "version_conflicts": 0, "failures": []}),
                _ => json!({"total": 1, "version_conflicts": 0, "failures": []}),
            };
            let client = RecordingClient::answering(body);

            let err = delete_by_keys(
                &client,
                "idx",
                &[pk("id", DataType::Utf8)],
                &["id".to_string()],
                &string_key_batch(vec![Some("ORDER-1024")]),
            )
            .await
            .expect_err("an unconfirmable delete must not report success");

            assert!(
                err.to_string().contains(&format!("no usable `{missing}`")),
                "the error should name the missing field: {err}"
            );
        }
    }

    /// A `version_conflicts` that is present but not a number is a rewritten body. Coercing it to
    /// zero would report a conflicted delete as clean — the exact failure this check exists to
    /// catch.
    #[tokio::test]
    async fn a_non_numeric_version_conflicts_is_not_read_as_zero() {
        let client = RecordingClient::answering(json!({
            "total": 1,
            "deleted": 1,
            "version_conflicts": "0",
            "failures": [],
        }));

        let err = delete_by_keys(
            &client,
            "idx",
            &[pk("id", DataType::Utf8)],
            &["id".to_string()],
            &string_key_batch(vec![Some("ORDER-1024")]),
        )
        .await
        .expect_err("an unconfirmable delete must not report success");

        assert!(
            err.to_string().contains("no usable `version_conflicts`"),
            "the error should name the unusable field: {err}"
        );
    }

    /// An absent `timed_out` asserts nothing the body denies, so it reads as `false`. A present one
    /// that is not a boolean is a rewritten body instead of that silence, and reading it as `false`
    /// would assert the timeout did not happen — the one claim the field exists to make.
    #[tokio::test]
    async fn a_non_boolean_timed_out_is_not_read_as_absent() {
        let client = RecordingClient::answering(json!({
            "timed_out": "true",
            "total": 1,
            "deleted": 1,
            "version_conflicts": 0,
            "failures": [],
        }));

        let err = delete_by_keys(
            &client,
            "idx",
            &[pk("id", DataType::Utf8)],
            &["id".to_string()],
            &string_key_batch(vec![Some("ORDER-1024")]),
        )
        .await
        .expect_err("an unconfirmable delete must not report success");

        assert!(
            err.to_string().contains("no usable `timed_out`"),
            "the error should name the unusable field: {err}"
        );
    }

    /// `deleted` counts documents drawn from the `total` the initial search matched, so a body
    /// reporting more deleted than matched is one no verdict can be read from. Saturating the
    /// difference to zero would read that contradiction as a fully-applied delete.
    #[tokio::test]
    async fn a_deleted_count_above_the_total_is_not_reported_as_a_successful_delete() {
        let client = RecordingClient::answering(json!({
            "timed_out": false,
            "total": 1,
            "deleted": 2,
            "version_conflicts": 0,
            "failures": [],
        }));

        let err = delete_by_keys(
            &client,
            "idx",
            &[pk("id", DataType::Utf8)],
            &["id".to_string()],
            &string_key_batch(vec![Some("ORDER-1024")]),
        )
        .await
        .expect_err("a contradictory delete response must not report success");

        assert!(
            err.to_string().contains("no usable `deleted`"),
            "the error should name the contradictory field: {err}"
        );
    }

    /// The happy path stays a success: a fully-applied delete reports `failures: []` and no
    /// conflicts, and every chunk of a multi-request delete is checked.
    #[tokio::test]
    async fn a_fully_applied_delete_succeeds_across_every_chunk() {
        let client = RecordingClient::answering(clean_delete_response(DELETE_CHUNK_ROWS as u64));
        let ids: Vec<String> = (0..=DELETE_CHUNK_ROWS).map(|i| i.to_string()).collect();

        delete_by_keys(
            &client,
            "idx",
            &[pk("id", DataType::Utf8)],
            &["id".to_string()],
            &string_key_batch(ids.iter().map(|s| Some(s.as_str())).collect()),
        )
        .await
        .expect("a fully applied delete should succeed");

        assert_eq!(
            client.queries().len(),
            2,
            "the batch should span two requests, both checked"
        );
    }

    /// A later chunk's partial delete is caught too — the check runs per request, not only on
    /// the first.
    #[tokio::test]
    async fn a_partial_delete_in_a_later_chunk_is_still_caught() {
        let client = RecordingClient::answering_in_turn(vec![
            clean_delete_response(DELETE_CHUNK_ROWS as u64),
            json!({"total": 1, "deleted": 0, "version_conflicts": 1, "failures": []}),
        ]);
        let ids: Vec<String> = (0..=DELETE_CHUNK_ROWS).map(|i| i.to_string()).collect();

        let err = delete_by_keys(
            &client,
            "idx",
            &[pk("id", DataType::Utf8)],
            &["id".to_string()],
            &string_key_batch(ids.iter().map(|s| Some(s.as_str())).collect()),
        )
        .await
        .expect_err("the second chunk's partial delete must surface");

        assert_eq!(
            client.queries().len(),
            2,
            "the first chunk should have been issued and accepted"
        );
        assert!(
            err.to_string().contains("1 version conflict(s)"),
            "the second chunk's conflict should be the reported one: {err}"
        );
    }

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
