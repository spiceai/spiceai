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
use elasticsearch::{Elasticsearch, FieldMapping};
use serde_json::{Value, json};
use snafu::ResultExt;

use super::write;
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

/// Where a user goes to fix a key column that Elasticsearch cannot match exactly.
const ES_VECTORS_DOCS: &str = "https://spiceai.org/docs/components/vectors/elasticsearch";

/// Deletes every document whose `key_columns` match a row of `keys` — an exact-key delete when
/// `key_columns` covers every `primary_key` column, a prefix delete when it's a strict subset
/// (the chunked-index case).
///
/// The two cases address documents differently, because filtering on a key *column* depends on
/// how that column is mapped. A key that Elasticsearch mapped dynamically as `text` has an
/// inverted index of *analyzed* tokens, and a `term` query is not analyzed — so
/// `{"term": {"id": "ORDER-1024"}}` looks for one token in an index holding `[order, 1024]` and
/// matches nothing, a delete that reports success having removed no documents (#12267).
///
/// So when `key_columns` covers the whole primary key, this addresses documents by `_id` via an
/// `ids` query. `_id` is the value the write path already stores for the row, derived by the
/// same [`write::extract_primary_key_from_fields`], so the delete matches exactly the documents
/// the write produced — no field mapping, no analysis, and no dependence on the key's type.
///
/// A strict subset of the key (the chunked-index case) cannot use `_id`, because the chunk id is
/// part of it and is unknown at delete time. That case filters on the key columns, and so has to
/// know how each one is actually indexed: [`resolve_term_fields`] reads the live mapping and
/// addresses the analyzed-string case through its `keyword` sub-field, rather than assuming the
/// bare field name is exact-matchable (#12272).
///
/// Only reads `key_columns` from `keys`, ignoring any other column present — `keys` may be
/// shaped by [`runtime_datafusion_index::Index::required_columns`] (a superset of the primary
/// key) rather than the primary key alone, since that's what the default
/// [`runtime_datafusion_index::Index::resolve_delete_keys`] resolves against.
///
/// Issues one `_delete_by_query` request per [`DELETE_CHUNK_ROWS`] addressed keys rather than a
/// single request for the whole batch, so a large delete can't build an unbounded clause or id
/// list.
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

    if addresses_whole_key {
        return delete_by_document_id(client, es_index, primary_key, keys).await;
    }
    if keys.num_rows() == 0 || key_columns.is_empty() {
        return Ok(());
    }

    // The mapping is a property of the index, not of the keys, so it is read once — and every
    // row's key is turned into a filter clause *before* the first request goes out. A delete
    // cannot be rolled back, so a key the 513th row cannot express must not be discovered after
    // the first 512 rows are already gone.
    let term_fields = resolve_term_fields(client, es_index, key_columns).await?;
    let row_clauses = build_row_term_clauses(es_index, &term_fields, keys)?;

    issue_chunked_deletes(client, es_index, &row_clauses, or_of_row_clauses).await
}

/// Deletes the documents `keys` names by the `_id` the write path derived for each row.
async fn delete_by_document_id(
    client: &dyn Elasticsearch,
    es_index: &str,
    primary_key: &[Field],
    keys: &RecordBatch,
) -> DataFusionResult<()> {
    let ids = document_ids(primary_key, es_index, keys)?;

    issue_chunked_deletes(client, es_index, &ids, ids_query).await
}

/// Issues one `_delete_by_query` per [`DELETE_CHUNK_ROWS`]-item slice of `items`, `query` turning
/// each slice into that request's query, and reads every response before moving on.
///
/// Both addressing paths go through here so that neither can grow a request whose response nobody
/// checks — the defect this shape exists to prevent (#12364).
///
/// A response that does not account for the documents it matched stops the delete where it is: the
/// remaining slices are not attempted, and the error says the index still holds rows the dataset
/// does not. Continuing would remove more but report the last slice's verdict for all of them.
async fn issue_chunked_deletes(
    client: &dyn Elasticsearch,
    es_index: &str,
    items: &[Value],
    query: impl Fn(&[Value]) -> Value,
) -> DataFusionResult<()> {
    for chunk in items.chunks(DELETE_CHUNK_ROWS) {
        let response = client
            .delete_by_query(es_index, &query(chunk))
            .await
            .boxed()
            .map_err(DataFusionError::External)?;
        ensure_delete_applied(es_index, &response)?;
    }

    Ok(())
}

/// The `_id`s the write path derived for `keys`, as the values of an `ids` query.
///
/// Rows whose key is NULL (any component, for a composite key) yield no `_id`: the write path
/// skips them rather than writing under a generated `_id`, so there is no document to delete.
fn document_ids(
    primary_key: &[Field],
    es_index: &str,
    keys: &RecordBatch,
) -> DataFusionResult<Vec<Value>> {
    let ids = write::extract_primary_key_from_fields(primary_key, es_index, keys)
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    Ok(ids.into_iter().flatten().map(Value::String).collect())
}

/// `{"ids": {"values": ["<_id>", ...]}}` — the documents to remove, addressed by `_id`.
fn ids_query(ids: &[Value]) -> Value {
    json!({ "ids": { "values": ids } })
}

/// Elasticsearch field types that index *analyzed* tokens, so an unanalyzed `term` query against
/// the bare field name cannot match the value that was written.
const ANALYZED_FIELD_TYPES: &[&str] = &[
    "text",
    "match_only_text",
    "annotated_text",
    "search_as_you_type",
];

/// How to address one key column in a `term` query: which field path actually holds an
/// exact-matchable copy of the value, and the `ignore_above` beyond which that copy does not
/// exist at all.
#[derive(Debug, Clone, PartialEq, Eq)]
struct TermField {
    /// The column to read values from in the key batch.
    column: String,
    /// The Elasticsearch field path to filter on — the column name, or a `keyword` sub-field of it.
    path: String,
    /// `keyword`'s character limit, when the resolved field declares one.
    ignore_above: Option<usize>,
}

impl TermField {
    /// The resolution for a column whose mapping makes the bare field name exact-matchable.
    fn bare(column: &str) -> Self {
        Self {
            column: column.to_string(),
            path: column.to_string(),
            ignore_above: None,
        }
    }
}

/// Resolves how to filter on each of `key_columns` by reading the index's live mapping.
///
/// Nothing guarantees a key column is exact-matchable under its own name. The runtime maps its
/// key columns as `keyword` when it creates the index, but an index that predates that — or one
/// the user pre-created, or one where the key is also a configured text field — can have the key
/// mapped as analyzed `text`, whose inverted index holds tokens (`ORDER-1024` → `[order, 1024]`)
/// that no `term` query can match.
///
/// Per column:
/// - mapped as an exact type (`keyword`, `long`, `boolean`, `date`, …) → filter on the bare name.
/// - mapped as analyzed text with a `keyword` sub-field → filter on `<column>.<sub-field>`. The
///   sub-field cannot simply be assumed: for a key mapped `keyword` outright the bare name is the
///   correct one, and for analyzed text without such a sub-field there is no exact-matchable copy
///   at all, which is an error rather than a delete that silently removes nothing.
/// - absent from the mapping, and the index maps new fields → the bare name. Nothing has ever been
///   indexed under it, so there is no document for the filter to match either way. Under
///   `dynamic: false` or `strict`, an absent field is *never* searchable however, so that is an
///   error instead.
/// - carrying a `normalizer` → an error. A normalizer applies to the `term` query's value too, so
///   two distinct keys can collapse to one term and the delete would remove *more* rows than asked.
///
/// Every rejection is a `term` filter that cannot address one row's key. Erring is the
/// conservative outcome in both directions: neither removing nothing nor removing too much.
async fn resolve_term_fields(
    client: &dyn Elasticsearch,
    es_index: &str,
    key_columns: &[String],
) -> DataFusionResult<Vec<TermField>> {
    let mapping = client
        .get_mapping(es_index)
        .await
        .boxed()
        .map_err(DataFusionError::External)?;

    // `es_index` may name an alias, in which case the response is keyed by the concrete indexes
    // behind it. Sorted so a mismatch is reported the same way every time.
    let mut indexes: Vec<_> = mapping.iter().collect();
    indexes.sort_by_key(|(name, _)| *name);

    key_columns
        .iter()
        .map(|column| {
            // Resolve against each index and require agreement — a column addressed one way in one
            // index and another way in the next cannot be deleted by a single query.
            let mut resolved: Option<TermField> = None;
            for (_, index) in &indexes {
                let candidate = match index.mappings.properties.get(column) {
                    Some(field) => resolve_one_term_field(es_index, column, field)?,
                    None if index.mappings.maps_new_fields() => TermField::bare(column),
                    // `dynamic: false` keeps an unmapped field in `_source` and never indexes it,
                    // so documents can carry the key while no `term` query can reach it.
                    None => {
                        return Err(DataFusionError::External(format!(
                            "Failed to delete from Elasticsearch index '{es_index}': primary key column '{column}' is not in the index mapping and the index does not map new fields (dynamic is not enabled), so Elasticsearch never indexed the key and the delete would remove nothing. Add '{column}' to the index mapping as 'keyword' and reindex the existing documents. See: {ES_VECTORS_DOCS}"
                        ).into()));
                    }
                };
                match &resolved {
                    None => resolved = Some(candidate),
                    Some(previous) if previous == &candidate => {}
                    Some(previous) => {
                        return Err(DataFusionError::External(format!(
                            "Failed to delete from Elasticsearch index '{es_index}': primary key column '{column}' is indexed as '{}' in one of the index's mappings and '{}' in another, so one delete cannot address both. Point the dataset at a single index, or give the column the same mapping in each.",
                            previous.path, candidate.path
                        ).into()));
                    }
                }
            }
            Ok(resolved.unwrap_or_else(|| TermField::bare(column)))
        })
        .collect()
}

/// Resolves one key column against its [`FieldMapping`]; see [`resolve_term_fields`].
fn resolve_one_term_field(
    es_index: &str,
    column: &str,
    field: &FieldMapping,
) -> DataFusionResult<TermField> {
    let field_type = field.field_type.as_deref().unwrap_or_default();
    if !ANALYZED_FIELD_TYPES.contains(&field_type) {
        ensure_addresses_one_value(es_index, column, column, field)?;
        return Ok(TermField {
            column: column.to_string(),
            path: column.to_string(),
            ignore_above: field.ignore_above,
        });
    }

    // Prefer the conventional `keyword` name, then the first remaining candidate by name — a
    // multi-field map is unordered, and which sub-field a delete filters on cannot be a coin flip.
    let mut candidates: Vec<_> = field
        .fields
        .iter()
        .flatten()
        .filter(|(_, sub)| sub.field_type.as_deref() == Some("keyword"))
        .collect();
    candidates.sort_by(|(a, _), (b, _)| {
        (a.as_str() != "keyword", a.as_str()).cmp(&(b.as_str() != "keyword", b.as_str()))
    });

    let Some((sub_name, sub)) = candidates.first() else {
        return Err(DataFusionError::External(
            format!(
                "Failed to delete from Elasticsearch index '{es_index}': primary key column '{column}' is mapped as '{field_type}', which indexes analyzed tokens rather than the key itself, and has no 'keyword' sub-field to match exactly against. Deleting a row would silently remove nothing. Re-create the index with '{column}' mapped as 'keyword' and reindex the existing documents — adding a sub-field to the live mapping does not populate it for documents already indexed. See: {ES_VECTORS_DOCS}"
            )
            .into(),
        ));
    };

    let path = format!("{column}.{sub_name}");
    ensure_addresses_one_value(es_index, column, &path, sub)?;

    Ok(TermField {
        column: column.to_string(),
        path,
        ignore_above: sub.ignore_above,
    })
}

/// Rejects a resolved field whose `term` query would not address exactly the key it is given.
///
/// A `normalizer` is applied to the indexed value *and* to a `term` query's value, so two distinct
/// keys can reduce to the same term — `{"term": {"id": "bar"}}` against a lowercase-normalized
/// field also matches the row keyed `BÀR`. The `_id` path is immune because it compares the raw
/// key, so accepting a normalized field here would make a partial-key delete remove *more* rows
/// than the exact-key delete would: worse than the under-deletion this whole path guards against.
fn ensure_addresses_one_value(
    es_index: &str,
    column: &str,
    path: &str,
    field: &FieldMapping,
) -> DataFusionResult<()> {
    if let Some(normalizer) = field.normalizer.as_deref() {
        return Err(DataFusionError::External(
            format!(
                "Failed to delete from Elasticsearch index '{es_index}': primary key column '{column}' is indexed at '{path}' with normalizer '{normalizer}', which rewrites the key both when indexing and when matching, so one key can match another row's documents and the delete could remove rows it was not asked to. Re-create the index with '{column}' mapped as 'keyword' without a normalizer, and reindex the existing documents. See: {ES_VECTORS_DOCS}"
            )
            .into(),
        ));
    }

    // `index: false` — the value is in `_source` but has no inverted index, so no query reaches it.
    // A declared non-filterable metadata column that is also the primary key lands here.
    if field.index == Some(false) {
        return Err(DataFusionError::External(
            format!(
                "Failed to delete from Elasticsearch index '{es_index}': primary key column '{column}' is mapped at '{path}' with index=false, so Elasticsearch stores the key without indexing it and no filter can match it — the delete would remove nothing. Declare '{column}' as filterable, or re-create the index with it mapped as an indexed 'keyword', and reindex the existing documents. See: {ES_VECTORS_DOCS}"
            )
            .into(),
        ));
    }

    Ok(())
}

/// One `{"bool": {"filter": [{"term": {...}}, ...]}}` clause per row of `keys` whose key can be
/// addressed — `term_fields` ANDed together. Rows whose key is NULL contribute no clause: the
/// write path stored no document for them, so there is nothing to remove.
///
/// Every row is converted here, before any request is issued, so a key that cannot be expressed
/// fails the whole delete rather than half of it.
fn build_row_term_clauses(
    es_index: &str,
    term_fields: &[TermField],
    keys: &RecordBatch,
) -> DataFusionResult<Vec<Value>> {
    let arrays: Vec<_> = term_fields
        .iter()
        .map(|f| keys.column_by_name(&f.column).cloned())
        .collect::<Option<Vec<_>>>()
        .ok_or_else(|| {
            let names: Vec<&str> = term_fields.iter().map(|f| f.column.as_str()).collect();
            DataFusionError::Plan(format!(
                "delete key batch is missing one of the requested key columns: {names:?}"
            ))
        })?;

    let mut row_clauses = Vec::with_capacity(keys.num_rows());
    for row in 0..keys.num_rows() {
        let mut terms = Vec::with_capacity(term_fields.len());
        for (term_field, array) in term_fields.iter().zip(&arrays) {
            let value = ScalarValue::try_from_array(array.as_ref(), row)?;
            if value.is_null() {
                // A NULL key component has no stable identity, so the write path never stored a
                // document under it — skip this row's clause rather than emit a filter that would
                // match everything.
                terms.clear();
                break;
            }
            let json_value = scalar_to_term_value(&value).ok_or_else(|| {
                DataFusionError::External(format!(
                    "Failed to delete from Elasticsearch index '{es_index}': primary key column '{column}' has type {data_type}, which this delete cannot express as an exact-match filter, so it would remove nothing. Use a string or integer primary key for a chunked Elasticsearch index. See: {ES_VECTORS_DOCS}",
                    column = term_field.column,
                    data_type = value.data_type(),
                ).into())
            })?;
            ensure_within_ignore_above(es_index, term_field, &json_value)?;
            terms.push(json!({ "term": { term_field.path.as_str(): json_value } }));
        }
        if !terms.is_empty() {
            row_clauses.push(json!({ "bool": { "filter": terms } }));
        }
    }

    Ok(row_clauses)
}

/// `{"bool": {"should": [<row clause>, ...], "minimum_should_match": 1}}` — the rows ORed together.
fn or_of_row_clauses(row_clauses: &[Value]) -> Value {
    json!({
        "bool": {
            "should": row_clauses,
            "minimum_should_match": 1
        }
    })
}

/// How many `failures` entries an error names. Enough to diagnose the cause, bounded so a chunk
/// that fails for every one of its rows cannot build an unbounded message.
const REPORTED_DELETE_FAILURES: usize = 3;

/// `_delete_by_query` answers `200` for a request that only partially applied, so the response
/// body — not the status — says whether the documents are gone (#12364):
///
/// - `failures` carries the per-document rejections. The request succeeded; those deletes did not.
/// - `version_conflicts` counts documents whose version moved between the snapshot the request
///   scans and the delete itself, which a concurrent write over the same rows produces. The
///   default `conflicts=abort` stops the whole operation at the first one, leaving the rest of
///   that request's documents in place.
/// - `timed_out` marks a search phase that did not finish, so the scan never reached every
///   matching document.
///
/// Each leaves documents the caller asked to remove still searchable, which is the symptom
/// reported for a delete that "succeeded". The response must also *be* a `_delete_by_query`
/// response: every Elasticsearch version answers with a `failures` array, empty or not, so a body
/// without one is not evidence that anything was deleted — the same reasoning as the write path's
/// `errors` check on a `_bulk` response.
fn ensure_delete_applied(es_index: &str, response: &Value) -> DataFusionResult<()> {
    let Some(failures) = response.get("failures").and_then(Value::as_array) else {
        return Err(delete_not_applied(
            es_index,
            "Elasticsearch answered the delete without a 'failures' array, so the response does not report what it removed",
        ));
    };

    if !failures.is_empty() {
        let reported: Vec<String> = failures
            .iter()
            .take(REPORTED_DELETE_FAILURES)
            .map(describe_failure)
            .collect();
        return Err(delete_not_applied(
            es_index,
            &format!(
                "Elasticsearch rejected {} of the documents the delete matched, so they are still in the index. First {}: {}",
                failures.len(),
                reported.len(),
                reported.join("; ")
            ),
        ));
    }

    if let Some(canceled) = response.get("canceled") {
        if canceled.as_bool() == Some(true) {
            return Err(delete_not_applied(
                es_index,
                "the delete was cancelled before it finished, so Elasticsearch stopped short of the documents it had matched",
            ));
        }
        if let Some(reason) = canceled.as_str() {
            return Err(delete_not_applied(
                es_index,
                &format!(
                    "the delete was cancelled before it finished ('{reason}'), so Elasticsearch stopped short of the documents it had matched"
                ),
            ));
        }
    }

    let version_conflicts = response
        .get("version_conflicts")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    if version_conflicts > 0 {
        return Err(delete_not_applied(
            es_index,
            &format!(
                "{version_conflicts} of the documents the delete matched changed while it ran, so Elasticsearch left them in the index"
            ),
        ));
    }

    if response.get("timed_out").and_then(Value::as_bool) == Some(true) {
        return Err(delete_not_applied(
            es_index,
            "Elasticsearch timed out searching for the documents to delete, so it did not reach every matching document",
        ));
    }

    // The counters are the backstop: whatever ended a request early — a cancellation shape this
    // does not know, a field that arrived malformed and read as absent — shows up as fewer
    // documents deleted than the request matched.
    let (Some(total), Some(deleted)) = (
        response.get("total").and_then(Value::as_u64),
        response.get("deleted").and_then(Value::as_u64),
    ) else {
        return Err(delete_not_applied(
            es_index,
            "Elasticsearch answered the delete without both a 'total' and a 'deleted' count, so the response does not report whether it removed what it matched",
        ));
    };
    if deleted < total {
        return Err(delete_not_applied(
            es_index,
            &format!(
                "Elasticsearch removed {deleted} of the {total} documents the delete matched, without reporting why the rest were left"
            ),
        ));
    }

    Ok(())
}

/// One `failures` entry reduced to what diagnoses it: Elasticsearch's status and the cause's type.
///
/// Deliberately not the whole entry. A failure carries the document's `_id` — which the write
/// path derives from the row's primary key — and a free-form `reason` that quotes it, so
/// stringifying the entry copies primary keys into every log line a routine version conflict
/// produces. The callers of `delete_by_keys` log the error, so this text is operational output.
fn describe_failure(failure: &Value) -> String {
    let status = failure.get("status").and_then(Value::as_u64);
    // `_delete_by_query` reports a document-level rejection under `cause` and a search-phase one
    // under `reason`; both nest the class of failure under `type`.
    let cause = failure
        .get("cause")
        .or_else(|| failure.get("reason"))
        .and_then(|cause| cause.get("type"))
        .and_then(Value::as_str);
    match (status, cause) {
        (Some(status), Some(cause)) => format!("status {status}: {cause}"),
        (Some(status), None) => format!("status {status}"),
        (None, Some(cause)) => cause.to_string(),
        (None, None) => "no status or cause reported".to_string(),
    }
}

/// The shared message for a `_delete_by_query` that answered `200` having left documents behind.
fn delete_not_applied(es_index: &str, detail: &str) -> DataFusionError {
    DataFusionError::External(
        format!(
            "Failed to delete from Elasticsearch index '{es_index}': {detail}. Search can keep returning rows the dataset no longer has until the index is back in sync — re-run the dataset refresh, and reduce writes against the index while a delete runs. See: {ES_VECTORS_DOCS}"
        )
        .into(),
    )
}

/// Elasticsearch does not index a `keyword` value longer than the field's `ignore_above`, so a
/// `term` query for such a key matches nothing. Report that rather than issue a delete that
/// succeeds having removed the row from nowhere.
fn ensure_within_ignore_above(
    es_index: &str,
    term_field: &TermField,
    value: &Value,
) -> DataFusionResult<()> {
    let (Some(limit), Value::String(text)) = (term_field.ignore_above, value) else {
        return Ok(());
    };
    // `ignore_above` counts characters, not bytes.
    let length = text.chars().count();
    if length <= limit {
        return Ok(());
    }
    let column = &term_field.column;
    let path = &term_field.path;
    // The limit belongs to whichever field the filter actually addresses, which is the column
    // itself only when its own mapping is exact-matchable — otherwise it is the `keyword`
    // sub-field the resolver picked, and re-mapping the column alone would not lift the limit.
    let remedy = if path == column {
        format!("Re-create the index with '{column}' mapped as 'keyword' without an 'ignore_above'")
    } else {
        format!(
            "Re-create the index so the key is exact-matchable with no 'ignore_above' — either map '{column}' as 'keyword' outright, or give its analyzed mapping a 'keyword' sub-field that declares no 'ignore_above'"
        )
    };
    Err(DataFusionError::External(
        format!(
            "Failed to delete from Elasticsearch index '{es_index}': primary key column '{column}' has a key of {length} characters, but the delete filters on '{path}', which the index maps with ignore_above={limit}, so Elasticsearch never indexed that key and the delete would remove nothing. {remedy}, and reindex the existing documents. See: {ES_VECTORS_DOCS}"
        )
        .into(),
    ))
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
    use std::sync::atomic::{AtomicU32, Ordering};

    use arrow::array::{Int64Array, StringArray};
    use arrow_schema::{DataType, Schema};
    use elasticsearch::{
        Error as EsError, MappingResponse, Result as EsResult, SearchRequest, SearchResponse,
    };
    use std::sync::Arc;

    use super::*;
    use crate::index::chunking::{CHUNKED_INDEX_CHUNK_KEY, ChunkedSearchIndex};
    use arrow::array::{ArrayRef, UInt64Array};

    /// Records the `_delete_by_query` bodies it is asked to issue, and serves `mapping` (when set)
    /// as the index's live mapping; every other trait method is an error, so a test that reaches
    /// one fails loudly rather than silently passing.
    ///
    /// `get_mapping` errors while `mapping` is `None`, which is what pins that the exact-key
    /// (`_id`) path never consults the mapping at all.
    #[derive(Debug, Default)]
    struct RecordingClient {
        queries: Mutex<Vec<Value>>,
        mapping: Option<MappingResponse>,
        get_mapping_calls: AtomicU32,
        /// What `delete_by_query` answers with; `None` serves [`clean_delete_response`].
        delete_response: Option<Value>,
    }

    impl RecordingClient {
        /// A client serving one index whose `properties` are the given `(field, mapping)` pairs,
        /// as Elasticsearch's `GET /<index>/_mapping` would return them.
        fn with_properties(properties: &[(&str, Value)]) -> Self {
            Self::with_indexes(&[("idx", properties)])
        }

        /// A client serving several concrete indexes — what `get_mapping` returns for an alias.
        fn with_indexes(indexes: &[(&str, &[(&str, Value)])]) -> Self {
            let mapping = indexes
                .iter()
                .map(|(index, properties)| {
                    let properties: serde_json::Map<String, Value> = properties
                        .iter()
                        .map(|(name, mapping)| ((*name).to_string(), mapping.clone()))
                        .collect();
                    let index_mapping = serde_json::from_value(json!({
                        "mappings": { "properties": properties }
                    }))
                    .expect("index mapping should deserialize");
                    ((*index).to_string(), index_mapping)
                })
                .collect();
            Self {
                mapping: Some(mapping),
                ..Default::default()
            }
        }

        /// Answers every `_delete_by_query` with `response` instead of a clean one.
        fn answering(mut self, response: Value) -> Self {
            self.delete_response = Some(response);
            self
        }

        fn queries(&self) -> Vec<Value> {
            self.queries
                .lock()
                .expect("queries mutex should not be poisoned")
                .clone()
        }
    }

    /// What Elasticsearch answers for a `_delete_by_query` that applied in full — the shape the
    /// production code reads, so a test that never sets `delete_response` still exercises it.
    fn clean_delete_response(deleted: u64) -> Value {
        json!({
            "took": 1,
            "timed_out": false,
            "total": deleted,
            "deleted": deleted,
            "version_conflicts": 0,
            "noops": 0,
            "failures": [],
        })
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
            self.queries
                .lock()
                .expect("queries mutex should not be poisoned")
                .push(query.clone());
            Ok(self
                .delete_response
                .clone()
                .unwrap_or_else(|| clean_delete_response(0)))
        }

        async fn get_mapping(&self, _index: &str) -> EsResult<MappingResponse> {
            self.get_mapping_calls.fetch_add(1, Ordering::AcqRel);
            self.mapping
                .clone()
                .ok_or_else(|| unexpected("get_mapping"))
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

    /// A `_delete_by_query` that Elasticsearch answers `200` for can still have removed nothing:
    /// per-document rejections come back in `failures` while the request itself succeeded. The
    /// documents are still searchable, so this must not be reported as a completed delete.
    #[tokio::test]
    async fn per_document_failures_are_not_a_successful_delete() {
        let client = RecordingClient::default().answering(json!({
            "took": 4,
            "timed_out": false,
            "total": 2,
            "deleted": 1,
            "version_conflicts": 0,
            "failures": [{
                "index": "idx",
                "id": "ORDER-1024",
                "status": 409,
                "cause": {"type": "version_conflict_engine_exception", "reason": "current version is newer"},
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
        .expect_err("a delete Elasticsearch only partly applied must not report success");

        let message = err.to_string();
        assert!(
            message.contains("index 'idx'") && message.contains("1 of the documents"),
            "the error must name the index and how many were rejected: {message}"
        );
        assert!(
            message.contains("version_conflict_engine_exception"),
            "the error must carry the failure Elasticsearch reported: {message}"
        );
    }

    /// Under the default `conflicts=abort`, a document whose version moved since the request's
    /// snapshot stops the operation and is counted rather than deleted — reported without a
    /// `failures` entry when an intermediary sets `conflicts=proceed`, so the counter is read too.
    #[tokio::test]
    async fn version_conflicts_are_not_a_successful_delete() {
        let client = RecordingClient::default().answering(json!({
            "timed_out": false,
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
        .expect_err("documents left behind by a version conflict must not report success");

        assert!(
            err.to_string().contains("2 of the documents"),
            "the error must give the conflict count: {err}"
        );
    }

    /// A search phase that timed out never reached every matching document, so the delete is
    /// incomplete however many it did remove.
    #[tokio::test]
    async fn a_timed_out_delete_is_not_a_successful_delete() {
        let client = RecordingClient::default().answering(json!({
            "timed_out": true,
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
        .expect_err("a timed-out delete must not report success");

        assert!(
            err.to_string().contains("timed out"),
            "the error must say the search timed out: {err}"
        );
    }

    /// Every Elasticsearch version answers `_delete_by_query` with a `failures` array, empty or
    /// not. A body without one does not report what was removed, so it is not evidence of a
    /// completed delete — the same treatment `_bulk`'s missing `errors` field gets on the write
    /// path.
    #[tokio::test]
    async fn a_response_that_reports_nothing_is_not_a_successful_delete() {
        let client = RecordingClient::default().answering(json!({"acknowledged": true}));

        let err = delete_by_keys(
            &client,
            "idx",
            &[pk("id", DataType::Utf8)],
            &["id".to_string()],
            &string_key_batch(vec![Some("ORDER-1024")]),
        )
        .await
        .expect_err("a response that does not report failures must not be read as success");

        assert!(
            err.to_string().contains("'failures'"),
            "the error must say what the response was missing: {err}"
        );
    }

    /// A `_delete_by_query` runs as a task that can be cancelled through the task API. The
    /// response then reports the reason and stops short of the documents it had already matched.
    #[tokio::test]
    async fn a_cancelled_delete_is_not_a_successful_delete() {
        let client = RecordingClient::default().answering(json!({
            "timed_out": false,
            "total": 10,
            "deleted": 4,
            "version_conflicts": 0,
            "canceled": "by user request",
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
        .expect_err("a cancelled delete must not report success");

        assert!(
            err.to_string().contains("by user request"),
            "the error must carry the cancellation reason: {err}"
        );
    }

    /// The counters are the backstop for a partial delete this code has no named check for: fewer
    /// documents removed than matched, with nothing in the response saying why.
    #[tokio::test]
    async fn removing_fewer_documents_than_matched_is_not_a_successful_delete() {
        let client = RecordingClient::default().answering(json!({
            "timed_out": false,
            "total": 7,
            "deleted": 5,
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
        .expect_err(
            "a delete that removed fewer documents than it matched must not report success",
        );

        assert!(
            err.to_string().contains("5 of the 7 documents"),
            "the error must give both counts: {err}"
        );
    }

    /// A response that reports no counts cannot show the delete applied, so it is not a success
    /// either — an empty `failures` array on its own says only that nothing was *rejected*.
    #[tokio::test]
    async fn a_response_without_counts_is_not_a_successful_delete() {
        let client = RecordingClient::default().answering(json!({"failures": []}));

        let err = delete_by_keys(
            &client,
            "idx",
            &[pk("id", DataType::Utf8)],
            &["id".to_string()],
            &string_key_batch(vec![Some("ORDER-1024")]),
        )
        .await
        .expect_err("a response that reports no counts must not be read as a completed delete");

        assert!(
            err.to_string().contains("'deleted'"),
            "the error must say what the response was missing: {err}"
        );
    }

    /// The `_id` Elasticsearch reports a failure for is the row's primary key, and the failure's
    /// free-form `reason` quotes it. The callers of `delete_by_keys` log the error, so neither may
    /// reach it — a version conflict on a dataset keyed by an email address must not copy that
    /// address into an operator's log.
    #[tokio::test]
    async fn a_failure_does_not_report_the_row_key() {
        let client = RecordingClient::default().answering(json!({
            "timed_out": false,
            "total": 1,
            "deleted": 0,
            "version_conflicts": 0,
            "failures": [{
                "index": "idx",
                "id": "person@example.com",
                "status": 409,
                "cause": {
                    "type": "version_conflict_engine_exception",
                    "reason": "[person@example.com]: version conflict, current version [3] is different than the one provided [2]",
                },
            }],
        }));

        let err = delete_by_keys(
            &client,
            "idx",
            &[pk("id", DataType::Utf8)],
            &["id".to_string()],
            &string_key_batch(vec![Some("person@example.com")]),
        )
        .await
        .expect_err("a rejected delete must not report success");

        let message = err.to_string();
        assert!(
            !message.contains("person@example.com"),
            "the row's key must not reach the error: {message}"
        );
        assert!(
            message.contains("status 409") && message.contains("version_conflict_engine_exception"),
            "the error must still say what Elasticsearch rejected: {message}"
        );
    }

    /// The partial-key path issues its own requests, so it reads its own responses — a partially
    /// applied chunked delete is the case #12272's key-mapping fix cannot detect on its own.
    #[tokio::test]
    async fn a_partial_key_delete_reads_its_response_too() {
        let client = RecordingClient::with_properties(&[("id", json!({"type": "keyword"}))])
            .answering(json!({
                "timed_out": false,
                "total": 1,
                "deleted": 0,
                "version_conflicts": 1,
                "failures": [],
            }));
        let (primary_key, key_columns) = chunked_key("id", DataType::Utf8);

        delete_by_keys(
            &client,
            "idx",
            &primary_key,
            &key_columns,
            &string_key_batch(vec![Some("ORDER-1024")]),
        )
        .await
        .expect_err("the term-filter path must not report a partially applied delete as success");
    }

    /// A delete large enough to be split must stop at the request that failed: continuing would
    /// report the last chunk's verdict for the whole batch, and the error names an index whose
    /// remaining keys were never even attempted.
    #[tokio::test]
    async fn a_failed_chunk_stops_the_remaining_requests() {
        let client = RecordingClient::default().answering(json!({
            "timed_out": false,
            "total": 1,
            "deleted": 0,
            "version_conflicts": 1,
            "failures": [],
        }));
        let values: Vec<String> = (0..=DELETE_CHUNK_ROWS).map(|i| format!("k{i}")).collect();
        let keys = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, true)])),
            vec![Arc::new(StringArray::from(values))],
        )
        .expect("large key batch should build");

        delete_by_keys(
            &client,
            "idx",
            &[pk("id", DataType::Utf8)],
            &["id".to_string()],
            &keys,
        )
        .await
        .expect_err("a chunk that did not apply must fail the delete");

        assert_eq!(
            client.queries().len(),
            1,
            "the second chunk must not be issued after the first one came back incomplete"
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
        let query =
            ids_query(&document_ids(&primary_key, "idx", &keys).expect("ids should derive"));

        let addressed: Vec<Value> = written.into_iter().flatten().map(Value::String).collect();
        assert_eq!(query, json!({"ids": {"values": addressed}}));
    }

    /// A strict subset of the primary key (the chunked-index case) cannot use `_id`, because the
    /// chunk id is part of it and unknown at delete time — that path still filters on columns.
    /// An index the runtime created maps the key as `keyword`, so the bare field name is exact.
    #[tokio::test]
    async fn partial_key_falls_back_to_term_filters() {
        let client = RecordingClient::with_properties(&[("id", json!({"type": "keyword"}))]);

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

    /// The chunked key columns a partial-key delete filters on, as a chunked index hands them over.
    fn chunked_key(name: &str, data_type: DataType) -> (Vec<Field>, Vec<String>) {
        (
            vec![
                pk(name, data_type),
                pk(CHUNKED_INDEX_CHUNK_KEY, DataType::UInt64),
            ],
            vec![name.to_string()],
        )
    }

    /// The reported bug (#12272): a key that Elasticsearch mapped dynamically as analyzed `text`
    /// is not matchable under its own name, so the partial-key delete has to address the `keyword`
    /// sub-field. Filtering on the bare name would remove nothing while reporting success.
    #[tokio::test]
    async fn a_dynamically_mapped_text_key_is_deleted_through_its_keyword_sub_field() {
        let client = RecordingClient::with_properties(&[(
            "id",
            json!({
                "type": "text",
                "fields": { "keyword": { "type": "keyword", "ignore_above": 256 } },
            }),
        )]);
        let (primary_key, key_columns) = chunked_key("id", DataType::Utf8);

        delete_by_keys(
            &client,
            "idx",
            &primary_key,
            &key_columns,
            &string_key_batch(vec![Some("ORDER-1024")]),
        )
        .await
        .expect("delete should succeed");

        assert_eq!(
            client.queries(),
            vec![json!({
                "bool": {
                    "should": [{"bool": {"filter": [{"term": {"id.keyword": "ORDER-1024"}}]}}],
                    "minimum_should_match": 1
                }
            })],
            "an analyzed key must be matched through its keyword sub-field, not the bare field"
        );
    }

    /// The sub-field cannot simply be assumed: for a key mapped `keyword` outright there is no
    /// `id.keyword`, and filtering on it would match nothing.
    #[tokio::test]
    async fn a_keyword_mapped_key_is_deleted_on_the_bare_field_name() {
        let client = RecordingClient::with_properties(&[("id", json!({"type": "keyword"}))]);
        let (primary_key, key_columns) = chunked_key("id", DataType::Utf8);

        delete_by_keys(
            &client,
            "idx",
            &primary_key,
            &key_columns,
            &string_key_batch(vec![Some("ORDER-1024")]),
        )
        .await
        .expect("delete should succeed");

        assert_eq!(
            client.queries()[0]["bool"]["should"][0]["bool"]["filter"],
            json!([{ "term": { "id": "ORDER-1024" } }]),
        );
    }

    /// An analyzed key with no exact-matchable copy anywhere cannot be deleted at all. Reporting
    /// that beats issuing a query that removes nothing and returns success.
    #[tokio::test]
    async fn an_analyzed_key_with_no_keyword_sub_field_is_an_error() {
        let client = RecordingClient::with_properties(&[("id", json!({"type": "text"}))]);
        let (primary_key, key_columns) = chunked_key("id", DataType::Utf8);

        let err = delete_by_keys(
            &client,
            "idx",
            &primary_key,
            &key_columns,
            &string_key_batch(vec![Some("ORDER-1024")]),
        )
        .await
        .expect_err("an unmatchable key must not report a successful delete");

        let message = err.to_string();
        assert!(
            message.contains("'id'") && message.contains("keyword"),
            "the error must name the column and the fix: {message}"
        );
        assert!(
            client.queries().is_empty(),
            "no delete request should be issued when the key cannot be matched"
        );
    }

    /// `ignore_above` is the second way a `keyword` field silently fails to match: Elasticsearch
    /// does not index a value longer than the limit, so the key is simply absent from the index.
    #[tokio::test]
    async fn a_key_longer_than_ignore_above_is_an_error() {
        let client = RecordingClient::with_properties(&[(
            "id",
            json!({"type": "keyword", "ignore_above": 8}),
        )]);
        let (primary_key, key_columns) = chunked_key("id", DataType::Utf8);

        let err = delete_by_keys(
            &client,
            "idx",
            &primary_key,
            &key_columns,
            &string_key_batch(vec![Some("ORDER-1024")]),
        )
        .await
        .expect_err("a key Elasticsearch never indexed must not report a successful delete");

        let message = err.to_string();
        assert!(
            message.contains("ignore_above=8") && message.contains("10 characters"),
            "the error must give both the limit and the key's length: {message}"
        );
        assert!(
            !message.contains("sub-field"),
            "the column's own mapping carries the limit here, so the remedy is the column: {message}"
        );
        assert!(client.queries().is_empty());
    }

    /// The limit that stops the delete belongs to the field the filter addresses, which for an
    /// analyzed key is the `keyword` sub-field. Re-mapping the column alone would not lift it, so
    /// the error has to name the sub-field it read the limit from and say what to change.
    #[tokio::test]
    async fn a_key_longer_than_a_sub_fields_ignore_above_names_the_sub_field() {
        let client = RecordingClient::with_properties(&[(
            "id",
            json!({
                "type": "text",
                "fields": { "keyword": { "type": "keyword", "ignore_above": 8 } },
            }),
        )]);
        let (primary_key, key_columns) = chunked_key("id", DataType::Utf8);

        let err = delete_by_keys(
            &client,
            "idx",
            &primary_key,
            &key_columns,
            &string_key_batch(vec![Some("ORDER-1024")]),
        )
        .await
        .expect_err("a key the sub-field never indexed must not report a successful delete");

        let message = err.to_string();
        assert!(
            message.contains("'id.keyword'"),
            "the error must name the field the delete filters on: {message}"
        );
        assert!(
            message.contains("sub-field"),
            "the remedy must cover the sub-field, not just the column: {message}"
        );
        assert!(
            message.contains("index 'idx'"),
            "the error must name the index: {message}"
        );
        assert!(client.queries().is_empty());
    }

    /// The limit is a character count, not a byte count, so a multi-byte key within it still
    /// deletes — erring the other way would reject keys Elasticsearch indexed perfectly well.
    #[tokio::test]
    async fn a_multi_byte_key_within_ignore_above_still_deletes() {
        let client = RecordingClient::with_properties(&[(
            "id",
            json!({"type": "keyword", "ignore_above": 4}),
        )]);
        let (primary_key, key_columns) = chunked_key("id", DataType::Utf8);

        // 4 characters, 12 UTF-8 bytes.
        delete_by_keys(
            &client,
            "idx",
            &primary_key,
            &key_columns,
            &string_key_batch(vec![Some("注文書類")]),
        )
        .await
        .expect("a key within the character limit should delete");

        assert_eq!(client.queries().len(), 1);
    }

    /// A key column absent from the mapping has never had a value indexed under it, so there is no
    /// document for the filter to match either way — the bare name is the right query and this is
    /// not an error.
    #[tokio::test]
    async fn a_key_column_absent_from_the_mapping_uses_the_bare_field_name() {
        let client = RecordingClient::with_properties(&[("other", json!({"type": "keyword"}))]);
        let (primary_key, key_columns) = chunked_key("id", DataType::Utf8);

        delete_by_keys(
            &client,
            "idx",
            &primary_key,
            &key_columns,
            &string_key_batch(vec![Some("ORDER-1024")]),
        )
        .await
        .expect("delete should succeed");

        assert_eq!(
            client.queries()[0]["bool"]["should"][0]["bool"]["filter"],
            json!([{ "term": { "id": "ORDER-1024" } }]),
        );
    }

    /// `get_mapping` on an alias answers for each concrete index behind it. One `_delete_by_query`
    /// cannot address a column mapped two different ways, so that is reported rather than half
    /// applied.
    #[tokio::test]
    async fn an_alias_whose_indexes_map_the_key_differently_is_an_error() {
        let text_key = json!({
            "type": "text",
            "fields": { "keyword": { "type": "keyword" } },
        });
        let client = RecordingClient::with_indexes(&[
            ("idx-000001", &[("id", json!({"type": "keyword"}))]),
            ("idx-000002", &[("id", text_key)]),
        ]);
        let (primary_key, key_columns) = chunked_key("id", DataType::Utf8);

        let err = delete_by_keys(
            &client,
            "alias",
            &primary_key,
            &key_columns,
            &string_key_batch(vec![Some("ORDER-1024")]),
        )
        .await
        .expect_err("an ambiguous mapping must not be silently resolved one way");

        assert!(err.to_string().contains("'id'"), "unexpected error: {err}");
        assert!(client.queries().is_empty());
    }

    /// The mapping is a property of the index, not of the keys, so a delete large enough to be
    /// split across requests must still read it once.
    #[tokio::test]
    async fn the_mapping_is_read_once_per_delete_not_once_per_request() {
        let client = RecordingClient::with_properties(&[("id", json!({"type": "keyword"}))]);
        let rows = DELETE_CHUNK_ROWS + 3;
        let values: Vec<String> = (0..rows).map(|i| format!("key-{i}")).collect();
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, true)]));
        let keys = RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(values))])
            .expect("large key batch should build");
        let (primary_key, key_columns) = chunked_key("id", DataType::Utf8);

        delete_by_keys(&client, "idx", &primary_key, &key_columns, &keys)
            .await
            .expect("delete should succeed");

        assert_eq!(
            client.queries().len(),
            2,
            "the delete should still be split"
        );
        assert_eq!(client.get_mapping_calls.load(Ordering::Acquire), 1);
    }

    /// The exact-key path addresses documents by `_id` and so must not depend on — or even read —
    /// the mapping. `RecordingClient::default()` errors from `get_mapping`, which is what proves it.
    #[tokio::test]
    async fn the_exact_key_path_never_reads_the_mapping() {
        let client = RecordingClient::default();

        delete_by_keys(
            &client,
            "idx",
            &[pk("id", DataType::Utf8)],
            &["id".to_string()],
            &string_key_batch(vec![Some("ORDER-1024")]),
        )
        .await
        .expect("delete should succeed");

        assert_eq!(client.get_mapping_calls.load(Ordering::Acquire), 0);
    }

    /// A batch with nothing to delete must not spend a mapping round-trip either.
    #[tokio::test]
    async fn an_empty_partial_key_batch_reads_no_mapping_and_issues_no_request() {
        let client = RecordingClient::default();
        let (primary_key, key_columns) = chunked_key("id", DataType::Utf8);

        delete_by_keys(
            &client,
            "idx",
            &primary_key,
            &key_columns,
            &string_key_batch(vec![]),
        )
        .await
        .expect("delete should succeed");

        assert_eq!(client.get_mapping_calls.load(Ordering::Acquire), 0);
        assert!(client.queries().is_empty());
    }

    /// A `normalizer` rewrites the key when indexing *and* when matching, so `bar` matches the row
    /// keyed `BÀR`. That makes the partial-key delete remove **more** rows than the exact-key
    /// (`_id`) delete would — worse than the under-deletion this path exists to fix.
    #[tokio::test]
    async fn a_normalized_keyword_key_is_an_error_because_it_could_over_delete() {
        let client = RecordingClient::with_properties(&[(
            "id",
            json!({"type": "keyword", "normalizer": "lowercase"}),
        )]);
        let (primary_key, key_columns) = chunked_key("id", DataType::Utf8);

        let err = delete_by_keys(
            &client,
            "idx",
            &primary_key,
            &key_columns,
            &string_key_batch(vec![Some("BAR")]),
        )
        .await
        .expect_err("a normalized key could match another row's documents");

        let message = err.to_string();
        assert!(
            message.contains("normalizer 'lowercase'") && message.contains("'id'"),
            "the error must name the normalizer and the column: {message}"
        );
        assert!(client.queries().is_empty());
    }

    /// The same hazard reached through a text field's `keyword` sub-field.
    #[tokio::test]
    async fn a_normalized_keyword_sub_field_is_also_an_error() {
        let client = RecordingClient::with_properties(&[(
            "id",
            json!({
                "type": "text",
                "fields": { "keyword": { "type": "keyword", "normalizer": "lowercase" } },
            }),
        )]);
        let (primary_key, key_columns) = chunked_key("id", DataType::Utf8);

        let err = delete_by_keys(
            &client,
            "idx",
            &primary_key,
            &key_columns,
            &string_key_batch(vec![Some("BAR")]),
        )
        .await
        .expect_err("a normalized sub-field could match another row's documents");

        assert!(err.to_string().contains("id.keyword"), "unexpected: {err}");
    }

    /// `index: false` stores the key in `_source` without an inverted index, so no filter reaches
    /// it. A primary-key column the user declared `non-filterable` lands here.
    #[tokio::test]
    async fn an_unindexed_key_is_an_error() {
        let client =
            RecordingClient::with_properties(&[("id", json!({"type": "keyword", "index": false}))]);
        let (primary_key, key_columns) = chunked_key("id", DataType::Utf8);

        let err = delete_by_keys(
            &client,
            "idx",
            &primary_key,
            &key_columns,
            &string_key_batch(vec![Some("ORDER-1024")]),
        )
        .await
        .expect_err("an unindexed key cannot be matched");

        assert!(
            err.to_string().contains("index=false"),
            "unexpected error: {err}"
        );
        assert!(client.queries().is_empty());
    }

    /// Under `dynamic: false` an unmapped field stays in `_source` and is never indexed, so
    /// "absent from the mapping" no longer implies "no document carries it".
    #[tokio::test]
    async fn a_key_absent_from_a_non_dynamic_mapping_is_an_error() {
        let index_mapping = serde_json::from_value(json!({
            "mappings": { "dynamic": false, "properties": { "other": {"type": "keyword"} } }
        }))
        .expect("index mapping should deserialize");
        let client = RecordingClient {
            mapping: Some([("idx".to_string(), index_mapping)].into_iter().collect()),
            ..Default::default()
        };
        let (primary_key, key_columns) = chunked_key("id", DataType::Utf8);

        let err = delete_by_keys(
            &client,
            "idx",
            &primary_key,
            &key_columns,
            &string_key_batch(vec![Some("ORDER-1024")]),
        )
        .await
        .expect_err("an unmapped key under dynamic:false is never searchable");

        assert!(
            err.to_string().contains("does not map new fields"),
            "unexpected error: {err}"
        );
        assert!(client.queries().is_empty());
    }

    /// A key type the filter cannot express is not the same thing as a NULL key: NULL means no
    /// document was written, while an unsupported type means one was and cannot be addressed.
    /// Conflating them turns the second into a delete that removes nothing and returns success.
    #[tokio::test]
    async fn an_unsupported_key_type_is_an_error_not_a_skipped_row() {
        let client = RecordingClient::with_properties(&[("day", json!({"type": "date"}))]);
        let keys = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("day", DataType::Date32, true)])),
            vec![Arc::new(arrow::array::Date32Array::from(vec![19_723])) as ArrayRef],
        )
        .expect("date key batch should build");

        let err = delete_by_keys(
            &client,
            "idx",
            &[
                pk("day", DataType::Date32),
                pk(CHUNKED_INDEX_CHUNK_KEY, DataType::UInt64),
            ],
            &["day".to_string()],
            &keys,
        )
        .await
        .expect_err("an inexpressible key must not report a successful delete");

        assert!(
            err.to_string().contains("'day'"),
            "the error must name the column: {err}"
        );
        assert!(client.queries().is_empty());
    }

    /// A delete cannot be rolled back, so a key the last row cannot express must be found before
    /// the first request goes out — not after 512 rows have already been removed.
    #[tokio::test]
    async fn a_key_that_cannot_be_expressed_fails_before_any_row_is_deleted() {
        let client = RecordingClient::with_properties(&[(
            "id",
            json!({"type": "keyword", "ignore_above": 8}),
        )]);
        // The over-long key is in the second request's slice, past the first full chunk.
        let mut values: Vec<String> = (0..DELETE_CHUNK_ROWS).map(|i| format!("k{i}")).collect();
        values.push("a-key-past-the-limit".to_string());
        let keys = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, true)])),
            vec![Arc::new(StringArray::from(values))],
        )
        .expect("large key batch should build");
        let (primary_key, key_columns) = chunked_key("id", DataType::Utf8);

        delete_by_keys(&client, "idx", &primary_key, &key_columns, &keys)
            .await
            .expect_err("the unmatchable key must fail the delete");

        assert!(
            client.queries().is_empty(),
            "no row may be deleted when one of them cannot be addressed"
        );
    }

    /// A non-string key is exact-matchable under its own name whatever the mapping says about
    /// analysis, and carries no `ignore_above` to trip over.
    #[tokio::test]
    async fn an_integer_partial_key_is_deleted_on_the_bare_field_name() {
        let client = RecordingClient::with_properties(&[("id", json!({"type": "long"}))]);
        let (primary_key, key_columns) = chunked_key("id", DataType::Int64);
        let keys = base_keys(&[7]);

        delete_by_keys(&client, "idx", &primary_key, &key_columns, &keys)
            .await
            .expect("delete should succeed");

        assert_eq!(
            client.queries()[0]["bool"]["should"][0]["bool"]["filter"],
            json!([{ "term": { "id": 7 } }]),
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

    fn id_field() -> Field {
        Field::new("id", DataType::Int64, false)
    }

    /// The term fields for a chunked index's base key, resolved as an exactly-mapped index would.
    fn bare_term_fields(primary_key: &[Field]) -> Vec<TermField> {
        document_key_columns(primary_key)
            .iter()
            .map(|c| TermField::bare(c))
            .collect()
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
        let query = or_of_row_clauses(
            &build_row_term_clauses("idx", &bare_term_fields(&chunked), &base_keys(&[7]))
                .expect("clauses build"),
        );

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
        let full: Vec<TermField> = ChunkedSearchIndex::augment_primary_key(vec![id_field()])
            .iter()
            .map(|f| TermField::bare(f.name()))
            .collect();

        let err = build_row_term_clauses("idx", &full, &base_keys(&[7]))
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

        let query = or_of_row_clauses(
            &build_row_term_clauses("idx", &bare_term_fields(&chunked), &keys)
                .expect("clauses build"),
        );

        assert_eq!(
            query["bool"]["should"][0]["bool"]["filter"],
            serde_json::json!([{ "term": { "id": 7 } }]),
            "the chunk id must not narrow the delete: {query}"
        );
    }
}
