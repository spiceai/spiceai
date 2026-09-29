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

use std::collections::HashMap;
use std::collections::hash_map::Entry;

use arrow::array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use datafusion::common::ScalarValue;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use elasticsearch::{Elasticsearch, FieldMapping};
use serde_json::{Value, json};
use snafu::Snafu;

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

    #[snafu(display(
        "Failed to delete rows from the search index '{index}' (elasticsearch): its field mapping could not be read, so it is not known which field an exact-match filter on the key columns {columns} has to name; the delete was not issued. Cause: {source}"
    ))]
    KeyColumnMappingUnreadable {
        index: String,
        columns: String,
        source: elasticsearch::Error,
    },

    #[snafu(display(
        "Failed to delete rows from the search index '{index}' (elasticsearch): key column '{column}' is mapped `{mapped_as}`, which no exact-match filter can address — a `text` mapping indexes analyzed tokens rather than the value, and an unsearchable mapping indexes nothing — and it has no exact-match sub-field either, so the delete was not issued. Re-create the index and let the runtime map '{column}': it maps a string key as a searchable `keyword`, and every other key type to that type's own exact mapping. Elasticsearch cannot change an existing field's type. See: https://spiceai.org/docs/features/search"
    ))]
    KeyColumnNotExactlyMatchable {
        index: String,
        column: String,
        mapped_as: String,
    },

    #[snafu(display(
        "Failed to delete rows from the search index '{index}' (elasticsearch): key column '{column}' is mapped with the normalizer '{normalizer}', so an exact-match filter on it also matches every other value that normalizes the same way — deleting one row's documents would reach another row's — and it has no unnormalized exact-match sub-field either; the delete was not issued. Re-create the index and let the runtime map its key columns: it maps a string key `keyword` with no `normalizer`, and every other key type to that type's own exact mapping. Elasticsearch cannot change an existing field's normalizer. See: https://spiceai.org/docs/features/search"
    ))]
    KeyColumnNormalized {
        index: String,
        column: String,
        normalizer: String,
    },

    #[snafu(display(
        "Failed to delete rows from the search index '{index}' (elasticsearch): key column '{column}' holds {source_type} values but is mapped `{mapped_as}`, which {why}, so an exact-match filter on it also reaches every other value indexed under that same term — deleting one row's documents would reach another row's — and it has no exactly-addressable sub-field either; the delete was not issued. Re-create the index and let the runtime map its key columns: it maps a string key `keyword`, and every other key type to that type's own exact mapping (an `Int64` key becomes `long`, not `keyword`). Elasticsearch cannot change an existing field's type. See: https://spiceai.org/docs/features/search"
    ))]
    KeyColumnOverMatches {
        index: String,
        column: String,
        mapped_as: String,
        source_type: String,
        why: &'static str,
    },

    #[snafu(display(
        "Failed to delete rows from the search index '{index}' (elasticsearch): a key in column '{column}' is {length} characters, past the `ignore_above: {ignore_above}` of the '{path}' field it is matched on, so Elasticsearch never indexed it and no filter can address its documents; the delete was not issued. Re-create the index so the runtime maps its key columns as `keyword` with no `ignore_above`. See: https://spiceai.org/docs/features/search"
    ))]
    KeyValueNotIndexed {
        index: String,
        column: String,
        path: String,
        length: usize,
        ignore_above: i64,
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

/// Member budget for one group-remainder `_delete_by_query` ([`group_requests`]). Whole key
/// groups are accumulated until their members reach this many, which bounds both the group count
/// (every group holds at least one member) and the `ids` list inside each group's clause. Spent
/// in members rather than in groups for that reason — [`DELETE_CHUNK_ROWS`] bounds one `should`
/// clause per *row*, which is a different accounting.
///
/// A group is not one clause: Elasticsearch counts the leaves of nested `bool`s towards one
/// budget (it answers `too_many_nested_clauses` when they exceed it), and each group contributes
/// one `term` leaf per key column plus its `ids` leaf. So the widest request this budget admits
/// carries `PRUNE_MEMBERS_PER_REQUEST * (key columns + 1)` leaves — 1024, 1536 and 2048 for a
/// one-, two- and three-column key.
///
/// Those fit: on Elasticsearch 8 the clause budget is derived from the node's heap rather than
/// taken from `indices.query.bool.max_clause_count` (deprecated there, and ignored), measuring
/// 9362 on a 1 GiB-heap node and 2340 on a 256 MiB one — so even the narrowest node Elasticsearch
/// will start leaves the three-column request room to spare.
const PRUNE_MEMBERS_PER_REQUEST: usize = 512;

/// Deletes every document whose `key_columns` match a row of `keys` — an exact-key delete when
/// `key_columns` covers every `primary_key` column, a prefix delete when it's a strict subset
/// (the chunked-index case).
///
/// The two cases address documents differently, because filtering on a key *column* depends on
/// how that column is mapped. A string column with no mapping of its own is dynamically mapped
/// `text`, whose inverted index holds *analyzed* tokens; a `term` query is not analyzed, so
/// `{"term": {"id": "ORDER-1024"}}` looks for one token in an index holding `[order, 1024]` and
/// matches nothing — a delete that reports success having removed no documents (#12267, #13714).
///
/// So when `key_columns` covers the whole primary key, this addresses documents by `_id` via an
/// `ids` query. `_id` is the value the write path already stores for the row, derived by the
/// same [`write::extract_primary_key_from_fields`], so the delete matches exactly the documents
/// the write produced — no field mapping, no analysis, and no dependence on the key's type.
///
/// A strict subset of the key (the chunked-index case) cannot use `_id`, because the chunk id is
/// part of it and is unknown at delete time. That case filters on the key columns, so it first
/// reads the index mapping and resolves each key column to the field path a `term` matches the
/// stored value on — the column itself when it is mapped to an exact-match type, its `keyword`
/// multi-field when the column is `text` (which is what dynamic mapping gives an unmapped string,
/// and what the runtime maps a key column that is also a search field to). A column that resolves
/// to neither is refused by name rather than filtered on: the filter would match nothing, and
/// reporting that as a successful delete is the failure this addressing exists to avoid (#13714).
///
/// Only reads `key_columns` from `keys`, ignoring any other column present — `keys` may be
/// shaped by [`spice_table::Index::required_columns`] (a superset of the primary
/// key) rather than the primary key alone, since that's what the default
/// [`spice_table::Index::resolve_delete_keys`] resolves against.
///
/// Issues one `_delete_by_query` request per [`DELETE_CHUNK_ROWS`]-row slice of `keys` rather
/// than a single request for the whole batch, so a large delete can't build an unbounded
/// clause or id list. Every chunk is issued even when an earlier one comes back only partially
/// applied, or fails outright against an index that was reached; the first such failure is
/// reported once the batch is through. Only a refused connection ends the batch early, since it
/// is the one failure that says the later chunks have nothing to reach either.
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

    // An empty batch addresses nothing, so it must not reach out to the index at all — including
    // for the mapping read below, whose failure would report a delete of no rows as failed.
    if keys.num_rows() == 0 {
        return Ok(());
    }

    // Resolve once for the whole batch, before the first request: every chunk filters on the same
    // columns, and a key column with no exact-match field path fails the delete outright rather
    // than per chunk.
    let key_paths = if addresses_whole_key {
        Vec::new()
    } else {
        match resolve_term_exact_paths(client, es_index, key_columns, keys.schema_ref()).await {
            Ok(Some(paths)) => paths,
            // No document carries these columns, so there is nothing this delete can address.
            Ok(None) => return Ok(()),
            Err(e) => return Err(DataFusionError::External(Box::new(e))),
        }
    };

    // Before the first request, not per chunk: a key the resolved field never indexed fails the
    // whole delete, and finding that out three chunks in would leave the batch half applied.
    ensure_keys_are_indexable(es_index, &key_paths, keys)
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    // Hold the first failure and keep going. Each chunk is an independent `_delete_by_query` over
    // its own slice of `keys`, so returning at the first one would leave every later chunk
    // unissued, turning a report of a partial delete into a cause of a larger one.
    let mut failure: Option<DataFusionError> = None;

    let mut offset = 0;
    while offset < keys.num_rows() {
        let len = DELETE_CHUNK_ROWS.min(keys.num_rows() - offset);
        let chunk = keys.slice(offset, len);
        offset += len;

        let query = if addresses_whole_key {
            build_ids_query(primary_key, es_index, &chunk)?
        } else {
            build_or_of_row_term_queries(&key_paths, &chunk)?
        };
        let Some(query) = query else {
            continue;
        };

        let outcome = issue_delete_chunk(client, es_index, &query).await;
        if let Some(e) = outcome.failure {
            failure.get_or_insert(e);
        }
        if outcome.never_reached {
            break;
        }
    }

    if let Some(e) = failure {
        return Err(e);
    }

    Ok(())
}

/// Delete every document that agrees with a row of `members` on `group_columns` but whose own
/// `_id` is not one of that group's members — the rest of each named group, with the listed
/// members kept. See [`spice_table::Index::delete_group_remainder`] for what a caller expects
/// of it.
///
/// This is what removes the chunks a shortened row no longer produces (#13717). A row rewritten
/// to fewer chunks is upserted under chunk ids `0..n`, so every higher chunk id its previous
/// text produced stays in the index and keeps answering searches for content the row no longer
/// has. The write path knows the chunks it produced, not how many the previous text had, so it
/// names the survivors and Elasticsearch removes the rest.
///
/// One `should` clause per group, each carrying its own group's filter *and* its own group's
/// surviving `_id`s in `must_not`. The scoping is per group rather than per request so that a
/// group split across two requests cannot have one request delete the members the other
/// request's clause was protecting: every clause carries the whole of the group it names, or
/// the group is not named at all.
///
/// A group is skipped outright — nothing deleted, nothing reported — when it cannot be
/// addressed on both halves at once: a group-column value no `term` can express (NULL, or a
/// type that has no JSON term form), or a member whose `_id` the write path would not have
/// derived either. Deleting a group whose survivors cannot all be named would remove documents
/// this write just wrote, which is worse than leaving a superseded chunk behind.
///
/// Like every other delete on this write path, this is a `_delete_by_query` — a *search* — and a
/// document `_bulk` wrote is not searchable until the index refreshes. A prune issued inside the
/// same refresh window as the write that superseded its targets can therefore match nothing and
/// report a clean delete. That is shared with the eviction deletes either side of it rather than
/// introduced here, and closing it trades against `bulk_load_refresh_interval`'s whole purpose:
/// #14318 owns the decision.
///
/// Addressing, chunking, and error reporting are [`delete_by_keys`]': the group columns are
/// resolved to the field path a `term` matches their stored value on (a `text`-mapped column is
/// matched on its `keyword` multi-field), every request is issued even after an earlier one
/// comes back partially applied, and only a refused connection ends the batch early.
pub async fn delete_group_remainder(
    client: &dyn Elasticsearch,
    es_index: &str,
    primary_key: &[Field],
    group_columns: &[String],
    members: &RecordBatch,
) -> DataFusionResult<()> {
    if members.num_rows() == 0 {
        return Ok(());
    }

    let key_paths =
        match resolve_term_exact_paths(client, es_index, group_columns, members.schema_ref()).await
        {
            Ok(Some(paths)) => paths,
            // No document carries these columns, so there is no group here to prune.
            Ok(None) => return Ok(()),
            Err(e) => return Err(DataFusionError::External(Box::new(e))),
        };
    ensure_keys_are_indexable(es_index, &key_paths, members)
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    let groups = collect_groups(primary_key, es_index, &key_paths, members)?;

    let mut failure: Option<DataFusionError> = None;
    for request in group_requests(groups) {
        let outcome = issue_delete_chunk(client, es_index, &request).await;
        if let Some(e) = outcome.failure {
            failure.get_or_insert(e);
        }
        if outcome.never_reached {
            break;
        }
    }

    if let Some(e) = failure {
        return Err(e);
    }

    Ok(())
}

/// One key group of a [`delete_group_remainder`] call: the `term` clauses that address its
/// documents, and the `_id`s of the members to keep.
struct MemberGroup {
    terms: Vec<Value>,
    survivors: Vec<String>,
}

/// Gather `members` into one [`MemberGroup`] per distinct value of the group columns, in
/// first-seen order.
///
/// Rows are grouped by the term values their key columns render to, so nothing here depends on a
/// group's rows being adjacent in `members`, and two rows group together exactly when the same
/// `term` filter would reach both — which is the property the emitted query relies on. A row that
/// cannot be expressed as a `term` filter, or whose `_id` the write path would not have derived,
/// drops the *whole* group it belongs to, whichever end of the batch it sits at: those two halves
/// address the same documents from opposite sides, so a group that loses either one can no longer
/// name what to keep. A dropped group is held as `None` in place rather than removed, so a later
/// row for the same key cannot resurrect it.
fn collect_groups(
    primary_key: &[Field],
    es_index: &str,
    key_paths: &[KeyFieldPath],
    members: &RecordBatch,
) -> DataFusionResult<Vec<MemberGroup>> {
    let ids = write::extract_primary_key_from_fields(primary_key, es_index, members)
        .map_err(|e| DataFusionError::External(Box::new(e)))?;

    let arrays = key_column_arrays(key_paths, members, "group-remainder member batch")?;

    // Groups in first-seen order, `None` for one that turned out to be unaddressable; the map
    // only locates a key's slot.
    let mut groups: Vec<Option<MemberGroup>> = Vec::new();
    let mut slots: HashMap<Vec<String>, usize> = HashMap::new();

    for (row, id) in ids.into_iter().enumerate() {
        let Some(values) = row_term_values(key_paths, &arrays, row)? else {
            // No group identity, so this row names no group to protect or prune.
            continue;
        };

        let slot = match slots.entry(values.iter().map(ToString::to_string).collect()) {
            Entry::Occupied(e) => *e.get(),
            Entry::Vacant(e) => {
                groups.push(Some(MemberGroup {
                    terms: term_clauses(key_paths, &values),
                    survivors: Vec::new(),
                }));
                *e.insert(groups.len() - 1)
            }
        };

        match id {
            Some(id) => {
                if let Some(group) = groups[slot].as_mut() {
                    group.survivors.push(id);
                }
            }
            // The write path stores no document for a row whose `_id` it cannot derive, so this
            // group's membership is not fully known — leave it alone entirely.
            None => groups[slot] = None,
        }
    }

    Ok(groups
        .into_iter()
        .flatten()
        // Belt and braces: an empty `must_not` would make the group's filter delete the whole
        // group, members included. Every surviving group holds at least one member by
        // construction, since each row either contributes one or drops the group.
        .filter(|group| !group.survivors.is_empty())
        .collect())
}

/// Split `groups` into `_delete_by_query` bodies, never splitting a group across two of them.
///
/// Each request holds whole groups up to [`PRUNE_MEMBERS_PER_REQUEST`] members; a single group
/// larger than that budget is issued on its own rather than split.
fn group_requests(groups: Vec<MemberGroup>) -> Vec<Value> {
    let mut requests = Vec::new();
    let mut clauses: Vec<Value> = Vec::new();
    let mut members = 0usize;

    for group in groups {
        if !clauses.is_empty() && members + group.survivors.len() > PRUNE_MEMBERS_PER_REQUEST {
            requests.push(should_match_one(std::mem::take(&mut clauses)));
            members = 0;
        }
        members += group.survivors.len();
        clauses.push(json!({
            "bool": {
                "filter": group.terms,
                "must_not": [ids_query(&group.survivors)]
            }
        }));
    }

    if !clauses.is_empty() {
        requests.push(should_match_one(clauses));
    }

    requests
}

/// `{"bool": {"should": [...], "minimum_should_match": 1}}` — the clauses ORed.
///
/// Assembled rather than written as a `json!` literal so `clauses` is moved into the body; the
/// macro would take it by reference and rebuild every clause it already holds.
fn should_match_one(clauses: Vec<Value>) -> Value {
    let mut bool_query = serde_json::Map::new();
    bool_query.insert("should".to_string(), Value::Array(clauses));
    bool_query.insert("minimum_should_match".to_string(), Value::from(1));

    let mut query = serde_json::Map::new();
    query.insert("bool".to_string(), Value::Object(bool_query));
    Value::Object(query)
}

/// What issuing one `_delete_by_query` chunk produced: the failure to report, if any, and
/// whether the rest of the batch has anything to reach.
struct ChunkOutcome {
    failure: Option<DataFusionError>,
    never_reached: bool,
}

/// Issue one `_delete_by_query` and classify what came back.
///
/// Only a refused connection proves this chunk never reached the index, and so that no later
/// chunk will either — stopping there costs nothing and spares a dead node one request per
/// remaining chunk. Every other error leaves the delete's fate unknown: `JsonParse` is raised
/// *after* a 2xx, so Elasticsearch ran that delete and only the body was unreadable, and a
/// status error or a timeout can each land on a request the index already applied in part.
/// Treating those as "never reached" is what would leave the later chunks unissued — a larger
/// divergence than the one being reported.
async fn issue_delete_chunk(
    client: &dyn Elasticsearch,
    es_index: &str,
    query: &Value,
) -> ChunkOutcome {
    match client.delete_by_query(es_index, query).await {
        Ok(resp) => ChunkOutcome {
            // Report the first, which is the one whose surrounding state a reconcile starts
            // from; later chunks fail the same way once the index has diverged.
            failure: inspect_delete_response(&resp, es_index)
                .err()
                .map(|e| DataFusionError::External(Box::new(e))),
            never_reached: false,
        },
        Err(e) => {
            let never_reached = matches!(
                &e,
                elasticsearch::Error::HttpRequest { source } if source.is_connect()
            );
            ChunkOutcome {
                failure: Some(DataFusionError::External(Box::new(e))),
                never_reached,
            }
        }
    }
}

/// Delete the documents stored under `ids`, addressing them by `_id`.
///
/// The write path derives each document's `_id` from its row's primary key and holds it while
/// building the batch, so a write that could not index a row already has the identity of the
/// document it has to remove — no re-derivation, and no dependence on the key's field mapping
/// (see [`delete_by_keys`] for why filtering on a key column is not reliable).
///
/// Chunked and error-reported exactly as [`delete_by_keys`] is, via [`issue_delete_chunk`].
pub async fn delete_by_ids(
    client: &dyn Elasticsearch,
    es_index: &str,
    ids: &[String],
) -> DataFusionResult<()> {
    let mut failure: Option<DataFusionError> = None;

    for chunk in ids.chunks(DELETE_CHUNK_ROWS) {
        if chunk.is_empty() {
            continue;
        }
        let query = ids_query(chunk);

        let outcome = issue_delete_chunk(client, es_index, &query).await;
        if let Some(e) = outcome.failure {
            failure.get_or_insert(e);
        }
        if outcome.never_reached {
            break;
        }
    }

    if let Some(e) = failure {
        return Err(e);
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

    let values: Vec<String> = ids.into_iter().flatten().collect();
    if values.is_empty() {
        return Ok(None);
    }

    Ok(Some(ids_query(&values)))
}

/// Builds `{"bool": {"should": [{"bool": {"filter": [{"term": {...}}, ...]}}, ...], "minimum_should_match": 1}}`
/// — one `should` clause (the key columns ANDed) per row of `keys`, rows ORed together.
///
/// Each term names the field path [`resolve_term_exact_paths`] resolved for that column, which is
/// not always the column's own name: a `text`-mapped column is matched on its `keyword`
/// multi-field, since the column itself holds analyzed tokens.
fn build_or_of_row_term_queries(
    key_paths: &[KeyFieldPath],
    keys: &RecordBatch,
) -> DataFusionResult<Option<Value>> {
    if keys.num_rows() == 0 || key_paths.is_empty() {
        return Ok(None);
    }

    let arrays = key_column_arrays(key_paths, keys, "delete key batch")?;

    let mut row_clauses = Vec::with_capacity(keys.num_rows());
    for row in 0..keys.num_rows() {
        // A row with a value no `term` can express is skipped entirely rather than filtered on
        // what is left of its key — see `row_term_values`.
        if let Some(values) = row_term_values(key_paths, &arrays, row)? {
            row_clauses.push(json!({ "bool": { "filter": term_clauses(key_paths, &values) } }));
        }
    }

    if row_clauses.is_empty() {
        return Ok(None);
    }

    Ok(Some(should_match_one(row_clauses)))
}

/// The arrays `key_paths` names in `batch`, in the same order. `what` names the batch in the
/// error, which is the caller's own vocabulary for it.
fn key_column_arrays(
    key_paths: &[KeyFieldPath],
    batch: &RecordBatch,
    what: &str,
) -> DataFusionResult<Vec<ArrayRef>> {
    key_paths
        .iter()
        .map(|p| batch.column_by_name(&p.column).cloned())
        .collect::<Option<Vec<_>>>()
        .ok_or_else(|| {
            let columns: Vec<&str> = key_paths.iter().map(|p| p.column.as_str()).collect();
            DataFusionError::Plan(format!(
                "{what} is missing one of the requested key columns: {columns:?}"
            ))
        })
}

/// The values one row of `arrays` would be matched on, or `None` when any of its key columns
/// holds a value no `term` can express.
///
/// `None` is the whole row, not the one column: a filter built from the rest would name fewer
/// columns than the key has and so match documents the row does not identify — reporting that as
/// a successful delete is the failure this addressing exists to avoid (#13714). Both
/// [`build_or_of_row_term_queries`] and [`collect_groups`] read the rule from here so the two
/// cannot drift on it.
fn row_term_values(
    key_paths: &[KeyFieldPath],
    arrays: &[ArrayRef],
    row: usize,
) -> DataFusionResult<Option<Vec<Value>>> {
    let mut values = Vec::with_capacity(key_paths.len());
    for array in arrays {
        let value = ScalarValue::try_from_array(array.as_ref(), row)?;
        let Some(json_value) = scalar_to_term_value(&value) else {
            return Ok(None);
        };
        values.push(json_value);
    }
    Ok(Some(values))
}

/// One `{"term": {path: value}}` per key column, naming the field path each column's stored value
/// is matched on.
fn term_clauses(key_paths: &[KeyFieldPath], values: &[Value]) -> Vec<Value> {
    key_paths
        .iter()
        .zip(values)
        .map(|(key_path, value)| json!({ "term": { key_path.path.as_str(): value } }))
        .collect()
}

/// `{"ids": {"values": [...]}}` — documents addressed by the `_id` the write path stored.
fn ids_query(values: &[String]) -> Value {
    json!({ "ids": { "values": values } })
}

/// The Elasticsearch field a key column's values are matched on, and the length past which that
/// field stops indexing them.
#[derive(Debug, Clone, PartialEq, Eq)]
struct KeyFieldPath {
    /// The key column, as it is named in the delete-key batch.
    column: String,
    /// The field path a `term` query has to name to match that column's stored value — `column`
    /// itself, or one of its multi-fields (`column.keyword`).
    path: String,
    /// `path`'s `ignore_above`, when it declares one: a longer string is stored but not indexed,
    /// so no filter reaches it.
    ignore_above: Option<i64>,
}

/// Elasticsearch field types whose indexed form is the value itself, so an unanalyzed `term`
/// query matches what the write path stored.
///
/// `text` (and its variants) is deliberately absent: it holds the value's *analyzed* tokens, so a
/// `term` for `ORDER-1024` searches an index holding `[order, 1024]` and matches nothing.
///
/// Being one of these types is necessary and not sufficient — see [`is_term_exact`]. In
/// particular several of them index a *rounded* form of the value, so a `term` is unanalyzed and
/// still reaches more than the key names; [`term_over_matches`] is what rejects those, and it
/// stays a separate question because the answer depends on the column's own type.
const TERM_EXACT_FIELD_TYPES: &[&str] = &[
    "boolean",
    "byte",
    "constant_keyword",
    "date",
    "date_nanos",
    "double",
    "float",
    "half_float",
    "integer",
    "ip",
    "keyword",
    "long",
    "scaled_float",
    "short",
    "unsigned_long",
    "version",
    "wildcard",
];

/// Stands in for a key column the delete batch does not carry, until `key_column_arrays` fails
/// on it: the renderable type that the most mappings are refused for, so an absent column can
/// never resolve to a mapping a present one would have been refused for.
///
/// `Utf8` holds that position, and `assumed_key_type_is_refused_for_every_mapping_any_type_is`
/// keeps it there: a string is refused for every rounding type *and* for every exact type that
/// reaches it through a parse, which is a superset of what any other renderable type is refused
/// for.
const ASSUMED_KEY_TYPE: DataType = DataType::Utf8;

/// Field types whose indexed form is a *rounded* form of the value, whatever it holds, so one
/// term stands for a range of values and a `term` on one reaches documents the key never named.
///
/// These are exact-match types in the sense that a `term` is not analyzed, which is why they read
/// as safe; they are not exact in the sense this addressing needs, which is that the value the
/// write path stored is recoverable from the term. Measured against Elasticsearch 8.15.0, a
/// `term` for one row's value returns a second row's documents as well:
///
/// | type | two distinct values | `term` for the first matches |
/// |---|---|---|
/// | `scaled_float` (factor 100) | `1.234`, `1.2337` | both — `round(v * 100)` is `123` for each |
/// | `float` | `1.0000000000000002`, `1.0` | both — a 24-bit mantissa holds neither apart |
/// | `half_float` | `1.0001`, `1.0002` | both — an 11-bit mantissa is coarser still |
/// | `date` | `…:00.123456Z`, `…:00.123789Z` | both — `date` quantizes to milliseconds |
/// | `constant_keyword` | any two | *every* document — the index stores one value for the field |
const TERM_ROUNDING_FIELD_TYPES: &[(&str, &str)] = &[
    (
        "constant_keyword",
        "indexes one value for the whole index rather than the row's",
    ),
    ("date", "indexes the value rounded to the millisecond"),
    ("float", "indexes the value rounded to a 24-bit mantissa"),
    (
        "half_float",
        "indexes the value rounded to an 11-bit mantissa",
    ),
    (
        "scaled_float",
        "indexes `round(value * scaling_factor)` rather than the value",
    ),
];

/// Why a `term` on this field would reach more than the key names, or `None` when it would not.
///
/// Three ways one term can stand for several keys, and the caller needs to tell them apart only
/// to say so in the message:
///
/// - the type rounds every value it holds ([`TERM_ROUNDING_FIELD_TYPES`]);
/// - the type is exact but too narrow for this column. `double` is the only such pairing that
///   arises: Elasticsearch indexes it as an IEEE-754 binary64, whose integers are exact only up
///   to 2^53, so an `Int64`/`UInt64` column mapped `double` collapses `9007199254740992` and
///   `9007199254740993` onto one term — measured against Elasticsearch 8.15.0, a `term` for the
///   first returns both rows' documents, while the same two keys under `long` return one each.
///   `long` holds every Arrow integer, and `byte`/`short`/`integer` reject an out-of-range value
///   at index time rather than rounding it, so the write fails where this would have had to.
/// - the type is exact in its own values but reached through a parse, because the column's own
///   values are strings and the field is not. Elasticsearch parses the stored string and the
///   query's string with the same lenient parser, and that parse is not injective: measured
///   against Elasticsearch 8.15.0, `"1"` and `"01"` under `long` both index as the term `1` and
///   a `term` for either returns both rows' documents, as do `"1.0"`/`"1.00"` under `double` and
///   `"2020-01-01"`/`"2020-01-01T00:00:00Z"` under `date`, `"false"`/`""` under `boolean` and
///   `"2001:db8::1"`/`"2001:0db8:0:0:0:0:0:1"` under `ip`. [`STRING_FIELD_TYPES`] is the measured
///   set a string *does* reach as itself, and carries the rest of the measurements.
///
/// Only asked of a column whose values [`scalar_to_term_value`] can render, because a column it
/// cannot render issues no `term` at all: its group is dropped by [`collect_groups`] instead, and
/// [`key_renders_terms`] is what stops that drop being reported as a complete prune. That is what
/// keeps this rule pointed at over-matching rather than at mappings the runtime itself writes —
/// `primary_key_mapping` maps a `Float32` key column `float` and a timestamp key column `date`,
/// and neither renders a term; it maps a string key column bare `keyword`, so the parse rule
/// above only ever fires on an index the runtime did not create.
fn term_over_matches(mapping: &FieldMapping, source_type: &DataType) -> Option<TermOverMatch> {
    let field_type = mapping.field_type.as_deref()?;
    if !renders_a_term(source_type) {
        return None;
    }
    if let Some((mapped_as, why)) = TERM_ROUNDING_FIELD_TYPES
        .iter()
        .find(|(t, _)| *t == field_type)
    {
        return Some(TermOverMatch { mapped_as, why });
    }
    if field_type == "double" && matches!(source_type, DataType::Int64 | DataType::UInt64) {
        return Some(TermOverMatch {
            mapped_as: "double",
            why: "indexes integers past 2^53 rounded, being an IEEE-754 binary64",
        });
    }
    if !is_string_type(source_type) || STRING_FIELD_TYPES.contains(&field_type) {
        return None;
    }
    // Only an *exact* type can over-match, and looking the name up here is what enforces that: a
    // type that is not exact at all is refused by `is_term_exact` with
    // [`Error::KeyColumnNotExactlyMatchable`], whose message is the right one for it — saying
    // `text` "parses this column's strings" would describe analysis as coercion. The lookup also
    // supplies the `&'static str` the message needs, so no mapping-supplied name is carried.
    TERM_EXACT_FIELD_TYPES
        .iter()
        .find(|t| **t == field_type)
        .map(|mapped_as| TermOverMatch {
            mapped_as,
            why: "parses this column's strings to index them, and that parse maps several strings onto one value",
        })
}

/// The exact field types that a string column reaches without a lossy parse, so a `term` on one
/// matches the stored string and no other. Everything [`TERM_EXACT_FIELD_TYPES`] holds that is
/// *not* here is what [`term_over_matches`] refuses for a string column.
///
/// Measured against Elasticsearch 8.15.0 rather than reasoned from the type's name, and the pair
/// has to come from the type's *own* parser — a pair it rejects proves nothing, because rejecting
/// is not collapsing. Collapsing (so refused): `"1"`/`"01"` index as the same term under `byte`,
/// `short`, `integer`, `long` and `unsigned_long`; `"1.0"`/`"1.00"` under `double`;
/// `"2020-01-01"`/`"2020-01-01T00:00:00Z"` under `date` and `date_nanos`; `"false"`/`""` under
/// `boolean`, which reads an empty string as false; and `"2001:db8::1"`/`"2001:0db8:0:0:0:0:0:1"`
/// under `ip`, which canonicalizes the address. Each of those types *also* rejects some pairs
/// (`"True"` under `boolean`, `"1.2.3.04"` under `ip`) — which is why a single rejected pair is
/// not evidence the type is safe.
///
/// Distinct (so kept): `"1"`/`"01"`, `"A"`/`"a"` and `"ORDER-1"`/`"order-1"` under `keyword`,
/// `wildcard` and `version`; `version` stores the original string in its doc values, so
/// `"1.0.0"`/`"1.00.0"`, `"1.0.0"`/`"01.0.0"`, `"1.0"`/`"1.0.0"` and `"1.0.0"`/`"1.0.0+build"`
/// each index as themselves. `constant_keyword` is absent because [`TERM_ROUNDING_FIELD_TYPES`]
/// already refuses it for every source type.
const STRING_FIELD_TYPES: &[&str] = &["keyword", "version", "wildcard"];

/// Whether `source_type` holds strings, so a non-string mapping reaches it only through a parse.
fn is_string_type(source_type: &DataType) -> bool {
    matches!(
        source_type,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
    )
}

/// A mapping that would make a `term` reach more than the key it names: the type, and the clause
/// [`Error::KeyColumnOverMatches`] states it with.
struct TermOverMatch {
    mapped_as: &'static str,
    why: &'static str,
}

/// Whether every column of `primary_key` has a `term` form, so a group keyed on it can be
/// addressed at all.
///
/// A key column whose values [`scalar_to_term_value`] cannot render drops its whole group in
/// [`collect_groups`], which leaves that group's superseded documents in place. That is a
/// defensible thing to do — a partial filter would reach documents the row does not identify —
/// but it is not a complete prune, so an index over such a key reports
/// `GroupPruning::Unsupported` rather than `GroupPruning::Complete` and the caller warns.
/// The runtime maps a `Float32` key column `float` and a timestamp key column `date`
/// (`primary_key_mapping`), and neither renders a term, so these are key types a user can
/// actually declare rather than a theoretical gap.
pub(crate) fn key_renders_terms(primary_key: &[Field]) -> bool {
    primary_key.iter().all(|f| renders_a_term(f.data_type()))
}

/// Whether a value of `source_type` has a `term` form at all, mirroring [`scalar_to_term_value`].
///
/// `term_renderable_types_match_scalar_to_term_value` pins the two together, so a type added to
/// one is caught if it is not added to the other.
fn renders_a_term(source_type: &DataType) -> bool {
    matches!(
        source_type,
        DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Utf8View
            | DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
    )
}

/// Whether a `term` on this field matches the value the write path stored, and no other row's:
/// an exact type, searchable at all, unnormalized, and not standing for a range of values. A
/// field mapped `index: false` — which is what a column the user declared non-filterable gets —
/// is stored in `_source` and indexed nowhere, so a filter naming it matches nothing however
/// exact its type is.
fn is_term_exact(mapping: &FieldMapping, source_type: &DataType) -> bool {
    mapping.is_indexed()
        // A normalizer is applied to the query's value as well as the stored one, so `term` on a
        // normalized field matches every value that normalizes the same way. That is exact
        // enough to find a row's documents and not exact enough to stop at them.
        && mapping.normalizer.is_none()
        && mapping
            .field_type
            .as_deref()
            .is_some_and(|t| TERM_EXACT_FIELD_TYPES.contains(&t))
        && term_over_matches(mapping, source_type).is_none()
}

/// Resolves each of `key_columns` to the field path an exact-match `term` query has to name, by
/// reading `es_index`'s live mapping.
///
/// The runtime maps a key column as `keyword` when it creates the index, but the index may
/// predate that, may have been created by the user, or may map the column as a search field — so
/// what a `term` has to name is a property of the index in front of us, not of our own intent.
/// Three outcomes, and the third is the point:
///
/// - the column is mapped to an exact type — name the column;
/// - the column is `text` with an exact multi-field (`keyword`, which dynamic mapping derives for
///   every unmapped string) — name that sub-field;
/// - neither — return [`Error::KeyColumnNotExactlyMatchable`]. No filter can address the
///   documents, and issuing one anyway is what let a delete remove nothing and report success.
///
/// Returns `Ok(None)` when a key column is absent from the mapping entirely: Elasticsearch maps a
/// field the first time a document carries it, so no document holds that column and the delete
/// has nothing to address. That is a delete of rows the index does not have, not a failure. The
/// exception is an index the user pre-created with `dynamic: false` and then populated elsewhere,
/// where a document can carry an unmapped field — but the runtime maps its own key columns when
/// it prepares the index, so a column it writes is mapped by the time any delete runs.
async fn resolve_term_exact_paths(
    client: &dyn Elasticsearch,
    es_index: &str,
    key_columns: &[String],
    source_schema: &Schema,
) -> Result<Option<Vec<KeyFieldPath>>> {
    if key_columns.is_empty() {
        return Ok(None);
    }

    let response =
        client
            .get_mapping(es_index)
            .await
            .map_err(|source| Error::KeyColumnMappingUnreadable {
                index: es_index.to_string(),
                columns: key_columns
                    .iter()
                    .map(|c| format!("'{c}'"))
                    .collect::<Vec<_>>()
                    .join(", "),
                source,
            })?;

    let Some(properties) = elasticsearch::index_properties(&response, es_index) else {
        // An index with no mapping of its own holds no documents to delete.
        return Ok(None);
    };

    let mut paths = Vec::with_capacity(key_columns.len());
    for column in key_columns {
        let Some(mapping) = properties.get(column) else {
            return Ok(None);
        };

        // A column the batch does not carry fails later in `key_column_arrays`; until then
        // `ASSUMED_KEY_TYPE` stands in, so an unknown column cannot resolve to a mapping a known
        // one would have been refused for.
        let source_type = source_schema
            .column_with_name(column)
            .map_or(&ASSUMED_KEY_TYPE, |(_, f)| f.data_type());

        if is_term_exact(mapping, source_type) {
            paths.push(KeyFieldPath {
                column: column.clone(),
                path: column.clone(),
                ignore_above: mapping.ignore_above,
            });
            continue;
        }

        // Multi-fields index the same value a second way. `keyword` is the one dynamic mapping
        // derives and the one to prefer; any other exact sub-field is still exact, and taking the
        // lowest name keeps the choice stable across runs rather than following hash order.
        let sub_fields = mapping.fields.as_ref();
        let exact_sub = sub_fields
            .and_then(|fields| {
                fields
                    .get("keyword")
                    .filter(|m| is_term_exact(m, source_type))
                    .map(|m| ("keyword", m))
            })
            .or_else(|| {
                sub_fields
                    .into_iter()
                    .flatten()
                    .filter(|(_, m)| is_term_exact(m, source_type))
                    .min_by(|(a, _), (b, _)| a.cmp(b))
                    .map(|(n, m)| (n.as_str(), m))
            });

        let Some((sub_name, sub_mapping)) = exact_sub else {
            // A normalized column is refused by name: the generic message would say it cannot be
            // matched at all, when the real hazard is that it matches too much.
            if let Some(normalizer) = mapping.normalizer.as_deref() {
                return KeyColumnNormalizedSnafu {
                    index: es_index.to_string(),
                    column: column.clone(),
                    normalizer: normalizer.to_string(),
                }
                .fail();
            }
            // Likewise for a mapping that reaches past the key it names: the generic message
            // would say the column cannot be matched at all, when the hazard is the opposite.
            if let Some(over) = term_over_matches(mapping, source_type) {
                return KeyColumnOverMatchesSnafu {
                    index: es_index.to_string(),
                    column: column.clone(),
                    mapped_as: over.mapped_as.to_string(),
                    source_type: source_type.to_string(),
                    why: over.why,
                }
                .fail();
            }
            return KeyColumnNotExactlyMatchableSnafu {
                index: es_index.to_string(),
                column: column.clone(),
                mapped_as: mapping
                    .field_type
                    .as_deref()
                    .unwrap_or("object")
                    .to_string(),
            }
            .fail();
        };

        paths.push(KeyFieldPath {
            column: column.clone(),
            path: format!("{column}.{sub_name}"),
            ignore_above: sub_mapping.ignore_above,
        });
    }

    Ok(Some(paths))
}

/// Refuses the delete if any key in `keys` is longer than the `ignore_above` of the field it
/// would be matched on.
///
/// Elasticsearch stores such a value but does not index it, so no filter reaches it — the same
/// silent no-op an analyzed key column produces, one layer down. It cannot be caught while
/// resolving the field, which sees no values, so it is caught here: before the first request,
/// rather than by the chunk that happens to carry the long key, which would leave the chunks
/// before it applied and the ones after it unissued.
///
/// Only the columns whose resolved field declares a limit are scanned, and the runtime's own
/// mapping declares none — so on an index it created this walks nothing.
fn ensure_keys_are_indexable(
    es_index: &str,
    key_paths: &[KeyFieldPath],
    keys: &RecordBatch,
) -> Result<()> {
    for key_path in key_paths {
        let Some(ignore_above) = key_path.ignore_above else {
            continue;
        };
        let Some(array) = keys.column_by_name(&key_path.column) else {
            // A missing column is the query builder's error to report, with the whole key in hand.
            continue;
        };
        for row in 0..array.len() {
            let Ok(value) = ScalarValue::try_from_array(array.as_ref(), row) else {
                continue;
            };
            let Some(json_value) = scalar_to_term_value(&value) else {
                continue;
            };
            let Some(text) = json_value.as_str() else {
                continue;
            };
            let length = text.chars().count();
            if i64::try_from(length).is_ok_and(|length| length > ignore_above) {
                return KeyValueNotIndexedSnafu {
                    index: es_index.to_string(),
                    column: key_path.column.clone(),
                    path: key_path.path.clone(),
                    length,
                    ignore_above,
                }
                .fail();
            }
        }
    }

    Ok(())
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
    use arrow_schema::{DataType, Schema, TimeUnit};
    use elasticsearch::{
        Error as EsError, IndexMapping, MappingResponse, Mappings, Result as EsResult,
        SearchRequest, SearchResponse,
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
    ///
    /// It also models a document store keyed by `_id`: seed it with [`RecordingClient::with_ids`]
    /// and an `ids` query (the whole-key delete path) removes exactly those documents, so a test
    /// can assert the surviving set with [`RecordingClient::present_ids`] instead of the query
    /// body. The store is empty by default, so the many query-shape tests are unaffected.
    #[derive(Debug, Default)]
    struct RecordingClient {
        queries: Mutex<Vec<Value>>,
        ids: Mutex<Vec<String>>,
        /// One response per request, in order; the last one answers every request beyond it, so a
        /// single-element list answers a whole multi-request delete the same way. Empty — the
        /// default — means the stub reports exactly what the modeled store removed as fully
        /// applied, which is what leaves the store-based and query-shape tests indifferent to the
        /// response body.
        responses: Vec<Value>,
        /// Request ordinals (1-based) answered with a client error instead of a body. The error
        /// carries a status, so it stands for the kind Elasticsearch itself raised — the request
        /// reached the index, and the delete's fate there is unknown.
        erroring: Vec<usize>,
        /// The body `get_mapping` answers with. `None` — the default — errors instead, so a test
        /// of the whole-key path fails loudly if that path ever reads the mapping it does not
        /// need.
        mapping: Option<MappingResponse>,
    }

    impl RecordingClient {
        fn with_ids(ids: &[&str]) -> Self {
            Self {
                ids: Mutex::new(ids.iter().map(|s| (*s).to_string()).collect()),
                ..Self::default()
            }
        }

        /// Answers every request with `response` instead of a fully-applied one.
        fn answering(response: Value) -> Self {
            Self::answering_in_turn(vec![response])
        }

        /// Answers the nth request with the nth response, so a test can make one request in a
        /// multi-request delete differ from the rest.
        fn answering_in_turn(responses: Vec<Value>) -> Self {
            Self {
                responses,
                ..Self::default()
            }
        }

        /// Answers `get_mapping` for index `idx` with `fields`, so a partial-key delete can
        /// resolve the field path to filter on.
        fn mapped(fields: Vec<(&str, FieldMapping)>) -> Self {
            Self {
                mapping: Some(mapping_of("idx", fields)),
                ..Self::default()
            }
        }

        /// Answers the `nth` request (1-based) with a client error rather than a body.
        fn erroring_on(mut self, nth: usize) -> Self {
            self.erroring.push(nth);
            self
        }

        fn queries(&self) -> Vec<Value> {
            self.queries
                .lock()
                .expect("queries mutex should not be poisoned")
                .clone()
        }

        fn present_ids(&self) -> Vec<String> {
            let mut ids = self
                .ids
                .lock()
                .expect("ids mutex should not be poisoned")
                .clone();
            ids.sort();
            ids
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
            if self.erroring.contains(&issued) {
                return Err(EsError::ElasticsearchError {
                    status: 502,
                    message: format!("request {issued} failed at the index"),
                });
            }

            // Apply an `ids` query to the modeled store so a test can assert the surviving set.
            let mut deleted = 0;
            if let Some(values) = query["ids"]["values"].as_array() {
                let doomed: std::collections::HashSet<&str> =
                    values.iter().filter_map(Value::as_str).collect();
                let mut ids = self.ids.lock().expect("ids mutex should not be poisoned");
                let before = ids.len();
                ids.retain(|id| !doomed.contains(id.as_str()));
                deleted = before - ids.len();
            }

            // A configured response wins, so a test can make the body disagree with what the
            // store removed — which is the whole point of the response-inspection tests. With
            // none configured, report that removal as fully applied so `inspect_delete_response`
            // reads it as clean.
            Ok(
                match self
                    .responses
                    .get(issued - 1)
                    .or_else(|| self.responses.last())
                {
                    Some(response) => response.clone(),
                    None => clean_delete_response(deleted as u64),
                },
            )
        }

        async fn get_mapping(&self, _index: &str) -> EsResult<MappingResponse> {
            match &self.mapping {
                Some(mapping) => Ok(mapping.clone()),
                None => Err(unexpected("get_mapping")),
            }
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

    /// A `GET /<index>/_mapping` body naming `fields` under `index`.
    fn mapping_of(index: &str, fields: Vec<(&str, FieldMapping)>) -> MappingResponse {
        MappingResponse::from([(
            index.to_string(),
            IndexMapping {
                mappings: Mappings {
                    properties: fields
                        .into_iter()
                        .map(|(name, mapping)| (name.to_string(), mapping))
                        .collect(),
                },
            },
        )])
    }

    fn field_mapping(field_type: &str) -> FieldMapping {
        FieldMapping {
            field_type: Some(field_type.to_string()),
            properties: None,
            fields: None,
            ignore_above: None,
            index: None,
            normalizer: None,
            dims: None,
            similarity: None,
        }
    }

    /// What dynamic mapping gives an unmapped string column: `text` holding analyzed tokens, plus
    /// a `keyword` multi-field that indexes the value itself up to `ignore_above`.
    fn dynamic_string_mapping() -> FieldMapping {
        let mut keyword = field_mapping("keyword");
        keyword.ignore_above = Some(256);
        FieldMapping {
            fields: Some(std::collections::HashMap::from([(
                "keyword".to_string(),
                keyword,
            )])),
            ..field_mapping("text")
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
        let client = RecordingClient::mapped(vec![("id", field_mapping("keyword"))]);

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

    /// Member rows as [`spice_table::Index::delete_group_remainder`] receives them from a
    /// chunked index: the base key plus the chunk id of every chunk the row still produces.
    fn member_batch(rows: &[(Option<&str>, u64)]) -> RecordBatch {
        member_batch_keyed(
            Arc::new(StringArray::from(
                rows.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
            )),
            &rows.iter().map(|(_, chunk)| *chunk).collect::<Vec<_>>(),
        )
    }

    /// The same batch for a key column of any type — the shape is one place so the chunk key's
    /// spelling cannot drift between the tests that build it.
    fn member_batch_keyed(ids: ArrayRef, chunks: &[u64]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", ids.data_type().clone(), true),
            Field::new(CHUNKED_INDEX_CHUNK_KEY, DataType::UInt64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![ids, Arc::new(UInt64Array::from(chunks.to_vec()))],
        )
        .expect("member batch should build")
    }

    /// The primary key a chunked index actually has, taken from the augmentation the chunking
    /// layer applies rather than re-spelled here, so these tests cannot drift from it.
    fn chunked_key() -> Vec<Field> {
        chunked_key_of(DataType::Utf8)
    }

    /// The same, for a key column of another type.
    fn chunked_key_of(data_type: DataType) -> Vec<Field> {
        ChunkedSearchIndex::augment_primary_key(vec![pk("id", data_type)])
    }

    /// The `_id`s a `must_not` clause protects, for every `should` clause of `query`.
    fn protected_ids(query: &Value) -> Vec<Vec<String>> {
        query["bool"]["should"]
            .as_array()
            .expect("the request ORs one clause per group")
            .iter()
            .map(|clause| {
                clause["bool"]["must_not"][0]["ids"]["values"]
                    .as_array()
                    .expect("each clause protects its group's members by _id")
                    .iter()
                    .map(|v| v.as_str().expect("an _id is a string").to_string())
                    .collect()
            })
            .collect()
    }

    /// Each group is pruned against its own surviving members: the filter names the group and the
    /// `must_not` beside it names only that group's `_id`s, so one group's members cannot be
    /// protected by another group's clause (or, worse, deleted by it).
    #[tokio::test]
    async fn each_group_is_pruned_against_only_its_own_members() {
        let client = RecordingClient::mapped(vec![("id", field_mapping("keyword"))]);

        delete_group_remainder(
            &client,
            "idx",
            &chunked_key(),
            &["id".to_string()],
            &member_batch(&[(Some("a"), 0), (Some("b"), 0), (Some("b"), 1)]),
        )
        .await
        .expect("the prune should succeed");

        assert_eq!(
            client.queries(),
            vec![json!({
                "bool": {
                    "should": [
                        {"bool": {
                            "filter": [{"term": {"id": "a"}}],
                            "must_not": [{"ids": {"values": [
                                "{\"_spice.chunk_id\":0,\"id\":\"a\"}"
                            ]}}]
                        }},
                        {"bool": {
                            "filter": [{"term": {"id": "b"}}],
                            "must_not": [{"ids": {"values": [
                                "{\"_spice.chunk_id\":0,\"id\":\"b\"}",
                                "{\"_spice.chunk_id\":1,\"id\":\"b\"}"
                            ]}}]
                        }}
                    ],
                    "minimum_should_match": 1
                }
            })],
        );
    }

    /// A group is never split across two requests. Splitting it would leave one request deleting
    /// the very members the other request's clause was protecting — the write's own chunks —
    /// so the budget is spent in whole groups even when that overshoots it.
    #[tokio::test]
    async fn a_group_is_never_split_across_two_requests() {
        let client = RecordingClient::mapped(vec![("id", field_mapping("keyword"))]);

        // Two groups whose members do not fit one request together, and a third that starts a
        // second request; `big` alone is over the budget, so it is issued on its own.
        let mut rows: Vec<(Option<&str>, u64)> = Vec::new();
        for chunk in 0..(DELETE_CHUNK_ROWS as u64 + 10) {
            rows.push((Some("big"), chunk));
        }
        rows.push((Some("small"), 0));
        rows.push((Some("small"), 1));

        delete_group_remainder(
            &client,
            "idx",
            &chunked_key(),
            &["id".to_string()],
            &member_batch(&rows),
        )
        .await
        .expect("the prune should succeed");

        let queries = client.queries();
        assert_eq!(queries.len(), 2, "the two groups do not fit one request");
        assert_eq!(
            protected_ids(&queries[0])
                .into_iter()
                .map(|ids| ids.len())
                .collect::<Vec<_>>(),
            vec![DELETE_CHUNK_ROWS + 10],
            "an oversized group is issued whole rather than split"
        );
        assert_eq!(
            protected_ids(&queries[1])
                .into_iter()
                .map(|ids| ids.len())
                .collect::<Vec<_>>(),
            vec![2],
        );
    }

    /// A NULL group-column value can be expressed by no `term`, so the group it belongs to is
    /// left alone entirely rather than addressed by a filter that would match every document.
    #[tokio::test]
    async fn a_null_group_key_prunes_nothing() {
        let client = RecordingClient::mapped(vec![("id", field_mapping("keyword"))]);

        delete_group_remainder(
            &client,
            "idx",
            &chunked_key(),
            &["id".to_string()],
            &member_batch(&[(None, 0)]),
        )
        .await
        .expect("the prune should succeed");

        assert!(
            client.queries().is_empty(),
            "a group with no expressible key must not be addressed at all"
        );
    }

    /// An empty member batch names no group, so it must not reach the index — not even for the
    /// mapping read, whose failure would report a prune of nothing as failed.
    #[tokio::test]
    async fn an_empty_member_batch_issues_no_request() {
        let client = RecordingClient::default();

        delete_group_remainder(
            &client,
            "idx",
            &chunked_key(),
            &["id".to_string()],
            &member_batch(&[]),
        )
        .await
        .expect("an empty prune should succeed without reaching the index");

        assert!(client.queries().is_empty());
    }

    /// A member whose `_id` the write path would not have derived leaves that member unnamed in
    /// `must_not` — so the group is left alone entirely rather than pruned against a survivor
    /// list that is missing one of its own documents.
    #[tokio::test]
    async fn a_group_with_an_underivable_member_id_is_left_alone() {
        let client = RecordingClient::mapped(vec![("id", field_mapping("keyword"))]);

        // The chunk id is NULL, so `_id` derivation yields nothing for that member while the
        // group's own key stays perfectly expressible.
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, true),
            Field::new(CHUNKED_INDEX_CHUNK_KEY, DataType::UInt64, true),
        ]));
        let members = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![Some("a"), Some("a"), Some("b")])),
                Arc::new(UInt64Array::from(vec![Some(0), None, Some(0)])),
            ],
        )
        .expect("member batch should build");

        delete_group_remainder(
            &client,
            "idx",
            &chunked_key(),
            &["id".to_string()],
            &members,
        )
        .await
        .expect("the prune should succeed");

        assert_eq!(
            protected_ids(&client.queries()[0]),
            vec![vec!["{\"_spice.chunk_id\":0,\"id\":\"b\"}".to_string()]],
            "only the group whose every member could be named is pruned"
        );
    }

    fn normalized_keyword(normalizer: &str) -> FieldMapping {
        FieldMapping {
            normalizer: Some(normalizer.to_string()),
            ..field_mapping("keyword")
        }
    }

    /// A `keyword` key column with a normalizer looks exactly matchable and is not: Elasticsearch
    /// normalizes a `term` query's value too, so a filter on `A` also matches the documents of a
    /// distinct row keyed `a`.
    ///
    /// On the prune that is worse than over-deleting, which is what it would be on a delete: the
    /// sibling's `_id`s are in no group's `must_not`, so an ordinary write of `A` would remove the
    /// chunks of a row `a` that still exists and was never written. The mapping is refused before
    /// a request is issued instead.
    #[tokio::test]
    async fn a_normalized_group_column_refuses_before_issuing() {
        let client = RecordingClient::mapped(vec![("id", normalized_keyword("lowercase"))]);

        let err = delete_group_remainder(
            &client,
            "idx",
            &chunked_key(),
            &["id".to_string()],
            &member_batch(&[(Some("A"), 0)]),
        )
        .await
        .expect_err("a normalized group column must fail the prune");

        let message = err.to_string();
        assert!(
            message.contains("'id'") && message.contains("lowercase"),
            "the error must name the column and its normalizer, got: {message}"
        );
        assert!(
            client.queries().is_empty(),
            "no _delete_by_query may be issued against a key field that matches sibling values"
        );
    }

    /// The same mapping is refused on the partial-key delete, which shares the resolution. There
    /// the filter over-deletes rather than crossing into a live row, but it is the same field that
    /// cannot address one row's documents and the same refusal.
    #[tokio::test]
    async fn a_normalized_key_column_refuses_a_partial_key_delete_too() {
        let client = RecordingClient::mapped(vec![("id", normalized_keyword("lowercase"))]);

        delete_by_keys(
            &client,
            "idx",
            &chunked_key(),
            &["id".to_string()],
            &string_key_batch(vec![Some("A")]),
        )
        .await
        .expect_err("a normalized key column must fail the delete");

        assert!(client.queries().is_empty());
    }

    /// A normalized column that also carries an unnormalized exact sub-field is still addressable
    /// — on that sub-field. Refusing it would fail a delete the index can serve exactly.
    #[tokio::test]
    async fn a_normalized_column_with_an_exact_sub_field_uses_the_sub_field() {
        let mut normalized = normalized_keyword("lowercase");
        normalized.fields = Some(std::collections::HashMap::from([(
            "exact".to_string(),
            field_mapping("keyword"),
        )]));
        let client = RecordingClient::mapped(vec![("id", normalized)]);

        delete_group_remainder(
            &client,
            "idx",
            &chunked_key(),
            &["id".to_string()],
            &member_batch(&[(Some("A"), 0)]),
        )
        .await
        .expect("the prune should succeed on the exact sub-field");

        assert_eq!(
            client.queries()[0]["bool"]["should"][0]["bool"]["filter"],
            json!([{"term": {"id.exact": "A"}}]),
        );
    }

    /// A key column whose type indexes a *rounded* form of the value is the normalizer hazard
    /// under a different name, and the same refusal answers it.
    ///
    /// Measured against Elasticsearch 8.15.0 rather than reasoned from the mapping: two documents
    /// carrying distinct `scaled_float` values `1.234` and `1.2337` (scaling factor 100) are both
    /// returned by `{"term": {"pk": 1.234}}`, because each indexes as `123`. Issuing the prune's
    /// own body for the first row — `filter` that term, `must_not` its surviving chunk `_id`s —
    /// then deleted three documents and reported no failures: the row's one superseded chunk, and
    /// both chunks of the *other* row, which no `must_not` named because it is not in the group.
    /// `float`, `half_float` and `date` collide the same way, and `constant_keyword` matches every
    /// document in the index.
    #[tokio::test]
    async fn a_rounding_group_column_refuses_before_issuing() {
        for (field_type, _) in TERM_ROUNDING_FIELD_TYPES {
            let client = RecordingClient::mapped(vec![("id", field_mapping(field_type))]);

            let err = delete_group_remainder(
                &client,
                "idx",
                &chunked_key(),
                &["id".to_string()],
                &member_batch(&[(Some("A"), 0)]),
            )
            .await
            .expect_err("a rounding group column must fail the prune");

            let message = err.to_string();
            assert!(
                message.contains("'id'") && message.contains(*field_type),
                "the error must name the column and its mapping, got: {message}"
            );
            assert!(
                client.queries().is_empty(),
                "no _delete_by_query may be issued against a key field mapped `{field_type}`, \
                 whose terms stand for a range of values"
            );
        }
    }

    /// The same mapping is refused on the partial-key delete, which shares the resolution — the
    /// filter over-deletes there rather than crossing into a live row, but it is the same field
    /// that cannot address one row's documents.
    #[tokio::test]
    async fn a_rounding_key_column_refuses_a_partial_key_delete_too() {
        for (field_type, _) in TERM_ROUNDING_FIELD_TYPES {
            let client = RecordingClient::mapped(vec![("id", field_mapping(field_type))]);

            delete_by_keys(
                &client,
                "idx",
                &chunked_key(),
                &["id".to_string()],
                &string_key_batch(vec![Some("A")]),
            )
            .await
            .expect_err("a rounding key column must fail the delete");

            assert!(client.queries().is_empty());
        }
    }

    /// A rounded column that also carries an exactly-indexed sub-field is still addressable — on
    /// that sub-field, which is the shape a `text` column with a `keyword` multi-field has too.
    #[tokio::test]
    async fn a_rounding_column_with_an_exact_sub_field_uses_the_sub_field() {
        let mut rounding = field_mapping("scaled_float");
        rounding.fields = Some(std::collections::HashMap::from([(
            "exact".to_string(),
            field_mapping("keyword"),
        )]));
        let client = RecordingClient::mapped(vec![("id", rounding)]);

        delete_group_remainder(
            &client,
            "idx",
            &chunked_key(),
            &["id".to_string()],
            &member_batch(&[(Some("A"), 0)]),
        )
        .await
        .expect("the prune should succeed on the exact sub-field");

        assert_eq!(
            client.queries()[0]["bool"]["should"][0]["bool"]["filter"],
            json!([{"term": {"id.exact": "A"}}]),
        );
    }

    /// `double` is an exact type and still cannot address an `Int64` key: Elasticsearch indexes
    /// it as an IEEE-754 binary64, exact for integers only up to 2^53.
    ///
    /// Measured against Elasticsearch 8.15.0, not reasoned from the mapping. Two documents keyed
    /// `9007199254740992` and `9007199254740993` under `{"type": "double"}` are both returned by
    /// `{"term": {"k": 9007199254740992}}`; the same two keys under `long` and under
    /// `unsigned_long` return one document each. So the prune would protect one row's chunk
    /// `_id`s and delete the other row's, which is the [`Error::KeyColumnNormalized`] hazard
    /// reached through the mapping's *width* rather than its kind.
    #[tokio::test]
    async fn an_int64_key_column_mapped_double_refuses_before_issuing() {
        let client = RecordingClient::mapped(vec![("id", field_mapping("double"))]);

        let err = delete_group_remainder(
            &client,
            "idx",
            &chunked_key_of(DataType::Int64),
            &["id".to_string()],
            &member_batch_keyed(
                Arc::new(Int64Array::from(vec![9_007_199_254_740_992_i64])),
                &[0],
            ),
        )
        .await
        .expect_err("an Int64 key column mapped `double` must fail the prune");

        let message = err.to_string();
        assert!(
            message.contains("'id'") && message.contains("double") && message.contains("Int64"),
            "the error must name the column, its mapping and the source type, got: {message}"
        );
        assert!(
            client.queries().is_empty(),
            "no _delete_by_query may be issued against a mapping that collapses distinct keys"
        );
    }

    /// [`ASSUMED_KEY_TYPE`] stands in for a column the batch does not carry, and the whole point
    /// of the choice is that it cannot let such a column through a mapping a real one would have
    /// been refused for. Pin that: every mapping refused for any renderable source type must be
    /// refused for the assumed one too.
    #[test]
    fn assumed_key_type_is_refused_for_every_mapping_any_type_is() {
        let renderable = [
            DataType::Utf8,
            DataType::LargeUtf8,
            DataType::Utf8View,
            DataType::Boolean,
            DataType::Int8,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
            DataType::UInt8,
            DataType::UInt16,
            DataType::UInt32,
            DataType::UInt64,
        ];
        for field_type in TERM_EXACT_FIELD_TYPES {
            let mapping = field_mapping(field_type);
            let refused_for_some = renderable
                .iter()
                .any(|t| term_over_matches(&mapping, t).is_some());
            if refused_for_some {
                assert!(
                    term_over_matches(&mapping, &ASSUMED_KEY_TYPE).is_some(),
                    "`{field_type}` is refused for some source type, so it must be refused for \
                     the assumed one — otherwise an absent key column resolves to a mapping a \
                     present one would have been refused for"
                );
            }
        }
    }

    /// A string key column mapped to a non-string type is reached only through Elasticsearch's
    /// parse of the stored string, and that parse is not injective — so a `term` for one key
    /// returns another key's documents and the prune deletes the sibling row's chunks.
    ///
    /// Measured against Elasticsearch 8.15.0, not reasoned from the mapping. Under
    /// `{"type": "long"}`, documents keyed `"1"` and `"01"` both index as the term `1`, and the
    /// prune this code builds for row `"1"` shortened to one chunk —
    /// `{"bool": {"filter": [{"term": {"row_key": "1"}}], "must_not": [{"ids": …}]}}` — reported
    /// `"deleted": 3`, removing row `"01"`'s two chunks along with row `"1"`'s superseded one.
    /// The same two keys under `keyword` leave `"deleted": 1`. `"1.0"`/`"1.00"` under `double`
    /// and `"2020-01-01"`/`"2020-01-01T00:00:00Z"` under `date` collide the same way, as do
    /// `"false"`/`""` under `boolean` (an empty string reads as false) and
    /// `"2001:db8::1"`/`"2001:0db8:0:0:0:0:0:1"` under `ip` (the address is canonicalized).
    ///
    /// `boolean` and `ip` are in this list because of those pairs, not despite rejecting others:
    /// `"True"` and `"1.2.3.04"` are refused at index time, and taking a rejected pair as proof
    /// the type is injective is what left both of them wrongly in [`STRING_FIELD_TYPES`] once.
    #[tokio::test]
    async fn a_string_key_column_mapped_to_a_parsed_type_refuses_before_issuing() {
        for mapped_as in [
            "byte",
            "short",
            "integer",
            "long",
            "unsigned_long",
            "double",
            "date",
            "date_nanos",
            "boolean",
            "ip",
        ] {
            let client = RecordingClient::mapped(vec![("id", field_mapping(mapped_as))]);

            let Err(err) = delete_group_remainder(
                &client,
                "idx",
                &chunked_key_of(DataType::Utf8),
                &["id".to_string()],
                &member_batch_keyed(Arc::new(StringArray::from(vec!["1"])), &[0]),
            )
            .await
            else {
                panic!("a Utf8 key column mapped `{mapped_as}` must fail the prune")
            };

            let message = err.to_string();
            assert!(
                message.contains("'id'") && message.contains(mapped_as) && message.contains("Utf8"),
                "the error must name the column, its mapping and the source type, got: {message}"
            );
            assert!(
                client.queries().is_empty(),
                "no _delete_by_query may be issued against `{mapped_as}`, which collapses distinct string keys"
            );
        }
    }

    /// The parse rule is about the *pairing*, not about string columns: a string key mapped to a
    /// type it reaches as itself is stored and matched as itself, and `keyword` is the mapping
    /// the runtime writes for every string key (`primary_key_mapping`). A blanket refusal of
    /// string keys would cost the common case its prune entirely.
    #[tokio::test]
    async fn a_string_key_column_mapped_to_a_type_it_reaches_as_itself_still_resolves() {
        for mapped_as in STRING_FIELD_TYPES {
            let client = RecordingClient::mapped(vec![("id", field_mapping(mapped_as))]);

            delete_group_remainder(
                &client,
                "idx",
                &chunked_key_of(DataType::Utf8),
                &["id".to_string()],
                &member_batch_keyed(Arc::new(StringArray::from(vec!["1"])), &[0]),
            )
            .await
            .unwrap_or_else(|e| {
                panic!("a Utf8 key column mapped `{mapped_as}` should address its group: {e}")
            });

            assert_eq!(
                client.queries().len(),
                1,
                "`{mapped_as}` addresses the group exactly, so the prune is issued"
            );
        }
    }

    /// A key column no `term` can render drops its whole group in [`collect_groups`], so an
    /// index over such a key prunes nothing — and [`key_renders_terms`] is what stops that being
    /// reported as a complete prune. `primary_key_mapping` maps a `Float32` key `float` and a
    /// timestamp key `date`, so these are keys a user can declare today.
    #[test]
    fn a_key_type_that_renders_no_term_is_not_addressable() {
        for unaddressable in [
            DataType::Float32,
            DataType::Float64,
            DataType::Date32,
            DataType::Date64,
            DataType::Timestamp(TimeUnit::Nanosecond, None),
        ] {
            assert!(
                !key_renders_terms(&chunked_key_of(unaddressable.clone())),
                "{unaddressable} renders no term, so a group keyed on it cannot be addressed"
            );
        }
        for addressable in [DataType::Utf8, DataType::Int64, DataType::UInt32] {
            assert!(
                key_renders_terms(&chunked_key_of(addressable.clone())),
                "{addressable} renders a term, so its groups are addressable"
            );
        }
    }

    /// The width rule is about the pairing, not about `double`: a `UInt32` column mapped
    /// `double` still addresses its group, because binary64 holds every `u32` apart, and the
    /// same `Int64` column mapped `long` does too. A blanket refusal of `double` would fail the
    /// first of these and cost an index a delete it can serve exactly.
    #[tokio::test]
    async fn a_mapping_wide_enough_for_the_source_type_still_resolves() {
        for (source, mapped_as, key, expected) in [
            (
                DataType::Int64,
                "long",
                ScalarValue::Int64(Some(9_007_199_254_740_993)),
                json!(9_007_199_254_740_993_i64),
            ),
            (
                DataType::UInt32,
                "double",
                ScalarValue::UInt32(Some(4_294_967_295)),
                json!(4_294_967_295_u32),
            ),
        ] {
            let client = RecordingClient::mapped(vec![("id", field_mapping(mapped_as))]);
            let members =
                member_batch_keyed(key.to_array().expect("key should render to an array"), &[0]);

            delete_group_remainder(
                &client,
                "idx",
                &chunked_key_of(source.clone()),
                &["id".to_string()],
                &members,
            )
            .await
            .unwrap_or_else(|e| panic!("{source} under `{mapped_as}` must be addressable: {e}"));

            assert_eq!(
                client.queries().len(),
                1,
                "{source} under `{mapped_as}` must reach the index, not return early"
            );
            assert_eq!(
                client.queries()[0]["bool"]["should"][0]["bool"]["filter"][0]["term"]["id"],
                expected,
                "the group must be filtered on the column itself"
            );
        }
    }

    /// `renders_a_term` has to agree with [`scalar_to_term_value`], which is the function that
    /// decides whether a `term` is emitted at all. They are separate because one answers on a
    /// type and the other on a value, so this pins them together: a type added to either without
    /// the other shows up here rather than as a refusal for a delete that never issues, or a
    /// missed refusal for one that does.
    #[test]
    fn term_renderable_types_match_scalar_to_term_value() {
        let cases = [
            ScalarValue::Utf8(Some("a".to_string())),
            ScalarValue::LargeUtf8(Some("a".to_string())),
            ScalarValue::Utf8View(Some("a".to_string())),
            ScalarValue::Boolean(Some(true)),
            ScalarValue::Int8(Some(1)),
            ScalarValue::Int16(Some(1)),
            ScalarValue::Int32(Some(1)),
            ScalarValue::Int64(Some(1)),
            ScalarValue::UInt8(Some(1)),
            ScalarValue::UInt16(Some(1)),
            ScalarValue::UInt32(Some(1)),
            ScalarValue::UInt64(Some(1)),
            ScalarValue::Float32(Some(1.0)),
            ScalarValue::Float64(Some(1.0)),
            ScalarValue::Date32(Some(1)),
            ScalarValue::Date64(Some(1)),
            ScalarValue::TimestampMillisecond(Some(1), None),
            ScalarValue::Decimal128(Some(1), 10, 2),
            ScalarValue::Binary(Some(vec![1])),
        ];
        for value in cases {
            assert_eq!(
                renders_a_term(&value.data_type()),
                scalar_to_term_value(&value).is_some(),
                "`renders_a_term` and `scalar_to_term_value` disagree about {}",
                value.data_type()
            );
        }
    }

    /// The width rule must be threaded into the sub-field fallback as well as the column itself,
    /// which is two call sites — a regression that dropped it from one would leave the other's
    /// test green.
    #[tokio::test]
    async fn an_int64_column_mapped_double_with_an_exact_sub_field_uses_the_sub_field() {
        let mut narrow = field_mapping("double");
        narrow.fields = Some(std::collections::HashMap::from([(
            "exact".to_string(),
            field_mapping("long"),
        )]));
        let client = RecordingClient::mapped(vec![("id", narrow)]);

        delete_group_remainder(
            &client,
            "idx",
            &chunked_key_of(DataType::Int64),
            &["id".to_string()],
            &member_batch_keyed(
                Arc::new(Int64Array::from(vec![9_007_199_254_740_992_i64])),
                &[0],
            ),
        )
        .await
        .expect("the prune should succeed on the exact sub-field");

        assert_eq!(
            client.queries()[0]["bool"]["should"][0]["bool"]["filter"],
            json!([{"term": {"id.exact": 9_007_199_254_740_992_i64}}]),
        );
    }

    /// The refusal must not fire on a mapping the runtime itself writes. `primary_key_mapping`
    /// maps a `Float32` key column `float` and a `Date32`/`Date64`/`Timestamp` one `date`, both
    /// of which round their values — but [`scalar_to_term_value`] renders neither type, so those
    /// columns emit no `term` and the group is dropped by [`collect_groups`] rather than
    /// refused. Refusing them would fail a delete with a message telling the user to re-map a
    /// column the runtime chose the mapping for.
    #[tokio::test]
    async fn a_rounding_mapping_the_runtime_writes_is_not_refused() {
        for (source, mapped_as) in [
            (DataType::Float32, "float"),
            (DataType::Float64, "double"),
            (DataType::Date32, "date"),
            (DataType::Date64, "date"),
            (DataType::Timestamp(TimeUnit::Millisecond, None), "date"),
        ] {
            let client = RecordingClient::mapped(vec![("id", field_mapping(mapped_as))]);

            delete_group_remainder(
                &client,
                "idx",
                &chunked_key_of(source.clone()),
                &["id".to_string()],
                &member_batch_keyed(arrow::array::new_null_array(&source, 1), &[0]),
            )
            .await
            .unwrap_or_else(|e| {
                panic!("{source} under `{mapped_as}` is what the runtime writes; it must not be refused: {e}")
            });
        }
    }

    /// The refusal's wording is what a user acts on, so it is asserted rather than left to a
    /// reword to quietly drop the column, the mapping, the source type or the remedy.
    #[test]
    fn the_over_match_refusal_names_the_column_mapping_source_and_remedy() {
        let message = Error::KeyColumnOverMatches {
            index: "reviews".to_string(),
            column: "order_id".to_string(),
            mapped_as: "scaled_float".to_string(),
            source_type: "Int64".to_string(),
            why: "indexes `round(value * scaling_factor)` rather than the value",
        }
        .to_string();

        for expected in [
            "'reviews'",
            "'order_id'",
            "Int64",
            "`scaled_float`",
            "round(value * scaling_factor)",
            "reaches every other value indexed under that same term",
            "the delete was not issued",
            "`keyword`",
            "https://spiceai.org/docs/features/search",
        ] {
            assert!(
                message.contains(expected),
                "the refusal must carry {expected:?}, got: {message}"
            );
        }
        assert!(
            !message.contains('\n'),
            "a log line must stay on one line, got: {message}"
        );
    }

    /// Every rounding type must also be an exact-match type, because `term_over_matches` is only
    /// consulted for a mapping that got that far. Dropping one from
    /// [`TERM_EXACT_FIELD_TYPES`] does not make it *more* refused — it makes it refused with the
    /// wrong message ("no exact-match filter can address") and, worse, refused even for a source
    /// type that renders no term, which is a mapping the runtime itself writes.
    #[test]
    fn every_rounding_type_is_also_an_exact_match_type() {
        for (rounding, _) in TERM_ROUNDING_FIELD_TYPES {
            assert!(
                TERM_EXACT_FIELD_TYPES.contains(rounding),
                "`{rounding}` must be in TERM_EXACT_FIELD_TYPES for term_over_matches to reach it"
            );
        }
    }

    /// The counterpart of the refusal: a type whose indexed form *is* the value still resolves to
    /// the column itself, so tightening the list did not cost an index its delete.
    ///
    /// The key here is `Utf8`, so the set is [`STRING_FIELD_TYPES`] rather than every non-rounding
    /// exact type: Elasticsearch reaches a string through a parse for the numeric and date types,
    /// and that parse collapses distinct keys onto one term (see [`STRING_FIELD_TYPES`] for the
    /// measurements). `a_string_key_column_mapped_to_a_parsed_type_refuses_before_issuing` is the
    /// other half, and `a_mapping_wide_enough_for_the_source_type_still_resolves` keeps the
    /// numeric types addressable for the numeric key columns they are exact for.
    #[tokio::test]
    async fn an_exactly_indexed_group_column_still_resolves_to_the_column() {
        let rounding: Vec<&str> = TERM_ROUNDING_FIELD_TYPES.iter().map(|(t, _)| *t).collect();
        for field_type in STRING_FIELD_TYPES {
            if rounding.contains(field_type) {
                continue;
            }
            let client = RecordingClient::mapped(vec![("id", field_mapping(field_type))]);

            delete_group_remainder(
                &client,
                "idx",
                &chunked_key(),
                &["id".to_string()],
                &member_batch(&[(Some("A"), 0)]),
            )
            .await
            .unwrap_or_else(|e| panic!("`{field_type}` must still address the group: {e}"));

            assert_eq!(
                client.queries()[0]["bool"]["should"][0]["bool"]["filter"],
                json!([{"term": {"id": "A"}}]),
                "`{field_type}` must be filtered on the column itself"
            );
        }
    }

    /// The group columns are resolved the same way [`delete_by_keys`] resolves them, so a
    /// `text`-mapped key column is filtered on its `keyword` multi-field rather than on the
    /// analyzed tokens the column itself indexes (#13714).
    #[tokio::test]
    async fn a_text_mapped_group_column_is_filtered_on_its_keyword_sub_field() {
        let client = RecordingClient::mapped(vec![("id", dynamic_string_mapping())]);

        delete_group_remainder(
            &client,
            "idx",
            &chunked_key(),
            &["id".to_string()],
            &member_batch(&[(Some("ORDER-1024"), 0)]),
        )
        .await
        .expect("the prune should succeed");

        assert_eq!(
            client.queries()[0]["bool"]["should"][0]["bool"]["filter"],
            json!([{"term": {"id.keyword": "ORDER-1024"}}]),
        );
    }

    /// A group column with no exact-match field fails the prune before a request is issued, for
    /// the same reason [`delete_by_keys`] refuses it: a filter that matches nothing would delete
    /// nothing and report success.
    #[tokio::test]
    async fn a_group_column_with_no_exact_match_field_refuses_before_issuing() {
        let client = RecordingClient::mapped(vec![("id", field_mapping("text"))]);

        let err = delete_group_remainder(
            &client,
            "idx",
            &chunked_key(),
            &["id".to_string()],
            &member_batch(&[(Some("ORDER-1024"), 0)]),
        )
        .await
        .expect_err("a group column with no exact-match field must fail the prune");

        assert!(
            err.to_string().contains("'id'"),
            "the error must name the group column, got: {err}"
        );
        assert!(client.queries().is_empty());
    }

    /// The reported bug (#13714) on the half `_id` addressing cannot reach: a chunked index's
    /// delete filters on the key column, and a string column the runtime did not map exactly is
    /// `text` — analyzed tokens an unanalyzed `term` never matches. Filtering on the column name
    /// removed nothing and reported success; the delete must name the `keyword` multi-field that
    /// holds the value itself.
    #[tokio::test]
    async fn a_text_mapped_key_column_is_filtered_on_its_keyword_sub_field() {
        let client = RecordingClient::mapped(vec![("id", dynamic_string_mapping())]);

        delete_by_keys(
            &client,
            "idx",
            &[
                pk("id", DataType::Utf8),
                pk(CHUNKED_INDEX_CHUNK_KEY, DataType::UInt64),
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
                    "should": [{"bool": {"filter": [{"term": {"id.keyword": "ORDER-1024"}}]}}],
                    "minimum_should_match": 1
                }
            })],
            "a `text`-mapped key column holds analyzed tokens, so the filter has to name the \
             exact-match sub-field instead"
        );
    }

    /// A `text` column with no exact sub-field cannot be filtered on at all. Issuing the filter
    /// anyway is what made a delete of nothing look like a delete of everything asked for, so the
    /// delete has to refuse — and refuse before issuing a request, not after.
    #[tokio::test]
    async fn a_key_column_with_no_exact_match_field_refuses_instead_of_deleting_nothing() {
        let client = RecordingClient::mapped(vec![("id", field_mapping("text"))]);

        let err = delete_by_keys(
            &client,
            "idx",
            &[
                pk("id", DataType::Utf8),
                pk(CHUNKED_INDEX_CHUNK_KEY, DataType::UInt64),
            ],
            &["id".to_string()],
            &string_key_batch(vec![Some("ORDER-1024")]),
        )
        .await
        .expect_err("a key column with no exact-match field must fail the delete");

        let message = err.to_string();
        assert!(
            message.contains("'id'") && message.contains("text"),
            "the error must name the key column and how it is mapped, got: {message}"
        );
        assert!(
            client.queries().is_empty(),
            "no _delete_by_query may be issued for a key the filter cannot address"
        );
    }

    /// A key column the user declared non-filterable is mapped `index: false` — an exact type
    /// that is searchable nowhere. Its documents are unreachable by any filter, so the delete has
    /// to refuse rather than issue a `term` that cannot match.
    #[tokio::test]
    async fn an_unsearchable_key_column_refuses_despite_an_exact_type() {
        let mut unsearchable = field_mapping("keyword");
        unsearchable.index = Some(serde_json::Value::Bool(false));
        let client = RecordingClient::mapped(vec![("id", unsearchable)]);

        let err = delete_by_keys(
            &client,
            "idx",
            &[
                pk("id", DataType::Utf8),
                pk(CHUNKED_INDEX_CHUNK_KEY, DataType::UInt64),
            ],
            &["id".to_string()],
            &string_key_batch(vec![Some("ORDER-1024")]),
        )
        .await
        .expect_err("an unsearchable key column must fail the delete");

        assert!(err.to_string().contains("'id'"), "got: {err}");
        assert!(client.queries().is_empty());
    }

    /// Elasticsearch maps a field the first time a document carries it, so a key column absent
    /// from the mapping means no document holds it — a delete of rows the index does not have.
    /// That is success with no request, not a refusal.
    #[tokio::test]
    async fn a_key_column_absent_from_the_mapping_issues_no_request() {
        let client = RecordingClient::mapped(vec![("other", field_mapping("keyword"))]);

        delete_by_keys(
            &client,
            "idx",
            &[
                pk("id", DataType::Utf8),
                pk(CHUNKED_INDEX_CHUNK_KEY, DataType::UInt64),
            ],
            &["id".to_string()],
            &string_key_batch(vec![Some("ORDER-1024")]),
        )
        .await
        .expect("an index holding no such column has nothing to delete");

        assert!(client.queries().is_empty());
    }

    /// The `keyword` multi-field dynamic mapping derives stops indexing past `ignore_above`, so a
    /// longer key is stored but unreachable — the same silent no-op one layer down. The delete
    /// has to say so rather than issue a filter that cannot match.
    #[tokio::test]
    async fn a_key_longer_than_ignore_above_refuses_rather_than_filtering_on_it() {
        let client = RecordingClient::mapped(vec![("id", dynamic_string_mapping())]);
        let long_key = "x".repeat(257);

        let err = delete_by_keys(
            &client,
            "idx",
            &[
                pk("id", DataType::Utf8),
                pk(CHUNKED_INDEX_CHUNK_KEY, DataType::UInt64),
            ],
            &["id".to_string()],
            &string_key_batch(vec![Some(long_key.as_str())]),
        )
        .await
        .expect_err("a key Elasticsearch never indexed must fail the delete");

        let message = err.to_string();
        assert!(
            message.contains("ignore_above") && message.contains("id.keyword"),
            "the error must name the limit and the field it applies to, got: {message}"
        );
        assert!(
            client.queries().is_empty(),
            "no _delete_by_query may be issued for a key the filter cannot address"
        );
    }

    /// A key at the limit is indexed, so it must still be deleted — the refusal above is a
    /// boundary, not a rounding-down of what the index holds.
    #[tokio::test]
    async fn a_key_exactly_at_ignore_above_is_still_deleted() {
        let client = RecordingClient::mapped(vec![("id", dynamic_string_mapping())]);
        let key = "x".repeat(256);

        delete_by_keys(
            &client,
            "idx",
            &[
                pk("id", DataType::Utf8),
                pk(CHUNKED_INDEX_CHUNK_KEY, DataType::UInt64),
            ],
            &["id".to_string()],
            &string_key_batch(vec![Some(key.as_str())]),
        )
        .await
        .expect("delete should succeed");

        assert_eq!(
            client.queries(),
            vec![json!({
                "bool": {
                    "should": [{"bool": {"filter": [{"term": {"id.keyword": key}}]}}],
                    "minimum_should_match": 1
                }
            })],
        );
    }

    /// `ignore_above` counts characters, not bytes, so a key of multi-byte characters that is
    /// within the limit is indexed and must be deleted rather than refused.
    #[tokio::test]
    async fn a_multi_byte_key_within_ignore_above_is_measured_in_characters() {
        let client = RecordingClient::mapped(vec![("id", dynamic_string_mapping())]);
        let key = "é".repeat(200);

        delete_by_keys(
            &client,
            "idx",
            &[
                pk("id", DataType::Utf8),
                pk(CHUNKED_INDEX_CHUNK_KEY, DataType::UInt64),
            ],
            &["id".to_string()],
            &string_key_batch(vec![Some(key.as_str())]),
        )
        .await
        .expect("a 200-character key is inside a 256-character limit");

        assert_eq!(client.queries().len(), 1);
    }

    /// An unreadable mapping leaves it unknown which field an exact-match filter has to name, and
    /// filtering on the column name regardless is the silent no-op this addressing exists to
    /// avoid. Report it instead — the callers log this rather than assuming the delete applied.
    #[tokio::test]
    async fn an_unreadable_mapping_fails_the_delete_rather_than_guessing() {
        // No mapping configured, so the stub's `get_mapping` errors.
        let client = RecordingClient::default();

        let err = delete_by_keys(
            &client,
            "idx",
            &[
                pk("id", DataType::Utf8),
                pk(CHUNKED_INDEX_CHUNK_KEY, DataType::UInt64),
            ],
            &["id".to_string()],
            &string_key_batch(vec![Some("ORDER-1024")]),
        )
        .await
        .expect_err("an unreadable mapping must fail the delete");

        assert!(err.to_string().contains("could not be read"), "got: {err}");
        assert!(client.queries().is_empty());
    }

    /// A numeric key column was never broken by the mapping gap — `long` is exact — so it must
    /// keep being filtered on the column itself, with no sub-field detour.
    #[tokio::test]
    async fn a_numeric_key_column_is_filtered_on_the_column_itself() {
        let client = RecordingClient::mapped(vec![("id", field_mapping("long"))]);
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));
        let keys = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![7_i64]))])
            .expect("int key batch should build");

        delete_by_keys(
            &client,
            "idx",
            &[
                pk("id", DataType::Int64),
                pk(CHUNKED_INDEX_CHUNK_KEY, DataType::UInt64),
            ],
            &["id".to_string()],
            &keys,
        )
        .await
        .expect("delete should succeed");

        assert_eq!(
            client.queries(),
            vec![json!({
                "bool": {
                    "should": [{"bool": {"filter": [{"term": {"id": 7}}]}}],
                    "minimum_should_match": 1
                }
            })],
        );
    }

    /// `GET /<alias>/_mapping` keys its body by the concrete index the alias resolves to, not by
    /// the name asked for, so the single entry is the one to read.
    #[tokio::test]
    async fn a_mapping_returned_under_another_index_name_is_still_read() {
        let client = RecordingClient {
            mapping: Some(mapping_of(
                "idx-000001",
                vec![("id", dynamic_string_mapping())],
            )),
            ..RecordingClient::default()
        };

        delete_by_keys(
            &client,
            "idx",
            &[
                pk("id", DataType::Utf8),
                pk(CHUNKED_INDEX_CHUNK_KEY, DataType::UInt64),
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
                    "should": [{"bool": {"filter": [{"term": {"id.keyword": "ORDER-1024"}}]}}],
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

    /// End-to-end over the modeled store: a whole-key delete removes exactly the addressed
    /// documents and leaves the rest, whatever the query body looks like.
    #[tokio::test]
    async fn a_whole_key_delete_removes_only_the_addressed_documents() {
        let client = RecordingClient::with_ids(&["ORDER-1024", "ORDER-1025", "ORDER-1026"]);

        delete_by_keys(
            &client,
            "idx",
            &[pk("id", DataType::Utf8)],
            &["id".to_string()],
            &string_key_batch(vec![Some("ORDER-1025")]),
        )
        .await
        .expect("delete should succeed");

        assert_eq!(client.present_ids(), vec!["ORDER-1024", "ORDER-1026"]);
    }

    fn ids(values: &[&str]) -> Vec<String> {
        values.iter().map(|v| (*v).to_string()).collect()
    }

    /// End-to-end over the modeled store: the write path's eviction removes exactly the
    /// documents whose rows it could not index, and leaves every other document alone.
    #[tokio::test]
    async fn delete_by_ids_removes_only_the_addressed_documents() {
        let client = RecordingClient::with_ids(&["ORDER-1024", "ORDER-1025", "ORDER-1026"]);

        delete_by_ids(&client, "idx", &ids(&["ORDER-1025"]))
            .await
            .expect("delete should succeed");

        assert_eq!(client.present_ids(), vec!["ORDER-1024", "ORDER-1026"]);
    }

    #[tokio::test]
    async fn delete_by_ids_addresses_documents_by_id() {
        let client = RecordingClient::default();

        delete_by_ids(&client, "idx", &ids(&["7", "8"]))
            .await
            .expect("delete should succeed");

        assert_eq!(
            client.queries(),
            vec![json!({"ids": {"values": ["7", "8"]}})]
        );
    }

    /// A write that rejected nothing must not reach Elasticsearch at all — an `ids` query
    /// with an empty list is not a no-op to send, it is a request that costs a round trip.
    #[tokio::test]
    async fn delete_by_ids_issues_no_request_for_an_empty_list() {
        let client = RecordingClient::default();

        delete_by_ids(&client, "idx", &[])
            .await
            .expect("delete should succeed");

        assert!(client.queries().is_empty());
    }

    /// Chunked exactly as [`delete_by_keys`] is, so a large eviction cannot build an
    /// unbounded id list in one request.
    #[tokio::test]
    async fn delete_by_ids_chunks_a_large_eviction() {
        let client = RecordingClient::default();
        let all: Vec<String> = (0..=DELETE_CHUNK_ROWS).map(|i| i.to_string()).collect();

        delete_by_ids(&client, "idx", &all)
            .await
            .expect("delete should succeed");

        let queries = client.queries();
        assert_eq!(queries.len(), 2, "one request per {DELETE_CHUNK_ROWS} ids");
        let first = queries[0]["ids"]["values"]
            .as_array()
            .expect("an ids query carries an array");
        assert_eq!(first.len(), DELETE_CHUNK_ROWS);
    }

    /// The eviction reports a partially-applied delete the same way [`delete_by_keys`] does:
    /// a document left in place is a stale vector still being served.
    #[tokio::test]
    async fn delete_by_ids_reports_a_partially_applied_delete() {
        let client = RecordingClient::answering(json!({
            "took": 1,
            "timed_out": false,
            "total": 2,
            "deleted": 1,
            "batches": 1,
            "version_conflicts": 1,
            "noops": 0,
            "retries": {"bulk": 0, "search": 0},
            "throttled_millis": 0,
            "failures": [],
        }));

        let err = delete_by_ids(&client, "idx", &ids(&["7", "8"]))
            .await
            .expect_err("a delete that left a document behind must be reported");

        assert!(
            err.to_string().contains("only partially"),
            "unexpected error: {err}"
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

    /// An early chunk's partial delete does not cancel the rest of the batch. The chunks address
    /// disjoint slices of `keys`, so stopping at the first partial response would leave documents
    /// behind for rows no request ever named — the very divergence this check exists to report.
    #[tokio::test]
    async fn an_early_partial_delete_still_issues_the_remaining_chunks() {
        let client = RecordingClient::answering_in_turn(vec![
            json!({"total": 1, "deleted": 0, "version_conflicts": 1, "failures": []}),
            clean_delete_response(DELETE_CHUNK_ROWS as u64),
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
        .expect_err("the first chunk's partial delete must still surface");

        assert_eq!(
            client.queries().len(),
            2,
            "the second chunk should have been issued despite the first coming back partial"
        );
        assert!(
            err.to_string().contains("1 version conflict(s)"),
            "the first chunk's conflict should be the reported one: {err}"
        );
    }

    /// The same reasoning as the partial-body case above, on the arm that reports through an
    /// `Err` instead of a 200 body. An error from the client does not mean the request never
    /// reached the index — `JsonParse` is raised only after a 2xx, and a status error can land on
    /// a delete Elasticsearch already applied in part — so returning at the first one would leave
    /// the later chunks unissued and turn one chunk's unknown outcome into a batch-wide one.
    #[tokio::test]
    async fn an_early_chunk_error_still_issues_the_remaining_chunks() {
        let client = RecordingClient::answering(clean_delete_response(DELETE_CHUNK_ROWS as u64))
            .erroring_on(1);
        let ids: Vec<String> = (0..=DELETE_CHUNK_ROWS).map(|i| i.to_string()).collect();

        let err = delete_by_keys(
            &client,
            "idx",
            &[pk("id", DataType::Utf8)],
            &["id".to_string()],
            &string_key_batch(ids.iter().map(|s| Some(s.as_str())).collect()),
        )
        .await
        .expect_err("the first chunk's failure must still surface");

        assert_eq!(
            client.queries().len(),
            2,
            "the second chunk should have been issued despite the first erroring"
        );
        assert!(
            err.to_string().contains("request 1 failed at the index"),
            "the first chunk's error should be the reported one: {err}"
        );
    }

    /// A later chunk's error is reported when every earlier one applied cleanly — without this,
    /// the loop could swallow the last chunk's failure and report the whole delete as applied.
    #[tokio::test]
    async fn a_later_chunk_error_is_still_reported() {
        let client = RecordingClient::answering(clean_delete_response(DELETE_CHUNK_ROWS as u64))
            .erroring_on(2);
        let ids: Vec<String> = (0..=DELETE_CHUNK_ROWS).map(|i| i.to_string()).collect();

        let err = delete_by_keys(
            &client,
            "idx",
            &[pk("id", DataType::Utf8)],
            &["id".to_string()],
            &string_key_batch(ids.iter().map(|s| Some(s.as_str())).collect()),
        )
        .await
        .expect_err("a failure in the last chunk must not be reported as a clean delete");

        assert_eq!(client.queries().len(), 2);
        assert!(
            err.to_string().contains("request 2 failed at the index"),
            "the erroring chunk should be the reported one: {err}"
        );
    }

    /// The first failure is the reported one whether it arrived as an `Err` or as a partial body:
    /// it is the one whose surrounding state a reconcile starts from.
    #[tokio::test]
    async fn the_first_of_two_failing_chunks_is_the_reported_one() {
        let client = RecordingClient::answering_in_turn(vec![
            clean_delete_response(0),
            json!({"total": 1, "deleted": 0, "version_conflicts": 1, "failures": []}),
        ])
        .erroring_on(1);
        let ids: Vec<String> = (0..=DELETE_CHUNK_ROWS).map(|i| i.to_string()).collect();

        let err = delete_by_keys(
            &client,
            "idx",
            &[pk("id", DataType::Utf8)],
            &["id".to_string()],
            &string_key_batch(ids.iter().map(|s| Some(s.as_str())).collect()),
        )
        .await
        .expect_err("both chunks failed, so the delete must not report success");

        assert_eq!(client.queries().len(), 2);
        assert!(
            err.to_string().contains("request 1 failed at the index"),
            "the earlier failure should win over the later partial body: {err}"
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

    /// The field paths a delete resolves for `columns` against an index that maps each of them
    /// exactly — which is what the runtime's own mapping produces. These tests are about *which*
    /// columns a query filters on, not about resolving the path to filter them on.
    fn exact_paths(columns: &[String]) -> Vec<KeyFieldPath> {
        columns
            .iter()
            .map(|column| KeyFieldPath {
                column: column.clone(),
                path: column.clone(),
                ignore_above: None,
            })
            .collect()
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
        let query = build_or_of_row_term_queries(
            &exact_paths(&document_key_columns(&chunked)),
            &base_keys(&[7]),
        )
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

        let err = build_or_of_row_term_queries(&exact_paths(&full), &base_keys(&[7]))
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

        let query =
            build_or_of_row_term_queries(&exact_paths(&document_key_columns(&chunked)), &keys)
                .expect("query builds")
                .expect("non-empty batch produces a query");

        assert_eq!(
            query["bool"]["should"][0]["bool"]["filter"],
            serde_json::json!([{ "term": { "id": 7 } }]),
            "the chunk id must not narrow the delete: {query}"
        );
    }

    /// Every leaf query under `query`, counting through nested `bool`s the way Elasticsearch
    /// does when it raises `too_many_nested_clauses`.
    fn leaf_clause_count(query: &Value) -> usize {
        let Some(bool_query) = query.get("bool") else {
            return 1;
        };
        ["should", "filter", "must", "must_not"]
            .iter()
            .filter_map(|occur| bool_query.get(*occur))
            .flat_map(|clauses| match clauses {
                Value::Array(clauses) => clauses.as_slice(),
                clause => std::slice::from_ref(clause),
            })
            .map(leaf_clause_count)
            .sum()
    }

    /// The group budget is spent in members, but Elasticsearch charges the request in leaves, and
    /// a group is worth one `term` leaf per key column plus its `ids` leaf. Pins that ceiling so a
    /// group that grows a clause — or a budget raised on the member accounting alone — has to say
    /// so here rather than in a rejected delete, which lands *after* the bulk write and leaves the
    /// index carrying the chunks the prune existed to remove.
    ///
    /// The bound is the smallest clause budget an Elasticsearch 8 node was measured to derive from
    /// its heap (2340 on a 256 MiB heap; 9362 on 1 GiB). `indices.query.bool.max_clause_count` is
    /// deprecated and ignored there, so its old 1024 default is not the ceiling to size against.
    #[test]
    fn group_remainder_request_leaf_count_stays_within_the_smallest_supported_clause_budget() {
        const SMALLEST_MEASURED_CLAUSE_BUDGET: usize = 2340;

        for key_columns in 1..=3 {
            // One member each, so the member budget admits as many groups as it can — the shape
            // that carries the most leaves for a given budget.
            let groups: Vec<MemberGroup> = (0..PRUNE_MEMBERS_PER_REQUEST)
                .map(|group| MemberGroup {
                    terms: (0..key_columns)
                        .map(|column| json!({ "term": { format!("k{column}"): format!("g{group}") } }))
                        .collect(),
                    survivors: vec![format!("id{group}")],
                })
                .collect();

            let requests = group_requests(groups);
            assert_eq!(
                requests.len(),
                1,
                "{key_columns} key columns: the member budget should hold every group in one request"
            );

            let leaves = leaf_clause_count(&requests[0]);
            assert_eq!(
                leaves,
                PRUNE_MEMBERS_PER_REQUEST * (key_columns + 1),
                "{key_columns} key columns: a group is worth one term leaf per column plus its ids leaf"
            );
            assert!(
                leaves <= SMALLEST_MEASURED_CLAUSE_BUDGET,
                "{key_columns} key columns: {leaves} leaves exceeds the {SMALLEST_MEASURED_CLAUSE_BUDGET} a 256 MiB-heap node allows"
            );
        }
    }
}
