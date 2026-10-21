/*
Copyright 2024-2026 The Spice.ai OSS Authors

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

//! Dataset-agnostic search harness: the SQL and result-mapping logic of a
//! `testoperator run search` run. Every spicepod exposes the same fixed schema — a `corpus` table,
//! a `test_queries` table with `_id`/`text`, and a `relevance_data` table with `query-id`,
//! `corpus-id`, and `score` (each may be a dataset or a view) — so these steps work for any
//! dataset the spicepod loads.

use std::collections::{BTreeMap, HashMap};

use test_framework::{
    anyhow,
    arrow::{
        self,
        array::{Array, AsArray, Int64Array, RecordBatch, StringArray},
        datatypes::DataType,
    },
    futures::TryStreamExt,
    spiced::SpicedInstance,
    spicetest::search::{SearchConfig, SearchRequest, SearchResponse, SearchResult, SqlSearchHit},
};

/// Reads a whole column as UTF-8 strings, accepting `Utf8`, `LargeUtf8`, or any type Arrow can cast
/// to text (for example an integer id column). Datasets come from arbitrary connectors, so the id
/// columns are not guaranteed to be `LargeUtf8` the way the MTEB parquet files are.
///
/// Returns one owned `String` per row; a NULL cell becomes an empty string, matching the previous
/// behaviour where every id/text cell was assumed present.
fn read_text_column(batch: &RecordBatch, name: &str) -> anyhow::Result<Vec<String>> {
    let column = batch
        .column_by_name(name)
        .ok_or_else(|| anyhow::anyhow!("Missing '{name}' column"))?;

    // A no-op when the column is already `Utf8`; otherwise this normalizes `LargeUtf8`/`Utf8View`
    // and casts integer ids to their text form so downstream matching against qrels is uniform.
    let utf8 = arrow::compute::cast(column, &DataType::Utf8).map_err(|e| {
        anyhow::anyhow!(
            "Failed to read '{name}' column as text (unsupported type {:?}): {e}",
            column.data_type()
        )
    })?;

    let string_array: &StringArray = utf8.as_string::<i32>();
    Ok((0..string_array.len())
        .map(|i| {
            if string_array.is_valid(i) {
                string_array.value(i).to_string()
            } else {
                String::new()
            }
        })
        .collect())
}

/// Builds the search config by reading the queries from `test_queries`, which every search spicepod
/// exposes with `_id`/`text` columns.
pub(crate) async fn init_search_config(
    spiced_instance: &SpicedInstance,
    search_limit: Option<usize>,
) -> anyhow::Result<SearchConfig> {
    let mut spice_client = spiced_instance.spice_client(None, false).await?;

    let records = execute_sql(
        &mut spice_client,
        "SELECT _id as id, text FROM test_queries",
    )
    .await?;

    let queries = to_search_requests(&records, search_limit)?;

    Ok(SearchConfig::new().add_requests(queries))
}

fn to_search_requests(
    records: &[RecordBatch],
    search_limit: Option<usize>,
) -> anyhow::Result<Vec<SearchRequest>> {
    let mut queries = Vec::new();
    for batch in records {
        let ids = read_text_column(batch, "id")?;
        let texts = read_text_column(batch, "text")?;

        for (id, text) in ids.into_iter().zip(texts) {
            let mut search_request = SearchRequest::new(id, text);
            if let Some(limit) = search_limit {
                search_request = search_request.with_limit(limit);
            }
            queries.push(search_request);
        }
    }

    Ok(queries)
}

/// Loads the relevance judgments (qrels) from `relevance_data`, keyed query id → corpus id → grade.
pub(crate) async fn get_query_relevance_data(
    spiced_instance: &SpicedInstance,
) -> anyhow::Result<HashMap<String, HashMap<String, i32>>> {
    let mut spice_client = spiced_instance.spice_client(None, false).await?;

    // Cast `score` to `BIGINT` so the loader handles both `int64` judgments (for example the MTEB
    // `_top_250_only_w_correct-v2` layout) and `float64` judgments (the standard MTEB layout).
    // Relevance judgments are whole numbers, so the cast is exact. Every dataset must supply
    // whole-number grades (see the issue #12935 schema).
    let records = execute_sql(
        &mut spice_client,
        r#"SELECT "query-id", "corpus-id", CAST(score AS BIGINT) AS score FROM relevance_data"#,
    )
    .await?;

    extract_query_relevance_from_batches(&records)
}

fn extract_query_relevance_from_batches(
    records: &[RecordBatch],
) -> anyhow::Result<HashMap<String, HashMap<String, i32>>> {
    let mut query_relevance = HashMap::new();

    for batch in records {
        let query_ids = read_text_column(batch, "query-id")?;
        let corpus_ids = read_text_column(batch, "corpus-id")?;

        let score_column = batch
            .column_by_name("score")
            .ok_or_else(|| anyhow::anyhow!("Missing 'score' column"))?
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| anyhow::anyhow!("Failed to downcast 'score' column to Int64Array"))?;

        for (i, (query_id, corpus_id)) in query_ids.into_iter().zip(corpus_ids).enumerate() {
            let score = i32::try_from(score_column.value(i))
                .map_err(|e| anyhow::anyhow!("Failed to convert score to i32: {e}"))?;

            query_relevance
                .entry(query_id)
                .or_insert_with(HashMap::new)
                .insert(corpus_id, score);
        }
    }

    Ok(query_relevance)
}

/// Converts raw search results into `query id → (corpus id → score)` for evaluation. A SQL pipeline
/// row reads the corpus id from its `_id` column. For a `/v1/search` result, the corpus row id comes
/// from the dataset's declared `row_id`, whose column name differs per dataset (`_id` for MTEB,
/// `chunk_id` for `ConTEB`), so this takes the single primary-key field of each result rather than
/// a fixed name. A corpus whose `row_id` names more than one column cannot be matched against the
/// single `corpus-id` qrel column, so such a result is skipped.
pub(crate) fn transform_search_results_for_eval(
    search: &BTreeMap<String, SearchResult>,
) -> HashMap<String, HashMap<String, f64>> {
    search
        .iter()
        .map(|(query_id, search_result)| {
            let corpus_scores = match &search_result.response {
                SearchResponse::SearchApi(response) => response
                    .results
                    .iter()
                    .filter_map(|result| {
                        sole_primary_key_value(&result.primary_key).map(|id| (id, result.score))
                    })
                    .collect(),
                SearchResponse::Sql(response) => rank_scores(&response.hits),
            };
            (query_id.clone(), corpus_scores)
        })
        .collect()
}

/// Returns the single primary-key value of a result as a corpus id, or `None` when the result has
/// zero or more than one primary-key field. A corpus with a multi-column `row_id` cannot be matched
/// against the single `corpus-id` qrel column, so it is skipped rather than guessed at.
fn sole_primary_key_value(primary_key: &HashMap<String, serde_json::Value>) -> Option<String> {
    let mut entries = primary_key.values();
    match (entries.next(), entries.next()) {
        (Some(value), None) => primary_key_value_to_string(value),
        _ => None,
    }
}

/// Scores SQL pipeline rows by their rank, so that evaluation uses the pipeline's own row order. The
/// evaluator sorts by score, and a pipeline's `_score` can be absent or in a different order than
/// its rows (for example, after a rerank). When a pipeline returns the same `_id` more than once,
/// the best rank is kept.
fn rank_scores(hits: &[SqlSearchHit]) -> HashMap<String, f64> {
    let mut scores = HashMap::with_capacity(hits.len());
    for (rank, hit) in hits.iter().enumerate() {
        #[expect(
            clippy::cast_precision_loss,
            reason = "a result count is far below 2^52, so the conversion is exact"
        )]
        let score = (hits.len() - rank) as f64;
        scores.entry(hit.id.to_string()).or_insert(score);
    }
    scores
}

fn primary_key_value_to_string(value: &serde_json::Value) -> Option<String> {
    match value {
        serde_json::Value::String(s) => Some(s.clone()),
        serde_json::Value::Number(n) => Some(n.to_string()),
        _ => None,
    }
}

async fn execute_sql(
    spice_client: &mut spiceai::Client,
    sql: &str,
) -> anyhow::Result<Vec<RecordBatch>> {
    let res = spice_client
        .sql(sql)
        .await?
        .try_collect::<Vec<RecordBatch>>()
        .await?;
    Ok(res)
}

#[cfg(test)]
mod tests {
    use super::{
        primary_key_value_to_string, rank_scores, read_text_column, sole_primary_key_value,
    };
    use std::collections::HashMap;
    use std::sync::Arc;
    use test_framework::arrow::array::{Int64Array, LargeStringArray, RecordBatch, StringArray};
    use test_framework::spicetest::search::SqlSearchHit;

    fn sql_hits(json: &str) -> Vec<SqlSearchHit> {
        serde_json::from_str(json).expect("failed to parse SQL rows")
    }

    #[test]
    fn rank_scores_follow_row_order_not_score() {
        // `_score` ascends, but the row order is the rank: "b" is first and must score highest.
        let scores = rank_scores(&sql_hits(
            r#"[{"_id":"b","_score":0.1},{"_id":"a","_score":0.9},{"_id":3}]"#,
        ));
        assert_eq!(scores.len(), 3);
        assert!(scores["b"] > scores["a"]);
        assert!(scores["a"] > scores["3"]);
    }

    #[test]
    fn rank_scores_keep_best_rank_of_duplicate_id() {
        let scores = rank_scores(&sql_hits(r#"[{"_id":"a"},{"_id":"b"},{"_id":"a"}]"#));
        assert_eq!(scores.len(), 2);
        assert!(scores["a"] > scores["b"]);
    }

    #[test]
    fn read_text_column_accepts_utf8_largeutf8_and_int() {
        // LargeUtf8 (the MTEB parquet width), Utf8 (common connector width), and an integer id
        // column must all read back as the same text, so a dataset from any connector works.
        let large = Arc::new(LargeStringArray::from(vec![Some("a"), Some("b")]));
        let small = Arc::new(StringArray::from(vec![Some("a"), Some("b")]));
        let ints = Arc::new(Int64Array::from(vec![1_i64, 2]));

        for column in [large as _, small as _, ints as _] {
            let batch =
                RecordBatch::try_from_iter(vec![("id", column)]).expect("failed to build batch");
            let values = read_text_column(&batch, "id").expect("failed to read column");
            // Integer ids stringify to "1"/"2"; string ids stay "a"/"b" — both are exercised here.
            assert_eq!(values.len(), 2);
        }
    }

    #[test]
    fn read_text_column_maps_null_to_empty_string() {
        let column = Arc::new(StringArray::from(vec![Some("x"), None]));
        let batch =
            RecordBatch::try_from_iter(vec![("id", column as _)]).expect("failed to build batch");
        let values = read_text_column(&batch, "id").expect("failed to read column");
        assert_eq!(values, vec!["x".to_string(), String::new()]);
    }

    #[test]
    fn read_text_column_errors_on_missing_column() {
        let column = Arc::new(StringArray::from(vec![Some("x")]));
        let batch =
            RecordBatch::try_from_iter(vec![("id", column as _)]).expect("failed to build batch");
        let err = read_text_column(&batch, "missing").expect_err("missing column should error");
        assert_eq!(err.to_string(), "Missing 'missing' column");
    }

    #[test]
    fn sole_primary_key_value_reads_single_key_regardless_of_name() {
        let mut pk = HashMap::new();
        pk.insert("my_pk".to_string(), serde_json::json!("doc-42"));
        assert_eq!(sole_primary_key_value(&pk), Some("doc-42".to_string()));

        let mut numeric = HashMap::new();
        numeric.insert("id".to_string(), serde_json::json!(7));
        assert_eq!(sole_primary_key_value(&numeric), Some("7".to_string()));
    }

    #[test]
    fn sole_primary_key_value_skips_zero_or_multiple_keys() {
        assert_eq!(sole_primary_key_value(&HashMap::new()), None);

        let mut multi = HashMap::new();
        multi.insert("a".to_string(), serde_json::json!("1"));
        multi.insert("b".to_string(), serde_json::json!("2"));
        assert_eq!(sole_primary_key_value(&multi), None);
    }

    #[test]
    fn primary_key_value_to_string_handles_string_and_number() {
        assert_eq!(
            primary_key_value_to_string(&serde_json::json!("s")),
            Some("s".to_string())
        );
        assert_eq!(
            primary_key_value_to_string(&serde_json::json!(3)),
            Some("3".to_string())
        );
        assert_eq!(primary_key_value_to_string(&serde_json::json!(true)), None);
    }
}
