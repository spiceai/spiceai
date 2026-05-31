/*
Copyright 2024-2025 The Spice.ai OSS Authors

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

use std::{collections::BTreeMap, collections::HashMap, fmt::Display};

use arrow::error::ArrowError;
use arrow_tools::format::to_markdown_documents;
use datafusion::sql::TableReference;
use futures::StreamExt;
use itertools::Itertools;
use search::aggregation::AggregationResult;
use search::collect_batches;
use search::{SEARCH_SCORE_COLUMN_NAME, aggregation::Error as SearchError};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use snafu::ResultExt;

use crate::error::{Result, SearchAggregationSnafu};

pub type ModelKey = String;
pub type VectorSearchResult = HashMap<TableReference, AggregationResult>;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct Match {
    /// The matches for this result
    matches: HashMap<String, Vec<Value>>,

    /// Addditional data from the `dataset` requested by the user.
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    data: HashMap<String, Value>,

    /// Primary key(s) identifying the matched item in the dataset
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    primary_key: HashMap<String, Value>,

    /// The similarity of the match to the query
    #[serde(rename = "_score")]
    score: f64,

    /// The name of the dataset where the match was found
    dataset: String,

    #[serde(skip_serializing_if = "HashMap::is_empty")]
    pub metadata: HashMap<String, serde_json::Value>,
}

impl Match {
    #[must_use]
    pub fn score(&self) -> f64 {
        self.score
    }

    #[must_use]
    pub fn dataset(&self) -> &str {
        &self.dataset
    }

    #[must_use]
    pub fn primary_key(&self) -> &HashMap<String, serde_json::Value> {
        &self.primary_key
    }

    #[must_use]
    pub fn metadata(&self) -> &HashMap<String, serde_json::Value> {
        &self.metadata
    }
}

pub async fn to_pretty(agg: AggregationResult) -> Result<impl Display, ArrowError> {
    // Add primary keys, 'score' & additional data columns to the document header.
    let header_fields = [
        vec![SEARCH_SCORE_COLUMN_NAME.to_string()],
        agg.primary_key.clone(),
        agg.data_columns.clone(),
    ]
    .concat();
    let rb = collect_batches(agg.data).await?;

    // For each record batch, create markdown documents for each column in `agg.matches`.
    let doc_sets: Vec<String> = agg
        .matches
        .iter()
        .map(|(derived_from, highlight_columns)| {
            highlight_columns
                .iter()
                .map(|col| {
                    to_markdown_documents(
                        rb.as_slice(),
                        col,
                        Some(derived_from.as_str()),
                        header_fields.as_slice(),
                    )
                })
                .collect::<Result<Vec<String>, ArrowError>>()
        })
        .collect::<Result<Vec<Vec<String>>, ArrowError>>()?
        .into_iter()
        .flatten()
        .filter(|s| !s.is_empty())
        .collect::<Vec<String>>();

    Ok(doc_sets.join("\n"))
}

pub async fn to_matches_sorted(result: VectorSearchResult, limit: usize) -> Result<Vec<Match>> {
    let mut matches: Vec<Match> = Vec::new();
    for (a, b) in result {
        let mut o = to_matches(&a, b).await.context(SearchAggregationSnafu)?;
        matches.append(&mut o);
    }

    // Sort by score descending, then by dataset + primary key ascending for deterministic ordering on ties.
    // Use BTreeMap for primary key serialization to ensure deterministic key ordering,
    // since HashMap iteration order is non-deterministic due to hash randomization.
    matches.sort_by(|a, b| {
        b.score
            .partial_cmp(&a.score)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.dataset.cmp(&b.dataset))
            .then_with(|| {
                let a_pk: BTreeMap<_, _> = a.primary_key.iter().collect();
                let b_pk: BTreeMap<_, _> = b.primary_key.iter().collect();
                let a_str = serde_json::to_string(&a_pk).unwrap_or_default();
                let b_str = serde_json::to_string(&b_pk).unwrap_or_default();
                a_str.cmp(&b_str)
            })
    });

    matches.truncate(limit);
    Ok(matches)
}

/// Consumes [`AggregationResult`] and converts all results to [`Match`] format.
pub async fn to_matches(
    tbl: &TableReference,
    mut result: AggregationResult,
) -> std::result::Result<Vec<Match>, SearchError> {
    let mut output = vec![];
    while let Some(Ok(rb)) = result.data.next().await {
        let data = result.data_json(&rb)?;
        let primary_key = result.primary_key_json(&rb)?;

        // Collect the highlights for each column. Value of map is a vector rows, each of which contains the highlights for that row.
        let matches = result
            .matches
            .iter()
            .map(|(underlying, derived_cols)| {
                let z = result
                    .columns_as_json(&rb, derived_cols)?
                    .into_iter()
                    .map(|x| x.into_values().collect_vec())
                    .collect::<Vec<_>>();
                Ok((underlying.clone(), z))
            })
            .collect::<std::result::Result<HashMap<String, Vec<Vec<Value>>>, SearchError>>()?;

        let matches = transpose_and_convert(matches);

        let scores = result.score_values(&rb)?;
        let mut matches = data
            .into_iter()
            .zip(primary_key)
            .zip(matches)
            .zip(scores)
            .map(|(((data, primary_key), matches), score)| Match {
                score,
                data,
                dataset: tbl.to_string(),
                primary_key,
                matches,
                metadata: HashMap::new(),
            })
            .collect::<Vec<_>>();
        output.append(&mut matches);
    }

    Ok(output)
}

/// Convert a map of {column name -> column values}, to a per-row representation.
fn transpose_and_convert(
    column_format: HashMap<String, Vec<Vec<Value>>>,
) -> Vec<HashMap<String, Vec<Value>>> {
    let max_rows = column_format
        .values()
        .map(std::vec::Vec::len)
        .max()
        .unwrap_or(0);

    let key_count = column_format.len();
    let mut rows: Vec<_> = (0..max_rows)
        .map(|_| HashMap::with_capacity(key_count))
        .collect();

    for (key, vv) in column_format {
        for (i, row_values) in vv.into_iter().enumerate() {
            if !row_values.is_empty() {
                rows[i].insert(key.clone(), row_values);
            }
        }
    }

    rows
}

#[cfg(test)]
mod tests {
    use super::*;
    use insta::assert_json_snapshot;
    use serde_json::Value;
    use std::collections::HashMap;

    fn sort_result(v: Vec<HashMap<String, Vec<Value>>>) -> Vec<Vec<(String, Vec<Value>)>> {
        v.into_iter()
            .map(|x| {
                x.into_iter()
                    .sorted_by_key(|(a, _)| a.clone())
                    .collect::<Vec<(String, Vec<Value>)>>()
            })
            .collect::<Vec<_>>()
    }

    #[test]
    fn test_transpose_and_convert_single_column() {
        let mut column_format = HashMap::new();
        column_format.insert(
            "key1".to_string(),
            vec![
                vec![Value::String("A".into())],
                vec![Value::String("B".into())],
                vec![],
            ],
        );

        assert_json_snapshot!(sort_result(transpose_and_convert(column_format)));
    }

    #[test]
    fn test_transpose_and_convert_multiple_columns() {
        let mut column_format = HashMap::new();
        column_format.insert(
            "key1".to_string(),
            vec![
                vec![Value::String("A".into())],
                vec![Value::String("B".into())],
                vec![],
            ],
        );
        column_format.insert(
            "key2".to_string(),
            vec![
                vec![],
                vec![Value::String("C".into())],
                vec![Value::String("D".into())],
            ],
        );

        assert_json_snapshot!(sort_result(transpose_and_convert(column_format)));
    }

    #[test]
    fn test_transpose_and_convert_all_rows_empty() {
        let mut column_format = HashMap::new();
        column_format.insert("key1".to_string(), vec![vec![], vec![], vec![]]);

        assert_json_snapshot!(sort_result(transpose_and_convert(column_format)));
    }

    #[test]
    fn test_transpose_and_convert_mixed_empty_and_non_empty_rows() {
        let mut column_format = HashMap::new();
        column_format.insert(
            "key1".to_string(),
            vec![
                vec![Value::String("A".into())],
                vec![],
                vec![Value::String("B".into())],
            ],
        );

        assert_json_snapshot!(sort_result(transpose_and_convert(column_format)));
    }

    /// Regression test: sorting matches with composite primary keys must produce
    /// deterministic ordering regardless of `HashMap` iteration order.
    #[test]
    fn test_sort_matches_deterministic_composite_pk() {
        let make_match = |score: f64, dataset: &str, pk: Vec<(&str, Value)>| Match {
            score,
            dataset: dataset.to_string(),
            primary_key: pk.into_iter().map(|(k, v)| (k.to_string(), v)).collect(),
            matches: HashMap::new(),
            data: HashMap::new(),
            metadata: HashMap::new(),
        };

        // Two matches with the same score/dataset but different composite primary keys.
        // The keys are "b" and "a" — HashMap might iterate them in any order,
        // but BTreeMap-based serialization will always produce {"a":...,"b":...}.
        let m1 = make_match(
            0.9,
            "ds",
            vec![
                ("b", Value::Number(2.into())),
                ("a", Value::Number(1.into())),
            ],
        );
        let m2 = make_match(
            0.9,
            "ds",
            vec![
                ("b", Value::Number(3.into())),
                ("a", Value::Number(1.into())),
            ],
        );

        let mut matches = [m2, m1];

        // Sort multiple times to verify stability across runs
        for _ in 0..10 {
            matches.sort_by(|a, b| {
                b.score
                    .partial_cmp(&a.score)
                    .unwrap_or(std::cmp::Ordering::Equal)
                    .then_with(|| a.dataset.cmp(&b.dataset))
                    .then_with(|| {
                        let a_pk: BTreeMap<_, _> = a.primary_key.iter().collect();
                        let b_pk: BTreeMap<_, _> = b.primary_key.iter().collect();
                        let a_str = serde_json::to_string(&a_pk).unwrap_or_default();
                        let b_str = serde_json::to_string(&b_pk).unwrap_or_default();
                        a_str.cmp(&b_str)
                    })
            });

            // m1 has a=1,b=2 and m2 has a=1,b=3.
            // Serialized deterministically: {"a":1,"b":2} < {"a":1,"b":3}
            // So m1 should always come first.
            assert_eq!(
                matches[0].primary_key.get("b"),
                Some(&Value::Number(2.into())),
                "First match should have b=2 (lower composite key)"
            );
            assert_eq!(
                matches[1].primary_key.get("b"),
                Some(&Value::Number(3.into())),
                "Second match should have b=3 (higher composite key)"
            );
        }
    }
}
