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

use std::{collections::HashMap, fmt::Display};

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

use super::{Result, SearchAggregationSnafu};

pub type ModelKey = String;
pub type VectorSearchResult = HashMap<TableReference, AggregationResult>;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
pub struct Match {
    /// The matches for this result
    matches: HashMap<String, MatchType>,

    /// Addditional data from the `dataset` requested by the user.
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    data: HashMap<String, Value>,

    /// Primary key(s) identifying the matched item in the dataset
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    primary_key: HashMap<String, Value>,

    /// The similarity of the match to the query
    score: f64,

    /// The name of the dataset where the match was found
    dataset: String,

    #[serde(skip_serializing_if = "HashMap::is_empty")]
    pub metadata: HashMap<String, serde_json::Value>,
}

#[derive(Clone, PartialEq, Debug, Deserialize, Serialize)]
#[cfg_attr(feature = "openapi", derive(utoipa::ToSchema))]
#[serde(untagged)]
pub enum MatchType {
    Single(Value),
    Multiple(Vec<Value>),
}

impl From<Vec<Value>> for MatchType {
    fn from(mut value: Vec<Value>) -> Self {
        if value.len() == 1 {
            let Some(v) = value.pop() else {
                unreachable!("The value array must have one element");
            };
            return MatchType::Single(v);
        }
        MatchType::Multiple(value)
    }
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

    // Sort by score in descending order
    matches.sort_by(|a, b| {
        b.score
            .partial_cmp(&a.score())
            .unwrap_or(std::cmp::Ordering::Equal)
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
) -> Vec<HashMap<String, MatchType>> {
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
                rows[i].insert(key.clone(), row_values.into());
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

    fn sort_result(v: Vec<HashMap<String, MatchType>>) -> Vec<Vec<(String, MatchType)>> {
        v.into_iter()
            .map(|x| {
                x.into_iter()
                    .sorted_by_key(|(a, _)| a.clone())
                    .collect::<Vec<(String, MatchType)>>()
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
}
